import asyncio
import time
from unittest import result
from screener.impulses import ImpulseDetector
from screener.ws_manager import WSManager
from screener.symbol_fetcher import SymbolFetcher
from notifier import Notifier
from logger import Logger
from screener.clusters import ClusterManager
from config import (
    ALLOWED_FILTERS,
    ENABLE_ATR_IMPULSE,
    ENABLE_DYNAMIC_THRESHOLD,
    IMPULSE_FIXED_THRESHOLD_PCT,
    CLUSTER_INTERVAL,
    CANDLE_TIMEFRAME_SEC,
)
from screener.signal_hub import SignalHub
from users_store import UsersStore

NUMERIC_KEYS = tuple(ALLOWED_FILTERS.keys())

def fmt_compact_usdt(x: float) -> str:
    try:
        x = float(x or 0)
    except Exception:
        return "0"

    absx = abs(x)

    def _fmt(val: float, suffix: str, dec: int):
        s = f"{val:.{dec}f}"
        # убираем .0
        if s.endswith(".0"):
            s = s[:-2]
        # русская запятая
        s = s.replace(".", ",")
        return f"{s}{suffix}"

    if absx >= 1_000_000_000:
        val = x / 1_000_000_000
        return _fmt(val, "B", 1)  # 1,5B
    if absx >= 1_000_000:
        val = x / 1_000_000
        return _fmt(val, "M", 1)  # 1,2M
    if absx >= 1_000:
        val = x / 1_000
        # K без десятых, как ты просил (300K)
        return _fmt(val, "K", 0)
    return f"{int(x)}"

def user_match_impulse(user_cfg: dict, payload: dict) -> bool:
    for k in NUMERIC_KEYS:
        if float(payload.get(k) or 0) < float(user_cfg.get(k) or 0):
            return False
    return True


class ATRImpulseScreener:
    def __init__(self):
        self.notifier = Notifier()
        self.last_alert_time = {}
        self.symbol_thresholds = {}
        self.cluster_mgr = ClusterManager()
        self.users = UsersStore("users.json")
        # детект/ATR — отдельно от тиков (очередь + воркеры)
        self.detector_queue: asyncio.Queue = asyncio.Queue(maxsize=20000)
        self._detector_tasks: list[asyncio.Task] = []

        self.impulse_detector = ImpulseDetector()
        self.ws_manager = WSManager(self.handle_trade)
        self.symbol_fetcher = SymbolFetcher()
        self.last_price = {}
        self.signal_hub = None
        self._signalhub_server = None


    async def handle_trade(self, symbol, data):
        price = float(data.get("p", 0))
        qty   = float(data.get("q", 0))
        ts    = time.time()

        self.last_price[symbol] = price

        # packing: единственное, что делаем на каждом тике
        finalized = self.cluster_mgr.add_tick(symbol, ts, price, qty)
        if not finalized:
            return

        if not ENABLE_ATR_IMPULSE:
            return

        # 1) обновляем ATR по закрытым кластерам (дёшево)
        # 2) запускаем детект НЕ на каждом закрытом cid, а один раз по последнему cid
        #    (детектор сам смотрит окно назад). Это снижает нагрузку.
        last_cid = int(finalized[-1])

        last_bucket = None
        for cid in finalized:
            close_ts = (int(cid) + 1) * float(CLUSTER_INTERVAL)
            bucket = int(close_ts // float(CANDLE_TIMEFRAME_SEC))
            # для flat-кластеров (пустые интервалы) достаточно обработать 1 кластер на bucket
            if last_bucket is None or bucket != last_bucket:
                self.cluster_mgr.on_cluster_close(symbol, int(cid), close_ts)
                last_bucket = bucket

        # ставим в очередь одно событие на символ (по последнему закрытому cid)
        try:
            self.detector_queue.put_nowait((symbol, last_cid))
        except asyncio.QueueFull:
            # под нагрузкой лучше пропустить часть проверок, чем копить лаг
            pass


    async def run(self):
        await self.notifier.start()
        await self.notifier.send_message("✅ ATR-скринер запущен.")
        await self.ws_manager.start()
        # детект-воркеры (не блокируют обработку тиков)
        # 2 воркера обычно достаточно: детект — CPU-лёгкий, но может быть много символов.
        if ENABLE_ATR_IMPULSE:
            self._detector_tasks = [
                asyncio.create_task(self._detector_worker(i), name=f"detector_worker_{i}")
                for i in range(2)
            ]

        self.signal_hub = SignalHub(
            auth_resolver=self.users.resolve_token,
            config_getter_for_user=self.users.get_user_cfg,
            config_patcher_for_user=self.users.patch_user_cfg,
            top_provider=self._get_top
        )
        self._signalhub_server = await self.signal_hub.start()
        self.notifier.set_signal_hub(self.signal_hub)


        try:
            while True:
                symbols_24h_volume = await self.symbol_fetcher.fetch_futures_symbols()

                # сохраняем 24h объём
                self.symbol_24h_volume = symbols_24h_volume
                self.symbol_thresholds = symbols_24h_volume["thresholds"]
                Logger.info(
                    f"Threshold mode: "
                    f"{'dynamic' if ENABLE_DYNAMIC_THRESHOLD else f'fixed={IMPULSE_FIXED_THRESHOLD_PCT}%'}"
                )
                # создаём список символов
                symbols = list(symbols_24h_volume["volumes"].keys())

                # сортировка: объём от большего к меньшему
                symbols.sort(key=lambda s: symbols_24h_volume["volumes"][s], reverse=True)

                Logger.info(f"Всего символов после фильтров: {len(symbols)}")
                Logger.info("Символ — Объём — Threshold:")

                for s in symbols:
                    vol = symbols_24h_volume["volumes"][s]
                    th = symbols_24h_volume["thresholds"][s]
                    Logger.info(f"{s.upper()}: {vol:,.0f} USDT — порог {th}%")

                Logger.info(f"Всего символов после фильтров: {len(symbols)}")
                #Logger.info(f"Символы:\n{', '.join(symbols)}")
                await self.ws_manager.set_symbols(symbols)
                await asyncio.sleep(3600)

        finally:
            # если run() отменили (Ctrl+C) — всё аккуратно закрываем
            await self.close()

    async def _detector_worker(self, wid: int):
        while True:
            symbol, cid = await self.detector_queue.get()
            try:
                threshold = self.symbol_thresholds.get(
                    symbol.lower(),
                    float(IMPULSE_FIXED_THRESHOLD_PCT),
                )

                res = await self.impulse_detector.check_on_cluster(
                    symbol=symbol,
                    last_closed_cid=int(cid),
                    cluster_mgr=self.cluster_mgr,
                    last_alert_time=self.last_alert_time,
                    symbol_threshold=float(threshold),
                )
                if res:
                    await self._deliver_impulse(res, time.time())
            except Exception as e:
                Logger.error(f"detector_worker[{wid}] err: {e}")
            finally:
                self.detector_queue.task_done()

    async def close(self):
        # остановить детектор-воркеры
        if self._detector_tasks:
            for t in self._detector_tasks:
                t.cancel()
            self._detector_tasks.clear()
        # стопнуть все binance ws таски
        await self.ws_manager.stop()

        # закрыть signalhub server
        if self._signalhub_server is not None:
            self._signalhub_server.close()
            await self._signalhub_server.wait_closed()
            self._signalhub_server = None

        # закрыть aiohttp сессию телеги
        await self.notifier.close()

    async def _get_top(self, mode: str, n: int):
        if not hasattr(self, "symbol_24h_volume") or not self.symbol_24h_volume:
            return []
        vols = self.symbol_24h_volume.get("volumes", {})
        items = sorted(vols.items(), key=lambda x: x[1], reverse=True)[:n]
        return [{"symbol": s.upper(), "value": float(v)} for s, v in items]



    async def _deliver_impulse(self, result: dict, ts: float) -> None:
        symbol_up = str(result["symbol"]).upper()

        # метрики символа из symbol_fetcher
        vol24h = float(self.symbol_24h_volume["volumes"].get(symbol_up.lower(), 0))
        trades24h = int((self.symbol_24h_volume.get("trades24h") or {}).get(symbol_up.lower(), 0))
        ob = (self.symbol_24h_volume.get("orderbook") or {}).get(symbol_up.lower(), {}) or {}

        payload = {
            "type": "impulse",
            "exchange": "BINANCE-FUT",
            "market": "FUTURES",
            "symbol": symbol_up,

            # ключи строго как в ALLOWED_FILTERS
            "volume_threshold": float(vol24h),                 # реальный 24h volume
            "min_trades_24h": int(trades24h),                  # реальные 24h trades
            "orderbook_min_bid": float((ob or {}).get("bid", 0.0)),    # реальный bid-volume в стакане
            "orderbook_min_ask": float((ob or {}).get("ask", 0.0)),    # реальный ask-volume в стакане
            "impulse_trades": int(result.get("impulse_trades") or 0),  # trades внутри импульса
        }

        # данные для сообщения
        
        ref_price = float(result.get("ref_price") or 0.0)
        trigger_price = float(result.get("trigger_price") or result.get("cur") or 0.0)
        max_delta_price = float(result.get("max_delta_price") or 0.0)
        cur_price = trigger_price
        pct_from_start = float(result.get("change_percent_from_start") or 0.0)
        pct_max_delta  = float(result.get("change_percent_max_delta") or 0.0)

        atr_from_start = float(result.get("atr_from_start") or 0.0)
        atr_max_delta  = float(result.get("atr_max_delta") or 0.0)

        duration = float(result.get("duration") or 0.0)
        change_percent = float(pct_from_start)  # для строки "Изменение" логичнее % от начала
        speed_percent = abs(change_percent) / max(duration, float(CLUSTER_INTERVAL))
        impulse_vol = float(result.get("impulse_volume_usdt") or 0.0)

        direction = cur_price - ref_price
        color = "🟢" if direction > 0 else "🔴"
        direction_text = "Памп" if direction > 0 else "Дамп"

        message = (
            f"{color} <code>{symbol_up}</code> {direction_text}\n"
            f"Изменение: {change_percent:.2f}% за {duration:.2f} сек\n\n"
            f"🚀 Цена: {cur_price}\n\n"

            f"Скорость: {speed_percent:.3f}%/сек\n"
            
            f"🎯 Цена срабатывания: {trigger_price}\n"
            f"📍 Цена начала импульса: {ref_price}\n"
            f"🏁 Цена max дельты: {max_delta_price}\n\n"

            f"📈 % от начала: {pct_from_start:.2f}%\n"
            f"📈 % max дельты: {pct_max_delta:.2f}%\n\n"

            f"📐 ATR от начала: {atr_from_start:.2f} ATR\n"
            f"📐 ATR max дельты: {atr_max_delta:.2f} ATR\n\n"

            f"📊 Объём 24ч: {fmt_compact_usdt(vol24h)} USDT\n"
            f"🔥 Объём импульса: {fmt_compact_usdt(impulse_vol)} USDT "
            f"({payload['impulse_trades']} сделок)"
        )

        # админ
        await self.notifier.send_message(message)
        Logger.info(f"ADMIN notify: {symbol_up} ({change_percent:.2f}%)")

        # пользователи (без sent_to_users — антиспам уже в impulses.py)
        for uid, user in self.users.all_users().items():
            if not user_match_impulse(user.cfg, payload):
                continue

            if self.signal_hub:
                await self.signal_hub.send_to_user(uid, payload)

            if user.tg_chat_id:
                await self.notifier.send_message(message, chat_id=user.tg_chat_id)

            Logger.info(
                f"DELIVER impulse: {symbol_up} -> user={uid} "
                f"(tg={'yes' if user.tg_chat_id else 'no'}, ws={'yes' if self.signal_hub else 'no'}) "
                f"chg={change_percent:.2f}% trades={payload['impulse_trades']}"
            )


