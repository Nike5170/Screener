import asyncio
import time
from unittest import result
from screener.impulses import ImpulseDetector
from screener.ws_manager import WSManager
from screener.symbol_fetcher import SymbolFetcher
from notifier import Notifier
from logger import Logger
from datetime import datetime
from screener.clusters import ClusterManager
from config import ENABLE_ATR_IMPULSE, ENABLE_DYNAMIC_THRESHOLD, ENABLE_MARK_DELTA, IMPULSE_FIXED_THRESHOLD_PCT
from screener.signal_hub import SignalHub
import math
from users_store import UsersStore

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

def fmt_signed_pct(x: float, decimals: int = 3) -> str:
    try:
        x = float(x)
    except Exception:
        return "0%"
    s = f"{x:+.{decimals}f}".replace(".", ",")
    return f"{s}%"

def dyn_threshold(volume: float, v_min: float, v_max: float, p_min: float, p_max: float, exponent: float) -> float:
    # защита от мусора
    volume = float(volume or 0.0)
    v_min = max(float(v_min or 1.0), 1.0)
    v_max = max(float(v_max or v_min), v_min + 1.0)
    p_min = float(p_min or 0.5)
    p_max = float(p_max or 5.0)
    exponent = float(exponent or 0.8)

    x = min(max(volume, v_min), v_max)
    norm = (math.log10(x) - math.log10(v_min)) / (math.log10(v_max) - math.log10(v_min))
    factor = max(0.0, min(1.0, norm)) ** exponent
    percent = p_max - (p_max - p_min) * factor
    return float(percent)

def user_match_impulse(user_cfg: dict, payload: dict, vol24h: float, trades24h: int, ob: dict) -> bool:
    # exclude
    excl = set((user_cfg.get("exclude_symbols") or []))
    if payload.get("symbol", "").upper() in excl:
        return False

    # enable blocks
    atr_enabled = bool((user_cfg.get("atr_impulse") or {}).get("enabled", True))
    mark_enabled = bool((user_cfg.get("mark_delta") or {}).get("enabled", True))

    reason = set(payload.get("reason") or [])
    has_mark = ("mark_delta" in reason) and (payload.get("mark_delta_pct") is not None)

    # Если ATR выключен, а mark включен — пропускаем только события, где реально есть mark_delta
    if (not atr_enabled) and mark_enabled:
        if not has_mark:
            return False

    # Если mark выключен — не требуем mark_delta
    # Если оба выключены — смысла нет
    if (not atr_enabled) and (not mark_enabled):
        return False

    # volume / trades24h / orderbook — только ужесточение (вариант A)
    v_thr = float(user_cfg.get("volume_threshold") or 20_000_000)
    if vol24h < v_thr:
        return False

    t_thr = int(user_cfg.get("min_trades_24h") or 10_000)
    if trades24h < t_thr:
        return False

    ob_bid_thr = float(user_cfg.get("orderbook_min_bid") or 20_000)
    ob_ask_thr = float(user_cfg.get("orderbook_min_ask") or 20_000)
    if float((ob or {}).get("bid", 0)) < ob_bid_thr:
        return False
    if float((ob or {}).get("ask", 0)) < ob_ask_thr:
        return False

    # impulse filters (тоже ужесточение)
    imp = user_cfg.get("impulse") or {}

    impulse_min_trades = int(imp.get("impulse_min_trades") or 1000)
    if int(payload.get("impulse_trades") or 0) < impulse_min_trades:
        return False

    # динамический порог по % для юзера
    p_min = float(imp.get("p_min") or 0.5)
    p_max = float(imp.get("p_max") or 5.0)
    exponent = float(imp.get("exponent") or 0.8)
    # v_min = user volume_threshold, v_max фикс 5B
    user_thr = dyn_threshold(vol24h, v_thr, 5_000_000_000, p_min, p_max, exponent)
    if float(payload.get("change_percent") or 0.0) < user_thr:
        return False

    # ATR multiplier фильтруем по atr_impulse (амплитуда в ATR)
    atr_mult = float(imp.get("atr_multiplier") or 2.0)
    if float(payload.get("atr_impulse") or 0.0) < atr_mult:
        return False

    # mark_delta pct фильтруем, если включено
    if mark_enabled:
        md = user_cfg.get("mark_delta") or {}
        md_thr = float(md.get("pct") or 1.0)
        md_val = payload.get("mark_delta_pct")
        if md_val is None or abs(float(md_val)) < md_thr:
            return False

    return True

class ATRImpulseScreener:
    def __init__(self):
        self.notifier = Notifier()
        self.last_alert_time = {}
        self.symbol_thresholds = {}
        self.cluster_mgr = ClusterManager()
        self.users = UsersStore("users.json")

        self.impulse_detector = ImpulseDetector()
        self.ws_manager = WSManager(self.handle_trade)
        self.ws_manager.set_mark_handler(self.handle_mark)
        self.symbol_fetcher = SymbolFetcher()
        self.last_price = {}
        self.mark_price = {}
        self.signal_hub = None
        self._signalhub_server = None
        # Чтобы отслеживать активные WS-задания
        self.active_ws_tasks = {}


    async def handle_trade(self, symbol, data):
        price = float(data.get("p", 0))
        qty   = float(data.get("q", 0))
        ts    = time.time()

        self.last_price[symbol] = price

        # ЕДИНСТВЕННОЕ место, где обновляется "история"
        self.cluster_mgr.add_tick(symbol, ts, price, qty)

        threshold = self.symbol_thresholds.get(
            symbol.lower(),
            float(IMPULSE_FIXED_THRESHOLD_PCT),
        )
        if not ENABLE_ATR_IMPULSE:
            return

        result = await self.impulse_detector.check_atr_impulse(
            symbol=symbol,
            cluster_mgr=self.cluster_mgr,
            last_alert_time=self.last_alert_time,
            symbol_threshold=threshold,
            last_price_map=self.last_price,
            mark_price_map=self.mark_price,
        )
        if not result:
            return

        await self._deliver_impulse(result, ts)


    async def run(self):
        await self.notifier.start()
        await self.notifier.send_message("✅ ATR-скринер запущен.")

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

                # Запуск WS для новых символов
                for symbol in symbols:
                    if symbol not in self.active_ws_tasks:
                        Logger.info(f"Запущен WebSocket для {symbol}")
                        self.ws_manager.start_task(symbol)
                        self.active_ws_tasks[symbol] = True

                # Остановка WS для неактивных символов
                to_remove = [s for s in self.active_ws_tasks if s not in symbols]
                for s in to_remove:
                    self.ws_manager.stop_task(s)
                    del self.active_ws_tasks[s]

                await asyncio.sleep(3600)

        finally:
            # если run() отменили (Ctrl+C) — всё аккуратно закрываем
            await self.close()

    async def close(self):
        # стопнуть все binance ws таски
        for s in list(self.active_ws_tasks.keys()):
            self.ws_manager.stop_task(s)
            self.active_ws_tasks.pop(s, None)

        # закрыть signalhub server
        if self._signalhub_server is not None:
            self._signalhub_server.close()
            await self._signalhub_server.wait_closed()
            self._signalhub_server = None

        # закрыть aiohttp сессию телеги
        await self.notifier.close()
        
    async def handle_mark(self, symbol, data):
        if not ENABLE_MARK_DELTA:
            return
        mp = float(data.get("p", 0))
        if mp:
            self.mark_price[symbol] = mp
            self.cluster_mgr.add_mark(symbol, time.time(), mp)


    def _get_runtime_config(self):
        from config import (
            IMPULSE_MAX_LOOKBACK, IMPULSE_MIN_LOOKBACK, IMPULSE_MIN_TRADES,
            CLUSTER_INTERVAL, MARK_DELTA_PCT, ENABLE_ATR_IMPULSE, ENABLE_MARK_DELTA
        )
        return {
            "IMPULSE_MAX_LOOKBACK": IMPULSE_MAX_LOOKBACK,
            "IMPULSE_MIN_LOOKBACK": IMPULSE_MIN_LOOKBACK,
            "IMPULSE_MIN_TRADES": IMPULSE_MIN_TRADES,
            "CLUSTER_INTERVAL": CLUSTER_INTERVAL,
            "MARK_DELTA_PCT": MARK_DELTA_PCT,
            "ENABLE_ATR_IMPULSE": ENABLE_ATR_IMPULSE,
            "ENABLE_MARK_DELTA": ENABLE_MARK_DELTA,
        }

    def _patch_runtime_config(self, patch: dict):
        import config as C
        allow = set(self._get_runtime_config().keys())
        for k, v in (patch or {}).items():
            if k in allow:
                setattr(C, k, v)
        return self._get_runtime_config()

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
            "change_percent": float(result.get("change_percent") or 0.0),
            "impulse_trades": int(result.get("impulse_trades") or 0),
            "impulse_volume_usdt": float(result.get("impulse_volume_usdt") or 0.0),
            "atr_impulse": float(result.get("atr_impulse") or 0.0),
            "mark_delta_pct": result.get("mark_delta_pct"),
            "mark_extreme": result.get("mark_extreme"),
            "ts": float(ts),
            "reason": result.get("reason") or ["atr"],
        }

        # данные для сообщения
        cur_price = float(result.get("cur") or 0.0)
        ref_price = float(result.get("ref_price") or 0.0)
        trigger_price = float(result.get("trigger_price") or result.get("cur") or 0.0)
        max_delta_price = float(result.get("max_delta_price") or 0.0)

        pct_from_start = float(result.get("change_percent_from_start") or 0.0)
        pct_max_delta  = float(result.get("change_percent_max_delta") or 0.0)

        atr_from_start = float(result.get("atr_from_start") or 0.0)
        atr_max_delta  = float(result.get("atr_max_delta") or 0.0)

        duration = float(result.get("duration") or 0.001)
        change_percent = float(payload["change_percent"])
        speed_percent = change_percent / max(duration, 0.001)

        direction = (cur_price - ref_price)
        color = "🟢" if direction > 0 else "🔴"
        direction_text = "Памп" if direction > 0 else "Дамп"

        # mark block
        mark_block = ""
        if ENABLE_MARK_DELTA:
            mark_trigger = payload.get("mark_delta_pct")
            mark_extreme = payload.get("mark_extreme")
            if mark_trigger is not None:
                mark_block += f"🧷 Δ Mark-Last (текущий экстремум): {fmt_signed_pct(mark_trigger)}\n"
            if mark_extreme:
                mark_block += (
                    f"📈 Δ Mark-Last max (окно): {fmt_signed_pct(mark_extreme['delta'])} "
                    f"(mark updates: {mark_extreme['mark_updates']})\n"
                )

        message = (
            f"{color} <code>{symbol_up}</code> {direction_text}\n"
            f"Изменение: {change_percent:.2f}% за {duration:.2f} сек\n\n"
            f"🚀 Цена: {cur_price}\n\n"

            f"Скорость: {speed_percent:.3f}%/сек\n"
            f"📐 Амплитуда: {float(payload['atr_impulse']):.2f} ATR\n"
            

            f"🎯 Цена срабатывания: {trigger_price}\n"
            f"📍 Цена начала импульса: {ref_price}\n"
            f"🏁 Цена max дельты: {max_delta_price}\n\n"

            f"📈 % от начала: {pct_from_start:.2f}%\n"
            f"📈 % max дельты: {pct_max_delta:.2f}%\n\n"

            f"📐 ATR от начала: {atr_from_start:.2f} ATR\n"
            f"📐 ATR max дельты: {atr_max_delta:.2f} ATR\n\n"

            f"{mark_block}\n"
            f"📊 Объём 24ч: {fmt_compact_usdt(vol24h)} USDT\n"
            f"🔥 Объём импульса: {fmt_compact_usdt(payload['impulse_volume_usdt'])} USDT "
            f"({payload['impulse_trades']} сделок)"
        )

        # админ
        await self.notifier.send_message(message)
        Logger.info(f"ADMIN notify: {symbol_up} ({change_percent:.2f}%)")

        # пользователи (без sent_to_users — антиспам уже в impulses.py)
        for uid, user in self.users.all_users().items():
            if not user_match_impulse(user.cfg, payload, vol24h, trades24h, ob):
                continue

            if self.signal_hub:
                await self.signal_hub.send_to_user(uid, payload)

            if user.tg_chat_id:
                await self.notifier.send_message(message, chat_id=user.tg_chat_id)

            Logger.info(
                f"DELIVER impulse: {symbol_up} -> user={uid} "
                f"(tg={'yes' if user.tg_chat_id else 'no'}, ws={'yes' if self.signal_hub else 'no'}) "
                f"chg={payload['change_percent']:.2f}% trades={payload['impulse_trades']}"
            )


