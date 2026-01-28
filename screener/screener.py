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
from config import ENABLE_ATR_IMPULSE, ENABLE_MARK_DELTA
from screener.signal_hub import SignalHub

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


class ATRImpulseScreener:
    def __init__(self):
        self.notifier = Notifier()
        self.last_alert_time = {}
        self.symbol_thresholds = {}
        self.cluster_mgr = ClusterManager()

        self.impulse_detector = ImpulseDetector()
        self.ws_manager = WSManager(self.handle_trade)
        self.ws_manager.set_mark_handler(self.handle_mark)
        self.symbol_fetcher = SymbolFetcher()
        self.last_price = {}
        self.mark_price = {}
        self.signal_hub = None
        # Чтобы отслеживать активные WS-задания
        self.active_ws_tasks = {}

    async def handle_trade(self, symbol, data):
        price = float(data.get("p", 0))
        qty   = float(data.get("q", 0))
        ts    = time.time()

        self.last_price[symbol] = price

        # ЕДИНСТВЕННОЕ место, где обновляется "история"
        self.cluster_mgr.add_tick(symbol, ts, price, qty)

        threshold = self.symbol_thresholds.get(symbol.lower(), 1.0)

        result = None
        if ENABLE_ATR_IMPULSE:
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

        cur = result["cur"]
        ref_price = result["ref_price"]
        ref_time = result["ref_time"]

        max_delta = result["max_delta"]
        max_delta_price = result["max_delta_price"]
        change_percent = result["change_percent"]
        now = time.time()

        impulse_trade_count = result["impulse_trades"]
        impulse_volume = result["impulse_volume_usdt"]
        reason = result.get("reason") or ["atr"]

        volume_24h = self.symbol_24h_volume["volumes"].get(symbol.lower(), 0)
        
        mark_trigger = result.get("mark_delta_pct")  # будет signed после правки impulses.py
        mark_extreme = None
        if ENABLE_MARK_DELTA:
            mark_extreme = self.cluster_mgr.get_mark_last_delta_extreme(symbol, ref_time, now)

        mark_block = ""
        if ENABLE_MARK_DELTA:
            if mark_trigger is not None:
                mark_block += f"🧷 Δ Mark-Last (срабатывание): {fmt_signed_pct(mark_trigger)}\n"
            if mark_extreme:
                mark_block += (
                    f"📈 Δ Mark-Last max (импульс): {fmt_signed_pct(mark_extreme['delta'])} "
                    f"(mark updates: {mark_extreme['mark_updates']})\n"
                )


        symbol_up = symbol.upper()
        if self.signal_hub:
            await self.signal_hub.broadcast({
                "type": "impulse",
                "exchange": "BINANCE-FUT",
                "market": "FUTURES",
                "symbol": symbol_up,
                "change_percent": change_percent,
                "impulse_trades": impulse_trade_count,
                "impulse_volume_usdt": impulse_volume,
                "ts": ts,
                "reason": ["atr", "trades"]
            })

        Logger.success(
            f"[{datetime.now().strftime('%H:%M:%S.%f')[:-3]}] ⚡ Сигнал отправлен в Signal Hub: {symbol_up}"
        )

        atr_value = self.cluster_mgr.get_atr(symbol) or 0.0
        atr_percent = (atr_value / cur * 100) if (cur and atr_value) else 0


        direction = result["direction"]
        color = "🟢" if direction > 0 else "🔴"
        direction_text = "Памп" if direction > 0 else "Дамп"
        duration = now - ref_time
        speed_percent = change_percent / max(duration, 0.001)
        atr_impulse = (abs(cur - ref_price) / atr_value) if atr_value else 0

        message = (
            f"{color} <code>{symbol_up}</code> {direction_text}\n"
            f"Изменение: {change_percent:.2f}% за {duration:.2f} сек\n"
            f"(Futures Binance, NATR 1m/14: {atr_percent:.2f}%)\n\n"
            f"📍 Начальная цена импульса: {ref_price}\n"
            f"📉 Цена максимальной дельты: {max_delta_price} (Δ={max_delta:.4f})\n"
            f"🚀 Цена срабатывания: {cur}\n\n"
            f"{mark_block}\n"
            f"Скорость: {speed_percent:.3f}%/сек\n"
            f"📐 Амплитуда импульса: {atr_impulse:.2f} ATR\n"
            f"📊 Объём 24ч: {fmt_compact_usdt(volume_24h)} USDT\n"
            f"🔥 Объём за импульс: {fmt_compact_usdt(impulse_volume)} USDT ({impulse_trade_count} сделок)"

        )

        # ████████████████████
        #    FIX: sending
        # ████████████████████
        await self.notifier.send_message(message)

        Logger.success(
            f"[{datetime.now().strftime('%H:%M:%S.%f')[:-3]}] ⚡ Сигнал отправлен в Telegram Worker: {symbol_up}"
        )
        self.last_alert_time[symbol] = now

    async def run(self):
        await self.notifier.start()
        await self.notifier.send_message("✅ ATR-скринер запущен.")

        self.signal_hub = SignalHub(
            config_getter=self._get_runtime_config,
            config_patcher=self._patch_runtime_config,
            top_provider=self._get_top
        )
        await self.signal_hub.start()
        self.notifier.set_signal_hub(self.signal_hub)

        try:
            while True:
                symbols_24h_volume = await self.symbol_fetcher.fetch_futures_symbols()

                # сохраняем 24h объём
                self.symbol_24h_volume = symbols_24h_volume
                self.symbol_thresholds = symbols_24h_volume["thresholds"]

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
