import asyncio
import time
from screener.atr import ATRCalculator
from screener.volume import VolumeCalculator
from screener.impulses import ImpulseDetector
from screener.ws_manager import WSManager
from screener.symbol_fetcher import SymbolFetcher
from notifier import Notifier
from logger import Logger
from datetime import datetime
from collections import deque
from config import PRICE_HISTORY_MAXLEN, CLUSTER_INTERVAL, VOLUME_HISTORY_MAXLEN
from statistics_calculator import StatisticsCalculator


class ATRImpulseScreener:
    def __init__(self):
        self.notifier = Notifier()
        self.price_history = {}
        self.volume_history = {}
        self.last_alert_time = {}
        self.symbol_thresholds = {}
        self.stats_calc = StatisticsCalculator()

        self.atr_calculator = ATRCalculator()
        # self.volume_calculator = VolumeCalculator()
        self.impulse_detector = ImpulseDetector()
        self.ws_manager = WSManager(self.handle_trade)
        self.symbol_fetcher = SymbolFetcher()

        # Чтобы отслеживать активные WS-задания
        self.active_ws_tasks = {}

    async def handle_trade(self, symbol, data):

        price = float(data.get("p", 0))
        qty   = float(data.get("q", 0))
        ts    = time.time()

        self.price_history.setdefault(symbol, deque(maxlen=PRICE_HISTORY_MAXLEN)).append((ts, price))
        self.volume_history.setdefault(symbol, deque(maxlen=VOLUME_HISTORY_MAXLEN)).append((ts, qty))

        asyncio.create_task(self.atr_calculator.update_atr_throttled(symbol, self.price_history))

        atr_cache = self.atr_calculator.atr_cache
        threshold = self.symbol_thresholds.get(symbol.lower(), 1.0)


        # ---- Импульс ----
        result = await self.impulse_detector.check_atr_impulse(
            symbol,
            self.price_history,
            atr_cache,
            self.last_alert_time,
            threshold
        )

        if not result:
            return

        symbol_up = symbol.upper()

        cur = result["cur"]
        ref_price = result["ref_price"]
        ref_time = result["ref_time"]

        cluster_ticks = result.get("cluster_ticks", [])

        Logger.warn("\n=== IMPULSE CLUSTER DETECTED ===")
        Logger.warn(f"Symbol: {symbol_up}")
        Logger.warn(f"Impulse detected at time: {ref_time:.3f}")
        Logger.warn(f"Cluster ID: {result['cluster_id']}")

        if cluster_ticks:
            Logger.warn(f"Cluster tick count: {len(cluster_ticks)}")
            Logger.warn("Cluster ticks:")
            for tick_time, tick_price in cluster_ticks:
                Logger.warn(f"  t={tick_time:.3f}, price={tick_price}")
        else:
            Logger.warn("❗Cluster ticks empty!")


        max_delta = result["max_delta"]
        max_delta_price = result["max_delta_price"]
        change_percent = result["change_percent"]
        now = time.time()

        # ████████████████████
        #     24H Volume
        # ████████████████████
        volume_24h = self.symbol_24h_volume["volumes"].get(symbol.lower(), 0)

        impulse = [
            (t, p, q) for (t, p), (_, q) in zip(
                self.price_history[symbol],
                self.volume_history[symbol]
            )
            if ref_time <= t <= now
        ]

        impulse_volume = sum(p * q for (t, p, q) in impulse)
        impulse_trade_count = len(impulse)

        atr_value = atr_cache.get(symbol, 0)
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
            f"Скорость: {speed_percent:.3f}%/сек\n"
            f"📐 Амплитуда импульса: {atr_impulse:.2f} ATR\n"
            f"📊 Объём 24ч: {volume_24h:,.0f} USDT\n"
            f"🔥 Объём за импульс: {impulse_volume:,.1f} USDT ({impulse_trade_count} сделок)"
        )

        Logger.success(
            f"[{datetime.now().strftime('%H:%M:%S.%f')[:-3]}] ⚡ Сигнал готов к отправке: {symbol_up}"
        )

        # ████████████████████
        #    FIX: sending
        # ████████████████████
        await self.notifier.send_clipboard(symbol_up)
        await self.notifier.send_message(message)

        self.last_alert_time[symbol] = now
        self.stats_calc.record_impulse(
            symbol=symbol,
            ref_time=ref_time,
            ref_price=ref_price,
            cur_price=cur,
            direction=direction
        )
        asyncio.create_task(self.stats_calc.update_impulse(symbol, self.price_history))

    async def run(self):
        await self.notifier.start()
        await self.notifier.send_message("✅ ATR-скринер запущен.")
        await self.notifier.init_clipboard()

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



