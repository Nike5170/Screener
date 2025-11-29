import time
import asyncio
from collections import defaultdict, deque
from config import ATR_PERIOD, CANDLE_TIMEFRAME_SEC

class ATRCalculator:
    def __init__(self):
        self.candles = defaultdict(lambda: deque(maxlen=ATR_PERIOD))  
        self.current_candle = {}
        self.atr_cache = {}
        self.running_tasks = {}
        self.locks = defaultdict(asyncio.Lock)

    async def update_atr_throttled(self, symbol, price_history):
        # ⬇️ атомарная блокировка
        async with self.locks[symbol]:
            await self.update_atr(symbol, price_history)

    async def update_atr(self, symbol, price_history):
        if symbol not in price_history or not price_history[symbol]:
            return

        ts, price = price_history[symbol][-1]
        minute = int(ts // CANDLE_TIMEFRAME_SEC)

        candle_just_closed = False

        # ---------------------------------
        #   ОБРАБОТКА СВЕЧИ
        # ---------------------------------
        if symbol not in self.current_candle or self.current_candle[symbol]["minute"] != minute:

            # если была предыдущая свеча → она закрывается
            if symbol in self.current_candle:
                old = self.current_candle[symbol]
                self.candles[symbol].append(old)
                candle_just_closed = True

            # открываем новую свечу
            self.current_candle[symbol] = {
                "minute": minute,
                "open": price,
                "high": price,
                "low": price,
                "close": price,
            }

        else:
            # обновление текущей свечи
            c = self.current_candle[symbol]
            c["high"] = max(c["high"], price)
            c["low"] = min(c["low"], price)
            c["close"] = price

        # ---------------------------------
        #   ATR считаем ТОЛЬКО при закрытии свечи
        # ---------------------------------
        if not candle_just_closed:
            return  # ❗ выходим мгновенно

        if len(self.candles[symbol]) < 2:
            return

        # ---------- ATR расчёт ----------
        prev_close = None
        tr_list = []

        for candle in self.candles[symbol]:
            high = candle["high"]
            low = candle["low"]
            close = candle["close"]

            if prev_close is None:
                tr = high - low
            else:
                tr = max(
                    high - low,
                    abs(high - prev_close),
                    abs(low - prev_close)
                )

            tr_list.append(tr)
            prev_close = close

        atr = sum(tr_list) / len(tr_list)
        self.atr_cache[symbol] = atr
