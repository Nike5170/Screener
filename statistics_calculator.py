# statistics_calculator.py
import time
from collections import defaultdict
from config import STAT_RISE_THRESHOLD, STAT_FALL_THRESHOLD
from logger import Logger


class StatisticsCalculator:
    def __init__(self):
        self.impulse_records = defaultdict(list)
        self.stats = defaultdict(lambda: {"rise_count": 0, "fall_count": 0, "total": 0})

    def record_impulse(self, symbol, ref_time, ref_price, cur_price, direction):
        self.impulse_records[symbol].append({
            "ref_time": ref_time,
            "ref_price": ref_price,
            "cur_price": cur_price,
            "direction": direction,
            "checked": False
        })
        self.stats[symbol]["total"] += 1

    async def update_impulse(self, symbol, price_history):
        """
        Проверяем новые тики для импульсов этого символа и обновляем статистику.
        """
        if symbol not in price_history:
            return

        prices = price_history[symbol]
        if not prices:
            return

        for rec in self.impulse_records[symbol]:
            if rec["checked"]:
                continue

            ref_time = rec["ref_time"]
            ref_price = rec["ref_price"]
            direction = rec["direction"]

            # все тики после импульса
            post_ticks = [(t, p) for t, p in prices if t >= ref_time]
            for t, price in post_ticks:
                change = (price - ref_price) / ref_price * 100

                if direction > 0:
                    if change >= STAT_RISE_THRESHOLD:
                        self.stats[symbol]["rise_count"] += 1
                        rec["checked"] = True
                        break
                    if change <= -STAT_FALL_THRESHOLD:
                        self.stats[symbol]["fall_count"] += 1
                        rec["checked"] = True
                        break
                else:
                    if change <= -STAT_RISE_THRESHOLD:
                        self.stats[symbol]["rise_count"] += 1
                        rec["checked"] = True
                        break
                    if change >= STAT_FALL_THRESHOLD:
                        self.stats[symbol]["fall_count"] += 1
                        rec["checked"] = True
                        break

    def get_statistics(self, symbol=None):
        if symbol:
            return self.stats.get(symbol, {"rise_count": 0, "fall_count": 0, "total": 0})
        return self.stats

    def log_statistics(self):
        for symbol, stat in self.stats.items():
            Logger.info(
                f"Статистика для {symbol.upper()}: "
                f"Всего импульсов={stat['total']}, "
                f"Рост >{STAT_RISE_THRESHOLD}%={stat['rise_count']}, "
                f"Обратное движение >{STAT_FALL_THRESHOLD}%={stat['fall_count']}"
            )
