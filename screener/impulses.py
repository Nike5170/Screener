import time

from config import (
    CLUSTER_INTERVAL,
    IMPULSE_MAX_LOOKBACK,
    IMPULSE_MIN_LOOKBACK,
    ATR_MULTIPLIER,
    ANTI_SPAM_PER_SYMBOL,
    ANTI_SPAM_BURST_COUNT,
    ANTI_SPAM_BURST_WINDOW,
    ANTI_SPAM_SILENCE,
)
from logger import Logger


class ImpulseDetector:
    def __init__(self):
        self.alert_times = []
        self.silence_until = 0

    async def check_atr_impulse(self, symbol, price_history, atr_cache, last_alert_time, symbol_threshold, cluster_extremes):
        now = time.time()
        prices = price_history.get(symbol, [])
        if len(prices) < 5:
            return

        cur_time, cur_price = prices[-1]

        clustered_prices = []
        for cid in sorted(cluster_extremes):
            t_base, p_min, p_max = cluster_extremes[cid]
            clustered_prices.append((t_base, p_min))
            clustered_prices.append((t_base, p_max))

        atr = atr_cache.get(symbol)
        if not atr:
            return

        ref_time = None
        ref_price = None
        max_delta = 0
        max_delta_price = None
        impulse_found = False

        for t, p in reversed(clustered_prices):
            if cur_time - t > IMPULSE_MAX_LOOKBACK:
                break

            delta = cur_price - p
            delta_abs = abs(delta)
            delta_percent = abs(delta / p) * 100

            if not impulse_found and delta_abs >= ATR_MULTIPLIER * atr and delta_percent >= symbol_threshold:
                impulse_found = True
                ref_price = p
                ref_time = t

            if delta_abs > max_delta:
                max_delta = delta_abs
                max_delta_price = p

        if not impulse_found:
            return

        direction = cur_price - ref_price
        duration = max(cur_time - ref_time, IMPULSE_MIN_LOOKBACK)

        last_alert = last_alert_time.get(symbol, 0)
        if now - last_alert < ANTI_SPAM_PER_SYMBOL or now < self.silence_until:
            return

        self.alert_times.append(now)
        if len([t for t in self.alert_times if now - t <= ANTI_SPAM_BURST_WINDOW]) >= ANTI_SPAM_BURST_COUNT:
            self.silence_until = now + ANTI_SPAM_SILENCE
            Logger.warn("≥5 сигналов за 30 сек — тишина 30 сек")
            return

        change_percent = (max_delta / ref_price) * 100

        return {
            "symbol": symbol,
            "cur": cur_price,
            "ref_price": ref_price,
            "change_percent": round(change_percent, 3),
            "ref_time": ref_time,
            "duration": duration,
            "direction": direction,
            "threshold": symbol_threshold,
            "atr_percent": (atr / cur_price) * 100,
            "max_delta": max_delta,
            "max_delta_price": max_delta_price
        }
    