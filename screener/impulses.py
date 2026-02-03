# screener/impulses.py
import time

from config import (
    IMPULSE_MAX_LOOKBACK,
    IMPULSE_MIN_LOOKBACK,
    ATR_MULTIPLIER,
    ANTI_SPAM_PER_SYMBOL,
    ANTI_SPAM_BURST_COUNT,
    ANTI_SPAM_BURST_WINDOW,
    ANTI_SPAM_SILENCE,
    IMPULSE_MIN_TRADES,
    ENABLE_MARK_DELTA,
    MARK_DELTA_PCT,
)
from logger import Logger


class ImpulseDetector:
    def __init__(self):
        self.alert_times = []
        self.silence_until = 0

# внутри screener/impulses.py

    async def check_atr_impulse(
        self,
        symbol,
        cluster_mgr,
        last_alert_time,
        symbol_threshold,
        last_price_map=None,
        mark_price_map=None,
        **kwargs,   # чтобы "лишние" параметры не ломали вызов
    ):
        now = time.time()

        lt = cluster_mgr.get_last_tick(symbol)
        if not lt:
            return

        cur_time, cur_price = lt


        cluster_extremes = cluster_mgr.get_extremes(symbol, cur_time)
        if len(cluster_extremes) < 3:
            return

        clustered_prices = []
        for cid in sorted(cluster_extremes):
            t_base, p_min, p_max = cluster_extremes[cid]
            clustered_prices.append((t_base, p_min))
            clustered_prices.append((t_base, p_max))

        atr = cluster_mgr.get_atr(symbol)
        if not atr:
            return

        ref_time = None
        ref_price = None
        max_delta = 0.0
        max_delta_price = None
        impulse_found = False

        # 1) базовый детект: ATR + threshold
        for t, p in reversed(clustered_prices):
            if cur_time - t > IMPULSE_MAX_LOOKBACK:
                break

            delta = cur_price - p
            delta_abs = abs(delta)
            delta_percent = abs(delta / p) * 100.0

            if (not impulse_found) and (delta_abs >= ATR_MULTIPLIER * atr) and (delta_percent >= symbol_threshold):
                impulse_found = True
                ref_price = p
                ref_time = t

            if delta_abs > max_delta:
                max_delta = delta_abs
                max_delta_price = p

        if not impulse_found or ref_time is None or ref_price is None:
            return

        # 2) доп-проверки: trades + volume (из cluster_mgr)
        impulse_trades, impulse_volume_usdt = cluster_mgr.get_impulse_stats(symbol, ref_time, cur_time)
        if impulse_trades < IMPULSE_MIN_TRADES:
            return

        mark_delta_pct = None
        mark_extreme = None

        if ENABLE_MARK_DELTA:
            # считаем, но НЕ режем импульс глобально
            mark_extreme = cluster_mgr.get_mark_last_delta_extreme(symbol, ref_time, cur_time)
            if mark_extreme:
                mark_delta_pct = mark_extreme["delta"]  # signed




        # 3) антиспам — только если все условия прошли
        last_alert = last_alert_time.get(symbol, 0)
        if now - last_alert < ANTI_SPAM_PER_SYMBOL or now < self.silence_until:
            return

        self.alert_times.append(now)
        burst = [t for t in self.alert_times if now - t <= ANTI_SPAM_BURST_WINDOW]
        if len(burst) >= ANTI_SPAM_BURST_COUNT:
            self.silence_until = now + ANTI_SPAM_SILENCE
            Logger.warn("≥5 сигналов за 30 сек — тишина 30 сек")
            return

        direction = cur_price - ref_price
        duration = max(cur_time - ref_time, IMPULSE_MIN_LOOKBACK)
        change_percent = (max_delta / ref_price) * 100.0

        reason = ["atr", "threshold", "trades"]
        if ENABLE_MARK_DELTA and (mark_delta_pct is not None):
            reason.append("mark_delta")


        return {
            "symbol": symbol,
            "cur": cur_price,
            "ref_price": ref_price,
            "change_percent": round(change_percent, 3),
            "ref_time": ref_time,
            "duration": duration,
            "direction": direction,
            "threshold": symbol_threshold,
            "atr_percent": (atr / cur_price) * 100.0,
            "max_delta": max_delta,
            "max_delta_price": max_delta_price,
            "impulse_trades": impulse_trades,
            "impulse_volume_usdt": impulse_volume_usdt,
            "mark_delta_pct": round(mark_delta_pct, 3) if mark_delta_pct is not None else None,
            "mark_extreme": mark_extreme,  # dict: {"delta","abs","mark_updates"} или None
            "reason": reason,
        }
