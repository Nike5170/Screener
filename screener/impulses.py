
# screener/impulses.py
import time
from config import (
    IMPULSE_MAX_CLUSTERS,
    IMPULSE_MIN_CLUSTERS,
    ATR_MULTIPLIER,
    IMPULSE_MIN_TRADES,
    ENABLE_MARK_DELTA,
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

    async def check_on_cluster(
        self,
        symbol: str,
        last_closed_cid: int,
        cluster_mgr,
        last_alert_time: dict,
        symbol_threshold: float,
    ):
        now = time.time()

        # текущая цена = последняя известная (last_price из ClusterManager state)
        cur_price = cluster_mgr.get_last_price(symbol)
        if not cur_price:
            return


        atr = cluster_mgr.get_atr(symbol)
        if not atr:
            return

        # идём назад по кластерам
        ref_cid = None
        ref_price = None

        max_delta = 0.0
        max_delta_price = None
        max_delta_cid = None

        clusters_checked = 0

        for c in cluster_mgr.iter_recent(symbol, last_closed_cid, IMPULSE_MAX_CLUSTERS):
            clusters_checked += 1

            # берём обе стороны экстремума кластера
            for p in (c.p_min, c.p_max):
                if p <= 0:
                    continue
                delta = cur_price - p
                da = abs(delta)
                dp = abs(delta / p) * 100.0

                if da > max_delta:
                    max_delta = da
                    max_delta_price = p
                    max_delta_cid = c.cid

                if ref_cid is None:
                    # первый момент, когда выполняются условия
                    if (clusters_checked >= IMPULSE_MIN_CLUSTERS) and (da >= ATR_MULTIPLIER * atr) and (dp >= symbol_threshold):
                        ref_cid = c.cid
                        ref_price = p

        if ref_cid is None or ref_price is None:
            return

        # trades/volume по диапазону cid (без отдельного prefix-массива — просто суммируем 300 кластеров, это редко и дёшево)
        # (Если захочешь ещё быстрее — сделаем prefix-sum по ring на закрытии кластеров)
        tr_sum = 0
        vol_sum = 0.0
        for c in cluster_mgr.iter_recent(symbol, last_closed_cid, last_closed_cid - ref_cid + 1):
            tr_sum += int(c.trades)
            vol_sum += float(c.volume)

        if tr_sum < IMPULSE_MIN_TRADES:
            return

        mark_delta_pct = None
        mark_extreme = None
        if ENABLE_MARK_DELTA:
            # ref_time/cur_time грубо восстанавливаем по cid
            ref_time = ref_cid * cluster_mgr_interval()
            cur_time = (last_closed_cid + 1) * cluster_mgr_interval()
            mark_extreme = cluster_mgr.get_mark_last_delta_extreme(symbol, ref_time, cur_time)
            if mark_extreme:
                mark_delta_pct = mark_extreme["delta"]

        # антиспам
        last_alert = last_alert_time.get(symbol, 0)
        if (now - last_alert) < ANTI_SPAM_PER_SYMBOL or now < self.silence_until:
            return

        self.alert_times.append(now)
        self.alert_times = [t for t in self.alert_times if now - t <= ANTI_SPAM_BURST_WINDOW]
        if len(self.alert_times) >= ANTI_SPAM_BURST_COUNT:
            self.silence_until = now + ANTI_SPAM_SILENCE
            Logger.warn("≥5 сигналов за 30 сек — тишина 30 сек")
            return

        last_alert_time[symbol] = now

        # метрики
        change_percent_from_start = abs(cur_price - ref_price) / ref_price * 100.0
        change_percent_max_delta = (max_delta / ref_price) * 100.0
        atr_from_start = abs(cur_price - ref_price) / atr
        atr_max_delta = max_delta / atr

        reason = ["atr", "threshold", "trades"]
        if ENABLE_MARK_DELTA and (mark_delta_pct is not None):
            reason.append("mark_delta")

        return {
            "symbol": symbol,
            "trigger_price": cur_price,
            "ref_price": ref_price,
            "max_delta_price": max_delta_price,
            "change_percent_from_start": round(change_percent_from_start, 3),
            "change_percent_max_delta": round(change_percent_max_delta, 3),
            "atr_impulse": round(atr_from_start, 3),  # текущая амплитуда
            "atr_from_start": round(atr_from_start, 3),
            "atr_max_delta": round(atr_max_delta, 3),
            "change_percent": round(change_percent_max_delta, 3),
            "impulse_trades": tr_sum,
            "impulse_volume_usdt": vol_sum,
            "mark_delta_pct": round(mark_delta_pct, 3) if mark_delta_pct is not None else None,
            "mark_extreme": mark_extreme,
            "reason": reason,
        }


def cluster_mgr_interval() -> float:
    # локально, чтобы не тащить config в hotpath
    from config import CLUSTER_INTERVAL
    return float(CLUSTER_INTERVAL)
