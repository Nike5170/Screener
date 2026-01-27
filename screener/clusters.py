# screener/clusters.py
from collections import defaultdict, deque

from config import (
    CLUSTER_INTERVAL,
    IMPULSE_MAX_LOOKBACK,
    CANDLE_TIMEFRAME_SEC,
    ATR_PERIOD,
)


class ClusterManager:
    """
    Единый "тик-стор" на символ:
      - ticks: deque[(t, p, q)] только в окне IMPULSE_MAX_LOOKBACK
      - cluster_stats: cid -> [t_base, p_min, p_max, trades, vol_usdt]
      - 1m свечи -> ATR (кеш atr_cache)

    Важно: мы не держим отдельные price_history / volume_history.
    """

    def __init__(self):
        self.ticks = defaultdict(lambda: deque())          # symbol -> deque[(t,p,q)]
        self.cluster_stats = defaultdict(dict)             # symbol -> {cid: [t_base,p_min,p_max,trades,vol_usdt]}

        # ATR via 1m candles
        self.current_candle = {}                           # symbol -> {"minute", "open","high","low","close"}
        self.candles = defaultdict(lambda: deque(maxlen=ATR_PERIOD))  # symbol -> deque[candle]
        self.atr_cache = {}                                # symbol -> float

    # ------------------------------
    # Public API
    # ------------------------------
    def add_tick(self, symbol: str, t: float, p: float, q: float):
        dq = self.ticks[symbol]
        dq.append((t, p, q))

        # 1) update cluster stats
        cid = int(t / CLUSTER_INTERVAL)
        st = self.cluster_stats[symbol].get(cid)
        vol_usdt = p * q

        if st is None:
            # [t_base, p_min, p_max, trades, vol_usdt]
            self.cluster_stats[symbol][cid] = [t, p, p, 1, vol_usdt]
        else:
            if t < st[0]:
                st[0] = t
            if p < st[1]:
                st[1] = p
            if p > st[2]:
                st[2] = p
            st[3] += 1
            st[4] += vol_usdt

        # 2) update candle + maybe ATR on close
        self._update_candle_and_atr(symbol, t, p)

        # 3) evict old ticks
        self._evict_old(symbol, t)

    def get_extremes(self, symbol: str, cur_time: float):
        """
        Вернуть dict cid -> [t_base, p_min, p_max] по текущему окну.
        """
        self._evict_old(symbol, cur_time)
        stats = self.cluster_stats.get(symbol, {})
        return {cid: [st[0], st[1], st[2]] for cid, st in stats.items()}

    def get_atr(self, symbol: str):
        return self.atr_cache.get(symbol)

    def get_impulse_stats(self, symbol: str, ref_time: float, cur_time: float):
        """
        Быстро посчитать trades + vol_usdt за импульс по тикам (одно окно).
        """
        dq = self.ticks.get(symbol)
        if not dq:
            return 0, 0.0

        trades = 0
        vol_usdt = 0.0
        for t, p, q in dq:
            if t < ref_time:
                continue
            if t > cur_time:
                break
            trades += 1
            vol_usdt += p * q
        return trades, vol_usdt

    # ------------------------------
    # Internal helpers
    # ------------------------------
    def _update_candle_and_atr(self, symbol: str, ts: float, price: float):
        minute = int(ts // CANDLE_TIMEFRAME_SEC)

        if symbol not in self.current_candle or self.current_candle[symbol]["minute"] != minute:
            # close previous candle
            if symbol in self.current_candle:
                closed = self.current_candle[symbol]
                self.candles[symbol].append(closed)
                self._recompute_atr(symbol)

            # open new candle
            self.current_candle[symbol] = {
                "minute": minute,
                "open": price,
                "high": price,
                "low": price,
                "close": price,
            }
        else:
            c = self.current_candle[symbol]
            c["high"] = max(c["high"], price)
            c["low"] = min(c["low"], price)
            c["close"] = price

    def _recompute_atr(self, symbol: str):
        cs = self.candles[symbol]
        if len(cs) < 2:
            return

        prev_close = None
        tr_list = []

        for candle in cs:
            high = candle["high"]
            low = candle["low"]
            close = candle["close"]

            if prev_close is None:
                tr = high - low
            else:
                tr = max(
                    high - low,
                    abs(high - prev_close),
                    abs(low - prev_close),
                )

            tr_list.append(tr)
            prev_close = close

        self.atr_cache[symbol] = sum(tr_list) / len(tr_list)

    def _evict_old(self, symbol: str, cur_time: float):
        cutoff = cur_time - IMPULSE_MAX_LOOKBACK
        dq = self.ticks[symbol]
        stats = self.cluster_stats[symbol]

        dirty_cids = set()

        while dq and dq[0][0] < cutoff:
            t_old, p_old, q_old = dq.popleft()
            cid_old = int(t_old / CLUSTER_INTERVAL)

            st = stats.get(cid_old)
            if st is None:
                continue

            # вычитаем trades/vol инкрементально
            st[3] -= 1
            st[4] -= (p_old * q_old)

            # если затронули экстремум/базовое время — пометим кластер на пересчёт
            if t_old == st[0] or p_old == st[1] or p_old == st[2]:
                dirty_cids.add(cid_old)

            # если trades ушли в 0 — кластер пустой
            if st[3] <= 0:
                stats.pop(cid_old, None)

        # пересчитываем только грязные кластера по текущему dq
        for cid in dirty_cids:
            self._recompute_cluster(symbol, cid)

    def _recompute_cluster(self, symbol: str, cid: int):
        dq = self.ticks[symbol]
        stats = self.cluster_stats[symbol]

        t_base = None
        p_min = None
        p_max = None
        trades = 0
        vol_usdt = 0.0

        for t, p, q in dq:
            if int(t / CLUSTER_INTERVAL) != cid:
                continue

            if t_base is None or t < t_base:
                t_base = t
            if p_min is None or p < p_min:
                p_min = p
            if p_max is None or p > p_max:
                p_max = p

            trades += 1
            vol_usdt += p * q

        if trades == 0:
            stats.pop(cid, None)
        else:
            stats[cid] = [t_base, p_min, p_max, trades, vol_usdt]
