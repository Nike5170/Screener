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

                # --- Mark сегменты (sample-and-hold) ---
        self._mark_cur = {}  # symbol -> current mark float
        self._mark_seg = {}  # symbol -> {"start","mark","last_min","last_max"}
        self._mark_segs = defaultdict(lambda: deque())  # symbol -> deque[seg]


    # ------------------------------
    # Public API
    # ------------------------------
    def add_tick(self, symbol: str, t: float, p: float, q: float):
        dq = self.ticks[symbol]
        dq.append((t, p, q))

        # update mark-segment extremes (last_min/last_max) при фиксированном mark
        seg = self._mark_seg.get(symbol)
        if seg is not None:
            if p < seg["last_min"]:
                seg["last_min"] = p
            if p > seg["last_max"]:
                seg["last_max"] = p

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

    def add_mark(self, symbol: str, t: float, mark: float):
        # дедуп: если mark не изменился — игнор
        prev = self._mark_cur.get(symbol)
        if prev is not None and prev == mark:
            return

        # закрываем предыдущий сегмент
        old = self._mark_seg.get(symbol)
        if old is not None:
            old["end"] = t
            self._mark_segs[symbol].append(old)

        # старт нового сегмента: mark фиксируется,
        # last_min/last_max инициализируем последним last (если есть)
        last_price = None
        dq = self.ticks.get(symbol)
        if dq:
            last_price = dq[-1][1]

        if last_price is None:
            last_price = mark  # fallback

        self._mark_cur[symbol] = mark
        self._mark_seg[symbol] = {
            "start": t,
            "end": None,          # open segment
            "mark": mark,
            "last_min": last_price,
            "last_max": last_price,
        }

        # чистим старые сегменты по окну
        self._evict_mark_old(symbol, t)

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

    def get_mark_last_delta_extreme(self, symbol: str, ref_time: float, cur_time: float):
        """
        Возвращает:
          - delta_pct_signed: экстремальная по модулю signed Δ% = (mark-last)/last*100
          - mark_updates: сколько раз обновлялся mark в окне (кол-во сегментов, пересекающих окно)
        """
        # актуализируем чистку
        self._evict_mark_old(symbol, cur_time)

        segs = self._mark_segs.get(symbol)
        cur = self._mark_seg.get(symbol)

        if (not segs or len(segs) == 0) and cur is None:
            return None

        def overlap(a_start, a_end, b_start, b_end):
            if a_end is None:
                a_end = b_end
            return not (a_end < b_start or a_start > b_end)

        best = None  # {"delta": signed, "abs": abs, "mark_updates": int}
        mark_updates = 0

        def consider(seg):
            nonlocal best, mark_updates
            if not overlap(seg["start"], seg.get("end"), ref_time, cur_time):
                return

            mark_updates += 1

            m = seg["mark"]
            last_min = seg["last_min"]
            last_max = seg["last_max"]

            # две стороны экстремума
            d_min = None
            if last_min:
                d_min = (m - last_min) / last_min * 100.0  # signed
            d_max = None
            if last_max:
                d_max = (m - last_max) / last_max * 100.0  # signed

            # выбираем экстремум по модулю
            cand = []
            if d_min is not None:
                cand.append(d_min)
            if d_max is not None:
                cand.append(d_max)
            if not cand:
                return

            d = max(cand, key=lambda x: abs(x))
            if best is None or abs(d) > best["abs"]:
                best = {"delta": d, "abs": abs(d)}

        # закрытые сегменты
        if segs:
            for s in segs:
                consider(s)

        # текущий открытый
        if cur is not None:
            consider(cur)

        if best is None:
            return None

        best["mark_updates"] = mark_updates
        return best

    def _evict_mark_old(self, symbol: str, cur_time: float):
        cutoff = cur_time - IMPULSE_MAX_LOOKBACK
        dq = self._mark_segs[symbol]

        # выкидываем сегменты, которые целиком закончились до cutoff
        while dq and dq[0].get("end") is not None and dq[0]["end"] < cutoff:
            dq.popleft()
