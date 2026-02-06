from collections import defaultdict, deque

from config import (
    CLUSTER_INTERVAL,
    IMPULSE_MAX_LOOKBACK,
    CANDLE_TIMEFRAME_SEC,
    ATR_PERIOD,
)


class ClusterManager:
    def __init__(self):
        self.cluster_stats = defaultdict(dict)  
        self._pref = {} 

        self.last_tick = {}
        self.current_candle = {} 
        self.candles = defaultdict(lambda: deque(maxlen=ATR_PERIOD))  
        self.atr_cache = {} 

        self._mark_cur = {}  
        self._mark_seg = {}  
        self._mark_segs = defaultdict(lambda: deque()) 

    def _rebuild_prefix(self, symbol: str, base_cid: int, max_cid: int):
        stats = self.cluster_stats.get(symbol, {})
        n = max_cid - base_cid + 1
        tr = [0] * n
        vol = [0.0] * n

        run_tr = 0
        run_vol = 0.0

        for i in range(n):
            cid = base_cid + i
            st = stats.get(cid)
            if st:
                run_tr += st[3]
                run_vol += st[4]
            tr[i] = run_tr
            vol[i] = run_vol

        self._pref[symbol] = {
            "base": base_cid,
            "max": max_cid,
            "tr": tr,
            "vol": vol,
        }

    # ------------------------------
    # Public API
    # ------------------------------
    def add_tick(self, symbol: str, t: float, p: float, q: float):
        # запоминаем последний тик
        self.last_tick[symbol] = (t, p)

        # update mark-segment extremes (last_min/last_max) при фиксированном mark
        seg = self._mark_seg.get(symbol)
        if seg is not None:
            if p < seg["last_min"]:
                seg["last_min"] = p
            if p > seg["last_max"]:
                seg["last_max"] = p

        # 1) update cluster stats (агрегация по cid)
        cid = int(t / CLUSTER_INTERVAL)
        st = self.cluster_stats[symbol].get(cid)
        vol_usdt = p * q

        if st is None:
            # [t_base, p_min, p_max, trades, vol_usdt]
            self.cluster_stats[symbol][cid] = [t, p, p, 1, vol_usdt]
        else:
            # t_base — время первого тика, обычно не меняется,
            # но если вдруг пришёл тик с меньшим t (редко) — подстрахуем
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

        # 3) evict old clusters (по cid)
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
        lt = self.last_tick.get(symbol)
        if lt:
            last_price = lt[1]

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
    
    def get_last_tick(self, symbol: str):
        return self.last_tick.get(symbol)  # (t, price) или None

    def get_atr(self, symbol: str):
        return self.atr_cache.get(symbol)

    def get_impulse_stats(self, symbol: str, ref_time: float, cur_time: float):
        self._evict_old(symbol, cur_time)

        stats = self.cluster_stats.get(symbol, {})
        if not stats:
            return 0, 0.0

        cid_from = int(ref_time / CLUSTER_INTERVAL)
        cid_to   = int(cur_time / CLUSTER_INTERVAL)

        # ✅ база — от реально живых кластеров, а не от "дрожащего" cutoff_time
        base_cid = min(stats.keys())
        max_cid  = cid_to

        if max_cid < base_cid:
            return 0, 0.0

        pref = self._pref.get(symbol)
        if (not pref) or (pref["base"] != base_cid) or (pref["max"] != max_cid):
            self._rebuild_prefix(symbol, base_cid, max_cid)
            pref = self._pref[symbol]

        # clamp диапазона в рамки base..max
        a = max(cid_from, base_cid)
        b = min(cid_to, max_cid)
        if a > b:
            return 0, 0.0

        ia = a - base_cid
        ib = b - base_cid

        tr = pref["tr"]
        vol = pref["vol"]

        right_tr = tr[ib]
        right_vol = vol[ib]

        left_tr = tr[ia - 1] if ia > 0 else 0
        left_vol = vol[ia - 1] if ia > 0 else 0.0

        return right_tr - left_tr, right_vol - left_vol



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
        cutoff_time = cur_time - IMPULSE_MAX_LOOKBACK
        cutoff_cid = int(cutoff_time / CLUSTER_INTERVAL)

        stats = self.cluster_stats[symbol]
        if not stats:
            return

        # удаляем кластеры целиком, которые полностью вне окна
        # (по cid это быстро)
        to_del = [cid for cid in stats.keys() if cid < cutoff_cid]
        if to_del:
            for cid in to_del:
                stats.pop(cid, None)
            self._pref.pop(symbol, None)


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
