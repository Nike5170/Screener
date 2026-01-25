# screener/clusters.py
from collections import defaultdict, deque
from config import CLUSTER_INTERVAL, IMPULSE_MAX_LOOKBACK

class ClusterManager:
    """
    Хранит тики только в окне IMPULSE_MAX_LOOKBACK и поддерживает экстремумы кластеров инкрементально.
    Возвращает cluster_extremes: cid -> [t_base, p_min, p_max]
    """
    def __init__(self):
        self.ticks = defaultdict(lambda: deque())  # symbol -> deque[(t, p)]
        self.cluster_extremes = defaultdict(dict)  # symbol -> {cid: [t_base, p_min, p_max]}

    def add_tick(self, symbol: str, t: float, p: float):
        dq = self.ticks[symbol]
        dq.append((t, p))

        # обновляем кластер для этого тика
        cid = int(t / CLUSTER_INTERVAL)
        ext = self.cluster_extremes[symbol].get(cid)

        if ext is None:
            self.cluster_extremes[symbol][cid] = [t, p, p]
        else:
            t_base, p_min, p_max = ext
            # t_base = самый ранний t в кластере (внутри окна)
            if t < t_base:
                ext[0] = t
            if p < p_min:
                ext[1] = p
            if p > p_max:
                ext[2] = p

        # выкидываем старые тики и чистим затронутые кластеры
        self._evict_old(symbol, t)

    def get_extremes(self, symbol: str, cur_time: float):
        """
        Вернуть dict cid->[t_base,p_min,p_max] только по окну.
        """
        self._evict_old(symbol, cur_time)
        return self.cluster_extremes.get(symbol, {})

    def _evict_old(self, symbol: str, cur_time: float):
        cutoff = cur_time - IMPULSE_MAX_LOOKBACK
        dq = self.ticks[symbol]
        extremes = self.cluster_extremes[symbol]

        # какие кластеры надо будет пересчитать (если удалённый тик мог быть min/max)
        dirty_cids = set()

        while dq and dq[0][0] < cutoff:
            t_old, p_old = dq.popleft()
            cid_old = int(t_old / CLUSTER_INTERVAL)

            ext = extremes.get(cid_old)
            if ext is None:
                continue

            # если удаляемый тик мог быть значимым для экстремумов — пометим кластер
            if p_old == ext[1] or p_old == ext[2] or t_old == ext[0]:
                dirty_cids.add(cid_old)

        # пересчитываем только грязные кластеры по текущему deque (в окне)
        if dirty_cids:
            for cid in dirty_cids:
                self._recompute_cluster(symbol, cid)

        # удаляем пустые/вышедшие кластеры (опционально, но полезно)
        # если в окне больше нет тиков данного cid, _recompute_cluster его удалит

    def _recompute_cluster(self, symbol: str, cid: int):
        dq = self.ticks[symbol]
        extremes = self.cluster_extremes[symbol]

        t_base = None
        p_min = None
        p_max = None
        found = False

        for t, p in dq:
            if int(t / CLUSTER_INTERVAL) != cid:
                continue
            if not found:
                found = True
                t_base = t
                p_min = p
                p_max = p
            else:
                if t < t_base:
                    t_base = t
                if p < p_min:
                    p_min = p
                if p > p_max:
                    p_max = p

        if not found:
            extremes.pop(cid, None)
        else:
            extremes[cid] = [t_base, p_min, p_max]
