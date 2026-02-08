# screener/clusters.py
from __future__ import annotations
from dataclasses import dataclass
from collections import defaultdict, deque
from typing import Dict, List, Optional, Tuple

from config import CLUSTER_INTERVAL, IMPULSE_MAX_CLUSTERS, CANDLE_TIMEFRAME_SEC, ATR_PERIOD


class Cluster:
    __slots__ = ("cid", "p_min", "p_max", "volume", "trades")

    def __init__(self):
        self.cid = -1
        self.p_min = 0.0
        self.p_max = 0.0
        self.volume = 0.0
        self.trades = 0

    def reset(self, cid: int, price_seed: float):
        self.cid = cid
        self.p_min = price_seed
        self.p_max = price_seed
        self.volume = 0.0
        self.trades = 0

    def update(self, price: float, qty: float):
        if price < self.p_min:
            self.p_min = price
        if price > self.p_max:
            self.p_max = price
        self.volume += price * qty
        self.trades += 1


@dataclass
class _SymState:
    ring: List[Cluster]
    last_cid: int = -1
    last_price: float = 0.0  # последняя известная цена (для заполнения пустых кластеров)


class ClusterManager:
    """
    Цель:
      - add_tick(): O(1) апдейт текущего кластера
      - при смене cid: "закрываем" прошлые кластера и возвращаем список finalized cid
      - пустые интервалы заполняем кластерами trades=0, volume=0, p_min=p_max=last_price
      - ATR считаем НЕ по тикам, а по закрытым кластерам (дешево)
    """

    def __init__(self):
        self._sym: Dict[str, _SymState] = {}
        # ATR state
        self._cur_candle: Dict[str, dict] = {}
        self._candles: Dict[str, deque] = defaultdict(lambda: deque(maxlen=ATR_PERIOD))
        self._atr: Dict[str, float] = {}

    def _get_state(self, symbol: str) -> _SymState:
        st = self._sym.get(symbol)
        if st is None:
            st = _SymState(ring=[Cluster() for _ in range(IMPULSE_MAX_CLUSTERS)])
            self._sym[symbol] = st
        return st

    def add_tick(self, symbol: str, ts: float, price: float, qty: float) -> List[int]:
        """
        Возвращает список cid, которые стали "полностью готовыми" (закрытыми),
        т.е. можно запускать детект/ATR обновления по ним.
        """
        st = self._get_state(symbol)
        st.last_price = price

        cid = int(ts / CLUSTER_INTERVAL)
        finalized: List[int] = []

        if st.last_cid == -1:
            # первый тик — просто открываем текущий cid
            self._ring_reset(symbol, st, cid, price_seed=price)
            st.last_cid = cid

        elif cid > st.last_cid:
            # закрываем все кластера между last_cid..cid-1
            # и заполняем пропуски пустыми кластерами
            for fc in range(st.last_cid, cid):
                if fc == st.last_cid:
                    # last_cid уже существует, просто считаем его "закрытым"
                    finalized.append(fc)
                else:
                    # пропущенный кластер (нет трейдов) — создаём пустой
                    self._ring_reset(symbol, st, fc, price_seed=st.last_price)
                    finalized.append(fc)

            # открываем новый текущий cid
            self._ring_reset(symbol, st, cid, price_seed=st.last_price)
            st.last_cid = cid

        # обновляем текущий кластер (cid == st.last_cid)
        idx = cid % IMPULSE_MAX_CLUSTERS
        c = st.ring[idx]
        if c.cid != cid:
            # на всякий случай (редко)
            c.reset(cid, st.last_price)
        c.update(price, qty)

        return finalized

    def _ring_reset(self, symbol: str, st: _SymState, cid: int, price_seed: float):
        idx = cid % IMPULSE_MAX_CLUSTERS
        st.ring[idx].reset(cid, price_seed)
        # ATR обновляем по кластеру при его закрытии отдельным вызовом on_cluster_close()

    def get_cluster(self, symbol: str, cid: int) -> Optional[Cluster]:
        st = self._sym.get(symbol)
        if not st:
            return None
        c = st.ring[cid % IMPULSE_MAX_CLUSTERS]
        if c.cid != cid:
            return None
        return c

    def iter_recent(self, symbol: str, from_cid: int, max_clusters: int):
        """
        Итерация назад: from_cid, from_cid-1, ...
        Останавливаемся при первом "дыре" (когда cid не совпадает).
        """
        st = self._sym.get(symbol)
        if not st:
            return
        for i in range(max_clusters):
            cid = from_cid - i
            c = st.ring[cid % IMPULSE_MAX_CLUSTERS]
            if c.cid != cid:
                break
            yield c

    # ---------------- ATR (по закрытым кластерам) ----------------

    def on_cluster_close(self, symbol: str, cid: int, close_ts: float):
        c = self.get_cluster(symbol, cid)
        if not c:
            return

        bucket = int(close_ts // CANDLE_TIMEFRAME_SEC)

        cc = self._cur_candle.get(symbol)
        if cc is None or cc["bucket"] != bucket:
            # закрываем предыдущую свечу
            if cc is not None:
                self._candles[symbol].append(cc)
                self._recompute_atr(symbol)

            # открываем новую (только high/low)
            self._cur_candle[symbol] = {
                "bucket": bucket,
                "high": c.p_max,
                "low":  c.p_min,
            }
        else:
            cc["high"] = max(cc["high"], c.p_max)
            cc["low"]  = min(cc["low"],  c.p_min)


    def _recompute_atr(self, symbol: str):
        cs = self._candles[symbol]
        if not cs:
            return

        tr_sum = 0.0
        n = 0
        for candle in cs:
            tr_sum += (candle["high"] - candle["low"])
            n += 1

        if n:
            self._atr[symbol] = tr_sum / n


    def get_atr(self, symbol: str) -> Optional[float]:
        return self._atr.get(symbol)
    
    def get_last_price(self, symbol: str) -> float | None:
        st = self._sym.get(symbol)
        if not st:
            return None
        return st.last_price

