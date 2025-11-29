import time
from collections import defaultdict, deque


class VolumeCalculator:
    def __init__(self):
        self.volume_first_seen = {}
        self.volume_last_update = {}
        self.volume_avg_cache = {}
        self.volume_avg_ready = {}
        self.volume_alert_times = deque(maxlen=50)

    async def update_volume_average(self, symbol, volume_history):
        now = time.time()
        last_update = self.volume_last_update.get(symbol, 0)
        if now - last_update < 60:
            return

        self.volume_last_update[symbol] = now
        cutoff = now - 14 * 60
        history = volume_history[symbol]

        while history and history[0][0] < cutoff:
            history.popleft()

        intervals = defaultdict(float)
        for t, q in history:
            minute = int(t) // 60
            intervals[minute] += q

        last_14 = [int(now) // 60 - i for i in reversed(range(14))]
        arr = [intervals.get(m, 0) for m in last_14]
        avg = sum(arr) / 14 if arr else 0

        self.volume_avg_cache[symbol] = avg
        self.volume_avg_ready[symbol] = True
