
from __future__ import annotations
from typing import Dict

ADMIN_TELEGRAM_TOKEN = "8549716017:AAEQTa-QugWoakJGJRH-l0Cbcfn8NtcZx1U"
ADMIN_TELEGRAM_CHAT_ID = "6360001973"

# ---------------- Allowed filters (для UI/patch) ----------------
ALLOWED_FILTERS: Dict[str, list] = {
    "volume_threshold": [10_000_000, 50_000_000, 100_000_000, 200_000_000, 500_000_000],
    "min_trades_24h": [10_000, 50_000, 100_000, 200_000],
    "orderbook_min_bid": [20_000, 50_000, 100_000, 200_000],
    "orderbook_min_ask": [20_000, 50_000, 100_000, 200_000],
    "impulse_trades": [100, 500, 1000],
}

# --- Фильтры ---
EXCLUDE_SYMBOLS = set()
# дефолты = первые значения из ALLOWED_FILTERS
VOLUME_THRESHOLD = int(ALLOWED_FILTERS["volume_threshold"][0])
MIN_TRADES = int(ALLOWED_FILTERS["min_trades_24h"][0])
ORDERBOOK_MIN_BID_VOLUME = float(ALLOWED_FILTERS["orderbook_min_bid"][0])
ORDERBOOK_MIN_ASK_VOLUME = float(ALLOWED_FILTERS["orderbook_min_ask"][0])
IMPULSE_TRADES = int(ALLOWED_FILTERS["impulse_trades"][0])
# --- Импульсы ---
ANTI_SPAM_PER_SYMBOL = 180        # сек
ANTI_SPAM_BURST_COUNT = 5         # сигналов
ANTI_SPAM_BURST_WINDOW = 30       # окно сек
ANTI_SPAM_SILENCE = 30 

# ATR
ATR_PERIOD = 14
ATR_MULTIPLIER = 2.2
CANDLE_TIMEFRAME_SEC = 60


# --------------- symbol_fetcher.py ---------------
# --- HTTP ---
HTTP_CONCURRENCY = 5
HTTP_TIMEOUT_SEC = 10

# --- ORDERBOOK ---
ORDERBOOK_DEPTH_PERCENT = 0.02
ORDERBOOK_REQUEST_DELAY = 0.1

# --- DYNAMIC IMPULSE THRESHOLD ---
IMPULSE_VOL_MIN = VOLUME_THRESHOLD
IMPULSE_VOL_MAX = 5_000_000_000
IMPULSE_P_MIN = 0.7
IMPULSE_P_MAX = 2.5
# чем меньшне значение, тем больше импульсов детектится, кривая смещается в сторону меньших объёмов
IMPULSE_EXPONENT = 0.8
ENABLE_DYNAMIC_THRESHOLD = False 
IMPULSE_FIXED_THRESHOLD_PCT = 0.5
# --- API ---
BINANCE_INFO_URL = "https://fapi.binance.com/fapi/v1/exchangeInfo"
BINANCE_24HR_URL = "https://fapi.binance.com/fapi/v1/ticker/24hr"
BINANCE_DEPTH_URL = "https://fapi.binance.com/fapi/v1/depth"

# --------------- notifier.py ---------------
SOUND_FILE = "notice1.wav"
SOUND_VOLUME = 0.5

CLIPBOARD_CONNECT_ATTEMPTS = 5
CLIPBOARD_RETRY_BACKOFF = 2

# thresholds for statistics calculation
STAT_RISE_THRESHOLD = 1.0   # % роста после импульса
STAT_FALL_THRESHOLD = 0.5   # % отката после импульса

ENABLE_ATR_IMPULSE = True

SIGNAL_HUB_HOST = "0.0.0.0"
SIGNAL_HUB_PORT = 9001
SIGNAL_HUB_TOKEN = "Qn8vX5sJp2Kz0mWcR4tY7uAa9eLd1HfG3iP6oBnV"

#NEW
CLUSTER_INTERVAL = 0.05
IMPULSE_MAX_CLUSTERS = 300
IMPULSE_MIN_CLUSTERS = 1