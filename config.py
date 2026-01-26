# config.py

TELEGRAM_TOKEN = "8549716017:AAEQTa-QugWoakJGJRH-l0Cbcfn8NtcZx1U"

TELEGRAM_CHAT_ID = "6360001973"

# --- Фильтры ---
EXCLUDE_SYMBOLS = {}
#"ETHUSDT", "BTCUSDT", "SOLUSDT"
VOLUME_THRESHOLD = 20_000_000
MIN_TRADES = 10_000

# --- Импульсы ---
PRICE_HISTORY_MAXLEN = 8000
VOLUME_HISTORY_MAXLEN = 8000
IMPULSE_MAX_LOOKBACK = 15
IMPULSE_MIN_LOOKBACK = 0.05

ANTI_SPAM_PER_SYMBOL = 180        # сек
ANTI_SPAM_BURST_COUNT = 5         # сигналов
ANTI_SPAM_BURST_WINDOW = 30       # окно сек
ANTI_SPAM_SILENCE = 30 

CLUSTER_INTERVAL = 0.1     # секунды

# ATR
ATR_PERIOD = 14
ATR_MULTIPLIER = 2.2
CANDLE_TIMEFRAME_SEC = 60


# --------------- symbol_fetcher.py ---------------
# --- HTTP ---
HTTP_CONCURRENCY = 5
HTTP_TIMEOUT_SEC = 10

# --- ORDERBOOK ---
ORDERBOOK_MIN_BID_VOLUME = 50_000
ORDERBOOK_MIN_ASK_VOLUME = 50_000
ORDERBOOK_DEPTH_PERCENT = 0.02
ORDERBOOK_REQUEST_DELAY = 1.0

# --- DYNAMIC IMPULSE THRESHOLD ---
IMPULSE_VOL_MIN = 30_000_000
IMPULSE_VOL_MAX = 5_000_000_000
IMPULSE_P_MIN = 0.7
IMPULSE_P_MAX = 2
# чем меньшне значение, тем больше импульсов детектится, кривая смещается в сторону меньших объёмов
IMPULSE_EXPONENT = 0.7

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

IMPULSE_MIN_TRADES = 1000

MARK_DELTA_PCT = 3.0
ENABLE_ATR_IMPULSE = True
ENABLE_MARK_DELTA = True

SIGNAL_HUB_HOST = "0.0.0.0"
SIGNAL_HUB_PORT = 9001
SIGNAL_HUB_TOKEN = "Qn8vX5sJp2Kz0mWcR4tY7uAa9eLd1HfG3iP6oBnV"
