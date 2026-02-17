import os
import aiohttp
from dotenv import load_dotenv

load_dotenv()

# --- Telegram ---
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
ADMIN_USER_ID = int(os.getenv("ADMIN_USER_ID") or "0")
FORUM_GROUP_ID = int(os.getenv("FORUM_GROUP_ID") or "0")

# Mapping alert_type -> message_thread_id (populated at startup)
TOPIC_IDS: dict[str, int] = {}

# --- Proxy ---
PROXY_URL = os.getenv("PROXY_URL", "")

# --- Orderbook ---
WALL_THRESHOLD_USD = float(os.getenv("WALL_THRESHOLD_USD") or "500000")
WALL_ALERT_USD = 2_000_000
WALL_CANCEL_ALERT_USD = 1_000_000
CONFIRMED_WALL_THRESHOLD_USD = 5_000_000
CONFIRMED_WALL_MAX_DISTANCE_PCT = 2.0  # max 2% from mid_price
CONFIRMED_WALL_DELAY_SEC = 60  # confirm after 60 seconds

# --- Trades ---
LARGE_TRADE_THRESHOLD_USD = float(os.getenv("LARGE_TRADE_THRESHOLD_USD") or "100000")
LARGE_TRADE_ALERT_USD = 500_000
MEGA_TRADE_ALERT_USD = 2_000_000

# --- Liquidations ---
LIQ_ALERT_USD = 1_000_000

# --- CVD ---
CVD_SPIKE_THRESHOLD_USD = 5_000_000

# --- Imbalance ---
IMBALANCE_ALERT_THRESHOLD = 0.4

# --- Snapshots ---
SNAPSHOT_INTERVAL_SEC = 60
REST_SNAPSHOT_INTERVAL_SEC = 3600
OB_PRUNE_DISTANCE_PCT = 0.5

# --- Aggregation ---
TRADE_AGG_INTERVAL_SEC = 60
CVD_RESET_HOUR_UTC = 0

# --- Alerts ---
ALERT_COOLDOWN_SEC = 300
ALERT_BATCH_THRESHOLD = 3
ALERT_BATCH_WAIT_SEC = 0.3
TELEGRAM_DELAY_SEC = 0.5

# --- WebSocket ---
WS_RECONNECT_DELAY_SEC = 5
WS_RECONNECT_MAX_DELAY_SEC = 300
WS_PING_INTERVAL_SEC = 180
WS_SILENCE_TIMEOUT_SEC = 30
WS_SNAPSHOT_ON_CONNECT = True

# --- Storage ---
ARCHIVE_AFTER_DAYS = 90
DB_VACUUM_INTERVAL_DAYS = 30

# --- Binance URLs ---
FUTURES_WS_URL = "wss://fstream.binance.com/stream?streams=btcusdt@depth@100ms/btcusdt@aggTrade/!forceOrder@arr"
SPOT_WS_URL = "wss://stream.binance.com/stream?streams=btcusdt@depth@100ms/btcusdt@aggTrade"
FUTURES_REST_DEPTH = "https://fapi.binance.com/fapi/v1/depth?symbol=BTCUSDT&limit=1000"
SPOT_REST_DEPTH = "https://api.binance.com/api/v3/depth?symbol=BTCUSDT&limit=5000"

# --- Global HTTP session ---
http_session: aiohttp.ClientSession | None = None


async def init_http(proxy_url: str | None = None):
    global http_session
    if proxy_url:
        from aiohttp_socks import ProxyConnector
        connector = ProxyConnector.from_url(proxy_url)
    else:
        connector = aiohttp.TCPConnector()
    http_session = aiohttp.ClientSession(connector=connector)


async def close_http():
    global http_session
    if http_session:
        await http_session.close()
        http_session = None
