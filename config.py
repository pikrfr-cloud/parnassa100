import os
from dotenv import load_dotenv

load_dotenv()


def _default_data_path(filename: str) -> str:
    override = os.getenv("STATE_DIR")
    if override:
        return os.path.join(override, filename)
    if os.path.isdir("/data") and os.access("/data", os.W_OK):
        return os.path.join("/data", filename)
    return os.path.join(os.getcwd(), "data", filename)


def _bool_env(name: str, default: str = "true") -> bool:
    return os.getenv(name, default).strip().lower() in {"1", "true", "yes", "on"}


class Config:
    # Telegram
    TELEGRAM_BOT_TOKEN: str = os.getenv("TELEGRAM_BOT_TOKEN", "")
    TELEGRAM_CHAT_ID: str = os.getenv("TELEGRAM_CHAT_ID", "")

    # Kalshi
    KALSHI_API_KEY: str = os.getenv("KALSHI_API_KEY", "")
    KALSHI_API_SECRET: str = os.getenv("KALSHI_API_SECRET", "")

    # Bot settings
    CHECK_INTERVAL_MINUTES: int = int(os.getenv("CHECK_INTERVAL_MINUTES", "120"))
    ALERT_THRESHOLD_BPS: int = int(os.getenv("ALERT_THRESHOLD_BPS", "15"))
    LANGUAGES: list[str] = [lang.strip() for lang in os.getenv("LANGUAGES", "en,he,fr").split(",") if lang.strip()]

    # Logging
    LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO")

    # Feature flags
    ENABLE_MARKET_INTEL: bool = _bool_env("ENABLE_MARKET_INTEL", "true")
    ENABLE_PLAYER_INTEL: bool = _bool_env("ENABLE_PLAYER_INTEL", "true")

    # --- Market categories ---
    MARKET_CATEGORIES = ["crypto", "politics", "macro", "sports", "tech", "climate"]

    # --- RSS Feeds ---
    RSS_FEEDS = {
        "central_banks": [
            {"name": "Federal Reserve", "url": "https://www.federalreserve.gov/feeds/press_all.xml", "lang": "en"},
            {"name": "ECB", "url": "https://www.ecb.europa.eu/rss/press.html", "lang": "en"},
            {"name": "Bank of Israel", "url": "https://www.boi.org.il/en/communication-and-publications/press-releases/rss/", "lang": "en"},
        ],
        "news": [
            {"name": "Reuters Business", "url": "https://www.reutersagency.com/feed/", "lang": "en"},
            {"name": "CoinDesk", "url": "https://www.coindesk.com/arc/outboundfeeds/rss/", "lang": "en"},
            {"name": "Politico", "url": "https://rss.politico.com/politics-news.xml", "lang": "en"},
        ],
        "legislation": [
            {"name": "US Congress Bills", "url": "https://www.govinfo.gov/rss/bills.xml", "lang": "en"},
            {"name": "EU Legislation", "url": "https://eur-lex.europa.eu/EN/display-feed.html", "lang": "en"},
        ],
    }

    # --- Polymarket API (public, no auth) ---
    POLYMARKET_API_BASE = os.getenv("POLYMARKET_API_BASE", "https://gamma-api.polymarket.com")
    POLYMARKET_DATA_API = os.getenv("POLYMARKET_DATA_API", "https://data-api.polymarket.com")
    # CLOB URL is recorded for market-data reference only. This bot never places orders.
    POLYMARKET_CLOB_BASE = os.getenv("POLYMARKET_CLOB_BASE", "https://clob.polymarket.com")

    # --- Kalshi API ---
    KALSHI_API_BASE = "https://api.elections.kalshi.com/trade-api/v2"

    # --- Player intel (watchlist of public proxy wallets) ---
    POLYMARKET_WATCHLIST: str = os.getenv("POLYMARKET_WATCHLIST", "")
    WATCHLIST_FILE: str = os.getenv("WATCHLIST_FILE", "")
    PLAYER_NOTABLE_USD: float = float(os.getenv("PLAYER_NOTABLE_USD", "500"))
    PLAYER_LEADERBOARD_LIMIT: int = int(os.getenv("PLAYER_LEADERBOARD_LIMIT", "25"))
    PLAYER_LEADERBOARD_PERIOD: str = os.getenv("PLAYER_LEADERBOARD_PERIOD", "DAY")
    PLAYER_LEADERBOARD_UNUSUAL_USD: float = float(os.getenv("PLAYER_LEADERBOARD_UNUSUAL_USD", "25000"))
    PLAYER_POSITION_LIMIT: int = int(os.getenv("PLAYER_POSITION_LIMIT", "100"))
    PLAYER_TRADE_LIMIT: int = int(os.getenv("PLAYER_TRADE_LIMIT", "50"))

    # --- Paper ledger (simulated fills only) ---
    PAPER_MAX_USD: float = float(os.getenv("PAPER_MAX_USD", "100"))
    PAPER_COPY_RATIO: float = float(os.getenv("PAPER_COPY_RATIO", "1.0"))
    PAPER_LEDGER_FILE: str = os.getenv("PAPER_LEDGER_FILE", "") or _default_data_path("paper_ledger.json")

    # --- State file for persistence ---
    STATE_FILE = os.getenv("STATE_FILE", "") or _default_data_path("bot_state.json")
