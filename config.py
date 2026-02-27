import os
from dotenv import load_dotenv

load_dotenv()


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
    LANGUAGES: list[str] = os.getenv("LANGUAGES", "en,he,fr").split(",")

    # Logging
    LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO")

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

    # --- Polymarket API ---
    POLYMARKET_API_BASE = "https://gamma-api.polymarket.com"
    POLYMARKET_CLOB_BASE = "https://clob.polymarket.com"

    # --- Kalshi API ---
    KALSHI_API_BASE = "https://api.elections.kalshi.com/trade-api/v2"

    # --- State file for persistence ---
    STATE_FILE = "/data/bot_state.json"
