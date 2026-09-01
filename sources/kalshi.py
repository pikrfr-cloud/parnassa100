"""Kalshi data fetcher using public API."""

import logging
from dataclasses import dataclass
from typing import Optional

import aiohttp

from config import Config

logger = logging.getLogger(__name__)


@dataclass
class KalshiMarket:
    id: str
    ticker: str
    title: str
    category: str
    yes_price: float  # 0-100 cents → we normalize to 0-1
    volume: int
    url: str
    event_ticker: Optional[str] = None
    subtitle: Optional[str] = None


# Map Kalshi categories to our internal categories
KALSHI_CATEGORY_MAP = {
    "Politics": "politics",
    "Economics": "macro",
    "Crypto": "crypto",
    "Climate and Weather": "climate",
    "Tech and Science": "tech",
    "Sports": "sports",
    "Finance": "macro",
}


async def fetch_active_markets(
    session: aiohttp.ClientSession,
    limit: int = 200,
    min_volume: int = 100,
) -> list[KalshiMarket]:
    """Fetch active markets from Kalshi API v2."""
    markets = []
    url = f"{Config.KALSHI_API_BASE}/markets"
    params = {
        "limit": limit,
        "status": "open",
    }
    headers = {"Accept": "application/json"}

    # Add auth if available
    if Config.KALSHI_API_KEY:
        headers["Authorization"] = f"Bearer {Config.KALSHI_API_KEY}"

    try:
        async with session.get(url, params=params, headers=headers,
                               timeout=aiohttp.ClientTimeout(total=30)) as resp:
            if resp.status != 200:
                logger.error(f"Kalshi API returned {resp.status}")
                # Try the public endpoint fallback
                return await _fetch_public_fallback(session, limit, min_volume)
            data = await resp.json()

        for m in data.get("markets", []):
            vol = m.get("volume", 0) or 0
            if vol < min_volume:
                continue

            # Kalshi prices are in cents (0-100)
            yes_price_cents = m.get("yes_ask", 0) or m.get("last_price", 0) or 0
            yes_price = yes_price_cents / 100.0  # normalize to 0-1

            category_raw = m.get("category", "Other")
            category = KALSHI_CATEGORY_MAP.get(category_raw, "other")

            ticker = m.get("ticker", "")
            markets.append(KalshiMarket(
                id=str(m.get("id", "")),
                ticker=ticker,
                title=m.get("title", "Unknown"),
                category=category,
                yes_price=yes_price,
                volume=vol,
                url=f"https://kalshi.com/markets/{ticker.lower()}" if ticker else "",
                event_ticker=m.get("event_ticker"),
                subtitle=m.get("subtitle"),
            ))

        logger.info(f"Fetched {len(markets)} active markets from Kalshi")

    except aiohttp.ClientError as e:
        logger.error(f"Kalshi fetch error: {e}")
    except Exception as e:
        logger.error(f"Kalshi unexpected error: {e}")

    return markets


async def _fetch_public_fallback(
    session: aiohttp.ClientSession,
    limit: int,
    min_volume: int,
) -> list[KalshiMarket]:
    """Fallback: fetch from Kalshi public events endpoint."""
    markets = []
    url = f"{Config.KALSHI_API_BASE}/events"
    params = {"limit": min(limit, 100), "status": "open"}

    try:
        async with session.get(url, params=params,
                               timeout=aiohttp.ClientTimeout(total=30)) as resp:
            if resp.status != 200:
                logger.warning(f"Kalshi fallback also returned {resp.status}")
                return markets
            data = await resp.json()

        for event in data.get("events", []):
            for m in event.get("markets", []):
                vol = m.get("volume", 0) or 0
                if vol < min_volume:
                    continue
                yes_price_cents = m.get("yes_ask", 0) or m.get("last_price", 0) or 0
                ticker = m.get("ticker", "")
                markets.append(KalshiMarket(
                    id=str(m.get("id", "")),
                    ticker=ticker,
                    title=m.get("title", event.get("title", "Unknown")),
                    category="other",
                    yes_price=yes_price_cents / 100.0,
                    volume=vol,
                    url=f"https://kalshi.com/markets/{ticker.lower()}" if ticker else "",
                    event_ticker=event.get("ticker"),
                ))
        logger.info(f"Fallback fetched {len(markets)} markets from Kalshi")

    except Exception as e:
        logger.error(f"Kalshi fallback error: {e}")

    return markets


def normalize_title(title: str) -> str:
    """Normalize market title for matching across platforms."""
    import re
    title = title.lower().strip()
    # Remove common prefixes/suffixes
    title = re.sub(r"^(will|is|does|has|can)\s+", "", title)
    title = re.sub(r"\?$", "", title)
    title = re.sub(r"\s+", " ", title)
    return title
