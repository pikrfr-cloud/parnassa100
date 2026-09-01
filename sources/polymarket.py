"""Polymarket data fetcher using Gamma API (public, no auth required)."""

import logging
from dataclasses import dataclass
from typing import Optional

import aiohttp

from config import Config

logger = logging.getLogger(__name__)

CATEGORY_KEYWORDS = {
    "crypto": ["bitcoin", "btc", "eth", "ethereum", "crypto", "solana", "defi", "token"],
    "politics": ["president", "election", "trump", "biden", "senate", "congress", "vote",
                  "governor", "democrat", "republican", "party", "prime minister"],
    "macro": ["fed", "rate", "inflation", "gdp", "recession", "treasury", "cpi",
              "unemployment", "interest rate", "central bank", "tariff"],
    "sports": ["nba", "nfl", "mlb", "super bowl", "world cup", "champion", "playoff"],
    "tech": ["ai", "openai", "apple", "google", "microsoft", "tesla", "spacex", "launch"],
    "climate": ["hurricane", "earthquake", "temperature", "climate", "wildfire"],
}


@dataclass
class PolymarketEvent:
    id: str
    title: str
    slug: str
    category: str
    outcomes: list[dict]  # [{"name": "Yes", "price": 0.72}, ...]
    volume: float
    url: str
    end_date: Optional[str] = None


def classify_market(title: str) -> str:
    """Classify a market into a category based on keywords."""
    title_lower = title.lower()
    for cat, keywords in CATEGORY_KEYWORDS.items():
        if any(kw in title_lower for kw in keywords):
            return cat
    return "other"


async def fetch_active_markets(
    session: aiohttp.ClientSession,
    limit: int = 100,
    min_volume: float = 10000,
) -> list[PolymarketEvent]:
    """Fetch active markets from Polymarket Gamma API."""
    events = []
    url = f"{Config.POLYMARKET_API_BASE}/events"
    params = {
        "active": "true",
        "closed": "false",
        "limit": limit,
        "order": "volume24hr",
        "ascending": "false",
    }

    try:
        async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=30)) as resp:
            if resp.status != 200:
                logger.error(f"Polymarket API returned {resp.status}")
                return events
            data = await resp.json()

        for event in data:
            # Extract market outcomes and prices
            markets = event.get("markets", [])
            if not markets:
                continue

            total_volume = sum(float(m.get("volume", 0) or 0) for m in markets)
            if total_volume < min_volume:
                continue

            # Use the primary market (first one, usually the main Yes/No)
            primary = markets[0]
            outcomes = []
            for outcome_name in ["Yes", "No"]:
                price_key = f"outcome{'Yes' if outcome_name == 'Yes' else 'No'}Price"
                # Gamma API uses outcomePrices as JSON string
                outcome_prices = primary.get("outcomePrices", "")
                if outcome_prices:
                    try:
                        import json
                        prices = json.loads(outcome_prices)
                        if outcome_name == "Yes" and len(prices) > 0:
                            outcomes.append({"name": "Yes", "price": float(prices[0])})
                        elif outcome_name == "No" and len(prices) > 1:
                            outcomes.append({"name": "No", "price": float(prices[1])})
                    except (json.JSONDecodeError, IndexError, ValueError):
                        pass

            # Fallback: use bestAsk/bestBid if available
            if not outcomes:
                yes_price = primary.get("bestAsk") or primary.get("lastTradePrice")
                if yes_price:
                    yp = float(yes_price)
                    outcomes = [
                        {"name": "Yes", "price": yp},
                        {"name": "No", "price": 1.0 - yp},
                    ]

            if not outcomes:
                continue

            title = event.get("title", "Unknown")
            slug = event.get("slug", "")
            events.append(PolymarketEvent(
                id=str(event.get("id", "")),
                title=title,
                slug=slug,
                category=classify_market(title),
                outcomes=outcomes,
                volume=total_volume,
                url=f"https://polymarket.com/event/{slug}" if slug else "",
                end_date=event.get("endDate"),
            ))

        logger.info(f"Fetched {len(events)} active markets from Polymarket")

    except aiohttp.ClientError as e:
        logger.error(f"Polymarket fetch error: {e}")
    except Exception as e:
        logger.error(f"Polymarket unexpected error: {e}")

    return events


def get_yes_price(event: PolymarketEvent) -> Optional[float]:
    """Get the 'Yes' price for an event (0-1 scale)."""
    for o in event.outcomes:
        if o["name"] == "Yes":
            return o["price"]
    return None
