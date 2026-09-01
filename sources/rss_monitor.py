"""RSS feed monitor for news, legislation, and central bank updates."""

import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional

import aiohttp
import feedparser

from config import Config

logger = logging.getLogger(__name__)


@dataclass
class RSSItem:
    feed_name: str
    feed_category: str  # "central_banks", "news", "legislation"
    title: str
    summary: str
    link: str
    published: Optional[datetime] = None
    guid: str = ""


# Keywords that indicate market-relevant RSS items
MARKET_KEYWORDS = [
    # Central banks / macro
    "interest rate", "rate decision", "monetary policy", "quantitative",
    "inflation", "cpi", "gdp", "employment", "unemployment", "payroll",
    "recession", "stimulus", "taper", "hike", "cut", "pause", "hold",
    "fed", "ecb", "boe", "boj", "fomc", "dot plot",
    # Crypto
    "bitcoin", "ethereum", "crypto", "stablecoin", "defi", "sec crypto",
    "digital asset", "cbdc", "blockchain",
    # Politics
    "election", "impeach", "legislation", "bill pass", "executive order",
    "sanction", "tariff", "trade war", "debt ceiling", "shutdown",
    # Geopolitics
    "war", "conflict", "ceasefire", "invasion", "nato", "missile",
    "nuclear", "sanctions",
    # Market-moving
    "breaking", "urgent", "flash", "surprise", "unexpected",
]


def is_market_relevant(title: str, summary: str) -> bool:
    """Check if an RSS item is relevant to prediction markets."""
    text = f"{title} {summary}".lower()
    return any(kw in text for kw in MARKET_KEYWORDS)


async def fetch_feed(
    session: aiohttp.ClientSession,
    feed_info: dict,
    category: str,
) -> list[RSSItem]:
    """Fetch and parse a single RSS feed."""
    items = []
    url = feed_info["url"]
    name = feed_info["name"]

    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=20)) as resp:
            if resp.status != 200:
                logger.warning(f"RSS feed {name} returned {resp.status}")
                return items
            content = await resp.text()

        feed = feedparser.parse(content)

        for entry in feed.entries[:20]:  # Last 20 items per feed
            title = entry.get("title", "")
            summary = entry.get("summary", entry.get("description", ""))
            # Truncate summary
            if len(summary) > 500:
                summary = summary[:497] + "..."

            link = entry.get("link", "")
            guid = entry.get("id", entry.get("guid", link))

            # Parse published date
            published = None
            if hasattr(entry, "published_parsed") and entry.published_parsed:
                try:
                    published = datetime(*entry.published_parsed[:6], tzinfo=timezone.utc)
                except (TypeError, ValueError):
                    pass

            items.append(RSSItem(
                feed_name=name,
                feed_category=category,
                title=title,
                summary=summary,
                link=link,
                published=published,
                guid=guid,
            ))

    except aiohttp.ClientError as e:
        logger.warning(f"RSS fetch error for {name}: {e}")
    except Exception as e:
        logger.warning(f"RSS parse error for {name}: {e}")

    return items


async def fetch_all_feeds(
    session: aiohttp.ClientSession,
    since: Optional[datetime] = None,
    market_relevant_only: bool = True,
) -> list[RSSItem]:
    """Fetch all configured RSS feeds and return new items."""
    all_items = []

    for category, feeds in Config.RSS_FEEDS.items():
        for feed_info in feeds:
            items = await fetch_feed(session, feed_info, category)
            for item in items:
                # Filter by time if provided
                if since and item.published and item.published < since:
                    continue
                # Filter by relevance
                if market_relevant_only and not is_market_relevant(item.title, item.summary):
                    continue
                all_items.append(item)

    logger.info(f"Fetched {len(all_items)} relevant RSS items across all feeds")
    return all_items
