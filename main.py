#!/usr/bin/env python3
"""
🇮🇷 בוט מודיעין שווקי הימורים — איראן
=========================================
מנטר שווקי הימורים (Polymarket + Kalshi) הקשורים לאיראן בלבד.
ניתוח AI עם Claude לכל התראה. עברית בלבד.

מאפיינים:
  - מעקב מחירים כל 2-3 דקות
  - התראות ארביטראז' (פער >5% בין פלטפורמות)
  - זיהוי קורלציות חריגות בין שווקים קשורים
  - מעקב חדשות איראן כל 5-10 דקות
  - ניתוח AI מעמיק לכל התראה
"""

import asyncio
import json
import logging
import os
import re
import signal
import sys
from datetime import datetime, timezone, timedelta
from difflib import SequenceMatcher
from typing import Any, Optional

import aiohttp
import feedparser
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger
from dotenv import load_dotenv
from telegram import Bot
from telegram.constants import ParseMode
from telegram.error import TelegramError, RetryAfter

load_dotenv()

# ═══════════════════════════════════════════════════════════
# CONFIG
# ═══════════════════════════════════════════════════════════

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "")
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
STATE_FILE = os.getenv("STATE_FILE", "/data/bot_state.json")

# Scan intervals
MARKET_SCAN_MINUTES = 3       # Scan markets every 3 minutes
NEWS_SCAN_MINUTES = 7         # Scan news every 7 minutes

# Alert thresholds
ARBITRAGE_THRESHOLD_PCT = 5.0   # Alert if gap > 5% between platforms
BIG_MOVE_THRESHOLD_PCT = 10.0   # Alert if market moves > 10% in 24h
CORRELATION_MOVE_PCT = 10.0     # Correlation alert: one moves 10%+, other doesn't

# APIs
POLYMARKET_API = "https://gamma-api.polymarket.com"
KALSHI_API = "https://api.elections.kalshi.com/trade-api/v2"
CLAUDE_API = "https://api.anthropic.com/v1/messages"
CLAUDE_MODEL = "claude-sonnet-4-20250514"

# ── Iran Keywords for Market Filtering ──
IRAN_KEYWORDS = [
    "iran", "iranian", "khamenei", "mojtaba", "supreme leader",
    "irgc", "revolutionary guard", "tehran", "persian",
    "assembly of experts", "ayatollah", "raisi",
    "iran nuclear", "iran sanction", "iran regime",
    "iran war", "iran strike", "iran attack",
    "iran deal", "jcpoa", "iran israel",
    "iran leadership", "iran succession",
    "iran collapse", "iran revolution",
]

# ── Iran News Search Keywords (for RSS/Google News) ──
IRAN_NEWS_QUERIES = [
    "Iran Supreme Leader successor",
    "Mojtaba Khamenei",
    "Assembly of Experts Iran",
    "Iran leadership transition",
    "Iran regime change",
    "IRGC Iran",
    "Iran nuclear deal",
    "Iran Israel conflict",
    "Iran sanctions",
    "Khamenei health",
]

# ── Known Correlated Market Pairs (Claude will also detect dynamically) ──
CORRELATION_HINTS = [
    ("supreme leader", "regime"),
    ("supreme leader", "succession"),
    ("nuclear", "sanctions"),
    ("war", "strike"),
    ("israel", "attack"),
    ("regime", "revolution"),
    ("irgc", "regime"),
    ("mojtaba", "supreme leader"),
]

# ═══════════════════════════════════════════════════════════
# LOGGING
# ═══════════════════════════════════════════════════════════

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s │ %(levelname)-7s │ %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("iran-bot")
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("telegram").setLevel(logging.WARNING)
logging.getLogger("apscheduler").setLevel(logging.WARNING)


# ═══════════════════════════════════════════════════════════
# CLAUDE AI ENGINE
# ═══════════════════════════════════════════════════════════

AI_SYSTEM = """אתה אנליסט מודיעין בכיר המתמחה בשווקי הימורים פוליטיים, עם מומחיות מיוחדת באיראן.

הידע שלך כולל:
- המבנה הפוליטי של איראן (מנהיג עליון, מועצת המומחים, משמרות המהפכה)
- שחקני המפתח (חמינאי, מוג'תבא חמינאי, ראיסי, IRGC)
- הגרעין האיראני, סנקציות, ו-JCPOA
- הדינמיקה האזורית (איראן-ישראל, איראן-ארה"ב)
- שוקי הימורים (Polymarket, Kalshi) ואיך לזהות הזדמנויות

כללים:
1. כתוב תמיד בעברית
2. היה מדויק ומבוסס עובדות
3. ציין תמיד רמת ביטחון
4. אל תפחד להגיד "לא ברור" כשאין מספיק מידע
5. ענה תמיד ב-JSON בלבד, בלי backticks"""


async def ask_claude(session: aiohttp.ClientSession, prompt: str, max_tokens: int = 1500) -> Optional[str]:
    """Call Claude API."""
    if not ANTHROPIC_API_KEY:
        logger.warning("No ANTHROPIC_API_KEY set")
        return None

    headers = {
        "x-api-key": ANTHROPIC_API_KEY,
        "anthropic-version": "2023-06-01",
        "content-type": "application/json",
    }
    payload = {
        "model": CLAUDE_MODEL,
        "max_tokens": max_tokens,
        "system": AI_SYSTEM,
        "messages": [{"role": "user", "content": prompt}],
    }

    try:
        async with session.post(CLAUDE_API, json=payload, headers=headers,
                                timeout=aiohttp.ClientTimeout(total=45)) as r:
            if r.status != 200:
                err = await r.text()
                logger.error(f"Claude API {r.status}: {err[:200]}")
                return None
            data = await r.json()
            content = data.get("content", [])
            if content and content[0].get("type") == "text":
                return content[0]["text"]
    except Exception as e:
        logger.error(f"Claude API error: {e}")
    return None


def parse_claude_json(text: str) -> dict:
    """Safely parse Claude's JSON response."""
    if not text:
        return {}
    try:
        clean = text.strip()
        if clean.startswith("```"):
            clean = re.sub(r"```json?|```", "", clean).strip()
        return json.loads(clean)
    except json.JSONDecodeError:
        logger.warning(f"JSON parse failed: {text[:150]}")
        return {}


# ═══════════════════════════════════════════════════════════
# AI ANALYSIS FUNCTIONS
# ═══════════════════════════════════════════════════════════

async def ai_analyze_arbitrage(session, market_name, poly_price, kalshi_price, gap_pct, poly_url, kalshi_url):
    """Deep AI analysis of an arbitrage opportunity."""
    prompt = f"""נתח הזדמנות ארביטראז' בשוק הימורים הקשור לאיראן:

שוק: {market_name}
מחיר Polymarket: {poly_price}% (כן)
מחיר Kalshi: {kalshi_price}% (כן)
פער: {gap_pct:.1f}%

נתח וענה ב-JSON:
{{
    "title_he": "שם השוק בעברית",
    "context": "הקשר פוליטי/גיאופוליטי — מה קורה באיראן שרלוונטי לשוק הזה (3-4 משפטים)",
    "why_gap": "למה כנראה קיים הפער — האם זה חוסר נזילות, מידע אסימטרי, או הבדל בבסיס המשתמשים (2-3 משפטים)",
    "risk_assessment": "הערכת סיכון — מה הסיכונים בניסיון לנצל את הפער (2-3 משפטים)",
    "opportunity": "האם זו הזדמנות אמיתית או מלכודת, ובאיזו רמת ביטחון (2-3 משפטים)",
    "recommendation": "המלצה ספציפית — מה כדאי לעשות ומה לא, עם סייגים (2-3 משפטים)",
    "watch_factors": ["גורם 1 לעקוב", "גורם 2 לעקוב", "גורם 3 לעקוב"],
    "confidence": "גבוהה" או "בינונית" או "נמוכה"
}}"""
    return parse_claude_json(await ask_claude(session, prompt))


async def ai_analyze_correlation(session, market_a, market_b, price_a, price_b, move_a, move_b):
    """Deep AI analysis of a correlation anomaly."""
    prompt = f"""זוהתה אנומליית קורלציה בין שני שווקי הימורים הקשורים לאיראן:

שוק א': {market_a['title']} — מחיר: {price_a}% — תנועה 24 שעות: {move_a:+.1f}%
שוק ב': {market_b['title']} — מחיר: {price_b}% — תנועה 24 שעות: {move_b:+.1f}%

שוק אחד זז משמעותית בלי שהשני הגיב — מה זה אומר?

ענה ב-JSON:
{{
    "title_he": "כותרת קצרה לאנומליה בעברית",
    "context": "הקשר — למה השווקים האלה אמורים להיות מקושרים (2-3 משפטים)",
    "anomaly_explanation": "הסבר — למה כנראה שוק אחד זז והשני לא (3-4 משפטים)",
    "opportunity": "האם יש כאן הזדמנות — האם השוק שלא זז 'מפגר' או שהוא צודק (2-3 משפטים)",
    "risk_assessment": "סיכונים — מה יכול להשתבש אם פועלים על הפער (2 משפטים)",
    "recommendation": "המלצה ספציפית עם סייגים (2-3 משפטים)",
    "expected_resolution": "מה צפוי לקרות — האם הפער ייסגר ואיך (2 משפטים)",
    "watch_factors": ["גורם 1", "גורם 2", "גורם 3"],
    "confidence": "גבוהה" או "בינונית" או "נמוכה"
}}"""
    return parse_claude_json(await ask_claude(session, prompt))


async def ai_analyze_big_move(session, market, old_price, new_price, timeframe):
    """Deep AI analysis of a big price move."""
    direction = "עלייה" if new_price > old_price else "ירידה"
    prompt = f"""נתח תנועת מחיר גדולה בשוק הימורים הקשור לאיראן:

שוק: {market['title']}
מחיר קודם: {old_price}%
מחיר נוכחי: {new_price}%
שינוי: {new_price - old_price:+.1f}%
כיוון: {direction}
טווח זמן: {timeframe}

ענה ב-JSON:
{{
    "title_he": "שם השוק בעברית",
    "what_happened": "מה כנראה גרם לתנועה הזו (3-4 משפטים, התבסס על ידע על המצב באיראן)",
    "significance": "כמה זה משמעותי ולמה (2-3 משפטים)",
    "impact_on_related": "איך זה עשוי להשפיע על שווקים קשורים אחרים (2-3 משפטים)",
    "direction_forecast": "האם התנועה צפויה להמשיך, להתהפך, או להתייצב (2-3 משפטים)",
    "recommendation": "המלצה ספציפית (2-3 משפטים)",
    "watch_factors": ["גורם 1", "גורם 2", "גורם 3"],
    "confidence": "גבוהה" או "בינונית" או "נמוכה"
}}"""
    return parse_claude_json(await ask_claude(session, prompt))


async def ai_analyze_news(session, news_items, current_markets):
    """AI analysis of news + its impact on current Iran markets."""
    markets_summary = "\n".join([
        f"  - {m['title']}: {m['yes_price']*100:.1f}% ({m['source']})"
        for m in current_markets[:15]
    ]) or "  אין שווקים פעילים כרגע"

    news_text = "\n".join([
        f"  - [{item['source']}] {item['title']}"
        for item in news_items[:5]
    ])

    prompt = f"""התקבלו חדשות חדשות הקשורות לאיראן. נתח את ההשפעה על שווקי ההימורים.

חדשות חדשות:
{news_text}

שווקים פעילים כרגע:
{markets_summary}

ענה ב-JSON:
{{
    "headline_he": "כותרת ראשית בעברית שמסכמת את החדשות (משפט אחד)",
    "summary_he": "סיכום מפורט בעברית של כל החדשות החדשות (3-5 משפטים)",
    "market_impact": [
        {{
            "market": "שם השוק המושפע",
            "current_price": "המחיר הנוכחי",
            "expected_direction": "עלייה" או "ירידה" או "ללא שינוי",
            "impact_level": "🔴 גבוהה" או "🟡 בינונית" או "🟢 נמוכה",
            "explanation": "למה השוק הזה מושפע (משפט אחד)"
        }}
    ],
    "key_insight": "התובנה המרכזית — מה הדבר הכי חשוב שצריך להבין מהחדשות האלה (2-3 משפטים)",
    "recommendation": "המלצה (2-3 משפטים)",
    "urgency": "דחוף" או "חשוב" או "לידיעה",
    "watch_factors": ["גורם 1", "גורם 2"],
    "confidence": "גבוהה" או "בינונית" או "נמוכה"
}}"""
    return parse_claude_json(await ask_claude(session, prompt, max_tokens=2000))


# ═══════════════════════════════════════════════════════════
# MARKET DATA FETCHING
# ═══════════════════════════════════════════════════════════

def is_iran_market(title: str, description: str = "") -> bool:
    """Check if a market is related to Iran."""
    text = f"{title} {description}".lower()
    return any(kw in text for kw in IRAN_KEYWORDS)


async def fetch_polymarket_iran(session: aiohttp.ClientSession) -> list[dict]:
    """Fetch Iran-related markets from Polymarket."""
    markets = []
    try:
        # Fetch a large batch and filter for Iran
        url = f"{POLYMARKET_API}/events"
        params = {"active": "true", "closed": "false", "limit": 200,
                  "order": "volume24hr", "ascending": "false"}
        async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=30)) as r:
            if r.status != 200:
                logger.error(f"Polymarket API: {r.status}")
                return markets
            data = await r.json()

        for ev in data:
            title = ev.get("title", "")
            desc = ev.get("description", "")
            if not is_iran_market(title, desc):
                continue

            ev_markets = ev.get("markets", [])
            if not ev_markets:
                continue

            slug = ev.get("slug", "")
            ev_url = f"https://polymarket.com/event/{slug}" if slug else ""

            for m in ev_markets:
                outcomes = []
                op = m.get("outcomePrices", "")
                if op:
                    try:
                        prices = json.loads(op)
                        if len(prices) > 0:
                            outcomes.append(float(prices[0]))
                        if len(prices) > 1:
                            outcomes.append(float(prices[1]))
                    except (json.JSONDecodeError, IndexError, ValueError):
                        pass

                if not outcomes:
                    yp = m.get("bestAsk") or m.get("lastTradePrice")
                    if yp:
                        outcomes = [float(yp), 1.0 - float(yp)]

                if not outcomes:
                    continue

                m_title = m.get("question", m.get("groupItemTitle", title))
                markets.append({
                    "id": f"poly_{m.get('id', ev.get('id', ''))}",
                    "title": m_title,
                    "event_title": title,
                    "yes_price": outcomes[0],
                    "source": "Polymarket",
                    "url": ev_url,
                    "volume": float(m.get("volume", 0) or 0),
                })

        # Also search specifically for Iran
        for query_term in ["iran", "khamenei", "supreme leader iran"]:
            try:
                search_url = f"{POLYMARKET_API}/events"
                search_params = {"active": "true", "closed": "false", "limit": 50,
                                 "tag": query_term}
                async with session.get(search_url, params=search_params,
                                       timeout=aiohttp.ClientTimeout(total=15)) as r:
                    if r.status != 200:
                        continue
                    search_data = await r.json()

                for ev in search_data:
                    title = ev.get("title", "")
                    ev_id = ev.get("id", "")
                    # Skip if already found
                    if any(f"poly_{ev_id}" in m["id"] for m in markets):
                        continue
                    if not is_iran_market(title, ev.get("description", "")):
                        continue

                    slug = ev.get("slug", "")
                    for m in ev.get("markets", []):
                        op = m.get("outcomePrices", "")
                        outcomes = []
                        if op:
                            try:
                                prices = json.loads(op)
                                if prices:
                                    outcomes = [float(prices[0])]
                            except Exception:
                                pass
                        if not outcomes:
                            yp = m.get("bestAsk") or m.get("lastTradePrice")
                            if yp:
                                outcomes = [float(yp)]
                        if not outcomes:
                            continue

                        m_title = m.get("question", m.get("groupItemTitle", title))
                        markets.append({
                            "id": f"poly_{m.get('id', ev_id)}",
                            "title": m_title,
                            "event_title": title,
                            "yes_price": outcomes[0],
                            "source": "Polymarket",
                            "url": f"https://polymarket.com/event/{slug}" if slug else "",
                            "volume": float(m.get("volume", 0) or 0),
                        })
            except Exception as e:
                logger.debug(f"Polymarket search '{query_term}': {e}")

        # Deduplicate by id
        seen = set()
        unique = []
        for m in markets:
            if m["id"] not in seen:
                seen.add(m["id"])
                unique.append(m)
        markets = unique

        logger.info(f"Polymarket Iran: {len(markets)} markets")
    except Exception as e:
        logger.error(f"Polymarket error: {e}")
    return markets


async def fetch_kalshi_iran(session: aiohttp.ClientSession) -> list[dict]:
    """Fetch Iran-related markets from Kalshi."""
    markets = []
    try:
        url = f"{KALSHI_API}/markets"
        params = {"limit": 500, "status": "open"}
        headers = {"Accept": "application/json"}
        async with session.get(url, params=params, headers=headers,
                               timeout=aiohttp.ClientTimeout(total=30)) as r:
            if r.status != 200:
                logger.warning(f"Kalshi API: {r.status}")
                return markets
            data = await r.json()

        for m in data.get("markets", []):
            title = m.get("title", "")
            subtitle = m.get("subtitle", "")
            if not is_iran_market(title, subtitle):
                continue

            yp = (m.get("yes_ask", 0) or m.get("last_price", 0) or 0) / 100.0
            ticker = m.get("ticker", "")
            markets.append({
                "id": f"kalshi_{m.get('id', '')}",
                "title": title,
                "event_title": title,
                "yes_price": yp,
                "source": "Kalshi",
                "url": f"https://kalshi.com/markets/{ticker.lower()}" if ticker else "",
                "volume": m.get("volume", 0) or 0,
                "subtitle": subtitle,
            })

        logger.info(f"Kalshi Iran: {len(markets)} markets")
    except Exception as e:
        logger.error(f"Kalshi error: {e}")
    return markets


# ═══════════════════════════════════════════════════════════
# NEWS FETCHING
# ═══════════════════════════════════════════════════════════

async def fetch_iran_news(session: aiohttp.ClientSession) -> list[dict]:
    """Fetch Iran-related news from Google News RSS and other sources."""
    items = []

    # Google News RSS allows keyword search
    for query in IRAN_NEWS_QUERIES:
        encoded = query.replace(" ", "+")
        url = f"https://news.google.com/rss/search?q={encoded}&hl=en&gl=US&ceid=US:en"
        try:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=15),
                                   headers={"User-Agent": "MarketIntelBot/1.0"}) as r:
                if r.status != 200:
                    continue
                content = await r.text()

            feed = feedparser.parse(content)
            for entry in feed.entries[:5]:
                title = entry.get("title", "")
                link = entry.get("link", "")
                guid = entry.get("id", entry.get("guid", link))

                # Extract source from Google News title format "Title - Source"
                source = "Google News"
                if " - " in title:
                    parts = title.rsplit(" - ", 1)
                    title = parts[0]
                    source = parts[1] if len(parts) > 1 else source

                pub = None
                if hasattr(entry, "published_parsed") and entry.published_parsed:
                    try:
                        pub = datetime(*entry.published_parsed[:6], tzinfo=timezone.utc)
                    except (TypeError, ValueError):
                        pass

                items.append({
                    "title": title,
                    "source": source,
                    "link": link,
                    "guid": guid,
                    "pub": pub,
                    "query": query,
                })
        except Exception as e:
            logger.debug(f"News fetch '{query}': {e}")

    # Deduplicate by title similarity
    unique = []
    seen_titles = []
    for item in items:
        is_dup = False
        for st in seen_titles:
            if SequenceMatcher(None, item["title"].lower(), st).ratio() > 0.8:
                is_dup = True
                break
        if not is_dup:
            unique.append(item)
            seen_titles.append(item["title"].lower())

    # Sort by date (newest first)
    unique.sort(key=lambda x: x.get("pub") or datetime.min.replace(tzinfo=timezone.utc), reverse=True)

    logger.info(f"Iran news: {len(unique)} unique items")
    return unique


# ═══════════════════════════════════════════════════════════
# ANALYSIS ENGINES
# ═══════════════════════════════════════════════════════════

def find_arbitrage_opportunities(poly_markets, kalshi_markets, threshold_pct=None):
    """Find price gaps between matched markets on different platforms."""
    if threshold_pct is None:
        threshold_pct = ARBITRAGE_THRESHOLD_PCT

    opportunities = []
    used_kalshi = set()

    for pm in poly_markets:
        best_match = None
        best_score = 0

        for km in kalshi_markets:
            if km["id"] in used_kalshi:
                continue

            # Compare titles
            pm_norm = pm["title"].lower().strip()
            km_norm = km["title"].lower().strip()
            score = SequenceMatcher(None, pm_norm, km_norm).ratio()

            # Also check event title vs subtitle
            if km.get("subtitle"):
                alt = SequenceMatcher(None, pm_norm, km["subtitle"].lower()).ratio()
                score = max(score, alt)
            if pm.get("event_title"):
                alt = SequenceMatcher(None, pm["event_title"].lower(), km_norm).ratio()
                score = max(score, alt)

            if score > best_score:
                best_score = score
                best_match = km

        if best_match and best_score >= 0.45:  # Lower threshold for Iran-specific
            used_kalshi.add(best_match["id"])
            poly_pct = pm["yes_price"] * 100
            kalshi_pct = best_match["yes_price"] * 100
            gap = abs(poly_pct - kalshi_pct)

            if gap >= threshold_pct:
                opportunities.append({
                    "name": pm["title"],
                    "poly_price": round(poly_pct, 1),
                    "kalshi_price": round(kalshi_pct, 1),
                    "gap_pct": round(gap, 1),
                    "poly_url": pm["url"],
                    "kalshi_url": best_match["url"],
                    "match_score": round(best_score, 2),
                })

    opportunities.sort(key=lambda x: x["gap_pct"], reverse=True)
    return opportunities


def find_correlation_anomalies(all_markets, price_history, threshold_pct=None):
    """Find markets that should be correlated but moved differently."""
    if threshold_pct is None:
        threshold_pct = CORRELATION_MOVE_PCT

    anomalies = []

    # Calculate 24h moves for each market
    moves = {}
    for m in all_markets:
        mid = m["id"]
        current = m["yes_price"] * 100
        history = price_history.get(mid, [])
        if not history:
            continue

        # Find price ~24h ago (or oldest available)
        oldest_price = history[0]["price"] * 100
        move = current - oldest_price
        moves[mid] = {"market": m, "current": current, "move": move}

    # Check correlation pairs
    market_list = list(moves.values())
    for i, a in enumerate(market_list):
        for b in market_list[i+1:]:
            # Check if these markets should be correlated
            title_a = a["market"]["title"].lower()
            title_b = b["market"]["title"].lower()

            is_correlated = False
            for kw_a, kw_b in CORRELATION_HINTS:
                if (kw_a in title_a and kw_b in title_b) or \
                   (kw_b in title_a and kw_a in title_b):
                    is_correlated = True
                    break

            # Also use title similarity
            if not is_correlated:
                sim = SequenceMatcher(None, title_a, title_b).ratio()
                if sim > 0.4:
                    is_correlated = True

            if not is_correlated:
                continue

            # Check if one moved significantly but the other didn't
            big_a = abs(a["move"]) >= threshold_pct
            big_b = abs(b["move"]) >= threshold_pct

            if big_a and not big_b and abs(b["move"]) < threshold_pct * 0.3:
                anomalies.append({
                    "mover": a, "laggard": b,
                    "mover_market": a["market"], "laggard_market": b["market"],
                })
            elif big_b and not big_a and abs(a["move"]) < threshold_pct * 0.3:
                anomalies.append({
                    "mover": b, "laggard": a,
                    "mover_market": b["market"], "laggard_market": a["market"],
                })

    return anomalies


def find_big_moves(all_markets, price_history, threshold_pct=None):
    """Find markets with big 24h price moves."""
    if threshold_pct is None:
        threshold_pct = BIG_MOVE_THRESHOLD_PCT

    moves = []
    for m in all_markets:
        mid = m["id"]
        current = m["yes_price"] * 100
        history = price_history.get(mid, [])
        if not history:
            continue

        oldest = history[0]["price"] * 100
        delta = current - oldest

        if abs(delta) >= threshold_pct:
            moves.append({
                "market": m,
                "old_price": round(oldest, 1),
                "new_price": round(current, 1),
                "delta": round(delta, 1),
            })

    moves.sort(key=lambda x: abs(x["delta"]), reverse=True)
    return moves


# ═══════════════════════════════════════════════════════════
# STATE PERSISTENCE
# ═══════════════════════════════════════════════════════════

class State:
    def __init__(self):
        self.price_history = {}    # market_id → [{"price": 0.72, "ts": "...", ...}, ...]
        self.seen_news = []        # GUIDs of sent news
        self.sent_arb_alerts = {}  # "poly_id:kalshi_id" → last_alert_ts
        self.sent_corr_alerts = {} # "id_a:id_b" → last_alert_ts
        self.sent_move_alerts = {} # market_id → last_alert_ts
        self.last_news_check = None
        self.scan_count = 0
        self._load()

    def _load(self):
        if os.path.exists(STATE_FILE):
            try:
                with open(STATE_FILE) as f:
                    s = json.load(f)
                self.price_history = s.get("price_history", {})
                self.seen_news = s.get("seen_news", [])
                self.sent_arb_alerts = s.get("sent_arb_alerts", {})
                self.sent_corr_alerts = s.get("sent_corr_alerts", {})
                self.sent_move_alerts = s.get("sent_move_alerts", {})
                self.last_news_check = s.get("last_news_check")
                self.scan_count = s.get("scan_count", 0)
                logger.info(f"State loaded — scan #{self.scan_count}, "
                           f"tracking {len(self.price_history)} markets")
            except Exception as e:
                logger.warning(f"State load failed: {e}")

    def save(self):
        try:
            os.makedirs(os.path.dirname(STATE_FILE), exist_ok=True)
            # Trim history to last 24h (keep ~500 data points per market)
            trimmed_history = {}
            cutoff = (datetime.now(timezone.utc) - timedelta(hours=25)).isoformat()
            for mid, entries in self.price_history.items():
                trimmed = [e for e in entries if e.get("ts", "") > cutoff]
                if trimmed:
                    trimmed_history[mid] = trimmed[-500:]  # Max 500 per market

            with open(STATE_FILE, "w") as f:
                json.dump({
                    "price_history": trimmed_history,
                    "seen_news": self.seen_news[-1000:],
                    "sent_arb_alerts": self.sent_arb_alerts,
                    "sent_corr_alerts": self.sent_corr_alerts,
                    "sent_move_alerts": self.sent_move_alerts,
                    "last_news_check": self.last_news_check,
                    "scan_count": self.scan_count,
                }, f)
        except Exception as e:
            logger.error(f"State save failed: {e}")

    def record_price(self, market_id: str, price: float):
        """Record a price point for a market."""
        if market_id not in self.price_history:
            self.price_history[market_id] = []
        self.price_history[market_id].append({
            "price": price,
            "ts": datetime.now(timezone.utc).isoformat(),
        })

    def can_alert_arb(self, key: str, cooldown_minutes: int = 30) -> bool:
        """Check if we can send another arbitrage alert (cooldown)."""
        last = self.sent_arb_alerts.get(key)
        if not last:
            return True
        try:
            last_dt = datetime.fromisoformat(last)
            return datetime.now(timezone.utc) - last_dt > timedelta(minutes=cooldown_minutes)
        except (TypeError, ValueError):
            return True

    def mark_arb_alert(self, key: str):
        self.sent_arb_alerts[key] = datetime.now(timezone.utc).isoformat()

    def can_alert_corr(self, key: str, cooldown_minutes: int = 60) -> bool:
        last = self.sent_corr_alerts.get(key)
        if not last:
            return True
        try:
            last_dt = datetime.fromisoformat(last)
            return datetime.now(timezone.utc) - last_dt > timedelta(minutes=cooldown_minutes)
        except (TypeError, ValueError):
            return True

    def mark_corr_alert(self, key: str):
        self.sent_corr_alerts[key] = datetime.now(timezone.utc).isoformat()

    def can_alert_move(self, market_id: str, cooldown_minutes: int = 120) -> bool:
        last = self.sent_move_alerts.get(market_id)
        if not last:
            return True
        try:
            last_dt = datetime.fromisoformat(last)
            return datetime.now(timezone.utc) - last_dt > timedelta(minutes=cooldown_minutes)
        except (TypeError, ValueError):
            return True

    def mark_move_alert(self, market_id: str):
        self.sent_move_alerts[market_id] = datetime.now(timezone.utc).isoformat()


# ═══════════════════════════════════════════════════════════
# TELEGRAM NOTIFIER (HEBREW)
# ═══════════════════════════════════════════════════════════

class Notifier:
    def __init__(self):
        self.bot = Bot(token=TELEGRAM_BOT_TOKEN)
        self.chat = TELEGRAM_CHAT_ID

    async def send(self, text: str) -> bool:
        try:
            # Split if too long (Telegram limit ~4096)
            if len(text) > 4000:
                parts = [text[i:i+4000] for i in range(0, len(text), 4000)]
                for part in parts:
                    await self.bot.send_message(chat_id=self.chat, text=part,
                                                disable_web_page_preview=True)
                    await asyncio.sleep(0.5)
                return True
            await self.bot.send_message(chat_id=self.chat, text=text,
                                        disable_web_page_preview=True)
            return True
        except RetryAfter as e:
            await asyncio.sleep(e.retry_after)
            return await self.send(text)
        except TelegramError as e:
            logger.error(f"TG error: {e}")
            return False

    async def send_startup(self, market_count: int):
        msg = (
            "🇮🇷 בוט מודיעין שווקי הימורים — איראן\n"
            "━━━━━━━━━━━━━━━━━━━━━━\n\n"
            "🔍 מצב: פעיל\n"
            f"📊 שווקים פעילים: {market_count}\n"
            f"⏰ סריקת שווקים: כל {MARKET_SCAN_MINUTES} דקות\n"
            f"📰 סריקת חדשות: כל {NEWS_SCAN_MINUTES} דקות\n"
            f"🎯 סף ארביטראז': {ARBITRAGE_THRESHOLD_PCT}%\n"
            f"📈 סף תנועה גדולה: {BIG_MOVE_THRESHOLD_PCT}%\n"
            f"🔗 סף קורלציה: {CORRELATION_MOVE_PCT}%\n"
            f"🧠 מנוע AI: {'✅' if ANTHROPIC_API_KEY else '❌'}\n"
            "🌐 שפה: עברית\n\n"
            "━━━━━━━━━━━━━━━━━━━━━━"
        )
        await self.send(msg)

    async def send_arbitrage(self, opp: dict, ai: dict):
        confidence = ai.get("confidence", "—")
        msg = (
            "⚖️ התראת ארביטראז'\n"
            "━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"📊 {ai.get('title_he', opp['name'])}\n\n"
            f"Polymarket: {opp['poly_price']}%\n"
            f"Kalshi: {opp['kalshi_price']}%\n"
            f"📐 פער: {opp['gap_pct']}%\n\n"
        )
        if ai.get("context"):
            msg += f"📋 הקשר:\n{ai['context']}\n\n"
        if ai.get("why_gap"):
            msg += f"❓ למה קיים הפער:\n{ai['why_gap']}\n\n"
        if ai.get("opportunity"):
            msg += f"💰 הזדמנות:\n{ai['opportunity']}\n\n"
        if ai.get("risk_assessment"):
            msg += f"⚠️ סיכונים:\n{ai['risk_assessment']}\n\n"
        if ai.get("recommendation"):
            msg += f"💡 המלצה:\n{ai['recommendation']}\n\n"
        if ai.get("watch_factors"):
            factors = "\n".join([f"  • {f}" for f in ai["watch_factors"]])
            msg += f"👁️ גורמים לעקוב:\n{factors}\n\n"

        msg += (
            f"🎯 רמת ביטחון: {confidence}\n\n"
            f"🔗 Polymarket: {opp['poly_url']}\n"
            f"🔗 Kalshi: {opp['kalshi_url']}\n"
            "━━━━━━━━━━━━━━━━━━━━━━"
        )
        await self.send(msg)

    async def send_correlation(self, anomaly: dict, ai: dict):
        mover = anomaly["mover"]
        laggard = anomaly["laggard"]
        confidence = ai.get("confidence", "—")

        msg = (
            "🔗 התראת קורלציה חריגה\n"
            "━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"📊 {ai.get('title_he', 'אנומליית קורלציה')}\n\n"
            f"שוק שזז: {mover['market']['title']}\n"
            f"  מחיר: {mover['current']:.1f}% | תנועה: {mover['move']:+.1f}%\n\n"
            f"שוק שלא הגיב: {laggard['market']['title']}\n"
            f"  מחיר: {laggard['current']:.1f}% | תנועה: {laggard['move']:+.1f}%\n\n"
        )
        if ai.get("context"):
            msg += f"📋 הקשר:\n{ai['context']}\n\n"
        if ai.get("anomaly_explanation"):
            msg += f"🔍 הסבר האנומליה:\n{ai['anomaly_explanation']}\n\n"
        if ai.get("opportunity"):
            msg += f"💰 הזדמנות:\n{ai['opportunity']}\n\n"
        if ai.get("recommendation"):
            msg += f"💡 המלצה:\n{ai['recommendation']}\n\n"
        if ai.get("watch_factors"):
            factors = "\n".join([f"  • {f}" for f in ai["watch_factors"]])
            msg += f"👁️ גורמים לעקוב:\n{factors}\n\n"

        msg += (
            f"🎯 רמת ביטחון: {confidence}\n"
            "━━━━━━━━━━━━━━━━━━━━━━"
        )
        await self.send(msg)

    async def send_big_move(self, move: dict, ai: dict):
        m = move["market"]
        arrow = "📈" if move["delta"] > 0 else "📉"
        confidence = ai.get("confidence", "—")

        msg = (
            f"{arrow} תנועה גדולה\n"
            "━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"📊 {ai.get('title_he', m['title'])}\n"
            f"מקור: {m['source']}\n\n"
            f"לפני: {move['old_price']}% → עכשיו: {move['new_price']}%\n"
            f"שינוי: {move['delta']:+.1f}%\n\n"
        )
        if ai.get("what_happened"):
            msg += f"📋 מה קרה:\n{ai['what_happened']}\n\n"
        if ai.get("significance"):
            msg += f"⚡ משמעות:\n{ai['significance']}\n\n"
        if ai.get("impact_on_related"):
            msg += f"🔗 השפעה על שווקים קשורים:\n{ai['impact_on_related']}\n\n"
        if ai.get("direction_forecast"):
            msg += f"🔮 תחזית כיוון:\n{ai['direction_forecast']}\n\n"
        if ai.get("recommendation"):
            msg += f"💡 המלצה:\n{ai['recommendation']}\n\n"
        if ai.get("watch_factors"):
            factors = "\n".join([f"  • {f}" for f in ai["watch_factors"]])
            msg += f"👁️ גורמים לעקוב:\n{factors}\n\n"

        msg += (
            f"🎯 רמת ביטחון: {confidence}\n\n"
            f"🔗 {m['url']}\n"
            "━━━━━━━━━━━━━━━━━━━━━━"
        )
        await self.send(msg)

    async def send_news(self, ai: dict, news_items: list):
        urgency = ai.get("urgency", "לידיעה")
        urgency_emoji = {"דחוף": "🚨", "חשוב": "⚠️", "לידיעה": "ℹ️"}.get(urgency, "ℹ️")
        confidence = ai.get("confidence", "—")

        msg = (
            f"📰 חדשות איראן {urgency_emoji}\n"
            "━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"📌 {ai.get('headline_he', 'עדכון חדש')}\n\n"
        )
        if ai.get("summary_he"):
            msg += f"{ai['summary_he']}\n\n"

        # Market impact
        impacts = ai.get("market_impact", [])
        if impacts:
            msg += "🎰 השפעה על שווקים:\n"
            for imp in impacts[:5]:
                direction = imp.get("expected_direction", "—")
                level = imp.get("impact_level", "🟡")
                dir_emoji = "📈" if direction == "עלייה" else "📉" if direction == "ירידה" else "➡️"
                msg += f"  {level} {imp.get('market', '—')} ({imp.get('current_price', '—')})\n"
                msg += f"    {dir_emoji} {direction} — {imp.get('explanation', '')}\n"
            msg += "\n"

        if ai.get("key_insight"):
            msg += f"💡 תובנה מרכזית:\n{ai['key_insight']}\n\n"
        if ai.get("recommendation"):
            msg += f"📝 המלצה:\n{ai['recommendation']}\n\n"
        if ai.get("watch_factors"):
            factors = "\n".join([f"  • {f}" for f in ai["watch_factors"]])
            msg += f"👁️ גורמים לעקוב:\n{factors}\n\n"

        # Source links
        msg += "🔗 מקורות:\n"
        for item in news_items[:3]:
            msg += f"  • {item['source']}: {item['link']}\n"

        msg += (
            f"\n🎯 רמת ביטחון: {confidence}\n"
            "━━━━━━━━━━━━━━━━━━━━━━"
        )
        await self.send(msg)

    async def send_market_snapshot(self, markets: list):
        """Send periodic market snapshot."""
        if not markets:
            return
        msg = "📊 מצב שווקים — איראן\n━━━━━━━━━━━━━━━━━━━━━━\n\n"
        for m in markets[:20]:
            pct = m["yes_price"] * 100
            msg += f"  {'●' if pct > 50 else '○'} {m['title']}\n    {m['source']}: {pct:.1f}%\n\n"
        msg += "━━━━━━━━━━━━━━━━━━━━━━"
        await self.send(msg)

    async def send_heartbeat(self, market_count: int, scan_count: int):
        ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
        msg = (
            f"💓 הבוט פעיל — {ts}\n"
            f"שווקים במעקב: {market_count}\n"
            f"סריקות שבוצעו: {scan_count}\n"
            f"🧠 AI: {'✅' if ANTHROPIC_API_KEY else '❌'}"
        )
        await self.send(msg)


# ═══════════════════════════════════════════════════════════
# MAIN SCAN LOOPS
# ═══════════════════════════════════════════════════════════

# Global state and notifier
state: Optional[State] = None
notifier: Optional[Notifier] = None


async def market_scan():
    """Main market scan — runs every 2-3 minutes."""
    global state, notifier
    state.scan_count += 1
    logger.info(f"═══ Market Scan #{state.scan_count} ═══")

    try:
        async with aiohttp.ClientSession() as s:
            # ── Fetch all Iran markets ──
            poly = await fetch_polymarket_iran(s)
            kalshi = await fetch_kalshi_iran(s)
            all_markets = poly + kalshi

            if not all_markets:
                logger.info("No Iran markets found this scan")
                state.save()
                return

            # ── Record prices ──
            for m in all_markets:
                state.record_price(m["id"], m["yes_price"])

            # ── 1. Arbitrage Detection ──
            arb_opps = find_arbitrage_opportunities(poly, kalshi)
            for opp in arb_opps[:3]:
                alert_key = f"{opp['name'][:30]}"
                if not state.can_alert_arb(alert_key):
                    continue
                logger.info(f"⚖️ ARBITRAGE: {opp['name']} — {opp['gap_pct']}%")
                ai = await ai_analyze_arbitrage(
                    s, opp["name"], opp["poly_price"], opp["kalshi_price"],
                    opp["gap_pct"], opp["poly_url"], opp["kalshi_url"]
                )
                if not ai:
                    ai = {"title_he": opp["name"]}
                await notifier.send_arbitrage(opp, ai)
                state.mark_arb_alert(alert_key)

            # ── 2. Big Moves ──
            big_moves = find_big_moves(all_markets, state.price_history)
            for move in big_moves[:3]:
                mid = move["market"]["id"]
                if not state.can_alert_move(mid):
                    continue
                logger.info(f"📈 BIG MOVE: {move['market']['title']} — {move['delta']:+.1f}%")
                ai = await ai_analyze_big_move(
                    s, move["market"], move["old_price"], move["new_price"], "24 שעות"
                )
                if not ai:
                    ai = {"title_he": move["market"]["title"]}
                await notifier.send_big_move(move, ai)
                state.mark_move_alert(mid)

            # ── 3. Correlation Anomalies ──
            anomalies = find_correlation_anomalies(all_markets, state.price_history)
            for anomaly in anomalies[:2]:
                key = f"{anomaly['mover_market']['id']}:{anomaly['laggard_market']['id']}"
                if not state.can_alert_corr(key):
                    continue
                logger.info(f"🔗 CORRELATION: {anomaly['mover_market']['title']} vs {anomaly['laggard_market']['title']}")
                ai = await ai_analyze_correlation(
                    s,
                    anomaly["mover_market"], anomaly["laggard_market"],
                    anomaly["mover"]["current"], anomaly["laggard"]["current"],
                    anomaly["mover"]["move"], anomaly["laggard"]["move"],
                )
                if not ai:
                    ai = {"title_he": "אנומליית קורלציה"}
                await notifier.send_correlation(anomaly, ai)
                state.mark_corr_alert(key)

            # ── 4. Periodic snapshot (every ~30 min = 10 scans) ──
            if state.scan_count % 10 == 0:
                await notifier.send_market_snapshot(all_markets)

            # ── 5. Heartbeat (every ~6h = 120 scans) ──
            if state.scan_count % 120 == 0:
                await notifier.send_heartbeat(len(all_markets), state.scan_count)

            state.save()
            logger.info(f"Market scan done — {len(all_markets)} markets tracked")

    except Exception as e:
        logger.exception(f"Market scan error: {e}")
        try:
            await notifier.send(f"⚠️ שגיאה בסריקת שווקים: {str(e)[:300]}")
        except Exception:
            pass


async def news_scan():
    """News scan — runs every 5-10 minutes."""
    global state, notifier
    logger.info("═══ News Scan ═══")

    try:
        async with aiohttp.ClientSession() as s:
            # Fetch news
            news = await fetch_iran_news(s)

            # Filter out already seen
            new_items = [n for n in news if n["guid"] not in state.seen_news]
            if not new_items:
                logger.info("No new Iran news")
                return

            # Take top 5 newest
            new_items = new_items[:5]

            # Get current markets for context
            poly = await fetch_polymarket_iran(s)
            kalshi = await fetch_kalshi_iran(s)
            all_markets = poly + kalshi

            # AI analysis
            logger.info(f"📰 {len(new_items)} new Iran news items — analyzing...")
            ai = await ai_analyze_news(s, new_items, all_markets)

            if ai:
                await notifier.send_news(ai, new_items)
            else:
                # Fallback: send raw
                for item in new_items[:2]:
                    msg = (
                        f"📰 חדשות איראן\n\n"
                        f"📌 {item['title']}\n"
                        f"מקור: {item['source']}\n"
                        f"🔗 {item['link']}"
                    )
                    await notifier.send(msg)

            # Mark as seen
            for item in new_items:
                state.seen_news.append(item["guid"])

            state.last_news_check = datetime.now(timezone.utc).isoformat()
            state.save()

    except Exception as e:
        logger.exception(f"News scan error: {e}")


# ═══════════════════════════════════════════════════════════
# ENTRY POINT
# ═══════════════════════════════════════════════════════════

async def main():
    global state, notifier

    state = State()
    notifier = Notifier()

    logger.info("🇮🇷 Starting Iran Market Intelligence Bot...")

    # Initial fetch to show market count
    async with aiohttp.ClientSession() as s:
        poly = await fetch_polymarket_iran(s)
        kalshi = await fetch_kalshi_iran(s)
        initial_count = len(poly) + len(kalshi)
        # Record initial prices
        for m in poly + kalshi:
            state.record_price(m["id"], m["yes_price"])
        state.save()

    await notifier.send_startup(initial_count)

    if "--once" in sys.argv:
        await market_scan()
        await news_scan()
        return

    # Set up scheduler with different intervals
    scheduler = AsyncIOScheduler()

    scheduler.add_job(
        market_scan,
        IntervalTrigger(minutes=MARKET_SCAN_MINUTES),
        id="market_scan",
        name="Iran Market Scan",
        max_instances=1,
        misfire_grace_time=120,
    )

    scheduler.add_job(
        news_scan,
        IntervalTrigger(minutes=NEWS_SCAN_MINUTES),
        id="news_scan",
        name="Iran News Scan",
        max_instances=1,
        misfire_grace_time=120,
    )

    scheduler.start()

    # Run first scans immediately
    await market_scan()
    await asyncio.sleep(5)
    await news_scan()

    logger.info(f"Scheduler active — Markets every {MARKET_SCAN_MINUTES}min, "
                f"News every {NEWS_SCAN_MINUTES}min")

    stop = asyncio.Event()

    def handle_sig(sig, frame):
        logger.info("Shutting down...")
        stop.set()

    signal.signal(signal.SIGINT, handle_sig)
    signal.signal(signal.SIGTERM, handle_sig)

    await stop.wait()
    scheduler.shutdown(wait=False)
    logger.info("Bot stopped.")


if __name__ == "__main__":
    asyncio.run(main())
