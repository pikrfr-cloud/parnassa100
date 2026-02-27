#!/usr/bin/env python3
"""
🚀 בוט מודיעין שווקים — גרסת AI
==================================
מנטר Polymarket, Kalshi ו-RSS feeds.
משתמש ב-Claude AI לתרגום, ניתוח השפעה, סיכום ודירוג.
שולח התראות בעברית לטלגרם.
"""

import asyncio
import json
import logging
import os
import re
import signal
import sys
from datetime import datetime, timezone
from difflib import SequenceMatcher
from typing import Any, Optional

import aiohttp
import feedparser
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger
from dotenv import load_dotenv
from telegram import Bot
from telegram.error import TelegramError, RetryAfter

load_dotenv()

# ═══════════════════════════════════════════════════════════
# CONFIG
# ═══════════════════════════════════════════════════════════

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "")
CHECK_INTERVAL = int(os.getenv("CHECK_INTERVAL_MINUTES", "120"))
ALERT_THRESHOLD = int(os.getenv("ALERT_THRESHOLD_BPS", "15"))
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
STATE_FILE = os.getenv("STATE_FILE", "/data/bot_state.json")

POLYMARKET_API = "https://gamma-api.polymarket.com"
KALSHI_API = "https://api.elections.kalshi.com/trade-api/v2"
CLAUDE_API = "https://api.anthropic.com/v1/messages"
CLAUDE_MODEL = "claude-sonnet-4-20250514"

RSS_FEEDS = {
    "central_banks": [
        {"name": "Federal Reserve", "url": "https://www.federalreserve.gov/feeds/press_all.xml"},
        {"name": "ECB", "url": "https://www.ecb.europa.eu/rss/press.html"},
    ],
    "news": [
        {"name": "CoinDesk", "url": "https://www.coindesk.com/arc/outboundfeeds/rss/"},
        {"name": "Politico", "url": "https://rss.politico.com/politics-news.xml"},
    ],
    "legislation": [
        {"name": "US Congress", "url": "https://www.govinfo.gov/rss/bills.xml"},
    ],
}

# ═══════════════════════════════════════════════════════════
# LOGGING
# ═══════════════════════════════════════════════════════════

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s │ %(levelname)-7s │ %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("bot")
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("telegram").setLevel(logging.WARNING)
logging.getLogger("apscheduler").setLevel(logging.WARNING)

# ═══════════════════════════════════════════════════════════
# CLAUDE AI ENGINE
# ═══════════════════════════════════════════════════════════

AI_SYSTEM_PROMPT = """אתה אנליסט מודיעין שווקי הימורים מומחה. התפקיד שלך:

1. לתרגם כותרות וסיכומים מאנגלית לעברית בצורה טבעית ומדויקת
2. לנתח את ההשפעה על שווקי הימורים (Polymarket, Kalshi)
3. לדרג את רמת ההשפעה
4. לתת המלצות ספציפיות

ענה תמיד ב-JSON בלבד, בלי backticks, בלי הסברים נוספים."""

async def ask_claude(session: aiohttp.ClientSession, prompt: str, max_tokens: int = 1000) -> Optional[str]:
    """Send a prompt to Claude and get a response."""
    if not ANTHROPIC_API_KEY:
        logger.warning("No ANTHROPIC_API_KEY — skipping AI analysis")
        return None

    headers = {
        "x-api-key": ANTHROPIC_API_KEY,
        "anthropic-version": "2023-06-01",
        "content-type": "application/json",
    }
    payload = {
        "model": CLAUDE_MODEL,
        "max_tokens": max_tokens,
        "system": AI_SYSTEM_PROMPT,
        "messages": [{"role": "user", "content": prompt}],
    }

    try:
        async with session.post(CLAUDE_API, json=payload, headers=headers,
                                timeout=aiohttp.ClientTimeout(total=30)) as r:
            if r.status != 200:
                error_text = await r.text()
                logger.error(f"Claude API error {r.status}: {error_text[:200]}")
                return None
            data = await r.json()
            content = data.get("content", [])
            if content and content[0].get("type") == "text":
                return content[0]["text"]
    except Exception as e:
        logger.error(f"Claude API call failed: {e}")
    return None


async def ai_analyze_gap(session: aiohttp.ClientSession, gap: dict) -> dict:
    """Use Claude to analyze a price gap between platforms."""
    prompt = f"""נתח את הפער הבא בין שני שווקי הימורים:

שוק: {gap['name']}
קטגוריה: {gap['cat']}
Polymarket: {gap['poly']}%
Kalshi: {gap['kalshi']}%
פער: {gap['gap']} נקודות בסיס
כיוון: {'Polymarket גבוה יותר' if gap['poly'] > gap['kalshi'] else 'Kalshi גבוה יותר'}

החזר JSON בפורמט הזה בלבד:
{{
    "title_he": "כותרת השוק בעברית",
    "summary": "סיכום קצר של 1-2 משפטים מה קורה בשוק הזה ולמה יש פער",
    "impact_level": "🔴 גבוהה" או "🟡 בינונית" או "🟢 נמוכה",
    "affected_markets": ["רשימת שווקים מושפעים"],
    "recommendation": "המלצה ספציפית: האם כדאי לשים לב לצד מסוים, או שזה פער טכני",
    "why_gap": "הסבר קצר למה כנראה יש פער"
}}"""

    result = await ask_claude(session, prompt)
    if result:
        try:
            clean = result.strip()
            if clean.startswith("```"):
                clean = re.sub(r"```json?|```", "", clean).strip()
            return json.loads(clean)
        except json.JSONDecodeError:
            logger.warning(f"Failed to parse Claude gap response: {result[:100]}")
    return {}


async def ai_analyze_move(session: aiohttp.ClientSession, move: dict) -> dict:
    """Use Claude to analyze a big price move."""
    prompt = f"""נתח את התנועה הגדולה הבאה בשוק הימורים:

שוק: {move['name']}
קטגוריה: {move['cat']}
מקור: {move['src']}
מחיר קודם: {move['old']}%
מחיר נוכחי: {move['new']}%
תנועה: {move['delta']} נקודות בסיס
טווח זמן: {move['tf']}

החזר JSON בפורמט הזה בלבד:
{{
    "title_he": "כותרת השוק בעברית",
    "summary": "סיכום קצר: מה כנראה גרם לתנועה הזו",
    "impact_level": "🔴 גבוהה" או "🟡 בינונית" או "🟢 נמוכה",
    "affected_markets": ["שווקי הימורים אחרים שעלולים להיות מושפעים"],
    "recommendation": "המלצה: האם זו הזדמנות, תיקון טכני, או תגובה לחדשות",
    "direction_analysis": "האם התנועה צפויה להמשך או לחזור אחורה"
}}"""

    result = await ask_claude(session, prompt)
    if result:
        try:
            clean = result.strip()
            if clean.startswith("```"):
                clean = re.sub(r"```json?|```", "", clean).strip()
            return json.loads(clean)
        except json.JSONDecodeError:
            logger.warning(f"Failed to parse Claude move response: {result[:100]}")
    return {}


async def ai_analyze_rss(session: aiohttp.ClientSession, item: dict) -> dict:
    """Use Claude to translate and analyze an RSS news item."""
    prompt = f"""תרגם ונתח את הידיעה הבאה:

מקור: {item['feed']}
קטגוריה: {item['cat']}
כותרת: {item['title']}
סיכום: {item['summary'][:500]}

החזר JSON בפורמט הזה בלבד:
{{
    "title_he": "הכותרת מתורגמת לעברית בצורה טבעית",
    "summary_he": "סיכום הידיעה בעברית ב-2-3 משפטים",
    "impact_level": "🔴 גבוהה" או "🟡 בינונית" או "🟢 נמוכה",
    "affected_markets": ["רשימת שווקי הימורים ספציפיים שמושפעים"],
    "recommendation": "המלצה: מה לעקוב אחריו בשווקי ההימורים בעקבות הידיעה",
    "urgency": "דחוף" או "חשוב" או "לידיעה"
}}"""

    result = await ask_claude(session, prompt)
    if result:
        try:
            clean = result.strip()
            if clean.startswith("```"):
                clean = re.sub(r"```json?|```", "", clean).strip()
            return json.loads(clean)
        except json.JSONDecodeError:
            logger.warning(f"Failed to parse Claude RSS response: {result[:100]}")
    return {}


# ═══════════════════════════════════════════════════════════
# FALLBACK (when no API key)
# ═══════════════════════════════════════════════════════════

CATEGORY_KEYWORDS = {
    "crypto": ["bitcoin", "btc", "eth", "ethereum", "crypto", "solana", "defi"],
    "politics": ["president", "election", "trump", "biden", "senate", "congress", "vote", "governor"],
    "macro": ["fed", "rate", "inflation", "gdp", "recession", "treasury", "cpi", "tariff", "interest rate"],
    "sports": ["nba", "nfl", "mlb", "super bowl", "world cup", "champion"],
    "tech": ["ai", "openai", "apple", "google", "microsoft", "tesla", "spacex"],
    "climate": ["hurricane", "earthquake", "temperature", "climate", "wildfire"],
}

CATEGORY_HEBREW = {
    "crypto": "קריפטו", "politics": "פוליטיקה", "macro": "מאקרו/ריביות",
    "sports": "ספורט", "tech": "טכנולוגיה", "climate": "אקלים", "other": "אחר",
}

RSS_KEYWORDS = [
    "interest rate", "rate decision", "monetary policy", "inflation", "cpi", "gdp",
    "recession", "fed", "ecb", "fomc", "bitcoin", "crypto", "stablecoin",
    "election", "legislation", "bill pass", "executive order", "sanction", "tariff",
    "war", "conflict", "ceasefire", "breaking", "urgent", "surprise",
]


def classify(title):
    t = title.lower()
    for cat, kws in CATEGORY_KEYWORDS.items():
        if any(k in t for k in kws):
            return cat
    return "other"


def normalize(title):
    t = title.lower().strip()
    t = re.sub(r"^(will|is|does|has|can)\s+", "", t)
    t = re.sub(r"\?$", "", t)
    return re.sub(r"\s+", " ", t)


def cat_he(cat):
    return CATEGORY_HEBREW.get(cat, "אחר")


# ═══════════════════════════════════════════════════════════
# POLYMARKET
# ═══════════════════════════════════════════════════════════

async def fetch_polymarket(session, limit=100, min_vol=10000):
    events = []
    try:
        url = f"{POLYMARKET_API}/events"
        params = {"active": "true", "closed": "false", "limit": limit,
                  "order": "volume24hr", "ascending": "false"}
        async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=30)) as r:
            if r.status != 200:
                logger.error(f"Polymarket API: {r.status}")
                return events
            data = await r.json()

        for ev in data:
            markets = ev.get("markets", [])
            if not markets:
                continue
            vol = sum(float(m.get("volume", 0) or 0) for m in markets)
            if vol < min_vol:
                continue

            primary = markets[0]
            outcomes = []
            op = primary.get("outcomePrices", "")
            if op:
                try:
                    prices = json.loads(op)
                    if len(prices) > 0:
                        outcomes.append({"name": "Yes", "price": float(prices[0])})
                    if len(prices) > 1:
                        outcomes.append({"name": "No", "price": float(prices[1])})
                except (json.JSONDecodeError, IndexError, ValueError):
                    pass

            if not outcomes:
                yp = primary.get("bestAsk") or primary.get("lastTradePrice")
                if yp:
                    y = float(yp)
                    outcomes = [{"name": "Yes", "price": y}, {"name": "No", "price": 1.0 - y}]

            if not outcomes:
                continue

            title = ev.get("title", "Unknown")
            slug = ev.get("slug", "")
            events.append({
                "id": f"poly_{ev.get('id', '')}",
                "title": title,
                "category": classify(title),
                "yes_price": outcomes[0]["price"],
                "volume": vol,
                "url": f"https://polymarket.com/event/{slug}" if slug else "",
                "source": "Polymarket",
            })

        logger.info(f"Polymarket: {len(events)} markets")
    except Exception as e:
        logger.error(f"Polymarket error: {e}")
    return events


# ═══════════════════════════════════════════════════════════
# KALSHI
# ═══════════════════════════════════════════════════════════

async def fetch_kalshi(session, limit=200, min_vol=100):
    markets = []
    try:
        url = f"{KALSHI_API}/markets"
        params = {"limit": limit, "status": "open"}
        headers = {"Accept": "application/json"}
        async with session.get(url, params=params, headers=headers,
                               timeout=aiohttp.ClientTimeout(total=30)) as r:
            if r.status != 200:
                logger.warning(f"Kalshi API: {r.status}")
                return markets
            data = await r.json()

        cat_map = {"Politics": "politics", "Economics": "macro", "Crypto": "crypto",
                   "Climate and Weather": "climate", "Tech and Science": "tech",
                   "Sports": "sports", "Finance": "macro"}

        for m in data.get("markets", []):
            vol = m.get("volume", 0) or 0
            if vol < min_vol:
                continue
            yp = (m.get("yes_ask", 0) or m.get("last_price", 0) or 0) / 100.0
            ticker = m.get("ticker", "")
            title = m.get("title", "Unknown")
            markets.append({
                "id": f"kalshi_{m.get('id', '')}",
                "title": title,
                "category": cat_map.get(m.get("category", ""), "other"),
                "yes_price": yp,
                "volume": vol,
                "url": f"https://kalshi.com/markets/{ticker.lower()}" if ticker else "",
                "source": "Kalshi",
                "subtitle": m.get("subtitle"),
            })

        logger.info(f"Kalshi: {len(markets)} markets")
    except Exception as e:
        logger.error(f"Kalshi error: {e}")
    return markets


# ═══════════════════════════════════════════════════════════
# RSS
# ═══════════════════════════════════════════════════════════

async def fetch_rss(session, since=None):
    items = []
    for cat, feeds in RSS_FEEDS.items():
        for fi in feeds:
            try:
                async with session.get(fi["url"], timeout=aiohttp.ClientTimeout(total=20)) as r:
                    if r.status != 200:
                        continue
                    content = await r.text()
                feed = feedparser.parse(content)
                for entry in feed.entries[:15]:
                    title = entry.get("title", "")
                    summary = entry.get("summary", entry.get("description", ""))[:500]
                    link = entry.get("link", "")
                    guid = entry.get("id", entry.get("guid", link))

                    pub = None
                    if hasattr(entry, "published_parsed") and entry.published_parsed:
                        try:
                            pub = datetime(*entry.published_parsed[:6], tzinfo=timezone.utc)
                        except (TypeError, ValueError):
                            pass

                    if since and pub and pub < since:
                        continue

                    text = f"{title} {summary}".lower()
                    if not any(kw in text for kw in RSS_KEYWORDS):
                        continue

                    items.append({
                        "feed": fi["name"], "cat": cat, "title": title,
                        "summary": summary, "link": link, "guid": guid, "pub": pub,
                    })
            except Exception as e:
                logger.warning(f"RSS {fi['name']}: {e}")

    logger.info(f"RSS: {len(items)} relevant items")
    return items


# ═══════════════════════════════════════════════════════════
# ANALYZER
# ═══════════════════════════════════════════════════════════

def match_and_find_gaps(poly, kalshi, threshold=None):
    if threshold is None:
        threshold = ALERT_THRESHOLD
    alerts = []
    used = set()

    for pe in poly:
        best, best_score = None, 0
        for km in kalshi:
            if km["id"] in used:
                continue
            score = SequenceMatcher(None, normalize(pe["title"]), normalize(km["title"])).ratio()
            if pe["category"] == km["category"] and pe["category"] != "other":
                score += 0.1
            if km.get("subtitle"):
                score = max(score, SequenceMatcher(None, normalize(pe["title"]), normalize(km["subtitle"])).ratio())
            if score > best_score:
                best_score, best = score, km

        if best and best_score >= 0.55:
            used.add(best["id"])
            pp, kp = pe["yes_price"] * 100, best["yes_price"] * 100
            gap = abs(pp - kp) * 100
            if gap >= threshold:
                alerts.append({
                    "name": pe["title"], "cat": pe["category"],
                    "poly": round(pp, 1), "kalshi": round(kp, 1),
                    "gap": round(gap),
                    "dir": "Poly > Kalshi" if pp > kp else "Kalshi > Poly",
                    "poly_url": pe["url"], "kalshi_url": best["url"],
                })
    alerts.sort(key=lambda a: a["gap"], reverse=True)
    return alerts


def find_big_moves(current, previous, info, threshold=None):
    if threshold is None:
        threshold = ALERT_THRESHOLD
    alerts = []
    for mid, new_p in current.items():
        old_p = previous.get(mid)
        if old_p is None:
            continue
        delta = abs(new_p - old_p) * 100 * 100
        if delta >= threshold:
            i = info.get(mid, {})
            alerts.append({
                "name": i.get("title", mid), "cat": i.get("category", "other"),
                "src": i.get("source", "?"), "old": round(old_p * 100, 1),
                "new": round(new_p * 100, 1), "delta": round(delta),
                "tf": f"{CHECK_INTERVAL} דקות", "url": i.get("url", ""),
            })
    alerts.sort(key=lambda a: a["delta"], reverse=True)
    return alerts


# ═══════════════════════════════════════════════════════════
# STATE PERSISTENCE
# ═══════════════════════════════════════════════════════════

class State:
    def __init__(self):
        self.prices = {}
        self.info = {}
        self.seen_guids = []
        self.last_run = None
        self.run_count = 0
        self._load()

    def _load(self):
        if os.path.exists(STATE_FILE):
            try:
                with open(STATE_FILE) as f:
                    s = json.load(f)
                self.prices = s.get("prices", {})
                self.info = s.get("info", {})
                self.seen_guids = s.get("seen_guids", [])
                self.last_run = s.get("last_run")
                self.run_count = s.get("run_count", 0)
                logger.info(f"State loaded — run #{self.run_count}")
            except Exception as e:
                logger.warning(f"State load failed: {e}")

    def save(self):
        try:
            os.makedirs(os.path.dirname(STATE_FILE), exist_ok=True)
            with open(STATE_FILE, "w") as f:
                json.dump({"prices": self.prices, "info": self.info,
                           "seen_guids": self.seen_guids[-3000:],
                           "last_run": self.last_run, "run_count": self.run_count}, f)
        except Exception as e:
            logger.error(f"State save failed: {e}")

    def get_last_run_dt(self):
        if self.last_run:
            try:
                return datetime.fromisoformat(self.last_run)
            except (TypeError, ValueError):
                pass
        return None


# ═══════════════════════════════════════════════════════════
# TELEGRAM (HEBREW + AI)
# ═══════════════════════════════════════════════════════════

class Notifier:
    def __init__(self):
        self.bot = Bot(token=TELEGRAM_BOT_TOKEN)
        self.chat = TELEGRAM_CHAT_ID

    async def send(self, text):
        try:
            await self.bot.send_message(chat_id=self.chat, text=text, disable_web_page_preview=True)
            return True
        except RetryAfter as e:
            await asyncio.sleep(e.retry_after)
            return await self.send(text)
        except TelegramError as e:
            logger.error(f"TG error: {e}")
            return False

    async def startup(self):
        ai_status = "✅ מופעל" if ANTHROPIC_API_KEY else "❌ לא מוגדר (עובד עם כללים בסיסיים)"
        msg = (
            "🚀 בוט מודיעין שווקים הופעל!\n\n"
            "🔍 מצב: פעיל\n"
            f"⏰ תדירות: כל {CHECK_INTERVAL} דקות\n"
            "📊 מקורות: Polymarket, Kalshi, RSS\n"
            f'🎯 סף התראה: {ALERT_THRESHOLD}+ נ"ב\n'
            "🌐 שפה: עברית\n"
            f"🧠 מנוע AI: {ai_status}\n"
            "🎰 כולל ניתוח השפעה על הימורים"
        )
        await self.send(msg)

    async def gap_alert(self, gap: dict, ai: dict):
        title_he = ai.get("title_he", gap["name"])
        summary = ai.get("summary", "")
        impact = ai.get("impact_level", "🟡 בינונית")
        affected = ai.get("affected_markets", [])
        rec = ai.get("recommendation", "")
        why = ai.get("why_gap", "")

        affected_str = ", ".join(affected[:4]) if affected else "—"

        msg = (
            f"🔔 התראת פער\n\n"
            f"📊 {title_he}\n"
            f"🏷️ קטגוריה: {cat_he(gap['cat'])}\n\n"
            f"Polymarket: {gap['poly']}%\n"
            f"Kalshi: {gap['kalshi']}%\n"
            f'📐 פער: {gap["gap"]} נ"ב\n\n'
        )
        if summary:
            msg += f"📝 סיכום: {summary}\n\n"
        if why:
            msg += f"❓ למה יש פער: {why}\n\n"

        msg += (
            f"🎰 השפעה על הימורים:\n"
            f"  {impact}\n"
            f"  שווקים מושפעים: {affected_str}\n"
        )
        if rec:
            msg += f"\n💡 המלצה: {rec}\n"

        msg += (
            f"\n🔗 Poly: {gap['poly_url']}\n"
            f"🔗 Kalshi: {gap['kalshi_url']}"
        )
        await self.send(msg)

    async def move_alert(self, move: dict, ai: dict):
        title_he = ai.get("title_he", move["name"])
        summary = ai.get("summary", "")
        impact = ai.get("impact_level", "🟡 בינונית")
        affected = ai.get("affected_markets", [])
        rec = ai.get("recommendation", "")
        direction = ai.get("direction_analysis", "")

        affected_str = ", ".join(affected[:4]) if affected else "—"

        arrow = "📈" if move["new"] > move["old"] else "📉"

        msg = (
            f"⚡ תנועה גדולה\n\n"
            f"📊 {title_he}\n"
            f"🏷️ קטגוריה: {cat_he(move['cat'])}\n"
            f"מקור: {move['src']}\n\n"
            f"{arrow} לפני: {move['old']}% → עכשיו: {move['new']}%\n"
            f'📐 תנועה: {move["delta"]} נ"ב\n'
            f"⏱️ טווח: {move['tf']}\n\n"
        )
        if summary:
            msg += f"📝 מה קרה: {summary}\n\n"

        msg += (
            f"🎰 השפעה על הימורים:\n"
            f"  {impact}\n"
            f"  שווקים מושפעים: {affected_str}\n"
        )
        if direction:
            msg += f"\n🔮 תחזית כיוון: {direction}\n"
        if rec:
            msg += f"💡 המלצה: {rec}\n"

        msg += f"\n🔗 {move['url']}"
        await self.send(msg)

    async def rss_alert(self, item: dict, ai: dict):
        title_he = ai.get("title_he", item["title"])
        summary_he = ai.get("summary_he", item["summary"][:200])
        impact = ai.get("impact_level", "🟡 בינונית")
        affected = ai.get("affected_markets", [])
        rec = ai.get("recommendation", "")
        urgency = ai.get("urgency", "לידיעה")

        affected_str = ", ".join(affected[:4]) if affected else "—"

        urgency_emoji = {"דחוף": "🚨", "חשוב": "⚠️", "לידיעה": "ℹ️"}.get(urgency, "ℹ️")

        msg = (
            f"📰 {item['feed']} — עדכון חדש\n"
            f"{urgency_emoji} דחיפות: {urgency}\n\n"
            f"📌 {title_he}\n\n"
            f"{summary_he}\n\n"
            f"🎰 השפעה על הימורים:\n"
            f"  {impact}\n"
            f"  שווקים מושפעים: {affected_str}\n"
        )
        if rec:
            msg += f"\n💡 המלצה: {rec}\n"

        msg += f"\n🔗 {item['link']}"
        await self.send(msg)

    async def heartbeat(self, mc, fc):
        ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
        ai_ok = "✅" if ANTHROPIC_API_KEY else "❌"
        msg = (
            f"💓 הבוט פעיל — {ts}\n"
            f"שווקים במעקב: {mc}\n"
            f"פידים במעקב: {fc}\n"
            f"מנוע AI: {ai_ok}"
        )
        await self.send(msg)


# ═══════════════════════════════════════════════════════════
# MAIN SCAN
# ═══════════════════════════════════════════════════════════

async def scan(state, notifier):
    logger.info(f"═══ Scan #{state.run_count + 1} ═══")
    sent = 0

    try:
        async with aiohttp.ClientSession() as s:
            # ── Fetch markets ──
            poly = await fetch_polymarket(s)
            kalshi_data = await fetch_kalshi(s)

            # ── Gap detection ──
            gaps = match_and_find_gaps(poly, kalshi_data)
            for gap in gaps[:5]:
                logger.info(f"📊 GAP: {gap['name']} — {gap['gap']} bps")
                ai = await ai_analyze_gap(s, gap)
                if not ai:
                    ai = {"title_he": gap["name"], "impact_level": "🟡 בינונית",
                           "affected_markets": [], "recommendation": "", "summary": "", "why_gap": ""}
                await notifier.gap_alert(gap, ai)
                sent += 1

            # ── Price tracking ──
            current = {}
            info = {}
            for m in poly + kalshi_data:
                current[m["id"]] = m["yes_price"]
                info[m["id"]] = {"title": m["title"], "category": m["category"],
                                 "source": m["source"], "url": m["url"]}

            # ── Big moves ──
            moves = find_big_moves(current, state.prices, {**state.info, **info})
            for move in moves[:5]:
                logger.info(f"⚡ MOVE: {move['name']} — {move['delta']} bps")
                ai = await ai_analyze_move(s, move)
                if not ai:
                    ai = {"title_he": move["name"], "impact_level": "🟡 בינונית",
                           "affected_markets": [], "recommendation": "", "summary": "",
                           "direction_analysis": ""}
                await notifier.move_alert(move, ai)
                sent += 1

            state.prices = current
            state.info = info

            # ── RSS ──
            rss = await fetch_rss(s, since=state.get_last_run_dt())
            rss_sent = 0
            for item in rss:
                if item["guid"] in state.seen_guids:
                    continue
                if rss_sent >= 3:
                    break
                logger.info(f"📰 RSS: [{item['feed']}] {item['title']}")
                ai = await ai_analyze_rss(s, item)
                if not ai:
                    ai = {"title_he": item["title"], "summary_he": item["summary"][:200],
                           "impact_level": "🟡 בינונית", "affected_markets": [],
                           "recommendation": "", "urgency": "לידיעה"}
                await notifier.rss_alert(item, ai)
                state.seen_guids.append(item["guid"])
                sent += 1
                rss_sent += 1

            # ── Heartbeat ──
            if state.run_count > 0 and state.run_count % 12 == 0:
                fc = sum(len(v) for v in RSS_FEEDS.values())
                await notifier.heartbeat(len(current), fc)

            state.run_count += 1
            state.last_run = datetime.now(timezone.utc).isoformat()
            state.save()

            logger.info(f"Done — {sent} alerts sent")

    except Exception as e:
        logger.exception(f"Scan error: {e}")
        try:
            await notifier.send(f"⚠️ שגיאה: {str(e)[:500]}")
        except Exception:
            pass


# ═══════════════════════════════════════════════════════════
# ENTRY POINT
# ═══════════════════════════════════════════════════════════

async def main():
    state = State()
    notifier = Notifier()

    logger.info("🚀 Starting...")
    await notifier.startup()

    if "--once" in sys.argv:
        await scan(state, notifier)
        return

    scheduler = AsyncIOScheduler()
    scheduler.add_job(scan, IntervalTrigger(minutes=CHECK_INTERVAL),
                      args=[state, notifier], id="scan", max_instances=1,
                      misfire_grace_time=300)
    scheduler.start()

    await scan(state, notifier)

    logger.info(f"Scheduler active — every {CHECK_INTERVAL} min")
    stop = asyncio.Event()

    def handle_sig(sig, frame):
        logger.info("Shutting down...")
        stop.set()

    signal.signal(signal.SIGINT, handle_sig)
    signal.signal(signal.SIGTERM, handle_sig)

    await stop.wait()
    scheduler.shutdown(wait=False)


if __name__ == "__main__":
    asyncio.run(main())
