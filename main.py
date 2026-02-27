#!/usr/bin/env python3
"""
🚀 בוט מודיעין שווקים — גרסה עברית
=====================================
מנטר Polymarket, Kalshi ו-RSS feeds.
שולח התראות בעברית לטלגרם עם ניתוח השפעה על הימורים.
"""

import asyncio
import json
import logging
import os
import re
import signal
import sys
from dataclasses import dataclass
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
CHECK_INTERVAL = int(os.getenv("CHECK_INTERVAL_MINUTES", "120"))
ALERT_THRESHOLD = int(os.getenv("ALERT_THRESHOLD_BPS", "15"))
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
STATE_FILE = os.getenv("STATE_FILE", "/data/bot_state.json")

POLYMARKET_API = "https://gamma-api.polymarket.com"
KALSHI_API = "https://api.elections.kalshi.com/trade-api/v2"

RSS_FEEDS = {
    "central_banks": [
        {"name": "הפדרל ריזרב", "url": "https://www.federalreserve.gov/feeds/press_all.xml"},
        {"name": "הבנק האירופי", "url": "https://www.ecb.europa.eu/rss/press.html"},
    ],
    "news": [
        {"name": "CoinDesk", "url": "https://www.coindesk.com/arc/outboundfeeds/rss/"},
        {"name": "Politico", "url": "https://rss.politico.com/politics-news.xml"},
    ],
    "legislation": [
        {"name": "הקונגרס האמריקאי", "url": "https://www.govinfo.gov/rss/bills.xml"},
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
# CATEGORY & KEYWORD CONFIG
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
    "crypto": "קריפטו",
    "politics": "פוליטיקה",
    "macro": "מאקרו/ריביות",
    "sports": "ספורט",
    "tech": "טכנולוגיה",
    "climate": "אקלים",
    "other": "אחר",
}

RSS_KEYWORDS = [
    "interest rate", "rate decision", "monetary policy", "inflation", "cpi", "gdp",
    "recession", "fed", "ecb", "fomc", "bitcoin", "crypto", "stablecoin",
    "election", "legislation", "bill pass", "executive order", "sanction", "tariff",
    "war", "conflict", "ceasefire", "breaking", "urgent", "surprise",
]

# ═══════════════════════════════════════════════════════════
# TRANSLATION DICTIONARY (common market terms EN→HE)
# ═══════════════════════════════════════════════════════════

TERM_TRANSLATIONS = {
    # Politics
    "president": "נשיא", "election": "בחירות", "senate": "סנאט",
    "congress": "קונגרס", "vote": "הצבעה", "governor": "מושל",
    "impeach": "הדחה", "democrat": "דמוקרטים", "republican": "רפובליקנים",
    "white house": "הבית הלבן", "supreme court": "בית המשפט העליון",
    # Macro
    "interest rate": "ריבית", "rate cut": "הורדת ריבית", "rate hike": "העלאת ריבית",
    "inflation": "אינפלציה", "recession": "מיתון", "gdp": "תוצר מקומי גולמי",
    "unemployment": "אבטלה", "tariff": "מכס", "trade war": "מלחמת סחר",
    "debt ceiling": "תקרת חוב", "federal reserve": "הפדרל ריזרב",
    "central bank": "בנק מרכזי", "monetary policy": "מדיניות מוניטרית",
    # Crypto
    "bitcoin": "ביטקוין", "ethereum": "אתריום", "crypto": "קריפטו",
    "stablecoin": "מטבע יציב", "halving": "חצייה", "etf": "תעודת סל",
    "token": "טוקן", "blockchain": "בלוקצ'יין",
    # Geopolitics
    "war": "מלחמה", "ceasefire": "הפסקת אש", "conflict": "סכסוך",
    "sanction": "סנקציה", "invasion": "פלישה", "missile": "טיל",
    "nato": "נאט\"ו",
    # General
    "yes": "כן", "no": "לא", "will": "האם",
    "before": "לפני", "after": "אחרי", "by": "עד",
    "win": "ניצחון", "lose": "הפסד", "above": "מעל", "below": "מתחת",
}


def translate_title(title: str) -> str:
    """Translate an English market title to Hebrew (keyword-based)."""
    result = title
    for en, he in sorted(TERM_TRANSLATIONS.items(), key=lambda x: -len(x[0])):
        pattern = re.compile(re.escape(en), re.IGNORECASE)
        result = pattern.sub(he, result)
    return result


# ═══════════════════════════════════════════════════════════
# BETTING IMPACT ANALYSIS
# ═══════════════════════════════════════════════════════════

# Map event keywords to related betting markets and impact level
IMPACT_RULES = [
    # Macro / Central Banks
    {
        "triggers": ["rate cut", "rate decision", "interest rate", "fed", "fomc", "monetary policy", "dovish", "hawkish"],
        "markets": ["שוקי ריביות (Kalshi/Poly)", "אג\"ח ממשלתי", "מט\"ח (דולר)", "מניות צמיחה"],
        "level": "🔴 גבוהה",
        "note": "החלטות ריבית משפיעות ישירות על שווקי התחזיות של ריביות, אג\"ח, ודולר",
    },
    {
        "triggers": ["inflation", "cpi", "pce"],
        "markets": ["שוקי ריביות", "הימורי מדיניות הפד", "סחורות"],
        "level": "🔴 גבוהה",
        "note": "נתוני אינפלציה מזיזים ציפיות ריבית ושווקי תחזיות",
    },
    {
        "triggers": ["recession", "gdp", "unemployment", "payroll", "jobs"],
        "markets": ["הימורי מיתון (Poly/Kalshi)", "שוקי מניות", "אג\"ח"],
        "level": "🟡 בינונית-גבוהה",
        "note": "נתוני תעסוקה וצמיחה משפיעים על הימורי מיתון והרגשת השוק",
    },
    {
        "triggers": ["tariff", "trade war", "trade deal", "import tax"],
        "markets": ["הימורי מלחמת סחר", "שווקי מניות בינלאומיים", "מט\"ח"],
        "level": "🟡 בינונית-גבוהה",
        "note": "מכסים יכולים לזעזע שווקים ולהשפיע על הימורי סחר ומט\"ח",
    },
    # Crypto
    {
        "triggers": ["bitcoin", "btc", "crypto", "ethereum", "eth"],
        "markets": ["הימורי מחיר ביטקוין", "הימורי ETF קריפטו", "אלטקוינים"],
        "level": "🟡 בינונית",
        "note": "חדשות קריפטו משפיעות על שווקי תחזיות מחירים ורגולציה",
    },
    {
        "triggers": ["etf approval", "sec crypto", "crypto regulation", "stablecoin bill"],
        "markets": ["הימורי אישור ETF", "הימורי רגולציה", "מחירי קריפטו"],
        "level": "🔴 גבוהה",
        "note": "החלטות רגולציה משנות את שוק הקריפטו באופן מהותי",
    },
    # Politics / Elections
    {
        "triggers": ["election", "poll", "primary", "ballot", "swing state"],
        "markets": ["הימורי בחירות (Poly/Kalshi)", "הימורי מדינות מפתח", "הימורי סנאט"],
        "level": "🔴 גבוהה",
        "note": "עדכוני בחירות משפיעים ישירות על שוקי ההימורים הפוליטיים",
    },
    {
        "triggers": ["impeach", "resign", "scandal", "indictment", "trial"],
        "markets": ["הימורי הדחה/התפטרות", "הימורי בחירות", "שוקי מניות"],
        "level": "🟡 בינונית-גבוהה",
        "note": "אירועים משפטיים/פוליטיים יכולים לשנות סיכויי מועמדים",
    },
    {
        "triggers": ["legislation", "bill pass", "executive order", "congress vote", "senate vote"],
        "markets": ["הימורי חקיקה", "הימורים ענפיים רלוונטיים"],
        "level": "🟡 בינונית",
        "note": "חקיקה חדשה יכולה לפתוח או לסגור שווקי הימורים",
    },
    # Geopolitics
    {
        "triggers": ["war", "invasion", "conflict", "attack", "missile", "military"],
        "markets": ["הימורי גיאופוליטיקה", "נפט וסחורות", "שוקי מניות", "מט\"ח"],
        "level": "🔴 גבוהה",
        "note": "אירועים צבאיים גורמים לתנודתיות חדה בכל השווקים",
    },
    {
        "triggers": ["ceasefire", "peace deal", "treaty", "negotiation"],
        "markets": ["הימורי הפסקת אש/שלום", "נפט", "שוקי מניות אזוריים"],
        "level": "🟡 בינונית-גבוהה",
        "note": "הפסקות אש והסכמי שלום מזיזים שווקי תחזיות גיאופוליטיים",
    },
    # Tech
    {
        "triggers": ["ai ", "artificial intelligence", "openai", "chatgpt", "agi"],
        "markets": ["הימורי AI (אבני דרך)", "מניות טכנולוגיה"],
        "level": "🟡 בינונית",
        "note": "פריצות דרך ב-AI משפיעות על הימורי אבני דרך טכנולוגיים",
    },
    {
        "triggers": ["spacex", "launch", "nasa", "mars", "rocket"],
        "markets": ["הימורי שיגורים/חלל", "הימורי SpaceX"],
        "level": "🟢 נמוכה-בינונית",
        "note": "אירועי חלל משפיעים על הימורי שיגור ספציפיים",
    },
    # Climate
    {
        "triggers": ["hurricane", "earthquake", "wildfire", "flood", "storm"],
        "markets": ["הימורי אקלים/מזג אוויר", "ביטוח", "סחורות חקלאיות"],
        "level": "🟡 בינונית",
        "note": "אירועי מזג אוויר קיצוניים משפיעים על הימורי אקלים וסחורות",
    },
    {
        "triggers": ["sanction", "embargo", "ban"],
        "markets": ["הימורי סנקציות", "נפט", "מט\"ח של מדינות מעורבות"],
        "level": "🟡 בינונית-גבוהה",
        "note": "סנקציות חדשות מזיזות שווקי אנרגיה והימורי גיאופוליטיקה",
    },
]


def analyze_impact(title: str, summary: str = "") -> str:
    """Analyze which betting markets could be affected and at what level."""
    text = f"{title} {summary}".lower()
    impacts = []

    for rule in IMPACT_RULES:
        if any(trigger in text for trigger in rule["triggers"]):
            impacts.append(rule)

    if not impacts:
        return ""

    # Deduplicate by level+note
    seen = set()
    unique = []
    for imp in impacts:
        key = imp["note"]
        if key not in seen:
            seen.add(key)
            unique.append(imp)

    lines = ["\n🎰 *השפעה על הימורים:*"]
    for imp in unique[:3]:  # Max 3 impacts per alert
        markets_str = ", ".join(imp["markets"][:4])
        lines.append(f"  {imp['level']} — {markets_str}")
        lines.append(f"  💡 {imp['note']}")

    return "\n".join(lines)


# ═══════════════════════════════════════════════════════════
# HELPERS
# ═══════════════════════════════════════════════════════════

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
                "title_he": translate_title(title),
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
                "title_he": translate_title(title),
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
                        "feed": fi["name"],
                        "cat": cat,
                        "title": title,
                        "title_he": translate_title(title),
                        "summary": summary,
                        "summary_he": translate_title(summary),
                        "link": link,
                        "guid": guid,
                        "pub": pub,
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
                    "name": pe["title"],
                    "name_he": pe.get("title_he", pe["title"]),
                    "cat": pe["category"],
                    "poly": round(pp, 1),
                    "kalshi": round(kp, 1),
                    "gap": round(gap),
                    "dir": "Poly גבוה יותר" if pp > kp else "Kalshi גבוה יותר",
                    "poly_url": pe["url"],
                    "kalshi_url": best["url"],
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
                "name": i.get("title", mid),
                "name_he": i.get("title_he", i.get("title", mid)),
                "cat": i.get("category", "other"),
                "src": i.get("source", "?"),
                "old": round(old_p * 100, 1),
                "new": round(new_p * 100, 1),
                "delta": round(delta),
                "tf": f"{CHECK_INTERVAL} דקות",
                "url": i.get("url", ""),
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
# TELEGRAM (HEBREW ONLY)
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
        msg = (
            "🚀 בוט מודיעין שווקים הופעל!\n\n"
            "🔍 מצב: פעיל\n"
            f"⏰ תדירות: כל {CHECK_INTERVAL} דקות\n"
            "📊 מקורות: Polymarket, Kalshi, RSS\n"
            f'🎯 סף התראה: {ALERT_THRESHOLD}+ נ"ב\n'
            "🌐 שפה: עברית\n"
            "🎰 כולל ניתוח השפעה על הימורים"
        )
        await self.send(msg)

    async def gap_alert(self, a):
        impact = analyze_impact(a["name"])
        msg = (
            f"🔔 התראת פער — {a['name_he']}\n\n"
            f"📊 שוק: {a['name_he']}\n"
            f"🏷️ קטגוריה: {cat_he(a['cat'])}\n\n"
            f"Polymarket: {a['poly']}%\n"
            f"Kalshi: {a['kalshi']}%\n"
            f'📐 פער: {a["gap"]} נ"ב\n'
            f"📈 כיוון: {a['dir']}\n"
            f"{impact}\n\n"
            f"🔗 Poly: {a['poly_url']}\n"
            f"🔗 Kalshi: {a['kalshi_url']}"
        )
        await self.send(msg)

    async def move_alert(self, a):
        impact = analyze_impact(a["name"])
        msg = (
            f"⚡ תנועה גדולה — {a['name_he']}\n\n"
            f"📊 {a['name_he']}\n"
            f"🏷️ קטגוריה: {cat_he(a['cat'])}\n"
            f"מקור: {a['src']}\n\n"
            f"לפני: {a['old']}% → עכשיו: {a['new']}%\n"
            f'📐 תנועה: {a["delta"]} נ"ב\n'
            f"⏱️ טווח: {a['tf']}\n"
            f"{impact}\n\n"
            f"🔗 {a['url']}"
        )
        await self.send(msg)

    async def rss_alert(self, item):
        impact = analyze_impact(item["title"], item.get("summary", ""))
        msg = (
            f"📰 {item['feed']} — עדכון חדש\n\n"
            f"📌 {item['title_he']}\n\n"
            f"{item['summary_he'][:300]}\n"
            f"{impact}\n\n"
            f"🔗 {item['link']}"
        )
        await self.send(msg)

    async def heartbeat(self, mc, fc):
        ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
        msg = f"💓 הבוט פעיל — {ts}\nשווקים במעקב: {mc}\nפידים במעקב: {fc}"
        await self.send(msg)


# ═══════════════════════════════════════════════════════════
# MAIN SCAN
# ═══════════════════════════════════════════════════════════

async def scan(state, notifier):
    logger.info(f"═══ Scan #{state.run_count + 1} ═══")
    sent = 0

    try:
        async with aiohttp.ClientSession() as s:
            # Fetch
            poly = await fetch_polymarket(s)
            kalshi_data = await fetch_kalshi(s)

            # Gap detection
            gaps = match_and_find_gaps(poly, kalshi_data)
            for a in gaps[:5]:
                logger.info(f"📊 GAP: {a['name']} — {a['gap']} bps")
                await notifier.gap_alert(a)
                sent += 1

            # Price tracking
            current = {}
            info = {}
            for m in poly + kalshi_data:
                current[m["id"]] = m["yes_price"]
                info[m["id"]] = {
                    "title": m["title"],
                    "title_he": m.get("title_he", m["title"]),
                    "category": m["category"],
                    "source": m["source"],
                    "url": m["url"],
                }

            # Big moves
            moves = find_big_moves(current, state.prices, {**state.info, **info})
            for a in moves[:5]:
                logger.info(f"⚡ MOVE: {a['name']} — {a['delta']} bps")
                await notifier.move_alert(a)
                sent += 1

            state.prices = current
            state.info = info

            # RSS
            rss = await fetch_rss(s, since=state.get_last_run_dt())
            rss_sent = 0
            for item in rss:
                if item["guid"] in state.seen_guids:
                    continue
                if rss_sent >= 3:
                    break
                logger.info(f"📰 RSS: [{item['feed']}] {item['title']}")
                await notifier.rss_alert(item)
                state.seen_guids.append(item["guid"])
                sent += 1
                rss_sent += 1

            # Heartbeat every 12 runs (~24h)
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
