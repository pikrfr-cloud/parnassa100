#!/usr/bin/env python3
"""
🚀 Market Intelligence Bot — Single File Edition
==================================================
Monitors Polymarket, Kalshi, and RSS feeds.
Sends multilingual alerts (EN/HE/FR) to Telegram.

Usage:
    python main.py              # Run continuously
    python main.py --once       # Single scan
"""

import asyncio
import json
import logging
import os
import re
import signal
import sys
from dataclasses import dataclass, field
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
LANGUAGES = os.getenv("LANGUAGES", "en,he,fr").split(",")
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
STATE_FILE = os.getenv("STATE_FILE", "/data/bot_state.json")

POLYMARKET_API = "https://gamma-api.polymarket.com"
KALSHI_API = "https://api.elections.kalshi.com/trade-api/v2"

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
# TRANSLATIONS (EN / HE / FR)
# ═══════════════════════════════════════════════════════════

TRANSLATIONS = {
    "en": {
        "bot_started": (
            "🚀 Market Intelligence Bot Started!\n\n"
            "🔍 Status: Active\n⏰ Frequency: Every {interval} min\n"
            "📊 Sources: Polymarket, Kalshi, RSS\n"
            "🎯 Alert threshold: {threshold}+ bps\n🌐 Languages: EN, HE, FR"
        ),
        "gap_title": "🔔 GAP ALERT — {name}",
        "gap_body": (
            "📊 Market: {name}\n🏷️ Category: {cat}\n\n"
            "Polymarket: {poly}%\nKalshi: {kalshi}%\n"
            "📐 Gap: {gap} bps\n📈 Direction: {dir}\n\n"
            "🔗 Poly: {poly_url}\n🔗 Kalshi: {kalshi_url}"
        ),
        "move_title": "⚡ BIG MOVE — {name}",
        "move_body": (
            "📊 {name}\n🏷️ Category: {cat}\nSource: {src}\n\n"
            "Before: {old}% → Now: {new}%\n📐 Move: {delta} bps\n"
            "⏱️ Timeframe: {tf}\n\n🔗 {url}"
        ),
        "rss_title": "📰 {feed} — New Update",
        "rss_body": "📌 {title}\n\n{summary}\n\n🔗 {link}",
        "heartbeat": "💓 Bot alive — {ts}\nMarkets: {mc} | Feeds: {fc}",
    },
    "he": {
        "bot_started": (
            "🚀 בוט מודיעין שווקים הופעל!\n\n"
            '🔍 מצב: פעיל\n⏰ תדירות: כל {interval} דקות\n'
            "📊 מקורות: Polymarket, Kalshi, RSS\n"
            '🎯 סף התראה: {threshold}+ נ"ב\n🌐 שפות: EN, HE, FR'
        ),
        "gap_title": "🔔 התראת פער — {name}",
        "gap_body": (
            "📊 שוק: {name}\n🏷️ קטגוריה: {cat}\n\n"
            "Polymarket: {poly}%\nKalshi: {kalshi}%\n"
            '📐 פער: {gap} נ"ב\n📈 כיוון: {dir}\n\n'
            "🔗 Poly: {poly_url}\n🔗 Kalshi: {kalshi_url}"
        ),
        "move_title": "⚡ תנועה גדולה — {name}",
        "move_body": (
            "📊 {name}\n🏷️ קטגוריה: {cat}\nמקור: {src}\n\n"
            'לפני: {old}% → עכשיו: {new}%\n📐 תנועה: {delta} נ"ב\n'
            "⏱️ טווח: {tf}\n\n🔗 {url}"
        ),
        "rss_title": "📰 {feed} — עדכון חדש",
        "rss_body": "📌 {title}\n\n{summary}\n\n🔗 {link}",
        "heartbeat": "💓 הבוט פעיל — {ts}\nשווקים: {mc} | פידים: {fc}",
    },
    "fr": {
        "bot_started": (
            "🚀 Bot Intelligence Marchés Activé!\n\n"
            "🔍 Statut: Actif\n⏰ Fréquence: Toutes les {interval} min\n"
            "📊 Sources: Polymarket, Kalshi, RSS\n"
            "🎯 Seuil: {threshold}+ pdb\n🌐 Langues: EN, HE, FR"
        ),
        "gap_title": "🔔 ALERTE ÉCART — {name}",
        "gap_body": (
            "📊 Marché: {name}\n🏷️ Catégorie: {cat}\n\n"
            "Polymarket: {poly}%\nKalshi: {kalshi}%\n"
            "📐 Écart: {gap} pdb\n📈 Direction: {dir}\n\n"
            "🔗 Poly: {poly_url}\n🔗 Kalshi: {kalshi_url}"
        ),
        "move_title": "⚡ MOUVEMENT — {name}",
        "move_body": (
            "📊 {name}\n🏷️ Catégorie: {cat}\nSource: {src}\n\n"
            "Avant: {old}% → Maintenant: {new}%\n📐 Mouvement: {delta} pdb\n"
            "⏱️ Période: {tf}\n\n🔗 {url}"
        ),
        "rss_title": "📰 {feed} — Mise à jour",
        "rss_body": "📌 {title}\n\n{summary}\n\n🔗 {link}",
        "heartbeat": "💓 Bot en vie — {ts}\nMarchés: {mc} | Flux: {fc}",
    },
}


def tr(key, lang="en", **kw):
    tmpl = TRANSLATIONS.get(lang, TRANSLATIONS["en"]).get(key, key)
    try:
        return tmpl.format(**kw)
    except KeyError:
        return tmpl


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
# DATA CLASSES
# ═══════════════════════════════════════════════════════════

CATEGORY_KEYWORDS = {
    "crypto": ["bitcoin", "btc", "eth", "ethereum", "crypto", "solana", "defi"],
    "politics": ["president", "election", "trump", "biden", "senate", "congress", "vote", "governor"],
    "macro": ["fed", "rate", "inflation", "gdp", "recession", "treasury", "cpi", "tariff", "interest rate"],
    "sports": ["nba", "nfl", "mlb", "super bowl", "world cup", "champion"],
    "tech": ["ai", "openai", "apple", "google", "microsoft", "tesla", "spacex"],
    "climate": ["hurricane", "earthquake", "temperature", "climate", "wildfire"],
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
            markets.append({
                "id": f"kalshi_{m.get('id', '')}",
                "title": m.get("title", "Unknown"),
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

                    items.append({"feed": fi["name"], "cat": cat, "title": title,
                                  "summary": summary, "link": link, "guid": guid, "pub": pub})
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
                    "gap": round(gap), "dir": "Poly > Kalshi" if pp > kp else "Kalshi > Poly",
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
                "tf": f"{CHECK_INTERVAL} min", "url": i.get("url", ""),
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
# TELEGRAM
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

    async def multi(self, key, **kw):
        for lang in LANGUAGES:
            await self.send(tr(key, lang, **kw))
            await asyncio.sleep(0.5)

    async def startup(self):
        await self.multi("bot_started", interval=CHECK_INTERVAL, threshold=ALERT_THRESHOLD)

    async def gap_alert(self, a):
        for lang in LANGUAGES:
            t1 = tr("gap_title", lang, name=a["name"])
            t2 = tr("gap_body", lang, **a)
            await self.send(f"{t1}\n\n{t2}")
            await asyncio.sleep(0.5)

    async def move_alert(self, a):
        for lang in LANGUAGES:
            t1 = tr("move_title", lang, name=a["name"])
            t2 = tr("move_body", lang, **a)
            await self.send(f"{t1}\n\n{t2}")
            await asyncio.sleep(0.5)

    async def rss_alert(self, item):
        for lang in LANGUAGES:
            t1 = tr("rss_title", lang, feed=item["feed"])
            t2 = tr("rss_body", lang, **item)
            await self.send(f"{t1}\n\n{t2}")
            await asyncio.sleep(0.5)

    async def heartbeat(self, mc, fc):
        ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
        await self.send(tr("heartbeat", LANGUAGES[0], ts=ts, mc=mc, fc=fc))


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
                info[m["id"]] = {"title": m["title"], "category": m["category"],
                                 "source": m["source"], "url": m["url"]}

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
            await notifier.send(f"⚠️ Error: {str(e)[:500]}")
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
