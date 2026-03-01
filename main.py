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

כללים קריטיים:
1. כתוב תמיד בעברית
2. היה מדויק ומבוסס עובדות — אל תמציא סיבות
3. ציין תמיד רמת ביטחון
4. אל תפחד להגיד "לא ברור" כשאין מספיק מידע
5. ענה תמיד ב-JSON בלבד, בלי backticks

כללים לגבי תאריכים ותזמון:
6. שווקים שהתאריך שלהם כבר עבר הם לא רלוונטיים — אל תנתח אותם, אל תחזה להם תנועות
7. כשאתה חוזה תזמון, היה ספציפי ולוגי — אל תגיד "12 שעות" סתם. הסבר למה דווקא טווח הזמן הזה
8. אם אירוע קרה לפני X שעות ולא היה שינוי עד עכשיו, אל תחזה שינוי מיידי אלא אם יש סיבה חדשה ספציפית
9. לגבי תחזיות: מה האירוע הבא שעשוי לגרום לתנועה? (למשל: הכרזה רשמית, פגישה, הלוויה, מינוי)"""


def get_current_datetime_str():
    """Get current date/time string for AI context."""
    now = datetime.now(timezone.utc)
    return now.strftime("%Y-%m-%d %H:%M UTC")


async def ask_claude(session: aiohttp.ClientSession, prompt: str, max_tokens: int = 1500) -> Optional[str]:
    """Call Claude API with current date context."""
    if not ANTHROPIC_API_KEY:
        logger.warning("No ANTHROPIC_API_KEY set")
        return None

    date_context = f"\n\n[תאריך ושעה נוכחיים: {get_current_datetime_str()}]\n\n"

    headers = {
        "x-api-key": ANTHROPIC_API_KEY,
        "anthropic-version": "2023-06-01",
        "content-type": "application/json",
    }
    payload = {
        "model": CLAUDE_MODEL,
        "max_tokens": max_tokens,
        "system": AI_SYSTEM,
        "messages": [{"role": "user", "content": date_context + prompt}],
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


async def ai_analyze_big_move(session, market, old_price, new_price, timeframe, recent_news=None):
    """Analyze a big price move using REAL recent news — not guessing."""
    direction = "עלייה" if new_price > old_price else "ירידה"

    news_context = ""
    if recent_news:
        news_lines = "\n".join([
            f"  - [{n['source']}] {n['title']}"
            for n in recent_news[:8]
        ])
        news_context = f"""
══ חדשות אחרונות שנמצאו (השתמש בהן!) ══
{news_lines}
"""
    else:
        news_context = "\n══ לא נמצאו חדשות רלוונטיות ══\n"

    prompt = f"""שוק הימורים הקשור לאיראן זז בצורה משמעותית:

שוק: {market['title']}
מחיר קודם: {old_price}%
מחיר נוכחי: {new_price}%
שינוי: {new_price - old_price:+.1f}%
כיוון: {direction}
טווח זמן: {timeframe}
{news_context}

הוראות קריטיות:
1. אם יש חדשות רלוונטיות — השתמש בהן כדי להסביר את התנועה. אל תנחש!
2. אם אין חדשות — אמור בפירוש שלא ברור מה גרם לתנועה
3. אל תמציא סיבות. אם אתה לא יודע — אמור "לא ברור"
4. התמקד בתחזית קדימה: מה צפוי לקרות עכשיו?

ענה ב-JSON:
{{
    "title_he": "שם השוק בעברית",
    "cause": "מה גרם לתנועה — על סמך חדשות אמיתיות בלבד. אם אין חדשות רלוונטיות כתוב 'הסיבה לא ברורה כרגע' (2-3 משפטים)",
    "news_based": true או false,
    "forward_prediction": "תחזית קדימה: מה צפוי לקרות בשוק הזה בימים הקרובים, על סמך החדשות והמגמה (3-4 משפטים)",
    "related_markets_prediction": "אילו שווקים אחרים צפויים לזוז בעקבות זה, ולאיזה כיוון (2-3 משפטים)",
    "action_window": "חלון הפעולה: כמה זמן לדעתך ההזדמנות פתוחה (משפט אחד)",
    "recommendation": "המלצה ספציפית — מה לעשות עכשיו (2-3 משפטים)",
    "watch_factors": ["גורם 1 לעקוב שישפיע על הכיוון", "גורם 2", "גורם 3"],
    "confidence": "גבוהה" או "בינונית" או "נמוכה"
}}"""
    return parse_claude_json(await ask_claude(session, prompt))


async def ai_filter_news(session, news_items, knowledge_base, sent_topics):
    """AI decides which news items are truly NEW and worth sending."""
    news_text = "\n".join([
        f"  {i+1}. [{item['source']}] {item['title']}"
        for i, item in enumerate(news_items[:10])
    ])

    topics_text = "\n".join([f"  - {t}" for t in sent_topics[-20:]]) if sent_topics else "  (אין נושאים קודמים)"

    prompt = f"""אתה מסנן חדשות חכם. התפקיד שלך: לזהות אילו חדשות הן באמת חדשות ואילו הן חזרה על מידע ישן.

══ מה שאנחנו כבר יודעים (מצב עדכני) ══
{knowledge_base or "(אין מידע קודם — זו הסריקה הראשונה)"}

══ נושאים שכבר דיווחנו עליהם ══
{topics_text}

══ חדשות שהתקבלו עכשיו ══
{news_text}

בדוק כל חדשה ושאל את עצמך:
1. האם זה מידע שכבר ידוע לנו מהמצב העדכני?
2. האם זה חוזר על נושא שכבר דיווחנו עליו?
3. האם יש כאן פרט חדש משמעותי שלא ידענו?

לדוגמה:
- אם כבר ידוע שחמינאי מת → כתבה "חמינאי מת" = לא חדש, לסנן
- אם כבר ידוע שחמינאי מת → כתבה "מוג'תבא מונה כממלא מקום" = חדש! לשלוח
- אם כבר דיווחנו על סנקציות חדשות → כתבה נוספת על אותן סנקציות = לא חדש

ענה ב-JSON בלבד:
{{
    "selected_indices": [1, 4],
    "reasoning": "הסבר קצר למה בחרת רק את אלה ולמה סיננת את האחרים"
}}

אם אף חדשה לא מביאה מידע חדש, החזר: {{"selected_indices": [], "reasoning": "הסבר"}}"""

    result = parse_claude_json(await ask_claude(session, prompt))
    if not result:
        return news_items[:3]  # Fallback: send first 3

    selected = result.get("selected_indices", [])
    reasoning = result.get("reasoning", "")
    if reasoning:
        logger.info(f"AI filter: {reasoning}")

    filtered = []
    for idx in selected:
        i = idx - 1  # Convert 1-indexed to 0-indexed
        if 0 <= i < len(news_items):
            filtered.append(news_items[i])

    return filtered


async def ai_analyze_news(session, news_items, current_markets, knowledge_base):
    """PREDICTIVE analysis: news → forecast which markets WILL move and how."""
    markets_summary = "\n".join([
        f"  - {m['title']}: {m['yes_price']*100:.1f}% ({m['source']})"
        for m in current_markets[:15]
    ]) or "  אין שווקים פעילים כרגע"

    news_text = "\n".join([
        f"  - [{item['source']}] {item['title']}"
        for item in news_items[:5]
    ])

    prompt = f"""אתה אנליסט שווקי הימורים. התפקיד שלך: לקרוא חדשות ולחזות איך שווקי ההימורים יגיבו — לפני שזה קורה.

══ מה שאנחנו כבר יודעים ══
{knowledge_base or "(אין מידע קודם)"}

══ חדשות חדשות ══
{news_text}

══ שווקים פעילים כרגע ══
{markets_summary}

הוראות קריטיות:
1. אל תסכם את החדשות — המשתמש יכול לקרוא בעצמו
2. התמקד ב: מה החדשות האלה אומרות על העתיד של השווקים
3. חזה תנועות ספציפיות: איזה שוק, לאיזה כיוון, בכמה, ומתי
4. אם חדשה מצביעה על הזדמנות — ציין אותה בבירור
5. היה ספציפי: "שוק X צפוי לעלות מ-60% ל-70-75% תוך 24-48 שעות" — לא "ייתכן שיהיה שינוי"
6. בדוק את התאריך הנוכחי! אם שוק מתייחס לתאריך שכבר עבר (למשל "ינואר 2026" כשאנחנו במרץ 2026) — התעלם ממנו לחלוטין, אל תכלול אותו בתחזיות
7. לגבי תזמון התחזיות — היה לוגי:
   - ציין מה האירוע הבא שיגרום לתנועה (למשל: "הכרזה רשמית על יורש", "הלוויה", "הצבעה במועצת המומחים")
   - אם אירוע כבר קרה לפני 20 שעות והשוק כבר הגיב — אל תחזה תנועה נוספת אלא אם יש טריגר חדש ספציפי
   - הטווח צריך להיות מבוסס על אירוע צפוי, לא מספר שרירותי

ענה ב-JSON:
{{
    "headline_he": "כותרת קצרה שמתמקדת בהשפעה על ההימורים, לא בחדשות עצמן (משפט אחד)",
    "news_summary_he": "סיכום קצר בלבד של החדשות (2 משפטים מקסימום)",
    "predictions": [
        {{
            "market": "שם השוק שצפוי לזוז",
            "current_price": "המחיר הנוכחי",
            "predicted_price": "המחיר הצפוי",
            "direction": "עלייה" או "ירידה",
            "trigger_event": "מה האירוע הספציפי שיגרום לתנועה (למשל: 'הכרזה רשמית על יורש', 'תוצאות הצבעה')",
            "timeframe": "מתי צפוי הטריגר (לא מספר שרירותי — מבוסס על האירוע)",
            "confidence": "גבוהה/בינונית/נמוכה",
            "logic": "למה — הקשר ישיר בין החדשה לתנועה הצפויה (משפט אחד)"
        }}
    ],
    "opportunity": "ההזדמנות המרכזית: מה אפשר לעשות עכשיו לפני שהשוק יגיב (2-3 משפטים)",
    "risk_warning": "סיכונים: מה יכול להשתבש עם התחזית (1-2 משפטים)",
    "action_items": ["פעולה ספציפית 1", "פעולה ספציפית 2"],
    "urgency": "דחוף" או "חשוב" או "לידיעה",
    "overall_confidence": "גבוהה" או "בינונית" או "נמוכה",
    "topic_summary": "תיאור קצר של הנושא (10-15 מילים)"
}}"""
    return parse_claude_json(await ask_claude(session, prompt, max_tokens=2000))


async def ai_update_knowledge(session, current_knowledge, new_info, news_titles):
    """AI updates the running knowledge base with new confirmed information."""
    news_list = "\n".join([f"  - {t}" for t in news_titles[:5]])

    prompt = f"""עדכן את מאגר הידע שלנו על המצב באיראן.

══ מאגר ידע נוכחי ══
{current_knowledge or "(ריק — זו ההתחלה)"}

══ מידע חדש שהתקבל ══
{new_info}

══ כותרות מקור ══
{news_list}

כתוב מאגר ידע מעודכן. הכללים:
1. שמור את כל המידע הישן שעדיין רלוונטי
2. הוסף את המידע החדש
3. אם מידע חדש סותר מידע ישן — עדכן (למשל: אם קודם כתבנו "חמינאי חולה" ועכשיו "חמינאי מת" — עדכן ל"מת")
4. סמן תאריכים כשאפשר
5. כתוב בצורה תמציתית — נקודות קצרות
6. מקסימום 500 מילים

ענה ב-JSON:
{{
    "updated_knowledge": "המאגר המעודכן כטקסט מובנה"
}}"""

    result = parse_claude_json(await ask_claude(session, prompt, max_tokens=1500))
    return result.get("updated_knowledge", current_knowledge) if result else current_knowledge


# ═══════════════════════════════════════════════════════════
# MARKET DATA FETCHING
# ═══════════════════════════════════════════════════════════

def is_iran_market(title: str, description: str = "") -> bool:
    """Check if a market is related to Iran."""
    text = f"{title} {description}".lower()
    return any(kw in text for kw in IRAN_KEYWORDS)


def is_expired_market(title: str) -> bool:
    """Check if a market's date has already passed."""
    now = datetime.now(timezone.utc)
    text = title.lower()

    # Month names to numbers
    months = {
        "january": 1, "february": 2, "march": 3, "april": 4,
        "may": 5, "june": 6, "july": 7, "august": 8,
        "september": 9, "october": 10, "november": 11, "december": 12,
        "jan": 1, "feb": 2, "mar": 3, "apr": 4,
        "jun": 6, "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12,
    }

    # Pattern: "by/in/before January 2026" or "in January 2026"
    for month_name, month_num in months.items():
        if month_name in text:
            # Try to find a year nearby
            year_match = re.search(r'20(\d{2})', text)
            if year_match:
                year = 2000 + int(year_match.group(1))
                # If the end of that month is in the past
                if year < now.year or (year == now.year and month_num < now.month):
                    logger.debug(f"Expired market filtered: {title}")
                    return True

    return False


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
            if not is_iran_market(title, desc) or is_expired_market(title):
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
                if is_expired_market(m_title):
                    continue
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
                    if not is_iran_market(title, ev.get("description", "")) or is_expired_market(title):
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
                        if is_expired_market(m_title):
                            continue
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
            if not is_iran_market(title, subtitle) or is_expired_market(title):
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
        self.knowledge_base = ""   # AI-maintained summary of what we already know
        self.sent_topics = []      # List of topic summaries already sent
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
                self.knowledge_base = s.get("knowledge_base", "")
                self.sent_topics = s.get("sent_topics", [])
                logger.info(f"State loaded — scan #{self.scan_count}, "
                           f"tracking {len(self.price_history)} markets, "
                           f"knowledge: {len(self.knowledge_base)} chars")
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
                    "knowledge_base": self.knowledge_base,
                    "sent_topics": self.sent_topics[-50:],
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

    async def send_startup(self, market_count: int, has_memory: bool = False):
        memory_status = "✅ פעיל" if has_memory else "🆕 ריק (יתמלא בסריקה הראשונה)"
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
            f"💾 זיכרון AI: {memory_status}\n"
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
        news_based = ai.get("news_based", False)
        source_tag = "📰 מבוסס חדשות" if news_based else "⚠️ הסיבה לא ברורה"

        msg = (
            f"{arrow} תנועה גדולה\n"
            "━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"📊 {ai.get('title_he', m['title'])}\n"
            f"מקור: {m['source']}\n"
            f"{source_tag}\n\n"
            f"לפני: {move['old_price']}% → עכשיו: {move['new_price']}%\n"
            f"שינוי: {move['delta']:+.1f}%\n\n"
        )
        if ai.get("cause"):
            msg += f"❓ למה זה קרה:\n{ai['cause']}\n\n"
        if ai.get("forward_prediction"):
            msg += f"🔮 תחזית קדימה:\n{ai['forward_prediction']}\n\n"
        if ai.get("related_markets_prediction"):
            msg += f"🔗 שווקים שצפויים לזוז:\n{ai['related_markets_prediction']}\n\n"
        if ai.get("action_window"):
            msg += f"⏰ חלון פעולה: {ai['action_window']}\n\n"
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
        confidence = ai.get("overall_confidence", ai.get("confidence", "—"))

        msg = (
            f"{urgency_emoji} תחזית שווקים — איראן\n"
            "━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"📌 {ai.get('headline_he', 'עדכון חדש')}\n\n"
        )
        if ai.get("news_summary_he"):
            msg += f"📰 מה קרה: {ai['news_summary_he']}\n\n"

        # Predictions - the main event
        predictions = ai.get("predictions", [])
        if predictions:
            msg += "🔮 תחזיות תנועה:\n"
            for pred in predictions[:5]:
                direction = pred.get("direction", "—")
                dir_emoji = "📈" if direction == "עלייה" else "📉"
                conf = pred.get("confidence", "—")
                msg += (
                    f"\n  {dir_emoji} {pred.get('market', '—')}\n"
                    f"    עכשיו: {pred.get('current_price', '—')} → צפי: {pred.get('predicted_price', '—')}\n"
                    f"    🎯 טריגר: {pred.get('trigger_event', '—')}\n"
                    f"    ⏰ {pred.get('timeframe', '—')} | ביטחון: {conf}\n"
                    f"    💬 {pred.get('logic', '')}\n"
                )
            msg += "\n"

        if ai.get("opportunity"):
            msg += f"💰 הזדמנות:\n{ai['opportunity']}\n\n"
        if ai.get("risk_warning"):
            msg += f"⚠️ סיכונים:\n{ai['risk_warning']}\n\n"

        action_items = ai.get("action_items", [])
        if action_items:
            items = "\n".join([f"  ✅ {a}" for a in action_items])
            msg += f"📋 פעולות מומלצות:\n{items}\n\n"

        # Source links
        msg += "🔗 מקורות:\n"
        for item in news_items[:3]:
            msg += f"  • {item['source']}: {item['link']}\n"

        msg += (
            f"\n🎯 רמת ביטחון כוללת: {confidence}\n"
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

            # ── 2. Big Moves — fetch news to explain WHY ──
            big_moves = find_big_moves(all_markets, state.price_history)
            if big_moves:
                # Fetch fresh news to explain the moves
                move_news = await fetch_iran_news(s)
                logger.info(f"Fetched {len(move_news)} news items to explain {len(big_moves)} moves")
            else:
                move_news = []

            for move in big_moves[:3]:
                mid = move["market"]["id"]
                if not state.can_alert_move(mid):
                    continue
                logger.info(f"📈 BIG MOVE: {move['market']['title']} — {move['delta']:+.1f}%")
                ai = await ai_analyze_big_move(
                    s, move["market"], move["old_price"], move["new_price"],
                    "24 שעות", recent_news=move_news
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
    """News scan — runs every 5-10 minutes. Uses AI memory to avoid duplicates."""
    global state, notifier
    logger.info("═══ News Scan ═══")

    try:
        async with aiohttp.ClientSession() as s:
            # Fetch news
            news = await fetch_iran_news(s)

            # Stage 1: Filter out already seen GUIDs
            new_items = [n for n in news if n["guid"] not in state.seen_news]
            if not new_items:
                logger.info("No new Iran news (all GUIDs seen)")
                return

            new_items = new_items[:10]  # Take top 10 for AI filtering

            # Stage 2: AI FILTER — Claude decides what's truly new
            logger.info(f"📰 {len(new_items)} new GUIDs — asking AI to filter...")
            filtered = await ai_filter_news(s, new_items, state.knowledge_base, state.sent_topics)

            # Mark ALL fetched items as seen (even filtered ones)
            for item in new_items:
                state.seen_news.append(item["guid"])

            if not filtered:
                logger.info("AI filter: nothing truly new — all filtered out")
                state.save()
                return

            logger.info(f"AI filter: {len(filtered)} items passed (out of {len(new_items)})")

            # Stage 3: Get current markets for context
            poly = await fetch_polymarket_iran(s)
            kalshi = await fetch_kalshi_iran(s)
            all_markets = poly + kalshi

            # Stage 4: AI analysis WITH knowledge base context
            ai = await ai_analyze_news(s, filtered, all_markets, state.knowledge_base)

            if ai:
                await notifier.send_news(ai, filtered)

                # Stage 5: Update knowledge base with new info
                topic_summary = ai.get("topic_summary", "")
                if topic_summary:
                    state.sent_topics.append(topic_summary)

                summary_he = ai.get("news_summary_he", "")
                if summary_he:
                    news_titles = [item["title"] for item in filtered]
                    updated_kb = await ai_update_knowledge(
                        s, state.knowledge_base, summary_he, news_titles
                    )
                    if updated_kb:
                        state.knowledge_base = updated_kb
                        logger.info(f"Knowledge base updated ({len(updated_kb)} chars)")
            else:
                # Fallback: send raw (only first item)
                item = filtered[0]
                msg = (
                    f"📰 חדשות איראן\n\n"
                    f"📌 {item['title']}\n"
                    f"מקור: {item['source']}\n"
                    f"🔗 {item['link']}"
                )
                await notifier.send(msg)

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

    await notifier.send_startup(initial_count, has_memory=bool(state.knowledge_base))

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
