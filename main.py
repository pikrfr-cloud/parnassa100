#!/usr/bin/env python3
"""
🎯 Edge Finder — שוקי הימורים איראן
=====================================
מטרה אחת: למצוא מתי השוק טועה.

ארכיטקטורה חסכונית:
  - סריקת מחירים כל 10 דקות (חינם — רק API של Polymarket/Kalshi)
  - ניתוח Claude רק כש:
    1. מחיר זז יותר מ-5% מאז הניתוח האחרון
    2. או כל שעתיים (ניתוח עומק מתוזמן)
  - קריאת Claude אחת בBATCH לכל השווקים (לא קריאה נפרדת לכל שוק)

עלות משוערת: ~$8-10/חודש
"""

import asyncio, json, logging, os, re, signal, sys
from datetime import datetime, timezone, timedelta
from difflib import SequenceMatcher

import aiohttp, feedparser
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
STATE_FILE = os.getenv("STATE_FILE", "/data/bot_state.json")

PRICE_SCAN_MIN = 10         # Free price check interval
DEEP_ANALYSIS_MIN = 120     # Full Claude analysis interval
MOVE_TRIGGER_PCT = 5.0      # Price move that triggers instant Claude
EDGE_THRESHOLD = 10          # Min edge % to alert
ARB_THRESHOLD = 5.0          # Min arbitrage gap %

POLYMARKET_API = "https://gamma-api.polymarket.com"
KALSHI_API = "https://api.elections.kalshi.com/trade-api/v2"
CLAUDE_API = "https://api.anthropic.com/v1/messages"
CLAUDE_MODEL = "claude-sonnet-4-20250514"

IRAN_KW = [
    "iran", "iranian", "khamenei", "mojtaba", "supreme leader",
    "irgc", "revolutionary guard", "tehran", "assembly of experts",
    "ayatollah", "iran nuclear", "iran sanction", "iran regime",
    "iran war", "iran strike", "iran attack", "iran deal", "jcpoa",
    "iran israel", "iran succession", "iran collapse", "iran revolution",
]
NEWS_QUERIES = [
    "Iran Supreme Leader", "Mojtaba Khamenei", "Iran regime",
    "Iran IRGC", "Iran Israel", "Iran nuclear", "Iran sanctions",
]

# ═══════════════════════════════════════════════════════════
# LOGGING
# ═══════════════════════════════════════════════════════════

logging.basicConfig(level=logging.INFO, format="%(asctime)s │ %(levelname)-7s │ %(message)s", datefmt="%H:%M:%S")
logger = logging.getLogger("edge")
for q in ["httpx", "telegram", "apscheduler"]:
    logging.getLogger(q).setLevel(logging.WARNING)

# ═══════════════════════════════════════════════════════════
# HELPERS
# ═══════════════════════════════════════════════════════════

def now_s(): return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
def now_u(): return datetime.now(timezone.utc)
def is_iran(t, d=""): return any(k in f"{t} {d}".lower() for k in IRAN_KW)

def is_expired(title):
    now = now_u()
    months = {"january":1,"february":2,"march":3,"april":4,"may":5,"june":6,
              "july":7,"august":8,"september":9,"october":10,"november":11,"december":12}
    t = title.lower()
    for name, num in months.items():
        if name in t:
            ym = re.search(r'20(\d{2})', t)
            if ym and (2000+int(ym.group(1)) < now.year or
                       (2000+int(ym.group(1)) == now.year and num < now.month)):
                return True
    return False

def norm(t):
    t = re.sub(r"^(will|is|does|has|can)\s+", "", t.lower().strip())
    return re.sub(r"\?$", "", t).strip()

# ═══════════════════════════════════════════════════════════
# CLAUDE — SMART CALLS
# ═══════════════════════════════════════════════════════════

SYS = """אתה אנליסט מודיעין בכיר. מומחיות: שווקי הימורים פוליטיים, איראן.
תפקיד: להעריך האם מחירי השווקים משקפים את המציאות ולזהות הזדמנויות.
כללים: עברית בלבד. התבסס על עובדות. אם אין מידע — אמור. היה אמיץ. שווקים שהתאריך עבר = לא רלוונטיים. JSON בלבד ללא backticks."""

async def call_claude(session, prompt, max_tok=2500):
    if not ANTHROPIC_API_KEY: return None
    try:
        async with session.post(CLAUDE_API,
            json={"model": CLAUDE_MODEL, "max_tokens": max_tok, "system": SYS,
                  "messages": [{"role":"user","content":f"[{now_s()}]\n\n{prompt}"}]},
            headers={"x-api-key": ANTHROPIC_API_KEY, "anthropic-version": "2023-06-01",
                     "content-type": "application/json"},
            timeout=aiohttp.ClientTimeout(total=60)) as r:
            if r.status != 200:
                logger.error(f"Claude {r.status}: {(await r.text())[:200]}")
                return None
            c = (await r.json()).get("content", [])
            return c[0]["text"] if c and c[0].get("type") == "text" else None
    except Exception as e:
        logger.error(f"Claude: {e}")
    return None

def pj(text):
    if not text: return {}
    try:
        c = text.strip()
        if c.startswith("```"): c = re.sub(r"```json?|```", "", c).strip()
        return json.loads(c)
    except: return {}


async def batch_evaluate(session, markets, news, knowledge):
    """ONE Claude call → evaluate ALL markets."""
    m_block = "\n".join([f"  {i+1}. [{m['source']}] {m['title']} — {m['price']}%"
                         for i, m in enumerate(markets)])
    n_block = "\n".join([f"  - [{n['source']}] {n['title']}" for n in news[:12]]) or "  (אין)"

    prompt = f"""הערך את כל השווקים בקריאה אחת.

══ ידע מצטבר ══
{knowledge or "(ריק)"}

══ חדשות 24 שעות ══
{n_block}

══ שווקים ══
{m_block}

הוראות:
1. לכל שוק — האם המחיר נכון? מה ההסתברות האמיתית?
2. דווח רק על edges של {EDGE_THRESHOLD}%+
3. התעלם משווקים שעברו
4. אין מידע = דלג

JSON:
{{
    "edges": [
        {{
            "market_index": 1,
            "market_he": "שם בעברית",
            "market_price": XX,
            "my_estimate_low": XX,
            "my_estimate_high": XX,
            "my_estimate_mid": XX,
            "edge_size": XX,
            "edge_direction": "קנה YES" או "קנה NO",
            "confidence": "גבוהה/בינונית/נמוכה",
            "reasoning": "למה השוק טועה (2-3 משפטים)",
            "key_news": "חדשה מפתח (משפט)",
            "trigger": "אירוע הבא שישנה מחיר (משפט)",
            "risk": "סיכון (משפט)"
        }}
    ],
    "no_edge_note": "למה אין edge בשאר (1-2 משפטים)",
    "knowledge_update": "עדכון ידע (3-5 נקודות קצרות)"
}}

אם אין edges — רשימה ריקה + הסבר."""
    return pj(await call_claude(session, prompt, 3000))


async def triggered_evaluate(session, market, news, knowledge):
    """Single market evaluation — triggered by big move."""
    n_block = "\n".join([f"  - [{n['source']}] {n['title']}" for n in news[:10]]) or "  (אין)"
    prompt = f"""שוק זז בחדות! הערך:

שם: {market['title']}
מחיר: {market['price']}% | תנועה: {market.get('move','?')}%
מקור: {market['source']}

══ ידע ══
{knowledge or "(ריק)"}

══ חדשות ══
{n_block}

JSON:
{{
    "market_he": "שם בעברית",
    "market_price": {market['price']},
    "my_estimate_low": XX, "my_estimate_high": XX, "my_estimate_mid": XX,
    "has_edge": true/false,
    "edge_size": XX,
    "edge_direction": "קנה YES/NO/אין",
    "confidence": "גבוהה/בינונית/נמוכה",
    "reasoning": "למה (2-3 משפטים)",
    "key_news": "חדשה (משפט)",
    "cause": "סיבת התנועה מהחדשות. אם לא ברור = 'לא ברור' (1-2 משפטים)",
    "trigger": "אירוע הבא (משפט)",
    "risk": "סיכון (משפט)"
}}"""
    return pj(await call_claude(session, prompt))


# ═══════════════════════════════════════════════════════════
# DATA FETCHING (FREE)
# ═══════════════════════════════════════════════════════════

async def fetch_poly(session):
    markets = []
    try:
        async with session.get(f"{POLYMARKET_API}/events",
            params={"active":"true","closed":"false","limit":200,"order":"volume24hr","ascending":"false"},
            timeout=aiohttp.ClientTimeout(total=30)) as r:
            if r.status != 200: return markets
            data = await r.json()
        for ev in data:
            t, d = ev.get("title",""), ev.get("description","")
            if not is_iran(t, d) or is_expired(t): continue
            slug = ev.get("slug","")
            for m in ev.get("markets",[]):
                mt = m.get("question", m.get("groupItemTitle", t))
                if is_expired(mt): continue
                p = None
                op = m.get("outcomePrices","")
                if op:
                    try: p = float(json.loads(op)[0])
                    except: pass
                if p is None:
                    yp = m.get("bestAsk") or m.get("lastTradePrice")
                    if yp: p = float(yp)
                if p is None: continue
                markets.append({"id":f"poly_{m.get('id',ev.get('id',''))}","title":mt,
                    "description":d[:300],"price":round(p*100,1),"source":"Polymarket",
                    "url":f"https://polymarket.com/event/{slug}" if slug else ""})
        seen = set()
        markets = [m for m in markets if m["id"] not in seen and not seen.add(m["id"])]
        logger.info(f"Poly: {len(markets)}")
    except Exception as e: logger.error(f"Poly: {e}")
    return markets

async def fetch_kal(session):
    markets = []
    try:
        async with session.get(f"{KALSHI_API}/markets",
            params={"limit":500,"status":"open"}, headers={"Accept":"application/json"},
            timeout=aiohttp.ClientTimeout(total=30)) as r:
            if r.status != 200: return markets
            data = await r.json()
        for m in data.get("markets",[]):
            t, sub = m.get("title",""), m.get("subtitle","")
            if not is_iran(t, sub) or is_expired(t): continue
            yp = (m.get("yes_ask",0) or m.get("last_price",0) or 0)/100.0
            tk = m.get("ticker","")
            markets.append({"id":f"kal_{m.get('id','')}","title":t,"description":sub,
                "price":round(yp*100,1),"source":"Kalshi",
                "url":f"https://kalshi.com/markets/{tk.lower()}" if tk else ""})
        logger.info(f"Kal: {len(markets)}")
    except Exception as e: logger.error(f"Kal: {e}")
    return markets

async def fetch_news(session):
    items, seen = [], set()
    for q in NEWS_QUERIES:
        try:
            async with session.get(
                f"https://news.google.com/rss/search?q={q.replace(' ','+')}&hl=en&gl=US&ceid=US:en",
                timeout=aiohttp.ClientTimeout(total=15),
                headers={"User-Agent":"EdgeFinder/1.0"}) as r:
                if r.status != 200: continue
                feed = feedparser.parse(await r.text())
            for e in feed.entries[:5]:
                t = e.get("title","")
                tl = t.lower()
                if any(SequenceMatcher(None,tl,s).ratio()>0.8 for s in seen): continue
                seen.add(tl)
                src = "News"
                if " - " in t:
                    parts = t.rsplit(" - ",1); t = parts[0]; src = parts[1] if len(parts)>1 else src
                pub = None
                if hasattr(e,"published_parsed") and e.published_parsed:
                    try: pub = datetime(*e.published_parsed[:6], tzinfo=timezone.utc)
                    except: pass
                items.append({"title":t,"source":src,"link":e.get("link",""),"pub":pub})
        except: pass
    cut = now_u() - timedelta(hours=48)
    items = [i for i in items if not i["pub"] or i["pub"]>cut]
    items.sort(key=lambda x: x.get("pub") or datetime.min.replace(tzinfo=timezone.utc), reverse=True)
    logger.info(f"News: {len(items[:20])}")
    return items[:20]

def find_arb(poly, kalshi):
    opps, used = [], set()
    for pm in poly:
        best, bs = None, 0
        for km in kalshi:
            if km["id"] in used: continue
            s = SequenceMatcher(None, norm(pm["title"]), norm(km["title"])).ratio()
            if s > bs: bs, best = s, km
        if best and bs >= 0.45:
            used.add(best["id"])
            gap = abs(pm["price"]-best["price"])
            if gap >= ARB_THRESHOLD:
                opps.append({"title":pm["title"],"poly_price":pm["price"],
                    "kalshi_price":best["price"],"gap":round(gap,1),
                    "poly_url":pm["url"],"kalshi_url":best["url"]})
    return sorted(opps, key=lambda x: x["gap"], reverse=True)


# ═══════════════════════════════════════════════════════════
# STATE
# ═══════════════════════════════════════════════════════════

class State:
    def __init__(self):
        self.knowledge = ""
        self.last_prices = {}
        self.last_analysis = None
        self.sent_edges = {}
        self.sent_arbs = {}
        self.scan_count = 0
        self.claude_today = 0
        self.claude_day = ""
        self._load()

    def _load(self):
        if os.path.exists(STATE_FILE):
            try:
                s = json.load(open(STATE_FILE))
                self.knowledge = s.get("knowledge","")
                self.last_prices = s.get("last_prices",{})
                self.last_analysis = s.get("last_analysis")
                self.sent_edges = s.get("sent_edges",{})
                self.sent_arbs = s.get("sent_arbs",{})
                self.scan_count = s.get("scan_count",0)
                self.claude_today = s.get("claude_today",0)
                self.claude_day = s.get("claude_day","")
                logger.info(f"State: scan#{self.scan_count}, Claude today: {self.claude_today}")
            except Exception as e: logger.warning(f"State: {e}")

    def save(self):
        try:
            os.makedirs(os.path.dirname(STATE_FILE), exist_ok=True)
            json.dump({"knowledge":self.knowledge,"last_prices":self.last_prices,
                "last_analysis":self.last_analysis,"sent_edges":self.sent_edges,
                "sent_arbs":self.sent_arbs,"scan_count":self.scan_count,
                "claude_today":self.claude_today,"claude_day":self.claude_day},
                open(STATE_FILE,"w"))
        except Exception as e: logger.error(f"Save: {e}")

    def tick_claude(self):
        d = now_u().strftime("%Y-%m-%d")
        if d != self.claude_day: self.claude_today = 0; self.claude_day = d
        self.claude_today += 1

    def needs_deep(self):
        if not self.last_analysis: return True
        try: return now_u() - datetime.fromisoformat(self.last_analysis) > timedelta(minutes=DEEP_ANALYSIS_MIN)
        except: return True

    def big_movers(self, markets):
        movers = []
        for m in markets:
            last = self.last_prices.get(m["id"])
            if last is None: continue
            mv = m["price"] - last
            if abs(mv) >= MOVE_TRIGGER_PCT:
                mc = dict(m); mc["move"] = round(mv,1); movers.append(mc)
        return movers

    def set_analysis(self, markets):
        self.last_prices = {m["id"]:m["price"] for m in markets}
        self.last_analysis = now_u().isoformat()

    def can_edge(self, mid, cd=60):
        l = self.sent_edges.get(mid,{}).get("ts")
        if not l: return True
        try: return now_u()-datetime.fromisoformat(l) > timedelta(minutes=cd)
        except: return True

    def mark_edge(self, mid, e):
        self.sent_edges[mid] = {"ts":now_u().isoformat(),"edge":e}

    def can_arb(self, key, cd=30):
        l = self.sent_arbs.get(key)
        if not l: return True
        try: return now_u()-datetime.fromisoformat(l) > timedelta(minutes=cd)
        except: return True

    def mark_arb(self, key):
        self.sent_arbs[key] = now_u().isoformat()


# ═══════════════════════════════════════════════════════════
# TELEGRAM
# ═══════════════════════════════════════════════════════════

class TG:
    def __init__(self):
        self.bot = Bot(token=TELEGRAM_BOT_TOKEN)
        self.chat = TELEGRAM_CHAT_ID

    async def send(self, text):
        try:
            if len(text)>4000:
                for i in range(0,len(text),4000):
                    await self.bot.send_message(chat_id=self.chat,text=text[i:i+4000],disable_web_page_preview=True)
                    await asyncio.sleep(0.5)
                return
            await self.bot.send_message(chat_id=self.chat,text=text,disable_web_page_preview=True)
        except RetryAfter as e:
            await asyncio.sleep(e.retry_after); await self.send(text)
        except TelegramError as e: logger.error(f"TG: {e}")

    async def startup(self, n):
        await self.send(
            "🎯 Edge Finder — איראן\n━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"📊 שווקים: {n}\n"
            f"⏰ מחירים: כל {PRICE_SCAN_MIN} דקות (חינם)\n"
            f"🧠 Claude: כל {DEEP_ANALYSIS_MIN} דק' + בתנועה של {MOVE_TRIGGER_PCT}%+\n"
            f"📐 סף edge: {EDGE_THRESHOLD}% | ארביטראז': {ARB_THRESHOLD}%\n\n"
            "שולח התראה רק כשהשוק טועה.\n━━━━━━━━━━━━━━━━━━━━━━")

    async def edge(self, market, ai):
        d = ai.get("edge_direction","—")
        de = "🟢 קנה YES" if "YES" in d else "🔴 קנה NO" if "NO" in d else "—"
        msg = (
            "🎯 EDGE — השוק טועה\n━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"📊 {ai.get('market_he',market['title'])}\n({market['source']})\n\n"
            f"💰 השוק: {market['price']}%\n"
            f"🧠 הערכה: {ai.get('my_estimate_low','?')}-{ai.get('my_estimate_high','?')}% "
            f"(אמצע: {ai.get('my_estimate_mid','?')}%)\n"
            f"📐 Edge: {ai.get('edge_size','?')} נקודות\n"
            f"👉 {de}\n\n")
        if ai.get("reasoning"): msg += f"📋 למה:\n{ai['reasoning']}\n\n"
        if ai.get("key_news"): msg += f"📰 חדשה מפתח: {ai['key_news']}\n\n"
        if ai.get("cause"): msg += f"❓ סיבת התנועה: {ai['cause']}\n\n"
        if ai.get("trigger"): msg += f"⏰ טריגר הבא: {ai['trigger']}\n\n"
        if ai.get("risk"): msg += f"⚠️ סיכון: {ai['risk']}\n\n"
        msg += f"🎯 ביטחון: {ai.get('confidence','—')}\n\n🔗 {market['url']}\n━━━━━━━━━━━━━━━━━━━━━━"
        await self.send(msg)

    async def arb(self, o):
        hi = "Polymarket" if o["poly_price"]>o["kalshi_price"] else "Kalshi"
        lo = "Kalshi" if hi=="Polymarket" else "Polymarket"
        await self.send(
            "⚖️ ארביטראז'\n━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"📊 {o['title']}\n\nPoly: {o['poly_price']}% | Kalshi: {o['kalshi_price']}%\n"
            f"📐 פער: {o['gap']}%\n\n👉 YES ב-{lo}, NO ב-{hi}\n\n"
            f"🔗 Poly: {o['poly_url']}\n🔗 Kalshi: {o['kalshi_url']}\n━━━━━━━━━━━━━━━━━━━━━━")

    async def status(self, markets, sc, cc):
        lines = "\n".join([f"  {'●' if m['price']>50 else '○'} {m['title']}: {m['price']}%"
                           for m in markets[:20]]) or "  אין"
        await self.send(f"📊 סטטוס — {now_s()}\n━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"סריקות: {sc} | Claude היום: {cc}\n\n{lines}\n━━━━━━━━━━━━━━━━━━━━━━")


# ═══════════════════════════════════════════════════════════
# MAIN LOOP
# ═══════════════════════════════════════════════════════════

state = None
tg = None

async def scan():
    global state, tg
    state.scan_count += 1
    logger.info(f"═══ Scan #{state.scan_count} ═══")

    try:
        async with aiohttp.ClientSession() as s:
            # 1. Prices (FREE)
            poly = await fetch_poly(s)
            kalshi = await fetch_kal(s)
            all_m = poly + kalshi
            if not all_m:
                logger.info("No markets"); state.save(); return

            news = None  # Lazy load

            # 2. Big movers → instant Claude
            movers = state.big_movers(all_m)
            if movers:
                news = await fetch_news(s)
                for mv in movers[:3]:
                    logger.info(f"⚡ {mv['title']}: {mv['move']:+.1f}%")
                    ai = await triggered_evaluate(s, mv, news, state.knowledge)
                    state.tick_claude()
                    if ai and ai.get("has_edge") and abs(ai.get("edge_size",0)) >= EDGE_THRESHOLD:
                        if state.can_edge(mv["id"]):
                            await tg.edge(mv, ai)
                            state.mark_edge(mv["id"], ai.get("edge_size",0))
                    if ai and ai.get("cause"):
                        state.knowledge += f"\n• {now_s()}: {mv['title']} — {ai['cause']}"
                        if len(state.knowledge) > 3000:
                            state.knowledge = state.knowledge[-2000:]
                    await asyncio.sleep(1)

            # 3. Deep analysis (every 2h)
            if state.needs_deep():
                logger.info("🧠 Deep analysis")
                if not news: news = await fetch_news(s)
                ai = await batch_evaluate(s, all_m, news, state.knowledge)
                state.tick_claude()
                if ai:
                    for edge in ai.get("edges",[]):
                        idx = edge.get("market_index",0)-1
                        if 0<=idx<len(all_m):
                            m = all_m[idx]
                            es = edge.get("edge_size",0)
                            if abs(es)>=EDGE_THRESHOLD and state.can_edge(m["id"]):
                                logger.info(f"🎯 EDGE: {m['title']} — {es}%")
                                await tg.edge(m, edge)
                                state.mark_edge(m["id"], es)
                    kb = ai.get("knowledge_update","")
                    if kb: state.knowledge = kb; logger.info(f"KB updated ({len(kb)}ch)")
                state.set_analysis(all_m)

            # 4. Arbitrage (free)
            if poly and kalshi:
                for o in find_arb(poly, kalshi)[:2]:
                    k = o["title"][:30]
                    if state.can_arb(k):
                        await tg.arb(o); state.mark_arb(k)

            # 5. Status every ~4h
            if state.scan_count % 24 == 0:
                await tg.status(all_m, state.scan_count, state.claude_today)

            state.save()
            logger.info(f"Done — {len(all_m)} mkts, Claude today: {state.claude_today}")

    except Exception as e:
        logger.exception(f"Scan: {e}")
        try: await tg.send(f"⚠️ {str(e)[:300]}")
        except: pass


async def main():
    global state, tg
    state, tg = State(), TG()
    logger.info("🎯 Starting Edge Finder...")

    async with aiohttp.ClientSession() as s:
        p, k = await fetch_poly(s), await fetch_kal(s)
        state.set_analysis(p+k); state.save()

    await tg.startup(len(p)+len(k))

    if "--once" in sys.argv: await scan(); return

    sched = AsyncIOScheduler()
    sched.add_job(scan, IntervalTrigger(minutes=PRICE_SCAN_MIN), id="scan", max_instances=1, misfire_grace_time=120)
    sched.start()
    await scan()

    stop = asyncio.Event()
    signal.signal(signal.SIGINT, lambda *_: stop.set())
    signal.signal(signal.SIGTERM, lambda *_: stop.set())
    await stop.wait()
    sched.shutdown(wait=False)

if __name__ == "__main__":
    asyncio.run(main())
