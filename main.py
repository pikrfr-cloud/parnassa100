import os
import json
import time
import requests
import anthropic
from datetime import datetime
from itertools import combinations

# ─── CONFIG ──────────────────────────────────────────────────────────────────
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")
SCAN_INTERVAL_H = 4  # scan every N hours
MIN_VOLUME = 500  # ignore markets below this $ volume
MIN_OPPORTUNITY_SCORE = 7  # Claude scores 1-10, alert only if >= this

POLYMARKET_API = "https://gamma-api.polymarket.com"

SYSTEM_PROMPT = """Tu es un chasseur d'inefficiences sur Polymarket.

On te donne un groupe de marchés liés au même thème. Ton travail :

1. Cherche les asymétries de règles de résolution (temporelle, conditionnelle, différence de scope)
2. Identifie si les prix impliquent une incohérence logique exploitable
3. Calcule le ratio d'allocation optimal pour être profitable dans les 2 scénarios principaux
4. Définis le signal de sortie anticipée PRÉCIS (pas "si ça monte" mais "si telle news sort" ou "si la donnée X dépasse Y")
5. Identifie le scénario perdant et sa probabilité réelle

Règles strictes :
- Ne valide JAMAIS sans avoir expliqué pourquoi le marché a ce prix (le marché a peut-être raison)
- Ignore si la seule différence est de la liquidité ou du bruit
- Sois brutal : la plupart des groupes n'ont PAS d'opportunité réelle

Réponds UNIQUEMENT en JSON :
{
  "score": <1-10>,
    "opportunite": <true/false>,
      "titre": "<titre court>",
        "these": "<explication en 2 phrases>",
          "position": {
              "jambe1": {"marche": "<titre>", "direction": "YES/NO", "allocation_pct": <0-100>},
                  "jambe2": {"marche": "<titre>", "direction": "YES/NO", "allocation_pct": <0-100>}
                    },
                      "scenarios": [
                          {"nom": "...", "probabilite_pct": <int>, "pnl_pct": <int>, "gagnant": <bool>}
                            ],
                              "signal_sortie": "<signal précis>",
                                "scenario_perdant": "<description>",
                                  "probabilite_perdant_pct": <int>
                                  }"""

# ─── POLYMARKET API ──────────────────────────────────────────────────────────

def fetch_markets(limit=200):
      """Fetch active markets with enough volume."""
      try:
                resp = requests.get(
                              f"{POLYMARKET_API}/markets",
                              params={"active": True, "closed": False, "limit": limit},
                              timeout=15
                )
                resp.raise_for_status()
                markets = resp.json()
                filtered = [m for m in markets if float(m.get("volume", 0)) >= MIN_VOLUME]
                print(f"[{now()}] Fetched {len(markets)} markets, {len(filtered)} above ${MIN_VOLUME} volume")
                return filtered
except Exception as e:
        print(f"[{now()}] Error fetching markets: {e}")
        return []

def group_markets_by_theme(markets):
      """Group markets that share keywords — these are candidates for arbitrage."""
      from collections import defaultdict

    stopwords = {"the","a","an","in","on","at","to","of","will","by","for","be",
                                  "is","or","and","as","it","its","with","from","that","this",
                                  "le","la","les","de","du","des","un","une","par","pour","dans",
                                  "est","sera","au","aux","en","et","ou","qui","que"}

    def keywords(text):
              words = text.lower().replace("?","").replace("-"," ").split()
              return {w for w in words if len(w) > 3 and w not in stopwords}

    index = defaultdict(list)
    for m in markets:
              title = m.get("question") or m.get("title") or ""
              for kw in keywords(title):
                            index[kw].append(m["id"])

          # find pairs/triples sharing 2+ keywords
          id_to_market = {m["id"]: m for m in markets}
    groups = []
    seen = set()

    for kw, ids in index.items():
              if len(ids) < 2:
                            continue
                        for pair in combinations(ids[:10], 2):
                                      key = tuple(sorted(pair))
                                      if key in seen:
                                                        continue
                                                    # check they share >= 2 keywords
                                                    m1_kw = keywords(id_to_market[pair[0]].get("question",""))
            m2_kw = keywords(id_to_market[pair[1]].get("question",""))
            shared = m1_kw & m2_kw
            if len(shared) >= 2:
                              seen.add(key)
                groups.append([id_to_market[pair[0]], id_to_market[pair[1]]])

    print(f"[{now()}] Found {len(groups)} related pairs to analyze")
    return groups

# ─── CLAUDE ANALYSIS ─────────────────────────────────────────────────────────

def analyze_group(group, client):
      """Send a group of markets to Claude for analysis."""
    market_summaries = []
    for m in group:
              title = m.get("question") or m.get("title") or "?"
        outcomes = m.get("outcomes", [])
        prices = m.get("outcomePrices", [])
        volume = float(m.get("volume", 0))
        end_date = m.get("endDate", m.get("endDateIso","?"))
        rules = m.get("description", "")[:400]

        price_str = ""
        if outcomes and prices:
                      price_str = " | ".join(f"{o}: {float(p)*100:.0f}c"
                                                                                for o, p in zip(outcomes, prices))

        market_summaries.append(
                      f"MARCHE: {title}\n"
                      f"Volume: ${volume:,.0f} | Expiration: {end_date}\n"
                      f"Prix: {price_str}\n"
                      f"Regles: {rules}\n"
        )

    user_msg = "Analyse ce groupe de marchés liés :\n\n" + "\n---\n".join(market_summaries)

    try:
              response = client.messages.create(
                            model="claude-sonnet-4-5",
                            max_tokens=1000,
                            system=SYSTEM_PROMPT,
                            messages=[{"role": "user", "content": user_msg}]
              )
        raw = response.content[0].text.strip()
        # strip possible ```json fences
        raw = raw.replace("```json","").replace("```","").strip()
        return json.loads(raw)
except json.JSONDecodeError:
        return None
except Exception as e:
        print(f"[{now()}] Claude error: {e}")
        return None

# ─── TELEGRAM ────────────────────────────────────────────────────────────────

def send_telegram(result, group):
      if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
                print(f"[{now()}] (Telegram non configure) Opportunite: {result['titre']}")
        return

    j1 = result["position"]["jambe1"]
    j2 = result["position"]["jambe2"]
    scenarios_str = "\n".join(
              f"  {'V' if s['gagnant'] else 'X'} {s['nom']} ({s['probabilite_pct']}%) -> {'+' if s['pnl_pct']>=0 else ''}{s['pnl_pct']}%"
              for s in result.get("scenarios", [])
    )

    text = (
              f"Opportunite Polymarket - Score {result['score']}/10\n\n"
              f"{result['titre']}\n\n"
              f"{result['these']}\n\n"
              f"Position :\n"
              f"- Jambe 1: {j1['direction']} {j1['marche'][:50]} -> {j1['allocation_pct']}%\n"
              f"- Jambe 2: {j2['direction']} {j2['marche'][:50]} -> {j2['allocation_pct']}%\n\n"
              f"Scenarios :\n{scenarios_str}\n\n"
              f"Signal de sortie : {result['signal_sortie']}\n\n"
              f"Scenario perdant ({result['probabilite_perdant_pct']}%) : {result['scenario_perdant']}"
    )

    try:
              requests.post(
                            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
                            json={"chat_id": TELEGRAM_CHAT_ID, "text": text},
                            timeout=10
              )
        print(f"[{now()}] Telegram alert sent: {result['titre']}")
except Exception as e:
        print(f"[{now()}] Telegram error: {e}")

# ─── MAIN LOOP ───────────────────────────────────────────────────────────────

def now():
      return datetime.now().strftime("%H:%M:%S")

def run_scan(client):
      print(f"\n[{now()}] Starting scan")
    markets = fetch_markets()
    if not markets:
              return

    groups = group_markets_by_theme(markets)
    opportunities = []

    for i, group in enumerate(groups):
              result = analyze_group(group, client)
        if not result:
                      continue

        score = result.get("score", 0)
        if result.get("opportunite") and score >= MIN_OPPORTUNITY_SCORE:
                      print(f"[{now()}] Score {score}/10 - {result.get('titre','?')}")
            send_telegram(result, group)
            opportunities.append(result)
else:
            pass  # silent skip

        # avoid hitting rate limits
        if (i+1) % 10 == 0:
                      time.sleep(2)

    print(f"[{now()}] Scan done. {len(opportunities)} opportunities found out of {len(groups)} pairs.")
    return opportunities

def main():
      if not ANTHROPIC_API_KEY:
                print("ERROR: Set ANTHROPIC_API_KEY environment variable")
        return

    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
    print("Polymarket Arbitrage Scanner started")
    print(f"Scan interval: every {SCAN_INTERVAL_H}h | Min volume: ${MIN_VOLUME} | Min score: {MIN_OPPORTUNITY_SCORE}/10")

    while True:
              run_scan(client)
        print(f"[{now()}] Next scan in {SCAN_INTERVAL_H}h...")
        time.sleep(SCAN_INTERVAL_H * 3600)

if __name__ == "__main__":
      main()
