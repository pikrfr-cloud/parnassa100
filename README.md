# 🚀 Market Intelligence Bot + Player Tracker

Prediction market monitoring bot that tracks **Polymarket**, **Kalshi**, and **RSS feeds**, plus a **paper-only Polymarket player/trader tracker**. Alerts are multilingual (EN 🇬🇧 / HE 🇮🇱 / FR 🇫🇷) over **Telegram**.

This repo does **not** place live orders. There are no private keys, no CLOB `SecureClient`, and no execution path. Every player signal is written to a paper ledger as if we had entered.

## Features

| Feature | Details |
|---------|---------|
| 👤 Player intel | Public Data API: leaderboard, per-wallet positions, trades, PnL |
| 📝 Paper ledger | Records size / price / time for each signal — zero live orders |
| 📊 Cross-platform gap detection | Fuzzy-matches markets across Polymarket ↔ Kalshi |
| ⚡ Big move alerts | Detects significant price moves between scan cycles |
| 📰 RSS monitoring | Central banks, legislation, crypto & political news |
| 🎯 Configurable threshold | Default: 15 bps (basis points) |
| 🌐 Trilingual | Every alert in English, Hebrew, French |
| 🛡️ Crash resilient | State persisted to disk, auto-restart via Docker |
| 💓 Heartbeat | Daily alive check |

## Quick Start (Docker)

### 1. Create a Telegram Bot

1. Message [@BotFather](https://t.me/BotFather) on Telegram
2. Send `/newbot` and follow the steps
3. Copy the **bot token**
4. Create a group/channel, add your bot, and get the **chat ID**
   - Send a message in the group, then visit:
     `https://api.telegram.org/bot<TOKEN>/getUpdates`
   - Find `"chat":{"id": -100XXXXXXXXXX}`

Telegram is optional. Without a token the bot still scans and writes the paper ledger; alerts are logged.

### 2. Configure

```bash
cp .env.example .env
```

Edit `.env` (see [Configuration](#configuration) and [Watchlist](#add-watchlist-wallets)).

### 3. Launch

```bash
docker compose up -d --build
```

### 4. View Logs

```bash
docker compose logs -f
```

### 5. Stop

```bash
docker compose down
```

## Run a scan (without Docker)

```bash
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
# Edit .env — at least POLYMARKET_WATCHLIST if you want wallet alerts
python main.py --once
```

Useful flags:

```bash
python main.py --once                 # one full scan (markets + players), then exit
python main.py --once --players-only  # leaderboard + watchlist only
python main.py --once --market-only   # gap / RSS / big-move only
python main.py                        # scheduler loop (CHECK_INTERVAL_MINUTES)
```

On first see of a wallet the bot **seeds** a snapshot and does not fire "open" alerts for every existing position. The next scan diffs against that snapshot.

State and the paper ledger default to `./data/` locally, or `/data/` inside Docker.

## Add watchlist wallets

Use **public Polymarket proxy addresses** only (the address on a profile / leaderboard). Never put a private key here.

**Option A — env var** (comma-separated, optional `:alias`):

```
POLYMARKET_WATCHLIST=0x56687bf447db6ffa42ffe2204a05edaa20f55839:ExampleTrader,0xabcabcabcabcabcabcabcabcabcabcabcabcabca:Whale
```

**Option B — JSON file** (copy `watchlist.example.json`):

```json
[
  {"address": "0x56687bf447db6ffa42ffe2204a05edaa20f55839", "alias": "ExampleTrader"}
]
```

```
WATCHLIST_FILE=./watchlist.json
```

File aliases win if the same address appears in both places.

Alerts fire when a watched wallet **opens**, **adds size to**, or **closes** a position whose notional is at least `PLAYER_NOTABLE_USD`, or when a leaderboard name shows unusual volume / PnL versus the last scan.

Each of those signals is copied into the paper ledger (capped by `PAPER_MAX_USD`).

## Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `TELEGRAM_BOT_TOKEN` | optional | Telegram bot token from BotFather |
| `TELEGRAM_CHAT_ID` | optional | Target chat/group/channel ID |
| `CHECK_INTERVAL_MINUTES` | `120` | Scan frequency (minutes) |
| `ALERT_THRESHOLD_BPS` | `15` | Min gap/move to trigger alert (basis points) |
| `LANGUAGES` | `en,he,fr` | Alert languages (comma-separated) |
| `ENABLE_PLAYER_INTEL` | `true` | Watchlist + leaderboard scan |
| `ENABLE_MARKET_INTEL` | `true` | Poly↔Kalshi gaps, big moves, RSS |
| `POLYMARKET_WATCHLIST` | empty | Public `0x` wallets, optional `:alias` |
| `WATCHLIST_FILE` | empty | JSON watchlist path |
| `PLAYER_NOTABLE_USD` | `500` | Min position/trade notional to alert |
| `PLAYER_LEADERBOARD_PERIOD` | `DAY` | `DAY`, `WEEK`, `MONTH`, or `ALL` |
| `PLAYER_LEADERBOARD_UNUSUAL_USD` | `25000` | Leaderboard vol/PnL jump to alert |
| `PAPER_MAX_USD` | `100` | Cap per paper fill |
| `PAPER_COPY_RATIO` | `1.0` | Scale player notional before the cap |
| `KALSHI_API_KEY` | optional | For authenticated Kalshi endpoints |
| `LOG_LEVEL` | `INFO` | Logging level |

## How It Works

```
Every N minutes:
  ├─ Player intel (public Data API, no keys)
  │    ├─ Fetch leaderboard → unusual size vs last scan → ALERT + paper note
  │    ├─ For each watchlist wallet: positions, trades, value/PnL
  │    ├─ Diff vs last snapshot → open / increase / close / notable trade
  │    └─ Record a paper fill (size, price, time) — never a live order
  ├─ Fetch Polymarket active markets (Gamma API)
  ├─ Fetch Kalshi active markets (public API)
  ├─ Fuzzy-match markets across platforms
  ├─ Detect gaps ≥ threshold → ALERT
  ├─ Compare prices to last scan → big moves → ALERT
  ├─ Fetch RSS feeds (central banks, news, legislation)
  ├─ Filter for market-relevant items → ALERT
  ├─ Save state + paper ledger to disk
  └─ Heartbeat every 24h
```

Player data comes from `https://data-api.polymarket.com` (`/v1/leaderboard`, `/positions`, `/trades`, `/value`, `/closed-positions`).

## Tests

```bash
pip install -r requirements.txt
pytest
```

API clients are mocked. The suite also checks that no live-order / private-key client slipped in.

## Alert Examples

**Player open (paper):**
```
👤 PLAYER OPEN — ExampleTrader

🟢 Watched wallet opened a notable position
👤 ExampleTrader
📊 Will Bitcoin reach $100k by Dec 2025?
🎯 Outcome: Yes
💵 Size: 2500.00 @ 0.720 ($1800)
📝 Paper fill: BUY 138.89 @ 0.720 ($100.00)
⚠️ Paper only — no live order placed
```

**Gap Alert:**
```
🔔 GAP ALERT — Will Bitcoin reach $100k by Dec 2025?

📊 Market: Will Bitcoin reach $100k by Dec 2025?
🏷️ Category: crypto

Polymarket: 72.3%
Kalshi: 55.1%
📐 Gap: 1720 bps
📈 Direction: Poly > Kalshi

🔗 Poly: https://polymarket.com/event/...
🔗 Kalshi: https://kalshi.com/markets/...
```

**Big Move Alert:**
```
⚡ BIG MOVE — Fed rate cut in March?

📊 Fed rate cut in March?
🏷️ Category: macro
Source: Polymarket

Before: 45.2% → Now: 62.8%
📐 Move: 1760 bps
⏱️ Timeframe: 120 min

🔗 https://polymarket.com/event/...
```

## Customization

### Add RSS Feeds
Edit `config.py` → `RSS_FEEDS` dictionary.

### Change Market Keywords
Edit `sources/rss_monitor.py` → `MARKET_KEYWORDS` list.

### Adjust Matching Sensitivity
Edit `alerts/analyzer.py` → `match_markets()` threshold parameter.

## License

MIT
