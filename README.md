# 🚀 Market Intelligence Bot

Prediction market monitoring bot that tracks **Polymarket**, **Kalshi**, and **RSS feeds** — sending multilingual alerts (EN 🇬🇧 / HE 🇮🇱 / FR 🇫🇷) to **Telegram**.

## Features

| Feature | Details |
|---------|---------|
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

### 2. Configure

```bash
cp .env.example .env
```

Edit `.env`:
```
TELEGRAM_BOT_TOKEN=123456:ABC-DEF...
TELEGRAM_CHAT_ID=-100123456789
CHECK_INTERVAL_MINUTES=120
ALERT_THRESHOLD_BPS=15
LANGUAGES=en,he,fr
```

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

## Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `TELEGRAM_BOT_TOKEN` | required | Telegram bot token from BotFather |
| `TELEGRAM_CHAT_ID` | required | Target chat/group/channel ID |
| `CHECK_INTERVAL_MINUTES` | `120` | Scan frequency (minutes) |
| `ALERT_THRESHOLD_BPS` | `15` | Min gap/move to trigger alert (basis points) |
| `LANGUAGES` | `en,he,fr` | Alert languages (comma-separated) |
| `KALSHI_API_KEY` | optional | For authenticated Kalshi endpoints |
| `LOG_LEVEL` | `INFO` | Logging level |

## How It Works

```
Every 2 hours:
  ├─ Fetch Polymarket active markets (Gamma API)
  ├─ Fetch Kalshi active markets (public API)
  ├─ Fuzzy-match markets across platforms
  ├─ Detect gaps ≥ threshold → ALERT
  ├─ Compare prices to last scan → big moves → ALERT
  ├─ Fetch RSS feeds (central banks, news, legislation)
  ├─ Filter for market-relevant items → ALERT
  ├─ Save state to disk (crash resilience)
  └─ Heartbeat every 24h
```

## Alert Examples

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

## Run Without Docker

```bash
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
# Edit .env...
python main.py
```

Single scan (no scheduler):
```bash
python main.py --once
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
