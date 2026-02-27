#!/usr/bin/env python3
"""
🚀 Market Intelligence Bot
===========================
Monitors Polymarket, Kalshi, and RSS feeds for significant gaps and moves.
Sends multilingual alerts (EN/HE/FR) to Telegram.

Usage:
    python main.py              # Run continuously (scheduler)
    python main.py --once       # Run a single scan and exit
"""

import asyncio
import logging
import signal
import sys
from datetime import datetime, timezone

import aiohttp
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger

from config import Config
from state import BotState
from sources import polymarket, kalshi
from sources.rss_monitor import fetch_all_feeds
from alerts.analyzer import detect_gaps, detect_big_moves
from alerts.telegram_bot import TelegramNotifier

# ── Logging ──────────────────────────────────────────────
logging.basicConfig(
    level=getattr(logging, Config.LOG_LEVEL, logging.INFO),
    format="%(asctime)s │ %(levelname)-7s │ %(name)-25s │ %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("bot")

# Quiet noisy loggers
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("telegram").setLevel(logging.WARNING)
logging.getLogger("apscheduler").setLevel(logging.WARNING)


# ── Main scan ────────────────────────────────────────────
async def run_scan(state: BotState, notifier: TelegramNotifier) -> None:
    """Execute one full scan cycle."""
    logger.info(f"═══ Scan #{state.run_count + 1} starting ═══")
    alerts_sent = 0

    try:
        async with aiohttp.ClientSession() as session:
            # ── 1. Fetch markets ──────────────────────────
            logger.info("Fetching Polymarket...")
            poly_events = await polymarket.fetch_active_markets(session)

            logger.info("Fetching Kalshi...")
            kalshi_markets = await kalshi.fetch_active_markets(session)

            logger.info(f"Got {len(poly_events)} Polymarket events, "
                       f"{len(kalshi_markets)} Kalshi markets")

            # ── 2. Detect cross-platform gaps ─────────────
            gap_alerts = detect_gaps(poly_events, kalshi_markets)
            for alert in gap_alerts[:5]:  # Max 5 gap alerts per cycle
                logger.info(f"📊 GAP: {alert.market_name} — {alert.gap_bps} bps")
                await notifier.send_gap_alert(alert)
                alerts_sent += 1

            # ── 3. Detect big moves ───────────────────────
            current_prices = {}
            current_info = {}

            for pe in poly_events:
                price = polymarket.get_yes_price(pe)
                if price is not None:
                    key = f"poly_{pe.id}"
                    current_prices[key] = price
                    current_info[key] = {
                        "title": pe.title,
                        "category": pe.category,
                        "source": "Polymarket",
                        "url": pe.url,
                    }

            for km in kalshi_markets:
                key = f"kalshi_{km.id}"
                current_prices[key] = km.yes_price
                current_info[key] = {
                    "title": km.title,
                    "category": km.category,
                    "source": "Kalshi",
                    "url": km.url,
                }

            move_alerts = detect_big_moves(
                current_prices,
                state.previous_prices,
                {**state.market_info, **current_info},
            )
            for alert in move_alerts[:5]:  # Max 5 move alerts per cycle
                logger.info(f"⚡ MOVE: {alert.market_name} — {alert.delta_bps} bps")
                await notifier.send_big_move_alert(alert)
                alerts_sent += 1

            # Update stored prices
            state.update_prices(current_prices, current_info)

            # ── 4. Check RSS feeds ────────────────────────
            logger.info("Checking RSS feeds...")
            last_run = state.get_last_run()
            rss_items = await fetch_all_feeds(session, since=last_run)

            rss_sent = 0
            for item in rss_items:
                if state.is_rss_seen(item.guid):
                    continue
                if rss_sent >= 3:  # Max 3 RSS alerts per cycle
                    break
                logger.info(f"📰 RSS: [{item.feed_name}] {item.title}")
                await notifier.send_rss_alert(item)
                state.mark_rss_seen(item.guid)
                alerts_sent += 1
                rss_sent += 1

            # ── 5. Heartbeat ─────────────────────────────
            if state.should_heartbeat():
                feed_count = sum(len(feeds) for feeds in Config.RSS_FEEDS.values())
                await notifier.send_heartbeat(
                    market_count=len(current_prices),
                    feed_count=feed_count,
                )

            # ── 6. Persist state ──────────────────────────
            state.mark_run()
            state.save()

            if alerts_sent == 0:
                logger.info("No significant alerts this cycle.")
                # Don't spam "no alerts" every cycle — only log it
            else:
                logger.info(f"Sent {alerts_sent} alerts this cycle.")

    except Exception as e:
        logger.exception(f"Scan error: {e}")
        try:
            await notifier.send_error(str(e))
        except Exception:
            pass


# ── Entry point ──────────────────────────────────────────
async def main():
    state = BotState()
    notifier = TelegramNotifier()

    # Send startup message
    logger.info("🚀 Bot starting up...")
    await notifier.send_startup()

    # Single run mode
    if "--once" in sys.argv:
        await run_scan(state, notifier)
        return

    # Scheduled mode
    scheduler = AsyncIOScheduler()
    scheduler.add_job(
        run_scan,
        trigger=IntervalTrigger(minutes=Config.CHECK_INTERVAL_MINUTES),
        args=[state, notifier],
        id="market_scan",
        name="Market Intelligence Scan",
        max_instances=1,
        misfire_grace_time=300,  # 5 min grace for crash resilience
    )
    scheduler.start()

    # Run first scan immediately
    await run_scan(state, notifier)

    # Keep alive
    logger.info(f"Scheduler active — next scan in {Config.CHECK_INTERVAL_MINUTES} min")
    stop_event = asyncio.Event()

    def handle_signal(sig, frame):
        logger.info(f"Received signal {sig}, shutting down...")
        stop_event.set()

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    await stop_event.wait()
    scheduler.shutdown(wait=False)
    logger.info("Bot stopped.")


if __name__ == "__main__":
    asyncio.run(main())
