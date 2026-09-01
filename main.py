#!/usr/bin/env python3
"""
Market Intelligence Bot + Polymarket player tracker (paper only)
================================================================
Monitors Polymarket, Kalshi, and RSS feeds for gaps/moves, and tracks
named Polymarket wallets via the public Data API.

Every player signal is recorded in a paper ledger. This process never
places live orders and never uses private keys.

Usage:
    python main.py                 # Run continuously (scheduler)
    python main.py --once          # One full scan and exit
    python main.py --once --players-only
    python main.py --once --market-only
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import signal
import sys

import aiohttp
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger

from alerts.analyzer import detect_big_moves, detect_gaps
from alerts.telegram_bot import TelegramNotifier
from config import Config
from paper.ledger import PaperLedger
from player_intel.client import DataAPIClient
from player_intel.scanner import (
    detect_leaderboard_anomalies,
    detect_new_trades,
    detect_position_changes,
    max_trade_timestamp,
)
from player_intel.watchlist import load_watchlist
from sources import kalshi, polymarket
from sources.rss_monitor import fetch_all_feeds
from state import BotState

logging.basicConfig(
    level=getattr(logging, Config.LOG_LEVEL, logging.INFO),
    format="%(asctime)s │ %(levelname)-7s │ %(name)-25s │ %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("bot")

logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("telegram").setLevel(logging.WARNING)
logging.getLogger("apscheduler").setLevel(logging.WARNING)


async def run_player_scan(
    session: aiohttp.ClientSession,
    state: BotState,
    notifier: TelegramNotifier,
    ledger: PaperLedger,
) -> int:
    """Scan watchlist wallets + leaderboard. Returns number of alerts sent."""
    alerts_sent = 0
    client = DataAPIClient(session=session, base_url=Config.POLYMARKET_DATA_API)
    watchlist = load_watchlist(Config.POLYMARKET_WATCHLIST, Config.WATCHLIST_FILE)
    logger.info("Player intel: %s watched wallet(s)", len(watchlist))

    board = await client.get_leaderboard(
        time_period=Config.PLAYER_LEADERBOARD_PERIOD,
        order_by="PNL",
        limit=Config.PLAYER_LEADERBOARD_LIMIT,
    )
    logger.info(
        "Leaderboard: %s traders (period=%s)",
        len(board),
        Config.PLAYER_LEADERBOARD_PERIOD,
    )
    previous_board = state.get_leaderboard()
    if previous_board:
        for signal in detect_leaderboard_anomalies(
            previous_board, board, Config.PLAYER_LEADERBOARD_UNUSUAL_USD
        ):
            fill = ledger.record_signal(signal)
            logger.info("LEADERBOARD: %s vol=$%.0f pnl=$%.0f", signal.label, signal.vol, signal.pnl)
            await notifier.send_player_alert(signal, fill)
            alerts_sent += 1
    state.update_leaderboard(board)

    for wallet in watchlist:
        try:
            snapshot = await client.get_wallet_snapshot(
                wallet.address,
                alias=wallet.alias,
                position_limit=Config.PLAYER_POSITION_LIMIT,
                trade_limit=Config.PLAYER_TRADE_LIMIT,
            )
        except Exception:
            logger.exception("Failed to fetch snapshot for %s", wallet.address)
            continue

        value = snapshot.pnl.portfolio_value if snapshot.pnl else 0.0
        logger.info(
            "Wallet %s (%s): %s positions, value=$%.0f, unrealized=$%.0f",
            wallet.label,
            wallet.address[:10],
            len(snapshot.positions),
            value,
            snapshot.pnl.unrealized_pnl if snapshot.pnl else 0.0,
        )

        if not state.has_player_snapshot(wallet.address):
            logger.info("Seeding player snapshot for %s (no alerts on first see)", wallet.label)
            state.update_player_snapshot(
                wallet.address,
                snapshot.position_map,
                max_trade_timestamp(snapshot.trades),
                alias=wallet.alias,
                value=value,
            )
            await asyncio.sleep(0.15)
            continue

        previous = state.get_player_positions(wallet.address)
        last_ts = state.get_last_trade_ts(wallet.address)
        signals = detect_position_changes(
            previous, snapshot, Config.PLAYER_NOTABLE_USD, alias=wallet.alias
        )
        signals.extend(
            detect_new_trades(
                snapshot,
                last_ts,
                Config.PLAYER_NOTABLE_USD,
                signals,
                alias=wallet.alias,
            )
        )
        for signal in signals:
            fill = ledger.record_signal(signal)
            logger.info(
                "PLAYER %s %s %s $%.0f",
                signal.kind,
                signal.label,
                signal.title[:50],
                signal.notional,
            )
            await notifier.send_player_alert(signal, fill)
            alerts_sent += 1

        state.update_player_snapshot(
            wallet.address,
            snapshot.position_map,
            max(last_ts, max_trade_timestamp(snapshot.trades)),
            alias=wallet.alias,
            value=value,
        )
        await asyncio.sleep(0.15)

    return alerts_sent


async def run_market_scan(
    session: aiohttp.ClientSession,
    state: BotState,
    notifier: TelegramNotifier,
) -> tuple[int, int]:
    """Gap / big-move / RSS cycle. Returns (alerts_sent, market_count)."""
    alerts_sent = 0

    logger.info("Fetching Polymarket...")
    poly_events = await polymarket.fetch_active_markets(session)

    logger.info("Fetching Kalshi...")
    kalshi_markets = await kalshi.fetch_active_markets(session)

    logger.info(
        "Got %s Polymarket events, %s Kalshi markets",
        len(poly_events),
        len(kalshi_markets),
    )

    gap_alerts = detect_gaps(poly_events, kalshi_markets)
    for alert in gap_alerts[:5]:
        logger.info("GAP: %s — %s bps", alert.market_name, alert.gap_bps)
        await notifier.send_gap_alert(alert)
        alerts_sent += 1

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
    for alert in move_alerts[:5]:
        logger.info("MOVE: %s — %s bps", alert.market_name, alert.delta_bps)
        await notifier.send_big_move_alert(alert)
        alerts_sent += 1

    state.update_prices(current_prices, current_info)

    logger.info("Checking RSS feeds...")
    last_run = state.get_last_run()
    rss_items = await fetch_all_feeds(session, since=last_run)

    rss_sent = 0
    for item in rss_items:
        if state.is_rss_seen(item.guid):
            continue
        if rss_sent >= 3:
            break
        logger.info("RSS: [%s] %s", item.feed_name, item.title)
        await notifier.send_rss_alert(item)
        state.mark_rss_seen(item.guid)
        alerts_sent += 1
        rss_sent += 1

    return alerts_sent, len(current_prices)


async def run_scan(
    state: BotState,
    notifier: TelegramNotifier,
    ledger: PaperLedger,
    *,
    run_markets: bool = True,
    run_players: bool = True,
) -> None:
    """Execute one full scan cycle."""
    logger.info("═══ Scan #%s starting ═══", state.run_count + 1)
    alerts_sent = 0
    market_count = len(state.previous_prices)
    watchlist = load_watchlist(Config.POLYMARKET_WATCHLIST, Config.WATCHLIST_FILE)

    try:
        async with aiohttp.ClientSession() as session:
            if run_players and Config.ENABLE_PLAYER_INTEL:
                alerts_sent += await run_player_scan(session, state, notifier, ledger)

            if run_markets and Config.ENABLE_MARKET_INTEL:
                market_alerts, market_count = await run_market_scan(session, state, notifier)
                alerts_sent += market_alerts

            if state.should_heartbeat():
                feed_count = sum(len(feeds) for feeds in Config.RSS_FEEDS.values())
                summary = ledger.summary()
                await notifier.send_heartbeat(
                    market_count=market_count,
                    feed_count=feed_count,
                    watchlist_count=len(watchlist),
                    paper_fills=summary["fills"],
                    paper_open_notional=summary["open_notional"],
                )

            state.mark_run()
            state.save()

            if alerts_sent == 0:
                logger.info("No significant alerts this cycle.")
            else:
                logger.info("Sent %s alerts this cycle. Paper: %s", alerts_sent, ledger.summary())

    except Exception as e:
        logger.exception("Scan error: %s", e)
        try:
            await notifier.send_error(str(e))
        except Exception:
            pass


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Market intel + Polymarket player tracker (paper only)")
    parser.add_argument("--once", action="store_true", help="Run a single scan and exit")
    parser.add_argument("--players-only", action="store_true", help="Skip gap/RSS market intel")
    parser.add_argument("--market-only", action="store_true", help="Skip player intel")
    return parser.parse_args(argv)


async def async_main(argv: list[str]) -> None:
    args = parse_args(argv)
    run_markets = not args.players_only
    run_players = not args.market_only

    state = BotState()
    notifier = TelegramNotifier()
    ledger = PaperLedger(
        Config.PAPER_LEDGER_FILE,
        max_usd=Config.PAPER_MAX_USD,
        copy_ratio=Config.PAPER_COPY_RATIO,
    )
    watchlist = load_watchlist(Config.POLYMARKET_WATCHLIST, Config.WATCHLIST_FILE)

    logger.info("Bot starting up (paper player tracking, no live orders)")
    logger.info("State file: %s", state.state_file)
    logger.info("Paper ledger: %s", ledger.path)
    logger.info("Watchlist: %s wallet(s)", len(watchlist))
    await notifier.send_startup(watchlist_count=len(watchlist))

    if args.once:
        await run_scan(
            state, notifier, ledger, run_markets=run_markets, run_players=run_players
        )
        return

    scheduler = AsyncIOScheduler()
    scheduler.add_job(
        run_scan,
        trigger=IntervalTrigger(minutes=Config.CHECK_INTERVAL_MINUTES),
        kwargs={
            "state": state,
            "notifier": notifier,
            "ledger": ledger,
            "run_markets": run_markets,
            "run_players": run_players,
        },
        id="market_scan",
        name="Market Intelligence Scan",
        max_instances=1,
        misfire_grace_time=300,
    )
    scheduler.start()

    await run_scan(
        state, notifier, ledger, run_markets=run_markets, run_players=run_players
    )

    logger.info("Scheduler active — next scan in %s min", Config.CHECK_INTERVAL_MINUTES)
    stop_event = asyncio.Event()

    def handle_signal(sig, frame):
        logger.info("Received signal %s, shutting down...", sig)
        stop_event.set()

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    await stop_event.wait()
    scheduler.shutdown(wait=False)
    logger.info("Bot stopped.")


def main() -> None:
    asyncio.run(async_main(sys.argv[1:]))


if __name__ == "__main__":
    main()
