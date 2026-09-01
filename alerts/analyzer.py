"""Analyzer: cross-platform gap detection, big move alerts, and market matching."""

import logging
import re
from dataclasses import dataclass
from difflib import SequenceMatcher
from typing import Optional

from config import Config
from sources.polymarket import PolymarketEvent, get_yes_price
from sources.kalshi import KalshiMarket, normalize_title

logger = logging.getLogger(__name__)


@dataclass
class GapAlert:
    """Alert for a price gap between Polymarket and Kalshi on the same market."""
    market_name: str
    category: str
    poly_price: float    # 0-100 (percentage)
    kalshi_price: float  # 0-100
    gap_bps: int         # basis points
    direction: str       # "Poly > Kalshi" or "Kalshi > Poly"
    poly_url: str
    kalshi_url: str


@dataclass
class BigMoveAlert:
    """Alert for a significant price move on a single platform."""
    market_name: str
    category: str
    source: str          # "Polymarket" or "Kalshi"
    old_price: float     # 0-100
    new_price: float     # 0-100
    delta_bps: int
    timeframe: str
    url: str


def similarity(a: str, b: str) -> float:
    """Compute string similarity ratio between two market titles."""
    return SequenceMatcher(None, normalize_title(a), normalize_title(b)).ratio()


def match_markets(
    poly_events: list[PolymarketEvent],
    kalshi_markets: list[KalshiMarket],
    threshold: float = 0.55,
) -> list[tuple[PolymarketEvent, KalshiMarket, float]]:
    """Match markets across platforms using fuzzy title matching."""
    matches = []
    used_kalshi = set()

    for pe in poly_events:
        best_match = None
        best_score = 0.0
        pe_normalized = normalize_title(pe.title)

        for km in kalshi_markets:
            if km.id in used_kalshi:
                continue
            km_normalized = normalize_title(km.title)
            score = similarity(pe.title, km.title)

            # Boost score if category matches
            if pe.category == km.category and pe.category != "other":
                score += 0.1

            # Check subtitle too
            if km.subtitle:
                alt_score = similarity(pe.title, km.subtitle)
                score = max(score, alt_score)

            if score > best_score:
                best_score = score
                best_match = km

        if best_match and best_score >= threshold:
            matches.append((pe, best_match, best_score))
            used_kalshi.add(best_match.id)

    logger.info(f"Matched {len(matches)} markets across platforms")
    return matches


def detect_gaps(
    poly_events: list[PolymarketEvent],
    kalshi_markets: list[KalshiMarket],
    threshold_bps: int = None,
) -> list[GapAlert]:
    """Detect price gaps above threshold between matched markets."""
    if threshold_bps is None:
        threshold_bps = Config.ALERT_THRESHOLD_BPS

    alerts = []
    matches = match_markets(poly_events, kalshi_markets)

    for pe, km, match_score in matches:
        poly_yes = get_yes_price(pe)
        if poly_yes is None:
            continue

        # Both in 0-1 scale, convert to percentage
        poly_pct = poly_yes * 100
        kalshi_pct = km.yes_price * 100

        gap_bps = abs(poly_pct - kalshi_pct) * 100  # 1% = 100bps

        if gap_bps >= threshold_bps:
            direction = "Poly > Kalshi" if poly_pct > kalshi_pct else "Kalshi > Poly"
            alerts.append(GapAlert(
                market_name=pe.title,
                category=pe.category,
                poly_price=round(poly_pct, 1),
                kalshi_price=round(kalshi_pct, 1),
                gap_bps=round(gap_bps),
                direction=direction,
                poly_url=pe.url,
                kalshi_url=km.url,
            ))
            logger.info(
                f"GAP DETECTED: {pe.title} — Poly {poly_pct:.1f}% vs Kalshi {kalshi_pct:.1f}% "
                f"({gap_bps:.0f} bps)"
            )

    alerts.sort(key=lambda a: a.gap_bps, reverse=True)
    return alerts


def detect_big_moves(
    current_prices: dict[str, float],
    previous_prices: dict[str, float],
    market_info: dict[str, dict],
    threshold_bps: int = None,
) -> list[BigMoveAlert]:
    """Detect big price moves by comparing current vs previous prices."""
    if threshold_bps is None:
        threshold_bps = Config.ALERT_THRESHOLD_BPS

    alerts = []
    for market_id, new_price in current_prices.items():
        old_price = previous_prices.get(market_id)
        if old_price is None:
            continue

        delta_bps = abs(new_price - old_price) * 100 * 100  # 0-1 → bps

        if delta_bps >= threshold_bps:
            info = market_info.get(market_id, {})
            alerts.append(BigMoveAlert(
                market_name=info.get("title", market_id),
                category=info.get("category", "other"),
                source=info.get("source", "Unknown"),
                old_price=round(old_price * 100, 1),
                new_price=round(new_price * 100, 1),
                delta_bps=round(delta_bps),
                timeframe=f"{Config.CHECK_INTERVAL_MINUTES} min",
                url=info.get("url", ""),
            ))

    alerts.sort(key=lambda a: a.delta_bps, reverse=True)
    return alerts
