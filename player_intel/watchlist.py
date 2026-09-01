"""Watchlist of Polymarket proxy wallets (public addresses only)."""

from __future__ import annotations

import json
import logging
import os
import re
from dataclasses import dataclass
from typing import Iterable, Optional

logger = logging.getLogger(__name__)

ADDRESS_RE = re.compile(r"^0x[a-fA-F0-9]{40}$", re.IGNORECASE)


@dataclass(frozen=True)
class WatchedWallet:
    address: str
    alias: str = ""

    def __post_init__(self):
        object.__setattr__(self, "address", self.address.lower())

    @property
    def label(self) -> str:
        return self.alias or self.address[:10]


def normalize_address(raw: str) -> Optional[str]:
    value = (raw or "").strip()
    if ADDRESS_RE.match(value):
        return value.lower()
    return None


def parse_watchlist_entry(raw: str) -> Optional[WatchedWallet]:
    """Parse ``0xabc...`` or ``0xabc...:Alias``."""
    text = (raw or "").strip()
    if not text:
        return None
    address_part, _, alias = text.partition(":")
    address = normalize_address(address_part.strip())
    if not address:
        logger.warning("Skipping invalid watchlist entry: %s", raw)
        return None
    return WatchedWallet(address=address, alias=alias.strip())


def parse_watchlist_env(raw: str) -> list[WatchedWallet]:
    """Parse comma-separated ``POLYMARKET_WATCHLIST`` values."""
    wallets: list[WatchedWallet] = []
    seen: set[str] = set()
    for chunk in (raw or "").split(","):
        entry = parse_watchlist_entry(chunk)
        if not entry or entry.address in seen:
            continue
        seen.add(entry.address)
        wallets.append(entry)
    return wallets


def load_watchlist_file(path: str) -> list[WatchedWallet]:
    """Load a JSON watchlist: list of addresses or ``{address, alias}`` objects."""
    if not path or not os.path.exists(path):
        return []
    try:
        with open(path, "r", encoding="utf-8") as handle:
            payload = json.load(handle)
    except (OSError, json.JSONDecodeError) as exc:
        logger.warning("Could not read watchlist file %s: %s", path, exc)
        return []

    rows: Iterable
    if isinstance(payload, dict):
        rows = payload.get("wallets") or payload.get("watchlist") or []
    elif isinstance(payload, list):
        rows = payload
    else:
        return []

    wallets: list[WatchedWallet] = []
    seen: set[str] = set()
    for row in rows:
        entry: Optional[WatchedWallet] = None
        if isinstance(row, str):
            entry = parse_watchlist_entry(row)
        elif isinstance(row, dict):
            address = row.get("address") or row.get("wallet") or ""
            alias = row.get("alias") or row.get("name") or ""
            entry = parse_watchlist_entry(f"{address}:{alias}" if alias else address)
        if not entry or entry.address in seen:
            continue
        seen.add(entry.address)
        wallets.append(entry)
    return wallets


def load_watchlist(
    env_value: Optional[str] = None,
    file_path: Optional[str] = None,
) -> list[WatchedWallet]:
    """Merge env + file watchlists (file aliases win on duplicate addresses)."""
    merged: dict[str, WatchedWallet] = {}
    for wallet in parse_watchlist_env(env_value or ""):
        merged[wallet.address] = wallet
    for wallet in load_watchlist_file(file_path or ""):
        merged[wallet.address] = wallet
    return list(merged.values())
