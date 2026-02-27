"""Persistent state management — survives restarts (crash resilience)."""

import json
import logging
import os
from datetime import datetime, timezone
from typing import Any, Optional

from config import Config

logger = logging.getLogger(__name__)


class BotState:
    """Manages persistent state: previous prices, seen RSS GUIDs, run count."""

    def __init__(self, state_file: str = None):
        self.state_file = state_file or Config.STATE_FILE
        self._state: dict[str, Any] = {
            "previous_prices": {},       # market_id → price (0-1)
            "market_info": {},           # market_id → {title, category, source, url}
            "seen_rss_guids": [],        # list of GUIDs already sent
            "last_run": None,            # ISO timestamp
            "run_count": 0,
            "last_heartbeat": None,
        }
        self._load()

    def _load(self):
        """Load state from disk."""
        if os.path.exists(self.state_file):
            try:
                with open(self.state_file, "r") as f:
                    saved = json.load(f)
                self._state.update(saved)
                logger.info(f"State loaded — run #{self._state['run_count']}, "
                           f"tracking {len(self._state['previous_prices'])} markets")
            except (json.JSONDecodeError, OSError) as e:
                logger.warning(f"Could not load state: {e}. Starting fresh.")

    def save(self):
        """Persist state to disk."""
        try:
            os.makedirs(os.path.dirname(self.state_file), exist_ok=True)
            with open(self.state_file, "w") as f:
                json.dump(self._state, f, indent=2, default=str)
        except OSError as e:
            logger.error(f"Could not save state: {e}")

    @property
    def previous_prices(self) -> dict[str, float]:
        return self._state["previous_prices"]

    @property
    def market_info(self) -> dict[str, dict]:
        return self._state["market_info"]

    def update_prices(self, new_prices: dict[str, float], info: dict[str, dict]):
        """Update stored prices and market info."""
        self._state["previous_prices"] = new_prices
        self._state["market_info"].update(info)
        # Trim old market_info entries not in current prices
        current_ids = set(new_prices.keys())
        self._state["market_info"] = {
            k: v for k, v in self._state["market_info"].items()
            if k in current_ids
        }

    def is_rss_seen(self, guid: str) -> bool:
        return guid in self._state["seen_rss_guids"]

    def mark_rss_seen(self, guid: str):
        self._state["seen_rss_guids"].append(guid)
        # Keep only last 5000 GUIDs to avoid unbounded growth
        if len(self._state["seen_rss_guids"]) > 5000:
            self._state["seen_rss_guids"] = self._state["seen_rss_guids"][-3000:]

    def get_last_run(self) -> Optional[datetime]:
        ts = self._state.get("last_run")
        if ts:
            try:
                return datetime.fromisoformat(ts)
            except (TypeError, ValueError):
                return None
        return None

    def mark_run(self):
        self._state["run_count"] += 1
        self._state["last_run"] = datetime.now(timezone.utc).isoformat()

    @property
    def run_count(self) -> int:
        return self._state["run_count"]

    def should_heartbeat(self, every_n_runs: int = 12) -> bool:
        """Send heartbeat every N runs (default: every 12 runs = 24h at 2h interval)."""
        return self._state["run_count"] % every_n_runs == 0
