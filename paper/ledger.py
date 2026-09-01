"""Paper-only trade ledger.

Every player signal is recorded as if we entered. There is no CLOB client,
no private key, and no order-placement path in this module or this project.
"""

from __future__ import annotations

import json
import logging
import os
import uuid
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Any, Optional

from player_intel.models import PlayerSignal

logger = logging.getLogger(__name__)

PAPER_MODE = "paper"
LIVE_TRADING = False


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def paper_notional(player_notional: float, max_usd: float, copy_ratio: float) -> float:
    if player_notional <= 0:
        return 0.0
    return round(min(abs(player_notional) * copy_ratio, max_usd), 6)


def paper_size(notional: float, price: float) -> float:
    if price <= 0:
        return 0.0
    return round(notional / price, 6)


@dataclass
class PaperFill:
    id: str
    ts: str
    signal_kind: str
    wallet: str
    alias: str
    market: str
    outcome: str
    side: str
    size: float
    price: float
    notional: float
    player_notional: float
    status: str  # open, closed, noted
    condition_id: str = ""
    asset: str = ""
    market_url: str = ""
    signal_id: str = ""
    closed_ts: Optional[str] = None
    close_price: Optional[float] = None
    realized_pnl: Optional[float] = None
    mode: str = PAPER_MODE
    live_order: bool = False

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "PaperFill":
        fields = cls.__dataclass_fields__
        known = {key: data[key] for key in fields if key in data}
        known.setdefault("id", str(uuid.uuid4()))
        known.setdefault("ts", _now_iso())
        known.setdefault("signal_kind", "")
        known.setdefault("wallet", "")
        known.setdefault("alias", "")
        known.setdefault("market", "")
        known.setdefault("outcome", "")
        known.setdefault("side", "")
        known.setdefault("size", 0.0)
        known.setdefault("price", 0.0)
        known.setdefault("notional", 0.0)
        known.setdefault("player_notional", 0.0)
        known.setdefault("status", "noted")
        known["live_order"] = False
        known["mode"] = PAPER_MODE
        return cls(**known)


@dataclass
class PaperSignalRecord:
    id: str
    ts: str
    fill_id: Optional[str]
    signal: dict[str, Any]
    mode: str = PAPER_MODE

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


class PaperLedger:
    """JSON ledger of simulated fills. Never places live orders."""

    def __init__(self, path: str, max_usd: float = 100.0, copy_ratio: float = 1.0):
        self.path = path
        self.max_usd = max_usd
        self.copy_ratio = copy_ratio
        self.fills: list[PaperFill] = []
        self.signals: list[PaperSignalRecord] = []
        self._load()

    def _load(self) -> None:
        if not self.path or not os.path.exists(self.path):
            return
        try:
            with open(self.path, "r", encoding="utf-8") as handle:
                payload = json.load(handle)
        except (OSError, json.JSONDecodeError) as exc:
            logger.warning("Could not load paper ledger: %s", exc)
            return
        if payload.get("live_trading"):
            logger.error("Refusing to load a ledger marked live_trading=true")
            return
        self.fills = [PaperFill.from_dict(row) for row in payload.get("fills", [])]
        self.signals = [
            PaperSignalRecord(
                id=row.get("id", str(uuid.uuid4())),
                ts=row.get("ts", _now_iso()),
                fill_id=row.get("fill_id"),
                signal=row.get("signal") or {},
                mode=PAPER_MODE,
            )
            for row in payload.get("signals", [])
        ]

    def save(self) -> None:
        directory = os.path.dirname(self.path)
        if directory:
            os.makedirs(directory, exist_ok=True)
        payload = {
            "mode": PAPER_MODE,
            "live_trading": LIVE_TRADING,
            "fills": [fill.to_dict() for fill in self.fills],
            "signals": [record.to_dict() for record in self.signals],
        }
        tmp = f"{self.path}.tmp"
        with open(tmp, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2)
        os.replace(tmp, self.path)

    def open_fills(self) -> list[PaperFill]:
        return [fill for fill in self.fills if fill.status == "open"]

    def _matching_open(self, signal: PlayerSignal) -> Optional[PaperFill]:
        for fill in reversed(self.fills):
            if fill.status != "open":
                continue
            if fill.wallet != signal.wallet:
                continue
            if signal.asset and fill.asset == signal.asset:
                return fill
            if (
                signal.condition_id
                and fill.condition_id == signal.condition_id
                and fill.outcome == signal.outcome
            ):
                return fill
        return None

    def record_signal(self, signal: PlayerSignal) -> PaperFill:
        """Record a player signal as a paper fill (or a note for leaderboard)."""
        existing = None
        if signal.kind == "close":
            existing = self._matching_open(signal)
            if existing:
                fill = self._close_fill(existing, signal)
            else:
                fill = self._new_fill(signal, status="closed", side="SELL")
                self.fills.append(fill)
        elif signal.kind == "leaderboard":
            fill = self._new_fill(signal, status="noted", side="")
            self.fills.append(fill)
        else:
            fill = self._new_fill(signal, status="open", side=signal.side or "BUY")
            self.fills.append(fill)

        if fill.live_order:
            raise RuntimeError("Paper ledger produced a live_order flag — aborting")
        if fill.mode != PAPER_MODE:
            raise RuntimeError("Paper ledger fill is not in paper mode")

        record = PaperSignalRecord(
            id=str(uuid.uuid4()),
            ts=_now_iso(),
            fill_id=fill.id,
            signal=signal.to_dict(),
        )
        self.signals.append(record)
        self.save()
        logger.info(
            "PAPER %s %s size=%s @ %s notional=$%.2f wallet=%s [no live order]",
            fill.side or fill.signal_kind,
            (fill.market or "")[:60],
            fill.size,
            fill.price,
            fill.notional,
            fill.wallet[:10],
        )
        return fill

    def _new_fill(self, signal: PlayerSignal, status: str, side: str) -> PaperFill:
        notional = paper_notional(signal.notional, self.max_usd, self.copy_ratio)
        price = signal.price
        size = paper_size(notional, price) if price else 0.0
        return PaperFill(
            id=str(uuid.uuid4()),
            ts=_now_iso(),
            signal_kind=signal.kind,
            wallet=signal.wallet,
            alias=signal.label,
            market=signal.title,
            outcome=signal.outcome,
            side=side,
            size=size,
            price=price,
            notional=notional,
            player_notional=signal.notional,
            status=status,
            condition_id=signal.condition_id,
            asset=signal.asset,
            market_url=signal.market_url,
            live_order=False,
            mode=PAPER_MODE,
        )

    def _close_fill(self, fill: PaperFill, signal: PlayerSignal) -> PaperFill:
        fill.status = "closed"
        fill.closed_ts = _now_iso()
        fill.close_price = signal.price
        if fill.price and signal.price:
            fill.realized_pnl = round((signal.price - fill.price) * fill.size, 6)
        fill.signal_kind = "close"
        return fill

    def summary(self) -> dict[str, Any]:
        open_fills = self.open_fills()
        closed = [fill for fill in self.fills if fill.status == "closed"]
        realized = sum(fill.realized_pnl or 0.0 for fill in closed)
        return {
            "mode": PAPER_MODE,
            "live_trading": LIVE_TRADING,
            "fills": len(self.fills),
            "open": len(open_fills),
            "closed": len(closed),
            "signals": len(self.signals),
            "open_notional": round(sum(fill.notional for fill in open_fills), 2),
            "realized_pnl": round(realized, 2),
        }
