"""Detect notable player opens/closes and unusual leaderboard size.

Pure functions over snapshots so tests can run with no network.
"""

from __future__ import annotations

from typing import Iterable, Optional

from player_intel.models import LeaderboardEntry, PlayerSignal, Position, Trade, WalletSnapshot
from player_intel.watchlist import WatchedWallet


def _display_name(snapshot: WalletSnapshot, wallet: Optional[WatchedWallet] = None) -> str:
    if wallet and wallet.alias:
        return wallet.alias
    return snapshot.display_name or snapshot.alias or snapshot.address[:10]


def _signal_from_position(
    kind: str,
    snapshot: WalletSnapshot,
    position: Position,
    *,
    side: str,
    extra: Optional[dict] = None,
    alias: str = "",
) -> PlayerSignal:
    return PlayerSignal(
        kind=kind,
        wallet=snapshot.address,
        alias=alias or snapshot.alias,
        user_name=_display_name(snapshot),
        title=position.title,
        outcome=position.outcome,
        side=side,
        size=position.size,
        price=position.price,
        notional=position.notional,
        pnl=position.cash_pnl if kind != "close" else position.realized_pnl,
        slug=position.slug,
        event_slug=position.event_slug,
        condition_id=position.condition_id,
        asset=position.asset,
        extra=extra or {},
    )


def detect_position_changes(
    previous: dict[str, Position],
    snapshot: WalletSnapshot,
    notable_usd: float,
    alias: str = "",
) -> list[PlayerSignal]:
    """Compare last-scan positions to current ones for one wallet.

    First-seen wallets should pass an empty previous map only after seeding;
    callers skip detection when there is no prior snapshot.
    """
    signals: list[PlayerSignal] = []
    current = snapshot.position_map

    for key, pos in current.items():
        prev = previous.get(key)
        if prev is None:
            if pos.notional >= notable_usd:
                signals.append(
                    _signal_from_position("open", snapshot, pos, side="BUY", alias=alias)
                )
            continue
        delta = pos.notional - prev.notional
        if delta >= notable_usd:
            signals.append(
                _signal_from_position(
                    "increase",
                    snapshot,
                    pos,
                    side="BUY",
                    extra={"previous_notional": prev.notional, "delta_usd": delta},
                    alias=alias,
                )
            )

    for key, prev in previous.items():
        if key in current:
            continue
        if prev.notional >= notable_usd:
            signals.append(
                _signal_from_position(
                    "close",
                    snapshot,
                    prev,
                    side="SELL",
                    extra={"closed_notional": prev.notional},
                    alias=alias,
                )
            )
    return signals


def detect_new_trades(
    snapshot: WalletSnapshot,
    last_trade_ts: int,
    notable_usd: float,
    already: Iterable[PlayerSignal],
    alias: str = "",
) -> list[PlayerSignal]:
    """Flag new fills above the notable threshold, de-duped against position signals."""
    seen_keys = {(s.condition_id, s.asset, s.kind) for s in already}
    signals: list[PlayerSignal] = []
    for trade in snapshot.trades:
        if trade.timestamp <= last_trade_ts:
            continue
        if trade.notional < notable_usd:
            continue
        kind_guess = "open" if trade.side == "BUY" else "close"
        if (trade.condition_id, trade.asset, kind_guess) in seen_keys:
            continue
        if (trade.condition_id, trade.asset, "increase") in seen_keys:
            continue
        signals.append(
            PlayerSignal(
                kind="trade",
                wallet=snapshot.address,
                alias=alias or snapshot.alias,
                user_name=trade.name or _display_name(snapshot),
                title=trade.title,
                outcome=trade.outcome,
                side=trade.side,
                size=trade.size,
                price=trade.price,
                notional=trade.notional,
                slug=trade.slug,
                event_slug=trade.event_slug,
                condition_id=trade.condition_id,
                asset=trade.asset,
                extra={"timestamp": trade.timestamp, "tx": trade.transaction_hash},
            )
        )
    return signals


def detect_leaderboard_anomalies(
    previous: list[LeaderboardEntry],
    current: list[LeaderboardEntry],
    unusual_usd: float,
    unusual_mult: float = 3.0,
) -> list[PlayerSignal]:
    """Alert when a ranked name's volume or |PnL| jumps by an unusual amount."""
    prev_map = {e.proxy_wallet: e for e in previous if e.proxy_wallet}
    signals: list[PlayerSignal] = []

    for entry in current:
        if not entry.proxy_wallet:
            continue
        prev = prev_map.get(entry.proxy_wallet)
        if prev is None:
            # Brand-new name on the board with already-huge size.
            if abs(entry.vol) >= unusual_usd or abs(entry.pnl) >= unusual_usd:
                signals.append(
                    _leaderboard_signal(
                        entry,
                        extra={"reason": "new_entry", "previous_vol": 0, "previous_pnl": 0},
                    )
                )
            continue

        vol_delta = abs(entry.vol - prev.vol)
        pnl_delta = abs(entry.pnl - prev.pnl)
        vol_jump = prev.vol > 0 and entry.vol >= prev.vol * unusual_mult and vol_delta >= unusual_usd
        pnl_jump = abs(prev.pnl) > 0 and abs(entry.pnl) >= abs(prev.pnl) * unusual_mult and pnl_delta >= unusual_usd
        size_jump = vol_delta >= unusual_usd or pnl_delta >= unusual_usd

        if vol_jump or pnl_jump or (size_jump and (vol_delta >= unusual_usd * 2 or pnl_delta >= unusual_usd * 2)):
            signals.append(
                _leaderboard_signal(
                    entry,
                    extra={
                        "reason": "size_jump",
                        "previous_vol": prev.vol,
                        "previous_pnl": prev.pnl,
                        "vol_delta": vol_delta,
                        "pnl_delta": pnl_delta,
                    },
                )
            )
    return signals


def _leaderboard_signal(entry: LeaderboardEntry, extra: dict) -> PlayerSignal:
    return PlayerSignal(
        kind="leaderboard",
        wallet=entry.proxy_wallet,
        alias=entry.user_name,
        user_name=entry.user_name,
        title=f"Leaderboard #{entry.rank} {entry.user_name}",
        outcome="",
        side="",
        size=0.0,
        price=0.0,
        notional=abs(entry.vol),
        pnl=entry.pnl,
        rank=entry.rank,
        vol=entry.vol,
        extra=extra,
    )


def max_trade_timestamp(trades: list[Trade]) -> int:
    if not trades:
        return 0
    return max(t.timestamp for t in trades)
