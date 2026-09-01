"""Dataclasses for Polymarket Data API player intel."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any, Optional


def _f(value: Any, default: float = 0.0) -> float:
    try:
        if value is None or value == "":
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _s(value: Any, default: str = "") -> str:
    if value is None:
        return default
    return str(value)


def _i(value: Any, default: int = 0) -> int:
    try:
        if value is None or value == "":
            return default
        return int(float(value))
    except (TypeError, ValueError):
        return default


@dataclass
class Position:
    proxy_wallet: str
    asset: str
    condition_id: str
    size: float
    avg_price: float
    initial_value: float
    current_value: float
    cash_pnl: float
    realized_pnl: float
    cur_price: float
    title: str
    slug: str
    event_slug: str
    outcome: str
    outcome_index: int = 0
    end_date: str = ""
    percent_pnl: float = 0.0

    @property
    def key(self) -> str:
        return f"{self.condition_id}:{self.asset}"

    @property
    def notional(self) -> float:
        if self.current_value:
            return abs(self.current_value)
        if self.initial_value:
            return abs(self.initial_value)
        return abs(self.size * (self.cur_price or self.avg_price))

    @property
    def price(self) -> float:
        return self.cur_price or self.avg_price

    @property
    def market_url(self) -> str:
        if self.event_slug:
            return f"https://polymarket.com/event/{self.event_slug}"
        if self.slug:
            return f"https://polymarket.com/market/{self.slug}"
        return ""

    @classmethod
    def from_api(cls, data: dict[str, Any]) -> "Position":
        return cls(
            proxy_wallet=_s(data.get("proxyWallet")).lower(),
            asset=_s(data.get("asset")),
            condition_id=_s(data.get("conditionId")),
            size=_f(data.get("size")),
            avg_price=_f(data.get("avgPrice")),
            initial_value=_f(data.get("initialValue")),
            current_value=_f(data.get("currentValue")),
            cash_pnl=_f(data.get("cashPnl")),
            realized_pnl=_f(data.get("realizedPnl")),
            cur_price=_f(data.get("curPrice")),
            title=_s(data.get("title"), "Unknown market"),
            slug=_s(data.get("slug")),
            event_slug=_s(data.get("eventSlug")),
            outcome=_s(data.get("outcome")),
            outcome_index=_i(data.get("outcomeIndex")),
            end_date=_s(data.get("endDate")),
            percent_pnl=_f(data.get("percentPnl")),
        )

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "Position":
        if "proxyWallet" in data:
            return cls.from_api(data)
        return cls(
            proxy_wallet=_s(data.get("proxy_wallet")).lower(),
            asset=_s(data.get("asset")),
            condition_id=_s(data.get("condition_id")),
            size=_f(data.get("size")),
            avg_price=_f(data.get("avg_price")),
            initial_value=_f(data.get("initial_value")),
            current_value=_f(data.get("current_value")),
            cash_pnl=_f(data.get("cash_pnl")),
            realized_pnl=_f(data.get("realized_pnl")),
            cur_price=_f(data.get("cur_price")),
            title=_s(data.get("title"), "Unknown market"),
            slug=_s(data.get("slug")),
            event_slug=_s(data.get("event_slug")),
            outcome=_s(data.get("outcome")),
            outcome_index=_i(data.get("outcome_index")),
            end_date=_s(data.get("end_date")),
            percent_pnl=_f(data.get("percent_pnl")),
        )


@dataclass
class Trade:
    proxy_wallet: str
    side: str
    asset: str
    condition_id: str
    size: float
    price: float
    timestamp: int
    title: str
    slug: str
    event_slug: str
    outcome: str
    name: str = ""
    transaction_hash: str = ""

    @property
    def notional(self) -> float:
        return abs(self.size * self.price)

    @property
    def key(self) -> str:
        return f"{self.condition_id}:{self.asset}"

    @property
    def market_url(self) -> str:
        if self.event_slug:
            return f"https://polymarket.com/event/{self.event_slug}"
        if self.slug:
            return f"https://polymarket.com/market/{self.slug}"
        return ""

    @classmethod
    def from_api(cls, data: dict[str, Any]) -> "Trade":
        return cls(
            proxy_wallet=_s(data.get("proxyWallet")).lower(),
            side=_s(data.get("side"), "BUY").upper(),
            asset=_s(data.get("asset")),
            condition_id=_s(data.get("conditionId")),
            size=_f(data.get("size")),
            price=_f(data.get("price")),
            timestamp=_i(data.get("timestamp")),
            title=_s(data.get("title"), "Unknown market"),
            slug=_s(data.get("slug")),
            event_slug=_s(data.get("eventSlug")),
            outcome=_s(data.get("outcome")),
            name=_s(data.get("name") or data.get("pseudonym")),
            transaction_hash=_s(data.get("transactionHash")),
        )


@dataclass
class LeaderboardEntry:
    rank: str
    proxy_wallet: str
    user_name: str
    vol: float
    pnl: float
    verified_badge: bool = False
    x_username: str = ""

    @classmethod
    def from_api(cls, data: dict[str, Any]) -> "LeaderboardEntry":
        wallet = _s(data.get("proxyWallet") or data.get("wallet")).lower()
        return cls(
            rank=_s(data.get("rank")),
            proxy_wallet=wallet,
            user_name=_s(data.get("userName") or data.get("name") or wallet[:10]),
            vol=_f(data.get("vol")),
            pnl=_f(data.get("pnl")),
            verified_badge=bool(data.get("verifiedBadge")),
            x_username=_s(data.get("xUsername")),
        )

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "LeaderboardEntry":
        if "proxyWallet" in data or "userName" in data:
            return cls.from_api(data)
        return cls(
            rank=_s(data.get("rank")),
            proxy_wallet=_s(data.get("proxy_wallet")).lower(),
            user_name=_s(data.get("user_name")),
            vol=_f(data.get("vol")),
            pnl=_f(data.get("pnl")),
            verified_badge=bool(data.get("verified_badge")),
            x_username=_s(data.get("x_username")),
        )


@dataclass
class WalletPnL:
    address: str
    portfolio_value: float
    unrealized_pnl: float
    realized_pnl: float
    position_count: int

    @property
    def total_pnl(self) -> float:
        return self.unrealized_pnl + self.realized_pnl


@dataclass
class WalletSnapshot:
    address: str
    alias: str
    display_name: str
    positions: list[Position] = field(default_factory=list)
    trades: list[Trade] = field(default_factory=list)
    pnl: Optional[WalletPnL] = None

    @property
    def position_map(self) -> dict[str, Position]:
        return {p.key: p for p in self.positions}


@dataclass
class PlayerSignal:
    """A notable player event. Never implies a live order."""

    kind: str  # open, close, increase, trade, leaderboard
    wallet: str
    alias: str
    user_name: str
    title: str
    outcome: str
    side: str
    size: float
    price: float
    notional: float
    pnl: float = 0.0
    slug: str = ""
    event_slug: str = ""
    condition_id: str = ""
    asset: str = ""
    rank: str = ""
    vol: float = 0.0
    extra: dict[str, Any] = field(default_factory=dict)

    @property
    def profile_url(self) -> str:
        return f"https://polymarket.com/profile/{self.wallet}"

    @property
    def market_url(self) -> str:
        if self.event_slug:
            return f"https://polymarket.com/event/{self.event_slug}"
        if self.slug:
            return f"https://polymarket.com/market/{self.slug}"
        return ""

    @property
    def label(self) -> str:
        return self.alias or self.user_name or self.wallet[:10]

    def to_dict(self) -> dict[str, Any]:
        data = asdict(self)
        data["profile_url"] = self.profile_url
        data["market_url"] = self.market_url
        data["label"] = self.label
        return data
