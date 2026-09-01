"""Public Polymarket Data API client — read-only, no keys, no CLOB."""

from __future__ import annotations

import logging
from typing import Any, Awaitable, Callable, Optional

import aiohttp

from player_intel.models import LeaderboardEntry, Position, Trade, WalletPnL, WalletSnapshot

logger = logging.getLogger(__name__)

JsonGetter = Callable[[str, dict[str, Any]], Awaitable[Any]]

DEFAULT_BASE = "https://data-api.polymarket.com"


class DataAPIError(RuntimeError):
    """Raised when the Data API returns a non-success payload we cannot use."""


class DataAPIClient:
    """Thin wrapper around data-api.polymarket.com.

    Inject ``get_json`` in tests to avoid network calls. This client never
    signs requests, never holds private keys, and never talks to the CLOB.
    """

    def __init__(
        self,
        session: Optional[aiohttp.ClientSession] = None,
        base_url: str = DEFAULT_BASE,
        get_json: Optional[JsonGetter] = None,
        timeout: float = 20.0,
    ):
        self.session = session
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self._get_json = get_json

    async def get_json(self, path: str, params: Optional[dict[str, Any]] = None) -> Any:
        if self._get_json is not None:
            return await self._get_json(path, params or {})
        if self.session is None:
            raise DataAPIError("No HTTP session and no get_json mock provided")
        url = f"{self.base_url}{path}"
        try:
            async with self.session.get(
                url,
                params=params or {},
                timeout=aiohttp.ClientTimeout(total=self.timeout),
            ) as resp:
                if resp.status != 200:
                    body = await resp.text()
                    logger.warning("Data API %s -> %s: %s", path, resp.status, body[:200])
                    return []
                return await resp.json()
        except aiohttp.ClientError as exc:
            logger.warning("Data API %s error: %s", path, exc)
            return []

    async def get_leaderboard(
        self,
        time_period: str = "DAY",
        order_by: str = "PNL",
        limit: int = 25,
        category: str = "OVERALL",
        offset: int = 0,
    ) -> list[LeaderboardEntry]:
        payload = await self.get_json(
            "/v1/leaderboard",
            {
                "timePeriod": time_period,
                "orderBy": order_by,
                "limit": limit,
                "category": category,
                "offset": offset,
            },
        )
        rows = payload if isinstance(payload, list) else []
        return [LeaderboardEntry.from_api(row) for row in rows if isinstance(row, dict)]

    async def get_positions(
        self,
        user: str,
        limit: int = 100,
        sort_by: str = "CURRENT",
    ) -> list[Position]:
        payload = await self.get_json(
            "/positions",
            {
                "user": user,
                "limit": limit,
                "sortBy": sort_by,
                "sortDirection": "DESC",
            },
        )
        rows = payload if isinstance(payload, list) else []
        return [Position.from_api(row) for row in rows if isinstance(row, dict)]

    async def get_closed_positions(self, user: str, limit: int = 50) -> list[Position]:
        payload = await self.get_json(
            "/closed-positions",
            {
                "user": user,
                "limit": limit,
                "sortBy": "REALIZEDPNL",
            },
        )
        rows = payload if isinstance(payload, list) else []
        return [Position.from_api(row) for row in rows if isinstance(row, dict)]

    async def get_trades(self, user: str, limit: int = 50) -> list[Trade]:
        payload = await self.get_json(
            "/trades",
            {
                "user": user,
                "limit": limit,
                "takerOnly": "false",
            },
        )
        rows = payload if isinstance(payload, list) else []
        return [Trade.from_api(row) for row in rows if isinstance(row, dict)]

    async def get_value(self, user: str) -> float:
        payload = await self.get_json("/value", {"user": user})
        if isinstance(payload, list) and payload:
            first = payload[0]
            if isinstance(first, dict):
                try:
                    return float(first.get("value") or 0)
                except (TypeError, ValueError):
                    return 0.0
        if isinstance(payload, dict):
            try:
                return float(payload.get("value") or 0)
            except (TypeError, ValueError):
                return 0.0
        return 0.0

    async def get_wallet_snapshot(
        self,
        address: str,
        alias: str = "",
        position_limit: int = 100,
        trade_limit: int = 50,
    ) -> WalletSnapshot:
        address = address.lower()
        positions = await self.get_positions(address, limit=position_limit)
        trades = await self.get_trades(address, limit=trade_limit)
        value = await self.get_value(address)
        unrealized = sum(p.cash_pnl for p in positions)
        realized = sum(p.realized_pnl for p in positions)
        display = alias
        if not display:
            for trade in trades:
                if trade.name:
                    display = trade.name
                    break
        if not display:
            display = address[:10]
        return WalletSnapshot(
            address=address,
            alias=alias,
            display_name=display,
            positions=positions,
            trades=trades,
            pnl=WalletPnL(
                address=address,
                portfolio_value=value,
                unrealized_pnl=unrealized,
                realized_pnl=realized,
                position_count=len(positions),
            ),
        )
