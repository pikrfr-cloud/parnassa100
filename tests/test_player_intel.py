"""Player-intel scanner + mocked Data API client."""

from player_intel.client import DataAPIClient
from player_intel.models import LeaderboardEntry, Position, Trade, WalletSnapshot
from player_intel.scanner import (
    detect_leaderboard_anomalies,
    detect_new_trades,
    detect_position_changes,
    max_trade_timestamp,
)

WALLET = "0x56687bf447db6ffa42ffe2204a05edaa20f55839"


def _pos(**overrides) -> Position:
    data = dict(
        proxy_wallet=WALLET,
        asset="asset-yes",
        condition_id="0x" + "ab" * 32,
        size=1000.0,
        avg_price=0.40,
        initial_value=400.0,
        current_value=600.0,
        cash_pnl=200.0,
        realized_pnl=0.0,
        cur_price=0.60,
        title="Will BTC hit 100k?",
        slug="btc-100k",
        event_slug="btc-100k",
        outcome="Yes",
    )
    data.update(overrides)
    return Position(**data)


def _snap(positions=None, trades=None) -> WalletSnapshot:
    return WalletSnapshot(
        address=WALLET,
        alias="Whale",
        display_name="Whale",
        positions=positions or [],
        trades=trades or [],
    )


def test_open_close_and_increase_detection():
    prev = _pos(current_value=400.0, size=800.0)
    opened = _pos(asset="asset-new", current_value=900.0, title="New market")
    grown = _pos(current_value=1200.0, size=2000.0)

    snapshot = _snap([grown, opened])
    signals = detect_position_changes({prev.key: prev}, snapshot, notable_usd=500)

    kinds = sorted(s.kind for s in signals)
    assert "increase" in kinds
    assert "open" in kinds
    assert "close" not in kinds

    empty = _snap([])
    closes = detect_position_changes({prev.key: prev}, empty, notable_usd=100)
    assert len(closes) == 1
    assert closes[0].kind == "close"
    assert closes[0].side == "SELL"


def test_small_positions_are_ignored():
    tiny = _pos(current_value=10.0, initial_value=10.0, size=20.0, cur_price=0.5)
    signals = detect_position_changes({}, _snap([tiny]), notable_usd=500)
    assert signals == []


def test_new_trades_dedupe_against_open_signal():
    trade = Trade(
        proxy_wallet=WALLET,
        side="BUY",
        asset="asset-yes",
        condition_id="0x" + "ab" * 32,
        size=1000,
        price=0.5,
        timestamp=1_700_000_100,
        title="Will BTC hit 100k?",
        slug="btc-100k",
        event_slug="btc-100k",
        outcome="Yes",
        name="Whale",
    )
    pos = _pos()
    snapshot = _snap([pos], [trade])
    opens = detect_position_changes({}, snapshot, notable_usd=100)
    assert opens and opens[0].kind == "open"

    extras = detect_new_trades(snapshot, last_trade_ts=1_700_000_000, notable_usd=100, already=opens)
    assert extras == []

    only_trades = detect_new_trades(
        _snap([], [trade]), last_trade_ts=1_700_000_000, notable_usd=100, already=[]
    )
    assert len(only_trades) == 1
    assert only_trades[0].kind == "trade"
    assert max_trade_timestamp([trade]) == 1_700_000_100


def test_leaderboard_unusual_size_and_new_name():
    prev = [
        LeaderboardEntry(rank="1", proxy_wallet=WALLET, user_name="Whale", vol=10_000, pnl=2_000),
    ]
    current = [
        LeaderboardEntry(rank="1", proxy_wallet=WALLET, user_name="Whale", vol=80_000, pnl=50_000),
        LeaderboardEntry(
            rank="2",
            proxy_wallet="0x0000000000000000000000000000000000000001",
            user_name="Newbie",
            vol=40_000,
            pnl=30_000,
        ),
    ]
    signals = detect_leaderboard_anomalies(prev, current, unusual_usd=25_000)
    labels = {s.user_name for s in signals}
    assert "Whale" in labels
    assert "Newbie" in labels
    assert all(s.kind == "leaderboard" for s in signals)

    quiet = detect_leaderboard_anomalies(
        prev,
        [LeaderboardEntry(rank="1", proxy_wallet=WALLET, user_name="Whale", vol=10_500, pnl=2_100)],
        unusual_usd=25_000,
    )
    assert quiet == []


async def test_data_api_client_parses_mocked_payloads():
    async def fake_get(path, params):
        if path == "/v1/leaderboard":
            return [
                {
                    "rank": "1",
                    "proxyWallet": WALLET,
                    "userName": "Whale",
                    "vol": 1234.5,
                    "pnl": 99.1,
                }
            ]
        if path == "/positions":
            return [
                {
                    "proxyWallet": WALLET,
                    "asset": "tok",
                    "conditionId": "0x" + "cd" * 32,
                    "size": 10,
                    "avgPrice": 0.2,
                    "initialValue": 2,
                    "currentValue": 3,
                    "cashPnl": 1,
                    "realizedPnl": 0,
                    "curPrice": 0.3,
                    "title": "Example",
                    "slug": "example",
                    "eventSlug": "example",
                    "outcome": "Yes",
                }
            ]
        if path == "/trades":
            return [
                {
                    "proxyWallet": WALLET,
                    "side": "BUY",
                    "asset": "tok",
                    "conditionId": "0x" + "cd" * 32,
                    "size": 10,
                    "price": 0.2,
                    "timestamp": 50,
                    "title": "Example",
                    "slug": "example",
                    "eventSlug": "example",
                    "outcome": "Yes",
                    "name": "Whale",
                }
            ]
        if path == "/value":
            return [{"user": WALLET, "value": 3.0}]
        raise AssertionError(f"unexpected path {path}")

    client = DataAPIClient(get_json=fake_get)
    board = await client.get_leaderboard()
    assert board[0].user_name == "Whale"
    snap = await client.get_wallet_snapshot(WALLET, alias="Whale")
    assert snap.pnl.portfolio_value == 3.0
    assert snap.pnl.unrealized_pnl == 1.0
    assert len(snap.positions) == 1
    assert snap.trades[0].side == "BUY"
