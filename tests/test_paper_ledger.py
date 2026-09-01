"""Paper ledger records signals without any live order path."""

import json

from paper.ledger import LIVE_TRADING, PAPER_MODE, PaperLedger, paper_notional
from player_intel.models import PlayerSignal

WALLET = "0x56687bf447db6ffa42ffe2204a05edaa20f55839"


def _signal(kind="open", **overrides) -> PlayerSignal:
    data = dict(
        kind=kind,
        wallet=WALLET,
        alias="Whale",
        user_name="Whale",
        title="Will BTC hit 100k?",
        outcome="Yes",
        side="BUY",
        size=1000.0,
        price=0.40,
        notional=400.0,
        condition_id="0x" + "ab" * 32,
        asset="asset-yes",
        event_slug="btc-100k",
    )
    data.update(overrides)
    return PlayerSignal(**data)


def test_open_is_capped_and_paper_only(tmp_path):
    ledger = PaperLedger(str(tmp_path / "ledger.json"), max_usd=100.0, copy_ratio=1.0)
    fill = ledger.record_signal(_signal(notional=10_000, price=0.50))
    assert fill.live_order is False
    assert fill.mode == PAPER_MODE
    assert LIVE_TRADING is False
    assert fill.status == "open"
    assert fill.side == "BUY"
    assert fill.notional == 100.0
    assert fill.size == 200.0  # 100 / 0.50
    assert fill.player_notional == 10_000.0

    saved = json.loads((tmp_path / "ledger.json").read_text(encoding="utf-8"))
    assert saved["live_trading"] is False
    assert saved["mode"] == "paper"
    assert saved["fills"][0]["live_order"] is False


def test_close_matches_open_and_computes_pnl(tmp_path):
    ledger = PaperLedger(str(tmp_path / "ledger.json"), max_usd=100.0)
    ledger.record_signal(_signal(kind="open", price=0.40, notional=400))
    close = ledger.record_signal(_signal(kind="close", side="SELL", price=0.70, notional=400))
    assert close.status == "closed"
    assert close.close_price == 0.70
    assert close.realized_pnl == round((0.70 - 0.40) * close.size, 6)
    assert len(ledger.open_fills()) == 0
    summary = ledger.summary()
    assert summary["live_trading"] is False
    assert summary["closed"] == 1
    assert summary["signals"] == 2


def test_leaderboard_is_a_note_not_a_position(tmp_path):
    ledger = PaperLedger(str(tmp_path / "ledger.json"))
    fill = ledger.record_signal(
        _signal(kind="leaderboard", title="Leaderboard #1 Whale", price=0, notional=50_000, size=0)
    )
    assert fill.status == "noted"
    assert fill.side == ""
    assert fill.live_order is False


def test_reload_persists_fills(tmp_path):
    path = str(tmp_path / "ledger.json")
    PaperLedger(path, max_usd=50).record_signal(_signal(notional=80, price=0.2))
    reloaded = PaperLedger(path, max_usd=50)
    assert len(reloaded.fills) == 1
    assert reloaded.fills[0].notional == 50.0


def test_paper_notional_helper():
    assert paper_notional(0, 100, 1) == 0
    assert paper_notional(40, 100, 1) == 40
    assert paper_notional(400, 100, 0.1) == 40
