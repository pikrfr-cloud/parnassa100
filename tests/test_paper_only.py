"""Guardrails: this repo must stay paper-only."""

from pathlib import Path

from i18n.translations import TRANSLATIONS, t
from paper.ledger import LIVE_TRADING

ROOT = Path(__file__).resolve().parents[1]
APP_DIRS = ("player_intel", "paper", "alerts", "sources", "i18n")
APP_FILES = ("main.py", "config.py", "state.py")

FORBIDDEN = (
    "SecureClient",
    "private_key",
    "PRIVATE_KEY",
    "create_and_post_order",
    "py_clob_client",
    "ClobClient",
    "post_order(",
    "create_order(",
)


def test_live_trading_flag_is_false():
    assert LIVE_TRADING is False


def test_source_has_no_order_placement():
    paths = [ROOT / name for name in APP_FILES]
    for folder in APP_DIRS:
        paths.extend((ROOT / folder).rglob("*.py"))
    offenders = []
    for path in paths:
        text = path.read_text(encoding="utf-8")
        for token in FORBIDDEN:
            if token in text:
                offenders.append(f"{path.relative_to(ROOT)}: {token}")
    assert offenders == []


def test_hebrew_player_strings_format():
    kwargs = dict(
        label="לויתן",
        title="BTC 100k",
        outcome="Yes",
        side="BUY",
        size=10.0,
        price=0.4,
        notional=4.0,
        paper_side="BUY",
        paper_size=5.0,
        paper_price=0.4,
        paper_notional=2.0,
        profile_url="https://polymarket.com/profile/0xabc",
        market_url="https://polymarket.com/event/x",
        rank="1",
        vol=1000.0,
        pnl=50.0,
        interval=120,
        threshold=15,
        watchlist_count=2,
    )
    player_keys = [
        key for key in TRANSLATIONS["he"]
        if key.startswith("player_") or key.startswith("leaderboard_") or key == "bot_started"
    ]
    for key in player_keys:
        rendered = t(key, lang="he", **kwargs)
        assert "{" not in rendered, key
        if key != "bot_started":
            assert "נייר" in rendered or "שחקן" in rendered or "לוח" in rendered
