"""Tests for watchlist parsing (env + JSON file)."""

from player_intel.watchlist import (
    load_watchlist,
    load_watchlist_file,
    normalize_address,
    parse_watchlist_entry,
    parse_watchlist_env,
)


VALID_A = "0x56687bf447db6ffa42ffe2204a05edaa20f55839"
VALID_B = "0x0000000000000000000000000000000000000001"


def test_normalize_address_accepts_checksum_and_lower():
    assert normalize_address(VALID_A) == VALID_A.lower()
    assert normalize_address(VALID_A.upper()) == VALID_A.lower()
    assert normalize_address("not-an-address") is None
    assert normalize_address("0x123") is None


def test_parse_entry_with_alias():
    entry = parse_watchlist_entry(f"{VALID_A}:Whale One")
    assert entry is not None
    assert entry.address == VALID_A.lower()
    assert entry.alias == "Whale One"
    assert entry.label == "Whale One"


def test_parse_env_skips_invalid_and_dedupes():
    raw = f"{VALID_A}:Alpha, not-valid, {VALID_A}:IgnoredDup, {VALID_B}"
    wallets = parse_watchlist_env(raw)
    assert [w.address for w in wallets] == [VALID_A.lower(), VALID_B.lower()]
    assert wallets[0].alias == "Alpha"


def test_load_watchlist_file_objects_and_strings(tmp_path):
    path = tmp_path / "watchlist.json"
    path.write_text(
        f'[{{"address": "{VALID_A}", "alias": "FromFile"}}, "{VALID_B}"]',
        encoding="utf-8",
    )
    wallets = load_watchlist_file(str(path))
    assert len(wallets) == 2
    assert wallets[0].alias == "FromFile"
    assert wallets[1].address == VALID_B.lower()


def test_load_watchlist_merges_file_over_env(tmp_path):
    path = tmp_path / "watchlist.json"
    path.write_text(
        f'[{{"address": "{VALID_A}", "alias": "FileWins"}}]',
        encoding="utf-8",
    )
    wallets = load_watchlist(env_value=f"{VALID_A}:EnvName,{VALID_B}", file_path=str(path))
    by_addr = {w.address: w for w in wallets}
    assert by_addr[VALID_A.lower()].alias == "FileWins"
    assert VALID_B.lower() in by_addr
