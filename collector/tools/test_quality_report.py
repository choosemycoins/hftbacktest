"""Tests for `quality_report.py` — Phase 2 of `docs/design-multi-venue-collection.md`.

Every fixture here is synthetic: a handful of lines written into a real gzip
file in `tmp_path`. No network, no recorded data. The point is that each
behaviour the design document names is pinned by a file whose contents — and
whose defects — are known exactly.

The timestamps are deliberately realistic nanosecond values (~1.78e18). They do
not survive a round trip through float64 (2^53 ~ 9e15), so any place the report
lets one become a float shows up here as an off-by-a-few-hundred-nanoseconds
mismatch rather than as a rounding nobody notices.
"""

import gzip
import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))

import quality_report as qr  # noqa: E402

# --------------------------------------------------------------------------
# fixture helpers
# --------------------------------------------------------------------------

DAY = "20260725"
NEXT_DAY = "20260726"

#: 2026-07-25T00:00:00Z, in nanoseconds. Chosen for the float trap: adding a
#: sub-second offset produces a value float64 cannot represent exactly.
D25 = 1_784_937_600_000_000_000
SEC = 1_000_000_000
MS = 1_000_000


def ns(seconds: float = 0, nanos: int = 0) -> int:
    """A timestamp `seconds` into 2026-07-25, as an exact int."""
    return D25 + int(seconds) * SEC + nanos


def ms_of(ts_ns: int) -> int:
    """The venue-side millisecond stamp matching a local ns timestamp."""
    return ts_ns // MS


def write_gz(path: Path, records, append: bool = False) -> None:
    """Writes `(local_ts_ns, obj)` pairs in the collector's line format.

    `append=True` starts a *new gzip member* in the same file, which is what a
    collector restart does (`collector/src/file.rs`, `File::options().append`).
    """
    with gzip.open(path, "ab" if append else "wb") as f:
        for ts, obj in records:
            f.write(f"{ts} {json.dumps(obj)}\n".encode())


def write_meta(directory: Path, instance: str, day: str, records) -> Path:
    """Writes the sidecar. Same line format, uncompressed, one JSON per line."""
    path = directory / f"_meta_{instance}_{day}.jsonl"
    with open(path, "w") as f:
        for ts, obj in records:
            f.write(f"{ts} {json.dumps(obj)}\n")
    return path


def truncate(path: Path, nbytes: int = 8) -> None:
    """Chops the tail off a gzip file — by default exactly the member trailer.

    This is what an unfinished member looks like: everything decompresses, and
    then the CRC/ISIZE the decoder wants is not there.
    """
    data = path.read_bytes()
    path.write_bytes(data[: -nbytes])


def session_start(exchange, symbols, hl_l2_modes=("slow", "fast"), bybit_depths=(1, 50)):
    return {
        "_collector": "session_start",
        "version": "0.1.0",
        "commit": "deadbeef",
        "branch": "feat/snapshot-marker",
        "dirty": "clean",
        "exchange": exchange,
        "symbols": list(symbols),
        "bybit_depths": list(bybit_depths),
        "hl_l2_modes": list(hl_l2_modes),
    }


# --- venue frame shapes (see py-hftbacktest/.../hyperliquid.py, binancefutures.py)


def hl_trade(coin, ts):
    return {
        "channel": "trades",
        "data": [
            {"coin": coin, "side": "A", "px": "21.2", "sz": "7.7", "time": ms_of(ts), "tid": 1}
        ],
    }


def hl_bbo(coin, ts):
    return {
        "channel": "bbo",
        "data": {
            "coin": coin,
            "time": ms_of(ts),
            "bbo": [{"px": "21.2", "sz": "1", "n": 1}, {"px": "21.3", "sz": "1", "n": 1}],
        },
    }


def hl_l2(coin, ts, fast=False):
    data = {"coin": coin, "time": ms_of(ts), "levels": [[], []]}
    if fast:
        data["fast"] = True
    return {"channel": "l2Book", "data": data}


def um_book_ticker(symbol, ts, u=1):
    return {
        "stream": f"{symbol.lower()}@bookTicker",
        "data": {
            "e": "bookTicker",
            "u": u,
            "s": symbol,
            "b": "24670.90",
            "B": "1",
            "a": "24671.00",
            "A": "2",
            "T": ms_of(ts),
            "E": ms_of(ts),
        },
    }


def um_depth(symbol, ts, u, pu):
    return {
        "stream": f"{symbol.lower()}@depth@0ms",
        "data": {
            "e": "depthUpdate",
            "E": ms_of(ts),
            "T": ms_of(ts),
            "s": symbol,
            "U": u - 5,
            "u": u,
            "pu": pu,
            "b": [],
            "a": [],
        },
    }


def um_trade(symbol, ts):
    return {
        "stream": f"{symbol.lower()}@trade",
        "data": {
            "e": "trade",
            "E": ms_of(ts),
            "T": ms_of(ts),
            "s": symbol,
            "t": 1,
            "p": "24670.90",
            "q": "0.022",
            "X": "MARKET",
            "m": True,
        },
    }


def bybit_book(symbol, depth, ts, u, kind="delta"):
    return {
        "topic": f"orderbook.{depth}.{symbol}",
        "type": kind,
        "ts": ms_of(ts),
        "data": {"s": symbol, "b": [], "a": [], "u": u, "seq": u * 10},
        "cts": ms_of(ts),
    }


# --- whole-directory builders


def hl_dir(tmp_path, modes=("slow", "fast"), day=DAY, name="hyperliquid", extra_meta=()):
    """A minimal, complete Hyperliquid recording: every declared stream present."""
    d = tmp_path / name
    d.mkdir()
    recs = [(ns(0), hl_trade("BTC", ns(0))), (ns(1), hl_bbo("BTC", ns(1)))]
    if "slow" in modes:
        recs.append((ns(2), hl_l2("BTC", ns(2), fast=False)))
    if "fast" in modes:
        recs.append((ns(3), hl_l2("BTC", ns(3), fast=True)))
    write_gz(d / f"btc_{day}.gz", recs)
    write_meta(
        d,
        "hyperliquid",
        day,
        [(ns(0), session_start("hyperliquid", ["BTC"], hl_l2_modes=modes)), *extra_meta],
    )
    return d


def um_dir(tmp_path, streams=("bookTicker", "trade", "depthUpdate"), day=DAY, name="binance"):
    d = tmp_path / name
    d.mkdir()
    recs = []
    if "bookTicker" in streams:
        recs.append((ns(0), um_book_ticker("BTCUSDT", ns(0))))
    if "trade" in streams:
        recs.append((ns(1), um_trade("BTCUSDT", ns(1))))
    if "depthUpdate" in streams:
        recs.append((ns(2), um_depth("BTCUSDT", ns(2), u=100, pu=99)))
    write_gz(d / f"btcusdt_{day}.gz", recs)
    write_meta(d, "binancefuturesum", day, [(ns(0), session_start("binancefuturesum", ["BTCUSDT"]))])
    return d


def run(*dirs, day=DAY, include_today=False, out=None, extra=()):
    """Runs the report over `dirs`, returning `(exit_code, report_dict)`."""
    argv = []
    for d in dirs:
        argv += ["--dir", str(d)]
    if day is not None:
        argv += ["--day", day]
    if include_today:
        argv.append("--include-today")
    if out is not None:
        argv += ["--json", str(out)]
    argv += list(extra)
    code = qr.main(argv)
    report = json.loads(Path(out).read_text()) if out is not None else None
    return code, report


def issues_of(report, venue, day=DAY):
    return report["venues"][venue]["days"][day]["issues"]


def checks_of(report, venue, day=DAY, severity=None):
    return [
        i["check"]
        for i in issues_of(report, venue, day)
        if severity is None or i["severity"] == severity
    ]


# --------------------------------------------------------------------------
# 1. finalized-only
# --------------------------------------------------------------------------


def test_todays_file_is_skipped_unless_include_today(tmp_path, monkeypatch):
    """The live day's gzip member is open by construction, so it is not checked.

    The default (no `--day`) must report yesterday and say nothing about today.
    """
    d = hl_dir(tmp_path)
    # A second day's recording, which "today" is pinned to below.
    write_gz(d / f"btc_{NEXT_DAY}.gz", [(ns(86_400), hl_trade("BTC", ns(86_400)))])
    write_meta(d, "hyperliquid", NEXT_DAY, [(ns(86_400), session_start("hyperliquid", ["BTC"]))])
    monkeypatch.setattr(qr, "utc_today", lambda: NEXT_DAY)

    out = tmp_path / "r.json"
    _, report = run(d, day=None, out=out)
    days = report["venues"]["hyperliquid"]["days"]
    assert set(days) == {DAY}, "today's unfinalized file must not be checked"

    _, report = run(d, day=None, out=out, include_today=True)
    assert set(report["venues"]["hyperliquid"]["days"]) == {DAY, NEXT_DAY}


def test_asking_for_today_without_the_flag_is_a_usage_error(tmp_path, monkeypatch):
    d = hl_dir(tmp_path, day=NEXT_DAY)
    monkeypatch.setattr(qr, "utc_today", lambda: NEXT_DAY)
    code, _ = run(d, day=NEXT_DAY)
    assert code == 2


# --------------------------------------------------------------------------
# 2. gzip integrity
# --------------------------------------------------------------------------


def test_truncated_gzip_on_a_finalized_day_is_red(tmp_path):
    d = hl_dir(tmp_path)
    truncate(d / f"btc_{DAY}.gz")

    out = tmp_path / "r.json"
    code, report = run(d, out=out)
    assert code == 1
    assert report["verdict"] == "red"
    assert "gzip_integrity" in checks_of(report, "hyperliquid", severity="red")


def test_truncation_of_todays_open_member_is_not_red(tmp_path, monkeypatch):
    """With `--include-today` the last member is *expected* to be unfinished.

    `file.rs` only writes the member trailer on rotation or shutdown, so a
    decoder rejects the live file by construction — reporting that as
    corruption would make every e2e run red for no reason.
    """
    d = hl_dir(tmp_path, day=NEXT_DAY)
    monkeypatch.setattr(qr, "utc_today", lambda: NEXT_DAY)
    truncate(d / f"btc_{NEXT_DAY}.gz")

    out = tmp_path / "r.json"
    code, report = run(d, day=NEXT_DAY, include_today=True, out=out)
    assert code == 0
    assert report["venues"]["hyperliquid"]["days"][NEXT_DAY]["verdict"] == "yellow"
    assert "gzip_integrity" in checks_of(report, "hyperliquid", NEXT_DAY, severity="yellow")


def test_records_before_the_truncation_are_still_read(tmp_path):
    """A truncated file is still evidence. Decode as far as it goes."""
    d = hl_dir(tmp_path)
    path = d / f"btc_{DAY}.gz"
    truncate(path)
    scan = qr.scan_symbol_file(path, "hyperliquid")
    assert scan.truncated is True
    assert scan.streams["trades"].count == 1
    assert scan.streams["bbo"].count == 1


def test_multi_member_gzip_is_read_whole(tmp_path):
    """A restart appends a member; both must be read (README, Multi-member gzip)."""
    d = tmp_path / "hl"
    d.mkdir()
    path = d / f"btc_{DAY}.gz"
    write_gz(path, [(ns(0), hl_bbo("BTC", ns(0)))])
    write_gz(path, [(ns(10), hl_bbo("BTC", ns(10)))], append=True)
    scan = qr.scan_symbol_file(path, "hyperliquid")
    assert scan.truncated is False
    assert scan.streams["bbo"].count == 2
    assert scan.streams["bbo"].first_ts == ns(0)
    assert scan.streams["bbo"].last_ts == ns(10)


# --------------------------------------------------------------------------
# 3. expected symbol x stream set
# --------------------------------------------------------------------------


def test_missing_required_stream_is_red(tmp_path):
    """`hl_l2_modes=slow,fast` was recorded, but no fast frame ever arrived."""
    d = hl_dir(tmp_path, modes=("slow", "fast"))
    write_gz(
        d / f"btc_{DAY}.gz",
        [(ns(0), hl_trade("BTC", ns(0))), (ns(1), hl_bbo("BTC", ns(1))), (ns(2), hl_l2("BTC", ns(2)))],
    )

    out = tmp_path / "r.json"
    code, report = run(d, out=out)
    assert code == 1
    reds = [i for i in issues_of(report, "hyperliquid") if i["severity"] == "red"]
    assert any(i["check"] == "missing_required" and "l2Book_fast" in i["detail"] for i in reds), reds


def test_missing_bookticker_is_red_but_missing_trade_and_depth_is_only_yellow(tmp_path):
    """Mode A's contract: `@bookTicker` is the only Binance stream it depends on."""
    out = tmp_path / "r.json"

    d = um_dir(tmp_path, streams=("bookTicker",), name="um-booktickeronly")
    code, report = run(d, out=out)
    assert code == 0, "a recording with the required stream is not a failure"
    assert report["venues"]["binancefuturesum"]["days"][DAY]["verdict"] == "yellow"
    reported = issues_of(report, "binancefuturesum")
    assert [i["severity"] for i in reported] == ["yellow"], reported
    assert reported[0]["check"] == "missing_optional"
    assert "trade" in reported[0]["detail"] and "depthUpdate" in reported[0]["detail"]
    sym = report["venues"]["binancefuturesum"]["days"][DAY]["symbols"]["btcusdt"]
    assert sym["missing_optional"] == ["trade", "depthUpdate"]
    assert sym["missing_required"] == []

    d2 = um_dir(tmp_path, streams=("trade", "depthUpdate"), name="um-nobookticker")
    code, report = run(d2, out=out)
    assert code == 1
    assert "missing_required" in checks_of(report, "binancefuturesum", severity="red")


def test_a_fast_only_run_is_not_red(tmp_path):
    """`--hl-l2-modes fast` is a legal recording, not a defect (доc, Фаза 2)."""
    d = hl_dir(tmp_path, modes=("fast",))
    out = tmp_path / "r.json"
    code, report = run(d, out=out)
    assert code == 0
    assert report["verdict"] == "green", issues_of(report, "hyperliquid")


def test_a_symbol_with_no_file_at_all_is_red(tmp_path):
    d = hl_dir(tmp_path)
    write_meta(
        d,
        "hyperliquid",
        DAY,
        [(ns(0), session_start("hyperliquid", ["BTC", "ETH"], hl_l2_modes=("slow", "fast")))],
    )
    out = tmp_path / "r.json"
    code, report = run(d, out=out)
    assert code == 1
    reds = [i for i in issues_of(report, "hyperliquid") if i["severity"] == "red"]
    assert any("eth" in i["detail"].lower() for i in reds), reds
    # A symbol with no file must still describe itself the same way as one with
    # a file, or a consumer walking the tree hits a missing key on the bad case.
    absent = report["venues"]["hyperliquid"]["days"][DAY]["symbols"]["eth"]
    present = report["venues"]["hyperliquid"]["days"][DAY]["symbols"]["btc"]
    assert set(absent) == set(present)
    assert absent["file"] is None and absent["streams"] == {}


def test_a_day_no_session_start_applies_to_cannot_be_verified_and_is_red(tmp_path):
    """No `session_start` in force means no expected set — fail closed.

    "In force" is directory-wide and time-ordered: a `session_start` written
    *after* the day describes a later process, not this one, so the day it does
    not cover stays unverifiable. (An *older* one does cover it — that is the
    normal shape of a collector running past midnight, and it is checked in
    `test_day_two_of_one_process_is_not_meta_missing`.)
    """
    d = hl_dir(tmp_path)
    (d / f"_meta_hyperliquid_{DAY}.jsonl").unlink()
    later = ns(86_400)
    write_meta(d, "hyperliquid", NEXT_DAY, [(later, session_start("hyperliquid", ["BTC"]))])
    out = tmp_path / "r.json"
    code, report = run(d, day=DAY, out=out)
    assert code == 1
    assert "meta_missing" in checks_of(report, "hyperliquid", severity="red")


def test_a_directory_with_no_sidecar_at_all_is_a_usage_error(tmp_path):
    d = tmp_path / "mystery"
    d.mkdir()
    write_gz(d / f"btc_{DAY}.gz", [(ns(0), hl_bbo("BTC", ns(0)))])
    code, _ = run(d)
    assert code == 2


# --------------------------------------------------------------------------
# 4. sequence gaps
# --------------------------------------------------------------------------


def test_um_pu_chain_break_is_counted(tmp_path):
    d = um_dir(tmp_path, streams=("bookTicker",), name="um-pu")
    write_gz(
        d / f"btcusdt_{DAY}.gz",
        [
            (ns(0), um_book_ticker("BTCUSDT", ns(0))),
            (ns(1), um_depth("BTCUSDT", ns(1), u=100, pu=99)),
            (ns(2), um_depth("BTCUSDT", ns(2), u=101, pu=100)),  # continuous
            (ns(3), um_depth("BTCUSDT", ns(3), u=200, pu=150)),  # break: pu != 101
            (ns(4), um_depth("BTCUSDT", ns(4), u=201, pu=200)),
        ],
    )
    out = tmp_path / "r.json"
    code, report = run(d, out=out)
    assert code == 0, "a sequence gap is a warning, not a failed dataset"
    assert "sequence_gap" in checks_of(report, "binancefuturesum", severity="yellow")
    detail = next(i for i in issues_of(report, "binancefuturesum") if i["check"] == "sequence_gap")
    assert "1" in detail["detail"], detail
    sym = report["venues"]["binancefuturesum"]["days"][DAY]["symbols"]["btcusdt"]
    assert sym["sequence_breaks"]["depthUpdate"] == 1


def test_a_sequence_break_across_a_reconnect_is_annotated_as_explained(tmp_path):
    """The first `depthUpdate` after a reconnect breaks the `pu` chain by
    construction. Reporting that as an unexplained loss would send an
    investigation after the one gap the sidecar already accounts for."""
    d = um_dir(tmp_path, streams=("bookTicker",), name="um-reconnect")
    write_gz(
        d / f"btcusdt_{DAY}.gz",
        [
            (ns(0), um_book_ticker("BTCUSDT", ns(0))),
            (ns(1), um_depth("BTCUSDT", ns(1), u=100, pu=99)),
            (ns(40), um_depth("BTCUSDT", ns(40), u=900, pu=880)),  # after the reconnect
        ],
    )
    write_meta(
        d,
        "binancefuturesum",
        DAY,
        [
            (ns(0), session_start("binancefuturesum", ["BTCUSDT"])),
            (ns(10), {"_collector": "disconnected", "error": "reset", "connected_for_ms": 10000}),
            (ns(12), {"_collector": "connected", "url": "wss://fstream.binance.com"}),
        ],
    )
    out = tmp_path / "r.json"
    _, report = run(d, out=out)
    detail = next(i for i in issues_of(report, "binancefuturesum") if i["check"] == "sequence_gap")
    assert "disconnected" in detail["detail"], detail
    sym = report["venues"]["binancefuturesum"]["days"][DAY]["symbols"]["btcusdt"]
    example = sym["sequence_break_examples"]["depthUpdate"][0]
    assert example["start_local_ts"] == ns(1)
    assert example["end_local_ts"] == ns(40)
    assert "disconnected" in example["explained_by"]


def test_bybit_update_id_break_is_counted_and_a_snapshot_resets_the_chain(tmp_path):
    d = tmp_path / "bybit"
    d.mkdir()
    write_gz(
        d / f"btcusdt_{DAY}.gz",
        [
            (ns(0), bybit_book("BTCUSDT", 50, ns(0), u=10, kind="snapshot")),
            (ns(1), bybit_book("BTCUSDT", 50, ns(1), u=11)),
            (ns(2), bybit_book("BTCUSDT", 50, ns(2), u=15)),  # break: 11 -> 15
            (ns(3), bybit_book("BTCUSDT", 50, ns(3), u=99, kind="snapshot")),  # reset
            (ns(4), bybit_book("BTCUSDT", 50, ns(4), u=100)),
        ],
    )
    write_meta(d, "bybit", DAY, [(ns(0), session_start("bybit", ["BTCUSDT"], bybit_depths=(50,)))])
    out = tmp_path / "r.json"
    code, report = run(d, out=out)
    assert code == 0
    sym = report["venues"]["bybit"]["days"][DAY]["symbols"]["btcusdt"]
    assert sym["sequence_breaks"]["orderbook.50"] == 1, sym["sequence_breaks"]


# --------------------------------------------------------------------------
# 5./8. cadence gaps and their explanation
# --------------------------------------------------------------------------


def test_a_cadence_gap_is_flagged_and_named(tmp_path, capsys):
    """`bbo` has a 0.14s median; a 60s hole is a gap, and it must be named."""
    d = hl_dir(tmp_path)
    write_gz(
        d / f"btc_{DAY}.gz",
        [
            (ns(0), hl_trade("BTC", ns(0))),
            (ns(0), hl_l2("BTC", ns(0))),
            (ns(0), hl_l2("BTC", ns(0), fast=True)),
            (ns(1), hl_bbo("BTC", ns(1))),
            (ns(61), hl_bbo("BTC", ns(61))),  # 60s hole
        ],
    )
    out = tmp_path / "r.json"
    code, report = run(d, out=out)
    assert code == 0
    assert "cadence_gap" in checks_of(report, "hyperliquid", severity="yellow")

    gaps = report["venues"]["hyperliquid"]["days"][DAY]["symbols"]["btc"]["streams"]["bbo"]["gaps"]
    assert len(gaps) == 1
    assert gaps[0]["start_local_ts"] == ns(1)
    assert gaps[0]["end_local_ts"] == ns(61)
    assert gaps[0]["duration_ns"] == 60 * SEC
    assert gaps[0]["explained_by"] is None

    # The doc's acceptance line: gaps are listed by name, not summarised away.
    text = capsys.readouterr().out
    assert "bbo" in text and "btc" in text and "cadence_gap" in text


def test_a_gap_spanned_by_a_disconnect_is_annotated_as_explained(tmp_path):
    d = hl_dir(tmp_path)
    write_gz(
        d / f"btc_{DAY}.gz",
        [
            (ns(0), hl_trade("BTC", ns(0))),
            (ns(0), hl_l2("BTC", ns(0))),
            (ns(0), hl_l2("BTC", ns(0), fast=True)),
            (ns(1), hl_bbo("BTC", ns(1))),
            (ns(61), hl_bbo("BTC", ns(61))),
        ],
    )
    write_meta(
        d,
        "hyperliquid",
        DAY,
        [
            (ns(0), session_start("hyperliquid", ["BTC"])),
            (ns(20), {"_collector": "disconnected", "error": "reset", "connected_for_ms": 20000}),
            (ns(25), {"_collector": "connected", "url": "wss://api.hyperliquid.xyz/ws"}),
        ],
    )
    out = tmp_path / "r.json"
    _, report = run(d, out=out)
    gaps = report["venues"]["hyperliquid"]["days"][DAY]["symbols"]["btc"]["streams"]["bbo"]["gaps"]
    assert gaps[0]["explained_by"] is not None
    assert "disconnected" in gaps[0]["explained_by"]
    issue = next(i for i in issues_of(report, "hyperliquid") if i["check"] == "cadence_gap")
    assert "disconnected" in issue["detail"]


def test_a_gap_within_the_expected_cadence_is_not_flagged(tmp_path):
    """`l2Book` slow runs at ~5.4s; a 6s interval is the feed working normally."""
    d = hl_dir(tmp_path)
    write_gz(
        d / f"btc_{DAY}.gz",
        [
            (ns(0), hl_trade("BTC", ns(0))),
            (ns(0), hl_bbo("BTC", ns(0))),
            (ns(0), hl_l2("BTC", ns(0), fast=True)),
            (ns(0), hl_l2("BTC", ns(0))),
            (ns(6), hl_l2("BTC", ns(6))),
        ],
    )
    out = tmp_path / "r.json"
    code, report = run(d, out=out)
    assert code == 0
    assert "cadence_gap" not in checks_of(report, "hyperliquid")


# --------------------------------------------------------------------------
# 6. monotonicity
# --------------------------------------------------------------------------


def test_non_monotonic_local_ts_within_a_file_is_red(tmp_path):
    d = hl_dir(tmp_path)
    write_gz(
        d / f"btc_{DAY}.gz",
        [
            (ns(0), hl_trade("BTC", ns(0))),
            (ns(0), hl_l2("BTC", ns(0))),
            (ns(0), hl_l2("BTC", ns(0), fast=True)),
            (ns(10), hl_bbo("BTC", ns(10))),
            (ns(5), hl_bbo("BTC", ns(5))),  # time goes backwards
        ],
    )
    out = tmp_path / "r.json"
    code, report = run(d, out=out)
    assert code == 1
    assert "monotonicity" in checks_of(report, "hyperliquid", severity="red")
    violation = report["venues"]["hyperliquid"]["days"][DAY]["symbols"]["btc"]["monotonic_violation"]
    assert violation["previous_local_ts"] == ns(10)
    assert violation["local_ts"] == ns(5)
    assert violation["violations"] == 1


def test_equal_local_ts_is_not_a_violation(tmp_path):
    """Two frames can share a nanosecond; only going backwards is a defect."""
    d = hl_dir(tmp_path)
    write_gz(
        d / f"btc_{DAY}.gz",
        [
            (ns(0), hl_trade("BTC", ns(0))),
            (ns(0), hl_l2("BTC", ns(0))),
            (ns(0), hl_l2("BTC", ns(0), fast=True)),
            (ns(0), hl_bbo("BTC", ns(0))),
            (ns(0), hl_bbo("BTC", ns(0))),
        ],
    )
    out = tmp_path / "r.json"
    code, report = run(d, out=out)
    assert code == 0, issues_of(report, "hyperliquid")


# --------------------------------------------------------------------------
# 7. coverage
# --------------------------------------------------------------------------


def test_coverage_spans_required_streams_only_and_keeps_exact_nanoseconds(tmp_path):
    """Optional streams must not widen the window Phase 3 intersects on.

    The timestamps here are not representable in float64, so an exact match is
    also a check that no stage of the report turned one into a double.
    """
    d = tmp_path / "um"
    d.mkdir()
    first_bt = ns(100, nanos=123_456_789)
    last_bt = ns(200, nanos=987_654_321)
    write_gz(
        d / f"btcusdt_{DAY}.gz",
        [
            (ns(0), um_trade("BTCUSDT", ns(0))),  # optional, earlier
            (first_bt, um_book_ticker("BTCUSDT", first_bt)),
            (last_bt, um_book_ticker("BTCUSDT", last_bt)),
            (ns(300), um_trade("BTCUSDT", ns(300))),  # optional, later
        ],
    )
    write_meta(d, "binancefuturesum", DAY, [(ns(0), session_start("binancefuturesum", ["BTCUSDT"]))])

    out = tmp_path / "r.json"
    _, report = run(d, out=out)
    coverage = report["venues"]["binancefuturesum"]["coverage"]
    assert coverage["first_local_ts"] == first_bt
    assert coverage["last_local_ts"] == last_bt
    assert float(coverage["first_local_ts"]) != first_bt, "the fixture must exercise the trap"


def test_coverage_is_null_when_no_required_frames_were_recorded(tmp_path):
    d = um_dir(tmp_path, streams=("trade",), name="um-empty")
    out = tmp_path / "r.json"
    _, report = run(d, out=out)
    coverage = report["venues"]["binancefuturesum"]["coverage"]
    assert coverage["first_local_ts"] is None
    assert coverage["last_local_ts"] is None
    symbol = report["venues"]["binancefuturesum"]["days"][DAY]["symbols"]["btcusdt"]
    assert symbol["coverage"]["first_local_ts"] is None
    assert symbol["coverage"]["last_local_ts"] is None


# --------------------------------------------------------------------------
# JSON contract and exit codes
# --------------------------------------------------------------------------


def test_json_contract_is_exactly_the_agreed_shape(tmp_path):
    """`build_dataset.py` reads these paths. Extra or renamed keys break it."""
    d = hl_dir(tmp_path)
    out = tmp_path / "r.json"
    _, report = run(d, out=out)

    assert set(report) == {"schema", "profile", "verdict", "venues"}
    assert report["schema"] == "quality-report-v1"
    assert report["profile"] == "mode-a-v1"
    assert report["verdict"] in ("green", "yellow", "red")

    venue = report["venues"]["hyperliquid"]
    assert set(venue) == {"data_dir", "exchange_as_recorded", "verdict", "coverage", "days"}
    assert venue["data_dir"] == str(d.resolve())
    assert Path(venue["data_dir"]).is_absolute(), (
        "the Phase 3 builder resolves a relative data_dir against the report "
        "file's directory, which is not where this ran"
    )
    assert set(venue["coverage"]) == {"first_local_ts", "last_local_ts", "note"}

    day = venue["days"][DAY]
    assert set(day) == {"verdict", "issues", "symbols"}
    for issue in day["issues"]:
        assert set(issue) == {"severity", "check", "detail"}
        assert issue["severity"] in ("red", "yellow")

    # Phase 3 trims to the per-symbol coverage, not the venue-wide union.
    symbol = day["symbols"]["btc"]
    assert set(symbol["coverage"]) == {
        "first_local_ts", "last_local_ts", "required_streams"}


def test_two_venues_report_side_by_side(tmp_path):
    hl = hl_dir(tmp_path)
    um = um_dir(tmp_path)
    out = tmp_path / "r.json"
    code, report = run(hl, um, out=out)
    assert code == 0
    assert set(report["venues"]) == {"hyperliquid", "binancefuturesum"}


def test_two_dirs_of_the_same_venue_are_refused(tmp_path):
    a = hl_dir(tmp_path, name="hl-a")
    b = hl_dir(tmp_path, name="hl-b")
    code, _ = run(a, b)
    assert code == 2


def test_an_unknown_profile_is_a_usage_error(tmp_path):
    d = hl_dir(tmp_path)
    code, _ = run(d, extra=["--profile", "mode-z-v9"])
    assert code == 2


def test_a_missing_directory_is_an_io_error(tmp_path):
    code, _ = run(tmp_path / "nope")
    assert code == 2


def test_verdict_rolls_up_worst_first(tmp_path):
    assert qr.worst(["green", "yellow"]) == "yellow"
    assert qr.worst(["yellow", "red", "green"]) == "red"
    assert qr.worst([]) == "green"


def test_a_line_is_split_on_the_first_space_only(tmp_path):
    ts, obj = qr.parse_line(b'1784937600123456789 {"channel":"bbo","data":{"coin":"BTC"}}')
    assert ts == 1_784_937_600_123_456_789
    assert isinstance(ts, int)
    assert obj["data"]["coin"] == "BTC"


def test_two_instances_recorded_into_one_directory_are_refused(tmp_path):
    """`lock.rs` stops this happening live; a directory assembled afterwards can
    still hold two, and merging their configurations would invent an expected
    set neither instance ever recorded."""
    d = hl_dir(tmp_path)
    write_meta(d, "binancefuturesum", DAY, [(ns(0), session_start("binancefuturesum", ["BTCUSDT"]))])
    code, _ = run(d)
    assert code == 2


def test_bybit_is_all_optional_under_mode_a_but_still_reports_coverage(tmp_path):
    """Bybit is not in mode A's dataset, so it cannot make that dataset red.

    Its declared topics are still checked — a silently rejected subscribe batch
    is the failure this report exists for — and its window is still reported.
    """
    d = tmp_path / "bybit-partial"
    d.mkdir()
    write_gz(
        d / f"btcusdt_{DAY}.gz",
        [(ns(0), bybit_book("BTCUSDT", 50, ns(0), u=1, kind="snapshot"))],
    )
    write_meta(
        d, "bybit", DAY, [(ns(0), session_start("bybit", ["BTCUSDT"], bybit_depths=(1, 50)))]
    )
    out = tmp_path / "r.json"
    code, report = run(d, out=out)
    assert code == 0, "bybit cannot fail a mode-A dataset it is not part of"
    assert "missing_optional" in checks_of(report, "bybit", severity="yellow")
    assert "missing_required" not in checks_of(report, "bybit")
    assert report["venues"]["bybit"]["coverage"]["first_local_ts"] == ns(0)


def test_a_symbol_name_containing_an_underscore_survives_the_split(tmp_path):
    """Binance delivery contracts are `BTCUSDT_251226`; only the day is split off."""
    d = tmp_path / "um-delivery"
    d.mkdir()
    write_gz(
        d / f"btcusdt_251226_{DAY}.gz",
        [(ns(0), um_book_ticker("BTCUSDT_251226", ns(0)))],
    )
    write_meta(
        d,
        "binancefuturesum",
        DAY,
        [(ns(0), session_start("binancefuturesum", ["BTCUSDT_251226"]))],
    )
    out = tmp_path / "r.json"
    code, report = run(d, out=out)
    assert code == 0, issues_of(report, "binancefuturesum")
    symbols = report["venues"]["binancefuturesum"]["days"][DAY]["symbols"]
    assert set(symbols) == {"btcusdt_251226"}


def test_unparseable_lines_are_reported_not_ignored(tmp_path):
    d = hl_dir(tmp_path)
    path = d / f"btc_{DAY}.gz"
    with gzip.open(path, "ab") as f:
        f.write(b"1784937600000000009 {this is not json\n")
    out = tmp_path / "r.json"
    code, report = run(d, out=out)
    assert code == 0
    assert "malformed_line" in checks_of(report, "hyperliquid", severity="yellow")


# --------------------------------------------------------------------------
# Review fixes: session_start lives in ONE sidecar per process, not per day
# --------------------------------------------------------------------------


def test_day_two_of_one_process_is_not_meta_missing(tmp_path, monkeypatch):
    """`session_start` is written once per process; the sidecar rotates daily.

    `collector/src/main.rs` writes it inside the startup block, and
    `RotatingFile::write` (`collector/src/file.rs`) opens a fresh
    `_meta_<instance>_<day>.jsonl` at every UTC midnight. A collector running
    since Monday therefore leaves Monday's sidecar holding the only
    `session_start` there is — day 2 onwards contains nothing but the per-minute
    disk gauge. Looking for the configuration in the day's own sidecar makes
    every day after the first red, and Phase 3 then refuses to build anything
    from a recording longer than one UTC day.
    """
    monkeypatch.setattr(qr, "utc_today", lambda: "20260801")
    d = hl_dir(tmp_path)  # day 1: data + the only session_start
    day2_ts = ns(0) + 86_400 * SEC
    write_gz(
        d / f"btc_{NEXT_DAY}.gz",
        [
            (day2_ts, hl_trade("BTC", day2_ts)),
            (day2_ts + 1, hl_bbo("BTC", day2_ts + 1)),
            (day2_ts + 2, hl_l2("BTC", day2_ts + 2, fast=False)),
            (day2_ts + 3, hl_l2("BTC", day2_ts + 3, fast=True)),
        ],
    )
    # What day 2's sidecar really holds: routine records, no session_start.
    write_meta(
        d,
        "hyperliquid",
        NEXT_DAY,
        [(day2_ts, {"_collector": "disk", "available_bytes": 1 << 40})],
    )

    out = tmp_path / "r.json"
    code, report = run(d, day=None, out=out)
    assert code == 0, issues_of(report, "hyperliquid", day=NEXT_DAY)
    assert "meta_missing" not in checks_of(report, "hyperliquid", day=NEXT_DAY)
    assert report["venues"]["hyperliquid"]["days"][NEXT_DAY]["verdict"] == "green"
    # ... and the second day must extend the coverage Phase 3 intersects on.
    assert report["venues"]["hyperliquid"]["coverage"]["last_local_ts"] == day2_ts + 3


def test_a_configuration_change_mid_recording_applies_from_its_own_day(tmp_path, monkeypatch):
    """A restart with a different configuration must not reach back in time.

    Day 1 ran `slow` only. Day 2's restart added `fast`. Day 1 never asked for
    fast frames and must not go red for their absence.
    """
    monkeypatch.setattr(qr, "utc_today", lambda: "20260801")
    d = hl_dir(tmp_path, modes=("slow",))
    day2_ts = ns(0) + 86_400 * SEC
    write_gz(
        d / f"btc_{NEXT_DAY}.gz",
        [
            (day2_ts, hl_trade("BTC", day2_ts)),
            (day2_ts + 1, hl_bbo("BTC", day2_ts + 1)),
            (day2_ts + 2, hl_l2("BTC", day2_ts + 2, fast=False)),
            (day2_ts + 3, hl_l2("BTC", day2_ts + 3, fast=True)),
        ],
    )
    write_meta(
        d,
        "hyperliquid",
        NEXT_DAY,
        [(day2_ts, session_start("hyperliquid", ["BTC"], hl_l2_modes=("slow", "fast")))],
    )
    out = tmp_path / "r.json"
    code, report = run(d, day=None, out=out)
    assert code == 0, issues_of(report, "hyperliquid")
    assert "missing_required" not in checks_of(report, "hyperliquid", day=DAY)


def test_a_directory_with_no_session_start_anywhere_is_still_red(tmp_path):
    """Widening the search must not make the missing-configuration case pass."""
    d = tmp_path / "orphan"
    d.mkdir()
    write_gz(d / f"btc_{DAY}.gz", [(ns(0), hl_trade("BTC", ns(0)))])
    write_meta(d, "hyperliquid", DAY, [(ns(0), {"_collector": "disk"})])
    code, _ = run(d)
    assert code == 2, "no session_start anywhere: the venue cannot even be identified"


# --------------------------------------------------------------------------
# Review fixes: the profile must be able to contradict the recording
# --------------------------------------------------------------------------


def test_a_recording_with_no_hl_book_is_red_under_mode_a(tmp_path):
    """`--hl-l2-modes none` is a legal recording and a useless mode-A dataset.

    Mode A's traded asset is the Hyperliquid book. A recording of trades and
    `bbo` alone converts to a feed with no depth events at all, and every
    backtest step blocks on `no_bid`. The expected set is `session_start` x the
    dataset profile, so the profile has to be able to say this.
    """
    d = tmp_path / "hl-nobook"
    d.mkdir()
    write_gz(
        d / f"btc_{DAY}.gz",
        [(ns(0), hl_trade("BTC", ns(0))), (ns(1), hl_bbo("BTC", ns(1)))],
    )
    write_meta(
        d,
        "hyperliquid",
        DAY,
        [(ns(0), session_start("hyperliquid", ["BTC"], hl_l2_modes=("none",)))],
    )
    out = tmp_path / "r.json"
    code, report = run(d, out=out)
    assert code == 1, "a dataset whose backtest can never place an order is not green"
    assert "profile_unsatisfiable" in checks_of(report, "hyperliquid", severity="red")


def test_a_fast_only_recording_still_satisfies_the_profile(tmp_path):
    """The doc's acceptance line: a legal `--hl-l2-modes fast` must not go red."""
    d = hl_dir(tmp_path, modes=("fast",), name="hl-fast-only")
    out = tmp_path / "r.json"
    code, report = run(d, out=out)
    assert code == 0, issues_of(report, "hyperliquid")


# --------------------------------------------------------------------------
# Review fixes: coverage is per symbol and per stream, not a venue-wide union
# --------------------------------------------------------------------------


def test_symbol_coverage_is_the_interval_where_every_required_stream_is_live(tmp_path):
    """A late-starting required stream must move the symbol's coverage start.

    Venue coverage is the union over symbols and streams, so an on-time `bbo`
    hides an `l2Book` that only started ten minutes in. Phase 3 trims to that
    union and the run begins over a window where the traded book does not exist
    yet — the opposite of §3.1's guarantee.
    """
    d = tmp_path / "hl-late-book"
    d.mkdir()
    late = ns(600)
    write_gz(
        d / f"btc_{DAY}.gz",
        [
            (ns(0), hl_trade("BTC", ns(0))),
            (ns(0), hl_bbo("BTC", ns(0))),
            (late, hl_l2("BTC", late, fast=False)),
            (ns(900), hl_bbo("BTC", ns(900))),
            (ns(900), hl_trade("BTC", ns(900))),
            (ns(900), hl_l2("BTC", ns(900), fast=False)),
        ],
    )
    write_meta(
        d, "hyperliquid", DAY,
        [(ns(0), session_start("hyperliquid", ["BTC"], hl_l2_modes=("slow",)))],
    )
    out = tmp_path / "r.json"
    _, report = run(d, out=out)
    sym = report["venues"]["hyperliquid"]["days"][DAY]["symbols"]["btc"]
    assert sym["coverage"]["first_local_ts"] == late
    assert sym["coverage"]["last_local_ts"] == ns(900)
    assert sym["coverage"]["required_streams"] == ["trades", "bbo", "l2Book_slow"]


def test_a_second_symbol_does_not_widen_the_first_symbols_coverage(tmp_path):
    """Per-symbol coverage: a partially accepted subscription is exactly this shape."""
    d = tmp_path / "um-two-symbols"
    d.mkdir()
    write_gz(
        d / f"btcusdt_{DAY}.gz",
        [
            (ns(600), um_book_ticker("BTCUSDT", ns(600))),
            (ns(1200), um_book_ticker("BTCUSDT", ns(1200))),
        ],
    )
    write_gz(
        d / f"ethusdt_{DAY}.gz",
        [
            (ns(0), um_book_ticker("ETHUSDT", ns(0))),
            (ns(1800), um_book_ticker("ETHUSDT", ns(1800))),
        ],
    )
    write_meta(
        d, "binancefuturesum", DAY,
        [(ns(0), session_start("binancefuturesum", ["BTCUSDT", "ETHUSDT"]))],
    )
    out = tmp_path / "r.json"
    _, report = run(d, out=out)
    symbols = report["venues"]["binancefuturesum"]["days"][DAY]["symbols"]
    assert symbols["btcusdt"]["coverage"]["first_local_ts"] == ns(600)
    assert symbols["btcusdt"]["coverage"]["last_local_ts"] == ns(1200)
    assert symbols["ethusdt"]["coverage"]["first_local_ts"] == ns(0)
    # The venue number stays the union, and is documented as such.
    assert report["venues"]["binancefuturesum"]["coverage"]["first_local_ts"] == ns(0)


# --------------------------------------------------------------------------
# Review fixes: cadence limits pinned to the nanosecond
# --------------------------------------------------------------------------


@pytest.mark.parametrize(
    "stream,limit_ns",
    [
        ("bbo", 14 * SEC),
        ("l2Book_slow", 54 * SEC),
        ("l2Book_fast", 5_400_000_000),
        ("trades", 120 * SEC),
    ],
)
def test_a_gap_of_exactly_the_limit_is_not_flagged_and_one_nanosecond_more_is(
    tmp_path, stream, limit_ns
):
    """Doubling every entry of MAX_GAP_NS used to leave the suite green."""
    assert qr.MAX_GAP_NS[(qr.HYPERLIQUID, stream)] == limit_ns

    def frames(gap_ns):
        maker = {
            "bbo": lambda t: hl_bbo("BTC", t),
            "trades": lambda t: hl_trade("BTC", t),
            "l2Book_slow": lambda t: hl_l2("BTC", t, fast=False),
            "l2Book_fast": lambda t: hl_l2("BTC", t, fast=True),
        }[stream]
        return [(ns(0), maker(ns(0))), (ns(0) + gap_ns, maker(ns(0) + gap_ns))]

    for name, gap_ns, expected in (("at", limit_ns, 0), ("over", limit_ns + 1, 1)):
        d = tmp_path / f"hl-{stream}-{name}"
        d.mkdir()
        write_gz(d / f"btc_{DAY}.gz", frames(gap_ns))
        write_meta(
            d, "hyperliquid", DAY,
            [(ns(0), session_start("hyperliquid", ["BTC"], hl_l2_modes=("slow", "fast")))],
        )
        out = tmp_path / f"r-{stream}-{name}.json"
        _, report = run(d, out=out)
        stat = report["venues"]["hyperliquid"]["days"][DAY]["symbols"]["btc"]["streams"][stream]
        assert stat["gap_count"] == expected, (stream, name, gap_ns)


# --------------------------------------------------------------------------
# Review fixes: CLI contract
# --------------------------------------------------------------------------


def test_help_exits_zero(capsys):
    """The docstring promises `2` for a usage error; `--help` is not one."""
    assert qr.main(["--help"]) == 0


def test_there_is_no_inert_mode_flag():
    """`--all-finalized` was declared, advertised and never read."""
    assert "--all-finalized" not in qr.build_parser().format_help()


def test_the_binancefutures_alias_is_canonicalised_to_the_backend_name(tmp_path):
    """`collector/src/main.rs` accepts both spellings for the USD-M backend and
    stamps the operator's word into `session_start` verbatim, so the same bytes
    would otherwise be buildable or not depending on which was typed."""
    d = tmp_path / "um-alias"
    d.mkdir()
    write_gz(d / f"btcusdt_{DAY}.gz", [(ns(0), um_book_ticker("BTCUSDT", ns(0)))])
    write_meta(d, "binancefutures", DAY, [(ns(0), session_start("binancefutures", ["BTCUSDT"]))])
    out = tmp_path / "r.json"
    code, report = run(d, out=out)
    assert code == 0, report
    assert set(report["venues"]) == {"binancefuturesum"}
    assert report["venues"]["binancefuturesum"]["exchange_as_recorded"] == "binancefutures"
