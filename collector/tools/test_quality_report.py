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
import re
import shutil
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


def um_mark_price(symbol, ts, index="63427.35155222"):
    """`markPriceUpdate` as the USD-M combined stream delivers it.

    Recorded from `<symbol>@markPrice@1s` (`binancefuturesum::STREAMS`). `i` is
    the **index price** — Binance's own spot basket, aggregated across its
    constituent exchanges — which with `r` (funding rate) is the reason the
    stream is recorded at all; neither can be reconstructed from the book or the
    tape afterwards.

    The field set is Binance's documented USD-M one. It could not be captured
    from the host these fixtures were written on — `fstream.binance.com` served
    only `@trade`, `@bookTicker` and `@depth@0ms` there — but the COIN-M sibling
    below WAS captured live and agrees on every field this report reads (`e`,
    and the stream envelope). COIN-M adds `ap` and `st`; nothing here looks at
    either.
    """
    return {
        "stream": f"{symbol.lower()}@markPrice@1s",
        "data": {
            "e": "markPriceUpdate",
            "E": ms_of(ts),
            "s": symbol,
            "p": "63406.00000000",
            "i": index,
            "P": "63402.48303662",
            "r": "0.00005945",
            "T": 1_785_254_400_000,
        },
    }


#: Captured verbatim from `dstream.binance.com` on 2026-07-28. Kept as the raw
#: line rather than a builder: the point of it is that the bytes the venue
#: actually sends classify, not that a dict this file wrote does.
CM_MARK_PRICE_CAPTURED = (
    '{"stream":"btcusd_perp@markPrice@1s","data":{"e":"markPriceUpdate",'
    '"E":1785239516000,"s":"BTCUSD_PERP","p":"63406.00000000",'
    '"ap":"63406.00000000","P":"63402.48303662","i":"63427.35155222",'
    '"r":"0.00005945","T":1785254400000,"st":2}}'
)


def hl_active_asset_ctx(coin, ts, oracle="63413.6"):
    """`activeAssetCtx`, the Hyperliquid half of the same information.

    `ctx.oraclePx` is Hyperliquid's own spot basket and the direct input to its
    funding calculation; `ctx.funding` is the rate itself. Subscribed for every
    coin unconditionally (`hyperliquid::ALWAYS_ON`).

    Field set captured verbatim from mainnet 2026-07-28. Note what is NOT in it:
    no venue timestamp and no sequence number of any kind, so `local_ts` and its
    cadence are the only evidence this report has about the feed — the same
    position `l2Book` and `bbo` are in.

    `ts` is accepted for symmetry with the other builders and deliberately
    unused: the frame carries no field to put it in.
    """
    return {
        "channel": "activeAssetCtx",
        "data": {
            "coin": coin,
            "ctx": {
                "funding": "0.0000125",
                "openInterest": "36584.28596",
                "prevDayPx": "65126.0",
                "dayNtlVlm": "2278613264.705988884",
                "premium": "-0.0003453518",
                "oraclePx": oracle,
                "markPx": "63390.0",
                "midPx": "63389.5",
                "impactPxs": ["63389.0", "63391.7"],
                "dayBaseVlm": "35444.01912",
            },
        },
    }


#: The same frame for a HIP-3 builder-dex instrument, captured live the same
#: day. The `dex:` prefix rides along in `data.coin`, which is what files it
#: next to that instrument's book instead of splitting one instrument in two.
HL_ACTIVE_ASSET_CTX_DEX_CAPTURED = (
    '{"channel":"activeAssetCtx","data":{"coin":"xyz:GOLD","ctx":'
    '{"funding":"0.00000625","openInterest":"41657.4252","prevDayPx":"4097.2",'
    '"dayNtlVlm":"39058137.6294000074","premium":"0.0001365578",'
    '"oraclePx":"4027.6","markPx":"4028.4","midPx":"4028.15",'
    '"impactPxs":["4028.1","4028.2"],"dayBaseVlm":"9602.0065"}}}'
)


def um_depth_snapshot(symbol, ts, last_update_id=1000):
    """The REST depth snapshot, written into the symbol file bare.

    `binancefuturesum/mod.rs` pulls it from a **detached** `tokio::spawn` after
    a `pu` break and sends it straight to the writer hop, while WS frames queue
    through the WS hop first. Two producers, two `Utc::now()` stamps, one FIFO —
    which is the whole reason `local_ts` order and file order can disagree.
    """
    return {
        "lastUpdateId": last_update_id,
        "E": ms_of(ts),
        "T": ms_of(ts),
        "bids": [["24670.90", "1"]],
        "asks": [["24671.00", "2"]],
    }


def lighter_book(market_id, ts, begin, nonce, kind="update"):
    """A Lighter order-book frame, shaped as mainnet sends them (2026-07-28).

    Two details this mirrors deliberately. The channel carries the **market
    id**, not the symbol — `order_book:0` — so the stream name has to be the
    head of it. And the chain lives inside `order_book`, not on the envelope:
    `begin_nonce`..`nonce`, with a snapshot carrying `begin_nonce: 0` and the
    `nonce` the first diff after it chains from.

    `offset` rides along because the venue sends it and it is a trap: it is
    API-server-local and jumps on reconnect, so it must never be read as a
    sequence number.
    """
    return {
        "channel": f"order_book:{market_id}",
        "type": f"{kind}/order_book",
        "timestamp": ms_of(ts),
        "last_updated_at": ts // 1000,
        "offset": 2043106 + nonce,
        "order_book": {
            "code": 0,
            "asks": [{"price": "1914.28", "size": "0.1021"}],
            "bids": [{"price": "1914.22", "size": "0.0000"}],
            "offset": 2043106 + nonce,
            "nonce": nonce,
            "begin_nonce": begin,
            "last_updated_at": ts // 1000,
        },
    }


def lighter_ticker(market_id, ts, symbol="ETH"):
    """The event-driven touch. Carries a bare `nonce` and no `begin_nonce`."""
    return {
        "channel": f"ticker:{market_id}",
        "type": "update/ticker",
        "timestamp": ms_of(ts),
        "last_updated_at": ts // 1000,
        "nonce": 17926043071,
        "ticker": {
            "s": symbol,
            "a": {"price": "1914.28", "size": "0.1021"},
            "b": {"price": "1914.26", "size": "0.1900"},
            "last_updated_at": ts // 1000,
        },
    }


def lighter_trade(market_id, ts):
    return {
        "channel": f"trade:{market_id}",
        "type": "update/trade",
        "nonce": 17926062253,
        "liquidation_trades": [],
        "trades": [
            {
                "trade_id": 26280867773,
                "market_id": market_id,
                "size": "0.0008",
                "price": "1913.99",
                "is_maker_ask": True,
                "timestamp": ms_of(ts),
                "transaction_time": ts // 1000,
            }
        ],
    }


def lighter_stats(market_id, ts, symbol="ETH"):
    """`market_stats` — the venue's funding/oracle aggregate."""
    return {
        "channel": f"market_stats:{market_id}",
        "type": "update/market_stats",
        "timestamp": ms_of(ts),
        "market_stats": {
            "symbol": symbol,
            "market_id": market_id,
            "index_price": "1915.31",
            "mark_price": "1914.32",
            "current_funding_rate": "-0.0012",
            "funding_timestamp": ms_of(ts),
            "open_interest": "86574362.972120",
        },
    }


def lighter_session_start(symbols, markets=None):
    """`session_start` as the lighter arm of `main.rs` writes it.

    The `lighter_markets` map is the venue-specific field: the frames name a
    market by integer and never by symbol, so without it a finished recording
    cannot say which instrument market 0 was.
    """
    record = session_start("lighter", symbols)
    record["lighter_markets"] = markets or {
        symbol: index for index, symbol in enumerate(symbols)
    }
    return record


def lighter_dir(tmp_path, channels=("order_book", "ticker", "trade", "market_stats"), day=DAY):
    """A minimal, complete Lighter recording of one market."""
    d = tmp_path / "lighter"
    d.mkdir()
    builders = {
        "order_book": lambda ts: lighter_book(0, ts, begin=0, nonce=100, kind="subscribed"),
        "ticker": lambda ts: lighter_ticker(0, ts),
        "trade": lambda ts: lighter_trade(0, ts),
        "market_stats": lambda ts: lighter_stats(0, ts),
    }
    # Two frames per channel, at both ends of the same window. Coverage is the
    # interval in which every stream is live — an intersection — so one frame
    # per channel at a different second each would leave no overlap at all and
    # a null window that says nothing about this venue.
    recs = [(ns(t), builders[c](ns(t))) for t in (0, 10) for c in channels]
    recs.sort(key=lambda pair: pair[0])
    write_gz(d / f"eth_{day}.gz", recs)
    write_meta(d, "lighter", day, [(ns(0), lighter_session_start(["ETH"]))])
    return d


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


def test_lighter_nonce_chain_break_is_counted_and_a_snapshot_resets_the_chain(tmp_path):
    """Lighter's chain runs `begin_nonce(N+1) == nonce(N)`, snapshots included.

    Unlike Bybit's `u`, the numbers do not increment by one — they are engine
    nonces and jump by tens between batches — so "the next id" is not a thing
    that can be checked here. Only the explicit link is.

    The snapshot restarting the chain is what the collector's own resubscribe
    produces after a break (`lighter/mod.rs`), so a report that did not treat
    it as a restart would count every repair as a second break.
    """
    d = tmp_path / "lighter"
    d.mkdir()
    write_gz(
        d / f"eth_{DAY}.gz",
        [
            (ns(0), lighter_book(0, ns(0), begin=0, nonce=100, kind="subscribed")),
            (ns(1), lighter_book(0, ns(1), begin=100, nonce=170)),
            (ns(2), lighter_book(0, ns(2), begin=999, nonce=1040)),  # break: 170 -> 999
            (ns(3), lighter_book(0, ns(3), begin=0, nonce=2000, kind="subscribed")),  # repair
            (ns(4), lighter_book(0, ns(4), begin=2000, nonce=2050)),
        ],
    )
    write_meta(d, "lighter", DAY, [(ns(0), lighter_session_start(["ETH"]))])
    out = tmp_path / "r.json"
    code, report = run(d, out=out)
    assert code == 0
    sym = report["venues"]["lighter"]["days"][DAY]["symbols"]["eth"]
    assert sym["sequence_breaks"]["order_book"] == 1, sym["sequence_breaks"]


def test_lighter_streams_are_classified_not_lumped_as_unknown(tmp_path):
    """The four channels are told apart by the head of `channel`.

    The market id is the tail (`order_book:0`), and it must not become part of
    the stream name: every symbol file would then carry a stream nothing has a
    cadence limit for, and the whole venue would read as `unclassified_frame`.
    """
    d = lighter_dir(tmp_path)
    out = tmp_path / "r.json"
    code, report = run(d, out=out)
    assert code == 0
    sym = report["venues"]["lighter"]["days"][DAY]["symbols"]["eth"]
    assert set(sym["streams"]) == {"order_book", "ticker", "trade", "market_stats"}, sym["streams"]
    assert sym["unclassified_frames"] == 0


def test_a_lighter_day_is_checked_but_can_never_be_red_for_a_missing_stream(tmp_path):
    """Lighter is not part of a mode-A dataset, so nothing it does blocks one.

    Its declared channels are still checked, as warnings, because a silently
    dropped subscription is exactly what this report exists to catch — the
    venue answers an unknown market with an error frame and keeps the socket
    open, so an unsubscribed channel looks identical to a quiet one.
    """
    d = lighter_dir(tmp_path, channels=("order_book", "ticker"))
    out = tmp_path / "r.json"
    code, report = run(d, out=out)
    assert code == 0, "a missing lighter stream must not block a build"
    day = report["venues"]["lighter"]["days"][DAY]
    assert day["verdict"] == "yellow", day["issues"]
    sym = day["symbols"]["eth"]
    assert sym["missing_required"] == []
    assert set(sym["missing_optional"]) == {"trade", "market_stats"}


def test_a_complete_lighter_day_reports_a_coverage_window(tmp_path):
    """Coverage is still computed for a venue nothing is required of.

    With no required stream the window falls back to everything recorded — the
    same path Bybit takes — and a null window on a directory full of data would
    read as "this day holds nothing".
    """
    d = lighter_dir(tmp_path)
    out = tmp_path / "r.json"
    code, report = run(d, out=out)
    assert code == 0
    cov = report["venues"]["lighter"]["days"][DAY]["symbols"]["eth"]["coverage"]
    assert cov["first_local_ts"] is not None and cov["last_local_ts"] is not None, cov
    assert set(cov["required_streams"]) == {"order_book", "ticker", "trade", "market_stats"}


def test_a_quiet_lighter_ticker_over_a_live_book_is_not_reported(tmp_path):
    """`ticker` fires on a change of the touch and on nothing else.

    Measured on mainnet 2026-07-28 it is the fastest channel of the four
    (0.0095s median), which is exactly why its silence means the least: a
    market whose top of book stops moving emits nothing while the batched
    `order_book` feed beside it keeps arriving. Same shape as Hyperliquid's
    `bbo`, same treatment.
    """
    d = tmp_path / "lighter"
    d.mkdir()
    # The book runs gaplessly across the whole window, chain and all.
    recs = [
        (ns(t), lighter_book(0, ns(t), begin=t * 10, nonce=(t + 2) * 10))
        for t in range(0, 60, 2)
    ]
    recs += [(ns(t), lighter_trade(0, ns(t))) for t in range(0, 60, 5)]
    recs += [(ns(t), lighter_stats(0, ns(t))) for t in range(0, 60, 5)]
    # A 40s hole in the ticker, well past its own limit, with the book running
    # gaplessly across it.
    recs += [(ns(t), lighter_ticker(0, ns(t))) for t in (0, 1, 41, 59)]
    recs.sort(key=lambda pair: pair[0])
    write_gz(d / f"eth_{DAY}.gz", recs)
    write_meta(d, "lighter", DAY, [(ns(0), lighter_session_start(["ETH"]))])
    out = tmp_path / "r.json"
    code, report = run(d, out=out)

    assert code == 0
    assert "cadence_gap" not in checks_of(report, "lighter"), issues_of(report, "lighter")


# --------------------------------------------------------------------------
# Extended: URL-based channels, RFQ sibling files
# --------------------------------------------------------------------------


def extended_book(symbol, ts, seq, kind="DELTA"):
    return {
        "type": kind,
        "data": {"t": kind, "m": symbol, "b": [], "a": [], "d": "f"},
        "ts": ms_of(ts),
        "seq": seq,
    }


def extended_trade(symbol, ts, seq):
    return {
        "data": [{"i": seq, "m": symbol, "S": "BUY", "tT": "TRADE", "T": ms_of(ts), "p": "1", "q": "1"}],
        "ts": ms_of(ts),
        "seq": seq,
    }


def extended_mark(symbol, ts, seq):
    return {"type": "MP", "data": {"m": symbol, "p": "1", "ts": 0}, "ts": ms_of(ts), "seq": seq}


def extended_funding(symbol, ts, seq):
    return {"ts": ms_of(ts), "data": {"m": symbol, "T": ms_of(ts), "f": "0.001"}, "seq": seq}


def extended_dir(tmp_path, streams=("orderbook", "trades", "mark", "funding"), rfq=False, day=DAY):
    """A minimal, complete Extended recording for BTC-USD.

    Each stream present spans the same window (a frame at t=0 and t=5), so a
    complete day has a non-empty coverage window — the intersection across the
    streams — rather than four disjoint single points. `rfq=True` also writes
    the `{market}-rfq` sibling file (executable book).
    """
    d = tmp_path / "extended"
    d.mkdir()
    recs = []
    seq = 0
    for t in (0, 5):
        if "orderbook" in streams:
            seq += 1
            kind = "SNAPSHOT" if t == 0 else "DELTA"
            recs.append((ns(t), extended_book("BTC-USD", ns(t), seq, kind=kind)))
        if "trades" in streams:
            seq += 1
            recs.append((ns(t), extended_trade("BTC-USD", ns(t), seq)))
        if "mark" in streams:
            seq += 1
            recs.append((ns(t), extended_mark("BTC-USD", ns(t), seq)))
        if "funding" in streams:
            seq += 1
            recs.append((ns(t), extended_funding("BTC-USD", ns(t), seq)))
    recs.sort(key=lambda pair: pair[0])
    write_gz(d / f"btc-usd_{day}.gz", recs)
    if rfq:
        write_gz(
            d / f"btc-usd-rfq_{day}.gz",
            [
                (ns(0), extended_book("BTC-USD", ns(0), 1, kind="SNAPSHOT")),
                (ns(1), extended_book("BTC-USD", ns(1), 2)),
            ],
        )
    write_meta(d, "extended", day, [(ns(0), session_start("extended", ["BTC-USD"]))])
    return d


#: The four stream classes of an Extended symbol file, and how to make one.
#:
#: One entry per class because on this venue one class IS one socket — the
#: firehose `/orderbooks`, and `/publicTrades/{m}`, `/funding/{m}`,
#: `/prices/mark/{m}` per market (`collector/src/extended/mod.rs::channel_urls`).
#: That is the whole content of the fan-out exemption, so the tests below build
#: their pairs by naming two classes rather than two frame shapes.
EXTENDED_FRAME = {
    "orderbook": lambda ts, seq: extended_book("BTC-USD", ts, seq),
    "trades": lambda ts, seq: extended_trade("BTC-USD", ts, seq),
    "mark": lambda ts, seq: extended_mark("BTC-USD", ts, seq),
    "funding": lambda ts, seq: extended_funding("BTC-USD", ts, seq),
}


def extended_inverted(tmp_path, name, first, second, delta_ns, day=DAY):
    """An Extended recording holding one out-of-order pair and nothing else odd.

    Write order is `first` then `second`, with `second` stamped `delta_ns`
    *before* `first` — i.e. exactly the shape the daily gate saw on 20260804.
    A book snapshot leads so the file has the one stream its profile calls for.
    """
    d = tmp_path / name
    d.mkdir()
    base = ns(1)
    write_gz(
        d / f"btc-usd_{day}.gz",
        [
            (ns(0), extended_book("BTC-USD", ns(0), 1, kind="SNAPSHOT")),
            (base, EXTENDED_FRAME[first](base, 2)),
            (base - delta_ns, EXTENDED_FRAME[second](base - delta_ns, 3)),
        ],
    )
    write_meta(d, "extended", day, [(ns(0), session_start("extended", ["BTC-USD"]))])
    return d


def test_family_of_and_expected_streams_know_extended():
    """The report used to raise `ValueError` on `session_start.exchange` =
    "extended", so a recorded day could not be gate-checked at all.

    Extended is not part of a mode-A dataset, so — like Bybit and Lighter —
    nothing it does can make one red: `required` is empty. The book is optional
    (every file carries one), the conditional feeds are informational.
    """
    assert qr.family_of("extended") == qr.EXTENDED

    expected = qr.expected_streams("mode-a-v1", "extended", {})
    assert expected.required == ()
    assert expected.optional == ("orderbook",)
    assert set(expected.informational) == {"trades", "funding", "mark"}
    assert expected.violation is None


@pytest.mark.parametrize(
    "raw,stream",
    [
        (json.dumps(extended_book("BTC-USD", ns(0), 1, kind="SNAPSHOT")), "orderbook"),
        (json.dumps(extended_book("BTC-USD", ns(0), 2)), "orderbook"),
        (json.dumps(extended_trade("BTC-USD", ns(0), 3)), "trades"),
        (json.dumps(extended_mark("BTC-USD", ns(0), 4)), "mark"),
        (json.dumps(extended_funding("BTC-USD", ns(0), 5)), "funding"),
    ],
)
def test_extended_frames_are_classified_not_lumped_as_unknown(raw, stream):
    """Each of the four shapes has to become a stream of its own.

    `mark` and `funding` are the trap: both are `{data:{m, …}}` objects, told
    apart only by `type:MP` versus the funding rate `f`. Getting it wrong would
    make the whole feed `(unclassified)` and every Extended day yellow.
    """
    assert qr.classify(qr.EXTENDED, json.loads(raw)) == stream


def test_a_complete_extended_day_is_checked_and_never_red(tmp_path):
    """A day with all four feeds is green and reports a coverage window.

    Nothing is required (Extended is not a mode-A venue), so with the book
    present and the informational feeds present-and-checked there is nothing to
    warn about.
    """
    d = extended_dir(tmp_path)
    out = tmp_path / "r.json"
    code, report = run(d, out=out)

    assert code == 0, issues_of(report, "extended")
    day = report["venues"]["extended"]["days"][DAY]
    assert day["verdict"] == "green", day["issues"]
    sym = day["symbols"]["btc-usd"]
    assert set(sym["streams"]) == {"orderbook", "trades", "mark", "funding"}
    assert sym["unclassified_frames"] == 0
    assert sym["missing_optional"] == []
    cov = sym["coverage"]
    assert cov["first_local_ts"] is not None and cov["last_local_ts"] is not None


def test_an_extended_book_absent_is_a_warning_not_a_crash(tmp_path):
    """The one stream every Extended file must carry is the book.

    A file with no book is a real, actionable yellow (`missing_optional`); the
    conditional feeds staying absent raise nothing, because an RFQ sibling or a
    spot market legitimately has none. Never red — Extended blocks no dataset.
    """
    d = extended_dir(tmp_path, streams=("trades",))
    out = tmp_path / "r.json"
    code, report = run(d, out=out)

    assert code == 0, "a missing Extended stream must never block a build"
    day = report["venues"]["extended"]["days"][DAY]
    assert day["verdict"] == "yellow", day["issues"]
    sym = day["symbols"]["btc-usd"]
    assert sym["missing_required"] == []
    assert "orderbook" in sym["missing_optional"]
    # The conditional feeds are informational: absent is silent, not a warning.
    assert set(sym["missing_informational"]) == {"trades", "funding", "mark"} - {"trades"}


def test_extended_rfq_sibling_file_is_expected_not_a_leftover(tmp_path):
    """`{market}-rfq` is the executable book recorded on purpose beside the
    plain one, so it must not read as `unexpected_symbol` — the check that
    otherwise fires for any file whose symbol was not in `session_start`.
    """
    d = extended_dir(tmp_path, rfq=True)
    out = tmp_path / "r.json"
    code, report = run(d, out=out)

    assert code == 0
    checks = checks_of(report, "extended")
    assert "unexpected_symbol" not in checks, issues_of(report, "extended")
    day = report["venues"]["extended"]["days"][DAY]
    # The sibling is still checked: it carries only the book, which satisfies
    # its optional set, and its informational feeds stay silent when absent.
    rfq_sym = day["symbols"]["btc-usd-rfq"]
    assert set(rfq_sym["streams"]) == {"orderbook"}
    assert rfq_sym["missing_optional"] == []


# --------------------------------------------------------------------------
# Paradex: JSON-RPC channels, two books
# --------------------------------------------------------------------------
#
# Every fixture below is a real frame captured from the Tokyo recorder on
# 2026-08-04 (`/opt/hft-collector/data/paradex`), trimmed of its level arrays.
# A synthetic shape would have pinned this report against a guess about the
# venue rather than against the venue.


def paradex_frame(channel, data):
    """The JSON-RPC envelope every Paradex data update arrives in."""
    return {"jsonrpc": "2.0", "method": "subscription", "params": {"channel": channel, "data": data}}


def paradex_bbo(market, ts, seq):
    return paradex_frame(
        f"bbo.{market}",
        {
            "market": market,
            "seq_no": seq,
            "ask": "1.75",
            "ask_size": "425.4",
            "bid": "1.725",
            "bid_size": "115.8",
            "last_updated_at": ms_of(ts),
        },
    )


def paradex_book(market, ts, seq, feed="snapshot"):
    """`order_book.{market}.{feed}@15@100ms` — the depth and refresh are in the
    channel name, so the feed word is the second `.`-segment of a three-segment
    channel whose market itself contains dashes."""
    return paradex_frame(
        f"order_book.{market}.{feed}@15@100ms",
        {
            "seq_no": seq,
            "market": market,
            "last_updated_at": ms_of(ts),
            "update_type": "s",
            "inserts": [{"side": "BUY", "price": "1.721", "size": "288.3"}],
            "updates": [],
            "deletes": [],
        },
    )


def paradex_trade(market, ts, seq):
    return paradex_frame(
        f"trades.{market}",
        {
            "id": f"{ms_of(ts)}2017092384900{seq:02d}",
            "market": market,
            "side": "BUY",
            "size": "0.0002",
            "price": "63502.5",
            "created_at": ms_of(ts),
            "trade_type": "RPI",
        },
    )


def paradex_funding(market, ts):
    return paradex_frame(
        f"funding_data.{market}",
        {
            "market": market,
            "funding_index": "0.02992082550491794701",
            "funding_premium": "0.0001231622482595002781",
            "funding_rate": "0.00007100916923",
            "created_at": ms_of(ts),
        },
    )


MARKET = "NEAR-USD-PERP"


def paradex_dir(
    tmp_path,
    streams=("book_snapshot", "book_interactive", "bbo", "trades", "funding"),
    day=DAY,
    times=(0, 5),
):
    """A Paradex recording for one market, `streams` present at each of `times`."""
    d = tmp_path / "paradex"
    d.mkdir()
    recs = []
    seq = 0
    for t in times:
        for stream in streams:
            seq += 1
            if stream == "book_snapshot":
                recs.append((ns(t), paradex_book(MARKET, ns(t), seq, feed="snapshot")))
            elif stream == "book_interactive":
                recs.append((ns(t), paradex_book(MARKET, ns(t), seq, feed="interactive")))
            elif stream == "bbo":
                recs.append((ns(t), paradex_bbo(MARKET, ns(t), seq)))
            elif stream == "trades":
                recs.append((ns(t), paradex_trade(MARKET, ns(t), seq)))
            elif stream == "funding":
                recs.append((ns(t), paradex_funding(MARKET, ns(t))))
    recs.sort(key=lambda pair: pair[0])
    write_gz(d / f"{MARKET.lower()}_{day}.gz", recs)
    write_meta(d, "paradex", day, [(ns(0), session_start("paradex", [MARKET]))])
    return d


def test_family_of_and_expected_streams_know_paradex():
    """The gate exited 2 on `session_start.exchange` = "paradex": no family, and
    `expected_streams` fell through to its fail-closed raise. One venue doing
    that fails the whole `gate@all` service, so a venue recording perfectly well
    took the report down for every other venue with it.

    Paradex is collect-only — no mode-A dataset reads it — so nothing it does
    may be red: `required` is empty. Both books are the norm on every file and
    are optional; the three per-market feeds are informational, because a thin
    perp legitimately prints nothing for hours (measured: 15 trades in 8.1h on
    NEAR-USD-PERP, 2026-08-04).
    """
    assert qr.family_of("paradex") == qr.PARADEX

    expected = qr.expected_streams("mode-a-v1", "paradex", {})
    assert expected.required == ()
    assert expected.optional == ("book_snapshot", "book_interactive")
    assert set(expected.informational) == {"bbo", "trades", "funding"}
    assert expected.violation is None


@pytest.mark.parametrize(
    "raw,stream",
    [
        (json.dumps(paradex_bbo(MARKET, ns(0), 1)), "bbo"),
        (json.dumps(paradex_book(MARKET, ns(0), 2, feed="snapshot")), "book_snapshot"),
        (json.dumps(paradex_book(MARKET, ns(0), 3, feed="interactive")), "book_interactive"),
        (json.dumps(paradex_trade(MARKET, ns(0), 4)), "trades"),
        (json.dumps(paradex_funding(MARKET, ns(0))), "funding"),
    ],
)
def test_paradex_frames_are_classified_not_lumped_as_unknown(raw, stream):
    """The two books are the trap: same envelope, same payload shape, told apart
    only by the `snapshot`/`interactive` word inside the third `.`-segment. They
    are DIFFERENT books — the RPI-inclusive one against the plain one is the
    whole reason this venue is recorded — so collapsing them into one stream
    would hide either going silent.
    """
    assert qr.classify(qr.PARADEX, json.loads(raw)) == stream


@pytest.mark.parametrize(
    "raw",
    [
        json.dumps({"jsonrpc": "2.0", "result": {"channel": f"bbo.{MARKET}"}, "id": 1}),
        json.dumps({"jsonrpc": "2.0", "error": {"code": -32000, "message": "bad channel"}, "id": 2}),
        json.dumps({"jsonrpc": "2.0", "result": {}, "id": 3}),
        json.dumps({"params": {"channel": f"order_book.{MARKET}.deltas@15@100ms", "data": {}}}),
    ],
)
def test_a_paradex_frame_with_no_data_channel_is_not_a_stream(raw):
    """Acks, venue errors and pongs carry no `params.channel` and are meta, the
    same call `collector/src/paradex/mod.rs::route` makes — classifying one as a
    stream would give it a cadence expectation it can never meet.

    A channel the collector records and this report has not been taught (the
    full-depth `deltas` feed, reserved for a core set) deliberately falls here
    too: an `unclassified_frame` yellow is the signal that says teach the report,
    which a name derived from the channel would have swallowed silently.
    """
    assert qr.classify(qr.PARADEX, json.loads(raw)) is None


def test_a_complete_paradex_day_is_checked_and_never_red(tmp_path):
    """All five feeds present: green, everything classified, coverage reported."""
    d = paradex_dir(tmp_path)
    out = tmp_path / "r.json"
    code, report = run(d, out=out)

    assert code == 0, issues_of(report, "paradex")
    day = report["venues"]["paradex"]["days"][DAY]
    assert day["verdict"] == "green", day["issues"]
    sym = day["symbols"][MARKET.lower()]
    assert set(sym["streams"]) == {"book_snapshot", "book_interactive", "bbo", "trades", "funding"}
    assert sym["unclassified_frames"] == 0
    assert sym["missing_optional"] == []


def test_a_paradex_day_of_books_alone_is_green_and_never_exit_2(tmp_path):
    """The normal shape of a thin market: both books, and hours with no print,
    no top-of-book change and (on a short run) no funding tick.

    Books satisfy the optional set; the three informational feeds absent is
    silent, not a warning. Nothing here may cost the gate a non-zero exit — one
    venue's exit 2 fails `gate@all` for every venue.
    """
    d = paradex_dir(tmp_path, streams=("book_snapshot", "book_interactive"))
    out = tmp_path / "r.json"
    code, report = run(d, out=out)

    assert code == 0, issues_of(report, "paradex")
    day = report["venues"]["paradex"]["days"][DAY]
    assert day["verdict"] == "green", day["issues"]
    sym = day["symbols"][MARKET.lower()]
    assert sym["missing_required"] == []
    assert sym["missing_optional"] == []
    assert set(sym["missing_informational"]) == {"bbo", "trades", "funding"}


def test_a_paradex_book_absent_is_a_warning_not_a_crash(tmp_path):
    """One book without the other is a real, actionable yellow: the pair is the
    measurement (RPI-inclusive against plain), so half of it is not the dataset
    this venue is recorded for. Still never red — it blocks no build.
    """
    d = paradex_dir(tmp_path, streams=("book_snapshot", "bbo"))
    out = tmp_path / "r.json"
    code, report = run(d, out=out)

    assert code == 0, "a missing Paradex stream must never block a build"
    day = report["venues"]["paradex"]["days"][DAY]
    assert day["verdict"] == "yellow", day["issues"]
    sym = day["symbols"][MARKET.lower()]
    assert sym["missing_required"] == []
    assert sym["missing_optional"] == ["book_interactive"]


def test_a_paradex_trades_drought_is_not_a_cadence_gap(tmp_path):
    """`trades` has no cadence limit at all, on purpose.

    Measured 2026-08-04 over 8.1h: NEAR-USD-PERP printed 15 times (worst hole
    1h33m), SOL-USD-PERP 37 times (worst 1h43m). Any limit that does not flag
    those is not a limit, and one that does flags a healthy thin market every
    day — so the honest answer is that this feed has no cadence to check.
    """
    d = paradex_dir(
        tmp_path,
        streams=("book_snapshot", "book_interactive", "trades"),
        times=(0, 100, 200, 300, 400, 500, 600, 7_000),
    )
    out = tmp_path / "r.json"
    code, report = run(d, out=out)

    assert code == 0
    sym = report["venues"]["paradex"]["days"][DAY]["symbols"][MARKET.lower()]
    assert sym["streams"]["trades"]["gap_count"] == 0, "trades must carry no cadence limit"
    gaps = [i for i in issues_of(report, "paradex") if i["check"] == "cadence_gap"]
    assert [g for g in gaps if "trades" in g["detail"]] == []


def test_a_quiet_paradex_bbo_hole_the_books_disprove_is_not_reported(tmp_path):
    """`bbo` fires on a change of the touch and on nothing else, so a thin market
    in a quiet hour emits nothing while the socket is healthy.

    Measured 2026-08-04, 8.1h: 13 such holes on NEAR-USD-PERP and 20 on
    ONDO-USD-PERP past the 300s limit (worst 41 minutes), with both books
    running across every one of them — worst book hole on those two markets,
    173.7s, inside their own limit. That is the same shape, and the same
    measured count, that earned Hyperliquid's `bbo` its reference.
    """
    d = tmp_path / "paradex"
    d.mkdir()
    recs = []
    seq = 0
    for t in range(100, 801, 100):
        seq += 1
        recs.append((ns(t), paradex_book(MARKET, ns(t), seq, feed="snapshot")))
        seq += 1
        recs.append((ns(t), paradex_book(MARKET, ns(t), seq, feed="interactive")))
    # One bbo at each end of the books' window and nothing between: a 700s hole,
    # spanned by two books that had none. The window starts at t=100 so that the
    # `session_start` record does not fall inside the hole — a hole the sidecar
    # already accounts for is never suppressed, and this test is about the case
    # where the sidecar has nothing to say.
    recs.append((ns(100), paradex_bbo(MARKET, ns(100), 900)))
    recs.append((ns(800), paradex_bbo(MARKET, ns(800), 901)))
    recs.sort(key=lambda pair: pair[0])
    write_gz(d / f"{MARKET.lower()}_{DAY}.gz", recs)
    write_meta(d, "paradex", DAY, [(ns(0), session_start("paradex", [MARKET]))])
    out = tmp_path / "r.json"
    code, report = run(d, out=out)

    assert code == 0
    assert "cadence_gap" not in checks_of(report, "paradex"), issues_of(report, "paradex")
    bbo = report["venues"]["paradex"]["days"][DAY]["symbols"][MARKET.lower()]["streams"]["bbo"]
    # Measured either way: the hole is recorded, and recorded as disproved.
    assert bbo["gap_count"] == 1
    assert bbo["suppressed_gap_count"] == 1


# --------------------------------------------------------------------------
# Aster: a literal Binance USD-M clone
# --------------------------------------------------------------------------
#
# Frames captured from the Tokyo recorder on 2026-08-04
# (`/opt/hft-collector/data/aster`), verbatim but for shortened level arrays.


def aster_book_ticker(symbol, ts, u):
    return {
        "stream": f"{symbol.lower()}@bookTicker",
        "data": {
            "e": "bookTicker",
            "u": u,
            "s": symbol,
            "b": "0.4026000",
            "B": "19318.4",
            "a": "0.4035000",
            "A": "2993.4",
            "T": ms_of(ts) - 40,
            "E": ms_of(ts),
        },
    }


def aster_depth(symbol, ts, first_u, last_u, prev_u):
    return {
        "stream": f"{symbol.lower()}@depth@0ms",
        "data": {
            "e": "depthUpdate",
            "E": ms_of(ts),
            "T": ms_of(ts) - 50,
            "s": symbol,
            "U": first_u,
            "u": last_u,
            "pu": prev_u,
            "b": [],
            "a": [["0.4039000", "18568.9"]],
        },
    }


def aster_trade(symbol, ts, tid):
    return {
        "stream": f"{symbol.lower()}@trade",
        "data": {
            "e": "trade",
            "E": ms_of(ts),
            "T": ms_of(ts) - 30,
            "s": symbol,
            "t": tid,
            "p": "0.4022000",
            "q": "20.1",
            "X": "MARKET",
            "m": True,
        },
    }


def aster_premium_index(symbol, ts):
    """The REST poller's element, written bare — no envelope, no `e`, and the
    symbol under `symbol` rather than `s`. Identical to Binance USD-M's, because
    the venue is a clone down to `GET /fapi/v1/premiumIndex`."""
    return {
        "symbol": symbol,
        "markPrice": "0.40330000",
        "indexPrice": "0.40383338",
        "estimatedSettlePrice": "0.40091637",
        "lastFundingRate": "-0.00004455",
        "interestRate": "0.00010000",
        "nextFundingTime": ms_of(ts) + 3_600_000,
        "time": ms_of(ts),
    }


ASTER_SYMBOL = "ETHFIUSDT"


def aster_dir(
    tmp_path,
    streams=("bookTicker", "depthUpdate", "trade", "premiumIndex"),
    day=DAY,
    times=(0, 5),
):
    d = tmp_path / "aster"
    d.mkdir()
    recs = []
    u = 499_657_978_442
    # The `pu` chain is per stream: each depth frame links to the LAST update id
    # of the previous depth frame, not to whatever id was handed out in between.
    # Chaining it wrong would make every fixture here report a sequence break.
    last_depth_u = u
    for t in times:
        for stream in streams:
            if stream == "bookTicker":
                u += 1
                recs.append((ns(t), aster_book_ticker(ASTER_SYMBOL, ns(t), u)))
            elif stream == "depthUpdate":
                first_u, u = u + 1, u + 2
                recs.append((ns(t), aster_depth(ASTER_SYMBOL, ns(t), first_u, u, last_depth_u)))
                last_depth_u = u
            elif stream == "trade":
                recs.append((ns(t), aster_trade(ASTER_SYMBOL, ns(t), 29_045 + int(t))))
            elif stream == "premiumIndex":
                recs.append((ns(t), aster_premium_index(ASTER_SYMBOL, ns(t))))
    recs.sort(key=lambda pair: pair[0])
    write_gz(d / f"{ASTER_SYMBOL.lower()}_{day}.gz", recs)
    write_meta(d, "aster", day, [(ns(0), session_start("aster", [ASTER_SYMBOL]))])
    return d


def test_family_of_and_expected_streams_know_aster():
    """Aster is a literal Binance USD-M clone — combined-stream `sym@channel`
    envelope, the same `data.e` event names, the same bare `premiumIndex`
    elements from the same REST path — so it reuses the Binance family rather
    than growing a second copy of those rules.

    Its expectations are its own, and they are permissive: Aster is collect-only,
    so unlike `binancefuturesum` (whose `bookTicker` mode A depends on) nothing
    it does may be red.
    """
    assert qr.family_of("aster") == qr.BINANCE

    expected = qr.expected_streams("mode-a-v1", "aster", {})
    assert expected.required == ()
    assert expected.optional == ("bookTicker", "depthUpdate")
    assert set(expected.informational) == {"trade", "premiumIndex"}
    assert expected.violation is None
    # The signal venue keeps its required stream: permissiveness is per exchange,
    # not a loosening of the family they share.
    assert qr.expected_streams("mode-a-v1", "binancefuturesum", {}).required == ("bookTicker",)


@pytest.mark.parametrize(
    "raw,stream",
    [
        (json.dumps(aster_book_ticker(ASTER_SYMBOL, ns(0), 1)), "bookTicker"),
        (json.dumps(aster_depth(ASTER_SYMBOL, ns(0), 2, 3, 1)), "depthUpdate"),
        (json.dumps(aster_trade(ASTER_SYMBOL, ns(0), 4)), "trade"),
        (json.dumps(aster_premium_index(ASTER_SYMBOL, ns(0))), "premiumIndex"),
    ],
)
def test_aster_frames_classify_under_the_binance_rules(raw, stream):
    """Pinned against real Aster frames, not against the claim that the venue is
    a clone: if it ever diverges, this is where it shows up rather than as a day
    of `unclassified_frame`.
    """
    assert qr.classify(qr.BINANCE, json.loads(raw)) == stream


def test_a_complete_aster_day_is_checked_and_never_red(tmp_path):
    d = aster_dir(tmp_path)
    out = tmp_path / "r.json"
    code, report = run(d, out=out)

    assert code == 0, issues_of(report, "aster")
    day = report["venues"]["aster"]["days"][DAY]
    assert day["verdict"] == "green", day["issues"]
    sym = day["symbols"][ASTER_SYMBOL.lower()]
    assert set(sym["streams"]) == {"bookTicker", "depthUpdate", "trade", "premiumIndex"}
    assert sym["unclassified_frames"] == 0
    assert sym["missing_optional"] == []


def test_an_aster_day_with_no_trades_is_not_red_and_not_exit_2(tmp_path):
    """A day of an Aster altcoin perp with no print at all is ordinary: measured
    2026-08-04, ETHFIUSDT printed 57 times in 8.2h and NEARUSDT 77. `trade` is
    therefore informational — absent is silent — and the day stays green.
    """
    d = aster_dir(tmp_path, streams=("bookTicker", "depthUpdate", "premiumIndex"))
    out = tmp_path / "r.json"
    code, report = run(d, out=out)

    assert code == 0, "a missing Aster stream must never block a build"
    day = report["venues"]["aster"]["days"][DAY]
    assert day["verdict"] == "green", day["issues"]
    sym = day["symbols"][ASTER_SYMBOL.lower()]
    assert sym["missing_required"] == []
    assert sym["missing_optional"] == []
    assert sym["missing_informational"] == ["trade"]


# --------------------------------------------------------------------------
# fail-closed: a venue nobody taught the report
# --------------------------------------------------------------------------


def test_an_untaught_exchange_is_still_fail_closed(tmp_path):
    """Paradex and Aster get explicit entries; nothing gets a blanket fallback.

    A venue this report has never seen must still raise, because the alternative
    — some permissive default — would silently pass a recording whose streams,
    cadences and expectations nobody has looked at. That is the whole failure
    this file exists to prevent, and it is why the two additions above are two
    named cases rather than one `else`.
    """
    with pytest.raises(ValueError, match="unknown exchange"):
        qr.family_of("bitmex")
    with pytest.raises(ValueError, match="no expected stream set"):
        qr.expected_streams("mode-a-v1", "bitmex", {})

    # End to end, it is exit 2 — the gate refusing to grade what it cannot read.
    d = tmp_path / "bitmex"
    d.mkdir()
    write_gz(d / f"xbtusd_{DAY}.gz", [(ns(0), {"table": "quote"})])
    write_meta(d, "bitmex", DAY, [(ns(0), session_start("bitmex", ["XBTUSD"]))])
    assert run(d)[0] == 2


# --------------------------------------------------------------------------
# 5./8. cadence gaps and their explanation
# --------------------------------------------------------------------------


def hl_quiet_book(bbo_gap=(1, 38), fast_every=5, fast_until=66):
    """A day where `bbo` falls silent while `l2Book_fast` keeps ticking.

    This is the shape of the 26 false yellows the Phase-2 gate produced for ENA
    on 2026-07-26: `bbo` is event-driven, so a top of book that does not move
    emits nothing, while the throttled book feed on the *same socket* runs
    without a hole and proves the connection was alive the whole time.
    """
    recs = [(ns(0), hl_trade("BTC", ns(0))), (ns(0), hl_l2("BTC", ns(0), fast=False))]
    recs += [
        (ns(t), hl_l2("BTC", ns(t), fast=True)) for t in range(0, fast_until, fast_every)
    ]
    recs += [(ns(t), hl_bbo("BTC", ns(t))) for t in bbo_gap]
    recs.sort(key=lambda pair: pair[0])
    return recs


def test_a_bbo_gap_over_a_live_reference_stream_is_not_reported(tmp_path):
    """A quiet top of book is not a hole, and must not reach the issue list.

    26 of these on one thin symbol in half a day is a gate nobody reads, which
    is exactly what the design document's acceptance line forbids.
    """
    d = hl_dir(tmp_path)
    write_gz(d / f"btc_{DAY}.gz", hl_quiet_book())
    out = tmp_path / "r.json"
    code, report = run(d, out=out)
    assert code == 0
    assert report["verdict"] == "green", issues_of(report, "hyperliquid")
    assert "cadence_gap" not in checks_of(report, "hyperliquid")

    # The measurement is still in the JSON — it is only not an issue.
    bbo = report["venues"]["hyperliquid"]["days"][DAY]["symbols"]["btc"]["streams"]["bbo"]
    assert bbo["gap_count"] == 1
    assert bbo["suppressed_gap_count"] == 1
    assert "l2Book_fast" in bbo["gaps"][0]["suppressed_by"]


def test_a_bbo_gap_overlapping_a_gap_in_the_reference_stream_is_reported(tmp_path, capsys):
    """When the book feed went quiet too, the connection was not merely calm."""
    d = hl_dir(tmp_path)
    write_gz(
        d / f"btc_{DAY}.gz",
        [
            (ns(0), hl_trade("BTC", ns(0))),
            (ns(0), hl_l2("BTC", ns(0))),
            (ns(0), hl_l2("BTC", ns(0), fast=True)),
            (ns(1), hl_bbo("BTC", ns(1))),
            (ns(61), hl_bbo("BTC", ns(61))),  # 60s hole ...
            (ns(61), hl_l2("BTC", ns(61), fast=True)),  # ... and the book too
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
    assert gaps[0]["suppressed_by"] is None
    assert gaps[0]["explained_by"] is None

    # The doc's acceptance line: gaps are listed by name, not summarised away.
    text = capsys.readouterr().out
    assert "bbo" in text and "btc" in text and "cadence_gap" in text


def test_a_gap_the_sidecar_accounts_for_is_never_suppressed(tmp_path):
    """A blip shorter than the reference stream's own limit leaves no hole in
    it, so liveness alone would drop a hole the collector itself reported.

    Suppression may only ever remove holes nothing is known about.
    """
    d = hl_dir(tmp_path)
    write_gz(d / f"btc_{DAY}.gz", hl_quiet_book())
    write_meta(
        d,
        "hyperliquid",
        DAY,
        [
            (ns(0), session_start("hyperliquid", ["BTC"])),
            # 1s: under the 5.4s l2Book_fast limit, so the book shows no gap.
            (ns(20), {"_collector": "disconnected", "error": "reset", "connected_for_ms": 20000}),
            (ns(21), {"_collector": "connected", "url": "wss://api.hyperliquid.xyz/ws"}),
        ],
    )
    out = tmp_path / "r.json"
    _, report = run(d, out=out)
    bbo = report["venues"]["hyperliquid"]["days"][DAY]["symbols"]["btc"]["streams"]["bbo"]
    assert bbo["gaps"][0]["suppressed_by"] is None
    assert "disconnected" in bbo["gaps"][0]["explained_by"]
    assert bbo["suppressed_gap_count"] == 0
    assert "cadence_gap" in checks_of(report, "hyperliquid", severity="yellow")


def test_the_reference_falls_back_to_the_slow_book_when_fast_is_not_recorded(tmp_path):
    """`--hl-l2-modes slow` is a legal recording; it still has a liveness feed."""
    d = hl_dir(tmp_path, modes=("slow",))
    write_gz(
        d / f"btc_{DAY}.gz",
        [
            (ns(0), hl_trade("BTC", ns(0))),
            (ns(0), hl_l2("BTC", ns(0))),
            (ns(10), hl_bbo("BTC", ns(10))),
            (ns(35), hl_bbo("BTC", ns(35))),  # 25s > the 14s bbo limit
            (ns(50), hl_l2("BTC", ns(50))),  # 50s <= the 54s slow limit: no hole
        ],
    )
    out = tmp_path / "r.json"
    code, report = run(d, out=out)
    assert code == 0
    assert "cadence_gap" not in checks_of(report, "hyperliquid")
    bbo = report["venues"]["hyperliquid"]["days"][DAY]["symbols"]["btc"]["streams"]["bbo"]
    assert "l2Book_slow" in bbo["gaps"][0]["suppressed_by"]


def test_bbo_keeps_its_absolute_limit_when_no_l2book_cadence_was_recorded(tmp_path):
    """With no book feed there is no second witness, so the limit is all there is."""
    d = hl_dir(tmp_path, modes=("slow",))
    write_gz(
        d / f"btc_{DAY}.gz",
        [
            (ns(0), hl_trade("BTC", ns(0))),
            (ns(1), hl_bbo("BTC", ns(1))),
            (ns(61), hl_bbo("BTC", ns(61))),
        ],
    )
    out = tmp_path / "r.json"
    code, report = run(d, out=out)
    # Red for the missing book, and the bbo hole is still reported alongside it.
    assert code == 1
    assert "missing_required" in checks_of(report, "hyperliquid", severity="red")
    assert "cadence_gap" in checks_of(report, "hyperliquid", severity="yellow")
    bbo = report["venues"]["hyperliquid"]["days"][DAY]["symbols"]["btc"]["streams"]["bbo"]
    assert bbo["gaps"][0]["suppressed_by"] is None


def test_suppressed_gaps_are_not_counted_as_further_gaps(tmp_path):
    """The `N further gap(s)` line must count reportable holes only.

    Counting them all would put the 26 false yellows straight back into the
    report as one summary line, which is the same noise with a smaller font.
    """
    d = hl_dir(tmp_path)
    recs = [(ns(0), hl_trade("BTC", ns(0))), (ns(0), hl_l2("BTC", ns(0)))]
    # 0..900 in minutes: both feeds hole together, so all 15 bbo holes are real.
    for t in range(0, 901, 60):
        recs.append((ns(t), hl_l2("BTC", ns(t), fast=True)))
        recs.append((ns(t), hl_bbo("BTC", ns(t))))
    # Then the book runs steadily while bbo goes quiet twice: not holes. Both
    # quiet stretches stay under MAX_SUPPRESSED_GAP_FACTOR x the 14s bbo limit,
    # which is what makes them suppressible at all.
    for t in range(905, 1101, 5):
        recs.append((ns(t), hl_l2("BTC", ns(t), fast=True)))
    recs += [(ns(905), hl_bbo("BTC", ns(905))), (ns(1000), hl_bbo("BTC", ns(1000))),
             (ns(1100), hl_bbo("BTC", ns(1100)))]
    recs.sort(key=lambda pair: pair[0])
    write_gz(d / f"btc_{DAY}.gz", recs)

    out = tmp_path / "r.json"
    _, report = run(d, out=out)
    bbo = report["venues"]["hyperliquid"]["days"][DAY]["symbols"]["btc"]["streams"]["bbo"]
    assert bbo["gap_count"] == 17
    assert bbo["suppressed_gap_count"] == 2

    bbo_issues = [
        i for i in issues_of(report, "hyperliquid")
        if i["check"] == "cadence_gap" and i["detail"].startswith("btc/bbo: ")
    ]
    named = [i for i in bbo_issues if "further gap" not in i["detail"]]
    assert len(named) == qr.MAX_GAP_ISSUES
    further = [i for i in bbo_issues if "further gap" in i["detail"]]
    assert len(further) == 1
    assert "5 further gap(s)" in further[0]["detail"], further[0]["detail"]


def test_a_truncated_reference_gap_list_cannot_prove_liveness(tmp_path, monkeypatch):
    """Fail closed: gaps the recorder stopped keeping cannot be shown not to overlap.

    `MAX_GAPS_RECORDED` is shrunk rather than writing thousands of frames; the
    branch under test is `gap_count > len(gaps)`, not the constant's value.
    """
    monkeypatch.setattr(qr, "MAX_GAPS_RECORDED", 1)
    d = hl_dir(tmp_path)
    recs = [
        (ns(0), hl_trade("BTC", ns(0))),
        (ns(0), hl_l2("BTC", ns(0))),
        (ns(0), hl_l2("BTC", ns(0), fast=True)),
        (ns(10), hl_l2("BTC", ns(10), fast=True)),  # gap 1, recorded
        (ns(20), hl_l2("BTC", ns(20), fast=True)),  # gap 2, dropped by the cap
    ]
    # The reference then runs steadily right across the bbo hole, so it brackets
    # it and shows no overlapping hole of its own: the incomplete list is the
    # only thing left standing between the hole and suppression.
    recs += [(ns(t), hl_l2("BTC", ns(t), fast=True)) for t in range(21, 161)]
    recs += [(ns(100), hl_bbo("BTC", ns(100))), (ns(140), hl_bbo("BTC", ns(140)))]
    recs.sort(key=lambda pair: pair[0])
    write_gz(d / f"btc_{DAY}.gz", recs)
    out = tmp_path / "r.json"
    _, report = run(d, out=out)
    bbo = report["venues"]["hyperliquid"]["days"][DAY]["symbols"]["btc"]["streams"]["bbo"]
    assert bbo["gaps"][0]["suppressed_by"] is None
    assert bbo["suppressed_gap_count"] == 0
    assert "cadence_gap" in checks_of(report, "hyperliquid", severity="yellow")


def test_a_reference_that_stopped_before_the_hole_cannot_prove_liveness(tmp_path):
    """A dead reference is not a live one. `count > 0` cannot tell them apart.

    A stream that simply stops leaves NO trailing gap — `StreamStat.observe`
    only measures between two frames — so "the reference has no overlapping
    hole" is satisfied vacuously by a reference that was not running at all.
    This is the shape `watchdog.rs` already names as the case it cannot catch:
    one Hyperliquid cadence stopping while the others continue.
    """
    d = hl_dir(tmp_path)
    recs = [(ns(0), hl_trade("BTC", ns(0))), (ns(0), hl_l2("BTC", ns(0)))]
    # The reference dies at t=10 and is never heard from again.
    recs += [(ns(t), hl_l2("BTC", ns(t), fast=True)) for t in range(0, 11)]
    # bbo then goes silent for 100s, which nothing witnessed.
    recs += [(ns(t), hl_bbo("BTC", ns(t))) for t in (0, 5, 10, 110, 115)]
    recs.sort(key=lambda pair: pair[0])
    write_gz(d / f"btc_{DAY}.gz", recs)

    out = tmp_path / "r.json"
    _, report = run(d, out=out)
    bbo = report["venues"]["hyperliquid"]["days"][DAY]["symbols"]["btc"]["streams"]["bbo"]
    assert bbo["gaps"][0]["suppressed_by"] is None, (
        "l2Book_fast last wrote at t=10 and was dead for every second of the "
        "hole it was allowed to excuse"
    )
    assert bbo["suppressed_gap_count"] == 0
    assert "cadence_gap" in checks_of(report, "hyperliquid", severity="yellow")


def test_a_reference_that_started_after_the_hole_cannot_prove_liveness(tmp_path):
    """The mirror image: a reference whose first frame is inside or after the
    hole says nothing about the time before it."""
    d = hl_dir(tmp_path)
    recs = [(ns(0), hl_trade("BTC", ns(0))), (ns(0), hl_l2("BTC", ns(0)))]
    recs += [(ns(t), hl_l2("BTC", ns(t), fast=True)) for t in range(100, 121)]
    recs += [(ns(t), hl_bbo("BTC", ns(t))) for t in (0, 5, 100, 105)]
    recs.sort(key=lambda pair: pair[0])
    write_gz(d / f"btc_{DAY}.gz", recs)

    out = tmp_path / "r.json"
    _, report = run(d, out=out)
    bbo = report["venues"]["hyperliquid"]["days"][DAY]["symbols"]["btc"]["streams"]["bbo"]
    assert bbo["gaps"][0]["suppressed_by"] is None
    assert "cadence_gap" in checks_of(report, "hyperliquid", severity="yellow")


def test_a_reference_seen_only_once_all_day_cannot_prove_liveness(tmp_path):
    """One frame is a recording, not a witness: it brackets nothing."""
    d = hl_dir(tmp_path)
    write_gz(
        d / f"btc_{DAY}.gz",
        [
            (ns(0), hl_trade("BTC", ns(0))),
            (ns(0), hl_l2("BTC", ns(0))),
            (ns(0), hl_l2("BTC", ns(0), fast=True)),  # the only one, all day
            (ns(10), hl_bbo("BTC", ns(10))),
            (ns(70), hl_bbo("BTC", ns(70))),
        ],
    )
    out = tmp_path / "r.json"
    _, report = run(d, out=out)
    bbo = report["venues"]["hyperliquid"]["days"][DAY]["symbols"]["btc"]["streams"]["bbo"]
    assert bbo["gaps"][0]["suppressed_by"] is None
    assert "cadence_gap" in checks_of(report, "hyperliquid", severity="yellow")


def test_a_dead_preferred_reference_does_not_shadow_a_live_fallback(tmp_path):
    """Selection is per gap, not per stream, or the tuple is never fallen through.

    `l2Book_fast` dies silently at t=100 — nothing in `_meta`, the socket is up.
    A real 60s outage then silences `bbo` AND `l2Book_slow`. Picking the first
    reference merely *present* would let the dead one excuse the hole its live
    sibling reported.
    """
    d = hl_dir(tmp_path)
    recs = [(ns(0), hl_trade("BTC", ns(0)))]
    recs += [(ns(t), hl_l2("BTC", ns(t), fast=True)) for t in range(0, 101)]
    alive = list(range(0, 106, 5)) + list(range(165, 300, 5))
    recs += [(ns(t), hl_l2("BTC", ns(t))) for t in alive]
    recs += [(ns(t), hl_bbo("BTC", ns(t))) for t in alive]
    recs.sort(key=lambda pair: pair[0])
    write_gz(d / f"btc_{DAY}.gz", recs)

    out = tmp_path / "r.json"
    _, report = run(d, out=out)
    streams = report["venues"]["hyperliquid"]["days"][DAY]["symbols"]["btc"]["streams"]
    assert streams["l2Book_slow"]["gap_count"] == 1, "the fallback saw the outage"
    assert streams["bbo"]["gaps"][0]["suppressed_by"] is None
    assert streams["bbo"]["suppressed_gap_count"] == 0


def test_a_multi_hour_hole_is_never_suppressed(tmp_path):
    """Past some size "the top of book did not move" stops being a hypothesis.

    A silently dropped per-channel subscription — socket alive, one channel
    dead, the Bybit `orderbook.500` precedent in AGENTS.md §4.1 — has exactly
    this signature, and it is the failure the report exists to catch. The
    reference proves the *socket* was up, not that `bbo` was.
    """
    d = hl_dir(tmp_path)
    span = 6 * 3600 + 60
    recs = [(ns(t), hl_l2("BTC", ns(t), fast=True)) for t in range(0, span, 5)]
    recs += [(ns(t), hl_l2("BTC", ns(t))) for t in range(0, span, 5)]
    recs += [(ns(t), hl_trade("BTC", ns(t))) for t in range(0, span, 5)]
    recs += [(ns(t), hl_bbo("BTC", ns(t))) for t in (0, 5, 6 * 3600 + 5, 6 * 3600 + 10)]
    recs.sort(key=lambda pair: pair[0])
    write_gz(d / f"btc_{DAY}.gz", recs)

    out = tmp_path / "r.json"
    _, report = run(d, out=out)
    bbo = report["venues"]["hyperliquid"]["days"][DAY]["symbols"]["btc"]["streams"]["bbo"]
    assert bbo["gap_count"] == 1
    assert bbo["suppressed_gap_count"] == 0
    assert bbo["gaps"][0]["suppressed_by"] is None
    assert "cadence_gap" in checks_of(report, "hyperliquid", severity="yellow")


def test_the_suppression_ceiling_is_pinned_to_the_nanosecond(tmp_path):
    """The false positives it exists for were 14-37s; the ceiling is 10x the
    channel's own limit. Raising it must break a test, not pass silently."""
    assert qr.MAX_SUPPRESSED_GAP_FACTOR == 10
    ceiling = qr.MAX_GAP_NS[(qr.HYPERLIQUID, "bbo")] * qr.MAX_SUPPRESSED_GAP_FACTOR
    assert ceiling == 140 * SEC

    for name, gap_ns, suppressed in (("at", ceiling, True), ("over", ceiling + 1, False)):
        d = tmp_path / f"hl-ceiling-{name}"
        d.mkdir()
        end = 10 + gap_ns // SEC + 2
        recs = [(ns(0), hl_trade("BTC", ns(0))), (ns(0), hl_l2("BTC", ns(0)))]
        recs += [(ns(t), hl_l2("BTC", ns(t), fast=True)) for t in range(0, end)]
        recs += [(ns(10), hl_bbo("BTC", ns(10))), (ns(10) + gap_ns, hl_bbo("BTC", ns(10)))]
        recs.sort(key=lambda pair: pair[0])
        write_gz(d / f"btc_{DAY}.gz", recs)
        write_meta(d, "hyperliquid", DAY, [(ns(0), session_start("hyperliquid", ["BTC"]))])
        out = tmp_path / f"r-{name}.json"
        _, report = run(d, out=out)
        bbo = report["venues"]["hyperliquid"]["days"][DAY]["symbols"]["btc"]["streams"]["bbo"]
        assert (bbo["gaps"][0]["suppressed_by"] is not None) is suppressed, (name, gap_ns)


def test_a_gap_spanned_by_a_disconnect_is_annotated_as_explained(tmp_path):
    d = hl_dir(tmp_path)
    write_gz(
        d / f"btc_{DAY}.gz",
        [
            (ns(0), hl_trade("BTC", ns(0))),
            (ns(0), hl_l2("BTC", ns(0))),
            (ns(0), hl_l2("BTC", ns(0), fast=True)),
            (ns(1), hl_bbo("BTC", ns(1))),
            # A disconnect silences the whole socket, book feed included — which
            # is also what keeps the bbo hole reportable at all.
            (ns(61), hl_bbo("BTC", ns(61))),
            (ns(61), hl_l2("BTC", ns(61), fast=True)),
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


# --------------------------------------------------------------------------
# 8. only lifecycle records explain a gap
# --------------------------------------------------------------------------


def hl_gapped_day(tmp_path, meta_extra):
    """A reportable 60s hole (both feeds silent) plus the given sidecar records."""
    d = hl_dir(tmp_path)
    write_gz(
        d / f"btc_{DAY}.gz",
        [
            (ns(0), hl_trade("BTC", ns(0))),
            (ns(0), hl_l2("BTC", ns(0))),
            (ns(0), hl_l2("BTC", ns(0), fast=True)),
            (ns(1), hl_bbo("BTC", ns(1))),
            (ns(61), hl_bbo("BTC", ns(61))),
            (ns(61), hl_l2("BTC", ns(61), fast=True)),
        ],
    )
    write_meta(
        d,
        "hyperliquid",
        DAY,
        [(ns(0), session_start("hyperliquid", ["BTC"])), *meta_extra],
    )
    return d


@pytest.mark.parametrize(
    "record",
    [
        {"_collector": "disk", "free_bytes": 812_345_678_901, "path": "/data/hyperliquid"},
        {"_collector": "universe", "symbols": [{"wire": "BTC", "szDecimals": 5}]},
    ],
)
def test_a_gauge_record_inside_a_gap_does_not_explain_it(tmp_path, record):
    """The minutely disk gauge says nothing about why data stopped arriving.

    It is written on a timer (`main.rs`, `disk_check.tick()`), so one lands
    inside any hole longer than a minute regardless of cause. Crediting it sends
    an investigation after a number that was going to be written anyway.
    """
    d = hl_gapped_day(tmp_path, [(ns(30), record)])
    out = tmp_path / "r.json"
    _, report = run(d, out=out)
    gaps = report["venues"]["hyperliquid"]["days"][DAY]["symbols"]["btc"]["streams"]["bbo"]["gaps"]
    assert gaps[0]["explained_by"] is None
    issue = next(i for i in issues_of(report, "hyperliquid") if i["check"] == "cadence_gap")
    assert "unexplained by _meta" in issue["detail"]


def test_the_more_suffix_counts_lifecycle_records_only(tmp_path):
    """`(+N more)` is how many other records bear on the hole, not how many
    timer ticks happened to land in it."""
    d = hl_gapped_day(
        tmp_path,
        [
            (ns(20), {"_collector": "disconnected", "error": "reset", "connected_for_ms": 20000}),
            (ns(25), {"_collector": "connected", "url": "wss://api.hyperliquid.xyz/ws"}),
            (ns(30), {"_collector": "disk", "free_bytes": 1, "path": "/data"}),
            (ns(40), {"_collector": "disk", "free_bytes": 1, "path": "/data"}),
        ],
    )
    out = tmp_path / "r.json"
    _, report = run(d, out=out)
    gaps = report["venues"]["hyperliquid"]["days"][DAY]["symbols"]["btc"]["streams"]["bbo"]["gaps"]
    assert gaps[0]["explained_by"].startswith("disconnected at ")
    assert gaps[0]["explained_by"].endswith("(+1 more)"), gaps[0]["explained_by"]


def test_every_explanatory_record_is_a_lifecycle_record():
    """The gauge records are the ones that must never appear as an explanation."""
    assert "disk" not in qr._EXPLANATORY
    assert "universe" not in qr._EXPLANATORY
    for wanted in ("session_start", "connected", "disconnected", "dial_failed",
                   "stream_ended", "subscribe", "probe_failed"):
        assert wanted in qr._EXPLANATORY


def test_an_unreachable_venue_explains_a_hole_under_its_own_name():
    """`probe_failed` is not `symbol_check_failed`, and a hole must not say it is.

    Lighter's `/stream` sits behind a jurisdiction check that refuses the
    WebSocket upgrade while REST keeps answering, so every symbol resolved and
    the recording is empty anyway. Borrowing the nearest existing name would
    annotate the hole "explained by symbol_check_failed" and send whoever reads
    it to check the symbol list.
    """
    assert "probe_failed" in qr._EXPLANATORY
    gap = qr.Gap(start_ts=ns(10), end_ts=ns(50), duration_ns=ns(40) - ns(0))
    explained = qr.explain_gap(gap, [(ns(20), "probe_failed")])
    assert explained is not None and explained.startswith("probe_failed at "), explained


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
# 6. monotonicity: per stream, with a bounded cross-stream tolerance
# --------------------------------------------------------------------------


def test_local_ts_going_backwards_within_one_stream_is_red(tmp_path):
    """One stream has one producer stamping in receive order, so this cannot
    happen without a clock step or two recordings in one file."""
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
    symbol = report["venues"]["hyperliquid"]["days"][DAY]["symbols"]["btc"]
    violation = symbol["monotonic_violation"]
    assert violation["stream"] == "bbo"
    assert violation["previous_local_ts"] == ns(10)
    assert violation["local_ts"] == ns(5)
    assert violation["violations"] == 1
    # An in-stream step is never also filed as an interleave.
    assert symbol["interleave_inversion"] is None
    assert symbol["interleave_excess"] is None


def test_a_sub_millisecond_step_backwards_within_one_stream_is_still_red(tmp_path):
    """The cross-stream tolerance must not leak into the per-stream check: one
    producer going backwards by a microsecond is a defect at any size."""
    d = hl_dir(tmp_path)
    write_gz(
        d / f"btc_{DAY}.gz",
        [
            (ns(0), hl_trade("BTC", ns(0))),
            (ns(0), hl_l2("BTC", ns(0))),
            (ns(0), hl_l2("BTC", ns(0), fast=True)),
            (ns(10, nanos=1_000), hl_bbo("BTC", ns(10))),
            (ns(10), hl_bbo("BTC", ns(10))),  # 1us backwards
        ],
    )
    out = tmp_path / "r.json"
    code, report = run(d, out=out)
    assert code == 1
    assert "monotonicity" in checks_of(report, "hyperliquid", severity="red")
    assert "interleave_inversion" not in checks_of(report, "hyperliquid")


def um_interleaved(delta_ns):
    """Incident B: the REST depth snapshot lands before WS frames stamped earlier.

    Line ordering is the writer's; the stamps are each producer's own receive
    moment. The snapshot skips the WS hop the market-data frames queue through,
    so it can overtake them by the difference between the two paths.
    """
    early = ns(0, nanos=100_000_000)
    book = ns(0, nanos=164_558_903)
    snap = book + delta_ns
    return [
        (early, um_book_ticker("BTCUSDT", early)),
        (early, um_trade("BTCUSDT", early)),
        (early, um_depth("BTCUSDT", early, u=100, pu=99)),
        (snap, um_depth_snapshot("BTCUSDT", snap)),
        (book, um_book_ticker("BTCUSDT", book)),  # <- written after, stamped before
        (book + 1_000, um_book_ticker("BTCUSDT", book + 1_000)),
    ]


def test_a_cross_stream_inversion_within_the_bound_is_yellow_and_named(tmp_path):
    """The 134us measured on ethusdt, 2026-07-26 — one per ~5M lines, only at a
    REST refetch. Both stamps are honest; the inversion is in the interleaving."""
    d = tmp_path / "um-interleave"
    d.mkdir()
    write_gz(d / f"btcusdt_{DAY}.gz", um_interleaved(134_021))
    write_meta(d, "binancefuturesum", DAY,
               [(ns(0), session_start("binancefuturesum", ["BTCUSDT"]))])

    out = tmp_path / "r.json"
    code, report = run(d, out=out)
    assert code == 0, issues_of(report, "binancefuturesum")
    assert "monotonicity" not in checks_of(report, "binancefuturesum")
    assert "interleave_inversion" in checks_of(report, "binancefuturesum", severity="yellow")

    symbol = report["venues"]["binancefuturesum"]["days"][DAY]["symbols"]["btcusdt"]
    assert symbol["monotonic_violation"] is None
    assert symbol["interleave_excess"] is None
    record = symbol["interleave_inversion"]
    assert record["previous_stream"] == "depthSnapshot"
    assert record["stream"] == "bookTicker"
    assert record["violations"] == 1
    assert record["max_delta_ns"] == 134_021
    # The two stamps are the record's own, so the delta is recoverable from it
    # without a field that could disagree with `max_delta_ns`.
    assert record["previous_local_ts"] - record["local_ts"] == 134_021

    detail = next(
        i for i in issues_of(report, "binancefuturesum") if i["check"] == "interleave_inversion"
    )["detail"]
    # The mechanism, not just the symptom.
    assert "depthSnapshot" in detail and "bookTicker" in detail
    assert "producer" in detail
    assert "REST" in detail


@pytest.mark.parametrize("mechanism", sorted(qr._SECOND_PRODUCER))
def test_every_mechanism_sentence_is_a_finished_sentence(mechanism):
    """The text an operator reaches for on a red day must not stop mid-clause.

    It is interpolated as `...; {mechanism}. This is {verdict}`, and the
    depthSnapshot entry used to end on a dangling "queue in".
    """
    text = qr._SECOND_PRODUCER[mechanism].mechanism
    assert not text.rstrip().endswith((" in", " the", " of", " through", ",")), text
    # Rendered exactly as the issue renders it, so a trailing preposition shows.
    assert "queue in." not in f"{text}. This is"


def test_a_snapshot_overtake_the_full_depth_of_the_socket_hop_is_not_red(tmp_path):
    """Incident A and incident B are the same day.

    The socket hop holds WS frames the REST snapshot skips, so the largest
    honest overtake is that hop's whole occupancy: `WS_QUEUE_CAPACITY` /
    `burst::PEAK_MSG_PER_S` = 16384 / 20 000 = 819.2ms. A burst is also what
    breaks the `pu` chain that triggers the refetch, so the deep hop and the
    snapshot co-occur by construction — a bound below the hop's depth goes red
    on precisely the days whose data is most wanted, and a red day is a hard
    build refusal in `build_dataset.py`.
    """
    d = tmp_path / "um-socket-hop"
    d.mkdir()
    write_gz(d / f"btcusdt_{DAY}.gz", um_interleaved(qr.SOCKET_HOP_NS))
    write_meta(d, "binancefuturesum", DAY,
               [(ns(0), session_start("binancefuturesum", ["BTCUSDT"]))])

    out = tmp_path / "r.json"
    code, report = run(d, out=out)
    assert code == 0, issues_of(report, "binancefuturesum")
    assert "interleave_inversion" in checks_of(report, "binancefuturesum", severity="yellow")


def test_a_cross_stream_inversion_beyond_the_bound_is_red(tmp_path):
    """Past the interleave bound the explanation stops being credible: not even
    a full socket hop reorders two producers by that much."""
    d = tmp_path / "um-interleave-wide"
    d.mkdir()
    write_gz(d / f"btcusdt_{DAY}.gz", um_interleaved(2_000 * MS))
    write_meta(d, "binancefuturesum", DAY,
               [(ns(0), session_start("binancefuturesum", ["BTCUSDT"]))])

    out = tmp_path / "r.json"
    code, report = run(d, out=out)
    assert code == 1
    assert "interleave_excess" in checks_of(report, "binancefuturesum", severity="red")
    symbol = report["venues"]["binancefuturesum"]["days"][DAY]["symbols"]["btcusdt"]
    assert symbol["interleave_inversion"] is None
    assert symbol["interleave_excess"]["max_delta_ns"] == 2_000 * MS


@pytest.mark.parametrize(
    "delta_ns,expected_code,expected_check",
    [
        (1_000 * MS, 0, "interleave_inversion"),
        (1_000 * MS + 1, 1, "interleave_excess"),
    ],
)
def test_the_interleave_bound_is_pinned_to_the_nanosecond(
    tmp_path, delta_ns, expected_code, expected_check
):
    """Widening the tolerance must break a test, not pass silently.

    It widened once, on 2026-07-29, and only because the socket hop it is
    derived from did — see `CROSS_STREAM_TOLERANCE_NS`.
    """
    assert qr.CROSS_STREAM_TOLERANCE_NS == 1_000 * MS
    d = tmp_path / f"um-bound-{delta_ns}"
    d.mkdir()
    write_gz(d / f"btcusdt_{DAY}.gz", um_interleaved(delta_ns))
    write_meta(d, "binancefuturesum", DAY,
               [(ns(0), session_start("binancefuturesum", ["BTCUSDT"]))])
    out = tmp_path / f"r-{delta_ns}.json"
    code, report = run(d, out=out)
    assert code == expected_code
    assert expected_check in checks_of(report, "binancefuturesum")


def test_the_interleave_bound_still_covers_the_socket_hop_it_is_derived_from(tmp_path):
    """The two halves of this must not drift apart.

    The bound is the socket hop's occupancy at the measured burst rate, and both
    numbers live in `collector/src/queue.rs`. Reading them here is what makes
    raising `WS_QUEUE_CAPACITY` re-check the gate instead of silently turning
    the burst days red.
    """
    source = (Path(__file__).resolve().parent.parent / "src" / "queue.rs").read_text()

    def rust_const(name):
        match = re.search(rf"const {name}: usize = ([0-9_]+);", source)
        assert match, f"{name} is no longer a usize constant of collector/src/queue.rs"
        return int(match.group(1).replace("_", ""))

    assert qr.WS_QUEUE_CAPACITY == rust_const("WS_QUEUE_CAPACITY")
    assert qr.PEAK_MSG_PER_S == rust_const("PEAK_MSG_PER_S")
    assert qr.SOCKET_HOP_NS == qr.WS_QUEUE_CAPACITY * SEC // qr.PEAK_MSG_PER_S
    assert qr.CROSS_STREAM_TOLERANCE_NS >= qr.SOCKET_HOP_NS, (
        "a WS frame can sit in the socket hop for its whole depth while the REST "
        "snapshot that skips the hop goes straight to the writer; a bound under "
        "that reports the collector's own design as corruption"
    )


def test_the_pollers_ceiling_is_not_the_socket_hops_bound(tmp_path):
    """Equal today, and they have to be two numbers.

    `CROSS_STREAM_TOLERANCE_NS` is the socket hop's occupancy and follows
    `WS_QUEUE_CAPACITY`: it has moved once already, 250ms -> 1s, when the hop
    went 4096 -> 16384, and the test above exists to make the next capacity
    change move it again. A poll written behind a frame stamped later never
    crossed that hop — its ceiling answers a different question, off a different
    measurement (`POLLER_HOP_CEILING_NS`). Written as one constant, the next
    capacity bump would quietly widen a lookahead detector by the same factor,
    with nothing failing and nobody deciding it.
    """
    source = Path(qr.__file__).read_text()
    assert re.search(r"^POLLER_HOP_CEILING_NS = [0-9_ *]+$", source, re.M), (
        "the poller's ceiling must be its own literal, not an expression over "
        "the socket hop's bound or capacity"
    )
    # And it really is the number the poll-late pair is measured against: a
    # ceiling that nothing reads would drift without anyone noticing.
    d = tmp_path / "um-poll-late-ceiling"
    d.mkdir()
    write_gz(d / f"btcusdt_{DAY}.gz", um_poll_after_newer_ticks(qr.POLLER_HOP_CEILING_NS + 1))
    write_meta(
        d, "binancefuturesum", DAY, [(ns(0), session_start("binancefuturesum", ["BTCUSDT"]))]
    )
    _, report = run(d, out=tmp_path / "r.json")
    symbol = report["venues"]["binancefuturesum"]["days"][DAY]["symbols"]["btcusdt"]
    assert symbol["interleave_excess"]["stream"] == "premiumIndex"


@pytest.mark.parametrize("delta_ns", [1, 5 * MS, 200 * MS, 2 * SEC])
def test_a_cross_stream_inversion_with_no_second_producer_is_red_at_any_size(
    tmp_path, delta_ns
):
    """The tolerance is a property of a mechanism, not of a duration.

    Hyperliquid has one WS reader stamping and routing every frame of a symbol
    file (`hyperliquid/mod.rs`), so write order IS receive order and there is
    nothing for the tolerance to excuse. Applying it venue-agnostically
    downgraded a real defect to yellow and printed a self-contradicting reason.

    The 2s case is here because of what the poller class became on 2026-07-30:
    an inversion of unbounded size is yellow when the row ahead of it was a poll
    that crossed a hand-off of its own, and on a venue with no such producer the
    same size must still be red. Hyperliquid, Bybit and Lighter have no second
    producer at all, so nothing about that change — nor about the shared-chain
    cursor it needed — may reach them.
    """
    d = hl_dir(tmp_path, name=f"hl-noproducer-{delta_ns}")
    write_gz(
        d / f"btc_{DAY}.gz",
        [
            (ns(0), hl_trade("BTC", ns(0))),
            (ns(0), hl_l2("BTC", ns(0))),
            (ns(1), hl_l2("BTC", ns(1), fast=True)),
            (ns(1) - delta_ns, hl_bbo("BTC", ns(1) - delta_ns)),
        ],
    )
    out = tmp_path / f"r-{delta_ns}.json"
    code, report = run(d, out=out)
    assert code == 1
    assert "interleave_excess" in checks_of(report, "hyperliquid", severity="red")
    detail = next(
        i for i in issues_of(report, "hyperliquid") if i["check"] == "interleave_excess"
    )["detail"]
    assert "no second producer" in detail
    # The old text said "within the bound, so nothing is missing" in the same
    # breath as reporting a defect.
    assert "nothing is missing" not in detail


# --------------------------------------------------------------------------
# Fan-out venues: one socket per channel, so every stream is its own producer
# --------------------------------------------------------------------------


@pytest.mark.parametrize(
    "first,second,delta_ns",
    [
        # The two findings that reddened the daily gate on 20260804 and paged.
        ("orderbook", "mark", 15_153),  # btc-usd, 4 occurrences, worst 15.153us
        ("trades", "mark", 489),  # sol-usd, 2 occurrences, worst 489ns
    ],
)
def test_an_extended_cross_stream_micro_inversion_is_the_race_it_is(
    tmp_path, first, second, delta_ns
):
    """A false red that paged daily, because the model called Extended
    single-reader when the backend is nothing of the kind.

    Extended dials one WebSocket per channel and
    `keep_connections` (`collector/src/extended/http.rs`) spawns one task per
    URL. Each stamps its own `Utc::now()` at its own read and races the others
    into the one shared socket hop, so two streams of a symbol file are two
    concurrent producers — the second-producer case, not the single-reader one.
    Both magnitudes here are the real ones, off the recording that paged: they
    are the width of a scheduler slice, five orders of magnitude inside the
    bound, and calling them corruption refuses a build over nothing.
    """
    d = extended_inverted(tmp_path, f"ext-{first}-{second}", first, second, delta_ns)
    code, report = run(d, out=tmp_path / f"r-{first}-{second}.json")
    assert code == 0, issues_of(report, "extended")
    assert "interleave_excess" not in checks_of(report, "extended")
    assert "interleave_inversion" in checks_of(report, "extended", severity="yellow")
    symbol = report["venues"]["extended"]["days"][DAY]["symbols"]["btc-usd"]
    assert symbol["interleave_inversion"]["max_delta_ns"] == delta_ns
    assert symbol["interleave_inversion"]["previous_stream"] == first
    assert symbol["interleave_inversion"]["stream"] == second
    assert symbol["interleave_excess"] is None

    # The explanation has to be the same decision as the finding: the venue's
    # own mechanism, not the "no second producer" sentence this used to print,
    # and not the snapshot pair's either — that one names the socket hop as
    # "the only queue these two do not share", which here is false twice over.
    detail = next(
        i for i in issues_of(report, "extended") if i["check"] == "interleave_inversion"
    )["detail"]
    assert "no second producer" not in detail
    assert "keep_connection_one per URL" in detail, "the mechanism, from the Rust"
    assert "two socket tasks" in detail, "the verdict, in the venue's own terms"
    assert "the only queue these two do not share" not in detail
    assert "nothing is missing" in detail


def test_an_extended_cross_stream_inversion_past_the_bound_is_still_red(tmp_path):
    """The exemption is bounded, so the gate is not blinded.

    A fan-out venue's two rows still meet in the shared socket hop and the
    shared writer hop, both FIFOs, so the only thing that can separate their
    stamps is the moment between one task's `Utc::now()` and its `send`. Past
    `CROSS_STREAM_TOLERANCE_NS` that stops being a scheduler slice and becomes
    the same two hypotheses every other venue's red carries.
    """
    delta_ns = 1_200 * MS
    assert delta_ns > qr.CROSS_STREAM_TOLERANCE_NS
    d = extended_inverted(tmp_path, "ext-over-bound", "orderbook", "mark", delta_ns)
    code, report = run(d, out=tmp_path / "r.json")
    assert code == 1
    assert "interleave_excess" in checks_of(report, "extended", severity="red")
    symbol = report["venues"]["extended"]["days"][DAY]["symbols"]["btc-usd"]
    assert symbol["interleave_excess"]["max_delta_ns"] == delta_ns
    detail = next(
        i for i in issues_of(report, "extended") if i["check"] == "interleave_excess"
    )["detail"]
    assert "nothing is missing" not in detail
    # And it says the true thing about this venue rather than the snapshot
    # pair's: a deep socket hop delays BOTH of these rows, so "check _meta for a
    # burst" is not a lead here, and the hop is not a queue they fail to share.
    assert "the only queue these two do not share" not in detail
    assert "both FIFOs" in detail
    assert "two recordings" in detail


@pytest.mark.parametrize("delta_ns", [1, 15_153, 2 * SEC])
def test_an_extended_inversion_within_one_stream_is_red_at_any_size(tmp_path, delta_ns):
    """The fan-out exemption is about two sockets; one socket keeps no tolerance.

    Each Extended channel is one task reading, stamping and enqueueing in that
    order, so within a stream write order IS receive order — the single-reader
    argument survives intact per channel, and a step backwards there is a clock
    or two recordings whatever its size. `interleave_kind` is never consulted
    for it: `scan_symbol_file` files a same-stream step under
    `monotonic_violation`, and that path is untouched by this change.
    """
    d = tmp_path / f"ext-within-{delta_ns}"
    d.mkdir()
    base = ns(1)
    write_gz(
        d / f"btc-usd_{DAY}.gz",
        [
            (ns(0), extended_book("BTC-USD", ns(0), 1, kind="SNAPSHOT")),
            (base, extended_mark("BTC-USD", base, 2)),
            (base - delta_ns, extended_mark("BTC-USD", base - delta_ns, 3)),
        ],
    )
    write_meta(d, "extended", DAY, [(ns(0), session_start("extended", ["BTC-USD"]))])
    code, report = run(d, out=tmp_path / f"r-{delta_ns}.json")
    assert code == 1
    assert "monotonicity" in checks_of(report, "extended", severity="red")
    symbol = report["venues"]["extended"]["days"][DAY]["symbols"]["btc-usd"]
    assert symbol["monotonic_violation"]["max_delta_ns"] == delta_ns
    assert symbol["interleave_inversion"] is None
    assert symbol["interleave_excess"] is None


@pytest.mark.parametrize(
    "first,second", [("bbo", "trades"), ("book_snapshot", "book_interactive")]
)
def test_a_paradex_cross_stream_inversion_stays_red_at_a_nanosecond(
    tmp_path, first, second
):
    """Paradex is genuinely single-reader and must not ride Extended's exemption.

    `collector/src/paradex/mod.rs` makes ONE `keep_connection(CHANNELS.to_vec(),
    markets, ws_tx)` to one `WS_URL`, every channel multiplexed on that one
    socket, so one reader stamps and queues every frame of a symbol file and
    write order IS receive order. The venue passed the gate on the same day
    Extended reddened it; a per-venue exemption written as a per-family or
    per-"new venue" one would have taken this red with it.
    """
    d = tmp_path / f"pdx-{first}-{second}"
    d.mkdir()
    make = {
        "bbo": lambda ts, seq: paradex_bbo(MARKET, ts, seq),
        "trades": lambda ts, seq: paradex_trade(MARKET, ts, seq),
        "book_snapshot": lambda ts, seq: paradex_book(MARKET, ts, seq, feed="snapshot"),
        "book_interactive": lambda ts, seq: paradex_book(
            MARKET, ts, seq, feed="interactive"
        ),
    }
    base = ns(1)
    write_gz(
        d / f"{MARKET.lower()}_{DAY}.gz",
        [
            (ns(0), paradex_book(MARKET, ns(0), 1, feed="snapshot")),
            (base, make[first](base, 2)),
            (base - 1, make[second](base - 1, 3)),
        ],
    )
    write_meta(d, "paradex", DAY, [(ns(0), session_start("paradex", [MARKET]))])
    code, report = run(d, out=tmp_path / f"r-{first}-{second}.json")
    assert code == 1
    assert "interleave_excess" in checks_of(report, "paradex", severity="red")
    detail = next(
        i for i in issues_of(report, "paradex") if i["check"] == "interleave_excess"
    )["detail"]
    assert "no second producer" in detail


@pytest.mark.parametrize(
    "exchange",
    ["bybit", "hyperliquid", "lighter", "binance", "binancefuturesum",
     "binancefuturescm", "paradex", "aster"],
)
def test_extended_is_the_only_venue_the_fan_out_exemption_reaches(exchange):
    """The set is named, closed, and its members are the ones with the shape.

    Written against every venue the report knows rather than against Extended
    alone: the failure this guards is a predicate that widens — an exemption
    keyed on "not Binance", on a family, or on "the venues added recently" —
    and only a test over the whole list can see that.
    """
    assert qr.fans_out_per_channel(exchange) is False
    assert qr.interleave_kind(exchange, "orderbook", "mark", 1) == qr.INTERLEAVE_EXCESS
    assert qr.second_producer_of(exchange, "orderbook", "mark") is None

    assert qr.fans_out_per_channel("extended") is True
    assert (
        qr.interleave_kind("extended", "orderbook", "mark", 1)
        == qr.INTERLEAVE_INVERSION
    )
    assert (
        qr.interleave_kind(
            "extended", "orderbook", "mark", qr.CROSS_STREAM_TOLERANCE_NS
        )
        == qr.INTERLEAVE_INVERSION
    )
    assert (
        qr.interleave_kind(
            "extended", "orderbook", "mark", qr.CROSS_STREAM_TOLERANCE_NS + 1
        )
        == qr.INTERLEAVE_EXCESS
    )


def test_no_fan_out_venue_has_a_stream_with_a_hand_off_of_its_own():
    """The invariant that keeps one binding point enough. Fails when it lapses.

    `scan_symbol_file` has two cursors, and which one catches an out-of-order
    pair depends on whether the late row is a `_SECOND_PRODUCER` stream. While
    no fan-out venue HAS such a stream, only the shared-chain cursor can ever
    classify a fan-out pair — so a venue dropped from the other call site would
    be caught by nothing, which is exactly what a mutation of that site showed
    (it survived the whole suite). `pair_kind` binds the venue once so the site
    cannot be got wrong; this test is the other half, and says when the second
    site stops being dead. Give Extended a REST poller and it goes live, and
    this failing is how that gets noticed rather than discovered in a report.
    """
    assert qr._FANS_OUT_PER_CHANNEL, "the set is empty; nothing below is checked"
    for exchange in qr._FANS_OUT_PER_CHANNEL:
        expected = qr.expected_streams("mode-a-v1", exchange, {})
        streams = (
            set(expected.required)
            | set(expected.optional)
            | set(expected.informational)
        )
        assert streams, f"{exchange} declares no streams, so this checks nothing"
        assert streams.isdisjoint(qr._SECOND_PRODUCER), (
            f"{exchange} fans out AND has a stream with a hand-off of its own; "
            f"the adjacent-pair cursor in scan_symbol_file can now classify one "
            f"of its pairs, and needs a test of its own"
        )


def test_an_inversion_between_two_binance_websocket_streams_is_red(tmp_path):
    """`bookTicker`, `trade` and `depthUpdate` all come off the one WS reader
    through `pump`, so the REST snapshot's excuse does not extend to them."""
    d = tmp_path / "um-ws-only"
    d.mkdir()
    base = ns(0, nanos=100_000_000)
    write_gz(
        d / f"btcusdt_{DAY}.gz",
        [
            (base, um_book_ticker("BTCUSDT", base)),
            (base + 2 * MS, um_trade("BTCUSDT", base + 2 * MS)),
            (base + MS, um_book_ticker("BTCUSDT", base + MS, u=2)),  # 1ms backwards
        ],
    )
    write_meta(d, "binancefuturesum", DAY,
               [(ns(0), session_start("binancefuturesum", ["BTCUSDT"]))])
    out = tmp_path / "r.json"
    code, report = run(d, out=out)
    assert code == 1
    assert "interleave_excess" in checks_of(report, "binancefuturesum", severity="red")


@pytest.mark.parametrize("delta_ns", [MS, SEC + 1, 30 * SEC])
def test_two_websocket_streams_stay_red_however_far_apart_they_are(tmp_path, delta_ns):
    """The pair that shares every hand-off keeps its red at every size.

    `bookTicker` and `trade` reach the file through the same socket hop, the
    same parser and the same writer hop, in that order and in one FIFO each, so
    there is no queue left that could hold one of them back — write order IS
    receive order for this pair whatever the burst. The poller's exemption
    (2026-07-30) is granted on a hand-off these two do not have, so a wide
    inversion here has to stay exactly as red as a narrow one; the 1s and 30s
    cases are the ones that would move if the exemption were read as being
    about the size.
    """
    d = tmp_path / f"um-ws-only-{delta_ns}"
    d.mkdir()
    base = ns(0, nanos=100_000_000)
    write_gz(
        d / f"btcusdt_{DAY}.gz",
        [
            (base, um_book_ticker("BTCUSDT", base)),
            (base + delta_ns, um_trade("BTCUSDT", base + delta_ns)),
            (base, um_book_ticker("BTCUSDT", base, u=2)),  # delta_ns backwards
        ],
    )
    write_meta(d, "binancefuturesum", DAY,
               [(ns(0), session_start("binancefuturesum", ["BTCUSDT"]))])
    out = tmp_path / f"r-{delta_ns}.json"
    code, report = run(d, out=out)
    assert code == 1
    assert "interleave_excess" in checks_of(report, "binancefuturesum", severity="red")
    symbol = report["venues"]["binancefuturesum"]["days"][DAY]["symbols"]["btcusdt"]
    assert symbol["interleave_excess"]["max_delta_ns"] == delta_ns
    assert "interleave_inversion_poller" not in symbol


def test_a_whole_file_out_of_order_is_red_not_a_tolerated_interleave(tmp_path):
    """One 134us interleave per five million lines is the measured shape. A
    stream of them is two recordings in one file, and stays red."""
    d = tmp_path / "um-shuffled"
    d.mkdir()
    base = ns(0, nanos=500_000_000)
    recs = [(base, um_book_ticker("BTCUSDT", base))]
    for i in range(1, 6):
        # Each snapshot is stamped at least two seconds after the bookTicker
        # written next to it: far beyond any hand-off skew, and beyond the
        # tolerance with the whole socket hop to spare. The offsets are
        # `(i + 1) * SEC` and not `i * SEC` for that last reason — at one
        # second the SMALLEST of the five inversions is 999ms, which slipped
        # under the bound the moment it followed the socket hop up to 1s on
        # 2026-07-29 and turned this into a four-violation file. The fixture
        # has to keep saying "every pair here is out of order" whatever the
        # bound is; pinning the count to whatever survives it would be reading
        # the answer off the implementation.
        at = base + (i + 1) * SEC
        recs.append((at, um_depth_snapshot("BTCUSDT", at, 1000 + i)))
        recs.append((base + i * MS, um_book_ticker("BTCUSDT", base + i * MS)))
    write_gz(d / f"btcusdt_{DAY}.gz", recs)
    write_meta(d, "binancefuturesum", DAY,
               [(ns(0), session_start("binancefuturesum", ["BTCUSDT"]))])
    out = tmp_path / "r.json"
    code, report = run(d, out=out)
    assert code == 1
    symbol = report["venues"]["binancefuturesum"]["days"][DAY]["symbols"]["btcusdt"]
    assert symbol["interleave_excess"]["violations"] == 5
    assert symbol["interleave_excess"]["max_delta_ns"] == 6 * SEC - 5 * MS
    assert symbol["interleave_inversion"] is None, (
        "every pair in this file is beyond the bound; a tolerated one left over "
        "means the fixture has drifted back under it"
    )


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


# --------------------------------------------------------------------------
# Index, oracle and funding: the informational stream class
#
# `<symbol>@markPrice@1s` (Binance UM/CM) and `activeAssetCtx` (Hyperliquid)
# were added to the collector on 2026-07-28. They carry each venue's own spot
# basket — the input its funding is priced against — which is why they are
# recorded; they are not order flow, mode A does not trade on them, and every
# recording made before that date lacks them entirely.
#
# So they are neither required nor optional: the profile knows them, checks
# them while they are there, and says nothing when they are not. Making their
# absence even a warning would turn every historical day yellow at once, which
# is precisely the noisy gate the design document's acceptance line rules out.
# --------------------------------------------------------------------------

#: Both feeds are periodic at ~1/s (measured 2026-07-28: 1.000s median on
#: COIN-M `markPrice`, 1.018s on Hyperliquid `activeAssetCtx`), so both take the
#: same K=10 the other periodic feeds in `MAX_GAP_NS` take.
INDEX_LIMIT_NS = 10 * SEC


def index_day(tmp_path, venue, name, gap_ns, meta_extra=()):
    """A one-symbol day whose index feed has a single hole of `gap_ns`.

    Everything the profile requires is written once, at the start: a stream seen
    once has no interval to measure and so contributes no hole of its own, which
    leaves exactly one gap in the report for the feed under test.
    """
    d = tmp_path / name
    d.mkdir()
    start, end = ns(0), ns(0) + gap_ns
    if venue == "hyperliquid":
        write_gz(
            d / f"btc_{DAY}.gz",
            [
                (start, hl_trade("BTC", start)),
                (start, hl_bbo("BTC", start)),
                (start, hl_l2("BTC", start)),
                (start, hl_l2("BTC", start, fast=True)),
                (start, hl_active_asset_ctx("BTC", start)),
                (end, hl_active_asset_ctx("BTC", end)),
            ],
        )
        write_meta(
            d,
            "hyperliquid",
            DAY,
            [(ns(0), session_start("hyperliquid", ["BTC"])), *meta_extra],
        )
        return d, "btc"

    write_gz(
        d / f"btcusdt_{DAY}.gz",
        [
            (start, um_book_ticker("BTCUSDT", start)),
            (start, um_trade("BTCUSDT", start)),
            (start, um_depth("BTCUSDT", start, u=100, pu=99)),
            (start, um_mark_price("BTCUSDT", start)),
            (end, um_mark_price("BTCUSDT", end)),
        ],
    )
    write_meta(
        d,
        "binancefuturesum",
        DAY,
        [(ns(0), session_start("binancefuturesum", ["BTCUSDT"])), *meta_extra],
    )
    return d, "btcusdt"


@pytest.mark.parametrize(
    "family,raw,stream",
    [
        (qr.BINANCE, json.dumps(um_mark_price("BTCUSDT", ns(0))), "markPriceUpdate"),
        (qr.BINANCE, CM_MARK_PRICE_CAPTURED, "markPriceUpdate"),
        (qr.HYPERLIQUID, json.dumps(hl_active_asset_ctx("BTC", ns(0))), "activeAssetCtx"),
        (qr.HYPERLIQUID, HL_ACTIVE_ASSET_CTX_DEX_CAPTURED, "activeAssetCtx"),
    ],
)
def test_the_index_and_funding_frames_classify_as_their_own_streams(family, raw, stream):
    """Each new shape has to become a stream of its own, not `(unclassified)`.

    Nothing in the classifier was added for them — Binance keys on `data.e` and
    Hyperliquid on `channel`, and both frames answer those — but "it happens to
    work" and "it is checked" are different states, and only the second one
    survives someone tightening either rule to a whitelist. An unclassified
    frame is a yellow issue per file, so getting this wrong would also have
    made every day carrying the new feeds noisy.
    """
    assert qr.classify(family, json.loads(raw)) == stream


@pytest.mark.parametrize(
    "family,stream",
    [(qr.BINANCE, "markPriceUpdate"), (qr.HYPERLIQUID, "activeAssetCtx")],
)
def test_the_index_feeds_have_a_cadence_expectation(family, stream):
    """A stream with no entry in `MAX_GAP_NS` is never checked for holes at all.

    That is the default for anything the table has not heard of, and it is the
    wrong default here: both feeds are periodic, so unlike an event-driven one
    their silence is evidence on its own and needs no liveness witness.
    """
    assert qr.MAX_GAP_NS[(family, stream)] == INDEX_LIMIT_NS
    assert qr.gap_limit(family, stream) == INDEX_LIMIT_NS


@pytest.mark.parametrize(
    "venue,stream",
    [("hyperliquid", "activeAssetCtx"), ("binancefuturesum", "markPriceUpdate")],
)
def test_an_index_gap_of_exactly_the_limit_is_not_flagged_and_one_nanosecond_more_is(
    tmp_path, venue, stream
):
    """Pins the limit to the nanosecond, the way the other cadences are pinned."""
    for label, gap_ns, expected in (
        ("at", INDEX_LIMIT_NS, 0),
        ("over", INDEX_LIMIT_NS + 1, 1),
    ):
        d, sym = index_day(tmp_path, venue, f"{venue}-{label}", gap_ns)
        out = tmp_path / f"r-{venue}-{label}.json"
        _, report = run(d, out=out)
        stat = report["venues"][venue]["days"][DAY]["symbols"][sym]["streams"][stream]
        assert stat["gap_count"] == expected, (venue, label, gap_ns)


@pytest.mark.parametrize(
    "venue,stream",
    [("hyperliquid", "activeAssetCtx"), ("binancefuturesum", "markPriceUpdate")],
)
def test_an_index_gap_is_flagged_and_a_reconnect_explains_it(tmp_path, venue, stream):
    """Present, so it is checked — and answerable from the sidecar like any other.

    A 30s hole in a 1/s feed is three times what the other streams' own limits
    would notice on this venue, so the informational class is not a quiet class:
    it is the finest cadence signal either socket has.
    """
    d, sym = index_day(
        tmp_path,
        venue,
        f"{venue}-reconnect",
        30 * SEC,
        meta_extra=[
            (ns(20), {"_collector": "disconnected", "error": "reset", "connected_for_ms": 20000}),
            (ns(25), {"_collector": "connected", "url": "wss://example.invalid/ws"}),
        ],
    )
    out = tmp_path / f"r-{venue}.json"
    code, report = run(d, out=out)
    assert code == 0, "a hole the collector itself reported is a warning, not a refusal"
    assert "unclassified_frame" not in checks_of(report, venue)

    detail = next(
        i["detail"]
        for i in issues_of(report, venue)
        if i["check"] == "cadence_gap" and stream in i["detail"]
    )
    assert "disconnected" in detail
    assert "limit 10.000s" in detail
    gap = report["venues"][venue]["days"][DAY]["symbols"][sym]["streams"][stream]["gaps"][0]
    assert gap["explained_by"].startswith("disconnected at ")
    assert gap["suppressed_by"] is None, (
        "these feeds are periodic, so nothing else running is evidence that "
        "their own silence was harmless"
    )


def test_an_absent_index_feed_is_a_fact_and_never_a_warning(tmp_path):
    """The whole point: recordings made before 2026-07-28 must not start yellowing.

    Every day of every recording in existence lacks both feeds. Reporting that
    as `missing_optional` would put a warning on all of them at once and bury
    the gate's real signal, so absence is recorded in the JSON and raises
    nothing.
    """
    hl = hl_dir(tmp_path)
    um = um_dir(tmp_path)
    out = tmp_path / "r.json"
    code, report = run(hl, um, out=out)

    assert code == 0
    assert checks_of(report, "hyperliquid") == []
    assert checks_of(report, "binancefuturesum") == []

    hl_sym = report["venues"]["hyperliquid"]["days"][DAY]["symbols"]["btc"]
    um_sym = report["venues"]["binancefuturesum"]["days"][DAY]["symbols"]["btcusdt"]
    assert hl_sym["missing_informational"] == ["activeAssetCtx"]
    # Both of USD-M's: the WS stream it no longer subscribes to (the venue
    # stopped serving that class) and the REST poller that replaced it.
    assert um_sym["missing_informational"] == ["markPriceUpdate", "premiumIndex"]
    assert hl_sym["missing_optional"] == []
    assert um_sym["missing_optional"] == []
    assert hl_sym["missing_required"] == []
    assert um_sym["missing_required"] == []
    # And they stay out of the window Phase 3 trims to, whether present or not.
    assert "activeAssetCtx" not in hl_sym["coverage"]["required_streams"]
    assert "markPriceUpdate" not in um_sym["coverage"]["required_streams"]


def test_a_symbol_with_no_file_at_all_still_lists_its_informational_streams(tmp_path):
    """The no-file branch builds its own JSON entry, so it needs the key too.

    A consumer reading `missing_informational` cannot be made to guess which
    shape of entry it is holding.
    """
    d = tmp_path / "um-one-symbol-missing"
    d.mkdir()
    write_gz(d / f"btcusdt_{DAY}.gz", [(ns(0), um_book_ticker("BTCUSDT", ns(0)))])
    write_meta(
        d,
        "binancefuturesum",
        DAY,
        [(ns(0), session_start("binancefuturesum", ["BTCUSDT", "ETHUSDT"]))],
    )
    out = tmp_path / "r.json"
    code, report = run(d, out=out)
    assert code == 1, "the symbol has no bookTicker, which mode A does require"
    sym = report["venues"]["binancefuturesum"]["days"][DAY]["symbols"]["ethusdt"]
    assert sym["missing_required"] == ["bookTicker"]
    assert sym["missing_informational"] == ["markPriceUpdate", "premiumIndex"]


def test_the_index_feeds_have_no_sequence_chain_and_leave_the_depth_one_alone(tmp_path):
    """No `pu` chain, no `u` chain — and no effect on the one that exists.

    The Python-side counterpart of the Rust guard: `markPriceUpdate` carries `s`
    but neither `u` nor `pu`, so a sequence tracker that keyed on the symbol
    rather than on the stream would see every mark-price frame break the depth
    chain, and the report would claim a lost-frame gap once a second.
    """
    d = tmp_path / "um-markprice-sequence"
    d.mkdir()
    write_gz(
        d / f"btcusdt_{DAY}.gz",
        [
            (ns(0), um_book_ticker("BTCUSDT", ns(0))),
            (ns(0), um_depth("BTCUSDT", ns(0), u=100, pu=99)),
            (ns(1), um_mark_price("BTCUSDT", ns(1))),
            (ns(2), um_depth("BTCUSDT", ns(2), u=101, pu=100)),
        ],
    )
    write_meta(
        d, "binancefuturesum", DAY, [(ns(0), session_start("binancefuturesum", ["BTCUSDT"]))]
    )
    out = tmp_path / "r-um.json"
    code, report = run(d, out=out)
    assert code == 0, issues_of(report, "binancefuturesum")
    assert "sequence_gap" not in checks_of(report, "binancefuturesum")
    sym = report["venues"]["binancefuturesum"]["days"][DAY]["symbols"]["btcusdt"]
    assert set(sym["sequence_breaks"]) == {"depthUpdate"}

    # Hyperliquid publishes no sequence number on any channel, and the new one
    # is no exception: cadence is all the evidence there is.
    hl = hl_dir(tmp_path, name="hl-ctx-sequence")
    write_gz(hl / f"btc_{DAY}.gz", [(ns(4), hl_active_asset_ctx("BTC", ns(4)))], append=True)
    out = tmp_path / "r-hl.json"
    code, report = run(hl, out=out)
    assert code == 0, issues_of(report, "hyperliquid")
    assert report["venues"]["hyperliquid"]["days"][DAY]["symbols"]["btc"]["sequence_breaks"] == {}


@pytest.mark.parametrize("venue", ["hyperliquid", "binancefuturesum"])
def test_an_index_stream_going_backwards_within_itself_is_red(tmp_path, venue):
    """One WS reader stamps these at receive time and queues them in that order.

    Both feeds come off the same socket loop as the book and the tape
    (`hyperliquid/mod.rs`, `binancefutures*::pump`), so within one of them a
    step backwards of any size is a clock or two recordings in one file — the
    same rule every other stream here is held to.
    """
    d, _ = index_day(tmp_path, venue, f"{venue}-backwards", 5 * SEC)
    path = next(d.glob(f"*_{DAY}.gz"))
    late = ns(5) - MS
    frame = (
        hl_active_asset_ctx("BTC", late)
        if venue == "hyperliquid"
        else um_mark_price("BTCUSDT", late)
    )
    write_gz(path, [(late, frame)], append=True)
    out = tmp_path / f"r-{venue}.json"
    code, report = run(d, out=out)
    assert code == 1
    assert "monotonicity" in checks_of(report, venue, severity="red")


@pytest.mark.parametrize("venue", ["hyperliquid", "binancefuturesum"])
def test_an_index_frame_out_of_order_against_another_stream_is_red(tmp_path, venue):
    """No second producer writes these files, so there is no race to excuse one.

    The REST depth-snapshot fetcher is the only concurrent producer the report
    knows of, and it writes neither of these feeds; granting them the interleave
    tolerance would downgrade a real defect to yellow.
    """
    d = tmp_path / f"{venue}-interleave"
    d.mkdir()
    base = ns(0, nanos=100_000_000)
    if venue == "hyperliquid":
        write_gz(
            d / f"btc_{DAY}.gz",
            [
                (base, hl_trade("BTC", base)),
                (base, hl_l2("BTC", base)),
                (base, hl_l2("BTC", base, fast=True)),
                (base + 2 * MS, hl_bbo("BTC", base + 2 * MS)),
                (base + MS, hl_active_asset_ctx("BTC", base + MS)),
            ],
        )
        write_meta(d, "hyperliquid", DAY, [(ns(0), session_start("hyperliquid", ["BTC"]))])
    else:
        write_gz(
            d / f"btcusdt_{DAY}.gz",
            [
                (base, um_book_ticker("BTCUSDT", base)),
                (base + 2 * MS, um_trade("BTCUSDT", base + 2 * MS)),
                (base + MS, um_mark_price("BTCUSDT", base + MS)),
            ],
        )
        write_meta(
            d, "binancefuturesum", DAY, [(ns(0), session_start("binancefuturesum", ["BTCUSDT"]))]
        )
    out = tmp_path / f"r-{venue}.json"
    code, report = run(d, out=out)
    assert code == 1
    assert "interleave_excess" in checks_of(report, venue, severity="red")
    detail = next(
        i["detail"] for i in issues_of(report, venue) if i["check"] == "interleave_excess"
    )
    assert "no second producer" in detail


# --------------------------------------------------------------------------
# premiumIndex: the REST poller that replaced the UM mark-price stream
#
# Measured 2026-07-28 from two independent network paths: `fstream.binance.com`
# serves no member of the markPrice class of public streams — `@markPrice@1s`,
# `@markPrice`, `!markPrice@arr`, `!markPrice@arr@1s`, `@indexPrice@1s` all
# delivered zero frames while `@trade` and `!bookTicker` delivered 802 in eight
# seconds on the same socket. COIN-M's `dstream` still serves them.
#
# So USD-M's index and funding data now come from a REST poller
# (`binancefuturesum::PREMIUM_INDEX_INTERVAL`) which writes the venue's own
# array elements, verbatim, into the symbol files. That makes them a frame
# shape this report has never seen, written by a producer it has never had on
# this venue's WS-only path.
# --------------------------------------------------------------------------

#: One element of `GET /fapi/v1/premiumIndex`, captured verbatim from
#: `fapi.binance.com` on 2026-07-28. Kept as the raw line for the same reason
#: the COIN-M mark-price fixture is: what has to classify is the bytes the venue
#: sends, and the collector writes them through untouched (`RawValue`, so not
#: even the key order changes).
UM_PREMIUM_INDEX_CAPTURED = (
    '{"symbol":"BTCUSDT","markPrice":"63466.95207971",'
    '"indexPrice":"63494.85043478","estimatedSettlePrice":"63524.24373551",'
    '"lastFundingRate":"0.00005166","interestRate":"0.00010000",'
    '"nextFundingTime":1785254400000,"time":1785244313000}'
)

#: The poller's period. Its cadence is a constant in the collector rather than
#: anything the venue imposes, so unlike every other entry in `MAX_GAP_NS` this
#: one is not a measurement — it is the interval the recorder was built with.
PREMIUM_INDEX_INTERVAL_NS = 10 * SEC

#: And the same K=10 the other periodic feeds take: 100s is nine consecutive
#: failed polls, which is well past a venue hiccup and still a third of the
#: `poller_degraded` threshold, so the two signals do not race each other.
PREMIUM_INDEX_LIMIT_NS = 100 * SEC


def um_premium_index(symbol, ts, mark="63466.95207971"):
    """A `premiumIndex` element as the poller files it: bare, no envelope."""
    return {
        "symbol": symbol,
        "markPrice": mark,
        "indexPrice": "63494.85043478",
        "estimatedSettlePrice": "63524.24373551",
        "lastFundingRate": "0.00005166",
        "interestRate": "0.00010000",
        "nextFundingTime": 1_785_254_400_000,
        "time": ms_of(ts),
    }


def premium_index_day(tmp_path, name, gap_ns, meta_extra=()):
    """A one-symbol UM day whose premium-index feed has a single hole."""
    d = tmp_path / name
    d.mkdir()
    start, end = ns(0), ns(0) + gap_ns
    write_gz(
        d / f"btcusdt_{DAY}.gz",
        [
            (start, um_book_ticker("BTCUSDT", start)),
            (start, um_trade("BTCUSDT", start)),
            (start, um_depth("BTCUSDT", start, u=100, pu=99)),
            (start, um_premium_index("BTCUSDT", start)),
            (end, um_premium_index("BTCUSDT", end)),
        ],
    )
    write_meta(
        d,
        "binancefuturesum",
        DAY,
        [(ns(0), session_start("binancefuturesum", ["BTCUSDT"])), *meta_extra],
    )
    return d, "btcusdt"


def test_a_premium_index_element_classifies_as_its_own_stream():
    """The one frame shape here that no existing rule could have reached.

    Every other Binance line is either a combined-stream envelope keyed on
    `data.e` or the REST depth snapshot keyed on `lastUpdateId`. A premium-index
    element is neither: no envelope, no `e`, and its symbol is under `symbol`
    rather than `s`. Left unclassified it would be a yellow `unclassified_frame`
    on every UM day recorded from now on, and the feed would be invisible to
    every check in this report.
    """
    assert qr.classify(qr.BINANCE, json.loads(UM_PREMIUM_INDEX_CAPTURED)) == "premiumIndex"
    assert qr.classify(qr.BINANCE, um_premium_index("ETHUSDT", ns(0))) == "premiumIndex"


@pytest.mark.parametrize(
    "raw,stream",
    [
        (json.dumps(um_book_ticker("BTCUSDT", ns(0))), "bookTicker"),
        (json.dumps(um_mark_price("BTCUSDT", ns(0))), "markPriceUpdate"),
        (CM_MARK_PRICE_CAPTURED, "markPriceUpdate"),
        ('{"lastUpdateId":1,"E":1,"T":1,"bids":[],"asks":[]}', "depthSnapshot"),
    ],
)
def test_the_premium_index_rule_does_not_swallow_the_frames_beside_it(raw, stream):
    """A discriminator, not a catch-all.

    `markPriceUpdate` is the trap: it carries a mark price, an index price and a
    funding rate too — the same three quantities under one-letter names — so a
    rule that keyed on "looks like index data" would relabel the whole COIN-M
    feed. What actually separates them is structural and not semantic: the WS
    frames name their event in `e` and their symbol in `s`, and the REST element
    has neither.
    """
    assert qr.classify(qr.BINANCE, json.loads(raw)) == stream


def test_the_premium_index_feed_has_a_cadence_expectation():
    """Without an entry it is never checked for holes at all — the wrong default.

    The poller is periodic by construction: it fires on a timer whether or not
    anything moved, so its silence is evidence on its own and needs no liveness
    witness, exactly like the other periodic feeds here.
    """
    assert qr.MAX_GAP_NS[(qr.BINANCE, "premiumIndex")] == PREMIUM_INDEX_LIMIT_NS
    assert qr.gap_limit(qr.BINANCE, "premiumIndex") == PREMIUM_INDEX_LIMIT_NS
    assert PREMIUM_INDEX_LIMIT_NS == 10 * PREMIUM_INDEX_INTERVAL_NS


def test_a_premium_index_gap_of_exactly_the_limit_is_not_flagged_and_one_more_ns_is(tmp_path):
    """Pins the limit to the nanosecond, the way the other cadences are pinned."""
    for label, gap_ns, expected in (
        ("at", PREMIUM_INDEX_LIMIT_NS, 0),
        ("over", PREMIUM_INDEX_LIMIT_NS + 1, 1),
    ):
        d, sym = premium_index_day(tmp_path, f"um-pi-{label}", gap_ns)
        out = tmp_path / f"r-pi-{label}.json"
        _, report = run(d, out=out)
        stat = report["venues"]["binancefuturesum"]["days"][DAY]["symbols"][sym]["streams"]
        assert stat["premiumIndex"]["gap_count"] == expected, (label, gap_ns)


def um_poll_before_older_ticks(delta_ns, name="BTCUSDT"):
    """The 2026-07-29 13:30:03 shape: a poll written before older WS frames.

    One `premiumIndex` row, stamped when the REST body arrived, written into the
    file immediately ahead of book ticks stamped `delta_ns` earlier. That is
    what the US-session burst produced in all thirteen symbols of
    `binancefuturesum-b` at the same instant, at 1.023-1.045s.
    """
    early = ns(0, nanos=100_000_000)
    tick = ns(0, nanos=164_558_903)
    poll = tick + delta_ns
    return [
        (early, um_book_ticker(name, early)),
        (early, um_trade(name, early)),
        (early, um_depth(name, early, u=100, pu=99)),
        (poll, um_premium_index(name, poll)),
        (tick, um_book_ticker(name, tick, u=2)),  # <- written after, stamped before
        (tick + 1_000, um_book_ticker(name, tick + 1_000, u=3)),
    ]


def um_poller_day(tmp_path, name, delta_ns):
    d = tmp_path / name
    d.mkdir()
    write_gz(d / f"btcusdt_{DAY}.gz", um_poll_before_older_ticks(delta_ns))
    write_meta(
        d, "binancefuturesum", DAY, [(ns(0), session_start("binancefuturesum", ["BTCUSDT"]))]
    )
    return d


def test_the_premium_index_poller_is_a_second_producer(tmp_path):
    """It skips the socket hop, so it can legally be written ahead of WS frames.

    This is the depth-snapshot situation again and it is now the common case
    rather than the rare one: the poller hands its elements **straight to the
    writer** while every WS frame queues through the socket hop first, so a
    premium-index line stamped later can reach the file before a book tick
    stamped earlier. Eight thousand polls a day against a hop that holds up to
    819.2ms of frames — without an entry in `_SECOND_PRODUCER` a healthy UM
    recording would go red on `interleave_excess`, and red is a hard build
    refusal in `build_dataset.py`.
    """
    assert "premiumIndex" in qr._SECOND_PRODUCER

    d = um_poller_day(tmp_path, "um-pi-interleave", MS)
    out = tmp_path / "r.json"
    code, report = run(d, out=out)

    assert code == 0, issues_of(report, "binancefuturesum")
    symbol = report["venues"]["binancefuturesum"]["days"][DAY]["symbols"]["btcusdt"]
    assert symbol["interleave_excess"] is None
    assert symbol["monotonic_violation"] is None
    assert symbol["interleave_inversion_poller"]["previous_stream"] == "premiumIndex"

    detail = next(
        i
        for i in issues_of(report, "binancefuturesum")
        if i["check"] == "interleave_inversion_poller"
    )["detail"]
    assert "premiumIndex" in detail and "producer" in detail


@pytest.mark.parametrize(
    "delta_ns",
    [
        MS,
        qr.CROSS_STREAM_TOLERANCE_NS,
        1_034_729_563,  # tiausdt, 2026-07-29T13:30:03 UTC, measured
        30 * SEC,
        5 * 60 * SEC,
    ],
)
def test_a_poll_written_before_older_ws_frames_is_yellow_at_any_magnitude(
    tmp_path, delta_ns
):
    """The finding the 2026-07-29 gate got wrong, and the model that fixes it.

    The poller is the only producer in the collector with a **hand-off of its
    own** (`queue::POLLER_HOP`, `main.rs` claims it for `binancefuturesum`
    alone). Its row therefore shares no FIFO with a WS frame at all: the two
    meet for the first time in `main`'s `select!`, which picks between the two
    arms as they become ready. What separates them is the WS frame's own wait —
    the socket hop **plus** the writer hop — and the writer hop drains at the
    speed of blocking gzip I/O, which has been measured stopping for longer than
    its own depth (`queue.rs`, 2026-07-26). No arithmetic over capacities bounds
    that, so no magnitude here is evidence of anything, and the bound that was
    applied to it went red on thirteen intact symbol files at once.

    Five minutes is in the list on purpose: it is the stall watchdog's default
    window, i.e. the longest a wedged writer can hold WS frames back before
    something outside this hop ends the process.
    """
    d = um_poller_day(tmp_path, f"um-poller-{delta_ns}", delta_ns)
    out = tmp_path / f"r-{delta_ns}.json"
    code, report = run(d, out=out)

    assert code == 0, issues_of(report, "binancefuturesum")
    assert "interleave_inversion_poller" in checks_of(
        report, "binancefuturesum", severity="yellow"
    )
    symbol = report["venues"]["binancefuturesum"]["days"][DAY]["symbols"]["btcusdt"]
    assert symbol["interleave_excess"] is None
    assert symbol["interleave_inversion"] is None
    assert symbol["monotonic_violation"] is None
    record = symbol["interleave_inversion_poller"]
    assert record["previous_stream"] == "premiumIndex"
    assert record["stream"] == "bookTicker"
    assert record["max_delta_ns"] == delta_ns
    assert record["violations"] == 1
    # Nothing was lost: the depth chain is where a dropped frame shows, and this
    # is the whole argument for the finding not being red.
    assert not any(symbol["sequence_breaks"].values())


def um_poll_after_newer_ticks(delta_ns, name="BTCUSDT"):
    """The other direction: the poll written *after* a book tick stamped later.

    Nothing on the WS side can have caused this. `writer_rx` is a FIFO, so a
    backlog holds up the rows queued in it, not a row that crossed
    `poller->writer` instead — the poll here is the row that waited, and its own
    hand-off is all it can have waited in. Measured worst on a full symbol-day
    (tiausdt, 2026-07-29): 3.128ms, against 1.034730s the other way round.
    """
    early = ns(0, nanos=100_000_000)
    tick = ns(10, nanos=164_558_903)
    poll = tick - delta_ns
    return [
        (early, um_book_ticker(name, early)),
        (early, um_trade(name, early)),
        (early, um_depth(name, early, u=100, pu=99)),
        (tick, um_book_ticker(name, tick, u=2)),
        (poll, um_premium_index(name, poll)),  # <- written after, stamped before
        (tick + 1_000, um_book_ticker(name, tick + 1_000, u=3)),
    ]


@pytest.mark.parametrize(
    "delta_ns,expected_code,expected_check",
    [
        (3_128_000, 0, "interleave_inversion"),  # the worst ever observed
        (qr.POLLER_HOP_CEILING_NS, 0, "interleave_inversion"),
        (qr.POLLER_HOP_CEILING_NS + 1, 1, "interleave_excess"),
    ],
)
def test_a_poll_written_after_newer_ws_frames_keeps_its_bound(
    tmp_path, delta_ns, expected_code, expected_check
):
    """The half of the poller pair that still has a mechanism, and a bound.

    An inversion is bounded by the *late* row's own latency — the row written
    second, stamped first — and here that row is the poll. It crossed
    `queue::POLLER_HOP` and nothing else: a hop `queue.rs` says cannot fill,
    carrying one element per recorded symbol per ten seconds, drained from an
    arm `main` offers every iteration. Granting this direction the unbounded
    yellow deletes the only detector the report has for a `premiumIndex` stamp
    that is not the receive moment, which is a lookahead defect — see
    `test_a_premium_index_stamp_that_is_not_the_receive_moment_is_red`.

    The bound itself is `POLLER_HOP_CEILING_NS`: a ceiling read off an
    observation rather than a model of this hop — ~320x the worst it has been
    seen doing — and a constant of its own, so that a deeper socket hop cannot
    widen it by a factor nobody decided.
    """
    d = tmp_path / f"um-poll-late-{delta_ns}"
    d.mkdir()
    write_gz(d / f"btcusdt_{DAY}.gz", um_poll_after_newer_ticks(delta_ns))
    write_meta(
        d, "binancefuturesum", DAY, [(ns(0), session_start("binancefuturesum", ["BTCUSDT"]))]
    )
    out = tmp_path / f"r-{delta_ns}.json"
    code, report = run(d, out=out)

    assert code == expected_code, issues_of(report, "binancefuturesum")
    symbol = report["venues"]["binancefuturesum"]["days"][DAY]["symbols"]["btcusdt"]
    assert symbol["monotonic_violation"] is None
    assert "interleave_inversion_poller" not in symbol, (
        "the poll is the row that waited here; the exemption is for the row it "
        "overtakes, and it has overtaken nothing"
    )
    record = symbol[expected_check]
    assert record["previous_stream"] == "bookTicker"
    assert record["stream"] == "premiumIndex"
    assert record["max_delta_ns"] == delta_ns
    detail = next(
        i for i in issues_of(report, "binancefuturesum") if i["check"] == expected_check
    )["detail"]
    assert "the poll was written last" in detail, (
        "the text has to name the row that waited, or it explains the pair by a "
        "mechanism that cannot have produced it"
    )


def test_a_premium_index_stamp_that_is_not_the_receive_moment_is_red(tmp_path):
    """Why that direction keeps its bound: this is the defect it detects.

    A poller stamping at request time, serving a cached body, or copying the
    venue's own `E` files each element into the recording ahead of the moment it
    was knowable — lookahead, in every dataset built from these files. A uniform
    offset is invisible to everything else in `check_day`: `premiumIndex` stays
    monotone within itself, keeps its cadence and its coverage, and carries no
    sequence chain to break. It shows up as the poll being written after rows
    stamped later, at a magnitude its own hand-off cannot account for, and
    nowhere else.

    Today the collector takes `Utc::now()` after the response body arrives
    (`binancefuturesum/mod.rs`), so this is not live — which is the point: a
    gate is for the code that has not been written yet.
    """
    d = tmp_path / "um-stale-poll"
    d.mkdir()
    base = ns(60, nanos=100_000_000)
    stale = 30 * SEC
    rows = [
        (base, um_book_ticker("BTCUSDT", base)),
        (base, um_depth("BTCUSDT", base, u=100, pu=99)),
    ]
    for k in range(1, 6):
        tick = base + k * PREMIUM_INDEX_INTERVAL_NS
        rows.append((tick, um_book_ticker("BTCUSDT", tick, u=k + 1)))
        rows.append((tick - stale, um_premium_index("BTCUSDT", tick - stale)))
    write_gz(d / f"btcusdt_{DAY}.gz", rows)
    write_meta(
        d, "binancefuturesum", DAY, [(ns(0), session_start("binancefuturesum", ["BTCUSDT"]))]
    )
    out = tmp_path / "r.json"
    code, report = run(d, out=out)

    assert code == 1
    assert "interleave_excess" in checks_of(report, "binancefuturesum", severity="red")
    symbol = report["venues"]["binancefuturesum"]["days"][DAY]["symbols"]["btcusdt"]
    record = symbol["interleave_excess"]
    assert record["stream"] == "premiumIndex"
    assert record["violations"] == 5
    assert record["max_delta_ns"] == stale
    # Every other check on this file is clean, which is the whole argument for
    # not taking the bound off this direction.
    assert symbol["monotonic_violation"] is None
    assert "interleave_inversion_poller" not in symbol
    assert not any(symbol["sequence_breaks"].values())
    assert symbol["streams"]["premiumIndex"]["gap_count"] == 0
    detail = next(
        i for i in issues_of(report, "binancefuturesum") if i["check"] == "interleave_excess"
    )["detail"]
    assert "poller->writer" in detail
    assert "ahead of the moment it was knowable" in detail, "name the defect, not just the size"


def test_a_poll_between_two_ws_frames_does_not_launder_their_inversion(tmp_path):
    """A poll row is not somewhere a WS pair can hide.

    `bookTicker` and `trade` share every hand-off, so an inversion between them
    is red at a nanosecond — and it stays red when a `premiumIndex` row is
    written between them, because that row crossed a different hop and cannot
    have reordered either. Compared against the previous *line* only, the pair
    examined would be premiumIndex->trade, which is the unbounded class: one
    poll row in the right place would take a real defect off the report, and
    there are ~8640 of them per symbol-day arriving during exactly the bursts a
    writer or parser defect surfaces in.
    """
    d = tmp_path / "um-launder"
    d.mkdir()
    base = ns(0, nanos=100_000_000)
    write_gz(
        d / f"btcusdt_{DAY}.gz",
        [
            (base, um_book_ticker("BTCUSDT", base)),
            (base, um_depth("BTCUSDT", base, u=100, pu=99)),
            (base + 5 * SEC, um_book_ticker("BTCUSDT", base + 5 * SEC, u=2)),
            (base + 5 * SEC + MS, um_premium_index("BTCUSDT", base + 5 * SEC + MS)),
            (base, um_trade("BTCUSDT", base)),  # 5s behind the tick two lines up
        ],
    )
    write_meta(
        d, "binancefuturesum", DAY, [(ns(0), session_start("binancefuturesum", ["BTCUSDT"]))]
    )
    out = tmp_path / "r.json"
    code, report = run(d, out=out)

    assert code == 1
    assert "interleave_excess" in checks_of(report, "binancefuturesum", severity="red")
    symbol = report["venues"]["binancefuturesum"]["days"][DAY]["symbols"]["btcusdt"]
    record = symbol["interleave_excess"]
    assert (record["previous_stream"], record["stream"]) == ("bookTicker", "trade"), (
        "the pair reported must be the two rows that share the chain, not the "
        "poll row that happened to be written between them"
    )
    assert record["max_delta_ns"] == 5 * SEC
    assert "interleave_inversion_poller" not in symbol


def test_the_shared_chain_cursor_reaches_across_a_snapshot_too(tmp_path):
    """The cursor is about the chain, not about the poller.

    A REST snapshot skips the socket hop, so the pair it makes with a WS frame
    has a bound; two WS frames have none, whichever second producer wrote a row
    between them. Compared line by line the pair here is depthSnapshot->trade,
    500ms apart and comfortably inside the socket hop's bound — yellow. The pair
    that exists is bookTicker->trade, and it shares every hop.
    """
    d = tmp_path / "um-launder-snapshot"
    d.mkdir()
    base = ns(0, nanos=100_000_000)
    write_gz(
        d / f"btcusdt_{DAY}.gz",
        [
            (base, um_book_ticker("BTCUSDT", base)),
            (base, um_depth("BTCUSDT", base, u=100, pu=99)),
            (base + 500 * MS, um_book_ticker("BTCUSDT", base + 500 * MS, u=2)),
            (base + 500 * MS, um_depth_snapshot("BTCUSDT", base + 500 * MS)),
            (base, um_trade("BTCUSDT", base)),  # 500ms behind the tick above
        ],
    )
    write_meta(
        d, "binancefuturesum", DAY, [(ns(0), session_start("binancefuturesum", ["BTCUSDT"]))]
    )
    out = tmp_path / "r.json"
    code, report = run(d, out=out)

    assert code == 1
    symbol = report["venues"]["binancefuturesum"]["days"][DAY]["symbols"]["btcusdt"]
    record = symbol["interleave_excess"]
    assert (record["previous_stream"], record["stream"]) == ("bookTicker", "trade")
    assert record["max_delta_ns"] == 500 * MS
    assert symbol["interleave_inversion"] is None


def test_the_folded_record_names_the_worst_occurrence_not_the_first(tmp_path):
    """One record per class, and the class folds magnitudes — so it has to
    anchor on the one the verdict is made of.

    Anchored on the first occurrence, the 2026-07-29 report read
    `worst 1.034s; first at line 741: depthUpdate <ts> -> premiumIndex <ts>`,
    where those two stamps are 119us apart at 00:00:15 and the 1.034s event is
    at 13:30:03 on line 1069889 — a sentence whose number and whose location
    come from different events, and no record of the incident anywhere in the
    report.
    """
    d = tmp_path / "um-worst-anchor"
    d.mkdir()
    base = ns(0, nanos=100_000_000)
    small, big = 119_473, 1_034_729_563
    write_gz(
        d / f"btcusdt_{DAY}.gz",
        [
            (base, um_book_ticker("BTCUSDT", base)),
            (base, um_trade("BTCUSDT", base)),
            (base, um_depth("BTCUSDT", base, u=100, pu=99)),
            (ns(10), um_premium_index("BTCUSDT", ns(10))),
            (ns(10) - small, um_book_ticker("BTCUSDT", ns(10) - small, u=2)),
            (ns(20), um_premium_index("BTCUSDT", ns(20))),
            (ns(20) - big, um_book_ticker("BTCUSDT", ns(20) - big, u=3)),
            (ns(21), um_book_ticker("BTCUSDT", ns(21), u=4)),
        ],
    )
    write_meta(
        d, "binancefuturesum", DAY, [(ns(0), session_start("binancefuturesum", ["BTCUSDT"]))]
    )
    out = tmp_path / "r.json"
    code, report = run(d, out=out)

    assert code == 0, issues_of(report, "binancefuturesum")
    symbol = report["venues"]["binancefuturesum"]["days"][DAY]["symbols"]["btcusdt"]
    record = symbol["interleave_inversion_poller"]
    assert record["violations"] == 2
    assert record["max_delta_ns"] == big
    assert record["line"] == 7, "the line of the worst pair, not of the first"
    assert record["previous_local_ts"] == ns(20)
    assert record["local_ts"] == ns(20) - big
    detail = next(
        i
        for i in issues_of(report, "binancefuturesum")
        if i["check"] == "interleave_inversion_poller"
    )["detail"]
    assert "worst 1.034s at line 7" in detail
    assert "first at line" not in detail, (
        "the number and the location have to come from the same event"
    )


def test_the_pollers_exemption_does_not_reach_the_depth_snapshot(tmp_path):
    """One file, two second producers, the same magnitude, two verdicts.

    The snapshot is detached but it sends on `writer_tx` — the *same* hop the
    parser feeds — so the two rows still meet in one FIFO and only the socket
    hop separates them. That is a bound made of one queue and a measured rate,
    and it has never gone red on a healthy recording (worst observed 134us,
    ethusdt 2026-07-26). The poller has a hop of its own and no shared FIFO at
    all. Reading the 2026-07-30 exemption as "REST rows are exempt" instead of
    "the row a poll overtook is" would take the red off the snapshot too, and
    nothing measured asks for that.
    """
    d = tmp_path / "um-both-producers"
    d.mkdir()
    over = qr.CROSS_STREAM_TOLERANCE_NS + SEC
    base = ns(0, nanos=100_000_000)
    tick = ns(10, nanos=164_558_903)
    write_gz(
        d / f"btcusdt_{DAY}.gz",
        [
            (base, um_book_ticker("BTCUSDT", base)),
            (base, um_trade("BTCUSDT", base)),
            (base, um_depth("BTCUSDT", base, u=100, pu=99)),
            (tick + over, um_premium_index("BTCUSDT", tick + over)),
            (tick, um_book_ticker("BTCUSDT", tick, u=2)),
            (tick + 20 * SEC + over, um_depth_snapshot("BTCUSDT", tick + 20 * SEC + over)),
            (tick + 20 * SEC, um_book_ticker("BTCUSDT", tick + 20 * SEC, u=3)),
        ],
    )
    write_meta(
        d, "binancefuturesum", DAY, [(ns(0), session_start("binancefuturesum", ["BTCUSDT"]))]
    )
    out = tmp_path / "r.json"
    code, report = run(d, out=out)

    assert code == 1
    symbol = report["venues"]["binancefuturesum"]["days"][DAY]["symbols"]["btcusdt"]
    assert symbol["interleave_excess"]["previous_stream"] == "depthSnapshot"
    assert symbol["interleave_excess"]["max_delta_ns"] == over
    assert symbol["interleave_inversion_poller"]["previous_stream"] == "premiumIndex"
    assert symbol["interleave_inversion_poller"]["max_delta_ns"] == over
    assert "interleave_excess" in checks_of(report, "binancefuturesum", severity="red")
    assert "interleave_inversion_poller" in checks_of(
        report, "binancefuturesum", severity="yellow"
    )


def um_poll_against_snapshot(tmp_path, name, poll_first, delta_ns):
    """A poll and a REST depth snapshot written out of order by `delta_ns`.

    The two share no queue at all — one crosses `poller->writer`, the other
    `parser->writer` — so the only thing the pair can be read by is which of
    them was written second, i.e. which one waited.
    """
    d = tmp_path / name
    d.mkdir()
    base = ns(0, nanos=100_000_000)
    late = ns(30, nanos=100_000_000)
    early = late - delta_ns
    first, second = (
        (um_premium_index("BTCUSDT", late), um_depth_snapshot("BTCUSDT", early))
        if poll_first
        else (um_depth_snapshot("BTCUSDT", late), um_premium_index("BTCUSDT", early))
    )
    write_gz(
        d / f"btcusdt_{DAY}.gz",
        [
            (base, um_book_ticker("BTCUSDT", base)),
            (base, um_trade("BTCUSDT", base)),
            (base, um_depth("BTCUSDT", base, u=100, pu=99)),
            (late, first),
            (early, second),
        ],
    )
    write_meta(
        d, "binancefuturesum", DAY, [(ns(0), session_start("binancefuturesum", ["BTCUSDT"]))]
    )
    return d


def test_a_snapshot_written_behind_a_poll_has_no_bound_either(tmp_path):
    """The poll went first, so the snapshot is the row that waited.

    It waited in `parser->writer`, the hop the poll skipped entirely, and that
    hop drains at the speed of blocking gzip I/O — the same unmeasured rate that
    takes the bound off a WS frame in this position. So the class follows the
    direction and not the streams: a snapshot behind a poll is the poller class
    for exactly the reason a book tick behind a poll is.
    """
    d = um_poll_against_snapshot(tmp_path, "um-snapshot-behind-poll", True, 20 * SEC)
    out = tmp_path / "r.json"
    code, report = run(d, out=out)

    assert code == 0, issues_of(report, "binancefuturesum")
    symbol = report["venues"]["binancefuturesum"]["days"][DAY]["symbols"]["btcusdt"]
    assert symbol["interleave_excess"] is None
    assert symbol["interleave_inversion"] is None
    record = symbol["interleave_inversion_poller"]
    assert record["previous_stream"] == "premiumIndex"
    assert record["stream"] == "depthSnapshot"
    assert record["max_delta_ns"] == 20 * SEC
    detail = next(
        i
        for i in issues_of(report, "binancefuturesum")
        if i["check"] == "interleave_inversion_poller"
    )["detail"]
    assert "poller->writer" in detail, "the mechanism named must be the one that decided"


@pytest.mark.parametrize(
    "delta_ns,expected_code,expected_check",
    [
        (3 * MS, 0, "interleave_inversion"),
        (qr.POLLER_HOP_CEILING_NS, 0, "interleave_inversion"),
        (20 * SEC, 1, "interleave_excess"),
    ],
)
def test_a_poll_written_behind_a_snapshot_keeps_its_bound(
    tmp_path, delta_ns, expected_code, expected_check
):
    """The same two streams the other way round, and it is a different question.

    Here the poll is the row that waited, and `poller->writer` is the only queue
    it can have waited in — the snapshot's own hop cannot hold it, since a FIFO
    delays the rows behind it and this row is not in it. So the pair keeps a
    bound in this direction, and a 20s wait on a hop that carries a dozen
    elements once every ten seconds is a defect, not an interleave.
    """
    d = um_poll_against_snapshot(
        tmp_path, f"um-poll-behind-snapshot-{delta_ns}", False, delta_ns
    )
    out = tmp_path / f"r-{delta_ns}.json"
    code, report = run(d, out=out)

    assert code == expected_code, issues_of(report, "binancefuturesum")
    symbol = report["venues"]["binancefuturesum"]["days"][DAY]["symbols"]["btcusdt"]
    assert "interleave_inversion_poller" not in symbol
    record = symbol[expected_check]
    assert record["previous_stream"] == "depthSnapshot"
    assert record["stream"] == "premiumIndex"
    assert record["max_delta_ns"] == delta_ns


def test_the_poller_finding_explains_why_no_bound_could_hold(tmp_path):
    """The text is the whole finding: nothing downstream acts on a yellow.

    It has to say which hand-off the row took (the thing that makes this class
    a class), that the recording is intact rather than merely tolerated, and
    where to look if the number is large — the same shape the interleave text
    has always had, which an operator reads on a red day.
    """
    d = um_poller_day(tmp_path, "um-poller-detail", 1_034_729_563)
    out = tmp_path / "r.json"
    _, report = run(d, out=out)
    detail = next(
        i
        for i in issues_of(report, "binancefuturesum")
        if i["check"] == "interleave_inversion_poller"
    )["detail"]

    assert "poller->writer" in detail, "the hop is what defines this class"
    # Formatted by the same `fmt_short` every other duration in the report goes
    # through; the exact nanoseconds are in the JSON, where a consumer reads them.
    assert "1.034s" in detail
    # Why it is not red, and what it is not claiming.
    assert "no bound" in detail
    assert "u/pu" in detail, "where a lost frame would show instead"
    assert "queue_overflow" in detail, "what to look at when the number is large"
    # The sentence must not still be reciting the bound it no longer applies.
    assert "beyond the" not in detail


def test_the_poller_still_has_a_hand_off_of_its_own(tmp_path):
    """The model reads the collector; if the collector moves, this must fail.

    `interleave_inversion_poller` exists because `premiumIndex` rows reach the
    writer on `queue::POLLER_HOP` while everything else arrives on
    `queue::WRITER_HOP`. Merge the two — hand the poller a clone of `writer_tx`
    — and the pair is back to sharing a FIFO, the socket hop is all that
    separates them again, and the bound this exemption removed is the right one
    once more. Reading the Rust here is what makes that a failing test rather
    than a stale comment.
    """
    queue_rs = (Path(__file__).resolve().parent.parent / "src" / "queue.rs").read_text()

    def hop_name(const):
        match = re.search(rf'pub const {const}: &str = "([^"]+)";', queue_rs)
        assert match, f"{const} is no longer a &str constant of collector/src/queue.rs"
        return match.group(1)

    writer_hop, poller_hop = hop_name("WRITER_HOP"), hop_name("POLLER_HOP")
    assert writer_hop != poller_hop
    assert qr._SECOND_PRODUCER["depthSnapshot"].hop == writer_hop
    assert qr._SECOND_PRODUCER["premiumIndex"].hop == poller_hop

    # And the poller really is handed that hop, rather than a clone of the
    # writer's: `main` claims `POLLER_HOP` for one backend and passes it in.
    main_rs = (Path(__file__).resolve().parent.parent / "src" / "main.rs").read_text()
    assert "queue::bounded(POLLER_HOP" in main_rs
    um_rs = (
        Path(__file__).resolve().parent.parent / "src" / "binancefuturesum" / "mod.rs"
    ).read_text()
    assert re.search(
        r"pub async fn run_collection\((?:[^)]*\n)*?[^)]*writer_tx: Tx<Record>,\s*"
        r"poller_tx: Tx<Record>,\s*\)",
        um_rs,
    ), "binancefuturesum::run_collection no longer takes a poller hop of its own"


def collector_src(*parts):
    """A file of `collector/src/`, read as text. The model mirrors the Rust; the
    tests below are what make the Rust fail this file when it moves."""
    return (Path(__file__).resolve().parent.parent / "src" / Path(*parts)).read_text()


def test_extended_still_opens_one_socket_per_channel(tmp_path):
    """The fan-out exemption is a claim about the backend. Read the backend.

    `fans_out_per_channel("extended")` downgrades a cross-stream inversion from
    red to a bounded yellow, and the only thing that entitles it to is that
    Extended really does spawn one socket task per channel. Refactor the backend
    onto one multiplexed socket — which is what every other venue here does —
    and the exemption becomes a hole that tolerates a real single-reader defect
    up to a full second, silently. This test is what turns that into a failure.

    Pinned on substrings rather than on line numbers or on the whole statement:
    the load-bearing facts are that the spawn is per URL and that each stream
    class has a URL of its own, and both survive ordinary edits around them.
    """
    http = collector_src("extended", "http.rs")
    assert re.search(
        r"for url in urls \{(?:.|\n)*?"
        r"tokio::spawn\(keep_connection_one\(url, ws_tx\.clone\(\)\)\)",
        http,
    ), (
        "extended::keep_connections no longer spawns one task per channel URL; "
        "if the backend now multiplexes, drop 'extended' from "
        "_FANS_OUT_PER_CHANNEL — its cross-stream inversions are single-reader "
        "defects again"
    )
    # Each task stamps its own receive moment, which is what makes them two
    # producers rather than one reader handing frames on in order.
    assert "let recv_time = Utc::now();" in http
    assert "ws_tx.send((recv_time, text))" in http

    # And one stream class really is one socket: the model's unit of exemption
    # is the stream, so two classes sharing a URL would be one producer wearing
    # two names.
    mod = collector_src("extended", "mod.rs")
    for path in ('/orderbooks"', "/publicTrades/{}", "/funding/{}", "/prices/mark/{}"):
        assert path in mod, (
            f"{path} is no longer a channel URL of its own; the fan-out "
            f"exemption assumes one socket per stream class"
        )


def test_paradex_still_multiplexes_every_channel_onto_one_socket(tmp_path):
    """The converse pin: a single-reader venue must not drift into the exemption.

    Paradex is red at a nanosecond between two streams because one reader stamps
    and queues every frame of a symbol file — one `keep_connection` over
    `CHANNELS.to_vec()` to one `WS_URL` (`collector/src/paradex/mod.rs`). Should
    it ever fan out the way Extended does, that red becomes a false one and the
    venue needs adding to `_FANS_OUT_PER_CHANNEL`; failing here is how that gets
    decided rather than endured.
    """
    mod = collector_src("paradex", "mod.rs")
    assert "keep_connection(CHANNELS.to_vec(), markets, ws_tx)" in mod, (
        "paradex no longer hands every channel to one connection; if it now "
        "opens a socket per channel, add it to _FANS_OUT_PER_CHANNEL"
    )
    assert "keep_connection_one" not in mod
    assert len(re.findall(r"tokio::spawn\(", mod)) == 0, (
        "paradex::mod.rs spawns nothing today; a new spawned producer here is "
        "exactly what the single-reader red assumes does not exist"
    )
    assert qr.fans_out_per_channel("paradex") is False

    http = collector_src("paradex", "http.rs")
    assert "connect(WS_URL, subscriptions, ws_tx.clone(), &mut connected_at)" in http, (
        "paradex no longer dials one URL with all its subscriptions"
    )


def test_the_socket_hop_the_fan_out_shares_is_the_one_the_rust_names(tmp_path):
    """`WS_HOP` is mirrored, so it is pinned like the other two hop names.

    The fan-out mechanism sentence names this hop as the one both channel
    sockets DO share — which is the reason their disagreement is bounded at all.
    A rename in `queue.rs` would leave the report explaining a hand-off that no
    longer exists by that name.
    """
    queue_rs = collector_src("queue.rs")

    def hop_name(const):
        match = re.search(rf'pub const {const}: &str = "([^"]+)";', queue_rs)
        assert match, f"{const} is no longer a &str constant of collector/src/queue.rs"
        return match.group(1)

    assert qr.WS_HOP == hop_name("WS_HOP")
    assert qr.WS_HOP != qr.WRITER_HOP != qr.POLLER_HOP
    assert qr._FAN_OUT_PRODUCER.hop == qr.WS_HOP
    # The fan-out producer must not look like the poller's: that hop is what
    # `crosses_a_hand_off_of_its_own` selects the unbounded yellow on.
    assert not qr.crosses_a_hand_off_of_its_own("orderbook")


#: The per-symbol JSON key set a venue with no poller has always had, in order.
#:
#: Frozen from the report `quality_report.py` produced before the poller class
#: existed (`git show HEAD~:...`, regated over the real Hyperliquid recording of
#: 2026-07-28). It is a literal rather than a computed list on purpose: the
#: point is to fail when the schema moves, and a list derived from the module
#: would move with it.
NO_POLLER_SYMBOL_KEYS = [
    "file",
    "lines",
    "truncated",
    "malformed_lines",
    "unclassified_frames",
    "monotonic_violation",
    "interleave_inversion",
    "interleave_excess",
    "sequence_breaks",
    "sequence_break_examples",
    "streams",
    "missing_required",
    "missing_optional",
    "missing_informational",
    "coverage",
]


def test_a_venue_with_no_poller_gets_the_report_it_always_got(tmp_path, capsys):
    """The poller class must be invisible where no poller runs.

    Hyperliquid, Bybit and Lighter have one WS reader and no second producer of
    any kind, so `interleave_inversion_poller` cannot fire on them — and a
    finding that cannot fire must not change their report either. Both ways it
    leaked when this class was added were unrelated to the model and cost the
    same thing: a regate over the real 2026-07-28 Hyperliquid recording differed
    from the previous release in every symbol of the JSON (an unconditional
    `"interleave_inversion_poller": null`) and in every issue line of the text
    (the check-name column widened from 19 to 27 to fit the new name).

    Neither is caught by asserting on findings, because neither is one. So this
    asserts on the *shape*: the key list, verbatim and in order, and the column
    an issue line puts its detail in.
    """
    d = hl_dir(tmp_path)
    # One yellow, so there is an issue row to measure the column on.
    write_gz(
        d / f"btc_{DAY}.gz",
        [
            (ns(0), hl_trade("BTC", ns(0))),
            (ns(0), hl_l2("BTC", ns(0))),
            (ns(0), hl_l2("BTC", ns(0), fast=True)),
            (ns(1), hl_bbo("BTC", ns(1))),
            (ns(61), hl_bbo("BTC", ns(61))),
            (ns(61), hl_l2("BTC", ns(61), fast=True)),
        ],
    )

    out = tmp_path / "r.json"
    code, report = run(d, out=out)
    assert code == 0

    symbol = report["venues"]["hyperliquid"]["days"][DAY]["symbols"]["btc"]
    assert list(symbol) == NO_POLLER_SYMBOL_KEYS

    text = capsys.readouterr().out
    row = next(line for line in text.splitlines() if "cadence_gap" in line)
    assert row.startswith("     [yellow] cadence_gap         btc/bbo:"), (
        "the check-name column is shared by every venue; widening it for a "
        "Binance-only finding rewrites every line of every other venue's report"
    )


def test_a_venue_with_a_poller_carries_the_key_where_the_class_fires(tmp_path):
    """The other half: absent is only ever "this did not happen here".

    A key that appears in some binancefuturesum files and not others would be a
    schema nobody could read. What decides it is the finding, not the venue —
    which is also what makes it invisible above, since the class cannot fire
    where no poller writes.
    """
    d = tmp_path / "um-poller-key"
    d.mkdir()
    poll, late = ns(10), ns(10) - 2 * SEC
    write_gz(
        d / f"btcusdt_{DAY}.gz",
        [
            (ns(0), um_book_ticker("BTCUSDT", ns(0))),
            (ns(0), um_trade("BTCUSDT", ns(0))),
            (ns(0), um_depth("BTCUSDT", ns(0), u=100, pu=99)),
            (poll, um_premium_index("BTCUSDT", poll)),
            (late, um_book_ticker("BTCUSDT", late, u=2)),
        ],
    )
    write_meta(d, "binancefuturesum", DAY, [(ns(0), session_start("binancefuturesum", ["BTCUSDT"]))])

    out = tmp_path / "r.json"
    code, report = run(d, out=out)
    assert code == 0
    symbol = report["venues"]["binancefuturesum"]["days"][DAY]["symbols"]["btcusdt"]
    assert symbol["interleave_inversion_poller"]["max_delta_ns"] == 2 * SEC
    # And it sits between the two it belongs with, not appended after them.
    keys = [k for k in symbol if k.startswith("interleave_")]
    assert keys == ["interleave_inversion", "interleave_inversion_poller", "interleave_excess"]


REAL_DAY = "20260729"
FIXTURE = Path(__file__).resolve().parent / "testdata" / "quality_report"


def test_the_real_1330_burst_is_yellow_and_intact(tmp_path):
    """301 lines cut verbatim out of the recording the gate got wrong.

    `tiausdt_20260729.gz`, lines 1069700-1070000 of the real file, around
    2026-07-29T13:30:03 UTC: the US-session open burst. One `premiumIndex` row
    stamped 13:30:03.059903387 sits in front of a book tick stamped
    13:30:02.025173824 — 1.034729563s of `local_ts` going backwards between two
    streams, which the gate called `interleave_excess` and red, in this and
    twelve other symbol files at the same instant.

    Nothing was lost: the `pu` chain steps straight across the inversion
    (…241997 -> …241997) and the report finds no sequence break in the window.
    That is the fact the verdict has to agree with.
    """
    d = tmp_path / "binancefuturesum-b"
    d.mkdir()
    shutil.copy(FIXTURE / f"tiausdt_{REAL_DAY}.gz", d / f"tiausdt_{REAL_DAY}.gz")
    # The recording's own sidecar is the whole day (~1 MB); only the symbol list
    # matters here, and the market data is what the fixture is for.
    write_meta(
        d,
        "binancefuturesum",
        REAL_DAY,
        [(1_785_331_801_374_026_979, session_start("binancefuturesum", ["TIAUSDT"]))],
    )

    out = tmp_path / "r.json"
    code, report = run(d, day=REAL_DAY, out=out)

    assert code == 0, issues_of(report, "binancefuturesum", day=REAL_DAY)
    day = report["venues"]["binancefuturesum"]["days"][REAL_DAY]
    assert day["verdict"] == "yellow"
    symbol = day["symbols"]["tiausdt"]
    assert symbol["interleave_excess"] is None
    assert symbol["monotonic_violation"] is None
    assert not any(symbol["sequence_breaks"].values()), "nothing was lost behind the burst"
    record = symbol["interleave_inversion_poller"]
    assert record["previous_stream"] == "premiumIndex"
    assert record["stream"] == "bookTicker"
    assert record["previous_local_ts"] == 1_785_331_803_059_903_387
    assert record["local_ts"] == 1_785_331_802_025_173_824
    assert record["max_delta_ns"] == 1_034_729_563
    assert record["violations"] == 1, (
        "one inversion in 301 lines; a fixture that grew a second one would be "
        "measuring something other than the incident"
    )


def test_the_real_recordings_poll_late_inversions_stay_yellow(tmp_path):
    """The other direction, also cut verbatim from the recording.

    `seiusdt_20260729.gz`, lines 545900-546200, around 09:56:02 UTC: a
    `premiumIndex` row stamped 09:56:02.742099774 written *after* a book tick
    stamped 09:56:02.743982641 — 1.882867ms the other way, and the worst of the
    38 such inversions in that whole symbol-day (tiausdt's worst is 3.128ms).

    This is the measurement the kept bound rests on. Keeping the ceiling on this
    direction has to cost the real recording nothing, or the fix would be the
    2026-07-29 failure again with the sign flipped — and 1.9ms against a 1.000s
    ceiling is the margin that says so.
    """
    d = tmp_path / "binancefuturesum-b"
    d.mkdir()
    shutil.copy(FIXTURE / f"seiusdt_{REAL_DAY}.gz", d / f"seiusdt_{REAL_DAY}.gz")
    write_meta(
        d,
        "binancefuturesum",
        REAL_DAY,
        [(1_785_318_960_080_451_082, session_start("binancefuturesum", ["SEIUSDT"]))],
    )

    out = tmp_path / "r.json"
    code, report = run(d, day=REAL_DAY, out=out)

    assert code == 0, issues_of(report, "binancefuturesum", day=REAL_DAY)
    symbol = report["venues"]["binancefuturesum"]["days"][REAL_DAY]["symbols"]["seiusdt"]
    assert symbol["interleave_excess"] is None, "the ceiling must not fire on a real day"
    assert "interleave_inversion_poller" not in symbol
    assert symbol["monotonic_violation"] is None
    record = symbol["interleave_inversion"]
    assert record["previous_stream"] == "bookTicker"
    assert record["stream"] == "premiumIndex"
    assert record["previous_local_ts"] == 1_785_318_962_743_982_641
    assert record["local_ts"] == 1_785_318_962_742_099_774
    assert record["max_delta_ns"] == 1_882_867
    assert record["violations"] == 1
    assert record["max_delta_ns"] * 300 < qr.POLLER_HOP_CEILING_NS, (
        "the ceiling is kept because it is orders of magnitude over what the "
        "poller's own hand-off does; if that stops being true, measure the hop"
    )
    detail = next(
        i
        for i in issues_of(report, "binancefuturesum", day=REAL_DAY)
        if i["check"] == "interleave_inversion"
    )["detail"]
    assert "the poll was written last" in detail


def test_a_premium_index_stream_going_backwards_within_itself_is_red(tmp_path):
    """One poller, one timer, one hand-off: within the stream there is no race.

    Two polls cannot overtake each other — the loop awaits each one before the
    next tick — so a step backwards here is a clock or two recordings in one
    file, the same as every other stream this report holds to that rule.
    """
    d, _ = premium_index_day(tmp_path, "um-pi-backwards", 5 * SEC)
    path = next(d.glob(f"*_{DAY}.gz"))
    late = ns(5) - MS
    write_gz(path, [(late, um_premium_index("BTCUSDT", late))], append=True)
    out = tmp_path / "r.json"
    code, report = run(d, out=out)
    assert code == 1
    assert "monotonicity" in checks_of(report, "binancefuturesum", severity="red")


def test_premium_index_frames_leave_the_depth_sequence_chain_alone(tmp_path):
    """No `u`, no `pu`, no chain of its own — and no effect on the one that exists.

    The element carries `symbol` rather than `s`, so a tracker keyed on the
    symbol instead of on the stream would see every poll break the depth chain
    and the report would claim a lost-frame gap every ten seconds.
    """
    d = tmp_path / "um-pi-sequence"
    d.mkdir()
    write_gz(
        d / f"btcusdt_{DAY}.gz",
        [
            (ns(0), um_book_ticker("BTCUSDT", ns(0))),
            (ns(0), um_depth("BTCUSDT", ns(0), u=100, pu=99)),
            (ns(1), um_premium_index("BTCUSDT", ns(1))),
            (ns(2), um_depth("BTCUSDT", ns(2), u=101, pu=100)),
        ],
    )
    write_meta(
        d, "binancefuturesum", DAY, [(ns(0), session_start("binancefuturesum", ["BTCUSDT"]))]
    )
    out = tmp_path / "r.json"
    code, report = run(d, out=out)
    assert code == 0, issues_of(report, "binancefuturesum")
    assert "sequence_gap" not in checks_of(report, "binancefuturesum")
    sym = report["venues"]["binancefuturesum"]["days"][DAY]["symbols"]["btcusdt"]
    assert set(sym["sequence_breaks"]) == {"depthUpdate"}


def test_only_the_usd_m_backend_expects_a_premium_index_feed():
    """COIN-M has no poller: its mark-price stream is alive and still recorded.

    Listing `premiumIndex` for COIN-M would cost nothing in verdicts — an absent
    informational stream raises nothing — but `missing_informational` is read as
    a statement about what the recording could have contained, and for COIN-M
    the answer is "never this".
    """
    um = qr.expected_streams("mode-a-v1", "binancefuturesum", {})
    cm = qr.expected_streams("mode-a-v1", "binancefuturescm", {})

    assert "premiumIndex" in um.informational
    assert "premiumIndex" not in cm.informational
    assert "markPriceUpdate" in cm.informational, (
        "dstream.binance.com still serves it, measured 2026-07-28"
    )
    assert "markPriceUpdate" in um.informational, (
        "the routing survives the subscription being dropped; if fstream ever "
        "serves the class again the frames must be checked, not unclassified"
    )
    # Neither venue's index data is ever load-bearing for mode A.
    for expected in (um, cm):
        assert expected.required == ("bookTicker",)
        assert "premiumIndex" not in expected.required + expected.optional


def test_an_absent_premium_index_feed_is_a_fact_and_never_a_warning(tmp_path):
    """Every UM day recorded before 2026-07-28 lacks it, and no rerun can fix that."""
    um = um_dir(tmp_path)
    out = tmp_path / "r.json"
    code, report = run(um, out=out)

    assert code == 0
    assert checks_of(report, "binancefuturesum") == []
    sym = report["venues"]["binancefuturesum"]["days"][DAY]["symbols"]["btcusdt"]
    assert "premiumIndex" in sym["missing_informational"]
    assert sym["missing_optional"] == []
    assert sym["missing_required"] == []
    assert "premiumIndex" not in sym["coverage"]["required_streams"]


# --------------------------------------------------------------------------
# the collector's host gauges: clock discipline and per-symbol liveness
# --------------------------------------------------------------------------


def clock_ok(max_error_us=16_000, offset_us=-12.0):
    return {
        "_collector": "clock",
        "sync": True,
        "est_error_us": 400,
        "max_error_us": max_error_us,
        "offset_us": offset_us,
        "freq_ppm": 13.0,
    }


def clock_unsynced(max_error_us=16_000_000):
    return {
        "_collector": "clock",
        "sync": False,
        "est_error_us": max_error_us,
        "max_error_us": max_error_us,
        "offset_us": 0.0,
        "freq_ppm": 0.0,
    }


def liveness(ages, threshold_s=60):
    return {"_collector": "liveness", "threshold_s": threshold_s, "ages_s": dict(ages)}


def test_the_host_gauges_are_known_records_and_not_unclassified(tmp_path):
    """A gauge the Rust side started writing must not become a finding by itself.

    The sidecar is not classified the way a symbol file is, so nothing counts
    these as `unclassified_frame` — this pins that, because the day they *were*
    routed anywhere near the stream classifier every recording would go yellow
    for the collector doing exactly what it was asked to.
    """
    d = hl_dir(
        tmp_path,
        extra_meta=[
            (ns(60), clock_ok()),
            (ns(60), liveness({"BTC": 0})),
            (ns(120), clock_ok()),
            (ns(120), liveness({"BTC": 1})),
        ],
    )
    out = tmp_path / "r.json"
    code, report = run(d, out=out)

    assert code == 0
    assert checks_of(report, "hyperliquid") == [], (
        "a healthy day with the host gauges recorded produced findings"
    )


def test_an_unsynchronised_clock_is_a_yellow_note_on_the_day(tmp_path):
    """The 2026-07-27 incident: a host came back from a reboot undisciplined,
    recorded a full day, and the skew was only found at assembly time.

    Informational by design. The recording is not corrupt and the venue-side
    timestamps are unaffected; what the note says is that every `local_ts` in
    that window is the host's own idea of the time, so a finding inside it may
    be the clock rather than the recording.
    """
    d = hl_dir(
        tmp_path,
        extra_meta=[
            (ns(60), clock_unsynced()),
            (ns(120), clock_unsynced()),
            (ns(180), clock_ok()),
        ],
    )
    out = tmp_path / "r.json"
    code, report = run(d, out=out)

    note = next(i for i in issues_of(report, "hyperliquid") if i["check"] == "clock_unsynced")
    assert note["severity"] == "yellow"
    assert code == 0, "an unsynchronised clock must annotate, not fail the day"
    assert "2 of 3" in note["detail"], note["detail"]
    assert qr.iso(ns(60)) in note["detail"], note["detail"]
    assert qr.iso(ns(120)) in note["detail"], note["detail"]
    assert "16000000" in note["detail"].replace(",", ""), note["detail"]


def test_a_disciplined_clock_says_nothing(tmp_path):
    """The gauge is written every minute of every recording. A note on a healthy
    clock would put one on every day there is."""
    d = hl_dir(tmp_path, extra_meta=[(ns(60), clock_ok()), (ns(120), clock_ok())])
    out = tmp_path / "r.json"
    _, report = run(d, out=out)

    assert "clock_unsynced" not in checks_of(report, "hyperliquid")


def test_a_clock_the_collector_could_not_measure_is_not_read_as_unsynchronised(tmp_path):
    """`{"unsupported": true}` off Linux, and `{"error": …}` when the syscall
    failed, both carry no `sync` field at all.

    "We did not measure it" is not "it was wrong". Reading a missing `sync` as
    false would yellow every recording made on a dev machine, and a warning
    that is always there is one nobody reads.
    """
    d = hl_dir(
        tmp_path,
        extra_meta=[
            (ns(60), {"_collector": "clock", "unsupported": True, "platform": "macos"}),
            (ns(120), {"_collector": "clock", "error": "Bad address (os error 14)"}),
        ],
    )
    out = tmp_path / "r.json"
    _, report = run(d, out=out)

    assert "clock_unsynced" not in checks_of(report, "hyperliquid")


def test_an_unsynchronised_clock_on_another_day_does_not_annotate_this_one(tmp_path):
    """Sidecars are read across the whole directory, because `session_start` is
    per process. The clock note must still be scoped to the day being checked,
    or one bad night would annotate every day in the directory."""
    d = hl_dir(tmp_path, extra_meta=[(ns(86_400 + 60), clock_unsynced())])
    out = tmp_path / "r.json"
    _, report = run(d, out=out)

    assert "clock_unsynced" not in checks_of(report, "hyperliquid")


@pytest.mark.parametrize("record", [clock_ok(), clock_unsynced(), liveness({"BTC": 300})])
def test_a_host_gauge_inside_a_gap_does_not_explain_it(tmp_path, record):
    """Same rule as the disk gauge, for the same reason. These are written on a
    one-minute timer, so one lands inside every hole longer than a minute
    whatever caused it. An unsynchronised clock in particular explains nothing:
    it makes the hole's measurement doubtful, which is the opposite of
    accounting for it."""
    d = hl_gapped_day(tmp_path, [(ns(30), record)])
    out = tmp_path / "r.json"
    _, report = run(d, out=out)

    gaps = report["venues"]["hyperliquid"]["days"][DAY]["symbols"]["btc"]["streams"]["bbo"]["gaps"]
    assert gaps[0]["explained_by"] is None


def test_every_host_gauge_is_named_and_none_of_them_explains_anything():
    """`_GAUGES` is documentation with a test on it: the whole set of
    `_collector` records that are measurements rather than events, and the
    assertion that not one of them is allowed to account for a gap.

    `cpu` is the sharpest case for the second half. It is the gauge most likely
    to be *about* a gap — steal at 80% is exactly why the writer fell behind —
    and it still must not annotate one: it is written every minute regardless,
    so it lands inside every hole longer than a minute whether or not the host
    was throttled. The evidence is the number in the record, read by whoever is
    investigating; an automatic "explained by cpu" would close the
    investigation instead of informing it.
    """
    for gauge in ("disk", "clock", "cpu", "liveness", "universe"):
        assert gauge in qr._GAUGES
        assert gauge not in qr._EXPLANATORY


def test_the_note_reports_the_worst_error_inside_the_unsynchronised_window(tmp_path):
    """The number in the note has to belong to the window the note is about.

    `max_error` grows between every poll on a perfectly healthy clock, so a day
    almost always holds a larger one somewhere outside the unsynchronised
    stretch. Reporting the day's worst inside a sentence about that stretch
    attributes an ordinary excursion to a fault, in a note whose whole purpose
    is to be the evidence a dataset manifest quotes.
    """
    d = hl_dir(
        tmp_path,
        extra_meta=[
            (ns(60), clock_unsynced(max_error_us=250_000)),
            (ns(120), clock_ok(max_error_us=9_000_000)),
        ],
    )
    out = tmp_path / "r.json"
    _, report = run(d, out=out)

    note = next(i for i in issues_of(report, "hyperliquid") if i["check"] == "clock_unsynced")
    assert "250000" in note["detail"].replace(",", ""), note["detail"]
    assert "9000000" not in note["detail"].replace(",", ""), (
        f"an excursion measured while the clock was synchronised was reported "
        f"as the worst error of the unsynchronised window: {note['detail']}"
    )


def test_samples_the_collector_could_not_measure_are_not_counted_as_healthy(tmp_path):
    """`2 of 4` reads as two good samples when the other two were never
    measured at all.

    `{"unsupported": true}` and `{"error": …}` carry no `sync`, and the
    denominator is what tells an operator how much of the day this note covers.
    Counting an unmeasured sample there understates the fault.
    """
    d = hl_dir(
        tmp_path,
        extra_meta=[
            (ns(60), clock_unsynced()),
            (ns(120), clock_unsynced()),
            (ns(180), {"_collector": "clock", "unsupported": True, "platform": "macos"}),
            (ns(240), {"_collector": "clock", "error": "Bad address (os error 14)"}),
        ],
    )
    out = tmp_path / "r.json"
    _, report = run(d, out=out)

    note = next(i for i in issues_of(report, "hyperliquid") if i["check"] == "clock_unsynced")
    assert "2 of 2" in note["detail"], note["detail"]
