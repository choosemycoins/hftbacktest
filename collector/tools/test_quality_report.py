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
    assert record["delta_ns"] == 134_021
    assert record["violations"] == 1
    assert record["max_delta_ns"] == 134_021

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
    text = qr._SECOND_PRODUCER[mechanism]
    assert not text.rstrip().endswith((" in", " the", " of", " through", ",")), text
    # Rendered exactly as the issue renders it, so a trailing preposition shows.
    assert "queue in." not in f"{text}. This is"


def test_a_snapshot_overtake_the_full_depth_of_the_socket_hop_is_not_red(tmp_path):
    """Incident A and incident B are the same day.

    The socket hop holds WS frames the REST snapshot skips, so the largest
    honest overtake is that hop's whole occupancy: `WS_QUEUE_CAPACITY` /
    `burst::PEAK_MSG_PER_S` = 4096 / 20 000 = 204.8ms. A burst is also what
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
    write_gz(d / f"btcusdt_{DAY}.gz", um_interleaved(600 * MS))
    write_meta(d, "binancefuturesum", DAY,
               [(ns(0), session_start("binancefuturesum", ["BTCUSDT"]))])

    out = tmp_path / "r.json"
    code, report = run(d, out=out)
    assert code == 1
    assert "interleave_excess" in checks_of(report, "binancefuturesum", severity="red")
    symbol = report["venues"]["binancefuturesum"]["days"][DAY]["symbols"]["btcusdt"]
    assert symbol["interleave_inversion"] is None
    assert symbol["interleave_excess"]["delta_ns"] == 600 * MS


@pytest.mark.parametrize(
    "delta_ns,expected_code,expected_check",
    [
        (250 * MS, 0, "interleave_inversion"),
        (250 * MS + 1, 1, "interleave_excess"),
    ],
)
def test_the_interleave_bound_is_pinned_to_the_nanosecond(
    tmp_path, delta_ns, expected_code, expected_check
):
    """Widening the tolerance must break a test, not pass silently."""
    assert qr.CROSS_STREAM_TOLERANCE_NS == 250 * MS
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


@pytest.mark.parametrize("delta_ns", [1, 5 * MS, 200 * MS])
def test_a_cross_stream_inversion_with_no_second_producer_is_red_at_any_size(
    tmp_path, delta_ns
):
    """The tolerance is a property of a mechanism, not of a duration.

    Hyperliquid has one WS reader stamping and routing every frame of a symbol
    file (`hyperliquid/mod.rs`), so write order IS receive order and there is
    nothing for the tolerance to excuse. Applying it venue-agnostically
    downgraded a real defect to yellow and printed a self-contradicting reason.
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


def test_a_whole_file_out_of_order_is_red_not_a_tolerated_interleave(tmp_path):
    """One 134us interleave per five million lines is the measured shape. A
    stream of them is two recordings in one file, and stays red."""
    d = tmp_path / "um-shuffled"
    d.mkdir()
    base = ns(0, nanos=500_000_000)
    recs = [(base, um_book_ticker("BTCUSDT", base))]
    for i in range(1, 6):
        # Each snapshot is stamped a full second after the bookTicker written
        # next to it: far beyond any hand-off skew.
        recs.append((base + i * SEC, um_depth_snapshot("BTCUSDT", base + i * SEC, 1000 + i)))
        recs.append((base + i * MS, um_book_ticker("BTCUSDT", base + i * MS)))
    write_gz(d / f"btcusdt_{DAY}.gz", recs)
    write_meta(d, "binancefuturesum", DAY,
               [(ns(0), session_start("binancefuturesum", ["BTCUSDT"]))])
    out = tmp_path / "r.json"
    code, report = run(d, out=out)
    assert code == 1
    symbol = report["venues"]["binancefuturesum"]["days"][DAY]["symbols"]["btcusdt"]
    assert symbol["interleave_excess"]["violations"] == 5
    assert symbol["interleave_excess"]["max_delta_ns"] == 5 * SEC - 5 * MS


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


def test_the_premium_index_poller_is_a_second_producer(tmp_path):
    """It skips the socket hop, so it can legally be written ahead of WS frames.

    This is the depth-snapshot situation again and it is now the common case
    rather than the rare one: the poller hands its elements **straight to the
    writer** while every WS frame queues through the socket hop first, so a
    premium-index line stamped later can reach the file before a book tick
    stamped earlier. Eight thousand polls a day against a hop that holds up to
    204.8ms of frames — without an entry in `_SECOND_PRODUCER` a healthy UM
    recording would go red on `interleave_excess`, and red is a hard build
    refusal in `build_dataset.py`.
    """
    assert "premiumIndex" in qr._SECOND_PRODUCER

    d = tmp_path / "um-pi-interleave"
    d.mkdir()
    base = ns(0, nanos=100_000_000)
    write_gz(
        d / f"btcusdt_{DAY}.gz",
        [
            (base, um_book_ticker("BTCUSDT", base)),
            # The poll landed first even though the tick below was stamped
            # earlier: it never entered the socket hop.
            (base + 2 * MS, um_premium_index("BTCUSDT", base + 2 * MS)),
            (base + MS, um_book_ticker("BTCUSDT", base + MS)),
        ],
    )
    write_meta(
        d, "binancefuturesum", DAY, [(ns(0), session_start("binancefuturesum", ["BTCUSDT"]))]
    )
    out = tmp_path / "r.json"
    code, report = run(d, out=out)

    assert code == 0, issues_of(report, "binancefuturesum")
    symbol = report["venues"]["binancefuturesum"]["days"][DAY]["symbols"]["btcusdt"]
    assert symbol["interleave_excess"] is None
    assert symbol["monotonic_violation"] is None
    assert symbol["interleave_inversion"]["previous_stream"] == "premiumIndex"

    detail = next(
        i for i in issues_of(report, "binancefuturesum") if i["check"] == "interleave_inversion"
    )["detail"]
    assert "premiumIndex" in detail and "producer" in detail


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
    assertion that not one of them is allowed to account for a gap."""
    for gauge in ("disk", "clock", "liveness", "universe"):
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
