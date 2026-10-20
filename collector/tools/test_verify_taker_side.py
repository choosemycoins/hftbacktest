"""Tests for `verify_taker_side.py` — the §1.4 sign-convention check of #71.

The spec (`docs/spec-mrr-gate.md`, v0.1-final + operator decisions 11.08) is
the only source of truth. Every fixture is a synthetic tape whose geometric
classification is known by construction, so a failure names the venue reader
(or the alignment rule) that drifted, not the market.

Timestamps are realistic nanosecond receive values (~1.786e18) so that any
place letting one become a float shows up here, in the style of the sibling
suites.
"""

import gzip
import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))

import verify_taker_side as vt  # noqa: E402

#: 2026-08-10T00:00:00Z in ms — the freshest offloaded day at writing time.
T0_MS = 1_786_320_000_000
T0_US = T0_MS * 1000
RECV0 = T0_MS * 1_000_000


def write_tape(path, payloads):
    """Write `<ns receive-ts> <JSON>` lines, receive clock strictly increasing."""
    with gzip.open(path, "wt") as f:
        for i, p in enumerate(payloads):
            f.write(f"{RECV0 + i * 1_000_000} {json.dumps(p, separators=(',', ':'))}\n")
    return str(path)


# --------------------------------------------------------------------------
# hyperliquid
# --------------------------------------------------------------------------


def hl_bbo(t_ms, bid, ask):
    return {
        "channel": "bbo",
        "data": {
            "coin": "ZRO",
            "time": t_ms,
            "bbo": [{"px": str(bid), "sz": "1", "n": 1}, {"px": str(ask), "sz": "1", "n": 1}],
        },
    }


def hl_trade(t_ms, side, px, tid):
    return {
        "channel": "trades",
        "data": [
            {"coin": "ZRO", "side": side, "px": str(px), "sz": "1", "time": t_ms,
             "hash": "0x0", "tid": tid, "users": ["0xa", "0xb"]}
        ],
    }


def test_a_print_at_the_ask_is_a_taker_buy_and_at_the_bid_a_taker_sell(tmp_path):
    tape = write_tape(
        tmp_path / "hl.gz",
        [
            hl_bbo(T0_MS, 100.0, 101.0),
            hl_trade(T0_MS + 10, "B", 101.0, 1),
            hl_trade(T0_MS + 20, "A", 100.0, 2),
        ],
    )
    r = vt.verify("hyperliquid", [tape])
    assert r["crosstab"]["B"] == {"taker_bought": 1, "taker_sold": 0}
    assert r["crosstab"]["A"] == {"taker_bought": 0, "taker_sold": 1}


def test_a_print_strictly_inside_the_spread_is_ambiguous_not_evidence(tmp_path):
    tape = write_tape(
        tmp_path / "hl.gz",
        [hl_bbo(T0_MS, 100.0, 101.0), hl_trade(T0_MS + 10, "B", 100.5, 1)],
    )
    r = vt.verify("hyperliquid", [tape])
    assert r["prints_ambiguous_inside"] == 1
    assert r["prints_classified"] == 0


def test_the_quote_in_force_is_strictly_earlier_by_the_exchange_clock(tmp_path):
    # The frame stamped at the print's own ms must not classify it; the
    # strictly-earlier frame (where 101 was still the ask) must.
    tape = write_tape(
        tmp_path / "hl.gz",
        [
            hl_bbo(T0_MS, 100.0, 101.0),
            hl_bbo(T0_MS + 10, 101.0, 102.0),  # same ms as the print
            hl_trade(T0_MS + 10, "B", 101.0, 1),
        ],
    )
    r = vt.verify("hyperliquid", [tape])
    assert r["crosstab"]["B"] == {"taker_bought": 1, "taker_sold": 0}


def test_a_print_before_any_quote_is_counted_as_bookless(tmp_path):
    tape = write_tape(tmp_path / "hl.gz", [hl_trade(T0_MS, "B", 101.0, 1)])
    r = vt.verify("hyperliquid", [tape])
    assert r["prints_with_no_book"] == 1
    assert r["prints_classified"] == 0


def test_replayed_prints_are_counted_once_by_their_id(tmp_path):
    tape = write_tape(
        tmp_path / "hl.gz",
        [
            hl_bbo(T0_MS, 100.0, 101.0),
            hl_trade(T0_MS + 10, "B", 101.0, 7),
            hl_trade(T0_MS + 10, "B", 101.0, 7),  # resubscribe replay
        ],
    )
    r = vt.verify("hyperliquid", [tape])
    assert r["prints_deduped"] == 1
    assert r["crosstab"]["B"] == {"taker_bought": 1, "taker_sold": 0}


def test_an_unrecognised_side_is_thrown_away_and_counted(tmp_path):
    tape = write_tape(
        tmp_path / "hl.gz",
        [hl_bbo(T0_MS, 100.0, 101.0), hl_trade(T0_MS + 10, "X", 101.0, 1)],
    )
    r = vt.verify("hyperliquid", [tape])
    assert r["populations"]["unrecognised_tag"] == 1
    assert r["prints_seen"] == 0


def test_the_book_does_not_leak_across_a_file_boundary(tmp_path):
    a = write_tape(tmp_path / "a.gz", [hl_bbo(T0_MS, 100.0, 101.0)])
    b = write_tape(tmp_path / "b.gz", [hl_trade(T0_MS + 10, "B", 101.0, 1)])
    r = vt.verify("hyperliquid", [a, b])
    assert r["prints_with_no_book"] == 1


# --------------------------------------------------------------------------
# bybit
# --------------------------------------------------------------------------


def by_book(ts_ms, cts_ms, bid, ask, typ="snapshot"):
    j = {
        "topic": "orderbook.1.VVVUSDT",
        "ts": ts_ms,
        "type": typ,
        "data": {"s": "VVVUSDT", "b": [[str(bid), "1"]], "a": [[str(ask), "1"]],
                 "u": 1, "seq": 1},
    }
    if cts_ms is not None:
        j["cts"] = cts_ms
    return j


def by_trade(t_ms, side, px, i, bt=False, rpi=False):
    return {
        "topic": "publicTrade.VVVUSDT",
        "type": "snapshot",
        "ts": t_ms,
        "data": [{"T": t_ms, "s": "VVVUSDT", "S": side, "v": "1", "p": str(px),
                  "i": i, "BT": bt, "RPI": rpi, "seq": 1}],
    }


def test_bybit_uses_cts_when_nearly_every_frame_carries_it(tmp_path):
    # cts says the frame precedes the print; ts says it does not. The per-file
    # rule (>= 99% cts) must pick cts, so the print classifies.
    tape = write_tape(
        tmp_path / "by.gz",
        [by_book(T0_MS + 20, T0_MS, 100.0, 101.0),
         by_trade(T0_MS + 10, "Buy", 101.0, "id1")],
    )
    r = vt.verify("bybit", [tape])
    assert r["clock_by_file"][tape]["clock"] == "cts"
    assert r["crosstab"]["Buy"] == {"taker_bought": 1, "taker_sold": 0}


def test_bybit_falls_back_to_ts_when_cts_is_rare(tmp_path):
    frames = [by_book(T0_MS + k, None, 100.0, 101.0) for k in range(99)]
    frames.append(by_book(T0_MS + 99, T0_MS + 99, 100.0, 101.0))
    frames.append(by_trade(T0_MS + 200, "Sell", 100.0, "id1"))
    tape = write_tape(tmp_path / "by.gz", frames)
    r = vt.verify("bybit", [tape])
    assert r["clock_by_file"][tape]["clock"] == "ts"
    assert r["crosstab"]["Sell"] == {"taker_bought": 0, "taker_sold": 1}


def test_bybit_block_trades_are_excluded_and_rpi_prints_are_kept(tmp_path):
    tape = write_tape(
        tmp_path / "by.gz",
        [
            by_book(T0_MS, T0_MS, 100.0, 101.0),
            by_trade(T0_MS + 10, "Buy", 101.0, "id1", bt=True),
            by_trade(T0_MS + 20, "Buy", 101.0, "id2", rpi=True),
        ],
    )
    r = vt.verify("bybit", [tape])
    assert r["populations"]["block_trade_excluded"] == 1
    assert r["populations"]["rpi_kept"] == 1
    assert r["crosstab"]["Buy"] == {"taker_bought": 1, "taker_sold": 0}


def test_bybit_delta_frames_are_counted_not_reconstructed(tmp_path):
    tape = write_tape(
        tmp_path / "by.gz",
        [by_book(T0_MS, T0_MS, 100.0, 101.0, typ="delta"),
         by_trade(T0_MS + 10, "Buy", 101.0, "id1")],
    )
    r = vt.verify("bybit", [tape])
    assert r["populations"]["orderbook1_delta_frame"] == 1
    assert r["prints_with_no_book"] == 1  # the delta never became a quote


# --------------------------------------------------------------------------
# lighter
# --------------------------------------------------------------------------


def lt_ticker(t_us, bid, ask):
    return {
        "channel": "ticker:60",
        "last_updated_at": t_us,
        "ticker": {"s": "ZRO", "a": {"price": str(ask), "size": "1"},
                   "b": {"price": str(bid), "size": "1"},
                   "last_updated_at": t_us},
        "timestamp": t_us // 1000,
        "type": "update/ticker",
    }


def lt_trade(t_us, is_maker_ask, px, tid, liq=False):
    row = {"trade_id": tid, "is_maker_ask": is_maker_ask, "price": str(px),
           "size": "1", "timestamp": t_us // 1000, "transaction_time": t_us}
    return {
        "channel": "trade:60",
        "type": "update/trade",
        "trades": [] if liq else [row],
        "liquidation_trades": [row] if liq else [],
    }


def test_lighter_maker_ask_means_the_taker_bought(tmp_path):
    tape = write_tape(
        tmp_path / "lt.gz",
        [
            lt_ticker(T0_US, 100.0, 101.0),
            lt_trade(T0_US + 10, True, 101.0, 1),
            lt_trade(T0_US + 20, False, 100.0, 2),
        ],
    )
    r = vt.verify("lighter", [tape])
    assert r["crosstab"]["maker_ask"] == {"taker_bought": 1, "taker_sold": 0}
    assert r["crosstab"]["maker_bid"] == {"taker_bought": 0, "taker_sold": 1}


def test_lighter_clocks_are_microseconds_on_both_legs(tmp_path):
    # 63 ms apart — the measured `timestamp` vs `transaction_time` skew of
    # §10.3. Read either leg as ms and the strictly-less alignment breaks.
    tape = write_tape(
        tmp_path / "lt.gz",
        [
            lt_ticker(T0_US, 100.0, 101.0),
            lt_ticker(T0_US + 63_000, 101.0, 102.0),
            lt_trade(T0_US + 63_001, True, 102.0, 1),
        ],
    )
    r = vt.verify("lighter", [tape])
    # against the newer book (101/102) this is a buy at the ask; against the
    # stale one it would be ambiguous-outside
    assert r["crosstab"]["maker_ask"] == {"taker_bought": 1, "taker_sold": 0}


def test_lighter_liquidations_stay_in_the_population_and_are_counted(tmp_path):
    tape = write_tape(
        tmp_path / "lt.gz",
        [lt_ticker(T0_US, 100.0, 101.0), lt_trade(T0_US + 10, True, 101.0, 1, liq=True)],
    )
    r = vt.verify("lighter", [tape])
    assert r["populations"]["liquidation_kept"] == 1
    assert r["crosstab"]["maker_ask"] == {"taker_bought": 1, "taker_sold": 0}


# --------------------------------------------------------------------------
# aster
# --------------------------------------------------------------------------


def as_book(t_ms, bid, ask):
    return {"stream": "zrousdt@bookTicker",
            "data": {"e": "bookTicker", "u": 1, "s": "ZROUSDT", "b": str(bid),
                     "B": "1", "a": str(ask), "A": "1", "T": t_ms, "E": t_ms}}


def as_trade(t_ms, m, px, tid, x="MARKET"):
    return {"stream": "zrousdt@trade",
            "data": {"e": "trade", "E": t_ms, "T": t_ms, "s": "ZROUSDT",
                     "t": tid, "p": str(px), "q": "1", "X": x, "m": m}}


def test_aster_m_false_means_the_taker_bought(tmp_path):
    tape = write_tape(
        tmp_path / "as.gz",
        [
            as_book(T0_MS, 100.0, 101.0),
            as_trade(T0_MS + 10, False, 101.0, 1),
            as_trade(T0_MS + 20, True, 100.0, 2),
        ],
    )
    r = vt.verify("aster", [tape])
    assert r["crosstab"]["m=false"] == {"taker_bought": 1, "taker_sold": 0}
    assert r["crosstab"]["m=true"] == {"taker_bought": 0, "taker_sold": 1}


def test_aster_non_market_prints_are_excluded_and_counted(tmp_path):
    tape = write_tape(
        tmp_path / "as.gz",
        [as_book(T0_MS, 100.0, 101.0),
         as_trade(T0_MS + 10, False, 101.0, 1, x="INSURANCE_FUND")],
    )
    r = vt.verify("aster", [tape])
    assert r["populations"]["non_market_excluded"] == 1
    assert r["prints_seen"] == 0


# --------------------------------------------------------------------------
# extended
# --------------------------------------------------------------------------


def ex_snapshot(ts_ms, bids, asks):
    return {"type": "SNAPSHOT",
            "data": {"t": "SNAPSHOT", "m": "VVV-USD",
                     "b": [{"q": str(q), "p": str(p)} for p, q in bids],
                     "a": [{"q": str(q), "p": str(p)} for p, q in asks]},
            "ts": ts_ms, "seq": 1}


def ex_delta(ts_ms, bids, asks):
    return {"type": "DELTA",
            "data": {"t": "DELTA", "m": "VVV-USD",
                     "b": [{"q": "0", "p": str(p), "c": str(c)} for p, c in bids],
                     "a": [{"q": "0", "p": str(p), "c": str(c)} for p, c in asks]},
            "ts": ts_ms, "seq": 2}


def ex_trade(t_ms, side, px, i):
    return {"data": [{"i": i, "m": "VVV-USD", "S": side, "tT": "TRADE",
                      "T": t_ms, "p": str(px), "q": "1"}],
            "ts": t_ms, "seq": 3}


def test_extended_book_is_rebuilt_from_snapshot_plus_deltas(tmp_path):
    # The delta removes the 101 ask (c=0) and posts 102; a buy at 102 is then
    # at the ask, which only a rebuilt book knows.
    tape = write_tape(
        tmp_path / "ex.gz",
        [
            ex_snapshot(T0_MS, [(100.0, 1)], [(101.0, 1)]),
            ex_delta(T0_MS + 5, [], [(101.0, 0), (102.0, 1)]),
            ex_trade(T0_MS + 10, "BUY", 102.0, 1),
        ],
    )
    r = vt.verify("extended", [tape])
    assert r["crosstab"]["BUY"] == {"taker_bought": 1, "taker_sold": 0}


def test_extended_deltas_before_any_snapshot_are_not_a_book(tmp_path):
    tape = write_tape(
        tmp_path / "ex.gz",
        [ex_delta(T0_MS, [(100.0, 1)], [(101.0, 1)]),
         ex_trade(T0_MS + 10, "BUY", 101.0, 1)],
    )
    r = vt.verify("extended", [tape])
    assert r["prints_with_no_book"] == 1


def test_extended_a_book_frame_not_earlier_than_the_print_is_no_evidence(tmp_path):
    tape = write_tape(
        tmp_path / "ex.gz",
        [ex_snapshot(T0_MS + 10, [(100.0, 1)], [(101.0, 1)]),
         ex_trade(T0_MS + 10, "BUY", 101.0, 1)],  # same ms, not strictly less
    )
    r = vt.verify("extended", [tape])
    assert r["prints_with_no_book"] == 1


# --------------------------------------------------------------------------
# paradex
# --------------------------------------------------------------------------


def px_bbo(t_ms, bid, ask):
    return {"jsonrpc": "2.0", "method": "subscription",
            "params": {"channel": "bbo.VVV-USD-PERP",
                       "data": {"market": "VVV-USD-PERP", "seq_no": 1,
                                "ask": str(ask), "ask_size": "1",
                                "bid": str(bid), "bid_size": "1",
                                "last_updated_at": t_ms}}}


def px_trade(t_ms, side, px, i, trade_type="FILL"):
    return {"jsonrpc": "2.0", "method": "subscription",
            "params": {"channel": "trades.VVV-USD-PERP",
                       "data": {"id": i, "market": "VVV-USD-PERP", "side": side,
                                "size": "1", "price": str(px),
                                "created_at": t_ms, "trade_type": trade_type}}}


def test_paradex_only_fill_prints_enter_the_verdict(tmp_path):
    payloads = [px_bbo(T0_MS, 100.0, 101.0)]
    payloads += [px_trade(T0_MS + 10 + k, "BUY", 101.0, f"f{k}") for k in range(3)]
    payloads += [px_trade(T0_MS + 50, "BUY", 101.0, "r1", trade_type="RPI")]
    tape = write_tape(tmp_path / "px.gz", payloads)
    r = vt.verify("paradex", [tape])
    assert r["crosstab"]["BUY/FILL"]["taker_bought"] == 3
    assert r["crosstab"]["BUY/RPI"]["taker_bought"] == 1  # diagnostics only
    assert r["prints_classified"] == 3  # RPI not in the verdict population


# --------------------------------------------------------------------------
# verdict semantics (§1.4 acceptance)
# --------------------------------------------------------------------------


def _bulk_hl_tape(tmp_path, n=600, flip=False):
    payloads = [hl_bbo(T0_MS, 100.0, 101.0)]
    for k in range(n):
        side = ("A" if flip else "B")
        payloads.append(hl_trade(T0_MS + 10 + k, side, 101.0, 1000 + k))
        other = ("B" if flip else "A")
        payloads.append(hl_trade(T0_MS + 10 + k, other, 100.0, 5000 + k))
    return write_tape(tmp_path / ("flip.gz" if flip else "ok.gz"), payloads)


def test_a_clean_tape_above_the_floor_is_verified(tmp_path):
    tape = _bulk_hl_tape(tmp_path, n=600)
    r = vt.verify("hyperliquid", [tape])
    assert r["prints_classified"] >= vt.MIN_CLASSIFIED
    assert r["P_taker_bought_given_BUY"] == 1.0
    assert r["P_taker_bought_given_SELL"] == 0.0
    assert r["verdict"] == "VERIFIED"


def test_a_maker_side_convention_fails_the_acceptance_loudly(tmp_path):
    tape = _bulk_hl_tape(tmp_path, n=600, flip=True)
    r = vt.verify("hyperliquid", [tape])
    assert r["verdict"] == "SIGN_UNVERIFIED"
    assert any("P(taker_bought|BUY)" in s for s in r["reasons"])


def test_too_few_classified_prints_is_sign_unverified_with_the_reason(tmp_path):
    tape = write_tape(
        tmp_path / "hl.gz",
        [
            hl_bbo(T0_MS, 100.0, 101.0),
            hl_trade(T0_MS + 10, "B", 101.0, 1),
            hl_trade(T0_MS + 20, "A", 100.0, 2),
        ],
    )
    r = vt.verify("hyperliquid", [tape])
    assert r["verdict"] == "SIGN_UNVERIFIED"
    assert any("n_classified" in s for s in r["reasons"])
    # perfect agreement does not rescue an underpowered day
    assert r["P_taker_bought_given_BUY"] == 1.0


def test_an_unknown_venue_is_refused(tmp_path):
    with pytest.raises(SystemExit):
        vt.verify("binancefuturesum", [])
