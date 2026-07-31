"""Tests for ``build_lighter_dataset.py`` — the Lighter market-data builder.

The book, trade and catalog fixtures are the **same real mainnet bytes** the
connector's Rust tests use (``connector/src/lighter/fixtures.rs``): CRV
``market_id=36`` from the first seconds of a recorded session, so the snapshot
and the six diffs after it are genuinely consecutive and their nonces chain.
Pinning the Python port to the same evidence as the proven live path is the
whole point — a divergence here is a divergence from the connector.

Synthetic gzip recordings for the ``build``/gate tests are written into
``tmp_path`` with ``test_build_dataset.write_gz``; no network, no real files.
"""

import json
import sys
from pathlib import Path

import numpy as np
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))

import build_lighter_dataset as bl  # noqa: E402
from build_dataset import BuildError, PriceGrid  # noqa: E402
from test_build_dataset import write_gz  # noqa: E402

# --- real mainnet frames, verbatim from connector/src/lighter/fixtures.rs ----

SNAPSHOT_CRV = r'{"channel":"order_book:36","last_updated_at":1785358811946459,"offset":415803,"order_book":{"code":0,"asks":[{"price":"0.20382","size":"7816.7"},{"price":"0.20383","size":"1915.0"},{"price":"0.20391","size":"18240.6"},{"price":"0.20395","size":"47502.5"},{"price":"0.20398","size":"39308.1"},{"price":"0.20399","size":"47502.5"},{"price":"0.20410","size":"47518.6"},{"price":"0.20421","size":"25374.8"},{"price":"0.20436","size":"2495.4"},{"price":"0.20452","size":"135935.0"},{"price":"0.20462","size":"39565.1"},{"price":"0.20501","size":"6666.0"},{"price":"0.20515","size":"195642.5"},{"price":"0.20526","size":"195642.5"},{"price":"0.20537","size":"195642.5"},{"price":"0.20548","size":"195317.5"},{"price":"0.20559","size":"195317.5"},{"price":"0.20570","size":"195317.5"},{"price":"0.20581","size":"195204.4"},{"price":"0.20592","size":"194780.1"},{"price":"0.20603","size":"195572.2"},{"price":"0.20614","size":"195482.0"},{"price":"0.20660","size":"200.4"},{"price":"0.20673","size":"10587.2"},{"price":"0.20681","size":"390870.9"},{"price":"0.21012","size":"509.7"},{"price":"0.21522","size":"509.7"},{"price":"0.22133","size":"411.7"},{"price":"0.24000","size":"700.0"},{"price":"0.24444","size":"330.0"},{"price":"0.25222","size":"700.0"},{"price":"0.26444","size":"700.0"},{"price":"0.27666","size":"700.0"},{"price":"0.28888","size":"700.0"},{"price":"0.30111","size":"700.0"},{"price":"0.31333","size":"700.0"},{"price":"0.32555","size":"700.0"},{"price":"0.33777","size":"700.0"},{"price":"0.35000","size":"700.0"},{"price":"0.62997","size":"426.6"}],"bids":[{"price":"0.20351","size":"7673.9"},{"price":"0.20350","size":"2495.4"},{"price":"0.20341","size":"47502.5"},{"price":"0.20338","size":"47502.5"},{"price":"0.20335","size":"39327.4"},{"price":"0.20334","size":"47518.6"},{"price":"0.20326","size":"18000.0"},{"price":"0.20308","size":"25775.3"},{"price":"0.20300","size":"32203.4"},{"price":"0.20290","size":"40189.8"},{"price":"0.20274","size":"6668.4"},{"price":"0.20270","size":"102857.3"},{"price":"0.20240","size":"195641.1"},{"price":"0.20229","size":"195687.4"},{"price":"0.20218","size":"195797.5"},{"price":"0.20207","size":"195896.8"},{"price":"0.20196","size":"196005.3"},{"price":"0.20185","size":"196110.4"},{"price":"0.20174","size":"196217.7"},{"price":"0.20163","size":"196325.6"},{"price":"0.20152","size":"196436.4"},{"price":"0.20141","size":"196549.0"},{"price":"0.20129","size":"200.4"},{"price":"0.20121","size":"10587.2"},{"price":"0.20058","size":"385714.9"},{"price":"0.19900","size":"20000.0"},{"price":"0.19785","size":"509.7"},{"price":"0.19676","size":"508.3"},{"price":"0.19510","size":"12814.0"},{"price":"0.19502","size":"512.8"},{"price":"0.19370","size":"413.1"},{"price":"0.19275","size":"509.7"},{"price":"0.19197","size":"1302.3"},{"price":"0.19110","size":"785.0"},{"price":"0.18910","size":"793.3"},{"price":"0.18710","size":"1336.2"},{"price":"0.18663","size":"411.7"},{"price":"0.17036","size":"587.0"},{"price":"0.10500","size":"426.6"}],"offset":415803,"nonce":18032383898,"last_updated_at":1785358811946459,"begin_nonce":0},"timestamp":1785358811976,"type":"subscribed/order_book"}'

UPDATE_CRV_1 = r'{"channel":"order_book:36","last_updated_at":1785358812048570,"offset":415806,"order_book":{"code":0,"asks":[{"price":"0.20416","size":"25374.8"},{"price":"0.20421","size":"0.0"}],"bids":[{"price":"0.20351","size":"0.0"},{"price":"0.20308","size":"0.0"},{"price":"0.20302","size":"25775.2"}],"offset":415806,"nonce":18032384006,"last_updated_at":1785358812048570,"begin_nonce":18032383898},"timestamp":1785358812058,"type":"update/order_book"}'

UPDATE_CRV_2 = r'{"channel":"order_book:36","last_updated_at":1785358812073840,"offset":415807,"order_book":{"code":0,"asks":[],"bids":[{"price":"0.20334","size":"55192.5"}],"offset":415807,"nonce":18032384057,"last_updated_at":1785358812073840,"begin_nonce":18032384006},"timestamp":1785358812115,"type":"update/order_book"}'

UPDATE_CRV_3 = r'{"channel":"order_book:36","last_updated_at":1785358812113531,"offset":415808,"order_book":{"code":0,"asks":[{"price":"0.20416","size":"0.0"},{"price":"0.20417","size":"25374.8"}],"bids":[{"price":"0.20315","size":"25775.2"},{"price":"0.20302","size":"0.0"}],"offset":415808,"nonce":18032384126,"last_updated_at":1785358812113531,"begin_nonce":18032384057},"timestamp":1785358812162,"type":"update/order_book"}'

UPDATE_CRV_4 = r'{"channel":"order_book:36","last_updated_at":1785358812597325,"offset":415809,"order_book":{"code":0,"asks":[],"bids":[{"price":"0.20129","size":"0.0"}],"offset":415809,"nonce":18032384612,"last_updated_at":1785358812597325,"begin_nonce":18032384126},"timestamp":1785358812609,"type":"update/order_book"}'

UPDATE_CRV_5 = r'{"channel":"order_book:36","last_updated_at":1785358812644078,"offset":415811,"order_book":{"code":0,"asks":[{"price":"0.20660","size":"0.0"}],"bids":[{"price":"0.20316","size":"25775.2"},{"price":"0.20315","size":"0.0"}],"offset":415811,"nonce":18032384667,"last_updated_at":1785358812644078,"begin_nonce":18032384612},"timestamp":1785358812659,"type":"update/order_book"}'

UPDATE_CRV_6 = r'{"channel":"order_book:36","last_updated_at":1785358812738144,"offset":415812,"order_book":{"code":0,"asks":[],"bids":[{"price":"0.20129","size":"200.4"}],"offset":415812,"nonce":18032384785,"last_updated_at":1785358812738144,"begin_nonce":18032384667},"timestamp":1785358812764,"type":"update/order_book"}'

TRADE_CRV = r'{"channel":"trade:36","liquidation_trades":[],"nonce":18032408814,"trades":[{"trade_id":26412422124,"trade_id_str":"26412422124","tx_hash":"00000019c75017e30000019fafade0cc000000000000000000000000000000000000000000000000","type":"trade","market_id":36,"size":"0.7","price":"0.20382","usd_amount":"0.142674","ask_id":10414574259102759,"is_maker_ask":true,"block_height":303153774,"timestamp":1785358835916,"transaction_time":1785358835917189}],"type":"update/trade"}'

TRADE_REPLAY_CRV = r'{"channel":"trade:36","liquidation_trades":[{"trade_id":26218621834,"type":"liquidation","market_id":36,"size":"8215.3","price":"0.20039","is_maker_ask":false,"block_height":301149026,"timestamp":1785193205747,"transaction_time":1785193205753071}],"nonce":18032383935,"trades":[{"trade_id":26412385647,"type":"trade","market_id":36,"size":"0.7","price":"0.20382","is_maker_ask":true,"block_height":303153355,"timestamp":1785358805929,"transaction_time":1785358805929929},{"trade_id":26412344671,"type":"trade","market_id":36,"size":"0.7","price":"0.20393","is_maker_ask":true,"block_height":303152935,"timestamp":1785358775942,"transaction_time":1785358775942778}],"type":"subscribed/trade"}'

TICKER_BTC = r'{"channel":"ticker:1","last_updated_at":1785252530834835,"nonce":17926043680,"ticker":{"s":"BTC","a":{"price":"63777.3","size":"0.00020"},"b":{"price":"63777.2","size":"2.49220"}},"timestamp":1785252530836,"type":"update/ticker"}'

HEIGHT = r'{"channel":"height","height":301813997,"timestamp":1785252531621,"type":"update/height"}'
CONNECTED = r'{"session_id":"b23c70b2-9ef8-4080-8f02-4edf34138329","type":"connected"}'

TICK = 1e-5
LOT = 0.1
NOW_NS = 1_785_358_812_000_000_000


def frame(text):
    return bl.parse_frame(json.loads(text))


def book_of(text):
    tag, book = frame(text)
    assert tag in ('book_snapshot', 'book_update'), tag
    return book


def shape(events):
    """``(side, px, qty)`` per event in emission order."""
    out = []
    for ev, _exch, _local, px, qty in events:
        if ev == bl.BID_DEPTH:
            side = 'B'
        elif ev == bl.ASK_DEPTH:
            side = 'A'
        else:
            raise AssertionError('event %#x is not a bid/ask depth event' % ev)
        out.append((side, px, qty))
    return out


# ---------------------------------------------------------------------------
# parse_frame — the msg.rs traps
# ---------------------------------------------------------------------------

def test_market_id_is_read_from_the_colon_spelling():
    # The venue's sharpest trap: subscribe as order_book/36, served order_book:36.
    assert bl.market_id_of('order_book:36') == 36
    assert bl.market_id_of('trade:0') == 0
    assert bl.market_id_of('order_book/36') is None
    assert bl.market_id_of('height') is None
    assert bl.market_id_of('market_stats:all') is None
    assert book_of(SNAPSHOT_CRV).market_id == 36


def test_a_snapshot_carries_the_whole_book_and_reseeds_the_chain():
    snap = book_of(SNAPSHOT_CRV)
    assert frame(SNAPSHOT_CRV)[0] == 'book_snapshot'
    assert (len(snap.bids), len(snap.asks)) == (39, 40)
    assert snap.begin_nonce == 0
    assert snap.nonce == 18032383898
    # Microseconds, not milliseconds — the §4.4 trap is a factor of 1000.
    assert snap.last_updated_at_us == 1785358811946459
    assert snap.bids[0].px == 0.20351 and snap.bids[0].qty == 7673.9
    assert snap.asks[0].px == 0.20382
    first = book_of(UPDATE_CRV_1)
    assert frame(UPDATE_CRV_1)[0] == 'book_update'
    assert first.begin_nonce == snap.nonce
    assert book_of(UPDATE_CRV_2).begin_nonce == first.nonce


def test_a_zero_size_is_a_deletion_kept_distinct_from_a_resize():
    diff = book_of(UPDATE_CRV_1)
    assert len(diff.bids) == 3 and len(diff.asks) == 2
    assert diff.bids[0].px == 0.20351 and diff.bids[0].qty == 0.0
    assert diff.asks[0].qty == 25374.8


def test_both_trade_arrays_are_published_with_the_aggressor_side():
    tag, trades = frame(TRADE_CRV)
    assert tag == 'trades' and len(trades) == 1
    t = trades[0]
    assert t.trade_id == 26412422124 and t.market_id == 36
    assert t.px == 0.20382 and t.qty == 0.7
    assert t.is_maker_ask is True  # resting side was the ask -> taker bought
    assert t.transaction_time_us == 1785358835917189
    _tag, replay = frame(TRADE_REPLAY_CRV)
    assert len(replay) == 3, 'two prints plus the liquidation beside them'
    assert any(r.trade_id == 26218621834 for r in replay)


def test_unmodelled_frames_are_skipped_not_mistaken_for_a_book():
    for text in (TICKER_BTC, HEIGHT, CONNECTED):
        assert frame(text)[0] == 'other'


def test_malformed_frames_raise_rather_than_invent_a_deletion():
    # A book frame whose channel names no market cannot be routed.
    with pytest.raises(BuildError):
        frame(SNAPSHOT_CRV.replace('order_book:36', 'order_book'))
    # A size that is not a number must not be read as zero — that is a deletion.
    with pytest.raises(BuildError):
        frame(UPDATE_CRV_1.replace('"size":"0.0"', '"size":""'))


# ---------------------------------------------------------------------------
# LighterBook — parity with depth.rs DepthMirror, same frames
# ---------------------------------------------------------------------------

def apply_snapshot(book, text, now=NOW_NS):
    b = book_of(text)
    events, refusal = book.on_snapshot(b.bids, b.asks, b.last_updated_at_us, now)
    assert refusal is None
    return events


def apply_delta(book, text, now=NOW_NS):
    b = book_of(text)
    events, refusal = book.on_delta(b.bids, b.asks, b.last_updated_at_us, now)
    assert refusal is None
    return events


def test_the_first_snapshot_becomes_one_insert_per_level():
    book = bl.LighterBook(TICK, LOT)
    events = apply_snapshot(book, SNAPSHOT_CRV)
    assert len(events) == 79, '39 bids and 40 asks'
    assert book.depth() == (39, 40)
    assert book.best_bid() == 0.20351
    assert book.best_ask() == 0.20382
    # Exchange time carried through from microseconds (§4.4).
    assert events[0][1] == 1_785_358_811_946_459 * 1_000
    assert events[0][2] == NOW_NS


def test_a_delta_deletes_a_zero_size_level_and_replaces_the_rest():
    book = bl.LighterBook(TICK, LOT)
    apply_snapshot(book, SNAPSHOT_CRV)
    before = book.depth()
    events = apply_delta(book, UPDATE_CRV_1)
    # Deletions first, both sides, ascending tick; then the inserts.
    assert shape(events) == [
        ('B', 0.20308, 0.0),
        ('B', 0.20351, 0.0),
        ('A', 0.20421, 0.0),
        ('B', 0.20302, 25775.2),
        ('A', 0.20416, 25374.8),
    ]
    assert book.best_bid() == 0.20350
    assert book.best_ask() == 0.20382
    assert book.depth() == (before[0] - 1, before[1])


def test_the_six_diffs_after_the_snapshot_keep_the_book_uncrossed():
    """The recorded consecutive run — snapshot + six diffs — never crosses, and
    the best bid/ask track the venue's the whole way."""
    book = bl.LighterBook(TICK, LOT)
    apply_snapshot(book, SNAPSHOT_CRV)
    for text in (UPDATE_CRV_1, UPDATE_CRV_2, UPDATE_CRV_3, UPDATE_CRV_4,
                 UPDATE_CRV_5, UPDATE_CRV_6):
        apply_delta(book, text)
        assert book.best_bid() < book.best_ask()
    assert book.counts.crossed == 0


def test_a_frame_that_would_publish_a_crossed_book_is_refused_whole():
    book = bl.LighterBook(TICK, LOT)
    book.on_snapshot([bl.Level(100.0, 1.0)], [bl.Level(101.0, 1.0)], 1_000, 0)
    events, refusal = book.on_delta([bl.Level(101.0, 5.0)], [], 1_001, 0)
    assert refusal == bl.REFUSAL_CROSSED and events == []
    assert book.counts.crossed == 1
    assert book.best_bid() == 100.0, 'the mirror is untouched'
    # Refused before the monotonic gate latches: the next good frame still lands.
    events, refusal = book.on_delta([bl.Level(100.0, 2.0)], [], 1_001, 0)
    assert refusal is None and shape(events) == [('B', 100.0, 2.0)]


def test_a_stale_frame_is_refused_and_does_not_rewind_the_book():
    book = bl.LighterBook(TICK, LOT)
    book.on_snapshot([bl.Level(100.0, 1.0)], [bl.Level(101.0, 1.0)], 2_000, 0)
    events, refusal = book.on_delta([bl.Level(90.0, 9.0)], [], 1_999, 0)
    assert refusal == bl.REFUSAL_STALE and events == []
    assert book.counts.stale == 1
    assert book.best_bid() == 100.0


def test_one_frame_from_the_future_cannot_latch_the_feed_shut():
    now_us = 1_785_358_811_946_459
    now_ns = now_us * 1_000
    book = bl.LighterBook(TICK, LOT)
    book.on_snapshot([bl.Level(100.0, 1.0)], [bl.Level(101.0, 1.0)], now_us, now_ns)
    # The same field read as if it were milliseconds: 1000x out.
    events, refusal = book.on_delta([bl.Level(100.0, 2.0)], [], now_us * 1_000, now_ns)
    assert refusal == bl.REFUSAL_IMPLAUSIBLE
    assert book.counts.implausible == 1
    # The frames that follow are unaffected.
    events, refusal = book.on_delta([bl.Level(100.0, 3.0)], [], now_us + 1, now_ns + 1_000)
    assert refusal is None and shape(events) == [('B', 100.0, 3.0)]
    assert book.counts.stale == 0


def test_a_level_that_rounds_below_one_lot_is_absent_and_counted():
    book = bl.LighterBook(TICK, LOT)
    book.on_snapshot([bl.Level(100.0, 1.0)], [bl.Level(101.0, 1.0)], 1_000, 0)
    events, _ = book.on_delta([bl.Level(100.0, LOT / 100.0)], [], 1_001, 0)
    assert shape(events) == [('B', 100.0, 0.0)], 'a deletion, not a dust size'
    assert book.best_bid() is None
    assert book.counts.sub_lot == 1
    # A genuine "0.0" deletion is not a symptom and is not counted as one.
    book.on_delta([], [bl.Level(101.0, 0.0)], 1_002, 0)
    assert book.counts.sub_lot == 1


def test_two_prices_on_one_tick_are_summed_and_counted():
    # A grid ten times coarser than the prices arriving.
    book = bl.LighterBook(1e-4, LOT)
    events, _ = book.on_snapshot([bl.Level(0.20351, 1.0), bl.Level(0.20353, 2.0)],
                                 [], 1_000, 0)
    assert shape(events) == [('B', 0.20351, 3.0)]
    assert book.counts.collapsed == 1


def test_the_trade_side_is_the_aggressors():
    _tag, trades = frame(TRADE_CRV)
    # is_maker_ask True -> taker bought -> a BUY trade event.
    assert (bl.BUY_TRADE if trades[0].is_maker_ask else bl.SELL_TRADE) == bl.BUY_TRADE


# ---------------------------------------------------------------------------
# TradeDedup — trades.rs
# ---------------------------------------------------------------------------

def test_a_replayed_frame_is_dropped_in_full():
    guard = bl.TradeDedup()
    _tag, replay = frame(TRADE_REPLAY_CRV)
    assert sum(guard.accept(t.market_id, t.trade_id) for t in replay) == len(replay)
    assert sum(guard.accept(t.market_id, t.trade_id) for t in replay) == 0
    assert guard.dropped == len(replay)


def test_only_new_prints_after_a_replay_get_through():
    guard = bl.TradeDedup()
    _tag, replay = frame(TRADE_REPLAY_CRV)
    for t in replay:
        guard.accept(t.market_id, t.trade_id)
    _tag, fresh = frame(TRADE_CRV)
    combined = replay + fresh
    assert sum(guard.accept(t.market_id, t.trade_id) for t in combined) == 1


def test_the_dedup_window_is_bounded_and_per_market():
    guard = bl.TradeDedup()
    for tid in range(bl.TRADE_ID_HISTORY):
        assert guard.accept(36, tid)
    assert not guard.accept(36, 0), 'still inside the window'
    assert guard.accept(36, 1000)
    assert guard.accept(36, 0), 'the oldest id fell out'
    # A busy market must not evict another's window.
    assert guard.accept(0, 7)


# ---------------------------------------------------------------------------
# reconcile_tick_lighter — measured wins, catalog is the cross-check
# ---------------------------------------------------------------------------

def grid_from(prices):
    g = PriceGrid()
    for p in prices:
        g.observe(p)
    return g


def test_measured_tick_matching_the_catalog_agrees():
    # CRV-shaped: prices on a 1e-5 grid, catalog price_decimals=5.
    g = grid_from(['0.20382', '0.20383', '0.20391', '0.20351', '0.20350',
                   '0.20341', '0.20302', '0.20416', '0.20660'])
    tl = bl.reconcile_tick_lighter(g, price_decimals=5, symbol='CRV')
    assert tl['value'] == 1e-5
    assert tl['cross_check'] == 'agree'
    assert tl['catalog'] == 1e-5


def test_a_window_coarser_than_the_catalog_uses_the_measurement_and_warns(capsys):
    # Every price on a 1e-4 grid (multiples of 1e-4, not 1e-3) while the catalog
    # allows 1e-5.
    g = grid_from(['0.2011', '0.2023', '0.2035', '0.2047', '0.2059',
                   '0.2061', '0.2073', '0.2085', '0.2097'])
    tl = bl.reconcile_tick_lighter(g, price_decimals=5, symbol='CRV')
    assert tl['value'] == 1e-4
    assert tl['cross_check'] == 'measured_coarser_than_catalog'
    assert 'coarser' in capsys.readouterr().err


def test_a_window_finer_than_the_catalog_is_a_loud_yellow(capsys):
    # Prices quoted at 1e-5 (multiples of 1e-5, not 1e-4) while the catalog
    # claims only 1e-4: the catalog record is stale for this market.
    g = grid_from(['4.50001', '4.50012', '4.50023', '4.50034', '4.50045',
                   '4.50056', '4.50067', '4.50078', '4.50089'])
    tl = bl.reconcile_tick_lighter(g, price_decimals=4, symbol='PENDLE')
    assert tl['value'] == 1e-5
    assert tl['cross_check'] == 'measured_finer_than_catalog'
    assert 'finer' in capsys.readouterr().err


def test_too_few_distinct_prices_refuses():
    g = grid_from(['0.2', '0.2', '0.3'])
    with pytest.raises(BuildError):
        bl.reconcile_tick_lighter(g, price_decimals=5, symbol='CRV')


# ---------------------------------------------------------------------------
# catalog
# ---------------------------------------------------------------------------

def universe_line(local_ts, markets):
    obj = {'_collector': 'universe', 'markets': markets}
    return (local_ts, json.dumps(obj))


CRV_ROW = {'market_id': 36, 'symbol': 'CRV', 'market_type': 'perp',
           'supported_price_decimals': 5, 'supported_size_decimals': 1}
HYPE_ROW = {'market_id': 24, 'symbol': 'HYPE', 'market_type': 'perp',
            'supported_price_decimals': 4, 'supported_size_decimals': 2}


def write_meta_jsonl(path, records):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, 'w') as f:
        for local_ts, payload in records:
            f.write('%d %s\n' % (local_ts, payload))
    return path


def test_catalog_resolves_symbol_to_id_and_decimals(tmp_path):
    meta = write_meta_jsonl(tmp_path / '_meta_lighter_20260729.jsonl',
                            [universe_line(1, [CRV_ROW, HYPE_ROW])])
    catalog = bl.load_catalog([meta])
    info = bl.resolve_symbol(catalog, 'CRV')
    assert info.market_id == 36
    assert info.catalog_tick() == 1e-5
    assert info.lot_size() == 0.1
    assert bl.resolve_symbol(catalog, 'HYPE').catalog_tick() == 1e-4


def test_a_case_mismatch_is_refused_by_name(tmp_path):
    meta = write_meta_jsonl(tmp_path / '_meta_lighter_20260729.jsonl',
                            [universe_line(1, [CRV_ROW])])
    catalog = bl.load_catalog([meta])
    with pytest.raises(BuildError) as e:
        bl.resolve_symbol(catalog, 'crv')
    assert 'CRV' in str(e.value)


def test_a_missing_universe_record_refuses(tmp_path):
    meta = write_meta_jsonl(tmp_path / '_meta_lighter_20260730.jsonl',
                            [(1, json.dumps({'_collector': 'cpu'}))])
    with pytest.raises(BuildError):
        bl.load_catalog([meta])


# ---------------------------------------------------------------------------
# the gate
# ---------------------------------------------------------------------------

def scan_with(**kw):
    s = bl.LighterScan(path='x', day='20260730', market_id=36)
    for k, v in kw.items():
        setattr(s, k, v)
    return s


def test_gate_green_when_book_and_trades_and_latency_are_healthy():
    s = scan_with(converted_book_frames=1000, converted_trade_prints=50,
                  min_local_minus_exch_ns=2_000_000)
    assert bl.lighter_verdict(s)[0] == 'green'


def test_gate_red_when_no_order_book_frames():
    s = scan_with(converted_book_frames=0, converted_trade_prints=50,
                  min_local_minus_exch_ns=2_000_000)
    verdict, red, _ = bl.lighter_verdict(s)
    assert verdict == 'red' and any('order_book' in r for r in red)


def test_gate_red_on_negative_feed_latency():
    s = scan_with(converted_book_frames=1000, converted_trade_prints=50,
                  min_local_minus_exch_ns=-1)
    verdict, red, _ = bl.lighter_verdict(s)
    assert verdict == 'red' and any('latency' in r for r in red)


def test_gate_red_when_no_trades_to_fill_against():
    s = scan_with(converted_book_frames=1000, converted_trade_prints=0,
                  min_local_minus_exch_ns=2_000_000)
    verdict, red, _ = bl.lighter_verdict(s)
    assert verdict == 'red' and any('trade' in r for r in red)


def test_gate_yellow_on_gaps_and_chain_breaks():
    s = scan_with(converted_book_frames=1000, converted_trade_prints=50,
                  min_local_minus_exch_ns=2_000_000, chain_breaks=2,
                  max_gap_ns={'order_book': 40 * bl.NS_PER_SEC})
    verdict, red, yellow = bl.lighter_verdict(s)
    assert verdict == 'yellow' and red == []
    assert any('order_book' in y for y in yellow)
    assert any('chain break' in y for y in yellow)


# ---------------------------------------------------------------------------
# convert + build (end to end, with the real finalizer)
# ---------------------------------------------------------------------------

def _has_native():
    try:
        import hftbacktest.data.validation  # noqa: F401
        return True
    except Exception:
        return False


native = pytest.mark.skipif(not _has_native(),
                            reason='needs the built hftbacktest package')

MS = 1_000_000
DAY_START_NS = 1_785_369_600 * 1_000_000_000  # 2026-07-30 00:00 UTC


def crv_recording(day_start_ns):
    """A short but complete CRV day: snapshot + 6 diffs + a trade + a replay.

    Each line's local_ts is the frame's exch time (µs->ns) plus a fixed 5 ms
    feed latency, so the time policy holds and the two timelines agree.
    """
    lines = []

    def emit(text):
        obj = json.loads(text)
        ob = obj.get('order_book')
        if ob is not None:
            exch_ns = ob['last_updated_at'] * 1000
        else:
            # trade frame: use the max transaction_time in it
            tt = []
            for arr in ('trades', 'liquidation_trades'):
                tt += [t['transaction_time'] for t in obj.get(arr, [])]
            exch_ns = max(tt) * 1000
        lines.append((exch_ns + 5 * MS, text))

    for text in (SNAPSHOT_CRV, UPDATE_CRV_1, UPDATE_CRV_2, UPDATE_CRV_3,
                 UPDATE_CRV_4, UPDATE_CRV_5, UPDATE_CRV_6, TRADE_CRV,
                 TRADE_REPLAY_CRV):
        emit(text)
    lines.sort()
    # A reconnect a moment later replays the last prints — the venue re-sends
    # TRADE_CRV's id, which the de-dup must drop. Placed after the tape so its
    # id is already seen.
    lines.append((lines[-1][0] + 10 * MS, TRADE_CRV))
    return lines


@pytest.fixture
def crv_dataset(tmp_path):
    data_dir = tmp_path / 'lighter'
    write_gz(data_dir / 'crv_20260730.gz', crv_recording(DAY_START_NS))
    write_meta_jsonl(data_dir / '_meta_lighter_20260730.jsonl',
                     [universe_line(DAY_START_NS, [CRV_ROW])])
    return {'data_dir': data_dir, 'out': tmp_path / 'out'}


def argv(ds, **overrides):
    out = ['--data-dir', str(ds['data_dir']), '--symbol', 'CRV',
           '--out-dir', str(ds['out']), '--min-window-hours', '0']
    for k, v in overrides.items():
        out += ['--' + k.replace('_', '-'), str(v)]
    return out


def manifest_of(ds):
    return json.loads((ds['out'] / 'manifest.json').read_text())


@native
def test_build_produces_a_valid_npz_and_manifest(crv_dataset):
    bl.main(argv(crv_dataset))
    m = manifest_of(crv_dataset)
    assert m['schema'] == 'lighter-dataset-v1'
    assert m['tick_size'] == 1e-5 and m['lot_size'] == 0.1
    assert m['tick_lot']['kind'] == 'measured'
    assert m['tick_lot']['tick']['cross_check'] == 'agree'
    assert m['instruments']['execution']['market_id'] == 36
    npz = crv_dataset['out'] / m['files']['execution_npz']
    assert npz.exists()
    data = np.load(npz)['data']
    # hftbacktest's contract: every row carries at least one timeline bit, and
    # each timeline is individually ordered (a print recovered from a replay may
    # be exch-only in the past and local-only in-window — that is the split).
    exch = (data['ev'] & bl.EXCH_EVENT) == bl.EXCH_EVENT
    local = (data['ev'] & bl.LOCAL_EVENT) == bl.LOCAL_EVENT
    assert (exch | local).all(), 'a row carries neither bit'
    assert (np.diff(data['exch_ts'][exch]) >= 0).all()
    assert (np.diff(data['local_ts'][local]) >= 0).all()
    # The tape is present and the replay was de-duplicated (1 real trade).
    is_trade = (data['ev'] & bl.TRADE_EVENT) == bl.TRADE_EVENT
    assert is_trade.sum() >= 1
    assert m['converter']['total_trades_deduplicated'] >= 1


@native
def test_build_refuses_a_day_with_no_trades(tmp_path):
    """A book-only recording is red: a passive grid fills off the tape."""
    data_dir = tmp_path / 'lighter'
    no_trades = [ln for ln in crv_recording(DAY_START_NS)
                 if '/trade' not in ln[1]]
    write_gz(data_dir / 'crv_20260730.gz', no_trades)
    write_meta_jsonl(data_dir / '_meta_lighter_20260730.jsonl',
                     [universe_line(DAY_START_NS, [CRV_ROW])])
    ds = {'data_dir': data_dir, 'out': tmp_path / 'out'}
    with pytest.raises(SystemExit) as e:
        bl.main(argv(ds))
    assert e.value.code == 1


@native
def test_an_explicit_tick_size_overrides_the_measurement(crv_dataset):
    bl.main(argv(crv_dataset, tick_size='0.001', lot_size='1.0'))
    m = manifest_of(crv_dataset)
    assert m['tick_size'] == 0.001 and m['tick_lot']['kind'] == 'cli'


def _ob_line(local_ns, exch_us, bids, asks, *, snapshot, nonce, begin_nonce):
    kind = 'subscribed/order_book' if snapshot else 'update/order_book'
    ob = {'code': 0, 'nonce': nonce, 'begin_nonce': begin_nonce,
          'last_updated_at': exch_us,
          'bids': [{'price': p, 'size': s} for p, s in bids],
          'asks': [{'price': p, 'size': s} for p, s in asks]}
    obj = {'channel': 'order_book:36', 'last_updated_at': exch_us,
           'order_book': ob, 'timestamp': exch_us // 1000, 'type': kind}
    return (local_ns, json.dumps(obj))


def _trade_line(local_ns, exch_us, tid, px, sz, is_maker_ask=True):
    obj = {'channel': 'trade:36', 'liquidation_trades': [], 'type': 'update/trade',
           'trades': [{'market_id': 36, 'trade_id': tid, 'price': px, 'size': sz,
                       'is_maker_ask': is_maker_ask, 'transaction_time': exch_us}]}
    return (local_ns, json.dumps(obj))


@native
def test_a_level_open_at_days_close_and_pulled_next_day_is_deleted_in_day2(tmp_path):
    """The carry-across-days invariant: the backtest concatenates the day files
    into one book, so a level established on day 1 and pulled early on day 2 —
    with no prior day-2 resize — must have its deletion emitted in day 2's npz.
    A fresh per-day book would miss it and leave it stale for the whole run."""
    MSx = 1_000_000
    d1_us = 1785283200_000000  # 2026-07-29 00:00 UTC in microseconds
    d2_us = 1785369600_000000  # 2026-07-30 00:00 UTC in microseconds
    STALE = '0.20000'  # a deep bid opened on day 1, never touched again until d2 pulls it

    day1 = [
        _ob_line(d1_us * 1000 + 5 * MSx, d1_us, snapshot=True, nonce=1, begin_nonce=0,
                 bids=[('0.20351', '7673.9'), (STALE, '1000.0')],
                 asks=[('0.20382', '7816.7')]),
        _trade_line((d1_us + 10) * 1000 + 5 * MSx, d1_us + 10, 111, '0.20382', '0.7'),
    ]
    day2 = [
        # First day-2 frame: pull the stale level. No prior resize of it in day 2.
        _ob_line(d2_us * 1000 + 5 * MSx, d2_us, snapshot=False, nonce=2, begin_nonce=1,
                 bids=[(STALE, '0.0')], asks=[]),
        _trade_line((d2_us + 10) * 1000 + 5 * MSx, d2_us + 10, 222, '0.20382', '0.7'),
    ]
    data_dir = tmp_path / 'lighter'
    write_gz(data_dir / 'crv_20260729.gz', day1)
    write_gz(data_dir / 'crv_20260730.gz', day2)
    write_meta_jsonl(data_dir / '_meta_lighter_20260729.jsonl',
                     [universe_line(d1_us * 1000, [CRV_ROW])])
    ds = {'data_dir': data_dir, 'out': tmp_path / 'out'}
    # Explicit tick/lot: this minimal book has too few distinct prices to measure
    # one from, which is a separate gate — here we are exercising the carry.
    bl.main(argv(ds, tick_size='0.00001', lot_size='0.1'))
    day2_npz = ds['out'] / ('lighter_crv_20260730.npz')
    data = np.load(day2_npz)['data']
    # A bid deletion (qty 0) at the stale price must appear in day 2.
    pulled = [(r['px'], r['qty']) for r in data
              if (r['ev'] & bl.BUY_EVENT) and (r['ev'] & bl.DEPTH_EVENT)
              and abs(r['px'] - 0.20000) < 1e-9]
    assert pulled and all(q == 0.0 for _p, q in pulled), (
        'day 2 must emit the deletion of the level day 1 left open: %r' % pulled)


@native
def test_the_book_warms_up_to_the_snapshot_touch(crv_dataset):
    """Replaying the produced events rebuilds the venue's touch: after the run,
    best bid 0.20350 / best ask 0.20382 — the book as the six diffs left it."""
    bl.main(argv(crv_dataset))
    m = manifest_of(crv_dataset)
    data = np.load(crv_dataset['out'] / m['files']['execution_npz'])['data']
    bids, asks = {}, {}
    for row in data:
        if not (row['ev'] & bl.DEPTH_EVENT):
            continue
        side = bids if (row['ev'] & bl.BUY_EVENT) else asks
        tick = round(row['px'] / 1e-5)
        if round(row['qty'] / 0.1) > 0:
            side[tick] = row['px']
        else:
            side.pop(tick, None)
    assert abs(max(bids.values()) - 0.20350) < 1e-9
    assert abs(min(asks.values()) - 0.20382) < 1e-9
