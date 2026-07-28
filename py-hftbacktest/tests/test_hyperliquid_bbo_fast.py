"""Tests for ``hftbacktest.data.utils.hyperliquid.convert`` with ``book_mode='bbo+fast'``.

Every recording here is synthetic: a tiny gzip file with known frames written
into ``tmp_path``. The two real-data checks at the bottom skip themselves when
the recording is not on this machine.

The subject is the offline fusion of Hyperliquid's two book feeds. ``bbo`` is a
one-level, event-driven touch feed (measured median inter-frame gap 86 ms on
``btc_20260727``); ``l2Book fast`` is a five-level full snapshot every ~0.54 s.
The converter used to read only ``l2Book`` and drop every ``bbo`` frame, which
made the backtest book strictly coarser than the live connector's. ``bbo+fast``
interleaves them into one ``DEPTH_EVENT`` stream.

Four properties carry the mode, and each has tests here:

* a ``bbo`` frame moves the top of book between snapshots;
* the book is never crossed, row by row;
* the next snapshot diffs against the book *as modified by* ``bbo``, so it
  neither re-emits what ``bbo`` already set nor swallows a revert;
* ``slow`` and ``fast`` are byte-for-byte what they were.
"""

import gzip
import hashlib
import json
from pathlib import Path

import numpy as np
import pytest

from hftbacktest.data.utils import hyperliquid
from hftbacktest.types import (
    BUY_EVENT,
    DEPTH_EVENT,
    EXCH_EVENT,
    LOCAL_EVENT,
    SELL_EVENT,
    TRADE_EVENT,
)

MS = 1_000_000

# 2026-07-26T00:00:00Z in milliseconds; nanosecond local timestamps derived from
# it are 19 digits wide, which is what the reader's fixed-width slice expects.
BASE_MS = 1_785_024_000_000

#: One real day of BTC, used by the spot checks. 978_751 lines: 655_873 `bbo`,
#: 159_194 `l2Book fast`, 16_081 `l2Book` slow, 147_603 `trades`.
REAL = Path('/Users/andrew/hft-data/hyperliquid/btc_20260727.gz')


# ---------------------------------------------------------------------------
# frame builders
# ---------------------------------------------------------------------------


def lvl(px, sz):
    return {'px': repr(float(px)), 'sz': repr(float(sz)), 'n': 1}


def ladder(best, step, n, sz=1.0):
    """``n`` levels walking away from ``best`` by ``step`` (negative for bids)."""
    return [lvl(best + i * step, sz + i) for i in range(n)]


def snapshot(ms, bids, asks, fast=True, coin='BTC'):
    data = {'coin': coin, 'time': BASE_MS + ms, 'levels': [bids, asks]}
    if fast:
        data['fast'] = True
    return ms, {'channel': 'l2Book', 'data': data}


def bbo(ms, bid, ask, coin='BTC'):
    """``bid``/``ask`` are ``(px, sz)`` or ``None`` for a missing side."""
    return ms, {
        'channel': 'bbo',
        'data': {
            'coin': coin,
            'time': BASE_MS + ms,
            'bbo': [
                lvl(*bid) if bid is not None else None,
                lvl(*ask) if ask is not None else None,
            ],
        },
    }


def trade(ms, px, sz, tid=None, side='B', coin='BTC'):
    entry = {'coin': coin, 'side': side, 'px': repr(float(px)),
             'sz': repr(float(sz)), 'time': BASE_MS + ms}
    if tid is not None:
        entry['tid'] = tid
    return ms, {'channel': 'trades', 'data': [entry]}


def write_recording(path, frames):
    """One frame per line. The local timestamp trails the venue timestamp by
    half a millisecond, so the feed latency is positive throughout and
    ``correct_local_timestamp`` never shifts anything."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with gzip.open(path, 'wb') as f:
        for ms, payload in frames:
            local_ts = (BASE_MS + ms) * MS + 500_000
            f.write(('%d %s\n' % (local_ts, json.dumps(payload))).encode())
    return path


def convert(path, **kwargs):
    kwargs.setdefault('num_levels', 5)
    kwargs.setdefault('book_mode', 'bbo+fast')
    return hyperliquid.convert(
        str(path), tick_size=0.1, lot_size=0.001, buffer_size=100_000, **kwargs
    )


def run(tmp_path, frames, **kwargs):
    return convert(write_recording(tmp_path / 'rec.gz', frames), **kwargs)


# ---------------------------------------------------------------------------
# readers
# ---------------------------------------------------------------------------


def depth_rows(data, side=None, ms=None):
    """``(exch_ms, side, px, qty)`` for the exchange copy of each depth row.

    ``correct_event_order`` splits a row whose exchange and local orders
    disagree into an exchange-side row and a local-side row, so counting one
    side counts each emitted row once.
    """
    out = []
    for r in data:
        ev = int(r['ev'])
        if ev & 0xff != DEPTH_EVENT or not ev & EXCH_EVENT:
            continue
        row_side = 'B' if ev & BUY_EVENT else 'A'
        row_ms = int(r['exch_ts']) // MS - BASE_MS
        if side is not None and row_side != side:
            continue
        if ms is not None and row_ms != ms:
            continue
        out.append((row_ms, row_side, float(r['px']), float(r['qty'])))
    return out


def replay(data, bit):
    """Apply the ``bit``-side copy of the depth rows one at a time.

    Yields ``(row, bids, asks)`` after every row, which is the granularity the
    uncrossed invariant is asserted at.
    """
    bids, asks = {}, {}
    for r in data:
        ev = int(r['ev'])
        if ev & 0xff != DEPTH_EVENT or not ev & bit:
            continue
        book = bids if ev & BUY_EVENT else asks
        px, qty = float(r['px']), float(r['qty'])
        if qty == 0:
            book.pop(round(px, 6), None)
        else:
            book[round(px, 6)] = qty
        yield r, bids, asks


def assert_never_crossed(data):
    for bit in (EXCH_EVENT, LOCAL_EVENT):
        for row, bids, asks in replay(data, bit):
            if bids and asks and max(bids) >= min(asks):
                raise AssertionError(
                    'crossed book after row ev=%#x px=%s qty=%s: best bid %s >= '
                    'best ask %s' % (int(row['ev']), row['px'], row['qty'],
                                     max(bids), min(asks))
                )


# ---------------------------------------------------------------------------
# 1. bbo moves the top of book between snapshots
# ---------------------------------------------------------------------------


def test_a_bbo_between_snapshots_moves_the_top_of_book(tmp_path):
    """The whole point of the mode: a touch update lands as a depth row.

    A new best is a sixth level, and the window holds five, so the deepest one
    leaves with it — inherent to a top-N window, and the next snapshot puts it
    back (see the reconcile tests).
    """
    data = run(tmp_path, [
        snapshot(0, ladder(100.0, -1.0, 5), ladder(101.0, 1.0, 5)),
        bbo(100, (100.5, 7.0), (101.0, 1.0)),
    ])

    assert depth_rows(data, ms=100) == [
        (100, 'B', 96.0, 0.0),
        (100, 'B', 100.5, 7.0),
    ]


def test_a_bbo_row_is_an_ordinary_depth_event(tmp_path):
    """Kind 1, not DEPTH_BBO_EVENT — ``Local::process`` ignores kind 5 — and
    carrying the same EXCH/LOCAL bits every other row of this converter has."""
    data = run(tmp_path, [
        snapshot(0, ladder(100.0, -1.0, 5), ladder(101.0, 1.0, 5)),
        bbo(100, (100.5, 7.0), (101.5, 8.0)),
    ])

    rows = [r for r in data if int(r['exch_ts']) // MS - BASE_MS == 100]
    assert rows, 'the bbo frame emitted nothing'
    for r in rows:
        ev = int(r['ev'])
        assert ev & 0xff == DEPTH_EVENT
        assert ev & (EXCH_EVENT | LOCAL_EVENT) == EXCH_EVENT | LOCAL_EVENT
        assert bool(ev & BUY_EVENT) != bool(ev & SELL_EVENT)


def test_a_bbo_that_repeats_the_book_emits_nothing(tmp_path):
    """Most bbo frames restate the touch; restating it must not churn the book."""
    data = run(tmp_path, [
        snapshot(0, ladder(100.0, -1.0, 5), ladder(101.0, 1.0, 5)),
        bbo(100, (100.0, 1.0), (101.0, 1.0)),
    ])

    assert depth_rows(data, ms=100) == []


def test_a_bbo_that_lowers_the_best_bid_deletes_the_levels_above_it(tmp_path):
    """bbo is authoritative about the touch: nothing may remain above it."""
    data = run(tmp_path, [
        snapshot(0, ladder(100.0, -1.0, 5), ladder(101.0, 1.0, 5)),
        bbo(100, (98.5, 4.0), (101.0, 1.0)),
    ])

    rows = depth_rows(data, ms=100)
    assert (100, 'B', 100.0, 0.0) in rows
    assert (100, 'B', 99.0, 0.0) in rows
    assert (100, 'B', 98.5, 4.0) in rows
    # 98/97/96 survive untouched, and the ask side was restated, not changed.
    assert len(rows) == 3


# ---------------------------------------------------------------------------
# 2. crossing hygiene
# ---------------------------------------------------------------------------


def test_a_crossing_bbo_deletes_the_mirrored_levels_it_crosses(tmp_path):
    """The mirror is up to ~0.54 s stale, so a bbo bid can arrive at or above a
    mirrored ask. Those asks are gone by construction and must be deleted."""
    data = run(tmp_path, [
        snapshot(0, ladder(100.0, -1.0, 5), ladder(101.0, 1.0, 5)),
        bbo(100, (102.5, 3.0), (103.0, 4.0)),
    ])

    rows = depth_rows(data, ms=100)
    for px in (101.0, 102.0):
        assert (100, 'A', px, 0.0) in rows, 'mirrored ask %s survived a crossing bbo' % px
    assert (100, 'B', 102.5, 3.0) in rows
    assert (100, 'A', 103.0, 4.0) in rows
    assert_never_crossed(data)


def test_the_crossed_out_levels_are_deleted_before_the_new_top_is_set(tmp_path):
    """Row by row, not just per frame: the deletion precedes the insert."""
    data = run(tmp_path, [
        snapshot(0, ladder(100.0, -1.0, 5), ladder(101.0, 1.0, 5)),
        bbo(100, (102.5, 3.0), (103.0, 4.0)),
    ])

    rows = depth_rows(data, ms=100)
    delete_ask_102 = rows.index((100, 'A', 102.0, 0.0))
    insert_bid = rows.index((100, 'B', 102.5, 3.0))
    assert delete_ask_102 < insert_bid


def test_a_one_sided_bbo_still_leaves_the_book_uncrossed(tmp_path):
    """A bid alone can cross the mirrored asks; that is not a reason to keep them."""
    data = run(tmp_path, [
        snapshot(0, ladder(100.0, -1.0, 5), ladder(101.0, 1.0, 5)),
        bbo(100, (102.5, 3.0), None),
    ])

    assert_never_crossed(data)
    rows = depth_rows(data, ms=100)
    assert (100, 'A', 101.0, 0.0) in rows
    assert (100, 'A', 102.0, 0.0) in rows


def test_suppressing_out_of_book_deletions_is_refused(tmp_path):
    """``delete_out_of_book=False`` cannot hold with the fusion, so it is refused.

    Suppressing a truncation deletion looks safe within the frame that produces
    it — the kept level is below the new lowest bid, so it cannot cross *yet*.
    It is not safe over time: the level is gone from the mirror the moment it is
    suppressed, so no later diff can ever delete it, and it crosses as soon as
    the market moves through it. Measured on ``btc_20260727``, running the mode
    with the flag off left 1 684 955 of 1 685 014 depth rows crossed and grew the
    book to 569 x 1562 levels.
    """
    path = write_recording(tmp_path / 'rec.gz', [
        snapshot(0, ladder(100.0, -1.0, 5), ladder(101.0, 1.0, 5)),
        bbo(100, (102.5, 3.0), (103.0, 4.0)),
    ])

    with pytest.raises(ValueError, match='delete_out_of_book'):
        convert(path, delete_out_of_book=False)


def test_a_level_a_bbo_evicted_cannot_strand_above_a_later_ask(tmp_path):
    """The invariant the refusal above buys, over more than one frame.

    A ``bbo`` best bid pushes the deepest mirrored bid out of the top-5 window;
    the market then crashes through it. With the deletion emitted, the evicted
    level is gone before the new asks arrive.
    """
    data = run(tmp_path, [
        snapshot(0, ladder(100.0, -1.0, 5), ladder(101.0, 1.0, 5)),
        bbo(100, (100.5, 2.0), (101.0, 1.0)),
        snapshot(200, ladder(90.0, -1.0, 5), ladder(91.0, 1.0, 5)),
    ])

    assert_never_crossed(data)
    assert (100, 'B', 96.0, 0.0) in depth_rows(data, ms=100)


def test_a_bbo_through_the_mirror_thins_the_book_to_one_level(tmp_path):
    """Thinning is not confined to the deepest level.

    A ``bbo`` whose touch moves *through* mirrored levels deletes every one of
    them, and nothing restores them until the next ``fast`` snapshot (median
    540 ms, max 16.3 s measured). Time-weighted over ``btc_20260727`` the fused
    book is 5 levels 96.9 % of the day, and one level 0.64 % of it — ~9 minutes.
    """
    data = run(tmp_path, [
        snapshot(0, ladder(100.0, -1.0, 5), ladder(101.0, 1.0, 5)),
        bbo(100, (95.5, 2.0), (101.0, 1.0)),
    ])

    bids = {}
    for _, bid_book, _ in replay(data, EXCH_EVENT):
        bids = bid_book
    assert sorted(bids) == [95.5], 'all five mirrored bids should have gone with the touch'


def test_the_book_is_never_crossed_through_an_adversarial_sequence(tmp_path):
    """A walk that jumps the touch across the mirror in both directions,
    interleaves snapshots, and empties levels."""
    frames = [snapshot(0, ladder(100.0, -1.0, 5), ladder(101.0, 1.0, 5))]
    ms = 0
    for step in (2.5, -3.5, 0.5, -0.5, 6.0, -6.0, 1.0, -1.0):
        ms += 10
        bid = 100.0 + step
        frames.append(bbo(ms, (bid, 2.0), (bid + 0.5, 3.0)))
        ms += 10
        frames.append(snapshot(ms, ladder(bid, -1.0, 5), ladder(bid + 0.5, 1.0, 5)))
        ms += 10
        frames.append(bbo(ms, (bid + 4.0, 1.0), (bid + 4.5, 1.0)))

    assert_never_crossed(run(tmp_path, frames))


# ---------------------------------------------------------------------------
# 3. reconcile: the snapshot diffs against the book as modified by bbo
# ---------------------------------------------------------------------------


def test_a_snapshot_does_not_re_emit_what_a_bbo_already_set(tmp_path):
    """The snapshot confirms the bbo. Nothing changed, so nothing is emitted."""
    data = run(tmp_path, [
        snapshot(0, ladder(100.0, -1.0, 5), ladder(101.0, 1.0, 5)),
        bbo(100, (100.0, 9.0), (101.0, 1.0)),
        snapshot(200, [lvl(100.0, 9.0)] + ladder(99.0, -1.0, 4, sz=2.0),
                 ladder(101.0, 1.0, 5)),
    ])

    assert depth_rows(data, ms=100) == [(100, 'B', 100.0, 9.0)]
    assert depth_rows(data, ms=200) == []


def test_a_snapshot_that_reverts_a_bbo_change_is_emitted(tmp_path):
    """The other direction, and the one a naive implementation loses.

    Diffing the snapshot against the *previous snapshot* would call this level
    unchanged and emit nothing — leaving the backtest book stuck on the bbo's
    size forever. It has to be diffed against the book the bbo left behind.
    """
    data = run(tmp_path, [
        snapshot(0, ladder(100.0, -1.0, 5), ladder(101.0, 1.0, 5)),
        bbo(100, (100.0, 9.0), (101.0, 1.0)),
        snapshot(200, ladder(100.0, -1.0, 5), ladder(101.0, 1.0, 5)),
    ])

    assert depth_rows(data, ms=100) == [(100, 'B', 100.0, 9.0)]
    assert depth_rows(data, ms=200) == [(200, 'B', 100.0, 1.0)]


def test_a_snapshot_restores_the_level_a_bbo_pushed_out_of_the_window(tmp_path):
    """A new best evicts the deepest mirrored level; the next snapshot, which
    still sees five levels, has to put it back."""
    data = run(tmp_path, [
        snapshot(0, ladder(100.0, -1.0, 5), ladder(101.0, 1.0, 5)),
        bbo(100, (100.5, 2.0), (101.0, 1.0)),
        snapshot(200, ladder(100.0, -1.0, 5), ladder(101.0, 1.0, 5)),
    ])

    # 96.0 (sz 5.0) fell out of the five-level window on the bbo ...
    assert (100, 'B', 96.0, 0.0) in depth_rows(data, ms=100)
    # ... and the snapshot, whose window reaches it again, re-inserts it.
    assert (200, 'B', 96.0, 5.0) in depth_rows(data, ms=200)


def test_a_snapshot_after_a_bbo_deletes_what_the_bbo_inserted_and_the_venue_did_not_have(
        tmp_path):
    """A bbo touch that the next snapshot does not confirm is a level to remove,
    not one to leave in the book."""
    data = run(tmp_path, [
        snapshot(0, ladder(100.0, -1.0, 5), ladder(101.0, 1.0, 5)),
        bbo(100, (100.5, 2.0), (101.0, 1.0)),
        snapshot(200, ladder(100.0, -1.0, 5), ladder(101.0, 1.0, 5)),
    ])

    assert (200, 'B', 100.5, 0.0) in depth_rows(data, ms=200)


# ---------------------------------------------------------------------------
# 4. missing sides
# ---------------------------------------------------------------------------


def test_a_null_bbo_side_leaves_that_side_untouched(tmp_path):
    """Measured on four coins over two days: Hyperliquid never sent one. The
    field is typed as nullable, so a null must mean "no news", not "empty"."""
    data = run(tmp_path, [
        snapshot(0, ladder(100.0, -1.0, 5), ladder(101.0, 1.0, 5)),
        bbo(100, None, (101.0, 6.0)),
    ])

    assert depth_rows(data, ms=100) == [(100, 'A', 101.0, 6.0)]


def test_a_bbo_with_neither_side_emits_nothing(tmp_path):
    data = run(tmp_path, [
        snapshot(0, ladder(100.0, -1.0, 5), ladder(101.0, 1.0, 5)),
        bbo(100, None, None),
    ])

    assert depth_rows(data, ms=100) == []


def test_a_recording_of_only_bbo_frames_still_converts(tmp_path):
    """No snapshot ever arrives; the mirror bootstraps from the touch feed."""
    data = run(tmp_path, [
        bbo(0, (100.0, 1.0), (101.0, 1.0)),
        bbo(10, (100.5, 2.0), (101.0, 1.0)),
    ])

    assert depth_rows(data, ms=0) == [(0, 'B', 100.0, 1.0), (0, 'A', 101.0, 1.0)]
    assert depth_rows(data, ms=10) == [(10, 'B', 100.5, 2.0)]


# ---------------------------------------------------------------------------
# 5. cadence selection and validation
# ---------------------------------------------------------------------------


def test_slow_snapshots_are_ignored_in_bbo_fast_mode(tmp_path):
    """A 20-level frame fed to a 5-level differ overruns its buffers; the mode
    takes the fast cadence and nothing else."""
    data = run(tmp_path, [
        snapshot(0, ladder(100.0, -1.0, 5), ladder(101.0, 1.0, 5)),
        snapshot(100, ladder(200.0, -1.0, 20), ladder(201.0, 1.0, 20), fast=False),
        bbo(200, (100.5, 2.0), (101.0, 1.0)),
    ])

    assert depth_rows(data, ms=100) == []
    assert depth_rows(data, ms=200) == [
        (200, 'B', 96.0, 0.0),
        (200, 'B', 100.5, 2.0),
    ]


def test_bbo_fast_requires_five_levels(tmp_path):
    path = write_recording(tmp_path / 'rec.gz', [
        snapshot(0, ladder(100.0, -1.0, 5), ladder(101.0, 1.0, 5)),
    ])
    with pytest.raises(ValueError, match='num_levels'):
        convert(path, num_levels=20)


@pytest.mark.parametrize('mode,wrong', [('slow', 5), ('fast', 20)])
def test_every_book_mode_requires_its_own_depth(tmp_path, mode, wrong):
    """The pairing is refused for the older modes too, not only for ``bbo+fast``.

    Every snapshot is truncated to ``num_levels`` — it has to be, because
    ``DiffOrderBookSnapshot`` writes ``len(bid_px)`` rows into a ``num_levels``
    buffer with no bounds check. Truncating silently turns a mispaired call into
    a half-depth book rather than an error: ``num_levels=5`` on a ``slow``
    recording returns a five-deep book from a twenty-level feed and says nothing.
    """
    path = write_recording(tmp_path / 'rec.gz', [
        snapshot(0, ladder(100.0, -1.0, 20), ladder(101.0, 1.0, 20), fast=False),
        snapshot(100, ladder(100.0, -1.0, 5), ladder(101.0, 1.0, 5)),
    ])
    with pytest.raises(ValueError, match='num_levels'):
        convert(path, book_mode=mode, num_levels=wrong)


def test_an_unknown_book_mode_names_bbo_fast(tmp_path):
    """The old message said fusion was unimplemented. It is implemented now."""
    path = write_recording(tmp_path / 'rec.gz', [
        snapshot(0, ladder(100.0, -1.0, 5), ladder(101.0, 1.0, 5)),
    ])
    with pytest.raises(ValueError) as e:
        convert(path, book_mode='nonsense')
    assert 'bbo+fast' in str(e.value)
    assert 'not implemented' not in str(e.value)


def test_trade_dedup_is_untouched_by_the_new_mode(tmp_path):
    """The tid ring is orthogonal to the book and stays that way."""
    stats = {}
    data = run(tmp_path, [
        snapshot(0, ladder(100.0, -1.0, 5), ladder(101.0, 1.0, 5)),
        trade(10, 100.0, 1.0, tid=1),
        trade(20, 100.0, 2.0, tid=2),
        trade(30, 100.0, 1.0, tid=1),
    ], stats=stats)

    assert stats['deduplicated_trades'] == 1
    trades = [r for r in data
              if int(r['ev']) & 0xff == TRADE_EVENT and int(r['ev']) & EXCH_EVENT]
    assert len(trades) == 2


def test_a_recording_with_no_convertible_frame_still_refuses(tmp_path):
    path = write_recording(tmp_path / 'rec.gz', [
        snapshot(0, ladder(100.0, -1.0, 20), ladder(101.0, 1.0, 20), fast=False),
    ])
    with pytest.raises(ValueError, match='no market-data records'):
        convert(path)


# ---------------------------------------------------------------------------
# 6. the pre-existing modes are byte-for-byte what they were
# ---------------------------------------------------------------------------

#: A recording that exercises both cadences, a bbo frame, a trade, level counts
#: that change, insertions, in-book deletions and out-of-book deletions.
PIN_FRAMES = [
    snapshot(0, ladder(100.0, -1.0, 20), ladder(101.0, 1.0, 20), fast=False),
    snapshot(100, ladder(100.0, -1.0, 5), ladder(101.0, 1.0, 5)),
    bbo(150, (100.5, 2.0), (101.0, 3.0)),
    trade(160, 100.5, 0.5, tid=7),
    snapshot(200, ladder(100.5, -1.0, 5), ladder(101.0, 1.0, 5)),
    bbo(250, (100.0, 4.0), (101.5, 5.0)),
    snapshot(300, ladder(101.0, -1.0, 20), ladder(102.0, 1.0, 20), fast=False),
    snapshot(400, ladder(100.0, -1.0, 5), ladder(101.0, 1.0, 5)),
]

#: sha256 of the converted array's bytes, captured before ``bbo+fast`` existed.
PIN = {
    'slow': (83, 'b9a6aa541da633599aabf9d94cda741f38a64c9ff398a191fafe113e7a84b1b0'),
    'fast': (31, 'e36ff2b1b561c2775b083bf22e5f1271ab2938cfdbf75da9f11e2ad59ee74459'),
}


@pytest.mark.parametrize('mode', sorted(PIN))
def test_the_pre_existing_modes_are_unchanged(tmp_path, mode):
    rows, digest = PIN[mode]
    data = run(tmp_path, PIN_FRAMES, book_mode=mode,
               num_levels=20 if mode == 'slow' else 5)

    assert len(data) == rows
    assert hashlib.sha256(data.tobytes()).hexdigest() == digest


def test_the_pre_existing_modes_still_ignore_bbo_frames(tmp_path):
    """Stated directly, so the pin above cannot be the only thing holding it."""
    without = [f for f in PIN_FRAMES if f[1]['channel'] != 'bbo']
    for mode, levels in (('slow', 20), ('fast', 5)):
        a = run(tmp_path / 'a', PIN_FRAMES, book_mode=mode, num_levels=levels)
        b = run(tmp_path / 'b', without, book_mode=mode, num_levels=levels)
        assert np.array_equal(a, b)


# ---------------------------------------------------------------------------
# 7. real-recording spot checks
# ---------------------------------------------------------------------------


def _slice_real(tmp_path, lines):
    out = tmp_path / 'real.gz'
    with gzip.open(REAL, 'rb') as src, gzip.open(out, 'wb') as dst:
        for i, line in enumerate(src):
            if i >= lines:
                break
            dst.write(line)
    return out


@pytest.mark.skipif(not REAL.exists(), reason='real recording not on this machine')
def test_a_real_slice_is_finer_than_fast_alone_and_never_crossed(tmp_path):
    src = _slice_real(tmp_path, 60_000)
    fused = hyperliquid.convert(str(src), tick_size=1.0, lot_size=0.00001,
                                num_levels=5, book_mode='bbo+fast',
                                buffer_size=5_000_000)
    fast = hyperliquid.convert(str(src), tick_size=1.0, lot_size=0.00001,
                               num_levels=5, book_mode='fast',
                               buffer_size=5_000_000)

    assert len(fused) > len(fast)
    assert_never_crossed(fused)
    ev = fused['ev']
    assert np.all(ev & (EXCH_EVENT | LOCAL_EVENT) != 0), 'a row carries neither bit'
    assert np.all(np.isin(ev & 0xff, [DEPTH_EVENT, TRADE_EVENT]))


@pytest.mark.skipif(not REAL.exists(), reason='real recording not on this machine')
def test_a_real_slice_keeps_the_trades_of_fast_mode_exactly(tmp_path):
    """Fusing the book must not touch the trade stream.

    The EXCH/LOCAL bits are deliberately not compared. ``correct_event_order``
    splits a row into an exchange copy and a local copy when the two orderings
    disagree *at that point in the whole stream*, so adding depth rows can turn
    a both-bits trade row into a split pair without the trade itself changing.
    """
    src = _slice_real(tmp_path, 60_000)
    fused = hyperliquid.convert(str(src), tick_size=1.0, lot_size=0.00001,
                                num_levels=5, book_mode='bbo+fast',
                                buffer_size=5_000_000)
    fast = hyperliquid.convert(str(src), tick_size=1.0, lot_size=0.00001,
                               num_levels=5, book_mode='fast',
                               buffer_size=5_000_000)

    def trades(d, bit):
        m = ((d['ev'] & 0xff) == TRADE_EVENT) & ((d['ev'] & bit) != 0)
        rows = d[m]
        return (rows['ev'] & ~np.uint64(EXCH_EVENT | LOCAL_EVENT),
                rows['exch_ts'], rows['local_ts'], rows['px'], rows['qty'])

    for bit in (EXCH_EVENT, LOCAL_EVENT):
        for a, b in zip(trades(fused, bit), trades(fast, bit)):
            assert np.array_equal(a, b)
