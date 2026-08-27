"""Tests for ``hftbacktest.data.utils.binancefutures.convert``.

Every recording here is synthetic: a tiny gzip file with known lines written
into ``tmp_path``. No network, no real recording.

The subject is what the converter does with a line that is neither a combined-
stream frame nor a REST depth snapshot. Our own collector writes such lines by
design — ``binancefuturesum`` polls ``GET /fapi/v1/premiumIndex`` every ten
seconds and files each element under its symbol, bare and verbatim — and until
this was guarded, the converter reached for ``message['T']`` on the first one
and died with ``KeyError: 'T'``. Measured 2026-08-27 on a live day file from the
Tokyo host: every USD-M recording made since the poller existed (2026-07-28) was
unconvertible, and ``collector/README.md`` claimed the opposite.
"""

import gzip
import json
from collections import Counter

import numpy as np

from hftbacktest.data.utils import binancefutures
from hftbacktest.types import (
    BUY_EVENT,
    DEPTH_CLEAR_EVENT,
    DEPTH_EVENT,
    DEPTH_SNAPSHOT_EVENT,
    SELL_EVENT,
    TRADE_EVENT,
)

MS = 1_000_000
#: 2026-08-27T00:00:00Z in milliseconds. The reader slices a fixed 19 digits off
#: the front of every line, which is what a nanosecond stamp of this era is.
BASE_MS = 1_787_788_800_000


def line(local_ms, payload):
    return f'{local_ms * MS} {json.dumps(payload)}\n'


def frame(stream, data):
    return {'stream': stream, 'data': data}


def depth_update(ms, u, pu):
    return frame(
        'btcusdc@depth@0ms',
        {
            'e': 'depthUpdate', 'E': ms, 'T': ms, 's': 'BTCUSDC',
            'U': u - 5, 'u': u, 'pu': pu,
            'b': [['100.0', '1.0']], 'a': [['101.0', '2.0']],
        },
    )


def trade(ms):
    return frame(
        'btcusdc@trade',
        {
            'e': 'trade', 'E': ms, 'T': ms, 's': 'BTCUSDC', 't': 1,
            'p': '100.5', 'q': '0.5', 'X': 'MARKET', 'm': True,
        },
    )


def book_ticker(ms, u=1):
    return frame(
        'btcusdc@bookTicker',
        {
            'e': 'bookTicker', 'u': u, 's': 'BTCUSDC',
            'b': '100.0', 'B': '1', 'a': '101.0', 'A': '2', 'T': ms, 'E': ms,
        },
    )


def rest_snapshot(ms):
    """The REST book, filed bare — no envelope — exactly as the collector files
    it after a break in the incremental feed."""
    return {
        'lastUpdateId': 1000, 'E': ms, 'T': ms,
        'bids': [['100.0', '1.0'], ['99.0', '3.0']],
        'asks': [['101.0', '2.0'], ['102.0', '4.0']],
    }


def premium_index(ms):
    """One element of `GET /fapi/v1/premiumIndex`, captured verbatim from
    `fapi.binance.com` on 2026-08-27 (symbol changed to the recorded one).

    No `data`, no `code`, and — the detail that mattered — no `T`.
    """
    return {
        'symbol': 'BTCUSDC', 'markPrice': '112263.30000000',
        'indexPrice': '112281.62628571', 'estimatedSettlePrice': '112277.63449275',
        'lastFundingRate': '0.00005479', 'interestRate': '0.00010000',
        'nextFundingTime': 1787817600000, 'time': ms,
    }


def write(path, lines):
    with gzip.open(path, 'wt') as f:
        f.writelines(lines)
    return str(path)


def test_a_recording_with_the_index_poller_in_it_converts(tmp_path):
    """The whole of the bug: one poller line used to end the conversion.

    A day file holds ~121 of these per symbol, so this is not an edge case —
    it is every USD-M file the collector has written since 2026-07-28.
    """
    src = write(tmp_path / 'btcusdc_20260827.gz', [
        line(BASE_MS + 0, depth_update(BASE_MS + 0, u=100, pu=95)),
        line(BASE_MS + 1, premium_index(BASE_MS + 1)),
        line(BASE_MS + 2, trade(BASE_MS + 2)),
        line(BASE_MS + 3, premium_index(BASE_MS + 3)),
        line(BASE_MS + 4, depth_update(BASE_MS + 4, u=110, pu=100)),
    ])

    out = binancefutures.convert(src)

    kinds = Counter(int(ev) & 0xFF for ev in out['ev'])
    assert kinds[DEPTH_EVENT] == 4, 'two diffs, one level each side'
    assert kinds[TRADE_EVENT] == 1
    assert len(out) == 5, 'and nothing at all from the two poller lines'


def test_a_rest_snapshot_is_still_converted(tmp_path):
    """The guard must not eat what it guards.

    The snapshot is filed bare too, so it travels the same branch as the poller
    line; the difference is that it carries a book. Identifying it by
    `bids`/`asks` is what separates them, and this is the half that would break
    if the guard were written as "skip the shapes we know about" instead.
    """
    src = write(tmp_path / 'btcusdc_20260827.gz', [
        line(BASE_MS + 0, premium_index(BASE_MS + 0)),
        line(BASE_MS + 1, rest_snapshot(BASE_MS + 1)),
        line(BASE_MS + 2, depth_update(BASE_MS + 2, u=1010, pu=1000)),
    ])

    out = binancefutures.convert(src)

    kinds = Counter(int(ev) & 0xFF for ev in out['ev'])
    assert kinds[DEPTH_SNAPSHOT_EVENT] == 4, 'two levels each side of the book'
    assert kinds[DEPTH_CLEAR_EVENT] == 2, 'one clear per side, before the levels'
    assert kinds[DEPTH_EVENT] == 2

    snapshot_bids = sorted(
        float(row['px']) for row in out
        if int(row['ev']) & 0xFF == DEPTH_SNAPSHOT_EVENT and int(row['ev']) & BUY_EVENT
    )
    assert snapshot_bids == [99.0, 100.0]


def test_the_touch_is_converted_when_asked_for(tmp_path):
    """`opt='t'` is how the touch reaches the dataset; the poller line must not
    disturb that path either."""
    src = write(tmp_path / 'btcusdc_20260827.gz', [
        line(BASE_MS + 0, book_ticker(BASE_MS + 0)),
        line(BASE_MS + 1, premium_index(BASE_MS + 1)),
        line(BASE_MS + 2, book_ticker(BASE_MS + 2, u=2)),
    ])

    out = binancefutures.convert(src, opt='t')

    kinds = Counter(int(ev) & 0xFF for ev in out['ev'])
    assert kinds[103] == 2 and kinds[104] == 2, 'best bid and best ask per frame'


def test_an_unknown_bare_line_is_skipped_rather_than_read_as_a_book(tmp_path):
    """Fail closed on shape, not on a list of known strangers.

    A REST answer the converter has never seen — a future poller, an error body
    that carries no `code` — must not be read as a snapshot just because it has
    a timestamp. Reading one as a book would insert phantom levels and clear
    real ones, which is worse than dropping it and quieter than crashing.
    """
    src = write(tmp_path / 'btcusdc_20260827.gz', [
        line(BASE_MS + 0, {'symbol': 'BTCUSDC', 'openInterest': '512.9', 'time': BASE_MS}),
        line(BASE_MS + 1, {'T': BASE_MS + 1, 'E': BASE_MS + 1, 'somethingNew': [['1', '2']]}),
        line(BASE_MS + 2, trade(BASE_MS + 2)),
    ])

    out = binancefutures.convert(src)

    assert len(out) == 1
    assert int(out['ev'][0]) & 0xFF == TRADE_EVENT
