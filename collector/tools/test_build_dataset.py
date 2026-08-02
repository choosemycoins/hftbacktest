"""Tests for ``build_dataset.py`` — Phase 3 of ``docs/design-multi-venue-collection.md``.

Every fixture here is synthetic: tiny gzip files written into ``tmp_path`` with
known contents, gaps and timestamps. No network, no real recordings.

The one test that exercises the real ``hftbacktest.data.utils.hyperliquid``
converter is guarded with ``importorskip`` — the native module may not be built.
Everything else injects a fake converter, which is also what proves the
time-policy gate runs *before* any conversion.
"""

import gzip
import hashlib
import json
import sys
import tracemalloc
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))

import build_dataset as bd  # noqa: E402

# ---------------------------------------------------------------------------
# fixtures
# ---------------------------------------------------------------------------

NS = 1_000_000_000
MS = 1_000_000

DAY0 = '20260725'
DAY1 = '20260726'
# ~1.784e18 ns — deliberately in the range where float64 has 256 ns granularity.
DAY0_START = int(datetime(2026, 7, 25, tzinfo=timezone.utc).timestamp()) * NS
DAY1_START = DAY0_START + 86_400 * NS


def hl_l2book_levels(coin, time_ms, bids, asks, fast=False):
    """One ``l2Book`` frame with as many levels a side as it is given.

    The two cadences differ in depth as well as in period — 20 levels every
    ~5.4 s against 5 every ~0.54 s — so a fixture that wants to tell them apart
    has to be able to write the deep one.
    """
    data = {
        'coin': coin,
        'time': time_ms,
        'levels': [
            [{'px': str(px), 'sz': '1.0', 'n': 1} for px in bids],
            [{'px': str(px), 'sz': '2.0', 'n': 1} for px in asks],
        ],
    }
    if fast:
        data['fast'] = True
    return json.dumps({'channel': 'l2Book', 'data': data})


def hl_l2book(coin, time_ms, bid_px, ask_px, fast=False):
    return hl_l2book_levels(coin, time_ms, [bid_px], [ask_px], fast=fast)


def hl_trades(coin, fills, side='B'):
    """One `trades` frame. `fills` is an iterable of `(time_ms, px, sz, tid)`.

    A `tid` of `None` is left out of the entry entirely, which is how a
    recording made before the field was captured looks.
    """
    data = []
    for time_ms, px, sz, tid in fills:
        entry = {'coin': coin, 'side': side, 'px': str(px), 'sz': str(sz), 'time': time_ms}
        if tid is not None:
            entry['tid'] = tid
        data.append(entry)
    return json.dumps({'channel': 'trades', 'data': data})


def hl_trade(coin, time_ms, px, sz, side='B', tid=None):
    return hl_trades(coin, [(time_ms, px, sz, tid)], side=side)


def hl_bbo(coin, time_ms, bid_px, ask_px):
    return json.dumps({
        'channel': 'bbo',
        'data': {
            'coin': coin,
            'time': time_ms,
            'bbo': [
                {'px': str(bid_px), 'sz': '1.0', 'n': 1},
                {'px': str(ask_px), 'sz': '1.0', 'n': 1},
            ],
        },
    })


def binance_book_ticker(symbol, ts_ms, bid_px, bid_qty, ask_px, ask_qty, u=1,
                        drop_update_id=False):
    """One ``@bookTicker`` frame.

    ``u`` is the venue's order-book update id and the key the two-socket union
    deduplicates on; ``drop_update_id`` writes the frame without it, which is
    what a recording the union cannot be built from looks like.
    """
    data = {
        'e': 'bookTicker', 'u': u, 's': symbol.upper(),
        'b': str(bid_px), 'B': str(bid_qty),
        'a': str(ask_px), 'A': str(ask_qty),
        'T': ts_ms, 'E': ts_ms,
    }
    if drop_update_id:
        del data['u']
    return json.dumps({'stream': f'{symbol.lower()}@bookTicker', 'data': data})


def binance_trade(symbol, ts_ms, px, qty):
    return json.dumps({
        'stream': f'{symbol.lower()}@trade',
        'data': {
            'e': 'trade', 'E': ts_ms, 'T': ts_ms, 's': symbol.upper(),
            't': 1, 'p': str(px), 'q': str(qty), 'X': 'MARKET', 'm': True,
        },
    })


def write_gz(path, records):
    """``records`` is an iterable of ``(local_ts_ns, payload_str)``."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with gzip.open(path, 'wb') as f:
        for local_ts, payload in records:
            f.write(('%d %s\n' % (local_ts, payload)).encode())
    return path


def write_meta(path, records):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, 'w') as f:
        for local_ts, obj in records:
            f.write('%d %s\n' % (local_ts, json.dumps(obj)))
    return path


def session_start(exchange, symbols, commit='abc1234'):
    return {
        '_collector': 'session_start', 'version': '0.1.0', 'commit': commit,
        'branch': 'feat/x', 'dirty': 'false', 'exchange': exchange,
        'symbols': symbols, 'bybit_depths': None, 'hl_l2_modes': ['slow', 'fast'],
    }


def universe_record(wire='BTC', sz_decimals=5):
    return {
        '_collector': 'universe',
        'symbols': [{
            'wire': wire, 'dex': '', 'collateral': 'USDC',
            'szDecimals': sz_decimals, 'maxLeverage': 40,
        }],
    }


def hl_day_lines(day_start, *, n=6, base_ms=None, latency_ns=200 * MS, fast=False):
    """A day of HL frames: slow book, fast book, bbo and trades, all sane."""
    base_ms = base_ms if base_ms is not None else day_start // MS
    out = []
    for i in range(n):
        exch_ms = base_ms + i * 1000
        local = exch_ms * MS + latency_ns
        out.append((local, hl_l2book('BTC', exch_ms, 100 + i, 101 + i)))
        out.append((local + 1000, hl_l2book('BTC', exch_ms, 100 + i, 101 + i, fast=True)))
        out.append((local + 2000, hl_bbo('BTC', exch_ms, 100 + i, 101 + i)))
        out.append((local + 3000, hl_trade('BTC', exch_ms, 100 + i, 0.5)))
    return out


#: ONDO on 2026-07-29, shortened: every recorded price on a 1e-5 grid while
#: szDecimals=0 says the tick is 1e-6. Prices are strings so the decimals reach
#: the recording exactly as written.
ONDO_QUOTES = [
    ('0.39040', '0.39045'),
    ('0.40000', '0.40005'),
    ('0.41209', '0.41210'),
    ('0.39875', '0.39880'),
    ('0.40155', '0.40160'),
    ('0.40500', '0.40505'),
]

#: BTC on 2026-07-28 was 63022..63823, one decade below the step that matters:
#: above 100 000 five significant figures make the venue tick 10, measured on
#: testnet 2026-07-28 (`design-hyperliquid-connector.md` §11.5 — `123456` is
#: rejected and rounds to `123460`). Every price here is a multiple of 10 and not
#: all of them are multiples of 100, so the coarsest grid they land on is 10.
HIGH_PRICE_QUOTES = [
    ('118230', '118240'),
    ('118240', '118250'),
    ('118250', '118260'),
    ('118260', '118270'),
]

#: A window that crosses a power of ten: 1e-4 below it, 1e-3 above. The venue
#: tick is a step function of the price and this window contains the step.
DECADE_CROSSING_QUOTES = [
    ('9.9995', '9.9996'),
    ('9.9998', '9.9999'),
    ('10.001', '10.002'),
    ('10.003', '10.004'),
    ('10.005', '10.006'),
    ('9.9990', '9.9991'),
]


def hl_price_day(day_start, quotes, *, coin='BTC', latency_ns=200 * MS):
    """A day of HL frames quoting exactly ``quotes`` — ``(bid, ask)`` price strings.

    Every channel the builder reads carries the same pair, so the grid the build
    measures is the grid of the strings written here and of nothing else.
    """
    base_ms = day_start // MS
    out = []
    for i, (bid, ask) in enumerate(quotes):
        exch_ms = base_ms + i * 1000
        local = exch_ms * MS + latency_ns
        out.append((local, hl_l2book(coin, exch_ms, bid, ask)))
        out.append((local + 1000, hl_l2book(coin, exch_ms, bid, ask, fast=True)))
        out.append((local + 2000, hl_bbo(coin, exch_ms, bid, ask)))
        out.append((local + 3000, hl_trade(coin, exch_ms, bid, 0.5)))
    return out


def bn_day_lines(day_start, *, n=6, base_ms=None, latency_ns=5 * MS):
    """A plain day of ``@bookTicker`` + ``@trade`` for one symbol.

    ``u`` is keyed off the exchange millisecond rather than left at the default,
    so every frame here carries a distinct update id and two days of this fixture
    never reuse one. The venue's own ids behave that way — ``u`` is the order
    book update id, so one value describes exactly one book — and a fixture that
    gave six different books the same id would be asserting the one thing
    :func:`build_signal_union` refuses to build over. The ids are far away from
    the small ones :func:`bn_union_lines` uses, which is what lets the two
    generators appear in one build without claiming to describe the same update.
    """
    base_ms = base_ms if base_ms is not None else day_start // MS
    out = []
    for i in range(n):
        exch_ms = base_ms + i * 1000
        local = exch_ms * MS + latency_ns
        out.append((local, binance_book_ticker('BTCUSDT', exch_ms, 100 + i, 1, 101 + i, 2,
                                               u=exch_ms)))
        out.append((local + 1000, binance_trade('BTCUSDT', exch_ms, 100 + i, 0.1)))
    return out


def day_entry(verdict='green', symbols=None):
    """One `days[<day>]` value in the Phase-2 shape."""
    return {'verdict': verdict, 'issues': [], 'symbols': symbols or {}}


def symbol_entry(first, last, streams=('bookTicker',)):
    """One `days[].symbols[<name>]` value, as far as Phase 3 reads it."""
    return {
        'coverage': {
            'first_local_ts': first,
            'last_local_ts': last,
            'required_streams': list(streams),
        },
    }


def make_report(path, hl_dir, bn_dir, hl_cov, bn_cov, *, verdict='green',
                hl_days=None, bn_days=None, hl_symbol='btc', bn_symbol='btcusdt'):
    """A Phase-2 report over one symbol per venue.

    `hl_days`/`bn_days` may be given as `{day: {'verdict': ...}}`; the per-symbol
    coverage Phase 3 actually trims on is then filled in from the venue range,
    which is what a single-symbol recording really produces.
    """
    def venue(data_dir, cov, days, symbol, streams):
        days = days or {DAY0: {'verdict': 'green'}}
        out = {}
        for day, entry in days.items():
            entry = dict(entry)
            entry.setdefault('issues', [])
            if 'symbols' not in entry:
                entry['symbols'] = {symbol: symbol_entry(cov[0], cov[1], streams)}
            out[day] = entry
        return {
            'data_dir': str(data_dir),
            'exchange_as_recorded': None,
            'coverage': {'first_local_ts': cov[0], 'last_local_ts': cov[1]},
            'days': out,
        }

    report = {
        'schema': 'quality-report-v1',
        'verdict': verdict,
        'venues': {
            'hyperliquid': venue(hl_dir, hl_cov, hl_days, hl_symbol,
                                 ('trades', 'bbo', 'l2Book_slow', 'l2Book_fast')),
            'binancefuturesum': venue(bn_dir, bn_cov, bn_days, bn_symbol,
                                      ('bookTicker',)),
        },
    }
    path.write_text(json.dumps(report))
    return path


class FakeConverter:
    """Stands in for ``hyperliquid.convert``: same selection rules, one row per frame.

    It reads the file it is handed, which is what makes it useful — the
    orchestration test can then assert the converter saw *trimmed* input.
    """

    def __init__(self):
        self.calls = []

    def __call__(self, *, input_filename, tick_size, lot_size, num_levels,
                 book_mode, base_latency=0, buffer_size=0, output_filename=None,
                 **kwargs):
        self.calls.append({
            'input_filename': input_filename, 'tick_size': tick_size,
            'lot_size': lot_size, 'num_levels': num_levels,
            'book_mode': book_mode, 'base_latency': base_latency,
            **kwargs,
        })
        rows = []
        with gzip.open(input_filename, 'rb') as f:
            for line in f:
                local_ts, payload = line.split(b' ', 1)
                msg = json.loads(payload)
                ch = msg.get('channel')
                if ch == 'trades':
                    for t in msg['data']:
                        rows.append((int(local_ts), t['time'] * MS))
                elif ch == 'l2Book':
                    is_fast = bool(msg['data'].get('fast', False))
                    if (book_mode == 'slow') == is_fast:
                        continue
                    rows.append((int(local_ts), msg['data']['time'] * MS))
                elif ch == 'bbo' and book_mode in bd.BBO_BOOK_MODES:
                    rows.append((int(local_ts), msg['data']['time'] * MS))
        arr = np.zeros(len(rows), bd.EVENT_DTYPE)
        for i, (local_ts, exch_ts) in enumerate(rows):
            arr[i]['local_ts'] = local_ts
            arr[i]['exch_ts'] = exch_ts
        if output_filename is not None:
            np.savez_compressed(output_filename, data=arr)
        return arr


class DedupReportingConverter(FakeConverter):
    """A converter that fills the `stats` out-param, as the real one does.

    `dropped` may be a list, one entry per call, so a build over several days
    can report a different count for each — or nothing at all for one of them.
    """

    def __init__(self, dropped=3):
        super().__init__()
        self.dropped = dropped if isinstance(dropped, list) else [dropped]

    def __call__(self, *, stats=None, **kwargs):
        arr = super().__call__(**kwargs)
        n = self.dropped[min(len(self.calls) - 1, len(self.dropped) - 1)]
        if stats is not None and n is not None:
            stats['deduplicated_trades'] = n
        return arr


class BboReportingConverter(FakeConverter):
    """A converter that reports how many `bbo` frames it fused, as the real one does.

    `fused` may be a list, one entry per call, so a multi-day build can report a
    different count per day — or `None` for a day, which is "did not report".
    """

    def __init__(self, fused=7):
        super().__init__()
        self.fused = fused if isinstance(fused, list) else [fused]

    def __call__(self, *, stats=None, **kwargs):
        arr = super().__call__(**kwargs)
        n = self.fused[min(len(self.calls) - 1, len(self.fused) - 1)]
        if stats is not None and n is not None:
            stats['bbo_depth_frames'] = n
        return arr


class OldConverter:
    """An installed `hftbacktest` predating the out-param: no `stats`, no `**kwargs`.

    Passing `stats` to this raises `TypeError`, which is the failure the build
    must not have.
    """

    def __init__(self):
        self._inner = FakeConverter()
        self.calls = self._inner.calls

    def __call__(self, *, input_filename, tick_size, lot_size, num_levels, book_mode,
                 base_latency=0, buffer_size=0, output_filename=None,
                 delete_out_of_book=True, exch_ts_multiplier=MS):
        return self._inner(
            input_filename=input_filename, tick_size=tick_size, lot_size=lot_size,
            num_levels=num_levels, book_mode=book_mode, base_latency=base_latency,
            buffer_size=buffer_size, output_filename=output_filename,
            delete_out_of_book=delete_out_of_book, exch_ts_multiplier=exch_ts_multiplier,
        )


class FakeSnapshotter:
    def __init__(self):
        self.calls = []

    def __call__(self, data, tick_size, lot_size, initial_snapshot=None,
                 output_snapshot_filename=None):
        self.calls.append({
            'data': list(data), 'tick_size': tick_size, 'lot_size': lot_size,
            'initial_snapshot': initial_snapshot,
            'output_snapshot_filename': output_snapshot_filename,
        })
        arr = np.zeros(1, bd.EVENT_DTYPE)
        if output_snapshot_filename is not None:
            np.savez_compressed(output_snapshot_filename, data=arr)
        return arr


@pytest.fixture
def dataset(tmp_path):
    """A one-day, clean, overlapping recording of both venues."""
    hl_dir = tmp_path / 'hl'
    bn_dir = tmp_path / 'bn'
    hl_lines = hl_day_lines(DAY0_START)
    bn_lines = bn_day_lines(DAY0_START)
    write_gz(hl_dir / f'btc_{DAY0}.gz', hl_lines)
    write_gz(bn_dir / f'btcusdt_{DAY0}.gz', bn_lines)
    write_meta(hl_dir / f'_meta_hyperliquid_{DAY0}.jsonl', [
        (DAY0_START, session_start('hyperliquid', ['BTC'])),
        (DAY0_START + 1, universe_record('BTC', 5)),
    ])
    write_meta(bn_dir / f'_meta_binancefuturesum_{DAY0}.jsonl', [
        (DAY0_START, session_start('binancefuturesum', ['BTCUSDT'], commit='def5678')),
    ])
    report = make_report(
        tmp_path / 'report.json', hl_dir, bn_dir,
        (hl_lines[0][0], hl_lines[-1][0]),
        (bn_lines[0][0], bn_lines[-1][0]),
    )
    return {
        'root': tmp_path, 'hl_dir': hl_dir, 'bn_dir': bn_dir, 'report': report,
        'out': tmp_path / 'out', 'hl_lines': hl_lines, 'bn_lines': bn_lines,
    }


@pytest.fixture
def priced_dataset(tmp_path):
    """A one-day recording whose Hyperliquid prices are chosen by the test.

    Same shape as :func:`dataset`, but the HL day quotes exactly the pairs given
    and the sidecar's ``szDecimals`` is a parameter — which is what the tick
    measurement and its cross-check against the venue rule are made of.
    """
    def make(quotes, *, sz_decimals=0):
        hl_dir = tmp_path / 'hl'
        bn_dir = tmp_path / 'bn'
        hl_lines = hl_price_day(DAY0_START, quotes)
        bn_lines = bn_day_lines(DAY0_START)
        write_gz(hl_dir / f'btc_{DAY0}.gz', hl_lines)
        write_gz(bn_dir / f'btcusdt_{DAY0}.gz', bn_lines)
        write_meta(hl_dir / f'_meta_hyperliquid_{DAY0}.jsonl', [
            (DAY0_START, session_start('hyperliquid', ['BTC'])),
            (DAY0_START + 1, universe_record('BTC', sz_decimals)),
        ])
        write_meta(bn_dir / f'_meta_binancefuturesum_{DAY0}.jsonl', [
            (DAY0_START, session_start('binancefuturesum', ['BTCUSDT'],
                                       commit='def5678')),
        ])
        report = make_report(
            tmp_path / 'report.json', hl_dir, bn_dir,
            (hl_lines[0][0], hl_lines[-1][0]),
            (bn_lines[0][0], bn_lines[-1][0]),
        )
        return {
            'root': tmp_path, 'hl_dir': hl_dir, 'bn_dir': bn_dir, 'report': report,
            'out': tmp_path / 'out', 'hl_lines': hl_lines, 'bn_lines': bn_lines,
        }
    return make


def base_argv(ds, **overrides):
    argv = [
        '--quality-report', str(ds['report']),
        '--hl-symbol', 'BTC',
        '--binance-symbol', 'BTCUSDT',
        '--out-dir', str(ds['out']),
        '--min-window-hours', '0',
    ]
    for k, v in overrides.items():
        argv += ['--' + k.replace('_', '-'), str(v)]
    return argv


# ---------------------------------------------------------------------------
# 3.1 window = intersection
# ---------------------------------------------------------------------------

def test_intersect_takes_the_overlap():
    w = bd.intersect((10, 20), (15, 30))
    assert (w.start_ns, w.end_ns) == (15, 20)
    assert w.duration_ns == 5
    assert not w.is_empty


def test_intersect_of_disjoint_ranges_is_empty():
    w = bd.intersect((10, 20), (30, 40))
    assert w.is_empty


def test_require_window_rejects_empty_and_names_the_numbers():
    w = bd.intersect((10, 20), (30, 40))
    with pytest.raises(bd.BuildError) as e:
        bd.require_window(w, ((10, 20), (30, 40)), min_window_ns=0)
    msg = str(e.value)
    assert '10' in msg and '20' in msg and '30' in msg and '40' in msg


def test_require_window_rejects_a_window_shorter_than_the_minimum():
    w = bd.intersect((0, 1800 * NS), (0, 3600 * NS))
    with pytest.raises(bd.BuildError) as e:
        bd.require_window(w, ((0, 1800 * NS), (0, 3600 * NS)), min_window_ns=3600 * NS)
    assert '1800' in str(e.value) or '1800000000000' in str(e.value)


def test_window_days_spans_utc_days():
    w = bd.Window(DAY0_START + 3600 * NS, DAY1_START + 60 * NS)
    assert w.days() == [DAY0, DAY1]


def test_empty_intersection_exits_1_without_converting(tmp_path, dataset):
    ds = dataset
    # Move Binance coverage entirely after HL coverage.
    make_report(
        ds['report'], ds['hl_dir'], ds['bn_dir'],
        (ds['hl_lines'][0][0], ds['hl_lines'][-1][0]),
        (ds['hl_lines'][-1][0] + 10 * NS, ds['hl_lines'][-1][0] + 20 * NS),
    )
    conv = FakeConverter()
    with pytest.raises(SystemExit) as e:
        bd.main(base_argv(ds), convert_fn=conv, snapshot_fn=FakeSnapshotter())
    assert e.value.code == 1
    assert conv.calls == []


def test_window_shorter_than_min_window_hours_exits_1(dataset):
    ds = dataset
    conv = FakeConverter()
    with pytest.raises(SystemExit) as e:
        bd.main(base_argv(ds, min_window_hours='1'), convert_fn=conv)
    assert e.value.code == 1
    assert conv.calls == []


def test_both_inputs_are_trimmed_to_the_window(dataset, tmp_path):
    """The converter must be handed the trimmed file, and the signal must be cut."""
    ds = dataset
    hl_lines = ds['hl_lines']
    # Window: skip the first two HL frames and the last two.
    start = hl_lines[2][0]
    end = hl_lines[-3][0]
    make_report(ds['report'], ds['hl_dir'], ds['bn_dir'], (start, end), (start, end))
    conv = FakeConverter()
    bd.main(base_argv(ds), convert_fn=conv, snapshot_fn=FakeSnapshotter())

    assert len(conv.calls) == 1
    trimmed = conv.calls[0]['input_filename']
    seen = []
    with gzip.open(trimmed, 'rb') as f:
        for line in f:
            seen.append(int(line.split(b' ', 1)[0]))
    assert min(seen) >= start and max(seen) <= end
    assert seen == [ts for ts, _ in hl_lines if start <= ts <= end]

    sig = np.load(ds['out'] / 'signal_binancefuturesum_btcusdt.npz')
    assert sig['ts'].min() >= start and sig['ts'].max() <= end


# ---------------------------------------------------------------------------
# 3.2 time policy — raw check runs before conversion
# ---------------------------------------------------------------------------

def test_negative_latency_in_raw_hl_trips_before_any_conversion(dataset):
    ds = dataset
    lines = list(ds['hl_lines'])
    # One l2Book frame whose local_ts precedes its venue timestamp by 5 ms.
    exch_ms = (DAY0_START // MS) + 2500
    lines.append((exch_ms * MS - 5 * MS, hl_l2book('BTC', exch_ms, 999, 1000)))
    lines.sort()
    write_gz(ds['hl_dir'] / f'btc_{DAY0}.gz', lines)
    make_report(ds['report'], ds['hl_dir'], ds['bn_dir'],
                (lines[0][0], lines[-1][0]),
                (ds['bn_lines'][0][0], ds['bn_lines'][-1][0]))

    conv = FakeConverter()
    with pytest.raises(SystemExit) as e:
        bd.main(base_argv(ds), convert_fn=conv, snapshot_fn=FakeSnapshotter())
    assert e.value.code == 1
    assert conv.calls == [], 'conversion must not start once the raw check failed'
    assert not (ds['out'] / 'manifest.json').exists()


def test_time_policy_scan_reports_file_line_and_value(tmp_path):
    exch_ms = DAY0_START // MS
    path = write_gz(tmp_path / f'btc_{DAY0}.gz', [
        (exch_ms * MS + 100, hl_l2book('BTC', exch_ms, 1, 2)),
        (exch_ms * MS - 7, hl_trade('BTC', exch_ms, 1, 1)),
    ])
    scan = bd.scan_hl_time_policy([path], book_mode='slow',
                                  window=bd.Window(0, 2 ** 62))
    v = scan[0].violation
    assert v is not None
    assert v.line_no == 2
    assert v.latency_ns == -7
    assert str(path) in str(v)


def test_time_policy_scan_covers_bbo_frames(tmp_path):
    """`bbo` carries a venue timestamp too, even though the converter drops it."""
    exch_ms = DAY0_START // MS
    path = write_gz(tmp_path / f'btc_{DAY0}.gz', [
        (exch_ms * MS + 100, hl_l2book('BTC', exch_ms, 1, 2)),
        (exch_ms * MS - 3, hl_bbo('BTC', exch_ms, 1, 2)),
    ])
    scan = bd.scan_hl_time_policy([path], book_mode='slow',
                                  window=bd.Window(0, 2 ** 62))
    assert scan[0].violation is not None
    assert scan[0].violation.channel == 'bbo'
    # ... but the converted subset (trades + slow book) is clean, so its own
    # minimum is unaffected.
    assert scan[0].converted_min_latency_ns == 100


def test_scan_min_latency_is_exact_int64_at_ns_scale(tmp_path):
    exch_ms = DAY0_START // MS
    path = write_gz(tmp_path / f'btc_{DAY0}.gz', [
        (exch_ms * MS + 1, hl_l2book('BTC', exch_ms, 1, 2)),
        (exch_ms * MS + 3, hl_l2book('BTC', exch_ms, 1, 3)),
    ])
    scan = bd.scan_hl_time_policy([path], book_mode='slow',
                                  window=bd.Window(0, 2 ** 62))
    assert scan[0].min_latency_ns == 1
    assert isinstance(scan[0].min_latency_ns, int)


def test_assert_no_silent_shift_catches_a_uniform_offset(tmp_path):
    exch_ms = DAY0_START // MS
    path = write_gz(tmp_path / f'btc_{DAY0}.gz', [
        (exch_ms * MS + 100, hl_l2book('BTC', exch_ms, 1, 2)),
    ])
    scan = bd.scan_hl_time_policy([path], book_mode='slow',
                                  window=bd.Window(0, 2 ** 62))[0]
    arr = np.zeros(1, bd.EVENT_DTYPE)
    arr[0]['exch_ts'] = exch_ms * MS
    arr[0]['local_ts'] = exch_ms * MS + 100
    bd.assert_no_silent_shift(arr, scan)  # clean: no exception

    arr[0]['local_ts'] += 1_000_000  # the shift validation.py would apply
    with pytest.raises(bd.BuildError) as e:
        bd.assert_no_silent_shift(arr, scan)
    assert 'shift' in str(e.value).lower()


def test_clock_correction_moves_both_arrays_by_the_same_integer(dataset):
    ds = dataset
    conv = FakeConverter()
    corr = 1_500_000_000
    bd.main(base_argv(ds, clock_correction_ns=corr), convert_fn=conv,
            snapshot_fn=FakeSnapshotter())
    hl = np.load(ds['out'] / f'hl_btc_{DAY0}.npz')['data']
    sig = np.load(ds['out'] / 'signal_binancefuturesum_btcusdt.npz')

    raw_hl_first = ds['hl_lines'][0][0]
    assert int(hl['local_ts'].min()) == raw_hl_first + corr
    # The window starts at HL's first frame, so the first Binance tick inside it
    # is the first bookTicker at or after that — trimming happens before the
    # correction, and the correction is the same integer on both sides.
    window_start = max(ds['hl_lines'][0][0], ds['bn_lines'][0][0])
    raw_bn_first = min(ts for ts, p in ds['bn_lines']
                       if 'bookTicker' in p and ts >= window_start)
    assert int(sig['ts'][0]) == raw_bn_first + corr
    # exch_ts is the venue's clock and must not move.
    assert int(hl['exch_ts'].min()) == (DAY0_START // MS) * MS


# ---------------------------------------------------------------------------
# gate: the quality report
# ---------------------------------------------------------------------------

def test_red_overall_verdict_is_refused_before_assembly(dataset):
    ds = dataset
    make_report(ds['report'], ds['hl_dir'], ds['bn_dir'],
                (ds['hl_lines'][0][0], ds['hl_lines'][-1][0]),
                (ds['bn_lines'][0][0], ds['bn_lines'][-1][0]),
                verdict='red')
    conv = FakeConverter()
    with pytest.raises(SystemExit) as e:
        bd.main(base_argv(ds), convert_fn=conv, snapshot_fn=FakeSnapshotter())
    assert e.value.code == 1
    assert conv.calls == []
    assert not ds['out'].exists() or not (ds['out'] / 'manifest.json').exists()


def test_a_red_day_of_a_consumed_venue_is_refused(dataset):
    ds = dataset
    make_report(ds['report'], ds['hl_dir'], ds['bn_dir'],
                (ds['hl_lines'][0][0], ds['hl_lines'][-1][0]),
                (ds['bn_lines'][0][0], ds['bn_lines'][-1][0]),
                verdict='green',
                hl_days={DAY0: {'verdict': 'red', 'reasons': ['required stream missing']}})
    conv = FakeConverter()
    with pytest.raises(SystemExit) as e:
        bd.main(base_argv(ds), convert_fn=conv)
    assert e.value.code == 1
    assert conv.calls == []


def test_a_red_day_outside_the_window_is_only_a_warning(dataset):
    """The window is inside DAY0; a red DAY1 contributes nothing to this build."""
    ds = dataset
    make_report(ds['report'], ds['hl_dir'], ds['bn_dir'],
                (ds['hl_lines'][0][0], ds['hl_lines'][-1][0]),
                (ds['bn_lines'][0][0], ds['bn_lines'][-1][0]),
                hl_days={DAY0: {'verdict': 'green'}, DAY1: {'verdict': 'red'}})
    bd.main(base_argv(ds), convert_fn=FakeConverter(), snapshot_fn=FakeSnapshotter())
    m = json.loads((ds['out'] / 'manifest.json').read_text())
    assert m['quality_report']['verdict'] == 'green'
    assert any(DAY1 in note for note in m['quality_report']['outside_window'])


def test_yellow_report_still_builds(dataset):
    ds = dataset
    make_report(ds['report'], ds['hl_dir'], ds['bn_dir'],
                (ds['hl_lines'][0][0], ds['hl_lines'][-1][0]),
                (ds['bn_lines'][0][0], ds['bn_lines'][-1][0]),
                verdict='yellow')
    bd.main(base_argv(ds), convert_fn=FakeConverter(), snapshot_fn=FakeSnapshotter())
    assert (ds['out'] / 'manifest.json').exists()


def test_unknown_schema_is_refused(dataset):
    ds = dataset
    ds['report'].write_text(json.dumps({'schema': 'something-else', 'venues': {}}))
    with pytest.raises(SystemExit) as e:
        bd.main(base_argv(ds), convert_fn=FakeConverter())
    assert e.value.code == 1


def test_unknown_verdict_word_is_treated_as_red(dataset):
    ds = dataset
    make_report(ds['report'], ds['hl_dir'], ds['bn_dir'],
                (ds['hl_lines'][0][0], ds['hl_lines'][-1][0]),
                (ds['bn_lines'][0][0], ds['bn_lines'][-1][0]),
                verdict='probably-fine')
    with pytest.raises(SystemExit) as e:
        bd.main(base_argv(ds), convert_fn=FakeConverter())
    assert e.value.code == 1


# ---------------------------------------------------------------------------
# book_mode / num_levels pairing
# ---------------------------------------------------------------------------

def test_num_levels_defaults_are_paired_with_book_mode(dataset):
    ds = dataset
    conv = FakeConverter()
    bd.main(base_argv(ds), convert_fn=conv, snapshot_fn=FakeSnapshotter())
    assert conv.calls[0]['book_mode'] == 'slow'
    assert conv.calls[0]['num_levels'] == 20

    conv2 = FakeConverter()
    bd.main(base_argv(ds, book_mode='fast'), convert_fn=conv2,
            snapshot_fn=FakeSnapshotter())
    assert conv2.calls[0]['book_mode'] == 'fast'
    assert conv2.calls[0]['num_levels'] == 5


def test_mismatched_explicit_num_levels_is_refused(dataset):
    ds = dataset
    conv = FakeConverter()
    with pytest.raises(SystemExit) as e:
        bd.main(base_argv(ds, book_mode='fast', num_levels=20), convert_fn=conv)
    assert e.value.code == 1
    assert conv.calls == []


def test_matching_explicit_num_levels_is_accepted(dataset):
    ds = dataset
    conv = FakeConverter()
    bd.main(base_argv(ds, book_mode='fast', num_levels=5), convert_fn=conv,
            snapshot_fn=FakeSnapshotter())
    assert conv.calls[0]['num_levels'] == 5


def test_max_hl_book_age_default_follows_book_mode(dataset):
    ds = dataset
    bd.main(base_argv(ds), convert_fn=FakeConverter(), snapshot_fn=FakeSnapshotter())
    m = json.loads((ds['out'] / 'manifest.json').read_text())
    assert m['max_hl_book_age_ns'] == 12_000 * MS

    bd.main(base_argv(ds, book_mode='fast'), convert_fn=FakeConverter(),
            snapshot_fn=FakeSnapshotter())
    m = json.loads((ds['out'] / 'manifest.json').read_text())
    assert m['max_hl_book_age_ns'] == 1_500 * MS


# ---------------------------------------------------------------------------
# book_mode 'bbo+fast'
# ---------------------------------------------------------------------------

def test_bbo_fast_pairs_with_five_levels(dataset):
    ds = dataset
    conv = FakeConverter()
    bd.main(base_argv(ds, book_mode='bbo+fast'), convert_fn=conv,
            snapshot_fn=FakeSnapshotter())
    assert conv.calls[0]['book_mode'] == 'bbo+fast'
    assert conv.calls[0]['num_levels'] == 5


def test_bbo_fast_with_the_slow_depth_is_refused(dataset):
    """It reads the five-level fast cadence, so twenty levels is a mispairing."""
    ds = dataset
    conv = FakeConverter()
    with pytest.raises(SystemExit) as e:
        bd.main(base_argv(ds, book_mode='bbo+fast', num_levels=20), convert_fn=conv)
    assert e.value.code == 1
    assert conv.calls == []


def test_max_hl_book_age_for_bbo_fast_matches_fast(dataset):
    """1500 ms, the same as ``fast`` — and deliberately not ~2x the bbo median.

    The guard watches the instant the *best* bid/ask last changed. ``bbo`` is
    event-driven, so a long gap in it means the touch did not move, and a
    threshold near its 86 ms median would block an ordinary quiet market.

    Sharing the number does not make the two modes equivalent: under ``fast``
    the touch can only come from the periodic snapshot, so the guard doubles as
    a liveness check on that feed, and under ``bbo+fast`` it does not. See
    `BOOK_MODE_MAX_AGE_MS` for the measurement and for where `l2Book fast`
    liveness is actually checked.
    """
    ds = dataset
    bd.main(base_argv(ds, book_mode='bbo+fast'), convert_fn=FakeConverter(),
            snapshot_fn=FakeSnapshotter())
    m = json.loads((ds['out'] / 'manifest.json').read_text())
    assert m['max_hl_book_age_ns'] == 1_500 * MS


def test_manifest_records_bbo_fast(dataset):
    ds = dataset
    bd.main(base_argv(ds, book_mode='bbo+fast'), convert_fn=FakeConverter(),
            snapshot_fn=FakeSnapshotter())
    m = json.loads((ds['out'] / 'manifest.json').read_text())
    assert m['book_mode'] == 'bbo+fast'
    assert m['num_levels'] == 5
    assert m['converter']['book_mode'] == 'bbo+fast'
    assert m['converter']['num_levels'] == 5
    assert '--book-mode' in m['rebuild_cmd']
    i = m['rebuild_cmd'].index('--book-mode')
    assert m['rebuild_cmd'][i + 1] == 'bbo+fast'
    assert m['rebuild_cmd'][m['rebuild_cmd'].index('--num-levels') + 1] == '5'


def test_time_policy_scan_counts_bbo_as_converted_in_bbo_fast(tmp_path):
    """The mode converts bbo frames, so their latency is the converted minimum.

    Leaving them out would make `assert_no_silent_shift` compare the npz
    minimum against a raw minimum drawn from a different set of frames, and
    diagnose a shift that never happened.
    """
    exch_ms = DAY0_START // MS
    path = write_gz(tmp_path / f'btc_{DAY0}.gz', [
        (exch_ms * MS + 100, hl_l2book('BTC', exch_ms, 1, 2, fast=True)),
        (exch_ms * MS + 40, hl_bbo('BTC', exch_ms, 1, 2)),
    ])
    window = bd.Window(0, 2 ** 62)

    fused = bd.scan_hl_time_policy([path], book_mode='bbo+fast', window=window)[0]
    assert fused.converted_frames == 2
    assert fused.converted_min_latency_ns == 40

    fast = bd.scan_hl_time_policy([path], book_mode='fast', window=window)[0]
    assert fast.converted_frames == 1
    assert fast.converted_min_latency_ns == 100


def test_a_recording_without_the_fast_cadence_is_refused_for_bbo_fast(tmp_path):
    """Fail closed: bbo alone is a one-level-per-side book, not a depth stream.

    `bbo` is unconditional in the collector (`hyperliquid::ALWAYS_ON`) while the
    l2Book cadences are selected with `--hl-l2-modes`, so a recording made with
    `--hl-l2-modes slow` has bbo frames and no fast ones. Counting those bbo
    frames as converted — which `bbo+fast` must — would otherwise let such a day
    build a dataset with no depth in it and say nothing.
    """
    exch_ms = DAY0_START // MS
    path = write_gz(tmp_path / f'btc_{DAY0}.gz', [
        (exch_ms * MS + 100, hl_bbo('BTC', exch_ms, 1, 2)),
        (exch_ms * MS + 100, hl_l2book('BTC', exch_ms, 1, 2)),   # slow only
        (exch_ms * MS + 100, hl_trade('BTC', exch_ms, 1, 1)),
    ])
    window = bd.Window(0, 2 ** 62)
    scan = bd.scan_hl_time_policy([path], book_mode='bbo+fast', window=window)[0]
    assert scan.converted_frames > 0, 'the bbo and trade frames do convert'

    with pytest.raises(bd.BuildError, match='l2Book'):
        bd.convert_hl_day(
            path, tmp_path / 'out.npz', day=DAY0, window=window, scan=scan,
            tick_size=1.0, lot_size=1.0, book_mode='bbo+fast', num_levels=5,
            buffer_size=1000, clock_correction_ns=0, work_dir=tmp_path,
            convert_fn=FakeConverter(),
        )


def test_a_recording_without_the_chosen_cadence_is_refused_for_fast_too(tmp_path):
    """The same guard, for the modes that had only the weaker one before: a day
    of trades and slow snapshots is not a `fast` dataset."""
    exch_ms = DAY0_START // MS
    path = write_gz(tmp_path / f'btc_{DAY0}.gz', [
        (exch_ms * MS + 100, hl_l2book('BTC', exch_ms, 1, 2)),   # slow only
        (exch_ms * MS + 100, hl_trade('BTC', exch_ms, 1, 1)),
    ])
    window = bd.Window(0, 2 ** 62)
    scan = bd.scan_hl_time_policy([path], book_mode='fast', window=window)[0]

    with pytest.raises(bd.BuildError, match='l2Book'):
        bd.convert_hl_day(
            path, tmp_path / 'out.npz', day=DAY0, window=window, scan=scan,
            tick_size=1.0, lot_size=1.0, book_mode='fast', num_levels=5,
            buffer_size=1000, clock_correction_ns=0, work_dir=tmp_path,
            convert_fn=FakeConverter(),
        )


def test_keep_out_of_book_is_refused_for_bbo_fast(dataset):
    """Refused on the command line, not five minutes into a conversion.

    The converter refuses the pair itself — the fused book's uncrossed invariant
    does not survive a suppressed truncation deletion — but by then the time
    policy has run over every raw file. Same place as the `--num-levels`
    pairing, before anything reads a file.
    """
    ds = dataset
    conv = FakeConverter()
    with pytest.raises(SystemExit) as e:
        bd.main(base_argv(ds, book_mode='bbo+fast') + ['--keep-out-of-book'],
                convert_fn=conv)
    assert e.value.code == 1
    assert conv.calls == []


def test_a_build_that_fused_no_bbo_frames_is_refused(dataset):
    """Fail closed on the mirror image of the guard above.

    A recording can hold `l2Book fast` and trades and no usable `bbo` at all —
    a partially accepted subscription, or a reconnect that dropped only the
    `bbo` topic for part of the day. Every frame count stays healthy, the build
    produces a dataset byte-identical to `book_mode='fast'`, and the manifest
    declares `bbo+fast` — a silently degraded dataset indistinguishable from a
    correct one. The converter already counts what it fused; refusing zero is
    what makes the count load-bearing.
    """
    ds = dataset
    conv = BboReportingConverter(fused=0)
    with pytest.raises(SystemExit) as e:
        bd.main(base_argv(ds, book_mode='bbo+fast'), convert_fn=conv,
                snapshot_fn=FakeSnapshotter())
    assert e.value.code == 1


def test_the_fused_bbo_frame_count_reaches_the_manifest(dataset):
    """Evidence that the fusion happened must outlive stdout."""
    ds = dataset
    bd.main(base_argv(ds, book_mode='bbo+fast'), convert_fn=BboReportingConverter(fused=11),
            snapshot_fn=FakeSnapshotter())
    m = json.loads((ds['out'] / 'manifest.json').read_text())
    assert m['converter']['bbo_depth_frames'] == 11


def test_a_converter_that_reports_no_bbo_count_is_not_taken_for_zero(dataset):
    """`null` is "did not report", which is not "fused nothing" — the same rule
    `deduplicated_trades` follows. Refusing on it would make the build depend on
    a stat an older `hftbacktest` has no way to produce."""
    ds = dataset
    bd.main(base_argv(ds, book_mode='bbo+fast'), convert_fn=FakeConverter(),
            snapshot_fn=FakeSnapshotter())
    m = json.loads((ds['out'] / 'manifest.json').read_text())
    assert m['converter']['bbo_depth_frames'] is None


def test_a_zero_bbo_count_is_not_a_refusal_outside_the_fusing_modes(dataset):
    """`fast` reads no bbo frames, so zero is the only correct answer there."""
    ds = dataset
    bd.main(base_argv(ds, book_mode='fast'), convert_fn=BboReportingConverter(fused=0),
            snapshot_fn=FakeSnapshotter())
    m = json.loads((ds['out'] / 'manifest.json').read_text())
    assert m['converter']['bbo_depth_frames'] == 0


def test_a_bbo_frame_holding_the_minimum_passes_the_shift_check(tmp_path):
    """End to end for the scan/convert pair: the row the converter emits from
    the fastest bbo frame is the one the raw minimum was measured on."""
    exch_ms = DAY0_START // MS
    path = write_gz(tmp_path / f'btc_{DAY0}.gz', [
        (exch_ms * MS + 100, hl_l2book('BTC', exch_ms, 1, 2, fast=True)),
        (exch_ms * MS + 40, hl_bbo('BTC', exch_ms, 1, 2)),
    ])
    scan = bd.scan_hl_time_policy([path], book_mode='bbo+fast',
                                  window=bd.Window(0, 2 ** 62))[0]
    arr = FakeConverter()(input_filename=str(path), tick_size=1.0, lot_size=1.0,
                          num_levels=5, book_mode='bbo+fast')

    assert 'ok' in bd.assert_no_silent_shift(arr, scan)


# ---------------------------------------------------------------------------
# the signal array
# ---------------------------------------------------------------------------

def test_signal_array_keeps_nanoseconds_exactly(tmp_path):
    """1.8e18 ns has 256 ns granularity in float64. The value must survive."""
    odd = 1_800_000_000_000_000_001
    path = write_gz(tmp_path / f'btcusdt_{DAY0}.gz', [
        (odd, binance_book_ticker('BTCUSDT', odd // MS, 1.5, 2, 3.5, 4)),
    ])
    ts, values = bd.build_signal([path], 'BTCUSDT', bd.Window(0, 2 ** 62))
    assert ts.dtype == np.int64
    assert values.dtype == np.float64
    assert int(ts[0]) == odd
    assert int(ts[0]) != int(np.float64(odd))  # the trap this guards against
    assert values.shape == (1, 4)
    assert list(values[0]) == [1.5, 2.0, 3.5, 4.0]


def test_signal_array_is_stable_on_equal_timestamps(tmp_path):
    ts0 = DAY0_START + 5 * NS
    path = write_gz(tmp_path / f'btcusdt_{DAY0}.gz', [
        (ts0, binance_book_ticker('BTCUSDT', ts0 // MS, 10, 1, 11, 1)),
        (ts0, binance_book_ticker('BTCUSDT', ts0 // MS, 20, 2, 21, 2)),
        (ts0, binance_book_ticker('BTCUSDT', ts0 // MS, 30, 3, 31, 3)),
        (ts0 - NS, binance_book_ticker('BTCUSDT', ts0 // MS, 5, 5, 6, 6)),
    ])
    ts, values = bd.build_signal([path], 'BTCUSDT', bd.Window(0, 2 ** 62))
    assert list(ts) == [ts0 - NS, ts0, ts0, ts0]
    # Arrival order preserved among the ties: 10, 20, 30 — never reordered.
    assert list(values[1:, 0]) == [10.0, 20.0, 30.0]


def test_signal_array_ignores_non_book_ticker_streams(tmp_path):
    ts0 = DAY0_START + 5 * NS
    path = write_gz(tmp_path / f'btcusdt_{DAY0}.gz', [
        (ts0, binance_trade('BTCUSDT', ts0 // MS, 1, 1)),
        (ts0 + 1, binance_book_ticker('BTCUSDT', ts0 // MS, 10, 1, 11, 1)),
    ])
    ts, values = bd.build_signal([path], 'BTCUSDT', bd.Window(0, 2 ** 62))
    assert len(ts) == 1


def test_signal_array_trims_to_the_window_inclusively(tmp_path):
    ts0 = DAY0_START
    path = write_gz(tmp_path / f'btcusdt_{DAY0}.gz', [
        (ts0 + i * NS, binance_book_ticker('BTCUSDT', (ts0 + i * NS) // MS, i, 1, i + 1, 1))
        for i in range(5)
    ])
    ts, _ = bd.build_signal([path], 'BTCUSDT', bd.Window(ts0 + NS, ts0 + 3 * NS))
    assert list(ts) == [ts0 + NS, ts0 + 2 * NS, ts0 + 3 * NS]


def test_signal_array_rejects_a_symbol_mismatch(tmp_path):
    ts0 = DAY0_START
    path = write_gz(tmp_path / f'ethusdt_{DAY0}.gz', [
        (ts0, binance_book_ticker('ETHUSDT', ts0 // MS, 1, 1, 2, 1)),
    ])
    ts, _ = bd.build_signal([path], 'BTCUSDT', bd.Window(0, 2 ** 62))
    assert len(ts) == 0


def test_empty_signal_is_refused(dataset):
    ds = dataset
    write_gz(ds['bn_dir'] / f'btcusdt_{DAY0}.gz',
             [(ts, p) for ts, p in ds['bn_lines'] if 'bookTicker' not in p])
    conv = FakeConverter()
    with pytest.raises(SystemExit) as e:
        bd.main(base_argv(ds), convert_fn=conv, snapshot_fn=FakeSnapshotter())
    assert e.value.code == 1


# ---------------------------------------------------------------------------
# the signal union: two independent recordings of the same venue
# ---------------------------------------------------------------------------
#
# Measured on the recording hosts: a single Binance USD-M socket loses 0.2-0.4%
# of the day to reconnects, in clusters of 0.5-0.8s, 10-19 times a day — and two
# sockets to the SAME venue drop at uncorrelated times. So a second recording of
# the same symbol covers the first one's holes, and the union of the two is the
# signal. Hyperliquid is deliberately not duplicated: its losses are already
# mitigated by the 30-trade replay and the bbo fusion.


def bn_union_lines(day_start, ids, *, latency_ns=5 * MS, symbol='BTCUSDT'):
    """``@bookTicker`` frames carrying exactly the given update ids.

    The exchange time is keyed off ``u`` rather than off the loop index, so the
    same ``u`` in two recordings describes the same venue update — which is what
    makes deduplicating on it meaningful. ``latency_ns`` is the socket's receive
    delay, and the whole point of the union is that the two differ.
    """
    out = []
    for u in ids:
        exch_ms = day_start // MS + u * 1000
        out.append((
            exch_ms * MS + latency_ns,
            binance_book_ticker(symbol, exch_ms, 100 + u, 1, 101 + u, 2, u=u),
        ))
    return out


def make_signal_report(path, bn_dir, cov, *, verdict='green', days=None,
                       symbol='btcusdt'):
    """A Phase-2 report over the secondary signal recording alone.

    One venue, because that is what `quality_report.py` produces for one
    instance directory: it refuses two directories of the same venue in one
    report ("one venue per report entry"), which is exactly the situation two
    USD-M recordings on one host create.
    """
    days = days or {DAY0: {'verdict': 'green'}}
    out = {}
    for day, entry in days.items():
        entry = dict(entry)
        entry.setdefault('issues', [])
        entry.setdefault('symbols', {symbol: symbol_entry(cov[0], cov[1], ('bookTicker',))})
        out[day] = entry
    path.write_text(json.dumps({
        'schema': 'quality-report-v1',
        'verdict': verdict,
        'venues': {
            'binancefuturesum': {
                'data_dir': str(bn_dir),
                'exchange_as_recorded': None,
                'coverage': {'first_local_ts': cov[0], 'last_local_ts': cov[1]},
                'days': out,
            },
        },
    }))
    return path


def add_secondary(ds, lines, *, verdict='green', days=None, day=DAY0):
    """Give the dataset a second USD-M recording of the same symbol."""
    bn_dir_b = ds['root'] / 'bn_b'
    write_gz(bn_dir_b / f'btcusdt_{day}.gz', lines)
    write_meta(bn_dir_b / f'_meta_binancefuturesum_{day}.jsonl', [
        (DAY0_START, session_start('binancefuturesum', ['BTCUSDT'], commit='b0b0b0b')),
    ])
    report_b = make_signal_report(
        ds['root'] / 'report_b.json', bn_dir_b,
        (min(t for t, _ in lines), max(t for t, _ in lines)),
        verdict=verdict, days=days,
    )
    ds['bn_dir_b'] = bn_dir_b
    ds['report_b'] = report_b
    return ds


def test_union_coverage_takes_the_hull_of_two_overlapping_recordings():
    assert bd.union_coverage((10, 20), (15, 30)) == (10, 30)
    assert bd.union_coverage((15, 30), (10, 20)) == (10, 30)
    # One recording entirely inside the other adds nothing and takes nothing.
    assert bd.union_coverage((10, 40), (20, 30)) == (10, 40)


def test_union_coverage_refuses_a_stretch_where_both_were_dark():
    """Two disjoint intervals are not one interval.

    The hull would claim coverage over the hole between them, which is the one
    stretch neither socket recorded — the exact opposite of what the union is
    for.
    """
    with pytest.raises(bd.BuildError) as e:
        bd.union_coverage((10, 20), (30, 40))
    assert 'both' in str(e.value).lower()
    assert '20' in str(e.value) and '30' in str(e.value)


def test_union_dedups_by_update_id_and_keeps_the_earliest_receive(tmp_path):
    """The same venue update arrives on both sockets; it is one row, timed by
    whichever socket saw it first. Keeping both would double every frame."""
    a = write_gz(tmp_path / 'a' / f'btcusdt_{DAY0}.gz',
                 bn_union_lines(DAY0_START, [1, 2, 3], latency_ns=9 * MS))
    b = write_gz(tmp_path / 'b' / f'btcusdt_{DAY0}.gz',
                 bn_union_lines(DAY0_START, [1, 2, 3], latency_ns=4 * MS))

    ts, values, stats = bd.build_signal_union(
        [('primary', [a]), ('secondary', [b])], 'BTCUSDT', bd.Window(0, 2 ** 62))

    assert len(ts) == 3, 'the same update id was kept twice'
    assert list(np.diff(ts)) == [NS, NS]
    # 4 ms, not 9 ms: socket B saw all three first.
    assert int(ts[0]) == (DAY0_START // MS + 1000) * MS + 4 * MS
    assert stats['sources']['secondary']['contributed'] == 3
    assert stats['sources']['primary']['contributed'] == 0
    assert stats['recovered_rows'] == 0, 'B recovered nothing A did not have'


def test_the_union_fills_a_hole_in_one_recording_from_the_other(tmp_path):
    """The whole point: socket A's reconnect gap is covered by socket B."""
    a = write_gz(tmp_path / 'a' / f'btcusdt_{DAY0}.gz',
                 bn_union_lines(DAY0_START, [1, 4, 5]))          # 2 and 3 lost
    b = write_gz(tmp_path / 'b' / f'btcusdt_{DAY0}.gz',
                 bn_union_lines(DAY0_START, [1, 2, 3, 4, 5]))

    ts, values, stats = bd.build_signal_union(
        [('primary', [a]), ('secondary', [b])], 'BTCUSDT', bd.Window(0, 2 ** 62))

    assert len(ts) == 5
    assert stats['rows'] == 5
    assert stats['primary_only_rows'] == 3, 'A alone would have produced three rows'
    assert stats['recovered_rows'] == 2, 'the union recovered the two frames A lost'
    assert stats['sources']['secondary']['exclusive'] == 2
    assert stats['sources']['primary']['exclusive'] == 0
    # The recovered rows carry the recovering socket's prices, in order.
    assert list(values[:, 0]) == [101.0, 102.0, 103.0, 104.0, 105.0]


def test_a_hole_in_both_recordings_stays_a_hole(tmp_path):
    """The union recovers what one socket missed, not what neither saw."""
    a = write_gz(tmp_path / 'a' / f'btcusdt_{DAY0}.gz',
                 bn_union_lines(DAY0_START, [1, 4]))
    b = write_gz(tmp_path / 'b' / f'btcusdt_{DAY0}.gz',
                 bn_union_lines(DAY0_START, [1, 4]))

    ts, _, stats = bd.build_signal_union(
        [('primary', [a]), ('secondary', [b])], 'BTCUSDT', bd.Window(0, 2 ** 62))

    assert len(ts) == 2
    assert stats['recovered_rows'] == 0
    # 1 -> 4 is a three-second hole in the union, exactly as it is in both.
    assert int(ts[1]) - int(ts[0]) == 3 * NS


def test_the_union_refuses_a_frame_without_an_update_id(tmp_path):
    """Fail closed. Without the key a frame can be neither matched nor dropped:
    keeping it double-counts an update the other socket also has, and dropping
    it loses one only this socket saw."""
    a = write_gz(tmp_path / 'a' / f'btcusdt_{DAY0}.gz',
                 bn_union_lines(DAY0_START, [1, 2]))
    b = write_gz(tmp_path / 'b' / f'btcusdt_{DAY0}.gz', [
        (DAY0_START + NS, binance_book_ticker('BTCUSDT', DAY0_START // MS, 1, 1, 2, 1,
                                              drop_update_id=True)),
    ])

    with pytest.raises(bd.BuildError) as e:
        bd.build_signal_union([('primary', [a]), ('secondary', [b])], 'BTCUSDT',
                              bd.Window(0, 2 ** 62))
    assert 'u' in str(e.value)
    assert str(b) in str(e.value), 'the refusal has to name the file and line'


def test_a_single_recording_is_not_deduplicated(tmp_path):
    """Behaviour of the one-recording build is untouched.

    `build_signal` never had a dedup and must not gain one: a repeated update id
    inside one recording is the venue's business, and collapsing rows would
    silently change every dataset built so far.
    """
    path = write_gz(tmp_path / f'btcusdt_{DAY0}.gz', [
        (DAY0_START + i * NS,
         binance_book_ticker('BTCUSDT', (DAY0_START + i * NS) // MS, i, 1, i + 1, 1, u=7))
        for i in range(3)
    ])
    ts, _ = bd.build_signal([path], 'BTCUSDT', bd.Window(0, 2 ** 62))
    assert len(ts) == 3


def test_the_union_keeps_no_per_source_set_of_update_ids(tmp_path):
    """The union has to hold `build_signal`'s memory discipline.

    `build_signal` is written around `array('q')`/`array('d')` for a stated
    reason — "a day of @bookTicker is millions of frames, and 8 bytes per number
    instead of a boxed Python object keeps this in memory" — and the union is on
    the same data. The easy way to lose it is a `{label: set(update_ids)}` beside
    the `u -> best` dict, which at a million ids per socket is tens of megabytes
    buying what one bit per source inside `best` already answers.

    The budget is a measurement, not a target: on this machine (CPython 3.13,
    50 000 shared ids) the two sets cost 601 bytes per update id and one bit in
    `best` costs 455. 560 sits between them, nearer the version that is wrong. A
    future CPython that moves container sizes may need the number re-measured —
    but a jump back towards 600 is per-id containers returning, not the
    allocator drifting.
    """
    n = 50_000
    ids = range(1, n + 1)
    a = write_gz(tmp_path / 'a' / f'btcusdt_{DAY0}.gz',
                 bn_union_lines(DAY0_START, ids, latency_ns=9 * MS))
    b = write_gz(tmp_path / 'b' / f'btcusdt_{DAY0}.gz',
                 bn_union_lines(DAY0_START, ids, latency_ns=4 * MS))

    tracemalloc.start()
    try:
        _ts, _values, stats = bd.build_signal_union(
            [('primary', [a]), ('secondary', [b])], 'BTCUSDT', bd.Window(0, 2 ** 62))
        peak = tracemalloc.get_traced_memory()[1]
    finally:
        tracemalloc.stop()

    assert stats['rows'] == n, 'the fixture has to be the size the budget assumes'
    assert peak / n < 560, (
        'the union costs %.0f bytes per update id; a per-source set of ids is '
        'the usual reason' % (peak / n)
    )


def test_the_union_widens_the_window_to_what_either_socket_covered(dataset, tmp_path):
    """B was up before A was, so the buildable window starts earlier."""
    ds = dataset
    early = bn_union_lines(DAY0_START, [1, 2, 3, 4, 5, 6])
    add_secondary(ds, early)
    # A starts a second after B does.
    a_cov = (early[1][0], early[-1][0])
    make_report(ds['report'], ds['hl_dir'], ds['bn_dir'],
                (ds['hl_lines'][0][0], ds['hl_lines'][-1][0]), a_cov)
    write_gz(ds['bn_dir'] / f'btcusdt_{DAY0}.gz', early[1:])

    conv = FakeConverter()
    m = bd.build(bd.parse_args(base_argv(ds) + ['--binance-report-b', str(ds['report_b'])]),
                 convert_fn=conv)

    assert m['window']['raw_start_ns'] == early[0][0], \
        'the window ignored the coverage only the second socket had'


def test_a_day_missing_from_one_signal_recording_is_covered_by_the_other(dataset):
    """A whole day absent from A is not a refusal when B recorded it."""
    ds = dataset
    add_secondary(ds, bn_union_lines(DAY0_START, [1, 2, 3]))
    # A recorded nothing at all that day; only its file for the day is gone.
    (ds['bn_dir'] / f'btcusdt_{DAY0}.gz').unlink()
    make_report(ds['report'], ds['hl_dir'], ds['bn_dir'],
                (ds['hl_lines'][0][0], ds['hl_lines'][-1][0]),
                (ds['bn_lines'][0][0], ds['bn_lines'][-1][0]),
                bn_days={DAY0: {'verdict': 'green',
                                'symbols': {'btcusdt': {'coverage': {
                                    'first_local_ts': None, 'last_local_ts': None,
                                    'required_streams': ['bookTicker']}}}}})

    m = bd.build(bd.parse_args(base_argv(ds) + ['--binance-report-b', str(ds['report_b'])]),
                 convert_fn=FakeConverter())
    assert m['signal']['rows'] == 3
    assert m['signal']['union']['sources']['secondary']['exclusive'] == 3


def test_a_red_secondary_recording_does_not_block_the_build(dataset):
    """Additive only. The secondary can only ever add frames, so a fault in it
    subtracts nothing from what the primary already justified."""
    ds = dataset
    add_secondary(ds, bn_union_lines(DAY0_START, [1, 2, 3]), verdict='red',
                  days={DAY0: {'verdict': 'red'}})

    m = bd.build(bd.parse_args(base_argv(ds) + ['--binance-report-b', str(ds['report_b'])]),
                 convert_fn=FakeConverter())
    assert m['signal']['union']['secondary']['verdict'] == 'red'
    assert m['quality_report']['verdict'] == 'green', \
        "the secondary's verdict must not become the build's"


def test_a_red_primary_recording_still_blocks_with_a_secondary_present(dataset):
    """The required-stream gate stays per recording, and the primary is the one
    the window and the day set come from."""
    ds = dataset
    add_secondary(ds, bn_union_lines(DAY0_START, [1, 2, 3]))
    make_report(ds['report'], ds['hl_dir'], ds['bn_dir'],
                (ds['hl_lines'][0][0], ds['hl_lines'][-1][0]),
                (ds['bn_lines'][0][0], ds['bn_lines'][-1][0]),
                bn_days={DAY0: {'verdict': 'red'}})

    with pytest.raises(bd.BuildError) as e:
        bd.build(bd.parse_args(base_argv(ds) + ['--binance-report-b', str(ds['report_b'])]),
                 convert_fn=FakeConverter())
    assert 'red' in str(e.value)


def test_the_same_recording_twice_is_refused(dataset):
    """Two reports pointing at one directory is an operator error, and a silent
    one: every frame would match itself and the union would report perfect
    redundancy it does not have."""
    ds = dataset
    make_signal_report(ds['root'] / 'report_b.json', ds['bn_dir'],
                       (ds['bn_lines'][0][0], ds['bn_lines'][-1][0]))

    with pytest.raises(bd.BuildError) as e:
        bd.build(bd.parse_args(base_argv(ds)
                               + ['--binance-report-b', str(ds['root'] / 'report_b.json')]),
                 convert_fn=FakeConverter())
    assert 'same directory' in str(e.value)


def test_the_manifest_records_both_signal_inputs_and_the_recovery(dataset):
    ds = dataset
    # Update ids 1..5: the `dataset` fixture's Hyperliquid day ends at +5.2s and
    # the window is its intersection with the signal, so a sixth second would
    # fall outside it and never reach the union at all.
    add_secondary(ds, bn_union_lines(DAY0_START, [1, 2, 3, 4, 5]))
    # A lost update 3.
    a_lines = bn_union_lines(DAY0_START, [1, 2, 4, 5], latency_ns=9 * MS)
    write_gz(ds['bn_dir'] / f'btcusdt_{DAY0}.gz', a_lines)
    make_report(ds['report'], ds['hl_dir'], ds['bn_dir'],
                (ds['hl_lines'][0][0], ds['hl_lines'][-1][0]),
                (a_lines[0][0], a_lines[-1][0]))

    m = bd.build(bd.parse_args(base_argv(ds) + ['--binance-report-b', str(ds['report_b'])]),
                 convert_fn=FakeConverter())

    union = m['signal']['union']
    assert union['enabled'] is True
    assert union['dedup_key'] == 'u'
    assert union['rows'] == 5
    assert union['primary_only_rows'] == 4
    assert union['recovered_rows'] == 1
    assert union['sources']['secondary']['exclusive'] == 1
    # Both recordings are fingerprinted, and the manifest names which is which.
    assert m['inputs']['binancefuturesum']['data'][0]['sha256']
    assert m['inputs']['binancefuturesum_secondary']['data'][0]['sha256']
    assert m['inputs']['binancefuturesum_secondary']['data_dir'] == str(ds['bn_dir_b'])
    assert m['inputs']['binancefuturesum_secondary']['role'] == 'signal union, additive only'
    # The mixed receive clock is stated rather than left for a reader to notice.
    assert 'clock' in union['local_ts_note']
    # And the rebuild reproduces the union rather than the primary alone.
    assert '--binance-report-b' in m['rebuild_cmd']


def test_a_secondary_that_contributed_nothing_warns_but_builds(dataset, capsys):
    """A second socket that recovered nothing is worth saying out loud — it is
    either redundant or broken — but it is not a reason to refuse."""
    ds = dataset
    add_secondary(ds, bn_union_lines(DAY0_START, [1, 2, 3]))
    write_gz(ds['bn_dir'] / f'btcusdt_{DAY0}.gz', bn_union_lines(DAY0_START, [1, 2, 3]))
    a_lines = bn_union_lines(DAY0_START, [1, 2, 3])
    make_report(ds['report'], ds['hl_dir'], ds['bn_dir'],
                (ds['hl_lines'][0][0], ds['hl_lines'][-1][0]),
                (a_lines[0][0], a_lines[-1][0]))

    m = bd.build(bd.parse_args(base_argv(ds) + ['--binance-report-b', str(ds['report_b'])]),
                 convert_fn=FakeConverter())
    assert m['signal']['union']['recovered_rows'] == 0
    assert 'recovered no' in capsys.readouterr().err


# --- one update id, two different books -------------------------------------
#
# The dedup key is the venue's `u`, so the union is only sound while one `u`
# means one book state. Where it does not — a matching engine restarted and the
# counter began again, a window spanning the restart, a file that is not what it
# is thought to be — the dict keeps whichever frame arrived first and the other
# simply is not in the dataset. Nothing counts it: `rows`, `recovered_rows` and
# every coverage number stay plausible. That is the one failure of this design
# that cannot be seen afterwards, so it is refused when it happens.


def test_the_union_refuses_one_update_id_that_carries_two_different_books(tmp_path):
    """The same `u` from the two sockets with different prices is a
    contradiction: one book update has one best bid and ask, and both sockets
    receive the same bytes for it. Keeping either is picking one at random."""
    a = write_gz(tmp_path / 'a' / f'btcusdt_{DAY0}.gz', [
        (DAY0_START + 9 * MS,
         binance_book_ticker('BTCUSDT', DAY0_START // MS, 100, 1, 101, 2, u=7)),
    ])
    b = write_gz(tmp_path / 'b' / f'btcusdt_{DAY0}.gz', [
        (DAY0_START + 4 * MS,
         binance_book_ticker('BTCUSDT', DAY0_START // MS, 555, 1, 999, 2, u=7)),
    ])
    with pytest.raises(bd.BuildError) as e:
        bd.build_signal_union([('primary', [a]), ('secondary', [b])], 'BTCUSDT',
                              bd.Window(0, 2 ** 62))
    assert 'update id 7' in str(e.value)


def test_a_reused_update_id_inside_one_recording_is_refused(tmp_path):
    """The realistic shape of it: `u` is per symbol and restarts when the
    venue's matching engine does, so a window spanning a restart has one id
    describing two book states minutes apart. The single-recording builder never
    saw this because it does not deduplicate; the union collapses them to one
    row and reports nothing."""
    a = write_gz(tmp_path / 'a' / f'btcusdt_{DAY0}.gz', [
        (DAY0_START + 1 * MS,
         binance_book_ticker('BTCUSDT', DAY0_START // MS, 100, 1, 101, 2, u=5)),
        (DAY0_START + 100 * NS,
         binance_book_ticker('BTCUSDT', (DAY0_START + 100 * NS) // MS, 200, 1, 201, 2, u=5)),
    ])
    b = write_gz(tmp_path / 'b' / f'btcusdt_{DAY0}.gz', [
        (DAY0_START + 2 * MS,
         binance_book_ticker('BTCUSDT', DAY0_START // MS, 100, 1, 101, 2, u=6)),
    ])
    # The single-recording path is unchanged: both rows survive there.
    assert len(bd.build_signal([a], 'BTCUSDT', bd.Window(0, 2 ** 62))[0]) == 2
    with pytest.raises(bd.BuildError) as e:
        bd.build_signal_union([('primary', [a]), ('secondary', [b])], 'BTCUSDT',
                              bd.Window(0, 2 ** 62))
    assert 'update id 5' in str(e.value)


def test_the_same_book_under_the_same_update_id_is_simply_deduplicated(tmp_path):
    """The ordinary case, which must not be caught by the check above: both
    sockets saw the update, so the two frames agree and one row comes out."""
    a = write_gz(tmp_path / 'a' / f'btcusdt_{DAY0}.gz',
                 bn_union_lines(DAY0_START, [1, 2], latency_ns=9 * MS))
    b = write_gz(tmp_path / 'b' / f'btcusdt_{DAY0}.gz',
                 bn_union_lines(DAY0_START, [1, 2], latency_ns=4 * MS))
    ts, _v, stats = bd.build_signal_union([('primary', [a]), ('secondary', [b])],
                                          'BTCUSDT', bd.Window(0, 2 ** 62))
    assert len(ts) == 2
    assert stats['rows'] == 2


# --- the two recordings' clocks ---------------------------------------------
#
# `earliest local_ts wins` is only "the socket that was up" while both sockets
# time their frames against ONE clock. Put the second recording on a second host
# whose clock is behind and it wins every update they share, and the whole
# signal's timeline moves by the skew — silently, because a skewed recording
# that also recovers a few frames looks exactly like a healthy one.
#
# The ids both recordings saw are the measurement: on one host their receive
# times differ by socket latency, which is milliseconds and centred near zero.


def test_the_union_measures_the_offset_between_the_two_recordings_clocks(tmp_path):
    a = write_gz(tmp_path / 'a' / f'btcusdt_{DAY0}.gz',
                 bn_union_lines(DAY0_START, [1, 2, 3, 4, 5], latency_ns=9 * MS))
    b = write_gz(tmp_path / 'b' / f'btcusdt_{DAY0}.gz',
                 bn_union_lines(DAY0_START, [1, 2, 3, 4, 5], latency_ns=9 * MS - 2 * NS))
    _ts, _v, stats = bd.build_signal_union([('primary', [a]), ('secondary', [b])],
                                           'BTCUSDT', bd.Window(0, 2 ** 62))
    assert stats['shared_update_ids'] == 5
    assert stats['clock_offset_ns'] == -2 * NS, \
        'the measured offset is the secondary minus the primary, over shared ids'


def test_a_secondary_recorded_against_another_clock_refuses_the_build(dataset):
    """End to end: B is 2 s behind and also recovers a frame, so nothing else
    in the build notices. The budget is `--max-signal-age-ms`: two sockets that
    disagree about `now` by more than the whole freshness window are not two
    views of one timeline."""
    ds = dataset
    primary = bn_union_lines(DAY0_START, [1, 2, 3, 4], latency_ns=9 * MS)
    write_gz(ds['bn_dir'] / f'btcusdt_{DAY0}.gz', primary)
    make_report(ds['report'], ds['hl_dir'], ds['bn_dir'],
                (ds['hl_lines'][0][0], ds['hl_lines'][-1][0]),
                (primary[0][0], primary[-1][0]))
    add_secondary(ds, bn_union_lines(DAY0_START, [1, 2, 3, 4, 5],
                                     latency_ns=9 * MS - 2 * NS))

    with pytest.raises(bd.BuildError) as e:
        bd.build(bd.parse_args(base_argv(ds) + ['--binance-report-b', str(ds['report_b'])]),
                 convert_fn=FakeConverter())
    assert 'clock' in str(e.value)


def test_two_sockets_on_one_host_are_not_read_as_a_skewed_clock(dataset):
    """The guard must not fire on what it is built to allow: one host, two
    sockets, a few milliseconds of receive latency between them."""
    ds = dataset
    primary = bn_union_lines(DAY0_START, [1, 2, 3, 4], latency_ns=9 * MS)
    write_gz(ds['bn_dir'] / f'btcusdt_{DAY0}.gz', primary)
    make_report(ds['report'], ds['hl_dir'], ds['bn_dir'],
                (ds['hl_lines'][0][0], ds['hl_lines'][-1][0]),
                (primary[0][0], primary[-1][0]))
    add_secondary(ds, bn_union_lines(DAY0_START, [1, 2, 3, 4, 5], latency_ns=4 * MS))

    m = bd.build(bd.parse_args(base_argv(ds) + ['--binance-report-b', str(ds['report_b'])]),
                 convert_fn=FakeConverter())
    assert m['signal']['union']['clock_offset_ns'] == -5 * MS
    assert m['signal']['union']['recovered_rows'] == 1


def test_a_repeated_frame_is_not_a_second_shared_update_id(tmp_path):
    """`shared_update_ids` is the ids BOTH recordings saw, so one id can count
    once however many times a socket repeats its frame. It is the denominator of
    the clock measurement and it is quoted in the refusal, so an inflated one
    reads as more evidence than there is."""
    a = write_gz(tmp_path / 'a' / f'btcusdt_{DAY0}.gz',
                 bn_union_lines(DAY0_START, [1, 2], latency_ns=4 * MS))
    # The secondary sends update 1 twice, both later than the primary's copy.
    b_lines = bn_union_lines(DAY0_START, [1, 2], latency_ns=9 * MS)
    b = write_gz(tmp_path / 'b' / f'btcusdt_{DAY0}.gz', [b_lines[0]] + b_lines)

    _ts, _v, stats = bd.build_signal_union([('primary', [a]), ('secondary', [b])],
                                           'BTCUSDT', bd.Window(0, 2 ** 62))
    assert stats['sources']['secondary']['frames'] == 3
    assert stats['shared_update_ids'] == 2
    assert stats['clock_offset_ns'] == 5 * MS


# --- the clock check's own blind spot ---------------------------------------
#
# `clock_offset_ns` is None exactly when the two recordings share no update id.
# For one venue and one symbol over one window that cannot happen while both
# were recording — two sockets to Binance see the same book updates — so it
# means one of them has no frame inside the window at all. Whether anything
# else notices depends on which one: a silent SECONDARY leaves
# `recovered_rows == 0` and the existing warning fires, but a silent PRIMARY
# leaves `recovered_rows == rows` and every number looking healthy while the
# second recording supplies the whole signal on a clock nothing checked.
#
# Said rather than refused: a socket down for a whole window is the case the
# union was built for, and the day-level half of it is legal by construction.


def test_no_shared_update_id_with_the_secondary_in_the_signal_is_reported(capsys):
    bd.require_one_clock({'clock_offset_ns': None, 'shared_update_ids': 0,
                          'rows': 4, 'primary_only_rows': 0, 'recovered_rows': 4},
                         500 * MS)
    err = capsys.readouterr().err
    assert 'no update id' in err
    assert 'clocks' in err


def test_no_shared_update_id_is_silent_while_the_secondary_added_nothing(capsys):
    """The other half: a secondary that put no row in the signal cannot have put
    its clock there either, and `build`'s "recovered no frames" warning is
    already the one that speaks."""
    bd.require_one_clock({'clock_offset_ns': None, 'shared_update_ids': 0,
                          'rows': 4, 'primary_only_rows': 4, 'recovered_rows': 0},
                         500 * MS)
    assert capsys.readouterr().err == ''


def test_a_primary_with_no_signal_frames_in_the_window_is_reported(dataset, capsys):
    """End to end, and the shape it really takes: the report says the primary's
    bookTicker was live all day — that is a liveness gauge, not a promise about
    the file — while the recording holds no bookTicker frame the window covers.
    The build then rests entirely on the second socket. Before this it was
    silent: `recovered_rows == rows` and `clock_offset_ns is None`, so the
    "recovered no frames" warning and the clock check both passed."""
    ds = dataset
    b_lines = bn_union_lines(DAY0_START, [1, 2, 3, 4, 5])
    add_secondary(ds, b_lines)
    # Trades only: a real file, a real day, and nothing `iter_book_ticker` yields.
    write_gz(ds['bn_dir'] / f'btcusdt_{DAY0}.gz',
             [(t, binance_trade('BTCUSDT', t // MS, 100, 0.1)) for t, _ in b_lines])
    make_report(ds['report'], ds['hl_dir'], ds['bn_dir'],
                (ds['hl_lines'][0][0], ds['hl_lines'][-1][0]),
                (b_lines[0][0], b_lines[-1][0]))

    m = bd.build(bd.parse_args(base_argv(ds) + ['--binance-report-b', str(ds['report_b'])]),
                 convert_fn=FakeConverter())
    assert m['signal']['union']['shared_update_ids'] == 0
    assert m['signal']['union']['primary_only_rows'] == 0
    assert 'no update id' in capsys.readouterr().err


# ---------------------------------------------------------------------------
# tick / lot
# ---------------------------------------------------------------------------

def test_tick_lot_from_sz_decimals_follows_the_hl_rule():
    # design-hyperliquid-connector.md §5.3: lot = 10^-sz, tick = 10^-(6-sz).
    assert bd.tick_lot_from_sz_decimals(5) == (0.1, 0.00001)
    assert bd.tick_lot_from_sz_decimals(0) == (1e-06, 1.0)
    assert bd.tick_lot_from_sz_decimals(2) == (0.0001, 0.01)


def test_tick_lot_source_is_recorded_when_derived(dataset):
    """The lot still comes from ``szDecimals``; the tick is measured.

    The fixture quotes whole numbers around 100, so the measured grid is 1.0
    while the venue rule at that price says 0.1 — a thin fixture, and the
    manifest has to say which of the two it registered and that they differed.
    """
    ds = dataset
    bd.main(base_argv(ds), convert_fn=FakeConverter(), snapshot_fn=FakeSnapshotter())
    m = json.loads((ds['out'] / 'manifest.json').read_text())
    assert m['tick_size'] == 1.0
    assert m['lot_size'] == 0.00001
    assert m['tick_lot_source']['kind'] == 'measured'
    assert m['tick_lot_source']['sz_decimals'] == 5
    assert m['tick_lot_source']['tick']['measured'] == 1.0
    assert m['tick_lot_source']['tick']['rule'] == 0.1
    assert m['tick_lot_source']['tick']['cross_check'] == 'measured_coarser_than_rule'
    assert m['tick_lot_source']['lot']['source'] == 'hl_universe'


# ---------------------------------------------------------------------------
# the measured tick (the ONDO/JTO sign flip, 2026-07-30)
#
# szDecimals gives only the LOWER bound of the Hyperliquid tick: the venue also
# caps a price at five significant figures, so the effective tick is a step
# function of the price. Registering the lower bound gave ONDO 1e-6 on a day
# every recorded price sat on a 1e-5 grid, and the backtest quoted levels that
# do not exist.
# ---------------------------------------------------------------------------

def grid_of(prices):
    grid = bd.PriceGrid()
    for px in prices:
        grid.observe(px)
    return grid


def test_the_measured_tick_is_the_coarsest_grid_every_price_lands_on():
    # ONDO, 2026-07-29: quoted 0.39040..0.41209, every price a multiple of 1e-5.
    grid = grid_of(['0.39040', '0.41209', '0.40000', '0.39875'])
    assert grid.tick_exponent == -5
    assert bd.tick_from_exponent(grid.tick_exponent) == 1e-05
    assert grid.distinct_prices == 4


def test_one_price_off_the_grid_makes_the_measured_tick_finer():
    """The grid is the coarsest one EVERY price lands on, not the common case."""
    assert grid_of(['0.39040', '0.41209']).tick_exponent == -5
    assert grid_of(['0.39040', '0.41209', '0.412095']).tick_exponent == -6


def test_the_measurement_is_not_capped_at_a_tick_of_one():
    """Above 100 000 the venue tick is 10, and the measurement has to be able to say so.

    Five significant figures make the tick a step function of the price, and the
    step does not stop at 1.0: `design-hyperliquid-connector.md` §11.5, measured
    on testnet 2026-07-28, has `123456` rejected and rounded to `123460`. A
    measurement that could not return anything coarser than 1.0 would answer 1.0
    on a book quoted in tens — a tick ten times too fine, which is the exact
    defect this measurement exists to remove, reintroduced by the tool rather
    than by szDecimals.
    """
    grid = grid_of(['118230', '118240', '118250', '118260'])
    assert grid.tick_exponent == 1
    assert bd.tick_from_exponent(grid.tick_exponent) == 10.0
    assert grid_of(['1182300', '1182400', '1182500']).tick_exponent == 2


def test_a_thin_coin_is_measured_on_exact_integers():
    """PUMP quotes ~0.001816; a tick of 1e-6 there is not a binary fraction.

    Through floats the divisibility test this measurement consists of is a
    rounding coin toss, so prices are held as exact integers scaled by 1e10 —
    ``rerank_metrics.scaled_px``, shared rather than reimplemented. The scaled
    value is asserted here too: the grid alone would also come out right from a
    float path that happened to round the right way.
    """
    assert bd.scaled_px('0.001816') == 18160000
    grid = grid_of(['0.001816', '0.001897', '0.001788'])
    assert grid.tick_exponent == -6
    assert bd.tick_from_exponent(grid.tick_exponent) == 1e-06


def test_non_positive_prices_are_not_observations():
    """An empty side of a thin coin's book prints a zero, which is not a price."""
    grid = grid_of(['0.39040', '0', '0.41209'])
    assert grid.tick_exponent == -5
    assert grid.distinct_prices == 2
    assert grid.non_positive == 1


def test_a_window_with_one_distinct_price_measures_nothing():
    """One price always 'lands on' a grid as coarse as its own last digit."""
    with pytest.raises(bd.BuildError) as e:
        bd.reconcile_tick(grid_of(['0.39040', '0.39040']), sz_decimals=0)
    assert 'distinct' in str(e.value)
    assert '--tick-size' in str(e.value)


def test_the_rule_tick_is_the_coarser_of_the_five_figure_and_szdecimals_terms():
    """Both HL constraints are ceilings on precision; the coarser one binds."""
    px = bd.scaled_px
    # ONDO: five significant figures at 0.41 is 1e-5, szDecimals=0 allows 1e-6.
    assert bd.hl_rule_tick_exponent(px('0.41209'), 0) == -5
    # SEI at 0.041: five figures allows 1e-6 and szDecimals=0 stops there too.
    assert bd.hl_rule_tick_exponent(px('0.041109'), 0) == -6
    # BTC at 63022 with szDecimals=5: five figures is 1.0, the floor is 0.1.
    assert bd.hl_rule_tick_exponent(px('63022.0'), 5) == 0
    # HYPE at 53.044, szDecimals=2: five figures is 1e-3, the floor is 1e-4.
    assert bd.hl_rule_tick_exponent(px('53.044'), 2) == -3
    # szDecimals binding: a 0.5 price whose venue floor is 1e-2, not 1e-5.
    assert bd.hl_rule_tick_exponent(px('0.5'), 4) == -2


def test_the_scan_measures_the_book_and_the_bbo_but_not_the_trades(tmp_path):
    """Which channels the grid is made of, through the real scan.

    ``bbo`` is a price source in its own right — under ``bbo+fast`` it is the
    majority of frames — so a grid built from ``l2Book`` alone would miss the
    touch. ``trades`` are deliberately excluded: a fill prints at a level that
    already existed, so it adds nothing, while one odd print would drag the
    measured tick finer and reopen the hole this whole measurement closes.
    """
    base_ms = DAY0_START // MS
    lines = [
        (DAY0_START + MS, hl_l2book('BTC', base_ms, '0.4000', '0.4100', fast=True)),
        (DAY0_START + 2 * MS, hl_bbo('BTC', base_ms, '0.40005', '0.41000')),
        (DAY0_START + 3 * MS, hl_trade('BTC', base_ms, '0.400051', 1.0)),
    ]
    path = write_gz(tmp_path / f'btc_{DAY0}.gz', lines)
    grid = bd.combined_price_grid(
        bd.scan_hl_time_policy([path], 'bbo+fast', bd.Window(0, 2 ** 62)))
    assert grid.tick_exponent == -5, 'the bbo touch is on the grid too'
    assert grid.prices == 4, 'two book levels and two bbo sides, no trade price'


def hl_two_cadence_day(tmp_path):
    """A recording whose slow cadence reaches below a decade the fast one never does.

    The touch and the five fast levels sit on 1e-3 above 10; the 20-level slow
    snapshot reaches 9.9905, three levels further down. Measured on ten day-29
    coins: the slow cadence's minimum sits 0.1–0.5 % below the ``bbo`` minimum
    (dot 0.74937 against 0.75125), so this shape is what every real day looks
    like, not a corner.
    """
    base_ms = DAY0_START // MS
    lines = [
        (DAY0_START + MS, hl_l2book('DOT', base_ms, '10.000', '10.001', fast=True)),
        (DAY0_START + 2 * MS, hl_bbo('DOT', base_ms, '10.005', '10.006')),
        (DAY0_START + 3 * MS,
         hl_l2book_levels('DOT', base_ms, ['10.000', '9.9990', '9.9905'], ['10.001'])),
    ]
    return write_gz(tmp_path / f'dot_{DAY0}.gz', lines)


def test_the_grid_reads_only_the_frames_the_book_mode_converts(tmp_path):
    """Depth the dataset will not contain is not evidence about the dataset's grid.

    ``hyperliquid.convert`` skips a whole ``l2Book`` frame whose cadence is not
    the one ``book_mode`` asked for (``is_fast != (book_mode != 'slow')``), and
    reads ``bbo`` only under a fusing mode. Prices from the frames it skips
    describe a book the backtest never sees; reading them widens the measured
    price range past the dataset's own, and the decade cross-check is built on
    that range.
    """
    path = hl_two_cadence_day(tmp_path)
    window = bd.Window(0, 2 ** 62)

    fused = bd.combined_price_grid(bd.scan_hl_time_policy([path], 'bbo+fast', window))
    assert fused.prices == 4, 'the fast touch and the bbo, not the slow snapshot'
    assert bd._unscale(fused.min_px) == 10.0
    assert bd.tick_from_exponent(fused.tick_exponent) == 0.001

    slow = bd.combined_price_grid(bd.scan_hl_time_policy([path], 'slow', window))
    assert slow.prices == 4, 'the slow snapshot alone — bbo is not converted here'
    assert bd._unscale(slow.min_px) == 9.9905


def test_a_decade_only_an_unconverted_frame_crosses_does_not_refuse(tmp_path):
    """The refusal is about the prices the dataset holds, not the ones on disk.

    Under ``bbo+fast`` nothing below 10 is converted, so one tick describes the
    window and there is nothing to refuse. Under ``slow`` the deep levels are
    the dataset, the crossing is real, and the refusal stands — the same
    recording, and the answer follows the mode.
    """
    path = hl_two_cadence_day(tmp_path)
    window = bd.Window(0, 2 ** 62)

    out = bd.reconcile_tick(
        bd.combined_price_grid(bd.scan_hl_time_policy([path], 'bbo+fast', window)),
        sz_decimals=0, symbol='DOT')
    assert out['value'] == 0.001
    assert out['cross_check'] == 'agree'

    with pytest.raises(bd.BuildError) as e:
        bd.reconcile_tick(
            bd.combined_price_grid(bd.scan_hl_time_policy([path], 'slow', window)),
            sz_decimals=0, symbol='DOT')
    assert 'decade' in str(e.value)


def test_the_grid_is_measured_inside_the_window_only(tmp_path):
    base_ms = DAY0_START // MS
    lines = [
        (DAY0_START + MS, hl_l2book('BTC', base_ms, '0.4000', '0.4100')),
        (DAY0_START + 10 * MS,
         hl_l2book('BTC', base_ms + 9, '0.400001', '0.410001')),
    ]
    path = write_gz(tmp_path / f'btc_{DAY0}.gz', lines)
    window = bd.Window(DAY0_START, DAY0_START + 5 * MS)
    grid = bd.combined_price_grid(bd.scan_hl_time_policy([path], 'slow', window))
    assert grid.prices == 2
    assert grid.tick_exponent == -2, 'the frame outside the window is not built'


def test_a_measured_tick_that_matches_the_rule_is_recorded_as_agreeing():
    out = bd.reconcile_tick(grid_of(['0.39040', '0.41209', '0.39875']), sz_decimals=0)
    assert out['value'] == 1e-05
    assert out['source'] == 'measured'
    assert out['measured'] == 1e-05
    assert out['rule'] == 1e-05
    assert out['cross_check'] == 'agree'
    assert out['sz_decimals'] == 0


def test_a_window_crossing_a_price_decade_is_refused():
    """Two ticks in one window, and a manifest can carry one.

    Below 10 the venue quotes on 1e-4 and above it on 1e-3; the measurement
    returns the finer grid because every price lands on it, and using it lets
    the backtest quote ten phantom levels between every real one above 10.
    """
    with pytest.raises(bd.BuildError) as e:
        bd.reconcile_tick(grid_of(['9.9995', '9.9999', '10.001', '10.002']),
                          sz_decimals=0)
    msg = str(e.value)
    assert 'decade' in msg
    assert '0.0001' in msg and '0.001' in msg, 'both ticks must be named'
    assert '--tick-size' in msg


def test_a_decade_crossing_the_szdecimals_floor_covers_is_not_refused():
    """The refusal is about the RULE not being constant, not about the decade.

    With szDecimals=6 the floor is 1.0 on both sides of the crossing, so the
    window has one tick after all and there is nothing to refuse.
    """
    out = bd.reconcile_tick(grid_of(['9.0', '11.0', '12.0']), sz_decimals=6)
    assert out['value'] == 1.0
    assert out['cross_check'] == 'agree'


def test_the_measurement_wins_when_szdecimals_says_the_tick_is_coarser(capsys):
    """Prices finer than the szDecimals floor are evidence about szDecimals.

    Registering the rule's coarser tick would make an observed price
    unrepresentable — the converter would round it onto a level the venue never
    quoted. The recording is the evidence (rerank_metrics.summarize_tick).

    Only the szDecimals term is contradicted here: the five-figure term allows
    1e-5 at 0.5 and the recording uses exactly that. The warning has to name the
    term it found wrong, because the other one is a measured venue rule (§11.5)
    and a measurement contradicting *it* is refused two tests down.
    """
    out = bd.reconcile_tick(grid_of(['0.50001', '0.50002', '0.51234']), sz_decimals=4)
    assert out['measured'] == 1e-05
    assert out['rule'] == 0.01
    assert out['value'] == 1e-05
    assert out['cross_check'] == 'measured_finer_than_rule'
    assert out['rule_terms'] == {'significant_figures': -5, 'sz_decimals': -2}
    assert out['exponent'] == out['rule_terms']['significant_figures'], (
        'the measurement matches the measured term and contradicts the read one')
    assert 'szDecimals=4' in capsys.readouterr().err


def test_a_price_above_a_hundred_thousand_measures_the_tick_the_rule_expects():
    """BTC at 118 230: five figures say 10, and the measurement says 10 too.

    The regression this pins is a tool artefact, not a venue one — a measurement
    that stopped at 1.0 read `measured_finer_than_rule`, won the cross-check
    against a rule that was right, and registered a tick ten times too fine while
    warning that the venue had quoted past its own rule.
    """
    out = bd.reconcile_tick(grid_of(['118230', '118240', '118250', '118260']),
                            sz_decimals=5)
    assert out['measured'] == 10.0
    assert out['rule'] == 10.0
    assert out['value'] == 10.0
    assert out['cross_check'] == 'agree'


def test_a_measurement_finer_than_five_significant_figures_is_refused(capsys):
    """Neither value can be registered, so the build stops (§1.1, fail closed).

    `10.0015` has six significant figures, which §11.5 measured the venue
    rejecting. Registering the measured 1e-4 would let the backtest quote levels
    the venue does not accept; registering the rule's 1e-3 would round an
    observed price onto a level that was never quoted. A disagreement with the
    *measured* half of the rule is a broken recording or a broken measurement,
    and the operator's `--tick-size` is the only thing that can settle it.
    """
    with pytest.raises(bd.BuildError) as e:
        bd.reconcile_tick(grid_of(['10.0015', '10.0025', '10.0035']), sz_decimals=0)
    msg = str(e.value)
    assert 'significant' in msg
    assert '0.0001' in msg and '0.001' in msg, 'both ticks must be named'
    assert '--tick-size' in msg


def test_a_quiet_window_measures_a_coarser_tick_than_the_rule_and_says_so(capsys):
    """A window that never used the finest legal increment measures its own grid.

    Every recorded price still lands on it, so nothing is rounded and the
    dataset is intact — the strategy is only held to the levels the day had.
    That is a fact about the window, not about the instrument, so it is warned
    about and carried in the manifest rather than corrected.
    """
    out = bd.reconcile_tick(grid_of(['0.4001', '0.4102', '0.3903']), sz_decimals=0)
    assert out['measured'] == 0.0001
    assert out['rule'] == 1e-05
    assert out['value'] == 0.0001
    assert out['cross_check'] == 'measured_coarser_than_rule'
    assert out['decades_from_rule'] == 1
    assert 'coarser' in capsys.readouterr().err


def test_the_measured_tick_reaches_the_converter_and_the_manifest(priced_dataset):
    """The regression: ONDO day 29 was converted with 1e-6 on a 1e-5 grid."""
    ds = priced_dataset(ONDO_QUOTES, sz_decimals=0)
    conv = FakeConverter()
    bd.main(base_argv(ds), convert_fn=conv, snapshot_fn=FakeSnapshotter())

    assert [c['tick_size'] for c in conv.calls] == [1e-05]
    m = json.loads((ds['out'] / 'manifest.json').read_text())
    assert m['tick_size'] == 1e-05, 'szDecimals alone would have said 1e-06'
    assert m['lot_size'] == 1.0
    src = m['tick_lot_source']
    assert src['kind'] == 'measured'
    assert src['tick'] == {
        'value': 1e-05, 'source': 'measured', 'measured': 1e-05, 'rule': 1e-05,
        'exponent': -5, 'rule_exponent': -5, 'cross_check': 'agree',
        'decades_from_rule': 0, 'sz_decimals': 0,
        'rule_terms': {'significant_figures': -5, 'sz_decimals': -6},
        'rule_note': src['tick']['rule_note'],
    }
    assert src['sz_decimals'] == 0, 'backtest_first.py reads this path'
    assert src['measurement']['distinct_prices'] >= 2
    assert src['measurement']['prices'] > 0
    assert src['measurement']['channels'] == ['l2Book', 'bbo']
    assert src['measurement']['min_px'] == 0.39040, 'the lowest bid quoted'
    assert src['measurement']['max_px'] == 0.41210, 'the highest ask quoted'


def test_a_high_priced_day_registers_the_tick_the_venue_enforces(priced_dataset):
    """The other end of the ONDO regression, end to end.

    A day quoted in tens must reach the converter and the manifest as a tick of
    10 — a measurement that could not exceed 1.0 registered 1.0 here, ten times
    too fine, and said in the log that the venue was at fault.
    """
    ds = priced_dataset(HIGH_PRICE_QUOTES, sz_decimals=5)
    conv = FakeConverter()
    bd.main(base_argv(ds), convert_fn=conv, snapshot_fn=FakeSnapshotter())

    assert [c['tick_size'] for c in conv.calls] == [10.0]
    m = json.loads((ds['out'] / 'manifest.json').read_text())
    assert m['tick_size'] == 10.0
    assert m['lot_size'] == 1e-05
    tick = m['tick_lot_source']['tick']
    assert tick['measured'] == 10.0 and tick['rule'] == 10.0
    assert tick['cross_check'] == 'agree'


def test_a_decade_crossing_day_refuses_the_build_before_converting(priced_dataset,
                                                                   capsys):
    ds = priced_dataset(DECADE_CROSSING_QUOTES, sz_decimals=0)
    conv = FakeConverter()
    with pytest.raises(SystemExit) as e:
        bd.main(base_argv(ds), convert_fn=conv)
    assert e.value.code == 1
    assert conv.calls == [], 'the refusal comes before any conversion'
    assert 'decade' in capsys.readouterr().err


def test_an_explicit_tick_size_overrides_the_decade_crossing_refusal(priced_dataset):
    """The override always wins and is recorded as `cli` — unchanged behaviour."""
    ds = priced_dataset(DECADE_CROSSING_QUOTES, sz_decimals=0)
    conv = FakeConverter()
    bd.main(base_argv(ds, tick_size='0.001', lot_size='1.0'), convert_fn=conv,
            snapshot_fn=FakeSnapshotter())
    m = json.loads((ds['out'] / 'manifest.json').read_text())
    assert m['tick_size'] == 0.001
    assert m['tick_lot_source']['kind'] == 'cli'
    assert [c['tick_size'] for c in conv.calls] == [0.001]


def test_the_cli_override_records_the_measurement_it_overrode(priced_dataset, capsys):
    """An override that disagrees with the recording is the ONDO shape again."""
    ds = priced_dataset(ONDO_QUOTES, sz_decimals=0)
    bd.main(base_argv(ds, tick_size='0.000001', lot_size='1.0'),
            convert_fn=FakeConverter(), snapshot_fn=FakeSnapshotter())
    m = json.loads((ds['out'] / 'manifest.json').read_text())
    src = m['tick_lot_source']
    assert src['kind'] == 'cli'
    assert src['measurement']['tick'] == 1e-05
    assert 'finer' in capsys.readouterr().err


def test_tick_lot_from_cli_overrides_and_is_recorded(dataset):
    ds = dataset
    bd.main(base_argv(ds, tick_size='0.5', lot_size='0.001'),
            convert_fn=FakeConverter(), snapshot_fn=FakeSnapshotter())
    m = json.loads((ds['out'] / 'manifest.json').read_text())
    assert (m['tick_size'], m['lot_size']) == (0.5, 0.001)
    assert m['tick_lot_source']['kind'] == 'cli'


def test_one_sided_tick_lot_flag_is_refused(dataset):
    ds = dataset
    with pytest.raises(SystemExit) as e:
        bd.main(base_argv(ds, tick_size='0.5'), convert_fn=FakeConverter())
    assert e.value.code == 1


def test_missing_universe_and_no_cli_tick_is_refused(dataset):
    ds = dataset
    write_meta(ds['hl_dir'] / f'_meta_hyperliquid_{DAY0}.jsonl',
               [(DAY0_START, session_start('hyperliquid', ['BTC']))])
    conv = FakeConverter()
    with pytest.raises(SystemExit) as e:
        bd.main(base_argv(ds), convert_fn=conv)
    assert e.value.code == 1
    assert conv.calls == []


# ---------------------------------------------------------------------------
# snapshots
# ---------------------------------------------------------------------------

def test_single_day_window_creates_no_snapshot(dataset):
    ds = dataset
    snap = FakeSnapshotter()
    bd.main(base_argv(ds), convert_fn=FakeConverter(), snapshot_fn=snap)
    assert snap.calls == []
    m = json.loads((ds['out'] / 'manifest.json').read_text())
    assert m['snapshots']['created'] == []
    assert 'first day' in m['snapshots']['note'].lower()


# ---------------------------------------------------------------------------
# 3.3 manifest
# ---------------------------------------------------------------------------

REQUIRED_MANIFEST_KEYS = [
    'schema', 'generated_at', 'rebuild_cmd', 'quality_report', 'inputs',
    'collector', 'converter', 'instruments', 'book_mode', 'num_levels',
    'tick_size', 'lot_size', 'tick_lot_source', 'window', 'min_window_hours',
    'min_window_ns', 'clock_correction_ns', 'max_signal_age_ns',
    'max_hl_book_age_ns', 'time_policy', 'outputs', 'signal', 'snapshots',
    'backtest_defaults',
]


def test_manifest_has_every_required_key(dataset):
    ds = dataset
    bd.main(base_argv(ds), convert_fn=FakeConverter(), snapshot_fn=FakeSnapshotter())
    m = json.loads((ds['out'] / 'manifest.json').read_text())
    missing = [k for k in REQUIRED_MANIFEST_KEYS if k not in m]
    assert missing == []


def test_manifest_records_inputs_with_sha256(dataset):
    ds = dataset
    bd.main(base_argv(ds), convert_fn=FakeConverter(), snapshot_fn=FakeSnapshotter())
    m = json.loads((ds['out'] / 'manifest.json').read_text())
    hl_inputs = m['inputs']['hyperliquid']['data']
    assert len(hl_inputs) == 1
    expected = hashlib.sha256((ds['hl_dir'] / f'btc_{DAY0}.gz').read_bytes()).hexdigest()
    assert hl_inputs[0]['sha256'] == expected
    assert m['inputs']['hyperliquid']['meta'][0]['sha256']
    assert m['inputs']['binancefuturesum']['data'][0]['sha256']


def test_manifest_records_collector_commit_and_converter_identity(dataset):
    ds = dataset
    bd.main(base_argv(ds), convert_fn=FakeConverter(), snapshot_fn=FakeSnapshotter())
    m = json.loads((ds['out'] / 'manifest.json').read_text())
    assert m['collector']['hyperliquid']['commits'] == ['abc1234']
    assert m['collector']['binancefuturesum']['commits'] == ['def5678']
    assert 'package_version' in m['converter']
    assert m['converter']['base_latency'] == 0


def test_shared_data_dir_keeps_each_venues_provenance_apart(tmp_path):
    """Days from several instances can be gathered into one directory.

    The sidecars carry the exchange name for exactly that reason
    (collector/README.md, "Output format"), so the directory alone must not
    decide whose collector commit ends up in the manifest.
    """
    shared = tmp_path / 'both'
    hl_lines = hl_day_lines(DAY0_START)
    bn_lines = bn_day_lines(DAY0_START)
    write_gz(shared / f'btc_{DAY0}.gz', hl_lines)
    write_gz(shared / f'btcusdt_{DAY0}.gz', bn_lines)
    write_meta(shared / f'_meta_hyperliquid_{DAY0}.jsonl', [
        (DAY0_START, session_start('hyperliquid', ['BTC'], commit='hlcommit')),
        (DAY0_START + 1, universe_record('BTC', 5)),
    ])
    write_meta(shared / f'_meta_binancefuturesum_{DAY0}.jsonl', [
        (DAY0_START, session_start('binancefuturesum', ['BTCUSDT'], commit='bncommit')),
    ])
    report = make_report(tmp_path / 'report.json', shared, shared,
                         (hl_lines[0][0], hl_lines[-1][0]),
                         (bn_lines[0][0], bn_lines[-1][0]))
    out = tmp_path / 'out'
    bd.main(['--quality-report', str(report), '--hl-symbol', 'BTC',
             '--binance-symbol', 'BTCUSDT', '--out-dir', str(out),
             '--min-window-hours', '0'],
            convert_fn=FakeConverter(), snapshot_fn=FakeSnapshotter())
    m = json.loads((out / 'manifest.json').read_text())
    assert m['collector']['hyperliquid']['commits'] == ['hlcommit']
    assert m['collector']['binancefuturesum']['commits'] == ['bncommit']
    assert [Path(f['path']).name for f in m['inputs']['hyperliquid']['meta']] \
        == [f'_meta_hyperliquid_{DAY0}.jsonl']


def test_manifest_window_and_policy_numbers_are_ints(dataset):
    ds = dataset
    bd.main(base_argv(ds), convert_fn=FakeConverter(), snapshot_fn=FakeSnapshotter())
    m = json.loads((ds['out'] / 'manifest.json').read_text())
    for key in ('start_ns', 'end_ns', 'duration_ns'):
        assert isinstance(m['window'][key], int)
    assert isinstance(m['max_signal_age_ns'], int)
    assert m['max_signal_age_ns'] == 1000 * MS
    assert m['clock_correction_ns'] == 0
    assert isinstance(m['time_policy']['min_local_minus_exch_ns'], int)


def test_manifest_rebuild_cmd_reproduces_the_build(dataset):
    ds = dataset
    bd.main(base_argv(ds), convert_fn=FakeConverter(), snapshot_fn=FakeSnapshotter())
    m = json.loads((ds['out'] / 'manifest.json').read_text())
    cmd = m['rebuild_cmd']
    assert isinstance(cmd, list) and all(isinstance(x, str) for x in cmd)
    assert cmd[1].endswith('build_dataset.py')
    for flag in ('--quality-report', '--hl-symbol', '--binance-symbol',
                 '--out-dir', '--book-mode', '--num-levels',
                 '--min-window-hours', '--clock-correction-ns',
                 '--max-signal-age-ms', '--max-hl-book-age-ms'):
        assert flag in cmd, flag
    # Feeding the recorded argv back in must produce the same manifest body.
    first = dict(m)
    bd.main(cmd[2:], convert_fn=FakeConverter(), snapshot_fn=FakeSnapshotter())
    second = json.loads((ds['out'] / 'manifest.json').read_text())
    for k in first:
        if k == 'generated_at':
            continue
        assert first[k] == second[k], k


def test_manifest_backtest_defaults_are_placeholders_with_the_doc_requirement(dataset):
    ds = dataset
    bd.main(base_argv(ds), convert_fn=FakeConverter(), snapshot_fn=FakeSnapshotter())
    m = json.loads((ds['out'] / 'manifest.json').read_text())
    bt = m['backtest_defaults']
    for key in ('fee_model', 'queue_model', 'exchange_kind', 'constant_latency_ns',
                'latency_offset'):
        assert key in bt
    assert bt['fee_model'] is None
    assert bt['latency_offset'] == 0
    assert 'PartialFillExchange' in json.dumps(bt)


def test_manifest_signal_block_documents_the_column_mapping(dataset):
    ds = dataset
    bd.main(base_argv(ds), convert_fn=FakeConverter(), snapshot_fn=FakeSnapshotter())
    m = json.loads((ds['out'] / 'manifest.json').read_text())
    sig = m['signal']
    assert sig['columns'] == ['bid_px', 'bid_qty', 'ask_px', 'ask_qty']
    assert sig['ts_dtype'] == 'int64' and sig['values_dtype'] == 'float64'
    assert sig['rows'] > 0


# ---------------------------------------------------------------------------
# plumbing
# ---------------------------------------------------------------------------

def test_trim_gz_preserves_the_original_bytes(tmp_path):
    src = write_gz(tmp_path / 'src.gz', [
        (100, '{"a":1}'), (200, '{"b":2}'), (300, '{"c":3}'),
    ])
    dst = tmp_path / 'dst.gz'
    kept = bd.trim_gz(src, dst, bd.Window(150, 300))
    assert kept == 2
    with gzip.open(dst, 'rb') as f:
        assert f.read() == b'200 {"b":2}\n300 {"c":3}\n'


def test_trim_gz_reads_multi_member_gzip(tmp_path):
    """A restart appends a second gzip member; both must be read."""
    path = tmp_path / 'multi.gz'
    with gzip.open(path, 'wb') as f:
        f.write(b'100 {"a":1}\n')
    with gzip.open(path, 'ab') as f:
        f.write(b'200 {"b":2}\n')
    dst = tmp_path / 'dst.gz'
    assert bd.trim_gz(path, dst, bd.Window(0, 1000)) == 2


def test_iter_records_rejects_a_malformed_line(tmp_path):
    path = write_gz(tmp_path / 'bad.gz', [(100, '{"a":1}')])
    with gzip.open(path, 'ab') as f:
        f.write(b'not-a-timestamp {"a":1}\n')
    with pytest.raises(bd.BuildError) as e:
        list(bd.iter_records(path))
    assert 'line 2' in str(e.value)


def test_missing_input_file_for_a_window_day_is_refused(dataset):
    ds = dataset
    (ds['hl_dir'] / f'btc_{DAY0}.gz').unlink()
    conv = FakeConverter()
    with pytest.raises(SystemExit) as e:
        bd.main(base_argv(ds), convert_fn=conv)
    assert e.value.code == 1
    assert conv.calls == []


def test_a_day_file_with_nothing_inside_the_window_is_skipped_and_recorded(dataset):
    """A window ending at midnight touches day N+1 whose file starts later."""
    ds = dataset
    hl2 = hl_day_lines(DAY1_START, base_ms=(DAY1_START // MS) + 1000)
    bn2 = bn_day_lines(DAY1_START, base_ms=(DAY1_START // MS) + 1000)
    write_gz(ds['hl_dir'] / f'btc_{DAY1}.gz', hl2)
    write_gz(ds['bn_dir'] / f'btcusdt_{DAY1}.gz', bn2)
    make_report(ds['report'], ds['hl_dir'], ds['bn_dir'],
                (ds['hl_lines'][0][0], DAY1_START),
                (ds['bn_lines'][0][0], DAY1_START),
                hl_days={DAY0: {'verdict': 'green'}, DAY1: {'verdict': 'green'}},
                bn_days={DAY0: {'verdict': 'green'}, DAY1: {'verdict': 'green'}})
    snap = FakeSnapshotter()
    bd.main(base_argv(ds), convert_fn=FakeConverter(), snapshot_fn=snap)
    m = json.loads((ds['out'] / 'manifest.json').read_text())
    assert [o['day'] for o in m['outputs']['hl_depth']] == [DAY0]
    assert [s['day'] for s in m['outputs']['skipped_days']] == [DAY1]
    assert snap.calls == [], 'nothing to chain a snapshot to'


def test_missing_signal_file_for_a_window_day_is_refused(dataset):
    """Mode A: the signal must cover the whole window, on both sides."""
    ds = dataset
    hl2 = hl_day_lines(DAY1_START)
    write_gz(ds['hl_dir'] / f'btc_{DAY1}.gz', hl2)
    # Binance coverage claims day 1, but no file for it exists.
    make_report(ds['report'], ds['hl_dir'], ds['bn_dir'],
                (ds['hl_lines'][0][0], hl2[-1][0]),
                (ds['bn_lines'][0][0], hl2[-1][0]),
                hl_days={DAY0: {'verdict': 'green'}, DAY1: {'verdict': 'green'}},
                bn_days={DAY0: {'verdict': 'green'}, DAY1: {'verdict': 'green'}})
    conv = FakeConverter()
    with pytest.raises(SystemExit) as e:
        bd.main(base_argv(ds), convert_fn=conv)
    assert e.value.code == 1
    assert conv.calls == []


def test_outputs_are_named_and_listed_in_the_manifest(dataset):
    ds = dataset
    bd.main(base_argv(ds), convert_fn=FakeConverter(), snapshot_fn=FakeSnapshotter())
    m = json.loads((ds['out'] / 'manifest.json').read_text())
    hl_out = m['outputs']['hl_depth'][0]
    assert Path(hl_out['path']).exists()
    assert hl_out['day'] == DAY0
    assert Path(m['outputs']['signal']['path']).exists()


# ---------------------------------------------------------------------------
# integration with the real converter (skipped when the native module is absent)
# ---------------------------------------------------------------------------

def test_end_to_end_with_the_real_hyperliquid_converter(dataset):
    hyperliquid = pytest.importorskip('hftbacktest.data.utils.hyperliquid')
    ds = dataset

    bd.main(base_argv(ds, buffer_size='20000'), convert_fn=hyperliquid.convert,
            snapshot_fn=FakeSnapshotter())
    out = np.load(ds['out'] / f'hl_btc_{DAY0}.npz')['data']
    assert len(out) > 0
    latency = out['local_ts'].astype(np.int64) - out['exch_ts'].astype(np.int64)
    assert latency.min() >= 0
    # The converter must not have shifted anything: base_latency=0 and the raw
    # minimum was already non-negative.
    assert int(out['local_ts'].min()) == ds['hl_lines'][0][0]


def test_reads_a_report_produced_by_the_phase_2_tool(dataset):
    """Contract test across the Phase 2 / Phase 3 seam.

    The verdict of a four-frame fixture is not the point and is not asserted —
    the key names, the nesting and the int-ness of the coverage are.
    """
    quality_report = pytest.importorskip('quality_report')
    ds = dataset
    report = quality_report.build_report(
        [ds['hl_dir'], ds['bn_dir']], profile='mode-a-v1', day=DAY0,
        include_today=False,
    )
    assert report['schema'] == bd.REPORT_SCHEMA
    hl_cov = bd.venue_coverage(report, bd.HL_VENUE)
    bn_cov = bd.venue_coverage(report, bd.SIGNAL_VENUE)
    assert all(isinstance(v, int) for v in hl_cov + bn_cov)
    assert bd.venue_data_dir(report, bd.HL_VENUE, ds['report']) == ds['hl_dir']

    # The seam Phase 3 actually trims on: per symbol, over its required streams.
    (hl_first, hl_last), hl_per_day = bd.symbol_coverage(report, bd.HL_VENUE, 'BTC')
    (bn_first, bn_last), _ = bd.symbol_coverage(report, bd.SIGNAL_VENUE, 'BTCUSDT')
    assert all(isinstance(v, int) for v in (hl_first, hl_last, bn_first, bn_last))
    assert set(hl_per_day) == {DAY0}
    window = bd.intersect((hl_first, hl_last), (bn_first, bn_last))
    assert not window.is_empty and window.days() == [DAY0]
    bd.require_symbol_days(hl_per_day, window.days(), bd.HL_VENUE, 'BTC')
    verdict, _reasons, _outside = bd.worst_verdict(
        report, (bd.HL_VENUE, bd.SIGNAL_VENUE), window.days())
    assert verdict in ('green', 'yellow', 'red')


def test_end_to_end_with_the_real_converter_over_two_days(dataset):
    """Two days through the real converter — this checks the call signature.

    Both days go to one `BacktestAsset` as a continuous stream, so no
    intra-window snapshot is built and none is needed; `snapshot_fn` is not
    called at all.
    """
    hyperliquid = pytest.importorskip('hftbacktest.data.utils.hyperliquid')
    ds = dataset
    hl2 = hl_day_lines(DAY1_START)
    bn2 = bn_day_lines(DAY1_START)
    write_gz(ds['hl_dir'] / f'btc_{DAY1}.gz', hl2)
    write_gz(ds['bn_dir'] / f'btcusdt_{DAY1}.gz', bn2)
    make_report(ds['report'], ds['hl_dir'], ds['bn_dir'],
                (ds['hl_lines'][0][0], hl2[-1][0]),
                (ds['bn_lines'][0][0], bn2[-1][0]),
                hl_days={DAY0: {'verdict': 'green'}, DAY1: {'verdict': 'green'}},
                bn_days={DAY0: {'verdict': 'green'}, DAY1: {'verdict': 'green'}})

    snap = FakeSnapshotter()
    bd.main(base_argv(ds, buffer_size='20000'), convert_fn=hyperliquid.convert,
            snapshot_fn=snap)
    m = json.loads((ds['out'] / 'manifest.json').read_text())
    assert snap.calls == []
    assert m['snapshots']['created'] == []
    assert len(m['outputs']['hl_depth']) == 2
    for out in m['outputs']['hl_depth']:
        assert np.load(out['path'])['data'].shape[0] > 0


# ---------------------------------------------------------------------------
# review fixes
# ---------------------------------------------------------------------------

def test_window_uses_the_built_symbols_coverage_not_the_venue_union(dataset, tmp_path):
    """§3.1's window is the *built instrument's* valid range.

    The venue number is a union over every symbol and every required stream, so
    a second symbol that started ten minutes earlier — a partially accepted
    subscription is exactly this shape — would open the window over a stretch
    where the traded symbol has no data at all. The doc: "обрезка гарантирует,
    что прогон не начнётся раньше первого сигнала".
    """
    ds = dataset
    bn_first = ds['bn_lines'][0][0]
    bn_last = ds['bn_lines'][-1][0]
    early = bn_first - 600 * NS
    report = json.loads(Path(ds['report']).read_text())
    venue = report['venues']['binancefuturesum']
    # The union widens; the built symbol's own coverage does not.
    venue['coverage']['first_local_ts'] = early
    venue['days'][DAY0]['symbols']['ethusdt'] = symbol_entry(early, bn_last)
    Path(ds['report']).write_text(json.dumps(report))

    bd.main(base_argv(ds), convert_fn=FakeConverter(), snapshot_fn=FakeSnapshotter())
    m = json.loads((ds['out'] / 'manifest.json').read_text())
    assert m['window']['start_ns'] == max(ds['hl_lines'][0][0], bn_first)
    assert m['window']['coverage']['binancefuturesum']['first_local_ts'] == bn_first


def test_a_late_required_stream_of_the_built_symbol_moves_the_window(dataset):
    """Same defect one level down: a late `l2Book` hidden by an on-time `bbo`."""
    ds = dataset
    late = ds['hl_lines'][0][0] + NS
    report = json.loads(Path(ds['report']).read_text())
    report['venues']['hyperliquid']['days'][DAY0]['symbols']['btc'] = symbol_entry(
        late, ds['hl_lines'][-1][0],
        ('trades', 'bbo', 'l2Book_slow', 'l2Book_fast'))
    Path(ds['report']).write_text(json.dumps(report))

    bd.main(base_argv(ds), convert_fn=FakeConverter(), snapshot_fn=FakeSnapshotter())
    m = json.loads((ds['out'] / 'manifest.json').read_text())
    assert m['window']['start_ns'] == late


def test_a_report_without_per_symbol_coverage_is_refused(dataset, capsys):
    """An older report cannot be trimmed correctly, so it is not guessed at."""
    ds = dataset
    report = json.loads(Path(ds['report']).read_text())
    del report['venues']['binancefuturesum']['days'][DAY0]['symbols']['btcusdt']['coverage']
    Path(ds['report']).write_text(json.dumps(report))
    with pytest.raises(SystemExit) as e:
        bd.main(base_argv(ds), convert_fn=FakeConverter(), snapshot_fn=FakeSnapshotter())
    assert e.value.code == 1
    assert 'coverage' in capsys.readouterr().err


def test_the_built_symbol_missing_from_the_report_is_refused(dataset, capsys):
    ds = dataset
    argv = base_argv(ds)
    argv[argv.index('--binance-symbol') + 1] = 'ETHUSDT'
    with pytest.raises(SystemExit) as e:
        bd.main(argv, convert_fn=FakeConverter(), snapshot_fn=FakeSnapshotter())
    assert e.value.code == 1
    err = capsys.readouterr().err
    assert 'ethusdt' in err.lower()


def test_clock_correction_moves_the_manifest_window_onto_the_shifted_scale(dataset):
    """Both arrays are shifted; the window has to end up on the same scale.

    With the window left on the raw scale, the first `c` ns of the declared
    window hold no data and the last `c` ns of the data are past `window_end`,
    which is what Phase 4 bounds its loop with.
    """
    ds = dataset
    c = 1_500_000
    bd.main(base_argv(ds, clock_correction_ns=c),
            convert_fn=FakeConverter(), snapshot_fn=FakeSnapshotter())
    m = json.loads((ds['out'] / 'manifest.json').read_text())
    raw_start = max(ds['hl_lines'][0][0], ds['bn_lines'][0][0])
    raw_end = min(ds['hl_lines'][-1][0], ds['bn_lines'][-1][0])
    assert m['window']['raw_start_ns'] == raw_start
    assert m['window']['raw_end_ns'] == raw_end
    assert m['window']['start_ns'] == raw_start + c
    assert m['window']['end_ns'] == raw_end + c

    signal = np.load(ds['out'] / 'signal_binancefuturesum_btcusdt.npz')
    assert int(signal['ts'][0]) >= m['window']['start_ns']
    assert int(signal['ts'][-1]) <= m['window']['end_ns']
    depth = np.load(m['outputs']['hl_depth'][0]['path'])['data']
    assert int(depth['local_ts'].max()) <= m['window']['end_ns']


def test_converter_arguments_are_explicit_and_recorded(dataset):
    """`delete_out_of_book` and `exch_ts_multiplier` decide what the backtest sees.

    Both were left to the converter's defaults, so a change to either would have
    altered every dataset built afterwards with no diff in the manifest — and
    the multiplier is also hard-coded in the pre-conversion time-policy scan,
    where a disagreement would produce a wrong diagnosis.
    """
    ds = dataset
    conv = FakeConverter()
    bd.main(base_argv(ds), convert_fn=conv, snapshot_fn=FakeSnapshotter())
    call = conv.calls[0]
    assert call['delete_out_of_book'] is True
    assert call['exch_ts_multiplier'] == bd.NS_PER_MS
    m = json.loads((ds['out'] / 'manifest.json').read_text())
    assert m['converter']['delete_out_of_book'] is True
    assert m['converter']['exch_ts_multiplier'] == bd.NS_PER_MS
    assert m['converter']['num_levels'] == 20
    assert m['converter']['book_mode'] == 'slow'


# ---------------------------------------------------------------------------
# replayed-trade de-duplication (converter -> manifest)
# ---------------------------------------------------------------------------


def add_second_day(ds):
    """Extend the one-day fixture to two days, report included."""
    hl2 = hl_day_lines(DAY1_START)
    bn2 = bn_day_lines(DAY1_START)
    write_gz(ds['hl_dir'] / f'btc_{DAY1}.gz', hl2)
    write_gz(ds['bn_dir'] / f'btcusdt_{DAY1}.gz', bn2)
    make_report(ds['report'], ds['hl_dir'], ds['bn_dir'],
                (ds['hl_lines'][0][0], hl2[-1][0]),
                (ds['bn_lines'][0][0], bn2[-1][0]),
                hl_days={DAY0: {'verdict': 'green'}, DAY1: {'verdict': 'green'}},
                bn_days={DAY0: {'verdict': 'green'}, DAY1: {'verdict': 'green'}})


def test_manifest_records_how_many_replayed_trades_were_dropped(dataset):
    """Hyperliquid replays its last 30 fills per coin on every (re)subscribe.

    The converter drops them; how many it dropped is a property of the dataset,
    so it belongs in the manifest next to the rest of the converter identity.
    """
    ds = dataset
    bd.main(base_argv(ds), convert_fn=DedupReportingConverter(3),
            snapshot_fn=FakeSnapshotter())
    m = json.loads((ds['out'] / 'manifest.json').read_text())
    assert m['converter']['deduplicated_trades'] == 3
    assert m['outputs']['hl_depth'][0]['deduplicated_trades'] == 3


def test_manifest_sums_the_dedup_count_over_the_days_of_the_window(dataset):
    ds = dataset
    add_second_day(ds)
    bd.main(base_argv(ds), convert_fn=DedupReportingConverter([3, 4]),
            snapshot_fn=FakeSnapshotter())
    m = json.loads((ds['out'] / 'manifest.json').read_text())
    assert [o['deduplicated_trades'] for o in m['outputs']['hl_depth']] == [3, 4]
    assert m['converter']['deduplicated_trades'] == 7


def test_dedup_count_is_null_when_the_converter_does_not_report_it(dataset):
    """Null, never a guess: a converter that says nothing is not a clean day."""
    ds = dataset
    bd.main(base_argv(ds), convert_fn=FakeConverter(), snapshot_fn=FakeSnapshotter())
    m = json.loads((ds['out'] / 'manifest.json').read_text())
    assert 'deduplicated_trades' in m['converter'], 'the key is always present'
    assert m['converter']['deduplicated_trades'] is None
    assert m['outputs']['hl_depth'][0]['deduplicated_trades'] is None


def test_a_converter_without_the_stats_parameter_still_builds(dataset):
    """The out-param is additive; an older installed `hftbacktest` has no `stats`."""
    ds = dataset
    conv = OldConverter()
    bd.main(base_argv(ds), convert_fn=conv, snapshot_fn=FakeSnapshotter())
    m = json.loads((ds['out'] / 'manifest.json').read_text())
    assert len(conv.calls) == 1
    assert m['converter']['deduplicated_trades'] is None


def test_a_partially_reported_dedup_count_is_null_not_a_partial_sum(dataset):
    """One day reporting 3 and another reporting nothing does not make the day 3."""
    ds = dataset
    add_second_day(ds)
    bd.main(base_argv(ds), convert_fn=DedupReportingConverter([3, None]),
            snapshot_fn=FakeSnapshotter())
    m = json.loads((ds['out'] / 'manifest.json').read_text())
    assert [o['deduplicated_trades'] for o in m['outputs']['hl_depth']] == [3, None]
    assert m['converter']['deduplicated_trades'] is None


def test_the_default_convert_fn_drops_stats_for_an_older_hftbacktest(tmp_path, monkeypatch):
    """The default path hides the installed converter behind its own `**kwargs`.

    `accepts_stats` can only see that wrapper, so it hands over the out-param;
    forwarding it verbatim to a converter that predates it would raise
    `TypeError` and kill the build instead of recording null.
    """
    hyperliquid = pytest.importorskip('hftbacktest.data.utils.hyperliquid')
    old = OldConverter()
    monkeypatch.setattr(hyperliquid, 'convert', old)
    src = write_gz(tmp_path / f'btc_{DAY0}.gz', hl_day_lines(DAY0_START))

    stats = {}
    arr = bd.default_convert_fn(
        input_filename=str(src), tick_size=0.1, lot_size=0.00001, num_levels=20,
        book_mode='slow', base_latency=0, buffer_size=20000, output_filename=None,
        delete_out_of_book=True, exch_ts_multiplier=bd.NS_PER_MS, stats=stats)

    assert len(arr) > 0
    assert stats == {}, 'nothing reported, so nothing is recorded'
    assert len(old.calls) == 1


def test_the_real_converter_reports_its_dedup_count_into_the_manifest(dataset):
    """The seam that matters: real converter, real replay, manifest number."""
    pytest.importorskip('hftbacktest.data.utils.hyperliquid')
    from hftbacktest.data.utils import hyperliquid

    ds = dataset
    base_ms = DAY0_START // MS
    lines = list(ds['hl_lines'])
    for i in range(3):
        exch_ms = base_ms + i * 1000
        lines.append((exch_ms * MS + 200 * MS + 3500,
                      hl_trades('BTC', [(exch_ms, 100 + i, 0.5, 900 + i)])))
    # The resubscribe frame: two fills already seen, with their original tids
    # and venue times, plus one that is new.
    replay_ms = base_ms + 4000
    lines.append((replay_ms * MS + 200 * MS + 3500, hl_trades('BTC', [
        (base_ms + 1000, 101, 0.5, 901),
        (base_ms + 2000, 102, 0.5, 902),
        (replay_ms, 104, 0.5, 904),
    ])))
    lines.sort(key=lambda record: record[0])
    write_gz(ds['hl_dir'] / f'btc_{DAY0}.gz', lines)

    bd.main(base_argv(ds, buffer_size='20000'), convert_fn=hyperliquid.convert,
            snapshot_fn=FakeSnapshotter())
    m = json.loads((ds['out'] / 'manifest.json').read_text())
    assert m['converter']['deduplicated_trades'] == 2


def test_multi_day_window_builds_no_intra_window_snapshots(dataset):
    """The chain was output nothing could consume.

    `asset.data([day1..dayN])` reads the days as one continuous stream, so the
    book already carries across the file boundary; the only snapshot that would
    change a run is one built from the day *before* the window, which is not in
    it. Phase 4 keyed its lookup on `for_day == first day` while this wrote
    `for_day = days[1..N]`, so every chained snapshot cost a native-engine run
    and was then never selected.
    """
    ds = dataset
    hl2 = hl_day_lines(DAY1_START)
    bn2 = bn_day_lines(DAY1_START)
    write_gz(ds['hl_dir'] / f'btc_{DAY1}.gz', hl2)
    write_gz(ds['bn_dir'] / f'btcusdt_{DAY1}.gz', bn2)
    make_report(ds['report'], ds['hl_dir'], ds['bn_dir'],
                (ds['hl_lines'][0][0], hl2[-1][0]),
                (ds['bn_lines'][0][0], bn2[-1][0]),
                hl_days={DAY0: {'verdict': 'green'}, DAY1: {'verdict': 'green'}},
                bn_days={DAY0: {'verdict': 'green'}, DAY1: {'verdict': 'green'}})
    snap = FakeSnapshotter()
    bd.main(base_argv(ds), convert_fn=FakeConverter(), snapshot_fn=snap)

    assert snap.calls == [], 'no snapshot is consumable inside one continuous run'
    m = json.loads((ds['out'] / 'manifest.json').read_text())
    assert m['snapshots']['created'] == []
    assert m['snapshots']['initial_snapshot'] is None
    assert len(m['outputs']['hl_depth']) == 2


def test_manifest_records_the_models_it_was_given(dataset):
    """§3.3 lists fee model, queue model, exchange kind and constant_latency.

    They were null placeholders in a shape Phase 4 could not read even when
    filled in — a dict where the reader wanted a 2-list — so a declared latency
    was silently replaced by the built-in profile.
    """
    ds = dataset
    bd.main(base_argv(ds, maker_fee=0.00015, taker_fee=0.00045,
                      entry_latency_ms=5, resp_latency_ms=9),
            convert_fn=FakeConverter(), snapshot_fn=FakeSnapshotter())
    m = json.loads((ds['out'] / 'manifest.json').read_text())
    bt = m['backtest_defaults']
    assert bt['fee_model'] == {
        'kind': 'TradingValueFeeModel', 'maker_fee': 0.00015, 'taker_fee': 0.00045}
    assert bt['constant_latency_ns'] == {'entry': 5_000_000, 'response': 9_000_000}
    assert bt['queue_model'] == {'kind': 'LogProbQueueModel2'}
    assert bt['exchange_kind']['kind'] == 'NoPartialFillExchange'


def test_manifest_no_longer_forbids_the_partial_fill_exchange(dataset):
    """`forbidden` used to ban PartialFillExchange on the ground that a partial
    fill never reached the strategy position. True once, false since the
    partial-fill fix (AGENTS.md §4.6): proc/local.rs and proc/l3_local.rs apply
    every execution response carrying exec_qty > 0, not only Status::Filled.

    Pinned because the claim does not merely sit in a comment — it is written
    into a machine-readable artifact that outlives the session and is read by
    whoever picks the models up months later. A false entry there talks a
    reader out of the only exchange model that fills the way a venue does.
    """
    ds = dataset
    bd.main(base_argv(ds), convert_fn=FakeConverter(), snapshot_fn=FakeSnapshotter())
    bt = json.loads((ds['out'] / 'manifest.json').read_text())['backtest_defaults']

    assert bt['forbidden'] == [], 'nothing is forbidden since the partial-fill fix'

    # The alternative is named in the artifact, so a reader holding only the
    # manifest learns the choice exists and how to make it.
    alt = bt['exchange_kind']['alternative']
    assert alt['kind'] == 'PartialFillExchange'
    assert '--exchange partial' in alt['how']
    assert 'continuity' in alt['why_not_default'].lower(), \
        'NoPartialFillExchange stays the default only to keep older runs comparable'

    # What PartialFillExchange still does not model is recorded as a caveat on
    # the model — not as a ban, and not dropped on the floor.
    assert 'IOC' in alt['residual_gap']

    # The correction states what changed, and points at the code that changed.
    assert 'proc/local.rs' in bt['forbidden_note']


def test_manifest_models_stay_null_when_nothing_was_declared(dataset):
    ds = dataset
    bd.main(base_argv(ds), convert_fn=FakeConverter(), snapshot_fn=FakeSnapshotter())
    bt = json.loads((ds['out'] / 'manifest.json').read_text())['backtest_defaults']
    assert bt['fee_model'] is None
    assert bt['constant_latency_ns'] == {'entry': None, 'response': None}


def test_half_a_latency_pair_is_refused(dataset, capsys):
    with pytest.raises(SystemExit):
        bd.main(base_argv(dataset, entry_latency_ms=5),
                convert_fn=FakeConverter(), snapshot_fn=FakeSnapshotter())
    assert 'resp' in capsys.readouterr().err


def test_signal_sort_is_stable_past_the_insertion_sort_cutoff(tmp_path):
    """numpy's introsort is stable below ~16 elements, so a small fixture cannot
    tell `kind='stable'` from `kind='quicksort'` — and a real `@bookTicker` day
    has millions of same-nanosecond ties, where quicksort is not."""
    ts0 = DAY0_START + 5 * NS
    n = 64
    records = [(ts0 - NS, binance_book_ticker('BTCUSDT', ts0 // MS, 5, 5, 6, 6))]
    records += [
        (ts0, binance_book_ticker('BTCUSDT', ts0 // MS, 10 + i, 1, 11 + i, 1))
        for i in range(n)
    ]
    path = write_gz(tmp_path / f'btcusdt_{DAY0}.gz', records)
    ts, values = bd.build_signal([path], 'BTCUSDT', bd.Window(0, 2 ** 62))
    assert list(ts) == [ts0 - NS] + [ts0] * n
    assert list(values[1:, 0]) == [float(10 + i) for i in range(n)], (
        'arrival order among the ties must survive the sort')
