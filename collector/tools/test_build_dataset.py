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


def hl_l2book(coin, time_ms, bid_px, ask_px, fast=False):
    data = {
        'coin': coin,
        'time': time_ms,
        'levels': [
            [{'px': str(bid_px), 'sz': '1.0', 'n': 1}],
            [{'px': str(ask_px), 'sz': '2.0', 'n': 1}],
        ],
    }
    if fast:
        data['fast'] = True
    return json.dumps({'channel': 'l2Book', 'data': data})


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


def binance_book_ticker(symbol, ts_ms, bid_px, bid_qty, ask_px, ask_qty):
    return json.dumps({
        'stream': f'{symbol.lower()}@bookTicker',
        'data': {
            'e': 'bookTicker', 'u': 1, 's': symbol.upper(),
            'b': str(bid_px), 'B': str(bid_qty),
            'a': str(ask_px), 'A': str(ask_qty),
            'T': ts_ms, 'E': ts_ms,
        },
    })


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


def bn_day_lines(day_start, *, n=6, base_ms=None, latency_ns=5 * MS):
    base_ms = base_ms if base_ms is not None else day_start // MS
    out = []
    for i in range(n):
        exch_ms = base_ms + i * 1000
        local = exch_ms * MS + latency_ns
        out.append((local, binance_book_ticker('BTCUSDT', exch_ms, 100 + i, 1, 101 + i, 2)))
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
# tick / lot
# ---------------------------------------------------------------------------

def test_tick_lot_from_sz_decimals_follows_the_hl_rule():
    # design-hyperliquid-connector.md §5.3: lot = 10^-sz, tick = 10^-(6-sz).
    assert bd.tick_lot_from_sz_decimals(5) == (0.1, 0.00001)
    assert bd.tick_lot_from_sz_decimals(0) == (1e-06, 1.0)
    assert bd.tick_lot_from_sz_decimals(2) == (0.0001, 0.01)


def test_tick_lot_source_is_recorded_when_derived(dataset):
    ds = dataset
    bd.main(base_argv(ds), convert_fn=FakeConverter(), snapshot_fn=FakeSnapshotter())
    m = json.loads((ds['out'] / 'manifest.json').read_text())
    assert m['tick_size'] == 0.1
    assert m['lot_size'] == 0.00001
    assert m['tick_lot_source']['kind'] == 'hl_universe'
    assert m['tick_lot_source']['sz_decimals'] == 5


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
    assert 'PartialFillExchange' in json.dumps(bt['forbidden'])


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
