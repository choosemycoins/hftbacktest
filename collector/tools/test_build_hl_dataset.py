"""Tests for ``build_hl_dataset.py`` — the Hyperliquid-execution-only builder.

Same discipline as ``test_build_dataset.py``: synthetic gzip recordings written
into ``tmp_path``, a fake converter, no network and no real files.

The fixture helpers are imported from ``test_build_dataset`` rather than copied.
They are the collector's line format and the Phase-2 report shape — one
definition, so a change to either cannot leave one of the two builders being
tested against a recording the other would not recognise.
"""

import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))

import build_dataset as bd  # noqa: E402
import build_hl_dataset as hl  # noqa: E402
from test_build_dataset import (  # noqa: E402
    DAY0,
    DAY0_START,
    DAY1,
    DAY1_START,
    DECADE_CROSSING_QUOTES,
    MS,
    ONDO_QUOTES,
    FakeConverter,
    hl_l2book,
    hl_price_day,
    make_report,
    session_start,
    universe_record,
    write_gz,
    write_meta,
)


@pytest.fixture
def hl_only(tmp_path):
    """One Hyperliquid day, plus a Binance venue that is red on purpose.

    A red signal venue is the situation this builder exists for: the day is
    tradeable on Hyperliquid and there is no mode-A dataset to be had from it.
    """
    def make(quotes=None, *, sz_decimals=0, days=None, hl_verdict='green',
             bn_verdict='red', lines=None):
        hl_dir = tmp_path / 'hl'
        bn_dir = tmp_path / 'bn'
        days = days or [(DAY0, DAY0_START)]
        all_lines = []
        for day, start in days:
            day_lines = (lines(start) if lines is not None
                         else hl_price_day(start, quotes or ONDO_QUOTES))
            write_gz(hl_dir / f'btc_{day}.gz', day_lines)
            all_lines += day_lines
        write_gz(bn_dir / f'btcusdt_{DAY0}.gz',
                 [(DAY0_START, '{"stream":"btcusdt@bookTicker","data":{}}')])
        write_meta(hl_dir / f'_meta_hyperliquid_{DAY0}.jsonl', [
            (DAY0_START, session_start('hyperliquid', ['BTC'])),
            (DAY0_START + 1, universe_record('BTC', sz_decimals)),
        ])
        report = make_report(
            tmp_path / 'report.json', hl_dir, bn_dir,
            (all_lines[0][0], all_lines[-1][0]),
            (DAY0_START, DAY0_START + 1),
            verdict='red',   # overall, because of the signal venue
            hl_days={day: {'verdict': hl_verdict} for day, _ in days},
            bn_days={DAY0: {'verdict': bn_verdict}},
        )
        # `quality_report.py` gives every venue its own verdict — the worst of
        # its days — and the report's is the worst of those. `make_report` omits
        # the venue level; fill it in, because the whole point of this builder
        # is which of the three levels it reads.
        raw = json.loads(report.read_text())
        raw['venues']['hyperliquid']['verdict'] = hl_verdict
        raw['venues']['binancefuturesum']['verdict'] = bn_verdict
        report.write_text(json.dumps(raw))
        return {'root': tmp_path, 'hl_dir': hl_dir, 'bn_dir': bn_dir,
                'report': report, 'out': tmp_path / 'out', 'hl_lines': all_lines}
    return make


def argv(ds, **overrides):
    out = [
        '--quality-report', str(ds['report']),
        '--hl-symbol', 'BTC',
        '--out-dir', str(ds['out']),
        '--min-window-hours', '0',
    ]
    for k, v in overrides.items():
        out += ['--' + k.replace('_', '-'), str(v)]
    return out


def manifest_of(ds):
    return json.loads((ds['out'] / 'manifest.json').read_text())


# ---------------------------------------------------------------------------
# the gate: Hyperliquid only
# ---------------------------------------------------------------------------

def test_a_red_signal_venue_does_not_stop_an_hl_only_build(hl_only):
    """The reason this tool exists: nothing here trades binancefuturesum."""
    ds = hl_only()
    hl.main(argv(ds), convert_fn=FakeConverter())
    m = manifest_of(ds)
    assert m['quality_report']['gated_on'] == 'hyperliquid'
    assert m['quality_report']['venue_verdicts']['binancefuturesum'] == 'red'
    assert m['quality_report']['enforced'] == ['hyperliquid']


def test_a_red_hyperliquid_day_refuses_the_build(hl_only, capsys):
    ds = hl_only(hl_verdict='red')
    conv = FakeConverter()
    with pytest.raises(SystemExit) as e:
        hl.main(argv(ds), convert_fn=conv)
    assert e.value.code == 1
    assert conv.calls == []
    assert 'red' in capsys.readouterr().err


def test_the_window_is_the_hl_symbol_coverage_alone(hl_only):
    """No intersection with a signal that is not part of this dataset."""
    ds = hl_only()
    hl.main(argv(ds), convert_fn=FakeConverter())
    m = manifest_of(ds)
    assert m['window']['start_ns'] == ds['hl_lines'][0][0]
    assert m['window']['end_ns'] == ds['hl_lines'][-1][0]
    assert m['window']['days'] == [DAY0]


# ---------------------------------------------------------------------------
# the tick — the same measured path as build_dataset.py
# ---------------------------------------------------------------------------

def test_the_tick_is_measured_here_too(hl_only):
    """ONDO-shaped: szDecimals=0 would say 1e-6 on a 1e-5 grid."""
    ds = hl_only(ONDO_QUOTES, sz_decimals=0)
    conv = FakeConverter()
    hl.main(argv(ds), convert_fn=conv)
    assert [c['tick_size'] for c in conv.calls] == [1e-05]
    m = manifest_of(ds)
    assert m['tick_size'] == 1e-05
    assert m['lot_size'] == 1.0
    assert m['tick_lot']['kind'] == 'measured'
    assert m['tick_lot']['tick']['cross_check'] == 'agree'
    assert m['tick_lot']['sz_decimals'] == 0


def test_a_decade_crossing_day_refuses_this_build_too(hl_only, capsys):
    ds = hl_only(DECADE_CROSSING_QUOTES, sz_decimals=0)
    conv = FakeConverter()
    with pytest.raises(SystemExit) as e:
        hl.main(argv(ds), convert_fn=conv)
    assert e.value.code == 1
    assert conv.calls == []
    assert 'decade' in capsys.readouterr().err


def test_an_explicit_tick_size_still_wins(hl_only):
    ds = hl_only(DECADE_CROSSING_QUOTES, sz_decimals=0)
    hl.main(argv(ds, tick_size='0.001', lot_size='1.0'), convert_fn=FakeConverter())
    m = manifest_of(ds)
    assert m['tick_size'] == 0.001
    assert m['tick_lot']['kind'] == 'cli'
    assert 'cli' in m['tick_lot_source']


# ---------------------------------------------------------------------------
# the manifest the bridge reads
# ---------------------------------------------------------------------------

def test_the_manifest_declares_the_hl_execution_only_profile(hl_only):
    ds = hl_only()
    hl.main(argv(ds), convert_fn=FakeConverter())
    m = manifest_of(ds)
    assert m['schema'] == 'hl-execution-dataset-v1'
    assert m['profile'] == 'hl-execution-only-v1'
    assert m['mode_a'] is False
    assert 'signal' not in m
    assert m['initial_snapshot'] is False
    assert any('No signal array' in line for line in m['limitations'])


def test_the_manifest_keeps_the_keys_the_bridge_reads(hl_only):
    """`hl-execution-dataset-v1` as built on 2026-07-30: only additions since."""
    ds = hl_only()
    hl.main(argv(ds), convert_fn=FakeConverter())
    m = manifest_of(ds)
    for key in ('schema', 'profile', 'mode_a', 'built_by', 'built_at_utc',
                'instruments', 'tick_size', 'lot_size', 'tick_lot_source',
                'book_mode', 'num_levels', 'window', 'files', 'converter',
                'quality_report', 'initial_snapshot', 'limitations'):
        assert key in m, key
    assert isinstance(m['tick_lot_source'], str), (
        'a string in this schema since the first artefact; the structured form '
        'is the added `tick_lot`')
    assert m['instruments']['execution'] == {
        'venue': 'hyperliquid', 'symbol': 'BTC', 'role': 'BacktestAsset'}
    assert m['files']['execution_npz'] == 'hl_btc_%s.npz' % DAY0
    assert (ds['out'] / m['files']['execution_npz']).exists()
    assert m['converter']['build_dataset_result']['rows'] > 0
    assert m['built_by'].endswith('collector/tools/build_hl_dataset.py')


def test_every_day_of_the_window_is_converted(hl_only):
    ds = hl_only(days=[(DAY0, DAY0_START), (DAY1, DAY1_START)])
    conv = FakeConverter()
    hl.main(argv(ds), convert_fn=conv)
    m = manifest_of(ds)
    assert len(conv.calls) == 2
    assert m['files']['execution_npz_all'] == [
        'hl_btc_%s.npz' % DAY0, 'hl_btc_%s.npz' % DAY1]
    assert m['files']['execution_npz'] is None, (
        'one name cannot stand for two days; the list is the answer')
    assert len(m['outputs']['hl_depth']) == 2


def test_the_manifest_records_the_rebuild_command(hl_only):
    ds = hl_only()
    hl.main(argv(ds), convert_fn=FakeConverter())
    cmd = manifest_of(ds)['rebuild_cmd']
    assert cmd[1].endswith('build_hl_dataset.py')
    assert '--hl-symbol' in cmd and 'BTC' in cmd
    assert '--book-mode' in cmd


# ---------------------------------------------------------------------------
# the checks inherited from build_dataset.py
# ---------------------------------------------------------------------------

def test_the_time_policy_runs_before_any_conversion(hl_only, capsys):
    """A negative feed latency is data to investigate, never to shift."""
    def bad_day(start):
        # An hour into the day, so the frame stays on the day its file is named
        # for; the second one carries a local_ts one millisecond BEFORE the
        # venue timestamp it quotes.
        first_ms = start // MS + 3_600_000
        return [
            (first_ms * MS + 200 * MS, hl_l2book('BTC', first_ms, '0.39040', '0.39045')),
            ((first_ms + 1000) * MS - MS,
             hl_l2book('BTC', first_ms + 1000, '0.39050', '0.39055')),
        ]

    ds = hl_only(lines=bad_day)
    conv = FakeConverter()
    with pytest.raises(SystemExit) as e:
        hl.main(argv(ds), convert_fn=conv)
    assert e.value.code == 1
    assert conv.calls == []
    assert 'time policy' in capsys.readouterr().err


def test_the_installed_converter_is_used_when_none_is_injected(hl_only, monkeypatch):
    """The CLI injects nothing: `python build_hl_dataset.py ...` must convert."""
    conv = FakeConverter()
    monkeypatch.setattr(hl, 'default_convert_fn', conv)
    hl.build(hl.parse_args(argv(hl_only())))
    assert len(conv.calls) == 1


def test_the_book_mode_and_num_levels_stay_paired(hl_only, capsys):
    ds = hl_only()
    with pytest.raises(SystemExit):
        hl.main(argv(ds, book_mode='fast', num_levels=20), convert_fn=FakeConverter())
    assert 'num-levels' in capsys.readouterr().err


def test_the_default_book_mode_is_recorded_with_its_paired_depth(hl_only):
    ds = hl_only()
    hl.main(argv(ds, book_mode='bbo+fast'), convert_fn=FakeConverter())
    m = manifest_of(ds)
    assert (m['book_mode'], m['num_levels']) == ('bbo+fast', 5)
    assert m['max_hl_book_age_ns'] == bd.BOOK_MODE_MAX_AGE_MS['bbo+fast'] * bd.NS_PER_MS
