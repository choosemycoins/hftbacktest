"""Tests for backtest_first.py — Phase 4 of docs/design-multi-venue-collection.md.

Everything here uses synthetic fixtures built in ``tmp_path``: tiny gzip
recordings, tiny ``.npz`` arrays, hand-written manifests. No network, no real
recordings, and — except for the two end-to-end tests at the bottom, which
``importorskip`` — no native module either.

The nanosecond trap is tested explicitly and repeatedly: 1.8e18 ns does not fit
float64 (2^53 ~ 9e15), so every timestamp assertion below uses values that
differ by ones and tens of nanoseconds. A float64 anywhere in the path collapses
them and the test fails.
"""

import gzip
import json
import pathlib
import subprocess
import sys

import numpy as np
import pytest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))

import backtest_first as bf  # noqa: E402

MS = 1_000_000
S = 1_000_000_000

# A realistic 2026 nanosecond epoch: 19 digits, far past float64's exact range.
T0 = 1_785_000_000_000_000_000


# ---------------------------------------------------------------------------
# fixtures
# ---------------------------------------------------------------------------

def write_npz_signal(path, ts, values):
    np.savez(path, ts=np.asarray(ts), values=np.asarray(values))
    return str(path)


def write_hl_npz(path, n=4):
    """A minimal event array in hftbacktest's on-disk shape."""
    dtype = np.dtype(
        [
            ('ev', 'u8'),
            ('exch_ts', 'i8'),
            ('local_ts', 'i8'),
            ('px', 'f8'),
            ('qty', 'f8'),
            ('order_id', 'u8'),
            ('ival', 'i8'),
            ('fval', 'f8'),
        ],
        align=True,
    )
    arr = np.zeros(n, dtype)
    arr['exch_ts'] = T0 + np.arange(n, dtype=np.int64) * S
    arr['local_ts'] = arr['exch_ts'] + 274 * MS
    np.savez_compressed(path, data=arr)
    return str(path)


def make_manifest(tmp_path, **over):
    """A manifest in the shape build_dataset.py (Phase 3) writes."""
    tmp_path = pathlib.Path(tmp_path)
    out = tmp_path / 'dataset'
    out.mkdir(parents=True, exist_ok=True)
    hl_npz = write_hl_npz(out / 'hl_btc_20260725.npz')
    sig_ts = np.arange(20, dtype=np.int64) * (100 * MS) + T0
    sig_val = np.zeros((20, 4), dtype=np.float64)
    sig_val[:, 0] = 60000.0  # bid_px
    sig_val[:, 1] = 1.0      # bid_qty
    sig_val[:, 2] = 60001.0  # ask_px
    sig_val[:, 3] = 1.0      # ask_qty
    sig_npz = write_npz_signal(out / 'signal_btcusdt.npz', sig_ts, sig_val)

    body = {
        'schema': 'hftbacktest/multi-venue-dataset/1',
        'generated_at': '2026-07-26T00:00:00Z',
        'rebuild_cmd': ['python', 'build_dataset.py'],
        'quality_report': str(out / 'report.json'),
        'inputs': {'hyperliquid': {'data': []}, 'binancefuturesum': {'data': []}},
        'collector': {'hyperliquid': {'commits': ['abc1234']}},
        'converter': {'package_version': '2.4.3', 'base_latency': 0},
        'instruments': {
            'hyperliquid': {'symbol': 'BTC'},
            'binancefuturesum': {'symbol': 'BTCUSDT'},
        },
        'book_mode': 'fast',
        'num_levels': 5,
        'tick_size': 0.1,
        'lot_size': 0.00001,
        'tick_lot_source': {'kind': 'hl_universe', 'sz_decimals': 5},
        'window': {
            'start_ns': int(sig_ts[0]),
            'end_ns': int(sig_ts[-1]),
            'duration_ns': int(sig_ts[-1] - sig_ts[0]),
        },
        'min_window_hours': 0,
        'min_window_ns': 0,
        'clock_correction_ns': 0,
        'max_signal_age_ns': 1000 * MS,
        'max_hl_book_age_ns': 1500 * MS,
        'time_policy': {'min_local_minus_exch_ns': 274 * MS},
        'outputs': {
            'hl_depth': [{'path': hl_npz, 'day': '20260725'}],
            'signal': {'path': sig_npz},
        },
        'signal': {
            'columns': ['bid_px', 'bid_qty', 'ask_px', 'ask_qty'],
            'ts_dtype': 'int64',
            'values_dtype': 'float64',
            'rows': 20,
        },
        'snapshots': {'created': [], 'initial_snapshot': None,
                      'note': 'first day of the recording'},
        'backtest_defaults': {
            # A manifest whose models were declared at build time, which is what
            # §3.3 asks for. `test_fees_declared_nowhere_are_refused_...` covers
            # the other shape: nulls, and then the CLI must supply them.
            'fee_model': {'kind': 'TradingValueFeeModel',
                          'maker_fee': 0.00015, 'taker_fee': 0.00045},
            'queue_model': {'kind': 'LogProbQueueModel2'},
            'exchange_kind': {'kind': 'NoPartialFillExchange'},
            'constant_latency_ns': {'entry': None, 'response': None},
            'latency_offset': 0,
        },
    }
    body.update(over)
    path = tmp_path / 'manifest.json'
    path.write_text(json.dumps(body, indent=2))
    return path


@pytest.fixture
def manifest_path(tmp_path):
    return make_manifest(tmp_path)


def fake_stats(**over):
    kw = dict(
        steps=100,
        guard_passed=60,
        guard_blocked={'no_bid': 10, 'no_ask': 0, 'crossed_or_locked': 0,
                       'book_stale': 20, 'signal_stale': 10},
        steps_outside_window=3,
        signal_reads=90,
        signal_absent=7,
        book_changes=42,
        submits=12,
        cancels=4,
        fills=3,
        final_position=0.5,
        balance=-30000.0,
        fee=0.12,
        trading_volume=1.5,
        trading_value=90000.0,
        equity=123.45,
        final_mid_price=60000.5,
        first_ts=T0,
        last_ts=T0 + 10 * S,
        first_guard_pass_step=5,
        elapsed_wall_s=1.25,
        end_reason='end_of_data',
    )
    kw.update(over)
    return bf.RunStats(**kw)


# ---------------------------------------------------------------------------
# 1. guard — truth table; every condition flips it on its own
# ---------------------------------------------------------------------------

GOOD = dict(
    best_bid_tick=600_000,
    best_ask_tick=600_001,
    invalid_min=bf.INVALID_MIN,
    invalid_max=bf.INVALID_MAX,
    last_book_change_ts=T0,
    now_ts=T0 + 500 * MS,
    max_book_age_ns=1500 * MS,
    last_signal_ts=T0 + 400 * MS,
    max_signal_age_ns=1000 * MS,
)


def call_guard(**over):
    kw = dict(GOOD)
    kw.update(over)
    return bf.guard_ok(
        kw['best_bid_tick'], kw['best_ask_tick'], kw['invalid_min'], kw['invalid_max'],
        kw['last_book_change_ts'], kw['now_ts'], kw['max_book_age_ns'],
        kw['last_signal_ts'], kw['max_signal_age_ns'],
    )


def call_reason(**over):
    kw = dict(GOOD)
    kw.update(over)
    return bf.guard_block_reason(
        kw['best_bid_tick'], kw['best_ask_tick'], kw['invalid_min'], kw['invalid_max'],
        kw['last_book_change_ts'], kw['now_ts'], kw['max_book_age_ns'],
        kw['last_signal_ts'], kw['max_signal_age_ns'],
    )


def test_guard_passes_when_everything_is_healthy():
    assert call_guard() is True
    assert call_reason() == bf.GUARD_OK


def test_guard_blocks_when_there_is_no_bid():
    assert call_guard(best_bid_tick=bf.INVALID_MIN) is False
    assert call_reason(best_bid_tick=bf.INVALID_MIN) == bf.GUARD_NO_BID


def test_guard_blocks_when_there_is_no_ask():
    assert call_guard(best_ask_tick=bf.INVALID_MAX) is False
    assert call_reason(best_ask_tick=bf.INVALID_MAX) == bf.GUARD_NO_ASK


def test_guard_blocks_a_locked_book():
    assert call_guard(best_ask_tick=600_000) is False
    assert call_reason(best_ask_tick=600_000) == bf.GUARD_CROSSED_OR_LOCKED


def test_guard_blocks_a_crossed_book():
    assert call_reason(best_bid_tick=600_002) == bf.GUARD_CROSSED_OR_LOCKED


def test_guard_accepts_a_book_exactly_at_the_age_bound():
    # The signal moves with `now` so that only the book age is under test.
    now = T0 + 1500 * MS
    assert call_guard(now_ts=now, last_signal_ts=now) is True


def test_guard_blocks_a_book_one_nanosecond_over_the_bound():
    """Float64 cannot tell these two apart at 1.8e18; int64 must."""
    now = T0 + 1500 * MS + 1
    assert call_guard(now_ts=now, last_signal_ts=now) is False
    assert call_reason(now_ts=now, last_signal_ts=now) == bf.GUARD_BOOK_STALE


def test_guard_blocks_when_the_book_was_never_observed():
    assert call_reason(last_book_change_ts=0) == bf.GUARD_BOOK_STALE


def test_guard_blocks_a_book_change_timestamped_in_the_future():
    assert call_reason(last_book_change_ts=T0 + 600 * MS) == bf.GUARD_BOOK_STALE


def test_guard_accepts_a_signal_exactly_at_the_age_bound():
    assert call_guard(last_signal_ts=T0 + 500 * MS - 1000 * MS) is True


def test_guard_blocks_a_signal_one_nanosecond_over_the_bound():
    now = T0 + 500 * MS
    assert call_guard(now_ts=now, last_signal_ts=now - 1000 * MS - 1) is False
    assert call_reason(now_ts=now, last_signal_ts=now - 1000 * MS - 1) == bf.GUARD_SIGNAL_STALE


def test_guard_blocks_an_absent_signal_through_the_same_path_as_a_stale_one():
    """Doc, mode A: 'signal absent => do not trade; the same code path'."""
    assert call_reason(last_signal_ts=bf.SIGNAL_TS_ABSENT) == bf.GUARD_SIGNAL_STALE


def test_guard_blocks_a_signal_timestamped_in_the_future():
    assert call_reason(last_signal_ts=T0 + 600 * MS) == bf.GUARD_SIGNAL_STALE


def test_guard_is_nanosecond_exact_at_the_epoch_scale():
    """max_age of 1 ns: ages of 1 and 2 ns must be distinguishable."""
    now = 1_800_000_000_000_000_002
    assert call_guard(now_ts=now, last_book_change_ts=now - 1, max_book_age_ns=1,
                      last_signal_ts=now, max_signal_age_ns=1) is True
    assert call_guard(now_ts=now, last_book_change_ts=now - 2, max_book_age_ns=1,
                      last_signal_ts=now, max_signal_age_ns=1) is False


def test_guard_reason_names_cover_every_code():
    for code in (bf.GUARD_OK, bf.GUARD_NO_BID, bf.GUARD_NO_ASK,
                 bf.GUARD_CROSSED_OR_LOCKED, bf.GUARD_BOOK_STALE, bf.GUARD_SIGNAL_STALE):
        assert isinstance(bf.GUARD_REASON_NAMES[code], str)
    assert len(bf.GUARD_REASON_NAMES) == 6


# ---------------------------------------------------------------------------
# 2. signal lookup — boundaries, ties, staleness, lag
# ---------------------------------------------------------------------------

def test_signal_lookup_takes_the_last_row_at_or_before_now():
    ts = np.array([T0, T0 + 100 * MS, T0 + 200 * MS], dtype=np.int64)
    assert bf.signal_index(ts, T0 + 150 * MS, 0) == 1


def test_signal_lookup_includes_the_exact_boundary():
    ts = np.array([T0, T0 + 100 * MS], dtype=np.int64)
    assert bf.signal_index(ts, T0 + 100 * MS, 0) == 1


def test_signal_lookup_resolves_ties_to_the_last_row():
    ts = np.array([T0, T0, T0], dtype=np.int64)
    assert bf.signal_index(ts, T0, 0) == 2


def test_signal_lookup_is_absent_before_the_first_row():
    ts = np.array([T0, T0 + 100 * MS], dtype=np.int64)
    assert bf.signal_index(ts, T0 - 1, 0) == bf.SIGNAL_INDEX_ABSENT
    assert bf.SIGNAL_INDEX_ABSENT == -1


def test_signal_lookup_on_an_empty_array_is_absent():
    ts = np.array([], dtype=np.int64)
    assert bf.signal_index(ts, T0, 0) == bf.SIGNAL_INDEX_ABSENT


def test_signal_lookup_still_returns_the_last_row_when_it_is_stale():
    """Staleness is the guard's job; the lookup itself just reports the row."""
    ts = np.array([T0], dtype=np.int64)
    assert bf.signal_index(ts, T0 + 3600 * S, 0) == 0


def test_signal_lookup_lag_shifts_the_comparison_back():
    ts = np.array([T0, T0 + 100 * MS], dtype=np.int64)
    now = T0 + 100 * MS
    assert bf.signal_index(ts, now, 0) == 1
    assert bf.signal_index(ts, now, 1 * MS) == 0


def test_signal_lookup_lag_can_make_the_signal_absent():
    ts = np.array([T0], dtype=np.int64)
    assert bf.signal_index(ts, T0, 1) == bf.SIGNAL_INDEX_ABSENT


def test_signal_lookup_is_nanosecond_exact():
    ts = np.array([T0, T0 + 1], dtype=np.int64)
    assert bf.signal_index(ts, T0 + 1, 0) == 1
    assert bf.signal_index(ts, T0 + 1, 1) == 0


def test_signal_price_mid_and_microprice():
    assert bf.signal_price(100.0, 1.0, 102.0, 1.0, False) == 101.0
    # microprice leans towards the side with less size on it
    assert bf.signal_price(100.0, 3.0, 102.0, 1.0, True) == pytest.approx(101.5)


def test_signal_price_falls_back_to_mid_on_zero_size():
    assert bf.signal_price(100.0, 0.0, 102.0, 0.0, True) == 101.0


# ---------------------------------------------------------------------------
# 3. HL price normalisation (doc, Phase 4: 'normalise with the live rule')
# ---------------------------------------------------------------------------

def test_normalize_price_rounds_a_bid_down_and_an_ask_up():
    assert bf.normalize_price(60000.5, 1.0, 5, -1, True) == 60000.0
    assert bf.normalize_price(60000.5, 1.0, 5, -1, False) == 60001.0


def test_normalize_price_applies_five_significant_figures():
    assert bf.normalize_price(123.456789, 0.001, 5, -1, True) == pytest.approx(123.45)
    assert bf.normalize_price(123.456789, 0.001, 5, -1, False) == pytest.approx(123.46)


def test_normalize_price_survives_the_log10_edge():
    assert bf.normalize_price(1000.0, 0.01, 5, -1, True) == pytest.approx(1000.0)


def test_normalize_price_caps_decimal_places():
    # HL perps: at most 6 - szDecimals decimals.
    assert bf.normalize_price(1.234567, 0.000001, 5, 2, True) == pytest.approx(1.23)


def test_normalize_price_aligns_to_the_engine_tick():
    assert bf.normalize_price(60000.7, 0.5, 0, -1, True) == pytest.approx(60000.5)


# ---------------------------------------------------------------------------
# 4. latency profiles — explicit, echoed, marked as an assumption
# ---------------------------------------------------------------------------

def test_latency_profiles_are_the_documented_pairs():
    assert bf.latency_profile_ns('low') == (20 * MS, 40 * MS)
    assert bf.latency_profile_ns('base') == (60 * MS, 120 * MS)
    assert bf.latency_profile_ns('high') == (150 * MS, 300 * MS)


def test_latency_profile_rejects_an_unknown_name():
    with pytest.raises(bf.ConfigError):
        bf.latency_profile_ns('medium')


def test_latency_assumption_note_names_the_missing_measurement():
    note = bf.LATENCY_ASSUMPTION.lower()
    assert 'assumption' in note
    assert 'submit' in note and 'ack' in note


# ---------------------------------------------------------------------------
# 5. manifest loading
# ---------------------------------------------------------------------------

def test_manifest_loads_the_phase3_schema(manifest_path):
    m = bf.load_manifest(str(manifest_path))
    assert m.tick_size == 0.1
    assert m.lot_size == 0.00001
    assert m.book_mode == 'fast'
    assert m.num_levels == 5
    assert m.max_signal_age_ns == 1000 * MS
    assert m.max_hl_book_age_ns == 1500 * MS
    assert m.clock_correction_ns == 0
    assert m.hl_symbol == 'BTC'
    assert m.signal_symbol == 'BTCUSDT'
    assert m.sz_decimals == 5
    assert len(m.hl_npz) == 1 and m.hl_npz[0].endswith('.npz')
    assert m.signal_npz.endswith('.npz')
    assert m.initial_snapshot is None


def test_manifest_records_its_own_identity(manifest_path):
    m = bf.load_manifest(str(manifest_path))
    assert m.identity['path'] == str(manifest_path)
    assert len(m.identity['sha256']) == 64
    assert m.identity['sha256'] == bf.sha256_file(str(manifest_path))
    assert m.identity['collector'] == {'hyperliquid': {'commits': ['abc1234']}}


def test_manifest_timestamps_stay_int(manifest_path):
    m = bf.load_manifest(str(manifest_path))
    for v in (m.window_start_ns, m.window_end_ns, m.max_signal_age_ns,
              m.max_hl_book_age_ns, m.clock_correction_ns):
        assert isinstance(v, int) and not isinstance(v, bool)


def test_manifest_missing_required_key_names_it(tmp_path):
    p = make_manifest(tmp_path)
    body = json.loads(p.read_text())
    del body['outputs']['signal']
    p.write_text(json.dumps(body))
    with pytest.raises(bf.ConfigError) as e:
        bf.load_manifest(str(p))
    assert 'signal' in str(e.value)
    assert 'outputs.signal.path' in str(e.value)


def test_manifest_missing_npz_file_is_refused(tmp_path):
    p = make_manifest(tmp_path)
    body = json.loads(p.read_text())
    pathlib.Path(body['outputs']['hl_depth'][0]['path']).unlink()
    with pytest.raises(bf.ConfigError) as e:
        bf.load_manifest(str(p))
    assert 'hl_btc_20260725.npz' in str(e.value)


def test_manifest_accepts_a_flat_hand_written_alias(tmp_path):
    """A hand-written manifest may use the short form; it must resolve."""
    out = tmp_path / 'd'
    out.mkdir()
    hl = write_hl_npz(out / 'hl.npz')
    sig = write_npz_signal(out / 's.npz',
                           np.array([T0], dtype=np.int64),
                           np.zeros((1, 4)))
    body = {
        'hl_npz': [hl],
        'signal_npz': sig,
        'tick_size': 1.0,
        'lot_size': 0.001,
        'window': {'start_ns': T0, 'end_ns': T0 + S},
        'max_signal_age_ns': 500 * MS,
        'max_hl_book_age_ns': 2 * S,
        # The instrument table is never inferred from a file name, so even the
        # short form has to state it (doc, "Режим A").
        'hl_symbol': 'BTC',
        'signal_symbol': 'BTCUSDT',
    }
    p = tmp_path / 'm.json'
    p.write_text(json.dumps(body))
    m = bf.load_manifest(str(p))
    assert m.hl_npz == [hl]
    assert m.signal_npz == sig
    assert (m.hl_symbol, m.signal_symbol) == ('BTC', 'BTCUSDT')
    assert m.resolution['signal_npz'] == 'signal_npz'


def test_manifest_resolution_records_where_each_value_came_from(manifest_path):
    m = bf.load_manifest(str(manifest_path))
    assert m.resolution['tick_size'] == 'tick_size'
    assert m.resolution['signal_npz'] == 'outputs.signal.path'
    assert m.resolution['hl_npz'] == 'outputs.hl_depth[].path'


def test_manifest_picks_the_initial_snapshot_for_the_first_day(tmp_path):
    p = make_manifest(tmp_path)
    body = json.loads(p.read_text())
    snap = write_hl_npz(tmp_path / 'dataset' / 'snap_20260725.npz')
    body['snapshots'] = {'created': [{'for_day': '20260725', 'path': snap}]}
    p.write_text(json.dumps(body))
    m = bf.load_manifest(str(p))
    assert m.initial_snapshot == snap


def test_manifest_window_must_be_ordered(tmp_path):
    p = make_manifest(tmp_path, window={'start_ns': T0 + S, 'end_ns': T0})
    with pytest.raises(bf.ConfigError) as e:
        bf.load_manifest(str(p))
    assert 'window' in str(e.value)


# ---------------------------------------------------------------------------
# 6. signal array loading — the numeric trap lives here
# ---------------------------------------------------------------------------

def test_signal_array_loads_int64_nanoseconds_exactly(tmp_path):
    odd = 1_800_000_000_000_000_001
    p = tmp_path / 's.npz'
    write_npz_signal(p, np.array([odd], dtype=np.int64), np.zeros((1, 4)))
    ts, values = bf.load_signal(str(p))
    assert ts.dtype == np.int64
    assert int(ts[0]) == odd
    assert int(ts[0]) != int(np.float64(odd))  # the trap


def test_signal_array_with_float_timestamps_is_refused(tmp_path):
    p = tmp_path / 's.npz'
    write_npz_signal(p, np.array([float(T0)], dtype=np.float64), np.zeros((1, 4)))
    with pytest.raises(bf.ConfigError) as e:
        bf.load_signal(str(p))
    assert 'int64' in str(e.value)


def test_signal_array_out_of_order_is_refused(tmp_path):
    p = tmp_path / 's.npz'
    write_npz_signal(p, np.array([T0 + S, T0], dtype=np.int64), np.zeros((2, 4)))
    with pytest.raises(bf.ConfigError) as e:
        bf.load_signal(str(p))
    assert 'sorted' in str(e.value).lower()


def test_signal_array_wrong_column_count_is_refused(tmp_path):
    p = tmp_path / 's.npz'
    write_npz_signal(p, np.array([T0], dtype=np.int64), np.zeros((1, 3)))
    with pytest.raises(bf.ConfigError) as e:
        bf.load_signal(str(p))
    assert '(N, 4)' in str(e.value)


def test_signal_array_length_mismatch_is_refused(tmp_path):
    p = tmp_path / 's.npz'
    write_npz_signal(p, np.array([T0, T0 + S], dtype=np.int64), np.zeros((1, 4)))
    with pytest.raises(bf.ConfigError):
        bf.load_signal(str(p))


def test_signal_array_rejects_the_absent_sentinel_as_a_real_timestamp(tmp_path):
    p = tmp_path / 's.npz'
    write_npz_signal(p, np.array([0], dtype=np.int64), np.zeros((1, 4)))
    with pytest.raises(bf.ConfigError):
        bf.load_signal(str(p))


# ---------------------------------------------------------------------------
# 7. CLI / config resolution — no silent defaults
# ---------------------------------------------------------------------------

def test_cli_accepts_a_negative_maker_fee(manifest_path):
    args = bf.build_parser().parse_args(
        ['--manifest', str(manifest_path), '--maker-fee', '-0.00003'])
    assert args.maker_fee == pytest.approx(-0.00003)


def test_exchange_model_is_an_explicit_choice(manifest_path):
    """AGENTS.md §4.6 — banned before the partial-fill fix, a choice after it.

    The default stays no-partial so runs made before the fix stay comparable,
    and whichever is used carries its provenance.
    """
    parser = bf.build_parser()
    with pytest.raises(SystemExit):
        parser.parse_args(['--manifest', 'x', '--exchange', 'both'])

    cfg = _cfg(manifest_path)
    assert cfg['exchange'] == 'no-partial'
    assert cfg['exchange_kind'] == 'NoPartialFillExchange'

    cfg = _cfg(manifest_path, '--exchange', 'partial')
    assert cfg['exchange'] == 'partial'
    assert cfg['exchange_kind'] == 'PartialFillExchange'
    assert cfg['sources']['exchange'] == 'cli'


def test_config_defaults_are_recorded_with_their_source(manifest_path):
    cfg = bf.resolve_config(
        bf.build_parser().parse_args(['--manifest', str(manifest_path)]),
        bf.load_manifest(str(manifest_path)))
    assert cfg['maker_fee'] == pytest.approx(0.00015)
    assert cfg['sources']['maker_fee'] == 'manifest'
    assert cfg['max_signal_age_ns'] == 1000 * MS
    assert cfg['sources']['max_signal_age_ns'] == 'manifest'
    assert cfg['tick_size'] == 0.1
    assert cfg['sources']['tick_size'] == 'manifest'
    assert cfg['latency_profile'] == 'base'
    assert cfg['entry_ns'] == 60 * MS and cfg['resp_ns'] == 120 * MS
    assert cfg['sources']['order_latency'] == 'default'
    assert cfg['elapse_ns'] == 100 * MS


def test_config_cli_overrides_the_manifest(manifest_path):
    cfg = bf.resolve_config(
        bf.build_parser().parse_args([
            '--manifest', str(manifest_path),
            '--maker-fee', '-0.0001', '--taker-fee', '0.0005',
            '--max-signal-age-ms', '250', '--elapse-ms', '50',
            '--signal-lag-ms', '10',
        ]),
        bf.load_manifest(str(manifest_path)))
    assert cfg['maker_fee'] == pytest.approx(-0.0001)
    assert cfg['sources']['maker_fee'] == 'cli'
    assert cfg['max_signal_age_ns'] == 250 * MS
    assert cfg['sources']['max_signal_age_ns'] == 'cli'
    assert cfg['elapse_ns'] == 50 * MS
    assert cfg['signal_lag_ns'] == 10 * MS


def test_config_explicit_latency_pair_overrides_the_profile(manifest_path):
    cfg = bf.resolve_config(
        bf.build_parser().parse_args([
            '--manifest', str(manifest_path), '--entry-ms', '5', '--resp-ms', '9']),
        bf.load_manifest(str(manifest_path)))
    assert (cfg['entry_ns'], cfg['resp_ns']) == (5 * MS, 9 * MS)
    assert cfg['latency_profile'] is None
    assert cfg['sources']['order_latency'] == 'cli'


def test_config_refuses_a_profile_and_an_explicit_pair_together(manifest_path):
    with pytest.raises(bf.ConfigError):
        bf.resolve_config(
            bf.build_parser().parse_args([
                '--manifest', str(manifest_path),
                '--latency-profile', 'high', '--entry-ms', '5', '--resp-ms', '9']),
            bf.load_manifest(str(manifest_path)))


def test_config_refuses_half_a_latency_pair(manifest_path):
    with pytest.raises(bf.ConfigError):
        bf.resolve_config(
            bf.build_parser().parse_args(
                ['--manifest', str(manifest_path), '--entry-ms', '5']),
            bf.load_manifest(str(manifest_path)))


def test_config_refuses_a_lag_that_exceeds_the_signal_age_budget(manifest_path):
    """Fail closed: every step would block, and the run would look like a bug."""
    with pytest.raises(bf.ConfigError) as e:
        bf.resolve_config(
            bf.build_parser().parse_args([
                '--manifest', str(manifest_path), '--signal-lag-ms', '1000']),
            bf.load_manifest(str(manifest_path)))
    assert 'max_signal_age' in str(e.value)


def test_config_carries_the_hl_decimal_rule_from_sz_decimals(manifest_path):
    cfg = bf.resolve_config(
        bf.build_parser().parse_args(['--manifest', str(manifest_path)]),
        bf.load_manifest(str(manifest_path)))
    assert cfg['hl_price_sig_figs'] == 5
    assert cfg['hl_max_decimals'] == 1  # 6 - szDecimals(5)


def test_the_measured_tick_lot_source_is_still_readable(manifest_path):
    """The shape `build_dataset.py` writes since the tick became measured.

    Built by the real function rather than hand-written, so this cannot go on
    passing after the writer moves `sz_decimals`: the manifest is a contract
    between two tools and only one of them is exercised here otherwise. The
    older `{'kind': 'hl_universe', ...}` shape stays covered by the fixture
    above — manifests carrying it are on disk and must keep loading.
    """
    import build_dataset as bd

    grid = bd.PriceGrid()
    for px in ('0.39040', '0.41209', '0.39875'):
        grid.observe(px)
    _tick, _lot, source = bd.resolve_tick_lot(
        grid, cli_tick=None, cli_lot=None,
        hl_meta=[(0, {'_collector': 'universe',
                      'symbols': [{'wire': 'ONDO', 'szDecimals': 0}]})],
        hl_meta_files=[], hl_dir=pathlib.Path('.'), symbol='ONDO')

    raw = json.loads(manifest_path.read_text())
    raw['tick_lot_source'] = source
    raw['tick_size'] = _tick
    manifest_path.write_text(json.dumps(raw))

    manifest = bf.load_manifest(str(manifest_path))
    assert manifest.tick_size == 1e-05
    assert manifest.sz_decimals == 0
    cfg = bf.resolve_config(
        bf.build_parser().parse_args(['--manifest', str(manifest_path)]), manifest)
    assert cfg['hl_max_decimals'] == 6  # 6 - szDecimals(0)


def test_config_grid_qty_defaults_to_one_lot(manifest_path):
    cfg = bf.resolve_config(
        bf.build_parser().parse_args(
            ['--manifest', str(manifest_path), '--strategy', 'grid']),
        bf.load_manifest(str(manifest_path)))
    assert cfg['strategy'] == 'grid'
    assert cfg['grid']['order_qty'] == 0.00001
    assert cfg['grid']['levels'] >= 1


# ---------------------------------------------------------------------------
# 8. results JSON — every model and parameter echoed
# ---------------------------------------------------------------------------

ECHOED = [
    ('models.backtest', 'HashMapMarketDepthBacktest'),
    ('models.asset_type.kind', 'LinearAsset'),
    ('models.asset_type.contract_size', 1.0),
    ('models.fee_model.kind', 'TradingValueFeeModel'),
    ('models.queue_model.kind', 'LogProbQueueModel2'),
    ('models.exchange_kind.kind', 'NoPartialFillExchange'),
    ('models.order_latency.kind', 'ConstantLatency'),
    ('models.latency_offset', 0),
]


def _dig(d, path):
    cur = d
    for part in path.split('.'):
        cur = cur[part]
    return cur


def test_results_echo_every_model_choice(manifest_path):
    m = bf.load_manifest(str(manifest_path))
    cfg = bf.resolve_config(
        bf.build_parser().parse_args(['--manifest', str(manifest_path)]), m)
    res = bf.build_results(cfg, m, fake_stats())
    for path, expected in ECHOED:
        assert _dig(res, path) == expected, path
    assert _dig(res, 'models.fee_model.maker_fee') == cfg['maker_fee']
    assert _dig(res, 'models.fee_model.taker_fee') == cfg['taker_fee']
    assert _dig(res, 'models.order_latency.entry_ns') == cfg['entry_ns']
    assert _dig(res, 'models.order_latency.resp_ns') == cfg['resp_ns']
    assert _dig(res, 'models.tick_size') == cfg['tick_size']
    assert _dig(res, 'models.lot_size') == cfg['lot_size']
    assert _dig(res, 'models.clock_correction_ns') == m.clock_correction_ns


def test_results_record_the_exchange_choice_and_the_partial_fill_fix(manifest_path):
    m = bf.load_manifest(str(manifest_path))
    cfg = bf.resolve_config(
        bf.build_parser().parse_args(['--manifest', str(manifest_path)]), m)
    res = bf.build_results(cfg, m, fake_stats())
    block = res['models']['exchange_kind']
    assert block['kind'] == 'NoPartialFillExchange'
    assert block['choice'] == 'no-partial'
    assert block['source'] == cfg['sources']['exchange']
    note = json.dumps(block)
    # The history is load-bearing: a reader of an old results file must be able
    # to tell whether partial fills counted.
    assert 'PartialFillExchange' in note
    assert '4.6' in note


def test_results_record_the_gap_partial_fill_exchange_still_has(manifest_path):
    m = bf.load_manifest(str(manifest_path))
    cfg = bf.resolve_config(
        bf.build_parser().parse_args(
            ['--manifest', str(manifest_path), '--exchange', 'partial']), m)
    res = bf.build_results(cfg, m, fake_stats())
    block = res['models']['exchange_kind']
    assert block['kind'] == 'PartialFillExchange'
    assert block['choice'] == 'partial'
    assert block['source'] == 'cli'
    assert 'IOC' in block['accepted_distortion']


def test_results_carry_the_latency_assumption(manifest_path):
    m = bf.load_manifest(str(manifest_path))
    cfg = bf.resolve_config(
        bf.build_parser().parse_args(['--manifest', str(manifest_path)]), m)
    res = bf.build_results(cfg, m, fake_stats())
    assert res['models']['order_latency']['assumption'] == bf.LATENCY_ASSUMPTION


def test_results_echo_every_parameter(manifest_path):
    m = bf.load_manifest(str(manifest_path))
    cfg = bf.resolve_config(
        bf.build_parser().parse_args(
            ['--manifest', str(manifest_path), '--signal-lag-ms', '10',
             '--elapse-ms', '250', '--strategy', 'grid']), m)
    res = bf.build_results(cfg, m, fake_stats())
    p = res['params']
    assert p['strategy'] == 'grid'
    assert p['signal_lag_ms'] == 10 and p['signal_lag_ns'] == 10 * MS
    assert p['elapse_ms'] == 250 and p['elapse_ns'] == 250 * MS
    assert p['max_signal_age_ns'] == m.max_signal_age_ns
    assert p['max_book_age_ns'] == m.max_hl_book_age_ns
    assert p['window_start_ns'] == m.window_start_ns
    assert p['window_end_ns'] == m.window_end_ns
    assert p['grid'] == cfg['grid']
    assert res['sources'] == cfg['sources']


def test_results_carry_the_manifest_identity(manifest_path):
    m = bf.load_manifest(str(manifest_path))
    cfg = bf.resolve_config(
        bf.build_parser().parse_args(['--manifest', str(manifest_path)]), m)
    res = bf.build_results(cfg, m, fake_stats())
    assert res['manifest']['path'] == str(manifest_path)
    assert res['manifest']['sha256'] == bf.sha256_file(str(manifest_path))
    assert res['manifest']['resolution']['tick_size'] == 'tick_size'


def test_results_carry_the_run_statistics(manifest_path):
    m = bf.load_manifest(str(manifest_path))
    cfg = bf.resolve_config(
        bf.build_parser().parse_args(['--manifest', str(manifest_path)]), m)
    stats = fake_stats()
    res = bf.build_results(cfg, m, stats)
    run = res['run']
    assert run['steps'] == 100
    assert run['guard_passed'] == 60
    assert run['guard_blocked'] == stats.guard_blocked
    assert run['guard_blocked_total'] == 40
    assert run['signal_reads'] == 90
    assert run['signal_absent'] == 7
    assert run['fills'] == 3
    assert run['final_position'] == 0.5
    assert run['equity'] == 123.45
    assert run['elapsed_wall_s'] == 1.25
    assert run['end_reason'] == 'end_of_data'
    assert run['first_ts'] == T0 and run['last_ts'] == T0 + 10 * S


def test_results_are_json_serialisable_without_numpy_scalars(manifest_path):
    m = bf.load_manifest(str(manifest_path))
    cfg = bf.resolve_config(
        bf.build_parser().parse_args(['--manifest', str(manifest_path)]), m)
    stats = fake_stats(steps=np.int64(7), final_position=np.float64(0.25),
                       first_ts=np.int64(T0))
    res = bf.build_results(cfg, m, stats)
    text = json.dumps(res)  # raises TypeError on a leaked numpy scalar
    assert json.loads(text)['run']['steps'] == 7
    assert json.loads(text)['run']['first_ts'] == T0


def test_results_timestamps_round_trip_exactly(manifest_path):
    """A float64 anywhere in the JSON path would shift these by ~256 ns."""
    odd = 1_800_000_000_000_000_001
    m = bf.load_manifest(str(manifest_path))
    cfg = bf.resolve_config(
        bf.build_parser().parse_args(['--manifest', str(manifest_path)]), m)
    res = bf.build_results(cfg, m, fake_stats(first_ts=odd, last_ts=odd + 1))
    back = json.loads(json.dumps(res))
    assert back['run']['first_ts'] == odd
    assert back['run']['last_ts'] == odd + 1


# ---------------------------------------------------------------------------
# 9. run_once with an injected runner (no native module involved)
# ---------------------------------------------------------------------------

def test_run_once_uses_the_injected_runner_and_writes_the_results(tmp_path, manifest_path):
    seen = {}

    def runner(config, manifest, signal):
        seen['config'] = config
        seen['rows'] = len(signal[0])
        return fake_stats()

    out = tmp_path / 'results.json'
    res = bf.run_once(str(manifest_path), ['--out', str(out)], runner=runner)
    assert seen['rows'] == 20
    assert seen['config']['strategy'] == 'noop'
    assert out.exists()
    assert json.loads(out.read_text())['run']['steps'] == 100
    assert res['run']['steps'] == 100


# ---------------------------------------------------------------------------
# 10. sweep — exactly one knob moves; zero difference is a result
# ---------------------------------------------------------------------------

def test_parse_sweep_spec():
    assert bf.parse_sweep_spec('signal-lag=0,10,50') == ('signal-lag', [0, 10, 50])
    assert bf.parse_sweep_spec('maker-fee=0,-0.00003')[1] == [0.0, -0.00003]


def test_parse_sweep_spec_rejects_an_unknown_knob():
    with pytest.raises(bf.ConfigError) as e:
        bf.parse_sweep_spec('alpha=1,2')
    assert 'signal-lag' in str(e.value)


def test_parse_sweep_spec_rejects_a_malformed_spec():
    with pytest.raises(bf.ConfigError):
        bf.parse_sweep_spec('signal-lag')
    with pytest.raises(bf.ConfigError):
        bf.parse_sweep_spec('signal-lag=')


def test_sweep_varies_exactly_one_knob(manifest_path):
    m = bf.load_manifest(str(manifest_path))
    base = bf.resolve_config(
        bf.build_parser().parse_args(['--manifest', str(manifest_path)]), m)
    configs = bf.sweep_configs(base, 'signal-lag', [0, 10, 50])
    assert len(configs) == 3
    owned = set(bf.SWEEP_KNOBS['signal-lag'])
    for other in configs[1:]:
        diff = {k for k in base if json.dumps(configs[0][k], sort_keys=True)
                != json.dumps(other[k], sort_keys=True)}
        assert diff <= owned, diff
        assert diff  # something did move
    assert [c['signal_lag_ns'] for c in configs] == [0, 10 * MS, 50 * MS]


def test_sweep_over_a_latency_profile_moves_only_its_own_fields(manifest_path):
    m = bf.load_manifest(str(manifest_path))
    base = bf.resolve_config(
        bf.build_parser().parse_args(['--manifest', str(manifest_path)]), m)
    configs = bf.sweep_configs(base, 'latency-profile', ['low', 'base', 'high'])
    owned = set(bf.SWEEP_KNOBS['latency-profile'])
    diff = {k for k in base if configs[0][k] != configs[2][k]}
    assert diff <= owned
    assert configs[0]['entry_ns'] == 20 * MS
    assert configs[2]['resp_ns'] == 300 * MS


class _RecordingAsset:
    """Only the two exchange builders exist: a wrong name raises AttributeError."""

    def __init__(self):
        self.calls = []

    def no_partial_fill_exchange(self):
        self.calls.append('no_partial_fill_exchange')
        return self

    def partial_fill_exchange(self):
        self.calls.append('partial_fill_exchange')
        return self


@pytest.mark.parametrize('exchange, expected', [
    ('no-partial', 'no_partial_fill_exchange'),
    ('partial', 'partial_fill_exchange'),
])
def test_the_chosen_exchange_model_is_the_one_built(exchange, expected):
    asset = _RecordingAsset()
    bf.apply_exchange_model(asset, exchange)
    assert asset.calls == [expected]


def test_sweep_over_the_exchange_model_moves_only_its_own_fields(manifest_path):
    """The distortion note's own question, now answerable on one dataset."""
    m = bf.load_manifest(str(manifest_path))
    base = bf.resolve_config(
        bf.build_parser().parse_args(['--manifest', str(manifest_path)]), m)
    configs = bf.sweep_configs(base, 'exchange', ['no-partial', 'partial'])
    owned = set(bf.SWEEP_KNOBS['exchange'])
    diff = {k for k in base if configs[0][k] != configs[1][k]}
    assert diff <= owned, diff
    assert [c['exchange_kind'] for c in configs] == [
        'NoPartialFillExchange', 'PartialFillExchange']
    assert configs[1]['sources']['exchange'] == 'sweep'


def test_sweep_rejects_an_unknown_exchange_model(manifest_path):
    m = bf.load_manifest(str(manifest_path))
    base = bf.resolve_config(
        bf.build_parser().parse_args(['--manifest', str(manifest_path)]), m)
    with pytest.raises(bf.ConfigError):
        bf.sweep_configs(base, 'exchange', ['no-partial', 'l3'])


def test_sweep_marks_its_provenance(manifest_path):
    m = bf.load_manifest(str(manifest_path))
    base = bf.resolve_config(
        bf.build_parser().parse_args(['--manifest', str(manifest_path)]), m)
    configs = bf.sweep_configs(base, 'signal-lag', [0, 10])
    assert configs[1]['sources']['signal_lag_ms'] == 'sweep'
    assert configs[0]['sources']['signal_lag_ms'] == 'sweep'


def test_sweep_rejects_a_lag_value_over_the_age_budget(manifest_path):
    m = bf.load_manifest(str(manifest_path))
    base = bf.resolve_config(
        bf.build_parser().parse_args(['--manifest', str(manifest_path)]), m)
    with pytest.raises(bf.ConfigError):
        bf.sweep_configs(base, 'signal-lag', [0, 5000])


def test_sweep_output_paths_are_one_per_run(tmp_path):
    out = tmp_path / 'results.json'
    assert bf.sweep_output_path(str(out), 'signal-lag', 10) == \
        str(tmp_path / 'results__signal-lag=10.json')
    assert bf.sweep_output_path(str(out), 'maker-fee', -3e-05).endswith(
        'results__maker-fee=-3e-05.json')


def test_sweep_summary_reports_no_difference_as_a_result():
    runs = [
        {'value': 0, 'run': {'fills': 3, 'final_position': 0.5, 'equity': 1.0,
                             'guard_passed': 60, 'steps': 100}},
        {'value': 10, 'run': {'fills': 3, 'final_position': 0.5, 'equity': 1.0,
                              'guard_passed': 60, 'steps': 100}},
    ]
    summary = bf.summarize_sweep('signal-lag', runs)
    assert summary['identical'] is True
    table = bf.format_sweep_table(summary)
    assert 'NO DIFFERENCE' in table
    assert 'valid result' in table.lower()


def test_sweep_summary_treats_float_noise_as_no_difference():
    """Measured: two runs of an identical config differ in the 15th digit of
    balance (Rust HashMap iteration order changes the summation order), so a
    bitwise comparison would report a sensitivity that does not exist."""
    runs = [
        {'value': 0, 'run': {'fills': 3, 'equity': 5.372953000002757}},
        {'value': 10, 'run': {'fills': 3, 'equity': 5.372953000003666}},
    ]
    summary = bf.summarize_sweep('signal-lag', runs)
    assert summary['identical'] is True
    assert summary['moved'] == []
    assert summary['near_identical'] == ['equity']
    assert summary['float_rel_tol'] == bf.SWEEP_FLOAT_REL_TOL
    table = bf.format_sweep_table(summary)
    assert 'NO DIFFERENCE' in table
    assert 'equity' in table and 'tolerance' in table.lower()


def test_sweep_summary_reports_a_float_move_above_the_tolerance():
    runs = [
        {'value': 0, 'run': {'equity': 5.0}},
        {'value': 10, 'run': {'equity': 5.1}},
    ]
    summary = bf.summarize_sweep('signal-lag', runs)
    assert summary['identical'] is False
    assert summary['moved'] == ['equity']
    assert summary['near_identical'] == []


def test_sweep_summary_reports_a_difference():
    runs = [
        {'value': 0, 'run': {'fills': 3, 'final_position': 0.5, 'equity': 1.0,
                             'guard_passed': 60, 'steps': 100}},
        {'value': 10, 'run': {'fills': 4, 'final_position': 0.5, 'equity': 2.0,
                              'guard_passed': 55, 'steps': 100}},
    ]
    summary = bf.summarize_sweep('signal-lag', runs)
    assert summary['identical'] is False
    assert set(summary['moved']) >= {'fills', 'equity', 'guard_passed'}
    table = bf.format_sweep_table(summary)
    assert 'NO DIFFERENCE' not in table
    assert '10' in table


def test_sweep_run_writes_one_file_per_run_plus_a_summary(tmp_path, manifest_path):
    calls = []

    def runner(config, manifest, signal):
        calls.append(config['signal_lag_ns'])
        return fake_stats(fills=len(calls))

    out = tmp_path / 'r.json'
    summary = bf.run_sweep(str(manifest_path),
                           ['--out', str(out), '--sweep', 'signal-lag=0,10,50'],
                           runner=runner)
    assert calls == [0, 10 * MS, 50 * MS]
    for v in (0, 10, 50):
        assert pathlib.Path(bf.sweep_output_path(str(out), 'signal-lag', v)).exists()
    assert (tmp_path / 'r__sweep-summary.json').exists()
    assert summary['knob'] == 'signal-lag'
    assert summary['identical'] is False


def test_sweep_requires_an_output_path(manifest_path):
    with pytest.raises(bf.ConfigError):
        bf.run_sweep(str(manifest_path), ['--sweep', 'signal-lag=0,10'],
                     runner=lambda *a: fake_stats())


# ---------------------------------------------------------------------------
# 11. module hygiene
# ---------------------------------------------------------------------------

def test_module_import_does_not_pull_in_the_native_module_or_numba():
    """Requirement: hftbacktest is imported inside main(), not at module top."""
    code = (
        'import sys; sys.path.insert(0, %r); import backtest_first; '
        'assert "hftbacktest" not in sys.modules, "hftbacktest imported at top level"; '
        'assert "numba" not in sys.modules, "numba imported at top level"; '
        'print("ok")' % str(pathlib.Path(bf.__file__).resolve().parent)
    )
    r = subprocess.run([sys.executable, '-c', code], capture_output=True, text=True)
    assert r.returncode == 0, r.stderr
    assert 'ok' in r.stdout


def test_module_docstring_names_the_phase_and_the_doc():
    doc = bf.__doc__
    assert 'design-multi-venue-collection.md' in doc
    assert 'Phase 4' in doc or 'Фаза 4' in doc


# ---------------------------------------------------------------------------
# 12. end to end against the real engine (skipped without the native module)
# ---------------------------------------------------------------------------

def _hl_l2_line(local_ts, exch_ms, mid_ticks, tick=0.1, levels=5):
    bids = [{'px': f'{(mid_ticks - 1 - i) * tick:.1f}', 'sz': '1.5', 'n': 1}
            for i in range(levels)]
    asks = [{'px': f'{(mid_ticks + 1 + i) * tick:.1f}', 'sz': '1.5', 'n': 1}
            for i in range(levels)]
    msg = {'channel': 'l2Book',
           'data': {'coin': 'BTC', 'time': exch_ms, 'fast': True,
                    'levels': [bids, asks]}}
    return f'{local_ts} {json.dumps(msg)}'


@pytest.fixture(scope='module')
def real_dataset(tmp_path_factory):
    hyperliquid = pytest.importorskip('hftbacktest.data.utils.hyperliquid')
    d = tmp_path_factory.mktemp('e2e')
    gz = d / 'btc_20260725.gz'
    lines = []
    start_ms = T0 // MS
    for i in range(400):
        exch_ms = start_ms + i * 100
        local_ts = exch_ms * MS + 274 * MS
        mid = 600_000 + (i % 7)
        lines.append(_hl_l2_line(local_ts, exch_ms, mid))
    with gzip.open(gz, 'wt') as f:
        f.write('\n'.join(lines) + '\n')

    npz = d / 'hl_btc_20260725.npz'
    hyperliquid.convert(str(gz), tick_size=0.1, lot_size=0.00001, num_levels=5,
                        book_mode='fast', output_filename=str(npz),
                        buffer_size=200_000)

    sig_n = 300
    sig_ts = np.array([T0 + 274 * MS + i * (100 * MS) for i in range(sig_n)],
                      dtype=np.int64)
    sig_val = np.zeros((sig_n, 4), dtype=np.float64)
    sig_val[:, 0] = 59999.9
    sig_val[:, 1] = 1.0
    sig_val[:, 2] = 60000.1
    sig_val[:, 3] = 1.0
    sig_npz = d / 'signal.npz'
    np.savez(sig_npz, ts=sig_ts, values=sig_val)

    manifest = d / 'manifest.json'
    manifest.write_text(json.dumps({
        'schema': 'hftbacktest/multi-venue-dataset/1',
        'instruments': {'hyperliquid': {'symbol': 'BTC'},
                        'binancefuturesum': {'symbol': 'BTCUSDT'}},
        'book_mode': 'fast',
        'num_levels': 5,
        'tick_size': 0.1,
        'lot_size': 0.00001,
        'tick_lot_source': {'kind': 'hl_universe', 'sz_decimals': 5},
        'window': {'start_ns': int(sig_ts[0]), 'end_ns': int(sig_ts[-1])},
        'clock_correction_ns': 0,
        'max_signal_age_ns': 1000 * MS,
        'max_hl_book_age_ns': 2000 * MS,
        'outputs': {'hl_depth': [{'path': str(npz), 'day': '20260725'}],
                    'signal': {'path': str(sig_npz)}},
        'snapshots': {'created': [], 'initial_snapshot': None,
                      'note': 'first day'},
        'backtest_defaults': {
            'fee_model': {'kind': 'TradingValueFeeModel',
                          'maker_fee': 0.00015, 'taker_fee': 0.00045},
            'queue_model': {'kind': 'LogProbQueueModel2'},
            'exchange_kind': {'kind': 'NoPartialFillExchange'},
            'constant_latency_ns': {'entry': None, 'response': None},
        },
    }))
    return {'dir': d, 'manifest': manifest}


def test_e2e_noop_runs_the_real_engine(real_dataset):
    pytest.importorskip('hftbacktest')
    out = real_dataset['dir'] / 'noop.json'
    res = bf.run_once(str(real_dataset['manifest']),
                      ['--out', str(out), '--strategy', 'noop'])
    run = res['run']
    assert run['steps'] > 10
    assert run['signal_reads'] > 0
    assert run['book_changes'] > 0
    assert run['guard_passed'] > 0, 'the guard never opened on a healthy book'
    assert run['fills'] == 0 and run['submits'] == 0
    assert sum(run['guard_blocked'].values()) + run['guard_passed'] \
        + run['steps_outside_window'] == run['steps']
    assert out.exists()


def test_e2e_grid_trades_and_reports_pnl(real_dataset):
    pytest.importorskip('hftbacktest')
    out = real_dataset['dir'] / 'grid.json'
    res = bf.run_once(str(real_dataset['manifest']),
                      ['--out', str(out), '--strategy', 'grid',
                       '--grid-levels', '2', '--grid-order-qty', '0.01'])
    run = res['run']
    assert run['submits'] > 0
    assert isinstance(run['equity'], float)
    assert res['models']['exchange_kind']['kind'] == 'NoPartialFillExchange'


def test_e2e_a_tight_signal_bound_blocks_and_is_counted_by_reason(real_dataset):
    """Wiring check: a blocked step lands in the right counter in the real loop."""
    pytest.importorskip('hftbacktest')
    out = real_dataset['dir'] / 'blocked.json'
    res = bf.run_once(str(real_dataset['manifest']),
                      ['--out', str(out), '--strategy', 'noop',
                       '--max-signal-age-ms', '5'])
    run = res['run']
    assert run['guard_blocked']['signal_stale'] > 0
    assert run['guard_blocked']['no_bid'] + run['guard_blocked']['no_ask'] \
        + run['guard_blocked']['crossed_or_locked'] == 0
    assert sum(run['guard_blocked'].values()) + run['guard_passed'] \
        + run['steps_outside_window'] == run['steps']


def test_e2e_cli_sweep_writes_a_file_per_run_and_a_table(real_dataset):
    pytest.importorskip('hftbacktest')
    out = real_dataset['dir'] / 'sweep' / 'r.json'
    r = subprocess.run(
        [sys.executable, str(pathlib.Path(bf.__file__).resolve()),
         '--manifest', str(real_dataset['manifest']),
         '--strategy', 'noop', '--max-steps', '60',
         '--sweep', 'signal-lag=0,10', '--out', str(out)],
        capture_output=True, text=True)
    assert r.returncode == 0, r.stderr
    assert 'sweep: signal-lag' in r.stdout
    for v in (0, 10):
        assert pathlib.Path(bf.sweep_output_path(str(out), 'signal-lag', v)).exists()
    summary = json.loads(pathlib.Path(bf.sweep_summary_path(str(out))).read_text())
    assert summary['values'] == [0, 10]
    assert isinstance(summary['identical'], bool)
    # Whichever way it came out, the table says so in words.
    assert ('NO DIFFERENCE' in r.stdout) == summary['identical']


# ---------------------------------------------------------------------------
# review fixes: the manifest's own model block must be readable
# ---------------------------------------------------------------------------

def _cfg(manifest_path, *extra):
    args = bf.build_parser().parse_args(['--manifest', str(manifest_path), *extra])
    return bf.resolve_config(args, bf.load_manifest(str(manifest_path)))


def test_manifest_declared_latency_is_read_in_the_shape_phase_3_writes(tmp_path):
    """Phase 3 writes a dict; the reader only accepted a 2-list.

    A manifest filled in exactly as §3.3 requires was therefore ignored, the
    built-in `base` profile ran instead, and `sources.order_latency` said
    `default` — a provenance lie in the one file whose stated purpose is that
    nothing is picked up silently.
    """
    p = make_manifest(tmp_path)
    body = json.loads(p.read_text())
    body['backtest_defaults']['constant_latency_ns'] = {
        'entry': 5 * MS, 'response': 9 * MS}
    p.write_text(json.dumps(body))
    cfg = _cfg(p)
    assert (cfg['entry_ns'], cfg['resp_ns']) == (5 * MS, 9 * MS)
    assert cfg['latency_profile'] is None
    assert cfg['sources']['order_latency'] == 'manifest'


def test_manifest_declared_latency_accepts_the_pair_form(tmp_path):
    p = make_manifest(tmp_path)
    body = json.loads(p.read_text())
    body['backtest_defaults']['constant_latency_ns'] = [5 * MS, 9 * MS]
    p.write_text(json.dumps(body))
    cfg = _cfg(p)
    assert (cfg['entry_ns'], cfg['resp_ns']) == (5 * MS, 9 * MS)
    assert cfg['sources']['order_latency'] == 'manifest'


def test_cli_latency_still_beats_a_manifest_declaration(tmp_path):
    p = make_manifest(tmp_path)
    body = json.loads(p.read_text())
    body['backtest_defaults']['constant_latency_ns'] = {
        'entry': 5 * MS, 'response': 9 * MS}
    p.write_text(json.dumps(body))
    cfg = _cfg(p, '--entry-ms', '1', '--resp-ms', '2')
    assert (cfg['entry_ns'], cfg['resp_ns']) == (1 * MS, 2 * MS)
    assert cfg['sources']['order_latency'] == 'cli'


@pytest.mark.parametrize('shape', [
    {'entry': 5 * MS},                     # half a pair
    {'entry': 5 * MS, 'response': 0},      # zero is not a latency
    [5 * MS, 9 * MS, 11 * MS],             # not a pair
    'fast',                                # not a number in sight
])
def test_an_unreadable_latency_declaration_is_refused_not_defaulted(tmp_path, shape):
    """Falling through to a default is what made the old bug invisible."""
    p = make_manifest(tmp_path)
    body = json.loads(p.read_text())
    body['backtest_defaults']['constant_latency_ns'] = shape
    p.write_text(json.dumps(body))
    with pytest.raises(bf.ConfigError) as e:
        _cfg(p)
    assert 'constant_latency_ns' in str(e.value)


def test_fees_are_read_from_the_manifest_fee_model(manifest_path):
    cfg = _cfg(manifest_path)
    assert cfg['maker_fee'] == pytest.approx(0.00015)
    assert cfg['taker_fee'] == pytest.approx(0.00045)
    assert cfg['sources']['maker_fee'] == 'manifest'
    assert cfg['sources']['taker_fee'] == 'manifest'


def test_cli_fees_override_the_manifest(manifest_path):
    cfg = _cfg(manifest_path, '--maker-fee', '-0.00003', '--taker-fee', '0.0005')
    assert cfg['maker_fee'] == pytest.approx(-0.00003)
    assert cfg['sources']['maker_fee'] == 'cli'


def test_fees_declared_nowhere_are_refused_rather_than_defaulted(tmp_path):
    """A maker *rebate* is the most favourable assumption a quoter can be given.

    The doc's first Phase-4 rule is that no model is silent, and it names fees
    ("Комиссии — реальные HL"). There is no sourced schedule in this repository,
    so the harness asks instead of inventing one — unlike the latency profiles,
    which are conservative and carry an ASSUMPTION string.
    """
    p = make_manifest(tmp_path)
    body = json.loads(p.read_text())
    body['backtest_defaults']['fee_model'] = None
    p.write_text(json.dumps(body))
    with pytest.raises(bf.ConfigError) as e:
        _cfg(p)
    message = str(e.value)
    assert '--maker-fee' in message and 'fee_model' in message


def test_a_manifest_queue_model_that_the_harness_cannot_build_is_refused(tmp_path):
    p = make_manifest(tmp_path)
    body = json.loads(p.read_text())
    body['backtest_defaults']['queue_model'] = {'kind': 'RiskAverseQueueModel'}
    p.write_text(json.dumps(body))
    with pytest.raises(bf.ConfigError) as e:
        _cfg(p)
    assert 'queue_model' in str(e.value)


def test_a_manifest_asking_for_partial_fill_exchange_is_honoured(tmp_path):
    """It was refused before the partial-fill fix; now it is a declaration."""
    p = make_manifest(tmp_path)
    body = json.loads(p.read_text())
    body['backtest_defaults']['exchange_kind'] = {'kind': 'PartialFillExchange'}
    p.write_text(json.dumps(body))
    cfg = _cfg(p)
    assert cfg['exchange_kind'] == 'PartialFillExchange'
    assert cfg['sources']['exchange'] == 'manifest'
    # ...and the CLI still wins over it, because every manifest Phase 3 writes
    # names an exchange and the flag would otherwise be unusable.
    assert _cfg(p, '--exchange', 'no-partial')['exchange_kind'] == \
        'NoPartialFillExchange'


def test_a_manifest_declaring_no_exchange_falls_back_to_the_default(tmp_path):
    p = make_manifest(tmp_path)
    body = json.loads(p.read_text())
    del body['backtest_defaults']['exchange_kind']
    p.write_text(json.dumps(body))
    cfg = _cfg(p)
    assert cfg['exchange'] == bf.DEFAULT_EXCHANGE == 'no-partial'
    assert cfg['sources']['exchange'] == 'default'


def test_a_manifest_asking_for_an_unbuildable_exchange_is_refused(tmp_path):
    p = make_manifest(tmp_path)
    body = json.loads(p.read_text())
    body['backtest_defaults']['exchange_kind'] = {'kind': 'L3NoPartialFillExchange'}
    p.write_text(json.dumps(body))
    with pytest.raises(bf.ConfigError) as e:
        _cfg(p)
    assert 'L3NoPartialFillExchange' in str(e.value)


# ---------------------------------------------------------------------------
# review fixes: the instrument table
# ---------------------------------------------------------------------------

def test_instrument_symbols_come_from_the_execution_and_signal_table(tmp_path):
    """Phase 3 writes `instruments.execution/signal`; this read `.<venue>`.

    The mapping table is the one thing mode A says must be explicit — "явная
    таблица в манифесте … никакого вывода из имён" — and the reproducibility
    record was quietly saying both instruments were unknown.
    """
    p = make_manifest(tmp_path, instruments={
        'execution': {'venue': 'hyperliquid', 'symbol': 'BTC', 'role': 'BacktestAsset'},
        'signal': {'venue': 'binancefuturesum', 'symbol': 'BTCUSDT',
                   'role': 'read-only array, not traded'},
        'mapping_note': 'explicit table',
    })
    m = bf.load_manifest(str(p))
    assert m.hl_symbol == 'BTC'
    assert m.signal_symbol == 'BTCUSDT'
    results = bf.build_results(_cfg(p), m, fake_stats())
    assert results['data']['hl_symbol'] == 'BTC'
    assert results['data']['signal_symbol'] == 'BTCUSDT'


def test_the_venue_keyed_instrument_table_is_still_accepted(manifest_path):
    m = bf.load_manifest(str(manifest_path))
    assert (m.hl_symbol, m.signal_symbol) == ('BTC', 'BTCUSDT')


def test_a_manifest_with_no_instrument_table_is_refused(tmp_path):
    p = make_manifest(tmp_path, instruments={})
    with pytest.raises(bf.ConfigError) as e:
        bf.load_manifest(str(p))
    assert 'instruments' in str(e.value)


# ---------------------------------------------------------------------------
# review fixes: sweep validation
# ---------------------------------------------------------------------------

@pytest.mark.parametrize('spec', ['entry-ms=0,60', 'entry-ms=-25,60', 'resp-ms=60,0'])
def test_a_sweep_cannot_smuggle_a_non_positive_order_latency_past_the_gate(
        manifest_path, spec):
    """`resolve_config` refuses entry/resp <= 0; the sweep re-derived them raw.

    A negative entry latency means the order reaches the exchange before it was
    submitted, and the results file would still stamp it with the assumption
    text as though it were a considered choice.
    """
    base = _cfg(manifest_path)
    knob, values = bf.parse_sweep_spec(spec)
    with pytest.raises(bf.ConfigError) as e:
        bf.sweep_configs(base, knob, values)
    assert 'latency' in str(e.value)


def test_a_sweep_of_positive_latencies_is_still_allowed(manifest_path):
    base = _cfg(manifest_path)
    cfgs = bf.sweep_configs(base, *bf.parse_sweep_spec('entry-ms=20,60'))
    assert [c['entry_ns'] for c in cfgs] == [20 * MS, 60 * MS]


# ---------------------------------------------------------------------------
# review fixes: the snapshot handoff, against a manifest Phase 3 really writes
# ---------------------------------------------------------------------------

def test_reads_a_manifest_produced_by_the_phase_3_tool(tmp_path):
    """Contract test across the Phase 3 / Phase 4 seam.

    The previous fixture pinned `snapshots.created[0].for_day == <first day>`,
    a shape the producer never wrote: it chained `for_day = days[1..N]`. Both
    sides agreed with their own tests and with nothing else.
    """
    bd = pytest.importorskip('build_dataset')
    # The sibling test module owns the Phase-3 fixtures; same directory, already
    # importable, and using them is what keeps this test honest about the shape.
    tbd = pytest.importorskip('test_build_dataset')

    hl_dir, bn_dir = tmp_path / 'hl', tmp_path / 'bn'
    hl_lines = tbd.hl_day_lines(tbd.DAY0_START)
    bn_lines = tbd.bn_day_lines(tbd.DAY0_START)
    tbd.write_gz(hl_dir / f'btc_{tbd.DAY0}.gz', hl_lines)
    tbd.write_gz(bn_dir / f'btcusdt_{tbd.DAY0}.gz', bn_lines)
    tbd.write_meta(hl_dir / f'_meta_hyperliquid_{tbd.DAY0}.jsonl', [
        (tbd.DAY0_START, tbd.session_start('hyperliquid', ['BTC'])),
        (tbd.DAY0_START + 1, tbd.universe_record('BTC', 5)),
    ])
    tbd.write_meta(bn_dir / f'_meta_binancefuturesum_{tbd.DAY0}.jsonl', [
        (tbd.DAY0_START, tbd.session_start('binancefuturesum', ['BTCUSDT'])),
    ])
    report = tbd.make_report(tmp_path / 'report.json', hl_dir, bn_dir,
                             (hl_lines[0][0], hl_lines[-1][0]),
                             (bn_lines[0][0], bn_lines[-1][0]))
    out = tmp_path / 'out'
    args = bd.parse_args([
        '--quality-report', str(report), '--hl-symbol', 'BTC',
        '--binance-symbol', 'BTCUSDT', '--out-dir', str(out),
        '--min-window-hours', '0',
        '--maker-fee', '0.00015', '--taker-fee', '0.00045',
        '--entry-latency-ms', '5', '--resp-latency-ms', '9',
    ])
    bd.build(args, convert_fn=tbd.FakeConverter(), snapshot_fn=tbd.FakeSnapshotter())

    m = bf.load_manifest(str(out / 'manifest.json'))
    assert m.initial_snapshot is None, 'Phase 3 builds none, and says so'
    assert m.hl_symbol == 'BTC' and m.signal_symbol == 'BTCUSDT'
    assert m.tick_size > 0 and m.lot_size > 0
    cfg = bf.resolve_config(
        bf.build_parser().parse_args(['--manifest', str(out / 'manifest.json')]), m)
    assert (cfg['entry_ns'], cfg['resp_ns']) == (5 * MS, 9 * MS)
    assert cfg['sources']['order_latency'] == 'manifest'
    assert cfg['maker_fee'] == pytest.approx(0.00015)
    assert cfg['sources']['maker_fee'] == 'manifest'
    assert cfg['window_start_ns'] == m.window_start_ns


def test_a_snapshot_that_matches_no_built_day_is_refused(tmp_path):
    """Silently ignoring it is how the disjoint `for_day` keying survived."""
    p = make_manifest(tmp_path)
    body = json.loads(p.read_text())
    snap = write_hl_npz(tmp_path / 'dataset' / 'snap_eod.npz')
    body['snapshots'] = {'created': [{'for_day': '20260731', 'path': snap}]}
    p.write_text(json.dumps(body))
    with pytest.raises(bf.ConfigError) as e:
        bf.load_manifest(str(p))
    assert 'for_day' in str(e.value)


def test_an_explicitly_declared_initial_snapshot_is_used(tmp_path):
    p = make_manifest(tmp_path)
    body = json.loads(p.read_text())
    snap = write_hl_npz(tmp_path / 'dataset' / 'snap_prev_day.npz')
    body['snapshots'] = {'created': [], 'initial_snapshot': snap}
    p.write_text(json.dumps(body))
    m = bf.load_manifest(str(p))
    assert m.initial_snapshot == snap


# ---------------------------------------------------------------------------
# review fixes: an empty leading signal region is a refusal, not a warning
# ---------------------------------------------------------------------------

def test_a_signal_starting_long_after_the_window_is_refused(tmp_path):
    """Mode A: "сигнал обязан покрывать всё торговое окно".

    A leading region with no signal at all cannot be traded — every step there
    blocks — so it is a broken dataset, not a warning to scroll past.
    """
    p = make_manifest(tmp_path)
    body = json.loads(p.read_text())
    body['window']['start_ns'] = T0 - 600 * S
    p.write_text(json.dumps(body))
    args = bf.build_parser().parse_args(['--manifest', str(p)])
    with pytest.raises(bf.ConfigError) as e:
        bf._run_once_from_args(args, runner=lambda *a, **k: fake_stats())
    assert 'max_signal_age' in str(e.value)


def test_a_signal_starting_within_the_age_bound_only_warns(tmp_path, capsys):
    p = make_manifest(tmp_path)
    body = json.loads(p.read_text())
    body['window']['start_ns'] = T0 - 10 * MS   # inside max_signal_age (1000 ms)
    p.write_text(json.dumps(body))
    args = bf.build_parser().parse_args(['--manifest', str(p), '--out', str(tmp_path / 'r.json')])
    bf._run_once_from_args(args, runner=lambda *a, **k: fake_stats())
    assert 'signal starts' in capsys.readouterr().err


# ---------------------------------------------------------------------------
# review fixes: the second obligatory sensitivity family (book_mode)
# ---------------------------------------------------------------------------

def test_comparing_two_manifests_moves_only_book_mode(tmp_path):
    """The doc names two obligatory families; only signal-lag had machinery.

    `book_mode` needs two datasets, so it cannot be a `--sweep` knob — but
    "run it twice by hand" gives no NO-DIFFERENCE statement and no guarantee
    that nothing else moved, which is exactly what the criterion rests on.
    """
    slow = make_manifest(tmp_path / 'a', book_mode='slow', num_levels=20)
    fast = make_manifest(tmp_path / 'b', book_mode='fast', num_levels=5)
    (tmp_path / 'a').mkdir(exist_ok=True)
    out = tmp_path / 'cmp.json'
    summary = bf.run_compare(str(slow), str(fast), ['--out', str(out)],
                             runner=lambda cfg, m, s: fake_stats(
                                 fills=3 if m.book_mode == 'slow' else 9))
    assert summary['knob'] == 'book_mode'
    assert summary['values'] == ['slow', 'fast']
    assert 'fills' in summary['moved']
    assert summary['identical'] is False
    assert json.loads(out.read_text())['knob'] == 'book_mode'


def test_comparing_two_manifests_reports_no_difference_as_a_result(tmp_path):
    slow = make_manifest(tmp_path / 'a', book_mode='slow', num_levels=20)
    fast = make_manifest(tmp_path / 'b', book_mode='fast', num_levels=5)
    summary = bf.run_compare(str(slow), str(fast),
                             ['--out', str(tmp_path / 'cmp.json')],
                             runner=lambda cfg, m, s: fake_stats())
    assert summary['identical'] is True
    assert 'NO DIFFERENCE' in bf.format_sweep_table(summary)


def test_comparing_manifests_that_differ_in_more_than_book_mode_is_refused(tmp_path):
    slow = make_manifest(tmp_path / 'a', book_mode='slow', num_levels=20)
    fast = make_manifest(tmp_path / 'b', book_mode='fast', num_levels=5,
                         tick_size=0.5)
    with pytest.raises(bf.ConfigError) as e:
        bf.run_compare(str(slow), str(fast), ['--out', str(tmp_path / 'c.json')],
                       runner=lambda cfg, m, s: fake_stats())
    assert 'tick_size' in str(e.value)


def test_comparing_two_manifests_of_the_same_book_mode_is_refused(tmp_path):
    a = make_manifest(tmp_path / 'a', book_mode='slow', num_levels=20)
    b = make_manifest(tmp_path / 'b', book_mode='slow', num_levels=20)
    with pytest.raises(bf.ConfigError) as e:
        bf.run_compare(str(a), str(b), ['--out', str(tmp_path / 'c.json')],
                       runner=lambda cfg, m, s: fake_stats())
    assert 'book_mode' in str(e.value)


# ---------------------------------------------------------------------------
# Hyperliquid order-request budget
#
# The engine models fills, fees and queue position but has no notion of the
# venue's CUMULATIVE order-request budget: `10000 + filled notional`, one unit
# per request, and a requote costs two because Hyperliquid has no modify. So a
# geometry that requotes on every flicker and fills rarely reads BETTER here
# than a calm one — it captures more spread and is charged nothing for the
# requests. These numbers make that cost visible; the selection gate that acts
# on them lives where a winner is actually picked (myhft
# scripts/summarize_guard_run.py).
# ---------------------------------------------------------------------------

def test_requests_per_dollar_is_requests_over_filled_notional():
    b = bf.request_budget_block(n_new=600, n_cancel=400, filled_notional_usd=2000.0)
    assert b['total_requests'] == 1000
    assert b['requests_per_dollar_filled'] == pytest.approx(0.5)
    assert b['verdict'] == 'sustainable'


def test_the_denominator_is_filled_notional_not_quoted_notional():
    # Identical churn, different FILLED notional. The venue pays budget on
    # fills, so this is the only denominator that separates the two.
    thin = bf.request_budget_block(n_new=600, n_cancel=400, filled_notional_usd=100.0)
    fat = bf.request_budget_block(n_new=600, n_cancel=400, filled_notional_usd=100_000.0)
    assert thin['requests_per_dollar_filled'] == pytest.approx(10.0)
    assert fat['requests_per_dollar_filled'] == pytest.approx(0.01)
    assert thin['verdict'] == 'infeasible'
    assert fat['verdict'] == 'sustainable'


def test_over_churn_is_infeasible():
    b = bf.request_budget_block(n_new=12_000, n_cancel=8_700, filled_notional_usd=10_000.0)
    assert b['requests_per_dollar_filled'] == pytest.approx(2.07)
    assert b['verdict'] == 'infeasible'
    assert b['feasible'] is False


def test_the_ceiling_is_feasible_and_a_hair_over_it_is_not():
    at = bf.request_budget_block(n_new=500, n_cancel=500, filled_notional_usd=1000.0)
    over = bf.request_budget_block(n_new=500, n_cancel=501, filled_notional_usd=1000.0)
    assert at['feasible'] is True
    assert over['feasible'] is False


def test_a_run_that_churns_without_filling_is_infeasible_not_a_zero_division():
    b = bf.request_budget_block(n_new=5000, n_cancel=5000, filled_notional_usd=0.0)
    assert b['verdict'] == 'infeasible'
    assert b['requests_per_dollar_filled'] is None
    assert 'no fills' in b['reason'].lower()


def test_a_run_that_did_nothing_is_infeasible_too():
    b = bf.request_budget_block(n_new=0, n_cancel=0, filled_notional_usd=0.0)
    assert b['verdict'] == 'infeasible'
    assert b['requests_per_dollar_filled'] is None


def test_the_window_check_is_reported_but_is_not_the_verdict():
    # 600 requests fit inside the one-time 10000 buffer, so the ABSOLUTE form
    # passes — while the ratio says the geometry is six times unsustainable.
    # Reading the absolute form as a pass is the whole mistake.
    b = bf.request_budget_block(n_new=300, n_cancel=300, filled_notional_usd=100.0)
    assert b['window_ok'] is True
    assert b['window_budget'] == pytest.approx(10_100.0)
    assert b['requests_per_dollar_filled'] == pytest.approx(6.0)
    assert b['verdict'] == 'infeasible'


def test_the_ceiling_is_a_named_constant():
    assert bf.BUDGET_REQUESTS_PER_DOLLAR_MAX == 1.0
    assert bf.BUDGET_INITIAL_BUFFER_USD == 10_000.0
    assert bf.BUDGET_WARN_RATIO < bf.BUDGET_REQUESTS_PER_DOLLAR_MAX


def _budget_results(manifest_path, stats):
    m = bf.load_manifest(str(manifest_path))
    cfg = bf.resolve_config(
        bf.build_parser().parse_args(['--manifest', str(manifest_path)]), m)
    return bf.build_results(cfg, m, stats)


def test_results_carry_the_budget_built_from_the_run_counts(manifest_path):
    # submits/cancels are counted at the strategy's own emit sites in the grid
    # loop, and trading_value is the engine's filled notional. The block must be
    # derived from those, never restated by hand.
    stats = fake_stats(submits=12_000, cancels=8_700, trading_value=10_000.0)
    budget = _budget_results(manifest_path, stats)['run']['request_budget']
    assert budget['n_new'] == 12_000
    assert budget['n_cancel'] == 8_700
    assert budget['total_requests'] == 20_700
    assert budget['filled_notional_usd'] == pytest.approx(10_000.0)
    assert budget['requests_per_dollar_filled'] == pytest.approx(2.07)
    assert budget['verdict'] == 'infeasible'


def test_a_sustainable_run_reports_sustainable(manifest_path):
    stats = fake_stats(submits=300, cancels=200, trading_value=10_000.0)
    results = _budget_results(manifest_path, stats)
    assert results['run']['request_budget']['verdict'] == 'sustainable'


def test_the_budget_metrics_are_compared_across_a_sweep():
    # A sweep that moves a geometry knob moves the churn with it; if the metric
    # is not in the compared set the tool prints "no difference" about the one
    # number that decides whether the geometry can run at all.
    assert 'total_requests' in bf.SWEEP_METRICS
    assert 'requests_per_dollar_filled' in bf.SWEEP_METRICS
