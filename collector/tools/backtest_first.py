#!/usr/bin/env python3
"""Phase 4 — exploratory backtest harness (docs/design-multi-venue-collection.md).

Mode A of that document: Hyperliquid is the single traded ``BacktestAsset``,
Binance USD-M is a read-only signal array the strategy looks up causally against
``hbt.current_timestamp``. This script runs that backtest and writes down
everything needed to reproduce it.

What the doc demands of this phase, and where it lives here:

* **No silent default.** ``BacktestAsset::new()`` quietly gives zero fees, zero
  latency, ``LogProbQueueModel2``, ``NoPartialFillExchange``
  (``py-hftbacktest/src/lib.rs:134-155``). Every one of those is set explicitly
  by :func:`native_runner` and echoed into the results JSON — including values
  that happen to equal the default — together with where each came from
  (``cli`` / ``manifest`` / ``default``).
* **The exchange model is a choice**, ``--exchange {no-partial,partial}``, and
  the choice plus where it came from go into the results. ``PartialFillExchange``
  used to be banned here because a partially filled order never reached the
  local position (the local side applied a fill only on ``Status::Filled``).
  That is fixed — both local processors now apply every execution response and
  ``exec_qty`` is per-execution, so the fills add up; AGENTS.md §4.6 has the fix,
  its pins and what the model still does not cover. The default stays
  ``no-partial`` so runs made before the fix remain comparable, and
  ``--sweep exchange=no-partial,partial`` measures the difference on one dataset
  instead of arguing about it.
* **Order latency is constant, explicit and non-zero**, with three named
  profiles. The numbers are an ASSUMPTION pending a live submit->ack
  measurement (open decision 5) and say so in the results.
* **Warm-up / liveness guard** (:func:`guard_ok`) correct from the first
  callback, including the first recorded day, which has no predecessor snapshot
  and therefore starts with an empty book (doc §3.4).
* **Book age is tracked by the strategy** from observed top-of-book changes:
  ``depth.timestamp`` is never written (AGENTS.md §4.7).
* **Sensitivity**: ``--sweep`` re-runs one configuration varying exactly one
  knob. No difference is a result and is printed as one. The doc's second
  obligatory family — ``book_mode`` slow/20 against fast/5 — needs two datasets
  and so has its own mode, ``--compare-manifest``, which produces the same
  comparison table and refuses two manifests that differ in anything else.

Timestamps are int64 nanoseconds end to end. 1.8e18 does not fit float64
exactly (2^53 ~ 9e15), so a single float cast silently rounds to ~256 ns; the
loader refuses a signal array whose ``ts`` is not int64.

Manifest
--------
Written by ``build_dataset.py`` (Phase 3). The keys read here::

    tick_size, lot_size                     floats
    book_mode, num_levels                   'slow'|'fast'|'bbo+fast', int
    tick_lot_source.sz_decimals             int, optional (HL price rule)
    window.start_ns, window.end_ns          int64 ns, on the same scale as the
                                            data (the builder applies
                                            clock_correction_ns to both)
    clock_correction_ns                     int, must be 0 unless deliberate
    max_signal_age_ns, max_hl_book_age_ns   int ns
    outputs.hl_depth[].path                 converted HL .npz, in order
    outputs.signal.path                     signal .npz: ts int64 (N,),
                                            values float64 (N,4) =
                                            bid_px, bid_qty, ask_px, ask_qty
    snapshots.initial_snapshot              explicit initial snapshot, or null;
    snapshots.created[].{for_day,path}      a built one whose for_day is the
                                            first day of the run. Phase 3 builds
                                            none (§3.4) — one that matches no
                                            built day is a refusal, not a
                                            shrug.
    instruments.execution.symbol            the traded HL instrument
    instruments.signal.symbol               the Binance signal instrument; both
                                            required, never inferred from a name
    backtest_defaults.fee_model             {maker_fee, taker_fee}: no default
                                            exists, so either this or
                                            --maker-fee/--taker-fee
    backtest_defaults.constant_latency_ns   {entry, response} in ns
    backtest_defaults.queue_model.kind      must be LogProbQueueModel2
    backtest_defaults.exchange_kind.kind    NoPartialFillExchange or
                                            PartialFillExchange; a manifest
                                            naming either is honoured, anything
                                            else is refused
    collector, converter, schema            identity only

Short hand-written aliases (``hl_npz``, ``signal_npz``, ``max_book_age_ns``…)
are accepted; the resolved key path for every value is recorded in the results
under ``manifest.resolution``.

Usage
-----
::

    backtest_first.py --manifest dataset/manifest.json --strategy noop \\
        --latency-profile base --out results.json

    backtest_first.py --manifest dataset/manifest.json --strategy grid \\
        --sweep signal-lag=0,10,50 --out sweep/results.json

    backtest_first.py --manifest slow/manifest.json --strategy grid \\
        --compare-manifest fast/manifest.json --out cmp/book_mode.json
"""

from __future__ import annotations

import argparse
import copy
import hashlib
import json
import math
import os
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional

import numpy as np

TOOL_NAME = 'backtest_first.py'
PHASE = 'Phase 4 — exploratory backtest (docs/design-multi-venue-collection.md)'
RESULTS_SCHEMA = 'hftbacktest/multi-venue-backtest-results/1'

MS = 1_000_000
S = 1_000_000_000

# hftbacktest/src/depth/mod.rs:21,24 — no best bid / no best ask.
INVALID_MIN = -9_223_372_036_854_775_808
INVALID_MAX = 9_223_372_036_854_775_807

#: Sentinel for "no signal row at all", passed to the guard in place of a
#: timestamp. Chosen as 0 rather than INT64_MIN so ``now - ts`` cannot overflow;
#: it makes the age enormous, so an absent signal blocks through exactly the
#: same branch as a stale one — the doc's fail-closed line for mode A.
SIGNAL_TS_ABSENT = 0

#: :func:`signal_index` result meaning "no row at or before now".
SIGNAL_INDEX_ABSENT = -1

GUARD_OK = 0
GUARD_NO_BID = 1
GUARD_NO_ASK = 2
GUARD_CROSSED_OR_LOCKED = 3
GUARD_BOOK_STALE = 4
GUARD_SIGNAL_STALE = 5

GUARD_REASON_NAMES = (
    'ok',
    'no_bid',
    'no_ask',
    'crossed_or_locked',
    'book_stale',
    'signal_stale',
)

#: (entry, resp) order latency in milliseconds.
LATENCY_PROFILES = {
    'low': (20, 40),
    'base': (60, 120),
    'high': (150, 300),
}

LATENCY_ASSUMPTION = (
    'ASSUMPTION, not a measurement: no live submit->ack sample exists yet '
    '(open decision 5 of docs/design-multi-venue-collection.md). The three '
    'profiles low/base/high = 20/40, 60/120, 150/300 ms (entry/resp) are '
    'conservative constants. Replace them once submit/ack/fill have been '
    'measured against the venue, and re-run the sensitivity family.'
)

PARTIAL_FILL_FIXED = (
    'PartialFillExchange was forbidden here until 2026-07-29: the exchange side '
    'set Status::PartiallyFilled but the local side applied a fill only on '
    'Status::Filled, so partial fills never reached the strategy position or '
    'PnL. Fixed in proc/local.rs and proc/l3_local.rs — every execution '
    'response is applied and exec_qty carries that single execution, so the '
    'fills add up without double-counting. AGENTS.md §4.6.'
)

PARTIAL_FILL_REMAINING_GAP = (
    'What PartialFillExchange still does not model (AGENTS.md §4.6): a '
    'liquidity-taking order walking several price levels reports only its last '
    'chunk to the local, and a partially executed IOC/FOK/Market order expires '
    'without reporting the executed part at all. A pure-maker grid touches '
    'neither, but a conclusion that leans on taking liquidity does.'
)

NO_PARTIAL_FILL_DISTORTION = (
    'NoPartialFillExchange fills the whole leaves_qty regardless of the '
    'liquidity resting at the level (proc/nopartialfillexchange.rs:58-62). '
    'Accepted in writing by the design doc; conclusions sensitive to partial '
    'fills can now be checked against --exchange partial rather than assumed.'
)

FEE_ASSUMPTION = (
    'Fees are not defaulted. Nothing in this repository cites a Hyperliquid '
    'schedule, and the number that used to be the default (-0.003% maker) is a '
    'volume-tier REBATE, i.e. the most favourable assumption a pure-maker grid '
    'can be handed — the opposite of the fail-closed rule. Whatever is passed '
    'here is an ASSUMPTION until a fill from the live venue confirms the tier; '
    'the results file records the value and where it came from.'
)

#: The queue model this harness actually builds. A manifest that declares
#: something else is refused rather than silently overridden — the whole point
#: of Phase 4 is that no model is picked up quietly.
SUPPORTED_QUEUE_MODEL = 'LogProbQueueModel2'

#: The exchange models this harness builds, keyed by the ``--exchange``
#: spelling. Unlike the queue model there are two of them, so a manifest naming
#: one of these is a declaration to honour, not a disagreement to refuse; only a
#: kind outside this table is refused. Whichever is used, the results say which
#: and where the choice came from.
EXCHANGE_KINDS = {
    'no-partial': 'NoPartialFillExchange',
    'partial': 'PartialFillExchange',
}

#: Continuity: every run made before the partial-fill fix used this one.
DEFAULT_EXCHANGE = 'no-partial'

DEFAULT_ELAPSE_MS = 100.0
DEFAULT_LATENCY_PROFILE = 'base'
DEFAULT_HL_SIG_FIGS = 5

OBSERVATION_NOTE = (
    'elapse(delta) gives an observation delay of 0..delta: the strategy sees a '
    'book or signal change only at the next tick of the grid. wait_next_feed '
    'would wake on any local HL event but never on a Binance signal row '
    '(backtest/mod.rs:794-797, :811-814), so the grid is the deliberate choice '
    'here. Keep delta well below max_signal_age_ns and max_hl_book_age_ns, or '
    'the age bounds are quantised by the grid rather than by the data.'
)

# --- end-of-run reasons -----------------------------------------------------
END_OF_DATA = 0
END_WINDOW = 1
END_MAX_STEPS = 2
END_REASON_NAMES = ('end_of_data', 'window_end', 'max_steps')

# --- njit counter slots -----------------------------------------------------
IST_STEPS = 0
IST_GUARD_PASS = 1
IST_BLK_NO_BID = 2
IST_BLK_NO_ASK = 3
IST_BLK_CROSSED = 4
IST_BLK_BOOK_STALE = 5
IST_BLK_SIGNAL_STALE = 6
IST_SIGNAL_READS = 7
IST_SIGNAL_ABSENT = 8
IST_BOOK_CHANGES = 9
IST_SUBMITS = 10
IST_CANCELS = 11
IST_OUTSIDE_WINDOW = 12
IST_FIRST_PASS_STEP = 13
IST_END_REASON = 14
IST_FIRST_TS = 15
IST_LAST_TS = 16
IST_FILLS = 17
IST_LEVEL_COLLISIONS = 18
N_ISTAT = 19

FST_POSITION = 0
FST_BALANCE = 1
FST_FEE = 2
FST_TRADING_VOLUME = 3
FST_TRADING_VALUE = 4
FST_FINAL_MID = 5
FST_LAST_SIGNAL_PX = 6
N_FSTAT = 7


class ConfigError(Exception):
    """A refusal: the run would be meaningless or misleading, so it stops."""


# ---------------------------------------------------------------------------
# pure functions — njit-compatible, wrapped with njit at the use site
# ---------------------------------------------------------------------------

def guard_block_reason(
        best_bid_tick,
        best_ask_tick,
        invalid_min,
        invalid_max,
        last_book_change_ts,
        now_ts,
        max_book_age_ns,
        last_signal_ts,
        max_signal_age_ns,
):
    """Why trading is blocked right now, or :data:`GUARD_OK`.

    The doc's warm-up / liveness conditions, in order, all int64:

    1. a best bid exists (``!= invalid_min``);
    2. a best ask exists (``!= invalid_max``);
    3. the spread is strictly positive in ticks;
    4. the book changed within ``max_book_age_ns`` — the caller tracks that
       itself, because ``depth.timestamp`` is never written (AGENTS.md §4.7);
    5. the signal is no older than ``max_signal_age_ns``; an absent signal
       arrives here as :data:`SIGNAL_TS_ABSENT` and blocks through this same
       branch.

    A negative age (a timestamp in the future relative to ``now_ts``) is broken
    bookkeeping and blocks too — fail closed rather than trade on it.
    """
    if best_bid_tick == invalid_min:
        return GUARD_NO_BID
    if best_ask_tick == invalid_max:
        return GUARD_NO_ASK
    if best_bid_tick >= best_ask_tick:
        return GUARD_CROSSED_OR_LOCKED
    book_age = now_ts - last_book_change_ts
    if book_age < 0 or book_age > max_book_age_ns:
        return GUARD_BOOK_STALE
    signal_age = now_ts - last_signal_ts
    if signal_age < 0 or signal_age > max_signal_age_ns:
        return GUARD_SIGNAL_STALE
    return GUARD_OK


def guard_ok(
        best_bid_tick,
        best_ask_tick,
        invalid_min,
        invalid_max,
        last_book_change_ts,
        now_ts,
        max_book_age_ns,
        last_signal_ts,
        max_signal_age_ns,
):
    """``True`` when every condition of :func:`guard_block_reason` holds."""
    return guard_block_reason(
        best_bid_tick, best_ask_tick, invalid_min, invalid_max,
        last_book_change_ts, now_ts, max_book_age_ns,
        last_signal_ts, max_signal_age_ns,
    ) == GUARD_OK


def signal_index(signal_ts, now_ts, signal_lag_ns):
    """Index of the last signal row with ``ts <= now_ts - signal_lag_ns``.

    ``side='right'`` resolves ties to the **last** row with that timestamp,
    which is the arrival order the Phase-3 builder guarantees. Returns
    :data:`SIGNAL_INDEX_ABSENT` when no such row exists — before the first row,
    or when the lag pushed the cutoff behind it. Staleness is *not* decided
    here; that is the guard's job.
    """
    if signal_ts.shape[0] == 0:
        return SIGNAL_INDEX_ABSENT
    cutoff = now_ts - signal_lag_ns
    idx = np.searchsorted(signal_ts, cutoff, side='right') - 1
    if idx < 0:
        return SIGNAL_INDEX_ABSENT
    return idx


def signal_price(bid_px, bid_qty, ask_px, ask_qty, use_microprice):
    """Mid or microprice of one signal row; mid when a size is missing."""
    if use_microprice:
        total = bid_qty + ask_qty
        if total > 0.0:
            return (bid_px * ask_qty + ask_px * bid_qty) / total
    return (bid_px + ask_px) / 2.0


def normalize_price(px, tick_size, sig_figs, max_decimals, round_down):
    """Round a quote to something the venue would actually accept.

    Hyperliquid has no fixed tick: a price is legal with at most ``sig_figs``
    significant figures and at most ``max_decimals`` decimal places, so the
    effective tick depends on the price (design-hyperliquid-connector.md §5.3).
    ``BacktestAsset`` registers a fixed ``tick_size`` regardless, so the
    strategy has to apply the venue rule itself — otherwise the backtest quotes
    levels the live connector could never place (doc, Phase 4).

    ``round_down`` (bids) always widens the quote; ``False`` (asks) rounds up,
    also widening. Passive in both directions. ``sig_figs <= 0`` or
    ``max_decimals < 0`` disables that step; ``tick_size <= 0`` disables the
    final alignment.
    """
    if not (px > 0.0) or not math.isfinite(px):
        return math.nan
    out = px
    if sig_figs > 0:
        exp = int(math.floor(math.log10(out)))
        # log10 is off by one ulp at exact powers of ten; correct it.
        if math.pow(10.0, float(exp)) > out:
            exp -= 1
        elif math.pow(10.0, float(exp + 1)) <= out:
            exp += 1
        scale = math.pow(10.0, float(sig_figs - 1 - exp))
        if round_down:
            out = math.floor(out * scale) / scale
        else:
            out = math.ceil(out * scale) / scale
    if max_decimals >= 0:
        scale = math.pow(10.0, float(max_decimals))
        if round_down:
            out = math.floor(out * scale) / scale
        else:
            out = math.ceil(out * scale) / scale
    if tick_size > 0.0:
        t = out / tick_size
        if round_down:
            out = math.floor(t) * tick_size
        else:
            out = math.ceil(t) * tick_size
    return out


def latency_profile_ns(name):
    """(entry_ns, resp_ns) for a named profile. See :data:`LATENCY_ASSUMPTION`."""
    try:
        entry_ms, resp_ms = LATENCY_PROFILES[name]
    except KeyError:
        raise ConfigError(
            'unknown --latency-profile %r; known: %s'
            % (name, ', '.join(sorted(LATENCY_PROFILES)))
        ) from None
    return entry_ms * MS, resp_ms * MS


# ---------------------------------------------------------------------------
# manifest and signal loading
# ---------------------------------------------------------------------------

def sha256_file(path):
    h = hashlib.sha256()
    with open(path, 'rb') as f:
        for chunk in iter(lambda: f.read(1 << 20), b''):
            h.update(chunk)
    return h.hexdigest()


@dataclass
class Manifest:
    path: str
    raw: dict
    hl_npz: list
    signal_npz: str
    initial_snapshot: Optional[str]
    tick_size: float
    lot_size: float
    book_mode: Optional[str]
    num_levels: Optional[int]
    sz_decimals: Optional[int]
    hl_symbol: Optional[str]
    signal_symbol: Optional[str]
    window_start_ns: int
    window_end_ns: int
    clock_correction_ns: int
    max_signal_age_ns: int
    max_hl_book_age_ns: int
    resolution: dict = field(default_factory=dict)
    identity: dict = field(default_factory=dict)


def _dig(obj, dotted):
    cur = obj
    for part in dotted.split('.'):
        if not isinstance(cur, dict) or part not in cur:
            return None
        cur = cur[part]
    return cur


def _pick(raw, name, paths, resolution, required=True, default=None):
    for p in paths:
        value = _dig(raw, p)
        if value is not None:
            resolution[name] = p
            return value
    if required:
        raise ConfigError(
            'manifest is missing %s: none of %s is present. Phase 3 '
            '(build_dataset.py) writes the first form.' % (name, ', '.join(paths))
        )
    resolution[name] = None
    return default


def _as_int(value):
    if isinstance(value, bool) or not isinstance(value, int):
        if isinstance(value, float) and value.is_integer():
            # Tolerated, but a float nanosecond count is a smell worth naming.
            return int(value)
        raise ConfigError('expected an integer nanosecond value, got %r' % (value,))
    return int(value)


def load_manifest(path):
    """Read a Phase-3 manifest and check that what it points at exists."""
    path = os.path.abspath(path)
    try:
        with open(path) as f:
            raw = json.load(f)
    except FileNotFoundError:
        raise ConfigError('manifest not found: %s' % path) from None
    except json.JSONDecodeError as e:
        raise ConfigError('manifest %s is not valid JSON: %s' % (path, e)) from None
    if not isinstance(raw, dict):
        raise ConfigError('manifest %s must be a JSON object' % path)

    res = {}

    hl_entries = _pick(raw, 'hl_npz',
                       ['outputs.hl_depth', 'hl_npz', 'outputs.hl', 'hl.npz'], res)
    hl_npz = []
    for entry in hl_entries if isinstance(hl_entries, list) else [hl_entries]:
        if isinstance(entry, dict):
            res['hl_npz'] = 'outputs.hl_depth[].path'
            hl_npz.append(str(entry['path']))
        else:
            hl_npz.append(str(entry))
    if not hl_npz:
        raise ConfigError('manifest %s lists no Hyperliquid .npz to backtest' % path)

    signal_npz = str(_pick(raw, 'signal_npz',
                           ['outputs.signal.path', 'signal_npz', 'signal.path',
                            'outputs.signal'], res))

    tick_size = float(_pick(raw, 'tick_size', ['tick_size', 'hl.tick_size'], res))
    lot_size = float(_pick(raw, 'lot_size', ['lot_size', 'hl.lot_size'], res))
    window = _pick(raw, 'window', ['window'], res)
    start_ns = _as_int(_pick(window, 'window.start_ns', ['start_ns', 'start'], res))
    end_ns = _as_int(_pick(window, 'window.end_ns', ['end_ns', 'end'], res))
    if end_ns <= start_ns:
        raise ConfigError(
            'manifest window is empty or reversed: start_ns=%d end_ns=%d' % (start_ns, end_ns))

    max_signal_age_ns = _as_int(_pick(
        raw, 'max_signal_age_ns',
        ['max_signal_age_ns', 'signal.max_age_ns'], res))
    max_book_age_ns = _as_int(_pick(
        raw, 'max_hl_book_age_ns',
        ['max_hl_book_age_ns', 'max_book_age_ns', 'hl.max_book_age_ns'], res))
    clock_correction_ns = _as_int(_pick(
        raw, 'clock_correction_ns', ['clock_correction_ns'], res,
        required=False, default=0))

    # The initial snapshot, if the dataset has one at all. Phase 3 builds none:
    # the days of a window are one continuous stream in a single BacktestAsset,
    # so only a snapshot of the day *preceding* the window would change a run,
    # and that day is outside it (doc §3.4 — the first day starts with an empty
    # book and the warm-up guard is what keeps the strategy out).
    #
    # A `snapshots.created` list that matches no built day is a refusal rather
    # than a silent None: that mismatch is exactly how the old disjoint keying
    # (`for_day = days[1..N]` written, `for_day == days[0]` read) stayed
    # invisible while a native-engine run per day produced files nothing read.
    snapshot = _dig(raw, 'snapshots.initial_snapshot')
    if snapshot is not None:
        res['initial_snapshot'] = 'snapshots.initial_snapshot'
        snapshot = str(snapshot)
    else:
        created = [e for e in (_dig(raw, 'snapshots.created') or [])
                   if isinstance(e, dict)]
        first_day = None
        if isinstance(hl_entries, list) and hl_entries and isinstance(hl_entries[0], dict):
            first_day = hl_entries[0].get('day')
        usable = [e for e in created if e.get('path') and (
            first_day is None or e.get('for_day') == first_day)]
        if usable:
            snapshot = str(usable[0]['path'])
            res['initial_snapshot'] = 'snapshots.created[].path'
        elif created:
            raise ConfigError(
                'manifest %s lists %d snapshot(s) but none is for the first day '
                'of the run (%r); their for_day values are %s. A snapshot that '
                'cannot be selected is either a mislabelled file or a producer '
                'and a consumer that disagree — both need looking at, and '
                'neither is something to run past.'
                % (path, len(created), first_day,
                   ', '.join(repr(e.get('for_day')) for e in created)))
        else:
            snapshot = _pick(raw, 'initial_snapshot',
                             ['initial_snapshot', 'hl.initial_snapshot'], res,
                             required=False, default=None)
            if snapshot is not None:
                snapshot = str(snapshot)

    # The instrument table. Mode A: "явная таблица в манифесте (HL BTC <-> UM
    # BTCUSDT); никакого вывода из имён" — so it is read, never inferred from a
    # file name, and a manifest without one is refused rather than recorded as
    # two nulls in the reproducibility record.
    hl_symbol = _pick(raw, 'hl_symbol',
                      ['instruments.execution.symbol',
                       'instruments.hyperliquid.symbol', 'hl_symbol'], res,
                      required=False, default=None)
    signal_symbol = _pick(raw, 'signal_symbol',
                          ['instruments.signal.symbol',
                           'instruments.binancefuturesum.symbol',
                           'instruments.binancefutures.symbol', 'signal_symbol'],
                          res, required=False, default=None)
    if hl_symbol is None or signal_symbol is None:
        raise ConfigError(
            'manifest %s does not name both instruments (execution=%r, '
            'signal=%r). Mode A maps them with an explicit table — '
            'instruments.execution.symbol and instruments.signal.symbol, which '
            'is what build_dataset.py writes — and never infers one from a file '
            'name.' % (path, hl_symbol, signal_symbol))

    missing = [p for p in hl_npz + [signal_npz] + ([snapshot] if snapshot else [])
               if not os.path.exists(p)]
    if missing:
        raise ConfigError(
            'manifest %s points at files that do not exist: %s'
            % (path, ', '.join(missing)))

    identity = {
        'path': path,
        'sha256': sha256_file(path),
        'schema': raw.get('schema'),
        'generated_at': raw.get('generated_at'),
        'quality_report': raw.get('quality_report'),
        'collector': raw.get('collector'),
        'converter': raw.get('converter'),
        'rebuild_cmd': raw.get('rebuild_cmd'),
        'resolution': res,
    }

    return Manifest(
        path=path,
        raw=raw,
        hl_npz=hl_npz,
        signal_npz=signal_npz,
        initial_snapshot=snapshot,
        tick_size=tick_size,
        lot_size=lot_size,
        book_mode=raw.get('book_mode'),
        num_levels=raw.get('num_levels'),
        sz_decimals=_dig(raw, 'tick_lot_source.sz_decimals'),
        hl_symbol=str(hl_symbol),
        signal_symbol=str(signal_symbol),
        window_start_ns=start_ns,
        window_end_ns=end_ns,
        clock_correction_ns=clock_correction_ns,
        max_signal_age_ns=max_signal_age_ns,
        max_hl_book_age_ns=max_book_age_ns,
        resolution=res,
        identity=identity,
    )


def load_signal(path):
    """Load the Binance signal array: ``ts`` int64 (N,), ``values`` (N, 4).

    Refuses anything that would let a nanosecond pass through float64.
    """
    try:
        npz = np.load(path)
    except FileNotFoundError:
        raise ConfigError('signal array not found: %s' % path) from None

    def take(names):
        for n in names:
            if n in npz:
                return npz[n]
        raise ConfigError(
            'signal array %s has no %s key (keys: %s)'
            % (path, ' / '.join(names), ', '.join(npz.files)))

    ts = take(['ts', 'local_ts', 'timestamp'])
    values = take(['values', 'data', 'value'])

    if ts.dtype != np.int64:
        raise ConfigError(
            'signal timestamps in %s are %s, not int64. Nanoseconds (~1.8e18) do '
            'not survive float64 (2^53 ~ 9e15): they round to ~256 ns. Rebuild '
            'the dataset rather than casting here.' % (path, ts.dtype))
    if ts.ndim != 1:
        raise ConfigError('signal ts in %s must be 1-D, got shape %s' % (path, ts.shape))
    if values.ndim != 2 or values.shape[1] != 4:
        raise ConfigError(
            'signal values in %s must be (N, 4) = bid_px, bid_qty, ask_px, ask_qty; '
            'got shape %s' % (path, values.shape))
    if values.shape[0] != ts.shape[0]:
        raise ConfigError(
            'signal ts (%d rows) and values (%d rows) disagree in %s'
            % (ts.shape[0], values.shape[0], path))
    if ts.shape[0] == 0:
        raise ConfigError('signal array %s is empty' % path)
    if np.any(np.diff(ts) < 0):
        raise ConfigError(
            'signal timestamps in %s are not sorted; the causal lookup needs a '
            'non-decreasing array' % path)
    if int(ts[0]) <= 0:
        raise ConfigError(
            'signal array %s starts at ts=%d; 0 is reserved as the "no signal" '
            'sentinel and a non-positive epoch is not a real recording'
            % (path, int(ts[0])))

    return np.ascontiguousarray(ts), np.ascontiguousarray(values, dtype=np.float64)


# ---------------------------------------------------------------------------
# CLI and configuration
# ---------------------------------------------------------------------------

def build_parser():
    p = argparse.ArgumentParser(
        prog=TOOL_NAME,
        description=PHASE,
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='PartialFillExchange is offered since the partial-fill fix — '
               'see --exchange and AGENTS.md §4.6.',
    )
    p.add_argument('--manifest', required=True,
                   help='Phase-3 manifest.json describing the dataset')
    p.add_argument('--out', default=None,
                   help='write the results JSON here (default: stdout)')

    p.add_argument('--strategy', choices=('noop', 'grid'), default=None,
                   help='noop exercises the whole loop and trades nothing; '
                        'grid is a research-grade symmetric quoter')
    p.add_argument('--elapse-ms', type=float, default=None,
                   help='observation grid delta in ms (default %g); the '
                        'observation delay is 0..delta' % DEFAULT_ELAPSE_MS)
    p.add_argument('--signal-lag-ms', type=float, default=None,
                   help='shift the causal signal lookup back by this much')
    p.add_argument('--max-signal-age-ms', type=float, default=None,
                   help='override the manifest max_signal_age_ns')
    p.add_argument('--max-book-age-ms', type=float, default=None,
                   help='override the manifest max_hl_book_age_ns')
    p.add_argument('--signal-price', choices=('mid', 'microprice'), default=None,
                   help='how the strategy derives one price from a signal row')
    p.add_argument('--exchange', choices=tuple(EXCHANGE_KINDS), default=None,
                   help='exchange model: no-partial (default, %s) or partial '
                        '(%s, usable since the partial-fill fix — AGENTS.md '
                        '§4.6). Overrides what the manifest declares; the '
                        'results record the kind and where it came from'
                        % (EXCHANGE_KINDS['no-partial'],
                           EXCHANGE_KINDS['partial']))

    p.add_argument('--latency-profile', choices=tuple(LATENCY_PROFILES), default=None,
                   help='constant order latency preset (ASSUMPTION, see results)')
    p.add_argument('--entry-ms', type=float, default=None,
                   help='explicit order entry latency; use with --resp-ms')
    p.add_argument('--resp-ms', type=float, default=None,
                   help='explicit order response latency; use with --entry-ms')

    p.add_argument('--maker-fee', type=float, default=None,
                   help='maker fee as a fraction of trading value. Required '
                        'unless the manifest declares backtest_defaults.'
                        'fee_model — there is no default (see FEE_ASSUMPTION)')
    p.add_argument('--taker-fee', type=float, default=None,
                   help='taker fee as a fraction of trading value; same rule')
    p.add_argument('--tick-size', type=float, default=None,
                   help='override the manifest tick size')
    p.add_argument('--lot-size', type=float, default=None,
                   help='override the manifest lot size')

    p.add_argument('--hl-price-sig-figs', type=int, default=DEFAULT_HL_SIG_FIGS,
                   help='Hyperliquid significant-figure limit for quotes '
                        '(0 disables)')
    p.add_argument('--hl-max-decimals', type=int, default=None,
                   help='decimal-place limit for quotes; default 6 - szDecimals '
                        'from the manifest, -1 disables')

    p.add_argument('--grid-levels', type=int, default=3)
    p.add_argument('--grid-half-spread-ticks', type=float, default=5.0)
    p.add_argument('--grid-interval-ticks', type=float, default=5.0)
    p.add_argument('--grid-order-qty', type=float, default=None,
                   help='default: one lot')
    p.add_argument('--grid-max-position', type=float, default=None,
                   help='default: levels x order qty')
    p.add_argument('--grid-signal-weight', type=float, default=0.5,
                   help='fraction of (signal price - HL mid) the quote centre '
                        'follows; research-grade placeholder, not an alpha')
    p.add_argument('--grid-max-shift-ticks', type=float, default=10.0,
                   help='clamp on that shift, in ticks')

    p.add_argument('--max-steps', type=int, default=0,
                   help='stop after this many elapse steps (0: run to the end)')
    p.add_argument('--sweep', default=None, metavar='KNOB=V1,V2,...',
                   help='re-run varying exactly one knob; known knobs: %s'
                        % ', '.join(sorted(SWEEP_KNOBS)))
    p.add_argument('--compare-manifest', default=None, metavar='MANIFEST',
                   help='the doc\'s other sensitivity family: run this same '
                        'configuration against a second dataset built with the '
                        'other book_mode, and compare the two the way --sweep '
                        'does. Needs --out.')
    return p


def _ms_to_ns(value):
    return int(round(float(value) * 1e6))


def _resolve(cli_value, manifest_value, default):
    if cli_value is not None:
        return cli_value, 'cli'
    if manifest_value is not None:
        return manifest_value, 'manifest'
    return default, 'default'


def require_positive_latency(entry_ns, resp_ns):
    """Order latency must be explicit and non-zero (doc, Phase 4).

    Factored out so the sweep goes through it too: it re-derived these fields
    itself and would happily have fed ``entry_ms=0`` or ``-25`` to
    ``constant_order_latency`` — an order arriving before it was sent — while
    the results file stamped it with the assumption text as a considered choice.
    """
    if entry_ns <= 0 or resp_ns <= 0:
        raise ConfigError(
            'order latency must be explicit and non-zero (doc, Phase 4); got '
            'entry=%d resp=%d ns' % (entry_ns, resp_ns))


def manifest_latency_ns(manifest):
    """``backtest_defaults.constant_latency_ns`` -> ``(entry_ns, resp_ns)`` or None.

    Phase 3 writes ``{'entry': …, 'response': …}``; a 2-list is accepted as a
    hand-written alias. Anything else is a refusal, not a fall-through to the
    built-in profile: silently ignoring a declared latency is what let a
    manifest say 5/9 ms while the run used 60/120 and the provenance field
    claimed ``default``.
    """
    raw = _dig(manifest.raw, 'backtest_defaults.constant_latency_ns')
    if raw is None:
        return None
    pair = None
    if isinstance(raw, dict):
        entry, resp = raw.get('entry'), raw.get('response', raw.get('resp'))
        if entry is None and resp is None:
            return None          # the placeholder Phase 3 writes when undeclared
        pair = (entry, resp)
    elif isinstance(raw, (list, tuple)) and len(raw) == 2:
        pair = tuple(raw)
    if pair is None or any(v is None for v in pair):
        raise ConfigError(
            'manifest backtest_defaults.constant_latency_ns is %r, which is not '
            'a latency this tool can read. Write {"entry": <ns>, "response": '
            '<ns>} (what build_dataset.py --entry-latency-ms/--resp-latency-ms '
            'produces) or a two-element list. Refusing rather than falling back '
            'to a built-in profile.' % (raw,))
    try:
        entry_ns, resp_ns = _as_int(pair[0]), _as_int(pair[1])
    except ConfigError:
        raise ConfigError(
            'manifest backtest_defaults.constant_latency_ns holds %r; entry and '
            'response must be integer nanoseconds.' % (raw,)) from None
    try:
        require_positive_latency(entry_ns, resp_ns)
    except ConfigError as e:
        # Name where the number came from: "must be non-zero" alone leaves the
        # operator looking at their command line for something the manifest said.
        raise ConfigError(
            'manifest backtest_defaults.constant_latency_ns is %r: %s'
            % (raw, e)) from None
    return entry_ns, resp_ns


def check_models(manifest):
    """Refuse a manifest declaring a model this harness does not build.

    ``queue_model`` and ``exchange_kind`` were never read at all, so a manifest
    could name one thing while the run used another and nothing said so.
    """
    queue = _dig(manifest.raw, 'backtest_defaults.queue_model.kind')
    if queue is not None and queue != SUPPORTED_QUEUE_MODEL:
        raise ConfigError(
            'manifest declares queue_model %r; this harness builds %s and only '
            'that. Change the manifest or extend the harness — do not let the '
            'two disagree.' % (queue, SUPPORTED_QUEUE_MODEL))
    kind = _dig(manifest.raw, 'backtest_defaults.exchange_kind.kind')
    if kind is not None and kind not in EXCHANGE_KINDS.values():
        raise ConfigError(
            'manifest declares exchange_kind %r; this harness builds %s. Change '
            'the manifest or extend the harness — do not let the two disagree.'
            % (kind, ' or '.join(sorted(EXCHANGE_KINDS.values()))))


def manifest_exchange(manifest):
    """The ``--exchange`` spelling the manifest declares, or ``None``.

    An unknown kind returns ``None`` here and is refused by :func:`check_models`
    under its own name, which is the message an operator can act on.
    """
    kind = _dig(manifest.raw, 'backtest_defaults.exchange_kind.kind')
    for spelling, name in EXCHANGE_KINDS.items():
        if name == kind:
            return spelling
    return None


def resolve_config(args, manifest):
    """Turn CLI + manifest into one flat, fully explicit configuration.

    Every value carries its provenance in ``config['sources']`` so the results
    JSON can show that nothing was picked up silently.
    """
    src = {}
    cfg = {}

    strategy, src['strategy'] = _resolve(args.strategy, None, 'noop')
    cfg['strategy'] = strategy

    elapse_ms, src['elapse_ms'] = _resolve(args.elapse_ms, None, DEFAULT_ELAPSE_MS)
    cfg['elapse_ms'] = float(elapse_ms)
    cfg['elapse_ns'] = _ms_to_ns(elapse_ms)
    if cfg['elapse_ns'] <= 0:
        raise ConfigError('--elapse-ms must be positive')

    lag_ms, src['signal_lag_ms'] = _resolve(args.signal_lag_ms, None, 0.0)
    cfg['signal_lag_ms'] = float(lag_ms)
    cfg['signal_lag_ns'] = _ms_to_ns(lag_ms)
    if cfg['signal_lag_ns'] < 0:
        raise ConfigError('--signal-lag-ms must not be negative')

    age, src['max_signal_age_ns'] = _resolve(
        None if args.max_signal_age_ms is None else _ms_to_ns(args.max_signal_age_ms),
        manifest.max_signal_age_ns, None)
    cfg['max_signal_age_ns'] = int(age)

    book_age, src['max_book_age_ns'] = _resolve(
        None if args.max_book_age_ms is None else _ms_to_ns(args.max_book_age_ms),
        manifest.max_hl_book_age_ns, None)
    cfg['max_book_age_ns'] = int(book_age)

    if cfg['signal_lag_ns'] >= cfg['max_signal_age_ns']:
        raise ConfigError(
            'signal lag %d ns is not below max_signal_age %d ns: the guard would '
            'block on every step and the run would look like a bug rather than a '
            'refusal. Raise --max-signal-age-ms or lower the lag.'
            % (cfg['signal_lag_ns'], cfg['max_signal_age_ns']))

    tick, src['tick_size'] = _resolve(args.tick_size, manifest.tick_size, None)
    lot, src['lot_size'] = _resolve(args.lot_size, manifest.lot_size, None)
    cfg['tick_size'] = float(tick)
    cfg['lot_size'] = float(lot)
    if cfg['tick_size'] <= 0 or cfg['lot_size'] <= 0:
        raise ConfigError('tick size and lot size must both be positive')

    # Fees. No default, deliberately: the doc's first Phase-4 rule is that no
    # model is silent, and it names fees ("Комиссии — реальные HL"). Nothing in
    # this repository sources a Hyperliquid schedule, and a maker *rebate* — the
    # number this used to default to — is the most favourable assumption a
    # pure-maker strategy can be handed. So it is asked for, not invented. The
    # latency profiles keep their defaults because they are conservative and
    # carry LATENCY_ASSUMPTION; a rebate is neither.
    check_models(manifest)

    # The exchange model resolves like every other value: CLI, else what the
    # manifest declares, else the pre-fix default. Nothing is silent — the kind,
    # the spelling and the source all reach the results.
    exchange, src['exchange'] = _resolve(
        args.exchange, manifest_exchange(manifest), DEFAULT_EXCHANGE)
    cfg['exchange'] = exchange
    cfg['exchange_kind'] = EXCHANGE_KINDS[exchange]

    manifest_maker = _dig(manifest.raw, 'backtest_defaults.fee_model.maker_fee')
    manifest_taker = _dig(manifest.raw, 'backtest_defaults.fee_model.taker_fee')
    maker, src['maker_fee'] = _resolve(args.maker_fee, manifest_maker, None)
    taker, src['taker_fee'] = _resolve(args.taker_fee, manifest_taker, None)
    if maker is None or taker is None:
        raise ConfigError(
            'the fee schedule is not declared anywhere: pass --maker-fee and '
            '--taker-fee, or build the dataset with them so the manifest\'s '
            'backtest_defaults.fee_model carries them. %s There is no default: '
            'the grid strategy is a pure maker, so this number signs the PnL.'
            % FEE_ASSUMPTION)
    cfg['maker_fee'] = float(maker)
    cfg['taker_fee'] = float(taker)

    # order latency: a profile or an explicit pair, never both, never half a pair
    manifest_latency = manifest_latency_ns(manifest)
    if args.latency_profile is not None and (
            args.entry_ms is not None or args.resp_ms is not None):
        raise ConfigError(
            '--latency-profile and --entry-ms/--resp-ms are alternatives; pick one')
    if (args.entry_ms is None) != (args.resp_ms is None):
        raise ConfigError('--entry-ms and --resp-ms must be given together')
    if args.entry_ms is not None:
        cfg['latency_profile'] = None
        cfg['entry_ns'] = _ms_to_ns(args.entry_ms)
        cfg['resp_ns'] = _ms_to_ns(args.resp_ms)
        src['order_latency'] = 'cli'
    elif args.latency_profile is not None:
        cfg['latency_profile'] = args.latency_profile
        cfg['entry_ns'], cfg['resp_ns'] = latency_profile_ns(args.latency_profile)
        src['order_latency'] = 'cli'
    elif manifest_latency is not None:
        cfg['latency_profile'] = None
        cfg['entry_ns'], cfg['resp_ns'] = manifest_latency
        src['order_latency'] = 'manifest'
    else:
        cfg['latency_profile'] = DEFAULT_LATENCY_PROFILE
        cfg['entry_ns'], cfg['resp_ns'] = latency_profile_ns(DEFAULT_LATENCY_PROFILE)
        src['order_latency'] = 'default'
    cfg['entry_ms'] = cfg['entry_ns'] / 1e6
    cfg['resp_ms'] = cfg['resp_ns'] / 1e6
    require_positive_latency(cfg['entry_ns'], cfg['resp_ns'])

    price_mode, src['signal_price_mode'] = _resolve(args.signal_price, None, 'mid')
    cfg['signal_price_mode'] = price_mode
    cfg['hl_price_sig_figs'] = int(args.hl_price_sig_figs)
    if args.hl_max_decimals is not None:
        cfg['hl_max_decimals'] = int(args.hl_max_decimals)
        src['hl_max_decimals'] = 'cli'
    elif isinstance(manifest.sz_decimals, int):
        # HL perps: at most 6 - szDecimals decimal places.
        cfg['hl_max_decimals'] = 6 - int(manifest.sz_decimals)
        src['hl_max_decimals'] = 'manifest'
    else:
        cfg['hl_max_decimals'] = -1
        src['hl_max_decimals'] = 'default'

    qty, src['grid_order_qty'] = _resolve(args.grid_order_qty, None, cfg['lot_size'])
    levels = int(args.grid_levels)
    if levels < 1:
        raise ConfigError('--grid-levels must be at least 1')
    if args.grid_half_spread_ticks < 1:
        raise ConfigError('--grid-half-spread-ticks must be at least 1 tick, or a '
                          'bid and an ask could share an order id')
    max_pos, src['grid_max_position'] = _resolve(
        args.grid_max_position, None, levels * float(qty))
    cfg['grid'] = {
        'levels': levels,
        'half_spread_ticks': float(args.grid_half_spread_ticks),
        'interval_ticks': float(args.grid_interval_ticks),
        'order_qty': float(qty),
        'max_position': float(max_pos),
        'signal_weight': float(args.grid_signal_weight),
        'max_shift_ticks': float(args.grid_max_shift_ticks),
        'time_in_force': 'GTX',
        'order_type': 'LIMIT',
        'note': 'research-grade: a minimal symmetric quoter whose centre follows '
                'signal_weight x (signal price - HL mid), clamped. It exists to '
                'produce fills for sensitivity comparison, not as an alpha. '
                'Never calls modify() — cancel + resubmit only (LiveBot::modify '
                'is todo!(), AGENTS.md §4.3).',
    }

    cfg['max_steps'] = int(args.max_steps)
    if cfg['max_steps'] < 0:
        raise ConfigError('--max-steps must not be negative')
    cfg['window_start_ns'] = manifest.window_start_ns
    cfg['window_end_ns'] = manifest.window_end_ns
    cfg['clock_correction_ns'] = manifest.clock_correction_ns
    cfg['sources'] = src
    return cfg


def config_warnings(cfg):
    """Non-fatal observations worth printing and recording."""
    out = []
    if cfg['elapse_ns'] > cfg['max_signal_age_ns']:
        out.append(
            'elapse (%d ns) exceeds max_signal_age (%d ns): the observation grid, '
            'not the data, decides signal freshness'
            % (cfg['elapse_ns'], cfg['max_signal_age_ns']))
    if cfg['elapse_ns'] > cfg['max_book_age_ns']:
        out.append(
            'elapse (%d ns) exceeds max_book_age (%d ns): book age is quantised '
            'by the observation grid' % (cfg['elapse_ns'], cfg['max_book_age_ns']))
    if cfg['clock_correction_ns'] != 0:
        out.append(
            'clock_correction_ns is %d, not 0. The time policy allows it only as '
            'a deliberate manifest choice applied to both venues at once.'
            % cfg['clock_correction_ns'])
    return out


# ---------------------------------------------------------------------------
# results
# ---------------------------------------------------------------------------

@dataclass
class RunStats:
    steps: int = 0
    guard_passed: int = 0
    guard_blocked: dict = field(default_factory=dict)
    steps_outside_window: int = 0
    signal_reads: int = 0
    signal_absent: int = 0
    book_changes: int = 0
    submits: int = 0
    cancels: int = 0
    grid_level_collisions: int = 0
    fills: int = 0
    final_position: float = 0.0
    balance: float = 0.0
    fee: float = 0.0
    trading_volume: float = 0.0
    trading_value: float = 0.0
    equity: Optional[float] = None
    final_mid_price: Optional[float] = None
    first_ts: int = 0
    last_ts: int = 0
    first_guard_pass_step: int = -1
    elapsed_wall_s: float = 0.0
    end_reason: str = 'end_of_data'


def _jsonable(obj):
    """Plain JSON types only. int stays int — never widened to float."""
    if isinstance(obj, dict):
        return {str(k): _jsonable(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_jsonable(v) for v in obj]
    if isinstance(obj, (bool, str)) or obj is None:
        return obj
    if isinstance(obj, (int, np.integer)):
        return int(obj)
    if isinstance(obj, (float, np.floating)):
        value = float(obj)
        return value if math.isfinite(value) else None
    return obj


def build_results(config, manifest, stats, sweep=None):
    """The reproducibility record: every model, every parameter, every count."""
    blocked = dict(stats.guard_blocked)
    results = {
        'tool': TOOL_NAME,
        'phase': PHASE,
        'schema': RESULTS_SCHEMA,
        'generated_at': datetime.now(timezone.utc).isoformat(timespec='seconds'),
        'manifest': manifest.identity,
        'models': {
            'backtest': 'HashMapMarketDepthBacktest',
            'explicit': True,
            'asset_type': {'kind': 'LinearAsset', 'contract_size': 1.0},
            'fee_model': {
                'kind': 'TradingValueFeeModel',
                'maker_fee': config['maker_fee'],
                'taker_fee': config['taker_fee'],
                'source': config['sources']['maker_fee'],
                'assumption': FEE_ASSUMPTION,
            },
            'queue_model': {'kind': SUPPORTED_QUEUE_MODEL},
            'exchange_kind': {
                'kind': config['exchange_kind'],
                'choice': config['exchange'],
                'source': config['sources']['exchange'],
                'partial_fill_fix': PARTIAL_FILL_FIXED,
                'accepted_distortion': (
                    NO_PARTIAL_FILL_DISTORTION if config['exchange'] == 'no-partial'
                    else PARTIAL_FILL_REMAINING_GAP),
            },
            'order_latency': {
                'kind': 'ConstantLatency',
                'profile': config['latency_profile'],
                'entry_ns': config['entry_ns'],
                'resp_ns': config['resp_ns'],
                'entry_ms': config['entry_ms'],
                'resp_ms': config['resp_ms'],
                'assumption': LATENCY_ASSUMPTION,
                'profiles_ms': {k: list(v) for k, v in LATENCY_PROFILES.items()},
            },
            'tick_size': config['tick_size'],
            'lot_size': config['lot_size'],
            'latency_offset': 0,
            'clock_correction_ns': config['clock_correction_ns'],
            'last_trades_capacity': 0,
            'initial_snapshot': manifest.initial_snapshot,
            'initial_snapshot_note': (
                'absent: the first recorded day has no predecessor to build one '
                'from, so the book starts empty and the guard is what keeps the '
                'strategy out until it fills (doc §3.4)'
                if manifest.initial_snapshot is None else 'from the manifest'),
        },
        'params': {
            'strategy': config['strategy'],
            'elapse_ms': config['elapse_ms'],
            'elapse_ns': config['elapse_ns'],
            'observation_note': OBSERVATION_NOTE,
            'signal_lag_ms': config['signal_lag_ms'],
            'signal_lag_ns': config['signal_lag_ns'],
            'signal_price_mode': config['signal_price_mode'],
            'max_signal_age_ns': config['max_signal_age_ns'],
            'max_book_age_ns': config['max_book_age_ns'],
            'book_age_note': (
                'measured by the strategy from observed top-of-book changes; '
                'depth.timestamp is never written (AGENTS.md §4.7), and the '
                'observation grid quantises the age by up to one elapse step'),
            'hl_price_sig_figs': config['hl_price_sig_figs'],
            'hl_max_decimals': config['hl_max_decimals'],
            'quote_normalisation_note': (
                'quotes are rounded with the venue rule before being placed '
                '(bids down, asks up), because Hyperliquid has no fixed tick and '
                'BacktestAsset does. run.grid_level_collisions counts grid levels '
                'that collapsed onto one price under that rule — nonzero means '
                'the requested grid interval is finer than the venue allows at '
                'this price, which is exactly what live would refuse.'),
            'grid': config['grid'],
            'max_steps': config['max_steps'],
            'window_start_ns': config['window_start_ns'],
            'window_end_ns': config['window_end_ns'],
            'book_mode': manifest.book_mode,
            'num_levels': manifest.num_levels,
        },
        'sources': config['sources'],
        'warnings': config_warnings(config),
        'data': {
            'hl_npz': manifest.hl_npz,
            'signal_npz': manifest.signal_npz,
            'hl_symbol': manifest.hl_symbol,
            'signal_symbol': manifest.signal_symbol,
        },
        'run': {
            'steps': stats.steps,
            'guard_passed': stats.guard_passed,
            'guard_blocked': blocked,
            'guard_blocked_total': sum(blocked.values()),
            'steps_outside_window': stats.steps_outside_window,
            'first_guard_pass_step': stats.first_guard_pass_step,
            'signal_reads': stats.signal_reads,
            'signal_absent': stats.signal_absent,
            'book_changes': stats.book_changes,
            'submits': stats.submits,
            'cancels': stats.cancels,
            'grid_level_collisions': stats.grid_level_collisions,
            'fills': stats.fills,
            'final_position': stats.final_position,
            'balance': stats.balance,
            'fee': stats.fee,
            'trading_volume': stats.trading_volume,
            'trading_value': stats.trading_value,
            'equity': stats.equity,
            'final_mid_price': stats.final_mid_price,
            'first_ts': stats.first_ts,
            'last_ts': stats.last_ts,
            'elapsed_wall_s': stats.elapsed_wall_s,
            'end_reason': stats.end_reason,
        },
        'sweep': sweep,
    }
    return _jsonable(results)


# ---------------------------------------------------------------------------
# sensitivity sweep
# ---------------------------------------------------------------------------

#: knob -> the config fields it owns. Nothing else may move between the runs of
#: one sweep; the doc's criterion is exactly one parameter at a time.
SWEEP_KNOBS = {
    'signal-lag': ('signal_lag_ms', 'signal_lag_ns'),
    'elapse': ('elapse_ms', 'elapse_ns'),
    'maker-fee': ('maker_fee',),
    'taker-fee': ('taker_fee',),
    'entry-ms': ('entry_ms', 'entry_ns'),
    'resp-ms': ('resp_ms', 'resp_ns'),
    'latency-profile': ('latency_profile', 'entry_ms', 'entry_ns',
                        'resp_ms', 'resp_ns'),
    # The sensitivity the NoPartialFillExchange distortion note asks about, now
    # that PartialFillExchange accounts for its fills (AGENTS.md §4.6).
    'exchange': ('exchange', 'exchange_kind'),
}

_SWEEP_TYPES = {
    'signal-lag': int,
    'elapse': float,
    'maker-fee': float,
    'taker-fee': float,
    'entry-ms': float,
    'resp-ms': float,
    'latency-profile': str,
    'exchange': str,
}

SWEEP_METRICS = (
    'steps', 'guard_passed', 'guard_blocked_total', 'signal_absent',
    'submits', 'cancels', 'fills', 'final_position', 'equity',
)


def parse_sweep_spec(spec):
    """``'signal-lag=0,10,50'`` -> ``('signal-lag', [0, 10, 50])``."""
    if '=' not in spec:
        raise ConfigError('--sweep must look like KNOB=V1,V2,...; got %r' % spec)
    knob, _, rest = spec.partition('=')
    knob = knob.strip()
    if knob not in SWEEP_KNOBS:
        raise ConfigError(
            'unknown sweep knob %r; known: %s. book_mode/num_levels is the doc\'s '
            'other sensitivity family and needs two datasets, so it has its own '
            'mode: --compare-manifest <the other book_mode\'s manifest>.'
            % (knob, ', '.join(sorted(SWEEP_KNOBS))))
    raw = [v.strip() for v in rest.split(',') if v.strip() != '']
    if not raw:
        raise ConfigError('--sweep %r lists no values' % spec)
    cast = _SWEEP_TYPES[knob]
    try:
        values = [cast(v) for v in raw]
    except ValueError as e:
        raise ConfigError('--sweep %r: %s' % (spec, e)) from None
    return knob, values


def sweep_configs(base_config, knob, values):
    """One configuration per value, differing in that knob and nothing else."""
    if knob not in SWEEP_KNOBS:
        raise ConfigError('unknown sweep knob %r' % knob)
    out = []
    for value in values:
        cfg = copy.deepcopy(base_config)
        if knob == 'signal-lag':
            cfg['signal_lag_ms'] = float(value)
            cfg['signal_lag_ns'] = _ms_to_ns(value)
            if cfg['signal_lag_ns'] >= cfg['max_signal_age_ns']:
                raise ConfigError(
                    'sweep value signal-lag=%s is not below max_signal_age %d ns: '
                    'that run would block on every step'
                    % (value, cfg['max_signal_age_ns']))
        elif knob == 'elapse':
            cfg['elapse_ms'] = float(value)
            cfg['elapse_ns'] = _ms_to_ns(value)
            if cfg['elapse_ns'] <= 0:
                raise ConfigError('sweep value elapse=%s must be positive' % value)
        elif knob == 'maker-fee':
            cfg['maker_fee'] = float(value)
        elif knob == 'taker-fee':
            cfg['taker_fee'] = float(value)
        elif knob == 'entry-ms':
            cfg['entry_ms'] = float(value)
            cfg['entry_ns'] = _ms_to_ns(value)
            require_positive_latency(cfg['entry_ns'], cfg['resp_ns'])
        elif knob == 'resp-ms':
            cfg['resp_ms'] = float(value)
            cfg['resp_ns'] = _ms_to_ns(value)
            require_positive_latency(cfg['entry_ns'], cfg['resp_ns'])
        elif knob == 'exchange':
            if str(value) not in EXCHANGE_KINDS:
                raise ConfigError(
                    'sweep value exchange=%s is not one of %s'
                    % (value, ', '.join(sorted(EXCHANGE_KINDS))))
            cfg['exchange'] = str(value)
            cfg['exchange_kind'] = EXCHANGE_KINDS[str(value)]
        else:  # latency-profile
            cfg['latency_profile'] = str(value)
            cfg['entry_ns'], cfg['resp_ns'] = latency_profile_ns(str(value))
            cfg['entry_ms'] = cfg['entry_ns'] / 1e6
            cfg['resp_ms'] = cfg['resp_ns'] / 1e6
            require_positive_latency(cfg['entry_ns'], cfg['resp_ns'])
        for f in SWEEP_KNOBS[knob]:
            cfg['sources'][f] = 'sweep'
        cfg['sources']['order_latency'] = (
            'sweep' if knob in ('entry-ms', 'resp-ms', 'latency-profile')
            else base_config['sources']['order_latency'])
        out.append(cfg)
    return out


def _fmt_value(value):
    if isinstance(value, bool):
        return str(value)
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        return repr(value)
    return str(value)


def sweep_output_path(out, knob, value):
    root, ext = os.path.splitext(out)
    return '%s__%s=%s%s' % (root, knob, _fmt_value(value), ext or '.json')


def sweep_summary_path(out):
    root, ext = os.path.splitext(out)
    return '%s__sweep-summary%s' % (root, ext or '.json')


#: Two runs of an *identical* configuration were measured to differ in the 15th
#: significant digit of balance (the engine iterates its order HashMap in an
#: unspecified order, which reorders the additions). Comparing floats bitwise
#: would therefore report sensitivity that does not exist, so the sweep uses a
#: tolerance and names the metrics that only moved inside it.
SWEEP_FLOAT_REL_TOL = 1e-9
SWEEP_FLOAT_ABS_TOL = 1e-12

NONDETERMINISM_NOTE = (
    'Floats are compared with rel_tol=%g / abs_tol=%g. Measured on this engine: '
    'two runs of one identical configuration differed in the last bits of '
    'balance, so exact equality is not available and a sub-tolerance move is '
    'noise, not a result.' % (SWEEP_FLOAT_REL_TOL, SWEEP_FLOAT_ABS_TOL)
)


def _metric_movement(values):
    """(moved, near_identical) for one metric across the runs of a sweep."""
    first = values[0]
    exact = all(v == first for v in values)
    if exact:
        return False, False
    if all(isinstance(v, float) for v in values):
        close = all(math.isclose(v, first, rel_tol=SWEEP_FLOAT_REL_TOL,
                                 abs_tol=SWEEP_FLOAT_ABS_TOL) for v in values)
        return (not close), close
    return True, False


def summarize_sweep(knob, runs):
    """Compare the runs metric by metric. Identical is a legitimate outcome."""
    rows = []
    for r in runs:
        row = {'value': r['value']}
        for m in SWEEP_METRICS:
            row[m] = r['run'].get(m)
        rows.append(row)
    moved = []
    near = []
    for m in SWEEP_METRICS:
        did_move, is_near = _metric_movement([row[m] for row in rows])
        if did_move:
            moved.append(m)
        elif is_near:
            near.append(m)
    return {
        'knob': knob,
        'values': [r['value'] for r in runs],
        'metrics': list(SWEEP_METRICS),
        'rows': rows,
        'moved': moved,
        'near_identical': near,
        'identical': not moved,
        'float_rel_tol': SWEEP_FLOAT_REL_TOL,
        'float_abs_tol': SWEEP_FLOAT_ABS_TOL,
        'nondeterminism': NONDETERMINISM_NOTE,
        'criterion': (
            'The doc\'s criterion is not "distinguishable results": runs must be '
            'reproducible, exactly one parameter may move, and the effect must be '
            'written down. No effect is a result.'),
    }


def format_sweep_table(summary):
    cols = ['value'] + list(summary['metrics'])
    rows = [[_fmt_value(r[c]) if r[c] is not None else '-' for c in cols]
            for r in summary['rows']]
    widths = [max(len(c), *(len(r[i]) for r in rows)) if rows else len(c)
              for i, c in enumerate(cols)]
    line = '  '.join(c.ljust(w) for c, w in zip(cols, widths))
    out = ['sweep: %s' % summary['knob'], line, '-' * len(line)]
    out += ['  '.join(v.ljust(w) for v, w in zip(r, widths)) for r in rows]
    out.append('')
    if summary['identical']:
        out.append('NO DIFFERENCE across %s=%s on any compared metric.'
                   % (summary['knob'],
                      ','.join(_fmt_value(v) for v in summary['values'])))
        out.append('That is a valid result, and the doc asks for it to be '
                   'recorded as one rather than treated as a failed experiment.')
    else:
        out.append('moved: %s' % ', '.join(summary['moved']))
    if summary['near_identical']:
        out.append('within tolerance (rel_tol=%g), i.e. numerical noise, not an '
                   'effect: %s' % (summary['float_rel_tol'],
                                   ', '.join(summary['near_identical'])))
        out.append(NONDETERMINISM_NOTE)
    return '\n'.join(out)


# ---------------------------------------------------------------------------
# the backtest itself — nothing above this line imports hftbacktest or numba
# ---------------------------------------------------------------------------

def _build_loops():
    """Compile the strategy loops. Imports numba lazily, on purpose."""
    from numba import njit, uint64, float64
    from numba.typed import Dict

    from hftbacktest import BUY, SELL, GTX, LIMIT

    guard_nb = njit(guard_block_reason)
    sigidx_nb = njit(signal_index)
    sigpx_nb = njit(signal_price)
    norm_nb = njit(normalize_price)

    @njit
    def noop_loop(hbt, sig_ts, sig_val, elapse_ns, lag_ns, use_micro,
                  win_start, win_end, max_book_age, max_sig_age, max_steps,
                  si, sf):
        asset_no = 0
        prev_bid_tick = INVALID_MIN
        prev_ask_tick = INVALID_MAX
        prev_bid_qty = -1.0
        prev_ask_qty = -1.0
        last_book_change = 0
        steps = 0
        end_reason = END_OF_DATA
        while hbt.elapse(elapse_ns) == 0:
            steps += 1
            now = hbt.current_timestamp
            if steps == 1:
                si[IST_FIRST_TS] = now
            si[IST_LAST_TS] = now

            depth = hbt.depth(asset_no)
            bid_tick = depth.best_bid_tick
            ask_tick = depth.best_ask_tick
            bid_qty = depth.best_bid_qty
            ask_qty = depth.best_ask_qty
            if (bid_tick != prev_bid_tick or ask_tick != prev_ask_tick
                    or bid_qty != prev_bid_qty or ask_qty != prev_ask_qty):
                last_book_change = now
                si[IST_BOOK_CHANGES] += 1
                prev_bid_tick = bid_tick
                prev_ask_tick = ask_tick
                prev_bid_qty = bid_qty
                prev_ask_qty = ask_qty

            if now > win_end:
                # Past the window end is outside the window; counting it keeps
                # blocked + passed + outside == steps exactly.
                si[IST_OUTSIDE_WINDOW] += 1
                end_reason = END_WINDOW
                break
            if now < win_start:
                si[IST_OUTSIDE_WINDOW] += 1
                if max_steps > 0 and steps >= max_steps:
                    end_reason = END_MAX_STEPS
                    break
                continue

            idx = sigidx_nb(sig_ts, now, lag_ns)
            if idx < 0:
                si[IST_SIGNAL_ABSENT] += 1
                signal_ts = SIGNAL_TS_ABSENT
            else:
                si[IST_SIGNAL_READS] += 1
                signal_ts = sig_ts[idx]
                sf[FST_LAST_SIGNAL_PX] = sigpx_nb(
                    sig_val[idx, 0], sig_val[idx, 1],
                    sig_val[idx, 2], sig_val[idx, 3], use_micro)

            reason = guard_nb(bid_tick, ask_tick, INVALID_MIN, INVALID_MAX,
                              last_book_change, now, max_book_age,
                              signal_ts, max_sig_age)
            if reason == GUARD_OK:
                si[IST_GUARD_PASS] += 1
                if si[IST_FIRST_PASS_STEP] < 0:
                    si[IST_FIRST_PASS_STEP] = steps
            elif reason == GUARD_NO_BID:
                si[IST_BLK_NO_BID] += 1
            elif reason == GUARD_NO_ASK:
                si[IST_BLK_NO_ASK] += 1
            elif reason == GUARD_CROSSED_OR_LOCKED:
                si[IST_BLK_CROSSED] += 1
            elif reason == GUARD_BOOK_STALE:
                si[IST_BLK_BOOK_STALE] += 1
            else:
                si[IST_BLK_SIGNAL_STALE] += 1

            if max_steps > 0 and steps >= max_steps:
                end_reason = END_MAX_STEPS
                break

        si[IST_STEPS] = steps
        si[IST_END_REASON] = end_reason
        _finalize(hbt, si, sf, asset_no)
        return end_reason

    @njit
    def _finalize(hbt, si, sf, asset_no):
        state = hbt.state_values(asset_no)
        sf[FST_POSITION] = state.position
        sf[FST_BALANCE] = state.balance
        sf[FST_FEE] = state.fee
        sf[FST_TRADING_VOLUME] = state.trading_volume
        sf[FST_TRADING_VALUE] = state.trading_value
        si[IST_FILLS] = state.num_trades
        depth = hbt.depth(asset_no)
        if depth.best_bid_tick != INVALID_MIN and depth.best_ask_tick != INVALID_MAX:
            sf[FST_FINAL_MID] = (depth.best_bid + depth.best_ask) / 2.0
        else:
            sf[FST_FINAL_MID] = np.nan

    @njit
    def grid_loop(hbt, sig_ts, sig_val, elapse_ns, lag_ns, use_micro,
                  win_start, win_end, max_book_age, max_sig_age, max_steps,
                  tick_size, levels, half_spread_ticks, interval_ticks,
                  order_qty, max_position, signal_weight, max_shift_ticks,
                  sig_figs, max_decimals, si, sf):
        asset_no = 0
        prev_bid_tick = INVALID_MIN
        prev_ask_tick = INVALID_MAX
        prev_bid_qty = -1.0
        prev_ask_qty = -1.0
        last_book_change = 0
        steps = 0
        end_reason = END_OF_DATA
        while hbt.elapse(elapse_ns) == 0:
            steps += 1
            now = hbt.current_timestamp
            if steps == 1:
                si[IST_FIRST_TS] = now
            si[IST_LAST_TS] = now
            hbt.clear_inactive_orders(asset_no)

            depth = hbt.depth(asset_no)
            bid_tick = depth.best_bid_tick
            ask_tick = depth.best_ask_tick
            bid_qty = depth.best_bid_qty
            ask_qty = depth.best_ask_qty
            if (bid_tick != prev_bid_tick or ask_tick != prev_ask_tick
                    or bid_qty != prev_bid_qty or ask_qty != prev_ask_qty):
                last_book_change = now
                si[IST_BOOK_CHANGES] += 1
                prev_bid_tick = bid_tick
                prev_ask_tick = ask_tick
                prev_bid_qty = bid_qty
                prev_ask_qty = ask_qty

            if now > win_end:
                # Past the window end is outside the window; counting it keeps
                # blocked + passed + outside == steps exactly.
                si[IST_OUTSIDE_WINDOW] += 1
                end_reason = END_WINDOW
                break
            if now < win_start:
                si[IST_OUTSIDE_WINDOW] += 1
                if max_steps > 0 and steps >= max_steps:
                    end_reason = END_MAX_STEPS
                    break
                continue

            idx = sigidx_nb(sig_ts, now, lag_ns)
            signal_px = 0.0
            if idx < 0:
                si[IST_SIGNAL_ABSENT] += 1
                signal_ts = SIGNAL_TS_ABSENT
            else:
                si[IST_SIGNAL_READS] += 1
                signal_ts = sig_ts[idx]
                signal_px = sigpx_nb(sig_val[idx, 0], sig_val[idx, 1],
                                     sig_val[idx, 2], sig_val[idx, 3], use_micro)
                sf[FST_LAST_SIGNAL_PX] = signal_px

            reason = guard_nb(bid_tick, ask_tick, INVALID_MIN, INVALID_MAX,
                              last_book_change, now, max_book_age,
                              signal_ts, max_sig_age)
            if reason != GUARD_OK:
                if reason == GUARD_NO_BID:
                    si[IST_BLK_NO_BID] += 1
                elif reason == GUARD_NO_ASK:
                    si[IST_BLK_NO_ASK] += 1
                elif reason == GUARD_CROSSED_OR_LOCKED:
                    si[IST_BLK_CROSSED] += 1
                elif reason == GUARD_BOOK_STALE:
                    si[IST_BLK_BOOK_STALE] += 1
                else:
                    si[IST_BLK_SIGNAL_STALE] += 1
                # Fail closed: blocked means out of the market, not "leave the
                # resting quotes and hope". Withdraw everything cancellable.
                blocked_orders = hbt.orders(asset_no)
                blocked_values = blocked_orders.values()
                while blocked_values.has_next():
                    order = blocked_values.get()
                    if order.cancellable:
                        hbt.cancel(asset_no, order.order_id, False)
                        si[IST_CANCELS] += 1
                if max_steps > 0 and steps >= max_steps:
                    end_reason = END_MAX_STEPS
                    break
                continue

            si[IST_GUARD_PASS] += 1
            if si[IST_FIRST_PASS_STEP] < 0:
                si[IST_FIRST_PASS_STEP] = steps

            mid = (depth.best_bid + depth.best_ask) / 2.0
            shift = 0.0
            if idx >= 0:
                shift = signal_weight * (signal_px - mid)
                limit = max_shift_ticks * tick_size
                if shift > limit:
                    shift = limit
                elif shift < -limit:
                    shift = -limit
            centre = mid + shift
            half = half_spread_ticks * tick_size
            step = interval_ticks * tick_size
            position = hbt.position(asset_no)

            new_bids = Dict.empty(uint64, float64)
            if position < max_position:
                price = centre - half
                last_tick = -1
                for _ in range(levels):
                    px = norm_nb(price, tick_size, sig_figs, max_decimals, True)
                    if px > 0.0 and px < depth.best_ask:
                        tick = int(round(px / tick_size))
                        if tick == last_tick:
                            si[IST_LEVEL_COLLISIONS] += 1
                        last_tick = tick
                        new_bids[uint64(tick)] = px
                    price -= step

            new_asks = Dict.empty(uint64, float64)
            if position > -max_position:
                price = centre + half
                last_tick = -1
                for _ in range(levels):
                    px = norm_nb(price, tick_size, sig_figs, max_decimals, False)
                    if px > 0.0 and px > depth.best_bid:
                        tick = int(round(px / tick_size))
                        if tick == last_tick:
                            si[IST_LEVEL_COLLISIONS] += 1
                        last_tick = tick
                        new_asks[uint64(tick)] = px
                    price += step

            orders = hbt.orders(asset_no)
            order_values = orders.values()
            while order_values.has_next():
                order = order_values.get()
                if order.cancellable:
                    if ((order.side == BUY and order.order_id not in new_bids)
                            or (order.side == SELL and order.order_id not in new_asks)):
                        hbt.cancel(asset_no, order.order_id, False)
                        si[IST_CANCELS] += 1

            for order_id, price in new_bids.items():
                if order_id not in orders:
                    hbt.submit_buy_order(asset_no, order_id, price, order_qty,
                                         GTX, LIMIT, False)
                    si[IST_SUBMITS] += 1
            for order_id, price in new_asks.items():
                if order_id not in orders:
                    hbt.submit_sell_order(asset_no, order_id, price, order_qty,
                                          GTX, LIMIT, False)
                    si[IST_SUBMITS] += 1

            if max_steps > 0 and steps >= max_steps:
                end_reason = END_MAX_STEPS
                break

        si[IST_STEPS] = steps
        si[IST_END_REASON] = end_reason
        _finalize(hbt, si, sf, asset_no)
        return end_reason

    return {'noop': noop_loop, 'grid': grid_loop}


_LOOPS = None


def apply_exchange_model(asset, exchange):
    """Set the exchange model on a ``BacktestAsset``.

    Separate from :func:`native_runner` so the choice is testable without the
    native module: this one line is the whole behavioural difference between
    ``--exchange no-partial`` and ``--exchange partial``.
    """
    if exchange == 'partial':
        asset.partial_fill_exchange()
    else:
        asset.no_partial_fill_exchange()
    return asset


def native_runner(config, manifest, signal):
    """Build the asset with every model set explicitly, then run the strategy."""
    global _LOOPS
    from hftbacktest import BacktestAsset, HashMapMarketDepthBacktest

    ts, values = signal

    asset = BacktestAsset()
    asset.data(list(manifest.hl_npz))
    if manifest.initial_snapshot is not None:
        asset.initial_snapshot(manifest.initial_snapshot)
    # Every one of these is also the constructor default, and every one is set
    # anyway: the doc forbids a silent default even where it agrees with us.
    asset.linear_asset(1.0)
    asset.tick_size(config['tick_size'])
    asset.lot_size(config['lot_size'])
    asset.trading_value_fee_model(config['maker_fee'], config['taker_fee'])
    asset.log_prob_queue_model2()
    apply_exchange_model(asset, config['exchange'])
    asset.last_trades_capacity(0)
    asset.latency_offset(0)
    # constant_order_latency is the non-deprecated spelling of constant_latency;
    # both build LatencyModel::ConstantLatency (py-hftbacktest/src/lib.rs:241,271).
    if hasattr(asset, 'constant_order_latency'):
        asset.constant_order_latency(config['entry_ns'], config['resp_ns'])
    else:
        asset.constant_latency(config['entry_ns'], config['resp_ns'])

    hbt = HashMapMarketDepthBacktest([asset])

    if _LOOPS is None:
        _LOOPS = _build_loops()

    si = np.zeros(N_ISTAT, dtype=np.int64)
    si[IST_FIRST_PASS_STEP] = -1
    sf = np.zeros(N_FSTAT, dtype=np.float64)
    use_micro = config['signal_price_mode'] == 'microprice'

    started = time.perf_counter()
    try:
        if config['strategy'] == 'noop':
            _LOOPS['noop'](
                hbt, ts, values,
                np.int64(config['elapse_ns']), np.int64(config['signal_lag_ns']),
                use_micro,
                np.int64(config['window_start_ns']), np.int64(config['window_end_ns']),
                np.int64(config['max_book_age_ns']), np.int64(config['max_signal_age_ns']),
                np.int64(config['max_steps']), si, sf)
        else:
            g = config['grid']
            _LOOPS['grid'](
                hbt, ts, values,
                np.int64(config['elapse_ns']), np.int64(config['signal_lag_ns']),
                use_micro,
                np.int64(config['window_start_ns']), np.int64(config['window_end_ns']),
                np.int64(config['max_book_age_ns']), np.int64(config['max_signal_age_ns']),
                np.int64(config['max_steps']),
                float(config['tick_size']), np.int64(g['levels']),
                float(g['half_spread_ticks']), float(g['interval_ticks']),
                float(g['order_qty']), float(g['max_position']),
                float(g['signal_weight']), float(g['max_shift_ticks']),
                np.int64(config['hl_price_sig_figs']),
                np.int64(config['hl_max_decimals']), si, sf)
    finally:
        elapsed = time.perf_counter() - started
        hbt.close()

    mid = float(sf[FST_FINAL_MID])
    position = float(sf[FST_POSITION])
    balance = float(sf[FST_BALANCE])
    fee = float(sf[FST_FEE])
    equity = balance + position * mid - fee if math.isfinite(mid) else None

    return RunStats(
        steps=int(si[IST_STEPS]),
        guard_passed=int(si[IST_GUARD_PASS]),
        guard_blocked={
            'no_bid': int(si[IST_BLK_NO_BID]),
            'no_ask': int(si[IST_BLK_NO_ASK]),
            'crossed_or_locked': int(si[IST_BLK_CROSSED]),
            'book_stale': int(si[IST_BLK_BOOK_STALE]),
            'signal_stale': int(si[IST_BLK_SIGNAL_STALE]),
        },
        steps_outside_window=int(si[IST_OUTSIDE_WINDOW]),
        signal_reads=int(si[IST_SIGNAL_READS]),
        signal_absent=int(si[IST_SIGNAL_ABSENT]),
        book_changes=int(si[IST_BOOK_CHANGES]),
        submits=int(si[IST_SUBMITS]),
        cancels=int(si[IST_CANCELS]),
        grid_level_collisions=int(si[IST_LEVEL_COLLISIONS]),
        fills=int(si[IST_FILLS]),
        final_position=position,
        balance=balance,
        fee=fee,
        trading_volume=float(sf[FST_TRADING_VOLUME]),
        trading_value=float(sf[FST_TRADING_VALUE]),
        equity=equity,
        final_mid_price=mid if math.isfinite(mid) else None,
        first_ts=int(si[IST_FIRST_TS]),
        last_ts=int(si[IST_LAST_TS]),
        first_guard_pass_step=int(si[IST_FIRST_PASS_STEP]),
        elapsed_wall_s=elapsed,
        end_reason=END_REASON_NAMES[int(si[IST_END_REASON])],
    )


# ---------------------------------------------------------------------------
# drivers
# ---------------------------------------------------------------------------

def _write_json(path, payload):
    if path is None:
        json.dump(payload, sys.stdout, indent=2)
        sys.stdout.write('\n')
        return
    directory = os.path.dirname(os.path.abspath(path))
    if directory:
        os.makedirs(directory, exist_ok=True)
    with open(path, 'w') as f:
        json.dump(payload, f, indent=2)
        f.write('\n')


def _parse(manifest_path, argv):
    return build_parser().parse_args(['--manifest', str(manifest_path)] + list(argv or []))


def _execute(config, manifest, signal, runner):
    for w in config_warnings(config):
        print('warning: %s' % w, file=sys.stderr)
    return (runner or native_runner)(config, manifest, signal)


def check_signal_covers_the_window(signal, manifest, config):
    """Mode A: "сигнал обязан покрывать всё торговое окно".

    A leading stretch with no signal row cannot be traded at all — every step
    there blocks on ``signal_stale`` — so a gap wider than ``max_signal_age`` is
    a broken dataset, not a warning to scroll past. Phase 3 trims both inputs to
    the intersection, so the only ways to get here are a hand-edited window or a
    report whose coverage did not describe this symbol. A gap *within* the age
    bound is normal: the window opens where the traded venue's data does, and
    the first signal row lands a few milliseconds later.
    """
    lead_ns = int(signal[0][0]) - manifest.window_start_ns
    if lead_ns <= 0:
        return
    if lead_ns > config['max_signal_age_ns']:
        raise ConfigError(
            'the signal starts %d ns after the window does, more than '
            'max_signal_age (%d ns): the first %.3f s of the run has no signal '
            'at all and every step there would block. The window must be the '
            'intersection of the two instruments coverage (Phase 3, §3.1) — '
            'rebuild the dataset rather than running over a hole.'
            % (lead_ns, config['max_signal_age_ns'], lead_ns / 1e9))
    print('warning: the signal starts %d ns after the window does; those steps '
          'block on an absent signal' % lead_ns, file=sys.stderr)


def _run_once_from_args(args, runner=None):
    manifest = load_manifest(args.manifest)
    config = resolve_config(args, manifest)
    signal = load_signal(manifest.signal_npz)
    check_signal_covers_the_window(signal, manifest, config)
    stats = _execute(config, manifest, signal, runner)
    results = build_results(config, manifest, stats)
    _write_json(args.out, results)
    return results


#: Fields two manifests of a `book_mode` comparison must agree on. Everything
#: not in this list is either allowed to differ (the book_mode pair itself, the
#: output paths, hashes and timestamps) or is not load-bearing for the
#: comparison. The doc's criterion is "между прогонами меняется ровно один
#: параметр", and two independently built datasets are exactly where that stops
#: being self-evident.
COMPARE_MUST_MATCH = (
    'window.start_ns', 'window.end_ns', 'clock_correction_ns',
    'tick_size', 'lot_size',
    'instruments.execution.symbol', 'instruments.signal.symbol',
    'outputs.signal.rows',
)

#: Allowed to differ, and held at the base manifest's value for both runs so
#: that only book_mode moves. `BOOK_MODE_MAX_AGE_MS` in the builder pairs a
#: staleness bound with the cadence, so the two manifests will normally disagree
#: here; the comparison names it instead of letting it move silently.
COMPARE_HELD_FIXED = ('max_hl_book_age_ns', 'max_signal_age_ns')


def compare_manifests(base, other):
    """Check that two datasets differ in `book_mode` and nothing that matters."""
    if base.book_mode == other.book_mode:
        raise ConfigError(
            'both manifests are book_mode=%r. The comparison exists to measure '
            'slow/20 against fast/5; two runs of the same cadence measure '
            'nothing.' % (base.book_mode,))
    differ = []
    for key in COMPARE_MUST_MATCH:
        a, b = _dig(base.raw, key), _dig(other.raw, key)
        if a != b:
            differ.append('%s: %r vs %r' % (key, a, b))
    if differ:
        raise ConfigError(
            'the two manifests differ in more than book_mode, so a difference '
            'in the results would not be attributable to it:\n  %s'
            % '\n  '.join(differ))
    held = {}
    for key in COMPARE_HELD_FIXED:
        a, b = getattr(base, key, None), getattr(other, key, None)
        if a != b:
            held[key] = {'base': a, 'other': b, 'used': a}
    return held


def run_compare(base_manifest, other_manifest, argv=None, runner=None):
    """The doc's second obligatory sensitivity family: `book_mode` slow vs fast.

    It cannot be a `--sweep` knob because it needs two datasets — but running
    the tool twice by hand gives no NO-DIFFERENCE statement and no guarantee
    that only that pair moved, which is the machinery the criterion depends on.
    So: one configuration, two manifests, the same comparison table the sweep
    produces.
    """
    args = _parse(base_manifest, argv)
    if not args.out:
        raise ConfigError('--compare-manifest needs --out')
    base = load_manifest(base_manifest)
    other = load_manifest(other_manifest)
    held = compare_manifests(base, other)

    # ONE configuration for both runs: resolved from the base manifest, so the
    # bounds that the builder pairs with book_mode do not move underneath.
    config = resolve_config(args, base)

    runs = []
    for manifest in (base, other):
        signal = load_signal(manifest.signal_npz)
        check_signal_covers_the_window(signal, manifest, config)
        stats = _execute(config, manifest, signal, runner)
        results = build_results(config, manifest, stats,
                                sweep={'knob': 'book_mode',
                                       'value': manifest.book_mode,
                                       'values': [base.book_mode, other.book_mode]})
        path = sweep_output_path(args.out, 'book_mode', manifest.book_mode)
        _write_json(path, results)
        runs.append({'value': manifest.book_mode, 'path': path,
                     'run': results['run']})

    summary = summarize_sweep('book_mode', runs)
    summary['manifests'] = [base.identity, other.identity]
    summary['num_levels'] = [base.num_levels, other.num_levels]
    summary['held_fixed'] = held
    summary['runs'] = [{'value': r['value'], 'path': r['path']} for r in runs]
    summary['criterion'] = summary['criterion'] + (
        ' book_mode needs two datasets, so the configuration is resolved once '
        'from the first manifest and used for both runs; anything the two '
        'manifests disagree on is either refused or listed under held_fixed.')
    _write_json(args.out, summary)
    print(format_sweep_table(summary))
    return summary


def _run_sweep_from_args(args, runner=None):
    if not args.out:
        raise ConfigError('--sweep needs --out: it writes one results file per run')
    knob, values = parse_sweep_spec(args.sweep)
    manifest = load_manifest(args.manifest)
    base = resolve_config(args, manifest)
    configs = sweep_configs(base, knob, values)
    signal = load_signal(manifest.signal_npz)
    check_signal_covers_the_window(signal, manifest, base)

    runs = []
    for value, config in zip(values, configs):
        stats = _execute(config, manifest, signal, runner)
        results = build_results(config, manifest, stats,
                                sweep={'knob': knob, 'value': value,
                                       'values': list(values)})
        path = sweep_output_path(args.out, knob, value)
        _write_json(path, results)
        runs.append({'value': value, 'path': path, 'run': results['run']})

    summary = summarize_sweep(knob, runs)
    summary['manifest'] = manifest.identity
    summary['runs'] = [{'value': r['value'], 'path': r['path']} for r in runs]
    _write_json(sweep_summary_path(args.out), summary)
    print(format_sweep_table(summary))
    return summary


def run_once(manifest_path, argv=None, runner=None):
    """Single run. ``argv`` holds the flags other than ``--manifest``."""
    return _run_once_from_args(_parse(manifest_path, argv), runner=runner)


def run_sweep(manifest_path, argv=None, runner=None):
    """Sensitivity run: same configuration N times, one knob moving."""
    return _run_sweep_from_args(_parse(manifest_path, argv), runner=runner)


def main(argv=None):
    args = build_parser().parse_args(argv)
    try:
        if args.sweep and args.compare_manifest:
            raise ConfigError(
                '--sweep and --compare-manifest are alternatives: each varies '
                'exactly one thing, and together they would vary two')
        if args.compare_manifest:
            run_compare(args.manifest, args.compare_manifest,
                        _argv_without_manifest(argv))
        elif args.sweep:
            _run_sweep_from_args(args)
        else:
            _run_once_from_args(args)
    except ConfigError as e:
        print('error: %s' % e, file=sys.stderr)
        return 1
    return 0


def _argv_without_manifest(argv):
    """`run_compare` re-parses, and `_parse` puts `--manifest` back itself."""
    argv = list(sys.argv[1:] if argv is None else argv)
    out = []
    skip = False
    for i, item in enumerate(argv):
        if skip:
            skip = False
            continue
        if item == '--manifest':
            skip = True
            continue
        if item.startswith('--manifest='):
            continue
        out.append(item)
    return out


if __name__ == '__main__':
    sys.exit(main())
