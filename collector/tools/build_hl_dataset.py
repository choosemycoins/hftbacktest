#!/usr/bin/env python3
"""Hyperliquid-execution-only dataset builder — profile ``hl-execution-only-v1``.

``build_dataset.py`` assembles the mode-A dataset of
``docs/design-multi-venue-collection.md``: a Hyperliquid instrument to trade
**and** a ``binancefuturesum`` signal array to read. That pairing costs a day on
which the signal recording is broken — the Hyperliquid side can be perfect and
there is still no dataset, because the gate is the whole report and the window is
the intersection with a feed nothing here trades.

This builds the other half: the executing venue alone. No signal array, no
intersection, and the gate is Hyperliquid's verdict — the signal venue's is
recorded and never enforced, exactly as ``build_dataset.py`` treats a secondary
signal recording. Everything else is the same code: the same time-policy scan
before conversion, the same measured tick, the same ``convert_hl_day``, so the
two builders cannot drift apart about what a Hyperliquid day is.

Ported from ``myhft``'s ``scripts/build_hl_dataset.py``, which produced the
``hl-execution-dataset-v1`` artefacts of 2026-07-30 and belongs in the fork: it
already imported this directory's ``build_dataset.py`` for every step that
matters. Two deliberate changes:

1. **The tick is measured** (``build_dataset.reconcile_tick``). The ported
   version registered ``10^-(6 - szDecimals)``, which is only the lower bound of
   the Hyperliquid tick; on 2026-07-29 that made ONDO and JTO ten times too fine
   and flipped the sign of a backtest. A window whose price range the venue rule
   cannot describe with one tick is refused rather than built.
2. **The window may span days.** The ported version built one, and its manifest
   said so in ``limitations``; here the day set comes from the window like it
   does next door, and ``files.execution_npz`` — one name — is null when there
   is more than one.

The manifest keeps every key of the artefacts already on disk and only adds
(``tick_lot``, ``rebuild_cmd``, ``inputs``, ``collector``, ``time_policy``,
``outputs``, ``max_hl_book_age_ns``). ``tick_lot_source`` stays the human string
it has always been in *this* schema — the structured form is the new
``tick_lot``. Beside it, ``build_dataset.py``'s ``dataset-manifest-v1`` keeps its
own dict-shaped ``tick_lot_source``; neither reader is asked to cope with a key
that changed type under it.

Usage::

    build_hl_dataset.py --quality-report report.json --hl-symbol ONDO \\
        --book-mode bbo+fast --out-dir dataset-ondo/

Nothing here talks to the network, and nothing overwrites a recording.
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Callable, Optional, Sequence

sys.path.insert(0, str(Path(__file__).resolve().parent))

from build_dataset import (  # noqa: E402
    BOOK_MODE_LEVELS,
    BOOK_MODE_MAX_AGE_MS,
    BBO_BOOK_MODES,
    HL_VENUE,
    NS_PER_HOUR,
    NS_PER_MS,
    REPORT_SCHEMA,
    SIGNAL_VENUE,
    BuildError,
    Window,
    _decimal_hours,
    _step,
    _utc_str,
    _venue,
    _warn,
    collector_provenance,
    combined_price_grid,
    convert_hl_day,
    converter_identity,
    default_convert_fn,
    enforce_time_policy,
    file_fingerprint,
    find_day_files,
    load_quality_report,
    load_venue_meta,
    require_symbol_days,
    require_window,
    resolve_tick_lot,
    scan_hl_time_policy,
    symbol_coverage,
    total_bbo_depth_frames,
    total_deduplicated_trades,
    venue_data_dir,
    worst_verdict,
)

MANIFEST_SCHEMA = 'hl-execution-dataset-v1'
PROFILE = 'hl-execution-only-v1'


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        prog='build_hl_dataset.py',
        description='Assemble a Hyperliquid-execution-only backtest dataset '
                    '(profile %s): no signal array, gated on the Hyperliquid '
                    'verdict alone.' % PROFILE,
    )
    p.add_argument('--quality-report', required=True, type=Path,
                   help='Phase 2 report (schema quality-report-v1). Only its '
                        'hyperliquid verdict gates this build; any other '
                        'venue in it is recorded, never enforced.')
    p.add_argument('--hl-symbol', required=True,
                   help='Hyperliquid wire name of the executing instrument, e.g. ONDO.')
    p.add_argument('--out-dir', required=True, type=Path)
    p.add_argument('--book-mode', choices=sorted(BOOK_MODE_LEVELS), default='bbo+fast',
                   help='Which depth stream to convert. Default: bbo+fast, the '
                        'pairing the live connector is specified on.')
    p.add_argument('--num-levels', type=int, default=None,
                   help='Snapshot depth. Must match --book-mode (slow=20, fast=5, '
                        'bbo+fast=5); omit to take the paired default.')
    p.add_argument('--tick-size', type=float, default=None)
    p.add_argument('--lot-size', type=float, default=None,
                   help='Give both --tick-size and --lot-size, or neither and let '
                        'the tick be measured from the recording and the lot come '
                        'from the HL universe record in the sidecar.')
    p.add_argument('--min-window-hours', type=_decimal_hours, default=Decimal('1'),
                   help='Refuse a window shorter than this. Default: 1.')
    p.add_argument('--clock-correction-ns', type=int, default=0,
                   help='The only permitted clock correction. Default: 0.')
    p.add_argument('--max-hl-book-age-ms', type=int, default=None,
                   help='Recorded in the manifest; the strategy enforces it. '
                        'Default: 12000 (slow) / 1500 (fast, bbo+fast).')
    p.add_argument('--buffer-size', type=int, default=100_000_000,
                   help='Converter row preallocation. Default: 100000000.')
    p.add_argument('--delete-out-of-book', dest='delete_out_of_book',
                   action='store_true', default=True,
                   help='Emit a deletion when a level falls out of the top-N '
                        'snapshot window (the converter default, and the default '
                        'here). Recorded in the manifest either way.')
    p.add_argument('--keep-out-of-book', dest='delete_out_of_book',
                   action='store_false',
                   help='Do not emit those deletions.')
    return p.parse_args(argv)


def _rebuild_cmd(args: argparse.Namespace, num_levels: int, max_hl_book_age_ms: int,
                 tick_size: float, lot_size: float) -> list[str]:
    """The exact argv that reproduces this build, defaults made explicit."""
    cmd = [
        sys.executable, str(Path(__file__).resolve()),
        '--quality-report', str(Path(args.quality_report).resolve()),
        '--hl-symbol', args.hl_symbol,
        '--out-dir', str(Path(args.out_dir).resolve()),
        '--book-mode', args.book_mode,
        '--num-levels', str(num_levels),
        '--min-window-hours', str(args.min_window_hours),
        '--clock-correction-ns', str(args.clock_correction_ns),
        '--max-hl-book-age-ms', str(max_hl_book_age_ms),
        '--buffer-size', str(args.buffer_size),
    ]
    if not args.delete_out_of_book:
        cmd.append('--keep-out-of-book')
    # Only passed on when that is where they came from: a rebuild would
    # otherwise record `cli` for a tick the recording decided.
    if args.tick_size is not None and args.lot_size is not None:
        cmd += ['--tick-size', repr(tick_size), '--lot-size', repr(lot_size)]
    return cmd


def build(args: argparse.Namespace, convert_fn: Optional[Callable] = None) -> dict:
    """Assemble the dataset and return the manifest it wrote."""
    convert_fn = convert_fn or default_convert_fn
    num_levels = BOOK_MODE_LEVELS[args.book_mode]
    if args.num_levels is not None and args.num_levels != num_levels:
        raise BuildError(
            '--num-levels %d does not match --book-mode %s, which delivers %d '
            'levels. DiffOrderBookSnapshot preallocates exactly num_levels rows '
            'and writes len(bid_px) without a bounds check '
            '(difforderbooksnapshot.py:35-41), so the pair is not free to choose.'
            % (args.num_levels, args.book_mode, num_levels)
        )
    if args.book_mode in BBO_BOOK_MODES and not args.delete_out_of_book:
        raise BuildError(
            '--keep-out-of-book cannot be used with --book-mode %s: a suppressed '
            'truncation deletion drops the level from the fusion mirror, so '
            'nothing can delete it afterwards and it crosses the book as soon as '
            'the touch moves past it.' % args.book_mode
        )
    if (args.tick_size is None) != (args.lot_size is None):
        raise BuildError('give both --tick-size and --lot-size, or neither')
    max_hl_book_age_ms = (args.max_hl_book_age_ms
                          if args.max_hl_book_age_ms is not None
                          else BOOK_MODE_MAX_AGE_MS[args.book_mode])

    # --- gate: Hyperliquid only ---------------------------------------------
    # The overall verdict is deliberately NOT consulted. It is the worst of
    # every venue in the report, and a red signal recording is precisely the
    # situation this profile exists for: it says nothing about whether the day
    # is tradeable on Hyperliquid, which is the only venue this dataset holds.
    report_path = Path(args.quality_report).resolve()
    report = load_quality_report(report_path)
    hl_dir = venue_data_dir(report, HL_VENUE, report_path)
    # Recorded, not gated on: it is the worst of ALL of hyperliquid's days,
    # including days outside the window, which `build_dataset.py` also declines
    # to refuse over. The gate is the day-level one below, once the window —
    # and therefore the set of days this dataset is made of — is known.
    hl_verdict = _venue(report, HL_VENUE).get('verdict')

    # --- window = the executing symbol's own coverage ------------------------
    hl_cov, hl_per_day = symbol_coverage(report, HL_VENUE, args.hl_symbol)
    window = Window(hl_cov[0], hl_cov[1])
    min_window_ns = int(args.min_window_hours * NS_PER_HOUR)
    require_window(window, {'%s/%s' % (HL_VENUE, args.hl_symbol): hl_cov},
                   min_window_ns)
    days = window.days()
    require_symbol_days(hl_per_day, days, HL_VENUE, args.hl_symbol)
    _step('window [%d, %d] = %s .. %s (%.3f h), days: %s'
          % (window.start_ns, window.end_ns, _utc_str(window.start_ns),
             _utc_str(window.end_ns), window.duration_ns / NS_PER_HOUR,
             ', '.join(days)))

    verdict, reasons, outside = worst_verdict(report, (HL_VENUE,), days,
                                              include_overall=False)
    if verdict == 'red':
        raise BuildError(
            'the quality report marks a hyperliquid day inside the window red — '
            'refusing to build:\n  %s' % '\n  '.join(reasons)
        )
    if reasons:
        _warn('quality report is %s inside the window: %s'
              % (verdict, '; '.join(reasons)))
    if outside:
        _warn('quality report has non-green hyperliquid days outside the window '
              '(not built over, recorded in the manifest): %s' % '; '.join(outside))

    other_verdicts = {v: _venue(report, v).get('verdict')
                      for v in report.get('venues', {}) if v != HL_VENUE}
    for venue, word in sorted(other_verdicts.items()):
        if word not in (None, 'green'):
            _warn('%s is %s in this report. Recorded, not enforced: this dataset '
                  'holds no %s data.' % (venue, word, venue))

    hl_files = find_day_files(hl_dir, args.hl_symbol, days)
    if not hl_files:
        raise BuildError(
            'no Hyperliquid recordings for %r in %s on days %s'
            % (args.hl_symbol, hl_dir, ', '.join(days))
        )
    missing = [d for d in days if d not in {day for day, _ in hl_files}]
    if missing:
        raise BuildError(
            'the window covers days %s but hyperliquid %s has no recording for '
            '%s. A hole the size of a day is not something to build over, and '
            'the quality report claimed coverage across it.'
            % (', '.join(days), args.hl_symbol, ', '.join(missing))
        )
    hl_meta_files, hl_meta = load_venue_meta(hl_dir, HL_VENUE, window.end_ns)

    # --- time policy: raw check BEFORE conversion ---------------------------
    _step('checking the time policy on %d raw Hyperliquid file(s)' % len(hl_files))
    scans = scan_hl_time_policy([p for _d, p in hl_files], args.book_mode, window)
    enforce_time_policy(scans)
    raw_mins = [s.min_latency_ns for s in scans if s.min_latency_ns is not None]
    overall_min = min(raw_mins) if raw_mins else None
    _step('time policy ok: min(local_ts - exch_ts) = %s ns'
          % ('n/a' if overall_min is None else overall_min))

    # --- tick / lot: measured, cross-checked, overridable --------------------
    tick_size, lot_size, tick_lot = resolve_tick_lot(
        combined_price_grid(scans),
        cli_tick=args.tick_size, cli_lot=args.lot_size,
        hl_meta=hl_meta, hl_meta_files=hl_meta_files, hl_dir=hl_dir,
        symbol=args.hl_symbol,
    )
    _step('tick_size=%r lot_size=%r (%s)' % (tick_size, lot_size, tick_lot['kind']))

    # --- conversion ----------------------------------------------------------
    out_dir = Path(args.out_dir).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)
    work_dir = out_dir / '_work'
    work_dir.mkdir(parents=True, exist_ok=True)

    scan_by_path = {s.path: s for s in scans}
    hl_outputs = []
    skipped_days = []
    for day, src in hl_files:
        out_npz = out_dir / ('hl_%s_%s.npz' % (args.hl_symbol.lower(), day))
        result = convert_hl_day(
            src, out_npz,
            day=day, window=window, scan=scan_by_path[str(src)],
            tick_size=tick_size, lot_size=lot_size,
            book_mode=args.book_mode, num_levels=num_levels,
            buffer_size=args.buffer_size,
            clock_correction_ns=args.clock_correction_ns,
            work_dir=work_dir, convert_fn=convert_fn,
            delete_out_of_book=bool(args.delete_out_of_book),
            exch_ts_multiplier=NS_PER_MS,
        )
        if result is not None:
            hl_outputs.append(result)
        else:
            skipped_days.append({'day': day, 'source': str(src),
                                 'reason': 'no lines inside the window'})
    if not hl_outputs:
        raise BuildError('no Hyperliquid day produced any rows inside the window')

    names = [Path(o['path']).name for o in hl_outputs]
    corrected_window = Window(window.start_ns + int(args.clock_correction_ns),
                              window.end_ns + int(args.clock_correction_ns))
    manifest = {
        'schema': MANIFEST_SCHEMA,
        'profile': PROFILE,
        'mode_a': False,
        'built_by': 'hftbacktest collector/tools/build_hl_dataset.py',
        'built_at_utc': datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ'),
        'rebuild_cmd': _rebuild_cmd(args, num_levels, max_hl_book_age_ms,
                                    tick_size, lot_size),
        'instruments': {
            'execution': {'venue': HL_VENUE, 'symbol': args.hl_symbol,
                          'role': 'BacktestAsset'},
        },
        'tick_size': tick_size,
        'lot_size': lot_size,
        # A string in this schema since its first artefact. The structured form
        # is `tick_lot`, added beside it rather than in place of it.
        'tick_lot_source': _tick_lot_sentence(tick_lot),
        'tick_lot': tick_lot,
        'book_mode': args.book_mode,
        'num_levels': num_levels,
        'max_hl_book_age_ns': int(max_hl_book_age_ms) * NS_PER_MS,
        'clock_correction_ns': int(args.clock_correction_ns),
        'min_window_hours': float(args.min_window_hours),
        'min_window_ns': min_window_ns,
        'window': {
            'days': days,
            'start_ns': corrected_window.start_ns,
            'end_ns': corrected_window.end_ns,
            'duration_ns': corrected_window.duration_ns,
            'start_utc': _utc_str(corrected_window.start_ns),
            'end_utc': _utc_str(corrected_window.end_ns),
            'raw_start_ns': window.start_ns,
            'raw_end_ns': window.end_ns,
            'clock_correction_ns': int(args.clock_correction_ns),
            'note': 'the coverage of the executing symbol itself, over its '
                    'required streams — not intersected with anything, because '
                    'this dataset holds one venue',
        },
        'files': {
            # One name, for the one-day readers this schema was written for;
            # null rather than a guess when the window holds more than one day.
            'execution_npz': names[0] if len(names) == 1 else None,
            'execution_npz_all': names,
            'source': hl_outputs[0]['source'] if len(hl_outputs) == 1 else None,
            'sources': [o['source'] for o in hl_outputs],
            'out_dir': str(out_dir),
        },
        'inputs': {
            HL_VENUE: {
                'data_dir': str(hl_dir),
                'data': [file_fingerprint(p, day) for day, p in hl_files],
                'meta': [file_fingerprint(p) for p in hl_meta_files],
            },
        },
        'collector': {HL_VENUE: collector_provenance(hl_meta)},
        'converter': {
            **converter_identity(
                book_mode=args.book_mode,
                num_levels=num_levels,
                delete_out_of_book=bool(args.delete_out_of_book),
                exch_ts_multiplier=NS_PER_MS,
                deduplicated_trades=total_deduplicated_trades(hl_outputs),
                bbo_depth_frames=total_bbo_depth_frames(hl_outputs),
            ),
            'converted_frames': sum(s.converted_frames for s in scans),
            'shift_proof': '; '.join(o['shift_check'] for o in hl_outputs),
            'exch_ts_multiplier_ns': NS_PER_MS,
            # The same dict as `outputs.hl_depth[0]`, kept where the first
            # artefacts of this schema put it. Null for a multi-day window: one
            # day's numbers cannot stand for several.
            'build_dataset_result': hl_outputs[0] if len(hl_outputs) == 1 else None,
        },
        'outputs': {
            'hl_depth': hl_outputs,
            'skipped_days': skipped_days,
            'work_dir': str(work_dir),
        },
        'time_policy': {
            'rule': 'min(local_ts - exch_ts) >= 0 checked on the raw .gz before '
                    'conversion; a violation stops the build, it is never '
                    'corrected (design-multi-venue-collection.md, "Политика '
                    'времени" п. 2)',
            'checked_files': len(scans),
            'checked_frames': sum(s.frames_with_ts for s in scans),
            'min_local_minus_exch_ns': overall_min,
            'base_latency': 0,
            'latency_offset': 0,
            'per_file': [s.summary() for s in scans],
        },
        'quality_report': {
            **file_fingerprint(report_path),
            'schema': REPORT_SCHEMA,
            'overall_verdict': report.get('verdict'),
            'venue_verdicts': {v: _venue(report, v).get('verdict')
                               for v in report.get('venues', {})},
            'gated_on': HL_VENUE,
            'enforced': [HL_VENUE],
            'hyperliquid_verdict': hl_verdict,
            'verdict_in_window': verdict,
            'notes': reasons,
            'outside_window': outside,
            'note': 'the overall verdict is the worst of every venue in the '
                    'report and is NOT the gate here: a red %s says nothing '
                    'about whether the day is tradeable on Hyperliquid, which '
                    'is the only venue this dataset holds.' % SIGNAL_VENUE,
        },
        'initial_snapshot': False,
        'limitations': [
            'No signal array: this is not a mode-A dataset, and any other '
            'venue\'s verdict above was recorded, not enforced.',
            'No initial depth snapshot — the book starts empty and warms up from '
            'the window\'s first depth events, so the strategy\'s warm-up guard '
            'must be correct from the first callback '
            '(design-multi-venue-collection.md §3.4).',
            '%d day(s); the window is the recorded coverage of %s, not 00:00-24:00.'
            % (len(days), args.hl_symbol),
        ],
    }

    manifest_path = out_dir / 'manifest.json'
    manifest_path.write_text(json.dumps(manifest, indent=2, ensure_ascii=False) + '\n')
    _step('manifest -> %s' % manifest_path)
    return manifest


def _tick_lot_sentence(tick_lot: dict) -> str:
    """The one-line human form this schema has always carried."""
    if tick_lot['kind'] == 'cli':
        measured = tick_lot['measurement']['tick']
        return ('cli --tick-size/--lot-size (measured tick %s)'
                % ('none' if measured is None else repr(measured)))
    tick = tick_lot['tick']
    return ('tick measured from the recording: %r, rule %r (%s); lot 10^-%d '
            'from the sidecar universe'
            % (tick['measured'], tick['rule'], tick['cross_check'],
               tick_lot['sz_decimals']))


def main(argv: Optional[Sequence[str]] = None,
         convert_fn: Optional[Callable] = None) -> int:
    args = parse_args(argv)
    try:
        build(args, convert_fn=convert_fn)
    except BuildError as e:
        print('error: %s' % e, file=sys.stderr)
        sys.exit(1)
    return 0


if __name__ == '__main__':
    sys.exit(main())
