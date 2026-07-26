#!/usr/bin/env python3
"""Reproducible dataset builder — Phase 3 of ``docs/design-multi-venue-collection.md``.

Turns raw collector recordings into the inputs of a mode-A backtest: Hyperliquid
is the executing venue (a real ``BacktestAsset``), ``binancefuturesum`` is a
read-only signal array the strategy scans causally against
``hbt.current_timestamp``.

What it does, in the order the design doc requires:

1. **Gate.** Reads the Phase 2 quality report (``quality-report-v1``) and refuses
   to build anything if the verdict is red. The gate runs before assembly.
2. **Window = intersection** of the coverage of the two *symbols* being built —
   per symbol, over its required streams, not the venue-wide union, which a
   second symbol or a late-starting stream would widen past the built
   instrument's real range. Both inputs are trimmed to it; empty or shorter than
   ``--min-window-hours`` is a refusal (§3.1).
3. **Time policy** (§"Политика времени", п. 2): ``min(local_ts - exch_ts) >= 0``
   is checked on the *raw* Hyperliquid ``.gz`` — every frame that carries a venue
   timestamp, ``l2Book``/``bbo`` ``data.time`` and each ``trades[].time`` — and a
   violation stops the build. This must happen **before** conversion because
   ``hyperliquid.convert`` calls ``correct_local_timestamp`` unconditionally
   (``hyperliquid.py:205``), which silently shifts a whole file by its own
   minimum when that minimum is negative (``validation.py:37-49``). Different
   files would get different constants and time could run backwards at a day
   boundary.
4. **Conversion** of the HL day files with ``base_latency=0`` and
   ``book_mode``/``num_levels`` paired, then a post-conversion assertion that no
   shift happened after all.
5. **Signal array** built straight from the raw ``@bookTicker`` frames — no
   converter is involved on the Binance side in mode A.
6. **No initial snapshot** (§3.4). The window's days go to one ``BacktestAsset``
   as a continuous stream, so an end-of-day snapshot *inside* the window changes
   nothing; the only useful one would come from the day before the window, which
   is not part of it. The first day therefore starts with an empty book and the
   Phase 4 warm-up guard is what keeps the strategy out.
7. **Manifest** (§3.3) with everything needed to rebuild, including the exact
   argv, the converter's knobs, and the models Phase 4 reads back.

Nanosecond timestamps (~1.8e18) do not fit float64 exactly — 2^53 is about
9e15, so a float64 round-trip quantises them to 256 ns. Every timestamp here is
a Python ``int`` or ``numpy.int64`` end to end, and the signal array keeps its
timestamps in a separate int64 array precisely so they never share a float64
matrix with prices.

Usage::

    build_dataset.py --quality-report report.json \
        --hl-symbol BTC --binance-symbol BTCUSDT --out-dir dataset/

Nothing here talks to the network, and nothing overwrites a recording.
"""

from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import subprocess
import sys
from array import array
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Callable, Iterator, Optional, Sequence

import numpy as np

NS_PER_SEC = 1_000_000_000
NS_PER_MS = 1_000_000
NS_PER_HOUR = 3_600 * NS_PER_SEC

REPORT_SCHEMA = 'quality-report-v1'
MANIFEST_SCHEMA = 'dataset-manifest-v1'

HL_VENUE = 'hyperliquid'
SIGNAL_VENUE = 'binancefuturesum'

#: ``book_mode`` and ``num_levels`` are not independent. ``DiffOrderBookSnapshot``
#: preallocates exactly ``levels`` rows and writes ``len(bid_px)`` without a
#: bounds check (``difforderbooksnapshot.py:35-41,67-72``), so a 20-level
#: snapshot fed to a 5-level differ overruns. The pair is fixed in
#: ``collector/README.md:275``.
BOOK_MODE_LEVELS = {'slow': 20, 'fast': 5}

#: Defaults for the strategy's book-staleness guard (Phase 4). Measured cadences
#: are ~5.4 s (slow) and ~0.54 s (fast) — ``collector/README.md:83-88`` — so these
#: sit roughly two intervals above the median.
BOOK_MODE_MAX_AGE_MS = {'slow': 12_000, 'fast': 1_500}

SIGNAL_COLUMNS = ['bid_px', 'bid_qty', 'ask_px', 'ask_qty']

#: Mirrors ``hftbacktest.data.types.event_dtype`` (``types.py:74``). Defined
#: locally so this module — and its tests — do not need the native extension.
EVENT_DTYPE = np.dtype(
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

VERDICT_ORDER = {'green': 0, 'yellow': 1, 'red': 2}


class BuildError(Exception):
    """A refusal to build. Always fatal: the caller exits non-zero."""


def _warn(msg: str) -> None:
    print('warning: %s' % msg, file=sys.stderr)


def _step(msg: str) -> None:
    print(msg)


# ---------------------------------------------------------------------------
# window
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Window:
    """A closed nanosecond interval ``[start_ns, end_ns]`` on the local clock."""

    start_ns: int
    end_ns: int

    def __post_init__(self) -> None:
        # int() rather than a check: numpy scalars would silently propagate.
        object.__setattr__(self, 'start_ns', int(self.start_ns))
        object.__setattr__(self, 'end_ns', int(self.end_ns))

    @property
    def is_empty(self) -> bool:
        return self.start_ns > self.end_ns

    @property
    def duration_ns(self) -> int:
        return max(0, self.end_ns - self.start_ns)

    def contains(self, ts: int) -> bool:
        return self.start_ns <= ts <= self.end_ns

    def days(self) -> list[str]:
        """UTC days the window touches, as ``YYYYMMDD`` — the file-name grain."""
        if self.is_empty:
            return []
        day = _utc_date(self.start_ns)
        last = _utc_date(self.end_ns)
        out = []
        while day <= last:
            out.append(day.strftime('%Y%m%d'))
            day += timedelta(days=1)
        return out


def _utc_date(ts_ns: int):
    return datetime.fromtimestamp(ts_ns // NS_PER_SEC, tz=timezone.utc).date()


def _utc_str(ts_ns: int) -> str:
    sec, rem = divmod(int(ts_ns), NS_PER_SEC)
    return datetime.fromtimestamp(sec, tz=timezone.utc).strftime('%Y-%m-%dT%H:%M:%S') \
        + '.%09dZ' % rem


def intersect(a: tuple[int, int], b: tuple[int, int]) -> Window:
    """Intersection of two closed coverage ranges. May come back empty."""
    return Window(max(int(a[0]), int(b[0])), min(int(a[1]), int(b[1])))


def _fmt_coverages(coverages) -> str:
    if isinstance(coverages, dict):
        items = list(coverages.items())
    else:
        items = [('input %d' % i, c) for i, c in enumerate(coverages)]
    return '; '.join(
        '%s=[%d, %d]' % (name, int(rng[0]), int(rng[1])) for name, rng in items
    )


def require_window(window: Window, coverages, min_window_ns: int) -> None:
    """§3.1: an empty or too-short intersection stops the build, with the numbers."""
    if window.is_empty:
        raise BuildError(
            'the venue coverages do not overlap: %s -> intersection would be '
            '[%d, %d] (start after end). Nothing to build.'
            % (_fmt_coverages(coverages), window.start_ns, window.end_ns)
        )
    if window.duration_ns < min_window_ns:
        raise BuildError(
            'window [%d, %d] is %d ns (%.3f h) long, shorter than the required '
            'minimum of %d ns (%.3f h). Coverages: %s'
            % (
                window.start_ns, window.end_ns, window.duration_ns,
                window.duration_ns / NS_PER_HOUR, min_window_ns,
                min_window_ns / NS_PER_HOUR, _fmt_coverages(coverages),
            )
        )


# ---------------------------------------------------------------------------
# quality report (Phase 2 output)
# ---------------------------------------------------------------------------


def load_quality_report(path: Path) -> dict:
    try:
        report = json.loads(Path(path).read_text())
    except FileNotFoundError:
        raise BuildError('quality report %s does not exist' % path)
    except json.JSONDecodeError as e:
        raise BuildError('quality report %s is not valid JSON: %s' % (path, e))
    schema = report.get('schema')
    if schema != REPORT_SCHEMA:
        raise BuildError(
            'quality report %s has schema %r, expected %r. Refusing to guess at '
            'its shape.' % (path, schema, REPORT_SCHEMA)
        )
    return report


def _venue(report: dict, venue: str) -> dict:
    venues = report.get('venues') or {}
    if venue not in venues:
        raise BuildError(
            'quality report has no venue %r (it has: %s). Mode A needs both %r '
            'and %r.' % (venue, ', '.join(sorted(venues)) or 'none', HL_VENUE,
                         SIGNAL_VENUE)
        )
    return venues[venue]


def venue_data_dir(report: dict, venue: str, report_path: Path) -> Path:
    entry = _venue(report, venue)
    raw = entry.get('data_dir')
    if not raw:
        raise BuildError('quality report: venue %r has no data_dir' % venue)
    path = Path(raw)
    if not path.is_absolute():
        path = Path(report_path).resolve().parent / path
    if not path.is_dir():
        raise BuildError('venue %r data_dir %s is not a directory' % (venue, path))
    return path


def venue_coverage(report: dict, venue: str) -> tuple[int, int]:
    """The venue-wide union. Recorded for context; the window is per symbol."""
    entry = _venue(report, venue)
    cov = entry.get('coverage') or {}
    first, last = cov.get('first_local_ts'), cov.get('last_local_ts')
    if first is None or last is None:
        # The Phase 2 tool writes null when a venue produced no valid data at
        # all on the days it checked.
        raise BuildError(
            'quality report: venue %r has no coverage (first_local_ts=%r, '
            'last_local_ts=%r). There is no window to intersect.'
            % (venue, first, last)
        )
    _require_ns(venue, first, last)
    return int(first), int(last)


def _require_ns(where: str, *values) -> None:
    """A float here would already have lost nanoseconds before we saw it."""
    for value in values:
        if isinstance(value, bool) or not isinstance(value, int):
            raise BuildError(
                'quality report: %s coverage is not integral nanoseconds (%r). '
                'Nanosecond timestamps must never pass through float.'
                % (where, value)
            )


def symbol_coverage(
        report: dict,
        venue: str,
        symbol: str,
) -> tuple[tuple[int, int], dict]:
    """Coverage of the ONE symbol being built, and its per-day breakdown.

    The venue-level number is a union over every symbol and every required
    stream, which is not the valid range of the instrument this build uses: a
    second symbol that started earlier, or an on-time ``bbo`` hiding an
    ``l2Book`` that started ten minutes late, would both open the window over a
    stretch the built instrument has no data in. §3.1 wants the opposite —
    "обрезка гарантирует, что прогон не начнётся раньше первого сигнала".

    Per day, Phase 2 reports the interval in which *every* required stream of
    the symbol is live (max of the firsts, min of the lasts). Across days it is
    a union: consecutive days of one recording are contiguous, and a missing day
    inside the window is refused separately.
    """
    entry = _venue(report, venue)
    days = entry.get('days') or {}
    name = symbol.lower()
    per_day: dict = {}
    listed: set = set()
    saw_symbol = False
    saw_coverage = False
    first = last = None

    for day in sorted(days):
        symbols = (days[day] or {}).get('symbols')
        if not isinstance(symbols, dict):
            continue
        listed.update(symbols)
        if name not in symbols:
            continue
        saw_symbol = True
        cov = (symbols[name] or {}).get('coverage')
        if not isinstance(cov, dict):
            continue
        saw_coverage = True
        day_first, day_last = cov.get('first_local_ts'), cov.get('last_local_ts')
        per_day[day] = (day_first, day_last)
        if day_first is None or day_last is None:
            continue
        _require_ns('%s/%s/%s' % (venue, day, name), day_first, day_last)
        first = day_first if first is None else min(first, int(day_first))
        last = day_last if last is None else max(last, int(day_last))

    if not saw_symbol:
        raise BuildError(
            'quality report: venue %r lists no symbol %r on any day (it lists: '
            '%s). Mode A maps instruments by an explicit table, never by name, '
            'so a symbol the report never checked cannot be built.'
            % (venue, name, ', '.join(sorted(listed)) or 'nothing')
        )
    if not saw_coverage:
        raise BuildError(
            'quality report: venue %r symbol %r has no per-symbol coverage. The '
            'report predates it and its venue-wide number is a union over every '
            'symbol and stream, which is not this instrument\'s valid range — '
            're-run quality_report.py rather than trimming to the wrong window.'
            % (venue, name)
        )
    if first is None or last is None:
        raise BuildError(
            'quality report: venue %r symbol %r has no interval in which every '
            'required stream is live (per day: %s). There is nothing to build '
            'over.' % (venue, name, per_day or 'no days')
        )
    return (int(first), int(last)), per_day


def require_symbol_days(
        per_day: dict,
        days: Sequence[str],
        venue: str,
        symbol: str,
) -> None:
    """Every day inside the window must be usable for the built symbol."""
    broken = [d for d in days if per_day.get(d, (None, None))[0] is None]
    if broken:
        raise BuildError(
            'the window covers days %s but %s %s has no interval on %s in which '
            'every required stream is live. A stretch with the traded book (or '
            'the signal) missing is not something to build over.'
            % (', '.join(days), venue, symbol, ', '.join(broken))
        )


def _verdict_word(word) -> Optional[str]:
    """Normalise a verdict. An unrecognised word is red: fail closed."""
    if word is None:
        return None
    key = str(word).lower()
    return key if key in VERDICT_ORDER else 'red'


def overall_verdict(report: dict) -> str:
    """The report-level verdict alone — the gate that runs before anything else."""
    return _verdict_word(report.get('verdict')) or 'green'


def worst_verdict(
        report: dict,
        venues: Sequence[str],
        days: Optional[Sequence[str]] = None,
) -> tuple[str, list[str], list[str]]:
    """Worst of the overall verdict and the day verdicts of the venues used.

    ``days`` restricts which day verdicts count. A red day the window does not
    touch contributes nothing to the dataset being built, so it comes back in
    the third list — reported, recorded in the manifest, but not a refusal.
    """
    worst = 'green'
    reasons: list[str] = []
    outside: list[str] = []
    wanted = None if days is None else set(days)

    def consider(word, where, counts):
        nonlocal worst
        key = _verdict_word(word)
        if key is None or key == 'green':
            return
        note = '%s: %s%s' % (where, key,
                             '' if str(word).lower() in VERDICT_ORDER
                             else ' (unrecognised verdict %r)' % word)
        if not counts:
            outside.append(note)
            return
        reasons.append(note)
        if VERDICT_ORDER[key] > VERDICT_ORDER[worst]:
            worst = key

    consider(report.get('verdict'), 'overall', True)
    for venue in venues:
        entry = _venue(report, venue)
        for day in sorted((entry.get('days') or {})):
            consider((entry['days'][day] or {}).get('verdict'),
                     '%s/%s' % (venue, day),
                     wanted is None or day in wanted)
    return worst, reasons, outside


# ---------------------------------------------------------------------------
# raw recording I/O
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Record:
    line_no: int
    local_ts: int
    payload: bytes
    raw: bytes


def iter_records(path: Path) -> Iterator[Record]:
    """Yield ``<local_ts_ns> <raw_venue_json>`` lines of a recording.

    ``gzip.open`` reads every member, which is what a restart-appended
    multi-member file needs (``collector/README.md`` "Multi-member gzip").
    """
    with gzip.open(path, 'rb') as f:
        for line_no, raw in enumerate(f, start=1):
            if not raw.strip():
                continue
            head, sep, payload = raw.partition(b' ')
            if not sep:
                raise BuildError('%s line %d: no space after the timestamp' % (path, line_no))
            try:
                local_ts = int(head)
            except ValueError:
                raise BuildError(
                    '%s line %d: %r is not an integer nanosecond timestamp'
                    % (path, line_no, head[:32])
                )
            if not raw.endswith(b'\n'):
                raw = raw + b'\n'
            yield Record(line_no, local_ts, payload.rstrip(b'\n'), raw)


def trim_gz(src: Path, dst: Path, window: Window) -> int:
    """Copy the lines of ``src`` whose ``local_ts`` is inside ``window``, verbatim."""
    Path(dst).parent.mkdir(parents=True, exist_ok=True)
    kept = 0
    with gzip.open(dst, 'wb') as out:
        for rec in iter_records(src):
            if window.contains(rec.local_ts):
                out.write(rec.raw)
                kept += 1
    return kept


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, 'rb') as f:
        for chunk in iter(lambda: f.read(1 << 20), b''):
            h.update(chunk)
    return h.hexdigest()


def file_fingerprint(path: Path, day: Optional[str] = None) -> dict:
    p = Path(path)
    entry = {'path': str(p), 'bytes': p.stat().st_size, 'sha256': sha256_file(p)}
    if day is not None:
        entry['day'] = day
    return entry


def find_day_files(data_dir: Path, symbol: str, days: Sequence[str]) -> list[tuple[str, Path]]:
    """``<symbol-lowercase>_<YYYYMMDD>.gz`` for each day of the window that has one."""
    out = []
    for day in days:
        path = Path(data_dir) / ('%s_%s.gz' % (symbol.lower(), day))
        if path.exists():
            out.append((day, path))
    return out


def find_meta_files(data_dir: Path) -> list[Path]:
    """Every sidecar in the directory, oldest name first.

    Not restricted to the window's days on purpose: ``session_start`` is written
    once per process, so a collector started three days ago left it in an older
    sidecar.
    """
    return sorted(Path(data_dir).glob('_meta_*.jsonl'))


def load_venue_meta(
        data_dir: Path,
        exchange: str,
        until_ns: int,
) -> tuple[list[Path], list[tuple[Optional[int], dict]]]:
    """Sidecars belonging to one venue, and their records up to ``until_ns``.

    Days recorded by several instances can be gathered into one directory for
    conversion — the sidecars carry the exchange name precisely so they do not
    overwrite each other (``collector/README.md``, "Output format"). So the
    directory alone does not identify the venue: a sidecar is claimed by the
    exchange its ``session_start`` names, and one that names none is kept
    because there is nothing to tell it apart by.
    """
    files: list[Path] = []
    records: list[tuple[Optional[int], dict]] = []
    for path in find_meta_files(data_dir):
        recs = read_meta_records([path])
        named = {obj.get('exchange') for _ts, obj in recs
                 if obj.get('_collector') == 'session_start'}
        named.discard(None)
        if named and exchange not in named:
            continue
        files.append(path)
        records.extend((ts, obj) for ts, obj in recs if ts is None or ts <= until_ns)
    return files, records


def read_meta_records(paths: Sequence[Path]) -> list[tuple[Optional[int], dict]]:
    """Parse sidecar lines into ``(local_ts | None, object)``.

    The sidecar uses the same ``<ts> <json>`` line format as the symbol files,
    but tolerate a bare JSON object too rather than failing the whole build over
    a format detail.
    """
    out: list[tuple[Optional[int], dict]] = []
    for path in paths:
        with open(path, 'rb') as f:
            for line_no, raw in enumerate(f, start=1):
                line = raw.strip()
                if not line:
                    continue
                ts: Optional[int] = None
                body = line
                head, sep, rest = line.partition(b' ')
                if sep and head.isdigit():
                    ts = int(head)
                    body = rest
                try:
                    obj = json.loads(body)
                except json.JSONDecodeError:
                    _warn('%s line %d: unparseable sidecar line, ignored' % (path, line_no))
                    continue
                if isinstance(obj, dict):
                    out.append((ts, obj))
    return out


# ---------------------------------------------------------------------------
# time policy (§"Политика времени", п. 2)
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class LatencyViolation:
    path: str
    line_no: int
    channel: str
    local_ts: int
    exch_ts: int
    latency_ns: int

    def __str__(self) -> str:
        return (
            '%s line %d (%s): local_ts - exch_ts = %d ns < 0 '
            '(local_ts=%d, exch_ts=%d)'
            % (self.path, self.line_no, self.channel, self.latency_ns,
               self.local_ts, self.exch_ts)
        )


@dataclass
class FileScan:
    """What the raw pre-conversion pass learned about one Hyperliquid file."""

    path: str
    lines: int = 0
    frames_with_ts: int = 0
    frames_without_ts: int = 0
    min_latency_ns: Optional[int] = None
    min_latency_line: Optional[int] = None
    violation: Optional[LatencyViolation] = None
    #: frames the converter will actually consume: trades + the selected
    #: ``l2Book`` cadence, restricted to the window.
    converted_frames: int = 0
    converted_min_latency_ns: Optional[int] = None
    converted_local: array = field(default_factory=lambda: array('q'))
    converted_exch: array = field(default_factory=lambda: array('q'))

    def summary(self) -> dict:
        return {
            'path': self.path,
            'lines': self.lines,
            'frames_with_venue_ts': self.frames_with_ts,
            'frames_without_venue_ts': self.frames_without_ts,
            'min_local_minus_exch_ns': self.min_latency_ns,
            'min_local_minus_exch_line': self.min_latency_line,
            'converted_frames_in_window': self.converted_frames,
            'converted_min_local_minus_exch_ns': self.converted_min_latency_ns,
        }


def scan_hl_time_policy(
        paths: Sequence[Path],
        book_mode: str,
        window: Window,
        exch_ts_multiplier: int = NS_PER_MS,
) -> list[FileScan]:
    """Read the raw HL recordings and measure ``local_ts - exch_ts``.

    Every frame that carries a venue timestamp counts — ``l2Book`` and ``bbo``
    (``data.time``) and each entry of a ``trades`` array (``time``), all in
    milliseconds. ``bbo`` is included even though ``hyperliquid.convert`` drops
    it silently: a negative latency there is still a clock step in the recording,
    i.e. data to investigate, not to quietly convert.

    Scanning a file stops at its first violation — the caller is going to refuse
    anyway, and the first offender is the one worth naming.
    """
    scans = []
    for path in paths:
        scan = FileScan(path=str(path))
        for rec in iter_records(path):
            scan.lines += 1
            try:
                msg = json.loads(rec.payload)
            except json.JSONDecodeError:
                raise BuildError('%s line %d: payload is not valid JSON' % (path, rec.line_no))
            channel = msg.get('channel')
            data = msg.get('data')

            samples: list[tuple[int, bool]] = []  # (exch_ts_ns, converted?)
            if channel == 'trades' and isinstance(data, list):
                for trade in data:
                    t = trade.get('time') if isinstance(trade, dict) else None
                    if t is None:
                        scan.frames_without_ts += 1
                        continue
                    samples.append((int(t) * exch_ts_multiplier, True))
            elif channel in ('l2Book', 'bbo') and isinstance(data, dict):
                t = data.get('time')
                if t is None:
                    scan.frames_without_ts += 1
                else:
                    is_fast = bool(data.get('fast', False))
                    converted = channel == 'l2Book' and (
                        (book_mode == 'fast') == is_fast
                    )
                    samples.append((int(t) * exch_ts_multiplier, converted))

            for exch_ts, converted in samples:
                scan.frames_with_ts += 1
                latency = rec.local_ts - exch_ts
                if scan.min_latency_ns is None or latency < scan.min_latency_ns:
                    scan.min_latency_ns = latency
                    scan.min_latency_line = rec.line_no
                if converted and window.contains(rec.local_ts):
                    scan.converted_frames += 1
                    scan.converted_local.append(rec.local_ts)
                    scan.converted_exch.append(exch_ts)
                    if (scan.converted_min_latency_ns is None
                            or latency < scan.converted_min_latency_ns):
                        scan.converted_min_latency_ns = latency
                if latency < 0 and scan.violation is None:
                    scan.violation = LatencyViolation(
                        path=str(path), line_no=rec.line_no, channel=str(channel),
                        local_ts=rec.local_ts, exch_ts=exch_ts, latency_ns=latency,
                    )
            if scan.violation is not None:
                break
        scans.append(scan)
    return scans


def enforce_time_policy(scans: Sequence[FileScan]) -> None:
    """Stop the build on a negative feed latency — never shift (§Политика времени, п. 2)."""
    for scan in scans:
        if scan.violation is not None:
            raise BuildError(
                'time policy violated: %s.\n'
                'A negative feed latency is a clock step or a broken venue '
                'timestamp, i.e. data to investigate — not something to correct. '
                'Converting anyway would let correct_local_timestamp '
                '(validation.py:37-49) shift this file, and only this file, by '
                'its own minimum.' % scan.violation
            )


def assert_no_silent_shift(arr: np.ndarray, scan: FileScan) -> str:
    """Post-conversion proof that ``correct_local_timestamp`` left the file alone.

    With ``base_latency=0`` and a non-negative raw minimum the shift is a no-op
    by construction, so this is a cheap assertion of something already true —
    and the thing that would catch it silently stopping being true.
    """
    if len(arr) == 0:
        raise BuildError('%s: the converter produced no rows' % scan.path)
    latency = arr['local_ts'].astype(np.int64) - arr['exch_ts'].astype(np.int64)
    npz_min = int(latency.min())
    if npz_min < 0:
        raise BuildError(
            '%s: converted data has min(local_ts - exch_ts) = %d < 0'
            % (scan.path, npz_min)
        )
    raw_min = scan.converted_min_latency_ns
    if raw_min is None:
        return 'no raw comparison available (no converted frames scanned)'
    if npz_min == raw_min:
        return 'ok (npz min == raw min == %d ns)' % npz_min

    # A mismatch has exactly two explanations. Either the converter shifted the
    # file, or the frame holding the raw minimum emitted no rows at all (an
    # l2Book snapshot identical to the previous one diffs to nothing). Tell them
    # apart by looking for the surviving minimum row in the raw frames: a
    # uniform shift moves every row off the raw (local_ts, exch_ts) set.
    idx = int(np.argmin(latency))
    local_ts = int(arr['local_ts'][idx])
    exch_ts = int(arr['exch_ts'][idx])
    raw_local = np.frombuffer(scan.converted_local, dtype=np.int64)
    raw_exch = np.frombuffer(scan.converted_exch, dtype=np.int64)
    present = bool(np.any((raw_local == local_ts) & (raw_exch == exch_ts)))
    if not present:
        raise BuildError(
            '%s: the converter shifted the local timestamps. npz '
            'min(local_ts - exch_ts) = %d, raw = %d, and the npz row holding it '
            '(local_ts=%d, exch_ts=%d) does not exist in the raw file. '
            'correct_local_timestamp must not have moved anything with '
            'base_latency=0 and a non-negative raw minimum.'
            % (scan.path, npz_min, raw_min, local_ts, exch_ts)
        )
    return (
        'ok, no shift (npz min %d ns > raw min %d ns because the frame holding '
        'the raw minimum produced no rows)' % (npz_min, raw_min)
    )


# ---------------------------------------------------------------------------
# Hyperliquid conversion
# ---------------------------------------------------------------------------


def default_convert_fn(**kwargs):
    try:
        from hftbacktest.data.utils import hyperliquid
    except Exception as e:  # pragma: no cover - depends on the native build
        raise BuildError(
            'cannot import hftbacktest.data.utils.hyperliquid (%s). Build the '
            'package first: maturin develop --release -m py-hftbacktest/Cargo.toml' % e
        )
    return hyperliquid.convert(**kwargs)


def convert_hl_day(
        src: Path,
        out_npz: Path,
        *,
        day: str,
        window: Window,
        scan: FileScan,
        tick_size: float,
        lot_size: float,
        book_mode: str,
        num_levels: int,
        buffer_size: int,
        clock_correction_ns: int,
        work_dir: Path,
        convert_fn: Callable,
        delete_out_of_book: bool = True,
        exch_ts_multiplier: int = NS_PER_MS,
) -> Optional[dict]:
    """Trim one raw day to the window, convert it, and save ``<out>/hl_<sym>_<day>.npz``."""
    trimmed = Path(work_dir) / ('%s.window.gz' % Path(src).stem)
    kept = trim_gz(src, trimmed, window)
    if kept == 0:
        _warn('%s has no lines inside the window; skipping' % src)
        return None
    if scan.converted_frames == 0:
        raise BuildError(
            '%s has %d lines inside the window but no %r-cadence l2Book or trade '
            'frames. Either book_mode is wrong for this recording or the day is '
            'unusable.' % (src, kept, book_mode)
        )

    _step('converting %s (%d lines in window, book_mode=%s, num_levels=%d)'
          % (src, kept, book_mode, num_levels))
    arr = convert_fn(
        input_filename=str(trimmed),
        tick_size=tick_size,
        lot_size=lot_size,
        num_levels=num_levels,
        book_mode=book_mode,
        # Never non-zero: base_latency feeds the same shift that the time policy
        # forbids (validation.py:44-48).
        base_latency=0,
        buffer_size=buffer_size,
        output_filename=None,
        # Both explicit even though both equal the converter's current default.
        # `delete_out_of_book` decides whether a level falling out of the top-N
        # window is emitted as a deletion — one of the three knobs the design
        # doc names as central to Phases 3-5 — and the multiplier is hard-coded
        # a second time in `scan_hl_time_policy`, where a silent disagreement
        # would make `assert_no_silent_shift` diagnose a shift that never
        # happened.
        delete_out_of_book=delete_out_of_book,
        exch_ts_multiplier=exch_ts_multiplier,
    )
    shift_check = assert_no_silent_shift(arr, scan)

    post_min = None
    if clock_correction_ns:
        arr['local_ts'] += np.int64(clock_correction_ns)
        post_min = int((arr['local_ts'].astype(np.int64)
                        - arr['exch_ts'].astype(np.int64)).min())
        if post_min < 0:
            _warn('after clock_correction_ns=%d the HL feed latency minimum is %d ns '
                  '(< 0) in %s' % (clock_correction_ns, post_min, src))

    Path(out_npz).parent.mkdir(parents=True, exist_ok=True)
    # The Rust reader hard-codes the array key `data` (reader.rs).
    np.savez_compressed(out_npz, data=arr)
    return {
        'day': day,
        'path': str(out_npz),
        'rows': int(len(arr)),
        'source': str(src),
        'trimmed_source': str(trimmed),
        'lines_in_window': kept,
        'shift_check': shift_check,
        'min_local_minus_exch_ns': int(
            (arr['local_ts'].astype(np.int64) - arr['exch_ts'].astype(np.int64)).min()
        ),
        'post_correction_min_local_minus_exch_ns': post_min,
    }


# ---------------------------------------------------------------------------
# Binance signal array
# ---------------------------------------------------------------------------


def build_signal(
        paths: Sequence[Path],
        symbol: str,
        window: Window,
        clock_correction_ns: int = 0,
) -> tuple[np.ndarray, np.ndarray]:
    """Build the mode-A signal array from raw ``@bookTicker`` frames.

    The design doc calls for ``(N, 5)`` — ``local_ts, bid_px, bid_qty, ask_px,
    ask_qty``. It is returned split instead: ``ts`` is ``int64 (N,)`` and
    ``values`` is ``float64 (N, 4)`` with the columns of :data:`SIGNAL_COLUMNS`.
    One matrix would force the timestamps through float64, where 1.8e18 ns
    quantises to 256 ns — the arrival order of two frames 100 ns apart would be
    lost, and with it the "last row with local_ts <= current_timestamp" rule.

    Sorting is stable, so frames sharing a ``local_ts`` keep their arrival
    order and the *last* of them is the one the rule selects.
    """
    # `array` rather than a list: a day of @bookTicker is millions of frames, and
    # 8 bytes per number instead of a boxed Python object keeps this in memory.
    ts_list = array('q')
    values = array('d')
    want = symbol.upper()
    for path in paths:
        for rec in iter_records(path):
            # The bulk of a recording is @depth@0ms frames of several kilobytes
            # each; a substring test skips parsing them. It only ever prefilters
            # — the parsed `e` field still decides.
            if b'bookTicker' not in rec.payload:
                continue
            try:
                msg = json.loads(rec.payload)
            except json.JSONDecodeError:
                raise BuildError('%s line %d: payload is not valid JSON' % (path, rec.line_no))
            data = msg.get('data') if isinstance(msg.get('data'), dict) else msg
            if not isinstance(data, dict) or data.get('e') != 'bookTicker':
                continue
            if str(data.get('s', '')).upper() != want:
                continue
            if not window.contains(rec.local_ts):
                continue
            ts_list.append(rec.local_ts)
            values.extend((
                float(data['b']), float(data['B']),
                float(data['a']), float(data['A']),
            ))

    ts = np.frombuffer(ts_list, dtype=np.int64)
    vals = np.frombuffer(values, dtype=np.float64).reshape(-1, len(SIGNAL_COLUMNS))
    if len(ts):
        # Fancy indexing copies, so the returned arrays no longer alias the
        # `array` buffers they were read out of.
        order = np.argsort(ts, kind='stable')
        ts = ts[order]
        vals = vals[order]
        if clock_correction_ns:
            ts = ts + np.int64(clock_correction_ns)
    else:
        ts = np.empty(0, dtype=np.int64)
        vals = np.empty((0, len(SIGNAL_COLUMNS)), dtype=np.float64)
    return ts, vals


# ---------------------------------------------------------------------------
# instrument metadata
# ---------------------------------------------------------------------------


def tick_lot_from_sz_decimals(sz_decimals: int, max_decimals: int = 6) -> tuple[float, float]:
    """``lot = 10^-sz``, ``tick = 10^-(MAX_DECIMALS - sz)`` for HL perps.

    ``design-hyperliquid-connector.md`` §5.3: Hyperliquid has no tick-size field
    and its effective tick is price-dependent, so the finest legal increment is
    registered and the 5-significant-figure rule is enforced at submit time.
    """
    sz = int(sz_decimals)
    if sz < 0 or sz > max_decimals:
        raise BuildError('szDecimals=%d is outside 0..%d' % (sz, max_decimals))
    tick = float(Decimal(1).scaleb(-(max_decimals - sz)))
    lot = float(Decimal(1).scaleb(-sz))
    return tick, lot


def find_universe_sz_decimals(
        records: Sequence[tuple[Optional[int], dict]],
        wire_symbol: str,
) -> Optional[int]:
    """``szDecimals`` for one wire name, from the sidecar's ``universe`` records."""
    seen: dict[int, int] = {}
    for _ts, obj in records:
        if obj.get('_collector') != 'universe':
            continue
        for entry in obj.get('symbols') or []:
            if entry.get('wire') == wire_symbol:
                sz = entry.get('szDecimals')
                if sz is None:
                    continue
                seen[int(sz)] = seen.get(int(sz), 0) + 1
    if not seen:
        return None
    if len(seen) > 1:
        raise BuildError(
            'the sidecar reports conflicting szDecimals for %r: %s. The '
            'instrument changed under the recording; resolve it before building.'
            % (wire_symbol, sorted(seen))
        )
    return next(iter(seen))


def collector_provenance(records: Sequence[tuple[Optional[int], dict]]) -> dict:
    """Version/commit of every collector session that touched this recording."""
    sessions = [obj for _ts, obj in records if obj.get('_collector') == 'session_start']

    def distinct(key):
        out = []
        for s in sessions:
            v = s.get(key)
            if v is not None and v not in out:
                out.append(v)
        return out

    return {
        'session_starts': len(sessions),
        'commits': distinct('commit'),
        'versions': distinct('version'),
        'branches': distinct('branch'),
        'dirty': distinct('dirty'),
        'exchanges': distinct('exchange'),
        'symbols': distinct('symbols'),
        'bybit_depths': distinct('bybit_depths'),
        'hl_l2_modes': distinct('hl_l2_modes'),
    }


def converter_identity(
        *,
        book_mode: str,
        num_levels: int,
        delete_out_of_book: bool,
        exch_ts_multiplier: int,
) -> dict:
    """Which converter code produced the ``.npz`` files, and with which knobs.

    Every argument that changes the emitted depth stream belongs here: without
    them a change to a converter default silently changes the dataset with no
    diff in the manifest, and §3.3's "без манифеста результат невоспроизводим"
    would be a claim the manifest does not support.
    """
    version = None
    try:
        from importlib.metadata import version as _pkg_version
        version = _pkg_version('hftbacktest')
    except Exception:
        pass
    describe = None
    try:
        repo = Path(__file__).resolve().parents[2]
        describe = subprocess.run(
            ['git', '-C', str(repo), 'describe', '--always', '--dirty'],
            capture_output=True, text=True, timeout=10,
        ).stdout.strip() or None
    except Exception:
        pass
    return {
        'module': 'hftbacktest.data.utils.hyperliquid.convert',
        'package_version': version,
        'git_describe': describe,
        'base_latency': 0,
        'book_mode': book_mode,
        'num_levels': num_levels,
        'delete_out_of_book': delete_out_of_book,
        'exch_ts_multiplier': exch_ts_multiplier,
        'snapshot_module': None,   # no snapshot is built — see `snapshots.note`
    }


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _decimal_hours(text: str) -> Decimal:
    try:
        value = Decimal(text)
    except InvalidOperation:
        raise argparse.ArgumentTypeError('%r is not a number of hours' % text)
    # Decimal('nan') and Decimal('inf') parse happily and would blow up later.
    if not value.is_finite():
        raise argparse.ArgumentTypeError('%r is not a finite number of hours' % text)
    if value < 0:
        raise argparse.ArgumentTypeError('hours must not be negative')
    return value


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        prog='build_dataset.py',
        description='Assemble a reproducible mode-A backtest dataset '
                    '(Phase 3 of docs/design-multi-venue-collection.md).',
    )
    p.add_argument('--quality-report', required=True, type=Path,
                   help='Phase 2 report (schema quality-report-v1). A red verdict '
                        'refuses the build.')
    p.add_argument('--hl-symbol', required=True,
                   help='Hyperliquid wire name of the executing instrument, e.g. BTC.')
    p.add_argument('--binance-symbol', required=True,
                   help='binancefuturesum symbol of the signal instrument, e.g. BTCUSDT.')
    p.add_argument('--out-dir', required=True, type=Path)
    p.add_argument('--book-mode', choices=sorted(BOOK_MODE_LEVELS), default='slow',
                   help='Which l2Book cadence to convert. Default: slow.')
    p.add_argument('--num-levels', type=int, default=None,
                   help='Snapshot depth. Must match --book-mode (slow=20, fast=5); '
                        'omit to take the paired default.')
    p.add_argument('--tick-size', type=float, default=None)
    p.add_argument('--lot-size', type=float, default=None,
                   help='Give both --tick-size and --lot-size, or neither and let '
                        'them come from the HL universe record in the sidecar.')
    p.add_argument('--min-window-hours', type=_decimal_hours, default=Decimal('1'),
                   help='Refuse a window shorter than this. Default: 1.')
    p.add_argument('--clock-correction-ns', type=int, default=0,
                   help='The only permitted clock correction. Applied to the local '
                        'timestamps of BOTH venues or to neither. Default: 0.')
    p.add_argument('--max-signal-age-ms', type=int, default=1000,
                   help='Recorded in the manifest; the strategy enforces it. Default: 1000.')
    p.add_argument('--max-hl-book-age-ms', type=int, default=None,
                   help='Recorded in the manifest. Default: 12000 (slow) / 1500 (fast).')
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

    models = p.add_argument_group(
        'backtest models (§3.3)',
        'Declared here so the manifest carries them and Phase 4 can read them '
        'back instead of inventing a default. Omitted: recorded as null, and '
        'backtest_first.py then requires them on its own command line.')
    models.add_argument('--maker-fee', type=float, default=None,
                        help='Maker fee as a fraction of trading value, e.g. '
                             '0.00015. Give both fees or neither.')
    models.add_argument('--taker-fee', type=float, default=None)
    models.add_argument('--entry-latency-ms', type=float, default=None,
                        help='Constant order entry latency. Give both latencies '
                             'or neither.')
    models.add_argument('--resp-latency-ms', type=float, default=None)
    return p.parse_args(argv)


def _rebuild_cmd(args: argparse.Namespace, num_levels: int, max_hl_book_age_ms: int,
                 tick_size: Optional[float], lot_size: Optional[float]) -> list[str]:
    """The exact argv that reproduces this build, defaults made explicit."""
    cmd = [
        sys.executable, str(Path(__file__).resolve()),
        '--quality-report', str(Path(args.quality_report).resolve()),
        '--hl-symbol', args.hl_symbol,
        '--binance-symbol', args.binance_symbol,
        '--out-dir', str(Path(args.out_dir).resolve()),
        '--book-mode', args.book_mode,
        '--num-levels', str(num_levels),
        '--min-window-hours', str(args.min_window_hours),
        '--clock-correction-ns', str(args.clock_correction_ns),
        '--max-signal-age-ms', str(args.max_signal_age_ms),
        '--max-hl-book-age-ms', str(max_hl_book_age_ms),
        '--buffer-size', str(args.buffer_size),
    ]
    # Only pass tick/lot on if that is where they came from; otherwise the
    # rebuild would record 'cli' for something the sidecar decided.
    if args.tick_size is not None and args.lot_size is not None:
        cmd += ['--tick-size', repr(tick_size), '--lot-size', repr(lot_size)]
    return cmd


# ---------------------------------------------------------------------------
# the build
# ---------------------------------------------------------------------------


def build(args: argparse.Namespace,
          convert_fn: Optional[Callable] = None,
          snapshot_fn: Optional[Callable] = None) -> dict:
    """Assemble the dataset. ``snapshot_fn`` is accepted and unused — see the
    initial-snapshot section below for why no snapshot is built."""
    convert_fn = convert_fn or default_convert_fn

    # --- book_mode / num_levels pairing, before anything reads a file --------
    num_levels = BOOK_MODE_LEVELS[args.book_mode]
    if args.num_levels is not None and args.num_levels != num_levels:
        raise BuildError(
            '--num-levels %d does not match --book-mode %s, which delivers %d '
            'levels. DiffOrderBookSnapshot preallocates exactly num_levels rows '
            'and writes len(bid_px) without a bounds check '
            '(difforderbooksnapshot.py:35-41), so the pair is not free to choose.'
            % (args.num_levels, args.book_mode, num_levels)
        )
    max_hl_book_age_ms = (args.max_hl_book_age_ms
                          if args.max_hl_book_age_ms is not None
                          else BOOK_MODE_MAX_AGE_MS[args.book_mode])
    if (args.tick_size is None) != (args.lot_size is None):
        raise BuildError('give both --tick-size and --lot-size, or neither')
    if (args.maker_fee is None) != (args.taker_fee is None):
        raise BuildError('give both --maker-fee and --taker-fee, or neither')
    if (args.entry_latency_ms is None) != (args.resp_latency_ms is None):
        raise BuildError(
            'give both --entry-latency-ms and --resp-latency-ms, or neither')
    if args.entry_latency_ms is not None and (
            args.entry_latency_ms <= 0 or args.resp_latency_ms <= 0):
        raise BuildError(
            'order latency must be non-zero and positive (doc, Phase 4); got '
            'entry=%r resp=%r ms' % (args.entry_latency_ms, args.resp_latency_ms))
    if abs(int(args.clock_correction_ns)) >= 2 ** 62:
        # Python ints are unbounded; int64 timestamps are not.
        raise BuildError('--clock-correction-ns %d does not fit an int64 timestamp'
                         % args.clock_correction_ns)

    # --- gate ---------------------------------------------------------------
    # Resolved up front so every path the manifest records is absolute and a
    # rebuild from a different working directory lands in the same places.
    report_path = Path(args.quality_report).resolve()
    report = load_quality_report(report_path)
    if overall_verdict(report) == 'red':
        raise BuildError(
            'quality report %s has an overall verdict of %r — refusing to build. '
            'The gate runs before assembly.' % (report_path, report.get('verdict'))
        )

    hl_dir = venue_data_dir(report, HL_VENUE, report_path)
    bn_dir = venue_data_dir(report, SIGNAL_VENUE, report_path)
    hl_venue_cov = venue_coverage(report, HL_VENUE)
    bn_venue_cov = venue_coverage(report, SIGNAL_VENUE)

    # --- 3.1 window = intersection -----------------------------------------
    # Of the two SYMBOLS being built, not of the two venues: see
    # `symbol_coverage`.
    hl_cov, hl_per_day = symbol_coverage(report, HL_VENUE, args.hl_symbol)
    bn_cov, bn_per_day = symbol_coverage(report, SIGNAL_VENUE, args.binance_symbol)
    window = intersect(hl_cov, bn_cov)
    min_window_ns = int(args.min_window_hours * NS_PER_HOUR)
    require_window(window, {'%s/%s' % (HL_VENUE, args.hl_symbol): hl_cov,
                            '%s/%s' % (SIGNAL_VENUE, args.binance_symbol): bn_cov},
                   min_window_ns)
    days = window.days()
    require_symbol_days(hl_per_day, days, HL_VENUE, args.hl_symbol)
    require_symbol_days(bn_per_day, days, SIGNAL_VENUE, args.binance_symbol)
    _step('window [%d, %d] = %s .. %s (%.3f h), days: %s'
          % (window.start_ns, window.end_ns, _utc_str(window.start_ns),
             _utc_str(window.end_ns), window.duration_ns / NS_PER_HOUR, ', '.join(days)))

    # The day-level half of the gate, now that the days are known. Still before
    # any assembly: nothing has been read or written yet.
    verdict, reasons, outside = worst_verdict(report, (HL_VENUE, SIGNAL_VENUE), days)
    if verdict == 'red':
        raise BuildError(
            'the quality report marks a day inside the window red — refusing to '
            'build:\n  %s' % '\n  '.join(reasons)
        )
    if reasons:
        _warn('quality report is %s inside the window: %s' % (verdict, '; '.join(reasons)))
    if outside:
        _warn('quality report has non-green days outside the window (not built '
              'over, recorded in the manifest): %s' % '; '.join(outside))

    hl_files = find_day_files(hl_dir, args.hl_symbol, days)
    bn_files = find_day_files(bn_dir, args.binance_symbol, days)
    if not hl_files:
        raise BuildError(
            'no Hyperliquid recordings for %r in %s on days %s'
            % (args.hl_symbol, hl_dir, ', '.join(days))
        )
    if not bn_files:
        raise BuildError(
            'no %s recordings for %r in %s on days %s'
            % (SIGNAL_VENUE, args.binance_symbol, bn_dir, ', '.join(days))
        )
    # A whole missing day is not the kind of hole `max_signal_age` covers at
    # runtime: it contradicts the coverage the report claimed. Mode A also
    # requires the signal to span the window on both sides, so the check is
    # symmetric.
    for label, symbol, found in ((HL_VENUE, args.hl_symbol, hl_files),
                                 (SIGNAL_VENUE, args.binance_symbol, bn_files)):
        missing = [d for d in days if d not in {day for day, _ in found}]
        if missing:
            raise BuildError(
                'the window covers days %s but %s %s has no recording for %s. A '
                'hole the size of a day is not something to build over, and the '
                'quality report claimed coverage across it.'
                % (', '.join(days), label, symbol, ', '.join(missing))
            )

    hl_meta_files, hl_meta = load_venue_meta(hl_dir, HL_VENUE, window.end_ns)
    bn_meta_files, bn_meta = load_venue_meta(bn_dir, SIGNAL_VENUE, window.end_ns)

    # --- 3.2 time policy: raw check BEFORE conversion -----------------------
    _step('checking the time policy on %d raw Hyperliquid file(s)' % len(hl_files))
    scans = scan_hl_time_policy([p for _d, p in hl_files], args.book_mode, window)
    enforce_time_policy(scans)
    raw_mins = [s.min_latency_ns for s in scans if s.min_latency_ns is not None]
    overall_min = min(raw_mins) if raw_mins else None
    _step('time policy ok: min(local_ts - exch_ts) = %s ns'
          % ('n/a' if overall_min is None else overall_min))

    # --- tick / lot ---------------------------------------------------------
    if args.tick_size is not None:
        tick_size, lot_size = float(args.tick_size), float(args.lot_size)
        tick_lot_source = {'kind': 'cli'}
    else:
        sz = find_universe_sz_decimals(hl_meta, args.hl_symbol)
        if sz is None:
            raise BuildError(
                'no `universe` record for %r in %s, and no --tick-size/--lot-size '
                'given. Without szDecimals the instrument has no lot or tick.'
                % (args.hl_symbol, ', '.join(str(p) for p in hl_meta_files) or hl_dir)
            )
        tick_size, lot_size = tick_lot_from_sz_decimals(sz)
        tick_lot_source = {
            'kind': 'hl_universe',
            'sz_decimals': int(sz),
            'rule': 'lot = 10^-szDecimals, tick = 10^-(6 - szDecimals) '
                    '(design-hyperliquid-connector.md §5.3)',
            'meta_files': [str(p) for p in hl_meta_files],
        }
    _step('tick_size=%r lot_size=%r (%s)' % (tick_size, lot_size, tick_lot_source['kind']))

    # --- 3.3 conversion -----------------------------------------------------
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
            # Legitimate at the edges: a window ending at midnight touches the
            # next day, whose file starts after the window. Recorded rather than
            # silently dropped.
            skipped_days.append({
                'day': day, 'source': str(src),
                'reason': 'no lines inside the window',
            })
    if not hl_outputs:
        raise BuildError('no Hyperliquid day produced any rows inside the window')

    # --- signal array -------------------------------------------------------
    _step('building the signal array from %d %s file(s)' % (len(bn_files), SIGNAL_VENUE))
    ts, values = build_signal([p for _d, p in bn_files], args.binance_symbol, window,
                              args.clock_correction_ns)
    if len(ts) == 0:
        raise BuildError(
            'no %s @bookTicker frames for %r inside the window. The signal is the '
            'one required stream in mode A; without it there is nothing to trade '
            'against.' % (SIGNAL_VENUE, args.binance_symbol)
        )
    signal_path = out_dir / ('signal_%s_%s.npz' % (SIGNAL_VENUE, args.binance_symbol.lower()))
    np.savez_compressed(signal_path, ts=ts, values=values)
    _step('signal: %d rows -> %s' % (len(ts), signal_path))

    # --- 3.4 initial snapshot -----------------------------------------------
    # Deliberately empty. The days of a window go to ONE `BacktestAsset` as
    # `asset.data([day1..dayN])` and are read as a single continuous stream
    # (`create_last_snapshot` itself takes a list, snapshot.py:31-37), so the
    # book already carries across a file boundary and an end-of-day snapshot
    # inside the window changes nothing. The chain this used to build was
    # therefore a native-engine run per day producing files nothing read — and
    # it recorded them under `for_day = days[1..N]`, while Phase 4 looks for
    # `for_day == days[0]`, so they were unselectable by construction too.
    #
    # The one snapshot that would matter is built from the day BEFORE the
    # window, which is outside it. §3.4 accepts the consequence in writing: the
    # first day starts with an empty book and the Phase 4 warm-up guard is what
    # keeps the strategy out until it fills.
    snapshots = {
        'created': [],
        'initial_snapshot': None,
        'unavailable': None,
        'note': 'No snapshot is built. The window\'s days are one continuous '
                'stream in a single BacktestAsset, so an end-of-day snapshot '
                'inside the window is redundant; the only useful one would come '
                'from the day preceding the window, which is not part of it. '
                'The first day therefore starts with an empty book (best_bid() '
                'is NaN, best_bid_tick() is INVALID_MIN) and the Phase 4 warm-up '
                'guard must be correct from the first callback '
                '(design-multi-venue-collection.md §3.4).',
    }

    # --- 3.3 manifest -------------------------------------------------------
    # The window the DATA now lives on. Trimming happened on the raw scale
    # (`trim_gz`, `build_signal`); the correction was applied to both arrays
    # afterwards, so everything downstream must use the shifted bounds.
    corrected_window = Window(window.start_ns + int(args.clock_correction_ns),
                              window.end_ns + int(args.clock_correction_ns))
    manifest = {
        'schema': MANIFEST_SCHEMA,
        'generated_at': datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ'),
        'rebuild_cmd': _rebuild_cmd(args, num_levels, max_hl_book_age_ms,
                                    tick_size, lot_size),
        'quality_report': {
            **file_fingerprint(report_path),
            'schema': REPORT_SCHEMA,
            'profile': report.get('profile'),
            'verdict': verdict,
            'overall_verdict': overall_verdict(report),
            'venue_verdicts': {v: _venue(report, v).get('verdict')
                               for v in (HL_VENUE, SIGNAL_VENUE)},
            'notes': reasons,
            'outside_window': outside,
        },
        'inputs': {
            HL_VENUE: {
                'data_dir': str(hl_dir),
                'data': [file_fingerprint(p, day) for day, p in hl_files],
                'meta': [file_fingerprint(p) for p in hl_meta_files],
            },
            SIGNAL_VENUE: {
                'data_dir': str(bn_dir),
                'data': [file_fingerprint(p, day) for day, p in bn_files],
                'meta': [file_fingerprint(p) for p in bn_meta_files],
            },
        },
        'collector': {
            HL_VENUE: collector_provenance(hl_meta),
            SIGNAL_VENUE: collector_provenance(bn_meta),
        },
        'converter': converter_identity(
            book_mode=args.book_mode,
            num_levels=num_levels,
            delete_out_of_book=bool(args.delete_out_of_book),
            exch_ts_multiplier=NS_PER_MS,
        ),
        'instruments': {
            'execution': {'venue': HL_VENUE, 'symbol': args.hl_symbol,
                          'role': 'BacktestAsset'},
            'signal': {'venue': SIGNAL_VENUE, 'symbol': args.binance_symbol,
                       'role': 'read-only array, not traded',
                       'required_stream': '@bookTicker'},
            'mapping_note': 'explicit table, never inferred from the names '
                            '(design-multi-venue-collection.md, "Режим A")',
        },
        'book_mode': args.book_mode,
        'num_levels': num_levels,
        'tick_size': tick_size,
        'lot_size': lot_size,
        'tick_lot_source': tick_lot_source,
        # start_ns/end_ns are on the SAME scale as the data: the correction is
        # applied to both local-timestamp arrays after trimming, so a window
        # left on the raw scale would declare an interval the data no longer
        # occupies — Phase 4 bounds its loop with these against
        # hbt.current_timestamp, which comes from the shifted feed.
        'window': {
            'start_ns': corrected_window.start_ns,
            'end_ns': corrected_window.end_ns,
            'duration_ns': corrected_window.duration_ns,
            'start_utc': _utc_str(corrected_window.start_ns),
            'end_utc': _utc_str(corrected_window.end_ns),
            'raw_start_ns': window.start_ns,
            'raw_end_ns': window.end_ns,
            'clock_correction_ns': int(args.clock_correction_ns),
            'days': days,
            'coverage': {
                HL_VENUE: {'first_local_ts': hl_cov[0], 'last_local_ts': hl_cov[1],
                           'symbol': args.hl_symbol.lower(), 'scale': 'raw'},
                SIGNAL_VENUE: {'first_local_ts': bn_cov[0], 'last_local_ts': bn_cov[1],
                               'symbol': args.binance_symbol.lower(), 'scale': 'raw'},
            },
            'venue_coverage': {
                HL_VENUE: {'first_local_ts': hl_venue_cov[0],
                           'last_local_ts': hl_venue_cov[1]},
                SIGNAL_VENUE: {'first_local_ts': bn_venue_cov[0],
                               'last_local_ts': bn_venue_cov[1]},
            },
            'note': 'intersection of the two BUILT SYMBOLS coverage (per symbol, '
                    'over its required streams), not of the venue-wide unions '
                    'also recorded here; both inputs are trimmed to it on the raw '
                    'scale so the run cannot start before the first signal, and '
                    'the bounds above then carry clock_correction_ns like the data',
            'days_note': 'YYYYMMDD file-name grain of the raw window',
        },
        'min_window_hours': float(args.min_window_hours),
        'min_window_ns': min_window_ns,
        'clock_correction_ns': int(args.clock_correction_ns),
        'max_signal_age_ns': int(args.max_signal_age_ms) * NS_PER_MS,
        'max_hl_book_age_ns': int(max_hl_book_age_ms) * NS_PER_MS,
        'time_policy': {
            'rule': 'min(local_ts - exch_ts) >= 0 checked on the raw .gz before '
                    'conversion; a violation stops the build, it is never '
                    'corrected (design-multi-venue-collection.md, "Политика '
                    'времени" п. 2)',
            'scope': 'every frame of the selected HL day files that carries a '
                     'venue timestamp: l2Book/bbo data.time and each trades[].time',
            'checked_files': len(scans),
            'checked_frames': sum(s.frames_with_ts for s in scans),
            'min_local_minus_exch_ns': overall_min,
            'base_latency': 0,
            'latency_offset': 0,
            'per_file': [s.summary() for s in scans],
        },
        'outputs': {
            'hl_depth': hl_outputs,
            'skipped_days': skipped_days,
            'signal': {
                'path': str(signal_path),
                'rows': int(len(ts)),
            },
            'work_dir': str(work_dir),
        },
        'signal': {
            'path': str(signal_path),
            'rows': int(len(ts)),
            'keys': {'ts': 'int64 (N,) local_ts nanoseconds',
                     'values': 'float64 (N, 4)'},
            'columns': list(SIGNAL_COLUMNS),
            'ts_dtype': 'int64',
            'values_dtype': 'float64',
            'source_stream': '@bookTicker',
            'note': 'the design doc calls this (N, 5) with local_ts as column 0; '
                    'it is split into an int64 ts array plus a float64 value '
                    'matrix because nanosecond timestamps (~1.8e18) do not fit '
                    'float64 exactly (2^53 ~ 9e15)',
            'selection_rule': 'last row with ts <= current_timestamp; on equal ts '
                              'the last in arrival order (the sort is stable)',
            'freshness_rule': 'no row within max_signal_age_ns => do not trade '
                              '(same code path as "no signal yet")',
        },
        'snapshots': snapshots,
        'backtest_defaults': {
            '__note__': 'Phase 4 requires every model to be set explicitly and '
                        'recorded here, even when it equals the BacktestAsset '
                        'default (py-hftbacktest/src/lib.rs:134-155 silently '
                        'gives zero fees, zero latency, LogProbQueueModel2 and '
                        'NoPartialFillExchange). backtest_first.py READS these: '
                        'fee_model.maker_fee/taker_fee and constant_latency_ns.'
                        '{entry,response}. null means "not chosen here", and '
                        'backtest_first.py then requires the value on its own '
                        'command line rather than inventing one. The resolved '
                        'set, with the provenance of each value, is written to '
                        'the Phase 4 results JSON.',
            'fee_model': None if args.maker_fee is None else {
                'kind': 'TradingValueFeeModel',
                'maker_fee': float(args.maker_fee),
                'taker_fee': float(args.taker_fee),
            },
            # Not free choices: the harness builds exactly these, and
            # PartialFillExchange is forbidden outright (see `forbidden`).
            'queue_model': {'kind': 'LogProbQueueModel2'},
            'exchange_kind': {
                'kind': 'NoPartialFillExchange',
                'accepted_distortion': 'fills the whole leaves_qty regardless of '
                                       'the liquidity at the level '
                                       '(proc/nopartialfillexchange.rs:58-62)',
            },
            'constant_latency_ns': {
                'entry': None if args.entry_latency_ms is None
                         else int(round(args.entry_latency_ms * NS_PER_MS)),
                'response': None if args.resp_latency_ms is None
                            else int(round(args.resp_latency_ms * NS_PER_MS)),
            },
            'latency_offset': 0,
            'latency_offset_note': 'must stay 0 unless another region is being '
                                   'modelled, and then it is set in pairs on the '
                                   'asset and the order-latency source '
                                   '("Политика времени" п. 4)',
            'forbidden': ['PartialFillExchange: partial fills never reach the '
                          'local position (proc/local.rs:102-103 applies a fill '
                          'only on Status::Filled) — AGENTS.md §4.6'],
            'elapse_note': 'the observation interval of elapse(delta) must be '
                           'chosen explicitly and reconciled with max_signal_age_ns',
        },
    }

    manifest_path = out_dir / 'manifest.json'
    manifest_path.write_text(json.dumps(manifest, indent=2, ensure_ascii=False) + '\n')
    _step('manifest -> %s' % manifest_path)
    return manifest


def main(argv: Optional[Sequence[str]] = None,
         convert_fn: Optional[Callable] = None,
         snapshot_fn: Optional[Callable] = None) -> int:
    args = parse_args(argv)
    try:
        build(args, convert_fn=convert_fn, snapshot_fn=snapshot_fn)
    except BuildError as e:
        print('error: %s' % e, file=sys.stderr)
        sys.exit(1)
    return 0


if __name__ == '__main__':
    sys.exit(main())
