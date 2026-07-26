#!/usr/bin/env python3
"""Offline quality report over raw collector recordings.

Implements **Фаза 2 — offline quality report по сырью** of
`docs/design-multi-venue-collection.md`. It answers the one question the
collector process cannot answer about itself (`collector/README.md`, "Going
silent"): *did we actually get everything we asked for, and is it readable?*

It is a **report**, not enforcement. Enforcement lives in the Phase 3 builder,
which consumes the JSON written by `--json`.

What it checks, per finalized UTC day per venue directory:

1. **Finalized files only.** The live day's gzip member has no trailer until
   rotation or shutdown (`collector/src/file.rs`), so it cannot pass an
   integrity check by construction. `--include-today` overrides this for
   end-to-end testing, and then a truncated *last* member is a warning rather
   than corruption.
2. **gzip integrity** — a full decode through every member (a restart appends
   one; Python's `gzip` reads them transparently).
3. **Expected symbol x stream set** = the `session_start` record x the dataset
   profile. Missing required stream => red; missing optional => warning. The
   profile may also contradict the recording outright: mode A trades the
   Hyperliquid book, so a recording configured with no `l2Book` cadence at all
   is red however cleanly it was written.

   `session_start` is written **once per process** (`collector/src/main.rs`)
   while the sidecar rotates at UTC midnight (`collector/src/file.rs`), so the
   configuration for a day is looked up across every sidecar in the directory,
   not just that day's. Otherwise every day after a collector's first would be
   red for a configuration that never changed.
4. **Sequence gaps** — Binance USD-M `pu` chain, Bybit `u` per topic. Hyperliquid
   has no sequence number at all: cadence is the only evidence there.
5. **Cadence gaps** — a hole larger than K x the measured cadence of the channel.
6. **`local_ts` monotonicity** inside each symbol file.
7. **Coverage at both ends**, reported **per symbol** as the interval in which
   *every* required stream of that symbol is live (max of the firsts, min of the
   lasts). That — not the venue-wide union — is what Phase 3 must trim to: a
   union lets an on-time `bbo` hide an `l2Book` that started ten minutes late,
   and the run would begin over a window where the traded book does not exist.
   The venue-level number is kept as the union across symbols, for the operator.
8. **`_meta` cross-check** — a gap spanned by a `disconnected` / `dial_failed` /
   restart record is annotated as explained rather than left as a mystery.

The sidecar is deliberately NOT checked for monotonicity: `main.rs` writes the
disk gauge and the terminal records straight to the `Writer`, bypassing the
queue, so `_meta` is not ordered by `local_ts` by design (see the Phase 1 status
block in the design doc). Everything here sorts it before use.

**Timestamps are int64 nanoseconds end to end.** ~1.78e18 does not fit a float64
mantissa (2^53 ~ 9e15), so a single implicit float conversion silently corrupts
the coverage window Phase 3 trims on. Nothing in this file lets one become a
float — durations and thresholds are ints too, and only display formatting
divides.

Usage::

    quality_report.py --dir /data/hyperliquid --dir /data/binance \\
        [--day 20260725] [--include-today] \\
        [--json report.json] [--profile mode-a-v1]

Every finalized day present is checked unless `--day` narrows it to one.

Exit codes: 0 green/yellow, 1 red, 2 usage or I/O error.
"""

from __future__ import annotations

import argparse
import gzip
import json
import sys
import zlib
from bisect import bisect_left
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator, Optional

SCHEMA = "quality-report-v1"

GREEN = "green"
YELLOW = "yellow"
RED = "red"
_SEVERITY_ORDER = {GREEN: 0, YELLOW: 1, RED: 2}

SEC_NS = 1_000_000_000

#: How many gaps of one (symbol, stream) reach the JSON, and how many are named
#: individually in the issue list. A pathological file must not produce a report
#: nobody can read, but "gaps are listed by name, not summarised away" is the
#: doc's acceptance line — so the cap is generous and the remainder is counted.
MAX_GAPS_RECORDED = 200
MAX_GAP_ISSUES = 10

#: How far outside a gap a lifecycle record may sit and still explain it.
#:
#: Deliberately tiny. A record that accounts for a hole falls strictly inside
#: it: every backend stamps a data frame with `Utc::now()` at receive time, so
#: the disconnect that ended a burst is always stamped after the last frame of
#: that burst, and the `connected` that ended the outage before the first frame
#: of the next one. The margin only absorbs the sub-millisecond skew between two
#: files written by the same process — widen it and a `session_start` at the top
#: of the recording starts "explaining" every gap in the day.
EXPLAIN_MARGIN_NS = SEC_NS // 10


# ---------------------------------------------------------------------------
# small helpers
# ---------------------------------------------------------------------------


def utc_today() -> str:
    """Today's UTC day as `YYYYMMDD` — the day whose files are still open."""
    return datetime.now(timezone.utc).strftime("%Y%m%d")


def worst(verdicts) -> str:
    """The most severe verdict in `verdicts`; `green` for none at all."""
    out = GREEN
    for v in verdicts:
        if _SEVERITY_ORDER[v] > _SEVERITY_ORDER[out]:
            out = v
    return out


def iso(ts: Optional[int]) -> str:
    """`local_ts` as an ISO instant, keeping all nine digits. Display only."""
    if ts is None:
        return "-"
    whole, nanos = divmod(int(ts), SEC_NS)
    stamp = datetime.fromtimestamp(whole, timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
    return f"{stamp}.{nanos:09d}Z"


def fmt_dur(nanos: int) -> str:
    """A duration as `12.345s`, computed with integers only."""
    whole, rest = divmod(int(nanos), SEC_NS)
    return f"{whole}.{rest // 1_000_000:03d}s"


# ---------------------------------------------------------------------------
# dataset profile: what a venue is expected to have recorded
# ---------------------------------------------------------------------------

#: Venue families, which decide how a frame is classified into a stream.
HYPERLIQUID = "hyperliquid"
BINANCE = "binance"
BYBIT = "bybit"

_FAMILY = {
    "hyperliquid": HYPERLIQUID,
    "binancefutures": BINANCE,
    "binancefuturesum": BINANCE,
    "binancefuturescm": BINANCE,
    "binance": BINANCE,
    "binancespot": BINANCE,
    "bybit": BYBIT,
}

#: `collector/src/main.rs` matches `"binancefutures" | "binancefuturesum"` onto
#: the one USD-M backend but stamps the operator's spelling into `session_start`
#: verbatim. Canonicalised here, in one place, so the same recorded bytes are
#: not buildable or unbuildable depending on which word was typed on the command
#: line. The word as recorded stays in the report as `exchange_as_recorded`.
_EXCHANGE_ALIAS = {
    "binancefutures": "binancefuturesum",
}


def canonical_exchange(exchange: str) -> str:
    """The backend name a `session_start.exchange` value denotes."""
    return _EXCHANGE_ALIAS.get(exchange, exchange)

#: Largest hole in a channel that is still the feed working normally, in
#: nanoseconds. Derived from the cadences measured on mainnet 2026-07-25 and
#: recorded in `collector/README.md`:
#:
#:   * `l2Book` slow  5.41s x 10  — a throttled snapshot feed, so K can be tight
#:   * `l2Book` fast  0.54s x 10
#:   * `bbo`          0.14s x 100 — event-driven and bursty; a quiet book simply
#:                    stops changing, so a small K would flag every calm minute
#:   * `trades`       0.60s x 200 — droughts are legal, two minutes is not
#:
#: Binance and Bybit cadences have never been measured in this repository (the
#: README's capacity table covers HL and Bybit volume only), so rather than
#: inventing a cadence they get a flat absolute limit. It is deliberately loose:
#: this check exists to name reconnect-sized holes, not to grade liquidity.
MAX_GAP_NS = {
    (HYPERLIQUID, "l2Book_slow"): 54 * SEC_NS,
    (HYPERLIQUID, "l2Book_fast"): 5_400_000_000,
    (HYPERLIQUID, "bbo"): 14 * SEC_NS,
    (HYPERLIQUID, "trades"): 120 * SEC_NS,
    (BINANCE, "bookTicker"): 30 * SEC_NS,
    (BINANCE, "depthUpdate"): 30 * SEC_NS,
    (BINANCE, "trade"): 120 * SEC_NS,
    (BYBIT, "orderbook"): 30 * SEC_NS,
    (BYBIT, "publicTrade"): 120 * SEC_NS,
}


@dataclass(frozen=True)
class Expected:
    """The stream set one symbol of this venue must (or may) contain.

    `violation` is set when the *profile* contradicts the recording
    configuration rather than the data: a legal recording that can never make
    the dataset the profile describes. It is a property of the day's
    `session_start`, not of any one symbol, and it is red.
    """

    required: tuple
    optional: tuple
    violation: Optional[str] = None


def family_of(exchange: str) -> str:
    """The frame-shape family of a `session_start.exchange` value."""
    try:
        return _FAMILY[exchange]
    except KeyError:
        raise ValueError(
            f"unknown exchange {exchange!r} in session_start; "
            f"known: {', '.join(sorted(_FAMILY))}"
        ) from None


def expected_streams(profile: str, exchange: str, config: dict) -> Expected:
    """The expected stream set = recording configuration x dataset profile.

    `config` is the merged `session_start` for the day: `symbols`,
    `hl_l2_modes`, `bybit_depths`.

    Profile `mode-a-v1` is the contract of "Режим A" in the design document:
    Hyperliquid is the traded venue and Binance USD-M is the signal, whose only
    load-bearing stream is `@bookTicker`.
    """
    if profile != "mode-a-v1":
        raise ValueError(f"unknown profile {profile!r}")

    exchange = canonical_exchange(exchange)

    if exchange == "hyperliquid":
        # Every declared cadence is required, and only the declared ones are:
        # `--hl-l2-modes fast` is a legal recording and must not go red for the
        # slow frames it never asked for.
        required = ["trades", "bbo"]
        for mode in config.get("hl_l2_modes") or []:
            if mode == "slow":
                required.append("l2Book_slow")
            elif mode == "fast":
                required.append("l2Book_fast")
            # "none" asks for no book at all — see the violation below.
        violation = None
        if not any(s.startswith("l2Book") for s in required):
            # The profile contradicting the recording, which is the other half
            # of "session_start x dataset profile". Mode A's traded asset IS the
            # Hyperliquid book: `hyperliquid.convert` has branches for `trades`
            # and `l2Book` only, so a recording with no cadence at all converts
            # to a feed carrying no depth event, and every backtest step blocks
            # on `no_bid`. Cheaper to say so here than after a conversion.
            violation = (
                "session_start declares hl_l2_modes=%r, i.e. no l2Book cadence "
                "at all. Mode A trades the Hyperliquid book, so this recording "
                "cannot become a mode-A dataset however cleanly it was written: "
                "the converter emits no depth event and every backtest step "
                "would block with no bid. Record with --hl-l2-modes slow or fast."
                % (config.get("hl_l2_modes") or [],)
            )
        return Expected(tuple(dict.fromkeys(required)), (), violation)

    if exchange in ("binancefuturesum", "binancefuturescm"):
        # Mode A depends on `@bookTicker` alone. `@trade` and `@depth@0ms` are
        # recorded (open decision 1) and their absence is worth a warning —
        # without depth the recording is permanently unconvertible into a
        # tradable asset and the `pu` check above loses its input — but the
        # backtest itself does not read them.
        return Expected(("bookTicker",), ("trade", "depthUpdate"))

    if exchange == "bybit":
        # Bybit is not part of the mode-A dataset, so nothing it does can make
        # that dataset red. Its declared topics are still checked, as warnings,
        # because a silently rejected subscribe batch is exactly the failure
        # this report exists to catch.
        optional = [f"orderbook.{d}" for d in config.get("bybit_depths") or []]
        optional.append("publicTrade")
        return Expected((), tuple(optional))

    raise ValueError(
        f"profile {profile!r} defines no expected stream set for exchange "
        f"{exchange!r}. Mode A is Hyperliquid (traded) plus Binance USD-M "
        f"(signal); spot has no converter in this repository."
    )


# ---------------------------------------------------------------------------
# reading a recording
# ---------------------------------------------------------------------------


class TruncatedRecording(Exception):
    """The gzip stream ended before its member trailer, or would not decode."""


def parse_line(raw) -> tuple:
    """`b"<local_ts_ns> <raw_venue_json>"` -> `(int, dict)`.

    Split on the FIRST space only: the payload is raw JSON and contains plenty
    of its own. The timestamp is parsed with `int`, which is exact at any
    magnitude — `float` would lose the last two digits of a nanosecond stamp.
    """
    if isinstance(raw, (bytes, bytearray)):
        raw = raw.decode("utf-8")
    ts_str, _, payload = raw.partition(" ")
    return int(ts_str), json.loads(payload)


def iter_gz_lines(path) -> Iterator[bytes]:
    """Yields every line of a possibly multi-member gzip file.

    Raises `TruncatedRecording` at the point decoding fails, having already
    yielded everything that decoded — a truncated file is still evidence, and
    the checks that do not need the tail still run over what was read.
    """
    with gzip.open(path, "rb") as f:
        while True:
            try:
                line = f.readline()
            except (EOFError, gzip.BadGzipFile, zlib.error, OSError) as error:
                raise TruncatedRecording(str(error) or type(error).__name__) from error
            if not line:
                return
            yield line


@dataclass
class Gap:
    start_ts: int
    end_ts: int
    duration_ns: int
    explained_by: Optional[str] = None

    def as_json(self) -> dict:
        return {
            "start_local_ts": int(self.start_ts),
            "end_local_ts": int(self.end_ts),
            "duration_ns": int(self.duration_ns),
            "explained_by": self.explained_by,
        }


@dataclass
class StreamStat:
    count: int = 0
    first_ts: Optional[int] = None
    last_ts: Optional[int] = None
    gaps: list = field(default_factory=list)
    gap_count: int = 0

    def observe(self, ts: int, max_gap_ns: Optional[int]) -> None:
        if self.first_ts is None:
            self.first_ts = ts
        elif max_gap_ns is not None and self.last_ts is not None:
            delta = ts - self.last_ts
            if delta > max_gap_ns:
                self.gap_count += 1
                if len(self.gaps) < MAX_GAPS_RECORDED:
                    self.gaps.append(Gap(self.last_ts, ts, delta))
        self.last_ts = ts
        self.count += 1

    def as_json(self) -> dict:
        return {
            "count": self.count,
            "first_local_ts": None if self.first_ts is None else int(self.first_ts),
            "last_local_ts": None if self.last_ts is None else int(self.last_ts),
            "gap_count": self.gap_count,
            "gaps": [g.as_json() for g in self.gaps],
        }


@dataclass
class FileScan:
    path: str
    symbol: str
    exchange: str
    lines: int = 0
    truncated: bool = False
    truncation_error: Optional[str] = None
    malformed: int = 0
    malformed_example: Optional[str] = None
    unclassified: int = 0
    monotonic_violation: Optional[dict] = None
    streams: dict = field(default_factory=dict)
    sequence_breaks: dict = field(default_factory=dict)
    #: stream -> the first few breaks, as the interval frames were lost over.
    sequence_break_gaps: dict = field(default_factory=dict)

    def as_json(self) -> dict:
        return {
            "file": self.path,
            "lines": self.lines,
            "truncated": self.truncated,
            "malformed_lines": self.malformed,
            "unclassified_frames": self.unclassified,
            "monotonic_violation": self.monotonic_violation,
            "sequence_breaks": dict(self.sequence_breaks),
            "sequence_break_examples": {
                stream: [g.as_json() for g in gaps]
                for stream, gaps in sorted(self.sequence_break_gaps.items())
            },
            "streams": {name: s.as_json() for name, s in sorted(self.streams.items())},
        }


def classify(family: str, obj: dict) -> Optional[str]:
    """The stream a recorded frame belongs to, or `None` if unrecognised.

    Frame shapes come from the converters that read these files:
    `hyperliquid.py` (`channel`, and `data.fast` telling the two `l2Book`
    cadences apart) and `binancefutures.py` (combined-stream envelope, `data.e`).
    Bybit's topic string is `orderbook.<depth>.<symbol>` / `publicTrade.<symbol>`
    (`collector/src/bybit/mod.rs` routes on its last segment).
    """
    if family == HYPERLIQUID:
        channel = obj.get("channel")
        if channel == "l2Book":
            data = obj.get("data") or {}
            return "l2Book_fast" if data.get("fast") else "l2Book_slow"
        return channel if isinstance(channel, str) else None

    if family == BINANCE:
        data = obj.get("data")
        if isinstance(data, dict):
            event = data.get("e")
            return event if isinstance(event, str) else None
        # The REST depth snapshot the collector pulls after a `pu` break is
        # written into the symbol file bare, with no stream envelope
        # (`binancefuturesum/mod.rs`); the converter recognises it the same way.
        if "lastUpdateId" in obj:
            return "depthSnapshot"
        return None

    if family == BYBIT:
        topic = obj.get("topic")
        if isinstance(topic, str):
            parts = topic.split(".")
            return ".".join(parts[:-1]) if len(parts) > 1 else topic
        return None

    return None


def gap_limit(family: str, stream: str) -> Optional[int]:
    """The cadence limit for a channel, or `None` if it has no expectation."""
    if (family, stream) in MAX_GAP_NS:
        return MAX_GAP_NS[(family, stream)]
    # Bybit's stream name carries the depth (`orderbook.50`); the limit does not
    # depend on it.
    head = stream.split(".", 1)[0]
    return MAX_GAP_NS.get((family, head))


#: How many break intervals are kept per chain. They exist to point an
#: investigation at the first few; a file that breaks a million times must not
#: cost a million entries.
MAX_BREAK_EXAMPLES = 10


def _track_sequence(
    scan: FileScan, family: str, stream: str, obj: dict, ts: int, prev: dict
) -> None:
    """Counts breaks in whatever sequence number the venue provides.

    Binance USD-M: `data.pu` of a `depthUpdate` must equal the previous `data.u`
    (`collector/src/binancefuturesum/mod.rs` uses the same rule live, to decide
    when to re-pull a REST snapshot). Bybit: `u` increments by one per topic,
    and a `snapshot` frame restarts the chain. Hyperliquid publishes no sequence
    number at all — for it, cadence is the only evidence there is.

    `prev` is the caller's per-file chain state: `stream -> (last id, its ts)`.
    A break is recorded as the interval between the two frames it sits between,
    so the sidecar can explain it exactly as it explains a cadence gap — the
    first `depthUpdate` after a reconnect breaks the chain by construction.
    """

    def note_break(since: int) -> None:
        scan.sequence_breaks[stream] += 1
        examples = scan.sequence_break_gaps.setdefault(stream, [])
        if len(examples) < MAX_BREAK_EXAMPLES:
            examples.append(Gap(since, ts, ts - since))

    if family == BINANCE and stream == "depthUpdate":
        data = obj.get("data") or {}
        u, pu = data.get("u"), data.get("pu")
        if not isinstance(u, int) or not isinstance(pu, int):
            return
        scan.sequence_breaks.setdefault(stream, 0)
        last = prev.get(stream)
        if last is not None and pu != last[0]:
            note_break(last[1])
        prev[stream] = (u, ts)
        return

    if family == BYBIT and stream.startswith("orderbook"):
        data = obj.get("data") or {}
        u = data.get("u")
        if not isinstance(u, int):
            return
        scan.sequence_breaks.setdefault(stream, 0)
        last = prev.get(stream)
        if obj.get("type") == "snapshot":
            # A snapshot is the venue restarting the chain, not a loss.
            prev[stream] = (u, ts)
            return
        if last is not None and u != last[0] + 1:
            note_break(last[1])
        prev[stream] = (u, ts)


def scan_symbol_file(path, exchange: str) -> FileScan:
    """Reads one `<symbol>_<day>.gz` and accumulates everything the report needs.

    Streaming and aggregate-only: a Bybit day is millions of lines, and holding
    the parsed frames would cost gigabytes for numbers that fit in a handful of
    counters.
    """
    path = Path(path)
    family = family_of(exchange)
    symbol = path.name.rsplit("_", 1)[0]
    scan = FileScan(path=str(path), symbol=symbol, exchange=exchange)
    prev_ts = None
    prev_id: dict = {}

    try:
        for lineno, raw in enumerate(iter_gz_lines(path), start=1):
            scan.lines += 1
            try:
                ts, obj = parse_line(raw)
            except (ValueError, UnicodeDecodeError, json.JSONDecodeError):
                scan.malformed += 1
                if scan.malformed_example is None:
                    text = raw.decode("utf-8", "replace") if isinstance(raw, bytes) else str(raw)
                    scan.malformed_example = f"line {lineno}: {text[:120].rstrip()}"
                continue

            if prev_ts is not None and ts < prev_ts:
                # Equal stamps are fine — two frames can share a nanosecond.
                # Going backwards cannot happen in a single writer appending in
                # receive order, so it means a clock step or two collectors in
                # one directory (which `lock.rs` exists to prevent). The count
                # separates those: one step is a clock, a million is two
                # interleaved recordings.
                if scan.monotonic_violation is None:
                    scan.monotonic_violation = {
                        "line": lineno,
                        "previous_local_ts": int(prev_ts),
                        "local_ts": int(ts),
                        "violations": 0,
                    }
                scan.monotonic_violation["violations"] += 1
            prev_ts = ts

            if not isinstance(obj, dict):
                scan.unclassified += 1
                continue
            stream = classify(family, obj)
            if stream is None:
                scan.unclassified += 1
                continue

            stat = scan.streams.get(stream)
            if stat is None:
                stat = scan.streams[stream] = StreamStat()
            stat.observe(ts, gap_limit(family, stream))
            _track_sequence(scan, family, stream, obj, ts, prev_id)
    except TruncatedRecording as error:
        scan.truncated = True
        scan.truncation_error = str(error)

    return scan


# ---------------------------------------------------------------------------
# the sidecar
# ---------------------------------------------------------------------------


def read_meta(path) -> list:
    """`(local_ts | None, record)` for every readable line of a sidecar.

    Lines carry the same `<local_ts> <json>` prefix as market data
    (`RotatingFile::write`), but the file is plain text, appended by several
    writers, and NOT ordered by `local_ts` — see the module docstring.
    Unreadable lines are skipped rather than fatal: the sidecar is diagnostic,
    and losing the tail of it must not stop the report.
    """
    out = []
    with open(path, "rb") as f:
        for raw in f:
            raw = raw.strip()
            if not raw:
                continue
            try:
                ts, obj = parse_line(raw)
            except (ValueError, UnicodeDecodeError, json.JSONDecodeError):
                try:
                    obj, ts = json.loads(raw.decode("utf-8", "replace")), None
                except (ValueError, json.JSONDecodeError):
                    continue
            if isinstance(obj, dict):
                out.append((ts, obj))
    return out


def sidecar_paths(data_dir: Path) -> list:
    """Every sidecar in the directory, oldest name first.

    Deliberately not per day: `session_start` is written once per process
    (`collector/src/main.rs`) while `RotatingFile` opens a new sidecar at every
    UTC midnight, so a day's configuration usually lives in an older file.
    `session_records_for_day` does the time-scoping afterwards.
    """
    return sorted(data_dir.glob("_meta_*.jsonl"))


def day_bounds_ns(day: str) -> tuple:
    """`[start, end)` of a UTC `YYYYMMDD` day, in integer nanoseconds."""
    midnight = datetime.strptime(day, "%Y%m%d").replace(tzinfo=timezone.utc)
    # A whole-second epoch is exact in float64 (1.8e9 << 2^53); the nanosecond
    # scaling that would not be is done in ints.
    start = int(midnight.timestamp()) * SEC_NS
    return start, start + 86_400 * SEC_NS


def session_records_for_day(records, day: str) -> list:
    """The `session_start` records in force during `day`.

    The collector writes one per process (`collector/src/main.rs`, inside the
    startup block) while `RotatingFile` opens a new sidecar at every UTC
    midnight (`collector/src/file.rs`). So a process started on Monday leaves
    the only description of Tuesday's recording in *Monday's* sidecar, and
    looking for it in Tuesday's finds nothing.

    In force during a day means: every `session_start` stamped inside it, plus
    the last one before it — the process that was already running. A later
    restart's configuration is deliberately excluded, so a day that only ever
    ran `slow` is not judged against the `fast` a Wednesday restart added.
    An undated record (a sidecar line without the `<ts> ` prefix) cannot be
    placed in time and is kept for every day rather than dropped.
    """
    start_ns, end_ns = day_bounds_ns(day)
    inside, before, undated = [], None, []
    for ts, rec in records:
        if rec.get("_collector") != "session_start":
            continue
        if ts is None:
            undated.append((ts, rec))
        elif start_ns <= ts < end_ns:
            inside.append((ts, rec))
        elif ts < start_ns and (before is None or ts > before[0]):
            before = (ts, rec)
    out = list(undated)
    if before is not None:
        out.append(before)
    out.extend(sorted(inside, key=lambda pair: pair[0]))
    return out


def merge_session_config(records) -> Optional[dict]:
    """One recording configuration for the day, from every `session_start`.

    A restart writes another `session_start`, and its configuration may differ.
    The union is the right expectation for a whole-day presence check: a day
    that ran `slow,fast` and then `fast` did record slow frames, and requiring
    them is correct; a day that only ever ran `fast` never asked for them and
    must not go red.
    """
    exchange = None
    symbols, modes, depths = [], [], []
    seen = False
    for _, rec in records:
        if rec.get("_collector") != "session_start":
            continue
        seen = True
        found = rec.get("exchange")
        if exchange is not None and found != exchange:
            # Two instances recorded into one directory. `lock.rs` prevents that
            # happening live, but a directory can also be assembled afterwards
            # for conversion (README, "Output format"), and there the two
            # configurations would silently merge into one expected set.
            raise ValueError(
                f"sidecars for this day name two exchanges ({exchange!r} and "
                f"{found!r}); a report directory must hold one collector "
                f"instance, so split them before checking"
            )
        exchange = exchange or found
        for src, dst in (
            (rec.get("symbols"), symbols),
            (rec.get("hl_l2_modes"), modes),
            (rec.get("bybit_depths"), depths),
        ):
            for item in src or []:
                if item not in dst:
                    dst.append(item)
    if not seen:
        return None
    return {
        "exchange": exchange,
        "symbols": symbols,
        "hl_l2_modes": modes,
        "bybit_depths": depths,
    }


#: Sidecar events that explain a hole, most conclusive first. A restart
#: (`session_start`) explains one as surely as a `disconnected` does.
_EXPLANATORY = (
    "disconnected",
    "dial_failed",
    "stalled",
    "queue_overflow",
    "hand_off_closed",
    "disk_exhausted",
    "symbol_check_failed",
    "stream_ended",
    "session_start",
    "subscribe",
    "connected",
)


def lifecycle_events(records) -> list:
    """`(ts, event)` for the collector's own records, sorted by `local_ts`."""
    out = [
        (ts, rec["_collector"])
        for ts, rec in records
        if ts is not None and isinstance(rec.get("_collector"), str)
    ]
    out.sort(key=lambda pair: pair[0])
    return out


#: Most lifecycle records one gap is described with. A reconnect storm can put
#: thousands inside a single hole (`README.md`, "Going silent": a couple a
#: second while a venue refuses connections), and the report only ever names
#: the most conclusive one and counts the rest.
MAX_EVENTS_PER_GAP = 1000


def explain_gap(gap: Gap, events) -> Optional[str]:
    """Names the lifecycle record that accounts for a gap, if there is one.

    `events` must be sorted by timestamp, which is what `lifecycle_events`
    guarantees — the sidecar itself is not ordered (see the module docstring).
    Located by bisection: this runs once per gap, and a day can hold both many
    gaps and many events.
    """
    lo = gap.start_ts - EXPLAIN_MARGIN_NS
    hi = gap.end_ts + EXPLAIN_MARGIN_NS
    start = bisect_left(events, (lo,))
    inside = []
    for ts, name in events[start : start + MAX_EVENTS_PER_GAP]:
        if ts > hi:
            break
        inside.append((ts, name))
    if not inside:
        return None
    for wanted in _EXPLANATORY:
        for ts, name in inside:
            if name == wanted:
                extra = f" (+{len(inside) - 1} more)" if len(inside) > 1 else ""
                return f"{name} at {iso(ts)}{extra}"
    ts, name = inside[0]
    return f"{name} at {iso(ts)}"


# ---------------------------------------------------------------------------
# per-day checking
# ---------------------------------------------------------------------------


def issue(severity: str, check: str, detail: str) -> dict:
    return {"severity": severity, "check": check, "detail": detail}


def discover_days(data_dir: Path) -> list:
    """Every day the directory holds a symbol file or a sidecar for.

    A day with a sidecar and no market data is included on purpose: "the
    collector ran and recorded nothing" is precisely the failure this report is
    for, and skipping it would report that day as absent instead of broken.
    """
    days = set()
    for entry in data_dir.iterdir():
        name = entry.name
        if name.endswith(".gz"):
            candidate = name[:-3].rsplit("_", 1)[-1]
        elif name.startswith("_meta_") and name.endswith(".jsonl"):
            candidate = name[: -len(".jsonl")].rsplit("_", 1)[-1]
        else:
            continue
        if len(candidate) == 8 and candidate.isdigit():
            days.add(candidate)
    return sorted(days)


def identify_venue(data_dir: Path) -> str:
    """The venue of a collector directory, from any `session_start` it holds."""
    for path in sidecar_paths(data_dir):
        config = merge_session_config(read_meta(path))
        if config and config.get("exchange"):
            return config["exchange"]
    raise ValueError(
        f"{data_dir}: no `session_start` record in any _meta_*.jsonl sidecar, so "
        f"the venue cannot be identified. A directory without a sidecar cannot "
        f"be checked against the configuration it was recorded with."
    )


def check_day(
    data_dir: Path,
    exchange: str,
    day: str,
    profile: str,
    is_today: bool,
    meta_records=None,
) -> dict:
    """The full report for one venue-day. Returns the JSON `days[<day>]` value.

    `meta_records` is every sidecar record of the directory, across all days —
    `session_start` is per process, not per day. Passed in so a multi-day report
    parses each sidecar once.
    """
    issues = []
    symbols_json = {}
    coverage_first = None
    coverage_last = None

    if meta_records is None:
        meta_records = []
        for path in sidecar_paths(data_dir):
            meta_records.extend(read_meta(path))
    events = lifecycle_events(meta_records)
    config = merge_session_config(session_records_for_day(meta_records, day))

    expected = None
    if config is None:
        issues.append(
            issue(
                RED,
                "meta_missing",
                f"no session_start record in any _meta_*.jsonl of {data_dir} that "
                f"applies to {day} (one is written per process, so the whole "
                f"directory is searched); the expected symbol x stream set for "
                f"this day is unknown, so completeness cannot be verified",
            )
        )
    else:
        expected = expected_streams(profile, exchange, config)
        if expected.violation:
            issues.append(issue(RED, "profile_unsatisfiable", expected.violation))

    present = {}
    for path in sorted(data_dir.glob(f"*_{day}.gz")):
        present[path.name[:-3].rsplit("_", 1)[0]] = path

    wanted = [s.lower() for s in (config or {}).get("symbols", [])]
    for name in sorted(present):
        if config is not None and name not in wanted:
            issues.append(
                issue(
                    YELLOW,
                    "unexpected_symbol",
                    f"{name}: recorded but not in session_start.symbols "
                    f"({', '.join(wanted) or 'none'}) — a leftover file, or a "
                    f"configuration that changed without the day rolling over",
                )
            )

    for name in wanted + [s for s in sorted(present) if s not in wanted]:
        path = present.get(name)
        if path is None:
            missing = list(expected.required) if expected else []
            if missing:
                issues.append(
                    issue(
                        RED,
                        "missing_required",
                        f"{name}: no {name}_{day}.gz at all — required streams "
                        f"{', '.join(missing)} are absent for the whole day",
                    )
                )
            else:
                issues.append(
                    issue(
                        YELLOW,
                        "missing_optional",
                        f"{name}: no {name}_{day}.gz at all",
                    )
                )
            symbols_json[name] = {
                "file": None,
                "lines": 0,
                "truncated": False,
                "malformed_lines": 0,
                "unclassified_frames": 0,
                "monotonic_violation": None,
                "sequence_breaks": {},
                "sequence_break_examples": {},
                "streams": {},
                "missing_required": missing,
                "missing_optional": list(expected.optional) if expected else [],
                "coverage": {
                    "first_local_ts": None,
                    "last_local_ts": None,
                    "required_streams": missing,
                },
            }
            continue

        scan = scan_symbol_file(path, exchange)

        if scan.truncated:
            if is_today:
                # The live member has no trailer until rotation or shutdown
                # (`file.rs`), so this is what a healthy recording in progress
                # looks like. Only the LAST member can be open, and everything
                # before it decoded, so the day is still usable — but it is not
                # finalized, and the report says so rather than staying silent.
                issues.append(
                    issue(
                        YELLOW,
                        "gzip_integrity",
                        f"{name}: last gzip member is unfinished ({scan.truncation_error}) "
                        f"— expected for today's open file; {scan.lines} line(s) decoded",
                    )
                )
            else:
                issues.append(
                    issue(
                        RED,
                        "gzip_integrity",
                        f"{name}: gzip decode failed on a finalized day "
                        f"({scan.truncation_error}) after {scan.lines} line(s) — "
                        f"the member trailer is missing or the file is damaged",
                    )
                )

        if scan.malformed:
            issues.append(
                issue(
                    YELLOW,
                    "malformed_line",
                    f"{name}: {scan.malformed} line(s) could not be parsed; "
                    f"first at {scan.malformed_example}",
                )
            )

        if scan.unclassified:
            issues.append(
                issue(
                    YELLOW,
                    "unclassified_frame",
                    f"{name}: {scan.unclassified} frame(s) matched no known "
                    f"stream of {exchange}",
                )
            )

        if scan.monotonic_violation:
            v = scan.monotonic_violation
            issues.append(
                issue(
                    RED,
                    "monotonicity",
                    f"{name}: local_ts goes backwards {v['violations']} time(s); "
                    f"first at line {v['line']}: "
                    f"{iso(v['previous_local_ts'])} -> {iso(v['local_ts'])}",
                )
            )

        missing_required, missing_optional = [], []
        if expected is not None:
            for stream in expected.required:
                if scan.streams.get(stream, StreamStat()).count == 0:
                    missing_required.append(stream)
            for stream in expected.optional:
                if scan.streams.get(stream, StreamStat()).count == 0:
                    missing_optional.append(stream)
            if missing_required:
                issues.append(
                    issue(
                        RED,
                        "missing_required",
                        f"{name}: no frames on required stream(s) "
                        f"{', '.join(missing_required)} "
                        f"(recorded: {', '.join(sorted(scan.streams)) or 'nothing'})",
                    )
                )
            if missing_optional:
                issues.append(
                    issue(
                        YELLOW,
                        "missing_optional",
                        f"{name}: no frames on optional stream(s) "
                        f"{', '.join(missing_optional)}",
                    )
                )

        for stream, count in sorted(scan.sequence_breaks.items()):
            if not count:
                continue
            examples = scan.sequence_break_gaps.get(stream, [])
            for gap in examples:
                gap.explained_by = explain_gap(gap, events)
            shown = []
            for gap in examples[:3]:
                tail = f" [{gap.explained_by}]" if gap.explained_by else ""
                shown.append(f"{iso(gap.end_ts)}{tail}")
            issues.append(
                issue(
                    YELLOW,
                    "sequence_gap",
                    f"{name}/{stream}: {count} sequence break(s), frames lost between "
                    f"consecutive updates; first at {', '.join(shown)}",
                )
            )

        for stream, stat in sorted(scan.streams.items()):
            named = 0
            for gap in stat.gaps:
                gap.explained_by = explain_gap(gap, events)
                if named < MAX_GAP_ISSUES:
                    named += 1
                    limit = gap_limit(family_of(exchange), stream)
                    tail = (
                        f"explained by {gap.explained_by}"
                        if gap.explained_by
                        else "unexplained by _meta"
                    )
                    issues.append(
                        issue(
                            YELLOW,
                            "cadence_gap",
                            f"{name}/{stream}: {fmt_dur(gap.duration_ns)} gap "
                            f"{iso(gap.start_ts)} -> {iso(gap.end_ts)} "
                            f"(limit {fmt_dur(limit)}); {tail}",
                        )
                    )
            if stat.gap_count > named:
                issues.append(
                    issue(
                        YELLOW,
                        "cadence_gap",
                        f"{name}/{stream}: {stat.gap_count - named} further gap(s) "
                        f"not listed individually; see the JSON report",
                    )
                )

        entry = scan.as_json()
        entry["missing_required"] = missing_required
        entry["missing_optional"] = missing_optional

        if expected is not None:
            # Coverage is measured over the REQUIRED streams: an optional feed
            # that started earlier or ran later must not widen the window Phase
            # 3 trims both venues to. A venue the profile requires nothing of
            # (Bybit under mode-a-v1) would otherwise report a null window
            # despite holding data, so there it falls back to everything
            # recorded — the number is informational for such a venue anyway.
            measured = expected.required or tuple(sorted(scan.streams))
            #: Per symbol, coverage is the interval in which EVERY required
            #: stream is live: max of the firsts, min of the lasts. Phase 3
            #: trims to this. The union would let an on-time bbo hide an l2Book
            #: that started ten minutes late, and the run would begin over a
            #: window whose traded book does not exist yet (§3.1).
            sym_first = sym_last = None
            complete = bool(measured)
            for stream in measured:
                stat = scan.streams.get(stream)
                if stat is None or stat.first_ts is None:
                    complete = False
                    continue
                if sym_first is None or stat.first_ts > sym_first:
                    sym_first = stat.first_ts
                if sym_last is None or stat.last_ts < sym_last:
                    sym_last = stat.last_ts
                # The venue-level number stays the union across symbols and
                # streams: an operator reading it wants "when was this venue
                # recording at all", and Phase 3 no longer builds on it.
                if coverage_first is None or stat.first_ts < coverage_first:
                    coverage_first = stat.first_ts
                if coverage_last is None or stat.last_ts > coverage_last:
                    coverage_last = stat.last_ts
            if not complete or (sym_first is not None and sym_last is not None
                                and sym_first > sym_last):
                # A required stream missing, or two whose live intervals do not
                # overlap at all. Either way there is no interval in which the
                # symbol is fully recorded; `missing_required` already made the
                # first case red.
                sym_first = sym_last = None
            entry["coverage"] = {
                "first_local_ts": None if sym_first is None else int(sym_first),
                "last_local_ts": None if sym_last is None else int(sym_last),
                "required_streams": list(measured),
            }
        else:
            entry["coverage"] = {
                "first_local_ts": None,
                "last_local_ts": None,
                "required_streams": [],
            }

        symbols_json[name] = entry

    return {
        "verdict": worst(i["severity"] for i in issues),
        "issues": issues,
        "symbols": symbols_json,
        # Not part of the JSON contract: stripped by `build_report` after the
        # venue-level coverage has been folded together.
        "_coverage": (coverage_first, coverage_last),
    }


def build_report(dirs, profile: str, day: Optional[str], include_today: bool) -> dict:
    """Runs every check over every directory and returns the report document."""
    today = utc_today()
    venues = {}

    for data_dir in dirs:
        # Resolved, so `data_dir` in the JSON is absolute. The Phase 3 builder
        # resolves a relative one against the *report file's* directory, which
        # is not where this ran — `--dir data/hyperliquid --json out/r.json`
        # would send it looking in `out/data/hyperliquid`.
        data_dir = Path(data_dir).resolve()
        if not data_dir.is_dir():
            raise FileNotFoundError(f"{data_dir} is not a directory")
        recorded_exchange = identify_venue(data_dir)
        exchange = canonical_exchange(recorded_exchange)
        if exchange in venues:
            raise ValueError(
                f"{data_dir} and {venues[exchange]['data_dir']} are both "
                f"{exchange!r}; one venue per report entry, so pass them "
                f"separately or merge the directories first"
            )

        # Parsed once for the whole directory: `session_start` is written per
        # process, so every day's configuration may live in an older sidecar.
        meta_records = []
        for path in sidecar_paths(data_dir):
            meta_records.extend(read_meta(path))

        days = [day] if day else discover_days(data_dir)
        if not include_today:
            days = [d for d in days if d != today]

        day_results = {}
        first_ts = last_ts = None
        for d in sorted(days):
            result = check_day(data_dir, exchange, d, profile, is_today=(d == today),
                               meta_records=meta_records)
            cov_first, cov_last = result.pop("_coverage")
            if cov_first is not None and (first_ts is None or cov_first < first_ts):
                first_ts = cov_first
            if cov_last is not None and (last_ts is None or cov_last > last_ts):
                last_ts = cov_last
            day_results[d] = result

        # No day checked at all is not a pass. Nothing was verified, and a gate
        # that reports green on an empty directory is worse than no gate.
        verdict = worst(r["verdict"] for r in day_results.values()) if day_results else RED

        venues[exchange] = {
            "data_dir": str(data_dir),
            # `binancefutures` and `binancefuturesum` are one backend
            # (`collector/src/main.rs`); the key is canonical, this is the word
            # the operator actually recorded with.
            "exchange_as_recorded": recorded_exchange,
            "verdict": verdict,
            "coverage": {
                "first_local_ts": None if first_ts is None else int(first_ts),
                "last_local_ts": None if last_ts is None else int(last_ts),
                "note": "union over symbols and required streams — the operator's "
                        "view of when this venue was recording. Phase 3 trims to "
                        "days[].symbols[].coverage instead, which is per symbol "
                        "and intersects its required streams.",
            },
            "days": day_results,
        }

    return {
        "schema": SCHEMA,
        "profile": profile,
        "verdict": worst(v["verdict"] for v in venues.values()) if venues else RED,
        "venues": venues,
    }


# ---------------------------------------------------------------------------
# human-readable output
# ---------------------------------------------------------------------------


def render_text(report: dict) -> str:
    """The operator's view: every issue named, never a bare "looks fine"."""
    lines = [
        f"quality report  schema={report['schema']}  profile={report['profile']}  "
        f"verdict={report['verdict'].upper()}"
    ]
    if not report["venues"]:
        lines.append("  (no venue directories were checked)")
        return "\n".join(lines) + "\n"

    for exchange, venue in sorted(report["venues"].items()):
        lines.append("")
        recorded = venue.get("exchange_as_recorded")
        alias = f" (recorded as {recorded})" if recorded and recorded != exchange else ""
        lines.append(
            f"=== {exchange}{alias}  {venue['data_dir']}  [{venue['verdict'].upper()}]"
        )
        cov = venue["coverage"]
        lines.append(
            f"    coverage (union over symbols): {iso(cov['first_local_ts'])} .. "
            f"{iso(cov['last_local_ts'])}"
        )
        if not venue["days"]:
            lines.append("    no finalized day to check — nothing was verified")
            continue

        for day, result in sorted(venue["days"].items()):
            lines.append(f"  -- {day}  [{result['verdict'].upper()}]")
            for name, sym in sorted(result["symbols"].items()):
                if not sym["streams"]:
                    lines.append(f"     {name:<12} (no frames)")
                for stream, stat in sorted(sym["streams"].items()):
                    lines.append(
                        f"     {name:<12} {stream:<14} n={stat['count']:<9} "
                        f"{iso(stat['first_local_ts'])} .. {iso(stat['last_local_ts'])} "
                        f"gaps={stat['gap_count']}"
                    )
                cov = sym.get("coverage") or {}
                if cov.get("required_streams"):
                    lines.append(
                        f"     {name:<12} {'coverage':<14} "
                        f"{iso(cov['first_local_ts'])} .. {iso(cov['last_local_ts'])} "
                        f"(all of: {', '.join(cov['required_streams'])})"
                    )
            if not result["issues"]:
                lines.append("     no issues")
            for i in result["issues"]:
                lines.append(f"     [{i['severity']:<6}] {i['check']:<19} {i['detail']}")

    return "\n".join(lines) + "\n"


# ---------------------------------------------------------------------------
# entry point
# ---------------------------------------------------------------------------


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="quality_report.py",
        description=(
            "Offline quality report over raw collector recordings "
            "(Phase 2 of docs/design-multi-venue-collection.md)."
        ),
    )
    parser.add_argument(
        "--dir",
        dest="dirs",
        action="append",
        required=True,
        metavar="DATA_DIR",
        help="One collector instance directory; repeat for several venues. The "
        "venue is read from session_start.exchange in its _meta sidecar.",
    )
    parser.add_argument(
        "--day",
        metavar="YYYYMMDD",
        help="Check this UTC day only. Default: every finalized day present.",
    )
    parser.add_argument(
        "--include-today",
        action="store_true",
        help="Also check today's UTC day. Its last gzip member is still open, "
        "so it is decoded as far as it goes and the missing trailer is a "
        "warning rather than corruption.",
    )
    parser.add_argument("--json", dest="json_out", metavar="OUT", help="Write the report here.")
    parser.add_argument(
        "--profile",
        default="mode-a-v1",
        help="Dataset profile deciding which streams are required (default: mode-a-v1).",
    )
    return parser


def main(argv=None) -> int:
    parser = build_parser()
    try:
        args = parser.parse_args(argv)
    except SystemExit as exit_code:  # argparse already printed the reason
        # `--help` exits 0 and is not a usage error; `code or 2` turned it into
        # one, so every wrapper treating non-zero as failure failed on --help.
        return 2 if exit_code.code is None else int(exit_code.code)

    if args.day is not None:
        if len(args.day) != 8 or not args.day.isdigit():
            print(f"--day expects YYYYMMDD, got {args.day!r}", file=sys.stderr)
            return 2
        if args.day == utc_today() and not args.include_today:
            print(
                f"--day {args.day} is today (UTC) and its gzip member is still "
                f"open; pass --include-today to check an unfinalized recording",
                file=sys.stderr,
            )
            return 2

    try:
        report = build_report(args.dirs, args.profile, args.day, args.include_today)
    except (ValueError, FileNotFoundError, NotADirectoryError, PermissionError, OSError) as error:
        print(f"error: {error}", file=sys.stderr)
        return 2

    sys.stdout.write(render_text(report))

    if args.json_out:
        try:
            with open(args.json_out, "w") as f:
                json.dump(report, f, indent=2)
                f.write("\n")
        except OSError as error:
            print(f"error: couldn't write {args.json_out}: {error}", file=sys.stderr)
            return 2

    return 1 if report["verdict"] == RED else 0


if __name__ == "__main__":
    sys.exit(main())
