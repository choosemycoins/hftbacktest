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
   profile. Missing required stream => red; missing optional => warning; missing
   *informational* => nothing at all, only a line in the JSON (see `Expected`:
   a stream added to the collector after a recording was made cannot be
   backfilled, and warning about it would yellow every historical day at once).
   The profile may also contradict the recording outright: mode A trades the
   Hyperliquid book, so a recording configured with no `l2Book` cadence at all
   is red however cleanly it was written.

   `session_start` is written **once per process** (`collector/src/main.rs`)
   while the sidecar rotates at UTC midnight (`collector/src/file.rs`), so the
   configuration for a day is looked up across every sidecar in the directory,
   not just that day's. Otherwise every day after a collector's first would be
   red for a configuration that never changed.
4. **Sequence gaps** — Binance USD-M `pu` chain, Bybit `u` per topic. Hyperliquid
   has no sequence number at all: cadence is the only evidence there.
5. **Cadence gaps** — a hole larger than K x the measured cadence of the channel,
   except where a steadier stream on the same socket was **running across that
   hole** and had none of its own (see `LIVENESS_REFERENCE` and
   `liveness_witness`). Bounded: no reference excuses a hole past
   `MAX_SUPPRESSED_GAP_FACTOR` x the channel's own limit, and none excuses one
   the sidecar already accounts for.
6. **`local_ts` monotonicity, per stream** — with a tolerance for the order two
   streams are interleaved in, allowed only where a second producer exists to
   have raced (`_SECOND_PRODUCER`) and only up to the socket hop that separates
   them (`CROSS_STREAM_TOLERANCE_NS`).
7. **Coverage at both ends**, reported **per symbol** as the interval in which
   *every* required stream of that symbol is live (max of the firsts, min of the
   lasts). That — not the venue-wide union — is what Phase 3 must trim to: a
   union lets an on-time `bbo` hide an `l2Book` that started ten minutes late,
   and the run would begin over a window where the traded book does not exist.
   The venue-level number is kept as the union across symbols, for the operator.
8. **`_meta` cross-check** — a gap spanned by a `disconnected` / `dial_failed` /
   restart record is annotated as explained rather than left as a mystery. Only
   the collector's *lifecycle* records may do that (`_EXPLANATORY`): the minutely
   disk gauge lands inside every hole longer than a minute and explains none of
   them.

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

#: Streams a *second*, concurrent producer writes.
#:
#: Load-bearing, not decoration: this is the whole reason two streams may be
#: written out of `local_ts` order, so it is also what decides whether an
#: inversion is tolerable at all. Where no entry matches, the venue has one WS
#: reader stamping and queueing every frame of a symbol file, write order IS
#: receive order, and a step backwards is a defect at any size — see
#: `scan_symbol_file`, which records where that was verified per venue.
_SECOND_PRODUCER = {
    "depthSnapshot": (
        "the REST depth-snapshot fetcher is one such producer: it runs detached "
        "(tokio::spawn, the same shape in all three binance* backends) and hands "
        "its frame straight to the writer, skipping the socket hop that WS "
        "frames queue through first"
    ),
    "premiumIndex": (
        "the premium-index poller is one such producer: it runs on its own "
        "timer beside the socket loop (binancefuturesum/mod.rs) and hands each "
        "element straight to the writer, skipping the socket hop that WS frames "
        "queue through first"
    ),
}

#: Said of an inversion between two streams that share the one producer.
_NO_SECOND_PRODUCER = (
    "no second producer is known for this venue's symbol files: one WS reader "
    "stamps every frame at receive time and hands them on in that order"
)


def second_producer_of(prev_stream, stream) -> Optional[str]:
    """The concurrent producer that can put these two out of order, if any."""
    for name in (prev_stream, stream):
        if name in _SECOND_PRODUCER:
            return _SECOND_PRODUCER[name]
    return None

#: The two numbers in `collector/src/queue.rs` the interleave bound is derived
#: from — the socket hop's depth (`WS_QUEUE_CAPACITY`) and the burst rate it was
#: sized against (`burst::PEAK_MSG_PER_S`, measured 2026-07-26 22:00 UTC).
#:
#: Mirrored rather than imported: this is a Python tool reading recorded bytes,
#: not the collector. `test_the_interleave_bound_still_covers_the_socket_hop_it_
#: is_derived_from` reads the Rust and fails if either number moves, so raising
#: the capacity re-checks the gate instead of silently reddening burst days.
WS_QUEUE_CAPACITY = 4096
PEAK_MSG_PER_S = 20_000

#: The longest a WS frame can sit behind the REST snapshot that skips it: the
#: whole socket hop, drained at the measured peak. 4096 / 20 000 = 204.8ms.
SOCKET_HOP_NS = WS_QUEUE_CAPACITY * SEC_NS // PEAK_MSG_PER_S

#: How far the *write* order of two different streams may disagree with their
#: `local_ts` order before it stops being an interleave and becomes a defect.
#:
#: A symbol file is written by one queue, but not always by one producer. Every
#: backend stamps `Utc::now()` at its own receive moment, and the Binance
#: backends run a second producer: the REST depth-snapshot fetcher is detached
#: (`tokio::spawn` in `binancefuturesum/mod.rs`) and sends **straight to the
#: writer hop**, while WS frames queue through the socket hop first. A snapshot
#: can therefore be written ahead of market data stamped earlier. Both stamps
#: are honest; only the interleaving is out of order.
#:
#: Sized from that hop rather than from the observation. Measured on ethusdt,
#: 2026-07-26: one inversion per ~5M lines, always at a REST refetch (12 that
#: day), worst 134us — but that was a writer keeping up. The two hops cancel
#: (both frames pass the writer hop), so the honest maximum is exactly
#: `SOCKET_HOP_NS`, and 250ms is that with ~20% of headroom. A bound at the
#: observed 134us, or at the 10ms this shipped with, would go red on precisely
#: the burst days whose data matters most: a burst is also what breaks the `pu`
#: chain the refetch responds to, so a deep socket hop and a REST snapshot
#: co-occur by construction. Red is a hard build refusal in `build_dataset.py`.
#:
#: Two things this bound is NOT. It does not apply within one stream: there is
#: one producer appending in receive order, so a step backwards of any size is
#: red. And it does not apply where no second producer exists — see
#: `_SECOND_PRODUCER`; a venue with one WS reader gets no tolerance at all,
#: whatever the size, because there is no mechanism to tolerate.
CROSS_STREAM_TOLERANCE_NS = 250 * 1_000_000


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


def fmt_short(nanos: int) -> str:
    """A duration at a scale that survives being small — `134.021us`.

    `fmt_dur` rounds to the millisecond, which renders the whole interleave
    check as `0.000s` and hides the number the operator needs. Integers only,
    and lossless below a second.
    """
    n = int(nanos)
    if n >= SEC_NS:
        return fmt_dur(n)
    if n >= 1_000_000:
        ms, rest = divmod(n, 1_000_000)
        # Three digits normally; all six only when the tail would be lost.
        return f"{ms}.{rest:06d}ms" if rest % 1_000 else f"{ms}.{rest // 1_000:03d}ms"
    if n >= 1_000:
        return f"{n // 1_000}.{n % 1_000:03d}us"
    return f"{n}ns"


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
#:
#: The two index/funding feeds are the exception on both counts, because both
#: were measured on 2026-07-28 (`collector/README.md`, "Index, oracle and
#: funding") and both are **periodic**: a frame arrives whether or not anything
#: changed — consecutive ones are frequently byte-identical — so their silence
#: is evidence on its own and needs no liveness witness, exactly like the
#: throttled `l2Book` feeds. They take the same K=10:
#:
#:   * `activeAssetCtx`  1.018s x 10  (n=292 over 300s, Hyperliquid mainnet)
#:   * `markPriceUpdate` 1.000s x 10  (n=298 over 300s, Binance COIN-M. USD-M
#:                       is no longer subscribed to this stream at all — the
#:                       venue stopped serving the whole markPrice class there,
#:                       measured 2026-07-28 from two network paths — but the
#:                       limit still applies to any UM day recorded while it
#:                       was, and to COIN-M, which serves it today)
#:
#: Both are written as a round 10s rather than as the cadence x 10 to the
#: decimal. Five minutes of each measures a median well and a tail not at all
#: (the widest interval anyone has watched for is 1.253s, over 45s of
#: `activeAssetCtx`), so a limit carrying two decimal places would be claiming
#: precision that was never observed; 10x a 1/s heartbeat is eight times that
#: worst interval either way.
#:
#: `premiumIndex` is the third periodic feed and the one exception to "measured":
#: it is not a venue stream at all but the collector's own REST poller, whose
#: 10s period is a constant in `binancefuturesum::PREMIUM_INDEX_INTERVAL`. It
#: takes the same K=10, which is 100s. That K is doing different work here —
#: with the cadence exact by construction the only jitter is a skipped cycle, and
#: a poll that fails is ordinary and deliberately costs one sample (see the error
#: policy on `poll_premium_index`). 100s is nine consecutive failures: past any
#: single venue hiccup, and a third of the 30 failures at which the collector
#: writes `poller_degraded` to the sidecar, so the two signals report in order
#: rather than racing.
#:
#: Note what this makes the periodic feeds: at 10s (and 100s for a 10s poller,
#: the same ten periods) they are the tightest cadence checks on either socket —
#: finer than Binance's 30s guesses and than `bbo`'s 14s. That is the point. A
#: periodic feed is the one channel whose silence means something without a
#: second opinion.
MAX_GAP_NS = {
    (HYPERLIQUID, "l2Book_slow"): 54 * SEC_NS,
    (HYPERLIQUID, "l2Book_fast"): 5_400_000_000,
    (HYPERLIQUID, "bbo"): 14 * SEC_NS,
    (HYPERLIQUID, "trades"): 120 * SEC_NS,
    (HYPERLIQUID, "activeAssetCtx"): 10 * SEC_NS,
    (BINANCE, "bookTicker"): 30 * SEC_NS,
    (BINANCE, "depthUpdate"): 30 * SEC_NS,
    (BINANCE, "trade"): 120 * SEC_NS,
    (BINANCE, "markPriceUpdate"): 10 * SEC_NS,
    (BINANCE, "premiumIndex"): 100 * SEC_NS,
    (BYBIT, "orderbook"): 30 * SEC_NS,
    (BYBIT, "publicTrade"): 120 * SEC_NS,
}

#: For an event-driven channel, the steadier channel on the SAME socket whose
#: silence decides whether its own silence meant anything. First name present in
#: the recording wins; if none was recorded the channel keeps its `MAX_GAP_NS`
#: limit as its only liveness signal.
#:
#: Why `bbo` needs one: it fires on a change of the top of book and on nothing
#: else, so a thin symbol in a quiet hour emits nothing for tens of seconds while
#: the connection is perfectly healthy. Measured 2026-07-26: 26 such holes on ENA
#: alone in half a day (14-37s), every one of them with `l2Book_fast` running
#: gapless across it. A limit alone cannot tell that from an outage, and a gate
#: whose yellows are mostly noise is a gate nobody reads — which the design
#: document's acceptance line rules out.
#:
#: Why `l2Book_fast` first and `l2Book_slow` second: both are throttled snapshot
#: feeds and arrive whether or not the book changed, so their cadence measures
#: the socket rather than the market. `fast` (0.54s) resolves a hole ten times
#: finer than `slow` (5.4s), so it is preferred where it was recorded; `slow` is
#: the fallback for a legal `--hl-l2-modes slow` run.
#:
#: Binance's `bookTicker` is event-driven too, but it is not listed: its 30s
#: limit is a flat guess (no cadence for that venue has ever been measured here),
#: `@depth@0ms` is optional and may be absent, and no false positive has been
#: observed. Adding a reference before the measurement would be inventing one.
#:
#: The 1/s index feeds would make fine references — they are periodic, they are
#: on the same socket, and `markPriceUpdate` is the first steady channel Binance
#: has here. They are deliberately not listed anyway. A reference can only ever
#: remove a warning, and adding one would loosen a check that has not been shown
#: to be noisy, on the strength of a stream no recording older than 2026-07-28
#: contains. `bbo`'s references are there because 26 false positives were counted
#: first; this one has no such measurement behind it yet.
LIVENESS_REFERENCE = {
    (HYPERLIQUID, "bbo"): ("l2Book_fast", "l2Book_slow"),
}


@dataclass(frozen=True)
class Expected:
    """The stream set one symbol of this venue must (or may) contain.

    Three classes, and the third is not a quieter second:

    * `required` — the dataset cannot be built without it. Absent: red.
    * `optional` — recorded on purpose and its absence costs something, but
      nothing mode A reads. Absent: yellow.
    * `informational` — checked exactly as the others are **while it is there**
      (classification, cadence, ordering), and not reported at all when it is
      not. Absent: nothing.

    The third class exists because a stream can be added to the collector after
    recordings have already been made. `@markPrice@1s` and `activeAssetCtx` were
    added on 2026-07-28; every day recorded before then lacks them by
    construction, and no rerun can fix that. Calling those days `missing_optional`
    would put a warning on every recording in existence at once — a gate whose
    yellows are mostly history is a gate nobody reads, which is the outcome the
    design document's acceptance line rules out. It is also a warning nobody can
    act on: Binance acks a stream name it will never serve and reports no error
    for it, so an absent `markPriceUpdate` is not always something the recording
    could have done anything about (`collector/README.md`, "Known limitations").

    What it is NOT is unknown. An unrecognised frame shape is still a yellow
    `unclassified_frame`, and the absence is still stated in the JSON as
    `missing_informational` — a fact the report reports, not a problem it raises.

    `violation` is set when the *profile* contradicts the recording
    configuration rather than the data: a legal recording that can never make
    the dataset the profile describes. It is a property of the day's
    `session_start`, not of any one symbol, and it is red.
    """

    required: tuple
    optional: tuple
    violation: Optional[str] = None
    informational: tuple = ()


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
        # `activeAssetCtx` (`hyperliquid::ALWAYS_ON`) carries `ctx.oraclePx` —
        # Hyperliquid's own spot basket and the direct input to its funding —
        # plus the funding rate itself. Mode A trades the book and does not read
        # either, so it is informational; see `Expected` for why that is not the
        # same as optional.
        return Expected(
            tuple(dict.fromkeys(required)), (), violation, ("activeAssetCtx",)
        )

    if exchange in ("binancefuturesum", "binancefuturescm"):
        # Mode A depends on `@bookTicker` alone. `@trade` and `@depth@0ms` are
        # recorded (open decision 1) and their absence is worth a warning —
        # without depth the recording is permanently unconvertible into a
        # tradable asset and the `pu` check above loses its input — but the
        # backtest itself does not read them.
        #
        # Both venues' index/funding data is informational: it is not order
        # flow, mode A does not read it, and it was added to the collector after
        # recordings had already been made. It reaches the two of them by
        # different routes, which is the whole of the difference below.
        #
        # `markPriceUpdate` is COIN-M's, and still live there (measured
        # 2026-07-28). It is listed for USD-M too even though USD-M no longer
        # subscribes: days recorded while it did exist, the venue could start
        # serving the class again, and the frame routing was kept for exactly
        # that case. Listed, it is checked if it turns up; unlisted, it would be
        # an `unclassified_frame` warning instead.
        #
        # `premiumIndex` is USD-M's, and USD-M's only: the venue stopped serving
        # the markPrice class on fstream entirely, so the collector polls
        # `GET /fapi/v1/premiumIndex` instead. COIN-M has no such poller, and
        # listing it there would say a COIN-M recording could have contained
        # something it never can.
        informational = ("markPriceUpdate",)
        if exchange == "binancefuturesum":
            informational = ("markPriceUpdate", "premiumIndex")
        return Expected(("bookTicker",), ("trade", "depthUpdate"), None, informational)

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
    #: The lifecycle record that accounts for the hole, if the sidecar has one.
    explained_by: Optional[str] = None
    #: Why this hole is not reportable at all — set when another stream on the
    #: same socket ran across it without one, i.e. nothing was lost. Distinct
    #: from `explained_by`, which says why a real hole happened.
    suppressed_by: Optional[str] = None

    def overlaps(self, other: "Gap") -> bool:
        """Whether the two holes share any instant. Touching ends count.

        Inclusive on purpose: an outage stops both feeds at approximately, not
        exactly, the same moment, and the direction to be wrong in is reporting.
        """
        return self.start_ts <= other.end_ts and other.start_ts <= self.end_ts

    def as_json(self) -> dict:
        return {
            "start_local_ts": int(self.start_ts),
            "end_local_ts": int(self.end_ts),
            "duration_ns": int(self.duration_ns),
            "explained_by": self.explained_by,
            "suppressed_by": self.suppressed_by,
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

    def gaps_truncated(self) -> bool:
        """Whether `MAX_GAPS_RECORDED` dropped holes this stream really had.

        A truncated list cannot prove another stream's hole does not overlap
        one of the holes it stopped keeping, so it may not suppress anything.
        """
        return self.gap_count > len(self.gaps)

    def suppressed_gap_count(self) -> int:
        return sum(1 for g in self.gaps if g.suppressed_by is not None)

    def as_json(self) -> dict:
        return {
            "count": self.count,
            "first_local_ts": None if self.first_ts is None else int(self.first_ts),
            "last_local_ts": None if self.last_ts is None else int(self.last_ts),
            # The raw count of over-limit holes. `suppressed_gap_count` of them
            # were disproved by another stream and reach no issue; the
            # measurement stays here either way.
            "gap_count": self.gap_count,
            "suppressed_gap_count": self.suppressed_gap_count(),
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
    #: `local_ts` went backwards WITHIN one stream: one producer, so this is a
    #: clock step or two recordings in one file. Red.
    monotonic_violation: Optional[dict] = None
    #: Two different streams written out of `local_ts` order, within
    #: `CROSS_STREAM_TOLERANCE_NS`: concurrent producers racing into one FIFO.
    #: Yellow.
    interleave_inversion: Optional[dict] = None
    #: The same, but beyond the bound — too far for any hand-off to explain. Red.
    interleave_excess: Optional[dict] = None
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
            "interleave_inversion": self.interleave_inversion,
            "interleave_excess": self.interleave_excess,
            "sequence_breaks": dict(self.sequence_breaks),
            "sequence_break_examples": {
                stream: [g.as_json() for g in gaps]
                for stream, gaps in sorted(self.sequence_break_gaps.items())
            },
            "streams": {name: s.as_json() for name, s in sorted(self.streams.items())},
        }


#: The keys a `GET /fapi/v1/premiumIndex` element must all carry to be one.
#:
#: Four of the eight the venue sends (captured 2026-07-28: `symbol`, `markPrice`,
#: `indexPrice`, `estimatedSettlePrice`, `lastFundingRate`, `interestRate`,
#: `nextFundingTime`, `time`). Not all eight, so that the venue adding or
#: retiring a field does not silently turn the whole feed into
#: `unclassified_frame`; not one or two, so that no other bare object in these
#: files can collide with it.
_PREMIUM_INDEX_KEYS = frozenset(
    {"symbol", "markPrice", "indexPrice", "lastFundingRate"}
)


def classify(family: str, obj: dict) -> Optional[str]:
    """The stream a recorded frame belongs to, or `None` if unrecognised.

    Frame shapes come from the converters that read these files:
    `hyperliquid.py` (`channel`, and `data.fast` telling the two `l2Book`
    cadences apart) and `binancefutures.py` (combined-stream envelope, `data.e`).
    Bybit's topic string is `orderbook.<depth>.<symbol>` / `publicTrade.<symbol>`
    (`collector/src/bybit/mod.rs` routes on its last segment).

    The two WebSocket index/funding feeds need no rule of their own and
    deliberately do not get one: Hyperliquid's `activeAssetCtx` names itself in
    `channel`, and Binance's `markPriceUpdate` in `data.e`, so both fall out of
    the rules above as streams in their own right — including the dex-prefixed
    `xyz:GOLD` form, whose coin only ever appears in the payload the routing
    already keyed on. Pinned by
    `test_the_index_and_funding_frames_classify_as_their_own_streams` over frames
    captured from mainnet, because "happens to work" is one whitelist away from
    a whole feed being counted as `unclassified_frame`.

    `premiumIndex` is the one that does need a rule, because it answers to
    nothing the existing ones read. USD-M stopped serving the markPrice class of
    public streams (measured 2026-07-28 from two independent network paths), so
    its index and funding data now arrive over REST from the collector's own
    poller and are written as the venue's array elements, verbatim: no
    combined-stream envelope, so no `data` and no `e`, and the symbol under
    `symbol` rather than `s`.

    The discriminator is structural rather than semantic, on purpose.
    `markPriceUpdate` carries a mark price, an index price and a funding rate
    too — the same three quantities under one-letter names — so "looks like
    index data" would relabel the whole COIN-M feed. What actually separates
    them is that a WS frame names its event in `e` and a REST element does not,
    and that the four keys required here are the venue's own spellings, which no
    stream envelope uses.
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
        if not _PREMIUM_INDEX_KEYS - obj.keys() and "e" not in obj:
            return "premiumIndex"
        return None

    if family == BYBIT:
        topic = obj.get("topic")
        if isinstance(topic, str):
            parts = topic.split(".")
            return ".".join(parts[:-1]) if len(parts) > 1 else topic
        return None

    return None


#: The bucket an unrecognised frame is ordered under. It has no stream, but it
#: is still a line in the file, so leaving it out of the ordering check would
#: let a whole unknown feed be written out of order unnoticed. Lumping every
#: unrecognised shape together is deliberate: `unclassified_frames` already
#: reports them, and one bucket cannot be worse than none.
UNCLASSIFIED = "(unclassified)"


def _note_inversion(
    record: Optional[dict],
    lineno: int,
    prev_stream: str,
    stream: str,
    prev_ts: int,
    ts: int,
) -> dict:
    """Folds one out-of-order pair into an O(1) summary of its kind.

    The first occurrence is kept whole — a line number and both stamps is what
    an investigation starts from — and everything after it only moves counters.
    """
    delta = prev_ts - ts
    if record is None:
        record = {
            "line": lineno,
            "previous_stream": prev_stream,
            "stream": stream,
            "previous_local_ts": int(prev_ts),
            "local_ts": int(ts),
            "delta_ns": int(delta),
            "violations": 0,
            "max_delta_ns": 0,
        }
    record["violations"] += 1
    if delta > record["max_delta_ns"]:
        record["max_delta_ns"] = int(delta)
    return record


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
    counters. The ordering check below keeps one stamp per stream, and there are
    at most a handful of streams in a symbol file.

    **Ordering has two different meanings here, and conflating them makes the
    check structurally unsatisfiable.**

    *Within one stream* `local_ts` must never go backwards. Verified for every
    stream this file classifies:

    * Hyperliquid — one WS reader stamps `Utc::now()` and `route`s every frame
      (`hyperliquid/mod.rs`); `trades`, `bbo`, `l2Book_fast`, `l2Book_slow` and
      `activeAssetCtx` all come off that one loop, in receive order. The last is
      one more subscription on the same socket (`hyperliquid::ALWAYS_ON`), not a
      second producer.
    * Bybit — likewise, one reader for `orderbook.*` and `publicTrade`
      (`bybit/mod.rs`); nothing else writes a symbol file.
    * Binance (`binance`, `binancefuturesum`, `binancefuturescm`) — `bookTicker`,
      `depthUpdate`, `trade` and `markPriceUpdate` come off the one WS reader
      through `pump`, so they hold; `@markPrice@1s` is a stream of the same
      combined-stream URL (`binancefutures{um,cm}::STREAMS`), which is also why
      it carries no `pu` chain to check and never enters `_track_sequence`.
      Two exceptions are worth knowing about, and both hold *within* their own
      stream for reasons of their own. `depthSnapshot`: each one is fetched by
      its own detached `tokio::spawn`, so two in flight could in principle be
      stamped and enqueued out of order — the window is the few instructions
      between `Utc::now()` and `send`, and the fetches are throttled to 100/min,
      so it stays red, because at that separation a real step backwards is a
      clock, not a race. `premiumIndex` (USD-M only): one poller awaiting each
      response before the next tick, so two polls cannot overtake each other at
      all.

    *Between two streams* it holds exactly where the venue has one producer,
    which is everywhere in the list above except `depthSnapshot` and
    `premiumIndex` — so the tolerance is granted on the mechanism, not on the
    venue and not on the size:
    `_SECOND_PRODUCER` has to name one of the two streams before
    `CROSS_STREAM_TOLERANCE_NS` is consulted at all. A step backwards between
    two Hyperliquid cadences, or between `bookTicker` and `trade`, is red at a
    nanosecond, because the one reader that stamped them also queued them in
    that order.

    The file's write order is compared with the previous line's stamp, which is
    what "the writer put these two the wrong way round" means locally; comparing
    against the maximum seen so far would instead count every frame written
    during the overtake.
    """
    path = Path(path)
    family = family_of(exchange)
    symbol = path.name.rsplit("_", 1)[0]
    scan = FileScan(path=str(path), symbol=symbol, exchange=exchange)
    prev_ts = None
    prev_stream = None
    last_of_stream: dict = {}
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

            stream = classify(family, obj) if isinstance(obj, dict) else None
            key = stream if stream is not None else UNCLASSIFIED

            # Equal stamps are fine throughout — two frames can share a
            # nanosecond, and on a burst many do.
            last_seen = last_of_stream.get(key)
            if last_seen is not None and ts < last_seen:
                scan.monotonic_violation = _note_inversion(
                    scan.monotonic_violation, lineno, key, key, last_seen, ts
                )
            elif prev_ts is not None and ts < prev_ts:
                # In order for its own stream but written after a line stamped
                # later. Tolerated only where two producers could actually have
                # raced, and then only up to the socket hop that separates them:
                # on a venue with one WS reader there is nothing to tolerate.
                tolerated = (
                    prev_ts - ts <= CROSS_STREAM_TOLERANCE_NS
                    and second_producer_of(prev_stream, key) is not None
                )
                if tolerated:
                    scan.interleave_inversion = _note_inversion(
                        scan.interleave_inversion, lineno, prev_stream, key, prev_ts, ts
                    )
                else:
                    scan.interleave_excess = _note_inversion(
                        scan.interleave_excess, lineno, prev_stream, key, prev_ts, ts
                    )
            last_of_stream[key] = ts
            prev_ts = ts
            prev_stream = key

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


#: How many times its own cadence limit a hole may be before no liveness
#: reference is allowed to excuse it.
#:
#: The false positives this check exists for were 14-37s on ENA (measured
#: 2026-07-26), against a 14s `bbo` limit — so 10x is roughly four times the
#: worst of them and cannot reach the failure on the other side. That failure is
#: a silently dropped per-channel subscription: socket up, one channel dead, the
#: `orderbook.500` precedent in `AGENTS.md` §4.1. It has exactly the signature
#: this check suppresses, and at some size "the top of book did not move" stops
#: being a hypothesis about a venue whose `bbo` median is 0.14s. A reference
#: proves the SOCKET was alive; it never proves the channel was.
MAX_SUPPRESSED_GAP_FACTOR = 10


def liveness_witness(streams: dict, reference_names: tuple, gap: Gap) -> Optional[str]:
    """The first reference stream that disproves this hole, or `None`.

    A reference disproves nothing unless it was **running across the hole**.
    Selecting on "was it recorded at all" is fail-open twice over: a stream that
    simply stops leaves no trailing hole of its own (`StreamStat.observe` only
    measures between two frames), so a dead reference satisfies "no overlapping
    gap" vacuously; and picking per stream rather than per gap lets a dead
    preferred reference shadow a live fallback that did report the outage.

    Hence the bracket test, and hence the loop: `LIVENESS_REFERENCE` is a tuple
    in preference order, and a name that cannot witness *this* hole is passed
    over for the next one rather than ending the search.
    """
    for name in reference_names:
        ref = streams.get(name)
        if ref is None or ref.first_ts is None or ref.last_ts is None:
            continue
        if ref.first_ts > gap.start_ts or ref.last_ts < gap.end_ts:
            # Recorded, but not over this interval. Its silence here is its own
            # absence, not evidence about anything.
            continue
        if ref.gaps_truncated():
            # Its hole list is incomplete, so "no overlapping hole" is not a
            # fact about the recording, only about what was kept. Fail closed.
            continue
        # Both lists are capped at MAX_GAPS_RECORDED, so the quadratic pair
        # count is bounded by 200x200 and needs no index.
        if any(gap.overlaps(other) for other in ref.gaps):
            continue
        return name
    return None


def suppress_quiet_book_gaps(family: str, streams: dict) -> None:
    """Marks the cadence gaps another stream on the same socket disproves.

    An event-driven channel emits nothing when nothing happens, so its silence
    is only evidence of a hole if the steady channel beside it went silent too.
    Mutates the `Gap` objects in place; see `LIVENESS_REFERENCE` for the choice
    of reference and the measurement behind it, and `liveness_witness` for what
    a reference has to have done to count as one.

    Three things stop a hole being suppressed, all of them reasons the reference
    cannot settle it:

    * an `explained_by` already set from the sidecar. An outage shorter than the
      reference's own cadence limit leaves no hole in it — a two-second
      reconnect is invisible to a feed allowed 5.4s between frames — and
      dropping a hole the collector itself reported would be the one way this
      check could lose information rather than noise. Call it after
      `explain_gap`, or the guard has nothing to read;
    * the hole being past `MAX_SUPPRESSED_GAP_FACTOR` x its own limit, where the
      quiet-book explanation stops being credible whatever else was running;
    * no reference that was actually live across it.
    """
    for stream, stat in streams.items():
        reference_names = LIVENESS_REFERENCE.get((family, stream))
        if reference_names is None or not stat.gaps:
            continue
        limit = gap_limit(family, stream)
        ceiling = None if limit is None else limit * MAX_SUPPRESSED_GAP_FACTOR

        for gap in stat.gaps:
            if gap.explained_by is not None:
                continue
            if ceiling is not None and gap.duration_ns > ceiling:
                continue
            ref_name = liveness_witness(streams, reference_names, gap)
            if ref_name is None:
                continue
            gap.suppressed_by = (
                f"{ref_name} ran without a gap over the same interval — {stream} "
                f"is event-driven, so this is the top of book not changing, not "
                f"a hole in the recording"
            )


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


#: The collector's **gauges** — `_collector` records that are measurements
#: rather than events in the recording's life. Three are written on the same
#: one-minute timer (`main.rs`, the `gauges.tick()` arm: `disk`, `clock`,
#: `liveness`) and `universe` once at startup.
#:
#: Named as a set for two reasons. They are known records, so nothing may treat
#: one as unrecognised — a gauge the Rust side starts writing must not turn
#: every recording yellow for the collector doing what it was asked to. And not
#: one of them may ever join `_EXPLANATORY` below; the assertion that they do
#: not is a test, because the rule is easy to break by adding a name to the
#: wrong tuple.
_GAUGES = ("disk", "clock", "liveness", "universe")

#: The collector's **lifecycle** records — the ones that say something happened
#: to the recording — most conclusive first. A restart (`session_start`) explains
#: a hole as surely as a `disconnected` does.
#:
#: This tuple is also the whitelist: a `_collector` record whose name is not
#: here explains nothing. That matters because the sidecar also carries the
#: `_GAUGES` above. One `disk` gauge lands inside every hole longer than a
#: minute no matter what caused it, so annotating a gap "explained by disk
#: at ..." states only that a minute passed. It closed investigations that had
#: not happened. The `clock` gauge is the sharpest case of the same rule: an
#: unsynchronised clock makes a hole's *measurement* doubtful, which is the
#: opposite of accounting for the hole.
#:
#: Fail closed by omission: a `_collector` record added to the Rust side later
#: will not explain anything until it is named here, and a new gauge silently
#: cannot.
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

# The two tuples must stay disjoint. A measurement that explains a gap is
# exactly how the minutely disk gauge came to close investigations that had not
# happened, and the tuples are adjacent — putting a name in the wrong one is a
# one-line mistake. Checked on import rather than only in a test, so it also
# holds for anything that imports this module without running the suite.
assert not set(_GAUGES) & set(_EXPLANATORY), (
    f"a gauge is listed as explanatory: {sorted(set(_GAUGES) & set(_EXPLANATORY))}"
)


def lifecycle_events(records) -> list:
    """`(ts, event)` for the collector's lifecycle records, sorted by `local_ts`.

    Filtered to `_EXPLANATORY` here rather than at the point of use, so the
    `(+N more)` count in an explanation is a number of records that bear on the
    hole — not a count of how many timer ticks landed inside it.
    """
    out = [
        (ts, rec["_collector"])
        for ts, rec in records
        if ts is not None and rec.get("_collector") in _EXPLANATORY
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
    # Unreachable while `lifecycle_events` filters to `_EXPLANATORY`, and it
    # must stay that way: falling back to "whatever was in there" is how the
    # minutely disk gauge came to explain gaps.
    return None


def clock_summary(records, day: str) -> Optional[dict]:
    """What the `clock` gauge said during one UTC day, or `None` if it said
    nothing.

    The gauge is the kernel's own view of how well `CLOCK_REALTIME` was being
    disciplined, sampled once a minute (`collector/src/clock.rs`). It exists
    because the alternative was finding out at assembly time: on 2026-07-27 a
    host came back from a reboot undisciplined, recorded a full day, and the
    time policy rejected all of it on a 7.04 ms local-exchange skew.

    Scoped to the day being checked even though sidecars are read across the
    whole directory — `session_start` is per process, but a clock reading is
    not, and one bad night must not annotate every day in the directory.

    Three things are deliberately *not* done here.

    A missing `sync` field does not count as unsynchronised: it is what an
    unsupported platform and a failed syscall both write, and "we did not
    measure it" is not "it was wrong". Nor does it count towards `samples`,
    which is the denominator the note quotes — "2 of 4" reads as two healthy
    readings, and a sample nobody took is not one of those.

    No threshold on `max_error_us` is applied — the collector owns that number
    (`clock::MAX_ERROR_WARN_US`) and duplicating it here would give two limits
    that drift apart. A dataset manifest applying its own policy reads the
    `_meta` records directly; what is summarised here is only what the note
    needs.

    And `worst_max_error_us` is scoped to the unsynchronised samples, not to the
    day. `max_error` grows between every poll on a perfectly healthy clock, so a
    day almost always holds a larger one outside the window — quoting that
    inside a sentence about the window would attribute an ordinary excursion to
    the fault.
    """
    start_ns, end_ns = day_bounds_ns(day)
    samples = unsynced = 0
    first_bad = last_bad = None
    worst_max_error = None
    for ts, rec in records:
        # An undated sidecar line cannot be placed in a day at all. Counting one
        # would let a record from any day annotate this one.
        if rec.get("_collector") != "clock" or ts is None:
            continue
        if not (start_ns <= ts < end_ns):
            continue
        sync = rec.get("sync")
        if not isinstance(sync, bool):
            continue
        samples += 1
        if sync:
            continue
        unsynced += 1
        max_error = rec.get("max_error_us")
        if isinstance(max_error, int) and (
            worst_max_error is None or max_error > worst_max_error
        ):
            worst_max_error = max_error
        if first_bad is None or ts < first_bad:
            first_bad = ts
        if last_bad is None or ts > last_bad:
            last_bad = ts
    if samples == 0:
        return None
    return {
        "samples": samples,
        "unsynced_samples": unsynced,
        "first_unsynced_ts": first_bad,
        "last_unsynced_ts": last_bad,
        "worst_max_error_us": worst_max_error,
    }


def clock_detail(clock: dict) -> str:
    """The note an unsynchronised window gets.

    Informational, and it says what it bears on rather than what it proves. The
    recording is not corrupt and the venues' own timestamps are untouched; what
    is in doubt is every `local_ts` stamped inside the window, which is what the
    cadence, monotonicity and coverage checks are all measured in.
    """
    worst = clock["worst_max_error_us"]
    worst_text = "" if worst is None else f", worst max_error in it {worst} us"
    return (
        f"the host clock was unsynchronised (STA_UNSYNC) for "
        f"{clock['unsynced_samples']} of {clock['samples']} measured minutely "
        f"sample(s), "
        f"{iso(clock['first_unsynced_ts'])} .. {iso(clock['last_unsynced_ts'])}"
        f"{worst_text}. local_ts inside that window is the host's own idea of "
        f"the time, so any finding there — a cadence gap, a monotonicity step, "
        f"the coverage bounds — may be the clock rather than the recording. The "
        f"venue timestamps inside the payloads are unaffected"
    )


# ---------------------------------------------------------------------------
# per-day checking
# ---------------------------------------------------------------------------


def issue(severity: str, check: str, detail: str) -> dict:
    return {"severity": severity, "check": check, "detail": detail}


def interleave_detail(name: str, record: dict) -> str:
    """Why two streams are out of order, and whether that is still credible."""
    delta = record["max_delta_ns"]
    mechanism = second_producer_of(record["previous_stream"], record["stream"])
    if mechanism is None:
        # No hand-off separates these two, so no hand-off can have reordered
        # them, and the size of the step is beside the point.
        verdict = (
            "not something an interleave can produce here: with one producer the "
            "write order IS the receive order, so this is a clock step, a frame "
            "classified into the wrong stream, or two recordings in one file"
        )
        mechanism = _NO_SECOND_PRODUCER
    elif delta > CROSS_STREAM_TOLERANCE_NS:
        # Two hypotheses, and they need different responses. The backlog one is
        # the reason to look at `_meta` first: a hop holding thousands of frames
        # is one capacity away from the overflow that ends the process, so a
        # wide interleave can be the last trace of a burst the recording only
        # just survived.
        verdict = (
            "beyond the %s interleave bound, which is the whole socket hop at "
            "the measured burst rate: either a queue was deeper or slower than "
            "that (check _meta for a burst or a queue_overflow near this line) "
            "or the file holds two recordings"
            % fmt_short(CROSS_STREAM_TOLERANCE_NS)
        )
    else:
        verdict = (
            "within the %s interleave bound, so nothing is missing and nothing "
            "is mis-stamped — only the write order and the receive order "
            "disagree" % fmt_short(CROSS_STREAM_TOLERANCE_NS)
        )
    return (
        f"{name}: local_ts goes backwards BETWEEN streams {record['violations']} "
        f"time(s), worst {fmt_short(delta)}; first at line {record['line']}: "
        f"{record['previous_stream']} {iso(record['previous_local_ts'])} -> "
        f"{record['stream']} {iso(record['local_ts'])}. Write order and local_ts "
        f"order can only disagree where two producers stamp their own receive "
        f"moment and race into one queue, so the question is whether this file "
        f"has two: {mechanism}. This is {verdict}"
    )


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

    # Raised first so it is read before the findings it qualifies. Yellow and
    # never red: the recording is not damaged, and which window a skewed clock
    # invalidates is a policy the Phase 3 builder applies, not one this report
    # decides.
    clock = clock_summary(meta_records, day)
    if clock is not None and clock["unsynced_samples"]:
        issues.append(issue(YELLOW, "clock_unsynced", clock_detail(clock)))

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
                "interleave_inversion": None,
                "interleave_excess": None,
                "sequence_breaks": {},
                "sequence_break_examples": {},
                "streams": {},
                "missing_required": missing,
                "missing_optional": list(expected.optional) if expected else [],
                "missing_informational": (
                    list(expected.informational) if expected else []
                ),
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
                    f"{name}/{v['stream']}: local_ts goes backwards within the "
                    f"stream {v['violations']} time(s), worst "
                    f"{fmt_short(v['max_delta_ns'])}; first at line {v['line']}: "
                    f"{iso(v['previous_local_ts'])} -> {iso(v['local_ts'])}. "
                    f"One stream has one producer stamping at receive time, so "
                    f"this is a clock step or two recordings in one file",
                )
            )

        for record, severity, check in (
            (scan.interleave_excess, RED, "interleave_excess"),
            (scan.interleave_inversion, YELLOW, "interleave_inversion"),
        ):
            if record:
                issues.append(issue(severity, check, interleave_detail(name, record)))

        missing_required, missing_optional, missing_informational = [], [], []
        if expected is not None:
            for stream in expected.required:
                if scan.streams.get(stream, StreamStat()).count == 0:
                    missing_required.append(stream)
            for stream in expected.optional:
                if scan.streams.get(stream, StreamStat()).count == 0:
                    missing_optional.append(stream)
            # Recorded, never raised. An informational stream absent from a day
            # older than the stream itself is the normal case, not a finding —
            # see `Expected`. It reaches the JSON so the question "does this day
            # carry the funding basket?" has an answer, and it deliberately
            # reaches no `issue()` below — which is also why `render_text` never
            # prints it: that view is the operator's issue list, and a fact
            # printed among warnings is read as one.
            for stream in expected.informational:
                if scan.streams.get(stream, StreamStat()).count == 0:
                    missing_informational.append(stream)
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

        # Ask the sidecar about every hole first, then drop the ones a steadier
        # stream on the same socket disproves — a quiet top of book is not a
        # hole, but one the collector reported on is, whatever else was running.
        for stat in scan.streams.values():
            for gap in stat.gaps:
                gap.explained_by = explain_gap(gap, events)
        suppress_quiet_book_gaps(family_of(exchange), scan.streams)

        for stream, stat in sorted(scan.streams.items()):
            reportable = [gap for gap in stat.gaps if gap.suppressed_by is None]
            named = min(len(reportable), MAX_GAP_ISSUES)
            limit = gap_limit(family_of(exchange), stream)
            for gap in reportable[:named]:
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
            # Gaps past `MAX_GAPS_RECORDED` were never examined, so they could
            # not have been suppressed; they are counted with the unnamed ones.
            remainder = (len(reportable) - named) + (stat.gap_count - len(stat.gaps))
            if remainder > 0:
                issues.append(
                    issue(
                        YELLOW,
                        "cadence_gap",
                        f"{name}/{stream}: {remainder} further gap(s) "
                        f"not listed individually; see the JSON report",
                    )
                )

        entry = scan.as_json()
        entry["missing_required"] = missing_required
        entry["missing_optional"] = missing_optional
        entry["missing_informational"] = missing_informational

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
                    # `gaps` is the raw count of over-limit holes; the note says
                    # how many of them a reference stream showed to be the feed
                    # simply being quiet, and so reach no issue.
                    quiet = stat.get("suppressed_gap_count") or 0
                    tail = f" ({quiet} quiet, not reported)" if quiet else ""
                    # Width 16 fits the longest stream name any venue produces,
                    # which is `markPriceUpdate` at 15; at 14 that one row broke
                    # the alignment of every column after it.
                    lines.append(
                        f"     {name:<12} {stream:<16} n={stat['count']:<9} "
                        f"{iso(stat['first_local_ts'])} .. {iso(stat['last_local_ts'])} "
                        f"gaps={stat['gap_count']}{tail}"
                    )
                cov = sym.get("coverage") or {}
                if cov.get("required_streams"):
                    lines.append(
                        f"     {name:<12} {'coverage':<16} "
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
