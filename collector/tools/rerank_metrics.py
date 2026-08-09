#!/usr/bin/env python3
"""Microstructure metrics over raw collector recordings.

Implements **§9.2 «Что посчитать на записи»** of
`docs/research-symbol-selection.md`. That document ranked 87 Hyperliquid perps
from a four-minute snapshot and then said, in §8.1/§8.3, exactly what is wrong
with having done so: seven point samples cannot separate a book that widens when
the market moves from one that does not, and every spread number in the ranking
is a sample rather than a measurement. This tool computes the replacements from
the recording the collector has been making since 2026-07-29.

One coin at a time, one Hyperliquid recording against one (or two) Binance USD-M
recordings, one or more UTC days::

    rerank_metrics.py --hl-dir /data/hyperliquid --coin PUMP \\
        --um-dir /data/binancefuturesum --um-symbol PUMPUSDT \\
        --day 20260729 [--day 20260730 ...] [--json out.json] [--txt out.txt]

With several `--day` there is one row per day plus a **pooled** row, which is the
same metric recomputed over every day's observations — not an average of the
per-day rows. With a second recording of the same Binance symbol
(`--um-dir-b`, or a repeated `--um-dir`) the `@bookTicker` frames are unioned on
the venue's update id `u`, earliest arrival winning: the same idea, for the same
reason, as `build_dataset.build_signal_union`.

What is measured, and the decision each number exists to make:

1. **tick** — the mode of the gaps between consecutive distinct best bids, over
   every channel that prints one, validated against the ask side. §2.1
   reconstructs the tick from Hyperliquid's
   price rules (`max(10^-(6-szDecimals), 10^(floor(log10 px)-4))`) and §8.6 flags
   that reconstruction as load-bearing for the whole re-ranking. A recording
   settles it. The `szDecimals` term is not in a recording, so the formula here
   is the five-significant-figure term alone; where it disagrees with the
   measurement by more than 5% the disagreement is reported and **the measured
   tick is what every downstream number uses**.
2. **spread** — time-weighted, because §9.3's gate is `spread_ticks >= 3` *for
   most of the session*, which a frame-count distribution cannot answer: a quote
   that stands for nine seconds and one that stands for 30ms are one frame each.
   Each frame's weight is the time to the next frame, capped at
   `SPREAD_WEIGHT_CAP_NS`, so a feed outage cannot outvote the day.
3. **conditional spread curve** — median spread per realized-vol quintile over
   60s windows. §8.3 calls this the cleanest signature of professional presence
   available from L2 data: organic books blow out on impulse, professionally made
   ones come back. This is the one output a snapshot cannot produce.
4. **touch queue** — order counts and USD at the best bid and ask, from `bbo`,
   which carries `n`, plus how long a best price survives. §2.2 measured a touch
   of one order per $250 from seven samples; this says whether it stays that way.
5. **depth at the planned grid rungs** — +-10/20/30bps from the 5-level `l2Book
   fast` feed, with the truncation §8.7 warns about counted rather than assumed
   away, and cross-checked against the 20-level slow feed.
6. **lead-lag against Binance** — §8.9's admitted gap: the HL/UM volume ratio is
   a proxy for cross-venue exposure and nothing measured it. Cross-correlation of
   50ms mid returns does.
7. **traversals** — how far the mid walked and how often it came back, as
   round trips of 5/10/20/30bps. Metrics 1-6 all measure the *shape of the book*;
   the 2026-07-30 sweep says the only grid shape that pays on this venue earns
   SPACING, and spacing is paid by a price that moves through the rungs, not by a
   book that sits still being narrow. `traversal_of` carries the argument. The
   round trips are the part that ranks coins — they converge as the sampling is
   refined; `path_bps` and `path_efficiency` in the same block do not and are
   labelled as the within-row diagnostics they are.

**Reading and channel classification are `quality_report.py`'s.** `iter_gz_lines`
handles the multi-member gzip a collector restart appends; `classify` tells the
two `l2Book` cadences apart by `data.fast` (5 levels at 0.54s versus 20 at 5.4s),
which metric 5 depends on and which nothing else in the frame reveals. Importing
it rather than re-deriving it means the two tools cannot drift apart about what a
frame is.

**Prices are exact integers**, scaled by `PX_SCALE`. A tick of 1e-6 on a
0.001816 price is not representable in binary floating point, and the tick metric
is a mode over price *differences*: in float those differences arrive as
9.99999e-07 and 1.00000002e-06 and their mode is noise. Timestamps stay int64 for
the reason the sibling module states — 1.78e18 ns does not fit a float64
mantissa.

**No wall clock and no unseeded randomness.** Two runs over the same recording
produce byte-identical output, which is what makes a re-ranking reviewable.

Exit codes: 0 on success, 2 on a usage or I/O error. This is a measurement tool
and has no verdict; the §9.3 decision rule is applied by a person reading it.
"""

from __future__ import annotations

import argparse
import json
import math
import sys
from array import array
from collections import Counter
from dataclasses import dataclass, field
from decimal import Decimal
from pathlib import Path
from typing import Optional, Sequence

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parent))

from quality_report import (  # noqa: E402
    BINANCE,
    HYPERLIQUID,
    TruncatedRecording,
    classify,
    iter_gz_lines,
    parse_line,
)

#: The single home of the funding convention (#40). Imported rather than
#: reimplemented: the settlement rule, the sign and the price basis are measured
#: against a real ledger in one place, and a second copy of them would drift.
import funding  # noqa: E402

SCHEMA = "rerank-metrics-v1"

MS = 1_000_000
SEC = 1_000_000_000

#: Prices are held as integers scaled by this. Ten decimal places covers every
#: Hyperliquid perp price (max 5 significant figures, max 6 decimals) and every
#: Binance USD-M one, and a 63963.0 price scaled by it is 6.4e14 — still exact in
#: the int64 the arrays use.
PX_DECIMALS = 10
PX_SCALE = 10 ** PX_DECIMALS

#: The most time one quote may be credited with. Hyperliquid's `bbo` has a median
#: interval of 0.14s, so this is ~35 intervals: long enough that no normal quiet
#: patch is truncated, short enough that a reconnect cannot carry the day.
SPREAD_WEIGHT_CAP_NS = 5 * SEC

#: How stale a quote may be and still be sampled onto the 1s vol grid. Same
#: value, same reason: past it, last-observation-carried-forward is inventing a
#: price, and a run of invented prices reads as zero volatility.
LOCF_MAX_AGE_NS = 5 * SEC

VOL_WINDOW_NS = 60 * SEC
VOL_GRID_NS = 1 * SEC
#: Of the 60 grid points in a window. A window the feed was mostly absent from
#: has a realized vol that measures the absence, and it would land in the calm
#: quintile and drag that quintile's spread with it.
MIN_VOL_SAMPLES = 30
#: A quintile split of four windows is not a quintile split.
MIN_VOL_WINDOWS = 5

DEPTH_RUNGS_BPS = (10, 20, 30)

#: The grid spacings metric 7 counts traversals at. 5bps is below anything the
#: 2026-07-30 sweep found tradeable and is kept as the bottom of the ladder: the
#: oscillation a coin has that the fee eats.
#:
#: It is NOT a control that separates coins, and the earlier comment here said it
#: was. Measured over the shortlist, the 5->10bps collapse ratio sits in a band of
#: 2.2-2.9x on every coin of both recorded days — 1.3x of spread against the 12x
#: spread in the rate itself — so it ranks nothing. Worse, it is not the same
#: measurement on every coin: see `MIN_SPACING_MID_STEPS`.
TRAVERSAL_SPACINGS_BPS = (5, 10, 20, 30)

#: A spacing worth fewer than this many mid steps is at the book's flicker floor:
#: the smallest oscillation the price grid can print already clears it, so the
#: count there is a property of the tick as much as of the path.
#:
#: The mid moves in half-ticks (one side requoting moves it half a tick), and the
#: tick is a fixed share of the price on this venue, so the share differs by an
#: order of magnitude across the shortlist: PUMP's measured tick is 5.4bps of its
#: mid — two mid steps clear 5bps — against HYPE's 0.18bps, which needs ~55.
#: Three steps is the smallest oscillation that is not one wiggle, and rungs below
#: it are reported `at_flicker_floor` rather than dropped: the number is still the
#: right one for a grid quoted that tight, it just is not comparable across coins.
MIN_SPACING_MID_STEPS = 3

#: The block the round-trip rate's spread is measured over. One hour, because the
#: rate is published per hour: a block's round-trip count then *is* its rate.
TRAVERSAL_BLOCK_NS = 3600 * SEC

#: Below this many whole blocks there is no spread to report — two numbers are a
#: pair, not a range. Same refusal as `MIN_VOL_WINDOWS`, and the rate itself still
#: ships: a three-hour fragment is normal here and per-hour is the only way to
#: compare it with a whole day.
MIN_TRAVERSAL_BLOCKS = 3

LEADLAG_CELL_NS = 50 * MS
LEADLAG_MAX_AGE_NS = 1 * SEC
LEADLAG_MAX_LAG_MS = 2000
LEADLAG_LAGS_MS = tuple(
    range(-LEADLAG_MAX_LAG_MS, LEADLAG_MAX_LAG_MS + 1, LEADLAG_CELL_NS // MS)
)
#: Below this many paired returns a correlation is an artefact of its sample.
MIN_LEADLAG_CELLS = 100

#: Stands in for a `@bookTicker` frame that carried no `u`. Negative because every
#: real Binance update id is non-negative, so no frame can collide with it.
NO_UPDATE_ID = -1

#: A hole this long in a channel is reported. Not a cadence check — that is
#: `quality_report.py`'s job, with per-channel limits; this only tells the reader
#: of a metric that part of the day is missing from it.
GAP_REPORT_NS = 60 * SEC

#: A hole in `bbo` this long makes the price run containing it unmeasurable: the
#: best price may have moved and come back while nothing was recorded.
#:
#: Deliberately `GAP_REPORT_NS` and **not** `LOCF_MAX_AGE_NS`, which asks a
#: different question — "is this quote too stale to sample onto the vol grid" —
#: and answers it with 5s. Hyperliquid publishes `bbo` on change, so a quote that
#: simply stands prints nothing at all, and on a thin coin those quiet patches
#: *are* what metric 4 measures. Measured over the 2026-07-29 recording: no hole
#: in any coin's whole day exceeds 60s, zero `bbo` frames repeat their
#: predecessor, and a 5s bound threw away 9.7% of PUMP's runs holding 44% of its
#: elapsed run time (p90 20.6s -> 13.5s) against 0.0% of liquid HYPE's — a
#: censoring rate that rises with thinness, which is the dimension being ranked.
LIFETIME_MAX_GAP_NS = GAP_REPORT_NS

LEADLAG_SIGN_CONVENTION = (
    "positive lag_ms = Binance USD-M LEADS Hyperliquid by that many milliseconds: "
    "the Hyperliquid return at t is correlated against the Binance return at "
    "t - lag_ms. Negative means Hyperliquid leads."
)

LEADLAG_CAVEAT = (
    "The lag is bounded below by each feed's own publication cadence, which is "
    "reported beside it: a venue that publishes its book every 600ms cannot show "
    "a move sooner than that, whoever discovered it. Read a lag of the order of "
    "`hl_frame_interval_p50_ms` as 'the Hyperliquid book a maker actually sees is "
    "this stale', which is the tradeable statement, and not as price discovery."
)

CONDITIONAL_CAVEAT = (
    "Realized vol and spread are both derived from the same `bbo` stream, so "
    "mechanical coupling between them is possible: a wider quote moves the mid "
    "it is measured from. The comparison this feeds (research-symbol-selection "
    "§8.3) is rising-versus-flat ACROSS coins, which survives a coupling common "
    "to all of them; the level of a single coin's ratio does not."
)

TRAVERSAL_CAVEAT = (
    "`gross_capture_potential_bps` is a CEILING and not a forecast: it credits "
    "every round trip with the whole rung and charges nothing for the fee, the "
    "adverse fill, the queue the rung may never have reached, or the inventory the "
    "unfinished traversal left behind. It is the most a one-rung grid at that "
    "spacing could have grossed on this recording — a number a strategy's P&L is "
    "compared against, never added to. "
    "RANK COINS ON THE ROUND TRIPS, NOT ON `path_bps` OR `path_efficiency`. "
    "`path_bps` is the first-order variation of a sampled path: it grows with the "
    "frame rate instead of converging (VVV 20260730 LOCF-resampled 4s -> raw: "
    "31936 -> 90183 bps, still climbing at the raw feed, while round trips at "
    "30bps settled at 75-76), and bbo frame rates differ 5x across the shortlist, "
    "so `frames_per_hour` is printed beside it and the two are read together or "
    "not at all. `path_efficiency` = path/|net| is dominated by its denominator "
    "and therefore ranks by how little the price DRIFTED, not by how much it "
    "oscillated: over the ten coin-days of 20260730 it correlates +0.79 with "
    "1/|net_bps| and -0.13 with the round-trip rate, and it swings 16-31x between "
    "3h blocks of a single coin-day. Read it within one row as 'did this session "
    "go anywhere', never across rows. "
    "The rate itself is a rate over the whole recording: `round_trips_per_hour_min"
    "/max` are the same rate over each whole hour of it, and one coin-day's hours "
    "routinely spread 2-4x, so a rate whose range is null (under "
    f"{MIN_TRAVERSAL_BLOCKS} whole hours) has nothing behind it but its own "
    "session length. "
    "`at_flicker_floor` marks a rung the coin's own tick makes trivial — under "
    f"{MIN_SPACING_MID_STEPS} mid steps, where the smallest wiggle the book can "
    "print already counts; that column is not comparable with another coin's. "
    "A recording hole is not bridged specially: the jump across it is "
    "one price move like any other, so a hole both hides the traversals inside it "
    "and can invent one across it — `warnings` names the holes."
)

COUNT_KEYS = (
    "bbo",
    "l2Book_fast",
    "l2Book_slow",
    "trades",
    "activeAssetCtx",
    "hl_unclassified",
    "hl_other_coin",
    "um_bookTicker",
    "um_recovered_by_second_recording",
    "um_conflicting_update_ids",
    "um_frames_without_update_id",
)


# ---------------------------------------------------------------------------
# exact prices
# ---------------------------------------------------------------------------


def scaled_px(text) -> int:
    """A venue price string as an exact integer scaled by `PX_SCALE`.

    String arithmetic rather than `float(text) * PX_SCALE`: the point of the
    scaling is that differences between neighbouring prices are exact, and a
    float round trip is what destroys that.
    """
    if not isinstance(text, str):
        text = repr(text) if isinstance(text, float) else str(text)
    s = text.strip()
    if "e" in s or "E" in s:  # no venue writes prices this way; be safe anyway
        return int(Decimal(s).scaleb(PX_DECIMALS).to_integral_value())
    negative = s.startswith("-")
    if negative or s.startswith("+"):
        s = s[1:]
    whole, _, frac = s.partition(".")
    if len(frac) > PX_DECIMALS:  # finer than the scale holds: round, never chop
        value = int(Decimal(text.strip()).scaleb(PX_DECIMALS).to_integral_value())
        return value
    value = int((whole or "0") + frac.ljust(PX_DECIMALS, "0"))
    return -value if negative else value


# ---------------------------------------------------------------------------
# series
# ---------------------------------------------------------------------------


@dataclass
class BboSeries:
    """Every `bbo` frame of a coin-day, column-wise.

    `weight` is the time in nanoseconds until the next frame, capped at
    `SPREAD_WEIGHT_CAP_NS`; the last frame gets zero, having no successor to be
    alive until. Prices are `PX_SCALE`d integers, `mid` and `spread_bps` the
    floats derived from them.
    """

    ts: np.ndarray
    bid_px: np.ndarray
    ask_px: np.ndarray
    bid_sz: np.ndarray
    ask_sz: np.ndarray
    bid_n: np.ndarray
    ask_n: np.ndarray
    mid: np.ndarray
    spread: np.ndarray
    spread_bps: np.ndarray
    weight: np.ndarray

    def __len__(self) -> int:
        return int(self.ts.size)


@dataclass
class FundingSeries:
    """`activeAssetCtx` rate samples: the raw running rate, not yet settled.

    `rate` is Hyperliquid's running accumulator for the hour in progress, signed
    and dimensionless; `oracle_px` is the notional basis at the same instant.
    Turning this into per-hour settled rates is `funding.curve_from_samples`,
    which is where the venue convention lives — this is only the tape.
    """

    ts: np.ndarray
    rate: np.ndarray
    oracle_px: np.ndarray

    def __len__(self) -> int:
        return int(self.ts.size)


class FundingBuilder:
    """Accumulates `activeAssetCtx` frames into a `FundingSeries`."""

    def __init__(self) -> None:
        self._ts = array("q")
        self._rate = array("d")
        self._px = array("d")

    def add(self, ts, rate, oracle_px) -> None:
        self._ts.append(int(ts))
        self._rate.append(float(rate))
        self._px.append(float(oracle_px))

    def finish(self) -> FundingSeries:
        ts = np.frombuffer(self._ts, dtype=np.int64).copy()
        rate = np.frombuffer(self._rate, dtype=np.float64).copy()
        px = np.frombuffer(self._px, dtype=np.float64).copy()
        if ts.size and not np.all(np.diff(ts) >= 0):
            order = np.argsort(ts, kind="stable")
            ts, rate, px = ts[order], rate[order], px[order]
        return FundingSeries(ts=ts, rate=rate, oracle_px=px)


class BboBuilder:
    """Accumulates `bbo` frames into a `BboSeries`.

    `array` rather than lists for the same reason `build_dataset.py` uses them: a
    busy coin-day is half a million frames and a list of Python floats is an
    order of magnitude more memory than the buffer it will become.
    """

    def __init__(self) -> None:
        self._ts = array("q")
        self._bid = array("q")
        self._ask = array("q")
        self._bid_sz = array("d")
        self._ask_sz = array("d")
        self._bid_n = array("q")
        self._ask_n = array("q")

    def add(self, ts, bid_px, bid_sz, bid_n, ask_px, ask_sz, ask_n) -> None:
        self._ts.append(int(ts))
        self._bid.append(scaled_px(bid_px))
        self._ask.append(scaled_px(ask_px))
        self._bid_sz.append(float(bid_sz))
        self._ask_sz.append(float(ask_sz))
        self._bid_n.append(int(bid_n))
        self._ask_n.append(int(ask_n))

    def finish(self) -> BboSeries:
        ts = np.frombuffer(self._ts, dtype=np.int64).copy()
        bid = np.frombuffer(self._bid, dtype=np.int64).copy()
        ask = np.frombuffer(self._ask, dtype=np.int64).copy()
        bid_sz = np.frombuffer(self._bid_sz, dtype=np.float64).copy()
        ask_sz = np.frombuffer(self._ask_sz, dtype=np.float64).copy()
        bid_n = np.frombuffer(self._bid_n, dtype=np.int64).copy()
        ask_n = np.frombuffer(self._ask_n, dtype=np.int64).copy()
        if ts.size and not np.all(np.diff(ts) >= 0):
            # Two streams racing into one FIFO can invert a pair of lines
            # (quality_report.py reports it; here it only has to not corrupt the
            # weights, which are differences between neighbours).
            order = np.argsort(ts, kind="stable")
            ts, bid, ask = ts[order], bid[order], ask[order]
            bid_sz, ask_sz = bid_sz[order], ask_sz[order]
            bid_n, ask_n = bid_n[order], ask_n[order]
        mid = (bid + ask) / 2.0 / PX_SCALE
        spread = ask - bid
        with np.errstate(divide="ignore", invalid="ignore"):
            spread_bps = np.where(mid > 0, spread / PX_SCALE / mid * 1e4, np.nan)
        if ts.size > 1:
            weight = np.minimum(np.diff(ts), SPREAD_WEIGHT_CAP_NS).astype(np.float64)
            weight = np.append(weight, 0.0)
        else:
            weight = np.zeros(ts.size, dtype=np.float64)
        return BboSeries(
            ts=ts,
            bid_px=bid,
            ask_px=ask,
            bid_sz=bid_sz,
            ask_sz=ask_sz,
            bid_n=bid_n,
            ask_n=ask_n,
            mid=mid,
            spread=spread,
            spread_bps=spread_bps,
            weight=weight,
        )


@dataclass
class BookSeries:
    """`l2Book` snapshots of one cadence, flattened.

    One row per snapshot in `ts`/`best_*`/`deepest_*`; one row per *level* in
    `row`/`is_ask`/`px`/`notional`, with `row` the index of the snapshot the level
    belongs to. That shape is what makes the depth metric a `bincount` instead of
    a loop over 160,000 snapshots.
    """

    ts: np.ndarray
    best_bid: np.ndarray
    best_ask: np.ndarray
    #: The same two prices as `PX_SCALE`d integers, for the tick metric — which is
    #: a mode over differences and cannot be taken in floating point.
    best_bid_px: np.ndarray
    best_ask_px: np.ndarray
    deepest_bid: np.ndarray
    deepest_ask: np.ndarray
    row: np.ndarray
    is_ask: np.ndarray
    px: np.ndarray
    notional: np.ndarray

    def __len__(self) -> int:
        return int(self.ts.size)


class BookBuilder:
    def __init__(self) -> None:
        self._ts = array("q")
        self._best_bid = array("d")
        self._best_ask = array("d")
        self._best_bid_px = array("q")
        self._best_ask_px = array("q")
        self._deepest_bid = array("d")
        self._deepest_ask = array("d")
        self._row = array("q")
        self._is_ask = array("b")
        self._px = array("d")
        self._notional = array("d")

    def add(self, ts, bids, asks) -> None:
        """`bids` descending, `asks` ascending, each `(px, sz)`, as the venue
        sends them."""
        index = len(self._ts)
        self._ts.append(int(ts))
        for side, levels in ((0, bids), (1, asks)):
            first = math.nan
            first_scaled = 0
            last = math.nan
            for px, sz in levels:
                price = float(px)
                if math.isnan(first):
                    first = price
                    first_scaled = scaled_px(px)
                last = price
                self._row.append(index)
                self._is_ask.append(side)
                self._px.append(price)
                self._notional.append(price * float(sz))
            if side == 0:
                self._best_bid.append(first)
                self._best_bid_px.append(first_scaled)
                self._deepest_bid.append(last)
            else:
                self._best_ask.append(first)
                self._best_ask_px.append(first_scaled)
                self._deepest_ask.append(last)

    def finish(self) -> BookSeries:
        return BookSeries(
            ts=np.frombuffer(self._ts, dtype=np.int64).copy(),
            best_bid=np.frombuffer(self._best_bid, dtype=np.float64).copy(),
            best_ask=np.frombuffer(self._best_ask, dtype=np.float64).copy(),
            best_bid_px=np.frombuffer(self._best_bid_px, dtype=np.int64).copy(),
            best_ask_px=np.frombuffer(self._best_ask_px, dtype=np.int64).copy(),
            deepest_bid=np.frombuffer(self._deepest_bid, dtype=np.float64).copy(),
            deepest_ask=np.frombuffer(self._deepest_ask, dtype=np.float64).copy(),
            row=np.frombuffer(self._row, dtype=np.int64).copy(),
            is_ask=np.frombuffer(self._is_ask, dtype=np.int8).copy().astype(bool),
            px=np.frombuffer(self._px, dtype=np.float64).copy(),
            notional=np.frombuffer(self._notional, dtype=np.float64).copy(),
        )


@dataclass
class VolWindows:
    """The 60s windows that survived the sample floor.

    `frame_window` is one entry per `bbo` frame: the index into `vol` of the
    window that frame is in, or -1 if that window was dropped. It is how the
    per-quintile spread is pooled over *frames* — the quintile's median spread is
    time-weighted over every frame in it, not a median of per-window medians.
    """

    start_ts: np.ndarray
    vol: np.ndarray
    spread_bps: np.ndarray
    frame_window: np.ndarray
    dropped_windows: int


# ---------------------------------------------------------------------------
# 1. tick
# ---------------------------------------------------------------------------


def price_delta_counts(px: np.ndarray) -> Counter:
    """Counts of the positive gaps between consecutive *distinct* prices.

    Repeats carry no information about the tick and are collapsed first; the
    downward moves are dropped rather than folded in, so the count is over one
    direction and a mode is a mode of something.

    Non-positive prices are skipped over rather than treated as observations: an
    `l2Book` side can arrive empty on a thin coin, and a zero in the middle of a
    price series produces one enormous positive delta on the way back out of it.
    """
    px = px[px > 0]
    if px.size < 2:
        return Counter()
    distinct = px[np.concatenate(([True], px[1:] != px[:-1]))]
    if distinct.size < 2:
        return Counter()
    delta = np.diff(distinct)
    return Counter(delta[delta > 0].tolist())


def mode_delta(counts) -> Optional[int]:
    """The most common gap; ties go to the smaller one.

    A tie between one tick and two is a book that alternates between them, and
    the tick is the smaller of the two by definition.
    """
    if not counts:
        return None
    return min(counts.items(), key=lambda kv: (-kv[1], kv[0]))[0]


def formula_tick(median_px: float) -> float:
    """§2.1's five-significant-figure term: `10^(floor(log10 px) - 4)`.

    The `max(10^-(6 - szDecimals), ...)` term of the full rule needs `szDecimals`,
    which no recording carries, so this is the half that can be computed here —
    and `summarize_tick` reports where it therefore disagrees with the recording.
    """
    if not median_px or median_px <= 0 or not math.isfinite(median_px):
        return math.nan
    return 10.0 ** (math.floor(math.log10(median_px)) - 4)


def _decade_split_fraction(mid: np.ndarray, weight, median_px: float):
    """Share of the session quoted in a different price decade from the median.

    Hyperliquid's tick is a step function of the price, so a row whose price
    crossed a power of ten has two ticks and the row reports one. Nothing else
    catches it: `mismatch` compares the single measured mode against the formula
    at the *median* price, and those agree as soon as either half of the window is
    the more populous one. What comes out wrong is the §9.3 gate — a coin one tick
    wide throughout reads `frac_time_ge_3_ticks ≈ 0.5` — so this number bounds how
    much of the row's tick arithmetic is not to be trusted.
    """
    if mid.size == 0 or weight is None or not math.isfinite(median_px) or median_px <= 0:
        return None
    weight = np.asarray(weight, dtype=np.float64)
    if weight.size != mid.size:
        return None
    usable = np.isfinite(mid) & (mid > 0) & (weight > 0)
    total = float(weight[usable].sum())
    if total <= 0:
        return None
    # Substituted rather than masked: `log10` of a zero or a negative would raise
    # the floating-point flags for lanes `usable` already excludes.
    decade = np.floor(np.log10(np.where(usable, mid, 1.0)))
    off = usable & (decade != math.floor(math.log10(median_px)))
    return float(weight[off].sum() / total)


def summarize_tick(bid_counts, ask_counts, mid: np.ndarray, weight=None) -> dict:
    """The measured tick against the reconstructed one.

    `bid_counts`/`ask_counts` are `price_delta_counts` summed over every channel
    that prints a best price. `used` is what the rest of the report divides by,
    and it is the measurement whenever there is one — `mismatch` says the formula
    disagreed, not that the measurement is in doubt (§8.6: the formula is the
    reconstruction, the recording is the evidence).

    `weight` is the `bbo` frame weighting, which turns "one tick per row" from an
    assumption into a checked one: see `_decade_split_fraction`.
    """
    bid_mode = mode_delta(bid_counts)
    ask_mode = mode_delta(ask_counts)
    median_px = float(np.median(mid)) if mid.size else math.nan
    formula = formula_tick(median_px)

    # The best bid is the primary series (§2.1 reconstructs the tick from the
    # price grid, which both sides share); the ask is the check. Where the bid
    # never moved and the ask did, the ask is better than nothing and says so.
    if bid_mode is not None:
        used_mode, source, counts = bid_mode, "bid", bid_counts
    elif ask_mode is not None:
        used_mode, source, counts = ask_mode, "ask", ask_counts
    else:
        used_mode, source, counts = None, None, Counter()

    empirical = None if used_mode is None else used_mode / PX_SCALE
    total = sum(counts.values())
    mismatch = False
    if empirical is not None and formula and math.isfinite(formula):
        mismatch = abs(empirical - formula) / formula > 0.05
    if empirical is not None:
        used, used_source = empirical, "empirical"
    elif math.isfinite(formula):
        used, used_source = formula, "formula"
    else:
        # No frames at all: a row of nulls, not a row naming a source it has not.
        used, used_source = None, None
    return {
        "median_px": _num(median_px),
        "empirical": _num(empirical),
        "empirical_from_bid": _num(None if bid_mode is None else bid_mode / PX_SCALE),
        "empirical_from_ask": _num(None if ask_mode is None else ask_mode / PX_SCALE),
        "empirical_source": source,
        "sides_agree": None if (bid_mode is None or ask_mode is None) else bid_mode == ask_mode,
        "mode_share": _num(None if not total else counts[used_mode] / total),
        "price_moves": int(total),
        "formula": _num(formula),
        "mismatch": bool(mismatch),
        "frac_time_off_median_decade": _num(_decade_split_fraction(mid, weight, median_px)),
        "used": _num(used),
        "used_source": used_source,
    }


# ---------------------------------------------------------------------------
# 2. spread
# ---------------------------------------------------------------------------


def time_weighted_quantile(
    values: np.ndarray, weights: np.ndarray, q: float
) -> Optional[float]:
    """The smallest value whose cumulative weight reaches `q` of the total.

    The median was always the `q=0.5` case of this; the pre-committed coin
    screen names the 25th percentile of the spread, so the `0.5` is a parameter
    now. Weighted, not `np.quantile`: a frame counts for the time it stood, and
    a burst of quotes in one busy second must not outvote a quiet minute.
    """
    if values.size == 0:
        return None
    finite = np.isfinite(values) & (weights > 0)
    if not finite.any():
        return None
    v = values[finite]
    w = weights[finite]
    order = np.argsort(v, kind="stable")
    v = v[order]
    cumulative = np.cumsum(w[order])
    index = int(np.searchsorted(cumulative, q * cumulative[-1], side="left"))
    return float(v[min(index, v.size - 1)])


def time_weighted_median(values: np.ndarray, weights: np.ndarray) -> Optional[float]:
    """The smallest value whose cumulative weight reaches half the total."""
    return time_weighted_quantile(values, weights, 0.5)


def _weight_fraction(mask: np.ndarray, weights: np.ndarray) -> Optional[float]:
    total = float(weights.sum())
    if total <= 0:
        return None
    return float(weights[mask].sum() / total)


def summarize_spread(series: BboSeries, tick_scaled: Optional[int]) -> dict:
    weight = series.weight
    weighted = weight > 0
    total_ns = float(weight.sum())
    out = {
        "frames": len(series),
        "frames_weighted": int(weighted.sum()),
        "weighted_seconds": total_ns / SEC,
        "crossed_or_zero_frames": int((series.spread <= 0).sum()),
        # p25 is what the pre-committed screen rule names
        # (`spread_p25 > fee_RT + adverse + funding x hold`); p50 and p75 are
        # here so the row shows the shape of the distribution the gate reads one
        # point of.
        "p25_bps": time_weighted_quantile(series.spread_bps, weight, 0.25),
        "p50_bps": time_weighted_median(series.spread_bps, weight),
        "p75_bps": time_weighted_quantile(series.spread_bps, weight, 0.75),
        "p50_ticks": None,
        "frac_time_at_1_tick": None,
        "frac_time_ge_3_ticks": None,
    }
    if tick_scaled:
        ticks = series.spread / float(tick_scaled)
        out["p50_ticks"] = time_weighted_median(ticks, weight)
        # Integer comparison: the whole point of the scaled prices is that "is
        # this spread exactly one tick" is a question with an answer.
        out["frac_time_at_1_tick"] = _weight_fraction(series.spread == tick_scaled, weight)
        out["frac_time_ge_3_ticks"] = _weight_fraction(series.spread >= 3 * tick_scaled, weight)
    return out


# ---------------------------------------------------------------------------
# 2b. funding (#40) — the third term of the pre-committed screen rule
# ---------------------------------------------------------------------------


#: The round-trip maker fee, in bps. MEASURED, not assumed: 139/139 maker fills
#: on our own Hyperliquid mainnet ledger paid 1.4991-1.5000 bps per side.
DEFAULT_FEE_RT_BPS = 3.0

#: The inventory bias used INSIDE the gate. One, deliberately: the one-sided
#: worst case, which gives the shortest break-even hold and is therefore
#: fail-closed. The MEASURED bias of a mid-following grid is 0.047
#: (|signed TWA| / |absolute TWA|) — but that is n=1 run, one coin, 43 hours,
#: 14 charge events, and it is a property of the STRATEGY, not of the coin.
#: Transplanting it across 18 coins is precisely the error this term exists to
#: stop; it is reported beside the gate as informational and never multiplied in.
GATE_INVENTORY_BIAS = 1.0
MEASURED_INVENTORY_BIAS = 0.047

#: `erased` if a hair over one hour in a hundred carries a rate above the whole
#: gross capture; `tail_risk` at one in two thousand. Pre-committed 2026-08-09,
#: before the candidates' data was seen, from the structure of the rule (a mean
#: gate and a tail gate) rather than tuned against a ranking. Changing either
#: needs a date and a measurement.
ERASED_TAIL_P = 0.01
TAIL_RISK_TAIL_P = 0.0005


def summarize_funding(
    series: "FundingSeries",
    *,
    spread_p25_bps: Optional[float] = None,
    fee_rt_bps: Optional[float] = None,
    adverse_bps: Optional[float] = None,
    hold_mean_s: Optional[float] = None,
    hold_p95_s: Optional[float] = None,
    interval_ns: int = funding.HL_INTERVAL_NS,
) -> dict:
    """Funding as the screen's third term — a distribution and a gate.

    THE STOPPING RULE IS INVERTED, NOT FED A CONSTANT. The pre-committed rule is

        spread_p25 > fee_RT + adverse + funding x hold

    which is the same statement as `hold < BE_hold`, where

        BE_hold_hours = (spread_p25 - fee_RT - adverse) / (bias x |rate|)

    Inverting it is what lets the coin be judged against a hold measured from
    its OWN run instead of a hold transplanted from HYPE's.

    THE RATE IS A DISTRIBUTION, NEVER A POINT ESTIMATE, and that is measured
    rather than stylistic. Predicting a day's mean hourly rate with the trailing
    7-day mean gives MAE 0.610 bps/h across 19 coins x 8 days against 0.640 for
    the flat interest default — a 5% edge, and WORSE than the constant for 9 of
    19 coins. A spot read is worse still: CASHCAT moved +4.125 -> +3.030 bps/h
    and ACE -0.938 -> -1.811 within two hours of the brief that quoted them, and
    over 168 hours CASHCAT averages +1.18 while ACE averages -8.93. A spot read
    mis-ranks the very coins the term was introduced to catch. So the gate is
    evaluated at three named points of the empirical distribution — the median
    rate, the p95 rate and the worst hour — and no forecast is made.

    `|rate|`, not the signed rate, at every one of those points. A coin whose
    hours cancel in the mean can still bleed on whichever side inventory
    happens to sit; fail-closed says assume the paying side.

    A missing input is `unknown`, and `unknown` DOES NOT PASS THE SCREEN. There
    is no default hold and no fallback to another coin's: `expected_hold` is
    measured from the run being scored or it is not known. `adverse_bps` comes
    from the #32 fill-quality harness and this tool refuses to guess it.
    """
    curve = funding.curve_from_samples(
        series.ts, series.rate, series.oracle_px, interval_ns=interval_ns
    )
    stats = funding.rate_stats(curve)
    rates_bps = np.array([abs(b.rate) for b in curve.boundaries], dtype=np.float64) * 1e4

    warnings: list = []
    if curve.unresolved:
        warnings.append(
            f"funding: {len(curve.unresolved)} hour mark(s) inside the recorded span "
            f"carry no rate; they are reported rather than zero-filled, and every "
            f"quantile below is over the {stats['hours_covered']} hours that do"
        )

    have_gate = None not in (spread_p25_bps, fee_rt_bps, adverse_bps, hold_mean_s, hold_p95_s)
    gross = (
        float(spread_p25_bps) - float(fee_rt_bps) - float(adverse_bps)
        if None not in (spread_p25_bps, fee_rt_bps, adverse_bps)
        else None
    )

    def _be(rate_bps: Optional[float]) -> Optional[float]:
        if gross is None or rate_bps is None or rate_bps <= 0:
            return None
        return gross / (GATE_INVENTORY_BIAS * rate_bps)

    median_abs = float(np.median(rates_bps)) if rates_bps.size else None
    p95_abs = float(np.percentile(rates_bps, 95)) if rates_bps.size else None
    worst_abs = float(rates_bps.max()) if rates_bps.size else None
    p_tail = (
        float((rates_bps >= gross).mean())
        if rates_bps.size and gross is not None and gross > 0
        else None
    )

    break_even = (
        {
            "at_median_rate": _num(_be(median_abs)),
            "at_p95_rate": _num(_be(p95_abs)),
            "at_worst_hour": _num(_be(worst_abs)),
        }
        if have_gate and gross is not None
        else None
    )

    verdict = "unknown"
    if have_gate and break_even is not None:
        mean_h = float(hold_mean_s) / 3600.0
        p95_h = float(hold_p95_s) / 3600.0
        be_median = break_even["at_median_rate"]
        be_worst = break_even["at_worst_hour"]
        erased = (be_median is not None and be_median < mean_h) or (
            p_tail is not None and p_tail > ERASED_TAIL_P
        )
        tail = (be_worst is not None and be_worst < p95_h) or (
            p_tail is not None and p_tail > TAIL_RISK_TAIL_P
        )
        verdict = "erased" if erased else ("tail_risk" if tail else "immaterial")

    return {
        "source": "activeAssetCtx",
        "interval_hours": interval_ns / 3600 / SEC,
        "hours_covered": stats["hours_covered"],
        "hours_unresolved": len(curve.unresolved),
        "rate_bps_per_hour": {
            "mean": _num(stats["mean"]),
            "median": _num(stats["median"]),
            "p05": _num(stats["p05"]),
            "p95": _num(stats["p95"]),
            "max_abs": _num(stats["max_abs"]),
            "median_abs": _num(median_abs),
            "p95_abs": _num(p95_abs),
            "frac_at_interest_default": _num(stats["frac_at_interest_default"]),
            "sign_flips": stats["sign_flips"],
        },
        "interest_default_bps_per_hour": funding.HL_INTEREST_DEFAULT * 1e4,
        "gross_capture_bps": _num(gross),
        "break_even_hold_hours": break_even,
        "tail": {
            "p_hour_rate_ge_gross": _num(p_tail),
            "worst_hour_bps": _num(worst_abs),
        },
        "hold_mean_s": _num(hold_mean_s),
        "hold_p95_s": _num(hold_p95_s),
        "fee_rt_bps": _num(fee_rt_bps),
        "adverse_bps": _num(adverse_bps),
        "inventory_bias_gate": GATE_INVENTORY_BIAS,
        "inventory_bias_informational": MEASURED_INVENTORY_BIAS,
        "expected_funding_bps_per_hour_informational": _num(
            None if median_abs is None else MEASURED_INVENTORY_BIAS * median_abs
        ),
        "verdict": verdict,
        "warnings": warnings,
        "caveat": FUNDING_CAVEAT,
    }


FUNDING_CAVEAT = (
    "rates are the empirical distribution of the settled hourly series, used as "
    "a risk description and NOT as a forecast (the trailing 7-day mean beats a "
    "flat constant by 5% across 19 coins and loses to it on 9 of them). the gate "
    "uses inventory bias 1.0 — the one-sided worst case, fail-closed — not the "
    "measured 0.047, which is one strategy on one coin over 43 hours. hold is "
    "supplied per coin from that coin's own run; absent, the verdict is "
    "'unknown' and the coin does not pass"
)


# ---------------------------------------------------------------------------
# 3. conditional spread curve
# ---------------------------------------------------------------------------


def grid_locf(ts: np.ndarray, values: np.ndarray, grid_ts: np.ndarray, max_age_ns: int):
    """Last observation carried forward onto `grid_ts`, with an age bound.

    Returns `(values_on_grid, valid)`. A grid point with no earlier observation,
    or whose newest observation is older than `max_age_ns`, is invalid — carrying
    a stale quote forward manufactures a zero return, and a run of those reads as
    a calm market rather than as a missing feed.
    """
    if ts.size == 0 or grid_ts.size == 0:
        return np.zeros(grid_ts.size), np.zeros(grid_ts.size, dtype=bool)
    index = np.searchsorted(ts, grid_ts, side="right") - 1
    valid = index >= 0
    safe = np.where(valid, index, 0)
    out = values[safe]
    age = grid_ts - ts[safe]
    valid = valid & (age <= max_age_ns) & np.isfinite(out) & (out > 0)
    return out, valid


def vol_windows(series: BboSeries) -> VolWindows:
    """Realized vol and median spread per 60s window.

    Windows are absolute multiples of `VOL_WINDOW_NS` since the epoch, so they
    are the same wall-clock minutes for every coin and every day and two runs
    bucket identically. Vol is the standard deviation of the log returns of the
    mid sampled on a 1s LOCF grid inside the window — a grid rather than the
    frames themselves, because a coin quoting five times a second would otherwise
    have five times another coin's number of returns and a different quantity.
    """
    empty = VolWindows(
        start_ts=np.zeros(0, dtype=np.int64),
        vol=np.zeros(0),
        spread_bps=np.zeros(0),
        frame_window=np.zeros(len(series), dtype=np.int64) - 1,
        dropped_windows=0,
    )
    if len(series) < 2:
        return empty

    ts = series.ts
    first = int(ts[0]) // VOL_WINDOW_NS * VOL_WINDOW_NS
    last = int(ts[-1]) // VOL_WINDOW_NS * VOL_WINDOW_NS
    starts = np.arange(first, last + VOL_WINDOW_NS, VOL_WINDOW_NS, dtype=np.int64)
    per_window = VOL_WINDOW_NS // VOL_GRID_NS
    grid = (starts[:, None] + np.arange(per_window, dtype=np.int64) * VOL_GRID_NS).ravel()
    mid, valid = grid_locf(ts, series.mid, grid, LOCF_MAX_AGE_NS)
    mid = mid.reshape(len(starts), per_window)
    valid = valid.reshape(len(starts), per_window)

    pair = valid[:, 1:] & valid[:, :-1]
    with np.errstate(divide="ignore", invalid="ignore"):
        returns = np.where(pair, np.log(np.where(valid, mid, 1.0)[:, 1:] / np.where(valid, mid, 1.0)[:, :-1]), 0.0)
    n = pair.sum(axis=1).astype(np.float64)
    s1 = returns.sum(axis=1)
    s2 = (returns * returns).sum(axis=1)
    with np.errstate(divide="ignore", invalid="ignore"):
        variance = np.where(n > 1, (s2 - s1 * s1 / np.where(n > 0, n, 1.0)) / np.where(n > 1, n - 1, 1.0), np.nan)
    vol = np.sqrt(np.maximum(variance, 0.0))
    keep = (valid.sum(axis=1) >= MIN_VOL_SAMPLES) & (n > 1) & np.isfinite(vol)

    # Frames are ts-sorted, so each window is a contiguous slice.
    bounds = np.searchsorted(ts, starts, side="left")
    bounds = np.append(bounds, len(series))
    spread_bps = np.full(len(starts), np.nan)
    for i in np.flatnonzero(keep):
        lo, hi = int(bounds[i]), int(bounds[i + 1])
        median = time_weighted_median(series.spread_bps[lo:hi], series.weight[lo:hi])
        spread_bps[i] = np.nan if median is None else median
    keep = keep & np.isfinite(spread_bps)

    compact = np.cumsum(keep) - 1
    frame_window_all = (ts - starts[0]) // VOL_WINDOW_NS
    frame_window = np.where(keep[frame_window_all], compact[frame_window_all], -1)
    return VolWindows(
        start_ts=starts[keep],
        vol=vol[keep],
        spread_bps=spread_bps[keep],
        frame_window=frame_window,
        dropped_windows=int((~keep).sum()),
    )


def _spearman(x: np.ndarray, y: np.ndarray) -> Optional[float]:
    """Rank correlation, average ranks for ties.

    Ties are the normal case here: every window of a book that never widens has
    the same median spread, and a tie-blind rank would invent an ordering among
    them.
    """
    if x.size < 3:
        return None
    rx = _average_ranks(x)
    ry = _average_ranks(y)
    sx = rx.std()
    sy = ry.std()
    if sx == 0 or sy == 0:
        return None
    return float(((rx - rx.mean()) * (ry - ry.mean())).mean() / (sx * sy))


def _average_ranks(values: np.ndarray) -> np.ndarray:
    order = np.argsort(values, kind="stable")
    ranks = np.empty(values.size, dtype=np.float64)
    ranks[order] = np.arange(values.size, dtype=np.float64)
    sorted_values = values[order]
    start = 0
    for i in range(1, values.size + 1):
        if i == values.size or sorted_values[i] != sorted_values[start]:
            if i - start > 1:
                ranks[order[start:i]] = (start + i - 1) / 2.0
            start = i
    return ranks


def conditional_spread_curve(windows: VolWindows, series: BboSeries) -> dict:
    out = {
        "windows": int(windows.vol.size),
        "dropped_windows": int(windows.dropped_windows),
        "p50_bps_by_vol_quintile": None,
        "vol_by_quintile": None,
        "ratio_q5_q1": None,
        "spearman_vol_spread": None,
        "note": None,
        "caveat": CONDITIONAL_CAVEAT,
    }
    if windows.vol.size < MIN_VOL_WINDOWS:
        out["note"] = (
            f"{windows.vol.size} usable windows, {MIN_VOL_WINDOWS} needed for a "
            f"quintile split ({windows.dropped_windows} dropped for fewer than "
            f"{MIN_VOL_SAMPLES} valid grid samples)"
        )
        return out

    order = np.argsort(windows.vol, kind="stable")
    quintile_of_window = np.empty(windows.vol.size, dtype=np.int64)
    for q, group in enumerate(np.array_split(order, 5)):
        quintile_of_window[group] = q

    in_window = windows.frame_window >= 0
    quintile_of_frame = np.where(
        in_window, quintile_of_window[np.where(in_window, windows.frame_window, 0)], -1
    )
    medians = []
    vols = []
    for q in range(5):
        frames = quintile_of_frame == q
        medians.append(time_weighted_median(series.spread_bps[frames], series.weight[frames]))
        vols.append(float(np.median(windows.vol[quintile_of_window == q])))
    out["p50_bps_by_vol_quintile"] = [_num(m) for m in medians]
    out["vol_by_quintile"] = [_num(v) for v in vols]
    if medians[0] and medians[4] is not None:
        out["ratio_q5_q1"] = _num(medians[4] / medians[0])
    out["spearman_vol_spread"] = _num(_spearman(windows.vol, windows.spread_bps))
    return out


# ---------------------------------------------------------------------------
# 4. touch queue
# ---------------------------------------------------------------------------


def price_run_lifetimes(ts: np.ndarray, px: np.ndarray, max_gap_ns: int):
    """How long each best price stood, in nanoseconds: `(measured, unmeasurable)`.

    A run is terminated by the first frame carrying a different price, so its
    lifetime is that frame's timestamp minus the run's first. The final run has no
    terminating frame and is not counted.

    A run containing a hole longer than `max_gap_ns` cannot be measured — the
    price may have moved and come back with nothing recording it — so it is
    returned separately rather than silently discarded. It has to be visible:
    censoring falls entirely on the *long* runs, so it pulls down every quantile
    of the very distribution being reported. `max_gap_ns` is a recording-hole
    bound (`LIFETIME_MAX_GAP_NS`), not a quote-staleness one; a standing quote on
    a venue that publishes on change looks exactly like silence.
    """
    empty = np.zeros(0, dtype=np.int64)
    if ts.size < 2:
        return empty, empty
    starts = np.flatnonzero(np.concatenate(([True], px[1:] != px[:-1])))
    if starts.size < 2:
        return empty, empty
    lifetime = ts[starts[1:]] - ts[starts[:-1]]
    step = np.diff(ts)
    worst = np.maximum.reduceat(step, starts[:-1])
    measurable = worst <= max_gap_ns
    return lifetime[measurable], lifetime[~measurable]


def _side_stats(n_orders: np.ndarray, usd: np.ndarray) -> dict:
    return {
        "n_orders_p50": _median(n_orders),
        "n_orders_p90": _percentile(n_orders, 90),
        "usd_p50": _median(usd),
        "usd_p90": _percentile(usd, 90),
    }


def _lifetime_stats(lifetimes: np.ndarray, dropped: np.ndarray) -> dict:
    """The measured runs, and how much of the distribution is not in them."""
    seconds = lifetimes / SEC
    return {
        "p50_seconds": _median(seconds),
        "p90_seconds": _percentile(seconds, 90),
        "runs": int(lifetimes.size),
        # A quantile taken over what survived a filter, with no count of what did
        # not, cannot be checked by its reader. These two say how much.
        "runs_dropped_to_gaps": int(dropped.size),
        "seconds_dropped_to_gaps": float(dropped.sum()) / SEC,
    }


def summarize_touch(series: BboSeries, runs=None) -> dict:
    """§2.2's touch, from `bbo` — the only channel carrying `n`.

    Frame-weighted, not time-weighted: the question §2.2 asks is what a small
    order joining the touch is up against when it arrives, and arrivals are
    events, not durations.

    `runs` is `(bid, ask)`, each a `price_run_lifetimes` pair, when the caller
    already computed them — which the pooled row must do: concatenating two days'
    frames would otherwise invent a price run bridging midnight out of the last
    quote of one day and the first of the next.
    """
    bid_usd = series.bid_px / PX_SCALE * series.bid_sz
    ask_usd = series.ask_px / PX_SCALE * series.ask_sz
    if runs is None:
        runs = (
            price_run_lifetimes(series.ts, series.bid_px, LIFETIME_MAX_GAP_NS),
            price_run_lifetimes(series.ts, series.ask_px, LIFETIME_MAX_GAP_NS),
        )
    return {
        "frames": len(series),
        "bid": _side_stats(series.bid_n, bid_usd),
        "ask": _side_stats(series.ask_n, ask_usd),
        "combined": _side_stats(
            np.concatenate((series.bid_n, series.ask_n)),
            np.concatenate((bid_usd, ask_usd)),
        ),
        # Named for what it measures. The recording carries no order ids, so an
        # order joining the touch cannot be followed; what this bounds is how
        # long a price *level* stays best, which is the most a public feed says.
        "touch_price_lifetime": {
            "bid": _lifetime_stats(*runs[0]),
            "ask": _lifetime_stats(*runs[1]),
            "gap_threshold_seconds": LIFETIME_MAX_GAP_NS / SEC,
            "measures": "time until the best price on that side changes, not "
            "queue-position survival",
        },
    }


# ---------------------------------------------------------------------------
# 5. depth at the rungs
# ---------------------------------------------------------------------------


def _depth_at(series: BookSeries, bps: float):
    """`(min-side notional per snapshot, truncated mask, usable mask)`.

    The thinner side is what a grid rung can actually lean on, so it is the one
    reported. `truncated` marks a snapshot whose deepest recorded level is still
    inside the window: everything past the feed's level cap is invisible and the
    number is a lower bound (§8.7).
    """
    usable = (
        np.isfinite(series.best_bid)
        & np.isfinite(series.best_ask)
        & (series.best_bid > 0)
        & (series.best_ask > 0)
    )
    if series.ts.size == 0:
        return np.zeros(0), np.zeros(0, dtype=bool), usable
    mid = (series.best_bid + series.best_ask) / 2.0
    lo = mid * (1.0 - bps / 1e4)
    hi = mid * (1.0 + bps / 1e4)
    inside = np.where(
        series.is_ask, series.px <= hi[series.row], series.px >= lo[series.row]
    )
    contribution = series.notional * inside
    bid_side = np.bincount(
        series.row, weights=contribution * ~series.is_ask, minlength=series.ts.size
    )
    ask_side = np.bincount(
        series.row, weights=contribution * series.is_ask, minlength=series.ts.size
    )
    value = np.minimum(bid_side, ask_side)
    truncated = (series.deepest_bid >= lo) | (series.deepest_ask <= hi)
    return value, truncated, usable


def summarize_depth(fast: BookSeries, slow: Optional[BookSeries] = None) -> dict:
    out = {"snapshots": 0, "snapshots_slow": 0}
    for bps in DEPTH_RUNGS_BPS:
        out[f"d{bps}"] = None
        out[f"truncated_frac_d{bps}"] = None
    out["d30_slow"] = None
    out["truncated_frac_d30_slow"] = None
    out["levels_per_snapshot_p50"] = None

    if len(fast):
        usable = None
        for bps in DEPTH_RUNGS_BPS:
            value, truncated, usable = _depth_at(fast, bps)
            out[f"d{bps}"] = _median(value[usable])
            out[f"truncated_frac_d{bps}"] = (
                None if not usable.any() else float(truncated[usable].mean())
            )
        out["snapshots"] = int(usable.sum())
        out["levels_per_snapshot_p50"] = _median(
            np.bincount(fast.row, minlength=fast.ts.size)[usable].astype(np.float64)
        )
    if slow is not None and len(slow):
        value, truncated, usable = _depth_at(slow, 30)
        out["d30_slow"] = _median(value[usable])
        out["truncated_frac_d30_slow"] = (
            None if not usable.any() else float(truncated[usable].mean())
        )
        out["snapshots_slow"] = int(usable.sum())
    return out


# ---------------------------------------------------------------------------
# 6. lead-lag
# ---------------------------------------------------------------------------


def _pair_sums(x, x2, vx, y, y2, vy, k: int, lo: int, hi: int) -> np.ndarray:
    """The six sums a Pearson correlation of `x[t]` against `y[t-k]` needs.

    Sums rather than a correlation so that days can be pooled by adding them, and
    dot products rather than boolean indexing so that 81 lags over 1.7M cells is
    BLAS work rather than 81 array allocations. `x` and `y` are already zero
    wherever they are invalid, which is what makes `x @ y` the masked sum of
    products and `x2 @ vy` the masked sum of squares.
    """
    a0 = max(lo, k if k > 0 else 0)
    a1 = min(hi, x.size + (k if k < 0 else 0))
    if a1 - a0 < 3:
        return np.zeros(6)
    xs, x2s, vxs = x[a0:a1], x2[a0:a1], vx[a0:a1]
    ys, y2s, vys = y[a0 - k:a1 - k], y2[a0 - k:a1 - k], vy[a0 - k:a1 - k]
    # Apple's Accelerate BLAS raises the floating-point flags for lanes it never
    # used, so every one of these dots warns about a division by zero it did not
    # do. The guards that matter are in `_corr_curve`, which refuses a lag with
    # too few observations or no variance rather than trusting the number.
    with np.errstate(all="ignore"):
        return np.array(
            [
                float(vxs @ vys),
                float(xs @ vys),
                float(ys @ vxs),
                float(x2s @ vys),
                float(y2s @ vxs),
                float(xs @ ys),
            ]
        )


def leadlag_accum(hl_ts, hl_mid, um_ts, um_mid) -> np.ndarray:
    """Cross-correlation sums, shape `(5, len(LEADLAG_LAGS_MS), 6)`.

    Row 0 is the whole overlap; rows 1-4 are its four quarters, which are the
    stability check — one lag measured over a whole day is one number and cannot
    show that it moved.

    Both venues are sampled onto one 50ms LOCF grid over the span they *both*
    cover, and a cell either venue is staler than `LEADLAG_MAX_AGE_NS` in is
    dropped. The grid is anchored on absolute multiples of the cell so that the
    same recording always produces the same cells.
    """
    lags = LEADLAG_LAGS_MS
    zero = np.zeros((5, len(lags), 6))
    hl_ts = np.asarray(hl_ts, dtype=np.int64)
    um_ts = np.asarray(um_ts, dtype=np.int64)
    if hl_ts.size < 2 or um_ts.size < 2:
        return zero
    start = max(int(hl_ts[0]), int(um_ts[0]))
    end = min(int(hl_ts[-1]), int(um_ts[-1]))
    start = -(-start // LEADLAG_CELL_NS) * LEADLAG_CELL_NS
    if end <= start:
        return zero
    grid = np.arange(start, end + 1, LEADLAG_CELL_NS, dtype=np.int64)
    if grid.size < 3:
        return zero

    hl_g, hl_v = grid_locf(hl_ts, np.asarray(hl_mid, dtype=np.float64), grid, LEADLAG_MAX_AGE_NS)
    um_g, um_v = grid_locf(um_ts, np.asarray(um_mid, dtype=np.float64), grid, LEADLAG_MAX_AGE_NS)

    def returns(values, valid):
        pair = valid[1:] & valid[:-1]
        safe = np.where(valid, values, 1.0)
        with np.errstate(divide="ignore", invalid="ignore"):
            r = np.where(pair, np.log(safe[1:] / safe[:-1]), 0.0)
        return np.nan_to_num(r, nan=0.0, posinf=0.0, neginf=0.0), pair.astype(np.float64)

    x, vx = returns(hl_g, hl_v)
    y, vy = returns(um_g, um_v)
    x2 = x * x
    y2 = y * y

    acc = np.zeros((5, len(lags), 6))
    cells = x.size
    ranges = [(0, cells)]
    edges = [round(cells * q / 4) for q in range(5)]
    ranges += [(edges[q], edges[q + 1]) for q in range(4)]
    step = LEADLAG_CELL_NS // MS
    for r, (lo, hi) in enumerate(ranges):
        for i, lag_ms in enumerate(lags):
            acc[r, i] = _pair_sums(x, x2, vx, y, y2, vy, lag_ms // step, lo, hi)
    return acc


def _corr_curve(sums: np.ndarray) -> np.ndarray:
    """Pearson correlation per lag from the accumulated sums; NaN where thin."""
    n, sx, sy, sxx, syy, sxy = (sums[:, i] for i in range(6))
    with np.errstate(divide="ignore", invalid="ignore"):
        safe = np.where(n > 0, n, np.nan)
        cov = sxy / safe - (sx / safe) * (sy / safe)
        vx = sxx / safe - (sx / safe) ** 2
        vy = syy / safe - (sy / safe) ** 2
        corr = cov / np.sqrt(vx * vy)
    return np.where((n >= MIN_LEADLAG_CELLS) & (vx > 0) & (vy > 0), corr, np.nan)


def _argmax_lag(corr: np.ndarray) -> Optional[int]:
    """The lag of the largest |corr|; ties go to the one nearest zero."""
    if not np.isfinite(corr).any():
        return None
    magnitude = np.where(np.isfinite(corr), np.abs(corr), -1.0)
    best = min(
        range(len(LEADLAG_LAGS_MS)),
        key=lambda i: (-magnitude[i], abs(LEADLAG_LAGS_MS[i]), LEADLAG_LAGS_MS[i]),
    )
    return LEADLAG_LAGS_MS[best]


def summarize_leadlag(acc: np.ndarray, cadence_ms=None) -> dict:
    """The lag curve, with the publication cadence that bounds how it reads.

    `cadence_ms` is `(hyperliquid, binance)` median frame intervals. They belong
    in this output rather than beside it: a venue that publishes every 600ms
    cannot show a move sooner than that, whoever discovered it, so a lag of that
    order is partly the feed and only partly the market.
    """
    zero_index = LEADLAG_LAGS_MS.index(0)
    corr = _corr_curve(acc[0])
    lag = _argmax_lag(corr)
    peak = None if lag is None else corr[LEADLAG_LAGS_MS.index(lag)]
    hl_ms, um_ms = cadence_ms if cadence_ms else (None, None)
    return {
        "cells": int(acc[0, zero_index, 0]),
        "lag_ms": lag,
        "peak_corr": _num(peak),
        "corr_at_lag_0": _num(corr[zero_index]),
        "lag_ms_by_quarter": [_argmax_lag(_corr_curve(acc[q])) for q in range(1, 5)],
        "lags_ms": list(LEADLAG_LAGS_MS),
        "corr_by_lag": [_num(c, 6) for c in corr],
        "cell_ms": LEADLAG_CELL_NS // MS,
        "max_stale_ms": LEADLAG_MAX_AGE_NS // MS,
        "hl_frame_interval_p50_ms": _num(hl_ms),
        "um_frame_interval_p50_ms": _num(um_ms),
        "sign_convention": LEADLAG_SIGN_CONVENTION,
        "caveat": LEADLAG_CAVEAT,
    }


# ---------------------------------------------------------------------------
# 7. traversals
# ---------------------------------------------------------------------------


def traversals_at(mid, spacing_bps: int) -> int:
    """Confirmed moves of at least `spacing_bps` alternating in direction.

    The classic zig-zag / turning-point scan. The state is the running extreme
    since the last confirmation; a traversal is confirmed at the first price that
    has moved `spacing_bps` away from that extreme, and that price becomes the new
    extreme — which is exact rather than an approximation, because every price
    between the extreme and the confirming one was inside the threshold by
    definition and so cannot be past the confirming price.

    Causal by construction. The confirming frame is the frame that counts it, no
    frame is counted from a later one, and the move still in progress when the
    session ends is not counted at all. Before the first confirmation both
    extremes are tracked from the opening quote and whichever fires first sets the
    direction; the two cannot fire on the same frame, since while neither has
    fired every seen price is inside the threshold of both.

    `mid` is a sequence of positive integers **in any fixed scale** — the test is
    `(extreme - px) * 10_000 >= spacing_bps * extreme`, so a factor common to both
    sides cancels. That is what lets the caller pass the exact integer
    `bid_px + ask_px` rather than a float mid, and it is not a detail: on a coin
    like PUMP (0.0018 quoted on a 1e-6 tick) a whole-bps threshold lands exactly on
    a tick boundary all day, and in float64 that tie falls the wrong way. Python
    ints and not `int64`, because `spacing_bps * extreme` at BTC scale overflows.

    The threshold is measured against the extreme the move started from, so it is
    asymmetric in price: a 10bps rise followed by a fall back to exactly where it
    started is 9.99bps of the high, and not a traversal.
    """
    values = mid.tolist() if isinstance(mid, np.ndarray) else mid
    if len(values) < 2 or spacing_bps <= 0:
        return 0
    count = 0
    trend = 0
    high = low = values[0]
    for px in values[1:]:
        if trend > 0:
            if px > high:
                high = px
            elif (high - px) * 10_000 >= spacing_bps * high:
                count += 1
                trend = -1
                low = px
        elif trend < 0:
            if px < low:
                low = px
            elif (px - low) * 10_000 >= spacing_bps * low:
                count += 1
                trend = 1
                high = px
        else:
            if px > high:
                high = px
            elif px < low:
                low = px
            if (high - px) * 10_000 >= spacing_bps * high:
                count += 1
                trend = -1
                low = px
            elif (px - low) * 10_000 >= spacing_bps * low:
                count += 1
                trend = 1
                high = px
    return count


def round_trips_at(mid, spacing_bps: int) -> int:
    """Completed round trips: non-overlapping pairs of consecutive traversals.

    Two consecutive traversals — down then up, or up then down — are exactly what
    a one-rung grid at that spacing needs to earn its rung once: the price reaches
    the rung, and then reaches the price one spacing away. Pairing them without
    overlap is what makes the count a number of *excursions*: seven alternating
    traversals are three round trips and one open position, not six.
    """
    return traversals_at(mid, spacing_bps) // 2


@dataclass
class Traversal:
    """One session's traversal accumulators. Every field is additive.

    Per session and never per concatenation, for the reason the price runs are:
    joining two days puts a jump between the last quote of one and the first of
    the next, and that jump is movement no market made. `path_bps` and `net_bps`
    are already bps of their own session's opening mid, which is what makes adding
    them across days meaningful; `net_bps` stays signed so two days that undid
    each other do not pool into a walk.
    """

    traversals: dict = field(
        default_factory=lambda: dict.fromkeys(TRAVERSAL_SPACINGS_BPS, 0)
    )
    round_trips: dict = field(
        default_factory=lambda: dict.fromkeys(TRAVERSAL_SPACINGS_BPS, 0)
    )
    #: Per spacing, the round trips of each whole `TRAVERSAL_BLOCK_NS` block of
    #: the session, in order — what the published rate is an average of. A list
    #: because pooling two days is one longer list of hours, and because these
    #: deliberately do NOT sum to `round_trips`: the traversal spanning a block
    #: boundary belongs to no block. They are the rate's dispersion, not its
    #: decomposition.
    hour_blocks: dict = field(
        default_factory=lambda: {s: [] for s in TRAVERSAL_SPACINGS_BPS}
    )
    path_bps: float = 0.0
    net_bps: float = 0.0
    seconds: float = 0.0
    frames: int = 0


def traversal_of(series: BboSeries, spacings: Sequence[int] = TRAVERSAL_SPACINGS_BPS) -> Traversal:
    """How far the mid actually walked, and how often it came back.

    **Why this metric exists.** The 2026-07-30 grid-geometry sweep put the
    touch-capture margin on Hyperliquid at about 0.3bp — at the touch, what a
    maker can capture is gone by the time the fee and the adverse fill are paid.
    The only grid shape that came out of that sweep with a profitable cell earns
    SPACING: rungs several bps apart. A rung several bps wide is not paid by a
    narrow book sitting still; it is paid only when the price WALKS down through
    the rung and back up through the one above it.

    Metrics 1–6 cannot see that. Every one of them is a shape-of-the-book
    measurement — the spread, its behaviour under volatility, the queue at the
    touch, the depth at the rungs, who leads whom — and a coin can win all six
    while never going anywhere. VVV was the reason to suspect that: the best
    microstructure on the shortlist and not one profitable cell in the sweep, and
    "its recorded days did not move" was the obvious candidate explanation.

    **It is not the explanation, and this metric is how that was settled.** Run
    over VVV's own recordings, VVV oscillates as much as the shortlist's middle:
    18.5 round trips per hour at 10bps on 20260730 — 4th of ten coin-days that
    day, above HYPE, JTO, INJ, NEAR, SUI and CRV — and 22.0 on the 20260729
    fragment, at the top of that day's group bar KAITO. Whatever cost VVV the
    sweep, a still price path was not it. Do not re-derive the old story from the
    fact that this metric exists; it exists because the story was checkable, and
    it came out false.

    Three numbers, all from the `bbo` mid and all causal:

    * `traversals` / `round_trips` per spacing — `traversals_at`,
      `round_trips_at`. **This is the part that ranks coins**: it converges under
      resampling, so it measures the path and not the recording.
    * `path_bps` — every `|dmid|` summed, in bps of the session's opening mid.
      The first-order variation of a *sampled* path, which grows with the sampling
      rate rather than converging to anything (measured: VVV 20260730 LOCF-resampled
      4s -> 2s -> 1s -> 500ms -> raw gives 31936 -> 38735 -> 47325 -> 65390 ->
      90183 bps, still rising at the raw feed, while round trips at 30bps settle at
      75-76 from 1s down). Since `bbo` frame rates differ 5x across the shortlist,
      it is a within-row diagnostic and NOT a cross-coin number; `frames_per_hour`
      is published beside it so that is visible.
    * `net_bps` — signed `close - open` in the same unit. `path_bps / |net_bps|`
      is a pure ratio of price sums, and it is dominated by its denominator: it
      ranks sessions by how little the price drifted, not by how much it
      oscillated. Over the ten coin-days of 20260730 it correlates +0.79 with
      1/|net_bps| and -0.13 with the round-trip rate — the richest oscillator of
      that sample ranked 5th on it, below the poorest. Read within one row as "did
      this session go anywhere"; never across rows.

    The mid is the exact integer `bid_px + ask_px`, i.e. twice the scaled mid; the
    factor two cancels out of both the threshold test and the ratio. A frame
    missing a side arrives as a zero price and is dropped rather than halving the
    mid — an `l2Book` side can empty on a thin coin, and a 5000bps round trip that
    never happened is worse than a missing frame.
    """
    usable = (series.bid_px > 0) & (series.ask_px > 0)
    mid = series.bid_px[usable] + series.ask_px[usable]
    out = Traversal(
        traversals=dict.fromkeys(spacings, 0),
        round_trips=dict.fromkeys(spacings, 0),
        hour_blocks={s: [] for s in spacings},
        frames=int(mid.size),
    )
    if mid.size < 2:
        return out
    values = mid.tolist()
    for spacing in spacings:
        count = traversals_at(values, spacing)
        out.traversals[spacing] = count
        out.round_trips[spacing] = count // 2
    ts = series.ts[usable]
    out.seconds = (int(ts[-1]) - int(ts[0])) / SEC
    # Whole blocks only, and the trailing part-hour is dropped rather than
    # divided: a rate published beside its own spread must not have a short block
    # in that spread pretending to be a quiet one.
    t0, t1 = int(ts[0]), int(ts[-1])
    n_blocks = (t1 - t0) // TRAVERSAL_BLOCK_NS
    if n_blocks:
        edges = np.searchsorted(
            ts, [t0 + b * TRAVERSAL_BLOCK_NS for b in range(int(n_blocks) + 1)]
        )
        for spacing in spacings:
            out.hour_blocks[spacing] = [
                traversals_at(values[int(edges[b]):int(edges[b + 1])], spacing) // 2
                for b in range(int(n_blocks))
            ]
    opening = values[0]
    out.path_bps = int(np.abs(np.diff(mid)).sum()) / opening * 1e4
    out.net_bps = (values[-1] - opening) / opening * 1e4
    return out


def merge_traversals(parts: Sequence[Traversal]) -> Traversal:
    """Several sessions as one. Addition, term by term — see `Traversal`."""
    parts = list(parts)
    if not parts:
        return Traversal()
    out = Traversal(traversals={}, round_trips={}, hour_blocks={})
    for part in parts:
        for spacing, count in part.traversals.items():
            out.traversals[spacing] = out.traversals.get(spacing, 0) + count
        for spacing, count in part.round_trips.items():
            out.round_trips[spacing] = out.round_trips.get(spacing, 0) + count
        for spacing, blocks in part.hour_blocks.items():
            out.hour_blocks.setdefault(spacing, []).extend(blocks)
        out.path_bps += part.path_bps
        out.net_bps += part.net_bps
        out.seconds += part.seconds
        out.frames += part.frames
    return out


def summarize_traversal(traversal: Traversal, tick_bps: Optional[float] = None) -> dict:
    """The row. Per session and per hour, because a partial day is the norm here.

    The 2026-07-29 recording began at 21:00 UTC, and a coin measured over three
    hours has three hours' worth of round trips. Only the per-hour column compares
    it with a coin measured over a whole day — and that is exactly why the rate
    ships with the spread of the hours it averages. Measured on whole days of the
    shortlist, one coin-day's own 3h blocks span 2.0-4.7x in round trips per hour
    (HYPE 20260730 at 30bps: 1.00-4.67 against a whole-day 2.29), which is as wide
    as the cross-coin spread this tool exists to resolve. A bare rate off a
    fragment would look like a measurement and rank like a coin flip;
    `round_trips_per_hour_min`/`_max` are the same rate over each whole hour, and
    they are null below `MIN_TRAVERSAL_BLOCKS` hours rather than computed from a
    pair.

    `tick_bps` is the coin's measured tick as bps of its own mid — from the `tick`
    block of the same row. It is what makes the ladder comparable across coins:
    the mid moves in half-ticks, so a spacing worth under
    `MIN_SPACING_MID_STEPS` mid steps counts the smallest wiggle the book can
    print (PUMP: 5bps is 1.9 steps). Absent, the row says `None` rather than
    implying the rungs are fine.
    """
    hours = traversal.seconds / 3600.0
    mid_step_bps = tick_bps / 2.0 if tick_bps else None
    by_spacing = {}
    for spacing in sorted(traversal.traversals):
        round_trips = traversal.round_trips.get(spacing, 0)
        gross = float(round_trips * spacing)
        blocks = traversal.hour_blocks.get(spacing) or []
        ranged = len(blocks) >= MIN_TRAVERSAL_BLOCKS
        steps = None if mid_step_bps is None else spacing / mid_step_bps
        by_spacing[str(spacing)] = {
            "traversals": int(traversal.traversals[spacing]),
            "round_trips": int(round_trips),
            "round_trips_per_hour": _num(round_trips / hours) if hours > 0 else None,
            "round_trips_per_hour_min": _num(min(blocks)) if ranged else None,
            "round_trips_per_hour_max": _num(max(blocks)) if ranged else None,
            "gross_capture_potential_bps": gross,
            "gross_capture_potential_bps_per_hour": _num(gross / hours) if hours > 0 else None,
            "mid_steps": _num(steps),
            "at_flicker_floor": None if steps is None else bool(steps < MIN_SPACING_MID_STEPS),
        }
    return {
        "frames": traversal.frames,
        "frames_per_hour": _num(traversal.frames / hours) if hours > 0 else None,
        "seconds": _num(traversal.seconds),
        "hours": _num(hours),
        "hour_blocks": max((len(b) for b in traversal.hour_blocks.values()), default=0),
        "mid_step_bps": _num(mid_step_bps),
        "path_bps": _num(traversal.path_bps),
        "net_bps": _num(traversal.net_bps),
        "path_efficiency": (
            _num(traversal.path_bps / abs(traversal.net_bps)) if traversal.net_bps else None
        ),
        "by_spacing": by_spacing,
        "caveat": TRAVERSAL_CAVEAT,
    }


# ---------------------------------------------------------------------------
# reading a coin-day
# ---------------------------------------------------------------------------


@dataclass
class DaySamples:
    """Everything one coin-day contributes to a metric row.

    Deliberately observations rather than results: the pooled row is these
    concatenated and summarized once, which is the metric over several days
    rather than an average of several days' metrics. The two differ whenever the
    days are of different lengths, which they always are.
    """

    day: str
    counts: dict = field(default_factory=lambda: dict.fromkeys(COUNT_KEYS, 0))
    warnings: list = field(default_factory=list)
    gaps: dict = field(default_factory=dict)
    bbo: BboSeries = field(default_factory=lambda: BboBuilder().finish())
    fast: BookSeries = field(default_factory=lambda: BookBuilder().finish())
    slow: BookSeries = field(default_factory=lambda: BookBuilder().finish())
    windows: Optional[VolWindows] = None
    #: The `activeAssetCtx` rate samples. Concatenates across days like the
    #: quote series does: the settled rate of the hour that straddles midnight
    #: is only recoverable if both days' samples are in one array.
    funding: "FundingSeries" = field(default_factory=lambda: FundingBuilder().finish())
    #: Price-gap counts over every channel that prints a best price, summed. Kept
    #: per day so that pooling adds counts rather than concatenating series: a
    #: concatenation would count one gap across midnight that no feed ever showed.
    bid_tick_counts: Counter = field(default_factory=Counter)
    ask_tick_counts: Counter = field(default_factory=Counter)
    lifetime_bid: np.ndarray = field(default_factory=lambda: np.zeros(0, dtype=np.int64))
    lifetime_ask: np.ndarray = field(default_factory=lambda: np.zeros(0, dtype=np.int64))
    #: The runs that spanned a recording hole, kept so the report can say how much
    #: of the lifetime distribution it is not showing.
    lifetime_bid_dropped: np.ndarray = field(default_factory=lambda: np.zeros(0, dtype=np.int64))
    lifetime_ask_dropped: np.ndarray = field(default_factory=lambda: np.zeros(0, dtype=np.int64))
    um_ts: np.ndarray = field(default_factory=lambda: np.zeros(0, dtype=np.int64))
    um_mid: np.ndarray = field(default_factory=lambda: np.zeros(0))
    leadlag: Optional[np.ndarray] = None
    #: Counted per day for the same reason as the lifetimes: a traversal must not
    #: be made out of the jump from one day's last quote to the next day's first.
    traversal: Traversal = field(default_factory=Traversal)


def _hl_path(hl_dir, coin: str, day: str) -> Path:
    return Path(hl_dir) / f"{coin.lower()}_{day}.gz"


def _um_path(um_dir, symbol: str, day: str) -> Path:
    return Path(um_dir) / f"{symbol.lower()}_{day}.gz"


class _GapTracker:
    def __init__(self) -> None:
        self.previous: dict = {}
        self.gaps: dict = {}

    def observe(self, stream: str, ts: int) -> None:
        previous = self.previous.get(stream)
        self.previous[stream] = ts
        if previous is None:
            return
        delta = ts - previous
        if delta > GAP_REPORT_NS:
            count, worst = self.gaps.get(stream, (0, 0))
            self.gaps[stream] = (count + 1, max(worst, delta))


def _gaps_in(ts: np.ndarray):
    """`(count, worst_ns)` of the holes over `GAP_REPORT_NS` in a ts-sorted series.

    For a series assembled from several recordings this must run on the assembled
    one, not on each part: two sockets to one venue drop at uncorrelated times, so
    a hole one recording has is normally a hole the other filled, and per-part
    counting reports it anyway — inverting the reason the second recording is
    passed. `None` where there is nothing to report.
    """
    if ts.size < 2:
        return None
    step = np.diff(ts)
    big = step[step > GAP_REPORT_NS]
    return (int(big.size), int(big.max())) if big.size else None


def _read_hl(path: Path, coin: str, samples: DaySamples) -> None:
    want = coin.upper()
    bbo = BboBuilder()
    fast = BookBuilder()
    slow = BookBuilder()
    funding = FundingBuilder()
    tracker = _GapTracker()
    first = last = None
    try:
        for line in iter_gz_lines(path):
            try:
                ts, obj = parse_line(line)
            except (ValueError, json.JSONDecodeError):
                samples.counts["hl_unclassified"] += 1
                continue
            stream = classify(HYPERLIQUID, obj)
            if stream is None:
                samples.counts["hl_unclassified"] += 1
                continue
            data = obj.get("data") or {}
            book_channel = stream in ("bbo", "l2Book_fast", "l2Book_slow")
            if book_channel and str(data.get("coin", "")).upper() != want:
                # A per-symbol file carrying another coin's frames is an anomaly
                # worth counting, but it must not be counted as this coin's: the
                # channel counts are what the metrics were computed from, and the
                # frame-count warnings read them.
                samples.counts["hl_other_coin"] += 1
                continue
            samples.counts[stream] = samples.counts.get(stream, 0) + 1
            tracker.observe(stream, ts)
            first = ts if first is None else first
            last = ts
            if stream == "activeAssetCtx":
                # Counted and gap-tracked above, and until #40 the payload was
                # dropped right here — the funding rate the whole screen term
                # needs was already passing through this function.
                ctx = (data.get("ctx") or {}) if isinstance(data, dict) else {}
                try:
                    funding.add(ts, ctx["funding"], ctx["oraclePx"])
                except (KeyError, TypeError, ValueError):
                    samples.counts["hl_unclassified"] += 1
                continue
            if not book_channel:
                continue
            if stream == "bbo":
                quote = data.get("bbo") or []
                if len(quote) != 2 or quote[0] is None or quote[1] is None:
                    samples.counts["hl_unclassified"] += 1
                    continue
                bid, ask = quote
                bbo.add(
                    ts,
                    bid["px"],
                    bid.get("sz", 0.0),
                    bid.get("n", 0),
                    ask["px"],
                    ask.get("sz", 0.0),
                    ask.get("n", 0),
                )
            else:
                levels = data.get("levels") or [[], []]
                builder = fast if stream == "l2Book_fast" else slow
                builder.add(
                    ts,
                    [(lv["px"], lv["sz"]) for lv in levels[0]],
                    [(lv["px"], lv["sz"]) for lv in levels[1]],
                )
    except TruncatedRecording as error:
        samples.warnings.append(
            f"{path.name}: gzip ended mid-member ({error}); metrics cover only "
            f"what decoded"
        )
    samples.bbo = bbo.finish()
    samples.fast = fast.finish()
    samples.slow = slow.finish()
    samples.funding = funding.finish()
    samples.gaps.update({f"hl.{k}": v for k, v in tracker.gaps.items()})
    samples.counts["hl_first_local_ts"] = first
    samples.counts["hl_last_local_ts"] = last


def _read_um_one(path: Path, symbol: str, samples: DaySamples):
    """`(ts, u, mid)` arrays of one recording's `@bookTicker` frames.

    The substring prefilter is `build_dataset.iter_book_ticker`'s: the bulk of a
    USD-M recording is multi-kilobyte `@depth@0ms` frames, and parsing them to
    learn they are not `bookTicker` is most of the runtime of a busy day.
    """
    want = symbol.upper()
    ts_out = array("q")
    u_out = array("q")
    mid_out = array("d")
    try:
        for line in iter_gz_lines(path):
            if b"bookTicker" not in line:
                continue
            try:
                ts, obj = parse_line(line)
            except (ValueError, json.JSONDecodeError):
                continue
            if classify(BINANCE, obj) != "bookTicker":
                continue
            data = obj["data"]
            if str(data.get("s", "")).upper() != want:
                continue
            update_id = data.get("u")
            if update_id is None:
                samples.counts["um_frames_without_update_id"] += 1
                update_id = NO_UPDATE_ID
            ts_out.append(ts)
            u_out.append(int(update_id))
            mid_out.append((float(data["b"]) + float(data["a"])) / 2.0)
    except TruncatedRecording as error:
        samples.warnings.append(
            f"{path.name}: gzip ended mid-member ({error}); metrics cover only "
            f"what decoded"
        )
    # Gaps are deliberately *not* measured here. They belong to the unioned
    # series the metrics are computed from — see `_gaps_in` at its call site.
    return (
        np.frombuffer(ts_out, dtype=np.int64).copy(),
        np.frombuffer(u_out, dtype=np.int64).copy(),
        np.frombuffer(mid_out, dtype=np.float64).copy(),
    )


def _union_um(parts, samples: DaySamples):
    """Union two recordings of one Binance symbol on the venue's `u`.

    `build_dataset.build_signal_union` states the argument: two sockets to one
    venue drop at uncorrelated times, the same update arrives at different
    instants so timestamps cannot match it, prices cannot either because a quiet
    market repeats them, and `u` is the only thing that identifies one update
    across both. The earliest arrival wins, because the recording that was up is
    the one that has it.

    Where that function refuses — a `u` describing two different books, a frame
    with no `u` — this one counts and carries on. It produces a measurement, not
    a dataset to trade off, and a coin whose metrics are missing entirely because
    of one odd frame is the worse failure. A frame with no `u` is **kept**: it
    cannot be matched, so it cannot be deduplicated, and they all carry the same
    `NO_UPDATE_ID` sentinel — deduplicating on that would treat the whole lot as
    one update and keep exactly one of them.
    """
    ts = np.concatenate([p[0] for p in parts]) if parts else np.zeros(0, dtype=np.int64)
    u = np.concatenate([p[1] for p in parts]) if parts else np.zeros(0, dtype=np.int64)
    mid = np.concatenate([p[2] for p in parts]) if parts else np.zeros(0)
    if len(parts) < 2 or ts.size == 0:
        order = np.argsort(ts, kind="stable")
        return ts[order], mid[order]

    primary = parts[0][1]
    matchable = np.flatnonzero(u != NO_UPDATE_ID)
    order = matchable[np.lexsort((ts[matchable], u[matchable]))]
    u_sorted, mid_sorted = u[order], mid[order]
    first = np.concatenate(([True], u_sorted[1:] != u_sorted[:-1])) if order.size else order
    lo = np.flatnonzero(first)
    if lo.size:
        lowest = np.minimum.reduceat(mid_sorted, lo)
        highest = np.maximum.reduceat(mid_sorted, lo)
        samples.counts["um_conflicting_update_ids"] = int((lowest != highest).sum())
    keep = np.concatenate((order[first], np.flatnonzero(u == NO_UPDATE_ID)))
    ts_keep, mid_keep = ts[keep], mid[keep]
    inner = np.argsort(ts_keep, kind="stable")
    # How much the second recording added: update ids the primary never saw.
    matched = u[keep]
    recovered = int(
        np.isin(matched[matched != NO_UPDATE_ID], primary, invert=True).sum()
    )
    samples.counts["um_recovered_by_second_recording"] = recovered
    return ts_keep[inner], mid_keep[inner]


def read_day(hl_dir, coin, um_dirs, um_symbol, day) -> DaySamples:
    """Read one coin-day and derive everything a metric row needs from it.

    A missing file is a warning, not an error: a re-ranking over sixteen coins
    and fourteen days should report the holes it found, not stop at the first.
    """
    samples = DaySamples(day=day)
    hl_path = _hl_path(hl_dir, coin, day)
    if hl_path.exists():
        _read_hl(hl_path, coin, samples)
    else:
        samples.warnings.append(f"no file {hl_path}")

    parts = []
    for um_dir in um_dirs:
        path = _um_path(um_dir, um_symbol, day)
        if path.exists():
            parts.append(_read_um_one(path, um_symbol, samples))
        else:
            samples.warnings.append(f"no file {path}")
    samples.um_ts, samples.um_mid = _union_um(parts, samples)
    samples.counts["um_bookTicker"] = int(samples.um_ts.size)
    um_gaps = _gaps_in(samples.um_ts)
    if um_gaps is not None:
        samples.gaps["um.bookTicker"] = um_gaps

    # Every channel carrying a best price is evidence about the price grid, and
    # §8.6 makes the tick the load-bearing reconstruction of the whole exercise.
    # Counted per channel and summed rather than interleaved: two feeds of
    # different latency merged by timestamp invent transitions neither showed.
    for series, bid_px, ask_px in (
        (samples.bbo, samples.bbo.bid_px, samples.bbo.ask_px),
        (samples.fast, samples.fast.best_bid_px, samples.fast.best_ask_px),
        (samples.slow, samples.slow.best_bid_px, samples.slow.best_ask_px),
    ):
        if len(series):
            samples.bid_tick_counts.update(price_delta_counts(bid_px))
            samples.ask_tick_counts.update(price_delta_counts(ask_px))

    samples.windows = vol_windows(samples.bbo)
    samples.lifetime_bid, samples.lifetime_bid_dropped = price_run_lifetimes(
        samples.bbo.ts, samples.bbo.bid_px, LIFETIME_MAX_GAP_NS
    )
    samples.lifetime_ask, samples.lifetime_ask_dropped = price_run_lifetimes(
        samples.bbo.ts, samples.bbo.ask_px, LIFETIME_MAX_GAP_NS
    )
    samples.leadlag = leadlag_accum(
        samples.bbo.ts, samples.bbo.mid, samples.um_ts, samples.um_mid
    )
    samples.traversal = traversal_of(samples.bbo)
    samples.warnings.extend(_channel_warnings(samples))
    return samples


#: Below this share of a UTC day, the coverage is announced. A day the collector
#: started or stopped inside is legal and common — the 2026-07-29 run began at
#: 21:00 UTC — but three hours of one coin is not comparable with a full day of
#: another, and nothing else in a metric row makes the difference visible.
FULL_DAY_FRACTION = 0.95


def _channel_warnings(samples: DaySamples) -> list:
    out = []
    first = samples.counts.get("hl_first_local_ts")
    last = samples.counts.get("hl_last_local_ts")
    if first and last:
        covered = (last - first) / (24 * 3600 * SEC)
        if covered < FULL_DAY_FRACTION:
            out.append(
                f"the recording covers {covered * 24:.1f}h of the day "
                f"({covered * 100:.0f}%); these numbers are not comparable with a "
                f"coin measured over a whole day"
            )
    for channel, floor in (("bbo", 1000), ("l2Book_fast", 100), ("um_bookTicker", 1000)):
        count = samples.counts.get(channel, 0)
        if count == 0:
            out.append(f"{channel}: no frames at all — every metric from it is null")
        elif count < floor:
            out.append(f"{channel}: only {count} frames (expected at least {floor} for a day)")
    if samples.counts.get("l2Book_slow", 0) == 0:
        out.append("l2Book_slow: no frames — the d30 cross-check is unavailable")
    for stream, (count, worst) in sorted(samples.gaps.items()):
        out.append(f"{stream}: {count} gap(s) over 60s, worst {worst / SEC:.0f}s")
    if samples.counts.get("um_conflicting_update_ids", 0):
        out.append(
            f"{samples.counts['um_conflicting_update_ids']} update id(s) describe "
            f"two different books across the two recordings; the earliest arrival "
            f"was kept"
        )
    if samples.counts.get("um_frames_without_update_id", 0):
        out.append(
            f"{samples.counts['um_frames_without_update_id']} bookTicker frame(s) "
            f"carry no update id and could not be deduplicated"
        )
    return out


# ---------------------------------------------------------------------------
# pooling
# ---------------------------------------------------------------------------


def _concat_bbo(parts) -> BboSeries:
    fields = ("ts", "bid_px", "ask_px", "bid_sz", "ask_sz", "bid_n", "ask_n", "mid",
              "spread", "spread_bps", "weight")
    joined = {name: np.concatenate([getattr(p, name) for p in parts]) for name in fields}
    return BboSeries(**joined)


def _concat_funding(parts) -> FundingSeries:
    """Rate samples concatenate, like the quotes.

    Unlike the frame weights and the price-run lifetimes, nothing here is
    per-day: the hour that straddles midnight settles from the last sample
    before it, whichever day's file that sample came out of, so splitting the
    series by day would lose exactly one boundary per day boundary.
    """
    return FundingSeries(
        ts=np.concatenate([p.ts for p in parts]),
        rate=np.concatenate([p.rate for p in parts]),
        oracle_px=np.concatenate([p.oracle_px for p in parts]),
    )


def _concat_books(parts) -> BookSeries:
    offset = 0
    rows = []
    for p in parts:
        rows.append(p.row + offset)
        offset += p.ts.size
    return BookSeries(
        ts=np.concatenate([p.ts for p in parts]),
        best_bid=np.concatenate([p.best_bid for p in parts]),
        best_ask=np.concatenate([p.best_ask for p in parts]),
        best_bid_px=np.concatenate([p.best_bid_px for p in parts]),
        best_ask_px=np.concatenate([p.best_ask_px for p in parts]),
        deepest_bid=np.concatenate([p.deepest_bid for p in parts]),
        deepest_ask=np.concatenate([p.deepest_ask for p in parts]),
        row=np.concatenate(rows) if rows else np.zeros(0, dtype=np.int64),
        is_ask=np.concatenate([p.is_ask for p in parts]),
        px=np.concatenate([p.px for p in parts]),
        notional=np.concatenate([p.notional for p in parts]),
    )


def _concat_windows(parts) -> VolWindows:
    offset = 0
    frame_windows = []
    for p in parts:
        frame_windows.append(np.where(p.frame_window >= 0, p.frame_window + offset, -1))
        offset += p.vol.size
    return VolWindows(
        start_ts=np.concatenate([p.start_ts for p in parts]),
        vol=np.concatenate([p.vol for p in parts]),
        spread_bps=np.concatenate([p.spread_bps for p in parts]),
        frame_window=np.concatenate(frame_windows) if frame_windows else np.zeros(0, dtype=np.int64),
        dropped_windows=sum(p.dropped_windows for p in parts),
    )


def pool_days(days: Sequence[DaySamples]) -> DaySamples:
    """Every day's observations as one sample set.

    Per-day quantities that must not bridge midnight are already computed:
    frame weights (the last frame of a day has none), price-run lifetimes, the
    lead-lag sums, which are added per lag, and the traversal accumulators, which
    are added term by term. Everything else concatenates.
    """
    pooled = DaySamples(day="pooled")
    for key in COUNT_KEYS:
        pooled.counts[key] = sum(d.counts.get(key, 0) or 0 for d in days)
    firsts = [d.counts.get("hl_first_local_ts") for d in days if d.counts.get("hl_first_local_ts")]
    lasts = [d.counts.get("hl_last_local_ts") for d in days if d.counts.get("hl_last_local_ts")]
    pooled.counts["hl_first_local_ts"] = min(firsts) if firsts else None
    pooled.counts["hl_last_local_ts"] = max(lasts) if lasts else None
    for d in days:
        pooled.warnings.extend(f"{d.day}: {w}" for w in d.warnings)
    for d in days:
        for stream, (count, worst) in d.gaps.items():
            have_count, have_worst = pooled.gaps.get(stream, (0, 0))
            pooled.gaps[stream] = (have_count + count, max(have_worst, worst))
    pooled.funding = _concat_funding([d.funding for d in days])
    pooled.bbo = _concat_bbo([d.bbo for d in days])
    pooled.fast = _concat_books([d.fast for d in days])
    pooled.slow = _concat_books([d.slow for d in days])
    pooled.windows = _concat_windows([d.windows for d in days])
    for d in days:
        pooled.bid_tick_counts.update(d.bid_tick_counts)
        pooled.ask_tick_counts.update(d.ask_tick_counts)
    pooled.lifetime_bid = np.concatenate([d.lifetime_bid for d in days])
    pooled.lifetime_ask = np.concatenate([d.lifetime_ask for d in days])
    pooled.lifetime_bid_dropped = np.concatenate([d.lifetime_bid_dropped for d in days])
    pooled.lifetime_ask_dropped = np.concatenate([d.lifetime_ask_dropped for d in days])
    pooled.um_ts = np.concatenate([d.um_ts for d in days])
    pooled.um_mid = np.concatenate([d.um_mid for d in days])
    pooled.leadlag = sum(d.leadlag for d in days)
    pooled.traversal = merge_traversals([d.traversal for d in days])
    return pooled


# ---------------------------------------------------------------------------
# the report
# ---------------------------------------------------------------------------


#: Above this share of the weighted session spent in another price decade, one
#: tick per row is the wrong model and the tick arithmetic — including §9.3's
#: `frac_time_ge_3_ticks` gate — is wrong by up to that share. A pooled row over
#: fourteen days is where this becomes reachable: PUMP at 0.00186 needs a 46%
#: drawdown to cross 0.001.
DECADE_SPLIT_WARN = 0.01


def summarize(samples: DaySamples, screen: Optional[dict] = None) -> dict:
    tick = summarize_tick(
        samples.bid_tick_counts,
        samples.ask_tick_counts,
        samples.bbo.mid,
        weight=samples.bbo.weight,
    )
    used = tick["used"]
    tick_scaled = None if used is None else int(round(used * PX_SCALE))
    first = samples.counts.get("hl_first_local_ts")
    last = samples.counts.get("hl_last_local_ts")
    warnings = list(samples.warnings)
    split = tick["frac_time_off_median_decade"]
    if used is not None and split is not None and split > DECADE_SPLIT_WARN:
        warnings.append(
            f"the price crossed a decade boundary: {split * 100:.0f}% of the "
            f"weighted session was quoted outside the median price's decade, where "
            f"the tick is not {used:.3g}; every _ticks number in this row, "
            f"frac_time_ge_3_ticks included, is wrong by up to that share"
        )
    screen = dict(screen or {})
    spread = summarize_spread(samples.bbo, tick_scaled)
    # The gate reads the p25 of THIS row's own pooled spread unless the operator
    # named one: the screen rule is about the spread the coin actually quoted,
    # and a Sunday spot reading of it moved 12.56 -> 6.33 bps in five minutes.
    screen.setdefault("spread_p25_bps", spread["p25_bps"])
    funding_block = summarize_funding(samples.funding, **screen)
    warnings.extend(funding_block["warnings"])

    return {
        "day": samples.day,
        "window": {
            "first_local_ts": first,
            "last_local_ts": last,
            "seconds": None if not (first and last) else (last - first) / SEC,
        },
        "counts": {k: samples.counts.get(k) for k in ("hl_first_local_ts", "hl_last_local_ts")}
        | {k: samples.counts.get(k, 0) for k in COUNT_KEYS},
        "tick": tick,
        "spread": spread,
        "funding": funding_block,
        "conditional_spread": conditional_spread_curve(samples.windows, samples.bbo),
        "touch": summarize_touch(
            samples.bbo,
            (
                (samples.lifetime_bid, samples.lifetime_bid_dropped),
                (samples.lifetime_ask, samples.lifetime_ask_dropped),
            ),
        ),
        "depth": summarize_depth(samples.fast, samples.slow),
        # The tick as a share of this coin's own mid is what tells the reader
        # which rungs of the traversal ladder are above the book's flicker floor.
        "traversal": summarize_traversal(
            samples.traversal,
            tick_bps=(
                None
                if not used or not tick["median_px"]
                else used / tick["median_px"] * 1e4
            ),
        ),
        "leadlag": summarize_leadlag(
            samples.leadlag,
            cadence_ms=(
                _median_interval_ms(samples.bbo.ts),
                _median_interval_ms(samples.um_ts),
            ),
        ),
        "warnings": warnings,
    }


def build_report(hl_dir, coin, um_dirs, um_symbol, days, screen=None) -> dict:
    read = [read_day(hl_dir, coin, um_dirs, um_symbol, day) for day in days]
    rows = [summarize(d, screen) for d in read]
    if len(read) > 1:
        rows.append(summarize(pool_days(read), screen))
    return _plain(
        {
            "schema": SCHEMA,
            "coin": coin.upper(),
            "um_symbol": um_symbol.upper(),
            "hl_dir": str(hl_dir),
            "um_dirs": [str(d) for d in um_dirs],
            "days": list(days),
            "conventions": {
                "leadlag_sign": LEADLAG_SIGN_CONVENTION,
                "spread_weighting": (
                    "each bbo frame weighs the time until the next one, capped at "
                    f"{SPREAD_WEIGHT_CAP_NS // SEC}s so a feed outage cannot outvote "
                    "the day; the last frame of a day has no weight"
                ),
                "tick": (
                    "the mode of the gaps between consecutive distinct best bids, "
                    "over bbo and both l2Book cadences — counted per channel and "
                    "summed, since merging feeds of different latency by timestamp "
                    "invents transitions neither showed; "
                    "the formula is the 5-significant-figure term 10^(floor(log10 "
                    "px)-4) alone, since szDecimals is not in a recording. Where "
                    "they differ by more than 5% `mismatch` is set and the measured "
                    "tick is what every other number here uses. One tick per row "
                    "assumes the price stayed inside one power of ten, since "
                    "Hyperliquid's tick is a step function of the price: "
                    "`frac_time_off_median_decade` is the share of the weighted "
                    "session where that assumption fails, and every _ticks number "
                    "in the row is wrong by up to that share"
                ),
                "realized_vol": (
                    f"std of log returns of the mid on a {VOL_GRID_NS // SEC}s LOCF "
                    f"grid inside each {VOL_WINDOW_NS // SEC}s window; a sample whose "
                    f"newest quote is over {LOCF_MAX_AGE_NS // SEC}s old is invalid and "
                    f"a window with fewer than {MIN_VOL_SAMPLES} valid samples is dropped"
                ),
                "depth": (
                    "per l2Book fast snapshot, the thinner side's notional inside "
                    "+-10/20/30bps of the mid, median over snapshots; "
                    "`truncated_frac_*` is the share of snapshots whose deepest "
                    "recorded level is still inside the window, where the value is "
                    "a lower bound"
                ),
                "touch": (
                    "frame-weighted, not time-weighted: what a small order joining "
                    "the touch is up against on arrival. `touch_price_lifetime` is "
                    "the time until the best price changes, not queue survival; a "
                    "run spanning a hole longer than "
                    f"{LIFETIME_MAX_GAP_NS // SEC}s cannot be measured and is "
                    "reported as `runs_dropped_to_gaps` rather than dropped "
                    "silently. Hyperliquid publishes `bbo` on change, so silence "
                    "shorter than that is a quote that stood, not an absent feed"
                ),
                "traversal": (
                    "from the bbo mid, held as the exact integer bid+ask. A "
                    "traversal is a move of at least the spacing away from the "
                    "running extreme, counted by the frame that confirms it and "
                    "never by a later one; it is measured against the price the "
                    "move started from, so the threshold is asymmetric in price and "
                    "a 10bps rise followed by a fall back to where it started is "
                    "9.99bps and not a traversal. Two consecutive opposite "
                    "traversals are one round trip — what a one-rung grid at that "
                    "spacing needs to earn the rung once — so seven alternating "
                    "traversals are three round trips and one open position. "
                    "Counted per session and added across days, never over the "
                    "concatenation, since a jump from one day's last quote to the "
                    "next day's first is movement no market made. "
                    "`round_trips_per_hour` is over the whole recording and "
                    "`_min`/`_max` are the same rate over each whole hour of it, "
                    f"null under {MIN_TRAVERSAL_BLOCKS} hours; the blocks do not "
                    "sum to the session count, a traversal spanning a boundary "
                    "belonging to no block, and they are the rate's dispersion "
                    "rather than its decomposition. `mid_steps` is the spacing in "
                    "half-ticks of the coin's own measured tick and "
                    "`at_flicker_floor` marks the rungs under "
                    f"{MIN_SPACING_MID_STEPS} of them, where the count is a "
                    "property of the tick as much as of the path. `path_bps` and "
                    "`net_bps` are bps of their own session's opening mid, which is "
                    "what makes them additive, and `net_bps` keeps its sign; "
                    "`path_bps` is the first-order variation of a sampled path and "
                    "grows with `frames_per_hour` instead of converging, and "
                    "`path_efficiency` (path over |net|) is dominated by its "
                    "denominator — it ranks by absence of drift, not by "
                    "oscillation. Neither is a cross-coin number; the round trips "
                    "are"
                ),
                "pooled_row": (
                    "the same metrics recomputed over every day's observations, not "
                    "an average of the per-day rows"
                ),
            },
            "rows": rows,
        }
    )


# ---------------------------------------------------------------------------
# human-readable output
# ---------------------------------------------------------------------------


def _cell(value, spec: str = "") -> str:
    if value is None or (isinstance(value, float) and not math.isfinite(value)):
        return "-"
    return format(value, spec) if spec else str(value)


def _table(header: Sequence[str], rows: Sequence[Sequence[str]], widths: Sequence[int]) -> list:
    out = ["  " + "".join(f"{h:<{w}}" for h, w in zip(header, widths))]
    for row in rows:
        out.append("  " + "".join(f"{c:<{w}}" for c, w in zip(row, widths)))
    return out


def render_text(report: dict) -> str:
    """The operator's view. One row per day, plus the pooled row."""
    lines = [
        f"rerank metrics  schema={report['schema']}  coin={report['coin']}  "
        f"um_symbol={report['um_symbol']}",
        f"  hl_dir={report['hl_dir']}",
    ]
    for d in report["um_dirs"]:
        lines.append(f"  um_dir={d}")
    lines.append(f"  {LEADLAG_SIGN_CONVENTION}")

    rows = report["rows"]
    lines.append("")
    lines.append("  tick and spread")
    lines += _table(
        ["day", "bbo", "seconds", "median_px", "tick", "src", "p50_bps", "p50_ticks",
         "t@1tick", "t>=3ticks"],
        [
            [
                r["day"],
                _cell(r["counts"]["bbo"]),
                _cell(r["window"]["seconds"], ".0f"),
                _cell(r["tick"]["median_px"], ".6g"),
                _cell(r["tick"]["used"], ".3g") + ("!" if r["tick"]["mismatch"] else ""),
                _cell(r["tick"]["used_source"])[:4],
                _cell(r["spread"]["p50_bps"], ".2f"),
                _cell(r["spread"]["p50_ticks"], ".2f"),
                _cell(r["spread"]["frac_time_at_1_tick"], ".3f"),
                _cell(r["spread"]["frac_time_ge_3_ticks"], ".3f"),
            ]
            for r in rows
        ],
        [11, 9, 9, 11, 10, 6, 9, 11, 9, 10],
    )

    lines.append("")
    lines.append("  conditional spread by realized-vol quintile (bps)")
    lines += _table(
        ["day", "q1", "q2", "q3", "q4", "q5", "q5/q1", "spearman", "windows", "dropped"],
        [
            [r["day"]]
            + [
                _cell((r["conditional_spread"]["p50_bps_by_vol_quintile"] or [None] * 5)[q], ".2f")
                for q in range(5)
            ]
            + [
                _cell(r["conditional_spread"]["ratio_q5_q1"], ".2f"),
                _cell(r["conditional_spread"]["spearman_vol_spread"], ".3f"),
                _cell(r["conditional_spread"]["windows"]),
                _cell(r["conditional_spread"]["dropped_windows"]),
            ]
            for r in rows
        ],
        [11, 8, 8, 8, 8, 8, 8, 10, 9, 8],
    )

    lines.append("")
    lines.append("  touch and depth (USD)")
    lines += _table(
        ["day", "n_bid", "usd_bid", "n_ask", "usd_ask", "life_p50s", "d10", "d20", "d30",
         "trunc30", "d30_slow"],
        [
            [
                r["day"],
                _cell(r["touch"]["bid"]["n_orders_p50"], ".0f"),
                _cell(r["touch"]["bid"]["usd_p50"], ".0f"),
                _cell(r["touch"]["ask"]["n_orders_p50"], ".0f"),
                _cell(r["touch"]["ask"]["usd_p50"], ".0f"),
                _cell(r["touch"]["touch_price_lifetime"]["bid"]["p50_seconds"], ".2f"),
                _cell(r["depth"]["d10"], ".0f"),
                _cell(r["depth"]["d20"], ".0f"),
                _cell(r["depth"]["d30"], ".0f"),
                _cell(r["depth"]["truncated_frac_d30"], ".2f"),
                _cell(r["depth"]["d30_slow"], ".0f"),
            ]
            for r in rows
        ],
        [11, 7, 10, 7, 10, 11, 10, 10, 10, 9, 10],
    )

    lines.append("")
    lines.append("  lead-lag Hyperliquid vs Binance USD-M")
    lines += _table(
        ["day", "lag_ms", "peak_corr", "corr@0", "cells", "hl_ms", "um_ms", "by quarter"],
        [
            [
                r["day"],
                _cell(r["leadlag"]["lag_ms"]),
                _cell(r["leadlag"]["peak_corr"], ".3f"),
                _cell(r["leadlag"]["corr_at_lag_0"], ".3f"),
                _cell(r["leadlag"]["cells"]),
                _cell(r["leadlag"]["hl_frame_interval_p50_ms"], ".0f"),
                _cell(r["leadlag"]["um_frame_interval_p50_ms"], ".0f"),
                " ".join(_cell(q) for q in r["leadlag"]["lag_ms_by_quarter"]),
            ]
            for r in rows
        ],
        [11, 8, 11, 9, 10, 8, 8, 30],
    )

    lines.append("")
    lines.append(
        "  traversals of the bbo mid — round trips at each grid spacing. "
        "`!` = the rung is under "
        f"{MIN_SPACING_MID_STEPS} mid steps of this coin's tick, so its count is "
        "not comparable with another coin's"
    )
    lines += _table(
        ["day", "hours", "rt@5", "rt@10", "rt@20", "rt@30",
         "rt/h@5", "rt/h@10", "rt/h@20", "rt/h@30"],
        [
            [r["day"], _cell(r["traversal"]["hours"], ".2f")]
            + [
                _cell(r["traversal"]["by_spacing"][str(s)]["round_trips"])
                + ("!" if r["traversal"]["by_spacing"][str(s)]["at_flicker_floor"] else "")
                for s in TRAVERSAL_SPACINGS_BPS
            ]
            + [
                _cell(r["traversal"]["by_spacing"][str(s)]["round_trips_per_hour"], ".1f")
                for s in TRAVERSAL_SPACINGS_BPS
            ]
            for r in rows
        ],
        [11, 7, 8, 8, 8, 8, 9, 9, 9, 9],
    )

    lines.append("")
    lines.append(
        "  how precise those rates are: the same rate over each whole hour of the "
        f"recording (lo-hi), and how many whole hours — under {MIN_TRAVERSAL_BLOCKS} "
        "there is no range and the rate above stands on its own"
    )
    lines += _table(
        ["day", "blocks", "rt/h@5", "rt/h@10", "rt/h@20", "rt/h@30"],
        [
            [r["day"], _cell(r["traversal"]["hour_blocks"])]
            + [
                "-"
                if r["traversal"]["by_spacing"][str(s)]["round_trips_per_hour_min"] is None
                else "{}-{}".format(
                    _cell(r["traversal"]["by_spacing"][str(s)]["round_trips_per_hour_min"], ".0f"),
                    _cell(r["traversal"]["by_spacing"][str(s)]["round_trips_per_hour_max"], ".0f"),
                )
                for s in TRAVERSAL_SPACINGS_BPS
            ]
            for r in rows
        ],
        [11, 7, 12, 12, 12, 12],
    )

    lines.append("")
    lines.append(
        "  grid arithmetic from those traversals (gross bps per session — a "
        "ceiling, not a forecast). path_bps and path/net are within-row "
        "diagnostics: both move with frames/h, neither ranks coins"
    )
    lines += _table(
        ["day", "gross@5", "gross@10", "gross@20", "gross@30", "path_bps", "net_bps",
         "path/net", "frames/h"],
        [
            [r["day"]]
            + [
                _cell(r["traversal"]["by_spacing"][str(s)]["gross_capture_potential_bps"], ".0f")
                for s in TRAVERSAL_SPACINGS_BPS
            ]
            + [
                _cell(r["traversal"]["path_bps"], ".0f"),
                _cell(r["traversal"]["net_bps"], ".0f"),
                _cell(r["traversal"]["path_efficiency"], ".1f"),
                _cell(r["traversal"]["frames_per_hour"], ".0f"),
            ]
            for r in rows
        ],
        [11, 10, 11, 11, 11, 10, 10, 10, 9],
    )

    lines.append("")
    lines.append(f"  caveat: {CONDITIONAL_CAVEAT}")
    lines.append(f"  caveat: {LEADLAG_CAVEAT}")
    lines.append(f"  caveat: {TRAVERSAL_CAVEAT}")
    lines.append("")
    lines.append("")
    lines.append("  funding (#40)")
    lines += _table(
        ["day", "hours", "unres", "median_bps_h", "p95_abs", "worst", "flips",
         "gross_bps", "BE_h@med", "BE_h@worst", "p_tail", "verdict"],
        [
            [
                r["day"],
                _cell(r["funding"]["hours_covered"]),
                _cell(r["funding"]["hours_unresolved"]),
                _cell(r["funding"]["rate_bps_per_hour"]["median"], ".4f"),
                _cell(r["funding"]["rate_bps_per_hour"]["p95_abs"], ".4f"),
                _cell(r["funding"]["tail"]["worst_hour_bps"], ".3f"),
                _cell(r["funding"]["rate_bps_per_hour"]["sign_flips"]),
                _cell(r["funding"]["gross_capture_bps"], ".3f"),
                _cell((r["funding"]["break_even_hold_hours"] or {}).get("at_median_rate"), ".2f"),
                _cell((r["funding"]["break_even_hold_hours"] or {}).get("at_worst_hour"), ".2f"),
                _cell(r["funding"]["tail"]["p_hour_rate_ge_gross"], ".5f"),
                r["funding"]["verdict"],
            ]
            for r in rows
        ],
        [10, 7, 7, 14, 10, 9, 7, 11, 11, 12, 9, 10],
    )
    lines.append(f"    {FUNDING_CAVEAT}")

    lines.append("")
    lines.append("  warnings")
    any_warning = False
    for r in rows:
        for w in r["warnings"]:
            any_warning = True
            lines.append(f"    {r['day']}: {w}")
    if not any_warning:
        lines.append("    none")
    return "\n".join(lines) + "\n"


# ---------------------------------------------------------------------------
# small helpers
# ---------------------------------------------------------------------------


def _num(value, digits: Optional[int] = None):
    """A plain float, or `None` where there is no number.

    NaN reaches JSON as the literal `NaN`, which is not JSON and which every
    strict reader rejects; a metric with no observations behind it is `null`.
    """
    if value is None:
        return None
    value = float(value)
    if not math.isfinite(value):
        return None
    return round(value, digits) if digits is not None else value


def _median_interval_ms(ts: np.ndarray) -> Optional[float]:
    """The median gap between frames, in milliseconds.

    Median rather than mean: a day holds a handful of reconnect-sized holes, and
    the mean of a cadence with those in it is not a cadence.
    """
    if ts.size < 2:
        return None
    return float(np.median(np.diff(ts))) / MS


def _median(values: np.ndarray) -> Optional[float]:
    values = np.asarray(values, dtype=np.float64)
    values = values[np.isfinite(values)]
    return None if values.size == 0 else float(np.median(values))


def _percentile(values: np.ndarray, q: float) -> Optional[float]:
    values = np.asarray(values, dtype=np.float64)
    values = values[np.isfinite(values)]
    return None if values.size == 0 else float(np.percentile(values, q))


def _plain(obj):
    """numpy scalars out, so `json.dump` cannot fail on a finished report."""
    if isinstance(obj, dict):
        return {k: _plain(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_plain(v) for v in obj]
    if isinstance(obj, np.integer):
        return int(obj)
    if isinstance(obj, np.floating):
        return _num(float(obj))
    if isinstance(obj, np.bool_):
        return bool(obj)
    if isinstance(obj, float) and not math.isfinite(obj):
        return None
    return obj


# ---------------------------------------------------------------------------
# entry point
# ---------------------------------------------------------------------------


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="rerank_metrics.py",
        description=(
            "Microstructure metrics over raw collector recordings (§9.2 of "
            "docs/research-symbol-selection.md)."
        ),
    )
    parser.add_argument("--hl-dir", required=True, metavar="DIR",
                       help="The Hyperliquid collector instance directory.")
    parser.add_argument("--coin", required=True, help="Hyperliquid coin, e.g. PUMP.")
    parser.add_argument(
        "--um-dir", dest="um_dirs", action="append", default=[], metavar="DIR",
        help="A Binance USD-M instance directory. Repeat (or add --um-dir-b) for a "
        "second recording of the same symbol; the two are unioned on the venue's "
        "update id.",
    )
    parser.add_argument("--um-dir-b", dest="um_dirs", action="append", metavar="DIR",
                       help="The second Binance USD-M recording. Same as repeating --um-dir.")
    parser.add_argument("--um-symbol", required=True, help="Binance USD-M symbol, e.g. PUMPUSDT.")
    parser.add_argument("--day", dest="days", action="append", default=[], metavar="YYYYMMDD",
                       required=True, help="A UTC day to measure. Repeat for several; "
                       "several days also produce a pooled row.")
    # The funding gate's inputs (#40). Every one of them is MEASURED per coin;
    # none has a default except the fee, which is measured too.
    #
    # --hold-mean-s / --hold-p95-s MUST come from a backtest_first.py run of THIS
    # coin at the geometry that passes its own request-budget gate. There is no
    # default and no fallback to another coin's hold: three separate "hold"
    # measurements already exist for HYPE alone and they differ by 10x (live
    # median 116s, live mean 687s, a replay of a different geometry 1177s).
    # DO NOT reintroduce a hold MODEL here. The driftless-random-walk one that
    # was fitted (hold = 31695 x (spread_bps/sigma_h)^2, one coin, one 43-hour
    # range-bound window) returns 15s for CASHCAT and 11s for ACE — shorter than
    # the measured 112s floor — because it has no drift and no adverse
    # selection, i.e. it omits the effect that dominates. A coin with no run of
    # its own gets verdict "unknown", and "unknown" does not pass.
    parser.add_argument("--hold-mean-s", type=float, default=None,
                       help="Mean inventory-episode length of THIS coin's own run, seconds.")
    parser.add_argument("--hold-p95-s", type=float, default=None,
                       help="p95 inventory-episode length of THIS coin's own run, seconds.")
    parser.add_argument("--fee-rt-bps", type=float, default=DEFAULT_FEE_RT_BPS,
                       help="Round-trip maker fee in bps (measured default: 3.0 on HL).")
    parser.add_argument("--adverse-bps", type=float, default=None,
                       help="Adverse selection in bps, from the #32 fill-quality harness. "
                            "No default: the tool refuses to guess it.")
    parser.add_argument("--spread-p25-bps", type=float, default=None,
                       help="Override the measured p25 spread the gate uses.")
    parser.add_argument("--json", dest="json_out", metavar="OUT", help="Write the report here.")
    parser.add_argument("--txt", dest="txt_out", metavar="OUT", help="Write the table here too.")
    return parser


def main(argv=None) -> int:
    parser = build_parser()
    try:
        args = parser.parse_args(argv)
    except SystemExit as exit_code:  # argparse already printed the reason
        return 2 if exit_code.code is None else int(exit_code.code)

    for day in args.days:
        if len(day) != 8 or not day.isdigit():
            print(f"--day expects YYYYMMDD, got {day!r}", file=sys.stderr)
            return 2
    if not args.um_dirs:
        print("--um-dir is required (the lead-lag metric needs the Binance side)", file=sys.stderr)
        return 2
    days = sorted(dict.fromkeys(args.days))

    screen = {
        "hold_mean_s": args.hold_mean_s,
        "hold_p95_s": args.hold_p95_s,
        "fee_rt_bps": args.fee_rt_bps,
        "adverse_bps": args.adverse_bps,
    }
    if args.spread_p25_bps is not None:
        screen["spread_p25_bps"] = args.spread_p25_bps

    try:
        report = build_report(
            args.hl_dir, args.coin, args.um_dirs, args.um_symbol, days, screen
        )
    except (ValueError, OSError) as error:
        print(f"error: {error}", file=sys.stderr)
        return 2

    text = render_text(report)
    sys.stdout.write(text)
    try:
        if args.json_out:
            with open(args.json_out, "w") as f:
                json.dump(report, f, indent=2, allow_nan=False)
                f.write("\n")
        if args.txt_out:
            with open(args.txt_out, "w") as f:
                f.write(text)
    except OSError as error:
        print(f"error: {error}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
