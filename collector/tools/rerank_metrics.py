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


def time_weighted_median(values: np.ndarray, weights: np.ndarray) -> Optional[float]:
    """The smallest value whose cumulative weight reaches half the total."""
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
    index = int(np.searchsorted(cumulative, 0.5 * cumulative[-1], side="left"))
    return float(v[min(index, v.size - 1)])


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
        "p50_bps": time_weighted_median(series.spread_bps, weight),
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
    frame weights (the last frame of a day has none), price-run lifetimes, and
    the lead-lag sums, which are added per lag. Everything else concatenates.
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


def summarize(samples: DaySamples) -> dict:
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
        "spread": summarize_spread(samples.bbo, tick_scaled),
        "conditional_spread": conditional_spread_curve(samples.windows, samples.bbo),
        "touch": summarize_touch(
            samples.bbo,
            (
                (samples.lifetime_bid, samples.lifetime_bid_dropped),
                (samples.lifetime_ask, samples.lifetime_ask_dropped),
            ),
        ),
        "depth": summarize_depth(samples.fast, samples.slow),
        "leadlag": summarize_leadlag(
            samples.leadlag,
            cadence_ms=(
                _median_interval_ms(samples.bbo.ts),
                _median_interval_ms(samples.um_ts),
            ),
        ),
        "warnings": warnings,
    }


def build_report(hl_dir, coin, um_dirs, um_symbol, days) -> dict:
    read = [read_day(hl_dir, coin, um_dirs, um_symbol, day) for day in days]
    rows = [summarize(d) for d in read]
    if len(read) > 1:
        rows.append(summarize(pool_days(read)))
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
    lines.append(f"  caveat: {CONDITIONAL_CAVEAT}")
    lines.append(f"  caveat: {LEADLAG_CAVEAT}")
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

    try:
        report = build_report(args.hl_dir, args.coin, args.um_dirs, args.um_symbol, days)
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
