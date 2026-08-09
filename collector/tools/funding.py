#!/usr/bin/env python3
"""Funding on held inventory, as a post-hoc cost.

Perpetual funding is a real cost of holding inventory and it is missing from
every number this operation currently ranks on. This module is the ONE place the
venue's funding convention is written down; the two consumers that need it
(`rerank_metrics.py` for the coin screen, `myhft/scripts/sweep_grid.py` for the
sweep ranking) import it or read the JSON it emits.

It deliberately lives outside the Rust engine. #40 asks for funding as a cost
for RANKING and SCREENING, not as something a strategy reacts to, and both
inputs are already on disk: the position series at 1 Hz in every
`BacktestRecorder` NPZ, and the rate at ~1 Hz in the collector's own
`activeAssetCtx` tape. Putting it in `hftbacktest/` would need a fifth event
kind (widening an `unsafe` transmute in the backtest hot path), a data-format
change and three `Bot` impls — and would still be backtest-only, because
`LiveBot` never calls `apply_fill`. The live source of truth for funding is the
venue's `userFunding`, exactly as it already is for PnL.


SIGN CONVENTION
---------------

    funding_cost_usd(t) = position_signed(t) x oracle_price(t) x funding_rate(t)
                          # POSITIVE = WE PAY.  NEGATIVE = WE RECEIVE.

    equity_after_funding = balance + position * price - fee - sum(funding_cost)

Long + positive rate => we pay.  Short + positive rate => we receive.
Long + negative rate => we receive.  Short + negative rate => we pay.

Hyperliquid docs, *Trading -> Funding*, verbatim: "the funding payment at the
end of the interval is `position_size * oracle_price * funding_rate`"; "the long
position will pay the short position" (rate positive); "Funding Rate (F) =
Average Premium Index (P) + clamp(interest rate - Premium Index (P), -0.0005,
0.0005)"; "interest rate component is predetermined at 0.01% every 8 hours,
which is 0.00125% every hour". Funding is paid hourly.

MEASURED against our own money: 14 real `userFunding` payments (account
0x96d3EDB96569bb73a4a3a48c00b54cf2fdAb1634, 2026-08-07T18:00Z ->
2026-08-09T11:00Z, all four sign quadrants present) reconstruct to a cost of
+0.000255555 USD against a ledger credit sum of -0.000255 USD — agreement to
1e-6 USD. `usdc` in `userFunding` is a signed CREDIT and is therefore the
NEGATIVE of our cost; anything consuming that field directly must invert it.
Pinned by `test_funding_ledger_golden_reproduces_the_venue_charges`.


THREE RULES THAT ARE PART OF THE CONTRACT
-----------------------------------------

1. **Position is sampled AT the boundary, not time-weighted across the
   interval.** The venue charges the position at the instant of settlement.
   MEASURED: the boundary rule reproduces our ledger to 1e-6 USD; the
   time-weighted variant misses by ~26% on the same fourteen events.
2. **A run that ends mid-interval pays nothing for the partial interval.** No
   pro-rating. Same reason.
3. **`oraclePx`, not `markPx`, not the NPZ mid.** INFERRED (doc-authority):
   Hyperliquid's docs say "the spot oracle price is used to convert the position
   size to notional value, **not the mark price**", but our own data cannot
   discriminate — HL truncates `usdc` at 6 decimals and our position was 0.2
   HYPE (~$11 notional), giving ~0.4% price resolution against a 0.06%
   oracle-vs-mark gap, so all 10 checkable payments admit both. Bound on the
   error if the docs are wrong: the premium, ~4 bps of a term that is itself
   ~0.1 bps/h.

These are measured against a real ledger. They are not preferences, and
"improving" them (pro-rating, time-weighting, treating an unresolved boundary as
free) silently changes the answer.


WHAT `+0.125 bps/h` IS AND IS NOT
---------------------------------

`HL_INTEREST_DEFAULT` is the INTEREST COMPONENT (0.01%/8h = 0.00125%/h), which
applies whenever the premium is ~0. It is **not a floor**: MEASURED, 3646 of
6384 coin-hours in a 19-coin x 14-day panel sit exactly there, and rates go both
below it and negative. Clamping to it would zero out exactly the adverse side
this module exists to price. Pinned by
`test_the_interest_default_is_not_treated_as_a_floor`.


PER-VENUE INTERVAL AND BASIS — so the next implementer does not re-measure
--------------------------------------------------------------------------

Only Hyperliquid is implemented; the `curve_from_*` seam is where another venue
would attach. MEASURED from our own recordings and each venue's docs:

| venue            | interval | basis            | carrier in our tape           |
|------------------|----------|------------------|-------------------------------|
| hyperliquid      | 1 h      | spot oracle px   | `activeAssetCtx` (~1.02 s)    |
| paradex          | 8 h      | spot oracle px   | `funding_data.{market}` (5 s) |
| extended         | 1 h      | UNDOCUMENTED     | `{base}/funding/{m}` (60 s)   |
| lighter          | 1 h      | mark vs index    | `market_stats` (~1.3 s)       |
| binancefuturesum | **4 h**  | mark price       | REST `premiumIndex` (10 s)    |
| binancefuturescm | 8 h      | mark price       | `@markPrice@1s`               |
| aster            | 8 h      | mark price       | REST `premiumIndex` (10 s)    |
| bybit            | 8 h      | mark price       | **NOT RECORDED AT ALL**       |

Two traps in that table. Binance USD-M is 4 h for HYPEUSDT, not the 8 h everyone
assumes (measured from `nextFundingTime`). Paradex accrues CONTINUOUSLY via
`funding_index` (`Accrued Funding PnL = -Position x (Current Index - Cached
Index)`), so the boundary-sampling rule below is exact for Hyperliquid and would
be an approximation there — that is the one condition under which this whole
post-hoc design should be revisited.
"""

from __future__ import annotations

import argparse
import json
import math
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Iterable, Optional, Sequence, Tuple

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parent))

from quality_report import TruncatedRecording, iter_gz_lines, parse_line  # noqa: E402

SEC_NS = 1_000_000_000
HL_INTERVAL_NS = 3600 * SEC_NS

#: The interest component of Hyperliquid's hourly rate — 0.01%/8h. NOT a floor;
#: see the module docstring.
HL_INTEREST_DEFAULT = 1.25e-5

#: How stale the newest `activeAssetCtx` frame before an hour mark may be and
#: still be read as that hour's settled rate. The feed prints about once a
#: second (MEASURED: ~84,918 frames/coin/day), so 60 s is two orders of
#: magnitude of slack — it is a hole detector, not a tuning knob.
HL_TAPE_MAX_AGE_NS = 60 * SEC_NS

#: Default bound on how stale the newest position record before a boundary may
#: be. The recorder writes at exactly 1 Hz (MEASURED: 431,990/431,990 deltas =
#: 1e9 ns), so 5 s means "the recorder was running", not "close enough".
DEFAULT_MAX_STALENESS_NS = 5 * SEC_NS

SCHEMA = "funding/1"


@dataclass(frozen=True)
class Boundary:
    """One settlement instant and everything needed to charge for it.

    `rate` is signed and dimensionless (a fraction, not bps, not a percent).
    `price` is the notional basis at `ts_ns` — `oraclePx` on Hyperliquid.
    `source` is `"tape"` or `"fundingHistory"`, so a curve assembled from both
    says which hours it had to fall back for.
    """

    ts_ns: int
    rate: float
    price: float
    source: str


@dataclass(frozen=True)
class FundingCurve:
    """A coin's settled rates over a window.

    `boundaries` is strictly ascending in `ts_ns`. `unresolved` lists interval
    marks INSIDE the covered span for which no rate could be recovered — a hole
    in the recording. It is carried rather than dropped because a curve that
    silently omits an hour and a curve that covers it look identical to the
    consumer, and the difference is money.
    """

    venue: str
    coin: str
    interval_ns: int
    boundaries: Tuple[Boundary, ...]
    unresolved: Tuple[int, ...]


@dataclass(frozen=True)
class Accrual:
    """What a curve costs a position series.

    `cost_usd` is signed, POSITIVE = we paid. `gross_abs_usd` is the sum of the
    magnitudes, i.e. the one-sided bound the cancellation between long and short
    hours was measured against. `charged` counts the boundaries that resolved;
    `unresolved` names the ones that did not, and they contribute NOTHING to
    the cost — the caller must surface the count rather than read a silent zero
    as a free hour. `cum_curve` is the running cost aligned to the input
    timestamps, a step function that steps at the first record at or after each
    boundary, so subtracting it from an equity path moves the drawdown where the
    venue actually debited.
    """

    cost_usd: float
    gross_abs_usd: float
    charged: int
    unresolved: Tuple[int, ...]
    cum_curve: np.ndarray


# ---------------------------------------------------------------------------
# building a curve
# ---------------------------------------------------------------------------


def curve_from_samples(
    ts_ns,
    rate,
    price,
    coin: str = "",
    *,
    venue: str = "hyperliquid",
    interval_ns: int = HL_INTERVAL_NS,
    max_age_ns: int = HL_TAPE_MAX_AGE_NS,
    source: str = "tape",
) -> FundingCurve:
    """Settle a running rate sample series into per-interval boundaries.

    The reduction the whole module turns on, in one place so that the file
    reader and the screen cannot drift apart: the value that settles at a mark
    is the LAST sample STRICTLY BEFORE it. Strictly, because `ctx.funding` is a
    running accumulator that resets at the mark — a sample stamped exactly on
    the mark belongs to the hour that is starting, not the one that just
    settled.

    A mark whose newest sample is older than `max_age_ns` is a hole in the
    recording, and lands in `unresolved` rather than being priced from a stale
    sample. `unresolved` is computed over the marks inside the sampled span
    only: a mark before the first sample or after the last one is not missing
    from this recording, it is outside it.
    """
    ts = np.asarray(ts_ns, dtype=np.int64)
    rates = np.asarray(rate, dtype=np.float64)
    prices = np.asarray(price, dtype=np.float64)
    if not (ts.size == rates.size == prices.size):
        raise ValueError("ts_ns/rate/price must be the same length")
    if ts.size == 0:
        return FundingCurve(venue, coin.upper(), interval_ns, (), ())

    order = np.argsort(ts, kind="stable")
    ts, rates, prices = ts[order], rates[order], prices[order]

    best: dict = {}
    for i in range(ts.size):
        t = int(ts[i])
        if not (math.isfinite(rates[i]) and math.isfinite(prices[i])):
            continue
        mark = -(-t // interval_ns) * interval_ns
        if t == mark:
            mark += interval_ns
        have = best.get(mark)
        if have is None or have < i:
            best[mark] = i

    boundaries = tuple(
        Boundary(int(mark), float(rates[i]), float(prices[i]), source)
        for mark, i in sorted(best.items())
        if mark - int(ts[i]) <= max_age_ns
    )
    covered = {b.ts_ns for b in boundaries}
    first, last = int(ts[0]), int(ts[-1])
    marks = range(-(-first // interval_ns) * interval_ns, last + 1, interval_ns)
    unresolved = tuple(m for m in marks if m not in covered)
    return FundingCurve(venue, coin.upper(), interval_ns, boundaries, unresolved)


def curve_from_hl_tape(
    paths: Iterable,
    coin: str,
    *,
    max_age_ns: int = HL_TAPE_MAX_AGE_NS,
) -> FundingCurve:
    """The settled hourly rates of `coin`, read out of our own recordings.

    `ctx.funding` is not a static field: it is Hyperliquid's RUNNING accumulator
    for the hour in progress, which resets at each mark and re-averages every
    few seconds. The value that matters is therefore the LAST one strictly
    before the mark — MEASURED to equal the venue's own `fundingHistory` rate at
    that mark bit for bit, 92/92 hours across HYPE/PUMP/VVV/AERO on 2026-08-08,
    and 14/14 against our own ledger. Taking the first value of the hour, the
    value at HH:00:00, or the mean over the hour all give a different number.

    That measurement is what makes the recording self-sufficient: the API is a
    cross-check, not a dependency. It also gives `oraclePx`, which the API does
    not.

    An hour mark inside the covered span whose newest frame is older than
    `max_age_ns` is reported in `unresolved` rather than filled from a stale
    frame or dropped.
    """
    want = coin.upper()
    ts_out, rate_out, px_out = [], [], []
    for path in paths:
        try:
            for line in iter_gz_lines(path):
                if b"activeAssetCtx" not in line:
                    continue
                try:
                    ts, obj = parse_line(line)
                except (ValueError, json.JSONDecodeError):
                    continue
                if obj.get("channel") != "activeAssetCtx":
                    continue
                data = obj.get("data") or {}
                if str(data.get("coin", "")).upper() != want:
                    continue
                ctx = data.get("ctx") or {}
                if "funding" not in ctx or "oraclePx" not in ctx:
                    continue
                ts_out.append(ts)
                rate_out.append(float(ctx["funding"]))
                px_out.append(float(ctx["oraclePx"]))
        except TruncatedRecording:
            # A truncated recording is still evidence; the hours that decoded
            # are real and the ones that did not fall out as `unresolved`.
            continue
    return curve_from_samples(
        np.array(ts_out, dtype=np.int64),
        np.array(rate_out),
        np.array(px_out),
        want,
        max_age_ns=max_age_ns,
    )


def curve_from_hl_funding_history(
    rows: Sequence[dict],
    price_lookup: Callable[[int], Optional[float]],
) -> FundingCurve:
    """The same curve from the venue's `fundingHistory`, for hours the tape lost.

    `rows` are the API's objects verbatim (`coin`, `fundingRate`, `time` in
    milliseconds); `time` is the SETTLEMENT instant and the rate covers the hour
    ENDING at it. `price_lookup` supplies the notional basis, because the API
    does not — an hour it cannot price is `unresolved`, not priced from a
    neighbour.
    """
    boundaries = []
    unresolved = []
    coin = ""
    for row in sorted(rows, key=lambda r: int(r["time"])):
        coin = coin or str(row.get("coin", "")).upper()
        ts_ns = int(row["time"]) // 1000 * SEC_NS
        price = price_lookup(ts_ns)
        if price is None or not math.isfinite(price) or price <= 0:
            unresolved.append(ts_ns)
            continue
        boundaries.append(
            Boundary(ts_ns, float(row["fundingRate"]), float(price), "fundingHistory")
        )
    return FundingCurve(
        "hyperliquid", coin, HL_INTERVAL_NS, tuple(boundaries), tuple(unresolved)
    )


def merge_curves(*curves: FundingCurve) -> FundingCurve:
    """One curve per coin-window, preferring the earlier argument per boundary.

    Written for "the tape, and `fundingHistory` for the hours it lost": pass the
    tape first. A mark the winner covers stops being unresolved.
    """
    curves = tuple(c for c in curves if c is not None)
    if not curves:
        raise ValueError("merge_curves needs at least one curve")
    by_ts: dict = {}
    for c in reversed(curves):
        for b in c.boundaries:
            by_ts[b.ts_ns] = b
    covered = set(by_ts)
    unresolved = sorted({u for c in curves for u in c.unresolved} - covered)
    head = curves[0]
    return FundingCurve(
        head.venue,
        head.coin,
        head.interval_ns,
        tuple(by_ts[k] for k in sorted(by_ts)),
        tuple(unresolved),
    )


# ---------------------------------------------------------------------------
# charging a position series
# ---------------------------------------------------------------------------


def accrue(
    curve: FundingCurve,
    ts_ns,
    position,
    *,
    max_staleness_ns: int = DEFAULT_MAX_STALENESS_NS,
) -> Accrual:
    """What `curve` costs the position series `(ts_ns, position)`.

    For each boundary whose instant lies within `[ts_ns[0], ts_ns[-1]]`, the
    charge is `position[i] * b.price * b.rate` where `i` is the LAST record at
    or before the boundary. Positive means we pay.

    Three things this does not do, each of them measured rather than chosen:

    * it does not time-weight the position across the interval (rule 1 above);
    * it does not pro-rate a partial interval at either end (rule 2);
    * it does not treat a boundary it cannot resolve as free. If there is no
      record within `max_staleness_ns` before the boundary, the boundary is
      counted in `unresolved`, contributes nothing, and the caller is expected
      to surface the count. A 400-second recorder gap is not a free hour.

    Boundaries OUTSIDE the covered span are not unresolved — they are simply not
    this run's business, and back-filling the mark that precedes the first
    record from `position[0]` would charge for an hour the run did not exist in.
    """
    ts = np.asarray(ts_ns, dtype=np.int64)
    pos = np.asarray(position, dtype=np.float64)
    if ts.size != pos.size:
        raise ValueError(f"ts_ns/position length mismatch: {ts.size} vs {pos.size}")
    cum = np.zeros(ts.size, dtype=np.float64)
    if ts.size == 0:
        return Accrual(0.0, 0.0, 0, (), cum)

    lo, hi = int(ts[0]), int(ts[-1])
    cost = 0.0
    gross = 0.0
    charged = 0
    unresolved = []
    # (index of the first record at or after the boundary, charge)
    steps = []

    for b in curve.boundaries:
        if b.ts_ns < lo or b.ts_ns > hi:
            continue
        if not (math.isfinite(b.rate) and math.isfinite(b.price)):
            unresolved.append(int(b.ts_ns))
            continue
        i = int(np.searchsorted(ts, b.ts_ns, side="right")) - 1
        if i < 0 or b.ts_ns - int(ts[i]) > max_staleness_ns:
            unresolved.append(int(b.ts_ns))
            continue
        p = float(pos[i])
        if not math.isfinite(p):
            unresolved.append(int(b.ts_ns))
            continue
        charge = p * b.price * b.rate
        cost += charge
        gross += abs(charge)
        charged += 1
        if charge:
            steps.append((int(np.searchsorted(ts, b.ts_ns, side="left")), charge))

    for index, charge in steps:
        if index < cum.size:
            cum[index] += charge
    np.cumsum(cum, out=cum)
    return Accrual(cost, gross, charged, tuple(unresolved), cum)


# ---------------------------------------------------------------------------
# serialization
# ---------------------------------------------------------------------------


def to_json(curve: FundingCurve, *, indent: Optional[int] = None) -> str:
    """A curve as JSON. Integers stay integers: a nanosecond stamp is not a float."""
    return json.dumps(
        {
            "schema": SCHEMA,
            "venue": curve.venue,
            "coin": curve.coin,
            "interval_ns": int(curve.interval_ns),
            "boundaries": [
                {
                    "ts_ns": int(b.ts_ns),
                    "rate": b.rate,
                    "price": b.price,
                    "source": b.source,
                }
                for b in curve.boundaries
            ],
            "unresolved": [int(u) for u in curve.unresolved],
        },
        indent=indent,
    )


def from_json(text: str) -> FundingCurve:
    obj = json.loads(text)
    return FundingCurve(
        venue=obj["venue"],
        coin=obj["coin"],
        interval_ns=int(obj["interval_ns"]),
        boundaries=tuple(
            Boundary(int(b["ts_ns"]), float(b["rate"]), float(b["price"]), b["source"])
            for b in obj["boundaries"]
        ),
        unresolved=tuple(int(u) for u in obj.get("unresolved", ())),
    )


def load_curve(path) -> FundingCurve:
    return from_json(Path(path).read_text())


# ---------------------------------------------------------------------------
# rate statistics — what the screen reports instead of a point estimate
# ---------------------------------------------------------------------------


def rate_stats(curve: FundingCurve) -> dict:
    """The empirical distribution of the curve's hourly rates, in bps/hour.

    A DESCRIPTION OF RISK, NOT A FORECAST, and the distinction is measured, not
    stylistic: predicting a day's mean hourly rate with the trailing 7-day mean
    gives MAE 0.610 bps/h across 19 coins x 8 days, against 0.640 for the
    constant interest default — a 5% edge that is WORSE than the constant for 9
    of the 19 coins (SAGA: 1.543 vs 0.194). A single scalar per coin is not
    defensible, and a spot read is worse: MEASURED, CASHCAT moved +4.125 ->
    +3.030 bps/h and ACE -0.938 -> -1.811 within two hours, and over 168 hours
    CASHCAT averages +1.18 while ACE averages -8.93. A spot read mis-ranks the
    very coins the term was introduced to catch.

    Hence quantiles and a tail, evaluated at three named points by the caller.
    """
    rates = np.array([b.rate for b in curve.boundaries], dtype=np.float64)
    rates = rates[np.isfinite(rates)]
    if rates.size == 0:
        return {
            "hours_covered": 0,
            "mean": None,
            "median": None,
            "p05": None,
            "p95": None,
            "max_abs": None,
            "frac_at_interest_default": None,
            "sign_flips": None,
        }
    bps = rates * 1e4
    signs = np.sign(rates[rates != 0.0])
    return {
        "hours_covered": int(rates.size),
        "mean": float(bps.mean()),
        "median": float(np.median(bps)),
        "p05": float(np.percentile(bps, 5)),
        "p95": float(np.percentile(bps, 95)),
        "max_abs": float(np.abs(bps).max()),
        "frac_at_interest_default": float(
            np.isclose(rates, HL_INTEREST_DEFAULT, rtol=0.0, atol=1e-12).mean()
        ),
        "sign_flips": int((np.diff(signs) != 0).sum()) if signs.size > 1 else 0,
    }


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _days(spec: str):
    if ".." not in spec:
        return [d.strip() for d in spec.split(",") if d.strip()]
    lo, hi = (s.strip() for s in spec.split("..", 1))
    import datetime as dt

    a = dt.datetime.strptime(lo, "%Y%m%d").date()
    b = dt.datetime.strptime(hi, "%Y%m%d").date()
    out = []
    while a <= b:
        out.append(a.strftime("%Y%m%d"))
        a += dt.timedelta(days=1)
    return out


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(
        description="Build a funding curve from the collector's own recordings."
    )
    ap.add_argument("--venue", default="hyperliquid", choices=["hyperliquid"])
    ap.add_argument("--coin", required=True)
    ap.add_argument("--hl-dir", required=True, type=Path)
    ap.add_argument("--days", required=True, help="20260801..20260805 or a comma list")
    ap.add_argument("--out", type=Path)
    args = ap.parse_args(argv)

    paths = [args.hl_dir / f"{args.coin.lower()}_{d}.gz" for d in _days(args.days)]
    missing = [p for p in paths if not p.exists()]
    for p in missing:
        print(f"missing recording: {p}", file=sys.stderr)
    curve = curve_from_hl_tape([p for p in paths if p.exists()], args.coin)
    text = to_json(curve, indent=1)
    if args.out:
        args.out.write_text(text)
        stats = rate_stats(curve)
        print(
            f"{curve.coin}: {stats['hours_covered']} hours, "
            f"{len(curve.unresolved)} unresolved, "
            f"median {stats['median']:.4f} bps/h -> {args.out}"
        )
    else:
        print(text)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
