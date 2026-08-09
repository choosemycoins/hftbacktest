"""Tests for `funding.py` — funding on held inventory, as a post-hoc cost.

Same discipline as the sibling suites: every synthetic fixture is small enough
that the answer is known **analytically**, and the three real fixtures under
`testdata/funding/` are our own recordings and our own money, not a
re-derivation of what the code happens to print.

The anchor is `funding_ledger_golden_reproduces_the_venue_charges`: fourteen
real Hyperliquid `userFunding` payments against a position series reconstructed
from our own fills and oracle prices read out of the collector's own tape. Every
sign, the sampling rule and the price basis are pinned by that one test at once;
the synthetic tests exist so that when it breaks, the reason is visible.

Timestamps are realistic nanosecond values (~1.78e18) throughout, for the reason
the sibling modules state: they do not survive a round trip through float64, so
any place that lets one become a float shows up here.
"""

import json
import math
import sys
from pathlib import Path

import numpy as np
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))

import funding as fu  # noqa: E402

TESTDATA = Path(__file__).resolve().parent / "testdata" / "funding"

SEC = 1_000_000_000
HOUR = 3600 * SEC

#: 2026-08-08T00:00:00Z in nanoseconds — an exact hour mark, so a synthetic
#: record written at `at(k)` seconds lands where the arithmetic says it does.
T0 = 1_786_147_200_000_000_000


def at(seconds: float) -> int:
    """`seconds` after T0, exactly."""
    return T0 + int(round(seconds * 1000)) * 1_000_000


def series(start_s: float, end_s: float, position, step_s: float = 1.0):
    """A 1 Hz position series over `[start_s, end_s)` seconds after T0.

    `position` is either a constant or a callable of the sample's second offset.
    """
    n = int(round((end_s - start_s) / step_s))
    ts = np.array([at(start_s + i * step_s) for i in range(n)], dtype=np.int64)
    if callable(position):
        pos = np.array([float(position(start_s + i * step_s)) for i in range(n)])
    else:
        pos = np.full(n, float(position))
    return ts, pos


def curve(*rows, venue: str = "hyperliquid", coin: str = "TEST") -> "fu.FundingCurve":
    """A curve from `(hour_offset, rate, price)` triples, hours after T0."""
    return fu.FundingCurve(
        venue=venue,
        coin=coin,
        interval_ns=fu.HL_INTERVAL_NS,
        boundaries=tuple(
            fu.Boundary(ts_ns=T0 + int(h * HOUR), rate=r, price=p, source="tape")
            for h, r, p in rows
        ),
        unresolved=(),
    )


# ---------------------------------------------------------------------------
# 1-3. the sign convention, in all four quadrants
# ---------------------------------------------------------------------------


def test_a_long_position_pays_a_positive_rate():
    """Long + positive rate = we pay. Positive cost.

    HL docs: "the long position will pay the short position". A charge of
    +2 x 100 x 0.001 = +0.2 USD, and `gross_abs` is its magnitude.
    """
    ts, pos = series(-600, 600, +2.0)
    a = fu.accrue(curve((0, 0.001, 100.0)), ts, pos)
    assert a.cost_usd == pytest.approx(+0.2, abs=1e-12)
    assert a.gross_abs_usd == pytest.approx(0.2, abs=1e-12)
    assert a.charged == 1
    assert a.unresolved == ()


def test_a_short_position_receives_a_positive_rate():
    """Short + positive rate = we receive. Negative cost, positive gross."""
    ts, pos = series(-600, 600, -2.0)
    a = fu.accrue(curve((0, 0.001, 100.0)), ts, pos)
    assert a.cost_usd == pytest.approx(-0.2, abs=1e-12)
    assert a.gross_abs_usd == pytest.approx(0.2, abs=1e-12)


def test_a_negative_rate_reverses_both_signs():
    """Long + negative rate = we receive; short + negative rate = we pay."""
    ts, long_pos = series(-600, 600, +2.0)
    _, short_pos = series(-600, 600, -2.0)
    c = curve((0, -0.001, 100.0))
    assert fu.accrue(c, ts, long_pos).cost_usd == pytest.approx(-0.2, abs=1e-12)
    assert fu.accrue(c, ts, short_pos).cost_usd == pytest.approx(+0.2, abs=1e-12)


# ---------------------------------------------------------------------------
# 4-8. the sampling rule: position AT the boundary, no pro-rating
# ---------------------------------------------------------------------------


def test_a_position_that_flips_sign_mid_interval_is_charged_on_the_sign_it_holds_at_the_boundary():
    """The venue charges the position at the instant of settlement.

    +2 for 00:10-00:50, -2 for 00:50-01:10. The 01:00 boundary sees -2, so the
    charge is -2 x 100 x 0.001 = -0.2 and there is exactly ONE of them.
    Time-weighting the hour would give roughly +2*(40/60) - 2*(10/60) = +1.0
    units of position, i.e. a POSITIVE cost — the opposite sign.
    """
    ts, pos = series(600, 4200, lambda s: +2.0 if s < 3000 else -2.0)
    a = fu.accrue(curve((1, 0.001, 100.0)), ts, pos)
    assert a.charged == 1
    assert a.cost_usd == pytest.approx(-0.2, abs=1e-12)


def test_a_position_held_across_several_intervals_is_charged_once_per_interval():
    """Four boundaries, four charges, each at its OWN rate and price."""
    ts, pos = series(1800, 1800 + 4 * 3600 + 1800, +2.0)  # 00:30 -> 04:30
    rows = [(1, 0.001, 100.0), (2, 0.002, 110.0), (3, -0.003, 120.0), (4, 0.004, 130.0)]
    a = fu.accrue(curve(*rows), ts, pos)
    assert a.charged == 4
    expected = sum(2.0 * p * r for _, r, p in rows)
    assert a.cost_usd == pytest.approx(expected, abs=1e-12)
    # Not the first rate applied four times, and not one charge for the episode.
    assert a.cost_usd != pytest.approx(4 * 2.0 * 100.0 * 0.001, abs=1e-9)


def test_a_zero_position_costs_exactly_zero():
    """Flat across six boundaries: zero cost, but six boundaries CHARGED.

    Exact equality, not `approx`: a flat account paying an epsilon is a leak,
    and a `continue` that skips the counter as well as the arithmetic hides how
    many boundaries the run actually covered.
    """
    ts, pos = series(-60, 6 * 3600 + 60, 0.0)
    a = fu.accrue(curve(*[(h, 0.001, 100.0) for h in range(6)]), ts, pos)
    assert a.cost_usd == 0.0
    assert a.gross_abs_usd == 0.0
    assert a.charged == 6
    assert a.unresolved == ()


def test_a_run_that_starts_mid_interval_is_not_charged_for_the_hour_it_started_in():
    """Records 00:30 -> 02:30: charges at 01:00 and 02:00, nothing at 00:00.

    The 00:00 mark precedes the data. Back-filling it from `position[0]` would
    charge for an hour the run did not exist in, and it would not even be
    reported as unresolved — it is outside the covered span, not missing from it.
    """
    ts, pos = series(1800, 9000, +2.0)
    a = fu.accrue(curve((0, 0.001, 100.0), (1, 0.001, 100.0), (2, 0.001, 100.0)), ts, pos)
    assert a.charged == 2
    assert a.unresolved == ()
    assert a.cost_usd == pytest.approx(2 * 2.0 * 100.0 * 0.001, abs=1e-12)


def test_a_run_that_ends_mid_interval_pays_nothing_for_the_partial_hour():
    """Records 00:00 -> 02:30 flat long: exactly two charges, no pro-rating.

    The 30 minutes after 02:00 cost zero. Pro-rating them would add
    2 x 100 x 0.001 x 0.5 = +0.1.
    """
    ts, pos = series(0, 9000, +2.0)
    a = fu.accrue(curve((0, 0.001, 100.0), (1, 0.001, 100.0), (2, 0.001, 100.0)), ts, pos)
    assert a.charged == 3  # 00:00 IS inside [ts[0], ts[-1]] — it is ts[0]
    assert a.cost_usd == pytest.approx(3 * 0.2, abs=1e-12)
    # and nothing beyond it
    assert a.cost_usd != pytest.approx(3 * 0.2 + 0.1, abs=1e-9)


# ---------------------------------------------------------------------------
# 9-10. resolution: unresolved is not free, and a boundary on a record uses it
# ---------------------------------------------------------------------------


def test_a_boundary_with_no_record_within_the_staleness_bound_is_unresolved_not_free():
    """A recorder gap straddling a boundary is REPORTED, not silently zero.

    This is the fail-closed pin. The gap is 400 s; the newest record before
    01:00 is 300 s stale, past the 5 s bound. The charge is dropped from the
    cost — using a 400 s-old position is a guess — and the boundary lands in
    `unresolved`, where the caller must see it.
    """
    left_ts, left_pos = series(0, 3300, +2.0)  # ..00:55
    right_ts, right_pos = series(3700, 7200, +2.0)  # 01:01:40..
    ts = np.concatenate([left_ts, right_ts])
    pos = np.concatenate([left_pos, right_pos])
    a = fu.accrue(curve((1, 0.001, 100.0)), ts, pos)
    assert a.unresolved == (T0 + HOUR,)
    assert a.charged == 0
    assert a.cost_usd == 0.0


def test_a_fill_in_the_record_after_the_boundary_is_not_charged_for():
    """The record AT the boundary, not the one after it.

    A fill that lands at HH:00:00.5 shows up in the next 1 Hz record. The venue
    settled half a second earlier, against the position we held THEN. Here the
    boundary record is flat and the next one is +10: the correct charge is zero,
    and reading `position[i+1]` charges +10 x 100 x 0.001 = +1.0 for inventory
    we did not own at settlement.

    Separate from the `side="left"` pin next door: that one is off by one in the
    other direction, and neither fixture catches both.
    """
    ts, pos = series(3540, 3660, lambda s: 0.0 if s <= 3600 else +10.0)
    a = fu.accrue(curve((1, 0.001, 100.0)), ts, pos)
    assert a.charged == 1
    assert a.cost_usd == 0.0


def test_a_boundary_landing_exactly_on_a_record_uses_that_record():
    """`ts[i] == b.ts_ns` resolves to `i`, not `i-1`.

    The record at 01:00 carries +2; the one before it carries -2. `side="left"`
    would pick the -2 and flip the sign of the charge.
    """
    ts, pos = series(3540, 3660, lambda s: -2.0 if s < 3600 else +2.0)
    assert at(3600) in set(int(x) for x in ts)
    a = fu.accrue(curve((1, 0.001, 100.0)), ts, pos)
    assert a.cost_usd == pytest.approx(+0.2, abs=1e-12)


# ---------------------------------------------------------------------------
# 11, 13. the tape is the source of truth
# ---------------------------------------------------------------------------


def test_the_tape_derived_rate_equals_the_venue_settled_rate():
    """The last `ctx.funding` before HH:00 IS the rate the venue settles at HH:00.

    Real fixture: four hours of the 2026-08-08 HYPE recording against the same
    four rows of the venue's own `fundingHistory`. Exact equality — this is what
    makes the recording self-sufficient and the API a cross-check rather than a
    dependency. Taking the FIRST value of the hour, or the value at HH:00:00, or
    the mean over the hour, all break it: `ctx.funding` is a running accumulator
    that resets at the mark.
    """
    c = fu.curve_from_hl_tape([TESTDATA / "hl_tape_slice.gz"], "HYPE")
    settled = {
        int(r["time"]) // 1000 * SEC: float(r["fundingRate"])
        for r in json.loads((TESTDATA / "hl_funding_history.json").read_text())
    }
    assert len(settled) >= 3
    by_ts = {b.ts_ns: b for b in c.boundaries}
    for ts_ns, rate in settled.items():
        assert ts_ns in by_ts, f"no boundary built for {ts_ns}"
        assert by_ts[ts_ns].rate == rate


def test_the_notional_basis_is_the_oracle_price():
    """`Boundary.price` carries `oraclePx`, not `markPx` and not `midPx`.

    Doc-authority (HL: "the spot oracle price is used ... not the mark price").
    The fixture frames have all three and they differ, so reading the wrong one
    is visible here even though our own $11-notional payments cannot tell them
    apart.
    """
    c = fu.curve_from_hl_tape([TESTDATA / "hl_tape_slice.gz"], "HYPE")
    golden = json.loads((TESTDATA / "hl_ledger_golden.json").read_text())
    oracle = {int(k): float(v["oracle_px"]) for k, v in golden["boundaries"].items()}
    mark = {int(k): float(v["mark_px"]) for k, v in golden["boundaries"].items()}
    checked = 0
    for b in c.boundaries:
        if b.ts_ns not in oracle:
            continue
        assert oracle[b.ts_ns] != mark[b.ts_ns], "fixture cannot discriminate"
        assert b.price == oracle[b.ts_ns]
        checked += 1
    assert checked >= 1


# ---------------------------------------------------------------------------
# 12. the anchor
# ---------------------------------------------------------------------------


def _golden_inputs():
    """The real ledger, position series and boundary prices as `accrue` inputs."""
    g = json.loads((TESTDATA / "hl_ledger_golden.json").read_text())
    segments = [(int(a), int(b), float(p)) for a, b, p in g["position_segments"]]
    w0, w1 = g["window_ms"]
    n = (w1 - w0) // 1000
    ts = np.array([(w0 + i * 1000) * 1_000_000 for i in range(n)], dtype=np.int64)
    pos = np.zeros(n)
    for a, b, p in segments:
        if p == 0.0:
            continue
        lo = max(0, (a - w0 + 999) // 1000)
        hi = min(n, (b - w0 + 999) // 1000)
        pos[lo:hi] = p
    boundaries = tuple(
        fu.Boundary(
            ts_ns=int(k),
            rate=float(e["funding_rate"]),
            price=float(g["boundaries"][k]["oracle_px"]),
            source="tape",
        )
        for k, e in (
            (str(int(e["time_ms"]) * 1_000_000 // fu.HL_INTERVAL_NS * fu.HL_INTERVAL_NS), e)
            for e in g["events"]
        )
    )
    c = fu.FundingCurve("hyperliquid", "HYPE", fu.HL_INTERVAL_NS, boundaries, ())
    return g, c, ts, pos


def test_funding_ledger_golden_reproduces_the_venue_charges():
    """Fourteen real payments, reproduced to 1e-6 USD, sign included.

    MEASURED: the venue credited -0.000255 USDC in total; charging
    `position(boundary) x oraclePx x rate` over the same fourteen hour marks
    gives a cost of +0.000255555, i.e. the same money with the sign inverted.
    `usdc` in `userFunding` is a signed CREDIT and is the negative of our cost.
    """
    g, c, ts, pos = _golden_inputs()
    a = fu.accrue(c, ts, pos)

    assert a.charged == 14
    assert a.unresolved == ()

    ledger_credit = sum(float(e["usdc"]) for e in g["events"])
    assert a.cost_usd == pytest.approx(-ledger_credit, abs=1e-6)
    assert a.cost_usd == pytest.approx(0.000255555, abs=1e-9)

    # per event, and the position we sampled is the one the venue named in szi
    per = {}
    for b in c.boundaries:
        i = int(np.searchsorted(ts, b.ts_ns, side="right")) - 1
        per[b.ts_ns] = pos[i] * b.price * b.rate
    for e in g["events"]:
        key = int(e["time_ms"]) * 1_000_000 // fu.HL_INTERVAL_NS * fu.HL_INTERVAL_NS
        assert per[key] == pytest.approx(-float(e["usdc"]), abs=1e-6)
        i = int(np.searchsorted(ts, key, side="right")) - 1
        assert pos[i] == pytest.approx(float(e["szi"]), abs=1e-12)


def test_the_time_weighted_variant_fails_the_ledger_agreement_the_boundary_rule_passes():
    """The rule the venue does NOT use, kept so "improving" it stays visible.

    MEASURED on the checked-in fixture: the boundary rule lands +0.000255555
    against a ledger cost of +0.000255 (inside 1e-6, which is the venue's own
    `usdc` truncation), while time-weighting the position across each hour lands
    +0.000271070 — 1.55e-5 out, fifteen times the tolerance the boundary rule
    clears. That is the whole claim: one rule reproduces our money and the other
    does not.

    Deliberately NOT the brief's "-0.000188, a 36% miss". That figure
    time-weights over all 45 funding hours in the campaign window, including the
    31 hours that were flat at the mark and so never appear in the ledger; this
    fixture carries the 14 charge hours only and cannot reproduce it. The sharp
    synthetic discriminator between the two rules is
    `test_a_position_that_flips_sign_mid_interval_...`, where they differ in
    SIGN rather than in size.
    """
    g, c, ts, pos = _golden_inputs()
    ledger_cost = -sum(float(e["usdc"]) for e in g["events"])

    exact = fu.accrue(c, ts, pos).cost_usd
    weighted = 0.0
    for b in c.boundaries:
        lo = int(np.searchsorted(ts, b.ts_ns - fu.HL_INTERVAL_NS, side="left"))
        hi = int(np.searchsorted(ts, b.ts_ns, side="right"))
        if hi > lo:
            weighted += float(pos[lo:hi].mean()) * b.price * b.rate

    assert abs(exact - ledger_cost) < 1e-6
    assert abs(weighted - ledger_cost) > 10e-6


# ---------------------------------------------------------------------------
# 14-15. the interest default, and the round trip
# ---------------------------------------------------------------------------


def test_the_interest_default_is_not_treated_as_a_floor():
    """+0.125 bps/h is the interest COMPONENT, not a bound.

    Rates below it and negative rates survive construction and a round trip
    untouched. Clamping them would zero out exactly the adverse side the whole
    term was introduced to price.
    """
    below = fu.HL_INTEREST_DEFAULT / 10.0
    c = curve((0, below, 100.0), (1, -0.004, 100.0), (2, fu.HL_INTEREST_DEFAULT, 100.0))
    assert [b.rate for b in c.boundaries] == [below, -0.004, fu.HL_INTEREST_DEFAULT]
    assert [b.rate for b in fu.from_json(fu.to_json(c)).boundaries] == [
        below,
        -0.004,
        fu.HL_INTEREST_DEFAULT,
    ]


def test_a_curve_round_trips_through_json():
    """Everything, including `unresolved` and `source`.

    Dropping `unresolved` from serialization would silently reset the count of
    hours the curve could not cover to zero in whichever consumer reads the file
    rather than building the curve itself.
    """
    c = fu.FundingCurve(
        venue="hyperliquid",
        coin="HYPE",
        interval_ns=fu.HL_INTERVAL_NS,
        boundaries=(
            fu.Boundary(T0, 1.25e-5, 54.0842, "tape"),
            fu.Boundary(T0 + HOUR, -1.62e-5, 54.1, "fundingHistory"),
        ),
        unresolved=(T0 + 2 * HOUR,),
    )
    back = fu.from_json(fu.to_json(c))
    assert back == c
    assert back.unresolved == (T0 + 2 * HOUR,)
    assert [b.source for b in back.boundaries] == ["tape", "fundingHistory"]


# ---------------------------------------------------------------------------
# the cumulative curve — what makes the equity path funding-aware
# ---------------------------------------------------------------------------


def test_the_cumulative_curve_is_a_step_function_aligned_to_the_input():
    """`cum_curve` is the running cost, one value per input record.

    Constant between boundaries and stepping at the first record at or after
    each one, so a caller can subtract it from an equity path and have the
    drawdown move where the venue actually debited.
    """
    ts, pos = series(0, 7200 + 60, +2.0)
    a = fu.accrue(curve((0, 0.001, 100.0), (1, 0.001, 100.0), (2, 0.001, 100.0)), ts, pos)
    assert a.cum_curve.shape == ts.shape
    assert a.cum_curve[0] == pytest.approx(0.2, abs=1e-12)
    assert a.cum_curve[3599] == pytest.approx(0.2, abs=1e-12)
    assert a.cum_curve[3600] == pytest.approx(0.4, abs=1e-12)
    assert a.cum_curve[-1] == pytest.approx(a.cost_usd, abs=1e-12)


def test_an_empty_series_charges_nothing_and_says_so():
    """No records: no cost, no charges, and no exception."""
    a = fu.accrue(curve((0, 0.001, 100.0)), np.zeros(0, dtype=np.int64), np.zeros(0))
    assert a.cost_usd == 0.0
    assert a.charged == 0
    assert a.unresolved == ()
    assert a.cum_curve.size == 0


def test_a_non_finite_price_or_rate_is_unresolved_rather_than_a_nan_cost():
    """One bad boundary must not turn the whole run's cost into NaN."""
    c = fu.FundingCurve(
        "hyperliquid",
        "T",
        fu.HL_INTERVAL_NS,
        (
            fu.Boundary(T0, 0.001, 100.0, "tape"),
            fu.Boundary(T0 + HOUR, float("nan"), 100.0, "tape"),
        ),
        (),
    )
    ts, pos = series(-60, 7200, +2.0)
    a = fu.accrue(c, ts, pos)
    assert math.isfinite(a.cost_usd)
    assert a.cost_usd == pytest.approx(0.2, abs=1e-12)
    assert a.unresolved == (T0 + HOUR,)
