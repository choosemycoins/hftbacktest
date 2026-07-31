"""Tests for `rerank_metrics.py` — the §9.2 microstructure metrics.

Same discipline as `test_quality_report.py`: every synthetic fixture is small
enough that the answer is known **analytically**, not by running the code and
writing down what came out. Where a metric is a distribution, the fixture is
built so the expected quantile is an exact number.

Two shapes of test live here:

* metric-level, against the pure functions over builder-made series. This is
  where the arithmetic is pinned — time weighting, the tick mode, the depth
  window, the lead-lag sign convention.
* file-level, against real gzip files written into `tmp_path` in the collector's
  line format, plus ONE real fixture: two minutes of the 2026-07-29 PUMP
  recording (`testdata/rerank_metrics/`), which the whole CLI is smoke-run over.

Timestamps are realistic nanosecond values (~1.78e18) for the reason the sibling
module states: they do not survive a round trip through float64, so any place
that lets one become a float shows up here rather than as a rounding nobody
notices.
"""

import gzip
import json
import math
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))

import rerank_metrics as rm  # noqa: E402

DAY = "20260729"
#: 2026-07-29T00:00:00Z in nanoseconds. A multiple of every grid step used here,
#: so a frame written at `ns(k)` lands exactly on a grid point.
D29 = 1_785_283_200_000_000_000
SEC = 1_000_000_000
MS = 1_000_000


def ns(seconds: float = 0.0) -> int:
    """`seconds` into 2026-07-29, as an exact int (milliseconds resolution)."""
    return D29 + int(round(seconds * 1000)) * MS


def ns_on(day: str, seconds: float = 0.0) -> int:
    """`seconds` into `day` (`YYYYMMDD`), as an exact int.

    A fixture that stamps every day's frames with day one's timestamps cannot
    pin anything the pooled row does: the days are then interchangeable, so
    "pooled" and "day one" are indistinguishable no matter how the pooling
    concatenates. Derived from the date rather than the clock, so it stays
    deterministic.
    """
    midnight = datetime.strptime(day, "%Y%m%d").replace(tzinfo=timezone.utc)
    return int(midnight.timestamp()) * SEC + int(round(seconds * 1000)) * MS


def test_the_day_epoch_helper_agrees_with_the_pinned_constant():
    assert ns_on(DAY) == D29
    assert ns_on("20260730") == D29 + 24 * 3600 * SEC


# ---------------------------------------------------------------------------
# builders
# ---------------------------------------------------------------------------


def bbo(rows) -> "rm.BboSeries":
    """`(ts, bid_px, bid_sz, bid_n, ask_px, ask_sz, ask_n)` -> a series."""
    b = rm.BboBuilder()
    for row in rows:
        b.add(*row)
    return b.finish()


def quotes(rows) -> "rm.BboSeries":
    """`(ts, bid_px, ask_px)` -> a series; sizes and counts are 1."""
    return bbo([(ts, bid, 1.0, 1, ask, 1.0, 1) for ts, bid, ask in rows])


def book(snapshots) -> "rm.BookSeries":
    """`(ts, [(bid_px, bid_sz), ...], [(ask_px, ask_sz), ...])` -> a series."""
    b = rm.BookBuilder()
    for ts, bids, asks in snapshots:
        b.add(ts, bids, asks)
    return b.finish()


# ---------------------------------------------------------------------------
# file fixtures (the collector's line format)
# ---------------------------------------------------------------------------


def write_gz(path: Path, records) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with gzip.open(path, "wb") as f:
        for ts, obj in records:
            f.write(f"{ts} {json.dumps(obj)}\n".encode())


def hl_bbo(coin, ts, bid, ask, bid_sz="1.0", ask_sz="1.0", bid_n=1, ask_n=1):
    return {
        "channel": "bbo",
        "data": {
            "coin": coin,
            "time": ts // MS,
            "bbo": [
                {"px": bid, "sz": bid_sz, "n": bid_n},
                {"px": ask, "sz": ask_sz, "n": ask_n},
            ],
        },
    }


def hl_l2(coin, ts, bids, asks, fast):
    data = {
        "coin": coin,
        "time": ts // MS,
        "levels": [
            [{"px": px, "sz": sz, "n": 1} for px, sz in bids],
            [{"px": px, "sz": sz, "n": 1} for px, sz in asks],
        ],
    }
    if fast:
        data["fast"] = True
    return {"channel": "l2Book", "data": data}


def hl_trade(coin, ts):
    return {
        "channel": "trades",
        "data": [{"coin": coin, "side": "A", "px": "100.00", "sz": "1.0", "time": ts // MS}],
    }


def um_book_ticker(symbol, ts, u, bid, ask, bid_sz="1", ask_sz="1"):
    return {
        "stream": f"{symbol.lower()}@bookTicker",
        "data": {
            "e": "bookTicker",
            "u": u,
            "s": symbol.upper(),
            "b": bid,
            "B": bid_sz,
            "a": ask,
            "A": ask_sz,
            "T": ts // MS,
            "E": ts // MS,
        },
    }


def um_premium_index(symbol, ts):
    """A REST poller element: no `stream`, no `e`. Must be ignored, not counted."""
    return {
        "symbol": symbol.upper(),
        "markPrice": "100.00",
        "indexPrice": "100.00",
        "estimatedSettlePrice": "100.00",
        "lastFundingRate": "0.00005",
        "interestRate": "0.0001",
        "nextFundingTime": ts // MS,
        "time": ts // MS,
    }


def a_day(tmp_path, hl_records, um_records, coin="PUMP", symbol="PUMPUSDT", day=DAY):
    """Writes one HL file and one UM file, returns `(hl_dir, um_dir)`."""
    hl_dir = tmp_path / "hyperliquid"
    um_dir = tmp_path / "binancefuturesum"
    write_gz(hl_dir / f"{coin.lower()}_{day}.gz", hl_records)
    write_gz(um_dir / f"{symbol.lower()}_{day}.gz", um_records)
    return hl_dir, um_dir


# ---------------------------------------------------------------------------
# 0. exact prices
# ---------------------------------------------------------------------------


def test_prices_are_scaled_to_exact_integers():
    """A tick of 1e-6 on a 0.001816 price is not representable in float64.

    The whole tick metric is a mode over price *differences*: in float those
    differences come out as 9.999999999e-07 and 1.0000000002e-06 and the mode of
    a few hundred thousand of them is meaningless. Scaled integers make the
    difference exact, so the mode is a count of identical values.
    """
    assert rm.scaled_px("0.001816") == 18_160_000
    assert rm.scaled_px("0.001817") - rm.scaled_px("0.001816") == rm.scaled_px("0.000001")
    assert rm.scaled_px("21.2") == 212_000_000_000
    assert rm.scaled_px("63963.0") == 639_630_000_000_000


# ---------------------------------------------------------------------------
# 1. tick
# ---------------------------------------------------------------------------


def test_the_tick_is_the_mode_of_the_gaps_between_distinct_best_bids():
    """Repeats contribute nothing and a two-tick jump does not win over ten
    one-tick steps."""
    series = ["100.00", "100.00", "100.01", "100.01", "100.02", "100.04", "100.03"]
    counts = rm.price_delta_counts(np.array([rm.scaled_px(p) for p in series], dtype=np.int64))
    # distinct: 100.00 100.01 100.02 100.04 100.03 -> +1 +1 +2 (-1 dropped)
    assert counts == {rm.scaled_px("0.01"): 2, rm.scaled_px("0.02"): 1}
    assert rm.mode_delta(counts) == rm.scaled_px("0.01")


def test_a_side_the_book_emptied_does_not_invent_a_price_gap():
    """A missing level is not a price of zero.

    `l2Book` can arrive with a side empty (measured on thin coins), and a zero in
    the middle of a price series produces one enormous positive delta on the way
    back out of it.
    """
    px = np.array([rm.scaled_px("100.00"), 0, rm.scaled_px("100.01")], dtype=np.int64)
    assert rm.price_delta_counts(px) == {rm.scaled_px("0.01"): 1}


def test_the_five_significant_figure_formula_gives_the_documented_tick():
    """§2.1: max 5 significant figures, so the tick is 10^(floor(log10 px) - 4)."""
    assert rm.formula_tick(100.0) == pytest.approx(0.01)
    assert rm.formula_tick(21.2) == pytest.approx(0.001)
    assert rm.formula_tick(63963.0) == pytest.approx(1.0)
    assert rm.formula_tick(0.001816) == pytest.approx(1e-7)


def test_a_tick_the_formula_gets_wrong_is_flagged_and_the_empirical_one_wins():
    """PUMP's real shape: the formula's 1e-7 is beaten by the szDecimals floor.

    §2.1's tick is `max(10^-(6 - szDecimals), 10^(floor(log10 px) - 4))` and the
    left term is not in a recording, so the formula alone is wrong wherever it
    binds. The recording settles it: the mismatch is reported and the measured
    tick is what every downstream number uses.
    """
    series = quotes(
        [
            (ns(0), "0.001816", "0.001817"),
            (ns(1), "0.001817", "0.001818"),
            (ns(2), "0.001818", "0.001819"),
            (ns(3), "0.001817", "0.001818"),
        ]
    )
    out = rm.summarize_tick(
        rm.price_delta_counts(series.bid_px),
        rm.price_delta_counts(series.ask_px),
        series.mid,
    )
    assert out["empirical"] == pytest.approx(1e-6)
    assert out["formula"] == pytest.approx(1e-7)
    assert out["mismatch"] is True
    assert out["used"] == pytest.approx(1e-6)
    assert out["used_source"] == "empirical"


def test_the_ask_side_validates_the_bid_side_tick():
    series = quotes([(ns(i), f"{100 + i * 0.01:.2f}", f"{100.02 + i * 0.01:.2f}") for i in range(6)])
    out = rm.summarize_tick(
        rm.price_delta_counts(series.bid_px),
        rm.price_delta_counts(series.ask_px),
        series.mid,
    )
    assert out["empirical"] == pytest.approx(0.01)
    assert out["empirical_from_ask"] == pytest.approx(0.01)
    assert out["sides_agree"] is True
    assert out["mismatch"] is False


def test_a_book_that_never_moves_leaves_the_tick_to_the_formula():
    series = quotes([(ns(i), "100.00", "100.02") for i in range(4)])
    out = rm.summarize_tick(
        rm.price_delta_counts(series.bid_px),
        rm.price_delta_counts(series.ask_px),
        series.mid,
    )
    assert out["empirical"] is None
    assert out["used"] == pytest.approx(0.01)
    assert out["used_source"] == "formula"
    assert out["mismatch"] is False


def a_price_decade_crossing() -> "rm.BboSeries":
    """1800s quoted at 0.9500 then 1800s at 1.050, one tick wide throughout.

    Two different ticks — 1e-4 below 1.0 and 1e-3 above it — so no single number
    describes the row. The coin is one tick wide for the whole hour, so the §9.3
    gate `frac_time_ge_3_ticks` is truly 0.0 and `frac_time_at_1_tick` truly 1.0.
    """
    rows = []
    for i in range(1800):
        bid = 0.9500 + 0.0001 * (i % 3)
        rows.append((ns(i), f"{bid:.4f}", f"{bid + 0.0001:.4f}"))
    for i in range(1800):
        bid = 1.050 + 0.001 * (i % 3)
        rows.append((ns(1800 + i), f"{bid:.3f}", f"{bid + 0.001:.3f}"))
    return quotes(rows)


def test_a_row_whose_prices_cross_a_price_decade_says_so():
    """One tick per row is a model, and it is wrong across a decade boundary.

    Reproduced: the row reports `frac_time_at_1_tick = 0.5001` where the truth is
    1.0 and `frac_time_ge_3_ticks = 0.4999` where the truth is 0.0 — the §9.3
    gate number inverted — with `mismatch: false`, because the median price sits
    at 1.0004 and the formula's 1e-4 agrees with the mode of the more populous
    half. Nothing else in the row moves, so the fraction of the session spent in
    another decade is the only thing that can warn a reader off the number.
    """
    series = a_price_decade_crossing()
    out = rm.summarize_tick(
        rm.price_delta_counts(series.bid_px),
        rm.price_delta_counts(series.ask_px),
        series.mid,
        weight=series.weight,
    )
    assert out["mismatch"] is False, "the existing flag cannot see this"
    assert out["frac_time_off_median_decade"] == pytest.approx(0.5, abs=0.01)
    # And the gate number it invalidates, so the reason for the flag is on record.
    spread = rm.summarize_spread(series, int(round(out["used"] * rm.PX_SCALE)))
    assert spread["frac_time_ge_3_ticks"] == pytest.approx(0.5, abs=0.01)


def test_a_row_that_stays_inside_one_decade_is_not_flagged():
    series = quotes([(ns(i), f"{100.00 + 0.01 * (i % 3):.2f}",
                      f"{100.01 + 0.01 * (i % 3):.2f}") for i in range(30)])
    out = rm.summarize_tick(
        rm.price_delta_counts(series.bid_px),
        rm.price_delta_counts(series.ask_px),
        series.mid,
        weight=series.weight,
    )
    assert out["frac_time_off_median_decade"] == pytest.approx(0.0)


def test_the_decade_crossing_reaches_the_operator_as_a_warning(tmp_path):
    """A number nobody reads is not a flag.

    `mode_share` was already a trace of this and it is not in the text table, so
    the operator's view showed nothing at all. The warnings block is in it.
    """
    series = a_price_decade_crossing()
    hl_dir, um_dir = a_day(
        tmp_path,
        [(int(t), hl_bbo("PUMP", int(t), f"{b / rm.PX_SCALE:.4f}", f"{a / rm.PX_SCALE:.4f}"))
         for t, b, a in zip(series.ts, series.bid_px, series.ask_px)],
        [(ns(0), um_book_ticker("PUMPUSDT", ns(0), 1, "1.0000", "1.0001"))],
    )
    report = rm.build_report(hl_dir, "PUMP", [um_dir], "PUMPUSDT", [DAY])
    row = report["rows"][0]
    assert row["tick"]["frac_time_off_median_decade"] == pytest.approx(0.5, abs=0.01)
    assert any("decade" in w for w in row["warnings"]), row["warnings"]
    assert "decade" in rm.render_text(report)


def test_a_day_with_no_frames_at_all_claims_no_tick_from_anywhere():
    """A missing day is a row of nulls, not a row that names a source it has not.

    Reachable from the CLI: `--day` accepts a day the recording does not cover,
    which is how a fourteen-day run reports the days it is missing.
    """
    out = rm.summarize_tick(Counter(), Counter(), np.zeros(0))
    assert out["used"] is None
    assert out["used_source"] is None
    assert out["mismatch"] is False


# ---------------------------------------------------------------------------
# 2. spread, time weighted
# ---------------------------------------------------------------------------


def test_the_time_weighted_median_is_the_weighted_middle_not_the_middle_value():
    values = np.array([1.0, 2.0, 3.0])
    weights = np.array([1.0, 1.0, 10.0])
    assert rm.time_weighted_median(values, weights) == 3.0
    assert rm.time_weighted_median(values, np.array([10.0, 1.0, 1.0])) == 1.0


def test_the_one_tick_fraction_is_time_weighted_not_frame_weighted():
    """One long frame at one tick against three short ones at three.

    By frames the book is at one tick a quarter of the time; by time it is half.
    The §9.3 gate is about *most of the session*, so only the second reading can
    answer it, and a frame-count fraction would report 0.25 here.
    """
    series = quotes(
        [
            (ns(0), "100.00", "100.01"),  # 1 tick, alive 3s
            (ns(3), "100.00", "100.03"),  # 3 ticks, alive 1s
            (ns(4), "100.00", "100.03"),  # 3 ticks, alive 1s
            (ns(5), "100.00", "100.03"),  # 3 ticks, alive 1s
            (ns(6), "100.00", "100.03"),  # last frame: no next, no weight
        ]
    )
    out = rm.summarize_spread(series, rm.scaled_px("0.01"))
    assert out["frames_weighted"] == 4
    assert out["frac_time_at_1_tick"] == pytest.approx(0.5)
    assert out["frac_time_ge_3_ticks"] == pytest.approx(0.5)
    # Exactly half the weight sits at one tick and half at three. The definition
    # — the smallest value whose cumulative weight reaches half — resolves the
    # tie downwards, and a metric that reads "3 ticks" off a book that is at one
    # tick half the time would be the more flattering of the two answers.
    assert out["p50_ticks"] == pytest.approx(1.0)


def test_a_feed_gap_cannot_dominate_the_time_weighted_spread():
    """The weight of a frame is capped at 5s.

    Without the cap a single quote sitting across a 100-second hole would carry
    99% of the day's weight, and the metric would describe the outage rather than
    the book. Uncapped this fixture reads 0.990; capped it reads 5/6.
    """
    series = quotes(
        [
            (ns(0), "100.00", "100.01"),  # 1 tick, next frame 100s later
            (ns(100), "100.00", "100.03"),  # 3 ticks, alive 1s
            (ns(101), "100.00", "100.03"),
        ]
    )
    out = rm.summarize_spread(series, rm.scaled_px("0.01"))
    assert out["frac_time_at_1_tick"] == pytest.approx(5.0 / 6.0)
    assert out["weighted_seconds"] == pytest.approx(6.0)


def test_the_median_spread_is_reported_in_both_bps_and_ticks():
    series = quotes([(ns(0), "100.00", "100.02"), (ns(1), "100.00", "100.02"), (ns(2), "100.00", "100.02")])
    out = rm.summarize_spread(series, rm.scaled_px("0.01"))
    # 0.02 on a mid of 100.01 -> 1.9998 bps
    assert out["p50_bps"] == pytest.approx(0.02 / 100.01 * 1e4)
    assert out["p50_ticks"] == pytest.approx(2.0)


def test_a_crossed_quote_is_counted_rather_than_hidden():
    series = quotes([(ns(0), "100.03", "100.01"), (ns(1), "100.00", "100.02"), (ns(2), "100.00", "100.02")])
    out = rm.summarize_spread(series, rm.scaled_px("0.01"))
    assert out["crossed_or_zero_frames"] == 1


# ---------------------------------------------------------------------------
# 3. conditional spread curve
# ---------------------------------------------------------------------------


def two_regime_bbo(calm_windows=5, wild_windows=5):
    """Ten 60s windows: five calm (1 tick, tiny mid jiggle), five wild (5 ticks).

    Frames every 200ms and a 1s sampling grid, so the grid lands on every fifth
    frame; five is odd, so consecutive samples alternate between the two mids and
    every return is `±log(1 + amp/mid_low)`. The *sample* standard deviation of
    59 such alternating returns is that times `_SAMPLE_STD_FACTOR` — see
    `_regime_vol`, which is what the vol assertions use.
    """
    rows = []
    for w in range(calm_windows + wild_windows):
        wild = w >= calm_windows
        amp = 0.40 if wild else 0.01
        spread = 0.05 if wild else 0.01
        for i in range(300):  # 300 * 200ms = 60s
            t = ns(w * 60 + i * 0.2)
            bid = 100.0 + amp * (i % 2)
            rows.append((t, f"{bid:.2f}", f"{bid + spread:.2f}"))
    rows.append((ns((calm_windows + wild_windows) * 60), "100.00", "100.01"))
    return quotes(rows)


#: The 60 grid points of a window give 59 returns, and they alternate `+r, -r`,
#: so the *sample* standard deviation is `r * sqrt((n - 1/n) / (n - 1))` and not
#: `r`. That factor is 1.00844 here — small enough that an eyeballed assertion
#: would have been written as `approx(r)` and would then have survived dropping
#: the `n - 1` divisor altogether.
_ALTERNATING_N = 59
_SAMPLE_STD_FACTOR = math.sqrt((_ALTERNATING_N - 1.0 / _ALTERNATING_N) / (_ALTERNATING_N - 1))


def _regime_vol(amp: float, spread: float) -> float:
    """The analytic realized vol of one `two_regime_bbo` window."""
    low = 100.0 + spread / 2.0
    return abs(math.log((low + amp) / low)) * _SAMPLE_STD_FACTOR


def test_the_realized_vol_of_a_window_is_the_sample_std_of_its_log_returns():
    """The published `vol_by_quintile` numbers, pinned to arithmetic.

    Without this the quintile *ordering* is checked and the vol *values* are not,
    so the whole variance computation is free: dropping the `n - 1` divisor makes
    every number 7.6x too large, and dividing by `n` instead of `n - 1` makes it
    0.85% too small, and neither shows up in a monotone curve or in a Spearman
    coefficient. The quintile splits ten windows into five pairs, so the middle
    quintile straddles the two regimes and its median is their mean.
    """
    calm = _regime_vol(amp=0.01, spread=0.01)
    wild = _regime_vol(amp=0.40, spread=0.05)
    series = two_regime_bbo()
    windows = rm.vol_windows(series)
    assert sorted(windows.vol)[:5] == pytest.approx([calm] * 5)
    assert sorted(windows.vol)[5:] == pytest.approx([wild] * 5)
    out = rm.conditional_spread_curve(windows, series)
    assert out["vol_by_quintile"] == pytest.approx(
        [calm, calm, (calm + wild) / 2.0, wild, wild]
    )


def test_the_conditional_curve_rises_on_two_regime_data():
    """A book that widens when it moves. The five medians must be monotone."""
    series = two_regime_bbo()
    windows = rm.vol_windows(series)
    assert len(windows.vol) == 10
    out = rm.conditional_spread_curve(windows, series)
    medians = out["p50_bps_by_vol_quintile"]
    assert len(medians) == 5
    assert medians == sorted(medians), medians
    # 5 ticks against 1 tick on the same mid.
    assert out["ratio_q5_q1"] == pytest.approx(5.0, rel=0.05)
    assert out["spearman_vol_spread"] > 0.9
    assert out["windows"] == 10


def test_a_window_with_too_few_grid_samples_is_dropped():
    """30 valid samples of the 60 the 1s grid asks for is the floor.

    A window the feed was mostly absent from has a realized vol that measures the
    hole, and it would land in the calm quintile and drag its median with it.
    """
    rows = [(ns(i), "100.00", "100.01") for i in range(20)]  # 20s of frames, then
    rows += [(ns(60 + i), "100.00", "100.01") for i in range(60)]  # a full window
    windows = rm.vol_windows(quotes(rows))
    # 20 frames plus the 5s the last one may be carried forward = 25 samples < 30.
    assert windows.dropped_windows == 1
    assert len(windows.vol) == 1
    assert windows.start_ts[0] == ns(60)


def test_a_grid_sample_older_than_five_seconds_is_not_valid():
    """LOCF must not invent a price out of a stale quote."""
    grid = np.array([ns(0), ns(3), ns(9)], dtype=np.int64)
    ts = np.array([ns(0)], dtype=np.int64)
    values = np.array([100.0])
    got, valid = rm.grid_locf(ts, values, grid, rm.LOCF_MAX_AGE_NS)
    assert valid.tolist() == [True, True, False]
    assert got[0] == 100.0


def test_fewer_than_five_windows_leaves_the_conditional_curve_null():
    """A quintile split of four windows is not a quintile split of anything."""
    rows = [(ns(i), "100.00", "100.01") for i in range(121)]
    series = quotes(rows)
    windows = rm.vol_windows(series)
    assert len(windows.vol) == 2
    out = rm.conditional_spread_curve(windows, series)
    assert out["p50_bps_by_vol_quintile"] is None
    assert out["ratio_q5_q1"] is None
    assert "windows" in out["note"]


def test_the_curve_states_its_own_caveat():
    """The vol and the spread come from one stream; the output has to say so."""
    out = rm.conditional_spread_curve(rm.vol_windows(two_regime_bbo()), two_regime_bbo())
    assert "bbo" in out["caveat"]
    assert "coupl" in out["caveat"].lower()


# ---------------------------------------------------------------------------
# 4. touch queue
# ---------------------------------------------------------------------------


def test_the_touch_medians_are_reported_per_side_and_pooled():
    series = bbo(
        [
            (ns(0), "100.00", 10.0, 1, "100.01", 100.0, 9),
            (ns(1), "100.00", 30.0, 3, "100.01", 300.0, 7),
            (ns(2), "100.00", 20.0, 2, "100.01", 200.0, 8),
        ]
    )
    out = rm.summarize_touch(series)
    assert out["bid"]["n_orders_p50"] == pytest.approx(2.0)
    assert out["ask"]["n_orders_p50"] == pytest.approx(8.0)
    # px * sz, and the median of {1000, 3000, 2000} is 2000.
    assert out["bid"]["usd_p50"] == pytest.approx(2000.0)
    assert out["ask"]["usd_p50"] == pytest.approx(20002.0)
    # Both sides pooled: {1,2,3,7,8,9} -> 5.0.
    assert out["combined"]["n_orders_p50"] == pytest.approx(5.0)


def test_the_touch_p90s_are_the_ninetieth_percentile_not_the_median():
    """§9.2's metric 4 is "median and p90 of n_orders and USD".

    A p90 that is silently a p50 reads as a much shallower touch than the coin
    has, and the two numbers sit side by side in the JSON looking consistent.
    """
    series = bbo(
        [(ns(i), "100.00", float(i + 1), i + 1, "100.01", float(i + 1), i + 1) for i in range(10)]
    )
    out = rm.summarize_touch(series)
    # n = 1..10: p50 = 5.5, p90 = 9.1 (numpy's linear interpolation).
    assert out["bid"]["n_orders_p50"] == pytest.approx(5.5)
    assert out["bid"]["n_orders_p90"] == pytest.approx(9.1)
    # USD = 100.00 * sz, sz = 1..10.
    assert out["bid"]["usd_p50"] == pytest.approx(550.0)
    assert out["bid"]["usd_p90"] == pytest.approx(910.0)
    assert out["ask"]["n_orders_p90"] == pytest.approx(9.1)
    assert out["ask"]["usd_p90"] == pytest.approx(100.01 * 9.1)
    # Both sides pooled: each value appears twice, which moves neither quantile.
    assert out["combined"]["n_orders_p90"] == pytest.approx(9.1)


def test_the_price_lifetime_is_the_time_until_the_best_price_changes():
    """Named `touch_price_lifetime` because that is what it measures.

    It is not queue-position survival: the recording carries no order ids, so an
    order joining the touch cannot be followed. What it bounds is how long a
    price *level* stays best, which is the most a public feed can say.
    """
    series = quotes(
        [
            (ns(0), "100.00", "100.02"),
            (ns(1), "100.00", "100.02"),
            (ns(2), "100.00", "100.02"),
            (ns(3), "100.01", "100.02"),  # bid changed after 3s
            (ns(4), "100.02", "100.03"),  # and again after 1s
            (ns(5), "100.02", "100.03"),
        ]
    )
    out = rm.summarize_touch(series)
    assert out["touch_price_lifetime"]["bid"]["p50_seconds"] == pytest.approx(2.0)
    assert out["touch_price_lifetime"]["bid"]["runs"] == 2


def test_a_price_run_spanning_a_recording_hole_is_dropped_and_counted():
    """The quote did not survive 100 seconds; the recording was not there.

    Dropped *and counted*: a duration metric that silently discards its longest
    observations is not auditable, and the runs that get discarded are the long
    ones, so the censoring pulls every reported quantile down.
    """
    ts = np.array([ns(0), ns(1), ns(101), ns(102), ns(103)], dtype=np.int64)
    px = np.array([rm.scaled_px(p) for p in ["100.00", "100.00", "100.00", "100.01", "100.02"]])
    life, dropped = rm.price_run_lifetimes(ts, px, rm.LIFETIME_MAX_GAP_NS)
    # Runs: [100.00 x3] runs to the 100.01 frame at 102s and spans the 100s hole
    # on the way, so it is unmeasurable; [100.01] lived 1s.
    assert life.tolist() == [1 * SEC]
    assert dropped.tolist() == [102 * SEC]
    stats = rm._lifetime_stats(life, dropped)
    assert stats["runs"] == 1
    assert stats["runs_dropped_to_gaps"] == 1
    assert stats["seconds_dropped_to_gaps"] == pytest.approx(102.0)


def test_a_quote_that_merely_stood_for_twenty_seconds_is_a_lifetime_not_a_gap():
    """Hyperliquid publishes `bbo` on change, so a standing quote prints nothing.

    Measured on the real 2026-07-29 recording: zero identical consecutive `bbo`
    frames on PUMP or on HYPE, and no hole in either coin's whole day over 60s.
    So a 20-second silence on a thin coin is evidence the quote STOOD — which is
    exactly what this metric measures — and rejecting it as a feed hole censors
    the long tail of the thin coins and only of the thin coins. With the bound at
    5s, PUMP lost 9.7% of its runs holding 44% of its elapsed run time (p90
    20.6s -> 13.5s) while liquid HYPE lost 0.0% and did not move, compressing the
    thin/liquid p50 contrast from 7.9x to 5.6x out of nothing but the filter.
    """
    ts = np.array([ns(0), ns(20), ns(21)], dtype=np.int64)
    px = np.array([rm.scaled_px(p) for p in ["100.00", "100.01", "100.02"]])
    life, dropped = rm.price_run_lifetimes(ts, px, rm.LIFETIME_MAX_GAP_NS)
    assert life.tolist() == [20 * SEC, 1 * SEC]
    assert dropped.tolist() == []


def test_the_lifetime_gap_bound_is_the_recording_hole_bound_not_the_locf_bound():
    """Two different questions, and they had the same answer by accident.

    `LOCF_MAX_AGE_NS` asks "is this quote too stale to sample onto the vol grid";
    this one asks "was the recording there". Tying the second to the first made
    the thinness of a coin decide how much of its own lifetime distribution
    survived.
    """
    assert rm.LIFETIME_MAX_GAP_NS == rm.GAP_REPORT_NS
    assert rm.LIFETIME_MAX_GAP_NS > rm.LOCF_MAX_AGE_NS


def test_the_touch_lifetime_p90_is_the_ninetieth_percentile():
    """Metric 4 is specified as "median and p90"; nothing pinned the p90."""
    ts = np.array([ns(0)] + [ns(sum(range(1, k + 1))) for k in range(1, 11)], dtype=np.int64)
    px = np.array([rm.scaled_px(f"{100.00 + 0.01 * i:.2f}") for i in range(11)])
    life, _dropped = rm.price_run_lifetimes(ts, px, rm.LIFETIME_MAX_GAP_NS)
    # Runs of exactly 1s, 2s, ... 10s.
    assert life.tolist() == [k * SEC for k in range(1, 11)]
    stats = rm._lifetime_stats(life, np.zeros(0, dtype=np.int64))
    assert stats["p50_seconds"] == pytest.approx(5.5)
    assert stats["p90_seconds"] == pytest.approx(9.1)


# ---------------------------------------------------------------------------
# 5. depth at the grid rungs
# ---------------------------------------------------------------------------


#: mid 100.01. 10bps -> [99.91001, 100.11001]; 20bps -> [99.80998, 100.21002].
_SNAP = (
    [("100.00", "10"), ("99.90", "100")],
    [("100.02", "20"), ("100.12", "200")],
)


def test_the_depth_at_a_rung_is_the_thinner_side_summed_inside_the_window():
    series = book([(ns(0), *_SNAP)])
    out = rm.summarize_depth(series)
    # bid: only 100.00 is inside -> 1000; ask: only 100.02 -> 2000.4; min = 1000.
    assert out["d10"] == pytest.approx(1000.0)
    # bid: both -> 1000 + 9990; ask: both -> 2000.4 + 20024; min = 10990.
    assert out["d20"] == pytest.approx(10990.0)
    assert out["d30"] == pytest.approx(10990.0)


def test_the_depth_medians_are_taken_over_snapshots():
    series = book(
        [
            (ns(0), [("100.00", "10")], [("100.02", "10")]),
            (ns(1), [("100.00", "30")], [("100.02", "30")]),
            (ns(2), [("100.00", "20")], [("100.02", "20")]),
        ]
    )
    out = rm.summarize_depth(series)
    assert out["d10"] == pytest.approx(2000.0)
    assert out["snapshots"] == 3


def test_a_snapshot_whose_deepest_level_is_inside_the_window_is_flagged_truncated():
    """§8.7: a truncated book makes every rung below the cut a LOWER BOUND.

    The 5-level `fast` feed cuts at 5 levels, so on a coin whose fifth level is
    still inside +-30bps the reported depth is not the depth.
    """
    series = book([(ns(0), *_SNAP)])
    out = rm.summarize_depth(series)
    # deepest bid 99.90 is outside 10bps but inside 20 and 30.
    assert out["truncated_frac_d10"] == pytest.approx(0.0)
    assert out["truncated_frac_d20"] == pytest.approx(1.0)
    assert out["truncated_frac_d30"] == pytest.approx(1.0)


def test_a_book_truncated_on_only_one_side_is_still_flagged_truncated():
    """The reported depth is `min(bid_side, ask_side)`, so either side truncating
    makes it a lower bound.

    `_SNAP` cannot show this: it truncates on both sides at 20bps and on neither
    at 10bps, so an `and` there, or a bid-only check, reads identically. The
    asymmetric case is the one that matters, because the thinner side is the one
    reported and it is not always the same side.
    """
    # mid 100.01; 20bps -> [99.80998, 100.21002].
    bid_only = book([(ns(0), [("100.00", "10"), ("99.90", "100")],
                      [("100.02", "20"), ("100.50", "200")])])
    ask_only = book([(ns(0), [("100.00", "10"), ("99.00", "100")],
                      [("100.02", "20"), ("100.12", "200")])])
    assert rm.summarize_depth(bid_only)["truncated_frac_d20"] == pytest.approx(1.0)
    assert rm.summarize_depth(ask_only)["truncated_frac_d20"] == pytest.approx(1.0)
    # And a book that reaches past the window on both sides is not truncated.
    neither = book([(ns(0), [("100.00", "10"), ("99.00", "100")],
                     [("100.02", "20"), ("100.50", "200")])])
    assert rm.summarize_depth(neither)["truncated_frac_d20"] == pytest.approx(0.0)


def test_the_slow_book_cross_check_is_taken_at_the_thirty_bps_rung():
    """20 levels reach further, so `d30_slow` is the honest comparison for d30.

    The level that separates the rungs sits between 20 and 30bps deliberately: a
    slow fixture whose second level is inside 10bps sums the same two levels at
    every rung, so `d30_slow` could be computed at any of them and still agree.
    On the real recording this number carries the whole message — HYPE reports
    `d30_slow` 106804 against `d30` 11015, a 10x correction — and a wrong rung
    there is silent.
    """
    # mid 100.01; 20bps -> [99.80998, 100.21002]; 30bps -> [99.69997, 100.32003].
    fast = book([(ns(0), [("100.00", "10")], [("100.02", "10")])])
    slow = book(
        [
            (
                ns(0),
                [("100.00", "10"), ("99.75", "1000")],
                [("100.02", "10"), ("100.27", "1000")],
            )
        ]
    )
    out = rm.summarize_depth(fast, slow)
    assert out["d30"] == pytest.approx(1000.0)
    # Both outer levels are outside 20bps and inside 30bps, so only the 30bps
    # rung sees them: at 10 or 20bps this would read 1000.
    assert out["d30_slow"] == pytest.approx(1000.0 + 99.75 * 1000)
    assert out["snapshots_slow"] == 1


def test_no_fast_snapshots_leaves_the_depth_null():
    out = rm.summarize_depth(book([]))
    assert out["d10"] is None
    assert out["snapshots"] == 0


# ---------------------------------------------------------------------------
# 6. lead-lag
# ---------------------------------------------------------------------------


def shifted_walk(cells=4000, lead_cells=4, seed=7):
    """A mid series both venues print, Binance `lead_cells * 50ms` earlier.

    Hyperliquid's frame at t carries the value Binance printed at `t - lead`, so
    the true lag is `+lead_cells * 50ms` under the documented sign convention.
    """
    rng = np.random.default_rng(seed)
    walk = 100.0 * np.exp(np.cumsum(rng.normal(0.0, 2e-4, cells)))
    grid = D29 + np.arange(cells, dtype=np.int64) * rm.LEADLAG_CELL_NS
    hl_mid = walk[: cells - lead_cells]
    hl_ts = grid[lead_cells:]
    return hl_ts, hl_mid, grid, walk


def test_binance_leading_hyperliquid_by_200ms_is_a_positive_lag():
    """The sign convention, pinned. Positive lag = Binance LEADS.

    Getting this backwards inverts the one conclusion the metric exists for —
    whether a candidate is designated stale liquidity or is leading the move —
    and nothing else in the output would look wrong.
    """
    hl_ts, hl_mid, um_ts, um_mid = shifted_walk(lead_cells=4)
    acc = rm.leadlag_accum(hl_ts, hl_mid, um_ts, um_mid)
    out = rm.summarize_leadlag(acc)
    assert out["lag_ms"] == 200
    assert out["peak_corr"] > 0.9
    assert abs(out["corr_at_lag_0"]) < 0.3
    assert out["sign_convention"].startswith("positive")


def test_hyperliquid_leading_binance_is_a_negative_lag():
    """The mirror image, so the convention cannot be half right."""
    um_ts, um_mid, hl_ts, hl_mid = shifted_walk(lead_cells=3)
    acc = rm.leadlag_accum(hl_ts, hl_mid, um_ts, um_mid)
    out = rm.summarize_leadlag(acc)
    assert out["lag_ms"] == -150


def test_a_simultaneous_pair_peaks_at_lag_zero():
    _hl_ts, _hl_mid, ts, mid = shifted_walk(lead_cells=0)
    acc = rm.leadlag_accum(ts, mid, ts, mid)
    out = rm.summarize_leadlag(acc)
    assert out["lag_ms"] == 0
    assert out["peak_corr"] == pytest.approx(1.0)
    assert out["corr_at_lag_0"] == pytest.approx(1.0)


def test_the_lead_lag_argmax_is_reported_per_quarter():
    hl_ts, hl_mid, um_ts, um_mid = shifted_walk(lead_cells=4)
    out = rm.summarize_leadlag(rm.leadlag_accum(hl_ts, hl_mid, um_ts, um_mid))
    assert out["lag_ms_by_quarter"] == [200, 200, 200, 200]


def test_the_quarters_show_a_lag_that_moved_halfway_through():
    """The stability check has to be able to show instability.

    Pinned only on data whose lag never moves, the quarter machinery could be
    four copies of the whole-day window and no test would notice — and then the
    one output that says "this lag is not a property of the day" would agree with
    itself for free. Here Binance leads for the first half and Hyperliquid leads
    for the second, so the whole-day argmax reports a single number that is true
    of neither half.
    """
    hl_ts, hl_mid, um_ts, um_mid = shifted_walk(cells=4000, lead_cells=4, seed=1)
    # Mirrored, so the roles swap: Hyperliquid leads over the second half.
    led_um_ts, led_um_mid, led_hl_ts, led_hl_mid = shifted_walk(
        cells=4000, lead_cells=4, seed=2
    )
    offset = 4000 * rm.LEADLAG_CELL_NS
    acc = rm.leadlag_accum(
        np.concatenate((hl_ts, led_hl_ts + offset)),
        np.concatenate((hl_mid, led_hl_mid)),
        np.concatenate((um_ts, led_um_ts + offset)),
        np.concatenate((um_mid, led_um_mid)),
    )
    out = rm.summarize_leadlag(acc)
    assert out["lag_ms_by_quarter"] == [200, 200, -200, -200]
    # The whole-day number cannot say this; that is what the quarters are for.
    assert out["lag_ms"] == -200


def test_a_cell_either_venue_is_stale_in_is_dropped():
    """A one-second staleness bound on each side, then the cell is dropped.

    Carrying a stale quote forward produces a zero return, and a run of zeros
    manufactures correlation at every lag out of nothing.
    """
    # HL prints for 10s, then goes silent for 60s, then returns.
    hl_ts = np.array([D29 + i * rm.LEADLAG_CELL_NS for i in range(200)], dtype=np.int64)
    hl_ts = np.append(hl_ts, D29 + 70 * SEC)
    hl_mid = np.append(100.0 + np.arange(200) * 0.001, 105.0)
    um_ts = np.array([D29 + i * rm.LEADLAG_CELL_NS for i in range(1400)], dtype=np.int64)
    um_mid = 100.0 + np.arange(1400) * 0.001
    acc = rm.leadlag_accum(hl_ts, hl_mid, um_ts, um_mid)
    out = rm.summarize_leadlag(acc)
    # 200 HL cells + the 20 cells the last print covers, minus a return each.
    assert out["cells"] < 260
    assert out["cells"] > 180


def test_the_lead_lag_reports_the_cadence_that_bounds_it():
    """A lag is not price discovery on its own.

    A venue publishing its book every 600ms cannot show a move sooner than that,
    whoever discovered it, so a measured lag of that order says as much about the
    feed as about the market. Measured on the real PUMP day: Hyperliquid `bbo`
    arrives every 226ms against Binance's 1ms, and the lag comes out at +800ms —
    3.5 cadences, so not an artefact, but a reader who cannot see the first
    number has no way to know that.
    """
    hl_ts, hl_mid, um_ts, um_mid = shifted_walk(lead_cells=4)
    acc = rm.leadlag_accum(hl_ts, hl_mid, um_ts, um_mid)
    out = rm.summarize_leadlag(acc, cadence_ms=(600.0, 3.0))
    assert out["hl_frame_interval_p50_ms"] == pytest.approx(600.0)
    assert out["um_frame_interval_p50_ms"] == pytest.approx(3.0)
    assert "cadence" in out["caveat"]


def test_too_few_overlapping_cells_leaves_the_lead_lag_null():
    ts = np.array([D29, D29 + rm.LEADLAG_CELL_NS], dtype=np.int64)
    mid = np.array([100.0, 100.1])
    out = rm.summarize_leadlag(rm.leadlag_accum(ts, mid, ts, mid))
    assert out["lag_ms"] is None
    assert out["cells"] < rm.MIN_LEADLAG_CELLS


# ---------------------------------------------------------------------------
# 7. traversals
# ---------------------------------------------------------------------------


def walk(mids, step_seconds=1.0, half_spread=0.01, day=DAY) -> "rm.BboSeries":
    """A `bbo` series whose mid is exactly the given sequence.

    The two sides are placed symmetrically around each mid, so the integer the
    metric reconstructs — the sum of the two scaled sides — is twice the number
    written here and nothing has been lost to the quote.
    """
    return quotes(
        [
            (ns_on(day, i * step_seconds), f"{m - half_spread:.6f}", f"{m + half_spread:.6f}")
            for i, m in enumerate(mids)
        ]
    )


def test_a_square_walk_has_one_traversal_per_move_and_a_round_trip_per_pair():
    """Eight points alternating 50bps apart: seven moves, seven traversals, three
    round trips.

    A one-rung grid at those two prices earns the rung on each completed
    down-then-up excursion — moves 2-3, 4-5 and 6-7. The opening up-move is not
    one, since nothing had traversed down to the rung yet, and pairing from the
    other end leaves the seventh move dangling instead. Either way seven
    traversals are three round trips and never four. Every spacing at or under the
    amplitude sees the same walk.
    """
    series = walk([100.00, 100.50] * 4)
    t = rm.traversal_of(series)
    for spacing in rm.TRAVERSAL_SPACINGS_BPS:
        assert t.traversals[spacing] == 7, spacing
        assert t.round_trips[spacing] == 3, spacing


def test_a_monotone_trend_is_zero_round_trips_and_a_path_equal_to_its_net():
    """Grid poison: 300bps of movement, not one rung round-tripped.

    The one traversal is the move away from the opening price, confirmed as soon
    as the price is a spacing above where the session started. After that the
    running high simply keeps extending and nothing ever confirms a turn.
    """
    series = walk([100.0 + 0.1 * i for i in range(31)])
    t = rm.traversal_of(series)
    for spacing in rm.TRAVERSAL_SPACINGS_BPS:
        assert t.traversals[spacing] == 1, spacing
        assert t.round_trips[spacing] == 0, spacing
    out = rm.summarize_traversal(t)
    assert out["path_bps"] == pytest.approx(300.0)
    assert out["net_bps"] == pytest.approx(300.0)
    assert out["path_efficiency"] == pytest.approx(1.0)


def test_a_traversal_counts_only_where_the_opposite_move_confirms_it():
    """Causality. A 30bps fall that never comes back is not a round trip: the
    grid bought and is still holding."""
    down = walk([100.00, 99.90, 99.80, 99.70])
    assert rm.traversal_of(down).traversals[20] == 1
    assert rm.traversal_of(down).round_trips[20] == 0
    back = walk([100.00, 99.90, 99.80, 99.70, 99.90, 100.00])
    assert rm.traversal_of(back).traversals[20] == 2
    assert rm.traversal_of(back).round_trips[20] == 1


def test_no_traversal_is_counted_before_the_frame_that_confirms_it():
    """The scan is causal: a prefix of the session holds exactly the traversals
    confirmed inside it.

    An implementation reaching for the session's global high and low — the
    natural way to write this wrong — passes every count above and fails here,
    because it would credit frame 2 with a fall it only learns about at frame 3.
    """
    mids = [100.00, 99.90, 99.80, 99.70, 99.90, 100.00]
    counts = [rm.traversal_of(walk(mids[:k])).traversals[20] for k in range(1, len(mids) + 1)]
    assert counts == [0, 0, 1, 1, 2, 2]


def test_the_threshold_is_an_exact_integer_comparison_at_the_boundary():
    """Ten bps of 10_000 units is 10 units, and `>=` means the tenth one counts.

    The primitive takes prices in any fixed scale — the threshold is relative, so
    a common factor cancels — which is what lets the series-level metric hand it
    the exact integer `bid + ask` instead of a float mid.
    """
    assert rm.traversals_at([10_000, 10_010], 10) == 1
    assert rm.traversals_at([10_000, 10_009], 10) == 0


def test_a_move_far_enough_to_overflow_int64_still_counts():
    """The cross-multiplication leaves `int64` on a six-figure coin.

    `(extreme - px) * 10_000` on a mid held at `PX_SCALE` passes 9.2e18 once the
    move is large, and numpy wraps around silently rather than raising. The scan
    runs on Python ints, which cannot.
    """
    high = 2 * rm.scaled_px("120000.0")  # a six-figure mid, as bid + ask
    low = high // 2  # halved, which is 5000bps and a traversal at any spacing
    assert (high - low) * 10_000 > np.iinfo(np.int64).max
    assert rm.traversals_at([high, low], 30) == 1
    assert rm.traversals_at(np.array([high, low], dtype=np.int64), 30) == 1


def test_the_return_leg_is_measured_against_the_price_it_starts_from():
    """A threshold in bps is asymmetric in price: 10bps of the high is more
    absolute movement than 10bps of the low, so a fall back to where an up-move
    started is 9.99bps and not a traversal. One unit further is."""
    assert rm.traversals_at([10_000, 10_010, 10_000], 10) == 1
    assert rm.traversals_at([10_000, 10_010, 9_999], 10) == 2
    assert rm.round_trips_at([10_000, 10_010, 9_999], 10) == 1


def test_a_move_of_exactly_the_spacing_counts_where_float_arithmetic_loses_it():
    """One 1e-6 tick on a mid of exactly 0.001 is exactly 10.000bps.

    A whole-bps threshold is an exact tie wherever the tick divides the price that
    way, which is not a contrivance on a venue whose tick is a fixed fraction of
    the quote: PUMP is quoted in the 0.001s on a 1e-6 tick. In float64 the same
    comparison comes out just under and the traversal is lost — the last assertion
    is that arithmetic, spelled out. In the scaled integers it is a tie, and `>=`
    counts it.
    """
    series = quotes(
        [
            (ns(0), "0.000999", "0.001001"),  # mid 0.001000 exactly
            (ns(1), "0.000998", "0.001000"),  # one tick lower: exactly 10bps
        ]
    )
    t = rm.traversal_of(series)
    assert t.traversals[10] == 1
    assert t.traversals[20] == 0
    assert (0.002 - 0.001998) * 1e4 < 10 * 0.002  # the float route, and it is wrong


def test_a_wider_spacing_can_only_ever_see_fewer_traversals():
    """A walk carrying wiggles of every size: each spacing filters the one below
    it, so the counts fall monotonically and no two of them are the same."""
    mids = [100.0]
    for step in (0.30, -0.50, 0.08, -0.08, 0.22, -0.12, 0.40, -0.60, 0.07, -0.07,
                 0.35, -0.20, 0.15, -0.45, 0.13, -0.13, 0.25):
        mids.append(round(mids[-1] + step, 2))
    t = rm.traversal_of(walk(mids))
    counts = [t.traversals[s] for s in rm.TRAVERSAL_SPACINGS_BPS]
    assert counts == [17, 13, 7, 6]
    assert counts == sorted(counts, reverse=True)


def test_round_trips_are_reported_per_session_and_per_hour():
    """Half an hour of a walk is comparable with a whole day of one only through
    the per-hour column, and a recording that started at 21:00 is the normal case
    here — the 2026-07-29 run did."""
    series = walk([100.00, 100.50] * 4 + [100.00], step_seconds=225.0)  # 8 moves, 1800s
    out = rm.summarize_traversal(rm.traversal_of(series))
    assert out["seconds"] == pytest.approx(1800.0)
    assert out["hours"] == pytest.approx(0.5)
    cell = out["by_spacing"]["30"]
    assert cell["round_trips"] == 4
    assert cell["round_trips_per_hour"] == pytest.approx(8.0)


def test_the_gross_capture_ceiling_is_the_round_trips_times_the_spacing():
    """And says in the output that it is a ceiling: it credits every round trip
    with the full rung and charges nothing for fees, adverse selection or the
    queue the rung never reached."""
    series = walk([100.00, 100.50] * 4 + [100.00], step_seconds=225.0)
    out = rm.summarize_traversal(rm.traversal_of(series))
    for spacing in rm.TRAVERSAL_SPACINGS_BPS:
        cell = out["by_spacing"][str(spacing)]
        assert cell["gross_capture_potential_bps"] == pytest.approx(cell["round_trips"] * spacing)
        assert cell["gross_capture_potential_bps_per_hour"] == pytest.approx(
            cell["round_trips_per_hour"] * spacing
        )
    assert out["by_spacing"]["30"]["gross_capture_potential_bps"] == pytest.approx(120.0)
    assert "ceiling" in out["caveat"].lower()


def test_path_efficiency_is_the_walked_distance_over_the_net_move():
    """Up 100bps, back 60, up 20: 180bps walked to get 60bps done."""
    series = walk([100.00, 101.00, 100.40, 100.60])
    out = rm.summarize_traversal(rm.traversal_of(series))
    assert out["path_bps"] == pytest.approx(180.0)
    assert out["net_bps"] == pytest.approx(60.0)
    assert out["path_efficiency"] == pytest.approx(3.0)


def test_a_session_that_ends_where_it_started_reports_no_ratio():
    """Pure oscillation is the limit case of grid food, and its ratio is a
    division by zero — which a JSON report may not carry."""
    series = walk([100.00, 100.50, 100.00])
    out = rm.summarize_traversal(rm.traversal_of(series))
    assert out["path_bps"] == pytest.approx(100.0)
    assert out["net_bps"] == 0.0
    assert out["path_efficiency"] is None


def test_a_falling_session_reports_a_signed_net_and_an_unsigned_ratio():
    series = walk([100.00, 99.00, 99.40])
    out = rm.summarize_traversal(rm.traversal_of(series))
    assert out["net_bps"] == pytest.approx(-60.0)
    assert out["path_bps"] == pytest.approx(140.0)
    assert out["path_efficiency"] == pytest.approx(140.0 / 60.0)


def test_a_repeated_quote_changes_nothing():
    """A venue that publishes on change still repeats a quote after a reconnect,
    and a standing price is not movement."""
    plain = rm.traversal_of(walk([100.00, 100.50, 100.00, 100.50]))
    repeated = rm.traversal_of(walk([100.00, 100.00, 100.50, 100.50, 100.00, 100.00, 100.50]))
    assert plain.traversals == repeated.traversals
    assert plain.path_bps == pytest.approx(repeated.path_bps)


def test_a_quote_missing_a_side_is_not_a_price_halving():
    """An empty side arrives as a zero and a mid taken from it is half the price —
    a 5000bps round trip that never happened. Skipped, as the tick metric skips
    them."""
    series = quotes(
        [
            (ns(0), "100.00", "100.02"),
            (ns(1), "0", "100.02"),
            (ns(2), "100.00", "100.02"),
        ]
    )
    t = rm.traversal_of(series)
    assert t.frames == 2
    assert t.traversals[30] == 0
    assert t.path_bps == pytest.approx(0.0)


def test_a_day_with_nothing_in_it_traverses_nothing():
    """No frames, and one frame: neither is a division by zero and neither is a
    NaN in the JSON."""
    for series in (bbo([]), quotes([(ns(0), "100.00", "100.02")])):
        out = rm.summarize_traversal(rm.traversal_of(series))
        assert out["seconds"] == 0.0
        assert out["path_bps"] == 0.0
        assert out["path_efficiency"] is None
        assert out["by_spacing"]["10"]["round_trips"] == 0
        assert out["by_spacing"]["10"]["round_trips_per_hour"] is None
        assert out["by_spacing"]["10"]["gross_capture_potential_bps"] == 0.0
        assert out["by_spacing"]["10"]["round_trips_per_hour_min"] is None
        assert out["hour_blocks"] == 0
        assert out["frames_per_hour"] is None


# --- the state machine itself, pinned by mutation ---------------------------
#
# The four tests below exist because the fixtures above cannot fail without
# them. Every synthetic traversal fixture in this file is either a two-level
# square walk — where the running extreme can never reach past the price that
# confirmed the turn, and the extreme before the turn already equals the price
# the walk returns to — or a monotone trend, which never turns at all. Both
# shapes are blind by construction to how the scan carries its state, and the
# real two-minute fixture at the end of this file was asserted only as
# `round_trips >= 1`.
#
# Measured by mutating `traversals_at` and running this whole file (AGENTS.md
# §1.3, §4.2's precedent): freezing the running high in the `trend > 0` branch,
# keeping the older extreme instead of the confirming price, and either `>=`
# turned into `>` all survived a green suite, while changing every published
# count on real recordings by up to -99.5% and +108%. Each assertion below is
# the smallest exact-integer path that separates the shipped scan from one of
# those mutants; the prices are unit-free, since the threshold is relative.
#
# Ten of the scan's twelve branch mutants now fail. The two that do not are
# equivalent, not uncovered: in the neutral branch the confirming price is
# always already the running extreme, so `low = px` and `low = min(low, px)`
# cannot differ there. Proof: if the down-check fires at a price P inside
# [L, H], then whichever of the two extremes was set later would itself have
# fired when it arrived, so (H-L) is inside the threshold and P < L —
# contradiction. Brute-forced over 200k random walks x 6 spacings as well: zero
# differences. Do not go looking for a test for those two.


def test_the_running_extreme_extends_while_the_trend_holds():
    """The next leg is measured from the furthest price since the turn.

    Not from the price that confirmed the turn: the third point here extends the
    extreme, and the fourth clears the threshold from the extended one and from
    nothing else. Freezing the running high collapses PUMP 20260730 at 5bps from
    1709 round trips to 8.
    """
    # up: +10bps confirms, +20bps extends the high, then 10bps back off THAT.
    assert rm.traversals_at([100_000, 100_100, 100_200, 100_099], 10) == 2
    # and the mirror, which the `trend < 0` branch already had covered.
    assert rm.traversals_at([100_000, 99_900, 99_800, 99_901], 10) == 2
    # one unit short of the extended extreme is not a turn either way.
    assert rm.traversals_at([100_000, 100_100, 100_200, 100_100], 10) == 1
    # 99_900 would be one, the return leg being cheaper in price than the way
    # down — 10bps of the low, not of the high (see the asymmetry test above).
    assert rm.traversals_at([100_000, 99_900, 99_800, 99_899], 10) == 1


def test_the_confirming_price_replaces_the_extreme_it_confirmed_against():
    """The turn starts the next leg from where it was confirmed, not from the
    old extreme.

    Keeping the older one (`low = min(low, px)`) starts the next leg further
    away and confirms it on a smaller move — +108% round trips on PUMP 20260730
    at 5bps. The last price here is inside the threshold of the confirming price
    and outside the threshold of the extreme before it.
    """
    assert rm.traversals_at([100_000, 99_800, 99_900, 100_000, 99_899, 99_998], 10) == 3
    assert rm.traversals_at([100_000, 100_200, 100_100, 100_000, 100_101, 100_002], 10) == 3


def test_a_leg_of_exactly_the_spacing_confirms_in_every_state_of_the_scan():
    """`>=`, and in all three states — not only the opening one.

    The existing boundary test enters the scan neutral, so both mid-scan
    thresholds could be `>` with every other test in this file still green.
    """
    # trend > 0: exactly 10bps below the running high, and one unit short of it.
    assert rm.traversals_at([99_000, 100_000, 99_900], 10) == 2
    assert rm.traversals_at([99_000, 100_000, 99_901], 10) == 1
    # trend < 0: exactly 10bps above the running low, and one unit short.
    assert rm.traversals_at([101_000, 100_000, 100_100], 10) == 2
    assert rm.traversals_at([101_000, 100_000, 100_099], 10) == 1


def test_the_extreme_extends_before_the_first_turn_as_well():
    """Both extremes are tracked from the opening quote until one of them fires.

    The intermediate price is what makes the last one a traversal: on its own it
    is half the threshold from the open.
    """
    assert rm.traversals_at([100_000, 100_050, 99_949], 10) == 1
    assert rm.traversals_at([100_000, 99_949], 10) == 0
    assert rm.traversals_at([100_000, 99_950, 100_051], 10) == 1
    assert rm.traversals_at([100_000, 100_051], 10) == 0


# --- what path_bps is, and what it is not -----------------------------------


def test_flicker_between_the_same_turns_moves_path_bps_and_no_round_trip():
    """`path_bps` is the first-order variation of a *sampled* path, so it counts
    the frames it was summed over.

    Both series here hold the same four excursions over the same 404 seconds and
    turn at the same four prices; the second one simply prints more often, with a
    1bp shiver far below any spacing. Nothing tradeable differs, and `path_bps`
    triples. On a real recording it does not converge either: LOCF-resampling VVV
    20260730 from 4s down to the raw feed took `path_bps` from 31936 to 90183,
    still climbing at the finest grid, while round trips at 30bps settled at
    75-76 from 1s down. `bbo` frame counts across the shortlist differ 5x, which
    is why the row also reports `frames_per_hour`: the two numbers must be read
    together or not at all.
    """
    turns = [100.00, 100.50, 100.00, 100.50, 100.00]
    shivered = []
    for i, px in enumerate(turns):
        shivered.append(px)
        if i < len(turns) - 1:
            aside = round(px + (0.01 if turns[i + 1] > px else -0.01), 2)
            shivered += [aside, px] * 50

    coarse = rm.summarize_traversal(rm.traversal_of(walk(turns, step_seconds=101.0)))
    fine = rm.summarize_traversal(rm.traversal_of(walk(shivered, step_seconds=1.0)))

    assert coarse["seconds"] == fine["seconds"] == pytest.approx(404.0)
    assert coarse["frames"] == 5
    assert fine["frames"] == 405
    assert coarse["path_bps"] == pytest.approx(200.0)  # 4 legs of 50bps
    assert fine["path_bps"] == pytest.approx(600.0)  # + 50 shivers of 2bps each
    assert coarse["frames_per_hour"] == pytest.approx(5 / (404 / 3600))
    assert fine["frames_per_hour"] == pytest.approx(405 / (404 / 3600))
    for spacing in rm.TRAVERSAL_SPACINGS_BPS:
        assert coarse["by_spacing"][str(spacing)]["round_trips"] == 2, spacing
        assert fine["by_spacing"][str(spacing)]["round_trips"] == 2, spacing


def test_path_efficiency_ranks_the_still_day_above_the_oscillating_one():
    """Which is why it is not the oscillation number and the row must not be read
    as if it were.

    `path / |net|` is dominated by its denominator: the first series here round
    trips 50bps nineteen times and then drifts 500bps away, the second wanders
    10bps and comes back to within 0.01bp of where it opened. The second scores
    two hundred times higher while holding not one round trip at any tradeable
    spacing. Measured across the ten coin-days of 20260730 the same way:
    spearman(path_efficiency, 1/|net_bps|) = +0.79, spearman(path_efficiency,
    round trips per hour at 10bps) = -0.13.
    """
    oscillating = rm.summarize_traversal(
        rm.traversal_of(walk([100.00, 100.50] * 20 + [105.00]))
    )
    still = rm.summarize_traversal(rm.traversal_of(walk([100.00, 100.05, 100.0001])))

    assert oscillating["by_spacing"]["30"]["round_trips"] == 19
    assert still["by_spacing"]["10"]["round_trips"] == 0
    assert oscillating["path_efficiency"] == pytest.approx(2400.0 / 500.0)
    assert still["path_efficiency"] > 500.0
    assert still["path_efficiency"] > oscillating["path_efficiency"]


# --- how precise the per-hour rate is ---------------------------------------


def busy_hour(at_second, quiet=False):
    """One hour: eight alternating 50bps quotes in its first eight minutes, or
    one standing quote."""
    if quiet:
        return [(ns(at_second), "99.99", "100.01")]
    return [
        (ns(at_second + i * 60.0),
         "99.99" if i % 2 == 0 else "100.49",
         "100.01" if i % 2 == 0 else "100.51")
        for i in range(8)
    ]


def test_the_round_trip_rate_ships_with_the_range_of_the_hours_it_averages():
    """A bare rate cannot be checked by its reader.

    Four hours, two of them busy and two standing still: the whole-session rate
    is 2/h and no hour of the session ran at it. That is the shape of the real
    thing — 3h blocks of one coin-day at 30bps span 1.00-4.67/h against a
    whole-day 2.29 (HYPE 20260730), and the cross-coin spread this tool exists to
    resolve is no wider. The rest of this file already refuses under-powered
    numbers (`MIN_VOL_WINDOWS`, `runs_dropped_to_gaps`); the rate is not exempt.
    """
    rows = (
        busy_hour(0.0)
        + busy_hour(3600.0, quiet=True)
        + busy_hour(2 * 3600.0, quiet=True)
        + busy_hour(3 * 3600.0)
        + [(ns(4 * 3600.0), "99.99", "100.01")]
    )
    out = rm.summarize_traversal(rm.traversal_of(quotes(rows)))
    assert out["hours"] == pytest.approx(4.0)
    assert out["hour_blocks"] == 4
    cell = out["by_spacing"]["30"]
    assert cell["round_trips"] == 8
    assert cell["round_trips_per_hour"] == pytest.approx(2.0)
    assert cell["round_trips_per_hour_min"] == pytest.approx(0.0)
    assert cell["round_trips_per_hour_max"] == pytest.approx(3.0)


def test_a_recording_too_short_for_a_range_reports_none_and_still_reports_the_rate():
    """Two blocks are two numbers, not a range — the same refusal as
    `MIN_VOL_WINDOWS`. The rate itself still ships: a three-hour fragment is the
    normal case here, and per-hour is the only way to compare it with a day."""
    out = rm.summarize_traversal(rm.traversal_of(walk([100.00, 100.50] * 8, step_seconds=600.0)))
    assert out["hours"] == pytest.approx(2.5)
    assert out["hour_blocks"] == 2 < rm.MIN_TRAVERSAL_BLOCKS
    cell = out["by_spacing"]["30"]
    assert cell["round_trips_per_hour"] == pytest.approx(7 / 2.5)
    assert cell["round_trips_per_hour_min"] is None
    assert cell["round_trips_per_hour_max"] is None


def test_pooling_pools_the_hours_and_not_the_rates():
    """Two days' blocks are one longer list of blocks, so the pooled range covers
    the still day as well as the busy one."""
    busy = rm.traversal_of(quotes(
        busy_hour(0.0) + busy_hour(3600.0) + busy_hour(2 * 3600.0)
        + [(ns(3 * 3600.0), "99.99", "100.01")]
    ))
    still = rm.traversal_of(quotes(
        busy_hour(0.0, quiet=True) + busy_hour(3600.0, quiet=True)
        + busy_hour(2 * 3600.0, quiet=True) + [(ns(3 * 3600.0), "99.99", "100.01")]
    ))
    out = rm.summarize_traversal(rm.merge_traversals([busy, still]))
    assert out["hour_blocks"] == 6
    cell = out["by_spacing"]["30"]
    assert cell["round_trips_per_hour_min"] == pytest.approx(0.0)
    assert cell["round_trips_per_hour_max"] == pytest.approx(3.0)


# --- what a spacing is worth in the coin's own tick --------------------------


def test_a_spacing_is_reported_in_the_mid_steps_the_tick_allows():
    """5bps means something different on a coin whose tick is 5.4bps of its mid.

    The mid moves in half-ticks, so PUMP's mid moves 2.7bps at a time and the
    smallest two-step wiggle its book can print already clears 5bps; HYPE's tick
    is 0.18bps, needing ~55 mid steps for the same threshold. Nothing else in the
    row relates the ladder to the tick, though the tick is measured two sections
    earlier.
    """
    series = rm.traversal_of(walk([100.00, 100.50]))
    out = rm.summarize_traversal(series, tick_bps=5.38)
    assert out["mid_step_bps"] == pytest.approx(2.69)
    assert out["by_spacing"]["5"]["mid_steps"] == pytest.approx(5 / 2.69)
    assert out["by_spacing"]["5"]["at_flicker_floor"] is True
    assert out["by_spacing"]["30"]["mid_steps"] == pytest.approx(30 / 2.69)
    assert out["by_spacing"]["30"]["at_flicker_floor"] is False
    # A liquid coin's ladder is above the floor at every rung.
    fine = rm.summarize_traversal(series, tick_bps=0.18)
    assert all(
        fine["by_spacing"][str(s)]["at_flicker_floor"] is False
        for s in rm.TRAVERSAL_SPACINGS_BPS
    )
    # No tick, no claim — not a claim of "fine".
    bare = rm.summarize_traversal(series)
    assert bare["mid_step_bps"] is None
    assert bare["by_spacing"]["5"]["mid_steps"] is None
    assert bare["by_spacing"]["5"]["at_flicker_floor"] is None


# ---------------------------------------------------------------------------
# reading, unions, warnings
# ---------------------------------------------------------------------------


def test_the_reader_counts_each_channel_and_ignores_other_coins(tmp_path):
    """`l2Book` fast and slow are told apart by `data.fast`, as the sibling
    module's `classify` does — the two feeds are 5 levels at 0.54s and 20 at
    5.4s, and metric 5 is specified on the fast one."""
    hl_dir, um_dir = a_day(
        tmp_path,
        [
            (ns(0), hl_bbo("PUMP", ns(0), "100.00", "100.02")),
            (ns(1), hl_l2("PUMP", ns(1), [("100.00", "10")], [("100.02", "10")], fast=True)),
            (ns(2), hl_l2("PUMP", ns(2), [("100.00", "10")], [("100.02", "10")], fast=False)),
            (ns(3), hl_trade("PUMP", ns(3))),
            (ns(4), hl_bbo("HYPE", ns(4), "40.000", "40.001")),
        ],
        [
            (ns(0), um_book_ticker("PUMPUSDT", ns(0), 1, "100.00", "100.02")),
            (ns(1), um_premium_index("PUMPUSDT", ns(1))),
            (ns(2), um_book_ticker("OTHERUSDT", ns(2), 2, "5.0", "5.1")),
        ],
    )
    day = rm.read_day(hl_dir, "PUMP", [um_dir], "PUMPUSDT", DAY)
    assert day.counts["bbo"] == 1
    assert day.counts["l2Book_fast"] == 1
    assert day.counts["l2Book_slow"] == 1
    assert day.counts["trades"] == 1
    assert day.counts["um_bookTicker"] == 1


def test_two_recordings_are_unioned_by_update_id_earliest_arrival_winning(tmp_path):
    """The same idea `build_dataset.build_signal_union` uses, for the same
    reason: two sockets to one venue drop at uncorrelated times, and the venue's
    `u` is the only thing that identifies one update across both."""
    hl_dir = tmp_path / "hl"
    write_gz(hl_dir / f"pump_{DAY}.gz", [(ns(0), hl_bbo("PUMP", ns(0), "100.00", "100.02"))])
    a = tmp_path / "um_a"
    b = tmp_path / "um_b"
    write_gz(
        a / f"pumpusdt_{DAY}.gz",
        [
            (ns(0.0), um_book_ticker("PUMPUSDT", ns(0.0), 100, "100.00", "100.02")),
            (ns(1.0), um_book_ticker("PUMPUSDT", ns(1.0), 101, "100.01", "100.03")),
        ],
    )
    write_gz(
        b / f"pumpusdt_{DAY}.gz",
        [
            (ns(0.1), um_book_ticker("PUMPUSDT", ns(0.1), 100, "100.00", "100.02")),
            (ns(2.0), um_book_ticker("PUMPUSDT", ns(2.0), 102, "100.02", "100.04")),
        ],
    )
    day = rm.read_day(hl_dir, "PUMP", [a, b], "PUMPUSDT", DAY)
    assert day.counts["um_bookTicker"] == 3
    assert day.counts["um_recovered_by_second_recording"] == 1
    assert day.um_ts.tolist() == [ns(0.0), ns(1.0), ns(2.0)]


def test_bookticker_frames_with_no_update_id_survive_the_union(tmp_path):
    """A frame with no `u` cannot be matched, so it cannot be deduplicated.

    They share a sentinel internally, and a dedup blind to that would treat every
    one of them as the same update and keep exactly one — silently deleting most
    of a feed while every count in the report still looked plausible.
    """
    hl_dir = tmp_path / "hl"
    write_gz(hl_dir / f"pump_{DAY}.gz", [(ns(0), hl_bbo("PUMP", ns(0), "100.00", "100.02"))])
    a, b = tmp_path / "um_a", tmp_path / "um_b"
    for directory, base in ((a, 0.0), (b, 0.5)):
        records = []
        for i in range(3):
            frame = um_book_ticker("PUMPUSDT", ns(base + i), 0, "100.00", "100.02")
            del frame["data"]["u"]
            records.append((ns(base + i), frame))
        write_gz(directory / f"pumpusdt_{DAY}.gz", records)
    day = rm.read_day(hl_dir, "PUMP", [a, b], "PUMPUSDT", DAY)
    assert day.counts["um_bookTicker"] == 6
    assert day.counts["um_frames_without_update_id"] == 6
    assert any("no update id" in w for w in day.warnings)


def test_a_hole_the_second_recording_covered_is_not_reported_as_a_hole(tmp_path):
    """The whole point of `--um-dir-b`, measured on the series the metrics use.

    Gaps counted per recording and then summed describe neither recording's
    coverage nor the union's: recording A is silent 100s..200s, recording B is
    silent everywhere else, and the union the lead-lag was computed from has no
    hole at all. A warning is how a coin gets discounted, so one that fires on a
    hole that was covered inverts the reason the second recording is passed.
    """
    hl_dir = tmp_path / "hl"
    write_gz(hl_dir / f"pump_{DAY}.gz", [(ns(0), hl_bbo("PUMP", ns(0), "100.00", "100.02"))])
    a, b = tmp_path / "um_a", tmp_path / "um_b"
    write_gz(
        a / f"pumpusdt_{DAY}.gz",
        [(ns(i), um_book_ticker("PUMPUSDT", ns(i), i, "100.00", "100.02"))
         for i in list(range(0, 101)) + list(range(200, 301))],
    )
    write_gz(
        b / f"pumpusdt_{DAY}.gz",
        [(ns(i), um_book_ticker("PUMPUSDT", ns(i), i, "100.00", "100.02"))
         for i in range(100, 201)],
    )
    day = rm.read_day(hl_dir, "PUMP", [a, b], "PUMPUSDT", DAY)
    assert int(np.diff(day.um_ts).max()) == 1 * SEC
    assert "um.bookTicker" not in day.gaps
    assert not [w for w in day.warnings if "um.bookTicker" in w and "gap" in w]


def test_a_hole_in_both_recordings_is_still_reported(tmp_path):
    """The other half of the same behaviour, so the fix cannot be "never warn"."""
    hl_dir = tmp_path / "hl"
    write_gz(hl_dir / f"pump_{DAY}.gz", [(ns(0), hl_bbo("PUMP", ns(0), "100.00", "100.02"))])
    a, b = tmp_path / "um_a", tmp_path / "um_b"
    for directory, shift, base_u in ((a, 0.0, 0), (b, 0.1, 1_000_000)):
        write_gz(
            directory / f"pumpusdt_{DAY}.gz",
            [(ns(i + shift),
              um_book_ticker("PUMPUSDT", ns(i + shift), base_u + i, "100.00", "100.02"))
             for i in list(range(0, 101)) + list(range(200, 301))],
        )
    day = rm.read_day(hl_dir, "PUMP", [a, b], "PUMPUSDT", DAY)
    # Last frame before the hole is B's at 100.1s, first after is A's at 200s.
    assert day.gaps["um.bookTicker"] == (1, 99_900_000_000)
    assert any("um.bookTicker" in w and "gap" in w for w in day.warnings)


def test_passing_the_same_recording_twice_does_not_double_the_gap_count(tmp_path):
    """`--um-dir` is repeatable and the help text invites repeating it."""
    hl_dir = tmp_path / "hl"
    write_gz(hl_dir / f"pump_{DAY}.gz", [(ns(0), hl_bbo("PUMP", ns(0), "100.00", "100.02"))])
    a = tmp_path / "um_a"
    write_gz(
        a / f"pumpusdt_{DAY}.gz",
        [(ns(i), um_book_ticker("PUMPUSDT", ns(i), i, "100.00", "100.02"))
         for i in list(range(0, 101)) + list(range(200, 301))],
    )
    day = rm.read_day(hl_dir, "PUMP", [a, a], "PUMPUSDT", DAY)
    assert day.gaps["um.bookTicker"] == (1, 100 * SEC)


def test_the_tick_uses_every_channel_that_prints_a_best_price(tmp_path):
    """`bbo` and both `l2Book` cadences all carry the best bid.

    §8.6 says the reconstructed tick is what the whole re-ranking rests on, so the
    measurement uses every observation of the price grid the recording holds — not
    only `bbo`, which on a thin coin can sit still for the whole window while the
    book feed shows the grid plainly. The channels are counted separately and
    summed: interleaving two feeds of different latency would invent transitions
    neither of them saw.
    """
    hl = [(ns(i), hl_bbo("PUMP", ns(i), "100.00", "100.02")) for i in range(4)]
    hl += [
        (
            ns(i),
            hl_l2(
                "PUMP",
                ns(i),
                [(f"{100.00 + 0.01 * i:.2f}", "10")],
                [(f"{100.02 + 0.01 * i:.2f}", "10")],
                fast=True,
            ),
        )
        for i in range(4)
    ]
    hl_dir, um_dir = a_day(
        tmp_path, hl, [(ns(0), um_book_ticker("PUMPUSDT", ns(0), 1, "100.00", "100.02"))]
    )
    row = rm.summarize(rm.read_day(hl_dir, "PUMP", [um_dir], "PUMPUSDT", DAY))
    assert row["tick"]["empirical"] == pytest.approx(0.01)
    assert row["tick"]["used_source"] == "empirical"
    assert row["tick"]["price_moves"] == 3


def test_a_missing_channel_and_a_long_gap_are_warned_about(tmp_path):
    hl_dir, um_dir = a_day(
        tmp_path,
        [
            (ns(0), hl_bbo("PUMP", ns(0), "100.00", "100.02")),
            (ns(200), hl_bbo("PUMP", ns(200), "100.00", "100.02")),
        ],
        [(ns(0), um_book_ticker("PUMPUSDT", ns(0), 1, "100.00", "100.02"))],
    )
    day = rm.read_day(hl_dir, "PUMP", [um_dir], "PUMPUSDT", DAY)
    joined = " | ".join(day.warnings)
    assert "l2Book_fast" in joined
    assert "gap" in joined


def test_a_day_the_recording_only_partly_covers_says_so(tmp_path):
    """Three hours of one coin against a full day of another is not a comparison.

    The real 2026-07-29 recording started at 21:00 UTC, so day one of the run is
    three hours long. Nothing else in the output makes that impossible to miss.
    """
    hl_dir, um_dir = a_day(
        tmp_path,
        [(ns(i), hl_bbo("PUMP", ns(i), "100.00", "100.02")) for i in range(0, 3600, 2)],
        [(ns(i), um_book_ticker("PUMPUSDT", ns(i), i, "100.00", "100.02")) for i in range(3600)],
    )
    day = rm.read_day(hl_dir, "PUMP", [um_dir], "PUMPUSDT", DAY)
    assert any("covers" in w and "of the day" in w for w in day.warnings)


def test_a_missing_file_is_a_warning_not_a_crash(tmp_path):
    hl_dir = tmp_path / "hl"
    hl_dir.mkdir()
    um_dir = tmp_path / "um"
    um_dir.mkdir()
    day = rm.read_day(hl_dir, "PUMP", [um_dir], "PUMPUSDT", DAY)
    assert any("no file" in w for w in day.warnings)
    assert day.counts["bbo"] == 0


# ---------------------------------------------------------------------------
# the report
# ---------------------------------------------------------------------------


def a_two_window_day(tmp_path, day=DAY, base=100.0, frames=1200):
    """Enough frames for every metric to have something to say.

    Frames are stamped on `day` itself, not on day one: a second day written at
    day one's timestamps makes the two interchangeable, and then no assertion
    about the pooled row can tell pooling from taking day one.
    """
    hl = []
    um = []
    for i in range(frames):  # 1200 * 200ms = 240s = 4 windows
        t = ns_on(day, i * 0.2)
        bid = base + 0.01 * (i % 3)
        hl.append((t, hl_bbo("PUMP", t, f"{bid:.2f}", f"{bid + 0.02:.2f}")))
        if i % 5 == 0:
            hl.append(
                (
                    t,
                    hl_l2(
                        "PUMP",
                        t,
                        [(f"{bid:.2f}", "1000"), (f"{bid - 0.05:.2f}", "1000")],
                        [(f"{bid + 0.02:.2f}", "1000"), (f"{bid + 0.07:.2f}", "1000")],
                        fast=True,
                    ),
                )
            )
        um.append((t, um_book_ticker("PUMPUSDT", t, 1000 + i, f"{bid:.2f}", f"{bid + 0.02:.2f}")))
    return a_day(tmp_path, hl, um, day=day)


def two_unequal_days(tmp_path):
    """Day one four windows long, day two two windows long. Returns the reads.

    Unequal on purpose. Where the two days are the same length, several ways of
    getting the pooling wrong land on the right answer by symmetry — folding every
    day's book levels into day one's snapshot slots, for one, leaves the median
    depth unchanged when the two halves are the same size.
    """
    hl_dir, um_dir = a_two_window_day(tmp_path, day="20260729", frames=1200)
    a_two_window_day(tmp_path, day="20260730", frames=600)
    return [
        rm.read_day(hl_dir, "PUMP", [um_dir], "PUMPUSDT", d)
        for d in ("20260729", "20260730")
    ]


def test_pooling_keeps_every_days_observations(tmp_path):
    """`pool_days`' contract, term by term.

    Eight independent ways of quietly reducing the pooled row to day one used to
    pass: each of `leadlag`, `fast`, `lifetime_*`, `windows`, the tick counts and
    `um_ts` taken from day one, plus both index remaps dropped. The blast radius
    of the `row` one alone, measured over two copies of the real fixture day, was
    a `d10` wrong by 40x.
    """
    one, two = two_unequal_days(tmp_path)
    pooled = rm.pool_days([one, two])

    assert pooled.counts["bbo"] == one.counts["bbo"] + two.counts["bbo"]
    assert len(pooled.bbo) == len(one.bbo) + len(two.bbo)
    assert len(pooled.fast) == len(one.fast) + len(two.fast)
    assert pooled.um_ts.size == one.um_ts.size + two.um_ts.size
    assert pooled.windows.vol.size == one.windows.vol.size + two.windows.vol.size
    assert pooled.lifetime_bid.size == one.lifetime_bid.size + two.lifetime_bid.size
    assert pooled.lifetime_ask.size == one.lifetime_ask.size + two.lifetime_ask.size
    assert pooled.bid_tick_counts == one.bid_tick_counts + two.bid_tick_counts

    # The lead-lag sums add per lag — not averaged, which would divide the
    # published `cells` by the number of days and move the MIN_LEADLAG_CELLS
    # guard with it, and not replaced by day one's.
    assert pooled.leadlag == pytest.approx(one.leadlag + two.leadlag)

    # Remap 1: every level still points at the snapshot it came from, and no
    # snapshot has been left without levels.
    levels = np.bincount(pooled.fast.row, minlength=len(pooled.fast))
    assert int(pooled.fast.row.max()) == len(pooled.fast) - 1
    assert (levels > 0).all()

    # Remap 2: every frame still points at a window that contains it.
    inside = pooled.windows.frame_window >= 0
    starts = pooled.windows.start_ts[pooled.windows.frame_window[inside]]
    stamps = pooled.bbo.ts[inside]
    assert ((stamps >= starts) & (stamps < starts + rm.VOL_WINDOW_NS)).all()


def test_pooling_does_not_credit_a_quote_with_the_time_between_days(tmp_path):
    """The frame weights are per-day and must stay that way.

    Recomputing them after concatenation credits the last frame of every day with
    up to `SPREAD_WEIGHT_CAP_NS` of the next day — time in which nothing was
    quoted and the recording may not even have been running.
    """
    one, two = two_unequal_days(tmp_path)
    pooled = rm.pool_days([one, two])
    last_of_day_one = len(one.bbo) - 1
    assert pooled.bbo.weight[last_of_day_one] == 0.0
    assert pooled.bbo.weight.sum() == pytest.approx(
        one.bbo.weight.sum() + two.bbo.weight.sum()
    )


def a_walking_day(tmp_path, day, mids, step_seconds=1.0, coin="PUMP", symbol="PUMPUSDT"):
    """A day whose mid follows `mids`, one `bbo` frame per step.

    Only the two quote channels are written. The traversal metric reads nothing
    else, and a fixture that also had to keep the depth and lead-lag metrics happy
    could not be checked against a walk by hand.
    """
    hl = []
    um = []
    for i, m in enumerate(mids):
        t = ns_on(day, i * step_seconds)
        bid, ask = f"{m - 0.01:.6f}", f"{m + 0.01:.6f}"
        hl.append((t, hl_bbo(coin, t, bid, ask)))
        um.append((t, um_book_ticker(symbol, t, 1000 + i, bid, ask)))
    return a_day(tmp_path, hl, um, coin=coin, symbol=symbol, day=day)


def test_pooling_adds_the_days_traversals_and_does_not_bridge_midnight(tmp_path):
    """Traversals are per-session and pool by addition, like the price runs.

    Day one squares 50bps around 100 and ends high; day two squares 99bps around
    201 and ends low. Re-scanning the two days concatenated finds 49 round trips
    rather than 48 — it reads the jump from 100.50 to 202.00 as movement the
    market never made — and a path over 10000bps longer than the two days walked.
    """
    hl_dir, um_dir = a_walking_day(tmp_path, "20260729", [100.0, 100.5] * 30)
    a_walking_day(tmp_path, "20260730", [202.0, 200.0] * 20)
    one, two = [
        rm.read_day(hl_dir, "PUMP", [um_dir], "PUMPUSDT", d)
        for d in ("20260729", "20260730")
    ]
    pooled = rm.pool_days([one, two])

    for spacing in rm.TRAVERSAL_SPACINGS_BPS:
        assert one.traversal.round_trips[spacing] == 29, spacing
        assert two.traversal.round_trips[spacing] == 19, spacing
        assert pooled.traversal.round_trips[spacing] == 48, spacing
        assert pooled.traversal.traversals[spacing] == 59 + 39, spacing

    assert one.traversal.path_bps == pytest.approx(2950.0)
    assert two.traversal.path_bps == pytest.approx(78.0 / 202.0 * 1e4)
    assert pooled.traversal.path_bps == pytest.approx(
        one.traversal.path_bps + two.traversal.path_bps
    )
    # Signed, so two days that undid each other do not read as a walk.
    assert one.traversal.net_bps == pytest.approx(50.0)
    assert two.traversal.net_bps == pytest.approx(-2.0 / 202.0 * 1e4)
    assert pooled.traversal.net_bps == pytest.approx(
        one.traversal.net_bps + two.traversal.net_bps
    )
    assert pooled.traversal.seconds == pytest.approx(59.0 + 39.0)
    assert pooled.traversal.frames == 100


def test_the_traversal_metric_reaches_the_row_the_json_and_the_table(tmp_path):
    hl_dir, um_dir = a_walking_day(tmp_path, DAY, [100.0, 100.5] * 30)
    report = rm.build_report(hl_dir, "PUMP", [um_dir], "PUMPUSDT", [DAY])
    block = report["rows"][0]["traversal"]
    assert sorted(block["by_spacing"], key=int) == ["5", "10", "20", "30"]
    assert block["by_spacing"]["30"]["round_trips"] == 29
    assert block["by_spacing"]["30"]["gross_capture_potential_bps"] == pytest.approx(29 * 30)
    assert block["path_efficiency"] == pytest.approx(2950.0 / 50.0)
    assert "ceiling" in block["caveat"].lower()
    assert "traversal" in json.dumps(report["conventions"])

    # Through `json.dumps`: the per-spacing keys are strings already, so a round
    # trip cannot silently renumber them.
    again = json.loads(json.dumps(report))["rows"][0]["traversal"]
    assert again["by_spacing"]["5"]["round_trips"] == 29

    text = rm.render_text(report)
    assert "traversal" in text
    assert "ceiling" in text


def test_the_report_names_its_schema_and_states_its_conventions(tmp_path):
    hl_dir, um_dir = a_two_window_day(tmp_path)
    report = rm.build_report(hl_dir, "PUMP", [um_dir], "PUMPUSDT", [DAY])
    assert report["schema"] == rm.SCHEMA
    assert report["coin"] == "PUMP"
    assert report["um_symbol"] == "PUMPUSDT"
    conventions = json.dumps(report["conventions"])
    assert "Binance" in conventions
    assert "5" in conventions
    row = report["rows"][0]
    assert row["day"] == DAY
    assert row["tick"]["used"] == pytest.approx(0.01)
    assert row["spread"]["p50_ticks"] == pytest.approx(2.0)
    assert row["depth"]["d10"] is not None
    assert row["touch"]["bid"]["n_orders_p50"] == 1


def test_several_days_get_a_pooled_row(tmp_path):
    hl_dir, um_dir = a_two_window_day(tmp_path, day="20260729", frames=1200)
    a_two_window_day(tmp_path, day="20260730", frames=600)
    report = rm.build_report(hl_dir, "PUMP", [um_dir], "PUMPUSDT", ["20260729", "20260730"])
    assert [r["day"] for r in report["rows"]] == ["20260729", "20260730", "pooled"]
    one, two, pooled = report["rows"]

    # Every metric block of the pooled row has to have seen both days. Five of the
    # six went unasserted while the only two checks present — counts doubled, and
    # `p50_ticks` equal — were both trivially true of any pooling scheme.
    assert pooled["counts"]["bbo"] == one["counts"]["bbo"] + two["counts"]["bbo"]
    assert pooled["tick"]["price_moves"] == one["tick"]["price_moves"] + two["tick"]["price_moves"]
    assert pooled["depth"]["snapshots"] == one["depth"]["snapshots"] + two["depth"]["snapshots"]
    assert pooled["conditional_spread"]["windows"] == (
        one["conditional_spread"]["windows"] + two["conditional_spread"]["windows"]
    )
    for side in ("bid", "ask"):
        assert pooled["touch"]["touch_price_lifetime"][side]["runs"] == (
            one["touch"]["touch_price_lifetime"][side]["runs"]
            + two["touch"]["touch_price_lifetime"][side]["runs"]
        )
    assert pooled["leadlag"]["cells"] == one["leadlag"]["cells"] + two["leadlag"]["cells"]
    assert pooled["spread"]["weighted_seconds"] == pytest.approx(
        one["spread"]["weighted_seconds"] + two["spread"]["weighted_seconds"]
    )
    # The pooled row is the same metric over both days, not an average of rows.
    assert pooled["spread"]["p50_ticks"] == pytest.approx(one["spread"]["p50_ticks"])
    # And the pooled window is the span, not day one's.
    assert pooled["window"]["first_local_ts"] == one["window"]["first_local_ts"]
    assert pooled["window"]["last_local_ts"] == two["window"]["last_local_ts"]


def test_a_single_day_gets_no_pooled_row(tmp_path):
    hl_dir, um_dir = a_two_window_day(tmp_path)
    report = rm.build_report(hl_dir, "PUMP", [um_dir], "PUMPUSDT", [DAY])
    assert [r["day"] for r in report["rows"]] == [DAY]


def test_the_text_table_has_a_row_per_day(tmp_path):
    hl_dir, um_dir = a_two_window_day(tmp_path)
    report = rm.build_report(hl_dir, "PUMP", [um_dir], "PUMPUSDT", [DAY])
    text = rm.render_text(report)
    assert rm.SCHEMA in text
    assert DAY in text
    assert "spread_ticks" in text or "p50_ticks" in text
    # Every row of the table has the same number of columns as its header.
    body = [l for l in text.splitlines() if l.startswith(f"  {DAY}") or l.startswith("  day")]
    assert body


def test_the_json_survives_a_round_trip(tmp_path):
    """No numpy scalars anywhere: `json.dump` refuses them, and a report that
    cannot be written is not a report."""
    hl_dir, um_dir = a_two_window_day(tmp_path)
    report = rm.build_report(hl_dir, "PUMP", [um_dir], "PUMPUSDT", [DAY])
    assert json.loads(json.dumps(report))["schema"] == rm.SCHEMA


def test_the_cli_writes_both_outputs(tmp_path):
    hl_dir, um_dir = a_two_window_day(tmp_path)
    out_json = tmp_path / "out.json"
    out_txt = tmp_path / "out.txt"
    code = rm.main(
        [
            "--hl-dir", str(hl_dir),
            "--coin", "PUMP",
            "--um-dir", str(um_dir),
            "--um-symbol", "PUMPUSDT",
            "--day", DAY,
            "--json", str(out_json),
            "--txt", str(out_txt),
        ]
    )
    assert code == 0
    assert json.loads(out_json.read_text())["schema"] == rm.SCHEMA
    assert DAY in out_txt.read_text()


def test_a_bad_day_argument_is_a_usage_error(tmp_path):
    hl_dir, um_dir = a_two_window_day(tmp_path)
    code = rm.main(
        ["--hl-dir", str(hl_dir), "--coin", "PUMP", "--um-dir", str(um_dir),
         "--um-symbol", "PUMPUSDT", "--day", "2026-07-29"]
    )
    assert code == 2


# ---------------------------------------------------------------------------
# the real fixture
# ---------------------------------------------------------------------------

FIXTURES = Path(__file__).resolve().parent / "testdata" / "rerank_metrics"


@pytest.mark.skipif(not FIXTURES.exists(), reason="real fixture not checked in")
def test_the_whole_tool_runs_over_two_real_minutes_of_pump():
    """Two minutes cut out of the 2026-07-29 recording, verbatim.

    Synthetic frames cannot show that the reader survives what the venues
    actually send: `l2Book` snapshots repeating a price to the last decimal, a
    `trades` array with several fills in one frame, `premiumIndex` elements with
    no envelope, and 0.001816-style prices where the tick formula is wrong.
    """
    report = rm.build_report(
        FIXTURES / "hyperliquid", "PUMP", [FIXTURES / "binancefuturesum-b"], "PUMPUSDT", [DAY]
    )
    row = report["rows"][0]
    assert row["counts"]["bbo"] > 100
    assert row["counts"]["l2Book_fast"] > 50
    assert row["counts"]["um_bookTicker"] > 100
    # PUMP is the tick-pinned control coin: 1e-6 measured, 1e-7 from the formula.
    assert row["tick"]["empirical"] == pytest.approx(1e-6)
    assert row["tick"]["mismatch"] is True
    assert row["tick"]["used"] == pytest.approx(1e-6)
    # A coin quoted at 0.0018 with a 1e-6 tick sits at one tick most of the time.
    assert row["spread"]["frac_time_at_1_tick"] > 0.5
    assert row["spread"]["p50_ticks"] == pytest.approx(1.0)
    assert row["depth"]["d10"] > 0.0
    assert row["touch"]["bid"]["n_orders_p50"] >= 1
    assert row["leadlag"]["cells"] > rm.MIN_LEADLAG_CELLS
    assert -2000 <= row["leadlag"]["lag_ms"] <= 2000
    # Every real `bbo` frame carried both sides, so none was dropped from the walk.
    assert row["traversal"]["frames"] == row["counts"]["bbo"]
    # Exact, not `>= 1`: real prices are the only fixture here that discriminates
    # between the shipped zig-zag scan and its mutants, and a floor assertion let
    # every one of them through. Freezing the running high gives [3, 1, 1, 1] on
    # these same two minutes and keeping the stale extreme gives [9, 2, 1, 1].
    assert [
        row["traversal"]["by_spacing"][s]["round_trips"] for s in ("5", "10", "20", "30")
    ] == [6, 1, 1, 1]
    # PUMP's measured tick is 1e-6 on a 0.0018 mid — 5.5bps, so the mid moves in
    # 2.8bps steps and a 5bps round trip is two of them. That column counts the
    # smallest wiggle this book can print, and the row says so.
    assert row["traversal"]["by_spacing"]["5"]["at_flicker_floor"] is True
    assert row["traversal"]["by_spacing"]["30"]["at_flicker_floor"] is False
    # Two minutes: a rate over the whole recording, and no whole hour to spread it
    # over. Published anyway, with nothing standing behind it but its length.
    assert row["traversal"]["hour_blocks"] == 0
    assert row["traversal"]["by_spacing"]["5"]["round_trips_per_hour"] > 0
    assert row["traversal"]["by_spacing"]["5"]["round_trips_per_hour_min"] is None
    assert row["traversal"]["path_efficiency"] > 1.0
    assert row["traversal"]["frames_per_hour"] > 0
    assert rm.render_text(report)


def test_the_fixture_is_two_minutes_of_real_lines():
    """Guards the fixture itself: a re-cut that lost the tail would quietly
    weaken every assertion above."""
    if not FIXTURES.exists():
        pytest.skip("real fixture not checked in")
    spans = {}
    for path in sorted(FIXTURES.glob("*/*.gz")):
        first = last = None
        for line in rm.iter_gz_lines(path):
            ts, _ = rm.parse_line(line)
            first = ts if first is None else first
            last = ts
        spans[path.parent.name] = (last - first) / SEC
    assert set(spans) == {"hyperliquid", "binancefuturesum-b"}
    for venue, seconds in spans.items():
        assert 60.0 <= seconds <= 180.0, (venue, seconds)
