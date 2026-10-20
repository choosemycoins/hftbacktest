"""Tests for `mrr_gate.py` — the MRR break-even gate of task #71.

The spec (`docs/spec-mrr-gate.md`, v0.1-final + operator decisions 11.08) is the
only source of truth here. Every synthetic fixture below has an **analytic**
answer, so a failure names the estimator that drifted rather than the market:

  - signs are a two-state Markov chain with `C(l) = rho^l` (book Eq 16.25), so
    `C(1)` is known before the code runs;
  - the mid is the MRR surprise surface `dm_t = G(eps_t - rho eps_{t-1})`, whose
    response is flat at `G(1 - rho^2)` for every lag (book Eq 16.22 read
    forwards) — the same construction the spec freezes for the plant cell
    (spec 5.2.4);
  - the gate arithmetic (`M_fast`, `M_patient`, `M_null`, `response_ratio`) is
    then a hand-computable function of those two numbers and the spread.

Real-tape tests are the spec's own acceptance numbers (spec 7) and skip when
`/home/storage/hft-data` is not mounted, in the style of the sibling suites.

Timestamps are realistic nanosecond values (~1.786e18) throughout: they do not
survive a round trip through float64, so any place that lets one become a float
shows up here.
"""

import json
import math
import sys
from pathlib import Path

import numpy as np
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))

import mrr_gate as mg  # noqa: E402

SEC = 1_000_000_000
MS = 1_000_000
#: 2026-08-08T00:00:00Z, the day the panel probes were taken on.
T0 = 1_786_147_200_000_000_000

HL_DAY = Path("/home/storage/hft-data/hyperliquid/hype_20260808.gz")
needs_tape = pytest.mark.skipif(
    not HL_DAY.exists(), reason="real tape not mounted at /home/storage/hft-data"
)


# --------------------------------------------------------------------------
# helpers: synthetic cells with analytically known estimators
# --------------------------------------------------------------------------


def synth_chains(
    n=20_000,
    chain_len=2_000,
    rho=0.5,
    g_bps=2.0,
    spread_bps=6.0,
    seed=7,
    drift_bps=0.0,
):
    """`n` orders in equal chains, signs Markov(rho), mid = MRR surface.

    `drift_bps` adds a deterministic per-order drift to the mid; the covariance
    form of `R` must be blind to it whenever `eps_bar != 0` is the only thing
    that couples them (spec 0, "Почему R centered").
    """
    rng = np.random.default_rng(seed)
    out = []
    for _ in range(n // chain_len):
        eps = mg.markov_signs(chain_len, rho, rng)
        mid = mg.mrr_surface(eps, g_bps, rho, drift_bps=drift_bps)
        spread = np.full(chain_len, spread_bps)
        out.append(mg.Series(eps, mid, spread))
    return out


def cell_from_chains(chains, *, days=10, **kw):
    """Wrap chains into a `CellInput` that passes every non-statistical gate."""
    by_day = {}
    per = max(1, len(chains) // days)
    for i, ch in enumerate(chains):
        d = f"202608{(i // per) % days + 1:02d}"
        by_day.setdefault(d, []).append(ch)
    n_orders = int(sum(len(c.eps) for c in chains))
    defaults = dict(
        venue="synthetic",
        symbol="plant",
        fee_bps=0.0,
        sign_verified=True,
        rfq=False,
        series_by_day=by_day,
        n_orders=n_orders,
        n_dropped_mid=0,
        n_qualifying_days=len(by_day),
        frac_at_touch=1.0,
        diag={},
    )
    defaults.update(kw)
    return mg.CellInput(**defaults)


# --------------------------------------------------------------------------
# 1. estimators on synthetics with a known answer (spec 1.6)
# --------------------------------------------------------------------------


def test_markov_chain_signs_have_the_correlation_they_were_built_with():
    rng = np.random.default_rng(1)
    eps = mg.markov_signs(400_000, 0.5, rng)
    st = mg.estimate([mg.Series(eps, np.full(len(eps), 100.0), np.full(len(eps), 6.0))], 0.0)
    assert st["C1"] == pytest.approx(0.5, abs=0.01)


def test_plateau_correlation_follows_rho_to_the_lag():
    rng = np.random.default_rng(2)
    eps = mg.markov_signs(200_000, 0.8, rng)
    st = mg.estimate([mg.Series(eps, np.full(len(eps), 100.0), np.full(len(eps), 6.0))], 0.0)
    # C(l) = rho^l: at l=20, 0.8^20 = 0.0115; the plateau mean is tiny.
    assert st["C_plateau"][20] == pytest.approx(0.8**20, abs=0.01)
    assert st["C_plateau_bar"] == pytest.approx(
        float(np.mean([0.8**l for l in mg.PLATEAU_LAGS])), abs=0.01
    )


def test_constructed_mrr_response_is_flat_at_g_times_one_minus_rho_squared():
    st = mg.estimate(synth_chains(n=200_000, chain_len=5_000, seed=3), 0.0)
    expected = 2.0 * (1 - 0.5**2)  # G(1 - rho^2) = 1.5 bps, every lag
    assert st["R1"] == pytest.approx(expected, abs=0.05)
    assert st["R_inf"] == pytest.approx(expected, abs=0.05)
    assert st["R_flatness"] < mg.R_PLATEAU_FLATNESS


def test_response_is_centred_so_a_drifting_tape_does_not_inflate_it():
    """spec 0: `R` is the covariance form; the raw form carries `eps_bar E[x]`."""
    rng = np.random.default_rng(4)
    n, lag = 60_000, 20
    # eps_bar = +0.6 by construction, and the mid drifts up 1 bps per order
    # independently of the signs: covariance R must be ~0, raw R must not.
    eps = np.where(rng.random(n) < 0.8, 1.0, -1.0)
    mid = 100.0 * np.exp(np.arange(n) * 1e-4)
    st = mg.estimate([mg.Series(eps, mid, np.full(n, 6.0))], 0.0)
    assert st["eps_bar"] == pytest.approx(0.6, abs=0.02)
    assert st["R_plateau"][lag] == pytest.approx(0.0, abs=0.05)
    assert st["R_raw"][lag] == pytest.approx(st["eps_bar"] * st["E_x"][lag], abs=0.05)
    assert abs(st["R_raw"][lag]) > 5.0  # the drift is huge and would dominate


def test_plateau_statistics_share_one_base_index_set():
    """spec 1.6 stats-M4: same base indices for every plateau lag."""
    n = 5_000
    rng = np.random.default_rng(5)
    eps = mg.markov_signs(n, 0.5, rng)
    mid = mg.mrr_surface(eps, 2.0, 0.5)
    spread = np.full(n, 6.0)
    mid[100:140] = np.nan  # a hole in the mid, sign chain untouched
    spread[100:140] = np.nan
    st = mg.estimate([mg.Series(eps, mid, spread)], 0.0)
    pairs = {l: st["n_pairs"][l] for l in mg.PLATEAU_LAGS}
    assert len(set(pairs.values())) == 1
    # every base index must have a valid mid at t and at t+20..t+50
    valid = ~np.isnan(mid)
    ok = [
        t
        for t in range(n - 50)
        if valid[t] and valid[t + 20 : t + 51].all()
    ]
    assert st["n_pairs"][50] == len(ok)


def test_a_dropped_mid_does_not_break_the_sign_chain():
    """spec 1.5 math-C1(b): the order keeps its `eps`, only its pairs go."""
    n = 3_000
    rng = np.random.default_rng(6)
    eps = mg.markov_signs(n, 0.5, rng)
    mid = mg.mrr_surface(eps, 2.0, 0.5)
    spread = np.full(n, 6.0)
    holed_mid = mid.copy()
    holed_spread = spread.copy()
    holed_mid[7] = np.nan
    holed_spread[7] = np.nan
    whole = mg.estimate([mg.Series(eps, mid, spread)], 0.0)
    holed = mg.estimate([mg.Series(eps, holed_mid, holed_spread)], 0.0)
    assert holed["n_pairs"][1] == whole["n_pairs"][1]  # C(1) sees every pair
    assert holed["C1"] == whole["C1"]
    assert holed["n_orders"] == whole["n_orders"]
    assert holed["n_pairs_R1"] == whole["n_pairs_R1"] - 2  # (6,7) and (7,8)


def test_pairs_do_not_cross_a_chain_boundary():
    a = np.array([1.0, 1.0, 1.0])
    b = np.array([-1.0, -1.0, -1.0])
    mid = np.array([100.0, 100.0, 100.0])
    spread = np.array([6.0, 6.0, 6.0])
    one = mg.estimate([mg.Series(np.concatenate([a, b]), np.tile(mid, 2), np.tile(spread, 2))], 0.0)
    two = mg.estimate([mg.Series(a, mid, spread), mg.Series(b, mid, spread)], 0.0)
    assert one["n_pairs"][1] == 5
    assert two["n_pairs"][1] == 4  # the (+1,-1) pair across the seam is gone
    assert two["C1"] > one["C1"]


def test_estimator_is_undefined_when_no_plateau_pair_survives():
    """spec 1.6 goodhart-M3: NaN is a code, never a False comparison."""
    n = 30
    eps = np.ones(n)
    st = mg.estimate([mg.Series(eps, np.full(n, 100.0), np.full(n, 6.0))], 0.0)
    assert st["n_pairs"][50] == 0
    assert not st["finite"]
    assert math.isnan(st["R_inf"])


def test_sigma_tilde_is_the_variance_of_the_twenty_lag_move_over_twenty():
    rng = np.random.default_rng(8)
    n = 100_000
    steps = rng.normal(0.0, 1.0, n)  # 1 bps per order, iid
    mid = 100.0 * np.exp(np.cumsum(steps) * 1e-4)
    eps = mg.markov_signs(n, 0.5, rng)
    st = mg.estimate([mg.Series(eps, mid, np.full(n, 6.0))], 0.0)
    assert st["sigma_tilde_sq_20"] == pytest.approx(1.0, rel=0.1)


# --------------------------------------------------------------------------
# 2. gate arithmetic (spec 2, 3)
# --------------------------------------------------------------------------


def test_m_null_is_the_truncation_null_not_zero():
    """spec 2 math-C3(a): the zero of `M_patient` is `(s_bar/2) C_plateau`."""
    st = mg.estimate(synth_chains(n=100_000, chain_len=5_000, rho=0.9, seed=9), 0.0)
    assert st["M_null"] == pytest.approx(st["s_bar"] / 2 * st["C_plateau_bar"], rel=1e-12)
    assert st["M_null"] > 0.0  # rho=0.9 leaves real correlation at l=20..50


def test_fee_shifts_the_spread_by_two_sides():
    chains = synth_chains(n=40_000, chain_len=5_000, seed=10)
    free = mg.estimate(chains, 0.0)
    paid = mg.estimate(chains, 1.4995)
    assert paid["s_eff"] == pytest.approx(free["s_bar"] - 2 * 1.4995, rel=1e-12)
    assert paid["M_patient"] == pytest.approx(free["M_patient"] - 1.4995, rel=1e-9)


def test_response_ratio_is_the_fig_164_ordinate():
    chains = synth_chains(n=100_000, chain_len=5_000, seed=11)
    st = mg.estimate(chains, 0.0)
    assert st["response_ratio"] == pytest.approx(
        2 * st["R1"] / ((1 - st["C1"]) * st["s_bar"]), rel=1e-12
    )
    # G=2, rho=0.5, s=6: 2*1.5/((1-0.5)*6) = 1.0 exactly, dead centre of Fig 16.4
    assert st["response_ratio"] == pytest.approx(1.0, abs=0.05)


def test_rho_val_denominator_is_mean_square_spread():
    """spec 3 math-M2: `<s^2>`, not `s_bar^2` — they differ by 1 + CV^2."""
    n = 20_000
    rng = np.random.default_rng(12)
    eps = mg.markov_signs(n, 0.5, rng)
    mid = mg.mrr_surface(eps, 2.0, 0.5)
    spread = np.where(np.arange(n) % 2 == 0, 2.0, 10.0)  # s_bar 6, <s^2> 52
    st = mg.estimate([mg.Series(eps, mid, spread)], 0.0)
    assert st["mean_s2"] == pytest.approx(52.0, rel=1e-9)
    assert st["rho_val"] == pytest.approx(
        st["sigma_tilde_sq_20"] / (st["mean_s2"] * (1 - st["C1"] ** 2) / 4), rel=1e-12
    )
    assert st["CV_s"] == pytest.approx(4.0 / 6.0, rel=1e-9)


# --------------------------------------------------------------------------
# 3. trade definition and sign, hyperliquid (spec 1.3, 1.4, 10.1)
# --------------------------------------------------------------------------


def hl_print(t_ms, side, taker, tid, px="10.0", sz="1.0", h="0xdead"):
    maker = "0xmaker"
    users = [taker, maker] if side == "B" else [maker, taker]
    return {
        "coin": "X",
        "side": side,
        "px": px,
        "sz": sz,
        "time": t_ms,
        "hash": h,
        "tid": tid,
        "users": users,
    }


def test_hyperliquid_taker_is_users0_on_buy_and_users1_on_sell():
    p_buy = mg.hl_parse_print(hl_print(1, "B", "0xtaker", 1))
    p_sell = mg.hl_parse_print(hl_print(1, "A", "0xtaker", 2))
    assert p_buy.sign == 1 and p_buy.key == "0xtaker"
    assert p_sell.sign == -1 and p_sell.key == "0xtaker"


def test_a_print_with_an_unreadable_side_is_thrown_away():
    """spec 1.4: no tick rule; a missing sign field means the print is dropped."""
    assert mg.hl_parse_print({"side": "?", "time": 1, "tid": 3, "users": ["a", "b"]}) is None


def test_prints_sharing_a_taker_and_a_sign_become_one_order():
    prints = [
        mg.Print(t_ex=T0, sign=1, t_local=T0 + 1, key="0xt", px=10.0, qty=1.0),
        mg.Print(t_ex=T0, sign=1, t_local=T0 + 2, key="0xt", px=10.1, qty=2.0),
    ]
    orders = mg.aggregate(prints)
    assert len(orders) == 1
    assert orders[0].n_prints == 2
    assert orders[0].qty == pytest.approx(3.0)
    assert orders[0].t_local == T0 + 1  # first print's receive ts, spec 1.3
    assert orders[0].px == 10.0  # first print's price, for the at-touch test


def test_the_same_taker_returning_later_is_a_second_order():
    prints = [
        mg.Print(T0, 1, T0, "0xa", 10.0, 1.0),
        mg.Print(T0 + MS, 1, T0 + MS, "0xb", 10.0, 1.0),
        mg.Print(T0 + 2 * MS, 1, T0 + 2 * MS, "0xa", 10.0, 1.0),
    ]
    assert len(mg.aggregate(prints)) == 3


def test_a_key_that_collected_both_signs_is_split_by_sign():
    prints = [
        mg.Print(T0, 1, T0, "0xa", 10.0, 1.0),
        mg.Print(T0, -1, T0, "0xa", 10.0, 1.0),
    ]
    orders = mg.aggregate(prints)
    assert [o.sign for o in orders] == [1, -1]


def test_equal_timestamps_keep_file_order():
    """spec 1.3 math-m2: the tie-break is load-bearing; sorting by sign lies."""
    prints = [
        mg.Print(T0, 1, T0, "0xa", 10.0, 1.0),
        mg.Print(T0, -1, T0, "0xb", 10.0, 1.0),
        mg.Print(T0, 1, T0, "0xa", 10.0, 1.0),
    ]
    stable = [o.sign for o in mg.aggregate(prints)]
    assert stable == [1, -1, 1]
    resorted = sorted(prints, key=lambda p: (p.t_ex, p.sign))
    assert [o.sign for o in mg.aggregate(resorted)] != stable


def test_hyperliquid_prints_are_deduped_by_tid():
    frames = [
        (T0, {"channel": "trades", "data": [hl_print(1, "B", "0xt", 77)]}),
        (T0 + SEC, {"channel": "trades", "data": [hl_print(1, "B", "0xt", 77)]}),
    ]
    prints, n_replay, _ = mg.hl_collect_prints(frames)
    assert len(prints) == 1 and n_replay == 1


def test_zero_hash_prints_are_not_glued_together():
    """spec 10.1 math-C4: 37% of HYPE prints carry hash 0x000...0."""
    zero = "0x" + "0" * 64
    frames = [
        (
            T0,
            {
                "channel": "trades",
                "data": [
                    hl_print(1, "B", "0xt1", 1, h=zero),
                    hl_print(2, "B", "0xt2", 2, h=zero),
                ],
            },
        )
    ]
    prints, _, _ = mg.hl_collect_prints(frames)
    assert len(mg.aggregate(prints)) == 2


# --------------------------------------------------------------------------
# 4. touch, liveness (spec 1.5)
# --------------------------------------------------------------------------


def frames(*rows):
    """rows: (t_ex_ns, t_local_ns, bid, ask)"""
    a = np.array(rows, dtype=object)
    return mg.TouchFrames(
        t_ex=np.array([r[0] for r in rows], dtype=np.int64),
        t_local=np.array([r[1] for r in rows], dtype=np.int64),
        bid=np.array([r[2] for r in rows], dtype=float),
        ask=np.array([r[3] for r in rows], dtype=float),
        bid_sz=np.full(len(rows), np.nan),
        ask_sz=np.full(len(rows), np.nan),
    )


def orders(*rows):
    """rows: (t_ex_ns, t_local_ns, sign, px)"""
    return mg.OrderTable(
        t_ex=np.array([r[0] for r in rows], dtype=np.int64),
        t_local=np.array([r[1] for r in rows], dtype=np.int64),
        sign=np.array([r[2] for r in rows], dtype=float),
        px=np.array([r[3] for r in rows], dtype=float),
        qty=np.ones(len(rows)),
        n_prints=np.ones(len(rows), dtype=np.int64),
    )


def test_touch_frame_is_the_last_one_strictly_earlier():
    f = frames((T0, T0, 9.9, 10.1), (T0 + MS, T0 + MS, 10.0, 10.2))
    o = orders((T0 + MS, T0 + MS, 1, 10.1))
    t = mg.attach_touch(o, f, np.array([], dtype=np.int64))
    assert t.valid[0]
    assert t.mid[0] == pytest.approx(10.0)  # the T0 frame, not the equal-ts one


def test_a_crossed_or_zero_frame_invalidates_the_mid():
    f = frames((T0, T0, 10.2, 10.0), (T0 + MS, T0 + MS, 0.0, 10.0))
    o = orders((T0 + SEC, T0 + SEC, 1, 10.0))
    t = mg.attach_touch(o, f, np.array([], dtype=np.int64))
    assert not t.valid[0]


def test_a_quiet_but_live_feed_keeps_its_last_frame():
    """spec 1.5 impl-M6: event silence is not staleness — this is the fix that
    unblocked the negative control (0-1 readable cells of 16 under v0.1)."""
    f = frames((T0, T0, 9.9, 10.1))
    o = orders((T0 + 30 * SEC, T0 + 30 * SEC, 1, 10.1))
    t = mg.attach_touch(o, f, np.array([], dtype=np.int64))
    assert t.valid[0] and t.mid[0] == pytest.approx(10.0)


def test_a_touch_gap_longer_than_the_liveness_window_kills_the_mid():
    f = frames((T0, T0, 9.9, 10.1))
    o = orders((T0 + 61 * SEC, T0 + 61 * SEC, 1, 10.1))
    t = mg.attach_touch(o, f, np.array([], dtype=np.int64))
    assert not t.valid[0]


def test_a_reconnect_between_frame_and_order_kills_the_mid():
    f = frames((T0, T0, 9.9, 10.1))
    o = orders((T0 + 10 * SEC, T0 + 10 * SEC, 1, 10.1))
    live = mg.attach_touch(o, f, np.array([], dtype=np.int64))
    dead = mg.attach_touch(o, f, np.array([T0 + 5 * SEC], dtype=np.int64))
    assert live.valid[0] and not dead.valid[0]


def test_at_touch_is_the_price_standing_on_the_quoted_side():
    f = frames((T0, T0, 9.9, 10.1))
    o = orders(
        (T0 + SEC, T0 + SEC, 1, 10.1),  # buy at the ask
        (T0 + SEC, T0 + SEC, -1, 9.9),  # sell at the bid
        (T0 + SEC, T0 + SEC, 1, 10.5),  # buy through the book
    )
    t = mg.attach_touch(o, f, np.array([], dtype=np.int64))
    assert list(t.at_touch) == [True, True, False]


# --------------------------------------------------------------------------
# 5. inclusion criteria and flags (spec 4.3, 1.5-1.8, 2)
# --------------------------------------------------------------------------


def test_a_clean_synthetic_cell_is_included_and_passes():
    cell = cell_from_chains(synth_chains(n=100_000, chain_len=5_000, seed=13))
    rep = mg.assess_cell(cell, k_cells=1, reps=200)
    assert rep["verdict"] in ("PASS", "PASS_UNREPLICATED")
    assert rep["exclusion_reasons"] == []
    assert rep["M_patient"] == pytest.approx(3.0 - 1.5, abs=0.1)


@pytest.mark.parametrize(
    "kw,code",
    [
        (dict(n_qualifying_days=4), "WINDOW_COVERAGE"),
        (dict(n_orders=9_999), "INSUFFICIENT_N"),
        (dict(n_dropped_mid=20_000), "STALE_QUOTES"),
        (dict(frac_at_touch=0.79), "TOUCH_MISMATCH"),
        (dict(fee_bps=None), "FEE_UNKNOWN"),
        (dict(sign_verified=False), "SIGN_UNVERIFIED"),
        (dict(rfq=True), "RFQ_MARKET"),
    ],
)
def test_each_exclusion_code_fires_on_its_own_condition(kw, code):
    cell = cell_from_chains(synth_chains(n=100_000, chain_len=5_000, seed=14), **kw)
    rep = mg.assess_cell(cell, k_cells=1, reps=50)
    assert rep["verdict"] == "EXCLUDED"
    assert code in rep["exclusion_reasons"]


def test_rfq_market_is_excluded_before_any_arithmetic():
    """spec 4.3-11: the flag is read before the estimators run."""
    cell = cell_from_chains(synth_chains(n=20_000, chain_len=5_000, seed=15), rfq=True)
    rep = mg.assess_cell(cell, k_cells=1, reps=10)
    assert rep["exclusion_reasons"] == ["RFQ_MARKET"]
    assert rep["M_patient"] is None


def test_insufficient_pairs_is_its_own_code():
    chains = [c for c in synth_chains(n=20_000, chain_len=100, seed=16)][:5]
    cell = cell_from_chains(chains, n_orders=20_000)
    rep = mg.assess_cell(cell, k_cells=1, reps=10)
    assert "INSUFFICIENT_PAIRS" in rep["exclusion_reasons"]


def test_a_cell_prints_every_reason_it_failed():
    cell = cell_from_chains(
        synth_chains(n=100_000, chain_len=5_000, seed=17),
        fee_bps=None,
        sign_verified=False,
    )
    rep = mg.assess_cell(cell, k_cells=1, reps=10)
    assert set(rep["exclusion_reasons"]) >= {"FEE_UNKNOWN", "SIGN_UNVERIFIED"}


def test_a_response_ratio_outside_the_band_excludes():
    # G=6 bps with rho=0.5 gives R(1)=4.5 and ratio 2*4.5/(0.5*6)=3.0 > 2.5
    cell = cell_from_chains(synth_chains(n=100_000, chain_len=5_000, g_bps=6.0, seed=18))
    rep = mg.assess_cell(cell, k_cells=1, reps=10)
    assert "RESPONSE_SPREAD_BAND" in rep["exclusion_reasons"]


def test_a_negative_lag_one_correlation_is_a_broken_estimator_not_a_market():
    """spec 1.6 math-C4(b): `C(1) <= 0` means SIGN_STRUCTURE, not a verdict."""
    n = 20_000
    eps = np.where(np.arange(n) % 2 == 0, 1.0, -1.0)  # perfectly alternating
    chains = [
        mg.Series(eps[i : i + 5_000], np.full(5_000, 100.0), np.full(5_000, 6.0))
        for i in range(0, n, 5_000)
    ]
    rep = mg.assess_cell(cell_from_chains(chains), k_cells=1, reps=10)
    assert "SIGN_STRUCTURE" in rep["exclusion_reasons"]


def test_nan_statistics_exclude_rather_than_compare_false():
    n = 5_000
    chains = [mg.Series(np.ones(n), np.full(n, np.nan), np.full(n, np.nan))]
    rep = mg.assess_cell(cell_from_chains(chains, n_orders=n * 3), k_cells=1, reps=10)
    assert "ESTIMATOR_UNDEFINED" in rep["exclusion_reasons"]
    assert rep["verdict"] == "EXCLUDED"


def test_day_concentration_blocks_pass():
    """spec 2 stats-M5: five qualifying days, but one of them is the cell."""
    chains = synth_chains(n=100_000, chain_len=5_000, seed=19)
    by_day = {"20260805": chains[:16]}
    for i, ch in enumerate(chains[16:]):
        by_day[f"2026080{6 + i}"] = [ch]
    cell = mg.CellInput(
        venue="synthetic",
        symbol="concentrated",
        fee_bps=0.0,
        sign_verified=True,
        rfq=False,
        series_by_day=by_day,
        n_orders=100_000,
        n_dropped_mid=0,
        n_qualifying_days=len(by_day),
        frac_at_touch=1.0,
        diag={},
    )
    rep = mg.assess_cell(cell, k_cells=1, reps=200)
    assert rep["day_share_max"] == pytest.approx(0.8)
    assert "DAY_CONCENTRATION" in rep["flags"]
    assert rep["verdict"] == "FAIL"


def test_large_tick_blocks_promotion_but_not_pass():
    cell = cell_from_chains(
        synth_chains(n=100_000, chain_len=5_000, seed=20),
        diag={"p_spread_1tick": 0.9},
    )
    rep = mg.assess_cell(cell, k_cells=1, reps=200)
    assert "LARGE_TICK" in rep["flags"]
    assert rep["verdict"] == "PASS_UNREPLICATED"
    assert rep["promote"] is False


def biased_markov(n, p_stay_up, p_up_from_down, rng):
    """Two-state chain with `E[eps] != 0`: corr = a - b, `pi+ = b/(1-a+b)`."""
    u = rng.random(n)
    s = np.empty(n)
    cur = 1.0
    for i in range(n):
        p = p_stay_up if cur > 0 else p_up_from_down
        cur = 1.0 if u[i] < p else -1.0
        s[i] = cur
    return s


def test_eps_drift_is_a_flag_and_nothing_more():
    """spec 0: `|eps_bar| > 0.15` is a caveat on the reading, not an exclusion."""
    rng = np.random.default_rng(21)
    n = 100_000
    # a=0.9, b=0.5 -> corr 0.4, eps_bar +0.67, and C(1) stays positive, so the
    # cell is readable and only the drift flag fires.
    eps = biased_markov(n, 0.9, 0.5, rng)
    mid = mg.mrr_surface(eps, 2.0, 0.4)
    chains = [
        mg.Series(eps[i : i + 5_000], mid[i : i + 5_000], np.full(5_000, 3.0))
        for i in range(0, n, 5_000)
    ]
    rep = mg.assess_cell(cell_from_chains(chains), k_cells=1, reps=100)
    assert rep["eps_bar"] > mg.EPS_DRIFT_MAX
    assert "EPS_DRIFT" in rep["flags"]
    assert rep["exclusion_reasons"] == []
    assert rep["verdict"] != "EXCLUDED"


def test_an_unresolved_plateau_blocks_pass():
    """spec 1.6: long-memory signs make `R(l)` climb; there is no plateau to
    read, so PASS is impossible however good the arithmetic looks."""
    rng = np.random.default_rng(22)
    n, chain_len = 100_000, 5_000
    chains = []
    for _ in range(n // chain_len):
        eps = mg.markov_signs(chain_len, 0.99, rng)
        mid = 100.0 * np.exp(np.cumsum(0.05 * eps) * 1e-4)
        chains.append(mg.Series(eps, mid, np.full(chain_len, 10.0)))
    rep = mg.assess_cell(cell_from_chains(chains), k_cells=1, reps=100)
    assert "R_INF_UNRESOLVED" in rep["flags"] or "R_GROWTH" in rep["flags"]
    assert rep["verdict"] != "PASS_UNREPLICATED"


def test_an_uncomputable_growth_test_blocks_pass_only():
    """spec 1.6: `n_pairs(200)` below the floor blocks PASS, never FAIL."""
    chains = synth_chains(n=100_000, chain_len=150, seed=23)
    rep = mg.assess_cell(cell_from_chains(chains), k_cells=1, reps=100)
    assert "R_GROWTH_UNCOMPUTABLE" in rep["flags"]
    assert rep["verdict"] == "FAIL"


# --------------------------------------------------------------------------
# 6. decision rule (spec 2)
# --------------------------------------------------------------------------


def test_pass_needs_the_family_lower_bound_above_the_null():
    cell = cell_from_chains(synth_chains(n=100_000, chain_len=5_000, seed=24))
    lone = mg.assess_cell(cell, k_cells=1, reps=400)
    crowd = mg.assess_cell(cell, k_cells=10_000_000, reps=400)
    assert lone["verdict"] == "PASS_UNREPLICATED"
    assert lone["D_patient_lb_family"] > 0
    assert crowd["D_patient_lb_family"] < lone["D_patient_lb_family"]


def test_bonferroni_percentile_is_alpha_over_k():
    d = np.linspace(-1.0, 1.0, 100_001)
    assert mg.family_lower_bound(d, 1) == pytest.approx(np.percentile(d, 5.0), abs=1e-6)
    assert mg.family_lower_bound(d, 10) == pytest.approx(np.percentile(d, 0.5), abs=1e-6)


def test_a_point_estimate_above_the_null_without_the_bound_is_marginal_fail():
    """spec 2: `s/2 = 1.52` against `R_inf = 1.5` — positive, but not by more
    than the bootstrap can tell apart from zero."""
    chains = synth_chains(n=100_000, chain_len=5_000, seed=25)
    # place the cell one bootstrap sigma (~0.05 bps) above break-even: the
    # point estimate clears the null, the lower bound cannot.
    r_inf = mg.estimate(chains, 0.0)["R_inf"]
    chains = [c._replace(spread=np.full(len(c.eps), 2 * (r_inf + 0.05))) for c in chains]
    cell = cell_from_chains(chains)
    rep = mg.assess_cell(cell, k_cells=1, reps=400)
    assert rep["M_patient"] > rep["M_null_bps"]
    assert rep["D_patient_lb_family"] < 0
    assert rep["verdict"] == "FAIL"
    assert rep["marginal"] is True


def test_first_pass_is_unreplicated_and_not_promoted():
    cell = cell_from_chains(synth_chains(n=100_000, chain_len=5_000, seed=26))
    rep = mg.assess_cell(cell, k_cells=1, reps=200)
    assert rep["verdict"] == "PASS_UNREPLICATED"
    assert rep["promote"] is False
    assert rep["n_screen_attempts"] == 1


def test_the_slow_only_regime_is_labelled_and_promotable():
    """spec 2 math-M1: Fig 17.2 labels the region; it is not a malfunction."""
    assert mg.regime_of(m_fast=-1.0, m_patient=+1.0) == "slow_only"
    assert mg.regime_of(m_fast=+1.0, m_patient=-1.0) == "fast_only"
    assert mg.regime_of(m_fast=+1.0, m_patient=+1.0) == "both"
    assert mg.regime_of(m_fast=-1.0, m_patient=-1.0) == "neither"


def test_the_day_median_must_agree_with_the_pool():
    """spec 2 stats-M5: five fat rich days carry the pool, six thin poor days
    carry the median — the pool alone must not be allowed to decide."""
    rng = np.random.default_rng(27)
    by_day = {}
    for i in range(5):  # rich: 9000 orders, spread 8 -> M_patient = +2.5
        eps = mg.markov_signs(9_000, 0.5, rng)
        by_day[f"2026080{i + 1}"] = [
            mg.Series(eps, mg.mrr_surface(eps, 2.0, 0.5), np.full(9_000, 8.0))
        ]
    for i in range(6):  # poor: 3000 orders, spread 1 -> M_patient = -1.0
        eps = mg.markov_signs(3_000, 0.5, rng)
        by_day[f"2026081{i}"] = [
            mg.Series(eps, mg.mrr_surface(eps, 2.0, 0.5), np.full(3_000, 1.0))
        ]
    n = int(sum(len(c.eps) for day in by_day.values() for c in day))
    cell = mg.CellInput(
        venue="synthetic",
        symbol="median",
        fee_bps=0.0,
        sign_verified=True,
        rfq=False,
        series_by_day=by_day,
        n_orders=n,
        n_dropped_mid=0,
        n_qualifying_days=len(by_day),
        frac_at_touch=1.0,
        diag={},
    )
    rep = mg.assess_cell(cell, k_cells=1, reps=200)
    assert rep["M_patient"] > rep["M_null_bps"]  # the pool would have passed
    assert rep["day_median_M_patient"] < rep["M_null_bps"]
    assert rep["verdict"] == "FAIL"


# --------------------------------------------------------------------------
# 7. bootstrap (spec 1.8)
# --------------------------------------------------------------------------


def test_block_count_is_the_ceiling_of_n_over_b():
    assert mg.blocks_per_replicate(100_000, 500) == 200
    assert mg.blocks_per_replicate(100_001, 500) == 201
    assert mg.blocks_per_replicate(1, 500) == 1


def test_a_mutated_block_count_inflates_the_bootstrap_spread():
    """spec 1.8 goodhart-M2: `k` was never frozen in v0.1 — mutate it and the
    interval moves, so the constant is load-bearing and pinned here."""
    rng = np.random.default_rng(29)
    n, chain_len = 40_000, 4_000
    chains = []
    for _ in range(n // chain_len):
        eps = mg.markov_signs(chain_len, 0.5, rng)
        chains.append(
            mg.Series(
                eps,
                mg.mrr_surface(eps, 2.0, 0.5),
                rng.normal(6.0, 1.0, chain_len),  # iid spread, sd 1 bps
            )
        )
    honest = mg.bootstrap_cell(chains, 0.0, reps=300, blocks=(500,), seed=1)
    analytic = 1.0 / math.sqrt(n)
    assert honest["per_b"][500]["sd_s_bar"] == pytest.approx(analytic, rel=0.25)

    real = mg.blocks_per_replicate
    try:
        mg.blocks_per_replicate = lambda n_, b: max(1, real(n_, b) // 4)
        mutant = mg.bootstrap_cell(chains, 0.0, reps=300, blocks=(500,), seed=1)
    finally:
        mg.blocks_per_replicate = real
    assert mutant["per_b"][500]["sd_s_bar"] > 1.7 * honest["per_b"][500]["sd_s_bar"]


def test_the_decisive_interval_is_the_widest_computable():
    chains = synth_chains(n=60_000, chain_len=3_000, seed=30)
    boot = mg.bootstrap_cell(chains, 0.0, reps=200, blocks=(500, 2000, 8000), seed=2)
    assert set(boot["per_b"]) == {500, 2000}  # no chain reaches 8000
    widths = {b: v["ub_D"] - v["lb_D"] for b, v in boot["per_b"].items()}
    assert boot["chosen_b"] == max(widths, key=widths.get)


def test_no_chain_long_enough_is_ci_unavailable():
    chains = synth_chains(n=20_000, chain_len=100, seed=31)
    boot = mg.bootstrap_cell(chains, 0.0, reps=50, blocks=mg.BOOTSTRAP_BLOCKS, seed=3)
    assert boot["per_b"] == {}
    assert "CI_UNAVAILABLE" in boot["flags"]


def test_a_growing_sd_is_ci_unconverged():
    boot = {"per_b": {500: {"sd_D": 0.02}, 2000: {"sd_D": 0.03}}}
    assert mg.ci_unconverged(boot["per_b"]) is True
    assert mg.ci_unconverged({500: {"sd_D": 0.02}, 2000: {"sd_D": 0.021}}) is False


def test_two_runs_with_the_same_seed_agree():
    chains = synth_chains(n=20_000, chain_len=5_000, seed=32)
    a = mg.bootstrap_cell(chains, 0.0, reps=100, blocks=(500,), seed=4)
    b = mg.bootstrap_cell(chains, 0.0, reps=100, blocks=(500,), seed=4)
    assert a["per_b"][500]["sd_D"] == b["per_b"][500]["sd_D"]


# --------------------------------------------------------------------------
# 8. the plant and the control protocol (spec 5)
# --------------------------------------------------------------------------


def test_the_plant_is_built_exactly_as_the_spec_froze_it():
    cell = mg.plant_cell()
    assert cell.n_orders == 100_000
    chains = [c for day in cell.series_by_day.values() for c in day]
    assert {len(c.eps) for c in chains} == {5_000}
    st = mg.estimate(chains, 0.0)
    assert st["C1"] == pytest.approx(0.5, abs=0.02)
    assert st["R_inf"] == pytest.approx(1.5, abs=0.05)
    assert st["s_bar"] == pytest.approx(6.0, rel=1e-12)
    assert st["M_patient"] == pytest.approx(1.5, abs=0.05)


def test_the_plant_reads_positive_and_covers_its_true_value():
    """spec 5.2.4 stats-C2: without this, the control only checked the sign of
    a subtraction — a bug that zeroed `R_inf` would have sailed through."""
    rep = mg.assess_cell(mg.plant_cell(), k_cells=1, reps=mg.BOOTSTRAP_REPS)
    assert rep["verdict"] == "PASS_UNREPLICATED"
    assert rep["D_patient_lb_family"] > 0
    lo, hi = rep["ci_M_patient"]
    assert lo <= 1.5 <= hi


def test_control_fails_when_a_readable_cell_reads_positive():
    verdicts = [
        {"cell": f"hl:{i}", "verdict": "FAIL", "M_patient": -1.0, "M_fast": -1.0}
        for i in range(16)
    ]
    ok = mg.control_criteria(verdicts, plant_ok=True)
    assert ok["passed"]
    verdicts[3]["M_patient"] = +0.2
    bad = mg.control_criteria(verdicts, plant_ok=True)
    assert not bad["passed"] and "positive_cell" in bad["failures"]


def test_control_fails_when_fewer_than_fourteen_cells_read():
    verdicts = [
        {"cell": f"hl:{i}", "verdict": "EXCLUDED", "M_patient": None, "M_fast": None}
        for i in range(3)
    ] + [
        {"cell": f"hl:{i}", "verdict": "FAIL", "M_patient": -1.0, "M_fast": -1.0}
        for i in range(13)
    ]
    out = mg.control_criteria(verdicts, plant_ok=True)
    assert not out["passed"] and "too_few_readable" in out["failures"]


def test_control_fails_when_the_plant_does_not_read_positive():
    verdicts = [
        {"cell": f"hl:{i}", "verdict": "FAIL", "M_patient": -1.0, "M_fast": -1.0}
        for i in range(16)
    ]
    out = mg.control_criteria(verdicts, plant_ok=False)
    assert not out["passed"] and "plant" in out["failures"]


# --------------------------------------------------------------------------
# 9. tooling enforces the order of operations (spec 5.4 goodhart-M4)
# --------------------------------------------------------------------------


def test_cells_json_is_refused_without_a_passing_control(tmp_path):
    run = tmp_path / "run-20260816"
    run.mkdir()
    (run / "sign-verification.json").write_text(json.dumps({"hyperliquid": {"passed": True}}))
    with pytest.raises(mg.ControlNotSealed):
        mg.require_sealed_control(run)


def test_cells_json_is_refused_without_sign_verification(tmp_path):
    run = tmp_path / "run-20260816"
    run.mkdir()
    (run / "control.json").write_text(json.dumps({"passed": True, "failures": []}))
    with pytest.raises(mg.ControlNotSealed):
        mg.require_sealed_control(run)


def test_a_failed_control_does_not_unlock_the_screen(tmp_path):
    run = tmp_path / "run-20260816"
    run.mkdir()
    (run / "control.json").write_text(
        json.dumps({"passed": False, "failures": ["too_few_readable"]})
    )
    (run / "sign-verification.json").write_text(json.dumps({"hyperliquid": {"passed": True}}))
    with pytest.raises(mg.ControlNotSealed):
        mg.require_sealed_control(run)


def test_a_sealed_control_unlocks_the_screen(tmp_path):
    run = tmp_path / "run-20260816"
    run.mkdir()
    (run / "control.json").write_text(json.dumps({"passed": True, "failures": []}))
    (run / "sign-verification.json").write_text(json.dumps({"hyperliquid": {"passed": True}}))
    mg.require_sealed_control(run)  # must not raise


def test_the_writer_itself_refuses_a_failed_control_and_leaves_no_file(tmp_path):
    """spec 5.4 goodhart-M4: the refusal has to sit on the WRITER.

    The four tests above call `require_sealed_control` directly, so deleting its
    call site inside `write_cells` left all 82 of them green while `cells.json`
    landed in a run directory whose control says `passed: false` (mutation M4e,
    measured 2026-08-11). The guard is only worth what the writer is pinned to.
    """
    run = tmp_path / "run-20260816"
    run.mkdir()
    (run / "control.json").write_text(
        json.dumps({"passed": False, "failures": ["too_few_readable"]})
    )
    (run / "sign-verification.json").write_text(json.dumps({"hyperliquid": {"passed": True}}))
    cells = {"schema": mg.SCHEMA, "cells": [{"cell": "a", "verdict": "FAIL"}]}
    with pytest.raises(mg.ControlNotSealed):
        mg.write_cells(run, cells)
    assert not (run / "cells.json").exists()
    assert not (run / "summary.md").exists()


def test_the_writer_writes_once_the_control_is_sealed(tmp_path):
    """The other half: without it the refusal above is satisfied by refusing always."""
    run = tmp_path / "run-20260816"
    run.mkdir()
    (run / "control.json").write_text(json.dumps({"passed": True, "failures": []}))
    (run / "sign-verification.json").write_text(json.dumps({"hyperliquid": {"passed": True}}))
    cells = {"schema": mg.SCHEMA, "cells": [{"cell": "a", "verdict": "FAIL"}]}
    sha = mg.write_cells(run, cells)
    assert (run / "cells.json").exists() and (run / "summary.md").exists()
    assert sha == mg.dump_json(tmp_path / "ref.json", cells)


def test_summary_is_generated_from_cells_json(tmp_path):
    cells = {
        "schema": mg.SCHEMA,
        "cells": [
            {
                "cell": "hyperliquid:hype",
                "verdict": "FAIL",
                "M_patient": -1.9,
                "M_fast": -1.5,
                "M_null_bps": 0.004,
                "flags": ["LARGE_TICK"],
                "exclusion_reasons": [],
                "promote": False,
            }
        ],
        "program_kill": "NOT_EVALUABLE",
    }
    text = mg.render_summary(cells)
    assert "hyperliquid:hype" in text
    assert "NOT_EVALUABLE" in text
    assert "LARGE_TICK" in text


def test_two_writes_of_the_same_cells_hash_identically(tmp_path):
    cells = {"schema": mg.SCHEMA, "cells": [{"cell": "a", "verdict": "FAIL"}]}
    a = mg.dump_json(tmp_path / "one.json", cells)
    b = mg.dump_json(tmp_path / "two.json", cells)
    assert a == b


def test_the_window_is_ten_days_ending_on_the_named_friday():
    """spec 4.1: the 16.08 run is 05-14.08, weekdays 05,06,07,10,11,12,13,14."""
    assert mg.window_days("20260814") == [
        "20260805", "20260806", "20260807", "20260810",
        "20260811", "20260812", "20260813", "20260814",
    ]
    assert len(mg.window_days("20260814", weekdays_only=False)) == mg.WINDOW_DAYS


def test_a_record_carries_every_field_spec_11_lists():
    """spec 11: the `cells.json` field list is a contract, not a suggestion."""
    cell = cell_from_chains(
        synth_chains(n=100_000, chain_len=5_000, seed=34),
        diag={
            "C1_unaggregated": 0.51,
            "tick": 1e-5,
            "s_bar_ticks": 6.0,
            "p_spread_1tick": 0.1,
            "E_theta_proxy": 0.3,
            "frac_orders_sharing_ms": 0.02,
            "n_lines_malformed": 0,
        },
    )
    rec = mg.assess_cell(cell, k_cells=1, reps=100)
    required = [
        "eps_bar", "E_x20", "E_x50", "R_raw_20", "R_raw_50", "R_hat_inf_1622",
        "n_pairs", "C1_unaggregated", "response_ratio", "frac_at_touch",
        "rho_val", "CV_s", "tick", "s_bar_ticks", "p_spread_1tick",
        "R1_price_changing_bps", "p_mo_price_changing", "E_theta_proxy",
        "frac_orders_sharing_ms", "n_lines_malformed", "day_share_max",
        "M_null_bps", "D_patient_lb_family", "regime", "n_screen_attempts",
        "bootstrap", "flags", "verdict", "exclusion_reasons", "capacity",
        "bybit_reading", "promote",
    ]
    assert [k for k in required if k not in rec] == []
    assert set(rec["bootstrap"]) >= {"sd_b500", "sd_b2000", "sd_b8000", "chosen_b", "coverage"}
    assert {"20", "50", "200"} <= set(rec["n_pairs"])
    assert rec["capacity"]["quote_notional_usd"] == mg.QUOTE_NOTIONAL_USD
    json.dumps(rec)  # must survive the artifact writer: no NaN, no numpy scalars


# --------------------------------------------------------------------------
# 9b. replication history and the sealed list (spec 2, 9.2, 11)
# --------------------------------------------------------------------------


def _prior_run(root, name, window, verdict):
    d = root / name
    d.mkdir(parents=True)
    (d / "cells.json").write_text(
        json.dumps(
            {
                "schema": mg.SCHEMA,
                "window": window,
                "cells": [{"cell": "hyperliquid:hype", "verdict": verdict}],
            }
        )
    )


def test_a_prior_pass_on_a_disjoint_window_counts_as_replication(tmp_path):
    _prior_run(tmp_path, "run-20260802", ["20260722", "20260723"], "PASS_UNREPLICATED")
    h = mg.load_history(tmp_path, "hyperliquid:hype", ["20260805", "20260806"])
    assert h["replicated"] is True
    assert h["attempts"] == 1


def test_a_prior_pass_on_an_overlapping_window_does_not(tmp_path):
    _prior_run(tmp_path, "run-20260809", ["20260805", "20260806"], "PASS_UNREPLICATED")
    h = mg.load_history(tmp_path, "hyperliquid:hype", ["20260806", "20260807"])
    assert h["replicated"] is False


def test_excluded_runs_are_not_screen_attempts(tmp_path):
    _prior_run(tmp_path, "run-20260802", ["20260722"], "EXCLUDED")
    _prior_run(tmp_path, "run-20260809", ["20260729"], "FAIL")
    h = mg.load_history(tmp_path, "hyperliquid:hype", ["20260805"])
    assert h["attempts"] == 1
    assert h["replicated"] is False


def test_sealing_appends_and_never_forgets(tmp_path):
    rows = [
        {"cell": "hyperliquid:crv", "fast_kill": True, "M_patient": -5.9},
        {"cell": "hyperliquid:hype", "fast_kill": False, "M_patient": -1.9},
    ]
    mg.seal_cells(tmp_path, rows, "run-20260816")
    assert mg.load_sealed(tmp_path) == {"hyperliquid:crv"}
    mg.seal_cells(tmp_path, [{"cell": "hyperliquid:jto", "fast_kill": True}], "run-20260823")
    assert mg.load_sealed(tmp_path) == {"hyperliquid:crv", "hyperliquid:jto"}


def test_a_sealed_cell_is_marked_and_never_promoted():
    cell = cell_from_chains(synth_chains(n=100_000, chain_len=5_000, seed=33))
    rep = mg.assess_cell(cell, k_cells=1, reps=100, sealed={"synthetic:plant"})
    assert rep["sealed"] is True
    assert rep["promote"] is False


# --------------------------------------------------------------------------
# 10. program-level fast kill (spec 9.2)
# --------------------------------------------------------------------------


def test_program_kill_is_not_evaluable_below_eight_cells():
    rows = [{"verdict": "FAIL", "M_patient": -5.0} for _ in range(7)]
    assert mg.program_kill(rows, control_passed=True) == "NOT_EVALUABLE"


def test_program_kill_fires_when_the_best_cell_is_deeply_dead():
    rows = [{"verdict": "FAIL", "M_patient": -5.0} for _ in range(8)]
    assert mg.program_kill(rows, control_passed=True) == "FAST_KILL"
    rows[0]["M_patient"] = -0.5
    assert mg.program_kill(rows, control_passed=True) == "ALIVE"


def test_program_kill_needs_a_passing_control():
    rows = [{"verdict": "FAIL", "M_patient": -5.0} for _ in range(8)]
    assert mg.program_kill(rows, control_passed=False) == "NOT_EVALUABLE"


# --------------------------------------------------------------------------
# 11. the spec's own acceptance numbers on the real tape (spec 7)
# --------------------------------------------------------------------------


@needs_tape
def test_v01_rules_reproduce_the_panel_probe_script_on_hype_0808():
    """spec 7, in the only form that is reproducible — see the sibling test.

    `mrr_probe.py` (panel scratchpad `wf71-mrr/`) is v0.1 as the panel coded it:
    key `hash`, 2 s staleness on the exchange clock, and a chain broken at every
    dropped order. This module reproduces its pooled output to every digit it
    printed, which is what §7 asks for: a disagreement is then the
    implementation, never the market.
    """
    cell = mg.read_cell(
        "hyperliquid",
        "hype",
        ["20260808"],
        rules=mg.LEGACY_V01,
        enforce_day_criteria=False,
    )
    chains = cell.chains()
    st = mg.estimate(chains, mg.FEE_BPS["hyperliquid"], rules=mg.LEGACY_V01)
    assert st["n_orders"] == 51_546
    assert st["eps_bar"] == pytest.approx(0.117177, abs=5e-6)
    assert st["s_bar"] == pytest.approx(0.240201, abs=5e-6)
    assert st["C1"] == pytest.approx(-0.018741, abs=5e-6)
    assert st["R1"] == pytest.approx(0.138125, abs=5e-6)
    assert st["R_inf"] == pytest.approx(0.766131, abs=5e-6)
    assert st["n_pairs"][50] == 22_676


@needs_tape
def test_the_spec_7_acceptance_row_is_not_reproducible_under_v01_chains():
    """SPEC_DEFECT pin (§7 acceptance row).

    §7 says the module must reproduce, "under v0.1 rules", `n = 51 546,
    eps_bar = 0.117, C(1) = 0.011, R(1) = 0.133, R_inf = 0.585, s_bar = 0.240`,
    citing `probe_hl_multi_0808.txt`. Three of those numbers are properties of
    the tape and hold under every rule set (`n`, `eps_bar`, `s_bar`); `R_inf`
    and `R(1)` only appear when dropped orders are **removed without breaking
    the chain** — which is neither v0.1 (§1.6 lists the break) nor v0.1-final
    (§1.5 keeps the order in the chain). Under real v0.1 rules the same day
    gives `R_inf = 0.766`, and `C(1) = +0.011` is reproduced by no variant at
    all: the panel's own two other artifacts print −0.0131 and −0.0187 for it.

    The row is therefore not usable as the frozen acceptance set. What is
    pinned here is the variant that reproduces four of its six numbers, plus
    the disagreement on `C(1)`, so the defect cannot be quietly lost.
    """
    variant = mg.EstimatorRules(
        "spec7-probe",
        aggregate_key="hash",
        stale_max_ns=2 * mg.NS,
        drop_breaks_chain=False,
        common_subset=False,
        centre_R=False,
    )
    cell = mg.read_cell(
        "hyperliquid", "hype", ["20260808"], rules=variant, enforce_day_criteria=False
    )
    chains = []
    for day in cell.series_by_day.values():
        for c in day:
            keep = np.isfinite(c.mid)
            if keep.any():
                chains.append(mg.Series(c.eps[keep], c.mid[keep], c.spread[keep]))
    st = mg.estimate(chains, 0.0, rules=variant)
    assert st["n_orders"] == 51_546
    assert st["eps_bar"] == pytest.approx(0.117, abs=0.0005)
    assert st["s_bar"] == pytest.approx(0.240, abs=0.0005)
    assert st["R_inf"] == pytest.approx(0.585, abs=0.001)
    assert st["R1"] == pytest.approx(0.133, abs=0.004)
    # the quoted C(1) = +0.011 is not reproduced by this or any other variant
    assert st["C1"] == pytest.approx(-0.0131, abs=0.0005)


@needs_tape
def test_the_frozen_rules_read_the_same_tape_with_the_taker_key():
    """spec 10.1: the taker key must beat the hash key on the at-touch gate."""
    cell = mg.read_cell("hyperliquid", "hype", ["20260808"], enforce_day_criteria=False)
    assert cell.frac_at_touch > mg.AT_TOUCH_MIN
    assert cell.diag["p_spread_1tick"] > mg.LARGE_TICK_P1  # HYPE is large-tick
