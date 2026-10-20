#!/usr/bin/env python3
"""MRR break-even gate — executor for `docs/spec-mrr-gate.md` (task #71).

The spec is the only source of truth. This module implements, in the spec's own
order and with its own names:

  §1.0  frozen constants                     — `SCHEMA` … `REPORTS_DIR` below
  §1.2  exchange clocks for alignment, local receive clock for gaps only
  §1.3  a trade is a market order, not a print (venue key + sign, consecutive)
  §1.4  the sign is a venue field; no tick rule, ever
  §1.5  mid/spread with the liveness rule (a quiet feed is not a stale feed)
  §1.6  chains, the common plateau subset, `C(l)`, `R(l)`, `R_inf`, `M_null`
  §1.7  `sigma~^2(20)`
  §1.8  block bootstrap with block escalation; widest interval decides
  §2    the two gate conditions and the fail-closed decision rule
  §3    the Fig 16.4 integrity gate and the at-touch predicate
  §4    day and cell inclusion criteria, including 4.3-11 `RFQ_MARKET`
  §5    the negative control (16 sealed HL coins) and the plant cell
  §9    verdicts, flags, the closed list of exclusion codes, FAST KILL
  §11   run artifacts, determinism

Everything the spec calls a decision lives here (§5.4 goodhart-M4): the module
refuses to write `cells.json` until the run directory holds a *passing*
`control.json` and a `sign-verification.json`, and `summary.md` is generated
from `cells.json` rather than written by hand.

Scope of this build (wf71, 2026-08-11): the reader layer implements
**hyperliquid** — the venue the pre-freeze control trial of §5.5 runs on. The
other five venues of §10 have their clocks, keys and populations frozen in the
spec but no reader here yet; `read_cell` raises for them rather than guessing,
so a missing venue is a loud gap and never a silent zero.

Read-only with respect to `/home/storage/hft-data`; writes only under
`REPORTS_DIR` (or wherever `--run-dir` points).
"""

from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import math
import os
import sys
from dataclasses import dataclass, field
from datetime import date as _date
from datetime import datetime, timezone
from typing import Iterable, NamedTuple, Sequence

import numpy as np

# ---------------------------------------------------------------------------
# §1.0 frozen constants — the single source of truth
# ---------------------------------------------------------------------------

SCHEMA = "mrr-gate/2"
WINDOW_DAYS = 10
WEEKDAYS_ONLY = True
MIN_QUALIFYING_DAYS = 5
MIN_ORDERS_PER_DAY = 200
MIN_ORDERS_PER_CELL = 10_000
MIN_PAIRS_PLATEAU = 1_000
PLATEAU_LAGS = range(20, 51)
GROWTH_LAGS = (50, 200)
SIGMA_LAG = 20
TOUCH_DEAD_S = 60.0
MAX_DROPPED_FRACTION = 0.10
GAP_BREAK_S = 60.0
DAY_COVERAGE_MIN_H = 23.0
DAY_MAX_GAP_MIN = 30.0
MALFORMED_MAX_FRAC = 0.001
RESPONSE_BAND = (0.4, 2.5)
AT_TOUCH_MIN = 0.80
R_PLATEAU_FLATNESS = 0.25
R_GROWTH_MAX = 0.25
LARGE_TICK_P1 = 0.5
EPS_DRIFT_MAX = 0.15
DAY_SHARE_MAX = 0.25
BOOTSTRAP_BLOCKS = (500, 2000, 8000)
BOOTSTRAP_REPS = 1000
BOOTSTRAP_SEED = 20260816
CI_LEVEL_BASE = 0.95
ALPHA_FAMILY = 0.05
BYBIT_LISTING_AGE_D = 28
FASTKILL_CELL_UB = 0.0
FASTKILL_PROGRAM_N = 8
FASTKILL_PROGRAM_BPS = -1.0
CAPACITY_FLOOR_USD_H = 10.0
QUOTE_NOTIONAL_USD = 100.0
REPORTS_DIR = "/home/storage/hft-data/reports/wf71-mrr/"

DATA_ROOT = "/home/storage/hft-data"

NS = 1_000_000_000
NS_MS = 1_000_000
NS_US = 1_000
TOUCH_DEAD_NS = int(TOUCH_DEAD_S * NS)
GAP_BREAK_NS = int(GAP_BREAK_S * NS)
DAY_MAX_GAP_NS = int(DAY_MAX_GAP_MIN * 60 * NS)

#: §1.9. `None` means the row is not filled/not confirmed → `FEE_UNKNOWN`.
#: bybit 2.0 is CONFIRMED by the operator (2026-08-11); lighter's declared 0.0
#: stays UNVERIFIED until `maker_fee` is decoded on one of our own fills.
FEE_BPS = {
    "hyperliquid": 1.4995,
    "bybit": 2.0,
    "lighter": None,
    "aster": None,
    "extended": None,
    "paradex": None,
}

#: §5.1 — the sealed control corpus (#57), ENA excluded, ten days, no weekday
#: filter.
CONTROL_COINS = (
    "aero", "crv", "dot", "ethfi", "gram", "hype", "inj", "jto",
    "kaito", "near", "ondo", "pendle", "pump", "sui", "vvv", "zro",
)
CONTROL_DAYS = (
    "20260730", "20260731", "20260801", "20260802", "20260803",
    "20260804", "20260805", "20260806", "20260807", "20260808",
)

#: §9.3 — the closed list.
EXCLUSION_CODES = (
    "WINDOW_COVERAGE",
    "INSUFFICIENT_N",
    "INSUFFICIENT_PAIRS",
    "STALE_QUOTES",
    "RESPONSE_SPREAD_BAND",
    "TOUCH_MISMATCH",
    "SIGN_STRUCTURE",
    "ESTIMATOR_UNDEFINED",
    "FEE_UNKNOWN",
    "SIGN_UNVERIFIED",
    "PARSE_ERROR",
    "RFQ_MARKET",
)

#: §2 — flags that make PASS impossible. `LARGE_TICK` and `EPS_DRIFT` are not
#: here on purpose (§1.5, §2): the first blocks promotion only, the second is a
#: reading caveat.
BLOCKING_FLAGS = (
    "R_INF_UNRESOLVED",
    "R_GROWTH",
    "R_GROWTH_UNCOMPUTABLE",
    "CI_UNCONVERGED",
    "CI_UNAVAILABLE",
    "DAY_CONCENTRATION",
)


@dataclass(frozen=True)
class EstimatorRules:
    """The estimator switches the spec froze — and the v0.1 set they replaced.

    `LEGACY_V01` exists for exactly one reason: §7 requires the module to
    reproduce the panel's probe numbers on `hype_20260808.gz` under v0.1 rules,
    so that a disagreement can be attributed to the implementation rather than
    to the market. Nothing in the gate path uses it.
    """

    name: str
    aggregate_key: str = "venue"      # "venue" = §10 key; "hash" = v0.1 (HL)
    stale_max_ns: int | None = None   # v0.1 age limit; None = liveness rule §1.5
    drop_breaks_chain: bool = False   # v0.1 broke the chain on a dropped mid
    common_subset: bool = True        # §1.6 stats-M4
    centre_R: bool = True             # §0 math-M3


SPEC_RULES = EstimatorRules("v0.1-final")
LEGACY_V01 = EstimatorRules(
    "v0.1",
    aggregate_key="hash",
    stale_max_ns=2 * NS,
    drop_breaks_chain=True,
    common_subset=False,
    centre_R=False,
)


class ControlNotSealed(RuntimeError):
    """§5.4 goodhart-M4 — the screen may not run before the control sealed."""


# ---------------------------------------------------------------------------
# small typed carriers
# ---------------------------------------------------------------------------


class Series(NamedTuple):
    """One chain (§1.6). `mid`/`spread` carry NaN where the mid is invalid."""

    eps: np.ndarray
    mid: np.ndarray
    spread: np.ndarray


class Print(NamedTuple):
    t_ex: int
    sign: int
    t_local: int
    key: str
    px: float
    qty: float


class Order(NamedTuple):
    t_ex: int
    sign: int
    t_local: int
    key: str
    px: float
    qty: float
    n_prints: int


class TouchFrames(NamedTuple):
    t_ex: np.ndarray
    t_local: np.ndarray
    bid: np.ndarray
    ask: np.ndarray
    bid_sz: np.ndarray
    ask_sz: np.ndarray


class OrderTable(NamedTuple):
    t_ex: np.ndarray
    t_local: np.ndarray
    sign: np.ndarray
    px: np.ndarray
    qty: np.ndarray
    n_prints: np.ndarray


class TouchResult(NamedTuple):
    mid: np.ndarray
    spread_bps: np.ndarray
    spread_price: np.ndarray
    valid: np.ndarray
    at_touch: np.ndarray
    ate_touch: np.ndarray
    touch_known: np.ndarray


@dataclass
class CellInput:
    """Everything a verdict needs, with the file reading already done."""

    venue: str
    symbol: str
    fee_bps: float | None
    sign_verified: bool
    rfq: bool
    series_by_day: dict[str, list[Series]]
    n_orders: int
    n_dropped_mid: int
    n_qualifying_days: int
    frac_at_touch: float | None
    diag: dict = field(default_factory=dict)

    @property
    def cell_id(self) -> str:
        return f"{self.venue}:{self.symbol}"

    def chains(self) -> list[Series]:
        return [c for day in self.series_by_day.values() for c in day]


# ---------------------------------------------------------------------------
# synthetic generators: the plant (§5.2.4) and its building blocks
# ---------------------------------------------------------------------------


def markov_signs(n: int, rho: float, rng: np.random.Generator) -> np.ndarray:
    """Two-state Markov chain of signs with `C(l) = rho^l` (book Eq 16.25)."""
    p_same = (1.0 + rho) / 2.0
    mult = np.where(rng.random(n) < p_same, 1.0, -1.0)
    mult[0] = 1.0 if rng.random() < 0.5 else -1.0
    return np.cumprod(mult)


def mrr_surface(
    eps: np.ndarray,
    g_bps: float,
    rho: float,
    m0: float = 100.0,
    drift_bps: float = 0.0,
) -> np.ndarray:
    """MRR mid: `dm_t = G(eps_t - rho eps_{t-1})`, applied after order `t`.

    `mid[t]` is the mid **before** order `t`, so `R(l) = G(1 - rho^2)` for every
    `l >= 1` — flat by construction, which is what makes the plant's answer
    known. Increments are multiplicative so that a bps move stays a bps move
    however far the level wanders.
    """
    n = len(eps)
    d = np.empty(n)
    d[0] = g_bps * eps[0]
    d[1:] = g_bps * (eps[1:] - rho * eps[:-1])
    d = d + drift_bps
    logf = np.log1p(d / 1e4)
    cum = np.concatenate([[0.0], np.cumsum(logf)[:-1]])
    return m0 * np.exp(cum)


def plant_series(
    n: int = 100_000,
    chain_len: int = 5_000,
    rho: float = 0.5,
    g_bps: float = 2.0,
    spread_bps: float = 6.0,
    n_days: int = 10,
    seed: int = BOOTSTRAP_SEED,
) -> dict[str, list[Series]]:
    """§5.2.4 verbatim: rho=0.5, G=2 bps, s=6 bps, N=100k in chains of 5000.

    The spec does not say how the plant's chains sit in days, and two per-day
    rules need to know (`DAY_CONCENTRATION`, the day-median co-condition), so
    they are spread evenly over ten synthetic days — the same count as the
    control window. Recorded as an interpretation, not a free parameter: any
    even split gives day_share = 1/n_days <= 0.25 and the same pooled answer.
    """
    rng = np.random.default_rng(seed)
    out: dict[str, list[Series]] = {}
    n_chains = n // chain_len
    for i in range(n_chains):
        eps = markov_signs(chain_len, rho, rng)
        mid = mrr_surface(eps, g_bps, rho)
        spread = np.full(chain_len, float(spread_bps))
        day = f"plant-{i % n_days:02d}"
        out.setdefault(day, []).append(Series(eps, mid, spread))
    return out


def plant_cell(seed: int = BOOTSTRAP_SEED) -> CellInput:
    by_day = plant_series(seed=seed)
    n = sum(len(c.eps) for day in by_day.values() for c in day)
    return CellInput(
        venue="plant",
        symbol="mrr",
        fee_bps=0.0,          # §5.2.4: fee 0
        sign_verified=True,   # synthetic: the sign is the generator's own
        rfq=False,
        series_by_day=by_day,
        n_orders=n,
        n_dropped_mid=0,
        n_qualifying_days=len(by_day),
        frac_at_touch=1.0,
        diag={"synthetic": True, "M_patient_true": 1.5},
    )


# ---------------------------------------------------------------------------
# §1.6 / §1.7 estimators
# ---------------------------------------------------------------------------


def _plateau_base(valid: np.ndarray, n: int) -> np.ndarray:
    """§1.6 stats-M4: base indices shared by every plateau lag.

    `t` qualifies iff `t + 50` is in the same chain and the mid is valid at `t`
    and at every `t + l`, `l in 20..50` — which is exactly what makes `n_pairs`
    identical across the plateau.
    """
    base = np.zeros(n, dtype=bool)
    lo, hi = min(PLATEAU_LAGS), max(PLATEAU_LAGS)
    if n <= hi:
        return base
    cs = np.concatenate([[0], np.cumsum(valid.astype(np.int64))])
    t = np.arange(n - hi)
    window = cs[t + hi + 1] - cs[t + lo]
    base[: n - hi] = valid[: n - hi] & (window == (hi - lo + 1))
    return base


def _rel(mid: np.ndarray, i: np.ndarray, lag: int) -> np.ndarray:
    return 1e4 * (mid[i + lag] - mid[i]) / mid[i]


def estimate(
    series: Sequence[Series],
    fee_bps: float,
    rules: EstimatorRules = SPEC_RULES,
) -> dict:
    """All per-cell estimators of §1.6-§1.7 and the gate arithmetic of §2-§3."""
    plateau = list(PLATEAU_LAGS)
    n_orders = 0
    sum_eps = 0.0
    c1_num = 0.0
    c1_cnt = 0
    r1_ex = r1_x = 0.0
    r1_cnt = 0
    pc_ex = pc_x = 0.0
    pc_cnt = 0
    pair1_cnt = 0
    plat_cnt = 0
    plat_ee = np.zeros(len(plateau))
    plat_ex = np.zeros(len(plateau))
    plat_x = np.zeros(len(plateau))
    plat_cnt_lag = np.zeros(len(plateau), dtype=np.int64)
    x20_sum = x20_sq = 0.0
    x20_cnt = 0
    s_sum = s_sq = 0.0
    s_cnt = 0
    g_cnt = 0
    g_ex = np.zeros(2)
    g_x = np.zeros(2)

    for ser in series:
        eps = np.asarray(ser.eps, dtype=float)
        mid = np.asarray(ser.mid, dtype=float)
        spr = np.asarray(ser.spread, dtype=float)
        n = len(eps)
        if n == 0:
            continue
        n_orders += n
        sum_eps += float(eps.sum())
        valid = np.isfinite(mid) & (mid > 0)

        sv = np.isfinite(spr)
        if sv.any():
            s_sum += float(spr[sv].sum())
            s_sq += float((spr[sv] ** 2).sum())
            s_cnt += int(sv.sum())

        if n > 1:
            c1_num += float(eps[:-1] @ eps[1:])
            c1_cnt += n - 1
            m1 = valid[:-1] & valid[1:]
            if m1.any():
                i = np.nonzero(m1)[0]
                x = _rel(mid, i, 1)
                r1_ex += float(eps[i] @ x)
                r1_x += float(x.sum())
                r1_cnt += len(i)
                pair1_cnt += len(i)
                moved = mid[i + 1] != mid[i]
                if moved.any():
                    pc_ex += float(eps[i][moved] @ x[moved])
                    pc_x += float(x[moved].sum())
                    pc_cnt += int(moved.sum())

        if rules.common_subset:
            base = _plateau_base(valid, n)
            idx = np.nonzero(base)[0]
            plat_cnt += len(idx)
            for j, lag in enumerate(plateau):
                if len(idx) == 0:
                    continue
                x = _rel(mid, idx, lag)
                plat_ee[j] += float(eps[idx] @ eps[idx + lag])
                plat_ex[j] += float(eps[idx] @ x)
                plat_x[j] += float(x.sum())
                plat_cnt_lag[j] += len(idx)
                if lag == SIGMA_LAG:
                    x20_sum += float(x.sum())
                    x20_sq += float((x * x).sum())
                    x20_cnt += len(x)
        else:
            for j, lag in enumerate(plateau):
                if n <= lag:
                    continue
                m = valid[:-lag] & valid[lag:]
                idx = np.nonzero(m)[0]
                if len(idx) == 0:
                    continue
                x = _rel(mid, idx, lag)
                plat_ee[j] += float(eps[idx] @ eps[idx + lag])
                plat_ex[j] += float(eps[idx] @ x)
                plat_x[j] += float(x.sum())
                plat_cnt_lag[j] += len(idx)
                if lag == SIGMA_LAG:
                    x20_sum += float(x.sum())
                    x20_sq += float((x * x).sum())
                    x20_cnt += len(x)

        # growth test (§1.6 math-C3(в)): its own subset, both legs on it
        lag_lo, lag_hi = GROWTH_LAGS
        if n > lag_hi:
            m = np.zeros(n, dtype=bool)
            t = np.arange(n - lag_hi)
            m[: n - lag_hi] = valid[t] & valid[t + lag_lo] & valid[t + lag_hi]
            idx = np.nonzero(m)[0]
            if len(idx):
                g_cnt += len(idx)
                for j, lag in enumerate(GROWTH_LAGS):
                    x = _rel(mid, idx, lag)
                    g_ex[j] += float(eps[idx] @ x)
                    g_x[j] += float(x.sum())

    nan = float("nan")
    eps_bar = sum_eps / n_orders if n_orders else nan
    centre = eps_bar if rules.centre_R else 0.0

    def _cov(num_ex, num_x, cnt):
        if cnt <= 0:
            return nan
        return num_ex / cnt - centre * (num_x / cnt)

    C1 = (c1_num / c1_cnt - eps_bar**2) if c1_cnt else nan
    R1 = _cov(r1_ex, r1_x, r1_cnt)
    R1_pc = _cov(pc_ex, pc_x, pc_cnt)

    C_plateau, R_plateau, R_raw, E_x, n_pairs = {}, {}, {}, {}, {}
    for j, lag in enumerate(plateau):
        cnt = int(plat_cnt_lag[j])
        n_pairs[lag] = cnt
        if cnt:
            C_plateau[lag] = plat_ee[j] / cnt - eps_bar**2
            R_plateau[lag] = _cov(plat_ex[j], plat_x[j], cnt)
            R_raw[lag] = plat_ex[j] / cnt
            E_x[lag] = plat_x[j] / cnt
        else:
            C_plateau[lag] = R_plateau[lag] = R_raw[lag] = E_x[lag] = nan
    n_pairs[1] = c1_cnt

    plateau_ok = all(n_pairs[l] > 0 for l in plateau)
    R_inf = float(np.mean([R_plateau[l] for l in plateau])) if plateau_ok else nan
    C_bar = float(np.mean([C_plateau[l] for l in plateau])) if plateau_ok else nan
    R_min = float(np.min([R_plateau[l] for l in plateau])) if plateau_ok else nan
    if plateau_ok:
        with np.errstate(divide="ignore", invalid="ignore"):
            R_hat = float(
                np.mean([R_plateau[l] / (1.0 - C_plateau[l]) for l in plateau])
            )
    else:
        R_hat = nan

    s_bar = s_sum / s_cnt if s_cnt else nan
    mean_s2 = s_sq / s_cnt if s_cnt else nan
    var_s = mean_s2 - s_bar**2 if s_cnt else nan
    CV_s = math.sqrt(var_s) / s_bar if s_cnt and var_s > 0 and s_bar > 0 else nan

    if x20_cnt > 1:
        var20 = (x20_sq - x20_sum**2 / x20_cnt) / (x20_cnt - 1)
        sigma_tilde_sq_20 = var20 / SIGMA_LAG
    else:
        sigma_tilde_sq_20 = nan

    R_flatness = (
        abs(R_plateau[50] - R_plateau[20]) / abs(R_inf)
        if plateau_ok and np.isfinite(R_inf) and R_inf != 0
        else nan
    )

    if g_cnt:
        R_g = [_cov(g_ex[j], g_x[j], g_cnt) for j in range(2)]
        R_growth = (R_g[1] / R_g[0] - 1.0) if R_g[0] != 0 else nan
    else:
        R_g = [nan, nan]
        R_growth = nan
    n_pairs[200] = g_cnt

    response_ratio = (
        2.0 * R1 / ((1.0 - C1) * s_bar)
        if np.isfinite(R1) and np.isfinite(C1) and np.isfinite(s_bar) and s_bar > 0 and C1 != 1
        else nan
    )
    denom = mean_s2 * (1 - C1**2) / 4 if np.isfinite(mean_s2) and np.isfinite(C1) else nan
    rho_val = sigma_tilde_sq_20 / denom if np.isfinite(denom) and denom > 0 else nan

    M_null = (s_bar / 2) * C_bar if np.isfinite(s_bar) and np.isfinite(C_bar) else nan
    s_eff = s_bar - 2 * fee_bps
    M_fast = s_eff * (1 - C1) / 2 - R1
    M_patient = s_eff / 2 - R_inf
    M_patient_best = s_eff / 2 - R_min

    core = [C1, R1, R_inf, C_bar, s_bar, mean_s2, sigma_tilde_sq_20, M_null]
    finite = bool(n_orders > 0 and plateau_ok and c1_cnt > 0 and r1_cnt > 0)
    finite = finite and all(np.isfinite(v) for v in core)

    return dict(
        n_orders=n_orders,
        eps_bar=eps_bar,
        s_bar=s_bar,
        mean_s2=mean_s2,
        CV_s=CV_s,
        C1=C1,
        C_plateau=C_plateau,
        C_plateau_bar=C_bar,
        R1=R1,
        R1_price_changing_bps=R1_pc,
        p_mo_price_changing=(pc_cnt / pair1_cnt) if pair1_cnt else nan,
        R_plateau=R_plateau,
        R_raw=R_raw,
        E_x=E_x,
        R_inf=R_inf,
        R_min_plateau=R_min,
        R_hat_inf_1622=R_hat,
        R_flatness=R_flatness,
        R_growth=R_growth,
        R_50_growthset=R_g[0],
        R_200_growthset=R_g[1],
        sigma_tilde_sq_20=sigma_tilde_sq_20,
        rho_val=rho_val,
        response_ratio=response_ratio,
        n_pairs=n_pairs,
        n_pairs_R1=r1_cnt,
        n_pairs_plateau=plat_cnt if rules.common_subset else int(plat_cnt_lag[-1]),
        n_pairs_growth=g_cnt,
        M_null=M_null,
        s_eff=s_eff,
        M_fast=M_fast,
        M_patient=M_patient,
        M_patient_best=M_patient_best,
        finite=finite,
    )


# ---------------------------------------------------------------------------
# §1.8 block bootstrap with block escalation
# ---------------------------------------------------------------------------


def blocks_per_replicate(n_in_chains: int, block: int) -> int:
    """§1.8 goodhart-M2: `k = ceil(N_in_chains / b)`, frozen."""
    return max(1, -(-int(n_in_chains) // int(block)))


def family_lower_bound(dist, k_cells: int) -> float:
    """§2 stats-C4: one-sided Bonferroni bound at confidence `1 - 0.05/K`.

    The spec's prose says "percentile of level 1 - 0.05/K"; a *lower* bound at
    confidence `1 - alpha` is the `alpha` quantile, so the percentile taken here
    is `100 * 0.05 / K`. Read the other way the bound would sit above the point
    estimate and make PASS easier as the family grows, which inverts the
    correction the clause exists for. Recorded as SPEC_DEFECT-candidate.
    """
    q = 100.0 * ALPHA_FAMILY / max(1, int(k_cells))
    return float(np.percentile(np.asarray(dist, dtype=float), q))


def ci_unconverged(per_b: dict) -> bool:
    """§1.8: SD at the largest computable block > 1.25x the previous one."""
    bs = sorted(per_b)
    if len(bs) < 2:
        return False
    last, prev = per_b[bs[-1]]["sd_D"], per_b[bs[-2]]["sd_D"]
    if not (np.isfinite(last) and np.isfinite(prev)) or prev <= 0:
        return False
    return bool(last > 1.25 * prev)


def _prefix(arr: np.ndarray) -> np.ndarray:
    out = np.zeros(len(arr) + 1, dtype=np.float64)
    np.cumsum(arr, out=out[1:])
    return out


def _bootstrap_tables(series: Sequence[Series]):
    """Per-index contributions, concatenated, with chain offsets.

    Blocks never cross a chain boundary, so a block sum is a difference of two
    prefix sums and no pair is ever formed across a block seam (§1.8).
    """
    plateau = list(PLATEAU_LAGS)
    cols: dict[str, list[np.ndarray]] = {
        "eps": [], "s": [], "s2": [], "sv": [],
        "c1": [], "c1n": [], "r1e": [], "r1x": [], "r1n": [], "basen": [],
    }
    for lag in plateau:
        cols[f"pe{lag}"] = []
        cols[f"px{lag}"] = []
        cols[f"pex{lag}"] = []
    bounds = []
    off = 0
    for ser in series:
        eps = np.asarray(ser.eps, dtype=float)
        mid = np.asarray(ser.mid, dtype=float)
        spr = np.asarray(ser.spread, dtype=float)
        n = len(eps)
        if n == 0:
            continue
        valid = np.isfinite(mid) & (mid > 0)
        sv = np.isfinite(spr)
        s = np.where(sv, np.nan_to_num(spr), 0.0)
        cols["eps"].append(eps)
        cols["s"].append(s)
        cols["s2"].append(s * s)
        cols["sv"].append(sv.astype(float))

        c1 = np.zeros(n)
        c1n = np.zeros(n)
        if n > 1:
            c1[:-1] = eps[:-1] * eps[1:]
            c1n[:-1] = 1.0
        cols["c1"].append(c1)
        cols["c1n"].append(c1n)

        r1e = np.zeros(n)
        r1x = np.zeros(n)
        r1n = np.zeros(n)
        if n > 1:
            m1 = valid[:-1] & valid[1:]
            idx = np.nonzero(m1)[0]
            if len(idx):
                x = _rel(mid, idx, 1)
                r1e[idx] = eps[idx] * x
                r1x[idx] = x
                r1n[idx] = 1.0
        cols["r1e"].append(r1e)
        cols["r1x"].append(r1x)
        cols["r1n"].append(r1n)

        base = _plateau_base(valid, n)
        idx = np.nonzero(base)[0]
        cols["basen"].append(base.astype(float))
        for lag in plateau:
            pe = np.zeros(n)
            px = np.zeros(n)
            pex = np.zeros(n)
            if len(idx):
                x = _rel(mid, idx, lag)
                pe[idx] = eps[idx] * eps[idx + lag]
                px[idx] = x
                pex[idx] = eps[idx] * x
            cols[f"pe{lag}"].append(pe)
            cols[f"px{lag}"].append(px)
            cols[f"pex{lag}"].append(pex)

        bounds.append((off, off + n))
        off += n

    if not bounds:
        return None, [], 0
    pref = {}
    for k in list(cols):
        pref[k] = _prefix(np.concatenate(cols.pop(k)))  # free as we go: 100+ lags
    return pref, bounds, off


def bootstrap_cell(
    series: Sequence[Series],
    fee_bps: float,
    reps: int = BOOTSTRAP_REPS,
    blocks: Sequence[int] = BOOTSTRAP_BLOCKS,
    seed: int = BOOTSTRAP_SEED,
) -> dict:
    """§1.8: escalate the block length; the widest computable interval decides."""
    plateau = list(PLATEAU_LAGS)
    pref, bounds, n_total = _bootstrap_tables(series)
    out: dict = {"per_b": {}, "flags": [], "chosen_b": None, "coverage": {}}
    if pref is None:
        out["flags"].append("CI_UNAVAILABLE")
        return out

    lo_alpha = 100.0 * (1.0 - CI_LEVEL_BASE) / 2.0
    hi_alpha = 100.0 - lo_alpha

    for b in blocks:
        starts = np.concatenate(
            [np.arange(a, e - b + 1, dtype=np.int64) for a, e in bounds if e - a >= b]
            or [np.empty(0, dtype=np.int64)]
        )
        if len(starts) == 0:
            continue
        covered = sum(e - a for a, e in bounds if e - a >= b)
        k = blocks_per_replicate(n_total, b)
        rng = np.random.default_rng(seed + b)
        s = starts[rng.integers(0, len(starts), size=(reps, k))]

        def blk(name: str, span: int) -> np.ndarray:
            p = pref[name]
            return (p[s + span] - p[s]).sum(axis=1)

        n_rep = float(k * b)
        eps_bar = blk("eps", b) / n_rep
        sv = blk("sv", b)
        s_bar = np.where(sv > 0, blk("s", b) / np.maximum(sv, 1), np.nan)
        c1n = blk("c1n", b - 1)
        C1 = blk("c1", b - 1) / np.maximum(c1n, 1) - eps_bar**2
        r1n = blk("r1n", b - 1)
        R1 = blk("r1e", b - 1) / np.maximum(r1n, 1) - eps_bar * (
            blk("r1x", b - 1) / np.maximum(r1n, 1)
        )
        span_p = max(b - max(plateau), 0)
        cnt = blk("basen", span_p) if span_p else np.zeros(reps)
        cntm = np.maximum(cnt, 1)
        R_l = np.empty((len(plateau), reps))
        C_l = np.empty((len(plateau), reps))
        for j, lag in enumerate(plateau):
            if span_p:
                R_l[j] = blk(f"pex{lag}", span_p) / cntm - eps_bar * (
                    blk(f"px{lag}", span_p) / cntm
                )
                C_l[j] = blk(f"pe{lag}", span_p) / cntm - eps_bar**2
            else:
                R_l[j] = np.nan
                C_l[j] = np.nan
        R_inf = R_l.mean(axis=0)
        R_min = R_l.min(axis=0)
        C_bar = C_l.mean(axis=0)
        M_null = s_bar / 2 * C_bar
        s_eff = s_bar - 2 * fee_bps
        M_patient = s_eff / 2 - R_inf
        M_fast = s_eff * (1 - C1) / 2 - R1
        M_best = s_eff / 2 - R_min
        D = M_patient - M_null

        ok = np.isfinite(D) & (cnt > 0)
        if ok.sum() < max(10, reps // 10):
            continue
        D = D[ok]
        out["per_b"][b] = dict(
            k=k,
            n_starts=int(len(starts)),
            coverage=covered / n_total if n_total else 0.0,
            sd_D=float(np.std(D, ddof=1)),
            lb_D=float(np.percentile(D, lo_alpha)),
            ub_D=float(np.percentile(D, hi_alpha)),
            sd_M_patient=float(np.std(M_patient[ok], ddof=1)),
            lb_M_patient=float(np.percentile(M_patient[ok], lo_alpha)),
            ub_M_patient=float(np.percentile(M_patient[ok], hi_alpha)),
            ub_M_patient_best=float(np.percentile(M_best[ok], hi_alpha)),
            sd_M_fast=float(np.std(M_fast[ok], ddof=1)),
            sd_s_bar=float(np.std(s_bar[ok], ddof=1)),
            _dist_D=D,
        )
        out["coverage"][b] = out["per_b"][b]["coverage"]

    if not out["per_b"]:
        out["flags"].append("CI_UNAVAILABLE")
        return out
    widths = {b: v["ub_D"] - v["lb_D"] for b, v in out["per_b"].items()}
    out["chosen_b"] = max(widths, key=lambda b: (widths[b], b))
    if ci_unconverged(out["per_b"]):
        out["flags"].append("CI_UNCONVERGED")
    return out


# ---------------------------------------------------------------------------
# §2 / §9 verdict
# ---------------------------------------------------------------------------


def regime_of(m_fast: float, m_patient: float) -> str:
    """§2 math-M1: `slow_only` is a region on Fig 17.2, not a malfunction."""
    if m_patient > 0 and m_fast > 0:
        return "both"
    if m_patient > 0 >= m_fast:
        return "slow_only"
    if m_fast > 0 >= m_patient:
        return "fast_only"
    return "neither"


def _day_share_max(series_by_day: dict[str, list[Series]]) -> float:
    counts = {d: sum(len(c.eps) for c in ch) for d, ch in series_by_day.items()}
    total = sum(counts.values())
    return max(counts.values()) / total if total else float("nan")


def assess_cell(
    cell: CellInput,
    k_cells: int = 1,
    reps: int = BOOTSTRAP_REPS,
    seed: int = BOOTSTRAP_SEED,
    rules: EstimatorRules = SPEC_RULES,
    history: dict | None = None,
    sealed: Iterable[str] = (),
    capacity: dict | None = None,
) -> dict:
    """One cell → one `cells.json` record (§4.3, §2, §3, §6, §8, §9)."""
    nan = float("nan")
    rec: dict = {
        "cell": cell.cell_id,
        "venue": cell.venue,
        "symbol": cell.symbol,
        "schema": SCHEMA,
        "fee_bps": cell.fee_bps,
        "n_orders": cell.n_orders,
        "n_qualifying_days": cell.n_qualifying_days,
        "exclusion_reasons": [],
        "flags": [],
        "verdict": None,
        "promote": False,
        "marginal": False,
        "fast_kill": False,
        "sealed": cell.cell_id in set(sealed),
        "M_patient": None,
        "M_fast": None,
        "M_null_bps": None,
        "D_patient_lb_family": None,
        "ci_M_patient": None,
        "regime": None,
        "n_screen_attempts": (history or {}).get("attempts", 0) + 1,
        "bybit_reading": ("unaged" if cell.venue == "bybit" else None),
    }
    rec.update({k: v for k, v in (cell.diag or {}).items()})

    # §4.3-11: the RFQ flag is read before any arithmetic.
    if cell.rfq:
        rec["exclusion_reasons"] = ["RFQ_MARKET"]
        rec["verdict"] = "EXCLUDED"
        return rec

    chains = cell.chains()
    fee_known = cell.fee_bps is not None
    fee = float(cell.fee_bps) if fee_known else 0.0
    st = estimate(chains, fee, rules=rules)

    dropped_frac = cell.n_dropped_mid / cell.n_orders if cell.n_orders else nan
    day_share = _day_share_max(cell.series_by_day)

    reasons: list[str] = []
    if cell.n_qualifying_days < MIN_QUALIFYING_DAYS:
        reasons.append("WINDOW_COVERAGE")
    if cell.n_orders < MIN_ORDERS_PER_CELL:
        reasons.append("INSUFFICIENT_N")
    if st["n_pairs"].get(50, 0) < MIN_PAIRS_PLATEAU:
        reasons.append("INSUFFICIENT_PAIRS")
    if not (np.isfinite(dropped_frac) and dropped_frac <= MAX_DROPPED_FRACTION):
        reasons.append("STALE_QUOTES")
    if not st["finite"]:
        reasons.append("ESTIMATOR_UNDEFINED")
    else:
        lo, hi = RESPONSE_BAND
        if not (lo < st["response_ratio"] < hi):
            reasons.append("RESPONSE_SPREAD_BAND")
        if not (st["C1"] > 0 and st["C_plateau_bar"] <= st["C1"]):
            reasons.append("SIGN_STRUCTURE")
    if cell.frac_at_touch is None or not (cell.frac_at_touch >= AT_TOUCH_MIN):
        reasons.append("TOUCH_MISMATCH")
    if not fee_known:
        reasons.append("FEE_UNKNOWN")
    if not cell.sign_verified:
        reasons.append("SIGN_UNVERIFIED")

    # printed regardless of the verdict (§9: EXCLUDED is not interpreted, but
    # the inputs are still on the record)
    rec.update(
        eps_bar=_f(st["eps_bar"]),
        s_bar=_f(st["s_bar"]),
        s_eff=_f(st["s_eff"]) if fee_known else None,
        mean_s2=_f(st["mean_s2"]),
        CV_s=_f(st["CV_s"]),
        C1=_f(st["C1"]),
        C_plateau_bar=_f(st["C_plateau_bar"]),
        R1=_f(st["R1"]),
        R_inf=_f(st["R_inf"]),
        R_hat_inf_1622=_f(st["R_hat_inf_1622"]),
        R_flatness=_f(st["R_flatness"]),
        R_growth=_f(st["R_growth"]),
        R_raw_20=_f(st["R_raw"].get(20, nan)),
        R_raw_50=_f(st["R_raw"].get(50, nan)),
        E_x20=_f(st["E_x"].get(20, nan)),
        E_x50=_f(st["E_x"].get(50, nan)),
        R1_price_changing_bps=_f(st["R1_price_changing_bps"]),
        p_mo_price_changing=_f(st["p_mo_price_changing"]),
        sigma_tilde_sq_20=_f(st["sigma_tilde_sq_20"]),
        rho_val=_f(st["rho_val"]),
        response_ratio=_f(st["response_ratio"]),
        frac_at_touch=cell.frac_at_touch,
        n_pairs={str(k): int(v) for k, v in st["n_pairs"].items()},
        dropped_fraction=_f(dropped_frac),
        day_share_max=_f(day_share),
    )
    if fee_known and st["finite"]:
        rec["M_patient"] = _f(st["M_patient"])
        rec["M_fast"] = _f(st["M_fast"])
        rec["M_null_bps"] = _f(st["M_null"])
        rec["regime"] = regime_of(st["M_fast"], st["M_patient"])

    if reasons:
        rec["exclusion_reasons"] = sorted(set(reasons))
        rec["verdict"] = "EXCLUDED"
        return rec

    # ---- flags (§1.5-§1.8) ----
    flags: list[str] = []
    if not (np.isfinite(st["R_flatness"]) and st["R_flatness"] <= R_PLATEAU_FLATNESS):
        flags.append("R_INF_UNRESOLVED")
    if st["n_pairs"].get(200, 0) < MIN_PAIRS_PLATEAU or not np.isfinite(st["R_growth"]):
        flags.append("R_GROWTH_UNCOMPUTABLE")
    elif st["R_growth"] > R_GROWTH_MAX:
        flags.append("R_GROWTH")
    if np.isfinite(day_share) and day_share > DAY_SHARE_MAX:
        flags.append("DAY_CONCENTRATION")
    p1t = rec.get("p_spread_1tick")
    if p1t is not None and np.isfinite(p1t) and p1t > LARGE_TICK_P1:
        flags.append("LARGE_TICK")
    if np.isfinite(st["eps_bar"]) and abs(st["eps_bar"]) > EPS_DRIFT_MAX:
        flags.append("EPS_DRIFT")

    boot = bootstrap_cell(chains, fee, reps=reps, seed=seed)
    flags.extend(boot["flags"])
    chosen = boot["chosen_b"]
    rec["bootstrap"] = {
        f"sd_b{b}": _f(boot["per_b"][b]["sd_D"]) if b in boot["per_b"] else None
        for b in BOOTSTRAP_BLOCKS
    }
    rec["bootstrap"].update(
        chosen_b=chosen,
        coverage=_f(boot["coverage"].get(chosen)) if chosen else None,
        k=boot["per_b"][chosen]["k"] if chosen else None,
        sd_M_patient=_f(boot["per_b"][chosen]["sd_M_patient"]) if chosen else None,
    )
    if chosen is not None:
        dist = boot["per_b"][chosen]["_dist_D"]
        rec["D_patient_lb_family"] = _f(family_lower_bound(dist, k_cells))
        rec["ci_M_patient"] = [
            _f(boot["per_b"][chosen]["lb_M_patient"]),
            _f(boot["per_b"][chosen]["ub_M_patient"]),
        ]
        rec["ub95_M_patient_best"] = _f(boot["per_b"][chosen]["ub_M_patient_best"])

    # ---- per-day agreement (§2 stats-M5) ----
    day_M = {}
    for day, ch in sorted(cell.series_by_day.items()):
        d_st = estimate(ch, fee, rules=rules)
        if d_st["finite"]:
            day_M[day] = _f(d_st["M_patient"])
    rec["per_day_M_patient"] = day_M
    day_median = float(np.median(list(day_M.values()))) if day_M else nan
    rec["day_median_M_patient"] = _f(day_median)

    rec["flags"] = sorted(set(flags))

    blocking = [f for f in rec["flags"] if f in BLOCKING_FLAGS]
    lb = rec["D_patient_lb_family"]
    point_above = bool(np.isfinite(st["M_patient"]) and st["M_patient"] > st["M_null"])
    median_above = bool(np.isfinite(day_median) and day_median > st["M_null"])
    passed = bool(lb is not None and lb > 0 and median_above and not blocking)

    if passed:
        replicated = bool((history or {}).get("replicated", False))
        rec["verdict"] = "PASS" if replicated else "PASS_UNREPLICATED"
    else:
        rec["verdict"] = "FAIL"
        rec["marginal"] = point_above

    # ---- cell-level FAST KILL (§9.2) ----
    ub_best = rec.get("ub95_M_patient_best")
    seal_blockers = {"R_INF_UNRESOLVED", "CI_UNCONVERGED", "CI_UNAVAILABLE"}
    if (
        cell.n_orders >= 3 * MIN_ORDERS_PER_CELL
        and st["n_pairs"].get(50, 0) >= MIN_PAIRS_PLATEAU
        and not (set(rec["flags"]) & seal_blockers)
        and ub_best is not None
        and ub_best < FASTKILL_CELL_UB
    ):
        rec["fast_kill"] = True

    # §6 co-gate. Layer 1 is readable off the tape; layer 2 needs `fills/hr`
    # from `backtest_first.py`, which impl-M2 records as unavailable outside HL
    # — no run, no number, `UNKNOWN`, no promotion.
    cap = dict(capacity or {})
    turnover = (cell.diag or {}).get("turnover_usd_median")
    cap.setdefault("turnover_usd_median", turnover)
    cap.setdefault("alive_turnover", (turnover >= 1e6) if turnover is not None else None)
    cap.setdefault("touch_notional_usd_tw", None)
    fills_hr = cap.get("fills_per_hour")
    if fills_hr is not None and rec["M_patient"] is not None:
        cap["usd_per_hour_upper_bound"] = (
            fills_hr * rec["M_patient"] * QUOTE_NOTIONAL_USD / 1e4
        )
        cap["status"] = (
            "OK"
            if (cap["usd_per_hour_upper_bound"] >= CAPACITY_FLOOR_USD_H
                and cap.get("alive_turnover"))
            else "BELOW_FLOOR"
        )
    else:
        cap.setdefault("usd_per_hour_upper_bound", None)
        cap.setdefault("status", "UNKNOWN")
    cap["quote_notional_usd"] = QUOTE_NOTIONAL_USD
    rec["capacity"] = cap

    rec["promote"] = bool(
        rec["verdict"] == "PASS"
        and cap.get("status") == "OK"
        and "LARGE_TICK" not in rec["flags"]
        and rec["bybit_reading"] in (None, "news")
        and not rec["sealed"]
    )
    return rec


def _f(v):
    """JSON-safe float: NaN/Inf never reach the artifact as bare tokens."""
    if v is None:
        return None
    v = float(v)
    return v if math.isfinite(v) else None


def program_kill(rows: Sequence[dict], control_passed: bool) -> str:
    """§9.2: pre-registered program-level stop, with `NOT_EVALUABLE` printed."""
    included = [
        r for r in rows
        if r.get("verdict") not in ("EXCLUDED", None) and r.get("M_patient") is not None
    ]
    if not control_passed or len(included) < FASTKILL_PROGRAM_N:
        return "NOT_EVALUABLE"
    best = max(r["M_patient"] for r in included)
    return "FAST_KILL" if best < FASTKILL_PROGRAM_BPS else "ALIVE"


# ---------------------------------------------------------------------------
# §5 control protocol
# ---------------------------------------------------------------------------


def control_criteria(rows: Sequence[dict], plant_ok: bool) -> dict:
    """§5.2 — the four criteria, evaluated together, all failures reported."""
    readable = [r for r in rows if r.get("verdict") != "EXCLUDED"]
    failures = []
    if len(readable) < 14:
        failures.append("too_few_readable")
    positives = [
        r["cell"]
        for r in readable
        if (r.get("M_patient") is None or r["M_patient"] >= 0)
        or (r.get("M_fast") is None or r["M_fast"] >= 0)
    ]
    if positives:
        failures.append("positive_cell")
    if not plant_ok:
        failures.append("plant")
    return {
        "passed": not failures,
        "failures": failures,
        "n_cells": len(rows),
        "n_readable": len(readable),
        "positive_cells": positives,
        "plant_ok": bool(plant_ok),
    }


def require_sealed_control(run_dir) -> dict:
    """§5.4 goodhart-M4 — enforced by the tool, not by self-attestation."""
    run = os.fspath(run_dir)
    control_p = os.path.join(run, "control.json")
    sign_p = os.path.join(run, "sign-verification.json")
    if not os.path.exists(control_p):
        raise ControlNotSealed(f"missing {control_p}: run the control first (§5.4)")
    if not os.path.exists(sign_p):
        raise ControlNotSealed(f"missing {sign_p}: verify the sign convention (§1.4)")
    with open(control_p) as f:
        control = json.load(f)
    if not control.get("passed"):
        raise ControlNotSealed(
            f"control did not pass ({control.get('failures')}): STOP (§5.3)"
        )
    with open(sign_p) as f:
        sign = json.load(f)
    return {"control": control, "sign_verification": sign}


# ---------------------------------------------------------------------------
# §11 artifacts
# ---------------------------------------------------------------------------


def dump_json(path, obj) -> str:
    """Deterministic write; returns the sha256 of the bytes written."""
    payload = json.dumps(obj, sort_keys=True, indent=2, allow_nan=False).encode()
    with open(path, "wb") as f:
        f.write(payload)
        f.write(b"\n")
    return hashlib.sha256(payload + b"\n").hexdigest()


def render_summary(cells: dict) -> str:
    """§5.4: `summary.md` is generated from `cells.json`, never hand-written."""
    lines = [
        f"# MRR gate run — schema `{cells.get('schema')}`",
        "",
        f"- generated: {cells.get('generated_utc', '')}",
        f"- window: {cells.get('window', '')}",
        f"- cells: {len(cells.get('cells', []))}",
        f"- program_kill: **{cells.get('program_kill', 'NOT_EVALUABLE')}**",
        "",
        "| cell | verdict | M_patient | M_fast | M_null | LB(D) | flags | exclusions | promote |",
        "|---|---|---|---|---|---|---|---|---|",
    ]
    for r in cells.get("cells", []):
        lines.append(
            "| {cell} | {verdict} | {mp} | {mf} | {mn} | {lb} | {fl} | {ex} | {pr} |".format(
                cell=r.get("cell"),
                verdict=r.get("verdict"),
                mp=_fmt(r.get("M_patient")),
                mf=_fmt(r.get("M_fast")),
                mn=_fmt(r.get("M_null_bps")),
                lb=_fmt(r.get("D_patient_lb_family")),
                fl=",".join(r.get("flags") or []) or "-",
                ex=",".join(r.get("exclusion_reasons") or []) or "-",
                pr=r.get("promote"),
            )
        )
    return "\n".join(lines) + "\n"


def _fmt(v):
    return "-" if v is None else f"{v:+.3f}"


def load_history(reports_dir, cell_id: str, window: Sequence[str]) -> dict:
    """§2 goodhart-M1: replication needs two PASSes on windows with no shared day.

    The spec names the requirement and the `n_screen_attempts` field but not the
    store, so the store is the run directory itself: every `run-*/cells.json`
    already under `REPORTS_DIR` is a previous attempt. Recorded as an
    interpretation.
    """
    root = os.fspath(reports_dir)
    attempts = 0
    prior_pass_windows = []
    if os.path.isdir(root):
        for name in sorted(os.listdir(root)):
            if not name.startswith("run-"):
                continue
            path = os.path.join(root, name, "cells.json")
            if not os.path.exists(path):
                continue
            try:
                with open(path) as f:
                    doc = json.load(f)
            except ValueError:
                continue
            for rec in doc.get("cells", []):
                if rec.get("cell") != cell_id:
                    continue
                if rec.get("verdict") != "EXCLUDED":
                    attempts += 1
                if str(rec.get("verdict", "")).startswith("PASS"):
                    prior_pass_windows.append(list(doc.get("window") or []))
    cur = set(window)
    replicated = any(not (cur & set(w)) for w in prior_pass_windows)
    return {
        "attempts": attempts,
        "replicated": replicated,
        "prior_pass_windows": prior_pass_windows,
    }


def load_sealed(reports_dir) -> set:
    """§11 goodhart-C2: the sealed list lives in the persistent directory only."""
    path = os.path.join(os.fspath(reports_dir), "sealed-cells.json")
    if not os.path.exists(path):
        return set()
    with open(path) as f:
        doc = json.load(f)
    return {r["cell"] for r in doc.get("sealed", []) if "cell" in r}


def seal_cells(reports_dir, rows: Sequence[dict], run_id: str) -> list:
    """§9.2: append fast-killed cells; unsealing is a material change, not a run."""
    root = os.fspath(reports_dir)
    os.makedirs(root, exist_ok=True)
    path = os.path.join(root, "sealed-cells.json")
    doc = {"schema": SCHEMA, "sealed": []}
    if os.path.exists(path):
        with open(path) as f:
            doc = json.load(f)
    known = {r["cell"] for r in doc.get("sealed", [])}
    for r in rows:
        if r.get("fast_kill") and r["cell"] not in known:
            doc.setdefault("sealed", []).append(
                {
                    "cell": r["cell"],
                    "run": run_id,
                    "sealed_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
                    "M_patient": r.get("M_patient"),
                    "ub95_M_patient_best": r.get("ub95_M_patient_best"),
                    "n_orders": r.get("n_orders"),
                }
            )
            known.add(r["cell"])
    dump_json(path, doc)
    return doc["sealed"]


def write_cells(run_dir, cells: dict) -> str:
    """Refuses to write until the control of §5 is sealed in the same run dir."""
    require_sealed_control(run_dir)
    sha = dump_json(os.path.join(os.fspath(run_dir), "cells.json"), cells)
    with open(os.path.join(os.fspath(run_dir), "summary.md"), "w") as f:
        f.write(render_summary(cells))
    return sha


# ---------------------------------------------------------------------------
# §1.3-§1.5 reader layer  (hyperliquid; §10.1)
# ---------------------------------------------------------------------------


def hl_parse_print(tr: dict, t_local: int = 0, key: str = "taker") -> Print | None:
    """§1.4 + §10.1: sign from `side`, key = the taker's address.

    `key="hash"` reproduces the v0.1 rule the panel measured as degenerate
    (37.3% of HYPE prints carry `hash = 0x000…0`); it exists for the §7
    acceptance test and for nothing else.
    """
    side = tr.get("side")
    users = tr.get("users") or []
    if side == "B":
        sign = 1
        taker = users[0] if len(users) > 0 else None
    elif side == "A":
        sign = -1
        taker = users[1] if len(users) > 1 else None
    else:
        return None
    if not taker:
        return None
    if key == "hash":
        taker = tr.get("hash", "")
    try:
        t_ex = int(tr["time"]) * NS_MS
        px = float(tr["px"])
        qty = float(tr["sz"])
    except (KeyError, TypeError, ValueError):
        return None
    return Print(t_ex, sign, int(t_local), taker, px, qty)


def hl_collect_prints(frames: Iterable[tuple[int, dict]], key: str = "taker"):
    """Dedup by `tid` (§10.1: ~30 replayed fills per (re)subscribe)."""
    prints: list[Print] = []
    seen = set()
    n_replay = 0
    n_no_key = 0
    for t_local, obj in frames:
        for tr in obj.get("data", []) or []:
            tid = tr.get("tid")
            if tid is not None:
                if tid in seen:
                    n_replay += 1
                    continue
                seen.add(tid)
            p = hl_parse_print(tr, t_local, key=key)
            if p is None:
                n_no_key += 1
                continue
            prints.append(p)
    return prints, n_replay, n_no_key


def aggregate(prints: Sequence[Print], key_of=None) -> list[Order]:
    """§1.3: consecutive prints with the same (key, sign) are one market order.

    The sort is stable on `(t_ex, position in file)` — math-m2 measured that a
    `(t, sign)` tie-break moves HYPE `C(1)` by 200% of the estimate itself.
    """
    if key_of is None:
        key_of = lambda p: p.key  # noqa: E731
    idx = sorted(range(len(prints)), key=lambda i: (prints[i].t_ex, i))
    out: list[Order] = []
    cur_key = None
    for i in idx:
        p = prints[i]
        k = (key_of(p), p.sign)
        if k == cur_key and out:
            last = out[-1]
            out[-1] = Order(
                last.t_ex, last.sign, last.t_local, last.key, last.px,
                last.qty + p.qty, last.n_prints + 1,
            )
            continue
        cur_key = k
        out.append(Order(p.t_ex, p.sign, p.t_local, key_of(p), p.px, p.qty, 1))
    return out


def attach_touch(
    orders: OrderTable,
    frames: TouchFrames,
    reconnects: np.ndarray,
    rules: EstimatorRules = SPEC_RULES,
) -> TouchResult:
    """§1.5: the last frame strictly earlier on the exchange clock, plus the
    liveness rule (or, under `LEGACY_V01`, the age limit it replaced)."""
    n = len(orders.t_ex)
    nan = np.full(n, np.nan)
    if len(frames.t_ex) == 0 or n == 0:
        return TouchResult(
            nan, nan, nan, np.zeros(n, bool), np.zeros(n, bool),
            np.zeros(n, bool), np.zeros(n, bool),
        )
    order_ex = np.argsort(frames.t_ex, kind="stable")
    f_ex = frames.t_ex[order_ex]
    f_local = frames.t_local[order_ex]
    f_bid = frames.bid[order_ex]
    f_ask = frames.ask[order_ex]
    f_bsz = frames.bid_sz[order_ex]
    f_asz = frames.ask_sz[order_ex]

    pos = np.searchsorted(f_ex, orders.t_ex, side="left") - 1
    have = pos >= 0
    p = np.clip(pos, 0, None)

    bid = np.where(have, f_bid[p], np.nan)
    ask = np.where(have, f_ask[p], np.nan)
    valid = have & (bid > 0) & (bid < ask)

    if rules.stale_max_ns is not None:
        age = orders.t_ex - np.where(have, f_ex[p], 0)
        valid &= age <= rules.stale_max_ns
    else:
        frame_local = np.where(have, f_local[p], 0)
        valid &= (orders.t_local - frame_local) <= TOUCH_DEAD_NS
        # a receive gap in the touch stream itself
        loc_sorted = np.sort(f_local)
        if len(loc_sorted) > 1:
            gaps = np.diff(loc_sorted)
            dead_at = loc_sorted[1:][gaps > TOUCH_DEAD_NS]
        else:
            dead_at = np.empty(0, dtype=np.int64)
        for marks in (dead_at, np.asarray(reconnects, dtype=np.int64)):
            if len(marks) == 0:
                continue
            marks = np.sort(marks)
            lo = np.searchsorted(marks, frame_local, side="right")
            hi = np.searchsorted(marks, orders.t_local, side="right")
            valid &= (hi - lo) <= 0

    mid = np.where(valid, (bid + ask) / 2.0, np.nan)
    spread_price = np.where(valid, ask - bid, np.nan)
    with np.errstate(invalid="ignore"):
        spread_bps = np.where(valid, 1e4 * spread_price / mid, np.nan)
        at_touch = valid & np.where(
            orders.sign > 0, orders.px == ask, orders.px == bid
        )
        touch_sz = np.where(orders.sign > 0, f_asz[p], f_bsz[p])
        touch_sz = np.where(have, touch_sz, np.nan)
        touch_known = valid & np.isfinite(touch_sz)
        ate = touch_known & (orders.qty >= touch_sz)
    return TouchResult(mid, spread_bps, spread_price, valid, at_touch, ate, touch_known)


def _hl_read_file(path: str):
    """One coin-day: touch frames, trade frames, malformed count, truncation."""
    q_ex, q_loc, q_bid, q_ask, q_bsz, q_asz = [], [], [], [], [], []
    trade_frames = []
    n_lines = 0
    n_malformed = 0
    truncated = False
    recv = []
    try:
        with gzip.open(path, "rb") as f:
            for raw in f:
                n_lines += 1
                sp = raw.find(b" ")
                if sp < 0 or not raw.endswith(b"\n"):
                    n_malformed += 1
                    continue
                try:
                    t_local = int(raw[:sp])
                except ValueError:
                    n_malformed += 1
                    continue
                body = raw[sp + 1 :]
                recv.append(t_local)
                is_bbo = b'"channel":"bbo"' in body
                is_trade = b'"channel":"trades"' in body
                if not (is_bbo or is_trade):
                    continue
                try:
                    obj = json.loads(body)
                except ValueError:
                    n_malformed += 1
                    continue
                if is_bbo:
                    d = obj.get("data") or {}
                    bbo = d.get("bbo") or []
                    if len(bbo) != 2 or bbo[0] is None or bbo[1] is None:
                        continue
                    try:
                        q_ex.append(int(d["time"]) * NS_MS)
                        q_bid.append(float(bbo[0]["px"]))
                        q_ask.append(float(bbo[1]["px"]))
                        q_bsz.append(float(bbo[0].get("sz", "nan")))
                        q_asz.append(float(bbo[1].get("sz", "nan")))
                        q_loc.append(t_local)
                    except (KeyError, TypeError, ValueError):
                        n_malformed += 1
                else:
                    trade_frames.append((t_local, obj))
    except (EOFError, OSError, gzip.BadGzipFile) as exc:  # §1.1: fail closed
        truncated = True
        _ = exc
    frames = TouchFrames(
        np.asarray(q_ex, dtype=np.int64),
        np.asarray(q_loc, dtype=np.int64),
        np.asarray(q_bid, dtype=float),
        np.asarray(q_ask, dtype=float),
        np.asarray(q_bsz, dtype=float),
        np.asarray(q_asz, dtype=float),
    )
    return frames, trade_frames, n_lines, n_malformed, truncated, np.asarray(recv, dtype=np.int64)


def read_meta_reconnects(venue: str, date: str, data_root: str = DATA_ROOT) -> np.ndarray:
    """§1.5-3: reconnect records in the venue's `_meta` sidecar."""
    path = os.path.join(data_root, venue, f"_meta_{venue}_{date}.jsonl")
    marks: list[int] = []
    if not os.path.exists(path):
        return np.asarray(marks, dtype=np.int64)
    interesting = {"connected", "disconnected", "dial_failed"}
    with open(path, "rb") as f:
        for raw in f:
            sp = raw.find(b" ")
            if sp < 0:
                continue
            body = raw[sp + 1 :]
            if b'"_collector"' not in body:
                continue
            try:
                obj = json.loads(body)
            except ValueError:
                continue
            if obj.get("_collector") in interesting:
                try:
                    marks.append(int(raw[:sp]))
                except ValueError:
                    pass
    return np.asarray(sorted(marks), dtype=np.int64)


READERS = {"hyperliquid": _hl_read_file}


def read_cell(
    venue: str,
    symbol: str,
    dates: Sequence[str],
    data_root: str = DATA_ROOT,
    rules: EstimatorRules = SPEC_RULES,
    sign_verified: bool = True,
    fee_bps: float | None = "auto",
    rfq: bool = False,
    enforce_day_criteria: bool = True,
    weekdays_only: bool = False,
) -> CellInput:
    """Build one cell from the tape (§1.1-§1.6, §4.2)."""
    if venue not in READERS:
        raise NotImplementedError(
            f"no reader for venue {venue!r} in this build; §10 freezes its clocks "
            "and keys but the code is not written — see the wf71 hand-off"
        )
    reader = READERS[venue]
    if fee_bps == "auto":
        fee_bps = FEE_BPS.get(venue)

    series_by_day: dict[str, list[Series]] = {}
    n_orders = n_dropped = 0
    at_touch_num = at_touch_den = 0
    ate_num = ate_den = 0
    spread_prices: list[np.ndarray] = []
    day_reports = []
    n_malformed_total = 0
    n_prints_total = n_replay_total = 0
    c1u_num = 0.0
    c1u_cnt = 0
    c1u_sum = 0.0
    c1u_n = 0
    shared_ms = 0
    turnover = {}

    for date in dates:
        if weekdays_only and _is_weekend(date):
            continue
        path = os.path.join(data_root, venue, f"{symbol}_{date}.gz")
        if not os.path.exists(path):
            day_reports.append({"day": date, "usable": False, "reasons": ["MISSING"]})
            continue
        frames, trade_frames, n_lines, n_malformed, truncated, recv = reader(path)
        n_malformed_total += n_malformed
        prints, n_replay, n_no_key = hl_collect_prints(
            trade_frames, key=("hash" if rules.aggregate_key == "hash" else "taker")
        )
        n_prints_total += len(prints)
        n_replay_total += n_replay
        orders = aggregate(prints)

        reasons = []
        if truncated:
            reasons.append("GZIP_TRUNCATED")
        if n_lines and n_malformed / n_lines > MALFORMED_MAX_FRAC:
            reasons.append("MALFORMED")
        coverage_h = _coverage_hours(recv)
        max_gap = int(np.max(np.diff(recv))) if len(recv) > 1 else 0
        if coverage_h < DAY_COVERAGE_MIN_H:
            reasons.append("COVERAGE")
        if max_gap > DAY_MAX_GAP_NS:
            reasons.append("GAP")
        if len(orders) < MIN_ORDERS_PER_DAY:
            reasons.append("FEW_ORDERS")
        if not _touch_every_hour(frames.t_local, np.asarray([o.t_local for o in orders], dtype=np.int64)):
            reasons.append("NO_TOUCH")

        usable = not reasons
        day_reports.append(
            {
                "day": date,
                "usable": usable,
                "reasons": reasons,
                "n_orders": len(orders),
                "n_prints": len(prints),
                "n_replay": n_replay,
                "n_no_key": n_no_key,
                "n_lines": n_lines,
                "n_malformed": n_malformed,
                "coverage_h": round(coverage_h, 3),
                "max_gap_s": round(max_gap / NS, 1),
            }
        )
        if enforce_day_criteria and not usable:
            continue
        if not orders:
            continue

        tbl = OrderTable(
            np.asarray([o.t_ex for o in orders], dtype=np.int64),
            np.asarray([o.t_local for o in orders], dtype=np.int64),
            np.asarray([o.sign for o in orders], dtype=float),
            np.asarray([o.px for o in orders], dtype=float),
            np.asarray([o.qty for o in orders], dtype=float),
            np.asarray([o.n_prints for o in orders], dtype=np.int64),
        )
        touch = attach_touch(tbl, frames, read_meta_reconnects(venue, date, data_root), rules)

        n_orders += len(orders)
        n_dropped += int((~touch.valid).sum())
        at_touch_num += int(touch.at_touch.sum())
        at_touch_den += int(touch.valid.sum())
        ate_num += int(touch.ate_touch.sum())
        ate_den += int(touch.touch_known.sum())
        sp = touch.spread_price[np.isfinite(touch.spread_price)]
        if len(sp):
            spread_prices.append(sp)
        turnover[date] = float(sum(p.px * p.qty for p in prints))

        # C(1) without aggregation (§1.6 stats-m3), print level, within the day
        if len(prints) > 1:
            e = np.asarray([p.sign for p in sorted(prints, key=lambda p: p.t_ex)], dtype=float)
            c1u_num += float(e[:-1] @ e[1:])
            c1u_cnt += len(e) - 1
            c1u_sum += float(e.sum())
            c1u_n += len(e)

        uniq, counts = np.unique(tbl.t_ex, return_counts=True)
        shared_ms += int(counts[counts > 1].sum())

        # chains (§1.6)
        eps = tbl.sign
        mid = np.where(touch.valid, touch.mid, np.nan)
        spread = np.where(touch.valid, touch.spread_bps, np.nan)
        cuts = [0]
        gaps = np.nonzero(np.diff(tbl.t_local) > GAP_BREAK_NS)[0] + 1
        cuts.extend(gaps.tolist())
        cuts.append(len(orders))
        day_chains = []
        for a, b in zip(cuts[:-1], cuts[1:]):
            if rules.drop_breaks_chain:
                keep = touch.valid[a:b]
                seg_e, seg_m, seg_s = eps[a:b], mid[a:b], spread[a:b]
                start = 0
                for i in range(len(keep) + 1):
                    if i == len(keep) or not keep[i]:
                        if i > start:
                            day_chains.append(
                                Series(seg_e[start:i], seg_m[start:i], seg_s[start:i])
                            )
                        start = i + 1
            else:
                if b > a:
                    day_chains.append(Series(eps[a:b], mid[a:b], spread[a:b]))
        if day_chains:
            series_by_day[date] = day_chains

    if rules.drop_breaks_chain:
        # v0.1 counted only the surviving orders
        n_orders = sum(len(c.eps) for day in series_by_day.values() for c in day)

    all_sp = np.concatenate(spread_prices) if spread_prices else np.empty(0)
    tick = float(all_sp[all_sp > 0].min()) if (all_sp > 0).any() else None
    diag = {
        "n_lines_malformed": n_malformed_total,
        "n_prints": n_prints_total,
        "n_replay": n_replay_total,
        "frac_orders_sharing_ms": (shared_ms / n_orders) if n_orders else None,
        "tick": tick,
        "s_bar_ticks": float(all_sp.mean() / tick) if tick else None,
        "p_spread_1tick": (
            float(np.mean(np.round(all_sp / tick) == 1)) if tick and len(all_sp) else None
        ),
        "E_theta_proxy": (ate_num / ate_den) if ate_den else None,
        "C1_unaggregated": (
            c1u_num / c1u_cnt - (c1u_sum / c1u_n) ** 2 if c1u_cnt and c1u_n else None
        ),
        "turnover_usd_per_day": turnover,
        "turnover_usd_median": float(np.median(list(turnover.values()))) if turnover else None,
        "days": day_reports,
        "rules": rules.name,
    }
    return CellInput(
        venue=venue,
        symbol=symbol,
        fee_bps=fee_bps,
        sign_verified=sign_verified,
        rfq=rfq,
        series_by_day=series_by_day,
        n_orders=n_orders,
        n_dropped_mid=n_dropped,
        n_qualifying_days=sum(1 for d in day_reports if d["usable"]),
        frac_at_touch=(at_touch_num / at_touch_den) if at_touch_den else None,
        diag=diag,
    )


def _coverage_hours(recv: np.ndarray) -> float:
    """§4.2 "покрытие >= 23 ч": distinct UTC hours holding at least one record.

    The spec does not define the measure; hour-buckets are the cheap reading
    that cannot be gamed by a single record at each end of the day.
    """
    if len(recv) == 0:
        return 0.0
    return float(len(np.unique(recv // (3600 * NS))))


def _touch_every_hour(touch_local: np.ndarray, order_local: np.ndarray) -> bool:
    """§4.2: at least one touch frame in every hour that carried trades."""
    if len(order_local) == 0:
        return True
    if len(touch_local) == 0:
        return False
    hrs_t = set((touch_local // (3600 * NS)).tolist())
    hrs_o = set((order_local // (3600 * NS)).tolist())
    return hrs_o.issubset(hrs_t)


def _is_weekend(date: str) -> bool:
    d = _date(int(date[:4]), int(date[4:6]), int(date[6:8]))
    return d.weekday() >= 5


def window_days(window_end: str, n: int = WINDOW_DAYS, weekdays_only: bool = WEEKDAYS_ONLY):
    """§4.1: the last `n` calendar days ending on `window_end` (inclusive)."""
    end = datetime.strptime(window_end, "%Y%m%d").date()
    days = [(end - _days(i)).strftime("%Y%m%d") for i in range(n)][::-1]
    if weekdays_only:
        days = [d for d in days if not _is_weekend(d)]
    return days


def _days(k: int):
    from datetime import timedelta

    return timedelta(days=k)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _control_one(args):
    coin, days, data_root, reps, sign_verified = args
    cell = read_cell(
        "hyperliquid",
        coin,
        days,
        data_root=data_root,
        sign_verified=sign_verified,
        weekdays_only=False,
    )
    rec = assess_cell(cell, k_cells=1, reps=reps)
    return rec


def sign_verified_venues(path: str | None) -> dict:
    """§1.4: which venues cleared the sign-convention check, from the artifact.

    No artifact means no venue is verified — the fail-closed reading of §4.3-10,
    not a default of "the table says MEASURED".
    """
    if not path:
        return {}
    with open(path) as f:
        doc = json.load(f)
    venues = doc.get("venues", doc)
    return {v: bool(r.get("passed")) for v, r in venues.items() if isinstance(r, dict)}


def run_control(
    run_dir: str,
    days: Sequence[str] = CONTROL_DAYS,
    coins: Sequence[str] = CONTROL_COINS,
    data_root: str = DATA_ROOT,
    reps: int = BOOTSTRAP_REPS,
    jobs: int = 8,
    sign_verified: bool = False,
) -> dict:
    """§5.1-§5.2, executed FIRST. Writes `control.json` into `run_dir`."""
    os.makedirs(run_dir, exist_ok=True)
    tasks = [(c, list(days), data_root, reps, sign_verified) for c in coins]
    if jobs > 1:
        import multiprocessing as mp

        with mp.get_context("fork").Pool(jobs) as pool:
            rows = pool.map(_control_one, tasks)
    else:
        rows = [_control_one(t) for t in tasks]

    plant = assess_cell(plant_cell(), k_cells=1, reps=reps)
    ci = plant.get("ci_M_patient") or [None, None]
    plant_ok = bool(
        plant["verdict"] in ("PASS", "PASS_UNREPLICATED")
        and plant.get("D_patient_lb_family") is not None
        and plant["D_patient_lb_family"] > 0
        and ci[0] is not None
        and ci[0] <= 1.5 <= ci[1]
    )
    out = control_criteria(rows, plant_ok)
    out.update(
        schema=SCHEMA,
        generated_utc=datetime.now(timezone.utc).isoformat(timespec="seconds"),
        days=list(days),
        coins=list(coins),
        bootstrap_reps=reps,
        cells=rows,
        plant=plant,
    )
    dump_json(os.path.join(run_dir, "control.json"), out)
    return out


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    sub = ap.add_subparsers(dest="cmd", required=True)

    c = sub.add_parser("control", help="§5 negative control (16 HL coins + plant)")
    c.add_argument("--run-dir", required=True)
    c.add_argument("--data-root", default=DATA_ROOT)
    c.add_argument("--days", default=",".join(CONTROL_DAYS))
    c.add_argument("--coins", default=",".join(CONTROL_COINS))
    c.add_argument("--reps", type=int, default=BOOTSTRAP_REPS)
    c.add_argument("--jobs", type=int, default=8)
    c.add_argument(
        "--sign-verification",
        default=None,
        help="§1.4 artifact; absent means no venue is verified (fail closed)",
    )

    p = sub.add_parser("plant", help="the plant cell alone (§5.2.4)")
    p.add_argument("--reps", type=int, default=BOOTSTRAP_REPS)

    s = sub.add_parser("screen", help="§4 screen (needs a sealed control)")
    s.add_argument("--run-dir", required=True)
    s.add_argument("--window-end", required=True)
    s.add_argument("--data-root", default=DATA_ROOT)
    s.add_argument("--venues", default="hyperliquid")
    s.add_argument("--reps", type=int, default=BOOTSTRAP_REPS)
    s.add_argument("--jobs", type=int, default=8)

    a = ap.parse_args(argv)
    if a.cmd == "control":
        verified = sign_verified_venues(a.sign_verification)
        if not verified.get("hyperliquid"):
            print(
                "WARNING: hyperliquid is not verified in the sign-verification "
                "artifact (§1.4); every control cell will read SIGN_UNVERIFIED",
                file=sys.stderr,
            )
        out = run_control(
            a.run_dir,
            days=a.days.split(","),
            coins=a.coins.split(","),
            data_root=a.data_root,
            reps=a.reps,
            jobs=a.jobs,
            sign_verified=bool(verified.get("hyperliquid")),
        )
        print(json.dumps({k: v for k, v in out.items() if k not in ("cells", "plant")}, indent=2))
        return 0 if out["passed"] else 1
    if a.cmd == "plant":
        rec = assess_cell(plant_cell(), k_cells=1, reps=a.reps)
        print(json.dumps(rec, indent=2, sort_keys=True))
        return 0
    if a.cmd == "screen":
        raise SystemExit(
            "screen mode needs the venue readers of §10 that this build does not "
            "carry (hyperliquid only); see the wf71 hand-off"
        )
    return 2


if __name__ == "__main__":
    sys.exit(main())
