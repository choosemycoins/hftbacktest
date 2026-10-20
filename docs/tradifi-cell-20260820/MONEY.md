# MONEY — #105, mature TRADIFI_PERPETUAL on Binance USD-M

**PREREG §10 executed in full on the frozen cube. Nothing re-estimated.**

Substrate: `cache/cube` (pass2.py output, frozen), 10,421 symbol-days, 150 symbols,
236 class-days, 889,994,024 prints, $1.302 T notional, 2025-12-26 .. 2026-08-18.
PREREG sha256 `0edd00e0…`, COHERENCE sha256 `0ae704a0…`. Machine-readable twin: `money.json`.

---

## 0. What this document is, and what it is not

PREREG §9 condition **(v) money FAILED at every horizon** — K8 fired. The ground rules for
that case are explicit: *"если планка счёта не пройдена — считай калибровку «сколько стоит
шум» и пометь именно так."*

**So this is a NOISE CALIBRATION, and it is labelled as one.** Section 6 is the substance
of the document. Sections 2–5 are the declared money form; section 6 is what those dollars
are worth as evidence, measured against the study's own frozen placebo. The single most
important number here is not a P&L: it is that **the argmax of pure noise on this substrate
clears the project's $28.75/day KILL floor (#73) in 100.0 % of draws, at every horizon,
under every capacity model tried.** The KILL floor is not a bar this instrument can
discriminate on. Anything below roughly $400/day on a top-5 screen of this class is
indistinguishable from sign-flipped nothing.

> **MANDATORY LINE (PREREG §10(д), Lighter lesson #102).** Every dollar in this document is
> a **POPULATION POOL** and therefore an **UPPER BOUND ON OUR CAPTURE — never an
> expectation.** The Binance tape does not decompose by account, so who-captures cannot be
> split. Lighter is the standing proof the two come apart: a positive population pool there
> coexisted with a zero-fee cohort losing −2.855 bps. For this class the caveat is not even
> load-bearing — **the pool itself is negative on both money weightings.**

---

## 1. Frozen inputs, printed as assumptions

| Input | Value | Provenance |
|---|---|---|
| Maker fee | **0.00 bps** | measured 2026-08-19, bare VIP0 key; class policy since 12.2024, not promo |
| Taker fee | **4.00 bps** | same measurement |
| Fee sensitivity column | **2.00 bps** | PREREG §10(б), crypto-scale maker fee |
| KILL floor | **$28.75/day** | project floor, #73 |
| Class median half-tick | **0.4013 bps** | print-weighted median 0.0974 bps; p90 2.2405, max 6.5920 |
| φ_queue (frozen) | **0.3768** | $50 quote vs $82.7 median touch, KR six, **transferred as an assumption** |
| φ_size (frozen) | **0.0342** | $50 vs $1,463 class mean print notional |
| φ_live (measured here) | **0.1899** median | §4.3 — measured on the mature symbols themselves, 2026-08-20T00:39:37Z |

---

## 2. (а) Term structure — realized spread, bps per print, half-tick beside every number

Class median half-tick **0.4013 bps**; print-weighted median half-tick **0.0974 bps**.
Three weightings, all three declared before any number existed.

| h | per-SYMBOL | per-PRINT | per-DOLLAR | half-tick | capture c | adverse m |
|---|---|---|---|---|---|---|
| 1 s | **+1.6193** | −0.2109 | −0.4608 | 0.4013 | +1.1186 | +0.8003 |
| 10 s | **+0.8696** | −0.3805 | −0.5620 | 0.4013 | +1.1193 | +1.0169 |
| 60 s | **+0.2910** *(inside the half-tick = lattice)* | −0.4281 | −0.5597 | 0.4013 | +1.1116 | +1.1048 |
| 300 s | **+0.1351** *(inside; p = 0.135)* | −0.4618 | −0.4938 | 0.4013 | +1.0974 | +1.1558 |
| session-end | **−0.9442** | −0.8671 | −1.3097 | 0.4013 | +0.9882 | +1.3168 |

Capture is flat (+1.10 … +1.12 bps at every horizon) and adverse selection rises
0.80 → 1.16 and **crosses it between 10 s and 60 s**. That crossing is the whole answer in
one line; RS = c − m holds numerically to 3.3e-16.

---

## 3. (б) Net at maker 0.00 — and what it costs to bank it

**At maker 0.00 the net IS the gross.** That is the whole point of the class and it is why
the question was asked. The declared 2.0 bps sensitivity column, and one column the prereg
did not require but arithmetic does:

| h | gross (per-symbol) | half-tick | net @ maker 2.0 bps | net if the exit CROSSES (taker 4.00) | gross as % of one taker fee |
|---|---|---|---|---|---|
| 1 s | +1.6193 | 0.4013 | **−0.3807** | **−2.3807** | 40 % |
| 10 s | +0.8696 | 0.4013 | **−1.1304** | **−3.1304** | 22 % |
| 60 s | +0.2910 | 0.4013 | **−1.7090** | **−3.7090** | 7 % |
| 300 s | +0.1351 | 0.4013 | **−1.8649** | **−3.8649** | 3 % |
| SE | −0.9442 | 0.4013 | **−2.9442** | **−4.9442** | −24 % |

**The exit column is not a modelling choice, it is arithmetic.** Realized spread is a
mark-to-market against the mid at h, not a round trip. Banking it needs the other side. If
that side is passive, it is another fill of exactly the measured quality — which is what the
60 s and 300 s rows already show happening. If it crosses, it pays the class taker fee of
4.00 bps. **The largest gross this class offers at any horizon, +1.6193 bps at 1 s, is 40 %
of one taker fee.** So the short-horizon positive reading cannot be converted into money by
crossing out, at any capacity, on any symbol, ever. This closes the 1 s row independently of
everything in section 6.

---

## 4. (в) The pie, in $/day

### 4.1 Class table — CEILING, capacity, KILL floor

**CEILING** = Σ(notional × RS)/1e4 ÷ 236 class-days = what a maker would earn taking
**100 % of every print in the class**. Physically unreachable; declared as the ceiling.

*Identity, verified to 2.6e-16: the ceiling is exactly the per-DOLLAR column monetized —
per_dollar_RS/1e4 × measurable notional/day ($3.95–4.79 B/day). It is not a third opinion.*

| h | CEILING $/day | × φ_queue 0.377 | × φ_size 0.034 | × φ_live 0.190 (§4.3) | at maker 2.0 bps | **KILL floor** |
|---|---|---|---|---|---|---|
| 1 s | **−185,048** | −69,721 | −6,324 | −3,898 | −988,285 | **+28.75** |
| 10 s | **−223,593** | −84,243 | −7,642 | −10,236 | −1,019,313 | **+28.75** |
| 60 s | **−221,849** | −83,586 | −7,582 | −11,797 | −1,014,577 | **+28.75** |
| 300 s | **−195,065** | −73,494 | −6,667 | −11,907 | −985,178 | **+28.75** |
| SE | **−626,815** | −236,165 | −21,423 | −43,097 | −1,584,008 | **+28.75** |

**K8 fired at every horizon.** The physically unreachable 100 %-of-tape maker is between
−$185 k and −$627 k per day. No capacity model can rescue a negative ceiling: φ ∈ (0,1]
multiplies it, never flips its sign. This alone closes the class question.

Breadth, not outliers — at 60 s: **88 of 150 symbols negative**, median symbol
**−$49.43/day**, sum of all negative symbols −$507,233/day against a sum of ALL profitable
symbols of only **+$10,056/day**. Dropping the 5 worst still leaves the class at
−$244,419/day (PART I).

### 4.2 Top and bottom of the pie (h = 60 s, $/day at the ceiling)

Every one of the **eight largest books by notional is a loser**, and they are where the
money is:

| symbol | type | notional/day | prints/day | RS/symbol | RS/dollar | half-tick | CEILING $/day | φ_live | at φ_live |
|---|---|---|---|---|---|---|---|---|---|
| SNDKUSDT | EQUITY | $1.274 B | 1,186,921 | −0.452 | −0.687 | 0.032 | **−44,862** | 0.024 | −1,069 |
| SKHYNIXUSDT | KR_EQUITY | $1.269 B | 1,472,474 | −0.467 | −0.570 | 0.040 | **−39,444** | 0.008 | −326 |
| XAGUSDT | COMMODITY | $1.065 B | 132,598 | +0.089 | −0.506 | 0.672 | **−48,118** | 0.000 | −21 |
| SPCXUSDT | EQUITY | $1.047 B | 342,661 | −0.284 | −0.897 | 0.342 | **−75,738** | 0.005 | −386 |
| SOXLUSDT | EQUITY | $1.018 B | 636,320 | −0.576 | −0.613 | 0.269 | **−44,595** | 0.012 | −522 |
| XAUUSDT | COMMODITY | $1.009 B | 285,212 | −0.235 | −0.467 | 0.011 | **−30,641** | 0.001 | −22 |

Best of the pie, same horizon:

| symbol | type | days | notional/day | RS/symbol | half-tick | CEILING $/day | own null sd | t | φ_live | at φ_live |
|---|---|---|---|---|---|---|---|---|---|---|
| BMNRUSDT | EQUITY | 54 | $5.15 M | 1.470 | **3.062** *(lattice)* | +924 | 256 | 3.62 | 0.015 | **+13** |
| HK1810USDT | HK_EQUITY | 13 | $3.52 M | 0.975 | **1.895** *(lattice)* | +832 | 327 | 2.55 | 0.053 | +44 |
| SQQQUSDT | EQUITY | 36 | $7.28 M | 0.012 | 1.236 | +581 | 350 | 1.66 | 0.005 | +3 |
| SMCIUSDT | EQUITY | 43 | $4.15 M | 0.873 | **1.712** *(lattice)* | +574 | 321 | 1.79 | 0.147 | +85 |
| ASMLUSDT | EQUITY | 54 | $2.99 M | 0.612 | 0.028 | +555 | 470 | 1.18 | 0.121 | +67 |
| IBMUSDT | EQUITY | 62 | $12.11 M | 0.182 | 0.212 | +434 | 801 | 0.54 | 0.168 | +73 |

**Two composition traps, both visible in this table.** First, the symbols carrying the
positive per-symbol headline sit inside their own half-tick (BMNR +1.470 vs 3.062;
HK1810 +0.975 vs 1.895; SMCI +0.873 vs 1.712) — lattice, not effect. Second, and this is
new here: **the symbols with the biggest ceiling have the DEEPEST touch queues.** Measured
live: BMNR's touch holds **$3,374** a side, NOK's **$8,067**, SQQQ's **$9,801** — φ_live
0.0146, 0.0062, 0.0051. You cannot get filled there without standing behind thousands of
dollars. The positive money and the reachable money are in different symbols.

### 4.3 REAL touch depth — measured on the mature class, not transferred

PREREG §10 measures φ on the KR six (day-0 listings) and transfers it; PART II named that
transfer the weakest joint of the study. It has now been measured on the mature symbols
themselves: `depthprobe.py`, 169 symbols × 6 snapshots, 55 s apart, 2026-08-20T00:39:37Z,
median over snapshots, bid and ask separately.

| | touch notional per side | φ at our $50 | live spread at the touch |
|---|---|---|---|
| class median | **$213.3** | **0.1899** | 4.68 bps |
| p25 / p75 of φ (= p75 / p25 of depth) | $889.9 / $95.0 | 0.0532 / 0.3449 | 9.53 / 1.75 bps |
| min / max | $7.2 / $113,381 | — | — |
| KR six, day-0 (frozen input) | $82.7 | 0.3768 | — |

| group | n | touch/side | φ_live | spread |
|---|---|---|---|---|
| COMMODITY | 8 | $17,039 | 0.0032 | 1.34 bps |
| EQUITY | 137 | $209.9 | 0.1924 | 4.65 bps |
| HK_EQUITY | 12 | $236.1 | 0.1865 | 5.97 bps |
| KR_EQUITY | 8 | $126.0 | 0.2847 | 9.03 bps |
| PREMARKET | 2 | $421.5 | 0.1208 | 6.03 bps |

**The frozen φ_queue was generous by 2×.** Mature books are 2.6× deeper at the touch than
the six day-0 Korean listings it was transferred from. Direction of the probe's own bias is
stated rather than hoped: it is one moment with the US cash session **closed**, so books are
at their thinnest, which makes φ **larger** and the money **more generous**. A negative
verdict under this φ is the harder verdict to reach, and it is the one we get.

### 4.4 FCFS under one capital

A maker cannot be first in queue on 150 books at once. Priced on the live per-symbol touch
depth, with the capital actually split — the same $50 becomes $50/k on each book, so φ
shrinks per book (Model S). Model N (a full $50 on each of k books, i.e. k× the capital) is
printed beside it as the loose upper bound.

| h | top-1 symbol | its touch | top-1 $/day | top-5, Model N | **top-5, ONE capital split** | vs KILL $28.75 |
|---|---|---|---|---|---|---|
| 1 s | APPUSDT | $31.3 | +304.02 | +1,244.24 | **+425.55** | clears |
| 10 s | BSPUSDT | $23.8 | +327.48 | +1,122.78 | **+416.93** | clears |
| 60 s | BSPUSDT | $23.8 | +282.27 | +861.38 | **+333.30** | clears |
| 300 s | NBISUSDT | $232.2 | +819.65 | +1,815.33 | **+495.54** | clears |
| SE | MRVLUSDT | $349.5 | +2,758.27 | +6,304.83 | **+1,766.46** | clears |

Read this table **only** together with §6.2 and §6.5. Every row here is an **argmax over 150
books chosen after seeing the answer**, and section 6 prices exactly that argmax under the
null. Four of these five rows are worse than noise.

---

## 5. Funding, as a post-hoc jump (#40), never folded into RS

#40's finding applied, not re-derived: funding is not a drag on the horizon, it is a jump
paid only by inventory still open when a stamp fires (00:00 / 08:00 / 16:00 UTC).

38,427 stamps across 169 symbols: **69.8 % exactly zero**, mean |rate| **1.7084 bps/8h**
(notional-weighted 2.5796), mean **signed** +0.4006 bps/8h (notional-weighted +0.9832) —
i.e. longs pay, mildly and systematically.

| group | n | mean \|rate\| | mean signed | frac zero |
|---|---|---|---|---|
| EQUITY | 137 | 1.6561 | +0.3539 | 74.7 % |
| HK_EQUITY | 12 | 3.6791 | **+1.3699** | 64.7 % |
| KR_EQUITY | 8 | 3.9880 | **+0.9943** | 60.5 % |
| COMMODITY | 8 | 0.7069 | +0.1304 | 62.8 % |
| PREMARKET | 2 | 0.5000 | +0.5000 | 0.0 % |
| CN_EQUITY | 2 | 33.6351 | **−33.6351** | 31.2 % |

| h | E[stamps crossed] | worst-case cost bps/print | $/day at the ceiling | signed (if always long) |
|---|---|---|---|---|
| 1 s | 0.00003 | 0.00009 | $36 | +0.00003 |
| 10 s | 0.00035 | 0.00090 | $356 | +0.00034 |
| 60 s | 0.00208 | 0.00537 | $2,130 | +0.00205 |
| 300 s | 0.01042 | 0.02687 | $10,615 | +0.01024 |
| SE | **1.50** | **3.86933** | $1,851,849 | +1.47478 |

**Verdict on funding: not the culprit at h ≤ 300 s** — 0.027 bps against an effect of
0.1–1.6 bps and a loss of 0.46–0.56 bps per dollar, two orders below. A maker flat at every
stamp pays exactly zero. **But at session-end it is not negligible at all:** 12 hours of
inventory crosses 1.5 stamps, costing 3.87 bps worst-case, which is four times the |RS| at
that horizon. PART I's "four orders below the effect" was computed for 300 s only; it does
not extend to SE, and SE is the horizon a genuine overnight-carry reading would use.
CN_EQUITY's −33.6 bps/8h is an eye-catching number that changes nothing — both CN names are
removed by the frozen 14-day age rule and contribute zero symbol-days.

---

## 6. THE NOISE CALIBRATION — what noise costs on this substrate

The frozen placebo is a day-level Rademacher sign flip, the **same flip applied to every
symbol on that date**, which preserves the cross-sectional dependence. It is applied here to
dollar accumulators rather than to cluster means.

> **Declared, not inherited.** The frozen §8 null flips CLUSTER MEANS. The money statistic
> has no cluster-mean representation — the same structural reason PART II gave for refusing
> to print a flip p on the per-dollar column. So everything in §6 is the frozen null's
> **mechanics** applied to a new accumulator, and it is named as such rather than passed off
> as the pre-registered test. Sign-flip variance is closed-form for a linear statistic
> (Var Σεx = Σx²), so the class band needs no simulation; it was verified against 5,000
> simulated draws anyway (exact vs simulated agree).

### 6.1 What a dollar of evidence is worth at class level

| h | CEILING $/day | own null sd $/day | t | **KILL floor in null-sd** |
|---|---|---|---|---|
| 1 s | −185,048 | 18,633 | **−9.93** | 0.0015 |
| 10 s | −223,593 | 22,333 | **−10.01** | 0.0013 |
| 60 s | −221,849 | 22,123 | **−10.03** | 0.0013 |
| 300 s | −195,065 | 26,531 | **−7.35** | 0.0011 |
| SE | −626,815 | 360,214 | −1.74 | 0.00008 |

Two readings, both load-bearing.

1. **The negative class verdict is resolved and then some.** −7 to −10 null-sd at
   h ≤ 300 s. This is not a marginal sign.
2. **The instrument cannot see the KILL floor at class scale at all.** $28.75/day is
   **0.0011–0.0015 of one null sd** — the class money noise is roughly **700–900× the
   entire project floor**. So no class-scale money claim near $28.75/day could ever be
   certified from this data in either direction. The only reason there is a verdict is that
   the observed number is enormous relative to the floor, on the negative side.
3. h = SE fails on its own money terms too (t = −1.74), corroborating the frozen coherence
   check that declared SE unresolvable before unblinding (its RS null sd 0.8995 bps against
   a 1.19 bps target).

### 6.2 The argmax of noise — at the frozen φ

The operational money statistic is not a class sum, it is *"pick the best books and quote
them"*. An argmax over a noise field is positive by construction. Same selection rule, run
on 5,000 sign-flipped datasets:

| h | observed top-1 $/day | noise top-1 median | noise p95 | p | observed top-5 | noise top-5 median | p |
|---|---|---|---|---|---|---|---|
| 1 s | 1,344 | 7,966 | 19,201 | 0.911 | 4,572 | 19,978 | 0.888 |
| 10 s | 965 | 9,106 | 22,265 | 0.971 | 3,659 | 23,199 | 0.954 |
| 60 s | 924 | 13,546 | 31,083 | **0.997** | 3,466 | 33,680 | **0.996** |
| 300 s | 4,627 | 24,217 | 58,136 | 0.973 | 9,467 | 59,277 | 0.993 |
| SE | 200,762 | 295,154 | 804,732 | 0.705 | 341,449 | 687,812 | 0.807 |

**The best symbol on the real tape is worse than the median best symbol of its own
sign-flipped null, at every horizon.** At 60 s the real argmax sits in the bottom 0.3 % of
the noise argmax distribution. And the noise top-1 clears the $28.75/day KILL floor in
**100.0 % of draws** at every horizon.

### 6.3 The residual — the one thing PART II left alive — priced against its own rule

PART II's only surviving route was *"selective quoting on the 6 half-tick-clearing
survivors"*. Those six, at 60 s: HYUNDAI, NFLX, SMH, SNOW, SOFI, TBT. Their money, and then
the **same selection rule** (per-symbol studentized t, then the symbol's own half-tick) run
on sign-flipped data, with selection and money flipped by the same date signs so they stay
coherent:

| h | observed, ceiling | at φ_queue | at φ_size | **null of the SAME rule, median** | null p95 | **p** | null clears KILL at φ_queue |
|---|---|---|---|---|---|---|---|
| 60 s | $725.88/day | $273.49 | **$24.81** | **$920.06** | $15,729 | **0.575** | 88.6 % of draws |
| 300 s | $612.10/day | $230.62 | **$20.92** | **$1,207.57** | $17,139 | **0.655** | 84.6 % of draws |

**The residual earns less than the median of what its own selection rule pays on noise.**
And at the honest capacity (φ_size) it is **below the KILL floor outright**: $24.81/day
against $28.75/day. The last live route in the study is closed in money.

Individually the six are: NFLX +$99.33/day, SNOW +$105.31, SOFI +$64.75, SMH +$15.98,
TBT +$0.70 at φ_queue — and **HYUNDAI −$12.58, a symbol with RS/symbol +0.908 bps and a
NEGATIVE ceiling.** That single row is the whole per-symbol-vs-per-dollar composition
problem in one line.

### 6.4 The KILL-floor screen itself, at the live φ

"How many symbols clear $28.75/day?" is a screen, and a screen on a noise field returns a
positive count by construction:

| h | tape: symbols above floor | their sum $/day | noise median count | noise median sum | p(count) | p(sum) |
|---|---|---|---|---|---|---|
| 1 s | **41** | $3,413 | 18 | $1,541 | **0.000** | **0.002** |
| 10 s | 19 | $1,986 | 21 | $2,370 | 0.655 | 0.702 |
| 60 s | 22 | $1,728 | 24 | $3,586 | 0.693 | 0.953 |
| 300 s | 24 | $3,215 | 29 | $5,567 | 0.881 | 0.924 |
| SE | 29 | $10,099 | 42 | $26,819 | 0.967 | 0.977 |

Only h = 1 s beats its own null. At 60 s the tape's shortlist is **smaller and poorer than
noise's**. The 1 s exception is real and is killed by §3's arithmetic, not by statistics:
+1.6193 bps is 40 % of one taker fee, and a 1-second hold has to be crossed out.

The FCFS top-5 of §4.4, priced against the identical argmax on the null:

| h | observed top-5 (one capital split) | noise median | noise p95 | **p** |
|---|---|---|---|---|
| 1 s | $425.55 | $257.26 | $379.49 | **0.0088** |
| 10 s | $416.93 | $393.70 | $582.25 | 0.4146 |
| 60 s | $333.30 | $613.95 | $968.41 | 0.9044 |
| 300 s | $495.54 | $928.45 | $1,474.98 | 0.9100 |
| SE | $1,766.46 | $4,110.99 | $6,589.92 | 0.9754 |

### 6.5 A degeneracy worth naming, and the design lesson it buys

At h = 60 s the noise top-1 distribution has median = p95 = **$1,081.87** — a constant.
Cause: **GIGADEVUSDT has exactly one admitted day**, so its sign flip is a two-point
±$1,081.87/day and the argmax picks the positive branch every single draw. **Short-history
symbols set the noise floor of any money screen on this class.** The observed top-1 is
BSPUSDT with 33 days at $282/day; noise's top-1 is a one-day symbol at $1,082/day.

Re-run with a minimum-history floor applied to **both** the tape and the null — declared
**post-hoc**, chosen after seeing the degeneracy, and therefore a design lesson for a future
screen, never a promotion route for this one:

| floor | h | observed top-5 | noise median | noise p95 | p |
|---|---|---|---|---|---|
| ≥30 days (118 eligible) | 1 s | $334.13 | $192.41 | $325.05 | 0.0396 |
| ≥30 days | 60 s | $256.51 | $380.53 | $632.91 | 0.8584 |
| ≥30 days | 300 s | $508.48 | $597.36 | $983.94 | 0.6750 |
| **≥60 days (75 eligible)** | 1 s | $139.93 | $128.01 | $250.83 | **0.4216** |
| ≥60 days | 60 s | **$66.74** | $288.63 | $496.12 | **0.9920** |
| ≥60 days | 300 s | $239.86 | $426.08 | $717.55 | 0.9072 |

**With 60 days of required history the 1 s money edge evaporates entirely (p = 0.42).** The
only configuration in the entire study where money beats noise is 1 s with short-history
symbols admitted — and it dies twice over: on history, and on the taker fee.

Noise cleared the $28.75/day floor in **100 % of draws in every one of these
configurations.**

### 6.6 Per-symbol: is $28.75/day even resolvable on a single book?

Count of symbols whose own money-noise sd already exceeds the entire KILL floor:

| h | at the ceiling (φ=1) | at φ_queue | at φ_size |
|---|---|---|---|
| 1 s | 120 / 150 | 95 | 17 |
| 60 s | **126 / 150** | 106 | 27 |
| 300 s | 130 / 150 | 111 | 34 |

At 60 s, **84 % of symbols cannot resolve a $28.75/day edge on 236 days of tape even at the
physically impossible 100 % capture.** Break-even capacity tells the same story from the
other side: of the 62 symbols with a positive ceiling at 60 s, 46 need φ ≤ 1, 34 need
φ ≤ φ_queue, and exactly **1 needs φ ≤ φ_size** (minimum required φ = 0.0311).

---

## 7. Verdict, in money

| Falsifier | Status |
|---|---|
| **K8 — ceiling below the KILL floor** | **FIRED at all five horizons.** −$185 k … −$627 k/day at 100 % of the tape. A negative ceiling cannot be rescued by any φ. |
| PREREG §9(v) | **FAILED at all five horizons** — the money condition that dominates the promotion rule. |
| §6 noise calibration | Class verdict resolved (t = −7 … −10). Every top-of-pie route indistinguishable from, or worse than, its own null except 1 s. |
| §3 exit arithmetic | The 1 s exception cannot be banked: +1.6193 bps = 40 % of one taker fee. |

**No standing positive cell, and the money form is the strongest of the five conditions
against it.** The class is not merely un-promotable — the physically unreachable maker who
takes 100 % of every print in it loses a fifth of a million dollars a day.

**Calibration statement, as required:** on this substrate the price of noise at the top-5
screen level is **$250–$600/day at 60 s** (noise median, one capital, live φ), and noise
clears the $28.75/day project floor in **100 % of draws**. Any future money claim on this
class below roughly **$600/day** on a top-5 selection, or below **$28.75/day** at all,
carries no information — the screen would return it from sign-flipped nothing.

**What would reopen this.** Nothing in the current data; the loss is broad, survives the
measured instrument bias, survives an independent exogenous mark, and is negative in all 24
hours. A genuine reopen needs a new mechanism, and it must clear a floor set by §6.5 (a
minimum-history filter) and by §3 (a gross exceeding one taker fee if the exit ever crosses).

---

## 8. Caveats — the load-bearing ones only

1. **UPPER BOUND ON OUR CAPTURE, NEVER AN EXPECTATION** (§0, #102). Population pool; the
   Binance tape does not decompose by account. Our cohort would be a latency-disadvantaged
   newcomer joining the back of every queue, so our realisation is bounded above by these
   numbers and plausibly far below them. For the class question the pool itself is negative,
   so the caveat is not even needed; it binds on §6.3's residual.
2. **The mid is a tape proxy, not a book.** data.binance.vision publishes no bookTicker for
   this class (none venue-wide since 2024-04); bookDepth's tightest level is ±20 bps. The
   bias is measured (b = +0.4674 / +0.0418 / −0.3691 / −0.0210 bps at 1/10/60/300 s) and the
   negative per-print result survives it — but at 60 s the bias magnitude (0.3691) exceeds
   the per-symbol estimate it would correct (+0.2910), which is PREREG §9(iv) failing on a
   measurement. Every money number inherits a reconstructed mid.
3. **φ was the study's weakest joint; it is now measured, and the frozen value was
   generous by 2×** (§4.3). What remains transferred is the *bias* b(h), still calibrated on
   the six KR day-0 symbols.
4. **The live depth probe is one moment** (2026-08-20T00:39:37Z, 6 snapshots, US cash
   closed). Its bias direction is toward thinner books → larger φ → more generous money, so
   it cannot manufacture the negative verdict.
5. **§6 is the frozen null's mechanics on a new accumulator**, declared in §6, not the
   pre-registered test. It is the honest available calibration, not a promotion instrument.
6. **The min-history floor of §6.5 is post-hoc** and is reported as a design lesson only.
7. **Funding at SE is NOT negligible** (3.87 bps worst-case vs |RS| 0.94) — PART I's "four
   orders below the effect" holds for 300 s and does not extend to session-end.
8. `nullsd`, `t_money` and every band in §6 use the same seed (20260820) and the same 5,000
   draws as the frozen inference; exact closed-form and simulated class bands agree.

---

*Artifacts: `money.json` (141 KB, machine-readable), `cache/money_{class,detail,noise,
residual_null,screen_null,fcfs_null,fcfs_null_minday,funding,phi_live}.json`,
`cache/money_sym_{1,10,60,300,SE}.{parquet,csv}`, `cache/touchdepth{,_summary}.{json,csv}`,
`cache/money_survivors.csv`. Code: `money2.py … money11.py`, `depthprobe.py`.*

---

## 9. Provenance and reproducibility

* **`depthprobe.py` was sha256-hashed BEFORE it was launched** (`dc11c8ea…`, recorded into
  `CODE.sha256.money` at launch time), so the live-probe rule — every admitted symbol, 6
  snapshots, 55 s apart, median over snapshots, no selection by eye — was fixed before any
  depth number existed.
* **The whole chain was re-run end to end after a cosmetic code cleanup and reproduces
  bit-identically:** 0 of ~3,600 numeric cells in `money.json` differ between the two runs,
  0 keys added or dropped. Seeds are fixed (flip/bootstrap 20260820, 5,000 draws).
* **`audit_money.py` mechanically re-checks every load-bearing number in this document
  against `money.json`** — ceilings, capacity columns, fee columns, null sds, t-statistics,
  exit arithmetic, FCFS rows, argmax p-values, residual rows. **0 failures.** The audit
  found and fixed one real rounding error in an earlier draft (−69,720 → −69,721).
* Hashes: `MONEY.sha256` (MONEY.md, money.json, 12 cache artifacts), `CODE.sha256.money`
  (12 scripts). Both `chmod 444`.
* **Pre-existing provenance defect, inherited and NOT caused here** (PART II caveat 9): five
  entries in the study's `CODE.sha256` were already stale when PART II started. Untouched;
  the money hashes live in separate files precisely so the two are not confused.
* **No new artifact page.** The study already has one (PART I/II). PART III is not on it;
  if it should be, that page must be updated in place via its URL, not re-published.
