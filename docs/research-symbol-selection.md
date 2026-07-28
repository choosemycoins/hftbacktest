# Research: выбор символов для пассивного грида на Hyperliquid

Статус: Research note, 2026-07-29 — **скрин, не бэктест**
Связано: `docs/design-multi-venue-collection.md` (режим A — каждому кандидату нужна UM-пара),
`docs/design-hyperliquid-connector.md` §5.3 (5-significant-figures — источник тик-эффекта из §2.1)
Постановка (заказчик): инструменты с живым потоком, но без Wintermute-класса профессионального
маркет-мейкинга — «и чтобы не полтора бомжа в стакане».

**Status: DRAFT. Nothing here is backtested.** This is a one-snapshot screen whose purpose is to pick
*what to record*, not what to trade. The deploy decision comes after the recording run in §9.

Date: 2026-07-29. Inputs: METRICS agent (`metrics.json`, 232 coins swept / 87 sampled) and
MM-LANDSCAPE agent (`landscape.md`). Two additional measurements were derived here — see §2.

---

## 1. TL;DR

**Shortlist (13).** Ranked: **VVV, JTO, GRAM, ZRO, PENDLE, AERO, CRV, NEAR, DOT, ONDO, KAITO, INJ, ETHFI**.

**Current set: 4 of 10 survive.** Keep **JTO, PENDLE, ONDO, INJ**. Drop **SUI, HYPE, xyz:GOLD, ENA**
(all four are in professionally-pinned books; SUI/HYPE/xyz:GOLD are the three most saturated instruments
in the entire sample). Park **SEI, TIA** — good book shape, no tape.

**The finding that reorders everything:** the brief's `spread_bps >= 2` filter does not mean what it
looks like. Hyperliquid quantises perp prices to 5 significant figures, so the *tick itself* is worth
0.12–5.0 bps depending on the coin. Half the metrics agent's "wide spread" A_strict tier — PUMP, kPEPE,
kSHIB, kBONK — is not wide at all: those books sit at **exactly one tick**, the tightest state the venue
permits, with 5–13 professional orders stacked at the touch. Their bps number is a tick artifact.
Conversely ZRO and DOT look tight at 1.94/1.96 bps but are **15–16 ticks wide** with a single $250 order
at the touch. Spread in *ticks* is the saturation measure; spread in *bps* is the revenue measure. You
need both, and the brief only had one.

---

## 2. What this synthesis adds to its inputs

Two measurements not present in either input document.

### 2.1 Tick reconstruction → `spread_ticks`

HL perp prices allow at most 5 significant figures and at most `6 − szDecimals` decimals, so

```
tick = max(10^-(6 - szDecimals), 10^(floor(log10(px)) - 4))
spread_ticks = spread_bps_median / (tick / px * 1e4)
```

**Validation:** across all 87 sampled coins the resulting `spread_ticks` lands within 0.05 of an integer
for **98.9%** of them, median deviation exactly **0.000**. A wrong tick model would not produce integers.
This is strong evidence the reconstruction is right, but it is a reconstruction from documented price
rules, not a field read from the API — confirm before it drives sizing.

Why it matters: `spread_ticks == 1` means *nobody can quote tighter*. That is maximum maker competition
by definition, and it is invisible in the bps column.

### 2.2 Touch-queue contention (measured, deliberately unscored)

The raw 20-level books carry `[price, size, n_orders]`. Median across the 7 samples of the order count
and USD size resting at the best bid/ask:

| | touch orders | touch USD |
|---|---|---|
| ETH | 23.0 | $121,651 |
| BTC | 18.5 | $272,793 |
| SOL | 8.5 | $50,937 |
| XRP | 7.5 | $32,347 |
| **PUMP** | **7.5** | **$5,401** |
| **kPEPE** | **6.5** | **$4,930** |
| **xyz:GOLD** | **5.0** | **$16,354** |
| **HYPE** | **2.5** | **$9,729** |
| NEAR | 2.5 | $1,843 |
| **shortlist (VVV, JTO, ZRO, AERO, DOT, INJ, CRV…)** | **1.0–2.0** | **$241–$589** |

In every shortlist book the front of the queue is **one or two orders of roughly $250**. That is the
operational answer to "can a small passive grid ever be the best bid?" — here, yes; in PUMP or kPEPE, no.

This metric is **not** part of the score. It is held out as an independent check, and it corroborates the
ranking without having influenced it.

### 2.3 HL-vs-Binance notional ratio (resolves an open question in `landscape.md`)

`landscape.md` §6.7 names the Mode-A tension and says the best available mitigation is "prefer coins where
HL volume is comparable to or exceeds the Binance perp" — but could not measure it. Fetched
`fapi/v1/ticker/24hr`, closeTime **2026-07-28T22:32:04Z**, ten minutes after the HL snapshot at
**22:21:56Z**, so the windows are directly comparable.

Binance is **4×–40× larger than HL on almost every alt perp**. Range across the shortlist: HYPE 0.74
(highest anywhere), VVV 0.38, GRAM 0.34, ZRO 0.23, CRV 0.19 … INJ 0.048, **DOT 0.036** (lowest in the
shortlist), TIA 0.026 (lowest in the current set). Low ratio = HL is decisively the lagging venue =
your resting quote is the designated stale liquidity for IOC arbitrageurs. This is scored at 12 points.

---

## 3. Methodology

### 3.1 Data provenance

| Item | Source | Timestamp | Reliability |
|---|---|---|---|
| volume / OI / funding / day return | HL `metaAndAssetCtxs` | 2026-07-28T22:21:56Z, single reading | point estimate |
| spread, depth, touch queue | HL `l2Book`, **7 samples ~35 s apart over ~4 min** | same window | median of 7 |
| Binance UM pair existence | `fapi/v1/exchangeInfo` | 2026-07-28 | reliable |
| Binance UM 24h notional | `fapi/v1/ticker/24hr` | 2026-07-28T22:32:04Z | point estimate |
| Lighter listings | Lighter `orderBooks` | 2026-07-28 | reliable, 227 books / 209 active |
| MM landscape | published sources | research 2026-07-29 | **inferential — see §8.4** |
| tick, spread_ticks, touch queue | derived here from the above | — | §2.1 validated |

Coverage: 232 canonical perps swept for volume/OI/funding; **87 sampled for book metrics** (top 85 by
volume ∪ current set ∪ xyz:GOLD). 55 of the 232 carry an upstream `isDelisted` flag and were not sampled.

### 3.2 Gates — and why each one

A coin failing any gate is **demoted with the failing gate named**, never silently dropped.

| Gate | Why |
|---|---|
| `vol_usd >= $1M` | The "не полтора бомжа" floor. Relaxed from the brief's $5M because the sample day was abnormally quiet — only 33 of 232 coins cleared $3M. Under the brief's own $5M floor the shortlist would be 3 coins. |
| `spread_bps_median >= 1.9` | HL base maker fee is 0.015%/side = **3.0 bps round trip**. Below ~2 bps the touch is not worth crossing the fee floor, and — more importantly — a sub-2 bps book is a book someone is working. 1.9 not 2.0 to avoid a knife-edge; CRV (1.881) and NEAR (1.808) are admitted as conditional and flagged. |
| `spread_ticks >= 3` | §2.1. A 1–2 tick book is at or next to the venue minimum: maximum maker competition, long FIFO queues. This is the gate that removes PUMP/kPEPE/kSHIB/kBONK/ZEC. |
| `depth_10bps_min_side >= $2,500` | The dead-book trap. CASHCAT posts $5.76M/day nominal volume with **$0** resting within 10 bps on either side. Nominal volume cannot catch that; this can. |
| Binance UM pair exists | Mode-A signal feed. Only 3 of 87 sampled coins fail: xyz:GOLD, CASHCAT, and VINE (UM contract exists but status is `SETTLING`, i.e. being delisted). |
| not hard-blacklisted | CME regulated futures listed **or** live US spot ETF. Both create a permanent professional hedging/creation-redemption loop. |

### 3.3 Score components (base 0–100)

| Component | w | Shape | Rationale |
|---|---|---|---|
| `slack` = spread_ticks | 16 | 1→0.0, 5→0.70, 9–16→1.0, 22→0.85, 35+→decay | saturation. Decays at the top: 35+ ticks is a dead book, not a slack one. |
| `spread` = spread_bps | 20 | 1.3→0.08, 1.9→0.30, 3–6→0.78–1.0, 14+→decay | revenue vs the 3.0 bps fee floor. Decays high: a 20 bps spread means nobody is there. |
| `flow` = vol_usd | 17 | $0.5M→0, $5M–$60M→1.0, $150M→0.70, $400M→0.15 | the brief's $5–150M band, log-scaled, with a soft rather than hard cap. |
| `hlshare` = HL/UM | 12 | 0.03→0.10, 0.15→0.60, 0.30+→0.90–1.0 | §2.3. Mode-A stale-liquidity exposure. |
| `depth` = d10 min side | 12 | $1.5k→0.30, $8k–$60k→1.0, $150k→0.55, $400k→0.20 | two-sided: too little is a dead book, too much is a wall you queue behind. |
| `vpd` = vol / d10 | 9 | 30→0.35, 300→1.0, 2500→0.60 | flow per unit of crowding. **Clamped to ≤30 where d10 < $1.5k** — a ratio built on an empty book is meaningless, not excellent. |
| `vol` = day_abs_ret | 9 | 0.8%→0.30, 3–7%→1.0, 16%→0.25 | a grid needs traversal; too much is inventory risk. |
| `oiv` = OI/vol | 5 | 1.5–5→1.0, 8→0.55, 12+→decay | positioning vs turnover. |

### 3.4 Landscape adjustments (additive, −44 … +17)

Deliberately capped well below the metric base, per `landscape.md`'s own caveat §6.2 that mandate
evidence is *CEX-spot-shaped* and should be weighted below measured book behaviour.

`−32` CME futures · `−30/−26` live US spot ETF · `−14` no UM pair · `−10` documented tier-1 mandate ·
`−10` HIP-3 builder dex · `−8` cross-firm consensus set · `−8` ETF filed · `−7` inside 0–18mo mandate
window · `−6` named in tier-1 supported set · `−4` TGE date unknown · `−4` recent foundation raise
(re-arm risk) · `−3` low-confidence MM claim · `+3` Lighter listed · `+3` funding off the 1.00 bps/8h
baseline · `+4` spread widened during sampling · `+4` retail catalyst · `+3/+6` mandate window expired.

**One correction to the input:** `landscape.md` is internally inconsistent on SUI — §4.2 omits it from
the live-ETF list while §5 and the blacklist both cite 21Shares TSUI live on Nasdaq from 2026-02-24.
Took the specific sourced claim. SUI therefore carries both CME and live-ETF penalties.

---

## 4. Shortlist

`ticks` = §2.1 · `d10`/`d25` = min-side USD resting within 10/25 bps · `touch` = median USD at best
bid/ask · `HL/UM` = §2.3 · **CORE** = passes all gates · **COND** = one named gate miss.

| # | Coin | Score | Tier | HL vol | UM vol | HL/UM | spread | ticks | touch | d10 | d25 | day% | UM pair | Lighter |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1 | **VVV** | 91.2 | CORE | $7.75M | $20.2M | 0.38 | 4.54 | 6 | $250 | $2.9k | $15.4k | 1.79 | VVVUSDT | VVV#69 |
| 2 | **JTO** | 90.5 | CORE | $1.85M | $14.6M | 0.13 | 2.22 | 12 | $250 | $6.6k | $51.2k | 6.25 | JTOUSDT | JTO#134 |
| 3 | **GRAM** | 84.6 | CORE | $3.91M | $11.7M | 0.34 | 3.44 | 5 | $502 | $12.0k | $29.8k | 1.64 | GRAMUSDT | GRAM#12 |
| 4 | **ZRO** | 84.3 | CORE | $3.34M | $14.6M | 0.23 | 1.94 | 16 | $504 | $13.6k | $16.9k | 9.45 | ZROUSDT | ZRO#60 |
| 5 | **PENDLE** | 83.6 | CORE | $1.19M | $10.3M | 0.12 | 3.37 | 5 | $589 | $8.9k | $28.6k | 2.66 | PENDLEUSDT | PENDLE#37 |
| 6 | **AERO** | 83.1 | CORE | $1.36M | $19.9M | 0.07 | 3.41 | 15 | $241 | $6.1k | $11.3k | 2.35 | AEROUSDT | AERO#65 |
| 7 | **CRV** | 80.7 | COND | $3.35M | $17.9M | 0.19 | 1.88 | 4 | $250 | $17.2k | $32.0k | 3.30 | CRVUSDT | CRV#36 |
| 8 | **NEAR** | 79.5 | COND | $13.5M | $109M | 0.12 | 1.81 | 3 | $1,843 | $63.5k | $115k | 3.72 | NEARUSDT | NEAR#10 |
| 9 | **DOT** | 76.0 | CORE | $2.04M | $56.3M | 0.036 | 1.96 | 15 | $250 | $21.1k | $21.1k | 2.03 | DOTUSDT | DOT#11 |
| 10 | **ONDO** | 75.1 | CORE | $9.95M | $86.8M | 0.11 | 1.96 | 8 | $434 | $23.0k | $25.1k | 2.41 | ONDOUSDT | ONDO#38 |
| 11 | **KAITO** | 73.2 | COND | $15.6M | $131M | 0.12 | 4.19 | 5 | $364 | **$1.3k** | $61.0k | 9.46 | KAITOUSDT | KAITO#33 |
| 12 | **INJ** | 71.6 | CORE | $1.30M | $27.1M | 0.048 | 3.05 | 14 | $314 | $16.0k | $22.3k | 4.06 | INJUSDT | **none** |
| 13 | **ETHFI** | 63.6 | CORE | $1.19M | $12.3M | 0.10 | 4.08 | 17 | $250 | $12.3k | $23.1k | **0.09** | ETHFIUSDT | ETHFI#64 |

### Per-coin rationale

**1. VVV — 91.2.** $7.75M/day squarely in the target band and HL carries 38% of the VVV/Binance pair, the
second-best cross-venue ratio in the shortlist — this is not a pure lagging venue. Book is 6 ticks wide at
4.54 bps with a single $250 order at the touch: a small grid can hold front-of-queue.
No CME, no ETF, no named tier-1 mandate anywhere in the research.
*Risks:* thinnest book of the CORE group ($2.9k min side within 10 bps) — price will gap through rungs,
so size down and widen spacing. Venice AI TGE ~Jan 2025 → ~18 months, right at the mandate-window edge.

**2. JTO — 90.5.** The best-shaped book in the set: $250 and one order at the touch, but $51.2k by 25 bps —
shallow where you quote, deep where you need the market to not run away. 12 ticks of slack at 2.22 bps,
6.25% daily range. 31 months post-TGE, no CME/ETF/mandate; JTX self-custody terminal (public July 2026) is
a genuine retail, spread-crossing catalyst.
*Risks:* **rank is landscape-driven.** Base metric score is 73.5; +17 of adjustment (Lighter, expired
mandate, retail catalyst, spread widening) carries it to #2. Volume is the weak leg at $1.85M/day.

**3. GRAM — 84.6.** Highest base score of any gate-passer (85.6) on metrics alone. $3.91M/day, HL/UM 0.34,
5 ticks at 3.44 bps, and the healthiest depth ladder in the CORE group ($12.0k at 10 bps → $29.8k at 25).
*Risks:* **provenance unverified.** No TGE date, no mandate evidence either way — scored −4 for the
unknown rather than researched. The metrics agent verified `onboardDate` to rule out a same-ticker
collision, but nobody has established what GRAM actually is. Resolve that before any capital.

**4. ZRO — 84.3.** 16 ticks of slack — the book is wide in venue terms even though 1.94 bps looks tight,
because the tick is only 0.121 bps. $13.6k min side, $504 at the touch, HL/UM 0.23. 9.45% daily range is
the highest in the CORE group. LayerZero, ~25 months post-TGE, no institutional loop.
*Risks:* 1.94 bps sits right on the fee-viability line — the grid must earn from spacing, not the touch.
OI/vol 9.0 means positions are parked rather than turning over.

**5. PENDLE — 83.6.** 5 ticks at 3.37 bps, $589 touch, $8.9k→$28.6k depth ladder, funding off baseline
(+0.34). 63 months post-TGE — mandate long expired. The DWF/GSR partnership claim is contradicted across
aggregators (`landscape.md` §6.5) and is scored at only −3.
*Risks:* $1.19M/day is thin, and HL/UM 0.12.

**6. AERO — 83.1.** 15 ticks at 3.41 bps with a $241 touch — genuinely uncontested at the front. 35 months
post-TGE, no CME/ETF/named mandate, Lighter listed.
*Risks:* **HL/UM 0.068 is the weakest cross-venue ratio in the CORE group.** Binance is 15× larger, so
this is close to a textbook Binance-leads/HL-lags pair — the exact profile that gets picked off by IOC arb.
$1.36M/day.

**7. CONDITIONAL — CRV — 80.7.** *Gate miss: spread 1.881 vs 1.9 — a 1% miss, inside measurement noise.*
Best-formed depth ladder in the whole shortlist ($17.2k → $32.0k), $3.35M/day, HL/UM 0.19, funding −0.40
off baseline. 72 months post-TGE, no CME/ETF/mandate.
*Risks:* only 4 ticks of slack, and 1.88 bps is below the round-trip fee floor at the touch.

**8. CONDITIONAL — NEAR — 79.5.** *Gate miss: spread 1.808 and only 3 ticks.* Highest volume of any
clean-landscape coin in the sample at $13.5M/day.
*Risks:* **the most crowded book of any candidate** — $1,843 across 2–3 orders at the touch, $63.5k at
10 bps, $115k at 25 bps. That much depth at that volume is precisely `landscape.md`'s "someone is being
paid or hedged to hold it there" tell. Admit only as a wide-grid, larger-size instrument, or not at all.

**9. DOT — 76.0.** 15 ticks at 1.96 bps, $250 touch, $21.1k min side, 70 months post-TGE, no CME, no live
ETF, no named mandate. A clean, quiet, uncontested book.
*Risks:* **HL/UM 0.036 — the worst in the shortlist.** Binance UM is 28× the HL book. Maximum Mode-A
stale-liquidity exposure. Also note `d10 == d25 == $21.1k`, meaning the 20-level API cap binds: true depth
is larger than shown and crowding is understated.

**10. ONDO — 75.1.** Best raw metrics in the current traded set: $9.95M/day in band, 8 ticks at 1.96 bps,
$23.0k min side, vpd 138.7, and OI/vol 1.53 — the most turnover-driven (least position-parked) book in the
set. Funding −0.95 off baseline.
*Risks:* carries the heaviest landscape penalty of any keeper (−18: documented Wintermute USDY partnership
plus cross-firm consensus overlap). The counter-argument is `landscape.md`'s own §6.2 — that mandate is an
OTC/spot obligation and does not put Wintermute in the HL perp book — and the measured book behaviour is
good. Metrics win; keep, and re-check if the book tightens.

**11. CONDITIONAL — KAITO — 73.2.** *Gate miss: $1,268 min side within 10 bps, half the $2.5k floor.*
Highest volume of any non-blacklisted candidate at $15.6M/day, 5 ticks at 4.19 bps, and vpd 1431 — flow per
unit of resting depth is ~10× the CORE group. OI/vol 0.65 is the lowest in the shortlist: pure turnover, no
parked positioning.
*Risks:* the raw samples show the touch swinging $4,594 → $33 → $128 across four minutes. A 9.46% daily
move against a $1.3k near-touch book means price gaps straight through rungs. 17 months post-TGE, so a
mandate may still be live. Trial at minimum size with wide spacing, or defer to the recorded re-rank.

**12. INJ — 71.6.** 14 ticks at 3.05 bps, $16.0k min side, $314 touch, 70 months post-TGE. Funding
**−6.11 bps/8h** is by far the largest dislocation in the entire 232-coin sweep — positioning-driven rather
than arb-flattened, which is a green pattern.
*Risks:* 21Shares spot INJ ETF filed 2025-10-20 (anticipatory positioning starts at filing);
**no Lighter listing**, the only shortlist member without one; HL/UM 0.048; $1.30M/day.

**13. ETHFI — 63.6.** Widest slack in the shortlist at 17 ticks / 4.08 bps, $12.3k → $23.1k ladder.
*Risks:* **day_abs_ret 0.09%** — the price essentially did not move on the sample day, which for a grid is
the single most disqualifying observation available; a grid on a still instrument pays fees and earns
nothing. Plus GSR names EtherFi as a client (−10). Ranked last for those two reasons and included only
because the book shape is otherwise excellent. **Do not deploy without an intraday-vol re-measure.**

---

## 5. Current set — honest verdict

| Coin | Score | Rank /87 | Verdict | Why |
|---|---|---|---|---|
| **JTO** | 90.5 | 2 | **KEEP — best-chosen** | Shallow-touch/deep-25bps ladder, 12 ticks, retail catalyst, no institutional loop. |
| **PENDLE** | 83.6 | 5 | **KEEP** | 5 ticks at 3.37 bps, mandate long expired, only the contradicted DWF/GSR claim against it. |
| **ONDO** | 75.1 | 15 | **KEEP, caveat** | Best metrics in the set; heaviest mandate baggage. Metrics outrank a spot-shaped mandate. |
| **INJ** | 71.6 | 21 | **KEEP, smallest size** | Good book, huge funding dislocation; but ETF filed, no Lighter, HL/UM 0.048. |
| **SEI** | 71.0 | 22 | **PARK** | Book is fine (12 ticks, 2.78 bps, $12.8k) and landscape is clean — but $570k/day and HL/UM 0.038. Flow-starved, not saturated. Re-check on a normal-volume week. |
| **TIA** | 62.8 | 43 | **PARK / drop** | Best *book* in the set (17 ticks, 5.15 bps, clean landscape) and the lowest *volume* in the entire 87-coin sample ($318k). HL/UM 0.026 is the worst ratio in the set. Celestia's $100M Bain-led raise is an unresolved mandate re-arm trigger. |
| **ENA** | 69.4 | 27 | **DROP** | 1.32 bps. 11 ticks of slack on a 0.12 bps tick is economically nothing. Wintermute authored *and passed* Ethena's revenue-share governance change; GSR names Ethena. HL/UM 0.067. |
| **xyz:GOLD** | 8.3 | 81 | **DROP** | **$1,067,373 resting per side within 10 bps** — the literal "$1M wall" red flag — with flow-per-depth of 12.9, 6th lowest of 85 (`metrics.json` calls it the lowest, but that holds only within its top-40; PAXG, WLFI, BCH, XLM and HBAR are lower across the full sample). 1-tick book, $16.4k across 5 orders at the touch. HIP-3, where Jump ($3.15B), Selini ($1.03B) and Wintermute ($229.6M) are the *documented* population. No UM pair → signal-less. |
| **HYPE** | −3.4 | 82 | **DROP** | 0.18 bps = 1 tick, $9.7k at the touch, $152k min side. Live US spot ETF whose S-1 amendment **names Wintermute and Flowdesk as approved trading counterparties**. HL's own token = maximum on-venue maker competition by construction. Its one virtue (HL/UM 0.74, best anywhere) cannot offset the rest. |
| **SUI** | −4.4 | 83 | **DROP** | 0.144 bps = 1 tick. **Three independent professional loops**: CME futures listed 2026-05-04 (24/7 since 05-29), live 21Shares TSUI spot ETF since 2026-02-24, plus filed products. HL/UM 0.064. The most institutionally saturated instrument in the current set. |

**Summary: 4 keeps, 2 parks, 4 drops.** The set's real problem is not that it is badly chosen — JTO,
PENDLE and ONDO are genuinely good picks — it is that **three of ten slots (SUI, HYPE, xyz:GOLD) are spent
on the three most professionally-quoted books available**, and two more (SEI, TIA) on books with no tape.
Half the capital is in instruments that cannot pay a small passive grid.

---

## 6. Watch tier

Not shortlisted, but the group most likely to move on re-measurement.

**6.1 Coarse-tick locked books — revisit only with a queue-position study.**
PUMP ($32.1M/day, 5.02 bps/tick), kPEPE ($14.1M, 3.55), kBONK ($2.79M, 3.28), kSHIB ($5.30M, 2.15),
EIGEN ($0.92M, 5.10), BOME ($0.68M, 18.6).
All sit at **exactly 1 tick** — the whole spread *is* the tick, so nobody can quote tighter. What makes
this group different from the 1-tick names in §7 is that their tick is genuinely coarse, so the locked
spread still clears the 3.0 bps round-trip fee floor: PUMP's 5.02 bps nets ~2 bps if you get both sides.
The obstacle is the queue, not the economics — PUMP shows 7.5 orders and $5,401 at the touch (top-5 min
side $111,214), kPEPE 6.5 orders and $4,930 ($162,969), and you would join at the back of a FIFO queue at
a single price against desks running a 1.8 bps/side fee advantage. This is a latency-sensitive game rather
than a passive-grid one; it is not dead, it is just a different strategy.

**6.2 Good book shape, no flow — the group to re-check first.**
MET ($864k, 14 ticks), ALGO ($830k, 21 ticks, funding −1.46), MEGA ($748k, 14), DASH ($646k, 12),
POL ($563k, 17), SEI ($570k, 12), CHIP ($518k, 14, funding −2.06), ENS ($512k, 16), TIA ($318k, 17).
Every one has a clean landscape and a slack, uncontested book. They failed only the $1M flow gate, on a day
when just 33 of 232 coins cleared $3M. **If a normal-volume week lifts any of these over $1M/day they
become CORE candidates immediately** — this is where the sample-day distortion bites hardest.

**6.3 In-band volume, professionally tight — no fee headroom.**
UNI (1.28 bps), WLD (1.25), ENA (1.32), XMR (1.47), JUP (1.57), LDO (1.60), FARTCOIN (1.60), VIRTUAL (1.22).
Flow is fine; the spread is at or under the 3.0 bps round-trip fee floor. FARTCOIN is notable for HL/UM
0.46 — the second-highest ratio anywhere — but at 2 ticks and 1.60 bps there is nothing to capture.

---

## 7. Avoid tier

| Coin(s) | Reason |
|---|---|
| **BTC, ETH, SOL, XRP, ADA, LINK, XLM, AVAX** | CME regulated futures listed, all 24/7 since 2026-05-29 → permanent basis-desk loop. BTC/ETH/SOL/XRP also have live US spot ETFs, and their touch queues are 7.5–23 orders deep holding $32k–$273k. ADA/LINK/XLM/AVAX have thin touches ($250–$1,224) but are 1–3 tick books at 0.12–1.85 bps — tight for structural reasons, not crowded ones. Zero edge either way for a small passive grid. |
| **SUI** | CME futures (2026-05-04) **and** live 21Shares TSUI spot ETF. 1-tick 0.144 bps book. Three professional loops. |
| **HYPE** | Live spot ETF naming Wintermute + Flowdesk as trading counterparties; HL's own token; 1-tick 0.18 bps; $152k min side. |
| **xyz:GOLD** | $1.07M resting per side within 10 bps; 1-tick; HIP-3 builder dex with documented Jump/Selini/Wintermute presence; **no Binance UM pair → signal-less**. |
| **ENA** | 1.32 bps with Wintermute having authored and passed Ethena's revenue-share governance change (implies a large stake and ongoing relationship); GSR names Ethena. |
| **XPL** | 1.19 bps, and the **only public per-coin MM attribution on HL core anywhere in the research** — a suspected Auros wallet deposited 30M USDC and accumulated ~$17.25M of XPL on-chain. Also 10 months post-TGE. |
| **AAVE, TAO** | Cross-firm consensus overlap set (multiple tier-1 desks converge) plus 0.99 / 0.51 bps 1-tick books. |
| **ZEC, LIT** | 1-tick books on a *fine* tick — ZEC $80.6M/day at 0.213 bps, LIT $26.0M/day at 0.432 bps. Real volume, but the entire spread is 2–4 hundredths of a basis point wide; there is nothing above the 3.0 bps fee floor to capture. LIT additionally onboarded 2025-12-23, i.e. deep inside the mandate window. |
| **PENGU, APT, ARB, ASTER, DOGE, HBAR, PAXG, BNB, LTC, TRX, BCH** | 1–2 tick locked books at 0.14–1.70 bps (TRX and BCH are 2-tick, the rest 1-tick). APT is additionally named in Jump's focus set and has a filed ETF; PAXG is Wintermute's tokenized-gold OTC franchise and shows $230k min-side depth. |
| **CASHCAT** | **The dead-book trap.** $5.76M/day nominal clears the strict volume band, but median spread is 23.3 bps and there is **$0 resting within 10 bps of mid on either side** — the entire book sits outside the band, ~84 ticks wide, $28 at the touch. No UM pair. Nominal volume cannot catch this; `depth_10bps_min_side` can. |
| **VINE** | 20.2 bps, $0 within 10 bps, and its Binance UM contract is in `SETTLING` status (being delisted) → no signal feed. |
| **GRIFFAIN, NIL, SKR, IMX, REZ** | Sub-$1M flow ($0.52M–$0.84M) with $0.77k–$2.2k min-side depth within 10 bps and 5.4–11.4 bps spreads. Wide because empty, not because slack — the difference from §6.2 is that those books have depth and no flow, while these have neither. |

---

## 8. Threats to validity

Read this section before acting on §4.

**8.1 It is a four-minute snapshot, not a regime estimate.** Seven touch samples per coin, ~35 s apart,
on a single day. Enough to separate "always ~0.15 bps" from "always ~4 bps"; **not** enough to characterise
intraday variation, session effects, or behaviour during a move. Do not size anything on these medians.

**8.2 The sample day was abnormally quiet, and this biases every volume-dependent conclusion.** Only 33 of
232 canonical perps cleared $3M/24h; only 4 cleared $150M. BTC marked $63,963 vs $64,686 prev-day. The
$1M flow gate is calibrated on that day, and it is the gate that demoted the entire §6.2 group. On an
active week the shortlist could look materially different — most plausibly *longer*.

**8.3 Spreads were sampled, not recorded — so the single best available saturation test was not run.**
`landscape.md` names *conditional spread vs realised-vol quantile* as "the cleanest professional-presence
signature available from L2 data alone": organic books blow out on impulse, professionally-made books
mean-revert to fair value within seconds. Seven point samples cannot compute that curve. The
`spread widened during sampling` bonus (+4, applied to **28 of 87 coins**) is a 7-observation proxy for it
and is explicitly low-confidence — it fires on roughly a third of the sample, which is itself a sign it is
picking up sampling noise as much as book behaviour. Four of the top six shortlist entries carry it, so
removing it would compress the top band substantially.

**8.4 MM presence is inferential everywhere.** There is **no public per-asset MM-share data for HL core
perps**. Every concentration figure in `landscape.md` (top-5 = 50% of MM volume, 363 MM wallets,
order-to-fill 19.4, the `pct_alo ≥ 80%` classifier) comes from HIP-3 / Trade.xyz, a different venue surface.
Further: mandates are CEX-spot obligations and do not put a firm in the HL perp book; HL has no DMM
program, no rebate lock-in and no latency tier; and Wintermute held positions in **111 HL assets** in
May 2026, so no HL perp is genuinely MM-free. The landscape is a **prior for ordering the queue**, which is
why it is capped at roughly a fifth of the score. §2.2 (touch queue) is the only *measured* saturation
signal here.

**8.5 TGE dates are my estimates, not sourced.** The mandate-window term (−7 … +6) rests on them.
GRAM, MET, CHIP, STABLE, MEGA, SKR and CASHCAT have unknown TGE dates and were penalised −4 rather than
researched — which directly caps GRAM at #3 despite the highest base score in the set.

**8.6 The tick model is a reconstruction.** §2.1 validates strongly (98.9% integer, median deviation 0)
but it is inferred from HL's documented price rules, not read from an API field. Every `spread_ticks` value
— and therefore the entire reordering in §1 — depends on it.

**8.7 `depth_10bps` is a lower bound where the 20-level API cap binds.** Flagged upstream for 14 coins;
among the shortlist that is **ONDO, DOT, INJ** (and NEAR/ENA in the watch/avoid tiers). Their crowding is
understated and their `vpd` correspondingly overstated. DOT's `d10 == d25 == $21.1k` is the visible symptom.

**8.8 `day_abs_ret_pct` is markPx vs prevDayPx — an endpoint difference, not realised volatility.**
A grid's economics depend on *intraday path length*, which this cannot see. ETHFI's 0.09% may be a full
round trip that returned to its start. This is the weakest input to the score and it carries 9 points.

**8.9 The Mode-A tension is mitigated, not resolved.** Requiring a Binance UM perp selects for coins with
the most active cross-venue arb loop — the mechanism that picks off resting quotes. HL/UM share is the best
available proxy and it is only 12 of 100 points. Actual lead-lag was not measured; see §9.

**8.10 Funding is a weak discriminator here.** HL pulls hard to a 0.00125%/hr baseline = exactly
1.00 bps/8h and most coins sit precisely there. Only genuine outliers carry information (INJ −6.11,
CASHCAT +2.42, CHIP −2.06, ALGO −1.46, KAITO −1.15, ONDO −0.95). Reading anything into the coins pinned at
1.00 would be noise, and the +3 bonus is applied only to the outliers.

**8.11 Weights are judgement, fitted to nothing.** No P&L, no backtest. Sensitivity was not run. The
ordering within ±5 points is not meaningful — treat §4 as roughly three bands (91–83, 80–75, 73–63), not
as a strict ranking.

**8.12 The snapshot decays fast.** Wintermute went $40M → $4M on HL in a single day (2026-05-18); Oros
Global closed 175 positions in about two hours. Treat the landscape half of this document as having a
roughly one-quarter half-life.

---

## 9. Recommended next step — record, then re-rank

**We own the tooling for exactly this.** The collector already records the right HL streams
(`trades`, `bbo`, `activeAssetCtx`, `l2Book × {slow,fast}`) and the matching Binance UM streams
(`@trade`, `@bookTicker`, `@depth@0ms`, + REST `premiumIndex`). Nothing needs to be built.

### 9.1 The run

Record the 13 shortlist coins plus the 4 current-set keepers plus a deliberate control group — SUI and
HYPE (known-saturated) and PUMP (known tick-locked) — so the re-rank has calibration points at the
saturated end.

```
collector /mnt/marketdata hyperliquid \
  VVV JTO GRAM ZRO PENDLE AERO CRV NEAR DOT ONDO KAITO INJ ETHFI  SUI HYPE PUMP
collector /mnt/marketdata binancefuturesum \
  vvvusdt jtousdt gramusdt zrousdt pendleusdt aerousdt crvusdt nearusdt dotusdt \
  ondousdt kaitousdt injusdt ethfiusdt  suiusdt hypeusdt pumpusdt
```

`--hl-l2-modes` default `slow,fast` is correct — keep both; `book_mode='bbo+fast'` is the pairing the live
connector is specified on, so the recording matches live behaviour.

**Duration: 10–14 days,** to cover at least one weekend and one volatile session. §8.2 is the binding
uncertainty and only calendar time fixes it.

**Capacity.** HL measured at 15–23 MB/day/coin for BTC/ETH/SOL (alts will be lighter) + 2.4 MB/day/coin for
`activeAssetCtx` → ~**5 GB** for 16 coins over 14 days. Binance UM is far heavier: budget with real
headroom and note the `premiumIndex` poller costs **~1.6 GB/day of ingress** regardless of symbol count —
on a metered link that is the number that matters, not the file sizes.

### 9.2 What to compute on the recording

1. **Conditional spread curve** — median touch spread bucketed by realised-vol quantile, from `bbo` at
   0.14 s median interval. *Flat curve = professionally made; rising curve = organic.* §8.3. This is the
   single highest-value output and the one thing a snapshot fundamentally cannot produce.
2. **Time-at-one-tick** — fraction of the session the book sits at exactly 1 tick. §2.1 gives a point
   estimate; the recording gives the distribution. This is the number that most directly separates the
   shortlist from §6.1, and it converts the whole tick argument from inference to measurement.
3. **Touch-queue survival** — distribution of order count and USD at the best bid/ask, and how long a
   front-of-queue position survives. §2.2 says the shortlist touch is one $250 order; confirm that it
   *stays* that way rather than being a sampling artifact.
4. **Depth at the intended grid rungs** (±10 / ±20 / ±30 bps) from `l2Book fast` at 0.54 s — not the 20-level
   slow feed at 5.4 s, and not the 20-level cap that produced §8.7.
5. **HL↔Binance lead-lag, in milliseconds** — cross-correlate HL `bbo` against UM `@bookTicker` per coin.
   This replaces the HL/UM volume ratio proxy with a direct measurement of the §8.9 exposure, and it is the
   only way to know which candidates are actually the designated stale liquidity.
6. **Re-run `score.py`** with recorded medians substituted for the 7-sample medians, and with components 1–5
   added. Then re-read §8.11 before treating the new ordering as meaningful.

### 9.3 Decision rule

Deploy only coins that, on recorded data: keep `spread_ticks ≥ 3` for a **majority of the session**, show a
**rising** conditional-spread curve, and hold a touch queue a small order can realistically join. Start with
the top 3–5 at minimum size. Re-run the whole screen **quarterly** — §8.12.

### 9.4 One live-path prerequisite, unrelated to selection — ALREADY MET

The trap this section would have warned about (`AGENTS.md` §4.1/§4.1a: `LiveBot` applies only kind-1
incremental depth events while Hyperliquid publishes only full snapshots) is closed: the HL connector's
`DepthMirror` synthesises kind-1 deltas from `bbo` + `l2Book fast` (Phase 1, testnet-verified with a live
LiveBot — non-empty uncrossed book tracking `bbo`; deletions-before-inserts keeps the §4.7 fusion path
inert), and the full order path traded a real testnet grid session end to end. See
`docs/design-hyperliquid-connector.md` §10–11.14.

---

## 10. Reproduction

| File | What |
|---|---|
| `metrics.json` | METRICS agent output, 233 rows, 87 sampled |
| `landscape.md` | MM-LANDSCAPE agent output |
| `l2_full.json` | raw 20-level books, 7 samples × 87 coins — source for §2.2 |
| `binance_24hr.json` | Binance UM 24h ticker, closeTime 2026-07-28T22:32:04Z — source for §2.3 |
| `enriched.json` | metrics rows + `tick_bps` / `spread_ticks` (§2.1) |
| `score.py` | gates, weights, landscape table, touch-queue merge — all thresholds are literals in one file |
| `scored.json` | full 87-row output with per-component breakdown, gate failures and landscape notes |

`python3 score.py` regenerates `scored.json` and prints the three tables. Every threshold in §3 is a
named constant; changing a weight and re-running is a one-line edit.
