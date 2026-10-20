# PREREG — перенос двух идей Teletype на секундную историю Binance

Frozen before the first strategy run, 2026-09-17. Source:
https://teletype.in/@calllsaul/F85WXTvNwav (2025-06-29).
Template: /home/andrew/hft-research/methodology/PREREG_TEMPLATE.md.

## 0. Disclosure before freezing
Read the article, archive names, cache loader/parser and its old integrity report.
Read previous study memory: spot1s_bench R1 resting limits generally adverse;
R2 shock relaxation sometimes positive, particularly after falls and latency-sensitive.
No outcomes of the rules below have been computed. The archive is previously used
research data, NOT an untouched holdout for the organisation. This is an explicitly
requested replication/price screen, not a search for a proprietary trading edge.

## 1. Three questions
Public signal, no proprietary advantage established. Capacity and $10/day at $2,000
unknown. Price prediction, not a venue procedure. The template's commercial screen
therefore does NOT pass: run because user explicitly asks to test this article;
do not authorise trading or claim TRADEABLE from this screen.

## 2. Prior
Prior weak/negative after costs. A trade through a distant quote selects adverse
price paths. The article targets MEXC small tokens: BTC/SOL Binance spot is a different
population. Failure here is failure of these formal rules on this population.

## 3. Clocks, inputs, windows, embargo
UTC exchange seconds. Input OHLC for bar t becomes available after t+1 second;
publication delay is NOT recorded. Cached c/h/l are float32; gaps are separately
recorded. No book, quantity, queue or receive-time data in this cache.

| Input | Source | Stamp | Availability | Status |
|---|---|---|---|---|
| last close, previous 30-min high/low | Binance spot 1s OHLC | bar open UTC | bar end + assumed lag | not measured |
| future high/low/close | same | bar open UTC | labels only | never signal |
| gap_s/gap_n | cached source omissions | UTC offsets | quality flags | excludes incomplete experiments |

Use all 108 BTC monthly cells (2017-08..2026-07), all 72 SOL cells
(2020-08..2026-07). Restart flat each UTC day: decisions start after 1800s warmup,
end early enough that TTL+hold+exit latency fit within the day. Reject a decision
if any source gap is present in its complete feature+possible-label envelope;
count exclusions; no forward-filled prices used as actual observations. All days
in each symbol's calendar window remain in the money denominator, including zero
trade days. Main comparative portfolio starts 2020-08-12 (both symbols have data).
Chronological diagnostic split 2023-08-01, no fitting. Purge 3630s on both sides;
this exceeds 1800s feature window + 1800s maximum hold + 30s lag. Training never
selects thresholds, symbols or variants.

## 4. Universe
BTCUSDT and SOLUSDT spot, fixed by pre-existing long archives, not by outcomes.
Long only is the cash-funded spot proxy. Shorts reported separately as hypothetical
price experiments, NOT spot executions, and not pooled into capital P&L. No leverage.
Do not extrapolate to MEXC, small coins or Binance perpetuals. DEX basis cannot be
tested from these data: requires synchronous prices, token mapping, gas and routing.

## 5. Signal and truth
At each UTC minute decision d read c[d-1] and history [d-1800,d).
S (spike): place buy at c[d-1]*0.99, separately mirror sell at *1.01. TTL 60s.
After fill: target 1%, stop 1%, max hold 900s.
R (range): past 1800s range L,H; (H-L)/c in [1%,7%],
abs(c[d-1]-c[d-1800])/(H-L) <= 0.25. Buy L+0.1*(H-L),
mirror sell H-0.1*(H-L); require each limit passive vs decision close.
Target midpoint (L+H)/2; stop 0.5*(H-L) away from entry; max hold 1800s.
Frozen levels, TTL 60s. These numeric definitions are ours, not a fully specified
algorithm supplied by the author. Barrier widths follow the article's percentages
and observed range, rather than introducing fitted volatility multiples.

## 6. All rules and sensitivities
Each rule/side has at most one pending order or position. Flat: next UTC minute.
Order active only d+lag. Cancel unfilled after 60s from activation. Both benchmark
markout and barrier labels computed on the same entry; the next decision waits for
max(actual exit time, fixed-markout horizon) to avoid overlap in either money column.
Main lag=1s, required penetration=1bp. Six scenarios: lag 0/1/2/5/30s at 1bp
penetration; lag1 with 0bp penetration (touch optimism). Lag0 still uses only closed
bars; not fill on the signal bar. No parameter selection across scenarios.
For long/short respectively fill if low < limit*(1-eps) / high > limit*(1+eps).
Strict inequality at eps0 too. Entry at limit, NOT at the more favourable extreme.
Stop checked before take when both occur in a bar; same entry-bar take never
credited. Entry-bar stop is checked conservatively because within-second order
is unknown. A triggered stop exits at close[trigger+max(1,lag)]; timeout also
exits at close[fill+hold+max(1,lag)]. TP is a resting limit filled only after
1bp penetration (or scenario eps); no execution improvement assumed.

## 7. Execution/cost
Queue, actual fills, tick/lot, book size and market impact unobserved: this is a
price-path proxy, not a limit-order backtest. Penetration is sensitivity, not proof
of a fill. Main fee 10bp each leg (Binance spot regular tariff verified 2026-09-17:
https://www.binance.com/en/fee/trading), applied to actual entry/exit notionals.
Not a reconstruction of historical fee holidays. Also 7.5bp/leg BNB-discount
scenario, and zero-fee/zero-slippage mutation. Base adverse slippage 1bp only on
stop/timeout exits (ASSUMED, not measured); markout exit also 1bp. Main fill/exit
lags same scenario ladder. These are not measured venue latencies.

## 8. Money, two columns
Markout at 900s S / 1800s R, AND first-touch barrier P&L, arithmetic returns on
entry notional; fees deducted on both legs. Fixed $1,000 sleeve per symbol, cash
longs only, $2,000 total without reinvestment. Reserve entry fee inside $1,000.
No overlapping trades per sleeve. Report unmeasured capacity, conditional on
full $1,000 fill. Report all-day $/day, CI90 moving-block bootstrap of 7 UTC days
(2,000 draws, seed 20260917), trade means/counts, stop/take/timeout and annuals.
An apparent positive is at most UNRESOLVED until book/queue/capacity validation.

## 9. Killers
K1: at least 20 fills and CI90 >0 for long cash portfolio, both gross and net.
K2: cannot pass TRADEABLE: exact capacity/actual fills unknown. Economic target
is net mean >=$10/day at $2,000 and CI90 lower >0.
K3: paired sign-flip/random-direction control on the SAME filled events/entry
times and fixed-horizon exit (1999 random signs); this tests directional
post-fill continuation vs chance, NOT whether limit fills are real. A separate
random-time control would mismatch the price level selection and is not used.
K4: independent synthetic arithmetic oracle, fees and causal timestamps; validate
raw-versus-cache 300 bars at start/middle/end of BTC2017-08/SOL2020-08/BTC2025-01/
SOL2026-07 within float32 relative tolerance 1e-7. This is a data/accounting
anchor, not independent replication of the article (no numeric series supplied).
K5: signed return from fill-hold to fill vs fill to fill+hold. Report both;
past is selected by quote crossing and is diagnostic, not itself tradable.
K6: DSR >=0.95, conservative 48 trials (2 rules*2 symbols*2 sides*6 variants),
daily Sharpe, skew/kurtosis correction; also report years and chronological split.
No claim from a best sensitivity cell. EXISTS only means this proxy's effect;
NOT EXISTS refers only to the frozen rule/population; incomplete audit UNRESOLVED.

## 10. Mutation contract
Tests must catch using signal-bar high/low to fill; touching without penetration;
same-bar TP; choosing TP before SL; ignoring execution lag; fees only one leg;
inventing fills across missing bars; overlapping sleeve holdings. Zero costs
must improve each fixed path by the exact fees+slippage amount. Independent pure
Python execution oracle vs compiled evaluator on seeded synthetic paths.

## 11. Computation
At most 12 workers, OMP/OPENBLAS/MKL threads=1; <60GB. Hash every cache file and
source code in data/pins.tsv. Keep raw files read-only. Freeze sha256 and a local
isolated git commit before outcomes. Persist PREREG, implementation, tests,
machine-readable outputs, source links, limitations, deviations and git bundle.
No scheduled forward or trading actions. Independent-agent review is not part
of this run; disclose that template requirement as unmet, never imply review.
