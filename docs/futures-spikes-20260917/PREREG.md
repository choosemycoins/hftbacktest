# Futures only: resting spike quotes scaled by preceding ATR

Frozen 2026-09-17 before strategy P&L. Template:
`/home/andrew/hft-research/methodology/PREREG_TEMPLATE.md`.
User explicitly restricts all further research to futures. No spot inputs.

## 0. Prior inspection
Saw raw schemas, file counts/sizes, one historical exchangeInfo snapshot and
commission records, earlier spot study outcomes (not transferable). No outcomes
of the rules below inspected. Extractor built/tested on synthetic messages first.

## 1. Three questions
Public volatility/context; no exclusive signal proven. Capacity/$10 per day
unknown. This predicts rebound, not venue procedure. Commercial discovery gate
not passed: explicit user-requested test still proceeds, never implicit trading.

## 2. Prior
Low prior for net profitability. ATR may normalize quote distance across regimes
without curing adverse selection. Fixed-1% control uses identical futures engine.

## 3. Clocks, inputs and split
Decisions and horizons on Tokyo collector local UTC receipt clock, integer ns.
Quotes/trades retain exchange transaction T as a second clock. Trade must ALSO
have T >= assumed order activation UTC: old delayed prints cannot fill new orders.
No alignment shift fitted; actual order acknowledgements/clock offset unavailable.
| Input | stamp | available | source |
| BBO bid/ask/size | T and local receive | local receive | raw bookTicker |
| Trades price/qty/aggressor | T and local receive | local receive | raw trade MARKET |
| ATR candles | end of receipt-clock interval | only strictly completed bars | BBO mid |
| tick, lot, min notional | archived receive | snapshot preceding UTC day | exchangeInfo |
| next funding | REST receive | receive, max age 1800s | premiumIndex |
Period 2026-08-12..2026-09-09 inclusive, 29 calendar days. Daily reset flat;
225min warmup for all configurations (14 complete 15min TRs plus prior close).
No input clamping. Full prospective label envelope must have no BBO gap >10s;
this is missing-data censoring, not a live signal. At decision BBO age <=2s.
Chronological split at 2026-08-27; no fitting. Embargo 14,461 seconds each side
of split (225min feature + 60s TTL + 900s hold + 1s baseline), conservatively
14,470s for all variants. Daily reset already enforces larger overnight separation.

## 4. Universe / reachable set
All 15 crypto perpetuals recorded by binancefuturesum-c, no outcome selection:
1000BONKUSDT,1000PEPEUSDT,1000SHIBUSDT,ACEUSDT,APTUSDT,DYMUSDT,MERLUSDT,
MORPHOUSDT,NEIROUSDT,NILUSDT,PENGUUSDT,PEOPLEUSDT,PYTHUSDT,SAGAUSDT,XAIUSDT.
435 daily source files, approximately 44.1GiB. No USDC/spot/stock contracts.
Daily metadata must say PERPETUAL/TRADING, USDT settlement, COIN underlying.
Both directions have separate hypothetical hedge-mode sleeves; no real account
mode or permissions assumed. $2000 / (15*2) maximum notional per sleeve, no leverage.

## 5. Signal / price / units
Previous completed 1/5/15-minute BBO-mid candles: true range = max(high-low,
abs(high-prevclose),abs(low-prevclose)). ATR is arithmetic mean of previous 14 TRs.
Distance = max(k*ATR/mid,5*tick/mid), skip distance >5%. Order level rounded away
from current mid to legal tick. Fixed control distance=1%, same tick/eligibility.
ATR and current reference use only observations available before decision.
All outcome prices from the traded perpetual's own BBO/trades, not an external lead.

## 6. Rules, locked variants
Main: 5min ATR, k=2, lag=1s. Sensitivities: timeframe 1/5/15min x k=1/2/3
(9 rules) plus fixed1% (10 total). Additional lag0.1s and lag5s for main+fixed
(4 cells), hence 14 configurations. No winning-cell selection for verdict.
At each UTC minute if flat/unquoted, post buy below / sell above mid at distance.
Order reaches exchange after lag. Post-only check against then-available BBO;
if it would cross, reject and retry at a later minute. TTL60s after activation.
Fill only on correct aggressor trade strictly THROUGH level by >=1 tick, with
single observed print qty >= our precomputed qty, positive book, and T>=activation.
Qty: floor to lot min($2000/30/limit, 10% of smaller observed BBO side qty).
Must meet minQty/notional. This is a conservative full-fill proxy, not queue proof.
One position per side/symbol. After flattening, next UTC minute can quote again;
no artificial 15min cooldown. Paired fixed/adaptive comparison uses same executor.
TP/SL remain +/-1% of entry and max hold900s to isolate entry-distance change.
TP/SL detected on executable bid(long)/ask(short), exits TAKING at next fresh BBO
after lag, plus 1 adverse tick. Timeout likewise. Price can gap past the barrier.
Also compute fixed900s markout on executable BBO (a diagnostic, not a second
simultaneously fundable strategy). Signal features and order levels frozen at entry.

## 7. Costs / executable constraints
USDT regular maker2bp entry, taker5bp ALL exits: current published schedule
https://www.binance.com/en-KZ/support/faq/detail/360033544231 . Account-specific
USDT tier unverified: label assumption. BBO spread measured, adverse tick assumed.
Hypothetical zero fee and zero impact, plus midpoint-exit diagnostic.
Stress maker3/taker6bp and lag0.1/1/5s. Capacity flag if full exit qty exceeds
observed BBO size; never silently discard its P&L or pretend multi-level fill known.
Block new orders whose latest observed nextFundingTime <= possible max exit+60s;
unknown/stale funding schedule => no quote. Check any actual crossing separately.
No funding assumed zero for an observed crossing; such monetary result unresolved.
If params missing/stale/changed within day, identify and disclose, do not fabricate.

## 8. Two money columns
Entry/exit actual qty cash flows, both fees on their own notionals. Fixed sleeve
notional, no reinvestment: a P&L normalization, not bankruptcy-safe account equity.
Barrier P&L is money verdict; 900s markout diagnostics may overlap and are not
reported as concurrently executable portfolio income. Report gross/net $/29 calendar
days, n, direction, instrument, TP/SL/time, capacity/funding/gap exclusions.
CI90: 5,000 bootstrap replicates, shared 3-day UTC blocks across all symbols,
seed20260917. Per-symbol and two temporal halves; paired main-minus-fixed daily
P&L on same calendar. 29 days is limited regime coverage, no multi-year claims.

## 9. Killers
K1 n>=20, net CI90 lower>0, both halves positive. K2 mean>=$10/day at $2000,
capacity clean, no funding crossings, and strict latency; otherwise no TRADEABLE.
K3 paired adaptive-minus-fixed same-day statistic with 1999 Rademacher block
sign flips: tests benefit of volatility normalization over fixed1%, not all MR.
K4 synthetic independent execution/accounting oracle plus raw message parser
cross-check against Python JSON on >=100 messages/file type in three source files.
K5 same-direction return over 900s strictly before fill vs after; selected drop
is expected and not a proof of predictability. No causality claim from this alone.
K6 DSR>=.95 with 420 trials (14 variants*15 symbols*2 sides), no post-hoc winner.
Positive strict price result is UNRESOLVED without actual queue/ack validation;
negative result rejects these rules on this period, not every spike strategy.

## 10. Mutation contract
Catch future candle access, reversed aggressor, old trade filling new order,
touch/insufficient print qty, wrong tick direction, post-only marketability,
ignoring funding, wrong fee notionals, early stop execution, overlapping positions.
Zero-cost cash-flow identity must hold. Independent second calculation of the
two decisive main/fixed portfolio figures; no claim of separate-agent review.

## 11. Compute / provenance
<=12 workers, numerical libraries one thread, <60GB. Raw archive read-only.
SHA256 source archives, parameters and implementation, isolated git commit and
PREREG checksum before outcome calculation. Artifacts under /tmp then persistent
docs/futures-spikes-20260917. Independent-agent template requirement unmet;
separate oracle/mutations used, limitation disclosed. No forward trading/scheduler.
