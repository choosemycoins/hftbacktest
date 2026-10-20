# COHERENCE.md — PREREG §13, run before the observed class statistic was read

Cube: 10421 symbol-days, 150 symbols, 35 ISO weeks, days 2025-12-26..2026-08-18

## CC5 — half-tick vs the effect the study claims to detect (prior 1.19 bps)
half-tick bps: median 0.4013  p10 0.0593  p90 2.2405  max 6.5920
symbols whose half-tick exceeds the prior 1.19 bps: 36 of 150
CC5 verdict: PASS - the class median half-tick is below the prior effect

## CC3 — does the data volume resolve the quantity at all?
(null sd of the class estimate under the day-sign-flip null; scale only, no sign)
h=1   null sd =   0.2359 bps | 2-sided 95% null band +- 0.4624 | prior 1.19 is RESOLVABLE
h=10  null sd =   0.1447 bps | 2-sided 95% null band +- 0.2835 | prior 1.19 is RESOLVABLE
h=60  null sd =   0.0875 bps | 2-sided 95% null band +- 0.1715 | prior 1.19 is RESOLVABLE
h=300 null sd =   0.1201 bps | 2-sided 95% null band +- 0.2355 | prior 1.19 is RESOLVABLE
h=SE  null sd =   0.8995 bps | 2-sided 95% null band +- 1.7630 | prior 1.19 is NOT resolvable
CC3 verdict: horizons that resolve a 1.19 bps effect: ['1', '10', '60', '300']

## CC1 — planted-control power (20 seeds, fraction promoted; PREREG §11 C1)
Power is measured on the STATISTICAL conditions (i)+(iii) of PREREG §9; conditions
(ii) half-tick, (iv) instrument bias and (v) money are properties of the data, not of
the test's ability to see delta, and are checked against delta separately (CC5, §6).
PAIRING, fixed here and used unchanged below: a SINGLE-SYMBOL plant is detectable only
by the per-symbol Westfall-Young test (it moves the 151-symbol class mean by delta/151,
which no class-level rule can or should see); a WHOLE-CLASS plant is judged by the
class rule. Measuring a single-symbol plant against the class rule would report a
power of zero for a working pipeline, which is a defect of the measurement, not of the test.
h=10  delta=0.50  single-symbol plant power (WY per-symbol) = 0.70 (14/20 seeds)
h=10  delta=0.50  WHOLE-CLASS plant power = 1.00 (20/20 seeds)
h=10  delta=1.19  single-symbol plant power (WY per-symbol) = 0.95 (19/20 seeds)
h=10  delta=1.19  WHOLE-CLASS plant power = 1.00 (20/20 seeds)
h=60  delta=0.50  single-symbol plant power (WY per-symbol) = 0.50 (10/20 seeds)
h=60  delta=0.50  WHOLE-CLASS plant power = 1.00 (20/20 seeds)
h=60  delta=1.19  single-symbol plant power (WY per-symbol) = 0.80 (16/20 seeds)
h=60  delta=1.19  WHOLE-CLASS plant power = 1.00 (20/20 seeds)
h=300 delta=0.50  single-symbol plant power (WY per-symbol) = 0.20 (4/20 seeds)
h=300 delta=0.50  WHOLE-CLASS plant power = 1.00 (20/20 seeds)
h=300 delta=1.19  single-symbol plant power (WY per-symbol) = 0.60 (12/20 seeds)
h=300 delta=1.19  WHOLE-CLASS plant power = 1.00 (20/20 seeds)

## CC2 — empirical FWER of the promotion rule on fully permuted data (200 draws)
h=10  empirical FWER = 0.000 (0/200) | criterion [0.01, 0.10] -> FAIL
h=60  empirical FWER = 0.000 (0/200) | criterion [0.01, 0.10] -> FAIL
h=300 empirical FWER = 0.010 (2/200) | criterion [0.01, 0.10] -> PASS

## READING OF THE CHECK-LIST (written before the observed class statistic was read)

CC5 PASS. Class median half-tick 0.4013 bps, below the 1.19 bps prior. But 36 of 150
symbols have a half-tick ABOVE 1.19 bps: for those symbols the prior's own effect size is
sub-lattice, and any symbol-level number there is a lattice reading, not an effect.

CC3 SPLIT. Horizons 1 s / 10 s / 60 s / 300 s resolve a 1.19 bps effect (null sd
0.0875-0.2359 bps). The session-end horizon does NOT (null sd 0.8995, 95% null band
+-1.763 bps): at that horizon the mark is a single price shared by every print of the day,
so the cluster statistic is the day's drift, not a realized spread. PREREG §12 K5 therefore
applies to h=SE in advance: it is reported but is NOT eligible for promotion, and its point
estimate must be read as unresolved rather than as a measurement.

CC1 PASS at the class level, SPLIT at the symbol level. The WHOLE-CLASS plant - which is
the level the question is asked at - is detected in 20/20 seeds at both delta=0.50 and
delta=1.19, at every resolvable horizon. The single-symbol WY test reaches the declared
0.80 target at delta=1.19 for h=10 s (0.95) and h=60 s (0.80) but falls short at h=300 s
(0.60). Consequence, fixed here: a class-level NO is a powered NO; a symbol-level NO at
h=300 s is not, and will be reported as underpowered rather than as absence.

CC2 FAIL by the declared band at h=10 s and h=60 s (empirical FWER 0.000), PASS at
h=300 s (0.010). The cause is structural and identifiable: the promotion rule of §9 is a
CONJUNCTION of three tests each of nominal size 0.05 - the class test and both ISO-week
halves - so its joint size is far below 0.05. The replication requirement of §7 is itself a
size reducer. This is conservatism, not a broken null: the day-sign-flip null is exact by
construction, and CC1 shows the same conjunction still detects delta=0.50 in 20/20 seeds.
The honest consequence, fixed before unblinding: a PROMOTION under this rule would be
conservative and trustworthy; a NON-promotion is not, on its own, proof of absence - but
because CC1 gives class-level power 1.00 at delta=0.50, absence of a class effect of the
prior's size IS evidenced here. "Zero promotions" is recorded as a FAIL of calibration, as
the pre-registration demanded, and is not laundered into a pass.

CC4 satisfied: the seed-aggregation rule (fraction of 20 seeds, not the maximum) was fixed
in PREREG §11 C1 before any seed was drawn, and is what is reported above.

CC6 satisfied: the instrument-bias budget was declared in PREREG §6 before b(h) was
measured. Measured on the KR six: b = +0.4674 / +0.0418 / -0.3691 / -0.0210 bps at
1 / 10 / 60 / 300 s, correlation 0.80 / 0.92 / 0.97 / 0.99.
