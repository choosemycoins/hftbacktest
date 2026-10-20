# RESULTS #105 — mature TRADIFI_PERPETUAL, passive maker at the touch
PREREG sha256: 0edd00e031b391c09968198afb9fa1aa4eec6fa236abc2eddd12f76f04aff3b3

Admitted: 10421 symbol-days, 150 symbols, 2025-12-26..2026-08-18, 35 ISO weeks
Class median half-tick = 0.4013 bps (p90 2.2405)

## (a) Realized-spread term structure, bps per print  [maker fee 0.00 bps]
h      RS eq-wt    RS $-wt  capture c  adverse m      CI lo      CI hi   p(flip)  n prints
1       +1.6193    -0.4608    +1.1186    +0.8003    +1.5499    +1.6898    0.0000 607971996
10      +0.8696    -0.5620    +1.1193    +1.0169    +0.7953    +0.9436    0.0000 595571733
60      +0.2910    -0.5597    +1.1116    +1.1048    +0.1703    +0.4128    0.0000 591571008
300     +0.1351    -0.4938    +1.0974    +1.1558    -0.0558    +0.3365    0.1350 586742854
SE      -0.9442    -1.3097    +0.9882    +1.3168    -1.6544    +0.2025    0.8326 712112746
half-tick reference: class median 0.4013 bps  (any |RS| <= its half-tick = lattice)

## (b) net at maker 0.00 (= RS itself) and sensitivity at 2.0 bps maker fee
h       net @0.00   net @2.0bp    half-tick instr bias b(h)
1         +1.6193      -0.3807       0.4013      +0.4674
10        +0.8696      -1.1304       0.4013      +0.0418
60        +0.2910      -1.7090       0.4013      -0.3691
300       +0.1351      -1.8649       0.4013      -0.0210
SE        -0.9442      -2.9442       0.4013         +nan

## C5 — legs in arithmetic price space, reported separately
h       maker BUY        n buy   maker SELL       n sell
1         -0.1956    303459390      -0.2261    304512606
10        -0.3386    297176436      -0.4222    298395297
60        -0.3477    295132204      -0.5081    296438804
300       -0.2803    292696175      -0.6425    294046679
SE       +19.0073    355306461     -20.6579    356806285

## MASK-A (primary) vs MASK-B (all 1440 minutes)
h          MASK-A       MASK-B
1         +1.6193      +1.5110
10        +0.8696      +0.8452
60        +0.2910      +0.4233
300       +0.1351      +0.2147
SE        -0.9442      -0.9783
MASK-A kept prints: median share 0.9901 ; symbols with all-1440 mask: 68 of 150

## (K3) replication across ISO-week halves
h=1    odd weeks  +1.6325 (p=0.0000, 5343 sym-days)   even weeks  +1.6196 (p=0.0000, 5078)
h=10   odd weeks  +0.8915 (p=0.0000, 5343 sym-days)   even weeks  +0.8583 (p=0.0000, 5078)
h=60   odd weeks  +0.3093 (p=0.0023, 5343 sym-days)   even weeks  +0.2806 (p=0.0187, 5078)
h=300  odd weeks  +0.1801 (p=0.1690, 5343 sym-days)   even weeks  +0.0990 (p=0.2623, 5078)
h=SE   odd weeks  -0.8048 (p=0.8093, 5343 sym-days)   even weeks  -0.4535 (p=0.7150, 5078)

## per-ISO-week class estimate, h=60s and h=300s
h=60  202552:+2.404 202601:+0.364 202602:+0.145 202603:+0.046 202604:-0.011 202605:-0.214 202606:-0.174 202607:-0.351 202608:-0.491 202609:-0.214 202610:-0.559 202611:-0.530 202612:-0.246 202613:-0.355 202614:-0.103 202615:-0.254 202616:-0.289 202617:-0.072 202618:-0.234 202619:-0.101 202620:-0.238 202621:-0.182 202622:-0.270 202623:-0.266 202624:-0.252 202625:-0.067 202626:+0.113 202627:+0.271 202628:+0.187 202629:+0.056 202630:-0.015 202631:+0.164 202632:+0.369 202633:+0.242 202634:+0.194
h=60  weeks positive: 12 of 35
h=300  202552:+2.574 202601:-0.096 202602:+0.222 202603:+0.137 202604:+0.054 202605:-0.019 202606:-0.007 202607:-0.215 202608:-0.495 202609:-0.119 202610:-0.547 202611:-0.419 202612:-0.155 202613:-0.406 202614:-0.032 202615:-0.363 202616:-0.369 202617:+0.013 202618:-0.276 202619:-0.184 202620:-0.189 202621:-0.205 202622:-0.417 202623:-0.367 202624:-0.123 202625:-0.158 202626:+0.048 202627:+0.010 202628:+0.063 202629:-0.397 202630:-0.096 202631:-0.011 202632:+0.043 202633:+0.414 202634:+0.182
h=300  weeks positive: 11 of 35

## sub-classes (underlyingType), max-T over 6 sub-classes
h=60
   COMMODITY    n_sym=  8  RS= -0.1602  p(flip)=1.0000
   EQUITY       n_sym=130  RS= +0.3165  p(flip)=0.0000
   HK_EQUITY    n_sym=  7  RS= +0.5061  p(flip)=0.0063
   KR_EQUITY    n_sym=  3  RS= +0.0439  p(flip)=0.2687
   PREMARKET    n_sym=  2  RS= +0.0525  p(flip)=0.3537
h=300
   COMMODITY    n_sym=  8  RS= -0.1492  p(flip)=1.0000
   EQUITY       n_sym=130  RS= +0.1395  p(flip)=0.1560
   HK_EQUITY    n_sym=  7  RS= +0.5591  p(flip)=0.0227
   KR_EQUITY    n_sym=  3  RS= -0.0759  p(flip)=0.7313
   PREMARKET    n_sym=  2  RS= -0.1762  p(flip)=0.8537

## Westfall-Young max-T over symbols (day-level sign flip, 5000 draws)
h=60  family-adjusted 95% max-T threshold = 3.992 ; symbols promoted = 18 of 149
    symbol     mean         t  nday  adj_p
   NOKUSDT 1.424053 15.304461  64.0 0.0000
  BMNRUSDT 1.470194 10.397259  54.0 0.0000
  ONDSUSDT 2.514870  8.575329  62.0 0.0000
HK1810USDT 0.975377  7.317036  13.0 0.0004
   TBTUSDT 5.747873  6.082300   8.0 0.0186
   EWJUSDT 0.508732  5.987728 138.0 0.0186
  SMCIUSDT 0.873014  5.292779  43.0 0.0196
  KORUUSDT 0.489822  5.125195  43.0 0.0202
h=300  family-adjusted 95% max-T threshold = 3.961 ; symbols promoted = 9 of 149
  symbol      mean        t  nday  adj_p
 NOKUSDT  1.413976 7.988235  64.0 0.0000
BMNRUSDT  1.596930 6.270616  54.0 0.0000
 TBTUSDT  5.940883 5.554769   7.0 0.0090
BITOUSDT 10.058407 5.423194   8.0 0.0094
SOFIUSDT  3.322689 5.124577  14.0 0.0098
ONDSUSDT  2.377181 4.562656  62.0 0.0128
NFLXUSDT  1.321149 4.491954  56.0 0.0136
KSTRUSDT  1.336759 4.230740  33.0 0.0378

## (C3) moment-mirror: markPrice 1m as an independent mark (backward, 0-60s coarse)
h=60    tape-mid  +0.2910   markPrice  +0.7606   p(flip)=0.0000   sign agrees: YES   n=889994024
h=300   tape-mid  +0.1351   markPrice  +0.4236   p(flip)=0.0005   sign agrees: YES   n=889994024
h=SE    tape-mid  -0.9442   markPrice  +0.2150   p(flip)=0.2605   sign agrees: NO -> instrument   n=889994024

## (c) money: $/day pie at the CEILING (maker takes 100%% of every print)
h=1     class ceiling = -185048.26 $/day   (sum    -43671390 $ over 236 distinct days)
h=10    class ceiling = -223592.87 $/day   (sum    -52767917 $ over 236 distinct days)
h=60    class ceiling = -221848.75 $/day   (sum    -52356305 $ over 236 distinct days)
h=300   class ceiling = -195064.72 $/day   (sum    -46035275 $ over 236 distinct days)
h=SE    class ceiling = -626814.80 $/day   (sum   -147928292 $ over 236 distinct days)
last 14 days only (current class size):
h=60    class ceiling = -527329.87 $/day over 14 days
h=300   class ceiling = -430111.45 $/day over 14 days

h=60  top-1 symbol BMNRUSDT +924.05 $/day ; top-5 sum +3465.98 $/day ; positive symbols 62 of 150
      top-5: BMNRUSDT +924.05, HK1810USDT +831.74, SQQQUSDT +580.57, SMCIUSDT +574.50, ASMLUSDT +555.12
h=300  top-1 symbol NBISUSDT +4626.64 $/day ; top-5 sum +9466.68 $/day ; positive symbols 62 of 150
      top-5: NBISUSDT +4626.64, BMNRUSDT +1306.06, AAOIUSDT +1287.71, HK1810USDT +1209.11, IRENUSDT +1037.17
KILL floor (#73) = $28.75/day

## notional and print scale
class notional $/day = 5517049864 ; prints/day = 3771161 ; median print notional $1462.96

## session structure (hourly, MASK-B universe): where the volume and the RS live
hUTC   %notional  capture c     RS 60s    RS 300s  in MASK-A
0         0.0307    +0.9886    -0.5015    -0.4757      0.993
1         0.0451    +1.0299    -0.4414    -0.6456      0.992
2         0.0335    +0.9629    -0.2921    -0.1963      0.990
3         0.0264    +0.9518    -0.2123    -0.2061      0.988
4         0.0225    +0.9135    -0.2159    -0.1914      0.987
5         0.0322    +0.9407    -0.3114    -0.3089      0.990
6         0.0330    +0.9177    -0.2799    -0.3163      0.989
7         0.0278    +0.8517    -0.1664    -0.1560      0.987
8         0.0393    +1.0400    -0.3117    -0.4627      0.992
9         0.0301    +0.8847    -0.2416    -0.2554      0.987
10        0.0288    +0.9350    -0.3248    -0.3543      0.985
11        0.0373    +1.0333    -0.3667    -0.4351      0.988
12        0.0500    +1.0770    -0.3572    -0.3972      0.992
13        0.1233    +1.5790    -0.7017    -0.8334      0.999
14        0.1066    +1.1373    -0.4254    -0.3365      1.000
15        0.0725    +0.9672    -0.2880    -0.2819      0.998
16        0.0526    +0.9126    -0.3496    -0.3814      0.997
17        0.0403    +0.8581    -0.3608    -0.2751      0.994
18        0.0370    +0.8820    -0.4209    -0.4584      0.994
19        0.0400    +0.9686    -0.3957    -0.5771      0.995
20        0.0297    +1.7514    -0.7277    -0.8718      0.987
21        0.0130    +1.2545    -0.3555    -0.4618      0.975
22        0.0232    +0.9691    -0.5117    -0.6893      0.982
23        0.0250    +0.9269    -0.5852    -0.4011      0.985
hours with RS(300s) > 0: 0 of 24 ; notional share of those hours: 0.0000

## funding as a post-hoc cost (method #40: a jump, paid only across a stamp)
funding stamps observed: 38427 ; exactly zero: 69.8% ; mean|rate| = 0.000171 (1.7084 bps per 8h)
   h=1   s  P(cross a stamp)=0.00003  expected |funding| = 0.000059 bps
   h=10  s  P(cross a stamp)=0.00035  expected |funding| = 0.000593 bps
   h=60  s  P(cross a stamp)=0.00208  expected |funding| = 0.003559 bps
   h=300 s  P(cross a stamp)=0.01042  expected |funding| = 0.017796 bps
   session-end (assume 12h mean hold): expected |funding| = 2.5626 bps


## THE WEIGHTING FORK (all three declared in PREREG §4/§10, none chosen after the fact)
h      per-SYMBOL    per-PRINT   per-DOLLAR    half-tick
1         +1.6193      -0.2109      -0.4608       0.4013
10        +0.8696      -0.3805      -0.5620       0.4013
60        +0.2910      -0.4281      -0.5597       0.4013
300       +0.1351      -0.4618      -0.4938       0.4013
SE        -0.9442      -0.8671      -1.3097       0.4013

The per-SYMBOL statistic gives a symbol with 8 admitted days the same vote as one with 236,
and a symbol with 30k prints the same vote as one with 144M. The per-PRINT and per-DOLLAR
statistics are what a maker actually collects. They disagree in SIGN.

## is the positive per-symbol number carried by thin symbols? (composition)
              n  rs_mean  prints_med  nday_med
q                                             
Q1 thinnest  30   1.1645     28531.5      28.5
Q2           30   0.4911    146344.0      55.0
Q3           30   0.3914    594964.0      57.0
Q4           30  -0.1873   3449815.0      85.5
Q5 busiest   30  -0.4049  13862354.5     104.5
Spearman(rs, prints) = -0.5842

## WY-promoted symbols vs their OWN half-tick and their OWN money
h=60 : 18 promoted; of those 6 clear their own half-tick; 1 clear KILL $28.75 at phi_size=0.034
     symbol  mean  halftick  above_halftick  nday  ceil_usd_day  phi_size_usd  phi_queue_usd
   BMNRUSDT 1.470     3.062           False  54.0       924.050        31.582        348.154
 HK1810USDT 0.975     1.895           False  13.0       831.743        28.427        313.375
   SMCIUSDT 0.873     1.712           False  43.0       574.495        19.635        216.452
    NOKUSDT 1.424     4.658           False  64.0       315.481        10.782        118.864
   SNOWUSDT 2.663     0.156            True  25.0       279.507         9.553        105.310
   NFLXUSDT 0.796     0.678            True  56.0       263.623         9.010         99.325
   SOFIUSDT 2.971     2.734            True  14.0       171.856         5.874         64.750
    EWJUSDT 0.509     0.542           False 138.0        78.107         2.669         29.428
    SMHUSDT 2.169     0.085            True   6.0        42.418         1.450         15.982
    TBTUSDT 5.748     1.303            True   8.0         1.849         0.063          0.696
   ONDSUSDT 2.515     6.361           False  62.0         0.646         0.022          0.243
HYUNDAIUSDT 0.908     0.169            True  63.0       -33.378        -1.141        -12.576
    BBXUSDT 1.439     5.568           False  64.0       -96.593        -3.301        -36.393
    WENUSDT 2.681     6.592           False  26.0      -359.658       -12.292       -135.508
   USARUSDT 0.585     2.501           False  90.0      -389.253       -13.304       -146.659
   SNXXUSDT 0.977     4.486           False  26.0     -9708.761      -331.820      -3657.963
   KORUUSDT 0.490     2.576           False  43.0    -27281.692      -932.416     -10278.904
    XAGUSDT 0.089     0.672           False 209.0    -48118.224     -1644.553     -18129.470

h=300 : 9 promoted; of those 4 clear their own half-tick; 1 clear KILL $28.75 at phi_size=0.034
  symbol   mean  halftick  above_halftick  nday  ceil_usd_day  phi_size_usd  phi_queue_usd
BMNRUSDT  1.597     3.062           False  54.0      1306.062        44.638        492.084
NFLXUSDT  1.321     0.678            True  56.0       422.747        14.448        159.278
ONDSUSDT  2.377     6.361           False  62.0       345.583        11.811        130.205
 NOKUSDT  1.414     4.658           False  64.0       341.159        11.660        128.538
 EWJUSDT  0.535     0.542           False 138.0       122.065         4.172         45.990
SOFIUSDT  3.323     2.734            True  14.0       103.606         3.541         39.036
BITOUSDT 10.058     5.800            True   8.0         0.587         0.020          0.221
 TBTUSDT  5.941     1.303            True   7.0         0.414         0.014          0.156
KSTRUSDT  1.337     2.002           False  33.0      -275.097        -9.402       -103.648

## FCFS under one capital (PREREG §10): a maker cannot be first in queue on 150 symbols
h=60   top-1  ceiling   +924.05 $/day | x phi_size(0.034) =   +31.58 | x phi_queue(0.377) =  +348.15 | KILL 28.75
h=60   top-5  ceiling  +3465.98 $/day | x phi_size(0.034) =  +118.46 | x phi_queue(0.377) = +1305.87 | KILL 28.75
h=300  top-1  ceiling  +4626.64 $/day | x phi_size(0.034) =  +158.13 | x phi_queue(0.377) = +1743.18 | KILL 28.75
h=300  top-5  ceiling  +9466.68 $/day | x phi_size(0.034) =  +323.55 | x phi_queue(0.377) = +3566.76 | KILL 28.75

## class money under the capacity haircut (whole class, all 150 symbols)
h=60   class ceiling  -221848.75 $/day | x phi_size =    -7582.20 | x phi_queue =   -83585.80
h=300  class ceiling  -195064.72 $/day | x phi_size =    -6666.79 | x phi_queue =   -73494.40

## per-PRINT reading after the MEASURED instrument correction (PREREG §6)
h       per-print   minus bias    survives?
1         -0.2109      -0.6783 still negative
10        -0.3805      -0.4223 still negative
60        -0.4281      -0.0590 still negative
300       -0.4618      -0.4408 still negative

## moment-mirror read per PRINT (markPrice, backward 0-60s -> understates adverse -> biased UP)
h=60   tape-mid per-print  -0.4281 | markPrice per-print  -0.2754 | sign agrees: YES
h=300  tape-mid per-print  -0.4618 | markPrice per-print  -0.4399 | sign agrees: YES
h=SE   tape-mid per-print  -0.8671 | markPrice per-print  -0.7343 | sign agrees: YES

## is the class money a few symbols, or broad? (h=60s ceiling, $/day)
symbols positive 62 / negative 88 of 150
sum of all -497176.65 ; sum of 5 worst -252757.70 ; sum of 5 best +3465.98
5 worst: SPCXUSDT -75738, XAGUSDT -48118, SNDKUSDT -44862, SOXLUSDT -44595, SKHYNIXUSDT -39444
class ex-5-worst = -244418.95 $/day
median symbol -49.43 $/day ; sum of all POSITIVE symbols +10056.03 $/day

## sanity: does the sign convention match the real book? (KR six, PREREG C4)
capture measured against the TRUE bookTicker mid: mean +6.5067 bps, min +1.7576, all positive: True
(a maker resting at the touch must capture a POSITIVE half-spread; a flipped convention
 would make every one of these negative)


# PART II — the frozen grid filled in: class AND every declared sub-class, EVERY horizon

Generated by gross.py + report2.py on the same frozen cube; PART I above is byte-identical
to the version hashed in RESULTS.sha256 as RESULTS_v1.md. Nothing in PART I was edited or
re-estimated. PART II adds what PART I did not print: the sub-classes at every horizon
instead of 60/300 s only, each sub-class against its OWN half-tick, the three declared
weightings side by side everywhere, the measured instrument correction as its own column,
and the week replication for the sub-classes.

PREREG sha256    0edd00e031b391c09968198afb9fa1aa4eec6fa236abc2eddd12f76f04aff3b3
COHERENCE sha256 0ae704a0fd9c6ff3454479771b735751e4b818db46fa1a375a935e6aea4687dc
substrate        cache/cube (pass2.py, frozen) — not recomputed in this pass
admitted         10421 symbol-days, 150 symbols, 2025-12-26..2026-08-18, 236 UTC days, 35 ISO weeks, 889994024 prints, $1.302 T notional
nulls            day-level sign flip, 5000 draws, seed 20260820 (shared across class, sub-classes and week halves within a horizon)
CIs              cluster bootstrap over whole UTC days, 2000 draws, seed 20260821
fees             maker 0.00 bps / taker 4.00 bps (measured 2026-08-19, bare VIP0 key) — the net at maker 0.00 IS the gross

Reproduction check (PART I must survive this pass unchanged):
  h=1    per-symbol now +1.6193   PART I +1.6193
  h=10   per-symbol now +0.8696   PART I +0.8696
  h=60   per-symbol now +0.2910   PART I +0.2910
  h=300  per-symbol now +0.1351   PART I +0.1351
  h=SE   per-symbol now -0.9442   PART I -0.9442
Independent recomputation (verify_gross.py: plain pandas groupby instead of bincount over
factorized codes) of all 90 class/sub-class x horizon x weighting cells: max |deviation|
8.9e-16 bps; KR instrument bias 0.0e+00; identity RS = c - m 3.3e-16. PASS = True.

What the frozen grid says, in one block (details below, every number with its half-tick):
  * the SIGN of the population gross is a WEIGHTING CHOICE, and all three weightings were
    declared before any number existed: at 60 s per-SYMBOL +0.2910, per-PRINT -0.4281,
    per-DOLLAR -0.5597, against a class median half-tick of 0.4013;
  * per-DOLLAR is negative at EVERY horizon for the class AND in 25 of 25 sub-class x
    horizon cells — no group in the frozen partition has a positive money-weighted gross;
  * NO sub-class is promoted at any horizon (0 of 25 cells), and condition (v), money,
    fails in all 25 on its own: every sub-class ceiling is negative;
  * the measured instrument bias at 60 s (-0.3691) is LARGER in magnitude than the class
    estimate it would correct (+0.2910), which is PREREG §9(iv) failing on a measurement.

## II.1 Class term structure — three declared weightings, half-tick beside every number

The half-tick column is the class MEDIAN half-tick (PREREG §3: any |estimate| <= its
half-tick is lattice, not effect). The print-weighted median half-tick is printed too,
because the per-PRINT statistic is not a symbol-equal-weight object and the symbol-median
bar is the wrong lattice reference for it.

h      per-SYMBOL  per-PRINT per-DOLLAR   CI sym lo   CI sym hi  p(flip)+  p(flip)- half-tick  HT pr-wt
1         +1.6193    -0.2109    -0.4608     +1.5516     +1.6950    0.0000    1.0000    0.4013    0.0974
10        +0.8696    -0.3805    -0.5620     +0.7931     +0.9441    0.0000    1.0000    0.4013    0.0974
60        +0.2910    -0.4281    -0.5597     +0.1635     +0.4185    0.0000    1.0000    0.4013    0.0974
300       +0.1351    -0.4618    -0.4938     -0.0637     +0.3330    0.1354    0.8646    0.4013    0.0974
SE        -0.9442    -0.8671    -1.3097     -1.6615     +0.2277    0.8240    0.1760    0.4013    0.0974

p(flip)+ is the frozen one-sided p (P[null >= observed]) for the per-SYMBOL statistic;
p(flip)- is its mirror, printed so a negative estimate can be read for significance instead
of silently scoring p ~ 1. The same pair for the per-PRINT statistic, which is the one that
carries the negative sign:

h       per-PRINT   p(flip)+   p(flip)-      nullsd
1         -0.2109     1.0000     0.0000      0.0284
10        -0.3805     1.0000     0.0000      0.0442
60        -0.4281     1.0000     0.0000      0.0499
300       -0.4618     1.0000     0.0000      0.0593
SE        -0.8671     0.9966     0.0034      0.3527

The negative per-print reading is not noise: at 1/10/60/300 s it sits 7.4 to 8.6 null sds
below zero under the frozen placebo, and the mirrored p is 0.0000 at all four. It is also
NOT lattice on its own reference: |per-PRINT| is 0.2109 .. 0.4618 bps against a print-weighted
median half-tick of 0.0974 bps — 2.2x to 4.7x the lattice of the symbols that carry the prints.

Own null sd of the class estimate (PREREG §13 CC3), per-symbol / per-print:
  h=1    nullsd(sym) 0.2354  nullsd(prn) 0.0284  — bar (class median half-tick) 0.4013, prior 1.19
  h=10   nullsd(sym) 0.1441  nullsd(prn) 0.0442  — bar (class median half-tick) 0.4013, prior 1.19
  h=60   nullsd(sym) 0.0861  nullsd(prn) 0.0499  — bar (class median half-tick) 0.4013, prior 1.19
  h=300  nullsd(sym) 0.1191  nullsd(prn) 0.0593  — bar (class median half-tick) 0.4013, prior 1.19
  h=SE   nullsd(sym) 0.9195  nullsd(prn) 0.3527  — bar (class median half-tick) 0.4013, prior 1.19

Session-mask sensitivity (PREREG §5: MASK-A primary, MASK-B = all 1440 minutes, both
declared in advance). PART I printed it per-SYMBOL; the per-PRINT statistic barely moves:

h       A per-sym   B per-sym   A per-prn   B per-prn half-tick
1         +1.6193     +1.5110     -0.2109     -0.1944    0.4013
10        +0.8696     +0.8452     -0.3805     -0.3677    0.4013
60        +0.2910     +0.4233     -0.4281     -0.4174    0.4013
300       +0.1351     +0.2147     -0.4618     -0.4551    0.4013
SE        -0.9442     -0.9783     -0.8671     -0.8607    0.4013

The mask moves the per-SYMBOL headline by up to 0.13 bps (less than the half-tick) and the
per-PRINT number by at most 0.017 bps. MASK-A keeps a median 99.01% of prints and is
all-1440-minutes for 68 of 150 symbols: on this class it turned out to be a volume filter,
not a session filter, and it is reported as such rather than credited with a job it did not do.

## II.2 The decomposition behind the sign: capture is flat, adverse selection climbs

h      capture c  adverse m     RS=c-m identity dev half-tick
1        +1.1186    +0.8003    +0.3183      2.2e-16    0.4013
10       +1.1193    +1.0169    +0.1024      2.1e-16    0.4013
60       +1.1116    +1.1048    +0.0068      8.8e-17    0.4013
300      +1.0974    +1.1558    -0.0585      7.6e-17    0.4013
SE       +0.9882    +1.3168    -0.3286      3.3e-16    0.4013

c and m are on the common subset (both mids finite), so RS here is that subset's RS and the
identity RS = c - m is checked numerically, not declared. Capture never moves: +1.11 .. +1.12
bps at 1..300 s. Adverse selection is what moves: +0.80 -> +1.16 and it crosses capture
between 10 s and 60 s. That crossing IS the answer to #105 at the class level.

Note the RS column here is NOT the per-PRINT number of II.1: this subset additionally
requires a finite mid at the fill instant (needed for c), and that requirement selects a
less negative subset — +0.3183 vs -0.2109 at 1 s, +0.0068 vs -0.4281 at 60 s. The decomposition is
therefore weaker evidence than the headline (PREREG §3 said so in advance) and is read for
SHAPE — flat c, rising m — not for level. The shape is the same on the true book (§II.8).

## II.3 Sub-classes at EVERY horizon (PREREG §8 partition, outcome-blind)

Mapping of the requested cut onto the frozen partition (exchangeInfo `underlyingType`):
  KR      -> KR_EQUITY   (3 mature symbols)
  HK      -> HK_EQUITY   (7)
  US      -> EQUITY      (130) — this bucket also HOLDS the US-listed ETFs
  CN      -> CN_EQUITY   (0 mature symbols: both CN names onboarded inside the last 14 days,
                          removed by the frozen age rule — the sub-class cannot appear at all)
  metals  -> COMMODITY   (8)
  (extra) -> PREMARKET   (2: ANTHROPICUSDT, OPENAIUSDT)
  ETF     -> NO SUCH CUT EXISTS in the frozen data. `underlyingSubType` is ('TradFi',) for 167
             of 170 symbols, ('Pre-IPO','TradFi') for 2, and ('ETF','TradFi') for exactly ONE
             (EWJUSDT). SPY, QQQ, SOXL, SQQQ, GDX are all plain EQUITY. An ETF sub-class would
             therefore need a hand-made ticker list written AFTER seeing the numbers, so it is
             not run as a promotion cut and no ETF row is printed. EWJUSDT — the one symbol the
             venue itself tags ETF — is reported per-symbol in PART I (+0.509 bps at 60 s
             against its own half-tick 0.542: lattice).

### h = 1
group        nsym  ndays  per-SYMBOL  per-PRINT per-DOLLAR      CI lo      CI hi   own HT    p raw   p maxT vs ownHT ceiling $/d
COMMODITY       8   1343     +0.1625    -0.1042    -0.4433    +0.1386    +0.1860   0.5793   0.0000   0.0000  lattice      -85711
EQUITY        130   8664     +1.7747    -0.2111    -0.4565    +1.6959    +1.8574   0.4013   0.0000   0.0000    ABOVE      -85214
HK_EQUITY       7     88     +1.1825    +0.2868    -0.2448    +1.0986    +1.3430   0.8547   0.0000   0.0004    ABOVE         -84
KR_EQUITY       3    189     +0.4581    -0.4373    -0.6606    +0.3429    +0.5819   0.1688   0.0000   0.0000    ABOVE      -13937
PREMARKET       2    133     +0.6172    +0.5993    -0.6525    +0.4100    +0.7987   0.0351   0.0000   0.0000    ABOVE        -102

### h = 10
group        nsym  ndays  per-SYMBOL  per-PRINT per-DOLLAR      CI lo      CI hi   own HT    p raw   p maxT vs ownHT ceiling $/d
COMMODITY       8   1343     -0.0851    -0.2422    -0.5238    -0.1152    -0.0526   0.5793   1.0000   1.0000  lattice     -101613
EQUITY        130   8663     +0.9613    -0.4068    -0.5923    +0.8774    +1.0380   0.4013   0.0000   0.0000    ABOVE     -108171
HK_EQUITY       7     88     +0.7342    +0.0121    -0.4682    +0.5833    +1.0012   0.8547   0.0000   0.0002  lattice        -154
KR_EQUITY       3    189     +0.1409    -0.4638    -0.6467    +0.0345    +0.2493   0.1688   0.0066   0.0278  lattice      -13427
PREMARKET       2    133     +0.2940    +0.0865    -1.4784    +0.0786    +0.5115   0.0351   0.0058   0.0292    ABOVE        -228

### h = 60
group        nsym  ndays  per-SYMBOL  per-PRINT per-DOLLAR      CI lo      CI hi   own HT    p raw   p maxT vs ownHT ceiling $/d
COMMODITY       8   1343     -0.1602    -0.2643    -0.5181    -0.1993    -0.1231   0.5793   1.0000   1.0000  lattice     -100569
EQUITY        130   8661     +0.3165    -0.4671    -0.6043    +0.1757    +0.4513   0.4013   0.0000   0.0004  lattice     -109506
HK_EQUITY       7     88     +0.5061    -0.2130    -0.4403    +0.2579    +0.9743   0.8547   0.0064   0.0376  lattice        -144
KR_EQUITY       3    189     +0.0439    -0.4514    -0.5522    -0.0930    +0.1820   0.1688   0.2646   0.7584  lattice      -11368
PREMARKET       2    133     +0.0525    -0.1741    -1.7176    -0.2045    +0.3078   0.0351   0.3472   0.8464    ABOVE        -261

### h = 300
group        nsym  ndays  per-SYMBOL  per-PRINT per-DOLLAR      CI lo      CI hi   own HT    p raw   p maxT vs ownHT ceiling $/d
COMMODITY       8   1343     -0.1492    -0.2548    -0.4693    -0.1963    -0.0995   0.5793   1.0000   1.0000  lattice      -91063
EQUITY        130   8631     +0.1395    -0.5251    -0.5336    -0.0842    +0.3662   0.4013   0.1488   0.5214  lattice      -96093
HK_EQUITY       7     88     +0.5591    -0.2902    -1.7767    +0.0256    +1.1424   0.8547   0.0236   0.1046  lattice        -566
KR_EQUITY       3    189     -0.0759    -0.3800    -0.3415    -0.3220    +0.1651   0.1688   0.7198   0.9962  lattice       -6987
PREMARKET       2    133     -0.1762    -0.3975    -2.6437    -0.5039    +0.1500   0.0351   0.8406   0.9998  lattice        -356

### h = SE
group        nsym  ndays  per-SYMBOL  per-PRINT per-DOLLAR      CI lo      CI hi   own HT    p raw   p maxT vs ownHT ceiling $/d
COMMODITY       8   1014     +0.4760    +0.1928    -0.9379    -1.0821    +2.1561   0.5793   0.2930   0.8110  lattice     -212100
EQUITY        130   4735     -1.0583    -1.0763    -1.6135    -1.8350    +0.2328   0.4013   0.8138   0.9998  lattice     -356384
HK_EQUITY       6     42     +0.1319    +1.6321    -4.1635    -5.1109    +4.2645   1.0197   0.4810   0.9428  lattice       -1333
KR_EQUITY       3    142     -0.9343    -1.1360    -1.8186    -2.8163    +0.6627   0.1688   0.8278   1.0000  lattice      -56680
PREMARKET       2     41     -2.4498    -4.4782    -3.6320    -5.9249    +0.4865   0.0351   0.9276   1.0000  lattice        -318

p raw is the per-group day-flip p as report.py executed it in PART I; p maxT is the frozen
PREREG §8 correction (studentize each group by its own null sd, max over the groups). The
frozen number is p maxT; p raw is printed only so PART I stays auditable against PART II.

## II.4 Every sub-class estimate against its OWN half-tick (k-law lesson)

PART I printed one half-tick — the class median 0.4013 bps — beside numbers belonging to
groups whose own lattice is coarser. This is the table that check demands.

group       ownHT med ownHT p90 ownHT max  HT pr-wt | per-SYMBOL by horizon (1/10/60/300/SE), '*' = above its own half-tick
COMMODITY      0.5793    1.0705    1.7123    0.0259 | +0.1625  -0.0851  -0.1602  -0.1492  +0.4760 
EQUITY         0.4013    2.3181    6.5920    0.1737 | +1.7747* +0.9613* +0.3165  +0.1395  -1.0583 
HK_EQUITY      0.8547    2.1509    2.5342    0.3188 | +1.1825* +0.7342  +0.5061  +0.5591  +0.1319 
KR_EQUITY      0.1688    0.2487    0.2687    0.0396 | +0.4581* +0.1409  +0.0439  -0.0759  -0.9343 
PREMARKET      0.0351    0.0386    0.0394    0.0308 | +0.6172* +0.2940* +0.0525* -0.1762  -2.4498 

(The printed ownHT columns are the h=60 membership of each group; the '*' marks use the
group's half-tick at THAT horizon, which differs only for HK_EQUITY at SE, where one of the
seven symbols has no session-end observation and the group median moves 0.8547 -> 1.0197.)

Read the 60 s column against the column before it: HK_EQUITY at 60 s is +0.5061 bps — the
largest sub-class estimate at that horizon, the one that survived max-T in PART I — and its
own median half-tick is 0.8547 bps. It is INSIDE its own lattice. So is EQUITY at 60 s
(+0.3165 vs 0.4013). COMMODITY is negative at 10/60/300 s and |estimate| < 0.5793 there too:
lattice on the negative side, which is not evidence of a negative cell either.

Census over the 25 sub-class x horizon cells:
  per-DOLLAR negative in 25 of 25
  per-PRINT  negative in 19 of 25
  per-SYMBOL positive in 17 of 25, and above its OWN half-tick in 7
  ceiling above the $28.75/day KILL floor in 0 of 25

## II.5 Corrected values (PREREG §6) — separate column, never folded into the estimate

b_cond = conditional bias of the tape-mid proxy, measured on the KR six against our own
recorded bookTicker on the prints the proxy keeps. b_total = b_cond plus the selection
effect of the 27-45% of prints the proxy drops. Neither is subtracted anywhere except in
the columns marked 'corrected'. h=SE has no measurement (our recording window does not
contain a session-end mark on the true book), so it is left blank rather than guessed.

h      per-SYMBOL    b_cond   b_total    corr(cond)   corr(total) |   per-PRINT    corr(cond)   corr(total) half-tick
1         +1.6193   +0.4674   +0.7037       +1.1519       +0.9156 |     -0.2109       -0.6783       -0.9146    0.4013
10        +0.8696   +0.0418   +0.2173       +0.8278       +0.6523 |     -0.3805       -0.4223       -0.5978    0.4013
60        +0.2910   -0.3691   -0.1585       +0.6601       +0.4495 |     -0.4281       -0.0590       -0.2696    0.4013
300       +0.1351   -0.0210   -0.7701       +0.1561       +0.9052 |     -0.4618       -0.4408       +0.3083    0.4013
SE        -0.9442       n/a       n/a           n/a           n/a |     -0.8671           n/a           n/a    0.4013

Sub-classes, corrected per-SYMBOL (conditional bias), with each group's own half-tick:
group                 h=1          h=10          h=60         h=300          h=SE
COMMODITY         -0.3049       -0.1269       +0.2089       -0.1282           n/a   (own HT 0.5793)
EQUITY            +1.3073       +0.9195       +0.6856       +0.1605           n/a   (own HT 0.4013)
HK_EQUITY         +0.7151       +0.6924       +0.8752       +0.5801           n/a   (own HT 0.8547)
KR_EQUITY         -0.0093       +0.0991       +0.4130       -0.0549           n/a   (own HT 0.1688)
PREMARKET         +0.1498       +0.2522       +0.4216       -0.1552           n/a   (own HT 0.0351)

The correction is not a rescue and not a kill: at 60 s it moves the class per-print reading
from -0.4281 to -0.0590 (still negative) and the class per-symbol reading from +0.2910 to +0.6601
(above the half-tick after correction, below it before) — which is exactly why PREREG §9(iv)
refuses a promotion whose size is smaller than the measured bias of its own instrument.

Flagging the one cell where the correction flips a lattice verdict, rather than leaving it
for a reader to find: HK_EQUITY at 60 s moves from +0.5061 (inside its own half-tick 0.8547) to
+0.8752 (outside it, by 0.0205 bps). Nothing rides on it: that sub-class still fails §9(iii)
replication (even-week p 0.0620) and §9(v) money (ceiling -144 $/day), and the correction it
rides on is a bias measured on six day-0 Korean symbols and TRANSFERRED to Hong Kong equities.
A promotion whose whole margin over the lattice is 0.02 bps of a transferred correction is not
a promotion; it is the correction's own uncertainty wearing a verdict.

## II.6 Replication across NON-OVERLAPPING ISO-week halves (PREREG §7)

h       odd weeks        p  even weeks        p same sign  odd(print) even(print) half-tick
1         +1.6325   0.0000     +1.6196   0.0000       yes     -0.2231     -0.1985    0.4013
10        +0.8915   0.0000     +0.8583   0.0000       yes     -0.3979     -0.3628    0.4013
60        +0.3093   0.0030     +0.2806   0.0122       yes     -0.4458     -0.4099    0.4013
300       +0.1801   0.1660     +0.0990   0.2638       yes     -0.5106     -0.4120    0.4013
SE        -0.8048   0.7810     -0.4535   0.7258       yes     -1.3167     -0.4209    0.4013

Sub-classes, same split (per-SYMBOL):
h=1
   COMMODITY   odd  +0.1634 (p 0.0000) | even  +0.1618 (p 0.0000) | same sign yes | both p<=.05 True  | own HT 0.5793
   EQUITY      odd  +1.7822 (p 0.0000) | even  +1.7760 (p 0.0000) | same sign yes | both p<=.05 True  | own HT 0.4013
   HK_EQUITY   odd  +1.2553 (p 0.0026) | even  +1.1802 (p 0.0022) | same sign yes | both p<=.05 True  | own HT 0.8547
   KR_EQUITY   odd  +0.4287 (p 0.0000) | even  +0.4904 (p 0.0000) | same sign yes | both p<=.05 True  | own HT 0.1688
   PREMARKET   odd  +0.7156 (p 0.0000) | even  +0.5166 (p 0.0040) | same sign yes | both p<=.05 True  | own HT 0.0351
h=10
   COMMODITY   odd  -0.0945 (p 1.0000) | even  -0.0755 (p 1.0000) | same sign yes | both p<=.05 False | own HT 0.5793
   EQUITY      odd  +0.9775 (p 0.0000) | even  +0.9534 (p 0.0000) | same sign yes | both p<=.05 True  | own HT 0.4013
   HK_EQUITY   odd  +0.9362 (p 0.0020) | even  +0.6101 (p 0.0096) | same sign yes | both p<=.05 True  | own HT 0.8547
   KR_EQUITY   odd  +0.0749 (p 0.1664) | even  +0.2135 (p 0.0072) | same sign yes | both p<=.05 False | own HT 0.1688
   PREMARKET   odd  +0.3395 (p 0.0048) | even  +0.2471 (p 0.1162) | same sign yes | both p<=.05 False | own HT 0.0351
h=60
   COMMODITY   odd  -0.1810 (p 1.0000) | even  -0.1390 (p 1.0000) | same sign yes | both p<=.05 False | own HT 0.5793
   EQUITY      odd  +0.3283 (p 0.0028) | even  +0.3074 (p 0.0110) | same sign yes | both p<=.05 True  | own HT 0.4013
   HK_EQUITY   odd  +0.7402 (p 0.0064) | even  +0.4553 (p 0.0620) | same sign yes | both p<=.05 False | own HT 0.8547
   KR_EQUITY   odd  +0.0567 (p 0.2852) | even  +0.0299 (p 0.3926) | same sign yes | both p<=.05 False | own HT 0.1688
   PREMARKET   odd  +0.1212 (p 0.1936) | even  -0.0164 (p 0.5270) | same sign NO  | both p<=.05 False | own HT 0.0351
h=300
   COMMODITY   odd  -0.1681 (p 1.0000) | even  -0.1298 (p 1.0000) | same sign yes | both p<=.05 False | own HT 0.5793
   EQUITY      odd  +0.1907 (p 0.1758) | even  +0.1023 (p 0.2916) | same sign yes | both p<=.05 False | own HT 0.4013
   HK_EQUITY   odd  +0.6642 (p 0.0712) | even  +0.4537 (p 0.1436) | same sign yes | both p<=.05 False | own HT 0.8547
   KR_EQUITY   odd  -0.0298 (p 0.5748) | even  -0.1267 (p 0.7320) | same sign yes | both p<=.05 False | own HT 0.1688
   PREMARKET   odd  -0.2515 (p 0.8632) | even  -0.0984 (p 0.6508) | same sign yes | both p<=.05 False | own HT 0.0351
h=SE
   COMMODITY   odd  +0.1672 (p 0.4532) | even  +0.8165 (p 0.2398) | same sign yes | both p<=.05 False | own HT 0.5793
   EQUITY      odd  -1.0053 (p 0.7898) | even  -0.2544 (p 0.6518) | same sign yes | both p<=.05 False | own HT 0.4013
   HK_EQUITY   odd  +2.3669 (p 0.0852) | even  -5.0497 (p 0.6998) | same sign NO  | both p<=.05 False | own HT 1.0197
   KR_EQUITY   odd  -1.4195 (p 0.8072) | even  -0.3712 (p 0.6374) | same sign yes | both p<=.05 False | own HT 0.1688
   PREMARKET   odd  -0.3562 (p 0.5322) | even  -4.2134 (p 0.9946) | same sign yes | both p<=.05 False | own HT 0.0351

## II.7 Per-week table, ALL five horizons (PREREG §7: every week is printed)

h=1    weeks 35 ; positive per-SYMBOL 30 ; positive per-PRINT 6
  per-SYMBOL: 202552:+2.967 202601:+0.793 202602:+0.242 202603:+0.065 202604:+0.089 202605:-0.147 202606:-0.160 202607:-0.202 202608:+0.053 202609:+0.289 202610:-0.021 202611:-0.037 202612:+0.001 202613:+0.117 202614:+0.286 202615:+0.345 202616:+0.285 202617:+0.405 202618:+0.292 202619:+0.288 202620:+0.153 202621:+0.292 202622:+0.242 202623:+0.349 202624:+0.467 202625:+0.888 202626:+1.432 202627:+1.120 202628:+1.329 202629:+1.325 202630:+1.284 202631:+1.497 202632:+1.492 202633:+1.406 202634:+1.268
  per-PRINT : 202552:+2.244 202601:+0.934 202602:+0.250 202603:+0.093 202604:+0.051 202605:-0.196 202606:-0.245 202607:-0.166 202608:-0.100 202609:+0.039 202610:-0.193 202611:-0.153 202612:-0.197 202613:-0.165 202614:-0.188 202615:-0.133 202616:-0.052 202617:-0.022 202618:-0.100 202619:-0.074 202620:-0.218 202621:-0.137 202622:-0.099 202623:-0.218 202624:-0.253 202625:-0.193 202626:-0.114 202627:-0.242 202628:-0.263 202629:-0.315 202630:-0.243 202631:-0.288 202632:-0.197 202633:-0.161 202634:-0.178
h=10   weeks 35 ; positive per-SYMBOL 16 ; positive per-PRINT 4
  per-SYMBOL: 202552:+2.889 202601:+0.644 202602:+0.134 202603:-0.003 202604:-0.026 202605:-0.204 202606:-0.206 202607:-0.362 202608:-0.324 202609:-0.085 202610:-0.426 202611:-0.421 202612:-0.330 202613:-0.236 202614:+0.006 202615:-0.018 202616:-0.070 202617:+0.070 202618:-0.058 202619:-0.005 202620:-0.121 202621:-0.133 202622:-0.144 202623:-0.104 202624:+0.073 202625:+0.389 202626:+0.747 202627:+0.478 202628:+0.638 202629:+0.577 202630:+0.632 202631:+0.708 202632:+0.750 202633:+0.821 202634:+0.664
  per-PRINT : 202552:+2.066 202601:+0.746 202602:+0.143 202603:+0.023 202604:-0.069 202605:-0.212 202606:-0.302 202607:-0.246 202608:-0.295 202609:-0.193 202610:-0.456 202611:-0.451 202612:-0.427 202613:-0.368 202614:-0.398 202615:-0.391 202616:-0.296 202617:-0.256 202618:-0.449 202619:-0.320 202620:-0.424 202621:-0.386 202622:-0.349 202623:-0.574 202624:-0.352 202625:-0.392 202626:-0.363 202627:-0.440 202628:-0.385 202629:-0.425 202630:-0.358 202631:-0.423 202632:-0.351 202633:-0.278 202634:-0.298
h=60   weeks 35 ; positive per-SYMBOL 12 ; positive per-PRINT 4
  per-SYMBOL: 202552:+2.404 202601:+0.364 202602:+0.145 202603:+0.046 202604:-0.011 202605:-0.214 202606:-0.174 202607:-0.351 202608:-0.491 202609:-0.214 202610:-0.559 202611:-0.530 202612:-0.246 202613:-0.355 202614:-0.103 202615:-0.254 202616:-0.289 202617:-0.072 202618:-0.234 202619:-0.101 202620:-0.238 202621:-0.182 202622:-0.270 202623:-0.266 202624:-0.252 202625:-0.067 202626:+0.113 202627:+0.271 202628:+0.187 202629:+0.056 202630:-0.015 202631:+0.164 202632:+0.369 202633:+0.242 202634:+0.194
  per-PRINT : 202552:+1.598 202601:+0.339 202602:+0.154 202603:+0.098 202604:-0.049 202605:-0.275 202606:-0.270 202607:-0.242 202608:-0.343 202609:-0.236 202610:-0.536 202611:-0.531 202612:-0.441 202613:-0.409 202614:-0.479 202615:-0.513 202616:-0.429 202617:-0.335 202618:-0.500 202619:-0.351 202620:-0.508 202621:-0.419 202622:-0.377 202623:-0.625 202624:-0.515 202625:-0.435 202626:-0.445 202627:-0.467 202628:-0.387 202629:-0.479 202630:-0.407 202631:-0.446 202632:-0.323 202633:-0.381 202634:-0.389
h=300  weeks 35 ; positive per-SYMBOL 11 ; positive per-PRINT 4
  per-SYMBOL: 202552:+2.574 202601:-0.096 202602:+0.222 202603:+0.137 202604:+0.054 202605:-0.019 202606:-0.007 202607:-0.215 202608:-0.495 202609:-0.119 202610:-0.547 202611:-0.419 202612:-0.155 202613:-0.406 202614:-0.032 202615:-0.363 202616:-0.369 202617:+0.013 202618:-0.276 202619:-0.184 202620:-0.189 202621:-0.205 202622:-0.417 202623:-0.367 202624:-0.123 202625:-0.158 202626:+0.048 202627:+0.010 202628:+0.063 202629:-0.397 202630:-0.096 202631:-0.011 202632:+0.043 202633:+0.414 202634:+0.182
  per-PRINT : 202552:+1.596 202601:-0.075 202602:+0.211 202603:+0.150 202604:-0.008 202605:+0.001 202606:-0.069 202607:-0.331 202608:-0.331 202609:-0.167 202610:-0.532 202611:-0.547 202612:-0.425 202613:-0.430 202614:-0.534 202615:-0.655 202616:-0.472 202617:-0.404 202618:-0.579 202619:-0.402 202620:-0.465 202621:-0.480 202622:-0.446 202623:-0.731 202624:-0.543 202625:-0.465 202626:-0.501 202627:-0.533 202628:-0.402 202629:-0.516 202630:-0.273 202631:-0.601 202632:-0.360 202633:-0.363 202634:-0.397
h=SE   weeks 34 ; positive per-SYMBOL 14 ; positive per-PRINT 13
  per-SYMBOL: 202601:-8.129 202602:+1.707 202603:+1.944 202604:-18.445 202605:+1.506 202606:+0.089 202607:+1.409 202608:-1.207 202609:-0.746 202610:+1.711 202611:-0.751 202612:+0.636 202613:-2.276 202614:+1.580 202615:-0.658 202616:-1.656 202617:+1.432 202618:-0.959 202619:-0.645 202620:+1.011 202621:-3.653 202622:-2.644 202623:+0.609 202624:+0.213 202625:-0.255 202626:-0.669 202627:+0.588 202628:+1.712 202629:-0.171 202630:-1.486 202631:-1.885 202632:-0.482 202633:-0.875 202634:-2.003
  per-PRINT : 202601:-7.743 202602:+0.544 202603:+2.449 202604:-17.002 202605:+4.908 202606:+2.592 202607:+1.954 202608:-2.099 202609:-2.866 202610:+2.675 202611:-0.710 202612:-0.996 202613:-2.212 202614:+0.462 202615:-1.108 202616:-5.675 202617:+0.602 202618:-0.624 202619:+1.268 202620:+0.150 202621:-0.704 202622:-2.135 202623:-1.002 202624:-0.306 202625:+0.551 202626:+0.186 202627:-1.977 202628:+0.658 202629:-1.922 202630:-0.842 202631:-2.394 202632:-0.223 202633:-0.958 202634:-4.244

The parity halves both come out positive at 1/10/60 s while only 12 of 35 individual weeks
are positive at 60 s. Both halves inherit the same symbol-equal-weight composition, so the
parity split is the weaker of the two replication reads and the per-week table is the
revealing one. On the per-PRINT statistic only 4 of 35 weeks are positive at 60 s.

## II.8 Instrument cross-check: our own recorded book vs the mirror archives, SAME UTC days

Pairing (our recordings in /home/storage/hft-data/binancefuturesum-d vs the mirror's
aggTrades for the identical symbol-days):
  HANMIUSDT            ours 6 days 20260813..20260818 | mirror 5 | PAIRED 5
  KODEX200USDT         ours 6 days 20260813..20260818 | mirror 5 | PAIRED 5
  LGELECTRONICSUSDT    ours 6 days 20260813..20260818 | mirror 5 | PAIRED 5
  NAVERUSDT            ours 6 days 20260813..20260818 | mirror 5 | PAIRED 5
  SAMSUNGEMUSDT        ours 6 days 20260813..20260818 | mirror 5 | PAIRED 5
  ZHONGJIUSDT          ours 6 days 20260813..20260818 | mirror 5 | PAIRED 5
  Our 2026-08-13 files are 5 KB stubs: the recorder was started before the venue listed these
  six (onboardDate 2026-08-14 02:00-02:25 UTC), so there is no archive to pair with and the
  pairing is complete at 6 symbols x 5 days = 30 symbol-days.

h        n prints RS true book RS tape proxy  bias b(h)      CI lo      CI hi     corr  drop proxy  drop true
1          233583      +3.8328      +4.3002    +0.4674    +0.2446    +0.7043   0.8019       0.272      0.000
10         231789      +2.5865      +2.6283    +0.0418    -0.0897    +0.1723   0.9239       0.274      0.000
60         225527      +1.6032      +1.2341    -0.3691    -0.5363    -0.2154   0.9713       0.300      0.000
300        191992      -0.9423      -0.9633    -0.0210    -0.1289    +0.0982   0.9910       0.448      0.053

Three things this table says, in order of how much they matter:

1. The proxy TRACKS: correlation with the true-book RS is 0.80 / 0.92 / 0.97 / 0.99 at
   1 / 10 / 60 / 300 s, and the bias is small against the spreads involved (mean true touch
   spread on these six is 21.003 bps).
2. The bias is SIGNED and horizon-dependent: +0.4674 at 1 s (CI +0.2446..+0.7043, does not cover 0),
   +0.0418 at 10 s (CI covers 0), -0.3691 at 60 s (CI -0.5363..-0.2154, does not cover 0), -0.0210 at 300 s.
   At 60 s |b| = 0.3691 against a class estimate of +0.2910: PREREG §9(iv) fails there, and it
   fails on a measurement, not on an argument.
3. On the TRUE book the realized spread of these six goes NEGATIVE by 300 s (-0.9423 bps)
   while capture at the touch is strongly positive (+6.5067 bps, min +1.7576, positive on all 30
   symbol-days — PREREG C4, the sign convention verified against a real book rather than
   asserted). Adverse selection eats a 6.5 bps touch capture inside five minutes on real
   quotes. That is the same shape the class shows through the proxy, measured on a different
   instrument and on symbols that are not in the mature population at all.

Attrition and the selection half of the bias (audit/kr_selection.py):
h          kept    selection   conditional      TOTAL
1         0.728      +0.2364       +0.4674    +0.7037
10        0.726      +0.1755       +0.0418    +0.2173
60        0.700      +0.2106       -0.3691    -0.1585
300       0.550      -0.7491       -0.0210    -0.7701

The proxy keeps 73% of prints at 1 s and 55% at 300 s. The prints it drops are not the
quiet ones (audit: same-minute print rate ratio 1.05, |move| ratio 0.96), but at 300 s the
selection term is -0.7491 bps — the single largest instrument term anywhere in this study, and
it is bigger than the class estimate it would correct (+0.1351). That is the honest reason the
300 s horizon is reported and not promoted.

## II.9 Sub-class promotion bar (PREREG §9, five conditions, evaluated mechanically)

Declared ambiguity, resolved by printing BOTH readings rather than by choosing: §9(ii) says
'the estimate exceeds the class median half-tick' and §9's sub-class clause says 'the same
five conditions'. Reading A uses each group's OWN median half-tick (stricter for the coarse
groups); reading B uses the literal class median 0.4013 bps. The verdict is the same under
both, so nothing rides on the choice.

h     group            i  iiOwn  iiCls    iii     iv      v  PROMO(A)  PROMO(B)
1     COMMODITY      yes     NO     NO    yes     NO     NO        no        no
1     EQUITY         yes    yes    yes    yes    yes     NO        no        no
1     HK_EQUITY      yes    yes    yes    yes    yes     NO        no        no
1     KR_EQUITY      yes    yes    yes    yes     NO     NO        no        no
1     PREMARKET      yes    yes    yes    yes    yes     NO        no        no
10    COMMODITY       NO     NO     NO     NO    yes     NO        no        no
10    EQUITY         yes    yes    yes    yes    yes     NO        no        no
10    HK_EQUITY      yes     NO    yes    yes    yes     NO        no        no
10    KR_EQUITY      yes     NO     NO     NO    yes     NO        no        no
10    PREMARKET      yes    yes     NO     NO    yes     NO        no        no
60    COMMODITY       NO     NO     NO     NO     NO     NO        no        no
60    EQUITY         yes     NO     NO    yes     NO     NO        no        no
60    HK_EQUITY      yes     NO    yes     NO    yes     NO        no        no
60    KR_EQUITY       NO     NO     NO     NO     NO     NO        no        no
60    PREMARKET       NO    yes     NO     NO     NO     NO        no        no
300   COMMODITY       NO     NO     NO     NO    yes     NO        no        no
300   EQUITY          NO     NO     NO     NO    yes     NO        no        no
300   HK_EQUITY       NO     NO    yes     NO    yes     NO        no        no
300   KR_EQUITY       NO     NO     NO     NO    yes     NO        no        no
300   PREMARKET       NO     NO     NO     NO    yes     NO        no        no
SE    COMMODITY       NO     NO    yes     NO    n/a     NO        no        no
SE    EQUITY          NO     NO     NO     NO    n/a     NO        no        no
SE    HK_EQUITY       NO     NO     NO     NO    n/a     NO        no        no
SE    KR_EQUITY       NO     NO     NO     NO    n/a     NO        no        no
SE    PREMARKET       NO     NO     NO     NO    n/a     NO        no        no

PROMOTED SUB-CLASSES: 0 of 25 cells under reading A, 0 of 25 under reading B.
Condition (v) — money — fails in 25 of 25 cells on its own: every sub-class ceiling is
negative, i.e. a maker who took 100% of every print in that sub-class would lose money.

## II.10 Money beside every sub-class (ceiling = 100% of every print; physically unreachable)

phi_queue = 0.377 (our $50 quote against the measured $82.7 median resting touch notional on
the KR six, transferred to the class as a DECLARED ASSUMPTION); phi_size = 0.034 (our $50
against the $1462.96 class mean print notional). KILL floor (#73) = $28.75/day.

h     group        ceiling $/day    x phi_queue     x phi_size    vs KILL
60    COMMODITY          -100569       -37891.4        -3437.2      BELOW
60    EQUITY             -109506       -41258.5        -3742.6      BELOW
60    HK_EQUITY             -144          -54.3           -4.9      BELOW
60    KR_EQUITY           -11368        -4283.3         -388.5      BELOW
60    PREMARKET             -261          -98.4           -8.9      BELOW
300   COMMODITY           -91063       -34309.7        -3112.3      BELOW
300   EQUITY              -96093       -36204.7        -3284.2      BELOW
300   HK_EQUITY             -566         -213.3          -19.4      BELOW
300   KR_EQUITY            -6987        -2632.4         -238.8      BELOW
300   PREMARKET             -356         -134.2          -12.2      BELOW

Class, for the same two horizons: h=60 ceiling -221849 $/day, h=300 ceiling -195065 $/day.

## II.11 What PART II changes and what it does not

Changed: nothing in the class headline — it reproduces to the last printed digit. What is
new is that the sub-class story printed in PART I now has its lattice reference and its
replication attached, and both remove it:

  * HK_EQUITY, the only sub-class still positive at 300 s under the RAW per-group p that
    PART I executed (p 0.0236), does not survive the frozen §8 max-T correction there
    (p 0.1046), is INSIDE its own half-tick at EVERY horizon (+0.5061 vs 0.8547 at 60 s;
    +0.5591 vs 0.8547 at 300 s), and fails the week-parity condition at 60 s (even-week
    p = 0.0620). Its whole ceiling is $-144/day at 60 s.
  * EQUITY at 60 s (+0.3165) is inside the class half-tick 0.4013 as well; it is the only
    group that passes replication at 60 s, and it dies on the half-tick and on money.
  * COMMODITY (the metals) is NEGATIVE per-symbol at 10/60/300 s and negative on all three
    weightings at 10/60/300 s.
  * KR_EQUITY — the sub-class the +1.19 bps day-0 prior came from — is +0.0439 bps at 60 s
    with p(maxT) = 0.7584 and a CI that covers zero. The prior does not survive into the mature
    regime of its own sub-class.
  * per-DOLLAR is negative in 25 of 25 sub-class x horizon cells — there is no group anywhere
    in the frozen partition where the money-weighted population gross is positive.

MANDATORY (Lighter lesson, #102): every number above is a POPULATION POOL and therefore an
UPPER BOUND on our capture, never an expectation. The Binance tape does not decompose by
account, so who-captures cannot be split. On Lighter a positive population pool coexisted
with a zero-fee cohort losing -2.855 bps. Here the pool itself is negative on the two
weightings that describe money, so the caveat is not even load-bearing for the class
verdict — it binds only on the per-symbol residual.

