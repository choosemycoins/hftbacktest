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
