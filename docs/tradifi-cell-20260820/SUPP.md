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
