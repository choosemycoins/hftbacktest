# VERDICT — PREREG §9 five conditions, evaluated mechanically
PREREG sha256 0edd00e031b391c09968198afb9fa1aa4eec6fa236abc2eddd12f76f04aff3b3
COHERENCE sha256 0ae704a0fd9c6ff3454479771b735751e4b818db46fa1a375a935e6aea4687dc

h           RS        p      odd     even  halftick    |bias|    nullsd $/day ceil  VERDICT
1      +1.6193   0.0000  +1.6325  +1.6196    0.4013   0.4674   0.2359 -185048.26  NO (v:money)
10     +0.8696   0.0000  +0.8915  +0.8583    0.4013   0.0418   0.1447 -223592.87  NO (v:money)
60     +0.2910   0.0000  +0.3093  +0.2806    0.4013   0.3691   0.0875 -221848.75  NO (ii:halftick,iv:bias,v:money)
300    +0.1351   0.1350  +0.1801  +0.0990    0.4013   0.0210   0.1201 -195064.72  NO (i:sign/p,ii:halftick,iii:weeks,v:money)
SE     -0.9442   0.8326  -0.8048  -0.4535    0.4013      nan   0.8995 -626814.80  NO (i:sign/p,iii:weeks,iv:bias,v:money)

## falsifiers (PREREG §12)
K1 class RS<=0 at every horizon, or |RS|<=half-tick : not fired
K2 positive at 1s only, <=0 by 300s              : not fired
K3 ISO-week halves disagree in sign              : not fired
K8 money ceiling (100% of tape) < $28.75/day      : FIRED at 1,10,60,300,SE

CLASS PROMOTION: NO at every horizon
