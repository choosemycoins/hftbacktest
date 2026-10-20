"""Separate vectorized replay/accounting audit; does not import screen.py."""
from pathlib import Path
import json
import numpy as np

ROOT=Path(__file__).resolve().parent
CACHE=Path('/home/storage/hft-scratch/spot1s_bench/cache')
totals={0:[0,0.,0.,0.],1:[0,0.,0.,0.]}
verified=0
start=1597190400 # 2020-08-12T00:00Z
for p in sorted((ROOT/'out').glob('*.npz')):
    with np.load(p) as saved:
        rows=saved['trades'];sec0=int(saved['sec0'])
    rows=rows[rows[:,2]==1]
    with np.load(CACHE/p.name) as z:
        c=z['c'].astype(float);h=z['h'].astype(float);l=z['l'].astype(float)
    for row in rows:
        rule,side=map(int,row[:2]);d=int(row[3])-sec0;fi=int(row[4])-sec0
        hold=900 if rule==0 else 1800
        if rule==0:
            entry=c[d-1]*(.99 if side==1 else 1.01)
            tp=entry*(1+side*.01);sl=entry*(1-side*.01)
        else:
            low=l[d-1800:d].min();high=h[d-1800:d].max();width=high-low
            assert .01<=width/c[d-1]<=.07
            assert abs(c[d-1]-c[d-1800])/width<=.25
            entry=low+.1*width if side==1 else high-.1*width
            tp=(low+high)/2;sl=entry-side*.5*width
        a=d+1
        possible=np.flatnonzero(l[a:a+60]<entry*.9999 if side==1 else h[a:a+60]>entry*1.0001)
        assert len(possible) and fi==a+possible[0]
        stop=np.flatnonzero(l[fi:fi+hold+1]<=sl if side==1 else h[fi:fi+hold+1]>=sl)
        take=np.flatnonzero(h[fi+1:fi+hold+1]>tp*1.0001 if side==1 else l[fi+1:fi+hold+1]<tp*.9999)+1
        sj=int(stop[0]) if len(stop) else hold+1
        tj=int(take[0]) if len(take) else hold+1
        if sj<=hold and sj<=tj:
            reason=2;ex=fi+sj+1;px=c[ex]
        elif tj<=hold:
            reason=1;ex=fi+tj;px=tp
        else:
            reason=3;ex=fi+hold+1;px=c[ex]
        assert ex+sec0==int(row[5]) and reason==int(row[14])
        assert np.isclose(px,row[7],rtol=1e-12) and np.isclose(entry,row[6],rtol=1e-12)
        actual=px if reason==1 else px*(1-side*.0001)
        # Compute cash P&L by quantity and explicit entry and exit fee cashflows.
        qty=1/entry
        net=(side*qty*(actual-entry)-.001*qty*entry-.001*qty*actual)*1e4
        gross=side*qty*(px-entry)*1e4
        markpx=c[fi+hold]*(1-side*.0001)
        marknet=(side*qty*(markpx-entry)-.001*qty*entry-.001*qty*markpx)*1e4
        assert abs(net-row[9])<1e-8 and abs(gross-row[8])<1e-8 and abs(marknet-row[12])<1e-8
        assert gross>=net
        verified+=1
        if side==1 and row[4]>=start:
            totals[rule][0]+=1;totals[rule][1]+=net;totals[rule][2]+=gross;totals[rule][3]+=marknet
summary=json.loads((ROOT/'out'/'summary.json').read_text())
checks=[]
for s in summary['portfolios']:
    if s['variant']!=1:continue
    n,net,gross,mark=totals[s['rule']]
    dollars=net*.1/1.001/2180
    assert n==s['trades'] and abs(net/n-s['net_bp'])<1e-9
    assert abs(dollars-s['net_usd_day'])<1e-9
    checks.append(dict(rule=s['rule'],trades=n,net_bp=net/n,net_usd_day=dollars,gross_bp=gross/n,mark_net_bp=mark/n))
result=dict(main_variant_fills_independently_replayed=verified,portfolio_checks=checks,
            separate_human_or_agent_review=False)
(ROOT/'out'/'audit.json').write_text(json.dumps(result,indent=2))
print(json.dumps(result,indent=2))
