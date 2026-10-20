"""Independent replay of all main/control filled paths, no engine imports."""
from pathlib import Path
import json,math
import numpy as np
ROOT=Path(__file__).resolve().parent
NS=10**9
params=json.loads((ROOT/'data/params.json').read_text())
verified=0;totals={4:dict(n=0,net=0.,gross=0.),9:dict(n=0,net=0.,gross=0.)}
max_atr_error=0.;capacity=0
for path in sorted((ROOT/'out').glob('*.npz')):
    with np.load(path) as z:r=z['trades'];day=int(z['day_ns'])
    rows=r[np.isin(r[:,0],[4,9])]
    if not len(rows):continue
    with np.load(ROOT/'cache'/path.name) as z:e=z['events']
    b=e[(e['kind']==1)&(e['p']>0)&(e['a']>=e['p'])&(e['local']>=day)&(e['local']<day+86400*NS)]
    t=e[e['kind']==2]
    bt=b['local']-day;tt=t['local']-day
    mid=(b['p']+b['a'])/2
    cfg=params[path.stem];tick=cfg['tick'];lot=cfg['lot']
    last_exit={1:-1,-1:-1};last_variant=-1
    for row in rows:
        v,side=int(row[0]),int(row[1]);d,fi,ex=map(int,row[2:5]);entry,qty=row[5:7]
        if v!=last_variant:last_exit={1:-1,-1:-1};last_variant=v
        assert fi>last_exit[side];last_exit[side]=ex
        before=np.searchsorted(bt,d)-1;ref=mid[before]
        if v==4:
            end=d//(300*NS);ranges=[];prev=None
            for bar in range(end-15,end):
                lo=np.searchsorted(bt,bar*300*NS);hi=np.searchsorted(bt,(bar+1)*300*NS)
                assert hi>lo
                high=mid[lo:hi].max();low=mid[lo:hi].min();close=mid[hi-1]
                if prev is not None:ranges.append(max(high-low,abs(high-prev),abs(low-prev)))
                prev=close
            atr=sum(ranges)/14
            max_atr_error=max(max_atr_error,abs(atr-row[18]))
            assert abs(atr-row[18])<=max(1e-12,abs(atr)*1e-10)
            distance=max(2*atr/ref,5*tick/ref)
        else:distance=.01
        level=ref*(1-side*distance)/tick
        expected=(math.floor(level+1e-10) if side==1 else math.ceil(level-1e-10))*tick
        assert abs(expected-entry)<tick*1e-6
        q=math.floor(min(2000/30/entry,.1*min(b['q'][before],b['aq'][before]),cfg['maxqty'])/lot+1e-10)*lot
        assert abs(q-qty)<lot*1e-6
        active=d+NS;cancel=active+60*NS
        ab=np.searchsorted(bt,active,side='right')-1
        assert (entry<b['a'][ab] if side==1 else entry>b['p'][ab])
        lo=np.searchsorted(tt,active);hi=np.searchsorted(tt,cancel);sub=t[lo:hi]
        valid=(sub['exchange']>=day+active)&(sub['side']==-side)&(sub['q']+1e-10>=qty)&((entry-sub['p'])*side+tick*1e-7>=tick)
        ix=np.flatnonzero(valid);assert len(ix) and tt[lo+ix[0]]==fi
        start=max(0,np.searchsorted(bt,fi,side='right')-1)
        finish=np.searchsorted(bt,fi+900*NS,side='right')
        px=b['p'][start:finish] if side==1 else b['a'][start:finish]
        ret=side*(px/entry-1)
        fresh=np.maximum(bt[start:finish],fi)-bt[start:finish]<=2*NS
        hit=np.flatnonzero(((ret<=-.01)|(ret>=.01))&fresh)
        if len(hit):
            j=int(hit[0]);trigger=max(int(bt[start+j]),fi);reason=2 if ret[j]<=-.01 else 1
        else:trigger=fi+900*NS;reason=3
        ei=np.searchsorted(bt,trigger+NS)
        while b['exchange'][ei]<day+trigger+NS:ei+=1
        assert bt[ei]==ex and reason==int(row[10])
        raw=b['p'][ei] if side==1 else b['a'][ei]
        actual=raw-side*tick
        gross=qty*side*(raw-entry)
        net=qty*side*(actual-entry)-qty*entry*.0002-qty*actual*.0005
        assert abs(net-row[12])<1e-9 and abs(gross-row[11])<1e-9
        totals[v]['n']+=1;totals[v]['net']+=net;totals[v]['gross']+=gross
        verified+=1
summary=json.loads((ROOT/'out/summary.json').read_text())
for v,r in totals.items():
    s=summary['configurations'][v]
    assert r['n']==s['trades'] and abs(r['net']-s['net_usd'])<1e-7
    r['net_usd_day']=r['net']/29;r['gross_usd_day']=r['gross']/29
result=dict(verified_main_and_control_fills=verified,max_atr_error=max_atr_error,totals=totals,separate_agent_review=False)
(ROOT/'out/audit.json').write_text(json.dumps(result,indent=2))
print(json.dumps(result,indent=2))
