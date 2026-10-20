"""Independent Python JSON reading against the Rust-produced binary/NPZ data."""
from pathlib import Path
import gzip,json
import numpy as np
ROOT=Path(__file__).resolve().parent
RAW=Path('/home/storage/hft-data/binancefuturesum-c')
cases=['xaiusdt_20260812','aceusdt_20260820','merlusdt_20260909']
out=[]
for name in cases:
    counts={1:0,2:0,3:0};samples=[]
    with gzip.open(RAW/(name+'.gz'),'rb') as f:
        for line in f:
            kind=(1 if b'"e":"bookTicker"'in line else 2 if b'"e":"trade"'in line else 3 if b'"nextFundingTime"'in line else 0)
            if not kind or counts[kind]>=100:continue
            stamp,s=line.split(b' ',1);j=json.loads(s);d=j.get('data',j)
            if kind==1:r=(int(stamp),int(d['T'])*1000000,1,int(d['u']),0,float(d['b']),float(d['B']),float(d['a']),float(d['A']))
            elif kind==2:
                if d['X']!='MARKET':continue
                r=(int(stamp),int(d['T'])*1000000,2,int(d['t']),-1 if d['m'] else 1,float(d['p']),float(d['q']),0.,0.)
            else:r=(int(stamp),int(d['time'])*1000000,3,int(d['nextFundingTime'])*1000000,0,float(d['markPrice']),float(d['lastFundingRate']),0.,0.)
            samples.append(r);counts[kind]+=1
            if min(counts.values())>=100:break
    with np.load(ROOT/'cache'/(name+'.npz')) as z:ev=z['events']
    for r in samples:
        ix=np.searchsorted(ev['local'],r[0]);matched=False
        while ix<len(ev) and ev['local'][ix]==r[0]:
            rr=ev[ix]
            if rr['kind']==r[2] and rr['id']==r[3]:
                assert tuple(rr)==r,(r,tuple(rr));matched=True;break
            ix+=1
        assert matched,r
    assert min(counts.values())>=100,counts
    out.append(dict(file=name,matched=len(samples),per_kind=counts))
(ROOT/'out/parser_validation.json').write_text(json.dumps(out,indent=2))
print(json.dumps(out,indent=2))
