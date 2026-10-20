"""Frozen causal price-path screen; deliberately NOT a book/queue simulator."""
import os
for _v in ('OMP_NUM_THREADS','OPENBLAS_NUM_THREADS','MKL_NUM_THREADS','NUMBA_NUM_THREADS'):
    os.environ[_v]='1'
import argparse
import concurrent.futures as cf
import hashlib
import json
from pathlib import Path
import sys
import numpy as np
import pandas as pd
from numba import njit

ROOT=Path(__file__).resolve().parent
CACHE=Path('/home/storage/hft-scratch/spot1s_bench/cache')
SCENARIOS=((0,.0001),(1,.0001),(2,.0001),(5,.0001),(30,.0001),(1,0.))
COLUMNS=['rule','side','variant','decision','fill','exit','entry','exit_price',
         'gross_bp','net_bp','bnb_bp','mark_gross_bp','mark_net_bp','past_bp',
         'reason','ambiguous']


@njit(cache=True)
def evaluate_order(c,h,l,side,limit,take,stop,active,ttl,hold,lag,eps):
    fi=-1
    for j in range(active,active+ttl):
        if (side==1 and l[j]<limit*(1-eps)) or (side==-1 and h[j]>limit*(1+eps)):
            fi=j
            break
    if fi<0:return (-1,-1,0,0.,0.,0)
    for j in range(fi,fi+hold+1):
        sl=l[j]<=stop if side==1 else h[j]>=stop
        tp=(j>fi) and (h[j]>take*(1+eps) if side==1 else l[j]<take*(1-eps))
        if sl:
            ex=j+max(1,lag)
            return(fi,ex,2,float(c[ex]),float(c[fi+hold]),int(tp))
        if tp:return(fi,j,1,take,float(c[fi+hold]),0)
    ex=fi+hold+max(1,lag)
    return(fi,ex,3,float(c[ex]),float(c[fi+hold]),0)


@njit(cache=True)
def returns_bps(side,entry,exit_price,reason,fee,slip):
    gross=side*(exit_price/entry-1)*1e4
    actual=exit_price if reason==1 else exit_price*(1-side*slip)
    net=(side*(actual/entry-1)-fee*(1+actual/entry))*1e4
    return gross,net


@njit(cache=True)
def levels(c,h,l,d,rule,side):
    p=float(c[d-1])
    if rule==0:
        limit=p*(1-side*.01)
        return(limit,limit*(1+side*.01),limit*(1-side*.01),900)
    low=float(np.min(l[d-1800:d])); high=float(np.max(h[d-1800:d]))
    return range_levels(p,float(c[d-1800]),low,high,side)


@njit(cache=True)
def range_levels(p,past,low,high,side):
    width=high-low
    if width<=0 or width/p<.01 or width/p>.07 or abs(p-past)/width>.25:
        return(0.,0.,0.,1800)
    limit=low+.1*width if side==1 else high-.1*width
    if side*(p-limit)<=0:return(0.,0.,0.,1800)
    return(limit,(low+high)/2,limit-side*.5*width,1800)


@njit(cache=True)
def envelope_complete(bad_prefix,start,end):
    return bad_prefix[end+1]==bad_prefix[start]


@njit(cache=True)
def next_decision(decision,exit_time,mark_time):
    return (max(decision,exit_time,mark_time)//60+1)*60


@njit(cache=True)
def replay(c,h,l,sec0,decisions,params,bad,rule,side,variant,lag,eps):
    out=np.empty((len(decisions),16),np.float64)
    nr=0;busy=0;gaps=0;eligible=0;purged=0
    split=1690848000 # 2023-08-01 UTC
    for k in range(len(decisions)):
        d=decisions[k];stamp=sec0+d
        if stamp<busy:continue
        limit,take,stop= params[k,0],params[k,1],params[k,2]
        hold=int(params[k,3])
        if limit==0:continue
        end=d+lag+60+hold+max(1,lag)
        # Require the full envelope in this UTC day and source cell.
        if d<1800 or end>=len(c) or stamp%86400<1800 or (sec0+end)//86400!=stamp//86400:
            continue
        if abs(stamp-split)<=3630:
            purged+=1;continue
        if not envelope_complete(bad,d-1800,end):
            gaps+=1;continue
        eligible+=1
        r=evaluate_order(c,h,l,side,limit,take,stop,d+lag,60,hold,lag,eps)
        fi,ex,reason,exit_price,mark,amb=r
        # An unfilled order also occupies the sleeve until cancellation.
        busy=((stamp+lag+60-1)//60+1)*60
        if fi<0:continue
        gross,net=returns_bps(side,limit,exit_price,reason,.001,.0001)
        _,bnb=returns_bps(side,limit,exit_price,reason,.00075,.0001)
        mg,mn=returns_bps(side,limit,mark,3,.001,.0001)
        past=side*(float(c[fi])/float(c[fi-hold])-1)*1e4
        out[nr]=np.array([rule,side,variant,stamp,sec0+fi,sec0+ex,limit,exit_price,
                          gross,net,bnb,mg,mn,past,reason,amb])
        nr+=1
        busy=next_decision(stamp,sec0+ex,sec0+fi+hold)
    return out[:nr],eligible,gaps,purged


def worker(pathstr):
    path=Path(pathstr)
    with np.load(path) as z:
        c=z['c'];h=z['h'];l=z['l'];sec0=int(z['sec0']);n=int(z['n'])
        assert n==len(c) and bool(z['mono']) and int(z['ndup'])==0
        assert np.all(np.isfinite(c)) and np.all(l>0) and np.all(h>=c) and np.all(l<=c)
        missing=np.zeros(n,dtype=np.int8)
        for s,ln in zip(z['gap_s'],z['gap_n']):missing[int(s):int(s+ln)]=1
        bad=np.r_[0,np.cumsum(missing,dtype=np.int64)]
        gap_seconds=int(missing.sum())
    decisions=np.arange((-sec0)%60+1800,n-1892,60,dtype=np.int64)
    # Exactly [d-1800,d): endpoints read at d-1, never at d.
    lows=pd.Series(l).rolling(1800,min_periods=1800).min().to_numpy()[decisions-1]
    highs=pd.Series(h).rolling(1800,min_periods=1800).max().to_numpy()[decisions-1]
    parts=[];counts=[]
    for rule in (0,1):
        for side in (1,-1):
            params=np.empty((len(decisions),4))
            for k,d in enumerate(decisions):
                if rule==0:params[k]=levels(c,h,l,d,0,side)
                else:params[k]=range_levels(float(c[d-1]),float(c[d-1800]),lows[k],highs[k],side)
            for variant,(lag,eps) in enumerate(SCENARIOS):
                rows,eligible,gaps,purged=replay(c,h,l,sec0,decisions,params,bad,rule,side,variant,lag,eps)
                parts.append(rows)
                counts.append(dict(rule=rule,side=side,variant=variant,eligible=eligible,gaps=gaps,purged=purged,fills=len(rows)))
    rows=np.concatenate(parts)
    np.savez_compressed(ROOT/'out'/(path.stem+'.npz'),trades=rows,sec0=sec0,n=n)
    digest=hashlib.file_digest(path.open('rb'),'sha256').hexdigest()
    return dict(file=str(path),sha256=digest,rows=n,sec0=sec0,gap_seconds=gap_seconds,counts=counts)


def main():
    ap=argparse.ArgumentParser();ap.add_argument('--workers',type=int,default=12)
    ap.add_argument('--one');args=ap.parse_args()
    (ROOT/'out').mkdir(exist_ok=True);(ROOT/'data').mkdir(exist_ok=True)
    files=sorted(CACHE.glob('*.npz'))
    if args.one: files=[p for p in files if p.stem==args.one]
    metadata=[]
    with cf.ProcessPoolExecutor(max_workers=args.workers) as pool:
        futs={pool.submit(worker,str(p)):p for p in files}
        for i,f in enumerate(cf.as_completed(futs),1):
            r=f.result();metadata.append(r)
            if i%12==0 or i==len(files):print(f'{i}/{len(files)} months complete',flush=True)
    (ROOT/'out'/'manifest.json').write_text(json.dumps(sorted(metadata,key=lambda x:x['file']),indent=2))
    (ROOT/'data'/'pins.tsv').write_text('sha256\tpath\n'+''.join(r['sha256']+'\t'+r['file']+'\n' for r in sorted(metadata,key=lambda x:x['file'])))


if __name__=='__main__':main()
