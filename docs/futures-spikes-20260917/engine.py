import os
for k in ['OMP_NUM_THREADS','OPENBLAS_NUM_THREADS','MKL_NUM_THREADS','NUMBA_NUM_THREADS']:os.environ[k]='1'
from pathlib import Path
import json,datetime as dt,concurrent.futures as cf
import numpy as np
from numba import njit

ROOT=Path(__file__).resolve().parent
NS=10**9
# Main index 4: frame 5min x multiplier2. Fixed control index9.
VARIANTS=[(w,k,1.) for w in [1,5,15] for k in [1.,2.,3.]]+[(0,0.,1.),(5,2.,.1),(0,0.,.1),(5,2.,5.),(0,0.,5.)]
COLS=['variant','side','decision','fill','exit','entry','qty','exit_price','raw_exit','exit_mid','reason','gross','net','stress','zero_mid','mark_net','past_bp','distance_bp','atr','exit_qty','capacity_ok','funding_cross','lag','feed_delay_ms']

def atr_features(t,p,gap):
    out=np.full((1440,3),np.nan)
    for col,w in enumerate([1,5,15]):
        width=w*60*NS;n=1440//w;idx=t//width
        high=np.full(n,-np.inf);low=np.full(n,np.inf)
        np.maximum.at(high,idx,p);np.minimum.at(low,idx,p)
        ends=np.arange(1,n+1,dtype=np.int64)*width
        last=np.searchsorted(t,ends,side='left')-1
        close=np.where(last>=0,p[np.maximum(last,0)],np.nan)
        bad=(np.bincount(idx,minlength=n)==0)
        gi=np.flatnonzero(gap)
        bad[idx[gi]]=True
        bad[idx[np.maximum(gi-1,0)]]=True
        tr=np.full(n,np.nan)
        tr[1:]=np.maximum(high[1:]-low[1:],np.maximum(abs(high[1:]-close[:-1]),abs(low[1:]-close[:-1])))
        tr[bad|np.r_[True,bad[:-1]]]=np.nan
        for minute in range(1440):
            end=minute//w
            if end>=15:
                window=tr[end-14:end]
                if np.all(np.isfinite(window)):out[minute,col]=window.mean()
    return out

@njit(cache=True)
def quote_level(mid,distance,side,tick):
    raw=mid*(1-side*distance)/tick
    return (np.floor(raw+1e-10) if side==1 else np.ceil(raw-1e-10))*tick

@njit(cache=True)
def order_size(price,bq,aq,lot,minnotional,minqty,maxqty):
    q=np.floor(min(2000/30/price,.1*min(bq,aq),maxqty)/lot+1e-10)*lot
    if q<minqty or q*price<minnotional:return 0.
    return q

@njit(cache=True)
def first_fill(t,exchange,p,q,side,active,cancel,direction,limit,qty,tick):
    start=np.searchsorted(t,active);end=np.searchsorted(t,cancel)
    for i in range(start,end):
        if exchange[i]<active or side[i]!=-direction or q[i]+1e-10<qty:continue
        through=(limit-p[i])*direction
        if through+tick*1e-7>=tick:return i
    return -1

@njit(cache=True)
def exit_order(t,exchange,bid,ask,fill,side,entry,hold,lag):
    end=fill+hold;reason=3;trigger=end
    start=max(0,np.searchsorted(t,fill,side='right')-1)
    for i in range(start,len(t)):
        now=max(t[i],fill)
        if now>end:break
        price=bid[i] if side==1 else ask[i]
        if price<=0 or now-t[i]>2*NS:continue
        r=side*(price/entry-1)
        if r<=-.01:reason=2;trigger=now;break
        if r>=.01:reason=1;trigger=now;break
    ex=np.searchsorted(t,trigger+lag)
    while ex<len(t) and (exchange[ex]<trigger+lag or bid[ex]<=0 or ask[ex]<bid[ex]):ex+=1
    return ex,reason

@njit(cache=True)
def cashflows(side,entry,raw_exit,exit_mid,qty,tick):
    actual=raw_exit-side*tick
    gross=side*qty*(raw_exit-entry)
    net=side*qty*(actual-entry)-qty*(entry*.0002+actual*.0005)
    stress=side*qty*(actual-entry)-qty*(entry*.0003+actual*.0006)
    return gross,net,stress,side*qty*(exit_mid-entry)

@njit(cache=True)
def funding_safe(decision,observed,nextfund,lag):
    return observed>=0 and decision-observed<=1800*NS and nextfund>decision+(60+900+60)*NS+2*lag

@njit(cache=True)
def simulate(bt,be,bp,bq,ap,aq,tt,te,tp,tq,ts,ft,fn,features,gap_prefix,tick,lot,minq,minnot,maxq,frame,k,lag,variant,side,day_ns):
    out=np.empty((1440,24),np.float64);n=0;busy=0
    # gates: attempts, stale, feature, funding, gap, sizing, postonly, nofill
    counts=np.zeros(8,np.int64)
    for minute in range(225,1420):
        d=minute*60*NS
        if d<busy:continue
        counts[0]+=1
        if abs(day_ns+d-1787788800*NS)<=14470*NS:continue # 2026-08-27 UTC
        bi=np.searchsorted(bt,d)-1
        if bi<0 or d-bt[bi]>2*NS or bp[bi]<=0 or ap[bi]<bp[bi]:counts[1]+=1;continue
        if not np.all(np.isfinite(features[minute])):counts[2]+=1;continue
        fi=np.searchsorted(ft,d)-1
        if fi<0 or not funding_safe(d,ft[fi],fn[fi],lag):counts[3]+=1;continue
        horizon=d+(60+900)*NS+2*lag+10*NS
        stop=np.searchsorted(bt,horizon)
        if stop>=len(bt) or gap_prefix[stop+1]-gap_prefix[bi+1]>0:counts[4]+=1;continue
        mid=(bp[bi]+ap[bi])/2
        atr=features[minute,0 if frame==1 else 1 if frame==5 else 2]
        distance=.01 if frame==0 else max(k*atr/mid,5*tick/mid)
        if distance>.05 or distance<=0:counts[2]+=1;continue
        limit=quote_level(mid,distance,side,tick)
        qty=order_size(limit,bq[bi],aq[bi],lot,minnot,minq,maxq)
        if qty<=0:counts[5]+=1;continue
        active=d+lag;cancel=active+60*NS
        abi=np.searchsorted(bt,active,side='right')-1
        busy=(cancel//(60*NS)+1)*60*NS
        if active-bt[abi]>2*NS or (side==1 and limit>=ap[abi]) or (side==-1 and limit<=bp[abi]):counts[6]+=1;continue
        ti=first_fill(tt,te,tp,tq,ts,active,cancel,side,limit,qty,tick)
        if ti<0:counts[7]+=1;continue
        fill=tt[ti]
        ex,reason=exit_order(bt,be,bp,ap,fill,side,limit,900*NS,lag)
        if ex>=len(bt):continue
        raw=bp[ex] if side==1 else ap[ex];touchqty=bq[ex] if side==1 else aq[ex]
        exitmid=(bp[ex]+ap[ex])/2
        gross,net,stress,zero=cashflows(side,limit,raw,exitmid,qty,tick)
        mark=np.searchsorted(bt,fill+900*NS)
        markraw=bp[mark] if side==1 else ap[mark]
        _,marknet,_,_=cashflows(side,limit,markraw,(bp[mark]+ap[mark])/2,qty,tick)
        pre=np.searchsorted(bt,fill-900*NS,side='right')-1
        at=np.searchsorted(bt,fill,side='right')-1
        past=side*((bp[at]+ap[at])/(bp[pre]+ap[pre])-1)*1e4
        out[n]=np.array([variant,side,d,fill,bt[ex],limit,qty,raw-side*tick,raw,exitmid,reason,gross,net,stress,zero,marknet,past,distance*1e4,atr,touchqty,int(qty<=touchqty),int(bt[ex]>=fn[fi]),lag/NS,(tt[ti]-te[ti])/1e6])
        n+=1;busy=(bt[ex]//(60*NS)+1)*60*NS
    return out[:n],counts

def worker(pathstr):
    path=Path(pathstr);cfg=json.loads((ROOT/'data/params.json').read_text())[path.stem]
    day=path.stem.rsplit('_',1)[1]
    day_ns=int(dt.datetime.strptime(day,'%Y%m%d').replace(tzinfo=dt.timezone.utc).timestamp())*NS
    with np.load(path) as z:ev=z['events']
    b=ev[ev['kind']==1];t=ev[ev['kind']==2];f=ev[ev['kind']==3]
    b=b[(b['p']>0)&(b['a']>=b['p'])&(b['local']>=day_ns)&(b['local']<day_ns+86400*NS)]
    bt=np.ascontiguousarray(b['local']-day_ns);be=np.ascontiguousarray(b['exchange']-day_ns)
    gap=np.r_[False,np.diff(bt)>10*NS]
    feat=atr_features(bt,(b['p']+b['a'])/2,gap)
    args=[bt,be]+[np.ascontiguousarray(b[x]) for x in ['p','q','a','aq']]
    args+=[np.ascontiguousarray(t['local']-day_ns),np.ascontiguousarray(t['exchange']-day_ns)]+[np.ascontiguousarray(t[x]) for x in ['p','q','side']]
    args+=[np.ascontiguousarray(f['local']-day_ns),np.ascontiguousarray(f['id']-day_ns),feat,np.r_[0,np.cumsum(gap)]]
    args+=[cfg[x] for x in ['tick','lot','minqty','minnotional','maxqty']]
    parts=[];gates=[]
    for v,(w,k,lag) in enumerate(VARIANTS):
        for side in (1,-1):
            rows,count=simulate(*args,w,k,int(lag*NS),v,side,day_ns)
            parts.append(rows);gates.append(dict(variant=v,side=side,counts=count.tolist()))
    trades=np.concatenate(parts)
    np.savez_compressed(ROOT/'out'/(path.stem+'.npz'),trades=trades,day_ns=np.int64(day_ns))
    return dict(file=path.name,n_book=len(b),n_trade=len(t),n_premium=len(f),gaps=int(gap.sum()),gates=gates,trades=len(trades))

if __name__=='__main__':
    paths=sorted((ROOT/'cache').glob('*.npz'))
    assert len(paths)==435,len(paths)
    changed=set(json.loads((ROOT/'data/params_changed.json').read_text()))
    paths=[p for p in paths if p.stem not in changed]
    manifest=[]
    with cf.ProcessPoolExecutor(max_workers=12) as pool:
        futs=[pool.submit(worker,str(p)) for p in paths]
        for i,f in enumerate(cf.as_completed(futs),1):
            manifest.append(f.result())
            if i%30==0 or i==len(paths):print(f'{i}/{len(paths)} futures days replayed',flush=True)
    (ROOT/'out/replay_manifest.json').write_text(json.dumps(manifest,indent=1))
