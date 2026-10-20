import os
os.environ['OPENBLAS_NUM_THREADS']='1';os.environ['MPLCONFIGDIR']='/tmp/futures-spikes-mpl'
from pathlib import Path
import json,math
import numpy as np
import pandas as pd
from scipy.stats import norm,skew,kurtosis
from engine import COLS,VARIANTS
ROOT=Path(__file__).resolve().parent

def boot(values):
    if values.ndim==1:values=values[:,None]
    rng=np.random.default_rng(20260917);n=len(values);L=3;q,r=divmod(n,L)
    ext=np.concatenate([values,values[:L]])
    cs=np.vstack([np.zeros(values.shape[1]),np.cumsum(ext,axis=0)])
    blocks=cs[L:n+L]-cs[:n];ix=rng.integers(0,n,(5000,q))
    sums=blocks[ix].sum(axis=1)
    if r:
        tail=rng.integers(0,n,5000);sums+=cs[tail+r]-cs[tail]
    return sums/n

def dsr(x):
    n=len(x);sd=np.std(x,ddof=1)
    if n<4 or sd<=0:return 0.
    sr=np.mean(x)/sd;N=420;g=.5772156649015329
    benchmark=((1-g)*norm.ppf(1-1/N)+g*norm.ppf(1-1/(math.e*N)))/np.sqrt(n-1)
    variance=1-skew(x,bias=False)*sr+(kurtosis(x,fisher=False,bias=False)-1)*sr*sr/4
    return float(norm.cdf((sr-benchmark)*np.sqrt(n-1)/np.sqrt(max(variance,1e-12))))

def stats(part,calendar):
    sums=part.groupby('day')[['net','gross','stress','zero_mid','mark_net']].sum().reindex(calendar,fill_value=0)
    sums['count']=part.groupby('day').size().reindex(calendar,fill_value=0)
    booted=boot(sums.to_numpy())
    total_notional=float((part.entry*part.qty).sum())
    result=dict(trades=len(part),days=len(calendar),net_usd=float(part.net.sum()),
        net_usd_day=float(sums.net.mean()),net_usd_day_ci90=np.quantile(booted[:,0],[.05,.95]).tolist(),
        gross_usd_day=float(sums.gross.mean()),gross_usd_day_ci90=np.quantile(booted[:,1],[.05,.95]).tolist(),
        stress_usd_day=float(sums.stress.mean()),zero_mid_usd_day=float(sums.zero_mid.mean()),
        net_bp=part.net.sum()/total_notional*1e4 if total_notional else None,
        gross_bp=part.gross.sum()/total_notional*1e4 if total_notional else None,
        mean_entry_notional=float((part.entry*part.qty).mean()) if len(part) else 0,
        mark_net_bp=part.mark_net.sum()/total_notional*1e4 if total_notional else None,
        past_bp=float(part.past_bp.mean()) if len(part) else None,
        take=int((part.reason==1).sum()),stop=int((part.reason==2).sum()),timeout=int((part.reason==3).sum()),
        capacity_shortfalls=int((part.capacity_ok==0).sum()),funding_crossings=int(part.funding_cross.sum()),
        net_win_fraction=float((part.net>0).mean()) if len(part) else None,
        distance_bp_median=float(part.distance_bp.median()) if len(part) else None,
        dsr_420=dsr(sums.net.to_numpy()),positive_days=int((sums.net>0).sum()),
        max_drawdown=float(np.max(np.maximum.accumulate(np.r_[0,sums.net.cumsum()])-np.r_[0,sums.net.cumsum()])))
    return result,sums

def paired(a,b):
    diff=a-b;blocks=np.array([diff[i:i+3].sum() for i in range(0,len(diff),3)])
    rng=np.random.default_rng(20260917)
    null=(rng.choice(np.array([-1,1]),size=(1999,len(blocks)))*blocks).sum(axis=1)/len(diff)
    return dict(main_minus_fixed_usd_day=float(diff.mean()),ci90=np.quantile(boot(diff)[:,0],[.05,.95]).tolist(),
                signflip_p=float((1+np.sum(null>=diff.mean()))/2000))

def main():
    frames=[]
    for p in sorted((ROOT/'out').glob('*.npz')):
        with np.load(p) as z:
            f=pd.DataFrame(z['trades'],columns=COLS);day_ns=int(z['day_ns'])
        f['symbol']=p.stem.rsplit('_',1)[0].upper();f['day']=day_ns//(86400*10**9)
        for col in ['decision','fill','exit']:f[col]=f[col].astype(np.int64)+day_ns
        frames.append(f)
    df=pd.concat(frames,ignore_index=True)
    for col in ['variant','side','reason','capacity_ok','funding_cross']:df[col]=df[col].astype(int)
    df.to_csv(ROOT/'out/trades.csv.gz',index=False)
    calendar=np.arange(pd.Timestamp('2026-08-12').value//(86400*10**9),pd.Timestamp('2026-09-10').value//(86400*10**9))
    split=pd.Timestamp('2026-08-27').value//(86400*10**9)
    configs=[];series={}
    for v in range(len(VARIANTS)):
        part=df[df.variant==v];s,da=stats(part,calendar)
        s.update(variant=v,frame=VARIANTS[v][0],k=VARIANTS[v][1],lag=VARIANTS[v][2])
        s['halves']={}
        for label,days in [('early',calendar[calendar<split]),('late',calendar[calendar>=split])]:
            s['halves'][label]=stats(part[part.day.isin(days)],days)[0]
        configs.append(s);series[v]=da
    cells=[]
    universe=sorted({key.rsplit('_',1)[0].upper() for key in json.loads((ROOT/'data/params.json').read_text())})
    for v in range(len(VARIANTS)):
        for sym in universe:
            for side in (1,-1):
                part=df[(df.variant==v)&(df.symbol==sym)&(df.side==side)]
                s,_=stats(part,calendar);s.update(variant=v,symbol=sym,side=side);cells.append(s)
    pair=paired(series[4].net.to_numpy(),series[9].net.to_numpy())
    summary=dict(configurations=configs,cells=cells,paired_main_fixed=pair,days=29,symbols=15,variants=VARIANTS)
    (ROOT/'out/summary.json').write_text(json.dumps(summary,indent=2,allow_nan=False))
    pd.DataFrame([{k:v for k,v in s.items() if k!='halves'} for s in configs]).to_csv(ROOT/'out/configurations.csv',index=False)
    pd.DataFrame(cells).to_csv(ROOT/'out/cells.csv',index=False)
    pd.DataFrame({v:series[v].net for v in series}).to_csv(ROOT/'out/daily_net.csv')
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    fig,ax=plt.subplots(figsize=(10,5))
    dates=pd.to_datetime(calendar,unit='D')
    for v,label in [(4,'5-minute ATR x 2'),(9,'Fixed 1%')]:
        ax.plot(dates,series[v].net.cumsum(),label=label+' net')
        ax.plot(dates,series[v].gross.cumsum(),ls='--',alpha=.7,label=label+' before fees/impact')
    ax.set_title('Binance USDT perpetuals, 15 symbols, both directions\nFixed sleeves, maximum aggregate notional $2,000; conditional trade-through fills')
    ax.set_ylabel('Cumulative P&L, USD');ax.grid(alpha=.2);ax.legend();fig.autofmt_xdate();fig.tight_layout();fig.savefig(ROOT/'out/pnl.png',dpi=140)
    print(json.dumps(dict(main=configs[4],fixed=configs[9],paired=pair),indent=2))

if __name__=='__main__':main()
