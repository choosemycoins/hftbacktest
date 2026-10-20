import os
os.environ['OPENBLAS_NUM_THREADS']='1'
os.environ['MPLCONFIGDIR']='/tmp/teletype-mpl'
import json
import math
from pathlib import Path
import numpy as np
import pandas as pd
from scipy.stats import norm,skew,kurtosis
from screen import COLUMNS,SCENARIOS

ROOT=Path(__file__).resolve().parent


def boot_means(values,B=2000):
    if values.ndim==1:values=values[:,None]
    n=len(values);length=7
    rng=np.random.default_rng(20260917)
    q,r=divmod(n,length)
    extended=np.concatenate([values,values[:length]])
    cs=np.vstack([np.zeros(values.shape[1]),np.cumsum(extended,axis=0)])
    blocks=cs[length:n+length]-cs[:n]
    ids=rng.integers(0,n,size=(B,q))
    samples=blocks[ids].sum(axis=1)
    if r:
        tail=rng.integers(0,n,size=B)
        samples+=cs[tail+r]-cs[tail]
    return samples/n


def dsr(values,trials=48):
    n=len(values);sd=float(np.std(values,ddof=1))
    if sd==0:return 0.
    sr=float(np.mean(values))/sd
    v=1-float(skew(values,bias=False))*sr+(float(kurtosis(values,fisher=False,bias=False))-1)/4*sr*sr
    gamma=.5772156649015329
    maxz=(1-gamma)*norm.ppf(1-1/trials)+gamma*norm.ppf(1-1/(trials*math.e))
    benchmark=maxz/math.sqrt(n-1)
    return float(norm.cdf((sr-benchmark)*math.sqrt(n-1)/math.sqrt(max(v,1e-12))))


def signflip(values):
    if not len(values):return None
    rng=np.random.default_rng(20260917);hits=0;obs=float(values.mean())
    for _ in range(1999):
        trial=np.dot(rng.integers(0,2,len(values))*2-1,values)/len(values)
        hits+=int(trial>=obs)
    return (hits+1)/2000


def daily_for(df,start,end):
    days=np.arange(start,end)
    cols=['gross_bp','net_bp','bnb_bp','mark_gross_bp','mark_net_bp','past_bp']
    daily=df.groupby('day')[cols].sum().reindex(days,fill_value=0)
    daily['count']=df.groupby('day').size().reindex(days,fill_value=0)
    return daily


def summarize(df,daily,notional=1000,placebo=False):
    vals=daily.to_numpy(float);boot=boot_means(vals)
    ci=np.quantile(boot,[.05,.95],axis=0)
    n=len(df)
    # Fee reserve: entry cost plus $999.00 notional is exactly the $1,000 sleeve.
    scale=notional/1.001/1e4
    gross_scale=notional/1e4
    avg=df.mean(numeric_only=True)
    result=dict(days=len(daily),trades=n,fills_per_day=n/len(daily),
        gross_bp=float(avg.get('gross_bp',0)) if n else None,
        net_bp=float(avg.get('net_bp',0)) if n else None,
        bnb_bp=float(avg.get('bnb_bp',0)) if n else None,
        mark_gross_bp=float(avg.get('mark_gross_bp',0)) if n else None,
        mark_net_bp=float(avg.get('mark_net_bp',0)) if n else None,
        past_bp=float(avg.get('past_bp',0)) if n else None,
        net_usd_day=float(daily.net_bp.mean()*scale),net_usd_day_ci90=(ci[:,1]*scale).tolist(),
        gross_usd_day=float(daily.gross_bp.mean()*gross_scale),gross_usd_day_ci90=(ci[:,0]*gross_scale).tolist(),
        mark_net_usd_day=float(daily.mark_net_bp.mean()*scale),mark_net_usd_day_ci90=(ci[:,4]*scale).tolist(),
        mark_gross_usd_day=float(daily.mark_gross_bp.mean()*gross_scale),
        dsr_48_trials=dsr(daily.net_bp.to_numpy()),win_fraction=float((df.net_bp>0).mean()) if n else None,
        take=int((df.reason==1).sum()),stop=int((df.reason==2).sum()),timeout=int((df.reason==3).sum()),
        ambiguous=int(df.ambiguous.sum()),
        max_drawdown_usd=float(np.max(np.maximum.accumulate(np.r_[0,np.cumsum(daily.net_bp)*scale])-np.r_[0,np.cumsum(daily.net_bp)*scale])))
    if n:
        valid=boot[:,-1]>0
        result['net_bp_ci90']=np.quantile(boot[valid,1]/boot[valid,-1],[.05,.95]).tolist()
        result['gross_bp_ci90']=np.quantile(boot[valid,0]/boot[valid,-1],[.05,.95]).tolist()
    if placebo: result['signflip_markout_p_one_sided']=signflip(df.mark_gross_bp.to_numpy())
    return result


def main():
    manifest=json.loads((ROOT/'out'/'manifest.json').read_text())
    dates={}
    for sym in ('BTCUSDT','SOLUSDT'):
        cells=[r for r in manifest if Path(r['file']).name.startswith(sym)]
        dates[sym]=(min(r['sec0']//86400 for r in cells),max((r['sec0']+r['rows']-1)//86400+1 for r in cells))
    frames=[]
    for p in sorted((ROOT/'out').glob('*.npz')):
        with np.load(p) as z:df=pd.DataFrame(z['trades'],columns=COLUMNS)
        df['symbol']=p.name.split('-')[0];frames.append(df)
    df=pd.concat(frames,ignore_index=True)
    for c in ['rule','side','variant','decision','fill','exit','reason','ambiguous']:df[c]=df[c].astype(np.int64)
    df['day']=df['fill']//86400
    df['year']=pd.to_datetime(df['fill'],unit='s',utc=True).dt.year
    df.to_csv(ROOT/'out'/'trades.csv.gz',index=False)
    cells=[]
    for (rule,sym,side,var),part in df.groupby(['rule','symbol','side','variant']):
        start,end=dates[sym];daily=daily_for(part,start,end)
        s=summarize(part,daily,placebo=var==1)
        s.update(rule=int(rule),symbol=sym,side=int(side),variant=int(var),lag=SCENARIOS[var][0],penetration_bp=SCENARIOS[var][1]*1e4)
        cells.append(s)
    annual=[]
    for (rule,sym,side,year),part in df[df.variant==1].groupby(['rule','symbol','side','year']):
        annual.append(dict(rule=int(rule),symbol=sym,side=int(side),year=int(year),trades=len(part),
                           net_bp=float(part.net_bp.mean()),gross_bp=float(part.gross_bp.mean()),
                           mark_net_bp=float(part.mark_net_bp.mean()),net_usd=float(part.net_bp.sum()/1e4*1000/1.001)))
    portfolios=[];curves={}
    start=int(pd.Timestamp('2020-08-12',tz='UTC').timestamp())//86400
    end=dates['SOLUSDT'][1]
    for rule in (0,1):
        for var in range(6):
            part=df[(df.rule==rule)&(df.side==1)&(df.variant==var)&(df.day>=start)]
            daily=daily_for(part,start,end)
            s=summarize(part,daily,placebo=var==1)
            s.update(rule=rule,variant=var,capital=2000,start='2020-08-12',end='2026-07-31')
            if var==1:
                split=int(pd.Timestamp('2023-08-01',tz='UTC').timestamp())//86400
                s['halves']={}
                for name,a,b in [('early',start,split),('late',split,end)]:
                    part2=part[(part.day>=a)&(part.day<b)]
                    s['halves'][name]=summarize(part2,daily_for(part2,a,b))
                curves[rule]=daily
            portfolios.append(s)
    out=dict(cells=cells,annual=annual,portfolios=portfolios,total_saved_trades=len(df),dsr_trials=48)
    (ROOT/'out'/'summary.json').write_text(json.dumps(out,indent=2,allow_nan=False))
    pd.DataFrame(cells).to_csv(ROOT/'out'/'cells.csv',index=False)
    pd.DataFrame(annual).to_csv(ROOT/'out'/'annual.csv',index=False)
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    fig,axs=plt.subplots(2,1,figsize=(11,7),sharex=True)
    for rule,daily in curves.items():
        dates2=pd.to_datetime(daily.index,unit='D',origin='unix')
        axs[rule].plot(dates2,np.cumsum(daily.gross_bp)*.1,label='Gross, zero costs')
        axs[rule].plot(dates2,np.cumsum(daily.net_bp)*.1/1.001,label='Barrier P&L after fees')
        axs[rule].plot(dates2,np.cumsum(daily.mark_net_bp)*.1/1.001,label='Fixed-horizon P&L after fees',alpha=.7)
        axs[rule].set_title(['Resting quote 1% below price','Past 30-minute range'][rule]);axs[rule].set_ylabel('Cumulative USD');axs[rule].grid(alpha=.2);axs[rule].legend()
    fig.suptitle('Binance spot price-path proxy: two $1,000 long sleeves, fixed notional\nNot an executable fill/queue or compounding-equity backtest')
    fig.tight_layout();fig.savefig(ROOT/'out'/'pnl.png',dpi=140)
    print(json.dumps([s for s in portfolios if s['variant']==1],indent=2))


if __name__=='__main__':main()
