import os
for k in ['OMP_NUM_THREADS','OPENBLAS_NUM_THREADS','MKL_NUM_THREADS']:os.environ[k]='1'
from pathlib import Path
import concurrent.futures as cf
import datetime as dt
import gzip,hashlib,json,subprocess,time
import numpy as np

ROOT=Path(__file__).resolve().parent
RAW=Path('/home/storage/hft-data/binancefuturesum-c')
DTYPE=np.dtype([(x,'<i8') for x in ['local','exchange','kind','id','side']]+[(x,'<f8') for x in ['p','q','a','aq']])

def one(path):
    out=ROOT/'cache'/path.stem
    binary=out.with_suffix('.bin')
    result=subprocess.run([str(ROOT/'extractor/target/release/futures-spike-extract'),str(path),str(binary)],capture_output=True,text=True,check=True)
    counts=json.loads(result.stdout)
    arr=np.fromfile(binary,dtype=DTYPE)
    disorder=int(np.count_nonzero(np.diff(arr['local'])<0))
    if disorder:arr.sort(order='local',kind='stable')
    # Deduplicate by type and exchange id, preserving first receipt.
    keep=np.ones(len(arr),bool);dups=0
    for kind in (1,2):
        ix=np.flatnonzero(arr['kind']==kind)
        if len(ix):
            _,first=np.unique(arr['id'][ix],return_index=True)
            keep[ix]=False;keep[ix[first]]=True;dups+=len(ix)-len(first)
    arr=arr[keep]
    np.savez_compressed(out.with_suffix('.npz'),events=arr)
    binary.unlink()
    return dict(path=str(path),sha256=hashlib.file_digest(path.open('rb'),'sha256').hexdigest(),
                counts=counts,disorder=disorder,duplicates=dups,events=len(arr),
                cache_bytes=out.with_suffix('.npz').stat().st_size)

def metadata():
    snapshots=[];sources=[]
    for p in sorted(Path('/home/storage/hft-data/params/binanceusdm').glob('*.gz')):
        if p.name[-11:-3]>'20260909':continue
        sources.append(dict(path=str(p),sha256=hashlib.file_digest(p.open('rb'),'sha256').hexdigest()))
        for ln in gzip.open(p,'rt'):
            r=json.loads(ln)
            if r['kind']=='snapshot':snapshots.append((dt.datetime.fromisoformat(r['ts']).timestamp(),r['payload']['symbols']))
    snapshots.sort(key=lambda r:r[0]);out={};changed=[]
    syms=sorted({p.stem.rsplit('_',1)[0].upper() for p in RAW.glob('*.gz')})
    for i in range(29):
        day=dt.datetime(2026,8,12,tzinfo=dt.timezone.utc)+dt.timedelta(days=i);ts=day.timestamp()
        prior=[r for r in snapshots if r[0]<ts]
        assert prior
        stamp,rows=prior[-1];by={r['symbol']:r for r in rows}
        for sym in syms:
            r=by[sym];assert r['contractType']=='PERPETUAL' and r['quoteAsset']=='USDT' and r['status']=='TRADING'
            f={v['filterType']:v for v in r['filters']}
            cfg=dict(tick=float(f['PRICE_FILTER']['tickSize']),lot=float(f['LOT_SIZE']['stepSize']),
                     minqty=float(f['LOT_SIZE']['minQty']),minnotional=float(f['MIN_NOTIONAL']['notional']),
                     market_lot=float(f['MARKET_LOT_SIZE']['stepSize']),maxqty=min(float(f['LOT_SIZE']['maxQty']),float(f['MARKET_LOT_SIZE']['maxQty'])),
                     param_stamp=stamp)
            assert cfg['lot']==cfg['market_lot']
            key=sym.lower()+'_'+day.strftime('%Y%m%d');out[key]=cfg
            for t,rr in snapshots:
                if not ts<=t<ts+86400:continue
                z=next(x for x in rr if x['symbol']==sym);ff={x['filterType']:x for x in z['filters']}
                if float(ff['PRICE_FILTER']['tickSize'])!=cfg['tick'] or float(ff['LOT_SIZE']['stepSize'])!=cfg['lot']:
                    changed.append(key)
    (ROOT/'data/params.json').write_text(json.dumps(out,indent=1))
    (ROOT/'data/param_sources.json').write_text(json.dumps(sources,indent=1))
    (ROOT/'data/params_changed.json').write_text(json.dumps(sorted(set(changed))))

if __name__=='__main__':
    for d in ['cache','out','data']:(ROOT/d).mkdir(exist_ok=True)
    metadata()
    paths=sorted(p for p in RAW.glob('*.gz') if '20260812'<=p.stem.rsplit('_',1)[1]<='20260909')
    assert len(paths)==435
    start=time.time();manifest=[]
    with cf.ThreadPoolExecutor(max_workers=12) as pool:
        for i,r in enumerate(pool.map(one,paths),1):
            manifest.append(r)
            if i%15==0:print(f'{i}/435 extracted; {(time.time()-start)/60:.1f} min',flush=True)
    (ROOT/'data/manifest.json').write_text(json.dumps(manifest,indent=1))
