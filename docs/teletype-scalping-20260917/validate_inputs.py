"""Independent original CSV vs float32 cache check, including ms/us transition."""
import hashlib
import json
from pathlib import Path
import zipfile
import numpy as np
import pandas as pd

ROOT=Path(__file__).resolve().parent
BASE=Path('/home/storage/hft-scratch')
CASES=[('BTCUSDT','2017-08'),('SOLUSDT','2020-08'),('BTCUSDT','2025-01'),('SOLUSDT','2026-07')]
results=[]
for sym,month in CASES:
    src=BASE/('spot1s_oos' if sym=='BTCUSDT' else 'spot1s_sol')/(sym+'-1s-'+month+'.zip')
    cache=BASE/'spot1s_bench'/'cache'/(sym+'-'+month+'.npz')
    with zipfile.ZipFile(src) as z,z.open(z.namelist()[0]) as f:
        raw=pd.read_csv(f,header=None,usecols=[0,2,3,4]).to_numpy()
    # timestamps in the original csv are exact integers (<2**53 even after switch).
    timestamps=raw[:,0].astype(np.int64)
    scale=1000000 if timestamps[0]>10**14 else 1000
    sec=timestamps//scale
    with np.load(cache) as z:
        samples=np.r_[np.arange(300),np.arange(len(raw)//2,len(raw)//2+300),np.arange(len(raw)-300,len(raw))]
        idx=sec[samples]-int(z['sec0'])
        error=0.
        for raw_col,key in [(1,'h'),(2,'l'),(3,'c')]:
            rel=np.abs(z[key][idx].astype(float)/raw[samples,raw_col]-1)
            error=max(error,float(rel.max()))
        assert error<=1e-7,(sym,month,error)
        assert np.all(np.diff(sec)>0)
    results.append(dict(symbol=sym,month=month,raw_rows=len(raw),sampled_rows=len(samples),
                        timestamp_divisor=scale,max_relative_error=error,
                        raw_path=str(src),sha256=hashlib.file_digest(src.open('rb'),'sha256').hexdigest()))
(ROOT/'data').mkdir(exist_ok=True)
(ROOT/'data'/'source_checks.json').write_text(json.dumps(results,indent=2))
print(json.dumps(results,indent=1))
