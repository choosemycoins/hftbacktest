from pathlib import Path
import tempfile,subprocess,shutil,json,os,sys
ROOT=Path(__file__).resolve().parent
src=(ROOT/'engine.py').read_text()
mutants={
 'future_candle':('end=minute//w','end=minute//w+1'),
 'aggressor_reversed':('side[i]!=-direction','side[i]!=direction'),
 'old_print_allowed':('exchange[i]<active or ','False or '),
 'ignore_trade_quantity':('q[i]+1e-10<qty','False'),
 'no_penetration':('through+tick*1e-7>=tick','through+tick*1e-7>=0'),
 'early_stop_exit':('trigger+lag','trigger'),
 'one_fee_only':('entry*.0002+actual*.0005','actual*.0005'),
 'ignore_funding':('if fi<0 or not funding_safe(d,ft[fi],fn[fi],lag):','if fi<0:'),
 'ignore_postonly':('(side==1 and limit>=ap[abi]) or (side==-1 and limit<=bp[abi])','False'),
 'wrong_tick_rounding':('np.floor(raw+1e-10)','np.ceil(raw-1e-10)'),
 'overlapping_positions':('if d<busy:continue','if False:continue'),
}
env=dict(os.environ,NUMBA_DISABLE_JIT='1',OPENBLAS_NUM_THREADS='1')
baseline=subprocess.run([sys.executable,'-m','pytest','-q','test_engine.py'],cwd=ROOT,env=env,capture_output=True,text=True)
assert baseline.returncode==0,baseline.stdout
results=[]
for name,(a,b) in mutants.items():
    assert a in src
    p=Path(tempfile.mkdtemp(prefix='futures-spike-mut-'))
    (p/'engine.py').write_text(src.replace(a,b));shutil.copy2(ROOT/'test_engine.py',p/'test_engine.py')
    r=subprocess.run([sys.executable,'-m','pytest','-q','--tb=no','test_engine.py'],cwd=p,env=env,capture_output=True,text=True)
    results.append(dict(name=name,killed=r.returncode==1,output=r.stdout))
    print(name,'killed' if r.returncode==1 else 'SURVIVED',flush=True)
(ROOT/'out/mutations.json').write_text(json.dumps(results,indent=2))
assert all(r['killed'] for r in results),results
