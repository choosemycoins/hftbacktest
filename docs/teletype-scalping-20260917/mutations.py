"""Deliberately break execution invariants; every mutant must be killed by tests."""
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile

ROOT=Path(__file__).resolve().parent
src=(ROOT/'screen.py').read_text()
mutations={
 'fill_on_previous_bar':('range(active,active+ttl)','range(active-1,active+ttl)'),
 'no_penetration':('l[j]<limit*(1-eps)','l[j]<limit'),
 'entry_bar_take':('(j>fi) and (h[j]>take*(1+eps)','(j>=fi) and (h[j]>take*(1+eps)'),
 'take_before_stop':('if sl:\n','if sl and not tp:\n'),
 'ignore_stop_latency':('ex=j+max(1,lag)','ex=j'),
 'one_fee_only':('fee*(1+actual/entry)','fee*(actual/entry)'),
 'ignore_gaps':('return bad_prefix[end+1]==bad_prefix[start]','return True'),
 'ignore_markout_overlap':('max(decision,exit_time,mark_time)','max(decision,exit_time)'),
}
env=dict(os.environ,NUMBA_DISABLE_JIT='1',OPENBLAS_NUM_THREADS='1')
baseline=subprocess.run([sys.executable,'-m','pytest','-q','test_screen.py'],cwd=ROOT,env=env,capture_output=True,text=True)
assert baseline.returncode==0,baseline.stdout+baseline.stderr
report=[]
for name,(old,new) in mutations.items():
    assert old in src,name
    temp=Path(tempfile.mkdtemp(prefix='teletype-mutation-'))
    (temp/'screen.py').write_text(src.replace(old,new))
    shutil.copy2(ROOT/'test_screen.py',temp/'test_screen.py')
    p=subprocess.run([sys.executable,'-m','pytest','-q','--tb=no','test_screen.py'],cwd=temp,env=env,capture_output=True,text=True)
    report.append(dict(mutation=name,killed=p.returncode==1,returncode=p.returncode,test_output=p.stdout))
    assert p.returncode==1,(name,p.stdout,p.stderr)
    print(name,'KILLED',flush=True)
(ROOT/'out'/'mutation_results.json').write_text(json.dumps(report,indent=2))
