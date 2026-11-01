# Copyright 2026 The Gocql Authors. All rights reserved.
# Use of this source code is governed by a BSD-style license.
"""Join sequential measurement/native owners and preserve exact live handles."""
import hashlib,json,os,subprocess,time
from pathlib import Path
ROOT=Path(__file__).resolve().parents[2]
OUT=Path('/extra/scylladb/gocql-api-test-cluster/step17/release')
assert len(json.loads((OUT/'checks/checks.json').read_text()))==38
assert len(json.loads((OUT/'performance/checks.json').read_text()))==19
rows=[]
for name,tool in [('supplemental','step17_supplemental.py'),('native-matrix','step17_native_matrix.py'),('cancel-probe','step17_cancel_probe.py')]:
    command=['python3','feature-specs/tools/'+tool];log=OUT/(name+'-supervisor.log');start=time.monotonic()
    with log.open('wb') as output:
        child=subprocess.Popen(command,cwd=ROOT,env=dict(os.environ,GOMAXPROCS='4'),stdout=output,stderr=subprocess.STDOUT)
        row=dict(name=name,command=command,pid=child.pid,log=str(log),phase='running');rows.append(row)
        (OUT/'pipeline.json').write_text(json.dumps(dict(pid=os.getpid(),steps=rows),indent=2)+'\n')
        code=child.wait()
    row.update(exit_code=code,seconds=time.monotonic()-start,phase='joined',sha256=hashlib.sha256(log.read_bytes()).hexdigest())
    (OUT/'pipeline.json').write_text(json.dumps(dict(pid=os.getpid(),steps=rows),indent=2)+'\n');print(json.dumps(row),flush=True)
    if code:raise SystemExit(code)
