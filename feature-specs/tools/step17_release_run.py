# Copyright 2026 The Gocql Authors. All rights reserved.
# Use of this source code is governed by a BSD-style license.
"""Run every mandatory final gate after the complete test-oracle batch froze."""
import hashlib,json,os,subprocess,time
from pathlib import Path
ROOT=Path(__file__).resolve().parents[2]
OUT=Path('/extra/scylladb/gocql-api-test-cluster/step17/release')
sha=lambda p:hashlib.sha256(Path(p).read_bytes()).hexdigest()
pin=json.loads((OUT/'source-frozen.json').read_text());binding=sha(OUT/'source-frozen.json')
rows=[]
for name,tool in [('checks','step17_checks.py'),('performance','step17_performance.py'),('pipeline','step17_pipeline.py')]:
    for path,digest in pin.items():assert sha(ROOT/path)==digest,'frozen runtime drift '+path
    command=['python3','feature-specs/tools/'+tool];log=OUT/(name+'-supervisor.log');start=time.monotonic()
    with log.open('wb')as output:
        child=subprocess.Popen(command,cwd=ROOT,env=dict(os.environ,GOMAXPROCS='4'),stdout=output,stderr=subprocess.STDOUT)
        row=dict(name=name,command=command,pid=child.pid,phase='running',log=str(log));rows.append(row)
        (OUT/'release-run.json').write_text(json.dumps(dict(pid=os.getpid(),source_sha256=binding,steps=rows),indent=2)+'\n')
        code=child.wait()
    row.update(phase='joined',exit_code=code,seconds=time.monotonic()-start,sha256=sha(log))
    (OUT/'release-run.json').write_text(json.dumps(dict(pid=os.getpid(),source_sha256=binding,steps=rows),indent=2)+'\n');print(json.dumps(row),flush=True)
    if code:raise SystemExit(code)
