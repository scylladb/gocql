# Copyright 2026 The Gocql Authors. All rights reserved.
# Use of this source code is governed by a BSD-style license.
"""Run source-bound additive measurement oracles, then complete native matrix."""
import hashlib,json,os,subprocess,time
from pathlib import Path
ROOT=Path(__file__).resolve().parents[2]
OUT=Path('/extra/scylladb/gocql-api-test-cluster/step17/release')
checks=[]
for name,tool in [('complete-tags','step17_tag_compile.py'),('latency-deadlines','step17_latency_comparison.py'),('text-identity','step15_text_identity.py'),('vectors','step15_vector_overlay.py'),('batch-envelopes','step15_envelope_overlay.py'),('segments','step15_segment_overlay.py'),('probe','step17_probe_overlay.py'),('heap','step15_heap_overlay.py')]:
    command=['python3','feature-specs/tools/'+tool];log=OUT/('supplemental-'+name+'.log');start=time.monotonic()
    env=dict(os.environ,GOMAXPROCS='4',GOCQL_STEP15_CHECKS=str(OUT/'performance'))
    with log.open('wb') as output:code=subprocess.run(command,cwd=ROOT,env=env,stdout=output,stderr=subprocess.STDOUT).returncode
    row=dict(name=name,command=command,exit_code=code,seconds=time.monotonic()-start,log=str(log),sha256=hashlib.sha256(log.read_bytes()).hexdigest(),source_sha256=hashlib.sha256((OUT/'checks/source.json').read_bytes()).hexdigest(),tool_sha256=hashlib.sha256((ROOT/'feature-specs/tools'/tool).read_bytes()).hexdigest())
    checks.append(row);(OUT/'supplemental-checks.json').write_text(json.dumps(checks,indent=2)+'\n');print(json.dumps(row),flush=True)
    if code:raise SystemExit(code)
