# Copyright 2026 The Gocql Authors. All rights reserved.
# Use of this source code is governed by a BSD-style license.
"""Repeat only fuzz gates with empty, retained corpus caches for exact seed provenance."""
import hashlib
import json
from pathlib import Path
import re
import shutil
import subprocess
import time

ROOT=Path(__file__).resolve().parents[2]
OUT=Path('/extra/scylladb/gocql-api-test-cluster/step14/verified')
checks_path=OUT/'checks/checks.json';checks=json.loads(checks_path.read_text())
old=[row for row in checks if row['name'].startswith('fuzz-')]
assert len(old)==12 and all(row['exit_code']==0 for row in old)
prior=OUT/'fuzz-prior';prior.mkdir()
(prior/'checks.json').write_text(json.dumps(old,indent=2)+'\n')
for row in old:shutil.copy2(row['log'],prior/Path(row['log']).name)
declarations={row['Name']:row for row in json.loads((OUT/'current-declarations.json').read_text())}
seed_counts=dict(FuzzStreamCells=2,FuzzStreamEnvelope=2,FuzzValidatedUnits=2,FuzzV2StreamingValues=4,FuzzV2ScalarUTF8=6,FuzzV2ExclusiveIncrementalFraming=3,FuzzStep07StreamExecutionBoundary=3,FuzzV2PagingHistoryBudgets=2,FuzzV2PageDestinationFraming=2,FuzzStep06TabletPayload=1,FuzzSpanCompression=3,FuzzStep14StreamingCancellationLifetime=8)
results=[]
for oldrow in old:
    row=dict(oldrow);target=row['name'].removeprefix('fuzz-');cache=OUT/'fuzz-corpus'/target
    assert not cache.exists();cache.mkdir(parents=True)
    command=oldrow['command']+['-args','-test.fuzzcachedir='+str(cache)]
    log=OUT/'checks'/('seeded-'+row['name']+'.log');start=time.monotonic()
    with log.open('wb') as out:status=subprocess.run(command,cwd=ROOT,stdout=out,stderr=subprocess.STDOUT).returncode
    raw=log.read_text();baseline=re.search(r'gathering baseline coverage: 0/(\d+) completed',raw)
    assert baseline and int(baseline.group(1))==seed_counts[target],(target,raw[:300])
    summary=[line for line in raw.splitlines() if 'execs:' in line]
    source=declarations[target]
    row.update(command=command,exit_code=status,seconds=time.monotonic()-start,log=str(log),sha256=hashlib.sha256(log.read_bytes()).hexdigest(),seed_provenance=dict(initial_cache_empty=True,source_testdata_corpus=[],initial_builtin_seed_count=seed_counts[target],builtin_seed_function_source=source['Source'],builtin_seed_function_sha256=source['SHA'],source_file=source['File'],retained_mutation_corpus={str(p.relative_to(cache)):hashlib.sha256(p.read_bytes()).hexdigest() for p in cache.rglob('*') if p.is_file()}),fuzz_summary=summary[-1] if summary else None)
    checks=[row if c['name']==row['name'] else c for c in checks];checks_path.write_text(json.dumps(checks,indent=2)+'\n')
    results.append(row);(OUT/'fuzz-verification.json').write_text(json.dumps(results,indent=2)+'\n')
    print(json.dumps(dict(target=target,exit_code=status,seeds=seed_counts[target],summary=row['fuzz_summary'])),flush=True)
    if status:raise SystemExit(status)
