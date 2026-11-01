# Copyright 2026 The Gocql Authors. All rights reserved.
# Use of this source code is governed by a BSD-style license.
"""Run bounded final-source checks; retain each whole command's exit and log."""
import hashlib
import json
import os
from pathlib import Path
import subprocess
import time

ROOT = Path(__file__).resolve().parents[2]
OUT = Path('/extra/scylladb/gocql-api-test-cluster/step14/verified/checks')
OUT.mkdir(parents=True, exist_ok=True)
source = {}
for name in subprocess.check_output(['git','ls-files','--cached','--others','--exclude-standard','-z'],cwd=ROOT).decode().split('\0'):
    path=ROOT/name
    if path.is_file() and not name.startswith('feature-specs/') and '__pycache__' not in name and path.suffix in ('.go','.mod','.sum','.py','.java','.cql','.crt','.key','.p12','.cnf','.sh'):
        source[name]=hashlib.sha256(path.read_bytes()).hexdigest()
(OUT/'source.json').write_text(json.dumps(source,sort_keys=True,indent=2)+'\n')
source_sha=hashlib.sha256((OUT/'source.json').read_bytes()).hexdigest()
checks=[]
def run(name,command,environment=None,**extra):
    env=os.environ.copy();env.update(environment or {})
    log=OUT/(name+'.log');start=time.monotonic()
    with log.open('wb') as out:
        result=subprocess.run(command,cwd=ROOT,env=env,stdout=out,stderr=subprocess.STDOUT)
    record=dict(name=name,command=command,environment=environment or {},exit_code=result.returncode,seconds=round(time.monotonic()-start,3),log=str(log),sha256=hashlib.sha256(log.read_bytes()).hexdigest(),source_sha256=source_sha,**extra)
    checks.append(record);(OUT/'checks.json').write_text(json.dumps(checks,indent=2)+'\n');print(json.dumps(record),flush=True)
    return result.returncode
run('spec-verifier',['python3','feature-specs/verify.py'])
run('fixture-python',['python3','-m','unittest','discover','-s','tests/fixtures','-p','test_*.py'])
run('unit-race',['go','test','-json','-race','-tags','unit','-count=1','-timeout=6m','./...'])
run('vet',['go','vet','-tags','unit integration gocql_debug bench','./...'])
run('all-tags',['go','test','-json','-tags','all ccm bench','-run','^$','./...'],scope='compile only')
run('386',['go','test','-json','-tags','unit','-count=1','-timeout=6m','./...'],{'GOARCH':'386','CGO_ENABLED':'0'},scope='actual 32-bit execution')
run('arm64-build',['go','test','-exec','/bin/true','-tags','all ccm bench','-run','^$','./...'],{'GOARCH':'arm64','CGO_ENABLED':'0'},scope='compile only')
run('module-verify',['go','mod','verify'])
run('cost-comparison',['go','test','-tags','unit','-run','^$','-bench','^BenchmarkStep07ExecutionStorage$','-benchmem','-benchtime=20x','-count=1','.'],scope='equivalent result-shape allocation/cost comparison; static payload metrics are not storage measurements')
run('cost-fragmented',['go','test','-tags','unit bench','-run','^$','-bench','Benchmark(V2ExclusiveTransport|V2ExclusiveRowsScaling|V2StreamFinalStorage|V2StreamingNestedElements|V2RowDestinationFragments|Step08IndependentStreamOracle)$','-benchmem','-benchtime=10x','-count=1','.','./internal/codec'],scope='focused smoke; allocation/backing assertions remain separate')
for package,target,budget in [
 ('./internal/wire','FuzzStreamCells','30s'),('./internal/frame','FuzzStreamEnvelope','30s'),
 ('./internal/segment','FuzzValidatedUnits','30s'),('./internal/codec','FuzzV2StreamingValues','30s'),
 ('./internal/codec','FuzzV2ScalarUTF8','30s'),('.','FuzzV2ExclusiveIncrementalFraming','30s'),
 ('.','FuzzStep07StreamExecutionBoundary','30s'),('.','FuzzV2PagingHistoryBudgets','30s'),
 ('.','FuzzV2PageDestinationFraming','30s'),('.','FuzzStep06TabletPayload','30s'),
 ('./internal/wirecompress','FuzzSpanCompression','30s'),('.','FuzzStep14StreamingCancellationLifetime','60s')]:
 run('fuzz-'+target,['go','test','-tags','unit','-run','^$','-fuzz','^'+target+'$','-fuzztime='+budget,'-parallel=2','-timeout=3m',package],fuzz_budget=budget,workers=2)
if any(row['exit_code'] for row in checks):raise SystemExit(1)
