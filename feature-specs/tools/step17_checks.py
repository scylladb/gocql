# Copyright 2026 The Gocql Authors. All rights reserved.
# Use of this source code is governed by a BSD-style license.
"""Final-tree release checks. Every receipt binds exact inputs and complete exits."""
import hashlib,json,os,platform,subprocess,time
from pathlib import Path

ROOT=Path(__file__).resolve().parents[2]
OUT=Path('/extra/scylladb/gocql-api-test-cluster/step17/release/checks')
OUT.mkdir(parents=True,exist_ok=True)
sha=lambda p:hashlib.sha256(Path(p).read_bytes()).hexdigest()
def snapshot():
    paths=subprocess.check_output(['git','ls-files','--cached','--others','--exclude-standard','-z'],cwd=ROOT).decode().split('\0')
    return {p:sha(ROOT/p) for p in sorted(set(paths)) if p and not p.startswith('feature-specs/') and '__pycache__' not in p and (ROOT/p).is_file() and (ROOT/p).suffix in ('.go','.mod','.sum','.py','.java','.cql','.crt','.key','.p12','.cnf','.sh')}
source=snapshot()
(OUT/'source.json').write_text(json.dumps(source,sort_keys=True,indent=2)+'\n')
binding=sha(OUT/'source.json')
checks=json.loads((OUT/'checks.json').read_text()) if (OUT/'checks.json').exists() else []
def run(name,cmd,extra=None):
    prior=next((c for c in checks if c['name']==name and c['exit_code']==0),None)
    if prior:
        assert prior['command']==cmd and prior['source_sha256']==binding and sha(prior['log'])==prior['sha256']
        return
    failed=[c for c in checks if c['name']==name and c['exit_code']!=0]
    if failed:
        history=OUT/'diagnostic-checks.json'
        rows=json.loads(history.read_text()) if history.exists() else []
        for row in failed:
            prior_log=Path(row['log']);saved=OUT/(name+'-diagnostic-'+str(len(rows))+'.log')
            prior_log.rename(saved);row=dict(row,log=str(saved));rows.append(row)
        history.write_text(json.dumps(rows,indent=2)+'\n')
    env=dict(os.environ,GOMAXPROCS='4');env.update(extra or {})
    log=OUT/(name+'.log');start=time.monotonic()
    with log.open('wb') as output:
        code=subprocess.run(cmd,cwd=ROOT,env=env,stdout=output,stderr=subprocess.STDOUT).returncode
    row=dict(name=name,command=cmd,environment=dict(GOMAXPROCS='4',**(extra or {})),exit_code=code,seconds=time.monotonic()-start,log=str(log),sha256=sha(log),source_sha256=binding)
    checks[:]=[c for c in checks if c['name']!=name]+[row]
    (OUT/'checks.json').write_text(json.dumps(checks,indent=2)+'\n');print(json.dumps(row),flush=True)
    assert snapshot()==source,'runtime inputs changed during checks'
    if code:raise SystemExit(code)

run('environment',['sh','-c','go version; go env GOOS GOARCH GOVERSION CGO_ENABLED; lscpu'])
run('spec-verifier',['python3','feature-specs/verify.py'])
run('fixture-python',['/extra/scylladb/scylladb/.venv/bin/python','-m','unittest','discover','-s','tests/fixtures','-p','test_*.py'])
run('default',['go','test','-json','-count=1','-timeout=6m','./...'])
run('unit',['go','test','-json','-tags','unit','-count=1','-timeout=6m','./...'])
run('unit-race',['go','test','-json','-race','-tags','unit','-count=1','-timeout=5m','./...'])
run('operation-counters',['go','test','-json','-tags','unit gocql_perf','-count=1','-run','^Test(Step15|Step16)','./internal/wire','./internal/codec','./internal/wirecompress','.'])
run('counters-race',['go','test','-json','-race','-tags','unit gocql_perf','-count=1','-run','^Test(Step15|Step16)','./internal/wire','./internal/codec','./internal/wirecompress','.'])
run('vet',['go','vet','-tags','unit integration gocql_debug bench gocql_perf','./...'])
run('all-tags',['go','test','-json','-tags','all ccm bench gocql_perf','-run','^$','./...'])
run('integration-build',['go','test','-c','-tags','integration gocql_debug','-o',str(OUT/'integration.test'),'.'])
run('386',['go','test','-json','-tags','unit','-count=1','-timeout=6m','./...'],{'GOARCH':'386','CGO_ENABLED':'0'})
run('arm64-build',['go','test','-exec','/bin/true','-tags','all ccm bench gocql_perf','-run','^$','./...'],{'GOARCH':'arm64','CGO_ENABLED':'0'})
run('module-verify',['go','mod','verify'])
run('module-drift',['make','check-go-mod-drift'])
run('dependency-catalog',['go','list','-deps','-json','./...'])
run('architecture-boundaries',['python3','feature-specs/tools/step12_dependency_audit.py'])
run('public-contracts',['python3','feature-specs/tools/step15_public.py'],{'GOCQL_STEP15_CHECKS':str(OUT)})
run('declarations',['go','run','feature-specs/tools/step12_declarations.go'])
run('replay-operations',['go','test','-json','-race','-count=3','-run','^TestSingleConnectionReplay$','./tests/bench'])
run('replay-benchmark',['go','test','-run','^$','-bench','^BenchmarkSingleConnection(Select|Insert)$','-benchtime=1x','-count=1','-benchmem','./tests/bench'])
run('docs-version-tests',['python3','-m','unittest','discover','-s','docs','-p','test_*.py'])
run('docs-build',['uv','run','--project','docs','--frozen','sphinx-build','-W','-b','html','docs/source',str(OUT/'docs-html')])
run('cost-comparison',['go','test','-tags','unit','-run','^$','-bench','^BenchmarkStep07ExecutionStorage$','-benchmem','-benchtime=20x','-count=3','.'])
run('cost-fragmented',['go','test','-tags','unit bench','-run','^$','-bench','Benchmark(V2ExclusiveTransport|V2ExclusiveRowsScaling|V2StreamFinalStorage|V2StreamingNestedElements|V2RowDestinationFragments|Step08IndependentStreamOracle)$','-benchmem','-benchtime=10x','-count=3','.','./internal/codec'])

targets=[('./internal/wire','FuzzStreamCells','30s'),('./internal/frame','FuzzStreamEnvelope','30s'),('./internal/segment','FuzzValidatedUnits','30s'),('./internal/codec','FuzzV2StreamingValues','30s'),('./internal/codec','FuzzV2ScalarUTF8','30s'),('.','FuzzV2ExclusiveIncrementalFraming','30s'),('.','FuzzStep07StreamExecutionBoundary','30s'),('.','FuzzV2PagingHistoryBudgets','30s'),('.','FuzzV2PageDestinationFraming','30s'),('.','FuzzStep06TabletPayload','30s'),('./internal/wirecompress','FuzzSpanCompression','30s'),('.','FuzzStep14StreamingCancellationLifetime','60s'),('./internal/wire','FuzzCursorTake','30s')]
for package,target,budget in targets:
    cache=OUT.parent/'fuzz-corpus'/target
    if not any(c['name']=='fuzz-'+target and c['exit_code']==0 for c in checks):
        assert not cache.exists(),'fresh campaign cache must not already exist'
        cache.mkdir(parents=True)
    run('fuzz-'+target,['go','test','-tags','unit','-run','^$','-fuzz','^'+target+'$','-fuzztime='+budget,'-parallel=2','-timeout=3m',package,'-args','-test.fuzzcachedir='+str(cache)])
print(json.dumps(dict(source_sha256=binding,inputs=len(source),checks=len(checks))),flush=True)
