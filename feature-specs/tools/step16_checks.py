# Copyright 2026 The Gocql Authors. All rights reserved.
# Use of this source code is governed by a BSD-style license.
"""Source-bound focused independent-review checks; no fixture/publication actions."""
import hashlib,json,os,subprocess,time
from pathlib import Path
ROOT=Path(__file__).resolve().parents[2]
OUT=Path('/extra/scylladb/gocql-api-test-cluster/step16/final-checks')
OUT.mkdir(parents=True,exist_ok=True)
sha=lambda p:hashlib.sha256(Path(p).read_bytes()).hexdigest()
def snapshot():
 paths=subprocess.check_output(['git','ls-files','--cached','--others','--exclude-standard','-z'],cwd=ROOT).decode().split('\0')
 return {p:sha(ROOT/p) for p in sorted(set(paths)) if p and not p.startswith('feature-specs/') and (ROOT/p).is_file() and (ROOT/p).suffix in ('.go','.mod','.sum','.py','.java','.cql','.crt','.key','.p12','.cnf','.sh')}
source=snapshot();(OUT/'source.json').write_text(json.dumps(source,sort_keys=True,indent=2)+'\n');binding=sha(OUT/'source.json');checks=[]
def run(name,cmd,extra=None):
 env=dict(os.environ,GOMAXPROCS='4');env.update(extra or {});log=OUT/(name+'.log');start=time.monotonic()
 with log.open('wb')as f:code=subprocess.run(cmd,cwd=ROOT,env=env,stdout=f,stderr=subprocess.STDOUT).returncode
 row=dict(name=name,command=cmd,environment=dict(GOMAXPROCS='4',**(extra or {})),exit_code=code,seconds=time.monotonic()-start,log=str(log),sha256=sha(log),source_sha256=binding)
 checks.append(row);(OUT/'checks.json').write_text(json.dumps(checks,indent=2)+'\n');print(json.dumps(row),flush=True)
 if code:raise SystemExit(code)
run('affected-correctness',['go','test','-json','-tags','unit','-count=1','-timeout=6m','./internal/wire','./internal/codec','./internal/wirecompress','./internal/frame','./internal/segment','./internal/protocol','./internal/transport','./internal/pool','./internal/metadata','./internal/routing','./internal/cluster','./internal/execution','.'])
run('affected-race',['go','test','-json','-race','-tags','unit','-count=1','-timeout=6m','./internal/wirecompress','./internal/protocol','./internal/transport','./internal/routing','.'])
run('operation-counters',['go','test','-json','-tags','unit gocql_perf','-count=1','-run','^Test(Step15|Step16)','./internal/wire','./internal/codec','./internal/wirecompress','.'])
run('386',['go','test','-json','-tags','unit','-count=1','-run','^TestStep16','./internal/transport','./internal/wirecompress','./internal/protocol','.'],{'GOARCH':'386','CGO_ENABLED':'0'})
run('affected-benchmarks',['go','test','-tags','unit','-run','^$','-bench','^Benchmark(TypedBuildRequest|Step06FirstTarget|SpanCompression)$','-benchtime=100ms','-count=3','-benchmem','./internal/wirecompress','.'])
run('all-tag-build',['go','test','-run','^$','-tags','all ccm bench gocql_perf','./...'])
run('vet',['go','vet','-tags','unit integration gocql_debug bench gocql_perf','./...'])
run('dependency-catalog',['go','list','-deps','-json','./...'])
run('public-contracts',['python3','feature-specs/tools/step15_public.py'],{'GOCQL_STEP15_CHECKS':str(OUT)})
run('module-verify',['go','mod','verify'])
run('module-drift',['make','check-go-mod-drift'])
run('repository-unit-race',['go','test','-json','-race','-tags','unit','-count=1','-timeout=5m','./...'])
assert snapshot()==source,'source changed during checks'
print(json.dumps(dict(source_sha256=binding,inputs=len(source),checks=len(checks))),flush=True)
