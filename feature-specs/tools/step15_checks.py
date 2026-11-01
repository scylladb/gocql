# Copyright 2026 The Gocql Authors. All rights reserved.
# Use of this source code is governed by a BSD-style license.
"""Retain final-source performance/correctness receipts, without native overlap."""
import hashlib,json,os,subprocess,time
from pathlib import Path
ROOT=Path(__file__).resolve().parents[2]
OUT=Path(os.environ.get('GOCQL_STEP15_CHECKS','/extra/scylladb/gocql-api-test-cluster/step15/checks'));OUT.mkdir(exist_ok=True)
checks=[]
def sha(p):return hashlib.sha256(Path(p).read_bytes()).hexdigest()
def source():
 paths=subprocess.check_output(['git','ls-files','--cached','--others','--exclude-standard','-z'],cwd=ROOT).decode().split('\0')
 return {name:sha(ROOT/name) for name in sorted(paths) if name and (ROOT/name).is_file() and not name.startswith('feature-specs/') and '__pycache__' not in name and (ROOT/name).suffix in ('.go','.mod','.sum','.py','.java','.cql','.crt','.key','.p12','.cnf','.sh')}
(OUT/'source.json').write_text(json.dumps(source(),sort_keys=True,indent=2)+'\n');snapshot=sha(OUT/'source.json')
def run(name,command,env=None):
 environment=dict(os.environ,GOMAXPROCS='4');environment.update(env or {});path=OUT/(name+'.log');start=time.monotonic()
 with path.open('wb') as log:status=subprocess.run(command,cwd=ROOT,env=environment,stdout=log,stderr=subprocess.STDOUT).returncode
 row=dict(name=name,command=command,environment=dict(GOMAXPROCS='4',**(env or {})),exit_code=status,seconds=time.monotonic()-start,log=str(path),sha256=sha(path),source_sha256=snapshot)
 checks.append(row);(OUT/'checks.json').write_text(json.dumps(checks,indent=2)+'\n');print(json.dumps(row),flush=True)
 if status:raise SystemExit(status)
run('operation-counters',['go','test','-v','-count=1','-tags','unit gocql_perf','-run','^TestStep15','./internal/wire','./internal/codec','./internal/wirecompress','.'])
run('affected-correctness',['go','test','-json','-count=1','-tags','unit','-timeout=6m','./internal/wire','./internal/codec','./internal/segment','./internal/wirecompress','./internal/routing','./internal/paging','./internal/execution','.'])
run('unit-race',['go','test','-json','-race','-tags','unit','-count=1','-timeout=6m','./...'])
run('counters-race',['go','test','-race','-tags','unit gocql_perf','-count=1','-run','^TestStep15','./internal/wire','./internal/codec','./internal/wirecompress','.'])
run('bench-foundations',['go','test','-p=1','-tags','unit','-run','^$','-bench','Benchmark(V2|Step15|RangeLookup|CursorCells|Builder|BorrowLarge|CursorCross|Lease|SpanCompression|ValidatedUnits|StreamSegments)','-benchtime=150ms','-count=3','-benchmem','./internal/wire','./internal/codec','./internal/routing','./internal/segment','./internal/wirecompress','./internal/murmur'])
run('bench-runtime',['go','test','-tags','unit','-run','^$','-bench','Benchmark(Step15|TypedBuild|V2|Step05|Step06|Step07|Step10)','-benchtime=150ms','-count=3','-benchmem','.'])
run('contention-profiles',['go','test','-tags','unit','-run','^$','-bench','^BenchmarkStep15WarmConcurrentBindings$','-benchtime=1s','-count=1','-benchmem','-cpuprofile='+str(OUT/'concurrency-cpu.pprof'),'-mutexprofile='+str(OUT/'concurrency-mutex.pprof'),'-blockprofile='+str(OUT/'concurrency-block.pprof'),'.'])
run('codec-cpu-profile',['go','test','-tags','unit','-run','^$','-bench','BenchmarkStep15(DynamicRows|NarrowWideRows|LazyAndMaterialized)$','-benchtime=100ms','-count=1','-cpuprofile='+str(OUT/'codec-cpu.pprof'),'-memprofile='+str(OUT/'codec-allocs.pprof'),'./internal/codec'])
run('transport-cpu-profile',['go','test','-tags','unit','-run','^$','-bench','Benchmark(V2TLSWriteAmplification|Step15SocketWrites)$','-benchtime=100ms','-count=1','-cpuprofile='+str(OUT/'transport-cpu.pprof'),'.'])
# OS syscall diagnostics execute a compiled binary so compiler file I/O is absent.
run('socket-binary',['go','test','-c','-tags','unit','-o',str(OUT/'socket.test'),'.'])
run('syscall-harness-control',['strace','-f','-c','-e','trace=read,write,readv,writev,recvfrom,sendto','-o',str(OUT/'syscall-harness-control.strace'),str(OUT/'socket.test'),'-test.run=^$','-test.bench=^$','-test.count=1'])
for tls in ('false','true'):
 for spans in (8,1024,4096):
  for mixed in ('false','true'):
   name=f'syscalls-tls{tls}-spans{spans}-mixed{mixed}'
   run(name,['strace','-f','-c','-e','trace=read,write,readv,writev,recvfrom,sendto','-o',str(OUT/(name+'.strace')),str(OUT/'socket.test'),'-test.run=^$','-test.bench=^BenchmarkStep15SocketWrites/tls'+tls+'/spans'+str(spans)+'/mixed'+mixed+'$','-test.benchtime=10x','-test.count=1','-test.benchmem'])
for target in ['FuzzV2ScalarUTF8','FuzzV2StreamingValues']:
 run('fuzz-'+target,['go','test','-tags','unit','-run','^$','-fuzz','^'+target+'$','-fuzztime=30s','-parallel=2','-timeout=2m','./internal/codec'])
run('fuzz-FuzzCursorTake',['go','test','-tags','unit','-run','^$','-fuzz','^FuzzCursorTake$','-fuzztime=30s','-parallel=2','-timeout=2m','./internal/wire'])
run('fuzz-FuzzSpanCompression',['go','test','-tags','unit','-run','^$','-fuzz','^FuzzSpanCompression$','-fuzztime=30s','-parallel=2','-timeout=2m','./internal/wirecompress'])
run('vet',['go','vet','-tags','unit integration gocql_debug bench gocql_perf','./...'])
run('386',['go','test','-tags','unit','-count=1','-timeout=6m','./internal/wire','./internal/codec','./internal/wirecompress','.'],{'GOARCH':'386','CGO_ENABLED':'0'})
run('all-tags',['go','test','-run','^$','-tags','all ccm bench gocql_perf','./...'])
run('spec-verifier',['python3','feature-specs/verify.py'])
run('module-verify',['go','mod','verify'])
run('module-drift',['make','check-go-mod-drift'])
run('architecture-boundaries',['python3','feature-specs/tools/step12_dependency_audit.py'])
run('public-package-metadata',['go','list','-json','-tags','all,ccm,bench','./...'])
run('public-api-and-external-contracts',['python3','feature-specs/tools/step15_public.py'])
run('declarations',['go','run','feature-specs/tools/step12_declarations.go'])
assert source()==json.loads((OUT/'source.json').read_text()),'source changed during checks'
