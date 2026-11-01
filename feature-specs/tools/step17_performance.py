# Copyright 2026 The Gocql Authors. All rights reserved.
# Use of this source code is governed by a BSD-style license.
"""Final-tree release checks. Every receipt binds exact inputs and complete exits."""
import hashlib,json,os,platform,subprocess,time
from pathlib import Path

ROOT=Path(__file__).resolve().parents[2]
OUT=Path('/extra/scylladb/gocql-api-test-cluster/step17/release/performance')
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
    env=dict(os.environ,GOMAXPROCS='4');env.update(extra or {})
    log=OUT/(name+'.log');start=time.monotonic()
    with log.open('wb') as output:
        code=subprocess.run(cmd,cwd=ROOT,env=env,stdout=output,stderr=subprocess.STDOUT).returncode
    row=dict(name=name,command=cmd,environment=dict(GOMAXPROCS='4',**(extra or {})),exit_code=code,seconds=time.monotonic()-start,log=str(log),sha256=sha(log),source_sha256=binding)
    checks[:]=[c for c in checks if c['name']!=name]+[row]
    (OUT/'checks.json').write_text(json.dumps(checks,indent=2)+'\n');print(json.dumps(row),flush=True)
    assert snapshot()==source,'runtime inputs changed during checks'
    if code:raise SystemExit(code)

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

print(json.dumps(dict(source_sha256=binding,inputs=len(source),checks=len(checks))),flush=True)
