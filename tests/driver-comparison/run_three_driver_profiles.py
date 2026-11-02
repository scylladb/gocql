#!/usr/bin/env python3
"""Three-driver local comparison, including stock scylla-go-driver paging."""
import concurrent.futures
import importlib.util
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import time

HERE=Path(__file__).resolve().parent
spec=importlib.util.spec_from_file_location('local_profiles',HERE/'run_emulator_profiles.py')
p=importlib.util.module_from_spec(spec);spec.loader.exec_module(p)
r=p.r
context=json.loads(Path('/tmp/gocql-three-driver-current.json').read_text())
r.CACHE=Path(context['cache'])
OUT=p.OUT
SERVER=p.SERVER

def main():
    os.sched_setaffinity(0,{2,18})
    assert OUT==Path(context['output'])
    OUT.mkdir(parents=True,exist_ok=False)
    implementation=OUT/'implementation';implementation.mkdir()
    for name in ('main.go','profile.go','old.go','v2.go','scyllago.go','core_test.go','go.mod','go.sum'):
        shutil.copyfile(Path(context['harness'])/name,implementation/name)
    for name in ('run_three_driver_profiles.py','run_emulator_profiles.py','analyze_emulator_profiles.py','emulator/main.go','emulator/main_test.go'):
        destination=implementation/name;destination.parent.mkdir(exist_ok=True,parents=True);shutil.copyfile(HERE/name,destination)
    shutil.copyfile(Path(context['scylla_source'])/'benchmark_access.go',implementation/'benchmark_access.go')
    assert subprocess.check_output(['git','-C',context['scylla_source'],'diff','--name-only'],text=True).strip()==''
    assert subprocess.check_output(['git','-C',context['v2_source'],'diff','--name-only'],text=True).strip()==''
    manifest=dict(context)
    manifest.update(seconds=30,repeats=2,expected_observations=24,client_cpu=8,client_gomaxprocs=1,server_cpu=[0,1,4,5],server_gomaxprocs=4,server_pid=SERVER,
        binary_hashes={name:r.sha(r.CACHE/name) for name in ('master','v2','scyllago')},
        emulator_sha256=r.sha(Path('/tmp/gocql-three-driver-emulator')),emulator_source_sha256=r.sha(HERE/'emulator/main.go'),
        compiler=subprocess.check_output(['go','version'],text=True).strip(),
        master_head=subprocess.check_output(['git','-C',context['master_source'],'rev-parse','HEAD'],text=True).strip(),
        v2_head=subprocess.check_output(['git','-C',context['v2_source'],'rev-parse','HEAD'],text=True).strip(),
        source_files={str(f.relative_to(implementation)):r.sha(f) for f in implementation.rglob('*') if f.is_file()},
        backend='Synthetic native-v4 TCP emulator; three hosts with one advertised shard each; shard-aware port 9042; no metadata-ID extension, replication, disk I/O or consistency work.',
        scylla_adaptations=['Only new build-tagged benchmark_access.go added to ce81923; no existing upstream file edited.',
            'Raw int32/blob value access and per-worker Query clone; prepared consistency set to LOCAL_QUORUM because upstream Prepare hard-codes ALL.',
            'Explicit paging forwards page state through original Query.Exec and execution callback; no prefetch.',
            'Stock upstream iterator measured separately with mandatory prefetch.',
            'Unbound admin/cleanup queries use public transport outside timing: upstream NTS leaves fallback node lists empty.',
            'Caller supplies 10-second logical operation deadline; native writer batching and 1ms empty-queue sleep unchanged.',
            'Owned upstream result-cell bytes are consumed directly; no additional AsBlob copy.'],
        comparison_limits=['Master/V2 coalescing disabled; upstream scylla-go-driver coalescing cannot be disabled through its API.',
            'No claim this benchmark shim is an unmodified public API or a complete supported API implementation.',
            'CPU includes identical SHA256 row validation and common harness accounting.',
            'Closed-loop throughput; not a fixed-rate sustainable-capacity measurement.'],started=r.now())
    r.write(OUT/'context.json',manifest)
    correctness=[('master',False,{}),('v2',False,{}),('v2',True,{}),('scyllago',False,{}),('scyllago',False,{'scylla-stock-iterator':'true'})]
    for index,(driver,stream,options) in enumerate(correctness):
        path=OUT/f'correctness-{index}-{driver}.json'
        if r.client(path,p.HOSTS,driver,'check',stream=stream,**options):raise RuntimeError(path)
    cases=[('point-master','master','read',False,64,{}),('point-scyllago','scyllago','read',False,64,{}),('point-v2-buffered','v2','read',False,64,{}),('point-v2-stream','v2','read',True,4,{}),
        ('paged-master','master','paged',False,4,{}),('paged-scyllago-explicit','scyllago','paged',False,4,{}),('paged-scyllago-stock','scyllago','paged',False,4,{'scylla-stock-iterator':'true'}),('paged-v2-buffered','v2','paged',False,4,{}),('paged-v2-stream','v2','paged',True,4,{}),
        ('write-master','master','write',False,64,{}),('write-scyllago','scyllago','write',False,64,{}),('write-v2','v2','write',False,64,{})]
    records=[]
    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
        for repeat in (1,2):
            for name,driver,scenario,stream,concurrency,options in (cases if repeat==1 else list(reversed(cases))):
                d=OUT/f'{name}-{repeat}';d.mkdir()
                print(f'WARM {d.name}',flush=True)
                if r.client(d/'warm.json',p.HOSTS,driver,'warm',scenario,stream,**options):raise RuntimeError(d)
                before=p.stats();cpu_before=p.proc();start=time.monotonic()
                future=executor.submit(p.server_profile,d/'emulator.cpu.pprof')
                print(f'RUN {d.name}',flush=True)
                rc=r.client(d/'measurement.json',p.HOSTS,driver,'run',scenario,stream,concurrency=concurrency,duration='30s',warmup='0s',rate=0,output=d/'measurement.json',**{'profile-prefix':d/'measurement'},**options)
                future.result();elapsed=time.monotonic()-start
                after=p.stats();cpu_after=p.proc()
                r.write(d/'emulator.json',{'before':before,'after':after,'cpu_before':cpu_before,'cpu_after':cpu_after,'elapsed':elapsed})
                if rc:raise RuntimeError(f'client failed {d}')
                result=r.compact(d/'measurement.json');m=result['measurement']
                if not r.valid(result) or m['timeouts'] or m['rejected'] or after['failures']!=before['failures']:raise RuntimeError(f'invalid {d}')
                record={'case':name,'repeat':repeat,'throughput':m['throughput'],'p99_us':m['latency']['p99_us'],
                    'cpu_us_per_op':m['cpu_seconds_including_drain']*1e6/m['completed'],
                    'bytes_per_op':m['alloc_bytes']/m['completed'],'allocs_per_op':m['mallocs']/m['completed'],
                    'server_cpu_cores':sum(max(0,cpu_after[k]-cpu_before.get(k,0)) for k in cpu_after)/elapsed,
                    'errors':m['errors'],'timeouts':m['timeouts'],'rejected':m['rejected']}
                records.append(record);r.write(OUT/'observations.json',records);p.summarize(records)
                r.write(OUT/'state.json',{'status':'running','completed':len(records),'expected':24,'latest':d.name})
                print(json.dumps(record),flush=True)
    r.write(OUT/'state.json',{'status':'complete','completed':len(records),'expected':24,'finished':r.now()})

if __name__=='__main__':main()
