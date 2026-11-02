#!/usr/bin/env python3
"""Compare frozen clients against the native-v4 fixture emulator; no database."""
import concurrent.futures
import datetime
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import statistics
import subprocess
import sys
import time
import urllib.request

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent.parent
spec = importlib.util.spec_from_file_location('comparison', HERE / 'run.py')
r = importlib.util.module_from_spec(spec)
spec.loader.exec_module(r)
r.CACHE = ROOT / '.codex-cache/driver-comparison/master-comparison-20261101T222337Z'
HOSTS = '127.0.0.2,127.0.0.3,127.0.0.4'
OUT = Path(sys.argv[1]).resolve()
SERVER = int(sys.argv[2])

def stats():
    with urllib.request.urlopen('http://127.0.0.1:19080/stats', timeout=5) as f:
        return json.load(f)

def proc():
    data = {}
    for task in Path(f'/proc/{SERVER}/task').glob('*'):
        try:
            fields = (task/'stat').read_text().rsplit(')', 1)[1].split()
            mask = next(x for x in (task/'status').read_text().splitlines() if x.startswith('Cpus_allowed_list:')).split(':', 1)[1].strip()
            if mask != '0-1,4-5':
                raise RuntimeError(f'emulator affinity {mask}')
            data[task.name] = (int(fields[11])+int(fields[12])) / os.sysconf('SC_CLK_TCK')
        except FileNotFoundError:
            pass
    return data

def server_profile(path):
    with urllib.request.urlopen('http://127.0.0.1:19080/debug/pprof/profile?seconds=30', timeout=45) as f:
        path.write_bytes(f.read())

def summarize(records):
    summary = {}
    for case in sorted({x['case'] for x in records}):
        chosen = [x for x in records if x['case'] == case]
        med = lambda k: statistics.median(x[k] for x in chosen)
        summary[case] = {k:med(k) for k in ('throughput','p99_us','cpu_us_per_op','bytes_per_op','allocs_per_op','server_cpu_cores')}
        summary[case].update(runs=len(chosen), throughput_range=[min(x['throughput'] for x in chosen),max(x['throughput'] for x in chosen)])
    r.write(OUT/'summary.json', summary)

def main():
    os.sched_setaffinity(0, {2,18})
    OUT.mkdir(parents=True, exist_ok=False)
    r.write(OUT/'context.json', {
        'kind':'native-v4 local emulator; no database, replication, persistence or consistency work',
        'client_cpu':8, 'client_gomaxprocs':1, 'server_cpu':[0,1,4,5], 'server_gomaxprocs':4,
        'hosts':HOSTS, 'server_pid':SERVER, 'seconds':30, 'repeats':2,
        'master_head':'1ceaac8172bda26e8b8d701ac356982d33729b27',
        'v2_head':subprocess.check_output(['git','rev-parse','HEAD'],cwd=ROOT,text=True).strip(),
        'binary_hashes':{n:r.sha(r.CACHE/n) for n in ('master','v2')},
        'emulator_sha256':r.sha(Path('/tmp/gocql-native-v4-emulator')),
        'source_sha256':r.sha(HERE/'emulator/main.go'),
        'write_note':'64 concurrent acknowledged writes against synthetic storage; not the earlier one-worker real-database comparison',
        'server_profile_scope':'30-second interval surrounding client launch; approximate alignment; client profile covers measured phase exactly',
        'started':r.now(),
    })
    for driver,stream in [('master',False),('v2',False),('v2',True)]:
        path=OUT/f'correctness-{driver}-{stream}.json'
        if r.client(path,HOSTS,driver,'check',stream=stream): raise RuntimeError(path)
    cases=[('point-master','master','read',False,64),('point-v2-buffered','v2','read',False,64),('point-v2-stream','v2','read',True,4),
           ('paged-master','master','paged',False,4),('paged-v2-buffered','v2','paged',False,4),('paged-v2-stream','v2','paged',True,4),
           ('write-master','master','write',False,64),('write-v2','v2','write',False,64)]
    records=[]
    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
        for repeat in (1,2):
            for name,driver,scenario,stream,concurrency in (cases if repeat==1 else list(reversed(cases))):
                d=OUT/f'{name}-{repeat}';d.mkdir()
                print(f'WARM {d.name}',flush=True)
                if r.client(d/'warm.json',HOSTS,driver,'warm',scenario,stream):raise RuntimeError(d)
                before=stats();cpu_before=proc();start=time.monotonic()
                future=executor.submit(server_profile,d/'emulator.cpu.pprof')
                print(f'RUN {d.name}',flush=True)
                rc=r.client(d/'measurement.json',HOSTS,driver,'run',scenario,stream,concurrency=concurrency,duration='30s',warmup='0s',rate=0,output=d/'measurement.json',**{'profile-prefix':d/'measurement'})
                future.result();elapsed=time.monotonic()-start
                after=stats();cpu_after=proc()
                r.write(d/'emulator.json',{'before':before,'after':after,'cpu_before':cpu_before,'cpu_after':cpu_after,'elapsed':elapsed})
                if rc:raise RuntimeError(f'client failed {d}')
                result=r.compact(d/'measurement.json');m=result['measurement']
                if not r.valid(result) or m['timeouts'] or m['rejected'] or after['failures']!=before['failures']:raise RuntimeError(f'invalid {d}')
                record={'case':name,'repeat':repeat,'throughput':m['throughput'],'p99_us':m['latency']['p99_us'],
                        'cpu_us_per_op':m['cpu_seconds_including_drain']*1e6/m['completed'],
                        'bytes_per_op':m['alloc_bytes']/m['completed'],'allocs_per_op':m['mallocs']/m['completed'],
                        'server_cpu_cores':sum(max(0,cpu_after[k]-cpu_before.get(k,0)) for k in cpu_after)/elapsed,
                        'errors':m['errors'],'timeouts':m['timeouts'],'rejected':m['rejected']}
                records.append(record);r.write(OUT/'observations.json',records);summarize(records)
                r.write(OUT/'state.json',{'status':'running','completed':len(records),'expected':16,'latest':d.name})
                print(json.dumps(record),flush=True)
    r.write(OUT/'state.json',{'status':'complete','completed':len(records),'expected':16,'finished':r.now()})

if __name__=='__main__':
    main()
