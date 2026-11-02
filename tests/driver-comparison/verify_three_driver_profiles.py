#!/usr/bin/env python3
"""Join the comparison, run untimed adapter race checks, and retain an audit."""
import gzip
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import shutil
import signal
import socket
import subprocess
import time

HERE=Path(__file__).resolve().parent
c=json.loads(Path('/tmp/gocql-three-driver-current.json').read_text())
out=Path(c['output']);cache=Path(c['cache'])
context=json.loads((out/'context.json').read_text());server=context['server_pid']
spec=importlib.util.spec_from_file_location('comparison',HERE/'run.py')
r=importlib.util.module_from_spec(spec);spec.loader.exec_module(r);r.CACHE=cache
os.sched_setaffinity(0,{2,18})
deadline=time.monotonic()+240
while json.loads((out/'state.json').read_text())['status']!='complete':
    if time.monotonic()>deadline:raise RuntimeError('comparison did not complete within verification wait')
    time.sleep(1)

race=out/'race-checks';race.mkdir()
checks=[]
for stock in (False,True):
    path=race/f'correctness-{stock}.json'
    rc=r.client(path,'127.0.0.2,127.0.0.3,127.0.0.4','scyllago-race','check',**{'scylla-stock-iterator':str(stock).lower()})
    checks.append({'mode':'check','stock_iterator':stock,'returncode':rc})
for scenario,workers,stock in [('read',64,False),('paged',4,False),('paged',4,True),('write',64,False)]:
    path=race/f'{scenario}-{stock}.json'
    rc=r.client(path,'127.0.0.2,127.0.0.3,127.0.0.4','scyllago-race','run',scenario,concurrency=workers,duration='2s',warmup='0s',rate=0,output=path,**{'scylla-stock-iterator':str(stock).lower()})
    valid=False
    if path.exists():valid=r.valid(r.compact(path))
    checks.append({'mode':scenario,'stock_iterator':stock,'returncode':rc,'valid':valid})
r.write(out/'race-checks.json',checks)
print(json.dumps({'race_checks':checks}),flush=True)

# Shutdown only the emulator PID recorded at this experiment's creation.
assert Path(f'/proc/{server}/exe').resolve().name=='gocql-three-driver-emulator'
os.kill(server,signal.SIGTERM)
ports={}
for _ in range(100):
    ports={}
    for host,port in [('127.0.0.2',9042),('127.0.0.3',9042),('127.0.0.4',9042),('127.0.0.1',19080)]:
        with socket.socket() as s:
            s.settimeout(.2);ports[f'{host}:{port}']=s.connect_ex((host,port))!=0
    if all(ports.values()):break
    time.sleep(.1)
assert all(ports.values())
subprocess.run(['python3',str(HERE/'analyze_emulator_profiles.py'),str(out)],check=True)
subprocess.run(['python3',str(HERE/'report_three_driver_profiles.py'),str(out)],check=True)
observations=json.loads((out/'observations.json').read_text())
assert len(observations)==24
processes=list(out.glob('*/measurement.json.process.json'))
assert len(processes)==24
for path in processes:
    proc=json.loads(path.read_text());assert proc['returncode']==0 and proc['masks']==['8'] and proc['checks']>0 and not proc['affinity_error']
for path in out.glob('*/measurement.json.log'):
    if path.stat().st_size>1<<20:
        original=path.read_bytes()
        with gzip.open(str(path)+'.gz','wb') as f:f.write(original)
        assert gzip.decompress(Path(str(path)+'.gz').read_bytes())==original
        path.unlink()
for name,expected in context['binary_hashes'].items():assert r.sha(cache/name)==expected
for name,expected in context['source_files'].items():assert r.sha(out/'implementation'/name)==expected
assert subprocess.check_output(['git','-C',c['v2_source'],'diff','--name-only'],text=True).strip()==''
assert subprocess.check_output(['git','-C',c['scylla_source'],'diff','--name-only'],text=True).strip()==''
race_ok=all(row['returncode']==0 and row.get('valid',True) for row in checks)
audit={'status':'passed' if race_ok else 'timed measurements passed; race checks need inspection','observations':24,'seconds_each':30,'errors':sum(x['errors'] for x in observations),'timeouts':sum(x['timeouts'] for x in observations),'rejected':sum(x['rejected'] for x in observations),'affinity_process_records':24,'binary_and_source_hashes_verified':True,'emulator_stopped':True,'ports_closed':ports,'untimed_adapter_race_checks_passed':race_ok,'emulator_race_tests_passed':True,'production_driver_diff':'empty','upstream_existing_file_diff':'empty; one benchmark API shim added'}
r.write(out/'audit.json',audit)
for name in ('report_three_driver_profiles.py','verify_three_driver_profiles.py'):
    shutil.copyfile(HERE/name,out/'implementation'/name)
manifest={str(p.relative_to(out)):r.sha(p) for p in out.rglob('*') if p.is_file() and p.name!='SHA256SUMS.json'}
r.write(out/'SHA256SUMS.json',manifest)
print(json.dumps(audit),flush=True)
