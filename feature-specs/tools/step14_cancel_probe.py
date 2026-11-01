# Copyright 2026 The Gocql Authors. All rights reserved.
# Use of this source code is governed by a BSD-style license.
"""Exercise first/repeated/late cancellation on one exact locked native fixture."""
import hashlib
import json
import os
from pathlib import Path
import signal
import subprocess
import time

ROOT=Path(__file__).resolve().parents[2]
OUT=Path('/extra/scylladb/gocql-api-test-cluster/step14/verified/cancel-probe')
OUT.mkdir(parents=True,exist_ok=True)
config=json.loads((OUT.parent/'stable-config.json').read_text())
lane=config['lanes'][0];lane['ccm']=False
marker=OUT/'marker'
lane['modes']=[dict(name='signal-lifetime',tests='^TestStep08HarnessLifecycle$',args=['-proto=4'],environment=dict(GOCQL_FIXTURE_ACTION='cancel',GOCQL_FIXTURE_MARKER=str(marker)))]
path=OUT/'config.json';path.write_text(json.dumps(config,indent=2)+'\n')
receipt=OUT/'native.json';log=OUT/'runner.log'
command=['/extra/scylladb/scylladb/.venv/bin/python','tests/fixtures/native.py','--config',str(path),'--state','/extra/scylladb/gocql-api-test-cluster/step08','--output',str(receipt)]
sent=[]
with log.open('wb') as output:
    runner=subprocess.Popen(command,cwd=ROOT,stdout=output,stderr=subprocess.STDOUT,start_new_session=True)
    start=time.monotonic();first=False;phases=set()
    while runner.poll() is None:
        if time.monotonic()-start>360:raise RuntimeError('bounded cancellation probe timeout; inspect exact retained runner '+str(runner.pid))
        data=json.loads(receipt.read_text()) if receipt.exists() else {}
        if marker.exists() and not first:
            os.kill(runner.pid,signal.SIGTERM);sent.append(dict(signal=15,phase='testing',time=time.monotonic()-start));first=True
        phase=data.get('phase')
        if first and phase in ('cleanup','stop-join') and phase not in phases:
            for sig in (signal.SIGTERM,signal.SIGINT):
                os.kill(runner.pid,sig);sent.append(dict(signal=int(sig),phase=phase,time=time.monotonic()-start));time.sleep(.02)
            phases.add(phase)
        time.sleep(.01)
    status=runner.wait()
data=json.loads(receipt.read_text())
assert first and phases=={'cleanup','stop-join'},sent
assert status!=0 and data['status']=='failed' and data['cancel_requested']
assert all(data['lanes'][0]['joined'].values()) and data['lanes'][0]['endpoints_refused']
assert any(c['cancelled'] and c['exit_code']!=0 for c in data['commands'])
assert any(c['name'].endswith('-cleanup') and c['exit_code']==0 for c in data['commands'])
for c in data['commands']:
    assert hashlib.sha256(Path(c['log']).read_bytes()).hexdigest()==c['sha256']
    try:os.kill(c['pid'],0)
    except ProcessLookupError:pass
    else:raise AssertionError('test/cleanup child survived join')
report=dict(command=command,runner_pid=runner.pid,runner_exit=status,source_sha256=data['source_sha256'],receipt=str(receipt),receipt_sha256=hashlib.sha256(receipt.read_bytes()).hexdigest(),log=str(log),log_sha256=hashlib.sha256(log.read_bytes()).hexdigest(),signals_sent=sent,phases=data['signals'],expected_cancelled_receipt=True,test_and_cleanup_children_gone=True,all_nodes_joined=True,endpoints_refused=True,owned_namespace=marker.read_text())
(OUT/'verification.json').write_text(json.dumps(report,indent=2)+'\n');print(json.dumps(report),flush=True)
