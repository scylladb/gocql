# Copyright 2026 The Gocql Authors. All rights reserved.
# Use of this source code is governed by a BSD-style license.
"""Run all native modes with exact owned handles; never restart an observed run."""
import hashlib,json,subprocess,time
from pathlib import Path

ROOT=Path(__file__).resolve().parents[2]
OUT=Path('/extra/scylladb/gocql-api-test-cluster/step17/release')
PYTHON='/extra/scylladb/scylladb/.venv/bin/python'
summary=[]
def run(lane,config):
    receipt=OUT/(lane+'.json');log=OUT/(lane+'-runner.log')
    assert not receipt.exists(),'existing exact run must be inspected, not restarted: '+str(receipt)
    command=[PYTHON,'feature-specs/tools/step17_native.py','--config',str(config),'--state','/extra/scylladb/gocql-api-test-cluster/step08','--output',str(receipt)]
    start=time.monotonic()
    with log.open('wb') as output:
        code=subprocess.run(command,cwd=ROOT,stdout=output,stderr=subprocess.STDOUT).returncode
    row=dict(lane=lane,command=command,exit_code=code,seconds=time.monotonic()-start,receipt=str(receipt),runner_log=str(log),runner_log_sha256=hashlib.sha256(log.read_bytes()).hexdigest())
    summary.append(row);(OUT/'native-matrix.json').write_text(json.dumps(summary,indent=2)+'\n');print(json.dumps(row),flush=True)
    data=json.loads(receipt.read_text())
    assert not code and data['status']=='passed',(code,data.get('error'),receipt)
    assert all(data['lanes'][0]['joined'].values()) and data['lanes'][0]['endpoints_refused']

run('stable',OUT/'stable-config.json')
run('development',OUT/'development-config.json')
run('stable-v3tls',OUT/'stable-v3tls-config.json')
configuration=json.loads((OUT/'cassandra-config.json').read_text());base=configuration['lanes'][0]
modes={m['name']:m for m in base['modes']}
for mode in ['v5-no-compression','v5-lz4','v5-no-compression-tls','v5-lz4-tls','v4-no-compression','v4-no-compression-tls','v4-lz4','v4-lz4-tls']:
    config=OUT/('cassandra-'+mode+'-config.json')
    config.write_text(json.dumps({'lanes':[dict(base,modes=[modes[mode]])]},indent=2)+'\n')
    run('cassandra-'+mode,config)
