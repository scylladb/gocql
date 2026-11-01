# Copyright 2026 The Gocql Authors. All rights reserved.
# Use of this source code is governed by a BSD-style license.
"""Run every mandatory Cassandra whole-package mode on fresh owned fixture data."""
import json
from pathlib import Path
import subprocess

ROOT=Path(__file__).resolve().parents[2]
OUT=Path('/extra/scylladb/gocql-api-test-cluster/step14/verified')
configuration=json.loads((OUT/'cassandra-config.json').read_text())
base=configuration['lanes'][0]
modes={mode['name']:mode for mode in base['modes']}
order=['v5-no-compression','v5-lz4','v5-no-compression-tls','v5-lz4-tls','v4-no-compression','v4-no-compression-tls','v4-lz4','v4-lz4-tls']
summary=[]
for mode in order:
    lane=dict(base,modes=[modes[mode]])
    config=OUT/('cassandra-'+mode+'-config.json');config.write_text(json.dumps({'lanes':[lane]},indent=2)+'\n')
    receipt=OUT/('cassandra-'+mode+'.json');log=OUT/('cassandra-'+mode+'-runner.log')
    command=['/extra/scylladb/scylladb/.venv/bin/python','tests/fixtures/native.py','--config',str(config),'--state','/extra/scylladb/gocql-api-test-cluster/step08','--output',str(receipt)]
    with log.open('wb') as out:status=subprocess.run(command,cwd=ROOT,stdout=out,stderr=subprocess.STDOUT).returncode
    data=json.loads(receipt.read_text())
    summary.append(dict(mode=mode,command=command,exit_code=status,receipt=str(receipt),runner_log=str(log),source_sha256=data['source_sha256']))
    (OUT/'cassandra-modes.json').write_text(json.dumps(summary,indent=2)+'\n');print(json.dumps(summary[-1]),flush=True)
    if status or data['status']!='passed':raise SystemExit(status or 1)
    assert all(data['lanes'][0]['joined'].values()) and data['lanes'][0]['endpoints_refused']
