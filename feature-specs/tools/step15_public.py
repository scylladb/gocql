# Copyright 2026 The Gocql Authors. All rights reserved.
# Use of this source code is governed by a BSD-style license.
"""Run normal-build go/types reachability and external interface contracts."""
import hashlib,json,os,shutil,subprocess,time
from pathlib import Path
ROOT=Path(__file__).resolve().parents[2];OUT=Path(os.environ.get('GOCQL_STEP15_CHECKS','/extra/scylladb/gocql-api-test-cluster/step15/final-checks'))/'api-audit';OUT.mkdir(parents=True,exist_ok=True)
shutil.copy2('/tmp/gocql-step13-audit/accepted-declarations.json',OUT/'accepted-declarations.json')
commands=[]
def run(name,argv):
 p=OUT/(name+'.json');start=time.monotonic()
 with p.open('wb')as h:status=subprocess.run(argv,cwd=ROOT,env=dict(os.environ,GOCQL_STEP13_AUDIT_DIR=str(OUT),GOMAXPROCS='4'),stdout=h,stderr=subprocess.STDOUT).returncode
 commands.append(dict(command=argv,exit_code=status,seconds=time.monotonic()-start,log=str(p),sha256=hashlib.sha256(p.read_bytes()).hexdigest()))
 if status:raise RuntimeError((name,p))
 return p
run('packages-all',['go','list','-deps','-export','-json','-tags','all,ccm,bench','./...'])
p=run('public-audit',['go','run','feature-specs/tools/step13_public_audit.go']);r=json.loads(p.read_text());reachable=r['reachable_internal_types'];assert 'github.com/scylladb/gocql/v2/internal/wire.Builder' not in reachable;assert 'github.com/scylladb/gocql/v2/internal/wire.ReceiveCache' not in reachable
assert not r['issues'] and len(r['public_exports'])==507 and len(r['thin_facade_functions'])==130
receipt=dict(commands=commands,public_exports=507,thin_forwarders=130,mutable_arena_or_receive_cache_reachable=False,external_consumer_exit_code=r['external_implementation_consumer']['exit_code'])
(OUT/'verification.json').write_text(json.dumps(receipt,indent=2)+'\n');print(json.dumps(receipt),flush=True)
