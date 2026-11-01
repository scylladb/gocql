# Copyright 2026 The Gocql Authors. All rights reserved.
# Use of this source code is governed by a BSD-style license.
"""Link the complete tag union and cross-compile it; execute no fixture tests."""
import hashlib,json,os,subprocess,time
from pathlib import Path
ROOT=Path(__file__).resolve().parents[2]
OUT=Path('/extra/scylladb/gocql-api-test-cluster/step17/release')
TAGS='all unit integration ccm bench gocql_debug gocql_perf'
rows=[]
for name,command,extra in [
 ('complete-tag-link',['go','test','-json','-tags',TAGS,'-run','^$','./...'],{}),
 ('complete-tag-arm64',['go','test','-exec','/bin/true','-tags',TAGS,'-run','^$','./...'],{'GOARCH':'arm64','CGO_ENABLED':'0'}),
 ('complete-tag-production-graph',['go','list','-deps','-json','-tags',TAGS,'./...'],{}),
]:
    log=OUT/(name+'.log');start=time.monotonic()
    with log.open('wb')as output:code=subprocess.run(command,cwd=ROOT,env=dict(os.environ,GOMAXPROCS='4',**extra),stdout=output,stderr=subprocess.STDOUT).returncode
    row=dict(name=name,command=command,environment=dict(GOMAXPROCS='4',**extra),exit_code=code,seconds=time.monotonic()-start,log=str(log),sha256=hashlib.sha256(log.read_bytes()).hexdigest(),source_sha256=hashlib.sha256((OUT/'checks/source.json').read_bytes()).hexdigest())
    rows.append(row);(OUT/'complete-tag-checks.json').write_text(json.dumps(rows,indent=2)+'\n');print(json.dumps(row),flush=True)
    if code:raise SystemExit(code)
