# Copyright 2026 The Gocql Authors. All rights reserved.
# Use of this source code is governed by a BSD-style license.
"""One explicit failed-lane verification after diagnosis, then untouched modes.

No retry-until-green loop. Existing receipts/logs remain immutable. The exact
successful selected receipt for every mandatory lane is recorded separately.
"""
import hashlib,json,os,subprocess,time
from pathlib import Path
ROOT=Path(__file__).resolve().parents[2]
OUT=Path('/extra/scylladb/gocql-api-test-cluster/step17/release')
PYTHON='/extra/scylladb/scylladb/.venv/bin/python'
read=lambda p:json.loads(Path(p).read_text())
sha=lambda p:hashlib.sha256(Path(p).read_bytes()).hexdigest()
pin=sha(OUT/'source-frozen.json');rows=[]
selected={name:str(OUT/(name+'.json'))for name in ['stable','development','stable-v3tls','cassandra-v5-no-compression','cassandra-v5-lz4']}
for name,path in selected.items():assert read(path)['status']=='passed'and read(path)['source_sha256']==pin
old=read(OUT/'cassandra-v5-no-compression-tls.json')
assert old['status']=='failed'and old['source_sha256']==pin and all(old['lanes'][0]['joined'].values())
for receipt in [old,read(OUT.parent/'cassandra-agreement-diagnostic/run/native.json'),read(OUT.parent/'cassandra-agreement-diagnostic/run-full/native.json')]:
    assert all(all(lane['joined'].values())and lane['endpoints_refused']for lane in receipt['lanes'])
    try:os.kill(receipt['pid'],0)
    except ProcessLookupError:pass
    else:raise AssertionError('exact prior owner still live')
base=read(OUT/'cassandra-config.json')['lanes'][0];modes={m['name']:m for m in base['modes']}
def save():
    (OUT/'native-selected.json').write_text(json.dumps(dict(source_sha256=pin,selected=selected,diagnostic_failed_receipt=str(OUT/'cassandra-v5-no-compression-tls.json'),reason='One strict unmodified verification after focused and full logging diagnosis; original transient post-DDL deadline cause remains unknown; no source/bound/assertion change and no waiver.'),indent=2)+'\n')
    (OUT/'resume-run.json').write_text(json.dumps(dict(pid=os.getpid(),source_sha256=pin,steps=rows),indent=2)+'\n')
save()
for mode in ['v5-no-compression-tls','v5-lz4-tls','v4-no-compression','v4-no-compression-tls','v4-lz4','v4-lz4-tls']:
    name='cassandra-'+mode;config=OUT/(name+'-config.json')
    if not config.exists():config.write_text(json.dumps({'lanes':[dict(base,modes=[modes[mode]])]},indent=2)+'\n')
    assert read(config)=={'lanes':[dict(base,modes=[modes[mode]])]}
    receipt=OUT/(name+('-verification'if mode=='v5-no-compression-tls'else'')+'.json');assert not receipt.exists()
    command=[PYTHON,'feature-specs/tools/step17_native.py','--config',str(config),'--state','/extra/scylladb/gocql-api-test-cluster/step08','--output',str(receipt)]
    log=OUT/(receipt.stem+'-runner.log');start=time.monotonic()
    with log.open('wb')as output:
        child=subprocess.Popen(command,cwd=ROOT,env=dict(os.environ,GOMAXPROCS='4'),stdout=output,stderr=subprocess.STDOUT)
        row=dict(name=name,command=command,pid=child.pid,phase='running',log=str(log),receipt=str(receipt));rows.append(row);save();code=child.wait()
    row.update(exit_code=code,phase='joined',seconds=time.monotonic()-start,sha256=sha(log));save();print(json.dumps(row),flush=True)
    actual=read(receipt)
    if code or actual['status']!='passed':raise SystemExit(code or 1)
    assert actual['source_sha256']==pin and all(actual['lanes'][0]['joined'].values())and actual['lanes'][0]['endpoints_refused']
    selected[name]=str(receipt);save()
# The earlier pipeline stopped at the failed lane, so its cancellation lifecycle
# probe has not run. Execute it once only after the strict mandatory matrix.
command=['python3','feature-specs/tools/step17_cancel_probe.py'];log=OUT/'cancel-probe-supervisor.log';start=time.monotonic()
with log.open('wb')as output:
    child=subprocess.Popen(command,cwd=ROOT,env=dict(os.environ,GOMAXPROCS='4'),stdout=output,stderr=subprocess.STDOUT)
    row=dict(name='cancel-probe',command=command,pid=child.pid,phase='running',log=str(log));rows.append(row);save();code=child.wait()
row.update(exit_code=code,phase='joined',seconds=time.monotonic()-start,sha256=sha(log));save();print(json.dumps(row),flush=True)
raise SystemExit(code)
