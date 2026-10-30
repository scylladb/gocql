import hashlib,json,pathlib,socket,subprocess,shutil,os
root=pathlib.Path(__file__).resolve().parents[2];out=root/'feature-specs/evidence';state=pathlib.Path('/extra/scylladb/gocql-api-test-cluster/step08');raw=state/'raw-evidence';raw.mkdir(exist_ok=True)
def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
def read(name):return json.loads((out/name).read_text())
source=read('step08-final-source.json')
assert all(sha(root/name)==digest for name,digest in source.items()),'frozen source changed'
# Go's CPU header includes trailing whitespace. Preserve byte-exact original,
# then normalize committed presentation and update only matching log receipts.
formatting={}
for p in out.glob('step08*.log'):
 data=p.read_bytes();normalized=b'\n'.join(line.rstrip(b' \t\r') for line in data.split(b'\n'))
 if normalized!=data:
  retained=raw/p.name
  if not retained.exists():retained.write_bytes(data)
  formatting[p.name]={'raw_path':str(retained),'raw_sha256':sha(retained),'sha256':hashlib.sha256(normalized).hexdigest()}
  p.write_bytes(normalized)
for p in out.glob('step08*.json'):
 try:d=json.loads(p.read_text())
 except:continue
 changed=False
 def walk(v):
  global changed
  if isinstance(v,dict):
   log=v.get('log')
   if isinstance(log,str) and pathlib.Path(log).name in formatting:
    v.update(formatting[pathlib.Path(log).name]);changed=True
   for value in list(v.values()):walk(value)
  elif isinstance(v,list):
   for value in v:walk(value)
 walk(d)
 if changed:p.write_text(json.dumps(d,indent=2)+'\n')
(out/'step08-log-formatting.json').write_text(json.dumps(formatting,indent=2)+'\n')
verified=[]
for name in ['step08-verified-scylla.json','step08-verified-cassandra.json']:
 d=read(name);assert d['status']=='passed';assert d['source_sha256']==sha(out/'step08-final-source.json')
 for lane in d['lanes']:
  assert lane['status']=='passed' and lane['endpoints_refused'] and all(lane['joined'].values())
  assert lane['node_restart_stopped']
 for command in d['commands']:
  assert command['exit_code']==0
  p=pathlib.Path(command['log']);assert sha(p)==command['sha256']
  if '-cleanup' in command['name'] or command['name'].endswith(('javac','jar','version')):continue
  events=[]
  for line in p.read_text().splitlines():
   try:events.append(json.loads(line))
   except:pass
  command['test_passes']=[e['Test'] for e in events if e.get('Action')=='pass' and e.get('Test')]
  command['test_skips']=[e['Test'] for e in events if e.get('Action')=='skip' and e.get('Test')]
  assert not command['test_skips'],command['test_skips']
 verified.append({'receipt':name,'sha256':sha(out/name),'runner_alive':pathlib.Path('/proc/'+str(d['pid'])).exists(),'summary':d})
 assert not verified[-1]['runner_alive']
# Independently recheck all task addresses after completed runners.
health=[]
for lane in [20,21,22]:
 for node in [1,2,3]:
  for port in [9042,9142,19042,19142,7000,7001,10000]:
   address='127.0.'+str(lane)+'.'+str(node)
   try:conn=socket.create_connection((address,port),.15)
   except OSError:health.append({'address':address,'port':port,'refused':True});continue
   conn.close();raise RuntimeError('task endpoint still live '+address+':'+str(port))
(out/'step08-post-stop-health.json').write_text(json.dumps(health,indent=2)+'\n')
lifecycle=read('step08-lifecycle-results.json')
for record in lifecycle:
 assert record['cleanup_verified']
 name='step08-lifecycle-'+record['action']+'.json';d=read(name)
 assert d['source_sha256']==sha(out/'step08-final-source.json')
 assert d['status']=='failed' and all(l['endpoints_refused'] and all(l['joined'].values()) for l in d['lanes'])
 assert not pathlib.Path('/proc/'+str(d['pid'])).exists()
record=read('step08-lifecycle-conflict.json');assert 'BlockingIOError' in record['error']
# Keep copied runner/source artifacts for all retained snapshots that exist.
snapshots={}
for p in out.glob('step08*.json'):
 try:d=json.loads(p.read_text())
 except:continue
 if not isinstance(d,dict) or 'fixture_root' not in d:continue
 fixture=pathlib.Path(d['fixture_root'])
 for original in ['native.py','source.json']:
  source_path=fixture/original
  if source_path.exists():
   target=out/(p.stem+'-'+original)
   shutil.copy2(source_path,target);snapshots[target.name]=sha(target)
# One fixture-only identity probe changed after unit/performance gate. Native
# selected packages and affected race build tags were recompiled on final source.
before=read('step08-before-identity-source.json');delta=[n for n in source if before.get(n)!=source[n]]
assert delta==['step08_fixture_integration_test.go'],delta
manifest={'agent':'/root/step08_fixture_harness','branch':'v2-08-fixture-harness','predecessor':'f4a5b430f95195f5ae3ad105940b7d176abcbf97',
 'gate':'Assigned reusable fixture/harness gate complete; full174 migration09 and final-wide matrices14/17 remain.',
 'source_snapshot':'feature-specs/evidence/step08-final-source.json','source_sha256':sha(out/'step08-final-source.json'),'frozen_sources_unchanged':True,
 'unit_gate_source_snapshot':'feature-specs/evidence/step08-before-identity-source.json','unit_gate_source_sha256':sha(out/'step08-before-identity-source.json'),
 'unit_to_final_delta':delta,'delta_reason':'Only integration fixture identity query changed: Scylla release_version is Cassandra compatibility3.0.8, actual build identity uses system.versions. Unit/runtime/wire/performance source unchanged; final native packages and both integration/CCM race builds use final fixture.',
 'required_checks':read('step08-final-check-results.json'),'affected_final_builds':read('step08-identity-build-results.json'),
 'verified_native':verified,'lifecycle_injected_failures':lifecycle,'lock_conflict':{'receipt':'step08-lifecycle-conflict.json','reason':record['error']},
 'cleanup_health':{'path':'feature-specs/evidence/step08-post-stop-health.json','sha256':sha(out/'step08-post-stop-health.json'),'all_task_endpoints_refused':True},
 'retained_snapshots':snapshots,'log_formatting':'feature-specs/evidence/step08-log-formatting.json',
 'diagnostics':'Other step08 initial/first/second/third/fourth/final-native/lock-conflict receipts and logs are superseded diagnostics, not verified passes. Early diagnostics predate retained source snapshots; frozen verified receipts/source snapshots above are authoritative.',
 'scope_limits':['Cassandra5.0.9 native protocols4 and5 verified; a separate Cassandra4.x server installation was not run.',
 'Java handler write failure is deliberately injected; echo executes ordinary real database CQL and uses server-native serialization. Incomplete peer is independent v4 wire injection, not system-table mutation.',
 'Literal LZ4 wire oracle proves receive boundaries, while real Cassandra LZ4 lanes prove compressor interoperability.',
 'Remaining hardcoded gocql_test callsites require migration09 before running all174 with namespace prefixes.',
 'Selected real streaming tests preserve public RunStream large/nested/empty/paged/early-close; complete metadata/matrix migration remains09/14/17.',
 'Benchmarks include public API setup, peer scheduling and oracle stream/checksum rebind; immutable payload/final caller storage excluded. Full native performance remains15.',
 'SIGKILL/power loss cannot execute cleanup; retained exact task roots/PIDs enable bounded recovery. No shared container reset was performed.']}
(out/'step08-verification.json').write_text(json.dumps(manifest,indent=2)+'\n')
print('VERIFIED',[(x['receipt'],sum(len(l['tests']) for l in x['summary']['lanes'])) for x in verified], 'source',manifest['source_sha256'])
