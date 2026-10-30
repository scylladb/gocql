import subprocess,pathlib,time,json,os,signal
root=pathlib.Path(__file__).resolve().parents[2];out=root/'feature-specs/evidence';state=pathlib.Path('/extra/scylladb/gocql-api-test-cluster/step08');records=[]
base=['/extra/scylladb/scylladb/.venv/bin/python','tests/fixtures/native.py','--state',str(state)]
for action in ['fail','cancel']:
 marker=state/(action+'.marker');marker.unlink(missing_ok=True)
 cmd=base+['--config',str(out/('step08-'+action+'-config.json')),'--output',str(out/('step08-lifecycle-'+action+'.json'))]
 log=out/('step08-lifecycle-'+action+'.log')
 with log.open('wb') as target:
  child=subprocess.Popen(cmd,cwd=root,stdout=target,stderr=subprocess.STDOUT)
  try:
   if action=='cancel':
    end=time.monotonic()+180
    while not marker.exists():
     if child.poll() is not None:raise RuntimeError('cancel fixture exited before marker')
     if time.monotonic()>end:raise RuntimeError('cancel marker deadline')
     time.sleep(.1)
    conflict=base+['--config',str(out/'step08-cancel-config.json'),'--output',str(out/'step08-lifecycle-conflict.json')]
    with (out/'step08-lifecycle-conflict.log').open('wb') as target2:r=subprocess.run(conflict,cwd=root,stdout=target2,stderr=subprocess.STDOUT,timeout=15)
    if r.returncode==0:raise RuntimeError('exclusive reservation accepted concurrent runner')
    child.send_signal(signal.SIGTERM)
   status=child.wait(timeout=180)
   if status==0:raise RuntimeError('injected '+action+' unexpectedly passed')
   receipt=json.loads((out/('step08-lifecycle-'+action+'.json')).read_text())
   if not all(l.get('endpoints_refused') and all(l.get('joined',{}).values()) for l in receipt['lanes']):raise RuntimeError('injected '+action+' cleanup did not join')
   cleanup=[c for c in receipt.get('commands',[]) if c['name'].endswith('-cleanup')]
   if not cleanup or cleanup[-1]['exit_code']!=0:raise RuntimeError('namespace cleanup failed')
   if not marker.exists():raise RuntimeError('lifecycle test did not provision namespace')
   namespace=marker.read_text()
   if 'DROPPED '+namespace not in pathlib.Path(cleanup[-1]['log']).read_text():raise RuntimeError('namespace not removed')
   records.append({'action':action,'command':cmd,'exit_code':status,'namespace':namespace,'cleanup_verified':True,'receipt':str(out/('step08-lifecycle-'+action+'.json'))})
   (out/'step08-lifecycle-results.json').write_text(json.dumps(records,indent=2)+'\n');print(records[-1],flush=True)
  finally:
   if child.poll() is None:child.send_signal(signal.SIGTERM);child.wait(timeout=60)
