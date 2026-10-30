import concurrent.futures,hashlib,json,os,pathlib,subprocess,time
root=pathlib.Path('/extra/scylladb/gocql');e=root/'feature-specs/evidence'
paths=subprocess.check_output(['git','ls-files','-z','--cached','--others','--exclude-standard','--','*.go','go.mod','go.sum'],cwd=root).decode().split('\0')
paths=sorted(set(p for p in paths if p))
def snapshot():return {p:hashlib.sha256((root/p).read_bytes()).hexdigest() for p in paths}
before=snapshot();source=e/'step06-final-source.json';source.write_text(json.dumps(before,indent=2)+'\n')
commands=[
 ('unit-race',['go','test','-race','-tags','unit','./...','-count=1','-timeout=6m']),
 ('focused-race',['go','test','-race','-tags','unit','.','./tablets','-run','^TestStep06|^TestV2Routing|^TestV2Latency|^TestV2Tablet|^TestTypedTablet|^TestV2StreamingPartition|^TestV2ReservedPreferred','-count=2','-timeout=3m']),
 ('vet',['go','vet','-tags','unit','./...']),
 ('integration-build',['go','test','-race','-tags','integration gocql_debug','-c','-o','/tmp/step06-integration.test','.']),
 ('ccm-build',['go','test','-race','-tags','ccm integration gocql_debug','-c','-o','/tmp/step06-ccm.test','.']),
 ('fuzz',['go','test','-tags','unit','.','-run','^$','-fuzz','^FuzzStep06TabletPayload$','-fuzztime=5s','-parallel=2']),
 ('benchmarks',['go','test','-tags','unit','.','-run','^$','-bench','^BenchmarkStep06','-benchmem','-benchtime=100ms','-count=1']),
 ('shard-386',['go','test','-tags','unit','.','-run','^TestStep06PickerStrictAndAdvisory$','-count=1'])]
def run(item):
 name,cmd=item;env=os.environ.copy()
 if name=='shard-386':env['GOARCH']='386';env['CGO_ENABLED']='0'
 log=e/('step06-'+name+'.log');start=time.time()
 with log.open('wb') as output:p=subprocess.run(cmd,cwd=root,env=env,stdout=output,stderr=subprocess.STDOUT)
 result={'name':name,'command':cmd,'env':{'GOARCH':'386','CGO_ENABLED':'0'} if name=='shard-386' else {},'exit_code':p.returncode,'elapsed_seconds':round(time.time()-start,3),'log':str(log.relative_to(root)),'sha256':hashlib.sha256(log.read_bytes()).hexdigest()}
 print(json.dumps(result),flush=True);return result
with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:results=list(executor.map(run,commands))
report={'branch':subprocess.check_output(['git','branch','--show-current'],cwd=root,text=True).strip(),'predecessor':'0ca4fd253c59f4196ba466e41b66ae8d29210bca','source_snapshot':str(source.relative_to(root)),'source_snapshot_sha256':hashlib.sha256(source.read_bytes()).hexdigest(),'source_unchanged':before==snapshot(),'results':results}
(e/'step06-check-results.json').write_text(json.dumps(report,indent=2)+'\n')
if not report['source_unchanged'] or any(r['exit_code'] for r in results):raise SystemExit(1)
