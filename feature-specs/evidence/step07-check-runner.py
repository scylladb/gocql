import pathlib,subprocess,time,json,hashlib,os
root=pathlib.Path('/extra/scylladb/gocql');ev=root/'feature-specs/evidence';results=[]
checks=[
 ('unit-race',['go','test','-race','-tags','unit','./...','-count=1','-timeout=6m'],{}),
 ('focused-race',['go','test','-race','-tags','unit','-run','^TestStep07|TestV2(Runtime|Prepared|Metadata|Routing|Result|Paging|Metrics)|TestStep06','-count=2','-timeout=3m','.'],{}),
 ('vet',['go','vet','-tags','unit','./...'],{}),
 ('integration-build',['go','test','-c','-race','-tags','integration gocql_debug','-o','/tmp/step07-final-integration.test','.'],{}),
 ('ccm-build',['go','test','-c','-race','-tags','ccm integration gocql_debug','-o','/tmp/step07-final-ccm.test','.'],{}),
 ('fuzz-execution',['go','test','-tags','unit','-run','^$','-fuzz','^FuzzStep07StreamExecutionBoundary$','-fuzztime=5s','-parallel=2','.'],{'GOMAXPROCS':'2'}),
 ('fuzz-codecs',['go','test','-tags','unit','-run','^$','-fuzz','^FuzzV2StreamingValues$','-fuzztime=5s','-parallel=2','.'],{'GOMAXPROCS':'2'}),
 ('fuzz-framing',['go','test','-tags','unit','-run','^$','-fuzz','^FuzzV2ExclusiveIncrementalFraming$','-fuzztime=5s','-parallel=2','.'],{'GOMAXPROCS':'2'}),
 ('benchmarks',['go','test','-tags','unit','-run','^$','-bench','^BenchmarkStep07|^BenchmarkV2Exclusive(RowsScaling|Transport)$','-benchtime=100x','-count=1','.'],{}),
]
for name,cmd,extra in checks:
 started=time.time();log=ev/('step07-final-'+name+'.log')
 with log.open('wb')as out:r=subprocess.run(cmd,cwd=root,env=dict(os.environ,**extra),stdout=out,stderr=subprocess.STDOUT)
 item={'name':name,'command':cmd,'env':extra,'exit_code':r.returncode,'elapsed_seconds':round(time.time()-started,3),'log':str(log.relative_to(root)),'sha256':hashlib.sha256(log.read_bytes()).hexdigest()};results.append(item)
 (ev/'step07-final-check-results.json').write_text(json.dumps(results,indent=2)+'\n');print(json.dumps(item),flush=True)
print('REQUIRED_CHECK_FAILURES',sum(r['exit_code']!=0 for r in results),flush=True)
