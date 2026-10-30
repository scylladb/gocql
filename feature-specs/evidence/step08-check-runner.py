import hashlib,json,os,pathlib,subprocess,time
root=pathlib.Path(__file__).resolve().parents[2];out=root/'feature-specs/evidence';records=[]
checks=[
 ('harness-unit',['python3','-m','unittest','discover','-s','tests/fixtures','-p','test_*.py']),
 ('unit-race',['go','test','-race','-tags','unit','./...','-count=1','-timeout=6m']),
 ('focused-race',['go','test','-race','-tags','unit','-run','^TestStep08|^TestStep07Stream|^TestV2Exclusive|^TestTypedTLS|^TestWriteCoalescer|^TestDeadlineContextWriter','-count=2','-timeout=3m','.']),
 ('vet',['go','vet','-tags','unit','./...']),
 ('integration-build',['go','test','-c','-race','-tags','integration gocql_debug','-o','/tmp/step08-final-integration.test','.']),
 ('ccm-build',['go','test','-c','-race','-tags','ccm integration gocql_debug','-o','/tmp/step08-final-ccm.test','.']),
 ('benchmarks',['go','test','-tags','unit','-run','^$','-bench','^BenchmarkStep08|^BenchmarkV2ExclusiveTransport$|^BenchmarkV2TLS(Write|Receive)Amplification$|^BenchmarkStep07ExecutionStorage$','-benchtime=20x','-count=1','-timeout=3m','.']),
 ('copy-contract',['go','test','-race','-tags','unit','-run','^TestStep08IndependentRawDirectRead$|^TestStep07StreamRawReadCallerBacking$|^TestV2Exclusive.*(Working|Direct|Copy|Memory|Storage)|^TestTypedTLSReceivesOriginalPlaintextSpans$','-count=1','-timeout=1m','.'])]
for name,command in checks:
 log=out/('step08-final-'+name+'.log');start=time.monotonic()
 with log.open('wb') as target:r=subprocess.run(command,cwd=root,stdout=target,stderr=subprocess.STDOUT)
 result={'name':name,'command':command,'exit_code':r.returncode,'seconds':round(time.monotonic()-start,3),'log':str(log.relative_to(root)),'sha256':hashlib.sha256(log.read_bytes()).hexdigest()};records.append(result);(out/'step08-final-check-results.json').write_text(json.dumps(records,indent=2)+'\n');print(json.dumps(result),flush=True)
 if r.returncode:raise SystemExit(r.returncode)
