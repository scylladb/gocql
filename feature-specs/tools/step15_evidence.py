# Copyright 2026 The Gocql Authors. All rights reserved.
# Use of this source code is governed by a BSD-style license.
"""Bind every performance table row to actual source, measurements and limits."""
import hashlib,json,os,re,socket,subprocess
from pathlib import Path
ROOT=Path(__file__).resolve().parents[2]
TASK=Path('/extra/scylladb/gocql-api-test-cluster/step15')
E=ROOT/'feature-specs/evidence'
def sha(p):return hashlib.sha256(Path(p).read_bytes()).hexdigest()
def read(p):return json.loads(Path(p).read_text())
def write(name,value):(E/('step15-'+name+'.json')).write_text(json.dumps(value,sort_keys=True,indent=2)+'\n')

def main():
 checks=read(TASK/'final-checks/checks.json');assert all(c['exit_code']==0 and sha(c['log'])==c['sha256'] for c in checks)
 source=read(TASK/'final-checks/source.json');source_sha=sha(TASK/'final-checks/source.json')
 # inventory below is checked against the driver/tooling requirements

 expected_checks={'operation-counters','affected-correctness','unit-race','counters-race','bench-foundations','bench-runtime','contention-profiles','codec-cpu-profile','transport-cpu-profile','socket-binary','syscall-harness-control','fuzz-FuzzV2ScalarUTF8','fuzz-FuzzV2StreamingValues','fuzz-FuzzCursorTake','fuzz-FuzzSpanCompression','vet','386','all-tags','spec-verifier','module-verify','module-drift','architecture-boundaries','public-package-metadata','public-api-and-external-contracts','declarations'}|{'syscalls-tls'+tls+'-spans'+str(spans)+'-mixed'+mixed for tls in ['false','true'] for spans in [8,1024,4096] for mixed in ['false','true']}
 assert {c['name'] for c in checks}==expected_checks
 assert all(c['source_sha256']==source_sha for c in checks)
 for name,digest in source.items():assert sha(ROOT/name)==digest,name
 native={};cleanup=[];results=[];memory_results=[]
 for name,path in [('stable',TASK/'final-policy/stable.json'),('cassandra',TASK/'accepted/cassandra.json')]:
  r=read(path);assert r['status']=='passed'
  if r['source_sha256']!=source_sha:
   proof=read(E/'step15-v5-scope-proof.json')
   assert name=='cassandra' and r['source_sha256']==proof['old_source_sha256'],(name,r['source_sha256'],source_sha)
   r['final_tree_scope_proof']='step15-v5-scope-proof.json'
   r['same_complete_input_snapshot_as_final']=False
  else:r['same_complete_input_snapshot_as_final']=True
  assert all(r['lanes'][0]['joined'].values()) and r['lanes'][0]['endpoints_refused']
  expected_modes=({'v4-'+codec+suffix for codec in ['no-compression','snappy','lz4']for suffix in ['','-tls']}|{'v3-raw','v3-tls'})if name=='stable' else {'v5-'+codec+suffix for codec in ['no-compression','lz4']for suffix in ['','-tls']}
  assert {m['name']for m in r['configuration']['lanes'][0]['modes']}==expected_modes,(name,'mandatory modes')

  try:os.kill(r['pid'],0);raise AssertionError('runner alive')
  except ProcessLookupError:pass
  assert all(c['exit_code']==0 and sha(c['log'])==c['sha256'] for c in r['commands'])
  for label,pin in r.get('comparison_sources',{}).items():
   assert sha(pin['snapshot'])==pin['sha256']
   for path,digest in read(pin['snapshot']).items():assert sha(Path(pin.get('root',str(TASK/'baseline')))/path)==digest,('baseline-source-drift',path)

  for m in r['configuration']['lanes'][0]['modes']:
   root=Path(m['environment']['GOCQL_STEP15_OUTPUT'])
   measured=read(root/'results.json');baseline=read(root/'baseline/results.json');memory=read(root/'concurrent-memory.json')
   assert len(measured)==(18 if '-snappy' in m['name'] or '-lz4' in m['name'] and m['name'].startswith('v4') else 27)
   assert len(baseline)==9 and len(memory)==(6 if len(measured)==18 else 9)
   shapes={('small',1,32,1,1,False),('page1',128,4096,1,1,False),('page32',128,4096,32,1,False),('rows1',1,4096,128,1,False),('rows128',128,4096,128,1,False),('cell256K',1,256<<10,1,1,False),('cell8M',1,8<<20,1,1,False),('concurrent',128,4096,32,4,False),('slow',128,4096,32,1,True)}
   shape=lambda x:tuple(x['Shape'][key]for key in ['Name','Rows','Cell','Page','Concurrency','Slow'])
   assert {shape(x)for x in baseline}==shapes
   apis={'row','into'}|({'stream'}if len(measured)==27 else set())
   assert {x['API']for x in measured}==apis
   for api in apis:assert {shape(x)for x in measured if x['API']==api}==shapes
   for x in measured+baseline:
    assert x['Operations']==(8 if x['Shape']['Slow']else 32)*x['Shape']['Concurrency']
    assert x['Protocol']==int(m['name'][1]) and x['TLS']==m['name'].endswith('tls')
   assert {(x['API'],x['Workers'],x['Rows'],x['Cell'],x['Page'])for x in memory}=={(api,workers,128,4096,128)for api in apis for workers in [1,4,8]}

   for x in memory:x['mode']=name+'/'+m['name'];x['source_sha256']=r['source_sha256']
   memory_results+=memory
   assert all(x['PercentileSamples']==x['Operations'] for x in measured+baseline)
   normalize=lambda x:sorted((n['host_id'],n['host_connections'],n['missing_connections'],n['shards'],n['shard'],n['protocol'],n['compression'],n['metadata_id'],n.get('tls_version'),n.get('tls_cipher'),n['authentication_fixture'])for n in x['Negotiated'])
   expected=normalize(baseline[0]);assert expected and all(normalize(x)==expected for x in measured+baseline),('negotiated configuration mismatch',name,m['name'])
   assert all(n['missing_connections']==0 for x in measured+baseline for n in x['Negotiated'])
   compressor='snappy'if 'snappy'in m['name'] else 'lz4'if 'lz4'in m['name']else 'none'
   assert all(n['protocol']==int(m['name'][1])and n['compression']==compressor for x in measured+baseline for n in x['Negotiated'])
   if m['name'].endswith('tls'):assert all(n.get('tls_handshake_complete') and n.get('tls_version') and n.get('tls_cipher')for x in measured+baseline for n in x['Negotiated'])


   for x in measured+baseline:x['mode']=name+'/'+m['name']
   results+=measured+baseline
   # Derived rates never replace raw totals or latency sample counts.
   for x in measured+baseline:
    shape=x['Shape'];ops=x['Operations'];payload=shape['Rows']*(shape['Cell']+11)
    x['derived']={'ns_per_operation':x['WallNS']/ops,'cpu_ns_per_operation':x['CPUNS']/ops,'allocated_bytes_per_operation':x['AllocatedBytes']/ops,'allocations_per_operation':x['Allocations']/ops,'application_result_bytes_per_operation':payload,'application_MB_per_second':payload*ops*1e3/x['WallNS'],'percentile_estimator':'sorted sample at floor((N-1)*q); q=0.5,0.95,0.99; lower interpolation; no stable tail confidence claim','process_syscall_scope':'all process syscr/syscw; counter endpoint overhead not subtracted; see counter-calibration.json','tcp_byte_scope':'TCP_INFO client sockets, includes protocol/TLS bytes; not packet/record counts'}

   prefix=r['configuration']['lanes'][0]['name']+'-'+m['name']
   assert all(any(c['name']==prefix+suffix for c in r['commands']) for suffix in ['', '-baseline','-concurrent-memory','-affected-stream','-cleanup'])
  for address in r['configuration']['lanes'][0]['addresses']:
   for port in [9042,9142,19042,19142,7000,7001,10000]:
    try:socket.create_connection((address,port),.05).close();raise AssertionError('owned endpoint active')
    except OSError:cleanup.append(dict(address=address,port=port,refused=True))
  native[name]=r
 # Original complete assertions stay exact; this gate does not claim that old
 # native passes replace the release matrix on the changed production tree.
 declarations=read(TASK/'final-checks/declarations.log')
 byid={(r['File'],r.get('Receiver',''),r['Name']):r for r in declarations}
 ledger=read(E/'step13-assertion-source.json');preserved=[]
 for r in ledger['tests']:
  current=byid[(r['translated']['file'],'',r['test'])];assert current['Source']==r['translated']['source'],r['identity'];preserved.append(r['identity'])
 assert len(preserved)==174
 helpers=[]
 for group in ['helpers','supporting_methods']:
  for identity,body in ledger[group]['translated'].items():
   member=identity.split('::')[1];receiver=''
   if '.' in member:receiver,member=member.split('.',1);receiver=re.sub(r'\[.*\]','',receiver)
   assert byid[(body['file'],receiver,member)]['Source']==body['source'],identity
   helpers.append(identity)
 # Every row of BOTH mandatory tables is copied verbatim, then discharged below.
 text=(ROOT/'feature-specs/gocql-v2-implementation.md').read_text();tables=text[text.index('| Benchmark (steady state'):text.index('Measure ns/op, bytes/op',text.index('| Benchmark (steady state'))]
 rows=[line.split('|')[1:3] for line in tables.splitlines() if line.startswith('| ') and not line.startswith('| Benchmark ') and not line.startswith('| Scaling/') and not line.startswith('| ---')]
 assert len(rows)==23,len(rows)
 bindings=[
 ('scalar-arena',['TestStep15AllScalarArenaWrites','TestStep15ScalarArenaCounts','TestCursorScalarDoesNotAllocate'],['BenchmarkStep15ScalarArena','BenchmarkV2ScalarInt64Decode'],'one body serialization; framing counted separately; arena slab/descriptor allocations are amortized per request'),
 ('borrowed-sizes',['TestStep15BorrowIdentityAndWrites','TestStep15CanonicalAssembly','TestStep15OriginalTextBacking','TestTypedRequestMatchesLegacyWireAndKeepsPayload'],['BenchmarkStep15BorrowedSizes','BenchmarkTypedBuildRequest'],'0/32/4096/512000 bytes; borrowed identity and gather counter; owned conversion separate'),
 ('row-ranges',['TestStep15TraversalCounts','TestSegmentedRangeViewsDoNotAllocate'],['BenchmarkStep15NarrowWideRows','BenchmarkV2RowDestinationFragments'],'shared O(S) descriptor index and O(N+S) actual visit counters'),
 ('streaming',['TestStep14StreamingStorageMatrix','TestStep15SegmentCountMemory','TestStep15ConcurrentLiveMemory','TestStep07RunStreamLive','TestStep15NativeCacheOwnership'],['TestStep15NativePerformance','BenchmarkV2ExclusiveTransport'],'native source/output independent; sampled heap and post-GC live heap distinguish; source/TLS/harness-inclusive bounds labeled'),
 ('next-into',['TestV2RowDecodeIntoUsesCallerFieldsAndResets','TestStep15ContiguousScalarSafety'],['BenchmarkStep15NarrowWideRows','BenchmarkV2PageCallerDestination'],'4/32/128 actual scalar/text/blob/list columns, fragmented fields, no complete row temporary'),
 ('lazy-containers',['TestStep15LazyTraversalCounts','TestV2CollectDetachesEveryNestedFamily'],['BenchmarkStep15LazyAndMaterialized','BenchmarkV2ContainerSegmentedIteration'],'iterator allocations fixed per iterator; zero per-element copies/allocations; Collect/Clone explicit'),
 ('dynamic',['TestStep15DynamicLazyReuse','TestStep15CompiledScalarSafetyAndWarmChecks','TestStep15DynamicCopiedRowsIndexAndClone','TestStep15DynamicIndexJBounds'],['BenchmarkStep15DynamicRows','BenchmarkStep15DynamicRepeatedAccess'],'schema shapes reused; no reflect/any destination or eager payload conversion'),
 ('routing-token',['TestV2RoutingHashUsesSegmentedCanonicalBytes','TestV2StreamingPartitioners','TestStep06BindingCapturesPartitionerAndBorrowedSpans'],['BenchmarkV2RoutingCompositeSpans','BenchmarkStreamSegments'],'canonical baseline byte/hash oracle, encoders once, no route payload allocation'),
 ('canonical-attempts',['TestStep15CanonicalAssembly','TestV2PreparedRetryRepreparePagingNeverReencodes','TestStep07StreamReprepareCanonicalInputAndNoRepairAfterOutput'],['BenchmarkTypedBuildRequest','BenchmarkStep15BatchAndAttempts'],'actual writer spies prove4 buffered/3 stream payload backings identical; 8 request/batch envelopes reuse canonical body'),
 ('v5',['TestEncodeSpansOracleAndBacking','TestTypedReceiveLargeFragmentedRows','TestTypedSharedSegmentPagesHaveIndependentOwnership'],['BenchmarkValidatedUnits','BenchmarkV2ExclusiveTransport','BenchmarkStep15V5SplitFrames'],'raw spans retained; CRC/header/index allocations separate; streaming integrity-unit-to-parser transform copies explicitly nonzero'),
 ('compression',['TestStep15CompressionCopyBoundary','TestStep15CompressionOutputFrameIdentity'],['BenchmarkSpanCompression'],'actual compressor-output identity survives native envelope; repetitive/random/mixed spans; input/output bytes and ratios'),
 ('tls',['TestTypedTLSReceivesOriginalPlaintextSpans','TestV2StreamingTransportReadTargetsAndWorkingMemory'],['BenchmarkV2TLSWriteAmplification','BenchmarkV2TLSReceiveAmplification','BenchmarkStep15SocketWrites'],'authenticated plaintext boundary; actual TLS record headers counted; net.Conn calls, OS syscalls, TCP bytes distinct'),
 ('paging-retention',['TestStep15RetainedPrefetchCharges','TestV2PageNextIntoMixedCurrentAndRetention','TestV2DeliveredPageSurvivesSessionCloseUntilAdvance','TestStep15ReceiveCacheOwnership','TestStep15NativeCacheOwnership'],['BenchmarkV2PagingHistoryLookup'],'ordinary cells no payload clone; 1/4 workers ×1/1024 cells prefetch0/1 owner transfers constant per page'),
 ('selection',['TestStep15HostWorkCounts','TestStep06ConcurrentPlansAndFeedback'],['BenchmarkStep06FirstTarget','BenchmarkStep07SharedAdmission'],'fixed first-target work over3/30/300 hosts; fallback advances lazily'),
 ('N-S-depth-J',['TestStep15TraversalCounts','TestStep15LazyTraversalCounts','TestStep15DynamicLazyReuse','TestStep15NestedVisitBounds','TestStep15DynamicIndexJBounds','TestStep15NestedVectorBounds'],['BenchmarkCursorCellsAndFragments','BenchmarkRangeLookupScaling'],'real N+S visits, J*logS comparisons; depths1/4/16; shared index pointer proof'),
 ('warm-plans',['TestStep15WarmAndChangedPlans','TestStep15CompiledScalarSafetyAndWarmChecks','TestV2PreparedOldPageKeepsMatchingPlan','TestV2ResultPlanPublicationSharesEpochGate'],['BenchmarkV2ResultPlanExactEntry'],'10000 warm Compile/CheckType/name maps0; changed candidate once; old plan/source fence preserved'),
 ('hosts',['TestStep15HostWorkCounts','TestStep06LeaderLocalityLWTAndFallback'],['BenchmarkStep06FirstTarget'],'3/30/300, ordinary/pinned/conditional/leader/latency/fallback; first path no host index build or fallback visits'),
 ('history',['TestStep15HistoryCounts','TestV2PagingHistoryCyclesLimitsAndTokenIdentity','TestV2PagingHistoryHashCollisionComparesFullBytes'],['BenchmarkV2PagingHistoryLookup'],'1/1000/10000, empty/colliding/nonconsecutive; incoming bytes and exact equality counted, retained token address unchanged'),
 ('warm-concurrency',['TestV2WarmPreparedHandleSurvivesDiscoveryCacheLock','TestV2PreparedColdHostsDoNotSerialize','TestStep06ConcurrentPlansAndFeedback'],['BenchmarkStep15WarmConcurrentBindings'],'discovery LRU locked during warm parallel bind/host selection; unrelated schema+placement updates; borrowed/owned costs separate'),
 ('optional',['TestStep15OptionalWorkCounts'],['BenchmarkStep07SharedAdmission','BenchmarkStep15WarmConcurrentBindings'],'disabled0 events/callbacks/timers/snapshots/prefetch workers; default Options one worker only with real continuation; profiles separate'),
 ('feedback',['TestStep06PlacementFencesSharingAndOverflow','TestStep06FeedbackEnqueueDoesNotWaitForPublication'],['BenchmarkStep06PlacementUpdate','BenchmarkStep06FeedbackQueue'],'duplicate no publication/0alloc; changed structural sharing; bounded queue strips source snapshot/page dependency'),
 ('write-amplification',['TestTypedWritersPartialCompletion','TestStep15ScalarArenaCounts'],['BenchmarkStep15SocketWrites','BenchmarkV2TLSWriteAmplification'],'8/1024/4096 spans, optional500KiB blob, actual coalescer, unwrapped TCP writev; separate strace/CPU/record diagnostics'),
 ('retained-concurrency',['TestStep15RetainedPrefetchCharges','TestStep15ConcurrentLiveMemory'],['TestStep15NativePerformance','BenchmarkStep15WarmConcurrentBindings'],'all workers parked with live responses; logical leased frame charge/actual slab allocation/idle cache capacities/refcount owner transfers separate from physical aliases; live GC heap includes TLS/harness and fixed application object metadata')]
 text_identity=read(TASK/'final-checks/text-identity.json');assert text_identity['exit_code']==0 and sha(text_identity['log'])==text_identity['sha256']
 passed_tests=set();bench_names=set();test_evidence={};benchmark_evidence={}
 for c in checks+[c for r in native.values() for c in r['commands']]:
  for line in Path(c['log']).read_text().splitlines():
   try:
    event=json.loads(line)
    if isinstance(event,dict) and event.get('Action')=='pass' and event.get('Test'):passed_tests.add(event['Test'].split('/')[0]);test_evidence.setdefault(event['Test'].split('/')[0],{}).setdefault(c['name'],{'command':c['name'],'log':c['log'],'sha256':c['sha256'],'package':event.get('Package'),'passed_cases':[]})['passed_cases'].append(event['Test'])
   except ValueError:pass
   match=re.search(r'--- PASS: ([^ (]+)',line)
   if match:passed_tests.add(match.group(1).split('/')[0]);test_evidence.setdefault(match.group(1).split('/')[0],{}).setdefault(c['name'],{'command':c['name'],'log':c['log'],'sha256':c['sha256'],'passed_cases':[]})['passed_cases'].append(match.group(1))
   match=re.search(r'^(Benchmark[^/\s-]+)',line)
   if match:bench_names.add(match.group(1));benchmark_evidence.setdefault(match.group(1),{}).setdefault(c['name'],{'command':c['name'],'log':c['log'],'sha256':c['sha256'],'samples':[]})['samples'].append(line)
 for log,receipt in [(TASK/'final-checks/batch-attempt-envelope.log',TASK/'final-checks/batch-envelope.json'),(TASK/'final-checks/text-identity.log',TASK/'final-checks/text-identity.json'),(TASK/'final-checks/segment-throughput.log',TASK/'final-checks/segment-throughput.json'),(TASK/'final-checks/nested-vectors.log',TASK/'final-checks/nested-vectors.json')]:
  metadata=read(receipt);assert metadata['exit_code']==0 and sha(log)==metadata['sha256']
  for line in log.read_text().splitlines():
   passed=re.search(r'--- PASS: ([^ (]+)',line)
   if passed:passed_tests.add(passed.group(1).split('/')[0]);test_evidence.setdefault(passed.group(1).split('/')[0],{}).setdefault(log.name,{'command':metadata['command'],'log':str(log),'sha256':sha(log),'overlay_receipt':str(receipt),'passed_cases':[]})['passed_cases'].append(passed.group(1))
   match=re.search(r'^(Benchmark[^/\s-]+)',line)
   if match:bench_names.add(match.group(1));benchmark_evidence.setdefault(match.group(1),{}).setdefault(log.name,{'command':metadata['command'],'log':str(log),'sha256':sha(log),'overlay_receipt':str(receipt),'samples':[]})['samples'].append(line)
 requirements=[]
 for (requirement,cost),(id,tests,benches,method) in zip(rows,bindings):
  assert set(tests)<=passed_tests,(id,'unpassed',set(tests)-passed_tests)
  for measure in benches:
   assert measure in bench_names or measure in passed_tests,(id,'measurement absent',measure)
  requirements.append(dict(id=id,requirement=requirement.strip(),required_evidence=cost.strip(),tests=tests,measurements=benches,method=method,actual_test_evidence={name:list(test_evidence[name].values())for name in tests},actual_measurement_evidence={name:list((benchmark_evidence if name.startswith('Benchmark') else test_evidence)[name].values())for name in benches},status='tested'))
 artifacts={}
 for p in TASK.rglob('*'):
  if p.is_file() and any(str(p).startswith(str(TASK/x)) for x in ['final-policy/','final-checks/','accepted/','matched-smoke/','compression-experiment/','scaling-experiment/','receive-pool-experiment/','cache-admission-experiment/']) and p.name not in ['socket.test']:
   artifacts[str(p)]=sha(p)
 overlay=read(TASK/'final-checks/live-heap-overlay.json');assert overlay['exit_code']==0 and sha(overlay['log'])==overlay['sha256']
 baseline_root=TASK/'baseline-matched'
 assert subprocess.check_output(['git','diff','--stat'],cwd=baseline_root,text=True)==''
 baseline_files={str(p):sha(p) for p in baseline_root.glob('step15_*test.go')}
 configuration=read(TASK/'matched-configuration.json');configuration.pop('baseline_adapter_sha256',None)
 baseline_summary={'pinned_original_revision':configuration['baseline_revision'],'matched_configuration':configuration,'matched_original_driver':{'root':str(baseline_root),'tracked_production_diff_empty':True,'adapter_files_sha256':baseline_files,'per_mode_sources':{name:receipt['comparison_sources']for name,receipt in native.items()}},'historical_default_policy_original':{'root':str(TASK/'baseline'),'classification':'historical default-policy comparison; coalesce200us, old one-per-shard vs initial typed one-per-host; not matched latency credit','adapter_files_sha256':{str(p):sha(p)for p in (TASK/'baseline').glob('step15_*test.go')}},'typed_normal_harness':{'path':str(ROOT/'step15_performance_integration_test.go'),'sha256':sha(ROOT/'step15_performance_integration_test.go'),'native_input_snapshot_sha256':source_sha},'historical_original_microbench':{'log':str(TASK/'micro/original-baseline.log'),'sha256':sha(TASK/'micro/original-baseline.log'),'classification':'includes historical unchecked scalars and separately named UTF8/output-matched microcases; no universal speedup claim'}}
 for receipt in native.values():
  for mode,pin in receipt['comparison_sources'].items():
   saved=read(pin['snapshot']);root=Path(pin.get('root',str(TASK/'baseline')))
   assert root==baseline_root,(mode,root)
   for name in baseline_root.glob('step15_*test.go'):assert saved[str(name.relative_to(baseline_root))]==sha(name),(mode,name)

 write('baseline',baseline_summary)
 artifacts[str(TASK/'baseline-harness.json')]=sha(TASK/'baseline-harness.json')
 artifacts[str(TASK/'environment.json')]=sha(TASK/'environment.json')
 write('source',source);write('native',native);write('cleanup',cleanup);write('measurements',results);write('memory',memory_results)
 comparisons=[]
 for mode in sorted({x['mode'] for x in results}):
  original={x['Shape']['Name']:x for x in results if x['mode']==mode and x['API']=='legacy-scan'}
  for x in results:
   if x['mode']!=mode or x['API']=='legacy-scan':continue
   old=original[x['Shape']['Name']]
   comparisons.append(dict(mode=mode,shape=x['Shape'],api=x['API'],baseline='legacy-scan',v2_over_original_cpu=x['derived']['cpu_ns_per_operation']/old['derived']['cpu_ns_per_operation'],v2_over_original_allocated_bytes=x['derived']['allocated_bytes_per_operation']/old['derived']['allocated_bytes_per_operation'],v2_over_original_end_p50=x['EndNS'][0]/old['EndNS'][0],interpretation='same completed output, query/result bytes/shapes/concurrency; differences include required v2 typed safety, request lifetime and independently retained-page lease semantics; no universal speedup claim'))
 write('comparisons',comparisons)

 write('requirements',requirements);write('preservation',dict(identities=preserved,helpers_and_methods=helpers,all174_complete_sources_unchanged=True,step14_native_lineage='step14-test-mapping.json',step17_required_on_final_tree=True))
 report=dict(branch='v2-15-performance',accepted_predecessor='5edeb77e37756549d1ed1cbb7581ff2c2484301b',source_sha256=source_sha,source_inputs=len(source),requirements=len(requirements),native_modes=sum(len(r['configuration']['lanes'][0]['modes']) for r in native.values()),checks=checks,artifacts=artifacts,live_heap_overlay=overlay,baseline=baseline_summary,environment=read(TASK/'environment.json'),gate='passed',limitations=['Empirical p99 uses32/128 observations (slow shape8); no stable tail inference.','HeapAlloc sampling can miss transients; synchronized live postGC and actual storage capacities supply separate evidence.','Inclusive runtime-delta heap contains driver, TLS, harness/GC bookkeeping; it is not a driver-only exact heap attribution.','Kernel TCP_INFO bytes are actual socket bytes; process I/O syscall snapshots include endpoint/background/file I/O and are not net.Conn call or TLS record counters.','Original scalar unchecked baseline timings lack v2 safety contracts; no speedup claim derives from those cases.','Frame-compressed v3/v4 RunStream rejects per contract; buffered Row/NextInto compare in those modes.','Step16 independent review and step17 final complete native/release matrix remain.'])

 # Evidence and method sources are outside the native source pin but explicitly
 # hashed, so diagnostic-only tooling edits remain reviewable too.
 report['method_source_sha256']={str(p.relative_to(ROOT)):sha(p) for p in list((ROOT/'feature-specs/tools').glob('step15_*.py'))+[ROOT/'feature-specs/execution/15-performance.md']}
 report['artifact_reports_sha256']={str(p.relative_to(ROOT)):sha(p) for p in E.glob('step15-*.json') if p.name!='step15-verification.json'}
 write('verification',report);print(json.dumps(dict(gate=report['gate'],source=source_sha,rows=len(requirements),modes=report['native_modes'],checks=len(checks))))
if __name__=='__main__':main()
