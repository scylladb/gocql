# Copyright 2026 The Gocql Authors. All rights reserved.
# Use of this source code is governed by a BSD-style license.
"""Bind independent review artifacts to actual checked sources and outcomes."""
import collections,hashlib,json,os,subprocess
from pathlib import Path
ROOT=Path(__file__).resolve().parents[2];TASK=Path('/extra/scylladb/gocql-api-test-cluster/step16');CHECK=TASK/'final-checks';REVIEW=TASK/'review'
sha=lambda p:hashlib.sha256(Path(p).read_bytes()).hexdigest()
def read(p):return json.loads(Path(p).read_text())
def documents(p):
 raw=Path(p).read_text();d=json.JSONDecoder();out=[]
 while raw.strip():
  value,end=d.raw_decode(raw.lstrip());out.append(value);raw=raw.lstrip()[end:]
 return out
def outcomes(p):
 events=[]
 for line in Path(p).read_text().splitlines():
  try:e=json.loads(line)
  except ValueError:continue
  if isinstance(e,dict):events.append(e)
 packages={e['Package']:e['Action']for e in events if 'Package'in e and 'Test'not in e and e.get('Action')in ['pass','fail','skip']}
 tests={e['Package']+'::'+e['Test']:e['Action']for e in events if 'Test'in e and e.get('Action')in ['pass','fail','skip']}
 assert not any(v=='fail'for v in packages.values()) and not any(v=='fail'for v in tests.values())
 return dict(packages=packages,counts=dict(collections.Counter(tests.values())),tests=tests)
module='github.com/scylladb/gocql/v2';denied={'wire':set(),'cqltype':{'settings','routing','metadata','cluster','execution','protocol','transport','pool'},'codec':{'settings','routing','metadata','cluster','execution','protocol','transport','pool'},'protocol':{'settings','routing','metadata','cluster','execution','transport','pool'},'connectivity':{'settings','routing','metadata','cluster','execution','transport','pool'},'transport':{'settings','routing','metadata','cluster','execution','pool'},'pool':{'settings','routing','metadata','cluster','execution'},'metadata':{'cluster','execution','transport','pool'}}
closures={}
for mode,path in [('normal',CHECK/'dependency-catalog.log'),('all-tags',CHECK/'api-audit/packages-all.json')]:
 graph={p['ImportPath']:p.get('Imports',[])for p in documents(path)};closures[mode]={}
 for owner,prohibited in denied.items():
  todo=[module+'/internal/'+owner];seen=set()
  while todo:
   p=todo.pop()
   if p in seen:continue
   seen.add(p);todo.extend(x for x in graph.get(p,[])if x==module or x.startswith(module+'/'))
  assert module not in seen,(owner,'root dependency')
  assert not seen.intersection(module+'/internal/'+x for x in prohibited),(owner,seen)
  closures[mode][owner]=sorted(seen-{module+'/internal/'+owner})
(REVIEW/'dependencies.json').write_text(json.dumps(dict(prohibited={k:sorted(v)for k,v in denied.items()},closures=closures),sort_keys=True,indent=2)+'\n')
source=read(CHECK/'source.json');binding=sha(CHECK/'source.json')
for p,h in source.items():assert sha(ROOT/p)==h,p
checks=read(CHECK/'checks.json');assert len(checks)==12
for c in checks:
 assert c['exit_code']==0 and c['source_sha256']==binding and sha(c['log'])==c['sha256']
 c['outcomes']=outcomes(c['log'])if c['name']in ['affected-correctness','affected-race','operation-counters','386','repository-unit-race']else None
native=read(TASK/'native.json');assert native['status']=='passed'and native['source_sha256']==binding
for lane in native['lanes']:
 assert lane['joined'] and all(lane['joined'].values())and lane['endpoints_refused']
for c in native['commands']:
 assert c['exit_code']==0 and sha(c['log'])==c['sha256']
 if 'v4-'in c['name']:c['outcomes']=outcomes(c['log'])
try:os.kill(native['pid'],0)
except ProcessLookupError:pass
else:raise AssertionError('native runner remains alive')
api=read(CHECK/'api-audit/verification.json');assert api['external_consumer_exit_code']==0
preserved=read(REVIEW/'preservation.json');assert len(preserved['original174'])==174
for row in preserved['files']:assert sha(ROOT/row['file'])==row['sha256']
contracts=['plan.md','feature-specs/gocql-v2-api.md','feature-specs/gocql-v2-implementation.md','feature-specs/gocql-v2-streaming-receive.md','feature-specs/routing.md','feature-specs/connections.md','feature-specs/execution.md','feature-specs/metadata-control-events.md','feature-specs/configuration-security-observability.md','feature-specs/serialization-protocol.md']
report={'branch':'v2-16-independent-review','accepted_predecessor':'7a358b2405b7ff9820ad39032e1e33bd309f6e9c','gate':'passed: two actionable findings fixed; zero unresolved actionable findings after source rereview and focused verification','source_sha256':binding,'source_inputs':len(source),'source_manifest':str(CHECK/'source.json'),'contracts_sha256':{p:sha(ROOT/p)for p in contracts},'checks':checks,'native':native,'public_api':api,'preservation':preserved,'dependencies':read(REVIEW/'dependencies.json'),'findings':[{'id':'S16-1','severity':'P2','status':'fixed','file':'internal/transport/v2_transport.go','original_line':177,'invariant':'Validated positive frame/decompressed limits through signed int32 are effective across transport paths; 256 MiB is the configurable default.','trigger':'Configure 268435457,536870912,2147483647: actual typed limits all returned268435456.','pre_fix_log':str(TASK/'limits-before.log'),'pre_fix_log_sha256':sha(TASK/'limits-before.log'),'production_source_binding':'accepted_predecessor','fixes':['Remove silent typed cap; reuse checked limits in native request, raw/segmented receive and streaming envelope paths.','Propagate physical limits to pooled and handshake framers; check raw/decompressed/encoded lengths.','Exact built-in configured decoder validates connection and signed-int32 limits before output acquisition; public standalone256MiB and custom fallback semantics remain.','Keep v5 header+body arithmetic and coalesced span completion in int64 on32-bit.'],'regressions':['TestStep16ConfiguredFrameLimits','TestStep16LargeRequestLimitWithoutPayloadAllocation','TestStep16HandshakeFramersCaptureConfiguredLimits','TestStep16HandshakeFrameBudgets','TestStep16ConfiguredFrameDecodeBounds','TestStep16CustomFrameDecoderRetainsFallback','TestStep16CoalescedLargeSpanCount','TestStep16StreamingLargeEnvelopeAdmission']},{'id':'S16-2','severity':'P2','status':'fixed','file':'internal/routing/v2_policy.go','original_line':379,'invariant':'RetryPeriod expiration admits a concurrency-safe slow-host probe before the next decision publication; healthy alternatives stay preferred by other plans.','trigger':'32 concurrent plans all chose the same expired slow replica before a new sample; observed32/32 first-target probes.','pre_fix_log':str(TASK/'latency-before.log'),'pre_fix_log_sha256':sha(TASK/'latency-before.log'),'production_source_binding':'accepted_predecessor','fixes':['Per-host atomic probe deadline claimed when Next selects the expired candidate; no policy-wide completion mutex.','Observe refreshes deadline; unconsumed plans claim nothing and an abandoned selected probe expires after RetryPeriod.','Penalty remains ordering only: explicit pinning, eligibility/locality/leader and last-resort fallback retain their prior contracts.'],'regressions':['TestStep16LatencyProbeReservation']}],'rereview':{'unresolved_actionable_findings':0,'method':'Re-derived contract boundaries, inspected implementation control flow and aliases, reproduced both failures before fixes, inspected resulting diff including newly reachable32-bit widths, then ran source-frozen affected checks. Existing accepted gate booleans were not correctness proof.','additional_fix_review':'Caught max-int32-plus-header v5 integer addition and coalescer int completion truncation while reviewing the larger-limit fix; added an actual typed coalescer counting-writer regression and386 execution.','false_positive_checks':['Custom frame decoder can alias its input: fallback stays unpooled; only exact builtins get configured decoder override.','Released old page descriptors may retain Go references; public lease checks and fresh identities prevent access/ABA/reuse while owned.','LZ4 standalone historical zero-length behavior was not changed; malformed result framing cannot publish a valid native result.','NoRows streaming response decoding remains separate from Statement.Rows preparation validation; no buffered decoder adapter added.']},'measurement_scope':{'prior_step15_results':'Historical exact source bindings remain unchanged; no prior native/performance result relabeled as a660-input full release result.','affected_current_evidence':'Operation/copy/backing counters, zero-allocation configured decoder wrapper, affected compression/request/routing benchmarks and live cache/type checks.','benchmark_timing_limit':'Timings are diagnostic shared-host runs, not matched latency/throughput comparisons or a new speedup claim. Allocation/backing/operation checks support the bounded implementation claims.','large_limit_tests':'Logical large frames/headers use repeated1MiB backing or metadata-only streams; no claim that a multiGiB physical native frame was transferred.','native_stream_modes':'v4 raw delivers streamed rows; v4 Snappy/LZ4 intentionally test the required pre-submit capability error. Required v3/v5/TLS streaming acceptance remains step17 alongside focused unit coverage.'},'limitations':['Step17 must rerun every mandatory final-tree native mode, all174semantic cases/subcases, streaming acceptance, fuzz/race and final requirement/API audits.','No final-release or complete native-matrix claim is made by this independent-review gate.','The first diagnostic source run was stopped after its passing correctness command when rereview required32-bit widening fixes; its partial race output receives no final credit.','All258existing test files including all174baseline bodies/assertions/helpers are unchanged here; original API-translation semantic provenance remains step13/09, not replaced by this preservation check.'],'retained_artifacts':{str(p):sha(p)for p in [TASK/'limits-before.log',TASK/'latency-before.log',TASK/'checks-supervisor.log',TASK/'final-checks-supervisor.log',TASK/'native-supervisor.log',TASK/'native-config.json',REVIEW/'preservation.json',REVIEW/'dependencies.json',CHECK/'checks.json',CHECK/'source.json',CHECK/'api-audit/public-audit.json',TASK/'native.json']},'method_sha256':{p:sha(ROOT/p)for p in ['feature-specs/tools/step16_checks.py','feature-specs/tools/step16_evidence.py','feature-specs/tools/step15_functions.go','feature-specs/tools/step15_public.py','feature-specs/tools/step13_public_audit.go']}}
report['review_report_sha256']=sha(ROOT/'feature-specs/execution/16-independent-review.md')
(ROOT/'feature-specs/evidence/step16-verification.json').write_text(json.dumps(report,sort_keys=True,indent=2)+'\n')
print(json.dumps({'gate':report['gate'],'source_sha256':binding,'inputs':len(source),'checks':len(checks),'native_modes':len(native['lanes'][0]['tests']),'preserved_test_files':len(preserved['files']),'original_tests':174}))
