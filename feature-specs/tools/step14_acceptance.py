# Copyright 2026 The Gocql Authors. All rights reserved.
# Use of this source code is governed by a BSD-style license.
"""Bind each streaming acceptance class to current assertions and actual passes."""
import json
import argparse
from pathlib import Path
import re
import step14_matrix as matrix

GROUPS={
 'split-framing-arithmetic-text':('Every split/truncated header/cell/text/nested framing; configured total work/overflow/depth/count limits.', ['TestStep08StreamIndependentEverySplitAndTruncation','TestV2StreamingScalarDomains','TestV2StreamingBytesCapacityNullAndText','TestV2StreamingNestedCollectionCapacityAndBacking']),
 'first-row-before-remainder':('All eight modes deliver row1 behind independent source gate; v5 gate starts only after required complete CRC-validated unit.', ['TestStep08StreamIndependentFirstRowGate','TestStep07StreamFirstRowBeforePageAndNoReplay']),
 'integrity-publication':('Bad checksums/trailers/truncation cannot return a successful invalid row; multi-row and cross-unit cells remain incremental.', ['TestStep08StreamIndependentCorruptChecksums','TestStep07StreamBadV5IntegrityCannotDeliver','TestV2StreamingTransportReadTargetsAndWorkingMemory']),
 'seeded-capacity-types-reset':('Exact/insufficient capacity, null/empty/unset/UTF8; nested/vector/tuple/UDT/encoded final backing and missing/reset capabilities.', ['TestV2StreamingBytesCapacityNullAndText','TestV2StreamingNumericPayloadStorage','TestV2StreamingNestedCollectionCapacityAndBacking','TestV2StreamingVectors','TestV2StreamingRecordsAndEncodedOutput','TestV2StreamingRowsDuplicateNamesAndMissingUDTFields','TestV2StreamingMissingPayloadResetPreservesSeeds','TestV2StreamingAbsentCustomDescendantResetCapability','TestV2StreamingAbsentCustomResetterSucceeds']),
 'direct-final-ranges':('Independent peer bytes land in final caller ranges at raw socket or authenticated TLS plaintext boundary; v5 unit copies counted separately.', ['TestStep08IndependentRawDirectRead','TestV2StreamingDirectReadTargets','TestV2StreamingEncodedNestedFinalReadIdentity','TestStep07StreamRawReadCallerBacking']),
 'simultaneous-byte-storage':('Generated source avoids full response fixture; independently grow cell256KB→8MB, page1→128rows, skip8MB, slow consumer and unit count; sample actual live byte capacities inside reads and between calls across all8 modes. Excludes source, final output, fixed schema plans and stdlibTLS records explicitly.', ['TestStep14StreamingStorageMatrix','TestV2StreamingSkippedValuesAreBoundedAndChecked','TestStep07StreamLatestAttemptMetadataRemainsBudgetedDuringRetry']),
 'mixed-buffered-iteration-lease':('Buffered Page/Rows mixed Next/NextInto use correct Current/reset/retained leases and preserve old matching metadata plans.', ['TestV2PageNextIntoMixedCurrentAndRetention','TestV2PageNextIntoTerminalClearing','TestV2PageNextIntoPartialErrorClears','TestV2RowsNextIntoPagingAndCloseInterrupt','TestV2PreparedOldPageKeepsMatchingPlan','TestV2DeliveredPageSurvivesSessionCloseUntilAdvance']),
 'metadata-fences-continuation':('Metadata/NO_METADATA shape/schema fences, separate buffered/streaming plans and retry/reprepare use exact canonical input plus continuation; no repair after output.', ['TestStep07StreamPreparedMetadataFencesAndPlanIsolation','TestStep07StreamReprepareCanonicalInputAndNoRepairAfterOutput','TestStep07StreamPagingCycleAndLimits']),
 'custom-decoder-error':('Explicit custom decoder errors after destination write are terminal, invalid rows never published, exact error retained, attempts=1 and no later destination access.', ['TestStep14CustomDecoderErrorAfterWrite','TestV2StreamingCustomPlansReuseCompiledChildren']),
 'reservation-lifecycle':('One configured pool slot reserved; shared orphan/writer/read owners acknowledged; idle/exhausted capacity and admissions joined before teardown.', ['TestV2ExclusiveWaitsForOrphanAndAcquisitionCancellation','TestV2ExclusiveAcquisitionDrainsAcceptedWriterOwnership','TestV2ExclusiveHandoffWaitsForWholeV5Segment','TestV2ExclusiveHandoffFinishesPartialHeader','TestStep07StreamIdleSessionDrainAndPoolExhaustion','TestStep06StrictStreamingReservationUsesExistingCapacity']),
 'last-write-cancel-close':('Close/cancel/session Close block until final destination access joins in all8 modes; no writes after return; generation cancellation cannot reach later page/shared use.', ['TestStep08StreamCloseDuringFinalCallerAccess','TestStep07StreamCloseJoinsDestinationAndConcurrentRejection','TestV2PageCancellationGenerationDoesNotReachLaterPageOrSharedUse','TestV2PageContextCancellationWinsAndJoinsRead','TestStep07CloseGenerationRejectsLateCancellation']),
 'no-output-replay':('Failures before/after first write and after delivered rows retain terminal error/progress; no transparent replay after output.', ['TestStep07StreamFirstRowBeforePageAndNoReplay','TestStep07StreamPartialFirstRowNeverReplays','TestStep07StreamReprepareCanonicalInputAndNoRepairAfterOutput','TestStep14CustomDecoderErrorAfterWrite']),
 'bounded-control-dispatch':('Actual control envelopes overflow/retire without blocking locked consumer; every charge released. Live schema event dispatch succeeds while cursor idle and its host pool exhausted.', ['TestV2ExclusiveControlOverflowPoisonsAndReleases','TestV2ExclusiveV5TrailingControlReuse','TestV2EventEnqueueCloseRaceReleasesEveryCharge','TestStep05OverflowHighestGenerationAndIsolation','TestStep07RunStreamLive']),
 'live-server-modes':('Actual native large blob/text/nested values, empty/multipage/metadata change, early Close/pool recovery and coordinator stop/join after row1; restart/shared execution recovery.', ['TestStep07RunStreamLive','TestV2ExclusiveTransportLive','TestStep13PublicExample']),
 'unsupported-frame-compression':('v3/v4 frame compression rejected before request submission; buffered compressed lanes remain real successful packages.', ['TestStep07StreamCapabilityBeforeQuery','TestV2UnsupportedCompressionRejectedBeforeStartupOrQuery','TestStep07RunStreamLive']),
 'bounded-fuzz':('12 actual campaigns: framing/cells/CRC/UTF8/capacity/metadata/paging/tablets/compression plus cancellation lifetime oracle. Configured payload, capacity, byte/history/depth limits; terminal stability/no panic.', ['FuzzStreamCells','FuzzStreamEnvelope','FuzzValidatedUnits','FuzzV2StreamingValues','FuzzV2ScalarUTF8','FuzzV2ExclusiveIncrementalFraming','FuzzStep07StreamExecutionBoundary','FuzzV2PagingHistoryBudgets','FuzzV2PageDestinationFraming','FuzzStep06TabletPayload','FuzzSpanCompression','FuzzStep14StreamingCancellationLifetime']),
}
# Accepted name is TestStep07LatestAttempt..., kept explicit rather than fuzzy matching.
GROUPS['simultaneous-byte-storage'][1][-1]='TestStep07LatestAttemptMetadataRemainsBudgetedDuringRetry'

def build(allow_incomplete=False):
 report=matrix.audit(allow_incomplete)
 declarations=matrix.read(matrix.TASK/'current-declarations.json')
 byname={d['Name']:d for d in declarations if d['Kind']=='func'}
 command_map={}
 for lane,receipt in report['native'].items():
  for c in receipt['commands']:command_map[lane+'/'+c['name']]=c
 for c in report['checks']:command_map['checks/'+c['name']]=c
 outcomes={k:matrix.package_outcomes(c) for k,c in command_map.items()}
 criteria=[];problems=list(report['problems'])
 for name,(requirement,tests) in GROUPS.items():
  refs=[]
  for test in tests:
   d=byname.get(test)
   if not d:problems.append('missing assertion test '+test);continue
   package='github.com/scylladb/gocql/v2'+('/'+str(Path(d['File']).parent) if Path(d['File']).parent!=Path('.') else '')
   passes=[]
   for command,result in outcomes.items():
    rows={n:a for (p,n),a in result.items() if p==package}
    if matrix.effective(rows,test):passes.append(command)
   if not passes:problems.append('no actual effective test pass '+test)
   assertions=[line.strip() for line in d['Source'].splitlines() if re.search(r'\b(t\.(Fatal|Error|Fail)|require\.|assert\.)',line)]
   refs.append(dict(test=test,file=d['File'],line=d['Line'],complete_source_sha256=d['SHA'],assertions=assertions,actual_effective_passes=passes))
  criteria.append(dict(criterion=name,requirement=requirement,tests=refs))
 # Require every supported deterministic mode, not merely a passing parent.
 unit=next(c for c in report['checks'] if c['name']=='unit-race')
 rows=matrix.package_outcomes(unit);root='github.com/scylladb/gocql/v2'
 modes=['v3','v4','v4tls','v5raw','v5lz4','v3tls','v5rawtls','v5lz4tls']
 for test in ['TestStep08StreamIndependentEverySplitAndTruncation','TestStep08StreamIndependentFirstRowGate','TestStep14StreamingStorageMatrix','TestStep14CustomDecoderErrorAfterWrite']:
  flat={n:a for (p,n),a in rows.items() if p==root}
  for mode in modes:
   if not matrix.effective(flat,test+'/'+mode):problems.append('missing required deterministic mode '+test+'/'+mode)
 for mode in modes:
  for action in ['close','cancel','session']:
   if rows.get((root,'TestStep08StreamCloseDuringFinalCallerAccess/'+mode+'-'+action))!='pass':problems.append('missing last-write lifetime mode '+mode+'/'+action)
 # Native supported modes must reach interruption/recovery, rather than
 # returning early through the expected frame-compression preflight branch.
 live=[('stable','v4-no-compression'),('stable','v4-no-compression-tls'),('stable','v3-maintenance-stream'),('stable-v3tls','v3-tls-stream')]+[('cassandra-'+m,m) for m in ['v5-no-compression','v5-lz4','v5-no-compression-tls','v5-lz4-tls']]
 live_outcomes=[]
 for lane,mode in live:
  receipt=report['native'].get(lane)
  command=next((c for c in (receipt or {}).get('commands',[]) if c['name'].endswith('-'+mode)),None)
  ok=bool(command and command['exit_code']==0)
  text=''.join(e.get('Output','') for e in matrix.events(command['log']) if e.get('Test')=='TestStep07RunStreamLive') if command else ''
  ok=ok and 'actual coordinator stop/join after one delivered row:' in text and 'actual schema event dispatched while cursor idle' in text and 'live metadata change: 4 -> 5' in text
  if not ok:problems.append('required actual live stream completion missing '+lane+'/'+mode)
  live_outcomes.append(dict(lane=lane,mode=mode,complete_interruption_metadata_event_recovery=ok))
 dev=report['native'].get('development',{})
 dev_text=''.join(e.get('Output','') for c in dev.get('commands',[]) if c['name'].endswith('-v4-development') for e in matrix.events(c['log']))
 if 'tablet v2 negotiated=true' not in dev_text or 'independently decoded leader=' not in dev_text:problems.append('actual tablet-v2/global/leader negotiation missing')
 storage=[]
 for event in matrix.events(unit['log']):
  if 'peak_driver_byte_storage=' in event.get('Output',''):storage.append(event['Output'].strip())
 if len(storage)!=40:problems.append('expected40 measured storage shapes')
 result=dict(source_sha256=report['source_sha256'],criteria=criteria,live_modes=live_outcomes,storage_measurements=storage,problems=problems,gate='passed' if not problems else 'pending',cost_scope='cost-comparison supplies equivalent row/cell result-shape allocation timings for Run/Row, NextInto and RunStream; static benchmark payload metrics are not live-memory measurements. All-mode live byte capacities, direct identities and validated-unit copy counts are separate assertions. Comprehensive independent CPU/throughput/peak-heap comparison remains step15.')
 matrix.write(matrix.E/'step14-streaming-acceptance.json',result)
 print(json.dumps(dict(criteria=len(criteria),storage_shapes=len(storage),problems=problems)))
 if problems and not allow_incomplete:raise SystemExit(1)
 return report,result
if __name__=='__main__':
 parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--allow-incomplete',action='store_true');build(parser.parse_args().allow_incomplete)
