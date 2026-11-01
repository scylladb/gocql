# Copyright 2026 The Gocql Authors. All rights reserved.
# Use of this source code is governed by a BSD-style license.
"""Prove exactly which accepted pre-policy mode paths the final delta affects."""
import hashlib,json,subprocess
from pathlib import Path
ROOT=Path(__file__).resolve().parents[2];TASK=Path('/extra/scylladb/gocql-api-test-cluster/step15');OLD=TASK/'pre-admission-source'
sha=lambda p:hashlib.sha256(Path(p).read_bytes()).hexdigest()
def main():
 old=json.loads((TASK/'pre-admission-source.json').read_text());changed=[p for p,h in old.items()if sha(ROOT/p)!=h]
 production=[p for p in changed if p.endswith('.go')and not p.endswith('_test.go')]
 assert set(production)=={'internal/wire/receive_cache.go','internal/wirecompress/lz4.go','internal/wirecompress/snappy_frame.go','internal/transport/v2_transport_receive.go'},production
 for path in ['step15_performance_integration_test.go','step15_concurrent_memory_integration_test.go','step15_cache_ownership_integration_test.go','internal/settings/v2_config.go','internal/settings/connectivity.go','internal/cluster/session.go','internal/transport/v2_connection_state.go']:
  assert sha(ROOT/path)==old[path],path
 files=['internal/wire/receive_cache.go','internal/wirecompress/lz4.go','internal/transport/v2_transport_receive.go','internal/segment/spans.go','internal/segment/units.go','internal/transport/v2_stream_transport.go','internal/transport/v2_connection_state.go']
 def funcs(root):return json.loads(subprocess.check_output(['go','run','feature-specs/tools/step15_functions.go',str(root)]+files,cwd=ROOT,text=True))
 before,after=funcs(OLD),funcs(ROOT);unchanged=[k for k,v in before.items()if after.get(k)==v]
 required=['internal/wirecompress/lz4.go::LZ4Compressor.AppendDecompressed','internal/segment/spans.go::ReadOwnedPayload','internal/transport/v2_transport_receive.go::*Conn.InternalReadTypedSegmentPayload','internal/transport/v2_connection_state.go::*Conn.InternalReserveStream','internal/wire/receive_cache.go::*ReceiveCache.Clear','internal/wire/receive_cache.go::*ReceiveCache.Close']
 # The reserve method's exact name is recorded by the parsed receiver map.
 required=[k for k in required if not k.endswith('.InternalReserveStream')]
 assert set(required)<=set(unchanged),set(required)-set(unchanged)
 for path in ['internal/segment/units.go','internal/transport/v2_stream_transport.go','internal/transport/v2_connection_state.go']:
  assert all(k in unchanged for k in before if k.startswith(path+'::')),path
 old_receive=before['internal/transport/v2_transport_receive.go::*Conn.receiveTypedCall'];new_receive=after['internal/transport/v2_transport_receive.go::*Conn.receiveTypedCall']
 # Real v5 frame sources hold their segment leases. This branch does not enter
 # raw body acquisition, frame Decode/DecodeTo or ReceiveCache.Release.
 start='else if src.InternalOwner != nil {';stop='} else {'
 def branch(source):
  at=source.index(start);begin=source.index('{',at);depth=1;end=begin+1
  while depth:
   if source[end]=='{':depth+=1
   if source[end]=='}':depth-=1
   end+=1
  return source[at:end]
 assert branch(old_receive)==branch(new_receive)

 proof={'old_source_sha256':sha(TASK/'pre-admission-source.json'),'old_native_receipts':['accepted/cassandra.json'],'changed_inputs':changed,'changed_production_inputs':production,'unchanged_function_sources':{k:hashlib.sha256(before[k].encode()).hexdigest()for k in unchanged},'v5_buffered_path':['Conn.InternalReadTypedSegmentPayload -> segment.ReadOwnedPayload','segment ReadOwnedPayload -> existing LZ4Compressor.AppendDecompressed for transformed units','frameSource.InternalOwner != nil -> Retain original owner/body; no raw frame cache/decode branch','result owner is segment lease/combined lease, whose finalizers release segment owners, not ReceiveCache.Release'],'streaming_path':['unchanged exclusive reservation/cache.Clear and restore','unchanged direct uncompressed reader and v5 UnitReader','unchanged AppendDecompressed integrity-unit transform','no buffered receiveTypedCall/DecodeTo adapter'],'matched_normal_harness_and_settings_byte_identical':True,'claims':['pre-policy v5 normal throughput/latency/baseline and direct-stream metrics remain on unchanged executed data paths/configuration','final changed-source full correctness/race/copy/heap/fuzz checks still required','all changed v3/v4 raw/compressed normal/memory/ownership modes rerun on final source','step17 full final-release native matrix remains mandatory']}
 (ROOT/'feature-specs/evidence/step15-v5-scope-proof.json').write_text(json.dumps(proof,sort_keys=True,indent=2)+'\n');print(json.dumps({'production_delta':production,'unchanged_functions':len(unchanged),'v5_scope':'verified'}))
if __name__=='__main__':main()
