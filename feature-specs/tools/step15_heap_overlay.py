# Copyright 2026 The Gocql Authors. All rights reserved.
# Use of this source code is governed by a BSD-style license.
"""Sample live GC heap at every real Read in all 40 accepted stream shapes.

A retained Go overlay adds only diagnostic sampling to the accepted generated
source/storage test. Production and normal benchmark sources remain byte exact.
This is deliberately excluded from latency/CPU measurements. Source generation
has one bounded unit; final caller storage and fixed TLS metadata are labeled.
"""
import hashlib,json,os,subprocess,time
from pathlib import Path
ROOT=Path(__file__).resolve().parents[2]
OUT=Path(os.environ.get('GOCQL_STEP15_CHECKS','/extra/scylladb/gocql-api-test-cluster/step15/checks'));OUT.mkdir(exist_ok=True)
original=ROOT/'step14_stream_acceptance_test.go'
body=original.read_text().replace('"net"','"net"\n "runtime"',1)
needle='func TestStep14StreamingStorageMatrix(t *testing.T) {'
assert needle in body
body=body.replace(needle,needle+'\n runtime.GC();var initialHeap runtime.MemStats;runtime.ReadMemStats(&initialHeap)',1)
needle='var peak int64\n\t\t\t\t\tsample := func() {'
assert needle in body
body=body.replace(needle,'var peak int64\n var peakHeap uint64\n\t\t\t\t\tsample := func() {\n runtime.GC();var m runtime.MemStats;runtime.ReadMemStats(&m);peakHeap=max(peakHeap,m.HeapAlloc)',1)
needle='t.Logf("mode=%s rows=%d cell=%d skipped=%v slow=%v peak_driver_byte_storage=%d'
assert needle in body
body=body.replace(needle,'t.Logf("heap_initial_runtime=%d heap_peak_postGC=%d heap_driver_TLS_source_harness_inclusive_delta=%d source_unit_payload<=130000 source_unit_encoded<=131071 producer_transient_encoding_INCLUDED final_output=%d",initialHeap.HeapAlloc,peakHeap,int64(peakHeap)-int64(initialHeap.HeapAlloc)-int64(func()int{if work.skip{return 0};return work.size}()),func()int{if work.skip{return 0};return work.size}())\n '+needle,1)
replacement=OUT/'step14_stream_acceptance.overlay.go';replacement.write_text(body)
overlay=OUT/'heap-overlay.json';overlay.write_text(json.dumps({'Replace':{str(original):str(replacement)}},indent=2)+'\n')
command=['go','test','-overlay',str(overlay),'-tags','unit','-run','^TestStep14StreamingStorageMatrix$','-v','-count=1','-timeout=6m','.']
start=time.monotonic();log=OUT/'live-heap-all-shapes.log'
with log.open('wb') as handle:status=subprocess.run(command,cwd=ROOT,env=dict(os.environ,GOMAXPROCS='4'),stdout=handle,stderr=subprocess.STDOUT).returncode
sha=lambda p:hashlib.sha256(Path(p).read_bytes()).hexdigest()
receipt=dict(command=command,exit_code=status,seconds=time.monotonic()-start,log=str(log),sha256=sha(log),original=str(original),original_sha256=sha(original),diagnostic_overlay=str(replacement),diagnostic_overlay_sha256=sha(replacement),overlay=str(overlay),overlay_sha256=sha(overlay),scope='all40 generated shape/mode cases; read-site forcedGC heap including driver/TLS/source/harness; final app bytes separately deducted; static actual-capacity gate unchanged; zero throughput/latency claim')
(OUT/'live-heap-overlay.json').write_text(json.dumps(receipt,indent=2)+'\n');print(json.dumps(receipt),flush=True)
raise SystemExit(status)
