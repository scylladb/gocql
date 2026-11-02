#!/usr/bin/env python3
"""Render the completed three-driver experiment without mixing prior runs."""
import json
from pathlib import Path
import sys

out=Path(sys.argv[1]).resolve()
state=json.loads((out/'state.json').read_text())
assert state['status']=='complete' and state['completed']==24
s=json.loads((out/'summary.json').read_text())
context=json.loads((out/'context.json').read_text())
rows=json.loads((out/'observations.json').read_text())
assert len(rows)==24 and all(not row[k] for row in rows for k in ('errors','timeouts','rejected'))
lines=['Three-driver local benchmark and CPU profiling','',
    'All three implementations were measured in this experiment; no numbers are reused from earlier reports.',
    'Sources: gocql/master '+context['master_head']+', current '+context['v2_head']+',',
    'scylla-go-driver '+context['scylla_head']+' plus the disclosed benchmark API-access shim.',
    'Two 30-second measurements per case, forward then reverse order, zero errors/timeouts/rejections.',
    'All clients: CPU 8, GOMAXPROCS=1, native v4, LOCAL_QUORUM, no TLS/compression/retries.',
    'Point and write: 64 workers. Paging: 4 workers, 128 rows x 4 KiB, pages of 32 plus empty terminal page.',
    'Point data: 100000 keys x 1 KiB. Full dataset validation before each run; SHA256 inside timing.',
    'Emulator: CPU 0,1,4,5, GOMAXPROCS=4, three hosts/one shard per host, no replication or persistence.',
    'Closed-loop throughput, not a fixed-rate sustainable-capacity claim. Values below are medians of two runs.','',
    'THROUGHPUT (operations/s; paged operation = a complete 128-row partition)',
    'Case | master | current buffered | scylla-go-driver (explicit pages, no prefetch)']
mapping=[('Point','point-master','point-v2-buffered','point-scyllago'),('Paged','paged-master','paged-v2-buffered','paged-scyllago-explicit'),('Write','write-master','write-v2','write-scyllago')]
for label,a,b,c in mapping:lines.append(f'{label} | {s[a]["throughput"]:.1f} | {s[b]["throughput"]:.1f} | {s[c]["throughput"]:.1f}')
lines+=['','FULL MEASUREMENTS','Case | workers | ops/s | CPU us/op | client CPU cores | allocated B/op | allocations/op | p99 us | throughput min..max']
tsv=['case\tworkers\tops_per_second\tcpu_us_per_op\tclient_cpu_cores\tallocated_bytes_per_op\tallocations_per_op\tp99_us\truns']
for name,v in sorted(s.items()):
    workers=4 if name.startswith('paged-') or name=='point-v2-stream' else 64
    cpu=v['cpu_us_per_op']*v['throughput']/1e6
    values=[name,workers,round(v['throughput'],3),round(v['cpu_us_per_op'],3),round(cpu,4),round(v['bytes_per_op'],3),round(v['allocs_per_op'],3),v['p99_us'],v['runs']]
    tsv.append('\t'.join(map(str,values)))
    lines.append(' | '.join(map(str,values[:-1]))+f' | {v["throughput_range"][0]:.1f}..{v["throughput_range"][1]:.1f}')
lines+=['','WHAT THE THIRD DRIVER CHANGES ABOUT THE CONCLUSION',
    'Scylla-go-driver does not outperform master or current at these worker counts. Its point/write CPU',
    'cost per operation is lower, but its client is largely idle. The unmodified writer batches requests',
    'and sleeps for 1 ms when its request queue is empty. This is a throughput/latency/CPU tradeoff,',
    'not evidence that lower CPU/op automatically means higher throughput. The causal contribution',
    'of batching versus other implementation differences has not been isolated by an A/B test.',
    'Master/current write coalescing is disabled; scylla-go-driver exposes no disable switch. Both',
    'already have buffering/coalescing facilities; this must not be described as a missing feature.',
    '',
    'For paging, scylla-go-driver allocates roughly a complete partition payload on every operation',
    'because ParseRowsResult/Buffer.readCopy copies cell bytes. It is not a good zero-copy model.',
    'The stock iterator, measured separately, keeps its mandatory prefetch and still uses the same',
    'copying decoder. Compare its separate row rather than silently treating it as no-prefetch paging.',
    '',
    'Useful implementation candidates remain: synchronous non-speculative execution; compact request',
    'lifetime state; reusable scalar/header encoding scratch; lightweight routing cursors; fewer',
    'scalar decoder wrapper copies; bounded streaming read-ahead. Current already implements many',
    'general optimizations, and no proposed production optimization was implemented or proven here.',
    '',
    'SCYLLA-GO-DRIVER ADAPTATIONS (all existing upstream source files unchanged)',
    *context['scylla_adaptations'],
    'Its session keyspace is bench_compare, required for its default-keyspace-only token routing.',
    'Master/current use fully qualified CQL; the same prepared CQL strings and data are used for all three.',
    'Scylla-go-driver always requests omitted prepared result metadata. Master/current preserve their',
    'metadata-ID-based safety policy; no metadata-ID extension is advertised by this emulator.',
    'There is no equivalent incremental streaming API in scylla-go-driver; current streaming rows',
    'are supplementary cases and are not relabeled as a scylla-go-driver streaming implementation.',
    '',
    'VALIDATION AND ARTIFACTS',
    'Five correctness modes passed: master, current buffered/streaming, scylla explicit/stock iterator.',
    'All 24 observations include process affinity records, client CPU and before/after heap profiles,',
    'a separate emulator CPU profile, workload JSON, and lossless raw HDR histograms.',
    'Readable CPU/allocation profiles are under each case directory. profile-attribution.json normalizes',
    'CPU samples by completed operations; cumulative entries overlap and must not be added together.',
    'context.json pins sources, builds and adaptations. setup-notes.json retains initial smoke failures.',
    'The original parked artifacts and retained driver commits remain unchanged.','']
(out/'REPORT.txt').write_text('\n'.join(lines))
(out/'comparison.tsv').write_text('\n'.join(tsv)+'\n')
print('\n'.join(lines[15:22]))
