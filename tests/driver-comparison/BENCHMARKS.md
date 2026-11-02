# Benchmark and test information

This describes the final parked harness. Earlier smoke/preflight binaries differ; use their own source/build records if examining them. See [progress](PROGRESS.md) and [findings](FINDINGS.md).

## Workloads and client equivalence

|Scenario|CQL/application operation|Dataset|Final measurement status|
|---|---|---|---|
|Point read|Prepared `SELECT k,r,p FROM bench_compare.point WHERE k=?`; consume and verify one 1 KiB blob|100,000 keys|6 maximum + 5 sustainable rounds complete|
|Point write|Prepared `INSERT INTO bench_compare.point (k,r,p) VALUES (?,?,?)`; await acknowledgement|Same 100,000-key space|No final pilots or recorded rounds|
|Paged read|Prepared `SELECT k,r,p FROM bench_compare.paged WHERE k=?`; consume all 128 × 4 KiB rows|1,024 partitions, page size 32|Correctness/probes passed; no final pilots or recorded rounds|

A paged partition is one operation; rows/s = ops/s ×128 and blob MiB/s = ops/s ×0.5. Payloads and key sequences are deterministic. Both adapters feed the same owned row consumer and perform full SHA-256, key/row ID, length and row-count checks inside operations. Immutable borrowed v2 input is closed/drained; buffered borrowed output is copied before consumption. Legacy blob decoding copies to preallocated caller storage. No map/reflection row mapping or unsafe borrowing.

RF=3/datacenter1, LOCAL_QUORUM, v4, plaintext, no compression, retries, speculation or prefetch. Zero write coalescing; serial consistency unset. One data connection per shard plus one control connection. Final metadata is refreshed before v2 preparation and token/key routing is checked. Packet captures prove representative equivalence, not every packet of timed traffic.

## Measurement protocol

- Pilots: concurrency 1/4/16/64/256/1,024; 5 s warmup +15 s measurement. Pick smallest concurrency within 3% of highest valid throughput. V2 read candidates get two extra trials at the selected setting; compare three-sample medians, with a ≤3% difference favoring buffered NextInto.
- Primary rounds: three scenarios × two drivers × two phases × three rounds =36. Each is 30 s warmup +120 s measurement. Driver order alternates.
- Maximum throughput is the median of three successful-completion rates. Sustainable target is 70% of that same driver’s median maximum.
- Open-loop arrivals are scheduled evenly; queue capacity is max(1,024, 4×workers), 1,024 for selected point reads. Retain scheduled-to-completion latency, dispatch latency, queueing, scheduling lag, rejections and schedule misses above 1 ms.
- Throughput counts successful completions inside the fixed window /120. Latencies retain all admitted operations through bounded draining. Operation context limit: 10 s; phase drain deadline: 15 s.
- HDR histograms: 1 µs–60 s, three significant digits, explicit overflow. Sample CV limits: throughput 5%; p90/p95/p99 10%. Sustainable acceptance also needs ≥99% target, no errors/rejections or growing backlog. Repeat an affected three-round set once and retain originals.
- Before every timed run: clear only bench_compare snapshots, truncate/reload identical target dataset, flush all nodes, verify one schema version across all nodes, observe no active/pending compaction or flush work, then validate/warm the complete target dataset. Loader pacing: 20,000 point rows/s or 5,000 paged rows/s.
- Metrics every five seconds: server reactor/I/O/compaction/commitlog/transport and host CPU/disk counters. Internal client affinity checks each second; runner samples threads about every 200 ms. Monitoring uses CPUs 2,18; every application client uses CPU8 and GOMAXPROCS=1. Profiling was not performed.

## Final point-read pilot sweep

|Workers|Legacy ops/s|V2 buffered ops/s|V2 stream ops/s|
|---:|---:|---:|---:|
|1|26,662.13|20,559.73|18,832.60|
|4|91,380.40|61,698.20|49,772.53|
|16|133,084.13|69,065.40|46,077.60|
|64|138,289.20|71,730.00|41,722.07|
|256|133,127.20|62,930.13|27,470.53|
|1024|122,207.53|47,560.27|10,637.93|

|Candidate|Selected workers|Pilot observations at selected setting, ops/s|Median ops/s|
|---|---:|---|---:|
|old buffered|64|138,289.20|138,289.20|
|v2 buffered|64|71,730.00, 71,429.67, 70,964.13|71,429.67|
|v2 stream|4|49,772.53, 49,810.27, 47,796.20|49,772.53|

All 22 final pilots had zero operation errors. Buffered v2 won; neither driver hit a rising-capacity limit. Writes and paged reads still need their own sweeps; the read selection must not be reused for them.

## Completed recorded rounds

|Result file|Successful ops/s|Errors|Rejected|Completed / drained|p90 / p95 / p99 ms|
|---|---:|---:|---:|---:|---:|
|[read-maximum-old-initial-1.json](results/2026-11-01/read-maximum-old-initial-1.json)|134,605.800|0|0|16,152,760 / 64|0.710 / 0.817 / 1.146|
|[read-maximum-old-initial-2.json](results/2026-11-01/read-maximum-old-initial-2.json)|135,388.775|0|0|16,246,717 / 64|0.704 / 0.805 / 1.132|
|[read-maximum-old-initial-3.json](results/2026-11-01/read-maximum-old-initial-3.json)|134,436.708|0|0|16,132,469 / 64|0.709 / 0.813 / 1.148|
|[read-maximum-v2-initial-1.json](results/2026-11-01/read-maximum-v2-initial-1.json)|68,929.292|0|0|8,271,579 / 64|1.672 / 1.852 / 2.293|
|[read-maximum-v2-initial-2.json](results/2026-11-01/read-maximum-v2-initial-2.json)|70,077.533|0|0|8,409,368 / 64|1.650 / 1.819 / 2.163|
|[read-maximum-v2-initial-3.json](results/2026-11-01/read-maximum-v2-initial-3.json)|66,999.250|0|0|8,039,974 / 64|1.731 / 1.916 / 2.359|
|[read-sustainable-old-initial-1.json](results/2026-11-01/read-sustainable-old-initial-1.json)|94,218.800|0|628|11,306,260 / 4|1.315 / 2.795 / 5.879|
|[read-sustainable-old-initial-2.json](results/2026-11-01/read-sustainable-old-initial-2.json)|94,223.633|0|48|11,306,840 / 4|2.113 / 3.527 / 6.091|
|[read-sustainable-old-initial-3.json](results/2026-11-01/read-sustainable-old-initial-3.json)|94,222.800|0|147|11,306,741 / 5|1.634 / 3.015 / 6.023|
|[read-sustainable-v2-initial-1.json](results/2026-11-01/read-sustainable-v2-initial-1.json)|48,230.758|0|1,984|5,788,077 / 386|1.664 / 4.155 / 13.463|
|[read-sustainable-v2-initial-2.json](results/2026-11-01/read-sustainable-v2-initial-2.json)|48,247.892|0|311|5,789,750 / 3|2.028 / 5.255 / 13.943|

Maximum rows use operation latency; sustainable rows use scheduled-arrival-to-completion latency. Dispatch percentiles and schedule/queue statistics remain in JSON/CSV. Interrupted `read-sustainable-v2-initial-3` has no result and is excluded.

## Tests and verification

|Test/check|Coverage / observed result|
|---|---|
|Go TestOwnedValidation, both builds, race detector|Valid row accepted; full-payload corruption and wrong row ID rejected|
|Go TestWindowAndDrain, both builds, race detector|Completion-window versus drain accounting, all admitted requests joined, histogram count|
|Go TestBoundedScheduledQueue, both builds, race detector|Queue stays bounded under overload, rejects arrivals, scheduled latency includes queue delay|
|Python test_stability|Percentile CV violation detected|
|Python test_incomplete|Incomplete round set rejected|
|Python test_capacity_is_not_target|98% delivery fails 99% target requirement|
|Python test_rejections_invalidate_sustainable|Any rejected arrival invalidates sustainable acceptance|
|Python test_raw_histograms_survive_compaction|Compressed raw snapshot preserved and compaction idempotent|
|go vet, both adapter builds|Passed|
|Python py_compile|All benchmark Python scripts passed|
|Live correctness|Insert/overwrite/readback/missing-key/paging passed for legacy, buffered v2 and streaming v2|
|Wire/connection probes|Matching CQL/values/flags/consistency/page size; counts 1/1/5; six data connections plus control|
|Metadata diagnostics|No token before full schema loading; stale handle fails after loading; corrected preparation passes frequent-refresh and 75 s controls|

Parking re-ran the harness race tests, vet and Python checks successfully; see [PARK-VERIFICATION.md](results/2026-11-01/PARK-VERIFICATION.md). These are harness checks, not a full driver repository test suite, not new integration performance measurements, and not completion of the 36-run audit.

Commands (from repository root):

```sh
GOMAXPROCS=1 GOWORK=off taskset -c 8 go -C tests/driver-comparison test -race -count=1 ./...
GOMAXPROCS=1 GOWORK=off taskset -c 8 go -C tests/driver-comparison test -race -tags old -count=1 ./...
GOWORK=off go -C tests/driver-comparison vet ./...
GOWORK=off go -C tests/driver-comparison vet -tags old ./...
python3 -m unittest discover -s tests/driver-comparison -p 'test_*.py' -v
python3 -m py_compile tests/driver-comparison/*.py
```

The parking evidence audit checked **11 completed rounds, 88 histogram snapshots, 22 valid pilots, 110 completed process records and 15 source identities**. It confirmed matching final wire parameters. [park-audit.json](results/2026-11-01/park-audit.json) records these checks. The interrupted metric gzip may be missing its trailer; preserve it as partial evidence.

## Source, build and machine identity

- Legacy source: `1ceaac8172bda26e8b8d701ac356982d33729b27` (remote master was checked at setup).
- V2 source: `5a07badd5f3a567f3990e0c70aba6c8d28e7fbc0`.
- Compiler: `go version go1.25.0 linux/amd64`; Linux/amd64, GOAMD64=v1, CGO_ENABLED=1; `-trimpath -buildvcs=false`, legacy adds `-tags old`, GOWORK=off.
- Legacy binary SHA-256: `075c14aae858b6887d659a8404bf297a9b39fd33aa7f6d6caad5864cfa5ea98d`.
- V2 binary SHA-256: `357bd2b296d7c99994b0c0a516bc9ac122ca65c89df3c32bf819a45379c7fe6f`.
- Host: AMD Ryzen 9 9950X3D, 16 physical cores /32 logical CPUs. Server groups 0–1,4–5,6–7 share L3 domain0; client8 belongs to L3 domain1, SMT sibling24 excluded from benchmark server/monitoring affinity.
- Scylla 2026.3.4 image: `docker.io/scylladb/scylla@sha256:2a3cf99b55d793a41e19b4d8afa4084744bc05517f4b4a211483c8d2abe4ef3a`; two shards/node, 3 GiB Scylla RAM, 5 GiB container limit, no swap allowance.
- Storage: ext4 on CT2000P310SSD8 NVMe; no artificial disk budget. Final setup had 69,674,835,968 free bytes (~64.9 GiB), filesystem 97% full. No injected netem on final paths; cgroup chains checked for CPU/I/O throttles.
- Physical calibration: read IOPS361,523; write IOPS161,533; read bandwidth7,290,835,456 bytes/s; write bandwidth6,301,072,384 bytes/s. Calibration reported 13% random-write deviation; properties are calibration inputs, not workload throughput. Device nomerges=0 and write-back cache were retained.

## Raw artifacts

Each successful measurement has `.json`, `.histograms.json.gz`, `.json.log`, `.json.process.json`, `.metrics.jsonl.gz`, preparation/warm logs, schema agreement text and settling JSON. `environment.json` contains build options, topology, cgroups, qdiscs, disk and source hashes. `sources/` matches the original hashed source snapshot. `probes/` contains pcaps, decoded requests, startup options, system.clients snapshots and sockets. Data/binaries/worktree stay under `.codex-cache/driver-comparison/`; existing clusters’ preserved container configuration is in `preserved-containers.json`.
