# Findings: one client vCPU driver comparison

Parked 2026-11-01T14:15:38.255021+00:00. This is an incomplete experiment, not a completed 36-run comparison. See [overall progress](PROGRESS.md), [benchmarks and tests](BENCHMARKS.md), and [resume instructions](RESUME.md).

## Accepted point-read capacity result

Both drivers used 64 workers. V2 used buffered `Run` / `NextInto` with an explicit copy to owned reusable row storage. Results below are medians with min–max across three 120-second measurement windows, each following 30 seconds of warmup. One operation is one prepared SELECT of a validated 1 KiB blob.

|Driver|Successful ops/s|p90 ms|p95 ms|p99 ms|Throughput sample CV|p99 sample CV|
|---|---:|---:|---:|---:|---:|---:|
|Legacy master|134,605.800 (134,436.708–135,388.775)|0.709 (0.704–0.710)|0.813 (0.805–0.817)|1.146 (1.132–1.148)|0.377%|0.763%|
|V2|68,929.292 (66,999.250–70,077.533)|1.672 (1.650–1.731)|1.852 (1.819–1.916)|2.293 (2.163–2.359)|2.265%|4.390%|

V2 throughput was **48.8% lower**; median maximum-load p99 was **100.1% higher**. All six rounds had zero operation errors/timeouts, successful drain accounting, no histogram overflow, and verified application affinity. Both sets meet throughput CV ≤5% and p90/p95/p99 CV ≤10%. This result applies to this pinned point-read workload and environment; writes and paged reads were not measured in the final matrix.

## Client and server resources

|Maximum-read medians|Legacy|V2|
|---|---:|---:|
|Client CPU, % of one CPU|99.43|99.94|
|Allocated bytes/completed operation|1,993.40|17,289.67|
|Allocations/completed operation|26.04|79.01|
|GC cycles per measured phase|8,514.00|28,861.00|
|Server CPU cores, sum over nodes/shards|3.34|1.91|

Client resource deltas include bounded draining. Server summaries use approximately 115 seconds between metric samples inside each 120-second window. Reported server read/write queue byte rates were zero in these fully warmed reads. Allocation and GC differences are observations, not a demonstrated cause of the regression. No profiling was performed.

## Sustainable read observations: not accepted

**Rates are 70% of each driver’s own capacity: legacy 94,224.06 ops/s; v2 48,250.50 ops/s. Latency is being compared at different absolute loads.**

|Driver / round|Successful ops/s|% target|Errors / timeouts|Rejected arrivals|Scheduled p90 / p95 / p99, ms|Dispatch p99, ms|End queue|
|---|---:|---:|---:|---:|---:|---:|---:|
|old / 1|94,218.800|99.99442%|0 / 0|628|1.315 / 2.795 / 5.879|1.076|0|
|old / 2|94,223.633|99.99955%|0 / 0|48|2.113 / 3.527 / 6.091|1.174|0|
|old / 3|94,222.800|99.99866%|0 / 0|147|1.634 / 3.015 / 6.023|1.128|0|
|v2 / 1|48,230.758|99.95908%|0 / 0|1,984|1.664 / 4.155 / 13.463|2.327|322|
|v2 / 2|48,247.892|99.99459%|0 / 0|311|2.028 / 5.255 / 13.943|2.411|0|

All five completed sustainable rounds achieved at least 99% of their own target and had zero operation errors/timeouts, but **every round rejected arrivals**. Each queue reached its 1,024-entry bound. Legacy p90 CV is 23.805% and p95 CV is 12.068%, also exceeding the 10% limit; its p99 CV is 1.805%. Legacy sustainable is a complete failed initial set. V2 sustainable has only two complete rounds; round three was interrupted and has no JSON result. Both drivers require the planned full-set repeat. There is no accepted sustainable-latency conclusion yet.

The first v2 sustainable round finished with 322 queued requests and drained 386 successful requests after the window. Other completed rounds ended with an empty waiting queue, while a few in-flight operations still drained. Queueing, scheduling lag, and dispatch-to-completion latency are retained separately. The origin of rejected bursts has not been established; there are no profiles proving scheduler, GC, host contention, or transport causality.

## Correctness, equivalence, and preparation findings

1. Final correctness checks passed for legacy, buffered v2, and streaming v2: insert, overwrite, readback, missing keys, and all 128 paged rows. Full measured payload validation checks key, row ID, length, count, and SHA-256.
2. The three final wire probes have matching representative CQL, bound payload hashes, consistency, flags, and page size. Each point operation has one EXECUTE. Each paged read has **five EXECUTEs**: four 32-row data pages and one terminal empty page. Protocol is v4, plaintext, with no negotiated compression. Connection inventories show six data connections (one per shard on each of three nodes) plus one control connection.
3. Default legacy write coalescing (200 µs) and default v2 serial consistency initially differed. Final adapters use zero coalescing and unset serial consistency. Legacy reuses query templates, args and scan storage; its public WithContext copy is reassigned so the prepared cache follows the reused template. V2 closes and waits for borrowed bindings.
4. **V2 preparation requires populated table metadata for this workload.** A public-API reproduction found zero loaded tables and no routing token before RefreshMetadata. Loading metadata supplied two tables; the old handle then returned SchemaChangedError while a newly prepared handle had a token and succeeded. The final adapter calls RefreshMetadata before preparing and asserts token routing; legacy asserts a four-byte routing key.
5. With the corrected initialization, a 1-second periodic-refresh diagnostic for 5 seconds and a 75-second event-driven control both completed without errors. Final recorded runs disable periodic polling for matched event-driven behavior. The earlier long runs with uninitialized metadata are excluded; this was a benchmark setup correction, not a modification of either pinned driver.
6. The provisioning helper does not configure the Scylla I/O scheduler when neither an I/O budget nor physical properties are supplied. Initial startup logs exposed that. Physical NVMe calibration was applied before the final comparison.
7. An unpaced paged-data load timed out at LOCAL_QUORUM during setup. Logs included a queued-work warning, and the filesystem was near full. These are observations, not proof of one root cause. Clearing only benchmark-keyspace snapshots recovered about 15 GiB. Pacing untimed preparation at 20,000 point rows/s or 5,000 paged rows/s then passed loading and complete readback.

## Limitations and excluded evidence

Only point-read capacity has a completed accepted three-round comparison. The matrix stopped after 11 completed primary rounds out of 36. No final write or paged-read pilots/recorded rounds ran. Earlier short smoke measurements used earlier adapter settings and must not be promoted into final performance claims. CPU affinity is enforced for application clients and benchmark monitoring/server work; unrelated services remained running. NVMe was shared with those services and about 97% full at final setup.

See [BENCHMARKS.md](BENCHMARKS.md) for exact workloads, timings, tests, pilot rates, build identities and paths. Raw evidence remains in [the final run directory](results/2026-11-01/); [REPORT.md](results/2026-11-01/REPORT.md) is corrected to mark the legacy sustainable set failed. The pre-parking report had prematurely treated an unassessed complete set as stable; it is retained only as superseded evidence.
