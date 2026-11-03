# Reusable-query redesign: implementation and evidence

**Historical implementation snapshot. Optimization has resumed under the
[measured convergence policy](CONVERGENCE.md).**

**Status: incomplete and experimental. No performance change is accepted and no
release was published.** The implementation improves original v2 in local
screening, and these historical observations compare with pinned master. The former
master-win requirement is superseded. Real Scylla acceptance is also blocked by the unchanged write
control. [Machine-readable state](REDESIGN-STATE.json) records the remaining work.

## Source and environment

- Original production source: `7e1ad6f718929342200c5b9bc126e5a5e7fc7d1c`.
- Master: `1ceaac8172bda26e8b8d701ac356982d33729b27`.
- Toolchain: Go 1.25.0, Linux/amd64. Client CPU 8, GOMAXPROCS=1; emulator CPUs
  0,1,4,5, GOMAXPROCS=4; monitoring CPUs 2,18; development/tests CPUs 10–15.
- New results: [redesign-20261102T124236Z](results/redesign-20261102T124236Z/status.json).
- Frozen original source/harness/build: `.codex-cache/driver-comparison/redesign-original/`.
  Frozen master, wave 2, wave 3 and final build manifests remain in the comparison
  cache; source and binary SHA-256 values are recorded with each observation.
  Earlier parked evidence was preserved.
- Canonical settings remain native v4, LOCAL_QUORUM, no compression/TLS,
  configured coalescing zero, page size 32, prefetch zero, no application retries
  or speculation, the same connection budget and seeded payload validation.
  A paged operation consumes all 128 rows and validates exact counts and SHA-256.
  Master comparisons for streaming decoders use its buffered adapter at **four**
  workers. Its 64-worker read is a separate baseline.

## Implemented

`NewQuery` / `NewUnpreparedQuery` create worker-owned reusable queries with
parameter-taking Exec, Run, RunPage and RunUnpaged methods. Prepared plans stay
shareable. Admission copies input, captures cell boundaries during encoding,
and computes routing from those bytes. Concurrent use returns ErrBusy. Close
stops admission and cancels execution; Wait joins accepted writers and callbacks.
Generation checks prevent stale cursors from affecting reuse. BatchEntry captures
an owned immutable member immediately; public batches also own option payloads.
The public Bound/binding execution API is removed. Historical protocol fixtures
retain test-only internal seams, which are absent from the normal public build.

Queries register with the session once. The ordinary path embeds request,
logical execution and routing cursor state. It executes attempts directly when
observers, speculation and routing latency observation are disabled; optional
features retain the general path. Acknowledgements avoid row/page construction.
Closed queries remain registered until their accepted work drains.

Connections use persistent submission writers and pooled transport tickets.
Queued cancellation prevents transmission; accepted writes keep packet/input
storage alive after caller cancellation. Connection loss before transmission
remains retryable. Partial-write errors retire the connection and preserve
per-request byte accounting. Zero coalescing drains at most 16 immediately ready
requests or 64 KiB, with no timer; an individual larger frame stands alone.
Configured coalescers skip queued cancelled frames before flushing.

Shared receive uses an 8 KiB reader. Exclusive incremental acquisition explicitly
transfers unread buffered bytes at a checked frame/unit boundary and restores
remaining bytes before shared reads resume. Streaming-decoder queries are
page-buffered by default on normal multiplexed connections. Explicit incremental
mode remains available; the archived bounded native read-ahead and cancellation
cause fixes were ported. Integer/blob fusion, reusable owned blob output with
RowInto, exact validated metadata reuse and compiled-plan reuse are implemented.
Other built-in scalars retain their checked generic path.

Receive caching now permits 64 objects while retaining the 16 MiB total and 9 MiB
individual idle ceilings. Session metrics expose hits, misses and retention.
Idle parameter backing, including reusable cell/offset descriptor storage, is
bounded by 64 KiB per query. Shared reader memory is
8 KiB per connection; exact metadata reuse retains at most 64 KiB of encoded
metadata plus its immutable decoded columns per connection. Default streaming
memory is proportional to concurrent response pages within frame/decompression
limits. Incremental scratch and shared-connection storage are separate from that
full-page memory demand. Saturation/oversize/epoch and ownership tests cover idle
cache bounds; a long-run RSS plateau is still outstanding.

README, API contract, user guides, examples and benchmark adapters use the new
query model. The standalone streaming example accepts `-incremental` explicitly.
The comparison adapter accepts `-stream=true -incremental=true` for separate
incremental observations.

## Real Scylla baseline gate

Only benchmark-owned storage properties and containers were changed. Original
properties and inspections are retained. A new 120-second iotune evaluation
completed; the benchmark nodes used its resulting physical I/O properties.
CPU/memory allocation, RF3, server write timeouts, workers, queue bounds and the
52,503.01 ops/s control were not relaxed. The host still has one shared NVMe
filesystem at roughly 92–93% usage.

With development isolated from measurement CPUs, original v2 delivered two
healthy 30-second maxima: **80,358.40** and **80,910.17 ops/s**. The first unchanged
52,503.01/s control passed. The second rejected **759 arrivals**, with peak queue
1,024 and zero server/client operation errors. Qualification stopped; the required
three healthy maxima and three controls were not obtained. Earlier preliminary
observations were retained and explicitly excluded because development CPU
isolation had not yet been established.

One-second server metrics, device counters and all readable cgroup I/O counters
are retained in [qualification evidence](results/redesign-20261102T124236Z/qualification-isolated/qualification.json)
and [storage summary](results/redesign-20261102T124236Z/storage-summary.json). The failed control
had peak sampled device write await 1.84 ms and average I/O queue 11.32. These
observations do **not** establish storage as the cause of rejected arrivals.
The sustained baseline remains unqualified. Dedicated benchmark storage or a
host is the prescribed fallback prerequisite; no unrelated service was changed.

## Local throughput screening

Each table is a separate frozen revision and comparison. ABBA screening comprises
two A–B/B–A pairs, each observation 30 seconds. All 40 observations across both
matrices completed without errors, timeouts or rejected arrivals. Ratios below
are ratios of variant medians, not final six-pair confidence bounds.

Original v2 versus wave 2:

| Case | Workers | Original v2 ops/s | Candidate ops/s | Ratio |
|---|---:|---:|---:|---:|
| Point read, buffered | 64 | 81,518.08 | 95,910.62 | 1.177 |
| Point read, streaming decoder | 4 | 46,031.68 | 84,099.00 | 1.827 |
| Acknowledged write | 64 | 88,035.23 | 104,533.02 | 1.187 |
| Complete partition, buffered | 4 | 2,815.82 | 3,193.80 | 1.134 |
| Complete partition, streaming decoder | 4 | 1,814.40 | 2,940.40 | 1.621 |

Pinned master versus wave 3:

| Case | Workers | Master ops/s | Candidate ops/s | Ratio |
|---|---:|---:|---:|---:|
| Point read, buffered | 64 | 129,008.98 | 97,417.17 | 0.755 |
| Point read, streaming decoder | 4 | 123,305.38 | 85,435.78 | 0.693 |
| Acknowledged write | 64 | 138,936.97 | 105,096.78 | 0.756 |
| Complete partition, buffered | 4 | 3,383.02 | 3,224.88 | 0.953 |
| Complete partition, streaming decoder | 4 | 3,401.93 | 2,931.70 | 0.862 |

These historical screens did not justify confirmation under the former policy. CPU/op, allocation counts/bytes, raw histograms, profiles, process
affinity and emulator boundary snapshots are retained with each observation.
Maximum-rate p99 values are descriptive; they do not qualify a matched-offered-rate
latency gate. New-revision fixed-rate controls, RSS/syscall accounting and healthy
real-Scylla comparisons remain outstanding.

Later correctness changes (retired capture cleanup, stream limit propagation,
uninitialized-query validation, descriptor retention limits and queued connection-loss classification) are
covered by tests. Their final source snapshot has not been reconfirmed against
master; no final-tree performance claim is made.

## Explicit incremental receive

These separate 30-second observations use four workers, the same dataset,
validation, connection budget and configured coalescing. They have no acceptance target
and are individual diagnostics, not paired acceptance evidence.

| Case | ops/s | CPU µs/op | Allocated bytes/op | Allocations/op |
|---|---:|---:|---:|---:|
| point-incremental | 58,827.83 | 16.76 | 10,981.61 | 120.62 |
| paged-incremental | 2,428.73 | 386.59 | 29,401.04 | 278.79 |

Both observations had zero errors, timeouts and rejected arrivals. They measured
the frozen delivery build, separately from default page buffering.

## Verification

- Default tests, full unit race tests and vet passed.
- Query ownership/cancellation/reuse tests passed 50 race repetitions.
- Comparison harness race tests passed on CPU 8/GOMAXPROCS=1 for both v2 and master;
  harness vet and 18 Python tests passed.
- Fragmented page-buffered protocol/TLS tests cover v3/v4/v5, v5 LZ4, and v3/v4
  frame compression. Existing malformed-input, reprepare, retry, speculation,
  metadata fencing and cancellation suites remain in the full test run.
- The new RF3 live **protocol-v4 race test passed** reusable writes/reads,
  overlapping rejection, stale cursors, owned output, explicit paging/unpaged
  reads, page-buffered and incremental streams, batch snapshots and verified
  namespace cleanup.
- Live protocol v5 is unavailable on this benchmark image: it replies with v4
  when v5 is explicitly requested. The failed attempt is retained, not a pass.
  V5/TLS/compression fixture coverage is independent of that limitation.
- Earlier live attempts exposed setup/cleanup metadata publication races. They
  remain retained. The final test waits for schema agreement, retries only a
  bounded read-only refresh, and verifies cleanup independently without replaying
  successful DDL. No production refresh exception was introduced.

Logs and exact checks are linked in [status.json](results/redesign-20261102T124236Z/status.json).
All benchmark nodes and emulator processes were stopped after the checks;
unrelated running services were preserved. No push or release was made. The
local parking commit preserves the experimental changes for explicit resumption.

## Work still required

The implementation is a working-tree redesign, not an accepted optimization
series. Remaining work includes request header/framing reuse with transport-safe
ownership, moving remaining cursor state behind lightweight generation handles,
remaining scalar fusion, and further allocation/CPU reductions. Reprofile those
costs and qualify changes independently.

The active prerequisites and retention rules are in [CONVERGENCE.md](CONVERGENCE.md).
They require a dedicated real benchmark host, unprofiled comparisons, frozen master
matched rates, and 120-second confirmation windows. Master is a comparison
reference, without a required winning margin. The task remains incomplete.
