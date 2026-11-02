# New-driver profiling and optimization

This new experiment follows the user's request for 30-second v2-only runs and
a fresh agent for every optimization iteration. The parked legacy comparison
and its frozen results remain historical, incomplete observations; they are not
being resumed as part of this optimization experiment.

## Iteration 01 — buffered point read

Agent: `/root/read_iteration_01`. One production change: reduce parameter
encoding slabs from 4,096 to 256 bytes in `internal/binding/v2_statement.go`.
An int32 partition key previously retained a 4 KiB slab for eight encoded bytes.
Builder ownership, borrowed payload lifetimes, wire framing, and larger scalar
support remain covered by the existing implementation and codec/wire tests.

Both observations used CPU8, GOMAXPROCS=1, 64 buffered workers, LOCAL_QUORUM,
the same 100,000-key/1 KiB dataset and complete row validation. Every measured
phase was 30 seconds with zero timed warmup. Dataset reset, flush, schema
agreement, settling and complete validation/warming were outside measurement;
the baseline setup is retained separately in `optimization-read-01-setup`.
Only v2 was executed. CPU and counters include bounded drain (64 operations),
and exclude connect, priming, dataset work, and heap snapshot collections.

|Metric|Before|After|Change|
|---|---:|---:|---:|
|Successful ops/s|70,959.8|70,778.0|−0.26%|
|Allocated bytes/completed op|17,289.87|13,449.75|−22.21%|
|Allocations/completed op|79.014|79.012|Effectively unchanged|
|GC cycles|6,556|5,174|−21.08%|
|CPU µs/completed op|14.085|14.117|+0.23%|
|p99 µs|2,141|2,435|+13.73%|
|Operation errors|0|0|—|

Retained for the reproducible 3,840-byte allocation reduction per operation.
These are single 30-second observations: no throughput gain is established, and
the p99 increase needs repeated evaluation before any latency conclusion.
Do not report the change as a speedup or imply that point-read optimization is
finished. The smaller parameter slab can affect all typed read/write bindings;
write and paged workload performance still needs measurement, particularly
larger scalar/composite bindings that may span more slabs.

The baseline allocation profile attributed 38.52% of sampled bytes to
`wire.(*Builder).writable`, including ~24% through parameter encoding. After the
change that site was 20.60%. The observed ~3,840-byte/op reduction agrees with
the source change. Baseline CPU was dominated by syscalls (26.19% flat), with
allocation/GC work also substantial. After-change syscalls were 27.86% flat.

Evidence:

- [Baseline run](results/optimization-read-01-before/measurement.json) and
  [after run](results/optimization-read-01-after/measurement.json).
- [Machine-readable comparison](results/optimization-read-01-summary.json).
- Both result directories retain CPU pprof, before/after heap pprof, flat and
  cumulative CPU reports, allocation byte/object reports, retained heap reports,
  server/host metrics, raw histograms, client thread-affinity observations,
  binary hashes/build settings, source hashes, driver diff and harness snapshot.
- Setup process evidence is in `results/optimization-read-01-setup/`.
- Retained binaries: `.codex-cache/driver-comparison/profile-read-01-before`
  and `profile-read-01-after`; hashes appear in each environment record.
- Full repository `go test ./...` passed before and after this change (parent
  evidence logs `optimization-baseline-tests.log` and
  `optimization-read-01-tests.log` in the comparison cache). Codec/wire tests
  and profiling-harness race tests also passed.

Heap pprof uses Go's default 512 KiB statistical sampling. Allocation deltas
from `runtime.MemStats` are the precise per-operation counters above; sampled
retained heaps include long-lived setup/session objects and should not be
interpreted as measured per-operation allocation. CPU profiles report ~30.10 s
including profiler shutdown synchronization, while the measurement is exactly
30 s. No legacy comparison results or frozen snapshots were overwritten.

## Iteration 02 — buffered point-read request framing

Agent: `/root/read_iteration_02`. One additional production change: reduce
`NewTypedEnvelope` slabs from 1,024 to 256 bytes in
`internal/protocol/request.go`. Each ordinary request creates a body envelope
and a nine-byte header envelope. Strings, prepared IDs, bound values and payloads
remain borrowed spans, so these slabs hold only short scalar framing. The
builder still allocates additional slabs for larger envelopes without moving
published bytes. This preserves the prior parameter-slab change.

The allocation profile after iteration01 attributed 20.60% of sampled bytes to
`Builder.writable`. Reducing two 1,024-byte slabs to 256 predicts a 1,536-byte
saving per request; the measured counter reduction is 1,536.13 bytes/op.

|Metric|Iteration01 after / baseline|Iteration02 after|Change|
|---|---:|---:|---:|
|Successful ops/s|70,778.0|74,046.8|+4.62%|
|Allocated bytes/completed op|13,449.75|11,913.62|−11.42%|
|Allocations/completed op|79.012|79.011|Effectively unchanged|
|GC cycles|5,174|4,718|−8.81%|
|CPU µs/completed op|14.117|13.488|−4.45%|
|p99 µs|2,435|2,297|−5.67%|
|Operation errors|0|0|—|

Retained for the allocation reduction, which agrees with the source change.
The baseline reuses the preceding iteration's 30-second after observation. The
new observation performed fresh v2-only correctness, dataset reset, node flush,
schema agreement, settling and full validation/warming before measuring 30
seconds with 64 buffered workers and no timed warmup. As with iteration01,
single observations establish neither a speedup nor a latency improvement.
The cumulative reduction from the original 17,289.87 bytes/op is 31.09%.

Evidence: [after run](results/optimization-read-02-after/measurement.json),
[comparison](results/optimization-read-02-summary.json), and the after directory's
CPU/heap profiles, readable profile reports, source hashes/diff, harness snapshot,
binary identity, node/host metrics, raw histograms and affinity observations.
The retained binary is `.codex-cache/driver-comparison/profile-read-02-after`,
SHA-256 `bb32ea2fb6c57d9c799da1330cdf59e793813d8158722acbac0f5b55b596cc89`.
Protocol/wire/transport tests and typed request wire-oracle tests passed; see
`.codex-cache/driver-comparison/optimization-read-02-focused-tests.log`.
The parent also passed full repository `go test ./...` after this change;
its log is `.codex-cache/driver-comparison/optimization-read-02-tests.log`.

The after allocation profile attributes 10.22% to `Builder.writable`, 9.68% to
`ReceiveCache.Acquire`, 8.11% to admission, 7.99% to `executePage`, and 6.56% to
routing `Plan`. Syscalls remain 29.37% of flat CPU samples; allocation/GC and
context-management work remain substantial. The receive cache's four-object
session cap is an explicit ownership/resource invariant; do not simply increase
it to hide misses. Further work should inspect context/admission lifetimes or
routing plan storage before selecting another bounded optimization.

## Iteration 03 — one cancellation scope per buffered page

Agent: `/root/read_iteration_03`. Retained one production change in
`internal/execution/v2_execute.go`: replace the nested positive-page-timeout
context plus unconditional cancellation context with one call to the existing
`InternalExecutionContext` helper. A positive timeout already produces an
independently cancellable context; zero timeout still creates a page cancellation
scope. Successful completion continues cancelling speculative siblings without
cancelling the logical operation or caller. Streaming uses its separate page
context setup and does not receive this optimization directly.

The iteration02 baseline profile attributed 6.43% of sampled allocation bytes
to cancellation propagation and 24.26% cumulatively to buffered page execution.
The same 64-worker buffered read configuration and 30-second measurement with
zero warmup produced:

|Metric|Iteration02 baseline|Iteration03 after|Change|
|---|---:|---:|---:|
|Successful ops/s|74,046.8|75,266.3|+1.65%|
|Allocated bytes/completed op|11,913.62|11,449.58|−3.90% (464 bytes/op)|
|Allocations/completed op|79.011|74.011|−5 objects/op|
|GC cycles|4,718|4,610|−2.29%|
|CPU µs/completed op|13.488|13.269|−1.63%|
|p99 µs|2,297|2,249|−2.09%|
|Operation errors|0|0|—|

Retained for the exact allocation reduction. Throughput and latency remain
single-observation comparisons and do not establish a speedup. The after run
passed new v2-only correctness, reset, flush/schema agreement/settling and full
dataset validation/warming. No legacy driver ran.

New buffered deadline coverage verifies a page timeout preserves the caller's
context and drains accepted input. The speculative loser ownership regression
now runs with both zero and positive page deadlines. Focused race tests covering
buffered execution, speculative losers, session close, delivered-page lifetime,
page cancellation generations and streaming page deadlines passed; evidence is
`.codex-cache/driver-comparison/optimization-read-03-focused-tests.log`.
The parent passed full repository `go test ./...` after this change; its log is
`.codex-cache/driver-comparison/optimization-read-03-tests.log`. Execution/cluster
`go vet` and `git diff --check` also passed.

Evidence: [after observation](results/optimization-read-03-after/measurement.json),
[machine-readable comparison](results/optimization-read-03-summary.json),
[artifact audit](results/optimization-read-03-audit.json). The binary is
`.codex-cache/driver-comparison/profile-read-03-after`, SHA-256
`254a4ee96b77d47e08137613011a9efe36bc91d35c34dca35404a79ebd4c9cc5`.
The result directory includes CPU/heap profiles, flat/cumulative CPU and
allocation reports, retained heap, raw histograms, source identity snapshots,
thread-affinity observations and server metrics. `verify_profiles.py` passed
this observation's driver, duration, binary, profile, histogram and affinity
integrity checks; this audit does not establish convergence.

The after profile still attributes 20.85% cumulatively to `executePage`,
10.90% of bytes to builder slab/descriptor work, 10.13% to raw receive cache
misses, 8.19% to admission and 6.78% to routing plan allocation. A concrete next
candidate is the delayed-retry channel currently allocated for every buffered
page even when all requests succeed: a fresh agent can inspect creating that
channel only when a positive-delay retry is scheduled, preserving cancellation
and concurrent delayed-attempt delivery. The receive cache's fixed four-object
idle cap remains an invariant, not a knob to enlarge for this workload.

## Iteration 04 — allocate delayed-retry channels on demand

Agent: `/root/read_iteration_04`. Retained one production change in
`internal/execution/v2_execute.go`: keep the delayed-attempt channel nil until
the first positive-delay retry is scheduled. A nil channel disables its select
case, so successful requests avoid an unused channel. Every scheduled timer
receives the channel as a value, preserving concurrent timer delivery without
capturing a mutable channel variable. All attempts still retain/drain input.

The iteration03 allocation profile attributed about 249 MiB to this unused
channel across its buffered point-read observation. Reusing iteration03-after
as baseline, a fresh v2-only correctness/reset/flush/agreement/settling/full
warming sequence and the same 64-worker, zero-warmup 30-second run produced:

|Metric|Iteration03 baseline|Iteration04 after|Change|
|---|---:|---:|---:|
|Successful ops/s|75,266.3|78,265.4|+3.98%|
|Allocated bytes/completed op|11,449.58|11,337.53|−0.98% (112 bytes/op)|
|Allocations/completed op|74.011|73.010|−1 object/op|
|CPU µs/completed op|13.269|12.744|−3.96%|
|GC cycles/million completed ops|2,041.58|2,020.42|−1.04%|
|p99 µs|2,249|2,053|−8.71%|
|Operation errors|0|0|—|

Retained for the exact channel-sized allocation reduction. Single observations
still do not establish a throughput or latency improvement. Raw GC counts grew
from 4,610 to 4,744 with the larger number of completed operations; the table
normalizes GC to completed work. Cumulative allocation reduction from the
original baseline is 34.43%. This change applies to buffered page execution;
streaming has a separate retry loop.

New race-tested regressions cover two successive positive-delay retries,
including RetrySame then RetryNext, and cancellation while a one-hour retry
is scheduled. They verify delivery, attempt/retry accounting, prompt cancellation,
and release of bound input. Focused runtime/speculation/prepared-retry/Step07
race tests passed; the parent also passed full repository `go test ./...`.
Logs are `.codex-cache/driver-comparison/optimization-read-04-focused-tests.log`
and `optimization-read-04-tests.log`.

Evidence: [after observation](results/optimization-read-04-after/measurement.json),
[comparison](results/optimization-read-04-summary.json), and
[artifact integrity audit](results/optimization-read-04-audit.json).
The retained binary is `.codex-cache/driver-comparison/profile-read-04-after`,
SHA-256 `78e429b47d9489a3141ee43dd48640cb3156bd274ed8ac67100b9e6ee76b3e59`.
The result directory retains CPU/heap profiles and readable reports, raw
histograms, source identity/diff, harness snapshot, server metrics and affinity
checks. Artifact integrity checks passed and do not establish convergence.

The after profile still attributes 10.92% of allocation bytes to builder
slab/descriptor work, 10.15% to receive-cache misses, 8.51% to admission and
6.61% to routing plans. A fresh agent can inspect the single-component partition
key path in `InternalNewPartitionKey`: it builds/freezes descriptors around the
existing immutable cell-body chain even though only composite keys need framing.
Preserve invalid/null/unset handling, ownership and token byte order if adding
such a fast path. Do not enlarge the receive cache's fixed idle-object cap.

## Iteration05 — bounded writer descriptor reuse

Agent: `/root/read_iteration_05`. `InternalDeadlineContextWriter.InternalWriteSpans`
now reuses its span descriptor slice under the existing exclusive write semaphore.
Every exit clears all borrowed payload references before releasing the semaphore,
including short writes, errors, and panics. At most 16 descriptors (384 bytes)
are retained per writer; more fragmented request arrays are discarded. Request
payload bytes, write ordering, synchronous completion, and the receive cache's
four-object idle bound are unchanged.

The previous allocation profile attributed 3.20% of sampled bytes to the writer's
repeated descriptor-slice growth. Identical CPU8/GOMAXPROCS1, 64-worker buffered
point-read observations, each 30 seconds with no warmup, produced:

|Metric|Iteration04 baseline|Iteration05 after|Change|
|---|---:|---:|---:|
|Successful ops/s|78,265.4|79,630.8|+1.74%|
|Allocated bytes/completed op|11,337.53|10,977.53|−3.18% (360 bytes/op)|
|Allocations/completed op|73.010|69.010|−4 objects/op|
|GC cycles|4,744|4,650|−1.98%|
|GC cycles/million completed ops|2,020.4|1,946.4|−3.66%|
|CPU µs/completed op|12.744|12.540|−1.60%|
|p99 µs|2,053|2,005|−2.34%|
|Operation errors|0|0|—|

Retained for the exact 360-byte/four-object per-operation allocation reduction;
these single observations do not establish a speed or latency improvement.
The after observation performed new-driver correctness/reset/flush/schema
agreement/settling/full validation before profiling. The measured phase was
exactly 30 seconds; profiling includes bounded draining and excludes setup.

New race tests cover descriptor payload release on success/short-write/error/
panic, independent subsequent writes, bounded oversized storage, and concurrent
request-span ordering. Existing typed partial-write, cancellation/join,
writev-vector-limit, TLS/coalescing borrowed order, and blocked timeout tests
also passed under the race detector. Focused evidence is in
`.codex-cache/driver-comparison/optimization-read-05-focused-tests.log`.
Parent full repository `go test ./...` passed (log
`optimization-read-05-tests.log`), transport vet and diff checks passed.

Evidence: [after observation](results/optimization-read-05-after/measurement.json),
[machine-readable comparison](results/optimization-read-05-summary.json),
[artifact audit](results/optimization-read-05-audit.json). Binary
`.codex-cache/driver-comparison/profile-read-05-after` SHA-256 is
`95b37fbd32ac75504f52b6b5ceef884e3eefd040a90834eb90c6ef56a75ae804`.
Its result directory retains CPU/allocation/live-heap reports and raw profiles,
source identities/diff including the new test source, binary identity, process
records, server metrics, and histograms.

Point-read optimization is not yet exhausted: the new profile's remaining
largest allocation sites are builder storage/descriptors (11.27% writable,
4.65% Borrow, 3.87% Freeze), raw receive allocations (10.36%), admission (8.62%
flat), and routing plans (7.23%). Raw cache limits and borrowed lifetimes remain
constraints. A single-component `InternalNewPartitionKey` shortcut would help
custom partitioners, but this benchmark's built-in Murmur3 path bypasses that
function, so it would not explain or improve this profile. A fresh agent must
choose any next change from the actual profile. Streaming and sustainable read
phases, writes, and paged reads remain to be measured and optimized; final
repeat measurements are still required after shared changes.

## Iteration06 — retain only the pinned routing destination

Agent: `/root/read_iteration_06`. Retained one production change in
`internal/routing/v2_policy.go`: remove the complete `RoutingInfo` field from
each lazy plan. Ordinary plans had no use for it after construction. Pinned
plans now capture their exact host/shard in the existing leader-target slot,
which their separate pinned traversal never uses for replica selection.
Metadata snapshots, locality rules, target ordering, leader routing, invalidation
and plan ownership remain unchanged.

Iteration05 attributed 7.23% of sampled allocation bytes to the routing plan
object. Removing the unused state reduces its allocator size class by 128 bytes.
The same 64-worker buffered workload, fresh correctness/reset/flush/agreement/
settling/full warming, zero timed warmup, and exactly 30-second v2 measurement
produced:

|Metric|Iteration05 baseline|Iteration06 after|Change|
|---|---:|---:|---:|
|Successful ops/s|79,630.8|79,603.1|−0.035%|
|Allocated bytes/completed op|10,977.53|10,849.50|−1.17% (128 bytes/op)|
|Allocations/completed op|69.010|69.010|Effectively unchanged|
|CPU µs/completed op|12.540|12.547|+0.05%|
|GC cycles|4,650|4,612|−0.82%|
|p99 µs|2,005|2,030|+1.25%|
|Operation errors|0|0|—|

Retained for the exact allocation reduction. Single observations establish no
speed or latency improvement. Routing plan allocation is now 5.79% of sampled
bytes; syscalls remain 30.52% of flat CPU. New race-tested coverage creates
several pinned plans before consuming them in reverse, checking independent
hosts/shards, strict zero-shard selection, remote/down pins, disabled/unknown
hosts and absence of fallback. Existing routing, latency, leader/locality,
feedback-concurrency and strict/advisory picker race tests also passed.
Focused results are in
`.codex-cache/driver-comparison/optimization-read-06-focused-tests.log`.
The parent also passed full repository `go test ./...`; its log is
`.codex-cache/driver-comparison/optimization-read-06-tests.log`. Routing `go vet`,
`git diff --check`, and the profiling artifact audit passed.

Evidence: [after observation](results/optimization-read-06-after/measurement.json),
[comparison](results/optimization-read-06-summary.json), and
[artifact integrity audit](results/optimization-read-06-audit.json).
The retained binary is `.codex-cache/driver-comparison/profile-read-06-after`,
SHA-256 `0909ba485069532566d0d3025eb02e64dae9c242ca300db6990bab492ccf87f5`.
CPU/heap profiles, readable reports, precise source identities/diff, harness
snapshot, raw histograms, affinity observations and server metrics are retained.

The remaining receive allocation (10.47% sampled bytes) is constrained by the
fixed four-object session idle-cache invariant; admission/lifetime allocations
support independently cancellable executions and input draining. Those facts
do not prove optimization convergence. A concrete fresh-agent candidate is the
separate builder object held by `protocol.TypedEnvelope`: allocation stacks
through `NewTypedEnvelope` accounted for 2.51% of all bytes in iteration05.
Embedding a freshly initialized builder could permit stack allocation, but must
be checked with escape analysis and wire/lifetime tests. No such change was made
in this iteration. The [coverage state](OPTIMIZATION-STATE.json) tracks the
remaining streaming, sustainable, write and paged workload observations.

## Iteration07 — embed request-envelope builder storage

Agent: `/root/read_iteration_07`. Retained one request-framing optimization:
`TypedEnvelope` embeds its `wire.Builder` instead of allocating a separate
builder. `SetSlabSize` configures future slabs in place, so the existing
256-byte minimum is preserved without copying or resetting a used builder.
Envelope documentation now explicitly forbids copying after first use.
Published spans, borrowed payloads, mark identity, and Freeze ownership stay
unchanged.

An earlier point-read allocation profile attributed 2.51% of sampled bytes to builder
objects allocated through request-envelope construction. Compiler escape
analysis confirms that before this change each request's body/header builders
escaped, while the envelope wrappers stayed on the stack. After embedding,
the complete body/header envelopes stay on the stack. Fresh v2-only correctness,
reset/flush/agreement/settling/full warming and the same 64-worker, zero-warmup,
30-second measured run produced:

|Metric|Iteration06 baseline|Iteration07 after|Change|
|---|---:|---:|---:|
|Successful ops/s|79,603.1|80,327.2|+0.91%|
|Allocated bytes/completed op|10,849.50|10,593.48|−2.36% (256 bytes/op)|
|Allocations/completed op|69.010|67.010|−2 objects/op|
|CPU µs/completed op|12.547|12.432|−0.92%|
|GC cycles|4,612|4,500|−2.43%|
|p99 µs|2,030|2,013|−0.84%|
|Operation errors|0|0|—|

Retained for the exact allocation reduction, which agrees with eliminating two
128-byte builder objects. The cumulative bytes/op reduction from the original
baseline is 38.73%. Throughput and latency remain single-observation comparisons;
no speedup or convergence is established. Focused race tests passed for builder
configuration/mark/storage stability, protocol encoding, typed wire oracles,
borrowed payloads, runtime cancellation and Step07/Step15 lifetime behavior.
The new builder regression verifies that changing future slab sizes preserves
existing marks and frozen storage across reuse, including default sizes and a
scalar larger than its configured minimum. Protocol/wire vet, diff checks and
the profile integrity audit also passed.
The parent passed full repository `go test ./...` after the change; its log is
`.codex-cache/driver-comparison/optimization-read-07-tests.log`.

Evidence: [after observation](results/optimization-read-07-after/measurement.json),
[comparison](results/optimization-read-07-summary.json), and
[artifact integrity audit](results/optimization-read-07-audit.json).
The retained binary is `.codex-cache/driver-comparison/profile-read-07-after`,
SHA-256 `c61a25b07917779b2de54ab8b39118b4c4d163132f845c61500dbfb9bda27898`.
The result directory retains CPU/heap profiles, readable reports, source identity
and diff, harness snapshot, histograms, affinity observations and server metrics.
Focused race and before/after escape-analysis logs are in the comparison cache
with prefix `optimization-read-07-`. The no-copy comment was added after the
measured source snapshot; it has no executable effect.

The after profile still attributes 11.97% of bytes to builder slab/descriptor
growth, 10.95% to receive allocation, 8.86% to admission, 7.91% directly to page
execution, 6.11% to routing, and 4.12% to Freeze descriptors. Separate NewBuilder
objects fell to 1.14%, now through parameter encoding rather than request
envelopes. Further builder descriptor/parameter storage changes require fresh
escape analysis and lifetime checks; no further change belongs to this iteration.
Streaming, sustainable, write and paged observations remain tracked by the
[coverage state](OPTIMIZATION-STATE.json).

## Iteration 08 — streaming point reads and scheduled arrivals

Agent: `/root/read_iteration_08`. One production optimization: validate scalar
kinds without constructing and discarding a `DataTypeDescriptor`.
`ScalarType`, dynamic descriptor-height checking, and streaming validation now
share `cqltype.ValidateScalarKind`; supported kinds and `kind` validation errors
are unchanged. Streaming compiled plans and all ownership/resource limits remain
unchanged. Before choosing this change, this fresh agent profiled streaming
maximum load and both read adapters under scheduled arrivals.

The streaming allocation profile attributed 4.53% of sampled bytes to
`ScalarType`: 51.39% through `compileStreamValidationMemo` and 48.61% through
`checkedTypeHeight`. Both callers discarded the constructed descriptor and used
only its validation error. For the three scalar result columns, eliminating
these checks' allocations predicts six descriptors / 576 bytes per operation.

|Streaming maximum, four workers|Before|After|Change|
|---|---:|---:|---:|
|Successful ops/s|51,035.83|51,744.10|+1.39%|
|Allocated bytes/completed op|12,383.54|11,807.42|−4.65%|
|Allocations/completed op|130.519|124.517|−6.002|
|CPU µs/completed op|19.089|18.943|−0.77%|
|GC cycles per million completed ops|2,121.38|2,031.14|−4.25%|
|p99 µs|287|276|−3.83%|
|Operation errors|0|0|—|

Retained for the exact allocation reduction, also reproduced at the fixed
scheduled-arrival rate below. These single observations establish neither a
speedup nor a latency improvement. The streaming four-worker result cannot be
compared directly with buffered 64-worker latency as an adapter latency claim.

Every measured phase used v2 only, CPU8, GOMAXPROCS=1, exactly 30 seconds and
zero timed warmup. Each observation freshly checked correctness, reset the point
dataset, flushed/settled the nodes, checked schema agreement and validated/warmed
every key outside the measured phase. CPU/heap/allocation profiles, source and
binary identities, raw histograms, external affinity and server metrics are
retained for all observations.

Scheduled rates were fixed at 70% of individual exploratory maximum observations:
buffered `80,327.2 × 0.7 = 56,229.04` ops/s from iteration07, and streaming
`51,035.833333333336 × 0.7 = 35,725.083333333336` ops/s from this iteration's
before run. They are exploratory single-observation targets; final capacity
repeats, adapter/concurrency selection and sustainable acceptance remain pending.
The after streaming observation kept the before target unchanged.

|Scheduled observation|Buffered before|Streaming before|Streaming after|
|---|---:|---:|---:|
|Target ops/s|56,229.04|35,725.0833|35,725.0833|
|Successful ops/s|56,092.90|35,725.00|35,725.03|
|Allocated bytes/completed op|10,623.28|12,368.24|11,791.90|
|Rejected arrivals|4,081|0|0|
|Schedule misses over 1 ms|3,705|4,673|7,356|
|Peak / final queue depth|1,024 / 0|937 / 0|734 / 0|
|Operation errors|0|0|0|

Buffered scheduled delivery **failed** because arrivals were rejected, despite
99.76% of target throughput. Streaming delivered the fixed target with no
rejections in these observations, but timing misses remain explicit and this
does not substitute for repeated acceptance/backlog assessment. No queue was
enlarged and no rate was lowered to make delivery pass.

An earlier manually transcribed buffered target was 0.0467 ops/s below the
exact derivation. That observation is preserved in
`optimization-read-08-buffered-sustainable-mistarget/` with `EXCLUDED.json`;
the correct target was rerun separately. Its artifact invocation paths retain
the original directory name. It is excluded from the table and comparisons.

Evidence: [comparison and phase observations](results/optimization-read-08-summary.json),
[exact target derivation](results/optimization-read-08-targets.json),
[artifact integrity audit](results/optimization-read-08-audit.json), and result
directories with prefix `optimization-read-08-`. Retained binaries are
`.codex-cache/driver-comparison/profile-read-08-before` and `profile-read-08-after`.
Focused root streaming/lifecycle race tests and complete codec/cqltype race
suites passed, including unsupported-kind/error-field preservation and
allocation-free validation checks. Codec/cqltype vet and diff checks passed.
The parent passed full repository `go test ./...` after this change; its log is
`.codex-cache/driver-comparison/optimization-read-08-tests.log`.

## Iteration 09 — point write, rejected native-VOID candidate

Agent: `/root/write_iteration_09`. **The candidate was rejected and its
production changes were reverted.** The execution source matches its exact
pre-09 SHA-256, `d2e0446174515bb00d04453d7a85812a2db51bf40e311d9580b89579df6e27c7`;
earlier page-context/retry-channel changes remain intact. The previous eight
optimizations remain the driver implementation for subsequent work.

The write allocation profile exposed general Rows/Page/plan-cache machinery
being created for native VOID acknowledgements. The candidate completed only
validated, empty native VOID replies directly and preserved the original Rows
fallback for all other kinds. It saved about **832 B and seven allocations per
operation**, but repeated throughput concerns could not be ruled out.

|Comparison|Pre-change v2 ops/s|Candidate ops/s|Throughput change|CPU/completed op change|p99 change|
|---|---:|---:|---:|---:|---:|
|Initial 64-worker observations|75,004.3|57,900.0|−22.80%|−8.18%|+43.67%|
|Post-restart 16-worker diagnostics|50,125.9|44,756.2|−10.71%|+5.13%|+25.19%|

The post-restart pair allocated 10,442.22 → 9,610.29 B/completed operation,
71.013 → 64.014 objects, and consumed 13.318 → 14.001 CPU µs/completed operation.
Its p99 changed from 1.568 to 1.963 ms. Server/compaction conditions differed,
so these data do not prove a causal regression; they also do not justify
retaining an 8% memory saving with unresolved capacity/CPU concerns. The rejected
patch, source copies, binary identities and evidence remain available in
[the candidate archive](results/optimization-write-09-candidate-rejected/REJECTED.json).

All completed observations used only v2, CPU8/GOMAXPROCS=1 and exactly 30-second
measurement windows. Setup, complete data validation/warming and heap snapshot
collections stayed outside timing. Only the first maximum reused a separately
retained reset/agreement/settling setup in `optimization-write-09-setup`.
All other observations reset afresh. No legacy-driver binary was executed.

Capacity/delivery limits remain explicit:

- The 16-worker pre-change capacity probe was 47,456.8 ops/s, below its initial
  64-worker 75,004.3. The candidate at 256 workers achieved 49,459.9, below
  its 57,900.0 at 64 workers;
  these do not establish rising capacity at the upper probe, but none is an
  accepted stable maximum set.
- The initial sustainable target was exactly 52,503.01/s (70% of 75,004.3).
  It delivered 52,407.5/s with zero operation errors but 2,801 rejections and a
  full 1,024-entry queue. It fails sustainable acceptance and was never lowered
  or replaced by the later health probes.
- A pre-change v2 replay with 64 workers failed with 64 server write-timeout
  errors (only one of two LOCAL_QUORUM responses); a subsequent pre-change
  diagnostic with 16 workers failed with 4,867 server hint-backpressure errors. Both retain
  `INVALID.json`, the original failed JSON, compacted histograms, full profiles,
  server metrics and diagnostic node logs.
- Separate 20,000/s scheduled health probes had zero operation errors. Pre-change
  v2 lost 12 producer-boundary arrivals; the complete candidate retry rejected 173
  and filled the queue. Neither is accepted sustainable delivery. One candidate
  health run was started after a failed sequencing assertion, terminated and
  joined; its `INTERRUPTED.json` and partial artifacts are preserved and excluded
  from completed observations.
- The parent performed a controlled benchmark-only restart, preserving data and
  configuration. [Recovery evidence](results/optimization-cluster-recovery-02)
  records new addresses 10.89.18.23/.24/.25. Post-restart 16-worker diagnostics
  remain separate from all pre-restart observations. Both had zero operation
  errors but renewed commitlog-related long reactor queues, so neither is a
  healthy/stable-capacity acceptance result. Storage attribution is provisional.

[The summary](results/optimization-write-09-summary.json) accounts for 11 completed
observations (including two invalid operation-error observations) and one
interrupted attempt. [The artifact audit](results/optimization-write-09-audit.json)
checks their integrity without treating invalid/unhealthy results as acceptance.
Every completed observation retains CPU flat/cumulative reports, allocation
byte/object deltas, retained-heap reports, binary/source identities and an
`io-compaction-summary.json`. Sparse five-second metric gauges are not proof of
no intervening compaction work. Disk space was about 59 GiB after the later
pre-restart failures; current availability is recorded by each environment/run.

Behavioral regression tests remain in `v2_exec_void_test.go` and pass after
reverting the candidate. They cover trace/warning/custom-payload ownership,
observer completion, malformed VOID/ROWS, paged rejection/drain, and cancellation
or session close before/after transport acknowledgement. Broader focused race
tests and parent full repository tests passed on the restored source (cache
`profile-write-09-reverted-focused-tests.log` and
`optimization-write-09-reverted-tests.log`).

A fresh independent audit found no verified correctness/ownership regression.
It noted a test limitation: the acknowledgement cancellation fixture blocks in
ObserveAttempt before executePage accepts the outcome. It therefore does not
deterministically exercise the candidate's final post-outcome cancellation or
shutdown checks. No intrusive test seam was added; the limitation remains in the
archive even though the candidate is rejected.

The write workload is not exhausted or accepted at sustainable load. Future
fresh-agent work can revisit actual framing/descriptor/admission hotspots once
server conditions permit meaningful comparison. Paged workloads proceed from
the restored driver; do not use the rejected candidate binary for new baselines.

## Iteration 10 — paged reads and shared native-header framing

Agent: `/root/paged_iteration_10`. Retained one production optimization in
`internal/protocol/request.go`: encode the fixed native header in the same builder
as its uncompressed body, eagerly fill known fields, then use the existing checked
`Mark`/`PatchUint32` operation for the four-byte body length before `Freeze`. This
shares the scalar slab and final immutable span index. It respects the builder
reservation/no-copy contract; the native v3/v4 compressed path still compresses
only the body, and v5 segments contain the native header exactly once. Body limits
exclude the nine header bytes and sticky encoding errors keep their precedence.

All observations use v2 only, CPU8/GOMAXPROCS=1, exactly 30 seconds measured,
zero warmup, and full correctness/reset/flush/agreement/settle/partition warming
outside timing. One operation consumes and hashes all 128 × 4 KiB rows, with
page size 32 and prefetch zero. Rows/s = partitions/s ×128; blob MiB/s =
partitions/s ×0.5. Old comparison artifacts and binaries remain untouched.

### Concurrency selection, separate from source effects

|Adapter|Workers|Partitions/s|Allocated B/partition|Allocations/partition|
|---|---:|---:|---:|---:|
|buffered|1|1,065.63|36,107.42|213.34|
|buffered|4|2,682.87|36,068.57|213.11|
|buffered|16|2,444.53|235,576.03|214.51|
|buffered|64|2,477.80|485,195.63|216.43|
|stream|1|909.23|38,461.50|313.93|
|stream|4|1,807.30|38,459.14|314.76|
|stream|16|1,788.20|38,618.37|324.12|

Both adapters select **4 workers** using the smallest concurrency within 3% of
the best valid observed rate. Buffered 16/64 initially allocated 235,576/485,196
B/partition because more pages remained outstanding under the unchanged four-object
receive-cache cap. Buffered 4 allocates 36,069 B/partition and achieves higher
throughput; this operating-point improvement is separate from the source change.
Both one-worker observations are substantially slower. The frozen exploratory
scheduled targets are **1,878.0066666666667/s buffered** and **1,265.11/s streaming**,
computed programmatically as 70% of each selected actual observation.
See [selection and derivation](results/optimization-paged-10-selection.json).

### Source comparison at fixed concurrency and target

|Adapter / load|Partitions/s before → after|B/partition before → after|Allocations before → after|
|---|---:|---:|---:|
|buffered / maximum|2,682.87 → 2,684.77|36,068.57 → 28,963.99|213.11 → 179.09|
|buffered / sustainable|1,877.97 → 1,877.97|36,086.39 → 28,977.46|213.23 → 179.20|
|stream / maximum|1,807.30 → 1,799.60|38,459.14 → 31,354.45|314.76 → 280.74|
|stream / sustainable|1,265.07 → 1,265.10|38,442.62 → 31,338.20|313.92 → 279.90|

Retained for approximately **7,105 fewer bytes and 34 fewer allocations per
partition**, reproduced in both maximum and same-rate scheduled observations.
Maximum allocated bytes fall 19.70% buffered and 18.47% streaming. Single-run
throughput changes are +0.07% and −0.43%; no speed or latency gain is established.
The after maximum rates correspond to 343,650 rows/s and 1,342.38 blob MiB/s
buffered, and 230,349 rows/s and 899.80 blob MiB/s streaming.

All four source-comparison after observations have zero operation errors, zero
rejected arrivals, and empty end queues. Scheduled misses over 1 ms remain
visible: buffered 664 →526 and streaming 309 →209; peak queues are retained in
JSON. These single observations do not establish repeated stable capacity or
complete the overall optimization goal.

The initial buffered-16 setup emitted one database warning per node that data
written after the truncation timestamp had been incorrectly truncated. Full
reload and complete partition validation/warming subsequently passed. Every later
observation has no warning/error line in its bounded setup/measurement server
window; no hint overload or failure-detector deterioration occurred. The retained
node-status check shows all three restarted addresses UN.

Focused race tests cover independent v3/v4/v5 query/prepare/execute/batch wire
oracles, compression decode, borrowed payload ownership, body-limit boundaries,
sticky errors, writer/cancellation paths and runtime lifetimes. The exact-source
full repository `GOWORK=off taskset -c 2,18 go test ./...` passed. Escape analysis
confirms both envelope constructions remain stack-local; `git diff --check` passed.
Full commands and logs are recorded in the summary.

Evidence: [summary](results/optimization-paged-10-summary.json),
[artifact audit](results/optimization-paged-10-audit.json), and
[isolated source/test patch](results/optimization-paged-10-source.patch).
The before/after production source snapshots are verified against their profile
source hashes. Thirteen observations retain binary/source identities, CPU pprof,
before/after heap pprof, CPU/allocation/retained-heap text reports, histograms,
thread affinity, server metrics, and bounded per-node logs.

## Iteration 13 — bounded prepared-request descriptor reservation

Agent: `/root/remaining_iteration_13`. Retained one optimization: reserve a
bounded initial descriptor capacity in native prepared-request envelopes.
A prepared ID and nonempty bound values require at least four parts. Fragmented
values or following paging fields require more than four, so those requests
reserve eight parts. Other requests reserve four. Generic envelopes, unprepared
queries, PREPARE and compressed native framing retain their existing behavior.
`wire.Builder.ReserveParts` only reserves descriptor storage; it preserves scalar
slabs, marks, borrowed payloads, immutable Freeze indices and normal growth for
larger requests. No per-builder fields or retained caches were added.

Current iteration10-after profiles attributed 14.45% of buffered paged allocation
bytes and 12.95% of streaming paged bytes to the scalar-write descriptor append.
The request shape determines the existing power-of-two growth threshold; the
hint avoids the discarded smaller backing arrays. An untimed 10,000-build
allocation probe established the following exact framing changes:

|Request shape|Bytes before → after|Objects before → after|
|---|---:|---:|
|Prepared scalar read without paging fields|576 → 480|5 → 3|
|Prepared read with page size|864 → 640|6 → 3|
|Prepared read with page size and state|896 → 672|6 → 3|
|Prepared write with fragmented values|864 → 640|6 → 3|
|Small unprepared query|544 → 544|5 → 5|
|Small PREPARE|384 → 384|4 → 4|
|Compressed prepared read with page size|1,376 → 1,376|15 → 15|

All measured client observations used only v2, CPU8/GOMAXPROCS1, exactly 30
seconds and zero timed warmup. Four new point-read baselines used the retained
iteration10-after binary and validated source provenance. Their exploratory
scheduled targets were frozen at 70% of their own observed maxima:
56,794.75666666667/s buffered and 32,233.27333333333/s streaming. These targets
remain separate from historical sustainable failures. Four point-read and four
paged after observations retain complete CPU/heap, source/binary, raw histogram,
affinity and server evidence. Paged observations keep the selected four workers
and frozen targets 1,878.0066666666667/s and 1,265.11/s.

The first buffered maximum changed 81,135.37 → 77,118.43/s, so retention required
controlled replay. A full point prepare/flush/settle was followed by balanced
old/current/current/old observations using `--reuse-prepared`; every observation
still validated and warmed all keys. No writes or reload occurred between the
four observations. Full repository tests ran with no client between the first
and second replay; the outer batch was paused to prevent overlap.

|Controlled buffered read, 64 workers|Old v2 mean|Current v2 mean|
|---|---:|---:|
|Successful ops/s|78,331.87|79,547.40|
|Allocated bytes/completed op|9,633.47|9,409.46|
|Allocations/completed op|61.010|58.010|
|Client CPU µs/completed op|12.735|12.546|
|p99 µs, mean of observations|2,267|2,172|
|Server CPU cores, sampled mean|2.126|2.159|

The original 4.95% throughput decline did not recur in these controlled means.
Retained for the reproducible **224-byte/three-object point-read reduction**;
the small observation set establishes no speedup. The single streaming maximum
changed 46,047.53 → 45,506.00/s, with 10,847.25 → 10,623.54 B/op. Paged buffered
maximum changed 2,684.77 → 2,765.07/s and 28,963.99 → 27,841.21 B/partition;
streaming changed 1,799.60 → 1,893.43/s and 31,354.45 → 30,232.86 B/partition.
These approximately 1,120-byte/15-object partition savings match five requests.
Single paged observations establish no throughput or latency improvement.

Delivery limitations remain explicit. Buffered scheduled read rejected 1,626
arrivals before and 200 after, reaching its 1,024-entry queue in both runs.
Streaming rejected zero before and **one after**, so the after run also fails
strict sustainable delivery. All four paged scheduled observations in the
before/after comparison had zero errors/rejections at their frozen targets.

One permitted current-source 16-worker write diagnostic completed with zero
operation errors at 16,214.23/s, but emitted failure-detector RPC timeouts and
long reactor queues referencing commitlog. Its `UNHEALTHY.json` and server logs
mark it as diagnostic only. Further write load stopped: no 20,000/s scheduled
diagnostic or write after comparison ran. This observation does not establish
write capacity and does not replace the historical 52,503.01/s target. The
untimed write shape probe verifies framing allocation savings; it supplies no
write throughput/CPU acceptance result.

The artifact audit passed all **17 completed observations**, including the
explicitly unhealthy write diagnostic. Sixteen read/paged setup/measurement log
windows had no warnings/errors. One earlier attempt used a nonexistent identity
path and exited before taking the lock or running a client; its empty directory
has `NOT_STARTED.json`, and the successful baseline uses suffix `before1`.

New race-tested builder coverage preserves marks and scalar addresses through
reservation, then checks prior frozen descriptors and borrowed storage through
reuse/growth. Complete wire/protocol/transport race suites and focused root
streaming/exclusive/typed/lifetime race tests passed. Exact-source full repository
`GOWORK=off taskset -c 2,18 go test ./...`, wire/protocol vet and diff checks passed.
Compiler reports both BuildTypedRequest envelopes still do not escape.
Evidence logs have prefix `optimization-remaining-13-` in the comparison cache.

Evidence: [summary](results/optimization-remaining-13-summary.json),
[artifact audit](results/optimization-remaining-13-audit.json),
[isolated verified patch](results/optimization-remaining-13.patch),
[target derivation](results/optimization-remaining-13-targets.json), and all result
directories with prefix `optimization-remaining-13-`. Before binary is retained
pre-change v2 `profile-paged-10-after`; current binary is
`profile-remaining-13-after`, SHA-256
`26b4eb90182ea3d43801a47ddcdfba4540aaf2c00f9d474b7297e1181cf70cc5`.

Convergence remains unverified. Current paged streaming profiles attribute
12.56% cumulative allocation bytes to NewStreamRowReader and 7.77% to
compileStreamValidationMemo. Point streaming attributes 4.35% to fmt.Errorf
through idle-header timeout wrapping. Larger metadata/tablet/page-state request
shapes still have descriptor growth beyond the conservative eight-part hint;
any larger reservation needs a fresh exact-shape audit, including small and
compressed cases. These are fresh-agent candidates, not further changes in
this iteration. Receive-cache bounds remain four objects, 16 MiB total and
9 MiB per object.

## Iteration 14 — initial validation sharing left unaccepted

Agent: `/root/stream_iteration_14`. The initial version shared immutable
validation plans across pages, but also routed the first-page constructor
through a new helper. Its point scheduled controls rejected **1,934 and 8**
arrivals while the corresponding baseline controls rejected zero. The version
remained unaccepted, despite repeated paged allocation savings and passing
correctness tests. Its seven candidate observations remain explicitly excluded;
no result was erased. See [v1 summary](results/optimization-stream-14-v1-summary.json),
[v1 patch](results/optimization-stream-14-v1.patch), and
[17-observation integrity audit](results/optimization-stream-14-audit.json).

A scoped revision restored the original first-page constructor and transport
installer bodies verbatim, then invoked validation sharing only for continuation
pages. Agent14 completed three point scheduled controls for that revision and
transferred it to a fresh agent before any retention decision. Its batch and
last client were fully joined before iteration16 execution began. See the
[handoff](results/optimization-stream-14-handoff.json).

## Iteration 16 — continuation validation plans with fresh page state

Agent: `/root/validation_iteration_16`. Independently reviewed and retained the
scoped revision. `NewStreamRowReaderReusingValidation` shares only the immutable
validation-plan slice when the new and preceding columns have the exact same
backing storage and length. Equal replacement metadata recompiles validation,
so a new page cannot retain replaced metadata after its budget owner releases
it. Every page still receives fresh mutable decode state and its own scope,
context and limits. The original `NewStreamRowReader` and transport
`InternalInstallColumns` bodies match the retained iteration13 source exactly;
only an existing response selects the continuation helper. There is no global
cache, cross-page scratch reuse, custom decoder compilation change or resource
limit increase.

All observations used only v2, CPU8/GOMAXPROCS1, four streaming workers, exactly
30 seconds and zero timed warmup. Both retained datasets were fully validated
and warmed before each observation, using `--reuse-prepared`; no writes or
reloads occurred between the comparisons. The fresh paged maximum and fixed
1,265.11/s observations each used baseline/current/current/baseline order.
Values below are arithmetic means of the two observations for each source.

|Paged streaming metric|Iteration13 baseline|Scoped revision|
|---|---:|---:|
|Maximum partitions/s|1,864.48|1,857.50|
|Maximum allocated B/partition|30,233.12|28,344.64|
|Maximum allocations/partition|265.747|233.729|
|Maximum client CPU µs/partition|521.63|526.03|
|Fixed-rate partitions/s|1,265.05|1,265.08|
|Fixed-rate allocated B/partition|30,220.90|28,331.84|
|Fixed-rate allocations/partition|264.968|232.946|
|Fixed-rate client CPU µs/partition|561.40|561.25|

Retained for the repeated **approximately 1,888 fewer bytes and 32 fewer objects
per partition**: about 6.25% fewer bytes and 12.05% fewer objects at maximum load.
Maximum throughput changed −0.37% and CPU +0.84%; scheduled CPU was effectively
unchanged. These observations establish no speedup or latency improvement.
The sampled baseline attributes 12.06% cumulative allocation bytes to the row
reader constructor and 7.87% to validation compilation. The current first
paged maximum reports 2.38% and 1.36%, plus 3.43% for the fresh continuation
reader/state constructor. Precise MemStats counters, rather than statistical
pprof percentages, establish the allocation reduction.

Point controls verify the scoped revision separately from the unaccepted v1.
The balanced scheduled order was current14/baseline14/baseline14/current16 at
the frozen 32,233.27333333333/s target. Full/race tests ran without a client
during the handoff gap; no dataset writes occurred. Both current runs rejected
zero arrivals; the baseline runs rejected zero and **two**. The fresh point
maximum pair changed 46,180.93 →46,197.07/s, 10,623.485 →10,623.466 B/op and
20.188 →20.263 client CPU µs/op. Point allocation is effectively unchanged.
The paged scheduled baseline rejected **one** arrival in its first observation
and zero in its second; both current observations rejected zero. All maximum
observations rejected zero. These delivery failures remain in the artifacts,
and repeated sustainable capacity remains unproven.

An independent source audit verified every current driver source hash against
the scoped binary provenance, the exact three production-file/two-test delta
against iteration13, unchanged original constructor bodies, and before/after
source snapshots. Full repository `go test ./...` and focused codec/root race
tests passed again in iteration16. They cover immutable ownership identity,
equal replacement metadata, concurrent independent readers, malformed nested
values, fresh contexts and sticky errors, stricter page byte/depth/count limits,
old-response release and metadata-budget accounting, and custom Compile/Reset/
DecodeInto callback counts. The exact-source full-repository vet from14 and
iteration16 diff checks also passed.

The integrity audit passed all **14 scoped-source observations**: eleven fresh
iteration16 observations and the three retained point scheduled controls from14.
All have zero operation errors/timeouts and zero WARN/ERROR lines in their
bounded server windows. Sparse selected metrics only vary in the existing
commitlog flush-limit counter, in both baseline and current observations;
5-second sampling does not exclude intervening backend work. The existing
write-load restriction remains in force. Every observation retains CPU and
before/after heap profiles, readable CPU/allocation/retained-heap reports,
source/binary identities, histograms, server metrics and affinity records.

Evidence: [summary](results/optimization-validation-16-summary.json),
[integrity audit](results/optimization-validation-16-audit.json),
[isolated patch](results/optimization-validation-16.patch),
[source manifest](results/optimization-validation-16-source/manifest.json), and
full/race logs with prefix `optimization-validation-16-`. The retained current
binary is `.codex-cache/driver-comparison/profile-stream-14-v2-after`, SHA-256
`529839229335a3ccec31edbb00ee6d61840a2feadbca4e1cbf32be2d0a9f8b8b`.
The baseline remains `profile-remaining-13-after`, SHA-256
`26b4eb90182ea3d43801a47ddcdfba4540aaf2c00f9d474b7297e1181cf70cc5`.

Convergence remains unverified. The current second paged maximum still
attributes 11.36% flat allocation bytes to builder scalar storage/descriptors,
11.33% to routing plans, and 3.94% to the continuation constructor's fresh
reader/state. A fresh agent can inspect colocating that reader and its fresh
`streamDecodeState`, preserving independent mutable lifetime, nested-reader
ownership, sticky errors, page limits and storage accounting. Point maximum
still attributes 4.29% cumulative allocation bytes to idle-header error
formatting, the separate pending iteration15 candidate. SHA validation,
receive-cache caps (four objects, 16 MiB total, 9 MiB individual), metadata
budgets and custom decoder semantics remain constraints.

## Iteration 15 — serve-only idle-header timeout formatting (rejected)

Agent: `/root/header_iteration_15`. One candidate separated the shared serve
loop's discarded zero-byte idle-header timeout signal from the eager formatted
error returned by direct/startup reads. Only `InternalServe` passed the private
serving mode. `ReadHeader` and direct Conn entry points retained the existing
`fmt.Errorf("%w: %w", sentinel, cause)` behavior, including custom Formatter
execution, exact stored text, `errors.Is`/`errors.As`, and ordered multi-cause
unwrapping. Partial headers, body reads, continuations, deadline disarming and
exclusive ownership were unchanged. V5 retained its inner segment-header error
construction and skipped only the outer discarded normalization.

**Rejected and reverted.** Streaming allocation savings were reproducible, but
repeated buffered and paged maximum comparisons left a throughput/CPU concern.
The complete iteration16 source was restored and verified against every hash in
its accepted binary's provenance. The rejected binary must not become a future
baseline. No source optimization from this iteration remains in the worktree.

All 28 observations used only v2, CPU8/GOMAXPROCS1, exactly 30 seconds measured,
zero warmup and complete validation/warming of retained immutable datasets.
`--reuse-prepared` prevented writes/reloads between observations. Streaming used
four workers; the buffered point control used 64. Initial phases ran
old/new/new/old, with extra paged maximum and buffered controls in the reverse
new/old/old/new order. Nothing was discarded from these means.

|Mean metric / workload|Before|Rejected candidate|Change|
|---|---:|---:|---:|
|Point stream maximum ops/s (2/version)|49,177.75|51,379.15|+4.48%|
|Point stream bytes/op|10,623.28|10,171.32|−451.96 B|
|Point stream allocations/op|115.512|100.516|−14.997|
|Point stream CPU µs/op|19.507|18.686|−4.21%|
|Paged stream maximum partitions/s (4/version)|1,902.46|1,867.51|−1.84%|
|Paged stream bytes/partition|28,343.96|27,893.03|−450.93 B|
|Paged stream allocations/partition|233.732|218.745|−14.987|
|Paged stream CPU µs/partition|514.540|523.980|+1.83%|
|Buffered control ops/s (4/version)|80,963.17|78,693.53|−2.80%|
|Buffered control CPU µs/op|12.328|12.674|+2.81%|
|Buffered control mean p99 µs|2,053.25|2,282.00|+11.14%|

Buffered allocations remained effectively identical at 9,409.44/9,409.47 B and
58.009 objects/op. Point stream showed an observed CPU/rate improvement, but
these small samples do not establish a general speedup. Both paged maximum
batches had modestly lower candidate means. The additional reverse-order
buffered batch also had lower candidate throughput/higher CPU. The concern
therefore remained unresolved after the planned repeat; memory savings alone
were not used to accept this implementation.

Scheduled point reads retained the fixed 32,233.273333333328/s target. Each
version rejected one arrival across its two runs (old 1/0, candidate 0/1), so
strict delivery remains unresolved. Scheduled paged reads retained 1,265.11/s;
all four runs delivered it without rejection. Mean paged scheduled CPU changed
555.285 →553.552 µs/partition (−0.31%). Both scheduled scenarios reproduced
approximately 452–455 fewer bytes and 15 fewer objects per logical operation.
An idle shared-reader interruption happens once per streamed logical partition,
not once per page request.

All 28 runs had zero operation errors, zero server WARN/ERROR lines, empty end
queues, validated binary/source identities, CPU/heap profiles, readable reports,
raw histograms, affinity/process evidence and server metrics. The artifact audit
passed all 28. No write clients ran; the existing unhealthy-write restriction
and receive-cache caps remain unchanged.

Candidate default/unit full suites, complete protocol/transport race suites,
focused unit-tag header/exclusive/TLS/Step07/cancellation lifecycle race suites,
protocol/transport vet and diff checks passed. Parent independently reviewed the
serving-mode call graph and found no verified correctness issue. New tests pinned
eager Formatter behavior and ordered unwrapping, synthetic custom readers,
native/v5 compressed/raw idle-versus-partial classification, deadline restoration,
retry on idle, and fatal shutdown on partial input. Candidate tests and source
are archived because this implementation was reverted. The initial test fixture
incorrectly demanded a raw EOF from v5; changing that assertion to `errors.Is`
retained the existing segment wrapper and required no production fix.

After restoration, full repository tests and the focused unit-tag lifecycle race
suite passed again. All accepted iteration16 source hashes match, and a final
process check found no remaining benchmark client. The summary retains those
restoration checks and their logs.

Evidence: [summary](results/optimization-header-15-summary.json),
[28-observation audit](results/optimization-header-15-audit.json),
[isolated candidate patch](results/optimization-header-15.patch),
[source manifest](results/optimization-header-15-source/manifest.json),
[initial batch](results/optimization-header-15-batch.log), and
[reverse repeats](results/optimization-header-15-repeat.log). Source snapshots,
test logs, and full profile sets use the `optimization-header-15-` prefix.
Accepted baseline binary remains `profile-stream-14-v2-after`, SHA-256
`529839229335a3ccec31edbb00ee6d61840a2feadbca4e1cbf32be2d0a9f8b8b`.
Rejected binary `profile-header-15-after` is retained only for evidence, SHA-256
`b2c7753c19c3530ce2df83e42cf6c32173e8857ab7f758f4f3caae975eade498`.

Idle-timeout formatting remains measurable in the accepted source. A future
fresh agent may audit a narrower implementation that preserves common successful
receive paths, but must not repeat this serving-mode refactor as an accepted
optimization or assume timing neutrality from the allocation reduction.
Convergence remains unverified.

## Iteration 17 — lazy input-owner wait channels (rejected)

Agent: `/root/hotspot_iteration_17`. The candidate created an input owner's
notification channel only when Wait encountered a closed owner with accepted
references still pending. Drained Wait returned after the drain callback under
the same mutex; concurrent pending waiters shared one channel. Writer references,
callback ordering and `InternalExecutionLifetime` shutdown signaling were unchanged.

**Rejected and reverted.** Two fewer eager channels repeatedly saved about
**224 bytes and two allocations per operation/partition**. Balanced fixed-rate
buffered point reads nevertheless used 2.15% more CPU and rejected more arrivals
in both candidate/baseline pairings. No correctness defect was found, but the
allocation savings did not resolve that performance concern.

All 28 observations used only v2, exactly 30 seconds, zero timed warmup,
CPU8/GOMAXPROCS1, full validation/warming and `--reuse-prepared`. No writes or
reloads occurred. Maximum phases used old/new/new/old for buffered adapters and
new/old/old/new for streaming. Fixed-rate pairs alternated source order; observed
point-read delivery failures justified reverse repeats for those two modes only.

|Maximum workload|Baseline rate/s|Candidate rate/s|CPU change|Bytes saved/op|Objects saved/op|
|---|---:|---:|---:|---:|---:|
|Point buffered, 64 workers|79,742.85|80,359.72|−0.84%|224.02|2.000|
|Point streaming, 4 workers|49,079.05|48,578.12|+0.67%|224.00|2.001|
|Paged buffered, 4 workers|2,742.15|2,726.68|+0.35%|224.78|2.005|
|Paged streaming, 4 workers|1,890.88|1,899.18|−0.13%|224.68|2.020|

Values are means of two observations per source. Paged rates are partitions/s
and allocations are per partition. Buffered point baseline endpoints drifted
82,557.93 →76,927.77/s; these small samples do not establish a speedup.

Fixed-rate buffered point reads kept 56,794.75666666667/s. Baseline rejected
**545 / 477** arrivals; candidate rejected **2,685 / 804**. Mean CPU changed
15.02167 →15.34428 µs/op (+2.15%). The reverse pair narrowed the initial CPU
difference from 3.73% to 0.58%, but both candidate runs had higher CPU and
rejection counts. Streaming point reads kept 32,233.27333333333/s: baseline
rejected 0 / 0, candidate 1 / 0, with mean CPU −0.68%. Both paged fixed-rate
pairs delivered their unchanged 1,878.0066666666667/s buffered and 1,265.11/s
streaming targets without rejection; CPU changed +0.60% and +0.83%. Every
delivery failure remains in the evidence; sustainable capacity is unproven.

All 28 artifact audits passed, with zero operation errors, timeouts or bounded
server WARN/ERROR lines. Each observation retains CPU and before/after heap
profiles, readable reports, binary/source identities, histograms, affinity/process
evidence and server metrics. Sparse samples do not exclude intervening backend work.

Candidate full repository tests, vet and focused race suites passed. Tests
covered nil/canceled/expired contexts, Wait-before-Close, accepted retained
writers, concurrent waiters blocked behind the drain callback, and Close/final
release races. The candidate's one production file was restored from its exact
accepted16 snapshot, and its new test was archived and removed. All 639 accepted
driver-source hashes match again. Restored full tests and focused lifecycle race
checks passed. No benchmark clients remain; no write diagnostic ran.

Evidence: [summary](results/optimization-owner-17-summary.json),
[28-observation audit](results/optimization-owner-17-audit.json),
[candidate patch](results/optimization-owner-17.patch),
[source manifest](results/optimization-owner-17-source/manifest.json), and
[restoration](results/optimization-owner-17-restored-source.json).
Snapshots, tests and profiles use the `optimization-owner-17-` prefix.
Accepted source/binary remains16, `profile-stream-14-v2-after`, SHA-256
`529839229335a3ccec31edbb00ee6d61840a2feadbca4e1cbf32be2d0a9f8b8b`.
Rejected `profile-owner-17-after` is evidence only, SHA-256
`b1a054777f97b6a1244f42943847ef8a0406e6ed02c01a3cf7492af3d06272fb`.

Fresh18 may review compact routing permutation storage. A separate agent may
assess redundant logical-execution signaling using original eager owner.Wait.
Naively colocating a continuation root reader and decode state would newly retain
root column metadata through an escaped value reader's state pointer after the
old response releases its metadata charge. State plus depth-zero value colocation
may preserve the existing graph, but needs nested-reader, first-page and working
storage review. Cache caps, SHA validation, metadata budgets and the write hold
remain unchanged. Convergence remains unverified.

## Iteration 18 — compact inline replica permutations

Agent: `/root/routing_iteration_18`. Retained one routing-plan storage change:
the sixteen inline permutation positions now use `uint8`. These positions index
the replica slice and are bounded by its complete length, never by eligible-host
count. Replica sets larger than sixteen still use full `[]int`; global host
indices, host/RF counts and all fallback groups remain unrestricted. Both paths
retain the exact PCG draws, rejection sampling and Fisher–Yates order. Traversal
uses the replica length, so placement invalidation clearing the replicas and int
fallback also prevents stale inline positions from being visited. No extra mode
field or cache was introduced; the eight inline emitted targets remain intact.

DWARF verifies actual struct sizes **624 →512 bytes**. The existing first-target
benchmark verifies the corresponding allocator cost **640 →512 B, one object
per plan** across 3/30/300 hosts. Focused tests separately cover complete replica
sets through 300 entries, including 16/17 and 255/256/257 boundaries with global
host positions above 255. They verify deterministic shuffle order at three
seeds, SERIAL/LOCAL_SERIAL order, complete host traversal, locality and remote
stages, leaders, disabled/disconnected/down hosts, distinct shards, latency
second passes, placement invalidation before and during traversal, and concurrent
independent randomized plans. Full repository tests, focused root/routing race
tests, full vet and diff checks passed before timing. An initial fixture omitted
`Kind: OrdinaryStatement`; its preserved failure log records the test-only
correction before timing. The production candidate was unchanged throughout all
measurements.

All **16 observations** used v2 only, exactly 30 seconds, zero timed warmup,
CPU8/GOMAXPROCS1, point buffered concurrency64 and concurrency4 for point stream
and both paged adapters. Each group used baseline/current/current/baseline order.
Both retained datasets were fully validated and warmed before every observation
with `--reuse-prepared`; no writes, reloads or legacy-driver builds occurred.
Means of two observations per source:

|Maximum workload|Operations or partitions/s, baseline →current|Allocated B/op or partition, baseline →current|CPU µs/op or partition, baseline →current|
|---|---:|---:|---:|
|Point buffered|81,192.53 →80,600.70|9,409.43 →9,281.45|12.295 →12.386|
|Point stream|46,775.72 →48,078.35|10,623.29 →10,495.27|19.970 →19.664|
|Paged buffered|2,657.58 →2,685.82|27,845.42 →27,203.08|366.302 →365.905|
|Paged stream|1,788.62 →1,810.52|28,347.07 →27,706.28|535.529 →533.751|

Retained for repeated **approximately 128 fewer bytes per point operation and
640 fewer bytes per five-page partition**, with allocation counts unchanged.
These are 1.36%/1.21% fewer bytes for buffered/streaming point reads and
2.31%/2.26% for buffered/streaming paged reads. Mean point-buffered throughput
changed −0.73% and CPU +0.74%; the other groups changed +1.06% to +2.78% throughput
and −0.11% to −1.53% CPU. There is no systematic measured CPU regression and no
speedup or latency claim. The first paged-stream allocation sample attributes
11.70% of baseline bytes and 9.71% of current bytes to routing Plan; precise
MemStats counters and the allocation-class benchmark establish the byte savings.

The parent requested a pause between observations after the seventh completed
measurement to create eleven signed commits for prior accepted optimizations.
The live client finished normally before the stopped batch parent was terminated;
no observation was interrupted. The final buffered-paged baseline resumed after
about nine minutes. Source bytes and both binary hashes were unchanged across
the pause, and `--identity-from` retained their archived provenance. Buffered
paged baseline throughput varied 2,770.77 →2,544.40/s across that gap, further
limiting timing comparisons. The parent verified accepted HEAD's routing source
still exactly matches the pre-candidate source.

The audit verifies all sixteen CPU/heap profiles, exact binary/source identities,
30-second duration, zero timed warmup and CPU affinity. All observations report
zero operation errors, timeouts, rejected arrivals and server WARN/ERROR lines.
Sparse backend metrics retain existing commitlog variations; the write hold
remains. Additional scheduled observations were not required to resolve a
regression in these four maximum comparisons. **Sustainable capacity and overall
optimization convergence remain unverified**; all earlier delivery failures are
preserved. Resource/cache limits, SHA validation, metadata lifetime, eager
input-owner drain signaling and previous accepted changes are unchanged.

See the [summary](results/optimization-routing-18-summary.json),
[integrity audit](results/optimization-routing-18-audit.json),
[isolated source patch](results/optimization-routing-18.patch),
[layout proof](results/optimization-routing-18-layout.json) and
[pause record](results/optimization-routing-18-pause.json). Candidate binary:
`profile-routing-18-after`, SHA-256
`2dba2de5398062b6c1c4301a68e321c9f4c85721ff9c4eeb92d9a55bcc3e9027`;
baseline: `profile-stream-14-v2-after`, SHA-256
`529839229335a3ccec31edbb00ee6d61840a2feadbca4e1cbf32be2d0a9f8b8b`.

## Iteration 19 — registered-owner drain reuse (rejected)

Agent: `/root/lifetime_iteration_19`. Rejected and restored exact accepted18
source. The single candidate removed the redundant logical execution `done`
channel and made session shutdown join the registered input owner with its
existing eager `InternalWait(context.Background())`. Registered owners are
permanently closed; their signal closes after the parent release callback and
registry deletion. The all-operation cancellation pass, late stop installation,
shutdown locking, public context semantics and borrowed/output lifetimes were
unchanged. Full repository tests, focused lifecycle/race tests and vet passed;
no correctness issue was found.

Actual allocation measurements showed logical objects shrinking from144 to128B
(lifetime48 to40B, logical136 to128B). Heap profile locations confirmed the
separate112B channel disappeared, and all four maximum read modes reproduced
approximately128B and one allocation saved per logical operation. Five-page
partitions save this once per partition, independently of page count.

The balanced30-second maximum observations and original frozen-rate controls
left repeated performance/delivery concerns:

|Read mode|Maximum CPU/op|Maximum p99|Original-rate control evidence|
|---|---:|---:|---|
|Point buffered|+2.15%|+5.07%|Candidate rejected1471/2153 arrivals versus415/953 baseline; CPU+0.89%|
|Paged buffered|+1.13%|+17.23%|p99+19.68%; candidate rejects0/0 versus baseline0/1|
|Point stream|+1.94%|+3.05%|Only candidate new1 completed; unbalanced, no comparison claim|
|Paged stream|−0.92%|−0.07%|CPU−0.09%, p99−2.25%, zero rejected arrivals|

These observations do not establish causality or a speedup. The repeated
buffered concerns are sufficient to reject the candidate under the retention
bar despite its exact allocation reduction. Every rejection remains counted.
The original rates were56794.75666666667 point buffered,32233.273333333328 point
stream,1878.0066666666667 paged buffered and1265.11 paged stream. Queue and
concurrency settings were unchanged; no lower-rate diagnostic was launched.

There are29 completed observations:16 maximum and13 original-rate controls,
each30seconds, zero timed warmup, CPU8/GOMAXPROCS1, v2 only, full immutable-data
validation/warming before measurement, CPU and memory profiles retained. All
operation errors and server warning counts were zero. At the parent stop
request, the coordinator alone was paused and the live point-stream control
completed normally. That final observation was postprocessed, then the idle
coordinator terminated; no next observation launched or timed client was
interrupted. The final three stream controls were deliberately omitted after
sufficient negative evidence. No write or legacy client ran.

Evidence: [summary](results/optimization-drain-19-summary.json),
[29-observation audit](results/optimization-drain-19-audit.json),
[isolated candidate patch](results/optimization-drain-19.patch),
[layout proof](results/optimization-drain-19-layout.json),
[profile allocation proof](results/optimization-drain-19-profile-layout.json),
[source proof](results/optimization-drain-19-source-audit.json),
[stop record](results/optimization-drain-19-stop.json) and
[exact source restoration](results/optimization-drain-19-restored-source.json).
The rejected binary is preserved as `profile-drain-19-after`, SHA-256
`c76af5fdfaec62ca3770dc8df3e76577a9c6634523fa845b7d4a54b1635634e5`.
All five candidate files, including the added race tests and layout benchmark,
are archived under `results/optimization-drain-19-source/after/`.

## Iteration 20 — native serve-local idle-header adapter (rejected)

Agent: `/root/timeout_audit_20`. This candidate used one reusable native
serve-local reader adapter, rather than iteration15's serving-mode propagation.
Only the native `InternalServe` path passed it to the existing frame processor.
The adapter forwarded deadline disarming to the original reader and normalized
only a zero-byte timeout while waiting for the first header byte. Progress cleared
its local disarm before header remainders or bodies. `Conn.InternalR` was never
replaced; startup/direct readers, protocol APIs and v5 stayed on their original
paths. No mutable Conn policy or per-read/frame allocation was introduced.

**Rejected and reverted.** Original-rate buffered point controls repeatedly
rejected more candidate arrivals and used more CPU with higher p99 latency.
Streaming allocation savings did not override that unresolved common-path
concern. This is an observed retention failure, not a proven causal attribution.
No source revision or second prototype followed the measured failure.

All24 completed observations used only v2, CPU8/GOMAXPROCS1, exactly30 measured
seconds, zero warmup and validated/warmed retained datasets with
`--reuse-prepared`. Maximum groups ran old/new/new/old; original-rate buffered
controls ran new/old/old/new. Each mean below includes two runs per version.

|Mean metric / workload|Before|Rejected candidate|Change|
|---|---:|---:|---:|
|Buffered point maximum ops/s|81,067.52|82,157.93|+1.35%|
|Buffered point maximum CPU µs/op|12.317|12.148|−1.37%|
|Buffered paged maximum partitions/s|2,720.88|2,745.03|+0.89%|
|Buffered paged maximum CPU µs/partition|359.987|360.212|+0.06%|
|Paged stream maximum partitions/s|1,832.33|1,874.75|+2.31%|
|Paged stream maximum CPU µs/partition|528.532|518.892|−1.82%|
|Paged stream bytes/partition|27,705.94|27,252.93|−453.00 B|
|Paged stream allocations/partition|233.748|218.751|−14.997|
|Point stream maximum ops/s|44,856.18|44,094.20|−1.70%|
|Point stream maximum CPU µs/op|20.627|20.535|−0.45%|
|Point stream bytes/op|10,495.39|10,043.35|−452.04 B|
|Point stream allocations/op|115.512|100.511|−15.001|
|Original-rate buffered point CPU µs/op|15.304|15.709|+2.65%|
|Original-rate buffered point p99 µs|11,983|20,703|+72.77%|

The fixed buffered point target remained56,794.75666666667/s. Candidate runs
rejected5,178/3,235 arrivals versus baseline1,278/0; mean delivered throughput
changed56,772.15→56,652.55/s. Fixed buffered paged target remained
1,878.0066666666667/s and rejected no arrivals; mean CPU was effectively unchanged
(+0.03%), with p99 +6.54%. Strict delivery remains unresolved. End-window queues
were16 for candidate point1,12 for baseline point2 and1 for candidate paged2;
every admitted operation fully drained/completed. All queues and failures remain
in the audit rather than being treated as passing target delivery.

Point stream maximum had a clear chronological rate drift
(41,151.9→43,108.9→45,079.5→48,560.5/s across old/new/new/old), so its mean alone
would have required a reverse-order repeat. The repeated original-rate buffered
failure already disproved retention: the coordinator was stopped after the active
paged run completed normally. Eight scheduled stream observations and the
proposed point maximum repeat did not launch. No observation was truncated or
discarded. No write clients ran; the unhealthy-write restriction remains.

All24 observations had zero operation errors, timeouts and server WARN/ERROR
lines, verified source/binary identities, CPU and before/after heap profiles,
allocation/retained reports, raw histograms, process/affinity evidence and server
metrics. The audit separately records rejected arrivals and nonempty end-window
queues. All receive/cache/lifetime/resource limits and validation remain intact.

Candidate full default/unit suites, complete protocol/transport race suites,
focused unit-tag header/exclusive/TLS/Step07/cancellation/startup race checks,
vet and diff checks passed. Focused tests pinned eager custom Formatter timing,
exact direct/startup text and ordered multi-cause errors; untouched idle versus
partial headers (including progress plus error), body failures, original reader
identity, idle retry/fatal partial shutdown, and panic deadline restoration.
A reused successful header allocated zero objects; escape analysis showed one
native adapter allocation before the serve loop and only cold wrapped-error
classification storage. V5 did not allocate the adapter.

The exact accepted18 source was restored, including the original eager
input-owner channel and logical completion channel. Restoration verification is
recorded in the summary. Accepted baseline remains
`profile-routing-18-after`, SHA-256
`2dba2de5398062b6c1c4301a68e321c9f4c85721ff9c4eeb92d9a55bcc3e9027`.
Rejected binary `profile-adapter-20-after`, SHA-256
`22e0c3d9b58e308fd221408eec956389bc4a925a1ca92ecf0b989e128a3937fd`, is evidence only.

Evidence: [summary](results/optimization-adapter-20-summary.json),
[24-observation audit](results/optimization-adapter-20-audit.json),
[comparisons](results/optimization-adapter-20-comparisons.json),
[isolated patch](results/optimization-adapter-20.patch),
[source manifest](results/optimization-adapter-20-source.json),
[batch state](results/optimization-adapter-20-batch-state.json), and
[candidate checks](results/optimization-adapter-20-tests.json). Both candidate
files are archived in `results/optimization-adapter-20-source/after/`.

## Iteration 21 — bounded prepared-EXECUTE scalar slab (rejected)

Agent: `/root/convergence_audit_21`. Rejected and restored exact accepted18.
The single production candidate used a64-byte scalar slab only for uncompressed,
inline native, non-PREPARE, non-batch, payload-free prepared EXECUTE requests.
All contents remain borrowed: even every optional field requires at most37
owned bytes on v3/v4 and46 on v5. General/custom-payload/prepare/batch/query and
compressed constructors retained their256-byte minimum. No ownership,
validation, timeout, routing, resource-limit or lifecycle behavior was changed.

Independent wire-oracle tests covered the exact scalar bounds, nil versus
present-empty paging state,65535 values and overflow,65535-byte IDs/keyspace,
500KiB multipart values, multi-segment v5, borrowed backing pointers and retained
headers across later requests. Full default/race tests and vet passed before
load. Same-shape baseline/candidate allocation benchmarks proved192B saved with
unchanged allocation count for9 eligible version/shape pairs;14 general-path
controls were unchanged. Raw heap byte labels independently show256→64B at the
actual native-header slab allocation in all four read modes.

All40 observations were30seconds, zero timed warmup, CPU8/GOMAXPROCS1, v2 only,
with the same prepared immutable data fully warmed/validated before every run.
There were32 original balanced observations (all maximum and original-rate
phases) followed by exactly8 bounded point-stream maximum/control repeats.
Source bytes stayed frozen throughout timing. No writes or legacy client ran.
The exact saving repeated as192B/point and960B/five-page partition, with unchanged
allocation counts. These savings alone do not establish a worked optimization.

The original point-stream scheduled p99 rose31.85%; the fresh balanced repeat
rose36.72%. Across all four observations per source, mean p99 rose34.39%,
from4656 to6257µs. Candidate rejected arrivals were1/1/281/1 versus baseline
0/0/0/1. Pooled scheduled CPU changed+0.23%; maximum CPU changed+1.37%, with its
repeat normalizing to−0.07%. The repeated scheduled tail/delivery concern is
sufficient to reject the candidate without claiming a correctness bug or
causal attribution. No additional repeat or source variant was attempted.

Other modes do not erase that concern. Point-buffered maximum CPU changed−1.94%
and original-rate CPU+0.66%, with candidate rejections199/0 versus192/1682.
Paged maximum CPU changed+0.04% buffered/+0.48% stream; scheduled CPU changed
+0.16%/+0.51%, with zero rejected arrivals. Paged scheduled p99 rose8.62%/6.27%.
Every original rejection and observation remains counted. All operation errors,
timeouts and server WARN/ERROR counts were zero; sparse backend metrics cannot
exclude intervening variation. No speedup or sustainable-capacity claim is made.

All 640 accepted18 source identities and the exact source-file set were restored;
only this worker's new test was removed, after archiving it with the candidate.
Full default/race tests and vet pass after restoration. All clients/coordinators
exited and the experiment lock is free; the cluster remains for the next
parent-owned iteration. **Scalar storage remains256B, not64B. No commit was
created;12 accepted signed optimizations remain.**

Evidence: [summary](results/optimization-slab-21-summary.json),
[40-observation audit](results/optimization-slab-21-audit.json),
[allocation-class benchmark](results/optimization-slab-21-layout.json),
[raw profile allocation proof](results/optimization-slab-21-profile-layout.json),
[isolated patch](results/optimization-slab-21.patch),
[exact restoration](results/optimization-slab-21-restored-source.json),
[shutdown](results/optimization-slab-21-shutdown.json) and
[ranked remaining-hotspot audit](results/optimization-slab-21-hotspot-audit.json).
Rejected binary `profile-slab-21-after` SHA-256:
`dfb19560ea71d18af0fe158889e3cb6bd1e3667436425991d145342e5cd5504f`.

## Iteration 22 — exact nine-descriptor continuation hint (rejected)

Agent: `/root/descriptor_iteration_22`. One measured prototype reserved exactly
nine descriptors for uncompressed native prepared continuations with contiguous
values, a nonempty transmitted metadata ID, a nonempty paging state and a default
timestamp. Payload, keyspace, PREPARE, batch and compressed shapes kept their
existing behavior. Generic four/eight-slot hints and the 256-byte scalar slab
were preserved. No bytes, borrowed spans, marks or immutable Freeze indices
changed; larger multipart requests were excluded to avoid growing a nine-slot
reservation to eighteen instead of sixteen.

An independent native-byte oracle passed across v3/v4/v5, negotiated metadata,
absent and present-empty states/IDs, optional flags, and nested single/multipart
Chain slices. Exact accepted18 versus candidate allocation probes measured
**1,280 → 800 bytes and four → three allocations per matching v3/v4 request**;
v5 also saved 480 bytes and one allocation. Measured excluded shapes, including
point, multipart, payload, keyspace, batch, PREPARE and compression, were unchanged.
Full tests, full race tests and vet passed before timing.

A separate read-only packet capture verified the actual runtime workload through
both adapters: each existing 128-row partition used four 32-row data pages and
one empty terminal page. All four continuation requests, including the terminal
request, carried nonempty metadata ID/state and a timestamp. The scalar int32
parameter uses one contiguous framed span. This supports **1,920 bytes and four
allocations saved per partition**, rather than assuming a 160-row workload.
Actual heap sample classes showed the old 512-byte request descriptor growth and
no corresponding growth in the candidate; native scalar slabs stayed 256 bytes.

All **32** observations used only v2, CPU8/GOMAXPROCS1, exactly 30 seconds, zero
timed warmup, complete dataset validation/warming and retained source identities.
Each case used two observations per version with balanced old/new order. All
original worker counts, fixed rates and queue sizes stayed unchanged. No dataset
writes, legacy builds/runs, rate reductions or larger queues were used.

|Maximum case, mean of two observations|Accepted18 → candidate ops/s|Bytes/op before → candidate|CPU µs/op before → candidate|
|---|---:|---:|---:|
|Point buffered, 64 workers|74,399.65 → 74,842.33|9,281.52 → 9,281.54|13.427 → 13.343|
|Point streaming, 4 workers|48,235.83 → 47,188.50|10,495.45 → 10,495.23|19.583 → 19.916|
|Paged buffered, 4 workers|2,644.68 → 2,624.37|27,204.58 → 25,284.72|366.428 → 368.104|
|Paged streaming, 4 workers|1,833.68 → 1,845.98|27,704.77 → 25,785.43|521.238 → 522.543|

Both original-rate paged controls had zero errors/rejections on both versions at
1,878.0066666666667/s buffered and 1,265.11/s streaming. Allocation savings
reproduced, but no CPU/throughput improvement was established. Paged control mean
p99 changed 4,401 → 4,587 µs buffered and 5,427 → 6,301 µs streaming; individual
tails varied substantially.

**Rejected under the unchanged original-rate delivery gate.** At the original
32,233.273333333328/s point-stream target with **four workers**, accepted18
controls rejected **0 / 0** arrivals; candidate controls rejected **0 / 103**.
The failed candidate reached the unchanged 1,024-entry queue and p99 23,135 µs;
baseline p99 values were 10,799 / 13,943 µs. Point-buffered controls at the original
56,794.75666666667/s also failed sustainable delivery on both sides: accepted18
rejected 1,192 / 2,741 and candidate rejected 11,394 / 2,272 arrivals. These small
balanced samples do not identify the instruction causing the delivery difference;
the failed gate prevents retaining the measured allocation benefit. No repeat,
second prototype or production commit was made.

All observations had zero operation errors and no server warnings/errors. The
artifact audit passed all 32 source/binary, CPU/heap, histogram, affinity, joined
completion, original-worker and fixed-rate checks. After all clients exited,
only this iteration's production change and owned tests were removed. All 640
accepted18 source paths/hashes and frozen HEAD `7e1ad6f` were restored; full/race/vet
checks pass on the restored source. Eager input-owner and logical-done channels,
receive-cache bounds (four objects, 16 MiB total, 9 MiB/object), the write hold,
and every rejected observation remain preserved.

Evidence: [summary](results/optimization-descriptors-22-summary.json),
[comparisons](results/optimization-descriptors-22-comparisons.json),
[32-observation audit](results/optimization-descriptors-22-audit.json),
[allocation oracle](results/optimization-descriptors-22-layout.json),
[actual request capture](results/optimization-descriptors-22-runtime/request-shapes.json),
[profile classes](results/optimization-descriptors-22-profile-layout.json),
[archived patch](results/optimization-descriptors-22.patch), and
[exact restoration](results/optimization-descriptors-22-restored-source.json).
Rejected binary `profile-descriptors-22-after` SHA-256:
`35a9d7821bcdb145db763372ad5ddcdb1b20c49aa9e37f17c84364afae979565`.

## Iteration 23 — compact emitted routing identities (rejected)

Agent: `/root/emission_audit_23`. **Rejected and reverted.** One candidate
replaced each emitted full `Target` with a full-width host index plus the full
`Option[uint32]` shard. It retained eight inline entries and unbounded overflow.
Leader/replica traversal reused its existing host lookup index; fallback reused
its direct host index. No extra map lookup, cache, host/RF/shard limit or retained
memory budget was introduced. Comparing captured immutable host UUIDs when
indexes differ preserved duplicate-UUID topology semantics. Unset shard options
kept their wildcard behavior, distinct explicit shards remained distinct,
`StrictShard` remained absent from duplicate identity, and latency probe
reservation still occurred before duplicate suppression.

Layout and actual first-target benchmarks verified **512 →416 bytes/plan**, one
allocation unchanged, across 3/30/300 hosts and ordinary/LWT/leader/latency/pinned
plans. Actual CPU/heap observations reproduced approximately **96 bytes/point
operation and 480 bytes/five-page partition** saved. Allocation profile byte
labels confirmed the routing allocation class changed 512 →416 in all four modes.
The captured metadata/index lifetime and placement invalidation behavior were
unchanged. An independent pre-change full-Target traversal oracle, high host
indices, RF through 300, duplicate UUIDs, shard 0/1/MaxUint32, unset nonzero shard
values, overflow, locality/remote/leader/pinned/availability, slow second passes,
probe reservations, invalidation and concurrent plans passed. Full repository
tests, full race checks and vet passed before timing. A test-only generic-int
literal build failure and a nonexecutable build-script invocation are preserved;
both were corrected before timing without changing the production candidate.

All **22 completed observations** used v2 only, exactly 30 seconds, zero timed
warmup, CPU 8/GOMAXPROCS 1, full retained-data validation and `--reuse-prepared`.
The 16 maximum observations used baseline/current/current/baseline at 64 workers
for buffered point and four for both paged modes and streaming point. Means:

|Maximum workload|Baseline rate/s|Candidate rate/s|Bytes saved/op or partition|CPU change|
|---|---:|---:|---:|---:|
|Point buffered|81,418.18|81,424.27|95.99|+0.07%|
|Point streaming|50,223.75|50,743.33|95.85|-0.58%|
|Paged buffered|2,758.43|2,751.02|479.03|+0.37%|
|Paged streaming|1,858.28|1,906.65|481.81|-2.24%|

The original buffered point control kept **56,794.75666666667/s, 64 workers and
the same 1024-entry queue**. Baseline rejected **2,060 / 2,024** arrivals; candidate
rejected **3,577 / 4,578**. Candidate p99 was **19,951 / 21,103 µs**, versus baseline
**18,527 / 19,487 µs**. Mean CPU changed 15.23405 →15.34377 µs/op (+0.72%). Both
candidate observations had worse delivery and tail outcomes. These small samples
do not identify an instruction causing the difference; the repeated concern
prevents retaining the measured memory saving. Baseline buffered sustainable
delivery remains unresolved.

The parent then stopped further launches. One paged-buffered candidate/baseline
control pair had already launched; its live 30-second child finished normally.
Both delivered the unchanged 1,878.0066666666667/s target without rejection, but
the reverse pair and both streaming control groups were not run. The coordinator
paused before `paged-buffered-control-old2`; the stop artifact preserves all 22
completed names and 10 unlaunched cases. No observation was interrupted, repeat
or second prototype attempted, write performed, or production commit created.

All 22 artifact audits passed source/binary, CPU/heap, histogram, affinity,
completion, original-worker/rate and server-health checks, with zero operation
errors/timeouts/server WARN/ERROR lines. After all clients/coordinators joined,
only this iteration's owned files were restored/removed. All 640 accepted18
source paths/hashes, the 512 B layout expectation and unchanged HEAD `7e1ad6f`
were verified; restored full tests, full race checks and vet pass. Accepted
source/binary remains 18. This rejection does not establish optimization
convergence; a fresh iteration 24 audit was subsequently run and rejected below. The write hold persists.

Evidence: [summary](results/optimization-emission-23-summary.json),
[comparisons](results/optimization-emission-23-comparisons.json),
[22-observation audit](results/optimization-emission-23-audit.json),
[layout/allocator proof](results/optimization-emission-23-layout.json),
[profile classes](results/optimization-emission-23-profile-layout.json),
[archived patch](results/optimization-emission-23.patch),
[stopped batch](results/optimization-emission-23-stop.json), and
[exact restoration](results/optimization-emission-23-restored-source.json).
Rejected `profile-emission-23-after` SHA-256:
`6ca66e33073bf80e8873373e8badff15318756f2b15580ba0968f9444e93cc3a`.

## Iteration 24: certified parameter slabs rejected

A fresh agent tested one cached scalar bound in typed parameter binding.
Only the private compiled builtin scalar encoder and private `paramsPlan` could
certify a bound. A builtin int32 parameter then used one eight-byte slab;
custom, variable-width, wrapped, composite and unknown plans retained 256 bytes.
Metadata alone could not certify a custom encoder's body length. Framing,
validation, ownership, rollback and concurrent plan semantics were preserved.
The compiled plan grew from 24 to 32 bytes once per preparation. The pinned
allocation benchmark measured 640 to 392 B/bind, with six allocations unchanged;
live point reads confirmed approximately 248 B/op saved.

All 16 balanced maximum-load observations completed without errors or warnings.
Point-buffered throughput improved 1.96% on average, paged buffered improved
0.56%, paged streaming was essentially unchanged, and point streaming fell
0.99%. Those speed observations did not establish a consistent win.
At the unchanged original point-buffered target of 56,794.75666666667/s with
64 workers and the original 1,024-entry queue, baseline rejected 89/642 arrivals
and candidate rejected 639/3,085. Baseline p99 was 6.435/15.495 ms versus
14.511/20.127 ms for candidate; candidate mean CPU/op rose 0.87%. Baseline
variation was material, but the repeated delivery/tail concern was sufficient
to reject the candidate. No causal instruction attribution is claimed.

The current paged-buffered child finished normally, then the coordinator stopped
before the next launch. All 23 completed observations are preserved; the nine
remaining controls were never launched. Every completed observation has a
30-second CPU profile, before/after heap profiles, warmed read data, pinned
source/binary identities and CPU8/GOMAXPROCS1 validation. There were no writes.
The candidate patch and tests are archived; all 640 accepted18 source paths and
hashes were restored exactly. Full tests, full race tests and vet passed after
restoration; HEAD stayed unchanged and no commit was made. The fresh iteration25
audit and rejected helper candidate are recorded below; convergence remains unproved.

Evidence: [summary](results/optimization-binding-24-summary.json),
[comparisons](results/optimization-binding-24-comparisons.json),
[23-observation audit](results/optimization-binding-24-audit.json),
[sealed bound and footprint proof](results/optimization-binding-24-proof.json),
[allocation benchmark](results/optimization-binding-24-benchmark.txt),
[archived patch](results/optimization-binding-24.patch),
[stopped batch](results/optimization-binding-24-stop.json), and
[exact restoration](results/optimization-binding-24-restored-source.json).
Rejected `profile-binding-24-after` SHA-256:
`01824918076172459303f54cf42f0b5ff5729efed3d83d15df4a90016dc17f7e`.

## Iteration 25 — direct buffered lease check (rejected)

Agent: `/root/convergence_review_25`. One candidate replaced only
`requirePresent`'s `CheckBytesAlive(r.Bytes())` with the same direct lease
predicate. Reader errors still preceded nonpresent-state errors, which still
preceded the identical `ErrReleased`. Public decoder signatures, constructors,
`Bytes`, callbacks, cursor/framing checks and all resource limits stayed intact.
No pointer-signature or validated-constructor change was bundled.

The bounded proof passed 20 state/error/live-or-released combinations, nil-lease
success and a real fragmented int32 decoder with retained/finally released
borrowed storage. Compiled helper copies fell from five to three, its stack
frame from 344 to 192 bytes, and isolated CPU8/GOMAXPROCS1 benchmarks remained
at zero bytes/allocations. Three sequential one-second observations per source
measured approximately 13–15 → 3.4–3.6 ns per helper call. Real Int64 and Blob
scalar benchmarks also improved. The actual paged profile removed the sampled
`CheckBytesAlive` copy caller. These proofs justified live profiling; they did
not establish a worked optimization.

All 20 completed observations used v2 only, exactly 30 seconds, zero timed
warmup, the original workers/rates/queues, complete read-data warming, and pinned
source/binary identities. Maximum groups used baseline/candidate/candidate/baseline
order, with two observations per source in each of four modes. Mean CPU changed
−4.28% for point buffered, −1.46% for paged buffered, −0.90% for paged stream and
+0.70% for point stream; allocation counts and bytes were essentially unchanged.
Individual timing variation remained material; no causal speedup is claimed.

**Rejected under the unchanged original-rate buffered delivery gate.** At
56,794.75666666667/s, 64 workers and the same 1,024-entry queue, baseline rejected
867 / 742 arrivals; candidate rejected 2,776 / 4,717. Baseline p99 was
16,327 / 11,967 µs versus candidate 20,303 / 20,431 µs. Mean CPU was essentially
unchanged (15.32345 → 15.32018 µs/op), but both candidate observations had worse
delivery and p99. Baseline sustainable delivery remains unresolved. Every
failure is preserved, without claiming which instruction caused the difference.

The coordinator joined at its planned boundary after the first balanced control
group; the remaining 12 controls were never launched and no observation was
interrupted. All 20 artifact audits passed source/binary, CPU/heap, histogram,
affinity, completion/accounting, original worker/rate and server-health checks.
Operation errors, timeouts and server warnings/errors were zero. There were no
writes, repeats or second prototypes.

All 640 accepted18 source paths/hashes and HEAD `7e1ad6f` were restored exactly;
only the owned scalar helper was restored and its owned test removed after
archiving. Full default tests, full unit-tag race tests, default/unit vet and
diff checks pass after restoration. Clients/coordinator joined and the experiment
lock is free. No commit was created; 12 signed accepted optimizations remain.

The fresh comprehensive audit does **not** claim global optimality or convergence.
A separate fresh26 read-only audit owns the private validated-constructor
hypothesis. Packed state/value storage still predicts zero bytes saved per
five-page partition, though object-count proof is outstanding. General stream
plan caching still needs exact custom-callback, mutable-plan and metadata-budget
proof. Latest-source write coverage and reliable capacity remain incomplete;
a parent-authorized limited diagnostic cannot replace the failed original
capacity/sustainable targets. Final frozen-source repeated verification,
required compiles/harness checks and benchmark shutdown remain parent-owned.

Evidence: [summary](results/optimization-liveness-25-summary.json),
[comparisons](results/optimization-liveness-25-comparisons.json),
[20-observation audit](results/optimization-liveness-25-audit.json),
[compiler and allocation proof](results/optimization-liveness-25-proof.json),
[actual profile copy proof](results/optimization-liveness-25-profile-copy-proof.json),
[archived patch](results/optimization-liveness-25.patch),
[exact restoration](results/optimization-liveness-25-restored-source.json),
[shutdown](results/optimization-liveness-25-shutdown.json), and
[remaining proof/coverage matrix](results/optimization-liveness-25-hotspot-audit.json).
Rejected `profile-liveness-25-after` SHA-256:
`b80b22debc4d0959ccae63ae2730a59295f8fd35f93c2f75cca228a1e347c35b`.

## Iteration 26 — validated framed-reader construction (rejected)

Agent: `/root/value_constructor_26`. One candidate replaced only the three
successful `readFramedValue` calls to `NewValueReader` with direct immutable
reader literals. Entry validation and framing already prove the constructor's
type/state/body conditions. Public `NewValueReader`, all signatures, callback
counts, type/lease/cursor error precedence, rollback, nested/sticky failures and
value/depth limits stayed unchanged. The 1,344-case differential oracle and 16
valid-path allocation checks passed. Actual compiled framing copies fell from
13 to four sites, stack use from 552 to 368 bytes, and all three constructor
calls disappeared. Isolated reads improved 24–42% with zero bytes/allocations.

All 20 observations were v2-only, exactly30 seconds with CPU+heap profiles,
zero timed warmup, CPU8/GOMAXPROCS1, original workers/rates/queues, full immutable
data warming and pinned source/binary identity. Balanced maximum groups showed
mean CPU changes of −1.34% point buffered, −1.07% paged buffered, −1.50% point
stream and +0.28% paged stream, with allocation costs essentially unchanged.
Actual paged copy samples fell from about200 to10ms per30seconds. These proofs
establish removed work, without proving causality for every timing difference.

**Rejected on repeated original-rate dispatch-to-finish p99 regression.** At
56,794.75666666667/s, 64 workers and the unchanged1,024-entry queue, baseline
dispatch p99 was2,247/2,265µs versus candidate2,391/2,449µs: both repeats worsened,
with means2,256→2,420µs (+7.27%). Scheduled-to-finish p99 and delivery were mixed:
baseline rejected1,982/0 arrivals versus candidate1,215/504; scheduled p99 means
were16,135→15,771µs. Scheduled CPU was+0.31%. All material latency participates
in the acceptance gate; favorable total-latency means do not erase the repeated
dispatch-tail concern. Sustainable delivery remains unproved. Every rejection
and all raw histograms are retained.

The coordinator paused and joined after16max+4original-rate buffered controls.
The remaining12 controls were never launched; there were no writes, interrupted
observations, second prototypes or commits. All20artifact audits passed with
zero operation errors/timeouts or server WARN/ERROR lines. Exact640accepted18
source paths/hashes and HEAD7e1ad6f were restored; the owned test is archived.
Restored full/default, full unit-race, unit-vet and diff checks are recorded in
the restoration logs. Twelve signed accepted optimizations remain.

Fresh27 owns only the distinct state plus root **value** storage proof after
parent release. The [remaining-code and latest-source coverage audit](results/optimization-constructor-26-hotspot-audit.json)
does not claim convergence. Latest-source writes, final repeated all-mode
verification and benchmark shutdown remain parent-owned.

Evidence: [summary](results/optimization-constructor-26-summary.json),
[all20profile audits](results/optimization-constructor-26-audit.json),
[full latency gate](results/optimization-constructor-26-gate20.json),
[actual compiler proof](results/optimization-constructor-26-profile-compiler-proof.json),
[archived patch](results/optimization-constructor-26.patch),
[exact restoration](results/optimization-constructor-26-restored-source.json), and
[join/lock record](results/optimization-constructor-26-stop.json).
Rejected binary `profile-constructor-26-after` SHA-256:
`c6358c419ff96c19d900be86cc180d035c24e0e6f4685db78a9869404885dcb3`.

## Reproduction

With this benchmark's retained cluster resumed, build a distinctly named
current-worktree binary and select a new output directory:

```sh
sh tests/driver-comparison/build-profile.sh \
  .codex-cache/driver-comparison/profile-next
python3 tests/driver-comparison/profile_run.py \
  --binary .codex-cache/driver-comparison/profile-next \
  --output tests/driver-comparison/results/optimization-next \
  --scenario read --concurrency 64
```

The helper acquires the experiment lock, rejects existing output directories,
executes only the specified v2 binary, resets and warms the dataset, and fixes
measurement to 30 seconds with no warmup. `--stream` selects the v2 streaming
adapter; scenarios are `read`, `write`, and `paged`. `--reuse-prepared` is only
for a documented dataset already prepared and settled by the same experiment;
it still warms/validates every key before measurement. `--rate OPS_PER_SECOND`
selects scheduled arrivals. Continuing optimization gates keep their original
fixed targets: point buffered 56,794.75666666667/s with 64 workers, point streaming
32,233.273333333328/s with four workers, paged buffered 1,878.0066666666667/s and
paged streaming 1,265.11/s, both with four workers. Preserve queue sizes and all
rejection/delivery evidence; do not recalculate lower rates or increase workers
to make a candidate pass. Zero rate selects closed-loop maximum load.

Inspect allocation samples after subtracting setup:

```sh
go tool pprof -top -sample_index=alloc_space \
  -base RESULTS/measurement.heap.before.pprof \
  BINARY RESULTS/measurement.heap.after.pprof
go tool pprof -top BINARY RESULTS/measurement.cpu.pprof
```

## Handoff to a fresh iteration agent

Iteration18 compact inline routing permutations remain the latest retained
change on top of16 continuation-validation sharing. Iterations15,17,19,20,21,22,23,24 and25
are rejected. Preserve their measured tradeoffs, the write hold and every
scheduled delivery failure. Original eager shutdown/input-owner signals remain;
both general scalar minima remain256B; neither the request64B candidate nor
the certified parameter8B candidate was retained.

Iteration23 measured and rejected compact emitted-routing identities despite
verified 96 B/plan savings: both original-rate buffered controls had more rejected
arrivals and worse p99 than baseline. All 640 accepted18 source paths/hashes and
restored full/race/vet checks pass. Remaining controls were stopped after the
repeated concern; no second prototype or commit was made. Iteration24 then
rejected its parameter-slab candidate; iteration25 subsequently rejected its direct
lease-check candidate. The fresh26 constructor candidate was also rejected; fresh27 owns a distinct state/value packing proof.
Preserve the [rejection and partial control evidence](results/optimization-emission-23-summary.json).

Iteration24 measured and rejected a sealed builtin parameter-slab bound. It
saved 248 B per builtin int32 binding with unchanged allocation count and only
an eight-byte preparation-plan increase, but original-rate buffered controls
showed delivery/tail concern. Preserve the [23-profile evidence and proof](results/optimization-binding-24-summary.json);
do not retry the same mechanism in the fresh remaining-hotspot audit.

Iteration25 verified a direct lease-check copy reduction but rejected it after
repeated original-rate buffered delivery/p99 concern. All 20 observations and
the [remaining proof/coverage matrix](results/optimization-liveness-25-hotspot-audit.json)
are retained; the exact accepted18 source and restored checks pass. Fresh26 subsequently rejected its constructor prototype on repeated dispatch p99 concern; fresh27 owns only the separate state/value packing proof after parent release.

Iteration22 measured and rejected the exact nine-descriptor continuation hint.
It verified 480 bytes/one allocation saved per continuation and 1,920 bytes/four
allocations per actual 128-row partition, but failed the original four-worker
point-stream delivery gate (baseline zero/zero rejects, candidate zero/103).
The existing four/eight-slot hints remain. Preserve this negative result; any
substantive successor belongs to a fresh agent and must keep original controls.
Actual point reads also set PageSize32, already reserve8 and show no request
descriptor growth; the earlier160B no-page point hypothesis describes a generic
benchmark shape, not this measured workload. See the
[ranked audit](results/optimization-slab-21-hotspot-audit.json) for exact evidence,
other candidates and scoped exclusions. Root reader/state colocation still has
its documented escaped-metadata retention hazard; packed state/value colocation
predicts zero bytes saved per five-page partition after its empty-page cost.
The suggestions and percentages below are historical; exact immutable
continuation validation sharing is already implemented.

Continue fresh-agent hotspot audits before declaring optimization exhausted.
Iteration10 has already removed the duplicate native-header builder; do not
repeat that change. Its after paged-stream profile shows remaining body-builder
growth (18.02% of allocated bytes), routing plans (9.79%), and per-page stream
reader/validation construction (11.33% cumulative bytes; 17.42% allocation objects).
A fresh agent can examine bounded validation-plan reuse across unchanged page
metadata, preserving metadata/lifetime checks and working-storage accounting.
Required SHA validation consumes about 56% of buffered and 39% of streaming CPU;
streaming syscalls consume another 31%. Raw streaming receive ownership and
read-ahead/resource limits must survive any attempted syscall reduction.

Iteration08 adds an actual streaming candidate: `protocol.ReadHeader` eagerly
formats benign zero-byte idle-timeout errors used when exclusive streaming
reservations interrupt the shared read loop. `fmt.Errorf` and its formatting
children account for 3.52% of streaming allocation bytes through that site.
A lazy wrapper may avoid formatting normal control-flow errors, but must
preserve `errors.Is(ErrReadHeaderTimeout)`, `errors.As(net.Error)`, the original
cause, and identical `Error()` text. Partial-header timeouts must remain fatal.
This was unchanged in iteration08. Iteration15 later tested and rejected one
serve-only implementation, and iteration20 rejected a distinct native serve-local
reader adapter after repeated original-rate buffered delivery/CPU/latency concern.
Both saved about452 B/15 objects per streamed partition. Preserve both failures;
no current timeout-formatting candidate is accepted.

Earlier buffered iteration07 profiles also show these remaining candidates:

1. Builder scalar/descriptor growth accounts for 11.97% of sampled bytes,
   Borrow descriptors 4.72%, and Freeze descriptors 4.12%. Request/header builders
   are stack-local after iteration07; further work must preserve stable slab and
   borrowed-span ownership instead of copying encoded payloads or used builders.
2. `internal/wire/receive_cache.go`: `ReceiveCache.Acquire` accounts for 10.95%
   sampled bytes. Its four-object idle cap is a resource invariant.
3. Admission, buffered page execution and cancellation remain substantial.
   Preserve independent cancellation and input drains; the redundant page scope
   and unconditional delayed-retry channel have already been removed.
4. Routing Plan contributes 6.11% of sampled bytes after its state-size reduction.
   Preserve metadata snapshots, locality, ordering and independent plan state.
5. NewBuilder is now 1.14%, through parameter encoding. A fresh agent can inspect
   whether parameter writer storage permits the same embedding benefit; external
   codec interfaces may force escape, so this is a hypothesis requiring evidence.

Repeat any noisy speed/latency comparison, keeping every run exactly30 seconds.
Use a brand-new agent for the next optimization. Read modes need further audits,
and writes and both paged adapters still require fresh hotspot audits and final
regression measurements after shared driver changes. Buffered
scheduled-arrival rejection remains unresolved; retain its evidence.
Point-write baselines now exist; iteration09 rejected its candidate and restored
the source. Capacity/delivery and server-health limitations remain as above.

## Iteration27: inline streaming value state rejected

Fresh iteration27 tested only an inline depth-zero `StreamValueReader` in its
private decode state, with the active flag moved into existing padding. Root
row readers stayed separate, nested readers retained stable independent
identities, and each response still allocated fresh mutable state. The
candidate and focused tests are archived in
[the iteration27 evidence](results/optimization-packed-27-summary.json).

The isolated allocator proof saved24B/two objects for a populated one-page
point stream, and eight objects with zero net bytes for the actual four
populated pages plus empty terminal page. An empty-only page cost96B more;
its inline descriptor was charged even before use. This diagnostic accounting
does not control runtime budget admission. Metadata/transform/depth/count
limits and the original320KiB storage-test ceilings were unchanged. Full
semantics, callback escape/sticky errors, page isolation, nested accounting,
default tests, full unit-tag race tests and vet passed before profiling.

All40 completed observations ran only v2 for exactly30 seconds, preserving
original workers, rates, queues, complete row/hash validation and CPU/heap
profiles. Original32 point controls had mixed total/queue tails, so the parent
released one opposite-ABBA repeat of the two scheduled point modes. Buffered
pooled results resolved that concern, but the last point-stream baseline logged
critical98% disk-utilization warnings on all three servers. Its execution had
zero operation errors, but it is explicitly unhealthy and excluded from healthy
acceptance comparisons. This is backend evidence, not a finding that the
candidate caused the warning. The resulting healthy point-stream comparison is
unbalanced and does not resolve the original tail concern; no further repeat
or variant was attempted.

Iteration27 is therefore rejected conservatively. All40 artifact audits pass;
39 observations are healthy and one retains its health/exclusion markers.
Raw schedule histograms also distinguish exact end-window cutoffs from full
queues while preserving every strict rejected-arrival failure. All640 accepted18
source paths/hashes and HEAD `7e1ad6f` are restored; the candidate test was removed
only after archival. Restored default/full unit-race/default and unit-vet/diff
checks pass. Clients and coordinators are joined, the run lock is free, and no
commit was made. No optimization-convergence or write-capacity claim is made.

## Storage recovery03 and iteration28 handoff

On 2026-11-01 at 21:54 UTC, the parent losslessly archived 302 large,
terminal client stdout/stderr logs from this optimization experiment.
Their 6,847,573,435 plaintext bytes occupy 46,931,833 gzip bytes. Each
compressed file was decompressed and its SHA-256 matched the original
before unlinking the plaintext path. This preserved every log byte while
freeing approximately 6.3 GiB. The parked `2026-11-01*` experiment, database
data/configuration, binaries, CPU/heap profiles, result JSON, raw histograms
and server metrics were unchanged.

[Archive manifest](results/optimization-log-archive.json) maps each original
`*.json.log` path to its `*.json.log.gz` file and original SHA-256. Read
these logs with gzip, or restore using `gzip -dc COMPRESSED > ORIGINAL` and
verify the recorded checksum. The original archival script and its hash
are retained in [method evidence](results/optimization-log-archive-method.json).

All three existing nodes then explicitly reported critical disk mode
`false`, compaction enabled and repair enabled. Three subsequent idle
probes found no queued preparation work. There was no restart or change
to dataset, addresses, ownership, resource limits, rates or workers.
[Recovery03 evidence](results/optimization-cluster-recovery-03/summary.json)
retains the server transitions, state, metrics and approximately 42.9 GiB
of available filesystem space. This recovery does not establish healthy
write capacity or repair the excluded iteration27 baseline observation.

Iteration27 remains rejected and the exact accepted18 source is restored.
Fresh iteration28 is released only for the distinct fixedCursor length-view
compiler/allocation/semantic proof and full checks; the parent will release
its read-only workload profiles after verifying those results. All write clients remain
held for a separately labeled, bounded current-source diagnostic. The full
goal, current-source write evidence, final verification and shutdown remain
incomplete.


## Parked checkpoint

The user parked all advances and test results. The 12 accepted optimizations
remain committed at `7e1ad6f718929342200c5b9bc126e5a5e7fc7d1c`; no unaccepted candidate was committed.
The subsequent comparison against master is incomplete: 12 of 24 planned
30-second observations finished. All benchmark processes and nodes are stopped,
with data and configuration retained. See [the parked checkpoint](PARKED-STATE.json)
for bookmark branches, backup paths, result locations and resume constraints.


## Roadmap experiment, 2026-11-02

This is a separate experiment starting from retained v2 `7e1ad6f`, with master
`1ceaac8` used only as an implementation reference. No master or
scylla-go-driver workload was run. [ROADMAP-STATE.json](ROADMAP-STATE.json)
records a disposition and named prerequisite for all twenty ideas, along with
exact source/binary identities and validation priorities.

Five fresh agents implemented independent prototypes. Complete patches for all
five apply cleanly to the original baseline; their tests, failed revisions,
source snapshots, profiles, raw histograms, and allocation proofs remain in
`results/roadmap-01/` through `results/roadmap-05/`. No prototype was committed or
left applied to production. The original unfinished request-copy patch is
preserved separately in `results/roadmap-01/initial.patch`.

| Item | Decision | Local primary evidence |
| --- | --- | --- |
| 1: page request copy | Deferred | 12.014% fewer allocated bytes/partition, paired 95% interval [12.002%, 12.025%]; CPU regression 0.181%, within the 2% memory-candidate ceiling. |
| 2: bounded streaming read-ahead | Deferred | 28.530% lower CPU/partition [28.122%, 28.983%], 36.864% higher throughput; allocated bytes increase 15.815%. Scratch is capped at 4 KiB and ReadAheadBytes per reservation. |
| 3: fused integer/blob decoding | Deferred | 9.329% lower CPU/partition [9.094%, 9.563%], 10.175% higher throughput; allocation volume essentially unchanged. |
| 4: one-pass parameter capture | Rejected | ABBA CPU improvement 1.282% and throughput improvement 1.191%, below 3%; adds 32 bytes/bind. |
| 5: pooled framing scratch | Rejected | ABBA CPU improvement 1.311%, throughput improvement 1.379%, and byte reduction 5.984%; misses both acceptance thresholds. |

These are **local synthetic native-v4 observations**, not real-Scylla acceptance.
Items 1–3 have six paired comparisons: three A–B and three B–A, with deterministic
20,000-resample paired bootstrap intervals. Rejected items stopped after their
ABBA screen. Each measurement lasted 30 seconds and retained complete untimed
warming and in-operation SHA256 checks. Original workers, datasets, page shape,
queue bounds, consistency, retries, prefetch, coalescing, CPU8/GOMAXPROCS1, and
fixed-rate targets were preserved. The initial contiguous framing variant also
failed the unchanged borrowed-backing identity test and is archived separately;
the measured revision preserves every canonical value span through writer
completion. Streaming cancellation timing exposed a winning-cause propagation
race; the revised candidate passes deterministic entry-gate/manual-close tests
and 100 repetitions of the existing cancellation test.

The first item01 observations overlapped dedicated Scylla startup/idle/flush
work, and item02's initial screen overlapped its shutdown. Those observations
remain exploratory and unchanged. A new item01 ABBA screen and six-pair
confirmation, and a new item02 ABBA screen, ran with dedicated Scylla stopped.
The item02 and item03 confirmation batches also ran with those nodes stopped.
The ledger selects the replacement observations explicitly. No repeated testing
was used to promote a rejected candidate.

### Why real acceptance is blocked

The frozen original implementation failed its original 64-worker, 30-second
acknowledged-write maximum with **64 server WriteTimeout errors** at LOCAL_QUORUM.
Its context-timeout counter and rejected-arrival count were zero; those do not
negate server errors. The fixed 52,503.01/s control was not launched after the
failed maximum. No rates or queue bounds were relaxed.

[Backend diagnosis](results/roadmap/backend-diagnosis.json) retains the failure
and read-only investigation. All three nodes developed commitlog scheduling
queues of roughly 8,000–13,000 tasks and about 467 MB of flushing memtables per
node. Device write latency rose from about 0.5 ms to about 40 ms, with a large
I/O queue increase. No CPU throttling or OOM was observed. The proximate failure
is commitlog reserve starvation and storage/flush backpressure; the ultimate
shared-device or Scylla scheduling cause remains unresolved. Storage attribution
and remediation followed by a clean original 64-worker maximum and unchanged
fixed-rate baseline are prerequisites. This is not evidence that a prototype
caused the backend failure.

Items 1–3 therefore remain deferred despite meeting local primary thresholds.
They still need healthy real-Scylla paired confirmation, all five canonical
workloads and fixed-rate controls, and applicable RSS/memory/default comparisons.
Items 6–12 await post-retention profiles or their specific lifetime/contention
proofs. Items 13–20 have individual configuration, mixed-workload, ownership,
connection-budget, callback-lease, v5/LZ4, or transport-cancellation prerequisites
in the ledger. Recommended validation order after backend recovery is 2, 3, 1.

### Verification and retained evidence

Restored production default tests, full unit-tag race tests, vet, and integration
compilation pass. Integration compilation is not live integration acceptance.
Each final prototype passed its recorded focused/default checks; items02 and03
also passed their recorded broad race checks. The v2 harness race tests pass on
CPU8/GOMAXPROCS1, vet passes, and all eighteen Python tests pass. An evidence
module boundary at `results/go.mod` excludes archived source copies from Go
package discovery. Initial recursive-harness and wrong-affinity test failures
are retained with their corrected runs, alongside failed prototype tests.

[Final artifact audit](results/roadmap/final-audit.json) verifies frozen source
and binary hashes, profiles, raw histograms, warming, affinity, configuration,
error/drain accounting, and qualification gaps. The earliest build manifests
record the input harness go.mod hash before relocating its two module paths;
the audit preserves those originals and verifies the exact frozen counterpart.
[Shutdown evidence](results/roadmap/final-shutdown.json) records joined clients,
stopped benchmark servers, closed endpoints, and the free run lock. Historical
artifacts, unrelated services, and database configuration/data are retained.

There are zero accepted changes, so production and the original v2 baseline are
identical by source/tree identity. No combined optimization gain or convergence
is claimed; real combined qualification remains blocked by the failed original
write baseline.

Final audit: **76 successful local observations and one failed original real-Scylla
write observation**, all 30 seconds, across twelve sessions and six frozen builds.
Artifact integrity passes with no issues; this does not grant performance acceptance.


## User-requested parked checkpoint

All roadmap work is parked. The checkpoint commits the harness, complete
prototype patches, ledgers, and selected summaries. Raw evidence, binaries,
frozen sources, worktrees, and stopped database data are preserved in a local
archive identified by [PARKED-CHECKPOINT.json](PARKED-CHECKPOINT.json). No
optimization has been accepted or applied to production. Resume only on an
explicit user request; the original write-baseline blocker remains unresolved.
