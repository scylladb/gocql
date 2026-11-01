# Step 15: performance, copy boundaries and scaling

Branch: `v2-15-performance`. Accepted predecessor:
`5edeb77e37756549d1ed1cbb7581ff2c2484301b`.
Gate: passed, pending parent evidence review. Steps 16–17 remain required.

Final frozen candidate: 654 native inputs, SHA-256
`f49b9406cedad53fe46af038ad6c39ca4194aae4d800aa5ab526de105b7f44f3`.
The 653-input `8e56033...` snapshot supplies explicitly scoped unchanged v5
measurements; it is not labeled the complete final input snapshot.
Earlier 650-input `38626719...` measurements remain bound to their own sources.

The final performance gate binds every row of both implementation-contract tables
in `step15-requirements.json`. Raw command output, native fixture receipts,
profiles and the complete source snapshot are retained below
`/extra/scylladb/gocql-api-test-cluster/step15/`; their hashes are recorded in
`step15-verification.json`. Earlier interrupted/auth-preflight receipts and the
pre-specialization measurements remain diagnostic/history, without final credit.

## Changes and safety

Contiguous UUID decoding uses its existing single span after the ordinary
metadata/state/lease/length checks. Fragmented UUID decoding retains its checked
cursor path. Text uses `utf8.Valid` for one existing contiguous span; segmented
text and streaming validation keep the shared incremental implementation. No
flattening enables either path. UUID, IPv6 and duration encoding reserve their
final scalar arena range once. Cell-length framing remains a separate write.
Duration writes its vint components directly into that final range, without a
standalone payload buffer. Golden, every-split, invalid-state/type/width/version,
invalid UTF-8, expired-lease, owned/borrowed and rollback tests remain required.

Compiled scalar/text/blob codecs and nullable/empty/named/dynamic wrappers bind
their `CheckType` contract during compilation. Their execution paths retain
constant kind/immutable metadata guards plus all state/lease/width/content
validation. This is zero warm `CheckType` contract calls, not zero validation
work. Real operation counters verify 10,000 stable rows/pages without warm
compilation, type-contract calls or name-map rebuilding; a changed candidate
compiles/checks once and retains the old page's matching plan and schema fence.

Dynamic rows previously rescanned every preceding physical cell on every
At/Get. They now share a pointer-owned `sync.Once` index across copied row
values, built lazily on first access from offset/length/state descriptors only.
Clone detaches data and owns a fresh independent index. Every cached access
still checks the lease; duplicate/missing names, null/empty states and malformed
framing preserve their checked behavior. Cold index allocation is reported
separately from warm repeated lookup.

Nested bulk child ranges previously revisited every payload fragment at each
ancestor. The nested cursor now visits at most eight local descriptors before
jumping through the existing shared index. Cells up to 32 bytes retain ordinary
sequential traversal. Ordinary row `Cursor.Take` is unchanged. Independent
trimmed-range/zero-length/end-boundary and fuzz checks protect cursor state and
backing identity. Actual depth-16/65,536-fragment traversal falls from 2,097,072
to 304 sequential visits; 16 repeated late-column accesses over 4,096 columns
fall from 1,114,112 sequential visits to zero after the index build, plus 512
shared-index comparisons.

The span compressor previously examined every potential match position on
incompressible input. A bounded progressive search step skips increasingly
unproductive positions while preserving the format, maximum offsets, raw
fallback and source/output ownership. A retained overlay experiment used
identical raw-block/preallocated-output semantics on repetitive, random and
mixed 1 MiB inputs at 1/16/4,096 spans. Random single-span LZ4 improves from
5.75 ms to 72 µs and Snappy from 5.53 ms to 84 µs on that experiment; repetitive
medians remain within about 2%. Exact final-source measurements and independent
decoding/ratio/copy/fuzz checks supply the release evidence. These numbers do
not compare differently framed APIs.

The `gocql_perf` build adds opt-in atomic counters at real work sites. Disabled
hooks inline away in ordinary builds. Counter builds establish operation/copy/
ownership bounds; uninstrumented builds supply CPU, throughput and latency.
Writer spies additionally check the actual canonical payload addresses through
four buffered retry/reprepare/page attempts and three streaming attempts. Frame
construction verifies original canonical backing in requests and batches;
compressor output identity survives v4 LZ4/Snappy and v5 segment framing.

## Equivalent work and baseline

The original driver is pinned at
`82b52574146b42f34ed3fe967d2d5e17cc53996b`, the parent of the preserved
implementation checkpoint. It runs in an isolated detached worktree. Original
tracked driver sources remain unchanged. Only additive measurement files and
the existing fixture certificates are supplied; exact adapter and complete
per-mode baseline source snapshots are retained. The original test-only eager
tablet capability preflight cannot authenticate this fixture and would mutate
an unowned test keyspace. The additive adapter disables that unrelated
`TestMain` callback. Its capability globals have no production references; the
measured original `CreateSession`/`Query`/`Iter`/`Scan` path is unchanged.

Final matched modes run v2 followed immediately by the original-driver comparison
under the same live locked server owner, Go toolchain, GOMAXPROCS=4, protocol,
compression and TLS settings. Both create/drop their own run-prefixed namespaces.
Both use the same 1/128 rows, 32 B/4 KiB/256 KiB/8 MiB cell sizes, 1/32/128 page
sizes, 1/4 worker concurrency and slow consumer shapes. Row, buffered NextInto,
RunStream and old Scan must produce identical completed application output:
scalar ordinal, every blob byte and both nested list elements. Borrowed APIs'
explicit application copies are counted separately; streaming fills the same
seeded output directly. Preparation/compilation, fixture writes and two warm
operations per worker precede measured steady execution.

The first baseline adapter retained the original 200 µs write-coalescing default
and one connection per shard while typed runs used zero coalescing and one per
host. Those historical results are explicitly default-policy comparisons and
support no matched-latency/speedup claim. The final comparison sets both to zero
coalescing, token-aware/shuffled-replica routing with round-robin fallback,
QUORUM/LOCAL_SERIAL, no prefetch/speculation, equivalent retry limits, and one
connection per actual shard (one per Cassandra host). Each old worker reuses its
own precreated Query, matching the prebound typed setup. The original initial
one-second heartbeat settles before measured work; remaining control/background
I/O stays visible. `step15-matched-configuration.json` records legitimate API
contract differences and every actual negotiated host/shard connection,
protocol/compression/metadata-ID and TLS cipher/version is compared across both
clients, for all shapes. Configured defaults alone receive no matching credit.

The historical unchecked varchar/UUID numbers are not a semantic speedup
baseline. A separate original text case includes UTF-8 validation and the same
caller-byte output copy. Remaining constant typed metadata/state/lifetime guard
costs are explicit. New API capabilities, host-policy implementations and lazy
versus materialized container semantics do not support direct speedup claims.
Before/after tuning measurements compare the same v2 operations and checks.

## Memory, I/O and profiling methods

Normal latency/CPU/allocation intervals exclude forced GC, memory sampling and
profiles. First-row latency ends only after complete final-output validation;
end-to-end latency includes exhaustion and Close. Empirical p50/p95/p99 retain
sample counts: 32 observations per ordinary serial shape, 128 at concurrency
four and eight for the intentionally slow shape. These small samples do not
establish stable tail probabilities. Process CPU comes from `RUSAGE_SELF`.

Source data resides on the external native fixture; final output is separately
preallocated. GC heap is recorded before Connect, after Connect, after setup,
at warm idle and during a separate diagnostic. Inclusive driver-plus-TLS-plus-
harness retained heap subtracts the pre-Connect runtime baseline and explicitly
listed final output bytes. Warm pool storage remains included. Incremental
heap over warm idle is a different metric and is never called total driver heap.
Sampled HeapAlloc can include uncollected garbage and can miss short peaks;
post-GC samples report live retained heap. Standard-library TLS, parser plans,
metadata and harness bookkeeping remain visible in the inclusive metric.

A separate native diagnostic uses 1/4/8 workers, the same application outputs,
and the same pool settings across APIs. Every worker parks after its first
complete validated row while retaining its current response/cursor. GC and
sampling observe them simultaneously. Release, context cancellation and worker
joins finish before owned schema cleanup. It records both inclusive retained
heap and the increment over its separately measured warm idle.

Independent generated-source probes vary cells, rows, page bytes and segment
count. The fixed 1 MiB result uses 9/129/2,049 segments while holding cell/row
shape constant. Actual driver byte capacities and post-GC heap are distinct.
The accepted 40-shape storage test additionally covers 256 KiB/8 MiB cells,
128-row pages, ignored 8 MiB cells and slow consumers across all eight raw/TLS/
v5/LZ4 modes. A retained Go overlay adds read-site GC heap sampling to that same
body without changing production or normal benchmark files. Its source unit
and final output accounting stay explicit; producer transient encoding costs
remain included, and no heap claim derives merely from allocation counts.

Native throughput sockets remain unwrapped, preserving OS writev. TCP_INFO
reports kernel-accounted bytes for exactly the driver's sockets. Process
`syscr`/`syscw` include background/control and endpoint-file I/O; the endpoint's
quiescent overhead is separately calibrated and is not silently subtracted.
These counts are neither net.Conn Read/Write calls nor TCP-segment/TLS-record
counts. Actual coalescer loopback benchmarks run separate strace diagnostics
above the OS iovec limit, with an empty-harness control. TLS diagnostics parse
actual application record headers and report net.Conn calls/wire bytes apart.
CPU, mutex, block, heap and allocation profiles are separate from normal timing.

## Algorithmic and ownership evidence

Actual counters distinguish N cells, S descriptor fragments and J random jumps.
Sequential range traversal bounds visits by N+S and performs no binary lookup,
payload copy or per-cell retain/release. Random nested ranges share one O(S)
index and bound comparisons by 2J(log2 S+1), including depths 1/4/16. Lazy list,
map, tuple, UDT, vector and dynamic traversal compile no child/schema plan while
iterating; explicit iterator allocation is fixed, with Collect/Clone separate.
Real narrow/wide rows contain 4/32/128 scalar/text/blob/list columns and split
fields, without complete-row driver temporaries.

Host counts 3/30/300 exercise ordinary, pinned, conditional, leader, latency and
fallback cases. First-target work performs no host-index build/full-host scan;
fallback visits occur only when advanced. Histories 1/1,000/10,000 count incoming
hash bytes/exact comparisons and preserve token addresses, including empty,
colliding and nonconsecutive states. Concurrent warm binding/host selection runs
with the discovery LRU lock held while unrelated schema/placement changes occur.
Borrowed and owned bindings have separate allocation/CPU measurements.

Disabled observation/speculation/prefetch creates zero optional events,
callbacks, metrics snapshots, timers or workers. Default Options creates its
single prefetch worker only for an actual continuation. Changed placement
feedback structurally shares unaffected state; duplicate feedback publishes
nothing, and queue ownership remains bounded without receive-page retention.
Retained-page/prefetch probes count logical per-owner leased-frame byte charges,
actual scalar slab allocations and refcount transfers. These logical charges are not backing capacities or deduplicated physical
bytes; cache capacities and live GC heap are recorded independently. Their 1/4 workers × 1/1,024 cell cases have
constant transfers per page, and retained old output remains valid after Close.

## Verification and remaining release work

Final-source focused correctness/race/architecture/preservation checks and all
required performance modes must pass before this step is accepted. All 174
original test bodies and their accepted helper/method bodies remain preserved;
earlier step-14 behavioral passes remain lineage. They do not replace step 17's
complete native matrix on the final implementation. Step 16 independently
reviews the resulting ownership/cancellation/recovery and performance changes.

## Investigated buffered allocation cost

The native comparison exposes a material buffered allocation/CPU cost, reported
without a universal speedup claim. For the measured v4 raw 8 MiB shape, buffered
Row allocates approximately 8.42 MB/op; the original driver's reusable Scan
buffers allocate approximately 1.98 MB/op. Sampled process CPU is approximately
2.67 ms/op versus 2.14 ms/op; streaming is approximately 1.02 ms/op and 22.8
KB/op on that run. These are empirical measurements at the stated sample count
and paired order. Final per-mode totals/ratios retain all shapes and settings.

The source-bound alloc-space profile attributes about 82.9% of aggregate allocated
space to `receiveTypedCall`, whose raw response path makes a new frame buffer and
publishes a lease with no release callback. No receive-buffer cache currently
applies; existing framer/call/channel pools have different ownership. A separate
rows128 profile shows substantial GC scanning work. Independent page leases
require reuse only after the final reference, but do not require a fresh malloc
when all prior references are released. A retained overlay experiment therefore
compares a bounded session-owned cache against the unchanged source before any
production adoption. It has explicit owner/refcount/ABA/epoch/shutdown tests,
raw/TLS comparisons and mixed buffered/streaming traffic. Candidate measurements
must account for its idle retained bytes as well as allocation savings; its
result and disposition are recorded separately. The existing candidate remains
unchanged while that experiment runs.

The adopted cache is shared by all connections in one session, bounded by
16 MiB, four objects and 9 MiB per object. It returns raw v3/v4 frames only on
final lease release, uses a fresh Lease object on every reuse, and does not retain
Conn/Session pointers in its callback. Close drains idle storage and rejects
late returns. Streaming reservation clears idle storage and increments an epoch,
so old retained pages cannot repopulate that cache later. Compressed raw input
returns separately after transformation; v5 segment owners remain independent.
The raw/TLS experiment reduced 8 MiB row allocation from approximately 8.42 MB
to 20.5 KiB/op and raw CPU from 2.67 ms to 1.32 ms/op, while retaining 8.389 MB
idle cache, explicitly included in heap. Streaming clears that idle cache to zero.
Alternating buffered/streaming large reads forfeits reuse and is reported with
126.7 MB allocated across 32 mixed operations, without an all-workload-win claim.

A subsequent mixed-size check found that four tiny metadata frames could occupy
all slots and starve a useful data buffer despite the free byte budget. A bounded
replace-smallest admission overlay evaluates that concrete policy flaw while
preserving the same hard byte/object/size caps, epochs and final-reference rules.
Its disposition and affected final measurements are recorded before acceptance.

The final admission policy replaces smaller idle blocks when four tiny metadata
buffers would otherwise starve a useful data buffer. It scans at most four slots
and preserves all existing caps and epoch/close rules. Cache misses and nonzero
allocation remain visible. Frame compression now decodes LZ4/Snappy directly into
a final leased arena: common checked DecodedLen validates both protocol and
builtin expansion limits before acquiring output storage, LZ4 shares its existing
AppendDecompressed block decoder, and Snappy uses the same s2 decoder. Encoded
input and decompressed output have independent returns; only final output is held
by its result lease. Error/partial/empty/oversize and owner/backing tests gate it.
Decoders without that internal capability keep their original unpooled input
ownership, avoiding an assumption that their output cannot alias input.

The final eight Scylla v3/v4 modes rerun matched performance, simultaneous memory,
stream recovery, dynamic values and live cache ownership on this exact input
snapshot. Four previously passed Cassandra v5 modes retain their original
653-input source binding. A separate AST/source/control-flow proof establishes
unchanged v5 leased-segment source handling, AppendDecompressed, integrity-unit
and streaming paths, with byte-identical measured harness/configuration. The
changed raw-frame cache/decode branch is not executed by those v5 data owners.
All final-tree correctness, race, copy, allocation, heap, fuzz and public API
checks still run on 654 inputs. This scoped performance reuse does not replace
step 17's required complete native verification on the final release tree.

## Final numerical results and limits

The final manifest covers all 23 required rows, 37 successful final-source
commands, 396 native API/shape results and 96 simultaneous-retention memory
cases. Eight Scylla modes use all 654 final inputs; four Cassandra v5 modes use
the explicit unchanged-path proof above. All 174 original bodies and 403 accepted
helper/method bodies remain exact. Final public go/types reachability and a
normal external consumer confirm 507 root exports, 130 thin forwarders and no
reachable mutable Builder/ScalarBytes or ReceiveCache API. Six owned nodes join,
both final runner PIDs disappear, and all 42 reserved endpoints independently
refuse connections. No native fixture or check remains running.

Representative matched final results below report process CPU in µs/op,
allocated bytes/op and empirical end-to-end p50 in µs. The complete raw totals,
counts, p95/p99 and per-mode comparisons remain in the JSON reports. These rows
use GOMAXPROCS=4 and 32 serial observations; the concurrent shape has 128.

| Native mode and completed output | API | CPU µs/op | Allocated B/op | End p50 µs |
| --- | --- | ---: | ---: | ---: |
| v4 raw, one 32 B row | original Scan | 43.5 | 3,872 | 33.9 |
| v4 raw, one 32 B row | Row / NextInto / RunStream | 70.4 / 34.8 / 52.2 | 20,841 / 20,804 / 22,206 | 52.9 / 40.3 / 56.3 |
| v4 raw, 128 × 4 KiB rows | original Scan | 130.0 | 108,722 | 209.4 |
| v4 raw, 128 × 4 KiB rows | Row / NextInto / RunStream | 319.2 / 314.4 / 460.0 | 78,513 / 78,461 / 22,207 | 411.3 / 387.3 / 552.4 |
| v4 raw, one 8 MiB row | original Scan | 1,484.8 | 1,521,363 | 1,843.8 |
| v4 raw, one 8 MiB row | Row / NextInto / RunStream | 1,444.0 / 1,305.6 / 913.2 | 20,562 / 20,532 / 22,206 | 2,601.5 / 2,495.9 / 1,382.0 |
| v4 TLS raw, one 8 MiB row | original Scan | 3,714.9 | 1,632,775 | 4,952.3 |
| v4 TLS raw, one 8 MiB row | Row / NextInto / RunStream | 3,054.2 / 3,304.9 / 2,846.4 | 32,675 / 31,585 / 33,888 | 4,283.4 / 4,339.2 / 4,110.9 |
| v4 Snappy, one 8 MiB row | original Scan / NextInto | 4,394.1 / 3,207.3 | 8,530,698 / 18,684 | 5,257.4 / 4,441.6 |
| v4 LZ4, one 8 MiB row | original Scan / NextInto | 5,005.4 / 4,046.8 | 8,466,120 / 59,653 | 5,621.8 / 5,445.0 |
| v5 raw, 128 × 4 KiB rows | original Scan / NextInto / RunStream | 261.5 / 495.1 / 247.2 | 554,294 / 615,036 / 158,063 | 668.4 / 704.8 / 551.4 |

The small/128-row CPU and latency regressions remain explicit; there is no
universal speedup claim. Source-bound profiles expose typed reader/metadata/
lifetime accounting, fixed request setup, separate lazy iterator objects and
GC work. Iterator allocation is per iterator, not per cell/element. The required
safety/ownership contracts and exact final output validation stay enabled.
The measured full-frame allocation regression, repeated dynamic scans, nested
fragment revisits and incompressible scanner cost were resolved. The remaining
fixed costs do not violate the zero-copy, zero-per-cell allocation or algorithmic
bounds; they remain useful comparison data for later work.

A warm late-column DynamicRow access with N=4,096/S=65,536 costs about 212 ns and
zero B/op/allocs; its separate first-row/index-build benchmark at 128 columns and
1,024 fragments costs about 18.6 µs and 3,296 B/op/four row-level allocations.
The 128-column fragmented typed NextInto benchmark reports zero B/op/allocs.
Actual vector and list depths 1/4/16 satisfy the same indexed nested bounds.
Final LZ4 on random 1 MiB input at 1,024 spans is about 57 µs, with a 32 KiB
single descriptor-index allocation and ratio 1.004; repetitive input keeps a
useful ratio. Literal transform bytes, descriptors and framing are separate.

Simultaneously parked raw-v4 workers retain inclusive driver/TLS/harness heap
of about 5.78 MB for eight buffered cursors versus 1.54 MB for eight streaming
cursors, after separately accounting 8 × 4,107 application payload bytes.
Idle cache capacity before workers, while parked and after Close is recorded
separately. A negative increment over warm idle is valid when streaming clears
previous cached buffers; it is not negative working memory. Ignoring an 8 MiB
cell across the eight deterministic modes gives an inclusive post-GC driver/
TLS/producer/harness delta of about 48 KiB–1.00 MiB with zero final output;
actual driver byte capacities separately stay below 320 KiB. Producer transient
encoding and TLS objects remain included in that heap delta.

Write amplification is visible rather than hidden by zero-copy counters. For
4,096 one-byte spans, the separate raw loopback strace observes 44 writev calls
across benchmark calibration plus ten measured iterations (11 total transfers,
four writev calls per transfer). TLS reports about 45,079 write syscalls in its
corresponding diagnostic, including handshake/harness work. The empty-harness
control is retained separately. The 64 KiB/1,024-span TLS diagnostic counts
1,024 actual application records, 88,064 wire bytes and 1,024 net.Conn writes per
operation. These standard-library TLS costs are not OS syscall counts, and no
driver payload gather is added to reduce them.

All metrics retain exact Go/CPU/config/concurrency/source/profile bindings.
Percentiles use the lower sorted-sample index floor((N−1)q), not interpolation;
with 32 samples p99 is not a stable tail estimate. Process syscall counters
include background/control and endpoint-file I/O; quiescent endpoint calibration
and independent strace provide their practical scope. Sampled HeapAlloc can miss
transients, while synchronized retained heap and true backing/capacity counters
cover separate invariants. These limitations do not waive any required table row.

Negative-control verification rejects source drift, a missing required check,
a changed live-input adapter label, a missing result shape and mismatched actual
negotiation before emitting a passing gate. Historical interrupted fixtures,
unauthenticated baseline preflight failures, stale comparison setups and an
overlapping failed diagnostic log receive no final gate credit.
