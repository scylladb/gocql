# Step 16: independent implementation review

Branch: `v2-16-independent-review`. Accepted predecessor:
`7a358b2405b7ff9820ad39032e1e33bd309f6e9c`.
Gate: passed. Two actionable findings fixed; zero unresolved actionable findings
on the reviewed result. Step 17 remains required.

Final frozen implementation: 660 inputs, SHA-256
`ba3cdc2cdfa5528062a99d7e0648f3c4c22f48b27099cdaf26eebc6675d2ebdb`.
The complete checks, outcomes, source/contract bindings, historical reproduction
logs, dependency closures and test preservation hashes are retained in
[`step16-verification.json`](../evidence/step16-verification.json).
Raw artifacts live under `/extra/scylladb/gocql-api-test-cluster/step16/`.

## Method and review boundaries

This was a fresh independent review. The reviewer derived invariants from
plan.md, the three Go contracts and the relevant six reference contracts,
then inspected implementation control flow, ownership and tests. Prior accepted
check booleans were evidence to investigate, not correctness proof. The early
risk map covered receive/decompression aliases and ownership; shared dynamic
indices and cursor bounds; compiled codec validation; pull-stream budgets,
reservation and cancellation; metadata/schema fences; and API/feature/assertion
completeness. It produced the two reproducible findings below.

Review included static paths and executed injected-peer/socket paths. Focused
native checks validate affected production paths on the final source. This gate
is an independent review and fix gate; it does not replace step 17's final
complete native, baseline and streaming acceptance matrix.

## Historical findings and fixes

### S16-1 — P2: accepted protocol limits silently narrowed to 256 MiB

At the predecessor's `internal/transport/v2_transport.go:177`, the physical
connection silently applied `min(configured, 256 MiB)` to both frame and
decompressed budgets. Config validation explicitly permits positive signed-int32
limits, and implementation-contract lines 627–631 make 256 MiB the configurable
default. Configuring 268,435,457, 536,870,912 or 2,147,483,647 returned
268,435,456 from the actual connection limits. The retained `limits-before.log`
contains the failing regression; production was the accepted predecessor.

The connection now preserves validated configured limits. Native request
construction, raw and segmented receive, and streaming envelopes consume those
same limits. Pooled and initial handshake framers also receive the physical
limits; they enforce encoded/raw/decompressed bounds. Framer write limits count
the body, matching native framing rather than charging its nine-byte header.

An internal concrete frame decoder gives only exact LZ4/Snappy built-ins the
connection's explicit bound. It checks the advertised size and signed-int32
conversion before acquiring output or invoking the decoder. Decoding writes
into the final arena. The public standalone built-in methods retain their
documented default expansion ceiling. Custom decoders keep their own semantics;
custom decoders lacking final-arena capability still receive unpooled input so
an output alias cannot outlive a pooled encoded allocation. The wrapper adds
zero allocations with supplied output capacity.

Rereview of the larger supported range found two consequential width hazards:
v5 expected frame length added its header in `int`, and the coalescer converted
a span completion to `int` before returning it as `int64`. Header/body arithmetic
and typed completion now remain `int64`. The existing io.Writer control path
retains its integer result; each of those requests already has an int-sized
slice. Its count conversion is bounded. No original assertion/helper was edited.

Regressions exercise the actual request builder up to signed-int32 using repeated
1 MiB backing, actual streaming admission of metadata-only large envelopes,
small real handshake write/read/decompression bounds, configured/default decoder
boundaries and final backing, and a typed coalescer that completes a logical
2 GiB-plus-eight-byte chain through a counting writer. All run on amd64 and 386
without allocating a multiGiB payload. This proves framing, range and accounting
behavior; it does not claim a physical multiGiB server transfer.

### S16-2 — P2: expired slow-host probes had no concurrent reservation

At predecessor `internal/routing/v2_policy.go:379`, latency penalty expiration
was only `Now.Before(until)`. With an expired slow replica and healthy fallback,
32 concurrent first-target selections chose that replica 32 times before any
new observation. This violates the concurrency-safe probe reservation required
by implementation-contract lines 461–464. `latency-before.log` retains the
failing Plan/Next reproduction on the predecessor's production sources.

A per-host atomic deadline now reserves one expired candidate for probing.
Observe refreshes that deadline. An unconsumed plan reserves nothing; a selected
target abandoned before Observe becomes eligible after RetryPeriod, without a
new worker/timer or a permanently held reservation. The regression checks all
three behaviors and a completed probe's next retry interval without sleeping.

Selection checks host, connectivity, locality and shard eligibility before
`accept` considers the candidate. A successful first-pass probe claim is emitted
as the selected target. Subsequent visits in that captured plan have a future
probe deadline, and emitted-target deduplication prevents returning it twice.
The penalized second pass remains last-resort fallback when no healthy choice
exists; it does not grant new host eligibility. Explicit host pins retain their
existing pinning semantics. Ordinary disabled-latency paths add no probe work;
expired enabled candidates use per-host atomics without a policy-wide mutex.

## Independent contract coverage

The table records concrete paths inspected and current focused behavioral proof.
An implementation file alone received no completion credit. All named reference
feature groups were reconciled with the Go disposition table, including the
intentional Go differences in classification, ownership and metadata handling.

| Requirement/invariant | Implementation review and executed evidence |
| --- | --- |
| Input ownership, binding Close/Wait | `InternalInputOwner`, execution registration and writer leases retain independent references; admission closes atomically, timed Wait does not authorize reuse. Existing blocked-writer/loser, batch rollback, pager and session-drain regressions passed. |
| Output ownership, retained pages, cache ABA | `wire.Lease` final-zero CAS invokes a cache/data-only callback; leases are never recycled. Receive cache caps remain 16 MiB/four objects/9 MiB each, epoch clear/close and replace-smallest admission remain bounded. Retained/prefetch/cache ownership and fresh-lease tests passed, including live overwrite/late-release checks in all three native modes. |
| Cancellation, partial writes, recovery | Accepted span writes join before releasing input; partial writes poison the connection. Stable call identities reserve orphan streams until response/death and effective orphan budgets close exhausted connections. Existing queued, coalesced, TLS, timeout, late cancellation and repair tests passed. The larger-range typed completion regression also passed on 386. |
| Config and physical startup boundary | Reviewed validation, immutable snapshots, TLS detach, projection to connectivity, and attachment before pool/services/workers. Host/shard sizing, socket/source/NAT, DNS/setup deadlines, heartbeat, reconnect and explicit option behavior tests passed. S16-1 fixes the discovered silent override. |
| Profiles, retries, speculation, LWT, timestamps | Admission captures one profile/options version, canonical inputs and generated timestamp. Retry receives actual consistency/bytes-written evidence; unknown/conditional/batch/serial paths suppress speculation. Existing profile replacement, observers/losers, conditional classification, paging and retry/reprepare tests passed. |
| Prepared IDs, result plans and schema generations | Exact entry/revision plus dependency-local epoch fences gate publication under the lower schema tracker. Losing pages retain their matching plan/metadata; opaque IDs have equality only. Host prepare flights, changed bind/routing identity, old-page plan, no-metadata and conflict queue/scope/overflow tests passed. No shared lock wraps user compilation. |
| Topology, schema, refresh, agreement | Reviewed encoded metadata.Source query boundary, cold schema materialization, immutable snapshot copies, tombstones/partial retention and publication-before-events. Drop/recreate/read races, scope modes, coordinator agreement, zero success and structured partial errors passed. |
| Events, control and callback bounds | Streaming events transfer bounded metadata charge to a bounded worker; overflow resynchronizes without blocking receive. Subscription overflow guarantees a marker/closure. Existing queue, charge-transfer, shutdown, disabled-event, publication contention and callback isolation tests passed. |
| Tablet v1/v2, version hints and feedback | Negotiation/layout, v2 EXECUTE byte, opaque versions, overlap replacement and ordered replicas retain their actual code paths. Unknown tablet ranges do not fall back to vnodes. Feedback owns detached controls, validates epochs and shares unchanged placement. Existing payload/fence/saturation/copy/work-counter tests passed. |
| Leaders, strict targets, locality and latency | Original leader identity and host/shard strictness survive plan/acquisition; no follower promotion substitutes for an unresolved leader. Lazy fallback and publication-time indices remain. Leader/LWT/pinning/strict streaming capacity, locality, latency expiry and S16-2 concurrency tests passed. |
| Murmur3, CDC and custom partitioners | Canonical encoded spans, composite length/terminators and captured partitioner are reused without payload flattening or re-encoding. Signed-byte/composite/CDC/custom and original backing checks passed. |
| Auth, TLS/cloud, routes and identity | Reviewed per-connection exchange cleanup and typed handshake codecs, driver-owned TLS, physical hook context/snapshot, sticky route selection and route-deletion fences. Focused TLS/SNI/route/auth tests and external contracts passed; live raw/Snappy/LZ4-TLS sessions authenticated. Full cloud/resumption/development/Cassandra modes remain final step-17 work. |
| Public/custom codec/policy interfaces | Normal-build go/types reachability and a real external consumer passed: 507 exports, 130 thin forwarders and existing extension implementations; no reachable mutable Builder/ScalarBytes/ReceiveCache. Reviewed generic value/row/stream plan signatures and custom fallback. No root serializer, old Query/Iter/Batch/Marshal/Scan or reflection adapter was introduced. |
| Module and dependency boundaries | Independently traversed actual normal/all-tag production import graphs. Wire/cqltype/codec/protocol, lower connectivity/transport/pool and metadata exclude their forbidden transitive owners and public root. One v2 module and existing module integrity passed. Exact closures are retained. |
| Buffered destinations and dynamic values | Reviewed direct field DecodeInto, false-result clearing, destination-pointer release, mixed iteration and independent clone. DynamicRow uses pointer-owned sync.Once physical offsets/states shared across copies; Clone owns a new index/backing; cached access still checks lease. Framing/null/unset/empty/duplicates and concurrent access tests passed. |
| Compiled scalar and nested cursor costs | Compiled scalar guards retain type/state/lease/width/UTF-8/version checks; UUID/IPv6/duration write into one scalar arena range with transactional rollback. Nested Take visits at most eight descriptors before the shared index, small scalars/ordinary Take stay sequential. Existing trimmed/base/end/overflow, custom child, warm CheckType/compile and scaling checks passed. |
| Span compression/copy boundaries | LZ4/Snappy adaptive match search uses descriptor indices, valid offsets/overlap/mixed fragments, useful repetition ratio and raw fallback. Inputs and final outputs retain separate owners; compression output is attached directly. Existing independent-decode, malformed, backing/copy, counters and scaling checks passed with the new bounded decoder. |
| Streaming no-gather and final storage | Separate incremental plans/readers write only supplied final ranges; built-in scalar staging remains bounded and arbitrary buffered decoder adapters are absent. Raw direct-read identity, seeded nested/custom/encoded output, capacity-before-body, skipped-value validation and first-row gates passed. S16-1 also exercises large envelopes without payload accumulation. |
| Streaming budgets and integrity | Metadata accounting covers simultaneous replacement/control ownership; ProtocolLimits bound total work and history separately. v5 UnitReader validates CRC before exposure and retains one bounded transform allocation. Split, malformed/truncated/trailing, count/depth, transform and metadata-budget tests passed. v5 retained segment owners remain separate from raw-frame cache owners. |
| Streaming exclusive ownership and lifetime | Reservation is within configured pool capacity, drains shared admissions/calls/orphans and acknowledges reader handoff before query. Cancellation closes incomplete connections before joining destination access. Exact completion alone resumes shared I/O. Concurrent advance, Close/session races, slow consumer, empty pages and repair regressions passed. |
| Streaming retries and schema changes | OutputStarted precedes Reset; after output, decode/error/later-page failures cannot replay. Before output only safe retry/reprepare reuses canonical inputs. Page-state histories use exact equality and independent limits. Matching metadata is selected before output; buffered/streaming plans remain isolated. Existing canonical writer-spy, no-replay, metadata and history tests passed. |
| Original tests, examples and recordings | All 258 pre-existing test files are byte-identical to the predecessor, including all 174 original identities, nested cases and accepted helpers/methods. Full original-to-translated assertion provenance remains retained. Existing external consumer/benchmark corpus tests passed; recording dedup/conflict semantics and normal typed examples were not replaced with CRUD substitutes or feature rejection. |

## Verification and measurement scope

Twelve final-source commands passed. Go JSON records 2,248 correctness pass events
and two explicit skips; 1,873 race pass events and two explicit skips; 165
operation/copy-counter passes; and 11 386 regression pass events. Counts include
nested cases and are not counts of distinct original integration identities.
Affected compression, request construction and routing benchmarks, all-tag
compilation, vet, dependency/API consumer audits and module verification and module-drift checks passed. The repository-required
complete unit race suite also passed; its separate results are in the manifest.
Skipped unit cases and their exact reasons remain in the bound command logs.

One live, address-locked native.py owner used the existing pinned Scylla
2026.3.4 install and the shared step-08 reservation directory. V4 raw, V4 Snappy
and V4 LZ4 over TLS each passed six selected parent tests and 187 pass events:
cache ownership, stream behavior and scalar/tuple/vector/collection dynamic
assertions. Raw streaming delivers final caller storage; compressed v4 modes
explicitly prove pre-submit StreamCapabilityError, as required. Those compressed
passes do not claim streaming compressed v4 delivery. Per-mode cleanup passed,
all three actual nodes joined, reserved endpoints refused connections and the
runner process exited. No other workload/container was changed.

The final compressed decoder wrapper preserves final backing and adds zero
allocations with output capacity. Existing copy/work counters passed. Benchmark
timings here are diagnostics on a shared host, not matched throughput/latency
comparisons or a new speedup claim. Step 15's detailed results, residual fixed
CPU/latency costs, cache idle bytes/mixed churn, TLS/write amplification and
metric scopes keep their original exact source bindings. They are not relabeled
as full 660-input measurements. Step 17 must repeat affected final performance
and all mandatory release coverage.

The first diagnostic run passed its correctness command. It was stopped before
completion when fix rereview required the 32-bit widening changes; its partial
race log receives no final gate credit. The twelve final commands and focused
native receipt share the exact 660-input source hash above.

## Re-review and handoff

Re-reviewed the final diff against the two original invariants and the ownership,
copy, budget, integrity, replay, publication and dependency boundaries. No
remaining actionable defect was established. New tests are additive; no original
body/helper/assertion, feature contract or required functionality was removed or
weakened. Root-owned STATUS.md and RESUME.md remain outside this change.

This step leaves no running fixture/check process. Step 17 must run the complete
mandatory matrix, all 174 semantic cases/subcases, streaming acceptance, fuzz,
race, final API/requirement audit and affected performance checks on the final
release tree. This handoff does not claim that goal is complete.
