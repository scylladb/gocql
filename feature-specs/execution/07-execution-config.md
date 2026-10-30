# Step 07: execution configuration, lifetimes and public streaming

Owner: `/root/step07_execution_config`; branch: `v2-07-execution-config`.
Accepted predecessor: `07a3e0ed5b48f1785c7a98006b2a7e6eae7c9af3`.
Exact commands, source snapshots, log hashes, native configuration hashes,
runner identities and stop/join proof are in
[`step07-verification.json`](../evidence/step07-verification.json).
The exhaustive exposed-field/default/zero/snapshot audit is
[`07-config-field-audit.md`](07-config-field-audit.md).

## Result and invariants

Statements, prepared handles, bindings, routing and batches carry the explicit
unknown/ordinary/conditional classification. Negotiated server confirmation
refines unknown classification and conditional confirmation overrides ordinary.
CQL text does not infer classification. Unknown and conditional operations use
conservative deterministic routing and never speculate. Regular serial
consistency promotes conditional execution; optional serial alone does not.
Host pinning survives custom policies, and strict shard targets cannot silently
relax. Compatible retries and reprepare reuse canonical encoded input.

Per-operation closed input owners separate a binding's admissions from delivered
output. Accepted writers, speculative losers, delayed retries and prefetch work
retain that operation until its last input access ends. Session shutdown rejects
new admissions, cancels and joins active and idle work, and shares completion
between repeated/concurrent Close calls. It releases prefetched output while a
delivered buffered current page and independent PageLease remain valid until
advance/close. Input drains never wait for output leases.

Configuration values and per-call descriptors are captured once. Profile
replacement affects new admissions only. Metadata periodic refresh/request
bounds are 60/31 seconds, zero disables the local work/deadline, and internal
page deadlines include local decoding. Automatic server timeout is 30 seconds
on supporting Scylla targets; explicit zero omits it and positive unsupported
settings fail. Bootstrap, topology, schema, agreement and route queries use the
same selection. Qualified v3/v4 namespace overrides are checked without a
mutable USE operation. Explicit paging knobs for protocols/APIs that cannot
apply them return setting errors. Generated timestamps are obtained once per
eligible logical execution and reused. One prefetch worker is reused across a
pager's continuations. Percentiles publish from bounded atomic samples once per
second; Metrics reads the published summary. Request and attempt counters remain
exact. Observer/logger panics are contained and callbacks run outside driver
iterator/publication locks, under their documented prompt-return contract.

RunStream[B,R] composes existing Bound[B] canonical input with an independent
StreamRowDecoder[R]. It uses the configured pool's exclusive reservation and
incremental receive directly. It neither invokes a buffered decoder nor gathers
a cell, row, page or response. Actual metadata selects a checked immutable plan
through the same exact prepared revision/schema/ID fences. Raw v3/v4, TLS and v5
raw/LZ4 operate; v3/v4 frame compression returns an explicit pre-submit capability
error. Scylla v3's negotiated tablet-payload envelope is accepted only when that
extension was advertised; ordinary v3 still rejects that flag.

Streaming prefetch/speculation is disabled. Pull paging handles empty and
present-empty continuations, full cyclic history and count/byte limits. Output
starts before Reset; no row reset, partial write, delivered row or later server
error transparently replays. Caller seeded capacities/backing survive decoding
and close. A rejected concurrent NextInto never touches its destination. Close,
context/session cancellation and duplicate Close calls join active input and
destination access through one completion. Explicit early Close cannot acquire
an error from its own cancellation callback. Incomplete connections are
poisoned and repaired within the existing pool budget. StreamInfo retains
cumulative rows/pages/attempts, its first terminal error, and latest-attempt
warnings/trace/payload, including failed attempts. Failed-attempt metadata owners
remain charged while the next attempt allocates replacement metadata.

## Evidence and measured limits

Deterministic race tests cover unknown/LWT speculation gates, immutable profile
capture, safe zero-write retry classification, host pins, batch admission
rollback, generated input reuse through UNPREPARED, buffered retained output,
observer/logger panics, raw/v5 public streaming, first-row-before-rest-of-page,
raw direct read targets, capacity/nil errors, empty/cyclic/limited paging,
metadata fences/independent plan selection, integrity failure, partial first-row
writes, no replay after output, pool exhaustion, idle shutdown, custom Reset
joins, concurrent rejection and late cancellation after explicit Close.

Full uncached unit race, focused race twice, vet, integration/CCM race builds,
three bounded two-worker fuzz smokes and allocation/copy/scaling benchmarks
passed. The final two-file latest-attempt metadata change was separately frozen
and every affected unit/race/build/execution-fuzz/API-cost check reran. Native
wire/lifetime behavior and codec/framing primitives did not change; the retained
native and final source snapshots make that delta explicit.

Eight native lanes completed successfully in one CCM start/test/stop/join
runner using owned stable20 and Cassandra21 addresses: authenticated Scylla
2026.3.4 raw v3, raw v4, v4 TLS and v4 Snappy capability rejection; Cassandra
5.0.9 v5 raw/LZ4, each plaintext and TLS. Each lane ran the public streaming
integration test and buffered runtime smoke. Required stream lanes read five
480,000-byte rows with nested list/blob and text directly into seeded storage,
across three pages, then verify early close, bounded repair, shared execution
and empty results. Snappy's passing lane proves rejection, not streaming
support. Both clusters stopped/joined; all owned native/TLS endpoints refused
post-stop probes. Experimental22 was untouched.

Equivalent public API memory-reader/peer benchmarks vary rows and cells
independently and exclude final caller storage/immutable fixture construction.
Final RunStream setup stays around 9.3–9.5 KiB and 87–88 allocations for 1/100/
1,000 rows and 1 KiB/400 KiB/8 MiB cells, while buffered NextInto retains page
payload and rises to about 8.4 MiB for the largest single cell. Raw pointer tests
prove direct final-range reads; v5 bounded-unit copy/segment scaling is retained
from the same-source exclusive receiver benchmarks. History lookup independently
varies 1/1,024/65,536 continuation states with zero lookup allocations. Shared
admission runs at 1/4/16 workers. These measurements include peer scheduling and
API setup, not native database throughput or a comprehensive peak-memory/CPU/
first-row-latency comparison. Small-row streaming is slower than buffered
iteration in this fixture; no universal speedup is claimed. Step 15 owns the
full equivalent native performance matrix and independent page/consumer scaling.

## Diagnostics and handoff

Initial native v3 exposed rejection of negotiated tablet feedback. After its
correction, native TLS exposed own-cancellation replacing successful early Close.
Both failed lanes remain diagnostics; no partial passes count. A final targeted
UNPREPARED extension probe found missing terminal-attempt StreamInfo metadata;
its failure and the replacement-budget regression/pass are retained. Superseded
check logs/source snapshots are distinguished from final affected results.
Exploratory compile and fixture-free integration-start diagnostics remain in the
owned raw archive and are never counted as build/native passes.

No assigned step 07 required feature is deferred. Later steps retain fixture
infrastructure, all174 migration/matrix, legacy removal, package extraction,
module/docs, comprehensive native performance, independent review and release
verification. This handoff replaces none of those gates. Callback cancellation
and drain guarantees assume the explicit user callback contracts; arbitrary
blocking/asynchronous user code cannot be preempted safely.

Committed benchmark logs strip trailing whitespace from Go's CPU header. The final verification and active check
receipt `sha256` hashes the committed normalized copy; `raw_sha256` and
`raw_path` identify the byte-exact external original. The formatting index uses
repository-relative log names, matching command receipts and the final manifest.
Superseded diagnostic receipts preserve original runner paths/hashes and use
the retained diagnostic archive mappings.
