# Step 04: transport pools and exclusive incremental receive

Owner: `/root/step04_transport_pools`. Branch: `v2-04-transport-pools`.
Accepted predecessor: `30714309c6770a4651f49aff2c912c101d41908d`.
This handoff satisfies the assigned transport/pool seam gate. It does not claim
public RunStream, complete metadata/control/event orchestration, the full
174-test migration, or release verification. Exact final source hashes,
commands, statuses and log hashes are in `evidence/step04-verification.json`
relative to feature-specs.

## Configuration, dialing and pools

The public DialTarget/Dialer, DialHost and TLSForHost signatures now match the
settled context-aware contracts. Both hooks open raw connections; the driver
selects endpoint/source first and applies TLS once. Host snapshots detach token
slices. TLS snapshots detach mutable config/trust-pool containers and DER,
OCSP/SCT and algorithm slices. Parsed trust certificates, private keys,
callbacks/providers and session caches retain their documented immutable/shared
concurrency obligations. Nil TLSForHost inherits TLS, and cannot downgrade a
required TLS connection. Logical hostname/native identity survives translated
proxy endpoints, including IPv6 bracket/zone handling. Hook revalidation runs
outside driver locks before publication. Endpoint and sharder epochs fence late
opening attempts; compatible initial opens can join an installed layout,
whereas incompatible unknown-generation opens cannot replace it.

Transport defaults now use 5-second setup/DNS budgets, zero read/write/TCP
keepalive/coalescing delay, 30-second CQL keepalive interval/timeout, and source
ports 49,152..65,535. Invalid durations/ranges/linger precision fail before use.
The setup deadline includes DNS, translation, raw dial, TLS/startup/auth,
initial USE, REGISTER and final validation without a restarted budget. Each
resolved alias/port/endpoint owns the deadline claimed by its first opening;
there is no global IP ticket that another contact could steal. Later physical
openings have their own attempt budget under the outer caller/retry limits.

Raw TCP wrappers must expose actual requested socket controls or return
UnsupportedSettingError, including NODELAY and disabled keepalive. The cloud
SOCKS wrapper forwards controls to its actual raw TCP socket. Unsupported
requested Snappy/LZ4 negotiation fails before STARTUP/query, rather than silently
falling back to raw. Known native-v5/Snappy incompatibility fails before setup.
Raw mode and an incompressible v5 segment's documented raw representation remain
valid. Typed TLS supports positive CoalesceWait with ordered original spans;
partial writes poison the connection and drain input access before release.

Typed pool sizing is independent of shard membership. Positive per-host count
caps the total and disables targeted shard-port filling; otherwise the configured
count is maintained on each discovered shard (one logical shard for a nonsharded
host). Pending targeted dials avoid duplicate intentional fills. Excess limits
apply separately to actual shards, including zero; missing and excess counts do
not overflow. Source range exhaustion, unavailable special ports and NAT/wrong
shard observations take the documented ordinary-port fallback. TLSForHost keeps
source/shard selection intact.

Host pools distinguish initializing, ready, broken and terminal closed states.
Late handshakes cannot reinstall superseded endpoints/sharders or closed pools.
Typed error/query/reservation refill triggers all use ReconnectPolicy; a pending
delay or stop decision cannot be bypassed by acquisition or direct runtime fill.
Startup's initial fill is separate. Connection batches use at most 32 workers,
stop a failed batch, and do not allocate one goroutine/WaitGroup count per huge
configured target. Valid large representable targets remain valid.

Malformed SUPPORTED sharding is rejected before parser indexing or picker
allocation: required parameter presence, nonempty single values, nonzero
16-bit count, ID below count, and ignored-MSB range 0..63. Explicit zero MSBs are
valid. Legitimately absent/flag-only extensions and non-Scylla nodes remain
supported. Legacy parser entry points are panic-free too. Checked pool target,
excess and combined arithmetic runs against the actual count before allocation.
All 32,768 nonnegative native stream IDs are usable, and configured smaller
capacities are exact. Orphan budgets count abandoned transmitted calls without
an age exemption and clamp below effective capacity; zero closes on the first
orphan. The actual reporting schema accepts positive integers, so capacity
reporting is also exact at 32,768; an earlier inferred schema cap was disproven
and removed.

A single Unix contact is `unix:/absolute/path` (or a bare absolute path), with
explicit protocol 3 and InitialDiscovery=false. Mixed contacts, TLS and local
source selection fail explicitly. DialTarget carries Network=unix and the real
path; bootstrap snapshots do not pretend the private placeholder is advertised.
One socket cannot represent a remote strict host. The real maintenance-socket
lane passes. General bootstrap identity reconciliation remains step 05.

## Exclusive receive and ownership

A reservation takes one existing configured pool slot. It closes shared
admission, drains admitted calls/writers/orphans, then requests a reader pause.
The idle first-header-byte wake is serialized with deadline arming. A header or
event already started completes before acknowledgement; v5 acknowledgement
waits for the entire shared segment, not merely one frame. The exclusive owner
never reads concurrently with the multiplexer. Shared resumption is acknowledged
before admission reopens. Normal typed requests use another eligible free
connection or wait under their context; strict host/shard selection remains
intact and no extra slot is opened for a slow cursor.

exchange writes canonical request spans only after receive ownership is installed.
It returns checked extensions/metadata/row count without waiting for row bodies.
The separate streamResponse and compiled streaming readers consume directly
into final caller storage. The path never builds responsePage, a complete row,
cell or response/page staging buffer, and never adapts an arbitrary buffered
codec. Raw v3/v4 Read destinations are the final caller ranges. TLS uses the
standard authenticated plaintext reader, with its necessary handoff copies
accounted separately. Native v5 raw/LZ4 verifies at most one current bounded
input/output arena before exposing bytes. Rows/cells/headers can cross units,
without retaining a list of consumed units. Frame-compressed v3/v4 streaming is
rejected before sending.

Metadata/control allocations are checked before allocation under the shared
simultaneous-retention budget, including queued events and old/new metadata
replacement overlap. Event parsing is incremental with exact semantic body
consumption. One bounded connection worker replaces goroutine-per-event dispatch;
charges transfer with queued records and release after consumption/terminal
drain. Terminal enqueue is synchronized with worker shutdown. Overflow is
observable and poisons the affected streaming connection. On completed v5
release, valid control envelopes already in the final unit are dispatched;
malformed leftovers close the connection. No new socket unit is read merely to
drain an abandoned response.

Reservation close wins a terminal generation before touching the connection,
interrupts incomplete I/O before joining it, and is idempotent under concurrent
or delayed closure. A released cursor cannot close a later shared request.
Cancellation registration is amortized at reservation/page/context boundaries;
there is no per-row AfterFunc or escaping cancellation state. A page callback
claims termination under its generation gate before waiting for active I/O, so
completion cannot erase a cancellation that already won, and a stale page cannot
cancel a later page or shared reuse. Row reads use the response-owned page
context; the internal helper has no ignored per-row context parameter.

Close clears both UnitReader storage and interface-held unit slices. Response
release drops body, reader and control references. Caller-owned payload storage
survives closure. Accepted writes retain input ownership until the synchronous
writer finishes/fails; cancellation during an accepted blocked write cannot
release borrowed input or acquire exclusive ownership prematurely.

## Requirement evidence

All final commands below passed against `step04-release-source.json`.

| Required boundary | Authoritative checks |
| --- | --- |
| Pool overrides/counts/shards | Actual Scylla and Cassandra typed-picker host/shard counts, no targeted host-mode fills, two connections per shard, per-shard zero/exact excess, pending targets and wrapped selection position. |
| Source/TLS hooks/NAT | Real verified TLS shard source selection, host context/nil inheritance, detached snapshots/cert bytes, logical/translated/IPv6 identity, source range exhaustion and wrong-shard cooldown. Four original shard-port integration identities pass, including NAT/unreachable/disabled ports. |
| Limits/hostile setup | Empty/missing/ambiguous maps, count zero/65,536/MaxInt, invalid ID and MSB rejected before allocation; valid 65,535 and zero MSBs accepted; overflow and MaxInt target with bounded actual dial workers. Unsupported compressor produces typed error with no STARTUP/query fallback. |
| Runtime settings/states/repair | Normative defaults, exact capacities including stream 0, orphan capacity 1/2/65, actual TCP wrapper controls/SOCKS handshake, linger validation, disabled keepalive and timeout covering blocked write, endpoint/closed/sharder insertion fences. Actual HandleError/query/reservation paths respect long delay and policy stop. |
| Admission/read handoff | Partial-header and whole-v5-segment barriers, orphan draining, pending acquisition cancellation, ordinary typed execution waiting/release/cancel, no side slot, strict shard selection and free non-strict alternative. |
| Incremental modes | Raw v3/v4, TLS v3/v4, v5 raw/LZ4 and TLS counterparts, large real server rows/paging, first-row source gate, read-target identity, corruption/trailing control, exact consumption and early close/pool recovery. |
| Storage/cancellation | 400 KiB versus 8 MiB fixed driver transform storage, held-closed-response arena cleanup, queued event budget/terminal drain/enqueue races, active destination access stopped by close, winning page cancel, stale page/cursor callback after later page/shared reuse, accepted raw/TLS writer leases and TLS partial writes. |
| Regression/portability | Full uncached unit race; focused unit/race twice; vet; race integration and CCM binaries; 386 streams/frame/segment foundations. Full-root 386 legacy serialization limitation from step 02 is not relabeled a pass. |
| Fuzz/performance | Five-second two-worker malformed incremental-frame fuzz smoke; memory-source raw/v5 size scaling, row-count scaling and real TLS write/receive amplification with backing identity. Smoke is not exhaustive fuzzing. |

Final successful database lanes are stable Scylla v4 raw (two top-level tests),
stable v4 TLS (seven, including the four original shard-port tests and Unix),
Cassandra v5 LZ4/TLS (two), and v5 raw (one). Package exits and skips are retained
in the manifest; no failing lane's partial passes count as completion. Unique
per-run tables and exclusive task fixtures were used without shared-keyspace
reset. Both fixture reservations were released after owned processes terminated.

## Performance interpretation

The before/after row benchmark exposed and removed per-row cancellation setup:
1/32/1,024 rows previously used 54/178/4,146 allocations and approximately
5.5/10.4/165 KiB. Final setup is 45 allocations/5,272 bytes per page at all three
counts. These include request encoding/page metadata/reader setup and exclude
final caller storage/fixture construction; they do not claim an allocation-free
execution API.

For 1 KiB, 400 KiB and 8 MiB cells, synthetic raw-v4 transport allocations remain
constant (42), v5 raw (55), and v5 LZ4 (59). Validated unit storage is respectively
131,087 and 262,158 bytes independent of total response bytes/segment count.
Raw direct-read pointer checks show no driver payload gather. v5 unit-to-output
copies are measured separately. These memory readers are not socket throughput.

Real TLS uses net.Pipe plus crypto/tls. At 400 KiB receive the benchmark records
exactly 409,600 plaintext output-handoff bytes, roughly 410,219 encrypted wire
bytes, 26 raw reads and 39 plaintext reads per operation. Tiny borrowed write
spans impose a residual cost: 64 KiB as one span uses about four TLS writes and
65,624 wire bytes, while 1,024 spans use 1,024 TLS writes and 88,064 wire bytes.
Pointer checks verify zero driver plaintext gathers, not zero TLS/OS copies.
TLS benchmarks include both client/server standard-library and fixture costs;
setup/handshake is excluded and warmed separately. They cannot establish
end-to-end database performance or isolate every allocation to the client.
Full equivalent Run/Row/NextInto/RunStream comparisons remain step 15.

## Diagnostics and consumption by later steps

The first broad run exposed a newly introduced setup-context/coalescer lifetime
regression; coalescer workers now use the connection lifetime and final checks
pass. An accepted-writer fixture initially failed to join its late peer goroutine;
its explicit join is retained and race-clean. Transitional compile/guard logs
remain exploratory evidence, not final-gate claims.

The query-count failure was diagnosed independently from unrelated transport
changes. Original stochastic repeats passed ten times on the current source and
twenty on the unchanged predecessor, so there is no false claim of stochastic
predecessor reproduction. The exact state-sequence regression deterministically
failed on both with four duplicate queries: caller A decides refreshNeeded,
caller B completes and removes its singleflight, then A enters a new flight.
The callback now rechecks the published table inside the flight. It suppresses
that duplicate while a newer invalidation still issues the required four queries.
Original query-count assertions remain intact; the full final race gate passes.

Committed log copies expand tabs and strip trailing whitespace. Byte-exact raw
originals, hashes and command runners are archived externally as recorded by
`step04-log-formatting.json`; formatting never changes commands/outcomes.

Step 05 owns global host/event publication ordering (including delayed
handleNodeConnected after down/removal), typed topology/control/client routes,
broader discovery-disabled identity handling and full event/control worker
orchestration. Step 04 proves local closed-pool/insertion/generation fences,
not that larger metadata lifecycle. Conn.waitEventDispatch is the explicit
tracked worker join seam: call it after cancellation and releasing consumer
locks, never from its own worker.

Step 07 must compose public RunStream, binding admission, prepared/result metadata
fences, row-plan installation, progress/no-replay and automatic paging/history
with these primitives. Preserve the response-owned page context until exact
completion and merge request/session lifetime into the reservation context.
Its cursor registry must close/join active and idle cursors on session shutdown;
this handoff does not claim all existing session workers are joined. Extract
continuation state before response release, install matching retained columns
for NO_METADATA, release metadata/control ownership on every terminal path,
and keep output independent of input admission.

Steps 06 and 08–17 retain their complete routing/fixtures/migration/extraction,
module/docs, full matrix/performance/review/final-tree requirements. No later
streaming or 174-test release gate is discharged by this transport handoff.
