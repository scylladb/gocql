# Step 05: typed metadata, control and events

Owner: `/root/step05_metadata_control`. Branch: `v2-05-metadata-control`.
Accepted predecessor: `0a05d36bcf5cf0f3c0a195f299f1dde7cdc0beb7`.
The final commands, source hashes, outcomes, fixture ownership and individual
live skips are recorded in `feature-specs/evidence/step05-verification.json`.
Earlier successful and failed attempts are retained as diagnostics, with their
source snapshots distinguished from the final gate.

## Changed behavior

Production topology, control setup, schema agreement and client-route reads use
canonical typed requests/pages/codecs. Internal requests bypass user execution
profiles, retries, speculation and legacy Query/Iter/reflection decoding. The
remaining old query helpers serve the transitional legacy API and test fixtures;
wholesale legacy removal remains step 10. Supported metadata servers provide
`system_schema`; an unavailable capability returns UnsupportedSettingError.
Protocol 3 remains supported independently, including the real Scylla Unix lane.
No Cassandra 2.x metadata compatibility claim is made.

Discovery-disabled contacts each read their actual system.local UUID. Aliases
for the same server deduplicate by UUID, while distinct contacts remain distinct.
Unix bootstrap acquires the actual local identity without publishing its private
TCP placeholder. Explicit refresh respects discovery scope and executes typed
reads. Topology replacement is atomic and epoch fenced. Valid zero-token and
filtered hosts remain represented; filtered hosts receive no query pools.
Malformed tokens/ports fail explicitly. An unknown incomplete peer can be skipped
with bounded diagnostics; an incomplete known peer preserves the complete old
ring and reports PartialRefreshError. Removal tombstones reject stale gossip
until a corresponding new-node epoch permits rediscovery.

Schema publication uses keyspace and table revisions alongside schema-event
clocks. Disjoint table refreshes merge; a stale same-object read cannot undo a
drop or replace newer accepted data. Recoverable failures retain the latest valid
schema, including drops already published. Disabled, minimal, full and keyspace
filters apply to both explicit and event refreshes. Snapshot construction invokes
hooks before its publication gate, rechecks exact source identities/revisions,
and shares unchanged immutable keyspaces. A losing build rebuilds from current
sources, for at most eight attempts. Initialization/explicit refresh reports a
partial failure if no publication can be accepted. This also prevents startup
from returning an older empty snapshot. This publication revision is separate
from prepared dependency stamps; topology and unrelated schema still do not
invalidate prepared statements or retained pages.

Agreement reads each enabled host's own system.local through its available pool
connections at ONE, including down and zero-token hosts. Each bounded parallel
check retains one scope; retries take fresh snapshots. Missing hosts and zero
successful observations prevent agreement. Caller/configured deadlines bound
in-progress reads, and authorization/protocol/validation failures are terminal.
Automatic DDL handling additionally retains the exact successful coordinator's
identity/connection even when subsequent topology excludes it. Agreement/refresh
failure says the DDL succeeded and cannot replay the statement through retry.
Acknowledged success remains visible if the original context is canceled during
agreement. Agreement never refreshes metadata on its own.

Public subscriptions isolate consumers with bounded queues. Overflow discards
queued deltas, preserves the highest discarded generation in one terminal marker,
and closes/removes that subscription. Subscribe before reading a snapshot, then
ignore events at or below its generation; repeat that sequence after overflow.
Publication precedes ordinary event delivery. Sustained publication contention
produces a terminal resync marker instead of an unpublished delta.

Connection control events hand ownership to bounded downstream debouncers/route
work. Charges survive the connection dispatch callback until consumption or
terminal drain, and release exactly once. Debouncers serialize callbacks, invoke
them outside their queue lock, coalesce work and schedule full repair on overflow.
Route event Cartesian expansion is capped at 1,024 pairs; a larger event schedules
a full configured connection scope. Route queue saturation coalesces full repair.
Metadata, control, dispatch, retry, periodic, callback and debouncer workers are
tracked and joined after cancellation; repeated Session.Close joins the same
shutdown. A delayed pool notification rechecks the current host/pool and reconciles
policy callbacks against newer down/removal/replacement state. Broader active
execution/cursor shutdown remains step 07.

ClientRoutesOptions now has ordered ConnectionIDs and a copied HostnameOverrides
map. Selection follows configured order and remains sticky while usable. DNS and
connect failures can try other configured IDs for the same host under one setup
deadline. Known hosts never fall back to their advertised direct address; initial
contact/control bootstrap remains the necessary exception. Route reads/deletions
compare per-pair and connection epochs, retain disjoint changes, and expose partial
failures. Invalidation also advances the host transport epoch under the route
publication gate, closing the late validation/insertion gap. Incompatible user
translation/source-port shard dialing, missing ports and unverified TLS routes
fail explicitly. Logical TLS/SNI identity is retained across proxy endpoints.

## Gate evidence

The final manifest is authoritative for exact commands, exit codes, file hashes,
logs and the independently retained source snapshots.

| Boundary | Deterministic and live evidence |
| --- | --- |
| Identity/topology | Actual distinct contacts, explicit discovery scope, Unix identity, malformed ports/tokens, incomplete known peer preservation, removal/recreate tombstones and atomic ring updates. |
| Schema/publication | Same-object refresh/drop/failure, disjoint table refresh contention, publication crossing a drop, startup snapshot barrier, metadata modes/filters, capability errors, retained snapshots and stale diagnostics. |
| Agreement/DDL | Enabled down/missing hosts, zero successes, excluded exact coordinator, differing versions, disabled hosts, terminal authorization, original-context cancellation and no successful-DDL replay. |
| Events/shutdown | Highest-generation terminal overflow, independent subscribers, bounded serial debouncers, stalled downstream ownership, exact-once consumption/drain, bounded Cartesian fallback, repeated Close and delayed pool notification fencing. |
| Client routes | Typed UUID/text identifiers and canonical list parameters, configured order/stickiness, partial deletion/disjoint epochs, stale empty full reads, DNS/connect alternatives sharing a deadline, copied config, TLS/settings validation and transport insertion invalidation. |
| Final checks | Uncached full unit race, focused repeated race, vet, integration race/CCM builds and focused metadata/event/snapshot allocation checks. |
| Real databases | Task-owned native CCM Scylla 2026.3.4 auth/raw and auth/TLS/Unix lanes, plus Cassandra 5.0.9 v5/LZ4. Existing schema-query, route-table, agreement, event/reconnect and incomplete-peer assertions remain selected. |

The steady Metadata read and isolated event queue benchmarks allocate zero.
Unchanged-schema publication shares its schema graph: one versus 1,000 tables
has the same measured allocation count/bytes. These primitive measurements do
not establish whole-cluster throughput, schema-change cost or final release
performance.

## Diagnostics and remaining plan scope

The initial designated container listeners disappeared before native protocol
probes. Docker/Podman's missing pause process was not reset. Task-owned native
CCM runners instead started, checked, tested, stopped and joined their own nodes
inside one live process, preserving other fixture data. Cassandra's old CCM
Java-8 default, container filesystem links and configuration spelling needed
runner-local adaptations. The actual Cassandra runtime uses Java 17; Java 21's
security-manager startup failure is retained as an exploratory result.

Two real implementation defects surfaced during verification: a keyspace-wide
publication fence rejected independent table work, and a losing startup snapshot
build returned before accepted publication. Both were corrected and covered by
focused race tests; prior failed package/lane partial passes do not count.
Ownership correction exposed an old fixture expectation that assumed release at
connection dispatch. Canceled blocked handoffs now reject before enqueue, while
accepted downstream work retains its charge through consumption/drain.

No assigned step 05 requirement is deferred. Tablet/leader routing remains step
06; public RunStream and complete execution policy/cursor admission remain step
07; reusable fixture/Java aggregate infrastructure remains step 08. The 174-test
mapping, remaining client-route caller migration, extraction, full matrix,
performance and release-wide verification remain their later plan steps. These
focused live lanes do not substitute for that matrix.
