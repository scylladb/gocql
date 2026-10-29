# Metadata, control connection, and events

## Summary

The driver discovers topology and schema through a dedicated control connection,
reacts to server events, publishes coherent routing snapshots, and exposes schema
agreement and host notifications. This document describes the pinned Rust driver
at `f8b0ee928dc0dcb475247dc82cce4e42804b0621`; it does not promise that the proposed
Go API already implements these behaviors. Pool mechanics belong to
[connections](connections.md), replica selection to [routing](routing.md), and
client-route selection/configuration to
[configuration and security](configuration-security-observability.md).

## Language-independent behavior

### Defaults and externally visible controls

| Setting | Default and effect |
| --- | --- |
| Schema fetching | Enabled, full detail. Disabling it leaves no keyspace schema/replication metadata. |
| Full schema fetching | Enabled. Turning only this off retains minimal routing metadata. |
| Keyspace restriction | Empty list means all keyspaces; otherwise schema queries are restricted to the named keyspaces. It does not filter topology. |
| Metadata refresh interval | 60 seconds. Normally batches affected-keyspace schema work; it is not a periodic full refresh by default. |
| Periodic fetch mode | Affected keyspaces. Full-metadata mode makes a full configured fetch due at a periodic deadline, including when events are missing; in-flight full work can defer or consume that tick. |
| Metadata server timeout | 30 seconds; appended when metadata SELECT statements are prepared on ScyllaDB, subject to the prepared-cache boundary below. |
| Metadata client timeout | Explicit override, else configured server timeout plus 1 second with saturating addition, else 30 seconds. The normal default is therefore 31 seconds, including Cassandra despite its lack of server override. It applies separately to each execution page, not to the whole fetch or its initial PREPARE. |
| DNS lookup timeout | 5 seconds per lookup by default; may be disabled. |
| Schema agreement poll interval / deadline | 200 milliseconds / 60 seconds. |
| Automatic agreement and subsequent metadata refresh | Both enabled. Applies when an executed statement returns a schema-change result. |
| Host filter / listener | No filter or listener. Public host listeners require the unstable host-listener feature and unstable build configuration. |

Metadata SELECT executions use LOCAL_QUORUM with no serial consistency. They
use the control connection and dedicated metadata timeouts, independently of
the session's default execution profile. Setting or remapping that profile does
not change their consistency, timeout, retry, load-balancing, or speculation
behavior. Schema-agreement reads have a separate execution path described below.

An explicit metadata refresh always requests a full fetch according to the
schema-fetch configuration. Its successful completion means the resulting
snapshot has been published, not merely that metadata queries have finished.
Already-started full fetches cannot satisfy a newly received explicit request:
the request remains queued until a subsequent full fetch can start. The driver
does not guarantee agreement among the system tables while reading them.
Cancelling a caller after its refresh request has been enqueued does not retract
the request or cancel its fetch; the eventual reply is simply ignored if that
caller is gone. Refresh completion is not a barrier for every server event already
received: events arriving during its full fetch can still require later work, and
coalescing can satisfy several callers through one later published snapshot.

### Discovery and control connection establishment

All addresses returned for each contact-point hostname are considered, alongside
literal socket addresses, and duplicate socket addresses are removed. After any
non-timeout lookup error, the original hostname string is retried with port 9042;
this is not limited to strings lacking a port. Each lookup has its own timeout,
and failure of the fallback returns the original error. A timed-out first lookup
is not retried this way. Hostname resolutions run concurrently; individual failures
are logged and omitted. If no contact point resolves, session creation fails.
Contact points are direct addresses, not input to peer address translation.

Candidates are tried sequentially in a shuffled known-peer order. Opening a
candidate includes event registration, then a full configured metadata fetch.
Server events are drained concurrently even during this first fetch: otherwise
an event burst could fill the connection's event queue and prevent the same
connection from reading metadata responses. Fetch-triggering events observed
during establishment of the retained candidate are carried forward as outstanding
work because the initial read may predate them. A rejected or failed candidate's
fetch plan is discarded; if no connection is retained, recovery performs another
full fetch. Status hints already sent by that candidate remain independently queued.

After initial creation, a known-peer pass yielding neither a retained connection
nor usable metadata causes the original contact points to be resolved again and
retried. Usable metadata from rejected peers ends that pass successfully without
the contact-point fallback. The fallback uses the deduplicated socket-address
sort order, without another shuffle. On the initial attempt there is no second
resolution pass. Opening failures move to the next candidate. After a connection
opens, an initial metadata-fetch failure permits startup with dummy
metadata: known endpoints, generated host IDs, approximately evenly spaced
synthetic tokens, no location/schema/cluster-name information. Previously obtained
valid metadata from a rejected candidate is preferred to dummy metadata. That
candidate connection is discarded, and background recovery starts. If no
connection can be opened, session creation fails rather than using dummy data.

A successfully fetched topology replaces the future control-connection candidate
list, filtered through the host filter, before that topology can be published.
Valid metadata from a rejected peer is retained while other candidates are tried;
if all metadata-producing peers are rejected, metadata may be published without
retaining a control connection. Rejection of every discovered node is logged and
leaves the session unable to execute user queries through those nodes.

There is an implementation limitation: the post-fetch rejection check recognizes
only a discovered-peer endpoint whose address matches the metadata. An initial
contact-point endpoint is not matched by that check, so it can remain the control
connection even if the host filter rejects the corresponding discovered peer.
Host filtering therefore is not a promise that bootstrap/control traffic never
touches rejected hosts.
The rejection check runs when selecting a candidate, not after ordinary full or
partial fetches on an established control connection. A later filter change or
topology result can disable that host's query pool and remove it from future
candidates without moving its existing control connection. That connection stays
until a failure or another control-replacement condition occurs.

### Metadata content and validation

The full fetch reads peers, keyspaces, and (when configured) client routes
concurrently. Topology reads `system.peers` and the local row in `system.local`;
there is no `peers_v2` fallback in this implementation. Each node contains host ID,
broadcast RPC address, the control endpoint's port, optional datacenter/rack, and
tokens. Rows from both tables use the advertised RPC address as a translatable
peer address, including the local row. The connected socket IP is not substituted
for a bad advertised address. The optional cluster name comes from the local row.
Only full fetches replace the stored cluster name; partial topology fetches
discard that field. The public cluster-name accessor returns an empty string
when no name is known, including during dummy-metadata startup.

Invalid topology rows/query-stream errors are logged and skipped, as are rows
with null host IDs. A null token list means zero tokens. If any token text cannot
be parsed as a signed 64-bit token, that node gets a random dummy token instead
of failing the whole fetch. The aggregate peer list is rejected if it is empty or
every peer has an empty token list. Individual zero-token nodes are retained and
exposed in the public node list even though absent from the token ring.
This tolerance includes a failure to prepare or start either topology query and
a failure partway through paging. If the surviving rows satisfy the aggregate
checks, that incomplete peer list is accepted as a replacement; nodes omitted by
the failed read can disappear from the snapshot. Such a read does not itself
schedule a retry or replacement control connection.

Topology validation does not reject duplicate host IDs, socket addresses, or
tokens. When several accepted rows have the same host ID, the public node map
keeps the last processed row's node, while the ring retains tokens and node
references from every row. Conflicting rows can therefore leave ring entries
referring to a different node object/address than lookup by that host ID returns.
The concurrently read peer/local streams do not give such duplicates a stable
winner. Atomic publication does not validate this inconsistent server input.

Schema levels have different contracts:

- Disabled: no keyspaces are fetched; schema events have no schema effect.
- Minimal: keyspace replication strategy, durable writes, tablet/consistency
  flags, table and materialized-view names, view base-table names, and available
  per-table partitioners are fetched. Columns, primary-key definitions, and UDT
  maps remain empty. Prepared routing can use this metadata, but schema-dependent
  public partition-key serialization requires full definitions.
- Full: minimal data plus columns and their types/kinds, ordered partition and
  clustering keys, and recursively resolved UDT definitions. Tables and views
  each expose table schema. Function/aggregate events trigger a keyspace reread,
  but the metadata object does not expose fetched function/aggregate definitions,
  secondary indexes, or all server table options.

Replication maps recognize both qualified and unqualified SimpleStrategy,
NetworkTopologyStrategy, and LocalStrategy names. Simple replication factors and
per-datacenter factors must parse as nonnegative machine-sized integers; missing
required fields or malformed factors fail the fetch. Unknown strategies retain
their name and remaining string options rather than being rejected.

UDTs are dependency-sorted before construction, including references nested in
collections/tuples/vectors. Cycles and syntactically invalid UDT field types are
fetch errors. Missing UDT definitions are a recoverable error at keyspace scope.
Partition/clustering key positions must be contiguous from zero after sorting;
gaps, negative positions, or duplicate positions invalidate a listed table/view
and therefore that keyspace's fetched schema. This does not require a nonempty
partition key: a table/view listed without matching column rows is retained with
empty columns/keys and no partitioner, even at full detail. Column kinds are
partition key, clustering, regular, or static.
For rows that reach their respective validation steps, malformed column type
syntax or unknown kinds fail the fetch. Columns whose type string is exactly
`empty` are omitted before type/kind validation, regardless of their name.
A prior missing-UDT error for the keyspace skips column type/kind parsing and
propagates that recoverable error; a column's own unresolved UDT likewise skips
its kind validation. Recoverable table errors only reach the keyspace if that
table/view is returned by the subsequent name query. Leftover column definitions
and their recoverable errors are discarded when no table/view name matches;
the parser does not require all schema tables to describe the same objects.
Collection frozen state, tuple members, vector
dimensions, and UDT field order are represented. Vector dimensions parse as unsigned
16-bit decimal values, including zero; this metadata parser does not enforce a
positive dimension. UDT field names and parsed field types are zipped, so a length
mismatch truncates to the shorter list rather than producing a length error.

`system_schema.scylla_tables` supplies partitioners. Missing-table/column
compatibility falls back to no per-table override. `system_schema.scylla_keyspaces`
supplies tablet status when `initial_tablets` is non-null. With tablet-routing-v2
negotiated, the driver first also reads `consistency`; if unsupported it retries
without that column. If the table is unsupported, all keyspaces default to
non-tablet/eventual. Only the exact `global` value denotes strong consistency;
unknown, absent, and other values mean eventual. The strong-consistency public
field and its enum require both `scylla_unstable` and
`unstable-strong-consistency`, although internal metadata still records it. Compatibility
detection treats any database `Invalid` error in the relevant request-error shape
as a missing table/column, not only errors proven to have that cause.

Recoverable errors in a single keyspace preserve that keyspace's previous valid
schema. Without an older version, the keyspace is omitted until a later successful
fetch. Other keyspaces can still update. The failed keyspace is scheduled again
at a later schema tick; a newer event (particularly a drop) takes precedence over
that retry. Errors outside this recoverable category fail the containing fetch.
An explicit refresh can therefore succeed while retaining stale keyspace metadata
or omitting a newly discovered malformed keyspace; it does not wait for those
keyspaces' retries. In a partial batch containing both drops and rereads, a
nonrecoverable reread error also prevents publication of that batch's drops;
the entire failed batch is superseded by the scheduled full fetch.
A requested keyspace whose reread returns no keyspace row is removed, even if no
keyspace-drop event was received; absence is distinct from a recoverable error.

### Public metadata inspection

Reading the current snapshot, looking up a keyspace by name or a node by host ID,
and iterating known keyspaces/nodes do not trigger a fetch or wait for freshness.
A missing keyspace or host ID returns absence. Keyspace names are looked up
exactly as stored, without CQL identifier parsing, unquoting, or case folding.
Keyspace iteration and the node list have no guaranteed ordering. All retained
metadata is subject to the configured fetch scope and recovery rules above.

Node enablement and connectivity are separate observations: enabled means a pool
exists, while connected means the pool currently reports at least one connection.
An enabled node can be disconnected, and a disabled node reports disconnected.
Neither observation is a new network probe or a schema-agreement check. Public
token/endpoint inspection, including optional preserialized interop helpers, is
specified in [routing](routing.md#partition-keys-tokens-and-vnode-placement).
Session keyspace switching shares the cluster worker but follows the separate
[connection propagation contract](connections.md#keyspace-propagation-and-failure).

### Server events and scheduling

The control connection registers for topology, status, and schema changes; it
also registers for client-route changes only when a subscriber is configured.
The recognized topology changes are new-node and removed-node events. Schema
targets include keyspace, table, type, function, and aggregate. An unknown schema
change kind becomes an invalid-kind value and is treated as an alteration;
unknown event/target encodings follow the protocol error path described in
[serialization and protocol](serialization-protocol.md).

| Input | Immediate action | Metadata work owed |
| --- | --- | --- |
| Topology change | Record topology work | Full peer-list reread, not a schema reread |
| Status UP | Publish an UP hint | Full peer-list reread, also catching address changes |
| Status DOWN | Publish a DOWN hint | Full peer-list reread |
| Keyspace dropped | Record latest change for that keyspace | Remove it at the schema tick without querying it |
| Other schema change, including dropping an object inside a keyspace | Record alteration for that keyspace | Reread that entire keyspace at the schema tick |
| Client-route change | Union affected connection-ID/host-ID pairs | Query relevant pairs when that fetch slot is free |
| Explicit refresh | Queue/request full work | Full configured fetch |

Status notifications are hints, never authoritative liveness assignments. UP
wakes the pool worker and requests a refill-backoff reset. A needed refill still
uses the refill policy's delay and waits for an already-running refill to finish;
it does not guarantee immediate connection opening. The pool scheduling details
are in [connections](connections.md#filling-excess-connections-and-backoff).
DOWN requests an immediate keepalive probe on current connections; a successful
probe leaves them usable. The hints match known nodes by untranslated broadcast socket address,
not by translated connection endpoint. Unknown addresses are harmless no-ops.
Within unapplied updates, only the latest UP/DOWN hint per address survives.

Only one fetch of each partial type may run at a time. Topology, schema, and
client-route partial fetches may run concurrently with one another. Repeated
topology requests collapse to one outstanding reread; client-route pairs are
deduplicated; only the latest schema change per keyspace is retained. Schema work
waits for the periodic tick, even when it is the only work pending. Ticks advance
without work, avoiding immediate synchronized schema reads across clients. A
schema event arriving while a schema read is running can require another later
tick; this is batching, not a debounce timer restarted on every event.

A due full fetch cancels running partial fetches and subsumes previously recorded
partial work. Events received after that full fetch starts remain owed and are
processed after it completes. Full fetches exclude other fetches in the metadata
worker's scheduler. This cancellation drops partial-fetch futures and prevents
their results from being published; it does not guarantee that their network
requests have stopped. Later pages run in spawned pager tasks that hold the
connection and discover a dropped consumer when sending a page or error, so their
page requests can overlap the full fetch. Already submitted requests are not
cancelled at the server.
Starting a full fetch resets the periodic deadline. The deadline timer is disabled
while a full fetch runs. If another event wakes the loop after the deadline, the
deadline advances even though that full fetch prevents new work; that tick is not
saved for later. Otherwise an overdue deadline can start work after completion.
Deadlines advance from the time they are processed, with no replay of missed ticks.
A duration too large to add to the clock falls back to an immediately due deadline.
With the normal periodic mode and no affected keyspaces, a tick performs no metadata
query; missed events are therefore not repaired by routine ticks. Full mode is the
escape hatch.

The event-draining loop does not directly await metadata fetches or state
publication: it multiplexes events, fetch outcomes, refresh requests, and timers.
Failure of a partial fetch schedules a full fetch. Failure of a full fetch marks
the control connection unusable and enters replacement. Broken connections do
the same and abandon outstanding partial work because replacement fetches all
configured metadata. Repair starts immediately, then no more often than once
per second measured from the previous attempt's start (a slower attempt need not
sleep again). An explicit refresh received during repair wait triggers an early
attempt. A pending explicit refresh survives replacement; a failed replacement
attempt reports its error to that requester, while background repair continues.

### Merging and publishing snapshots

The metadata worker hands updates to the cluster-state worker through one pending
mergeable slot. This bounds queued update objects, not the number of keyspaces,
route pairs, hints, or waiting refresh responses within the object. It avoids
blocking server-event draining while the state worker rebuilds its snapshot.
The newest full metadata replaces older pending full/partial data, preserving
all explicit refresh response handles. Later partial results modify only their
own aspect. A newer broken keyspace result does not overwrite an older valid
pending keyspace. A drop removes it. Different partial aspects merge independently;
the newest topology replaces the whole pending peer list.

Pending absence is not a durable tombstone: a later recoverable error can replace
an unapplied keyspace drop, either in partial merging or in a newer full fetch.
When that error is applied, fallback consults the last published snapshot and can
reuse the pre-drop schema. If the drop was already published, there is no older
keyspace to reuse. Thus drop/recreate/error outcomes can depend on whether the
state worker consumed the drop before the newer result arrived.

Published topology/schema/replica information is an atomically replaced immutable
snapshot. Readers may retain an older snapshot safely. Node objects themselves
reference mutable connection pools, so a retained snapshot is not a frozen view
of connectivity. Partial updates share topology or the schema map when that
aspect is untouched, and retain unaffected keyspace/node objects and pools.
Full refreshes always construct a new topology container, schema map, locator,
and snapshot, even if their values are unchanged; reusable nodes/pools and
keyspaces retained after recoverable errors can still be shared. An unchanged
partial topology update and already-resolved duplicate tablet feedback publish
no new snapshot; repeated feedback with unresolved replicas still rebuilds state.
A schema partial update still constructs a schema map/snapshot; the implementation
does not suppress it merely because values compare equal.

Node identity is keyed by host ID. Unchanged enabled state, address, datacenter,
and rack permit object reuse. For an enabled node whose address alone changes,
the node object is replaced while its pool is retained and notified of the new
address. Datacenter/rack or enabled-state changes can require a new pool/object.
Filtered nodes remain represented without pools and may remain in ring metadata.
The public node list includes all known nodes, including disabled and zero-token
nodes. Host-filter changes are evaluated when topology is processed.
Public `Node::is_scylladb()` is an optional connection-derived observation based
on negotiated extensions; it can be unknown without an available pool. Its
sampling and sharding-independent detection are detailed in
[connection establishment](connections.md#connection-establishment-and-negotiated-properties).
Retained snapshots or node handles also retain their pools, even after removal or
replacement in the current topology. An address-only change updates the shared
pool's endpoint during construction of the new state, before publication; an old
node can therefore still report its old address while sharing a pool already
targeting the new endpoint. Removing a node from the current snapshot does not
itself close every pool retained by older readers.

Client routes are applied before pool construction. DOWN hints target the old
snapshot immediately, while UP hints and route-triggered refills target the new
snapshot when there is one. The state worker waits for pool initialization before
initial publication and, during replacement, only if no enabled node object is
shared with the old topology. The actual initialization iteration covers unique
nodes in the token ring, not every zero-token node. Shared-enabled-object detection
does not prove that that object's pool is currently connected.

Host notifications are emitted during topology processing before atomic
publication and before any required initialization wait. Explicit refresh
success is sent only after publication. A listener cannot assume that reading the
session snapshot inside its callback already returns the announced topology.

### Host listener event contract

Host callbacks run synchronously during initial session construction and,
afterward, on the cluster worker; they can delay construction or subsequent state
work. There is no asynchronous public event queue or durable event history.
Events include host ID and broadcast socket address. Addition reporting requires
the new node to be enabled; removal reporting requires the old node to be enabled.
Address-change candidates require only the old node to be enabled, so an address
change can still be reported when the new address causes the node to be filtered.
Removal and address-change reporting additionally require a recorded listener
status for that host ID; a missing status is logged and the notifications are
skipped.

- Newly discovered enabled node: `Added`, then optimistic `Up`, before waiting
  for its pool. A later failed pool initialization can emit `Down`.
- Removed node: `Down` first if previously connected, then `Removed`.
- Changed socket address: if previously connected, `Down` with old address,
  `AddressChanged` with both addresses and new address in the context, then `Up`
  with new address. An already-unreachable node receives only `AddressChanged`.
  This synthetic sequence does not change the recorded connectivity state.
- Pool connectivity for a host with recorded status: `Lost` changes connected to
  unreachable and emits `Down`;
  `Established` changes unreachable to connected and emits `Up`. Repeated equal
  states are suppressed. Events for unknown/removed host IDs or hosts without a
  recorded status are ignored.

Removed nodes are processed before address changes, then additions; iteration
order among nodes within each group is unspecified. A change only in the internal
address-translation marker does not emit `AddressChanged`. Topology membership
differences are keyed by ID: merely enabling/disabling an existing ID through a
changing filter does not inherently produce `Added`/`Removed` notifications.
In particular, a host first discovered while disabled has no recorded listener
status. Enabling that same known ID later does not create one, so its subsequent
pool-connectivity events are ignored even though its query pool can work.
Later address changes and removal of that enabled host also produce no listener
notifications while that status remains absent. Removal from the topology still
takes effect; a later enabled reappearance after removal is a new addition and
can establish listener status.
Connectivity events carry only a host ID, without a pool identity, generation,
or event-time address. They are interpreted against the current snapshot and
recorded status when consumed. A queued event, or an event from an old pool kept
alive by a retained snapshot, can therefore affect notifications for a replacement
node with the same ID and use that node's current address. There is no check that
the emitting pool is the current node's pool, that the current node is enabled,
or that its state still matches the event at delivery time.

### Client-route and tablet metadata coordination

Client routes are queried only when configured. Full route reads are filtered by
the subscriber's connection IDs. At the internal subscriber/query boundary, an
empty full-read filter means unrestricted; public client-routes configuration
requires a nonempty proxy/connection-ID list, so it cannot select that case.
Partial event handling filters affected pairs by literal membership in the
subscriber list, so an empty subscriber list selects no event pairs. The query
uses connection-ID and host-ID sets and can return their cross product; only
the originally requested relevant pairs are applied. A requested pair missing
from the result deletes that route. Deleting a host's final route removes its
empty route map. Newer updates win per pair and preserve disjoint pairs.
Changed routes can trigger refills using the newly installed route information;
route selection and connection replacement details are specified elsewhere.

Route reads fail as a whole on query, row-decoding, or port-conversion errors;
there is no missing-table compatibility fallback for `system.client_routes`.
Non-null ports must fit an unsigned 16-bit integer, including zero; the metadata
parser permits both ports to be null. Every returned row is decoded and validated
before filtering the cross product, so even a malformed unrequested pair can fail
the partial fetch. A failed route partial escalates to a full fetch, and a failed
route read within a full fetch fails that full fetch; valid topology/schema data
from the same full fetch is not published separately.

Tablet feedback travels separately from control events in a bounded channel of
8192 entries and is drained in batches of up to that size. The state worker
compares each feedback item against the state already updated by earlier items
in the batch. A batch avoids cloning/publication only when every item exactly
matches its stored range, version, ordered replicas, and shards, with all replicas
resolved. A stored tablet with unresolved replicas never counts as a duplicate,
so even identical repeated payloads can rebuild and publish state. New tablet
information may be stored even if its keyspace is not yet known. Unknown replica
host IDs are initially omitted from resolved replicas. Maintenance retries their
resolution and discards a tablet if any replica remains unknown; tablets referring
to removed nodes are also discarded. Topology/schema updates maintain tablet
entries for tablet-based tables and views, remove obsolete information, and replace
references to recreated nodes.
The replica/range/version conflict rules are owned by the routing specification.

### Schema agreement

An agreement check queries each node with working connections concurrently,
reading its local schema version at consistency ONE with an idempotent internal
statement. These are unprepared, unpaged QUERYs sent through ordinary node pools,
including zero-token nodes when their pools have working connections. They bypass
the control connection, metadata SELECT timeouts, and session execution-profile
load balancing, retry, speculation, and request-timeout settings. Connections
within one node are tried in sequence until one succeeds.
If all its connections fail, a non-broken-connection error takes priority over
broken-connection errors. Nodes with only broken connections may be ignored if
some other node provides a schema version. No usable node/only broken connections
is an error, not vacuous agreement. Equal versions from successful nodes establish
agreement; different versions mean agreement has not yet been reached.
The one-shot API returns the agreed schema-version UUID when there is agreement,
absence when versions differ, or an error; the waiting API returns the agreed
UUID or an error.

Each check retains one topology snapshot and takes working-connection handles
from its pools; it does not incorporate nodes added to a newer snapshot while
that check runs. All per-node reads are awaited before results are evaluated,
including when one has already failed. Each retry takes a fresh snapshot.
The one-shot check has no agreement timeout wrapper; the configured agreement
deadline bounds the waiting API, including its in-progress check. Agreement
does not freeze schema changes after the versions have been read.

Automatic agreement after a schema-changing result additionally requires the
statement coordinator to provide a successful version. Its absence or broken
connections cannot be silently ignored. This guards against observing agreement
among other nodes that have not yet learned the coordinator's schema change.
The optional interop waiting API can also require a caller-specified host ID,
using the same checks, retries, and deadline; omitting that ID behaves like the
ordinary manual wait. It requires both `scylla_unstable` and
`unstable-csharp-rs`. Like other manual agreement calls, this API does not refresh
metadata after agreement.

Waiting checks immediately, then sleeps the configured interval after an
unsuccessful/transient result, under the overall agreement deadline. Connection
pool errors, missing required hosts, broken connections, stream exhaustion,
several response parse errors, overload/timeouts, unprepared, rate limiting, and
bootstrapping are retryable here. Serialization/unexpected response failures,
malformed row results, authorization/configuration/syntax/protocol failures,
unavailable/read-or-write-failure/server/unknown database errors, and inconsistent
prepared IDs stop waiting. If the deadline expires and the last completed check
failed, that check's error is returned. If no check completed, or the last completed
check succeeded but found differing versions, a timeout is returned; a later
version mismatch therefore supersedes an earlier error. Consequently this API
does not always return a timeout error when the wait deadline was exhausted.

With automatic waiting enabled, a schema-changing statement can succeed on the
server yet return an automatic-agreement/metadata-refresh error to its caller.
Successful automatic agreement then performs a full metadata refresh by default.
Both steps follow successful core request execution and run outside that
statement's request deadline; the agreement deadline bounds only the waiting
step, not the subsequent refresh. Disabling automatic waiting also bypasses this
refresh even if its separate flag remains enabled. The same handling applies to
an automatic pager's first schema-change response, as described in
[execution](execution.md#results-timestamps-and-response-side-effects).
Manual agreement calls do not themselves refresh metadata.

### Shutdown and observed limitations

Dropping the cluster's worker handles/channels ends background work. The merge
channel delivers a last pending value before reporting that its sender is gone;
cancelled receives do not lose updates. Sending after receiver drop is rejected.
This does not synchronously tear down pools retained through external snapshots
or node handles, or abort already spawned pager and blocking locator tasks.
Unexpected loss of the control connection's failure sender is treated as runtime
shutdown. The source explicitly records a known issue: if an explicit refresh is
pending when this happens, the dropped reply channel can make the refresh caller
panic. This is recorded source behavior, not a desirable Go API contract.

## Rust implementation evidence

All links below are pinned to the inspected revision. Line anchors identify the
start of the relevant implementation; nearby comments are not substituted for
checking the actual branches.

| Contract | Implementation entry point |
| --- | --- |
| Public metadata types and fetch modes | [Keyspace/Table/View/Column](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/cluster/metadata/mod.rs#L224), [PeriodicFetchMode](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/cluster/metadata/mod.rs#L59), [internal schema fetch modes](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/cluster/metadata/mod.rs#L40) |
| Public snapshot and node inspection | [Session::get_cluster_state](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/client/session.rs#L2372), [ClusterState accessors](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/cluster/state.rs#L587), [Node observations and identity](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/cluster/node.rs#L222) |
| Actual defaults | [SessionConfig::new](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/client/session.rs#L469) |
| DNS expansion, default port, deduplication | [node.rs](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/cluster/node.rs#L346) |
| Candidate iteration, fallback, filtering | [establish_cc_and_fetch_metadata](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/cluster/metadata/cc_establisher.rs#L175) |
| Contact-point filter limitation | [is_cc_endpoint_rejected](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/cluster/metadata/cc_establisher.rs#L380) |
| Established control connections are not rechecked against the host filter | [successful full/topology fetch branches](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/cluster/metadata/worker.rs#L784) |
| Event registration and 32-entry server-event queue | [make_control_connection](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/cluster/metadata/cc_establisher.rs#L412) |
| Dummy startup metadata | [Metadata::new_dummy](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/cluster/metadata/mod.rs#L452) |
| Fetch composition and peer validation | [fetching.rs](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/cluster/metadata/fetching.rs#L171) |
| Peer rows, advertised addresses, token fallback | [query_peers / create_peer_from_row](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/cluster/metadata/fetching.rs#L285) |
| Filtered partial schema, absent keyspaces | [fetch_schema_update](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/cluster/metadata/fetching.rs#L665) |
| Full/minimal schema and per-keyspace errors | [query_keyspaces](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/cluster/metadata/fetching.rs#L759) |
| UDT construction and dependency sorting | [query_user_defined_types](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/cluster/metadata/fetching.rs#L942) |
| Columns and primary-key validation | [query_tables_schema](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/cluster/metadata/fetching.rs#L1391) |
| Conditional column validation and unlisted table errors | [column validation order](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/cluster/metadata/fetching.rs#L1419), [table/view name association](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/cluster/metadata/fetching.rs#L1288) |
| Duplicate topology rows are not reconciled | [aggregate peer checks](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/cluster/metadata/fetching.rs#L230), [node-map overwrite and ring accumulation](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/cluster/state.rs#L423), [ring retains sorted entries](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/routing/locator/token_ring.rs#L20) |
| Scylla-specific columns and compatibility fallback | [query_keyspaces_scylladb_specific](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/cluster/metadata/fetching.rs#L1815) |
| Strategy parsing | [strategy_from_string_map](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/cluster/metadata/fetching.rs#L1953) |
| Control query page size, prepared cache, timeouts | [page-size constant](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/cluster/control_connection.rs#L25), [effective client timeout](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/cluster/control_connection.rs#L58), [get_or_prepare_statement](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/cluster/control_connection.rs#L175), [query_iter](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/cluster/control_connection.rs#L201) |
| Per-page metadata execution deadline | [SingleConnectionPagingExecutor::fetch_one_page](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/client/pager.rs#L553) |
| Metadata consistency independent of execution profiles | [Session connection configuration](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/client/session.rs#L1460), [Connection::execute_iter](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/network/connection.rs#L1231), [Consistency default](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla-cql-core/src/frame/types.rs#L17) |
| Metadata EXECUTE recovery from UNPREPARED | [Connection::execute_raw_with_consistency recovery branch](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/network/connection.rs#L1177) |
| Full/partial scheduling and cancellation | [PendingFetches::start_due_fetches](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/cluster/metadata/worker.rs#L349) |
| Paging lifetime after fetch cancellation | [fetch_remaining_pages](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/client/pager.rs#L647), [spawned fixed-connection pager](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/client/pager.rs#L1168) |
| Refresh caller cancellation boundary | [Cluster::refresh_metadata](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/cluster/worker.rs#L203) |
| Establishment event draining | [fetch_on_candidate](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/cluster/metadata/worker.rs#L577) |
| Repair cadence and refresh completion/error handling | [work_without_cc](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/cluster/metadata/worker.rs#L670), [work_on_cc](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/cluster/metadata/worker.rs#L754) |
| Event dispatch | [handle_server_event](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/cluster/metadata/worker.rs#L911) |
| UP-hint delivery, backoff reset, and delayed refill | [trigger_immediate_refill](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/network/connection_pool.rs#L285), [refill notification and scheduling branches](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/network/connection_pool.rs#L753) |
| Update precedence and pending refresh replies | [MetadataUpdate::merge_metadata](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/cluster/metadata/update.rs#L113), [SchemaUpdate::merge](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/cluster/metadata/update.rs#L390) |
| Capacity-one cancel-safe delivery | [Sender::modify](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/cluster/metadata/merge_channel.rs#L102), [Receiver::recv](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/cluster/metadata/merge_channel.rs#L149) |
| State application, hints, routes, listener timing | [ClusterWorker::apply_metadata_update](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/cluster/worker.rs#L418) |
| Listener topology and connectivity ordering | [handle_topology_changes](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/cluster/worker.rs#L565), [handle_connectivity_change_event](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/cluster/worker.rs#L760) |
| Listener status required for removal/address-change reporting | [missing status on removal](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/cluster/worker.rs#L629), [missing status on address change](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/cluster/worker.rs#L667) |
| Retained pool lifetime and host-ID-only connectivity events | [NodeConnectionPool](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/network/connection_pool.rs#L192), [ConnectivityChangeEvent](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/network/connection_pool.rs#L1579) |
| Public host event vocabulary | [HostEventContext](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/policies/host_listener.rs#L21), [HostEvent](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/policies/host_listener.rs#L41), [HostListener](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/policies/host_listener.rs#L90) |
| Snapshot sharing, node reuse, stale keyspaces, tablets maintenance | [full state replacement](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/cluster/state.rs#L254), [partial state changes](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/cluster/state.rs#L304), [node reuse](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/cluster/state.rs#L423), [keyspace fallback](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/cluster/state.rs#L508), [tablet maintenance](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/cluster/state.rs#L536) |
| Client routes fetch and pair filtering | [fetch_client_routes_update](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/cluster/metadata/fetching.rs#L604) |
| Public client-route filter boundary | [ClientRoutesConfig::new and internal subscriber](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/client/client_routes.rs#L76) |
| Tablet batching and duplicate suppression | [worker tablet branch](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/cluster/worker.rs#L303), [with_updated_tablets](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/cluster/state.rs#L846) |
| Tablet duplicate suppression requires resolved replicas | [Tablet::is_same_as](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/routing/locator/tablets.rs#L553) |
| Automatic agreement and refresh | [handle_auto_await_schema_agreement](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/client/session.rs#L1822) |
| Automatic agreement outside core request deadlines | [Session::run_request side effects](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/client/session.rs#L2513), [pager first schema-change response](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/client/pager.rs#L1222) |
| Agreement probes use node pools and raw QUERY | [working connections per node](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/cluster/state.rs#L748), [schema-version statement](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/client/session.rs#L1593), [raw internal queries](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/network/connection.rs#L929) |
| Agreement polling, error taxonomy, required coordinator | [error classification](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/client/session.rs#L2549), [waiting/deadline](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/client/session.rs#L2639), [single check/required host](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/client/session.rs#L2706) |
| Optional caller-required host agreement | [await_schema_agreement_with_required_node_external](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/client/session.rs#L2858) |
| Wire event vocabulary and parsing | [EventV2](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla-cql/src/frame/response/event.rs#L35), [EventV2::deserialize](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla-cql/src/frame/response/event.rs#L171), [SchemaChangeEvent::deserialize](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla-cql/src/frame/response/event.rs#L270) |

Metadata SELECTs are prepared and cached by original query text across control
connection replacements, marked idempotent, and paged at 1024 rows. Each execution
uses a prepared-statement clone with the current client timeout. The fixed-connection
pager applies that timeout afresh to the first page and every later page; the initial
cache-miss PREPARE happens before that timeout is installed. There is no overall
metadata-fetch deadline here. Metadata pages use a fixed connection with a
fallthrough retry policy: normal session retries, failover, and speculation do not
apply to an individual page. The connection layer can still reprepare and resend
EXECUTE once after `UNPREPARED`, within that page's deadline. Other failures follow
the topology tolerance, compatibility fallback, or worker recovery rules above.
The server-side timeout is encoded in whole milliseconds
in the prepared query when prepared, truncating submillisecond precision. Cache
hits do not render that query again or reevaluate the current server family:
the cached statement retains whether its original preparation included
`USING TIMEOUT`. Consequently, switching the control connection between server
families does not add or remove that clause; `UNPREPARED` recovery also reprepares
the cached query text on the new server. Explicit
refresh and keyspace request queues each have capacity 32. Pool-connectivity events use an
unbounded channel. Snapshot publication uses `ArcSwap`; retained `Arc<Node>` pool
handles explain the mutable-connectivity qualification above. Locator rebuilding
is offloaded with `spawn_blocking`. These are implementation mechanisms, not
requirements to copy Rust primitives into Go.

The inspection API exposes borrowed keyspaces, an iterator of borrowed names and
keyspaces, a slice of shared node handles, and optional lookup by host ID.
Rust `Node` equality and hashing use only host ID, even when distinct retained
node objects have different addresses or pools. `ColumnKind::from_str` accepts
exactly `regular`, `static`, `clustering`, and `partition_key`; other strings return
`ColumnKindFromStrError`. The `Peer` type gains `Clone` only with
`scylla_unstable` plus `unstable-python-rs`, for binding adapters; this does not
change the fetch or publication rules.

## Invariants

- Control-event draining remains live during initial fetch, ordinary fetch, and
  control-connection repair fetch; it does not await the state worker.
- Full work excludes partial work in the metadata scheduler; partial concurrency
  is limited to one per type. Cancelled partial results cannot publish, although
  their network requests can outlive cancellation. Events newer than the start
  of a full fetch are not forgotten.
- Explicit refresh success follows publication from a full fetch started after
  that request was accepted; recoverable keyspace errors may retain older data,
  and coalescing preserves all owed replies.
- Failed partial/full fetches cannot publish a wholly failed replacement; a
  recoverable malformed keyspace cannot displace its last known valid version.
- All topology changes update future control candidates before publication.
- Schema filtering does not erase topology; zero-token nodes are not lost from
  the public node collection.
- Status-event hints trigger verification/refill, not unconditional node state.
- Readers see one atomically published topology/schema/locator snapshot, with
  mutable pool state outside snapshot immutability. This does not establish
  consistency of system-table reads or reconcile duplicate topology identities.
- Client-route updates precede construction of pools that depend on them.
- Manual agreement never accepts zero successful schema versions; automatic
  agreement additionally includes a successful coordinator version.

## Subfeatures

The named subsections under Language-independent behavior are the subfeatures of
this specification: configurable fetch scope; bootstrap and control recovery;
topology/schema normalization; public metadata inspection; event scheduling; snapshot merging/publication;
host listeners; client-route/tablet coordination; schema agreement; and shutdown.
Their implementation entry points are mapped individually in the evidence table.
The invariants above constrain their interactions as well as each local behavior.

## Test places

Targets below are relative to the pinned Rust repository. Their existence was
verified and representative assertions inspected; this catalog is not proof of
exhaustive coverage, and no Rust suite run is claimed by this document.

### Rust unit tests

Description: validate fetch-plan/polling logic, merge precedence, schema
normalization, node reuse, and cancellation. Most are deterministic local tests;
the fixed-connection tests below use proxy-backed connection setup.

Test cases:

- Partial-work plans retain different types, and a full-work plan supersedes their pending work: `scylla/src/cluster/metadata/worker.rs:partial_work_of_different_types_coexists,full_fetch_subsumes_partial_work`.
- Completing one partial fetch leaves the other in-flight types intact: `scylla/src/cluster/metadata/worker.rs:completed_partial_fetch_keeps_the_others_in_flight`.
- A pending partial fetch does not hide a completed other type and completed slots empty: `scylla/src/cluster/metadata/worker.rs:pending_partial_fetch_does_not_hide_a_completed_one,completed_fetch_resolves_and_empties`.
- Polling an unfinished scheduler fetch preserves its slot: `scylla/src/cluster/metadata/worker.rs:unfinished_fetch_stays_in_flight`. This does not cover network work after preemption.
- Last schema change wins per keyspace: `scylla/src/cluster/metadata/worker.rs:schema_work_keeps_only_the_last_change_per_keyspace`.
- Pending valid keyspace metadata survives newer errors: `scylla/src/cluster/metadata/update.rs:merging_metadata_reuses_pending_keyspace_on_error,merging_metadata_reuses_pending_partial_keyspace_on_error,merging_a_schema_update_does_not_overwrite_a_keyspace_with_an_error,merging_schema_updates_keeps_the_keyspace_a_newer_error_says_nothing_about`.
- A newer recoverable error replaces a pending absence: the `absent_then_broken` assertion in `scylla/src/cluster/metadata/update.rs:merging_schema_updates_keeps_the_keyspace_a_newer_error_says_nothing_about`. It checks the merged error, not subsequent fallback against published state.
- Route cross-product artifacts and irrelevant IDs are excluded, missing pairs delete: `scylla/src/cluster/metadata/update.rs:update_from_pairs_ignores_cross_product_artifacts_and_irrelevant_conn_ids,update_from_pairs_yields_none_for_missing_route`.
- Merged route updates equal sequential application: `scylla/src/cluster/metadata/update.rs:client_routes_merge_is_equivalent_to_sequential_application`.
- Final update survives sender drop and cancelled receives do not lose it: `scylla/src/cluster/metadata/merge_channel.rs:pending_value_survives_sender_drop,parked_recv_wakes_on_sender_drop,cancelled_recv_does_not_lose_value`.
- UDT dependency ordering across keyspaces and nested collections/tuples, cycle rejection, and sorting despite a missing referenced definition: `scylla/src/cluster/metadata/fetching.rs:test_udt_topo_sort_valid_case,test_udt_topo_sort_detects_cycles,test_udt_topo_sort_ignores_invalid_metadata`. The last test checks only sorting; it does not construct UDTs or prove keyspace-scoped recovery. Vector-nested dependencies are not exercised by these fixtures.
- Exact `global` consistency recognition and eventual fallback, plus valid schema type examples including nested collections/tuples and vectors of dimensions 5 and 1234: `scylla/src/cluster/metadata/fetching.rs:test_consistency_mode_parsing,test_cql_type_parsing`. The type test has no malformed-input or dimension-boundary assertions.
- Unchanged topology/tablet feedback returns no replacement state; carried enabled nodes are detected: `scylla/src/cluster/state.rs:partial_topology_update_is_skipped_when_nothing_changes,tablet_update_is_skipped_when_nothing_changes,shares_enabled_node_with_detects_carried_over_nodes`. These call state constructors/helpers rather than observing atomic publication. The topology test also checks a zero-token addition and topology sharing on a schema-only change; the tablet test checks ordering within a batch.
- Exact tablet duplicate comparison, including replica order and the requirement that all replicas be resolved: `scylla/src/routing/locator/tablets.rs:contains_requires_exact_match`. It checks that an identical unresolved payload is not considered present; it does not observe repeated unresolved feedback through snapshot publication.
- Filter decisions can change across IP updates: `scylla/src/cluster/state.rs:node_included_after_ip_change_not_filtered_anymore,node_filtered_out_after_ip_change,disabled_node_unchanged_attributes_reuses_object`.
- Contact points expand all addresses, deduplicate across sources, and omit failed resolutions: `scylla/src/cluster/node.rs:contact_points_expand_hostname_to_all_addresses,contact_points_dedup_across_sources,contact_points_skip_failed_resolution,contact_points_all_failed_yields_empty`. These inject a resolver and do not exercise the real default-port retry.
- Fixed-connection execution enforces first-page and later-page client deadlines: `scylla/src/network/connection.rs:connection_execute_iter_first_page_timeout,connection_execute_iter_later_page_timeout`. These are proxy-backed tests in a unit-test module, using a paused clock after setup; they do not prove a whole-fetch or PREPARE deadline.
- Fixed-connection server errors are terminal on first and later pages, with execution counts checking no policy retries: `scylla/src/network/connection.rs:connection_execute_iter_server_error`. This proxy-backed test does not cover the connection-layer `UNPREPARED` recovery exception.
- Server-timeout clause presence/absence with simulated ScyllaDB and non-ScyllaDB negotiation: `scylla/src/cluster/control_connection.rs:test_custom_timeouts`. This dry-proxy test awaits captured PREPARE requests for disabled and 2137-millisecond overrides; it does not execute pages or enforce server/client deadlines.
- Builder client-timeout override storage and periodic-mode defaults/selection: `scylla/src/client/session_builder.rs:metadata_request_clientside_timeout,periodic_metadata_fetch_mode`. These inspect configuration fields; they do not exercise effective timeout derivation or fetch scheduling.
- Direct scheduler-start tests for tick gating, cancellation of running partial futures, refresh acceptance during full work, and retry scheduling after keyspace errors: missing. The named worker tests inspect fetch plans and poll manually supplied futures; none calls `start_due_fetches`.
- Coalescing multiple explicit-refresh response handles and observing their replies after publication: missing; the named metadata-merge tests pass no response handles.
- Effective metadata client-timeout derivation/override/saturating-margin assertions, malformed schema types, zero/overflowing vector dimensions, and missing-UDT recovery through state publication: missing in the listed tests. The builder test above checks only override storage.
- Deterministic deadline advancement during a long full fetch, duration overflow, malformed UDT field-list lengths, and absent column rows in full schema: missing.
- Explicit regression for pending-refresh reply loss during runtime shutdown: missing.
- Explicit regression for a filtered bootstrap contact point retained as control connection: missing.
- Explicit regressions for accepted incomplete topology after a query/page failure, malformed route cross-product rows, and mixed schema-drop/reread batch failure: missing.
- Comparing state application of a pending keyspace drop followed by a recoverable error, coalesced versus separately published: missing.
- Refresh caller cancellation after enqueue, pager continuation after metadata preemption, and retained-pool events after node replacement: missing.
- Direct regressions for exact-name metadata lookup, `Node` equality across replacements, optional required-host interop calls, and cached timeout clauses across server-family changes: missing in the listed tests.
- Equal full fetches still constructing and publishing new topology/schema containers, distinguished from node/pool reuse: missing in the listed tests.
- Internal metadata consistency/timeout independence from a changed default execution profile, and agreement probes' separate pool/deadline path: missing in the listed tests.
- Conflicting duplicate topology host IDs across peer/local rows, duplicate token ownership, and node-map versus ring identity after publication: missing in the listed tests.
- Missing-UDT errors masking later column type/kind validation, and recoverable errors in column definitions whose table/view is not listed: missing in the listed tests.

### Rust integration tests

Description: exercise database metadata, event-driven reads, control-channel
progress, schema agreement, and host listener lifecycle with database/proxy/CCM.

Test cases:

- Topology and status events produce two control metadata executions and publish a changed host-filter result: `scylla/tests/integration/metadata/partial_fetch.rs:topology_change_event_triggers_only_a_topology_fetch,status_change_up_event_triggers_only_a_topology_fetch,status_change_down_event_triggers_only_a_topology_fetch`. Events name an unknown address, deliberately avoiding keepalive/refill effects; these tests do not prove the known-host status-hint behavior or an actual address change.
- Known-host DOWN hints issue one pool keepalive per hint on only the addressed host: `scylla/tests/integration/session/status_change_hints.rs:test_status_change_down_triggers_immediate_keepalive`. It uses one connection per host, a 10,000-second keepalive interval, excludes control connections from captured OPTIONS, and injects two sequential hints for each of three hosts. It does not assert post-probe liveness or UP-triggered refill.
- Schema events publish a created keyspace/table with the expected schema-fetch execution count; full mode discovers the table with all events dropped: `scylla/tests/integration/metadata/partial_fetch.rs:schema_change_event_triggers_only_a_schema_fetch,full_metadata_mode_picks_up_schema_changes_without_events`. The session is restricted to one keyspace and automatic post-agreement refresh is disabled. Counts subtract `UNPREPARED` resends; the tests do not decode bound keyspace filters or establish batching at exact tick boundaries.
- Refresh, initial build, and control replacement complete after 100 forged DOWN events arrive during delayed metadata reads: `scylla/tests/integration/metadata/event_flood.rs:metadata_fetch_completes_despite_event_flood,session_build_completes_despite_event_flood,cc_reestablishment_completes_despite_event_flood`. Schema fetching is disabled and the hinted address is unknown. Completion is asserted within 30 seconds after injection; preservation of owed event work and changed metadata content are not asserted.
- Disabled fetching omits the created keyspace; minimal fetching retains its replication strategy/table name and empty columns/keys/UDTs; explicit/empty keyspace restrictions select one/all created keyspaces: `scylla/tests/integration/metadata/configuration.rs:test_turning_off_schema_fetching,test_fetching_minimal_schema_metadata,test_keyspaces_to_fetch`.
- Minimal metadata supports a prepared Murmur3 insert with one traced execution node and exposes the CDC log partitioner: `scylla/tests/integration/metadata/configuration.rs:test_token_awareness_with_minimal_schema_metadata,test_cdc_partitioner_with_minimal_schema_metadata`. The routing test disables tablets; the CDC test is ignored under `cassandra_tests`. Minimal-mode view names/base-table names, durable writes, tablet flags, and consistency modes are not asserted by these tests.
- Full schema exposes nested collection/tuple/UDT definitions, all four column kinds, ordered primary keys, and durable-writes values: `scylla/tests/integration/metadata/contents.rs:test_schema_types_in_metadata,test_user_defined_types_in_metadata,test_column_kinds_in_metadata,test_primary_key_ordering_in_metadata,test_durable_writes_in_metadata`.
- Per-table CDC partitioner and view names/base-table names are checked by `scylla/tests/integration/metadata/contents.rs:test_table_partitioner_in_metadata,test_views_in_schema_info`; both are ignored under `cassandra_tests`. The view test does not assert the view's columns or keys. `test_session_should_have_cluster_metadata` asserts the three configured node addresses and cluster name `TestCluster`.
- Tablet flags are checked by `scylla/tests/integration/metadata/contents.rs:test_tablets_enabled_in_metadata,test_tablets_disabled_in_metadata`. The enabled test is ignored under `cassandra_tests` and returns early when tablets are unsupported; the disabled test adapts its DDL to support detection. These do not cover strong-consistency metadata or force the missing-column fallback.
- Metadata server-timeout text is checked for disabled, explicit, and default settings by `scylla/tests/integration/metadata/configuration.rs:test_custom_metadata_timeouts`. Each captured QUERY/PREPARE is checked according to the connected server, but the captured-message count is only logged, with no nonzero assertion. This is neither client-deadline coverage nor proof of both server-family branches in one run. `test_refresh_metadata_after_schema_agreement` separately checks the newly created UDT's exact definition.
- Forged schema-change/version responses distinguish broken non-coordinators from a required broken coordinator and assert the latter's retained broken-connection error at deadline: `scylla/tests/integration/session/schema_agreement.rs:test_schema_await_with_unreachable_node`. `test_schema_await_required_host_absent` prevents coordinator reconnection and asserts its host ID in the final error. These isolate driver behavior from real schema propagation.
- One injected `Overloaded` failure per node is retried by the waiting API: `scylla/tests/integration/session/schema_agreement.rs:test_schema_await_with_transient_failure`. Its one-shot sanity checks assert an error followed by an error-free result, without asserting `Some(version)` for that second result. `test_schema_await_with_various_apis,test_schema_await_refreshes_metadata` cover six query/execute APIs (unpaged, iterator, single-page), checking `SyntaxError` propagation from agreement and table visibility after DDL respectively.
- Host listener legal per-host transitions and selected status/filter events: `scylla/tests/integration/ccm/host_listener.rs:test_host_listener_with_status_changes,test_host_listener_with_topology_changes,test_host_listener_with_host_filter`. The module requires both `scylla_unstable` and `unstable-host-listener`. Its validator permits some Added-to-Down/Removed transitions; the topology test only validates the observed log and does not assert every commented expected event. The filter test explicitly checks accepted hosts' events and absence of rejected hosts' events.
- Host-listener address changes, optimistic Up before failed initialization, callback timing relative to publication, complete required topology-event sequences, and connectivity events after enabling an initially filtered host: missing in the cited listener tests (address changes and initially down hosts are explicit source TODOs). The state-level filter tests verify enablement/object replacement but do not run the listener status tracking.
- Missing-status suppression of an initially filtered host's later address-change/removal notifications, followed by status creation on enabled readdition: missing in the cited listener tests.
- Agreement mismatch/zero-success outcomes, the full transient/nontransient error taxonomy, and an error followed by a mismatch at deadline: missing in the cited agreement tests.
- Deterministic dropped-keyspace/recreated-keyspace ordering while a full fetch and state rebuild overlap: missing.
- Topology replacement during an agreement check and a blocked one-shot check versus the waiting API's deadline: missing.
- Automatic agreement/refresh extending beyond the original statement deadline, refresh extending beyond the completed agreement deadline, and automatic-wait-disabled/refresh-enabled behavior: missing in the cited agreement tests.
- Continued use of an established control connection after that same host becomes filtered: missing; the partial-fetch filter test deliberately rejects a different host.

The generated book-test module includes schema inspection and schema-agreement
examples from `docs/source/schema/schema.md` and
`docs/source/statements/schema-agreement.md`. These examples demonstrate API use;
they do not verify metadata scheduling, error precedence, or optional interop
behavior. In particular, prose in the book describing periodic schema fetching
or unconditional timeout errors is less precise than the implementation above.

The schema-event test injects a final keyspace-drop event to assert a query-free
removal. Its real-server drop-query-count assertion is commented out because a
server can emit a table-drop event after the keyspace-drop event. The last event
then wins and causes a reread; this catalog does not claim query-free removal for
every real DDL event ordering.

### Go unit/race tests

Description: the new Go implementation must prove publication ordering, channel
shutdown, coalescing, ownership, and listener concurrency without data races.

Test cases:

- Snapshot retention is safe across topology/schema/tablet publication: missing.
- Refresh completion follows publication and cancellation does not lose another caller's response: missing.
- Event bursts cannot block the reader behind a metadata request: missing.
- Schema batching, full-fetch preemption, and latest per-keyspace event ordering match the adopted contract: missing.
- Status hints and connection-derived host transitions remain distinct: missing.

### Go integration tests

Description: migrate existing gocql metadata, control, event-bus, schema agreement,
and tablet integration assertions to the new API. Exact migration targets belong
to the Go migration inventory; old API passes do not establish this gate.

Test cases:

- New API preserves existing control failover and metadata/event assertions against a real database: missing.
- Schema changes through the new API produce fresh metadata and correct agreement errors: missing.
- Host/address/tablet changes converge while queries remain active: missing.

### Go allocation benchmarks

Description: metadata construction is background work; the relevant hot-path
contract is reading the published snapshot and resolving already-known metadata.

Test cases:

- Retaining/reading the current snapshot and metadata lookup require no payload copying: missing.
- Routing reads coexist with repeated publication without hot-path locking or allocation regressions: missing.

## Test generation notes

Use controlled clocks for tick boundaries, refresh arrival during a full fetch,
repair attempts longer than one second, and schema events during pending schema
work. Cover events arriving after the periodic deadline during a long full fetch,
overflowing refresh durations, and separate deadlines for successive metadata pages.
Inject more than 32 control events while metadata responses are withheld.
Test dropped/recreated keyspaces and recoverable errors before and after merging;
retain old snapshots while changing host addresses and filter outcomes. Include
zero-token nodes, null/malformed peer data, unsupported Scylla schema tables,
multiple DNS addresses with duplicates, no resolvable/reachable peers, and route
cross-product rows. Exercise the default-port DNS fallback after non-timeout errors,
including its original-error preservation, and malformed schema with no column rows,
unequal UDT field lists, and zero/overflowing vector dimensions.
Separate a failed topology query with surviving valid peers from a wholly unusable
peer list. Inject malformed route rows outside the requested pairs, a failed
reread batched with a keyspace drop, and `UNPREPARED` during a metadata page; assert
the exact publication/recovery boundary and the unchanged per-page deadline.
Agreement tests must distinguish mismatch, required-host
absence, mixed broken/successful nodes, nontransient errors, and the last completed
check's outcome at deadline, including an error followed by a version mismatch.
Listener tests must assert per-host ordering without imposing an order on
independent hosts or assuming callbacks run after publication. Retain a replaced
node's old pool, then deliver its connectivity event after publication; distinguish
its host ID from the current pool's identity. Cancel a refresh after enqueue and
preempt a partial fetch with an outstanding later page, checking separately that
the discarded result cannot publish and that network work may continue. Coalesce
a keyspace drop with a later recoverable error and compare with publishing the
drop first. Change topology while schema agreement is reading its captured nodes,
and verify that only a subsequent check uses the new membership.
Exercise the optional required-host waiting API with both an absent ID and no
required ID. Keep a control connection open while its host becomes filtered,
and verify separately the query-pool and control-candidate changes. Reuse a
metadata statement cache across control connections with different negotiated
server families and check the retained timeout clause, including reprepare.
Enable a host that was initially filtered out and inspect both its working pool
and the absence of listener status/notifications; then change its address, remove
it, and readd it enabled to check when listener reporting resumes. Compare equal full refreshes
with unchanged partial topology updates to distinguish publication/container
replacement from node/pool reuse.
Remap the session's default profile and capture metadata EXECUTE consistency and
timeouts separately from schema-version QUERYs on node pools. Include a working
zero-token node in agreement checks. Delay automatic agreement past the original
statement deadline, then delay its subsequent refresh past the agreement deadline;
also disable automatic waiting while leaving refresh enabled.
Repeat identical tablet feedback with an unknown replica and distinguish its
new snapshot from the no-publication case after all replicas resolve.
Return conflicting peer/local rows with the same host ID and inspect both the
public node map and every ring entry; include repeated addresses and tokens.
Combine a missing UDT with malformed column types or unknown kinds to check
validation precedence. Omit a table/view name while retaining its column rows,
distinguishing a discarded recoverable key error from a fetch-wide syntax error.
