# Routing, replicas, tablets, and lightweight transactions

## Summary

Routing chooses an ordered sequence of coordinator nodes and shard hints for a
request. It combines statement routing metadata, an immutable cluster snapshot,
replica placement, location preference, connection availability, and optional
latency history. Token and tablet awareness reduce forwarding; deterministic LWT
routing reduces Paxos contention; leader awareness reduces forwarding for globally
consistent tablet keyspaces. These optimizations do not change the request's
consistency or make an otherwise invalid CQL operation valid.

This reference describes `scylladb/scylla-rust-driver` at
`f8b0ee928dc0dcb475247dc82cce4e42804b0621`. It is an extraction of current behavior,
including implementation limitations, rather than a proposed Go API. Connection
opening and pool mechanics belong to [connections](connections.md); publication
of metadata snapshots belongs to [metadata and events](metadata-control-events.md);
attempts, retries, and paging belong to [execution](execution.md).

## Language-independent behavior

### Routing inputs and request lifetime

- Routing input contains the first attempt's regular consistency, optional serial
  consistency, optional token, optional keyspace/table identity, server-confirmed
  conditional-statement flag, and session location preference. The default policy
  needs a token, table identity, and known keyspace replication strategy before
  taking its replica-aware path. Disabling token awareness disables that path,
  including tablet leader promotion.
- A prepared statement is token-aware when preparation supplied at least one
  partition-key bind index. Extract values in partition-key column order, not
  placeholder order. A missing indexed value is an extraction error. The table
  identity is taken from the first prepared column specification.
- Plain-session unpaged and manually paged unprepared requests have no token/table
  in their load-balancing input. Supplying bind values makes execution prepare on
  the chosen connection and can compute a tablet-version hint, but does not
  retroactively rebuild that request's plan or turn it into a token-aware plan.
  Automatic paging differs: an unprepared call with nonempty bind values is fully
  prepared through the session before entering the prepared pager, so token-aware
  planning and the table's partitioner are available. With empty values it keeps
  the token-unaware native QUERY path.
- The caching-session wrapper prepares query strings through the session before
  all execution modes, including calls with empty values, and passes prepared
  statements to execution. Its batches similarly prepare initially unprepared
  members before the batch's routing inputs are extracted. These paths can have
  token/table/LWT metadata at planning time; the plain-session unprepared rules
  above do not apply to them. Cache hits retain the prepared routing metadata and
  partitioner along with the statement identity.
- Missing partition-key metadata is a normal token-unaware case. An actual key
  extraction or hashing error aborts explicit prepared execution, prepared paging,
  and routing extraction from a batch's first prepared statement. In contrast,
  unpaged/manually paged unprepared calls suppress such errors when computing
  their optional tablet hint after local preparation: they use the no-token hint
  and continue to EXECUTE. Value serialization errors are still propagated.
- A batch takes token and table from its first statement only, and only if that
  statement is prepared. Any prepared conditional statement anywhere in the batch
  marks the batch as confirmed LWT. The driver does not combine every batch
  statement's partition key into a multi-partition routing plan. Connection-local
  preparation of initially unprepared batch statements occurs after planning and
  does not retroactively supply token/table metadata or the confirmed-LWT flag.
- Prepared routing metadata is retained from preparation: partition-key indexes,
  table identity, and the confirmed-LWT flag do not refresh when result metadata
  changes. The chosen partitioner is retained too. A later cluster metadata
  refresh or transparent UNPREPARED repair does not revise those inputs in an
  existing prepared statement. Replica placement still uses the execution's
  cluster snapshot, so fresh placement metadata can coexist with older prepared
  routing inputs.
- A plan retains one metadata snapshot and the initial routing input. Retry
  changes to consistency do not recompute it. The retained nodes have live pools:
  connection availability and advertised sharding information may change while
  the plan is consumed. Shared policy latency state can change independently too.
  Snapshot ownership therefore preserves topology/tablet data, not an atomic
  observation of all target eligibility and shard hints. An empty plan ends
  execution without a coordinator; it is not an instruction to ignore the policy
  and choose any known node.
- Automatic paging captures one metadata snapshot for the pager's lifetime. Each
  page constructs a fresh plan against that same snapshot and first tries the
  previous successful coordinator, as detailed in [execution](execution.md).
  Prepared paging also chooses its tablet-version hint once for the whole pager.
  Feedback learned during paging does not update that pager's snapshot or hint.
  Separate manual-page calls each capture a new current snapshot.

The first iterator advance invokes the policy's first-choice method; constructing
the plan alone does not. Speculative fibers share one plan with serialized iterator
advancement, so they consume successive entries instead of independently restarting
the plan. That serialization ends before connection acquisition and execution;
attempts can overlap, and same-target retries do not consume a new entry. It does
not guarantee distinct physical destinations when a policy returns duplicates or
connection selection relaxes a shard hint.

Failure to obtain a connection for a target advances directly to the next target
without consulting the retry policy or issuing a load-balancing success/failure
callback. When an attempt returns an error, its failure callback precedes the
retry decision; ignoring a write error therefore still reports that failed attempt.
Same-target retries reacquire a connection for the same node/shard hint, while next-target
retries consume the existing plan. Within an execution fiber, exhausting a
nonempty plan returns the last connection-selection or attempt error; an initially
empty overall plan yields the distinct empty-plan error. These paths do not refresh
routing metadata or restart the plan.

Callbacks are issued only after an attempt future returns. Client timeout,
cancellation, or losing a speculative race can drop an unfinished attempt without
a success/failure callback, even if the server has processed the request. The
reported elapsed time starts after connection acquisition and includes work
inside the attempt, including local preparation/repreparation and waiting to
enqueue tablet feedback; it is not strictly server execution time.

### Custom policy contract

A custom policy supplies a cheap optional first target and an on-demand fallback
iterator over node/optional-shard pairs. Fallback is requested when first choice
returns nothing, when execution needs another target, or when speculation needs
one. Returning no first choice is therefore different from returning an empty
complete plan. Policies receive the routing input and cluster snapshot and can
observe successful/failed attempts with node and elapsed time; failure callbacks
also receive the attempt error. Shared policies must support concurrent use.
The interface does not automatically wrap a custom policy with the default
policy's host, location, LWT, leader, or latency behavior.

### Default policy configuration

| Setting | Default and effect |
| --- | --- |
| Token awareness | Enabled. Prefer replicas when sufficient routing metadata exists. |
| Location preference | Inherit the session preference; session default is no preference. |
| Explicit policy preference | Overrides session preference: a datacenter, datacenter plus rack, or explicitly no preference. Clearing the override restores inheritance. |
| Datacenter failover | Disabled. With a preferred datacenter, remote nodes are excluded unless enabled. With no preference, this switch has no effect. |
| Replica shuffling | Enabled for ordinary requests. Disabling chooses a random seed once per policy, making replica selection/order repeatable for unchanged candidates during that policy's lifetime. It does not make the order cross-client deterministic. |
| Latency awareness | Disabled. Optional penalties move slow nodes later; upstream cautions that this can hurt workloads and conflict with LWT optimization. |

Datacenter failover is controlled by the location preference and explicit flag;
the current implementation does **not** prohibit it merely because regular or
serial consistency is local. A nonexistent preferred datacenter yields no local
candidates; it does not cause the driver to guess another datacenter. Enabled
failover can still supply remote candidates. A rack preference is a preference
within its datacenter and does not forbid other racks there.

### Default target ordering

For an ordinary request with replica metadata, the preferred groups are:

1. The eligible cached tablet leader, when leader-aware routing applies.
2. Connected replicas in the preferred rack and datacenter, if configured.
3. Connected replicas in the preferred datacenter, if configured.
4. Connected replicas anywhere, if no datacenter is preferred or failover is enabled.
5. Connected nodes in the preferred rack, then connected nodes in the preferred
   datacenter; these need not be replicas. With no preference, all ring nodes form
   the local node group.
6. Connected nodes anywhere, if datacenter failover is enabled.
7. Enabled local nodes, including those currently disconnected, as a last resort.
8. Enabled nodes anywhere, if datacenter failover is enabled.

Groups with no candidates are omitted. In particular, an eligible remote replica
precedes a local non-replica. Replica groups are shuffled for ordinary requests.
Connected non-replica node groups are cyclic traversals with a newly random
starting offset, not a persistent counter or a full random permutation. The fast
node choice uses that rotation even for its final enabled-node stages; the first
acceptable node after the offset need not be uniformly distributed over a sparse
filtered subset. The full fallback's final enabled-node groups instead retain the
locator's stored unique-node order, which follows first occurrence from the
lowest token in the corresponding ring.

The fast first-choice operation follows these priorities without materializing
the full fallback plan. The full default fallback removes repeated destinations:
two entries with explicit shards match only if host ID and shard match; if either
entry has no shard, matching host ID suffices. Distinct explicit shards on one
host can remain separate targets. The plan wrapper additionally removes the
initially chosen raw target from the fallback using exact node/optional-shard
equality; it does not provide general duplicate elimination for custom policies.

The default liveness predicate is whether the node has a connected pool; it does
not inspect the requested shard. A disconnected but enabled node can be returned
at the end. A host excluded by the host filter is disabled and is not reintroduced
by this last-resort behavior. The node groups come from the locator's token ring,
not blindly from every topology entry.

These eligibility checks do not all happen at one instant. On fallback creation,
ordinary replica lists are filtered and collected for shuffling, whereas LWT
replica and node-group filters normally run as their iterators advance. When
latency awareness has a global minimum available, it consumes the whole fallback
immediately to partition targets; without a minimum it passes the iterator through.
A target can lose its connection after any of these checks, and a completed
latency partition is not reordered again as statistics change.

### Partition keys, tokens, and vnode placement

- Tokens use signed 64-bit ordering. Construction from a computed hash normalizes
  the minimum signed value to the maximum signed value. The minimum is reserved
  as a special boundary; the CDC short-input behavior below is an exception.
- Supported partitioners are Murmur3 and CDC. Table partitioner names are matched
  by their suffix. Missing or unrecognized names fall back to Murmur3 when
  preparing through the session or computing a token from table metadata.
  Connection-local preparation retains the Murmur3 default; see the implementation
  constraints below for its effect on unprepared EXECUTE hints.
- Murmur3 reproduces Cassandra's signed-byte tail behavior, including inputs with
  bytes above 127. Its incremental result is independent of chunk boundaries.
  Hashing can stream encoded key fragments without allocating one concatenated
  partition-key buffer.
- The public low-level hashers also work without a session or schema metadata.
  They consume the supplied bytes directly: successive writes concatenate bytes,
  without inserting component lengths or terminators. Callers must supply the
  correct partition-key encoding. Finalization reads the current hash without
  consuming or resetting it; repeated finalization is allowed, and later writes
  extend the same stream.
- CDC consumes the first eight encoded bytes as a big-endian signed integer,
  applies normal token normalization, and ignores subsequent bytes. Fewer than
  eight bytes produce the reserved minimum token without normalization.
- A single effective prepared partition-key component contributes its raw bytes.
  Multiple components contribute, in column order, a two-byte unsigned big-endian
  length, the bytes, and a zero terminator for each component. A composite
  component longer than 65,535 bytes is an error. Current extraction skips null
  and unset components; it does not independently reject those invalid partition
  key values. With no effective bytes, it hashes empty input. This must not be
  mistaken for a guarantee that the database accepts the values.
- The sorted vnode ring starts a lookup at an exact token match or the next
  greater token, wraps after the maximum, and visits each ring entry at most once.
  An empty ring has no endpoint.
- SimpleStrategy takes the first distinct nodes in ring order, up to the
  replication factor or number of unique nodes, whichever is smaller. A zero
  factor has no replicas. Datacenter filtering is applied to this global replica
  set; it does not recompute a datacenter-local SimpleStrategy replica set.
- NetworkTopologyStrategy uses the configured factor per datacenter and a ring
  restricted to that datacenter. It excludes duplicate nodes, accounts for rack
  diversity, and caps results by available unique nodes. A missing rack value
  counts as its own rack. Its traversal accepts a previously unused rack or an
  already-used rack while the budget `max(replication_factor - rack_count, 0)`
  permits repeats; it is not simply "visit every rack before accepting repeats."
  Unconfigured or unknown datacenters have no replicas.
- LocalStrategy and unknown strategies use the locator's SimpleStrategy factor-one
  fallback; the latter is a compatibility fallback, not support for arbitrary
  server placement algorithms.
- Replica precomputation must agree with on-demand lookup. SimpleStrategy lists
  use prefixes of the maximum configured factor. Per-datacenter NTS prefix reuse
  is valid only through rack count; factors above rack count have their own lists.
  Tablet keyspaces do not drive vnode precomputation. Ordinary multi-datacenter
  NTS iteration may group datacenters rather than follow global ring order.
- Public token/endpoints inspection does not require executing a request. The
  ordinary token-calculation API requires base-table metadata and serializes
  exactly the partition-key values against its column specifications. It searches
  the keyspace's tables map, not its separate materialized-view map; an absent
  entry produces an unknown-table error. The checked endpoint lookup with a
  supplied token requires only keyspace metadata and distinguishes absence from
  an empty replica set; its deprecated counterpart collapses that error into an
  empty result. This endpoint lookup does not independently validate table
  existence, so it can also inspect cached tablet replicas for a view.
- Prepared statements separately expose encoded partition-key materialization and
  token calculation. Materialization serializes the bound row, extracts the key,
  and returns owned encoded bytes; token calculation streams that same encoding
  into the statement's partitioner. With no partition-key indexes, materialization
  returns empty bytes whereas token calculation returns no token, provided row
  serialization succeeds. Both propagate serialization/extraction errors and
  oversized composite-component errors.

### Direct replica and ring inspection

The cluster snapshot exposes its replica locator independently of the configured
load-balancing policy. Callers can supply a token, replication strategy, table
identity, and optional datacenter to obtain a replica set. This lower-level call
does not look up keyspace metadata or validate table existence; a cached tablet
entry still overrides the supplied vnode strategy. Results describe placement
without applying connected-host, host-filter, latency, or session-location
selection rules, so a replica in the result is not necessarily usable for a
request. The partition-key endpoint convenience API computes a token first and
therefore inherits ordinary table-metadata and serialization requirements.

Replica sets expose their length, emptiness, ordinary iteration, ordered
iteration, and random selection with a caller-supplied random generator and
predicate. Random selection first probes an indexed random replica; if that
replica fails the predicate, it selects from the filtered full iterator. It
returns no target for an empty set or no matching replica. Ordinary global NTS
iteration groups datacenters in their first-occurrence order on the global ring;
ordered iteration restores global ring order as described under LWT routing.
The ordinary replica iterator can be cloned and skipped with `nth`; skipping
beyond its remaining elements exhausts it. The ordered iterator is not cloneable.

The locator also exposes the full vnode ring, distinct global/per-datacenter
node lists, and datacenter names. Ring iteration from the start includes vnode
tokens; token-relative traversal can include or omit those tokens and visits
each entry once. A ring length counts vnode entries, not unique hosts. Nodes with
no datacenter remain in the global ring but do not create a named datacenter or
appear in a datacenter ring.

### LWT routing

A request is routed as LWT if either preparation confirmed an LWT or its **regular**
consistency is SERIAL or LOCAL_SERIAL. This includes serial reads whose CQL has
no conditional clause. Setting only the optional serial consistency does not
identify an ordinary request as LWT. Server-confirmed preparation flags rely on
the negotiated Scylla LWT metadata extension and are absent on Cassandra.

For LWT requests, eligible replicas retain deterministic global ring order for
vnode placement or server-provided replica order for tablet placement. Location
buckets and filters still apply, so deterministic means a shared ordering rule
for the same metadata/preferences and eligible nodes. Ordinary random shuffling
is bypassed. Non-replica fallback groups retain their ordinary behavior; this is
not a promise that every possible retry target is deterministic.

When choosing across all datacenters, the fast LWT path inspects the first ordered
replica only. If it exists but fails the predicate, the operation returns no fast
choice and deliberately delegates to the full fallback; it does not skip to a
random node or claim the plan is empty. The full fallback computes and filters
the remaining ordered replicas. Datacenter/rack-scoped selection can cheaply find
the first matching replica directly. Global NTS ordering computes the primary
without allocation and materializes the remaining ordered replica set only when
needed. Explicit latency penalties may move otherwise preferred LWT replicas
later; LWT routing is not insulated from that configuration.

### Tablet placement and feedback

- A table recognized as tablet-based uses cached tablet node/shard pairs instead
  of the vnode replication strategy. An unknown owning range returns an empty
  replica set so the policy uses non-replica fallback and can learn from EXECUTE
  responses.
  The cache includes empty entries for tablet tables and materialized views;
  absence of learned ranges must not accidentally invoke vnode routing.
- A server range is left-open/right-closed `(first, last]` and must satisfy
  `first < last`; empty, reversed, or wrapping ranges are rejected. Internally the
  range is inclusive `[first + 1, last]`. A negative shard ID is rejected. Every
  nonnegative signed 32-bit payload shard is representable; parsing does not
  compare it with the node's advertised shard count. An out-of-range hint is
  handled by connection selection, as described below.
  The cache preserves supplied replica order and explicit shards. Repeated hosts
  on different shards are representable.
- Errors in declared replica entries are all-or-nothing: one entry that fails
  deserialization or contains a negative shard rejects the entire feedback
  payload, rather than retaining its valid prefix. An empty replica list is
  accepted and can replace an existing mapping; it yields no replica or leader
  target, so normal non-replica fallback applies.
  This differs from valid host IDs absent from the topology, which permit the
  partial resolution described below.
- The generic deserializers impose weaker byte-consumption checks than a strict
  payload validator. A null replica list is accepted as empty; in V1, omitting
  the final replica-list field entirely has the same effect. V2 still requires
  its non-null version field. Trailing bytes after the expected outer tuple or
  replica tuple fields, and bytes beyond a replica list's declared element count,
  are ignored. Thus an undeclared malformed tail need not reject feedback, and
  an accepted null/omitted list can replace usable cached replicas with an empty
  mapping. These are implementation quirks, not recommended server encodings.
- Receiving a range removes **all** cached ranges that overlap it, then inserts
  the new range. Old non-overlapping fragments of replaced ranges are not retained.
  Adjacent non-overlapping tablets survive. This supports split/merge feedback
  without allowing overlapping cached ranges.
- The V1 extension supplies `tablets-routing-v1` feedback when a request contacts
  a shard that does not own the relevant tablet. It can leave an added replica
  undiscovered if requests continue reaching still-valid old replicas.
- Experimental V2 is negotiated as `TABLETS_ROUTING_V2_EXPERIMENTAL` and takes
  precedence over V1 when both are offered. Its `tablets-routing-v2` tuple adds an
  opaque 64-bit version to the range and replicas. Versions are hashes, not
  monotonic counters, so numeric greater-than comparisons are invalid.
- On a V2 connection every EXECUTE includes a one-byte version block. For a
  known version, select a random nibble index 0–15, put the index in the high
  nibble and that version nibble in the low nibble, numbering from the least
  significant nibble. A cache miss with table/token uses a random byte. With no
  table/token, use zero. The connection's negotiated feature controls whether
  the byte is written, independently of how the hint was computed. A request can
  reuse its chosen hint on retries; the random probing guarantee is across
  successive requests, not a required resample on every wire attempt.
- V2 detects changes probabilistically through disagreement with the server's
  version; there is no fixed number of requests guaranteeing refresh. Feedback
  parsing prefers the V2 payload if both keys appear; malformed V2 is not retried
  as V1. Malformed tablet feedback is logged and does not replace an otherwise
  valid query result with a tablet parse error.
- EXECUTE responses, including the response after automatic repreparation,
  forward feedback when prepared table identity is available. This happens before
  interpreting a database error, including the initial UNPREPARED response, so a
  failed query can still update the cache. A transport/body decoding failure that
  prevents a response from being returned cannot reach this feedback step.
  Feedback is sent through a bounded channel of 8,192 entries with an awaited
  send, so a full channel can delay return from execution. A closed receiver is
  ignored by the sender. This is not an unbounded, nonblocking telemetry queue.
  Enqueueing does not wait for snapshot publication. Cancellation while the send
  is pending can discard that feedback; once enqueued, it can still be applied
  after the request times out or loses a speculative race. Thus neither a wire
  response nor a successful query return guarantees that a subsequent snapshot
  already contains its feedback.
- Native QUERY and BATCH frames carry no tablet-version block, and their response
  paths do not forward tablet feedback to the cache. A batch can use tablet routing
  learned by other EXECUTE requests, but does not itself warm or repair that cache.
  An unpaged/manually paged unprepared call with bind values uses connection-local
  preparation and EXECUTE, so it can forward feedback despite retaining its
  token-unaware plan. Automatic paging with values uses the prepared path above.
- The cluster worker batches available feedback and publishes an updated cluster
  snapshot. Identical range/version/ordered node-shard data is a no-op only when
  all replicas are resolved. Updates within a batch are compared with the state
  produced by earlier entries in the same batch. A repeated unresolved mapping
  can still trigger work. No version ordering rejects older in-flight feedback.
  Metadata and tablet updates are applied serially by the same cluster worker,
  but feedback carries table names, not a schema generation or table ID. Late
  feedback can recreate a removed cache entry after schema maintenance or apply
  to a newly recreated table with the same name; maintenance is reconciliation,
  not a fence against such feedback.
- Unknown host IDs are omitted from usable replicas while the original list is
  retained for repair. At maintenance, all unresolved replicas are resolved again;
  if any remain missing, the whole problematic tablet is discarded. Tablets that
  refer to removed nodes are discarded. Recreated node objects replace old ones,
  and datacenter indexes are rebuilt if needed. Dropped/non-tablet tables and
  keyspaces lose their cached entries; tablet views are retained and initialized
  like tables.
- Cache data may be learned before its keyspace appears in fetched metadata.
  Feedback insertion does not require that keyspace to exist; the next metadata
  maintenance reconciles the cache. Updates use structural sharing and copy only
  changed table storage; they do not mutate snapshots held by in-flight requests.

### Leader-aware routing

When the keyspace metadata says global strong consistency, the request has
token/table metadata, and regular consistency is neither ONE nor LOCAL_ONE, the
policy promotes the known tablet leader ahead of the location buckets. This rule
uses consistency, not parsing of read versus write CQL. The server remains
responsible for rejecting consistency levels invalid for that operation.

The leader must be connected and eligible under the datacenter restriction. A
remote leader outranks a local-rack replica only if remote nodes are permitted.
The fast choice also rejects a latency-penalized leader. The full fallback first
promotes an eligible connected leader, then applies the stable latency partition
to the entire plan: a penalized leader moves behind unpenalized targets but stays
first among penalized targets. If the leader is unknown, disconnected, excluded,
or prohibited by the datacenter restriction, normal replica routing remains
available. The remaining replicas retain ordinary or LWT ordering before latency
partitioning.

The leader is the first **original** payload replica. If that host could not be
resolved, the first surviving follower must not be mistaken for the leader. A
missing follower does not hide a successfully resolved leader. V2 is the documented
source of leader-ordered feedback. The current `known_leader` implementation does
not additionally check that a cached mapping has a V2 version; it interprets the
first cached replica when called for a globally consistent table. Do not describe
an explicit V2-only guard that the source does not contain.

### Shard hints and explicit targeting

- For vnode replicas with sharding metadata, bias the signed token into unsigned
  order, shift away the configured most-significant bits, multiply by shard count,
  and take the high 64 bits. Without a sharder, the computed replica shard is zero.
  Tablet replica shards come directly from feedback rather than this formula.
- An unspecified shard from a policy is filled by a uniform random shard within
  the node's advertised count; without shard information it is zero. The built-in
  plan's random source is not configurable. A custom policy can avoid this step
  by always supplying a shard.
- A shard is a hint: an out-of-range shard or empty shard pool may fall back to
  another available connection on the same node. An explicit node policy does
  not make the shard hint a strict pin. The connection spec owns the fallback
  mechanics and exhaustion errors.
- Single-target policy accepts a node object, host ID, or address and an optional
  shard. A supplied node object is used directly; host ID/address are resolved in
  the snapshot. Lookup failure yields no target, and this policy has no fallback
  hosts. It does not apply the default policy's locality/liveness filters itself.
  Address matching is unreliable under translation because a node's stored address
  can change between translated/contact-point and advertised forms as control
  connections change; host ID is the stable lookup form.
- Shard-aware source ports map by `source_port mod shard_count`. The default
  candidate range is inclusive 49,152–65,535; custom ranges must be nonempty and
  within 1,024–65,535. Candidate iteration begins randomly and wraps through each
  compatible port once. A valid range can have no port for a given shard, even the
  default range when the shard count is large. The iterator is then empty; the
  public single-port draw helper panics instead. Source-port helpers also assert
  that the requested shard is below the advertised count.
  The advertised count is a nonzero 16-bit number (1–65,535). Parsing rejects
  missing/empty shard parameters, zero shard count, parse errors, and shard IDs
  outside the advertised count. See connections for handling these errors during
  startup.

### Latency awareness and host filtering

Latency statistics are per host ID, shared by all requests using the policy, not
per shard. Successful attempts and designated failure classes update an average;
zero-duration measurements do not change it. The previous average's weight is
`ln(1+d)/d`, where `d` is elapsed time divided by scale, and is one when elapsed
time is nonpositive. Invalid floating-point duration conversion keeps the previous
average value while advancing its timestamp and measurement count. Defaults are
threshold 2, minimum 50 measurements, scale 100 ms, global-minimum refresh interval
100 ms, and retry period 10 s. A zero scale is rejected by a panic in the Rust builder.
The measurement minimum and retry period accept zero: a zero minimum still needs
an actual nonzero-duration sample to create an average, and a zero retry period
prevents penalties. The exclusion threshold has no positivity or finiteness check;
the comparison below uses the supplied floating-point value directly.
The periodic minimum updater belongs to the policy instance, so sharing one
policy shares its samples and updater across sessions/profiles as well as requests.

A host is penalized only when it has enough measurements, its last measurement is
younger than the retry period, and its average is **strictly greater** than the
threshold times the last computed global minimum. Comparisons use microsecond
precision. Unknown hosts or absent minimum data are eligible. Periodic minimum
calculation considers hosts with enough measurements and does not itself expire
old averages. Penalty expiration gives the host another chance without deleting
its history. Statistics are keyed by host ID; the implementation has no explicit
"reset on node restart" hook despite that claim in a builder comment.

The fast connected-candidate stages reject penalized hosts. If all such choices
fail, the final enabled-node stages can still select one. The fallback stably
partitions the entire default plan into unpenalized and penalized targets, so
penalties may move a replica behind a non-replica or a preferred-local host behind
a permitted remote host. They never admit a host excluded from the underlying
plan.

Driver-side serialization failures, broken connections, stream-ID exhaustion,
and fast server errors (bootstrapping, unavailable, unprepared, overloaded, and
rate limiting) do not update latency. Result/error/body-extension parse errors,
unexpected responses, repreparation identity errors, unfinished-page errors, and
the enumerated remaining server errors do. Unrecognized numeric server error
codes become `DbError::Other` and do update latency; only additional enum variants
not covered by the explicit match are excluded. This classification is an explicit
policy, not measurement of whether each failure physically involved a replica
round-trip.

Host filtering controls whether a node gets pools and is distinct from locality
preference. Built-ins accept all peers, accept one datacenter, or accept resolved
allowlisted socket addresses. The allowlist accepts untranslatable addresses
(contact points or already-accepted translated nodes); it compares translatable
peer addresses against the configured set. A custom filter can disable every
usable node and thereby produce empty default plans.

## Subfeatures

The language-independent sections above define the named subfeatures: routing
input extraction, custom policy planning, location-aware default planning,
partition-key hashing, vnode replica placement and precomputation, deterministic
LWT routing, tablet placement/feedback/version tracking, leader-aware routing,
shard hints/source-port mapping, single-target policy, latency awareness, host
filtering, and endpoint/replica/ring inspection. The implementation table below maps each to
its source entry points; these remain one routing feature section for review.

## Invariants

- The default policy falls through to its eligible coordinator groups when routing
  information is missing; it does not manufacture a replica mapping or bypass its
  host/datacenter exclusions. Custom policies define their own eligibility rules.
- Tablet tables with unknown ranges must not be treated as vnode tables.
- The default policy recognizes an LWT's regular SERIAL/LOCAL_SERIAL consistency
  and prepared confirmation; optional serial consistency alone is insufficient.
- For complete valid keys and the correct partitioner, token calculation and
  encoded partition-key order agree with server placement, including chunked
  hashing and signed-byte Murmur3 compatibility. The exceptional paths below
  must not be interpreted as providing additional key or partitioner validation.
- The first-choice optimization may defer work, but fallback must still be called
  when no cheap first target is available.
- Same-host explicit different-shard targets remain distinguishable; shard hints
  can be relaxed only through the connection selection contract.
- Tablet snapshots contain sorted non-overlapping ranges. Repeated equivalent
  resolved feedback does not publish a new snapshot.
- Unknown original leaders must never be replaced by the first resolved follower.
- Retry consistency changes do not silently rebuild the initial plan.
- Concurrent speculative attempts share serialized plan advancement. Retained
  metadata snapshots remain valid while node pools and policy latency data evolve.
- Tablet feedback enqueue and snapshot publication are separate events;
  cancellation supplies no publication acknowledgement or rollback.
- Default-policy penalties reorder eligible targets; they do not authorize new
  destinations or change CQL consistency semantics.

## Rust implementation evidence

All links are pinned to the source revision above. They identify implementations,
not merely public-facing documentation.

| Contract | Source entry points |
| --- | --- |
| Routing input, LWT classification, custom policy callbacks | [RoutingInfo](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/policies/load_balancing/mod.rs#L28), [LWT classification](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/policies/load_balancing/mod.rs#L113), [LoadBalancingPolicy](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/policies/load_balancing/mod.rs#L180) |
| Lazy first/fallback state machine and random shard fill | [Plan::new](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/policies/load_balancing/plan.rs#L81), [Plan::next and shard filling](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/policies/load_balancing/plan.rs#L94) |
| Shared speculative iterator and cancellation boundaries | [SharedPlan](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/client/execution.rs#L101), [timeout and attempt callbacks](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/client/execution.rs#L445), [speculative completion](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/policies/speculative_execution.rs#L187) |
| Pager snapshot, fresh per-page plans, and reused tablet hint | [PagingExecutor](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/client/pager.rs#L134), [per-page plan](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/client/pager.rs#L340), [pager hint](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/client/pager.rs#L997) |
| Connection-selection failure, callbacks, retry targets, and plan exhaustion | [Request execution fiber](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/client/execution.rs#L547) |
| Default first-choice order | [DefaultPolicy::pick](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/policies/load_balancing/default.rs#L146) |
| Fallback order, deduplication, latency wrapping | [DefaultPolicy::fallback](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/policies/load_balancing/default.rs#L333) |
| Live pool observations and fallback evaluation timing | [pool liveness and sharder](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/network/connection_pool.rs#L266), [replica filters and shuffling](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/policies/load_balancing/default.rs#L703), [latency partition](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/policies/load_balancing/default.rs#L3489) |
| Deterministic versus shuffled replicas and random node rotations | [Replica selection helpers](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/policies/load_balancing/default.rs#L703) |
| Leader eligibility and unrestricted explicit DC failover | [Leader selection and failover](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/policies/load_balancing/default.rs#L941) |
| Defaults, preference override, fixed random seed | [DefaultPolicyBuilder::new and build](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/policies/load_balancing/default.rs#L1050) |
| Latency updates, error classification, penalties | [Latency-awareness module](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/policies/load_balancing/default.rs#L3270) |
| Explicit single target and address limitation | [NodeIdentifier](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/policies/load_balancing/single_target.rs#L16), [SingleTargetLoadBalancingPolicy::pick](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/policies/load_balancing/single_target.rs#L72) |
| Tokens, partitioners, stream hashing | [Token::new](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/routing/mod.rs#L39), [partitioner dispatch](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/routing/partitioner.rs#L36), [public streaming traits](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/routing/partitioner.rs#L107) |
| Prepared partition-key extraction and encoding | [Prepared metadata and token API](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/statement/prepared.rs#L310), [PartitionKey extraction and encoding](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/statement/prepared.rs#L791) |
| Cached preparation before routing and retained prepared inputs | [Caching-session dispatch](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/client/caching_session.rs#L100), [cache-hit reconstruction](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/client/caching_session.rs#L200), [prepared shared data and partitioner](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/statement/prepared.rs#L205), [result-only reprepare update](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/network/connection.rs#L773) |
| Batch first-statement routing, whole-batch LWT marking, and absence of tablet feedback handling | [Session::batch routing construction](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/client/session.rs#L1327), [Connection::batch_with_consistency](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/network/connection.rs#L1252) |
| Unprepared planning, local hint errors, and automatic-paging preparation | [Unprepared request execution](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/client/session.rs#L1681), [Automatic paging dispatch](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/client/session.rs#L1841), [Prepared pager routing](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/client/pager.rs#L974), [Prepared execution key errors](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/client/session.rs#L2095) |
| Ring lookup, replication strategy calculation | [TokenRing::ring_range_full](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/routing/locator/token_ring.rs#L35), [SimpleStrategy and NTS calculation](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/routing/locator/replication_info.rs#L112) |
| Replica strategy dispatch, tablet override, ordered NTS | [ReplicaLocator](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/routing/locator/mod.rs#L114), [ordered NTS iterator](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/routing/locator/mod.rs#L837) |
| Precomputation, including above-rack-count factors | [PrecomputedReplicas::compute](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/routing/locator/precomputed_replicas.rs#L80) |
| Shard calculation, source ports, SUPPORTED parsing | [port-range validation](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/routing/sharding.rs#L29), [shard arithmetic and source ports](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/routing/sharding.rs#L121), [SUPPORTED parsing](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/routing/sharding.rs#L276) |
| Tablet payload/version parser and block encoding | [TabletVersion block encoding](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/routing/locator/tablets.rs#L208), [RawTablet payload parsing](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/routing/locator/tablets.rs#L242) |
| Tablet null/missing lists and ignored trailing bytes | [ListlikeIterator deserialization](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla-cql-core/src/deserialize/value.rs#L1010), [tuple deserialization](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla-cql-core/src/deserialize/value.rs#L1729), [fixed-length list iteration](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla-cql-core/src/deserialize/value.rs#L2094) |
| Leader resolution, range replacement, cache maintenance | [Tablet::known_leader](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/routing/locator/tablets.rs#L498), [TableTablets replacement and maintenance](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/routing/locator/tablets.rs#L780), [TabletsInfo::perform_maintenance](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/routing/locator/tablets.rs#L986) |
| Extension negotiation and per-connection hint emission | [ProtocolFeatures::add_startup_options](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla-cql-core/src/frame/protocol_features.rs#L111), [choose_tablet_block_hint](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/client/execution.rs#L73), [connection EXECUTE](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/network/connection.rs#L1105), [EXECUTE serialization](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla-cql/src/frame/request/execute.rs#L81) |
| Feedback transport, batching, snapshot deduplication | [Feedback sender](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/network/connection.rs#L2045), [cluster worker](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/cluster/worker.rs#L303), [snapshot tablet update](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/cluster/state.rs#L846) |
| Public endpoint/token APIs and optional preserialized interop helpers | [ClusterState::compute_token](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/cluster/state.rs#L618), [checked endpoint lookup](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/cluster/state.rs#L690), [interop helpers and feature gate](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/cluster/state.rs#L898), [preserialized token implementations](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/cluster/state.rs#L945) |
| Direct replica/ring inspection, caller-supplied RNG, iterator skipping | [ReplicaLocator](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/routing/locator/mod.rs#L114), [ReplicaSet](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/routing/locator/mod.rs#L362), [iterator](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/routing/locator/mod.rs#L594) |
| Optional Python routing-input construction | [RoutingInfo::new](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/policies/load_balancing/mod.rs#L129) |
| Host filters | [accept-all filter](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/policies/host_filter.rs#L33), [allowlist filter](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/policies/host_filter.rs#L65), [datacenter filter](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/policies/host_filter.rs#L90) |

### Implementation constraints and exceptional behavior

These details constrain claims of exact parity and should be explicit decisions
when designing the Go API:

- Parsing a token from a string directly stores the integer, unlike `Token::new`;
  it does not normalize the minimum signed value. The source therefore does not
  support a universal assertion that every Token value is normalized.
- Prepared-key extraction chooses single/composite framing after filtering null
  and unset values. The separate serialized-partition-key helper chooses it from
  the original element count and then skips null/unset values. These helpers can
  differ on invalid composite keys; valid complete keys agree.
- The optional `scylla_unstable` plus `unstable-csharp-rs` APIs accept preserialized
  partition-key values. One looks up the table's partitioner; the explicit-partitioner
  variant bypasses table metadata entirely. They perform no schema-based type,
  component-count, or order validation, and use the serialized-key framing helper
  above. This is an exception to the ordinary token API's metadata requirement.
- The independent `scylla_unstable` plus `unstable-python-rs` gate exposes
  `RoutingInfo::new` for interop. It directly stores the six supplied routing
  fields, including borrowed table/location inputs; it performs no token
  calculation, metadata lookup, or consistency validation.
- The public `Partitioner` and `PartitionerHasher` traits are sealed. Callers can
  use the built-in Murmur3/CDC types or `PartitionerName`/`PartitionerHasherAny`
  dispatch, but cannot implement an additional partitioner through these traits.
- Connection-local preparation constructs a statement with the default Murmur3
  partitioner ([constructor](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/statement/prepared.rs#L248),
  [connection preparation](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/network/connection.rs#L763)).
  Only session preparation replaces it from table metadata
  ([assignment](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/client/session.rs#L2018)).
  Consequently, an unpaged/manually paged unprepared call with bind values can
  compute its tablet hint using Murmur3 even for a CDC table. Its plan is already
  token-unaware; do not infer CDC-correct hint selection from this path's local
  preparation alone. Automatic paging with values uses session preparation and
  therefore does select the table's partitioner.
  Session partitioner lookup searches the base-table map only, not the
  materialized-view map; preparing a view therefore retains the default even if
  its view schema has a partitioner override ([lookup](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/client/session.rs#L2027)).
- The global ordered-NTS fast primary search checks whether the node's datacenter
  occurs in the replication-factor map, not whether that factor is positive.
  Do not claim that this optimized path explicitly excludes configured zero-factor
  datacenters. The ordinary replica-set calculation does handle zero factors.
- The default fallback's mixed explicit/unspecified-shard comparator is designed
  for its replica-first/node-later ordering. The general Plan wrapper only removes
  exact copies of the first raw target; custom policies must implement any broader
  uniqueness contract they need.
- Sharding metadata parsing accepts an unsigned byte for the ignored-bit count
  without an explicit semantic range check before the later shift. This is not a
  promise of graceful handling for every malformed server-supplied shift.
- Building a policy with latency awareness spawns its periodic updater through
  Tokio, so construction requires an active runtime. Dropping the last owner of
  the policy drops its remote handle and cancels the updater. Unlike zero scale, a zero update
  rate is not rejected by its setter: the spawned updater's timer construction
  panics when polled. The remote handle is retained without being awaited, so
  this failure is not returned as a policy-construction error. It is not a
  supported way to disable updates ([updater construction](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/policies/load_balancing/default.rs#L3439),
  [unchecked setter](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/policies/load_balancing/default.rs#L3760)).
- Allocation avoidance is strongest for first-choice routing and precomputed
  replica lookup. Full fallback shuffling, global ordered-NTS continuation,
  latency fallback partitioning, and snapshot changes allocate. These reference
  paths do not already meet a blanket zero-allocation/zero-copy Go requirement.

## Test places

The following are inspected source targets, not claims that tests were run in this
workspace. Rust paths are relative to the pinned Rust checkout. Go coverage is
pending migration and is deliberately not claimed from similar legacy behavior.

### Rust unit tests

#### Description

Check deterministic algorithms, ordering groups, invalid input, and cache
transitions independently of a server. Default-policy tests inspect complete
`Plan` output assembled from first choice and fallback, usually over 256 samples;
they do not separately prove equivalence of every `pick` and `fallback` result.
Most policy fixtures substitute enabled status for connected status. The leader
down fixture leaves one enabled node disconnected to check last-resort placement.

#### Test cases

- Ordinary token-aware and token-unaware plans respect location/failover groups: `scylla/src/policies/load_balancing/default.rs:test_default_policy_with_token_aware_statements,test_default_policy_with_token_unaware_statements`.
- Confirmed LWT and regular SERIAL/LOCAL_SERIAL use ordered replicas: `scylla/src/policies/load_balancing/default.rs:test_default_policy_with_lwt_statements`.
- Policy preference overrides or inherits the routing input: `scylla/src/policies/load_balancing/default.rs:test_default_policy_uses_routing_info_preference`.
- Leader promotion respects consistency, location restrictions, and unavailable leaders: `scylla/src/policies/load_balancing/default.rs:test_default_policy_leader_aware_routing,test_default_policy_leader_aware_routing_with_dc_preference,test_default_policy_leader_aware_routing_falls_back_when_leader_down`.
- No cheap first choice still invokes fallback: `scylla/src/policies/load_balancing/plan.rs:plan_calls_fallback_even_if_pick_returned_none`.
- Nonempty-ring boundary vectors wrap once, and vnode strategy sets agree with expected replicas: `scylla/src/routing/locator/token_ring.rs:test_token_ring`; `scylla/src/routing/locator/test.rs:test_locator`.
- Direct locator inspection, capped replica-set lengths, filtered random selection, and iterator skipping are covered by helpers called from `scylla/src/routing/locator/test.rs:test_locator`: `test_datacenter_info`, `test_replica_set_len`, `test_replica_set_choose_filtered`, `test_replica_set_iterator_nth`, and `test_replica_set_iterator_nth_large_n_regression`. The last checks oversized and maximum-index skips for plain vnode sets; `test_tablet_replica_sets` separately checks masked tablet skipping. Random-choice tests sample membership, not statistical uniformity.
- On-demand and precomputed SimpleStrategy/NTS lists match explicit vectors, including zero factors, factors above rack count, and precomputation misses: `scylla/src/routing/locator/replication_info.rs:test_simple_strategy,test_network_topology_strategy`; `scylla/src/routing/locator/precomputed_replicas.rs:test_simple_stategy,test_network_topology_strategy` (the first precomputation test name is misspelled in source). These are fixed examples, not an exhaustive equivalence check.
- Ordered vnode replicas follow ring order: `scylla/src/routing/locator/mod.rs:test_replicas_ordered`. Tablet replica order, explicit shards, datacenter masks, and unknown ranges are checked by `scylla/src/routing/locator/test.rs:test_tablet_replica_sets`; masks spanning 64-bit words are checked by `scylla/src/routing/locator/mod.rs:masked_sharded_replica_set_spans_words`.
- Prepared key extraction reorders three selected bind values into partition-key column order: `scylla/src/statement/prepared.rs:test_partition_key_multiple_columns_shuffled`. This assertion checks extracted bytes and column specifications, not composite wire framing.
- Murmur3 and CDC match fixed vectors, including non-ASCII Murmur3 input and short CDC input: `scylla/src/routing/partitioner.rs:test_murmur3_partitioner,test_cdc_partitioner`. `partitioners_output_same_result_no_matter_how_input_is_partitioned` compares whole-input hashing with 1,000 seeded four-chunk splits for each of four fixed inputs and both partitioners.
- Shard arithmetic, invalid advertised IDs, empty source-port candidates, and single-port draw failure behave as specified: `scylla/src/routing/sharding.rs:test_shard_of,shard_info_rejects_out_of_range_shard_ids,test_iter_source_ports_for_shard,test_iter_source_ports_for_shard_from_too_small_range_is_empty,draw_source_port_for_shard_panics_on_no_choice`.
- The tested malformed tablet ranges and payloads fail; a valid range with an empty replica list parses; V2 wins over V1; version blocks preserve nibble encoding: `scylla/src/routing/locator/tablets.rs:test_raw_tablet_deser_wrong_token_range,test_raw_tablet_deser_trash,test_raw_tablet_deser_v2_correct,test_raw_tablet_deser_prefers_v2_over_v1,test_choose_tablet_version_block_encoding`.
- Version-block tests check each sampled nibble against its version and require variation across 1,000 draws; cache-miss bytes also vary: `scylla/src/routing/locator/tablets.rs:test_choose_tablet_version_block_encoding,test_tablet_version_block_for_cache_miss_is_random`. They do not exhaustively force all 16 indices or establish a refresh deadline.
- STARTUP negotiates only V2 when both extensions are advertised, falls back to V1 when only V1 is advertised, and sends neither when unsupported: `scylla-cql-core/src/frame/protocol_features.rs:parses_tablets_v1_and_v2_support,negotiates_only_v2_when_both_supported,negotiates_v1_when_only_v1_supported,negotiates_v2_when_only_v2_supported,negotiates_no_tablets_routing_when_unsupported`.
- Overlap replacement and metadata maintenance preserve cache invariants: `scylla/src/routing/locator/tablets.rs:test_adding_tablet_overlapping_one,test_adding_tablet_neighbours_not_removed,replace_multiple_tablets_middle,table_maintenance_tests,maintenance_keyspace_add_remove_test`.
- Unresolved followers retain a leader, while an unresolved original leader stays unknown: `scylla/src/routing/locator/tablets.rs:known_leader_survives_a_follower_failing_to_resolve,known_leader_is_unknown_when_the_leader_fails_to_resolve`.
- Identical resolved feedback makes `with_updated_tablets` return no replacement state; changed version/shard/range and in-batch replacement order are checked: `scylla/src/cluster/state.rs:tablet_update_is_skipped_when_nothing_changes`. Worker publication itself is outside this unit test.
- Supplied-token endpoint inspection reports unknown keyspaces, while the deprecated API returns an empty list: `scylla/src/cluster/state.rs:unknown_keyspace_has_no_replicas`.
- Latency fixtures cover absent statistics, insufficient measurements, averages below/above the threshold, expired retry periods, and zero-scale rejection: `scylla/src/policies/load_balancing/default.rs:latency_aware_default_policy_does_not_penalise_if_no_latency_info_available_yet,latency_aware_default_policy_does_not_penalise_if_not_enough_measurements,latency_aware_default_policy_does_not_penalise_if_exclusion_threshold_not_crossed,latency_aware_default_policy_does_not_penalise_if_retry_period_expired,latency_aware_default_policy_penalises_if_conditions_met,zero_scale_panics_on_construction`. Threshold fixtures use 0.95/1.05 multiples, not exact equality; the positive-penalty test explicitly ticks the minimum updater.
- Token-aware latency fixtures move penalized local/remote vnode replicas behind eligible non-replica nodes and exercise a retained cached minimum: `scylla/src/policies/load_balancing/default.rs:latency_aware_default_policy_is_correctly_token_aware`. These are full-plan node-group assertions with injected statistics and an explicitly ticked updater; they do not exercise tablet leaders.
- With a paused clock and a nonzero new sample, the average stays unchanged while its measurement count increases: `scylla/src/policies/load_balancing/default.rs:timestamped_average_works_when_clock_stops`. This covers zero elapsed time between samples, not a zero-duration sample.
- Explicit regressions for missing indexed prepared values, null/unset component framing differences, and the 65,535/65,536-byte composite-component boundary: missing.
- Explicit regressions for negative tablet payload shards and malformed V2 accompanied by valid V1 (no parse fallback): missing.
- The cached-minimum update test also checks threshold equality: after explicitly setting the minimum to 10 ms, it raises one host to exactly the threshold times that still-cached minimum and keeps the host unpenalized; a slower host remains penalized until the next explicit update. `scylla/src/policies/load_balancing/default.rs:latency_aware_default_policy_stops_penalising_after_min_average_increases_enough_only_after_update_rate_elapses`. Despite its name, this test manually ticks the updater; it does not wait for a real refresh interval.
- Explicit regression combining leader promotion with latency penalties in first choice and full fallback: missing.
- Explicit regression for global ordered NTS with a configured zero-factor datacenter: missing.
- Explicit regression for global-consistency leader interpretation of V1-only cached feedback: missing.
- Explicit ordering regression for multiple disconnected enabled nodes in the final fallback groups: missing.
- Explicit regressions for materialized-view token-calculation rejection, metadata-free interop token calculation, zero retry period/minimum measurements, and non-finite exclusion thresholds: missing.
- Explicit regression for a nonnegative tablet payload shard above the advertised node shard count: missing.
- Explicit regressions for rejecting a valid replica prefix followed by a malformed declared entry, ignored trailing bytes, null/omitted replica lists, and empty feedback replacing a usable cached replica list: missing.
- Explicit regressions for plan iteration across live pool/sharder changes, shared speculative iterator advancement, and concurrent latency updates during fallback construction: missing.
- Explicit regressions for repeated hasher finalization followed by more writes, prepared key materialization with no partition-key indexes, optional Python routing-input construction, and latency zero-update-rate/updater teardown behavior: missing.
- Explicit regressions for retained prepared routing inputs across metadata refresh/reprepare, and ignored materialized-view partitioner overrides during preparation: missing.

### Rust integration tests

#### Description

Observe actual node/shard selection through database/proxy traffic and verify
feature negotiation, server feedback, and routing convergence.

Coverage depends on guards. The three `load_balancing/tablets.rs` tests return
early without tablet support; LWT cases additionally require `LWT_WITH_TABLETS`,
the view test requires `VIEWS_WITH_TABLETS`, and the CDC test requires
`CDC_WITH_TABLETS`. Every `ccm/tablets_routing_v2.rs` test can return early if the
cluster fails to start with the requested experimental strong-consistency
feature, or if tablets are unsupported. Its V2/mixed/leader tests additionally
require V2 negotiation; the V1 test deliberately does not. A passing test process
alone therefore does not establish that these guarded assertions ran.

#### Test cases

- Prepared writes with keys of lengths 1–49 involve one traced node at vnode RF=1, through both `execute_unpaged` and `execute_iter`: `scylla/tests/integration/load_balancing/token_awareness.rs:test_token_awareness`. `scylla/tests/integration/load_balancing/simple_strategy.rs:simple_strategy_test` checks inserted rows under SimpleStrategy RF=1 through literal, bound-unprepared, and prepared execution; it does not inspect coordinator placement. Both disable tablets when supported.
- Prepared and metadata-based token calculation agree with server `token(...)` for simple/composite keys, including permuted bind order: `scylla/tests/integration/statements/prepared.rs:test_prepared_statement,test_token_calculation`. `test_prepared_partitioner` checks Murmur3/CDC selection and CDC preservation through the prepared cache; it is ignored under `cassandra_tests`.
- Token-unaware pagers drain without error and return at least one row for unprepared empty-value and prepared no-PK-bind cases: `scylla/tests/integration/session/pager.rs:test_token_unaware_pager`. This covers missing routing metadata, not actual key-extraction failures.
- For each tested key, each node receiving captured EXECUTEs sees one reported shard after full-pool readiness: `scylla/tests/integration/load_balancing/shards.rs:test_consistent_shard_awareness`. It does not compare that shard with an independently computed owner or require every node to receive traffic. `test_shard_out_of_range` checks a successful query with a custom hint of 10,000; it assumes that exceeds the test node's count without asserting the count.
- Prepared/batch LWT executions contact exactly one node when the optimization mark is offered, including a batch whose second prepared statement is LWT; without the mark the test expects more than one node, as for ordinary requests: `scylla/tests/integration/load_balancing/lwt_optimisation.rs:if_lwt_optimisation_mark_offered_then_negotiatied_and_lwt_routed_optimally`. These are sampled node-count assertions, not independent primary-replica or shard assertions.
- Tablet tests require feedback while warming sampled keys, compare cached node/shard sets with server tablet data, then require no feedback during verification: `scylla/tests/integration/load_balancing/tablets.rs:test_tablets,test_tablets_materialized_view,test_tablets_cdc_log`. The base-table matrix also checks prepared token-awareness, no feedback for literal native QUERY cases, and one coordinator/shard per token-aware LWT key. View/CDC cases refresh metadata before verification; CDC explicitly checks its partitioner. Migration-aware retries bound the observation to a usable layout.
- For one partition, V2 warm-up must produce feedback and every captured EXECUTE must carry a block; a stable measured phase must produce no V2 feedback: `scylla/tests/integration/ccm/tablets_routing_v2.rs:test_tablets_routing_v2_execute_carries_block_and_converges`. `test_tablets_routing_v2_mixed_feature_connections` forces traffic to nodes with V2 visible/hidden and decodes each frame under the applicable feature set. `test_tablets_routing_v1_used_when_v2_unavailable` requires V1 feedback, no V2 payload/block, and measured coordinator host IDs within the cached replica set. These finite samples do not guarantee convergence within a fixed request count.
- Strong LOCAL_QUORUM reads use the leader host ID decoded independently from V2 feedback after cache warm-up, with repeated measurements when feedback signals movement: `scylla/tests/integration/ccm/tablets_routing_v2.rs:test_leader_aware_routing_targets_the_raft_leader`. The assertion compares host IDs, not leader shard IDs.
- Native BATCH cache-feedback exclusion and the distinction from unprepared calls executed as EXECUTE: explicit regression missing.
- CDC tablet-hint selection through connection-local preparation of an unpaged/manually paged unprepared call: explicit regression missing.
- Token-aware and CDC routing through automatic paging's implicit session preparation, and differing treatment of token-calculation errors across these entry points: explicit regression missing.
- Routing assertions contrasting plain-session strings with cached preparation, including batches and empty-value calls: explicit regression missing. Existing prepared-partitioner coverage checks the cached CDC partitioner, not this full dispatch matrix.
- Valid tablet feedback on failed EXECUTE/UNPREPARED responses and malformed feedback preserving the original query outcome: explicit regression missing.
- Connection-selection failure skipping retry-policy/load-balancing callbacks, and failed-attempt feedback when a retry policy ignores a write error: explicit callback regressions missing.
- Tablet channel saturation interrupted by timeout/speculative cancellation, feedback publication after cancellation, and late feedback after drop/recreate of the same table name: explicit regressions missing.
- Automatic paging retaining its initial snapshot and tablet hint while other requests publish new tablet mappings: explicit regression missing.
- With latency awareness enabled, the invalid CQL string `whatever` completes with an error: `scylla/tests/integration/load_balancing/latency_awareness.rs:latency_aware_query_completes`. It does not assert a successful result, an error kind, measured penalties, or coordinator choice.

### Go unit/race tests

#### Description

Validate the eventual Go API's routing and ownership contracts using equivalent
placement vectors and concurrent snapshot/policy operations.

#### Test cases

- Generic encoded keys produce server-compatible tokens without key concatenation: missing.
- Policy defaults, LWT/leader eligibility, and exact fallback priorities match the adopted Go contract: missing.
- Concurrent tablet feedback and plan iteration preserve snapshot ownership: missing.
- Custom policy target deduplication and strict versus advisory shard behavior are explicit: missing.

### Go integration tests

#### Description

Migrate the existing gocql routing, policy, tablet, shard-aware-port, batch/LWT,
and control integration scenarios to the new API without weakening assertions.

#### Test cases

- Migrated token, tablet, LWT, and connection-targeting scenarios pass on supported database lanes: missing.
- Unknown ranges, stale/unknown replicas, failover restrictions, and node changes remain recoverable: missing.

### Go allocation benchmarks

#### Description

Measure adopted hot paths separately from unavoidable fallback or metadata work,
with explicit buffer ownership and representative routing metadata.

#### Test cases

- Stream key hashing, cached replica lookup, first-choice routing, and shard selection avoid per-request buffer copies and allocations: missing.
- Fallback, LWT continuation, latency policy, and tablet update allocation costs are separately visible and satisfy the Go specification's budgets: missing.

## Test generation notes

Use exact token boundaries, minimum/maximum token values, empty rings, many vnodes
per host, multiple racks including missing names, unknown datacenters, zero and
oversized replication factors, mixed-shard duplicate hosts, and a preferred rack
with no live replicas. Include confirmed and unconfirmed serial reads, batches
whose first statement is unprepared and later statements are LWT, changed retry
consistency, and exhausted first-choice filters. Distinguish a disconnected node
from a disabled one. Compare unprepared automatic paging with unpaged/manual
calls, including key-calculation errors and CDC partitioner selection. Separate
connection-acquisition failures from attempt failures when checking callbacks.

For tablets exercise cold caches, contiguous versus overlapping ranges, splits and
merges, unknown leaders versus followers, materialized views, out-of-order feedback,
V1-only/V2/mixed connections, repeated identical updates, late feedback after schema
changes, and more than 64 replicas in datacenter masks. Check V2 probing with a
controlled random source rather than assuming a finite probabilistic convergence
bound. Distinguish malformed declared replicas from ignored trailing bytes;
include null/omitted lists, empty-list replacement, and feedback on failed
EXECUTEs. Test channel saturation and snapshot lifetime separately from
routing correctness, including cancellation before enqueue, publication after
enqueue, feedback arriving after table recreation, and automatic paging over a
concurrently replaced mapping. Pause between plan advances to change pool/sharder
state, and distinguish shared iterator advancement from concurrent attempts.
For latency, control time and measurements; cover equality at the
threshold, penalty expiration, zero samples, stale global minima, and all-targets
penalized behavior.
