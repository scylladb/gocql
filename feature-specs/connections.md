# Connections, selection, and pool lifecycle

## Summary

A session maintains a pool for each enabled node, multiplexes requests over each
connection, and replaces failed connections asynchronously. The pool can target a
connection count per node or per shard. Requests prefer their target shard but can
use another live shard on the same node when its pool is incomplete. This is a
description of Rust revision `f8b0ee928dc0dcb475247dc82cce4e42804b0621`, not a proposed
Go contract. Node selection belongs to [routing](routing.md); control-connection
selection and topology handling belong to [metadata and events](metadata-control-events.md).

## Language-agnostic behavior

The following subsections specify the connection subsystem's named subfeatures.

### Defaults and boundaries

| Setting | Effective session default | Consequence |
| --- | --- | --- |
| Pool target | One connection per shard | A non-sharded server is treated as one shard. |
| Advanced shard-aware port | Allowed | Used for missing shards only after initial discovery and only with per-shard sizing. |
| Local address | Unspecified | The operating system selects an interface through an IPv4/IPv6 wildcard bind. |
| Local shard-aware port range | Inclusive 49152–65535 | A configured range must be nonempty and contained in 1024–65535. |
| Connect timeout | Five seconds | Bounds socket creation, option application, bind, and TCP connect; it is not a total address-translation/TLS/CQL-handshake deadline. |
| TCP no-delay | Enabled | Applied before bind/connect. |
| TCP keepalive | Disabled | Independent of CQL keepalive. |
| TCP receive/send buffers, reuse-address, linger | Unspecified | Preserve operating-system defaults. |
| CQL keepalive interval and timeout | Thirty seconds each | Each connection periodically sends an uncompressed, untraced OPTIONS request. |
| Write coalescing | Enabled with a scheduler yield | An empty request queue can be given a brief, nondeterministic opportunity to accumulate more work. |
| Reconnection policy | Exponential | Nominal initial delay 50 ms, maximum 10 s, jitter multiplier sampled from 0.85–1.15 and clamped to those limits. |

These defaults come from session configuration. The low-level connection defaults
compiled for tests disable CQL keepalive and must not be confused with session
defaults. The configurable reconnect-policy API is unstable and gated by both a
build configuration flag and a feature; the default policy is also used when that
public customization is unavailable.
Session creation rejects a zero CQL keepalive interval. It does not reject zero
connect or CQL keepalive timeouts; zero is a timeout value, not a disable switch.
CQL keepalive is disabled by an absent interval, and its timeout is disabled by
an absent timeout. Pool counts and the millisecond coalescing delay use nonzero
values.

### Connection establishment and negotiated properties

1. Resolve the endpoint's connect address. Contact-point addresses and peer
   addresses marked untranslatable bypass address translation. Other peer
   addresses use the configured translator with host identity, address, data
   center, and rack, or their advertised address if no translator is configured.
   Translation failure fails this attempt.
2. Create a socket for the destination address family; apply socket options;
   bind the requested source IP/port or wildcard IP/ephemeral port; connect under
   the connect timeout. Socket setup errors propagate. A configured source IP is
   used as provided; the driver does not silently substitute a compatible family.
   Configured TCP receive/send buffer sizes are cast to unsigned 32-bit values
   before application, so larger values can truncate on 64-bit platforms. The
   operating system can adjust the effective buffer sizes.
3. Establish TLS when configured, then start the request router. Detailed
   authentication and TLS contracts are in
   [configuration and security](configuration-security-observability.md).
4. Send OPTIONS and parse SUPPORTED. Discover sharding information, the TLS or
   plaintext shard-aware port appropriate to this connection, optional host ID,
   compression capabilities, and protocol extensions. Sharding parser errors
   cause operation without sharding information: the three required fields must
   have first values that parse as a 16-bit shard ID, a nonzero 16-bit shard count,
   and an 8-bit ignored-MSB count, and the shard ID must be below the shard count.
   The ignored-MSB count has no additional range validation here; values 64–255
   are accepted by discovery. Malformed optional host IDs are logged and treated
   as absent. Only the shard-port key matching the connection's TLS mode is
   inspected, and only its first string is parsed as an unsigned 16-bit port
   (including zero). An absent, empty, or invalid matching entry does not fall
   back to the opposite TLS/plaintext key or a later string.
5. Send STARTUP with supported feature opt-ins, CQL version, driver/application
   identity, and session identity. Configuration reporting, when supplied, is
   attached to the control connection. A requested unsupported compressor falls
   back to no compression. Complete authentication if requested by the server.
6. A connection configured to receive server events sends REGISTER and requires
   READY. A database error, unexpected response, request/response serialization
   failure, stream-allocation failure, or broken connection during setup fails
   establishment. Ordinary data connections do not register for events.
7. Before a newly opened data connection becomes selectable, apply the pool's
   current keyspace, if any. Keyspace setup has its own connect-timeout-sized
   deadline. Recheck the current keyspace when setup completes and repeat setup
   if another switch occurred while the connection was being initialized.

SUPPORTED is decoded into a map before discovery. If the wire response repeats
an option key, its last value list replaces the entire earlier list; the lists
are not merged and duplicate keys are not rejected. The first-value rules above
apply to that retained list. A malformed string/list encoding fails OPTIONS;
it does not take the fallback used for malformed sharding values in a successfully
decoded map.

The connect timeout is not restarted around a single umbrella operation covering
all seven steps. Address translation, TLS, OPTIONS, authentication, STARTUP, and
REGISTER have no such common deadline inside the open-connection routine.
Cancellation by an owning layer can still terminate an attempt. CQL keepalive
starts only after TLS and router construction, so it cannot bound address
translation or TLS. A keepalive failure can fail a pending CQL setup request,
but it does not impose a deadline on an authenticator's asynchronous callbacks.

An advertised host ID different from the pool's expected peer ID produces an
error-level diagnostic after keyspace setup. The connection is nevertheless
accepted if otherwise useful. Missing host IDs are accepted silently. The driver
does not turn this diagnostic into a node-identity enforcement guarantee.

Compression is negotiated independently for each connection. OPTIONS, STARTUP,
and AUTH_RESPONSE are sent uncompressed; REGISTER and ordinary query, prepare,
execute, and batch requests use the negotiated compressor when enabled. Response
decompression follows the response frame's compression flag rather than whether
its corresponding request was compressed. One connection's unsupported-compressor
fallback does not rewrite the session configuration or other connections.

### Pool state and initialization

Each pool publishes one of three states:

| State | Meaning | Selection result |
| --- | --- | --- |
| Initializing | No first usable connection and no completed first failed fill | An initializing-pool error |
| Ready | At least one published usable connection | A selected connection; full target capacity is not required |
| Broken | Empty after failed initialization or loss of all usable connections | A broken-pool error carrying the last connection error |

A ready snapshot can briefly contain a connection that has failed but has not yet
been removed by the worker. “Connected” means the pool is Ready, not that every
shard has reached its target or that a new network probe succeeded. An
initialization waiter completes as soon as the first usable connection is
published or the first fill concludes unsuccessfully. It does not wait for a full
pool. Notification registration precedes the state check to avoid a lost wakeup.

Disabled nodes have no pool, never open data connections, report disconnected,
and return a host-filter-disabled error on connection selection. Pool-refill and
keepalive hints on such a node are no-ops. An address-only change preserves the
pool when the node remains enabled and its host ID, datacenter, and rack remain
unchanged, and updates the endpoint used by future connection attempts. Existing
connections and already-started attempts are not synchronously replaced. If the
refreshed node is enabled, a datacenter/rack change or enabling a previously
disabled node instead creates a new pool. A transition to disabled creates a
node without a pool. Retained older node handles can still
own their earlier pools, as detailed in
[metadata snapshot publication](metadata-control-events.md#merging-and-publishing-snapshots).

### Connection-derived node properties

The public node sharder accessor returns no sharder when the node is disabled,
its pool is Initializing or Broken, or the ready pool has no valid sharding
information. Missing sharding information does not identify the database product:
ScyllaDB can suppress its sharding advertisement.

The public ScyllaDB-detection accessor returns an unknown value when the node has
no Ready pool. Otherwise it examines the first connection in the published pool
(the first nonempty shard bucket in a sharded pool). It returns true if that
connection has valid sharding information, a parsed matching-mode shard-aware
port, a parsed LWT metadata mask or rate-limit error code, tablet-routing v1/v2
support, or metadata-ID support; otherwise it returns false. A host ID alone is
not a detection signal. This observes one connection's negotiated features, not
an independently verified product identity or agreement among all connections.
The control connection uses its own detection result for ScyllaDB-only metadata
timeouts and their configuration report, as described in
[metadata and events](metadata-control-events.md) and
[configuration and security](configuration-security-observability.md).

### Connection selection

For a non-sharded pool, choose one existing connection uniformly; a one-element
pool bypasses random-number generation. For a sharded pool with a requested shard,
first choose uniformly among that shard's connections. If the requested shard is
empty or outside the discovered shard range, visit all shards in a random
permutation until a nonempty shard is found and select a random connection there.
An externally supplied shard number too large for 16 bits is diagnosed and
converted to shard zero before this selection. A representable but out-of-range
shard is diagnosed and takes the fallback path.

Without a requested shard, first draw a shard uniformly, then use the same
selection/fallback algorithm. This describes the pool's random-connection helper.
Ordinary session execution instead resolves an absent policy shard hint in the
routing plan, using the node's then-observed shard count or shard zero without
sharding information, and subsequently calls the pool with that explicit hint.
The plan's observation and the pool selection can therefore use different
snapshots. Neither random choice is weighted by the number of connections
per shard. Selection is independent of in-flight request counts and does not
reserve stream capacity. A selected connection can therefore reject a request
because it has no stream IDs even when another connection has capacity. Cross-node
retry or fallback remains the execution layer's responsibility. Failure to obtain
a connection skips directly to the next plan target without consulting the retry
policy; stream exhaustion after selection is an attempt error and does consult
that policy, as described in [execution](execution.md#request-attempt-deadline-and-error-boundaries).

Each selection uses one retained pool snapshot, including its sharder and
connection buckets. Separate calls to inspect connectivity or the sharder and
then select a connection can observe different snapshots. Publication can remove
a connection while a caller is still selecting from an older snapshot; the
returned connection reference remains usable until its own router fails or its
owners release it. A connectivity observation does not reserve a connection for
the next operation.

### Filling, excess connections, and backoff

The first fill starts immediately. Whenever the active pool is empty, start only
one ordinary-port connection attempt to avoid a burst of likely failures. Once
there are active connections, fill the missing target count concurrently:

- Per-host mode counts all active connections together and accepts any shard
  distribution. It does not use targeted shard-aware-port attempts.
- Per-shard mode counts each shard separately. With usable shard information and
  an advertised allowed shard port, request the exact missing count for each
  shard. Otherwise open the total number of missing connections on the ordinary
  port and accept the server's shard assignment.

A new fill is scheduled only when the pool is under target and the previous
batch of connection/setup futures is finished. At that scheduling decision, a
clear failure flag resets the nominal backoff to 50 ms; a flag set by any
ordinary-port or keyspace-setup failure doubles it, up to 10 s. A batch that leaves
the pool full does not itself invoke either outcome callback. Each scheduled
delay gets independent jitter and is clamped. Thus the first failed immediate
fill normally schedules a nominal 100 ms delay. Dropping
below target after a connection failure does not itself set the batch-failure
flag; the next scheduling decision uses the current flag. Refill is persistent,
with no fixed retry-count limit, provided the pending attempts finish. A stalled
translation, TLS handshake, authentication callback, or unanswered CQL setup
request can leave a batch pending indefinitely. In the last case this can happen
with keepalive disabled, or while keepalive responses continue to arrive. Such a
pending attempt prevents a new refill batch even if the active pool later becomes
empty; a refill hint does not cancel or replace it.

An explicit refill hint clears the scheduled timer, resets the policy and failure
flag, and then schedules a new fill at the policy's initial delay if needed. It
does not guarantee a zero-delay attempt, cancel already-running attempts, or
start another batch concurrently with the current batch. A failure arriving
after the reset can set the flag again.

A reconnect-policy factory is called once per pool, and that pool retains the
returned mutable policy session. A custom implementation can still share state
internally; the driver does not enforce isolation between those sessions.
Its successful-fill/fill-error callbacks are invoked when scheduling a
needed refill, according to the failure flag, rather than once for every completed
batch. A refill hint also invokes successful-fill immediately, even if no refill
is needed; scheduling after that hint can invoke it again. The built-in exponential
policy permits custom minimum and maximum delays, requiring minimum no greater
than maximum. Its jitter range must be nonempty and start at a nonnegative
multiplier. The alternative constant policy multiplies its fixed duration by
the same default jitter range, without exponential growth or clamping; its
success/error callbacks leave the duration unchanged. A multiplier range of
exactly one disables jitter. These customization contracts do not promise that
invalid floating-point configurations outside the explicit checks are accepted
or normalized.

Ordinary-port connections landing on an already-full shard are retained as excess
connections, excluded from request selection, while later fills seek missing
shards. Their failures are monitored, but existing excess connections are never
promoted when an active connection fails. Losing the last active connection can
therefore publish Broken while live excess connections remain, even on a shard
that now lacks capacity; recovery opens new connections. Excess connections are
also excluded from keyspace-switch snapshots and immediate keepalive hints.
The excess list is cleared when its size
exceeds ten times the shard count, or once the active pool is full. Per-host mode
has an excess limit of zero. A targeted connection that cannot be accepted causes
an immediate ordinary-port replacement attempt and is discarded, rather than
being retained as excess. Resharding discards both active and excess collections
and creates buckets for the newly discovered shard configuration before accepting
the new connection. Here a configuration change includes a change in shard count
or ignored-MSB count, and either gaining or losing valid sharding information;
loss of that information creates one non-sharded bucket. These changes occur only
after the new connection's keyspace setup succeeds. Each such completed
connection also replaces the pool's advertised shard port, even if it is later
classified as excess; a missing port clears the previous value. References
already held by requests can outlive removal from the pool; publication is not
synchronous cancellation of those users.

An opening attempt captures its endpoint and, for a targeted attempt, the sharder
and advertised port when it is created. Endpoint updates and resharding do not
cancel those attempts or reject their results by generation. A late successful
attempt can therefore be accepted at an old endpoint and can replace the pool's
sharder or advertised port with older discovery results. The keyspace recheck
above is a separate safeguard; it does not refresh those negotiated properties.

An ordinary-port connection error publishes Broken only when the pool is empty
and the fill has no remaining attempts. A keyspace-setup failure similarly
decrements the opened-connection metric, marks the fill unsuccessful, and
publishes an error if the pool is empty and no attempts remain. A failed attempt
does not replace a nonempty Ready pool with Broken.

### Advanced shard awareness and fallback

For a requested shard, choose local ports satisfying `port mod shard_count =
shard`, beginning at a uniformly chosen candidate and then advancing by the
shard count, wrapping once through the configured inclusive range. Try every
candidate at most once. I/O errors classified as address-in-use,
address-not-available, or permission-denied advance to another candidate; any
other result ends this candidate search. Exhausting candidates
returns the no-source-port-for-shard error, including when the range contains no
candidate for that shard.

The public sharder helpers use the default ephemeral range: iteration can return
no candidates, whereas the single-port draw helper panics if none exists. Both
assert that the requested shard is below the shard count. Those helper contracts,
also described in [routing](routing.md), differ from pool selection's invalid-shard
fallback and the connection-opening routine's no-source-port error.

Each candidate invokes the complete open-connection routine again, including
address translation, and receives a fresh TCP connect timeout. The candidate
search has no cumulative deadline. Only a top-level connection I/O error with one
of those three classifications advances the loop; a similarly named I/O error
wrapped inside a failed CQL setup request does not.

Any failure while opening a targeted shard-port connection starts an ordinary-port
attempt immediately. A subsequent keyspace-setup failure instead follows the
ordinary failed-fill handling described above. The opening failure alone does not
mark the fill unsuccessful or increase
backoff: the ordinary port might work, and multiple successful ordinary-port fills
can be necessary to cover all shards. The port used in a targeted endpoint is
set before address translation, so translation must preserve the intended
reachability semantics.

If a successful targeted connection reports a different shard while reporting
the same sharding configuration used to calculate its source port, suspend
advanced shard awareness for that node for 300 seconds. This commonly indicates
source-port rewriting. Already-in-flight attempts can still finish. Further
mismatches during the suspension do not extend its deadline. A mismatch explained
by changed shard configuration does not cause this suspension, even if the pool
has already adopted that new configuration from another attempt. After the
deadline, later fills may use the shard port again; expiration alone does not
replace a full pool. A mismatched connection may still be accepted into its actual
shard when that shard needs capacity. Token/shard routing remains available while
advanced connection establishment is suspended.

### Multiplexing, buffering, and write coalescing

Each connection has a bounded submission queue with capacity 1024. A request is
serialized before queue submission and assigned a per-connection 64-bit request
identity distinct from its eventual wire stream ID. Its counter starts at zero
and wraps modulo 2^64 without a wrap guard, so identities repeat after 2^64
allocations on that connection. Queue submission
waits when full; the queue limit is not an overall bound on already-serialized
requests held by waiting callers. The writer allocates a free nonnegative 16-bit
stream ID immediately before writing. All 32768 IDs, including zero and 32767,
are usable; allocation chooses the lowest currently free ID. Exhaustion returns
a stream-allocation error for that request without
writing it; it does not wait for a free stream and does not by itself close the
connection. Keepalive shares this stream space: if its OPTIONS request encounters
exhaustion, the keepalive failure closes the connection as described below.

The writer consumes queued requests and writes their serialized frames through
an 8192-byte buffer. When the queue becomes empty it either flushes immediately,
yields once to the scheduler, or waits the configured positive millisecond delay
and checks again. The delay occurs on an observed empty queue, not before every
request and not as a fixed latency guarantee. Buffered writes may flush earlier
when capacity requires it. The reader also has an 8192-byte buffer. Coalescing
does not change request identities or response association.

Responses can arrive out of order. A nonnegative stream ID releases its stream
slot and is dispatched to its waiting request, or discarded if the request was
marked orphaned. An unsolicited nonnegative stream breaks the connection.
Stream minus one is treated as a server event only when an event receiver is
configured; otherwise it is ignored. Other negative streams are ignored. This
filtering happens after reading the whole frame: invalid direction/version or
an unknown response opcode still breaks the connection, and an incomplete body
can stall the reader even on a stream that would be ignored. An ignored frame's
body is not decompressed or deserialized. Event
decoding failures, unexpected event responses, or failure to deliver to the event
receiver break the connection. Event delivery awaits a bounded receiver, so
event backpressure can stall that connection's reader.

A fatal router error fails registered pending requests with the same shared
connection error and notifies the pool monitor. Queued requests whose channels
are dropped observe channel/connection failure. A database error in an ordinary
request's response is a request-level result, not automatically a fatal transport
error. Response-body parsing for ordinary requests occurs after dispatch in the
request path; event parsing occurs in the reader.

### Cancellation, orphaned streams, and shutdown

Cancelling a request's response future sends its request identity to an unbounded
orphan-notification channel. If it still maps to an allocated stream, remove its
response handler and record the stream's orphaning time while keeping the stream
reserved. A later response frees the slot and removes the orphan record without
delivering data to another request. With distinct request identities, normal
response processing removes the request-to-stream mapping, so a late cancellation
notification cannot orphan a
new request that reused that stream ID. Notifications with no current mapping
are ignored; the implementation does not retain pre-allocation cancellation
notifications or promise that queued cancelled work is never transmitted.
If a notification is processed before the writer allocates that queued request's
stream, the writer can subsequently allocate and send it without an orphan
record. Its handler and stream then remain until a response or router termination,
and the old-orphan counter below does not count it. Cancellation alone therefore
does not guarantee that every abandoned request enters orphan-age tracking.

Once per second the router counts streams orphaned for approximately at least
one second. If that count is greater than 1024, it closes the connection with a
too-many-orphaned-streams error. Exactly 1024 does not trigger closure; young
orphans and old orphans below the count threshold are tolerated. The age uses
time of orphan notification processing, not original request submission time.
This is a recovery mechanism for stuck requests, not server-side cancellation.

Connection and pool background-task lifetime is tied to retained handles. Dropping
the last pool handle cancels its refiller; closure of the keyspace request channel
also terminates the worker when that branch can be polled. Dropping a connection
cancels its router when no connection owner remains. Removing a connection from
a published pool does not revoke other retained references, and there is no
graceful-drain promise in these internal connection/pool APIs.

### Keepalive and status hints

CQL keepalive begins after the first configured interval, not immediately at
router start. Missed ticks use delay behavior rather than burst catch-up.
An immediate keepalive hint wakes a connection's keepalive loop, sends OPTIONS,
and resets the next periodic interval. Hints received during an in-progress
keepalive retain a wakeup for the next iteration; multiple unconsumed hints can
coalesce. A pool forwards the hint to every currently published connection.
When CQL keepalive is disabled, hints do not create an active keepalive loop.

If the keepalive request fails or its configured timeout expires, the router
breaks the connection. The timeout covers queue submission and waiting for the
raw response, so a congested submission queue can consume it before OPTIONS is
written. Without a timeout it can wait indefinitely for the raw response. This
path checks successful raw-frame receipt and does not deserialize
or validate that the response is SUPPORTED. Ordinary request traffic does not
reset the periodic schedule. A server DOWN event uses this probing mechanism
rather than blindly discarding live connections; event interpretation is specified
in [metadata and events](metadata-control-events.md).

Optional TCP keepalive configures idle time to the user's interval. Where the
platform supports it, probe spacing is one second and the retry count is ten.
These operating-system probes and CQL OPTIONS probes are independent.

### Keyspace propagation and failure

An explicit session keyspace call and a successful raw USE response both enter
the propagation path below. For raw USE, the initial request has already changed
its selected connection before the session propagates the server-returned name
with case-sensitive quoting. This also applies to an automatic pager's first
SET_KEYSPACE response. Propagation can fail after that initial database success;
the response-side entry points and request-deadline boundary are described in
[execution](execution.md).

Session-wide switches enter a 32-element cluster request queue. The cluster
worker processes one fanout at a time, records the desired keyspace for future
nodes, and concurrently switches the nodes in one retained cluster snapshot.
It accepts the next switch only when the previous fanout finishes, while still
processing topology and connectivity updates. Racing calls have no prescribed
winner. Cancelling a caller after cluster enqueueing does not withdraw that
fanout while the cluster remains alive.

A keyspace name must contain 1–48 ASCII letters, digits, or underscores; an
underscore is allowed in the first position. Case-sensitive switching quotes the
name in USE; other switching sends it unquoted. The response must be SET_KEYSPACE,
and its name is compared case-insensitively even for a quoted request.

The session's keyspace-name accessor returns a retained snapshot of a separate
reported string. That string is stored before name validation and cluster
enqueueing, so even an invalid name can replace it without changing a pool's
desired keyspace. The accessor does not certify a completed switch and can
disagree with connection state after failure, cancellation, or concurrent calls.

The pool records the requested keyspace before switching a snapshot of its active
connections concurrently. If it has no active connections, the pool request
succeeds immediately and future connections inherit the keyspace. Otherwise it
tracks completions while continuing to process connection errors and refill
events. Only one keyspace switch is processed at a time; later switch requests
can wait in the one-element request queue. New connections can be prepared for
the new keyspace while the snapshot switch is in progress.

Once a pool switch request has entered that queue, dropping the caller's waiting
future does not withdraw it or cancel an in-progress switch while the pool remains
alive. The worker still changes its desired keyspace, sends USE, and applies
failure handling; it ignores failure to deliver the final result to the departed
caller. The switch snapshot retains its own connection references. Removing those
connections from the active pool, including during resharding, does not remove
them from the pending switch or its result aggregation.

A live connection that fails USE for a non-broken-connection reason is removed
from the active pool when the worker processes that result. Subsequent published
snapshots exclude it, but references retained from earlier snapshots are not
revoked and can still use its previous keyspace.
Already-broken connections are removed through normal failure monitoring. The
snapshot switch has one shared deadline, set to the connect timeout when the pool
starts processing it; waiting in the keyspace request queue is outside that
deadline. When the worker selects the expired-deadline branch, all
still-outstanding connections are removed and the switch reports timeout. Ready
completion events compete with that branch; there is no deadline-first priority
when both are ready. New connections that cannot establish the current
keyspace are never published. The requested keyspace remains current after
failure; this is not a rollback transaction.

Across the completed snapshot, any error other than a broken-connection error
causes failure. At least one success plus only broken-connection failures counts
as success, because replacements will inherit the new keyspace. If all snapshot
requests fail with broken-connection errors, return such an error. This aggregation
does not guarantee that every node was reachable or every connection switched
successfully. Concurrent application requests can race with a switch; the pool
does not offer a global atomic keyspace-change barrier.

The cluster uses the same aggregation rule for node results. A disabled node
returns success without sending USE, as does an enabled node whose pool has no
active connections. These no-op successes can satisfy the aggregation's success
condition, so an overall successful switch need not mean that any server has
validated the requested keyspace.

### Connectivity notifications

Publish the updated connection snapshot before notifying initialization waiters
and emitting connectivity events. With a configured internal connectivity-event
sender, Initializing or Broken to Ready emits Established; Ready to Broken emits
Lost; the first Initializing to Broken also emits Lost. Ready-to-Ready and
Broken-to-Broken updates produce no connectivity transition event. Event send
failure is ignored.
Data-node pools always have this sender, independently of whether a public host
listener is configured. The cluster worker consumes the events and updates its
recorded status before optionally invoking that listener; its callback filtering
and timing are specified in the
[host listener contract](metadata-control-events.md#host-listener-event-contract).
Removing the last active connection additionally tries a nonblocking pool-empty
notification; a full notification channel already represents a pending signal.
This separate notification is sent before the updated snapshot is published, so
its receiver can wake while the published pool still appears Ready. Data-node
pools discard this separate receiver and use connectivity events.

## Rust implementation evidence

Every link below is pinned to the revision described above. Line ranges are
navigation aids; symbols name the actual implementation being specified.

| Area | Source entry points |
| --- | --- |
| Effective defaults, socket configuration, validation | [SessionConfig::new, tcp_socket_options, validate](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/client/session.rs#L469-L671) |
| Open sequence, discovery, STARTUP, REGISTER, shard-port candidate loop | [open_connection and open_connection_to_shard_aware_port](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/network/connection.rs#L2077-L2318) |
| TCP timeout and router construction | [Connection::new and new_with_transport](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/network/connection.rs#L520-L618) |
| Authentication callbacks and setup errors | [perform_authenticate and authenticate_response](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/network/connection.rs#L823-L927) |
| Socket options and TCP probes | [connect_with_source_ip_and_port, apply_socket_options, setup_tcp_keepalive](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/network/connection.rs#L2320-L2426) |
| Pool modes and state representation | [PoolSize, MaybePoolConnections, NodeConnectionPool](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/network/connection_pool.rs#L38-L264) |
| Public node observations and feature-based product detection | [Node::sharder and Node::is_scylladb](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/cluster/node.rs#L188-L209), [pool observation](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/network/connection_pool.rs#L316-L334), [Connection::is_to_scylladb](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/network/connection.rs#L2005-L2015) |
| Selection, state errors, initialization, hints | [NodeConnectionPool methods](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/network/connection_pool.rs#L266-L502) |
| Plan shard resolution and selection-versus-attempt error boundary | [Plan::with_random_shard_if_unknown](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/policies/load_balancing/plan.rs#L94-L107), [NodeAttemptTarget::get_connection](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/client/execution.rs#L246-L254), [execution fiber](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/client/execution.rs#L560-L647) |
| Refill scheduling and targeted fills | [PoolRefiller::run and start_filling](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/network/connection_pool.rs#L670-L912) |
| Setup outcomes, NAT fallback, host-ID diagnostics, resharding | [handle_ready_connection through maybe_reshard](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/network/connection_pool.rs#L914-L1211) |
| Snapshot publication, connectivity, removal | [update_shared_conns, emit_events, remove_connection](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/network/connection_pool.rs#L1213-L1364) |
| Keyspace state and excess bound | [PoolRefiller keyspace methods and excess_connection_limit](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/network/connection_pool.rs#L1366-L1533) |
| Backoff policies, per-pool sessions, and unstable gate | [reconnect policy module](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/policies/reconnect.rs#L1-L246), [per-pool factory call](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/network/connection_pool.rs#L83-L97), [public module gate](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/policies/mod.rs#L25-L28), [configured or default policy selection](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/client/session.rs#L1412-L1424) |
| Source-port range validation and enumeration | [ShardAwarePortRange](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/routing/sharding.rs#L11-L42), [Sharder source-port methods](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/routing/sharding.rs#L143-L237) |
| Public source-port helpers | [single draw and default-range iteration](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/routing/sharding.rs#L126-L205) |
| Source-port retryable I/O errors | [ConnectionError::is_address_unavailable_for_use](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/errors.rs#L611-L627) |
| Sharding discovery validation | [ShardInfo::new](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/routing/sharding.rs#L85-L103), [ShardInfo::try_from](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/routing/sharding.rs#L274-L307) |
| SUPPORTED wire-map decoding and duplicate keys | [Supported::deserialize](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla-cql/src/frame/response/supported.rs#L15-L22), [read_string_multimap](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla-cql/src/frame/types.rs#L347-L358) |
| Submission and cancellation notifier | [RouterHandle and OrphanhoodNotifier](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/network/connection.rs#L70-L226) |
| Router, reader, writer, keepalive, event dispatch | [Connection router implementation](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/network/connection.rs#L1527-L1983) |
| Frame validation and body reads before stream filtering | [read_response_frame](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla-cql/src/frame/mod.rs#L150-L200), [Connection::reader](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/network/connection.rs#L1680-L1710) |
| Stream ownership and orphan age tracking | [OrphanageTracker, ResponseHandlerMap, StreamIdSet](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/network/connection.rs#L2428-L2586) |
| USE request and keyspace validation | [Connection::use_keyspace and verify_use_keyspace_result](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/network/connection.rs#L1370-L1416), [VerifiedKeyspaceName](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/network/connection.rs#L2589-L2646) |
| Raw USE propagation entry points | [Session::handle_set_keyspace_response](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/client/session.rs#L1775-L1789), [pager first response](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/client/pager.rs#L1192-L1221) |
| Session keyspace reporting and serialized cluster fanout | [Session::use_keyspace](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/client/session.rs#L2325-L2339), [Session::get_keyspace](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/client/session.rs#L2395-L2408), [ClusterWorker keyspace branches](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/cluster/worker.rs#L366-L411) |
| Keyspace success aggregation | [ClusterWorker::send_use_keyspace](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/cluster/worker.rs#L399-L411), [use_keyspace_broke_connection and use_keyspace_result](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/cluster/worker.rs#L804-L847) |
| Disabled nodes and preserved pools on address change | [Node lifecycle and pool delegation](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/cluster/node.rs#L114-L290) |
| Topology reuse conditions and public listener boundary | [ClusterState::calculate_new_topology](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/cluster/state.rs#L419-L479), [internal channel creation](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/cluster/worker.rs#L81-L89), [ClusterWorker::handle_connectivity_change_event](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/cluster/worker.rs#L760-L801) |

Rust-specific implementation choices include atomically published reference-counted
pool snapshots; a worker with concurrent sets of connection-opening, error, and
keyspace-switch futures; reader/writer/orphan/keepalive futures joined within one
router task; and a stream-handler map whose mutex is never held across a suspension
point. Connection monitors retain weak references, so monitoring alone does not
keep a removed connection alive. These are evidence of the observed behavior,
not requirements that a Go implementation reproduce the same data structures.

The router receives a configuration clone before OPTIONS negotiation. Ordinary
response parsing uses the connection's subsequently negotiated protocol features
and effective compressor. Event parsing instead uses default protocol features
and the router's original compression configuration; these are not updated when
discovery completes or an unsupported requested compressor is cleared on the
connection. See [send_request and parse_response](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/network/connection.rs#L1459-L1525),
[router construction](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/network/connection.rs#L577-L618),
and [event parsing](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/network/connection.rs#L1939-L1983).

The reference is not zero-copy throughout: requests are serialized to owned
buffers, ordinary pool snapshots clone connection references, gathering all
connections allocates a vector, and missing-shard fallback allocates a vector of
shards to randomize. A Go zero-copy/allocation contract must be specified and
measured independently rather than inferred from this implementation.

## Invariants

- A published Ready pool contains at least one active connection. Excess
  connections do not make a pool Ready or satisfy a missing shard's target.
- Pool initialization success and node connectivity do not imply a full pool.
- A new connection is not published before it has the currently selected keyspace.
- Processing a non-broken USE failure or switch timeout removes affected
  connections from subsequent pool snapshots without revoking retained
  references; errors do not roll back the pool's desired keyspace.
- Each allocated stream identifies at most one live response owner. Orphaning
  removes the owner without prematurely reusing the wire stream.
- A late response to an orphan is discarded; an unexplained stream response is
  fatal. Cancellation is not a database rollback or execution guarantee.
- Shard-aware connection failure can fall back to the ordinary port; a connection
  reaching the wrong shard can remain useful and must be classified by its actual
  shard. Changes in sharding configuration are distinguished from port rewriting.
- Publication precedes initialization-waiter notifications and connectivity
  transition events; the separate pool-empty signal precedes publication.
  Connectivity changes occur at the empty/nonempty boundary, not on every
  connection addition or removal.
- Node identity mismatch is diagnostic only at this revision.

## Test places

All Rust targets below were located in the pinned source. They have not been run
as part of drafting this document. Source-module tests can use simulated protocol
transports; their placement below reflects the algorithm/state-machine behavior
or externally observable connection behavior being tested, not only the Rust
test-runner target.

### Rust unit tests

#### Description

Check candidate enumeration and simulated pool transitions, keyspace failure
recovery, temporal suppression, and resource accounting without relying on a live
cluster's timing.

#### Test cases

- Source ports enumerate the desired shard without duplicates and handle a range with no candidate: scylla/src/routing/sharding.rs:test_iter_source_ports_for_shard,test_iter_source_ports_for_shard_from_too_small_range_is_empty
- The public single-port draw helper panics when its default range has no candidate for a valid shard: scylla/src/routing/sharding.rs:draw_source_port_for_shard_panics_on_no_choice
- The source-port range constructor accepts the default range and rejects a reversed range and one starting at zero: scylla/src/routing/sharding.rs:test_shard_aware_port_range_constructor
- Sharding discovery rejects shard IDs equal to or above the shard count and accepts the highest valid ID in the tested four-shard configuration: scylla/src/routing/sharding.rs:shard_info_rejects_out_of_range_shard_ids,shard_info_constructor_rejects_out_of_range_shard_ids,shard_info_accepts_highest_valid_shard_id
- Failure while establishing the new connection's keyspace leaves the pool refillable and preserves appropriate pool state: scylla/src/network/connection_pool.rs:keyspace_setup_failure_triggers_refill,keyspace_setup_failure_preserves_pool_state
- Temporary advanced-shard-awareness suspension keeps the eligibility guard false at 299 seconds and restores it at 301 seconds under a paused clock; this does not exercise an actual refill or the exact 300-second boundary: scylla/src/network/connection_pool.rs:advanced_shard_awareness_is_blocked_temporarily
- Repeated mismatches during suspension leave its original deadline unchanged: missing:advanced-shard-awareness-repeated-mismatch-deadline
- A changed sharder discards the old active connection with balanced metrics; this test starts with no excess connections and requires the metrics feature: scylla/src/network/connection_pool.rs:resharding_discards_old_connections_and_balances_metrics
- Resharding clears a nonempty excess collection and balances its metric contribution: missing:resharding-nonempty-excess-connections
- Losing valid sharding metadata or changing only ignored-MSB count replaces the old pool, and a later missing shard-port advertisement clears the remembered port: missing:pool-sharding-and-port-rediscovery-boundaries
- Late opening results can restore older endpoint/sharding observations, while selection retains a coherent individual snapshot across publication: missing:pool-inflight-opening-and-selection-snapshots
- Excess limits and full-pool cleanup release retained connections with balanced metrics, with the metrics feature enabled: scylla/src/network/connection_pool.rs:excess_connection_limit_clears_connections_and_balances_metrics,full_pool_clears_excess_connections_and_balances_metrics
- Active-connection loss does not promote existing excess connections, including when only excess connections remain; excess connections receive neither keyspace switches nor immediate keepalive hints: missing:excess-connections-after-active-failure
- Synthetic targeted-attempt results disable shard-port eligibility on a same-sharder mismatch; two results carrying the old sharder after resharding preserve eligibility, while a new-sharder mismatch disables it. The helper opens an ordinary-port connection and attaches requested-shard metadata, so these tests do not exercise actual source-port rewriting: scylla/src/network/connection_pool.rs:shard_mismatch_blocks_advanced_shard_awareness,resharding_does_not_block_advanced_shard_awareness
- The full 32768-stream space, exhaustion, reuse, and response-versus-cancellation races are explicitly checked: missing:stream-id-allocation-and-cancellation-races
- Cancellation processed before stream allocation leaves a later-sent request outside orphan-age tracking until response or router termination: missing:queued-cancellation-before-stream-allocation
- Initialization/connectivity notifications follow publication, while the separate pool-empty signal can precede it: missing:pool-notification-publication-order
- Selection is uniform within a nonempty shard and correctly handles all invalid/empty shard cases: missing:connection-selection-distribution-and-fallback
- Backoff batch outcomes, jitter bounds, and refill hints obey their exact scheduling contract: missing:pool-reconnect-scheduling
- Product detection distinguishes unavailable pools, host-ID-only advertisements, every supported detection signal, and mixed connection feature sets: missing:node-product-detection-boundaries
- An address change that makes the host filter reject the node produces a disabled replacement node instead of reusing the enabled object: scylla/src/cluster/state.rs:node_filtered_out_after_ip_change
- Address-only pool reuse, replacement on location changes, and continued use through retained older node handles have dedicated pool-lifetime assertions: missing:topology-change-pool-lifetime-boundaries
- Internal connectivity events update recorded status without a configured public host listener: missing:connectivity-events-without-host-listener

### Rust integration tests

#### Description

Exercise the actual socket/router or a protocol proxy, including timeout,
cancellation, coalescing, server events, and fallback behavior. Some source-module
tests use simulated transports; that proves the state machine under the supplied
responses, not live-database interoperability.

#### Test cases

- A USE error or manually triggered switch timeout removes the affected connection; an unanswered USE reaches its actual deadline during both new-connection setup and a pool switch: scylla/src/network/connection_pool.rs:keyspace_switch_failure_removes_connection,unanswered_use_keyspace_times_out
- Session and builder keyspace selection permit unqualified table queries; empty names, 49-character names, and a semicolon are rejected. A nonexistent server keyspace makes the switch and a subsequent query fail; a later switch to `system`, followed by waiting for a node to reconnect, restores a successful query: scylla/tests/integration/session/use_keyspace.rs:test_use_keyspace
- Quoted and unquoted switching select different uppercase/lowercase keyspaces, verified by distinct table contents: scylla/tests/integration/session/use_keyspace.rs:test_use_keyspace_case_sensitivity
- Raw USE through an unpaged query enables unqualified reads, preserves quoted/unquoted interpretation, and also propagates through automatic pager creation: scylla/tests/integration/session/use_keyspace.rs:test_raw_use_keyspace
- A failed pool switch refills into the same desired keyspace without a second switch request: missing:keyspace-switch-refill-without-second-switch
- Empty/disabled-node successes are distinguished from server-validated successes in keyspace aggregation: missing:keyspace-noop-success-aggregation
- The exact 48-character boundary, leading underscore, non-ASCII rejection, wrong SET_KEYSPACE name, and case-insensitive response-name comparison for a quoted request have dedicated assertions: missing:keyspace-validation-and-response-boundaries
- Accepted keyspace switches survive caller cancellation, retain removed connections until completion or timeout, and arbitrate ready results against the deadline: missing:keyspace-switch-cancellation-and-snapshot-lifetime
- A second session keyspace call completes after the first when the proxy delays the first fanout: scylla/tests/integration/session/use_keyspace.rs:test_concurrent_use_keyspace_calls_are_serialized
- The keyspace accessor reports no initial keyspace, a successfully selected keyspace, and a configured initial keyspace: scylla/tests/integration/session/use_keyspace.rs:test_get_keyspace_name
- The reported session keyspace remains distinct from pool state after invalid names, cancelled queued calls, failed switches, and concurrent calls: missing:keyspace-accessor-failure-and-concurrency
- Four hundred concurrent source-port-selected connections complete a dry-mode proxy handshake, both with default reuse-address and with reuse-address enabled. This smoke test does not force a collision or assert the assigned shard: scylla/src/network/connection_pool.rs:many_connections
- A valid SUPPORTED host ID is read, while a malformed UUID or absent key yields no host ID without failing establishment: scylla/src/network/connection.rs:host_id_is_read_from_supported
- Matching and missing peer identities produce no error log; a mismatch logs both IDs once, and a connection needing keyspace setup is diagnosed only after setup and only once: scylla/src/network/connection_pool.rs:unexpected_host_id_is_reported
- A misconfigured address translator reaching the wrong node produces the expected host-ID diagnostic; assertions are skipped if the server does not advertise SCYLLA_HOST_ID: scylla/tests/integration/session/host_id_mismatch.rs:mismatched_host_id_is_reported
- Public ScyllaDB detection agrees with the integration target's Cassandra/ScyllaDB build configuration: scylla/tests/integration/session/new_session.rs:is_scylladb_matches_cluster_type
- A CCM ScyllaDB node configured with `enable_shard_aware_drivers: false` exposes no sharder, still reports ScyllaDB, and emits captured control QUERY/PREPARE requests containing the configured `USING TIMEOUT 2137ms` clause: scylla/tests/integration/ccm/using_timeout.rs:test_control_connection_using_timeout_without_shard_awareness
- Missing keepalive responses eventually close the connection: scylla/src/network/connection.rs:connection_is_closed_on_no_response_to_keepalives
- Configured TCP buffers, linger, and reuse-address allow establishment and are observable on the socket; this test's proxy returns errors to application queries: scylla/src/network/connection.rs:tcp_socket_options_do_not_break_connection
- 1025 stuck orphaned requests close the connection, while brief spikes of 1025 and exactly 1024 old orphans preserve successful queries: scylla/src/network/connection.rs:test_orphaning_shuts_down_stuck_connections,test_orphaning_tolerates_short_spikes_of_orphaned_requests,test_orphaning_tolerates_long_running_orphaned_requests_under_count_threshold
- Sustained short-lived orphan traffic is tolerated, while gradual accumulation beyond the old-orphan threshold closes the connection: scylla/src/network/connection.rs:test_orphaning_tolerates_many_short_lived_orphaned_requests,test_orphaning_closes_connection_after_gradual_increase_of_orphaned_requests
- Scheduler-yield, millisecond-delay, and disabled coalescing preserve query results under concurrent load; this is a smoke test, not a latency or syscall-count proof: scylla/tests/integration/session/write_coalescing.rs:test_coalescing
- DOWN events immediately probe only the targeted node's existing connections: scylla/tests/integration/session/status_change_hints.rs:test_status_change_down_triggers_immediate_keepalive
- Requested-shard fallback exposes the selected connection's reported shard, using rewritten SUPPORTED data; the test skips servers without at least two advertised shards: scylla/tests/integration/statements/coordinator.rs:test_coordinator_shard_fallback
- Stalled translator/TLS/STARTUP phases demonstrate the exact setup timeout boundary: missing:connection-setup-timeout-boundaries
- A permanently pending setup attempt prevents later refill batches despite a refill hint or loss of all active connections: missing:stalled-setup-blocks-refill
- Source-port retries repeat translation with separate TCP timeouts and distinguish top-level retryable I/O errors from wrapped setup failures: missing:source-port-retry-scope
- Actual source-port rewriting suspends targeted fills and a later fill resumes them after the suspension: missing:advanced-shard-awareness-source-port-rewrite-and-resumption
- Shard-port negotiation checks only the matching TLS/plaintext key and its first value, including an advertised zero port: missing:shard-port-negotiation-boundaries
- Duplicate SUPPORTED wire-map keys replace earlier lists before discovery, while malformed map encoding fails setup instead of disabling only sharding: missing:supported-wire-map-negotiation-boundaries
- Negative-stream filtering distinguishes malformed headers, stalled bodies, and complete frames whose bodies are ignored without decompression or deserialization: missing:negative-stream-frame-validation-boundaries
- Disabled CQL keepalive and unvalidated raw keepalive responses have explicit regression coverage: missing:keepalive-disable-and-response-validation
- Stream exhaustion rejects an ordinary request without closing the connection, but exhaustion for a keepalive OPTIONS breaks it: missing:keepalive-stream-exhaustion
- Setup-message compression flags and event parsing's original configuration remain distinct from ordinary-response negotiation: missing:handshake-compression-and-event-configuration

### Go unit/race tests

#### Description

The new Go API must establish its own concurrency and ownership guarantees for
selection, stream reuse, cancellation, publication, shutdown, and refill. Existing
Go behavior is not assumed to match the Rust contract.

#### Test cases

- Go connection and pool state-machine conformance with cancellation and keyspace races: missing:go-connection-pool-conformance
- Published buffers and stream identities cannot be reused while retained by active users: missing:go-connection-buffer-lifetime

### Go integration tests

#### Description

Migrate the existing driver's shard-aware-port, connection, control, and event
scenarios to the proposed API while preserving their assertions and database
coverage. The migration ledger will enumerate exact original tests.

#### Test cases

- Migrated data/control connections survive pool refill, shard-port fallback, node failures, and keyspace switching: missing:go-connection-integration-migration

### Go allocation benchmarks

#### Description

Measure the generic API's hot paths independently from the Rust implementation,
including the less common missing-shard path and cancellation bookkeeping.

#### Test cases

- Connection selection, stream acquisition, steady-state writes, and response dispatch meet the specified allocation/copy budget: missing:go-connection-hot-path-budget
- Missing-shard fallback and high-cancellation workloads preserve the stated allocation/copy budget: missing:go-connection-fallback-budget

## Test generation notes

Exercise one and multiple shards; one and multiple connections per shard;
partially filled pools; disabled hosts; all shards empty; the largest valid and
first invalid shard/stream values; empty/full source-port candidate sets; port
conflicts; and translated endpoints whose source ports are rewritten. Inject
resharding and endpoint changes while multiple old attempts are in flight,
including an old handshake that completes after newer discovery was published.
Retain a selection snapshot across removal and compare it with a fresh selection.
Retain excess connections while removing the last active connection, including a
same-shard excess connection; check Broken publication and lack of promotion.
Distinguish an unavailable shard-aware port from a failed ordinary-port fallback
when checking backoff.

Encode repeated SUPPORTED keys directly on the wire, because constructing a map
first loses the duplicates. Distinguish malformed map encoding from valid map
entries whose sharding values fail to parse. For ignored negative streams, vary
header validity and body completeness separately from body decode validity.

For temporal tests use a controlled clock and explicitly distinguish exactly 1024
old orphans from 1025, and the age boundary from the periodic observation tick.
Schedule cancellation before queue submission, after queueing, after stream
assignment, concurrently with response dispatch, and after stream reuse. Force
an orphan notification to be consumed before allocation and check that the later
stream is absent from orphan-age tracking. Include
unsolicited streams, ignored negative streams, full event channels, shutdown with
queued requests, and failed keepalive while ordinary responses are arriving.

For keyspace tests interleave a switch, newly opened connections, refill failures,
another queued switch, application requests, caller cancellation after enqueueing,
retained connections removed by resharding, and simultaneously ready completion
and timeout branches. Assert the desired
keyspace after failure and which connections remain selectable rather than only
checking the returned error. Keepalive tests must distinguish OPTIONS receipt
from protocol-response validation; coalescing tests must distinguish preserved
results from throughput or latency claims. Do not infer a graceful drain,
identity-enforcement guarantee, or an all-stages connect timeout from defaults.
