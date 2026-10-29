# Configuration, security, and observability

## Summary

This section specifies session configuration, authentication, TLS, connection
address policy, identity, diagnostic reporting, metrics, request history, and
tracing in `scylladb/scylla-rust-driver` at
`f8b0ee928dc0dcb475247dc82cce4e42804b0621`. It describes the pinned implementation,
including its limits; it is not a proposal to change those behaviors in Go.

Connection and pool algorithms belong to [connections](connections.md), query
policy resolution and execution to [execution](execution.md), topology discovery
and server events to [metadata/control/events](metadata-control-events.md), routing
preferences to [routing](routing.md), and value/protocol encoding to
[serialization/protocol](serialization-protocol.md).

## Language-agnostic behavior

### Session configuration and startup

A session starts from a configuration containing contact points and optional
policies. Contact points can be hostnames or explicit socket addresses; hostnames
without a port use 9042. Contact points bootstrap discovery rather than defining
the entire set of nodes. Their addresses are not passed through the address
translator. Configuring contact points appends them in caller order. Session
creation validates the configuration before generating its identity or opening
connections, establishes cluster state, and then applies the configured initial
keyspace before returning success. Discovery and pool readiness details are in
the metadata and connections sections.

Hostname-resolution failures are logged and skipped if any contact address remains;
if none remains, session creation fails with the failed-hostname list. Each hostname
contributes all of its resolved addresses, duplicates are removed, and initial
connection candidates are shuffled rather than attempted in configuration order.
Hostname lookups run concurrently, but startup awaits the whole group before
trying candidates. With no DNS timeout, one unfinished lookup can therefore hold
up startup even when an explicit socket address is also configured.
The DNS timeout applies separately to each lookup. After a non-timeout error,
the resolver retries the original string paired with port 9042, even if that
string already included a port. This fallback receives a fresh timeout budget;
if it also fails, the original error is returned, even when the fallback timed
out. A first-lookup timeout skips the fallback. Thus the configured duration is
not a single deadline for the complete hostname-resolution operation.
A failed candidate handshake does not prevent trying another contact point. If an
initial handshake succeeds but metadata fetching fails, startup can proceed with
previously obtained metadata or dummy contact-point metadata and no retained control
connection; background repair then attempts to restore it. Successful session
creation therefore does not guarantee complete metadata or usable pools.

The builder is reusable: building a session clones its configuration. Ordinary
configuration values are captured by the resulting session; supplied policy,
authentication, and TLS objects can be shared. A default execution-profile handle
is an intentional indirection that can change the profile used by already-created
sessions and statements. Two separately created handles to the same profile are
not the same remapping group. A request keeps the profile snapshot it acquired
across retries and speculative attempts. An automatic pager captures its settings
once for all pages; separate manual page calls acquire their profiles separately.
Remapping changes subsequent acquisition, not these existing snapshots. The
snapshot fixes the selected settings and policy objects, not mutable state inside
shared policies.

The default configuration is:

| Setting | Default and effect |
| --- | --- |
| Contact points | Empty; creation fails until at least one is supplied. |
| Node location preference | Any datacenter/rack; individual load-balancing policies may override this preference. |
| Local bind IP | Unspecified address of the destination's IP family. |
| Shard-aware source ports | Inclusive range 49152–65535. |
| Compression | Disabled; selectable LZ4 or Snappy. |
| TCP no-delay | Enabled. |
| TCP keepalive | Not configured. |
| TCP receive/send buffer | OS defaults, individually configurable. |
| TCP address reuse / linger | OS defaults; normally reuse disabled and linger disabled. |
| TLS / authenticator | Neither configured. |
| TCP connect timeout | 5 seconds. See connections for its precise scope. |
| Hostname-resolution timeout | 5 seconds per lookup, including a fresh budget for the default-port fallback; an absent timeout permits indefinite resolution. |
| Connection pool target | One connection per shard; non-sharded nodes count as one shard. |
| Shard-aware port | Allowed; the specialized client-routes builder disables it. |
| Pool reconnect policy | Exponential, initial 50 ms, maximum 10 s, jitter multiplier 0.85–1.15, clamped to the configured limits. |
| Initial keyspace | None; case-insensitive interpretation if one is supplied without changing the flag. |
| Client timestamp generator | None: server-generated timestamps unless a statement supplies one. |
| Keyspaces to fetch | Empty list means all keyspaces. |
| Schema metadata / full schema metadata | Both enabled. |
| Metadata server timeout override | 30 seconds, appended using the ScyllaDB extension when a metadata statement is first prepared on a supported target. Cached statements retain that query text across control-connection replacements; see the reporting boundary below. |
| Metadata client timeout override | Unset, so derived from server override plus 1 second: **31 seconds with the defaults**, including non-ScyllaDB targets. The addition saturates at the duration maximum. With neither override configured it is 30 seconds. An explicit value wins, including zero; unset does not disable the deadline. Applies per page execution, excluding cache-miss PREPARE and the overall multi-page refresh. |
| CQL keepalive interval / timeout | Both 30 seconds. |
| Schema agreement poll / total timeout | 200 ms / 60 seconds. |
| Automatic schema-agreement wait | Enabled after applicable schema-changing statements. |
| Refresh metadata after automatic agreement | Enabled. |
| Address translator / host filter / host listener | None. |
| Write coalescing | Enabled with a small nondeterministic scheduling delay. |
| Trace-info lookup | 10 attempts, 3 ms between unsuccessful readiness checks, consistency ONE. |
| Periodic metadata refresh | 60 seconds, fetching affected keyspaces. |
| Custom identity fields | Unset. |
| Driver configuration reporting | Enabled. |
| Client routes | Unconfigured. |

The default execution profile specifies LOCAL_QUORUM, LOCAL_SERIAL, a 30-second
request timeout, the default load-balancing policy, the default retry policy, and
no speculative-execution policy. The default page size is 5000 and statements are
non-idempotent by default. A profile builder distinguishes an unset optional
field from an explicit disabled value, such as no request timeout or no serial
consistency. Statement overrides and policy precedence are specified in execution.

Configuration validation rejects an empty contact list and a zero CQL keepalive
interval. Disabling schema fetching while retaining full-schema fetching produces
a warning and disables all schema fetching; it does not reject the session.
An explicitly shorter metadata client timeout than the server timeout produces a
warning, without changing either value. Source-port ranges reject an empty range
or a lower bound below 1024; pool sizes and trace lookup attempt counts use
nonzero values. This is not a claim that every duration or combination is globally
validated: some constraints are enforced by individual configuration types or
later socket/runtime operations.

### Transport options and compression

Socket options are applied before bind/connect. A configured receive/send size is
cast to an unsigned 32-bit size and passed to the socket API without a range
check. On platforms with a wider configuration integer, values above 2³²−1
lose their high bits; for example, 2³² reaches the socket API as zero. This is
truncation, not rejection or saturation, and OS adjustment is also possible.
Explicit zero differs from leaving the option unspecified. Linger is truncated
to whole seconds: zero and positive values below one second request reset-on-close;
values of at least one second request timed lingering. The TCP
keepalive setting means idle time before probes begin, not the interval between
probes. Where supported by the target OS, probes are one second apart and the
retry count is ten; the platform-specific branches do not promise identical
support on every OS. TCP keepalive is separate from CQL OPTIONS keepalive.

Compression is negotiated separately on each connection using OPTIONS/SUPPORTED
before STARTUP. If the requested codec is absent from the server's supported list,
the connection logs a warning and continues without compression. The session
configuration is not globally rewritten by this one connection's fallback.
Authentication, TLS, compression, and all connection setup failures remain
connection/session errors; compression negotiation failure by absence alone is
deliberately nonfatal.

### Authentication

Authentication starts only when the server responds to STARTUP with AUTHENTICATE.
A server that returns READY needs no authentication exchange, even if the client
has a provider. If authentication is requested but no provider is configured,
connection setup fails with missing-authentication error.

The provider receives the server's authenticator name and produces an initial
response plus a fresh authentication exchange object for that connection. The
initial response and subsequent challenge/success tokens distinguish null from
empty. Each AUTH_CHALLENGE invokes the exchange object's challenge handler and
sends its returned AUTH_RESPONSE. AUTH_SUCCESS invokes the success handler, and
setup succeeds only if that handler succeeds. There is no fixed challenge-count
limit in this loop. Provider creation, challenge evaluation, success handling,
server errors, unexpected response kinds, and transport errors remain distinct
error causes.

The built-in username/password provider emits a zero byte, UTF-8 username, another
zero byte, and UTF-8 password. It does not select behavior based on the advertised
authenticator name. It accepts the success phase without interpreting its token
and rejects any challenge. Configuring this provider does not enable TLS; transport
encryption is a separate choice. A custom provider can implement challenge-based
authentication and is shared safely across simultaneous connection handshakes,
while each exchange has its own mutable state. The driver does not serialize calls
to a shared provider; within one exchange, it awaits each challenge handler before
proceeding. The success handler runs only for AUTH_SUCCESS. A failed or cancelled
handshake can drop its exchange without calling that handler; it is not a general
cleanup notification.

### TLS, verification, and session resumption

TLS is optional and requires an explicitly supplied backend configuration. Both
supported backends use the connected **IP address**, including any translation,
rather than the original contact-point DNS name as the expected server identity.
The driver supplies this IP to the TLS backend's verification mechanism.
Certificate trust, client certificates,
protocol versions, and backend-level verification policy come from the supplied
configuration. A certificate rejected by that configuration or a failed handshake
fails connection setup; the driver does not retry that connection in plaintext.

The recommended OpenSSL constructor takes a connector builder, preserving its peer
verification defaults. A constructor from a raw context builder is explicitly
dangerous because the underlying raw builder does not enable verification by
default. The driver still sets the expected IP, but that alone does not turn
verification on. A previously built legacy OpenSSL context remains accepted and
is deprecated; the driver cannot add its ticket callbacks to it.
Both managed constructors replace the builder's session-cache mode with client
caching without OpenSSL's internal store and install the driver's new-session
callback. A caller-supplied new-session callback is therefore not preserved by
these constructors; their handling of resumption is part of configuration setup.

The managed OpenSSL configuration enables resumption by default. Clones share
the cache, keyed by destination IP rather than port or host ID. Each node stores
the newest reusable TLS 1.2 session and at most eight TLS 1.3 tickets. TLS 1.3
tickets are consumed once, oldest first; overflow drops the oldest. When both
versions are cached, a TLS 1.3 ticket takes precedence; a TLS 1.2 session is
selected only if the TLS 1.3 queue is empty. At most 256
node entries are retained, evicting the least recently *stored-to* node on overflow.
Empty nodes are removed. Stored sessions are serialized so abrupt connection
teardown does not invalidate the cached session object.
Cache insertion and selection are serialized. A TLS 1.3 ticket is removed when
the per-connection TLS object selects it, before the handshake; another concurrent
connection cannot select that ticket. It is not restored if decoding, offering,
or the handshake fails, or if the handshake is cancelled. TLS 1.2 selection instead
copies the reusable session, so concurrent connections may offer the same session.
The reusable slot is named for TLS 1.2, but the implementation puts every
non-TLS-1.3 session there. Older versions permitted by a supplied OpenSSL
configuration therefore share that slot rather than receiving separate banks.

Disabling managed OpenSSL tickets suppresses both offering and banking tickets
on new connections made from that configuration value. The flag is copied per
configuration clone, while the cache is shared: disabling one clone neither clears
the cache nor disables other clones or callbacks on existing TLS objects.
Failure to allocate the callback index, serialize/decode
a ticket, or offer a ticket degrades to an ordinary full handshake. A ticket that
is not usable for the subsequently negotiated TLS version also falls back to a
full handshake. The connection selects only one cached entry: decoding or offering
it unsuccessfully does not try another ticket or the cached TLS 1.2 session.
This best-effort fallback does not weaken certificate verification.
The legacy built-context API does not use driver-managed resumption. Rustls uses
its supplied client's native resumption configuration; the driver does not add a
second cache. Pool filling does not wait for a supply of TLS 1.3 tickets, so opening
many connections can require full handshakes even when resumption is enabled.

TLS resumption does not resume a CQL authentication exchange: every new connection
still performs OPTIONS/STARTUP and creates a fresh exchange if AUTHENTICATE is
returned. The managed cache banks sessions through TLS callbacks independently of
CQL authentication. A later authentication or other CQL-setup failure does not
purge sessions already banked for that endpoint.

### Address translation and host filtering

An address translator receives the peer's host ID, advertised socket address,
datacenter, and rack, and asynchronously returns a connect address or an error.
It applies to eligible discovered peer endpoints, not original contact points or
endpoints marked untranslatable. A map translator requires a matching rule: a
missing rule is an error, not implicit identity translation. The string-map
implementation parses keys, ignores unparseable nonmatching keys, and reports an
invalid translated address when the matching value cannot be parsed.

Host filtering determines which discovered nodes may have pools and filters the
discovered-peer candidates for re-establishing the control connection. It is
stronger than a load-balancing preference, but it is not a prohibition on all
traffic to rejected hosts. Original contact points bypass that candidate filter,
and the post-fetch rejection check recognizes only discovered-peer endpoints.
A contact-point connection can therefore remain the control connection even when
the corresponding discovered peer is rejected. Established control connections
are not rechecked after ordinary metadata refreshes; a later rejection can disable
the query pool and remove future candidates without replacing that control
connection. The [metadata section](metadata-control-events.md#discovery-and-control-connection-establishment)
specifies these bootstrap and recovery boundaries.

Built-in choices accept all nodes, accept a given datacenter exactly, or use an
allow-list of socket addresses. The allow-list
resolves all supplied names at construction and fails on resolution errors. It
compares translatable peers against advertised addresses; an untranslatable peer
is accepted without that membership check. A datacenter filter rejects a peer
with no matching datacenter. Filtering does not rewrite the metadata's existence
of a node into evidence that it is physically down.
Allow-list construction uses synchronous address resolution and stores the
resulting address set. The session's asynchronous hostname-resolution timeout does
not bound this constructor, and later DNS changes do not refresh that set.

### Client routes

Optional client-routes configuration identifies one or more proxy connection IDs
and optional per-ID hostname overrides. The list must be nonempty; this constructor
does not additionally reject empty or duplicate ID strings. The specialized builder
disables advanced shard-aware-port use. Assigning the client-routes field directly
does **not** set that flag. At this revision, session validation rejects combining
client routes with user address translation or with TLS. The internal translator's
TLS-port support is therefore not proof that public client-routes sessions support
TLS.

The driver reads route records associated with configured connection IDs and uses
its own translator. A node must have a route; a missing route never falls back to
the advertised direct address. Mixed deployments requiring direct access for
some nodes and client-route access for others are unsupported. A missing required
port or failed DNS lookup is an explicit translation error.

For each host, one connection ID is sticky while it remains in route metadata;
the others are replacements if that route disappears, not a per-connection
round-robin plan. DNS failure, a missing port, or connection failure does not
rotate to another configured ID. The first choice among available IDs is
unspecified. Full snapshots remove absent hosts/routes and
preserve the sticky ID where possible. When that ID vanishes, an arbitrary
remaining route becomes sticky. Partial updates only alter addressed host/ID
pairs; a deletion can remove the host when no alternatives remain. Partial updates
for untracked IDs are warned about and ignored. A full snapshot returns hosts
whose sticky route changed or that gained their first route; a change confined to
alternative routes does not enter that refill set. A partial update returns a host
for every accepted non-deletion entry, even an identical or nonsticky route, without
checking its port or DNS usability. Deletion-only partial updates do not themselves
add a host to that returned refill set, even when they change its sticky route.
These returned hosts signal the metadata/pool machinery to reconsider refill.

A hostname override wins over the route's hostname but retains the route's port.
Resolution happens on every connection attempt with the per-lookup DNS timeout
and default-port fallback described above;
one returned address is selected randomly and is not saved as the permanent
resolved address. This permits later attempts to try another IP while the route
ID remains sticky. Each full replacement or partial merge holds the route-table
write lock for the entire update. Translation copies the selected hostname and
port under a read lock, then releases that lock before awaiting DNS. An update or
deletion during DNS does not invalidate the copied endpoint: that in-flight
translation can still return it, with no route recheck before connecting. This
does not authorize direct-address fallback when a lookup finds no route.
Route-event fetching and merge ordering belong to the metadata section.

### Identity and effective-configuration reporting

Each successfully initiated session has a freshly generated random UUID that is
stable for its lifetime and accessible to the application. Every connection sends
it as SESSION_ID, including pool and control connections. It is independent of
the optional client-supplied CLIENT_ID.

All connections advertise the built-in driver name/version unless overridden.
APPLICATION_NAME, APPLICATION_VERSION, and CLIENT_ID are sent only when supplied.
The identity API exposes those named fields rather than arbitrary STARTUP-option
replacement. Disabling configuration reporting does not suppress identity or
SESSION_ID.

Only a control connection carries DRIVER_CONFIG, enabled by default. It contains
a version-1 JSON report of effective connection, control-plane, and default-query
settings. It is rebuilt for each handshake so profile remapping and target-specific
metadata timeout support are reflected. The query portion acquires the default
profile once, so remapping during rendering does not mix settings from different
profile versions. A later remap does not update the report already sent by an open
control connection; a subsequent handshake produces a new report.

The server-metadata-timeout field describes the current target's negotiated
support. Metadata prepared statements are instead cached by original query text
across control-connection replacements: a cache hit keeps the timeout clause, or
its absence, from the original preparation. Reconnection between targets with
different support can therefore make the new report disagree with the cached
statement's actual text. Client metadata deadlines are assigned to a per-use
prepared-statement clone and do follow the current timeout configuration.

A serialization problem or report above
32 KiB logs a warning and omits the option; a diagnostic report must not prevent
connection establishment. Exactly 32 KiB is permitted by that size check.

The report describes recognized built-in retry/reconnection/speculation and
load-balancing policies, falling back to custom policy names when applicable.
Connection-node preference reflects recognized DC host filtering; query-node
preference reflects the recognized default load-balancing policy's own preference
or session fallback. For a custom load-balancing policy it reports the session
preference, without inspecting the custom policy's routing behavior.
These are different settings. Reporting shard-aware-port use also accounts for
per-host pooling, not merely the allow/disallow flag. Schema limitations mean some
runtime settings are intentionally absent: credentials and TLS configuration
contents are not serialized, TLS is an empty presence object, arbitrary host
filters are not described, and v1 has no fields for write-coalescing/heartbeat
detail. The report is not a complete restorable session configuration.

Reported request limits are 32768 in-flight requests per connection and an
orphan threshold of 1024. The latter cannot express the runtime age condition:
the connection closes when a periodic check finds **more than** 1024 orphans
older than one second; it is not a cap on every outstanding orphan. The report
does not include the configured number of pooled connections. Socket keepalive
is reported as an enabled boolean, without its idle duration. These diagnostic
fields describe configured capabilities and limits, not current connection counts
or proof that a server offers a shard-aware port.

Query defaults always report page size 5000 and non-idempotence, because neither
has a profile/session override; individual statements can change both, and unpaged
execution does not send a page size. Client timestamps report whether a generator
is configured. Disabled request timeouts and absent serial consistency omit their
respective fields. Retry policies report their recognized type or custom name,
without backoff or a global retry bound. Built-in reconnection reports omit a
maximum-attempt count. Speculation's `max-executions` is populated from the maximum
number of **additional** executions permitted by the policy.

A default load-balancing policy with token awareness disabled is reported by name
as custom. For a token-aware default policy, fixed replica shuffling seed reports
`replica-set` distribution and an unfixed seed reports `shuffle`; this describes
replica tiers, not every tier or LWT ordering. Latency awareness is reported only
as the `latency` adaptive-ordering signal, without its thresholds. The reported
fallback boolean follows the reported location: it is true without a location
and for a rack preference, while a datacenter-only preference uses the policy's
DC-failover flag. Thus a true value for a rack does not establish permission to
leave that datacenter. Empty datacenter names omit the location, and an empty rack
retains only its nonempty datacenter. These are report normalization choices and
do not rewrite the configured preferences.

Positive-millisecond report fields clamp zero/sub-millisecond values to one and
saturate at the integer maximum; fields allowing zero truncate to milliseconds.
The Scylla-only server metadata timeout is omitted for other targets and when its
wire millisecond value is zero. Schema-agreement timeout preserves zero. Zero
socket-buffer hints are omitted even though explicit zero has runtime meaning.
The buffer report checks for zero before the same unsigned 32-bit truncation used
by the socket call: on wider platforms a positive multiple of 2³² therefore emits
`size-bytes: 0`, contrary to the report schema's positive-integer requirement.
The runtime reporter does not validate the completed document against that schema;
its serialization and size checks do not reject this case. Linger is represented
in whole seconds. A built-in speculative policy whose
maximum retry count is zero is omitted because it never fires. A percentile
policy with a nonfinite percentile or one outside the schema's exclusive 0–100
range is reported generically by name, rather than with invalid numeric details.
These report normalization rules do not modify runtime behavior. Custom-policy
default names strip the outer type's module path while preserving paths inside
generic arguments and never deliberately return an empty name.
An explicitly empty custom load-balancing policy name is reported as `unknown`.
That substitution does not apply to the hidden `reported_name` hooks of custom
retry, reconnection, and speculative-execution policies. An override returning an
empty string is serialized unchanged, producing another schema-invalid report
that the runtime serialization and size checks do not reject.

### Metrics

Metrics are an optional compile-time capability. Without it, internal recording
is a no-op and public metrics-query APIs are absent; a caller is not presented
with meaningful-looking zero counters. With it enabled, all metric handles/clones
share one session's counters and latency histogram.
These are independently updated and read, without one atomic snapshot spanning
counts, rates, and latency. A single histogram snapshot computes its statistics
from one loaded histogram; separate getter calls can observe different concurrent
activity. A retained metrics handle keeps that storage alive independently of
the session value.

Unpaged, manually paged, and automatically paged request/error counts are separate.
An execution attempt increments the relevant request count after a connection is
chosen, before per-attempt preparation or serialization. It can fail without
sending the final QUERY, EXECUTE, or BATCH frame; such a failed attempt still
increments its error count. A retry is
another counted attempt, and each automatic page can therefore contribute more
than once when retried. A connection-selection failure alone does not count as a
failed request. The retry counter increments when the retry policy chooses same-
or next-target retry, not simply when a speculative branch starts. A client
request deadline has a separate timeout counter; expiring that deadline drops
pending attempt work without incrementing its attempt-error counter or recording
its latency. Successful attempts contribute
millisecond latency; unsuccessful attempts and time spent selecting a connection
are not histogram samples. The histogram is configured with maximum-value power
16 and grouping power 12 (documented maximum 65535 ms); out-of-range recording
can fail. Histogram recording errors are ignored by execution.

These counts describe the instrumented execution core, not every protocol frame
or every public API failure. Explicit PREPARE and control-connection metadata
requests bypass these request counters. Failure before entering the core, such as
prepared-value serialization failure, adds no request or error count. Preparation
or repreparation inside an attempt does not create a separately counted attempt.
Conversely, trace-table lookups use ordinary session query execution and contribute
to its unpaged counters. A retry policy that ignores a write error still leaves
the attempt-error count incremented and records no successful-attempt latency for
that attempt, even though the overall request returns success.

The active-connection gauge is updated by pool connection lifecycle paths. At
this revision the control-connection implementation has no corresponding metric
increments, so this gauge must not be treated as a census of every session socket.
It can also overcount pool sockets: a successfully opened shard-targeted connection
is counted before admission, but the path that discards it because its actual
shard is already full starts an ordinary replacement without decrementing the
discarded connection's count.
Connection timeout counts are recorded on the applicable pool-open failure path.
Deprecated aggregate query/error getters combine unpaged and manually paged counts;
the automatic-paging aggregates remain separate.

Latency queries expose average, percentile, and a snapshot with minimum, maximum,
mean, standard deviation, median, and 75/95/98/99/99.9 percentiles. Values are
histogram approximations: means/quantiles use bucket midpoints and min/max use
occupied bucket boundaries. Empty histograms return an explicit empty error;
invalid percentile requests can return a histogram error. The public percentile
scale is 0–100. Mean request rate is total counted attempts divided by elapsed
time. One-, five-, and fifteen-minute rates use five-second EWMA ticks. Ticking
is driven by recording a request, not by rate getters or a background timer, so
reading an idle meter does not itself decay the stored moving averages.

### Request history, logs, and server tracing

A statement can attach a history listener. It records logical request start and
success/error, speculative branch creation, attempt start with actual connect
address, and attempt success/error including the retry decision. The listener
must produce unique IDs for requests, branches, and attempts. Automatic paging
creates a separate request history for each page fetch. A request deadline records
a request error while leaving cancelled attempts without an outcome; dropping the
request future externally can also leave its request outcome absent. The public
listener documentation warns consumers to tolerate speculative events after a
request finishes. That is a broader contract than the pinned executor demonstrates:
its speculative branches are futures polled in the same task, and unfinished
branches are dropped before the terminal request callback. They are not detached
tasks that continue producing history after that callback. Other requests and
automatic page fetches can still call the same listener.
Callbacks run synchronously in the executing task, without a separate callback
queue; independent requests can call a shared listener concurrently. Request-start
runs before the execution deadline wrapper, and the terminal callback runs after
that wrapper resolves.

Automatic paging fetches pages after the first in a detached task. Dropping the
pager does not abort a page already being fetched: the worker observes a closed
receiver when it tries to send the completed page or error. Its page metrics and
history can therefore continue after the pager is dropped. This differs from
dropping the directly awaited execution future described above.

Request history starts and ends in the execution core. Explicit PREPARE has no
separate request history, and a failure before entering that core can produce no
history at all. Attempt-start history precedes per-attempt preparation/serialization
and is not proof that a final request frame was sent. A request-success event
precedes keyspace propagation, automatic
schema-agreement handling, and subsequent result/page validation. Errors in those
later steps can therefore make the public operation fail after its history already
reports success; they do not retroactively add an attempt-error count or remove its
latency sample. Ignoring a write error records an attempt error with that retry
decision followed by request success.

The built-in collector stores UTC timestamps and events behind a mutex. Cloning
collected data leaves the collector intact; taking data drains events while
preserving ID counters. There is no built-in bounded retention.
Taking or cloning data is serialized with event insertion, but does not wait for
active requests or close the collector. A later drain can contain child/outcome
events whose request or attempt start was in an earlier drain. Structured history
groups attempts under ordinary/speculative branches and requests; incomplete
history is representable with missing outcomes. Events whose parents are absent
can be omitted from the structured representation. A poisoned collector logs a
warning and uses temporary dummy storage rather than panicking, so collection and
ID continuity are not guaranteed after poisoning.

Driver logs and instrumentation use the tracing ecosystem; the application
installs its subscriber/logger. Request spans distinguish unprepared, prepared,
batch, and control-connection requests. Depending on the path they include query
text, decoded partition-key values and token, sizes, replicas, shard, and a count
of speculative executions recorded on span drop. Failed partition-key formatting
produces a decoding-error placeholder. Iterator formatting clones the iterator on
each formatting call, permitting multiple subscribers to format the same field
without consuming it once. Query text and partition keys can therefore
appear in enabled traces; there is no blanket driver-side redaction guarantee.
The ordinary request constructors declare `result_size` and `result_rows`, but
the pinned implementation never records either field. It instead tries to record
`raw_result_size`, which those constructors do not declare, so that recording does
not add the field to their spans. These declarations are not evidence of emitted
result-size or row-count diagnostics. Control-connection spans declare only their
kind, query contents, and request size; ordinary shard/speculation recording calls
do not add those undeclared fields to a control-connection span.

Server warnings are logged at warning level after body-extension parsing, before
the main response body is deserialized. Successfully returned ordinary query
results retain warnings and tracing IDs. Automatic paging retains page tracing IDs
but does not expose the page warnings; conversion of a server error to the public
error also discards its warnings and tracing ID. Custom payloads exist only on the
internal response and can supply tablet-routing updates; they are dropped by
non-error response conversion and are not exposed on public query results.
Server-side query tracing is enabled per statement and is distinct from local
instrumentation and history collection. Trace IDs from results, prepares, and
individual pages can be used to fetch database tracing records.

Trace lookup concurrently queries the sessions and events tracing tables. A result
is ready only when a session row exists, its duration is present, and at least one
event exists. These are independent reads, not one database snapshot; this
readiness condition does not establish that every event has arrived.
An absent/incomplete result is retried up to the configured nonzero
attempt count, sleeping after each incomplete attempt including the final one.
Query failures propagate immediately. Type-check and deserialization errors
propagate when their result is inspected, but the events result is inspected only
after a session row with a duration exists. Consequently an incomplete session can
defer discovering malformed events. Exhaustion returns an empty-results
tracing error. The internal statements use the configured consistency, are marked
idempotent, and carry page-size setting 1024 while the lookup executes them via
the unpaged path. Event activity, source, elapsed time, and thread can be absent;
the helper listing involved nodes deduplicates available event source addresses.

## Rust implementation evidence

All links below pin the source revision; local test targets later in this file are
relative to the Rust reference checkout, not claims about tests already run here.

| Concern | Pinned entry points |
| --- | --- |
| Complete configuration and defaults | [SessionConfig](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/client/session.rs#L207), [defaults](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/client/session.rs#L469), [validation](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/client/session.rs#L618), [connect](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/client/session.rs#L1407) |
| Startup partial success | [contact-point expansion and partial DNS failures](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/cluster/node.rs#L415), [candidate failures and initial metadata fallback](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/cluster/metadata/cc_establisher.rs#L261) |
| DNS fallback deadline and error precedence | [per-lookup timeout and original-error fallback](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/cluster/node.rs#L326) |
| Metadata deadline precedence | [explicit/derived client timeout and target-gated server timeout](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/cluster/control_connection.rs#L45) |
| Metadata report/cache boundary | [query-text cache hit precedes server-timeout rendering](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/cluster/control_connection.rs#L175), [current-target timeout report](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/client/driver_config.rs#L843) |
| Builders and profiles | [builder variants](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/client/session_builder.rs#L36), [build clones config](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/client/session_builder.rs#L852), [profile defaults](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/client/execution_profile.rs#L174), [shared remapping](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/client/execution_profile.rs#L494) |
| Profile snapshot boundaries | [one profile acquisition per ordinary query](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/client/session.rs#L1664), [one settings capture per pager](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/client/pager.rs#L149), [one profile acquisition per report](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/client/driver_config.rs#L881) |
| Authentication | [provider and exchange traits, plaintext provider](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/authentication/mod.rs#L13), [challenge loop and errors](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/network/connection.rs#L823) |
| TLS | [backend dispatch](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/network/tls.rs#L128), [ticket storage](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/network/tls/openssl.rs#L17), [managed constructors and callbacks](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/network/tls/openssl.rs#L195), [IP verification and handshake](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/network/connection.rs#L1559) |
| TLS cache concurrency | [locked selection consumes TLS 1.3 tickets](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/network/tls/openssl.rs#L145), [shared cache and copied enablement flag](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/network/tls/openssl.rs#L183), [per-connection selection and callback enablement](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/network/tls/openssl.rs#L271) |
| TLS version and authentication boundaries | [all non-TLS-1.3 sessions share the reusable slot](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/network/tls/openssl.rs#L115), [banking callback independent of CQL](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/network/tls/openssl.rs#L234), [CQL startup follows TLS on every connection](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/network/connection.rs#L2136) |
| Socket and startup options | [identity options](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/network/connection.rs#L240), [handshake and compression negotiation](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/network/connection.rs#L2136), [socket configuration](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/network/connection.rs#L2354) |
| Translation/filtering | [translator contract and built-ins](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/policies/address_translator.rs#L103), [host filters](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/policies/host_filter.rs#L15) |
| Host-filter control-connection limits | [contact-point fallback](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/cluster/metadata/cc_establisher.rs#L214), [peer-only post-fetch rejection](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/cluster/metadata/cc_establisher.rs#L380), [established-connection refresh processing](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/cluster/metadata/worker.rs#L784) |
| Client routes | [configuration](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/client/client_routes.rs#L78), [snapshot and partial merge](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/client/client_routes.rs#L204), [translation](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/client/client_routes.rs#L382) |
| Identity/reporting | [identity fields](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/client/self_identity.rs#L3), [report schema/types](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/client/driver_config.rs#L43), [normalization](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/client/driver_config.rs#L490), [report rendering](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/client/driver_config.rs#L725) |
| Report interpretation limits | [connection fields](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/client/driver_config.rs#L758), [query and routing policy fields](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/client/driver_config.rs#L881), [runtime orphan cutoff](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/network/connection.rs#L1829) |
| Empty custom policy-name limits | [reconnection name passed through](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/client/driver_config.rs#L798), [speculative/retry names passed through](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/client/driver_config.rs#L991), [schema nonempty-string requirement](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/client/driver-config-schema.json#L39) |
| Metrics | [counters, rates, histogram](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/observability/metrics.rs#L234), [feature-disabled no-op](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/observability/metrics_noop.rs#L1), [actual attempt instrumentation](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/client/execution.rs#L560) |
| Instrumentation lifetime and snapshots | [histogram snapshot and independent counter getters](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/observability/metrics.rs#L407), [collector drain and lock](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/observability/history.rs#L152), [pager detects closure after fetching](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/client/pager.rs#L202), [detached paging worker](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/client/pager.rs#L919) |
| Observability boundaries | [deadline and terminal history](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/client/execution.rs#L513), [history per automatic page](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/client/pager.rs#L306), [pool-open metric increment](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/network/connection_pool.rs#L1135), [discarded shard-targeted connection](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/network/connection_pool.rs#L1050) |
| Outcome and response-metadata limits | [side effects after history completion](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/client/session.rs#L2496), [control requests without metrics/history](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/client/pager.rs#L553), [response conversion drops custom payloads and error metadata](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/response/request_response.rs#L33) |
| Listener contract versus execution | [public late-event warning](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/observability/history.rs#L38), [local speculative futures and cancellation](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/policies/speculative_execution.rs#L187), [terminal callback after execution](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/client/execution.rs#L531) |
| History/logs/warnings | [listener and collector](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/observability/history.rs#L40), [request spans](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/observability/driver_tracing.rs#L21), [warning parsing](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/network/connection.rs#L1487) |
| Database tracing | [trace data types](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/observability/tracing.rs#L17), [lookup loop](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/client/session.rs#L2380), [readiness checks](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/client/session.rs#L2427) |
| Compile-time capabilities | [Cargo feature declarations](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/Cargo.toml#L34) |
| Module exports and formatting | [client exports](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/client/mod.rs#L1), [public crate surface](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/lib.rs#L279), [feature-dependent observability exports](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/observability/mod.rs#L1), [policy exports and naming](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/policies/mod.rs#L1), [repeatable iterator formatting](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/utils/safe_format.rs#L1), [utility-module exports](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/utils/mod.rs#L1) |

## Subfeatures

### Configuration ownership

Configuration can contain shared `Arc` policy/provider objects. Execution profiles
use an immutable inner value behind `Arc<ArcSwap<...>>`; cloned handles share the
remapping cell. A builder is not consumed by `build`. No general runtime mutation
API is implied for settings captured at connect time. A failed initial keyspace
operation fails session creation. Subsequent `use_keyspace` and the limitations of
the locally cached keyspace name are covered in execution/connections.

The builder publicly exposes its configuration for inspecting or setting options
not represented by a convenience method. Profiles expose getters and can seed a
new builder with all current settings. A handle can likewise produce a profile
snapshot or a builder from its current pointee; later handle remapping does not
change those previously obtained values. Optional handle labels appear in debug
output only: equal labels do not join remapping groups, and remapping the profile
does not replace a handle's label.

### Feature availability

No optional Cargo features are enabled by default. `openssl-010` and `rustls-023`
enable their TLS providers. `metrics` enables public metric collection and the
percentile-based speculation capability. `chrono-04`, `time-03`, `num-bigint-03`,
`num-bigint-04`, `bigdecimal-04`, `secrecy-08`, and `secrecy-10` enable integrations
with those external value types. `full-serialization` groups these external-type
features; it does not include the separate `serde` flag. The latter enables serde
representations whose shape is explicitly outside the driver's semantic-version
guarantee. Actual CQL conversion rules remain in serialization/protocol.
Configuration JSON reporting, history collection, local driver tracing, and
server-side tracing do not require either `metrics` or `serde` to be enabled.

Experimental flags include testing, C++/Node.js/Python/C# adapters, host listeners,
reconnection policy, client routes, and strong consistency. Several APIs additionally
require the `scylla_unstable` compiler configuration; enabling a similarly named
Cargo flag is not universally sufficient. In particular host listeners and custom
reconnection configuration use both gates, while the session's client-routes
configuration field and specialized builder use the feature gate. The
`client_routes` module and its `ClientRoutesConfig`/`ClientRoutesProxy` value types
are publicly exported even without that feature. Adapter surfaces are not evidence
of a stable general-purpose API.

## Invariants

- Authentication exchange state belongs to a connection; providers may be shared.
- TLS handshake failure never silently downgrades to plaintext. Resumption failure
  can fall back to a full TLS handshake using the same verification configuration.
- Missing client-route metadata never authorizes a direct-address connection.
  A route update is not a revocation barrier for translations already awaiting DNS.
- Host filtering, routing preference, and address translation have different jobs
  and must not be treated as interchangeable policy settings. Host filtering is
  not a guarantee that bootstrap or retained control traffic avoids rejected hosts.
- SESSION_ID is stable across all connections of one session, and report opt-out
  changes neither that identifier nor the configured identity fields.
- Diagnostics must not claim the report is complete or counters count only logical
  operations. The public listener's warning about late events is distinct from
  the pinned speculative loop's cancellation ordering.
- Successful execution-core history does not guarantee that later result handling
  and side effects let the public API operation succeed.
- Draining history or dropping a pager is not a barrier against later instrumentation.
- Disabled metric collection removes public metrics-query capability rather than returning
  a fabricated working metrics object.
- Source limitations above are reference behavior; any deliberate Go divergence
  must be specified separately rather than silently edited into this reference.

## Test places

### Rust unit tests: source-level algorithm, type, protocol, and state-machine tests.

#### Description

Verify selected configuration assignments/defaults, zero-keepalive rejection,
sequential profile remapping in reports, reporting normalization, TLS ticket
bounds, client-route state changes, histogram calculations, and history grouping
independently of a database. The OpenSSL tests use a local TLS server; the linger
report test inspects JSON without observing socket-close behavior. OpenSSL unit
tests require `openssl-010`; histogram tests and percentile-policy report cases
require `metrics`. The report's separate schema-key coverage sweep also requires
`metrics`, so that its test configurations can reach the percentile-policy branch.

#### Test cases

- Empty contact list and absent compression in a new builder, plus compression assignment: scylla/src/client/session_builder.rs:default_session_builder,compression
- Zero CQL keepalive interval returns IllegalConfig with the expected message: scylla/src/client/session_builder.rs:zero_keepalive_interval_is_rejected
- Builder retains default/configured consistency, serial consistency, timeout, and load-balancing policy name: scylla/src/client/session_builder.rs:execution_profile
- Default report and remapped-profile report describe effective settings: scylla/src/client/driver_config_tests.rs:default_configuration_report,remapping_the_default_profile_changes_the_report
- Selected report configurations conform to schema, and exact-size/over-limit report boundaries are checked: scylla/src/client/driver_config_tests.rs:every_report_conforms_to_the_schema,oversized_reports_are_omitted
- Schema keys omitted across the report configuration sweep match an explicit allowed-omission set in both directions; this does not prove every runtime setting is represented: scylla/src/client/driver_config_tests.rs:every_reportable_schema_key_is_reported
- Token-aware/custom policy reporting, rack/DC fallback distinctions, replica-distribution reporting, normalized locations, and empty custom policy names are checked: scylla/src/client/driver_config_tests.rs:default_load_balancing_policy_matrix,load_distribution_follows_replica_shuffling,effective_node_preference_is_reported,custom_load_balancing_policies_are_reported_by_name
- Disabled query-default fields and the additional-execution limit in speculation reports are checked: scylla/src/client/driver_config_tests.rs:unset_query_defaults_are_omitted,speculative_execution_report_cases
- Zero/submillisecond normalization and pool mode reporting remain explicit: scylla/src/client/driver_config_tests.rs:millisecond_helpers_handle_zero_and_sub_millisecond_durations,shard_aware_port_is_not_reported_as_enabled_for_a_per_host_pool
- Linger reporting yields zero seconds for 0/1/999 ms and one second for 1999 ms: scylla/src/client/driver_config_tests.rs:sub_second_linger_is_reported_as_zero
- TLS tickets resume connections, remain node-specific, and tolerate unusable entries: scylla/src/network/tls/openssl.rs:banked_sessions_are_offered_and_resumed,banked_sessions_are_not_offered_to_another_node,an_unusable_banked_session_falls_back_to_a_full_handshake
- TLS 1.2 sessions are reusable and TLS 1.3 tickets/cache bounds hold: scylla/src/network/tls/openssl.rs:tls12_session_is_reusable,tls13_ticket_queue_is_bounded_fifo_and_single_use,node_keys_are_bounded_and_evict_the_stalest_node
- Mixed-version ticket banks prefer TLS 1.3 and do not try a second entry after decode/offer failure: missing
- Concurrent TLS 1.3 selection consumes each ticket once, cancelled handshakes do not return tickets, and disabling a clone leaves the shared cache and existing connections intact: missing
- Older TLS versions allowed by the backend share the managed cache's reusable non-TLS-1.3 slot: missing
- Resumed TLS connections still authenticate independently, and CQL setup failure does not purge already banked TLS sessions: missing
- Missing client routes fail and hostname overrides take precedence: scylla/src/client/client_routes.rs:translate_address_errors_when_no_client_routes,translate_with_client_routes_uses_overridden_hostname
- Snapshot replacement removes absent hosts while partial updates preserve unrelated hosts: scylla/src/client/client_routes.rs:replace_client_routes_removes_hosts_absent_from_snapshot,merge_client_routes_update_preserves_hosts_absent_from_update
- Snapshot refill sets omit unchanged hosts and include sticky-ID changes; partial refill sets include updates and omit deleted hosts: scylla/src/client/client_routes.rs:replace_omits_unchanged_host,replace_returns_host_when_sticky_connection_id_changes,merge_returns_updated_host,merge_omits_deleted_host
- Full-snapshot alternative-only changes and identical/nonsticky partial updates have distinct refill-set behavior: missing
- Client-route changes during DNS do not revoke the endpoint already copied by a translation: missing
- Histogram statistics for 0–100, public percentile scale/endpoints and empty-histogram error, and ordering across four seeded distributions: scylla/src/observability/metrics.rs:characterization_known_distribution,regression_get_latency_percentile_ms,test_snapshot_ordering
- Exact large-bucket midpoint/boundary expectations, invalid percentile behavior, and out-of-range latency recording: missing
- Manually inserted history events represent incomplete requests, failed attempts, and speculative branches: scylla/src/observability/history.rs:empty_request,two_error_atempts,empty_fibers,complex
- Explicit direct-config rejection of client-routes/TLS and client-routes/custom-translator combinations: missing
- Socket-buffer values above 2³²−1 truncate without configuration rejection; positive multiples of 2³² can produce a schema-invalid report: missing
- Empty custom retry/reconnection/speculation `reported_name` overrides produce schema-invalid reports without the load-balancing name substitution: missing
- Complete challenge-loop error and null-versus-empty token matrix: missing
- Concurrent provider calls keep exchange state separate, and cancelled authentication does not call the success handler: missing
- Idle EWMA getters do not tick and active-connection gauge excludes the control socket: missing
- Discarding an excess shard-targeted connection can leave the active-connection gauge overstated: missing
- Incomplete tracing sessions defer parsing malformed event rows: missing
- Deadline and externally cancelled requests preserve incomplete attempt/request history: missing
- Pinned speculative execution drops unfinished branches before the terminal history callback, while listener consumers tolerate the broader documented event contract: missing
- Automatic pager drop allows an in-flight page to finish its instrumentation, and collector drains can split parent and child events: missing
- Concurrent metric getters do not imply a global snapshot; all statistics within one histogram snapshot use the same loaded histogram: missing
- Profile remapping during requests, pager iteration, and report generation respects each acquisition boundary: missing
- Profile/handle inspection returns independent snapshots, while debug labels neither merge remapping groups nor change on remapping: missing
- Allow-list DNS is resolved synchronously once and is not controlled by the session DNS timeout: missing
- Rejected contact points or later filter changes can leave a retained control connection, while discovered query pools follow the filter: missing
- A rebuilt control report reflects current target support even when cached metadata query text retains an earlier target's server-timeout clause: missing
- Managed OpenSSL constructors replace caller-supplied new-session callbacks and session-cache modes: missing
- One unfinished DNS lookup holds up the initial group even with an explicit socket contact point: missing
- Default-port DNS fallback receives a second timeout budget and preserves the first error when that fallback fails or times out: missing
- Ignored write errors retain attempt-error metrics without successful-attempt latency; post-core side-effect or result-validation errors do not rewrite successful history: missing
- PREPARE/control requests, pre-core serialization failures, and trace-lookup queries have distinct instrumentation coverage: missing
- Query-result warning/tracing-ID access and preservation when a non-row result is returned inside a rows-conversion error: scylla/src/response/query_result.rs:test_query_result,test_query_result_returns_self_if_not_rows
- Pager/error warning and tracing-ID loss, and internal custom-payload removal at response conversion: missing
- Trace readiness does not require a consistent two-table snapshot or a complete event set: missing
- Every declared local tracing span field is either emitted or explicitly documented as unavailable: missing

### Rust integration tests: database/proxy tests of externally visible behavior.

#### Description

Exercise actual handshake bytes, database authentication and certificates, route
changes, execution-profile selection and sequential timeout remapping, ordinary
trace-table retrieval, metrics, and history.

The CCM TLS and ticket modules compile only when both `openssl-010` and
`rustls-023` are enabled; the metrics module requires `metrics`, and the CCM
client-routes module has an `unstable-client-routes` gate. Cargo feature
unification matters: the driver's CCM bridge dev-dependency itself enables
client routes. CCM tests create/start clusters; the ordinary proxy helper uses
three configured cluster endpoints. These are environment requirements, not
evidence of a successful local run. The `system.clients` test also has the skip
conditions stated below.

#### Test cases

- Built-in and custom providers connect and run DDL against password authentication; the custom provider rejects challenges: scylla/tests/integration/ccm/authenticate.rs:authenticate_superuser_cluster_one_node,custom_authentication_cluster_one_node
- Both OpenSSL APIs and Rustls connect with trusted IP SANs, reject a mismatched IP SAN with a session-build error, and connect with client credentials; the missing-client-certificate rejection case uses Rustls only: scylla/tests/integration/ccm/tls.rs:test_connect_tls_no_client_auth,test_tls_verifies_hostname,test_connect_tls_with_client_auth
- Rejection of an untrusted CA and missing client credentials for each OpenSSL API, with the specific TLS failure cause checked: missing
- With server tickets enabled, both TLS versions are pinned: managed OpenSSL/Rustls resume all connections after the first per node for TLS 1.2 and at least two per node for TLS 1.3; disabled managed OpenSSL and legacy OpenSSL offer no resumption and resume none: scylla/tests/integration/ccm/tls_tickets.rs:test_tls_session_tickets
- Default/custom identity fields are checked in one captured STARTUP per identity configuration: scylla/tests/integration/session/self_identity.rs:self_identity_is_set_properly_in_startup_message
- The expected initial connection-frame batch carries the session UUID throughout and exactly one version-1 report; opt-out removes reports while preserving UUIDs. The test counts reports without independently identifying the control socket: scylla/tests/integration/session/startup_options.rs:session_id_is_sent_on_every_connection,driver_config_is_sent_only_on_the_control_connection_and_can_be_disabled
- Control-reconnection STARTUP preserves session identity and regenerates the report after default-profile remapping: missing
- Effective configuration is readable in database client metadata (ignored for Cassandra; returns early when `system.clients.client_options` is absent): scylla/tests/integration/session/startup_options.rs:driver_config_is_readable_from_system_clients
- Session/statement profile selection and statement consistency override affect query, execute, and batch policy callbacks; this test changes handles rather than remapping them: scylla/tests/integration/statements/execution_profiles.rs:test_execution_profiles
- Remapping a cloned session-default handle changes subsequent query/execute/batch timeout behavior, and statement timeouts override that default: scylla/tests/integration/statements/request_timeout.rs:test_request_timeout
- Invalid contact points do not prevent use of other valid contact points: scylla/tests/integration/session/new_session.rs:proceed_if_only_some_hostnames_are_invalid,all_hostnames_invalid
- Client routes survive remapping, topology changes, and reroute events: scylla/tests/integration/ccm/client_routes.rs:test_client_routes_nlb_port_remap,test_client_routes_multi_dc_topology_change,test_client_routes_event_driven_reroute
- Successful and forged-error query/execute calls increment exactly their own request/error counters across unpaged/manual/automatic APIs, using one-page results and a no-retry policy: scylla/tests/integration/session/metrics.rs:test_paged_and_unpaged_metrics_are_counted_separately
- Counter increments across later automatic pages, retried pages, speculative attempts, and connection-selection failures are asserted together with timeout/latency outcomes: missing
- Request history records success, failure, and pagination: scylla/tests/integration/session/history.rs:successful_query_history,failed_query_history,pager_query_history
- Single-page query/execute pagers leave exactly one successful request/attempt and no extra unfinished history request: scylla/tests/integration/session/history.rs:pager_single_page_history
- Ignored errors on the first and later automatic pages keep the failed-attempt event and close the page request as successful: scylla/tests/integration/session/history.rs:pager_ignore_write_error_history
- Query/execute/prepare/batch and initial pager tracing IDs are present only when enabled and have session-table rows; one ordinary query also checks get_tracing_info fields/events. Pager helpers use an empty table and do not fetch later pages: scylla/tests/integration/session/tracing.rs:test_tracing
- Default trace-readiness budget (10 attempts/3 ms), deterministic incomplete-result/exhaustion paths, and accumulating tracing IDs across later pager pages: missing
- Unsupported requested compression falls back without changing other connections' negotiation: missing

The tracing integration session helper overrides lookup readiness to 200 attempts
and 50 ms intervals. Its separate session-table polling helper also uses 200
attempts and 50 ms; this test therefore does not validate the driver's default
readiness budget. History display tests normalize timestamps, node addresses,
and database-error text before comparison; they establish event structure and
outcomes without asserting those original runtime values.

### Go unit/race tests: API contracts, codec boundaries, concurrency, and ownership.

#### Description

The new Go design must specify deliberate differences before implementation and
then check configuration snapshots, policy-handle concurrency, per-connection auth
state, TLS verification, reporting, metrics accounting, and late listener events.

#### Test cases

- Go configuration and diagnostics contracts are mapped to explicit Rust behavior or documented divergences: missing
- Concurrent authentication, profile replacement, listener draining, and metrics snapshots preserve ownership: missing
- Missing route and failed TLS verification cannot silently choose an unintended transport: missing

### Go integration tests: the existing gocql suite migrated to the new API.

#### Description

Preserve upstream authentication, TLS, event/listener, routing-policy,
configuration, trace, and metrics assertions while migrating their API calls.
The migration inventory and actual database runs are separate completion gates.

#### Test cases

- Existing integration behavior for configuration, authentication, transport security, tracing, and observability passes through the new API: missing

### Go allocation benchmarks: encoding, decoding, request construction, paging, routing, and connection selection hot paths with explicit ownership boundaries.

#### Description

Disabled and enabled instrumentation are distinct benchmark modes. Setup-only
allocations, retained history, optional trace formatting, and copying for ownership
must not be concealed inside a claim about the zero-copy request path.

#### Test cases

- Disabled diagnostics add no per-value copy and measured request-path allocation budgets are explicit: missing
- Enabled metrics and callback instrumentation preserve borrowed-buffer lifetime without retaining invalid views: missing

## Test generation notes

Exercise empty contact lists, partial DNS failures, zero/submillisecond timeouts,
nonzero-type boundaries, socket-buffer truncation at 2³² and its report-schema
consequence, metadata timeout precedence and saturating derivation, both TLS providers, wrong
IP SANs, untrusted roots, mutual TLS, multiple null/empty authentication challenges,
ticket cache exhaustion/churn, mixed-version ticket selection, and simultaneous
handshakes. Exercise TLS handshakes followed by failed CQL authentication and backend-enabled older
versions sharing the reusable cache slot. Include empty custom policy-name hooks,
default-port lookup fallback budgets, and the original-error precedence. Include multiple
connection IDs, route removal/replacement, hostname override, missing ports,
multi-address DNS, updates during a pending lookup, incomplete bootstrap lookup
groups, and absence of direct fallback. Verify opt-out, report-size boundaries,
profile remapping during requests and across pager pages, the next control-handshake
report, ticket cancellation and clone-local disabling, and custom policies.
Include profile-inspection snapshots and labels, allow-list construction and stale
DNS entries, rejected contact-point and retained-control exceptions, cached metadata
query text after a target-support change, managed TLS callback replacement, and report routing-field
normalization. Check the schema-key coverage sweep as well as schema conformance;
their scopes differ, and neither establishes runtime coverage of every setting.
For observability distinguish a logical request from an attempt, page, retry,
speculative branch, and failed connection selection. Check empty histograms,
invalid percentiles, idle rate meters, concurrent metric reads, late history
callbacks, drains that split requests, instrumentation after pager drop,
warnings on successful responses, incomplete tracing rows, and immediate trace
query failures, and event-deserialization failures both before and after session
readiness. Include ignored-write success, failures after successful execution-core
history, failures before history starts, and the different warning/custom-payload
retention paths. These source targets were inspected; this draft
does not claim that their test suites were executed.
