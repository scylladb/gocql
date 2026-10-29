# Statements, execution, paging, retry, and speculation

## Summary

This section specifies the statement-to-result behavior of the Rust reference at
`f8b0ee928dc0dcb475247dc82cce4e42804b0621`. It describes implemented behavior,
including current limitations. It does not prescribe the future Go API.
The execution layer resolves request settings, prepares and binds statements,
chooses a query plan, performs attempts and recovery, applies a client deadline,
processes response side effects, and exposes results or a paged stream.

The companion [routing](routing.md), [connection](connections.md),
[metadata/control/events](metadata-control-events.md),
[serialization/protocol](serialization-protocol.md), and
[configuration/security/observability](configuration-security-observability.md)
sections specify the underlying mechanisms and their broader configuration.

## Language-agnostic behavior

### Statement forms and explicit paging choice

The public execution surface distinguishes an unprepared query, a prepared
statement, a prepared statement bound to serialized values, and a batch. A query
or prepared statement has three explicit execution modes:

| Mode | Observable contract |
| --- | --- |
| Unpaged | The stable unpaged APIs request all results in one response, irrespective of configured page size. A response claiming further pages is an error; the driver never silently discards the continuation. |
| Single page | Fetches one page and returns both a result and an opaque continuation indicating completion or more pages. The caller decides whether and when to continue. |
| Automatic paging | Fetches the first page before returning a pager; later pages are fetched by a background worker and consumed as a typed row stream. |

Batch execution is unpaged. Batches do not support SELECT statements; the server
validates statement legality and batch-type restrictions. A result-producing
conditional batch is still possible. The driver does not interpret CQL strings to
infer idempotence. Every statement and batch is treated as not known to be
idempotent by default.

On servers supporting SELECT without FROM, expressions can be queried or
prepared without a user table or schema metadata. Result names and types come
from the server, including aliases and function-derived names. Parameters still
require server-inferred types. The driver propagates rejection by servers without
the extension; it does not rewrite such queries or infer a fictitious user-table
routing key.

The default page size is 5,000. Setters accept positive signed 32-bit values only;
nonpositive sizes cause a programmer-error failure, not a database request.
Page size is a server request parameter, not a guarantee that every returned
page has exactly that many rows. Empty pages with a continuation are consumed
until a nonempty page, final completion, or error is reached.

Manual paging begins with a start state and accepts a returned continuation on
the next single-page call. Continuation bytes can be exported, retained, and
restored; the driver neither interprets them nor binds them to a query, values,
or session. Restoring bytes does not validate that they belong to the requested
query. A start state has no bytes; an explicitly supplied empty byte sequence is
still a continuation, and a response containing it still means more pages.
Completion is a separate response state, not a reusable start token. Each manual
page call resolves its own profile and cluster snapshot and can use a different
coordinator; it does not inherit the automatic pager's coordinator preference.

### Configuration and execution-profile resolution

When request configuration is resolved, a statement-specific execution-profile
handle takes precedence over the session's default handle. Individual statement overrides
then take precedence over the selected profile. The default effective settings
are:

| Setting | Default and override rule |
| --- | --- |
| Regular consistency | Local quorum; statement value overrides profile. |
| Serial consistency | Local serial; a statement can inherit, explicitly choose a serial level, or explicitly omit the serial-consistency field. |
| Client request timeout | 30 seconds; a statement duration overrides the profile, but an unset statement timeout inherits. Disabling the deadline requires a profile with no timeout. |
| Retry | Default retry policy; a statement policy overrides profile. |
| Load balancing | Default load-balancing policy; a statement policy overrides profile. |
| Speculation | Disabled; configured only through the selected profile and gated by statement idempotence. |
| Tracing | Disabled; statement/batch setting, not an inherited profile property. |
| Explicit timestamp | Absent; connection-level timestamp generator or server time is used. |
| History listener | Absent; attached per statement/batch. |
| Cached result metadata | Disabled by the statement setting; negotiated metadata-ID support changes effective behavior as described below. |

A profile is immutable after construction. A handle can be redirected to a new
profile, and every clone of that handle observes the redirection for later
requests. Separately created handles can point to the same profile and later be
remapped independently. An already running request retains its resolved profile.
An automatic pager resolves configuration once at creation; remapping the handle
does not reconfigure its later pages. Cloning a statement copies its configuration
and preserves shared handles and policy objects rather than creating independent
policy instances. The snapshot selects policy objects; it does not freeze their
internal state, such as load-balancing feedback or metrics used by speculation.
In Rust, resolution occurs when the asynchronous body reaches the handle load,
not when an unpolled future is constructed. Preparation and serialization that
precede that load can therefore finish after a handle has been remapped.

Regular and serial overrides can be unset to restore profile inheritance.
Clearing an individual retry/load-balancing policy restores the selected profile's
policy; clearing the profile handle restores the session's default handle.
Statement option getters expose stored overrides, not resolved effective settings.
In particular, the serial-consistency getter returns no value both for inheritance
and for explicit omission, even though those states execute differently. The
cached-metadata getter likewise reports the configured preference, not the
negotiated connection behavior. Profiles and handles can produce builders
initialized from their current settings; a profile obtained from a handle is a
snapshot and does not follow subsequent handle remapping. Optional handle labels
serve debugging only.

### Unprepared execution and automatic preparation

For unpaged or single-page execution of an unprepared query with no bound values,
each attempt sends the query string directly. With nonempty bound values, each
attempt first prepares the query on the chosen connection, validates and serializes
the values against that metadata, and executes it on that connection. This local
preparation is part of the attempt and therefore within the request deadline.
The coordinator was already chosen without a partition token, table, or confirmed
conditional-write flag. Local preparation does not restart routing with the newly
learned token. Token calculation for a tablet hint is best effort on this path:
failure omits useful hint information rather than failing the query.
This connection-local preparation retains the default Murmur3 partitioner;
unlike explicit session preparation, it does not install the table's partitioner
from cluster metadata before calculating that hint.

Automatic paging differs: a query with values is explicitly prepared through the
session before the pager is created, and its values are serialized once. A query
without values uses a direct query pager. This distinction affects preparation
fan-out, routing, validation timing, and the timeout limitation of explicit
preparation. Repeated unprepared executions do not automatically reuse a
session-wide prepared cache; callers must retain a prepared statement or use the
caching wrapper.

### Explicit preparation and statement identity

Preparation takes a cluster snapshot and first sends a prepare request concurrently
on one selected working connection per node. It waits for every attempted
preparation to finish. One successful response suffices even when other nodes
failed, provided the successful prepared identifiers agree. If that preparation
pass returns an error, a fallback pass attempts preparation across working shard
connections. The second pass also waits for all attempts. No working connection
or failure of all contacted connections produces a preparation error. Failure to
obtain the initial working-connection iterator returns immediately, before the
fallback pass.

When every contacted connection fails, that pass reports the first connection's
error in iteration order, not an aggregate or the last error to arrive. Once the
fallback starts, its result replaces the first pass's error; even a first-pass
identifier mismatch can be followed by fallback success or a different fallback
error. Failed preparation does not undo preparation already performed on servers.

Successful responses must agree on the prepared identifier. A mismatch is an
error for that pass; it is not hidden by selecting whichever successful result
arrived first. The result retains metadata from the first successful result in
connection-iteration order and collects successful preparation trace identifiers.
Partitioner information is taken from the cluster's table metadata when available,
otherwise the default partitioner is used.

The prepared object retains the query string, prepared ID, bind-column metadata,
partition-key indexes, original result metadata, mutable current result metadata,
and confirmed conditional-write information. Statement options and page size
carry over from the input statement. Missing preparation on a node can be repaired
on execution; the driver does not require successful preparation everywhere.

**Current limitation:** explicit preparation has no request-timeout wrapper, no
load-balancing execution plan, no retry-policy loop, and no speculative execution.
A stuck preparation can delay completion indefinitely even after another node
succeeded. The all-shards fallback is a separate preparation algorithm, not an
invocation of the statement retry policy. A preparation attempt's transport can
still fail independently.

### Binding and prepared execution

Binding validates and serializes values using the prepared bind-column metadata
and stores those bytes together with the prepared statement. The bound object
owns the serialized values; later execution does not consult the original value
objects or serialize them again. Binding preserves the statement configuration,
and the bound object can override its execution settings afterward. It can be
executed in all three paging modes and can calculate its routing token.

Ordinary prepared execution serializes supplied values before entering the core
request deadline. It calculates the partition key/token when available and uses
the table, token, and confirmed conditional-write flag for routing. Failure to
find an indexed serialized value or calculate a token is an execution error on
this path, unlike the best-effort hint calculation for unprepared queries.
NULL and UNSET partition-key entries are present values and are skipped by token
extraction, rather than producing a missing-index error. The driver can therefore
hash only the remaining components or empty input; token calculation does not
establish that the partition key is legal. See [routing](routing.md) for the
component-encoding rules.
The same serialized values are reused across attempts. An automatic prepared
pager retains its serialized values for every page.

Clones of a prepared statement share identity, bind metadata, and the mutable
result-metadata cache. Configuration and page size are local to each clone.
Cloning does not copy the list of preparation trace IDs; the clone's list is
empty. A bound object's clone follows the prepared-clone behavior.

### Transparent reprepare

When execution receives an unprepared-statement response, the connection
prepares the original query again on that same connection and resends execution
once. The newly prepared ID must equal the original ID; otherwise execution
fails with both identities and statement context. An unprepared response to the
resend is returned to the outer execution machinery. This repair is inside one
attempt and is independent of the user's retry policy and idempotence flag.
For a single EXECUTE, the ID named by the unprepared error is logged but is not
compared with the executed ID: even an unrelated reported ID triggers repair of
the original statement. The identity check applies to the subsequent PREPARED
response. Batch repair instead uses the error's ID to locate a member.

The resend preserves values, consistency, paging state, explicit/generated
timestamp, and negotiated tablet-hint byte. It reloads current result metadata
after reprepare, because either preparation or a concurrent execution may have
changed it. Both original response and resend can supply result-metadata updates
and tablet feedback. Invalid tablet feedback is logged and does not turn an
otherwise successful request into an error.

Tablet feedback is processed only for EXECUTE responses, including this resend.
Native QUERY and BATCH responses do not update the tablet cache. An unprepared
query with values can update it because that path sends EXECUTE after preparation.
When prepared table identity is available, feedback is forwarded before the
response's database error is interpreted, so failed EXECUTEs, including the
initial unprepared response, can also update routing. Valid feedback uses an
awaited send to the cluster worker's bounded queue. A full queue can delay the
attempt or let its deadline expire after the server has already replied; a closed
receiver is ignored. Enqueueing does not wait for publication of a new cluster
snapshot. Already enqueued feedback can be applied after cancellation, while a
pending send can be dropped. These boundaries are detailed in
[routing](routing.md#tablet-placement-and-feedback).

Batch recovery identifies the failing prepared ID among the batch members,
reprepares that member on the same connection, and resends the whole batch.
It repeats for further unprepared responses, rather than allowing only one
repair per batch. An ID absent from the batch is an error. The enclosing request
deadline bounds this loop when a deadline is configured; the loop itself has no
fixed retry count.

### Result metadata across schema changes

The prepared object distinguishes original result metadata from current result
metadata. Current metadata is shared across clones and updated atomically as a
whole snapshot. Original metadata remains available through a deprecated getter
and can be stale. Each response is decoded against the metadata snapshot
associated with that attempt, or against new metadata supplied by the response.
The current-column-specification getter also retains a snapshot: keeping its
guard keeps those column specifications alive and unchanged while other requests
replace the cache. A fresh getter call is needed to observe a later snapshot.

Bind-column metadata is immutable, including after transparent reprepare.
Schema changes affecting bind types, such as adding a UDT field, can therefore
require a new explicit preparation before new-shaped values can be serialized.
Refreshing result metadata does not refresh bind metadata or already bound bytes.

Without negotiated metadata-ID support, suppressing response metadata is opt-in.
That opt-in does not validate cached metadata against schema changes; stale
metadata can cause decoding errors or silently misinterpreted values. Transparent
reprepare without a metadata ID does not refresh that result-metadata cache.
With that extension, nonempty cached metadata is used automatically even when
the statement's cache setting is false. Empty cached metadata forces a metadata
request; statements can return no metadata during preparation but rows during
execution. On extension-capable connections an empty metadata ID is sent when
there is no usable cached ID, including prepared objects obtained from an older
server in a mixed-version cluster. Connections without the extension never send
the extension's ID field.

Requesting metadata does not make its absence a protocol error by itself. A row
response marked NO_METADATA with no supplied cache is decoded with empty column
metadata, even if its advertised column or row count is nonzero. This decoder
fallback does not reconstruct the missing column specifications or validate them
against the request; a zero-column row type can accept the advertised rows while
leaving payload bytes unexamined.

An EXECUTE row response carrying a different metadata ID updates the current
cache. The same ID also updates the cache if the cache was empty and the response
has columns. Reprepare updates the cache only when the server supplies an ID
different from the loaded cache snapshot; it skips an empty replacement when
that snapshot is nonempty. Skipping this cache update does not fail reprepare.
The same-ID/empty-cache exception above applies to row responses, not reprepare.
These checks and the replacement are separate operations, not an atomic
compare-and-swap. A concurrent execution can install nonempty metadata after
reprepare checks an empty snapshot, then have it overwritten by reprepare's empty
replacement. Concurrent responses install snapshots in store order, which need
not be response-arrival or schema-version order. Even a response decoded with an
older cached snapshot can reinstall that snapshot after another execution has
updated the cache. Already retained results and guards keep their own snapshots.

### Batches and their routing inputs

Batches can be logged, unlogged, or counter batches; the default is logged. They
can mix query strings and prepared statements. Each member has a corresponding
value list, including an empty list when that member needs no values. Too few or
too many lists is a serialization error. More than 65,535 statements is rejected
before batch dispatch. The session's batch API checks this limit before implicit
preparation and value serialization. The caching wrapper can prepare members
before forwarding to that API and reaching its count check.
Member options are not independently applied as execution
profiles; the batch supplies consistency, serial consistency, tracing, timestamp,
idempotence, timeout, and policies for the request.

Routing uses only the first member's table and token, and only if that member is
already prepared when the session builds the routing information. A later
prepared member does not supply a replacement routing key. First-member values
serialized for token extraction are cached and reused in batch serialization.
Other member values are serialized when the batch frame is sent, and can be
serialized again on outer retries and internal batch-repair resends. There is no
whole-batch snapshot of serialized values comparable to a bound statement.
If any already prepared member is a confirmed conditional write, the entire
batch is marked conditional for routing. Query strings are not parsed to infer
this flag. See [routing](routing.md) for the effect on target ordering.

Before sending a batch, the chosen connection prepares unprepared query strings
whose value lists are nonempty. Repeated identical query text in that preparation
set is prepared once for the connection attempt. Queries requiring no values
remain unprepared unless they share text with a member selected for preparation.
This implicit preparation does not retroactively improve session-level batch
routing. Explicit batch preparation instead returns a new batch with all query
members prepared, preserves batch settings, leaves already prepared members
prepared, and prepares the remaining members concurrently. It does not deduplicate
equal query strings. A preparation failure returns no batch and drops member
futures still pending when the combined operation returns, although preparations
already sent can have server-side effects. There is no driver-level guarantee
that the first completed member error ends the combined operation immediately.
This differs from waiting for every connection attempt within each individual
statement's preparation pass. Connection-local implicit preparation
instead prepares the deduplicated query strings sequentially, in unspecified
set-iteration order, and stops on the first preparation error.

### Prepared-statement cache

The optional caching session wraps an existing session, optionally through shared
ownership. Its query APIs obtain a cached prepared statement and then call the
corresponding prepared API; batch execution prepares and caches unprepared
members as needed. An already fully prepared batch bypasses cache preparation.
The default builder capacity is 128; zero capacity is rejected as a programmer
error. A custom key hasher and result-metadata-cache preference can be supplied.
The metadata-cache preference defaults to false in all constructors and the
builder; negotiated metadata-ID support still determines effective caching.

The wrapper also supports explicit cache population/lookup without execution,
returning a configured prepared statement that can be bound or executed directly.
This operation accepts query text/options rather than importing an existing
prepared object. Explicit cached batch preparation returns a new prepared batch
without executing it. The underlying session and configured maximum capacity
remain inspectable; operations performed directly on that session do not use the
wrapper's cache.

Cache keys are query strings only. They do not include current keyspace,
consistency, page size, values, or other statement options. Reusing unqualified
query text after a keyspace switch can therefore reuse a previously prepared
object; callers must not assume keyspace-scoped keys or invalidation. Each cache
lookup reconstructs a configured handle from the current call's statement
options and page size. Options from the first use do not leak into later uses.
The wrapper's metadata-cache preference is then applied to that handle.

A miss prepares first, then evicts entries while the observed size is at least
the configured capacity, then inserts the prepared object. Eviction removes the
first entry returned by the concurrent map iterator: it is neither an LRU nor a
FIFO guarantee. Concurrent misses for the same query are not coalesced and can
prepare redundantly. The check/evict/insert sequence is not atomic, so capacity is
not a strict instantaneous bound under concurrency. The repeated eviction check
allows later insertions to repair excess size and can evict more than necessary.

Eviction or replacement removes only the map's ownership; it does not invalidate
prepared handles already returned or used by requests and pagers. Concurrent
misses for the same text each return their own prepared object, while the last
insertion determines what later cache hits use. Those independently prepared
objects have separate mutable result-metadata caches. Capacity limits map entries,
not all prepared state retained by callers or in-flight operations.

A failed cache-miss preparation neither inserts a negative cache entry nor evicts
an existing entry for that miss. Cached batch preparation runs member lookups
concurrently and drops unfinished member futures when the combined operation
returns a member failure. It shares explicit batch preparation's error-delivery
timing limitation.
Successful members may already have inserted entries and evicted older ones;
those cache changes are not rolled back. Cache-miss preparation, including batch
members, precedes the underlying execution deadline and uses the explicit
preparation algorithm described above.

### Request, attempt, deadline, and error boundaries

A request resolves one cluster snapshot and constructs a load-balancing plan of
node/shard targets. Each attempt acquires a connection for its current target.
Failure to obtain a connection skips to the next target and retains the pool
error as the branch's last error. It does not invoke attempt
history callbacks, load-balancing failure feedback, or the retry policy, and does
not count as a sent or failed query attempt in query metrics. Once a connection
is acquired, the attempt count and history begin before serialization or local
preparation; an attempt can therefore fail without sending the final query,
execute, or batch frame. The retry policy is consulted after such an attempt
fails.

A successful attempt returns its response and coordinator. An attempt error
notifies the load-balancing policy, then is supplied to the retry session with
the statement's idempotence and the **current** regular consistency. The history
listener receives the error together with the resulting retry decision. That
decision can stop, retry the same target, advance to the next target, or report an
ignored write error as success. Same-target retry picks
a connection again; it does not promise reuse of the same socket. A retry can
change regular consistency, and subsequent attempts and retry decisions see
that changed value. The plan's routing information retains the original routing
consistency; a downgrade does not rebuild routing information.

Retry-session state is independent for every speculative branch and created
lazily on that branch's first failed attempt. A request with no targets returns
an empty-plan error. Exhaustion after attempts returns the last encountered
attempt/pool error. There is no driver-wide numeric retry limit: the plan,
selected retry policy, and deadline determine termination. A custom same-target
policy and disabled deadline can therefore retry indefinitely.

The client timeout encloses connection picking, attempts, local prepare/execute
repair, retry loops, and speculative branches for that request. It is not a
server execution cancellation guarantee. It does not enclose prior ordinary
prepared-value serialization, first-member batch serialization, or partition-key/
token calculation done before the runner. Explicit session preparation and
post-success keyspace/schema side effects are also outside this deadline.
A timeout returns a distinct request-timeout error
and increments the timeout counter. Connection cleanup for a request abandoned
while the server is still executing belongs to [connections](connections.md).

Dropping a caller-owned execution or preparation future drops its unfinished
attempt futures; it does not send a server-side cancellation or undo already
completed work. Dropping an execution future is distinct from the driver's
timeout path: it does not itself increment the timeout counter or finish the
request's history record. Attempts abandoned by caller cancellation, a timeout,
or a winning speculative branch can likewise retain a start event without a
success/error callback. Automatic paging after page one has the separate worker
lifetime described below.

Public result-shape validation also occurs after the core request succeeds.
In particular, an unexpected unpaged continuation or a non-row response on a
later automatic page is returned as an error without consulting the retry policy
or revising the core's success history/metrics. Typed row decoding is outside the
request deadline.

Ignored-write success becomes an empty non-row result with its coordinator and
without warnings or a trace ID. For automatic paging it instead ends the stream;
if it happens on page one, the returned pager is empty. This result is explicitly
permitted by a selected retry policy; it is not proof the requested consistency
was satisfied. Its underlying failed attempt still increments the failure count,
reports load-balancing failure, and records the error and ignore decision in
attempt history. The logical request is then recorded as successful; no successful
attempt callback or latency sample is added for that ignored error.

### Default retry decisions

The default policy never changes consistency and has separate one-use budgets
for unavailable, read-timeout, and batch-log-write-timeout recovery. These budgets
are per retry session, not per target. If the request's **regular consistency** is
serial or local serial, all errors are terminal; this check is distinct from
having a serial-consistency option on an ordinary conditional write.

| Error/condition | Default decision |
| --- | --- |
| Broken connection | Next target only for idempotent statements. |
| Overloaded, server error, truncate error | Next target only for idempotent statements. |
| Unavailable | Next target at most once, independent of idempotence. |
| Read timeout with received replies at least required and no data response | Same target at most once, independent of idempotence. Other read timeouts stop. |
| Batch-log write timeout | Same target at most once and only if idempotent. Other write timeout kinds stop. |
| Node bootstrapping | Next target, independent of idempotence. |
| No available stream ID | Next target, independent of idempotence. |
| Syntax, invalid request, already exists, UDF failure, authentication/authorization/configuration/protocol errors | Stop. |
| Read failure, write failure, rate-limit error, unprepared remaining after repair, unknown database error | Stop. |
| Serialization, response parsing, unexpected response, changed reprepare ID, missing batch reprepare ID, invalid nonfinished unpaged result | Stop. |

The fallthrough policy always stops after an attempt error. It does not prevent
skipping a target whose connection could not be acquired, nor disable the
connection's transparent unprepared-statement repair.

### Optional consistency-downgrading retry decisions

The downgrading policy is an explicit opt-in and can weaken the requested
consistency or ignore a write timeout. It shares a single one-use budget across
unavailable/read-timeout/write-timeout handling. For a reported live/reply count,
its fallback is THREE for at least three, TWO for two, ONE for one; an original
EACH_QUORUM also permits ONE when the count is zero or negative. Otherwise it
stops. Error counts are decoded as signed integers without a nonnegative check.
The fallback is not clamped to the request's consistency: for example, an
unavailable response reporting two live replicas makes a request at ONE retry at
TWO. Despite its name, the policy does not guarantee that every changed
consistency is weaker than the previous one.

- Unavailable consumes that budget and retries the same target at the fallback
  consistency if possible.
- A read timeout with fewer replies than required consumes the budget and uses
  that fallback. Enough replies but no data consumes the budget and retries the
  same target at unchanged consistency. Enough replies with data stops.
- A write timeout requires idempotence and the unused shared budget. A simple or
  logged-batch write with at least one reply is ignored as success. An unlogged
  batch retries with the count-based consistency; a batch-log write retries at
  unchanged consistency. Counter, conditional, view, CDC, other kinds, and simple
  or batch writes with no reply stop.
- Broken connections and overload/server/truncate errors advance only when
  idempotent; bootstrapping and stream-ID exhaustion advance regardless.
- At regular SERIAL/LOCAL_SERIAL consistency, unavailable advances to another
  target without the ordinary one-use downgrade budget; all other errors stop.
- Remaining errors are terminal as in the default policy.

### Speculative execution

Only statements explicitly marked idempotent use a configured speculative policy.
The policy supplies a maximum number of additional branches, excluding the
original branch, and an interval. The simple policy uses a fixed interval. The
metrics-dependent percentile policy obtains an interval in milliseconds from
recorded latency and falls back to 100 ms when that lookup fails. The percentile
policy and the metrics field of its context exist only with the `metrics` feature,
which is disabled by default. Fixed-interval and custom speculative policies
remain available without metrics.
The branch limit and interval are read once per logical request (once per page
for automatic paging). Later launches reuse that interval. An ignorable failure
does not launch a replacement immediately; it waits for the existing timer.

Branches share a synchronized target-plan iterator, so each branch consumes new
plan entries rather than independently starting at the first target. Each branch
has independent retries and starts at the original effective consistency. A
successful branch or a terminal error ends the race immediately and drops the
other branch futures. A branch reporting a transient error can be ignored while
other branches run or while scheduled additional branches remain. When a branch
finds no remaining target at its start, the scheduler stops launching new branches
but waits for already running ones. A branch that used targets and then exhausted
the plan reports its last error instead; a later scheduled branch may still run
to observe the empty plan.
When all branches finish without a decisive result, the last ignorable error is
returned, or an empty-plan error when there was none.

Ignorable outcomes include pool/connection failures, stream-ID exhaustion,
unavailable, overloaded, bootstrapping, read/write timeout or failure, unprepared,
server error, and rate-limit errors. Syntax/invalid/already-exists/unauthorized,
authentication, protocol, unknown database, UDF, configuration and truncate errors
are terminal. Serialization/parsing errors, unexpected responses, reprepare-ID
errors, and invalid unpaged continuations are terminal. The branch error
classification differs from the default retry policy: an error can be terminal
for one branch's retry loop yet be ignored while racing another branch.

Only the winning response reaches the session's keyspace/schema side-effect
handling. Connection-level work already performed by losing branches, including
reprepare, result-metadata updates, and tablet feedback, is not rolled back.
Dropping those branches does not cancel requests already executing on servers.

### Automatic paging state and ownership

The pager retains a fixed cluster snapshot, resolved profile settings, statement,
and serialized values. Every page is a new logical request with a new plan,
deadline, history request, and branch-local retry sessions. A consistency downgrade
does not leak into the next page. Prepared pagers choose their tablet-version hint
once and reuse it for every page.
The snapshot does not freeze connection pools, shared policy state, or the
prepared object's result-metadata cache; those can change while paging continues.

After a successful page with a continuation, the next page prefers the previous
coordinator's node/shard. That target is filtered out of the rest of the fresh
plan. For an unsharded node, all entries for that node are filtered regardless of
synthetic shard value. A connection is selected for the preferred node/shard;
the previous physical connection is not retained as a requirement. On error,
ordinary retry/fallback can choose another coordinator, which becomes the next
page's preference after success.

Every retry or speculative branch for one page uses that page's same input
continuation. The continuation advances only after a successful row response.
Recovery does not replay earlier successful pages or retract rows already yielded
to the caller. A page failure ends background fetching; after receiving that
error, polling again cannot restart the failed fetch. The automatic pager exposes no
continuation token for manual resumption.
Continuation bytes are not checked for repetition or forward progress. Repeated
tokens can therefore refetch rows, and an endless sequence of successful empty
pages with continuations can keep a row poll pending indefinitely. There is no
total page-count or whole-stream deadline; completing each page within its own
deadline does not bound the duration of iteration.

Page one is fetched on the caller's task. Only if it has a continuation does the
driver spawn a worker. The page channel has capacity one. The worker fetches then
sends, so in addition to the consumer's page and one queued page it can hold a
newly fetched page while waiting to send; this is not an exact one-page total
memory bound. Dropping the pager closes the receiver, and the worker stops when
its next send observes closure. It does not immediately cancel an already
in-flight fetch. Its page deadline still applies unless disabled.
The detached worker owns the state needed to finish that fetch, so request
metrics, history callbacks, and connection-level feedback can occur after the
pager has been dropped.

A non-row success on the first page produces an empty stream, after required
keyspace or schema-change handling. A non-row response on a subsequent page is
an unexpected-response error. Later request failures are delivered through the
stream and stop the background fetching loop. Empty pages are skipped without
mistaking them for final completion. Cancellation of waiting for the next row
does not consume a row.

The public typed stream yields owned values because its interface cannot return
values borrowing from a changing page. Conversion to a typed stream checks the
initial page's result types. Later pages are type-checked when their first raw row
is yielded, before typed decoding; empty pages are skipped without that check.
This permits detection of schema changes on pages containing rows. A type-check
or typed-value decoding error has already consumed that row. Raw row-framing
errors decrement the remaining-row count but can recur at the same malformed
bytes. These errors do not themselves stop the background worker; the caller can
continue polling. Manual/unpaged results can instead lend references into their
retained response bytes; see
[serialization/protocol](serialization-protocol.md).

The pager retains trace IDs and coordinator descriptors for consumed pages,
starting with page one. Merely fetching a later page in the background does not
yet append those descriptors to the exposed lists. Metadata access describes
the current page. Unlike a single-page result, the pager does not expose a
warnings collection; page processing discards response warnings.

### Results, timestamps, and response side effects

A public single-page/unpaged result identifies the node and shard that served
the winning attempt, exposes server warnings and optional trace ID, and
distinguishes row responses from non-row success. An empty SELECT is still a row
response. Requiring a non-row result rejects even an empty row response.
Conversion to a row result rejects a non-row response and returns the original
result inside that error. Row decoding is typed and lazy, with distinct type
checking and value-decoding errors.

The coordinator also exposes the actual connection address after address
translation, port selection, or client routing. That address can differ between
connections to the same node and is not a node identity; the node's host ID is
the identity. Its shard is optional and comes from the selected connection's
advertised shard information, including when connection selection falls back
from the requested shard. An unsharded connection has no shard value.

Row access supports an iterator, optional first row, required first row, and
exactly one row. The optional-first operation permits zero rows; required-first
rejects zero; exactly-one rejects both zero and multiple rows. These convenience
operations do not scan and validate all unused later row payloads. Column metadata
can be inspected by position or name; name lookup uses exact string equality
and returns the first matching column and its index when names repeat.
The result reports row count and encoded
row-byte size independently of decoding into application values.

Timestamp precedence is an explicit statement/batch timestamp, then the session's
configured generator, otherwise no client timestamp. Timestamps are microseconds.
An explicit CQL timestamp clause has server-side precedence. No generator is
configured by default. A generated timestamp is obtained inside each connection
attempt, so retries, speculative branches, and pages can get different timestamps.
The internal unprepared-repair resend and repeated batch-repair sends reuse their
already constructed request timestamp. The simple generator uses wall-clock time
without monotonic guarantees and panics if time precedes the epoch. The monotonic
generator atomically chooses a greater wall-clock value or the preceding value
plus one; its guarantee is per generator instance, including concurrent callers.
It warns about clock drift exceeding one second at most once per second by
default, with configurable or disabled drift warnings. A wall clock before the
epoch also takes the increment path and emits a warning.

A successful keyspace-change response causes session-wide keyspace propagation
before the public operation completes. A successful schema-change response, when
automatic agreement is enabled, waits for agreement including the response's
coordinator and then refreshes metadata if configured. Both options default to
enabled. The default agreement interval is 200 ms and its separate timeout is
60 seconds. A side-effect failure is returned even though the original database
operation may already have succeeded. This handling occurs after the core request
has logged its success and outside its request deadline; there is no automatic
retry of the original successful statement because side-effect handling failed.
The same first-response handling applies to automatic pagers. Exact agreement,
reachability, and keyspace-switch rules are in
[metadata/control/events](metadata-control-events.md) and [connections](connections.md).

Tracing details are fetched separately by querying trace-session and trace-event
tables concurrently. These independent reads do not establish that every trace
event has arrived. An incomplete trace means no session row, missing duration,
or no events; that case is retried up to the configured attempt count, default ten,
with a default 3 ms interval. An incomplete attempt sleeps even when it is the
last configured attempt. Query errors return immediately. Row/type decoding
errors return when that result is inspected: event rows are inspected only after
a session row with a duration exists, so incomplete session data can defer
discovery of malformed event rows.
Default trace-fetch consistency is ONE. Exhausted incomplete attempts return an
empty-results error. Query history and metrics count the execution core's
requests/attempts; their full listener/reporting contract belongs to
[configuration/security/observability](configuration-security-observability.md).

## Rust implementation evidence

All links below are pinned to the reference commit. Link anchors identify concrete
entry points; the contracts above were checked against surrounding implementations,
not only public comments. Some comments lag current behavior, notably cache
eviction order and result-metadata allocation.
Both explicit batch-preparation implementations delegate member aggregation to
`futures::try_join_all`; the driver does not implement a separate first-error
race. Its [`futures` dependency declaration](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/Cargo.toml#L120)
allows compatible dependency versions, so their internal scheduling thresholds
are not a pinned driver contract.

| Contract/subfeature | Entry points |
| --- | --- |
| Statement defaults and positive page-size validation | [statement/mod.rs](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/statement/mod.rs#L26), [unprepared setters](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/statement/unprepared.rs#L45) |
| Manual continuation storage and completion | [paging state types](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla-cql-core/src/frame/request/query.rs#L9), [single-page dispatch](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/client/session.rs#L1629) |
| Profile defaults and handle remapping | [profile defaults](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/client/execution_profile.rs#L182), [profile handles](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/client/execution_profile.rs#L525), [resolution](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/client/execution.rs#L150) |
| Override inspection/reset and profile snapshots | [statement overrides](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/statement/prepared.rs#L436), [profile builders and labels](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/client/execution_profile.rs#L446), [handle snapshot](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/client/execution_profile.rs#L519) |
| Unprepared modes and local preparation | [query implementation](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/client/session.rs#L1656), [query pager preparation](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/client/session.rs#L1841) |
| Explicit preparation and fallback | [prepare_nongeneric and prepare_on_all](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/client/session.rs#L1933) |
| Binding and shared statement state | [PreparedStatement shared data and clone](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/statement/prepared.rs#L201), [bind](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/statement/prepared.rs#L680), [BoundStatement](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/statement/bound.rs#L21) |
| Prepared execution and routing information | [session execute](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/client/session.rs#L2085) |
| Transparent reprepare and metadata ID validation | [connection reprepare](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/network/connection.rs#L773), [execute and resend](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/network/connection.rs#L1105) |
| Tablet feedback before error handling, queue backpressure, and asynchronous publication | [feedback send](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/network/connection.rs#L2045), [cluster worker publication](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/cluster/worker.rs#L303) |
| Metadata snapshots and extension decisions | [bind/result metadata limitations](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/statement/prepared.rs#L143), [result-metadata accessors](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/statement/prepared.rs#L569), [metadata update and request parameters](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/network/connection.rs#L996), [response metadata selection](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla-cql/src/frame/response/result.rs#L877) |
| Batch validation, routing, preparation, and repair | [session batch](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/client/session.rs#L1294), [first-value caching](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/statement/batch.rs#L308), [connection batch](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/network/connection.rs#L1252), [explicit prepare_batch](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/client/session.rs#L2258) |
| Cache keys, options, capacity, and concurrency | [cache lookup and insertion](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/client/caching_session.rs#L200), [builder](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/client/caching_session.rs#L287) |
| Cache preparation without execution and session access | [cached prepare_batch and add_prepared_statement](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/client/caching_session.rs#L171), [cache/session accessors](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/client/caching_session.rs#L248) |
| Core timeout, attempts, retry, and metrics boundaries | [request runner](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/client/execution.rs#L431), [attempt loop](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/client/execution.rs#L547) |
| Cancellation and transport ownership | [submitted request and orphan notification](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/network/connection.rs#L138), [speculative future ownership](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/policies/speculative_execution.rs#L187) |
| Default, fallthrough, and downgrading retry policies | [default](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/policies/retry/default.rs#L62), [fallthrough](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/policies/retry/fallthrough.rs#L34), [downgrading](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/policies/retry/downgrading_consistency.rs#L68) |
| Signed retry-error counts and missing result-metadata fallback | [error count decoding](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla-cql-core/src/frame/response/error.rs#L54), [NO_METADATA without a cache](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla-cql/src/frame/response/result.rs#L891), [column-driven row traversal](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla-cql-core/src/deserialize/result.rs#L56) |
| Custom retry lifecycle and gated request-info construction | [retry traits](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/policies/retry/retry_policy.rs#L72), [gated RequestInfo constructor](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/policies/retry/retry_policy.rs#L30), [lazy session creation](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/client/execution.rs#L345) |
| Speculative scheduling and outcome classification | [scheduler](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/policies/speculative_execution.rs#L187), [branch error classification](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/policies/speculative_execution.rs#L136), [database error classification](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla-cql-core/src/frame/response/error.rs#L451) |
| Page request state, stability, deadlines, and errors | [PagingExecutor](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/client/pager.rs#L134) |
| First-page fetch, worker/channel, and ownership | [QueryPager construction](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/client/pager.rs#L827), [prepared pager](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/client/pager.rs#L926), [typed row stream](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/client/pager.rs#L1344) |
| Result shape, warnings, trace IDs, and row cardinality | [QueryResult](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/response/query_result.rs#L73), [typed row access](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/response/query_result.rs#L322), [response normalization](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/response/request_response.rs#L63) |
| Actual coordinator address/shard and column lookup | [Coordinator](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/response/coordinator.rs#L30), [ColumnSpecs](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/response/query_result.rs#L49) |
| Timestamp precedence and generators | [query timestamp](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/network/connection.rs#L960), [timestamp generators](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/policies/timestamp_generator.rs#L16) |
| Side effects and trace fetches | [session post-response handling](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/client/session.rs#L2496), [schema side effects](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/client/session.rs#L1822), [pager side effects](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/client/pager.rs#L1192), [tracing retrieval](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/client/session.rs#L2380) |

The stable API uses `Session::{query,execute}_{unpaged,single_page,iter}`,
`Session::execute_bound_*`, `Session::{prepare,prepare_batch,batch}`,
`PreparedStatement::bind`, `CachingSession`, `QueryResult`, `QueryRowsResult`, and
`QueryPager::rows_stream`. `PagingState::{start,as_bytes_slice,new_from_raw_bytes}`
supports manual continuation storage, while `PagingStateResponse::{finished,
into_paging_control_flow,new_from_raw_bytes}` represents completion independently.
`CachingSession::{add_prepared_statement,prepare_batch}` provides preparation
without execution. The deprecated public `QueryPager::type_check` checks only
the current page; it does not establish type compatibility for later pages.
`rows_stream` performs the required repeated checks. Custom `RetryPolicy`
implementations create branch-local `RetrySession` objects; although the trait
includes `reset`, this execution core creates fresh sessions instead of resetting
and reusing them across requests/pages.
The unified execution core also serves a fixed-connection
pager for internal metadata requests. That internal path uses one connection,
fallthrough retries, no speculation/metrics/history, a per-page timeout, and
requires row responses; its complete control-connection context is specified in
[metadata/control/events](metadata-control-events.md).

Several preserialized-value and raw-result APIs are conditional wrapper-driver
interfaces, not general stable APIs: Python serialization/execution/raw result
access, C# preserialized paging/raw column iteration, C++ result decomposition,
and Node.js raw-row/metadata access require their respective unstable feature
gates and the unstable configuration.
Preserialized execution can bypass normal bind validation; raw result access
leaves row type checking/decoding to its caller. These interfaces have their own
contracts. In particular, Python's
[`Session::execute_unstable`](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/client/session.rs#L2048)
accepts already serialized values, a paging flag, and an input continuation.
With paging disabled it omits page size but still returns the response's
continuation, without the stable unpaged method's finished-state check.
Python also exposes `PreparedStatement::serialize_values_unstable` (which does
validate values), `QueryResult::deserialized_metadata_and_rows`, and the
`RequestInfo::new` constructor for retry-policy integration.
Specifically, [`QueryPager::next_column_iterator`](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/client/pager.rs#L748) is public only with both
`scylla_unstable` and `unstable-csharp-rs`. It is a cancellation-safe lending
operation returning a column iterator and a fresh-page flag; callers must check
each new page's types before typed decoding. See
[serialization/protocol](serialization-protocol.md) for this gated borrowing API.

## Invariants

- Stable unpaged APIs reject a server continuation; the gated Python execution
  interface returns it explicitly even with paging disabled.
- Statement overrides are resolved against exactly one selected profile; explicit
  omission of serial consistency is distinct from inheritance.
- Retry decisions receive the consistency actually used by the failed attempt.
- Speculative execution is enabled only for explicitly idempotent statements;
  this does not imply that every default-policy retry requires idempotence.
- A transparent reprepare cannot silently replace the prepared identifier.
- Each automatic page gets fresh plan/retry/deadline state; prior consistency
  downgrades do not become the next page's default.
- A result or page retains the metadata snapshot selected for that response,
  even while other executions update the prepared object's cache. Opting into
  cached metadata without metadata-ID support does not guarantee schema freshness.
- Result iteration cannot outlive the retained bytes it borrows. The stable
  automatic stream yields owned rows; borrowed manual rows are a separate mode.
- Successful DDL or keyspace changes can be followed by a local side-effect error;
  success metrics for the database attempt do not prove side effects completed.
- Caching reuse preserves the current caller's statement options, not those of
  the initial cache fill.
- No strict LRU, preparation deadline, immediate pager-drop cancellation, or
  single timestamp across all retries is implied by the reference implementation.

## Test places

The targets below exist in the pinned Rust checkout. Their source is evidence of
the assertions described here; no Rust or Go test execution is claimed.
Go targets remain explicit backlog until the implementation/migration stage.

### Rust unit tests

#### Description

Policy decisions, speculative scheduler termination, monotonic timestamps,
result shape/cardinality, cache option isolation, and cache configuration
validation can be checked without a production database. The in-source async
cache tests use a dry-mode protocol proxy.

#### Test cases

- Selected terminal errors stop, and broken-connection/overloaded/server/truncate retries are gated by idempotence at regular consistency ONE; the tests do not cover every terminal error variant: scylla/src/policies/retry/default.rs:default_never_retries,default_idempotent_next_retries
- Repeated unavailable, read-timeout, and batch-log-timeout errors exhaust their respective budgets: scylla/src/policies/retry/default.rs:default_unavailable,default_read_timeout,default_write_timeout
- Downgrading decisions cover nonserial regular consistencies and positive reply counts for unavailable/read timeout and batch-log, unlogged-batch, or simple write timeout; the unlogged-batch matrix also includes count-based retries above the original consistency: scylla/src/policies/retry/downgrading_consistency.rs:downgrading_consistency_unavailable,downgrading_consistency_read_timeout,downgrading_consistency_write_timeout
- Bootstrapping advances with either idempotence setting: scylla/src/policies/retry/default.rs:default_bootstrapping; scylla/src/policies/retry/downgrading_consistency.rs:downgrading_consistency_bootstrapping
- A branch reporting an empty speculative plan stops new branches and waits for running ones: scylla/src/policies/speculative_execution.rs:test_exhausted_plan_with_running_fibers,test_exhausted_plan_last_running_fiber
- Synthetic stream-ID-exhaustion failures terminate without panic after the configured speculative branches, with elapsed time asserted using a paused clock: scylla/src/policies/speculative_execution.rs:test_se_panic_on_ignorable_errors
- Timestamp values increase within each calling thread and are unique across threads sharing one generator: scylla/src/policies/timestamp_generator.rs:monotonic_timestamp_generator_is_monotonic,monotonic_timestamp_generator_is_monotonic_with_concurrency
- Result helpers distinguish non-row/empty/one/multiple rows, type-check versus decoding errors, column lookup, warnings, and trace IDs; failed non-row conversion returns the original result: scylla/src/response/query_result.rs:test_query_result,test_query_result_returns_self_if_not_rows
- Cache hits use the current caller's regular/serial consistency, timestamp, and page size in EXECUTE frames, with exactly one preparation asserted: scylla/src/client/caching_session.rs:test_parameters_caching
- Sequential cache fills evict either prior entry at capacity, and repeated calls through the cache APIs avoid repeat preparation: scylla/src/client/caching_session.rs:test_full,test_execute_cached
- Zero cache capacity is rejected by all constructors: scylla/src/client/caching_session.rs:test_builder_zero_capacity_panics,test_from_zero_capacity_panics,test_with_hasher_zero_capacity_panics
- Manual paging tokens preserve start versus empty-byte continuation, round-trip raw bytes, and map completion into control flow: missing:manual-paging-state-roundtrip
- Serial getters conflate inherited and explicitly omitted values while clearing overrides restores profile resolution: missing:statement-serial-getter-and-reset-resolution
- A profile snapshot and a builder copied from a handle retain their selected settings after remapping: missing:profile-public-snapshot-and-builder
- Duplicate column names resolve to the first exact matching name: missing:result-duplicate-column-lookup
- Wrapper-driver feature gates and Python unpaged continuation propagation are checked explicitly: missing:unstable-wrapper-execution-contracts
- Every terminal default-policy error variant, including rate-limit, response-parsing, unexpected-response, and nonfinished-paging-state errors, is asserted directly: missing:default-retry-terminal-error-matrix
- Default and downgrading policies handle regular SERIAL/LOCAL_SERIAL, and mixed error types exercise separate versus shared retry budgets: missing:retry-serial-and-mixed-error-budgets
- Downgrading handles zero-reply read/write timeouts and the EACH_QUORUM zero-live/zero-reply fallback exception; unavailable with zero live replicas at ALL has integration coverage below: missing:downgrading-zero-reply-boundary
- Negative error counts preserve the EACH_QUORUM fallback, and unavailable at ONE with two live replicas retries at TWO: missing:downgrading-negative-count-and-unavailable-increase
- Speculative success/terminal-error races, complete error classification, and non-idempotent suppression are asserted directly: missing:speculative-outcome-and-idempotence-matrix
- Cache hits preserve current timeout, idempotence, tracing, history, profile, and policy settings as well as the four tested wire parameters: missing:cache-nonwire-option-isolation
- Concurrent miss races do not imply a strict instantaneous capacity bound: missing:cache-concurrent-capacity-bound
- Pager drop during an in-flight fetch is distinguished from cancellation at channel send: missing:pager-drop-inflight-fetch

### Rust integration tests

#### Description

Database and proxy suites exercise statement behavior and inspect protocol traffic,
including recovery, retry decisions, page boundaries, and result metadata changes.
Tests using multiple nodes, Scylla extensions, or CCM need those environments;
their existence does not establish portability or a passing local run.
In particular, metadata-ID tests return early when that extension is absent;
`test_mixed_cluster` disables its advertisement on one proxy node rather than
running different server versions. The special-query metadata test starts a CCM
cluster with password authentication and authorization enabled. The large-batch
and CDC-partitioner tests are ignored under `cassandra_tests`, and the metrics
test module is compiled only with the `metrics` feature. The SELECT-without-FROM
test skips only on a `SyntaxError` from its initial capability probe; other errors
fail the test. Tracing integration helpers use 200 fetch attempts at 50 ms,
so they do not verify readiness with the production defaults of ten at 3 ms.

#### Test cases

- Proxy smoke tests exercise execution with and without values, but drop the unwanted opcode on only one node and do not assert the query result: scylla/tests/integration/statements/unprepared.rs:test_prepare_query_with_values,test_query_with_no_values
- SELECT without FROM preserves typed expression results and bind metadata on supporting servers: scylla/tests/integration/statements/select_without_from.rs:test_select_without_from
- Preparation tries one connection per node, tolerates partial failure, and succeeds after first-pass failures or reports all-attempt failure; the shard-check helper only checks counts for observed shards and does not prove every expected shard was contacted: scylla/tests/integration/statements/prepared.rs:test_preparation_logic
- Prepared configuration, column metadata, partitioner, and cached-metadata options remain available: scylla/tests/integration/statements/prepared.rs:test_prepared_config,test_prepared_partitioner,test_prepared_statement_col_specs,test_skip_result_metadata
- Bound values execute in all three paging modes; inherited page size, tracing, and history are observed during execution. Post-binding overrides and resets are checked through stored-option getters: scylla/tests/integration/statements/bound.rs:test_bound_statement
- Mismatched value counts are rejected, NULL non-key values are stored as null, and UNSET non-key values leave columns unchanged: scylla/tests/integration/statements/positional_values.rs:test_positional_values
- Named bind markers are resolved by name: scylla/tests/integration/statements/named_bind_markers.rs:test_named_bind_markers
- Batch value-list count and the 65,535-statement limit are enforced: scylla/tests/integration/statements/batch.rs:batch_statements_and_values_mismatch_detected,test_large_batch_statements
- Implicit and explicit batch preparation work with mixed statement forms: scylla/tests/integration/statements/batch.rs:test_quietly_prepare_batch,test_prepare_batch,test_batches_of_various_statement_kinds
- Logged batch writes and conditional batch results are asserted: scylla/tests/integration/statements/batch.rs:test_batch,test_batch_lwts
- A native counter batch is accepted; the separate multi-table test prepares and executes a textual BEGIN BATCH statement. These two smoke tests do not read back the written values: scylla/tests/integration/statements/batch.rs:test_counter_batch,test_batch_to_multiple_tables
- The combined statement-kind suite does read back counter increments across three tables and asserts server rejection of counters in logged batches and non-counter writes in counter batches: scylla/tests/integration/statements/batch.rs:test_batches_of_various_statement_kinds
- Invalidated prepared statements are repaired for execute, batches, and cached execution: scylla/tests/integration/statements/transparent_reprepare.rs:test_unprepared_reprepare_in_execute,test_unprepared_reprepare_in_batch,test_unprepared_reprepare_in_caching_session_execute
- Metadata IDs refresh result schema, including preparation on a proxy node with metadata-ID advertisement disabled and execution on extension-capable nodes: scylla/tests/integration/statements/result_metadata_extension.rs:test_basic_metadata_update,test_mixed_cluster
- INSERTs send an empty metadata ID on each execution and return VOID: scylla/tests/integration/statements/result_metadata_extension.rs:test_insert_sends_empty_result_metadata_id
- Special queries return decodable rows, acquire missing result metadata, suppress metadata on later executions, and retain useful cached metadata after injected UNPREPARED; the initial-metadata assertion adapts to what the server actually returns: scylla/tests/integration/ccm/result_metadata_extension.rs:test_result_metadata_extension
- Profile selection, statement overrides, and lazy retry-policy creation control execution: scylla/tests/integration/statements/execution_profiles.rs:test_execution_profiles
- Request parameters and routing information receive the selected regular and serial consistency: scylla/tests/integration/statements/consistency.rs:consistency_is_correctly_set_in_cql_requests,consistency_is_correctly_set_in_routing_info
- Regular SERIAL is accepted for a SELECT; this smoke test does not exercise retry decisions or prove Paxos behavior: scylla/tests/integration/statements/consistency.rs:consistency_allows_for_paxos_selects
- A remapped default profile affects subsequent requests, and per-statement timeouts override it: scylla/tests/integration/statements/request_timeout.rs:test_request_timeout
- Retry policies receive updated regular consistency after downgrade: scylla/tests/integration/session/retries.rs:test_session_retry_receives_current_consistency
- Idempotent requests succeed when selected nodes' local PREPARE requests are dropped or fail; these smoke tests do not count launched speculative branches or compare them with a non-idempotent request: scylla/tests/integration/session/retries.rs:speculative_execution_is_fired,retries_occur
- Unavailable retries downgrade ALL to TWO on the wire, with both idempotence settings, while zero live replicas stop after one request: scylla/tests/integration/session/retries.rs:downgrading_consistency_policy_downgrades_on_unavailable
- Pagers return all expected rows across pages and handle modification statements as empty streams: scylla/tests/integration/session/pager.rs:test_pager_basic,test_iter_methods_with_modification_statements
- A custom IgnoreWriteError policy is invoked for invalid CQL and execution after table removal, and the returned streams finish without error; the test does not inject a write timeout, use the built-in downgrading policy, or assert a zero-row count: scylla/tests/integration/session/pager.rs:test_iter_works_when_retry_policy_returns_ignore_write_error
- Adding a column during iteration eventually produces a later-page TypeCheckError: scylla/tests/integration/session/pager.rs:test_iter_methods_when_altering_table
- Each page has its own timeout, retry consistency, and fresh execution plan: scylla/tests/integration/session/pager.rs:test_pager_timeouts,test_pager_retry_receives_current_consistency,test_pager_consistency_reset_between_pages,test_pager_per_page_plan_prevents_exhaustion
- Token-unaware queries and prepared statements, including non-partition-key bind markers, page without partition-key errors; the system.peers cases require a peer row: scylla/tests/integration/session/pager.rs:test_token_unaware_pager
- Pagers keep the same coordinator node and switch after errors; these assertions compare host IDs, not shards: scylla/tests/integration/session/pager.rs:test_pager_coordinator_stability,test_pager_coordinator_switches_on_error
- Caching avoids repeat preparation and sets outgoing skip_metadata according to its option and negotiated metadata-ID support: scylla/tests/integration/session/caching_session.rs:test_caching_session_statement_cache,test_caching_session_metadata_cache
- Cached batch execution and explicit cached preparation handle unprepared, mixed, and fully prepared members and verify written rows; malformed members are rejected: scylla/tests/integration/session/caching_session.rs:test_batch
- Explicit and generated timestamps apply to request forms: scylla/tests/integration/statements/timestamps.rs:test_timestamp,test_timestamp_generator
- Returned coordinator descriptors identify actual serving targets: scylla/tests/integration/statements/coordinator.rs:test_exposed_request_coordinator
- A coordinator reports the selected connection's advertised shard when the requested shard has no connection; the proxy remaps advertised shards to zero and the test skips unless the node has at least two shards: scylla/tests/integration/statements/coordinator.rs:test_coordinator_shard_fallback
- Query, execute, prepare, batch, and initial automatic-page trace IDs support later trace lookup: scylla/tests/integration/session/tracing.rs:test_tracing
- Automatic-agreement failure propagates and successful agreement refreshes metadata through all six query/execute paging variants: scylla/tests/integration/session/schema_agreement.rs:test_schema_await_with_various_apis,test_schema_await_refreshes_metadata
- Raw USE responses trigger session-wide keyspace behavior: scylla/tests/integration/session/use_keyspace.rs:test_raw_use_keyspace
- History records each automatic page as a request, first-page completion, and ignored-error request success with an error/ignore attempt: scylla/tests/integration/session/history.rs:pager_query_history,pager_single_page_history,pager_ignore_write_error_history
- Paged and unpaged metric counts are separate: scylla/tests/integration/session/metrics.rs:test_paged_and_unpaged_metrics_are_counted_separately
- Preparation fallback contacts the complete expected shard set, with expected versus observed shard identities compared: missing:prepare-fallback-all-shards
- Coordinator stability preserves the prior shard, and unsharded-node filtering prevents duplicate node attempts: missing:pager-coordinator-shard-and-unsharded-filtering
- Later-page trace/coordinator descriptors become visible on consumption rather than background fetch: missing:pager-consumed-page-descriptors
- Unpaged APIs reject an injected continuation after core success without retrying it: missing:unpaged-unexpected-continuation
- Preparation rejects differing successful IDs; execute and batch repair reject changed PREPARED IDs, and batch repair rejects an error ID absent from its members: missing:prepared-identity-recovery-errors
- Single EXECUTE repairs its original statement despite an unrelated UNPREPARED error ID, while batch repair rejects an ID absent from its members: missing:unprepared-error-id-validation-boundary
- Cache reuse across keyspace changes explicitly documents query-text-only keys: missing:cache-keyspace-change-contract
- A stalled explicit prepare remains outside the statement request timeout: missing:prepare-timeout-boundary
- Timestamp generation is per outer attempt while internal reprepare retains its timestamp: missing:timestamp-retry-generation-boundary
- Every selected target uses PREPARE/EXECUTE for nonempty values and QUERY for empty values, with outgoing opcodes and successful results asserted: missing:unprepared-opcode-contract
- Empty pages and continued polling after type-check/row-decoding errors preserve the documented stream behavior: missing:pager-empty-page-type-check-and-error-continuation
- Repeated continuation bytes do not stop automatic paging, and successful empty pages are not limited by a whole-stream deadline: missing:pager-repeated-continuation-progress
- NO_METADATA without a supplied cache uses empty column specifications despite nonzero advertised counts, without validating unused row payload bytes: missing:uncached-no-metadata-response
- Post-binding overrides affect outgoing requests and execution policy selection, beyond stored-option getter checks: missing:bound-post-binding-override-execution
- NULL/UNSET partition-key entries are omitted from token calculation instead of producing a missing-index error: missing:prepared-null-unset-routing-boundary
- The textual BEGIN BATCH spanning non-counter tables verifies persisted values after prepared execution: missing:textual-multitable-batch-readback
- Batch repair and retry preserve only the cached first member's serialization: missing:batch-retry-serialization-boundary
- Bind metadata stays unchanged after transparent reprepare, while fresh explicit preparation accepts a changed bind type: missing:immutable-bind-metadata-after-reprepare
- Legacy metadata-cache opt-in can remain stale across schema changes without metadata-ID support: missing:legacy-cached-metadata-schema-change
- Explicit batch preparation drops outstanding member futures when it returns an error, while already sent preparation requests can finish; an earlier stalled member can delay delivery of a later error under dependency scheduling: missing:prepare-batch-partial-failure
- Cached batch preparation can precede rejection of the 65,536th member: missing:cached-batch-size-check-order
- Preparation reports the fallback pass's error and selects its first failure in connection order: missing:prepare-failure-selection
- Failed cached batch preparation preserves already completed cache insertions and evictions: missing:cached-batch-partial-failure
- A failed later page yields previously fetched rows before its error, then ends without restarting the fetch: missing:pager-terminal-failure-continuation
- Profile remapping races preserve the resolved profile for an in-flight request and all later pages: missing:execution-profile-remapping-snapshot
- Concurrent metadata updates preserve retained results/guards but can overwrite a newer or nonempty cache snapshot: missing:prepared-metadata-update-interleavings
- Equal-text concurrent cache misses and eviction preserve independent returned prepared handles: missing:cache-prepared-handle-lifetimes
- Caller cancellation and losing speculative branches preserve completed feedback without synthesizing attempt completion: missing:execution-cancellation-feedback-boundary
- Tablet-feedback queue saturation can time out an attempt after a server response, while successful enqueueing need not publish a new snapshot before return: missing:execution-tablet-feedback-backpressure
- Incomplete trace-session data defers event-row decoding, and trace readiness does not imply that every event has arrived: missing:trace-fetch-decode-order-and-completeness

### Go unit/race tests

#### Description

The future Go API must verify its chosen execution contract independently, including
generic serialization ownership, cancellation, retries, and concurrency. No Go
implementation is claimed by this reference document.

#### Test cases

- Typed binding does not reserialize values on retries or pages: missing:go-bound-values-reuse
- Cancellation safely releases request and page resources while responses may arrive late: missing:go-execution-cancellation-ownership
- Shared prepared metadata and profile updates remain race-free: missing:go-execution-shared-metadata-races
- Retry and speculative branches preserve idempotence and consistency contracts: missing:go-execution-retry-speculation
- Manual borrowing and owned-stream results enforce documented lifetimes: missing:go-result-lifetimes

### Go integration tests

#### Description

Existing gocql query, batch, paging, timeout, conditional-write, prepared-cache,
schema, tracing, and value tests must be mapped and migrated to the new API with
their assertions preserved. The root migration inventory will provide exact
legacy-to-new test mappings rather than declaring these requirements satisfied.

#### Test cases

- Every legacy statement and result integration test runs against the new API: missing:go-execution-integration-migration
- Real database recovery and paging exercise typed serialization paths: missing:go-execution-integration-recovery

### Go allocation benchmarks

#### Description

Measure steady-state request construction, bound execution, retry reuse, result
iteration, and page handoff with explicit buffer ownership. Buffer reuse must not
silently alias data still retained by application code or an in-flight request.

#### Test cases

- Bound execution reuses encoded values without intermediate value copies: missing:go-bound-execution-allocations
- Borrowed row iteration performs no per-cell payload copy: missing:go-row-borrowing-allocations
- Page handoff and retry preserve bytes without cloning full payloads: missing:go-page-retry-payload-reuse

## Test generation notes

Exercise empty and nonempty values, empty/final/intermediate pages, all three
execution modes, zero/one/many rows, NULL versus UNSET, mixed prepared/unprepared
batches, missing partition keys, and result metadata that changes during paging.
Inject failures before connection selection, during local preparation, during an
EXECUTE, on the internal reprepare resend, and after the database succeeds while
schema/keyspace side effects fail. Separate statement deadlines from preparation,
per-page deadlines, and automatic-agreement deadlines.

For concurrency, race profile remapping with request creation; race cache misses
for equal and unequal strings; change keyspace while reusing unqualified cache
keys; update metadata from concurrent results; drop a pager before, during, and
after a background fetch. Verify actual node/shard and request bytes, not only
eventual success. Compare default and downgrading retry policies and check both
regular serial consistency and an ordinary consistency with a serial option.
The Rust implementation's limitations must stay visible when the Go design
deliberately improves them; improvements are not retroactive Rust guarantees.
