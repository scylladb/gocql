# Connections review — round 03

- Section: `feature-specs/connections.md`
- Round: 03, with additional focus on errors, partial success, failover, retry,
  and recovery; the complete section was audited.
- Agent identity: `/root/review_connections_03`
- Source checkout: `/extra/scylladb/gocql-rust-reference-f8b0ee9`
- Source commit: `f8b0ee928dc0dcb475247dc82cce4e42804b0621`, confirmed with
  `git rev-parse HEAD`; the source working tree was clean.
- Before SHA-256: `2dba91f45339cebd669b4f86fac46575e0063b83b7e065fde1b7e7f9ee96a711`
- After SHA-256: `b8e9726c656b0827efe75d5d24746788504053c9dff1c80738abd5d099214d32`

## Scope and inspected source

Read `feature-specs/reviews/REVIEW-INSTRUCTIONS.md`, `feature-specs/README.md`,
and the complete connections specification. Behavior was traced independently
from the pinned source, without using prior reports as proof. Only the assigned
specification and this report were edited; no Go or Rust code was changed.

The following files and symbols were inspected (line numbers refer to the pinned
checkout):

- `scylla/src/network/connection_pool.rs:38–1575`: `PoolSize`, pool states,
  `NodeConnectionPool` selection, initialization and hint methods;
  `PoolRefiller::run`, refill eligibility/scheduling, opening and setup handling,
  resharding, publication, removal, keyspace switching and deadlines, excess
  limits, and weak error monitors.
- `scylla/src/network/connection.rs:70–618,620–716,823–927,1371–1983,2077–2646`:
  request submission and orphan notification, configuration and socket creation,
  setup request validation, authentication callbacks, USE verification,
  serialization/response parsing, router/reader/writer/orphaner/keepaliver,
  event handling, translation/opening, source-port retries, socket options,
  stream ownership/allocation, and keyspace-name validation.
- `scylla/src/client/session.rs:469–671,1415–1488`: effective defaults,
  socket-option mapping, validation, and production connection/pool wiring.
- `scylla/src/policies/reconnect.rs:1–246`: unstable gate, per-pool policy state,
  exponential/constant delays, callback effects, jitter, and validation.
- `scylla/src/routing/sharding.rs:11–307`: source-port range validation,
  `ShardInfo` construction/parsing, sharder equality fields and candidate
  enumeration.
- `scylla/src/errors.rs:551–627`: pool/connection error variants and the exact
  top-level I/O error classification for source-port retries.
- `scylla/src/cluster/node.rs:114–290`: enabled/disabled pools, endpoint changes,
  selection delegation, keyspace no-op success, initialization and hints.
- `scylla/src/cluster/worker.rs:389–411,804–847`: node-level keyspace fan-out,
  broken-connection classification, and success/error aggregation.
- Test implementations in `connection_pool.rs:1634–2649`,
  `connection.rs:3203–3785`, `sharding.rs:334–493`, and the four integration files
  listed under test coverage below.

## Verified findings and fixes

### 1. A permanently pending setup attempt can prevent pool recovery

The specification described persistent retry and setup timeout boundaries but
did not state their combined liveness limitation. `need_filling` requires the
set of opening/setup futures to be empty; an immediate-refill hint resets policy
state but leaves those futures intact
([pool worker and fill eligibility](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/network/connection_pool.rs#L670-L814)).
Translation runs before connection construction, and TLS completes before the
router is spawned
([translation/opening](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/network/connection.rs#L2077-L2151),
[TLS/router construction](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/network/connection.rs#L1527-L1611)).
Authentication callbacks are awaited directly without a timeout or selection
against the router's error receiver
([perform_authenticate](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/network/connection.rs#L823-L869)).

Fixed the setup and refill sections to state that keepalive cannot bound
translation/TLS or impose a callback deadline, and that a stuck setup attempt
can prevent later batches even after all active connections disappear. A stalled
CQL setup request can remain pending if keepalive is disabled or continues to
receive responses. Added an explicit missing test for this refill obstruction.

### 2. Stream exhaustion has a fatal keepalive path

The ordinary allocator returns `UnableToAllocStreamId` to the individual request
and the writer continues
([allocation and writing](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/network/connection.rs#L1746-L1818)).
However, keepalive uses the same queue/allocator and wraps every request error in
`KeepaliveRequestError`; returning that error terminates the router
([keepaliver](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/network/connection.rs#L1860-L1936),
[router failure fan-out](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/network/connection.rs#L1639-L1677)).

Made this exception explicit beside the nonfatal ordinary-request exhaustion
description, and added a missing test distinguishing the two paths. The existing
description of any keepalive request failure as fatal remains correct.

### 3. Cluster keyspace success can be supplied entirely by no-op results

An empty pool records the desired keyspace and returns `Ok(())` without sending
USE
([pool keyspace handling](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/network/connection_pool.rs#L1370-L1385)).
A disabled node also returns `Ok(())`
([Node::use_keyspace](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/cluster/node.rs#L260-L267)).
The cluster joins all node results and passes them through the same aggregator,
which accepts a success plus only broken-connection failures
([fan-out](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/cluster/worker.rs#L399-L411),
[aggregation](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/cluster/worker.rs#L817-L847)).

Added the cluster-level boundary: overall success need not establish that any
server validated the keyspace. Added the caller to the evidence table and a
missing test for no-op aggregation. Sent this cross-section fact to the parent
for consistency checking in execution/metadata specifications.

### 4. Named keyspace-switch tests do not demonstrate completed recovery

`keyspace_switch_failure_removes_connection` checks removal, returned errors,
pending-switch cleanup, and optional metrics; its timeout subcase calls the
timeout handler directly. `unanswered_use_keyspace_times_out` exercises actual
deadlines for new-connection keyspace setup and a live pool switch. Neither
waits for a successful replacement connection after a failed pool switch
([the two tests](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/network/connection_pool.rs#L1645-L1840)).

Reworded their test-place claim to identify the asserted behavior and separated
successful post-switch refill into an explicit missing test. The distinct
`keyspace_setup_failure_triggers_refill` unit test does assert `need_filling()`;
its existing narrower claim was retained.

## Checked behaviors without further findings

- Session defaults versus test-only defaults; nonzero interval validation;
  socket-option casts, application order and propagation; TCP versus CQL
  keepalive independence; source-IP family handling.
- Translation bypass rules; OPTIONS discovery fallback; first-value parsing and
  TLS-specific shard-port selection; compression fallback; setup response/error
  validation; REGISTER only when events are configured; diagnostic-only host ID.
- Initializing/Ready/Broken errors, partial readiness, wait registration,
  disabled-node selection, preservation of pools on endpoint changes, and
  snapshots that can briefly retain a broken connection.
- Uniform selection within a shard, shard-first random selection, missing-shard
  fallback, invalid-shard normalization, and absence of stream-capacity-aware
  selection or automatic cross-node failover in the pool.
- Empty-pool single attempts, concurrent deficit fills, per-host/per-shard
  differences, targeted ordinary-port fallback, error-flag/backoff sequencing,
  excess retention/cleanup, and sharder/port rediscovery after keyspace setup.
- Source-port candidate enumeration, top-level retryable I/O classification,
  fresh translation/TCP timeout per candidate, and the 300-second NAT mismatch
  block versus genuine resharding, including later obsolete-generation attempts.
- Bounded submission versus unbounded waiting callers, owned serialization,
  32768 lowest-free stream IDs, request-ID wrap, coalescing boundaries, response
  association, fatal unexpected streams/events, and shared fatal-error fan-out.
- Cancellation mapping removal, orphan reservation/late response disposal,
  ignored pre-allocation notifications, the strictly-greater-than-1024 old-orphan
  count, weak monitors, retained-reference lifetime, and lack of graceful drain.
- Keepalive timing/hints, timeout covering queue submission, raw-frame-only
  acceptance, and event backpressure on the reader.
- Keyspace validation, shared snapshot deadline, removal of live USE failures,
  timeout removing outstanding connections, no desired-keyspace rollback,
  new-connection rechecks, and aggregation after all snapshot responses.
- Publication before initialization wakeups and Established/Lost events; separate
  pool-empty signal; no events for Ready-to-Ready or Broken-to-Broken updates.
- Language-independent behavior is distinguished from Rust data structures and
  proposed Go tests/allocation contracts. Existing invariants remain supported.

## Test coverage and validation

Read the named source tests and checked their assertions, rather than inferring
coverage from names. All 30 named test references resolve to functions. The 25
pinned source links have existing files and valid line ranges.

- Sharding tests cover default/reversed/zero-start port ranges, four-shard
  enumeration without duplicates, an empty candidate range, invalid shard IDs,
  and the highest valid ID of the tested configuration.
- Pool tests cover keyspace setup failures, switch errors/deadlines, temporary
  advanced-awareness blocking, simulated mismatch/resharding, excess cleanup,
  and host-ID diagnostics. The three metric cleanup tests require `metrics`.
  The resharding cleanup test begins without excess connections. `many_connections`
  opens 400 connections with each of two socket configurations; it does not
  prove that real shard assignment follows the requested source port.
- Connection tests cover valid/malformed/absent SUPPORTED host IDs; keepalive
  timeout and subsequent query failure; socket option propagation; 1025 stuck
  orphans versus tolerated 1024; short spikes; and sustained orphan traffic.
  Several use simulated in-memory protocol transports and paused clocks.
- Read `scylla/tests/integration/session/write_coalescing.rs:test_coalescing`:
  it compares returned data across yield, 1 ms, and disabled coalescing, not
  latency or syscall counts.
- Read `scylla/tests/integration/session/host_id_mismatch.rs:mismatched_host_id_is_reported`:
  its three-node translator scenario skips assertions without `SCYLLA_HOST_ID`.
- Read `scylla/tests/integration/session/status_change_hints.rs:80–182`:
  the test observes OPTIONS on the target node with one connection per node;
  it does not demonstrate recovery of a failed node.
- Read `scylla/tests/integration/statements/coordinator.rs:282–410`:
  rewritten SUPPORTED shard metadata exercises fallback and reports shard zero
  for unpaged and paged queries; it skips below two advertised shards.

No Rust tests, Go tests, benchmarks, or live-database tests were run in this
documentation review. `git diff --check` was clean for the owned specification.
Source inspection supports the documented behavior but does not establish a
runtime pass. Existing missing coverage remains explicit, with new gaps for
stalled setup/refill, keepalive exhaustion, and actual keyspace recovery/no-op
aggregation. No remaining verified contradiction was left in the owned file.
