# Connections review, round 04

- Section: `feature-specs/connections.md`.
- Round: 04 — whole-section audit with additional concurrency, cancellation,
  snapshot, lifetime, and race focus.
- Agent identity: `/root/review_connections_04`.
- Source checkout: `/extra/scylladb/gocql-rust-reference-f8b0ee9`.
- Verified source commit: `f8b0ee928dc0dcb475247dc82cce4e42804b0621`.
- Before SHA-256: `b8e9726c656b0827efe75d5d24746788504053c9dff1c80738abd5d099214d32`.
- After SHA-256: `1e7fcc0f4ecb9e0e1ab7d26ebc1cd0e8edf9f8e606e0db0fd4fba97e05007062`.
- Scope: read the complete review instructions, README, and assigned section;
  independently traced pinned implementation and test bodies. Prior reports
  were not used as proof. Edited only this report and the assigned specification.
  No Go/Rust implementation changes or test execution.

## Source inspected

Paths below are relative to the pinned Rust checkout unless stated otherwise.

- `scylla/src/network/connection_pool.rs`: `PoolSize`, pool states, pool ownership
  and construction (39–264); endpoint updates, keepalive hints, sharder access,
  selection, keyspace submission, initialization, snapshot acquisition
  (266–502); refiller fields and pending switches (514–605); complete worker loop,
  refill conditions and advanced-shard-awareness suspension (670–912); opening
  outcomes, keyspace recheck, host-ID diagnostics, endpoint/sharder capture,
  resharding (914–1211); publication, events, reference-identity removal
  (1213–1364); all keyspace and excess-limit methods (1366–1533); weak-reference
  error monitoring and attempt event fields (1536–1569).
- `scylla/src/network/connection.rs`: request ID allocation, submission and
  `OrphanhoodNotifier` (98–226); low-level defaults (473–501); socket timeout and
  router construction (520–618); authentication callbacks (823–927); USE,
  REGISTER and response parsing (1371–1524); TLS, router join, reader, stream
  allocation, writer, orphaner, keepaliver and event dispatch (1527–1983);
  translation, opening, discovery, negotiation, targeted-port iteration and
  socket configuration (2077–2426); orphan tracking, handler lookup, bitmap
  allocation and keyspace validation (2428–2646).
- `scylla/src/client/session.rs`: session defaults and validation (469–671);
  effective reconnect policy and data-connection configuration (1415–1480);
  public keyspace-switch behavior (2278–2339) and accessor (2395–2408).
- `scylla/src/cluster/node.rs`: pool creation, disabled nodes, preserved pool on
  endpoint change, state/sharder access and delegation (114–290).
- `scylla/src/cluster/worker.rs`: 32-element keyspace queue (81–82), request
  submission (220–235), retained pending fanout (274–279), serial worker branches
  and fanout aggregation (366–411), broken-connection classification and result
  aggregation (804–847).
- `scylla/src/policies/reconnect.rs`: complete policy implementation (1–246),
  including unstable gating, mutable per-pool state, jitter and limit validation.
- `scylla/src/routing/sharding.rs`: range and shard validation (11–107), candidate
  enumeration (143–237), SUPPORTED parsing (274–307), relevant tests (334–493).
- `scylla/src/errors.rs`: top-level source-address error classification (611–627).
- Test bodies in `connection_pool.rs` (1634–2063, 2066–2184, 2252–2580),
  `connection.rs` (3203–3465, 3473–3784),
  `scylla/tests/integration/session/write_coalescing.rs`,
  `scylla/tests/integration/session/status_change_hints.rs`,
  `scylla/tests/integration/session/host_id_mismatch.rs`,
  `scylla/tests/integration/session/use_keyspace.rs` (281–415), and
  `scylla/tests/integration/statements/coordinator.rs` (282–408).
- Supplemental dependency checks: the pinned `Cargo.lock` selects
  `futures-util 0.3.34` and `tokio 1.54.1`. Their locally cached
  `future/future/remote_handle.rs` and `sync/notify.rs` confirm asynchronous remote
  cancellation and notification registration semantics. Dependency evidence was
  used to check existing lifetime/lost-wakeup claims, not as a substitute for
  reading the pinned driver.

## Findings and fixes

1. **Cancellation before stream allocation can evade orphan tracking.** The
   existing text allowed queued cancelled work to be sent but omitted its resource
   lifetime. `RouterHandle::send_request` creates the notifier before queueing
   (`connection.rs:153–170`), `ResponseHandlerMap::orphan` discards an identity
   without a current stream (`2507–2516`), and the writer allocates and writes
   without checking whether the response receiver was dropped (`1774–1785`). If
   orphan processing wins that race, a later allocated stream has a handler but
   no orphan record and cannot contribute to the old-orphan count. Added this
   limitation, the response/router-termination release boundary, and a specific
   missing-test entry.

2. **Selection and failed-USE removal needed explicit snapshot boundaries.**
   `with_connections` retains one `load_full()` result (`connection_pool.rs:490–502`)
   and selection clones a connection reference (`471–487`); separate sharder,
   connectivity and selection calls acquire separately. `update_shared_conns`
   replaces the published snapshot (`1216–1240`) and removal uses reference
   identity (`1306–1350`), without revoking older owners. Added the single-selection
   coherence boundary and the possibility of selection from an older snapshot.
   Qualified the failed-USE prose and invariant: after the worker handles a
   non-broken USE error or timeout, later snapshots exclude that connection;
   already retained references can still use it.

3. **Late connection attempts have no endpoint or sharding generation filter.**
   `start_opening_connection` clones endpoint/configuration and captures sharder
   and port (`connection_pool.rs:1131–1183`). Completion first rechecks keyspace,
   then uses the connection's discovery to reshard and update the advertised port
   (`964–1016`, `1186–1211`); it does not reject superseded endpoint or discovery
   results. Added that old-endpoint connections and older discovery results can
   still be accepted after newer state was published. Added a missing regression
   target and test-generation interleavings.

4. **Pool keyspace-switch cancellation and snapshot lifetime were omitted.**
   Pool requests transfer the keyspace and response sender into the queue
   (`connection_pool.rs:428–443`), while the worker owns the pending operation and
   its strong connection references (`596–604`, `1375–1411`). Dropping the waiting
   caller does not withdraw an accepted request; response delivery failure is
   ignored (`1383`, `1454`, `1476`). Resharding/removal does not edit the switch's
   outstanding set. Documented continued work after caller cancellation and
   retention/aggregation of removed connections. Also clarified that the deadline
   takes effect when its worker branch wins: completion and deadline branches
   compete in the same select (`726–738`), without deadline-first priority. Added
   a missing regression target covering all three boundaries.

5. **The pool-empty signal is an exception to notification ordering.**
   Initialization waiters and connectivity events follow snapshot swap
   (`connection_pool.rs:1233–1240`), but removal sends the separate pool-empty
   signal before `update_shared_conns` (`1343–1349`). The previous invariant's
   broad notification wording obscured this exception. Documented that its
   receiver can wake while the published snapshot still appears Ready, and
   narrowed the invariant. Added a missing ordering test.

6. **Session-level keyspace serialization and reported-name state were missing.**
   The cluster queue has capacity 32 (`worker.rs:81–82`), and the worker gates the
   next request on completion of the previous retained fanout (`366–411`). This
   is stronger than only serializing each pool; the worker can still process
   other events while the fanout is pending. Queued requests survive caller
   cancellation because final response delivery failure is ignored (`389–396`).
   Added these boundaries. Separately, `Session::use_keyspace` stores its reported
   name before validation and enqueueing (`session.rs:2325–2339`), and
   `get_keyspace` returns that separate retained value (`2395–2408`). Added that
   the accessor can disagree with pools after invalid input, failure, cancellation
   or concurrent calls. Added pinned evidence links, two existing integration
   test references, and an explicit missing accessor-race/failure test.

7. **Two test descriptions needed more precise expectations.** The TCP-options
   test establishes connections and inspects buffers/linger/reuse-address, while
   its proxy rejects application queries (`connection.rs:3376–3465`); narrowed
   “preserve working connections” accordingly. The persistent-orphan tolerance
   test submits exactly 1024, while the shutdown and short-spike tests submit
   1025 (`3573–3694`); replaced the vague count description with those exact cases.

## Behaviors checked without further changes

- Session versus low-level test defaults; zero keepalive interval rejection;
  absent interval/timeout semantics; default exponential policy under either
  public feature configuration; nonzero pool and millisecond-delay types.
- TCP-only timeout scope, wildcard/family-specific socket binding, socket option
  application and buffer-size casts, TLS-before-router ordering, separate
  authentication callback lifetime, discovery parsing, matching TLS shard-port key
  and first-value parsing, compression fallback, REGISTER requiring READY and
  ordinary data connections not registering.
- Initializing/Ready/Broken states; first-connection rather than full-pool
  initialization; waiter creation before state inspection; disabled-node errors
  and no-op hints; endpoint changes preserving existing pool handles.
- Uniform within-bucket selection, random shard fallback, non-16-bit and
  representable out-of-range shard handling, lack of stream-capacity reservation.
- Immediate first fill, single ordinary attempt when empty, concurrent per-host
  or per-shard filling, pending-batch blocking, policy callback scheduling,
  ordinary/keyspace failure flags, targeted fallback, hint resets, jitter bounds,
  excess retention/cleanup, and failed-fill preservation of a nonempty Ready pool.
- Candidate-port enumeration and only top-level retryable address errors;
  repeated translation and per-candidate TCP timeout; original attempt sharder
  used for NAT mismatch attribution; 300-second suspension without extending an
  active deadline; actual-shard acceptance and host-ID mismatch as diagnostic only.
- Serialization before the 1024-element submission queue, wrapping request IDs,
  all 32768 stream IDs, lowest-free allocation, exhaustion behavior, 8192-byte
  buffers, coalescing on an observed empty queue, and request-level body parsing.
- Out-of-order dispatch, orphan-versus-response lookup, stale cancellation after
  ordinary stream reuse, negative streams, awaited event backpressure, fatal
  error fanout, keepalive sharing the queue/stream budget, raw-response-only
  keepalive success, hint retention/coalescing, and independent TCP probes.
- Keyspace validation/quoting and case-insensitive response comparison;
  new-connection keyspace rechecks and separate timeout; no-op empty/disabled-node
  successes; aggregation of successful, broken and non-broken outcomes;
  desired-keyspace persistence after failure; no application-request barrier.
- Remote-handle lifetime, weak error monitors, retained connection ownership,
  absence of graceful draining, and explicit separation of Rust allocation
  behavior from future Go ownership/allocation requirements.

## Test evidence and remaining gaps

Read the named test bodies; none was executed. Confirmed source-port uniqueness
and no-candidate cases, sharding validation, setup failure preserving pool state,
manually invoked and actual USE deadlines, advanced-awareness suspension,
resharding/mismatch classification, metrics-gated cleanup, host-ID discovery and
diagnostics, keepalive timeout, orphan count/age scenarios, coalescing smoke
coverage, targeted DOWN hints and coordinator shard fallback. The existing
resharding metric test begins with no excess connections, matching the explicit
coverage gap. Host-ID integration assertions skip servers without the advertised
field; coordinator fallback skips servers advertising fewer than two shards.
The concurrent session keyspace test delays the first fanout and asserts return
order; the accessor test covers initial/configured/success cases, not failure or
concurrency. None of these supplies the new missing race/lifetime cases.

Static validation succeeded: all 32 named Rust test references resolve to
functions in the pinned source, and all 28 pinned source links resolve to existing
files and valid line ranges. `git diff --check` passed for the assigned spec and
report. No runtime, database, Go conformance, race, or allocation result is
claimed. The newly recorded concurrency interleavings remain source-derived
behavior requiring regression coverage; existing missing-test entries remain
explicit.

Cross-section note sent to the parent: execution delegates keyspace-switch rules
to this section, so the new session serialization/accessor distinctions belong
here; no other section was edited. This round found no additional confirmed
cross-section contradiction requiring another owner's edit.
