# Connections review, round 10

- Section: `feature-specs/connections.md`.
- Round: 10, final comprehensive fidelity and completeness audit.
- Agent identity: `/root/review_connections_10`.
- Reference checkout: `/extra/scylladb/gocql-rust-reference-f8b0ee9`.
- Source commit: `f8b0ee928dc0dcb475247dc82cce4e42804b0621`; confirmed with `git rev-parse HEAD`, and the reference checkout had no changes in `git status --short`.
- Before SHA-256: `496ccd0e652c82613e502a415fddb4552621c6801b860340e49aeb3a93f4331d`.
- After SHA-256: `496ccd0e652c82613e502a415fddb4552621c6801b860340e49aeb3a93f4331d`.

## Findings and disposition

**Zero verified findings.** No specification edit was warranted. This round read
the review instructions, README, complete connections specification, actual
implementation, and cited test assertions independently. Previous review reports
were not used as proof. No Go or Rust implementation was changed, and no test
execution is claimed.

## Implementation and behavior checked

Paths below are relative to the pinned reference checkout. These are the files
and symbols actually inspected; ranges identify the relevant source rather than
claiming that every unrelated feature in a shared file was reviewed.

| Evidence inspected | Checked behavior and result |
| --- | --- |
| `scylla/src/client/session.rs:469–671, 1407–1486`; `scylla/src/network/connection.rs:277–470`; `scylla/src/policies/mod.rs:25–28` | Session defaults, socket-option mapping, nonzero keepalive validation, test-only keepalive defaults, coalescing enable/delay mapping, and configured/default reconnect policy agree with the spec. The public reconnect module requires both gates. |
| `scylla/src/network/connection.rs:520–714, 823–927, 1418–1457, 1527–1623, 2077–2426` | Followed translation through TCP, TLS/router creation, OPTIONS, discovery, STARTUP, authentication and REGISTER. Translation and TLS are outside the TCP timeout; authenticator callbacks lack an umbrella deadline. Data configs have no event registration or configuration reporter. Checked family-specific sockets, explicit source address, option application before bind, buffer casts, and platform-specific TCP probe settings. |
| `scylla/src/routing/sharding.rs:11–42, 85–103, 126–307`; `scylla/src/errors.rs:611–627` | Inclusive source-port bounds, all first-value sharding parses, nonzero count, shard-ID validation, lack of ignored-MSB range validation, random pivot/wrap enumeration, helper assertions/panic, no-candidate error and the three top-level retryable I/O classifications match. Each port candidate reruns the whole opener with a separate TCP timeout. |
| `scylla-cql/src/frame/response/supported.rs:15–22`; `scylla-cql/src/frame/types.rs:347–358`; `scylla-cql-core/src/frame/protocol_features.rs:64–143`; `scylla/src/network/connection.rs:2136–2292` | SUPPORTED duplicate keys replace lists; malformed map decoding fails setup. Verified matching-mode shard-port/host-ID first-value parsing, independent compression fallback, protocol feature opt-ins, session identity, and diagnostic-only host-ID behavior. |
| `scylla/src/network/connection_pool.rs:38–502`; `scylla/src/cluster/node.rs:114–290` | Per-host/per-shard targets, Initializing/Ready/Broken states, initialization notification ordering, disabled-node no-ops/errors, Ready-based connectivity, optional sharder and first-published-connection product detection agree. Selection retains one snapshot, selects uniformly within a bucket, randomizes fallback shards, handles oversized/out-of-range hints, and does not reserve stream capacity. |
| `scylla/src/network/connection.rs:2005–2015`; `scylla/src/network/connection_pool.rs:316–334` | Checked every ScyllaDB detection signal against the actual predicate, including valid port without sharding, LWT/rate-limit fields, tablets v1/v2, metadata IDs, and exclusion of host ID. No pool-wide feature agreement is inferred. |
| `scylla/src/network/connection_pool.rs:510–912`; entire `scylla/src/policies/reconnect.rs` | Immediate first fill, singleton empty-pool attempt, concurrent missing-count fill, per-host prohibition on targeted fills, completion-gated scheduling, flag-driven callback timing, jitter/clamping, constant policy and validation all match. Refill hints reset scheduled state without cancelling pending attempts. A stalled opening future blocks later batches. |
| `scylla/src/network/connection_pool.rs:914–1211, 1512–1533` | Traced ordinary/targeted/keyspace setup failure branches, keyspace recheck before acceptance, host-ID diagnostic timing, actual-shard classification, immediate ordinary replacement, excess limits, absence of excess promotion, full-pool cleanup, and reshard clearing. Endpoint/sharder/port captures have no generation rejection; every completed ready connection can replace discovered sharder/port. |
| `scylla/src/network/connection_pool.rs:811–850, 987–1003` | Same-attempt-sharder mismatch disables advanced awareness for 300 seconds, repeated active blocks do not extend it, and changed-sharder results do not establish source-port rewriting. Expiration only reopens eligibility for later fills. |
| `scylla/src/network/connection.rs:70–226, 577–618, 1459–1525, 1624–1983, 2428–2586` | Checked serialization before the 1024-slot submission queue, wrapping 64-bit request IDs, lowest-free allocation of all 32768 stream IDs, request-only exhaustion, reader/writer 8192-byte buffers, observed-empty-queue coalescing, out-of-order dispatch, shared fatal errors, and deferred ordinary-body parsing. |
| `scylla-cql/src/frame/mod.rs:150–200`; `scylla/src/network/connection.rs:1680–1743, 1939–1983` | Entire frame/header validation precedes negative-stream filtering; ignored bodies are not decoded. Event decoding, wrong response kind, receiver closure and receiver backpressure match the stated reader behavior. Event parsing uses original router compression/default features rather than later negotiated connection fields. |
| `scylla/src/network/connection.rs:138–226, 1829–1937, 2428–2551`; `scylla/src/network/connection_pool.rs:202–264, 670–783, 1535–1550` | Cancellation mapping removal, orphan reservation/late-response discard, cancellation-before-allocation gap, one-second age/count checks and strict count greater than 1024 are accurately distinguished. Retained connection/pool handles own task lifetimes; weak monitors do not keep discarded connections alive. No graceful drain or server rollback is promised. |
| `scylla/src/network/connection.rs:1865–1937, 2025–2043`; `scylla/src/network/connection_pool.rs:300–314` | Periodic keepalive skips its instant tick, uses delayed missed ticks, consumes/coalesces hints, and applies timeout to submission plus raw response. Disabled keepalive remains disabled. Raw receipt does not validate SUPPORTED, and keepalive stream exhaustion is connection-fatal. Hints reach published active connections only. |
| `scylla/src/network/connection.rs:1370–1416, 2589–2646`; `scylla/src/network/connection_pool.rs:431–447, 967–982, 1366–1510`; `scylla/src/client/session.rs:1775–1789, 2325–2339, 2395–2408`; `scylla/src/client/pager.rs:1192–1221`; `scylla/src/cluster/worker.rs:81–89, 366–411, 804–847` | Verified validation/quoting, case-insensitive response comparison, raw USE and pager propagation, pre-validation reported keyspace, cluster/pool queue capacities, serialized snapshot fanouts, cancellation survival, current-keyspace inheritance, non-broken failure removal, shared deadline arbitration, desired-state retention after errors, broken-error aggregation, and empty/disabled-node no-op successes. |
| `scylla/src/network/connection_pool.rs:1213–1364`; `scylla/src/cluster/node.rs:114–185`; `scylla/src/cluster/state.rs:419–479`; `scylla/src/cluster/worker.rs:760–801` | Snapshot publication precedes initialization/connectivity notices; the separate last-active pool-empty signal precedes publication. Verified all transition-event cases, ignored send errors, unconditional data-pool internal sender, listener-independent recorded status update, address-only pool reuse, and replacement/retained-pool boundaries. |
| `scylla/src/policies/load_balancing/plan.rs:94–107`; `scylla/src/client/execution.rs:246–254, 560–647` | An absent policy hint is resolved before pool selection, with potentially different snapshots. Selection failure skips a plan target without retry-policy consultation; failure after acquisition reaches the retry policy. |

These checks cover every named behavior subsection and all nine listed
invariants. Request framing ownership, reference cloning, vector collection,
and fallback shard-vector allocation support the explicit refusal to infer a
Go zero-copy or allocation contract from Rust.

## Catalogue, links and cross-section boundaries

The machine-readable catalogue assigns four production modules to connections:
`network/connection.rs`, `network/connection_pool.rs`, `network/mod.rs`, and
`policies/reconnect.rs`. Their connection/pool contracts are covered above.
`network/mod.rs` was read in full to verify public exports and internal constants.
`book_tests.rs`, the shared test-evidence assignment, was read as an include-based
documentation test harness; it was not counted as executed conformance coverage.
Shared query/serialization, TLS/authentication, metadata and routing responsibilities
were followed through the specific caller/callee paths above.

A static check found **50 pinned source links**, all at the requested commit with
existing files and valid line ranges. The associated symbols were inspected in
source. Every local Markdown destination and heading anchor also resolves.

Compared the adjacent specs' shard hints/source-port helpers, execution
selection-versus-attempt and post-success keyspace boundaries, metadata node/pool
reuse and host-listener contract, configuration socket/compression behavior, and
protocol frame/ownership rules. No cross-section discrepancy requiring a report
to the parent was found. Detailed protocol, configuration, routing and execution
contracts remain owned by those sections.

## Test evidence checked

A symbol scan found all **39 named Rust test targets** in this specification.
Their relevant bodies and helpers were then read, including the following
assertion limits. This was source inspection only; no Rust, Go, proxy, CCM or
database test was executed.

| Test source and inspected range | Assertions checked against the specification |
| --- | --- |
| `scylla/src/routing/sharding.rs:334–505` | Seven cited tests cover range constructor acceptance/rejection, three shard-ID cases, complete nonduplicated port enumeration, an empty custom range, and the public draw helper's no-choice panic. They do not establish statistical uniformity or all maximum boundaries. |
| `scylla/src/network/connection_pool.rs:1648–2061` | Four keyspace tests distinguish a manually invoked switch timeout from an actual unanswered USE deadline, verify setup failures leave refill needed/Broken state, and preserve an existing nonsharded pool until successful setup permits resharding. Metrics assertions are feature-gated. |
| `scylla/src/network/connection_pool.rs:2063–2137, 2166–2183` | The many-connections helper opens 400 source-port-selected connections in each socket configuration without forcing collision or validating shard assignment. The temporal test checks eligibility after 299 and 301 seconds, not the exact boundary or real refill. |
| `scylla/src/network/connection_pool.rs:2254–2493` | Three metrics-gated cleanup tests establish the stated accounting scenarios; resharding begins without excess. Two mismatch tests synthesize requested-shard metadata around ordinary opening and distinguish two obsolete-sharder results from a current-sharder mismatch. They do not exercise real NAT rewriting. |
| `scylla/src/network/connection_pool.rs:2496–2650`; `scylla/src/network/connection.rs:3206–3274` | Host-ID valid/malformed/missing handling and diagnostics for matching, mismatching, missing and keyspace-delayed connections match the claims. |
| `scylla/src/network/connection.rs:3277–3466, 3473–3786` | Keepalive failure uses a paused clock/in-memory transport; socket options are observed after a dry handshake that returns query errors. All five orphan tests distinguish 1025 stuck requests, short spikes, exactly 1024 old requests, sustained below-threshold traffic and gradual above-threshold accumulation. They do not establish all allocation/cancellation races. |
| `scylla/src/cluster/state.rs:1146–1186` | Address change into a rejected host-filter address asserts a disabled replacement node and non-reuse of the old object. It does not test complete retained-pool lifetime behavior. |
| `scylla/tests/integration/session/use_keyspace.rs`, complete file | Five tests cover successful session/builder and raw/paged USE, the named invalid names, nonexistent-keyspace failure and later recovery after waiting for connectivity, distinct quoted/unquoted contents, accessor success states and delayed fanout completion order. The spec correctly leaves invalid/cancelled/concurrent accessor semantics and richer cancellation assertions as gaps. |
| `scylla/tests/integration/session/host_id_mismatch.rs`, complete file; `scylla/tests/integration/session/new_session.rs:125–133`; `scylla/tests/integration/ccm/using_timeout.rs`, complete file | Wrong-node translation diagnostics skip assertions without host-ID advertisement. Product detection compares the Cassandra build flag. The CCM test disables shard advertisement, checks detection, captures at least one control QUERY/PREPARE and checks every captured body for `USING TIMEOUT 2137ms`. |
| `scylla/tests/integration/session/write_coalescing.rs`, complete file; `scylla/tests/integration/session/status_change_hints.rs`, complete file; `scylla/tests/integration/statements/coordinator.rs:282–410` | Coalescing proves results for three modes rather than latency/syscall counts. DOWN hints are checked on each targeted node with control traffic excluded and a distant periodic interval. Shard fallback observes rewritten advertised shard zero in query and pager paths and skips without at least two shards. |

## Remaining uncertainty and gaps

The specification's `missing:` entries remain coverage requirements, not claims
that tests ran or that their asserted outcomes were demonstrated. In particular,
there is no new runtime evidence here for source-port rewriting/resumption,
whole setup timeout boundaries, stalled-refill recovery, cancellation races,
snapshot lifetimes, exact timer boundaries, duplicate wire keys, ignored-frame
body boundaries, or keyspace cancellation/aggregation edge cases. Inspection
supports their described implementation behavior, but does not establish
database interoperability, runtime scheduling guarantees, or Go conformance.

No additional unresolved specification ambiguity was found within this section's
scope. The unchanged before/after hash records that the tenth review did not
manufacture an edit merely to produce a finding.
