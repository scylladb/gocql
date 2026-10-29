# Connections review, round 07

- Section: `feature-specs/connections.md`
- Round: 07 — source links, anchors, and implementation-versus-contract precision
- Agent: `/root/review_connections_07`
- Reference: `/extra/scylladb/gocql-rust-reference-f8b0ee9`
- Source commit verified with `git rev-parse HEAD`: `f8b0ee928dc0dcb475247dc82cce4e42804b0621`
- Before SHA-256: `90f81be77f1b6ff8d6e2aebc459816f8c8798b1d625051f30b410d15d672745a`
- After SHA-256: `42dafe64ecf8151af2d3f06384c4f50484f7cc4c1acb13fb4e9a6614619eca98`

Read the complete review instructions, section README, and assigned specification.
Traced the pinned implementation directly; previous reports were not used as
proof. Only the assigned specification and this report were changed.

## Findings and fixes

1. **Per-pool policy sessions do not enforce isolation of custom state.** The
   specification said a custom factory supplies “independent mutable state” for
   every pool. `PoolConfig::to_host_pool_config` calls `new_session()` once for
   the pool (`connection_pool.rs:83–97`), and the returned boxed object is retained
   by `PoolRefiller`. `ReconnectPolicy::new_session` (`reconnect.rs:30–38`) does
   not constrain state a custom object shares internally. Reworded the contract
   to state the actual per-pool factory call and retained session, while explicitly
   permitting internally shared custom state. The built-in exponential factory
   does allocate its own mutable delay state (`reconnect.rs:163–171`).
2. **The backoff evidence entry did not link the actual public-visibility gate.**
   The existing `reconnect.rs:1–246` anchor contains the policy code, a descriptive
   gate comment, and conditional lint attributes; public-versus-crate visibility
   is controlled in `policies/mod.rs:25–28`. Added that exact gate link, the
   per-pool factory call, and `session.rs:1412–1424`, which selects the configured
   policy when customization is available and constructs the default otherwise.
   The underlying behavior statement was correct; its implementation evidence is
   now direct.

## Implementation inspected and behaviors checked

- `scylla/src/network/connection_pool.rs:38–502`: default pool size, per-host and
  per-shard sizing, Initializing/Ready/Broken state errors, retained snapshots,
  endpoint updates, connectivity and product observations, initialization
  notification registration, uniform selection, missing-shard permutation,
  invalid shard conversion, one-element shortcut, and hint forwarding.
- `connection_pool.rs:614–1595`: immediate first fill, empty-pool single attempt,
  pending-batch barrier, backoff callbacks at scheduling time, hint resets,
  advanced-port eligibility and suspension, targeted ordinary-port fallback,
  excess connections and bounds, host-ID diagnostics after keyspace setup,
  sharder/port rediscovery, old in-flight endpoint results, snapshot publication,
  connectivity transitions, pool-empty notification order, weak error monitors,
  serialized pool keyspace requests, shared switch deadline, removal and result
  aggregation, and separate new-connection keyspace deadlines.
- `scylla/src/network/connection.rs:70–226,434–618,620–716,823–927,930–993,
  1280–1315,1370–1458,1459–2015,2077–2646`: submission queue and cancellation
  notifier; low-level test defaults; TCP deadline versus TLS/router construction;
  OPTIONS/STARTUP/authentication/REGISTER results; negotiated compression and
  ordinary response parsing; reader/writer/coalescing buffers; negative streams
  and event dispatch; shared fatal errors; keepalive raw-frame handling and hints;
  orphan timing/accounting and all 32768 stream IDs; product signals; translation;
  shard-port and host-ID first-value parsing; source-port retry scope; socket
  options and TCP probes; USE response checks and keyspace-name validation.
- `scylla/src/routing/sharding.rs:11–307`: nonempty inclusive port ranges,
  required sharding fields, invalid shard rejection, unvalidated ignored-MSB
  range, default-range draw/iteration helpers, uniform starting candidate,
  wraparound, and empty candidate sets. `scylla/src/errors.rs:611–627` restricts
  candidate retry to the three top-level I/O kinds described in the specification.
- `scylla/src/policies/reconnect.rs:1–246` and `policies/mod.rs:25–28`: exponential
  defaults and clamps, per-pool factory interface, constant policy, configurable
  jitter and explicit assertions, and unstable public visibility.
- `scylla/src/client/session.rs:469–671,1412–1424,1775–1789,2325–2339,
  2395–2408` and `client/pager.rs:1192–1221`: effective defaults and zero-interval
  rejection, policy fallback, raw-USE/pager propagation, keyspace reporting before
  validation, and the accessor's retained snapshot.
- `scylla/src/cluster/node.rs:114–290` and `cluster/worker.rs:81–89,220–239,
  366–411,804–847`: disabled-node behavior, pool preservation on address changes,
  32-element cluster keyspace queue, serialized fanouts, node no-op success, and
  the exact broken-connection aggregation exception.
- `scylla-cql/src/frame/mod.rs:249–285` and
  `scylla-cql-core/src/frame/protocol_features.rs:38–130`: response compression
  flag handling and feature parsing used by product detection. Checked that
  ordinary responses use negotiated configuration whereas event parsing retains
  the original router configuration and default protocol features.

All existing source anchors were valid and contained the named symbols. A final
local check validated all **40** pinned source URLs and line ranges after the
evidence additions; all relative specification links resolve. This was backed by
the source tracing above, not treated as proof merely from file existence.

## Test coverage checked

Located all **38 named Rust test symbols in 28 reference groups**. Read their
relevant bodies and assertions, including:

- Sharding tests at `routing/sharding.rs:333–505`: range constructor, ID bounds,
  no-duplicate port enumeration, empty candidate range, and single-draw panic.
- Pool tests at `network/connection_pool.rs:1647–2650`: USE error/manual timeout
  removal, actual unanswered-USE deadlines, failed setup and refill eligibility,
  preserved ready state, 400-connection smoke cases, 299/301-second suspension
  guard, metric-gated reshard/excess/full-pool cleanup, synthetic targeted results
  across resharding, and host-ID logging before/after keyspace initialization.
- Connection tests at `network/connection.rs:3205–3786`: valid/malformed/missing
  host ID, unanswered keepalive closure, observable socket settings, 1025 stuck
  orphans, short spikes, exactly 1024 old orphans, and sustained orphan traffic.
- All named cases in `tests/integration/session/use_keyspace.rs`,
  `write_coalescing.rs`, `status_change_hints.rs`, and `host_id_mismatch.rs`;
  `new_session.rs:124–133`; `tests/integration/ccm/using_timeout.rs:55–171`; and
  `tests/integration/statements/coordinator.rs:282–410`.

The specification correctly limits evidence: the suspension test does not test
exactly 300 seconds or actual refill; targeted-result helpers do not rewrite real
source ports; `many_connections` does not force collisions or assert shard
assignment; resharding metrics start with no excess collection; coalescing checks
results rather than performance; the host-ID and coordinator tests have the
documented server-dependent skips. The socket test's application-query responses
are forged errors, as stated.

Existing `missing:` cases remain explicit, including allocation/cancellation
races, queued cancellation before stream allocation, exact timing boundaries,
stalled setup/refill, keepalive stream exhaustion and raw-response validation,
selection distributions, keyspace no-op aggregation and retained-snapshot races,
and Go conformance/allocation work. No test was executed; no database or proxy
run is claimed. `git diff --check` reported no errors; because these files are
currently untracked, both were also checked directly for trailing whitespace,
missing final newlines, and conflict markers, with no errors.

## Remaining uncertainty and cross-section check

No additional contradiction was found in this section's behavior, invariants, or
named test claims. The targeted-excess connection metric overcount is already
documented in `configuration-security-observability.md`; the relevant pool path
was independently verified and does not contradict this section's narrower
metric assertions. No other section was edited. This is a source-level audit,
not runtime validation of all documented edge cases.
