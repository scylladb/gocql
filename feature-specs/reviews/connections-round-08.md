# Connections review round 08

- Section: `feature-specs/connections.md`
- Round: 08 — cross-section consistency and caller/callee boundaries
- Agent identity: `/root/review_connections_08`
- Reference checkout: `/extra/scylladb/gocql-rust-reference-f8b0ee9`
- Source commit, checked with `git rev-parse HEAD`: `f8b0ee928dc0dcb475247dc82cce4e42804b0621`
- Before SHA-256: `42dafe64ecf8151af2d3f06384c4f50484f7cc4c1acb13fb4e9a6614619eca98`
- After SHA-256: `62828997d3d670f8d2e29d0245136711ffe35c99c34fe08def59725bc0e5b5d7`
- Scope: independently reviewed the complete section and edited only that section and this report. No Go or Rust implementation changed; no child agents used.

## Sources inspected

Read `feature-specs/README.md`, the complete review instructions, and the complete connections section. Compared relevant contracts in `routing.md` (routing lifetime, custom policy, shard hints), `execution.md` (attempt acquisition, deadlines, retry, coordinator and USE side effects), `metadata-control-events.md` (control establishment, status hints, topology publication, host callbacks), and `configuration-security-observability.md` (startup/defaults, transport, translation, client routes, metrics).

Pinned Rust implementation inspected:

| File | Symbols and lines |
| --- | --- |
| `scylla/src/network/connection_pool.rs` | `PoolSize`, configuration/session creation and state types, 39–264; observations, selection, hints, keyspace queue and initialization, 266–502; `PoolRefiller` state, scheduling and fill strategy, 514–912; setup handling, host-ID verification, captured opening state and resharding, 914–1211; publication, event emission, removal and keyspace switching, 1213–1533; weak error monitor, 1536–1550. |
| `scylla/src/network/connection.rs` | `RouterHandle` and cancellation notifier, 70–226; connection configuration and construction, 340–618; OPTIONS/STARTUP and authentication, 620–715 and 823–927; USE/REGISTER, response parsing, TLS/router startup, reader/writer/orphaner/keepaliver and event decoding, 1371–1983; node detection and hint delivery, 1985–2043; translation, negotiation, source-port loop and socket options, 2077–2426; orphan maps, stream allocator and keyspace validator, 2428–2646. |
| `scylla/src/cluster/node.rs` | Enabled/disabled construction, address inheritance, observations, pool delegation and disabled error, 114–290. |
| `scylla/src/cluster/state.rs` | `calculate_new_topology`, especially reuse guards and fresh-pool branches, 419–479; address/filter regression, 1143–1185; disabled-object reuse regression, 1188–1215. |
| `scylla/src/cluster/worker.rs` | Queue/internal event-channel creation, 81–89; keyspace submission, 220–235; event and keyspace branches, 354–411; DOWN/UP/client-route hint dispatch, 418–442; `handle_connectivity_change_event`, 760–801; USE error classification/aggregation, 804–846. |
| `scylla/src/policies/load_balancing/plan.rs` | `with_random_shard_if_unknown` and iterator use, 94–167. |
| `scylla/src/client/execution.rs` | `AttemptTarget`/`NodeAttemptTarget`, 191–270; connection acquisition, callbacks and retry decisions, 547–672. |
| `scylla/src/client/session.rs` | Defaults, socket mapping and validation, 469–671; reconnect-policy selection, 1407–1424; raw USE handling, 1775–1789; keyspace reporting/propagation and accessor, 2325–2339 and 2395–2408. |
| `scylla/src/client/pager.rs` | First-page SET_KEYSPACE propagation, 1192–1221. |
| `scylla/src/cluster/control_connection.rs` | Single control-connection ownership, feature detection and metadata timeout selection/cache-miss preparation, 35–192. |
| `scylla/src/cluster/metadata/cc_establisher.rs` | Configuration reporter attachment and separation from data pools, 99–135. |
| `scylla/src/routing/sharding.rs` | Range validation, `ShardInfo::new`, source-port helpers and discovery parser, 11–42, 85–103, 126–237 and 274–307. |
| `scylla/src/policies/reconnect.rs` | Per-pool policy sessions, exponential and constant delay implementations and validation, 1–246. |
| `scylla/src/network/tls.rs` | Global TLS provider/config construction and per-connection backend selection, 1–155. |
| `scylla/src/errors.rs` | Top-level retryable source-address I/O classification, 611–627. |

## Verified findings and fixes

1. **Pool-preservation wording was broader than the topology caller permits.** The section said endpoint changes for an existing node preserve its pool. `ClusterState::calculate_new_topology` only takes the reuse/address-inheritance branch when the same host ID is enabled in both states and datacenter/rack are equal (`state.rs:439–478`). A location change for an enabled replacement creates a new pool; filtering can instead produce a disabled replacement. `Node::inherit_with_ip_changed` updates the preserved pool endpoint (`node.rs:175–188`). Narrowed the statement to these conditions and linked the metadata snapshot lifetime contract. Added the existing address/filter regression to test places and explicitly marked missing dedicated pool-lifetime coverage.

2. **The routing-plan boundary was implicit in the description of random selection.** The pool's `random_connection` chooses from one current snapshot (`connection_pool.rs:362–375`), but ordinary execution does not pass an absent shard into that helper. `Plan::with_random_shard_if_unknown` resolves absent policy hints using a separate live sharder observation (`plan.rs:94–107`), and `NodeAttemptTarget` later calls `connection_for_shard` (`execution.rs:246–254`). Clarified that these observations can use different snapshots. Also made the error handoff explicit: connection acquisition failure advances the plan without retry policy involvement (`execution.rs:564–575`), while stream exhaustion returned by an attempt reaches the policy (`execution.rs:594–639`). Added pinned evidence for these boundaries. This aligns the section with the existing routing and execution contracts without changing the pool's documented selection algorithm.

3. **“With a configured listener” conflated internal connectivity events with the optional public host listener.** `Node::new` always supplies a connectivity sender (`node.rs:128–136`), backed by the unconditional channel in `Cluster::new` (`worker.rs:81–89`). `emit_events` tests that internal sender (`connection_pool.rs:1246–1301`); it does not test the public listener. The cluster worker updates recorded status before checking whether a public listener exists (`worker.rs:760–800`). Replaced the ambiguous condition, documented the internal/public boundary, added evidence and a cross-link, and kept dedicated no-listener event coverage explicitly missing.

## Whole-section checks with no further correction

- Session defaults agree with effective pool/connection configuration: per-shard target one, advanced awareness allowed, five-second TCP timeout, socket defaults, thirty-second CQL interval/timeout, coalescing enabled, and exponential reconnect limits/jitter. Zero keepalive interval validation and absent-versus-zero timeout semantics match the source.
- Translation bypass rules, IPv4/IPv6 bind choice, buffer-size casts, TLS placement, OPTIONS discovery, malformed metadata fallback, matching-mode first shard-port value, independent compression negotiation, authentication callbacks and REGISTER behavior match. The TCP deadline does not cover translation, TLS or the whole CQL handshake; new-connection USE has its separate deadline.
- Ready/Initializing/Broken selection errors, early initialization completion, disabled pools, first-connection feature detection, uniform shard/slice choice, invalid shard conversion and same-node fallback match. Selection reserves no stream capacity and retained snapshots can outlive publication.
- Fill batching, the single attempt when empty, per-host/per-shard targets, ordinary versus targeted failure handling, stalled batches, hints, backoff callback timing, excess bounds, weak error monitoring, resharding, advertised-port replacement and late opening results match.
- Source-port candidates visit compatible ports once, use a new complete opening attempt and TCP timeout per candidate, and retry only the three top-level I/O classes. Same-sharder mismatch suspends advanced awareness for 300 seconds without extending an active block; changed-sharder results do not trigger that classification.
- Submission capacity 1024, prequeue serialization, wrapping request identities, all 32768 streams, lowest-free allocation, stream-exhaustion handling, 8192-byte buffers and empty-queue coalescing behavior match. Ordinary parsing happens after dispatch; negative/event streams and reader backpressure follow the documented paths.
- Orphan notification mapping, reservation until response, pre-allocation cancellation gap, age/count thresholds, fatal-router fanout and retained-owner shutdown boundaries match. Keepalive checks raw frame receipt, includes queue time in its timeout, shares stream capacity, supports coalesced hints and does not run when disabled.
- Explicit/raw/pager USE entry points, prevalidation reported-name update, cluster/pool queue capacities, serialized fanouts, snapshot retention, cancellation after enqueue, nonbroken failure removal, shared switch deadline, replacement setup, nonrollback desired state and disabled/empty-pool no-op aggregation match the caller/callee chain.
- Publication precedes initialization and connectivity notifications; the separate pool-empty hint precedes publication and is discarded by ordinary data-node construction. Control-connection ownership is separate from node pools. The configuration section's active-connection metric limitations remain consistent with the inspected admission/removal paths.

## Test claims and validation

Read the named test assertions and supporting setup/helpers in `connection_pool.rs`, `connection.rs`, and `routing/sharding.rs`, including setup/switch failures, actual USE deadlines, source-port smoke coverage, temporal suspension, synthetic shard mismatches, metrics cleanup, host-ID parsing/logging, socket options, keepalive failure, and orphan threshold cases. Confirmed that the synthetic mismatch helper opens an ordinary-port connection before attaching requested-shard metadata (`connection_pool.rs:2388–2404`), and that the 400-connection helper does not assert assigned shards (`2066–2099`). The paused-clock suspension test checks 299/301 seconds, not the exact deadline.

Read `scylla/tests/integration/session/use_keyspace.rs:23–404` for validation, quoted/unquoted behavior, raw USE/pager propagation, accessor and serialized-call assertions. Checked assertions/skip paths in `session/host_id_mismatch.rs`, `session/new_session.rs`, `ccm/using_timeout.rs`, `session/status_change_hints.rs`, and `statements/coordinator.rs`. Read `session/write_coalescing.rs`, including its concurrent write/readback helper: its contract remains result-preservation smoke coverage, not a latency/syscall claim. Read the newly referenced `cluster/state.rs:node_filtered_out_after_ip_change` regression; it asserts replacement and disabled state, not eventual shutdown of old retained pools.

A read-only reference check found all **39 named Rust test references** and validated all **57 Markdown links**, including pinned revision/path/line bounds and local file anchors. It reported no errors. Rust/Go unit or integration tests were **not executed** in this documentation review. No new runtime coverage is claimed. Existing `missing:` cases remain gaps; dedicated pool-lifetime and no-public-listener connectivity assertions are also explicitly missing.

## Cross-section issue reported to parent

`metadata-control-events.md` described UP as requesting an “immediate pool refill.” The actual hint clears the current timer and resets policy state, then schedules `get_delay()` only when no batch is pending (`connection_pool.rs:753–790`, `812–814`). With the built-in policy that is normally the initial 50 ms delay, not a guaranteed zero-delay opening. Sent the parent the evidence and recommendation to describe an immediate **hint/reset** and refer to this section's scheduling boundary. Did not edit the other section.
