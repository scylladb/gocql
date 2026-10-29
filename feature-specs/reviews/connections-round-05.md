# Connections review — round 05

- Section: `feature-specs/connections.md`
- Round: 05 — test expectations, assertions, guards, and coverage claims
- Agent identity: `/root/review_connections_05`
- Source checkout: `/extra/scylladb/gocql-rust-reference-f8b0ee9`
- Source commit verified with `git rev-parse HEAD`: `f8b0ee928dc0dcb475247dc82cce4e42804b0621`
- Before SHA-256: `1e7fcc0f4ecb9e0e1ab7d26ebc1cd0e8edf9f8e606e0db0fd4fba97e05007062`
- After SHA-256: `6f1f2944af0ba7dfdfd973c7cfb85f7b850f3f20e41c732a7e52d3e6a3826a11`

Read the review instructions, README, and complete specification. Independently
traced the implementation and read the cited test bodies and their helpers;
prior review reports were not used as proof. Only the assigned specification and
this report were edited. No Go or Rust implementation changed.

## Source inspected

All source paths below are relative to the pinned checkout.

| Source | Symbols and ranges inspected |
| --- | --- |
| `scylla/src/network/connection_pool.rs` | Pool configuration/state/creation (38–264); selection, initialization, hints, and retained snapshots (266–502); refiller scheduling, setup, shard mismatch, endpoint capture, resharding (670–1211); publication, connectivity, removal, keyspace switching and excess limits (1213–1533); weak error monitors (1535–1550); all cited tests and helpers (1596–2652). |
| `scylla/src/network/connection.rs` | Router submission and orphan notifier (70–226); test defaults and connection construction (434–618); OPTIONS/STARTUP parsing (620–704), authentication (823–927), USE and REGISTER (1370–1458); TLS/router/reader/writer/orphaner/keepaliver/event handling (1527–1983); translation, opening, negotiation, candidate retries, socket options (2077–2426); orphan/stream maps and keyspace validation (2428–2646); all cited host-ID, keepalive, socket, and orphan tests and helpers (3202–3785). |
| `scylla/src/routing/sharding.rs` | Range validation, `ShardInfo`, source-port draw/iteration and SUPPORTED parser (11–307); constructor, shard-ID, enumeration, empty-candidate, and draw-panic tests (323–507). |
| `scylla/src/policies/reconnect.rs` | Public feature gate, per-pool sessions, exponential and constant policy validation/callbacks/jitter (1–246). |
| `scylla/src/client/session.rs` | Defaults/validation (469–671), production connection configuration (1457–1489), `use_keyspace`/`get_keyspace` (2325–2339, 2395–2408). |
| `scylla/src/cluster/node.rs` | Enabled/disabled construction, address inheritance, pool delegation and no-op keyspace behavior (114–290). |
| `scylla/src/cluster/worker.rs` | Queue capacity (81–82), caller enqueue/response wait (220–237), serialized fanout (366–411), broken-connection classification and aggregation (804–847). |
| `scylla/src/errors.rs` | Top-level retryable source-address I/O classifications (611–627). |
| `scylla/tests/integration/session/use_keyspace.rs` | Entire file: actual table contents, validation, failure/recovery, case sensitivity, raw USE, accessor, and concurrent-call ordering assertions (23–409). |
| `scylla/tests/integration/session/host_id_mismatch.rs` | Entire test, translator setup, SUPPORTED guard, connected-pool wait, query result and diagnostic assertions. |
| `scylla/tests/integration/session/write_coalescing.rs` | Entire test/helper: three coalescing configurations, concurrent writes, exact returned rows. |
| `scylla/tests/integration/session/status_change_hints.rs` | Entire test/helper: completed one-connection pools, control exclusion, two DOWN hints per node, targeted feedback and surplus checks. |
| `scylla/tests/integration/statements/coordinator.rs` | `test_coordinator_shard_fallback` (282–410), SUPPORTED rewriting, shard-count guard, unpaged and paged coordinator assertions. |

## Verified findings and fixes

1. **The keyspace coverage map omitted real validation, selection, and recovery
   assertions, and bundled distinct recovery gaps.** `test_use_keyspace`
   (`session/use_keyspace.rs:23–131`) checks unqualified reads after session and
   builder selection, rejects empty/49-character/semicolon names, checks failure
   for a nonexistent keyspace and the following health query, then switches to
   `system`, waits for connectivity, and checks that another health query works.
   `test_use_keyspace_case_sensitivity` (134–213) selects distinct data from
   uppercase/lowercase keyspaces. Added both tests with these exact expectations.
   A later switch to `system` is not proof of recovery into an unchanged desired
   keyspace: retained that as an explicit missing test, separately from no-op
   aggregation. Added an explicit gap for exact name/response-validation
   boundaries absent from these assertions.

2. **Shard-awareness and many-connections evidence needed narrower scope.**
   `advanced_shard_awareness_is_blocked_temporarily` (`connection_pool.rs:2166–2186`)
   checks the eligibility guard at 299 and 301 seconds, without running a refill
   or checking exactly 300 seconds. `shard_aware_attempt_result` (2391–2405) calls
   ordinary `open_connection` and attaches synthetic requested-shard metadata;
   the two mismatch tests (2419–2493) check `can_use_shard_aware_port`, including
   two stale-sharder results, without rewriting a source port. Clarified those
   limits and added missing actual source-port-rewrite/resumption coverage.
   `many_connections`/its helper (2066–2134) jointly await 400 socket connections
   with and without explicit reuse-address; they neither force a collision nor
   verify the assigned shard. Replaced its generic coverage description with
   these concrete conditions and limitations.

3. **Host-ID test descriptions left verified fallback and diagnostic behavior
   implicit.** `host_id_is_read_from_supported` (`connection.rs:3206–3273`) checks
   a valid UUID, malformed UUID, and absent key, with establishment succeeding in
   all three cases. `unexpected_host_id_is_reported`
   (`connection_pool.rs:2496–2652`) checks silence for matching/missing IDs, both
   IDs in exactly one mismatch log, and no diagnostic before keyspace setup with
   exactly one afterward. Expanded the test entries accordingly. This does not
   turn host-ID diagnosis into rejection or identity enforcement.

No additional production-behavior mismatch was found in the complete section.
The edits correct the evidence map, not the observed Rust behavior.

## Explicit behavior checks

- Confirmed session defaults, low-level test-default differences, nonzero pool
  counts, zero-interval validation, and optional keepalive timeout/disable rules.
- Confirmed translation exceptions, socket-option ordering/casts, TCP-only
  connect-timeout scope, TLS before router startup, OPTIONS/STARTUP/authentication/
  REGISTER ordering, sharding validation, matching-mode shard-port parsing,
  unsupported compression fallback, and diagnostic-only peer identity checking.
- Confirmed Initializing/Ready/Broken publication, early initialization success,
  disabled-node behavior, uniform connection choice within buckets, randomized
  fallback, invalid shard handling, and retained snapshot/reference lifetimes.
- Confirmed immediate first fill, empty-pool single attempts, per-host/per-shard
  target calculations, pending-batch blocking, error flags, callback timing,
  jitter/clamping, refill hints, excess cleanup, resharding, and acceptance of
  late endpoint/discovery results.
- Confirmed candidate enumeration and retryable top-level I/O kinds, ordinary
  fallback after targeted opening failures, separate keyspace-setup failure
  handling, 300-second eligibility block and non-extension during a block.
- Confirmed bounded submission queue, request identity wrap, 32768 lowest-free
  stream IDs, exhaustion behavior, buffering/coalescing, negative/event stream
  dispatch, reader backpressure, and fatal-router error propagation.
- Confirmed cancellation before/after allocation, mapping removal on response,
  orphan-age/count thresholds, retained-handle lifetime, keepalive timeout scope,
  raw-response-only acceptance, and DOWN hints.
- Confirmed keyspace queues, serialized cluster/pool switches, current-keyspace
  recheck for new connections, shared switch deadline, removal and retained
  snapshots on failure, accessor storage before validation, and no-op-success
  aggregation. Confirmed snapshot publication precedes initialization and
  connectivity events while the separate pool-empty signal precedes publication.
- Checked the pinned implementation links against the inspected local files;
  language-independent observations remain separate from Rust data structures
  and proposed Go ownership/allocation requirements.

## Test assertions and guards checked

| Tests | Verified coverage and limit |
| --- | --- |
| Source-port/shard-info tests | Modulo correctness, no duplicate ports, expected count for default/custom four-shard ranges, empty single-port candidate range; constructor accepts default and rejects reversed/zero-start ranges; shard IDs 4/5 rejected and 3 accepted for four shards. No distribution proof or complete boundary matrix. |
| Keyspace setup/switch source tests | Injected setup timeout sets error/refill state and Broken publication; failed setup preserves the old Ready/unsharded pool, successful setup permits resharding. Switch-error test directly invokes timeout handling; the separate unanswered-USE test exercises actual 100 ms deadlines for initialization and switching. Metrics assertions inside these tests require `metrics`. |
| Reshard/excess metric tests | All three tests are gated by `metrics`. Resharding begins without excess connections; limit cleanup checks zero excess/one active/one counted connection; full-pool worker cleanup waits for the metric to reach one. They do not prove resharding cleanup of a nonempty excess list. |
| Keyspace live tests | Added assertions above; accessor tests cover only initial/success/configured cases. Concurrent calls assert completion order after observing the first fanout and delaying its responses, not every connection's final keyspace or caller cancellation. |
| Host-ID tests | Dry-mode valid/malformed/missing parsing and diagnostic assertions above. Live translation-mismatch assertions return early when the server omits `SCYLLA_HOST_ID`; after the guard they check both mismatching pools, no matching-pool diagnostic, and a real query to the reached node. |
| Keepalive/socket tests | Paused-clock keepalive test uses an in-memory transport, first permits successful probes/queries, then drops OPTIONS and asserts `KeepaliveTimeout` plus subsequent query failure. Socket test uses real sockets; buffer sizes are lower-bounded, linger equals 42 seconds, reuse-address is true; application requests receive forged errors. |
| Five orphan tests | Paused clock and simulated responses: 1025 stuck requests cause the specific orphan error after the age threshold; 1025 brief orphans and 1024 old orphans allow a later query; sustained 0.9-threshold traffic survives and 1.1-threshold traffic closes. They do not cover all stream IDs or response/cancellation interleavings. |
| Coalescing | Scheduler-yield, 1 ms, and disabled modes assert exact persisted/read-back rows with one data connection. No latency, flush-count, or syscall claim. |
| DOWN hint | Three-node proxy, one data connection per node, long periodic interval, control-connection exclusion, two hints per target and no surplus observed feedback. Covers targeted OPTIONS receipt, not failed-probe eviction or every keepalive timing race. |
| Coordinator fallback | Rewrites advertised shard to zero and requests the last shard; unpaged and paged results report zero. Returns early if the node has no usable sharder with at least two shards. Does not establish the server's actual physical execution shard. |

## Remaining gaps and validation

Missing tests remain explicit for unchanged-desired-keyspace recovery, no-op
aggregation, keyspace validation/response boundaries, cancellation and snapshot
races, stream exhaustion/full-range allocation, notification order, source-port
retry scope, real source-port rewriting/resumption, stalled setup/refill,
negotiation boundaries, and keepalive disable/raw-response handling. The existing
missing markers for repeated mismatch deadlines and nonempty excess resharding
remain justified. No Go conformance or allocation result is inferred from Rust.

Searched the pinned `scylla/src` and `scylla/tests` for the relevant keyspace
validation, aggregation, and suspension symbols to check these gaps. Checked all
concrete test references in the final specification against function definitions
and all pinned source links against local file/line existence. Read-only source
inspection and documentation-reference validation were performed; **no Rust or
Go tests were executed**, and no live-database result is claimed.
