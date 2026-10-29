# Routing reference review — round 03

- Section: `feature-specs/routing.md`
- Round: `03` — errors, partial success, failover, retry, and recovery.
- Agent identity: `/root/review_routing_03`
- Source checkout: `/extra/scylladb/gocql-rust-reference-f8b0ee9`
- Source commit, verified with `git rev-parse HEAD`: `f8b0ee928dc0dcb475247dc82cce4e42804b0621`
- Before SHA-256: `da5b1d02b55b730f693c422ba7255e4de1689ac5cf0123e76b6e33f78ac38e14`
- After SHA-256: `d9e9948e3c1cfa3f843f76f82093f743ac9703e1b21db81a5457dcffc3277ff1`

Read the complete review instructions, README, and assigned specification. The
round 02 report was context only; source evidence below was inspected independently.
Changes are limited to this report and the routing specification. No child agents
were created and no Go or Rust implementation was changed.

## Verified findings and fixes

1. **Automatic paging invalidated the universal unprepared-routing claim.**
   The prior specification said an ordinary unprepared request always plans
   without token/table metadata and prepares only on its chosen connection.
   `Session::query` does exactly that for unpaged/manual requests
   (`scylla/src/client/session.rs:1681–1748`), but `Session::do_query_iter`
   session-prepares nonempty bind values before invoking the prepared pager
   (`:1841–1855`). That pager extracts the token and builds prepared routing
   information (`scylla/src/client/pager.rs:974–1000`). Session preparation selects
   the partitioner from table metadata (`session.rs:2018–2038`). Consequently,
   automatic paging can use token-aware routing and the CDC partitioner even
   when its public input was an unprepared statement. Empty-value automatic
   calls retain QUERY and empty routing metadata (`pager.rs:827–867`). Scoped
   every affected statement, including the CDC-hint limitation and feedback
   paragraph, and added the automatic-paging dispatch/pager evidence links.
   Flagged this distinction to the parent for cross-section consistency.

2. **Routing-data errors and absent routing metadata have different outcomes.**
   `PreparedStatement::extract_partition_key_and_calculate_token` returns
   `Ok(None)` for a statement without partition-key indexes, but propagates
   extraction/hash errors (`statement/prepared.rs:373–386`). Explicit prepared
   execution propagates the error before constructing routing information
   (`client/session.rs:2095–2098`); the prepared pager rejects it before its first
   fetch (`client/pager.rs:974–981`), and a batch's first prepared value row also
   propagates it (`statement/batch.rs:308–329`). In contrast, unpaged/manual
   unprepared requests deliberately call `.unwrap_or(None)` for the optional
   tablet hint after local preparation and successful value serialization
   (`client/session.rs:1718–1733`). The absent token then yields zero from
   `choose_tablet_block_hint` (`client/execution.rs:73–86`), and EXECUTE proceeds.
   Added this exception and kept serialization failures distinct. Added an
   explicit regression gap for the differing entry-point behavior.

3. **Target acquisition failure was missing from the custom-policy/retry
   boundary.** `run_request_speculative_fiber` records a connection-selection
   error and advances to the next target before attempt timing, callbacks or
   retry-policy consultation (`client/execution.rs:560–580`). Once an attempt
   returns an error, the failure callback runs before the retry decision
   (`:594–639`), including when that decision later ignores a write error
   (`:661–665`). Same-target retries reacquire a connection under the same
   node/shard target; next-target retries consume the existing iterator
   (`:649–658`). The fiber returns its last recorded error after nonempty
   exhaustion (`:671`); an empty non-speculative plan becomes `EmptyPlan`
   (`:497–509`). Added these routing boundaries, qualified last-error behavior
   per execution fiber, and added an implementation link and callback-test gap.
   Full speculative-result arbitration remains owned by execution.

4. **Tablet recovery needed explicit parse/resolve and error-response
   boundaries.** `RawTablet::build` collects replica tuples into a
   `Result<Vec<_>, _>` (`routing/locator/tablets.rs:285–318`): any malformed tuple
   or negative shard discards the whole payload, while an empty list succeeds.
   This differs from unknown valid host IDs, where `TabletReplicas::from_raw_replicas`
   retains resolved entries and returns unresolved IDs (`:349–375`), and the
   original full list survives for later repair (`:515–539`). Empty accepted
   feedback can replace an overlapping usable mapping (`:780–799`) and leaves
   no replica or leader; normal non-replica routing remains available.
   Separately, `Connection::execute_raw_with_consistency` processes feedback
   immediately after each successful response decode, before matching database
   errors (`network/connection.rs:1158–1188,1193–1221`). Thus even an initial
   UNPREPARED or another database error can supply useful feedback. Transport
   or response-decoding errors returned by `send_request` bypass this step.
   Added these distinctions and regression gaps. The existing valid-range
   branch of `test_raw_tablet_deser_wrong_token_range` already accepts an empty
   replica list (`tablets.rs:1133–1176`), so updated that coverage description
   without claiming it tests replacement of a populated cache.

## Files, symbols, and whole-section checks

Paths below are relative to the pinned Rust checkout. Ranges identify the
implementation portions actually inspected; test inspection is described below.

| Source inspected | Behaviors checked |
| --- | --- |
| `policies/load_balancing/mod.rs` and `plan.rs` under `scylla/src` | Complete interfaces/state machine: initial consistency, regular SERIAL/LOCAL_SERIAL classification, concurrent custom policies, fallback after `pick(None)`, raw optional-shard equality and random shard fill. |
| `scylla/src/policies/load_balancing/default.rs:145–1274` | `pick`, `fallback`, replica helpers, builder and processed inputs: local/rack groups, remote replicas before local non-replicas, configured DC failover, connected versus enabled stages, host/shard deduplication, fixed replica seed, random rotations, global LWT deferral and leader eligibility. |
| `scylla/src/policies/load_balancing/default.rs:3270–3878` | `TimestampedAverage`, latency predicates/wrapping, reliable-error classification, updater and builder: zero measurements, strict threshold/count/freshness, persistent stale minima, penalty expiry and stable reordering. |
| `scylla/src/policies/load_balancing/single_target.rs`, `policies/host_filter.rs` | Complete files: direct node/ID/address targets, empty fallback, address-translation limitation; allowlist/DC/all-peer filters and untranslatable-peer acceptance. |
| `scylla/src/routing/mod.rs`, `partitioner.rs:24–545`, `sharding.rs:1–311` | Token normalization/string exception; partitioner suffix/defaults; signed Murmur3 tail and incremental hashing; CDC first-eight-byte/short-input behavior; composite framing; shard math, advertised limits and source-port exhaustion. |
| `scylla/src/statement/prepared.rs:307–455,781–877`; `statement/batch.rs:308–341` | Prepared routing eligibility, table identity, ordered key extraction, null/unset filtering, composite length failure, first-batch-row token/error extraction. |
| `scylla/src/routing/locator/token_ring.rs`, `replication_info.rs:62–288`, `precomputed_replicas.rs:78–215`, `replicas.rs` | Exact/next token lookup, wrapping, empty rings, unique-node placement, NTS rack-repeat budget, zero/capped factors, precomputed-prefix boundaries and borrowed/owned replica storage. |
| `scylla/src/routing/locator/mod.rs:114–326,367–554,782–1014` | Tablet override/cold ranges, strategy fallback, public replica-set selection, computed vnode shards, DC masks, global ordered-NTS continuation and configured-zero-factor primary quirk. |
| `scylla/src/routing/locator/tablets.rs:179–1050` | V1/V2 parse preference and version nibbles; range/shard validation, partial host resolution, original-leader preservation, mask construction, full-overlap replacement, equality, maintenance and structural sharing. |
| `scylla/src/cluster/state.rs:569–585,618–737,837–975`; `cluster/worker.rs:302–340` | Tablet-keyspace exclusion from vnode precomputation; token/endpoints inspection and optional preserialized APIs; same-batch feedback comparison, lazy snapshot copy, unknown host handling and feedback publication. |
| `scylla/src/client/execution.rs:1–672` | Complete execution core: hints, shared speculative-plan iterator, callback targets, connection selection, request timeout and retry/exhaustion boundaries. |
| `scylla/src/client/session.rs:1314–1378,1646–1755,1841–1855,1952–2038,2050–2178`; `client/pager.rs:827–1015,1118–1157` | Batch/unprepared/prepared input construction, automatic-paging preparation, first-page key errors, per-request hint lifetime and session partitioner lookup. |
| `scylla/src/network/connection.rs:912–992,1110–1223,1252–1368,2045–2069`; `network/connection_pool.rs:336–428`; `cluster/node.rs:210–240` | QUERY/BATCH exclusion, local batch preparation, EXECUTE/reprepare feedback ordering, awaited channel send, advisory-shard fallback, actual connected/enabled predicates. Channel capacity/construction also checked at `client/session.rs:73,1442`. |
| `scylla-cql-core/src/frame/protocol_features.rs:62–200` | SUPPORTED/STARTUP feature negotiation, V2 preference and negotiated prepared LWT mark. |

No additional changes were needed for the existing location/failover rules:
local consistency does not prohibit enabled DC failover; a nonexistent preferred
DC does not cause a guessed replacement; disabled nodes stay excluded; an
eligible remote replica can precede a local non-replica. Final default fallback
nodes retain ring order while fast choices rotate. LWT determinism applies to
replicas, with latency reordering still allowed. Leader routing excludes ONE and
LOCAL_ONE, preserves original-leader identity, and has no explicit V2-only guard.

Also checked the established recovery invariants: `pick(None)` is not an empty
plan; shard hints can fall back within a node; unknown tablet ranges do not use
vnode replicas; overlap replacement drops whole old ranges; equivalent resolved
feedback is a no-op; versions do not reject older arrivals; unresolved mappings
are repaired or discarded during maintenance; snapshot copies preserve prior
table storage. Missing keyspace metadata prevents default replica selection even
when feedback has already populated the tablet cache. Endpoint inspection and
optional interop APIs keep their documented metadata/validation differences.

## Test coverage and validation

No Rust, Go, proxy, or database test suite was executed. This round is source
inspection. A read-only Python check verified all **42 pinned source anchors**
and **51 cited test declarations/modules**, with no missing targets or trailing
whitespace in the routing specification.

Inspected policy test assertions for token-aware/unaware groups, regular serial
and confirmed LWT inputs, leader consistency/down-node handling, inherited versus
explicit preferences, insufficient latency measurements, expiry, and positive
penalties (`default.rs:1599–1708,2155–2228,2780–2845,3055–3268,4008–4070,4128–4275`).
Read the Plan fallback regression and ring/hash tests. Checked tablet-replica
ordering and cold-range assertions (`locator/test.rs:799–903`), masks crossing
64 bits (`locator/mod.rs:1030–1070`), shard-range assertions (`sharding.rs:321–413`),
empty valid payload acceptance, unresolved-replica repair/removal cases
(`tablets.rs:1917–1983`), and snapshot deduplication/same-batch replacement
(`cluster/state.rs:1381–1464`). The test declaration check alone is not treated as
proof of every edge-case assertion.

Read the token-awareness, SimpleStrategy, shard-targeting and latency integration
tests. Token-awareness asserts one traced node for explicit prepared unpaged and
automatic execution; it does not test the implicit preparation of `query_iter`.
The latency integration test expects a query error, so it establishes only that
the real request path completes. LWT assertions cover a conditional statement
later in a prepared batch (`lwt_optimisation.rs:133–245`). Tablet base/view/CDC
helpers assert learned feedback convergence; CDC explicitly session-prepares and
checks `PartitionerName::CDC` (`tablets.rs:911–975`). V2 tests inspect cold/warm
block presence, mixed per-connection feature types, V1 fallback and observed
leader targeting. Their warm-ups and migration handling are not deterministic
refresh guarantees. The token-unaware pager test (`session/pager.rs:872–984`)
checks successful missing-token cases, not actual extraction errors or the new
implicit-preparation distinction.

Searching integration tests for load-balancing callbacks found only empty
implementations in `statements/execution_profiles.rs:68–84`; those do not cover
connection-selection failure or ignored-write callback behavior. New explicit
gaps are listed in the specification for these callbacks, token-error entry-point
differences, failed-EXECUTE feedback, malformed replica tails and empty-cache
replacement. Prior gaps, including ordered-NTS zero-factor behavior, V1 leader
interpretation and unpaged/manual CDC hint selection, remain. Go coverage and
allocation goals remain pending. Cross-section findings were sent to the parent;
no other section was edited.
