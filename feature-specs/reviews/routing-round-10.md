# Routing review — round 10

- Section: `feature-specs/routing.md`
- Round: 10, final comprehensive fidelity and completeness audit
- Agent identity: `/root/review_routing_10`
- Reference checkout: `/extra/scylladb/gocql-rust-reference-f8b0ee9`
- Source commit, independently checked with `git rev-parse HEAD`:
  `f8b0ee928dc0dcb475247dc82cce4e42804b0621`
- Before SHA-256:
  `13c2b3f38c408722833359928d9b78753bae1f2b3e409c953ffaadd07c8938ab`
- After SHA-256:
  `9cd27ab9bd963c8090a4562b0a5bcced129a69afe5626b724d62d9d4bdaffaad`

Read `reviews/REVIEW-INSTRUCTIONS.md`, `README.md`, the complete routing spec,
and the routing assignments in `rust-source-catalog.json`. This was an independent
source audit; earlier reports were not used as proof. Changes are confined to the
routing spec and this report. No implementation files were edited and no child
agents were used.

## Findings and fixes

1. **Clarified unknown numeric server errors in latency measurement.** The
   previous statement about excluding unknown newer database error variants did
   not distinguish enum evolution from an unrecognized wire error code. The
   response parser maps unrecognized numeric codes to `DbError::Other(code)`
   (`scylla-cql-core/src/frame/response/error.rs:152`), and
   `LatencyAwareness::reliable_latency_measure` explicitly accepts `Other(_)`
   (`scylla/src/policies/load_balancing/default.rs:3556–3614`). Only additional
   enum variants reaching the wildcard arm are excluded. The spec now states
   this distinction explicitly, preserving the accepted classification a port
   must reproduce rather than implying that every unfamiliar server error is
   ignored.

2. **Completed two latency unit-test mappings.** Added
   `latency_aware_default_policy_is_correctly_token_aware`
   (`default.rs:4400–4593`), whose first fixture places eligible non-replica
   nodes before penalized local/remote vnode replicas and whose other fixtures
   exercise a retained cached minimum. Added
   `timestamped_average_works_when_clock_stops` (`default.rs:4602–4618`), which
   uses a paused clock and a nonzero sample to retain the average while increasing
   its count. The added descriptions distinguish full-plan fixtures from tablet
   leader coverage and distinguish zero elapsed time from a zero-duration sample.

No other behavior mismatch was verified. These are a classification clarification
and coverage-inventory additions, not claims of new Rust functionality.

## Production source and checked behavior

Paths below are relative to the pinned Rust checkout. All production modules
assigned to routing in the catalogue were inspected, including the internal
replica-storage helper.

| Source inspected | Independently checked contracts |
| --- | --- |
| `scylla/src/policies/load_balancing/mod.rs:28–220` | Routing fields and defaults; initial regular consistency; confirmed LWT or regular SERIAL/LOCAL_SERIAL classification; optional serial consistency alone is insufficient; Send/Sync custom-policy interface; success/failure callbacks; gated Python constructor stores its supplied fields. |
| `scylla/src/policies/load_balancing/plan.rs:81–168` | Construction is lazy; first advance calls `pick`; no pick still invokes fallback; first raw target is filtered by exact equality; fallback-first targets are filtered on subsequent advances too; unspecified shards use the currently observed sharder count or zero. No general custom-policy deduplication. |
| `scylla/src/policies/load_balancing/default.rs:146–615` | Leader, rack/DC/global replica, connected node, and enabled-node priorities; remote replicas can precede local non-replicas; host-plus-optional-shard deduplication; latency wrapping after deduplication; success/failure reporting. |
| `default.rs:617–1025` | Token/strategy availability and token-awareness switch; nonexistent preferred DC; rack filters; deterministic versus random replicas; global LWT first-replica deferral; fixed-seed replica choice/shuffle; fresh random node rotations; connected-pool liveness ignores shard; global consistency/ONE/LOCAL_ONE leader gate; explicit DC failover does not inspect local consistency. |
| `default.rs:1050–1280` | Builder defaults, session preference inheritance and explicit override/clear, fixed seed chosen once when replica shuffling is disabled, replica strategy lookup requiring table/keyspace metadata. |
| `default.rs:3270–3874` | Per-host shared statistics; zero-duration sample omission; average weighting and failed-duration-conversion fallback; cached microsecond minimum; strict threshold; sample count/retry-period gates; stale averages retained; stable whole-fallback partition; exact failure classification; unchecked threshold/retry/minimum/update-rate parameters, checked nonzero scale, runtime-spawned updater and retained remote handle. |
| `scylla/src/policies/load_balancing/single_target.rs:16–105` | Node object used directly; host/address lookup in snapshot; no fallback; no default-policy filtering; translation makes address matching unstable. |
| `scylla/src/routing/mod.rs:32–97`, `routing/sharding.rs:78–83` | Signed token ordering, `Token::new` normalization, raw string parsing exception, location preference forms. |
| `scylla/src/routing/partitioner.rs:20–423` | Sealed built-in partitioners and dispatch; suffix matching; Murmur3 signed-byte tail and incremental buffer; non-consuming finalization; CDC first eight bytes, short-input sentinel, ignored suffix; serialized-key framing uses original element count and skips null/unset. |
| `scylla/src/routing/sharding.rs:14–307` | Source-port range validation and defaults; unsigned token bias/shift/product formula; nonzero 16-bit shard count versus 32-bit hint; source-port modulo, randomized wrap iteration, no-candidate behavior, requested-shard assertions; missing/empty/invalid advertised fields and unvalidated ignored-bit semantics. |
| `scylla/src/routing/locator/token_ring.rs:15–69` | Sorted ring, exact/next-greater lookup, wrap and single traversal, empty ring, vnode count versus unique nodes. |
| `scylla/src/routing/locator/replicas.rs:10–61` | Borrowed precomputed versus owned fallback replica storage; indexing/iteration and empty replicas. |
| `scylla/src/routing/locator/replication_info.rs:62–209` | Unique-node order from sorted ring; unnamed DC remains global; missing rack forms a distinct rack; SimpleStrategy cap/dedup; NTS DC rings and repeat-rack budget. |
| `scylla/src/routing/locator/precomputed_replicas.rs:63–209` | Global maximum-factor prefix reuse; minimum factor-one precomputation; per-DC reuse through rack count; separate above-rack-count factors; misses fall back to on-demand calculation. |
| `scylla/src/routing/locator/mod.rs:70–1025` | Direct locator API bypasses policy eligibility; tablet override and empty unknown ranges; filtered SimpleStrategy versus DC-local NTS; Local/Other factor-one fallback; global DC grouping; set length/random probe and filtered fallback; live computed sharder; iterator clone/nth exhaustion; ordered global NTS lazy primary plus allocated continuation, including the documented zero-factor membership quirk. |
| `scylla/src/routing/locator/tablets.rs:138–319` | V1/V2 field layouts and V2 precedence; opaque version encoding and randomized nibble probe/cache miss; nonwrapping range validation and inclusive conversion; negative-shard rejection; complete declared-list parsing before insertion. |
| `tablets.rs:349–610`, `tablets.rs:636–1050` | Partial unknown-host resolution with original list retained; original leader identity; DC masks spanning words; equality requires fully resolved ordered replicas/version/range; sorted nonoverlap lookup/replacement; all overlaps removed without preserving fragments; maintenance removal/repair/recreated-node replacement; table/view initialization; structural sharing and unfenced name-based feedback insertion. |

## Callers, callees, and cross-section contracts

The following checks followed the routing values through their actual producers
and consumers rather than relying on API comments:

- `scylla/src/statement/prepared.rs:200–417,791–869`: retained preparation
  metadata/LWT/partitioner versus swappable result metadata; first-column table
  identity; PK-index extraction and key-column order; missing indexed values;
  null/unset filtering; effective single/composite framing and 65,535-byte limit;
  empty materialized key versus absent token when no PK indexes exist.
- `scylla/src/client/session.rs:1294–1366,1681–1755,1841–1863,1976–2038,2085–2160`
  and `client/caching_session.rs:102–244`: batch first-statement routing and any
  prepared-member LWT flag; ordinary unprepared no-token planning; connection-local
  prepare and suppressed optional-hint token errors; automatic paging's session
  prepare; session-only base-table partitioner installation; cached preparation
  before all execution modes, including batch members.
- `scylla/src/client/execution.rs:73–114,445–671` and
  `client/pager.rs:134–175,306–372,974–1093`: frozen routing input/snapshot;
  serialized shared speculative plan advancement; callbacks after completed
  attempts; connection-selection failures skip retry and load-balancing callbacks;
  same-target reacquisition versus next-target advancement; elapsed time begins
  after acquisition; empty-plan/last-error distinction; pager snapshot and hint
  retention, fresh per-page plans and previous successful coordinator preference.
- `scylla/src/network/connection.rs:730–815,1105–1368,2045–2068` and
  `network/connection_pool.rs:266–360`: negotiated LWT marking; reprepare updates
  result metadata rather than routing inputs; feedback before database-error
  interpretation and after reprepare resend; malformed feedback logged; awaited
  send whose receiver failure is ignored; native BATCH lacks feedback handling;
  live pool/sharder observation and advisory shard conversion/fallback boundary.
- `scylla/src/client/session.rs:73,1442`,
  `cluster/worker.rs:303–340`, and `cluster/state.rs:536–580,618–735,846–974`:
  channel capacity 8,192; batched publication and resolved-update suppression;
  tablet keyspaces excluded from vnode precomputation; base-table-only token API;
  supplied-token lookup requires keyspace but does not validate table existence;
  deprecated empty-result collapse; optional preserialized interop bypass.
- `scylla-cql-core/src/frame/protocol_features.rs:65–141` and
  `scylla-cql/src/frame/request/execute.rs:81–119`: V2-only STARTUP preference
  when both extensions are present; per-connection optional trailing byte;
  no requirement to resample a retained request hint on retry.
- `scylla-cql-core/src/deserialize/value.rs:1009–1042,1732–1765,2099–2136`:
  null list accepted empty; missing trailing tuple fields passed as null; V2
  non-null version still required; trailing tuple/list bytes not fully consumed;
  declared malformed entries still propagate errors.
- `scylla/src/policies/host_filter.rs:33–101`: accept-all/DC/resolved allowlist
  filters; untranslatable addresses accepted by the allowlist; filtering remains
  distinct from location preference.

Read related passages in `connections.md`, `execution.md`,
`metadata-control-events.md`, `serialization-protocol.md`, and
`configuration-security-observability.md`. No contradictory cross-section
contract was verified. The routing section preserves the distinctions between
attempt callbacks and successful-attempt metrics, learned tablet placement and
publication timing, prepared versus result metadata, and shard hints versus strict
connection pinning.

## Test-claim audit and validation

This round **read tests; it did not run Rust, Go, database, race, or allocation
tests**. The test inventory remains source evidence, and all pending Go coverage
remains explicitly missing.

- Inspected policy fixture framework (`default.rs:1303–1622`), token-aware and
  token-unaware matrices, confirmed/regular-serial LWT cases and disabled-primary
  regression, location inheritance tests, leader consistency/DC/down fixtures,
  and latency fixtures. Full `Plan` samples check node groups; most fixtures use
  enabled-as-connected status. A lone disconnected leader does not cover ordering
  among several disconnected nodes. The paused-clock and token-aware latency
  tests are now included in the spec.
- Inspected ring vectors; locator strategy/length/filtered-choice/nth helpers;
  the oversized `nth` and tablet set/multiword-mask regressions; ordered replica
  vectors; and on-demand/precomputed strategy vectors. Confirmed the tests use
  finite membership samples and explicit expected vectors, not exhaustive
  equivalence or statistical uniformity proofs.
- Inspected partitioner vectors and the 1,000 seeded four-chunk comparisons;
  prepared three-value reorder assertion; shard arithmetic, shard-ID errors,
  source-port enumeration/no-choice cases; tablet parser/range/V2 preference and
  version-block sampling; overlap, maintenance, original-leader, and state-update
  tests. Confirmed unit state-update tests do not observe worker publication.
- Inspected integration sources in
  `scylla/tests/integration/load_balancing/{token_awareness,simple_strategy,shards,lwt_optimisation,latency_awareness,tablets}.rs`,
  `statements/prepared.rs`, `session/pager.rs`, and
  `ccm/tablets_routing_v2.rs`. Rechecked the assertions: traced single-node RF=1
  token requests; server-token equality and cached CDC partitioner; no-PK pager
  success; per-receiving-node single shard rather than independent shard ownership;
  sampled LWT coordinator counts; cache replica/shard comparisons and no-feedback
  phases; V2 block presence, mixed-connection framing, V1 fallback; independent
  leader host ID rather than shard; invalid-query completion with latency enabled.
- Rechecked tablet/LWT/view/CDC support guards, CCM startup early return, and V2
  negotiation guards. Passing test discovery or process exit alone cannot show
  that their guarded routing assertions executed.
- A local validation script checked all **78 pinned implementation URL targets**
  for the correct commit, existing files, and in-range line anchors, and all
  **69 test/helper references** for named functions/modules in their specified
  source files. It reported no errors. Source reading supplied the semantic check;
  symbol existence alone was not treated as behavioral evidence.

## Remaining uncertainty and gaps

No unresolved factual discrepancy was identified in this source audit. Existing
explicit coverage gaps remain: concurrent plan/pool/statistics changes; tablet
send cancellation and delayed publication; stale feedback after schema recreation;
leader plus latency behavior; global ordered-NTS zero-factor DCs; V1 leader
interpretation; malformed/null/trailing tablet encodings; retained prepared routing
inputs; interop paths; and updater lifecycle/invalid parameter behavior. The new
numeric-error clarification is traced to parser and classifier code, not an
end-to-end latency callback regression.

Source quirks remain documented as reference limitations, not endorsed Go design:
null/unset key framing differences, raw string token normalization differences,
base-table-only partitioner lookup, unvalidated ignored-bit counts, and allocation
in fallback/update paths. No claim is made that runtime behavior or performance
was established by this documentation-only review.
