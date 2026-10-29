# Routing reference review — round 02

- Section: `feature-specs/routing.md`
- Round: `02` — defaults, exact conditions, limits, ordering, and fallback boundaries.
- Agent identity: `/root/review_routing_02`
- Source checkout: `/extra/scylladb/gocql-rust-reference-f8b0ee9`
- Source commit, checked with `git rev-parse HEAD`: `f8b0ee928dc0dcb475247dc82cce4e42804b0621`
- Before SHA-256: `3a6318c5b13df966a16784eb8c320a9b59c4d30af31df8aa000f511c454ae05f`
- After SHA-256: `da5b1d02b55b730f693c422ba7255e4de1689ac5cf0123e76b6e33f78ac38e14`

Read the complete review instructions, `feature-specs/README.md`, and the complete
assigned section. The previous report was context only; the behavior below was
traced independently in the pinned source. Changes are confined to the assigned
section and this report. No Go or Rust code was changed and no child agents were
created.

## Verified findings and fixes

1. **Token inspection's metadata requirement needed narrower scope.**
   `ClusterState::compute_token` serializes against `pk_column_specs`, and
   `lookup_table_meta` searches only `keyspace.tables`
   (`scylla/src/cluster/state.rs:618–661`). It does not search the separate
   materialized-view map. By contrast, supplied-token endpoint lookup checks only
   the keyspace, then delegates to the locator (`:690–719`), whose tablet cache
   includes views (`scylla/src/routing/locator/tablets.rs:993–1035`). Also, the
   `scylla_unstable` plus `unstable-csharp-rs` implementation exposes
   `compute_token_preserialized_with_partitioner`, which directly invokes the
   hashing helper without metadata lookup (`state.rs:897–974`). The original
   universal wording, “Token calculation requires table metadata,” omitted this
   exception. Scoped the ordinary API, documented the base-table/view boundary,
   and added the optional interop APIs' validation and metadata boundaries.
   Added the existing `unknown_keyspace_has_no_replicas` regression to the test
   inventory and identified the missing view/interop regressions.

2. **Connection-local preparation does not select the table's partitioner or
   rebuild batch routing metadata.** `PreparedStatement::new` initializes
   `partitioner_name` to its Murmur3 default (`statement/prepared.rs:248–269`;
   `routing/partitioner.rs:24–25`). `Connection::prepare` merely converts raw
   preparation into that statement (`network/connection.rs:762–770` and
   `statement/prepared.rs:86–108`). Session preparation separately replaces the
   partitioner using table metadata (`client/session.rs:2018–2038`). The
   unprepared-with-values execution branch calls the connection-local helper and
   immediately uses its token to choose the tablet hint (`:1718–1733`), so CDC
   metadata does not select the CDC partitioner on this path. Documented that
   limitation, scoped the general partitioner-selection claim, and qualified the
   server-compatible hashing invariant by valid keys and the correct partitioner.
   Separately confirmed that batch routing is constructed at `session.rs:1327–1349`
   before `Connection::prepare_batch` runs (`connection.rs:1259,1323–1368`); added
   the explicit boundary that its local preparation cannot retroactively supply
   the token/table or confirmed-LWT flag. The CDC integration case explicitly
   uses session preparation and checks `PartitionerName::CDC`
   (`tests/integration/load_balancing/tablets.rs:932–939`), so it does not cover
   the newly documented local-preparation limitation. Recorded that gap and sent
   the parent the cross-section implication for execution/configuration claims.

3. **Tablet shard validation and advertised shard limits were underspecified.**
   The wire shard is an `i32`, converted to `Shard` with `try_into`
   (`routing/locator/tablets.rs:285–306`); `Shard` is `u32`, while advertised
   `ShardCount` is `NonZeroU16` (`routing/sharding.rs:49–62`). Thus every
   nonnegative wire shard is representable even when it exceeds the known node's
   shard count. Payload parsing does not consult that count. Documented this
   boundary and the advertised 1–65,535 count range. Connection selection deals
   with unrepresentable `u16` hints by substituting zero and with absent shard
   pools by searching other shards (`network/connection_pool.rs:336–425`), so
   the spec continues to treat shards as hints. The existing out-of-range custom
   policy integration test covers execution fallback, not malformed tablet
   feedback; added the missing tablet-payload regression explicitly.

4. **Latency configuration's zero/unvalidated boundaries were missing.**
   The measurement minimum, retry period and exclusion threshold setters simply
   store their inputs (`policies/load_balancing/default.rs:3713–3747`). The
   predicate requires `num_measures >= minimum`, `age < retry_period`, and a
   strict floating-point microsecond comparison (`:3849–3873`). A zero retry
   period therefore prevents penalties; zero minimum still needs an actual
   average, and zero-duration samples never create one (`:3320–3332`). The
   threshold has no positivity/finiteness validation. Documented these exact
   conditions and the missing boundary tests. Also clarified that failed average
   conversion preserves the previous numeric value while still advancing the
   measurement count and timestamp (`:3346–3373`), and called the 100 ms default
   the global-minimum refresh interval rather than a minimum interval.

## Source and behavior independently checked

Paths in this section are relative to the pinned Rust checkout.

| Implementation inspected | Checks performed |
| --- | --- |
| `scylla/src/policies/load_balancing/mod.rs`, `plan.rs` | Complete policy/routing interfaces and Plan state machine: regular SERIAL/LOCAL_SERIAL classification, initial consistency lifetime, concurrent policy contract, fallback after `pick(None)`, exact raw-target filtering, unspecified-shard randomization. |
| `scylla/src/policies/load_balancing/default.rs:145–1274` | First choice and fallback grouping; connected versus enabled predicates; host/shard deduplication; random node rotations versus stored-order final fallback; fixed-seed replica behavior; token-awareness switch; inherited/explicit locality; DC failover; global LWT deferral; leader consistency/DC eligibility. |
| `scylla/src/policies/load_balancing/default.rs:3270–3873` | Moving-average update, zero measurements, cached minimum, error classification, stable latency partitioning, defaults, unchecked setter inputs and strict threshold/count/freshness conditions. |
| `scylla/src/policies/load_balancing/single_target.rs`, `scylla/src/policies/host_filter.rs` | Complete files: direct node versus ID/address lookup, empty fallback, address-translation caveat, allowlist resolution and acceptance of untranslatable peers, DC filter. |
| `scylla/src/routing/mod.rs`, `partitioner.rs:24–527`, `statement/prepared.rs:307–434,791–868` | Token normalization and parsing exception; supported suffixes; signed-byte Murmur3 tail and incremental hashing; CDC eight-byte and short-input behavior; partition-key index ordering, skipped null/unset values, composite length limit and distinct framing helpers. |
| `scylla/src/routing/locator/token_ring.rs`, `replication_info.rs:62–284`, `precomputed_replicas.rs:78–209` | Sorted exact/next lookup, wrap and empty rings; unique-node SS/NTS placement; zero/capped factors; rack repeat budget and missing rack; precomputed SS prefixes and NTS rack-count boundary. |
| `scylla/src/routing/locator/mod.rs:114–323,367–552,782–1012`, `replicas.rs` | Tablet override, strategy dispatch, missing range/DC, replica choice, shard calculation, borrowed/owned lists, deterministic global NTS continuation and the existing configured-zero-factor primary quirk. |
| `scylla/src/routing/locator/tablets.rs:179–1050` | Version nibble encoding, V2 payload preference, signed range and shard conversion, leader preservation, DC masks, unknown replica handling, whole-overlap replacement, equality/deduplication and metadata maintenance/sharing. |
| `scylla/src/routing/sharding.rs:15–307`; `scylla/src/cluster/node.rs:213–237`; `network/connection_pool.rs:336–425` | Port range/default, shard math, randomized complete compatible-port traversal, empty draw/iterator distinction, SUPPORTED validation, connected/enabled distinction, advisory-shard fallback. |
| `scylla-cql-core/src/frame/protocol_features.rs:62–199`; `client/execution.rs:61–113` | V2 precedence in STARTUP, prepared LWT mark, tablet-hint construction and shared speculative-plan synchronization. |
| `scylla/src/client/session.rs:1327–1369,1681–1755,1952–2038`; `network/connection.rs:718–770,1113–1223,1252–1368,2045–2069` | Batch and unprepared metadata lifetime; preparation partitioner difference; EXECUTE hint emission and repreparation feedback; BATCH exclusion; bounded awaited feedback send and logged parsing failure. Session constants/defaults at `:73,471,1442` were also checked. |
| `scylla/src/cluster/state.rs:569–583,613–737,837–974`; `cluster/worker.rs:302–340` | Tablet keyspaces excluded from vnode precomputation, inspection APIs, optional interop entry points, feedback batching, lazy snapshot creation, same-batch comparisons and absence of version ordering. |

No changes were needed to the established policy defaults, ordering or failover
rules. In particular, explicit DC failover is allowed at local consistency levels;
a nonexistent preferred DC has no local nodes; a remote replica may precede a
local non-replica; rack preference does not prohibit other racks; disabled nodes
do not reappear in the last resort. The full fallback's final enabled-node groups
retain stored ring order while fast node picks rotate randomly. LWT order applies
to replicas, and neither ordinary node fallback nor configured latency penalties
inherits that determinism. Leader promotion still requires token awareness,
global consistency, a known original leader, eligible DC and liveness, and excludes
ONE/LOCAL_ONE. No V2-version guard exists in `known_leader`.

Tablet invariants were also checked: left-open wire ranges become inclusive,
nonoverlapping cached ranges; whole overlapping entries are replaced; unknown
replicas are omitted until maintenance; unresolved leaders cannot be replaced by
followers; resolved equivalent feedback is a no-op; out-of-order versions remain
accepted; snapshot sharing does not mutate retained table storage. Native BATCH
does not forward tablet feedback. Port exhaustion, the unchecked ignored-bit
range, invalid-key framing differences, and allocation caveats remain explicit.

## Test coverage and validation

This was source inspection: **no Rust, Go, or database test suite was executed**.
A read-only Python check verified all **51** cited test declarations and **38**
pinned source file/line anchors in the revised section; it also checked trailing
whitespace. These checks do not establish runtime correctness.

Inspected policy test assertions cover token-aware and token-unaware groups,
fixed-seed replica ordering, local consistency with failover, no local-rack
replica, regular SERIAL/LOCAL_SERIAL, leader location/down-node handling and
preference inheritance (`default.rs`, selected cases at `1600–1831`,
`2035–2220`, `2601–2765`, `2770–3270`). The Plan fallback regression was read in
full. Locator/ring/hash/shard tests were checked for expected placement vectors,
tablet order and masks crossing 64 bits, hash vectors/chunking, advertised shard
bounds and source-port exhaustion. Tablet tests inspected include invalid ranges,
payload V2 preference, nibble encoding, overlap/neighbor replacement, keyspace
maintenance and leader resolution. `state.rs:1381–1460` checks no-change updates,
same-batch replacement and differences in version/shard/range.

Latency tests inspected use counts below/equal to the configured minimum,
latencies below/above the threshold, expiry after twice the retry period and
explicit minimum updates (`default.rs:4008–4274`, selected token-aware cases at
`4520–4599`). They do not prove zero retry period, zero minimum, non-finite
threshold behavior or exact time/threshold equality; those remain source-derived
conditions or explicit coverage gaps.

Integration tests inspected distinguish RF-one traced ownership, ordinary
SimpleStrategy results, one shard per contacted node and an out-of-range custom
hint. LWT tests check repeated prepared calls, batches and an LWT in a later
prepared batch slot. Tablet tests learn and verify base/view/CDC routing and
refresh survival; the CDC path uses session preparation. V2 tests assert
cold/warm EXECUTE block presence, convergence, separate negotiated connection
types, observed-leader LOCAL_QUORUM reads, and V1 operation with V2 hidden
(`tests/integration/ccm/tablets_routing_v2.rs`, selected assertions at
`409–556`, `625–661`, `760–837`, `839–987`). Mixed connections are forced onto
each node/shard; that test does not assert a retry crossing feature types. Feature
gates and probabilistic warm-up are not treated as unconditional coverage.

New regression gaps are listed in the spec. Previously identified gaps—global
ordered-NTS zero-factor behavior, V1 leader interpretation, several disconnected
fallback nodes, and BATCH versus EXECUTE cache feedback—remain. Go API, race,
integration and allocation coverage remains pending. The parent was notified of
the preparation/partitioner cross-section concern; no other section was edited.
