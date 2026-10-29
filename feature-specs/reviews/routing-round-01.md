# Routing reference review — round 01

- Section: `feature-specs/routing.md`
- Round: `01` — trace claims to implementation and correct factual mismatches.
- Agent identity: `/root/review_routing_01`
- Source checkout: `/extra/scylladb/gocql-rust-reference-f8b0ee9`
- Source commit, verified with `git rev-parse HEAD`: `f8b0ee928dc0dcb475247dc82cce4e42804b0621`
- Before SHA-256: `4e2bff84fca2d0d2c118ad641b023d8617ceacb768e812e45f6d14fe0db21ff5`
- After SHA-256: `3a6318c5b13df966a16784eb8c320a9b59c4d30af31df8aa000f511c454ae05f`

Read `feature-specs/README.md`, the complete routing specification, and the review
instructions. Inspected the pinned implementation independently. Only the assigned
specification and this report were changed; no Go or Rust implementation was edited.

## Verified findings and fixes

1. **The final fallback node groups do not use a random rotation.** The original
   description applied random rotations to all non-replica groups. In
   `scylla/src/policies/load_balancing/default.rs:482–493`, the final enabled-node
   groups use direct slice iteration; connected groups use `round_robin_nodes`
   (`:451–476`), and fast picks use `pick_node` even at the enabled-node stages
   (`:312–320`, `:878–900`). `ReplicationInfo::new`
   (`scylla/src/routing/locator/replication_info.rs:63–88`) obtains those slices
   by retaining first unique occurrences in the sorted global/datacenter rings.
   Corrected the ordering paragraph to distinguish these cases. Added the missing
   explicit multi-disconnected-node ordering regression to the test inventory.

2. **Tablet cache learning was described too generally for QUERY and BATCH.**
   The unknown-range paragraph said fallback learns from responses without naming
   the response path. `Connection::query_raw_with_consistency`
   (`scylla/src/network/connection.rs:960–994`) returns the response directly, and
   `batch_with_consistency` (`:1252–1321`) performs its result/repreparation handling
   without forwarding tablet feedback. Only the EXECUTE path calls
   `update_tablets_from_response` (`:1167–1173`, `:1211–1215`), and only EXECUTE
   emits the version block (`:1129–1156`). Session unprepared calls with bind
   values prepare and EXECUTE locally (`scylla/src/client/session.rs:1715–1758`).
   Scoped learning to EXECUTE, documented the native QUERY/BATCH boundary and the
   bound-unprepared exception, added the BATCH implementation link, and recorded
   the missing explicit regression for this distinction.

3. **Source-port exhaustion omitted the public draw helper's panic behavior.**
   `Sharder::draw_source_port_for_shard`
   (`scylla/src/routing/sharding.rs:136–141`) unwraps the optional port with an
   `expect`; iteration instead returns empty when there is no candidate
   (`:214–220`). Both ranged helpers assert that the shard is within count
   (`:172`, `:207`). The default ephemeral range can also lack a candidate for
   large shard counts: the existing `draw_source_port_for_shard_panics_on_no_choice`
   test (`:499–505`) uses 40,000 shards and shard 30,000. Documented these limits
   and added that existing regression to the test references.

4. **The leader integration coverage statement was broader than its assertion.**
   `test_leader_aware_routing_targets_the_raft_leader`
   (`scylla/tests/integration/ccm/tablets_routing_v2.rs:693–837`) warms the cache
   with an insert and reads, then compares the coordinators of LOCAL_QUORUM
   SELECT requests against the leader observed in V2 feedback (`:804–816`).
   It does not assert coordinator choice for all strongly consistent operations.
   Narrowed the test description to strong LOCAL_QUORUM reads after warm-up.

## Implementation inspected

Paths below are relative to the pinned Rust checkout. Line ranges identify code
actually inspected, including relevant callers/callees and source tests.

| Files and symbols | Evidence inspected |
| --- | --- |
| `scylla/src/policies/load_balancing/mod.rs` | `RoutingInfo`, `should_route_as_lwt`, policy methods and callbacks, lines 25–240 |
| `scylla/src/policies/load_balancing/plan.rs` | First/fallback state machine, exact-target filtering, random shard fill, and no-first-choice regression, lines 90–256 |
| `scylla/src/policies/load_balancing/default.rs` | `pick`, `fallback`, replica/node helpers, leader eligibility, processed routing metadata and builder defaults, lines 145–1277; policy expected groups and LWT/leader/preference tests at 1600–3270; latency implementation at 3270–3880 and relevant tests at 4008–4599 |
| `scylla/src/policies/load_balancing/single_target.rs` | Node object, host ID and socket-address lookup; empty fallback; complete file |
| `scylla/src/policies/host_filter.rs` | Accept-all, datacenter and allowlist filters, including untranslatable-address handling; complete file |
| `scylla/src/routing/mod.rs` | Token normalization and location preference, lines 20–109 |
| `scylla/src/routing/partitioner.rs` | Partitioner suffix recognition, sealed streaming interface, signed-byte Murmur3 tail, CDC state machine, serialized-key framing, lines 24–424; vector/chunking tests at 440–526 |
| `scylla/src/statement/prepared.rs` | Token-awareness flag, partition key/token APIs and table identity, lines 307–434; extraction and encoding, lines 781–872 |
| `scylla/src/routing/locator/token_ring.rs` | Sorting, exact/next token lookup and wrap, lines 16–75; ring-boundary assertions starting at 85 |
| `scylla/src/routing/locator/replication_info.rs` | Unique node lists, per-DC rings, rack count, SS/NTS iterators and zero-factor behavior, lines 63–220; placement assertions through 297 |
| `scylla/src/routing/locator/precomputed_replicas.rs` | Maximum SS factor, NTS rack-count prefix boundary and per-factor lists, lookup fallback, lines 78–223 |
| `scylla/src/routing/locator/mod.rs` | Tablet override, strategy dispatch, version/leader lookup, lines 114–239; lazy ordered NTS and zero-factor quirk, lines 782–1010; ordered/multiword-mask assertions at 1024–1179 |
| `scylla/src/routing/locator/test.rs` | Locator assertions beginning at 224 and tablet replica/mask/unknown-range assertions at 799–899 |
| `scylla/src/routing/locator/tablets.rs` | Version/payload parsing, range validation, replica resolution, leader resolution, replacement, sharing and maintenance, lines 179–1060; parser, nibble, range-replacement, maintenance and leader tests at 1083–1370, 1671–2239 and 2280–2320 |
| `scylla/src/routing/sharding.rs` | Port range validation, Token string parsing, sharder math and helpers, SUPPORTED parsing, lines 15–307; arithmetic, invalid IDs, empty range and draw-panic tests at 351–505 |
| `scylla-cql-core/src/frame/protocol_features.rs` | V1/V2 detection and V2 precedence; LWT prepared-flag test, lines 62–150; negotiation tests starting at 169 |
| `scylla/src/client/session.rs` | Channel capacity/construction at 73 and 1442; first-statement batch metadata and whole-batch LWT flag, lines 1327–1374; unprepared routing/local prepare, lines 1681–1758; partitioner metadata extraction, lines 2018–2038 |
| `scylla/src/client/execution.rs` | Version hint selection and shared plan synchronization, lines 61–110 |
| `scylla/src/network/connection.rs` | Native QUERY, versioned EXECUTE and repreparation, native BATCH, feedback sender, lines 960–994, 1113–1224, 1252–1369 and 2045–2069 |
| `scylla/src/cluster/state.rs` | Exclusion of tablet keyspaces from vnode precomputation, lines 569–584; public token/endpoint methods at 613–737; batch-aware lazy tablet updates at 837–892; no-change regression at 1381–1468 |
| `scylla/src/cluster/worker.rs` | Bounded feedback batching and conditional snapshot publication, lines 302–340 |
| `scylla/tests/integration/load_balancing/` | `token_awareness.rs`, `simple_strategy.rs`, `shards.rs`, `lwt_optimisation.rs`, and `latency_awareness.rs`; tablet feedback verification, view/CDC maintenance and feature-gated cases in `tablets.rs:596–645,832–990,1068–1235` |
| `scylla/tests/integration/ccm/tablets_routing_v2.rs` | V2 cold/warm blocks, mixed-connection framing, observed-leader read routing and V1 fallback assertions, lines 409–675,693–925 |

## Checked behaviors and source evidence

- Routing metadata uses initial consistency and is not rebuilt on retry;
  `RoutingInfo` documents this boundary. Prepared keys use partition-key sequence,
  skip null/unset inputs and error on missing indexed values. Table identity comes
  from the first column specification. Batch routing uses only the first prepared
  statement's token/table but scans all prepared statements for LWT confirmation.
- Custom policies retain control over destinations. The plan invokes fallback
  after `pick` returns `None`, removes exact copies of its first raw target, and
  samples only unspecified shards. Default fallback has its own host/shard
  comparator; explicit different shards on one host remain distinct.
- Default token awareness, location inheritance/override, disabled DC failover,
  optional fixed random seed, connected/last-resort grouping and host exclusion
  match the builder, helpers and policy implementation. Local consistency does not
  override explicitly permitted datacenter failover.
- Regular SERIAL/LOCAL_SERIAL and prepared confirmation each enable LWT routing;
  optional serial consistency alone does not. Global LWT first-choice selection
  defers to fallback when its first ordered replica fails the predicate. DC/rack
  paths filter deterministically; non-replica and latency behavior stays separate.
- Leader promotion requires global keyspace consistency, token/table metadata,
  token awareness, a known tablet leader, permitted DC and liveness, and excludes
  ONE/LOCAL_ONE. The original first replica remains the leader only if resolved;
  an unresolved follower does not hide it. `known_leader` contains no V2-version
  guard; the existing warning and missing-regression note are accurate.
- SS/NTS placement, rack repeat budget, cap/zero factors, unknown-strategy RF-one
  fallback, precomputation boundary and lazy global NTS ordering match the source.
  The fast ordered-NTS path checks DC-map membership, not a positive factor; the
  existing limitation is retained.
- Murmur3's signed-byte tail, streaming chunk independence, CDC first-eight-byte
  behavior and short-input minimum token agree with the implementation. Token
  string parsing bypasses normalization. Prepared extraction and the standalone
  serialized-key helper choose composite framing differently for invalid keys.
- Tablet ranges are left-open/right-closed on the wire, nonwrapping and nonempty;
  cache replacement removes entire overlapping entries. Known tablet tables and
  views have empty cache entries before learning. Missing ranges yield no replicas.
- V2 negotiation outranks V1; payload parsing prefers V2 without retrying malformed
  V2 as V1. Nibble index/value encoding, random cold hints, zero metadata-free hints,
  per-connection emission and probabilistic refresh agree with the source.
- EXECUTE feedback awaits the 8,192-entry channel and ignores channel closure;
  malformed tablet data is logged. Cluster updates compare against earlier entries
  in the same batch, deduplicate only fully resolved equivalents and do not impose
  version order. Structural sharing preserves snapshots. Maintenance resolves or
  drops unknown mappings, drops removed-node tablets and replaces recreated nodes.
- Shard hints are advisory, the sharder formula and source-port modulo mapping
  agree with the source, and parsed shard count/ID checks are present. The ignored
  MSB byte has no semantic range validation; the existing explicit limitation is
  retained, including in response to the connections reviewer's cross-check.
- Single-target lookup and host filtering match their implementations. Endpoint
  lookup requires keyspace metadata but does not separately validate table existence;
  token computation requires table metadata, and deprecated lookup hides its error.
- Latency measurements use host IDs, the documented moving-average weight, sample
  count/freshness/strict microsecond threshold, and a cached minimum without age
  filtering. Zero samples and failure classification match the source; penalties
  stably reorder fallback targets without admitting excluded nodes.

## Test coverage and validation

The review read source tests; **no Rust, Go or database tests were executed**.
All 50 cited Rust test declarations in the revised specification were checked
against the pinned files. Inspected assertions support the specified policy
groups, SERIAL/LOCAL_SERIAL routing, fallback invocation, leader location/down-node
handling, placement/order/mask behavior, hash vectors/chunking, shard boundaries,
tablet parsing/replacement/maintenance, no-change updates and latency penalties.

Integration assertions were checked for owner/shard selection, stable LWT and
later-in-batch LWT marking, base/view/CDC tablet routing, negotiated V1/V2 framing,
convergence and leader reads. View/CDC/LWT tablet cases have server feature gates;
V2 tests also gate unsupported servers. The mixed-connection test forces requests
onto each connection type; it does not itself induce a retry between feature types.
The latency integration test sends invalid CQL and expects an error, so it supports
the specification's modest claim that the policy executes a real request.

Remaining explicit gaps are the global ordered-NTS zero-factor regression,
V1-only leader interpretation, multiple disconnected-node fallback ordering and
native BATCH versus EXECUTE cache learning. Source-port draw exhaustion already
has a regression, now referenced. Go unit/race, integration and allocation coverage
remains unimplemented in this reference task. No passing runtime coverage is
inferred from inspected source tests. File/link/test-reference and whitespace
checks were performed on the revised Markdown.
