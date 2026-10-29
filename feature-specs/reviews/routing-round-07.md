# Routing review round 07

- Section: `feature-specs/routing.md`
- Round: 07 — source anchors and implementation-versus-contract wording
- Agent identity: `/root/review_routing_07`
- Reference checkout: `/extra/scylladb/gocql-rust-reference-f8b0ee9`
- Source commit, verified with `git rev-parse HEAD`:
  `f8b0ee928dc0dcb475247dc82cce4e42804b0621`
- Before SHA-256:
  `f8046ad4c31630752e70c76e9441d81ac2eec3f1c199654dea368cca8dc51529`
- After SHA-256:
  `e36ca99f28606829fbe81ad48a42beca9c10e5fb2687d85fe7c32f2886f8f37d`

Read `REVIEW-INSTRUCTIONS.md`, `feature-specs/README.md`, and the complete routing
section. Reviewed the pinned implementation independently; prior reports were
consulted only after the source audit, as context. Changed only the assigned
specification and this report. No Go or Rust implementation was changed.

## Verified findings and fixes

1. **Several evidence links did not land on the implementation named by their
   labels.** All original 55 distinct anchors named existing files and valid
   lines, but existence alone did not make them precise evidence. Examples:
   `plan.rs:90` was a constructor field rather than the lazy iterator;
   `default.rs:1012` was `Default for DefaultPolicy`, not the builder that chooses
   the fixed seed; `tablets.rs:925` was the end of `add_tablet`, not metadata
   maintenance; and `sharding.rs:15` was a port-range type attribute, not the
   sharder or SUPPORTED parser. Replaced these and other imprecise entry points
   with the implementing methods, and split combined links where separate
   symbols supply the evidence. In particular, the revised table directly links
   `Plan::new`/shard filling/`next` (`plan.rs:81–167`), builder initialization and
   construction (`default.rs:1050–1091`), `TabletsInfo::perform_maintenance`
   (`tablets.rs:986–1049`), and sharding validation/arithmetic/parsing
   (`sharding.rs:29–35,121–254,276–309`). Added direct EXECUTE serializer evidence:
   `scylla-cql/src/frame/request/execute.rs:81–104` appends the optional block byte
   after parameters, while `connection.rs:1105–1155` decides whether it is present.
   The revised specification has 68 distinct valid pinned source anchors.

2. **Two invariants read as universal policy requirements despite custom policies
   having their own behavior.** The locality and LWT invariants are now explicitly
   scoped to the default policy. `LoadBalancingPolicy` (`mod.rs:180–218`) delegates
   target production to the implementation. `Plan::next` (`plan.rs:113–167`) uses
   those returned targets without adding default locality or LWT rules.
   `SingleTargetLoadBalancingPolicy::pick` (`single_target.rs:72–97`) demonstrates
   this distinction by directly using a supplied node or resolving an identifier.
   The default policy applies LWT classification at `default.rs:174–178,344–348`
   and its own eligible node groups at `:269–330,440–492`. This correction makes the
   invariants agree with the existing custom-policy section without proposing a
   new Go contract.

3. **Leader eligibility wording obscured the different latency stages in fast
   choice and full fallback.** The previous combined connected/latency-predicate
   sentence could suggest that a penalized leader is never promoted. The fast
   choice passes `pick_predicate` to leader selection (`default.rs:185–197`),
   which includes latency when configured (`:1061–1071`). The full fallback passes
   only liveness (`:362–369`), puts that leader first (`:427–435`), then stably
   partitions the entire plan by latency (`:564–569,3489–3525`). Clarified that a
   penalized eligible leader moves behind unpenalized targets but remains first
   among penalized targets in that fallback. Added an explicit missing regression
   for the combined leader/latency behavior; the inspected leader and latency
   fixtures test those features separately.

## Source inspected and behaviors checked

Paths below are relative to the pinned Rust checkout. Ranges describe inspected
implementation regions; test inspection is recorded separately below.

| Source | Symbols and checks |
| --- | --- |
| `scylla/src/policies/load_balancing/mod.rs:25–220`, `plan.rs:8–167` | Six routing inputs, first consistency, regular SERIAL/LOCAL_SERIAL classification, custom-policy concurrency/callback contract, lazy first choice, fallback after `None`, first raw-target exclusion, random shard filling. |
| `scylla/src/policies/load_balancing/default.rs:145–1275` | Routing metadata gate, token-awareness disable, location inheritance/override, explicit failover, replica/node priorities, enabled final groups, host/shard deduplication, shuffling versus rotation, LWT first-primary deferral, leader consistency and DC eligibility, builder defaults/fixed seed. |
| `scylla/src/policies/load_balancing/default.rs:3270–3873` | Per-host averages and locking, zero-duration handling, elapsed weighting, cached minimum, count/freshness/strict threshold conditions, stable fallback partition, error classification, defaults, unchecked threshold/update rate, zero scale, updater construction/lifetime. |
| `scylla/src/policies/load_balancing/single_target.rs:11–110`, `scylla/src/policies/host_filter.rs:15–98` | Direct node/host-ID/address targeting, empty fallback, address caveat, allow-all/DC/allowlist filters, unconditional acceptance of untranslatable allowlist addresses. |
| `scylla/src/client/execution.rs:61–114,430–673`, `scylla/src/policies/speculative_execution.rs:187–243` | Tablet hint cases, serialized shared-plan advancement, connection-failure continuation, callbacks before retry decisions, same/next target, final errors, timeouts and dropped unfinished speculative attempts. |
| `scylla/src/client/session.rs:1327–1368,1660–1755,1841–1860,2018–2021,2095–2098`; `scylla/src/client/pager.rs:134–145,325–375,940–1035` | Batch first-prepared routing and whole-batch LWT marking; local preparation and suppressed key errors; automatic paging preparation; session partitioner assignment; retained pager snapshot, stability target, per-page plans and reused tablet hint. |
| `scylla/src/statement/prepared.rs:248–275,307–435,784–876`; `scylla/src/routing/partitioner.rs:19–145,300–425`; `scylla/src/routing/mod.rs:19–97` | PK indexes/order, missing values, null/unset filtering, single/composite framing, streaming token versus owned key APIs, partitioner suffix/default selection, sealed hashers, CDC short input/first-eight-byte behavior, token normalization. |
| `scylla/src/routing/locator/token_ring.rs:15–69`, `replication_info.rs:54–207`, `precomputed_replicas.rs:63–217` | Exact/next/wrapped ring lookup, unique-node order, SimpleStrategy filtering, per-DC NTS and rack repeat budget, zero/capped factors, compressed prefixes and above-rack-count precomputation. |
| `scylla/src/routing/locator/mod.rs:65–310,362–910` | Tablet override including unknown ranges, strategy dispatch, direct ring/replica APIs, random filtered selection, datacenter iteration order, iterator skipping, lazy global NTS order and its zero-factor primary quirk. |
| `scylla/src/routing/locator/tablets.rs:179–1049` | Version blocks, V2 precedence without parse fallback, signed range/shard validation, all-or-nothing parsing, partial host resolution, original-leader preservation, DC masks, overlap replacement, no-op comparison, repair/removal, table/view maintenance and structural sharing. |
| `scylla/src/cluster/state.rs:618–740,837–980`, `scylla/src/cluster/worker.rs:302–387` | Base-table token metadata versus keyspace-only supplied-token lookup, interop gates and metadata bypass, sequential in-batch comparison, no schema-generation fence, serial worker publication. |
| `scylla/src/network/connection.rs:1105–1252,2045–2070`, `scylla-cql-core/src/frame/protocol_features.rs:63–135`, `scylla-cql/src/frame/request/execute.rs:81–104` | V2-over-V1 STARTUP, connection-specific byte emission, feedback before database-error handling and after repreparation, awaited channel send and ignored receiver closure. Channel size/construction checked at `session.rs:73,1442`. |
| `scylla/src/routing/sharding.rs:18–309`, `scylla/src/network/connection_pool.rs:266–273,316–420`, `scylla/src/cluster/node.rs:213–240` | Token/string normalization distinction, shard arithmetic, source-port range/iteration/panic cases, advertised count/ID validation, unchecked MSB shift range, live pool/sharder reads, advisory shard fallback and enabled/connected distinction. |

The complete language-independent sections, subfeature list, invariants,
implementation exceptions, Go coverage placeholders, and test-generation notes
were reviewed. No additional mismatch was established in these sections.

## Test claims and limitations

- Mechanically verified all 66 path-qualified test-reference occurrences against
  actual function declarations, including the intentionally misspelled
  `test_simple_stategy`. This check establishes reference accuracy only.
- Read policy test harnesses and representative ordinary/LWT/location/leader
  cases, including the 256-plan grouping helper (`default.rs:1599–1625`),
  enabled-as-connected fixtures, leader-down setup, and separate latency fixtures
  (`:3989–4255`). Their assertions support the stated grouped-plan and sampled
  ordering claims, not independent equivalence of `pick` and `fallback`.
- Checked the plan fallback regression (`plan.rs:228–247`), ring vectors,
  on-demand/precomputed replication vectors, direct locator/`nth` helpers
  (`locator/test.rs:224–260,623–795`), and multiword tablet masks
  (`locator/mod.rs:1030–1070`). These retain the stated finite-vector limits.
- Read prepared extraction assertions (`prepared.rs:910–951`), hash vectors and
  seeded four-chunk tests (`partitioner.rs:440–527`), shard arithmetic/port tests
  (`sharding.rs:351–505`), tablet parser/version assertions
  (`tablets.rs:1083–1177,1235–1368`), range/maintenance fixtures, original-leader
  tests (`:2280–2319`), snapshot deduplication (`state.rs:1381–1460`), and the
  unknown-keyspace endpoint fixture (`:1221` onward). No claim of exhaustive
  malformed-input, threshold-boundary, or concurrency coverage was added.
- Read the actual integration assertions in `load_balancing/token_awareness.rs`,
  `simple_strategy.rs`, `shards.rs`, `lwt_optimisation.rs`, and
  `latency_awareness.rs`, plus prepared token-comparison/partitioner and
  token-unaware-pager cases. Confirmed the distinctions between one traced node,
  one sampled shard per contacted node, row correctness, and an expected query
  error.
- Inspected tablet feedback/coordinator checks and feature guards in
  `load_balancing/tablets.rs:249–312,596–641,713–810,832–990,1014–1225`, and
  startup/feature guards, V2/mixed/V1 checks, and decoded-leader host comparisons
  in `ccm/tablets_routing_v2.rs`. Existing skip caveats, finite convergence samples,
  and the leader test's host-only assertion remain accurate.
- Added only the explicit combined leader/latency test gap. Existing gaps for
  key errors, parser tails, late feedback, cancellation/publication races, exact
  latency equality, special interop paths, and Go migration remain unresolved.

No Rust or Go tests, database processes, benchmarks, or network checks were run.
Validation consisted of pinned-source inspection, anchor/function-reference
checks, whitespace checking, and SHA-256 verification. No cross-section conflict
requiring another agent's file to change was found.
