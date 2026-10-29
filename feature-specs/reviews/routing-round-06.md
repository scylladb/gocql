# Routing review round 06

- Section: `feature-specs/routing.md`
- Round: 06, omitted public/optional features and less-common algorithms/APIs
- Agent identity: `/root/review_routing_06`
- Rust source commit: `f8b0ee928dc0dcb475247dc82cce4e42804b0621`
- Reference checkout: `/extra/scylladb/gocql-rust-reference-f8b0ee9`
- Before SHA-256: `5ef2ce2902280f536cac83e31165fc2e42568984c3a2b5db16e06140904318a5`
- After SHA-256: `f8046ad4c31630752e70c76e9441d81ac2eec3f1c199654dea368cca8dc51529`

Read `REVIEW-INSTRUCTIONS.md`, `feature-specs/README.md`, and the complete
assigned specification. Checked the reference HEAD and clean tracked worktree.
The audit followed implementation independently; earlier review reports were not
used as proof. Only this report and `routing.md` were edited. No implementation
or test files were changed, and no Rust, Go, or database tests were executed.

## Catalogue-owned modules inspected

The machine-readable catalogue assigns these 13 production modules to routing.
Paths below are relative to the pinned reference checkout.

| Module | Inspected behavior and key source locations |
| --- | --- |
| `scylla/src/policies/load_balancing/mod.rs` | `RoutingInfo` fields/default/LWT classification, optional Python constructor, policy callbacks and fallback contract, lines 25–234. |
| `scylla/src/policies/load_balancing/plan.rs` | Lazy `Plan::next`, no-pick fallback, exact first-target removal, shard filling, lines 90–195; regression at 267. |
| `scylla/src/policies/load_balancing/single_target.rs` | All `NodeIdentifier` variants, snapshot lookup/direct node use, empty fallback, lines 12–111. |
| `scylla/src/policies/load_balancing/default.rs` | `pick`/`fallback`, deduplication, helpers, leader/LWT/location decisions, builder defaults, lines 145–1275; latency averages, updater, setters and error classification, lines 3270–3875; test harness and selected test assertions. |
| `scylla/src/routing/mod.rs` | `Token` normalization/sentinel and `NodeLocationPreference`, lines 20–106. |
| `scylla/src/routing/partitioner.rs` | Public sealed traits and enum dispatch, Murmur3 signed-tail stream state, CDC first-eight-byte state, serialized-key framing, lines 22–423; hash/chunk tests, lines 425–529. |
| `scylla/src/routing/sharding.rs` | Port-range validation, token parsing, shard arithmetic, source-port iteration/draw, SUPPORTED parsing, lines 15–308; boundary tests at 334–508. |
| `scylla/src/routing/locator/mod.rs` | Replica-strategy/tablet dispatch, public inspection, caller-RNG selection, ordinary iterator/`nth`/size hints, ordered NTS continuation, lines 67–1013; multiword-mask test, lines 1030–1070. |
| `scylla/src/routing/locator/replicas.rs` | Borrowed versus owned replica arrays, indexed access and iteration, lines 1–62. |
| `scylla/src/routing/locator/replication_info.rs` | Global/DC rings, missing-rack counting, SimpleStrategy and NTS repeat budgets, lines 53–224; fixed vectors afterward. |
| `scylla/src/routing/locator/precomputed_replicas.rs` | Minimum global RF=1, compressed prefixes, separate above-rack-count lists, lookup misses, lines 62–210; fixed precomputation tests afterward. |
| `scylla/src/routing/locator/token_ring.rs` | Sorting, token-relative traversal, exact/next lookup, wrap and length, lines 16–70; nonempty boundary vectors afterward. |
| `scylla/src/routing/locator/tablets.rs` | V1/V2 parsing/version blocks, replica resolution/DC masks, leader identity, overlap insertion, copy-on-write maintenance, lines 179–1051; parser, mask and maintenance test bodies. |

The two catalogue test-evidence modules were also checked:
`scylla/src/routing/locator/test.rs` (the `test_locator` helper call graph and
tablet replica tests, lines 224–888) and `scylla/src/book_tests.rs:150–158`
(load-balancing Markdown doctest inclusions). Read the included load-balancing
documentation as context; implementation took precedence over prose.

Cross-module traces included:

- `scylla/src/statement/prepared.rs:307–430,781–884`: public routing/key APIs,
  extraction and encoded-key writer.
- `scylla/src/cluster/state.rs:613–758,837–976`: token/endpoints inspection,
  tablet update deduplication and C# interop APIs; locator construction and host
  filtering at 427–579.
- `scylla/src/client/session.rs:1327–1361,1681–1750,1841–1856,2018–2038,2095–2133`:
  batch first-statement routing, unprepared dispatch/hint errors, automatic
  preparation and metadata-based partitioner assignment.
- `scylla/src/client/execution.rs:61–113,445–510,547–673` and
  `scylla/src/client/pager.rs:340–378,974–1013`: shared plans, callback/retry
  boundaries, per-page plans, stable coordinators and retained tablet hints.
- `scylla/src/network/connection.rs:1113–1221,2045–2068`,
  `scylla/src/cluster/worker.rs:302–340`, and
  `scylla-cql-core/src/frame/protocol_features.rs:62–145`: per-connection
  negotiation/emission, EXECUTE feedback/error handling, enqueue/publication.
- `scylla/src/policies/host_filter.rs:15–116`: accept-all, DC and address
  allowlist semantics; `scylla/Cargo.toml:74–85` for optional interop gates.

## Verified findings and fixes

1. **Direct public replica/ring APIs were not specified.** The section described
   placement algorithms and high-level endpoint APIs but omitted
   `ClusterState::replica_locator`, `ReplicaLocator` inspection, and
   `ReplicaSet::choose_filtered` with a caller-supplied RNG. Added a dedicated
   subsection covering supplied strategies/tablet override, absence of policy
   eligibility filtering, global/DC ring inspection, vnode counts, selection,
   ordinary versus ordered iteration, cloning and skip exhaustion. Evidence:
   `cluster/state.rs:717–758`; `routing/locator/mod.rs:114–205,243–263,369–500,602–774,776–1013`;
   `routing/locator/token_ring.rs:27–70`; `replication_info.rs:62–111`.
   Added the existing locator API/iterator tests with their actual scope.

2. **The public raw-byte hashing contract was missing.** The previous text
   described stream hashing only as an internal optimization. Added that public
   hashing requires no session/schema, inserts no partition-key framing, permits
   repeated non-consuming finalization and later writes, and uses sealed
   Murmur3/CDC traits rather than a custom-partitioner extension point. Evidence:
   `routing/partitioner.rs:47–135,226–310,336–378`. `finish(&self)` reads the
   current state; framing occurs in separate prepared/serialized-key helpers.

3. **Prepared key materialization was omitted and differs from token absence.**
   `PreparedStatement::compute_partition_key` returns owned encoded bytes, while
   `calculate_token` streams the encoding and returns an optional token. With no
   PK indexes and successful row serialization, the former returns empty bytes
   and the latter returns `None`. Added those public behaviors and propagated
   error boundaries. Evidence: `statement/prepared.rs:307–313,348–408,787–884`.

4. **The optional Python routing-input constructor was missing.** The C#
   preserialized token APIs were already covered, but the independent
   `scylla_unstable` + `unstable-python-rs` constructor was not. Added its six-field
   passthrough behavior, borrowed inputs and absence of computation/validation.
   Evidence: `policies/load_balancing/mod.rs:126–151`,
   `scylla/Cargo.toml:79–85`. Existing C# behavior was checked independently at
   `cluster/state.rs:897–976`.

5. **Latency updater ownership and zero-update-rate behavior were omitted.**
   Added policy-instance ownership/sharing, runtime-required updater spawning,
   cancellation on final policy drop, and the unchecked zero update rate causing
   the timer to panic inside the updater rather than returning a builder error.
   Evidence: `default.rs:1061–1074,3394–3398,3439–3467,3760–3765`.
   Corroborated dependency semantics in locally resolved Tokio 1.54.1
   `src/time/interval.rs:73–76`, `src/task/spawn.rs:174–215`, and futures-util
   0.3.34 `src/future/future/remote_handle.rs:24–40,91–121`. These dependency
   sources are in the Cargo registry; the checkout's generated/ignored lockfile
   is not part of the pinned commit. No claim is made that dependency resolution
   itself is pinned by that commit.

## Whole-section checks retained without further findings

- Verified first-attempt consistency and input lifetime; prepared/unprepared,
  automatic/manual paging and batch extraction boundaries; key errors versus
  benign missing routing metadata; no plan restart on retry consistency changes.
- Verified lazy pick/fallback, exact raw-first-target suppression, shared
  speculative advancement, shard filling, connection-acquisition failures
  skipping retry/callbacks, failed-attempt callbacks before retry decisions, and
  the distinction between feedback enqueueing and later snapshot publication.
- Verified default configuration, session preference inheritance/override, all
  ordering groups, failover independent of local consistency, LWT primary
  deferral, leader eligibility, disconnected-enabled final stages, live pool
  observations and ordinary versus LWT filtering timing.
- Verified token normalization/string parsing exceptions, Murmur3 signed tails,
  CDC short inputs, effective-key framing, NTS rack budgets/zero-factor quirk,
  precomputation boundaries, explicit tablet shards and advisory shard behavior.
- Verified cold tablet entries, view initialization, range rejection/replacement,
  parser all-or-nothing behavior, empty replica lists, unresolved original leader
  preservation, V2 preference/block encoding, error-response feedback, cache
  structural sharing, metadata reconciliation and absence of version ordering.
- Verified latency defaults, zero-scale rejection, zero samples/retry period,
  unchecked thresholds, strict comparison, stale minimum behavior, error-class
  measurement policy, and stable fallback penalty partitioning.
- Verified single-target lookup variants and empty fallback, address-translation
  limitation, host filtering and port-range/shard parsing boundaries.

## Test-claim and artifact validation

- A static check resolved all **57** pinned source-link paths/line bounds and all
  **66** path-qualified test-function references in the updated specification.
  This validates reference existence, not execution or exhaustive coverage.
- Inspected `test_locator`'s direct calls to the length, filtered-choice, `nth`
  and large-`nth` helpers. The large-index regression checks the plain vnode
  variant; the tablet test separately checks masked traversal. Multiword masks
  are exercised by `masked_sharded_replica_set_spans_words`. Choice assertions
  establish sampled membership, not a statistical uniformity guarantee.
- Inspected hash fixed vectors/chunk splits, precomputation/ring vectors, shard
  limits, tablet parser/mask/maintenance tests, default-plan sampling harness,
  zero-scale panic and stopped-clock average test. Existing distinctions between
  fixed examples and exhaustive equivalence remain explicit.
- Checked integration assertion sites in `load_balancing/token_awareness.rs`,
  `simple_strategy.rs`, `shards.rs`, `lwt_optimisation.rs`, `tablets.rs`,
  `latency_awareness.rs`, `ccm/tablets_routing_v2.rs`, `statements/prepared.rs`
  and `session/pager.rs`. Confirmed the described tablet/V2/view/CDC guards,
  tracing node-count limits, sampled host/shard claims, and the invalid-query
  latency integration test's limited assertion.
- No relevant claim was promoted to local test success. Existing concurrency,
  malformed-feedback, unavailable-metadata and Go migration gaps remain.
  Added explicit missing regressions for repeatable hasher finalization followed
  by writes, no-index prepared key materialization, optional Python input
  construction and zero-update-rate/updater teardown.

Result: five evidence-backed omission fixes. No cross-section specification
conflict requiring another owner's edit was identified.
