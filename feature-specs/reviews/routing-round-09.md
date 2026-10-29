# Routing review, round 09

- Section: `feature-specs/routing.md`
- Round: 09, whole-section review with adversarial boundary-case focus
- Agent identity: `/root/review_routing_09`
- Reference checkout: `/extra/scylladb/gocql-rust-reference-f8b0ee9`
- Source commit, verified with `git rev-parse HEAD`: `f8b0ee928dc0dcb475247dc82cce4e42804b0621`
- Before SHA-256: `3eae061a345059f7f4c53de8d65b8fedf5bb597c474a742fb9389565756d6a16`
- After SHA-256: `13c2b3f38c408722833359928d9b78753bae1f2b3e409c953ffaadd07c8938ab`

Read `feature-specs/reviews/REVIEW-INSTRUCTIONS.md`, `feature-specs/README.md`,
and the complete routing specification. Independently followed the implementation
and test sources below. Only the assigned specification and this report were
edited; no implementation changes or child agents were used.

## Verified findings and fixes

### 1. Null or omitted replica lists can produce accepted empty mappings

The specification described accepted empty lists but did not identify the
additional encodings accepted by the underlying deserializers.

Evidence: `scylla/src/routing/locator/tablets.rs:258-330` delegates V1/V2 payloads
to tuple deserialization, then collects the list without requiring a nonempty
result. `scylla-cql-core/src/deserialize/value.rs:1010-1041` makes a null
`ListlikeIterator` empty. Its tuple implementation at lines 1729-1770 treats an
omitted trailing field as null. Consequently, a V1 payload containing only its
two valid range endpoints has an empty replica list; a V2 null list is also
accepted when followed by the required non-null bigint version. V2's missing
version does not receive this exemption. `TableTablets::add_tablet` at
`tablets.rs:780-800` replaces overlapping cached mappings even when the accepted
mapping has no replicas.

Fix: documented these accepted encodings and their empty-cache replacement
effect, marked them as implementation quirks, linked the deserializers, and
added their missing routing regressions to the test inventory and generation
notes.

### 2. All-or-nothing parsing applies to declared entries, not all payload bytes

The previous statement that a malformed replica tuple rejects the payload was
too broad, as was the test-gap description of a valid prefix followed by
malformed data.

Evidence: `RawTablet::build` at `tablets.rs:307-316` uses a fallible collection, so
any deserialization error or negative shard encountered among the declared
replicas rejects the complete update. However, tuple deserialization at
`scylla-cql-core/src/deserialize/value.rs:1729-1770` returns after the expected
fields without checking that the input is exhausted. The same implementation
serves both the outer payload and each replica tuple.
`FixedLengthBytesSequenceIterator` at lines 2094-2130 explicitly permits bytes
remaining after the declared number of elements. A malformed undeclared tail is
therefore not equivalent to a malformed declared replica.

Fix: restricted the rejection guarantee to errors in declared entries and
described ignored outer-tuple, replica-tuple, and list tails. Narrowed the test
claim to the malformed fixtures actually exercised. Added explicit missing
trailing-byte regressions. Reported the generic-deserializer implication to the
parent for cross-section review; no other section was edited.

### 3. Existing latency coverage includes exact threshold equality

The specification incorrectly listed equality at the exclusion threshold as
entirely missing.

Evidence: the test
`latency_aware_default_policy_stops_penalising_after_min_average_increases_enough_only_after_update_rate_elapses`
at `scylla/src/policies/load_balancing/default.rs:4277-4397` manually computes a
10 ms minimum, then changes node 3's average to exactly the exclusion threshold
times that cached minimum. Before the next manual tick it expects node 3 in the
unpenalized local group, while node 1 remains after the remote group. A `>=`
comparison would violate that expected grouping. The final explicit tick then
makes node 1 eligible again. The setup helper at lines 3910-3938 changes host
statistics without updating the cached minimum.

Fix: replaced the incorrect missing-coverage entry with this concrete assertion
and its limitation: the test manually ticks the updater and does not establish
real periodic timer scheduling despite its name.

## Implementation inspected and behaviors checked

All paths in this section are relative to the pinned Rust checkout. Line ranges
identify the main inspected code; the review also followed nearby callers and
test assertions.

| Files and symbols | Checks |
| --- | --- |
| `scylla/src/policies/load_balancing/mod.rs:28-147`, `RoutingInfo`, `should_route_as_lwt`, optional interop constructor | Initial consistency is retained; confirmed LWT or regular SERIAL/LOCAL_SERIAL classifies LWT; serial consistency alone does not. Python interop directly stores fields. |
| `scylla/src/policies/load_balancing/default.rs:146-600,605-1135`, `pick`, `fallback`, replica selection, leader selection, builder | Token/table/keyspace requirements, token-awareness disablement, location overrides, all fallback groups, connected versus enabled status, host/shard deduplication, random rotations, fixed replica seed, no consistency guard on explicit DC failover, leader location and latency eligibility. Global LWT first-choice rejection defers to fallback. |
| `scylla/src/policies/load_balancing/default.rs:3270-3874`, `TimestampedAverage`, `LatencyAwareness`, `MinAvgUpdater`, `fast_enough` | Zero-duration samples, weighting, microsecond comparison, strict threshold, stale minima, retry expiry, zero minimum/retry/scale/update-rate behavior, unchecked non-finite thresholds, error classification, host-ID statistics, eager stable fallback partitioning and updater lifetime. |
| `scylla/src/policies/load_balancing/plan.rs:81-177`, `Plan`; `single_target.rs:16-110` | Lazy first choice, fallback even after no cheap choice, exact raw-target filtering, random missing-shard fill, direct node/ID/address targeting and empty explicit-policy fallback. |
| `scylla/src/routing/mod.rs:39-55`; `routing/sharding.rs:29-310`; `routing/partitioner.rs:36-423` | Token normalization versus direct string parsing, Murmur3 signed-byte tails and incremental finalization, CDC first-eight-byte/short-input behavior, suffix dispatch, sealed hashers, component framing, shard arithmetic, nonzero 16-bit counts, advertised-ID checks, unchecked ignored-bit range, empty source-port candidate sets. |
| `scylla/src/statement/prepared.rs:310-433,790-877`; `client/session.rs:1327-1380,1681-1754,1841-1863,2018-2039,2095-2145`; `client/caching_session.rs:100-245` | PK index ordering, null/unset filtering, 65,535-byte composite limit, no-PK materialization versus token calculation, first batch statement routing and any prepared LWT marker, cached preparation, plain unprepared versus automatic-paging dispatch, suppressed local hint errors, retained partitioner and prepared inputs. |
| `scylla/src/routing/locator/token_ring.rs:20-70`; `replication_info.rs:112-210`; `precomputed_replicas.rs:58-190`; `locator/mod.rs:114-305,362-590,837-1010` | Exact/next token and wrapping, empty ring, unique-node replica caps, SimpleStrategy filtering, NTS rack-repeat budget and zero factors, precomputed prefixes, local/unknown strategy fallback, tablet override, direct inspection, random predicate selection, ordinary versus ordered iteration, global ordered NTS zero-factor primary quirk. |
| `scylla/src/routing/locator/tablets.rs:208-565,720-1048`; `scylla-cql-core/src/deserialize/value.rs:945-1078,1729-1770,2094-2138` | Range validation and inclusive conversion; V2 preference without parse fallback; negative versus oversized nonnegative shard handling; findings 1–2; overlap replacement; unresolved original leader handling; V1 leader interpretation; removal/re-resolution/recreated nodes; table/view maintenance and structural sharing. |
| `scylla/src/cluster/state.rs:568-581,618-735,846-978`; `cluster/worker.rs:303-355` | Tablet keyspaces excluded from vnode precomputation, metadata-dependent token APIs, supplied-token lookup errors, optional preserialized helpers, same-batch deduplication, no version monotonicity or schema-generation fence, serial feedback/metadata publication. |
| `scylla/src/network/connection.rs:763-820,1105-1220,1252-1355,2045-2070`; `scylla-cql-core/src/frame/protocol_features.rs:64-146`; `scylla-cql/src/frame/request/execute.rs:81-108` | Local prepare defaults and result-only repair; feedback before database-error interpretation and on repreparation; awaited feedback send; native BATCH exclusion; V2-over-V1 STARTUP negotiation; per-connection EXECUTE block emission. |
| `scylla/src/client/execution.rs:73-112,445-515,547-693`; `client/pager.rs:340-374,974-1023`; `policies/speculative_execution.rs:187-239` | Shared speculative iterator, same/next-target retry behavior, connection failures skipping policy callbacks, callback timing, last error/empty plan, cancellation by dropping attempts, fixed pager snapshot and hint with fresh per-page plans. |
| `scylla/src/network/connection_pool.rs:266-275,316-425`; `scylla/src/policies/host_filter.rs` | Live liveness/sharder observations, shard hints relaxed on the same node, oversize shard conversion, host-filter distinction from location preference, allowlist treatment of untranslatable addresses. |

The language-independent descriptions, subfeature list, invariants, Rust evidence
table, implementation exceptions, all test-place categories, and generation
notes were checked against those paths. The retained Go test/benchmark entries
remain pending requirements, not claims about the existing Go implementation.

## Test coverage and validation

Read the partitioner vectors/chunk-splitting tests, prepared-key reordering test,
token-ring vectors, on-demand replica vectors, tablet parse/version tests and
leader-resolution assertions. Checked the default-policy grouping harness and
leader-down fixtures, the latency setup/updater tests, and the named replica
iterator/maintenance test locations. Inspected integration assertions and guards
in `load_balancing/{token_awareness,simple_strategy,shards,lwt_optimisation,tablets,latency_awareness}.rs`,
`statements/prepared.rs`, `session/pager.rs`, and `ccm/tablets_routing_v2.rs`.
The described sampled membership/node-count assertions, feature skips, and
finite convergence observations remain narrower than exhaustive routing proofs.

A local source-reference check validated all 78 pinned file/line anchors and all
67 test/helper symbol references in the updated specification. `git diff --check`
reported no errors; because the owned files are currently untracked, a separate
direct content check also verified final newlines and absence of trailing
whitespace in both files. These are documentation/source
checks; no Rust, Go, database, proxy, or benchmark tests were executed.

Remaining gaps include explicit tablet null/omitted-list and trailing-byte
regressions, malformed declared replica tails, empty-mapping replacement,
zero-factor global ordered NTS, malformed sharding shifts, leader plus latency
interactions, asynchronous feedback cancellation/publication, live pool changes,
and the Go implementation/test/benchmark work already marked missing. No new
behavior was inferred from a test run, and no unresolved finding requires an
implementation edit in this review's scope.
