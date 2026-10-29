# Routing review — round 05

- Section: `feature-specs/routing.md`
- Round: 05 — test assertions, guards, and coverage mappings; whole-section audit
- Agent identity: `/root/review_routing_05`
- Reference checkout: `/extra/scylladb/gocql-rust-reference-f8b0ee9`
- Verified source commit: `f8b0ee928dc0dcb475247dc82cce4e42804b0621`
- Before SHA-256: `80e3282e7499ff63c0dc54a61dd809249bf21b780f0993d51c773886a3737a06`
- After SHA-256: `5ef2ce2902280f536cac83e31165fc2e42568984c3a2b5db16e06140904318a5`

Read `feature-specs/reviews/REVIEW-INSTRUCTIONS.md`, `feature-specs/README.md`,
and the entire routing specification. Reviewed the pinned implementation and
test bodies directly; earlier reports were not used as proof. Only the assigned
specification and this report were edited. No Go or Rust code was changed, and no
unit, integration, database, or benchmark tests were executed.

## Source inspected

Paths below are relative to the reference checkout. Line numbers identify the
inspected implementations or test entry points, not claims of test execution.

| Files | Key symbols and inspected behavior |
| --- | --- |
| `scylla/src/policies/load_balancing/mod.rs:25`, `plan.rs:77`, `single_target.rs:12` | `RoutingInfo`, `should_route_as_lwt`, policy callbacks, `Plan::next`, random shard completion, exact first-target filtering, node/ID/address selection |
| `scylla/src/policies/load_balancing/default.rs:145,333,613,703,941,1049,1250` | `pick`, `fallback`, routing metadata gate, location groups, replica order/shuffling, leader eligibility, failover, builder defaults |
| `scylla/src/policies/load_balancing/default.rs:1538,1599,1640,1674,2155,2770,2950,3055,3103,3175` | Policy fixtures, 256-plan assertion helper, ordinary/LWT/location/leader test matrices, simulated connectivity and enabled-but-disconnected leader |
| `scylla/src/policies/load_balancing/default.rs:3270,3489,3557,3688,3849,3989,4008,4068,4128,4192,4598` | Latency average, partitioning, error classification, defaults, strict threshold predicate, negative/positive penalty fixtures, zero-scale panic |
| `scylla/src/policies/host_filter.rs:15` | All/allowlist/datacenter filters, address translation distinction |
| `scylla/src/routing/mod.rs:20`, `partitioner.rs:24,163,345,397,440,458,517` | Token normalization, Murmur3 signed tails and streaming, CDC prefix/short input, serialized-key framing, fixed vectors and seeded chunk tests |
| `scylla/src/statement/prepared.rs:307,373,412,781,910` | Token-awareness, extraction errors, first-column table identity, key-order extraction/framing, shuffled-column assertion |
| `scylla/src/routing/sharding.rs:15,77,119,136,205,278,351,379,425,484,500` | Source-port range, raw token parsing, arithmetic, candidate generation, advertised-shard parser, exact shard/port tests |
| `scylla/src/routing/locator/token_ring.rs:16,78`, `replication_info.rs:63,117,130,216,259`, `precomputed_replicas.rs:78,230,290` | Ring lookup, unique-node/rack traversal, zero/oversized RF, precomputed RF prefixes and above-rack lists, fixed expected vectors |
| `scylla/src/routing/locator/mod.rs:114,782,1030,1073`, `test.rs:224,799` | Strategy/tablet dispatch, global ordered NTS primary, word-spanning masks, ordered vnode tests, tablet order/shard/DC/range tests |
| `scylla/src/routing/locator/tablets.rs:179,285,477,698,801,925` | Payload preference/validation, nibble encoding, leader resolution, overlap replacement, unknown/stale-node repair and schema maintenance |
| `scylla/src/routing/locator/tablets.rs:1083,1089,1101,1152,1180,1235,1272,1283,1331,1671,1694,1732,1767,1799,2166,2280,2296,2310` | Parser, sampled version-block, cache-transition, maintenance, and resolved/unresolved-leader assertions |
| `scylla/src/cluster/state.rs:613,837,897,1221,1381`, `cluster/worker.rs:302` | Public token/endpoints APIs, interop gate, lazy update/no-op detection, unknown-keyspace tests, update-builder tests, conditional publication |
| `scylla/src/client/session.rs:73,1327,1681,1841,2018,2095`, `client/pager.rs:134,340,974` | Batch routing, native QUERY versus local prepare/EXECUTE, automatic session preparation, token-error handling, pager snapshot/hint lifetime |
| `scylla/src/client/execution.rs:61,99,445,547`, `policies/speculative_execution.rs:187` | Hint choice, shared iterator mutex, callback/connection/retry ordering, speculative completion |
| `scylla/src/network/connection.rs:1113,1252,2045`, `network/connection_pool.rs:266` | Negotiated hint emission, feedback before error/reprepare handling, BATCH path, awaited channel send, live pool liveness |
| `scylla-cql-core/src/frame/protocol_features.rs:62,110,161` | V1/V2 parsing and STARTUP preference plus direct negotiation assertions |
| `scylla/tests/integration/load_balancing/{token_awareness,simple_strategy,shards,lwt_optimisation,latency_awareness}.rs` | Actual trace/row/node/shard/error assertions and LWT feature branch |
| `scylla/tests/integration/load_balancing/tablets.rs:249,596,713,830,911,1014,1068,1194,1214` | Warm-up evidence, endpoint comparison, query matrix, view/CDC refresh, full-pool prerequisite, feature guards |
| `scylla/tests/integration/ccm/tablets_routing_v2.rs:82,127,253,296,409,572,693,851` | Startup/feature skips, frame decoding, V2 convergence, mixed negotiation, independently decoded leader host, V1 cached-host checks |
| `scylla/tests/integration/statements/prepared.rs:39,284,361`, `session/pager.rs:898` | Server token comparisons, permuted key binds, CDC partitioner/cache tests and Cassandra ignore, token-unaware pager assertions |

## Verified findings and fixes

1. **The ordered-tablet test was mapped to a vnode-only test.**
   `locator/mod.rs:test_replicas_ordered` constructs vnode strategy cases and
   compares node order; it has no tablet fixture. The tablet ordered iterator
   assertion is in `locator/test.rs:test_tablet_replica_sets` at lines 850–863.
   Split these mappings and retained the separate multiword-mask regression.

2. **Several coverage descriptions exceeded what the fixtures assert.**
   The default-policy helper at `default.rs:1599` collects 256 complete `Plan`s;
   it does not independently compare all first-choice and fallback results.
   Most fixtures replace pool connectivity with enabled status, while the
   leader-down fixture intentionally leaves one enabled node disconnected.
   Hash chunking at `partitioner.rs:458` samples four fixed inputs with seeded
   four-chunk splits, and tablet block tests at `tablets.rs:1235,1272` sample
   randomness rather than force all indices. The snapshot test at
   `state.rs:1381` asserts the update builder returns `None`, with publication
   belonging to the worker. Reworded these claims to match those boundaries.

3. **Feature and environment guards were missing from the integration map.**
   Tablet tests return early without tablet support (`tablets.rs:1040`);
   LWT/view/CDC branches have additional named-feature guards at
   `1158,1197,1217`. The CCM helper returns early on any cluster-start failure
   at `tablets_routing_v2.rs:105`, after requesting the experimental
   strong-consistency feature, and optionally returns early without V2 at
   `144`. All four tests additionally check tablet support. Added the guards
   and explicitly distinguished the V1 test's `false` V2 requirement. A test
   process passing is not evidence that guarded assertions executed.

4. **Routing integration expectations needed to distinguish their actual
   observables.** `token_awareness.rs:52,63` asserts one traced node for RF=1
   prepared writes, including `execute_iter`; `simple_strategy.rs:71` asserts
   rows. `shards.rs:71` checks only nonempty per-node captured shard sets and
   `:102` sends hint 10,000 without asserting the advertised count. The LWT
   test at `lwt_optimisation.rs:104–122` checks one/multiple contacted nodes
   depending on the offered metadata mark, including a later-LWT batch at
   `193–201`. The CCM leader test compares host IDs at `811`, not shards;
   V1 checks measured hosts against the driver's cached replicas at `958–969`.
   Updated each description, including what is not established by these tests.

5. **Existing direct coverage was omitted.** Added the shuffled prepared-key
   extraction test (`prepared.rs:910`), CDC fixed vectors (`partitioner.rs:517`),
   on-demand/precomputed strategy vector tests, and the V1/V2 STARTUP tests
   (`protocol_features.rs:161–201`). Added integration server-token comparisons
   for simple/composite/permuted keys, the explicit Murmur3/CDC prepared-cache
   test with its Cassandra ignore, and the token-unaware pager regression.
   The latter covers absent key metadata, so the separate gap for actual
   token-calculation errors and automatic implicit-preparation routing remains.

6. **The latency integration test and boundary coverage needed explicit limits.**
   `latency_awareness.rs:8–25` sends `whatever` and calls `unwrap_err`; it does
   not establish successful execution or a penalty decision. The below/above
   threshold fixtures at `default.rs:4068,4192` use 0.95 and 1.05 multiples;
   exact equality is not tested. The positive fixture ticks its updater, while
   several negative fixtures use the background updater. Replaced the vague
   integration claim, named absent-statistics/below-threshold coverage, and
   recorded equality as missing.

7. **Parser/key boundary gaps were not explicit.** Source enforces the prepared
   indexed-value error and composite `u16` limit (`prepared.rs:810,846`), skips
   null/unset values before choosing prepared-key framing (`:831`), rejects
   negative payload shards (`tablets.rs:299`), and does not retry malformed V2
   as V1 (`:237`). The reviewed tests and error-symbol searches did not expose
   explicit regressions for those boundaries. Added missing-coverage entries
   rather than assigning those assertions to generic malformed-payload or
   token-awareness tests.

## Whole-section behaviors checked without additional contract edits

- Routing needs token, table identity, and known keyspace strategy; disabled
  token awareness also bypasses leader selection. Batch first-statement token
  routing and any-prepared-statement LWT marking agree with callers.
- Unprepared unpaged/manual requests preserve token-unaware plans while local
  preparation can provide EXECUTE hints/feedback; automatic paging with values
  uses session preparation. Prepared key errors propagate, while local optional
  hint calculation suppresses them. CDC partitioner assignment is session-level.
- Plan advancement is lazy and shared across speculative fibers; pool/sharder
  observations remain live. Connection acquisition failure bypasses attempt
  callbacks and retry decisions; returned attempt failures report before retry.
  Pager plans are fresh per page against one retained snapshot and reuse one
  prepared tablet hint.
- Default ordering, permitted remote replica priority, random node rotations,
  enabled-node fallback, exact versus mixed-shard deduplication, LWT ordering,
  fast-primary deferral, leader consistency/location checks, and custom-policy
  freedom match the specification. The global ordered-NTS zero-factor quirk
  remains explicitly documented.
- Token normalization versus string parsing, signed-byte Murmur3 tails, CDC's
  eight-byte prefix and short-input sentinel, composite key framing, ring
  wrapping, SimpleStrategy/DC filtering, NTS rack-repeat budget, unknown-strategy
  fallback, and precomputation boundaries agree with source.
- Tablet payload all-or-nothing parsing, empty-list acceptance, V2 preference,
  nibble encoding, overlap removal, original-leader identity, unresolved-node
  maintenance, view retention, and structural sharing agree with source.
  EXECUTE feedback precedes error interpretation; QUERY/BATCH do not call the
  feedback helper. Enqueue, publication, and cancellation remain distinct, and
  there is no schema-generation or monotonic-version fence.
- Shards remain advisory, source-port iteration/draw have their documented empty
  candidate behavior, single-target lookup has no default-policy filters, and
  latency/host filters preserve their documented scope. Latency thresholds are
  strict, history does not expire with the global minimum, and failure classes
  are explicit.
- Public token calculation requires base-table metadata, supplied-token endpoint
  lookup requires keyspace metadata, and the gated preserialized interop path
  remains distinct. Go unit/race/integration/allocation coverage stays pending.

## Validation and remaining limits

- Confirmed checkout `HEAD` equals the pinned source commit.
- Checked all 65 fully qualified mapped test-function references and 51 pinned
  implementation link anchors in the revised document: files/functions exist,
  and anchor lines are within their files. Contextual shorthand names were also
  inspected in their identified source files.
- Captured the before/after SHA-256 values above. Documentation whitespace checks
  passed; these checks are not a Rust test run.
- Existing missing regressions for concurrency/cancellation, live pool changes,
  automatic-pager snapshot/hint reuse, failed EXECUTE feedback, native BATCH
  feedback exclusion, interop/view edge cases, and ordered-NTS zero factors
  remain explicit. Added key/parser/exact-latency boundary gaps; no missing
  database coverage was inferred from unit fixtures or legacy Go behavior.
- Finite random sampling and migration retries do not prove a deterministic
  convergence deadline. Some negative latency tests use background timing;
  they are test targets, not an exhaustive proof of every prerequisite in
  isolation. Runtime/database behavior remains unverified in this round.
