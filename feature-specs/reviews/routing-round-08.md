# Routing review round 08

- Section: `feature-specs/routing.md`
- Round: 08, whole-section audit with additional caller/callee and cross-section consistency focus.
- Agent identity: `/root/review_routing_08`
- Reference checkout: `/extra/scylladb/gocql-rust-reference-f8b0ee9`
- Verified source commit: `f8b0ee928dc0dcb475247dc82cce4e42804b0621`
- Before SHA-256: `e36ca99f28606829fbe81ad48a42beca9c10e5fb2687d85fe7c32f2886f8f37d`
- After SHA-256: `3eae061a345059f7f4c53de8d65b8fedf5bb597c474a742fb9389565756d6a16`
- Scope: only this report and the assigned routing specification were edited. No implementation edits or child agents.

## Verified findings and fixes

1. **Plain-session unprepared routing did not identify the caching-wrapper exception.**
   `client/caching_session.rs:100-159` first obtains a prepared statement for all
   three execution modes and prepares unprepared batch members before invoking
   `Session::batch`. There is no empty-values bypass. `prepare_batch` at 171-188
   replaces query members with prepared members; session batch routing at
   `client/session.rs:1327-1356` then sees their table/token/LWT information.
   This differs from the connection-local preparation after plan construction at
   `client/session.rs:1681-1755`. Qualified the existing rule as plain-session
   behavior and added the wrapper's preparation-before-planning contract,
   including cache-hit preservation of the partitioner. This agrees with the
   execution section's caching-wrapper description.

2. **Prepared routing metadata lifetime was implicit across metadata refresh and reprepare.**
   `statement/prepared.rs:205-229` stores immutable prepared metadata and the LWT
   flag in shared data and copies the retained partitioner on clone;
   `get_table_spec` at 412-418 reads the original bind-column metadata.
   `network/connection.rs:773-822` checks the reprepared ID and may replace result
   metadata, but does not replace bind metadata, the LWT flag, or the partitioner.
   `client/session.rs:2095-2128` computes the token from that prepared object while
   obtaining a current execution snapshot separately. Cache handles preserve
   those inputs at `statement/prepared.rs:692-725`. Added the explicit lifetime
   boundary: a fresh placement snapshot can coexist with older prepared routing
   inputs. Added an explicit missing regression for refreshing/repreparing those
   inputs. The existing execution claim that bind metadata is immutable remains
   consistent.

3. **Session preparation's partitioner fallback omitted materialized views.**
   `client/session.rs:2018-2040` installs a partitioner only through
   `keyspaces[...].tables[...]`; it never searches the separate view map.
   `statement/prepared.rs:248-270` supplies the Murmur3 default. Thus a prepared
   view retains the default even if its view schema has an override. This differs
   from tablet cache maintenance, which deliberately includes both tables and
   views at `routing/locator/tablets.rs:986-1033`. Added this precise limitation
   beside the existing connection-local preparation limitation, a pinned source
   link, and a missing targeted regression. No assertion was added that any
   particular live server permits a nondefault view partitioner.

## Files and key symbols inspected

Unless a crate directory is explicitly included, implementation paths below are
relative to `scylla/src` in the pinned Rust checkout. These were source reads,
not deductions from prior review reports.

- `policies/load_balancing/mod.rs:28-227`: `RoutingInfo`, LWT classification,
  optional Python constructor, custom policy interface and callbacks.
- `policies/load_balancing/plan.rs:81-179`: lazy first choice, fallback on no
  choice, exact first-target filtering, optional-shard resolution.
- `policies/load_balancing/default.rs:146-635,703-1170,3270-3880`: first-choice and
  fallback groups, replica/node helpers, leader eligibility, defaults and
  preference overrides, latency measurement/update/partition/error classification.
- `policies/load_balancing/single_target.rs:16-109`: direct node/ID/address
  resolution and empty fallback. `policies/host_filter.rs:15-116`: built-in filters
  and synchronous allowlist resolution.
- `routing/mod.rs:18-57`, `routing/partitioner.rs:20-427`: normalization,
  partitioner suffix dispatch, sealed streaming traits, Murmur3 signed tail,
  CDC short input, and distinct serialized-key framing.
- `routing/sharding.rs:11-237,275-307`: shard arithmetic, source-port ranges and
  candidate enumeration, input parsing and its unchecked ignored-bit value.
- `routing/locator/token_ring.rs:15-78`, `replication_info.rs:65-208`,
  `precomputed_replicas.rs:80-205`, and `routing/locator/mod.rs:114-280,362-520,837-920`:
  ring traversal, rack-aware placement, precomputation, tablet override, direct
  inspection, filtered random selection, global ordered-NTS primary/continuation.
- `routing/locator/tablets.rs:208-380,498-535,745-856,986-1050`: version blocks,
  V1/V2 parsing, original-leader resolution, overlap replacement, unresolved-node
  repair, and table/view reconciliation.
- `statement/prepared.rs:205-418,692-725,791-870`: retained routing data,
  token/materialization APIs, cache handles, extraction and component framing.
- `client/session.rs:1300-1365,1681-1755,1841-1864,1988-2040,2095-2177`;
  `client/caching_session.rs:100-243`; `client/pager.rs:306-384,974-1025`:
  entry-point differences, batch construction, partitioner lookup, pager snapshot
  and coordinator stability, and lifetime of version hints.
- `client/execution.rs:73-115,547-680`: hint construction, shared speculative plan,
  connection failures, attempt callbacks, retries and exhaustion.
- `network/connection.rs:745-822,1105-1225,2045-2074` and
  `network/connection_pool.rs:266-322,336-430`: preparation/reprepare, feedback
  before database-error interpretation, negotiated version emission, live pool
  liveness/sharder observations, and advisory shard selection.
- `cluster/state.rs:618-745,846-893,898-982`; `cluster/worker.rs:303-335`;
  `cluster/metadata/fetching.rs:758-896`: public token/endpoint APIs, feedback
  deduplication and batching, optional preserialized APIs, and disabled/minimal/full
  metadata dependencies.
- `scylla-cql-core/src/frame/protocol_features.rs:20-153` and
  `scylla-cql/src/frame/request/execute.rs:65-109`: extension negotiation, LWT mask
  interpretation, and the trailing EXECUTE version block.

The local README and full routing section were read. Relevant passages of
`execution.md`, `connections.md`, `metadata-control-events.md`,
`serialization-protocol.md`, and `configuration-security-observability.md` were
compared with the caller/callee traces above. No verified correction to another
agent's file was required; the parent was notified of the three routing fixes.

## Checked behaviors without further findings

- Routing prerequisites, normal missing-PK handling, extraction errors, first
  batch member routing, whole-batch LWT marking, and ordinary versus automatic
  unprepared paging remain correct.
- Plans preserve initial routing consistency and metadata; shared speculative
  iteration does not serialize attempts. Connection-selection failure bypasses
  retry/callback handling, same-target retries reacquire connections, and
  cancellation can omit unfinished-attempt callbacks.
- Default locality/failover ordering, remote-replica precedence, randomized node
  rotations, final enabled-node ordering, exact Plan filtering versus the default
  fallback comparator, live-pool eligibility, and latency partition timing agree
  with the source.
- SERIAL/LOCAL_SERIAL regular consistency and prepared confirmation identify
  LWTs; optional serial consistency alone does not. Ordered NTS's zero-factor
  primary quirk and the lack of a V2-only leader guard remain explicitly recorded.
- Key framing, null/unset skipping, composite size checks, streaming hashes,
  token normalization exception, vnode strategies, tablet override, replica
  inspection, and precomputation boundaries remain supported.
- Tablet payload rejection, empty replica lists, unresolved leaders/followers,
  version probing, overlap replacement, serial batch updates, schema/name race,
  and snapshot sharing match the implementation. Feedback is EXECUTE-only and
  awaited through the bounded channel; native QUERY/BATCH do not warm the cache.
- Session location preference differs from host filtering. Single-target policy
  bypasses default eligibility, and all policy shard targets remain hints at
  connection selection. Oversized hints are narrowed to zero before pool lookup;
  representable out-of-range/empty shard hints follow the documented fallback.
- Latency defaults, zero-duration behavior, minimum-measurement and retry-period
  boundaries, strictly-greater comparison, microsecond precision, lack of sample
  expiry in minimum calculation, and error classification remain accurate.
- Disabled metadata yields no keyspaces; minimal metadata includes table/view
  names and partitioners. The latter supports prepared routing while full
  partition-key definitions are needed for normal schema-based serialization.

## Coverage and validation

- A source-target check resolved all 66 explicitly path-qualified named targets
  in routing's backticked test references to a function or module in the pinned
  checkout; none were missing. This establishes target existence only.
- Read the default-policy 256-plan helper and inspected representative source
  assertions/guards for token-awareness, prepared server-token comparison, cached
  CDC partitioner preservation, shard selection/out-of-range hints, tablet/view/CDC
  support gates, V2/strong-consistency startup skips, and latency smoke coverage.
  Existing descriptions correctly distinguish sampled placement assertions from
  exhaustive/probabilistic guarantees.
- In particular, `statements/prepared.rs:test_prepared_partitioner` checks CDC
  partitioners on both cache miss and cache hit, but does not test the complete
  cached/plain-session routing dispatch matrix. That missing matrix is now
  explicit, as are retained-routing-input and view-partitioner regressions.
- No Rust tests, Go tests, or live database scenarios were executed. No claim of
  runtime coverage follows from the source audit.
- Verified no trailing whitespace and no missing local Markdown link targets in
  the edited section. Confirmed the final SHA-256 above. The specification is
  currently untracked in this workspace, so an empty `git diff --check` result
  alone was not treated as content validation.

## Remaining gaps

The existing explicit gaps for malformed/invalid routing inputs, concurrency,
cancellation, metadata publication, guarded integration paths, and Go migration
remain. The three added boundaries have implementation evidence, with the
targeted tests noted above still missing. This round introduces no implementation
or test-run claim.
