# Execution review round 04

- Section: `feature-specs/execution.md`.
- Round: 04 — whole-section audit, with additional concurrency, cancellation,
  snapshot, lifetime, and race review.
- Agent: `/root/review_execution_04`.
- Reference checkout: `/extra/scylladb/gocql-rust-reference-f8b0ee9`.
- Reference commit verified with `git rev-parse HEAD`:
  `f8b0ee928dc0dcb475247dc82cce4e42804b0621`.
- Before SHA-256:
  `8f16cfc5d8bdb66f1723951645c153edabc06c1a418680a654882419dfac8649`.
- After SHA-256:
  `07c68d22e3f229ab3de9ad5a269fd5e727a7c5679b166e1706c1e829fb6ad387`.
- Scope: only the assigned specification and this report were edited. No Go or
  Rust implementation edits, child agents, or database/test runs were performed.

Read `feature-specs/README.md`, the complete assigned specification, and the review
instructions. Findings below come from the pinned source, not prior review reports.
Line references below are relative to that Rust checkout.

## Source inspected

| File | Key symbols and regions inspected |
| --- | --- |
| `scylla/src/statement/mod.rs` | `StatementConfig`, default page size, `PageSize::new`, lines 25–104 |
| `scylla/src/statement/unprepared.rs` | `Statement::new`, positive page-size setter, lines 24–78 |
| `scylla/src/statement/prepared.rs` | prepared response conversion; shared data and clone, lines 90–280; current metadata/getter guard, lines 568–603; serialization and configured/unconfigured handles, lines 677–725 |
| `scylla/src/statement/bound.rs` | `BoundStatement`, serialization ownership, token calculation and delegated settings, lines 19–95 |
| `scylla/src/statement/batch.rs` | `peek_first_token`, `BatchValuesFirstSerialized`, lines 295–405 |
| `scylla/src/client/execution_profile.rs` | defaults, lines 178–212; immutable profile and shared policy fields, builder cloning, independently created handles, `access` and remapping, lines 400–527 |
| `scylla/src/client/session.rs` | public serialization call sites; batch count/routing, lines 1294–1382; unprepared modes, lines 1604–1745; agreement handling and query pager preparation, lines 1822–1864; preparation passes, lines 1934–2039; execute resolution/routing, lines 2085–2185; `prepare_batch`, lines 2258–2275; tracing, lines 2380–2392 and 2427–2481; common success side effects, lines 2496–2531; configuration defaults around lines 482–514 |
| `scylla/src/client/execution.rs` | `SharedPlan`, resolved parameters, target abstraction, retry-session creation, metrics and history, lines 99–419; timeout/speculative runner and full attempt loop, lines 431–672 |
| `scylla/src/client/caching_session.rs` | constructors and execution entry points, lines 49–161; concurrent batch preparation and lookup/eviction/insert, lines 171–255; builder defaults/capacity, lines 259–340; `test_parameters_caching`, lines 629–705, and zero-capacity test locations |
| `scylla/src/client/pager.rs` | captured state/configuration, lines 134–190; background loop and per-page runner, lines 202–373; response classification, lines 375–498; fixed-connection executor, lines 528–690; raw polling and typed stream construction, lines 714–824; session pager creation/spawn and hint capture, lines 827–1110; first-page side effects, lines 1192–1243; typed stream polling, lines 1280–1367 |
| `scylla/src/network/connection.rs` | router submission/orphan notifier, lines 138–225; reprepare, lines 773–820; metadata update/parameters and execute/resend, lines 996–1222; batch recovery/implicit preparation, lines 1252–1369; response parsing and cached metadata handoff, lines 1459–1517 |
| `scylla/src/cluster/node.rs`, `scylla/src/network/connection_pool.rs` | retained node/pool ownership and live connection selection, node lines 83–112 and 213–228, pool lines 336–408 |
| `scylla/src/policies/retry/{default,downgrading_consistency,fallthrough}.rs` | complete decision branches, policy/session budgets, serial-consistency cases, and test entry points |
| `scylla/src/policies/speculative_execution.rs` | fixed/percentile policies, error classification, timer and future ownership, lines 28–239; all three scheduler tests, lines 272–405 |
| `scylla-cql-core/src/frame/response/error.rs` | `DbError::can_speculative_retry`, lines 451–487 |
| `scylla/src/policies/timestamp_generator.rs` | timestamp generation and warning configuration, lines 15–157; sequential/concurrent tests, lines 160–211 |
| `scylla/src/response/{query_result,request_response}.rs` | result ownership and row/non-row conversion, lazy typed access/cardinality and unstable gates, query-result lines 73–230 and 322–424; response normalization, lines 65–142 |
| `scylla-cql/src/frame/response/result.rs` | retained cached metadata versus response-owned metadata and row storage, lines 878–932 and 973–980 |

Test-source inspection additionally covered all 75 named test references for
existence, assertion excerpts across the integration inventory, and focused
bodies described in the coverage section below.

## Verified findings and fixes

1. **Reprepare's non-destructive metadata check was overstated as a concurrent
   guarantee.** `connection.rs:797–817` loads one current metadata snapshot,
   checks its column count and ID, then calls an unconditional store at
   `prepared.rs:576–580`. There is no compare-and-swap tying the store to the
   checked value. A permitted interleaving is: reprepare reads empty metadata
   with ID A; a concurrent execution stores nonempty metadata; reprepare stores
   an empty replacement with different ID B. The former wording said reprepare
   must not replace useful metadata, which does not hold across that race.
   The spec now scopes the guard to the loaded snapshot and states the race.
   It also replaces response-arrival ordering with actual store ordering:
   `connection.rs:1009–1027` can reinstall even the old snapshot used to decode a
   response after another execution updated the cache. No schema-version order
   is enforced. Added an explicit missing concurrency test.

2. **Metadata getter lifetime was omitted.** `ColumnSpecsGuard` owns an ArcSwap
   guard (`prepared.rs:234–244`), and
   `get_current_result_set_col_specs` loads it once (`598–603`). This retains a
   stable snapshot while concurrent cache updates proceed. The spec now states
   that keeping the guard retains its metadata and that observing a later
   snapshot requires another getter call. The response path separately retains
   the attempt metadata in `result.rs:881–932`, so cache updates do not rewrite
   metadata already selected for a result.

3. **Profile and pager snapshots needed exact timing and scope.** Handle access
   is an atomic `load_full` (`execution_profile.rs:509–510`) inside asynchronous
   session execution (`session.rs:1664–1667`, `2100–2103`) or pager construction
   (`pager.rs:150–190`). Merely constructing an unpolled Rust future does not
   perform that load. For value-bearing query pagers, preparation and serialization
   precede construction (`session.rs:1848–1854`). Shared policy objects are Arc
   fields, and fresh pages call those same policy objects; their feedback and
   metrics are not frozen. Likewise `Node::connection_for_shard` selects from a
   live pool even when a pager retains an old `ClusterState`. The specification
   now distinguishes the captured object/configuration from mutable state behind
   it, while retaining the existing fixed-cluster and fixed-tablet-hint contract.

4. **Cache capacity and equal-text misses lacked the retained-handle boundary.**
   `caching_session.rs:213–243` independently prepares every miss, inserts an
   unconfigured handle, and returns that miss's own prepared object. A racing
   insertion does not make the earlier caller return the winning map entry.
   `prepared.rs:258–265` allocates a new shared metadata object for each explicit
   preparation; `692–725` shares it only with handles derived from that object.
   Eviction/removal therefore does not invalidate retained prepared handles, and
   equal-text concurrent fills can leave distinct mutable metadata caches alive.
   The spec now states these consequences and distinguishes map entry capacity
   from memory held by active callers/pagers. Added a missing lifetime test.

5. **Caller cancellation and speculative cancellation lacked their feedback
   boundaries.** Core request completion history is emitted after awaiting the
   runner (`execution.rs:514–535`), and attempt completion feedback is emitted
   only after the attempt future returns (`594–645`). There is no completion
   guard on these records. Dropping a caller future or losing branch can leave
   start records without completion; caller drop alone does not execute the
   timeout-counting branch. The transport owns the serialized submitted request
   and separately reports orphaning on future drop (`connection.rs:144–174`,
   `220–224`); no server cancellation is sent. Speculation owns ordinary futures
   in `FuturesUnordered` and returns on a decisive result
   (`speculative_execution.rs:198–235`). Connection reprepare/metadata/tablet work
   already completed before that return is not undone, while session
   keyspace/schema handling applies only after a winning outcome
   (`session.rs:2511–2528`). These boundaries are now explicit, with a missing
   feedback/cancellation test.

6. **Detached pager work can produce observations after drop.** The existing
   capacity-one and delayed closure-observation description is correct:
   `pager.rs:217–251` fetches before attempting a send and checks closure there;
   constructors discard the worker join handle (`919`, prepared constructor
   after `1107–1110`). The executor owns policy, metrics, history, cluster, and
   request state (`134–190`). Added the omitted consequence that callbacks,
   metrics, and connection feedback can continue after dropping the pager.
   The internal fixed-connection pager has the same fetch-before-send boundary
   (`646–690`); its broader context remains delegated to the metadata section.

7. **The profile-remapping test citation described the wrong test.**
   `statements/execution_profiles.rs:test_execution_profiles` selects and clears
   statement handles, verifies load-balancing/retry identity and consistency
   overrides, and never calls `map_to_another_profile` (lines 124–338).
   Sequential remapping is actually exercised in
   `statements/request_timeout.rs:84–132`, including the session's cloned default
   handle at line 94. Corrected both inventory descriptions and explicitly left
   concurrent remapping and pager snapshot behavior as missing coverage.

## Checked behaviors retained without correction

- Three explicit paging choices; positive page size and 5,000 default; unpaged
  continuation rejection after the core request; supported-server SELECT without
  FROM behavior and its integration target.
- Statement/profile precedence, nested serial-consistency inheritance/omission,
  default timeout/retry/load balancing, idempotence gate and tracing/timestamp
  defaults. A profile can disable its deadline; an unset statement timeout inherits.
- Empty-value QUERY versus nonempty-value local PREPARE/EXECUTE; no rerouting
  after local preparation; full preparation before a value-bearing query pager.
- Explicit preparation's one-per-node pass, all-attempt wait, success-ID agreement,
  first-success metadata, trace aggregation, all-shards fallback/error replacement,
  cluster-derived partitioner, and absence of the execution timeout/retry loop.
- Binding ownership; prepared serialization and token validation before the core
  timeout; values reused by retries/pages; shared prepared metadata and clone-local
  options/trace-list behavior.
- Same-connection one-resend execute repair with preserved request parameters;
  batch repair loops, missing-ID failure, and no update of bind metadata.
- Metadata-ID negotiation, mixed-version empty ID, empty-metadata requests,
  legacy opt-in staleness, row-response same-ID empty-cache exception, and
  immutable metadata retained by each result.
- Batch count limit, first-member routing/serialization cache, any-prepared-member
  LWT flag, sequential deduplicated implicit preparation versus concurrent explicit
  preparation, and partial-failure effects without rollback.
- Cache query-text-only keys, current-caller option reconstruction, default/zero
  capacity rules, non-LRU eviction, non-coalesced misses, and non-atomic capacity.
- Pool failures skip attempt accounting/retry policy; acquired-connection attempts
  start before local work; retry sessions are lazy and branch-local; consistency
  downgrades affect later attempts but not original plan routing information.
- Default and downgrading policy budgets/conditions, regular serial-consistency
  behavior, ignored-write success, fallthrough behavior, and lack of a universal
  numeric retry bound.
- Speculative count/interval snapshot per request, shared plan iteration,
  independent retry/consistency state, transient-versus-terminal classification,
  timer-delayed launches, exhaustion handling, and final error selection.
- Per-page plan/deadline/retry/history state; previous successful coordinator
  preference; same page input across retries; terminal fetch error after queued
  rows; first-page side effects; type checking and row-consumption boundaries;
  owned typed stream versus borrowed manual results; consumed-page trace/coordinator
  lists and discarded pager warnings.
- Result cardinality helpers and lazy decoding; generated timestamp per outer
  attempt and retained timestamp for internal resends; atomic per-generator
  monotonicity; post-core schema/keyspace effects; trace-fetch concurrency,
  incompleteness retries and defaults; conditional wrapper-driver API gates.
- Invariants remain scoped to reference behavior. Go tests, integration migration,
  allocation work, and race tests remain explicit backlog rather than claims of
  an implemented or verified Go contract.

## Test coverage and validation

- A local source-reference check found all 75 named test symbols in their cited
  Rust files. This verifies references, not passing executions or complete coverage.
- Read the three speculative scheduler tests: their paused-clock assertions cover
  empty-plan termination with live fibers and all-ignorable-error termination,
  not cancellation feedback or actual concurrent metadata races.
- Read monotonic timestamp tests: concurrent test asserts increasing values within
  each thread and global uniqueness across ten threads using one shared generator.
- Read cache option-isolation assertions: the second same-text call is served from
  cache and its wire consistency, serial consistency, timestamp, and page size
  match that caller (`caching_session.rs:629–705`). This does not cover duplicate
  concurrent fills, eviction with retained handles, or instantaneous capacity.
- Read profile selection and timeout/remapping bodies as described in finding 7.
  No concurrent handle remapping or pager-remapping assertion was found there.
- Read metadata-ID integration entry points and protocol assertions: ordinary
  metadata refresh, mixed-extension behavior, and INSERT's empty ID are represented.
  These tests are conditional on negotiated extension support and do not establish
  the load/check/store interleavings documented in finding 1.
- Inspected pager timeout, consistency reset and fresh-plan tests and the relevant
  assertions in coordinator/error/owned-stream tests. They support those existing
  coverage descriptions, but do not prove immediate pager-drop cancellation or
  callback quiescence after drop; neither guarantee is now claimed.
- Checked assertion excerpts for the remaining statement/result/batch/recovery,
  tracing, agreement, history, and metrics test references. No new blanket claim
  of coverage was added. Existing missing markers for preparation failure ordering,
  batch partial effects, serialization reuse, legacy metadata staleness, malformed
  row continuation and other edge cases were retained.
- Added four explicit missing targets for profile snapshot races, metadata update
  interleavings/guards, cache handle lifetime, and cancellation feedback boundaries.
- `git diff --check -- feature-specs/execution.md` passed. SHA-256 was captured
  before and after edits. No Rust or Go tests were run; the metadata overwrite
  interleaving is a source-derived possibility, not a reproduced database failure.

Cross-section findings were sent to the parent: related profile/pager cancellation
and routing snapshot details are being reconciled by their respective owners.
This round did not edit their files. Remaining uncertainty is runtime incidence
and deterministic race reproduction, now represented by the explicit test gaps.
