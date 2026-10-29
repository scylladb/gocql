# Execution reference review — round 07

- Section: `feature-specs/execution.md`
- Round: `07` — exact source anchors and implementation-versus-contract wording
- Agent identity: `/root/review_execution_07`
- Source checkout: `/extra/scylladb/gocql-rust-reference-f8b0ee9`
- Source commit, checked with `git rev-parse HEAD`: `f8b0ee928dc0dcb475247dc82cce4e42804b0621`
- Before SHA-256: `1a52652e34fdd0edd2a17755de658fad128e04dea276c54c61b85210c65bc90f`
- After SHA-256: `521381fa37555bcd799be643cf89be94a65fb4fae03c73d364fb4a11a0166514`

Read `REVIEW-INSTRUCTIONS.md`, `feature-specs/README.md`, and the complete
execution specification. Independently inspected the pinned source; prior
round reports were not used as proof. Only the assigned specification and this
report were edited. No Go or Rust implementation changes were made.

## Findings and fixes

### 1. Several evidence anchors identified a nearby or different operation

All original source paths and line numbers existed, but existence was not enough
to substantiate their labels. In particular:

- The profile-defaults link landed on an import at
  `scylla/src/client/execution_profile.rs:178`; defaults start at line 182.
- The handle-snapshot link selected `pointee_to_builder` at line 514, whereas
  `to_profile` at line 519 is the public profile-snapshot operation.
- The copied-profile builder claim pointed to the blank `builder` at line 434;
  `to_builder` at line 446 copies settings. The adjacent label/handle code was
  also inspected.
- The resolution anchor was inside retry-policy selection at
  `scylla/src/client/execution.rs:165`; the complete resolver starts at line 150.
- The column-lookup link pointed to `get_by_index` at
  `scylla/src/response/query_result.rs:43`; exact name matching and first-match
  selection are implemented by `get_by_name` at line 49.
- The speculation link landed in public policy documentation at
  `scylla/src/policies/speculative_execution.rs:32`. Scheduling is implemented
  by `execute` at line 187, and the full request-error classification starts
  at line 136, before delegating database errors to
  `scylla-cql-core/src/frame/response/error.rs:451`.
- The retry-traits link pointed to the gated constructor's documentation;
  `RetryPolicy` starts at line 72. Added a separate anchor to the constructor's
  `scylla_unstable`/Python gate at line 30.

Corrected 29 anchors to their relevant declaration/implementation starts and
added the two separate constructor/classification links. Other adjustments
include the actual query implementation, first-value caching, cache builder,
attempt loop, fallthrough decision, typed-stream polling, response normalization,
session response handling, and public lending wrapper. Inspected their bodies
and callers rather than treating the new anchor's existence as behavioral proof.

Final static validation checked all 61 pinned source links against the local
commit and all relative Markdown destinations. The remaining metadata-limitation
link intentionally identifies explanatory source documentation, whose claims
were checked against the immutable bind metadata and mutable result cache.

### 2. Result-metadata wording needed to distinguish ignored updates from errors

`scylla/src/network/connection.rs:996-1031` updates metadata only for an EXECUTE
Rows response carrying an ID. `reprepare` at lines 773-821 returns `Ok(())`
without installing metadata when the new metadata has no ID or would replace a
nonempty snapshot with an empty one. It also leaves the cache alone for an
unchanged ID; these conditions do not reject an otherwise valid reprepare.

Changed the general “response” update statement to “EXECUTE row response” and
specified that skipping an empty reprepare cache replacement does not fail
reprepare. Preserved the separately documented changed-prepared-ID error and
the non-atomic cache-update interleavings.

### 3. Default retry test coverage was broader in wording than in assertions

`scylla/src/policies/retry/default.rs:216-272` checks a selected set of terminal
errors. It does not construct every terminal variant handled by the implementation
at lines 62-180; rate-limit, response-parse, unexpected-response, and nonfinished
paging-state examples are absent. The idempotence test at lines 290-304 explicitly
uses broken connection, overloaded, server, and truncate errors.

Changed “listed terminal errors” to selected errors, named the four idempotence
cases, and added `missing:default-retry-terminal-error-matrix`. The implementation
decision table remains unchanged.

### 4. Bound-statement tests do not execute the post-binding override matrix

`scylla/tests/integration/statements/bound.rs:218-310`
(`bound_statement_can_be_configured_after_binding`) checks setters and resetters
through the underlying prepared statement's getters and policy identities.
`bound_statement_is_executed_with_inherited_config` at lines 317-376 verifies
inherited page size, tracing, and history during execution. The value-carrying
helper at lines 61-100 executes all three bound modes and reads back values.

Replaced the claim that inherited and overridden options all execute correctly
with these specific assertions. Added
`missing:bound-post-binding-override-execution` for observing post-binding
overrides in request bytes and policy selection.

### 5. Batch test descriptions conflated smoke tests and semantic readback tests

`scylla/tests/integration/statements/batch.rs:574-623` accepts a native counter
batch but never reads the counters back. `test_batch_to_multiple_tables` at
lines 627-661 prepares and executes a textual `BEGIN BATCH` statement; it does
not exercise the native BATCH request entry point or inspect stored rows.

The combined `test_batches_of_various_statement_kinds` does provide stronger
coverage: it calls `counter_batch_increments_counters` at lines 982-1012, which
executes a native counter batch across three tables and reads each increment,
and the helpers at lines 1015-1075 check invalid batch-type combinations.

Split the original broad batch-semantics bullet into assertions for logged/LWT
results, the two smoke tests, and the combined suite's actual counter/type
checks. Added only the missing textual multi-table readback assertion as
`missing:textual-multitable-batch-readback`; did not describe native multi-table
counter readback as missing.

## Whole-section implementation checks

The following paths and symbols were inspected in addition to the findings above.
Line references are to the pinned checkout.

| Source and key symbols | Behaviors checked |
| --- | --- |
| `scylla/src/statement/mod.rs:26-104`, `unprepared.rs:24-219`, `prepared.rs:201-310,350-691`, `bound.rs:20-150` | Page size 5,000 and positive-size validation; false idempotence/tracing/cache defaults; serial inheritance versus omission; option reset/getter behavior; shared prepared identity and result cache; per-clone config/page size and cleared preparation trace IDs; binding, token calculation, and forwarding bound setters. |
| `scylla-cql-core/src/frame/request/query.rs:9-94` | Start versus explicitly empty continuation, opaque byte export/restoration, separate completion, and control-flow conversion. |
| `scylla/src/client/execution_profile.rs:175-208,278-528` | Profile defaults, immutable settings, copied builders, independently created handles, shared remapping, loaded snapshots, and labels. |
| `scylla/src/client/session.rs:906-944,1048-1075,1183-1246,1294-1381,1604-1801,1841-1866,1933-2037,2048-2207,2258-2275` | Explicit paging modes; serialization/deadline ordering; unprepared empty/nonempty-value paths; best-effort tablet token; first-member batch routing and count limit; preparation fan-out, fallback, ID agreement, first result/error selection, partitioner installation, and lack of explicit-prepare timeout; batch preparation cancellation; Python preserialized unpaged continuation behavior. |
| `scylla/src/cluster/state.rs:792-830` | Working-connection iterators, per-node selection, filtered pool failures, and immediate error when the initial iterator cannot supply a connection. |
| `scylla/src/network/connection.rs:138-223,730-821,960-1218,1252-1370` | Request orphan ownership; raw prepare conversion; reprepare ID checking; timestamp construction; metadata-ID negotiation, snapshot selection and replacement; one EXECUTE resend versus repeated batch repair; best-effort tablet feedback only on EXECUTE; sequential deduplicated implicit batch preparation. |
| `scylla/src/statement/batch.rs:296-424`, `scylla/src/client/caching_session.rs:54-337` | First serialized member reuse; other batch-member serialization on send; cache keys, current-call options, shared metadata, eviction order, non-atomic capacity repair, independent misses, prepared handle lifetime, partial cached-batch changes, and cache preparation before execution. |
| `scylla/src/client/execution.rs:150-185,241-310,344-351,431-673` | Resolution, target connection selection, attempt/history/metrics boundaries, retry laziness, current versus routing consistency, shared speculative plan, idempotence gate, deadline ownership, ignored-write result, cancellation, and last-error/empty-plan behavior. |
| `scylla/src/policies/retry/default.rs:62-180`, `fallthrough.rs:33-39`, `downgrading_consistency.rs:68-252`, `retry_policy.rs:9-105` | Full decision tables, serial regular-consistency checks, separate versus shared one-use budgets, EACH_QUORUM zero-count fallback, write-timeout idempotence, custom-policy lifetime/reset distinction, and constructor gates. |
| `scylla/src/policies/speculative_execution.rs:20-247`, `scylla-cql-core/src/frame/response/error.rs:451-493` | Policy interval/branch count, fixed and metrics-gated percentile policies, 100 ms fallback, timed launches, branch exhaustion, transient versus terminal outcomes, and owned losing futures. |
| `scylla/src/client/pager.rs:134-689,706-1402` | Fixed configuration/cluster snapshot, per-page core calls, coordinator node/shard preference and filtering, first-page fetch, ignored/non-row responses, continuation advance, one-slot channel and detached worker, terminal fetch errors, consumer-visible descriptors, empty-page handling, repeated type checks, lending feature gate, and internal fixed-connection paging. |
| `scylla-cql/src/frame/response/result.rs:840-934`, `scylla-cql/src/deserialize/result.rs:25-110` | Cached versus response metadata snapshots, retained response bytes, row-count parsing, and remaining-count/cursor behavior on raw row-framing errors. |
| `scylla/src/response/query_result.rs:19-426`, `request_response.rs:32-143`, `coordinator.rs:12-60` | Row/non-row distinction, returned original non-row result, typed lazy decoding and cardinality helpers, exact first-match column lookup, warnings/traces, actual connection address/shard, and post-core shape validation. |
| `scylla/src/policies/timestamp_generator.rs:15-211`, `scylla/src/client/session.rs:470-514,1822-1838,2380-2482,2496-2540` | Timestamp precedence and per-attempt generation, monotonic CAS and warning configuration, schema/tracing defaults, separate schema/keyspace side effects after core success, concurrent trace-table reads, incomplete-trace retry/sleep boundaries. |
| `scylla/Cargo.toml:35,76-85`, `scylla/src/client/session.rs:2048-2074,2220-2227`, `scylla/src/response/query_result.rs:132-140,395-423` | Metrics disabled by default and Python/C#/C++/Node.js execution/result feature gates. |

All language-independent subsections, Rust evidence, invariants, and test-generation
notes were considered against these paths. The core contracts concerning retries,
deadlines, preparation, paging, cache concurrency, cancellation, and result
ownership did not require behavioral changes in this round beyond the clarified
metadata-update wording.

## Test claims checked and limits

Resolved all 84 distinct named Rust test symbols in the final specification.
Read the referenced policy/speculation/timestamp/result/cache unit assertions;
inspected integration test assertions and followed relevant helper calls rather
than inferring coverage from test names. The integration inspection included:

- Statement preparation/options, bound values, positional/named values, mixed
  batches, transparent reprepare, SELECT without FROM, metadata extension and
  mixed-extension behavior, profiles, consistency, timeouts, timestamps, and
  coordinator descriptors.
- Session retry/downgrade behavior, pager timeout/consistency/plan/coordinator
  assertions, cache option and prepare-count checks, schema/keyspace side
  effects, tracing, page history, and paging-specific metrics.
- The weak assertions already called out in the spec remain accurate: the
  unprepared opcode smoke tests ignore the request result; preparation's shard
  helper checks only observed shards; speculation smoke tests do not count
  branches; coordinator stability assertions compare host IDs; ignored-write
  paging uses a custom policy and does not inject a write timeout.
- Metadata-extension tests skip when unsupported. The mixed-cluster test removes
  the extension advertisement from one proxy, and the special-query CCM test
  enables authentication/authorization. SELECT-without-FROM skips only the
  capability probe's SyntaxError. Cassandra exclusions and the metrics feature
  gate match their annotations. `tests/integration/utils.rs:368-369` overrides
  trace readiness to 200 attempts/50 ms rather than the production defaults.

No Rust, Go, database, proxy, CCM, race, or allocation tests were executed.
Static source/link/symbol checks do not prove the tests pass. Existing missing
concurrency, failure-injection, protocol-boundary, and Go migration/benchmark
coverage remains explicit; three narrowly scoped missing assertions were added
for the verified coverage limitations above. No cross-section defect requiring
another agent's file to change was identified.
