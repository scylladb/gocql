# Execution review round 03

- Section: `feature-specs/execution.md`
- Round: 03 — errors, partial success, failover, retry, and recovery
- Agent identity: `/root/review_execution_03`
- Reference checkout: `/extra/scylladb/gocql-rust-reference-f8b0ee9`
- Source commit, confirmed with `git rev-parse HEAD`:
  `f8b0ee928dc0dcb475247dc82cce4e42804b0621`
- Before SHA-256:
  `e94e9a23632873066a6202d3b5738f480d37df19d9f0adc4cfdb1c4ea0f2b530`
- After SHA-256:
  `8f16cfc5d8bdb66f1723951645c153edabc06c1a418680a654882419dfac8649`

Read `REVIEW-INSTRUCTIONS.md`, `feature-specs/README.md`, and the complete
execution specification. Independently traced the implementation below; prior
round reports were not used as evidence. Only the assigned specification and
this report were edited. No Go or Rust source was changed, and no child agent
was spawned.

## Verified findings and fixes

1. **Preparation failure selection was unspecified.**
   `Session::prepare_on_all` waits for `join_all`, then uses `find_or_first` and
   reports `AllAttemptsFailed { first_attempt }` from connection iteration order
   (`scylla/src/client/session.rs:1981–1996`). This is neither a completion-order
   error nor an aggregate. `prepare_nongeneric` discards any first-pass error and
   returns the fallback result (`1939–1958`); the discarded error can include an
   ID mismatch from `2000–2007`. Added the error-selection rule, fallback error
   replacement, and the absence of server-side rollback after failed preparation.
   Added `missing:prepare-failure-selection` for distinguishing error payloads and
   a first-pass mismatch followed by a different fallback outcome.

2. **Caching omitted failure and partial-success effects.**
   A miss awaits `Session::prepare` before eviction/insertion
   (`scylla/src/client/caching_session.rs:212–241`), so a failed miss does not
   insert a negative entry or evict for that miss. Cached `prepare_batch` uses
   `try_join_all` around member lookups (`171–186`), each of which may already
   have committed its cache insertion and eviction before another member fails.
   No rollback exists. The wrapper prepares before forwarding to the execution
   API (`101–159`), outside that API's request timeout. Added these boundaries
   and `missing:cached-batch-partial-failure`.

3. **Attempt accounting and ignored-write success needed precise boundaries.**
   Connection acquisition errors continue the target loop before attempt metrics,
   history, retry decisions, or load-balancing feedback
   (`scylla/src/client/execution.rs:564–597`). After connection selection, attempt
   counters/history begin before the attempt closure. The query closure can fail
   in local preparation or serialization before EXECUTE
   (`scylla/src/client/session.rs:1720–1743`); batch preparation/serialization can
   likewise fail before BATCH (`scylla/src/network/connection.rs:1251–1296`).
   Clarified that counted attempts need not send the final execution frame.
   An `IgnoreWriteError` decision follows the failed-attempt counter,
   load-balancing failure callback, and error-history event
   (`execution.rs:615–665`), then closes logical request history successfully
   (`531–535`). It does not pass through success/latency handling (`601–609`).
   Added this distinction so empty successful results do not imply successful
   attempt telemetry. Sent the cross-section observability boundary to the parent.

4. **Pager recovery did not explicitly describe partial delivery and resumption.**
   `PagingExecutor::fetch_one_page` clones the same input paging state into every
   attempt (`scylla/src/client/pager.rs:333–340`); `process_first_page` and
   `process_next_page` advance it only for successful row continuations
   (`386–403`, `477–485`). `query_remaining_pages` sends a terminal error and
   returns (`250–252`); `poll_fill_page`/`poll_next_page` consume existing page rows
   and queued results in order (`756–801`). The public pager and typed stream
   expose no paging-state accessor (`810–824`, `1253–1270`, `1316–1333`). Added
   page-local retry state, preservation of already delivered rows, termination
   after a surfaced page-fetch failure, and the lack of a manual-resume token.
   Added `missing:pager-terminal-failure-continuation` for asserting the subsequent
   end-of-stream and absence of a restarted request.

## Whole-section source audit

The following source files and key symbols were actually inspected. Line spans
refer to the pinned checkout, not to future revisions.

| Source | Symbols and checked behavior |
| --- | --- |
| `scylla/src/statement/mod.rs:25–100`, `statement/unprepared.rs:24–207` | `StatementConfig`, `PageSize`, statement setters: page-size 5,000 and positive validation, default idempotence/tracing, serial omission versus inheritance, timeout inheritance. |
| `scylla/src/client/execution_profile.rs:178–217,506–535`, `client/execution.rs:155–200` | Profile defaults, handle access/remapping, per-statement overrides and retained profile snapshots. |
| `scylla/src/statement/prepared.rs:48–110,143–305,560–734`, `statement/bound.rs:20–145` | Preparation metadata/configuration transfer, clone trace-ID clearing, shared mutable result metadata, immutable bind metadata, bound value ownership and token calculation. |
| `scylla/src/client/session.rs:1294–1385,1604–1858,1934–2038,2085–2290` | Batch limit/routing, direct versus locally prepared queries, full preparation for valued automatic queries, preparation fallback, prepared token errors, explicit batch fan-out and early failure. |
| `scylla/src/statement/batch.rs:24–92,301–385` | Batch configuration and first-member serialization/token caching. |
| `scylla/src/network/connection.rs:721–820,959–1382` | PREPARE response validation, reprepare ID equality, one EXECUTE resend versus repeated batch repair, missing batch ID error, metadata updates, tablet feedback, timestamp reuse, and implicit batch preparation. |
| `scylla-cql/src/frame/response/result.rs:878–942` | Response metadata overrides cached metadata when present; otherwise the response retains the selected cached snapshot. |
| `scylla/src/client/caching_session.rs:99–357` | Prepared execution wrappers, query-text keys, option restoration, metadata preference, capacity defaults/validation, non-atomic eviction, and failure effects. |
| `scylla/src/client/execution.rs:431–673` | Timeout scope, shared plan, per-branch current consistency and lazy retry sessions, connection skip, callbacks, exhaustion, and ignored-write conversion. |
| `scylla/src/policies/retry/default.rs:62–193`, `retry/downgrading_consistency.rs:68–240`, `retry/fallthrough.rs:1–40`, `retry/retry_policy.rs:10–115` | Full decision tables, idempotence gates, regular serial-consistency handling, budgets, consistency changes, and fallthrough semantics. |
| `scylla/src/policies/speculative_execution.rs:95–252`, `scylla-cql-core/src/frame/response/error.rs:451–491` | Interval/default, branch count, all error classifications, timer-driven launches, empty plans, decisive results and ignorable failure termination. |
| `scylla/src/client/pager.rs:124–499,529–694,715–1105,1192–1377` | Frozen executor settings, fresh per-page plans/deadlines, stable coordinator, channel backpressure, first-page side effects, fixed-connection pager, trace/coordinator collection, typed stream and cancellation boundaries. |
| `scylla-cql/src/deserialize/result.rs:26–109` | Raw row iterator consumes the row count before framing; successfully skipped columns advance bytes, and framing errors can recur at malformed bytes. |
| `scylla/src/response/query_result.rs:73–235,285–490`, `response/request_response.rs:32–153` | Row/non-row distinction, retained original result on wrong-shape conversion, cardinality helpers, lazy typed decoding, warnings, tracing, coordinator, and unpaged continuation rejection. |
| `scylla/src/policies/timestamp_generator.rs:15–240` | Timestamp units, wall-clock and monotonic generators, drift warnings, concurrent monotonicity tests. |
| `scylla/src/client/session.rs:480–514,1784–1840,2380–2534` | Agreement/tracing defaults, post-success keyspace/schema handling, trace completeness and retry sleep, and side effects outside execution timeout/retry. |

Explicitly checked and retained the following contracts: local unprepared token
errors are swallowed only for the tablet hint, while prepared execution and
prepared pagers propagate token errors; automatic valued queries use full session
preparation; metadata-ID repair does not refresh bind metadata; batches retry the
whole batch on repair while retaining only first-member serialized bytes; ordinary
serialization precedes the core deadline; default retry and speculative-error
classification intentionally differ; successful schema/keyspace side effects are
outside the core deadline; dropped pagers do not immediately cancel an in-flight
fetch. These match the source. The spec's invariants and stable/conditional API
separation remain consistent with the inspected implementations.

## Test evidence and remaining gaps

All 75 existing named Rust test references resolve to actual function definitions;
this was checked mechanically against the pinned files. Source reading, not a test
run, established the following coverage:

- `statements/prepared.rs:801–1015` asserts node preparation, partial failures,
  all-shard fallback, and final `AllAttemptsFailed`; all injected errors have the
  same type, so it does not establish heterogeneous error selection or mismatch
  replacement.
- `statements/transparent_reprepare.rs:32–245` exercises execute, batch, and cached
  execution across schema renames, checks intermediate failures, and verifies
  final rows. It does not force a changed prepared ID or repeated UNPREPARED
  after the EXECUTE resend.
- `session/retries.rs:27–181` exercises speculation and retries during local
  preparation. Its additional named consistency tests exist. Default retry unit
  tests and downgrading unit tests inspected in their implementation files assert
  budgets, reply counts, write kinds, and idempotence. Speculation unit tests
  `test_exhausted_plan_with_running_fibers`, `test_exhausted_plan_last_running_fiber`,
  and `test_se_panic_on_ignorable_errors` assert final errors and virtual timing.
- `session/pager.rs:154–242` tests ignored first-page errors; `370–568` checks
  first-page timeout, second-page timeout after a delivered row, and a cumulative
  retry timeout. The second-page case does not poll again to prove termination.
  `1258–1395` verifies coordinator switching and complete row delivery.
  `296–366` tests a later schema type-check failure but stops at its first error.
- `session/history.rs:338–405,515–648` checks successful page history and a later
  ignored error with failed attempt history plus successful request history.
  The specification's referenced metrics test has assertions distinguishing the
  six paging-mode request/error counters; it does not directly cover the entire
  ignored-error callback/latency boundary.
- `session/schema_agreement.rs:379–510` injects an agreement failure after DDL,
  confirms propagation across entry points, and separately checks metadata refresh.
  These support the distinction between database success and operation failure.
- Checked assertion locations for bound options/results, SELECT without FROM,
  metadata extension/mixed versions, batch count/preparation/semantics, request
  timeouts, timestamps, and profile remapping. Checked the unprepared opcode smoke
  tests directly: the existing caution about dropping one node's opcode without
  asserting the returned result remains accurate.

The three new `missing:` targets above are source-proven behavior with targeted
coverage still needed. Existing missing cases for prepare deadlines, cache
concurrency, retry serialization, pager drop, bind-metadata staleness, and error
continuation remain unresolved. No Rust, Go, database, proxy, race, or benchmark
suite was executed. Markdown/source-reference validation is not runtime proof.
