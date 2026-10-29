# Execution reference review — round 01

- Section: `feature-specs/execution.md`.
- Round: 01, independent implementation-fidelity audit and fixes.
- Agent identity: `/root/review_execution_01`.
- Source checkout: `/extra/scylladb/gocql-rust-reference-f8b0ee9`.
- Verified source HEAD: `f8b0ee928dc0dcb475247dc82cce4e42804b0621`.
- Before SHA-256: `96c3107063fce103927d2714f7def4e116c17f6926ee40677832585177041f02`.
- After SHA-256: `2c2758aeca79d6478503a458f46b5d02cf57df6236bc3b2fa1662bb94b68f9c7`.

Read the review instructions, section README, and complete execution specification.
The entire section was checked, including the language-independent contracts,
implementation links, invariants, optional interfaces, and test claims. Only this
section and this report were edited. No implementation or test files were changed.

## Source inspected

All source paths below are relative to the pinned Rust checkout. Line references
refer to that revision, not to current upstream.

| Files and key symbols | Inspection scope |
| --- | --- |
| `scylla/src/statement/mod.rs:25–94`, `unprepared.rs:24–115` | Statement defaults, inherited options, positive page-size validation. |
| `scylla/src/statement/prepared.rs:201–420,548–725` | Shared identity/metadata, clone, token extraction, bind, current/original metadata access, cached configured handles. |
| `scylla/src/statement/bound.rs:20–150` | Owned serialized values, token calculation, forwarded statement options. |
| `scylla/src/statement/batch.rs:23–125,301–400` | Batch construction/options and `peek_first_token`/first-value reuse. |
| `scylla/src/client/execution_profile.rs:178–210,394–464,506–529` | Defaults, immutable profiles, distinct handles, atomic remapping/access. |
| `scylla/src/client/session.rs:482–514,906–990,1135–1247,1294–1382,1604–1838,1841–2278,2380–2531` | Public modes, bound APIs, routing inputs, preparation fan-out/fallback, implicit preparation, side effects, tracing retrieval. |
| `scylla/src/cluster/state.rs:775–830` | Working-connection iterators and their immediate failure boundary. |
| `scylla/src/client/execution.rs:1–673` | Resolved settings, targets, shared plans, timeout, metrics/history, retry loop and consistency mutation. |
| `scylla/src/client/pager.rs:90–640,710–1402` | First/later page processing, fixed connection path, worker/channel ownership, configuration snapshots, stability, stream type checking and errors. |
| `scylla/src/network/connection.rs:138–173,716–822,929–1405,1459–1522` | Serialization on every send, preparation/reprepare, metadata-ID negotiation/update, tablet feedback, QUERY/EXECUTE/BATCH construction, parsing. |
| `scylla/src/client/caching_session.rs:1–370,628–708,869–898` | Query-text keys, configuration isolation, eviction/concurrency, builder and zero-capacity checks. |
| `scylla/src/policies/retry/default.rs:62–184,211–506`; `downgrading_consistency.rs:68–235,285–713`; `fallthrough.rs:1–42` | All production retry arms, serial-consistency distinction, per-session budgets and representative unit assertions. |
| `scylla/src/policies/speculative_execution.rs:1–405`; `scylla-cql-core/src/frame/response/error.rs:451–493` | Policy feature gate, intervals, synchronized racing, termination and complete error classifications. |
| `scylla/src/policies/timestamp_generator.rs:1–214` | Wall-clock and monotonic generators, warning configuration, epoch failure and concurrency tests. |
| `scylla/src/response/query_result.rs:73–215,310–443`; `request_response.rs:1–166` | Result kind, metadata access, lazy rows, cardinality, continuation validation and wrapper feature gates. |
| `scylla-cql/src/deserialize/result.rs:26–110` | Lending iterator ownership, raw framing-error advancement. |
| `scylla-cql/src/frame/request/batch.rs:58–174`; `scylla-cql-core/src/frame/request/batch.rs:1–38` | Values serialization/count checking, batch type semantics. |

## Verified findings and fixes

1. **Preparation fallback and metadata selection needed precise boundaries.**
   `Session::prepare_nongeneric` (`session.rs:1941–1965`) uses `?` when obtaining
   the initial working-connection iterator, so that error does not start the
   all-shards fallback. `prepare_on_all` (`1990–2030`) joins all preparations and
   selects the first successful result in connection-iteration order. Added the
   early-return boundary and made selection order explicit.

2. **Reprepare metadata updates omitted the changed-ID condition.**
   `Connection::reprepare` (`connection.rs:793–818`) requires a supplied metadata
   ID, preserves nonempty metadata against empty replacement, and only stores
   when the ID differs. The same-ID/empty-cache exception exists separately in
   `handle_result_metadata_new_id` (`996–1028`). Documented this distinction.

3. **Tablet-feedback scope was implicit.**
   `execute_raw_with_consistency` processes tablet feedback for both EXECUTE
   responses (`connection.rs:1165–1175,1212–1220`). QUERY (`959–994`) and BATCH
   (`1251–1325`) do not call that processor. Added the native QUERY/BATCH boundary
   and explained why an unprepared query with values still uses the EXECUTE path.
   This was also independently raised by the routing reviewer through the parent.

4. **Batch serialization reuse was incompletely specified.**
   Only `peek_first_token` caches the first prepared member's values
   (`batch.rs:301–400`). Every `RouterHandle::send_request` serializes its request
   (`connection.rs:138–145`); `Batch::do_serialize` obtains a fresh values iterator
   (`scylla-cql/src/frame/request/batch.rs:84–104`). The internal repair loop
   resends through that function (`connection.rs:1289–1316`). Added that other
   members can reserialize on outer retries and repair resends, and recorded a
   dedicated missing test.

5. **History/retry callback ordering was reversed.**
   The failure callback to load balancing occurs first (`execution.rs:620–626`),
   the retry decision is then computed (`640`), and history receives the error
   together with that decision (`646`). Corrected the ordering claim.

6. **Some public errors occur after core success.**
   Unpaged continuation validation happens after `execute`/`query` returns
   (`session.rs:917–940,1604–1625`). Later-page shape validation follows the core
   result (`pager.rs:239–259,464–499`). Core success history and latency were
   already recorded (`execution.rs:609–617,531–535`). Added the validation/error
   boundary, including lack of a retry-policy decision and row decoding outside
   the request deadline.

7. **Pager type checking and error continuation needed qualifications.**
   `poll_fill_page` skips empty pages (`pager.rs:763–778`), while typed checking
   occurs after `next` has obtained a raw row (`720–738,1340–1361`). Typed errors
   consume the row; the stream does not close its receiver or stop the worker.
   Raw framing errors decrement the count but can repeat at malformed bytes
   (`scylla-cql/src/deserialize/result.rs:53–82`). Documented empty-page behavior
   and the distinct error advancement, with an explicit missing test.

8. **An optional public wrapper interface was missing.**
   `QueryRowsResult::raw_rows_with_metadata` is public under both `scylla_unstable`
   and `unstable-nodejs-rs` (`query_result.rs:432–438`). Added Node.js beside the
   already documented Python, C#, and C++ interfaces.

9. **Two timing/failure details were less exact than the implementation.**
   The simple timestamp generator panics on pre-epoch time via `unwrap`
   (`timestamp_generator.rs:33–38`); changed generic “fails” to “panics”.
   `get_tracing_info` sleeps after every incomplete attempt, including the final
   one (`session.rs:2382–2392`); documented that final interval.

10. **Unprepared-opcode test coverage was overstated.**
    `test_prepare_query_with_values` and `test_query_with_no_values`
    (`statements/unprepared.rs:122–222`) drop the unwanted opcode only on node 2
    and accept any completion through `_res`, without asserting success or
    observing actual outgoing opcodes. Reworded these as smoke coverage and
    added a missing target for a deterministic opcode/result assertion. The
    implementation contract itself remains established by `session.rs:1702–1755`.

## Checked behaviors retained without correction

- Three explicit paging modes, no silent unpaged continuation loss, default page
  size 5,000, statement/batch idempotence false, and server validation of batch
  contents and SELECT-without-FROM support.
- LocalQuorum/LocalSerial/30-second profile defaults; statement overrides;
  three-state serial consistency; profile-based deadline disabling; immutable
  profile snapshots and shared handle remapping.
- Local prepare per nonempty-value unprepared attempt versus session preparation
  before an automatic pager; explicit preparation waits for all attempts, allows
  partial success, checks IDs, and has no statement timeout/retry/speculation.
- Bound values are serialized once; ordinary prepared serialization precedes
  the request timer; prepared token errors propagate; clones share metadata but
  clear preparation trace IDs and retain local options/page size.
- EXECUTE repairs one UNPREPARED on the same connection, validates identity, and
  retains request parameters/timestamp; batch repair loops and errors on unknown
  IDs. Metadata extension negotiation handles empty and mixed-version metadata.
- Batch maximum/count checks, first-member routing, any-prepared-LWT routing
  flag, implicit query-text deduplication, explicit concurrent batch preparation.
- Cache capacity 128, zero-capacity panic, text-only keys, current-call options,
  wrapper metadata preference, iterator-order eviction, and non-atomic capacity.
- Pool errors skip targets without sent/failed counters; retry state is lazy and
  branch-local; retry decisions see current consistency; routing retains the
  original consistency; ignore-write success is empty and has no trace/warnings.
- Every default/downgrading/fallthrough production error arm, one-use budgets,
  regular serial consistency special cases, idempotence gates, and downgrade
  reply-count choices match the tables.
- Speculation requires idempotence; branches share target consumption, keep
  independent retries, obey decisive/ignorable error classification, and drain
  running branches after plan exhaustion. Percentile policy requires metrics and
  uses 100 ms if percentile lookup fails.
- Automatic pages preserve the initial cluster/profile snapshot and tablet hint,
  reset plan/retry/deadline state, favor the preceding actual coordinator, and
  exclude that target from fallback. Channel capacity one permits a worker-held
  additional page; dropping the pager is observed on send, not immediate fetch
  cancellation. Trace/coordinator lists advance when pages are consumed.
- Empty SELECT remains a rows result; optional/required/exactly-one row APIs
  check cardinality without decoding unused payloads. Manual rows can borrow;
  the stable stream yields owned values. Warnings are unavailable on pagers.
- Timestamp precedence, per-outer-attempt generation, resend reuse, monotonic
  shared-instance behavior, schema/keyspace post-success side effects, default
  agreement settings, and trace-fetch completeness/defaults match source.
- Fixed-connection internal paging uses one connection, fallthrough retry,
  no history/metrics/speculation, and rows-only results. The implementation and
  missing Go migration/race/allocation work remain explicitly separate.

## Test claims and validation

An automated source lookup found all **75 named Rust test targets across 27
files**. Assertion and setup inspection covered the cited retry/scheduler,
timestamp and caching unit tests; integration assertions were checked across the
cited prepared/unprepared/bound/batch, metadata-extension, profile/consistency,
timeout, retry, pager, cache, tracing, timestamp, coordinator, history/metrics,
schema-agreement, keyspace, and value-binding suites. Test names alone were not
used as evidence of runtime success.

In particular, read the preparation fault-injection cases, all retry production
arms and budget assertions, paused-time speculative termination tests, cache
wire-parameter assertions, page timeout/consistency/coordinator assertions,
schema-change stream type-error assertion, metadata-ID wire assertions, and
batch count/limit assertions. The weak unprepared-opcode tests were corrected as
described above. `test_select_without_from` skips only an initial SyntaxError;
its supported path asserts names, types, aliases, prepared metadata and values.

Validated all **42 pinned source URLs** against local file existence and line
counts, and verified all relative Markdown links exist. Both owned files are
untracked in the current workspace, so an explicit line-by-line trailing-space
and final-newline check supplied whitespace validation; `git diff --check` alone
does not inspect them. The report's after hash was checked against the spec.

No Rust tests, Go tests, database/proxy processes, or benchmarks were executed.
Database portability and passing runtime status remain unverified. Existing
missing targets for concurrent cache capacity, keyspace cache reuse, prepare
timeouts, timestamp retries, and pager-drop cancellation remain open. New missing
targets cover deterministic unprepared opcodes, pager empty/error continuation,
and batch retry serialization. Raw framing-error recovery and concurrent metadata
arrival ordering are source-derived behavior, not claims of dedicated coverage.

Cross-section observations (EXECUTE-only tablet feedback, Node.js raw access, and
history callback ordering) were sent to the parent; other sections were not edited.
