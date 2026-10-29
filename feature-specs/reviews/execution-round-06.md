# Execution review, round 06

- Section: `feature-specs/execution.md`
- Round: 06 — omitted public, optional, and less-common APIs and behavior
- Agent identity: `/root/review_execution_06`
- Reference checkout: `/extra/scylladb/gocql-rust-reference-f8b0ee9`
- Verified source commit: `f8b0ee928dc0dcb475247dc82cce4e42804b0621`
- Before SHA-256: `993bc3eaa8b8a80983f39143c5d592a3cd87c846dcad43cf72ef8ef8b1e96d90`
- After SHA-256: `1a52652e34fdd0edd2a17755de658fad128e04dea276c54c61b85210c65bc90f`

Read the review instructions, README, complete execution section, and execution
catalog assignments. Traced implementation independently; prior round reports
were not used as proof. Only the assigned specification and this report were
edited. No Go or Rust implementation was changed.

## Findings and fixes

1. **Manual paging omitted its public persistence and completion contract.**
   `scylla-cql-core/src/frame/request/query.rs:9-94` implements start state,
   shared raw continuation bytes, restoration without validation, the separate
   completion enum, and conversion into control flow. `Some(empty bytes)` is
   still a continuation; `None` alone denotes start or, in the response
   constructor, completion. `scylla-cql/src/frame/request/query.rs:133-174`
   includes the paging-state flag whenever bytes are present. Session single-page
   calls resolve new execution settings and plans (`session.rs:1629-1767`,
   `2085-2187`) rather than retaining automatic-pager coordinator preference.
   Added these contracts and the public helper names, with a dedicated test gap.

2. **Configuration inspection and reset behavior was absent.**
   `statement/unprepared.rs:62-95`, `statement/prepared.rs:436-469,512-545,612-663`,
   `statement/batch.rs:83-116,174-225`, and `statement/bound.rs:63-148` implement
   reset and getter behavior. Serial getters flatten `Option<Option<_>>`, so
   inheritance and explicit omission are indistinguishable to the getter while
   execution distinguishes them (`client/execution.rs:170-191`). Metadata-cache
   getters likewise report preference, not negotiation. `execution_profile.rs:
   434-525` provides settings-copy builders, labeled handles, and a profile
   snapshot that does not track later remapping. Added these details and explicit
   coverage gaps for serial getter/resolution and public profile snapshots.

3. **The cache wrapper omitted preparation without execution.**
   `client/caching_session.rs:171-208` exposes cached batch preparation and
   `add_prepared_statement`; the latter accepts an unprepared `Statement`, clones
   it, and obtains a configured cached/new prepared handle. It does not import an
   existing prepared object. Lines 248-255 expose capacity and the underlying
   session. Added this public use case and the distinction between wrapper calls
   and direct underlying-session calls. Existing cache tests exercise these
   paths, so they were not labeled wholly untested.

4. **Optional execution APIs needed a precise exception to the stable unpaged
   contract.** `client/session.rs:2048-2077` implements Python's gated
   `execute_unstable`: `paged=false` omits page size but calls internal `execute`
   and returns continuation state. It does not run the check in stable
   `execute_unpaged_nongeneric` at lines 918-941. Qualified the stable-mode table
   and invariant, and documented this exception explicitly. Also named the
   Python serialization/result/retry-info helpers (`statement/prepared.rs:
   669-675`, `response/query_result.rs:132-139`, `policies/retry/retry_policy.rs:
   29-43`) and distinguished validation-preserving serialization from
   preserialized execution. The C#, C++, and Node.js gates were rechecked at
   `session.rs:2220-2229`, `pager.rs:746-753`, and `query_result.rs:395-424`.
   Added a wrapper-feature/continuation test gap.

5. **Metrics-dependent speculation availability was implicit.**
   `policies/speculative_execution.rs:19-24,80-90,106-129` gates the context's
   metrics field and percentile policy on `metrics`. `scylla/Cargo.toml:34-74`
   leaves features disabled by default. The scheduler and fixed/custom policies
   remain available using the internal no-op metrics implementation
   (`observability/mod.rs:15-19`, `client/execution.rs:452-490`). Made these
   conditions explicit instead of implying the percentile policy always exists.

6. **Result/coordinator helper behavior was underspecified.**
   `response/coordinator.rs:26-59` records the actual connection address and
   optional advertised shard, not simply the requested plan target. Its address
   is unsuitable as a stable node identifier. `query_result.rs:43-55` performs
   exact name matching and returns the first duplicate name. Added these rules.
   `pager.rs:800-824` also exposes a deprecated current-page-only type-check
   helper; documented why it does not establish compatibility for later pages.
   Added a duplicate-column-name test gap.

7. **The public retry-session reset method needed its actual lifecycle stated.**
   `policies/retry/retry_policy.rs:75-111` exposes `new_session`, decision, and
   reset methods. `client/execution.rs:344-349,474-480,505-511` creates fresh,
   lazy branch-local sessions; neither the core nor pager/session callers invoke
   `reset`. Added the distinction so custom policy authors do not infer a reset
   callback between requests or pages.

8. **The deadline exclusions omitted first-member batch work.**
   `client/session.rs:1294-1368` invokes `peek_first_token` before the request
   runner; `statement/batch.rs:308-344` serializes first-member values and computes
   its token there. Prepared token extraction likewise precedes the runner
   (`session.rs:2095-2109`, `pager.rs:972-982`). Expanded deadline exclusions to
   include that serialization and pre-runner partition-key/token calculation.

## Whole-section checks and inspected source

The following existing behaviors were checked and retained:

- Statement/bound/batch defaults, positive page sizes, all stable execution modes,
  inherited configuration, and prepared clone metadata/configuration ownership:
  `statement/{mod,unprepared,bound,batch,prepared}.rs`, especially `prepared.rs:
  143-243,274-435,559-606,680-726` and `session.rs:746-1257` entry points.
- Connection-local preparation for unprepared values versus explicit preparation
  for automatic paging; best-effort local token hints; prepare fan-out, fallback,
  first-success metadata, ID comparison, and lack of a prepare timeout:
  `session.rs:1604-1870,1934-2040` and `connection.rs:760-816`.
- Prepared serialization reuse, first-member batch caching, per-attempt
  serialization of remaining batch values, implicit deduplication and sequential
  preparation, explicit concurrent batch preparation, and batch-count ordering:
  `session.rs:1294-1388,2085-2276`, `batch.rs:301-407`,
  `connection.rs:1251-1391`, and `caching_session.rs:102-245`.
- EXECUTE's single internal reprepare resend, batch's repeated repair loop,
  timestamp preservation during those resends, ID mismatch/missing-ID errors,
  current-result-metadata updates, empty-cache conditions, and EXECUTE-only tablet
  feedback: `connection.rs:773-816,959-1215,1251-1391` and
  `scylla-cql/src/frame/response/result.rs:878-930`.
- Pool acquisition skips, lazy retry creation, actual-consistency retry input,
  success/error/ignore history and metrics, timeouts, cancellation ownership,
  and response-shape checks after core success: `client/execution.rs:165-205,
  344-693`, `response/request_response.rs:65-157`, `errors.rs:45-152,907-1023`,
  and connection orphan notification at `connection.rs:138-161`.
- Default/downgrading/fallthrough decision matrices, regular-serial special cases,
  independent versus shared retry budgets, and speculative scheduling/error
  classification: `policies/retry/{mod,retry_policy,default,
  downgrading_consistency,fallthrough}.rs`, `policies/speculative_execution.rs:
  19-239`, and `scylla-cql-core/src/frame/response/error.rs:451-489`.
- Pager configuration and cluster snapshots, per-page plans/deadlines/retries,
  preferred coordinator filtering, continuation advancement, worker/channel
  lifetime, empty pages, consumed-page descriptors, repeated type checks, and
  stream errors: `client/pager.rs:124-448,560-594,701-1410`, including prepared
  constructors at 926-1115 and first-response side effects at 1192-1250.
- Row/non-row shape, lazy typed access, cardinality, warnings and traces, timestamp
  generators, keyspace/schema side effects, tracing readiness attempts/defaults,
  and internal fixed-connection paging: `response/{mod,query_result,
  request_response,coordinator}.rs`, `policies/timestamp_generator.rs:15-218`,
  `session.rs:482-510,1822-1840,2380-2395,2424-2530`, and the pager paths above.
- Catalogued `scylla/src/book_tests.rs` was inspected as generated documentation
  test inclusion, including statement and profile chapters; documentation prose
  was not substituted for the implementation.

## Test evidence and validation

- Statically resolved all **84** named Rust test references in the section to
  functions in the pinned checkout. Checked all **59** pinned source links for
  the correct commit, existing paths, and valid line bounds. These are source
  checks, not executed Rust tests or HTTP link probes.
- Read relevant test bodies and assertion blocks for result helpers, cache fills
  and option isolation, retry policies and scheduler timing, prepared statements,
  batch/value forms, transparent repair, metadata extensions, profile selection,
  consistency, request timeouts, pager behavior, history/metrics, timestamps,
  coordinator reporting, trace lookup, raw USE, and schema agreement. Retained
  the existing qualifications about smoke tests, coordinator host-ID versus shard
  assertions, and the incomplete expected-shard-set assertion in preparation.
- `caching_session.rs:test_full,test_execute_cached` exercises public explicit
  cache preparation and repeat reuse; `statements/bound.rs:220-295` checks stored
  override reset but does not prove getter equivalence plus effective serial
  omission/inheritance; `query_result.rs:514-526,588-638` uses unique column names,
  so it does not cover duplicate lookup. Coordinator tests inspect node/shard,
  not translated connection-address identity. No new test-run claim was added.
- Rechecked conditional coverage: `cassandra_tests` exclusions for large batches
  and CDC, metadata-extension early returns and simulated mixed advertisement,
  CCM authentication setup, SELECT-without-FROM's syntax-only capability skip,
  metrics module gating, and tracing helpers' 200 attempts/50 ms settings.
- Added five explicit missing test targets for the newly documented less-common
  behavior. Existing Go test and allocation targets remain backlog.
- The tracked-file `git diff --check` command passed, but these files are
  untracked. Therefore both files were also checked with `git diff --no-index
  --check /dev/null <file>`: no whitespace diagnostics, exit 1 for differing
  files. No Rust/Go tests, feature-combination builds, database runs, or
  allocation measurements were run.

Remaining gaps are explicitly documented rather than treated as demonstrated
coverage. No additional cross-section contradiction was identified in the checked
routing, serialization, and configuration references.
