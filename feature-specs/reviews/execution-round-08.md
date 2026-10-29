# Execution reference review — round 08

- Section: `feature-specs/execution.md`.
- Round: 08, with additional focus on cross-section callers and callees.
- Agent identity: `/root/review_execution_08`.
- Reference checkout: `/extra/scylladb/gocql-rust-reference-f8b0ee9`.
- Verified source commit: `f8b0ee928dc0dcb475247dc82cce4e42804b0621`.
- Before SHA-256: `521381fa37555bcd799be643cf89be94a65fb4fae03c73d364fb4a11a0166514`.
- After SHA-256: `94c51a2a9d64955d136e62875b410aae85d3150cc92d78b9db95aae5c8ba48aa`.
- Outcome: two verified corrections; only this report and the assigned execution
  specification were edited. No Go or Rust implementation changes, child agents,
  or test executions.

## Scope and method

Read `reviews/REVIEW-INSTRUCTIONS.md`, `feature-specs/README.md`, and the complete
assigned specification. Independently followed session entry points through
configuration resolution, routing inputs, connection execution, response parsing,
the shared executor, and pager processing. Reviewed the statement/cache/result
APIs, retry/speculation policies, timestamp generation, invariants, feature gates,
and test claims in addition to the round's cross-section focus.

Compared the relevant contracts in `routing.md`, `connections.md`,
`metadata-control-events.md`, `serialization-protocol.md`, and
`configuration-security-observability.md`. Those files were read as boundary
context, not edited. Source behavior, rather than neighboring prose or previous
reports, determined the corrections.

## Verified findings and fixes

### 1. Trace event decoding does not always happen on an incomplete attempt

The execution section said that query or decoding errors return immediately.
That omitted the distinction between completing the two queries and inspecting
their lazy row results. The configuration section already described the ordering
more precisely.

Evidence: `scylla/src/client/session.rs:2437` starts both trace-table requests
with `tokio::try_join!`, so their execution failures propagate. At lines 2443–2463
the session rows are decoded first, and absent session data or missing duration
returns `Ok(None)`. Only at lines 2466–2475 does the function check and decode
event rows. `get_tracing_info` at lines 2380–2392 retries that incomplete result
and sleeps after every incomplete attempt. At lines 2477–2481, any nonempty
decoded events vector satisfies readiness; there is no check that every event
has arrived.

Fix: distinguished request failures from row/type decoding failures, documented
that incomplete session data defers event-row inspection, and stated that the
independent reads do not establish event completeness. Added the explicit gap
`missing:trace-fetch-decode-order-and-completeness`.

### 2. EXECUTE tablet feedback crosses the execution deadline and publication boundary

The existing execution text mentioned tablet-cache updates and nonfatal malformed
feedback, but did not explain that forwarding valid feedback is awaited inside
the attempt or that publication happens separately. This matters when execution
has a server response but the feedback queue is full.

Evidence: `scylla/src/network/connection.rs:1164–1181` awaits tablet handling before
metadata-cache processing and before matching the response's database error.
Lines 1207–1216 do the same for the internal resend. The table identity guard
means forwarding requires a prepared table specification. The implementation of
`update_tablets_from_response` at lines 2045–2069 parses feedback and awaits
`sender.send`, ignoring a closed receiver. `session.rs:73,1442` supplies a bounded
8,192-entry channel. `scylla/src/cluster/worker.rs:303–338` separately receives a
batch, creates changed state when necessary, and publishes it. The attempt closure
is within the deadline in `scylla/src/client/execution.rs:514–529`; completion
callbacks happen only after that closure returns at lines 604–630.

Fix: documented feedback forwarding before database-error handling, full-queue
delay/timeout after a server reply, ignored channel closure, enqueueing versus
publication, and pending versus completed enqueue cancellation. Added a precise
cross-link to routing, pinned evidence for the sender and publisher, and the gap
`missing:execution-tablet-feedback-backpressure`.

## Implementation inspected and behaviors checked

Line references below identify the inspected implementation areas in the pinned
checkout, rather than asserting that every line of each file was read.

| Files and key symbols | Checked behavior |
| --- | --- |
| `scylla/src/statement/mod.rs:26–105`; `prepared.rs:201–370,436–714`; `bound.rs:20–106` | Page-size/default statement settings, serialization at bind, clone sharing, immutable bind metadata, current/original result metadata, option reset/getter behavior, bound token calculation. |
| `scylla/src/client/execution_profile.rs:182–214,445–526`; `execution.rs:150–194` | Defaults; statement/profile precedence; serial omission versus inheritance; inability to disable inherited timeout with an unset statement timeout; shared remapping cells and retained profile snapshots. |
| `scylla/src/client/session.rs:1604–1854,2085–2186` | Stable unpaged continuation rejection, manual paging parameters, empty/nonempty unprepared paths, connection-local preparation, best-effort tablet-hint token extraction, automatic-paging session preparation, fatal ordinary prepared key/token errors, unchanged initial routing consistency. |
| `session.rs:1933–2038,2258–2275`; `network/connection.rs:730–818` | Preparation waits for all connections per pass, successful-ID agreement, first error/success in iterator order, fallback result replacing the initial failure, metadata partitioner installation, no explicit-prepare request deadline, batch-member preparation with `try_join_all`. |
| `statement/batch.rs:308–388`; `session.rs:1294–1383`; `network/connection.rs:1252–1385` | Batch count check before session serialization/preparation; first-member routing and row caching; whole-batch confirmed-LWT input; deduplicated sequential implicit preparation; per-send serialization of other members; repeatable same-connection repair and missing-ID error. |
| `client/caching_session.rs:1–350`; `statement/prepared.rs:695–714` | Query-text-only keys, cache preparation before execution deadline, empty-value calls still using prepared APIs, current call's options, arbitrary eviction, independent concurrent misses, returned-handle lifetime, builder defaults, partial cached batch effects. |
| `network/connection.rs:773–818,960–1220`; `scylla-cql/src/frame/response/result.rs:877–963` | Same-ID reprepare requirement, one EXECUTE resend, preserved timestamp/values/paging/hint, reread metadata on resend, negotiated metadata-ID/skip rules, empty-cache exception, reprepare nonempty protection, unconditional cache replacement after checks, per-send decoding snapshot. |
| `client/execution.rs:201–395,431–687` | Connection acquisition precedes attempt metrics/history; pool errors skip callbacks/retry; failure feedback precedes retry decision; lazy branch-local retry sessions; current consistency is supplied to policy; ignored-error success retains failed-attempt accounting; timeout/cancellation completion boundaries. |
| `policies/retry/default.rs:62–253`; `downgrading_consistency.rs:68–292`; `fallthrough.rs:1–39`; `retry_policy.rs:20–94` | Default separate budgets, opt-in shared downgrade budget, serial regular-consistency special cases, idempotence gates, terminal serialization/protocol errors, unconditional fallthrough stop, same/next-target consistency changes, custom-policy contract. |
| `policies/speculative_execution.rs:1–415`; `scylla-cql-core/src/frame/response/error.rs:451–488` | Additional-branch limit and fixed interval per request, metrics-gated percentile fallback, shared target iteration, ignorable versus terminal outcomes, empty-plan termination, loser future drop, paused-clock regression assertions. |
| `client/pager.rs:134–635,675–1075,1110–1260,1344–1380` | Fixed topology/settings, per-page plans/deadlines/retries, preferred actual coordinator/shard, unsharded-node filtering, continuation advancement, eager first page, one-slot channel and extra in-flight page, detached worker cancellation, first-page side effects, later non-row failure, lazy typed stream and consumed-page descriptors. |
| `client/pager.rs:540–635,1118–1190` | Fixed-connection internal pager uses fallthrough retry, no speculation/metrics/history, zero tablet hint, configured per-page deadline, and row-only responses. |
| `response/query_result.rs:1–435`; `request_response.rs:63–145`; `response/coordinator.rs:1–66` | Non-row/empty-row distinction, original result retained on conversion error, lazy cardinality helpers, exact first-match column lookup, warning/trace retention, actual connection address and advertised shard, internal-only unknown coordinators. |
| `policies/timestamp_generator.rs:1–217`; `network/connection.rs:960–995,1105–1159,1268–1296` | Explicit/generator/server precedence, per-attempt generation, internal-resend reuse, per-instance monotonic CAS, configurable drift warnings, pre-epoch behavior, per-thread ordering and shared-generator uniqueness assertions. |
| `session.rs:1778–1838,2380–2483,2496–2531` | Successful core request precedes keyspace propagation and automatic schema handling; only winning response reaches those side effects; separate agreement deadline/refresh; trace request/readiness/decoding ordering. |
| `network/connection.rs:138–175`; `cluster/worker.rs:303–338`; `session.rs:73,1442` | Serialized request ownership and orphan notification at transport submission; tablet enqueue/publication separation and deadline implications. |
| `scylla-cql-core/src/frame/request/query.rs:9–104`; `session.rs:2041–2073`; `pager.rs:748–754`; `query_result.rs:134–146,390–429`; `retry_policy.rs:30–42` | Start versus empty continuation bytes, independent completion state, gated preserialized execution returning a continuation without stable unpaged validation, gated borrowing/raw result APIs and retry-info constructor. |

Cross-section consistency checks explicitly confirmed that:

- The routing plan can retain old topology/tablet data while node pools and shared
  policy feedback remain live; same-target retry reacquires a connection.
- Native BATCH does not publish tablet feedback, although its first prepared
  member can route using previously learned tablet placement.
- Result metadata updates do not replace bind metadata, routing metadata, or the
  retained partitioner. Automatic-pager hints retain their original snapshot.
- Serialization errors before the runner differ from serialization errors inside
  a connection attempt for deadlines, metrics, history, and retries.
- Profile remapping does not reconfigure an existing pager, and retry consistency
  changes do not update that page's routing input or the next page's defaults.
- Keyspace propagation and automatic schema agreement/refresh follow core success
  and may cause public failure without retrying the successful database operation.
- Pager drop differs from dropping a directly awaited execution future; the
  detached fetch can still produce metrics, history, and feedback.

## Test claims and validation

No Rust, Go, proxy, or database tests were executed. The checks were source-based.

- Resolved all **85** named test symbols referenced by the execution specification
  against their stated source files; none were missing. Symbol existence was
  checked separately from assertion coverage.
- Read the trace test and its helpers in
  `scylla/tests/integration/session/tracing.rs:15–230`, plus the configured tracing
  helper values at `utils.rs:368–369`. They cover result/prepare/initial-pager trace
  IDs and successful trace lookup, with 200 attempts at 50 ms. They do not inject
  malformed event rows while session data is incomplete or prove event completeness.
- Read `session/schema_agreement.rs:421–552` and
  `session/use_keyspace.rs:216–278`: failure/refresh assertions cover the API
  matrix, and raw USE/pager USE affects subsequent unqualified queries. These
  checks do not prove the separate deadline/cancellation boundaries by themselves.
- Read `statements/coordinator.rs:282–405`: the test suppresses advertised shards,
  skips nodes with fewer than two shards, and checks shard zero for ordinary and
  pager execution. This supports actual-selected-connection identity, not universal
  per-page preferred-shard stability.
- Read `statements/select_without_from.rs:7–119`: the capability probe skips only
  syntax rejection; later assertions check result aliases/types and bind types.
- Rechecked assertion/guard locations in preparation, bound statements, batches,
  profile selection, request timeout, and result-metadata extension suites. The
  existing qualifications remain necessary: post-bind getters do not establish
  every outgoing override, mixed metadata capability is proxy-induced, and
  ignored/early-return test paths do not establish coverage on every server.
- Rechecked the implementation-side retry and speculation unit-test scaffolding,
  including regular ONE fixtures, nonserial downgrade fixtures, and paused-clock
  speculative tests. Retained the explicit gaps for the full terminal-error matrix,
  mixed budgets, serial behavior, and success/terminal speculative races.
- Searched trace/tablet implementations and integration tests for queue saturation
  and trace-event decoding-order coverage. No direct regression establishing the
  two newly documented boundaries was found; both remain explicit `missing:` cases.
- Validated all **63** pinned Rust links for the intended commit, existing file,
  and in-range line anchor, and all five relative companion-document paths.
  Inspected the relevant linked entry points during source review.

The specification is currently untracked in the shared checkout, so
`git diff --check -- feature-specs/execution.md` alone is not meaningful whitespace
validation. A direct file-content whitespace check was used as well. Hashes above
identify the actual before/after artifacts independently of Git tracking state.

## Remaining gaps and cross-section disposition

The documented concurrency, fault-injection, and Go implementation/migration
backlog remains open. This review does not infer test execution from source
presence or prove server behavior on unsupported extension/version combinations.

The corrected execution claims now agree with the neighboring routing and
configuration contracts. No independent other-file defect requiring an edit was
identified. The parent was notified of the execution findings; neighboring files
were preserved.
