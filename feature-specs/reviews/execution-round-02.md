# Execution reference review — round 02

- Section: `feature-specs/execution.md`
- Round: 02 — defaults, exact conditions, limits, ordering, and fallback boundaries
- Agent identity: `/root/review_execution_02`
- Source checkout: `/extra/scylladb/gocql-rust-reference-f8b0ee9`
- Source commit, verified with `git rev-parse HEAD`: `f8b0ee928dc0dcb475247dc82cce4e42804b0621`
- Before SHA-256: `2c2758aeca79d6478503a458f46b5d02cf57df6236bc3b2fa1662bb94b68f9c7`
- After SHA-256: `e94e9a23632873066a6202d3b5738f480d37df19d9f0adc4cfdb1c4ea0f2b530`

Read the review instructions, README, and complete execution specification.
Independently traced the pinned implementation. No earlier report was used as
proof. Modified only the assigned specification and this report; no Go or Rust
implementation changes and no child agents.

## Source inspected

Paths below are relative to the pinned Rust checkout; ranges describe the source
read, not executed test coverage.

| Files / key symbols | Relevant source |
| --- | --- |
| `statement/mod.rs`: `StatementConfig`, `PageSize`; unprepared setters | `scylla/src/statement/mod.rs:25–103`; `statement/unprepared.rs:24–100` |
| Profile defaults and shared handle remapping | `scylla/src/client/execution_profile.rs:178–210,488–534` |
| `PreparedStatementSharedData`, clone, constructor, binding, token extraction, cache handles | `scylla/src/statement/prepared.rs:143–269,342–426,654–725`; `statement/bound.rs:20–150` |
| Batch defaults/configuration and `peek_first_token` | `scylla/src/statement/batch.rs:20–145,290–350`; `scylla-cql/src/frame/request/batch.rs:56–145` |
| Public execution modes, batch validation, unprepared local prepare, explicit preparation/fallback, prepared routing, batch preparation | `scylla/src/client/session.rs:1294–1375,1604–1775,1841–2035,2050–2230,2258–2276` |
| Request parameter resolution, timeout wrapper, target acquisition, retry loop | `scylla/src/client/execution.rs:58–197,430–709` |
| Prepare/reprepare, query/execute timestamp and metadata selection, resend, batch repair/preparation | `scylla/src/network/connection.rs:723–821,929–1231,1252–1370` |
| Working-connection iterator behavior | `scylla/src/cluster/state.rs:780–836` |
| Caching APIs, miss/evict/insert, builder and defaults | `scylla/src/client/caching_session.rs:1–405` |
| Default, fallthrough, downgrading retry decisions | `scylla/src/policies/retry/default.rs:62–190`; `retry/fallthrough.rs:1–42`; `retry/downgrading_consistency.rs:68–258` |
| Speculative timer and branch classification | `scylla/src/policies/speculative_execution.rs:19–251`; `scylla-cql-core/src/frame/response/error.rs:451–496` |
| `PagingExecutor`, first/later page processing, fixed-connection pager, channel handoff, typed stream | `scylla/src/client/pager.rs:124–430,532–825,827–1065,1090–1405` |
| Result shape/cardinality and response metadata snapshot choice | `scylla/src/response/query_result.rs:73–195,295–510`; `response/request_response.rs:1–130`; `scylla-cql/src/frame/response/result.rs:874–928` |
| Timestamp generators, side effects, tracing retrieval and defaults | `scylla/src/policies/timestamp_generator.rs:15–220`; `scylla/src/client/session.rs:476–515,1822–1838,2380–2481,2496–2531` |

## Verified findings and fixes

1. **The metadata invariant promised freshness that the legacy cache opt-in cannot
   provide.** `Connection::calculate_cached_metadata_params` at
   `connection.rs:1032–1102` enables skipping metadata on nonempty cached metadata
   when the setting is true even without the extension. Reprepare returns without
   updating metadata when no ID is present (`connection.rs:793–795`). The decoder
   uses the cached snapshot on `NO_METADATA`
   (`scylla-cql/src/frame/response/result.rs:881–889`), with no schema comparison.
   The prepared-statement contract explicitly describes possible decoding errors
   and silent corruption (`prepared.rs:175–185`). Added that limitation and
   changed the invariant to guarantee snapshot retention, not schema freshness.

2. **Result-metadata refresh was documented without the immutable bind-metadata
   boundary.** `PreparedStatementSharedData.metadata` is an ordinary
   `PreparedMetadata`, while only current result metadata has `ArcSwap`
   (`prepared.rs:212–219`). `serialize_values` uses the unchanged prepared
   metadata (`prepared.rs:684–691`), and `Connection::reprepare` only updates
   result metadata (`connection.rs:773–821`). Added that bind-type schema changes
   may require a fresh explicit preparation and do not update already bound
   bytes. Added explicit test gaps for this boundary and legacy metadata staleness.

3. **Batch preparation lacked its ordering and failure boundaries.**
   `Session::prepare_batch` clones the batch and uses `try_join_all` over members
   (`session.rs:2258–2276`); equal query strings are separate member futures,
   and one member error ends the join and drops unfinished futures. In contrast,
   connection-local `prepare_batch` first builds a `HashSet`, then awaits each
   preparation in a sequential loop (`connection.rs:1323–1357`). Added the
   concurrent/no-deduplication/fail-fast explicit path and the
   sequential/deduplicated/unspecified-order implicit path. Clarified that
   dropping pending member futures does not undo already sent preparation
   requests. Added a dedicated partial-failure test gap.

4. **The batch limit needed an API-specific validation boundary.**
   `Session::batch` checks `len() > u16::MAX` before taking a snapshot or
   serializing values (`session.rs:1302–1311`). `CachingSession::batch` prepares
   unprepared members before calling `Session::batch`
   (`caching_session.rs:145–165`). Qualified “before dispatch” as before BATCH
   dispatch and documented that cached execution may send PREPARE first. Added a
   gap for this wrapper ordering rather than attributing it to the existing
   large-batch test.

5. **Speculative launch timing and exhaustion detection were too broad.**
   `execute` reads the maximum and interval once, resets one timer using that
   interval, and records ignorable errors without launching immediately
   (`speculative_execution.rs:201–251`). Its shutdown condition is a branch
   returning `None`. The core returns `None` only when that branch consumed no
   target; a branch exhausting targets after failures returns its last error
   (`execution.rs:562–709`). Added the timer snapshot, delayed replacement, and
   distinction between an exhausted branch and a new branch observing an empty
   plan. Narrowed the matching test description to its actual empty-plan signal.

6. **The caching wrapper's metadata preference default was absent.** Both legacy
   constructors set `use_cached_metadata: false`
   (`caching_session.rs:57–90`), as does `new_shared`
   (`caching_session.rs:321–329`). Added the explicit false default and kept the
   negotiated-extension override separate.

7. **Local unprepared execution's partitioner boundary needed to remain visible.**
   Parent supplied a cross-section concern, independently checked here:
   connection-local preparation constructs a statement with the default
   partitioner (`connection.rs:763–770`, `prepared.rs:248–269`), and unprepared
   execution calculates the hint directly (`session.rs:1718–1733`). Only explicit
   session preparation installs the metadata-selected partitioner
   (`session.rs:2018–2022`). Added the local Murmur3 limitation; routing owns its
   broader consequences. No other section was edited.

## Checked behavior retained

- Page size is 5,000 and strictly positive; unpaged calls omit page size and
  reject continuations. Empty automatic pages do not end the stream by themselves.
- Profile/statement precedence, the three serial-consistency states, 30-second
  timeout inheritance, LocalQuorum/LocalSerial defaults, disabled speculation,
  tracing/idempotence defaults, and shared handle remapping match the source.
- Empty-value unprepared execution sends QUERY; nonempty-value execution prepares
  locally inside an attempt; automatic paging with values explicitly prepares
  beforehand. Explicit preparation waits for all attempts in a pass, tolerates
  partial success, checks IDs, and falls back after a pass error; failure to obtain
  the first iterator returns before fallback. It has no request-timeout wrapper.
- Prepared and bound execution reuse serialized bytes; prepared clones share
  identity/cache, copy configuration, and clear preparation trace IDs.
- EXECUTE repair resends once and validates the ID; BATCH repair loops without a
  fixed count. Metadata/tablet updates apply on the EXECUTE response paths;
  response-ID and same-ID/empty-cache updates differ from reprepare updates.
- First-member batch token/table routing and any-prepared-member LWT detection
  match implementation. Value-count mismatch and the exact 65,535-member maximum
  remain explicit, as does first-member serialization reuse.
- Query-text-only cache keys, caller option reconstruction, capacity 128,
  zero-capacity panics, non-LRU eviction, redundant concurrent prepares, and
  non-atomic capacity checks were retained.
- Default retry budgets, idempotence exceptions, serial regular-consistency
  exclusion, fallthrough behavior, downgrade counts/write kinds, current
  consistency passed to retries, and unchanged routing consistency were checked.
- Pool acquisition failures skip targets before query-attempt metrics; deadlines
  enclose attempts/repair/retries/speculation but exclude preceding explicit
  preparation, ordinary prepared serialization, typed decoding, and side effects.
- Pagers retain settings/cluster state, reset per-page retry/deadline state,
  prefer the previous coordinator, use a capacity-one channel with fetch-before-
  send, and do not immediately cancel an in-flight fetch on drop. Owned typed rows,
  per-page type checking, consumption of error rows, consumed-page descriptors,
  and discarded page warnings remain described.
- Result cardinality helpers are lazy; timestamps use explicit/generator/server
  precedence and generator calls occur per attempt while internal repair reuses
  the timestamp. Agreement defaults (200 ms / 60 s / enabled) and trace-fetch
  defaults (ten attempts / 3 ms / ONE), including the last incomplete-attempt
  sleep, match the implementation.
- Stable versus gated wrapper APIs and the fixed-connection pager's separate
  timeout/policy/result constraints were checked in their definitions.

## Test claims and validation

Resolved all **75 named Rust test references** in the specification to actual
function definitions in the pinned checkout. Checked all pinned source-link paths
and line bounds, and checked for trailing whitespace. These are static checks,
not Rust or Go test execution.

Read substantive assertions in retry-policy tests, all three named speculative
scheduler tests, timestamp tests, `test_parameters_caching`, the three
zero-capacity tests, batch count/mismatch tests, preparation fallback and metadata
flag tests, metadata-extension tests, timeout/consistency/profile tests,
SELECT-without-FROM tests, pager consistency/plan/coordinator tests, and separate
paging metrics tests. The remaining named targets were verified for existence;
their presence alone does not establish every adjacent prose claim.

In particular, the large-batch test asserts success at 65,535 and rejection at
65,536 using an already prepared batch; it does not cover cached-wrapper
preparation order. Preparation fallback tests assert one request per node and
all-shard fallback but do not establish cancellation of explicit batch member
preparation. Metadata-extension tests check schema/ID updates and mixed-version
behavior, not a freshness guarantee for the legacy opt-in. Speculative tests
signal exhaustion by returning `None` and assert timer-based completion times.

No database, Rust, Go, race, or allocation tests were run. Existing missing-test
markers remain; four additional focused gaps were added. In-flight cancellation,
cache races, schema-change bind metadata, and wrapper ordering remain source-
derived behavior pending dedicated runtime tests.
