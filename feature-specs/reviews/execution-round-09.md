# Execution reference review — round 09

- Section: `feature-specs/execution.md`.
- Round: 09, adversarial edge cases and source quirks, with a whole-section audit.
- Agent identity: `/root/review_execution_09`.
- Reference checkout: `/extra/scylladb/gocql-rust-reference-f8b0ee9`.
- Source commit, verified with `git rev-parse HEAD`:
  `f8b0ee928dc0dcb475247dc82cce4e42804b0621`.
- Before SHA-256:
  `94c51a2a9d64955d136e62875b410aae85d3150cc92d78b9db95aae5c8ba48aa`.
- After SHA-256:
  `c80cbee00f20dd66dd27a48de791499fe6e829648b6bed1ebabc0e31b0f5ef7b`.
- Scope: read the review instructions, README, complete execution specification,
  and relevant pinned source; edited only the assigned specification and this
  report. No implementation files were changed and no subagents were spawned.

## Findings and fixes

### 1. Count-based consistency selection is not always a downgrade

`scylla/src/policies/retry/downgrading_consistency.rs:86-107` selects THREE/TWO/ONE
from `known_ok`, with `previous_cl == EachQuorum` also selecting ONE when the count
is below one. It does not cap the new level at the original level. The unavailable
branch at lines 151-159 calls this helper with the server's `alive` value.
`scylla-cql-core/src/frame/response/error.rs:54-86` decodes the relevant error
counts with signed `read_int`, without rejecting negatives.

The specification previously named only the zero-count EACH_QUORUM exception and
left the policy's name suggesting that changed consistency could only weaken.
It now includes negative counts and the concrete ONE-to-TWO unavailable case.
The test list distinguishes existing positive-count unlogged-batch coverage from
missing negative-count and unavailable-increase assertions.

### 2. Single EXECUTE does not validate the ID in an UNPREPARED error

`scylla/src/network/connection.rs:1176-1190` binds `statement_id` from the error
only for logging and reparses the original executed statement. The ID check in
`reprepare` at lines 783-790 compares the later PREPARED ID with the original ID.
In contrast, the batch loop at lines 1298-1324 searches for the error's ID among
prepared batch members and rejects an absent match.

The specification now separates these two identity checks. Its test backlog no
longer suggests that single EXECUTE rejects an unrelated error ID, and it adds
the explicit single-versus-batch boundary case.

### 3. Automatic paging has no continuation progress or whole-stream limit

`PagingExecutor::process_first_page` and `process_next_page` in
`scylla/src/client/pager.rs:377-503` assign returned continuation state without
comparing it with previous state. `query_remaining_pages` at lines 212-278 loops
while continuations remain. `fetch_one_page` at lines 334-376 supplies a fresh
request timeout for each page, while `QueryPager::poll_fill_page` at lines 760-777
keeps seeking a nonempty page.

The specification now states that repeated tokens can refetch rows and an endless
sequence of successful empty pages can keep a row poll pending indefinitely.
It explicitly distinguishes page deadlines from a total iteration deadline or
page-count cap. A repeated-continuation/progress regression remains missing.

### 4. Absent requested result metadata can fall back to empty metadata

`RawMetadataAndRawRows::deserialize` in
`scylla-cql/src/frame/response/result.rs:773-824` accepts a nonnegative advertised
column count with NO_METADATA, provided the other flags are valid. Its
`deserialize_metadata` method at lines 877-933 uses `ResultMetadataHolder::mock_empty`
when NO_METADATA is set and no cache was supplied, then separately decodes the
row count. It does not reject this case merely because metadata was requested.
`RawRowIterator::next` in `scylla-cql-core/src/deserialize/result.rs:56-74` and
`RawRowLendingIterator::next` in `scylla-cql/src/deserialize/result.rs:53-84`
traverse columns from the chosen specifications. Empty specifications leave
payload bytes unexamined. The tuple implementation in
`scylla-cql-core/src/deserialize/row.rs:239-303` accepts the zero-column row type.

The specification now records this fallback and its unused-payload consequence,
instead of allowing the metadata-request rule to imply response validation.
Pinned evidence and an explicit missing adversarial-response test were added.
The parent was notified of the corresponding serialization/protocol boundary;
that section was not edited.

### 5. NULL/UNSET partition-key entries differ from missing indexed values

`PreparedStatement::extract_partition_key_and_calculate_token` in
`scylla/src/statement/prepared.rs:373-387` calls `PartitionKey::new`. At lines
804-824, a missing indexed entry errors, but an entry other than `RawValue::Value`
is left absent from the effective key. `PartitionKey::iter` at lines 828-831
flattens those omissions; encoding and hashing at lines 835-878 use the remaining
components, including empty input when all are omitted.

The specification's broad missing-component wording now names missing indexed
serialized values precisely and explicitly documents NULL/UNSET omission. The
positional-values test description was narrowed to its actual non-key NULL/UNSET
cases, with missing partition-key execution coverage recorded separately. The
existing routing section already documents this behavior, so the execution text
now points to its component-encoding rules.

## Whole-section checks and inspected implementation

The following checks did not require further edits:

- `scylla/src/statement/mod.rs:26-99`, `statement/bound.rs:21-151`, and
  `statement/prepared.rs:201-269,436-470,558-704`: defaults, positive signed page
  sizes, owned serialized binding, option overrides, clone-local configuration,
  omitted clone tracing IDs, shared immutable bind metadata and mutable result
  metadata, and snapshot column guards.
- `scylla/src/client/execution_profile.rs:182-218,446-539` and
  `client/execution.rs:150-188`: profile defaults, statement precedence, explicit
  serial omission, timeout inheritance, shared handle remapping, and snapshot
  resolution. Policy objects retain their own mutable internal state.
- `scylla-cql-core/src/frame/request/query.rs:9-113`: start state versus an empty
  byte continuation, opaque raw-byte restoration, and separate completion state.
- `scylla/src/client/session.rs:1603-1771,1841-1855,1933-2030,2048-2072,2085-2210`:
  unpaged validation after success, empty/nonempty unprepared dispatch, local
  versus explicit prepare, preparation fan-out/fallback, first-success metadata
  and error selection, serialization and token timing, and Python's gated
  preserialized execution path without stable-unpaged continuation rejection.
- `scylla/src/client/session.rs:1294-1380,2258-2278` and
  `statement/batch.rs:308-353`: statement-count checking before session dispatch,
  first-prepared-member routing and serialization caching, any-prepared-member
  LWT flag, and concurrent explicit preparation with partial effects on failure.
- `scylla/src/network/connection.rs:773-823,996-1222,1252-1382`: transparent
  reprepare, one EXECUTE resend versus repeated batch repairs, preserved resend
  timestamp and values, metadata-ID extension choices, non-destructive reprepare
  checks, non-atomic metadata replacement, and sequential deduplicated local
  batch preparation. Retained response snapshots survive later cache stores.
- `scylla/src/client/caching_session.rs:50-259`: nonzero capacity, query-text-only
  keys, current-call option reconstruction, non-coalesced misses, arbitrary
  concurrent eviction, non-strict capacity, independent returned prepared
  handles, and non-transactional cached batch preparation.
- `scylla/src/client/execution.rs:345-698`: lazy branch-local retry sessions,
  connection-selection skips without attempt callbacks, current consistency
  passed to retries, ignored errors, request timeout/history boundaries, and
  cancellation by dropping unfinished futures.
- `scylla/src/policies/retry/default.rs:62-187`,
  `retry/downgrading_consistency.rs:68-247`, and `retry/fallthrough.rs:34-40`:
  regular serial-consistency special handling, idempotence exceptions, separate
  versus shared retry budgets, error matrices, and fallthrough behavior.
- `scylla/src/policies/speculative_execution.rs:23-245` and
  `scylla-cql-core/src/frame/response/error.rs:451-489`: metrics feature gating,
  interval lookup/fallback, additional-branch counting, timer-driven launches,
  decisive versus ignorable outcomes, waiting for existing branches after an
  empty-plan branch, and dropping losing branches.
- `scylla/src/policies/load_balancing/plan.rs:78-168`: lazy plan consumption and
  fallback, consistent with shared speculative consumption and fresh page plans.
- `scylla/src/client/pager.rs:134-503,705-811,827-1030,1100-1248,1270-1381`:
  fixed snapshots/policies and tablet hints, page-local consistency reset,
  coordinator preference and unsharded filtering, eager first-page fetch,
  detached worker/channel ownership, terminal background fetch errors, empty
  pages, consumed-page descriptors, type-check/decoding timing, and cancellation
  of a pending row wait without consuming a row.
- `scylla/src/network/connection.rs:138-175,2045-2068` and
  `scylla/src/cluster/worker.rs:303-329`: orphan notification, awaited tablet
  feedback enqueueing, dropped pending sends, and independent cluster-worker
  publication. No server-side cancellation or rollback is implied.
- `scylla/src/response/query_result.rs:30-153,213-238,322-416`,
  `response/request_response.rs:33-151`, and `response/coordinator.rs:30-62`:
  result shape, warnings/tracing, exact first-name lookup, lazy typed/cardinality
  access, actual connection address/shard, and raw wrapper-driver access gates.
- `scylla/src/policies/timestamp_generator.rs:16-157` and
  `client/session.rs:482-514,1822-1838,2380-2395,2432-2482,2496-2537`:
  timestamp generation and scope, default agreement/trace settings, post-success
  keyspace/schema effects outside the core deadline, concurrent trace queries,
  deferred event decoding, incomplete-trace retries, and final-attempt sleep.

## Test claims and validation

All 85 test references in the section resolve to 84 distinct function symbols in
29 files in the pinned checkout. Inspected assertion excerpts across the named
statement and session integration targets, plus detailed policy, speculation,
cache, timestamp, reprepare, pager, and result implementation/test paths.

Confirmed the documented coverage limits: the unprepared opcode tests are smoke
tests; preparation's shard helper does not establish the complete expected shard
set; coordinator-stability assertions compare host IDs; speculative smoke tests
do not directly count branches or contrast non-idempotence; ordinary metadata
tests do not inject missing metadata; positional NULL/UNSET assertions target
non-key columns; and reprepare tests exercise schema invalidation rather than
an unrelated UNPREPARED error ID. Existing downgrading write-timeout cases do
exercise count-derived levels above the request level, so this was not marked
wholly untested.

The new negative-count, unavailable-increase, unexpected-ID, repeated-token,
missing-metadata, and NULL/UNSET-key cases remain explicit test gaps. Existing
concurrency/cancellation/cache and partial-success gaps remain intact. Go tests
and allocation benchmarks remain proposals, not implemented or passing claims.

A local structural check verified all 66 pinned source links point to existing
files and in-range line anchors, all referenced test symbols exist, and the
specification has no trailing whitespace. No Rust or Go tests, database tests,
or benchmark runs were performed. The findings are source-derived; adversarial
wire examples were not executed against a proxy or database in this round.
