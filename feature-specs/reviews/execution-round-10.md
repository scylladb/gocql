# Execution review, round 10

- Section: `feature-specs/execution.md`.
- Round: 10, final comprehensive fidelity and completeness audit.
- Independent agent: `/root/review_execution_10`.
- Reference: `/extra/scylladb/gocql-rust-reference-f8b0ee9`, HEAD
  `f8b0ee928dc0dcb475247dc82cce4e42804b0621`; tracked working tree clean.
- Before SHA-256: `c80cbee00f20dd66dd27a48de791499fe6e829648b6bed1ebabc0e31b0f5ef7b`.
- After SHA-256: `c960b754e6f15693e711eed68349bc2651db3fe0476a4f608c513d93e4de3091`.
- Read the complete review instructions, section, and feature-spec README. Prior
  review reports were not used as proof. No child agents or implementation edits.

## Findings and fixes

1. **Batch preparation does not independently guarantee immediate delivery of
   the first completed member error.** `Session::prepare_batch`
   (`scylla/src/client/session.rs:2258–2278`) and
   `CachingSession::prepare_batch` (`scylla/src/client/caching_session.rs:171–188`)
   both await `futures::try_join_all`. They do not race errors through a separate
   completion-order channel. The previous statement that a member error
   “returns immediately” overstated this behavior.

   I followed that dependency rather than relying on its public comment:
   the locally resolved `futures-util` 0.3.34 implementation selects
   `FuturesOrdered::try_collect` for its large-input path
   (`src/future/try_join_all.rs:135–143,189`);
   `src/future/join_all.rs:35` defines its current small-input threshold as 30.
   `src/stream/futures_ordered.rs:195–216` queues a completed result until earlier
   input positions are ready, including an error result. Thus, in this dependency
   resolution, an earlier pending member can delay a later member's error for a
   batch larger than that threshold. `TryCollect::poll`
   (`src/stream/try_stream/try_collect.rs:45–53`) can only propagate the error once
   that ordered stream yields it. The registry files inspected are under
   `/home/dkropachev/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/futures-util-0.3.34/`.

   The reference declares `futures = "0.3.6"` at `scylla/Cargo.toml:120`, and
   `.gitignore:4` excludes Cargo.lock. The local untracked/ignored lock resolves
   futures-util 0.3.34; its threshold is therefore not a guarantee pinned by the
   driver commit. The spec now describes dropping outstanding futures when the
   combined operation returns an error, removes the unconditional immediacy
   promise in both batch paths, and explicitly separates dependency scheduling
   from the pinned driver contract. The existing partial-failure test gap now
   includes delayed error delivery. No dependency version or threshold was added
   as a language-independent driver requirement.

2. **The test catalogue omitted direct cached-batch preparation coverage.**
   `scylla/tests/integration/session/caching_session.rs:test_batch`
   (139–262) checks unprepared, mixed, and fully prepared batches through cached
   execution and explicit cached preparation; it reads back the expected rows,
   checks prepared member forms, and rejects malformed members. Added this exact
   target alongside the existing cache traffic tests. It does not establish
   concurrent partial-failure ordering, which remains explicitly missing.

## Implementation inspected and checked behavior

The execution assignments in `rust-source-catalog.json` were checked against the
section's breadth. The following are the actual production sources/symbols
inspected, including shared boundaries outside that assignment:

- `scylla/src/statement/mod.rs:26–103`, `unprepared.rs:15–240`,
  `prepared.rs:55–105,201–870`, `bound.rs:21–149`, and
  `batch.rs:22–115,308–423`: default page size and positive validation;
  option inheritance/reset; serial omission versus inheritance; binding owns
  serialized values; prepared clone/configuration/cache/trace-ID behavior;
  immutable bind metadata; partition-key extraction and NULL/UNSET omission;
  first-member batch serialization reuse. Public prepared/bound introspection
  and mutable result-column guards were checked as well.
- `scylla-cql-core/src/frame/request/query.rs:9–94`: completion is distinct from
  start, and empty continuation bytes are still present bytes. Exported tokens
  carry no query/session identity or validation.
- `scylla/src/client/execution_profile.rs:182–210,232–526`:
  LOCAL_QUORUM/LOCAL_SERIAL, 30-second default, immutable profiles,
  shared remappable handles, independent handle creation, builder/profile
  snapshots, labels, and no default speculation.
- `scylla/src/client/session.rs:1294–1388,1601–1745,1822–2278`:
  session batch limit precedes preparation; token/table use the first prepared
  member and conditional status uses any prepared LWT member; stable unpaged
  validation is after core execution; values select local PREPARE/EXECUTE versus
  direct QUERY; automatic paging with values uses explicit preparation; explicit
  prepare waits for all connections, selects the first success/error in input
  order, checks IDs, and performs its separate fallback; ordinary prepared
  serialization/token work precedes the deadline. Python preserialized execution
  returns continuation even when its paging flag is disabled. C# preserialized
  paging is feature-gated.
- `scylla/src/client/caching_session.rs:23–422`: query-text-only keys;
  per-call option reconstruction; default capacity 128 and zero rejection;
  arbitrary iterator eviction; preparation before eviction/insertion;
  concurrent miss/capacity and retained-handle boundaries; shared session
  construction; all-prepared batch bypass; partial cache changes are not rolled
  back on combined preparation failure.
- `scylla/src/network/connection.rs:138–176,723–820,960–1368,2045–2068`:
  repair preserves ID, request values/consistency/timestamp/state/tablet byte;
  single EXECUTE repairs its original statement regardless of the error ID;
  batch repair finds the named member and can repeat; batch implicit preparation
  deduplicates selected query strings and prepares them sequentially;
  metadata-ID negotiation and empty-cache decisions; row/reprepare update
  conditions; feedback precedes database-error interpretation and awaits queue
  send. Reprepare updates neither bind metadata nor prepared routing identity.
- `scylla-cql/src/frame/response/result.rs:877–932`,
  `scylla-cql/src/deserialize/result.rs:26–109`, and
  `scylla-cql-core/src/deserialize/result.rs:13–110`: retained metadata snapshots,
  uncached NO_METADATA fallback, lazy typed decoding, raw row framing/count
  progress, and lack of validation of unused row payloads.
- `scylla/src/client/execution.rs:101–702`: shared speculative plan, resolved
  settings, actual coordinator, connection-pick failure bypass, lazy retry
  sessions, current consistency versus original routing consistency, success and
  failure feedback, ignored-write handling, timeout/history/metrics ordering,
  cancellation, and lack of a global retry-count bound.
- `scylla/src/policies/retry/{default.rs:62–179,
  downgrading_consistency.rs:68–233,fallthrough.rs:23–38,
  retry_policy.rs:10–104,mod.rs}`:
  complete policy branches and serial guards; separate default budgets versus
  shared downgrade budget; EACH_QUORUM zero/negative exception; fallback can
  increase consistency; reset is public but unused by this core.
- `scylla/src/policies/speculative_execution.rs:20–240` and
  `scylla-cql-core/src/frame/response/error.rs:451–487`: optional metrics
  percentile policy, fixed interval and launch count captured once, delayed
  replacement launch, empty-plan handling, decisive result cancellation, and
  database/transport error classifications distinct from retry policy decisions.
- `scylla/src/client/pager.rs:134–500,539–690,724–804,827–1401`:
  fixed settings/cluster snapshot; fresh plan, deadline, and retry state per
  page; stable actual coordinator node/shard preference and duplicate filtering;
  first page before worker creation; channel capacity one with an additional
  fetched page possible; detached-worker drop semantics; repeated token and
  empty-page limitations; first/later non-row differences; no restart after
  terminal page error; consumption-time descriptors; typed stream ownership and
  repeated checks; discarded warnings; fixed-connection internal pager.
- `scylla/src/response/{query_result.rs:19–423,
  request_response.rs:17–142,coordinator.rs:12–59,mod.rs}`:
  rows/non-rows, warnings/traces, conversion error retaining the original result,
  cardinality operations, exact first matching name lookup, row/byte counts,
  actual address/shard and node identity, and unstable wrapper accessors.
- `scylla/src/policies/timestamp_generator.rs:16–211` and connection request
  constructors: explicit/generator/server precedence, per-attempt generation,
  repair timestamp reuse, simple wall-clock and monotonic atomic generators,
  warning configuration and pre-epoch behavior.
- `scylla/src/client/session.rs:482–514,2380–2392,2427–2481,2496–2529`:
  side effects outside the execution deadline/history success; agreement and
  tracing defaults; concurrent trace reads, incomplete-result retries and final
  sleep, deferred event decoding, and no complete-event-set guarantee.
- `scylla/src/errors.rs:45–152,907–1045`: preparation/request/attempt/pager
  distinctions and conversions agree with the stated failure boundaries.
  `scylla/src/cluster/worker.rs:303–343` independently confirms asynchronous
  tablet publication after queue delivery.

All invariants were checked against these paths. The companion sections were
cross-checked at their execution boundaries: routing snapshots/tablet feedback,
transport cancellation, schema/keyspace side effects, row ownership/metadata,
and profile/history/metrics lifetimes. No additional cross-section correction
was identified. The shared core does not imply that explicit preparation,
post-success side effects, or lazy application decoding fall inside its timeout.

## Test evidence and validation

No Rust/Go tests, database, proxy, or dependency probe were executed. The following
are source/assertion checks, not claims of passing test runs:

- Default/downgrading unit matrices, speculative empty-plan and elapsed-time
  regressions, timestamp thread/uniqueness assertions, result helper assertions,
  and cache proxy/count/capacity tests were inspected in their production-source
  test modules. The catalogue correctly leaves serial/mixed budgets, complete
  terminal variants, race outcomes, and non-wire option isolation missing.
- Statement integration assertions were checked for unprepared opcode smoke
  limitations; SELECT-without-FROM capability handling and typed results;
  preparation fan-out, stored settings, partitioner, and metadata; bound modes
  and stored overrides; positional/named values; batch size/mixed types/LWT and
  counter restrictions; transparent reprepare; metadata-ID changes/mixed
  advertisement; profile/consistency/timeout settings; timestamps; and actual
  coordinator/shard fallback. The preparation shard helper at
  `statements/prepared.rs:765–788` only examines observed shards. Counter and
  textual multi-table smoke cases at `statements/batch.rs:574–657` do not read
  back values, unlike the combined batch-kind helpers.
- Session integration assertions were checked in `retries.rs`, `pager.rs`,
  `caching_session.rs`, `tracing.rs`, `schema_agreement.rs`, `use_keyspace.rs`,
  `history.rs`, and `metrics.rs`: wire consistency downgrade/reset, per-page
  timeout/plan/coordinator behavior, ignored-error history, cache traffic,
  trace lookup, side effects, and separate page-mode counters. Existing cautions
  remain accurate: speculative smoke tests do not count branches; node stability
  does not prove shard stability; metadata-extension tests may return early;
  traced pager helpers only inspect initial page IDs.
- Inspected `tests/integration/ccm/result_metadata_extension.rs` assertion and
  capability paths. Its first metadata expectation adapts to the PREPARED
  response, and the later protocol assertions test suppression and reprepare.
  `tests/integration/utils.rs:368–369` sets trace fetching to 200 attempts/50 ms;
  `session/mod.rs:8–9` gates metrics tests; `scylla/Cargo.toml:35` has no default
  features. These do not establish production-default readiness or portability.
- After edits, a local script validated all 67 pinned source-link paths/line
  bounds, all 86 named Rust test references, and all relative links/anchors in
  this section. A direct trailing-whitespace/final-newline check passed for both
  owned files; `git diff --check` also reported no issues (these files are
  currently untracked, so the direct check is the substantive whitespace check).

## Remaining uncertainty and gaps

The actual dependency version of a future build is not fixed by the driver
revision; the local futures-util scheduling observation must not be generalized
to every compatible version or target. The document now makes the weaker
supported guarantee. No deterministic batch error-ordering test is claimed.

The section's explicit missing tests remain backlog, including metadata/cache
races, caller/pager cancellation, tablet queue saturation, identity-repair
errors, preparation deadlines/failure ordering, and result-stream edge cases.
Database-dependent expectations were checked in source only. Future Go tests,
migration, race checks, and allocation benchmarks remain requirements, with no
implementation or coverage asserted by this review.
