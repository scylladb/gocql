# Execution review round 05

- Section: `feature-specs/execution.md`.
- Round: 05; additional focus: actual test assertions, guards, and behavior-to-test mappings.
- Agent identity: `/root/review_execution_05`.
- Reference checkout: `/extra/scylladb/gocql-rust-reference-f8b0ee9`.
- Source commit independently checked with `git rev-parse HEAD`: `f8b0ee928dc0dcb475247dc82cce4e42804b0621`.
- Before SHA-256: `07c68d22e3f229ab3de9ad5a269fd5e727a7c5679b166e1706c1e829fb6ad387`.
- After SHA-256: `993bc3eaa8b8a80983f39143c5d592a3cd87c846dcad43cf72ef8ef8b1e96d90`.
- Owned and edited only `feature-specs/execution.md` and this report. No Go or Rust implementation edits; no child agents.

Read `reviews/REVIEW-INSTRUCTIONS.md`, `feature-specs/README.md`, and the entire
assigned specification. Prior round reports were not used as proof. Checked the
language-independent behavior, implementation evidence, invariants, and test
sections against the pinned implementation and test bodies. Rust/Go tests were
read, not executed; no database or CCM run is claimed.

## Verified findings and fixes

### 1. Preparation's all-shard test assertion was overstated

`scylla/tests/integration/statements/prepared.rs:751–787` distinguishes an exact
one-prepare-per-node assertion from `assert_preparation_attempted_on_all_shards`.
The latter iterates only the shard IDs already present in observed feedback and
asserts each observed count is positive. It never obtains or compares the expected
shard set, and skips the check on unsharded feedback. Its comment therefore does
not establish complete shard coverage. `test_preparation_logic:801–967` does
exercise initial failures, partial success, all-attempt failure, and eventual
success after one rejection per node.

Narrowed the test mapping to those actual assertions and added
`missing:prepare-fallback-all-shards`. Kept the implementation contract: session
`prepare_nongeneric` and `prepare_on_all` at `session.rs:1933–2024` actually use the
working-shard iterator for the fallback, await `join_all`, and validate IDs.

### 2. Metadata test coverage conflated three different cases and omitted guards

`statements/result_metadata_extension.rs:285–410,429–555` returns before its main
assertions when metadata-ID support is absent. `test_mixed_cluster` rewrites one
node's SUPPORTED advertisement, prepares through that simulated legacy node, and
executes through extension-capable nodes. It does not start mixed server versions.
The INSERT test asserts empty outgoing metadata IDs and VOID responses, not an
empty prepared schema later becoming rows.

The missing special-query mapping exists in
`ccm/result_metadata_extension.rs:99–268`: `test_result_metadata_extension` starts
an authenticated/authorized CCM cluster, checks LIST ROLES and DESCRIBE results,
observes metadata flags over repeated executions, and injects UNPREPARED. Its first
metadata assertion is conditional on whether that server returned columns during
PREPARE; it does not require PREPARE to be empty. The CCM body also skips without
the extension.

Split the mappings, added the CCM test and its conditional assertion, and made
the runtime gates and simulated mixed-capability setup explicit. Independently
checked cache/request decisions at `connection.rs:773–820,996–1225` and metadata
selection at `scylla-cql/src/frame/response/result.rs:878–934`; no change to the
documented runtime metadata contract was needed.

### 3. Result-helper coverage was absent from the unit-test map

`response/query_result.rs:529–797` checks warnings, tracing IDs, column lookup,
non-row conversion, empty/one/two-row cardinality, type-check failures, and value
decoding failures. `test_query_result_returns_self_if_not_rows:800–835` verifies
preservation of tracing/warnings in the returned original result. Added both tests
to the map rather than leaving the substantial result contract without a target.
The implementation at `query_result.rs:73–220,322–405` supports the specified
shape, borrowing, and lazy cardinality behavior.

### 4. Cache-option coverage was broader than the asserted parameters

`client/caching_session.rs:629–710` captures exactly regular consistency, serial
consistency, timestamp, and page size from two EXECUTEs sharing one preparation.
It does not assert current timeout, idempotence, tracing, history, execution-profile,
or policy settings. Restricted that mapping and added an explicit missing target
for the remaining option isolation. `add_prepared_statement_owned:200–243`
does reconstruct the full configuration, so the runtime contract remains intact.

Also added existing dry-mode tests `test_full:455–491` and
`test_execute_cached:494–595`: they check sequential eviction and repeated use
through unpaged, automatic, single-page, and batch APIs. They do not establish a
concurrent capacity bound. Checked the three zero-capacity `should_panic` tests at
`869–898`; retained their coverage and the separate concurrency gap.

### 5. Retry/speculation tests needed bounded claims and explicit gaps

Default-policy helpers at `policies/retry/default.rs:193–213` fix regular
consistency to ONE. The default budget cases test repeated errors of each kind,
not a sequence mixing unavailable/read/write errors. The downgrading test matrix
at `downgrading_consistency.rs:247–257` excludes SERIAL/LOCAL_SERIAL. The selected
cases at `433–748` use positive reply counts and only batch-log, unlogged-batch,
and simple write types. Neither suite directly tests the contrasting regular
serial behavior or independent-versus-shared mixed-error budgets. The zero-live
ALL case is covered separately by `session/retries.rs:297–338`; it does not cover
zero-reply read/write timeouts or the EACH_QUORUM exception.

Narrowed those labels, added the existing bootstrapping tests, and added focused
missing targets for the actual gaps. Runtime policy decisions were checked
against `default.rs:62–179`, `downgrading_consistency.rs:68–233`, and
`fallthrough.rs:33–38`.

The speculative unit tests at `speculative_execution.rs:263–405` use
UnableToAllocStreamId as their synthetic ignorable error and assert paused-clock
termination. The integration smoke tests at `session/retries.rs:26–170` perturb
connection-local PREPARE on idempotent requests; they do not count speculative
launches or run a non-idempotent comparison. The specification now describes
these assertions and retains a missing direct outcome/classification/idempotence
matrix. Scheduler behavior at `speculative_execution.rs:132–239`, the execution
gate at `execution.rs:447–491`, and database-error classification at
`scylla-cql-core/src/frame/response/error.rs:451–488` support the existing contract.

### 6. Pager coverage conflated behavior, assertion strength, and coordinator scope

`session/pager.rs:154–241` invokes a custom IgnoreWriteError policy for invalid CQL
and execution after table removal, then drains streams without an error. It does
not inject a write timeout, run the built-in downgrading policy, or assert zero
rows. Narrowed its mapping. `test_iter_methods_when_altering_table:296–366`
specifically expects a later-page TypeCheckError after adding a column; it does
not test continued polling after that error, and the existing continuation gap
remains explicit.

Coordinator stability/switch assertions at `pager.rs:1206–1226,1326–1353` compare
host IDs only. Added a missing target for shard stability/unsharded-node filtering
while keeping the node coverage. Added the existing token-unaware pager test
(`898–984`) and noted its system.peers cases require a peer row. Added the distinct
coordinator fallback test (`statements/coordinator.rs:282–410`), which rewrites
advertised shard IDs and skips nodes with fewer than two shards; it verifies the
chosen connection's advertised shard, not stability across later pages.

Also made absent direct coverage for unexpected unpaged continuation and
preparation/reprepare identity errors explicit. Searching those error variants
finds runtime checks and policy classification tests, not end-to-end error
injection assertions. The policy tests' synthetic error values are not proof that
the preparation/repair algorithms generate them correctly.

### 7. Test environment and observability boundaries were underspecified

Recorded `cassandra_tests` ignores for the large-batch boundary and CDC partitioner
(`statements/batch.rs:83–104`, `prepared.rs:282–351`), the metrics module's feature
gate (`session/mod.rs:8–9`), and the SELECT-without-FROM SyntaxError-only capability
probe (`statements/select_without_from.rs:17–32`).

`session/tracing.rs:169–216` obtains initial pagers but does not consume later
pages. Narrowed the tracing mapping to initial automatic pages and added a gap
for fetched-versus-consumed descriptor visibility. Its shared builder sets
200 attempts/50 ms (`utils.rs:355–369`), not the production defaults checked at
`session.rs:512–514`; documented that distinction.

History tests were relabeled as history assertions; metric counters remain mapped
to their separate metrics test. The schema-agreement tests iterate the six
query/execute entry points (`entry_point.rs:35–53,129–138`), so the description now
names that scope. The SERIAL SELECT test (`consistency.rs:485–500`) is only an
accepted-execution smoke test, now separated from request/routing-field assertions.
Verified that profile remapping is correctly mapped to `request_timeout.rs:86–138`,
while `execution_profiles.rs` checks handle selection and lazy retry sessions.

### 8. The gated C# lending pager API was only named generically

Following a cross-section note from the parent, independently inspected
`client/pager.rs:716–759`. Named and linked `QueryPager::next_column_iterator`,
both `scylla_unstable`/`unstable-csharp-rs` gates, cancellation-safe lending, and
the fresh-page flag's caller type-check responsibility. It was already covered
generically by the wrapper-driver caveat; the addition makes the stable owned
stream versus gated lending API boundary explicit. No other section was edited.

## Whole-section checks without additional runtime-contract corrections

- Statement forms and defaults: `statement/mod.rs:25–99`,
  `unprepared.rs:24–92`, `execution_profile.rs:178–207,506–528`;
  checked 5,000 page size, nonpositive rejection, false idempotence, nested serial
  override, 30-second timeout, profile precedence and remappable shared handles.
- Unprepared/prepared modes: `session.rs:906–938,1604–1771,1841–1869,2085–2190`;
  checked no-value QUERY versus local PREPARE/EXECUTE, best-effort tablet token,
  session preparation before a valued pager, serialization/deadline boundaries,
  routing inputs, and rejection of unexpected unpaged continuation.
- Preparation/binding: `session.rs:1933–2024,2258–2275`,
  `statement/prepared.rs:201–270,568–610,680–699`, `statement/bound.rs:20–62`;
  checked join-all preparation, partial success, fallback/error ordering, equal IDs,
  retained first metadata, immutable bind metadata, shared current-result snapshots,
  configuration cloning, cleared cloned trace IDs, and owned serialized bindings.
- Batch and cache: `session.rs:1294–1387`, `statement/batch.rs:301–405`,
  `connection.rs:1251–1366`, `caching_session.rs:22–243,259–324`;
  checked early size limit, first-member routing/value reuse, any prepared LWT flag,
  sequential deduplicated local preparation, repeated batch repair, concurrent
  explicit preparation, query-text-only keys, arbitrary eviction, non-coalesced
  misses, partial cache effects, and preparation outside execution deadlines.
- Attempts, recovery, and cancellation: `execution.rs:165–203,431–691`,
  `connection.rs:138–178,773–820,959–1225`; checked pool-skip versus sent-attempt
  accounting, current retry consistency, lazy per-branch sessions, timeout/history
  boundaries, losing branch cleanup, metadata/tablet feedback, timestamp reuse
  within reprepare, and lack of server cancellation/rollback.
- Paging: `pager.rs:124–405,716–827,827–1000,1080–1230,1280–1369`;
  checked per-page fresh request state, coordinator preference/filtering, first
  page on caller task, capacity-one channel with extra fetched-page retention,
  detached worker drop behavior, late errors, type-check timing, ownership,
  descriptor consumption timing, and fixed-connection internal paging.
- Results/timestamps/side effects: `query_result.rs:73–220,322–405`,
  `request_response.rs:65–141`, `timestamp_generator.rs:15–214`,
  `session.rs:482–514,1822–1839,2380–2479,2485–2529`;
  checked result shape/cardinality/borrowing, timestamp precedence and per-attempt
  generation, monotonic atomic update, post-core schema/keyspace work, trace-fetch
  completeness conditions and retry sleep, and documented defaults.
- Reviewed all listed invariants against the above paths; kept their limitations,
  including no strict LRU/capacity guarantee, no explicit-prepare deadline,
  no immediate pager-drop cancellation, and no cross-attempt timestamp guarantee.
- Go test/benchmark entries remain explicit implementation/migration backlog.

## Test coverage inventory and validation

Inspected assertion and control-flow blocks in all test files referenced before
the review: unit tests in retry/default, retry/downgrading_consistency,
speculative_execution, timestamp_generator, and client/caching_session; integration
statement tests in unprepared, select_without_from, prepared, bound,
positional_values, named_bind_markers, batch, transparent_reprepare,
result_metadata_extension, execution_profiles, consistency, request_timeout,
timestamps, and coordinator; integration session tests in retries, pager,
caching_session, tracing, schema_agreement, use_keyspace, history, and metrics.
Also inspected the newly mapped response/query_result and CCM metadata tests,
plus integration entry_point/utils/module gates and the CCM runner.

A source-reference script verified all 84 final named test functions in 29 files
exist. A separate check verified whitespace and the pinned source file/line
anchors. `git diff --check` produced no errors, but the specification is untracked,
so the explicit whitespace/reference script is the meaningful file validation.
No tests were executed. The missing targets in the specification are intended
coverage gaps, not invented test functions or claims of observed runtime failure.
The parent was informed of the preparation shard-assertion weakness and the
gated C# API alignment; no unresolved cross-section edit is required from this
round.
