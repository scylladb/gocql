# Step 10: remove legacy execution and serialization

Owner: `/root/step10_remove_legacy`; branch: `v2-10-remove-legacy`.
Accepted predecessor: `f542da9507d9a6bd05a9e3278ff71bd3f7201a3a`.
The assigned removal/migration gate is complete. Exact commands, statuses,
retained log hashes, source snapshots, native receipts and limitations are in
[step10-verification.json](../evidence/step10-verification.json).

## Removed paths and migrated callers

The product no longer declares `Query`, `Iter`, mutable `Batch`, `QueryInfo`,
`ExecutableQuery`, callback binding, `Marshal`/`Unmarshal`, their custom
marshaler interfaces, `Scan`/`MapScan`/`SliceMap`/`RowData`, named/unset legacy
bindings, or the old retry/speculation interfaces. Old public cluster/session
constructors are internal shared settings/lifecycle helpers; public callers use
`Config`/`Connect`. The old session/connection execution methods, executor,
prepared/routing caches, reflection factories/serializers and query-value pools
are deleted. Remaining protocol type descriptors describe metadata and cannot
infer or construct Go destinations.

All examples compile through explicit codecs, prepared/unprepared bindings,
typed rows, caller destinations, conditional batches and paging. Custom value
and UDT examples use explicit typed plans. The optional hostpool policy now
implements `RoutingPolicy`/`QueryPlan`/attempt observation, including finite
host traversal and strict targets. Recorder/replayer dialers implement the typed
`DialTarget` contract and honor the explicit local source address. Cloud and
tracing continue through the already-migrated typed paths. Configuration
reporting reads the current typed profile, retry/routing policy and locality,
including explicit profile replacement.

Application QUERY/EXECUTE/BATCH writes use the canonical span writer. The only
remaining production QUERY-frame builder is the parameter-free `USE` needed
while opening a connection: CQL, ONE, zero flags. It has no values or binding
adapter. Already-encoded independent protocol oracles live only in
`protocol_request_fixture_test.go`; oracle self-tests do not establish product
coverage. Actual typed writer tests continue comparing bytes against them.
Schema fixtures now contain framed native cells and use explicit canonical
codecs; their fixture object has no iteration/scanning/execution behavior.

## Streaming boundary and behavioral preservation

Both forbidden buffered scalar adapters are removed: ordinary streaming scalar
decode and validation of ignored/encoded scalar values. Native streaming scalar
plans never create `ValueReader`, invoke buffered/dynamic decoders, or enter
`Query`/`Iter`. Fixed scalar conversion uses at most 16 bytes of scratch. Duration
readers feed one shared native varint state and range/sign validator; the
streaming path reads incrementally. Keeping the buffered cursor concrete also
preserves zero-allocation duration decode in the smoke.

Ignored scalar and encoded-output validation compile those same native plans.
Variable values and nested children still target final seeded caller storage;
capacity checks, reset semantics, bounded discard/metadata/integrity/transform
storage, exclusive pool reservation and no replay after output remain. The
[boundary audit](../evidence/step10-streaming-boundary-audit.json) distinguishes
these permitted transformations from forbidden payload gathers. AST/export and
streaming call-site audits have 0 findings. Focused parity checks cover malformed
lengths, ranges, duration overflow/sign/trailing bytes, existing nonminimal
varint behavior, every short read and truncated scalar prefix; existing direct
read/backing/reset/skip/capture/cancellation tests pass in the full race suite.

All 29 original file hashes and all 174 original body hashes match the immutable
baseline and accepted step09 source. Every original identity remains mapped.
173 translated test token streams are unchanged, ignoring comments and optional
semicolons. The sole changed test, `TestSessionMetadataAPIs`, calls existing
`v2TabletsSupported` on its own typed session instead of the deleted global
legacy probe; all existing assertions remain. Complete original/current test
bodies and reachable helper assertions are retained in
[step10-assertion-source.json](../evidence/step10-assertion-source.json), with the
[identity proof](../evidence/step10-baseline-preservation.json) and
[current/historical outcomes](../evidence/step10-test-mapping.json) separate.

The [test disposition inventory](../evidence/step10-test-disposition.json)
records 290 removed test/example/benchmark declarations and 177 deleted source
files against accepted step09. Supported scalar/container wire values,
null/unset/empty distinctions, numeric/date/duration/decimal/vector edges,
malformed framing and runtime ownership/error behavior remain covered by the
canonical suites and all 96 binary fixtures, now independent of reflection
serializers. Arbitrary pointer/interface coercion, struct-tag discovery,
mutable object layout, manual query metrics and old retry setters are obsolete
contracts. Coverage source links are explicit dispositions, not a numerical
assertion-equivalence claim. COW topology helpers and useful lower-level tests
remain.

## Verification and exact source boundary

The native/runtime snapshot contains 412 files, SHA-256
`516e69033811d98bdd7c396a62df14a06cad792728410dfd95689fbc105b77e0`.
The full amd64 unit race suite, full 386 unit suite, vet, combined
`all integration unit ccm gocql_debug` compilation, arm64 cross compilation,
nested lz4 tests, benchmark module compilation and codec/copy smokes passed.
Arm64 used an execution substitute and is a compilation claim only. Removal
eliminated the old 386 serialization overflows. Actual 386 execution additionally
exposed unaligned fixture/tablet counters; aligned atomic wrappers fix them.
The shared-segment ownership regression now joins its exact segment dispatch
before asserting that the receive-loop share is gone, removing a scheduler race
without weakening either page-lifetime assertion.

| Actual successful native package | Top-level pass / skip |
| --- | --- |
| Scylla 2026.3.4 focused raw v4 stream/fixture | 2 / 0 |
| Scylla 2026.3.4 full selected raw v4 | 143 / 32 |
| Scylla 2026.3.4 full selected mutual TLS v4 | 145 / 30 |
| Exact owned Scylla CCM lifecycle | 1 / 0 |
| Cassandra 5.0.9 focused raw v4 stream/fixture | 2 / 0 |
| Cassandra 5.0.9 full selected LZ4 v5 | 153 / 22 |

These lanes exercise control/topology/schema, CAS/batches, tracing/observation,
cloud UUID SNI, authentication, maintenance sockets, metadata changes and
streaming interruption/recovery. Each selected original test has a fresh pass
when applicable; 158 original identities have current behavioral passes. The 16
development SELECT-without-FROM identities remain preserved and token-identical,
with actual successful applicable-lane evidence at step09. This task does not
claim their historical outcomes on the current source. All per-test/nested skips
remain visible; all-skipped parents do not earn behavioral credit. Complete
current/final174 matrices remain 14/17.

Both native runners exited0. Each namespace cleanup succeeded, all 6 owned nodes
stopped/joined and reserved endpoints refused connections. The development lane
and unrelated workloads were untouched. Raw receipts/logs remain under
`/extra/scylladb/gocql-api-test-cluster/step10`; compact receipts/outcomes are
committed. Final local logs are retained under that directory's `checks/`.

After those runners joined, required original/new copyright headers were
restored/added in 24 files. The [header proof](../evidence/step10-header-equivalence.json)
records every exact delta: complete Go token streams excluding comments match,
parsing succeeds and build constraints are byte-identical.21 files belong to
the runtime snapshot; 3 are audit tools. The final412-file snapshot is
`c9f09480622f2a3313ce91192de153fb3c985f83d47e5c67dbd73a1b6b692ab4`.
Combined-tag,386 combined-tag and benchmark-module builds pass after headers.
Runtime/native evidence remains bound to its original snapshot and this explicit
comment-only equivalence proof. `git diff --check` passes.

Five-iteration independent public streaming/cell/row/mode and canonical routing
smokes passed, as did migrated scalar/container/vector benchmark callers.
Duration decoding and canonical composite routing report 0 per-operation
allocations in this smoke. These are identity/boundary/cost checks, not native
throughput, peak-memory or universal-speedup results; comprehensive performance
remains 15.

## Explicit remaining work

Historical single-host replay captures still describe pre-migration control
traffic. Their integrity is checked as historical data. The exact operational
replay command fails with an unmatched typed request and is retained as failed
evidence, never a benchmark pass. Step 13 must regenerate functioning typed
control captures and restore equivalent operational select/insert workload and
assertions while consolidating modules. Migrated callers and the benchmark
module compile now; no skip hides the mismatch.

Foundation/runtime extraction remains 11/12; final facade/module/docs remain 13;
complete matrix, performance, independent review and release verification remain
14–17. No required step10 API/path removal is deferred. Root-owned STATUS.md and
RESUME.md are excluded from this commit.
