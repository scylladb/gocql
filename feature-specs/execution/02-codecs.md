# Step 02: typed codecs and caller destinations

Owner: `/root/step02_codecs`. Branch: `v2-02-codecs`.
Accepted predecessor: `6cfb37a99dea150d2a50b06ea1fba7944e74b30d`.
This handoff covers codec/reader/composition foundations. It does not claim
Rows/Page.NextInto orchestration, exclusive connection reservation, RunStream,
live server validation or final-tree release completion; those remain with
steps 03/04/07/09/14–17.

## Changed behavior

All RowPlan implementations expose DecodeInto. Row fields, OneColumn,
PairRows, tuple/UDT records, CAS rows, DynamicRows and internal schema rows
write into supplied fields and clear unmapped buffered Go state. Explicit
MapRows callbacks still perform their requested value-returning mapping.
Custom plans receive the actual caller destination. Plans never retain that
pointer. Value-returning record decoding uses cleared, concurrency-safe pooled
scratch; direct destination decoding does not use that complete-row scratch.

Nullable, BindValue, Empty and named scalar wrappers recursively compile child
codecs. Dynamic row/value plans retain checked descriptor graphs and vector
widths, and shared descriptor DAGs are memoized rather than expanded repeatedly.
UDTOptions requires exact expected Keyspace/Name, including null inputs.
Zero-arity tuples follow the specified ordinary/Empty interpretation.
BorrowText and TextValue return checked errors for malformed UTF-8. Every
existing caller affected by the signature change compiles with explicit error
handling; integration UDT fixtures supply their actual expected identities.
Collect recursively clones typed and dynamic values for all iterator families,
including input-side dynamic arrays, and never substitutes a borrowed result
when clone information is missing.

Separate StreamRowDecoder/StreamDecoder plans implement Reset/DecodeInto for
scalars, text/ASCII/blob, signed varint bytes, decimal payload storage,
nullable/Empty wrappers, lists/sets/maps, fixed/variable/nested vectors,
tuples/UDTs, explicit encoded output and custom incremental codecs. Readers
expose scoped context/type/state/remaining/read/child/skip/end operations.
Fixed scalar scratch is bounded and shares canonical buffered scalar validation;
text validation shares its incremental UTF-8/ASCII state machine. Arbitrary
buffered decoders are never adapted by gathering a cell.

Text/blob and encoded-output payload reads target final caller arrays. Nested
count/dimension capacity is checked before element bodies; decimal checks its
unscaled capacity before even reading its scale. No decoder grows output
implicitly. Null and empty remain distinct, result unset is rejected, and
preseeded nested backing survives repeated decoding. Ignored fields are
recursively validated with bounded discard scratch. Compiled validation graphs
and stable descriptor identities avoid per-element schema compilation. Equivalent
custom child descriptors resolve against precompiled actual children; a bounded
compatibility cache prevents unbounded retained custom descriptors.

Root accepted one design correction: a UDT application field absent from server
metadata has no DataType to compile. Optional public StreamResetter[T] provides
metadata-independent declaration reset. Built-ins propagate reset capability
through compositions, preserve seeded storage, and reject an absent custom
mapping lacking required reset capability with ValidationError. Mapped custom
fields continue to use their compiled plan.Reset. The three normative design
documents were updated by the design owner and are included in this handoff;
this addition is not attributed to historical design review cycles.

Nil compiled custom plans fail at compilation boundaries with ValidationError.
Tiny immutable wire.Stream Context/Limits accessors support custom reader
cancellation and count limits without background work.

## Gate evidence

Exact command outcomes and source hashes are recorded in
`feature-specs/evidence/step02-verification.json`. All logs named below are
under `feature-specs/evidence/`.

| Requirement | Current evidence |
| --- | --- |
| Direct buffered caller destinations | Accessor address checks for wide outer/nested records, custom pair and CAS plan propagation without Decode, unmapped reset, nil destinations, and schema-independent clone after release. |
| Scalar/type/null/Empty correctness | Existing golden/malformed/boundary suites plus streaming scalar domains at every short-read size, UTF-8/ASCII splits and malformed sequences, numeric ranges, exact widths, UDT identity before null, zero tuple/Empty and result-unset rejection. |
| Recursive plans and concurrency | Existing compile-count/concurrent tuple tests, custom stream compilation exactly once across 100 rows, concurrent immutable streaming plans, memoized 64-level shared descriptor DAG/equality checks and excessive-depth rejection. |
| Borrowed/owned/clone boundaries | Existing encoded ownership and segmented backing suites; new typed/dynamic nested Collect results survive source lease release and caller input mutation. Direct buffered destination accessors receive the actual application addresses. |
| Final streaming storage | Large blob Read target addresses lie in final caller ranges; encoded nested output also reads bulk bytes into final ranges. Capacity failures leave value bodies unread, including decimal scale. Seeded byte, nested collection/map/vector and UDT storage is reused. |
| Ignored/custom/record paths | Recursive invalid ignored booleans/text/nested values fail; an 8 MiB ignored blob uses 127-byte discard scratch with bounded read count. Custom Child uses compiled children and context; extra/missing UDT policies and absent custom declaration reset are explicit. |
| Nil custom compilation | Built-in row/field/collection/wrapper/tuple and buffered row/parameter compile boundaries reject (nil,nil) plans with ValidationError. |
| Allocation and cost | See focused and existing codec benchmarks below; regression test compares 1 versus 1,024 tuple elements. |
| Regression/build coverage | Full uncached unit race, focused race, integration+gocql_debug binary, CCM-tag binary, vet and documentation traceability checks; detailed final outcomes in manifest. |
| Fuzz smoke | Final streaming value and scalar UTF-8 fuzz targets run with two workers for five seconds each; bounded fuzz inputs exercise malformed lengths/counts/state/text and seeded capacity without unsafe writes. This is smoke coverage, not exhaustive fuzzing. |

The broadened codec benchmark exposed a value-returning tuple regression:
a local T escaping through DecodeInto allocated once per tuple. Cleared typed
scratch restored constant iterator/setup allocations. The allocation assertion
is intentionally compiled without -race because Go's race instrumentation
randomly drops sync.Pool entries; the observed diagnostic is retained in
`step02-race-pool-allocation-diagnostic.log`. Concurrency and ownership tests
still run under the race detector. No allocation claim relies on race-mode
pool behavior.

The first vet run exposed an unused String result in a migrated panic assertion;
the assertion now discards it explicitly, and successful rerun evidence is
retained. Earlier failing/diagnostic logs remain visible.

## Performance interpretation

Benchmarks distinguish compilation/setup from warmed decoding. Direct buffered
rows with a large application representation and 500 KiB borrowed payload have
zero steady allocations and no payload copies. Fragment counts of 1, 1,024 and
65,536 vary descriptor traversal independently; both Decode and DecodeInto
preserve backing, with the direct path avoiding the convenience row copy.

Streaming final-storage benchmarks process 32 rows per operation. Page scope
setup is included, final output and compiled reader/plan setup are excluded.
The observed setup is 216 bytes/four allocations per 32-row page for 32-byte,
4 KiB and 500 KiB blobs. Nested streaming records 264 bytes/six allocations per
32-row page at both 16 and 1,024 elements. These counters do not scale with
payload bytes or element count. Unit synthetic readers still copy source bytes
into final output as a real Reader does; these are not socket/TLS throughput or
zero-copy claims. Runtime/syscall, transform-copy, first-row/backpressure and
peak-live-memory comparisons remain later gates.

Existing scalar/compiled-container benchmarks report zero steady decode
allocations. Typed container and tuple iterators have constant iterator/closure
setup costs, not one allocation per element. Encoding builder/slab allocations
and explicit Clone/Collect materialization remain separate operations.

## Consumption and remaining later gates

- Step 03 installs RowPlan.DecodeInto in Rows/Page.NextInto, including false/nil
  clearing, mixed iteration, retained pages and prepared metadata fences.
- Steps 04/07 construct a StreamRowReader once for a checked page shape, then
  reuse it by resetting its index between rows. Retain compiled validation and
  reader state across a page; do not reconstruct it for every row. Plans do not
  reset the whole arbitrary destination inside DecodeInto: runtime sets the
  output-started boundary before calling Reset, then DecodeInto synchronously.
- Runtime maps protocol type-depth limits to framing scopes with the outer cell
  scope accounted for. Readers enforce configured count/byte/framing limits;
  actual metadata depth and metadata/control budgets also remain runtime gates.
- StreamRowReader.RequireEnd checks the physical column count only. Runtime
  tracks response row counts and exact envelope completion, supplies verified v5
  units, handles events, owns connection interruption and prohibits replay after
  output. The codec layer creates no payload worker or background goroutine.
- Public custom StreamValueReader.Child accepts actual declared child types;
  plans compile and retain expected descriptors once. It does not synthesize
  types or compile unknown children while decoding.
- Unit/direct-reader tests establish these codec seams. They do not establish
  complete transport memory, pool reservation, real database support or final
  release readiness. Those full streaming and 174-baseline gates remain intact.

Optional root GOARCH=386 compilation failed in unchanged legacy
serialization/varint and serialization/cqlint int-width constants before reaching
v2 code. `step02-386-legacy-diagnostic.json` proves those files match the
accepted predecessor byte-for-byte. The separate 386 foundation lane passes.
Root accepted retaining this full-root portability issue for legacy removal
in step 10. It is not counted as a pass and does not narrow the mandatory later
release architecture matrix. The supported amd64 codec gate remains separate.

The unchanged benchmark submodule manifest also requests `go mod tidy` before
its normal build. Its source compiles using the retained alternate manifest,
which uses the same source and absolute local replacement without modifying
repository module files. Manifest consolidation remains step 13; the original
failed check is not relabeled successful.

The step 02 codec gate is satisfied by the final successful checks and source
hashes recorded in the manifest. No unresolved step 02 finding remains. The
explicit optional legacy-root-386 and benchmark-manifest diagnostics above do
not establish final release readiness or discharge any later gate.


Evidence formatting: committed log copies expand tabs and strip trailing spaces
so Go-emitted benchmark padding/stack indentation passes Git whitespace checks.
Raw byte-exact originals and hashes are retained in the external checkpoint
archive named by step02-log-formatting.json. Commands and outcomes are unchanged.
