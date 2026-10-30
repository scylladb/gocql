# Step 11: extract foundations

Owner: `/root/step11_extract_foundations`; branch: `v2-11-extract-foundations`.
Accepted predecessor: `61ae08b0184f54b49274bb11ef14d5129fb62b82`.
The foundation extraction gate is complete. Exact commands, source bindings,
statuses and artifact/log hashes are in
[step11-verification.json](../evidence/step11-verification.json).

## Actual implementation boundaries

`internal/cqltype` owns immutable complete descriptors/columns, metadata-only
TypeInfo descriptors and schema type parsing, UUID and scalar value domains,
borrowed text/bytes, cell states and shared errors. Public aliases retain opaque
storage fields. Internal descriptor/view accessors transfer descriptors and
ownership without gathering or converting payloads.

`internal/codec` owns checked value readers/writers, scalar/container/vector/
tuple/UDT/dynamic codecs, parameter/row composition including conditional rows,
and every incremental scalar/collection/row/reset/skip/capture validation plan.
It imports only cqltype and wire within this module. Fixed streaming scalar
conversion remains bounded to 16 bytes; duration still uses the same incremental
varint and sign/range validation. No ValueReader/buffered decoder adapter was
introduced. Caller storage, direct final reads, segmented views, compiled plans
and lease behavior remain covered by the moved tests.

`internal/model` contains nongeneric encoded cells/values, immutable attempt and
batch inputs, routing information, physical response data, results and page
state. Root responsePage embeds that data alongside the runtime's prepared
metadata identity. `ParseTypedResponseInto` fills that embedded destination
without a second response object or payload conversion.

`internal/protocol` owns checked canonical QUERY/EXECUTE/BATCH requests,
responses/errors/metadata/events, incremental metadata cursors and shared live
budgets, plus STARTUP/OPTIONS/auth/REGISTER and the narrow parameter-free USE
writer. Its immutable Settings takes wire settings, never Conn or Session. The
older independent PREPARE/EXECUTE/BATCH byte oracles are test-only. Existing
wire/segment/checksum/hash implementations remain intact; the existing bounded
Snappy implementation moved to wirecompress behind its public alias.

Root frame.go now contains public observation/consistency facade code. Root
Conn/framerPool/cache code owns connection orchestration, pooling, callback
installation, released guards and reader/writer lifecycle; it consumes the
internal Framer's reusable wire state and release hook. Execution, schema fences,
metadata reconciliation, routing and exclusive reservations remain runtime work
for step 12. The [dependency audit](../evidence/step11-dependency-audit.json)
records the exact import graph, 0 violations, opaque public domains and remaining
root responsibilities. No lower package imports the public driver.

## Test and source preservation

All 174 original identities and original body hashes match accepted step10.
170 translated bodies retain their tokens. Four require only explicit extraction
rewrites: canonical routing uses model Cells; the negative-stream probe calls
WriteHeader/Finish; SELECT-without-FROM rejection names moved request/owner
fields; vector metadata expectations use the existing NativeType constructor
with the same zero proto/custom fields and call the moved metadata methods.
The [one-way rewrite proof](../evidence/step11-baseline-preservation.json) accepts
only those changes, ignoring comments, semicolons and optional trailing commas.
Complete original/current bodies and reachable helpers/methods remain in the
[assertion inventory](../evidence/step11-assertion-source.json).

Every one of the predecessor's 957 named root test/example/benchmark declarations
has exactly one current declaration. The [relocation inventory](../evidence/step11-test-disposition.json)
links source locations; it is an identity inventory, not a numerical assertion
comparison. Pure descriptor, codec, page-state, metadata, authentication and
framing tests moved beside their implementations. Runtime injection fixtures
remain with their consumers; shared helpers/oracles compile as test-only code.
A new adversarial metadata-budget check proves rejection before column-payload
reads. No original test was deleted to avoid private access.

## Verification and exact final delta

The frozen 454-source native/full-unit tree is SHA-256
`3368c1e1ab2c1151f7aaa40104269ccbefdee66a031b680165e3dffcfcd79b90`.
Full amd64 unit race and actual 386 unit execution passed. Four focused native
packages on that same tree passed:

| Applicable lane | Top-level pass / skip |
| --- | --- |
| Scylla 2026.3.4 raw v4 | 24 / 4 |
| Scylla 2026.3.4 mutual TLS v4 | 25 / 3 |
| Cassandra 5.0.9 LZ4 v5 | 26 / 2 |
| Cassandra 5.0.9 LZ4 + TLS v5 | 26 / 2 |

These lanes cover actual startup/authentication, scalar/nested/vector domains,
metadata IDs, QUERY/EXECUTE/BATCH/CAS, custom payload/error parsing, paging,
schema replay, cloud TLS and public streaming. Streaming retains exact seeded
backing, inter-page ALTER reconciliation and interruption/no-replay/recovery
assertions. Each runner held the shared address locks across start/tests/cleanup
and stop/join. Both exited 0, every owned namespace cleanup passed, all six owned
nodes joined and reserved endpoints refused connections. Development and
unrelated workloads were untouched. Raw receipts/logs remain under
`/extra/scylladb/gocql-api-test-cluster/step11`.

Vet identified three newly exported internal framer names whose signatures do
not match standard io interfaces. After both runners joined, WriteTo, ReadByte
and WriteByte became WriteFrameTo, ReadByteValue and WriteByteValue. The
[go/types-bound proof](../evidence/step11-method-rename-proof.json) retains exactly
3 declaration and 36 bound-selector edits across 6 files, unchanged signatures,
and complete before/after source. Their former signatures already fail
io.WriterTo/io.ByteReader/io.ByteWriter; no project interface or reflection
method-name dispatch depends on them. A separate [gofmt proof](../evidence/step11-format-proof.json)
records token-identical formatting in two files. The original package documentation was also restored byte-for-byte from the
accepted predecessor; its [documentation proof](../evidence/step11-doc-preservation.json)
shows 0 declarations, imports or compiler/build directives. The final
455-source snapshot is
`bbf18b3fb46282cb10eca28e75b5885662783cd7d61f5759009e73f097e6502f`.

Final foundation/transport race checks, vet, combined
`all unit integration ccm gocql_debug bench` compilation, arm64 cross compilation,
nested lz4 tests and benchmark-module compilation passed. Arm64 is compilation
only, using an execution substitute. Source/log bindings distinguish the native
snapshot and exact final identifier/formatting deltas. No behavioral edit
followed native verification. `git diff --check` passes.

Five-iteration codec/container/stream/row and independent public streaming/copy
smokes passed. Canonical composite routing and direct row destinations report
0 allocations/op in this smoke; backing/capacity/lease checks remain assertions,
not allocation-count substitutes. These are regression/copy/cost checks, not a
native throughput or peak-memory comparison. Comprehensive performance remains
step 15.

## Explicit limits and next work

The [174-test mapping](../evidence/step11-test-mapping.json) records 26 original
identities with fresh effective passes in the focused lanes. All predecessor
158/current and step09 all174 results retain their own source bindings; skipped
or unselected identities are never credited as fresh passes. The development
SELECT-without-FROM matrix and final-wide all174 acceptance remain steps 14/17.

Runtime extraction remains 12; final public/module/docs work remains 13. The
accepted historical control-traffic replay mismatch still requires regenerated
typed captures and equivalent operational workload in 13; benchmark-module
compilation is not a replay pass. Root-owned STATUS.md and RESUME.md are excluded
from this commit. No foundation implementation or required ownership/streaming
contract is deferred.
