# Step 14: full same-source matrix

Branch: `v2-14-full-matrix`.
Accepted predecessor: `75f015a390167da694f99b5d326dbc41fce39111`.
Gate: passed, pending parent review. All 174 original identities and translated
nested-subcase gates have effective passes in successful current-source packages.
All 17 native modes, actual CCM, and streaming criteria pass. Steps 15–17 remain.

The frozen native snapshot contains 623 inputs, SHA-256
`69e72007e67fa3c52d8ca760ba6765c0ad0cfe11618f4782095a28886e697350`.
Runtime inputs have not changed since this freeze. Root-owned STATUS.md and
RESUME.md remain outside this work and its commit.

## Matrix and assertion lineage

The matrix uses full `^Test` root packages with integration/debug tags and race
instrumentation for stable Scylla v4 raw/Snappy/LZ4 with plaintext/TLS, actual
Scylla development v4 with strongly consistent tables enabled, and Cassandra
5.0.9 v4/v5 raw/LZ4 with plaintext/TLS. Raw v3 maintenance/streaming and real v3
TLS streaming are additional explicit modes. Stable also runs actual TestCCM
stop/join/start through the live runner's confined control socket.

The original 174 identities, their 135 helpers and 94 supporting methods retain
complete accepted sources. The verifier checks translated assertion hashes,
whole-package exits, terminal events for every original root test in every full
package, and historical translated nested-subcase identities. Historical results
supply lineage only. A parent whose terminal descendants all skip receives no
behavioral credit. Fixture/version skips retain their text and gates.

Stable exercises actual tablets, shards, LWT, pools, metadata/events, auth,
client certificates, trusted identity, ticket resumption, cloud UUID SNI and
host-specific TLS hooks. The development fixture actually negotiates tablet-v2
and validates global-table leader routing as well as SELECT without FROM.
Cassandra supplies actual protocol-v5 segmentation, >500 KB responses, metadata
IDs, payload echo and injected write failures. Incomplete peers come from the
independent wire fixture, without system-table mutation. Scylla v5 rejection
is not counted as successful v5 coverage.

## Streaming assertions and measured scope

Eight deterministic modes cover v3/v4 raw and v5 raw/LZ4, each with optional TLS.
Every split/truncated prefix, checksum corruption, first-row gate, final caller
range, cancellation/final-write join and explicit custom-decoder-error case is
bound to actual passing tests. For v5 the first-row gate follows the last complete
validated unit needed by that row. Actual native modes exercise large blob/text,
nested values, empty/multiple pages, metadata changes, early Close, pool recovery
and coordinator interruption after one delivered row. A real schema event also
arrives while a cursor is idle and its exact host reservation is exhausted.
Frame-compressed v3/v4 streaming rejects before submission as specified.

The generated-source storage test independently varies 256 KB versus 8 MB cells,
128-row/8 MB pages, an 8 MB ignored cell and a slow consumer across all eight
modes. Input generation retains only bounded metadata/counters or one source
unit; it never creates a complete response fixture. Caller output is allocated
separately and its exact contents/backing are asserted.

Live driver byte capacities are sampled inside reads and between calls. The
measurement sums actual wire/codec discard capacities, conservative scope
capacities, all simultaneously charged connection metadata/control, optional
read-ahead and one transform arena including simultaneous input/output/framing.
Unit views alias that arena and are counted once; encoded captures alias final
caller storage. Fixed schema plans/maps/objects, standard-library TLS record
storage, producer storage and final application output are separate. This is a
byte-capacity bound, not a total-heap measurement.

Observed capacities range from 592 to 33,360 bytes for v3/v4, 131,679 to 164,447
bytes for raw v5, and 262,750 to 295,518 bytes for LZ4 v5. TLS has the same driver
capacity ranges. All forty shapes remain below the asserted 320 KiB ceiling.
Independent read spies establish raw socket/authenticated-plaintext final-range
identity. Validated-unit-to-parser copies are counted separately and reach
8,389,169 bytes for the largest generated frame; those are required transform
copies. TLS transform work is not labeled zero-copy.

The comparative Run/Row, buffered NextInto and RunStream benchmark uses equivalent
row/cell shapes; fragmented/nested and all-mode smokes also run. These provide
focused allocation/cost checks. Their static payload metrics are not live-memory
measurements. Comprehensive independent CPU, throughput, peak-heap, syscall and
latency comparisons remain step 15.

## Fixture fixes, cancellation and diagnostic boundaries

A broad initial package exposed cleanup running as an ordinary test and cleanup
of all run namespaces inside the killed-child test. Cleanup now requires an
explicit runner-only environment flag. Mid-package cleanup uses the same
implementation with an exact allowlist, validates run ownership before DROP,
quotes identifiers and verifies absence. Compiled children are killed/joined
before that cleanup. Cross-node child-namespace observations await actual schema
agreement and still query the exact namespace within their bounded context.

The runner terminates/joins its test process before bounded cleanup. Repeated
signals are shielded during teardown and cannot interrupt final node joins.
An actual probe sends SIGTERM during testing, then SIGTERM/SIGINT during cleanup
and stop/join. It retains an expected canceled receipt, successful cleanup,
joined/gone test and cleanup children, joined nodes and refused endpoints.

Diagnostic receipts under the task root's `step14/` and `step14/final/` retain
failed packages and old source bindings. A late signal interrupted an already
failing Cassandra stop there; the exact retained owned cluster was inspected,
stopped/joined and independently probed. That recovery is diagnostic evidence.
The first current-source combined Cassandra fixture had four successful v4
packages, but its fourth batch cleanup exceeded 30 seconds after large schema
teardown; the entire combined receipt remains failed and supplies no credit.
Each mandatory Cassandra mode therefore runs on fresh fixture data under the
same source/install pins, with full package scope and the original cleanup bound.
No source/budget relaxation or unrelated workload reset is involved.

Twelve bounded fuzz campaigns use new, retained per-target caches. Their initial
built-in seed counts/functions and saved interesting-mutation hashes are recorded;
the persistence corpus does not represent every in-memory execution. Budgets are
30 seconds per target and 60 seconds for cancellation, with two workers. Prior
same-source campaigns that reused the global cache remain separate history.

Full unit race, actual 386 execution, arm64/all-tag compilation, vet, module
verification/drift, normal external contracts/examples and the public/transitive
boundary audits pass on the snapshot. All eight fresh Cassandra full-package
receipts pass, including cleanup and node joins. The final verifier binds
[all 174 identities](../evidence/step14-test-mapping.json),
[17 actual native modes and explicit skips](../evidence/step14-lanes.json),
[16 streaming criterion groups](../evidence/step14-streaming-acceptance.json),
[12 seeded campaigns](../evidence/step14-fuzz.json), and
[checks](../evidence/step14-checks.json) to this same source. Independent final
process/endpoint probes are in [cleanup](../evidence/step14-cleanup.json).
The [gate manifest](../evidence/step14-verification.json) records artifact,
configuration, command and log hashes. Steps 15–17 remain.

The fixture README is documentation outside the 623-input native manifest. Its
post-freeze explanation of dedicated cleanup, cancellation shielding and fresh
Cassandra mode data matches the observed runner behavior. The gate manifest
hashes it separately; no native source input changed.
