# Step 09: integration migration and live streaming

Owner: `/root/step09_integration_migration`; branch: `v2-09-integration-migration`.
Accepted predecessor: `beef9d9bac292f8bec93df87bd624780f4841362`.
The assigned gate is complete. Every one of the 174 original identities has an
observed behavioral pass in a successful applicable package on the same frozen
582-file source. Exact commands, statuses, source/config/log hashes and cleanup
proof are in [step09-verification.json](../evidence/step09-verification.json).
Individual distribution/protocol skips remain visible and never count as passes.

## Migration and assertion preservation

The [complete mapping](../evidence/step09-test-mapping.json) links every original
file/test identity to its translated behavior, intentional API changes, fixture
requirements, direct/shared skip gates and actual command outcomes. The
[assertion inventory](../evidence/step09-assertion-source.json) preserves complete
original/current test bodies, exact assertion expressions, 75 original/135
translated reachable function helpers and supporting receiver methods. Original
file hashes match all 29 files in the immutable baseline manifest. Shared helper
assertions replace repeated setup guards; assertion counts alone are not treated
as equivalence evidence. Nested outcomes are retained too: a parent with every
child skipped does not establish a behavioral pass.

Client-route writes now use typed Prepare/BindOwned/Exec at ALL consistency. All
nine UUID route rows and full-connection, paired, single-host and TLS-port
comparisons remain. Explicit integration CQL literals, metadata lookups and UDT
codec identities use the run namespace at their source call sites; no driver
boundary rewrites CQL. Routing-key coverage again compares the exact canonical
single/composite bytes as well as metadata indices/types, cache reuse/size and
tokens. Present-empty host IDs now assert ValidationError; malformed, nonempty
unknown and down-host cases retain their separate assertions. The all-hosts-down
fixture restores physical picker down states and rebuilds only its synthetic
membership index, retaining all nine original policies and one-host cases.

Vnode ring fixtures explicitly disable tablets where supported; the separate
TestTablets still requires actual wrong-shard feedback and shard-specific
convergence. Both Scylla distributions run two shards. Native plaintext/TLS shard
ports are explicitly 19042/19142, preventing the server from assigning TLS to the
plaintext advanced endpoint. This exposed and corrected fixture behavior that
one-shard runs could not exercise. Schema recreation keeps every input, golden,
dump execution and second-dump assertion. Only the exact server-declared aborted
group0 concurrent-modification transaction receives a bounded fixture retry;
other errors propagate.

## Runtime regressions exposed by live coverage

Cassandra conditional PREPARE can omit result columns. Prepare now defers that
absent shape for unknown/conditional statements, then validates actual EXECUTE
metadata before decoding or output. Explicit ordinary and present result shapes
still validate eagerly. Reprepare compares a routing hasher when partition-key
components exist; result-only evolution of a statement without those components
cannot invalidate nonexistent canonical routing. Input types, index order,
table/kind and metadata revision/schema fences remain checked.

A live streaming continuation previously rejected any stale prepared epoch once
output had started. It now reconciles metadata before submitting the next page,
using exactly the existing canonical input and continuation. It never resubmits
an exposed page. Deterministic regressions assert the first EXECUTE once,
PREPARE before continuation, that continuation once, changed physical shape,
unchanged seeded backing, exact attempt/row/page counts and no early destination
Reset/write. A next-page server error remains terminal without retry even for
idempotent input. Existing partial-write and UNPREPARED-after-output no-replay
regressions remain unchanged and pass.

## Actual server and lifecycle coverage

All 12 selected native packages and all cleanup commands passed. The primary
package totals below include the two added fixture tests; baseline identities
and nested outcomes are separately recorded in the mapping.

| Real fixture/mode | Go top-level pass / skip |
| --- | --- |
| Scylla 2026.3.4 raw v4 | 143 / 32 |
| Scylla 2026.3.4 mutual TLS v4 | 145 / 30 |
| Cassandra 5.0.9 raw v4 | 143 / 32 |
| Cassandra 5.0.9 raw v5 | 151 / 24 |
| Cassandra 5.0.9 LZ4 v5 | 153 / 22 |
| Scylla development 2027.1 raw v4 | 159 / 16 |

Three focused packages run before the broad modes in the same owned runners.
Both Cassandra v5 TLS modes additionally passed the full live streaming scenario.
The original TestCCM passed actual status, stop/join and restart/status through a
finite CLI forwarding to the runner's exact locked cluster, without CCM CURRENT.
Cloud UUID SNI, TLS ticket resumption, authentication rejection, maintenance Unix
sockets, injected native payload/failure/incomplete-peer cases, tracing,
session/batch statistics, schema/routing, metadata IDs, segmentation/compression,
all serialization cases and all 16 development SELECT-without-FROM identities
have actual successful applicable package evidence.

The live stream reads five rows containing 480000-byte blobs, 288000-byte UTF-8
text, nested list<blob> and scalar IDs into seeded storage across three pages.
It checks exact values/backing, empty results, early Close, configured pool
capacity and subsequent buffered reads. A second real client ALTERs SELECT *
between pages; physical metadata changes 4→5 while all five rows remain exact and
unique. The runner then stops and joins the actual selected coordinator after
one delivered row. Further advancement/Close stays terminal with no replay; the
same runner restarts/health-checks that node, and a pinned buffered read proves
pool recovery. This passes on Scylla raw/TLS, Cassandra v4 and v5 raw/LZ4 with and
without TLS, plus development Scylla.

Control uses a private mode-0600 Unix socket and exact CCM node objects under the
existing address locks. It accepts only status, stop/join and start/health;
records paths, addresses and old/new PIDs; and joins its worker before final
cluster stop. Cleanup disables unsolicited event subscriptions, drops only
actual enumerated run-prefixed namespaces, awaits agreement and queries schema
again to prove none remain. This avoids a real bounded event-overflow failure
when dropping the complete suite's large schema, without enlarging product
buffers. All three final runners exited; all nine owned nodes joined and their
reserved endpoints refused connections. Unrelated live workloads were untouched.

## Checks, costs and retained diagnostics

Seven final checks passed: uncached full unit race, focused race twice, vet,
integration/CCM race builds, six Python harness checks and copy/scaling benchmark
smokes. Commands and normalized/raw log hashes are in the verification manifest.
The independent public streaming oracle still varies row count separately from
1KiB/400KiB/8MiB cells across raw v3/v4, TLS and v5 raw/LZ4; canonical composite
routing retains zero per-operation allocations in this smoke. Raw setup stays in
roughly 14–19 KiB here and v5 LZ4 about 277 KiB, reflecting bounded integrity/transform
storage; caller storage is outside measurement. Five benchmark iterations under
concurrent native fixture load are a cost/copy regression smoke, not a throughput,
peak-memory or universal-speedup claim. Comprehensive native comparisons remain 15.

Failed attempts remain separately retained diagnostics with exact commands,
source snapshots, logs and stop/join evidence. None contributes a partial pass.
They exposed blank-host validation and synthetic-index mistakes, absent CAS
metadata, the sole-capacity ALTER fixture deadlock, result-only hasher rejection,
the streaming stale-epoch gate, advanced-port TLS configuration, schema-event
cleanup overflow and explicitly aborted development DDL. Focused native probes
preceded the final whole packages after those fixes.

The checked-in mapping can be regenerated with the commands recorded in the
manifest. Raw command/server logs and data remain at the exact task-owned paths
in retained receipts; compact per-test outcomes and source evidence are committed.
The final-wide verification matrices remain 14/17. Legacy removal, extraction,
module/docs, comprehensive performance and independent release review remain
assigned to 10–17. No required step09 feature or original identity is deferred.

`git diff --check` retains two diagnostics at common_test.go465/472: the
spaces-before-tabs are inside raw CQL literals and byte-identical to accepted
predecessor beef9d9. Explicit namespace concatenation changed those closing lines,
so Git newly reports the inherited literal whitespace. The tested source bytes
are retained with root authorization; this cosmetic diagnostic is recorded
separately and is not claimed as a passing formatting check.
