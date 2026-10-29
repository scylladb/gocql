# Step 03: prepared metadata and paging

Owner: `/root/step03_prepared_paging`. Branch: `v2-03-prepared-paging`.
Accepted predecessor: `1fc00bfe1745ef7d16a4d2fc0200f4588eb3a2ad`.
This handoff covers prepared/paging behavior and direct buffered destinations.
It does not establish exclusive streaming transport, RunStream, complete
metadata/control orchestration, or final release verification.

## Changed behavior

Prepared validity uses dependency-local schema clocks for known tables, UDTs,
keyspaces and partitioners. Unidentified dependencies conservatively use the
relevant keyspace; unscoped statements use the global unknown-scope clock.
Function dependencies not described by PREPARED conservatively use their
keyspace. Unchanged metadata snapshots, unrelated tables/types/keyspaces,
topology and placement publication do not invalidate a known statement.
Changed result metadata registers newly introduced UDT dependencies too.
The historical schema generation remains diagnostic, not a NO_METADATA fence.

Host entries are immutable and retained by individual attempts. Initial cache
publication, host PREPARE publication, EXECUTE metadata publication and decoder
plan publication compare exact local entry/revision and captured dependency
epoch under the schema publication gate. A failed publication is discarded;
it is never retried against a fresh revision. Opaque server IDs are compared
for equality only. A matching received NO_METADATA page uses the attempt's
retained metadata even after a concurrent schema event or cache replacement.
A losing full response similarly retains its actual supplied metadata.

Reprepare validates encoded parameter types/order/identity and immutable routing
schema: partition-key index order, effective table/keyspace, partitioner name and
conditional classification. Incompatibility returns SchemaChangedError before
resending. Compatible host-specific IDs change without invoking input encoders.
Per-host prepare flights are independent and waiters can cancel individually.
Their shared worker captures immutable Columns only, not borrowed input spans;
connection closure interrupts it and Session.Close joins admitted prepare workers.
Broader repeated/session shutdown and execution policy work remains step 07.

Warm binding execution reads its directly held prepared state without the global
discovery cache mutex/LRU. A per-application-decoder result-plan cache retains
only its current shape plus plans held by live pages. Exact immutable entry hits
are O(1); new equal shapes are compared once and reuse the existing plan. User
Compile executes outside both cache and schema gates. The final plan-store gate
prevents invalidation interleaving with its epoch check/store.

Discovery bookkeeping also remains bounded after a stale entry is removed and
its replacement fails: the stale order record is removed, duplicate records are
collapsed, and eviction skips stale records before admitting another entry.
Capacity zero, capacity one and concurrent distinct misses are tested. Eviction
does not invalidate live prepared/bound handles.

Rows.NextInto and Page.NextInto advance through RowPlan.DecodeInto on the actual
caller destination. They share Next's framing, plan and paging rules; mixed
Next/NextInto calls each advance. Row and Clone read the current supplied
instance, including caller edits. The pointer is dropped at the next advance or
Close without modifying an older destination. False clears a non-nil destination
for exhaustion, closure, cancellation, metadata/framing or codec failure.
Active nil fails terminally before row/paging work; exhausted/terminal calls
preserve their earlier result. Custom partial writes are never published.

Retain pins the independent physical page, not caller storage. Delivered output
survives Session.Close until advancement/iterator Close; retained leases survive
those operations too. Iterator mutexes protect Close against active advancement;
request cancellation unblocks a pending next-page receive. Completion callbacks
run after unlocking, so observers can inspect Err/Info/Close without deadlocking.
Close releases exactly once, including concurrent/reentrant closure.

Automatic paging uses a shared nongeneric hash-indexed history with full-byte
collision checks. It recognizes consecutive/nonconsecutive and present-empty
cycles. Positive MaxPagingStates (65,536 default) and MaxPagingStateBytes
(16 MiB default) bound retained index entries and token bytes. Limits/cycles
stop the next fetch, including prefetch. History, internal sending and PageState
share the same immutable owned token bytes; only public Bytes/Restore copy.
RunPage has no cross-call history, preserves empty continuations after closure,
and rejects output-only completion with ErrPagingComplete before admission.

Conflicting full metadata observations from different hosts trigger bounded
relevant typed reconciliation rather than guessed server chronology. The
existing retry scheduler coalesces at most 64 pending/in-flight keyspaces;
known scopes use only their typed WHERE queries and scoped DESCRIBE. Unknown
statement scope alone enumerates internally under existing metadata limits.
Internal validation honors public Disabled/Minimal/keyspace-filter publication
semantics. Queue overflow propagates ProtocolError at stage `prepared schema
reconciliation` to the logical operation. Earlier delivered pages remain valid;
unscheduled failures stay dirty and retry registration when space becomes
available. Failed scheduled scopes remain queued for bounded retry; successful
reconciliation allows subsequent conflicts to schedule again. The normative
API/implementation contracts record this step 03 refinement outside historical
design reviews.

## Gate evidence

The authoritative final source/document hashes and exact successful commands,
exit statuses and retained log hashes are in
`feature-specs/evidence/step03-verification.json`. The earlier source before the
last discovery-only correction is retained separately, with the delta explicit.

| Required boundary | Evidence |
| --- | --- |
| Direct buffered destinations | Mixed iteration, actual caller fields, Row/Clone observing edits, destination clearing on false/partial failure/nil, terminal exhaustion, and old-destination preservation. Codec address tests from step 02 remain applicable. |
| Page/input ownership | Retained views after page/session closure, delivered current page survival until advance, cancellation during blocked next-page I/O, exact-once concurrent Close and reentrant observer inspection. |
| Paging semantics and bounds | Empty/nonconsecutive cycles, full-byte hash-collision comparison, byte/count budget preflight with/without prefetch, positive config validation, manual present-empty state after Close and complete-state rejection before admission. |
| Exact metadata fences | Late PREPARE/EXECUTE replacement, exact pointer identity even at equal revision, epoch changes during fetch, initial prepare crossing an epoch, new result UDT dependency events, and a deterministic publication/invalidation exclusion test. Losing responses retain matching metadata. |
| Canonical immutable binding | A single encoder invocation across injected retry, UNPREPARED repair and automatic paging. Parameter/type/table/partitioner/PK-order/conditional incompatibilities fail before resend. |
| Scoped cache behavior | Unrelated table/type/keyspace and unchanged-snapshot checks; direct warm selection while the discovery mutex is held; independent host flights and cancelable waiters. Failed stale reprepare, duplicate order, zero/one capacity and concurrent misses preserve cache bounds. |
| Required reconciliation | Actual typed relevant-schema queries, a backend that rejects unrelated queries, no public expansion under Disabled/filter/Minimal settings, coalescing, dirty failure retry, visible queue overflow with earlier page retention, registration retry and later-conflict rescheduling. |
| Regression/build/fuzz | Full uncached unit race, focused unit/race, vet, integration race binary and CCM binary compilation, plus two five-second malformed-page/history fuzz smoke runs. Fuzz smoke is not exhaustive fuzzing. |
| Live database behavior | Successful parallel stable Scylla v4/Snappy race lane (10 selected top-level tests), successful Cassandra 5.0.9 v5/LZ4 race lane (five selected top-level tests), and a final targeted live cache race test after the discovery-only correction. |

The new live concurrent-DDL test reads 64 rows into caller storage with three-row
pages while repeatedly creating/dropping another table. It checks plan reuse,
terminal destination clearing and a retained first-page payload. It uses unique
per-run tables with cleanup; stable/Cassandra fixture lanes remain exclusive.

## Diagnostics and evidence limits

The original rare `TestPagingWithBind` metadata error was not reproduced by the
single clean predecessor comparison lane: it passed. The first predecessor run
failed on stale task tables, which were cleaned in the exclusively reserved lane.
The unsafe old global-generation comparison is addressed by deterministic
reordered invalidation/retained-response tests and the new scoped implementation;
there is no claim that a clean predecessor live failure was observed here.

The initial v5 metadata-ID fixture failed with replicas reporting unknown/new
columns or incompatible schema. Retained Cassandra logs show it wrote after
CREATE/ALTER before those changes reached replicas. The helper claimed an exact
connection flow but created through an unpinned helper and checked agreement via
another host. CREATE and ALTER now both execute on the inspected coordinator and
wait through its existing all-node agreement implementation before dependent
writes. All metadata-ID/cache assertions are preserved. General public/automatic
coordinator-inclusive agreement remains a required step 05 feature.

Exploratory failures are retained, including a test helper initially missing its
result-entry epoch, an overbroad first conflict-refresh implementation replaced
by scoped validation, and an overly strict query fixture that rejected the
relevant scoped DESCRIBE. Those earlier rounds are not final-gate evidence.
No passing tests from a failed package lane count as completed coverage.

The successful v5 lane passes the native metadata-ID subtest and explicitly
skips its v4-only Scylla extension sibling. The stable lane separately exercises
Scylla empty metadata-ID repair. Skips are retained individually, not relabeled
as passes. These focused lanes do not discharge the full 174-test release matrix.

## Performance and consumption

Steady Page.Next/NextInto, direct warm prepared selection, exact result-plan hits
and history lookups allocate zero in the final focused benchmarks. History lookup
is roughly 6–12 ns at 1/1,024/65,536 retained tokens, with full collision equality.
A 500 KiB borrowed row varies fragmentation independently (1 versus 1,024 spans);
NextInto writes the destination without a complete-row temporary or payload copy.
Warm prepared selection is roughly 33 ns and plan entry selection roughly 10 ns
in these primitive measurements. Setup, explicit materialization, network/TLS,
compression, end-to-end throughput and final-tree benchmarks remain separate.

Steps 04/07 should reuse preparedSchemaStamp/immutable entries and pagingHistory
for streaming, keeping streaming and buffered decoder caches separate. Keep
shared PREPARE detached from input payloads and preserve its shutdown admission.
Step 05 owns remaining metadata/control behavior, general agreement and full
background-worker orchestration. Step 07 still owns settled statement-kind and
execution-policy/config completion; this step freezes and checks the current
bind-time conditional routing classification rather than claiming that broader
migration complete. Steps 14–17 must rerun release-wide verification on their
resulting final tree.

The step 03 gate is satisfied by the final command/source manifest. No
unresolved assigned step 03 finding remains. Current-source full checks and
targeted cache validation cover the final discovery-only change; prior focused
full live lanes remain explicitly associated with their source snapshot.

Evidence formatting: committed plain-text log copies expand tabs and remove
trailing whitespace to satisfy Git checks. Byte-exact raw originals and hashes
are retained under the external step03-raw-evidence archive recorded by
step03-log-formatting.json; JSON event streams remain byte-exact. Commands and
outcomes are unchanged. Retained external command-runner snapshots support
provenance; reruns must use fresh evidence paths and exclusive fixture ownership.
