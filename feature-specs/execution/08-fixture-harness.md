# Step 08: reusable fixtures and native harness

Owner: `/root/step08_fixture_harness`; branch: `v2-08-fixture-harness`.
Accepted predecessor: `f4a5b430f95195f5ae3ad105940b7d176abcbf97`.
Exact commands, statuses, log/config/install/source hashes, test identities,
runner PIDs and cleanup evidence are in
[`step08-verification.json`](../evidence/step08-verification.json).
The reusable setup contract is [`tests/fixtures/README.md`](../../tests/fixtures/README.md).

## Behavior

The native runner uses explicit pinned install paths and hashes, task-address
flocks, occupied-endpoint checks and fresh random data directories. It never
changes CCM CURRENT or resets shared containers. START, native OPTIONS/SUPPORTED
health, node stop/start, test subprocesses, namespace cleanup and STOP/join run
inside one live runner. Source snapshots, copied runner scripts, node configuration
hashes, actual versions, exact commands and exit statuses are retained. Logs and
receipts survive failures. SIGINT/SIGTERM cancel and join the test subprocess,
then clean its exact namespace before stopping and joining the database.
Provisioning verifies mandatory archive hashes, rejects existing destinations and
unsafe paths, and unpacks into a new confined destination. Environment variables
supply installation locations; the checked-in example has no personal paths.
The retained run configuration records this environment's concrete paths.

Typed shared helpers, maintenance, recreation and streaming fixtures use random
per-run namespaces and isolated object IDs. Cleanup drops only that run's prefix.
The deliberate failing and paused/canceled real-server probes both created an
owned keyspace, then proved its removal and final endpoint refusal. A second
runner while the cancellation probe was paused failed address-lock acquisition.
Python tests independently cover lock conflict/release, health short reads,
hash rejection before provisioning, safe archive extraction and failed-command
receipt/log retention. SIGKILL/power loss cannot execute cleanup; retained exact
owned roots and PIDs are the bounded recovery surface.

The test logger synchronizes writes and String snapshots. Cloud fixtures use a
fresh capture, authenticated backend credentials, bounded context-aware dialing,
tracked accepted/backend connections and idempotent close that joins handlers
and copy workers. Cancellation joins a connection stalled before ClientHello.
The reconnect helper supports typed picker membership/targets, removes a snapshot
under admission locks and closes outside pool locks; ticket tests still require
new handshakes and real resumption on each reconnect round.

Cassandra schema recreation has equivalent Java state/final functions alongside
Scylla Lua. A quote/$$-aware splitter preserves Java semicolons. Original golden
comparison, dump execution and second-dump identity assertions remain. Both
implementations additionally compute average(2,4,9)=5 before and after recreation.
Distribution-specific compression and materialized-view inputs satisfy each
server's grammar while preserving resulting schema assertions.

Cassandra's test-only query-handler subclass executes ordinary QUERY/EXECUTE/BATCH
through its real processor and native payload mirror. Only writes to this run's
exact `_fail` keyspace throw an injected WriteFailureException. Cassandra itself
serializes v4/v5 errors, error maps, custom payloads, LZ4 segments and TLS. This
proves injected-fault handling, not a natural storage failure. Incomplete peers
are appended to real uncompressed v4 metadata responses by an independent native
wire parser; system tables are never mutated and no driver Marshal/parser shim
constructs the injected row.

`internal/testkit` independently builds ROWS frames and v5 raw/literal-LZ4 units
with CRC24/CRC32. Public RunStream fixtures cover every byte split and truncated
prefix, every checksum byte, first-row gates, interrupted request writes, raw
v3/v4/TLS and v5 raw/LZ4 receive, seeded final storage, and close/context/session
shutdown during the final caller write. Successful net.Pipe TLS fixtures wait for
consumer acknowledgment before closing: net.Pipe rejects deadline setters after
peer close while TLS may still retain decrypted bytes. Truncation fixtures close
immediately. Direct-read tests count actual returned byte ranges, including TLS
short reads, and prove the decoded plaintext enters the final caller ranges.

Two narrow runtime defects exposed by these gates were corrected: initial
control connection errors now preserve errors.Is/As and server error codes;
disabling write timeout clears the absolute startup deadline in shared, span and
coalesced writes. Deadline setter failures still reject the write. Unit regressions
cover all three write paths and preserved bootstrap sentinel/server causes; real
rejected-credential and long-lived Cassandra schema lanes prove the boundaries.

## Verified gates

All eight final checks passed: uncached full unit race, focused race twice, vet,
integration and CCM race builds, Python harness tests, public streaming/affected
transport/TLS benchmarks and copy contracts. The final identity-probe correction
changed only one integration fixture file after the unit/performance gate;
both affected race build tags and every native package used the final source.
The 580-file frozen snapshot still matches the worktree.

| Real server | Successful selected package lanes |
| --- | --- |
| Scylla 2026.3.4-0.20261018.daef9dad4a01 | Raw v3, raw v4, mutual-TLS v4; actual auth acceptance/rejection, Unix socket, schema recreation, cloud UUID SNI, TLS ticket resumption and public streaming. |
| Cassandra 5.0.9, Java 17 | Native v4 raw/TLS; v5 raw/LZ4, each plaintext/TLS; all recreation subtests including Java aggregate, echo QUERY/EXECUTE/BATCH, nullable payload values, write-failure maps, incomplete peers and public streaming. |
| Scylla 2027.1.0~dev-0.20261028.24d4faafa6a9 | Actual development SELECT without FROM and public streaming over v4; read-only adapter uses image digest sha256:f254ffaca9977cab0d344f30eea71106d2b217135bd188543577d8bf2290be22. |

All ten selected native packages passed with no test skips. Every lane performed
an actual owned node stop/start. Both native runners stopped and joined all nodes;
independent post-stop probes refused all reserved native/TLS/shard/internode/API
endpoints. Lifecycle failure/cancel receipts are expected failures, separately
verified for cleanup; earlier failed packages remain diagnostic evidence and do
not contribute partial passes.

The independent public API benchmark varies rows (1/32/256) separately from cells
(1 KiB/400 KiB/8 MiB). Final caller storage and immutable packet construction are
outside measurement; API setup, peer scheduling and first-unit stream/checksum
rebinding are included. Raw v3/v4 stays about 10.5–11.4 KiB and 87–88 allocations
across shapes; v5 raw stays about 143 KiB and v5 literal-LZ4 about 274 KiB, reflecting
bounded unit storage. TLS retains standard-library record costs, including more
allocations for large payloads. Actual-byte range checks and existing transform
budget/copy tests passed. These are fixture/API seam measurements, not universal
speedups, comprehensive peak-memory accounting or native database throughput.

## Handoff

Assigned fixture/harness scope is complete. Cassandra 5.0.9 supplies native
protocols 4 and 5; a separate Cassandra 4.x server was not installed or claimed.
Remaining hardcoded `gocql_test` callers and the complete 174-test reconciliation
remain step 09; full wide matrices remain 14/17, full native performance remains
15, and extraction/legacy removal remain their assigned later steps. Existing
fixture commands expose those requirements and do not silently rewrite CQL or
replace skips with passes. Public API and streaming ownership/copy contracts were
preserved.
