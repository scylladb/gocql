# Typed replay workloads

This directory belongs to the root `github.com/scylladb/gocql/v2` module.
`go test ./tests/bench` performs ten SELECT and ten INSERT operations against
checked-in native-protocol captures. SELECT case `i` must return exactly one
row containing `Name_i`; INSERT case `i` writes `(i, i % 5, Name_i)` and must
succeed. Benchmark cases preserve the same ten workloads and assertions:

```sh
go test -race -count=3 ./tests/bench
go test -tags 'unit bench' -run '^$' -bench SingleConnection -benchtime=5x -benchmem ./tests/bench
```

Control and data connections are recorded separately as `connection-0` and
`connection-1`. Connection selection uses an atomic ordinal only while dialing;
there is no serialization lock on measured operations. The configuration opens
one data connection, disables periodic metadata refresh/keepalives/configuration
reporting, fixes protocol 4 and uses the fixture's known `cassandra` credentials.
Close joins driver I/O, then checks every recorder's cached descriptor-close
result before generation succeeds. A reconnect or an unexpected connection
count fails the workload.

To regenerate, use a dedicated **owned** ScyllaDB fixture at an explicit native
endpoint. The native fixture runner retains exclusive address locks through
start, test and stop; its step 13 mode sets `GOCQL_REGENERATE_BENCH=1` and runs
`TestStep13RegenerateReplay`. The underlying command is:

```sh
GOCQL_BENCH_ADDRESS=127.0.20.1:9042 \
  go test -race -count=1 -run '^TestSingleConnectionReplay$' ./tests/bench \
  -args -update-golden
```

The update command clears only these two recording directories, creates
`single_conn_bench.table1`, records the original ten inserts and matching ten
reads, closes both sessions, and immediately runs the operational replay test.
Run it only on the dedicated fixture: it intentionally recreates that fixed
benchmark keyspace. The native runner then runs its namespace cleanup and joins
all owned nodes. No container reset or unrelated workload is involved.

Captures retain original wire bytes, stamps and stream IDs. Typed startup
currently reads `system.local` twice. The loader collapses a repeated request
only if both request and response frames are byte-identical apart from stream
IDs. Different requests with one hash or conflicting repeated results are
errors. Unmatched operational requests still panic; there is no CQL rewrite,
request fallback or old-control compatibility path.

Generation source/config/server/capture hashes, actual operation results and
cleanup receipts are recorded in
[step13-verification.json](../../feature-specs/evidence/step13-verification.json).
The old page-5000/timestamp captures and their failed operational replay remain
historical evidence in the accepted predecessor and step 10/12 reports; they do
not count as successful current replay. These are benchmark smoke checks.
Comprehensive native throughput, scaling and retained-memory measurements
remain step 15.
