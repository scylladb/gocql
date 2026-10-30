# Native integration fixtures

`native.py` owns provisioning, start, protocol health, tests, namespace cleanup,
node stop/start and final stop/join within one live Python runner. Use a Python
venv with scylla-ccm and the Go/Java tools required by the selected installation.
It never selects CCM's `CURRENT` cluster or changes shared container storage.

Copy `config.example.json` outside the repository, fill in exact versions and
SHA-256 hashes, and provide installation locations through environment variables.
Each server binary/JAR is checked before creating a cluster. The runner supports
Scylla relocatable installations and read-only Scylla container distribution
layers (`layout: scylla-container`, with an explicit `config` YAML source). The
latter adapter invokes that distribution's loader and libraries without starting
or changing a container. Cassandra is copied into an isolated install overlay;
its original files remain read-only. The verified Cassandra 5.0.9 server supports
native protocol 4 and 5. A separate Cassandra 4.x server installation is not part
of step 08's verified matrix.

```sh
python tests/fixtures/native.py --config /path/to/pinned-config.json \
  --state /path/to/task-owned-state --output /path/to/receipt.json
```

Use `--lane NAME` to select named lanes. Configuration paths accept environment
variables. `--state` must be the same absolute directory for all cooperating
runners: per-address `flock` reservations are stored there and fail immediately
on conflicts. The verified task reservations are stable `127.0.20.1..3`,
Cassandra `127.0.21.1..3`, and development `127.0.22.1..3`. The runner rejects
other addresses, occupied native/TLS/shard/internode/API endpoints, and existing
installation hash mismatches. It announces its exact reservation and creates a
fresh random data directory. It cannot arbitrate another program ignoring these
locks; endpoint checks add an independent guard. Never launch two processes that
use the same server lane outside this runner.

The retained environment uses `ccm` distribution version 2.0.5 from a source
archive whose SHA-256 is
`991348ec07346cd8c8f89e1714e18a010355e085e8dff669e792de02ae0d8f96`.
The runner records every installed CCM Python source hash. `requirements.lock`
records the exact dependency versions used here. For a new environment, create a
private venv, install those dependencies, then install the matching retained CCM
archive with `pip install --no-deps /path/to/archive.zip`. A mutable archive URL
must never be treated as that pin. The archive or available installation must be
supplied explicitly; the harness does not silently download a different build.
`provision.py --archive ... --sha256 ... --destination ...` verifies and unpacks
pinned server/source archives into new confined directories. `--url` permits an
explicit download with the same mandatory content digest. Existing destinations
and unsafe archive members are rejected.

Each runner chooses a random `GOCQL_TEST_RUN` identifier. Typed shared keyspaces,
recreation keyspaces, maintenance keyspaces and public streaming fixtures use it;
objects remain isolated by their test/session IDs. The cleanup subprocess drops
only this exact run's namespaces and runs after success, failure, SIGINT or
SIGTERM, before stopping the database. Shutdown waits for every owned node and
checks that reserved endpoints refuse connections. Cleanup failures fail the
receipt; they do not silently turn into successful lanes. Interrupted runs retain
logs and data beneath the task root for diagnosis. SIGKILL/power loss cannot run
cleanup; recover only the exact retained owned root and PIDs after inspecting the
receipt. Other lanes and historical data must remain untouched.

Integration CQL call sites and codec identities use the run namespace explicitly;
there is no CQL rewrite at a driver boundary. The complete original identity and
assertion mapping is retained in `feature-specs/evidence/step09-test-mapping.json`.
Two-shard lanes set `smp: 2`; plaintext and TLS shard endpoints are explicitly
19042 and 19142, respectively, alongside native 9042 and 9142.

During tests the same live runner holds a mode-0600 Unix control socket inside a
private temporary directory. `GOCQL_FIXTURE_CONTROL` and `GOCQL_FIXTURE_NODE`
identify it and the first owned node. It accepts only status, stop/join and
start/health for exact nodes from that locked cluster. Streaming stops the actual
coordinator after its first delivered row, verifies terminal failure without
replay, restarts it and probes pool recovery. A separate real client performs
ALTER between pull-driven pages, because the stream owns its configured pool
capacity across those pages. Setting `ccm: true` additionally runs the original
TestCCM through a finite CLI facade targeting this socket, without CCM CURRENT.
The control worker joins before final cluster stop; command/PID/address/path and
health evidence are retained in the native receipt. Cleanup disables event
subscriptions, drops only enumerated owned namespaces, waits for agreement and
queries the schema again to prove none survived.

The checked-in `testdata/pki` material is dedicated test PKI, with a trusted
server URI identity and client certificates. Its exact hashes are included in
source snapshots. Scylla requires client certificates and password auth. Tests
prove successful and rejected credentials, cloud UUID SNI and ticket resumption.
Cassandra uses its real PKCS12/TLS implementation. Certificate generation remains
in `testdata/pki/generate_certs.sh`; run it only in a new scratch directory with
copied configurations when replacing the dedicated fixture material.

`FixtureQueryHandler.java` extends Cassandra's own payload-mirroring handler.
Ordinary QUERY, EXECUTE and BATCH requests execute on the real database. Only
modifications to the exact per-run `_fail` keyspace throw an injected
`WriteFailureException`; Cassandra itself serializes errors, payloads, native v5
segments, LZ4 and TLS. This proves client handling of injected server responses,
not a natural database storage failure. `TestInvalidPeerEntry` uses a separate
uncompressed v4 transport fixture that appends an incomplete peer to a real
response without changing any system table. Its wire metadata parser and bytes
come from `internal/testkit`, independently of the driver parser/serializer.

Cassandra Java and Scylla Lua aggregates both compute the average of non-null
inputs. Schema recreation retains dump comparison, execution of the dump, and
second-dump identity checks. The fixture additionally computes the same average
before and after recreation. Java function bodies require a CQL splitter that
respects quoted and `$$` bodies. Distribution-specific table compression and
materialized-view inputs preserve the same resulting table/view schema.

`internal/testkit` also provides independent ROWS frames and CRC-protected v5
units. Literal-only LZ4 blocks test receiver correctness independently of the
production compressor; actual Cassandra lanes provide compression interoperability.
Public `RunStream` fixture tests cover every split/truncated prefix, every checksum
byte, first-row gates, request-write interruption, direct final ranges and
close/cancel/session shutdown during the last caller write, across v3/v4/TLS and
v5 raw/LZ4. Successful TLS net.Pipe fixtures keep the peer alive until consumption
is acknowledged because net.Pipe rejects deadline setters after peer close even
when TLS still has decrypted bytes; truncation fixtures close immediately.
The public API benchmark constructs immutable responses and final caller storage
outside measurement, varies rows/cell sizes independently, and includes peer
scheduling plus first-unit stream/checksum rebinding. It is not native throughput
or a complete peak-memory/CPU comparison.

```sh
python3 -m unittest discover -s tests/fixtures -p 'test_*.py'
go test -race -tags unit -run '^TestStep08' -count=2 .
go test -tags unit -run '^$' -bench '^BenchmarkStep08' -benchtime=20x .
```

`TestStep08HarnessLifecycle` is an explicit failure/cancellation probe. It skips
unless `GOCQL_FIXTURE_ACTION=fail|cancel` is configured. It creates a real owned
keyspace and writes `GOCQL_FIXTURE_MARKER`, then fails or waits for cancellation.
These are deliberate failed receipts, retained separately from passing DB gates.
