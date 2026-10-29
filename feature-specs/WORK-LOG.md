# Work log and completion gates

Task: extract comprehensive Rust driver feature specs, perform ten fresh-agent
review/fix rounds per section, design and implement a Go-specific API with generic
serialization and zero-copy hot paths, adapt the existing gocql integration tests,
and run them successfully.

User clarification (2026-10-29): this is **gocql v2**; no backward compatibility
is needed. The new API may replace legacy signatures and representations outright.
Integration tests must be migrated to the v2 interface with behavioral coverage
preserved. Do not retain legacy APIs or introduce adapters merely for compatibility.

## Required gates

- [x] Six source-grounded reference sections and complete production-module catalog.
- [x] Ten separate-agent review/fix rounds for each reference section (60 reports).
- [ ] Go API specification, explicit generic codec contracts, buffer ownership,
  cancellation/concurrency rules, and migration mapping for every integration test.
- [ ] Implementation satisfies the API specification; no compatibility shim is
  substituted for the requested generic/zero-copy execution paths.
- [ ] All existing integration tests adapted with their behavior assertions intact.
- [ ] Unit/race/ownership/allocation checks pass with recorded evidence.
- [ ] Complete database integration matrix passes; skips and environment limits
  are individually audited rather than counted as passes.
- [ ] Final requirement-by-requirement completion audit.

## Current state

2026-10-29: Empty workspace populated with the pinned upstream gocql baseline.
Rust source and optional specification template cloned into separate sibling
directories. No pre-existing user files were present. Reference drafting started.

Baseline `go test ./...` passed but does not exercise unit-tagged tests.
`go test -tags unit ./...` initially failed because fixture certificates were
absent; after `make .prepare-pki`, the complete command passed (exit 0), recorded
in `evidence/baseline-unit-with-pki.log`. No Go source edits were needed.

Dedicated test cluster: three rootless Podman containers `gocql-api-node1..3`,
Scylla 2026.3.4, two shards and 2 GiB each, loopback addresses `127.0.20.1..3`,
native/shard-aware ports 9042/19042 and TLS 9142/19142. Image digest
`sha256:2a3cf99b55d793a41e19b4d8afa4084744bc05517f4b4a211483c8d2abe4ef3a`.
Configuration lives in `/extra/scylladb/gocql-api-test-cluster`.
`nodetool -h 127.0.20.1 status` showed all three nodes UN. This cluster belongs to
this task; unrelated existing clusters were not modified.

Baseline integration command (completed, exit 0):

```sh
go test -json -tags 'integration gocql_debug' -timeout=20m . -args \
  -distribution=scylla -gocql.timeout=60s -proto=4 -rf=3 -clusterSize=3 \
  -autowait=2000ms -compressor=snappy -gocql.cversion=2026.3.4 \
  -cluster=127.0.20.1,127.0.20.2,127.0.20.3
```

Output: `evidence/baseline-scylla-v4-snappy.jsonl`. 573 passing test/subtest events,
38 skipped events, zero failures. Individual outcomes are retained in
`evidence/baseline-scylla-summary.json`. Skips are not counted as exercised coverage;
protocol v5, TLS, maintenance socket, auth, unsupported server features, and
pre-existing unconditional skips still need their appropriate validation.
The baseline proves the current driver, not the new API (which is not implemented).

All six reference drafts are written. Fresh-agent round 01 is in progress across
the six sections; connections round 01 is complete and round 02 is running.
`python3 feature-specs/verify.py --allow-pending` checks source catalog membership,
pinned link/anchor validity, required headings, and review report structure. It
does not prove behavioral completeness. The final documentation gate omits
`--allow-pending`.

Additional baseline checks:

- Hot-path benchmarks completed (exit 0), output in
  `evidence/baseline-hot-path-benchmarks.log`; existing token-aware picking reports
  7 allocations per operation in the measured cases.
- Scylla v4/LZ4/TLS suite is running (exec session `63013`). It has exposed existing
  harness defects: shard-port tests force plaintext port 9042 despite TLS config;
  cloud test queries system.local/system.peers through potentially different
  coordinators and can miss a host. Its maintenance socket path also exceeded the
  Unix socket address limit; a shorter symlink now exists at
  `/extra/scylladb/gocql-api-test-cluster/cql.m`. Preserve these failures for the
  migration phase; do not count this lane as passing.
- Cassandra 5.0.9 image acquired (digest
  `sha256:b85d75da9e1069317540e64b70a7376e633917c40288c93e2514eeadf8b7020b`).
  Task-owned `gocql-api-cassandra1..3` containers at `127.0.21.1..3` are starting;
  config enables optional TLS, client certificates, UDFs and materialized views.
  JMX is at localhost ports 7191..7193. Two nodes were confirmed UN before the
  third was started. This will exercise protocol v5 cases unavailable on Scylla.
- Scylla nightly image pull is running (exec session `49317`), to exercise the
  existing SELECT-without-FROM tests unsupported by stable 2026.3.

Current baseline inventory includes pre-existing unconditional skips in
TestUnpreparedBatch, TestWriteFailure, and custom payload tests; these require
explicit investigation before claiming all requested behavior is verified.

Baseline follow-up evidence and infrastructure fixes:

- Stable Scylla TLS run completed with 570 passes, 36 skips, five failures.
  `baseline-scylla-tls-summary.json` records each failure. The cloud fixture now
  pins peer discovery to the local-query coordinator and closes/checks scanners;
  maintenance socket test explicitly disables TCP TLS; shard-port tests preserve
  the configured transport port, and their dialer honors that port. The shared
  no-reconnection helper now reports session/pool setup failures instead of
  silently returning. Five focused cases passed initially; the remaining
  unreachable-port case passed after its port-helper fix. Unit shard-port tests
  also pass. These preserve assertions and improve baseline validity, not v2 API
  implementation.
- CA generation now requests the configured `req_ext` extensions. Existing test
  leaf keys/certs were retained while the CA certificate was regenerated with
  the same key and valid CA constraints. Both leaf chains verify. Cassandra TLS
  now negotiates with verified client/server chains; related unit tests pass.
- Cassandra v5/LZ4/TLS baseline after the CA fix is running in exec `5151`, log
  `evidence/baseline-cassandra-v5-valid-ca.jsonl`. The earlier setup failure is
  retained in `baseline-cassandra-v5-lz4-tls.jsonl`.
- Three-node Scylla nightly at `127.0.22.1..3`, version
  `2027.1.0~dev-0.20261028.24d4faafa6a9`, image digest
  `sha256:3c724c0a3a535c19297640d88f33ebe0bf9794d7babd8dc506ec72eb543590f7`:
  complete v4/snappy baseline passed (exit 0), 612 pass events, 22 skips, zero
  failures. `SELECT 1` was independently verified. Log and per-test summary are
  `evidence/baseline-scylla-nightly-v4-snappy*`.
- Maintenance-socket focused baseline passes with the shorter symlink.
- Isolated CCM cluster setup is in progress under
  `/extra/scylladb/gocql-api-test-cluster/ccm`; use `CCM_CONFIG_DIR` for every CCM
  operation so unrelated user clusters are untouched. Native package reference is
  `release:2026.3.4`. Initial bare version attempt failed before downloading;
  corrected setup log is `evidence/ccm-setup-release.log`.

Reference review progress: every section has completed rounds 01 and 02;
round 03 is underway, with connections already in round 04. Actual reports and
hash chains are authoritative. Serialization round 03 additionally confirmed
`cargo +1.94.1 check -p scylla-cql-core --no-default-features` and 228 core library
unit tests pass, correcting round 02's mistaken source-only feature-gating claim.

Cassandra v5/LZ4/TLS baseline completed (exit 0): 582 pass events, 29 skips,
zero failures, retained in `baseline-cassandra-v5-valid-ca-summary.json`.
Isolated CCM `TestCCM` passed with real native Scylla (4.03s); output and exit 0
are in `baseline-ccm*`. CCM start and test must share one live exec command:
background native processes do not survive the tool command's process-group
cleanup. Use default CCM startup options (2 shards/1024M); a single quoted string
in `--jvm_arg` is passed as one invalid Scylla option by this installed CCM.

Across baseline lanes, 166/174 original top-level tests have an observed pass.
Remaining tests with no pass:

- `TestInvalidPeerEntry`: unconditional skip; intended incomplete-peer handling,
  disabled because original implementation mutates system tables. Rewrite with
  a faithful metadata/transport fixture during migration rather than counting skip.
- `TestUnpreparedBatch`: unconditional flake skip; inspect counter-batch semantics.
- `TestCreateSession_DontSwallowError`: unconditional skip; legacy assertion depends
  on old server error strings despite locally invalid protocol value 256.
- `TestAuthentication`: opt-in, requires a server actually enforcing authentication.
- `TestWriteFailure`: needs Cassandra `cassandra.test.fail_writes_ks=test` fixture
  and error-unwrapping assertions appropriate to wrapped driver errors.
- `TestCustomPayloadMessages`, `TestCustomPayloadValues`: need Cassandra's built-in
  `org.apache.cassandra.cql3.CustomPayloadMirroringQueryHandler` enabled. Class was
  verified in the 5.0.9 jar and official tagged source. Existing assertions must
  be executed, with iterator errors/close checked.
- `TestTLSTicketResumption`: unconditional old-version skip; helper overwrites TLS
  client certificate config, and ticket-count equality is not itself evidence of
  successful resumed handshakes. Preserve the intended resumption behavior with
  actual TLS connection-state evidence.

The task-owned Cassandra configs now enable the write-failure/custom-payload
fixtures used by the upstream Makefile; container restart is in progress. No
integration tests have been silently counted as passing through these skips.

All 174 original top-level integration/CCM tests now have at least one observed
pass across the pre-v2 baseline and corrected harness lanes. Exact per-test
evidence is `evidence/baseline-all-tests-coverage.json`. This is not v2 verification.

Recent reactivations and fixes:

- `-runcassandrafixtures` selects the real Cassandra server fixtures; Makefile's
  Cassandra lane enables it. Write-failure checks unwrap the driver error; custom
  payload iterators now close/check errors. All three tests passed on v5/LZ4/TLS.
- Unprepared batch assertions (one row, counter value 100) pass unchanged after
  removing the unconditional skip. Invalid session protocol test now asserts the
  local configuration error is preserved, avoiding obsolete server text.
- TLS ticket test preserves fixture client certificate/CA settings and checks
  `tls.ConnectionState.DidResume` and new handshake counts across two real pool
  replacements. Its helper previously returned without closing open pools; this
  reversed condition is fixed, under the pool lock. Ticket-cache eviction with a
  nil state is handled. Focused TLS resumption passes on Scylla nightly.
- Auth helper applies credentials to bootstrap probes when `-runauth` is set.
  Native Scylla now needs explicit `auth_superuser_name` and salted password in
  addition to PasswordAuthenticator; merely waiting for an implicit default role
  does not work. Explicit role setup passed readiness and wrong-password rejection
  probes, then `TestAuthentication` passed. Logs: `auth-explicit-role-probes.log`,
  `password-authentication-explicit-role.jsonl`. The native fixture was stopped.
- `TestInvalidPeerEntry` now uses `metadata_peer_fixture_test.go` to append an
  incomplete row to actual Cassandra native-v4 transport responses, without
  mutating system tables. It verifies injection occurred during discovery, the
  invalid host is excluded, all valid hosts remain, and a real query succeeds.
  The fixture explicitly scopes itself to uncompressed native v4/Cassandra.
- Full stable Scylla v4/LZ4/TLS after the first harness fixes passed: 575 pass
  events, 36 skips, zero failures (`scylla-v4-lz4-tls-harness-fixed*`). Later
  reactivated cases were verified separately as listed above.

Current processes: focused race runs for new metadata/Cassandra fixtures (exec
`52784`, `metadata-server-fixtures-race.jsonl`) and reactivated Scylla tests (exec
`31216`, `reactivated-tests-race.jsonl`). Poll these handles before re-running.
Reference reviews: all sections completed round 07; rounds 08/09 are progressing,
and connections round 10 is completing. Actual reports/hash chains are authoritative.

## Resumed state after usage interruption

The user requested continuation. Current files verify 59 of the required 60
review reports with valid hash chains; execution round 10 was interrupted before
its report was saved and is being resumed in the same independent agent.
The pending integration-API audit is also being resumed; the completed hot-path
audit is retained in `evidence/go-hot-path-audit.md`.

Both focused race runs finished with exit 0: four Cassandra transport/server
fixture tests and three reactivated Scylla tests. Their JSONL logs and summaries
are saved under `evidence/`. All nine task-owned database containers remain
running and available. No v2 implementation or API-spec completion is claimed.

The final execution review resumed and completed. The strict command
`python3 feature-specs/verify.py` now passes: 144 catalogued Rust modules, all six
sections, 418 pinned code links, 60 distinct-agent reports, continuous per-section
review hashes and final-file hashes. The source-reference gates are complete.
Go v2 API and implementation specifications are now being authored, with the
integration API and transport audits informing exact implementation boundaries.

Module groundwork: changed the root module and production/test imports to
`github.com/scylladb/gocql/v2`; the benchmark module now requires local v2. Removed
v1 retractions, which are invalid under a v2 module path. The standalone LZ4
compressor module remains independent. Root unit-tagged package compilation,
integration test-binary compilation, and benchmark-module compilation pass.
Evidence: `v2-module-build.log`, `v2-module-integration-build.log`, and
`v2-module-bench-build.log`. This verifies module wiring only, not new API behavior.

The complete integration API dependency audit is saved in
`evidence/go-integration-api-audit.md`: all 174 tests, eight disjoint migration
groups, fourteen private-access families, binding/result/codec patterns and
explicit dispositions needed for old mutable API assertions.

## V2 implementation in progress

Concrete API and implementation contracts are now saved in `gocql-v2-api.md`
and `gocql-v2-implementation.md`. Independent API review corrected typed row
cloning, composition arity, type/function name collisions, frozen metadata's
unknown state, standalone typed value encoding/decoding for binary fixtures, and
built-in policy/custom-partitioner configuration. Implementation is actively
integrating; the root package can temporarily fail compilation as runtime and
transport files are added. No new-API integration pass is claimed yet.

Completed foundation components:

- `internal/wire`: immutable borrowed chains, stable reference-count leases,
  checked segmented cursors, scalar slabs and transactional builder markers.
  Worker reports race/vet/fuzz passed; root reran package tests successfully.
- `internal/wirecompress`: real LZ4/Snappy block encoders consume span descriptors
  directly, with independent decoder oracles, race/vet/32-bit/fuzz checks. Input
  is not gathered. Preallocated output uses zero allocations through eight spans;
  larger descriptor-index allocation does not scale with payload bytes.
- `internal/murmur.Stream` hashes the original segmented partition components
  without composite-key materialization. Random/high-byte inputs and every split
  through 255 bytes, plus 1 MiB inputs at multiple span sizes, match the existing
  contiguous algorithm; race tests pass. `v2_routing_token.go` uses this primitive.
- Root `v2_type.go`, `v2_view.go`, `v2_value_io.go` supply full immutable metadata,
  borrowed views and checked cell framing. Scalar codecs and generic parameter/
  row composition are written; integrated tests await compiling runtime seams.

Ownership: transport worker owns conn/frame/framer/segment and v2_transport files;
execution worker owns session and v2 statement/execute/result/profile/batch/config;
policy worker owns v2 policy/metadata/auth/events and host/event publication hooks.
Root owns v2 row composition and streaming token helper. Legacy RetryPolicy was
renamed internally to oldRetryPolicy to free the public v2 interface name; the
UUID generator is NewTimeUUID, freeing TimeUUID for the typed CQL value. These
mechanical changes are not test migration completion.

### Current implementation and verification checkpoint

All foundational typed codec/transport tests passed together under the race
detector (`v2-integrated-race.log`). New standalone core components also pass
their race checks. Segmented container iteration initially allocated a descriptor
array per cell; Chain range views now share immutable descriptor tails with a
trimmed endpoint. Race tests verify nested ranges and zero-allocation Take/Slice.
The 1,024-element segmented iterator now uses two setup allocations total, as
does contiguous iteration. Typed tuple/row scratch is reused without retaining
payloads; returned values are copied before clearing scratch. Pair composition
reuses scoped reader/writer state. `v2-container-range-bench.log` records results.

New typed runtime database smoke passed on stable Scylla with ~600 KiB blob
values, prepared parameters, automatic paging, borrowed page retention, clones
and binding input retirement. Full wire-level typed tests cover v3/v4/v5,
compression, TLS, segmentation, partial writes, cancellation/orphans and limits;
see `evidence/v2-transport-validation.md`. This is new-API evidence, separate
from the original baseline passes.

Migration ownership/current state (files may still be under test/fix):

- `migrate_main_integration`: all 74 original cassandra_test.go identities are
  rewritten, live stable-Scylla verification/fixes ongoing.
- `migrate_serialization_integration`: 33 original tests in five files are
  rewritten; nightly Scylla matrix running, golden binary fixtures retained.
- `migrate_control_integration`: 18 original tests in four files rewritten;
  isolated auth/Scylla CCM race lanes pass, Cassandra/TLS/multi-node checks ongoing.
- Root: all 16 SELECT-without-FROM tests rewritten; compile passes; live nightly
  lane awaits serialization worker releasing its database reservation.
- Typed schema mock tests now exercise the new raw metadata backend, preserving
  the old error/count/concurrency assertions. Focused schema/mock race passes.
- Remaining test groups (network, session/batch/tracing, metadata/policy recreation)
  still require direct API migration and verification. Old public Query/Iter/etc
  removal and system/control query migration are not complete.

Live fixture reservations: main tests on 127.0.20.1..3 (stable Scylla),
serialization on 127.0.22.1..3 (nightly), control on 127.0.21.1..3 (Cassandra).
Do not reset reserved keyspaces while another process uses that lane.
The isolated CCM authentication fixture at 127.0.23.1 must be started/tested/stopped
in a single exec command; helper currently leaves it stopped after tests.

Full unit race run after typed schema mock migration is in exec session `20152`,
output `evidence/v2-unit-race-after-mocks.log`; poll before restarting. Earlier
`v2-stage-unit-race.log` failed on stale mock injection and the legacy split-frame
allocation assumption; those tests were migrated to typed fixtures/leased spans
and must be revalidated in the current integrated run.

## Review ledger

Drafting does not count toward review rounds. Reports will be named
`reviews/<section>-round-<01..10>.md` and include reviewer identity.
