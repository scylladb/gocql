# Go integration-test API audit before v2 design

This is a **factual pre-v2 migration audit, not a Go API design**. It inventories the tests named by [integration-baseline.json](../integration-baseline.json), their current public call patterns, and the fixture/internal dependencies that must be accounted for when migrating to a clean v2 implementation. It does not prescribe replacement APIs or require backward compatibility with existing syntax.

Scope: **174 named top-level tests in 29 files**, plus shared helpers used by those files. The manifest is based on revision `1ceaac8172bda26e8b8d701ac356982d33729b27`; this audit describes the current working-tree source. No tests were executed, no driver or test code was edited, and existing changes were preserved. The complete manifest-test appendix below records every named test, its current source location, and selected directly visible calls. Files with no named tests still matter to integration compilation and fixtures.

## Migration-critical findings

1. **The suite is coupled to both the public API and the runtime's private representation.** Most files use package `gocql`; cloud tests use package `gocql_test` through integration-only exports. Prepared-cache injection, connection pinning, metadata-cache invalidation, pool refill/state manipulation, direct frame construction, and negotiated-feature inspection cannot be migrated by renaming public query methods.
2. **Binding and decoding include substantially more than scalar positional parameters.** The suite covers callback binding based on prepared metadata, repeated rebinding, named values, unset values, null pointers, tuple expansion, reflected UDTs, custom whole-value/UDT codecs, dynamic maps, nested collections, vector subtypes, raw encoded-cell reads, and the same values in conditional batches.
3. **Many assertions deliberately pin old mutable API semantics.** Query defaults/setters, metrics stored on requests, public batch entries, `MapScan` pointer replacement, tuple flattening, old null-to-zero conversions, and query pooling are observable in the tests. Those assertions need an explicit migration disposition; preserving server behavior does not automatically preserve their current API mechanics.
4. **Meaningful protocol coverage depends on fixture gates and negotiated state.** v5 segmentation/compression, v4 metadata-ID extensions, TLS resumption, cloud SNI, authentication, Unix sockets, Cassandra error/payload fixtures, and no-FROM SELECT probes each have distinct prerequisites. Merely keeping test names compiling would not preserve the exercised paths.
5. **The manifest includes local implementation tests as well as live-database tests.** `TestRetryOnTopologyBusy`, session/query/batch basic API tests, statement classification, consistency-name checks, and vector subtype parsing are examples. `TestCCM` has a separate build tag and controls database nodes. Three manifest files contain zero named tests.
6. **The current suite does not establish a borrowed-row lifetime contract.** Most results are copied into Go destinations; dynamic-map tests rely on ordinary retained values and raw `DirectUnmarshal` output is retained. Existing `Iter` exhaustion/close and scanner-error patterns are migration dependencies, but should not be mistaken for tests of new borrowing, leases, or safe view retention.

## How to read the inventory and counts

The numerical inventory counts syntactic selector calls inside only the 174 named test bodies, including nested closures/subtests. Comments and string literals are removed before counting. Counts are call sites, not runtime executions, do not expand loops, and do not include separate helper bodies or benchmarks. They are deliberately not a type-checked call graph: `.Query` includes session queries and batch entries, `.Scan` includes query/iterator/scanner calls, and `.Bind` includes query rebinding and session/batch callbacks. Qualitative findings include the separately inspected helper bodies. The generated tables and appendix follow the narrative.

Separate helpers significantly expand coverage: `runMetadataChangedFlow`, `ClusteredKeyValue.Bind`, the serialization matrix helpers, `WikiTest` methods, the shard-port test helpers, and common DDL/setup functions all contain driver calls not counted in top-level bodies.

## Shared fixture and runtime dependencies

| Source | Dependencies and behavior to retain during migration |
|---|---|
| [common_test.go](../../common_test.go), `TestMain`, `createCluster` | CLI flags select hosts, protocol/CQL versions, replication factor, expected cluster size, retries, discovery wait, SSL, auth, compression, timeouts, distribution/version, and Unix-socket paths. Config uses `NewCluster`, exported mutable `ClusterConfig` fields, `Quorum`, `SimpleRetryPolicy`, `PasswordAuthenticator`, `SslOptions`, `SnappyCompressor`, and the LZ4 subpackage. Options are `func(*ClusterConfig)` mutations. |
| `addSslOptions` | Selects Cassandra port 9042 or Scylla port 9142 and uses `testdata/pki/{gocql.crt,gocql.key,ca.crt}`. The fixture uses a client certificate and CA, not just an insecure TLS boolean. |
| Tablet setup in `common_test.go`; `integration_test.go` init | Runs tablet feature probes before the tests, through `system.local` and `DESCRIBE KEYSPACE` dynamic map scans. Creates/drops a probe keyspace. Shared keyspaces distinguish tablets-enabled/default and tablets-disabled fixtures. |
| `createKeyspace`, `createSessionFromClusterHelper` | Copies config, uses `system`, sets a longer timeout, resets the copied host policy to avoid sharing an initialized policy between sessions, creates keyspaces once, mutates `cluster.Keyspace`, and waits through `session.control.awaitSchemaAgreement`. |
| `createTable`, `createTables` | Execute DDL with retry/idempotence settings, call private control schema-agreement methods, and invalidate private table/keyspace schema caches via `metadataDescriber`. Their deterministic cache visibility is part of many metadata tests. |
| `testTableName`, `testTypeName`, `testKeyspaceName` | Produce unique bounded CQL identifiers from test names; parallel tests depend on this isolation. Common helpers also create views, materialized views, functions, and aggregates. |
| `waitForHostConns`, `getRandomConn`, `pollUntil` in [cassandra_test.go](../../cassandra_test.go) | Inspect `session.hostSource`, `session.pool`, pool `Pick`, `session.getConn`, `HostInfo` state and addresses. Wait for one usable connection on every expected host before assertions about per-host negotiated state. |
| [scylla_shard_aware_port_common_test.go](../../scylla_shard_aware_port_common_test.go) | `waitUntilPoolsStopFilling` reads private pool filling state; fullness checks use picker size; refill calls `fill_debounce`. These helpers are also used outside shard-port tests, including cloud and config-reporting tests. |
| [integration_only.go](../../integration_only.go) | Adds integration-only `Session.MissingConnections` and `CloseAllConnections`; directly closes/clears picker connection arrays and triggers refill. It declares `ConnPickerIntegration` using `Token` and `ExecutableQuery`, so even the testing facade depends on the old execution interfaces. TLS resumption uses this facade. |
| [export_test.go](../../export_test.go) | Exposes flags, `CreateCluster`, a logger, pool-fill waiting, and `GetRingAllHosts` to external-package cloud tests. `GetRingAllHosts` wraps the private host source. This file has no named tests but is required by them. |
| [metadata_peer_fixture_test.go](../../metadata_peer_fixture_test.go) | New/current helper outside the manifest's file list. Wraps real connections, tracks QUERY/PREPARE/EXECUTE streams and prepared IDs, parses response frames with `newFramer`/`parseFrame`, appends an incomplete peer encoded through `Marshal`/`appendBytes`, and adjusts row count/frame length. Requires plaintext, uncompressed Cassandra v4 with result metadata enabled. |
| [internal/tests/serialization/valcases](../../internal/tests/serialization/valcases) | `GetSimple` supplies 20 CQL scalar types, 96 named binary-value cases, and 196 language cases in the current table. `LangCase.Value` is `any`; typed nils are built with reflection. These cases feed marshal, unmarshal, database round-trip, and raw-cell checks. |

## Public binding, result, and codec patterns

| Pattern | Concrete users and migration dependency |
|---|---|
| Positional values and pointer inputs | `Session.Query(stmt, values...).Exec/Scan/Iter` dominates. Scalars include integer widths and conversions, floats, booleans, strings/blobs, UUID/timeuuid, `net.IP`, `time.Time`, `time.Duration`, `Duration`, `big.Int`, and `inf.Dec`; pointers and typed nil pointers are exercised. The wiki fixture binds nine heterogeneous fields, including decimal pointer, sets, and a map with named `[]byte` values. |
| Reusable queries | `TestRebindQueryInfo` binds 23, consumes, then binds 24 on the same query. `TestPagingWithBind` executes a paged query, rebinds different bounds, and executes again. The routing benchmark in `stress_test.go` repeatedly mutates one query per worker. |
| Callback metadata binding | `TestStaticQueryInfo`, `TestBoundQueryInfo`, `TestBatchQueryInfo`, `TestSessionBindRoutingKey`, and basic API tests use `Session.Bind` or `Batch.Bind` with `func(*QueryInfo) ([]any,error)`. `ClusteredKeyValue.Bind` reads `QueryInfo.Args`, matches column names to Go fields by reflection, and returns field pointers. These callbacks depend on metadata arriving before execution, not just static variadic arguments. |
| Named/unset/null values | `TestQuery_NamedValues` uses three `NamedValue` sites. `TestUnsetCol` and `TestUnsetColBatch` contain five `UnsetValue` references and verify unchanged existing cells. Nil query values, empty timestamps, nullable tuple members, nullable nested collection fields, null UDTs, and empty/nil collections have independent assertions. |
| Positional decoding | `Query.Scan` returns a single-row error; `Iter.Scan` returns bool and errors are obtained on `Close`. Nil scan destinations skip columns. Tests depend on `ErrNotFound` for absent single rows, count mismatch errors, and both reused destination variables and pointer-to-pointer null decoding. |
| Scanner | `TestObserve_Pagination` advances `Scanner.Next` without calling `Scan`, testing per-page observations while discarding row values. Cloud tests use `Next/Scan/Err`, and `Err` is their terminal cleanup operation. This is a distinct cursor consumption pattern. |
| Dynamic rows | `MapScan` and `SliceMap` are used for system tables, schema/metadata changes, ordinary data, no-FROM generated column names, tuples, collections, UDTs, and vectors. `TestMapScanWithRefMap` preloads a map with destination pointers; the scan fills the variables and replaces map entries with values. Other keys are allocated dynamically. |
| Dynamic type/null defaults | `TestSliceMapMapScanTypes` checks 20 scalar types plus null results; separate tests cover counter, tuples, two vector subtypes, and list/set/map. Old defaults include CQL int as Go `int`, inet as string, varint/decimal pointers, null scalar zero values, null slices/maps, and the current null-date sentinel. Tests distinguish nil from empty containers and compare set membership without relying on order. |
| Tuples | Nine `tuple_test.go` tests cover scalar tuple members expanded into extra scan destinations; null/unset tuple cells; map keys derived from tuple-member position; trailing empty member; list-of-tuple values represented as `[][]any`, arrays, or structs; and nullable nested members. A physical tuple column is not treated as one positional destination by the old top-level scan API. |
| UDTs | Eight `udt_test.go` tests cover a custom `position` codec, reflected structs with `cql` tags, null object reset, dynamic `[]map[string]any` nested UDT results, missing fields, nil collections, field updates, and null custom UDT decoding. Missing fields and null resets are behavior assertions, not merely type support. |
| Custom codecs | Exactly two user-defined codec types appear in the manifest files: `FullName` implements whole-value `MarshalCQL/UnmarshalCQL`; `position` implements per-field `MarshalUDT/UnmarshalUDT` and delegates field coding to `Marshal/Unmarshal`. The serialization matrix also scans into `DirectUnmarshal` to compare raw bytes. Tests named vector “Marshaler” exercise built-in collection/UDT encoding; they do not introduce additional custom codec implementations. |
| Serialization matrix | `checkTypeMarshal` and `checkTypeUnmarshal` call `Marshal/Unmarshal` directly using `NewNativeType(4, Type(...))`; `checkTypeInsertSelect` sends each language representation, creates reflected destinations, and compares a raw `DirectUnmarshal` result. Expected encode/decode failures and float bit patterns are included. This corpus is wider than a handful of typed round trips. |
| Vectors | Six vector tests cover fixed/variable-length members, numerous scalar and nested collection/tuple/UDT representations, empty data and wrong dimensions, plus seven private parser cases for subtype nesting. Tests use `any` tables and reflection to allocate destinations. Cassandra 5.0 and different Scylla 2025.3/2025.4 gates apply. |
| Conditional results | `TestCAS`, `TestMapScanCAS`, and `TestScanCASWithNilArguments` cover applied flags, failure row values, skipped destinations, count errors, invalid use on non-CAS queries, and dynamic maps. `ExecuteBatchCAS` returns an iterator for additional rows; callers scan the remaining rows including `[applied]`. These tests also check `ErrNotFound` for a non-CAS batch. |
| Batch construction | Both `Batch`/`NewBatch` plus `ExecuteBatch`, and chained `Batch.Query(...).Exec`, are present. Prepared entries, text-only entries, callback entries, logged/unlogged batches, timestamp/now/keyspace options, custom payload, maximum count, and argument count failures are covered. `TestProto1BatchInsert` actually sends textual BEGIN/APPLY BATCH through `Session.Query`; its name does not establish a protocol-v1 fixture. |
| Metadata and observations | `Iter.Columns`, `NumRows`, `Host`, and `PageState`; `Query.Keyspace/Table`; `Session.GetHosts`, metadata methods and session ID; query/batch observers, tracing, attempts and latency all appear. `ObservedBatch.Values` is explicitly asserted as `[][]any`. Observer success means valid execution even when single-row `Scan` reports no row. |

## File/group inventory

Every manifest file is represented here. The appendix supplies the complete individual test list; grouped names below identify the implementation dependency of each cluster of assertions.

| File / named tests | Public/runtime dependency and special private access |
|---|---|
| [batch_test.go](../../batch_test.go) — 4 | Invalid statements in batches; explicit timestamps; v5 `WithNowInSeconds` TTL arithmetic; v5 batch keyspace override. Uses mutable batch builders and `session.cfg.ProtoVersion` gates. |
| [cass1batch_test.go](../../cass1batch_test.go) — 2 | Textual CQL batch via positional `[]any`; `Query.shouldPrepare` classification with direct `Query{stmt,routingInfo}` construction. |
| [cassandra_test.go](../../cassandra_test.go) — 74 | Detailed grouping below: CRUD/codec/paging/CAS/observer behavior plus direct connection execution, prepared/routing cache state, schema caches, protocol framing and feature negotiation. |
| [client_routes_test.go](../../client_routes_test.go) — 1 | Populates a synthetic client-route table at consistency ALL, then calls private `getHostPortMappingForConnectionIDs`/`ForPairs` through `session.control`; compares private `clientRoute` and `pair` fields, including plaintext/TLS port choice. ALL writes matter because control reads use ONE. |
| [cloud_cluster_test.go](../../cloud_cluster_test.go) — 1 | External package; cloud config YAML, `scyllacloud.NewCloudCluster`, custom `DialContext`, real TLS ClientHello inspection through a local SNI proxy, host-targeted reads of system metadata, exported test hooks, and exactly `NumConns` SNI observations per discovered host. Scylla SSL fixture only. |
| [control_integration_test.go](../../control_integration_test.go) — 1 | Unix maintenance socket via custom dialer; explicitly disables TLS, uses v3, one connection, no initial lookup/reconnect/coalescing, and disables event subscriptions. Requires `-cluster-socket`. |
| [dns_test.go](../../dns_test.go) — 1 | Mutable custom `DNSResolver`, address translator, one-name-per-node/cluster and broken-first-address cases. Calls private `refreshRingNow` and counts `hostSource` hosts. |
| [errors_test.go](../../errors_test.go) — 1 | Duplicate DDL must unwrap to `RequestErrAlreadyExists` with expected table name. |
| [exec_test.go](../../exec_test.go) — 1 | Standalone `NewSingleHostQueryExecutor`, iterator scan of timeuuid bytes, iterator/executor cleanup. |
| [export_test.go](../../export_test.go) — 0 | Integration test exports required by external cloud tests; see shared helpers. |
| [integration_serialization_scylla_test.go](../../integration_serialization_scylla_test.go) — 7 | One deterministic topology-busy retry-helper test; scalar byte/round-trip matrix; five dynamic scan groups covering scalar, counter, tuple, vector, and collections. Reflection, `unsafe` float-bit comparison, `TypeInfo`, `DirectUnmarshal`, and `valcases` dependency. TRUNCATE has a separate bounded topology-contention retry helper. |
| [integration_test.go](../../integration_test.go) — 14 | Auth; discovery/filtering; application startup fields; session ID/driver-config reporting; Cassandra write failures/custom payload mirroring; schema agreement; low timeout matrix. Private host source/policy arrays/pool maps/control connection, `driverConfigReport` constants, system-query method and control reconnect. |
| [internal/ccm/ccm_test.go](../../internal/ccm/ccm_test.go) — 1 | Separate `all || ccm` build constraint and package. Calls `AllUp`, `Status`, `NodeDown`, `NodeUp`; requires a CCM cluster/node1 and performs disruptive lifecycle actions. No gocql query API dependency. |
| [keyspace_table_test.go](../../keyspace_table_test.go) — 1 | Fully qualified statement, token-aware policy, context/consistency, `Query.Keyspace/Table` updated after preparation. Calls private schema agreement. |
| [policies_integration_test.go](../../policies_integration_test.go) — 5 | Invalid DC validation for token/DC/rack policies; token policy initialized with/without keyspace; all-hosts-down does not hang. Inspects concrete `tokenAwareHostPolicy`, token ring/replica metadata; mutates pool host state and policy AddHost/RemoveHost. All-hosts-down intentionally runs without `t.Parallel`. |
| [recreate_test.go](../../recreate_test.go) — 1 | Seven schema groups: keyspace, table, materialized views, index, secondary index, UDT, aggregates. Reads CQL/goldens from `testdata/recreate`; compares `KeyspaceMetadata.ToCQL`; optional `-update-golden` writes fixtures. Uses private schema refresh/invalidation, control DESCRIBE, `getCreateStatements`, and internal `frm.ErrorFrame`. Includes explicit `Query.Release`. |
| [schema_queries_test.go](../../schema_queries_test.go) — 1 | Token-aware session plus direct `metadataDescriber.GetKeyspace`. |
| [scylla_shard_aware_port_common_test.go](../../scylla_shard_aware_port_common_test.go) — 0 | Shared runtime test functions; ten concurrent sessions in no-reconnect case, custom source-port/destination-port dialers, `scyllaShardForSourcePort`, private picker assertions (`*scyllaConnPicker`, `nrShards`), pool refill/fullness and host fields. |
| [scylla_shard_aware_port_integration_test.go](../../scylla_shard_aware_port_integration_test.go) — 4 | Delegates all behavior to the preceding helpers: no redundant reconnects, source-port-changing NAT, unreachable shard port fallback, and explicitly disabled shard port. Uses the current cluster/TLS setup through its cluster factory. |
| [select_without_from_integration_test.go](../../select_without_from_integration_test.go) — 16 | No-keyspace session, literal/function/cast/multiple/alias metadata, filtered empty result, three execution modes, typed bind-marker contexts, routing fallback, maps, aggregate/JSON, keyspace/UDF resolution, and post-error connection health. Private `skipPrepare`, `q.conn`, `session.getConn`, and internal server-error codes. Runtime capability probe distinguishes syntax unsupported from real failures; Scylla-specific. |
| [session_event_bus_integration_test.go](../../session_event_bus_integration_test.go) — 2 | `SubscribeToEvents` filtered channels, typed schema/control events, `Stop`; invokes private `control.reconnect`. Schema-event test avoids parallel execution and filters its unique keyspace. |
| [session_test.go](../../session_test.go) — 8 | Session/query/batch API-state assertions, classification/stringification, retry policy observation, TLS resumption. Directly constructs incomplete sessions/queries/hosts, builds private pools/executor, installs metrics, inspects private fields, and uses integration-only connection reset hooks. |
| [stress_test.go](../../stress_test.go) — 0 | Two benchmarks still compile under the integration tag: concurrent Query/Exec and repeated Bind/GetRoutingKey. Not part of the 174 named-test execution count. |
| [tablet_integration_test.go](../../tablet_integration_test.go) — 1 | Token-aware CRUD and tablet-route hint stabilization, `Iter.GetCustomPayload`; private host addresses and `unmarshalTabletHint`. Tablet feature probe gates execution. |
| [tracer_test.go](../../tracer_test.go) — 1 | `NewTracer`, request tracing, all trace IDs, readiness polling, activities and coordinator timing; live server trace tables. |
| [tuple_test.go](../../tuple_test.go) — 9 | Expanded positional/member-map decoding, null/empty/unset tuple semantics, reflected nested tuple destinations and nullable members. |
| [udt_test.go](../../udt_test.go) — 8 | Per-field custom codecs, reflected tagged structs, missing fields, nested dynamic UDT maps, null reset and empty collection behavior. |
| [vector_test.go](../../vector_test.go) — 6 | Built-in vector serialization and nested types; reflected heterogeneous case matrix; null/empty/dimension behavior. `TestVector_SubTypeParsing` directly uses framer writes/`readTypeInfo` and constructs `NativeType` private fields. |
| [wiki_test.go](../../wiki_test.go) — 3 | Helper-driven realistic schema/insert/select/count with UUID, time, decimal pointer, set strings, map of named blob values. File also contains benchmarks outside the manifest's count. |

### The 74 tests in cassandra_test.go

- **Setup/discovery/errors:** `TestEmptyHosts`, `TestInvalidPeerEntry`, `TestUseStatementError`, `TestInvalidKeyspace`, `TestCreateSessionTimeout`, `TestReconnection`, `TestCreateSession_DontSwallowError`, `TestControl_DiscoverProtocol`. Includes explicit sentinels, low connection timeout, private `handleNodeDown`/host state, negotiated `session.cfg.ProtoVersion`, and the out-of-manifest incomplete-peer wire fixture.
- **Observability:** `TestTracing`, `TestObserve`, `TestObserve_Pagination`, `TestQueryStats`, `TestIterHost`, `TestBatchStats`, `TestBatchObserve`. Covers session/request tracers and observers, valid-but-empty query observation, page row counts, host association, post-execution attempts/latency, and original `[][]any` batch values.
- **Paging/binding:** `TestPaging`, `TestPagingWithAllowFiltering`, `TestPagingWithBind`, `TestManualQueryPaging`, `TestRebindQueryInfo`, `TestStaticQueryInfo`, `TestBoundQueryInfo`, `TestBatchQueryInfo`, `TestSessionBindRoutingKey`. Includes tombstones/empty pages, manual state read after Close, same-query rebinding, runtime metadata callbacks and callback routing fallback.
- **CAS/batches/validation:** `TestCAS`, `TestConsistencySerial`, `TestMapScanCAS`, `TestBatch`, `TestUnpreparedBatch`, `TestBatchLimit`, `TestWhereIn`, `TestTooManyQueryArgs`, `TestNotEnoughQueryArgs`, `Test_RetryPolicyIdempotence`, `TestScanWithNilArguments`, `TestScanCASWithNilArguments`. Includes exact serial-consistency panic strings, 65,537-entry rejection, conditional batches with remaining rows, custom retry policies inspecting `QueryError.IsIdempotent`, and mismatched binding counts.
- **Scalar/dynamic serialization:** `TestDurationType`, `TestMapScanWithRefMap`, `TestMapScan`, `TestSliceMap`, `TestSmallInt`, `TestMarshalFloat64Ptr`, `TestMarshalInet`, `TestVarint`, `TestNilInQuery`, `TestEmptyTimestamp`, `TestJSONSupport`, `TestUnmarshallNestedTypes`, `TestUnsetCol`, `TestUnsetColBatch`, `TestQuery_NamedValues`. Uses `FullName` custom codec, pointer-filled maps, named primitive/container representations, nulls, nested collections and explicit JSON strings.
- **Preparation/cache/schema:** `TestPrepare_MissingSchemaPrepare`, `TestPrepare_ReprepareStatement`, `TestPrepare_ReprepareBatch`, `TestQueryInfo`, `TestPrepare_PreparedCacheEviction`, `TestPrepare_PreparedCacheKey`, `TestGetKeyspaceMetadata`, `TestSessionMetadataAPIs`, `TestGetKeyspaceMetadataFails`, `TestSchemaReset`. Private direct execute/prepare calls, injected `inflightPrepare`/`preparedStatment`/`NativeType`, LRU size/order/key access, and schema refresh/invalidation. Metadata APIs check columns and key ordering, indexes/views, ALTER/drop/recreate, missing/empty table/keyspace, and `ErrNotFound`/`ErrNoTable`/`ErrNoKeyspace`.
- **Routing/protocol/options:** `TestRoutingKey`, `TestTokenAwareConnPool`, `TestNegativeStream`, `TestQuery_SetHostID`, `TestQuery_WithNowInSeconds`, `TestQuery_SetKeyspace`, `TestLargeSizeQuery`, `TestCompressorNegotiated`, `TestQueryCompressionNotWorthIt`, `TestPrepareExecuteMetadataChangedFlag`, `TestStmtCacheUsesOverriddenKeyspace`, `TestRoutingKeyCacheUsesOverriddenKeyspace`, `TestPrepareExecuteScyllaEmptyMetadataID`. Checks exact single/composite routing bytes/index/type/cache entries, private host/pool state, manually built negative stream frame, query pinning/unprepared mode, per-host compressor negotiation, v5 segments, and v4/v5 result metadata IDs.

## Protocol, TLS, auth, cloud, and feature fixture constraints

- **v3/v4/v5 are separate paths.** Unix-socket test explicitly selects v3; unset requires v4+; now-in-seconds/keyspace override and large segmented frame tests require v5. Protocol auto-discovery sets protocol zero and disables compression, then inspects the negotiated config. Invalid protocol is a local configuration error assertion. A test named Proto1 is not pinned to protocol 1.
- **Compression is verified after negotiation.** `TestCompressorNegotiated` obtains one usable connection per expected host and inspects `conn.compressor`; config alone is insufficient. `TestQueryCompressionNotWorthIt` requires v5 plus a negotiated compressor. `TestLargeSizeQuery` round-trips a 500,000-byte string and is explicitly v5-gated. These round trips alone do not expose a packet-level assertion for every claimed compression detail.
- **Result metadata IDs have two gates.** `TestPrepareExecuteMetadataChangedFlag` covers native v5 and v4 `SCYLLA_USE_METADATA_ID`. The latter checks server advertisement and actual `usesMetadataID`; advertisement without negotiation is a failure, not a skip. `runMetadataChangedFlow` pins all statements to one connection, alters schema, checks row decoding, reinjects stale prepared metadata, and checks cache replacement/identity and METADATA_CHANGED flags. The empty-ID test injects a prepared statement with a nil result metadata ID and verifies recovery. These require controllable private cache generations and connection affinity in the test harness.
- **Cassandra-only injected failures/payloads:** `TestWriteFailure`, `TestCustomPayloadMessages`, `TestCustomPayloadValues` require distribution Cassandra and `-runcassandrafixtures`. The write-failure case expects `RequestErrWriteFailure`, with host error-map behavior differing in v5. Custom payload tests expect a server mirroring fixture, including nil maps and nil values.
- **Incomplete peer fixture:** `TestInvalidPeerEntry` explicitly forces Cassandra v4, no TLS/compression, and `DisableSkipMetadata`. The wrapper parses wire frames and marshals an extra row. It is independent of system-table mutation and must stay reachable in the replacement suite.
- **Auth:** `TestAuthentication` only runs under `-runauth` and constructs `PasswordAuthenticator` with fixture credentials. Common cluster setup also applies auth to other sessions. This inventory does not imply coverage of every custom authenticator/provider handshake.
- **TLS resumption:** Scylla SSL fixture, version 2025.2+ gate, cloned TLS config preserving fixture cert/CA, custom `tls.ClientSessionCache`, and `VerifyConnection` counting `DidResume`. Test waits for pool fullness and forcibly replaces connections twice. Ticket cache population alone is insufficient; it asserts resumed replacement handshakes.
- **Cloud:** Requires Scylla SSL on 9142, `datacenter1`, temporary YAML referencing certificates, host UUID SNI names under `cloud.scylladb.com`, custom dial redirect, and ring/pool hooks exposed by `export_test.go`. Host metadata queries are pinned to a common coordinator view. SNI observations and expected `NumConns` are part of the outcome.
- **Shard-aware ports:** Custom dialers log source/destination ports, change source port to mimic NAT, or permit only the ordinary port. The shared tests validate per-shard fill, fallback, disabled-port behavior and TLS-vs-plaintext advertised port choice. They depend on current picker/pool internals rather than just Session queries.
- **No-FROM SELECT:** The 16 tests use a Scylla-only capability probe (`SELECT 1`), skip syntax-unsupported, and fail other errors. They compare prepared, prepared-with-metadata, and forced plain QUERY; no session keyspace; metadata aliases/types/source; routing errors/fallback; and healthy subsequent use of the same connection after rejections. The UDF test uses Lua and only treats explicit server rejection as unsupported.
- **Schema and tablets:** Shared probe/default keyspaces, tablets-disabled CAS/recreate fixtures, distribution-specific recreation CQL/goldens, version-gated vector support, and private schema invalidation affect which cases truly execute. Schema-event and shared HostInfo mutation tests intentionally avoid parallel execution.

## Assertions tied to the old API or private representation

These are existing dependencies to disposition during migration, not requirements that v2 preserve their exact mechanics:

- `TestSessionAPI`, `TestQueryBasicAPI`, `TestBatchBasicAPI` construct internal objects, mutate defaults/setters and public fields, and assert private storage (`cons`, `pageSize`, `prefetch`, `trace`, `rt`, `values`, `binding`, metrics). They also expect old not-ready/closed sentinel behavior and idempotent close.
- Rebinding mutates a request in place; manual `PageState(nil)` disables auto-paging and successive states are assigned back to the same query. `TestPagingWithAllowFiltering` reads state/LastPage after iterator Close and asserts nonterminal manual pages are not empty.
- Request metrics remain readable through `Query.Attempts/Latency` and `Batch.Attempts/Latency`; basic tests manually seed metrics and call `AddAttempts`. Retry tests inspect retryable requests and the old error wrapper. Batch observers explicitly receive original `[][]any` inputs.
- Tuple scans expand one column into several destinations; dynamic tuple maps expose member-index names. Dynamic map scans can consume preseeded pointers, overwrite entries with values, allocate the other entries, and rely on old automatic Go type/null mappings.
- Serial-consistency validation asserts specific panic values; scan count failures assert an error prefix; multiple tests compare sentinels directly. A typed validation/error model will require deliberate test translation rather than accidental retention of these strings.
- `Query.Keyspace/Table` are learned through execution/preparation. `GetRoutingKey` exposes the old discovery/cache behavior, including expected ErrNoKeyspace/ErrNotFound for no-FROM bind markers while execution still succeeds through routing fallback.
- `q.skipPrepare` and `q.conn` force execution modes and exact connection affinity. Cache tests directly install stale/invalid prepared entries, read pointer identity and response flags, inspect LRU capacity/order, and access routing cache indexes/types/keyspace. Those are implementation assertions distinct from public result correctness.
- `Query.Release` is explicitly used in schema recreation; other tests often rely on exhaustion or close for iterators and some only inspect metadata/host without closing. This audit records the patterns, not an endorsement of leaking or retaining resources under a new ownership model.
- `TestVector_SubTypeParsing` creates raw framers and `NativeType{typ,custom}` values. Classification tests call private `shouldPrepare`/`isUseStatement`. These are local engine tests under the integration build tag, not end-to-end API contracts.
- The suite has places where the assertion is narrower than the name: token-aware pool test has a TODO to verify the chosen host, nil-in-query checks the inserted ID rather than reading the null cell, and protocol1-named batch test does not force v1. Migration completeness should track the actual assertion as well as the name.

## Coverage boundaries

This audit does not execute or type-check the suite, prove dynamic test-case reachability, validate server-version claims externally, or inventory all tests outside the manifest. It reports checked current source. The scalar case counts are literal entries in the current `valcases/simple.go`, not successful executions. Benchmarks and helper files are dependency notes, not additions to the 174-test baseline. No public v2 API is specified here.

## Disjoint migration ownership groups

The following eight groups partition all 29 manifest files without overlapping file ownership. The counts total 174. They support direct translation to the eventual typed v2 API, not an old-API compatibility facade. Shared helper signatures and test-only access boundaries need to be agreed before the dependent groups change their call sites. In particular, `cassandra_test.go` should have one file owner even though its logical groups differ; concurrent function-level edits to that large file would need additional coordination.

| Group | Exclusive file ownership | Baseline tests | Coordination |
|---|---|---:|---|
| 1. Common fixtures and test access | `export_test.go`; supporting `common_test.go`, `integration_only.go`, `metadata_peer_fixture_test.go` | 0 | Own common setup, schema barriers, test naming, external-package exports and wire injection. `cassandra_test.go` helpers remain with group 2 and shard helper definitions with group 6 unless moved by explicit coordination. |
| 2. Main integration behavior | `cassandra_test.go` | 74 | Own direct query/bind/CAS/paging conversion plus cache/connection test helpers and prepared metadata scenarios. Depends on common fixture and core typed execution/lifetime contracts. |
| 3. Serialization and row shapes | `integration_serialization_scylla_test.go`, `tuple_test.go`, `udt_test.go`, `vector_test.go`, `wiki_test.go`; supporting `internal/tests/serialization/valcases` if changes become necessary | 33 | Own case-table adaptation, custom codecs, dynamic cases and container/null expectations. Coordinate codec requirements with production codec ownership; do not route the new typed path through legacy `Marshal/Scan`. |
| 4. Request/session semantics and tracing | `batch_test.go`, `cass1batch_test.go`, `session_test.go`, `exec_test.go`, `tracer_test.go`, `stress_test.go` | 16 | Own explicit disposition of mutable state assertions, standalone executor, tracing and benchmarks. TLS resumption test is here; its connection-reset facade belongs to group 1. |
| 5. Control-plane behavior and events | `integration_test.go`, `session_event_bus_integration_test.go`, `errors_test.go`, `control_integration_test.go` | 18 | Own auth/startup reports, system/control query timeouts, event subscription/reconnect, error typing, custom-payload fixture use and Unix sockets. |
| 6. Network discovery fixtures | `cloud_cluster_test.go`, `dns_test.go`, `scylla_shard_aware_port_common_test.go`, `scylla_shard_aware_port_integration_test.go` | 6 | Own DNS/dialers/SNI/source-port harness and shared pool-fill helpers. Coordinate exported access with group 1 and pool users in groups 2/5. |
| 7. Schema, routing, and virtual-table behavior | `client_routes_test.go`, `keyspace_table_test.go`, `policies_integration_test.go`, `recreate_test.go`, `schema_queries_test.go`, `tablet_integration_test.go`, `select_without_from_integration_test.go` | 26 | Own schema/route outcomes and explicit disposition of private policy/cache expectations, plus server-generated metadata/column naming. Group 1 owns common schema-cache barriers. |
| 8. CCM lifecycle | `internal/ccm/ccm_test.go` | 1 | Separate package/tag and disruptive cluster lifecycle. Does not depend on the query API migration. |

Implementation constraints supplied for the migration are compatible with these boundaries but are not finalized API names: typed operations will use package-level generic functions where Go methods cannot introduce type parameters; borrowed result/page lifetime must be explicit; routing and execution must consume canonical encoded inputs; cancellation must respect input and result ownership. Tests need to exercise those paths directly. Existing early iterator termination, per-page observations, metadata replacement, conditional multirow results and callback-built values are specific cases where a superficial call-name rewrite would miss the behavioral dependency.

## Fourteen private-access families to disposition

This is a count of **14 conceptual access families observed in this audit**, not a count of fields, calls, or required public v2 methods. Some old representation assertions may become irrelevant; the remaining scenarios need an intentional internal-test boundary or observable outcome. Exposing all old fields through a shim would preserve the coupling rather than complete the migration.

| # | Family | Concrete current access / principal users |
|---:|---|---|
| 1 | Session construction, effective config, readiness | `Session{cfg,cons,policy,logger}`, `cfg.PoolConfig.buildPool`, private executor construction, `session.cfg.ProtoVersion/Keyspace`; basic session tests and protocol gates. |
| 2 | Query/batch storage and metrics | `Query{stmt,routingInfo}`, `values`, `binding`, `trace`, `rt`, `pageSize`, `prefetch`, `metrics`, `preFilledQueryMetrics`, `hostMetrics`; basic API tests, batch entry binding. |
| 3 | Statement classification | `Query.shouldPrepare`, `isUseStatement`; `cass1batch_test.go` and `session_test.go`. |
| 4 | Prepared statement/cache generation | `stmtsLRU` locks/LRU/keyFor/get/add/clear, `inflightPrepare`, `preparedStatment`, request/response columns, prepared IDs, result metadata IDs, flags and pointer identity; reprepare/eviction/metadata-ID tests. |
| 5 | Routing-key preparation/cache | `routingKeyInfo`, indexes/types/keyspace, `routingKeyInfoCache`, `routingKeyInfoCacheKey`, `inflightCachedEntry`, `query.routingInfo`; routing-key/keyspace override tests. |
| 6 | Schema cache/control metadata internals | `metadataDescriber` get/refresh/invalidate functions, `getKeyspaceMetadata`, `getCreateStatements`, private schema agreement and DESCRIBE access; common DDL, metadata/recreate/schema tests. |
| 7 | Discovered host source and state | `hostSource.getHostsList/GetHostsFromSystem/hosts`, `refreshRingNow`, `handleNodeDown`, private HostInfo address/hostname/UUID/state; discovery, host targeting, DNS, tablet and reconnection tests. |
| 8 | Concrete host-policy metadata | `*roundRobinHostPolicy` host storage, `*tokenAwareHostPolicy.getMetadataReadOnly`, token ring tokens/hosts/partitioner and replicas; filter/policy initialization tests. |
| 9 | Connection pool/picker accounting and mutation | `pool.hostConnPools`, `getPool/getPoolByHostID`, locks, filling/fill_debounce, `connPicker`, concrete Scylla picker shard count/connection arrays; shard tests, wait helpers and TLS reset. |
| 10 | Direct connection/control execution and affinity | `session.getConn`, `q.conn`, `conn.executeQuery/executeBatch/prepareStatement/exec`, `getCurrentKeyspace`, `control.getConn/query/reconnect`, `querySystem`; preparation, metadata changes, negative streams, no-FROM recovery and timeout tests. |
| 11 | Raw framing/parser/type internals | `frameWriterFunc`, `newFramer`, `writeHeader/finish/readTypeInfo`, `f.buf/header`, `resultRowsFrame/resultPreparedFrame`, `appendBytes`, `NativeType{typ,custom}`, internal `frm` op/error types; negative-stream, vector parser and peer-injection fixtures. |
| 12 | Negotiated protocol/compression/capabilities | `conn.version`, `conn.compressor`, `scyllaSupported`, `usesMetadataID`, `isScyllaConn`, private `q.skipPrepare`; per-host compression, metadata-ID, report gating and forced plain QUERY cases. |
| 13 | Startup report representation | `sessionIDStartupKey`, `driverConfigStartupKey`, `driverConfigReport`, `driverConfigVersion`; config-reporting assertions in `integration_test.go`. |
| 14 | Client-route/tablet wire helpers | `controlConnection`, private `clientRoute`/`pair`, `getHostPortMappingForConnectionIDs/ForPairs`, `unmarshalTabletHint`; synthetic routing table and tablet hint tests. |


## Current-source verification and measured call sites

All 174 manifest test functions were found in the current source. At artifact completion, 14 files differ from the manifest SHA-256 values:

- `cassandra_test.go`
- `cloud_cluster_test.go`
- `control_integration_test.go`
- `integration_serialization_scylla_test.go`
- `integration_test.go`
- `keyspace_table_test.go`
- `recreate_test.go`
- `schema_queries_test.go`
- `scylla_shard_aware_port_common_test.go`
- `scylla_shard_aware_port_integration_test.go`
- `select_without_from_integration_test.go`
- `session_event_bus_integration_test.go`
- `session_test.go`
- `vector_test.go`

The other 15 manifest files match their recorded hashes. Seven additional differences appeared during this audit as concurrent module-import migrations to `github.com/scylladb/gocql/v2`: `integration_serialization_scylla_test.go`, `keyspace_table_test.go`, `recreate_test.go`, `schema_queries_test.go`, `select_without_from_integration_test.go`, `session_event_bus_integration_test.go`, and `vector_test.go`. Those inspected diffs change imports only. The previously changed seven files include the current fixture changes described above. Shared helpers outside the manifest are not covered by its hashes. No production-source pin is implied for changed test files.

| Selector | Direct call sites | Named tests containing it |
|---|---:|---:|
| `Query` | 326 | 124 |
| `Bind` | 12 | 9 |
| `Exec` | 128 | 87 |
| `Iter` | 53 | 41 |
| `Scan` | 113 | 78 |
| `Scanner` | 3 | 2 |
| `Next` | 4 | 2 |
| `MapScan` | 20 | 14 |
| `SliceMap` | 14 | 11 |
| `ScanCAS` | 9 | 2 |
| `MapScanCAS` | 4 | 2 |
| `Batch` | 25 | 16 |
| `NewBatch` | 6 | 4 |
| `ExecuteBatch` | 17 | 15 |
| `ExecuteBatchCAS` | 5 | 1 |
| `MapExecuteBatchCAS` | 3 | 1 |
| `GetRoutingKey` | 7 | 3 |
| `PageSize` | 7 | 6 |
| `Prefetch` | 1 | 1 |
| `PageState` | 5 | 2 |
| `Release` | 2 | 1 |
| `WithContext` | 8 | 4 |
| `Consistency` | 14 | 10 |
| `RetryPolicy` | 4 | 4 |
| `SetKeyspace` | 8 | 4 |
| `SetHostID` | 4 | 2 |
| `KeyspaceMetadata` | 11 | 2 |
| `TableMetadata` | 9 | 1 |
| `AwaitSchemaAgreement` | 5 | 4 |
| `Columns` | 9 | 9 |

The methods in this table overlap within tests. For example, a single chain contributes to Query, Iter and Scan counts; totals must not be added to infer a number of tests. Standalone helper methods and free functions are excluded by this counting method.

## Complete 174-test manifest appendix

Links point to current repository files; line anchors record this inspection snapshot. The selected-call column is a lexical aid, not a full dependency list. “Helper/local assertions” means the named body has no selected direct calls; use the preceding file inventory for its actual dependency, including delegated live-cluster scenarios.

### batch_test.go — 4 named tests

Build constraint: `integration`.

| Baseline test | Selected directly visible calls |
|---|---|
| [TestBatch_Errors](../../batch_test.go#L38) | `Query`, `Exec`, `Batch` |
| [TestBatch_WithTimestamp](../../batch_test.go#L57) | `Query`, `Exec`, `Scan`, `Batch`, `WithTimestamp` |
| [TestBatch_WithNowInSeconds](../../batch_test.go#L90) | `Query`, `Scan`, `NewBatch`, `ExecuteBatch`, `WithNowInSeconds` |
| [TestBatch_SetKeyspace](../../batch_test.go#L120) | `Query`, `Exec`, `Iter`, `Scan`, `NewBatch`, `ExecuteBatch`, `SetKeyspace` |

### cass1batch_test.go — 2 named tests

Build constraint: `integration`.

| Baseline test | Selected directly visible calls |
|---|---|
| [TestProto1BatchInsert](../../cass1batch_test.go#L36) | `Query`, `Exec`, `Consistency` |
| [TestShouldPrepareFunction](../../cass1batch_test.go#L58) | Helper/local assertions; see file inventory. |

### cassandra_test.go — 74 named tests

Build constraint: `integration`.

| Baseline test | Selected directly visible calls |
|---|---|
| [TestEmptyHosts](../../cassandra_test.go#L53) | `CreateSession` |
| [TestInvalidPeerEntry](../../cassandra_test.go#L64) | `Query`, `Scan`, `CreateSession` |
| [TestUseStatementError](../../cassandra_test.go#L102) | `Query`, `Exec` |
| [TestInvalidKeyspace](../../cassandra_test.go#L118) | `CreateSession` |
| [TestTracing](../../cassandra_test.go#L134) | `Query`, `Exec`, `Scan`, `Trace` |
| [TestObserve](../../cassandra_test.go#L179) | `Query`, `Exec`, `Scan`, `Observer` |
| [TestObserve_Pagination](../../cassandra_test.go#L276) | `Query`, `Exec`, `Iter`, `Scanner`, `Next`, `PageSize`, `Observer` |
| [TestPaging](../../cassandra_test.go#L332) | `Query`, `Exec`, `Iter`, `Scan`, `PageSize` |
| [TestPagingWithAllowFiltering](../../cassandra_test.go#L363) | `Query`, `Exec`, `Iter`, `Scan`, `PageSize`, `PageState`, `Consistency`, `NumRows`, `LastPage` |
| [TestPagingWithBind](../../cassandra_test.go#L500) | `Query`, `Bind`, `Exec`, `Iter`, `Scan`, `PageSize` |
| [TestCAS](../../cassandra_test.go#L544) | `Query`, `Exec`, `Scan`, `ScanCAS`, `MapScanCAS`, `Batch`, `ExecuteBatch`, `ExecuteBatchCAS`, `MapExecuteBatchCAS` |
| [TestConsistencySerial](../../cassandra_test.go#L725) | `Query`, `Exec`, `Scan` |
| [TestDurationType](../../cassandra_test.go#L815) | `Query`, `Exec`, `Scan` |
| [TestMapScanCAS](../../cassandra_test.go#L877) | `Query`, `MapScanCAS` |
| [TestBatch](../../cassandra_test.go#L919) | `Query`, `Scan`, `Batch`, `ExecuteBatch` |
| [TestUnpreparedBatch](../../cassandra_test.go#L948) | `Query`, `Scan`, `Batch`, `ExecuteBatch` |
| [TestBatchLimit](../../cassandra_test.go#L986) | `Query`, `Batch`, `ExecuteBatch` |
| [TestWhereIn](../../cassandra_test.go#L1008) | `Query`, `Exec`, `Iter`, `Scan` |
| [TestTooManyQueryArgs](../../cassandra_test.go#L1038) | `Query`, `Iter`, `SliceMap`, `Batch`, `ExecuteBatch` |
| [TestNotEnoughQueryArgs](../../cassandra_test.go#L1070) | `Query`, `Iter`, `SliceMap`, `Batch`, `ExecuteBatch` |
| [TestCreateSessionTimeout](../../cassandra_test.go#L1099) | `CreateSession` |
| [TestReconnection](../../cassandra_test.go#L1127) | Helper/local assertions; see file inventory. |
| [TestMapScanWithRefMap](../../cassandra_test.go#L1170) | `Query`, `Exec`, `Iter`, `MapScan` |
| [TestMapScan](../../cassandra_test.go#L1240) | `Query`, `Exec`, `Iter`, `MapScan` |
| [TestSliceMap](../../cassandra_test.go#L1288) | `Query`, `Exec`, `Iter`, `MapScan`, `SliceMap` |
| [Test_RetryPolicyIdempotence](../../cassandra_test.go#L1433) | `Query`, `Exec`, `Consistency`, `RetryPolicy`, `Attempts` |
| [TestSmallInt](../../cassandra_test.go#L1470) | `Query`, `Exec`, `Iter`, `SliceMap` |
| [TestScanWithNilArguments](../../cassandra_test.go#L1499) | `Query`, `Exec`, `Iter`, `Scan` |
| [TestScanCASWithNilArguments](../../cassandra_test.go#L1535) | `Query`, `ScanCAS` |
| [TestRebindQueryInfo](../../cassandra_test.go#L1583) | `Query`, `Bind`, `Exec`, `Iter`, `Scan` |
| [TestStaticQueryInfo](../../cassandra_test.go#L1627) | `Query`, `Bind`, `Exec`, `Iter`, `Scan` |
| [TestBoundQueryInfo](../../cassandra_test.go#L1699) | `Bind`, `Exec`, `Iter`, `Scan` |
| [TestBatchQueryInfo](../../cassandra_test.go#L1741) | `Bind`, `Iter`, `Scan`, `Batch`, `ExecuteBatch` |
| [TestPrepare_MissingSchemaPrepare](../../cassandra_test.go#L1933) | `Query` |
| [TestPrepare_ReprepareStatement](../../cassandra_test.go#L1959) | `Query` |
| [TestPrepare_ReprepareBatch](../../cassandra_test.go#L1977) | `Query`, `Batch` |
| [TestQueryInfo](../../cassandra_test.go#L1996) | Helper/local assertions; see file inventory. |
| [TestPrepare_PreparedCacheEviction](../../cassandra_test.go#L2019) | `Query`, `Exec`, `Scan` |
| [TestPrepare_PreparedCacheKey](../../cassandra_test.go#L2114) | `Query`, `Exec`, `Scan`, `CreateSession` |
| [TestMarshalFloat64Ptr](../../cassandra_test.go#L2167) | `Query`, `Exec` |
| [TestMarshalInet](../../cassandra_test.go#L2185) | `Query`, `Exec`, `Scan` |
| [TestVarint](../../cassandra_test.go#L2240) | `Query`, `Exec`, `Scan` |
| [TestQueryStats](../../cassandra_test.go#L2353) | `Query`, `Exec`, `Attempts`, `Latency` |
| [TestIterHost](../../cassandra_test.go#L2372) | `Query`, `Iter` |
| [TestBatchStats](../../cassandra_test.go#L2386) | `Query`, `Batch`, `ExecuteBatch`, `Attempts`, `Latency` |
| [TestBatchObserve](../../cassandra_test.go#L2420) | `Query`, `Batch`, `ExecuteBatch`, `Observer` |
| [TestNilInQuery](../../cassandra_test.go#L2485) | `Query`, `Exec`, `Scan` |
| [TestEmptyTimestamp](../../cassandra_test.go#L2510) | `Query`, `Exec`, `Scan` |
| [TestGetKeyspaceMetadata](../../cassandra_test.go#L2539) | Helper/local assertions; see file inventory. |
| [TestSessionMetadataAPIs](../../cassandra_test.go#L2574) | `Query`, `Exec`, `KeyspaceMetadata`, `TableMetadata` |
| [TestGetKeyspaceMetadataFails](../../cassandra_test.go#L2931) | Helper/local assertions; see file inventory. |
| [TestRoutingKey](../../cassandra_test.go#L2945) | `Query`, `GetRoutingKey` |
| [TestTokenAwareConnPool](../../cassandra_test.go#L3078) | `Query`, `Exec`, `Scan`, `Consistency` |
| [TestNegativeStream](../../cassandra_test.go#L3129) | Helper/local assertions; see file inventory. |
| [TestManualQueryPaging](../../cassandra_test.go#L3151) | `Query`, `Exec`, `Iter`, `Scan`, `PageSize`, `PageState`, `LastPage` |
| [TestSessionBindRoutingKey](../../cassandra_test.go#L3207) | `Bind`, `Exec` |
| [TestJSONSupport](../../cassandra_test.go#L3242) | `Query`, `Exec`, `Scan` |
| [TestUnmarshallNestedTypes](../../cassandra_test.go#L3290) | `Query`, `Exec`, `Scan` |
| [TestSchemaReset](../../cassandra_test.go#L3327) | `Query`, `Exec`, `Scan` |
| [TestCreateSession_DontSwallowError](../../cassandra_test.go#L3387) | `CreateSession` |
| [TestControl_DiscoverProtocol](../../cassandra_test.go#L3406) | `CreateSession` |
| [TestUnsetCol](../../cassandra_test.go#L3433) | `Query`, `Exec`, `Scan` |
| [TestUnsetColBatch](../../cassandra_test.go#L3466) | `Query`, `Scan`, `Batch`, `ExecuteBatch`, `Attempts`, `Latency` |
| [TestQuery_NamedValues](../../cassandra_test.go#L3512) | `Query`, `Exec`, `Scan` |
| [TestQuery_SetHostID](../../cassandra_test.go#L3539) | `Query`, `Exec`, `Scan`, `SetHostID` |
| [TestQuery_WithNowInSeconds](../../cassandra_test.go#L3587) | `Query`, `Exec`, `Scan`, `WithNowInSeconds` |
| [TestQuery_SetKeyspace](../../cassandra_test.go#L3617) | `Query`, `Exec`, `Scan`, `SetKeyspace` |
| [TestLargeSizeQuery](../../cassandra_test.go#L3687) | `Query`, `Exec`, `Scan` |
| [TestCompressorNegotiated](../../cassandra_test.go#L3741) | Helper/local assertions; see file inventory. |
| [TestQueryCompressionNotWorthIt](../../cassandra_test.go#L3770) | `Query`, `Exec`, `Scan` |
| [TestPrepareExecuteMetadataChangedFlag](../../cassandra_test.go#L3837) | Helper/local assertions; see file inventory. |
| [TestStmtCacheUsesOverriddenKeyspace](../../cassandra_test.go#L4042) | `Query`, `Scan`, `NewBatch`, `ExecuteBatch`, `SetKeyspace` |
| [TestRoutingKeyCacheUsesOverriddenKeyspace](../../cassandra_test.go#L4119) | `Query`, `NewBatch`, `GetRoutingKey`, `SetKeyspace` |
| [TestPrepareExecuteScyllaEmptyMetadataID](../../cassandra_test.go#L4224) | `Query`, `Exec`, `Iter`, `Scan`, `MapScan`, `WithContext` |

### client_routes_test.go — 1 named tests

Build constraint: `integration`.

| Baseline test | Selected directly visible calls |
|---|---|
| [TestGetHostPortMapping](../../client_routes_test.go#L15) | `Query`, `Exec`, `Consistency`, `CreateSession` |

### cloud_cluster_test.go — 1 named tests

Build constraint: `integration`.

| Baseline test | Selected directly visible calls |
|---|---|
| [TestCloudConnection](../../cloud_cluster_test.go#L25) | `Query`, `Iter`, `Scan`, `Scanner`, `Next`, `SetHostID`, `CreateSession` |

### control_integration_test.go — 1 named tests

Build constraint: `integration`.

| Baseline test | Selected directly visible calls |
|---|---|
| [TestUnixSockets](../../control_integration_test.go#L23) | `CreateSession` |

### dns_test.go — 1 named tests

Build constraint: `integration`.

| Baseline test | Selected directly visible calls |
|---|---|
| [TestDNS](../../dns_test.go#L58) | `Query`, `Exec`, `CreateSession` |

### errors_test.go — 1 named tests

Build constraint: `integration`.

| Baseline test | Selected directly visible calls |
|---|---|
| [TestErrorsParse](../../errors_test.go#L36) | Helper/local assertions; see file inventory. |

### exec_test.go — 1 named tests

Build constraint: `integration`.

| Baseline test | Selected directly visible calls |
|---|---|
| [TestSingleHostQueryExecutor](../../exec_test.go#L10) | `Iter`, `Scan` |

### export_test.go — 0 named tests

Build constraint: `integration`.

No named baseline tests. This file remains in the migration inventory for helpers, exports, or benchmarks.

### integration_serialization_scylla_test.go — 7 named tests

Build constraint: `integration`.

| Baseline test | Selected directly visible calls |
|---|---|
| [TestRetryOnTopologyBusy](../../integration_serialization_scylla_test.go#L85) | Helper/local assertions; see file inventory. |
| [TestSerializationSimpleTypesCassandra](../../integration_serialization_scylla_test.go#L173) | Helper/local assertions; see file inventory. |
| [TestSliceMapMapScanTypes](../../integration_serialization_scylla_test.go#L462) | Helper/local assertions; see file inventory. |
| [TestSliceMapMapScanCounterTypes](../../integration_serialization_scylla_test.go#L646) | `Query`, `Exec`, `Iter`, `MapScan`, `SliceMap` |
| [TestSliceMapMapScanTupleTypes](../../integration_serialization_scylla_test.go#L709) | `Query`, `Exec`, `Iter`, `MapScan`, `SliceMap` |
| [TestSliceMapMapScanVectorTypes](../../integration_serialization_scylla_test.go#L828) | `Query`, `Exec`, `Iter`, `MapScan`, `SliceMap` |
| [TestSliceMapMapScanCollectionTypes](../../integration_serialization_scylla_test.go#L955) | `Query`, `Exec`, `Iter`, `MapScan`, `SliceMap` |

### integration_test.go — 14 named tests

Build constraint: `integration`.

| Baseline test | Selected directly visible calls |
|---|---|
| [TestAuthentication](../../integration_test.go#L50) | `CreateSession` |
| [TestGetHostsFromSystem](../../integration_test.go#L73) | Helper/local assertions; see file inventory. |
| [TestRingDiscovery](../../integration_test.go#L89) | Helper/local assertions; see file inventory. |
| [TestHostFilterDiscovery](../../integration_test.go#L118) | Helper/local assertions; see file inventory. |
| [TestHostFilterInitial](../../integration_test.go#L145) | Helper/local assertions; see file inventory. |
| [TestApplicationInformation](../../integration_test.go#L169) | `Query`, `Iter`, `Scan`, `SliceMap`, `CreateSession` |
| [TestDriverConfigReporting](../../integration_test.go#L278) | `Query`, `Iter`, `Scan`, `SliceMap`, `CreateSession` |
| [TestWriteFailure](../../integration_test.go#L417) | `Query`, `Exec`, `CreateSession` |
| [TestCustomPayloadMessages](../../integration_test.go#L461) | `Query`, `Iter`, `Batch`, `ExecuteBatch`, `Consistency`, `GetCustomPayload` |
| [TestCustomPayloadValues](../../integration_test.go#L507) | `Query`, `Iter`, `Consistency`, `GetCustomPayload` |
| [TestSessionAwaitSchemaAgreement](../../integration_test.go#L543) | `AwaitSchemaAgreement` |
| [TestSessionAwaitSchemaAgreementSessionClosed](../../integration_test.go#L554) | `AwaitSchemaAgreement` |
| [TestSessionAwaitSchemaAgreementContextCanceled](../../integration_test.go#L566) | `AwaitSchemaAgreement` |
| [TestNewConnectWithLowTimeout](../../integration_test.go#L579) | `Query`, `Exec`, `CreateSession` |

### internal/ccm/ccm_test.go — 1 named tests

Build constraint: `all || ccm`.

| Baseline test | Selected directly visible calls |
|---|---|
| [TestCCM](../../internal/ccm/ccm_test.go#L34) | Helper/local assertions; see file inventory. |

### keyspace_table_test.go — 1 named tests

Build constraint: `integration`.

| Baseline test | Selected directly visible calls |
|---|---|
| [TestKeyspaceTable](../../keyspace_table_test.go#L39) | `Query`, `Exec`, `Scan`, `WithContext`, `Consistency`, `CreateSession` |

### policies_integration_test.go — 5 named tests

Build constraint: `integration`.

| Baseline test | Selected directly visible calls |
|---|---|
| [TestDCValidationTokenAware](../../policies_integration_test.go#L13) | `CreateSession` |
| [TestDCValidationDCAware](../../policies_integration_test.go#L27) | `CreateSession` |
| [TestDCValidationRackAware](../../policies_integration_test.go#L39) | `CreateSession` |
| [TestTokenAwareHostPolicy](../../policies_integration_test.go#L51) | Helper/local assertions; see file inventory. |
| [TestNoHangAllHostsDown](../../policies_integration_test.go#L105) | `Query`, `Exec`, `WithContext` |

### recreate_test.go — 1 named tests

Build constraint: `integration`.

| Baseline test | Selected directly visible calls |
|---|---|
| [TestRecreateSchema](../../recreate_test.go#L33) | `Query`, `Exec`, `Release`, `KeyspaceMetadata`, `AwaitSchemaAgreement` |

### schema_queries_test.go — 1 named tests

Build constraint: `integration`.

| Baseline test | Selected directly visible calls |
|---|---|
| [TestSchemaQueries](../../schema_queries_test.go#L12) | Helper/local assertions; see file inventory. |

### scylla_shard_aware_port_common_test.go — 0 named tests

Build constraint: `integration || unit`.

No named baseline tests. This file remains in the migration inventory for helpers, exports, or benchmarks.

### scylla_shard_aware_port_integration_test.go — 4 named tests

Build constraint: `integration`.

| Baseline test | Selected directly visible calls |
|---|---|
| [TestShardAwarePortIntegrationNoReconnections](../../scylla_shard_aware_port_integration_test.go#L8) | Helper/local assertions; see file inventory. |
| [TestShardAwarePortIntegrationMaliciousNAT](../../scylla_shard_aware_port_integration_test.go#L16) | Helper/local assertions; see file inventory. |
| [TestShardAwarePortIntegrationUnreachable](../../scylla_shard_aware_port_integration_test.go#L24) | Helper/local assertions; see file inventory. |
| [TestShardAwarePortIntegrationUnusedIfNotEnabled](../../scylla_shard_aware_port_integration_test.go#L32) | Helper/local assertions; see file inventory. |

### select_without_from_integration_test.go — 16 named tests

Build constraint: `integration`.

| Baseline test | Selected directly visible calls |
|---|---|
| [TestSelectWithoutFromLiteral](../../select_without_from_integration_test.go#L157) | `Query`, `Iter`, `Scan`, `Columns`, `NumRows` |
| [TestSelectWithoutFromFilteredOut](../../select_without_from_integration_test.go#L204) | `Query`, `Iter`, `Scan`, `Columns`, `NumRows` |
| [TestSelectWithoutFromNow](../../select_without_from_integration_test.go#L258) | `Query`, `Iter`, `Scan`, `Columns` |
| [TestSelectWithoutFromFunctionCast](../../select_without_from_integration_test.go#L300) | `Query`, `Iter`, `Scan`, `Columns` |
| [TestSelectWithoutFromMultipleSelectors](../../select_without_from_integration_test.go#L340) | `Query`, `Iter`, `Scan`, `Columns` |
| [TestSelectWithoutFromAlias](../../select_without_from_integration_test.go#L386) | `Query`, `Iter`, `Scan`, `MapScan`, `Columns` |
| [TestSelectWithoutFromExecutionModes](../../select_without_from_integration_test.go#L476) | `Query`, `Iter`, `Scan`, `Columns` |
| [TestSelectWithoutFromBindMarker](../../select_without_from_integration_test.go#L563) | `Query`, `Exec`, `Iter`, `Scan`, `Columns` |
| [TestSelectWithoutFromRoutingKey](../../select_without_from_integration_test.go#L661) | `Query`, `Exec`, `GetRoutingKey` |
| [TestSelectWithoutFromTokenAware](../../select_without_from_integration_test.go#L731) | `Query`, `Scan`, `CreateSession` |
| [TestSelectWithoutFromMapScan](../../select_without_from_integration_test.go#L776) | `Query`, `Iter`, `MapScan`, `SliceMap`, `Columns` |
| [TestSelectWithoutFromAggregate](../../select_without_from_integration_test.go#L818) | `Query`, `Scan` |
| [TestSelectWithoutFromJSON](../../select_without_from_integration_test.go#L836) | `Query`, `Scan` |
| [TestSelectWithoutFromWithKeyspace](../../select_without_from_integration_test.go#L883) | `Query`, `Scan` |
| [TestSelectWithoutFromUnqualifiedUDF](../../select_without_from_integration_test.go#L911) | `Query`, `Scan` |
| [TestSelectWithoutFromRejected](../../select_without_from_integration_test.go#L990) | `Query`, `Exec`, `Scan` |

### session_event_bus_integration_test.go — 2 named tests

Build constraint: `integration`.

| Baseline test | Selected directly visible calls |
|---|---|
| [TestSessionEventBusReceivesSchemaChangeEvent](../../session_event_bus_integration_test.go#L18) | `Query`, `Exec`, `SubscribeToEvents`, `CreateSession` |
| [TestSessionEventBusReceivesControlReconnectEvent](../../session_event_bus_integration_test.go#L56) | `SubscribeToEvents`, `CreateSession` |

### session_test.go — 8 named tests

Build constraint: `integration`.

| Baseline test | Selected directly visible calls |
|---|---|
| [TestSessionAPI](../../session_test.go#L41) | `Query`, `Bind`, `Iter`, `Batch`, `ExecuteBatch` |
| [TestQueryBasicAPI](../../session_test.go#L132) | `Bind`, `PageSize`, `Prefetch`, `Consistency`, `RetryPolicy`, `Observer`, `Trace`, `Attempts`, `Latency` |
| [TestQueryShouldPrepare](../../session_test.go#L207) | Helper/local assertions; see file inventory. |
| [TestBatchBasicAPI](../../session_test.go#L228) | `Query`, `Bind`, `Batch`, `RetryPolicy`, `Trace`, `Attempts`, `Latency` |
| [TestConsistencyNames](../../session_test.go#L324) | Helper/local assertions; see file inventory. |
| [TestIsUseStatement](../../session_test.go#L348) | Helper/local assertions; see file inventory. |
| [TestRetryType_IgnoreRethrow](../../session_test.go#L394) | `Query`, `Exec`, `RetryPolicy`, `Observer` |
| [TestTLSTicketResumption](../../session_test.go#L508) | `MissingConnections`, `CloseAllConnections` |

### stress_test.go — 0 named tests

Build constraint: `integration`.

No named baseline tests. This file remains in the migration inventory for helpers, exports, or benchmarks.

### tablet_integration_test.go — 1 named tests

Build constraint: `integration`.

| Baseline test | Selected directly visible calls |
|---|---|
| [TestTablets](../../tablet_integration_test.go#L14) | `Query`, `Exec`, `Iter`, `WithContext`, `Consistency`, `GetCustomPayload` |

### tracer_test.go — 1 named tests

Build constraint: `integration`.

| Baseline test | Selected directly visible calls |
|---|---|
| [TestTracingNewAPI](../../tracer_test.go#L11) | `Query`, `Exec`, `Scan`, `Trace` |

### tuple_test.go — 9 named tests

Build constraint: `integration`.

| Baseline test | Selected directly visible calls |
|---|---|
| [TestTupleSimple](../../tuple_test.go#L36) | `Query`, `Exec`, `Scan` |
| [TestTuple_NullTuple](../../tuple_test.go#L81) | `Query`, `Exec`, `Scan` |
| [TestTuple_TupleNotSet](../../tuple_test.go#L120) | `Query`, `Exec`, `Scan` |
| [TestTupleMapScan](../../tuple_test.go#L174) | `Query`, `Exec`, `MapScan` |
| [TestTupleMapScanNil](../../tuple_test.go#L208) | `Query`, `Exec`, `MapScan` |
| [TestTupleMapScanNotSet](../../tuple_test.go#L241) | `Query`, `Exec`, `MapScan` |
| [TestTupleLastFieldEmpty](../../tuple_test.go#L274) | `Query`, `Exec`, `Scan` |
| [TestTuple_NestedCollection](../../tuple_test.go#L315) | `Query`, `Exec`, `Scan` |
| [TestTuple_NullableNestedCollection](../../tuple_test.go#L368) | `Query`, `Exec`, `Scan` |

### udt_test.go — 8 named tests

Build constraint: `integration`.

| Baseline test | Selected directly visible calls |
|---|---|
| [TestUDT_Marshaler](../../udt_test.go#L71) | `Query`, `Exec`, `Scan` |
| [TestUDT_Reflect](../../udt_test.go#L128) | `Query`, `Exec`, `Scan` |
| [TestUDT_NullObject](../../udt_test.go#L181) | `Query`, `Exec`, `Scan` |
| [TestMapScanUDT](../../udt_test.go#L236) | `Query`, `Exec`, `MapScan` |
| [TestUDT_MissingField](../../udt_test.go#L324) | `Query`, `Exec`, `Scan` |
| [TestUDT_EmptyCollections](../../udt_test.go#L375) | `Query`, `Exec`, `Scan` |
| [TestUDT_UpdateField](../../udt_test.go#L432) | `Query`, `Exec`, `Scan` |
| [TestUDT_ScanNullUDT](../../udt_test.go#L490) | `Query`, `Exec`, `Scan` |

### vector_test.go — 6 named tests

Build constraint: `integration`.

| Baseline test | Selected directly visible calls |
|---|---|
| [TestVector_Marshaler](../../vector_test.go#L54) | `Query`, `Exec`, `Scan` |
| [TestVector_Types](../../vector_test.go#L107) | `Query`, `Exec`, `Scan` |
| [TestVector_MarshalerUDT](../../vector_test.go#L270) | `Query`, `Exec`, `Scan` |
| [TestVector_Empty](../../vector_test.go#L323) | `Query`, `Exec`, `Scan` |
| [TestVector_MissingDimension](../../vector_test.go#L373) | `Query`, `Exec` |
| [TestVector_SubTypeParsing](../../vector_test.go#L401) | Helper/local assertions; see file inventory. |

### wiki_test.go — 3 named tests

Build constraint: `integration`.

| Baseline test | Selected directly visible calls |
|---|---|
| [TestWikiCreateSchema](../../wiki_test.go#L151) | Helper/local assertions; see file inventory. |
| [TestWikiCreatePages](../../wiki_test.go#L174) | Helper/local assertions; see file inventory. |
| [TestWikiTypicalCRUD](../../wiki_test.go#L282) | Helper/local assertions; see file inventory. |
