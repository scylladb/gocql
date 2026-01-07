# ScyllaDB GoCQL — AI Agent Instructions

These instructions help AI coding agents work productively in this repo. Focus on existing patterns and workflows; avoid inventing new ones.

## Big Picture

- A production-grade Go driver for Cassandra/Scylla with Scylla-specific shard-awareness and protocol extensions.
- Core flow: `ClusterConfig` → `Session` → ring discovery and metadata → connection pools → `queryExecutor` → user `Query`/`Batch` APIs.
- Shard-aware behavior selects per-shard connections and may use Scylla’s shard-aware port.

## Key Components

- Cluster: configuration and policies in [cluster.go](cluster.go). Important knobs: `PoolConfig.HostSelectionPolicy`, `DisableShardAwarePort`, TLS via `SslOpts`, observers (`QueryObserver`, `BatchObserver`, `FrameHeaderObserver`, `StreamObserver`).
- Session: orchestration in [session.go](session.go) — `ringDescriber` (topology) [ring_describer.go](ring_describer.go), `policyConnPool`/`hostConnPool` [connectionpool.go](connectionpool.go), `ConnPicker` [connpicker.go](connpicker.go), `queryExecutor` [query_executor.go](query_executor.go), prepared LRU [prepared_cache.go](prepared_cache.go), event bus.
- Scylla Extensions: shard-aware port, LWT metadata mark, tablets routing, rate limit errors in [scylla.go](scylla.go). Custom dialers: `ScyllaShardAwareDialer`, `ScyllaGetSourcePort`.
- Policies: host selection and token awareness in [policies.go](policies.go); use `TokenAwareHostPolicy` with `RoundRobinHostPolicy()` or `DCAwareRoundRobinPolicy(localDC)`.

## Developer Workflows

- Unit tests: `go test -tags unit -race ./...` or `make test-unit`. Generates PKI as needed.
- Integration (Scylla): Use VS Code task “Setup: Prepare Scylla Cluster” or `make scylla-start`, then:
  - `go test -v -tags "integration gocql_debug" -distribution scylla -timeout=5m -gocql.timeout=60s -proto=4 -rf=3 -clusterSize=3 -autowait=2000ms -compressor=snappy -gocql.cversion=$(ccm liveset) -cluster=$(ccm liveset) ./...` (see [Makefile](Makefile)).
  - Local compose flow and flags example in [integration.sh](integration.sh).
- Lint/build: `make check` (build + golangci-lint), auto-fix with `make fix`.

## Project Conventions

- Token-aware must be enabled for shard-awareness. In multi-DC, prefer local consistency:
  - Example: `c.PoolConfig.HostSelectionPolicy = gocql.TokenAwareHostPolicy(gocql.DCAwareRoundRobinPolicy(localDC))`.
- For Scylla, `NumConns` is ignored; driver opens one connection per shard; excess shard connections limited by `MaxExcessShardConnectionsRate`.
- Default `DisableSkipMetadata: true`; rows are parsed with full metadata rather than reusing prepared metadata.
- Observability via observers; prefer them over ad-hoc logging: set on `ClusterConfig` to collect metrics.
- Internal events use `EventBus` (non-blocking fanout) — see [internal/eventbus/README.md](internal/eventbus/README.md).

## Integration Points

- Shard-aware port: requires node config for `native_shard_aware_transport_port` and no NAT source-port rewrites. Custom dialers should bind the correct source port; see examples in [README.md](README.md) and APIs in [scylla.go](scylla.go).
- Addressing: prefer IPs matching node broadcast/listen addresses; `AddressTranslator` can rewrite addresses; DNS v4 preference is configurable.
- Compression: enable via `ClusterConfig.Compressor` (`SnappyCompressor` or `lz4.LZ4Compressor`) — examples in [README.md](README.md).

## Helpful References

- Architecture: [session.go](session.go), [cluster.go](cluster.go), [connectionpool.go](connectionpool.go), [connpicker.go](connpicker.go), [query_executor.go](query_executor.go), [prepared_cache.go](prepared_cache.go).
- Scylla specifics: [scylla.go](scylla.go), shard-aware port docs and dialer examples in [README.md](README.md).
- Topology/ring: [ring_describer.go](ring_describer.go), tokens in [token.go](token.go), Scylla tokens in [scylla_tokens_test.go](scylla_tokens_test.go).
- Testing: workflows and flags in [Makefile](Makefile), integration helpers in [integration_only.go](integration_only.go), compose script in [integration.sh](integration.sh).

## Tips for Changes

- Preserve public APIs; changes should match existing patterns (policies, pools, observers).
- Prefer adding observers or using `EventBus` for instrumentation.
- Validate config via `ClusterConfig.Validate()` and TLS via `ValidateAndInitSSL()` paths when touching init code.

If any section is unclear or missing important patterns, tell me what to refine (e.g., specific test flags, dialer usage, or policies you plan to touch).