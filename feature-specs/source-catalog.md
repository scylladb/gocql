# Rust source coverage catalog

Pinned source: `f8b0ee928dc0dcb475247dc82cce4e42804b0621`. This catalog assigns every source module in the four runtime/macro crates to a reference section. Assignment establishes an audit checklist, not proof that prose captures every branch. Tests embedded in production files remain part of their evidence.

| Module | Role | Owning section |
| --- | --- | --- |
| `scylla/src/authentication/mod.rs` | production | [configuration-security-observability](configuration-security-observability.md) |
| `scylla/src/book_tests.rs` | test-evidence | [configuration-security-observability](configuration-security-observability.md), [connections](connections.md), [execution](execution.md), [metadata-control-events](metadata-control-events.md), [routing](routing.md), [serialization-protocol](serialization-protocol.md) |
| `scylla/src/client/caching_session.rs` | production | [execution](execution.md) |
| `scylla/src/client/client_routes.rs` | production | [configuration-security-observability](configuration-security-observability.md) |
| `scylla/src/client/driver_config.rs` | production | [configuration-security-observability](configuration-security-observability.md) |
| `scylla/src/client/driver_config_tests.rs` | test-evidence | [configuration-security-observability](configuration-security-observability.md) |
| `scylla/src/client/execution.rs` | production | [execution](execution.md), [configuration-security-observability](configuration-security-observability.md) |
| `scylla/src/client/execution_profile.rs` | production | [configuration-security-observability](configuration-security-observability.md) |
| `scylla/src/client/mod.rs` | production | [configuration-security-observability](configuration-security-observability.md) |
| `scylla/src/client/pager.rs` | production | [execution](execution.md) |
| `scylla/src/client/self_identity.rs` | production | [configuration-security-observability](configuration-security-observability.md) |
| `scylla/src/client/session.rs` | production | [execution](execution.md), [configuration-security-observability](configuration-security-observability.md) |
| `scylla/src/client/session_builder.rs` | production | [configuration-security-observability](configuration-security-observability.md) |
| `scylla/src/cluster/control_connection.rs` | production | [metadata-control-events](metadata-control-events.md) |
| `scylla/src/cluster/metadata/cc_establisher.rs` | production | [metadata-control-events](metadata-control-events.md) |
| `scylla/src/cluster/metadata/fetching.rs` | production | [metadata-control-events](metadata-control-events.md) |
| `scylla/src/cluster/metadata/merge_channel.rs` | production | [metadata-control-events](metadata-control-events.md) |
| `scylla/src/cluster/metadata/mod.rs` | production | [metadata-control-events](metadata-control-events.md) |
| `scylla/src/cluster/metadata/update.rs` | production | [metadata-control-events](metadata-control-events.md) |
| `scylla/src/cluster/metadata/worker.rs` | production | [metadata-control-events](metadata-control-events.md) |
| `scylla/src/cluster/mod.rs` | production | [metadata-control-events](metadata-control-events.md) |
| `scylla/src/cluster/node.rs` | production | [metadata-control-events](metadata-control-events.md) |
| `scylla/src/cluster/state.rs` | production | [metadata-control-events](metadata-control-events.md) |
| `scylla/src/cluster/worker.rs` | production | [metadata-control-events](metadata-control-events.md) |
| `scylla/src/errors.rs` | production | [execution](execution.md) |
| `scylla/src/lib.rs` | production | [configuration-security-observability](configuration-security-observability.md), [serialization-protocol](serialization-protocol.md) |
| `scylla/src/network/connection.rs` | production | [connections](connections.md), [configuration-security-observability](configuration-security-observability.md) |
| `scylla/src/network/connection_pool.rs` | production | [connections](connections.md) |
| `scylla/src/network/mod.rs` | production | [connections](connections.md) |
| `scylla/src/network/tls/openssl.rs` | production | [configuration-security-observability](configuration-security-observability.md) |
| `scylla/src/network/tls.rs` | production | [configuration-security-observability](configuration-security-observability.md) |
| `scylla/src/observability/driver_tracing.rs` | production | [configuration-security-observability](configuration-security-observability.md) |
| `scylla/src/observability/history.rs` | production | [configuration-security-observability](configuration-security-observability.md) |
| `scylla/src/observability/metrics.rs` | production | [configuration-security-observability](configuration-security-observability.md) |
| `scylla/src/observability/metrics_noop.rs` | production | [configuration-security-observability](configuration-security-observability.md) |
| `scylla/src/observability/mod.rs` | production | [configuration-security-observability](configuration-security-observability.md) |
| `scylla/src/observability/tracing.rs` | production | [configuration-security-observability](configuration-security-observability.md) |
| `scylla/src/policies/address_translator.rs` | production | [configuration-security-observability](configuration-security-observability.md) |
| `scylla/src/policies/host_filter.rs` | production | [configuration-security-observability](configuration-security-observability.md) |
| `scylla/src/policies/host_listener.rs` | production | [metadata-control-events](metadata-control-events.md) |
| `scylla/src/policies/load_balancing/default.rs` | production | [routing](routing.md) |
| `scylla/src/policies/load_balancing/mod.rs` | production | [routing](routing.md) |
| `scylla/src/policies/load_balancing/plan.rs` | production | [routing](routing.md) |
| `scylla/src/policies/load_balancing/single_target.rs` | production | [routing](routing.md) |
| `scylla/src/policies/mod.rs` | production | [configuration-security-observability](configuration-security-observability.md) |
| `scylla/src/policies/reconnect.rs` | production | [connections](connections.md) |
| `scylla/src/policies/retry/default.rs` | production | [execution](execution.md) |
| `scylla/src/policies/retry/downgrading_consistency.rs` | production | [execution](execution.md) |
| `scylla/src/policies/retry/fallthrough.rs` | production | [execution](execution.md) |
| `scylla/src/policies/retry/mod.rs` | production | [execution](execution.md) |
| `scylla/src/policies/retry/retry_policy.rs` | production | [execution](execution.md) |
| `scylla/src/policies/speculative_execution.rs` | production | [execution](execution.md) |
| `scylla/src/policies/timestamp_generator.rs` | production | [execution](execution.md) |
| `scylla/src/response/coordinator.rs` | production | [execution](execution.md) |
| `scylla/src/response/mod.rs` | production | [execution](execution.md) |
| `scylla/src/response/query_result.rs` | production | [execution](execution.md) |
| `scylla/src/response/request_response.rs` | production | [execution](execution.md) |
| `scylla/src/routing/locator/mod.rs` | production | [routing](routing.md) |
| `scylla/src/routing/locator/precomputed_replicas.rs` | production | [routing](routing.md) |
| `scylla/src/routing/locator/replicas.rs` | production | [routing](routing.md) |
| `scylla/src/routing/locator/replication_info.rs` | production | [routing](routing.md) |
| `scylla/src/routing/locator/tablets.rs` | production | [routing](routing.md) |
| `scylla/src/routing/locator/test.rs` | test-evidence | [routing](routing.md) |
| `scylla/src/routing/locator/token_ring.rs` | production | [routing](routing.md) |
| `scylla/src/routing/mod.rs` | production | [routing](routing.md) |
| `scylla/src/routing/partitioner.rs` | production | [routing](routing.md) |
| `scylla/src/routing/sharding.rs` | production | [routing](routing.md) |
| `scylla/src/serde_tests.rs` | test-evidence | [serialization-protocol](serialization-protocol.md) |
| `scylla/src/statement/batch.rs` | production | [execution](execution.md) |
| `scylla/src/statement/bound.rs` | production | [execution](execution.md) |
| `scylla/src/statement/mod.rs` | production | [execution](execution.md) |
| `scylla/src/statement/prepared.rs` | production | [execution](execution.md) |
| `scylla/src/statement/unprepared.rs` | production | [execution](execution.md) |
| `scylla/src/utils/mod.rs` | production | [configuration-security-observability](configuration-security-observability.md), [serialization-protocol](serialization-protocol.md) |
| `scylla/src/utils/safe_format.rs` | production | [configuration-security-observability](configuration-security-observability.md), [serialization-protocol](serialization-protocol.md) |
| `scylla/src/utils/test_utils.rs` | test-evidence | [serialization-protocol](serialization-protocol.md) |
| `scylla-cql/src/deserialize/mod.rs` | production | [serialization-protocol](serialization-protocol.md) |
| `scylla-cql/src/deserialize/result.rs` | production | [serialization-protocol](serialization-protocol.md) |
| `scylla-cql/src/frame/mod.rs` | production | [serialization-protocol](serialization-protocol.md) |
| `scylla-cql/src/frame/request/auth_response.rs` | production | [serialization-protocol](serialization-protocol.md) |
| `scylla-cql/src/frame/request/batch.rs` | production | [serialization-protocol](serialization-protocol.md) |
| `scylla-cql/src/frame/request/execute.rs` | production | [serialization-protocol](serialization-protocol.md) |
| `scylla-cql/src/frame/request/mod.rs` | production | [serialization-protocol](serialization-protocol.md) |
| `scylla-cql/src/frame/request/options.rs` | production | [serialization-protocol](serialization-protocol.md) |
| `scylla-cql/src/frame/request/prepare.rs` | production | [serialization-protocol](serialization-protocol.md) |
| `scylla-cql/src/frame/request/query.rs` | production | [serialization-protocol](serialization-protocol.md) |
| `scylla-cql/src/frame/request/register.rs` | production | [serialization-protocol](serialization-protocol.md) |
| `scylla-cql/src/frame/request/startup.rs` | production | [serialization-protocol](serialization-protocol.md) |
| `scylla-cql/src/frame/response/authenticate.rs` | production | [serialization-protocol](serialization-protocol.md) |
| `scylla-cql/src/frame/response/custom_type_parser.rs` | production | [serialization-protocol](serialization-protocol.md) |
| `scylla-cql/src/frame/response/event.rs` | production | [serialization-protocol](serialization-protocol.md) |
| `scylla-cql/src/frame/response/mod.rs` | production | [serialization-protocol](serialization-protocol.md) |
| `scylla-cql/src/frame/response/result.rs` | production | [serialization-protocol](serialization-protocol.md) |
| `scylla-cql/src/frame/response/supported.rs` | production | [serialization-protocol](serialization-protocol.md) |
| `scylla-cql/src/frame/server_event_type.rs` | production | [serialization-protocol](serialization-protocol.md) |
| `scylla-cql/src/frame/types.rs` | production | [serialization-protocol](serialization-protocol.md) |
| `scylla-cql/src/lib.rs` | production | [serialization-protocol](serialization-protocol.md) |
| `scylla-cql/src/serialize/mod.rs` | production | [serialization-protocol](serialization-protocol.md) |
| `scylla-cql/src/serialize/raw_batch.rs` | production | [serialization-protocol](serialization-protocol.md) |
| `scylla-cql/src/utils/mod.rs` | production | [serialization-protocol](serialization-protocol.md) |
| `scylla-cql/src/utils/parse.rs` | production | [serialization-protocol](serialization-protocol.md) |
| `scylla-cql/src/value.rs` | production | [serialization-protocol](serialization-protocol.md) |
| `scylla-cql-core/src/_macro_internal.rs` | production | [serialization-protocol](serialization-protocol.md) |
| `scylla-cql-core/src/deserialize/frame_slice.rs` | production | [serialization-protocol](serialization-protocol.md) |
| `scylla-cql-core/src/deserialize/mod.rs` | production | [serialization-protocol](serialization-protocol.md) |
| `scylla-cql-core/src/deserialize/result.rs` | production | [serialization-protocol](serialization-protocol.md) |
| `scylla-cql-core/src/deserialize/row.rs` | production | [serialization-protocol](serialization-protocol.md) |
| `scylla-cql-core/src/deserialize/row_tests.rs` | test-evidence | [serialization-protocol](serialization-protocol.md) |
| `scylla-cql-core/src/deserialize/value.rs` | production | [serialization-protocol](serialization-protocol.md) |
| `scylla-cql-core/src/deserialize/value_tests.rs` | test-evidence | [serialization-protocol](serialization-protocol.md) |
| `scylla-cql-core/src/frame/frame_errors.rs` | production | [serialization-protocol](serialization-protocol.md) |
| `scylla-cql-core/src/frame/mod.rs` | production | [serialization-protocol](serialization-protocol.md) |
| `scylla-cql-core/src/frame/protocol_features.rs` | production | [serialization-protocol](serialization-protocol.md) |
| `scylla-cql-core/src/frame/request/batch.rs` | production | [serialization-protocol](serialization-protocol.md) |
| `scylla-cql-core/src/frame/request/mod.rs` | production | [serialization-protocol](serialization-protocol.md) |
| `scylla-cql-core/src/frame/request/query.rs` | production | [serialization-protocol](serialization-protocol.md) |
| `scylla-cql-core/src/frame/response/error.rs` | production | [serialization-protocol](serialization-protocol.md) |
| `scylla-cql-core/src/frame/response/mod.rs` | production | [serialization-protocol](serialization-protocol.md) |
| `scylla-cql-core/src/frame/response/result.rs` | production | [serialization-protocol](serialization-protocol.md) |
| `scylla-cql-core/src/frame/types.rs` | production | [serialization-protocol](serialization-protocol.md) |
| `scylla-cql-core/src/lib.rs` | production | [serialization-protocol](serialization-protocol.md) |
| `scylla-cql-core/src/macros_doctests.rs` | test-evidence | [serialization-protocol](serialization-protocol.md) |
| `scylla-cql-core/src/macros_tests.rs` | test-evidence | [serialization-protocol](serialization-protocol.md) |
| `scylla-cql-core/src/pretty.rs` | production | [serialization-protocol](serialization-protocol.md) |
| `scylla-cql-core/src/serialize/batch.rs` | production | [serialization-protocol](serialization-protocol.md) |
| `scylla-cql-core/src/serialize/batch_tests.rs` | test-evidence | [serialization-protocol](serialization-protocol.md) |
| `scylla-cql-core/src/serialize/mod.rs` | production | [serialization-protocol](serialization-protocol.md) |
| `scylla-cql-core/src/serialize/row.rs` | production | [serialization-protocol](serialization-protocol.md) |
| `scylla-cql-core/src/serialize/row_tests.rs` | test-evidence | [serialization-protocol](serialization-protocol.md) |
| `scylla-cql-core/src/serialize/value.rs` | production | [serialization-protocol](serialization-protocol.md) |
| `scylla-cql-core/src/serialize/value_tests.rs` | test-evidence | [serialization-protocol](serialization-protocol.md) |
| `scylla-cql-core/src/serialize/writers.rs` | production | [serialization-protocol](serialization-protocol.md) |
| `scylla-cql-core/src/utils/mod.rs` | production | [serialization-protocol](serialization-protocol.md) |
| `scylla-cql-core/src/utils/parse.rs` | production | [serialization-protocol](serialization-protocol.md) |
| `scylla-cql-core/src/utils/safe_format.rs` | production | [serialization-protocol](serialization-protocol.md) |
| `scylla-cql-core/src/value.rs` | production | [serialization-protocol](serialization-protocol.md) |
| `scylla-macros/src/deserialize/mod.rs` | production | [serialization-protocol](serialization-protocol.md) |
| `scylla-macros/src/deserialize/row.rs` | production | [serialization-protocol](serialization-protocol.md) |
| `scylla-macros/src/deserialize/value.rs` | production | [serialization-protocol](serialization-protocol.md) |
| `scylla-macros/src/lib.rs` | production | [serialization-protocol](serialization-protocol.md) |
| `scylla-macros/src/parser.rs` | production | [serialization-protocol](serialization-protocol.md) |
| `scylla-macros/src/serialize/mod.rs` | production | [serialization-protocol](serialization-protocol.md) |
| `scylla-macros/src/serialize/row.rs` | production | [serialization-protocol](serialization-protocol.md) |
| `scylla-macros/src/serialize/value.rs` | production | [serialization-protocol](serialization-protocol.md) |

## Workspace tools

- `benchmarks`: performance test harness.
- `examples`: sample applications; behaviors belong to the six driver sections.
- `scylla-proxy`: integration fault-injection harness, not runtime driver.
- `scylla-ccm-bridge`: database cluster test harness.
- `utils`: development tools.
