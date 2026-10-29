# Configuration, security, and observability — round 10

- Section: `feature-specs/configuration-security-observability.md`
- Round: **10 — final comprehensive fidelity and completeness audit**
- Agent identity: `/root/review_configuration_10`
- Source checkout: `/extra/scylladb/gocql-rust-reference-f8b0ee9`
- Verified source HEAD: `f8b0ee928dc0dcb475247dc82cce4e42804b0621`
- Before SHA-256: `040fa2bebfba401c2e091c9e49fe24e4fa7398c3465553e56bac0d4804b8b93b`
- After SHA-256: `040fa2bebfba401c2e091c9e49fe24e4fa7398c3465553e56bac0d4804b8b93b`
- Result: **0 new verified findings; no specification edits.**

Read `REVIEW-INSTRUCTIONS.md`, `feature-specs/README.md`, the complete assigned
specification, and its source-catalog assignments. This was an independent source
audit; prior round reports were not used as proof. No child agents were spawned.
No Go or Rust implementation was edited, and no tests were executed.

## Source inspected and behaviors checked

Paths below are relative to the pinned Rust checkout. Line numbers identify the
relevant definitions or inspected regions, not executed coverage.

| Area | Source inspected | Checks and conclusion |
| --- | --- | --- |
| Configuration and startup | `scylla/src/client/session.rs:207–680,1407–1585`; `session_builder.rs:36–155,852`; `scylla/src/routing/sharding.rs:13–40`; `scylla/src/policies/reconnect.rs:30–170` | Checked the complete configuration field/default inventory, public builder configuration, clone-on-build ownership, validation-before-UUID ordering, initial keyspace failure propagation, source-port validation, and default exponential backoff/jitter. The spec correctly distinguishes warnings from rejection and does not imply comprehensive duration validation. |
| Bootstrap DNS and partial success | `scylla/src/cluster/node.rs:326–462`; `scylla/src/cluster/metadata/cc_establisher.rs:158–405` | Checked per-lookup timeout, first-error preservation on fallback, all-address expansion, deduplication, concurrent group completion, shuffled initial candidates, candidate handshake failure, and initial metadata fallback without a retained control connection. |
| Metadata deadlines | `scylla/src/cluster/control_connection.rs:45–64,160–234`; `scylla/src/client/pager.rs:553–605` | Checked explicit/derived client deadline, saturating margin, target-gated server clause, cache lookup before clause rendering, per-use timeout assignment, and the PREPARE/whole-refresh exclusions. |
| Execution profiles | `scylla/src/client/execution_profile.rs:174–212,355–539`; `session.rs:1664–1720`; `pager.rs:149–188`; `driver_config.rs:881–912` | Confirmed LOCAL_QUORUM/LOCAL_SERIAL/30-second defaults, optional-field builder semantics, separate remapping cells, cloned-handle sharing, immutable profile snapshots, labels, and separate acquisition boundaries for ordinary calls, automatic pagers, and reports. |
| Authentication | `scylla/src/authentication/mod.rs:13–102`; `scylla/src/network/connection.rs:823–910,2270–2295` | Checked provider sharing versus per-connection exchanges, null/empty tokens, the unrestricted challenge loop, distinct provider/challenge/success failures, READY bypass, missing-provider failure, plaintext-provider bytes, and absence of a success callback on cancellation/failure. |
| TLS | `scylla/src/network/tls.rs`; `tls/openssl.rs:17–344`; `connection.rs:1559–1600,2136–2148` | Checked backend dispatch and connected-IP verification, managed versus legacy constructors, replaced callbacks/cache modes, clone-local enablement with shared cache, serialized sessions, TLS 1.3 FIFO/single-use behavior, reusable non-TLS-1.3 slot, both bounds, store-age eviction, one-entry selection, and full-handshake fallback. TLS banking is independent of later CQL authentication, which still runs on every new connection. |
| Socket options and compression | `scylla/src/network/connection.rs:2136–2430` | Checked translation before connection creation, options before bind/connect, buffer narrowing, linger handoff, platform-gated TCP keepalive settings, per-connection OPTIONS/SUPPORTED negotiation, and nonfatal unsupported-compression fallback. No plaintext retry follows TLS failure. |
| Translation and filtering | `scylla/src/policies/address_translator.rs`; `host_filter.rs`; `scylla/src/cluster/metadata/cc_establisher.rs:214–405`; `metadata/worker.rs:784–815` | Checked map errors, advertised versus connect addresses, original-contact bypass, synchronous one-time allow-list DNS, untranslatable allow-list acceptance, DC matching, filtered query pools/candidates, and retained/bootstrap control-connection exceptions. |
| Client routes | `scylla/src/client/client_routes.rs:24–440`; `session.rs:653–669`; `session_builder.rs:115–155` | Checked nonempty configuration, accepted empty/duplicate IDs, specialized builder behavior, TLS/translator incompatibility, sticky selection, lack of connection-error rotation/direct fallback, full versus partial update semantics and refill sets, per-ID hostname override, required port, random per-attempt DNS choice, and read-lock release before lookup. |
| Identity and reporting | `scylla/src/client/self_identity.rs`; `scylla/src/network/connection.rs:240–279,2230–2277`; `scylla/src/client/driver_config.rs:43–57,490–1041`; `driver-config-schema.json:39`; `connection.rs:1829–1843` | Checked UUID lifetime and all-connection STARTUP inclusion, named optional identities, control-only reporter assignment, opt-out, report snapshotting and size boundary, target-specific timeout reporting, policy recognition/names, routing normalization, omitted runtime settings, fixed query defaults, additional speculation count, orphan age/count interpretation, zero/submillisecond normalization, and known schema-invalid buffer/name cases. |
| Metrics | `scylla/src/observability/metrics.rs:1–655`; `metrics_noop.rs`; `scylla/src/client/execution.rs:382–680`; `scylla/src/network/connection_pool.rs:1030–1068,1125–1165`; metric-update call sites | Checked independent counters and histogram loads, shared storage, all three paging kinds, attempt/retry/timeout boundaries, ignored write errors, PREPARE/control exclusions, successful-attempt latency, bucket statistics, percentile scale, idle EWMA behavior, control-socket exclusion, and the discarded shard-targeted connection gauge overcount. |
| History and cancellation | `scylla/src/observability/history.rs:16–454`; `scylla/src/client/execution.rs:429–680`; `scylla/src/policies/speculative_execution.rs:187–242`; `pager.rs:202–350,919`; `session.rs:2496–2540` | Checked synchronous listener callbacks, deadline and external cancellation outcomes, local speculative-future teardown, detached paging work, post-core side effects, collector locking/draining, retained ID counters, parentless structured-event loss, poisoning fallback, and unbounded retention. |
| Local and server tracing | `scylla/src/observability/driver_tracing.rs`; `tracing.rs`; `scylla/src/utils/safe_format.rs`; `scylla/src/network/connection.rs:1487–1518`; `scylla/src/response/request_response.rs`; `scylla/src/client/session.rs:1570–1595,2380–2483` | Checked query/partition-key exposure, repeatable iterator formatting, declared-versus-recorded result fields, control-span field limits, warning logging order, result/error/custom-payload retention boundaries, tracing IDs, concurrent trace-table reads, readiness ordering, incomplete-session deferral of event decoding, final sleep, immediate query errors, and source-node deduplication. |
| Optional features and exports | `scylla/Cargo.toml:34–175`; `scylla-ccm-bridge/Cargo.toml`; `scylla/src/lib.rs:279–320`; `client/mod.rs`; `observability/mod.rs`; `policies/mod.rs`; `utils/mod.rs`; `book_tests.rs` | Checked empty default features, both TLS features, metrics/no-op visibility, percentile speculation, external serialization feature grouping, separate serde semantics, dual unstable gates, ungated client-route value types, and doctest-module assignments. Reporting/history/tracing do not depend on the optional metrics or serde flags. |

## Findings and fixes

No additional mismatch or omission requiring a reference-spec change was verified.
The more restrictive boundaries already in the spec are supported by executable
source, including the control-connection host-filter exception, cached metadata
query-text/report disagreement, TLS ticket consumption before handshake, metric
gauge overcount, and unavailable local result-size span fields. They were retained.

The catalogue assigns 26 modules to this section: 24 production modules and two
test-evidence modules. Checked those assignments against the feature inventory.
Connection lifecycle, metadata scheduling/events, execution policy algorithms,
routing, and CQL conversion remain explicitly delegated to their respective
sections. The supported-compression round-trip integration test is already
catalogued in `serialization-protocol.md`; its existence does not establish the
unsupported-codec fallback test that this section marks missing. No cross-section
contradiction requiring another owner's edit was found.

All 80 pinned source links use the required commit and resolve to existing lines
within the appropriate definitions or surrounding implementation regions. All 65
named test targets resolve to functions in the pinned checkout. These are static
reference checks, not test runs.

## Test claims checked

- `session_builder.rs:1604–1875`: inspected assertions for empty/default contact
  and compression settings, compression assignment, zero keepalive rejection, and
  configured/default profile values. These do not prove all configuration values
  are globally validated.
- `driver_config_tests.rs:215–234,485–595,656–1048,1420–1475,1484–1780`:
  inspected default/remapped reports, omitted optional defaults, load-balancing
  and location matrices, replica distribution, empty custom load-balancing names,
  speculation cases, timing normalization, schema validation, the metrics-gated
  schema-key coverage comparison, and exact/over-limit sizes. The sweep does not
  cover every runtime setting or invalidate the documented schema counterexamples.
- `tls/openssl.rs:531–710`: inspected reusable TLS 1.2, bounded FIFO TLS 1.3,
  node eviction, node-specific resumption, disabled banking, incompatible-version,
  and corrupt-entry assertions. They do not establish concurrent consumption,
  cancellation, mixed-bank fallback, or callback-replacement coverage.
- `client_routes.rs:517–532,719–742,959–1090,1691–1720,1919–2041`: inspected
  missing-route, override, replacement, partial preservation, sticky-ID change,
  and returned-refill-set assertions. They do not cover the identical/nonsticky
  partial-update or update-during-DNS cases marked missing.
- `observability/metrics.rs:684–806`, `observability/history.rs:610–880`, and
  `response/query_result.rs:530–590,800–835`: checked the stated histogram
  distribution/percentile/order assertions, manually inserted incomplete and
  speculative histories, and retained warnings/tracing IDs. These are narrower
  than concurrent instrumentation or full response-conversion coverage.
- Integration `ccm/authenticate.rs`, `ccm/tls.rs:197–380`, and
  `ccm/tls_tickets.rs:80–165,200–409,438–557`: checked password-authenticated DDL,
  the challenge-rejecting custom provider, trusted and mismatched IP SAN cases,
  mutual TLS, Rustls-only missing-client-certificate rejection, version pinning,
  backend ticket matrix, per-node resumption minima, and disabled/legacy no-offer
  assertions. The spec correctly leaves broader TLS rejection causes missing.
- Integration `session/self_identity.rs:15–113`,
  `session/startup_options.rs:25–235,259–335`,
  `statements/execution_profiles.rs:124–335`,
  `statements/request_timeout.rs:19–155`, and `session/new_session.rs:12–44`:
  checked one-frame identity inspection, expected initial STARTUP batch counts,
  report opt-out, `system.clients` skip conditions, profile selection, sequential
  default-handle timeout remapping, statement overrides, and partial DNS failure.
  Initial-frame counting does not independently identify the control socket or
  test control reconnection.
- Integration `ccm/client_routes.rs:100–213,340–381,520–663`:
  followed the named wrappers to topology-change, proxy-port-remap, and injected
  reroute/recovery helpers and their query-feedback assertions.
- Integration `session/metrics.rs`, `session/history.rs:122–325,337–646`, and
  `session/tracing.rs`: checked one-page/no-retry counter separation, history
  success/failure/pagination and ignored-error expectations, initial pager trace
  IDs, and ordinary trace-record inspection. Also checked the 200-attempt/50-ms
  helper overrides in `integration/utils.rs:368–369`, TLS/metrics/client-route
  module gates, and the CCM bridge dev-dependency's feature unification. These
  tests do not establish the default trace-readiness budget or later-page tracing
  accumulation.

## Remaining gaps and uncertainty

The explicit `missing` cases remain gaps rather than passing tests. In particular,
concurrent remapping/cancellation, arbitrary authentication challenges, cross-target
metadata cache/report differences, TLS cache concurrency, idle meters, collector
drains, and instrumentation after pager drop are supported here by source tracing,
not by a new executable regression test. No database, proxy, local TLS-server,
unit, race, or allocation suite was run in this review. Backend/OS behavior beyond
the inspected driver calls was not experimentally validated. Go implementation
and migration completion remain separate gates.
