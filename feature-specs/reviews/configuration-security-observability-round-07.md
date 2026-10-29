# Configuration, security, and observability — round 07

- Section: `feature-specs/configuration-security-observability.md`
- Round: 07 — source anchors and implementation versus contract wording.
- Agent: `/root/review_configuration_07`
- Reference checkout: `/extra/scylladb/gocql-rust-reference-f8b0ee9`
- Verified reference HEAD: `f8b0ee928dc0dcb475247dc82cce4e42804b0621`
- Before SHA-256: `1b5f5b12cbdeeae97bbb8b418146c81e1c78b177a92a0c689c4a9e93d24d7a5c`
- After SHA-256: `e8ff009d4da3b97530ce7748357b374d4f90ac48df5d917df4ab884eca9655aa`

Read `feature-specs/README.md`, `reviews/REVIEW-INSTRUCTIONS.md`, and the complete
assigned section. This was an independent source audit; prior review reports were
not used as proof. Only the assigned specification and this report were edited.
No Go or Rust implementation was changed, and no test suite was executed.

## Verified findings and fixes

### 1. Several evidence anchors did not identify the implementation named by their labels

The paths and revision were valid, but some anchors landed on unrelated statements
or omitted the relevant function. Examples:

- `driver_config.rs:746` is the report-size guard, whereas `connection_report`
  begins at line 758. The connection-fields link now targets 758.
- `history.rs:140` constructs a collector, whereas `take_collected` starts at
  152. The drain link now targets 152.
- `session.rs:2374` closes the preceding method. The tracing link now separately
  names `get_tracing_info` at 2380 and `try_getting_tracing_info` at 2427.
- `connection.rs:2324` is a parameter of the bind/connect helper. The
  socket-configuration link now targets `apply_socket_options` at 2354.
- `tls.rs:34` is an import, while backend dispatch is in `TlsConfig::new_tls`
  at 128. The dispatch link now targets that method.
- `Cargo.toml:39` is the Rustls feature alone. The capability link now starts at
  the `[features]` table at 34, including `default = []`.

Also tightened the contact-point expansion, candidate fallback, builder-kind,
profile-default, authentication, TLS selection, client-route configuration,
normalization, rendering, query-report, metrics, response-conversion, and listener
anchors. Split the combined configuration/default and TLS-storage/constructor
links so both parts have direct entry points. Added explicit evidence for the
listener-contract distinction below. Mechanically checked all 68 resulting pinned
links for the specified revision, existing local source path, and valid line
number; checked the local Markdown cross-links as well. These checks used the
pinned checkout, not the current upstream branch or a network link checker.

### 2. Late speculative history was presented as implementation behavior without distinguishing the public warning

`HistoryListener` documentation at `observability/history.rs:38–39` warns that
speculative events may arrive after a request finishes. The pinned executor has
a narrower lifecycle:

- `policies/speculative_execution.rs:187–258` owns branches in a local
  `FuturesUnordered`, polls them in the caller task, and returns on a winning or
  terminal result. It does not spawn those branches; returning drops unfinished
  futures.
- `client/execution.rs:442–510` creates the branch history and awaits that
  executor; `514–536` applies the deadline and invokes terminal history afterward.
- All production attempt-history call sites are in that execution core
  (`execution.rs:352–377,591–645`), not the detached transport router.

Changed the prose and invariant to attribute late-event tolerance to the public
listener documentation while explicitly describing the pinned loop's branch
cancellation ordering. Kept the independent warnings about shared listeners,
collector drains, and detached automatic page work. Added an explicit missing
test for this contract/implementation boundary. `execution.md` already describes
dropping losing branches and did not require a cross-section change.

### 3. Tracing fields were described too weakly as merely path-dependent

`observability/driver_tracing.rs:22–115` declares `result_size` and `result_rows`
on ordinary request spans. A source-wide search finds no recording of either.
`record_raw_rows_fields` at 124–126 records `raw_result_size` instead, which the
constructors never declare. The control-connection constructor at 83–95 declares
only `kind`, `contents`, and `request_size`; generic shard and speculation record
calls do not add undeclared fields.

Checked the locked dependency as well: `Cargo.lock:2739–2742` selects `tracing`
0.1.44, and its locally cached `src/span.rs:1193–1208` records only a field found
in the span's existing metadata. Thus this is not merely uncertainty about which
path supplies result sizes: those values are not recorded into the constructed
spans. Updated the spec to name the unavailable fields and the control-span
boundary precisely. The missing tracing-emission test remains marked missing.

## Whole-section source checks

The following files and symbols were inspected independently of earlier reports.
Ranges identify the relevant source areas, not claims of test execution.

| Area | Inspected source and checked behavior |
| --- | --- |
| Configuration and startup | `client/session.rs:207–675,1407–1618`: fields, default values, validation before UUID creation, client-routes restrictions, metadata-mode derivation, reporting opt-out, initial keyspace failure. `session_builder.rs:36–162,852–854`: public config, builder kinds, client-routes shard-port default, cloning on build. |
| Bootstrap and deadlines | `cluster/node.rs:378–452`: deduplication, all-address expansion, concurrent lookup group and logged failures. `cluster/metadata/cc_establisher.rs:107–138,175–330`: empty-resolution error, randomized candidates, handshake failure continuation, initial metadata fallback. `cluster/control_connection.rs:45–68`: explicit client deadline, saturating derivation and server target gate. |
| Profiles and ownership | `client/execution_profile.rs:174–214,399–535`: optional/default distinction, shared policy objects, independent handles, clone-shared remapping, snapshot getters and labels. `session.rs:1664`, `pager.rs:150–187`, `driver_config.rs:881`: acquisition boundaries for requests, pagers and reports. |
| Socket, compression and authentication | `network/connection.rs:823–900,1559–1606,2136–2284,2320–2424`: per-connection challenge exchange, null/empty tokens, error paths, READY bypass, translated IP identity, compression fallback and socket option ordering/casts. `authentication/mod.rs:13–103`: provider sharing and built-in byte format/challenge rejection. `routing/sharding.rs:13–40`, `network/connection_pool.rs:57`, `policies/reconnect.rs:100–170`: source-port limits, pool default and reconnect limits. |
| TLS | `network/tls.rs:35–151`, `network/tls/openssl.rs:17–324`: backend selection, legacy behavior, IP verification, constructor callback replacement, clone-shared cache and clone-local flag, TLS 1.2 reuse, single-use FIFO TLS 1.3 tickets, bounds, store-based eviction, decode/offer fallback and lack of alternate-ticket retry. |
| Translation and filtering | `policies/address_translator.rs:20–167`, `host_filter.rs:15–105`: peer data, map failures, synchronous allow-list construction, advertised-address checks and untranslatable exemption. `client/client_routes.rs:35–449`: config validation, sticky routes, full/partial refill-set differences, read/write lock scope, endpoint copy before DNS, random address selection and no direct fallback. |
| Identity and reports | `client/self_identity.rs`, `network/connection.rs:240–278,2214–2272`, `driver_config.rs:450–1010`: optional identity fields, stable session UUID, control reporter, size boundary, effective-profile read, policy recognition, DC/rack normalization, custom names, zero/submillisecond handling and buffer truncation. `connection.rs:1825–1842`: orphan threshold is strict and age-conditioned. |
| Metrics | `observability/metrics.rs:50–664`, `metrics_noop.rs`, `observability/mod.rs`: feature-gated public API, shared storage, separate getters, histogram loading/calculations, recording errors, percentile scale and request-driven EWMA ticks. `client/execution.rs:380–658`: attempts, retries, ignored writes, successful latency and deadlines. `connection_pool.rs:960–1090,1131–1167`: gauge increment before admission and missing decrement on discarded targeted connection. |
| History and paging | `observability/history.rs:16–260,310–407`: IDs, mutex insertion/drain, poisoned fallback and structured incomplete histories. `client/pager.rs:202–345,553–607,875–931,1118–1157,1192–1234`: per-page history, closure after fetch, detached worker, control-path instrumentation exclusion and post-core handling. `session.rs:2496–2536`: successful execution history precedes keyspace/schema effects. |
| Warnings and tracing | `observability/driver_tracing.rs`, `tracing.rs`, `utils/safe_format.rs`, `network/connection.rs:1487–1524`, `response/request_response.rs:17–154`, `session.rs:1571–1589,2380–2480`: repeatable formatting, warning logging before body decoding, public metadata preservation/loss, readiness checks, concurrent table reads, deferred event parsing and final unsuccessful-attempt sleep. |
| Feature surface | `scylla/Cargo.toml:34–96,178–210`, `client/mod.rs`, `observability/mod.rs`, `policies/mod.rs`, `utils/mod.rs`, `lib.rs:275–309`: empty default feature set, optional TLS/metrics/value-type integrations, separate serde flag, unstable dual gates, unconditional client-routes value-type exports and report/history availability. |

## Test claims checked and remaining gaps

Verified the existence of all 65 named Rust test symbols in the section and read
the relevant assertions/helpers for their claims. In particular:

- Builder tests check assignments and zero-keepalive rejection, rather than
  network correctness. Report tests check normalization, remapping, schema
  conformance, exact/over-limit size and the separate allowed-omission key sweep.
  The schema-key sweep's enclosing metrics gate remains explicit.
- OpenSSL unit tests exercise a local TLS server, bounded FIFO storage, reusable
  TLS 1.2 sessions, node-specific selection and unusable-session fallback.
  CCM TLS tests check successful backends and mismatched IP SAN build failures;
  the no-client-certificate sanity check uses Rustls alone. The ticket test's
  actual assertions and `min_resumptions_per_node` support the distinct TLS 1.2
  and TLS 1.3 claims.
- Client-route unit tests check missing rules, hostname overrides, replacement
  versus merging, and refill sets. CCM helpers check traffic reaching nodes
  after topology changes, port remapping and route events.
- Identity tests capture one STARTUP per identity configuration. Startup-report
  tests count an expected initial batch without independently identifying the
  control socket. `system.clients` retains its Cassandra ignore gate and
  missing-column early return.
- Execution-profile tests select handles; timeout tests explicitly remap a cloned
  session handle. Metric tests use a no-retry policy and one-page results. History
  tests normalize display data and check single-page/ignored-write outcomes;
  manual history fixtures do not establish runtime cancellation ordering.
- Tracing integration uses empty-table pager helpers and only initial page IDs.
  `integration/utils.rs:368–369` overrides readiness to 200 attempts/50 ms;
  the separate tracing-table helper uses the same budget. No default-budget or
  later-page coverage was inferred. Query-result tests preserve warnings/IDs
  when returning the original result in a non-row conversion error.
- Confirmed CCM TLS/ticket, metrics, and client-routes module feature gates in
  their `mod.rs` files and the CCM bridge's dev-dependency feature unification.

No tests were compiled or run. Source-only checks cannot establish database,
socket, race, or cancellation behavior experimentally. Existing explicit coverage
gaps remain, including auth challenge/error matrices, concurrent ticket handling,
route updates during DNS, socket truncation, metric gauge drift, tracing emission,
and profile/collector races. Added the speculative-history ordering gap described
above. No unresolved cross-section conflict was identified.
