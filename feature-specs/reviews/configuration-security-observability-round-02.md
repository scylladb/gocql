# Configuration, security, and observability — round 02

- Section: `feature-specs/configuration-security-observability.md`
- Round: 02 — defaults, exact conditions, limits, ordering, and fallback boundaries
- Agent identity: `/root/review_configuration_02`
- Reference checkout: `/extra/scylladb/gocql-rust-reference-f8b0ee9`
- Verified source commit: `f8b0ee928dc0dcb475247dc82cce4e42804b0621`
- Before SHA-256: `da1466e7a293222d275d557f04b16ef9c22a699fb86dd8b75c66d02e7e4c9c24`
- After SHA-256: `1fc4a25b43b807f60aaaaadcebc53f96fb5c101452889c5cc08d65b1c743ec4c`

Read the review instructions, README, and entire assigned specification. Checked
claims independently against the pinned implementation. Changed only the assigned
specification and this report; no Go or Rust implementation changes were made.

## Verified findings and fixes

1. **Socket-buffer truncation and the resulting report-schema exception were
   underspecified.** `network/connection.rs:2362–2366` uses `usize as u32` for both
   socket buffer options. `SessionConfig::validate` in `client/session.rs:618–676`
   performs no corresponding range check. The original statement that sizes are
   cast did not explain the boundary behavior. The spec now states that high bits
   are discarded on wider platforms, including the concrete `2³² → 0` case.
   Independently following the reporting path found that
   `client/driver_config.rs:511–516` filters the original value for positivity
   before making the same cast. Consequently a positive multiple of 2³² emits
   `size-bytes: 0`. The bundled `driver-config-schema.json:31–34,345–367` requires
   this field to be at least one. `DriverConfigReporter::report` at
   `driver_config.rs:710–755` serializes and checks length without runtime schema
   validation. Added this source limitation, a missing regression-test entry,
   and boundary-generation guidance. Qualified the conformance-test description
   to selected configurations: the test name does not establish conformance for
   every representable configuration. Reported the cross-section transport lead
   and reporting consequence to the parent.

2. **Client-route refill conditions conflated snapshots and partial updates.**
   `client/client_routes.rs:247–259,271–296` includes a previously known host in
   a full snapshot's returned set only when its sticky route changes; changes
   confined to alternatives are not included. Newly added hosts are included.
   In contrast, `merge_client_routes_update` at `301–378` includes every tracked
   `Some(route)` entry, without comparing the previous route or checking route
   usability. This includes identical and nonsticky routes. Deletions never add
   a host themselves, including deletion that replaces the sticky route with an
   alternative. Replaced the broad “changed or newly usable routes” description
   with these exact conditions. Also scoped the defensive untracked-ID warning
   to partial updates, where it actually occurs. Added existing refill-set test
   targets and explicitly marked the remaining asymmetric cases as missing.

3. **Mixed-version TLS cache selection and fallback ordering were omitted.**
   `network/tls/openssl.rs:145–157` pops the oldest TLS 1.3 ticket before considering
   the reusable TLS 1.2 session. `OpenSsl010Config::new_ssl` at `288–313` consumes
   only one selected entry; decode or offer failure does not retry another cache
   entry before the full handshake. Added both the priority rule and this
   fallback boundary. Existing tests establish FIFO, reuse, unusable-entry
   fallback, and cache limits separately; added a missing test entry for their
   mixed-version combination rather than claiming it is covered.

4. **Metadata client-deadline derivation lacked exact edge conditions.**
   `cluster/control_connection.rs:45–67` gives an explicit client timeout
   precedence, including zero; otherwise it saturating-adds one second to the
   configured server timeout, independently of target type, or uses 30 seconds
   if neither override exists. `serverside` alone filters by ScyllaDB target.
   `control_connection.rs:216` applies the computed client value as `Some`.
   Following the metadata review's cross-section lead independently confirmed
   that `get_or_prepare_statement` at `175–193` completes cache-miss PREPARE
   before `query_iter` at `203–227` attaches that deadline for page execution.
   Expanded the default table to state saturation, zero/explicit precedence,
   the 31-second default on non-ScyllaDB targets too, and that unset is derivation
   rather than disabling the deadline. Explicitly excluded cache-miss PREPARE
   and the overall multi-page refresh from its scope. Added its pinned evidence
   link.

## Whole-section checks and inspected source

Paths below are relative to the Rust checkout unless otherwise stated.

- `client/session.rs:207–685,1407–1589,2374–2493` — configuration fields/defaults,
  warning versus error validation, identity allocation after validation,
  connection/pool option transfer, reporting opt-out, initial-keyspace failure,
  trace-query construction, readiness order, immediate query errors, and the
  final unsuccessful-attempt sleep. The default table otherwise matches source.
- `client/session_builder.rs:30–168,852–854,1604–1845` and
  `client/execution_profile.rs:173–205,494–539` — reusable build cloning, client
  routes builder flags, configured/default profiles, shared remapping cells.
  `statement/mod.rs:26,83` and `network/connection_pool.rs:53–60` confirm page size
  5000 and per-shard pool size one. `routing/sharding.rs:13–40` confirms inclusive
  49152–65535 defaults and rejection of empty/below-1024 source-port ranges.
  `policies/reconnect.rs:70–245` confirms exponential default limits, jitter and
  clamping, as well as custom-policy constructor boundaries.
- `authentication/mod.rs:8–100` and `network/connection.rs:823–900,2136–2430` —
  per-connection authentication state, null/empty token types, unbounded
  challenge loop, plaintext credential bytes, READY bypass, missing-provider
  failure, OPTIONS/STARTUP compression fallback, socket-option ordering, TCP
  keepalive time versus probe interval/retries, and identity/report placement.
- `network/tls.rs:34–172`, `network/tls/openssl.rs:17–336,531–711`, and
  `network/connection.rs:1559–1608` — both IP verification paths, raw-context
  verification caveat, legacy context limitations, shared IP-keyed ticket cache,
  eight-ticket and 256-node bounds, oldest-ticket consumption and eviction by
  last store, serialization, disabled-ticket paths, and no plaintext fallback.
- `policies/address_translator.rs:103–174`, `policies/host_filter.rs:15–115`,
  `cluster/node.rs:326–374`, and `client/client_routes.rs:24–437` — contact-point
  translation exclusion, strict map behavior, allow-list construction and
  untranslatable-peer acceptance, exact datacenter match, nonempty routes
  configuration, sticky route IDs, missing-route/port failures, hostname override,
  per-attempt DNS, random returned-address selection, and no alternate-ID retry.
- `client/self_identity.rs:3–125`, `client/driver_config.rs:43–1015`,
  `client/driver-config-schema.json`, `cluster/metadata/cc_establisher.rs:100–137`,
  and `policies/mod.rs:37–85` — named identity fields, control-only report,
  version/size limits, profile reacquisition, zero/millisecond/linger
  normalization, policy recognition/fallback, node-preference distinction,
  intentionally unreported groups, and default custom-policy naming.
- `observability/metrics.rs:50–525,684–810`, `metrics_noop.rs`,
  `client/execution.rs:460–713`, `client/pager.rs:306–370`, and
  `network/connection_pool.rs:1010–1160` — shared counters/histogram, attempt
  accounting, retries, deadline-specific accounting, successful latency samples,
  histogram bounds and approximation, aggregate getters, record-driven EWMA,
  feature-disabled API absence, control-socket exclusion, and the already
  documented discarded-shard-connection overcount. Searches of `cluster` found
  no control-connection gauge increments.
- `observability/history.rs:26–455,610–788`,
  `observability/driver_tracing.rs:21–209`, `observability/tracing.rs`,
  `utils/safe_format.rs`, and `network/connection.rs:1487–1525` — listener IDs,
  cancellation/terminal history, UTC collection, draining/poison behavior,
  incomplete parent grouping, span fields and decoding placeholder, repeated
  iterator formatting, warning logging/metadata, and trace record fields.
- `scylla/Cargo.toml:39–100`, `client/mod.rs`, `observability/mod.rs`,
  `policies/mod.rs`, `utils/mod.rs`, and `lib.rs:274–325` — optional-feature
  defaults and exports, serialization grouping, metrics/TLS gating, unstable
  double gates, and unconditional public client-routes value types.

## Test coverage and validation

No Rust or Go tests, database tests, or benchmarks were executed. This is a source
review of documentation and test assertions, not evidence of runtime success.

Inspected unit-test assertions for builder defaults/compression, pre-connect zero
keepalive rejection, profile selection, default/remapped reports, duration and
linger normalization, per-host report behavior, selected schema-conformance
configurations and negative controls, exact 32-KiB report-size boundary, TLS
resumption/bounds/fallback, route absence/overrides/snapshot/partial updates and
refill sets, histogram distribution/percentile scale/order, and incomplete/error/
speculative history. The socket-buffer conformance cases use zero and ordinary
sizes, not the wider-integer truncation boundary.

Inspected integration-test definitions, setup, and relevant assertions in:

- `ccm/authenticate.rs`, `ccm/tls.rs`, and `ccm/tls_tickets.rs` for built-in/custom
  authentication, IP-SAN rejection, configured client certificates, and cluster
  ticket-resumption scenarios. The custom authentication example is plaintext
  authentication with a custom provider, not multi-challenge coverage.
- `session/self_identity.rs` and `session/startup_options.rs` for STARTUP fields,
  shared session IDs, control-only report and opt-out, and the documented
  Cassandra/absent-column skip boundaries.
- `session/new_session.rs`, `statements/execution_profiles.rs`, and
  `ccm/client_routes.rs` for partial/all DNS failure, configured execution
  profiles, topology/port-remap/reroute scenarios.
- `session/metrics.rs`, `session/history.rs`, and `session/tracing.rs` for separate
  paging counters, successful/failed/page histories, and query/execute/prepare/
  batch/pager tracing calls and table lookups.

A local documentation check resolved all **44 pinned source links** to existing
files/line numbers and all **53 named test targets** to function declarations.
That check passed. It checks reference validity, not test behavior.

The explicit missing-test entries remain gaps, including malformed/incomplete
trace readiness, cancellation, gauge/EWMA edge cases, compression fallback, and
Go API/race/integration/benchmark coverage. Newly added edge cases are source
inferences from direct branches and casts, with no claim of an executed socket,
TLS, DNS, schema-validator, or database reproduction. No other changes were
necessary in the assigned section.
