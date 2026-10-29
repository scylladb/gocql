# Configuration, security, and observability — round 06

- Section: `feature-specs/configuration-security-observability.md`
- Round: 06 — omitted public, optional, and less-common behavior; catalogue-owned modules and configuration observability.
- Agent identity: `/root/review_configuration_06`
- Reference checkout: `/extra/scylladb/gocql-rust-reference-f8b0ee9`
- Source commit verified with `git rev-parse HEAD`: `f8b0ee928dc0dcb475247dc82cce4e42804b0621`
- Before SHA-256: `90977ea08747fc7e804f36877330e88280cb483615ad4b4bdb29ab9d67b1e63f`
- After SHA-256: `1b5f5b12cbdeeae97bbb8b418146c81e1c78b177a92a0c689c4a9e93d24d7a5c`

Read the complete section, `feature-specs/README.md`, and
`feature-specs/reviews/REVIEW-INSTRUCTIONS.md`. Used the source catalogue to check
module breadth. Findings below come from the pinned implementation and test
assertions; previous review reports were not used as proof. Changed only the
assigned specification and this report. No Go or Rust implementation changed.

## Verified findings and fixes

1. **The effective-configuration report omitted important interpretation rules.**
   `scylla/src/client/driver_config.rs:160–182, 299–352, 510–577, 601–634,
   746–779, 879–997` shows that its output is not a literal copy of every session
   setting. Added the reported in-flight/orphan thresholds, omitted pool count and
   keepalive duration, fixed statement defaults, omitted disabled query defaults,
   retry/reconnection omissions, and the additional-execution meaning of
   `max-executions`. Checked the orphan cutoff against
   `network/connection.rs:74–77, 1829–1842, 2457–2464, 2558–2560`: the reported
   1024 is an age-qualified threshold, with closure above it, not a hard count of
   all outstanding orphans. Added token-awareness fallback to custom reporting,
   fixed-seed distribution, the rack/DC fallback distinction, latency signal
   reporting, empty-location normalization, and the `unknown` replacement for an
   explicitly empty load-balancing name. These additions describe reporting
   behavior without altering the routing contracts in another section.

2. **Public configuration inspection and handle labels were undocumented.**
   `client/session_builder.rs:79–83` exposes `config`; `client/execution_profile.rs:
   415–424, 444–490, 506–530` implements getters, `to_builder`,
   `pointee_to_builder`, `to_profile`, labeled handles, and remapping. Added that
   inspection produces values based on the current profile, that later remapping
   does not update previously obtained snapshots/builders, and that labels are
   debug information rather than shared-handle identity. The pre-existing caveat
   about mutable state in shared policy objects still applies. Added a missing
   focused-test entry rather than claiming that report-remapping tests prove
   these inspection/label contracts.

3. **Allow-list hostname resolution had a different lifetime and timeout model.**
   `policies/host_filter.rs:44–77` uses synchronous `ToSocketAddrs` during
   construction and keeps only a `HashSet<SocketAddr>`. It has no session timeout
   parameter or refresh path. Added synchronous, one-time resolution, independence
   from `hostname_resolution_timeout`, and retention of the original resolved set
   after DNS changes. Existing statements about resolution errors and
   untranslatable-peer acceptance remain accurate. Added the focused coverage gap.

4. **Managed OpenSSL construction changes caller-provided resumption settings.**
   `network/tls/openssl.rs:192–252` makes both constructors call `configure`, which
   sets `CLIENT | NO_INTERNAL_STORE` and installs the driver's new-session
   callback. Added that this replaces the supplied cache mode and new-session
   callback. This qualifies the otherwise broad description of backend
   configuration while leaving certificate verification, protocol versions,
   clone-local ticket enablement, and shared ticket storage unchanged. Existing
   ticket tests exercise the installed callback, but do not test replacement of a
   caller callback; that gap is explicit.

5. **Feature availability and configuration-report test coverage were incomplete.**
   `scylla/Cargo.toml:39–119, 139–150, 194–199`, `client/mod.rs:23–42`, and
   `observability/mod.rs:8–21` establish that reporting JSON, history, local
   tracing, and database tracing do not depend on optional `metrics` or `serde`.
   Added that distinction. `client/driver_config_tests.rs:1477–1485, 1497–1564,
   1688–1733` contains a separate, metrics-gated schema-key coverage sweep that was
   absent from the test inventory. Added its bidirectional comparison with the
   documented omission set and its limit: it checks schema-key breadth, not
   representation of every runtime setting. Also added the existing focused
   report tests for routing normalization, omitted defaults, and speculation
   limits, whose assertions support finding 1.

## Source inspected and whole-section checks

Paths below are relative to the pinned Rust checkout. Line ranges identify the
principal inspected implementations, not assertions that every line in a large
module was relevant.

| Source and key symbols inspected | Behaviors checked |
| --- | --- |
| `scylla/src/client/session.rs:207–683, 1407–1599, 2345–2531, 2836`; `client/session_builder.rs:79–180, 193–388, 852, 1589–1700` | Configuration fields/defaults and validation; public builder config; cloning on build; generated UUID; reporter/pool/control configuration; initial keyspace failure; tracing statement configuration and lookup; session metrics/profile access. |
| `scylla/src/client/execution_profile.rs:173–239, 395–530` | Default CL/serial CL/timeout/policies; immutable profile and shared remapping cell; snapshots, seeded builders, labels. |
| `scylla/src/client/client_routes.rs:24–460` | Nonempty proxy list; optional hostname overrides; sticky route replacement/merge; no direct fallback; missing ports; DNS after releasing read lock; refill-set distinctions. |
| `scylla/src/client/self_identity.rs`; `network/connection.rs:240–277, 2136–2298` | Named identity fields, STARTUP UUID/report placement, per-connection compression negotiation and fallback. |
| `scylla/src/client/driver_config.rs:43–1010`; `client/driver_config_tests.rs:193–505, 519–1058, 1477–1768` | Report groups, omissions and normalization; profile acquisition; size boundary; built-in/custom policy mapping; schema coverage. |
| `scylla/src/authentication/mod.rs`; `network/connection.rs:823–900` | Null/empty token types, per-connection exchange creation, challenge loop, success callback and distinct error phases; plaintext token layout and challenge rejection. |
| `scylla/src/network/tls.rs`; `network/tls/openssl.rs:17–350, 430–716`; `network/connection.rs:1559–1595` | Backend feature dispatch and IP identity; constructor effects; TLS 1.2/1.3 cache behavior, bounds, locking and fallback; clone-local disabling; no plaintext downgrade. |
| `scylla/src/policies/address_translator.rs`; `policies/host_filter.rs` | Translator input/accessors, socket/string maps and errors; feature-gated adapter constructors; filter variants, synchronous DNS and untranslatable exception. |
| `scylla/src/observability/metrics.rs:1–823`; `observability/metrics_noop.rs`; `client/execution.rs:500–699` | Optional APIs; per-attempt request/error/timeout/retry accounting; histogram scope/approximations and getters; EWMA update timing; independent reads and shared storage. |
| `scylla/src/observability/history.rs:1–507`; `observability/driver_tracing.rs`; `observability/tracing.rs`; `utils/safe_format.rs` | Listener/collector events and IDs; drain/poison behavior; incomplete structured history; spans and missing raw-result field declaration; repeatable formatting; trace data/readiness. |
| `scylla/src/client/mod.rs`; `scylla/src/lib.rs:279–326`; `policies/mod.rs`; `observability/mod.rs`; `utils/mod.rs`; `scylla/Cargo.toml` | Public/internal exports, default-empty feature set, TLS/metrics feature gates, dual `scylla_unstable` gates, ungated client-route value types, policy names, always-available diagnostics. |
| `scylla/src/cluster/node.rs:385–510`; `cluster/metadata/cc_establisher.rs:183–380`; `cluster/control_connection.rs:45–68` | Concurrent contact resolution and deduplication; shuffled candidates; partial DNS/startup metadata behavior; explicit/derived metadata deadline. |
| `scylla/src/client/pager.rs:149–230, 306–355, 919–940`; `network/connection_pool.rs:1050–1085, 1135–1170`; `response/request_response.rs` | Pager profile capture and detached worker; successful-open metric before admission and discarded shard-targeted connection; loss of error metadata/custom payloads. |
| `scylla/src/book_tests.rs` | The catalogue's test-evidence module includes documentation chapters for authentication, TLS, client routes, profiles, metrics, logging and tracing; it is not an executed integration suite. |

No additional mismatch was found in the existing authentication/TLS security
invariants, route fallback restrictions, profile request/pager acquisition
boundaries, report opt-out/UUID independence, or instrumented-core-versus-public
outcome distinctions. These were checked against implementation, rather than
accepted from earlier reports.

## Tests and validation

Read the new report coverage/matrix assertions directly, including
`default_load_balancing_policy_matrix`,
`load_distribution_follows_replica_shuffling`,
`effective_node_preference_is_reported`,
`custom_load_balancing_policies_are_reported_by_name`,
`unset_query_defaults_are_omitted`, `speculative_execution_report_cases`, and
`coverage::every_reportable_schema_key_is_reported`. Checked the feature gate on
the last test and its exact declared-minus-reported equality.

Rechecked the metrics characterization/percentile/ordering assertions and OpenSSL
unit ticket tests. Inspected integration test entry points and relevant assertions
in `tests/integration/session/{startup_options,self_identity,metrics,history,
tracing,new_session}.rs`, `tests/integration/ccm/{authenticate,tls,tls_tickets,
client_routes}.rs`, and `tests/integration/statements/{execution_profiles,
request_timeout}.rs`; checked the integration module gates. Retained the existing
limits on what these tests establish: one-page metrics cases, nondefault tracing
readiness budget, STARTUP report counting without independently identifying the
control socket, and deployment-dependent `system.clients` coverage.

A local static reference check resolved all 65 test-symbol references and all 62
pinned source links; link validation checked revision, local file existence, and
line-number bounds. Direct checks of both owned Markdown files found no trailing
whitespace or conflict markers; the recorded final hash matches the section. No
Rust/Go test, database test, feature-matrix compilation,
benchmark, or external source fetch was run in this round.

The focused gaps added for profile inspection/labels, allow-list DNS lifetime,
and managed TLS callback replacement remain untested claims established from
source. Existing missing concurrency, cancellation, failure-matrix and large
socket-buffer coverage remains explicit. No cross-section edit was needed.
