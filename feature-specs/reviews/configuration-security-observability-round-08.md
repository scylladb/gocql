# Configuration, security, and observability — round 08

- Section: `feature-specs/configuration-security-observability.md`
- Round: 08 — cross-section consistency and caller/callee boundaries.
- Agent: `/root/review_configuration_08`
- Reference checkout: `/extra/scylladb/gocql-rust-reference-f8b0ee9`
- Verified source HEAD: `f8b0ee928dc0dcb475247dc82cce4e42804b0621`
- Before SHA-256: `e8ff009d4da3b97530ce7748357b374d4f90ac48df5d917df4ab884eca9655aa`
- After SHA-256: `c6fa07177763be0d42c4d84010a9cd7cabfe69ec83ec1f9d69ae3c62d57bf712`

Read `feature-specs/README.md`, the complete assigned section, and
`reviews/REVIEW-INSTRUCTIONS.md`. Independently inspected the pinned implementation
and compared the overlapping contracts in all five neighboring sections. Edited
only the assigned section and this report. No Go or Rust code was changed, no
subagents were used, and no Rust or database tests were executed.

## Verified findings and fixes

### 1. Host-filter wording omitted the control-connection exceptions

The assigned section said filtering determines which peers may be selected when
re-establishing control connections, without recording the exceptions already
specified in `metadata-control-events.md`.

`cluster/metadata/cc_establisher.rs:120–146` constructs the initial candidate list
from contact points. `214–238` resolves those original contacts again on recovery;
neither path applies the discovered-peer filter. `357–377` filters the future
discovered-peer list, but `380–408` matches a retained candidate to metadata only
when its endpoint is `UntranslatedEndpoint::Peer`. A contact-point endpoint can
therefore survive even when its discovered peer is rejected. The established
connection's successful full/topology refresh branches in
`cluster/metadata/worker.rs:784–804` publish updates without invoking this rejection
check, so a later rejection does not itself move that control connection.

Updated the filtering prose and invariant to distinguish query-pool eligibility,
future discovered-peer candidates, contact-point bootstrap/fallback, and retained
control sockets. Added direct source evidence, the metadata cross-link, and an
explicit missing regression-test entry. This fixes the assigned section; the
neighboring metadata section already states the correct behavior.

### 2. Metadata timeout reporting omitted its prepared-cache boundary

The defaults table described server metadata timeouts as applied only on supported
targets, while the report prose said each handshake reflects target-specific
support. These statements omitted the difference between a new report and cached
metadata query text.

`cluster/control_connection.rs:175–195` returns a cached prepared statement before
calling `maybe_append_timeout_override`. Its cache key is the original query text,
not the target or negotiated capabilities. The cache is created once in
`cluster/metadata/cc_establisher.rs:127` and cloned into candidate connections at
`287`; it survives replacement of the control connection. By contrast,
`network/connection.rs:2251–2254` renders a new report with the current connection's
ScyllaDB detection result, and `client/driver_config.rs:843–867` gates the reported
server timeout on that result. A target-support change can consequently yield a
fresh report whose server-timeout field does not describe the clause retained in
an older cached query. `control_connection.rs:208–228` assigns the client deadline
on a per-use clone, outside that server-clause caching boundary.

Corrected the default table and added the distinction to reporting, with direct
source links and a missing test for report/query-text disagreement. The metadata
section already documents persistent query text and needs no edit.

## Whole-section behavior and implementation checks

Source paths below are relative to the reference checkout. These are inspected
implementation areas, not executed tests.

| Area | Files/symbols inspected and checked behavior |
| --- | --- |
| Configuration/startup | `client/session.rs:207–675,1407–1597` (`SessionConfig`, `new`, `validate`, `connect`, internal trace statements): defaults, warnings versus rejection, validation before UUID, reporting opt-out, connection config assembly, schema mode and keyspace initialization. `session_builder.rs:36–162,852–854`: public reusable configuration, specialized client-route builder and build-time cloning. |
| DNS/bootstrap | `cluster/node.rs:407–452` (`resolve_contact_points_inner`): all resolved addresses, deduplication, logged individual failure and whole-group wait. `metadata/cc_establisher.rs:120–408`: initial candidates, shuffling, recovery contacts, initial metadata fallback, retained metadata and filter checks. |
| Profiles | `client/execution_profile.rs:174–214,494–535`, `session.rs:1655–1705`, `pager.rs:149–187`, `driver_config.rs:881–913`: defaults, clone-shared remapping cell, inspection snapshots, per-request/pager/report acquisition and shared policy state. |
| Transport/authentication | `network/connection.rs:823–913,1487–1524,1559–1615,2136–2447`, `authentication/mod.rs`: STARTUP READY/authentication split, fresh exchanges, challenge/success sequencing and errors, nullable tokens, credential bytes, independent compression fallback, TLS before router creation, socket-option casts/order and platform keepalive branches. `scylla-cql/src/frame/{request/auth_response,response/authenticate}.rs` confirms optional-byte framing. |
| Bounds/reconnection | `routing/sharding.rs:13–40` and `policies/reconnect.rs:100–170`: source-port validation and default exponential delays/jitter. The connections section has the precise TCP-only timeout and refill scheduling scopes; this section's defaults remain consistent with them. |
| TLS | `network/tls.rs`, `network/tls/openssl.rs:17–340`: translated-IP identity, legacy/managed backend dispatch, replacement callback/cache mode, shared bounded store, copied enablement flag, FIFO TLS 1.3 consumption, reusable TLS 1.2, store-time eviction, single-entry decode/offer fallback and no plaintext downgrade. |
| Address policies/routes | `policies/address_translator.rs`, `host_filter.rs`, `client/client_routes.rs:35–449`: peer inputs, strict map lookup, synchronous fixed allow-list, advertised-address comparison, nonempty route list, sticky IDs, full versus partial refill sets, no missing-rule fallback, hostname override, lock release before DNS and random resolved address. |
| Identity/report | `client/self_identity.rs`, `connection.rs:240–278,2214–2284`, `driver_config.rs:490–1010`: named identity fields, stable UUID, control-only reporting, size omission, profile acquisition, DC/rack and custom policy reporting, zero/duration/buffer normalization, TLS-presence-only reporting and incomplete configuration schema. |
| Metrics | `observability/metrics.rs:50–645`, `metrics_noop.rs`, `observability/mod.rs`, `client/execution.rs:385–658`: shared storage, independent reads, one loaded histogram per snapshot, attempt/error/retry/deadline distinctions, ignored-write handling and request-triggered EWMA ticks. `connection_pool.rs:1020–1175` confirms the gauge admission/discard discrepancy and pool-only timeout increment. |
| History/lifetime | `observability/history.rs:16–260`, `execution.rs:442–658`, `policies/speculative_execution.rs:187–242`, `pager.rs:202–375,553–607,875–934`: synchronous listener callbacks, preserved IDs across drains, poisoning fallback, deadline/outcome boundaries, locally polled speculative futures and detached later-page work. `session.rs:2496–2533` places keyspace/schema side effects after successful core history. |
| Response diagnostics/tracing | `observability/driver_tracing.rs`, `tracing.rs`, `utils/safe_format.rs`, `response/request_response.rs`, `session.rs:1571–1597,2380–2480`: declared/unrecorded span fields, repeatable formatting, warnings before body parse, public result versus pager/error metadata, trace-table concurrency, readiness, deferred events parsing and final incomplete-attempt sleep. |
| Feature surface | `scylla/Cargo.toml:34–94,175`, `client/mod.rs`, `observability/mod.rs`: empty default features, TLS/metrics/external value integrations, separate serde flag, client-route types exported independently of the session feature and CCM dev-dependency feature unification. Session/builder source confirms dual host-listener/reconnect gates. |

## Consistency across the five neighboring sections

- `routing.md`: profile-policy mutability, fixed pager snapshots, callback timing,
  retry/connection-selection separation and DC/rack report interpretation agree.
  Read `policies/load_balancing/default.rs` preference, failover and fixed-seed
  declarations/call sites alongside the report implementation.
- `connections.md`: socket defaults, TCP timeout scope, per-connection negotiation,
  query-pool filtering and retained connection lifetimes agree. Its targeted
  excess-connection discard behavior is the branch behind the gauge caveat
  documented in the assigned section.
- `metadata-control-events.md`: DNS/startup behavior, per-page client deadlines,
  control replacement, client-route update/refill boundaries and filter behavior
  agree after the two fixes above.
- `execution.md`: settings snapshots, explicit preparation outside core
  instrumentation, attempts/retries, ignored writes, post-core side effects,
  automatic page lifetimes and losing speculative branch cancellation agree.
- `serialization-protocol.md`: authentication optional tokens, negotiated
  compression and internal versus public response extensions agree. Owned
  warning/trace metadata is not treated as borrowed value-row storage.

No independently verified problem requiring another agent's file to change was
found. The parent was notified of the two assigned-section findings during review.

## Test claims and validation

Mechanically checked all 73 pinned implementation links for revision, existing
source file and in-range source line, and all 65 named Rust test symbols for
existence. Inspected test declarations/assertions in builder, driver-config,
client-route, OpenSSL, metrics, history and query-result unit-test modules.
Focused body/helper reads checked these boundaries:

- Report normalization and remapping tests describe JSON behavior; the schema-key
  sweep compares both missing and stale allowed omissions. The size test accepts
  exactly 32 KiB and rejects one byte over. Neither the report tests nor the
  server-timeout clause test exercises cache reuse across target-support changes.
- Startup tests count one report in the expected initial frame batch and preserve
  UUIDs under opt-out. They do not independently identify the control connection
  or exercise control replacement after remapping.
- Metrics integration uses a fallthrough policy and a single-row page. It does
  not establish combined retry/speculation/later-page/deadline accounting.
- TLS mismatch tests assert session-build failure. The missing-client-certificate
  negative case uses Rustls. Ticket tests distinguish reusable TLS 1.2 from bounded
  TLS 1.3 supply and assert no offers for disabled/legacy managed paths.
- Execution-profile selection tests change handles, whereas the timeout test
  explicitly remaps a cloned session handle. Trace tests use an empty table for
  initial pager IDs and a helper overriding readiness to 200 attempts/50 ms.
- Confirmed TLS/ticket dual-feature, metrics and client-route module gates in
  integration `mod.rs` files. Existing manual-history fixtures and histogram tests
  do not establish the listed concurrency, cancellation or large-bucket gaps.

No tests were compiled or run. New and existing `missing` entries remain coverage
gaps, not evidence of experimental validation. Source inspection supports the two
fixes; OS/TLS/backend behavior, scheduling races and database behavior still need
the separately tracked runtime verification. The untracked documentation files
were checked directly for trailing whitespace; `git diff --check` alone would not
cover those files.
