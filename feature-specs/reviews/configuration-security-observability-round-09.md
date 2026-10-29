# Configuration, security, and observability — round 09

- Section: `feature-specs/configuration-security-observability.md`
- Round: 09, adversarial edge cases and source quirks, with a whole-section audit.
- Agent identity: `/root/review_configuration_09`
- Reference checkout: `/extra/scylladb/gocql-rust-reference-f8b0ee9`
- Verified source commit: `f8b0ee928dc0dcb475247dc82cce4e42804b0621`
- Before SHA-256: `c6fa07177763be0d42c4d84010a9cd7cabfe69ec83ec1f9d69ae3c62d57bf712`
- After SHA-256: `040fa2bebfba401c2e091c9e49fe24e4fa7398c3465553e56bac0d4804b8b93b`

Read `REVIEW-INSTRUCTIONS.md`, `feature-specs/README.md`, and the complete assigned
section. Traced source independently; previous round reports were not used as
evidence. Changed only the assigned section and this report. No Go or Rust source
was edited, and no Rust, Go, database, or TLS test suite was executed.

## Verified findings and fixes

1. **The hostname-resolution timeout is not one overall deadline.**
   `scylla/src/cluster/node.rs:326–375` wraps each `lookup_host` separately.
   After any first error other than `DnsLookupError::Timeout`, `resolve_hostname`
   tries `(original_string, 9042)` with a fresh timeout, including when the string
   already contains a port. `.or(Err(e))` preserves the first error if the second
   lookup fails or times out. The prior default table and client-route wording
   did not expose this budget/error boundary. Added the exact fallback behavior,
   qualified the default as per lookup, applied that boundary to route resolution,
   and recorded the missing deterministic test. The metadata section already
   describes this accurately; no cross-section correction is needed.

2. **Empty custom policy-name normalization covers only load balancing.**
   `driver_config.rs:577–584` substitutes `unknown` for an empty load-balancing
   name, whereas `reconnection_policy_report` at `798–827`,
   `speculative_execution_report` at `991–1017`, and `retry_policy_report` at
   `1020–1040` serialize `reported_name()` directly. The hook can be overridden
   on `RetryPolicy` (`policies/retry/retry_policy.rs:92`), `ReconnectPolicy`
   (`policies/reconnect.rs:56`), and `SpeculativeExecutionPolicy`
   (`policies/speculative_execution.rs:61`). Their default names are nonempty,
   but an explicit empty override violates the schema's `nonEmptyString`
   requirement (`driver-config-schema.json:39`, `441`, `556`, `638`). Runtime
   `report()` only serializes and checks size (`driver_config.rs:725–755`), so it
   still emits that document. Added this limitation and its missing-test entry
   without changing the valid default-name or load-balancing behavior.

3. **The OpenSSL reusable slot is not restricted to TLS 1.2 by the source.**
   `network/tls/openssl.rs:115–138` selects the single-use queue only for
   `SslVersion::TLS1_3`; every other version enters the slot named `tls12`.
   `take()` (`145–162`) reuses that slot whenever the TLS 1.3 queue is empty.
   The section described TLS 1.2 and TLS 1.3 behavior correctly but omitted the
   branch taken by older versions enabled in a supplied backend configuration.
   Added that qualification and a missing-test entry. This is an implementation
   classification, not a claim that older versions are enabled by default or
   were exercised here.

4. **TLS session reuse and CQL authentication have separate lifetimes.**
   TLS setup in `network/connection.rs:1559–1598` precedes the unconditional
   OPTIONS/STARTUP sequence in `open_connection` (`2136–2294`). AUTHENTICATE
   always invokes `perform_authenticate`, whose provider call at `835–838`
   creates a fresh exchange. The managed TLS callback banks sessions using only
   the endpoint IP (`network/tls/openssl.rs:234–249`), and no later CQL failure
   path purges that cache. Added the explicit rule that resumed transport still
   authenticates when requested and that later authentication/CQL setup failure
   does not invalidate already banked sessions. Added the corresponding coverage
   gap; existing resumption tests do not establish this combined failure case.

All four fixes include pinned source links or links to the directly supporting
functions, and the test-generation notes now include these boundaries.

## Whole-section behaviors checked

- Configuration/defaults/validation: inspected `client/session.rs:207–680` and
  `1407–1570`, including all `SessionConfig` fields, empty-contact and zero-CQL-
  keepalive rejection, warning-only schema/timeout combinations, client-routes
  conflicts, UUID generation order, default reconnect policy substitution,
  metadata fetch-level derivation, and initial keyspace application.
- Ownership and profiles: `client/session_builder.rs:36–145` and `852–854`,
  `client/execution_profile.rs:174–240` and `494–533`, and
  `client/pager.rs:149–195` confirm reusable builders, shared handle remapping,
  immutable profile snapshots, labels, and once-per-pager settings. Defaults
  remain LOCAL_QUORUM/LOCAL_SERIAL, 30 seconds, and no speculative policy.
- Startup and filtering: `cluster/node.rs:326–462`,
  `cluster/metadata/cc_establisher.rs:214–423`, and
  `policies/host_filter.rs` confirm all-address expansion/deduplication,
  whole-group DNS waiting, candidate failure handling, initial metadata fallback,
  original-contact-point exceptions, synchronous fixed allow-list resolution,
  advertised-address matching, and untranslatable-peer acceptance.
- Socket/negotiation/identity: `network/connection.rs:240–278`, `2136–2294`,
  `2354–2430`, `client/self_identity.rs`, and `routing/sharding.rs:18–38`
  confirm explicit identity fields, session UUID on STARTUP, per-connection
  unsupported-compression fallback, unchecked `usize` to `u32` buffer conversion,
  OS-specific TCP keepalive options, and source-port-range validation. The
  connections section retains the precise TCP-only setup timeout and compression
  parsing boundaries.
- Authentication: read `authentication/mod.rs` and
  `network/connection.rs:823–923`; confirmed READY bypass, missing-provider
  failure, null versus empty tokens, serialized challenge handling within an
  exchange, distinct setup causes, no fixed challenge count, shared providers,
  built-in byte encoding, challenge rejection, and the limited success callback.
- TLS: read `network/tls.rs:128–155`, `network/tls/openssl.rs:17–343`, and
  the connection backend dispatch. Confirmed connected-IP verification,
  backend-supplied trust policy, dangerous raw-builder defaults, callback/cache-
  mode replacement, shared IP-keyed cache, 8/256 limits, FIFO consumption,
  store-recency eviction, DER storage, clone-local disabling, one-entry fallback,
  legacy limitations, and absence of plaintext fallback.
- Client routes: inspected `client/client_routes.rs:24–445`; confirmed nonempty
  list validation without unique/nonempty-ID validation, sticky selection,
  missing-route/port errors, no direct fallback, replacement/merge refill-set
  distinctions, hostname overrides, random per-attempt address choice, and the
  lock/copy-before-DNS boundary. Builder/config gate differences remain explicit.
- Reporting: inspected `client/driver_config.rs:490–1040`, its schema, and
  `cluster/control_connection.rs:35–70`, `165–226`. Confirmed report opt-out,
  control-only attachment, remapping snapshots, 32-KiB bound, target-gated timeout
  reporting versus retained prepared query text, positive/zero millisecond
  normalization, buffer truncation mismatch, location/rack fallback semantics,
  custom-policy fallback, additional speculative execution counts, omitted
  credentials/TLS contents/heartbeat details, and no runtime schema validation.
  `network/connection.rs:1829–1844` confirms the report's orphan count does not
  encode the runtime age and strict-greater-than conditions.
- Metrics and history: inspected `observability/metrics.rs:1–525`,
  `metrics_noop.rs`, `client/execution.rs:440–700`,
  `client/pager.rs:149–350` and `909–926`, and
  `network/connection_pool.rs:1038–1174`. Confirmed feature-disabled API removal,
  shared storage, non-global snapshots, successful-attempt-only histogram
  samples, request/error/timeout/retry boundaries, no count for connection
  selection failure, ignored-write error accounting, idle EWMA behavior,
  excluded control sockets, excess-shard discard overcount, and detached page
  work after pager drop. `observability/history.rs:1–260` confirms synchronous
  callbacks, drain/counter behavior, unbounded retention, and poisoning fallback.
- Logs/tracing/results: read `observability/driver_tracing.rs`,
  `observability/tracing.rs`, `client/session.rs:2380–2499`,
  `network/connection.rs:1487–1505`, and `response/request_response.rs`.
  Confirmed query/key disclosure, undeclared `raw_result_size`, warning logging
  before body deserialization, dropped custom payload/error metadata, tracing
  readiness requirements, deferred events deserialization, separate unpaged
  queries, final incomplete-attempt sleep, and nullable tracing event fields.
- Optional features: inspected `scylla/Cargo.toml:34–94`,
  `observability/mod.rs`, `policies/mod.rs:17–30`, and cfgs in session/builder
  and speculative-execution code. Confirmed no default optional features, both
  TLS providers, metrics-gated percentile speculation, serialization integration
  grouping distinct from serde, and the unstable-feature/compiler-cfg boundaries.

## Test claims and verification limits

- Checked report test bodies for schema conformance, bidirectional schema-key
  coverage, load-balancing empty-name substitution, and exact/over-size bounds
  in `client/driver_config_tests.rs:909–929`, `1380–1470`, `1484–1490`,
  `1702–1790`. These are selected configuration sweeps, not validation of every
  possible custom hook. The new empty-hook case remains explicitly missing.
- Checked authentication CCM test bodies: both providers run DDL; the custom
  provider rejects challenges. They do not cover the complete challenge/error
  matrix or failed authentication after resumed TLS.
- Checked certificate and ticket assertions in `tests/integration/ccm/tls.rs`
  and `tls_tickets.rs`: wrong-IP SAN checks assert session-build failure, only
  Rustls supplies the missing-client-credentials negative case, and ticket tests
  pin TLS 1.2/1.3 and inspect actual ticket offers/resumptions. These do not test
  older protocol slot classification. OpenSSL cache tests were inspected for
  bounded queues, reuse, per-node selection, and unusable-session fallback.
- Checked startup-option capture/counting and report schema-version assertions,
  partial/all-invalid hostname tests, sequential default-profile timeout remap
  assertions, metrics' single-page/no-retry request/error checks, and the history
  normalization/single-page/ignored-write assertions. Their documented scope
  remains accurate; no coverage of all metric interleavings is claimed.
- Checked trace helpers: pager helpers inspect initial trace IDs without fetching
  later pages; session helper and table polling use 200 attempts/50 ms rather than
  validating the driver's 10-attempt/3-ms defaults. Checked module cfgs and the
  metrics gate on the schema-key coverage sweep.
- A local read-only Python check resolved **65 named test-symbol references** and
  **80 pinned source-line references**, with none missing/out of range. This is
  structural verification, not test execution or proof of each assertion.
- All four newly documented adversarial cases remain test gaps. No compile-time
  feature matrix, real DNS timing test, older-version TLS negotiation, authentication
  recovery experiment, database run, or allocation benchmark was performed.

The corrected source contracts and explicit coverage gaps are the deliverable;
there is no claim that the reference driver has been fixed.
