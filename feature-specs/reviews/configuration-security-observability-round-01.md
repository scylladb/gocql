# Configuration, security, and observability — round 01

- Section: `configuration-security-observability`
- Round: `01` — trace claims to implementation and correct factual mismatches.
- Agent identity: `/root/review_configuration_01`
- Source checkout: `/extra/scylladb/gocql-rust-reference-f8b0ee9`
- Source commit, verified with `git rev-parse HEAD`: `f8b0ee928dc0dcb475247dc82cce4e42804b0621`
- Specification: `feature-specs/configuration-security-observability.md`
- Before SHA-256: `7d6322b4da969b45860de7dfccf10424d064fbadf339ed8d78a1ea4a3bc6cd28`
- After SHA-256: `da1466e7a293222d275d557f04b16ef9c22a699fb86dd8b75c66d02e7e4c9c24`

Read `feature-specs/README.md`, the review instructions, and the complete assigned
specification. Changed only the specification and this report. No Go or Rust
implementation changed; no subagents were used.

## Source inspected

All references below are relative to the pinned Rust checkout.

- `scylla/src/client/session.rs`: `SessionConfig`, defaults and validation
  (207–681), `Session::connect` (1407–1568), cached tracing statements
  (1571–1590), and tracing lookup/readiness (2380–2483).
- `scylla/src/client/session_builder.rs`: builder variants, configuration
  assignment, `build` (852), and configuration tests (1604–1855);
  `execution_profile.rs`: defaults (173–213), builder optional fields, and
  `ExecutionProfileHandle` sharing/remapping (494–540).
- `scylla/src/authentication/mod.rs`: provider/session traits and plaintext
  exchange; `network/connection.rs`: identity insertion (240–278), authentication
  exchange/error mapping (823–928), response warnings (1487–1521), TLS dispatch
  (1559–1610), translation (2077–2130), OPTIONS/STARTUP negotiation
  (2136–2290), and socket options (2324–2425).
- `scylla/src/network/tls.rs`: backend construction and legacy dispatch;
  `network/tls/openssl.rs`: `TicketStore`, node/ticket bounds, callbacks,
  constructors, expected IP, ticket opt-out and fallback (17–363), plus the
  named ticket tests (531–705).
- `scylla/src/policies/address_translator.rs` and `host_filter.rs`: translator
  inputs, map implementations, allow-list resolution, DC matching and bypasses.
  `client/client_routes.rs`: exported value types, full/partial route replacement,
  sticky selection and DNS translation (1–446), and cited translator/merge tests.
- `scylla/src/client/self_identity.rs`: every public identity field accessor;
  `client/driver_config.rs`: report schema/types, normalization, snapshots,
  rendering, policy recognition and node-preference reporting (43–985).
  `client/driver_config_tests.rs`: defaults, normalization, remapping, schema
  sweep and exact report-size boundary tests.
- `scylla/src/observability/metrics.rs`: request/error counters, clone ownership,
  EWMA ticks, histogram sampling/statistics and cited characterization tests;
  `metrics_noop.rs`: feature-disabled internal surface.
  `client/execution.rs`: history and metrics around deadlines, connection
  selection, attempts and retry decisions (435–676);
  `client/pager.rs`: per-page execution parameters (306–350).
- `scylla/src/network/connection_pool.rs`: admission/discard paths (1015–1080),
  successful-open and timeout accounting (1125–1180), and searched decrement
  sites. `cluster/control_connection.rs`: metadata timeout derivation (45–69)
  and absence of connection-gauge updates.
- `scylla/src/observability/history.rs`: listener, collector, poisoning/draining,
  structured grouping and cited tests; `driver_tracing.rs`: every span constructor,
  recorder and drop handler; `tracing.rs`: trace row types and node deduplication;
  `utils/safe_format.rs`: repeatable formatting.
- `scylla/Cargo.toml`, `client/mod.rs`, `observability/mod.rs`, `policies/mod.rs`,
  `utils/mod.rs`, and `lib.rs`: features, public/private exports and policy naming.
  `policies/reconnect.rs` (95–180) and `routing/sharding.rs` (20–40): default
  reconnect values and source-port validation.
- Integration sources: `tests/integration/ccm/{authenticate,tls,tls_tickets,
  client_routes}.rs`, `tests/integration/session/{self_identity,startup_options,
  new_session,metrics,history,tracing}.rs`, and
  `tests/integration/statements/execution_profiles.rs`. Paths have the `scylla/`
  prefix in the specification.

## Verified findings and fixes

1. **Positive linger did not always mean a timed lingering close.** The old
   transport paragraph included all positive durations. `connection.rs:2369–2372`
   applies the duration through the socket setter; `driver_config.rs:203–211`
   explains whole-second truncation, and
   `driver_config_tests.rs:502–518` checks 1 ms and 999 ms becoming zero. Corrected
   the runtime paragraph to include positive sub-second values in reset-on-close
   behavior and added the existing normalization test to the test inventory.

2. **TLS identity input was phrased as an unconditional verification guarantee.**
   `tls/openssl.rs:224–243` permits a raw context with verification disabled;
   `new_ssl` sets the expected IP but does not enable verification. Rustls receives
   the configured client and an IP `ServerName` (`connection.rs:1583–1590`).
   Clarified that the driver supplies the IP as expected identity and that
   certificate rejection depends on the supplied backend configuration. The
   no-plaintext-downgrade rule remains unchanged.

3. **Client-route stickiness needed its metadata boundary stated explicitly.**
   `client_routes.rs:204–379` replaces a sticky ID through route updates;
   `translate_address` at 382–446 reads only that sticky route and propagates
   missing-port/DNS errors. It never tries another ID upon such a failure.
   Clarified that remaining routes are replacements when metadata removes the
   sticky route, and that DNS or connection health does not rotate IDs.

4. **Custom load-balancing policy reporting cannot describe its own preference.**
   `driver_config.rs:906–931` recognizes `DefaultPolicy` before reading policy
   preferences and otherwise falls back to the session preference. Narrowed the
   reporting claim and explicitly stated that a custom policy's routing behavior
   is not inspected.

5. **Deadline accounting and history boundaries were incomplete.**
   `execution.rs:513–537` increments only the request-timeout counter and records
   a request error when the enclosing deadline expires. Attempt error and latency
   accounting happen only after the attempt future returns (595–645). External
   cancellation can bypass request-terminal logging entirely. `pager.rs:306–350`
   invokes the request engine per page. Added these distinctions, including
   incomplete cancelled attempts and separate per-page request histories; added
   an explicit cancellation-test gap.

6. **The pool-connection gauge has an additional overcount path.** The connections
   reviewer flagged this through the parent; I independently inspected the cited
   branches. `connection_pool.rs:1135–1140` increments every successful pool open.
   At 1050–1062, a shard-targeted open landing on a full shard is discarded and an
   ordinary replacement started, without decrementing the discarded connection.
   Added this limitation beside the existing control-socket exclusion and marked
   regression coverage missing. The spec does not promise an exact live-socket
   census.

7. **Trace decoding errors are not all discovered before readiness retries.**
   `session.rs:2431–2463` executes both queries but returns `None` for an absent
   session or absent duration before decoding events at 2466–2478. The previous
   unqualified immediate-deserialization-error claim was too broad. Clarified
   that encountered errors propagate, while event-row decoding is deferred until
   session readiness; added the missing combined malformed-events/incomplete-
   session case to test expectations.

8. **Client-route type exports were conflated with feature-gated configuration.**
   `client/mod.rs:26` publicly exports `client_routes` unconditionally, and that
   module's `ClientRoutesConfig`/`ClientRoutesProxy` definitions are ungated.
   The `SessionConfig` field (`session.rs:389–390`) and specialized builder
   (`session_builder.rs:110`) are feature-gated. Corrected the feature-availability
   paragraph to make those surfaces distinct.

9. **A database metadata test has explicit skip conditions.**
   `startup_options.rs:257–294` ignores the test for Cassandra and returns early
   when `system.clients.client_options` is absent. Added both conditions to its
   test entry so a nominal passing test is not mistaken for exercised behavior.

10. **Required invariant structure was missing.** Split the combined
    `Subfeatures and invariants` organization into `## Subfeatures` and canonical
    `## Invariants`, preserving all invariant content. Added pinned evidence for
    newly described observability boundaries.

## Behaviors checked without additional changes

- All listed session defaults, validation-before-UUID ordering, initial keyspace
  failure, builder reuse, and shared execution-profile remapping agree with source.
- Authentication starts only after AUTHENTICATE, distinguishes null/empty tokens,
  uses connection-specific exchange state, handles all challenge/success errors,
  and gives plaintext credentials no implicit transport encryption.
- Compression absence falls back only on that connection. TLS IP selection,
  legacy behavior, shared ticket caches, FIFO single use, TLS 1.2 reuse, 8/256
  limits, store-age eviction and full-handshake fallback match implementation.
- Contact-point translation bypass, strict map lookup, allow-list behavior,
  client-route configuration rejection, snapshot versus partial merge semantics,
  hostname overrides and randomized per-attempt DNS selection match implementation.
- Session identity/control-only reporting, exact 32 KiB acceptance, profile
  re-reading, recognized-policy rendering, zero/submillisecond normalization,
  omitted credentials and schema limitations were checked.
- Metrics remain optional; attempt/page/retry accounting, successful-only latency,
  histogram approximations and record-driven EWMA ticking match source.
- History draining preserves IDs, poisoning uses temporary storage, missing parents
  can disappear from structured history, and the documented late-event contract
  is retained. Span formatting, undeclared raw-size field, warning logging,
  tracing readiness, final unsuccessful sleep and node deduplication were checked.
- Optional feature groups and unstable compiler gates were checked against actual
  declarations/exports. The Go test and benchmark entries remain future gates,
  not claims of implemented or passing Go behavior.

## Test claims, verification, and remaining gaps

Read the named test sources and relevant assertions; no Rust, Go, database, or
benchmark suite was executed. Ticket unit tests perform real local OpenSSL
handshakes with verification disabled for their cache-focused setup, so they do
not establish certificate-validation coverage. Certificate/IP-SAN/client-auth
coverage instead comes from the CCM TLS tests. The authentication integration
provider does not exercise challenges; the full null/empty/error matrix remains
missing. Profile integration tests check selected statement/session profiles;
the report unit test separately exercises remapping. History unit tests construct
events directly, whereas the paging integration test checks at least four page
histories. Trace integration tests establish happy-path tracing and table presence,
not the full readiness/error matrix.

A local document check validated all 43 pinned source links (commit, path, and
line bounds), all 49 named Rust test targets, the canonical invariant heading,
and absence of trailing whitespace. This was static verification, not test
execution. Existing explicit gaps remain for full authentication error coverage,
direct client-route invalid combinations, idle-meter/control-gauge coverage,
span-field completeness, and compression isolation; the newly documented gauge,
cancellation and trace-ordering edge cases also remain untested here. No
cross-section file was edited.
