# Configuration, security, and observability — round 05

- Section: `feature-specs/configuration-security-observability.md`
- Round: 05 — test assertions, guards, expectations, and coverage; the entire section was checked.
- Agent identity: `/root/review_configuration_05`
- Source checkout: `/extra/scylladb/gocql-rust-reference-f8b0ee9`
- Source commit, independently confirmed with `git rev-parse HEAD`: `f8b0ee928dc0dcb475247dc82cce4e42804b0621`
- Before SHA-256: `7a2b2d054b3d97c4b513f4de0856176644db0722e97a5d097ed1f4be6bd26803`
- After SHA-256: `90977ea08747fc7e804f36877330e88280cb483615ad4b4bdb29ab9d67b1e63f`

Read the review instructions, `feature-specs/README.md`, and the complete assigned
section. The round 04 report supplied context only; the evidence below was read
from the pinned implementation and tests. Edited only the assigned section and
this report. No child agents were spawned, and no Go or Rust implementation was
changed.

## Source inspected

Paths and line numbers below refer to the pinned Rust checkout.

- `scylla/src/client/session.rs`: configuration/defaults (207–527), validation
  (618–681), startup and initial keyspace (1407–1569), ordinary profile acquisition
  (1664–1677), tracing lookup (2380–2481), and execution side effects (2496–2531).
  `session_builder.rs`: reusable `build` (852–854), defaults, assignments,
  zero-keepalive rejection, and profile tests (1604–1857).
- `scylla/src/client/execution_profile.rs`: defaults (174–212), profile/handle
  operations and shared remapping (429–527). `cluster/node.rs`: contact-point
  expansion, concurrent lookup group, and deduplication (385–455).
  `cluster/metadata/cc_establisher.rs`: candidate shuffle and initial metadata
  fallback (183–320). `cluster/control_connection.rs`: timeout derivation
  (45–67). `routing/sharding.rs`: source-port bounds; `policies/reconnect.rs`:
  defaults and configuration bounds.
- `scylla/src/authentication/mod.rs`: provider/exchange traits and plaintext
  implementation. `network/connection.rs`: identity options (240–277),
  authentication (823–910), warning parsing (1487–1515), TLS handshakes
  (1559–1605), compression/STARTUP (2136–2295), and socket options
  (2324–2425). `network/tls.rs`: backend dispatch/gates;
  `network/tls/openssl.rs`: cache bounds, selection, callbacks and constructors
  (17–324), plus all six cited tests (531–703).
- `scylla/src/policies/address_translator.rs`: map implementations (116–166);
  `policies/host_filter.rs`: resolution and acceptance (15–98).
  `client/client_routes.rs`: configuration, snapshot/partial update and
  translation (35–449), no-route and override tests (517–529, 719–741),
  replacement/merge tests (959–1081), refill-set tests (1691–1717,
  1919–2051), and nonsticky replacement test (2131–2195).
- `scylla/src/client/driver_config.rs`: schema/report types (43–150), normalization
  (486–651), rendering (725–755), connection/control/query reporting (758–907).
  `driver-config-schema.json`: empty heartbeat/coalescing placeholders.
  `driver_config_tests.rs`: default/PerHost reports (215–234), duration and linger
  normalization (485–517), sequential remapping (529–588), schema conformance
  (1420–1475), metrics-gated omissions audit (1484–1740), and size boundaries
  (1745–1769).
- `scylla/src/observability/metrics.rs`: atomics, EWMA ticking, ownership,
  histogram snapshots and statistics, and all cited histogram tests (1–802);
  `metrics_noop.rs` and `observability/mod.rs`: disabled-feature behavior.
  `client/execution.rs`: history/deadline boundary and per-attempt instrumentation
  (431–667); `client/pager.rs`: profile capture, worker closure checks, page
  execution, control-request exclusion, and task spawn (149–384, 553–606,
  890–923). `network/connection_pool.rs`: excess shard-targeted discard and
  metric increments (1025–1078, 1131–1160).
- `scylla/src/observability/history.rs`: listener, collector/drain and conversion
  (26–212, 334–453), display normalization and cited unit tests (511–890).
  `driver_tracing.rs`, `tracing.rs`, `utils/safe_format.rs`, and
  `response/request_response.rs`: spans, trace data, repeatable formatting,
  and response metadata conversion. `response/query_result.rs`: warning/ID
  tests and retention through `ResultNotRows` (530–585, 800–832).
- `scylla/Cargo.toml`, `client/mod.rs`, and `observability/mod.rs`: optional
  features and public exports. `scylla-ccm-bridge/Cargo.toml`: test-dependency
  feature propagation; `scylla-ccm-bridge/src/lib.rs:199–235`: cluster setup.
- Integration tests: complete `ccm/authenticate.rs`, `ccm/tls.rs`,
  `session/startup_options.rs`, `session/self_identity.rs`, `session/tracing.rs`,
  `session/new_session.rs`, `statements/execution_profiles.rs`, and
  `statements/request_timeout.rs`; `ccm/tls_tickets.rs` backend/version matrix,
  resumption threshold and assertions (70–175, 273–541); `ccm/client_routes.rs`
  query-feedback helper, topology/remap tests and reroute phases (80–214,
  340–385, 520–667); `session/metrics.rs` six-counter snapshot and all test
  paths; `session/history.rs` normalization, success/failure, pagination,
  single-page and ignored-error tests (30–646). Also read `ccm/mod.rs`,
  `session/mod.rs`, `main.rs`, and `utils.rs:160–205,355–369` for guards/setup.
  Searched other integration metric getters and tracing-ID assertions; inspected
  the single-result tracing assertion in `session/pager.rs:244–272`.

## Verified findings and fixes

1. **The tracing test does not cover the default readiness budget or later-page
   ID accumulation.** `session/tracing.rs:15–35` creates a table and invokes the
   pager helpers before the batch inserts a row. The helpers at 164–217 inspect
   the eager first page's IDs without consuming further pages. Most API helpers
   only establish that a matching `system_traces.sessions` row exists;
   `test_get_tracing_info` (115–161) checks the richer session/event fields for
   one ordinary query. `utils.rs:355–369` changes readiness from the driver
   default to 200 attempts/50 ms, and the separate polling helper at
   `tracing.rs:241–269` uses that same longer budget. Narrowed the inventory and
   explicitly marked default-budget, incomplete/exhaustion, and later-page
   accumulation coverage missing.

2. **The profile integration reference attributed the wrong kind of switching
   to its test.** `statements/execution_profiles.rs:218–323` assigns and removes
   statement-specific handles; it checks load-balancing/retry callbacks and
   consistency overrides without invoking `map_to_another_profile`. The actual
   sequential remapping test is `statements/request_timeout.rs:86–138`, which
   remaps a clone of the session-default handle, observes subsequent timeout
   errors, and checks long statement overrides for query, execute, and batch.
   Split these coverage claims and added the correct test. Existing concurrent
   request/pager/report snapshot gaps remain explicit. The reporter unit test
   also remaps sequentially between reports (`driver_config_tests.rs:529–588`).

3. **The metrics integration reference needed its single-page/no-retry scope.**
   `session/metrics.rs:96–284` queries one `system.local` row and uses
   `FallthroughRetryPolicy`. Its helper compares all six request/error counters
   exactly after successful or forged-error query/execute calls. It does not
   assert counts across later pages, retries, speculative attempts, selection
   failures, or timeout/latency outcomes. Made the existing claim precise and
   added that combined instrumentation gap. Unit histogram references now state
   their actual fixed-distribution, percentile endpoint, empty-error, and seeded
   ordering assertions (`metrics.rs:684–802`); exact large-bucket expectations,
   invalid-percentile behavior, and out-of-range recording remain missing.

4. **Test guards and security matrices were unstated.** `ccm/mod.rs:8–13` requires
   both TLS features even for the shared TLS tests; `session/mod.rs:8–9` gates
   metrics; client routes has its own module gate. The CCM bridge dev-dependency
   enables the client-route feature through feature unification. Added these
   build requirements and cluster/proxy setup requirements. Narrowed the TLS
   identity claim to the asserted generic session-build error for a wrong IP
   SAN (`tls.rs:249–293`). Both OpenSSL APIs and Rustls exercise successful
   client authentication, but only Rustls exercises missing client credentials
   (`tls.rs:318–337`). Untrusted-CA and OpenSSL missing-credential cases with
   precise error causes are explicit gaps. Authentication uses password login
   even in the custom-provider test; that provider rejects challenges
   (`authenticate.rs:69–100`), so it cannot prove challenge-loop coverage.

5. **The ticket integration expectation was underspecified.**
   `tls_tickets.rs:145–149,320–417,491–519` asserts exact total connections,
   the pinned TLS version, a full first handshake per node, ticket presentation
   for resumptions, and version-specific minima. For managed OpenSSL and Rustls,
   TLS 1.2 resumes every connection after the first per node; TLS 1.3 resumes at
   least two. Disabled managed OpenSSL and legacy OpenSSL offer/resume none.
   Expanded the inventory to preserve these assertions, including server-ticket
   enablement. These test expectations do not promise that all TLS 1.3 pool
   connections resume in production.

6. **STARTUP coverage counted frames without identifying the control socket.**
   `startup_options.rs:41–105,160–243` waits for the expected initial count,
   validates UUIDs, and asserts exactly one bounded version-1 report. It does
   not associate that report with an independently classified control socket or
   force a control reconnection. `self_identity.rs:62–94` checks one captured
   STARTUP for each identity configuration. Made those scopes explicit and
   added the control-reconnection/session-ID/report-regeneration gap. The
   production control-only rule remains supported by `session.rs:1480` and
   the control-connection setup, not inferred solely from the test name.

7. **Several unit and history descriptions mixed source behavior with asserted
   observations, and one relevant regression test was omitted.** Builder tests
   inspect selected configuration fields; the keepalive test asserts the error
   variant/message. The linger unit test only checks JSON values, while actual
   socket application is production evidence. History unit tests insert events
   manually, and integration display helpers normalize timestamp/address/error
   text. Clarified these boundaries. Added `pager_single_page_history`
   (`session/history.rs:344–411`), which checks exactly one successful request
   and attempt for query/execute pagers, with no extra unfinished request. The
   existing ignored-error test checks terminal history, not latency/counter
   accounting; those separate gaps were retained.

## Whole-section checks with no behavioral corrections

- Defaults, validation order, reusable configuration cloning, profile handle
  indirection, bootstrap DNS group completion, and initial metadata fallback
  remain consistent with source. Initial keyspace failure still fails startup.
- Compression fallback is per connection; socket options precede bind/connect;
  buffer truncation, keepalive OS gates, and linger/report normalization remain
  distinguished. The schema's heartbeat/coalescing objects are empty placeholders
  and cannot express runtime detail; no report completeness guarantee was added.
- Authentication remains per-connection state with a shared provider, null/empty
  tokens and distinct phase errors. TLS uses the connected IP and has no plaintext
  retry after failure. OpenSSL cache bounds, FIFO/single-use TLS 1.3 versus reusable
  TLS 1.2, clone-local disabling, and selection-before-handshake remain accurate.
- Host filters and translators have different roles. Client-route validation,
  sticky selection, missing-route/port failures, snapshot/partial refill sets,
  overrides, and releasing the read lock before DNS match source.
- Identity, control-only reporting, report size/normalization, custom-policy
  naming, report profile acquisition, and feature availability remain accurate.
- Request counters record attempts after connection selection; retries and
  deadlines have their stated distinct counters. Explicit PREPARE/control paths,
  ignored errors, and post-core failures retain their instrumentation boundaries.
  The active-connection gauge exclusion/overcount and request-driven EWMA ticks
  remain explicit source limitations.
- Metrics lack a global atomic snapshot. Collector drains do not stop requests,
  detached paging can finish after drop, incomplete history is representable,
  and poisoned collection uses dummy storage. Local span field limitations,
  query/warning/custom-payload exposure, trace readiness predicates, final
  incomplete-attempt sleep, and deferred event parsing remain as documented.
- Checked invariants, subfeatures, pinned evidence, Go test/benchmark placeholders,
  and generation notes against these boundaries. No Go implementation or test
  result was inferred from the Rust tests.

## Validation and remaining gaps

No Rust test, database test, Go test, race test, or benchmark was executed. This
round inspected source assertions and guards. A document validation script
confirmed all 58 named Rust test targets exist, all 59 pinned source links name
existing files and in-range line anchors at the correct revision, and the spec
has no trailing whitespace. This validates references, not semantic test results.

The earlier explicit race, cancellation, token/error-matrix, route-update/DNS,
report-schema, and observability gaps remain. The new entries distinguish the
observed test scopes from untested paths; successful execution in the required
database/feature matrix is still unverified. Shared profile and tracing coverage
observations were sent to the parent for cross-section consistency; no other
section was edited.
