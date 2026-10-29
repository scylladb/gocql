# Configuration, security, and observability — round 03

- Section: `feature-specs/configuration-security-observability.md`
- Round: 03 — errors, partial success, failover, retry, and recovery; the whole section was reviewed.
- Agent identity: `/root/review_configuration_03`
- Source checkout: `/extra/scylladb/gocql-rust-reference-f8b0ee9`
- Source commit: `f8b0ee928dc0dcb475247dc82cce4e42804b0621` (verified with `git rev-parse HEAD`).
- Before SHA-256: `1fc4a25b43b807f60aaaaadcebc53f96fb5c101452889c5cc08d65b1c743ec4c`
- After SHA-256: `e127cab316b0071069674e12abc6a4d0834dec59607a69d0669a1c6ef619c25a`
- Owned edits: this report and the assigned specification only. No Go or Rust implementation changes; no child agents.

Read `reviews/REVIEW-INSTRUCTIONS.md`, `feature-specs/README.md`, and the complete
assigned specification. Findings below come from the pinned implementation and
test bodies, not from previous reports. Source paths below are relative to the
Rust checkout.

## Verified findings and fixes

### 1. Startup needed an explicit partial-success boundary

The startup description did not explain that failed DNS names and connection
candidates can be skipped, or that successfully constructing a session can follow
a failed initial metadata fetch.

Evidence:

- `scylla/src/cluster/node.rs:385–455`: `resolve_contact_points_inner` expands all
  resolved addresses, logs and drops individual hostname failures, and deduplicates
  the aggregate by socket address.
- `scylla/src/cluster/metadata/cc_establisher.rs:118–125,183–211,281–323`:
  construction fails if no address remains; candidates are shuffled; handshake
  errors advance to another candidate; an initial metadata-fetch error returns
  retained rejected-node metadata or dummy metadata with no control connection.
- `scylla/src/cluster/worker.rs:131–159`: cluster creation accepts that metadata
  and waits for initialization of pools, not a guarantee that all pools work.
- `scylla/src/cluster/metadata/worker.rs:640–675`: absent control connections enter
  the background establishment/repair loop.

Fix: added the partial DNS, candidate-order, handshake-failover, dummy-metadata,
and repair behavior beside startup. Explicitly distinguished successful session
creation from complete metadata and usable pools, while retaining the links to
the sections owning the detailed algorithms.

### 2. Metrics and history did not fully identify the execution-core boundary

The spec correctly distinguished attempts from requests, but could still be read
as describing every frame or the final public API outcome. The core can report
success before later processing fails, and ignored write errors are a different
success path with failed-attempt diagnostics.

Evidence:

- `scylla/src/client/execution.rs:426–439,513–537,560–681`:
  history begins inside `run_request_no_side_effects`; counts start after
  connection acquisition, before the per-attempt closure can prepare/serialize
  or send its final frame; successful attempts record latency; attempt errors are
  counted before consulting the retry policy; `IgnoreWriteError` returns `Ok`,
  causing request-success history without attempt-success latency.
- `scylla/src/client/session.rs:906–913,1924–2019`: prepared serialization can
  fail before execution begins, and explicit PREPARE directly calls connection
  preparation without entering the request core.
- `scylla/src/network/connection.rs:1177–1220`: reprepare and the following
  EXECUTE occur within one attempt and do not separately invoke core accounting.
- `scylla/src/client/pager.rs:553–569`: control-connection execution sets both
  the metric sink and history listener to `None`.
- `scylla/src/client/session.rs:2427–2442`: tracing-table queries use ordinary
  session unpaged execution and therefore do contribute to its counters.
- `scylla/src/client/session.rs:2496–2535`: successful core execution/history
  precedes keyspace propagation and automatic schema-agreement handling, whose
  errors propagate from the outer method.
- `scylla/src/client/pager.rs:270–297,464–502` and
  `scylla/src/response/request_response.rs:63–90`: page/result validation can
  reject a successful core response after its history and latency were recorded.
- `scylla/tests/integration/session/history.rs:433–647`:
  `pager_ignore_write_error_history` checks a failed attempt with an ignore
  decision and successful request history for first-page and later-page cases.

Fix: documented instrumentation exclusions, internal trace-query inclusion,
attempts that fail before their final frame is sent, ignored-error accounting,
and history success preceding public-call completion.
Added an invariant, cited the execution boundaries, added the existing ignored
error history test, and retained explicit gaps for its metric counterpart and
errors before/after core execution.

### 3. Warning, tracing-ID, and custom-payload retention was overstated

The sentence saying warnings, tracing IDs, and custom payloads are retained did
not distinguish internal responses from public query results, errors, or pagers.

Evidence:

- `scylla/src/network/connection.rs:1487–1524`: warning logging occurs after
  extension parsing but before response-body deserialization. The internal
  `QueryResponse` receives the extension metadata only if parsing succeeds.
- `scylla/src/response/request_response.rs:16–40`: only the internal response
  has `custom_payload`. Non-error conversion omits it; converting an error exits
  through `into_non_error_response()?` before retaining warning or tracing fields.
- `scylla/src/network/connection.rs:2045–2068`: tablet processing can consume the
  internal custom payload before that conversion.
- `scylla/src/response/query_result.rs:76–94,152–159,282–289`: ordinary query
  results expose warnings and tracing IDs.
- `scylla/src/client/pager.rs:64–73,107–115,375–456,464–481,1255–1256`: page
  structures retain tracing IDs but no warning field; the pager exposes its IDs.
- `scylla/src/response/query_result.rs:530–587,800–833`: unit tests cover access
  to warning/tracing metadata and retaining the original non-row query result
  inside a failed rows conversion. This is distinct from a server-error response.

Fix: stated each retention boundary and logging order, removed the implication
of a public custom-payload facility, and added precise test references/gaps.

## Whole-section checks and inspected source

- **Configuration and ownership:** `session.rs:207–680,1407–1590`,
  `session_builder.rs:95–154,852–854`, `execution_profile.rs:173–217,494–540`,
  `routing/sharding.rs:13–35`, and `policies/reconnect.rs:110–175`. Checked the
  default table, validation before UUID/connections, nonzero boundaries, warnings
  rather than rejection for schema/timeout combinations, cloned builder state,
  shared profile remapping, per-handle independence, and initial-keyspace failure.
- **Transport/authentication:** `authentication/mod.rs:8–106`,
  `network/connection.rs:823–928,1559–1609,2073–2430`. Checked null versus empty
  tokens, provider/start/challenge/success errors, missing provider, READY bypass,
  no challenge limit, plaintext-provider bytes and challenge rejection, per-socket
  compression fallback, socket option errors, buffer casts, TCP keepalive platform
  branches, and absence of TLS-to-plaintext fallback.
- **TLS and resumption:** `network/tls.rs:34–166` and
  `network/tls/openssl.rs:17–353`. Checked translated-IP identity, constructor
  verification differences, legacy context limitations, shared cache/IP keys,
  TLS 1.3 FIFO single use, reusable TLS 1.2 entries, bounds and eviction,
  mixed-version preference, one selected ticket, disabled-ticket behavior,
  serialization/decode/offer fallback, and rustls-native resumption.
- **Address policy/client routes:** `policies/address_translator.rs:103–176`,
  `policies/host_filter.rs:15–108`, `client/client_routes.rs:24–450`, and the
  establisher paths above. Checked missing-map-rule errors, malformed matching
  values, allow-list resolution/advertised-address matching, untranslatable
  bypass, DC matching, validation gates, sticky IDs, snapshot/partial deletion
  differences, refill sets, per-attempt DNS, overrides, missing-port errors,
  and no translator fallback to direct access.
- **Identity/reporting:** `client/self_identity.rs`,
  `network/connection.rs:240–272,2210–2288`,
  `client/driver_config.rs:486–1010`, and `cluster/control_connection.rs:45–70`.
  Checked session/client ID separation, report opt-out/control-only placement,
  profile re-read per report, serialization/size fallback including equality at
  32 KiB, duration normalization, buffer truncation/schema limitation, omitted
  settings, recognized/custom policies, and target-dependent metadata timeout.
- **Metrics/history/local traces:** `observability/metrics.rs:50–580`,
  `metrics_noop.rs`, `history.rs:26–260` plus structured-history/test portions,
  `driver_tracing.rs:21–211`, `utils/safe_format.rs`, and
  `network/connection_pool.rs:1030–1195`. Checked histogram approximations and
  percentile scale, idle EWMA behavior, separate paging counters, timeout
  cancellation, connection-selection exclusion, pool/control gauge distinction,
  the existing discarded-connection overcount, history draining and poisoning,
  partial records, span fields, query/key visibility, and repeatable formatting.
- **Database tracing:** `observability/tracing.rs:17–120` and
  `session.rs:2378–2482`. Checked concurrent table queries, immediate query errors,
  readiness/duration/event checks, final unsuccessful sleep, delayed event parsing,
  nonzero attempt count, nullable fields, node deduplication, and statement settings.
- **Feature/export claims:** `scylla/Cargo.toml:35–100`, `client/mod.rs`,
  `observability/mod.rs`, `policies/mod.rs`, `lib.rs:280–320`, and `utils/mod.rs`.
  Checked no default optional features, TLS/metrics gates, external-type adapter
  groups, serde separation, unstable gates, and public client-routes value types.
  Optional chrono adapters do not imply that chrono ceases to be a dependency.

## Test coverage and remaining gaps

Read the named builder/report tests, including schema-validation negative
controls, profile remapping, exact report-size boundaries, zero/submillisecond
normalization, per-host shard reporting, and subsecond linger. The linger test
asserts the report value; its socket effect is source evidence, not a socket test.

Read the six named OpenSSL ticket tests, the named client-route translation,
snapshot, partial-update and refill tests, and the histogram/history test bodies.
Ticket tests exercise actual in-process handshakes but disable certificate
verification; certificate validation evidence comes from the CCM TLS tests.
The custom-authentication CCM test implements plaintext exchange and does not
cover a successful multi-challenge protocol.

Read authentication/TLS integration assertions, STARTUP identity/report capture and
opt-out assertions, the `system.clients` capability skip, partial DNS tests,
execution-profile test setup/assertion helpers, client-route topology/remap/reroute
scenarios, separate paging-counter assertions, successful/failed/paged/ignored
error history assertions, and tracing test dispatch/result assertions. The metrics
integration test deliberately uses a fallthrough retry policy and does not prove
retry, ignored-error, or post-core accounting. Existing missing-coverage entries
remain explicit, with the new gaps identified above.

No Rust tests, Go tests, or database/proxy scenarios were executed in this round.
Verification was source inspection plus local documentation checks. No claim is
made that every edge case marked missing has no test anywhere; these are gaps in
identified supporting coverage. No additional cross-section edit was made.

Final documentation validation checked 49 pinned source links for revision, file,
and line existence; 56 named test references for function existence; both edited
files for trailing whitespace; and the final specification hash against this
report. All checks passed. These checks do not establish runtime test results.
