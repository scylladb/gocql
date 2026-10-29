# Configuration, security, and observability — round 04

- Section: `feature-specs/configuration-security-observability.md`
- Round: 04 — concurrency, cancellation, snapshots, lifetimes, and races; the entire section was also checked.
- Agent identity: `/root/review_configuration_04`
- Source checkout: `/extra/scylladb/gocql-rust-reference-f8b0ee9`
- Source commit, confirmed with `git rev-parse HEAD`: `f8b0ee928dc0dcb475247dc82cce4e42804b0621`
- Before SHA-256: `e127cab316b0071069674e12abc6a4d0834dec59607a69d0669a1c6ef619c25a`
- After SHA-256: `7a2b2d054b3d97c4b513f4de0856176644db0722e97a5d097ed1f4be6bd26803`

Read `feature-specs/README.md`, the complete assigned section, and the review
instructions. Findings below come from the pinned implementation and test source,
not previous review reports. Only this report and the assigned specification were
edited. No Rust or Go implementation was changed, and no child agents were used.

## Source inspected

Paths below are relative to the pinned Rust checkout; line numbers refer to that
commit.

- `scylla/src/client/session.rs`: `SessionConfig` and defaults (207–527),
  validation (618–681), `Session::connect` (1407–1558), query/profile selection
  (1312–1323, 1664–1677, 2100–2112), metrics access (2360–2366), trace lookup
  (2380–2392, 2427–2481), and post-core side effects (2496–2535).
- `scylla/src/client/session_builder.rs`: builder variants, reusable `build`
  (852–854), default/compression/profile/keepalive tests (1604–1857).
- `scylla/src/client/execution_profile.rs`: defaults (173–218), immutable profile
  and shared policy fields, handle creation, acquisition and replacement
  (372–538).
- `scylla/src/cluster/node.rs`: contact expansion and `join_all` (385–455);
  `scylla/src/cluster/metadata/cc_establisher.rs`: candidate shuffle, fallback,
  initial fetch failure (183–320);
  `scylla/src/cluster/control_connection.rs`: timeout derivation (45–72).
- `scylla/src/authentication/mod.rs`: both traits and plaintext implementation
  (8–111); `scylla/src/network/connection.rs`: authentication (823–906),
  response warning/metadata handling (1459–1525), TLS backend handshake
  (1559–1597), address translation and STARTUP negotiation (2084–2294), and
  socket options (2324–2425).
- `scylla/src/network/tls.rs`: shared configuration and per-connection objects
  (36–153); `scylla/src/network/tls/openssl.rs`: bounded cache, locks,
  constructors, callback, flag and ticket selection (17–324), and unit tests
  (531–703).
- `scylla/src/policies/address_translator.rs`: trait and built-in maps;
  `scylla/src/policies/host_filter.rs`: allow-list resolution and peer acceptance;
  `scylla/src/client/client_routes.rs`: configuration, full replacement, partial
  merge and translation (24–449), plus the listed route tests.
- `scylla/src/client/self_identity.rs` and connection STARTUP identity options;
  `scylla/src/client/driver_config.rs`: normalization (486–651), captured fields
  (654–707), rendering/size limit (710–755), connection/control reports
  (758–877), and single profile acquisition (879–907).
- `scylla/src/observability/metrics.rs`: atomics, EWMA locks and tick selection
  (8–220), shared ownership (222–304), independent updates/getters and one
  histogram load per snapshot (310–545), statistics and tests (559–802);
  `metrics_noop.rs` and `observability/mod.rs`: disabled-feature behavior.
- `scylla/src/client/execution.rs`: history start and deadline placement
  (431–540), attempt metrics/history and retry decisions (560–667);
  `scylla/src/client/pager.rs`: settings capture (149–190), worker/send boundary
  (202–255), per-page core (306–384), first-page handling, and worker creation
  (827–923). Also inspected prepared/control pager worker call sites.
- `scylla/src/observability/history.rs`: listener contract, mutex snapshots/drain,
  event insertion, structured conversion and tests (26–455, 597–930);
  `driver_tracing.rs` (16–198), `tracing.rs`, `utils/safe_format.rs`, and
  `response/request_response.rs`: local fields, repeatable formatting, trace data,
  response metadata retention.
- `scylla/src/network/connection_pool.rs`: excess shard-targeted connection
  discard (1025–1078) and open-counter updates (1130–1160).
- `scylla/Cargo.toml`, `client/mod.rs`, `observability/mod.rs`,
  `policies/mod.rs`, and reconnect defaults (100–142): feature gates, public
  exports, naming and configuration defaults.

## Verified findings and fixes

1. **Bootstrap DNS has a group-completion boundary.**
   `cluster/node.rs:437–451` starts lookup futures together and awaits `join_all`
   before returning any candidates; literal addresses are held in the same
   result until then. Added that an unbounded unfinished lookup can delay startup
   even with a literal contact point. This qualifies the existing partial-DNS
   success statement without changing its error semantics.

2. **Profile snapshots extend beyond an individual attempt.**
   `execution_profile.rs:498–535` atomically acquires/stores the immutable profile
   pointer. Session query/execute/batch call sites acquire once before constructing
   execution parameters; `pager.rs:149–190` copies policy references and settings
   once for the pager. `driver_config.rs:879–907` similarly acquires once for the
   report's query section, and `connection.rs:2254–2257` sends it only at STARTUP.
   Clarified retry/speculation lifetime, whole-pager capture, independent manual
   page calls, and report replacement only on a later handshake. Shared policy
   internal state is not itself frozen by the snapshot.

3. **Authentication success is not a cancellation cleanup callback.**
   `authentication/mod.rs:12–49` allows a shared provider and mutable per-exchange
   handlers. `connection.rs:833–864` calls the provider directly, awaits each
   handler, and invokes `success` only in the AUTH_SUCCESS branch. Added that
   independent handshakes are not serialized by the driver and failed/cancelled
   exchanges can be dropped without a success callback.

4. **TLS ticket selection and disabling have distinct ownership boundaries.**
   `tls/openssl.rs:104–158` protects the bank with one mutex and removes a TLS 1.3
   ticket at selection, while cloning the reusable TLS 1.2 session.
   `:271–313` takes the ticket before handshake, with no restoration path for
   failure or cancellation. The `Clone` configuration contains a shared cache but
   an ordinary boolean (`:183–188`); `set_use_tls_tickets` changes only that value
   (`:265–267`). Existing SSL objects retain their ex-data marker and shared
   callback (`:234–248`, `:275–285`). Added concurrent single-use selection,
   cancellation consumption, and the fact that disabling a clone neither purges
   the cache nor changes other clones or existing objects.

5. **Client-route updates do not revoke translations already resolving DNS.**
   `client_routes.rs:209–296` and `:299–378` hold the write lock for each whole
   update. Translation copies the hostname/port under a read lock (`:390–425`)
   and awaits resolution after releasing it (`:427–449`).
   `connection.rs:2142–2146` uses that returned address directly. Added the
   snapshot boundary and explicit possibility of returning/connecting to an
   endpoint removed during DNS, while preserving the no-direct-fallback rule.

6. **Metric reads are not a session-wide transaction.**
   `metrics.rs:222–263` gives cloned handles shared reference-counted storage;
   request counters and meter updates happen separately (`:310–338`), and
   getters load independent fields (`:439–545`). `get_snapshot` uses one loaded
   histogram for all its statistics (`:407–437`). Added these boundaries and
   retained-handle lifetime (`session.rs:2360–2366`). This does not claim that the
   external histogram dependency supplies a linearizable whole-session snapshot.

7. **History callbacks and drains are not asynchronous completion barriers.**
   `history.rs:40–75` defines synchronous methods; execution calls them directly,
   with request-start before `timeout` and terminal history after it
   (`execution.rs:442–443`, `:513–540`). Independent requests can invoke a shared
   listener concurrently. `history.rs:140–188` clones/swaps data under the same
   mutex used for insertion, without waiting for active requests; conversion
   (`:336–452`) may omit events whose parents were drained earlier. Added callback
   placement, concurrency, and the cross-drain parent/child boundary.

8. **Pager destruction does not immediately stop page instrumentation.**
   `pager.rs:919` spawns the remaining-page worker and drops its join handle.
   The worker completes `fetch_one_page` before checking the send result
   (`:217–251`). Added that a dropped pager can still produce page metrics and
   history until this detached worker finishes the page or error path. This is
   distinct from dropping a directly awaited execution future.

9. **Trace readiness does not establish a complete, consistent trace snapshot.**
   `session.rs:2437–2440` runs two independent query futures, then checks only
   session-row presence, duration, and nonempty events (`:2442–2479`). Added that
   this predicate establishes neither one database snapshot nor arrival of all
   events. Existing readiness retries and error precedence remain unchanged.

Added pinned evidence links, corresponding missing test cases, generation notes,
and route/history lifetime invariants. These are reference-behavior clarifications,
not proposed Go divergences.

## Whole-section checks with no further changes

- Verified the configuration defaults, empty contacts/zero keepalive rejection,
  warning-only combinations, reusable builder, shared profile remapping cells,
  initial keyspace failure behavior, and initial metadata fallback.
- Verified per-connection compression fallback, socket buffer truncation,
  platform-gated keepalive settings, authentication challenge/error paths,
  connected-IP TLS identity, managed versus legacy OpenSSL behavior, cache bounds,
  and Rustls delegation. There is no plaintext fallback after TLS failure.
- Verified untranslated contact points, built-in translator failure behavior,
  host-filter address/DC semantics, client-route validation, sticky IDs, full
  versus partial refill sets, hostname override, and per-attempt resolution.
- Verified session identity/control-only reporting, size and normalization limits,
  target-specific metadata timeout reporting, recognized/custom policies, and
  feature gates. Existing report-schema limitations remain explicit.
- Verified attempts versus logical requests/pages, connection-selection exclusion,
  retry and timeout counters, success-only latency, ignored-write behavior,
  explicit PREPARE/control exclusions, post-core side-effect failures, the control
  socket exclusion and discarded-shard gauge overcount, and request-driven EWMA
  ticks.
- Verified incomplete history, poisoning fallback, local span fields and absent
  raw-result field declaration, repeatable formatting, warning/tracing-ID/custom
  payload retention, trace retries including the final sleep, deferred malformed
  events, and optional trace event fields.
- Checked the subfeatures, invariants, Rust evidence table, Go test/benchmark
  placeholders, and test-generation notes against these source boundaries.

## Test-coverage audit and validation

Tests were read, not executed. Checked that every named test target in the section
exists, then inspected relevant assertions and helper call sites. In particular:

- Builder tests check defaults/assignment and keepalive rejection; the profile
  test checks selected settings. `driver_config_tests.rs:529–589` remaps
  sequentially between two reports; it does not race report generation.
  Schema conformance (`:1420–1476`) covers selected generated reports, and report
  size (`:1745–1770`) checks below/exactly/above the limit.
- OpenSSL tests (`openssl.rs:531–703`) check local handshakes, reusable TLS 1.2,
  FIFO/single-use bounds, node eviction, and unusable-entry fallback. The TLS
  integration matrix (`tls_tickets.rs:438–541`) includes enabled/disabled managed
  OpenSSL, legacy OpenSSL, Rustls, and both TLS versions. These do not directly
  test concurrent selection, cancellation after selection, or disabling an
  already-used clone.
- Route tests check snapshot removal, partial preservation, sticky selection,
  overrides and refill-set membership. Integration helper paths exercise port
  remapping, topology changes and reroute events. They do not directly control
  a route change while a translation's DNS await is pending.
- Metrics characterization/percentile/order tests (`metrics.rs:684–802`) use
  recorded distributions, not a concurrent global-snapshot assertion. The
  integration metrics test (`session/metrics.rs:96`) distinguishes request types.
  History unit tests construct incomplete and speculative event sequences;
  integration history tests include success, failure, paging and ignored writes.
  They do not prove pager-drop quiescence or race-safe parent retention across
  independent drains; the new spec explicitly avoids those claims.
- Authentication integration uses built-in and custom providers against password
  authentication, not a full challenge/cancellation matrix. TLS identity tests
  reject the wrong IP SAN. Startup-option tests cover session ID, control-only
  report placement, and opt-out; the `system.clients` test is ignored for
  Cassandra and returns early when the column is absent.
- Execution-profile integration checks policy/consistency selection and overrides;
  tracing integration dispatches query, execute, prepare, batch and pager helpers,
  checking IDs and ordinary trace contents. Neither proves the new concurrent
  snapshot guarantees, which remain explicitly missing test cases.
- Existing query-result tests verify warning/tracing-ID retention, including
  `ResultNotRows` conversion. Existing missing-coverage entries were retained.

Document validation checked source commit, SHA-256, pinned-link source paths/line
anchors, named test symbol existence, and trailing whitespace. No database,
Rust test, Go test, race test, or benchmark run is claimed.

Remaining uncertainty: external histogram-internal snapshot linearizability was
not audited; the specification only states what the driver's own load/getter
boundaries establish. New cancellation and race cases remain unexecuted and are
listed as missing coverage. Cross-section pager/profile and route lifetime
observations were sent to the parent; no other agent's section was edited.
