# Metadata, control connection, and events — round 08

- Section: `feature-specs/metadata-control-events.md`
- Round: 08 — whole-section audit, with caller/callee and cross-section focus.
- Agent identity: `/root/review_metadata_08`
- Source checkout: `/extra/scylladb/gocql-rust-reference-f8b0ee9`
- Source commit verified with `git rev-parse HEAD`: `f8b0ee928dc0dcb475247dc82cce4e42804b0621`
- Before SHA-256: `3d65a36573f9f70c9716eb93f75276b09f664f4a179393057088a8686a2f4b78`
- After SHA-256: `4035a8cc990094bdae0803f0f927c0e7db83b6bafdf393d1d260c8b38c8e0904`

Read `REVIEW-INSTRUCTIONS.md`, `feature-specs/README.md`, and the complete assigned
specification. Independently inspected the implementation; prior reports were not
used as proof. Edited only the assigned specification and this report. No Go or
Rust implementation was edited, and no child agent was created.

## Findings and fixes

1. **Metadata execution settings were incompletely separated from ordinary
   execution profiles.** `Session::connect` supplies
   `ConnectionConfig::default_consistency = Default::default()`
   (`scylla/src/client/session.rs:1460–1467`), whose value is LOCAL_QUORUM
   (`scylla-cql-core/src/frame/types.rs:17–41`). Control statements set page size
   and idempotence but no consistency/profile (`cluster/control_connection.rs:175–223`).
   `Connection::execute_iter` takes consistency from that connection default and
   flattens the unset serial consistency (`network/connection.rs:1231–1249`);
   `SingleConnectionPagingExecutor::fetch_one_page` installs its own fallthrough
   policy and dedicated timeout (`client/pager.rs:553–600`). Added the fixed
   LOCAL_QUORUM/no-serial-consistency behavior and its independence from default
   execution-profile selection/remapping to the language-independent contract.
   Added pinned evidence and a coverage gap.

2. **Agreement's connection and request-policy boundary was implicit.**
   `check_schema_agreement_with_required_node` obtains working connections from
   `ClusterState::iter_working_connections_per_node`, which iterates the complete
   known-node map rather than the token ring (`client/session.rs:2706–2724`,
   `cluster/state.rs:748–790`). Its `read_node_schema_version` and
   `fetch_connection_schema_version` calls send raw unpaged QUERYs through those
   node-pool connections (`client/session.rs:2785–2832`,
   `network/connection.rs:929–991`). The schema statement explicitly uses ONE
   (`client/session.rs:1593–1602`). Added that these probes include working
   zero-token nodes, use ordinary pools, and bypass control metadata timeouts and
   session execution-profile load balancing/retry/speculation/request deadlines.
   The already documented per-node fallback and outer polling remain the recovery
   mechanisms. Added evidence and test-generation cases.

3. **Automatic agreement/refresh omitted their outer deadline and enablement
   boundaries.** `Session::run_request` awaits the core request before invoking
   side effects (`client/session.rs:2513–2529`). The automatic pager does the
   equivalent processing after its first schema-change response
   (`client/pager.rs:1222–1242`). `handle_auto_await_schema_agreement` gates both
   agreement and refresh on automatic waiting, and performs refresh only after
   the separately timed agreement wait has returned (`client/session.rs:1822–1839`,
   `2639–2694`). Added that both steps lie outside the original statement
   deadline, refresh lies outside the completed agreement deadline, and disabling
   automatic waiting also bypasses refresh even when the refresh flag stays set.
   Linked the corresponding execution subsection and marked the direct deadline
   and disabled-wait regressions as missing.

4. **Empty client-route filter semantics needed a public/internal qualification.**
   Empty full-fetch ID lists select the unrestricted query branch
   (`cluster/metadata/fetching.rs:562–589`), whereas partial fetches require literal
   membership and return without querying when no IDs survive (`604–650`).
   `ClientRoutesConfig::new` rejects an empty proxy list, its field is private,
   and `ClientRoutesSubscriber` is crate-private (`client/client_routes.rs:72–101`).
   The translator derives its IDs from that nonempty configuration (`171–184`).
   Qualified the existing empty-filter description as internal behavior not
   selectable through public client-routes configuration. This now agrees
   explicitly with the configuration section.

## Source and boundary inspection

Paths beginning `cluster/`, `client/`, `network/`, or `routing/` in this report
are relative to `scylla/src/` in the pinned checkout. Other Rust paths are relative
to the checkout root.

- `cluster/node.rs:222–313,346–466`: enablement/connectivity, pool operations,
  host-ID equality/hash, DNS fallback, concurrent expansion, and deduplication.
- `cluster/metadata/cc_establisher.rs:102–496`: construction, known-peer and
  contact-point passes, rejected metadata/dummy fallback, topology guard,
  host-filter limitation, and event registration.
- `cluster/control_connection.rs:25–260`: prepared cache, page size, timeout
  derivation/rendering, per-use clone, and shutdown event classification.
- `cluster/metadata/mod.rs:40–224,452–483`: fetch modes, metadata error scope,
  optional consistency field, endpoints, and dummy metadata.
- `cluster/metadata/fetching.rs:171–423,550–1019,1027–1119,1391–1740,1769–2010`:
  full/partial composition, tolerant peer reads, route pair filtering, schema
  fetch levels, UDT resolution/sorting, primary-key validation, type parser,
  Scylla compatibility fallbacks, and strategy parsing.
- `cluster/metadata/worker.rs:156–282,349–436,535–1090`: fetch-plan precedence,
  tick advancement, full preemption, establishment event draining, repair,
  explicit-refresh acceptance/replies, status/schema/route dispatch, retries.
- `cluster/metadata/update.rs:72–180,294–412` and
  `cluster/metadata/merge_channel.rs:102–182`: pending full/schema/route merge,
  preserved refresh replies, absent-versus-error behavior, cancellation/drop.
- `cluster/state.rs:254–790,846–898`: topology/schema sharing, node/pool reuse,
  keyspace fallback, locator rebuild, inspection, agreement pool enumeration,
  tablet duplicate detection; `routing/locator/tablets.rs:801–847,986–1050`:
  unknown-replica recovery, stale-node replacement, and table/view maintenance.
- `cluster/worker.rs:81–215,303–801`: startup route application, pool wait,
  metadata/tablet/connectivity work, hints, refresh publication, and listener
  bookkeeping/ordering. `network/connection_pool.rs:285–306,753–796` confirms
  the hint-to-keepalive/refill boundary.
- `client/session.rs:469–510,1460–1500,1593–1602,1822–1839,2513–2529,2549–2865`,
  `client/pager.rs:553–600,647–683,1117–1182,1222–1242`, and
  `network/connection.rs:763–805,929–991,1231–1249`: defaults and the execution,
  agreement, timeout, and retained-pager boundaries above.
- `scylla-cql/src/frame/response/event.rs:171–325`: event decoding and invalid
  schema-change-kind fallback.
- Neighboring specifications read at the corresponding boundaries:
  `connections.md` (pool/liveness, state publication, keyspace propagation),
  `routing.md` (snapshot ownership, minimal metadata, inspection, tablet
  maintenance), `execution.md` (profile resolution, internal paging, response
  side effects), and `configuration-security-observability.md` (defaults,
  host filtering, route construction and subscriber application).

## Behaviors checked without further findings

- Defaults, DNS fallback/error preservation, shuffled sequential candidates,
  dummy startup versus failed opening, re-resolution only during recovery,
  rejected-candidate metadata, and the bootstrap/established host-filter limits.
- Peer/query-stream tolerance, aggregate nonempty-token validation, advertised
  addresses, cluster-name replacement, zero-token retention, schema scope,
  unknown strategies, recoverable keyspace fallback, missing keyspace removal,
  whole-fetch failures, and Scylla-table compatibility handling.
- Independent partial slots, schema tick batching, event retention across an
  in-flight full fetch, refresh acceptance after previous full work, partial
  preemption without a network-cancellation guarantee, one-second repair cadence,
  pending refresh preservation, and success only after state publication.
- Merge precedence and drop/error coalescing, immutable state with mutable pools,
  address-only pool reuse, routes-before-pools, old-state DOWN/new-state UP,
  token-ring-only initialization waiting, and full versus unchanged partial
  publication.
- Optimistic listener Up, callback timing, removed/address/added ordering,
  initial-filter status omission, host-ID-only delayed connectivity events,
  client-route cross-product filtering/deletion, and tablet batch maintenance.
- Required coordinator, per-node error priority, successful-version agreement,
  mismatch/zero-success outcomes, fresh snapshot per retry, error taxonomy,
  last-completed-check deadline result, manual no-refresh behavior, and optional
  required-host interop API.
- Retained snapshot/pool lifetime, final merge-slot delivery, and the documented
  refresh-reply loss panic on unexpected runtime shutdown.

## Test coverage and validation

Read representative assertion bodies and setup in:

- Worker scheduler tests and update/merge-channel tests; UDT sort/type parsing
  tests; state reuse/filter/duplicate-tablet tests; fixed-connection first/later
  page timeout tests and server-error test setup.
- `scylla/tests/integration/metadata/{partial_fetch,event_flood,configuration,contents}.rs`:
  fetch counts and unknown hinted addresses, event dropping/flood completion,
  minimal/disabled/restricted schemas, partitioners/types/keys/views/tablets,
  and timeout-clause checks.
- `scylla/tests/integration/session/{status_change_hints,schema_agreement}.rs`
  and `scylla/tests/integration/ccm/host_listener.rs`: targeted DOWN probes,
  required-host and transient failures, six automatic-agreement APIs, refreshed
  table visibility, and listener transition/filter assertions.

A local read-only validation script checked all 74 pinned source links against
the verified checkout, all 68 referenced test functions for actual declarations,
and local Markdown section anchors. All passed. `git diff --check --
feature-specs/metadata-control-events.md` passed. The before/after digests were
calculated with `sha256sum`.

No Rust, Go, database, or proxy test suite was executed. Existing missing-coverage
qualifications remain; the new profile-isolation, zero-token agreement, combined
deadline, and disabled-automatic-wait cases are not proved by the cited tests.
No additional contradiction requiring another section's edit was verified.
