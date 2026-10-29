# Metadata, control connection, and events — round 09

- Section: `feature-specs/metadata-control-events.md`
- Round: 09, whole-section review with adversarial-input/event-loss focus
- Agent identity: `/root/review_metadata_09`
- Source checkout: `/extra/scylladb/gocql-rust-reference-f8b0ee9`
- Source commit: `f8b0ee928dc0dcb475247dc82cce4e42804b0621` (verified with `git rev-parse HEAD`)
- Before SHA-256: `4035a8cc990094bdae0803f0f927c0e7db83b6bafdf393d1d260c8b38c8e0904`
- After SHA-256: `641a788d38bd3c710d42bac09b23878c1dd0962c2f91ba09e29e3f3a7096b0f0`

Read `feature-specs/README.md`, all of the assigned specification, and
`feature-specs/reviews/REVIEW-INSTRUCTIONS.md`. Independently traced the pinned
implementation. No prior report was used as evidence. Changed only the assigned
specification and this report; no Go or Rust implementation changes, and no child
agents.

## Findings and fixes

1. **Topology identity validation was overstated by omission.** The document
   described node identity and coherent snapshots without describing accepted
   conflicting identities. `ControlConnection::validate_peers`
   (`scylla/src/cluster/metadata/fetching.rs:230-242`) only rejects an empty peer
   vector or all-empty token lists. `query_peers` merges concurrent local/peer
   streams and appends rows (`fetching.rs:339-370`), without checking duplicate
   host IDs, addresses, or tokens. `ClusterState::calculate_new_topology`
   (`scylla/src/cluster/state.rs:423-489`) looks up reusable nodes in the old map,
   overwrites the new map for every matching host ID, and appends every row's
   token/node pairs. `TokenRing::new`
   (`scylla/src/routing/locator/token_ring.rs:20-24`) sorts without deduplication;
   `ReplicationInfo::new` (`scylla/src/routing/locator/replication_info.rs:62-106`)
   preserves that ring. Conflicting rows can therefore leave a ring node object
   and address different from the public map's object for the same ID. Added the
   input limitation, qualified the snapshot invariant as atomic publication,
   linked the evidence, and added a missing test/generation case. This is source
   behavior, not a proposed requirement to reproduce invalid-input quirks in Go.

2. **Column validation and recoverable-schema-error scope were too broad.** The
   prior text said unknown column kinds fail a fetch and invalid key positions
   invalidate a keyspace, without the actual validation order and table-name
   association conditions. `query_tables_schema`
   (`fetching.rs:1415-1487`) first skips `empty` columns, then propagates a
   keyspace-level missing-UDT error without parsing column types or kinds. An
   unresolved UDT in the current column also returns before kind validation.
   Consequently a missing-UDT error can mask a malformed type or unknown kind
   that would otherwise fail the containing fetch. Separately, `query_tables`
   and `query_views` (`fetching.rs:1288-1387`) consume schema entries only for
   returned table/view names. Unmatched column-derived recoverable errors are
   left in the temporary map and discarded by `query_keyspaces`
   (`fetching.rs:786-808`); a gap in an unlisted table's key therefore need not
   invalidate the keyspace. Qualified both rules, kept fetch-wide syntax errors
   distinct from recoverable errors, linked the implementation, and added
   explicit missing tests and generation scenarios.

## Source inspected and behaviors checked

- `scylla/src/cluster/metadata/mod.rs:40-165,224-340,452-479`: fetch modes,
  recoverable error variants, public schema fields, optional strong-consistency
  visibility, peer endpoint representation, column kinds, and dummy metadata.
- `scylla/src/client/session.rs:469-520,1455-1485,1593-1603,1822-1840,2549-2865`:
  exact defaults; metadata consistency independent of default profiles; local
  ONE agreement statement; automatic agreement/refresh ordering; per-node
  sequential attempts, concurrent node reads, required host handling, transient
  classification, last-completed-check error precedence, and optional interop.
- `scylla/src/cluster/node.rs:222-310,346-445`: enabled versus connected,
  pool-trigger forwarding, host-ID equality/hash, DNS default-port retry,
  per-lookup deadline, original-error retention, and address deduplication.
- `scylla/src/cluster/metadata/cc_establisher.rs:175-445`: shuffled known-peer
  pass, initial versus recovery fallback, dummy startup after a successful open,
  rejected-peer metadata retention, contact-point rejection limitation, future
  candidate updates, event registration, and event-queue capacity.
- `scylla/src/cluster/metadata/fetching.rs:1-242,245-430,450-651,665-1030,1030-1115,1288-2010`:
  concurrent full fetch composition, tolerating failed topology streams,
  random-token fallback, aggregate rejection, filtered schema reads and absent
  keyspace deletion, full/minimal scope, UDT dependency ordering and truncating
  field-list zip, primary-key position checks, type grammar, Scylla compatibility
  fallback, exact consistency recognition, replication strategies, route port
  conversion and pre-filter validation of all returned route rows.
- `scylla/src/cluster/metadata/worker.rs:1-480,522-1085`: one outstanding fetch
  per type; full preemption; event work retained after fetch start; latest
  per-keyspace events; retry errors inserted only if no newer event exists;
  periodic deadline consumption; establishment event draining; immediate repair
  followed by one-second cadence; explicit requests blocked behind running full
  work; preservation through replacement; runtime-shutdown reply-loss caveat.
- `scylla/src/cluster/metadata/update.rs:1-260,350-410` and its tests:
  full/partial replacement precedence, refresh-handle retention, pending valid
  schema surviving errors, unapplied absence replaced by errors, route-pair
  merge behavior, and status-hint coalescing.
- `scylla/src/cluster/metadata/merge_channel.rs:1-240` and cancellation/drop test
  bodies: capacity-one merging, last value delivered after sender drop,
  cancelled-receive safety, and rejection after receiver drop.
- `scylla/src/cluster/worker.rs:65-110,270-333,418-809`:
  bounded refresh/keyspace queues, unbounded connectivity events, tablet batch
  reception, routes before pool construction, DOWN hints on old state and UP
  hints on selected new state, listener calls before initialization/publication,
  refresh responses afterward, filter-only transitions, and host-ID-only
  connectivity notification handling.
- `scylla/src/cluster/state.rs:40-180,254-610,748-790,846-891`:
  immutable snapshot structure, full rebuilding versus partial sharing,
  node/pool reuse and address mutation, equality-based suppression, old-schema
  fallback, zero-token public nodes versus ring-only initialization, working
  connection iteration, and per-batch tablet duplicate checks.
- `scylla/src/routing/locator/mod.rs:65-93`,
  `scylla/src/routing/locator/replication_info.rs:1-110`,
  `scylla/src/routing/locator/token_ring.rs:1-80`, and
  `scylla/src/routing/locator/tablets.rs:986-1054`: verified ring preservation
  and metadata-maintenance integration, including tablet table/view retention
  and removal of unknown/non-tablet schema entries.
- `scylla/src/cluster/control_connection.rs:25-240`,
  `scylla/src/client/pager.rs:553-585`, and
  `scylla/src/network/connection.rs:929-954,1165-1198,1231-1253`:
  page size, query-text cache, timeout derivation, PREPARE boundary, per-page
  fixed-connection policy, `UNPREPARED` recovery, and separate raw agreement path.
- `scylla/src/client/client_routes.rs:76-120`,
  `scylla/src/policies/host_listener.rs:1-105`, and
  `scylla-cql/src/frame/response/event.rs:171-335,380-455`: nonempty public route
  configuration, listener vocabulary, schema invalid-kind behavior, and malformed
  client-route event length/UUID rejection before worker pair zipping.

The spec's subfeatures, invariants, language-independent contract, implementation
table, and Go test gates were all reviewed. Existing descriptions of accepted
incomplete topology, full-fetch failure recovery, stale keyspace fallback,
drop/error merge ordering, lost-event limitations in affected-keyspace mode,
refresh cancellation, retained mutable pools, and agreement snapshots remain
supported. No additional edits were justified for those paths.

## Test coverage and verification

Read relevant test bodies/assertions and searched the full named test inventory:

- Metadata worker plan/poll tests, update merge tests, merge-channel drop and
  cancellation tests, UDT sorting/type parser tests, and state topology/tablet
  constructor tests support the narrower claims already in the document.
  They do not establish scheduler-start behavior or publication under malformed
  duplicate topology/schema-validation-order inputs.
- `scylla/tests/integration/metadata/partial_fetch.rs` asserts topology/schema
  execution counts and publication, includes full-mode event-loss recovery, and
  leaves the real-server drop-query-count assertion commented out.
- `scylla/tests/integration/metadata/event_flood.rs` delays metadata reads and
  sends 100 events; its completion checks do not assert the changed contents of
  later metadata or preservation of every owed event fetch.
- `scylla/tests/integration/metadata/configuration.rs` and `contents.rs` verify
  selected full/minimal/disabled fields and timeout text. Their successful-schema
  assertions do not test malformed-column short-circuits or duplicate host IDs.
- `scylla/tests/integration/session/status_change_hints.rs:80-175` verifies two
  sequential per-host DOWN probes, after pool initialization and excluding the
  control connection; it does not prove UP refill timing or post-probe liveness.
- `scylla/tests/integration/session/schema_agreement.rs` and
  `scylla/tests/integration/ccm/host_listener.rs` retain the coverage limits
  stated in the spec: injected error/coordinator checks, selected legal listener
  transitions, optional feature gates, and no complete adversarial matrix.
- Automated source-reference validation checked all **79** pinned file/line
  links and **68** named Rust test symbols in the final specification; all resolve
  in the pinned checkout. This checks existence/ranges, not executable coverage.

No Rust or Go test suite was run; this round is a source review and Markdown fix.
The two new behaviors are derived from traced branches rather than runtime
reproductions, and their absent regression coverage is explicit. Existing Go
unit/race, integration, and allocation gates remain missing; legacy test changes
elsewhere in the shared workspace are not evidence for the proposed API.

The duplicate-host-ID qualification was sent to the parent as a cross-section
routing review note; no other specification was edited.
