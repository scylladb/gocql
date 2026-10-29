# Metadata, control connection, and events — round 05

- Section: `feature-specs/metadata-control-events.md`
- Round: 05 — whole-section audit, with additional focus on actual test assertions and guards
- Agent identity: `/root/review_metadata_05`
- Reference checkout: `/extra/scylladb/gocql-rust-reference-f8b0ee9`
- Source commit, verified with `git rev-parse HEAD`: `f8b0ee928dc0dcb475247dc82cce4e42804b0621`
- Before SHA-256: `70b55c3b2a9cb260a8d8dfdd38053393ca2c802ec8a23260304e6e6447f6a273`
- After SHA-256: `7a2918cc619b0d03478dd2d15f74cd4deb1f0d7a3f2e52e73d9e026482ca2b02`
- Scope: changed only the assigned specification and this report. No Go or Rust implementation edits; no child agents.

Read `REVIEW-INSTRUCTIONS.md`, `feature-specs/README.md`, and the complete assigned
specification. Findings below come from the pinned source and test bodies, without
using previous reports as proof. Production behavior was traced independently;
this round found coverage-description problems and added a public-node API
cross-reference after a related review flagged its omission.

## Inspected source and test evidence

Paths in this report are relative to the reference checkout unless stated otherwise.

| Files and symbols inspected | Relevant lines / purpose |
| --- | --- |
| `scylla/src/client/session.rs`: `SessionConfig::new`, `get_schema_version_statement`, `handle_auto_await_schema_agreement`, agreement checker/waiter/classifier and per-node reads | 469–522, 1593–1602, 1822–1840, 2535–2832; defaults, ONE/idempotent query, automatic refresh, snapshot capture, error priority and deadline behavior |
| `scylla/src/cluster/control_connection.rs`: `MetadataRequestTimeouts`, preparation cache, `query_iter`, `wait_for_event`, `test_custom_timeouts` | 25–70, 155–266, 295–499; timeout derivation, PREPARE boundary, cache, shutdown signal, dry-proxy assertions |
| `scylla/src/cluster/node.rs`: hostname/contact-point resolution, `inherit_with_ip_changed`, resolver tests | 175–191, 346–456, 499–656; DNS retry/dedup, shared pool update, injected resolver coverage |
| `scylla/src/cluster/metadata/cc_establisher.rs`: establishment/candidate loop, known-peer updates, rejection check, event registration, topology guards | 171–499; shuffled candidates, fallback ordering, dummy startup, filtered contact-point limitation, 32-event queue |
| `scylla/src/cluster/metadata/mod.rs`: fetch modes, errors/public metadata, route merge, `Metadata::new_dummy` | 41–270, 408–478; supported content, recoverable categories, route deletion, synthetic topology |
| `scylla/src/cluster/metadata/fetching.rs`: full/topology fetches, peers, routes, schema filtering, UDTs, tables/views, type parsing, compatibility and strategies | 171–1107, 1290–1995; read composition, validation, error scope, field-list zip, missing-column fallback |
| Same fetching file: `test_udt_topo_sort_*`, `test_consistency_mode_parsing`, `test_cql_type_parsing` | 1161–1287, 2010–2213; actual fixtures and assertions |
| `scylla/src/cluster/metadata/worker.rs`: fetch plans/slots, scheduler, establishment drain, repair, event handling, publication, unit tests | 130–1348; full/partial ordering, cancellation, deadline advancement, errors, refresh ownership and test limits |
| `scylla/src/cluster/metadata/update.rs`: all merge paths and named merge/pair tests | 103–407, 415–1037; keyspace precedence, absent/error ordering, response retention, route filtering and deletions |
| `scylla/src/cluster/metadata/merge_channel.rs`: sender/receiver and tests | 79–291; capacity-one delivery, sender/receiver drop, cancelled receive |
| `scylla/src/cluster/state.rs`: topology sharing, pool initialization/hints, full/partial construction, keyspace fallback, tablet updates and named tests | 55–188, 235–570, 650–925, 1090–1462; coherent state, mutable retained pools, constructor-versus-publication coverage |
| `scylla/src/cluster/worker.rs`: construction, refresh queue, tablet branch, update application, listener handling | 81–220, 303–350, 418–803; routes before pools, hints, publication/replies, callback ordering and host-ID-only events |
| `scylla/src/network/connection_pool.rs`: pool handle, refill/keepalive triggers, connectivity event shape | 191–204, 280–314, 1577–1595; retained pool lifetime and event identity |
| `scylla/src/client/pager.rs`: fixed-connection page executor and spawning | 551–688, 1145–1188; per-page deadlines/fallthrough and continuation after consumer cancellation |
| `scylla/src/network/connection.rs`: UNPREPARED branch and fixed-connection tests | 1177–1250, 2830–3064; one reprepare/resend, first/later-page timeout and terminal-error assertions |
| `scylla/src/policies/host_listener.rs`; `scylla-cql/src/frame/response/event.rs` | listener vocabulary at 18–100; schema parsing 270–313 and topology/status/route parsing 370–452 |
| `scylla/src/routing/locator/tablets.rs`: maintenance | 800–848, 986–1055; unresolved replicas, removed/recreated nodes, table/view filtering |
| `scylla/tests/integration/metadata/{configuration,contents,partial_fetch,event_flood}.rs` | Entire files; named assertions, fixture restrictions, guards, proxy request counting and operation deadlines |
| `scylla/tests/integration/session/schema_agreement.rs`; `entry_point.rs`; `utils.rs::PerformDDL` | Entire agreement file; entry-point dispatch 1–195; DDL helper 476–496; six APIs, actual return-value assertions and ordinary DDL execution |
| `scylla/tests/integration/ccm/host_listener.rs`; `ccm/mod.rs`; integration `main.rs`; `scylla-ccm-bridge/src/lib.rs::run_ccm_test_with_configuration` | Entire listener file, module feature guards, CCM construction/run at 199–235 |

## Verified findings and fixes

1. **Fetch-plan/helper tests were easy to overread as scheduler/publication tests.**
   The worker tests at `worker.rs:1100–1348` construct plans or manually supplied
   futures; none invokes `start_due_fetches`. The state tests at
   `state.rs:1280–1462` check helper return values and object identity, not an
   `ArcSwap` publication or pool-initialization barrier. The metadata-merge tests
   at `update.rs:882–1037` pass `None` for refresh response handles.
   The spec now describes these precise layers and explicitly marks direct
   tick/preemption/refresh-acceptance, error-rescheduling, and coalesced-refresh
   reply/publication tests missing. The implementation supports these behaviors
   through `worker.rs:346–431,777–896` and cluster `worker.rs:418–523`; that source
   evidence does not turn helper tests into integration coverage.

2. **UDT/type coverage needed its malformed-input boundary stated.**
   `fetching.rs:1161–1227` checks dependency ordering across keyspaces and nested
   collections/tuples. `test_udt_topo_sort_ignores_invalid_metadata` at 1268 checks
   successful sorting with an absent referenced definition, not successful UDT
   construction or per-keyspace recovery. `test_cql_type_parsing` at 2033 contains
   valid examples only, including vector dimensions 5 and 1234. The spec now says
   this, identifies absent vector-dependency fixtures, and keeps missing malformed
   types, dimension boundaries, and missing-UDT publication recovery explicit.

3. **Configuration/content coverage was broader than the concrete assertions.**
   `configuration.rs:335–389` checks the minimal keyspace strategy/table and empty
   columns/keys/UDTs; it does not check minimal views, durable writes, tablet flags,
   or consistency modes. Its routing test at 392 disables tablets and checks a
   token-aware Murmur3 prepared statement plus one traced execution node. The
   view test at `contents.rs:436–486` checks names and base-table names, not view
   columns/keys. The CDC tests (`configuration.rs:439`, `contents.rs:393`) and view
   test (`contents.rs:435`) are ignored under `cassandra_tests`. The enabled-tablet
   test at `contents.rs:582–606` also returns early if the server lacks support.
   The spec now separates these claims and guards, records the exact cluster-name
   and three-address assertion, and does not imply strong-consistency or forced
   compatibility-fallback coverage.

4. **The timeout integration test has a non-vacuity limitation and omitted
   complementary unit evidence.** `configuration.rs:126–171` drains captured
   QUERY/PREPARE frames and checks each, but only logs the count. The test's
   selected server determines the ScyllaDB/non-ScyllaDB branch. The spec now
   records both limits. Added `control_connection.rs:test_custom_timeouts`
   (`295–499`): it simulates both negotiations and awaits a captured request for
   disabled/2137-ms overrides. Those requests fail at PREPARE by design, so this
   proves clause construction, not execution-time enforcement. The genuine
   first/later-page client-deadline tests remain separately described; client
   default/override/saturating-margin assertions are not supplied by these tests.

5. **Event tests prove constrained scenarios.** The topology/status helper at
   `partial_fetch.rs:97–170` injects an unknown address, intentionally suppressing
   known-host keepalive/refill effects, and induces a publication by changing a
   host filter. Schema tests restrict one keyspace and disable automatic metadata
   refresh (`383–396`); their request count subtracts UNPREPARED resends
   (`451–469`) rather than inspecting bound keyspace values. Flood tests use
   schema-disabled sessions, an unknown DOWN address, 100 events, and a 30-second
   post-injection completion bound (`event_flood.rs:38–64,74–308`). They do not
   assert owed-event-plan preservation or new schema content. The spec now states
   those boundaries and retains the existing manually injected keyspace-drop
   caveat.

6. **Agreement coverage needed exact error and assertion scope.** Unreachable
   and absent-required-host tests forge DDL/version responses and disable
   post-agreement metadata refresh (`schema_agreement.rs:78–224,310–376`). The
   former deliberately blocks the next check to assert the retained first
   broken-connection error at deadline. The transient test injects only
   `Overloaded`; its second one-shot call merely unwraps `Result<Option<Uuid>>`
   (`264–285`) and therefore does not assert `Some(version)`. API tests exercise
   the six `EntryPoint::ALL` cases, checking SyntaxError propagation and table
   visibility (`421–518`). The spec now separates these facts and explicitly
   lists missing mismatch/zero-success, full error-taxonomy, and error-then-mismatch
   deadline cases.

7. **Host-listener coverage omitted guards and overstated sequence validation.**
   `ccm/mod.rs:5–6` requires both `scylla_unstable` and
   `unstable-host-listener`. The log validator (`host_listener.rs:61–194`) permits
   Added-to-Down/Removed, so it does not enforce optimistic Up after every Added.
   The status test checks legal transitions and selected subsequent Up events
   (`416–424`); the topology test only calls `validate` (`523–528`). The filter
   test does explicitly check accepted-host events and absence for rejected
   hosts (`600–618`). Address changes and initially down nodes remain TODOs at
   `623–626`; the logger maps unrecognized events, including AddressChanged, to
   Unknown. The spec now describes actual assertions and marks exact topology
   sequences, address changes, optimistic initial Up, and callback/publication
   timing missing.

8. **Public node engine identification lacked a metadata-surface cross-reference.**
   A related connections review reported this omission through the parent. I
   independently checked `node.rs:199–208`, `connection_pool.rs:324–334`, and
   `connection.rs:2005–2015`: `Node::is_scylladb()` derives an optional result from
   a pooled connection's negotiated extensions and can detect ScyllaDB without
   sharding. Added a short cross-reference to the connections section, which owns
   the detailed sampling semantics; no other section was edited by this agent.

## Whole-section behaviors checked without further changes

- Defaults, schema levels, keyspace restriction and Scylla-only server clauses;
  client timeout override/derivation, per-page application and initial-PREPARE
  exception agree with `SessionConfig`, `MetadataRequestTimeouts`, cache and pager.
- Concurrent DNS expansion, default-port retry excluding initial timeout, original
  error preservation, deduplication, shuffle and contact-point fallback agree
  with resolution and establishment branches.
- Candidate event draining, rejected-candidate plan lifetime, queued status hints,
  dummy startup after an opened connection's failed fetch, and contact-point
  filtering limitation match the establishment implementation.
- Full topology/schema/routes composition; tolerant topology row/query failures;
  null host/token handling; random token replacement; nonempty aggregate checks;
  local advertised address and full-only cluster-name replacement are supported.
- Minimal/full schema composition, strategy parsing, UDT sorting/construction,
  contiguous key positions, empty-column fallback, exact `empty` omission,
  unsigned-16-bit vector parsing, field-list zip and compatibility handling match
  their branches. Recoverable errors remain distinct from whole-fetch failures.
- Event vocabulary, schema-drop-versus-alter handling, latest-per-keyspace/hint
  merging, partial concurrency, full preemption and newer-event retention,
  periodic deadline advancement/overflow, one-second repair cadence, and refresh
  replies after state publication match the worker/state paths.
- Pending valid schema survives newer errors; a pending absence can become an
  error and consult published-state fallback. Route pair filtering, missing-route
  deletion, whole-route-read errors and routes-before-pools ordering are intact.
- Snapshot sharing/object reuse, zero-token public membership, enabled-object
  initialization shortcut, retained mutable pools, listener ordering before
  publication, and host-ID-only connectivity events match the implementation.
- Tablet batching and duplicate detection, maintenance of tables/views, unknown
  replica removal and recreated-node replacement agree with state and locator.
- Agreement snapshots, concurrent per-node/sequential per-connection reads,
  broken-error suppression/priority, required coordinator, retry classifier,
  last-check deadline result, automatic refresh and manual non-refresh behavior
  are consistent. Merge-channel shutdown/cancellation and the recorded dropped
  refresh-reply shutdown issue remain accurately distinguished from Go targets.

## Validation, uncertainty and remaining coverage

No Rust unit, integration, proxy, or CCM suite was executed. Test conclusions above
are from source inspection, including real assertions, setup, guards and TODOs.
The spec continues to label Go v2 tests and allocation benchmarks missing.

A local reference check found all 65 named test references and all 39 pinned
source links' files/line bounds valid. This is reference validation, not an
assertion-coverage or runtime test result. Some later bullets use unqualified
test names within an already identified source file; their definitions were also
read directly. The spec's new missing-coverage statements are scoped to the cited
tests; they do not claim an exhaustive absence search across every Rust test.

Existing gaps remain explicit, including refresh cancellation/shutdown replies,
incomplete-topology replacement, malformed route rows, schema-drop/reread failure,
drop/error coalescing, overlapping full fetch/state rebuild, retained-pool events,
schema-agreement membership changes and blocked one-shot deadlines. Added gaps
are detailed in findings 1–7. The parent-coordinated API cross-reference in finding
8 required no edit outside this agent's ownership.
