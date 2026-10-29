# Metadata, control connection, and events — round 10

- Section: `feature-specs/metadata-control-events.md`
- Round: 10, final comprehensive fidelity and completeness audit
- Agent identity: `/root/review_metadata_10`
- Reference checkout: `/extra/scylladb/gocql-rust-reference-f8b0ee9`
- Source commit: `f8b0ee928dc0dcb475247dc82cce4e42804b0621`, independently
  confirmed with `git rev-parse HEAD`.
- Before SHA-256: `641a788d38bd3c710d42bac09b23878c1dd0962c2f91ba09e29e3f3a7096b0f0`
- After SHA-256: `3b8d5ed7edddee60feb44c555e083426bb4e7a5970c2f656192bae5ea9465db1`

Read `REVIEW-INSTRUCTIONS.md`, `feature-specs/README.md`, and the complete
assigned specification. The review traced the pinned implementation and test
assertions independently. No prior review report was used as proof. Only the
assigned specification and this report were edited; no Go/Rust source or other
agent's file was changed. No child agents were spawned.

## Verified findings and fixes

### 1. Missing listener status also suppresses removal and address-change events

The specification already explained that enabling a host initially discovered
while filtered does not create listener status, so connectivity events are
ignored. It did not carry that qualification into the removal/address-change
contract.

Evidence:

- `scylla/src/cluster/worker.rs:577-620` identifies additions/removals by host-ID
  membership and restricts address-change candidates to previously enabled hosts.
- `worker.rs:629-635` requires `node_status.remove(host_id)` to succeed before
  either removal notification; otherwise it logs and continues.
- `worker.rs:667-675` similarly skips every address-change notification when
  `node_status.get_mut` finds no status.
- `worker.rs:723-751` inserts optimistic connected status for an enabled new
  addition. An enabled reappearance after actual topology removal can therefore
  start reporting again.
- `worker.rs:499-526` performs notification processing before publishing the
  constructed state. Skipping a notification does not veto topology removal.

Fix: qualified removal/address-change reporting with its recorded-status
requirement, documented the initially-filtered/enabled/address-change/removal/
readdition sequence, added pinned branch links, and added the missing regression
case and generation guidance. This records the source limitation without making
it a proposed Go requirement.

### 2. Duplicate tablet feedback is suppressed only for resolved replicas

The publication and tablet-coordination paragraphs described duplicate feedback
as an unconditional no-op. This overstated the implementation and disagreed with
the already-qualified routing specification.

Evidence:

- `scylla/src/routing/locator/tablets.rs:553-566`, `Tablet::is_same_as`, requires
  matching endpoints, version, ordered host/shard pairs, and `self.failed.is_none()`.
- `tablets.rs:770-772,908-910` forwards that predicate through the cache lookup.
- `scylla/src/cluster/state.rs:846-891`, `with_updated_tablets`, skips only a
  successful `contains`; otherwise it clones lazily and installs the tablet,
  including unresolved replicas returned by `from_raw_tablet`.
- `scylla/src/cluster/worker.rs:326-333` publishes the resulting `Some` state.
- `tablets.rs:1609-1667`, `contains_requires_exact_match`, explicitly asserts
  that an identical payload with an unresolved replica is not considered present.

Fix: qualified both publication descriptions, described the exact comparison,
linked the predicate, catalogued the existing lower-level test with its limits,
and added a publication-level regression scenario. The routing section already
matched the source; the parent was informed and that file was not edited.

## Production source and behavior checked

The source catalog assigns 12 production modules and one book-test module to
this section. All 12 production assignments were inspected; their public types,
worker entry points, and relevant branches map to the named subfeatures. Line
numbers below refer to the pinned Rust checkout.

| Files / key symbols inspected | Independently checked behavior |
| --- | --- |
| `scylla/src/cluster/mod.rs:1-42`; `metadata/mod.rs:40-71,224-391,402-481` | Public exports, fetch modes, keyspace/table/view/column fields, optional consistency visibility, exact column kinds, route maps, generated dummy IDs/tokens and absent schema/name/routes. |
| `scylla/src/client/session.rs:469-526,1514-1522`; `cluster/control_connection.rs:25-66,159-225` | All stated metadata/DNS/agreement defaults; disabled/minimal/full selection; saturating timeout margin; independent per-page client limit; server-family timeout selection, millisecond formatting, shared query-text cache, page size 1024 and idempotence. |
| `cluster/node.rs:346-455`; `metadata/cc_establisher.rs:109-451` | Concurrent DNS expansion, fallback only after non-timeout errors, original-error preservation, address deduplication/sorting, shuffled known candidates, noninitial contact-point retry, event registration, host filtering, retained rejected metadata, dummy startup only after opening a connection, and contact-point rejection limitation. |
| `metadata/fetching.rs:171-435` | Concurrent full-fetch components; peer/local queries without peers-v2; endpoint port and advertised RPC IP; nullable IDs/tokens/name; row/query/page-error tolerance; random token fallback; aggregate validation; duplicate identity/address/token acceptance. |
| `metadata/fetching.rs:459-757,759-1031,1033-1115,1285-1603,1606-2000`; `scylla-cql/src/utils/parse.rs:84-91` | Route port conversion and query-wide failure; exact partial pair/keyspace filters; missing requested keyspace deletion; mixed drop/reread failure; minimal/full schema content; UDT dependency sorting and missing-UDT recovery; conditional column validation; discarded unlisted-table errors; contiguous key positions; empty column sets; frozen/tuple/vector/UDT representation; zero/u16 vector dimensions; zipped UDT field lists; strategy parsing; Scylla table/column compatibility and exact global-consistency recognition. |
| `metadata/worker.rs:156-285,349-512,542-635,670-1098` | Per-type concurrency, partial polling, latest schema event, retry precedence, full-fetch preemption, post-start owed events, tick advancement/overflow, explicit-refresh acceptance boundary, candidate event draining, immediate/one-second repair cadence, failed request reporting, established-control filter behavior, and shutdown reply-channel limitation. |
| `metadata/update.rs:113-284,309-406`; `metadata/merge_channel.rs:102-181` | Full/partial merge precedence; preservation of refresh replies; valid pending schema over newer errors; nondurable absence/tombstones; route deletion/disjoint merges; latest hint per address; one merge slot; cancellation-safe receive and final delivery on sender drop. |
| `cluster/state.rs:41-90,149-205,254-585,587-609,748-782,846-891` | Immutable snapshot containers with mutable pools; full replacement versus partial sharing/no-op; zero-token node inclusion; object/pool reuse; host-filter changes; duplicate-host map/ring divergence; keyspace fallback; tablet maintenance and duplicate comparison; exact-name/no-fetch inspection; agreement pool enumeration; initialization over token-ring nodes only. |
| `cluster/worker.rs:85-224,303-526,529-793`; `cluster/node.rs:115-189,197-306` | Channel capacities and worker ownership; initial route installation; current-state DOWN versus new-state UP/refill handling; listener timing and optimistic Up; initialization waits; success replies only after publication; retained pool lifetime/address mutation; host-ID-only equality and connectivity events; missing-status notification guards. |
| `policies/host_listener.rs:21-94`; `scylla/tests/integration/ccm/mod.rs:5-6`; `client/session.rs:1491-1502` | Listener vocabulary and synchronous callbacks; unstable public/test gates; normal topology and connectivity transition rules and initially filtered-host limitations. |
| `client/session.rs:1593-1601,1822-1841,2513-2531,2549-2864`; `network/connection.rs:929-959` | Raw ONE/idempotent schema-version QUERYs over ordinary pools; one snapshot per check; all node reads awaited; per-node sequential connections and error preference; required coordinator/optional interop ID; mismatch/zero-success behavior; full retry taxonomy; last-completed-result deadline precedence; automatic refresh outside original request and agreement deadlines; manual calls never refresh. |

Cross-boundary callees were inspected rather than relying on their comments:

- `client/pager.rs:553-684,1140-1180,1222-1245` and
  `network/connection.rs:1177-1251`: fixed-connection fallthrough execution,
  once-per-page deadlines, `UNPREPARED` reprepare/resend, spawned later-page
  lifetime, and automatic first-response schema-change handling.
- `client/session.rs:1457-1480` and
  `scylla-cql-core/src/frame/types.rs:17-42`: metadata execution uses default
  LOCAL_QUORUM and no serial consistency independently of execution profiles.
- `network/connection_pool.rs:192-201,275-310,753-787,1579-1592`: endpoint/pool
  retention, hint notifications, backoff reset versus delayed/in-flight refill,
  targeted keepalive, and host-ID-only connectivity signals.
- `client/client_routes.rs:76-110`: public nonempty route configuration versus
  internal subscriber filtering and application.
- `routing/locator/token_ring.rs:20-23` and
  `routing/locator/tablets.rs:515-566,770-783,801-847,908-927,986-1049`: retained
  duplicate ring entries, exact duplicate tablet suppression, unknown-replica
  handling, removed/recreated node maintenance, and tablet table/view cleanup.
- `scylla-cql/src/frame/response/event.rs:35-67,171-195,270-360` and
  `frame/server_event_type.rs`: event vocabulary and unknown-kind behavior.
  The corresponding protocol section's paired-route event validation and
  metadata section's worker dispatch remain consistent.

All listed invariants were rechecked against these paths: event draining during
fetches, scheduler exclusion, explicit-refresh publication ordering, failed-fetch
boundaries, future-candidate updates before publication, fetch-scope independence
from topology, liveness hints, snapshot ownership, route-before-pool ordering,
and nonvacuous/required-coordinator agreement. Existing qualifications concerning
inconsistent server input, stale recoverable keyspaces, and continued network
work after future cancellation remain necessary.

## Test claims checked

No Rust/Go unit, integration, CCM, race, or benchmark suite was executed. The
following are source/assertion inspections, not runtime passes:

- `metadata/worker.rs:1143-1350`: every named plan/polling test was read; none
  invokes `start_due_fetches`. Tick/preemption/refresh-barrier integration claims
  remain explicitly missing.
- `metadata/update.rs:670-789,882-1048`: route sequential/coalesced equivalence,
  cross-product exclusion, missing pair deletion, pending schema reuse and
  absence-then-error assertions. Refresh response arguments are `None`; no
  publication/reply-order claim was inferred.
- `metadata/merge_channel.rs:238-277`: last pending value, parked receiver and
  cancellation assertions; `node.rs:500-633`: injected DNS resolver expansion,
  duplicate elimination and failed lookup omission, without real DNS fallback.
- `metadata/fetching.rs:1140-1280,2010-2213`: UDT sorting/cycles/missing dependency
  and valid type/consistency fixtures; no construction/publication recovery,
  malformed-type or vector-boundary coverage was inferred.
- `state.rs:1101-1215,1280-1448`: enablement/object replacement, shared-enabled
  objects, unchanged partial topology, zero-token additions, schema sharing,
  tablet no-op and within-batch ordering. These are state helper tests rather
  than worker publication/listener tests. The additional resolved-replica
  comparison test is described in finding 2.
- `network/connection.rs:2873-3045`, `control_connection.rs:294-511`, and
  `client/session_builder.rs:1613-1630,1871-1884`: proxy-backed first/later-page
  deadlines, terminal server-error execution counts, captured timeout PREPARE
  clauses for both simulated families, and configuration-only builder assertions.
- `tests/integration/metadata/configuration.rs:65-233,241-523` and
  `contents.rs:95-632`: disabled/minimal/full schema, routing/CDC, keyspace scope,
  nested definitions, column kinds, primary-key order, durable writes, view
  names/base tables, cluster addresses/name and tablet flags. Cassandra gates,
  tablet support early return, absent view-key assertions and timeout-message
  count merely being logged were confirmed.
- `tests/integration/metadata/partial_fetch.rs:32-481`: exact topology/schema
  execution counts, noncontrol filter target, unknown-address status hints,
  restricted keyspace, disabled automatic refresh, event-free full mode,
  UNPREPARED-count adjustment, and forged query-free drop versus commented real
  drop assertion.
- `tests/integration/metadata/event_flood.rs:35-308`: three delayed-fetch paths,
  100 unknown-host DOWN events and 30-second completion checks; no claim of
  changed metadata contents or preserved owed work was inferred.
- `tests/integration/session/status_change_hints.rs:26-171`: one connection per
  host, 10,000-second keepalive interval, excluded control connections and two
  targeted hints per host. No post-probe liveness/UP-refill coverage was inferred.
- `tests/integration/session/schema_agreement.rs:142-518`: forged coordinator
  failure versus other-node failure, required-host absence/ID, transient
  overload retry, non-`Some`-asserting one-shot sanity check, six API entry points,
  SyntaxError propagation and table visibility after automatic refresh.
- `tests/integration/ccm/host_listener.rs:108-198,351-628`: permissive state-graph
  assertions, actual status/filter assertions, topology log validation without
  full expected-event assertions, and TODOs. No changing-filter/missing-status
  lifecycle regression was found among these cited tests.
- `book_tests.rs:74,212` and included schema/schema-agreement documentation:
  examples are API-use evidence, with the already-recorded prose limitations.

The Go test/race/integration/benchmark entries remain future migration gates;
this review adds no claims that those gates have passed. The existing missing
coverage list was checked against the named source tests and retained. The new
listener case and publication-level unresolved-tablet case remain gaps even
though the underlying source branches and tablet predicate test are clear.

## Validation and remaining uncertainty

- Checked 82 pinned source links for the correct revision, existing source path,
  and in-range line anchor; printed their target lines and reviewed their
  relevance while tracing the implementations.
- Confirmed all 69 named Rust test references resolve to actual function
  declarations, including the newly added tablet comparison test.
- Checked all nine relative Markdown links, including section anchors, against
  the local destination files. No trailing whitespace remains in the spec.
- Compared relevant connection refill/keyspace behavior, routing tablet
  maintenance, execution response side effects, configuration defaults/client
  routes, and wire-event definitions. The one confirmed cross-section mismatch
  was corrected in this owned section and reported to the parent.

No unresolved factual disagreement was found in the audited section after these
two fixes. Runtime behavior has not been executed in this review; concurrency,
malformed-input, shutdown and optional-feature scenarios explicitly marked as
missing remain unproven by the cited tests. The specification records reference
behavior and limitations, not an assertion that the proposed Go API exists or
must inherit undesirable Rust behavior.
