# Metadata, control connection, and events — round 07

- Section: `feature-specs/metadata-control-events.md`
- Round: 07 — precise source anchors and implementation-versus-contract wording
- Agent: `/root/review_metadata_07`
- Reference checkout: `/extra/scylladb/gocql-rust-reference-f8b0ee9`
- Source commit, verified with `git rev-parse HEAD`: `f8b0ee928dc0dcb475247dc82cce4e42804b0621`
- Before SHA-256: `2eae3f6b20b48989d5d87d58a6a2921a44bf4993be6ad45a95366a550152f87f`
- After SHA-256: `3d65a36573f9f70c9716eb93f75276b09f664f4a179393057088a8686a2f4b78`

Read `feature-specs/reviews/REVIEW-INSTRUCTIONS.md`, `feature-specs/README.md`, and the complete
assigned specification. This review independently traced the pinned source;
prior round reports were not used as proof. Only the assigned specification and
this report were edited. No Go or Rust implementation was changed.

## Findings and fixes

### 1. Evidence anchors did not consistently identify the named implementation

The event-dispatch link targeted `scylla/src/cluster/metadata/worker.rs:901`, the closing brace of
the preceding method, instead of `handle_server_event` at 911. The merge-channel
link targeted `scylla/src/cluster/metadata/merge_channel.rs:79`, the end of initialization, rather than the
delivery/cancellation logic at 102 and 149. The snapshot-sharing link targeted
`scylla/src/cluster/state.rs:235`, initial tablet maintenance, rather than full/partial state update
construction. The public-metadata link started at the internal `Disabled`
variant (`scylla/src/cluster/metadata/mod.rs:41`), without distinguishing its visibility from public
types and `PeriodicFetchMode`.

Corrected these and other imprecise entry anchors. Split combined links when the
claimed behavior spans distinct implementations: repair and established-worker
loops; listener topology and connectivity handling; full/partial state updates,
node reuse, keyspace recovery and tablet maintenance; agreement classification,
waiting and one-shot checks; event vocabulary and parsing. The evidence table
now links to actual declarations or the specific relevant branches. Inspected
the linked bodies and their callees; a valid line number alone was not accepted
as behavioral evidence.

### 2. Sharing language overgeneralized full-refresh container reuse

The statement that updates share unchanged topology/schema objects did not
distinguish full updates. `ClusterState::new_updated` (`scylla/src/cluster/state.rs:254–287`) creates
a new schema-map `Arc`, topology `Arc`, and locator on every full update.
`ClusterWorker::apply_metadata_update` (`scylla/src/cluster/worker.rs:445–455,514–519`) constructs and
publishes the replacement without an equality suppression branch. By contrast,
partial updates preserve untouched containers and can return no replacement
(`scylla/src/cluster/state.rs:304–347,356–377`). Nodes/pools can still be reused
(`scylla/src/cluster/state.rs:423–488`), and recoverable errors can reuse old keyspaces
(`scylla/src/cluster/state.rs:508–532`).

Clarified the distinction and added the absent regression for equal full
refreshes versus unchanged partial topology updates to the coverage gaps and
test-generation notes.

### 3. Listener connectivity transitions require an existing status record

An initially filtered host is represented in topology, but is excluded from
`added_nodes` (`scylla/src/cluster/worker.rs:580–585`). Listener status is seeded only while
processing enabled additions (`scylla/src/cluster/worker.rs:723–751`). Enabling that same known ID
later does not make it a membership addition, so no status record is created.
`handle_connectivity_change_event` requires both the current node and its status
record (`scylla/src/cluster/worker.rs:760–769`) and therefore ignores its later connectivity events.
This applies even though topology construction creates a working enabled pool
(`scylla/src/cluster/state.rs:439–478`).

Qualified the connectivity-transition contract, documented the initially
filtered-to-enabled case, and added its missing listener regression. The
state-level enablement tests do not exercise the worker's status map or callback
path; the existing CCM filter test uses a fixed datacenter filter.

### 4. An UP hint does not guarantee immediate connection opening

The connections review flagged the original phrase “immediate pool refill.”
Independently traced `Node::trigger_pool_refill` (`scylla/src/cluster/node.rs:244–247`) to
`NodeConnectionPool::trigger_immediate_refill` (`scylla/src/network/connection_pool.rs:285–286`).
That method signals a notification. The worker resets backoff and clears its
scheduled refill (`scylla/src/network/connection_pool.rs:753–767`), then schedules a needed refill
using the refill policy's delay (`774–790`). `need_filling` (`812–814`) excludes a
currently running refill.

Reworded the contract as an immediate worker hint/backoff-reset request with
policy delay and an existing refill still respected. Added a precise source row
and a link to the connections section's refill contract. No other section was
edited.

## Source inspected and checked behaviors

Paths below are relative to the pinned Rust checkout. The cited ranges identify
the principal inspected bodies, not merely symbol searches.

| Source and symbols | Behavior checked |
| --- | --- |
| `scylla/src/client/session.rs:469–521`; `scylla/src/cluster/control_connection.rs:25–225` | Actual defaults; timeout derivation and saturating margin; Scylla-only rendering; prepared cache across replacements; PREPARE outside the installed page timeout. |
| `scylla/src/cluster/node.rs:326–455`; `scylla/src/cluster/metadata/cc_establisher.rs:105–449` | Concurrent hostname expansion, original-error fallback, deduplication, shuffled candidate order, fallback order, startup dummy metadata, candidate rejection and contact-point exemption, registration and queue size. |
| `scylla/src/cluster/metadata/mod.rs:39–90,191–345,452–476` | Internal versus public fetch modes, public metadata vocabulary and optional strong-consistency visibility, column-kind parsing, dummy token construction. Also inspected `Peer`/endpoint definitions and the optional clone gate. |
| `scylla/src/cluster/metadata/fetching.rs:171–435` | Concurrent full fetch composition, peer/local advertised addresses, skipped read/row errors, null host/token handling, token parse fallback, aggregate validation and partial cluster-name behavior. |
| `scylla/src/cluster/metadata/fetching.rs:483–716,719–899` | Route port conversion and whole-read failure, cross-product filtering, empty subscriber distinction, query-free drops, absent versus broken keyspaces, fetch restrictions and full/minimal/disabled schema. |
| `scylla/src/cluster/metadata/fetching.rs:919–1119,1285–1609` | UDT parse/topological-sort failure versus keyspace recovery; zipped fields; default empty tables/views; column kinds, missing UDTs and primary-key position validation. |
| `scylla/src/cluster/metadata/fetching.rs:1615–2000`; `scylla-cql/src/utils/parse.rs:84–91` | Type parsing and unsigned vector dimensions, qualified/unqualified strategy parsing, Scylla-specific tables, exact `global` recognition and broad `Invalid` compatibility fallback. |
| `scylla/src/cluster/metadata/worker.rs:149–434,537–1097` | Coalescing plans, one slot per partial type, tick handling, full preemption, establishment event draining, repair cadence, deferred refresh acceptance, recoverable-keyspace retries, event dispatch and runtime-shutdown caveat. |
| `scylla/src/cluster/metadata/update.rs:103–405`; `scylla/src/cluster/metadata/merge_channel.rs:73–180` | Full/partial precedence, response preservation, per-address latest hints, route deletion/pair merging, pending drop then error behavior, bounded pending object and cancel-safe final delivery. |
| `scylla/src/cluster/state.rs:58–87,141–174,220–610,748–785,846–893` | Snapshot/container construction and sharing, node/pool reuse, stale-keyspace fallback, public inspection, zero-token node representation, ring-only initialization, working-connection snapshot and tablet duplicate suppression. |
| `scylla/src/cluster/worker.rs:65–216,303–386,418–800`; `scylla/src/cluster/node.rs:175–304` | Initial publication, queues, client-route-before-pool order, DOWN/UP target snapshots, refresh replies after publication, topology/callback order, retained mutable pools and host-ID-only node equality. |
| `scylla/src/network/connection_pool.rs:191–202,275–310,753–814,1577–1596`; `scylla/src/policies/host_listener.rs:18–92` | Shared pool lifetime, hint behavior, connectivity-event payload, synchronous callbacks and event vocabulary. Verified listener feature gates in `scylla/src/policies/mod.rs` and the CCM module. |
| `scylla/src/routing/locator/tablets.rs:801–846,986–1049`; `scylla/src/client/session.rs:73,1442` | Unknown-replica resolution/discard, removed/recreated nodes, schema maintenance, tablet queue capacity. Replica/range conflict rules remain owned by routing. |
| `scylla/src/client/pager.rs:553–689,1130–1189`; `scylla/src/network/connection.rs:1105–1249` | Per-page fixed-connection execution, fallthrough policy, spawned pager lifetime and one UNPREPARED recovery inside the page attempt. |
| `scylla/src/client/session.rs:1593–1601,1822–1838,2541–2864` | Internal agreement statement; captured membership, sequential connections and concurrent nodes; required host, zero-success/error handling, retry taxonomy, last-check deadline outcome, automatic refresh and optional interop wait. |
| `scylla-cql/src/frame/response/event.rs:19–43,48–146,171–191,270–340` | Event vocabulary, V2 event dispatch, schema targets and invalid schema-change-kind fallback. |

The summary, defaults, all named subfeatures, invariants, shutdown qualifications,
Go test/benchmark gaps and test-generation notes were checked against these
paths. No further behavioral correction was established in this round.

## Test coverage checked and remaining gaps

Read assertions and fixtures in the listed test groups, including their actual
scope and feature/database restrictions:

- Worker tests at `scylla/src/cluster/metadata/worker.rs:1143–1347` cover plans and polling; they do
  not call `start_due_fetches`. Merge tests at `scylla/src/cluster/metadata/update.rs:670–1035` check
  route composition and keyspace errors/absence, with no refresh response handles.
  Channel tests at `scylla/src/cluster/metadata/merge_channel.rs:189–289` check sender drop,
  receiver drop and cancellation.
- UDT tests at `scylla/src/cluster/metadata/fetching.rs:1161–1280` exercise sorting, missing
  dependencies and cycles, not state publication. Type/consistency tests at
  `2010–2210` use valid type strings and dimensions 5/1234; malformed and dimension
  boundary cases remain absent. State tests at `scylla/src/cluster/state.rs:1101–1215,1280–1435`
  check filter/object transitions and partial/tablet no-change behavior.
- Contact-point tests at `scylla/src/cluster/node.rs:500–628` use an injected resolver. Paging tests
  at `scylla/src/network/connection.rs:2873–3044` enforce page deadlines and terminal server
  errors. `scylla/src/cluster/control_connection.rs:294–484` checks PREPARE timeout text. Builder
  **test functions** at `scylla/src/client/session_builder.rs:1613,1871` check stored overrides and
  mode defaults; the identically named public setters are not the tests.
- Integration metadata `scylla/tests/integration/metadata/configuration.rs`, `scylla/tests/integration/metadata/contents.rs`, `scylla/tests/integration/metadata/partial_fetch.rs`
  and `scylla/tests/integration/metadata/event_flood.rs` retain the documented assertion boundaries: schema scope,
  exact schema values, tablet/CDC gating, partial-query counts, forged query-free
  keyspace drop, event-independent full mode, and 100-event completion tests.
  Existing assertions do not establish all the fault/publication races listed
  as missing.
- `scylla/tests/integration/session/status_change_hints.rs:80–167` checks addressed-host keepalives;
  `scylla/tests/integration/session/schema_agreement.rs:142–525` checks required-host/coordinator behavior,
  transient failures, six statement APIs and automatic metadata visibility.
  `scylla/tests/integration/ccm/host_listener.rs` checks legal observed transitions and a fixed filter;
  it does not cover a previously filtered known host becoming enabled.
- Read the schema/agreement book examples and verified their inclusion in
  `scylla/src/book_tests.rs:74,212`. Their prose does not supersede implementation
  timeout/error or periodic-fetch behavior.

No Rust, Go, database, proxy, or CCM test was executed. The new sharing and listener
qualifications are source-derived; direct regressions remain explicitly missing.
Existing missing scheduler, cancellation, shutdown, malformed-data and agreement
edge-case coverage remains a gap, not a passing claim.

## Validation

Verified all 65 pinned source link targets against the checkout, inspected their
referenced logic, and checked every local section anchor. Checked the edited
specification for trailing whitespace and conflict markers. Recomputed SHA-256
after the final spec edit. No unresolved cross-section change is required from
this round; the UP-hint wording now agrees with the connections contract.
