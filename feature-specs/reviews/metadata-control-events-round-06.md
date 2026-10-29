# Metadata, control connection, and events — round 06

- Section: `feature-specs/metadata-control-events.md`
- Round: 06 — omitted public, optional, and less-common behavior; whole-section audit.
- Agent identity: `/root/review_metadata_06`
- Source: `/extra/scylladb/gocql-rust-reference-f8b0ee9`, independently verified HEAD
  `f8b0ee928dc0dcb475247dc82cce4e42804b0621`.
- Before SHA-256: `7a2918cc619b0d03478dd2d15f74cd4deb1f0d7a3f2e52e73d9e026482ca2b02`
- After SHA-256: `2eae3f6b20b48989d5d87d58a6a2921a44bf4993be6ad45a95366a550152f87f`
- Changes are limited to the assigned specification and this report. No Go or
  Rust implementation was edited. No child agents were used.

## Source inspected

Read `feature-specs/README.md`, the complete assigned specification,
`reviews/REVIEW-INSTRUCTIONS.md`, and the source-catalog assignments. The review
used the pinned implementation directly; prior-round conclusions were not used
as proof. Every production module assigned to this section was inspected:

| Catalogue-owned file | Key implementation inspected |
| --- | --- |
| `scylla/src/cluster/mod.rs` | Public exports and separation of state, node, metadata, and worker modules. |
| `scylla/src/cluster/control_connection.rs` | Timeout derivation (49–69), original-text prepared cache (194–218), fixed-connection paging (228–261), control event/failure selection (266–307), timeout test. |
| `scylla/src/cluster/metadata/cc_establisher.rs` | Resolver initialization, shuffled candidates/contact fallback (104–245), rejected-metadata/dummy startup behavior (267–357), filter check (384–417), event registration (419–454), topology guards (470–493). |
| `scylla/src/cluster/metadata/fetching.rs` | Whole/partial composition and topology tolerance (171–433); route validation/filtering (483–649); filtered schema fetch and levels (665–900); UDT dependency construction (919–1121); tables/views/keys/type parsing (1285–1767); Scylla compatibility and strategy parsing (1770–2000). |
| `scylla/src/cluster/metadata/merge_channel.rs` | Single slot, modification and drop handling, notification registration, cancellation-safe receive, and all channel tests. |
| `scylla/src/cluster/metadata/mod.rs` | Fetch modes, error granularity, all public schema types, optional fields/traits, client-route merging, dummy metadata (41–479). |
| `scylla/src/cluster/metadata/update.rs` | Full/partial merge precedence, response retention, hints, route pair filtering, keyspace absence/error semantics (1–417); route and keyspace merge test assertions. |
| `scylla/src/cluster/metadata/worker.rs` | Fetch plans, due-fetch start/preemption, slot polling (118–513); candidate event draining, repair, serving loop (537–904); event translation, retries, publication and deadlines (911–1098); all scheduler/plan test bodies. |
| `scylla/src/cluster/node.rs` | Address representation, pool inheritance, public observations, host-ID equality/hash (28–305); DNS expansion/fallback/deduplication (328–455); resolver fixtures. |
| `scylla/src/cluster/state.rs` | Topology maps/sharing, pool hints, state construction/reuse, keyspace fallback, tablet maintenance (44–583); public accessors, token/endpoint helpers, agreement connections, optional interop (587–976); representative state-update assertions. |
| `scylla/src/cluster/worker.rs` | Initial route installation/publication (65–197); refresh replies, serialized keyspace propagation, update application, route ordering, initialization wait, listener transitions and status handling (199–847). |
| `scylla/src/policies/host_listener.rs` | Public context accessors, event variants, synchronous listener trait and feature gating (1–101). |

The catalogue's shared test-evidence module `scylla/src/book_tests.rs` was checked
for its schema/agreement includes (73–75, 210–214); the included
`docs/source/schema/schema.md` and `docs/source/statements/schema-agreement.md`
were read.

Callers/callees traced beyond the catalogue ownership:

- `scylla/src/client/session.rs`: actual defaults (469–522), schema-version
  statement (1593–1603), automatic agreement/refresh (1822–1838), explicit refresh
  and snapshot access (2348–2374), one-shot/waiting agreement and complete error
  classification (2535–2832), optional required-host wrapper (2853–2865).
- `scylla/src/client/session_builder.rs`: schema/timeout/periodic setters and
  builder tests at 1613–1629 and 1871–1885.
- `scylla/src/network/connection.rs`: `UNPREPARED` reprepare/resend and fixed
  connection pager entry (1177–1251).
- `scylla/src/client/pager.rs`: `SingleConnectionPagingExecutor::fetch_one_page`
  (551–607), fixed connection/deadline/retry-policy boundary.
- `scylla/src/network/connection_pool.rs`: `is_connected` (266–274), working
  connection snapshots (460–470).
- `scylla-cql/src/frame/response/event.rs`: event vocabulary, schema-kind fallback,
  topology/status parsers, and client-route pair-length/UUID validation (32–446).
- Read related routing and connection specification passages to assign the
  public endpoint/token APIs and keyspace propagation to their existing owners.

## Verified findings and fixes

1. **Public inspection and optional type capabilities were incomplete.**
   `Session::get_cluster_state` calls the in-memory `Cluster::get_state`;
   `state.rs:587–609` uses direct maps/slices, with exact keyspace lookup and
   absent results for missing IDs/names. `Topology::new` collects hash-map values
   (59–64), establishing no ordered-node guarantee. `node.rs:222–238` distinguishes
   connectivity from pool enablement, while `node.rs:292–305` defines equality
   and hashing solely by host ID. `metadata/mod.rs:112`, `197–246`, and `326–337`
   establish Python-only `Peer::Clone`, precise strong-consistency feature gates,
   and exact `ColumnKind` string conversion. Added a public-inspection subfeature,
   Rust-specific type details, and links to routing/keyspace-propagation behavior.

2. **The optional caller-required host agreement API was omitted.**
   `session.rs:2853–2865` exposes
   `await_schema_agreement_with_required_node_external` under both
   `scylla_unstable` and `unstable-csharp-rs`. It delegates directly to the same
   required-host loop used by automatic agreement. `session.rs:2639–2691` and
   `2706–2771` prove the same deadline/error/required-host semantics, with no
   refresh in this manual path; `1822–1838` locates refresh in the automatic path.
   Added this optional API, the `None` host case, and the public UUID/absence/error
   result distinction of ordinary wait/check APIs.

3. **Filter limitations did not cover an already established control connection.**
   Candidate selection calls `is_cc_endpoint_rejected` in
   `cc_establisher.rs:339–349`. In contrast, successful full/topology fetches in
   `metadata/worker.rs:787–815` only apply the topology guard and publish. The
   guard refreshes candidates, while `state.rs:423–489` independently applies
   host filtering to query nodes/pools. Added the verified distinction: a later
   rejected host can lose its query pool and future candidacy while its current
   control connection continues. The partial-fetch integration fixture explicitly
   rejects a node other than the control host (`partial_fetch.rs:104–110`), so it
   does not cover this case.

4. **The server-family timeout statement needed a cache qualification.**
   `control_connection.rs:194–218` returns cached prepared statements before
   `maybe_append_timeout_override` can inspect the current server. The shared
   cache is established once and cloned for later control connections
   (`cc_establisher.rs:119`, `294–300`); `connection.rs:1187–1189` reprepares the
   prepared statement's stored text on `UNPREPARED`. Therefore a cache hit retains
   the original presence/absence of `USING TIMEOUT` even after a server-family
   change. Qualified the defaults table and added this less-common boundary to
   implementation evidence and test-generation notes. This describes source
   behavior, not a claim that mixed server families form a supported cluster.

5. **Existing coverage omitted the known-host DOWN path and builder/book evidence.**
   Read `session/status_change_hints.rs:80–195`: each of three hosts receives two
   sequential hints; the test requires target OPTIONS, then asserts no surplus
   feedback on any host, with one pool connection per host, periodic keepalive
   set to 10,000 seconds, and control traffic excluded. Added this coverage with
   its limits: it does not assert post-probe liveness or UP-refill behavior.
   Added builder tests that check client-timeout storage and periodic mode, while
   preserving the gap for effective timeout derivation/enforcement. Added the
   schema/agreement book examples as API evidence only, explicitly qualifying
   their stale broad prose. Added missing direct regression cases for the newly
   documented public/optional/cache/filter behavior.

The no-resolved-contact-point startup sentence was also reworded to remove an
ambiguity; its behavior was unchanged.

## Other checked behavior

- Defaults and controls agree with `SessionConfig::new`; disabled/minimal/full
  schema paths and keyspace restrictions preserve topology and their documented
  schema scopes.
- Bootstrap resolves all addresses, retries non-timeout DNS errors with port
  9042, deduplicates, shuffles the initial pass, preserves the fallback ordering,
  and separates failed-open startup from dummy-metadata startup.
- Peer-row failure tolerance, aggregate validity checks, advertised address/port
  handling, token fallback, and full-only cluster-name replacement match source.
- Recoverable missing-UDT/key-position errors are keyspace-scoped; parse/strategy
  errors fail the containing fetch. Empty columns, UDT zip truncation, vector
  u16 dimensions, per-table partitioners, and broad `Invalid` compatibility
  fallback remain accurately qualified.
- Full fetches preempt partial futures; event work after full start remains owed;
  schema ticks are batching boundaries. Candidate event draining, repair cadence,
  explicit request acceptance and delayed publication replies match the actual
  branches rather than source comments alone.
- Full/partial merges retain valid keyspaces and refresh handles; a recoverable
  error can replace a pending absence. Status hints are independently coalesced.
  Snapshot sharing, zero-token nodes, pool reuse, initialization waits and
  listener callbacks before publication are distinguished correctly.
- Client-route cross-product filtering/validation, missing-pair deletions, route
  installation before pools, and tablet-batch duplicate suppression match source.
- Agreement snapshots, sequential connections per node, concurrent nodes,
  required coordinator, transient-error taxonomy, and last-completed-check
  precedence at deadline match implementation. Shutdown/cancel-safe channel
  behavior and the explicitly recorded refresh-reply panic remain qualified.

## Test evidence and validation

No Rust, Go, database, proxy, CCM, or doctest suite was executed. Inspected source
assertions in worker/merge-channel/update/fetching/node/state tests and
representative metadata content/configuration/event-flood/partial-fetch,
schema-agreement, and host-listener tests. Confirmed that scheduler tests do not
call `start_due_fetches`, merge tests omit refresh response handles, UDT tests do
not establish publication recovery, vector examples omit boundaries, listener
topology tests validate an event log rather than assert every proposed event,
and one-shot transient-agreement sanity checks do not assert `Some(version)`.
Existing missing-coverage statements remain gaps, not failed test results.

Static validation checked all 44 pinned source links for the exact commit, file
existence and in-range line anchors, all 68 named test references for actual
function declarations, and all local cross-section anchors. `git diff --check`
was run for the two owned files; because these files are untracked in this
workspace, a separate content check also confirmed no trailing whitespace and
final newlines. The report's after-hash was compared with the actual spec bytes.
These checks do not execute the behavior.

A cross-section bad commit URL in `connections.md` at the raw-USE entry was
reported to the parent rather than edited: it contained
`f8b0ee928dc0dcb475247dc82c4e42804b0621`, missing `ce` from the pinned hash. No other
cross-section edit was made. Newly documented optional bindings, server-family
cache reuse, and established-control-host filtering remain source-proven but
without a direct regression in the inspected tests.
