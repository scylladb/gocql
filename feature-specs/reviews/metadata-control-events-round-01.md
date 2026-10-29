# Metadata, control connection, and events — round 01

- Section: `feature-specs/metadata-control-events.md`
- Round: `01` — trace claims to implementation; correct factual mismatches.
- Agent identity: `/root/review_metadata_01`
- Rust source: `/extra/scylladb/gocql-rust-reference-f8b0ee9`
- Source commit, verified with `git rev-parse HEAD`: `f8b0ee928dc0dcb475247dc82cce4e42804b0621`
- Before SHA-256: `208cdcd8cf5e14320363644e59093227b82b4fa2f3ca8cddff0669adfd098d17`
- After SHA-256: `0e9e6acf5f8f9a58102c903bc8fda77031b39c91fa2852aea8a6cdd141081e9a`

Read `feature-specs/README.md`, `feature-specs/reviews/REVIEW-INSTRUCTIONS.md`,
and the complete assigned section. Independently traced the implementation and
read test assertions. Changed only this report and the assigned specification;
no Go or Rust implementation changes and no child agents.

## Findings and fixes

1. **Agreement deadline errors depend on the last completed check.** The old
   wording said the latest completed error was returned when available, which
   incorrectly includes an error followed by a completed version mismatch.
   `await_schema_agreement_with_required_node` sets the saved outcome to
   `Some(Ok(()))` on `Ok(None)` and returns `Timeout` for that outcome at the
   deadline. Corrected the contract and test-generation notes to distinguish
   error, disagreement, and no completed check.
   Evidence: `scylla/src/client/session.rs:2650–2684`.

2. **Listener execution and filtering have narrower conditions than stated.**
   `Cluster::new` calls `handle_topology_changes` before spawning the workers,
   so callbacks also run synchronously in the session-construction future.
   Address-change candidates test `old_node.is_enabled()` but never test the
   new node's enabled state. A newly rejected address can therefore still
   generate `AddressChanged`. Corrected both statements and distinguished the
   old-node removal filter from the new-node addition filter.
   Evidence: `scylla/src/cluster/worker.rs:151–185,571–620,660–720`.

3. **The public cluster-name fallback and partial-update behavior were omitted.**
   `query_topology` discards the returned cluster name, partial state construction
   clones the previous value, and `ClusterState::cluster_name` returns `""` when
   the optional value is absent. Added these boundaries, including dummy startup.
   Evidence: `scylla/src/cluster/metadata/fetching.rs:206–224`,
   `scylla/src/cluster/state.rs:341–347,587–589`, and
   `scylla/src/cluster/metadata/mod.rs:452–477`.

4. **An absent keyspace row has a distinct deletion outcome.** A partial reread
   of an altered keyspace produces `FetchedKeyspace::Absent` if the query returns
   no keyspace row. `updated_keyspaces` then removes it. This differs from the
   recoverable error path that retains old metadata. Added the missing behavior;
   a keyspace-drop event is not required for this removal.
   Evidence: `scylla/src/cluster/metadata/fetching.rs:700–716` and
   `scylla/src/cluster/state.rs:396–421`.

5. **Tablet maintenance can discard unresolved tablets.** The old description
   mentioned omitted unknown replicas and subsequent resolution but omitted the
   failure boundary: maintenance removes the entire tablet if any replica still
   cannot be resolved. Tablets referring to removed nodes are also discarded.
   Maintenance includes materialized views as well as tables. Added these
   outcomes and retained the routing section's ownership of conflict rules.
   Evidence: `scylla/src/routing/locator/tablets.rs:801–845,986–1047` and
   `scylla/src/cluster/state.rs:526–556,846–904`.

6. **Scheduling test claims conflated plans with running fetches.**
   `partial_work_of_different_types_coexists` and
   `full_fetch_subsumes_partial_work` exercise `FetchPlan`, not cancellation of
   running fetch futures. Narrowed that row to pending work and added
   `completed_partial_fetch_keeps_the_others_in_flight` for its actual
   `PendingFetches` behavior.
   Evidence: `scylla/src/cluster/metadata/worker.rs:1194–1230,1273–1350`.

7. **The timeout integration test checks server clauses, not client deadlines.**
   `test_custom_metadata_timeouts` inspects outgoing `QUERY`/`PREPARE` text for
   `USING TIMEOUT` with default, disabled, and explicit settings. It does not
   delay responses and assert client timeout enforcement. Narrowed the test
   catalog statement accordingly.
   Evidence: `scylla/tests/integration/metadata/configuration.rs:65–237`.

## Source inspected and behaviors checked

All paths below are relative to the verified Rust checkout. The whole section,
its subfeatures, invariants, implementation table, and test catalog were checked.

| Files and key symbols inspected | Checked behavior |
| --- | --- |
| `scylla/src/client/session.rs:469–525,1593–1602,1822–1841,2535–2834`; `SessionConfig::new`, automatic/manual agreement helpers | Fetch/timeout/agreement defaults; consistency ONE and idempotence; concurrent per-node checks, sequential per-connection fallback, required coordinator, nontransient error classification, last-check deadline semantics, automatic refresh and manual non-refresh. |
| `scylla/src/cluster/node.rs:330–458`; hostname/contact-point resolution | Timeout versus non-timeout default-port retry, all resolved addresses, duplicate removal, logged failed resolutions, literal contact points. |
| `scylla/src/cluster/metadata/cc_establisher.rs:100–490`; `new`, `establish_cc_and_fetch_metadata`, `try_establish_on_nodes`, `is_cc_endpoint_rejected`, `TopologyUpdateGuard` | Sequential known-peer candidates, later contact-point fallback, dummy initial metadata after fetch failure, valid rejected-candidate metadata retention, filtered candidate replacement before publication, contact-point rejection limitation, 32-entry event channel and event registration. |
| `scylla/src/cluster/metadata/mod.rs:41–477`; fetch enums, public metadata types, `Metadata::new_dummy` | Schema levels, periodic fetch modes, recoverable error variants, exposed schema fields, unstable consistency field, direct versus translatable endpoints, generated IDs/tokens and absent dummy schema/name/routes. |
| `scylla/src/cluster/metadata/fetching.rs:40–1115,1284–2000`; metadata/topology/schema/route queries, UDT sorting, column/type/strategy parsing | Concurrent full-fetch aspects; peers/local advertised addresses and token fallback; topology validation; restricted/disabled/minimal/full schema; absence versus errors; UDT cycles/missing definitions; key order validation; column kinds and Thrift omission; Scylla compatibility fallback; replication parsing; route pair filtering and missing-route deletion. |
| `scylla/src/cluster/control_connection.rs:25–263`; timeout calculation, prepared cache, `query_iter`, `wait_for_event` | 1024-row paging, idempotence, original-text cache shared across replacements, execution-clone client timeout, Scylla-only server clause, sender-loss shutdown signal. |
| `scylla/src/cluster/metadata/worker.rs:138–1100`; `FetchPlan`, `PendingFetches`, establishment/normal/repair loops, event dispatch | One partial fetch per type, cross-type concurrency, tick batching, full preemption, post-start event work, request acceptance guard, partial-to-full escalation, full failure replacement, repair cadence, retained refresh request, candidate-fetch event draining, keyspace retry precedence, known shutdown reply issue. |
| `scylla/src/cluster/metadata/update.rs:64–414`; full/partial merging and `ClientRoutesUpdate` | Last topology/result precedence, preserving pending valid keyspaces and refresh replies, per-keyspace drop handling, disjoint route pairs, explicit route deletion, latest status hint per address. |
| `scylla/src/cluster/metadata/merge_channel.rs:45–195`; `modify`, `recv`, endpoint drops | One mergeable pending object, receiver-drop rejection, cancelled receive preservation, last-value delivery after sender drop. |
| `scylla/src/cluster/worker.rs:82–226,303–805`; initialization, `apply_metadata_update`, host event handlers | Queue sizes, routes before pools, old-state DOWN hints and new-state UP hints, initialization waits, publication before refresh success, callback ordering, connectivity deduplication and unknown-ID handling, snapshot storage. |
| `scylla/src/cluster/state.rs:43–217,235–649,746–904`; topology/state constructors, schema resolution, tablet updates | All known nodes including disabled/zero-token nodes; object/pool reuse; broadcast-address hints; unchanged topology/tablet suppression; schema map replacement; retained snapshots with mutable pools; token-ring-only pool initialization; offloaded locator rebuild; tablet batch sequencing. |
| `scylla/src/network/connection_pool.rs:280–315,753–790`; immediate refill and keepalive handling | UP resets refill backoff and reschedules; DOWN requests current-connection probes rather than assigning liveness. |
| `scylla/src/routing/locator/tablets.rs:801–845,986–1047`; maintenance | Unknown replica resolution/removal, removed-node tablet eviction, recreated-node substitution, table/view maintenance. |
| `scylla/src/policies/host_listener.rs:18–96`; event/context/trait definitions | Public callback vocabulary, synchronous trait method, unstable exposure. |
| `scylla-cql/src/frame/response/event.rs:32–459`; event deserializers | Supported topology/status/schema targets, invalid schema change kinds, unknown encoding errors, route pair lengths and UUID parsing. |

## Test coverage and remaining gaps

- Verified all 59 original named Rust test functions exist. After adding the
  in-flight partial-fetch test, validated all 60 references. Every pinned source
  path and line anchor resolves within the local pinned checkout. Checked the
  edited Markdown for trailing whitespace.
- Read representative assertions in the worker, update, merge-channel, fetching,
  and state unit tests, including plan coalescing, completed-slot removal,
  preservation of valid keyspaces, route cross-product exclusion, cancellation,
  UDT ordering/cycles, type/consistency parsing, changed filters, zero-token
  topology changes, and within-batch tablet comparisons.
- Inspected `metadata/partial_fetch.rs:66–341` and
  `metadata/event_flood.rs:74–328`: query-count assertions, injected keyspace-drop
  removal, missing-event full mode, and stalled fetches with event floods during
  refresh, build, and replacement. The disabled real-DDL drop-count assertion
  remains accurately qualified in the specification.
- Inspected configuration/content assertions in
  `metadata/configuration.rs:65–521` and `metadata/contents.rs:95–629`, agreement
  scenarios in `session/schema_agreement.rs:100–490`, and listener validation and
  scenarios in `ccm/host_listener.rs:59–221,351–638`. These support the catalog's
  content, normal lifecycle, filter, unreachable-coordinator, transient-error,
  required-host, API, and refresh claims. CDC/view/tablet-specific tests have
  Cassandra exclusions or server-capability checks; coverage is conditional.
- No dedicated assertion was found in the inspected tests for a transient
  agreement error followed by a completed disagreement at the deadline, callback
  execution during construction, or address change combined with newly filtering
  that node. The listener test file explicitly lists IP changes as future work.
  The named plan tests do not establish running-full-fetch cancellation timing,
  and the named metadata timeout tests do not establish client deadline behavior.
- The existing stated gaps for shutdown refresh reply loss, filtered bootstrap
  control connections, and overlapping drop/recreation remain gaps. No exhaustive
  absence-of-tests claim is made beyond the inspected evidence.
- No Rust or Go tests were executed. This was a documentation/source review;
  the proposed Go test gates remain marked missing. No cross-section conflict
  requiring edits outside the assigned file was found.

Result: seven verified corrections/omissions fixed, with the remaining checked
contracts supported by the source above.
