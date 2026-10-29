# Metadata, control connection, and events — round 03

- Section: `metadata-control-events`
- Round: `03` — errors, partial success, failover, retry, and recovery
- Agent identity: `/root/review_metadata_03`
- Source repository: `/extra/scylladb/gocql-rust-reference-f8b0ee9`
- Verified source commit: `f8b0ee928dc0dcb475247dc82cce4e42804b0621`
- Specification: `feature-specs/metadata-control-events.md`
- Before SHA-256: `ce986e4437a045a55fbac6a04f02153d965802f6ef0bcf29b5a4c104c1462013`
- After SHA-256: `f1897252b42b32125d56fe098774c032223f685471174ee77be574261f6d6831`

Read `feature-specs/reviews/REVIEW-INSTRUCTIONS.md`, `feature-specs/README.md`,
and the complete assigned section. This was an independent source audit; prior
reports were not used as evidence. Only the assigned specification and this
report were changed. No Go or Rust implementation was changed, and no child
agents were spawned.

## Source inspected

All Rust paths below are relative to the pinned source repository. Line ranges
record the relevant source inspected, not execution of that source.

| File | Symbols and behavior inspected |
| --- | --- |
| `scylla/src/cluster/metadata/cc_establisher.rs` | `establish_cc_and_fetch_metadata`, `try_establish_on_nodes`, `update_known_peers`, `is_cc_endpoint_rejected`, `make_control_connection` (151–450): candidate order, fallback error precedence, dummy startup, rejection, registration |
| `scylla/src/cluster/control_connection.rs` | `MetadataRequestTimeouts`, statement cache and `query_iter` (25–225); `wait_for_event` (229–262): client/server timeout scope, failure sender shutdown |
| `scylla/src/cluster/metadata/fetching.rs` | `query_metadata`, `query_topology`, `validate_peers`, `query_peers`, `create_peer_from_row` (162–435); client-route conversion/query/filtering (461–648); partial schema and keyspace construction (651–899); UDT parsing/dependency handling (919–1105); column/key handling (1391–1628); Scylla compatibility and strategy parsing (1792–1995) |
| `scylla/src/cluster/metadata/worker.rs` | Fetch plans, retries and scheduling (149–434); fetch-slot completion (490–507); establishment event draining (533–633); repair and ordinary loops (660–899); event dispatch (911–1015); per-keyspace retry scheduling and publication (1019–1068) |
| `scylla/src/cluster/metadata/update.rs` | Full/partial merging, response retention, hints, pair filtering and keyspace precedence (103–400); valid-keyspace retention test assertions (882–913) |
| `scylla/src/cluster/metadata/merge_channel.rs` | Capacity-one construction, modify, sender drop, cancel-safe receive (72–165) |
| `scylla/src/cluster/metadata/mod.rs` | Fetch modes, recoverable keyspace-error enum, metadata/public schema types and feature gates (40–265); route merging and dummy metadata (419–477) |
| `scylla/src/cluster/node.rs` | DNS timeout/default-port retry, empty resolutions, concurrent contact-point expansion and deduplication (330–455) |
| `scylla/src/cluster/worker.rs` | Channels, bootstrap routes, initial publication and refresh response (63–217); tablet receive batching (303–324); update application/routes/listener ordering and connectivity transitions (418–800) |
| `scylla/src/cluster/state.rs` | Ring-node pool initialization (141–144); full/partial snapshots, node reuse, keyspace fallback (235–534); maintenance and locator rebuilding (536–588); usable-node selection (748–786); tablet duplicate suppression (846–893) |
| `scylla/src/routing/locator/tablets.rs` | Unknown/removed/recreated replica maintenance (801–846), table/view membership maintenance (986–1049) |
| `scylla/src/client/session.rs` | Actual defaults (469–520), schema-version statement (1593–1600), automatic agreement/refresh (1822–1838), agreement taxonomy/check/wait/per-node attempts (2535–2831) |
| `scylla/src/client/pager.rs` | `SingleConnectionPagingExecutor::fetch_one_page` (528–606): fixed connection, fallthrough policy, deadline |
| `scylla/src/network/connection.rs` | `execute_raw_with_consistency` (1105–1222): automatic reprepare and one resend; timeout/server-error test bodies (2868–3044) |
| `scylla-cql/src/frame/response/event.rs` | Event vocabulary (32–132), schema change parsing (274–277) |

## Verified findings and fixes

1. **Accepted topology can be incomplete after a query failure.** The section
   mentioned skipped query-stream errors without explaining that this also
   includes PREPARE/first-page failures and later-page failures. `query_peers`
   converts errors from both complete query streams to `(None, None)`
   (`fetching.rs:289–353`) and still returns surviving rows (`357–372`).
   `validate_peers` only requires a nonempty list and at least one token-bearing
   node (`229–238`), and successful topology replaces the old topology
   (`state.rs:362–377,423–488`). Added the possibility of publishing a reduced
   peer list and the absence of retry triggered by the tolerated error itself.
   Independent broken-connection notifications still follow the documented
   recovery path.

2. **Refresh success does not mean every keyspace was freshly fetched.**
   Recoverable keyspace errors are carried inside successful metadata
   (`fetching.rs:863–879`), scheduled for a later tick (`metadata/worker.rs:
   1019–1037`), and resolved using old data or omission (`state.rs:508–531`).
   Publication then replies with `Ok(())` (`cluster/worker.rs:514–519`). Added
   the explicit stale/omitted-keyspace success caveat and narrowed the refresh
   invariant to a full fetch *started* after request acceptance, allowing the
   documented retained keyspace data.

3. **A failed schema reread also delays drops in the same partial batch.**
   `fetch_schema_update` collects dropped entries first, but the altered-keyspace
   query uses `await?` before returning any update (`fetching.rs:686–716`). An
   outer query/parse error therefore discards that batch, including its drops;
   the worker schedules a full fetch (`metadata/worker.rs:835–840`). Added this
   boundary so the query-free drop rule does not imply that drops are published
   independently of failed rereads batched with them.

4. **Client-route error scope and validation were omitted.** Route ports use
   checked `i32` to `u16` conversion; zero and nulls are accepted, and no check
   requires either port to be present (`fetching.rs:483–513`). The entire query
   stream is decoded and converted with `try_collect` before the relevant pair
   filter (`537–560,640–648`), so a malformed cross-product row can fail a partial
   read even though that pair would never be applied. There is no missing-table
   fallback. A route failure fails a full `try_join!` (`171–192`), or schedules
   full work after a partial error (`metadata/worker.rs:820–825`). Added these
   validation and all-or-error publication boundaries.

5. **Metadata request retry boundaries were omitted.** Idempotent metadata
   statements do not receive ordinary session policy retries: the fixed-connection
   page executor uses `FallthroughRetryPolicy` and no speculation
   (`pager.rs:558–569,602–606`). However, `execute_raw_with_consistency` can
   reprepare and resend once on `UNPREPARED` (`connection.rs:1177–1219`), inside
   the same page attempt and deadline. Added both rules, a pinned connection-layer
   evidence link, and the existing terminal-server-error test as coverage.

## Whole-section checks without additional findings

- Defaults, full/minimal/disabled schema content and restrictions, metadata
  timeout derivation, DNS retry/omission and deduplication match the inspected
  branches. Initial fetch failure permits dummy metadata only after a connection
  opens; later failures iterate candidates and may resolve original contacts.
- Rejected peers may supply metadata without a retained control connection.
  Candidate topology updates precede publication, and the bootstrap contact-point
  filter limitation is documented accurately. Repair attempts preserve an already
  accepted refresh request until success or an attempt error; background repair
  continues after replying with that error.
- Partial slots can coexist; full work cancels/subsumes them; events received
  during establishment/full work remain owed. Schema batching, latest-event
  precedence over keyspace retry, timer reset/advancement, immediate repair and
  start-to-start one-second cadence match the worker.
- Empty/null/malformed tokens, advertised peer addresses, zero-token public
  membership, keyspace-level versus outer schema errors, UDT dependency handling,
  primary-key positions, compatibility fallback and strategy parsing agree with
  the section. No claim that schema reads are a database-consistent transaction
  was introduced.
- Merging retains refresh replies and valid older keyspaces, while full/topology
  replacements and independent partial aspects follow documented precedence.
  Snapshots retain mutable pool handles; no-op topology/tablet updates, reused
  nodes, route installation, initialization waits and listener-before-publication
  timing match source.
- Host additions/removals/address changes, connectivity suppression and filter
  limitations are consistent. Tablet maintenance retries unresolved replicas and
  discards unresolved/removed-node entries, including table/view maintenance.
- Schema agreement queries usable nodes concurrently and connections within a
  node sequentially, prioritizes non-broken errors on total node failure, requires
  a successful automatic coordinator, and uses the stated transient/error
  taxonomy. The last completed check controls deadline error selection. Automatic
  agreement precedes optional refresh; manual agreement does not refresh.
- Merge-channel receive cancellation/final-value behavior and the documented
  pending-refresh shutdown panic match their implementations. The subfeature list,
  invariants and Go test gaps remain separate from claims of implemented Go APIs.

## Coverage and validation

All 67 named Rust test symbols in the final section resolve to functions in the
pinned source. This was a static symbol check. Representative assertions inspected
include `merging_metadata_reuses_pending_keyspace_on_error`, the schema-drop event
test and its intentionally commented-out real-DDL query-count check, replacement
under an event flood, transient-overload agreement recovery, and first/later-page
timeouts. `connection_execute_iter_server_error` asserts both the server error
and exactly one failing EXECUTE for first and later pages (`connection.rs:
2982–3000,3025–3042`); it does not test the `UNPREPARED` exception.

Added explicit missing regressions for tolerated topology query/page errors,
malformed irrelevant route pairs, and failed mixed schema-drop/reread batches.
The listed test catalog does not establish all failure taxonomy or timing cases.
No Rust, Go, database or proxy tests were executed. Final static validation checked
the specification's named test symbols, local relative links, pinned source links,
trailing whitespace and recorded SHA-256. No cross-section edit was required.
