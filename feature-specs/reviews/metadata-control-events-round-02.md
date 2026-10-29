# Metadata, control connection, and events — review round 02

- Section: `feature-specs/metadata-control-events.md`
- Round: 02 — defaults, exact conditions, limits, ordering, and fallback boundaries
- Agent identity: `/root/review_metadata_02`
- Reference repository: `/extra/scylladb/gocql-rust-reference-f8b0ee9`
- Verified source commit: `f8b0ee928dc0dcb475247dc82cce4e42804b0621`
- Before SHA-256: `0e9e6acf5f8f9a58102c903bc8fda77031b39c91fa2852aea8a6cdd141081e9a`
- After SHA-256: `ce986e4437a045a55fbac6a04f02153d965802f6ef0bcf29b5a4c104c1462013`
- Scope: reviewed the complete section and edited only the assigned spec and this
  report. No Go or Rust source changed. Prior reports were not used as evidence.

## Source inspected

Read `feature-specs/README.md`, the complete assigned specification, and
`feature-specs/reviews/REVIEW-INSTRUCTIONS.md`. Source references below are relative
to the verified reference repository; ranges describe inspected implementation,
not executed tests.

| File | Symbols and relevant lines inspected |
| --- | --- |
| `scylla/src/client/session.rs` | `SessionConfig::new` 469–522; tablet-channel constant/construction 73, 1442; `get_schema_version_statement` 1593–1602; automatic agreement 1822–1838; request response handling 2518–2530; agreement classification, waiting, checking, per-node reads 2535–2829 |
| `scylla/src/cluster/control_connection.rs` | Timeout defaults/derivation 25–67; configuration, prepared cache, timeout clause and paged query setup 81–229; failure/event handling 232–271; server-timeout proxy test 294–498 |
| `scylla/src/cluster/node.rs` | Lookup timeout/default-port behavior and concurrent contact resolution 326–455; resolver/deduplication tests 500–654 |
| `scylla/src/cluster/metadata/cc_establisher.rs` | Initialization, candidate iteration, initial dummy fallback, rejected metadata, re-resolution, filtering, and registration 98–462; topology-update guard following those methods |
| `scylla/src/cluster/metadata/mod.rs` | Metadata/schema types and fetch modes; client-route representation/merge; `Metadata::new_dummy` 452–482 |
| `scylla/src/cluster/metadata/fetching.rs` | Type resolution 74–146; full/topology fetch and validation 171–251; peer streams and token fallback 285–437; client-route parsing and full/partial filtering 449–645; schema filtering 665–757; full/minimal schema 759–901; UDT parsing/construction/toposort 910–1120; table/view assembly 1285–1388; column/key validation and type parsing 1391–1766; partitioners, compatibility, consistency and replication strategy 1770–2000; UDT and parser tests |
| `scylla/src/cluster/metadata/worker.rs` | Entire production state machine 1–1098: plans, per-type slots, `start_due_fetches`, establishment event draining, repair, full/partial outcomes, event dispatch, response publication, `deadline_after`; unit-test plans and polling assertions |
| `scylla/src/cluster/metadata/update.rs` | Production merge paths, refresh handles, status hints, route-pair filtering and keyspace precedence 1–408; referenced unit-test assertions |
| `scylla/src/cluster/metadata/merge_channel.rs` | Slot/notification/drop/receive behavior 50–198; referenced shutdown/cancellation tests |
| `scylla/src/cluster/worker.rs` | Initial route application, listener calls, initialization and channels 80–215; tablet/event/keyspace loop 303–417; metadata application/publication 418–561; topology/connectivity listener logic 565–807 |
| `scylla/src/cluster/state.rs` | Shared-object test and pool wait 60–145; hint address matching 148–218; full/partial state building, node reuse and keyspace preservation 235–533; maintenance/locator 535–583; public metadata accessors and connection iteration 585–838; tablet batch update 846–897; referenced state unit assertions |
| `scylla/src/routing/locator/tablets.rs` | Unknown-replica resolution/removal and recreated nodes 801–846; table/view maintenance 986–1045 |
| `scylla/src/policies/host_listener.rs` | Feature gating, context, vocabulary and synchronous callback trait 1–105 |
| `scylla-cql/src/frame/response/event.rs` | Event variants, schema-change kinds, schema target deserialization, and event dispatch |
| `scylla/src/client/pager.rs` | `SingleConnectionPagingExecutor::fetch_one_page` 551–607; fixed-connection pager initialization 1118–1160 |
| `scylla/src/network/connection.rs` | Initial prepare 723–773; fixed-connection `execute_iter` 1231–1251; first/later page deadline tests 2873–2951 |
| `scylla-cql/src/utils/parse.rs` | `parse_u16` 84–91 |
| `scylla/src/client/session_builder.rs` | Direct assignment of agreement/metadata intervals 599–601, 1480–1483 |

Also inspected the referenced test declarations/assertions in
`scylla/tests/integration/metadata/{partial_fetch,event_flood,configuration,contents}.rs`,
`scylla/tests/integration/session/schema_agreement.rs`, and
`scylla/tests/integration/ccm/host_listener.rs`; deeper inspection is described below.

## Verified findings and fixes

1. **The metadata timeout needed an execution boundary.** The prior table gave
   the numeric timeout without distinguishing a page from an entire metadata
   operation. `control_connection.rs:175–229` prepares a cache miss first, then
   installs the client timeout on the prepared clone. `pager.rs:551–607` creates
   a new timed execution for each page. `connection.rs:723–773` performs initial
   PREPARE directly. Updated the table and implementation evidence to say that
   the timeout is per execution page, excludes initial PREPARE, and is not an
   overall metadata-fetch deadline. Also recorded the `as_millis()` truncation
   used by the server timeout clause at `control_connection.rs:159–172`.

2. **DNS fallback was described too narrowly and retry order was omitted.**
   `node.rs:356–367` retries every non-timeout error with `(original_string, 9042)`
   and returns the first error if that retry fails; it does not first determine
   whether the string lacks a port. Both lookups use their own timeout.
   `resolve_contact_points_inner` concurrently resolves hostnames and sorts/dedups
   endpoints. The second candidate pass in `cc_establisher.rs:223–237` consumes
   that order directly, unlike the shuffled known-peer pass at 184. Corrected
   these conditions and documented concurrent resolution and fallback order.

3. **Control replacement fallback and candidate-event retention needed narrower
   conditions.** `try_establish_on_nodes` returns `Ok((None, metadata))` when it
   retains valid metadata from rejected peers (`cc_establisher.rs:352–355`), so
   `establish_cc_and_fetch_metadata` returns before contact-point re-resolution.
   Only a pass returning an error reaches that fallback. Rejected/failed
   candidates drop the `FetchPlan` payload; `worker.rs:553–558` carries a plan
   forward only with the retained candidate. Status hints already sent through
   `fetch_on_candidate`/`handle_server_event` are independent. Qualified both
   the fallback trigger and the claim that establishment events carry forward.

4. **A periodic deadline does not guarantee one fetch per tick.**
   `worker.rs:349–437` advances a due deadline even if a full fetch prevents
   starting another fetch. The timer branch at 887–891 is disabled while full
   work is active, but a server event can wake the loop and consume an overdue
   tick. A completion can instead reveal an overdue deadline and start work.
   Deadlines restart from processing time, not from a fixed missed-tick sequence.
   `deadline_after` at 1096–1098 falls back to the current instant on addition
   overflow. Replaced “every tick” with the actual due-work behavior and added
   the missing timer and overflow boundaries.

5. **Full-schema validation is permissive when columns or keys are absent.**
   `query_tables`/`query_views` at `fetching.rs:1285–1388` synthesize empty table
   metadata if no corresponding column schema was assembled. Key validation at
   1532–1553 accepts an empty list, while rejecting negative, duplicate or gapped
   positions when present. The Thrift filter at 1416–1418 checks the exact type
   string `empty`, not the column's name. Documented those boundaries so “full”
   cannot be read as a guarantee of a nonempty partition key or complete columns.

6. **UDT length and vector-dimension constraints were unspecified.**
   `fetching.rs:1004` zips names with already-parsed field types without a length
   check. Vector parsing at 1686–1703 delegates to unsigned 16-bit decimal
   parsing (`scylla-cql/src/utils/parse.rs:84–91`), which accepts zero. Added the
   shorter-list truncation and the 16-bit/zero boundary. This describes metadata
   parsing only; it does not assert that such malformed definitions can be
   created through normal server DDL.

Updated two evidence anchors to the named functions (`fetch_client_routes_update`
at 604 and `query_keyspaces_scylladb_specific` at 1815), added the page-executor
entry point, and added relevant existing tests and explicit missing regressions.

## Checked behavior with no additional correction

- Defaults: schema/full schema enabled; unrestricted keyspaces; affected-keyspace
  periodic mode; 60-second refresh and agreement deadline; 200-millisecond
  agreement interval; 30-second server override; derived 31-second client timeout
  even for Cassandra; 5-second DNS lookup; automatic agreement/refresh enabled;
  host-listener feature/configuration gate. Explicit override and saturating
  client-timeout arithmetic match the source.
- Bootstrap: direct contact endpoints, sequential shuffled known-peer candidates,
  registration before fetch, initial-only dummy data after a connection opens,
  no dummy fallback when every opening fails, fresh UUID/synthetic token data,
  host-filter candidate replacement before publication, and the contact-endpoint
  filter limitation remain accurate.
- Metadata: concurrent full-fetch branches; advertised RPC addresses for both
  local and peers; skipped malformed rows/null IDs; token fallback; nonempty
  aggregate validation with zero-token nodes retained; full-only cluster name;
  schema-disabled/minimal/full distinctions; replication strategy parsing;
  recoverable keyspace errors; absent-keyspace deletion; compatibility fallback
  on the precise nested `Invalid` error; exact `global` consistency recognition.
- Scheduling/merge: one partial per type; different partial types coexist; full
  work preempts partial futures; latest per-keyspace schema event and per-address
  hint; deduplicated route pairs; tick-gated schema work; failed partial implies
  full work; failed full implies replacement; immediate/one-second repair cadence;
  refresh reply preservation and publication ordering; capacity-one merge slot;
  valid pending/published schema preservation across recoverable errors.
- State: immutable snapshot replacement with mutable pools; node identity/reuse
  rules; route application before pool construction; DOWN hints on old topology
  and UP hints on replacement; unique-ring-node pool initialization; shared-node
  object detection; no snapshot for unchanged partial topology/duplicate tablets;
  schema partials still rebuild; synchronous pre-publication listener order.
- Routes/tablets: full empty route filter versus partial membership filtering;
  cross-product exclusion and missing-pair deletion; 8192-item feedback capacity
  and batch; unknown replica handling; tablet table/view maintenance and node
  reference replacement.
- Agreement/shutdown: concurrent nodes and sequential per-node connections;
  required successful coordinator; non-broken error precedence; no vacuous
  agreement; immediate check then interval; transient/nontransient taxonomy;
  last completed check determines deadline error; automatic refresh only after
  successful automatic waiting; manual agreement does not refresh; final pending
  merge delivery and the documented runtime-shutdown refresh panic limitation.

## Test coverage and validation

- All 60 preexisting named Rust test references resolve to function declarations.
  Inspected their assertion sites; read complete representative bodies/helpers
  for UDT sorting, fetch-plan polling, resolver expansion, partial fetches,
  missed-event full fetching, metadata event floods, minimal schema, automatic
  schema refresh, and first/later page deadlines. The six new named references
  also resolve (four resolver tests and two fixed-connection deadline tests).
- The added resolver tests inject a resolver, so they establish expansion,
  deduplication and failure omission, not the actual lookup/default-port retry.
- Fixed-connection deadline tests use a local proxy and pause the clock only
  after setup. They establish first/later page failures; neither proves a
  PREPARE deadline or a total metadata-fetch deadline. The existing integration
  timeout test checks the emitted server-side clause, as the spec already says.
- `full_fetch_subsumes_partial_work` tests the fetch plan; it does not exercise
  `start_due_fetches` against a controlled clock. The missed-event integration
  test verifies eventual full refresh, not the newly documented long-fetch tick
  interleaving. Added explicit gaps for that interleaving, overflow, unequal UDT
  field lists, and missing full-schema column rows.
- Verified the partial-schema test's manually injected final keyspace drop and
  its commented-out real-server drop-order query-count assertion. Kept that
  coverage qualification. Existing Go tests/benchmarks remain explicitly missing.
- Checked pinned source-link paths/line bounds, named test existence, spec/report
  trailing whitespace, and final hashes using local scripts. No Rust or Go test
  suite, database integration test, or benchmark was executed.

Remaining gaps are the explicitly named missing tests and normal source-only
review limits. Sent the parent a cross-section note to reconcile per-page timeout
scope with configuration/execution; no other section was edited.
