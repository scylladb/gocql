# Connections review, round 06

- Section: `feature-specs/connections.md`.
- Round: 06 — omitted public features, optional features, and less common paths;
  the complete section was independently checked as well.
- Agent identity: `/root/review_connections_06`.
- Reference checkout: `/extra/scylladb/gocql-rust-reference-f8b0ee9`.
- Source commit, verified with `git rev-parse HEAD`:
  `f8b0ee928dc0dcb475247dc82cce4e42804b0621`.
- Before SHA-256:
  `6f1f2944af0ba7dfdfd973c7cfb85f7b850f3f20e41c732a7e52d3e6a3826a11`.
- After SHA-256:
  `90f81be77f1b6ff8d6e2aebc459816f8c8798b1d625051f30b410d15d672745a`.

## Scope and source actually inspected

Read `feature-specs/reviews/REVIEW-INSTRUCTIONS.md`, `feature-specs/README.md`, the
complete connections specification, and `rust-source-catalog.json`. This round
used the pinned implementation and test assertions as evidence, not prior review
reports. No Go or Rust implementation was edited, and no child agents were used.

The catalog assigns four production modules and one test-evidence module to
connections. Their relevant surfaces were enumerated and traced as follows:

| Catalog module | Inspected surface and destination |
| --- | --- |
| `scylla/src/network/mod.rs:1–24` | Public `PoolSize` and `WriteCoalescingDelay` exports; private connection/pool abstractions and TLS-module boundary. |
| `scylla/src/network/connection.rs:70–2646` | Request IDs and cancellation, configuration conversion, socket/TLS setup, OPTIONS/STARTUP/authentication/REGISTER, discovery and compression, all query/prepare/execute/batch entry points, USE, router/reader/writer/orphaner/keepaliver, event dispatch, ScyllaDB detection, tablet feedback, source-port retry loop, socket options, handler map and keyspace validation. Statement preparation/repreparation, metadata IDs, batch handling and paging remain owned by execution/protocol; tablet feedback remains owned by routing/metadata. |
| `scylla/src/network/connection_pool.rs:38–1594` | Both public sizing modes; all pool observations, selection and snapshot helpers; initialization and hint APIs; refiller scheduling, admission, excess connections, resharding and suppression; endpoint updates; host-ID diagnostics; error monitors; USE fanout/deadline/aggregation; notifications and metric call sites. |
| `scylla/src/policies/reconnect.rs:1–246` | Public factory/session traits, exponential and constant policies, jitter and limit checks, and hidden reporting hooks. The reporting hooks are accounted for by configuration/security/observability. |
| `scylla/src/book_tests.rs` | Generated documentation-test inventory, including connecting, compression, authentication, TLS, client routes and USE keyspace. Read the referenced `docs/source/statements/usekeyspace.md`; documentation examples are not counted as executed tests. |

Relevant modules assigned to neighboring sections were also followed:

- `scylla/src/cluster/node.rs:114–290`: pool creation, disabled nodes, endpoint
  inheritance, public `sharder`, `is_scylladb`, `is_connected` and `is_enabled`,
  and pool delegates.
- `scylla/src/routing/sharding.rs:11–307` and its tests: range validation, public
  sharder/source-port helpers, private custom-range helpers and SUPPORTED parsing.
- `scylla/src/client/session.rs:469–671,1410–1488,1775–1789,2325–2339,2395–2408,2496–2534`:
  effective defaults, validation, optional-policy selection, connection-config
  mapping, raw USE handling, explicit USE, accessor and post-request effects.
- `scylla/src/client/session_builder.rs`: local IP, source-port range, TCP/CQL
  keepalive, connect timeout and write-coalescing setters; `policies/mod.rs:25–28`
  and `scylla/Cargo.toml` establish the reconnect-policy gates.
- `scylla/src/cluster/worker.rs:81–82,220–237,342–411,804–847`: keyspace queue,
  serialized fanout, desired state and success aggregation.
- `scylla/src/cluster/state.rs:750–805`: working-connection snapshot collection
  and omission of unavailable pools by its higher-level iterator.
- `scylla/src/client/pager.rs:1192–1221`: first SET_KEYSPACE response handling.
- `scylla/src/cluster/control_connection.rs:148–172` and
  `scylla/src/client/driver_config.rs:715–750`: consumers of negotiated product
  detection for metadata timeouts and reporting.
- `scylla-cql-core/src/frame/protocol_features.rs`: extension parsing, presence
  versus parsed-value signals and STARTUP opt-ins.
- `scylla/src/network/tls.rs:1–120`: optional backend dispatch and shared context
  conversion; TLS verification/resumption details remain in the security spec.
- `scylla/src/errors.rs:611–627`: exact top-level source-port retry classifier.

## Verified findings and fixes

1. **Public connection-derived node observations were missing.**
   `Node::is_scylladb` delegates to the pool, which selects its first published
   connection (`node.rs:199–208`, `connection_pool.rs:324–334`).
   `Connection::is_to_scylladb` checks parsed sharding/port, LWT mask, rate-limit
   code, tablet v1/v2 and metadata-ID support (`connection.rs:2005–2015`). A host
   ID is not part of that condition. Added a named subsection specifying unknown
   results for unavailable pools, the single-connection observation boundary,
   the difference from `sharder()`, and control-connection consumers. Added two
   existing integration targets and an explicit missing boundary-test target.
   Flagged the catalog's assignment of `node.rs` to metadata to the parent;
   no other agent's specification was edited.

2. **The pool's graceful source-port failure description did not distinguish
   public helper failure behavior.** The public draw helper uses the default
   ephemeral range and calls `expect` when no candidate exists; iteration can
   instead be empty, and both paths assert a valid requested shard
   (`sharding.rs:132–237`). Added that distinction, linked the routing section,
   and added the existing `draw_source_port_for_shard_panics_on_no_choice` test.
   This is a connection-section completeness fix; the routing section already
   described the helper behavior correctly.

3. **Raw USE was absent from the keyspace propagation entry paths.**
   `Session::handle_set_keyspace_response` calls `use_keyspace` with the
   server-returned name and case sensitivity enabled (`session.rs:1775–1789`).
   The session request path and automatic pager's first-response handling both
   invoke it (`session.rs:2525–2529`, `pager.rs:1202–1221`). Added this entry path,
   the prior single-connection side effect, and an execution cross-reference for
   failure/deadline boundaries. Added `test_raw_use_keyspace`, whose assertions
   exercise unpaged raw USE, quoting and automatic pager creation. The execution
   specification already described the general response-side effect correctly.

4. **Compression and event configuration boundaries were underspecified.**
   OPTIONS, STARTUP and AUTH_RESPONSE pass `compress=false`; REGISTER and the
   ordinary statement request paths pass `true` (`connection.rs:620–927,
   929–1370,1418–1485`). Response parsing uses the configured compressor and
   response flags independently of request compression. Added these distinctions
   and the per-connection fallback scope. Separately documented the Rust-specific
   event-path quirk: the router captures a config clone before negotiation
   (`577–610`, `1648–1653`), and event parsing uses default protocol features
   (`1939–1960`), while ordinary responses use the updated connection settings.
   Added a missing test target rather than implying this edge is tested. Also
   made the already-implemented no-translator branch explicit in establishment
   step 1 (`2077–2130`).

## Whole-section checks without further findings

- Session defaults, nonzero keepalive interval validation, optional timeout
  disabling, positive pool/coalescing sizes and TCP option application agree
  with the effective session-to-connection configuration.
- Translation/TLS/setup timeouts are correctly separated; repeated candidate
  attempts repeat translation and TCP deadlines. Matching TLS/plaintext shard
  port parsing, malformed sharding/host-ID degradation and diagnostic-only
  peer identity handling agree with the source.
- Ready/Initializing/Broken behavior, disabled-node delegation, early
  initialization completion, uniform shard selection, invalid-shard fallback
  and retained snapshot/reference lifetime match the inspected methods.
- Immediate first fill, empty-pool single attempts, concurrent missing-target
  fills, persistent pending-batch blocking, hint resets, outcome callback timing,
  default jitter/backoff and custom constant policy behavior match the worker.
- Targeted-opening fallback, keyspace failures, useful wrong-shard admission,
  excess bounds/cleanup, resharding, advertised-port replacement, stale opening
  results and 300-second non-extending advanced-awareness suppression match the
  admission and refill paths.
- Queue capacity, 32768 stream IDs, lowest-free allocation, 64-bit request-ID
  wrap, cancellation-before-allocation hole, orphan age/count recovery, buffered
  writes and ordinary/event response failure boundaries match the router.
- Keepalive initial delay, delayed missed ticks, hint coalescing, disabled loop,
  uncompressed raw OPTIONS receipt, queue-inclusive timeout and fatal keepalive
  stream exhaustion match the keepaliver and shared submission path.
- Keyspace validation/casing, separate reported session name, accepted-request
  lifetime, pool switch snapshot/deadline, non-broken failure removal, desired
  state after errors and no-op-node success aggregation match the traced callers
  and callees. Publication/notification ordering and lack of a global keyspace
  barrier remain accurate.
- Optional metrics call sites do not justify a census of session sockets; the
  neighboring observability section already records the discarded targeted
  connection overcount. TLS backend/resumption, protocol extension encoding,
  statement retries and tablet feedback have appropriate neighboring ownership.

## Test claims checked and remaining gaps

Read the cited assertions in `connection_pool.rs:1648–2640`,
`connection.rs:3206–3472,3473–3568,3573–3792`, `routing/sharding.rs` tests,
and these integration files: `session/use_keyspace.rs`,
`session/write_coalescing.rs`, `session/status_change_hints.rs`,
`session/host_id_mismatch.rs`, `statements/coordinator.rs:282–405`,
`session/new_session.rs:124–133`, and `ccm/using_timeout.rs`.

The existing qualifications are supported: the 299/301-second test does not
exercise the exact suppression boundary; synthetic targeted results do not test
actual source-port rewriting; the resharding metric test starts without excess;
the many-connections test does not force collisions or verify shard assignment;
the coalescing test checks results rather than latency/syscalls; the host-ID and
coordinator tests have server-capability skips. Keyspace tests do not establish
all cancellation/no-op aggregation cases, and the orphan tests do not cover all
stream allocation races.

The added product-detection integration test checks `Some(!cfg!(cassandra_tests))`
on the test cluster. The CCM test explicitly disables sharding advertisement,
checks `sharder()==None` and `is_scylladb()==Some(true)`, then requires at least
one captured control QUERY/PREPARE and verifies its configured timeout clause.
Neither isolates every detection signal, unavailable-pool state, host-ID-only
advertisement or mixed-feature pool; those remain marked missing. Compression
setup flags and event configuration snapshots also retain an explicit coverage
gap. Other preexisting missing-test markers remain unresolved by this review.

A local structural check resolved all **37 pinned source links**, verified their
commit/path/line bounds, and found definitions for all **38 named test references**
in the updated specification. This was static validation only. No Rust tests,
Go tests, database tests, feature builds or benchmarks were executed.
