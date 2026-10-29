# Connections review, round 09

- Section: `feature-specs/connections.md`
- Round: 09 — adversarial edge cases and source quirks
- Agent identity: `/root/review_connections_09`
- Reference checkout: `/extra/scylladb/gocql-rust-reference-f8b0ee9`
- Source commit: `f8b0ee928dc0dcb475247dc82cce4e42804b0621`, verified with `git rev-parse HEAD`
- Before SHA-256: `62828997d3d670f8d2e29d0245136711ffe35c99c34fe08def59725bc0e5b5d7`
- After SHA-256: `496ccd0e652c82613e502a415fddb4552621c6801b860340e49aeb3a93f4331d`
- Result: three verified omissions/boundaries corrected; no implementation changes.

Read `feature-specs/reviews/REVIEW-INSTRUCTIONS.md`, `feature-specs/README.md`, and
the complete assigned specification. This was an independent source audit; prior
reports were not used as proof. Edits are limited to the section and this report.

## Verified findings and fixes

### 1. SUPPORTED first-value rules needed the wire-map decoding boundary

The discovery description explained first-value selection and malformed sharding
fallback, but omitted what happens when the wire response repeats an option key.
`Supported::deserialize` calls `read_string_multimap`, whose loop uses
`HashMap::insert`: the last occurrence replaces the whole preceding list. It
does not merge lists or reject duplicate keys. Consequently, discovery's first
value is the first value in the final retained list, not necessarily the first
value advertised on the wire. Invalid string/list encoding instead fails the
SUPPORTED response parser and fails OPTIONS before `ShardInfo::try_from` runs.

Evidence:

- `scylla-cql/src/frame/response/supported.rs:15–22`, `Supported::deserialize`.
- `scylla-cql/src/frame/types.rs:347–358`, `read_string_multimap`.
- `scylla/src/network/connection.rs:674–715`, `get_options`, and `2150–2210`,
  successful map decoding before individual discovery.
- `scylla/src/routing/sharding.rs:274–307`, `ShardInfo::try_from`.

Fix: documented replacement of duplicate top-level keys and distinguished malformed
wire encoding from malformed sharding values. Added pinned source links, a missing
regression entry, and a note to encode duplicate keys directly rather than through
a map fixture. The protocol section's existing first matching `KEY=` rule concerns
entries within an extension's value list; it is a separate boundary. This related
codec omission was reported to the parent without editing that section.

### 2. Excess connections are not promoted after active connections fail

The specification described excess retention and exclusion from selection, but
did not explicitly describe the behavior when an active slot subsequently becomes
empty. `remove_connection` removes the active connection and publishes the new
state; it never searches excess connections for a replacement. `start_filling`
opens new connections. Thus a partially filled sharded pool can retain a live
excess socket for a shard whose active connection failed, yet publish Broken
after losing its final active connection. Its existing excess sockets do not
restore selection. These sockets are also absent from the active-only keyspace
snapshot and immediate keepalive fanout.

Evidence:

- `scylla/src/network/connection_pool.rs:1030–1089`, active admission versus
  excess retention.
- `scylla/src/network/connection_pool.rs:301–313`, immediate hints iterate only
  published active connections.
- `scylla/src/network/connection_pool.rs:799–912`, active-only fullness/emptiness
  checks and new opening attempts.
- `scylla/src/network/connection_pool.rs:1216–1234,1306–1363`, Broken publication
  and removal without promotion.
- `scylla/src/network/connection_pool.rs:1370–1401`, keyspace-switch snapshot.

Fix: documented lack of promotion, the Broken-with-live-excess case, and exclusion
from keyspace switches and immediate hints. Added a missing regression entry and
a concrete test-generation scenario. Existing excess-limit/full-pool cleanup tests
do not assert this recovery boundary.

### 3. Ignored negative streams still undergo full frame reading

The unconditional-looking statement that negative streams are ignored omitted
the earlier frame-reading work. `Connection::reader` awaits
`read_response_frame` before inspecting the stream. That function validates the
server direction, protocol version and response opcode, then reads the declared
body. An invalid header on a negative stream still breaks the connection; an
incomplete body still blocks the reader until data or a failure arrives. Only
after this step does the negative-stream branch ignore the frame, bypassing body
decompression and deserialization. This also applies to stream -1 without an
event receiver.

Evidence:

- `scylla-cql/src/frame/mod.rs:150–200`, `read_response_frame`.
- `scylla/src/network/connection.rs:1680–1710`, read-before-stream-filter order.
- `scylla/src/network/connection.rs:1939–1983`, parsing occurs only for stream -1
  when an event receiver exists.

Fix: narrowed the ignore statement, added frame-reader evidence and an explicit
missing regression entry, and separated header validity, body completeness and
body decodability in the test-generation notes.

## Whole-section checks

The following existing claims were checked against implementation and retained:

- Session defaults and zero-value validation: pool target, socket options,
  five-second TCP timeout, CQL keepalive defaults, write-coalescing mode, and
  reconnect policy gating. The test-only low-level defaults remain distinct.
- Establishment order and deadline boundaries: translation precedes TCP timeout;
  TLS precedes router startup; OPTIONS, STARTUP/authentication and optional REGISTER
  follow; keyspace setup has its separate timeout and recheck. Authentication
  callbacks do not inherit a total handshake timeout.
- Malformed negotiation: required first sharding values, nonzero shard count,
  out-of-range shard rejection, unvalidated ignored-bit range, first matching-mode
  shard port including zero, malformed/absent host ID tolerance, and per-connection
  compression fallback. Protocol feature parsing and product detection were traced
  through `ProtocolFeatures`, including presence-based tablet/metadata flags.
- Initialization, Ready/Broken publication and selection: a partially filled pool
  is selectable; connectivity observations do not probe or reserve capacity;
  random selection/fallback uses one retained snapshot; absent routing hints can
  observe another snapshot; stream capacity does not influence selection.
- Disabled nodes and pool ownership: enabled address-only reuse, endpoint updates
  for future attempts, location-change replacement, retained old node lifetimes,
  and no-op keyspace/refill/keepalive behavior for disabled nodes.
- Refill and partial failure: one ordinary attempt when empty; concurrent missing
  counts thereafter; per-host versus per-shard admission; batch-gated scheduling;
  failure-flag timing; jitter/clamping and custom-policy callbacks; hints do not
  cancel stalled openings. Failed setup does not replace a nonempty Ready pool
  with Broken.
- Advanced shard awareness: exact source-port congruence, no-candidate handling,
  top-level retryable I/O classification, separate translation/TCP timeout per
  candidate, ordinary-port fallback, actual-shard admission, and suspension only
  for mismatch against the attempt's own sharder. The suspension is not extended
  by mismatches before its expiry.
- Rediscovery races: keyspace success precedes sharder/port updates; a late old
  opening can replace newer discovery; gaining/losing sharding or changing only
  ignored bits replaces buckets; monitors retain weak references.
- Multiplexing and coalescing: 1024-entry submission queue, serialization before
  submission, wrapping 64-bit request counter, all 32768 stream IDs, lowest-free
  allocation, immediate exhaustion failure, 8192-byte buffers and empty-queue
  coalescing behavior.
- Cancellation and shutdown: response/cancellation identity mapping; retained
  orphan streams until a response; pre-allocation cancellation can miss orphan
  tracking; strictly-more-than-1024 old-orphan threshold; handle-tied worker/router
  lifetimes and no graceful drain contract.
- Keepalive and events: first delayed tick, delayed missed-tick behavior, stored
  and coalesced hints, disabled-interval behavior, queue-inclusive timeout,
  raw-response acceptance, stream exhaustion closing through keepalive, event
  backpressure, and original-router compression/default event features.
- Keyspace switching: validation and case-insensitive response-name comparison;
  separate reported session name stored before validation; serialized cluster
  and pool fanouts; queueing outside pool deadlines; accepted work surviving
  caller cancellation; retained switch snapshots; ready-result/deadline race;
  desired state surviving failure; broken-connection-only aggregation and no-op
  successes without server validation.
- Notification order: published snapshots precede waiter and connectivity
  notifications; last-active removal can send the separate pool-empty signal
  before publication; data-node connectivity tracking exists without a public
  host listener.
- Invariants and implementation/Go distinction: the new clarifications preserve
  the documented active-versus-excess invariant. Rust ownership/allocation choices
  remain implementation evidence, and proposed Go tests/benchmarks remain gaps.

## Files and symbols inspected

All paths below are relative to the pinned reference checkout.

| Source | Inspected areas |
| --- | --- |
| `scylla/src/network/connection_pool.rs:38–1593` | Pool types, all production selection/refiller/keyspace/publication methods, weak error monitors |
| `scylla/src/network/connection_pool.rs:1648–2650` | All tests referenced by the section, source-port smoke helper, synthetic shard-attempt helper |
| `scylla/src/network/connection.rs:70–277,505–715,823–927` | Submission/orphan notifier, connection creation, OPTIONS/STARTUP and authentication |
| `scylla/src/network/connection.rs:1371–2042,2077–2646` | USE/REGISTER, parsing, router/reader/writer/orphaner/keepaliver, discovery/opening/socket options, stream/keyspace data structures |
| `scylla/src/network/connection.rs:3122–3786` | Negotiation/host-ID, keepalive, socket-option and all five orphaning tests |
| `scylla/src/routing/sharding.rs:11–307,309–505` | Range validation, sharder/source-port algorithms, discovery parser and unit tests |
| `scylla-cql/src/frame/response/supported.rs:1–23`; `scylla-cql/src/frame/types.rs:317–384`; `scylla-cql/src/frame/mod.rs:150–214` | SUPPORTED/map/list decoding, map roundtrip test, response frame parsing |
| `scylla-cql-core/src/frame/protocol_features.rs:1–201` | Feature parsing/STARTUP opt-ins, detection inputs and tablet negotiation tests |
| `scylla/src/cluster/node.rs:114–290` | Node lifecycle, public observations and pool delegation |
| `scylla/src/cluster/state.rs:419–479,1146–1225` | Topology reuse/replacement and filter-on-address-change test |
| `scylla/src/cluster/worker.rs:81–89,345–411,760–847` | Queue size, fanout serialization, status changes and keyspace result aggregation |
| `scylla/src/client/session.rs:469–671,1412–1424,1775–1789,2325–2339,2395–2408` | Defaults/validation, policy selection, raw USE propagation and reported keyspace |
| `scylla/src/client/pager.rs:1192–1221`; `scylla/src/client/execution.rs:550–658`; `scylla/src/policies/load_balancing/plan.rs:94–107` | Pager USE propagation, selection/attempt retry boundary and random shard hint |
| `scylla/src/policies/reconnect.rs`; `scylla/src/policies/mod.rs:20–31`; `scylla/src/errors.rs:605–627` | Reconnect factories/delay rules/gating and source-port retry classification |
| `scylla/src/observability/metrics.rs:341–348,534–535` | Connection metric mutation/read sites, checked against existing observability caveat |
| `scylla/tests/integration/session/use_keyspace.rs` | All five cited keyspace tests |
| `scylla/tests/integration/session/status_change_hints.rs`; `write_coalescing.rs`; `host_id_mismatch.rs` | Targeted hints, coalescing smoke assertions and diagnostic/skip boundaries |
| `scylla/tests/integration/session/new_session.rs:125–135`; `scylla/tests/integration/ccm/using_timeout.rs:55–199`; `scylla/tests/integration/statements/coordinator.rs:282–403` | Product detection, no-sharding metadata timeout and actual-shard fallback assertions |

Related specification paragraphs in routing, serialization/protocol,
metadata/events and observability were read to check boundaries; none was edited.

## Validation and remaining coverage

- A local reference check found all **39 named test references** in the pinned
  files and validated all **50 pinned source line links** against local file
  lengths. No trailing whitespace was found.
- Read the cited test bodies and relevant helpers. Existing caveats remain
  accurate: source-port smoke coverage does not force a collision or assert its
  assigned shard; synthetic mismatch tests do not rewrite real source ports;
  resharding cleanup starts without excess sockets; metrics cleanup tests require
  the metrics feature; coalescing is a result-preservation smoke test; relevant
  live-cluster tests retain their server/configuration skip conditions.
- The existing map roundtrip test starts from a HashMap and cannot preserve or
  exercise repeated wire keys. No dedicated regression was found for duplicate
  SUPPORTED keys, negative-stream validation/read boundaries, or excess promotion
  after active failure. These are now explicit missing coverage entries.
- No Rust, Go, proxy, CCM or live-database tests were executed. The new statements
  are source-traced behavior, not claims of runtime validation. Existing gaps for
  cancellation races, refill scheduling, stalled setup, complete negotiation
  boundaries and proposed Go conformance/allocation work remain explicit.
