# Connections review — round 02

- Section: `feature-specs/connections.md`
- Round: `02` — defaults, exact conditions, limits, ordering, and fallback boundaries.
- Agent identity: `/root/review_connections_02`
- Source checkout: `/extra/scylladb/gocql-rust-reference-f8b0ee9`
- Source commit, verified with `git rev-parse HEAD`: `f8b0ee928dc0dcb475247dc82cce4e42804b0621`
- Before SHA-256: `f9a3a33d1d9d0df3d69caa368c5827027d4d3f8c65790149e5cba22b138159f2`
- After SHA-256: `2dba91f45339cebd669b4f86fac46575e0063b83b7e065fde1b7e7f9ee96a711`

Read the complete section, `feature-specs/README.md`, review instructions, and the
prior report. The prior report was context; all conclusions below were checked
against the pinned implementation. Only this report and the assigned section
were edited. No child agents were used and no Go or Rust implementation changed.

## Verified findings and fixes

1. **The zero-value timeout boundary was omitted.**
   `SessionConfig::validate` in `scylla/src/client/session.rs:618–670` rejects
   `Some(Duration::ZERO)` only for the CQL keepalive interval. The connection
   timeout setter (`session_builder.rs:874–877`) stores its duration directly;
   the keepalive timeout setter (`1183–1193`) warns for small values but still
   stores zero. `Connection::new` uses the connect timeout at
   `connection.rs:544–553`; `keepaliver` tests optional presence at
   `1878,1902–1919,1933–1936`. Added that zero connect/keepalive timeout is not
   rejected and is not a disable switch; absence of the corresponding keepalive
   interval/timeout disables that mechanism. Extended the validation evidence
   link to include the complete method. This does not assert that zero-duration
   operations must always fail; timeout polling remains an implementation detail.

2. **TCP buffer-size width conversion was missing.**
   Public builder setters accept and retain `usize` sizes
   (`session_builder.rs:702–705,723–726`), and socket application casts them with
   `as u32` (`connection.rs:2362–2366`). There is no range validation in the
   session validator. Added that values exceeding 32 bits can truncate on 64-bit
   targets, and that effective socket buffers can differ from the requested size.
   The latter distinction is also acknowledged and asserted by the existing socket
   test (`connection.rs:3440–3456`). Sent the width-conversion boundary to the
   parent for the configuration section; did not edit that section.

3. **Shard-port discovery's fallback boundary was underspecified.**
   `open_connection` chooses exactly one key based on `config.is_tls()` at
   `connection.rs:2153–2156`, then removes that key, examines only `.first()`,
   and parses `u16` at `2181–2186`. It neither tries the other TLS/plaintext key
   nor later strings, and zero is accepted by the parser. Documented those exact
   rules and added a missing-test entry for the negotiation boundary.

4. **Pool rediscovery omitted loss of metadata and port replacement.**
   `handle_ready_connection` checks keyspace completion first
   (`connection_pool.rs:964–981`), derives the new optional sharder
   (`987–990`), reconfigures the pool (`1006`), and replaces the remembered port
   (`1008–1016`) before deciding whether the new connection is usable or excess
   (`1023–1089`). `Sharder` equality includes both shard count and ignored-MSB
   count (`sharding.rs:70–76`); `maybe_reshard` compares the full optional sharder
   and creates one bucket when it becomes absent
   (`connection_pool.rs:1186–1211`). Added that gain/loss of sharding information
   and an ignored-MSB-only change also replace active/excess collections, and
   that a completed excess connection can update or clear the advertised port.
   Added an explicit missing-test case for these rediscovery boundaries.

5. **Source-port retry attempts have independent setup/timeout scopes.**
   The candidate loop calls the complete `open_connection` for each port
   (`connection.rs:2297–2317`); that routine repeats address translation at
   `2141–2146`, and each `Connection::new` creates its own TCP timeout at
   `544–553`. There is no outer loop deadline. The retry predicate accepts only
   `ConnectionError::IoError` with the three specified kinds
   (`errors.rs:611–627`), not identically classified errors nested inside a CQL
   setup error. Added these boundaries and a missing-test entry. The existing
   immediate ordinary-port fallback after a failed targeted open remains correct.

6. **Request identity was described as unconditionally monotonic, and stream
   allocation order was unspecified.**
   The generator is an `AtomicU64` using `fetch_add(1)`
   (`connection.rs:109–135`), initialized to zero at `589–592`; there is no wrap
   guard. Replaced the unlimited-monotonic description with its modulo-2^64
   behavior and qualified late-cancellation isolation by distinct request
   identities. `StreamIdSet::allocate` scans bitmap blocks from the beginning and
   uses `trailing_ones()` (`2569–2578`), selecting the lowest free stream ID.
   Added that ordering without claiming a queue fairness or capacity-reservation
   guarantee. Existing missing stream-allocation/race coverage remains explicit.

7. **Existing boundary tests were absent from the section's coverage list.**
   Read and added `test_shard_aware_port_range_constructor`
   (`sharding.rs:333–348`) and the three shard-ID parser/constructor tests
   (`378–422`). Descriptions are limited to their actual inputs: the default,
   reversed, and zero-starting port ranges, and valid/invalid IDs for four shards.
   They are not presented as exhaustive maximum-width or distribution tests.

## Source inspected and behaviors checked

The entire section was audited, including defaults, every behavioral subsection,
implementation evidence, invariants, test references, and test-generation notes.

| Source | Symbols/ranges inspected |
| --- | --- |
| `scylla/src/client/session.rs` | `SessionConfig::new`, socket mapping, complete `validate` (`469–670`); session-to-connection/pool mapping and reconnect policy selection (`1407–1488`). |
| `scylla/src/client/session_builder.rs` | Buffer setters (`686–727`), connection timeout (`855–878`), keepalive interval/timeout (`1127–1194`). |
| `scylla/src/network/connection_pool.rs` | Pool sizing, state, selection and public internal operations (`38–502`); complete refiller, setup outcomes, endpoint snapshots, resharding, publication, removal and keyspace management (`507–1533`); weak error monitor and event types (`1535–1592`). |
| `scylla/src/network/connection.rs` | Queue/request identity/notifier (`70–226`); identity/config/defaults and creation (`228–716`); authentication (`823–927`); USE, REGISTER and response parsing (`1371–1525`); router, reader, writer, orphaner, keepalive and events (`1527–1983`); negotiated properties and hints (`1985–2043`); translation, establishment, port loop and socket options (`2077–2426`); orphan maps, stream allocator, keyspace validation (`2428–2646`). |
| `scylla/src/routing/sharding.rs` | `ShardAwarePortRange`, `ShardInfo`, `Sharder`, port drawing/enumeration, discovery parser (`11–307`). |
| `scylla/src/policies/reconnect.rs` | Entire policy module (`1–246`), including independent sessions, exponential limits/jitter, constant policy and explicit checks. |
| `scylla/src/policies/mod.rs` | Public/private reconnect-module gate (`25–28`). |
| `scylla/src/errors.rs` | I/O conversion and source-port retry classification (`605–627`). |
| `scylla/src/cluster/node.rs` | Disabled nodes, inherited pools, selection, state, keyspace delegation and hints (`114–290`). |
| `scylla/src/cluster/worker.rs` | Broken-connection classification and keyspace success aggregation (`804–847`). |

Explicitly checked and retained:

- Default per-shard target one, per-host counting, wildcard source bind, inclusive
  port range, five-second TCP timeout, TCP option defaults, 30-second CQL
  interval/timeout, scheduler-yield coalescing, 50 ms/10 s exponential bounds,
  inclusive 0.85–1.15 jitter and unstable policy gate.
- Translation bypasses, configured source-IP family behavior, TLS before router,
  OPTIONS before STARTUP, feature discovery, unsupported-compressor fallback,
  authentication and REGISTER ordering, diagnostic-only host-ID mismatch after
  keyspace setup, and absence of a total setup timeout.
- Nonempty Ready versus full target; initialization after first usable connection
  or unsuccessful first fill; registration before the initialization state check;
  disabled-node selection errors/no-op hints; future endpoint snapshots.
- Uniform connection selection within the chosen shard, uniform initial shard
  choice, invalid-shard handling, random fallback permutation, no load/capacity
  reservation, and allocating fallback storage.
- Immediate first fill and one ordinary attempt for empty pools; per-host versus
  per-shard fills; scheduling only after ready/setup futures finish; failure-flag
  reset/doubling; first failed immediate fill's nominal 100 ms next delay; hint
  rescheduling; persistent retries; independent policy sessions and exact
  callback timing; constant-policy jitter without clamping.
- Strictly-greater-than excess bound (ten times shard count); zero per-host
  limit; full-pool cleanup; immediate ordinary replacement of rejected targeted
  connections; keyspace failures handled separately from targeted opening
  failures; Ready preserved after a failed attempt when active connections remain.
- Candidate enumeration without duplicates; three retryable I/O kinds;
  shard-mismatch suppression for 300 seconds, nonextension while active, stale
  sharder exemption, mismatched connections still useful on their reported shard,
  and no full-pool replacement merely because suppression expired.
- Queue size 1024, prequeue serialization, all 32768 nonnegative streams,
  request-level exhaustion, 8192-byte buffers, observed-empty-queue coalescing,
  out-of-order dispatch, ignored negative streams, fatal unsolicited streams,
  event-channel backpressure and ordinary response parsing after dispatch.
- Orphans retain stream IDs until response; missing request mappings are ignored;
  cancellation does not promise unsent work or rollback; old-orphan count must
  exceed 1024; processing time determines age; pool/router handles govern
  cancellation and weak error monitors do not prolong connection lifetime.
- First periodic keepalive after one interval, delayed missed ticks, retained and
  coalesced hints, no hint-driven loop when disabled, timeout includes submission,
  raw response acceptance without SUPPORTED parsing, ordinary traffic does not
  postpone probes, platform-dependent TCP spacing/retries.
- ASCII keyspace limits and case-insensitive returned-name comparison; desired
  keyspace recorded before snapshot switch; empty-pool success; serial switches
  and one-element queue; concurrent refill/errors; shared switch timeout after
  dequeue; removal on live USE failure or timeout; desired keyspace retained;
  setup recheck; aggregation of successes and broken/nonbroken errors.
- Snapshot publication before initialization and connectivity events;
  Initializing-to-Broken Lost event; no Ready-to-Ready/Broken-to-Broken transition
  event; separate best-effort pool-empty signal. That separate signal occurs
  before publication in `remove_connection`; the publication ordering statement
  applies to initialization/connectivity events, not this separate notifier.
- Rust reference-counted snapshots, concurrent futures, mutex scope, weak
  monitors, and owned request buffers remain implementation evidence, not Go
  allocation or ownership guarantees.

## Test coverage checked and validation

Read the named tests and their relevant helpers, not only symbol names:

- `sharding.rs:333–504`: range validation, shard-ID validation, exact candidate
  membership/count/no-duplicates, empty candidate set, and the separate public
  draw helper's no-choice panic.
- `connection_pool.rs:1648–2064`: failed USE removal, explicit timeout removal,
  real timeout through the pool worker, setup-failure refill eligibility, and
  delayed sharding adoption after successful keyspace setup.
- `connection_pool.rs:2066–2649`: 400 connections in each of two socket
  configurations; temporary suppression; old active-connection replacement;
  excess cleanup and metric feature gates; same/stale sharder mismatch cases;
  host-ID logs including the post-keyspace single-log assertion. Existing tests
  do not establish loss-of-sharding or ignored-MSB-only reconfiguration.
- `connection.rs:3206–3785`: valid/malformed/missing host IDs; unanswered
  keepalive and subsequent unusability; socket option propagation/readback;
  1025 persistent orphans, a short 1025-orphan spike, 1024 persistent orphans,
  and sustained 90%/110% old-orphan workloads. The paused-clock orphan/keepalive
  tests use the proxy's in-memory transport.
- Full `write_coalescing.rs`, `status_change_hints.rs`, and
  `host_id_mismatch.rs`; `coordinator.rs:265–410`: data preservation for three
  coalescing settings, targeted DOWN probes with periodic/control traffic
  excluded, host-ID-advertisement skip, and rewritten-shard fallback for unpaged
  and paged paths with the fewer-than-two-shards skip. None is claimed to prove
  syscall count, precise coalescing latency, or physical shard identity.

No Rust, Go, proxy, or live database tests were executed. A local documentation
validation checked all 23 pinned source links against existing paths and line
bounds, all 30 referenced existing test symbols, four relative document links,
final newline, and absence of trailing whitespace. SHA-256 values were calculated
from the specification bytes.

The new missing entries explicitly retain gaps for optional-port negotiation,
per-candidate retry scopes, and sharding/port rediscovery. Existing missing
repeated-mismatch, nonempty-excess resharding, allocation/race, selection,
backoff, setup-timeout, disabled/raw keepalive, and Go conformance/benchmark
entries remain unexecuted requirements. Request-counter wrap, oversized socket
buffers, and zero timeout polling also have no dedicated tests identified here;
their documented boundaries are established by implementation tracing.
