# Connections review — round 01

- Section: `feature-specs/connections.md`
- Round: `01` — trace claims to implementation and correct factual mismatches.
- Agent identity: `/root/review_connections_01`
- Source checkout: `/extra/scylladb/gocql-rust-reference-f8b0ee9`
- Source commit, verified with `git rev-parse HEAD`: `f8b0ee928dc0dcb475247dc82cce4e42804b0621`
- Before SHA-256: `0a4d7c5dfdb94e191201c2f92d36a99440f1144227af47e36cfb3d86cff3f46f`
- After SHA-256: `f9a3a33d1d9d0df3d69caa368c5827027d4d3f8c65790149e5cba22b138159f2`

Read the complete assigned specification, `feature-specs/README.md`, and
`feature-specs/reviews/REVIEW-INSTRUCTIONS.md`. Only the assigned specification
and this report were edited. No implementation or tests were changed.

## Verified findings and fixes

1. **Permission-denied errors also advance the source-port search.** The draft
   said only address-in-use/unavailable errors retry, and all other results stop.
   `ConnectionError::is_address_unavailable_for_use` in `scylla/src/errors.rs:611–627`
   also accepts `ErrorKind::PermissionDenied`; the candidate loop calls this
   predicate at `scylla/src/network/connection.rs:2307–2313`. Added the missing
   classification and a pinned evidence link.

2. **Sharding discovery does not reject every semantically invalid parameter.**
   The blanket malformed-information fallback was broader than the parser.
   `ShardInfo::try_from` in `scylla/src/routing/sharding.rs:274–307` requires first
   values for all three fields, parses shard/count as `u16`, rejects zero count,
   and parses ignored-MSB count as `u8`. `ShardInfo::new` at `85–103` only adds
   the shard-below-count check. Thus ignored-MSB values 64–255 pass discovery.
   Replaced the blanket statement with the actual validation and linked both
   implementations. Reported this boundary to the parent for routing consistency.

3. **Reconnect outcome callbacks are scheduling callbacks, not per-batch
   completion notifications.** `PoolRefiller::run` in
   `scylla/src/network/connection_pool.rs:774–790` calls the selected callback only
   when a refill needs scheduling; `need_filling` at `812–814` requires an
   unfinished target and no connection/setup futures. A batch leaving a full
   pool invokes neither callback at completion. The hint branch at `753–767`
   calls successful-fill separately, even for a full pool, and the following
   scheduling decision can call it again. Corrected the backoff and custom-policy
   descriptions to specify those conditions. The actual reset/doubling/jitter
   operations were checked in `scylla/src/policies/reconnect.rs:67–90,118–171`.

4. **Opening failure and later keyspace-setup failure have different targeted
   fallback behavior.** `handle_ready_connection` at
   `scylla/src/network/connection_pool.rs:917–963` immediately retries ordinary
   ports only for the targeted `ConnectionSetupError::Connection` case.
   `ConnectionSetupError::Keyspace` sets the failure flag and uses normal refill
   scheduling. Qualified the broad targeted-failure statement so it cannot imply
   an immediate ordinary-port retry after failed USE setup.

5. **Timeout queue boundaries were incomplete.** The keepalive timeout encloses
   `RouterHandle::send_request`, including bounded-queue submission
   (`scylla/src/network/connection.rs:138–170,1866–1875,1902–1919`). A congested
   queue can consume the timeout before a probe is written. A snapshot keyspace
   switch receives one shared connect-timeout deadline when the worker processes
   it (`scylla/src/network/connection_pool.rs:428–443,740–743,1370–1411`); waiting
   to submit or dequeue that switch is outside the deadline. Added both facts.

6. **Two unit-test claims overstated their assertions.**
   `advanced_shard_awareness_is_blocked_temporarily` at
   `scylla/src/network/connection_pool.rs:2165–2184` blocks once, checks before
   expiry, then checks after expiry; it never delivers another mismatch while
   blocked. Narrowed the claim and added an explicit missing repeated-mismatch
   deadline case. `resharding_discards_old_connections_and_balances_metrics` at
   `2252–2313` starts with one active connection and no excess connections.
   Its empty-excess assertion cannot establish clearing a nonempty collection.
   Narrowed that claim and added the missing nonempty-excess case. Also documented
   the metrics feature gate on this test and both excess cleanup tests
   (`2315–2386`). The implementation still supports both uncovered behaviors:
   block nonextension at `830–849` and excess clearing at `1186–1211`.

7. **Integration-test claims needed their applicability and observation limits.**
   `mismatched_host_id_is_reported` in
   `scylla/tests/integration/session/host_id_mismatch.rs:96–109` returns before
   assertions when the server omits `SCYLLA_HOST_ID`. The coordinator fallback
   test rewrites SUPPORTED shard metadata to zero and skips nodes with fewer than
   two advertised shards (`scylla/tests/integration/statements/coordinator.rs:303–329,353–359`).
   Its assertions at `373–403` establish the selected connection's reported shard,
   not independent observation of the database's physical shard. Added these
   limits to the test descriptions.

## Source inspection and behaviors checked

The audit covered all specification subsections, implementation evidence,
invariants, and test-generation notes. Principal files and symbols inspected:

| Source | Inspected symbols/ranges and checked behavior |
| --- | --- |
| `scylla/src/client/session.rs` | `SessionConfig::new`, `tcp_socket_options`, `validate` (`469–650`), `Session::connect` configuration mapping (`1407–1486`): defaults, zero keepalive rejection, feature-gated/default reconnect policy, data-connection event/report settings. |
| `scylla/src/network/connection_pool.rs` | Pool configuration and state (`38–264`); node pool selection, hints, initialization, snapshot collection (`266–502`); worker state and complete refiller implementation (`507–1533`). |
| `scylla/src/network/connection.rs` | `RouterHandle`, `OrphanhoodNotifier` (`70–226`); socket/config definitions and test defaults (`280–461`); creation, OPTIONS/STARTUP (`520–716`); authentication (`823–927`); USE, REGISTER, request parsing (`1370–1526`); complete router/reader/writer/orphaner/keepaliver/event implementation (`1527–1983`); negotiated properties, keepalive hint (`1985–2043`); address translation, open sequence, source-port loop, TCP options (`2077–2426`); handler map, stream bitmap, orphan tracking, keyspace validation (`2428–2646`). |
| `scylla/src/routing/sharding.rs` | `ShardAwarePortRange`, `ShardInfo`, `Sharder` source-port functions and parser (`11–237,274–307`); source-port tests (`425–504`). |
| `scylla/src/policies/reconnect.rs` | Entire policy implementation (`1–246`): independent policy sessions, limits, jitter, exponential and constant callback behavior. |
| `scylla/src/policies/mod.rs` | Public/private reconnect-module feature gate (`25–28`). |
| `scylla/src/errors.rs` | `ConnectionError::is_address_unavailable_for_use` (`611–627`). |
| `scylla/src/cluster/node.rs` | Node creation, disabled nodes, inherited pool on endpoint changes, pool delegation (`114–290`). |
| `scylla/src/cluster/worker.rs` | `use_keyspace_broke_connection`, `use_keyspace_result` (`804–847`). |

Explicitly checked and retained:

- Per-shard default sizing; per-host distribution; first/empty fill opening one
  ordinary connection; under-target batch scheduling; jitter/clamping; ordinary
  versus targeted failure flag behavior; excess bounds and clearing.
- Ready as nonempty rather than fully filled; initialization completion; disabled
  node errors and no-op hints; endpoint update effects on future attempts;
  random shard/connection selection, invalid-shard fallback, and no capacity-aware
  selection.
- Source-IP family handling; address translation bypasses; TCP-only connect
  deadline; TLS preceding router start; OPTIONS discovery, compressor fallback,
  STARTUP identity, authentication and REGISTER ordering; diagnostic-only host-ID
  mismatch after keyspace setup.
- Mismatch suppression for 300 seconds, its nonextension, stale-sharder exception,
  useful mismatched connections, resharding replacement, retained reference
  lifetime, and weak error monitors.
- Queue capacity 1024, serialization before queueing, 32768 nonnegative streams,
  allocation failure without automatic connection shutdown, 8192-byte buffers,
  empty-queue coalescing, out-of-order dispatch, negative streams and bounded event
  backpressure.
- Orphan notifications before/after allocation and response, reserved orphaned
  slots, greater-than-1024 old-orphan threshold, processing-time age, and the
  absence of a cancellation/rollback or graceful-drain guarantee. The existing
  approximate age wording avoids falsely specifying a uniform equality boundary
  for the tuple-range implementation at `connection.rs:2457–2464`.
- Delayed first periodic keepalive, delayed missed-tick behavior, hint permits,
  disabled keepalive, raw response acceptance without SUPPORTED validation, TCP
  probe independence and platform-gated intervals/retries.
- Keyspace validation/quoting, case-insensitive returned-name comparison, desired
  keyspace retention after failures, setup recheck, active-snapshot switching,
  continued refill/error processing, failed-USE removal, timeout removal, and
  broken-connection-only aggregation rules.
- Snapshot publication before initialization/connectivity notifications,
  Initializing-to-Broken loss notification, no same-state transition events, and
  separate best-effort pool-empty signaling.
- Rust reference counting, atomically published snapshots, concurrent futures,
  lock boundaries, owned serialization buffers, and allocation caveats are
  implementation evidence rather than a proposed Go ownership/allocation contract.

## Test coverage checked and validation

Read the named tests, not just their names:

- Pool keyspace tests at `connection_pool.rs:1648–2064`: error and timeout removal,
  initial setup timeout, refill eligibility/failure flag, preservation of a Ready
  pool, and sharding adoption only after successful keyspace setup.
- Connection-count test/helper at `2066–2136`: 400 real-socket connections under
  the two configured reuse-address variants; it is not proof of target-shard
  correctness.
- Suspension, resharding, excess cleanup, mismatch and host-ID tests at
  `2165–2184,2252–2649`, including the evidence limits corrected above.
- Connection host-ID, keepalive timeout, socket-option and five orphan tests at
  `connection.rs:3206–3785`, including their dry proxy/in-memory transport use.
  The orphan cases exercise 1025 stuck requests, a 1025-request short spike,
  exactly 1024 persistent requests, and sustained traffic below/above the threshold.
- `write_coalescing.rs:1–124`: concurrent writes/readback for yield, 1 ms and no
  delay; no latency or syscall assertion.
- `status_change_hints.rs:1–182`: two DOWN hints for each of three nodes, one data
  connection per node, feedback assertions, and periodic/control traffic exclusion.
- Complete host-ID mismatch integration test and coordinator fallback test
  (`coordinator.rs:265–410`), including skip conditions and both unpaged/paged paths.

No Rust, Go, proxy, or database tests were executed. A local documentation check
validated all 23 pinned source links against existing files and line bounds, all
26 referenced existing test symbols, relative document links, final newline, and
absence of trailing whitespace. The before/after SHA-256 values were calculated
from the specification bytes.

Existing missing test entries remain gaps rather than claims of execution or
coverage. This round adds missing repeated-mismatch-deadline and nonempty-excess
resharding cases. Dedicated queue-delay timeout and exact callback-sequence tests
remain useful; the behavior above is established by source tracing.

One adjacent implementation caveat was sent to the parent without editing another
section: successful targeted opens increment total connections
(`connection_pool.rs:1135–1140`), while the rejected targeted-connection branch
(`1050–1062`) starts an ordinary replacement without decrementing that metric.
An observability specification should not infer a universal exact live-socket
gauge guarantee from the balanced-metric tests cited here.
