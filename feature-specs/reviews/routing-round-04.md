# Routing review round 04

- Section: `feature-specs/routing.md`
- Round: 04 — concurrency, cancellation, snapshots, lifetimes, and races, with a whole-section audit.
- Agent identity: `/root/review_routing_04`
- Reference checkout: `/extra/scylladb/gocql-rust-reference-f8b0ee9`
- Verified reference HEAD: `f8b0ee928dc0dcb475247dc82cce4e42804b0621`
- Before SHA-256: `d9e9948e3c1cfa3f843f76f82093f743ac9703e1b21db81a5457dcffc3277ff1`
- After SHA-256: `80e3282e7499ff63c0dc54a61dd809249bf21b780f0993d51c773886a3737a06`
- Scope: only the assigned routing specification and this report changed. No Go or Rust implementation changes; no child agents.

Read `feature-specs/reviews/REVIEW-INSTRUCTIONS.md`, `feature-specs/README.md`, and
the complete assigned specification. Findings below come from independent source
tracing, not prior-round reports. All Rust paths and line numbers below are
relative to the verified reference checkout.

## Verified findings and fixes

1. **A retained metadata snapshot does not freeze target eligibility or sharding.**
   The old wording mentioned changing connection availability, but omitted live
   sharder/policy state and the mixed timing of fallback evaluation.
   `Plan` borrows the cluster and policy (`policies/load_balancing/plan.rs:70-90`);
   `Node::sharder` and `Node::is_connected` query the pool
   (`cluster/node.rs:191-237`). Pool connection state is loaded independently
   (`network/connection_pool.rs:266-273,316-321`), and vnode shards are calculated
   through that live sharder (`routing/locator/mod.rs:318-323`).
   `DefaultPolicy::filtered_replicas` is lazy (`default.rs:703-725`), ordinary
   `shuffle` collects it (`912-925`), node rotations retain lazy filters
   (`903-908`), and latency wrapping consumes the whole iterator only when a
   global minimum exists (`3489-3522`). Latency measurements use shared locks
   independently of the cluster (`3381-3397,3470-3485,3525-3553`).
   Updated request lifetime, target ordering, invariants, and source links to
   distinguish retained metadata from live observations and fixed partitions.

2. **Plan iteration and automatic paging had undocumented lifetime boundaries.**
   `Plan::new` only creates state; `pick` occurs on the first `next`, and fallback
   is retained after first construction (`plan.rs:81-90,113-166`). Speculation
   wraps the same iterator in `SharedPlan`'s synchronous mutex, whose guard ends
   after one `next` (`client/execution.rs:99-113,445-479`). Connection acquisition
   and attempts run outside that guard (`560-597`); same-target retries do not
   consume another entry (`649-658`). `PagingExecutor` owns one cluster snapshot
   (`client/pager.rs:134-158`), constructs each page's fresh plan against it and
   prepends the previous coordinator (`340-368`), and prepared paging chooses one
   hint for all pages (`997-1013`). Manual-page entry points capture state per
   call (`client/session.rs:1669,2105`). Added these distinctions, including the
   lack of a physical-destination uniqueness guarantee when shard fallback or
   policy duplicates apply. Reported the pager boundary to the parent for
   cross-section consistency with execution.

3. **Attempt callbacks were described too broadly for cancellation.**
   The old statement that a running attempt reports failure did not specify the
   completion boundary. The callback is reached only after `run_request_once`
   returns (`client/execution.rs:594-629`); the outer timeout drops its runner
   (`514-529`) and speculative execution returns while other futures remain in
   its local collection (`policies/speculative_execution.rs:198-222`). A dropped
   attempt therefore has no synthesized policy failure callback. Its latency
   measurement starts after connection selection (`execution.rs:564-580`) and
   stops after the entire attempt closure, including connection-local preparation
   (`session.rs:1718-1748`) and awaited tablet feedback
   (`network/connection.rs:1167-1173,1211-1215,2065`). Corrected the callback claim
   and documented elapsed-time scope, timeout, cancellation, and speculative loss.

4. **Tablet enqueue, request completion, and snapshot publication are distinct.**
   `update_tablets_from_response` awaits a bounded-channel send and receives no
   publication acknowledgement (`network/connection.rs:2045-2068`). The cluster
   worker separately receives batches and publishes (`cluster/worker.rs:303-340`).
   Combined with the cancellation points above, a pending send may be dropped,
   while an already queued update is independent of request cancellation. Added
   both cases and stated that successful query return does not prove publication
   has occurred. Existing backpressure and closed-receiver behavior remains.

5. **Serial cache mutation does not protect against stale schema generations.**
   One cluster worker applies feedback and metadata updates in mutually exclusive
   branches (`cluster/worker.rs:307-351`). However, the feedback channel contains
   only `TableSpec` and `RawTablet` (`286`), storage is keyed by table names
   (`routing/locator/tablets.rs:870-883,913-925`), and insertion intentionally skips
   keyspace-presence validation (`cluster/state.rs:863-869`). Schema maintenance
   removes entries by current keyspace/table/view presence
   (`tablets.rs:993-1035`). Thus late feedback can repopulate a removed entry or
   apply to a recreated table with the same name; serial application is not a
   generation fence. Added this source consequence without claiming server-side
   operation cancellation or a new Go contract.

Added focused missing-test entries and test-generation guidance for these
boundaries. No existing test was represented as proving them.

## Whole-section source audit

In addition to the finding-specific paths, inspected these implementation areas:

| Files and symbols | Behaviors checked |
| --- | --- |
| `scylla/src/policies/load_balancing/mod.rs:25-220`, `default.rs:145-1080` | Routing fields, regular SERIAL/LOCAL_SERIAL classification, custom-policy concurrency traits and callbacks, defaults, location inheritance, failover, leader promotion, pick/fallback priorities, mixed-shard deduplication, replica versus node ordering. |
| `scylla/src/policies/load_balancing/single_target.rs:71-105` | Direct node use, snapshot host/address lookup, no fallback, no implicit default-policy filters. |
| `scylla/src/client/session.rs:1300-1371,1660-1755,1841-1855,2008-2039,2092-2168` | First-statement batch token/table, whole-batch confirmed LWT, unprepared execution hints and suppressed key errors, automatic paging preparation, table partitioner assignment, one-snapshot prepared execution. |
| `scylla/src/statement/prepared.rs:307-355,740-868` | Prepared token-awareness metadata, LWT flag, key extraction sequence, missing indexes, null/unset filtering, composite framing and length checks, streaming hashing. |
| `scylla/src/routing/mod.rs:20-56`, `partitioner.rs:24-153,269-422`, `sharding.rs:15-305` | Minimum-token normalization and string-parsing exception, suffix partitioner selection, signed-byte Murmur3 tail, CDC first-eight-byte behavior, encoded-key helper differences, shard arithmetic, source-port ranges, invalid advertised IDs, empty candidates. |
| `scylla/src/routing/locator/token_ring.rs:16-69`, `replication_info.rs:117-209`, `precomputed_replicas.rs:78-198`, `mod.rs:114-265,780-915` | Ring lookup and wrapping, SimpleStrategy filtering, NTS rack-repeat budget and caps, precomputation groups, tablet override/cold-cache behavior, global ordered-NTS continuation and zero-factor primary quirk. |
| `scylla/src/routing/locator/tablets.rs:179-345,477-609,698-1049` | V1/V2 parser precedence, nonmonotonic versions, nibble encoding, payload rejection, empty lists, unresolved followers/leaders, equivalence detection, overlap replacement, copy-on-write data, node replacement, schema/view maintenance. |
| `scylla-cql-core/src/frame/protocol_features.rs:63-140`, `scylla/src/network/connection.rs:1110-1222,1252-1369,2045-2068` | Negotiated V2 emission, initial and reprepared EXECUTE feedback before database error handling, BATCH's lack of feedback forwarding, local batch preparation. |
| `scylla/src/cluster/state.rs:620-741,837-894,897-974`, `cluster/worker.rs:302-351` | Typed and preserialized token APIs, table/view distinction, supplied-token endpoint errors, batched deduplication against intermediate state, serial publication. |
| `scylla/src/policies/load_balancing/default.rs:3270-3618,3626-3658,3790-3817,3849-3873`, `policies/host_filter.rs:15-98` | Shared per-host latency averages, elapsed weighting, minimum/count/freshness predicates, microsecond comparison, zero-scale rejection, failure classification, stale minima, allowlist and datacenter filtering. |

The existing invariants and implementation exceptions remain supported, including
the minimum-token exceptions, invalid-key helper differences, Murmur3-only local
preparation, ordered-NTS zero-factor quirk, shard-hint relaxation, and allocation
limits. No further correction was justified in those areas.

## Test coverage and validation

- Resolved all **51 named test/module references** in the specification to actual
  pinned source declarations. This check establishes reference existence, not
  execution or complete behavioral coverage.
- Checked default-policy grouping helpers and representative location/LWT/leader
  cases (`default.rs:1555-1622,2155-2215,2770-2840`), plus the no-pick fallback test
  (`plan.rs:197-245`). Their topology fixtures commonly substitute enabled state
  for connectivity; they do not establish correctness under pool transitions.
- Checked ring and replication vector assertions, tablet replica ordering and DC
  filtering (`locator/test.rs:224-265,799-870`), multiword masks
  (`locator/mod.rs:1030-1069`), partitioner vectors/chunking
  (`partitioner.rs:440-492`), and shard/port boundary assertions
  (`sharding.rs:351-394,425-451,484-503`).
- Checked tablet invalid-range, V2 tuple/preference, nibble encoding, and missing
  leader/follower assertions (`tablets.rs:1152-1176,1235-1360,2280-2306`). Checked
  the cluster-state no-op test's identical-batch, changed-range/version/shard,
  and within-batch replacement assertions (`state.rs:1381-1460`); this is a
  sequential state-update test, not a concurrent publication/lifetime test.
- Checked latency count/expiry/penalty and zero-scale tests
  (`default.rs:4008-4064,4128-4273,4596-4599`). These do not force the concurrent
  updates and iterator scheduling covered by this round's new gap entry.
- Inspected test functions/assertion sites in
  `scylla/tests/integration/load_balancing/{token_awareness,simple_strategy,shards,lwt_optimisation,tablets,latency_awareness}.rs`
  and `scylla/tests/integration/ccm/tablets_routing_v2.rs`. Existing scenarios cover
  owners/shards, prepared and batch LWT, table/view/CDC learning, mixed feature
  connections, version convergence, and warmed leader targeting. V2 integration
  tests can skip when unsupported; convergence tests use bounded probabilistic
  probes and migration-aware checks, not a fixed protocol convergence guarantee.
  Concurrent warm-up/query loops are not a controlled test of the new cancellation,
  same-name schema recreation, or retained-pager-snapshot boundaries.
- Validated all **51 pinned source file/line links** exist within their source
  files. `git diff --check` passed; because the files are currently untracked,
  separately checked both files for trailing whitespace and final newlines.
  Recomputed the final SHA-256 and verified it matches this report.
- No Rust, Go, database, race, or allocation tests were executed. This was a
  documentation/source review. Existing Go coverage and newly identified precise
  Rust concurrency regressions remain explicitly missing.

The cancellation and schema-generation consequences are deductions from the
verified future/channel/storage control flow, not results of an executed race
reproduction. The report makes no server-side cancellation or eventual-publication
guarantee after session/worker shutdown.
