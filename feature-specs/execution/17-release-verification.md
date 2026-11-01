# Step 17: final release verification

Branch: `v2-17-release-verification`. Accepted predecessor:
`bc3d9fe132c4c1f5dc16050870cfd4991d322a73`.
Gate: passed candidate, pending root audit before completing the user goal.
All required current-source runtime/native gates and the independent artifact
verifier passed. No publication is authorized.

Frozen final runtime: 663 inputs, SHA-256
`74a54ef2ae1485fb2f7065efee1ffb10218efed431f331b0e579f70ba4285180`.
All final commands and fixtures use this complete input pin. Documentation,
evidence and verification methods are separately bound to their final bytes.
Final raw artifacts are retained under
`/extra/scylladb/gocql-api-test-cluster/step17/release/`.
The candidate manifest is [step17-verification.json](../evidence/step17-verification.json).
It binds over 1,800 raw artifacts and the separate report/method/documentation bytes.

## Changes and findings

S17-1 is a test-oracle correction. The original stable Snappy/TLS package
reported that key 3 never learned a previously read authoritative replica set.
Server logs show migration of that exact table/token range during the test;
the original cache/hints were not logged, so migration is a correlation and the
sole failure cause remains unproven. A single mutable server snapshot was
nevertheless an invalid permanent oracle. The test now brackets each routed
query with independent reads pinned to every node. Transition state, pending
replicas or disagreement receive no credit. It retains three cold host pins,
then requires eight consecutive exact current cache/coordinator matches within
96 total attempts. Persistent instability fails. Set equality rejects duplicate
replicas in either direction and wrong shards; a separate regression covers it.
No tablet movement was disabled and no original baseline assertion was removed.

S17-2 is a production latency-policy correction. Positive MaxDuration values
are accepted configuration, but absolute UnixNano deadlines wrapped after 2262.
A correctly aged sample reproduced 32 concurrent probes instead of one;
MaxDuration UpdateInterval also republished before its deadline without a new
minimum. The probe deadline now uses full time.Time under the existing
per-host sample mutex, shared with Observe. Publication deadlines use immutable
time.Time pointers and CAS. No duration is clamped or rejected, no policy-wide
completion mutex/timer is added, and ordinary disabled/nonexpired decision
branches remain unchanged. Current tests cover ordinary monotonic clocks,
2262 crossings, MaxDuration, concurrent claims, Observe, abandoned expiry,
backwards clocks and losing publication claims.

One fresh Cassandra v5 raw/TLS package later failed the protected materialized
view recreation cleanup: DROP succeeded, then automatic agreement/refresh hit
the unchanged deadline. Server transformation logs alone do not establish the
versions returned by system.local. Three focused logging cases and one full
logging package passed, so no causal runtime/harness defect was established.
The original failure, exact overlays and unknown transient cause remain visible.
One explicit fresh unmodified whole-package verification uses the same source,
assertions and bounds; it supplies credit only if the entire package and cleanup
pass. No successful DROP is retried, timeout raised, fatal/golden weakened or
logging-overlay pass relabeled as final evidence.

The three new test files additionally prove actual Config/Options effects:
STARTUP identity, REGISTER event switches, timestamp reuse across retry/pages
and explicit-zero override, positive/zero socket settings, verified SNI proxy
selection, explicit latency threshold, raw read-ahead upper bound and EXECUTE
metadata inclusion. They use actual wire/socket operations and explicit output
assertions. The original 174 bodies and their protected helpers/methods remain
unchanged. The production delta is confined to
`internal/routing/v2_policy.go`.

## Final acceptance evidence

The final reports are generated only from successful current-source complete
packages and retained commands. Earlier 660/662-input results, interrupted
matrix receipts, targeted tablet diagnostics and preliminary measurements keep
their own source bindings and supply no final pass credit.

The requirement inventory retains every paragraph, list item, table row and
code declaration/field: 2,790 entries across plan.md, the three Go contracts and
all six reference contracts. Historical Rust descriptions, staging examples and
Go obligations have explicit dispositions. Shared proof dossiers retain complete
current assertion sources, concrete declarations and effective package passes;
keyword matches or broad-suite success alone do not establish completion.

The release verifier requires all 174 original identities and every expected
translated nested case to have effective passes in successful applicable final
packages. Parents whose terminal descendants all skip receive no behavioral
credit. Version/fixture skips retain their exact text. Required modes are stable
Scylla 2026.3.4 v4 raw/Snappy/LZ4 with plaintext/TLS; development 2027.1 v4 with
all SELECT-without-FROM and actual tablet-v2/global/leader behavior; Cassandra
5.0.9 v4/v5 raw/LZ4 with plaintext/TLS; raw v3 maintenance/streaming, v3 TLS
streaming and actual CCM lifecycle in one live owner. Each Cassandra mode uses
fresh owned data. Scylla v5 rejection never supplies successful v5 credit.
All 17 modes, 11 selected successful fixture receipts, 135 protected helpers and
94 supporting methods satisfy the final gate. The earlier failed package is
retained separately and receives no pass credit.

Every streaming criterion is rechecked on this pin: eight deterministic/live
supported modes, every split/truncation/CRC and required-unit first-row gate,
final-range identity, seeded nested/custom/encoded capacities and resets,
metadata fences, no replay after output, exclusive capacity and handoff,
control dispatch, final destination-write joins, actual interruption/recovery
and independent bounded storage across all 40 shapes.

The general gates include default/unit/full unit race, actual 386, arm64,
complete tag-union linking, vet, one-module/dependency/API/nameability and normal
external consumer checks, examples, Sphinx/release-tool tests, fixture Python,
module verification/drift, 60 exact replay case passes and 20 benchmark smokes.
Thirteen new per-target fuzz caches bind builtin seeds, complete oracles,
observed budgets/executions and retained mutations; cancellation runs 60 seconds
and other targets 30 seconds. Saved mutations are not every in-memory execution.

## Performance and limitations

Current evidence covers all 23 implementation performance/scaling rows and
affected latency-deadline costs. Equivalent Run/Row, buffered NextInto, RunStream
and the pinned original driver use identical validated output and actual
negotiated protocol/compression/TLS/shard capacity. Normal timing excludes forced
GC, samplers and profiles; diagnostics remain separate. Direct byte capacities,
post-GC retained heap, HeapAlloc garbage, TLS/source/harness and final application
storage are distinguished. Copy/ownership/algorithm counters remain correctness
gates; no timing improvement can waive them.

Probe checks allocate zero. Warm publication claims allocate zero; due and
competing publication candidates allocate 24 bytes each, including losing CAS
candidates. Integer allocs/op can round a fractional nonzero rate to zero, so
fixed-operation allocation profiles and claim counts are retained. The existing
per-host mutex adds bounded work in forced-expiry contention, with 3/30/300-host
and large-clock measurements. These costs are explicit; no universal speedup or
stable tail claim follows from shared-host empirical samples.
The deliberately ever-expiring concurrent clock workload measures current/old
median ratios of 1.24, 1.13 and 1.72 at 3, 30 and 300 hosts, with unchanged
768 B/one plan allocation. This is a correctness tradeoff in that contention
case, not a default-path speedup: ordinary 300-host selection is 1.01 and warm
Observe remains allocation-free. The separate fixed 250,000-operation profile
attributes 250,002 candidate allocations versus 91,696 winning claims;
setup/calibration remain included, so this is not an exact logical-request
allocation count.

Large signed-int32 protocol limits and >2 GiB completion arithmetic use logical
repeated backing and actual 386 execution. No physical multiGiB transfer is
claimed. Cookbook examples compile through normal public builds; output-bearing
examples execute in ordinary suites, and the standalone seeded streaming CLI
executes against applicable native modes with verified cleanup.

All fixtures/children/supervisors must be joined, exact owned endpoints refused,
and artifact/source/configuration/profile hashes verified before this gate can
close. Negative controls reject missing or falsified requirements, packages,
subcases, modes, campaigns, performance rows, joins and hashes. Root-owned
STATUS.md and RESUME.md are preserved outside this change. No push, deployment,
workflow or external message is performed.
Final independent cleanup found 272 joined/gone recorded processes, all 63 owned
endpoints refused connections, and no matching fixture/run process survived.
The verifier rejected all 17 negative controls. Thirteen fresh fuzz campaigns
executed 20,817,426 inputs and retained 530 interesting mutations. Current cost
evidence contains 23 requirement rows, 522 matched native results, 126 parked
memory cases and 174 profiles. These counts are scoped evidence, not substitutes
for the individual assertions, guards and manual requirement confirmations.
