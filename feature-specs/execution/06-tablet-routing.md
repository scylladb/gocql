# Step 06: tablets and routing

Owner: `/root/step06_tablet_routing`. Branch: `v2-06-tablet-routing`.
Accepted predecessor: `0ca4fd253c59f4196ba466e41b66ae8d29210bca`.
The authoritative commands, statuses, source and log hashes, protocol references,
and native fixture ownership are in `feature-specs/evidence/step06-verification.json`.

## Routing and protocol behavior

Typed connections negotiate `TABLETS_ROUTING_V2_EXPERIMENTAL` instead of v1 when
advertised. Legacy connections retain v1 until their planned removal. Every typed
EXECUTE on a v2 connection appends exactly one version byte after its normal query
parameters, including protocol 3/4/5 and transparent UNPREPARED repair. QUERY,
PREPARE and BATCH append none. Known versions select a least-significant-first
nibble; unknown table/token ranges probe randomly; absent table/token uses zero.
The captured byte is reusable across attempts. V2 feedback is the v1 tuple plus a
trailing non-null bigint. Negotiated v2 takes precedence; malformed v2 never
falls back to v1 or changes the otherwise valid query result. Feedback is accepted
before interpreting completed EXECUTE errors, including UNPREPARED, and native
QUERY/BATCH responses cannot populate the cache.

Tablet intervals are nonwrapping `(first,last]`, with `first < last`. Declared
UUIDs, shards and tuple/list envelopes validate as a whole. Negative shards,
empty/reversed ranges, truncation and trailing bytes fail. Nonnegative wire
shards retain their original 32-bit values until acquisition validates current
capacity. Overlap replacement removes whole old intervals; adjacent intervals
survive. Unknown ranges remain tablet-based and never use vnode replicas.
Original ordered replicas, opaque version bits and shard hints survive publication.

A persistent AVL index over tables and another over ranges share unaffected
subtrees and replica records. Exact validated duplicates do not allocate or
publish a metadata snapshot, increment its generation, or produce an event.
Changed feedback shares hosts, host/locality indices, keyspace/schema graphs,
vnode placement and unrelated table/range records. Public inspection detaches
mutable slices; normal selection reads private views.

Each response retains its request's table UUID, schema/table revisions and
placement invalidation epoch. Its affected-range observation is checked against
accepted local placement identities; server versions have equality semantics,
never numeric age ordering. Disjoint observations merge; older overlapping
responses cannot overwrite a newer mapping. Both snapshot and current schema
source identity are checked, including the period before a dropped/recreated
object's new snapshot publishes. Actual table UUIDs are required in typed schema
rows. The legacy schema fixture now supplies stable UUIDs too.

Feedback queues own bounded decoded controls and a bounded interval fence,
without retaining response pages or cluster/schema snapshots. The 1,024-entry
queue coalesces equivalent pending observations. Overflow advances an atomic
invalidation epoch immediately, makes placement unknown even to a not-yet-advanced
plan, and asynchronously publishes a bounded diagnostic/resync marker. Later
responses relearn placement. Enqueue never takes the publication mutex; the
tracked consumer starts at session initialization and joins on shutdown.

## Policies and acquisition

Topology publication builds host-ID and DC/rack indices. Built-in validation is
constant-time against these indices. Plans allocate private bounded replica
indices, produce their first target without cloning/scanning/sorting all hosts,
and visit shared fallback tiers only when advanced. Ordinary replica permutations
are private; a policy seed supplies repeatable order when shuffling is disabled.
Conditional and regular SERIAL/LOCAL_SERIAL requests retain deterministic replica
order. Distinct explicit shards on one host survive deduplication.

Connected rack/DC replicas precede allowed remote replicas, followed by connected
local nonreplicas, allowed remote nonreplicas and enabled disconnected fallback.
Remote replicas precede local nonreplicas. Missing configured locality fails
validation. Disabled/filtered hosts never reappear in fallback. Latency samples
use per-host locks and publish decision snapshots at bounded intervals; threshold
equality, sample minimums, retry expiry and LWT/leader penalties are covered.

Globally consistent tablet keyspaces promote the first original **v2** replica
only with token awareness, regular consistency other than ONE/LOCAL_ONE,
connected/enabled/locality-eligible membership and a valid shard. A missing
original leader never promotes a resolved follower. Missing followers do not
hide a known leader. Penalized/ineligible leaders use ordinary safe fallback.
Promoted leader targets preserve their strict shard through acquisition.

Target.StrictShard now distinguishes strict selection from an advisory replica
hint. Options.ShardID remains strict. Typed and transitional picker paths and
exclusive reservations preserve strict host/shard targets, wait for existing
reserved capacity, and cannot create side connections or silently relax a target.
Unsigned validation precedes conversion to int, including hostile uint32 targets.

Typed vnode precomputation uses distinct host UUIDs and the specified NTS rack
repeat budget, retaining global ring order for LWT. Factors are capped by available
hosts. Canonical encoded parameter spans feed Murmur3, CDC and custom partitioners;
binding retains the preparation's selected partitioner instead of consulting later
metadata. Unknown names and scopes unable to attest a table's partitioner disable
token routing with a diagnostic. No alternate serialization is introduced.

## Requirement evidence and limits

| Boundary | Evidence |
| --- | --- |
| Wire and hostile input | Every-byte split, all truncated prefixes, v1/v2 tuples, opaque nibble bits, negative shards, strict ranges, per-connection v3/v4/v5 body length and opcode exclusions. |
| Placement and ownership | Duplicate snapshot identity, unchanged backing records/host/schema indices, smaller opaque versions, independent disjoint updates, split/adjacent intervals, epoch/source drop/recreate fences, immediate overflow/relearning, bounded queue and no publication-lock wait. |
| Policy and capacity | Locality/remote ordering, conditional determinism, original leader/follower failures, invalid leader shards, LWT/leader latency ordering, distinct shard destinations, disconnected fallback, strict/advisory selection and exclusive capacity wait/release. |
| Tokens/vnodes | Existing canonical composite/CDC/signed Murmur vectors, custom partitioner payload backing identity and captured choice, duplicate vnodes and exact NTS rack budget/factor vectors. |
| Concurrency | Focused repeated race tests cover plan advancement against concurrently replaced placements; the full uncached unit race suite includes metadata/control/drop/event and pool regression tests. |
| Cost | First-target host scaling, changed/duplicate publication with independent table/range scaling, and bounded/coalesced feedback queue benchmarks. |
| Live server | Native task-owned Scylla 2026.3.4 ordinary v1 and experimental v2 lanes; independently read system.tablets replica sets, retained original TestTablets assertions, and 16 stable LOCAL_QUORUM reads to the leader decoded directly from a v2 payload. |

The first-target benchmark includes plan creation/validation/first Next and retains
one 768-byte allocation with 3, 30 and 300 hosts, including ordinary, conditional,
leader, latency and pinned cases. That constant setup allocation is not described
as an allocation-free request. Exact duplicate publication and warmed queue
coalescing measure zero allocations. Changed table/range index updates grow
logarithmically; the benchmarks separately vary 1/1,024 placement tables and
1/1,024/16,384 ranges. They include metadata control/index work and exclude fixture
construction and row payloads; they do not establish whole-cluster throughput.

Final required checks include full uncached unit race, focused race twice, vet,
integration/CCM race builds, bounded two-worker payload fuzz smoke, benchmarks and
both complete live lanes. The additional 386 root build probe cannot reach the
shard test because unchanged legacy cqlint/varint files contain int constants that
do not compile on 386. Its failure is retained and is not a portability pass.

Initial live diagnostics exposed fixture assumptions: absence of v1 feedback can
mean a cold request already hit an owning shard; RF equal to host count can keep
that true indefinitely. Warm-up now deliberately probes existing host/shard
slots while preserving the original stable membership/shard assertions. Another
probe counted only read feedback even though prepared writes had already warmed
v2 placement; it now counts both. Original tablet unit expectations for inclusive
lower bounds/empty intervals were corrected to the specified exclusive lower
bound, with explicit exclusion and first-contained-token assertions. Failed
lanes and their partial passes are retained as diagnostics, never final passes.

No assigned step 06 feature is deferred. Public RunStream composition,
tri-state unknown/conditional classification, speculation/retry orchestration,
pager-wide admission/snapshot semantics and complete execution shutdown remain
step 07. Steps 08–17 retain their fixture infrastructure, complete 174-test
migration, extraction, full database matrix, equivalent execution performance and
release-wide final-tree audits. This focused routing gate replaces none of them.
