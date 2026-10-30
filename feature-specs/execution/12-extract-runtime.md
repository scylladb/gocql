# Step 12: extract runtime

Owner: `/root/step12_extract_runtime`; branch: `v2-12-extract-runtime`.
Accepted predecessor: `c3505d578d530ad89e043a539d1be43bb85ff295`.
Exact commands, source bindings, outcomes and log hashes are retained in
[step12-verification.json](../evidence/step12-verification.json).

## Implementation boundaries

`internal/transport` owns actual socket opening, raw/TLS/shard dialing,
STARTUP/authentication, Conn/framer caches, multiplexing, stream retirement,
ordered span writes/coalescing, leased receive dispatch and exclusive incremental
receive. Opening captures immutable identity/settings and narrow event-worker
hooks. Transport has no Session or metadata-loader dependency. Pool reservation
release uses a separate notification contract. Connection I/O in the public
package is a private-core facade retaining the existing Read/Write assertions.

`internal/pool` owns actual host/shard membership, configured capacity, pickers,
exclusive reservations, generation replacement and repair/backoff. It receives
captured settings plus dial/connected/down/worker hooks and never imports cluster
or routing implementation. Target and RoutingInfo are model contracts. No extra
connection is created to provide streaming capacity.

`internal/metadata` owns schema domains/COW maps, cloning/recreation, typed schema
loaders/row plans, validation and CQL type parsing. Its Source interface supplies
encoded system I/O and immutable capabilities. The coordinator supplies a private
adapter. System metadata remains explicitly detached before leased pages are
released; ordinary application and streaming results do not enter this cold-path
materialization loop. Binding uses one shared canonical parameter implementation
in `internal/binding`, rather than a second serializer.

`internal/routing` owns immutable snapshots, lazy locality/latency/LWT plans,
encoded-key hashing and persistent placement indices. It never reaches into
mutable cluster state. Shared dependency clocks/stamps live in
`internal/schemafence`; nongeneric response identity lives in model. Transport
stores that same identity pointer next to embedded ResponseData and still parses
directly into the embedded destination without a second response/payload object.

`internal/cluster` owns actual session/control/topology, metadata publication,
events/subscriptions, client routes, agreement, discovery, observation/reporting
and pool orchestration. It attaches a typed execution owner before startup and
publishes shared schema fences directly, without calling execution under schema
publication locks. `internal/execution` owns actual preparation/cache CAS fences,
admission/input retirement, retries/speculation, batches, paging/prefetch,
streaming progress/no-replay and Close/drain coordination. Generic plans still
use the canonical codec package and encoded model contracts.

Root contains aliases/thin generic functions, consistency parsing and the I/O
facade. Relocated private state is accessed through internal functions; the
[recursive API audit](../evidence/step12-api-opacity-audit.json) includes types
reachable through exported option signatures. It finds no newly exposed Internal
fields/methods. The [package audit](../evidence/step12-dependency-audit.json)
records the actual graph and zero boundary violations. Final public/module/docs
consolidation remains step 13.

## Tests, fault hooks and preservation

Eight existing test files moved beside their execution, transport, routing,
metadata and cluster owners. Cross-owner public integration fixtures remain at
the root. `testkit/transport` constructs actual connection/socket peers and
injects failure through normal poison/retirement; `testkit/cluster` injects loss
of existing configured connections and measures their missing capacity. These
replace public Session fault methods. No test-only compatibility API was added.

New attachment tests cover assignment before startup, failed-startup worker
joining, stop-all-before-join with an outstanding acknowledged writer lease,
admission/Close races and typed pool configuration capture before publication.
The constructor regression verifies default/unset, host/shard overrides, caller
mutation isolation and the unchanged legacy-unset path through actual picker
initialization.

All 174 original identities/body hashes remain. The
[preservation proof](../evidence/step12-baseline-preservation.json) retains strict
normalized assertion tokens under explicit field/accessor, receiver-to-function,
constructor and keyed-literal extraction rewrites. Complete original/current
bodies and helper assertions remain in the
[assertion inventory](../evidence/step12-assertion-source.json). All 957 prior named
test/fuzz/example/benchmark declarations have exactly one location in the
[relocation inventory](../evidence/step12-test-disposition.json).

The [runtime declaration inventory](../evidence/step12-runtime-declarations.json)
accounts for 1,767 predecessor declarations, including explicit ownership moves
and the unused test-only interface removal. Destination filename collisions
occurred while separate portions of source files were moved. They were rebuilt
from the accepted predecessor and subsequent moves merge declarations. The
[bounded collision proof](../evidence/step12-collision-preservation.json) retains
complete before/after source and matching normalized bodies for all 49 repaired
reader/writer/connection-feature/resolver functions; compilation alone is not the
preservation claim.

## Verification and diagnosed boundaries

The initial step12 frozen source contains 618 inputs, SHA-256
`73a8a58156c1b4a825c786ea60dad88486680ebb76c19782fa19057ac7f1b097`.
Full unit race, vet, actual 386 execution, combined build-tag compilation,
arm64 compilation, nested lz4 tests, benchmark-module compilation and focused
runtime/routing copy/cost smokes pass. Arm64 uses an execution substitute and is
compilation only. Smokes include routing scale, warm prepare, caller page storage,
shared admission, segmented independent streaming and raw/compressed request
construction; they are not universal throughput/peak-memory results.

The full unit sweep caught a typed-nil startup-reporter interface and a moved
built-in policy name leaking into configuration reporting. The boundary now
passes a genuinely nil reporter when reporting is disabled and preserves the
original policy label. Existing disabled-reporting and exact-name assertions
pass unchanged.

Initial native runs found pool services capturing nil typed configuration before
bootstrap assignment. Those failed source-bound receipts are retained separately;
all six nodes were cleaned/stopped/joined. The common constructor now captures
the typed snapshot and attaches execution before metadata/event workers, pool
services or policy publication. It does not replace published configuration.
The subsequent runners on that source pass:

| Lane | Top-level pass / skip |
| --- | --- |
| Scylla 2026.3.4 focused constructor/stream v4 | 3 / 0 |
| Scylla 2026.3.4 full selected raw v4 | 149 / 33 |
| Scylla 2026.3.4 full selected mutual TLS v4 | 151 / 31 |
| Cassandra 5.0.9 focused constructor/stream LZ4 v5 | 3 / 0 |
| Cassandra 5.0.9 full selected LZ4 v5 | 158 / 24 |
| Cassandra 5.0.9 full selected LZ4 + TLS v5 | 158 / 24 |

The exact owned CCM lifecycle also passes. Each runner held address locks across
start/tests/namespace cleanup/stop/join. Both exit 0, all six nodes join and all
reserved endpoints refuse connections. Development and unrelated workloads are
untouched. Raw receipts/logs remain under
`/extra/scylladb/gocql-api-test-cluster/step12`, with successful final runs in
`retry/`. Their source bindings remain historical after the lower-settings gate fix below. One final empty line was removed from
a declaration/import/directive-free package alias file; the exact
[format proof](../evidence/step12-format-equivalence.json) retains both byte streams
and identical package tokens. The final source manifest is
[step12-final-source.json](../evidence/step12-final-source.json).

158 original identities have fresh effective applicable-lane passes in the
[current/historical mapping](../evidence/step12-test-mapping.json). The 16
development SELECT-without-FROM identities retain separately bound step09
results; complete final174 matrices remain steps 14/17. All skips remain visible.

Step 13 must regenerate functioning typed historical replay captures and retain
equivalent select/insert workload/assertions while consolidating benchmark
modules. The prior operational replay mismatch remains failed evidence, and
compilation is not a replay benchmark pass. Comprehensive performance remains
step 15. Root-owned STATUS.md and RESUME.md are excluded from this commit.


## Transitive dependency gate fix

Parent review found a real transitive edge missed by the original direct-only
report: transport/pool imported full settings, which imported routing and then
metadata. `internal/connectivity` now owns only physical dial/socket/pool/
heartbeat settings, metadata I/O budgets, and pure authentication/conviction/
reconnect contracts. Full Config, profiles, routing and metadata policy remain
above it. Settings projects its validated immutable descriptors once, before any
workers, pool services or connections publish; transport and pool share that
same lower snapshot. Typed auth and protocol limits remain separately captured.
There are no request/result payload fields in the projection and no extra
connection, serializer, response conversion or gathering path.

The actual TLS snapshot and Unix endpoint validation bodies moved unchanged into
connectivity. Seven pure contract declarations retain identical tokens. The
[bounded preservation proof](../evidence/step12-lower-settings-preservation.json)
retains those bodies/declarations, the two formerly missed dependency paths,
and 16 fixture rewrites whose complete source differs only by explicit settings
projection calls. The actual constructor test still exercises default, host and
shard picker targets after caller mutation, and the unset configuration still
selects the existing default picker. Regeneration also corrects one stale hash
in the 957 declaration inventory; that test source itself is unchanged.

The checked-in dependency auditor now follows every transitive production import
path, including optional integration/debug files. It checks connectivity,
transport, pool and protocol against prohibited owners, including public root
and full settings. The actual graph has zero findings; the prior graph is a
negative control which exposes both routing/metadata paths. Public API opacity,
all 174 baseline body proofs and all 49 collision proofs still pass. The
assertion-source inventory, baseline-body proof and collision-body proof are
byte-identical to the prior verified artifacts.

The gate-fix source freeze contains 622 inputs, SHA-256
`6582f9297904ea596a4319816e3f49ec708667ea495a76db0afcd9b37f40e4ef`,
recorded in [step12-lower-settings-source.json](../evidence/step12-lower-settings-source.json).
Full unit race (32 packages), actual 386 execution, vet, all-tag compilation,
arm64 compilation, benchmark-module compilation and runtime allocation/copy
smokes pass. The unchanged nested lz4 module and routing smoke retain their
previous source-bound results. Fresh native receipts/logs are retained under
`/extra/scylladb/gocql-api-test-cluster/step12/lower-settings/`; the verification
manifest records exact commands, source/log hashes and complete runner cleanup.
The same six native modes pass with the counts in the table above, plus CCM;
158 original identities have fresh effective passes on this gate-fix source.
Both runners exit 0, all six owned nodes join, and an independent check confirms
all 42 reserved endpoints refuse connections. No source delta follows this freeze.
Step 12 remains pending the parent gate review; no later step is advanced here.
