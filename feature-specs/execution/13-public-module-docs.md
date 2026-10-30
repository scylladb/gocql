# Step 13: public facade, module and documentation

Branch: `v2-13-public-module-docs`.
Accepted predecessor: `cdba62aaf867b8e1877aa18dc56b555665bc17d6`.
Gate: passed, pending parent review. Steps 14–17 remain required.

The final native source freeze has 621 inputs, SHA-256
`fc38c0172edc87124a8291303e1bd9208ef70fd7230b487228c6b7f60d646305`.
Headers and Go formatting preceded this freeze. No runtime/source delta follows
it. Runtime input hashes are in [step13-source.json](../evidence/step13-source.json);
[step13-verification.json](../evidence/step13-verification.json) binds the artifact
hashes, successful commands, source, native logs and cleanup receipts. Evidence
and audit tooling under feature-specs are outside the native source manifest.

## Public contract and single module

The normal package exposes 507 declarations. Ninety legacy root exports were
removed, including cluster options, mutable host/pool interfaces, old policy,
authentication, reconnect and compressor variants, SerialConsistency and old
metadata constructors. Typed AuthProvider/AuthExchange, ReconnectPolicy,
ConvictionPolicy, routing/retry/speculation, partitioning, dial/DNS/TLS/address
translation, metadata/events and observation seams remain available. Value-only
metadata/error descriptors and domain utilities remain as permitted by the
contract. The existing typed doc.go summary is unchanged.

Session exposes only AwaitSchemaAgreement, Close, Closed, ID, Metadata, Metrics,
RefreshMetadata, Subscribe and Trace. Old methods became internal package
functions. Test-only aliases/helpers retain private-runtime assertions without
exporting them to consumers. The [public audit](../evidence/step13-public-api.json)
checks required typed exports, removed compatibility names, the exact Session
method set, recursive Internal* escapes and all 130 exported facade functions.
Every audited facade function directly forwards to one implementation; no
serializer, reflection executor, payload conversion or gather was added.
The private-core Conn I/O facade remains unchanged.

There is one `github.com/scylladb/gocql/v2` Go module. Both nested go.mod/go.sum
pairs were removed. Benchmarks now participate in root `./...` commands.
The existing standalone LZ4 implementation and tests moved to
internal/wirecompress; all three source files are byte-identical after the
package declaration rename. Typed cluster startup selects that internal owner.
Existing span-aware LZ4 remains the outbound payload path. No duplicate adapter
compressor was introduced. The [compressor proof](../evidence/step13-compressor-preservation.json)
and [module audit](../evidence/step13-module-audit.json) cover the move and all
actual module/import edges. The old standalone module dependency is absent.

Build, benchmark, coverage, Renovate and release/documentation tooling now use
the single v2 module. Release validation accepts only major 2 and produces one
root candidate/tag; docs release selection excludes old-major/standalone tags.
Release controller tests and documentation-version tests exercise those rules.
No release workflow or remote publication was dispatched.

The [transitive dependency audit](../evidence/step13-dependency-audit.json) still
finds zero prohibited edges below connectivity, transport, pool and protocol.
Full settings/routing/metadata remain above the lower physical configuration.
Snapshot identity, constructor ordering, typed-nil reporting and lifecycle
behavior retain their existing tests and pass in the unit race suite.

## Examples, modes and lifecycle

README and every user guide under docs/source now describe typed preparation,
binding, execution, ownership and configuration. Compression uses enums without
a standalone import. scyllacloud remains an optional typed Config builder; its
actual native cloud test passes in the applicable stable TLS lane. Historical
contracts, reviews and failure evidence retain their original claims.

The [standalone program](../../examples/streaming/main.go) creates its own schema
and validates three streamed rows using seeded payload and nested list storage.
It checks exact contents/backing identity, Reset of unmapped state, multi-page
reuse, one-host pool occupancy, nonblocking Bound.Close, timed-out active Wait,
successful drain, and caller output survival after Close. Discovery permits real
schema agreement; explicit host targeting makes pool occupancy deterministic.
It is invoked with normal `go run` consumer compilation by the integration
harness, without root test aliases. All nine existing example files also compile
as ordinary .go files in a separate consumer module; the
[consumer receipt](../evidence/step13-consumer-examples.json) records exact inputs.

Fresh same-source native lanes run the program, runtime database smoke and
existing comprehensive RunStreamLive lifecycle/interruption assertions:

| Server | Modes | Result |
| --- | --- | --- |
| ScyllaDB 2026.3.4, SMP 2 | v4 raw, v4 TLS, v4 LZ4 | Pass |
| Cassandra 5.0.9, Java 17 | v5 raw, v5 LZ4, v5 TLS/LZ4 | Pass |

v4 LZ4 streaming explicitly returns StreamCapabilityError before submission;
buffered execution still verifies compression. v5 LZ4 streams use bounded
segment transforms. Unit/race fixtures additionally cover v3, splits, integrity,
direct caller backing, capacity errors, cancellation, Close/destination joins,
no replay, paging and pool recovery. The streaming guide specifies seeded outer
slots and nested capacity, Reset preservation, partial destination rules, pool
occupancy and input/output lifetimes. A true NextInto result publishes a complete
validated row; Close joins any in-progress destination access.

Both [stable](../evidence/step13-native-stable.json) and
[Cassandra](../evidence/step13-native-cassandra.json) runners exit zero. Namespace
cleanup passes after every mode, all six nodes join, both exact runner PIDs are
gone, and all 42 reserved endpoints independently refuse connections. The
[cleanup receipt](../evidence/step13-cleanup.json) records this. Install/config
pins and native raw paths are retained in the receipts; no Podman reset or
unrelated workload was touched.

## Genuine typed-control replay

The old page-5000/timestamp capture failure remains failed historical evidence
in step 10/12 and [the old corpus hash record](../evidence/step13-replay-history.json).
It is not relabeled as an operational or performance pass.

New captures were generated through current typed startup on the owned stable
fixture using TestStep13RegenerateReplay. They include page 1000/no timestamp
and the current topology/control setup. Separate ordinal control/data capture
files prevent stream-ID collisions between connections. There are exactly two
connections; Session.Close joins I/O and every recorder's cached Close result is
checked before generation succeeds. TCP socket controls forward to the actual
recording socket. Ordinal selection occurs only at dial setup, with no new
serialization lock in measured execution.

Typed startup repeats system.local. The loader deduplicates only requests and
responses that are each byte-identical apart from stream IDs. It rejects
hash-colliding distinct requests and conflicting repeated results; dedicated
negative tests cover both. Original captured bytes are retained. Existing
unmatched-request panic behavior remains. There is no request fallback, CQL
rewrite or driver serializer introduced for replay.

The original ten SELECT and ten INSERT cases remain. SELECT i returns exactly
one row containing Name_i; INSERT writes `(i, i % 5, Name_i)` and must succeed.
Generation immediately runs the operational test. Three subsequent race runs
produce 60 real case passes. A five-iteration benchmark smoke executes all 20
named benchmark cases successfully. [Replay evidence](../evidence/step13-replay.json)
records generation commands/source/server/config, all capture hashes and actual
operation logs. The reproduction recipe is [tests/bench/README.md](../../tests/bench/README.md).
These smoke results do not replace step 15 comprehensive performance.

Initial new-workload attempts exposed missing recorder TCP-control forwarding
and an example schema-agreement setup error. They were repaired; failed
receipts/logs remain under `/extra/scylladb/gocql-api-test-cluster/step13/` with
their original source hash. Intermediate successful receipts remain under
`step13/final/`; final frozen-source receipts are under `step13/verified/`.
A fixture-guard timeout assumption was corrected to the actual independently
negotiated 30s metadata server clause; repeated operational replay remains the
complete startup compatibility guard.

## Preservation and verification

All 174 complete translated test bodies, 135 helpers and 94 supporting methods
are byte-identical to accepted step 12. All 2,382 accepted test/helper function
and method declarations survive. Two explicit CI/guard renames reflect the
single-module/current-capture policy; nine private unit tests change only method
call form. The [preservation report](../evidence/step13-test-preservation.json)
retains exact hashes and bounded parsed-source equivalence for those call forms.
All 49 collision bodies remain byte-identical. All 1,767 actual runtime-owner
mappings remain present, with internal method-to-function relocation updated;
[runtime preservation](../evidence/step13-runtime-preservation.json) proves the
14 internal source deltas normalize only those calls, the compressor owner name
and comments. No payload operation changed in those bodies.

The [174 mapping](../evidence/step13-test-mapping.json) retains prior 158 step12
and all174 step09 results with their source boundaries. Only the affected native
subset was selected here: cloud has a fresh effective original-identity pass;
other original identities are not claimed as newly run. Skips are explicit.
Full final-tree 174 and streaming acceptance matrices remain steps 14/17.

The [check manifest](../evidence/step13-checks.json) retains exact successful
commands and log hashes for unit race, vet, module tidy/verify, actual 386
execution, all-tag compilation, arm64 compilation, replay/codec/stream/LZ4
benchmark smokes and a warning-free Sphinx build. All-tag/arm64 compile commands
include `ccm`, required by that fixture package's build constraints. Six docs
release-selection tests pass. The nine consumer examples compile separately.

Representative scalar borrowed decoders retain zero per-operation allocations;
stream final-storage/nested-element smokes retain bounded allocation counts as
payload/element counts grow. Existing unit assertions verify caller backing,
copy boundaries, leasing and direct-read ownership. Five-iteration timings are
smoke measurements, not universal speedup, peak-memory or native throughput
claims. Comprehensive performance (15), independent review (16), and full final
acceptance/release verification (14/17) remain required.

Root-owned STATUS.md and RESUME.md are preserved and excluded from the commit.
