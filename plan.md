# gocql v2 plan

Status: API and architecture reconciled through ten design review-and-fix cycles
in this session: 33 verified findings fixed, with no unresolved findings from
those cycles. See the [individual design review
reports](feature-specs/reviews/go-v2-design/). The user authorized implementation
to resume on 2026-10-29, with preparation followed by fresh agents for each
sequential task. Existing edits are preserved in checkpoint `661f169`; design
review does not claim the new driver is implemented or verified. Streaming
API/task reconciliation below is a new preparation addition, outside the
historical review cycles. Additional
hot-path design reviews and fixes are recorded in the [hot-path review
ledger](feature-specs/reviews/go-v2-hot-path/INDEX.md); these are design contracts,
not measured performance results.

## 1. Goal and scope

Build a clean Go driver under `github.com/scylladb/gocql/v2`, package `gocql`.
There is **no backward-compatibility requirement** with the v1 API.

The driver must provide:

- Generic, type-checked serialization and deserialization, including custom codecs.
- Borrowed result views and canonical encoded inputs reused by routing, retries,
  speculative attempts, batches, and paging.
- Direct decoding into a caller-supplied representation, with variable-size
  result payloads remaining in leased receive buffers.
- [Streaming receive decoding into final caller storage](feature-specs/gocql-v2-streaming-receive.md),
  without complete response/page buffering; bounded protocol, integrity and
  transform staging remains explicit. The API and sequential ownership are
  reconciled; implementation and acceptance evidence remain pending.
- Token, replica, shard, tablet, and LWT-aware routing; connection pools; topology
  and schema metadata; a control connection; events; retry and execution policies.
- Cassandra and Scylla native-protocol support, compression, TLS, authentication,
  cloud/SNI and client-route connectivity, tracing, and metrics.
- Direct migration of all 174 original integration/CCM tests, preserving their
  behavioral coverage and recording intentional API-semantic changes.

The reviewed Rust feature specs describe the reference implementation. Go v2 may
improve its validation, ownership, cancellation, and failure behavior;
accidental Rust limitations are not requirements to reproduce.

## 2. Design decisions

| Area | Decision |
| --- | --- |
| Go version | Go 1.25 initially, matching the current workspace/toolchain. |
| Public module | One driver module: `github.com/scylladb/gocql/v2`. |
| Public packages | `gocql` for the driver and codecs; `scyllacloud` for optional cloud configuration. |
| Generic operations | Package-level generic functions; methods only use their receiver's existing type parameters. |
| Request model | Immutable prepared handles and encoded bindings; per-call options replace mutable query setters. |
| Codec model | Explicit typed accessors/custom implementations; no reflection or `[]any` on execution paths. |
| Result model | Typed rows, including caller-supplied destinations via NextInto; borrowed text/blob/container views with explicit retention and cloning. |
| Routing input | Hash the same encoded spans that are transmitted, without serializing values a second time. |
| Metadata | Immutable snapshots with generations; stale updates cannot overwrite newer metadata. |
| Errors | Typed errors compatible with `errors.Is`/`errors.As`; malformed input returns errors rather than panics. |
| Compatibility | Remove public `Query`, `Iter`, reflection-based `Batch`, `Scan`, `MapScan`, `Marshal`, and `Unmarshal`. |
| Migration | Reuse correct lower-level machinery, but remove legacy execution paths after their callers migrate. |

The current experimental `v2_*.go` files are implementation scaffolding. Their
location in the root package is not the intended final internal organization.

## 3. User-facing API

### Configuration and session lifecycle

```go
func DefaultConfig(contactPoints ...string) Config
func Connect(ctx context.Context, config Config) (*Session, error)
func OpenSingleHost(ctx context.Context, config Config, host UUID) (*Session, error)

func (s *Session) Close() error
func (s *Session) Closed() bool
```

`Config` groups connection, pool, metadata, protocol-limit, and
execution-profile settings. It includes TLS/authentication, compression, DNS,
address translation, host filtering, client routes, identity/reporting, and
observers.

`Connect` validates and snapshots configuration. Later mutations of the caller's
configuration do not change an established session. Explicit profile replacement
is the supported mechanism for changing execution defaults.

Relevant transport hooks are:

```go
Dialer      Dialer
DialHost    func(context.Context, DialTarget) (net.Conn, error)
TLS         *tls.Config
TLSForHost  func(context.Context, HostSnapshot) (*tls.Config, error)
Auth        AuthProvider
```

`DialHost` opens a raw connection and takes precedence over `Dialer`. DialTarget
contains the host identity, selected network/address, local source address/port,
and optional shard target; the hook must honor that endpoint and source
selection or return an explicit unsupported-setting error. The driver applies
selected TLS settings afterward and performs authentication. TLS verifies the
server by default. The detailed transport hook contract is in the implementation
contract.

### Statements, preparation, and binding

```go
type Statement[P, R any] struct {
    CQL    string
    Kind   StatementKind // unknown / ordinary / conditional
    Params ParamEncoder[P]
    Rows   RowDecoder[R]
}

func Prepare[P, R any](
    ctx context.Context, session *Session,
    statement Statement[P, R], options PrepareOptions,
) (*Prepared[P, R], error)

func BindBorrowed[P, R any](*Prepared[P, R], P) (*Bound[R], error)
func BindOwned[P, R any](*Prepared[P, R], P) (*Bound[R], error)

func BindUnpreparedBorrowed[P, R any](Statement[P, R], Columns, P) (*Bound[R], error)
func BindUnpreparedOwned[P, R any](Statement[P, R], Columns, P) (*Bound[R], error)

func (b *Bound[R]) Close()
func (b *Bound[R]) Wait(context.Context) error
func (b *Bound[R]) Routing() RoutingInfo
```

- Preparation obtains parameter/result metadata and compiles typed plans.
- Binding validates and encodes once. Host-specific prepared IDs and response
  metadata IDs are attached later, separately from the encoded values.
- Borrowed binding retains references to caller storage. The caller must not
  mutate or recycle it until `Close` has been called and `Wait` succeeds.
- Owned binding explicitly detaches caller-backed payloads during encoding.
- Unprepared statements require declared input types because QUERY supplies no
  prepared parameter metadata. Statements without parameters use `NoParams`.
- Rebinding creates a new binding. A live binding is never mutated in place.

### Execution and paging

```go
func Run[R any](context.Context, *Session, *Bound[R], Options) (*Rows[R], error)
func RunPage[R any](context.Context, *Session, *Bound[R], PageState, Options) (*Page[R], error)
func RunUnpaged[R any](context.Context, *Session, *Bound[R], Options) (*Page[R], error)
func Exec[R any](context.Context, *Session, *Bound[R], Options) (Result, error)

func (r *Rows[R]) Next() bool
func (r *Rows[R]) NextInto(dst *R) bool
func (r *Rows[R]) Row() R
func (r *Rows[R]) Err() error
func (r *Rows[R]) Close() error
func (r *Rows[R]) Columns() Columns
func (r *Rows[R]) Info() Result
func (r *Rows[R]) Retain() (*PageLease, error)
func (r *Rows[R]) Clone() (R, error)
```

`Page` exposes the corresponding row operations and `State() PageState`. `Run`
performs bounded automatic paging; `RunPage` returns exactly one page;
`RunUnpaged` rejects an unexpected continuation. Page state distinguishes start,
continuation, and completion, including an empty continuation token.

`Exec` is for statements without result columns. Conditional statements return
typed rows rather than discarding their result through `Exec`.

Rows stop on the first error. Automatic paging handles empty nonterminal pages
and rejects any repeated continuation state, including nonconsecutive cycles.
Configurable count/byte budgets bound the retained state history; exhaustion
returns a terminal paging-state-limit error. Manual paging exposes those pages
and their states without retaining cross-call history or inventing rows.

`Rows.NextInto(dst)` and `Page.NextInto(dst)` advance and decode directly into
the supplied instance using the same compiled column mappings as `Next`.
Built-in field plans write into that destination without an intermediate row
or a decode-then-assign adapter. Text/blob and lazy collection fields reference
immutable leased receive storage, including segmented values; fixed scalars
are decoded into Go values. This is required planned functionality, not an
assertion that the current implementation already exposes it.

```go
var user UserView // application-defined fields use Text/Bytes/container views
for rows.NextInto(&user) {
    consume(user) // borrowed payloads; no String/OwnedBytes/Clone required
}
if err := rows.Err(); err != nil {
    return err
}
```

Each call replaces the destination's contents; false clears a non-nil
destination so a partial or stale row is not exposed. Nil is a terminal
ValidationError. Borrowed views expire at the next `Next`/`NextInto` or Close
unless Retain pins their page. Retaining bytes does not preserve a destination
that is later overwritten: copy its view descriptors to another value first.
After success, Row and Clone read the current supplied destination; the pager
may hold its pointer only until the next advance or Close. See the detailed
API contract for mixed iteration, failure and concurrency rules.

The separate [streaming receive contract](feature-specs/gocql-v2-streaming-receive.md)
requires decoding as bytes arrive into final caller storage without assembling
a complete response/page. Execution preparation settled the separate public API:

```go
func RunStream[B, R any](context.Context, *Session, *Bound[B], StreamRowDecoder[R], StreamOptions) (*RowStream[R], error)
func (s *RowStream[R]) NextInto(*R) bool
func (s *RowStream[R]) Err() error
func (s *RowStream[R]) Close() error
```

StreamRowDecoder compiles an immutable streaming plan with Reset/DecodeInto;
explicit scalar/text/blob/nested builders target preseeded caller storage and
return typed capacity errors without implicit growth. Raw v3/v4 and v5 raw/LZ4,
with optional TLS, are required; v3/v4 frame compression fails before submission.
The cursor holds one exclusive configured pool capacity unit across pull-driven
pages. Bounded metadata/read-ahead/discard/transform budgets, cancellation-safe
read ownership and no replay after output are normative. Buffered NextInto alone
does not provide it. The detailed API and task gates are settled in the feature
and API contracts; this addition was outside the historical review cycles and
remains unimplemented/unverified.

### Codecs and row composition

```go
type Encoder[T any] interface {
    CheckType(DataType) error
    Encode(*ValueWriter, T) error
}
type Decoder[T any] interface {
    CheckType(DataType) error
    Decode(ValueReader) (T, error)
    Clone(T) T
}
type Codec[T any] interface { Encoder[T]; Decoder[T] }

type ParamEncoder[P any] interface {
    Compile(Columns) (ParamPlan[P], error)
}
type ParamPlan[P any] interface {
    Encode(*ParamWriter, P) error
}
type RowDecoder[R any] interface {
    Compile(Columns) (RowPlan[R], error)
    Clone(R) R
}
type RowPlan[R any] interface {
    Decode(*RowReader) (R, error)
    DecodeInto(*RowReader, *R) error
    Clone(R) R
}

func Param[P, V any](string, Encoder[V], func(P) V) InputField[P]
func Params[P any](...InputField[P]) ParamEncoder[P]
func Column[R, V any](string, Decoder[V], func(*R) *V) OutputField[R]
func Row[R any](...OutputField[R]) RowDecoder[R]
func OneParam[T any](Encoder[T]) ParamEncoder[T]
func OneColumn[T any](Decoder[T]) RowDecoder[T]
```

Provide positional variants, ignored columns, pair composition with explicit
column arity, and typed mapping adapters. Names and indexes are resolved during
compilation, not on every row. `RowDecoder.Clone` must work independently of a
previous metadata compilation; caching an arbitrary first plan is not
sufficient.

Supported value families:

| Family | Public representation |
| --- | --- |
| Fixed scalars | Exact-width Go numbers, bool, UUID, TimeUUID, Counter. |
| Time and numeric domains | Date, TimeOfDay, Timestamp, Duration, Varint, Decimal; explicit standard-library conversions. |
| Text/blob | Borrowed `Text` and `Bytes`; explicit string/byte materialization. |
| Null/unset/empty | `Nullable[T]`, `BindValue[T]`, `EmptyValueOf[T]`; states remain distinct. |
| Collections | Typed list/set/map encoders; lazy `Sequence[T]` and `Mapping[K,V]` decoders. |
| Tuples/UDTs | Explicit parameter/row compositions; checked names, identity, field evolution, and nullable suffixes. |
| Vectors | Exact dimensions and native fixed/variable-width layouts, including nested vectors. |
| Dynamic schemas | Tagged `Value` and ordered `DynamicRow`; no `map[string]any` execution representation. |

Standalone serialization uses the same implementation as live execution:

```go
func EncodeValueBorrowed[T any](DataType, Encoder[T], T) (EncodedValue, error)
func EncodeValueOwned[T any](DataType, Encoder[T], T) (EncodedValue, error)
func DecodeValue[T any](Decoder[T], EncodedValue) (T, error)
```

### Example: a typed prepared read

```go
type Key struct { ID int32 }
type User struct { ID int32; Name gocql.Text }

statement := gocql.Statement[Key, User]{
    CQL: "SELECT id, name FROM app.users WHERE id = ?",
    Params: gocql.Params(
        gocql.Param("id", gocql.Int32, func(k Key) int32 { return k.ID }),
    ),
    Rows: gocql.Row(
        gocql.Column("id", gocql.Int32, func(u *User) *int32 { return &u.ID }),
        gocql.Column("name", gocql.TextCodec, func(u *User) *gocql.Text { return &u.Name }),
    ),
}

prepared, err := gocql.Prepare(ctx, session, statement, gocql.PrepareOptions{})
if err != nil { return err }
bound, err := gocql.BindOwned(prepared, Key{ID: 42})
if err != nil { return err }

rows, err := gocql.Run(ctx, session, bound, gocql.Options{})
if err != nil {
    bound.Close()
    _ = bound.Wait(context.Background())
    return err
}
for rows.Next() {
    user := rows.Row()
    fmt.Println(user.ID, user.Name.String()) // explicit owned string conversion
}
readErr := rows.Close()
bound.Close()
drainErr := bound.Wait(context.Background())
return errors.Join(readErr, drainErr)
```

Applications retaining a row beyond advancement use `Clone`, or retain the
physical page before advancing and close the returned `PageLease` afterward.

### Options, batches, metadata, and policy extension points

`Options` carries profile selection and explicit overrides for consistency,
serial consistency, timeout/page timeout, page size, bounded prefetch,
idempotence, timestamps, keyspace, tracing, payloads, host/shard targeting,
metadata inclusion, retry policy, and observers. Optional wrappers distinguish
an inherited value from an explicit zero or omission.

```go
func NewProfile(Profile) (*ProfileHandle, error)
func (p *ProfileHandle) Snapshot() Profile
func (p *ProfileHandle) Replace(Profile) error

func BatchEntry[R any](*Bound[R]) (BatchItem, error)
func RunBatch[R any](context.Context, *Session, BatchKind, []BatchItem, RowDecoder[R], Options) (*Page[R], error)
func ExecBatch(context.Context, *Session, BatchKind, []BatchItem, Options) (Result, error)
func CASRows[R any](RowDecoder[R]) RowDecoder[CASResult[R]]

func (s *Session) Metadata() *MetadataSnapshot
func (s *Session) RefreshMetadata(context.Context) error
func (s *Session) AwaitSchemaAgreement(context.Context) error
func (s *Session) Subscribe(EventFilter) (*Subscription, error)
func (s *Session) Metrics() MetricsSnapshot
func (s *Session) Trace(context.Context, UUID) (TraceInfo, error)
```

Profiles are snapshotted at admission and remain stable through retries and
later pages. Batch members reuse encoded bindings; all admissions succeed
together or are released before sending. Conditional batches expose every result
row.

Routing, retry, speculation, timestamp generation, reconnect, authentication,
logging, and observation are replaceable interfaces. Built-in configuration must
cover local DC/rack preference, explicit remote failover, replica shuffling,
latency filtering, deterministic LWT order, shard hints, and custom
partitioners. Unknown and conditional statements/batches must not be speculated;
explicit or reliable server-confirmed ordinary classification plus idempotence
is required. A configuration option must either work or return an explicit
unsupported-setting error; silent no-op options are unacceptable.

## 4. Ownership and zero-copy contract

| Object | Ownership and lifetime |
| --- | --- |
| Config/profile metadata | Validated snapshots; shared callback/provider concurrency documented. |
| Prepared handle | Immutable bind schema plus synchronized host-specific IDs/result generations. |
| Borrowed binding | Caller payload remains immutable until Close + successful Wait. |
| Owned binding | Caller-backed spans are copied explicitly once; already-owned encoding is reused. |
| Active request | Holds an admission through retry, queued writes, speculative losers, and paging; the same drain covers borrowed per-call custom payloads. |
| Result row | Borrowed views are valid until next Next/NextInto or Close unless the page is retained; filling caller storage does not transfer ownership of payload bytes. |
| PageLease | Pins physical receive storage independently of the request/session lifetime. |
| Clone | Explicitly detaches data; it must not retain a hidden pooled-buffer dependency. |

`Bound.Close` is nonblocking and prevents new admissions. `Bound.Wait` drains
admitted work. A timed-out Wait does not end the caller's borrowing obligation.
Output leases do not pin input buffers. Closing a session does not invalidate a
properly retained result page.

The row-advance rule is an application lifetime contract. The implementation may
physically retain bytes until the whole page is released; callers must not use
that as an extension of the documented lifetime. Released page owners are
detected; concurrent use while releasing the last owner is prohibited.

Zero-copy applies to driver-managed payload handling on every supported path:

- Encode once into owned scalar/header slabs plus borrowed variable-data spans.
- Hash those spans directly, including composite partition keys.
- Write spans without assembling another contiguous request buffer.
- Compress directly from spans; attach compressed output without copying it back.
- For incompressible v5 segments, send original spans rather than copying them.
- Receive into leased buffers; retain segment slices instead of gathering split frames.
- Decode text, blobs, collections, and vectors through segmented views.

Scalar encoding, compression/decompression, explicit owned conversions, OS I/O,
and standard Go TLS record processing are necessary transformations or explicit
ownership operations. Account for them separately. Zero allocations alone is not
proof of zero payload copies; verify backing-storage identity and copy
boundaries. Hot-path work must also satisfy the implementation contract's cost
bounds: shared range descriptors and linear sequential traversal; recursively
compiled codecs and reused result plans; lazy first-target routing over private
indices; hash-indexed paging history; direct warm prepared-state access;
amortized ownership, observation and scheduling; and structurally shared
placement feedback updates. Explicit public materialization/inspection APIs
remain separate from internal execution.

## 5. Module and package structure

Use one new Go module. Keep optional cloud support in a subpackage. V2 should
not depend on the old driver's standalone compressor module for its new data
path; compression implementations and tests belong under the new module.
Consolidate the benchmark module after migration rather than introducing more
nested modules.

```text
github.com/scylladb/gocql/v2
├── go.mod
├── config.go, session.go                 public lifecycle facade
├── statement.go, batch.go, result.go     typed preparation/binding/execution
├── codecs.go, values.go, rows.go         public generic codec facade
├── profiles.go, policies.go              public policies and execution options
├── metadata.go, events.go, auth.go        public snapshots and extension points
├── errors.go, tracing.go                 public errors and observability
├── scyllacloud/                          bundle → Config, per-host dial/TLS
├── internal/
│   ├── cqltype/                         immutable CQL descriptors/value domains
│   ├── codec/                           generic scalar/container/row implementations
│   ├── model/                           encoded request/page/routing contracts
│   ├── execution/                       admission, retries, speculation, paging
│   ├── cluster/                         session-owned control and topology orchestration
│   ├── metadata/                        schema loading, validation, snapshot merge
│   ├── routing/                         replica plans, locality, tablets and leaders
│   ├── pool/                            per-host/per-shard state and repair
│   ├── transport/                       connections, handshake, multiplexing, writes
│   ├── protocol/                        checked native request/response codecs
│   ├── wire/                            Span, Chain, Builder, Cursor, Lease
│   ├── segment/                         native-v5 segmentation and checksums
│   ├── wirecompress/                    span-aware LZ4/Snappy transforms
│   ├── murmur/, crc/, lru/, eventbus/     focused reusable primitives
│   └── testkit/                         protocol peers, fault injection, fixture helpers
├── tests/
│   ├── integration/                     public API/database behavior
│   └── bench/                           codec/routing/transport/end-to-end benchmarks
├── testdata/                            golden wire data and test certificates
├── feature-specs/                       reference contracts, reviews and evidence
└── plan.md
```

This is the **target** structure, not a claim that these moves are complete.
First stabilize behavior in the current working tree; then extract packages in
dependency order with compilation/tests at each step. Keep unit tests beside the
implementation they inspect. Move integration tests toward public behavior;
dedicated internal test hooks support genuine fault injection, not
compatibility.

### Dependency rules

1. `wire`, `crc`, and `murmur` have no dependency on the public driver package.
2. `cqltype` owns shared type descriptors. `codec` depends on `cqltype` and `wire`.
3. `model` contains nongeneric request/page contracts using these shared types.
4. `protocol` and `transport` depend on lower-level contracts, never public Session.
5. `pool` depends on transport and endpoint descriptors, not cluster orchestration.
6. `metadata` accepts a typed internal query interface; it cannot call public
   Session methods or legacy reflection serialization.
7. `cluster` coordinates control, metadata, pools, and events. Routing consumes
   immutable snapshots rather than reaching into mutable cluster internals.
8. `execution` coordinates cluster/routing and transport; public generic facades
   compile codecs and hand it encoded inputs and decode plans.
9. `scyllacloud` depends on the public API; the public API never imports it.

Public aliases/facade functions may expose the shared implementation types. They
must not add payload conversion, reflection, or a second serializer.

## 6. Internal execution structure

```text
Statement + codecs
  → Prepare: validate server parameter/result metadata; compile plans
  → Bind: canonical encoded cells/spans + input owner + routing token
  → admission: snapshot profile/options/topology; retain input ownership
  → query plan: eligible host and shard
  → attempt: host-specific prepare IDs + envelope + original value spans
  → transport: stream reservation → ordered span write → checked response
  → response page: immutable metadata + row spans + independent page owner
  → typed row decoder → borrowed values / explicit retained page / owned clone
```

### Transport and pools

Maintain distinct ownership of outbound data, stream IDs, and inbound pages.
After cancellation, an accepted writer must finish or fail before releasing
input storage. Stable request identity and cancellation state survive
queued-to-stream assignment; delayed cancellations cannot affect a reused
stream. Orphan budgets count all abandoned transmitted streams and close the
connection when exceeded (default budget 1,024, clamped below the effective
stream cap; zero closes on the first orphan). A transmitted request keeps its
stream reserved until its late response arrives or the connection closes.
Loser/prefetch/orphan responses release exactly once. Partial writes poison the
connection.

Pools expose explicit initializing/ready/broken/closed states. Host and shard
targets are distinct from pool sizing. Implement and test per-host overrides,
multiple connections per shard, source-port ranges, NAT fallback, excess
connections, keepalive, orphan budgets, and reconnect scheduling.

### Metadata, control, and events

Use typed internal queries for topology, schema, client routes, tracing, and
agreement. Publish validated snapshots atomically. Reconcile actual host
identity for bootstrap and discovery-disabled connections. Incomplete reads must
not silently replace complete topology. Newer drops beat stale refreshes;
recoverable failures retain old keyspace data and surface a partial-refresh
error.

Schema agreement must include the coordinator that applied DDL, avoiding a false
agreement on an old schema version. Event processing and subscriber delivery
must not block the control connection. Overflow delivers a guaranteed marker and
terminates that subscription; resubscribe before reading a snapshot, then
discard publication events at or below its generation. Events follow snapshot
publication.

### Prepared metadata and routing

Cache by effective keyspace/CQL and validated schema identity. Track
host-specific prepared IDs and response metadata revisions. Attempts capture an
exact local entry/revision and schema epoch; failed CAS publications are
discarded, never retried against a fresh revision. Opaque server IDs are
equality tokens and do not establish global schema chronology. Late responses
cannot overwrite newer local cache generations. A page may use its retained
matching metadata even while another execution updates the shared cache.
Incompatible bind schemas fail before resend; retries/reprepare never invoke
application encoding again.

Tablet v1/v2, version hints, unknown ranges, moves, shard replicas, and
leader-aware strong-consistency routing require explicit implementation and
tests. Until the leader path exists, reject its configuration; that rejection is
not completion of the feature.

## 7. Implementation sequence and gates

| Milestone | Work | Exit criterion |
| --- | --- | --- |
| M0: design reviewed in this session | Completed ten sequential review-and-fix cycles, reconciled detailed API/implementation specs, and recorded API-semantic changes and package boundaries. Not a future implementation-agent task. | 33 findings fixed; no unresolved findings from those cycles. Reports and document hashes are retained in the individual design review reports. |
| M1: stabilize foundations | Finish codec, wire, compression, hashing, protocol and ownership tests; enforce size/depth/count limits. | Goldens, cross-span cases, fuzz/race tests and copy-identity checks pass. |
| M2: close runtime gaps | Fix metadata-generation races, TLS shard dialing, pool overrides, config behavior, discovery-disabled identities, and control/client-route typed queries. | Every exposed option works or is explicitly rejected; required features have no silent omissions. |
| M3: finish API/test migration | Complete all 174 test mappings; migrate examples, helpers, cloud, tracing, topology and client routes. Remove the old public API and unused legacy execution. | Export/call-site audit and all package/build-tag compilations pass. |
| M4: extract internals | Move shared types, codecs, protocol, transport, pool, metadata, routing and execution in dependency order. Preserve useful test access through testkit. | No import cycles; unit and integration behavior unchanged by extraction. |
| M5: verify complete matrix | Run full suites on one pinned final tree, including auth/socket/cloud/failure fixtures and race lanes. | Every baseline test has a real pass in a successful applicable lane; skips remain explicit. |
| M6: performance and final review | Measure encoding, decoding, segmented containers, routing, transport and end-to-end paths; review cancellation and recovery independently. | Copy/allocation contracts met and requirement-by-requirement completion audit passes. |

### Current gaps to address first

- [Streaming receive](feature-specs/gocql-v2-streaming-receive.md) has settled
  API, budgets and task assignments. Implement its incremental primitives/codecs,
  exclusive receive and RunStream in agents 01/02/04/07; complete all memory,
  ownership, direct-read, live-server and performance acceptance checks. Buffered
  NextInto does not complete this feature.
- `TestPagingWithBind` failed in a parallel race run with “missing matching
  metadata”; investigate concurrent schema generation versus retained page metadata.
- Scylla `ConnectionsPerHost` override is not correctly applied by the picker.
- TLS host-specific dialing bypassed shard-aware dialing. A fix landed immediately
  before the checkpoint and is unverified.
- Multiple connections per shard, source-port ranges, and configured keepalive/
  orphan bounds need complete wiring and validation.
- Leader-aware routing/tablet-v2 leader metadata is not implemented.
- Ring/control topology and client-route queries still contain legacy execution.
- Multiple discovery-disabled contact points can retain colliding empty host IDs.
- Cloud TLS tests exposed a race in the test logger; repair the fixture and rerun.
- Client-route integration migration remains unfinished; schema/routing and
  session/batch groups still need live verification.
- Cassandra schema recreation needs an equivalent Java aggregate fixture rather
  than assuming Scylla's Lua fixture works there.
- Old public API removal, final package extraction, documentation/examples, and
  a complete same-tree test matrix remain unfinished.

## 8. Verification plan

Track each original test by identity in
[`integration-baseline.json`](feature-specs/integration-baseline.json), with its
translated assertions, fixture requirements, command, and outcome. A passing
earlier baseline or a compile-only run does not prove v2 correctness.

| Lane | Required coverage |
| --- | --- |
| Stable Scylla v4 | Routing/LWT, tablets, shards, pools, metadata/events, normal statements and paging. |
| Scylla development v4 | SELECT without FROM and newer server capabilities. |
| Cassandra v4 and v5 | Protocol-specific options, segmentation, large responses, metadata IDs, server errors, compression. |
| TLS | Client certificates, trusted identity, ticket resumption, shard ports, cloud SNI, per-host dial hooks. |
| Authentication | A server enforcing authentication; successful and rejected credentials. |
| Maintenance socket/CCM | Unix transport and actual node stop/start lifecycle. |
| Injected server fixtures | Custom payload echo, write failures, incomplete-peer responses without mutating system tables. |
| Race/fuzz | Leases, close/wait, speculative losers, prefetch, refresh/drop races, malformed framing and type metadata. |

Stable and current development Scylla fixtures reject protocol v5. Use Cassandra
for v5; do not count that rejection as a successful v5 execution.

Representative commands, to run after implementation resumes:

```sh
python3 feature-specs/verify.py
go test -race -tags unit -timeout=5m ./...
go test -c -tags 'integration gocql_debug' -o /tmp/gocql-v2-integration.test .
go test -json -race -tags 'integration gocql_debug' -timeout=20m . -args \
  -distribution=cassandra -proto=5 -compressor=lz4 -runssl \
  -runcassandrafixtures -gocql.cversion=5.0.9 \
  -cluster=127.0.21.1,127.0.21.2,127.0.21.3 -clusterSize=3 -rf=3
```

Fixture provisioning, clean per-run keyspaces, and exclusive lane ownership must
be scripted. Never drop a keyspace while another test process uses it. Native
CCM startup/test/shutdown must share the same live exec process in this
environment.

Each implementation agent runs the focused performance checks for its seam
before handoff, starting with wire/codec foundations; performance is not deferred
until M6/agent 15. Agent 15 composes and compares those checks end to end, and
agent 17 reruns affected performance checks after final fixes on the pinned
release tree. A newly violated cost bound blocks the responsible handoff.

Performance checks must distinguish setup costs from per-row/per-element work,
descriptor allocation from payload copying, and explicit owned conversion from
borrowed execution. Include large fragmented values and all compression/TLS
modes; a small contiguous scalar benchmark cannot substantiate an all-hot-path
claim. The detailed performance gate requires independent scaling of cells,
fragments, hosts, page-history length and concurrency, with CPU, allocation,
contention, write/TLS amplification and retained-memory evidence as applicable.

## 9. Checkpoint: built versus verified

| Area | Current evidence / remaining status |
| --- | --- |
| Rust reference specifications | Six sections; 60 separate-agent review/fix reports; strict verifier passes for 144 source modules and 418 pinned links. |
| Original baseline | Every one of the 174 original top-level tests has an observed pass across the old-driver/corrected-harness baseline lanes. This is not v2 completion. |
| Typed foundations | Wire/codec/compression/hash components implemented; focused race, golden, boundary and allocation tests passed. |
| Caller-supplied result destination | Planned: RowPlan.DecodeInto plus Rows/Page.NextInto; direct writes, segmented backing identity, reuse and lifetime tests, and allocation benchmarks remain pending. |
| Streaming receive into final caller storage | Public API, bounded budgets and sequential assignments settled during execution preparation; implementation, exclusive read ownership and all bounded-memory/direct-read/live/performance evidence remain pending. |
| Typed runtime smoke | Real v4 raw/Snappy/LZ4/TLS and Cassandra v5 LZ4/TLS execution passed, including large blobs and paging/leases. |
| Main 74 tests | Rewritten; stable Scylla run: 66 pass, 8 protocol/distribution skips. Parallel race failure remains; Cassandra v5 coverage pending. |
| Serialization 33 tests | Rewritten; all 33 passed on the development Scylla fixture, including binary fixtures and nested/vector cases. |
| Control 18 tests | Rewritten; observed real-fixture race passes across Cassandra, auth and Unix lanes. Final concurrent whole-suite validation still required. |
| SELECT without FROM 16 tests | Rewritten; all 16 passed, with 39 pass events including subtests, on development Scylla. |
| Network 6 tests | Rewritten; plaintext DNS/shard race cases passed. TLS/cloud run failed; fixes pending/unverified. |
| Session/batch/tracing 16 tests | Rewritten; normal/race binaries compile. Database verification pending. |
| Other schema/routing 10 tests | Most rewritten and compile; client-route test remains unmigrated. Database verification pending. |
| CCM 1 test | Baseline lifecycle pass recorded; include it in the final matrix. |
| Complete v2 release | Not established: remaining runtime gaps, legacy paths, API removal, module extraction and full verification are mandatory. |

The latest metadata worker's final focused test handle could not be recovered
after the checkpoint. Its preceding focused suites passed, but the last schema
depth/function-overload changes remain unverified until rerun.

## 10. Supporting documents

- [Reference feature index](feature-specs/README.md)
- [Sixty-round review ledger](feature-specs/reviews/INDEX.md)
- [Detailed proposed public API](feature-specs/gocql-v2-api.md)
- [Detailed implementation contracts](feature-specs/gocql-v2-implementation.md)
- [Streaming receive contract](feature-specs/gocql-v2-streaming-receive.md)
- [Hot-path design review cycles](feature-specs/reviews/go-v2-hot-path/INDEX.md)
- [Current hot-path audit](feature-specs/evidence/go-hot-path-audit.md)
- [Transport audit](feature-specs/evidence/go-transport-audit.md)
- [All-test API dependency and migration audit](feature-specs/evidence/go-integration-api-audit.md)
- [Working history and evidence locations](feature-specs/WORK-LOG.md)

This plan consolidates the intended end state and remaining work. Existing code
and test results are checkpoints, not a reason to reduce the target or weaken
its acceptance criteria.

## 11. Agent task split and execution order

The user authorized preparation and sequential implementation on 2026-10-29.
The complete tracked/untracked workspace is preserved in checkpoint `661f169`
on `v2-execution-checkpoint`, with ignored/generated files additionally retained
in the preparation archive recorded in the execution ledger. Design
reconciliation previously completed on `v2-design-reconciliation` through ten
review-and-fix cycles; the [individual review
reports](feature-specs/reviews/go-v2-design/) retain those historical results.
The streaming signatures, budgets and task assignments settled during execution
preparation are new design additions and require their own implementation and
independent verification; earlier review results do not cover these additions.

Use **17 separate implementation agents, executed sequentially in the order
below**. Each agent owns its assigned task and works on its own `v2-...` branch,
starting from the accepted result of the preceding agent. Sequential execution
avoids overlapping edits to the current root-package implementation.

Before execution, preserve the complete current workspace checkpoint, including
uncommitted changes and untracked files, as the common starting point. Do not
start from HEAD alone and lose the existing implementation scaffolding or
evidence. Each subsequent branch must include the accepted predecessor's work.

| Order | Branch | Agent task and completion gate |
| --- | --- | --- |
| 1 | `v2-01-wire-protocol` | Stabilize wire spans, leases, compression, hashing, segmentation, and protocol validation. Verify limits, malformed input, fragmented frames, copy identity, cancellation, and release behavior. Also supply checked incremental envelope/cell/child framing and bounded v5 integrity-unit primitives for streaming; cover split reads and total-work limits without complete-response accumulation. |
| 2 | `v2-02-codecs` | Complete scalar, collection, tuple/UDT, vector, dynamic-value, and row codecs, including required RowPlan.DecodeInto for direct caller-destination writes. Verify type checking, null/unset/empty distinctions, independent cloning, segmented decoding, owned/borrowed encoding, destination reuse and focused allocation/backing-identity checks. Implement the separate StreamRowDecoder/StreamDecoder plans and scalar/text/blob/numeric/null/empty/nested/vector/tuple/UDT/encoded-output builders; verify seeded capacity/backing reuse, direct writes and explicit custom incremental codecs. |
| 3 | `v2-03-prepared-paging` | Fix prepared-cache generations and the paging "missing matching metadata" failure. Add Rows/Page.NextInto with mixed iteration, destination clearing, errors and page-retention semantics. Cover host-specific IDs, retained page metadata, reprepare, retries without re-encoding, and continuation-state handling. Share exact metadata/schema revision fences and hash-indexed continuation/history primitives with the future streaming pager; preserve buffered and streaming plan isolation. |
| 4 | `v2-04-transport-pools` | Verify TLS shard dialing; fix connection overrides, multiple connections per shard, source ports, NAT fallback, keepalive, orphan bounds, reconnects, and pool states. Implement exclusive pool-budgeted streaming reservation with acknowledged shared-reader/writer handoff, raw final-range reads, bounded v5 raw/LZ4 units, events/control, cancellation and safe discard/reuse. No whole-response gather or unsupported mandatory mode. |
| 5 | `v2-05-metadata-control` | Replace legacy topology/control/client-route queries with typed queries. Fix discovery-disabled identities, refresh/drop races, partial refreshes, schema agreement, event overflow, and subscriber isolation. Include bounded nonblocking streaming control/event dispatch and metadata generation fences. |
| 6 | `v2-06-tablet-routing` | Complete tablet v1/v2 and leader-aware routing, including hints, unknown ranges, moves, shard replicas, locality, and deterministic LWT routing. Required routing features must actually work. Streaming reservations must preserve strict host/shard/leader targeting within configured capacity. |
| 7 | `v2-07-execution-config` | Audit configuration and execution together: snapshots, explicit overrides, profiles, retries, speculation, batch admission, cancellation, close/wait, and observers. Every option must work or fail explicitly; LWT speculation is prohibited. Explicit rejection does not satisfy implementation of a required feature. Complete RunStream admission, capability/budget checks, independent streaming metadata plans, on-demand paging, progress info, pre-output safe retries and no replay after output; disable streaming prefetch/speculation and prove destination access stops before Close/cancel returns. |
| 8 | `v2-08-fixture-harness` | Repair the cloud test logger; implement Cassandra Java aggregate and injected-server fixtures. Script provisioning, unique keyspaces, exclusive lane ownership, and CCM startup/test/shutdown in one live process. Add deterministic first-row gates, split/corrupt/interrupted stream fixtures and raw/TLS/v5 streaming setup. |
| 9 | `v2-09-integration-migration` | Finish client-route migration and reconcile all 174 original tests with their translated assertions and fixture requirements. Verify pending session/batch/tracing, schema/routing, network/cloud, and protocol-specific coverage. Add real streaming large/nested/empty/paged/metadata-change/early-close coverage in raw v4, TLS and Cassandra v5 raw/LZ4 while preserving all baseline coverage. |
| 10 | `v2-10-remove-legacy` | Migrate remaining callers, helpers, examples, cloud, tracing, and supporting packages. Remove the old public API and unused legacy execution/serialization paths. Pass export, call-site, and build-tag audits. Audit streaming paths for forbidden buffered-decoder adapters, reflection and gathers. |
| 11 | `v2-11-extract-foundations` | Extract shared types, wire/compression primitives, codecs, model contracts, and protocol into the target internal packages. Preserve behavior and enforce dependency rules. Extract incremental readers and streaming plans/model contracts without full-cell/row/page storage. |
| 12 | `v2-12-extract-runtime` | Extract transport, pools, metadata, routing, cluster orchestration, and execution in dependency order. Introduce focused testkit hooks; eliminate import cycles. Preserve exclusive streaming reservation/read ownership, bounded transforms and paging execution across extraction. |
| 13 | `v2-13-public-module-docs` | Finalize the public facade and scyllacloud, consolidate the benchmark module, and update documentation/examples. Verify module paths and ensure facades introduce no second serializer or payload conversion. Publish working RunStream examples including seeded nested storage, Reset/capacity rules, supported modes and pool/cancellation behavior. |
| 14 | `v2-14-full-matrix` | Run the complete verification matrix on one pinned tree: Scylla stable/development, Cassandra v4/v5, compression, TLS/cloud, authentication, Unix/CCM, injected failures, race, and fuzz coverage. Record actual outcomes per baseline test. Include every streaming split/race/fuzz, first-row gate, bounded-memory, integrity, required live-mode and interruption acceptance case. |
| 15 | `v2-15-performance` | Measure codecs, fragmented containers, routing, transport, and end-to-end execution across compression/TLS modes. Verify backing-storage identity and copy boundaries as well as allocations; fix violations and rerun affected checks. Compare equivalent Run/Row, buffered NextInto and RunStream; record direct-read identity, live working memory, copy categories, first-row latency, read/syscall counts, CPU/throughput and independent page/row/cell/segment/slow-consumer scaling. |
| 16 | `v2-16-independent-review` | Independently review ownership, cancellation, recovery, metadata generations, dependency boundaries, and requirement completeness. Resolve findings and verify fixes. Independently review streaming no-gather/budget/integrity/no-replay and reservation/destination lifetime invariants. |
| 17 | `v2-17-release-verification` | Rerun the required matrix on the final tree after performance/review fixes. Complete the requirement audit and 174-test evidence ledger, with explicit skips and outstanding limitations. Prove every streaming acceptance criterion again on the final tree alongside all baseline evidence. |

### Ownership and handoffs

Each agent owns implementation, relevant tests, and evidence for its assigned
scope. Agents must preserve prior accepted work and accommodate existing changes
rather than reverting them. Package extraction is deferred to agents 11 and 12;
earlier agents stabilize behavior in the current layout.

Every handoff must include:

- The branch and committed result, with a concise account of changes.
- Checks run, exact commands, outcomes, and retained evidence locations.
- The completion-gate assessment and any unresolved findings or limitations.

The streaming ownership table in the feature contract is part of these gates.
An unmet completion gate blocks the next agent. Fix the current task before
advancing. If a later agent exposes a regression in earlier work, resolve it and
rerun affected checks before handing off. Final release verification must cover
the resulting final tree; earlier passes do not establish final-tree
correctness.

Explicit skips and limitations must remain visible in the evidence. They do not
replace the requirement that every original baseline test have a real pass in a
successful applicable lane, or reduce any other acceptance criterion in this
plan.

## 12. Design reconciliation results

Ten sequential review-and-fix cycles were completed on
`v2-design-reconciliation`, using a fresh read-only reviewer each round and a
separate design-fix owner. The [individual
reports](feature-specs/reviews/go-v2-design/) retain reviewer identities,
findings/dispositions and before/after document hashes.
There were 33 verified findings; all were fixed. No unresolved design blockers
remain from these cycles. Implementation and live verification remain pending.

The reconciled contracts now specify:

- Checked text constructors, logical Empty wire mapping, expected UDT identity,
  directional evolution, and codec-selected tuple-empty behavior.
- Borrowed options payloads drained by binding Close/Wait, recursively detached
  Collect results, and current-row/page ownership independent of session shutdown.
- Immutable routing compatibility, exact local metadata revision/epoch fences,
  opaque server IDs, and bounded complete paging-state history.
- Validated pool/stream/orphan controls, queued cancellation identity, context-aware
  raw/TLS hooks, route/TLS target generations, and explicit timeout defaults.
- Conservative statement classification for LWT safety, preserved strict targets,
  concrete tablet/leader rules, and bounded feedback overload recovery.
- Typed internal query dependencies, schema/route tombstones, publication/event
  ordering, guaranteed overflow resynchronization and coordinator-inclusive DDL
  agreement without replaying a successful statement.

The required 174-test behavioral migration, mandatory fixture matrix,
scope without backward compatibility and zero-copy acceptance criteria are
unchanged. Agent 09 must map these recorded API-semantic choices to affected
original assertions. The future execution sequence now contains 17
implementation agents, beginning with wire/protocol foundations; design
reconciliation is not a queued task.
