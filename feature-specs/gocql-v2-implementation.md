# gocql v2 implementation and acceptance contract

This document binds the [public API](gocql-v2-api.md) to concrete implementation
seams, worker ownership, feature disposition and verification. The reviewed Rust
specifications describe their pinned source; this document specifies Go choices.
No item below is an assertion that implementation or testing is already
complete.

## Shared internal representation

`internal/wire` is independent of package gocql. It is the only shared
byte-chain primitive imported by codec, routing and transport workers. Its
private fields cannot expose mutable arena storage after publication. The public
gocql Bytes/Text wrappers provide lifetime checks; internal wire access assumes
an owner is held.

```go
package wire

type Span struct { /* private read-only byte or string range */ }
func Borrow([]byte) Span
func BorrowString(string) Span
func (s Span) Len() int
func (s Span) Slice(offset, length int) (Span, error)
func (s Span) Bytes() []byte // INTERNAL read-only view, never mutate or retain unowned

type Chain struct { /* immutable descriptor storage, checked total */ }
func NewChain(...Span) (Chain, error)
func (c Chain) Len() int64
func (c Chain) Each(func(Span) error) error
func (c Chain) Slice(offset, length int64) (Chain, error)
func (c Chain) CopyTo([]byte) (int, error)

type Lease struct { /* stable refcount and finalizer; never reset/reused */ }
func NewLease(Chain, func()) *Lease
func (l *Lease) Retain() bool
func (l *Lease) Release()
func (l *Lease) Chain() Chain
func (l *Lease) Alive() bool // observation only; caller still holds a reference

type Builder struct { /* non-growing slab arena, mutable descriptor list */ }
type Mark struct { /* builder identity, generation, logical byte offset */ }
func NewBuilder(slabSize int) *Builder
func (b *Builder) Borrow(Span) error
func (b *Builder) Uint8(uint8) error
func (b *Builder) Uint16(uint16) error
func (b *Builder) Uint32(uint32) error
func (b *Builder) Uint64(uint64) error
func (b *Builder) Mark() Mark
func (b *Builder) Rollback(Mark)
func (b *Builder) PatchUint32(Mark, uint32) error
func (b *Builder) Len() int64
func (b *Builder) Freeze() (Chain, error)

type Cursor struct { /* chain, span index, intra-span index, logical offset */ }
func NewCursor(Chain) *Cursor
func (c *Cursor) Remaining() int64
func (c *Cursor) Uint8() (uint8, error)
func (c *Cursor) Uint16() (uint16, error)
func (c *Cursor) Uint32() (uint32, error)
func (c *Cursor) Uint64() (uint64, error)
func (c *Cursor) Take(length int64) (Chain, error)
func (c *Cursor) Skip(length int64) error
func (c *Cursor) RequireEnd() error
```

NewChain copies descriptors, not payloads. Chains and their input descriptor
slices cannot mutate each other; chains may share immutable descriptor backing
with each other. Single-span ranges should stay inline to avoid an allocation.
Slice may allocate descriptors for a multi-span range, but never gathers
payload. All lengths/offset additions are checked before conversion to int.
CopyTo is an explicit materialization API; a short destination returns
io.ErrShortBuffer. Cursor failed reads leave position unchanged and return
io.ErrUnexpectedEOF for truncation. Scalar reads fold bytes into the scalar,
without a temporary cell buffer. RequireEnd rejects trailing bytes. Empty ranges
are valid; negative lengths/offsets are errors.

Builder only patches its own unpublished slab bytes. A Mark identifies the byte
offset before a placeholder Uint32. Patching through borrowed spans, stale
marks, or foreign marks fails. Invalid Rollback is a safe no-op; valid rollback
retracts both descriptors and owned bytes. Freeze transfers slab and descriptor
ownership, invalidates old marks, and starts fresh slabs on later writes. Slab
extension must never copy previously emitted payload into a larger slab. New
small scalar/header fields may share a slab while contiguous. Unsafe
string-to-byte views are confined to wire.BorrowString and are read-only for
their entire lifetime.

A Lease begins with one reference; Retain fails at zero and can never resurrect
it. Release calls the finalizer exactly once at zero. The Lease object itself is
not pooled. Chain() is valid only while a reference is held. Root Bytes/Text
carry the lease identity for error detection. Page leases aggregate segment
leases by retaining/releasing them in their finalizer; segment storage may
return to pools only at final release. A page does not reference outbound
binding storage.

## Current root-package stabilization seams

The codec-foundation responsibility covers `v2_type.go`, `v2_value_io.go`,
`v2_view.go`, `v2_value_api.go`, `v2_scalar.go` plus focused tests.
Composition/container owner creates `v2_rows_codec.go`, `v2_collections.go`,
`v2_dynamic.go` plus focused tests. These same-package seams let workers compile
independently without importing the legacy marshaler:

```go
// v2_value_io.go; codec owner owns these implementations.
func newValueWriter(builder *wire.Builder, typ DataType, allowUnset bool) *ValueWriter
func finishValueWriter(writer *ValueWriter) error
func abortValueWriter(writer *ValueWriter)
func newValueReader(typ DataType, state CellState, body wire.Chain, owner *wire.Lease) ValueReader
func encodeValue[T any](builder *wire.Builder, typ DataType, codec Encoder[T], value T, allowUnset bool) error

// v2_rows_codec.go; composition owner owns these implementations.
func newParamWriter(builder *wire.Builder, columns Columns, allowUnset bool) *ParamWriter
func finishParamWriter(writer *ParamWriter) error
func newRowReader(columns Columns, body wire.Chain, owner *wire.Lease) *RowReader
func finishRowReader(reader *RowReader) error

type encodedCell struct {
    typ DataType
    state CellState
    body wire.Chain
}
type encodedValues struct {
    cells []encodedCell   // immutable descriptor slice in server order
    framed wire.Chain     // signed-length prefixes + identical body spans
}
```

newValueWriter owns a signed 32-bit framing placeholder; finishValueWriter
patches it with body length, null or unset after range/state validation. Logical
Empty is validated for the type and emitted as length zero, never a negative
sentinel; raw decode readers normalize it to Present zero bytes for
codec-selected meaning. `encodeValue` handles rollback and is the sole top-level
framing implementation; scalar Encoder.Encode writes the body.
`ValueWriter.Child` uses the same transaction with unset forbidden. Owned binds
set ValueWriter owned-input mode before encoding; BorrowBytes and BorrowString
copy caller-backed input once into owned storage, Child propagates the mode, and
generated scalar bytes stay unchanged. A whole-chain clone after encoding is
forbidden. abortValueWriter rolls back when the codec returns an error; no
emitted frame survives failure. `newRowReader` is scoped to one row's framed
cell chain. Decoding counts physical cells, permits positional cursor access,
and exposes immutable indexed descriptors internally so compiled name plans can
access out-of-order columns without copying cell bytes. finishRowReader
validates expected count and trailing bytes. RowReader.allowMissing is false for
ordinary result rows. Tuple/UDT decoding may explicitly enable it where trailing
omitted members are permitted: only an exact end of the encoded body synthesizes
a null for each remaining metadata field. An incomplete length prefix or body is
still a framing error, and a nonnullable field decoder still rejects the
synthesized null. Pair/composed child readers propagate allowMissing; normal
Rows never opt into it. Tuple encoding writes all declared field frames,
including null suffixes. Ordinary empty-body tuple decode may synthesize
nullable suffixes, while explicit EmptyCodec intercepts that body's zero length
as Empty; the distinction is chosen by codec, not encoded as a separate wire
state.

RowDecoder.Clone(R) R is a required schema-independent operation in addition to
RowPlan.Clone. It follows the logical Go shape and declared child codecs,
including nested containers and nullable fields, so tuple/UDT decoders can clone
results of standalone DecodeValue without recovering a previous plan. Built-in
row, single, paired, mapped, conditional and dynamic decoders all implement it;
custom decoders must implement explicit detachment. Clone never depends on a
mutable first-plan cache, successful prior Compile/Decode, or server metadata
remembered by the decoder. A compiled plan may delegate Clone to its immutable
RowDecoder.

The runtime responsibility covers `v2_statement.go`, `v2_execute.go`,
`v2_result.go`, `v2_profiles.go`, `v2_batch.go`, and session/pool/control
integration. Connection implementation changes belong to the transport
responsibility. It owns admission/reference counters, per-host prepare IDs,
result metadata snapshots, retries, timers, speculation and result aggregation.
It invokes generic plans at bind/decode edges; the common runtime receives only
nongeneric encoded requests, selected metadata and a callback plan. Passing a
typed closure through an internal interface is allowed; converting fields to
`any` is not.

```go
type executionRequest struct {
    statement string
    prepared *preparedState // nil means plain QUERY
    values encodedValues
    owner *inputOwner
    routing RoutingInfo
    options resolvedOptions
}
type responsePage struct {
    columns Columns
    rows wire.Chain
    rowCount int32
    owner *wire.Lease
    state PageState
    result Result
}
func (s *Session) executePage(context.Context, *executionRequest, PageState) (*responsePage, error)
```

`preparedState`, `inputOwner`, `resolvedOptions` are owned by the runtime
worker. Bind-time adapters from old protocol TypeInfo to DataType are allowed
only for metadata and must recursively preserve all fields. Runtime query values
must never pass through old Marshal/Unmarshal/Scan or `[]any`. System/control,
tracing, schema and authentication-support queries must also use typed or tagged
plans. An internal legacy query path remaining after user APIs migrate would
violate the all-hot-path scope and is a completion blocker.

## Internal metadata query and publication boundary

The following nongeneric contract belongs in internal/model (shared descriptors,
encoded values, target kinds, options and page ownership). Names below are
internal seams, not extra public APIs:

```go
type InternalQuery struct {
    CQL string
    Values EncodedValues
    Target QueryTarget // Control, DirectHost(host ID), or AnyEligibleHost
    Options InternalQueryOptions // consistency, timeout, keyspace, metadata flag
    State PageState
}
type InternalQuerier interface {
    Query(context.Context, InternalQuery) (*ResponsePage, error)
}
```

Metadata compiles typed codec plans and decodes returned lower-level pages
through its injected InternalQuerier. Every returned page transfers one receive
owner; metadata closes it after decoding and releases admitted inputs after
writer drain. No metadata import of public gocql, internal/execution or
internal/cluster is allowed. Cluster supplies a transport/pool adapter
implementing this interface, with no user execution-profile dependency or legacy
serializer. Topology, schema and client routes target Control; agreement targets
each DirectHost; tracing uses AnyEligibleHost under its own context and explicit
consistency/deadline settings. These operations have no implicit user
retry/speculation/profile settings. Each subsystem owns its bounded
repair/retry/paging loop and cancellation; transport performs only one attempt
with typed lower-level protocol envelopes.

Each fetch records its start epoch. Schema changes/drops advance per-keyspace
and object epochs; route updates/deletions advance per-host/connection-ID pair
epochs. At publication, apply only results whose captured epoch still matches
the current scope; otherwise discard and schedule fresh work. A full read cannot
erase or replace a pair/object changed after that read started. Drops/deletions
publish tombstones independently of unrelated failed reads; recoverable errors
may retain only the latest non-dropped schema. Recreate assigns a new
identity/epoch. Keep tombstones until all older reads/feedback are fenced out.
Incomplete topology cannot replace a complete snapshot. Explicit refresh returns
only after its accepted publication, or a structured partial/error result; it is
not merely a query-completion notification. Subscriber overflow and agreement
scope follow the API contract, including publication-before-event and mandatory
DDL coordinator.

## Sending, compression, receive ownership and limits

The transport responsibility covers `conn.go`, `frame.go`, `framer.go`,
`internal/segment/**` and `v2_transport*.go`. The sequential task owner in
plan.md section 11 owns edits for its scope; these seam labels do not create
simultaneous exclusive worker assignments. Runtime owns shared request/response
types in `v2_runtime_types.go`; transport consumes this exact per-attempt seam
(no retries or policy selection inside it):

```go
type attemptRequest struct {
    statement string
    preparedID, resultMetadataID []byte // immutable control metadata
    columns Columns                    // cached result metadata with generation
    metadataGeneration uint64
    values encodedValues
    owner *wire.Lease
    options resolvedOptions
    state PageState
    routing RoutingInfo
    target Target // host, shard and strictness preserved through selection
}
func (c *Conn) executeTyped(context.Context, *attemptRequest) (*responsePage, error)
```

A nil preparedID selects QUERY; a present ID selects EXECUTE. Transport
validates connection features and prepares late stream/header/options/tablet
envelopes. It returns server errors and raw rows/metadata with ownership; the
runtime owns reprepare/retry policy and installs valid metadata generations.
Bound runtime constructs encodedValues by retaining each encoded cell range and
freezing its complete framed chain after finishParamWriter succeeds. Root
composition worker may add a private finalization helper for that bookkeeping
without changing public contracts. The single writer interface becomes: Replace
the mandatory contiguous write interface:

```go
type writeRequest struct {
    body wire.Chain
    owner *wire.Lease
    done chan writeResult
}
type writeResult struct { written int64; err error }
// Success means the writer no longer references any input span.
func (w *contextWriter) writeChain(context.Context, wire.Chain, *wire.Lease) (int64, error)
```

Submission acquires its own lease before admission. Cancellation before
admission sends nothing. After admission the writer completes or fails under one
connection serialization lock; it cannot return the owner while a syscall still
reads it. Partial frame writes poison the connection. Coalescing tracks
cumulative request byte boundaries separately from span boundaries, reports
every accepted request once, and handles partial writes across requests
correctly. net.Buffers descriptor scratch is private because WriteTo mutates it;
keep the lock across all writev batches when the OS iovec limit is exceeded. TLS
writes the same ordered spans under this lock. No driver plaintext gather is
introduced for TLS.

Each accepted request has a stable connection-local identity distinct from its
wire stream ID; identity reuse while cancellation/response records can survive
is forbidden. Cancellation state persists through queue-to-stream assignment. If
a queued write has not begun, the writer may discard it and release its input;
once writing starts it must finish or fail before release. A cancelled
transmitted request reserves its stream until a matching late response or
connection death. A delayed cancellation for an already completed identity
cannot orphan a newer request using the same stream. Undelivered late pages are
released exactly once. MaxOrphanedStreams counts all currently abandoned
transmitted streams, without an age exemption; close/poison and replace the
connection immediately when count is greater than the effective budget.
Effective budget is min(configured budget, effective stream capacity - 1), so
smaller MaxRequestsPerConnection values cannot exhaust all streams with orphans
without replacement. Zero closes on the first orphan; negative configured values
are invalid. Never free outstanding stream IDs just to meet the budget. The
default budget is 1,024. Session.Close follows the coordinated cancellation, I/O
interruption and writer/worker drain contract in the API document.

v3/v4 frames consist of their small owned header/envelope plus bound body spans.
v5 handshake retains pre-negotiation framing. After negotiation, logical frames
are sliced at 131,071-byte segment boundaries without payload copies. Build
segment header CRC24 and incremental CRC32 using IEEE CRC seeded exactly once
with `FA 2D 55 CA`. Self-contained segments contain only complete frames; every
segment of a split frame, including its last, is non-self-contained. Streaming
Murmur3 and custom partitioner input uses the same byte order as transport.
Composite partition-key hashing feeds checked uint16 component length, original
component spans and zero terminator; no routing-key flattening or re-encoding.
Null/unset partition keys disable token routing with a diagnostic; invalid
indexes/types or oversized composite components fail binding.

Compression is an actual representation transform. `internal/wirecompress` owns:

```go
func AppendLZ4(dst []byte, source wire.Chain) ([]byte, error)
func AppendSnappy(dst []byte, source wire.Chain) ([]byte, error)
```

LZ4 emits a raw block; v4 transport prepends the four-byte big-endian raw
length, while v5 uses per-segment raw blocks and no such prefix. Snappy emits
its native raw block including input-length varint. Both read matches across
spans and write final compressed output without gathering source.
Always-literal/stored output is not sufficient: repetitive input must compress.
v5 chooses original source spans when transformed bytes are not smaller; it must
not copy raw input into a failed compression destination. Compression output is
attached directly to envelope spans, never copied back into a frame buffer.
Errors do not publish a partial block. Decompression enforces the
declared/result length and configured budget before allocation, and writes
directly into final leased output. Contiguous compressed input can use
established decoder libraries; fragmented input needs a span decoder or receive
ownership strategy that already stores the block intact, not a gather introduced
solely for that library.

v3/v4 uncompressed receives fill final leased storage. Remove bufio
body-prefetch copies or use transferred receive chunks. v5 receives retain
CRC-verified raw or decompressed segment leases. Multiple frames in a segment
share its lease; split frames accumulate range descriptors only. The frame
header/cell prefix/UTF-8 sequence/collection element may cross segments; checked
cursors handle all cases. Connection closure or parser-pool reuse does not
invalidate delivered leases. Malformed split continuation releases all partial
owners and poisons the affected connection. No `recvSplitFrame` reassembly
payload buffer remains on the v2 path.

Limits are validated before allocation/submission: 256 MiB frame/decompressed
frame default configurable limit, 131,071-byte v5 payload segments, signed-int32
cell length and collection count, uint16 string/ID/count limits, uint16 batch
and parameter counts, positive int32 page size, nonzero vector dimensions with
checked nested width, maximum configurable metadata nesting depth (default 64),
and checked row-count-times-column-count. Compression ratio is not itself a
validity rule; an explicit decompressed-size budget bounds memory. All length
readers reject negative values except exactly defined null/unset sentinels.
Unknown required type/opcode/flags or malformed options fail; documented
optional future flags can be ignored only with protocol evidence. Full expected
payload consumption is required. Automatic paging retains full continuation
equality history under MaxPagingStates (default 65,536) and MaxPagingStateBytes
(default 16 MiB); positive validated budgets bound control-token storage even
with request timeouts disabled. History exhaustion returns ErrPagingStateLimit,
while any repeated state (including an empty token or nonconsecutive cycle)
returns ErrPagingNoProgress. Manual page calls do not share history.

Zero-copy is a driver protocol/codec ownership contract, not a claim of zero
physical OS/TLS copies. Scalar wire encoding, compression/decompression,
explicit Clone/owned conversions, TLS record assembly/encryption/decryption and
the standard crypto/tls plaintext Read handoff are recorded separately. The
standard TLS API cannot lease its internal plaintext; use it normally and
account for that copy. Driver gathers, assembly copies, v5 reassembly copies and
accidental cell clones are still prohibited on every protocol/TLS/compression
lane.

## Config and peripheral value shapes

All slices/config objects are copied or validated as immutable descriptors at
Connect. Shared interfaces (policies/providers/logger) have explicit concurrency
contracts. TLS snapshots use tls.Config.Clone plus defensive copies of mutable
trust pools, certificate DER/OCSP/SCT slices and protocol/cipher lists. Parsed
certificate/key objects, callbacks and session caches remain explicitly shared
immutable or concurrency-safe provider state; callers must not mutate those
shared objects. Hook-returned TLS configs receive the same per-connection
snapshot treatment. DefaultConfig supplies defaults; Config's zero value is not
silently identical to defaults.

```go
type Config struct {
    ContactPoints []string
    Port uint16
    Protocol int                         // 0 auto (highest common <=4), or 3/4/5
    Keyspace string
    CQLVersion string
    ConnectTimeout, DNSTimeout time.Duration
    WriteTimeout, KeepaliveInterval, KeepaliveTimeout time.Duration
    ReadTimeout time.Duration                    // socket reads; independent of request timeout
    SocketKeepalive time.Duration
    NoDelay bool
    ReceiveBuffer, SendBuffer Option[int]
    Linger Option[time.Duration]
    LocalAddress netip.Addr
    Dialer Dialer
    DialHost func(context.Context, DialTarget) (net.Conn, error)
    DNSResolver DNSResolver
    TLS *tls.Config
    TLSForHost func(context.Context, HostSnapshot) (*tls.Config, error)
    Auth AuthProvider
    Compression Compression
    CoalesceWait time.Duration
    Pool PoolOptions
    Reconnect ReconnectPolicy
    Conviction ConvictionPolicy
    AddressTranslator AddressTranslator
    HostFilter func(HostSnapshot) bool
    ClientRoutes *ClientRoutesOptions
    Identity IdentityOptions
    ReportConfiguration bool
    Metadata MetadataOptions
    Limits ProtocolLimits
    PreparedCacheSize int
    DefaultProfile *ProfileHandle
    Partitioners map[string]TokenPartitioner
    TimestampGenerator func() int64
    Observer Observer
    LifecycleObserver LifecycleObserver
    Logger StdLogger
}
// Both hooks open raw connections; the driver performs TLS and CQL setup.
type DialTarget struct {
    Host HostSnapshot
    Network, Address string             // TCP endpoint or Unix socket path
    LocalAddress netip.AddrPort          // selected source; invalid means OS default
    ShardID Option[uint32]
}
type Dialer interface { DialContext(context.Context, DialTarget) (net.Conn, error) }
type Compression uint8
const (NoCompression Compression = iota; LZ4Compression; SnappyCompression)
type PoolOptions struct {
    ConnectionsPerHost, ConnectionsPerShard int
    MaxExcessPerShard int
    MaxRequestsPerConnection int
    MaxOrphanedStreams int
    ShardAwarePort bool
    SourcePortStart, SourcePortEnd uint16
}
type ReconnectPolicy interface { Delay(attempt int) (time.Duration, bool) }
type ProtocolLimits struct {
    MaxFrameBytes, MaxDecompressedBytes int64
    MaxMetadataDepth int
    MaxPagingStates int
    MaxPagingStateBytes int64
}
type MetadataOptions struct {
    Mode MetadataMode
    Keyspaces []string
    InitialDiscovery bool
    PeriodicRefresh time.Duration
    RequestTimeout time.Duration
    ServerTimeout Option[time.Duration]
    AgreementTimeout, AgreementPoll time.Duration
    AutomaticAgreement, RefreshAfterAgreement bool
    TopologyEvents, StatusEvents, SchemaEvents, ClientRouteEvents bool
}
type MetadataMode uint8
const (MetadataDisabled MetadataMode = iota; MetadataMinimal; MetadataFull)
type IdentityOptions struct { DriverName, DriverVersion, ApplicationName, ApplicationVersion, ClientID string }
type ClientRoutesOptions struct {
    ConnectionIDs []string
    HostnameOverrides map[string]string // per configured connection ID
    TLS bool
    SNIProxyAddress string
}
type AuthProvider interface { Start(context.Context, HostSnapshot, string) (AuthExchange, Nullable[[]byte], error) }
type AuthExchange interface {
    Challenge(context.Context, Nullable[Bytes]) (Nullable[[]byte], error)
    Success(context.Context, Nullable[Bytes]) error
    Close()
}
func PasswordAuth(username, password string) AuthProvider
```

DialHost takes precedence over Dialer; if neither is supplied, use the built-in
dialer. The driver resolves/translates the endpoint and selects any shard-aware
port and local source port before invoking the hook. Hooks must honor DialTarget
or return an explicit unsupported-setting error; silently discarding source-port
requirements is forbidden. DialTarget.Host is a defensive metadata snapshot;
changing a hook argument cannot mutate routing state. Hook errors preserve
context cancellation. The hook returns a raw net.Conn; the driver selects
TLSForHost (when present) over TLS, applies TLS once, then runs protocol
negotiation/authentication. A nil TLSForHost result inherits TLS. If TLS is
required by TLS, a non-nil hook, cloud or client routes and no usable config
remains, fail setup explicitly; never interpret nil as permission to downgrade.
A hook cannot turn off configured TLS. TLS identity uses the logical host/SNI
configuration, not a rewritten proxy address; a missing required logical name is
a validation error. A Unix target carries its path in Address and has no
shard/source-port selection.

Auth output owns or borrows bytes until its handshake write completes, then the
runtime releases them. Exchange.Close always runs, including aborted handshakes;
Success runs only after AUTH_SUCCESS. Metadata internal queries obey context
bounds and new codecs. Unix sockets use Dialer and protocol 3 explicitly. Cloud
configuration is a builder for Config/TLSForHost/dial hooks; it retains SNI
UUID, per-host routing and ticket-cache verification without old query
interfaces. Pool sizing validates nonnegative ConnectionsPerHost and
ConnectionsPerShard. DefaultConfig sets host count 0 and shard count 1. Positive
ConnectionsPerHost selects per-host mode even when the shard count remains
positive; otherwise a positive ConnectionsPerShard is required. Both zero is
invalid. Per-host mode targets that total count across shards and disables
targeted shard-port filling, but retains token/shard-aware selection among
available connections. Per-shard mode targets its count separately on each
discovered shard (one for nonsharded hosts). MaxExcessPerShard is nonnegative;
zero retains no excess connections. MaxRequestsPerConnection is in 1..32,768
(default 32,768), including all native nonnegative stream IDs.
SourcePortStart..SourcePortEnd is an inclusive nonempty range within
1,024..65,535 when shard-aware dialing is enabled; defaults are 49,152..65,535.
An exhausted or unrepresentable shard-specific port search fails explicitly and
uses the documented ordinary-port fallback; no helper panic is permitted.
Initializing/ready/broken/closed are distinct pool states; closed is terminal
and cannot schedule refills. Endpoint/reshard generations fence late opening
attempts so they cannot reinstall superseded endpoint/sharder state. The same
fence covers route, proxy, SNI and TLS-target generations; late successful
handshakes for old targets are closed rather than published. Reconnect uses
bounded exponential backoff with jitter, independently of startup timeout. Host
filtering applies before query pool acquisition; bootstrap/control contact
exceptions are documented explicitly.

All user hooks/providers/policies/logger/observers are invoked without driver
locks and must support the documented shared concurrency. Synchronous callbacks
(HostFilter, timestamp generator, policy methods, observer/logger/lifecycle
calls) must return promptly, perform no blocking I/O and must not synchronously
call blocking Session.Close, Bound.Wait or other operations that wait on their
own execution. Schedule such work separately. Context-taking
DNS/dial/translation, TLSForHost and auth hooks must honor cancellation. The
driver cannot preempt arbitrary caller code; its deadline/drain promises assume
these callback contracts. Observer/logger panics are contained and diagnosed
without corrupting execution.

ClientRoutes requires nonempty unique connection IDs and nonempty override
hostnames for configured IDs only. Use the first configured available route and
keep it sticky while valid; deletion selects the next configured available ID.
DNS/connect failure may try remaining configured route IDs for that same host
under the same deadline, without changing unrelated hosts. Missing required
routes/ports fail explicitly; there is no fallback to the advertised direct
address. Route reads/partial deletions obey per-pair generation fences.
Combining client routes with a user AddressTranslator or enabled shard-aware
source-port dialing is invalid; the cloud/client-route builder disables the
latter explicitly. TLS routes require usable verified TLS and logical SNI;
setting the route flag alone never supplies trust configuration. Config
snapshots copy route IDs and override maps.

Duration defaults and validation are normative; all negative durations are
invalid. Options with Set=false inherit, while Set=true applies the stated zero
semantics. Caller context/session cancellation always remains effective when a
local timeout is disabled.

| Setting | DefaultConfig / default profile | Zero meaning / validation |
| --- | --- | --- |
| ConnectTimeout | 5 s per connection setup | Disable local setup timeout; setup includes target DNS/translation, dial, TLS, OPTIONS/STARTUP/auth/REGISTER. Caller context is the outer bound. |
| DNSTimeout | 5 s per lookup | Disable local lookup timeout; never restart the enclosing setup deadline. |
| ReadTimeout, WriteTimeout | Disabled (0) | Disable socket deadline; per-request deadlines and connection shutdown remain independent. |
| SocketKeepalive | Disabled (0) | Disable TCP probes. Unsupported requested socket settings return an explicit error. |
| KeepaliveInterval, KeepaliveTimeout | 30 s each | Interval 0 disables CQL keepalive; timeout 0 disables its local timeout. Timeout covers queue/write/response and still observes session cancellation. |
| CoalesceWait | 0 | Flush when the queue drains; positive values bound the extra batching wait. |
| Profile Timeout / PageTimeout | 30 s / 0 | Disable that local timeout; per-page timeout may only shorten the whole-operation context. |
| Metadata PeriodicRefresh | 60 s | Disable periodic work; explicit refresh and enabled server events still operate. |
| Metadata RequestTimeout | 31 s per internal page | Disable the local page timeout, not caller/setup context. |
| Metadata ServerTimeout | Unset: automatic 30 s on supporting targets, omitted otherwise | Set=false selects negotiated automatic behavior; explicit Set=true zero omits the extension, and explicit positive unsupported overrides return a setting error. |
| AgreementTimeout / AgreementPoll | 60 s / 200 ms | Both must be positive; the caller context can shorten the configured deadline. |
| Linger when Set | Unset (OS default) | Explicit 0 requests reset-on-close; positive values use checked OS-supported precision/range. |

Peripheral observation types are value snapshots, owned by the runtime/metadata
owner and free of payload/interface{} fields:

```go
type HostSnapshot struct {
    ID UUID
    Address netip.AddrPort
    Datacenter, Rack string
    Tokens []int64
    Enabled, Connected, Up bool
    Shards uint32
}
type TokenRange struct { Start, End int64; Replicas []Target }
type EventKind uint8
const (TopologyEvent EventKind = iota; StatusEvent; SchemaEvent; ControlEvent; ClientRouteEvent; OverflowEvent)
type Event struct {
    Kind EventKind
    Generation uint64
    Change, Target, Keyspace, Object string
    ArgumentTypes []string
    HostID UUID
    Address netip.AddrPort
}
type EventFilter struct { Kinds []EventKind; Keyspace string; Capacity int }
type Subscription struct { /* private queue, shutdown ownership */ }
type MetricsSnapshot struct {
    Requests, Attempts, Errors, Retries, Speculations, Cancelled, Orphans uint64
    InFlight int64
    LatencyP50, LatencyP95, LatencyP99 time.Duration
}
type LifecycleObserver interface { ObserveLifecycle(LifecycleEvent) }
type LifecycleEvent struct {
    Kind, Stage string
    HostID UUID
    Stream int16
    At time.Time
    Err error
}
type TraceInfo struct {
    ID UUID
    Coordinator netip.Addr
    Started time.Time
    Duration time.Duration
    Events []TraceEvent
}
type TraceEvent struct { ID UUID; Activity string; Source netip.Addr; Elapsed time.Duration; Thread string }
```

## Feature disposition against reviewed references

Every language-independent named subsection is covered below. A retained
implementation is still subject to the typed/ownership/validation contracts; the
word retain is not a completion claim. No Rust-only build gate or accidental
source limitation is silently a Go feature requirement.

| Reference named features | Go disposition and implementation owner |
| --- | --- |
| Routing inputs/request lifetime; custom policy contract; default policy configuration and target ordering | Canonical encoded token, snapshot at admission, RoutingPolicy/QueryPlan; runtime/routing owner. Default local-DC/rack preference and failover are explicit policy options. Plan supports cheap first target internally without requiring caller APIs. |
| Partition keys/tokens/vnodes; direct replica/ring inspection | Streaming Murmur3 with signed-byte compatibility, CDC/custom partitioner support, checked composite lengths, replication-strategy placement and reusable immutable replica maps; MetadataSnapshot.Replicas/TokenRanges. Unknown tablet ranges never fall back to vnode placement. |
| LWT routing; tablet placement/feedback; leader-aware routing | Tri-state statement classification (unknown/ordinary/conditional), server conditional metadata and regular serial consistency gate speculation and preserves deterministic coordinator/leader ordering; tablet v1/v2 negotiated per connection, version hints/reset/move/unknown replicas and strong-consistency leader metadata retained. Accepted feedback is at-most-once and bounded/coalesced; saturation advances a placement invalidation epoch, surfaces a diagnostic and relearns unknown ranges without failing a successful reply. |
| Shard hints/explicit targeting; latency awareness/filtering | Optional host/shard target, all targeted fallback rules explicit; existing shard picker/pool reused after typed token seam; policy latency feedback, minimum sample/retry windows and exclusions stay configurable. |
| Connection defaults; establishment/negotiated properties; node-derived properties | v3/v4/v5 handshake, metadata IDs/tablet/rate-limit/LWT feature negotiation, compressor fallback, shard properties and host ID retained. No version is inferred from brand. Connect context bounds all setup phases. |
| Pool state/initialization; selection; filling/excess/backoff; advanced shard awareness/fallback | Existing shard-aware port, source-port mapping/NAT fallback and excess-connection mechanics retained with per-shard/host size controls; zero-connection pools fail promptly and repair; runtime owner integrates immutable snapshots. |
| Multiplexing/buffering/coalescing; cancellation/orphaned streams/shutdown | Span writer/receiver replaces copying interfaces, stream IDs released only on response/connection death, orphan budget triggers replacement, late results released exactly once; transport owner. |
| Keepalive/status hints; keyspace propagation/failure; connectivity notification | Separate CQL/TCP keepalive and conviction/reconnect behavior retained. Go intentionally forbids concurrent session USE; initial keyspace, qualified CQL and v5 per-request overrides cover routing without propagation races. Lifecycle events expose state without sending I/O from callbacks. |
| Metadata fetch scope; discovery/control establishment; content/validation | Disabled/minimal/full modes, keyspace filtering, system.local/peers/peers_v2 version-aware queries, incomplete peer tolerance with complete-snapshot protection; full existing Go schema/recreation retained, stricter duplicates/counts/type checking. |
| Public metadata inspection; events/scheduling; merge/publication | Immutable generation snapshots, explicit refresh, coalesced refresh events, drop beats stale refresh, retained old keyspace on recoverable errors with surfaced partial error; metadata owner. |
| Host listener contract; routes/tablet coordination; agreement; shutdown | Event subscription/overflow and LifecycleObserver replace language-specific listener interfaces, route/schema generations reconciled, bounded agreement and complete cancellation/release; retained snapshots remain usable after close. |
| Statement forms/paging; options/profiles; unprepared/automatic preparation | Explicit Statement/Prepare/Bind/Run modes, direct typed QUERY with declared metadata, no hidden prepare/serialization per attempt. Three-state PageState and frozen profile handles. No-FROM statements remain real QUERY/PREPARE without fabricated routing. |
| Explicit prepare/identity; binding; reprepare; result schema changes | Lazy host preparation instead of mandatory Rust all-host fan-out; per-host ID and bind-generation validation; actual result metadata type-check before every changed page including empty pages; cached IDs CAS-updated. |
| Batches/routing; prepared cache | Immutable bound members, all members encoded once, first complete key routing, bounded cache keyed by effective keyspace/CQL and generation; cache eviction never invalidates live handles. Batch validation precedes I/O. |
| Request/attempt/deadline/errors; default/downgrading retries; speculation | Context applies to complete logical work, safe idempotence gate, named opt-in downgrading records achieved level, no LWT speculation, attempt accounting includes cancellation/losers. |
| Automatic paging/ownership; results/timestamps/side effects | Borrowed page leases with Clone, bounded prefetch, terminal error, repeated-state guard, unique generated timestamp per execution, contextual agreement/refresh, preserve warnings/payload/traces. |
| Framing; compression/extensions; handshake/event codecs | Full v3/v4/v5 Go support beyond Rust v4; strict checked framing and limits, negotiated extensions, span block compression and raw fallback; no payload gather on v5. |
| Parameters/rows/batches; scalar conversions; null/unset/empty | Exact generic codecs, explicit states, strict widths/text/time/duration/UUID rules, no nullable-collection-to-empty coercion, clear Go changes from permissive Rust quirks. |
| Collections/vectors/dynamic/custom codecs; generated row/UDT codecs | Lazy typed views, tagged Value, explicit accessors/code-generation-compatible interfaces, strict checked framing and vector dimension, UDT evolution options. Rust macro attributes become explicit Go descriptors; no reflection-based derivation is required or retained. |
| Result metadata/type checking/ownership; formatting/helper boundaries; validation/errors | Generation-bound plans, stable leases, streaming views, explicit materialization, structured path/stage errors; no implicit debug formatter reads released payload. CQL identifier/literal formatting is explicit utility behavior, not raw-query interpolation. |
| Configuration/startup; transport/compression; auth; TLS/resumption | Immutable Config, typed lifecycle, bounded DNS/setup, existing dial/unix/cloud behavior, per-connection challenge exchange and cleanup, crypto/tls and certificate validation, real ticket-resumption assertion; no Rust TLS backend duplication. |
| Address translation/filtering; client routes | Translation resolves peer addresses without rewriting contact points unexpectedly; host filters and SNI-aware route changes are explicit; client-routes events update route snapshots and pools; route fallback is tested. |
| Identity/effective configuration; metrics/history/logs/server tracing | STARTUP identity, opt-out configuration report with secrets redacted, typed observer/metrics/lifecycle/Trace APIs; no original dynamic argument arrays in observations. Report configured versus effective negotiated values distinctly. |
| Configuration ownership; Cargo feature availability | No Go Cargo feature emulation. Native big.Int/netip/time adapters explicit; existing inf.Dec migration may use a custom codec. TLS/metrics available through Go configuration. Foreign-runtime adapters and Rust serde external formats are not applicable, not missing Go features. Experimental routing/client-routes/leader capabilities have explicit Config/policy controls and tests. |

Rust quirks deliberately not adopted include unchecked sizes, random replacement
tokens, null collection coercion, accepting all negative response lengths as
null, ignored scalar/aggregate trailing bytes, short vector padding, stale bind
metadata reuse, regression of newer metadata snapshots, successful incomplete
topology replacement, profile changes leaking into a pager, unbounded same-state
paging, and callback omission on cancelled attempts. Tests translate these to
the stated Go contract rather than claim Rust already guarantees the stricter
behavior.

## Work ownership and migration sequence

| Historical staging responsibility | Current same-package seam |
| --- | --- |
| Wire core | `internal/wire/**`; chain/builder/cursor/lease algorithms and focused tests. |
| Span compression | `internal/wirecompress/**`; real LZ4/Snappy span encoders and compatible decoding/property tests. |
| Codec foundation | `v2_type.go`, `v2_value_io.go`, `v2_view.go`, `v2_value_api.go`, corresponding tests; DataType adapters reviewed with runtime owner. |
| Scalar codec | `v2_scalar*.go`, numeric/time helper files and corresponding tests; consumes foundation types without editing their owners. |
| Root composition | `v2_rows_codec.go` and tests; generic parameter/row plans and typed field accessors. |
| Container/dynamic codec | `v2_collections.go`, `v2_dynamic.go` and related tests; uses scalar/composition seams without uncoordinated edits. |
| Runtime | `v2_statement.go`, `v2_execute.go`, `v2_result.go`, `v2_batch.go`, `v2_profiles.go`, configuration/metadata/policy integration; sole owner of session/config/policy edits; no conn/frame/framer edits. |
| Transport | `v2_transport.go`, `frame.go`, `framer.go`, `internal/segment/**`; conn.go transport integration responsibility; runtime uses executeTyped. |
| Root/integration migration | Module/import paths; test-group assignment, old public API removal, run ledger/verifier and database runs. |

This table records same-package staging responsibilities, not a parallel worker
schedule or final package layout. The 17 sequential agents, branches, task gates
and handoffs in plan.md section 11 are authoritative. Agent 01 owns
wire/protocol, 02 codecs/composition, 03 prepared/paging, 04 transport/pools, 05
metadata/control, 06 tablet/routing, 07 execution/config, 08 fixtures, 09 test
migration and 10 legacy removal; 11 and 12 extract foundation/runtime internals,
13 finalizes module/docs, and 14–17 verify matrix/performance/review/release.
Each active task owns necessary changes in its scope and must preserve the
accepted predecessor's work. Shared signature changes update both artifacts and
notify affected owners before implementation. Intermediate compilation may
contain old private runtime implementations; release completion requires
removing the public compatibility surface and all old hot-path calls.

Preserve all 174 baseline test identities in
[integration-baseline.json](integration-baseline.json) and record each original
test's translated behavior, source location, required fixture, command and
actual outcome. Do not inflate the count with new tests or replace old
assertions with basic CRUD. Helpers and benchmarks in all 29 files must compile
too. A fixture/version skip is recorded as a skip, never a pass. Run every
feasible required lane and report any genuinely unavailable feature. The audit
appendix is the per-test dependency map; use its eight disjoint groups for
ownership and its fourteen private-access families for harness integration.

Migration translations are mandatory:

- Old reflection/Scan tests become typed or explicit Value tests preserving all
  scalar bytes, signedness, float-bit, null/empty/unset, nested/container/vector
  and custom-codec assertions. Heterogeneous test tables use typed closures or
  tagged Value cases, not `any` converted by a runtime switch.
- Tuple destination flattening becomes explicit tuple row structs/accessors;
  dynamic member access still checks every asserted member. Preseeded map-pointer
  mechanics become explicit typed field updates, while value/null results remain.
- Mutable Query/Batch setters become Options snapshots, prepared handles and new
  binds. Tests still assert defaults, overrides, validation, reuse, timeouts,
  achieved retries and attempt metrics. Old panic strings become typed validation
  assertions, with exact intentional behavior changes recorded per test.
- Manual paging keeps every state/last-page/tombstone assertion; state remains
  readable after Close. Codec metadata rebind/cache tests still inject generations,
  UNPREPARED repair and metadata-ID changes, without re-encoding on the hot path.
- Observers retain attempts/latency/host/empty-result/page-row/batch-entry assertions
  using statement/schema metadata and canonical routing, not original `[][]any`.
- TLS resumption, SNI cloud counts, shard-aware fallback/NAT/excess refill, unix
  sockets, custom DNS/translation, direct connection affinity, controlled stale
  cache injections, negative streams, payload fixtures and auth remain real tests.
- Control/schema/tablet and CCM disruption assertions retain their actual effects;
  tests that must serialize due to shared host/schema mutations stay serialized.

## Acceptance gates

1. **API/build gate:** module/import paths are v2; example, root, test helper,
   integration-tag, CCM and nested module packages compile. Export audit finds no
   old Query/Iter/Marshal/Unmarshal/Scan/binding surface. No v2/control/tracing/schema
   hot path invokes reflect or materializes `[]any`. Old internal code is deleted
   or demonstrably unreachable and removed before final completion.
2. **Codec/protocol gate:** every scalar and aggregate type is covered by golden
   byte cases, null/empty/unset distinctions, metadata mismatch, nested custom
   codecs, every split point of lengths/scalars/UTF-8, overflow/truncation/trailing
   bytes, lazy terminal errors, strict UDT/vector schema changes, and rollback.
   Protocol fuzzing bounds allocation by configured limits and never panics.
3. **Ownership/race gate:** tests retain old pages while new ones reuse the pool;
   close rows with prefetched results; cancel before/after admission; complete a
   speculative winner while a loser write blocks; retry/reprepare; use a pager;
   close sessions with delivered leases. Custom encoder call count is one per
   Bind across all routing/retries/pages/batches. Bound.Wait must not return before
   final write or active pager reference; retained result leases must not delay it.
4. **Transport gate:** uncompressed v3/v4/v5 canonical span/backing identity tests,
   segmented self-contained and split frames, incremental CRC equivalence, large
   (>500 KB) payloads, OS iovec limit, partial/coalesced writes, shutdown completion,
   TLS no driver gather, real compressed cross-span blocks and raw fallback all
   pass. Independent decoders validate compressed output, including incompressible
   and mixed input. Counters distinguish unavoidable transform bytes from
   forbidden payload copies.
5. **Runtime feature gate:** profile snapshot races, strict metadata refresh and
   NO_METADATA caches, conditional results, retries/speculation/idempotence,
   agreement/deadline/routing/tablet/leader/client-route state, observer accounting
   and all listed Config features have code paths and focused tests. A feature
   named in the table cannot be marked complete merely because its old file still
   exists.
6. **Integration gate:** actual Go test JSON runs cover the exact 174-test baseline
   plus helpers on stable Scylla v4 uncompressed/Snappy/LZ4/TLS, development Scylla
   v4 for SELECT without FROM/new capabilities, Cassandra v4 and v5 including
   segmentation/metadata IDs/large results and uncompressed/LZ4/TLS, enforcing-auth,
   cloud/SNI/resumption, Unix maintenance sockets and CCM lifecycle lanes. Injected
   fixtures cover custom payload echo, write failures and incomplete peers. Race
   and fuzz lanes cover the ownership/framing/metadata hazards in plan section 8.
   Current Scylla fixtures reject v5; that rejection is an explicit skip, never
   successful v5 coverage or a substitute for Cassandra v5. Future supported
   Scylla v5 is additional coverage, not a replacement required lane. Script fixture
   ownership and distinct per-run keyspaces; CCM startup/test/shutdown share one
   live exec process. The manifest verifier checks baseline identities, translated
   subcase assertions, actual successful exits and skips. Every baseline identity
   needs a real pass in at least one successful applicable final-tree lane; all
   mandatory lanes must run successfully with explicit fixture/version skips.
7. **Performance gate:** save benchmark commands/environment/results and compare
   to the pinned pre-v2 baseline. Measure the cases below with allocation counters
   plus backing-identity/copy instrumentation. No passing benchmark excuses an
   observed forbidden copy; lower allocation alone is insufficient.

| Benchmark (steady state after plan compilation) | Required evidence |
| --- | --- |
| Scalar encode/decode, owned scalar arena | Zero per-scalar heap allocations; one serialization write; no standalone scalar payload buffers. |
| Borrowed text/blob 0 B, 32 B, 4 KiB, 500 KiB | Zero payload-copy bytes at bind, uncompressed frame/segment assembly and decode, independent of input size. Descriptor/slab allocations reported separately. |
| Typed row scalar/text/blob; fragmented cell | Zero per-cell heap allocations for contiguous ranges; fragmented descriptor allocations recorded/bounded, zero flattened payload bytes. |
| Lazy list/map/tuple/UDT/vector traversal | No per-element payload copies or reflection; scalar iteration no per-element allocations. Explicit Collect/Clone measured separately. |
| Dynamic Value row/collection access | No reflect/`any` destinations, no eager variable payload materialization; decoder schema plan reused. |
| Single/composite routing, Murmur3/CDC | Encoders invoked once at bind, streaming composite token produces baseline-equivalent bytes/hash, zero routing key payload allocation. |
| Bound request, batch, retry and reprepare | Same canonical payload backing across all attempts; envelope/descriptor cost reported; no secondary marshal. |
| v5 segment send/receive, large split frames | Zero assembly/reassembly payload-copy bytes; segment/header/CRC allocations separate; throughput measured at several span sizes. |
| LZ4/Snappy compressible/incompressible | No source gather/output-to-frame copy; transformed bytes/count and compression ratio recorded; useful ratio on repetitive payload. |
| TLS transport | No driver gather; standard-library TLS costs explicitly labeled and measured separately from protocol transform. |
| Paging/prefetch/retention | Zero cell clone on ordinary Next, page lease bounded by demand, retained old page uncorrupted; explicit Clone cost separate. |
| Connection selection/policy/snapshot | No payload work, bounded/zero steady selection allocations; plan feedback remains race-free. |

Run focused unit/race tests during module integration and the required full
suites once stable; do not repeatedly run broad suites without new changes or
unresolved failures. All completion reporting distinguishes implemented, tested,
skipped and blocked. The 60 reference-spec reviews prove reference quality only,
not Go v2 implementation or integration success.
