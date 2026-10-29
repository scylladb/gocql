# gocql v2 API contract

Status: implementation contract, distinct from the six reviewed Rust reference
specifications. Module: `github.com/scylladb/gocql/v2`; package: `gocql`. The
current checkpoint keeps implementation scaffolding in the root package. The
final layout and dependency boundaries in plan.md section 5 are normative:
extract internals after behavior stabilizes, and migrate integration tests
toward public behavior with focused testkit hooks. This does not retain the v1
public API. `Query`, `Iter`, mutable reflection-based `Batch`, `Marshal`,
`Unmarshal`, `Scan`, `MapScan`, `SliceMap`, `RowData`, `NamedValue`, and
`...any` binding are removed as public execution/serialization interfaces.
Correct existing networking, policies, metadata and error types may be retained;
typed calls must never enter those old serialization paths. Rename/remove old
internal implementations after migrating callers. No compatibility facade or
tests running through one satisfy completion.

## Type metadata and codec contracts

The signatures below are normative. Existing `Type` constants denote CQL kinds;
`DataType` is the immutable descriptor of all wire-available type structure. It
contains nested element/key and value types, ordered tuple/UDT fields, UDT
identity, vector dimension and frozen state. Construction and access copy
descriptor slices, never value payloads. An invalid/zero descriptor fails
validation. Native protocol metadata does not carry collection/UDT frozen
annotations: FrozenKnown is false for that source; WithFrozen records an
explicitly known annotation. Equal and codec compatibility compare nested wire
structure/identity/names and ignore frozen annotations. Schema validation may
separately compare annotations when both are known. Fingerprints include all
compared structure and names; equality never relies solely on a hash.

```go
type DataType struct { /* immutable private descriptor */ }
type ColumnSpec struct { Keyspace, Table, Name string; Type DataType }
type Columns struct { /* immutable private ordered column specs */ }
func ScalarType(kind Type) (DataType, error)
func ListType(element DataType) (DataType, error)
func SetType(element DataType) (DataType, error)
func MapType(key, value DataType) (DataType, error)
func TupleType(fields ...DataType) (DataType, error)
func UDTType(keyspace, name string, fields ...ColumnSpec) (DataType, error)
func VectorDataType(element DataType, dimension uint16) (DataType, error)
func NewColumns(columns ...ColumnSpec) Columns
func (t DataType) Kind() Type
func (t DataType) Equal(other DataType) bool
func (t DataType) Frozen() bool
func (t DataType) FrozenKnown() bool
func (t DataType) WithFrozen(frozen bool) DataType
func (t DataType) Element() (DataType, bool)
func (t DataType) KeyValue() (DataType, DataType, bool)
func (t DataType) Fields() Columns
func (t DataType) UDTName() (keyspace, name string, ok bool)
func (t DataType) Dimension() (uint16, bool)
func (c Columns) Len() int
func (c Columns) At(index int) (ColumnSpec, bool)
func (c Columns) Equal(other Columns) bool

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
type ParamPlan[P any] interface { Encode(*ParamWriter, P) error }
type RowDecoder[R any] interface {
    Compile(Columns) (RowPlan[R], error)
    Clone(R) R
}
type RowPlan[R any] interface {
    Decode(*RowReader) (R, error)
    Clone(R) R
}
```

`CheckType` runs when compiling a plan, including all children even for empty,
null or unset values. It runs again when the actual metadata changes. A compiled
plan is immutable, safe for concurrent use, and does not capture mutable
per-call state. Encode/Decode get the checked full descriptor. Custom
implementations must not assume their callers have run CheckType; public entry
points always do. Custom `Clone` must recursively detach all borrowed storage;
scalar implementations return the value unchanged. Compilation may allocate
descriptor/mapping storage; row and scalar decoding must not repeat it.

RowDecoder.Clone is required independently of Compile: it recursively detaches
the logical Go value using its declared field codecs, without needing actual
server metadata, a prior successful plan, or retained decode state. This makes
TupleDecoder/UDTDecoder.Clone correct after standalone DecodeValue as well as
normal row execution. RowPlan.Clone remains available for an already compiled
plan and must provide the same ownership guarantee. Row, OneColumn, PairRows,
MapRows, CASRows, DynamicRows and every custom RowDecoder implement the
schema-independent method; custom decoders must supply explicit clone logic.
Caching the first successful plan to implement Clone or panicking because no
plan was previously compiled is not permitted.

```go
type ValueWriter struct { /* current checked type, framed sink */ }
func (w *ValueWriter) Type() DataType
func (w *ValueWriter) Null() error
func (w *ValueWriter) Unset() error
func (w *ValueWriter) Empty() error
func (w *ValueWriter) BorrowBytes([]byte) error
func (w *ValueWriter) BorrowString(string) error
func (w *ValueWriter) Uint8(uint8) error
func (w *ValueWriter) Uint16(uint16) error
func (w *ValueWriter) Uint32(uint32) error
func (w *ValueWriter) Uint64(uint64) error
func (w *ValueWriter) Child(DataType, func(*ValueWriter) error) error

type ValueReader struct { /* checked type, cell state, segmented bytes, lease */ }
func (r ValueReader) Type() DataType
func (r ValueReader) State() CellState
func (r ValueReader) Bytes() Bytes
func (r ValueReader) Cursor() *ValueCursor

type ValueCursor struct { /* checked segmented cursor */ }
func (c *ValueCursor) Remaining() int64
func (c *ValueCursor) Uint8() (uint8, error)
func (c *ValueCursor) Uint16() (uint16, error)
func (c *ValueCursor) Uint32() (uint32, error)
func (c *ValueCursor) Uint64() (uint64, error)
func (c *ValueCursor) Take(int64) (Bytes, error)
func (c *ValueCursor) Child(DataType) (ValueReader, error)
func (c *ValueCursor) RequireEnd() error

type ParamWriter struct { /* exact metadata count, framing transaction */ }
func WriteParam[T any](w *ParamWriter, encoder Encoder[T], value T) error
type RowReader struct { /* current physical row, checked cursor */ }
func ReadColumn[T any](r *RowReader, decoder Decoder[T]) (T, error)
func (r *RowReader) SkipColumn() error
```

Native cells have three framing states: present, null (-1), and unset (-2).
CellState.Empty is a logical supported-type state over present zero-length wire
bytes, never a fourth negative sentinel. Empty() checks type support, writes no
body, and cannot coexist with payload/Null/Unset writes. Unknown CellState
values fail validation. Null/Unset cannot follow or precede payload writes in a
cell. A value encoder writes the body; the enclosing writer owns the length.
Child writes a length-framed nested cell; raw scalar methods write network byte
order. Decode readers are value snapshots; independent cursors do not advance
other readers. Invalid custom framing or a failed encoder rolls back the entire
cell and causes binding to fail before network submission. A failed complete
parameter encoding never produces an executable bound object.

## Standalone serialization

Binary fixtures and applications can use the same checked generic codecs without
opening a session. EncodedValue holds an immutable descriptor, state and
segmented body. Its constructor validates state/body consistency, not scalar
semantics; DecodeValue validates type and payload. NewEncodedValue requires zero
body for Null/Unset/Empty, validates descriptors and rejects unknown states;
Empty also requires an EmptyCodec-supported type. Its logical Empty state is
normalized to Present zero bytes for ValueReader, so the selected decoder
determines meaning. Wire readers likewise report Present for length zero;
text/blob zero bytes always remain ordinary present values. EmptyCodec
interprets supported zero-byte bodies as Empty, while ordinary fixed-width
codecs reject invalid zero widths. Dynamic Value exposes logical Empty only for
its explicitly supported empty kinds. Outbound unset is permitted only in this
standalone representation or a top-level binding, never in a result decode.

```go
type EncodedValue struct { /* private type, state and Bytes */ }
func NewEncodedValue(DataType, CellState, Bytes) (EncodedValue, error)
func (v EncodedValue) Type() DataType
func (v EncodedValue) State() CellState
func (v EncodedValue) Bytes() Bytes
func (v EncodedValue) Clone() EncodedValue
func EncodeValueBorrowed[T any](DataType, Encoder[T], T) (EncodedValue, error)
func EncodeValueOwned[T any](DataType, Encoder[T], T) (EncodedValue, error)
func DecodeValue[T any](Decoder[T], EncodedValue) (T, error)
```

Borrowed encoded values require caller payload lifetime and immutability while
any encoded value/decoded view is used; these synchronous helpers start no
background work. They do not retain source page leases: the caller must retain
those leases separately. Original received views detect a released owner, but a
derived standalone encoded span cannot detect that its externally owned source
was released. Use EncodeValueOwned or Clone to detach. Owned encoding clones
caller-backed payload once, sharing already-owned scalar encoding. Clone
detaches bytes explicitly. These functions call the exact
ValueWriter/ValueReader used in request binding and result decoding; no second
serialization implementation is permitted.

## Typed parameter and result composition

No field discovery uses reflection or struct tags. Applications compose typed
accessors, or generate equivalent Go implementations. Named parameters are
mapped to server order once. Duplicate marker names may reuse one input field;
unused or missing source names fail with deterministic diagnostics. Result
aliases are the column names. Duplicate selected names require positional
mapping. All selected columns must be accounted for, including explicit ignored
fields.

```go
type Unit struct{}
type Pair[A, B any] struct { First A; Second B }
type InputField[P any] interface { /* sealed descriptor */ }
type OutputField[R any] interface { /* sealed descriptor */ }
func Param[P, V any](name string, codec Encoder[V], get func(P) V) InputField[P]
func ParamAt[P, V any](index int, codec Encoder[V], get func(P) V) InputField[P]
func Params[P any](fields ...InputField[P]) ParamEncoder[P]
func Column[R, V any](name string, codec Decoder[V], field func(*R) *V) OutputField[R]
func ColumnAt[R, V any](index int, codec Decoder[V], field func(*R) *V) OutputField[R]
func IgnoreColumn[R any](name string) OutputField[R]
func Row[R any](fields ...OutputField[R]) RowDecoder[R]
func OneParam[T any](Encoder[T]) ParamEncoder[T]
func OneColumn[T any](Decoder[T]) RowDecoder[T]
func PairParams[A, B any](leftColumns int, a ParamEncoder[A], b ParamEncoder[B]) ParamEncoder[Pair[A, B]]
func PairRows[A, B any](leftColumns int, a RowDecoder[A], b RowDecoder[B]) RowDecoder[Pair[A, B]]
func MapParams[A, B any](ParamEncoder[B], func(A) B) ParamEncoder[A]
func MapRows[A, B any](RowDecoder[A], func(A) B, func(B) B) RowDecoder[B]
var NoParams ParamEncoder[Unit]
var NoRows RowDecoder[Unit]
```

`Column`/`ColumnAt` use a typed field-address accessor so decoding and recursive
cloning both reach the same field without reflection. Accessors must return a
non-nil address belonging to the supplied row; they must not capture an external
mutable destination. `PairParams`/`PairRows` split Columns at the explicit
checked leftColumns arity and concatenate positional plans; named composition
uses `Params`/`Row` with accessors. `MapRows`' final function is its
ownership-detaching clone; omitting correct clone semantics is a custom codec
contract violation. `NoRows` accepts only results with zero physical columns;
use a real decoder for conditional writes. Ignored result fields still have
validated framing.

## Scalar values, null, unset and empty

```go
type CellState uint8
const (Present CellState = iota; Null; Unset; Empty)
type Nullable[T any] struct { Value T; Valid bool }
type BindValue[T any] struct { Value T; State CellState }
func NullableCodec[T any](Codec[T]) Codec[Nullable[T]]
func NullableEncoder[T any](Encoder[T]) Encoder[Nullable[T]]
func NullableDecoder[T any](Decoder[T]) Decoder[Nullable[T]]
func BindEncoder[T any](Encoder[T]) Encoder[BindValue[T]]
type EmptyValueOf[T any] struct { Value T; Empty bool }
func EmptyCodec[T any](Codec[T]) Codec[EmptyValueOf[T]]

// Existing UUID and Duration (months, days, nanoseconds) value types remain.
type Counter int64
type Date int32                  // days relative to Unix epoch
type TimeOfDay int64             // nanoseconds since midnight
type Timestamp int64             // milliseconds relative to Unix epoch
type TimeUUID UUID
type Varint struct { /* canonical/sign-preserving segmented two's complement */ }
type Decimal struct { Scale int32; Unscaled Varint }

var Bool Codec[bool]
var Int8 Codec[int8]
var Int16 Codec[int16]
var Int32 Codec[int32]
var Int64 Codec[int64]
var Float32 Codec[float32]
var Float64 Codec[float64]
var CounterCodec Codec[Counter]
var DateCodec Codec[Date]
var TimeCodec Codec[TimeOfDay]
var TimestampCodec Codec[Timestamp]
var DurationCodec Codec[Duration]
var UUIDCodec Codec[UUID]
var TimeUUIDCodec Codec[TimeUUID]
var InetCodec Codec[netip.Addr]
var VarintCodec Codec[Varint]
var DecimalCodec Codec[Decimal]
var StringEncoder Encoder[string]             // text/varchar
var ASCIIEncoder Encoder[string]
var BytesEncoder Encoder[[]byte]              // blob; borrows input
var TextCodec Codec[Text]                     // text/varchar borrowed view
var ASCIICodec Codec[Text]
var BlobCodec Codec[Bytes]
func NamedScalar[A, B any](Codec[B], func(A) B, func(B) A) Codec[A]
func VarintFromBigInt(*big.Int) Varint
func (v Varint) BigInt() *big.Int
func VarintFromBytes([]byte) (Varint, error)
func DateFromTime(time.Time) (Date, error)
func (d Date) Time() (time.Time, error)
func TimestampFromTime(time.Time) (Timestamp, error)
func (t Timestamp) Time() (time.Time, error)
```

Existing `TimeUUID()` UUID generator is renamed `NewTimeUUID()`; UUID creation,
parsing, timestamp extraction and ordering helpers otherwise remain value APIs.
VectorDataType has Kind TypeCustom plus Dimension; ScalarType(TypeCustom)
rejects an incomplete custom descriptor.

Nil `[]byte` passed to BytesEncoder is present empty blob. Null is always
explicit; nil collections likewise encode a zero count. Nonnullable decoders
reject null, including collections; Nullable preserves null independently of
empty. Unset is only an outer bound parameter on v4+; reject unset in results
and nested values. Empty is distinct from zero-byte text/blob. EmptyCodec
rejects ASCII/text/blob, whose zero-length payload is Present. It accepts
boolean, integers/floats, date, time, timestamp, UUID/timeuuid, inet, varint,
decimal and tuple, but never counter, duration, collection, UDT or vector. The
outbound type remains checked even when null/unset skips payload encoding.

Integers use exact widths. Fixed values reject wrong lengths. ASCII/UTF-8 are
validated in both directions, including across chunks. Boolean accepts 0 and 1
only. TimeOfDay must be within the day; TimeUUID must be version 1; duration
components have compatible signs and int32/int32/int64 ranges. Checked signed
vints consume the entire duration. Varint retains full signed precision; decimal
preserves the signed scale and the exact unscaled integer. Explicit big.Int,
calendar and owned-string conversions are allocation/representation operations,
not implicit work in typed execution. No numeric coercion silently narrows
values.

## Borrowed bytes, text and container values

```go
type Bytes struct { /* chain range + stable page-owner generation */ }
type Text struct { /* validated UTF-8/ASCII Bytes */ }
func BorrowBytes([]byte) Bytes
func BorrowText(string) (Text, error)
func (b Bytes) Len() int64
func (b Bytes) At(int64) (byte, error)
func (b Bytes) Slice(offset, length int64) (Bytes, error)
func (b Bytes) WriteTo(io.Writer) (int64, error)
func (b Bytes) CopyTo([]byte) (int, error)
func (b Bytes) Clone() Bytes
func (b Bytes) OwnedBytes() []byte
func (t Text) Bytes() Bytes
func (t Text) Clone() Text
func (t Text) String() string

type Sequence[T any] struct { /* lazy list/set/vector view */ }
type Mapping[K, V any] struct { /* lazy map view */ }
type Entry[K, V any] struct { Key K; Value V }
type ValueIterator[T any] struct { /* bounded checked cursor */ }
func (s Sequence[T]) Len() int
func (s Sequence[T]) Iterator() *ValueIterator[T]
func (s Sequence[T]) Clone() Sequence[T]
func (m Mapping[K, V]) Len() int
func (m Mapping[K, V]) Iterator() *ValueIterator[Entry[K, V]]
func (m Mapping[K, V]) Clone() Mapping[K, V]
func (i *ValueIterator[T]) Next() bool
func (i *ValueIterator[T]) Value() T
func (i *ValueIterator[T]) Err() error
func ListEncoder[T any](Encoder[T]) Encoder[[]T]
func SetEncoder[T any](Encoder[T]) Encoder[[]T]
func MapEncoder[K, V any](Encoder[K], Encoder[V]) Encoder[[]Entry[K, V]]
func ListDecoder[T any](Decoder[T]) Decoder[Sequence[T]]
func SetDecoder[T any](Decoder[T]) Decoder[Sequence[T]]
func MapDecoder[K, V any](Decoder[K], Decoder[V]) Decoder[Mapping[K, V]]
func VectorEncoder[T any](Encoder[T]) Encoder[[]T]
func VectorDecoder[T any](Decoder[T]) Decoder[Sequence[T]]
func TupleEncoder[T any](ParamEncoder[T]) Encoder[T]
func TupleDecoder[T any](RowDecoder[T]) Decoder[T]
type UDTOptions struct {
    Keyspace, Name string // required expected UDT identity, exact metadata names
    AllowExtraDatabaseFields, AllowMissingFields bool
}
func UDTEncoder[T any](ParamEncoder[T], UDTOptions) Encoder[T]
func UDTDecoder[T any](RowDecoder[T], UDTOptions) Decoder[T]
func Collect[T any](*ValueIterator[T]) ([]T, error)
```

BorrowText validates UTF-8 and returns ValidationError for invalid input.
TextValue likewise returns an error for invalid UTF-8. StringEncoder and
ASCIIEncoder return ordinary codec errors for invalid encoded input. Malformed
input is never a reason for a constructor panic.

The default receive representations retain fragments; no cell is flattened when
it crosses a v5 segment. String(), OwnedBytes(), Clone(), Collect() and explicit
big.Int/calendar adapters are requested materializations. Clone on a lazy
container clones its encoded backing range without eager child decoding. It
detaches from pool ownership; GC reclaims the clone. Collect allocates the
output slice and recursively detaches every yielded element using the iterator
decoder's Clone; Text, Bytes, Value and nested aggregates therefore remain
usable after the source page is released. On any iterator error it returns no
partial slice. Iterators are allocation-free per element, stop permanently on
the first error, enforce counts, reject malformed negative
lengths/overflows/trailing payload, and never publish partial malformed values.
A successful lazy decode validates enclosing framing/count structure; each
scalar is checked when requested. An early-stopped iterator does not claim
semantic validation of unvisited scalar values.

Tuples preserve physical columns: they never expand implicitly into destination
arguments. Missing serialized trailing tuple/UDT members are null only where the
protocol permits; a nonnullable destination rejects them. UDT matching defaults
to exact field names and full identity/type validation. UDTOptions.Keyspace/Name
must supply the expected identity; missing names fail CheckType/Compile with a
validation error, and equal layouts under a different identity are incompatible.
Explicit options permit extra database fields (encode explicit null; decode
checked skips) or application fields absent from the database (encode ignores
those fields; decode sets explicit zero defaults only when allowed).
Null-default behavior requires Nullable or a custom codec. Unknown application
fields are errors unless AllowMissingFields is explicitly set. All actual
framing is consumed even for ignored suffixes. For a present tuple with an empty
body, ordinary TupleDecoder applies the omitted nullable-suffix rule. Explicit
EmptyCodec(TupleDecoder/Encoder composition) instead intercepts the zero-byte
body as Empty=true before the tuple codec. These are codec-selected
interpretations of identical wire bytes, not distinguishable native states.
TupleEncoder emits a frame for every declared field, including explicit null
suffix frames, so an ordinary nonzero-arity tuple encoded through EmptyCodec
with Empty=false cannot accidentally become zero bytes. Zero-arity tuples have
no distinct ordinary/Empty wire representation and an Empty wrapper
canonicalizes their zero bytes to Empty=true. Dynamic Value's tuple-empty
variant uses that explicit empty interpretation. Tuple Empty support is
preserved.

Vectors validate dimension and checked aggregate width; zero dimension is
rejected. They use the server's fixed/variable vector layouts, including nested
fixed vectors; no short vectors, implicit null padding, or surplus elements are
accepted.

## Metadata-driven values

Dynamic behavior uses a tagged representation, never reflection-created Go
values or `map[string]any`. Value's private representation contains the actual
DataType, state, scalar payload or a lazy encoded aggregate. Constructors store
borrowed input payloads; normal binding lifetime rules apply. Decoded Value and
DynamicRow borrow the current page owner without acquiring an independent lease
reference. Their views expire under the same Next/Close rule as typed rows; only
an explicit PageLease or Clone extends their usable lifetime. An aggregate may
hold explicitly supplied `[]Value` descriptors on input or encoded bytes on
output, but neither form causes eager payload flattening.

```go
type Value struct { /* explicit discriminant; no any field */ }
type ValueField struct { Name string; Value Value }
type ValueEntry struct { Key, Value Value }
func (v Value) Type() DataType
func (v Value) State() CellState
func (v Value) Clone() Value
func (v Value) Bool() (bool, error)
func (v Value) Signed() (int64, error)
func (v Value) Float() (float64, error)
func (v Value) Text() (Text, error)
func (v Value) Bytes() (Bytes, error)
func (v Value) UUID() (UUID, error)
func (v Value) Inet() (netip.Addr, error)
func (v Value) Duration() (Duration, error)
func (v Value) Varint() (Varint, error)
func (v Value) Decimal() (Decimal, error)
func (v Value) Elements() (*ValueIterator[Value], error)
func (v Value) Entries() (*ValueIterator[ValueEntry], error)
func (v Value) Fields() (*ValueIterator[ValueField], error)
func NullValue(DataType) Value
func UnsetDynamic(DataType) Value
func EmptyValue(DataType) (Value, error)
func BoolValue(bool) Value
func TinyIntValue(int8) Value
func SmallIntValue(int16) Value
func IntValue(int32) Value
func BigIntValue(int64) Value
func CounterValue(Counter) Value
func FloatValue(float32) Value
func DoubleValue(float64) Value
func TextValue(string) (Value, error)
func ASCIIValue(string) (Value, error)
func BlobValue([]byte) Value
func UUIDValue(UUID) Value
func TimeUUIDValue(TimeUUID) (Value, error)
func InetValue(netip.Addr) (Value, error)
func DateValue(Date) Value
func TimeValue(TimeOfDay) (Value, error)
func TimestampValue(Timestamp) Value
func DurationValue(Duration) (Value, error)
func VarintValue(Varint) Value
func DecimalValue(Decimal) Value
func ListValue(DataType, []Value) (Value, error)
func SetValue(DataType, []Value) (Value, error)
func MapValue(DataType, []ValueEntry) (Value, error)
func TupleValue(DataType, []Value) (Value, error)
func UDTValue(DataType, []ValueField) (Value, error)
func VectorValue(DataType, []Value) (Value, error)
var DynamicCodec Codec[Value]
var DynamicParams ParamEncoder[[]Value]
var DynamicRows RowDecoder[DynamicRow]
type DynamicRow struct { /* ordered Columns and lazy Values */ }
func (r DynamicRow) Columns() Columns
func (r DynamicRow) At(int) (Value, error)
func (r DynamicRow) Get(string) (Value, error)
func (r DynamicRow) Clone() DynamicRow
```

Named dynamic binding uses `Params[[]ValueField]` accessors or the dedicated
`NamedDynamicParams ParamEncoder[[]ValueField]`. Duplicate names are rejected.
Dynamic aggregate variants must match the actual outer type, including list
versus set versus vector, and a UDT's identity. Dynamic malformed scalars are
rejected with the same rules as typed codecs; no implicit universal Empty
fallback.

## Statements, preparation, binding and execution

```go
type StatementKind uint8
const (UnknownStatement StatementKind = iota; OrdinaryStatement; ConditionalStatement)
type Statement[P, R any] struct {
    CQL string
    Kind StatementKind
    Params ParamEncoder[P]
    Rows RowDecoder[R]
}
type Prepared[P, R any] struct { /* immutable handle, synchronized metadata */ }
type Bound[R any] struct { /* immutable encoded params, ownership gate */ }
func Prepare[P, R any](context.Context, *Session, Statement[P, R], PrepareOptions) (*Prepared[P, R], error)
func BindBorrowed[P, R any](*Prepared[P, R], P) (*Bound[R], error)
func BindOwned[P, R any](*Prepared[P, R], P) (*Bound[R], error)
func BindUnpreparedBorrowed[P, R any](Statement[P, R], Columns, P) (*Bound[R], error)
func BindUnpreparedOwned[P, R any](Statement[P, R], Columns, P) (*Bound[R], error)
func (p *Prepared[P, R]) Parameters() Columns
func (p *Prepared[P, R]) Columns() Columns
func (p *Prepared[P, R]) Info() PreparedInfo
func (b *Bound[R]) Close()
func (b *Bound[R]) Wait(context.Context) error
func (b *Bound[R]) Routing() RoutingInfo
func Run[R any](context.Context, *Session, *Bound[R], Options) (*Rows[R], error)
func RunPage[R any](context.Context, *Session, *Bound[R], PageState, Options) (*Page[R], error)
func RunUnpaged[R any](context.Context, *Session, *Bound[R], Options) (*Page[R], error)
func Exec[R any](context.Context, *Session, *Bound[R], Options) (Result, error)

type PrepareOptions struct { Keyspace string; HostID Option[UUID]; Trace bool }
type PreparedInfo struct {
    Keyspace, Table string
    PartitionKeyIndexes []int
    Kind StatementKind
    Generation uint64
}
type Option[T any] struct { Value T; Set bool }
```

Prepare runs under its context including DNS/pool acquisition and all local
preparation. It prepares one eligible connection, installs metadata and caches
by exact CQL, effective keyspace and schema generation. Other hosts prepare
lazily; background warming may be enabled, but is never an implicit all-host
latency requirement. The server ID remains per-host/per-generation. A cache hit
still checks the supplied codec plans; a handle remains valid after LRU
eviction. Statement.Kind is an explicit caller classification, with zero meaning
unknown. A reliable negotiated server conditional flag refines unknown
classification; a confirmed conditional flag always overrides an ordinary
declaration. Cassandra and direct QUERY without such metadata remain unknown
unless the caller declares them. Declaring OrdinaryStatement asserts that CQL
has no conditional operation; an incorrect declaration violates the caller
contract. The driver does not infer this by parsing CQL. Unknown statements
conservatively disable speculation and use deterministic conditional ordering.
Regular SERIAL/LOCAL_SERIAL always makes the effective operation conditional;
setting only optional serial consistency does not. Statements with no FROM do
not invent table/routing metadata. Unprepared binding requires caller-declared
parameter Columns because QUERY has no prepare metadata; zero parameters use
NewColumns(). The QUERY values flag is populated and the provided types govern
encoding. Server validation remains authoritative.

Bind encodes exactly once into canonical immutable spans, checks all counts,
metadata and partition-key indexes, and creates the routing token from those
same bytes. It does not run custom encoders again for routing, retries,
speculation, batch repair or paging. BindOwned sets owned-input mode before
encoding: caller-backed text/blob spans are copied once when written, while
generated scalar/transform spans remain in the same arena. Children inherit the
mode; no whole-chain post-encoding clone runs. BindBorrowed borrows slices,
strings and recursively nested variable data. The caller must not mutate or
recycle them until the binding is closed and Wait succeeds. Retaining an
ordinary Go reference is insufficient if the application mutates the bytes. For
page-derived input views the caller must retain the source page lease through
successful Bound.Wait; BindBorrowed does not acquire those leases implicitly.

Close atomically rejects new Run/RunPage/Exec/batch admissions; it is idempotent
and nonblocking. Admitted executions continue. Wait requires Close first and
waits until every admitted request/pager and every active/queued writer,
including speculative losers and repair resends, has released its input
reference and all borrowed Options.CustomPayload references attached to that
admission. A timed out Wait is **not** permission to reuse inputs. Close
Rows/Page to release pager admissions before waiting. Output page leases never
retain a binding or its input owner. For example: `bound.Close(); rows.Close();
bound.Wait(ctx)` is valid. Owned bindings use the same Close/Wait mechanism to
release resources promptly.

On reprepare, validate bind metadata against the encoded schema, not merely
marker count. Incompatible type/order/identity yields `SchemaChangedError`
before resend; the application explicitly prepares and binds again. Routing
schema compatibility includes partition-key index order, effective table
identity, selected partitioner and conditional classification, in addition to
parameter types/order/identity. A change to any of these requires a new explicit
prepare/bind and fails the old binding with SchemaChangedError before resend.
Compatible metadata may update per-host prepared IDs without re-encoding or
mutating the binding's canonical routing inputs. Actual result metadata always
selects/recompiles RowPlan before rows are exposed, including empty pages.
Cached NO_METADATA requires a matching generation, column count and negotiated
ID; otherwise obtain valid metadata or return a protocol error. Atomic
generation/CAS updates prevent a stale response from overwriting newer local
publications. Each attempt captures an immutable result entry and its local
revision plus schema epoch. Publication compares that exact entry/revision and
epoch; on CAS failure it never retries the stale publication against a freshly
loaded entry. A losing response still decodes with its matching retained or
response-supplied metadata. Prepared IDs and result metadata IDs are opaque
equality tokens, not ordered server schema versions. These fences prevent local
stale-update races; conflicting remote observations require a typed metadata
refresh and cannot be represented as proven global schema chronology. No unsafe
cache-metadata opt-in bypasses these rules.

## Rows, pages, leases and conditional results

```go
type Rows[R any] struct { /* automatic pager */ }
func (r *Rows[R]) Next() bool
func (r *Rows[R]) Row() R
func (r *Rows[R]) Err() error
func (r *Rows[R]) Close() error
func (r *Rows[R]) Retain() (*PageLease, error)
func (r *Rows[R]) Clone() (R, error)
func (r *Rows[R]) Info() Result
func (r *Rows[R]) Columns() Columns

type Page[R any] struct { /* exactly one response page */ }
func (p *Page[R]) Next() bool
func (p *Page[R]) Row() R
func (p *Page[R]) Err() error
func (p *Page[R]) Close() error
func (p *Page[R]) Retain() (*PageLease, error)
func (p *Page[R]) Clone() (R, error)
func (p *Page[R]) State() PageState
func (p *Page[R]) Info() Result
func (p *Page[R]) Columns() Columns

type PageLease struct { /* stable shared reference to receive storage */ }
func (l *PageLease) Close()
type PageState struct { /* start / more / complete, opaque owned token */ }
func StartPage() PageState
func RestorePageState([]byte) PageState
func (p PageState) More() bool
func (p PageState) Complete() bool
func (p PageState) Bytes() []byte

type CASResult[R any] struct { Applied bool; Previous Option[R] }
func CASRows[R any](RowDecoder[R]) RowDecoder[CASResult[R]]
```

Row() is valid after successful Next; its views remain valid until the **next**
Next call or Close, whichever comes first. Retain pins the current physical page
before advancing; its owner has stable identity independent of pooled parsers.
All views from that page remain valid until that lease closes. Clone detaches
the current row using RowPlan.Clone. Copying a Go row struct does not detach it.
Access through a released view reports ErrReleased where a method returns error;
methods without error results panic with ErrReleased rather than reading
recycled storage. Len/type metadata remain inspectable. Do not concurrently use
a view while closing its last lease. Retained leases are independent of
Session.Close.

Rows owns one current page and at most the configured bounded prefetch. Next
consumes empty pages with continuations, propagates the first error terminally,
and never loses an error behind partial results. Context governs the entire
pager lifetime; a separate configured per-page timeout may shorten it. Repeated
continuation states are compared against every previously returned continuation
in that automatic pager, including empty tokens: A → B → A therefore returns
ErrPagingNoProgress before another fetch of A. Token equality checks full bytes;
a hash alone is insufficient. Retained history is bounded by Config.Limits
MaxPagingStates (default 65,536) and MaxPagingStateBytes (default 16 MiB);
exhausting either returns terminal ErrPagingStateLimit before storing/fetching
the next state. Both budgets must be positive. Manual RunPage keeps no
cross-call history. RunPage returns an empty page and its continuation as-is.
PageState distinguishes an absent start token, present empty continuation, and
completion. Tokens returned by State/Bytes remain available after page closure;
Bytes returns an explicit owned copy of the server token, not a serialized state
tag. The zero PageState and StartPage mean start. RestorePageState always
constructs a continuation, including for nil and empty input, and copies its
argument. Bytes is nil for start and complete; use More/Complete to distinguish
them. A continuation with zero token bytes still has More()==true. Complete is
output-only: passing it to RunPage returns ErrPagingComplete before
admission/I/O. This continuation-only import API does not round-trip start or
complete through Bytes. RunUnpaged rejects a continuation. Exec drains and
closes all rows and fails if result columns exist; conditional writes use
CASRows and consume every row, including multi-row conditional batches. CASRows
handles an applied-only response and checked previous-value metadata; Previous
is absent only if no previous columns were returned.

## Options, profiles, batches and results

```go
type Options struct {
    Profile *ProfileHandle
    Consistency Option[Consistency]
    SerialConsistency Option[Option[Consistency]] // inherit / omit / set
    Timeout Option[time.Duration]                 // 0 disables profile timeout
    PageTimeout Option[time.Duration]
    PageSize Option[int32]
    Prefetch Option[int]                          // 0 or 1 page
    Idempotent bool
    Timestamp Option[int64]                       // microseconds
    NowInSeconds Option[int32]
    Keyspace string
    Trace bool
    CustomPayload []PayloadEntry
    IncludeMetadata bool                          // request full result metadata
    HostID Option[UUID]
    ShardID Option[uint32]
    Retry RetryPolicy
    Observer Observer
}
type PayloadEntry struct { Key string; Value Nullable[Bytes] }
type Profile struct {
    Consistency Consistency
    SerialConsistency Option[Consistency]
    Timeout, PageTimeout time.Duration
    PageSize int32
    Prefetch int
    Retry RetryPolicy
    Routing RoutingPolicy
    Speculation SpeculationPolicy
}
type ProfileHandle struct { /* atomic immutable profile snapshot */ }
func NewProfile(Profile) (*ProfileHandle, error)
func (p *ProfileHandle) Snapshot() Profile
func (p *ProfileHandle) Replace(Profile) error

type BatchKind uint8
const (LoggedBatch BatchKind = iota; UnloggedBatch; CounterBatch)
type BatchItem struct { /* sealed bound-plan and owner reference */ }
func BatchEntry[R any](*Bound[R]) (BatchItem, error)
func RunBatch[R any](context.Context, *Session, BatchKind, []BatchItem, RowDecoder[R], Options) (*Page[R], error)
func ExecBatch(context.Context, *Session, BatchKind, []BatchItem, Options) (Result, error)

type Result struct {
    HostID UUID
    Attempts int
    Latency time.Duration
    Rows int64
    Warnings []string
    TraceIDs []UUID
    CustomPayload []PayloadEntry
    AchievedConsistency Consistency
}
```

A zero Options inherits the selected session profile, is non-idempotent, and has
no tracing/timestamp/keyspace override. Override validation returns typed
errors, not panics. Profile objects are validated immutable snapshots; Replace
affects new admissions only, never active retries or later pages. Defaults:
LOCAL_QUORUM, LOCAL_SERIAL, 30-second request timeout, 5,000-row pages, one-page
prefetch, no speculation, and the safe retry policy. Caller context always
bounds request work, including preparation, metadata refresh, agreement,
queueing and cleanup waits. Cleanup continues independently where required to
release an orphan stream; returning a context error never authorizes reuse of
still-borrowed input before Wait. At admission the driver copies CustomPayload
entry descriptors, never their payload bytes. Borrowed CustomPayload bytes are
attached to the same binding admission as the request and remain immutable until
Bound.Close plus successful Bound.Wait, even after an early context error or
speculative winner. If the payload uses received views, the caller retains the
source PageLease through that wait. Before-admission errors retain no options
payload references. For batches, close and successfully wait for every member
binding before recycling shared options payloads or their source leases. Callers
wanting independent payload storage explicitly Clone each Bytes value before
submission; there is no hidden payload copy at admission. Returned Result
payloads use owned storage, because Result outlives pages; this explicit escape
is an ownership conversion, not a per-cell receive copy.

Keyspace affects prepare/cache identity and v5 QUERY/BATCH flags; an
incompatible post-bind override is rejected. v3/v4 nondefault keyspace requires
a qualified CQL statement or a session established in that keyspace. No
concurrently mutable session-wide USE operation exists. Host pinning still
validates host eligibility; a shard ID is a hint unless Target.StrictShard is
set by the routing policy. StrictShard requires a present shard ID; absent,
out-of-range or unavailable strict shards return a typed target error without
same-host shard fallback. HostID pinning never falls back to a different host.
Target strictness survives plan, attempt and pool seams; SingleTargetRouting
propagates its strictShard argument unchanged. Unsupported version options fail
before send. Timestamps use one value per logical execution reused across
retries; generated timestamps are concurrency-safe.

BatchEntry holds a descriptor, not an admission. RunBatch atomically admits
every member before sending and releases all if any is closed. At most 65,535
entries and 65,535 parameters/member; reject size violations before
preparation/I/O. Mixed prepared and QUERY members are legal. Binding spans are
reused unchanged; UNPREPARED repairs only the matching host statement and
validates its new schema. All member statements must have compatible effective
keyspace. Routing chooses the first member with a complete valid partition key,
records this choice, and never claims a cross-partition batch has only one
target replica set. Counters and LWT retain server restrictions. Batch
idempotence defaults false and must be explicit. Batch classification is
conditional if any member is conditional (or regular consistency is serial),
otherwise unknown if any member is unknown, otherwise ordinary. Unknown and
conditional batches are never speculated.

## Policies, metadata, events, configuration and observation

Existing value-only metadata/error structures may be retained after removing
reflection hooks. The following stable seams make policy and lifecycle behavior
available without exposing old Query/Iter objects.

```go
type RoutingInfo struct {
    Keyspace, Table string
    Token Option[int64]
    Kind StatementKind
    Consistency Consistency
    HostID Option[UUID]
    ShardID Option[uint32]
}
type Target struct { HostID UUID; ShardID Option[uint32]; StrictShard bool }
type QueryPlan interface { Next() (Target, bool) }
type RoutingPolicy interface {
    Validate(*MetadataSnapshot) error
    Plan(RoutingInfo, *MetadataSnapshot) QueryPlan
    Observe(AttemptEvent)
}
type RetryDecision struct {
    Action RetryAction
    Consistency Option[Consistency]
    Delay time.Duration
}
type RetryAction uint8
const (StopRetry RetryAction = iota; RetrySame; RetryNext; IgnoreWriteError)
type RetryPolicy interface { Decide(AttemptError) RetryDecision }
type SpeculationPolicy interface { Delay(attempt int, metrics MetricsSnapshot) (time.Duration, bool) }
type AttemptError struct {
    Err error
    Attempt int
    Idempotent bool
    Consistency Consistency
    Conditional bool
    BytesWritten bool
}
type Observer interface { ObserveRequest(RequestEvent); ObserveAttempt(AttemptEvent) }
type RequestEvent struct {
    ID uint64
    Statement string
    BatchStatements []string
    Parameters Columns
    Routing RoutingInfo
    Result Result
    Err error
}
type AttemptEvent struct {
    RequestID uint64
    Attempt int
    Target Target
    Started time.Time
    Latency time.Duration
    Err error
    Cancelled, Speculative bool
}
```

Built-in routing has a concrete configuration surface. DefaultConfig selects a
token-aware policy with local preference when configured, replica shuffling for
ordinary requests, deterministic conditional ordering and no remote fallback
unless allowed. Validate runs at session startup and on eligible-topology
changes; missing preferred DC/rack returns a configuration error rather than an
empty plan. Plan itself never does I/O or reports a hidden failure through a
panic.

```go
type RoutingOptions struct {
    TokenAware bool
    Datacenter, Rack string
    AllowRemote bool
    ShuffleReplicas bool
    LeaderAware bool
    Latency LatencyOptions
}
type LatencyOptions struct {
    Enabled bool
    ExclusionThreshold float64
    MinimumSamples int
    Scale, RetryPeriod, UpdateInterval time.Duration
}
func NewRoutingPolicy(RoutingOptions) (RoutingPolicy, error)
func SingleTargetRouting(Target, strictShard bool) RoutingPolicy
type PartitionKey struct { /* logical canonical encoded component stream */ }
func (k PartitionKey) Len() int64
func (k PartitionKey) ForEach(func(Bytes) error) error
type TokenPartitioner interface { Token(PartitionKey) (int64, error) }
var Murmur3TokenPartitioner TokenPartitioner
var CDCTokenPartitioner TokenPartitioner
```

Tablet v2 takes precedence over v1 when both are negotiated. Every EXECUTE on a
v2 connection writes exactly one trailing version-block byte; QUERY/BATCH never
do. For a known opaque 64-bit version, choose nibble index 0..15 and encode it
in the high nibble with that version's selected least-significant-first nibble
in the low nibble. Unknown range with table/token uses a random byte; absent
table/token uses zero. Retries may reuse the chosen byte. Opaque versions have
equality only, never numeric age ordering. Valid ranges are nonwrapping
(first,last] with first<last; an accepted update replaces every overlapping
cached range entirely. Retain original replica order and shard hints, reject
malformed declared entries as a whole, and never fall back to vnode placement
for unknown tablet ranges.

Feedback carries the request's table identity and schema/placement epochs.
Reject feedback for dropped/recreated or invalidated identities; receipt order
cannot prove server-version freshness. Negotiate/apply only known payload
schemas; malformed optional feedback produces a diagnostic without replacing a
valid query result. For globally strong-consistent tablet keyspaces, a
token-aware plan at regular consistency other than ONE/LOCAL_ONE promotes the
first original v2 replica as leader only when connected, enabled and
locality-eligible. An unresolved original leader never promotes the first
resolved follower. Unknown/ineligible leaders use ordinary replica/nonreplica
fallback. LWT replica ordering remains deterministic; ordinary replica shuffling
and configured latency ordering apply without admitting excluded hosts. This
required leader path must be implemented.

Config.Partitioners maps server partitioner names to TokenPartitioner instances.
Built-in Murmur3 and CDC implementations are installed by default. Unknown
partitioners disable token routing and expose a diagnostic; they do not use the
wrong hash. PartitionKey includes composite length/terminator framing as needed;
ForEach streams it in canonical order without materialization. Custom
partitioners must consume it synchronously and not retain borrowed spans.

Policies may not mutate request inputs; no observer receives `[]any`. Observer
callbacks are synchronous and must not block; panic containment logs observer
failure without corrupting execution. There is one terminal request event and
one terminal event per started attempt, including cancellation and speculative
losers. Row cardinality is separate from successful server execution. Trace
IDs/warnings are preserved per response; Result aggregates across consumed
pages. Tracing lookup uses typed internal queries and context-bounded readiness
polling.

The safe retry policy only retries unknown-outcome writes when explicitly
idempotent. Read timeout/unavailable repair follows server evidence with bounded
attempts; deterministic failures never retry. Optional downgrading is separately
named/configured and reports achieved consistency. Only explicitly or reliably
server-confirmed ordinary statements may speculate, and only when idempotent.
Unknown/conditional statements and batches never speculate; regular
SERIAL/LOCAL_SERIAL and prepared LWT markers force conditional classification
regardless of declarations. Plan advancement is serialized across speculative
branches. Valid feedback accepted from completed responses is applied at most
once even if the branch loses; bounded coalescing may supersede equivalent
updates. Feedback must not block successful execution. On queue saturation,
advance a placement invalidation epoch (globally if necessary), mark affected
mappings unknown and surface a diagnostic; later EXECUTE responses relearn
placement. Do not retain known-stale mappings, leak page owners, or fail a
successful response due solely to saturation. Invalidation takes effect through
a bounded atomic signal even when the normal queue is full.

```go
func (s *Session) Metadata() *MetadataSnapshot
func (s *Session) RefreshMetadata(context.Context) error
func (s *Session) AwaitSchemaAgreement(context.Context) error
func (s *Session) Subscribe(EventFilter) (*Subscription, error)
func (s *Session) Metrics() MetricsSnapshot
func (s *Session) Trace(context.Context, UUID) (TraceInfo, error)
func (s *Session) Close() error
func (s *Session) Closed() bool
func (m *MetadataSnapshot) Hosts() []HostSnapshot
func (m *MetadataSnapshot) Keyspace(string) (*KeyspaceMetadata, bool)
func (m *MetadataSnapshot) Replicas(keyspace, table string, token int64) ([]Target, error)
func (m *MetadataSnapshot) TokenRanges(keyspace, table string) ([]TokenRange, error)
func (m *MetadataSnapshot) Generation() uint64
func (s *Subscription) Events() <-chan Event
func (s *Subscription) Close()
```

MetadataSnapshot exposes immutable snapshots, with full existing schema
features: keyspaces, replication, tables, columns, ordered keys, UDTs, views,
indexes, functions/aggregates, recreation CQL, node addresses/tokens and
tablets. Retained snapshots survive refresh. Accessors returning existing
mutable metadata structs or exported slice/map fields return defensive deep
metadata copies; private hot routing uses immutable internal snapshots directly.
HostSnapshot separates enabled/connected/up state. Events have explicit
topology/status/schema/control/client-route variants and carry a generation.
Publication events are emitted only after the corresponding snapshot is visible.
A subscription has a bounded queue (positive Capacity; zero selects 64, negative
is invalid). On overflow the dispatcher discards queued deltas, guarantees one
OverflowEvent carrying the highest discarded generation, then closes that
subscription after the marker; it never waits on control I/O. Resynchronize by
subscribing again first, then reading Metadata (or completing an explicit
refresh if freshness is required), and ignoring queued publication events at or
below the snapshot generation. This subscribe-before-snapshot rule also applies
to initial subscription. Subscription.Close is idempotent, cancels delivery
without requiring the caller to drain, and cannot block control work. Schema
refresh validates duplicate identities, key order, UDT cycles and type syntax;
an incomplete topology read cannot replace a complete valid snapshot. Bad
individual peers may be skipped with evidence; invalid tokens are never replaced
by random tokens. Recoverable keyspace errors preserve that keyspace's previous
schema and schedule retry, with newer drops winning. Refresh returns a
structured partial-refresh error when it preserves stale data. Fetches capture
change epochs; per-object drop and route-deletion tombstones fence out
superseded full/partial reads and recoverable-error fallback. Recreation
advances identity/epoch rather than allowing old results to resurrect an object.
Tombstones remain until no older fetch/feedback can publish.

AwaitSchemaAgreement checks all enabled hosts in one snapshot per check,
including zero-token hosts; filtered hosts are excluded. It queries each host
with a usable pool directly at ONE with idempotent typed queries. Missing/broken
peers prevent agreement and are retried until the minimum of caller/configured
agreement deadline; zero successes is never agreement.
Authorization/protocol/validation failures are terminal. Each retry takes a
fresh snapshot. Automatic handling of a successful DDL result additionally
requires that exact coordinator host identity to report successfully, even if a
later topology excludes or temporarily loses it; absence cannot count as
agreement. Manual agreement does not refresh metadata. Automatic
agreement/refresh runs within the original operation context; failure reports
that the DDL already succeeded and never retries the successful DDL.

Session.Close atomically rejects new session work, sets Closed()==true, cancels
active requests/pagers and background control/pool work, and closes connections
to interrupt blocked I/O. It waits for accepted writers and driver workers to
finish and releases only undelivered/prefetched pages. A delivered current page
keeps its independent output owner until the application's next Next or
Rows/Page Close, so Session.Close cannot recycle a still-valid current row.
Shutdown and binding input drains do not wait for that output owner or
independently retained PageLease objects. A subsequent Next releases the current
page and reports ErrClosed unless an earlier terminal error already won;
Rows/Page Close remains idempotent. Concurrent/repeated Session.Close calls wait
for the same shutdown completion and return the same shutdown result. Bindings
still require their own Close/Wait before input reuse; shutdown drains all
admissions made by this session without closing unadmitted bindings.

Configuration retains useful existing setup capabilities but is an immutable
snapshot after Connect. Exact public field definitions are given in the
companion implementation contract; these functions are the entry points:

```go
func DefaultConfig(contactPoints ...string) Config
func Connect(context.Context, Config) (*Session, error)
func OpenSingleHost(context.Context, Config, UUID) (*Session, error)
```

Configuration covers protocol 3/4/5 and autodiscovery, timeouts,
pools/shards/source ports, DNS/dial hooks, TLS and ticket cache, auth-provider
exchanges, LZ4/Snappy, coalescing/socket options, address
translation/filtering/client routes/cloud, startup identity and configuration
reporting, metadata scope/events/agreement, profiles/retry/speculation,
metrics/history/logger and timestamp generation. TLS uses Go crypto/tls,
validates host/CA by default, and never downgrades after a TLS failure. Each
auth exchange is per connection; providers are concurrency-safe. TLSForHost
accepts context, inherits TLS on a nil result, and cannot disable required
encryption. Context-taking hooks honor cancellation; synchronous callbacks are
prompt/nonblocking and cannot reenter blocking Close/Wait. The implementation
contract defines route selection, snapshots and duration defaults. Client
routes/cloud supply SNI-aware dial/TLS resolution and disable incompatible shard
source-port assumptions. Keepalive, orphan-stream budget, connection repair and
graceful close remain separate from user-request timeout.

Errors preserve `errors.Is`/`errors.As` and server error codes/details. Define
ErrClosed, ErrReleased, ErrNotFound, ErrNoKeyspace, ErrNoTable,
ErrPagingNoProgress, ErrPagingComplete, ErrPagingStateLimit, ErrNoRows and
ErrMultipleRows, plus ValidationError, UnsupportedSettingError, CodecError
(column/index/nested path and stage), ProtocolError (offset/stage),
SchemaChangedError, ExecutionError, PartialRefreshError and NoHostsError. The
existing RequestError server interface and concrete request error details stay
available through errors.As; ExecutionError unwraps the server/context error.
RequestError exposes `GetCode() int` and `GetMessage() string`, matching the
concrete native-protocol errors; wrappers must preserve both through errors.As.
Unrecognized future server errors retain code and opaque details. Invalid
lengths and metadata never panic or allocate based on unbounded advertised
counts.
