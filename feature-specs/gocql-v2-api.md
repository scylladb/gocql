# gocql v2 API contract

The reusable-query amendment dated 2026-11-02 supersedes the immutable binding
API and makes streaming decoders page-buffered by default. Performance
acceptance is incomplete; see [redesign status](../tests/driver-comparison/REDESIGN.md).

Status: implementation contract, distinct from the six reviewed Rust reference
specifications. Module: `github.com/scylladb/gocql/v2`; package: `gocql`. The
implementation uses the internal packages and public facade required by
plan.md section 5. Final release evidence is tracked in execution step 17;
the contract below does not substitute for that evidence. Integration tests
exercise typed behavior with focused testkit hooks. This does not retain the v1
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
    DecodeInto(*RowReader, *R) error
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
descriptor/mapping storage; row and scalar decoding must not repeat it. Built-in
compositions recursively bind child codecs and tuple/UDT field mappings during
Compile, including children of collections/vectors. Execution of those checked
plans reuses the bindings without recursive CheckType, Fields copies, name
lookup or nested plan construction per value/element. Value-dependent width,
state, text and framing validation still runs on every value. Standalone public
encode/decode calls validate their supplied descriptor on each call; arbitrary
custom codec work is accounted for separately.

RowPlan.DecodeInto is required for built-in and custom row plans. It fills a
non-nil caller-supplied destination using the checked metadata without retaining
the destination in the shared plan. It replaces the previous value, including
zeroing unmapped Go fields; custom plans must honor the same rule. Decode is
the value-returning convenience over the same decoding implementation. Built-in
Row field plans decode directly into destination fields: implementing DecodeInto
as Decode followed by assignment of an entire temporary R is not sufficient.
Compositions propagate destination fields to child plans where their semantics
permit; MapRows still invokes its explicitly supplied value-returning mapper,
whose work is accounted for separately. On an error, low-level DecodeInto may
have partially written the destination; the public iterator clears it before
returning false. Nil returns ValidationError. Plans must never retain reader or
destination pointers or capture per-call mutable state.

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
mapped to server order once. Named result plans likewise resolve names once and
decode in physical wire order, assigning each value through its compiled field
accessor. Fragmented cells remain immutable range views; sequential traversal
does not rescan earlier spans or allocate descriptor arrays per cell. Duplicate
marker names may reuse one input field;
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
scalar is checked when requested. Enclosing validation walks only immediate
child lengths/states, skipping child bodies without decoding descendants or
allocating an offset per element. A nested container validates its own layout
when that child is requested. Fragmented nested-body skips share a source
span-offset index, avoiding a walk through every skipped span at every ancestor;
they add at most logarithmic span-lookup work per jump. Ordinary nonoverlapping
sequential traversal remains linear in cells plus spans, and scalar validation
reads only the requested payload. Fixed-width vectors check the compiled aggregate width
against the body length without an element prepass; scalar semantics remain
checked during iteration. An explicit second Iterator is a new traversal.
An early-stopped iterator does not claim semantic validation of unvisited
scalar values or unvisited nested bodies.

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
reference. Their views expire under the same Next/NextInto/Close rule as typed rows; only
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
DynamicRows compiles column name lookup once per result shape. At/Get use a
single lazily built physical-cell offset index per current row for repeated
random access, without eagerly decoding values; there is no whole-row rescan
for each access. Its descriptors remain owned by that row/page and cannot alias
scratch reused by later rows. Dynamic aggregate variants must match the actual
outer type, including list versus set versus vector, and a UDT's identity.
Dynamic malformed scalars are
rejected with the same rules as typed codecs; no implicit universal Empty
fallback.

## Statements, preparation, queries and execution

Prepared statements and compiled plans remain immutable and shareable. Each
worker creates a reusable query; it permits one active execution and owns all
captured parameters through retries, pages, accepted writes and callbacks.

```go
func Prepare[P, R any](context.Context, *Session, Statement[P, R], PrepareOptions) (*Prepared[P, R], error)
func NewQuery[P, R any](*Prepared[P, R], Options) (*Query[P, R], error)
func NewUnpreparedQuery[P, R any](*Session, Statement[P, R], Columns, Options) (*Query[P, R], error)
func (q *Query[P, R]) Exec(context.Context, P) (Result, error)
func (q *Query[P, R]) Run(context.Context, P) (*Rows[R], error)
func (q *Query[P, R]) RunPage(context.Context, P, PageState) (*Page[R], error)
func (q *Query[P, R]) RunUnpaged(context.Context, P) (*Page[R], error)
func (q *Query[P, R]) Routing(P) (RoutingInfo, error)
func (q *Query[P, R]) BatchEntry(P) (BatchItem, error)
func (q *Query[P, R]) Close()
func (q *Query[P, R]) Wait(context.Context) error
```

The public immutable binding API and standalone binding-based execution
functions have been removed. Input is copied during execution admission,
including custom codec borrowed spans. Encoding records cell boundaries and
computes routing from the same owned bytes; encoders are not called again for
routing, retries, reprepare or paging. NewUnpreparedQuery requires explicit
parameter Columns. Query options are captured at construction, including owned
custom payload bytes; the selected profile is loaded per execution. Profile
replacement never changes an active execution's retry or continuation options.

Overlapping executions return ErrBusy. Cancellation may return while accepted
transport writes or callbacks still drain; the query remains busy and Wait joins
that work. Wait does not require Close. Cursor handles capture their execution
generation; stale handles cannot advance, close or modify another execution.
Close permanently stops admission and cancels active execution; it is idempotent.
Close a cursor on early termination before waiting for query reuse. Session
shutdown stops all queries before joining their transport/callback drains.

Prepare retains its existing schema-scoped dependency validation, per-host IDs,
LRU eviction independence, codec validation, conditional classification and
routing contracts. Statement.Kind is a caller classification; server-confirmed
conditional metadata takes precedence. Unknown statements disable speculation.
SERIAL/LOCAL_SERIAL execution is conditional. Routing uses captured parameters
and the currently selected metadata snapshot, preserving token distribution,
strict shard targeting and tablet feedback.

On reprepare, validate bind metadata against the encoded schema, not merely
marker count. Incompatible type/order/identity yields `SchemaChangedError`
before resend; the application explicitly prepares and binds again. Routing
schema compatibility includes partition-key index order, effective table
identity, selected partitioner and conditional classification, in addition to
parameter types/order/identity. A change to any of these requires a new explicit
prepare/query and fails the old query execution with SchemaChangedError before resend.
Compatible metadata may update per-host prepared IDs without re-encoding or
mutating the query's canonical routing inputs. Actual result metadata always
selects a compatible RowPlan before rows are exposed, including empty pages.
Reuse the immutable plan when the exact validated result entry is unchanged;
compile only on first use or a changed result shape. A new entry with the same
shape may reuse a plan after structural equality is established once. Plan reuse
is scoped to this handle's RowDecoder, never just CQL or a type fingerprint;
different application decoders cannot share an unchecked plan. Changed or
NO_METADATA responses still pass the generation/ID fences below. Pages retain
their selected plan independently of later cache replacement.
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
refresh and cannot be represented as proven global schema chronology. Step 03 specifies
bounded conflict reconciliation outside the historical design-review cycles:
coalesce at most 64 distinct pending keyspaces and query a known statement's
relevant keyspace through the typed schema path. Required internal validation
can run when optional schema metadata is disabled or filtered, but publication
continues to honor that mode/filter; it does not expose unrelated schema.
Only an unknown statement scope requires full internal enumeration under the
existing metadata limits. Queue exhaustion returns a ProtocolError with stage
"prepared schema reconciliation" through the logical operation. Earlier delivered
matching pages remain valid. Unscheduled/failed conflicts stay dirty, so later
calls can retry and scheduled failures are retried without broadening scope.
No unsafe cache-metadata opt-in bypasses these rules.

## Rows, pages, leases and conditional results

```go
type Rows[R any] struct { /* automatic pager */ }
func (r *Rows[R]) Next() bool
func (r *Rows[R]) NextInto(dst *R) bool
func (r *Rows[R]) Row() R
func (r *Rows[R]) Err() error
func (r *Rows[R]) Close() error
func (r *Rows[R]) Retain() (*PageLease, error)
func (r *Rows[R]) Clone() (R, error)
func (r *Rows[R]) Info() Result
func (r *Rows[R]) Columns() Columns

type Page[R any] struct { /* exactly one response page */ }
func (p *Page[R]) Next() bool
func (p *Page[R]) NextInto(dst *R) bool
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

Row() is valid after successful Next or NextInto; its views remain valid until
the **next** Next/NextInto call or Close, whichever comes first. Retain pins the
current physical page before advancing; its owner has stable identity independent
of pooled parsers.
All views from that page remain valid until that lease closes. Clone detaches
the current row using RowPlan.Clone. Copying a Go row struct does not detach it.
Access through a released view reports ErrReleased where a method returns error;
methods without error results panic with ErrReleased rather than reading
recycled storage. Len/type metadata remain inspectable. Do not concurrently use
a view while closing its last lease. Retained leases are independent of
Session.Close.

### Caller-supplied result destinations

This section decodes from received page storage. The separate
[streaming receive contract](gocql-v2-streaming-receive.md) specifies
incremental receive decoding into final caller storage through the separate
streaming-query API below; this NextInto contract remains the buffered path.

Implementation: DecodeInto is supplied by step 02 and NextInto by step 03;
final-tree release verification remains a later gate. Applications define the row representation
and may provide a reusable instance for decoding:

```go
type UserView struct {
    ID int32
    Name Text
    Payload Bytes
}

var user UserView
for rows.NextInto(&user) {
    consume(user) // Text and Bytes reference the leased receive storage
}
if err := rows.Err(); err != nil {
    return err
}
```

NextInto advances exactly one logical row, using the same framing, plan
selection, metadata validation and automatic/explicit paging rules as Next.
The compiled RowPlan.DecodeInto fills dst directly. The driver does not first
decode a complete temporary row, copy payloads, flatten split cells, or discover
fields through reflection. Text, Bytes and lazy container fields preserve the
original immutable receive/decompression backing and its page lease identity.
Fixed-width Go fields hold decoded values; compression/TLS transformations
retain their separately documented cost and ownership boundaries. Neither a Go
string nor an owned byte slice is implicitly materialized.

Every successful call replaces dst, including resetting fields not populated by
the declared decoder. On false, any non-nil dst is set to its zero value, whether
the cause is exhaustion, closure, cancellation, metadata/framing failure or a
codec error. Partially decoded rows are never published as successful. The first
error is terminal and available through Err; exhaustion alone is not an error.
On an active iterator, nil dst produces terminal ValidationError before consuming
a row or performing paging I/O. A terminal iterator stays terminal and preserves
its existing error. A failed call does not promise rollback of cursor progress.

Next and NextInto may be interleaved; every call advances, so NextInto after Next
reads the following row. After successful NextInto, Row returns the current
contents of *dst and Clone detaches those contents with the selected RowPlan.
The iterator may retain that destination pointer only until the next advance or
Close; no shared plan, prefetch worker or other background task may retain or
write it. Caller changes to the current destination are reflected by Row/Clone.
Advancing with a different destination or closing the iterator must not modify
the earlier destination, although its borrowed views still expire under the
advance/Close rule. Iteration and access/mutation of its current destination
are not concurrent operations; independent iterators may share compiled plans,
but require separate destination storage.

Retain after successful NextInto pins the current page just as after Next; it
does not pin the Go destination or preserve fields overwritten on reuse. To keep
a borrowed row while reusing dst, copy its view descriptors into another value
and retain the page before advancing, then close that lease after use. Use Clone
instead for fully detached data. Simply storing dst or copying its Go struct
without a page lease does not extend buffer lifetime.

### Paging and conditional results

Rows owns one current page and at most the configured bounded prefetch. Next
consumes empty pages with continuations, propagates the first error terminally,
and never loses an error behind partial results. Context governs the entire
pager lifetime; a separate configured per-page timeout may shorten it. Repeated
continuation states are compared against every previously returned continuation
in that automatic pager, including empty tokens: A → B → A therefore returns
ErrPagingNoProgress before another fetch of A. Token equality checks full bytes;
a hash alone is insufficient. Use a hash-indexed history with full-byte collision
checks, giving expected O(token bytes) insertion/lookup rather than scanning all
prior states on every page. Retain one immutable owned token representation for
history, next-page sending and PageState; internal use never calls the public
copying Bytes() accessor. Count the history entry/index storage under the count
budget and token bytes under the byte budget. Explicit Bytes() calls still copy.
Retained history is bounded by Config.Limits
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

## Streaming receive into final caller storage

Queries are page-buffered by default and use shared multiplexed connections.
Memory scales with concurrent response pages, bounded by frame/decompression
limits. Streaming codecs still write seeded caller destinations. Set
StreamOptions.Incremental for explicit bounded receive; the exclusive-connection,
raw receive, transform, read-ahead and working-storage clauses below describe
that explicit mode. StreamLimits receive budgets apply to incremental mode.
All framing, malformed-value, cancellation and decoder checks remain mandatory
in both modes. Incremental native read-ahead is bounded by its configured limit
and 4 KiB. Shared connections retain an 8 KiB reader, transferred without losing
unread bytes at a checked frame/unit boundary and restored before shared reads.


Status: normative implementation contract, settled during execution preparation on
2026-10-29. The implementation is present; final acceptance is tracked separately
in execution step 17. This addition
was not part of the preceding ten design review cycles. The detailed framing,
ownership and acceptance rules are in
[gocql-v2-streaming-receive.md](gocql-v2-streaming-receive.md).

```go
// B is the prepared buffered result type; R is the streaming destination type.
func NewStreamQuery[P, B, R any](*Prepared[P, B], StreamRowDecoder[R], StreamOptions) (*StreamQuery[P, R], error)
func (q *StreamQuery[P, R]) Run(context.Context, P) (*RowStream[R], error)
func (q *StreamQuery[P, R]) Close()
func (q *StreamQuery[P, R]) Wait(context.Context) error
type RowStream[R any] struct { /* exclusive pull receiver, automatic paging */ }
func (s *RowStream[R]) NextInto(*R) bool
func (s *RowStream[R]) Err() error
func (s *RowStream[R]) Close() error
func (s *RowStream[R]) Columns() Columns
func (s *RowStream[R]) Info() StreamInfo

type StreamOptions struct {
    Incremental bool
    Options Options
    Limits StreamLimits
}
type StreamLimits struct {
    MaxMetadataBytes int64
    ReadAheadBytes int
    DiscardBytes int
    MaxTransformBytes int64
}
type StreamInfo struct {
    Result Result
    DeliveredRows int64
    Pages int64
    OutputStarted bool
}

type StreamRowDecoder[R any] interface {
    Compile(Columns) (StreamRowPlan[R], error)
}
type StreamRowPlan[R any] interface {
    Reset(*R)
    DecodeInto(*StreamRowReader, *R) error
}
type StreamDecoder[T any] interface {
    Compile(DataType) (StreamValuePlan[T], error)
}
// Optional declaration capability; no server DataType is needed.
type StreamResetter[T any] interface { Reset(*T) }
type StreamValuePlan[T any] interface {
    Reset(*T)
    DecodeInto(*StreamValueReader, *T) error
}
type StreamOutputField[R any] struct { /* immutable checked field declaration */ }
func StreamColumn[R, V any](string, StreamDecoder[V], func(*R) *V) StreamOutputField[R]
func StreamColumnAt[R, V any](int, StreamDecoder[V], func(*R) *V) StreamOutputField[R]
func StreamRow[R any](reset func(*R), fields ...StreamOutputField[R]) StreamRowDecoder[R]
func StreamOneColumn[T any](StreamDecoder[T]) StreamRowDecoder[T]

// Readers are scoped mutable limited readers, never retained-byte views.
type StreamRowReader struct { /* checked count and column cursor */ }
func ReadStreamColumn[T any](*StreamRowReader, StreamValuePlan[T], *T) error
func (r *StreamRowReader) Context() context.Context
func (r *StreamRowReader) SkipColumn() error
func (r *StreamRowReader) RequireEnd() error

type StreamValueReader struct { /* checked type/state and exact body boundary */ }
func (r *StreamValueReader) Context() context.Context
func (r *StreamValueReader) Type() DataType
func (r *StreamValueReader) State() CellState
func (r *StreamValueReader) Remaining() int64
func (r *StreamValueReader) Read([]byte) (int, error)
func (r *StreamValueReader) Uint8() (uint8, error)
func (r *StreamValueReader) Uint16() (uint16, error)
func (r *StreamValueReader) Uint32() (uint32, error)
func (r *StreamValueReader) Uint64() (uint64, error)
func (r *StreamValueReader) Child(DataType, func(*StreamValueReader) error) error
func (r *StreamValueReader) Skip() error
func (r *StreamValueReader) RequireEnd() error

var StreamBool StreamDecoder[bool]
var StreamInt8 StreamDecoder[int8]
var StreamInt16 StreamDecoder[int16]
var StreamInt32 StreamDecoder[int32]
var StreamInt64 StreamDecoder[int64]
var StreamFloat32 StreamDecoder[float32]
var StreamFloat64 StreamDecoder[float64]
var StreamCounter StreamDecoder[Counter]
var StreamDate StreamDecoder[Date]
var StreamTime StreamDecoder[TimeOfDay]
var StreamTimestamp StreamDecoder[Timestamp]
var StreamDuration StreamDecoder[Duration]
var StreamUUID StreamDecoder[UUID]
var StreamTimeUUID StreamDecoder[TimeUUID]
var StreamInet StreamDecoder[netip.Addr]
var StreamText StreamDecoder[[]byte] // text/varchar; incremental UTF-8 check
var StreamASCII StreamDecoder[[]byte]
var StreamBlob StreamDecoder[[]byte]
var StreamVarint StreamDecoder[[]byte] // exact signed two's-complement bytes
// Decimal's unscaled bytes have the same caller-capacity rule.
type StreamDecimalValue struct { Scale int32; Unscaled []byte }
var StreamDecimal StreamDecoder[StreamDecimalValue]
func StreamNullable[T any](StreamDecoder[T]) StreamDecoder[Nullable[T]]
func StreamEmpty[T any](StreamDecoder[T]) StreamDecoder[EmptyValueOf[T]]
func StreamList[T any](StreamDecoder[T]) StreamDecoder[[]T]
func StreamSet[T any](StreamDecoder[T]) StreamDecoder[[]T]
type StreamMapEntry[K, V any] struct { Key K; Value V }
func StreamMap[K, V any](StreamDecoder[K], StreamDecoder[V]) StreamDecoder[[]StreamMapEntry[K, V]]
func StreamVector[T any](StreamDecoder[T]) StreamDecoder[[]T]
func StreamTuple[R any](StreamRowDecoder[R]) StreamDecoder[R]
func StreamUDT[R any](UDTOptions, StreamRowDecoder[R]) StreamDecoder[R]
// Explicit final encoded storage for dynamic/custom representations.
type StreamEncodedValue struct { Type DataType; State CellState; Bytes []byte }
var StreamEncoded StreamDecoder[StreamEncodedValue]

type StreamCapabilityError struct { Protocol int; Compression Compression; Reason string }
type StreamCapacityError struct { Path string; Required, Available int64 }
func (e *StreamCapabilityError) Error() string
func (e *StreamCapacityError) Error() string
```

StreamOutputField[R] is opaque, as for OutputField[R]. Unmapped physical columns
are skipped with recursive structural/type validation; no ignore field is
needed in StreamRow.
Compile validates actual metadata recursively, including children of null and
empty values, and returns immutable plans safe for independent cursors. Plans
capture neither readers nor destinations. Scalar rules match the buffered
codecs, including exact widths, UTF-8/ASCII, numeric ranges, null/unset and
logical Empty. StreamEmpty accepts exactly the types accepted by EmptyCodec.
No adapter may run an arbitrary buffered Decoder by accumulating its input.
StreamEncoded explicitly writes the entire encoded body into caller storage,
then exposes that caller-owned representation; nested framing/type validation
runs incrementally and does not allocate a complete cell in driver storage.

A streaming execution reuses a prepared or unprepared binding's encoded input,
routing, host IDs, metadata revisions and Close/Wait admission. B and R may
differ. The original Statement.Rows remains relevant to ordinary execution and
Prepare validation, but is never invoked to decode a streaming response. Actual
streaming response metadata selects this call's StreamRowPlan independently of
the buffered plan; cache reuse is scoped to this decoder and exact metadata.
NoRows still cannot be used to describe nonzero physical result columns.
Applications can use an ordinary typed or DynamicRows decoder at preparation
and a separate StreamRow decoder for streaming storage.

Streaming query execution installs the stream receiver before sending and returns after checked
response headers/metadata and bounded integrity staging, without waiting for the
complete page. It does not consume row payload waiting for an application
destination. NextInto reads one row into that destination and performs automatic
paging on demand, including empty-page progress checks. There is no Next, Row,
Clone or Retain: output already belongs to the caller and no PageLease is needed.
Columns returns current-page checked metadata. Info counters are cumulative;
its Result warnings, trace IDs and custom payload describe only the latest
page/attempt, bounded by MaxMetadataBytes, rather than accumulating per-page
payloads forever. OutputStarted becomes true before the first decoder operation
that can modify a destination, including Reset; it never becomes false again.
DeliveredRows increments only after complete checked rows. Pages counts checked
response pages, including empty pages. Errors retain this information through
Info even when a streaming query has already returned a cursor. An error before cursor
construction returns normally through the query Run method's error result.

Reset is called on a non-nil destination before each active NextInto attempt;
terminal/exhausted calls return false without touching it. Built-in value Reset
clears logical values while preserving byte/element slice capacity and nested
storage. StreamRow requires a non-nil application reset callback for unmapped
Go state, then resets mapped fields through child plans. The callback must
preserve mapped storage. A custom plan has the same explicit responsibility;
there is no promise to zero an arbitrary R while preserving its unknown storage.
Step 02 discovered and corrected a declaration-capability gap outside the
historical design reviews: an application UDT field absent from database
metadata has no DataType with which to compile its value plan. StreamResetter[T]
is an optional capability on decoder declarations (including row declarations),
providing metadata-independent Reset while preserving caller-seeded byte and
nested storage. Built-in scalar, text/blob/numeric payload, nullable/empty,
collection/map/vector, tuple/UDT, encoded-value and row compositions provide this
reset behavior recursively. For compositions with custom descendants, the
required declaration reset capability must be available for every absent
application mapping; it cannot be inferred from the custom Go representation.

With UDTOptions.AllowMissingFields, compilation permits an application field
missing from database metadata only when its decoder declaration supports
StreamResetter for that destination. Missing custom-field capability produces a
clear ValidationError during Compile, before row decoding; implementing
StreamResetter allows that custom field to participate. The declaration Reset
clears its logical value while preserving seeded storage, just like built-in
reset. This also applies recursively to missing fields inside compositions.
Mapped custom fields continue to use their successfully compiled
StreamValuePlan.Reset and need not implement declaration StreamResetter.
Custom row declarations used as tuple/UDT compositions follow the same rule
when metadata-independent reset is required. No synthetic DataType, reflection,
whole-row temporary or zero-assignment of arbitrary V supplies a substitute.
StreamRow's application callback retains responsibility for unmapped Go state
and must preserve mapped storage; this optional capability does not shift that
responsibility or relax the complete streaming type-support requirements.

Unlike buffered NextInto, false does not clear the whole destination. After a
failed attempt its contents may be partial and are invalid. Nil on an active
cursor is terminal ValidationError before row consumption. A terminal cursor
preserves its first error; exhaustion alone has nil Err.

Text/blob output reslices the supplied array without growing it. A declared
length beyond capacity yields StreamCapacityError before any body bytes of
that value are written. A null nullable value resets its logical Value and Valid
while preserving Value's capacity; an empty present value has Valid true and
zero length. For slices of nested elements, count/dimension must fit cap(dst)
before element bodies are read; decoding reslices the existing backing and
resets each element immediately before use, preserving its seeded child storage.
Map entries similarly preserve key/value storage. The caller must preseed each
nested destination it wants to reuse, including slots beyond its current length.
Tuple and UDT builders reuse the checked positional/name/evolution rules and
write into application fields; no driver-owned complete-row temporary is used.
Reader Child owns nested length boundaries, and RequireEnd rejects trailing
bytes. Read has io.Reader semantics bounded by Remaining and may write directly
to final caller ranges; custom codecs must honor the same no-gather/capacity and
lifetime rules. Custom callbacks/plans must return promptly on cancellation
using the reader context and must not start asynchronous destination accesses;
Close waits for active custom calls rather than returning with a writable
destination. Skip and SkipColumn validate discarded values with bounded
scratch, including nested structure, rather than trusting lengths alone.

The mode disables inherited prefetch and speculation regardless of profile
defaults. Explicit nonzero Options.Prefetch fails validation before submission;
zero or inherited Prefetch is accepted. Retry and reprepare are permitted only
before OutputStarted and any delivered row, under the ordinary safe-request
rules. A row decoder/reset error or later I/O failure is terminal, with no
transparent replay. Query Close/Wait covers the cursor through final transport and callback drain;
streamed output remains caller-owned.

StreamLimits zero fields select defaults: 1 MiB simultaneously retained
metadata/control bytes,
32 KiB connection read-ahead, 32 KiB discard scratch, and 262,158 transform bytes
(two maximum 131,071-byte v5 units plus 16 bytes of framing). Negative/overflowing
values fail validation. Positive values are upper bounds, never a reason to
grow with a response. Limits are copied at admission; lower ProtocolLimits still
apply to total frame/decompressed work, nesting, counts and paging history.
MaxMetadataBytes bounds simultaneously retained current-page metadata/control,
including old data still live while replacement metadata is allocated or
installed. Declared lengths accumulate within an envelope and are checked before
allocation; accounting resets only after the corresponding storage is released.
It is not a lifetime/page-total limit. Paging tokens and retained continuation
history use their separate ProtocolLimits budgets rather than this counter.
MaxTransformBytes covers simultaneous compressed input and plaintext output plus
unit framing; oversized units fail before allocation. No list of consumed units
is retained. Raw v3/v4 does not allocate transform units. Metadata compilation
and paging control/history have their own explicit limits; caller output is
accounted for separately.

Initial supported modes are native v3/v4 without frame compression, with or
without TLS, and native v5 raw or LZ4 segments, with or without TLS. Validate a
candidate connection's negotiated mode and the decoder's metadata before request
submission when known; otherwise select only eligible candidates and validate
actual response metadata before output. Unsupported protocol/compression or
custom mapping combinations produce StreamCapabilityError, never buffered
fallback. v3/v4 Snappy/LZ4 frame compression is rejected before sending the query.
A pool containing only incompatible connections fails promptly; the driver does
not silently change negotiated compression or create unlimited connections.

The cursor reserves one connection exclusively from the configured pool budget.
A connection is eligible only after its shared writer/read loop is quiescent,
all pending/in-flight/orphan/control operations are absent, and read ownership
has transferred atomically to the stream receiver. Pool acquisition honors
context, routing and strict host/shard targets. Idle cursors retain that bounded
capacity; other work waits or uses other pool connections. Control/event frames
encountered during streaming use bounded nonblocking dispatch and cannot steal
result bytes. No background operation writes caller storage between NextInto
calls. Close is idempotent, may interrupt NextInto, closes an incomplete
connection to unblock reads, and waits for destination access and input writers
to stop before returning. Context cancellation provides the same boundary when
the active NextInto returns. Concurrent NextInto calls are invalid and rejected
before the second call accesses storage. Exact response completion permits pool
reuse only after the shared reader resumes at a proven protocol boundary; early
close, capacity failure, malformed framing, timeout or cancellation discards the
reserved connection and schedules ordinary bounded pool repair. Session.Close
also interrupts active streams; already written caller output remains valid.

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
type BatchItem struct { /* immutable owned entry captured by query.BatchEntry */ }
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
affects new admissions only, never active retries or later pages. Admission loads
one immutable profile version, validates only per-call overrides and shares the
resolved options across attempts/pages. It does not revalidate or deep-copy the
whole profile on each attempt. CustomPayload descriptor copying occurs once per
logical admission (once for a batch), with all member drains still covering it.
Defaults:
LOCAL_QUORUM, LOCAL_SERIAL, 30-second request timeout, 5,000-row pages, one-page
prefetch, no speculation, and the safe retry policy. Caller context always
bounds request work, including preparation, metadata refresh, agreement,
queueing and cleanup waits. Cleanup continues independently where required to
release an orphan stream; returning a context error never authorizes reuse of
still-borrowed input before Wait. Query construction copies custom payload bytes and checks source views while
capturing options. Public batch execution also owns its options payload; batch
parameters are owned when query.BatchEntry is appended. There is no public
binding drain obligation. Returned Result
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
panic. The built-in policy reads private immutable host/replica indices and
produces its first eligible replica without enumerating/copying the full host
list or constructing all fallback targets. It advances fallback lazily only
when needed. Public defensive metadata accessors are inspection APIs, not the
built-in routing path. A plan captures a placement/topology snapshot; pool
acquisition still rechecks current availability and target strictness. Custom
policies retain their public Plan seam and account for their own work.

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
losers, for each configured observer. When no observer is configured, skip event
construction, batch-statement arrays and diagnostic formatting. Required policy
Observe feedback, metrics and Result accounting still run. Latency percentile
snapshots are maintained outside individual request completion; disabled
speculation does not fetch a MetricsSnapshot or create a scheduler/timer. Row
cardinality is separate from successful server execution. Trace
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
updates. Feedback must not block successful execution. Equivalent feedback with
the same
validated identity/epochs, range, version and ordered replicas/shards is a no-op;
version equality alone is insufficient. Publish changed placement with
structural sharing of unaffected tables/ranges instead of rebuilding complete
cluster/schema snapshots for every response. Queued feedback owns only its
bounded decoded control data and does not pin the response page. On queue
saturation,
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
keeps its independent output owner until the application's next Next/NextInto or
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


### Transport setup details settled during step 04

A Unix maintenance target is one `ContactPoints` entry `unix:/absolute/path`;
a bare absolute path is accepted as a convenience. It requires explicit
`Protocol = 3` and `Metadata.InitialDiscovery = false`. Mixed Unix/TCP contacts,
TLS or TLSForHost, and explicit LocalAddress on this transport fail with
UnsupportedSettingError. Default shard settings do not target a Unix socket.
Dialer receives Network `unix` and the exact path as Address; an unknown
bootstrap Host.Address is invalid rather than a fabricated TCP endpoint. A
socket represents only its reconciled local host, so remote strict targets fail.
For example:

```go
config := DefaultConfig("unix:/run/scylla/cql.m")
config.Protocol = 3
config.Metadata.InitialDiscovery = false
session, err := Connect(ctx, config)
```

The context-aware Dialer, DialHost and TLSForHost contracts above apply to all
new connections. DefaultDialer implements the raw target contract; DialerFunc
adapts a function. TCP endpoints and source ranges are selected before hooks;
TLSForHost nil inherits TLS, and cannot downgrade required TLS. TLS containers,
trust-pool containers and certificate byte arrays are snapshotted. Private keys,
callbacks, parsed trust-certificate objects and session caches/providers retain
their immutable/shared concurrency obligations. TLSForHost can be called again
before publication to verify that its SNI/TLS target is still current, always
outside driver locks. A superseded endpoint, route or sharder opening is closed.

The per-connection setup deadline starts before its target resolution and is
not restarted for translation, raw dialing, TLS, authentication, initial USE,
REGISTER, or final target validation. DNS's local timeout only shortens lookup.
Each resolved contact/opening owns its deadline, even when aliases or ports
share an IP. A failed physical opening can be retried under a new attempt budget
and reconnect policy; it never extends the spent attempt or caller deadline.

This clarification records implemented transport seams; public streaming queries and
its execution/paging composition remain later step 07 work. It does not extend
the historical design review results or establish full release verification.
