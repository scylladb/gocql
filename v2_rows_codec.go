package gocql

import (
	"fmt"
	"sync"

	"github.com/scylladb/gocql/v2/internal/wire"
)

// Unit is the value type for a statement without parameters or result columns.
type Unit struct{}

// Pair composes two independently typed parameter or row values.
type Pair[A, B any] struct {
	First  A
	Second B
}

// ParamEncoder compiles a reusable immutable encoder against server metadata.
type ParamEncoder[P any] interface {
	Compile(Columns) (ParamPlan[P], error)
}

type ParamPlan[P any] interface {
	Encode(*ParamWriter, P) error
}

// RowDecoder compiles a reusable immutable decoder against actual result metadata.
type RowDecoder[R any] interface {
	Compile(Columns) (RowPlan[R], error)
	Clone(R) R
}

type RowPlan[R any] interface {
	Decode(*RowReader) (R, error)
	Clone(R) R
}

// ParamWriter writes parameters in server order. It is confined to one binding.
type ParamWriter struct {
	builder     *wire.Builder
	columns     Columns
	index       int
	allowUnset  bool
	ownBorrowed bool
}

func newParamWriter(builder *wire.Builder, columns Columns, allowUnset bool) *ParamWriter {
	return &ParamWriter{builder: builder, columns: columns, allowUnset: allowUnset}
}

func finishParamWriter(w *ParamWriter) error {
	if w.index != w.columns.Len() {
		return fmt.Errorf("gocql: encoded %d parameters, expected %d", w.index, w.columns.Len())
	}
	return nil
}

// WriteParam encodes one typed parameter with transactional cell framing.
func WriteParam[T any](w *ParamWriter, encoder Encoder[T], value T) error {
	if w == nil || w.builder == nil || encoder == nil {
		return fmt.Errorf("gocql: nil parameter writer or encoder")
	}
	column, ok := w.columns.At(w.index)
	if !ok {
		return fmt.Errorf("gocql: too many parameters: index %d", w.index)
	}
	cell := newValueWriter(w.builder, column.Type, w.allowUnset)
	cell.ownBorrowed = w.ownBorrowed
	if err := encoder.Encode(cell, value); err != nil {
		abortValueWriter(cell)
		return fmt.Errorf("gocql: parameter %d (%s): %w", w.index, column.Name, err)
	}
	if err := finishValueWriter(cell); err != nil {
		abortValueWriter(cell)
		return fmt.Errorf("gocql: parameter %d (%s): %w", w.index, column.Name, err)
	}
	w.index++
	return nil
}

// RowReader consumes physical columns without tuple expansion or reflection.
type RowReader struct {
	columns      Columns
	cursor       *wire.Cursor
	lease        *wire.Lease
	index        int
	allowMissing bool
}

func newRowReader(columns Columns, body wire.Chain, lease *wire.Lease) *RowReader {
	return &RowReader{columns: columns, cursor: wire.NewCursor(body), lease: lease}
}

func finishRowReader(r *RowReader) error {
	if r.index != r.columns.Len() {
		return fmt.Errorf("gocql: decoded %d columns, expected %d", r.index, r.columns.Len())
	}
	return nil
}

func (r *RowReader) nextValue() (ValueReader, error) {
	if r == nil || r.cursor == nil {
		return ValueReader{}, fmt.Errorf("gocql: nil row reader")
	}
	column, ok := r.columns.At(r.index)
	if !ok {
		return ValueReader{}, fmt.Errorf("gocql: too many columns: index %d", r.index)
	}
	if r.allowMissing && r.cursor.Remaining() == 0 {
		r.index++
		return newValueReader(column.Type, Null, wire.Chain{}, r.lease), nil
	}
	v, err := readValue(r.cursor, column.Type, r.lease, false)
	if err != nil {
		return ValueReader{}, fmt.Errorf("gocql: column %d (%s): %w", r.index, column.Name, err)
	}
	r.index++
	return v, nil
}

func ReadColumn[T any](r *RowReader, decoder Decoder[T]) (T, error) {
	var zero T
	if decoder == nil {
		return zero, fmt.Errorf("gocql: nil column decoder")
	}
	value, err := r.nextValue()
	if err != nil {
		return zero, err
	}
	result, err := decoder.Decode(value)
	if err != nil {
		column, _ := r.columns.At(r.index - 1)
		return zero, fmt.Errorf("gocql: column %d (%s): %w", r.index-1, column.Name, err)
	}
	return result, nil
}

func (r *RowReader) SkipColumn() error {
	_, err := r.nextValue()
	return err
}

type fieldLocation struct {
	name       string
	index      int
	positional bool
}

// InputField is constructed with Param or ParamAt; it never boxes a field value.
type InputField[P any] interface {
	location() fieldLocation
	compile(DataType) (InputField[P], error)
	encode(*ParamWriter, P) error
}

type inputField[P, V any] struct {
	loc   fieldLocation
	codec Encoder[V]
	get   func(P) V
}

func Param[P, V any](name string, codec Encoder[V], get func(P) V) InputField[P] {
	return inputField[P, V]{loc: fieldLocation{name: name}, codec: codec, get: get}
}

func ParamAt[P, V any](index int, codec Encoder[V], get func(P) V) InputField[P] {
	return inputField[P, V]{loc: fieldLocation{index: index, positional: true}, codec: codec, get: get}
}

func (f inputField[P, V]) location() fieldLocation { return f.loc }
func (f inputField[P, V]) compile(typ DataType) (InputField[P], error) {
	if f.codec == nil || f.get == nil {
		return nil, fmt.Errorf("gocql: nil input codec or accessor")
	}
	codec, err := compileValueEncoder(f.codec, typ)
	if err != nil {
		return nil, err
	}
	f.codec = codec
	return f, nil
}
func (f inputField[P, V]) encode(w *ParamWriter, value P) error {
	return WriteParam(w, f.codec, f.get(value))
}

type paramsCodec[P any] struct{ fields []InputField[P] }
type paramsPlan[P any] struct{ ordered []InputField[P] }

func Params[P any](fields ...InputField[P]) ParamEncoder[P] {
	return paramsCodec[P]{fields: append([]InputField[P](nil), fields...)}
}

// mapFields resolves names/positions once. Parameters may repeat one named input;
// result columns require an explicit positional accessor for duplicate aliases.
func mapFields(columns Columns, locations []fieldLocation, repeatedNames bool) ([]int, error) {
	ordered := make([]int, columns.Len())
	for i := range ordered {
		ordered[i] = -1
	}
	for i, location := range locations {
		matches := 0
		if !location.positional && location.name == "" {
			return nil, fmt.Errorf("gocql: empty field name")
		}
		for j := 0; j < columns.Len(); j++ {
			column, _ := columns.At(j)
			match := location.positional && location.index == j || !location.positional && location.name == column.Name
			if !match {
				continue
			}
			if ordered[j] != -1 {
				return nil, fmt.Errorf("gocql: multiple accessors for column %d (%s)", j, column.Name)
			}
			ordered[j] = i
			matches++
		}
		if matches == 0 {
			return nil, fmt.Errorf("gocql: field %q at index %d has no database column", location.name, location.index)
		}
		if matches > 1 && !repeatedNames {
			return nil, fmt.Errorf("gocql: duplicate result name %q requires positional mapping", location.name)
		}
	}
	for i, field := range ordered {
		if field < 0 {
			column, _ := columns.At(i)
			return nil, fmt.Errorf("gocql: no accessor for column %d (%s)", i, column.Name)
		}
	}
	return ordered, nil
}

func (c paramsCodec[P]) Compile(columns Columns) (ParamPlan[P], error) {
	locations := make([]fieldLocation, len(c.fields))
	for i, field := range c.fields {
		if field == nil {
			return nil, fmt.Errorf("gocql: nil parameter field at index %d", i)
		}
		locations[i] = field.location()
	}
	mapping, err := mapFields(columns, locations, true)
	if err != nil {
		return nil, err
	}
	plan := paramsPlan[P]{ordered: make([]InputField[P], len(mapping))}
	for index, field := range mapping {
		column, _ := columns.At(index)
		compiled, err := c.fields[field].compile(column.Type)
		if err != nil {
			return nil, fmt.Errorf("gocql: parameter %d (%s): %w", index, column.Name, err)
		}
		plan.ordered[index] = compiled
	}
	return plan, nil
}

func (p paramsPlan[P]) Encode(w *ParamWriter, value P) error {
	mark := w.builder.Mark()
	start := w.index
	for _, field := range p.ordered {
		if err := field.encode(w, value); err != nil {
			w.builder.Rollback(mark)
			w.index = start
			return err
		}
	}
	return nil
}

// OutputField contains a typed field address accessor used for decoding and Clone.
type OutputField[R any] interface {
	location() fieldLocation
	compile(DataType) (OutputField[R], error)
	decode(*RowReader, *R) error
	clone(dst, source *R)
}

type outputField[R, V any] struct {
	loc   fieldLocation
	codec Decoder[V]
	field func(*R) *V
}

func Column[R, V any](name string, codec Decoder[V], field func(*R) *V) OutputField[R] {
	return outputField[R, V]{loc: fieldLocation{name: name}, codec: codec, field: field}
}
func ColumnAt[R, V any](index int, codec Decoder[V], field func(*R) *V) OutputField[R] {
	return outputField[R, V]{loc: fieldLocation{index: index, positional: true}, codec: codec, field: field}
}
func (f outputField[R, V]) location() fieldLocation { return f.loc }
func (f outputField[R, V]) compile(typ DataType) (OutputField[R], error) {
	if f.codec == nil || f.field == nil {
		return nil, fmt.Errorf("gocql: nil output codec or accessor")
	}
	codec, err := compileValueDecoder(f.codec, typ)
	if err != nil {
		return nil, err
	}
	f.codec = codec
	return f, nil
}
func (f outputField[R, V]) decode(reader *RowReader, row *R) error {
	field := f.field(row)
	if field == nil {
		return fmt.Errorf("gocql: output field accessor returned nil")
	}
	value, err := ReadColumn(reader, f.codec)
	if err != nil {
		return err
	}
	*field = value
	return nil
}
func (f outputField[R, V]) clone(dst, source *R) {
	*f.field(dst) = f.codec.Clone(*f.field(source))
}

type ignoredField[R any] struct{ loc fieldLocation }

func IgnoreColumn[R any](name string) OutputField[R] {
	return ignoredField[R]{loc: fieldLocation{name: name}}
}
func (f ignoredField[R]) location() fieldLocation                  { return f.loc }
func (f ignoredField[R]) compile(DataType) (OutputField[R], error) { return f, nil }
func (f ignoredField[R]) decode(r *RowReader, _ *R) error          { return r.SkipColumn() }
func (f ignoredField[R]) clone(dst, source *R)                     {}

type rowCodec[R any] struct{ fields []OutputField[R] }
type rowPlan[R any] struct {
	ordered []OutputField[R]
	// Scratch addresses are confined to Decode; accessors must not retain them.
	// The returned R is copied before scratch is cleared and recycled.
	scratch *sync.Pool
}

func Row[R any](fields ...OutputField[R]) RowDecoder[R] {
	return rowCodec[R]{fields: append([]OutputField[R](nil), fields...)}
}
func (c rowCodec[R]) Clone(source R) R {
	dst := source
	for _, field := range c.fields {
		if field == nil {
			panic("gocql: nil field in row clone")
		}
		field.clone(&dst, &source)
	}
	return dst
}
func (c rowCodec[R]) Compile(columns Columns) (RowPlan[R], error) {
	locations := make([]fieldLocation, len(c.fields))
	for i, field := range c.fields {
		if field == nil {
			return nil, fmt.Errorf("gocql: nil result field at index %d", i)
		}
		locations[i] = field.location()
	}
	mapping, err := mapFields(columns, locations, false)
	if err != nil {
		return nil, err
	}
	plan := rowPlan[R]{ordered: make([]OutputField[R], len(mapping)), scratch: &sync.Pool{New: func() any { return new(R) }}}
	for index, field := range mapping {
		column, _ := columns.At(index)
		compiled, err := c.fields[field].compile(column.Type)
		if err != nil {
			return nil, fmt.Errorf("gocql: result column %d (%s): %w", index, column.Name, err)
		}
		plan.ordered[index] = compiled
	}
	return plan, nil
}
func (p rowPlan[R]) Decode(reader *RowReader) (R, error) {
	row := p.scratch.Get().(*R)
	var zero R
	for _, field := range p.ordered {
		if err := field.decode(reader, row); err != nil {
			*row = zero
			p.scratch.Put(row)
			return zero, err
		}
	}
	result := *row
	*row = zero
	p.scratch.Put(row)
	return result, nil
}
func (p rowPlan[R]) Clone(source R) R {
	dst := source
	for _, field := range p.ordered {
		field.clone(&dst, &source)
	}
	return dst
}

func OneParam[T any](encoder Encoder[T]) ParamEncoder[T] {
	return Params(ParamAt(0, encoder, func(value T) T { return value }))
}
func OneColumn[T any](decoder Decoder[T]) RowDecoder[T] {
	return singleColumnRows[T]{decoder: decoder}
}

type singleColumnRows[T any] struct{ decoder Decoder[T] }
type singleColumnPlan[T any] struct{ decoder Decoder[T] }

func (c singleColumnRows[T]) Compile(columns Columns) (RowPlan[T], error) {
	if columns.Len() != 1 {
		return nil, fmt.Errorf("gocql: single column decoder expected 1 column, got %d", columns.Len())
	}
	column, _ := columns.At(0)
	decoder, err := compileValueDecoder(c.decoder, column.Type)
	if err != nil {
		return nil, err
	}
	return singleColumnPlan[T]{decoder: decoder}, nil
}
func (c singleColumnRows[T]) Clone(value T) T { return c.decoder.Clone(value) }
func (p singleColumnPlan[T]) Decode(reader *RowReader) (T, error) {
	return ReadColumn(reader, p.decoder)
}
func (p singleColumnPlan[T]) Clone(value T) T { return p.decoder.Clone(value) }

var NoParams ParamEncoder[Unit] = Params[Unit]()
var NoRows RowDecoder[Unit] = Row[Unit]()

type mappedParams[A, B any] struct {
	codec    ParamEncoder[B]
	mapValue func(A) B
}
type mappedParamPlan[A, B any] struct {
	plan     ParamPlan[B]
	mapValue func(A) B
}

func MapParams[A, B any](codec ParamEncoder[B], mapValue func(A) B) ParamEncoder[A] {
	return mappedParams[A, B]{codec: codec, mapValue: mapValue}
}
func (c mappedParams[A, B]) Compile(columns Columns) (ParamPlan[A], error) {
	if c.codec == nil || c.mapValue == nil {
		return nil, fmt.Errorf("gocql: nil parameter mapping")
	}
	plan, err := c.codec.Compile(columns)
	if err != nil {
		return nil, err
	}
	return mappedParamPlan[A, B]{plan: plan, mapValue: c.mapValue}, nil
}
func (p mappedParamPlan[A, B]) Encode(w *ParamWriter, value A) error {
	return p.plan.Encode(w, p.mapValue(value))
}

type mappedRows[A, B any] struct {
	codec      RowDecoder[A]
	mapValue   func(A) B
	cloneValue func(B) B
}
type mappedRowPlan[A, B any] struct {
	plan       RowPlan[A]
	mapValue   func(A) B
	cloneValue func(B) B
}

func MapRows[A, B any](codec RowDecoder[A], mapValue func(A) B, cloneValue func(B) B) RowDecoder[B] {
	return mappedRows[A, B]{codec: codec, mapValue: mapValue, cloneValue: cloneValue}
}
func (c mappedRows[A, B]) Clone(value B) B {
	if c.cloneValue == nil {
		panic("gocql: nil row clone function")
	}
	return c.cloneValue(value)
}
func (c mappedRows[A, B]) Compile(columns Columns) (RowPlan[B], error) {
	if c.codec == nil || c.mapValue == nil || c.cloneValue == nil {
		return nil, fmt.Errorf("gocql: nil row mapping")
	}
	plan, err := c.codec.Compile(columns)
	if err != nil {
		return nil, err
	}
	return mappedRowPlan[A, B]{plan: plan, mapValue: c.mapValue, cloneValue: c.cloneValue}, nil
}
func (p mappedRowPlan[A, B]) Decode(reader *RowReader) (B, error) {
	v, err := p.plan.Decode(reader)
	if err != nil {
		var zero B
		return zero, err
	}
	return p.mapValue(v), nil
}
func (p mappedRowPlan[A, B]) Clone(value B) B { return p.cloneValue(value) }

func splitColumns(columns Columns, split int) (Columns, Columns, error) {
	if split < 0 || split > columns.Len() {
		return Columns{}, Columns{}, fmt.Errorf("gocql: invalid column split %d/%d", split, columns.Len())
	}
	left, right := make([]ColumnSpec, split), make([]ColumnSpec, columns.Len()-split)
	for i := range left {
		left[i], _ = columns.At(i)
	}
	for i := range right {
		right[i], _ = columns.At(split + i)
	}
	return NewColumns(left...), NewColumns(right...), nil
}

type pairedParams[A, B any] struct {
	split int
	a     ParamEncoder[A]
	b     ParamEncoder[B]
}
type pairedParamPlan[A, B any] struct {
	left, right Columns
	a           ParamPlan[A]
	b           ParamPlan[B]
}

func PairParams[A, B any](leftColumns int, a ParamEncoder[A], b ParamEncoder[B]) ParamEncoder[Pair[A, B]] {
	return pairedParams[A, B]{split: leftColumns, a: a, b: b}
}
func (c pairedParams[A, B]) Compile(columns Columns) (ParamPlan[Pair[A, B]], error) {
	if c.a == nil || c.b == nil {
		return nil, fmt.Errorf("gocql: nil paired parameter codec")
	}
	left, right, err := splitColumns(columns, c.split)
	if err != nil {
		return nil, err
	}
	a, err := c.a.Compile(left)
	if err != nil {
		return nil, err
	}
	b, err := c.b.Compile(right)
	if err != nil {
		return nil, err
	}
	return pairedParamPlan[A, B]{left: left, right: right, a: a, b: b}, nil
}
func (p pairedParamPlan[A, B]) Encode(w *ParamWriter, value Pair[A, B]) error {
	mark := w.builder.Mark()
	columns, index := w.columns, w.index
	completed := false
	defer func() {
		w.columns, w.index = columns, index
		if completed {
			w.index += p.left.Len() + p.right.Len()
		}
	}()
	w.columns, w.index = p.left, 0
	err := p.a.Encode(w, value.First)
	if err == nil {
		err = finishParamWriter(w)
	}
	w.columns, w.index = p.right, 0
	if err == nil {
		err = p.b.Encode(w, value.Second)
	}
	if err == nil {
		err = finishParamWriter(w)
	}
	if err != nil {
		w.builder.Rollback(mark)
		return err
	}
	completed = true
	return nil
}

type pairedRows[A, B any] struct {
	split int
	a     RowDecoder[A]
	b     RowDecoder[B]
}
type pairedRowPlan[A, B any] struct {
	left, right Columns
	a           RowPlan[A]
	b           RowPlan[B]
}

func PairRows[A, B any](leftColumns int, a RowDecoder[A], b RowDecoder[B]) RowDecoder[Pair[A, B]] {
	return pairedRows[A, B]{split: leftColumns, a: a, b: b}
}
func (c pairedRows[A, B]) Clone(value Pair[A, B]) Pair[A, B] {
	return Pair[A, B]{First: c.a.Clone(value.First), Second: c.b.Clone(value.Second)}
}
func (c pairedRows[A, B]) Compile(columns Columns) (RowPlan[Pair[A, B]], error) {
	if c.a == nil || c.b == nil {
		return nil, fmt.Errorf("gocql: nil paired row codec")
	}
	left, right, err := splitColumns(columns, c.split)
	if err != nil {
		return nil, err
	}
	a, err := c.a.Compile(left)
	if err != nil {
		return nil, err
	}
	b, err := c.b.Compile(right)
	if err != nil {
		return nil, err
	}
	return pairedRowPlan[A, B]{left: left, right: right, a: a, b: b}, nil
}
func (p pairedRowPlan[A, B]) Decode(r *RowReader) (Pair[A, B], error) {
	var result Pair[A, B]
	columns, index := r.columns, r.index
	completed := false
	defer func() {
		r.columns, r.index = columns, index
		if completed {
			r.index += p.left.Len() + p.right.Len()
		}
	}()
	r.columns, r.index = p.left, 0
	av, err := p.a.Decode(r)
	if err == nil {
		err = finishRowReader(r)
	}
	if err != nil {
		return result, err
	}
	r.columns, r.index = p.right, 0
	bv, err := p.b.Decode(r)
	if err == nil {
		err = finishRowReader(r)
	}
	if err != nil {
		return result, err
	}
	completed = true
	return Pair[A, B]{First: av, Second: bv}, nil
}
func (p pairedRowPlan[A, B]) Clone(value Pair[A, B]) Pair[A, B] {
	return Pair[A, B]{First: p.a.Clone(value.First), Second: p.b.Clone(value.Second)}
}

// Composite codecs can bind nested field plans once with the enclosing plan.
// The returned codec must be immutable and safe for concurrent use.
type compiledValueDecoder[T any] interface {
	compileValueDecoder(DataType) (Decoder[T], error)
}
type compiledValueEncoder[T any] interface {
	compileValueEncoder(DataType) (Encoder[T], error)
}

func compileValueDecoder[T any](decoder Decoder[T], typ DataType) (Decoder[T], error) {
	if decoder == nil {
		return nil, fmt.Errorf("gocql: nil value decoder")
	}
	if compiled, ok := decoder.(compiledValueDecoder[T]); ok {
		return compiled.compileValueDecoder(typ)
	}
	if err := decoder.CheckType(typ); err != nil {
		return nil, err
	}
	return decoder, nil
}
func compileValueEncoder[T any](encoder Encoder[T], typ DataType) (Encoder[T], error) {
	if encoder == nil {
		return nil, fmt.Errorf("gocql: nil value encoder")
	}
	if compiled, ok := encoder.(compiledValueEncoder[T]); ok {
		return compiled.compileValueEncoder(typ)
	}
	if err := encoder.CheckType(typ); err != nil {
		return nil, err
	}
	return encoder, nil
}
