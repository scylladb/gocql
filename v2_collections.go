// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package gocql

import (
	"fmt"
	"math"
	"math/bits"

	"github.com/scylladb/gocql/v2/internal/wire"
)

// Entry preserves the order and duplicates in a map's encoded representation.
type Entry[K, V any] struct {
	Key   K
	Value V
}

// ValueIterator is a bounded cursor. A failed Next permanently stops iteration;
// Value never exposes the partially decoded value which caused that failure.
type ValueIterator[T any] struct {
	cursor       ValueCursor
	count, index int
	next         func(int) (T, error)
	nextCursor   func(*ValueCursor, int) (T, error)
	value        T
	err          error
	done         bool
}

func newValueIterator[T any](count int, next func(int) (T, error)) *ValueIterator[T] {
	return &ValueIterator[T]{count: count, next: next}
}

func (i *ValueIterator[T]) Next() bool {
	if i == nil || i.done {
		return false
	}
	var zero T
	i.value = zero
	if i.nextCursor != nil && i.err == nil {
		i.err = i.cursor.check()
	}
	if i.err != nil || i.index == i.count {
		i.done = true
		return false
	}
	if i.count < 0 || (i.next == nil && i.nextCursor == nil) {
		i.err = typeValidation("iterator", "uninitialized iterator")
		i.done = true
		return false
	}
	var value T
	var err error
	if i.nextCursor != nil {
		value, err = i.nextCursor(&i.cursor, i.index)
	} else {
		value, err = i.next(i.index)
	}
	if err != nil {
		i.err, i.done = err, true
		return false
	}
	i.index++
	i.value = value
	return true
}
func (i *ValueIterator[T]) Value() T {
	if i == nil {
		var zero T
		return zero
	}
	return i.value
}
func (i *ValueIterator[T]) Err() error {
	if i == nil {
		return typeValidation("iterator", "nil iterator")
	}
	return i.err
}

// Collect explicitly materializes the iterator's remaining values. It returns
// no partial slice on failure and does not detach borrowed values within it.
func Collect[T any](i *ValueIterator[T]) ([]T, error) {
	if i == nil {
		return nil, typeValidation("iterator", "nil iterator")
	}
	values := make([]T, 0, max(0, i.count-i.index))
	for i.Next() {
		values = append(values, i.Value())
	}
	if err := i.Err(); err != nil {
		return nil, err
	}
	return values, nil
}

// Sequence is a lazy list, set, or vector. Its encoded backing storage retains
// the source page lease; Clone detaches that storage without decoding elements.
type Sequence[T any] struct {
	body    Bytes
	count   int
	element DataType
	decoder Decoder[T]
	vector  bool
	width   int64
}

func (s Sequence[T]) Len() int           { return s.count }
func (s Sequence[T]) Clone() Sequence[T] { s.body = s.body.Clone(); return s }
func (s Sequence[T]) Iterator() *ValueIterator[T] {
	r := ValueReader{body: s.body.chain, lease: s.body.lease}
	i := &ValueIterator[T]{count: s.count, cursor: r.cursorValue()}
	if err := s.body.checkAlive(); err != nil {
		i.err = err
		return i
	}
	i.nextCursor = func(cursor *ValueCursor, index int) (T, error) {
		var zero T
		var child ValueReader
		var err error
		if s.vector {
			child, err = readVectorElement(cursor, s.element, s.width)
		} else {
			child, err = cursor.Child(s.element)
		}
		if err == nil {
			var value T
			value, err = s.decoder.Decode(child)
			if err == nil {
				return value, nil
			}
		}
		return zero, fmt.Errorf("gocql: element %d: %w", index, err)
	}
	return i
}

// Mapping is a lazy ordered map view; keys need not be Go-comparable.
type Mapping[K, V any] struct {
	body         Bytes
	count        int
	key, value   DataType
	keyDecoder   Decoder[K]
	valueDecoder Decoder[V]
}

func (m Mapping[K, V]) Len() int             { return m.count }
func (m Mapping[K, V]) Clone() Mapping[K, V] { m.body = m.body.Clone(); return m }
func (m Mapping[K, V]) Iterator() *ValueIterator[Entry[K, V]] {
	r := ValueReader{body: m.body.chain, lease: m.body.lease}
	i := &ValueIterator[Entry[K, V]]{count: m.count, cursor: r.cursorValue()}
	if err := m.body.checkAlive(); err != nil {
		i.err = err
		return i
	}
	i.nextCursor = func(cursor *ValueCursor, index int) (Entry[K, V], error) {
		var zero Entry[K, V]
		kr, err := cursor.Child(m.key)
		if err != nil {
			return zero, fmt.Errorf("gocql: map key %d: %w", index, err)
		}
		vr, err := cursor.Child(m.value)
		if err != nil {
			return zero, fmt.Errorf("gocql: map value %d: %w", index, err)
		}
		key, err := m.keyDecoder.Decode(kr)
		if err != nil {
			return zero, fmt.Errorf("gocql: map key %d: %w", index, err)
		}
		value, err := m.valueDecoder.Decode(vr)
		if err != nil {
			return zero, fmt.Errorf("gocql: map value %d: %w", index, err)
		}
		return Entry[K, V]{Key: key, Value: value}, nil
	}
	return i
}

type sequenceEncoder[T any] struct {
	kind    Type
	element Encoder[T]
	typ     DataType
	width   int64
}

func ListEncoder[T any](element Encoder[T]) Encoder[[]T] {
	return sequenceEncoder[T]{kind: TypeList, element: element}
}
func SetEncoder[T any](element Encoder[T]) Encoder[[]T] {
	return sequenceEncoder[T]{kind: TypeSet, element: element}
}
func VectorEncoder[T any](element Encoder[T]) Encoder[[]T] {
	return sequenceEncoder[T]{kind: TypeCustom, element: element}
}

func sequenceElement(typ DataType, kind Type) (DataType, int64, error) {
	if err := validateDataType(typ); err != nil {
		return DataType{}, 0, err
	}
	element, ok := typ.Element()
	if !ok || typ.Kind() != kind {
		return DataType{}, 0, typeValidation("type", "expected %s, got %s", kind, typ)
	}
	if kind != TypeCustom {
		return element, 0, nil
	}
	dimension, vector := typ.Dimension()
	if !vector || dimension == 0 {
		return DataType{}, 0, typeValidation("dimension", "vector dimension must be positive")
	}
	width, err := vectorFixedWidth(element)
	if err == nil && width > math.MaxInt32/int64(dimension) {
		err = typeValidation("dimension", "vector body exceeds signed 32-bit length")
	}
	return element, width, err
}
func (c sequenceEncoder[T]) CheckType(typ DataType) error {
	_, err := c.compileValueEncoder(typ)
	return err
}
func (c sequenceEncoder[T]) compileValueEncoder(typ DataType) (Encoder[[]T], error) {
	element, width, err := sequenceElement(typ, c.kind)
	if err != nil {
		return nil, err
	}
	encoder, err := compileValueEncoder(c.element, element)
	if err != nil {
		return nil, fmt.Errorf("gocql: element type: %w", err)
	}
	c.typ, c.element, c.width = typ, encoder, width
	return c, nil
}
func (c sequenceEncoder[T]) Encode(w *ValueWriter, values []T) error {
	if c.typ.descriptor == nil {
		bound, err := c.compileValueEncoder(w.Type())
		if err != nil {
			return err
		}
		return bound.Encode(w, values)
	}
	if !c.typ.Equal(w.Type()) {
		return typeValidation("type", "sequence metadata changed after compilation")
	}
	element, _ := c.typ.Element()
	if c.kind == TypeCustom {
		dimension, _ := c.typ.Dimension()
		if len(values) != int(dimension) {
			return typeValidation("dimension", "vector requires %d elements, got %d", dimension, len(values))
		}
		for i, value := range values {
			if err := writeVectorElement(w, element, c.width, c.element, value); err != nil {
				return fmt.Errorf("gocql: vector element %d: %w", i, err)
			}
		}
		return nil
	}
	if uint64(len(values)) > math.MaxInt32 {
		return typeValidation("count", "collection has too many elements")
	}
	if err := w.Uint32(uint32(len(values))); err != nil {
		return err
	}
	for i, value := range values {
		if err := writeCollectionElement(w, element, c.element, value); err != nil {
			return fmt.Errorf("gocql: element %d: %w", i, err)
		}
	}
	return nil
}
func writeCollectionElement[T any](w *ValueWriter, typ DataType, encoder Encoder[T], value T) error {
	return w.Child(typ, func(child *ValueWriter) error {
		if err := encoder.Encode(child, value); err != nil {
			return err
		}
		if child.state != Present && child.state != Empty {
			return typeValidation("element", "collection elements cannot be null or unset")
		}
		return nil
	})
}

type sequenceDecoder[T any] struct {
	kind    Type
	element Decoder[T]
	typ     DataType
	width   int64
}

func ListDecoder[T any](element Decoder[T]) Decoder[Sequence[T]] {
	return sequenceDecoder[T]{kind: TypeList, element: element}
}
func SetDecoder[T any](element Decoder[T]) Decoder[Sequence[T]] {
	return sequenceDecoder[T]{kind: TypeSet, element: element}
}
func VectorDecoder[T any](element Decoder[T]) Decoder[Sequence[T]] {
	return sequenceDecoder[T]{kind: TypeCustom, element: element}
}
func (c sequenceDecoder[T]) CheckType(typ DataType) error {
	_, err := c.compileValueDecoder(typ)
	return err
}
func (c sequenceDecoder[T]) compileValueDecoder(typ DataType) (Decoder[Sequence[T]], error) {
	element, width, err := sequenceElement(typ, c.kind)
	if err != nil {
		return nil, err
	}
	decoder, err := compileValueDecoder(c.element, element)
	if err != nil {
		return nil, fmt.Errorf("gocql: element type: %w", err)
	}
	c.typ, c.element, c.width = typ, decoder, width
	return c, nil
}
func (c sequenceDecoder[T]) Decode(r ValueReader) (Sequence[T], error) {
	if c.typ.descriptor == nil {
		bound, err := c.compileValueDecoder(r.Type())
		if err != nil {
			return Sequence[T]{}, err
		}
		return bound.Decode(r)
	}
	if !c.typ.Equal(r.Type()) {
		return Sequence[T]{}, typeValidation("type", "sequence metadata changed after compilation")
	}
	element, _ := c.typ.Element()
	body, count, width, err := validateSequenceLayout(r, c.kind, element, c.width)
	if err != nil {
		return Sequence[T]{}, err
	}
	return Sequence[T]{body: body, count: count, element: element, decoder: c.element, vector: c.kind == TypeCustom, width: width}, nil
}
func (c sequenceDecoder[T]) Clone(value Sequence[T]) Sequence[T] { return value.Clone() }

type mapEncoder[K, V any] struct {
	key   Encoder[K]
	value Encoder[V]
	typ   DataType
}

func MapEncoder[K, V any](key Encoder[K], value Encoder[V]) Encoder[[]Entry[K, V]] {
	return mapEncoder[K, V]{key: key, value: value}
}
func (c mapEncoder[K, V]) CheckType(typ DataType) error {
	_, err := c.compileValueEncoder(typ)
	return err
}
func (c mapEncoder[K, V]) compileValueEncoder(typ DataType) (Encoder[[]Entry[K, V]], error) {
	kt, vt, ok := typ.KeyValue()
	if !ok {
		return nil, typeValidation("type", "expected map, got %s", typ)
	}
	key, err := compileValueEncoder(c.key, kt)
	if err != nil {
		return nil, fmt.Errorf("gocql: map key type: %w", err)
	}
	value, err := compileValueEncoder(c.value, vt)
	if err != nil {
		return nil, fmt.Errorf("gocql: map value type: %w", err)
	}
	c.typ, c.key, c.value = typ, key, value
	return c, nil
}
func (c mapEncoder[K, V]) Encode(w *ValueWriter, entries []Entry[K, V]) error {
	if c.typ.descriptor == nil {
		bound, err := c.compileValueEncoder(w.Type())
		if err != nil {
			return err
		}
		return bound.Encode(w, entries)
	}
	if !c.typ.Equal(w.Type()) {
		return typeValidation("type", "map metadata changed after compilation")
	}
	if uint64(len(entries)) > math.MaxInt32 {
		return typeValidation("count", "map has too many entries")
	}
	if err := w.Uint32(uint32(len(entries))); err != nil {
		return err
	}
	kt, vt, _ := c.typ.KeyValue()
	for i, entry := range entries {
		if err := writeCollectionElement(w, kt, c.key, entry.Key); err != nil {
			return fmt.Errorf("gocql: map key %d: %w", i, err)
		}
		if err := writeCollectionElement(w, vt, c.value, entry.Value); err != nil {
			return fmt.Errorf("gocql: map value %d: %w", i, err)
		}
	}
	return nil
}

type mapDecoder[K, V any] struct {
	key   Decoder[K]
	value Decoder[V]
	typ   DataType
}

func MapDecoder[K, V any](key Decoder[K], value Decoder[V]) Decoder[Mapping[K, V]] {
	return mapDecoder[K, V]{key: key, value: value}
}
func (c mapDecoder[K, V]) CheckType(typ DataType) error {
	_, err := c.compileValueDecoder(typ)
	return err
}
func (c mapDecoder[K, V]) compileValueDecoder(typ DataType) (Decoder[Mapping[K, V]], error) {
	kt, vt, ok := typ.KeyValue()
	if !ok {
		return nil, typeValidation("type", "expected map, got %s", typ)
	}
	key, err := compileValueDecoder(c.key, kt)
	if err != nil {
		return nil, fmt.Errorf("gocql: map key type: %w", err)
	}
	value, err := compileValueDecoder(c.value, vt)
	if err != nil {
		return nil, fmt.Errorf("gocql: map value type: %w", err)
	}
	c.typ, c.key, c.value = typ, key, value
	return c, nil
}
func (c mapDecoder[K, V]) Decode(r ValueReader) (Mapping[K, V], error) {
	if c.typ.descriptor == nil {
		bound, err := c.compileValueDecoder(r.Type())
		if err != nil {
			return Mapping[K, V]{}, err
		}
		return bound.Decode(r)
	}
	if !c.typ.Equal(r.Type()) {
		return Mapping[K, V]{}, typeValidation("type", "map metadata changed after compilation")
	}
	body, count, err := validateMapBody(r)
	if err != nil {
		return Mapping[K, V]{}, err
	}
	kt, vt, _ := c.typ.KeyValue()
	return Mapping[K, V]{body: body, count: count, key: kt, value: vt, keyDecoder: c.key, valueDecoder: c.value}, nil
}
func (c mapDecoder[K, V]) Clone(value Mapping[K, V]) Mapping[K, V] { return value.Clone() }

// Collection structure is checked before publishing a lazy view. This pass
// examines only framing; semantic checks remain in each requested child codec.
func collectionCount(cursor *ValueCursor, minimum int64) (int, error) {
	n, err := cursor.Uint32()
	if err != nil {
		return 0, err
	}
	if n > math.MaxInt32 || int64(n) > cursor.Remaining()/minimum {
		return 0, typeValidation("count", "collection count %d exceeds available framing", n)
	}
	return int(n), nil
}
func checkedCollectionChild(cursor *ValueCursor, typ DataType) error {
	child, err := cursor.Child(typ)
	if err != nil {
		return err
	}
	if child.State() != Present {
		return typeValidation("element", "collection elements cannot be null or unset")
	}
	return nil
}
func validateSequenceBody(r ValueReader, kind Type) (Bytes, int, int64, error) {
	element, width, err := sequenceElement(r.Type(), kind)
	if err != nil {
		return Bytes{}, 0, 0, err
	}
	return validateSequenceLayout(r, kind, element, width)
}
func validateSequenceLayout(r ValueReader, kind Type, element DataType, width int64) (Bytes, int, int64, error) {
	if err := requirePresent(r); err != nil {
		return Bytes{}, 0, 0, err
	}
	cursor := r.cursorValue()
	if kind == TypeCustom {
		dimension, _ := r.Type().Dimension()
		count := int(dimension)
		if width > 0 {
			if r.body.Len() != int64(count)*width {
				return Bytes{}, 0, 0, typeValidation("dimension", "vector requires %d bytes, got %d", int64(count)*width, r.body.Len())
			}
		} else {
			if int64(count) > cursor.Remaining() {
				return Bytes{}, 0, 0, typeValidation("dimension", "vector dimension exceeds available lengths")
			}
			for i := 0; i < count; i++ {
				if _, err := readVectorElement(&cursor, element, 0); err != nil {
					return Bytes{}, 0, 0, fmt.Errorf("gocql: vector element %d: %w", i, err)
				}
			}
			if err := cursor.RequireEnd(); err != nil {
				return Bytes{}, 0, 0, err
			}
		}
		return r.Bytes(), count, width, nil
	}
	count, err := collectionCount(&cursor, 4)
	if err != nil {
		return Bytes{}, 0, 0, err
	}
	body, err := r.Bytes().Slice(4, r.body.Len()-4)
	if err != nil {
		return Bytes{}, 0, 0, err
	}
	for i := 0; i < count; i++ {
		if err := checkedCollectionChild(&cursor, element); err != nil {
			return Bytes{}, 0, 0, fmt.Errorf("gocql: element %d: %w", i, err)
		}
	}
	if err := cursor.RequireEnd(); err != nil {
		return Bytes{}, 0, 0, err
	}
	return body, count, 0, nil
}
func validateMapBody(r ValueReader) (Bytes, int, error) {
	if err := requirePresent(r); err != nil {
		return Bytes{}, 0, err
	}
	key, value, ok := r.Type().KeyValue()
	if !ok {
		return Bytes{}, 0, typeValidation("type", "expected map, got %s", r.Type())
	}
	cursor := r.cursorValue()
	count, err := collectionCount(&cursor, 8)
	if err != nil {
		return Bytes{}, 0, err
	}
	body, err := r.Bytes().Slice(4, r.body.Len()-4)
	if err != nil {
		return Bytes{}, 0, err
	}
	for i := 0; i < count; i++ {
		if err := checkedCollectionChild(&cursor, key); err != nil {
			return Bytes{}, 0, fmt.Errorf("gocql: map key %d: %w", i, err)
		}
		if err := checkedCollectionChild(&cursor, value); err != nil {
			return Bytes{}, 0, fmt.Errorf("gocql: map value %d: %w", i, err)
		}
	}
	if err := cursor.RequireEnd(); err != nil {
		return Bytes{}, 0, err
	}
	return body, count, nil
}

// vectorFixedWidth follows the server's serializer classification. In
// particular smallint, tinyint, date, time, and counter are variable-width here.
func vectorFixedWidth(typ DataType) (int64, error) {
	if err := validateDataType(typ); err != nil {
		return 0, err
	}
	switch typ.Kind() {
	case TypeBoolean:
		return 1, nil
	case TypeInt, TypeFloat:
		return 4, nil
	case TypeBigInt, TypeDouble, TypeTimestamp:
		return 8, nil
	case TypeUUID, TypeTimeUUID:
		return 16, nil
	case TypeCustom:
		dimension, ok := typ.Dimension()
		if !ok || dimension == 0 {
			return 0, typeValidation("dimension", "vector dimension must be positive")
		}
		element, _ := typ.Element()
		width, err := vectorFixedWidth(element)
		if err != nil {
			return 0, err
		}
		if width > math.MaxInt32/int64(dimension) {
			return 0, typeValidation("dimension", "nested vector width exceeds signed 32-bit length")
		}
		return width * int64(dimension), nil
	default:
		return 0, nil
	}
}
func readVectorElement(cursor *ValueCursor, typ DataType, width int64) (ValueReader, error) {
	if width == 0 {
		first, err := cursor.Uint8()
		if err != nil {
			return ValueReader{}, err
		}
		extra := bits.LeadingZeros8(^first)
		length := uint64(first & byte(0xff>>uint(extra)))
		for j := 0; j < extra; j++ {
			b, err := cursor.Uint8()
			if err != nil {
				return ValueReader{}, err
			}
			length = length<<8 | uint64(b)
		}
		if extra > 0 && length < uint64(1)<<uint(extra*7) {
			return ValueReader{}, cursor.failure(typeValidation("length", "noncanonical vector length"))
		}
		if length > math.MaxInt32 {
			return ValueReader{}, cursor.failure(typeValidation("length", "vector element length exceeds signed 32-bit range"))
		}
		width = int64(length)
	}
	body, err := cursor.Take(width)
	if err != nil {
		return ValueReader{}, err
	}
	return newValueReader(typ, Present, body.chain, body.lease), nil
}

// Fixed vector members write directly into the enclosing builder. Variable
// members use an independent descriptor builder because their unsigned vint
// length must precede the body and cannot be patched as a four-byte cell length.
func writeVectorElement[T any](w *ValueWriter, typ DataType, width int64, encoder Encoder[T], value T) error {
	if err := w.payload(); err != nil {
		return err
	}
	if width > 0 {
		start := w.builder.Len()
		child := &ValueWriter{builder: w.builder, typ: typ, mark: w.builder.Mark(), start: start - 4, ownBorrowed: w.ownBorrowed}
		if err := encoder.Encode(child, value); err != nil {
			abortValueWriter(child)
			return w.fail(err)
		}
		if child.err != nil {
			abortValueWriter(child)
			return w.fail(child.err)
		}
		if child.state != Present || w.builder.Len()-start != width {
			abortValueWriter(child)
			return w.fail(typeValidation("element", "fixed vector element requires %d present bytes", width))
		}
		child.finished = true
		return nil
	}
	builder := wire.NewBuilder(64)
	child := newValueWriter(builder, typ, false)
	child.ownBorrowed = w.ownBorrowed
	if err := encoder.Encode(child, value); err != nil {
		abortValueWriter(child)
		return w.fail(err)
	}
	if child.state != Present && child.state != Empty {
		abortValueWriter(child)
		return w.fail(typeValidation("element", "vector elements cannot be null or unset"))
	}
	if err := finishValueWriter(child); err != nil {
		return w.fail(err)
	}
	chain, err := builder.Freeze()
	if err != nil {
		return w.fail(err)
	}
	body, err := chain.Slice(4, chain.Len()-4)
	if err != nil {
		return w.fail(err)
	}
	if err := scalarWriteUnsignedVInt(w, uint64(body.Len())); err != nil {
		return err
	}
	return w.fail(body.Each(w.builder.Borrow))
}
