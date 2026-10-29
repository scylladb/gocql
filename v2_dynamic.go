// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package gocql

import (
	"fmt"
	"math"
	"net/netip"

	"github.com/scylladb/gocql/v2/internal/wire"
)

// Value is an explicitly typed CQL value. Variable-length payloads and aggregate
// input descriptors are borrowed; Clone detaches them. The type and state select
// the active payload; no payload is stored in an interface.
type Value struct {
	typ         DataType
	state       CellState
	bits        uint64
	uuid        UUID
	inet        netip.Addr
	duration    Duration
	scale       int32
	bytes       Bytes
	values      []Value
	fields      []ValueField
	entries     []ValueEntry
	encoded     bool
	count       int
	vectorWidth int64
	shape       *streamValidationPlan
}

type ValueField struct {
	Name  string
	Value Value
}
type ValueEntry struct{ Key, Value Value }

func (v Value) Type() DataType   { return v.typ }
func (v Value) State() CellState { return v.state }

var dynamicScalarTypes = func() [TypeDuration + 1]DataType {
	var result [TypeDuration + 1]DataType
	for kind := TypeAscii; kind <= TypeDuration; kind++ {
		result[kind], _ = ScalarType(kind)
	}
	return result
}()

func dynamicScalar(kind Type) Value { return Value{typ: dynamicScalarTypes[kind]} }
func NullValue(t DataType) Value    { return Value{typ: t, state: Null} }
func UnsetDynamic(t DataType) Value { return Value{typ: t, state: Unset} }
func EmptyValue(t DataType) (Value, error) {
	if err := DynamicCodec.CheckType(t); err != nil {
		return Value{}, err
	}
	if !emptySupported(t) {
		return Value{}, typeValidation("type", "%s does not support Empty", t)
	}
	return Value{typ: t, state: Empty}, nil
}
func BoolValue(b bool) Value {
	v := dynamicScalar(TypeBoolean)
	if b {
		v.bits = 1
	}
	return v
}
func TinyIntValue(n int8) Value    { v := dynamicScalar(TypeTinyInt); v.bits = uint64(n); return v }
func SmallIntValue(n int16) Value  { v := dynamicScalar(TypeSmallInt); v.bits = uint64(n); return v }
func IntValue(n int32) Value       { v := dynamicScalar(TypeInt); v.bits = uint64(n); return v }
func BigIntValue(n int64) Value    { v := dynamicScalar(TypeBigInt); v.bits = uint64(n); return v }
func CounterValue(n Counter) Value { v := dynamicScalar(TypeCounter); v.bits = uint64(n); return v }
func FloatValue(n float32) Value {
	v := dynamicScalar(TypeFloat)
	v.bits = uint64(math.Float32bits(n))
	return v
}
func DoubleValue(n float64) Value {
	v := dynamicScalar(TypeDouble)
	v.bits = math.Float64bits(n)
	return v
}
func TextValue(s string) (Value, error) {
	text, err := BorrowText(s)
	if err != nil {
		return Value{}, err
	}
	v := dynamicScalar(TypeText)
	v.bytes = text.Bytes()
	return v, nil
}
func ASCIIValue(s string) (Value, error) {
	v := dynamicScalar(TypeAscii)
	// ASCII validation is also UTF-8 validation; avoid a panicking text constructor.
	c, _ := wire.NewChain(wire.BorrowString(s))
	v.bytes = Bytes{chain: c}
	if err := validateText(v.bytes, true); err != nil {
		return Value{}, err
	}
	return v, nil
}
func BlobValue(b []byte) Value { v := dynamicScalar(TypeBlob); v.bytes = BorrowBytes(b); return v }
func UUIDValue(u UUID) Value   { v := dynamicScalar(TypeUUID); v.uuid = u; return v }
func TimeUUIDValue(u TimeUUID) (Value, error) {
	if UUID(u).Version() != 1 {
		return Value{}, typeValidation("timeuuid", "must have version 1")
	}
	v := dynamicScalar(TypeTimeUUID)
	v.uuid = UUID(u)
	return v, nil
}
func InetValue(a netip.Addr) (Value, error) {
	if !a.IsValid() || a.Zone() != "" {
		return Value{}, typeValidation("inet", "requires a valid address without a zone")
	}
	v := dynamicScalar(TypeInet)
	v.inet = a
	return v, nil
}
func DateValue(n Date) Value { v := dynamicScalar(TypeDate); v.bits = uint64(n); return v }
func TimeValue(n TimeOfDay) (Value, error) {
	if n < 0 || int64(n) >= nanosecondsPerDay {
		return Value{}, typeValidation("time", "outside one day")
	}
	v := dynamicScalar(TypeTime)
	v.bits = uint64(n)
	return v, nil
}
func TimestampValue(n Timestamp) Value {
	v := dynamicScalar(TypeTimestamp)
	v.bits = uint64(n)
	return v
}
func DurationValue(d Duration) (Value, error) {
	if !durationSignsValid(d) {
		return Value{}, typeValidation("duration", "components must have compatible signs")
	}
	v := dynamicScalar(TypeDuration)
	v.duration = d
	return v, nil
}
func VarintValue(n Varint) Value { v := dynamicScalar(TypeVarint); v.bytes = n.bytes; return v }
func DecimalValue(n Decimal) Value {
	v := dynamicScalar(TypeDecimal)
	v.scale = n.Scale
	v.bytes = n.Unscaled.bytes
	return v
}

func (v Value) require(kinds ...Type) error {
	if err := validateDataType(v.typ); err != nil {
		return err
	}
	if v.state != Present {
		return typeValidation("value", "%s has state %d", v.typ, v.state)
	}
	for _, kind := range kinds {
		if v.typ.Kind() == kind {
			return v.bytes.checkAlive()
		}
	}
	return typeValidation("value", "accessor does not accept %s", v.typ)
}
func (v Value) Bool() (bool, error) { err := v.require(TypeBoolean); return v.bits != 0, err }
func (v Value) Signed() (int64, error) {
	err := v.require(TypeTinyInt, TypeSmallInt, TypeInt, TypeBigInt, TypeCounter, TypeDate, TypeTime, TypeTimestamp)
	return int64(v.bits), err
}
func (v Value) Float() (float64, error) {
	if err := v.require(TypeFloat, TypeDouble); err != nil {
		return 0, err
	}
	if v.typ.Kind() == TypeFloat {
		return float64(math.Float32frombits(uint32(v.bits))), nil
	}
	return math.Float64frombits(v.bits), nil
}
func (v Value) Text() (Text, error) {
	if err := v.require(TypeText, TypeVarchar, TypeAscii); err != nil {
		return Text{}, err
	}
	return Text{bytes: v.bytes}, nil
}
func (v Value) Bytes() (Bytes, error) {
	if err := v.require(TypeBlob); err != nil {
		return Bytes{}, err
	}
	return v.bytes, nil
}
func (v Value) UUID() (UUID, error) {
	if err := v.require(TypeUUID, TypeTimeUUID); err != nil {
		return UUID{}, err
	}
	return v.uuid, nil
}
func (v Value) Inet() (netip.Addr, error) {
	if err := v.require(TypeInet); err != nil {
		return netip.Addr{}, err
	}
	return v.inet, nil
}
func (v Value) Duration() (Duration, error) {
	if err := v.require(TypeDuration); err != nil {
		return Duration{}, err
	}
	return v.duration, nil
}
func (v Value) Varint() (Varint, error) {
	if err := v.require(TypeVarint); err != nil {
		return Varint{}, err
	}
	return Varint{bytes: v.bytes}, nil
}
func (v Value) Decimal() (Decimal, error) {
	if err := v.require(TypeDecimal); err != nil {
		return Decimal{}, err
	}
	return Decimal{Scale: v.scale, Unscaled: Varint{bytes: v.bytes}}, nil
}

// Clone detaches a lazy aggregate by copying its encoded backing, without
// decoding its children. Explicit input descriptors are cloned recursively.
func (v Value) Clone() Value {
	if v.bytes.lease != nil || v.bytes.Len() != 0 {
		v.bytes = v.bytes.Clone()
	}
	if v.values != nil {
		values := make([]Value, len(v.values))
		for i := range values {
			values[i] = v.values[i].Clone()
		}
		v.values = values
	}
	if v.fields != nil {
		fields := make([]ValueField, len(v.fields))
		for i, f := range v.fields {
			fields[i] = ValueField{Name: f.Name, Value: f.Value.Clone()}
		}
		v.fields = fields
	}
	if v.entries != nil {
		entries := make([]ValueEntry, len(v.entries))
		for i, e := range v.entries {
			entries[i] = ValueEntry{Key: e.Key.Clone(), Value: e.Value.Clone()}
		}
		v.entries = entries
	}
	return v
}

// DynamicCodec checks and serializes the explicit variant selected by Value.Type.
var DynamicCodec Codec[Value] = dynamicCodec{}

type dynamicCodec struct{}

func (dynamicCodec) CheckType(t DataType) error { return checkDynamicType(t, 0) }
func checkDynamicType(t DataType, depth int) error {
	_, err := checkedTypeHeight(t, depth, make(map[*dataTypeDescriptor]int), make(map[*dataTypeDescriptor]bool))
	return err
}
func checkedTypeHeight(t DataType, depth int, heights map[*dataTypeDescriptor]int, active map[*dataTypeDescriptor]bool) (int, error) {
	if err := validateDataType(t); err != nil {
		return 0, err
	}
	if depth > 128 {
		return 0, typeValidation("type", "dynamic nesting exceeds 128")
	}
	d := t.descriptor
	if height, ok := heights[d]; ok {
		if depth+height > 128 {
			return 0, typeValidation("type", "dynamic nesting exceeds 128")
		}
		return height, nil
	}
	if active[d] {
		return 0, typeValidation("type", "cyclic descriptor")
	}
	active[d] = true
	height := 0
	child := func(t DataType) error {
		h, err := checkedTypeHeight(t, depth+1, heights, active)
		height = max(height, h+1)
		return err
	}
	var err error
	switch d.kind {
	case TypeList, TypeSet:
		err = child(d.element)
	case TypeMap:
		err = child(d.key)
		if err == nil {
			err = child(d.element)
		}
	case TypeTuple, TypeUDT:
		if d.kind == TypeUDT && (d.keyspace == "" || d.name == "") {
			return 0, typeValidation("type", "UDT identity is incomplete")
		}
		names := make(map[string]bool)
		for i, f := range d.fields.columns {
			if d.kind == TypeUDT {
				if f.Name == "" || names[f.Name] {
					return 0, typeValidation("type", "empty or duplicate UDT field %q", f.Name)
				}
				names[f.Name] = true
			}
			if err = child(f.Type); err != nil {
				return 0, fmt.Errorf("dynamic field %d: %w", i, err)
			}
		}
	case TypeCustom:
		if d.dimension == 0 {
			return 0, typeValidation("type", "incomplete vector type")
		}
		err = child(d.element)
		if err == nil {
			_, err = vectorFixedWidth(t)
		}
	default:
		_, err = ScalarType(d.kind)
	}
	if err != nil {
		return 0, err
	}
	delete(active, d)
	heights[d] = height
	return height, nil
}
func dynamicCompatible(actual, wanted DataType) bool {
	if actual.Equal(wanted) {
		return true
	}
	// text and varchar are protocol aliases handled by the same typed text codec.
	return (actual.Kind() == TypeText || actual.Kind() == TypeVarchar) && (wanted.Kind() == TypeText || wanted.Kind() == TypeVarchar)
}
func checkDynamicValue(t DataType, v Value, nested bool) error {
	if err := validateDataType(v.typ); err != nil {
		return err
	}
	if !dynamicCompatible(v.typ, t) {
		return typeValidation("value", "dynamic type %s does not match %s", v.typ, t)
	}
	switch v.state {
	case Present, Null:
	case Unset:
		if nested {
			return typeValidation("value", "unset is not permitted in nested values")
		}
	case Empty:
		if !emptySupported(t) {
			return typeValidation("value", "%s does not support Empty", t)
		}
	default:
		return typeValidation("value", "invalid state %d", v.state)
	}
	return nil
}
func dynamicAggregate(t DataType, kind Type, values []Value) (Value, error) {
	if err := DynamicCodec.CheckType(t); err != nil {
		return Value{}, err
	}
	return dynamicAggregateChecked(t, kind, values)
}
func dynamicAggregateChecked(t DataType, kind Type, values []Value) (Value, error) {
	if t.Kind() != kind {
		return Value{}, typeValidation("type", "expected %s, got %s", kind, t)
	}
	if kind == TypeCustom && len(values) != int(t.descriptor.dimension) {
		return Value{}, typeValidation("vector", "expected %d elements, got %d", t.descriptor.dimension, len(values))
	}
	if kind == TypeTuple && len(values) != t.descriptor.fields.Len() {
		return Value{}, typeValidation("tuple", "expected %d fields, got %d", t.descriptor.fields.Len(), len(values))
	}
	for i, v := range values {
		typ := t.descriptor.element
		if kind == TypeTuple {
			typ = t.descriptor.fields.columns[i].Type
		}
		if err := checkDynamicValue(typ, v, true); err != nil {
			return Value{}, fmt.Errorf("dynamic element %d: %w", i, err)
		}
		if (kind == TypeList || kind == TypeSet) && v.state == Null {
			return Value{}, typeValidation("element", "collection element %d cannot be null", i)
		}
		if kind == TypeCustom && v.state != Present {
			width, err := vectorFixedWidth(typ)
			if err != nil {
				return Value{}, err
			}
			if v.state != Empty || width != 0 {
				return Value{}, typeValidation("vector", "element %d must have a present body", i)
			}
		}
	}
	return Value{typ: t, values: values, count: len(values)}, nil
}
func ListValue(t DataType, v []Value) (Value, error)   { return dynamicAggregate(t, TypeList, v) }
func SetValue(t DataType, v []Value) (Value, error)    { return dynamicAggregate(t, TypeSet, v) }
func TupleValue(t DataType, v []Value) (Value, error)  { return dynamicAggregate(t, TypeTuple, v) }
func VectorValue(t DataType, v []Value) (Value, error) { return dynamicAggregate(t, TypeCustom, v) }
func MapValue(t DataType, entries []ValueEntry) (Value, error) {
	if err := DynamicCodec.CheckType(t); err != nil {
		return Value{}, err
	}
	return mapValueChecked(t, entries)
}
func mapValueChecked(t DataType, entries []ValueEntry) (Value, error) {
	if t.Kind() != TypeMap {
		return Value{}, typeValidation("type", "expected map, got %s", t)
	}
	for i, e := range entries {
		if e.Key.state == Null || e.Value.state == Null {
			return Value{}, typeValidation("element", "map entry %d cannot contain null", i)
		}
		if err := checkDynamicValue(t.descriptor.key, e.Key, true); err != nil {
			return Value{}, fmt.Errorf("dynamic map key %d: %w", i, err)
		}
		if err := checkDynamicValue(t.descriptor.element, e.Value, true); err != nil {
			return Value{}, fmt.Errorf("dynamic map value %d: %w", i, err)
		}
	}
	return Value{typ: t, entries: entries, count: len(entries)}, nil
}
func UDTValue(t DataType, fields []ValueField) (Value, error) {
	if err := DynamicCodec.CheckType(t); err != nil {
		return Value{}, err
	}
	return udtValueChecked(t, fields)
}
func udtValueChecked(t DataType, fields []ValueField) (Value, error) {
	if t.Kind() != TypeUDT {
		return Value{}, typeValidation("type", "expected UDT, got %s", t)
	}
	if len(fields) != t.descriptor.fields.Len() {
		return Value{}, typeValidation("fields", "expected %d UDT fields, got %d", t.descriptor.fields.Len(), len(fields))
	}
	for i, f := range fields {
		for j := 0; j < i; j++ {
			if fields[j].Name == f.Name {
				return Value{}, typeValidation("fields", "duplicate UDT field %q", f.Name)
			}
		}
		found := false
		for _, col := range t.descriptor.fields.columns {
			if col.Name == f.Name {
				found = true
				if err := checkDynamicValue(col.Type, f.Value, true); err != nil {
					return Value{}, fmt.Errorf("dynamic field %s: %w", f.Name, err)
				}
				break
			}
		}
		if !found {
			return Value{}, typeValidation("fields", "unknown UDT field %q", f.Name)
		}
	}
	return Value{typ: t, fields: fields, count: len(fields)}, nil
}

func (dynamicCodec) Clone(v Value) Value { return v.Clone() }
func (dynamicCodec) Encode(w *ValueWriter, v Value) error {
	if w == nil {
		return typeValidation("writer", "nil writer")
	}
	if err := DynamicCodec.CheckType(w.Type()); err != nil {
		return err
	}
	return encodeDynamicChecked(w, v)
}
func encodeDynamicChecked(w *ValueWriter, v Value) error {
	if err := checkDynamicValue(w.Type(), v, !w.allowUnset); err != nil {
		return err
	}
	switch v.state {
	case Null:
		return w.Null()
	case Unset:
		return w.Unset()
	case Empty:
		return w.emptyValue()
	}
	switch v.typ.Kind() {
	case TypeBoolean:
		return Bool.Encode(w, v.bits != 0)
	case TypeTinyInt:
		return Int8.Encode(w, int8(v.bits))
	case TypeSmallInt:
		return Int16.Encode(w, int16(v.bits))
	case TypeInt:
		return Int32.Encode(w, int32(v.bits))
	case TypeBigInt:
		return Int64.Encode(w, int64(v.bits))
	case TypeCounter:
		return CounterCodec.Encode(w, Counter(v.bits))
	case TypeFloat:
		return Float32.Encode(w, math.Float32frombits(uint32(v.bits)))
	case TypeDouble:
		return Float64.Encode(w, math.Float64frombits(v.bits))
	case TypeText, TypeVarchar:
		return TextCodec.Encode(w, Text{bytes: v.bytes})
	case TypeAscii:
		return ASCIICodec.Encode(w, Text{bytes: v.bytes})
	case TypeBlob:
		return BlobCodec.Encode(w, v.bytes)
	case TypeUUID:
		return UUIDCodec.Encode(w, v.uuid)
	case TypeTimeUUID:
		return TimeUUIDCodec.Encode(w, TimeUUID(v.uuid))
	case TypeInet:
		return InetCodec.Encode(w, v.inet)
	case TypeDate:
		return DateCodec.Encode(w, Date(v.bits))
	case TypeTime:
		return TimeCodec.Encode(w, TimeOfDay(v.bits))
	case TypeTimestamp:
		return TimestampCodec.Encode(w, Timestamp(v.bits))
	case TypeDuration:
		return DurationCodec.Encode(w, v.duration)
	case TypeVarint:
		return VarintCodec.Encode(w, Varint{bytes: v.bytes})
	case TypeDecimal:
		return DecimalCodec.Encode(w, Decimal{Scale: v.scale, Unscaled: Varint{bytes: v.bytes}})
	default:
		return encodeDynamicAggregate(w, v)
	}
}
func decodeDynamicScalar[T any](r ValueReader, c Decoder[T], makeValue func(T) Value) (Value, error) {
	payload, err := c.Decode(r)
	if err != nil {
		return Value{}, err
	}
	v := makeValue(payload)
	v.typ = r.Type()
	return v, nil
}
func (dynamicCodec) Decode(r ValueReader) (Value, error) {
	if err := DynamicCodec.CheckType(r.Type()); err != nil {
		return Value{}, err
	}
	return decodeDynamicChecked(r)
}
func decodeDynamicChecked(r ValueReader) (Value, error) { return decodeDynamicWithPlan(r, nil) }
func decodeDynamicWithPlan(r ValueReader, shape *streamValidationPlan) (Value, error) {
	if r.err != nil {
		return Value{}, r.err
	}
	if err := r.Bytes().checkAlive(); err != nil {
		return Value{}, err
	}
	switch r.State() {
	case Null:
		return NullValue(r.Type()), nil
	case Unset:
		return Value{}, typeValidation("state", "unset cannot be decoded as a result")
	case Empty:
		return Value{typ: r.Type(), state: Empty}, nil
	case Present:
	default:
		return Value{}, typeValidation("state", "invalid state %d", r.State())
	}
	if r.Bytes().Len() == 0 && emptySupported(r.Type()) {
		return Value{typ: r.Type(), state: Empty}, nil
	}
	switch r.Type().Kind() {
	case TypeBoolean:
		return decodeDynamicScalar(r, Bool, BoolValue)
	case TypeTinyInt:
		return decodeDynamicScalar(r, Int8, TinyIntValue)
	case TypeSmallInt:
		return decodeDynamicScalar(r, Int16, SmallIntValue)
	case TypeInt:
		return decodeDynamicScalar(r, Int32, IntValue)
	case TypeBigInt:
		return decodeDynamicScalar(r, Int64, BigIntValue)
	case TypeCounter:
		return decodeDynamicScalar(r, CounterCodec, CounterValue)
	case TypeFloat:
		return decodeDynamicScalar(r, Float32, FloatValue)
	case TypeDouble:
		return decodeDynamicScalar(r, Float64, DoubleValue)
	case TypeText, TypeVarchar, TypeAscii:
		codec := TextCodec
		if r.Type().Kind() == TypeAscii {
			codec = ASCIICodec
		}
		return decodeDynamicScalar(r, codec, func(t Text) Value { return Value{bytes: t.Bytes()} })
	case TypeBlob:
		return decodeDynamicScalar(r, BlobCodec, func(b Bytes) Value { return Value{bytes: b} })
	case TypeUUID:
		return decodeDynamicScalar(r, UUIDCodec, UUIDValue)
	case TypeTimeUUID:
		return decodeDynamicScalar(r, TimeUUIDCodec, func(u TimeUUID) Value { return Value{uuid: UUID(u)} })
	case TypeInet:
		return decodeDynamicScalar(r, InetCodec, func(a netip.Addr) Value { return Value{inet: a} })
	case TypeDate:
		return decodeDynamicScalar(r, DateCodec, DateValue)
	case TypeTime:
		return decodeDynamicScalar(r, TimeCodec, func(n TimeOfDay) Value { return Value{bits: uint64(n)} })
	case TypeTimestamp:
		return decodeDynamicScalar(r, TimestampCodec, TimestampValue)
	case TypeDuration:
		return decodeDynamicScalar(r, DurationCodec, func(d Duration) Value { return Value{duration: d} })
	case TypeVarint:
		return decodeDynamicScalar(r, VarintCodec, VarintValue)
	case TypeDecimal:
		return decodeDynamicScalar(r, DecimalCodec, DecimalValue)
	default:
		return decodeDynamicAggregate(r, shape)
	}
}

func decodeDynamicAggregate(r ValueReader, shape *streamValidationPlan) (Value, error) {
	v := Value{typ: r.Type(), encoded: true, shape: shape}
	var err error
	switch r.Type().Kind() {
	case TypeList, TypeSet, TypeCustom:
		if shape == nil {
			v.bytes, v.count, v.vectorWidth, err = validateSequenceBody(r, r.Type().Kind())
		} else {
			v.bytes, v.count, v.vectorWidth, err = validateSequenceLayout(r, r.Type().Kind(), r.Type().descriptor.element, shape.width)
		}
	case TypeMap:
		v.bytes, v.count, err = validateMapBody(r)
	case TypeTuple, TypeUDT:
		v.bytes = r.Bytes()
		v.count = r.Type().descriptor.fields.Len()
		cursor := r.cursorValue()
		for _, field := range r.Type().descriptor.fields.columns {
			if cursor.Remaining() == 0 {
				break
			} // Protocol permits omitted trailing members.
			if _, err = cursor.Child(field.Type); err != nil {
				return Value{}, err
			}
		}
		err = cursor.RequireEnd()
	default:
		return Value{}, typeValidation("type", "unsupported dynamic aggregate %s", r.Type())
	}
	if err != nil {
		return Value{}, err
	}
	return v, nil
}
func dynamicCursor(b Bytes) ValueCursor {
	return ValueCursor{cursor: *wire.NewCursor(b.chain), initialized: true, lease: b.lease, initial: b.Len()}
}
func dynamicField(c *ValueCursor, t DataType, shape *streamValidationPlan) (Value, error) {
	if err := c.check(); err != nil {
		return Value{}, err
	}
	if c.Remaining() == 0 {
		return NullValue(t), nil
	}
	r, err := c.Child(t)
	if err != nil {
		return Value{}, err
	}
	return decodeDynamicWithPlan(r, shape)
}

// Elements returns an independent iterator over list, set, vector, or tuple
// members. Tuple fields retain their physical ordinal and complete type.
func (v Value) Elements() (*ValueIterator[Value], error) {
	if err := v.require(TypeList, TypeSet, TypeCustom, TypeTuple); err != nil {
		return nil, err
	}
	if !v.encoded {
		return newValueIterator(len(v.values), func(i int) (Value, error) { return v.values[i], nil }, func(v Value) Value { return v.Clone() }), nil
	}
	typ := v.typ
	return &ValueIterator[Value]{clone: func(v Value) Value { return v.Clone() }, count: v.count, cursor: dynamicCursor(v.bytes), nextCursor: func(c *ValueCursor, i int) (Value, error) {
		if typ.Kind() == TypeTuple {
			return dynamicField(c, typ.descriptor.fields.columns[i].Type, dynamicShapeField(v.shape, i))
		}
		var r ValueReader
		var err error
		if typ.Kind() == TypeCustom {
			r, err = readVectorElement(c, typ.descriptor.element, v.vectorWidth)
		} else {
			r, err = c.Child(typ.descriptor.element)
		}
		if err != nil {
			return Value{}, err
		}
		return decodeDynamicWithPlan(r, dynamicShapeElement(v.shape))
	}}, nil
}
func (v Value) Entries() (*ValueIterator[ValueEntry], error) {
	if err := v.require(TypeMap); err != nil {
		return nil, err
	}
	if !v.encoded {
		return newValueIterator(len(v.entries), func(i int) (ValueEntry, error) { return v.entries[i], nil }, cloneDynamicEntry), nil
	}
	return &ValueIterator[ValueEntry]{clone: cloneDynamicEntry, count: v.count, cursor: dynamicCursor(v.bytes), nextCursor: func(c *ValueCursor, _ int) (ValueEntry, error) {
		key, err := c.Child(v.typ.descriptor.key)
		if err != nil {
			return ValueEntry{}, err
		}
		value, err := c.Child(v.typ.descriptor.element)
		if err != nil {
			return ValueEntry{}, err
		}
		k, err := decodeDynamicWithPlan(key, dynamicShapeKey(v.shape))
		if err != nil {
			return ValueEntry{}, err
		}
		valueDecoded, err := decodeDynamicWithPlan(value, dynamicShapeElement(v.shape))
		if err != nil {
			return ValueEntry{}, err
		}
		return ValueEntry{Key: k, Value: valueDecoded}, nil
	}}, nil
}
func (v Value) Fields() (*ValueIterator[ValueField], error) {
	if err := v.require(TypeTuple, TypeUDT); err != nil {
		return nil, err
	}
	columns := v.typ.descriptor.fields
	if !v.encoded {
		return newValueIterator(columns.Len(), func(i int) (ValueField, error) {
			field := columns.columns[i]
			if v.typ.Kind() == TypeTuple {
				if i >= len(v.values) {
					return ValueField{}, typeValidation("tuple", "missing field %d", i)
				}
				return ValueField{Name: field.Name, Value: v.values[i]}, nil
			}
			for _, f := range v.fields {
				if f.Name == field.Name {
					return f, nil
				}
			}
			return ValueField{}, typeValidation("fields", "missing UDT field %q", field.Name)
		}, cloneDynamicField), nil
	}
	return &ValueIterator[ValueField]{clone: cloneDynamicField, count: columns.Len(), cursor: dynamicCursor(v.bytes), nextCursor: func(c *ValueCursor, i int) (ValueField, error) {
		col := columns.columns[i]
		value, err := dynamicField(c, col.Type, dynamicShapeField(v.shape, i))
		if err != nil {
			return ValueField{}, err
		}
		return ValueField{Name: col.Name, Value: value}, nil
	}}, nil
}
func encodeDynamicChild(w *ValueWriter, typ DataType, v Value, shape *streamValidationPlan) error {
	v.shape = shape
	return w.Child(typ, func(child *ValueWriter) error { return encodeDynamicChecked(child, v) })
}
func encodeDynamicAggregate(w *ValueWriter, v Value) error {
	typ := w.Type()
	switch typ.Kind() {
	case TypeList, TypeSet, TypeCustom, TypeTuple:
		if !v.encoded {
			if _, err := dynamicAggregateChecked(v.typ, v.typ.Kind(), v.values); err != nil {
				return err
			}
		}
		iterator, err := v.Elements()
		if err != nil {
			return err
		}
		count := v.count
		if !v.encoded {
			count = len(v.values)
		}
		switch typ.Kind() {
		case TypeList, TypeSet:
			if uint64(count) > math.MaxInt32 {
				return typeValidation("count", "too many elements")
			}
			if err := w.Uint32(uint32(count)); err != nil {
				return err
			}
		case TypeTuple:
			if count != typ.descriptor.fields.Len() {
				return typeValidation("tuple", "wrong field count")
			}
		case TypeCustom:
			if count != int(typ.descriptor.dimension) {
				return typeValidation("vector", "wrong element count")
			}
		}
		var width int64
		if typ.Kind() == TypeCustom {
			if v.shape != nil {
				width = v.shape.width
			} else {
				width, err = vectorFixedWidth(typ.descriptor.element)
			}
			if err != nil {
				return err
			}
		}
		i := 0
		for iterator.Next() {
			value := iterator.Value()
			childType := typ.descriptor.element
			if typ.Kind() == TypeTuple {
				childType = typ.descriptor.fields.columns[i].Type
			}
			if typ.Kind() == TypeCustom {
				err = writeVectorElement(w, childType, width, checkedDynamicEncoder{typ: childType, shape: dynamicShapeChild(v.shape, childType)}, value)
			} else {
				err = encodeDynamicChild(w, childType, value, dynamicShapeChild(v.shape, childType))
			}
			if err != nil {
				return fmt.Errorf("dynamic element %d: %w", i, err)
			}
			i++
		}
		return iterator.Err()
	case TypeMap:
		if !v.encoded {
			if _, err := mapValueChecked(v.typ, v.entries); err != nil {
				return err
			}
		}
		count := v.count
		if !v.encoded {
			count = len(v.entries)
		}
		if uint64(count) > math.MaxInt32 {
			return typeValidation("count", "too many map entries")
		}
		iterator, err := v.Entries()
		if err != nil {
			return err
		}
		if err := w.Uint32(uint32(count)); err != nil {
			return err
		}
		for iterator.Next() {
			e := iterator.Value()
			if err := encodeDynamicChild(w, typ.descriptor.key, e.Key, dynamicShapeKey(v.shape)); err != nil {
				return err
			}
			if err := encodeDynamicChild(w, typ.descriptor.element, e.Value, dynamicShapeElement(v.shape)); err != nil {
				return err
			}
		}
		return iterator.Err()
	case TypeUDT:
		if !v.encoded {
			if _, err := udtValueChecked(v.typ, v.fields); err != nil {
				return err
			}
		}
		iterator, err := v.Fields()
		if err != nil {
			return err
		}
		index := 0
		for iterator.Next() {
			f := iterator.Value()
			col := typ.descriptor.fields.columns[index]
			if f.Name != col.Name {
				return typeValidation("fields", "expected %q, got %q", col.Name, f.Name)
			}
			if err := encodeDynamicChild(w, col.Type, f.Value, dynamicShapeField(v.shape, index)); err != nil {
				return fmt.Errorf("dynamic field %s: %w", f.Name, err)
			}
			index++
		}
		return iterator.Err()
	default:
		return typeValidation("type", "unsupported dynamic aggregate %s", typ)
	}
}

// DynamicParams binds explicitly typed values in physical marker order.
var DynamicParams ParamEncoder[[]Value] = dynamicParamsCodec{}

// NamedDynamicParams maps unique input names to server marker order. Repeated
// server markers reuse one input field; duplicate or unused input names fail.
var NamedDynamicParams ParamEncoder[[]ValueField] = namedDynamicParamsCodec{}

type dynamicParamsCodec struct{}
type dynamicParamsPlan struct {
	columns Columns
	shapes  []*streamValidationPlan
}
type namedDynamicParamsCodec struct{}
type namedDynamicParamsPlan struct {
	shapes  []*streamValidationPlan
	columns Columns
	names   []string
	lookup  map[string]int
	ordered []int
}

func checkDynamicColumns(columns Columns) error {
	for i, c := range columns.columns {
		if err := DynamicCodec.CheckType(c.Type); err != nil {
			return &CodecError{Column: c.Name, Index: i, Stage: "compile", Err: err}
		}
	}
	return nil
}
func (dynamicParamsCodec) Compile(columns Columns) (ParamPlan[[]Value], error) {
	if err := checkDynamicColumns(columns); err != nil {
		return nil, err
	}
	shapes, err := compileDynamicColumnShapes(columns)
	if err != nil {
		return nil, err
	}
	return dynamicParamsPlan{columns: columns, shapes: shapes}, nil
}
func (namedDynamicParamsCodec) Compile(columns Columns) (ParamPlan[[]ValueField], error) {
	if err := checkDynamicColumns(columns); err != nil {
		return nil, err
	}
	shapes, err := compileDynamicColumnShapes(columns)
	if err != nil {
		return nil, err
	}
	p := namedDynamicParamsPlan{columns: columns, lookup: make(map[string]int), ordered: make([]int, columns.Len()), shapes: shapes}
	for i, c := range columns.columns {
		if c.Name == "" {
			return nil, typeValidation("parameters", "marker %d has no name", i)
		}
		index, ok := p.lookup[c.Name]
		if !ok {
			index = len(p.names)
			p.lookup[c.Name] = index
			p.names = append(p.names, c.Name)
		}
		p.ordered[i] = index
	}
	return p, nil
}
func dynamicMetadataMatches(columns Columns, actual Columns, offset int) error {
	if offset < 0 || offset > actual.Len() || columns.Len() > actual.Len()-offset {
		return typeValidation("metadata", "not enough columns for compiled dynamic plan")
	}
	for i, col := range columns.columns {
		current := actual.columns[offset+i]
		if col.Name != current.Name || col.Keyspace != current.Keyspace || col.Table != current.Table || !col.Type.Equal(current.Type) {
			return typeValidation("metadata", "column %d differs from compiled dynamic plan", i)
		}
	}
	return nil
}
func (p dynamicParamsPlan) Encode(w *ParamWriter, values []Value) error {
	if w == nil || w.builder == nil {
		return typeValidation("writer", "nil parameter writer")
	}
	if len(values) != p.columns.Len() {
		return typeValidation("parameters", "expected %d values, got %d", p.columns.Len(), len(values))
	}
	if err := dynamicMetadataMatches(p.columns, w.columns, w.index); err != nil {
		return err
	}
	mark, start := w.builder.Mark(), w.index
	for _, v := range values {
		if err := WriteParam(w, checkedDynamicEncoder{typ: p.columns.columns[w.index-start].Type, shape: p.shapes[w.index-start]}, v); err != nil {
			w.builder.Rollback(mark)
			w.index = start
			return err
		}
	}
	return nil
}
func (p namedDynamicParamsPlan) Encode(w *ParamWriter, fields []ValueField) error {
	if w == nil || w.builder == nil {
		return typeValidation("writer", "nil parameter writer")
	}
	if err := dynamicMetadataMatches(p.columns, w.columns, w.index); err != nil {
		return err
	}
	positions := make([]int, len(p.names))
	for i := range positions {
		positions[i] = -1
	}
	for i, f := range fields {
		index, ok := p.lookup[f.Name]
		if !ok {
			return typeValidation("parameters", "unused input name %q", f.Name)
		}
		if positions[index] >= 0 {
			return typeValidation("parameters", "duplicate input name %q", f.Name)
		}
		positions[index] = i
	}
	for i, position := range positions {
		if position < 0 {
			return typeValidation("parameters", "missing input name %q", p.names[i])
		}
	}
	mark, start := w.builder.Mark(), w.index
	for _, index := range p.ordered {
		if err := WriteParam(w, checkedDynamicEncoder{typ: p.columns.columns[w.index-start].Type, shape: p.shapes[w.index-start]}, fields[positions[index]].Value); err != nil {
			w.builder.Rollback(mark)
			w.index = start
			return err
		}
	}
	return nil
}

// DynamicRows compiles a lazy row decoder retaining actual ordered metadata.
var DynamicRows RowDecoder[DynamicRow] = dynamicRowsCodec{}

type dynamicRowsCodec struct{}

func (dynamicRowsCodec) Clone(r DynamicRow) DynamicRow { return r.Clone() }

type dynamicRowsPlan struct {
	shapes  []*streamValidationPlan
	columns Columns
	lookup  map[string]int
}

// DynamicRow stores one encoded physical row. Its immutable name index is shared
// with the compiled plan; each access uses an independent cursor.
type DynamicRow struct {
	shapes       []*streamValidationPlan
	columns      Columns
	lookup       map[string]int
	body         Bytes
	allowMissing bool
}

func (r DynamicRow) Columns() Columns  { return r.columns }
func (r DynamicRow) Clone() DynamicRow { r.body = r.body.Clone(); return r }
func (r DynamicRow) At(index int) (Value, error) {
	if _, ok := r.columns.At(index); !ok {
		return Value{}, typeValidation("column", "ordinal %d outside %d columns", index, r.columns.Len())
	}
	if err := r.body.checkAlive(); err != nil {
		return Value{}, err
	}
	cursor := wire.NewCursor(r.body.chain)
	for i, col := range r.columns.columns {
		if r.allowMissing && cursor.Remaining() == 0 {
			if i == index {
				return NullValue(col.Type), nil
			}
			continue
		}
		value, err := readValue(cursor, col.Type, r.body.lease, false)
		if err == nil && i == index {
			var decoded Value
			decoded, err = decodeDynamicWithPlan(value, r.shapes[i])
			if err == nil {
				return decoded, nil
			}
		}
		if err != nil {
			return Value{}, &CodecError{Column: col.Name, Index: i, Stage: "decode", Err: err}
		}
	}
	return Value{}, typeValidation("column", "ordinal %d was not present", index)
}
func (r DynamicRow) Get(name string) (Value, error) {
	index, ok := r.lookup[name]
	if !ok {
		return Value{}, typeValidation("column", "unknown name %q", name)
	}
	if index < 0 {
		return Value{}, typeValidation("column", "duplicate result name %q requires ordinal access", name)
	}
	return r.At(index)
}
func (dynamicRowsCodec) Compile(columns Columns) (RowPlan[DynamicRow], error) {
	if err := checkDynamicColumns(columns); err != nil {
		return nil, err
	}
	p := dynamicRowsPlan{columns: columns, lookup: make(map[string]int, columns.Len()), shapes: make([]*streamValidationPlan, columns.Len())}
	for i, col := range columns.columns {
		shape, err := compileStreamValidation(col.Type, 0)
		if err != nil {
			return nil, err
		}
		p.shapes[i] = shape
		if _, exists := p.lookup[col.Name]; exists {
			p.lookup[col.Name] = -1
		} else {
			p.lookup[col.Name] = i
		}
	}
	return p, nil
}
func (p dynamicRowsPlan) Clone(r DynamicRow) DynamicRow { return r.Clone() }
func (p dynamicRowsPlan) DecodeInto(r *RowReader, dst *DynamicRow) error {
	if dst == nil {
		return typeValidation("destination", "nil dynamic row destination")
	}
	*dst = DynamicRow{}
	if r == nil || r.cursor == nil {
		return typeValidation("reader", "nil row reader")
	}
	if err := dynamicMetadataMatches(p.columns, r.columns, r.index); err != nil {
		return err
	}
	if r.lease != nil && !r.lease.Alive() {
		return ErrReleased
	}
	start := *r.cursor
	for range p.columns.columns {
		if _, err := r.nextValue(); err != nil {
			return err
		}
	}
	body, err := start.Take(start.Remaining() - r.cursor.Remaining())
	if err != nil {
		return err
	}
	dst.shapes = p.shapes
	dst.columns, dst.lookup, dst.body, dst.allowMissing = p.columns, p.lookup, Bytes{chain: body, lease: r.lease}, r.allowMissing
	return nil
}
func (p dynamicRowsPlan) Decode(r *RowReader) (DynamicRow, error) {
	var result DynamicRow
	err := p.DecodeInto(r, &result)
	if err != nil {
		return DynamicRow{}, err
	}
	return result, nil
}

type checkedDynamicDecoder struct {
	typ   DataType
	shape *streamValidationPlan
}

func (c checkedDynamicDecoder) CheckType(t DataType) error {
	if !c.typ.Equal(t) {
		return typeValidation("type", "compiled dynamic metadata changed")
	}
	return nil
}
func (c checkedDynamicDecoder) Decode(r ValueReader) (Value, error) {
	if err := c.CheckType(r.Type()); err != nil {
		return Value{}, err
	}
	return decodeDynamicWithPlan(r, c.shape)
}
func (c checkedDynamicDecoder) Clone(v Value) Value { return v.Clone() }
func (dynamicCodec) compileValueDecoder(t DataType) (Decoder[Value], error) {
	if err := DynamicCodec.CheckType(t); err != nil {
		return nil, err
	}
	shape, err := compileStreamValidation(t, 0)
	if err != nil {
		return nil, err
	}
	return checkedDynamicDecoder{typ: t, shape: shape}, nil
}

func cloneDynamicEntry(v ValueEntry) ValueEntry {
	return ValueEntry{Key: v.Key.Clone(), Value: v.Value.Clone()}
}
func cloneDynamicField(v ValueField) ValueField {
	return ValueField{Name: v.Name, Value: v.Value.Clone()}
}

type checkedDynamicEncoder struct {
	typ   DataType
	shape *streamValidationPlan
}

func (c checkedDynamicEncoder) CheckType(t DataType) error {
	if !c.typ.Equal(t) {
		return typeValidation("type", "compiled dynamic metadata changed")
	}
	return nil
}
func (c checkedDynamicEncoder) Encode(w *ValueWriter, v Value) error {
	if w == nil {
		return typeValidation("writer", "nil writer")
	}
	if err := c.CheckType(w.Type()); err != nil {
		return err
	}
	v.shape = c.shape
	return encodeDynamicChecked(w, v)
}
func (dynamicCodec) compileValueEncoder(t DataType) (Encoder[Value], error) {
	if err := DynamicCodec.CheckType(t); err != nil {
		return nil, err
	}
	shape, err := compileStreamValidation(t, 0)
	if err != nil {
		return nil, err
	}
	return checkedDynamicEncoder{typ: t, shape: shape}, nil
}

func dynamicShapeElement(p *streamValidationPlan) *streamValidationPlan {
	if p == nil {
		return nil
	}
	return p.element
}
func dynamicShapeKey(p *streamValidationPlan) *streamValidationPlan {
	if p == nil {
		return nil
	}
	return p.key
}
func dynamicShapeField(p *streamValidationPlan, index int) *streamValidationPlan {
	if p == nil || index < 0 || index >= len(p.fields) {
		return nil
	}
	return p.fields[index]
}
func dynamicShapeChild(p *streamValidationPlan, t DataType) *streamValidationPlan {
	if p == nil {
		return nil
	}
	return p.children[t.descriptor]
}

func compileDynamicColumnShapes(columns Columns) ([]*streamValidationPlan, error) {
	plans := make([]*streamValidationPlan, columns.Len())
	memo := make(map[*dataTypeDescriptor]*streamValidationPlan)
	for i, c := range columns.columns {
		p, err := compileStreamValidationMemo(c.Type, 0, memo)
		if err != nil {
			return nil, err
		}
		plans[i] = p
	}
	return plans, nil
}
