// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package codec

import (
	"fmt"
	"math"
	"net/netip"

	cqltype "github.com/scylladb/gocql/v2/internal/cqltype"
	"github.com/scylladb/gocql/v2/internal/wire"
)

// Value is an explicitly typed CQL value. Variable-length payloads and aggregate
// input descriptors are borrowed; Clone detaches them. The type and state select
// the active payload; no payload is stored in an interface.

type Value struct {
	typ         cqltype.DataType
	state       cqltype.CellState
	bits        uint64
	uuid        cqltype.UUID
	inet        netip.Addr
	duration    cqltype.Duration
	scale       int32
	bytes       cqltype.Bytes
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

func (v Value) Type() cqltype.DataType   { return v.typ }
func (v Value) State() cqltype.CellState { return v.state }

var dynamicScalarTypes = func() [cqltype.TypeDuration + 1]cqltype.DataType {
	var result [cqltype.TypeDuration + 1]cqltype.DataType
	for kind := cqltype.TypeAscii; kind <= cqltype.TypeDuration; kind++ {
		result[kind], _ = cqltype.ScalarType(kind)
	}
	return result
}()

func dynamicScalar(kind cqltype.Type) Value { return Value{typ: dynamicScalarTypes[kind]} }
func NullValue(t cqltype.DataType) Value    { return Value{typ: t, state: cqltype.Null} }
func UnsetDynamic(t cqltype.DataType) Value { return Value{typ: t, state: cqltype.Unset} }
func EmptyValue(t cqltype.DataType) (Value, error) {
	if err := DynamicCodec.CheckType(t); err != nil {
		return Value{}, err
	}
	if !emptySupported(t) {
		return Value{}, cqltype.TypeValidation("type", "%s does not support Empty", t)
	}
	return Value{typ: t, state: cqltype.Empty}, nil
}
func BoolValue(b bool) Value {
	v := dynamicScalar(cqltype.TypeBoolean)
	if b {
		v.bits = 1
	}
	return v
}
func TinyIntValue(n int8) Value {
	v := dynamicScalar(cqltype.TypeTinyInt)
	v.bits = uint64(n)
	return v
}
func SmallIntValue(n int16) Value {
	v := dynamicScalar(cqltype.TypeSmallInt)
	v.bits = uint64(n)
	return v
}
func IntValue(n int32) Value    { v := dynamicScalar(cqltype.TypeInt); v.bits = uint64(n); return v }
func BigIntValue(n int64) Value { v := dynamicScalar(cqltype.TypeBigInt); v.bits = uint64(n); return v }
func CounterValue(n Counter) Value {
	v := dynamicScalar(cqltype.TypeCounter)
	v.bits = uint64(n)
	return v
}
func FloatValue(n float32) Value {
	v := dynamicScalar(cqltype.TypeFloat)
	v.bits = uint64(math.Float32bits(n))
	return v
}
func DoubleValue(n float64) Value {
	v := dynamicScalar(cqltype.TypeDouble)
	v.bits = math.Float64bits(n)
	return v
}
func TextValue(s string) (Value, error) {
	text, err := cqltype.BorrowText(s)
	if err != nil {
		return Value{}, err
	}
	v := dynamicScalar(cqltype.TypeText)
	v.bytes = text.Bytes()
	return v, nil
}
func ASCIIValue(s string) (Value, error) {
	v := dynamicScalar(cqltype.TypeAscii)
	// ASCII validation is also UTF-8 validation; avoid a panicking text constructor.
	c, _ := wire.NewChain(wire.BorrowString(s))
	v.bytes = cqltype.BytesView(c, nil)
	if err := validateText(v.bytes, true); err != nil {
		return Value{}, err
	}
	return v, nil
}
func BlobValue(b []byte) Value {
	v := dynamicScalar(cqltype.TypeBlob)
	v.bytes = cqltype.BorrowBytes(b)
	return v
}
func UUIDValue(u cqltype.UUID) Value { v := dynamicScalar(cqltype.TypeUUID); v.uuid = u; return v }
func TimeUUIDValue(u TimeUUID) (Value, error) {
	if cqltype.UUID(u).Version() != 1 {
		return Value{}, cqltype.TypeValidation("timeuuid", "must have version 1")
	}
	v := dynamicScalar(cqltype.TypeTimeUUID)
	v.uuid = cqltype.UUID(u)
	return v, nil
}
func InetValue(a netip.Addr) (Value, error) {
	if !a.IsValid() || a.Zone() != "" {
		return Value{}, cqltype.TypeValidation("inet", "requires a valid address without a zone")
	}
	v := dynamicScalar(cqltype.TypeInet)
	v.inet = a
	return v, nil
}
func DateValue(n Date) Value { v := dynamicScalar(cqltype.TypeDate); v.bits = uint64(n); return v }
func TimeValue(n TimeOfDay) (Value, error) {
	if n < 0 || int64(n) >= nanosecondsPerDay {
		return Value{}, cqltype.TypeValidation("time", "outside one day")
	}
	v := dynamicScalar(cqltype.TypeTime)
	v.bits = uint64(n)
	return v, nil
}
func TimestampValue(n Timestamp) Value {
	v := dynamicScalar(cqltype.TypeTimestamp)
	v.bits = uint64(n)
	return v
}
func DurationValue(d cqltype.Duration) (Value, error) {
	if !durationSignsValid(d) {
		return Value{}, cqltype.TypeValidation("duration", "components must have compatible signs")
	}
	v := dynamicScalar(cqltype.TypeDuration)
	v.duration = d
	return v, nil
}
func VarintValue(n Varint) Value {
	v := dynamicScalar(cqltype.TypeVarint)
	v.bytes = cqltype.VarintBytes(n)
	return v
}
func DecimalValue(n Decimal) Value {
	v := dynamicScalar(cqltype.TypeDecimal)
	v.scale = n.Scale
	v.bytes = cqltype.VarintBytes(n.Unscaled)
	return v
}

func (v Value) require(kinds ...cqltype.Type) error {
	if err := cqltype.ValidateDataType(v.typ); err != nil {
		return err
	}
	if v.state != cqltype.Present {
		return cqltype.TypeValidation("value", "%s has state %d", v.typ, v.state)
	}
	for _, kind := range kinds {
		if v.typ.Kind() == kind {
			return cqltype.CheckBytesAlive(v.bytes)
		}
	}
	return cqltype.TypeValidation("value", "accessor does not accept %s", v.typ)
}
func (v Value) Bool() (bool, error) { err := v.require(cqltype.TypeBoolean); return v.bits != 0, err }
func (v Value) Signed() (int64, error) {
	err := v.require(cqltype.TypeTinyInt, cqltype.TypeSmallInt, cqltype.TypeInt, cqltype.TypeBigInt, cqltype.TypeCounter, cqltype.TypeDate, cqltype.TypeTime, cqltype.TypeTimestamp)
	return int64(v.bits), err
}
func (v Value) Float() (float64, error) {
	if err := v.require(cqltype.TypeFloat, cqltype.TypeDouble); err != nil {
		return 0, err
	}
	if v.typ.Kind() == cqltype.TypeFloat {
		return float64(math.Float32frombits(uint32(v.bits))), nil
	}
	return math.Float64frombits(v.bits), nil
}
func (v Value) Text() (cqltype.Text, error) {
	if err := v.require(cqltype.TypeText, cqltype.TypeVarchar, cqltype.TypeAscii); err != nil {
		return cqltype.Text{}, err
	}
	return cqltype.TextView(v.bytes), nil
}
func (v Value) Bytes() (cqltype.Bytes, error) {
	if err := v.require(cqltype.TypeBlob); err != nil {
		return cqltype.Bytes{}, err
	}
	return v.bytes, nil
}
func (v Value) UUID() (cqltype.UUID, error) {
	if err := v.require(cqltype.TypeUUID, cqltype.TypeTimeUUID); err != nil {
		return cqltype.UUID{}, err
	}
	return v.uuid, nil
}
func (v Value) Inet() (netip.Addr, error) {
	if err := v.require(cqltype.TypeInet); err != nil {
		return netip.Addr{}, err
	}
	return v.inet, nil
}
func (v Value) Duration() (cqltype.Duration, error) {
	if err := v.require(cqltype.TypeDuration); err != nil {
		return cqltype.Duration{}, err
	}
	return v.duration, nil
}
func (v Value) Varint() (Varint, error) {
	if err := v.require(cqltype.TypeVarint); err != nil {
		return Varint{}, err
	}
	return cqltype.VarintView(v.bytes), nil
}
func (v Value) Decimal() (Decimal, error) {
	if err := v.require(cqltype.TypeDecimal); err != nil {
		return Decimal{}, err
	}
	return Decimal{Scale: v.scale, Unscaled: cqltype.VarintView(v.bytes)}, nil
}

// Clone detaches a lazy aggregate by copying its encoded backing, without
// decoding its children. Explicit input descriptors are cloned recursively.
func (v Value) Clone() Value {
	if cqltype.BytesLease(v.bytes) != nil || v.bytes.Len() != 0 {
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

func (dynamicCodec) CheckType(t cqltype.DataType) error { return checkDynamicType(t, 0) }
func checkDynamicType(t cqltype.DataType, depth int) error {
	_, err := checkedTypeHeight(t, depth, make(map[*cqltype.DataTypeDescriptor]int), make(map[*cqltype.DataTypeDescriptor]bool))
	return err
}
func checkedTypeHeight(t cqltype.DataType, depth int, heights map[*cqltype.DataTypeDescriptor]int, active map[*cqltype.DataTypeDescriptor]bool) (int, error) {
	if err := cqltype.ValidateDataType(t); err != nil {
		return 0, err
	}
	if depth > 128 {
		return 0, cqltype.TypeValidation("type", "dynamic nesting exceeds 128")
	}
	d := cqltype.DescriptorOf(t)
	if height, ok := heights[d]; ok {
		if depth+height > 128 {
			return 0, cqltype.TypeValidation("type", "dynamic nesting exceeds 128")
		}
		return height, nil
	}
	if active[d] {
		return 0, cqltype.TypeValidation("type", "cyclic descriptor")
	}
	active[d] = true
	height := 0
	child := func(t cqltype.DataType) error {
		h, err := checkedTypeHeight(t, depth+1, heights, active)
		height = max(height, h+1)
		return err
	}
	var err error
	switch d.Kind {
	case cqltype.TypeList, cqltype.TypeSet:
		err = child(d.Element)
	case cqltype.TypeMap:
		err = child(d.Key)
		if err == nil {
			err = child(d.Element)
		}
	case cqltype.TypeTuple, cqltype.TypeUDT:
		if d.Kind == cqltype.TypeUDT && (d.Keyspace == "" || d.Name == "") {
			return 0, cqltype.TypeValidation("type", "UDT identity is incomplete")
		}
		names := make(map[string]bool)
		for i, f := range cqltype.ColumnSlice(d.Fields) {
			if d.Kind == cqltype.TypeUDT {
				if f.Name == "" || names[f.Name] {
					return 0, cqltype.TypeValidation("type", "empty or duplicate UDT field %q", f.Name)
				}
				names[f.Name] = true
			}
			if err = child(f.Type); err != nil {
				return 0, fmt.Errorf("dynamic field %d: %w", i, err)
			}
		}
	case cqltype.TypeCustom:
		if d.Dimension == 0 {
			return 0, cqltype.TypeValidation("type", "incomplete vector type")
		}
		err = child(d.Element)
		if err == nil {
			_, err = vectorFixedWidth(t)
		}
	default:
		_, err = cqltype.ScalarType(d.Kind)
	}
	if err != nil {
		return 0, err
	}
	delete(active, d)
	heights[d] = height
	return height, nil
}
func dynamicCompatible(actual, wanted cqltype.DataType) bool {
	if actual.Equal(wanted) {
		return true
	}
	// text and varchar are protocol aliases handled by the same typed text codec.
	return (actual.Kind() == cqltype.TypeText || actual.Kind() == cqltype.TypeVarchar) && (wanted.Kind() == cqltype.TypeText || wanted.Kind() == cqltype.TypeVarchar)
}
func checkDynamicValue(t cqltype.DataType, v Value, nested bool) error {
	if err := cqltype.ValidateDataType(v.typ); err != nil {
		return err
	}
	if !dynamicCompatible(v.typ, t) {
		return cqltype.TypeValidation("value", "dynamic type %s does not match %s", v.typ, t)
	}
	switch v.state {
	case cqltype.Present, cqltype.Null:
	case cqltype.Unset:
		if nested {
			return cqltype.TypeValidation("value", "unset is not permitted in nested values")
		}
	case cqltype.Empty:
		if !emptySupported(t) {
			return cqltype.TypeValidation("value", "%s does not support Empty", t)
		}
	default:
		return cqltype.TypeValidation("value", "invalid state %d", v.state)
	}
	return nil
}
func dynamicAggregate(t cqltype.DataType, kind cqltype.Type, values []Value) (Value, error) {
	if err := DynamicCodec.CheckType(t); err != nil {
		return Value{}, err
	}
	return dynamicAggregateChecked(t, kind, values)
}
func dynamicAggregateChecked(t cqltype.DataType, kind cqltype.Type, values []Value) (Value, error) {
	if t.Kind() != kind {
		return Value{}, cqltype.TypeValidation("type", "expected %s, got %s", kind, t)
	}
	if kind == cqltype.TypeCustom && len(values) != int(cqltype.DescriptorOf(t).Dimension) {
		return Value{}, cqltype.TypeValidation("vector", "expected %d elements, got %d", cqltype.DescriptorOf(t).Dimension, len(values))
	}
	if kind == cqltype.TypeTuple && len(values) != cqltype.DescriptorOf(t).Fields.Len() {
		return Value{}, cqltype.TypeValidation("tuple", "expected %d fields, got %d", cqltype.DescriptorOf(t).Fields.Len(), len(values))
	}
	for i, v := range values {
		typ := cqltype.DescriptorOf(t).Element
		if kind == cqltype.TypeTuple {
			typ = cqltype.ColumnSlice(cqltype.DescriptorOf(t).Fields)[i].Type
		}
		if err := checkDynamicValue(typ, v, true); err != nil {
			return Value{}, fmt.Errorf("dynamic element %d: %w", i, err)
		}
		if (kind == cqltype.TypeList || kind == cqltype.TypeSet) && v.state == cqltype.Null {
			return Value{}, cqltype.TypeValidation("element", "collection element %d cannot be null", i)
		}
		if kind == cqltype.TypeCustom && v.state != cqltype.Present {
			width, err := vectorFixedWidth(typ)
			if err != nil {
				return Value{}, err
			}
			if v.state != cqltype.Empty || width != 0 {
				return Value{}, cqltype.TypeValidation("vector", "element %d must have a present body", i)
			}
		}
	}
	return Value{typ: t, values: values, count: len(values)}, nil
}
func ListValue(t cqltype.DataType, v []Value) (Value, error) {
	return dynamicAggregate(t, cqltype.TypeList, v)
}
func SetValue(t cqltype.DataType, v []Value) (Value, error) {
	return dynamicAggregate(t, cqltype.TypeSet, v)
}
func TupleValue(t cqltype.DataType, v []Value) (Value, error) {
	return dynamicAggregate(t, cqltype.TypeTuple, v)
}
func VectorValue(t cqltype.DataType, v []Value) (Value, error) {
	return dynamicAggregate(t, cqltype.TypeCustom, v)
}
func MapValue(t cqltype.DataType, entries []ValueEntry) (Value, error) {
	if err := DynamicCodec.CheckType(t); err != nil {
		return Value{}, err
	}
	return mapValueChecked(t, entries)
}
func mapValueChecked(t cqltype.DataType, entries []ValueEntry) (Value, error) {
	if t.Kind() != cqltype.TypeMap {
		return Value{}, cqltype.TypeValidation("type", "expected map, got %s", t)
	}
	for i, e := range entries {
		if e.Key.state == cqltype.Null || e.Value.state == cqltype.Null {
			return Value{}, cqltype.TypeValidation("element", "map entry %d cannot contain null", i)
		}
		if err := checkDynamicValue(cqltype.DescriptorOf(t).Key, e.Key, true); err != nil {
			return Value{}, fmt.Errorf("dynamic map key %d: %w", i, err)
		}
		if err := checkDynamicValue(cqltype.DescriptorOf(t).Element, e.Value, true); err != nil {
			return Value{}, fmt.Errorf("dynamic map value %d: %w", i, err)
		}
	}
	return Value{typ: t, entries: entries, count: len(entries)}, nil
}
func UDTValue(t cqltype.DataType, fields []ValueField) (Value, error) {
	if err := DynamicCodec.CheckType(t); err != nil {
		return Value{}, err
	}
	return udtValueChecked(t, fields)
}
func udtValueChecked(t cqltype.DataType, fields []ValueField) (Value, error) {
	if t.Kind() != cqltype.TypeUDT {
		return Value{}, cqltype.TypeValidation("type", "expected UDT, got %s", t)
	}
	if len(fields) != cqltype.DescriptorOf(t).Fields.Len() {
		return Value{}, cqltype.TypeValidation("fields", "expected %d UDT fields, got %d", cqltype.DescriptorOf(t).Fields.Len(), len(fields))
	}
	for i, f := range fields {
		for j := 0; j < i; j++ {
			if fields[j].Name == f.Name {
				return Value{}, cqltype.TypeValidation("fields", "duplicate UDT field %q", f.Name)
			}
		}
		found := false
		for _, col := range cqltype.ColumnSlice(cqltype.DescriptorOf(t).Fields) {
			if col.Name == f.Name {
				found = true
				if err := checkDynamicValue(col.Type, f.Value, true); err != nil {
					return Value{}, fmt.Errorf("dynamic field %s: %w", f.Name, err)
				}
				break
			}
		}
		if !found {
			return Value{}, cqltype.TypeValidation("fields", "unknown UDT field %q", f.Name)
		}
	}
	return Value{typ: t, fields: fields, count: len(fields)}, nil
}

func (dynamicCodec) Clone(v Value) Value { return v.Clone() }
func (dynamicCodec) Encode(w *ValueWriter, v Value) error {
	if w == nil {
		return cqltype.TypeValidation("writer", "nil writer")
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
	case cqltype.Null:
		return w.Null()
	case cqltype.Unset:
		return w.Unset()
	case cqltype.Empty:
		return w.emptyValue()
	}
	switch v.typ.Kind() {
	case cqltype.TypeBoolean:
		return Bool.Encode(w, v.bits != 0)
	case cqltype.TypeTinyInt:
		return Int8.Encode(w, int8(v.bits))
	case cqltype.TypeSmallInt:
		return Int16.Encode(w, int16(v.bits))
	case cqltype.TypeInt:
		return Int32.Encode(w, int32(v.bits))
	case cqltype.TypeBigInt:
		return Int64.Encode(w, int64(v.bits))
	case cqltype.TypeCounter:
		return CounterCodec.Encode(w, Counter(v.bits))
	case cqltype.TypeFloat:
		return Float32.Encode(w, math.Float32frombits(uint32(v.bits)))
	case cqltype.TypeDouble:
		return Float64.Encode(w, math.Float64frombits(v.bits))
	case cqltype.TypeText, cqltype.TypeVarchar:
		return TextCodec.Encode(w, cqltype.TextView(v.bytes))
	case cqltype.TypeAscii:
		return ASCIICodec.Encode(w, cqltype.TextView(v.bytes))
	case cqltype.TypeBlob:
		return BlobCodec.Encode(w, v.bytes)
	case cqltype.TypeUUID:
		return UUIDCodec.Encode(w, v.uuid)
	case cqltype.TypeTimeUUID:
		return TimeUUIDCodec.Encode(w, TimeUUID(v.uuid))
	case cqltype.TypeInet:
		return InetCodec.Encode(w, v.inet)
	case cqltype.TypeDate:
		return DateCodec.Encode(w, Date(v.bits))
	case cqltype.TypeTime:
		return TimeCodec.Encode(w, TimeOfDay(v.bits))
	case cqltype.TypeTimestamp:
		return TimestampCodec.Encode(w, Timestamp(v.bits))
	case cqltype.TypeDuration:
		return DurationCodec.Encode(w, v.duration)
	case cqltype.TypeVarint:
		return VarintCodec.Encode(w, cqltype.VarintView(v.bytes))
	case cqltype.TypeDecimal:
		return DecimalCodec.Encode(w, Decimal{Scale: v.scale, Unscaled: cqltype.VarintView(v.bytes)})
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
	if err := cqltype.CheckBytesAlive(r.Bytes()); err != nil {
		return Value{}, err
	}
	switch r.State() {
	case cqltype.Null:
		return NullValue(r.Type()), nil
	case cqltype.Unset:
		return Value{}, cqltype.TypeValidation("state", "unset cannot be decoded as a result")
	case cqltype.Empty:
		return Value{typ: r.Type(), state: cqltype.Empty}, nil
	case cqltype.Present:
	default:
		return Value{}, cqltype.TypeValidation("state", "invalid state %d", r.State())
	}
	if r.Bytes().Len() == 0 && emptySupported(r.Type()) {
		return Value{typ: r.Type(), state: cqltype.Empty}, nil
	}
	switch r.Type().Kind() {
	case cqltype.TypeBoolean:
		return decodeDynamicScalar(r, Bool, BoolValue)
	case cqltype.TypeTinyInt:
		return decodeDynamicScalar(r, Int8, TinyIntValue)
	case cqltype.TypeSmallInt:
		return decodeDynamicScalar(r, Int16, SmallIntValue)
	case cqltype.TypeInt:
		return decodeDynamicScalar(r, Int32, IntValue)
	case cqltype.TypeBigInt:
		return decodeDynamicScalar(r, Int64, BigIntValue)
	case cqltype.TypeCounter:
		return decodeDynamicScalar(r, CounterCodec, CounterValue)
	case cqltype.TypeFloat:
		return decodeDynamicScalar(r, Float32, FloatValue)
	case cqltype.TypeDouble:
		return decodeDynamicScalar(r, Float64, DoubleValue)
	case cqltype.TypeText, cqltype.TypeVarchar, cqltype.TypeAscii:
		codec := TextCodec
		if r.Type().Kind() == cqltype.TypeAscii {
			codec = ASCIICodec
		}
		return decodeDynamicScalar(r, codec, func(t cqltype.Text) Value { return Value{bytes: t.Bytes()} })
	case cqltype.TypeBlob:
		return decodeDynamicScalar(r, BlobCodec, func(b cqltype.Bytes) Value { return Value{bytes: b} })
	case cqltype.TypeUUID:
		return decodeDynamicScalar(r, UUIDCodec, UUIDValue)
	case cqltype.TypeTimeUUID:
		return decodeDynamicScalar(r, TimeUUIDCodec, func(u TimeUUID) Value { return Value{uuid: cqltype.UUID(u)} })
	case cqltype.TypeInet:
		return decodeDynamicScalar(r, InetCodec, func(a netip.Addr) Value { return Value{inet: a} })
	case cqltype.TypeDate:
		return decodeDynamicScalar(r, DateCodec, DateValue)
	case cqltype.TypeTime:
		return decodeDynamicScalar(r, TimeCodec, func(n TimeOfDay) Value { return Value{bits: uint64(n)} })
	case cqltype.TypeTimestamp:
		return decodeDynamicScalar(r, TimestampCodec, TimestampValue)
	case cqltype.TypeDuration:
		return decodeDynamicScalar(r, DurationCodec, func(d cqltype.Duration) Value { return Value{duration: d} })
	case cqltype.TypeVarint:
		return decodeDynamicScalar(r, VarintCodec, VarintValue)
	case cqltype.TypeDecimal:
		return decodeDynamicScalar(r, DecimalCodec, DecimalValue)
	default:
		return decodeDynamicAggregate(r, shape)
	}
}

func decodeDynamicAggregate(r ValueReader, shape *streamValidationPlan) (Value, error) {
	v := Value{typ: r.Type(), encoded: true, shape: shape}
	var err error
	switch r.Type().Kind() {
	case cqltype.TypeList, cqltype.TypeSet, cqltype.TypeCustom:
		if shape == nil {
			v.bytes, v.count, v.vectorWidth, err = validateSequenceBody(r, r.Type().Kind())
		} else {
			v.bytes, v.count, v.vectorWidth, err = validateSequenceLayout(r, r.Type().Kind(), cqltype.DescriptorOf(r.Type()).Element, shape.width)
		}
	case cqltype.TypeMap:
		v.bytes, v.count, err = validateMapBody(r)
	case cqltype.TypeTuple, cqltype.TypeUDT:
		v.bytes = r.Bytes()
		v.count = cqltype.DescriptorOf(r.Type()).Fields.Len()
		cursor := r.cursorValue()
		for _, field := range cqltype.ColumnSlice(cqltype.DescriptorOf(r.Type()).Fields) {
			if cursor.Remaining() == 0 {
				break
			} // Protocol permits omitted trailing members.
			if _, err = cursor.Child(field.Type); err != nil {
				return Value{}, err
			}
		}
		err = cursor.RequireEnd()
	default:
		return Value{}, cqltype.TypeValidation("type", "unsupported dynamic aggregate %s", r.Type())
	}
	if err != nil {
		return Value{}, err
	}
	return v, nil
}
func dynamicCursor(b cqltype.Bytes) ValueCursor {
	return ValueCursor{cursor: *wire.NewCursor(cqltype.BytesChain(b)), initialized: true, lease: cqltype.BytesLease(b), initial: b.Len()}
}
func dynamicField(c *ValueCursor, t cqltype.DataType, shape *streamValidationPlan) (Value, error) {
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
	if err := v.require(cqltype.TypeList, cqltype.TypeSet, cqltype.TypeCustom, cqltype.TypeTuple); err != nil {
		return nil, err
	}
	if !v.encoded {
		return newValueIterator(len(v.values), func(i int) (Value, error) { return v.values[i], nil }, func(v Value) Value { return v.Clone() }), nil
	}
	typ := v.typ
	return &ValueIterator[Value]{clone: func(v Value) Value { return v.Clone() }, count: v.count, cursor: dynamicCursor(v.bytes), nextCursor: func(c *ValueCursor, i int) (Value, error) {
		if typ.Kind() == cqltype.TypeTuple {
			return dynamicField(c, cqltype.ColumnSlice(cqltype.DescriptorOf(typ).Fields)[i].Type, dynamicShapeField(v.shape, i))
		}
		var r ValueReader
		var err error
		if typ.Kind() == cqltype.TypeCustom {
			r, err = readVectorElement(c, cqltype.DescriptorOf(typ).Element, v.vectorWidth)
		} else {
			r, err = c.Child(cqltype.DescriptorOf(typ).Element)
		}
		if err != nil {
			return Value{}, err
		}
		return decodeDynamicWithPlan(r, dynamicShapeElement(v.shape))
	}}, nil
}
func (v Value) Entries() (*ValueIterator[ValueEntry], error) {
	if err := v.require(cqltype.TypeMap); err != nil {
		return nil, err
	}
	if !v.encoded {
		return newValueIterator(len(v.entries), func(i int) (ValueEntry, error) { return v.entries[i], nil }, cloneDynamicEntry), nil
	}
	return &ValueIterator[ValueEntry]{clone: cloneDynamicEntry, count: v.count, cursor: dynamicCursor(v.bytes), nextCursor: func(c *ValueCursor, _ int) (ValueEntry, error) {
		key, err := c.Child(cqltype.DescriptorOf(v.typ).Key)
		if err != nil {
			return ValueEntry{}, err
		}
		value, err := c.Child(cqltype.DescriptorOf(v.typ).Element)
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
	if err := v.require(cqltype.TypeTuple, cqltype.TypeUDT); err != nil {
		return nil, err
	}
	columns := cqltype.DescriptorOf(v.typ).Fields
	if !v.encoded {
		return newValueIterator(columns.Len(), func(i int) (ValueField, error) {
			field := cqltype.ColumnSlice(columns)[i]
			if v.typ.Kind() == cqltype.TypeTuple {
				if i >= len(v.values) {
					return ValueField{}, cqltype.TypeValidation("tuple", "missing field %d", i)
				}
				return ValueField{Name: field.Name, Value: v.values[i]}, nil
			}
			for _, f := range v.fields {
				if f.Name == field.Name {
					return f, nil
				}
			}
			return ValueField{}, cqltype.TypeValidation("fields", "missing UDT field %q", field.Name)
		}, cloneDynamicField), nil
	}
	return &ValueIterator[ValueField]{clone: cloneDynamicField, count: columns.Len(), cursor: dynamicCursor(v.bytes), nextCursor: func(c *ValueCursor, i int) (ValueField, error) {
		col := cqltype.ColumnSlice(columns)[i]
		value, err := dynamicField(c, col.Type, dynamicShapeField(v.shape, i))
		if err != nil {
			return ValueField{}, err
		}
		return ValueField{Name: col.Name, Value: value}, nil
	}}, nil
}
func encodeDynamicChild(w *ValueWriter, typ cqltype.DataType, v Value, shape *streamValidationPlan) error {
	v.shape = shape
	return w.Child(typ, func(child *ValueWriter) error { return encodeDynamicChecked(child, v) })
}
func encodeDynamicAggregate(w *ValueWriter, v Value) error {
	typ := w.Type()
	switch typ.Kind() {
	case cqltype.TypeList, cqltype.TypeSet, cqltype.TypeCustom, cqltype.TypeTuple:
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
		case cqltype.TypeList, cqltype.TypeSet:
			if uint64(count) > math.MaxInt32 {
				return cqltype.TypeValidation("count", "too many elements")
			}
			if err := w.Uint32(uint32(count)); err != nil {
				return err
			}
		case cqltype.TypeTuple:
			if count != cqltype.DescriptorOf(typ).Fields.Len() {
				return cqltype.TypeValidation("tuple", "wrong field count")
			}
		case cqltype.TypeCustom:
			if count != int(cqltype.DescriptorOf(typ).Dimension) {
				return cqltype.TypeValidation("vector", "wrong element count")
			}
		}
		var width int64
		if typ.Kind() == cqltype.TypeCustom {
			if v.shape != nil {
				width = v.shape.width
			} else {
				width, err = vectorFixedWidth(cqltype.DescriptorOf(typ).Element)
			}
			if err != nil {
				return err
			}
		}
		i := 0
		for iterator.Next() {
			value := iterator.Value()
			childType := cqltype.DescriptorOf(typ).Element
			if typ.Kind() == cqltype.TypeTuple {
				childType = cqltype.ColumnSlice(cqltype.DescriptorOf(typ).Fields)[i].Type
			}
			if typ.Kind() == cqltype.TypeCustom {
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
	case cqltype.TypeMap:
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
			return cqltype.TypeValidation("count", "too many map entries")
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
			if err := encodeDynamicChild(w, cqltype.DescriptorOf(typ).Key, e.Key, dynamicShapeKey(v.shape)); err != nil {
				return err
			}
			if err := encodeDynamicChild(w, cqltype.DescriptorOf(typ).Element, e.Value, dynamicShapeElement(v.shape)); err != nil {
				return err
			}
		}
		return iterator.Err()
	case cqltype.TypeUDT:
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
			col := cqltype.ColumnSlice(cqltype.DescriptorOf(typ).Fields)[index]
			if f.Name != col.Name {
				return cqltype.TypeValidation("fields", "expected %q, got %q", col.Name, f.Name)
			}
			if err := encodeDynamicChild(w, col.Type, f.Value, dynamicShapeField(v.shape, index)); err != nil {
				return fmt.Errorf("dynamic field %s: %w", f.Name, err)
			}
			index++
		}
		return iterator.Err()
	default:
		return cqltype.TypeValidation("type", "unsupported dynamic aggregate %s", typ)
	}
}

// DynamicParams binds explicitly typed values in physical marker order.
var DynamicParams ParamEncoder[[]Value] = dynamicParamsCodec{}

// NamedDynamicParams maps unique input names to server marker order. Repeated
// server markers reuse one input field; duplicate or unused input names fail.
var NamedDynamicParams ParamEncoder[[]ValueField] = namedDynamicParamsCodec{}

type dynamicParamsCodec struct{}
type dynamicParamsPlan struct {
	columns cqltype.Columns
	shapes  []*streamValidationPlan
}
type namedDynamicParamsCodec struct{}
type namedDynamicParamsPlan struct {
	shapes  []*streamValidationPlan
	columns cqltype.Columns
	names   []string
	lookup  map[string]int
	ordered []int
}

func checkDynamicColumns(columns cqltype.Columns) error {
	for i, c := range cqltype.ColumnSlice(columns) {
		if err := DynamicCodec.CheckType(c.Type); err != nil {
			return &cqltype.CodecError{Column: c.Name, Index: i, Stage: "compile", Err: err}
		}
	}
	return nil
}
func (dynamicParamsCodec) Compile(columns cqltype.Columns) (ParamPlan[[]Value], error) {
	if err := checkDynamicColumns(columns); err != nil {
		return nil, err
	}
	shapes, err := compileDynamicColumnShapes(columns)
	if err != nil {
		return nil, err
	}
	return dynamicParamsPlan{columns: columns, shapes: shapes}, nil
}
func (namedDynamicParamsCodec) Compile(columns cqltype.Columns) (ParamPlan[[]ValueField], error) {
	if err := checkDynamicColumns(columns); err != nil {
		return nil, err
	}
	shapes, err := compileDynamicColumnShapes(columns)
	if err != nil {
		return nil, err
	}
	p := namedDynamicParamsPlan{columns: columns, lookup: make(map[string]int), ordered: make([]int, columns.Len()), shapes: shapes}
	for i, c := range cqltype.ColumnSlice(columns) {
		if c.Name == "" {
			return nil, cqltype.TypeValidation("parameters", "marker %d has no name", i)
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
func dynamicMetadataMatches(columns cqltype.Columns, actual cqltype.Columns, offset int) error {
	if offset < 0 || offset > actual.Len() || columns.Len() > actual.Len()-offset {
		return cqltype.TypeValidation("metadata", "not enough columns for compiled dynamic plan")
	}
	for i, col := range cqltype.ColumnSlice(columns) {
		current := cqltype.ColumnSlice(actual)[offset+i]
		if col.Name != current.Name || col.Keyspace != current.Keyspace || col.Table != current.Table || !col.Type.Equal(current.Type) {
			return cqltype.TypeValidation("metadata", "column %d differs from compiled dynamic plan", i)
		}
	}
	return nil
}
func (p dynamicParamsPlan) Encode(w *ParamWriter, values []Value) error {
	if w == nil || w.builder == nil {
		return cqltype.TypeValidation("writer", "nil parameter writer")
	}
	if len(values) != p.columns.Len() {
		return cqltype.TypeValidation("parameters", "expected %d values, got %d", p.columns.Len(), len(values))
	}
	if err := dynamicMetadataMatches(p.columns, w.columns, w.index); err != nil {
		return err
	}
	mark, start := w.builder.Mark(), w.index
	for _, v := range values {
		if err := WriteParam(w, checkedDynamicEncoder{typ: cqltype.ColumnSlice(p.columns)[w.index-start].Type, shape: p.shapes[w.index-start]}, v); err != nil {
			w.builder.Rollback(mark)
			w.index = start
			return err
		}
	}
	return nil
}
func (p namedDynamicParamsPlan) Encode(w *ParamWriter, fields []ValueField) error {
	if w == nil || w.builder == nil {
		return cqltype.TypeValidation("writer", "nil parameter writer")
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
			return cqltype.TypeValidation("parameters", "unused input name %q", f.Name)
		}
		if positions[index] >= 0 {
			return cqltype.TypeValidation("parameters", "duplicate input name %q", f.Name)
		}
		positions[index] = i
	}
	for i, position := range positions {
		if position < 0 {
			return cqltype.TypeValidation("parameters", "missing input name %q", p.names[i])
		}
	}
	mark, start := w.builder.Mark(), w.index
	for _, index := range p.ordered {
		if err := WriteParam(w, checkedDynamicEncoder{typ: cqltype.ColumnSlice(p.columns)[w.index-start].Type, shape: p.shapes[w.index-start]}, fields[positions[index]].Value); err != nil {
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
	columns cqltype.Columns
	lookup  map[string]int
}

// DynamicRow stores one encoded physical row. Its immutable name index is shared
// with the compiled plan; each access uses an independent cursor.
type DynamicRow struct {
	shapes       []*streamValidationPlan
	columns      cqltype.Columns
	lookup       map[string]int
	body         cqltype.Bytes
	allowMissing bool
}

func (r DynamicRow) Columns() cqltype.Columns { return r.columns }
func (r DynamicRow) Clone() DynamicRow        { r.body = r.body.Clone(); return r }
func (r DynamicRow) At(index int) (Value, error) {
	if _, ok := r.columns.At(index); !ok {
		return Value{}, cqltype.TypeValidation("column", "ordinal %d outside %d columns", index, r.columns.Len())
	}
	if err := cqltype.CheckBytesAlive(r.body); err != nil {
		return Value{}, err
	}
	cursor := wire.NewCursor(cqltype.BytesChain(r.body))
	for i, col := range cqltype.ColumnSlice(r.columns) {
		if r.allowMissing && cursor.Remaining() == 0 {
			if i == index {
				return NullValue(col.Type), nil
			}
			continue
		}
		value, err := ReadValue(cursor, col.Type, cqltype.BytesLease(r.body), false)
		if err == nil && i == index {
			var decoded Value
			decoded, err = decodeDynamicWithPlan(value, r.shapes[i])
			if err == nil {
				return decoded, nil
			}
		}
		if err != nil {
			return Value{}, &cqltype.CodecError{Column: col.Name, Index: i, Stage: "decode", Err: err}
		}
	}
	return Value{}, cqltype.TypeValidation("column", "ordinal %d was not present", index)
}
func (r DynamicRow) Get(name string) (Value, error) {
	index, ok := r.lookup[name]
	if !ok {
		return Value{}, cqltype.TypeValidation("column", "unknown name %q", name)
	}
	if index < 0 {
		return Value{}, cqltype.TypeValidation("column", "duplicate result name %q requires ordinal access", name)
	}
	return r.At(index)
}
func (dynamicRowsCodec) Compile(columns cqltype.Columns) (RowPlan[DynamicRow], error) {
	if err := checkDynamicColumns(columns); err != nil {
		return nil, err
	}
	p := dynamicRowsPlan{columns: columns, lookup: make(map[string]int, columns.Len()), shapes: make([]*streamValidationPlan, columns.Len())}
	for i, col := range cqltype.ColumnSlice(columns) {
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
		return cqltype.TypeValidation("destination", "nil dynamic row destination")
	}
	*dst = DynamicRow{}
	if r == nil || r.cursor == nil {
		return cqltype.TypeValidation("reader", "nil row reader")
	}
	if err := dynamicMetadataMatches(p.columns, r.columns, r.index); err != nil {
		return err
	}
	if r.lease != nil && !r.lease.Alive() {
		return cqltype.ErrReleased
	}
	start := *r.cursor
	for range cqltype.ColumnSlice(p.columns) {
		if _, err := r.nextValue(); err != nil {
			return err
		}
	}
	body, err := start.Take(start.Remaining() - r.cursor.Remaining())
	if err != nil {
		return err
	}
	dst.shapes = p.shapes
	dst.columns, dst.lookup, dst.body, dst.allowMissing = p.columns, p.lookup, cqltype.BytesView(body, r.lease), r.allowMissing
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
	typ   cqltype.DataType
	shape *streamValidationPlan
}

func (c checkedDynamicDecoder) CheckType(t cqltype.DataType) error {
	if !c.typ.Equal(t) {
		return cqltype.TypeValidation("type", "compiled dynamic metadata changed")
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
func (dynamicCodec) compileValueDecoder(t cqltype.DataType) (Decoder[Value], error) {
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
	typ   cqltype.DataType
	shape *streamValidationPlan
}

func (c checkedDynamicEncoder) CheckType(t cqltype.DataType) error {
	if !c.typ.Equal(t) {
		return cqltype.TypeValidation("type", "compiled dynamic metadata changed")
	}
	return nil
}
func (c checkedDynamicEncoder) Encode(w *ValueWriter, v Value) error {
	if w == nil {
		return cqltype.TypeValidation("writer", "nil writer")
	}
	if err := c.CheckType(w.Type()); err != nil {
		return err
	}
	v.shape = c.shape
	return encodeDynamicChecked(w, v)
}
func (dynamicCodec) compileValueEncoder(t cqltype.DataType) (Encoder[Value], error) {
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
func dynamicShapeChild(p *streamValidationPlan, t cqltype.DataType) *streamValidationPlan {
	if p == nil {
		return nil
	}
	return p.children[cqltype.DescriptorOf(t)]
}

func compileDynamicColumnShapes(columns cqltype.Columns) ([]*streamValidationPlan, error) {
	plans := make([]*streamValidationPlan, columns.Len())
	memo := make(map[*cqltype.DataTypeDescriptor]*streamValidationPlan)
	for i, c := range cqltype.ColumnSlice(columns) {
		p, err := compileStreamValidationMemo(c.Type, 0, memo)
		if err != nil {
			return nil, err
		}
		plans[i] = p
	}
	return plans, nil
}
