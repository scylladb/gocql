// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package gocql

import (
	"encoding/binary"
	"fmt"
	"math"
	"strings"
)

// DataType is an immutable, complete CQL type descriptor. Its zero value is invalid.
type DataType struct{ descriptor *dataTypeDescriptor }
type dataTypeDescriptor struct {
	kind           Type
	frozen         bool
	frozenKnown    bool
	element, key   DataType
	fields         Columns
	keyspace, name string
	dimension      uint16
}

// ColumnSpec identifies one physical column and its complete type.
type ColumnSpec struct {
	Keyspace, Table, Name string
	Type                  DataType
}

// Columns is an immutable, ordered set of column specifications.
type Columns struct{ columns []ColumnSpec }

// ScalarType constructs a supported scalar descriptor.
func ScalarType(kind Type) (DataType, error) {
	if kind < TypeAscii || kind > TypeDuration {
		return DataType{}, typeValidation("kind", "unsupported scalar kind %v", kind)
	}
	return DataType{descriptor: &dataTypeDescriptor{kind: kind}}, nil
}
func ListType(element DataType) (DataType, error) { return collectionDataType(TypeList, element) }
func SetType(element DataType) (DataType, error)  { return collectionDataType(TypeSet, element) }
func collectionDataType(kind Type, element DataType) (DataType, error) {
	if err := validateDataType(element); err != nil {
		return DataType{}, err
	}
	return DataType{descriptor: &dataTypeDescriptor{kind: kind, element: element}}, nil
}
func MapType(key, value DataType) (DataType, error) {
	if err := validateDataType(key); err != nil {
		return DataType{}, err
	}
	if err := validateDataType(value); err != nil {
		return DataType{}, err
	}
	return DataType{descriptor: &dataTypeDescriptor{kind: TypeMap, key: key, element: value}}, nil
}
func TupleType(fields ...DataType) (DataType, error) {
	if len(fields) == 0 || len(fields) > math.MaxUint16 {
		return DataType{}, typeValidation("fields", "tuple requires 1..65535 fields")
	}
	columns := make([]ColumnSpec, len(fields))
	for i, t := range fields {
		if err := validateDataType(t); err != nil {
			return DataType{}, fmt.Errorf("tuple field %d: %w", i, err)
		}
		columns[i].Type = t
	}
	return DataType{descriptor: &dataTypeDescriptor{kind: TypeTuple, fields: Columns{columns: columns}}}, nil
}
func UDTType(keyspace, name string, fields ...ColumnSpec) (DataType, error) {
	if keyspace == "" || name == "" {
		return DataType{}, typeValidation("identity", "UDT keyspace and name must not be empty")
	}
	if len(fields) > math.MaxUint16 {
		return DataType{}, typeValidation("fields", "UDT has too many fields")
	}
	seen := make(map[string]struct{}, len(fields))
	for i, c := range fields {
		if c.Name == "" {
			return DataType{}, typeValidation("fields", "UDT field %d has no name", i)
		}
		if _, ok := seen[c.Name]; ok {
			return DataType{}, typeValidation("fields", "duplicate UDT field %q", c.Name)
		}
		seen[c.Name] = struct{}{}
		if err := validateDataType(c.Type); err != nil {
			return DataType{}, fmt.Errorf("UDT field %q: %w", c.Name, err)
		}
	}
	return DataType{descriptor: &dataTypeDescriptor{kind: TypeUDT, keyspace: keyspace, name: name, fields: NewColumns(fields...)}}, nil
}
func VectorDataType(element DataType, dimension uint16) (DataType, error) {
	if err := validateDataType(element); err != nil {
		return DataType{}, err
	}
	if dimension == 0 {
		return DataType{}, typeValidation("dimension", "vector dimension must be positive")
	}
	return DataType{descriptor: &dataTypeDescriptor{kind: TypeCustom, element: element, dimension: dimension}}, nil
}
func NewColumns(columns ...ColumnSpec) Columns {
	return Columns{columns: append([]ColumnSpec(nil), columns...)}
}
func (t DataType) Kind() Type {
	if t.descriptor == nil {
		return TypeCustom
	}
	return t.descriptor.kind
}
func (t DataType) Frozen() bool { return t.descriptor != nil && t.descriptor.frozen }

// FrozenKnown reports whether a schema or caller supplied the frozen annotation.
// Native protocol result metadata does not carry this annotation.
func (t DataType) FrozenKnown() bool { return t.descriptor != nil && t.descriptor.frozenKnown }
func (t DataType) WithFrozen(frozen bool) DataType {
	if t.descriptor == nil {
		return t
	}
	d := *t.descriptor
	d.frozen = frozen
	d.frozenKnown = true
	return DataType{descriptor: &d}
}
func (t DataType) Element() (DataType, bool) {
	if t.descriptor == nil {
		return DataType{}, false
	}
	if t.Kind() == TypeList || t.Kind() == TypeSet || t.descriptor.dimension != 0 {
		return t.descriptor.element, true
	}
	return DataType{}, false
}
func (t DataType) KeyValue() (DataType, DataType, bool) {
	if t.descriptor == nil || t.Kind() != TypeMap {
		return DataType{}, DataType{}, false
	}
	return t.descriptor.key, t.descriptor.element, true
}
func (t DataType) Fields() Columns {
	if t.descriptor == nil {
		return Columns{}
	}
	return NewColumns(t.descriptor.fields.columns...)
}
func (t DataType) UDTName() (keyspace, name string, ok bool) {
	if t.descriptor == nil || t.Kind() != TypeUDT {
		return "", "", false
	}
	return t.descriptor.keyspace, t.descriptor.name, true
}
func (t DataType) Dimension() (uint16, bool) {
	if t.descriptor == nil || t.descriptor.dimension == 0 {
		return 0, false
	}
	return t.descriptor.dimension, true
}
func (c Columns) Len() int { return len(c.columns) }
func (c Columns) At(index int) (ColumnSpec, bool) {
	if index < 0 || index >= len(c.columns) {
		return ColumnSpec{}, false
	}
	return c.columns[index], true
}
func (c Columns) Equal(other Columns) bool {
	if len(c.columns) != len(other.columns) {
		return false
	}
	for i, a := range c.columns {
		b := other.columns[i]
		if a.Keyspace != b.Keyspace || a.Table != b.Table || a.Name != b.Name || !a.Type.Equal(b.Type) {
			return false
		}
	}
	return true
}
func (t DataType) Equal(other DataType) bool {
	if t.descriptor == other.descriptor {
		return true
	}
	if t.descriptor == nil || other.descriptor == nil {
		return false
	}
	a, b := t.descriptor, other.descriptor
	return a.kind == b.kind && a.dimension == b.dimension && a.keyspace == b.keyspace && a.name == b.name && a.element.Equal(b.element) && a.key.Equal(b.key) && a.fields.Equal(b.fields)
}

// fingerprint is an unambiguous structural encoding, not a hash. Equality still
// compares descriptors directly and does not depend on this representation.
func (t DataType) fingerprint() string {
	var b strings.Builder
	var number [8]byte
	putNumber := func(n uint64) { binary.BigEndian.PutUint64(number[:], n); b.Write(number[:]) }
	putString := func(s string) { putNumber(uint64(len(s))); b.WriteString(s) }
	var appendType func(DataType)
	appendType = func(t DataType) {
		if t.descriptor == nil {
			b.WriteByte(0)
			return
		}
		b.WriteByte(1)
		d := t.descriptor
		putNumber(uint64(d.kind))
		putNumber(uint64(d.dimension))
		putString(d.keyspace)
		putString(d.name)
		appendType(d.key)
		appendType(d.element)
		putNumber(uint64(d.fields.Len()))
		for _, c := range d.fields.columns {
			putString(c.Keyspace)
			putString(c.Table)
			putString(c.Name)
			appendType(c.Type)
		}
	}
	appendType(t)
	return b.String()
}
func (t DataType) String() string {
	if t.descriptor == nil {
		return "<invalid>"
	}
	d := t.descriptor
	var s string
	switch d.kind {
	case TypeList, TypeSet:
		s = fmt.Sprintf("%s<%s>", d.kind, d.element)
	case TypeMap:
		s = fmt.Sprintf("map<%s,%s>", d.key, d.element)
	case TypeTuple:
		fields := make([]string, d.fields.Len())
		for i, c := range d.fields.columns {
			fields[i] = c.Type.String()
		}
		s = "tuple<" + strings.Join(fields, ",") + ">"
	case TypeUDT:
		s = d.keyspace + "." + d.name
	case TypeCustom:
		s = fmt.Sprintf("vector<%s,%d>", d.element, d.dimension)
	default:
		s = d.kind.String()
	}
	if d.frozen {
		return "frozen<" + s + ">"
	}
	return s
}
func validateDataType(t DataType) error {
	if t.descriptor == nil {
		return typeValidation("type", "invalid zero DataType")
	}
	return nil
}
func validateColumns(c Columns) error {
	for i, col := range c.columns {
		if err := validateDataType(col.Type); err != nil {
			return fmt.Errorf("column %d (%s): %w", i, col.Name, err)
		}
	}
	return nil
}
func typeValidation(field, format string, args ...any) error {
	return &ValidationError{Field: field, Err: fmt.Errorf(format, args...)}
}

// dataTypeFromInfo adapts protocol metadata only; it never constructs values or
// calls the legacy reflection-based serialization methods.
func dataTypeFromInfo(info TypeInfo) (DataType, error) { return dataTypeFromInfoDepth(info, 0) }
func dataTypeFromInfoDepth(info TypeInfo, depth int) (DataType, error) {
	if info == nil {
		return DataType{}, typeValidation("type", "nil protocol descriptor")
	}
	if depth > 128 {
		return DataType{}, typeValidation("type", "protocol descriptor nesting exceeds 128")
	}
	child := func(t TypeInfo) (DataType, error) { return dataTypeFromInfoDepth(t, depth+1) }
	switch t := info.(type) {
	case *NativeType:
		if t == nil {
			return DataType{}, typeValidation("type", "nil protocol descriptor")
		}
		return child(*t)
	case *CollectionType:
		if t == nil {
			return DataType{}, typeValidation("type", "nil protocol descriptor")
		}
		return child(*t)
	case *TupleTypeInfo:
		if t == nil {
			return DataType{}, typeValidation("type", "nil protocol descriptor")
		}
		return child(*t)
	case *UDTTypeInfo:
		if t == nil {
			return DataType{}, typeValidation("type", "nil protocol descriptor")
		}
		return child(*t)
	case *VectorType:
		if t == nil {
			return DataType{}, typeValidation("type", "nil protocol descriptor")
		}
		return child(*t)
	case CollectionType:
		e, err := child(t.Elem)
		if err != nil {
			return DataType{}, err
		}
		switch t.Type() {
		case TypeList:
			return ListType(e)
		case TypeSet:
			return SetType(e)
		case TypeMap:
			k, err := child(t.Key)
			if err != nil {
				return DataType{}, err
			}
			return MapType(k, e)
		}
		return DataType{}, typeValidation("type", "invalid collection kind %v", t.Type())
	case TupleTypeInfo:
		if len(t.Elems) > math.MaxUint16 {
			return DataType{}, typeValidation("fields", "too many tuple fields")
		}
		fields := make([]DataType, len(t.Elems))
		for i, f := range t.Elems {
			v, err := child(f)
			if err != nil {
				return DataType{}, err
			}
			fields[i] = v
		}
		return TupleType(fields...)
	case UDTTypeInfo:
		if len(t.Elements) > math.MaxUint16 {
			return DataType{}, typeValidation("fields", "too many UDT fields")
		}
		fields := make([]ColumnSpec, len(t.Elements))
		for i, f := range t.Elements {
			v, err := child(f.Type)
			if err != nil {
				return DataType{}, err
			}
			fields[i] = ColumnSpec{Name: f.Name, Type: v}
		}
		return UDTType(t.KeySpace, t.Name, fields...)
	case VectorType:
		if t.Dimensions <= 0 || t.Dimensions > math.MaxUint16 {
			return DataType{}, typeValidation("dimension", "invalid vector dimension %d", t.Dimensions)
		}
		e, err := child(t.SubType)
		if err != nil {
			return DataType{}, err
		}
		return VectorDataType(e, uint16(t.Dimensions))
	default:
		return ScalarType(info.Type())
	}
}
func columnsFromInfo(info []ColumnInfo) (Columns, error) {
	columns := make([]ColumnSpec, len(info))
	for i, c := range info {
		t, err := dataTypeFromInfo(c.TypeInfo)
		if err != nil {
			return Columns{}, fmt.Errorf("column %d (%s): %w", i, c.Name, err)
		}
		columns[i] = ColumnSpec{Keyspace: c.Keyspace, Table: c.Table, Name: c.Name, Type: t}
	}
	return Columns{columns: columns}, nil
}

// Encoder writes the body of one checked CQL value.
type Encoder[T any] interface {
	CheckType(DataType) error
	Encode(*ValueWriter, T) error
}

// Decoder reads one checked CQL value and recursively detaches borrowed values.
type Decoder[T any] interface {
	CheckType(DataType) error
	Decode(ValueReader) (T, error)
	Clone(T) T
}
type Codec[T any] interface {
	Encoder[T]
	Decoder[T]
}
type CellState uint8

const (
	Present CellState = iota
	Null
	Unset
	Empty
)

type Nullable[T any] struct {
	Value T
	Valid bool
}
type BindValue[T any] struct {
	Value T
	State CellState
}
type EmptyValueOf[T any] struct {
	Value T
	Empty bool
}
