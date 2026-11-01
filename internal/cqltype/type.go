// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cqltype

import (
	"encoding/binary"
	"fmt"
	"math"
	"strings"
)

// DataType is an immutable, complete CQL type descriptor. Its zero value is invalid.
type DataType struct{ descriptor *DataTypeDescriptor }
type DataTypeDescriptor struct {
	Kind           Type
	frozen         bool
	frozenKnown    bool
	Element, Key   DataType
	Fields         Columns
	Keyspace, Name string
	Dimension      uint16
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
	if err := ValidateScalarKind(kind); err != nil {
		return DataType{}, err
	}
	return DataType{descriptor: &DataTypeDescriptor{Kind: kind}}, nil
}

// ValidateScalarKind checks scalar support without constructing a descriptor.
func ValidateScalarKind(kind Type) error {
	if kind < TypeAscii || kind > TypeDuration {
		return TypeValidation("kind", "unsupported scalar kind %v", kind)
	}
	return nil
}
func ListType(element DataType) (DataType, error) { return collectionDataType(TypeList, element) }
func SetType(element DataType) (DataType, error)  { return collectionDataType(TypeSet, element) }
func collectionDataType(kind Type, element DataType) (DataType, error) {
	if err := ValidateDataType(element); err != nil {
		return DataType{}, err
	}
	return DataType{descriptor: &DataTypeDescriptor{Kind: kind, Element: element}}, nil
}
func MapType(key, value DataType) (DataType, error) {
	if err := ValidateDataType(key); err != nil {
		return DataType{}, err
	}
	if err := ValidateDataType(value); err != nil {
		return DataType{}, err
	}
	return DataType{descriptor: &DataTypeDescriptor{Kind: TypeMap, Key: key, Element: value}}, nil
}
func TupleType(fields ...DataType) (DataType, error) {
	if len(fields) > math.MaxUint16 {
		return DataType{}, TypeValidation("fields", "tuple requires 0..65535 fields")
	}
	columns := make([]ColumnSpec, len(fields))
	for i, t := range fields {
		if err := ValidateDataType(t); err != nil {
			return DataType{}, fmt.Errorf("tuple field %d: %w", i, err)
		}
		columns[i].Type = t
	}
	return DataType{descriptor: &DataTypeDescriptor{Kind: TypeTuple, Fields: Columns{columns: columns}}}, nil
}
func UDTType(keyspace, name string, fields ...ColumnSpec) (DataType, error) {
	if keyspace == "" || name == "" {
		return DataType{}, TypeValidation("identity", "UDT keyspace and name must not be empty")
	}
	if len(fields) > math.MaxUint16 {
		return DataType{}, TypeValidation("fields", "UDT has too many fields")
	}
	seen := make(map[string]struct{}, len(fields))
	for i, c := range fields {
		if c.Name == "" {
			return DataType{}, TypeValidation("fields", "UDT field %d has no name", i)
		}
		if _, ok := seen[c.Name]; ok {
			return DataType{}, TypeValidation("fields", "duplicate UDT field %q", c.Name)
		}
		seen[c.Name] = struct{}{}
		if err := ValidateDataType(c.Type); err != nil {
			return DataType{}, fmt.Errorf("UDT field %q: %w", c.Name, err)
		}
	}
	return DataType{descriptor: &DataTypeDescriptor{Kind: TypeUDT, Keyspace: keyspace, Name: name, Fields: NewColumns(fields...)}}, nil
}
func VectorDataType(element DataType, dimension uint16) (DataType, error) {
	if err := ValidateDataType(element); err != nil {
		return DataType{}, err
	}
	if dimension == 0 {
		return DataType{}, TypeValidation("dimension", "vector dimension must be positive")
	}
	return DataType{descriptor: &DataTypeDescriptor{Kind: TypeCustom, Element: element, Dimension: dimension}}, nil
}
func NewColumns(columns ...ColumnSpec) Columns {
	return Columns{columns: append([]ColumnSpec(nil), columns...)}
}
func (t DataType) Kind() Type {
	if t.descriptor == nil {
		return TypeCustom
	}
	return t.descriptor.Kind
}
func (t DataType) Frozen() bool { return t.descriptor != nil && t.descriptor.frozen }

// FrozenKnown reports whether a schema or caller supplied the frozen annotation.
// Native protocol result metadata does not carry this annotation.
func (t DataType) FrozenKnown() bool {
	return t.descriptor != nil && t.descriptor.frozenKnown
}
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
	if t.Kind() == TypeList || t.Kind() == TypeSet || t.descriptor.Dimension != 0 {
		return t.descriptor.Element, true
	}
	return DataType{}, false
}
func (t DataType) KeyValue() (DataType, DataType, bool) {
	if t.descriptor == nil || t.Kind() != TypeMap {
		return DataType{}, DataType{}, false
	}
	return t.descriptor.Key, t.descriptor.Element, true
}
func (t DataType) Fields() Columns {
	if t.descriptor == nil {
		return Columns{}
	}
	return NewColumns(t.descriptor.Fields.columns...)
}
func (t DataType) UDTName() (keyspace, name string, ok bool) {
	if t.descriptor == nil || t.Kind() != TypeUDT {
		return "", "", false
	}
	return t.descriptor.Keyspace, t.descriptor.Name, true
}
func (t DataType) Dimension() (uint16, bool) {
	if t.descriptor == nil || t.descriptor.Dimension == 0 {
		return 0, false
	}
	return t.descriptor.Dimension, true
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
	return equalDataTypes(t, other, make(map[typeDescriptorPair]bool))
}

type typeDescriptorPair struct{ a, b *DataTypeDescriptor }

func equalDataTypes(t, other DataType, seen map[typeDescriptorPair]bool) bool {
	if t.descriptor == other.descriptor {
		return true
	}
	if t.descriptor == nil || other.descriptor == nil {
		return false
	}
	key := typeDescriptorPair{t.descriptor, other.descriptor}
	if seen[key] {
		return true
	}
	a, b := t.descriptor, other.descriptor
	if a.Kind != b.Kind || a.Dimension != b.Dimension || a.Keyspace != b.Keyspace || a.Name != b.Name || a.Fields.Len() != b.Fields.Len() {
		return false
	}
	seen[key] = true
	if !equalDataTypes(a.Element, b.Element, seen) || !equalDataTypes(a.Key, b.Key, seen) {
		return false
	}
	for i, x := range a.Fields.columns {
		y := b.Fields.columns[i]
		if x.Name != y.Name || x.Table != y.Table || x.Keyspace != y.Keyspace || !equalDataTypes(x.Type, y.Type, seen) {
			return false
		}
	}
	return true
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
		putNumber(uint64(d.Kind))
		putNumber(uint64(d.Dimension))
		putString(d.Keyspace)
		putString(d.Name)
		appendType(d.Key)
		appendType(d.Element)
		putNumber(uint64(d.Fields.Len()))
		for _, c := range d.Fields.columns {
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
	switch d.Kind {
	case TypeList, TypeSet:
		s = fmt.Sprintf("%s<%s>", d.Kind, d.Element)
	case TypeMap:
		s = fmt.Sprintf("map<%s,%s>", d.Key, d.Element)
	case TypeTuple:
		fields := make([]string, d.Fields.Len())
		for i, c := range d.Fields.columns {
			fields[i] = c.Type.String()
		}
		s = "tuple<" + strings.Join(fields, ",") + ">"
	case TypeUDT:
		s = d.Keyspace + "." + d.Name
	case TypeCustom:
		s = fmt.Sprintf("vector<%s,%d>", d.Element, d.Dimension)
	default:
		s = d.Kind.String()
	}
	if d.frozen {
		return "frozen<" + s + ">"
	}
	return s
}
func ValidateDataType(t DataType) error {
	if t.descriptor == nil {
		return TypeValidation("type", "invalid zero DataType")
	}
	return nil
}
func validateColumns(c Columns) error {
	for i, col := range c.columns {
		if err := ValidateDataType(col.Type); err != nil {
			return fmt.Errorf("column %d (%s): %w", i, col.Name, err)
		}
	}
	return nil
}
func TypeValidation(field, format string, args ...any) error {
	return &ValidationError{Field: field, Err: fmt.Errorf(format, args...)}
}

// dataTypeFromInfo adapts protocol metadata only; it never constructs values or
// calls the legacy reflection-based serialization methods.
func DataTypeFromInfo(info TypeInfo) (DataType, error) { return dataTypeFromInfoDepth(info, 0) }
func dataTypeFromInfoDepth(info TypeInfo, depth int) (DataType, error) {
	if info == nil {
		return DataType{}, TypeValidation("type", "nil protocol descriptor")
	}
	if depth > 128 {
		return DataType{}, TypeValidation("type", "protocol descriptor nesting exceeds 128")
	}
	child := func(t TypeInfo) (DataType, error) { return dataTypeFromInfoDepth(t, depth+1) }
	switch t := info.(type) {
	case *NativeType:
		if t == nil {
			return DataType{}, TypeValidation("type", "nil protocol descriptor")
		}
		return child(*t)
	case *CollectionType:
		if t == nil {
			return DataType{}, TypeValidation("type", "nil protocol descriptor")
		}
		return child(*t)
	case *TupleTypeInfo:
		if t == nil {
			return DataType{}, TypeValidation("type", "nil protocol descriptor")
		}
		return child(*t)
	case *UDTTypeInfo:
		if t == nil {
			return DataType{}, TypeValidation("type", "nil protocol descriptor")
		}
		return child(*t)
	case *VectorType:
		if t == nil {
			return DataType{}, TypeValidation("type", "nil protocol descriptor")
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
		return DataType{}, TypeValidation("type", "invalid collection kind %v", t.Type())
	case TupleTypeInfo:
		if len(t.Elems) > math.MaxUint16 {
			return DataType{}, TypeValidation("fields", "too many tuple fields")
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
			return DataType{}, TypeValidation("fields", "too many UDT fields")
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
			return DataType{}, TypeValidation("dimension", "invalid vector dimension %d", t.Dimensions)
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
func ColumnsFromInfo(info []ColumnInfo) (Columns, error) {
	columns := make([]ColumnSpec, len(info))
	for i, c := range info {
		t, err := DataTypeFromInfo(c.TypeInfo)
		if err != nil {
			return Columns{}, fmt.Errorf("column %d (%s): %w", i, c.Name, err)
		}
		columns[i] = ColumnSpec{Keyspace: c.Keyspace, Table: c.Table, Name: c.Name, Type: t}
	}
	return Columns{columns: columns}, nil
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
