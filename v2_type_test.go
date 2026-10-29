package gocql

import (
	"errors"
	"testing"
)

func foundationScalar(t *testing.T, kind Type) DataType {
	t.Helper()
	d, err := ScalarType(kind)
	if err != nil {
		t.Fatal(err)
	}
	return d
}
func TestV2FoundationDescriptorImmutability(t *testing.T) {
	integer := foundationScalar(t, TypeInt)
	text := foundationScalar(t, TypeText)
	fields := []ColumnSpec{{Name: "a", Type: integer}, {Name: "b", Type: text}}
	udt, err := UDTType("ks", "record", fields...)
	if err != nil {
		t.Fatal(err)
	}
	fields[0] = ColumnSpec{Name: "changed", Type: text}
	first, _ := udt.Fields().At(0)
	if first.Name != "a" || !first.Type.Equal(integer) {
		t.Fatal("constructor retained mutable input")
	}
	exposed := udt.Fields()
	exposed.columns[0] = fields[0]
	again, _ := udt.Fields().At(0)
	if again.Name != "a" {
		t.Fatal("Fields exposed descriptor storage")
	}
	columns := NewColumns(ColumnSpec{Name: "record", Type: udt})
	c, _ := columns.At(0)
	c.Name = "changed"
	original, _ := columns.At(0)
	if original.Name != "record" {
		t.Fatal("At exposed mutable column")
	}
	frozen := udt.WithFrozen(true)
	if udt.Frozen() || udt.FrozenKnown() || !frozen.Frozen() || !frozen.FrozenKnown() || !udt.Equal(frozen) || udt.fingerprint() != frozen.fingerprint() {
		t.Fatal("frozen annotation changed wire compatibility or was lost")
	}
	changed, _ := UDTType("ks", "record", ColumnSpec{Name: "other", Type: integer}, ColumnSpec{Name: "b", Type: text})
	if udt.Equal(changed) || udt.fingerprint() == changed.fingerprint() {
		t.Fatal("UDT names missing from identity")
	}
	list, _ := ListType(udt)
	different, _ := ListType(changed)
	if list.Equal(different) || list.fingerprint() == different.fingerprint() {
		t.Fatal("nested identity lost")
	}
	mapType, _ := MapType(integer, list)
	k, v, ok := mapType.KeyValue()
	if !ok || !k.Equal(integer) || !v.Equal(list) {
		t.Fatal("map children lost")
	}
	vector, _ := VectorDataType(mapType, 17)
	dimension, ok := vector.Dimension()
	if !ok || dimension != 17 {
		t.Fatal("dimension lost")
	}
	element, ok := vector.Element()
	if !ok || !element.Equal(mapType) {
		t.Fatal("vector element lost")
	}
	if _, ok := columns.At(-1); ok {
		t.Fatal("negative index accepted")
	}
	if _, ok := columns.At(1); ok {
		t.Fatal("past-end index accepted")
	}
}
func TestV2FoundationDescriptorValidation(t *testing.T) {
	integer := foundationScalar(t, TypeInt)
	cases := []func() (DataType, error){func() (DataType, error) { return ScalarType(TypeList) }, func() (DataType, error) { return ScalarType(TypeCustom) }, func() (DataType, error) { return ListType(DataType{}) }, func() (DataType, error) { return MapType(integer, DataType{}) }, func() (DataType, error) { return TupleType() }, func() (DataType, error) { return VectorDataType(integer, 0) }, func() (DataType, error) { return UDTType("", "record") }, func() (DataType, error) {
		return UDTType("ks", "record", ColumnSpec{Name: "x", Type: integer}, ColumnSpec{Name: "x", Type: integer})
	}}
	for i, test := range cases {
		_, err := test()
		var validation *ValidationError
		if !errors.As(err, &validation) {
			t.Errorf("case %d: expected ValidationError, got %v", i, err)
		}
	}
	if err := validateDataType(DataType{}.WithFrozen(true)); err == nil {
		t.Fatal("zero descriptor became valid")
	}
}
func TestV2FoundationProtocolTypeConversion(t *testing.T) {
	native := NewNativeType(4, TypeInt)
	input := NewUDTType(4, "record", "ks", UDTField{Name: "x", Type: NewCollectionType(NewNativeType(4, TypeList), nil, native)})
	descriptor, err := dataTypeFromInfo(input)
	if err != nil {
		t.Fatal(err)
	}
	field, _ := descriptor.Fields().At(0)
	element, ok := field.Type.Element()
	if !ok || element.Kind() != TypeInt {
		t.Fatal("nested conversion lost type")
	}
	input.Elements[0].Name = "mutated"
	field, _ = descriptor.Fields().At(0)
	if field.Name != "x" {
		t.Fatal("protocol descriptor slices retained")
	}
	columns, err := columnsFromInfo([]ColumnInfo{{Keyspace: "ks", Table: "tbl", Name: "value", TypeInfo: native}})
	if err != nil {
		t.Fatal(err)
	}
	c, _ := columns.At(0)
	if c.Keyspace != "ks" || c.Table != "tbl" || c.Name != "value" || c.Type.Kind() != TypeInt {
		t.Fatalf("wrong column: %+v", c)
	}
	var nilNative *NativeType
	if _, err = dataTypeFromInfo(nilNative); err == nil {
		t.Fatal("typed nil accepted")
	}
	cyc := &CollectionType{NativeType: NewNativeType(4, TypeList)}
	cyc.Elem = cyc
	if _, err = dataTypeFromInfo(cyc); err == nil {
		t.Fatal("recursive external descriptor accepted")
	}
	for _, n := range []int{-1, 0, 65536} {
		if _, err = dataTypeFromInfo(VectorType{NativeType: NewNativeType(4, TypeCustom), SubType: native, Dimensions: n}); err == nil {
			t.Fatalf("invalid dimension %d accepted", n)
		}
	}
}
