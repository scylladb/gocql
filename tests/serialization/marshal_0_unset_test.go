//go:build unit
// +build unit

package serialization_test

import (
	"testing"

	"github.com/gocql/gocql"
)

func TestMarshalUnsetColumn(t *testing.T) {
	t.Parallel()

	type tCase struct {
		tp      gocql.TypeInfo
		nilData bool
		err     bool
	}

	elem := gocql.NewNativeType(3, gocql.TypeSmallInt, "")
	cases := []tCase{
		{gocql.NewNativeType(4, gocql.TypeBoolean, ""), true, false},
		{gocql.NewNativeType(4, gocql.TypeTinyInt, ""), true, false},
		{gocql.NewNativeType(4, gocql.TypeSmallInt, ""), true, false},
		{gocql.NewNativeType(4, gocql.TypeInt, ""), true, false},
		{gocql.NewNativeType(4, gocql.TypeBigInt, ""), true, false},
		{gocql.NewNativeType(4, gocql.TypeCounter, ""), true, false},
		{gocql.NewNativeType(4, gocql.TypeVarint, ""), true, false},
		{gocql.NewNativeType(4, gocql.TypeFloat, ""), true, false},
		{gocql.NewNativeType(4, gocql.TypeDouble, ""), true, false},
		{gocql.NewNativeType(4, gocql.TypeDecimal, ""), true, false},
		{gocql.NewNativeType(4, gocql.TypeVarchar, ""), true, false},
		{gocql.NewNativeType(4, gocql.TypeText, ""), true, false},
		{gocql.NewNativeType(4, gocql.TypeBlob, ""), true, false},
		{gocql.NewNativeType(4, gocql.TypeAscii, ""), true, false},
		{gocql.NewNativeType(4, gocql.TypeUUID, ""), true, false},
		{gocql.NewNativeType(4, gocql.TypeTimeUUID, ""), true, false},
		{gocql.NewNativeType(4, gocql.TypeInet, ""), true, false},
		{gocql.NewNativeType(4, gocql.TypeTime, ""), true, false},
		{gocql.NewNativeType(4, gocql.TypeTimestamp, ""), true, false},
		{gocql.NewNativeType(4, gocql.TypeDate, ""), true, false},
		{gocql.NewNativeType(4, gocql.TypeDuration, ""), true, false},

		{gocql.NewCollectionType(gocql.NewNativeType(3, gocql.TypeList, ""), nil, elem), true, false},
		{gocql.NewCollectionType(gocql.NewNativeType(3, gocql.TypeSet, ""), nil, elem), true, false},

		{gocql.NewCollectionType(gocql.NewNativeType(3, gocql.TypeMap, ""), elem, elem), true, false},

		{gocql.NewUDTType(3, "udt1", "", gocql.UDTField{Name: "1", Type: elem}), true, true},
		{gocql.NewTupleType(gocql.NewNativeType(3, gocql.TypeTuple, ""), elem), true, true},
	}

	for _, expected := range cases {
		data, err := gocql.Marshal(expected.tp, gocql.UnsetValue)
		if expected.nilData && data != nil {
			t.Errorf("marshallig unsetColumn for the cqltype %s should return nil data", expected.tp.Type())
		}
		if !expected.nilData && data == nil {
			t.Errorf("marshallig unsetColumn for the cqltype %s should return not nil data", expected.tp.Type())
		}
		if expected.err && err == nil {
			t.Errorf("marshallig unsetColumn for the cqltype %s should return an error", expected.tp.Type())
		}
		if !expected.err && err != nil {
			t.Errorf("marshallig unsetColumn for the cqltype %s should not return an error", expected.tp.Type())
		}
	}
}
