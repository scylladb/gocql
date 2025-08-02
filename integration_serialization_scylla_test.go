//go:build integration
// +build integration

package gocql

import (
	"bytes"
	"fmt"
	"gopkg.in/inf.v0"
	"math/big"
	"reflect"
	"testing"
	"unsafe"

	"github.com/scylladb/gocql/v2/internal/tests/serialization/valcases"
)

func TestSerializationSimpleTypesCassandra(t *testing.T) {
	const (
		pkColumn   = "test_id"
		testColumn = "test_col"
	)

	typeCases := valcases.GetSimple()

	session := createSession(t)
	defer session.Close()

	//Checks data and values conversion
	t.Run("Marshal", func(t *testing.T) {
		for _, tc := range typeCases {
			checkTypeMarshal(t, tc)
		}
	})

	t.Run("Unmarshal", func(t *testing.T) {
		for _, tc := range typeCases {
			checkTypeUnmarshal(t, tc)
		}
	})

	//Create are tables
	tables := make([]string, len(typeCases))
	for i, tc := range typeCases {
		table := "test_" + tc.CQLName

		stmt := fmt.Sprintf(`CREATE TABLE %s (%s text, %s %s, PRIMARY KEY (test_id))`, table, pkColumn, testColumn, tc.CQLName)
		if err := createTable(session, stmt); err != nil {
			t.Fatalf("failed to create table for cqltype (%s) with error '%v'", tc.CQLName, err)
		}
		tables[i] = table
	}

	//Check Insert and Select are values
	t.Run("InsertSelect", func(t *testing.T) {
		for i, tc := range typeCases {
			insertStmt := fmt.Sprintf("INSERT INTO %s (%s, %s) VALUES(?, ?)", tables[i], pkColumn, testColumn)
			selectStmt := fmt.Sprintf("SELECT %s FROM %s WHERE %s = ?", testColumn, tables[i], pkColumn)

			checkTypeInsertSelect(t, session, insertStmt, selectStmt, tc)
		}
	})
}

func checkTypeMarshal(t *testing.T, tc valcases.SimpleTypeCases) {
	cqlName := tc.CQLName
	t.Run(cqlName, func(t *testing.T) {
		tp := Type(tc.CQLType)
		cqlType := NewNativeType(4, tp, "")

		for _, valCase := range tc.Cases {
			for _, langCase := range valCase.LangCases {
				receivedData, err := Marshal(cqlType, langCase.Value)

				if !langCase.ErrInsert && err != nil {
					t.Errorf("failed to marshal case (%s)(%s) value (%T) with error '%v'", valCase.Name, langCase.LangType, langCase.Value, err)
				} else if langCase.ErrInsert && err == nil {
					t.Errorf("expected an error on marshal case (%s)(%s) value (%T)(%[2]v), but have no error", valCase.Name, langCase.LangType, langCase.Value)
				} else if !bytes.Equal(valCase.Data, receivedData) {
					t.Errorf("failed to equal case (%s)(%s) data: expected %d, got %d", valCase.Name, langCase.LangType, valCase.Data, receivedData)
				}
			}
		}
	})
}

func checkTypeUnmarshal(t *testing.T, tc valcases.SimpleTypeCases) {
	cqlName := tc.CQLName
	t.Run(cqlName, func(t *testing.T) {
		tp := Type(tc.CQLType)
		cqlType := NewNativeType(4, tp, "")

		for _, valCase := range tc.Cases {
			for _, langCase := range valCase.LangCases {
				received := newRef(langCase.Value)

				err := Unmarshal(cqlType, valCase.Data, received)
				if !langCase.ErrSelect && err != nil {
					t.Errorf("failed to unmarshal case (%s)(%s) value (%T) with error '%v'", valCase.Name, langCase.LangType, langCase.Value, err)
				}
				if langCase.ErrSelect && err == nil {
					t.Errorf("expected an error on unmarshal case (%s)(%s) value (%T)(%[2]v), but have no error", valCase.Name, langCase.LangType, langCase.Value)
				}
				received = deReference(received)
				if !equalVals(langCase.Value, received) {
					t.Errorf("failed to equal case (%s)(%s) value: expected %d, got %d", valCase.Name, langCase.LangType, langCase.Value, received)
				}
			}
		}
	})
}

func checkTypeInsertSelect(t *testing.T, session *Session, insertStmt, selectStmt string, tc valcases.SimpleTypeCases) {
	cqlName := tc.CQLName
	t.Run(cqlName, func(t *testing.T) {
		tp := Type(tc.CQLType)
		cqlType := NewNativeType(4, tp, "")

		for _, valCase := range tc.Cases {
			valCaseName := valCase.Name

			for _, langCase := range valCase.LangCases {
				var insertedValue interface{}
				//Check Insert value as values
				insertedValue = langCase.Value
				err := session.Query(insertStmt, valCaseName, insertedValue).Exec()
				if !langCase.ErrInsert && err != nil {
					t.Errorf("failed to insert case (%s) value (%T)(%[2]v) with error '%v'", valCaseName, insertedValue, err)
				} else if langCase.ErrInsert && err == nil {
					t.Errorf("expected an error on insert case (%s) value (%T)(%[2]v), but have no error", valCaseName, insertedValue, err)
				}

				//Check Select value as value
				selectedValue := newRef(langCase.Value)
				err = session.Query(selectStmt, valCase.Name).Scan(selectedValue)
				if !langCase.ErrSelect && err != nil {
					t.Errorf("failed to select case (%s) value (%T) with error '%v'", valCaseName, selectedValue, err)
				} else if langCase.ErrSelect && err == nil {
					t.Errorf("expected an error on select case (%s) value (%T)(%[2]v), but have no error", valCaseName, selectedValue)
				}
				selectedValue = deReference(selectedValue)
				if !equalVals(langCase.Value, selectedValue) {
					t.Errorf("failed to equal case (%s) value: expected: %d, got: %d", valCaseName, langCase.Value, selectedValue)
				}

				//Check Select value as bytes
				selectedValue = &DirectUnmarshal{}
				err = session.Query(selectStmt, valCase.Name).Scan(selectedValue)
				if err != nil {
					t.Errorf("failed to select case (%s) value (%T) for cqltype (%s) with error '%v'", valCaseName, selectedValue, cqlType, err)
				}
				selectedValue = *(*[]byte)(selectedValue.(*DirectUnmarshal))
				if !equalVals(valCase.Data, selectedValue) {
					t.Errorf("failed to equal case (%s) value for cqltype (%s): expected: %d, got: %d", valCaseName, cqlType, valCase.Data, selectedValue)
				}
			}
		}
	})
}

// newRef returns the nil reference to the input type value (*type)(nil)
func newRef(in interface{}) interface{} {
	out := reflect.New(reflect.TypeOf(in)).Interface()
	return out
}

func deReference(in interface{}) interface{} {
	return reflect.Indirect(reflect.ValueOf(in)).Interface()
}

func equalVals(in1, in2 interface{}) bool {
	rin1 := reflect.ValueOf(in1)
	rin2 := reflect.ValueOf(in2)
	if rin1.Kind() != rin2.Kind() {
		return false
	}
	if rin1.Kind() == reflect.Ptr && (rin1.IsNil() || rin2.IsNil()) {
		return rin1.IsNil() && rin2.IsNil()
	}

	switch vin1 := in1.(type) {
	case float32:
		vin2 := in2.(float32)
		return *(*[4]byte)(unsafe.Pointer(&vin1)) == *(*[4]byte)(unsafe.Pointer(&vin2))
	case *float32:
		vin2 := in2.(*float32)
		return *(*[4]byte)(unsafe.Pointer(vin1)) == *(*[4]byte)(unsafe.Pointer(vin2))
	case float64:
		vin2 := in2.(float64)
		return *(*[8]byte)(unsafe.Pointer(&vin1)) == *(*[8]byte)(unsafe.Pointer(&vin2))
	case *float64:
		vin2 := in2.(*float64)
		return *(*[8]byte)(unsafe.Pointer(vin1)) == *(*[8]byte)(unsafe.Pointer(vin2))
	case big.Int:
		vin2 := in2.(big.Int)
		return vin1.Cmp(&vin2) == 0
	case *big.Int:
		vin2 := in2.(*big.Int)
		return vin1.Cmp(vin2) == 0
	case inf.Dec:
		vin2 := in2.(inf.Dec)
		if vin1.Scale() != vin2.Scale() {
			return false
		}
		return vin1.UnscaledBig().Cmp(vin2.UnscaledBig()) == 0
	case *inf.Dec:
		vin2 := in2.(*inf.Dec)
		if vin1.Scale() != vin2.Scale() {
			return false
		}
		return vin1.UnscaledBig().Cmp(vin2.UnscaledBig()) == 0
	case fmt.Stringer:
		vin2 := in2.(fmt.Stringer)
		return vin1.String() == vin2.String()
	default:
		return reflect.DeepEqual(in1, in2)
	}
}
