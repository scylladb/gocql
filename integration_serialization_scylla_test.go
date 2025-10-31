//go:build integration
// +build integration

package gocql

import (
	"bytes"
	"fmt"
	"math/big"
	"reflect"
	"strings"
	"testing"
	"time"
	"unsafe"

	"gopkg.in/inf.v0"

	"github.com/gocql/gocql/internal/tests/serialization/valcases"
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
		cqlType := NewNativeType(4, tp)

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
		cqlType := NewNativeType(4, tp)

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
		cqlType := NewNativeType(4, tp)

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

// SliceMapTypesTestCase defines a test case for validating SliceMap/MapScan behavior
type SliceMapTypesTestCase struct {
	CQLType           string
	CQLValue          string      // Non-NULL value to insert
	ExpectedValue     interface{} // Expected value for non-NULL case
	ExpectedNullValue interface{} // Expected value for NULL
}

// compareCollectionValues compares collection values (lists, sets, maps) with special handling
func compareCollectionValues(t *testing.T, cqlType string, expected, actual interface{}) bool {
	switch {
	case strings.HasPrefix(cqlType, "set<"):
		// Sets are returned as slices, but order is not guaranteed
		expectedSlice := reflect.ValueOf(expected)
		actualSlice := reflect.ValueOf(actual)
		if expectedSlice.Kind() != reflect.Slice || actualSlice.Kind() != reflect.Slice {
			return false
		}
		if expectedSlice.Len() != actualSlice.Len() {
			return false
		}

		// Convert to maps for unordered comparison
		expectedSet := make(map[interface{}]bool)
		for i := 0; i < expectedSlice.Len(); i++ {
			expectedSet[expectedSlice.Index(i).Interface()] = true
		}

		actualSet := make(map[interface{}]bool)
		for i := 0; i < actualSlice.Len(); i++ {
			actualSet[actualSlice.Index(i).Interface()] = true
		}

		return reflect.DeepEqual(expectedSet, actualSet)

	default:
		// For lists, maps, and other collections, reflect.DeepEqual works fine
		return reflect.DeepEqual(expected, actual)
	}
}

// compareValues compares expected and actual values with type-specific logic
func compareValues(t *testing.T, cqlType string, expected, actual interface{}) bool {
	switch cqlType {
	case "varint":
		// big.Int needs Cmp() for proper comparison, but handle nil pointers safely
		if expectedBig, ok := expected.(*big.Int); ok {
			if actualBig, ok := actual.(*big.Int); ok {
				// Handle nil cases
				if expectedBig == nil && actualBig == nil {
					return true
				}
				if expectedBig == nil || actualBig == nil {
					return false
				}
				return expectedBig.Cmp(actualBig) == 0
			}
		}
		return reflect.DeepEqual(expected, actual)

	case "decimal":
		// inf.Dec needs Cmp() for proper comparison, but handle nil pointers safely
		if expectedDec, ok := expected.(*inf.Dec); ok {
			if actualDec, ok := actual.(*inf.Dec); ok {
				// Handle nil cases
				if expectedDec == nil && actualDec == nil {
					return true
				}
				if expectedDec == nil || actualDec == nil {
					return false
				}
				return expectedDec.Cmp(actualDec) == 0
			}
		}
		return reflect.DeepEqual(expected, actual)

	default:
		// reflect.DeepEqual handles nil vs empty slice/map distinction correctly for all types
		// including inet (net.IP), blob ([]byte), collections ([]T, map[K]V), etc.
		// This is critical for catching zero value behavior changes in the driver
		return reflect.DeepEqual(expected, actual)
	}
}

// TestSliceMapMapScanTypes tests SliceMap and MapScan with various CQL types
func TestSliceMapMapScanTypes(t *testing.T) {
	session := createSession(t)
	defer session.Close()

	tableCQL := `
		CREATE TABLE IF NOT EXISTS gocql_test.slicemap_test (
			id int PRIMARY KEY,
			tinyint_col tinyint,
			smallint_col smallint,
			int_col int,
			bigint_col bigint,
			float_col float,
			double_col double,
			boolean_col boolean,
			text_col text,
			ascii_col ascii,
			varchar_col varchar,
			timestamp_col timestamp,
			uuid_col uuid,
			timeuuid_col timeuuid,
			inet_col inet,
			blob_col blob,
			varint_col varint,
			decimal_col decimal,
			date_col date,
			time_col time,
			duration_col duration
		)`

	if err := createTable(session, tableCQL); err != nil {
		t.Fatal("Failed to create test table:", err)
	}

	if err := session.Query("TRUNCATE gocql_test.slicemap_test").Exec(); err != nil {
		t.Fatal("Failed to truncate test table:", err)
	}

	testCases := []SliceMapTypesTestCase{
		{"tinyint", "42", int8(42), int8(0)},
		{"smallint", "1234", int16(1234), int16(0)},
		{"int", "123456", int(123456), int(0)},
		{"bigint", "1234567890", int64(1234567890), int64(0)},
		{"float", "3.14", float32(3.14), float32(0)},
		{"double", "2.718281828", float64(2.718281828), float64(0)},
		{"boolean", "true", true, false},
		{"text", "'hello world'", "hello world", ""},
		{"ascii", "'hello ascii'", "hello ascii", ""},
		{"varchar", "'hello varchar'", "hello varchar", ""},
		{"timestamp", "1388534400000", time.Unix(1388534400, 0).UTC(), time.Time{}},
		{"uuid", "550e8400-e29b-41d4-a716-446655440000", mustParseUUID("550e8400-e29b-41d4-a716-446655440000"), UUID{}},
		{"timeuuid", "60d79c23-5793-11f0-8afe-bcfce78b517a", mustParseUUID("60d79c23-5793-11f0-8afe-bcfce78b517a"), UUID{}},
		{"inet", "'127.0.0.1'", "127.0.0.1", ""},
		{"blob", "0x48656c6c6f", []byte("Hello"), []byte(nil)},
		{"varint", "123456789012345678901234567890", mustParseBigInt("123456789012345678901234567890"), (*big.Int)(nil)},
		{"decimal", "123.45", mustParseDecimal("123.45"), (*inf.Dec)(nil)},
		{"date", "'2015-05-03'", time.Date(2015, 5, 3, 0, 0, 0, 0, time.UTC), time.Date(-5877641, 06, 23, 0, 0, 0, 0, time.UTC)},
		{"time", "'13:30:54.234'", 13*time.Hour + 30*time.Minute + 54*time.Second + 234*time.Millisecond, time.Duration(0)},
		{"duration", "1y2mo3d4h5m6s789ms", mustCreateDuration(14, 3, 4*time.Hour+5*time.Minute+6*time.Second+789*time.Millisecond), Duration{}},
	}

	for i, tc := range testCases {
		t.Run(tc.CQLType, func(t *testing.T) {
			testSliceMapMapScanSimple(t, session, tc, i)
		})
	}
}

// Simplified test function that tests both SliceMap and MapScan with both NULL and non-NULL values
func testSliceMapMapScanSimple(t *testing.T, session *Session, tc SliceMapTypesTestCase, id int) {
	colName := tc.CQLType + "_col"

	t.Run("NonNull", func(t *testing.T) {
		insertQuery := fmt.Sprintf("INSERT INTO gocql_test.slicemap_test (id, %s) VALUES (?, %s)", colName, tc.CQLValue)
		if err := session.Query(insertQuery, id*2).Exec(); err != nil {
			t.Fatalf("Failed to insert non-NULL value: %v", err)
		}

		for _, method := range []string{"SliceMap", "MapScan"} {
			t.Run(method, func(t *testing.T) {
				result := queryAndExtractValue(t, session, colName, id*2, method)
				validateResult(t, tc.CQLType, tc.ExpectedValue, result, method, "non-NULL")
			})
		}
	})

	t.Run("Null", func(t *testing.T) {
		insertQuery := fmt.Sprintf("INSERT INTO gocql_test.slicemap_test (id, %s) VALUES (?, NULL)", colName)
		if err := session.Query(insertQuery, id*2+1).Exec(); err != nil {
			t.Fatalf("Failed to insert NULL value: %v", err)
		}

		// Test both SliceMap and MapScan
		for _, method := range []string{"SliceMap", "MapScan"} {
			t.Run(method, func(t *testing.T) {
				result := queryAndExtractValue(t, session, colName, id*2+1, method)
				validateResult(t, tc.CQLType, tc.ExpectedNullValue, result, method, "NULL")
			})
		}
	})
}

func queryAndExtractValue(t *testing.T, session *Session, colName string, id int, method string) interface{} {
	fmt.Println("queryAndExtractValue")
	selectQuery := fmt.Sprintf("SELECT %s FROM gocql_test.slicemap_test WHERE id = ?", colName)

	switch method {
	case "SliceMap":
		iter := session.Query(selectQuery, id).Iter()
		sliceResults, err := iter.SliceMap()
		fmt.Println("Slice results: ", sliceResults[0][colName])
		iter.Close()
		if err != nil {
			t.Fatalf("SliceMap failed: %v", err)
		}
		if len(sliceResults) != 1 {
			t.Fatalf("Expected 1 result, got %d", len(sliceResults))
		}
		return sliceResults[0][colName]

	case "MapScan":
		mapResult := make(map[string]interface{})
		if err := session.Query(selectQuery, id).MapScan(mapResult); err != nil {
			t.Fatalf("MapScan failed: %v", err)
		}
		return mapResult[colName]

	default:
		t.Fatalf("Unknown method: %s", method)
		return nil
	}
}

func validateResult(t *testing.T, cqlType string, expected, actual interface{}, method, valueType string) {
	if expected != nil && actual != nil {
		expectedType := reflect.TypeOf(expected)
		actualType := reflect.TypeOf(actual)
		if expectedType != actualType {
			t.Errorf("%s %s %s: expected type %v, got %v", method, valueType, cqlType, expectedType, actualType)
		}
	}

	if !compareValues(t, cqlType, expected, actual) {
		t.Errorf("%s %s %s: expected value %v (type %T), got %v (type %T)",
			method, valueType, cqlType, expected, expected, actual, actual)
	}
}

func mustParseUUID(s string) UUID {
	u, err := ParseUUID(s)
	if err != nil {
		panic(err)
	}
	return u
}

func mustParseBigInt(s string) *big.Int {
	i := new(big.Int)
	if _, ok := i.SetString(s, 10); !ok {
		panic("failed to parse big.Int: " + s)
	}
	return i
}

func mustParseDecimal(s string) *inf.Dec {
	dec := new(inf.Dec)
	if _, ok := dec.SetString(s); !ok {
		panic("failed to parse inf.Dec: " + s)
	}
	return dec
}

func mustCreateDuration(months int32, days int32, timeDuration time.Duration) Duration {
	return Duration{
		Months:      months,
		Days:        days,
		Nanoseconds: timeDuration.Nanoseconds(),
	}
}

// TestSliceMapMapScanCounterTypes tests counter types separately since they have special restrictions
// (counter columns can't be mixed with other column types in the same table)
func TestSliceMapMapScanCounterTypes(t *testing.T) {
	session := createSessionFromClusterTabletsDisabled(createCluster(), t)
	defer session.Close()

	// Create separate table for counter types
	if err := createTable(session, `
		CREATE TABLE IF NOT EXISTS gocql_test_tablets_disabled.slicemap_counter_test (
			id int PRIMARY KEY,
			counter_col counter
		)
	`); err != nil {
		t.Fatal("Failed to create counter test table:", err)
	}

	// Clear existing data
	if err := session.Query("TRUNCATE gocql_test_tablets_disabled.slicemap_counter_test").Exec(); err != nil {
		t.Fatal("Failed to truncate counter test table:", err)
	}

	testID := 1
	expectedValue := int64(42)

	// Increment counter (can't INSERT into counter, must UPDATE)
	err := session.Query("UPDATE gocql_test_tablets_disabled.slicemap_counter_test SET counter_col = counter_col + 42 WHERE id = ?", testID).Exec()
	if err != nil {
		t.Fatalf("Failed to increment counter: %v", err)
	}

	// Test both SliceMap and MapScan
	for _, method := range []string{"SliceMap", "MapScan"} {
		t.Run(method, func(t *testing.T) {
			var result interface{}

			selectQuery := "SELECT counter_col FROM gocql_test_tablets_disabled.slicemap_counter_test WHERE id = ?"
			if method == "SliceMap" {
				iter := session.Query(selectQuery, testID).Iter()
				sliceResults, err := iter.SliceMap()
				iter.Close()
				if err != nil {
					t.Fatalf("SliceMap failed: %v", err)
				}
				if len(sliceResults) != 1 {
					t.Fatalf("Expected 1 result, got %d", len(sliceResults))
				}
				result = sliceResults[0]["counter_col"]
			} else {
				mapResult := make(map[string]interface{})
				if err := session.Query(selectQuery, testID).MapScan(mapResult); err != nil {
					t.Fatalf("MapScan failed: %v", err)
				}
				result = mapResult["counter_col"]
			}

			validateResult(t, "counter", expectedValue, result, method, "incremented")
		})
	}
}

// TestSliceMapMapScanTupleTypes tests tuple types separately since they have special handling
// (tuple elements get split into individual columns)
func TestSliceMapMapScanTupleTypes(t *testing.T) {
	session := createSession(t)
	defer session.Close()

	// Create test table with tuple column
	if err := createTable(session, `
		CREATE TABLE IF NOT EXISTS gocql_test.slicemap_tuple_test (
			id int PRIMARY KEY,
			tuple_col tuple<int, text>
		)
	`); err != nil {
		t.Fatal("Failed to create tuple test table:", err)
	}

	// Clear existing data
	if err := session.Query("TRUNCATE gocql_test.slicemap_tuple_test").Exec(); err != nil {
		t.Fatal("Failed to truncate tuple test table:", err)
	}

	// Test non-NULL tuple
	t.Run("NonNull", func(t *testing.T) {
		testID := 1
		// Insert tuple value
		err := session.Query("INSERT INTO gocql_test.slicemap_tuple_test (id, tuple_col) VALUES (?, (42, 'hello'))", testID).Exec()
		if err != nil {
			t.Fatalf("Failed to insert tuple value: %v", err)
		}

		// Test both SliceMap and MapScan
		for _, method := range []string{"SliceMap", "MapScan"} {
			t.Run(method, func(t *testing.T) {
				var result map[string]interface{}

				selectQuery := "SELECT tuple_col FROM gocql_test.slicemap_tuple_test WHERE id = ?"
				if method == "SliceMap" {
					iter := session.Query(selectQuery, testID).Iter()
					sliceResults, err := iter.SliceMap()
					iter.Close()
					if err != nil {
						t.Fatalf("SliceMap failed: %v", err)
					}
					if len(sliceResults) != 1 {
						t.Fatalf("Expected 1 result, got %d", len(sliceResults))
					}
					result = sliceResults[0]
				} else {
					result = make(map[string]interface{})
					if err := session.Query(selectQuery, testID).MapScan(result); err != nil {
						t.Fatalf("MapScan failed: %v", err)
					}
				}

				// Check tuple elements (tuples get split into individual columns)
				elem0Key := TupleColumnName("tuple_col", 0)
				elem1Key := TupleColumnName("tuple_col", 1)

				if result[elem0Key] != 42 {
					t.Errorf("%s tuple[0]: expected 42, got %v", method, result[elem0Key])
				}
				if result[elem1Key] != "hello" {
					t.Errorf("%s tuple[1]: expected 'hello', got %v", method, result[elem1Key])
				}
			})
		}
	})

	// Test NULL tuple
	t.Run("Null", func(t *testing.T) {
		testID := 2
		// Insert NULL tuple
		err := session.Query("INSERT INTO gocql_test.slicemap_tuple_test (id, tuple_col) VALUES (?, NULL)", testID).Exec()
		if err != nil {
			t.Fatalf("Failed to insert NULL tuple: %v", err)
		}

		// Test both SliceMap and MapScan
		for _, method := range []string{"SliceMap", "MapScan"} {
			t.Run(method, func(t *testing.T) {
				var result map[string]interface{}

				selectQuery := "SELECT tuple_col FROM gocql_test.slicemap_tuple_test WHERE id = ?"
				if method == "SliceMap" {
					iter := session.Query(selectQuery, testID).Iter()
					sliceResults, err := iter.SliceMap()
					iter.Close()
					if err != nil {
						t.Fatalf("SliceMap failed: %v", err)
					}
					if len(sliceResults) != 1 {
						t.Fatalf("Expected 1 result, got %d", len(sliceResults))
					}
					result = sliceResults[0]
				} else {
					result = make(map[string]interface{})
					if err := session.Query(selectQuery, testID).MapScan(result); err != nil {
						t.Fatalf("MapScan failed: %v", err)
					}
				}

				// Check tuple elements (NULL tuple gives zero values)
				elem0Key := TupleColumnName("tuple_col", 0)
				elem1Key := TupleColumnName("tuple_col", 1)

				if result[elem0Key] != 0 {
					t.Errorf("%s NULL tuple[0]: expected 0, got %v", method, result[elem0Key])
				}
				if result[elem1Key] != "" {
					t.Errorf("%s NULL tuple[1]: expected '', got %v", method, result[elem1Key])
				}
			})
		}
	})
}

// TestSliceMapMapScanVectorTypes tests vector types separately since they need Cassandra 5.0+ and special table setup
// (vectors need separate tables and version checks)
func TestSliceMapMapScanVectorTypes(t *testing.T) {
	session := createSession(t)
	defer session.Close()

	if *flagDistribution == "cassandra" && flagCassVersion.Before(5, 0, 0) {
		t.Skip("Vector types have been introduced in Cassandra 5.0")
	}

	if *flagDistribution == "scylla" && flagCassVersion.Before(2025, 3, 0) {
		t.Skip("Vector types have been introduced in ScyllaDB 2025.3")
	}

	// Create test table with vector columns
	if err := createTable(session, `
		CREATE TABLE IF NOT EXISTS gocql_test.slicemap_vector_test (
			id int PRIMARY KEY,
			vector_float_col vector<float, 3>,
			vector_text_col vector<text, 2>
		)
	`); err != nil {
		t.Fatal("Failed to create vector test table:", err)
	}

	// Clear existing data
	if err := session.Query("TRUNCATE gocql_test.slicemap_vector_test").Exec(); err != nil {
		t.Fatal("Failed to truncate vector test table:", err)
	}

	testCases := []struct {
		colName       string
		cqlValue      string
		expectedValue interface{}
		expectedNull  interface{}
	}{
		{"vector_float_col", "[1.0, 2.5, -3.0]", []float32{1.0, 2.5, -3.0}, []float32(nil)},
		{"vector_text_col", "['hello', 'world']", []string{"hello", "world"}, []string(nil)},
	}

	for _, tc := range testCases {
		t.Run(tc.colName, func(t *testing.T) {
			// Test non-NULL value
			t.Run("NonNull", func(t *testing.T) {
				testID := 1
				// Insert non-NULL value
				insertQuery := fmt.Sprintf("INSERT INTO gocql_test.slicemap_vector_test (id, %s) VALUES (?, %s)", tc.colName, tc.cqlValue)
				if err := session.Query(insertQuery, testID).Exec(); err != nil {
					t.Fatalf("Failed to insert non-NULL value: %v", err)
				}

				// Test both SliceMap and MapScan
				for _, method := range []string{"SliceMap", "MapScan"} {
					t.Run(method, func(t *testing.T) {
						var result interface{}

						selectQuery := fmt.Sprintf("SELECT %s FROM gocql_test.slicemap_vector_test WHERE id = ?", tc.colName)
						if method == "SliceMap" {
							iter := session.Query(selectQuery, testID).Iter()
							sliceResults, err := iter.SliceMap()
							iter.Close()
							if err != nil {
								t.Fatalf("SliceMap failed: %v", err)
							}
							if len(sliceResults) != 1 {
								t.Fatalf("Expected 1 result, got %d", len(sliceResults))
							}
							result = sliceResults[0][tc.colName]
						} else {
							mapResult := make(map[string]interface{})
							if err := session.Query(selectQuery, testID).MapScan(mapResult); err != nil {
								t.Fatalf("MapScan failed: %v", err)
							}
							result = mapResult[tc.colName]
						}

						validateResult(t, tc.colName, tc.expectedValue, result, method, "non-NULL")
					})
				}
			})

			// Test NULL value
			t.Run("Null", func(t *testing.T) {
				testID := 2
				// Insert NULL value
				insertQuery := fmt.Sprintf("INSERT INTO gocql_test.slicemap_vector_test (id, %s) VALUES (?, NULL)", tc.colName)
				if err := session.Query(insertQuery, testID).Exec(); err != nil {
					t.Fatalf("Failed to insert NULL value: %v", err)
				}

				// Test both SliceMap and MapScan
				for _, method := range []string{"SliceMap", "MapScan"} {
					t.Run(method, func(t *testing.T) {
						var result interface{}

						selectQuery := fmt.Sprintf("SELECT %s FROM gocql_test.slicemap_vector_test WHERE id = ?", tc.colName)
						if method == "SliceMap" {
							iter := session.Query(selectQuery, testID).Iter()
							sliceResults, err := iter.SliceMap()
							iter.Close()
							if err != nil {
								t.Fatalf("SliceMap failed: %v", err)
							}
							if len(sliceResults) != 1 {
								t.Fatalf("Expected 1 result, got %d", len(sliceResults))
							}
							result = sliceResults[0][tc.colName]
						} else {
							mapResult := make(map[string]interface{})
							if err := session.Query(selectQuery, testID).MapScan(mapResult); err != nil {
								t.Fatalf("MapScan failed: %v", err)
							}
							result = mapResult[tc.colName]
						}

						// Vectors should return nil slices for NULL values for consistency
						validateResult(t, tc.colName, tc.expectedNull, result, method, "NULL")
					})
				}
			})
		})
	}
}

// TestSliceMapMapScanCollectionTypes tests collection types separately since they have special handling
// (collections should return nil slices/maps for NULL values for consistency with other slice-based types)
func TestSliceMapMapScanCollectionTypes(t *testing.T) {
	session := createSession(t)
	defer session.Close()

	// Create test table with collection columns
	if err := createTable(session, `
		CREATE TABLE IF NOT EXISTS gocql_test.slicemap_collection_test (
			id int PRIMARY KEY,
			list_col list<text>,
			set_col set<int>,
			map_col map<text, int>
		)
	`); err != nil {
		t.Fatal("Failed to create collection test table:", err)
	}

	// Clear existing data
	if err := session.Query("TRUNCATE gocql_test.slicemap_collection_test").Exec(); err != nil {
		t.Fatal("Failed to truncate collection test table:", err)
	}

	testCases := []struct {
		colName       string
		cqlValue      string
		expectedValue interface{}
		expectedNull  interface{}
	}{
		{"list_col", "['a', 'b', 'c']", []string{"a", "b", "c"}, []string(nil)},
		{"set_col", "{1, 2, 3}", []int{1, 2, 3}, []int(nil)},
		{"map_col", "{'key1': 1, 'key2': 2}", map[string]int{"key1": 1, "key2": 2}, map[string]int(nil)},
	}

	for _, tc := range testCases {
		t.Run(tc.colName, func(t *testing.T) {
			// Test non-NULL value
			t.Run("NonNull", func(t *testing.T) {
				testID := 1
				// Insert non-NULL value
				insertQuery := fmt.Sprintf("INSERT INTO gocql_test.slicemap_collection_test (id, %s) VALUES (?, %s)", tc.colName, tc.cqlValue)
				if err := session.Query(insertQuery, testID).Exec(); err != nil {
					t.Fatalf("Failed to insert non-NULL value: %v", err)
				}

				// Test both SliceMap and MapScan
				for _, method := range []string{"SliceMap", "MapScan"} {
					t.Run(method, func(t *testing.T) {
						var result interface{}

						selectQuery := fmt.Sprintf("SELECT %s FROM gocql_test.slicemap_collection_test WHERE id = ?", tc.colName)
						if method == "SliceMap" {
							iter := session.Query(selectQuery, testID).Iter()
							sliceResults, err := iter.SliceMap()
							iter.Close()
							if err != nil {
								t.Fatalf("SliceMap failed: %v", err)
							}
							if len(sliceResults) != 1 {
								t.Fatalf("Expected 1 result, got %d", len(sliceResults))
							}
							result = sliceResults[0][tc.colName]
						} else {
							mapResult := make(map[string]interface{})
							if err := session.Query(selectQuery, testID).MapScan(mapResult); err != nil {
								t.Fatalf("MapScan failed: %v", err)
							}
							result = mapResult[tc.colName]
						}

						// For sets, we need special comparison since order is not guaranteed
						if strings.HasPrefix(tc.colName, "set_") {
							if !compareCollectionValues(t, tc.colName, tc.expectedValue, result) {
								t.Errorf("%s non-NULL %s: expected %v, got %v", method, tc.colName, tc.expectedValue, result)
							}
						} else {
							validateResult(t, tc.colName, tc.expectedValue, result, method, "non-NULL")
						}
					})
				}
			})

			// Test NULL value
			t.Run("Null", func(t *testing.T) {
				testID := 2
				// Insert NULL value
				insertQuery := fmt.Sprintf("INSERT INTO gocql_test.slicemap_collection_test (id, %s) VALUES (?, NULL)", tc.colName)
				if err := session.Query(insertQuery, testID).Exec(); err != nil {
					t.Fatalf("Failed to insert NULL value: %v", err)
				}

				// Test both SliceMap and MapScan
				for _, method := range []string{"SliceMap", "MapScan"} {
					t.Run(method, func(t *testing.T) {
						var result interface{}

						selectQuery := fmt.Sprintf("SELECT %s FROM gocql_test.slicemap_collection_test WHERE id = ?", tc.colName)
						if method == "SliceMap" {
							iter := session.Query(selectQuery, testID).Iter()
							sliceResults, err := iter.SliceMap()
							iter.Close()
							if err != nil {
								t.Fatalf("SliceMap failed: %v", err)
							}
							if len(sliceResults) != 1 {
								t.Fatalf("Expected 1 result, got %d", len(sliceResults))
							}
							result = sliceResults[0][tc.colName]
						} else {
							mapResult := make(map[string]interface{})
							if err := session.Query(selectQuery, testID).MapScan(mapResult); err != nil {
								t.Fatalf("MapScan failed: %v", err)
							}
							result = mapResult[tc.colName]
						}

						// Collections should return nil slices/maps for NULL values for consistency
						validateResult(t, tc.colName, tc.expectedNull, result, method, "NULL")
					})
				}
			})
		})
	}
}
