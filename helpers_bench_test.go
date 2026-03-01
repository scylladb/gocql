/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package gocql

import (
	"fmt"
	"testing"
)

// createMockIter creates a mock iterator with the specified number of simple columns
func createMockIter(numColumns int) *Iter {
	columns := make([]ColumnInfo, numColumns)
	for i := 0; i < numColumns; i++ {
		columns[i] = ColumnInfo{
			Keyspace: "test_keyspace",
			Table:    "test_table",
			Name:     fmt.Sprintf("column_%d", i),
			TypeInfo: NativeType{typ: TypeInt, proto: protoVersion4},
		}
	}

	return &Iter{
		meta: resultMetadata{
			columns:        columns,
			colCount:       numColumns,
			actualColCount: numColumns,
		},
		numRows: 1,
	}
}

// createMockIterWithTypes creates a mock iterator with varied column types
func createMockIterWithTypes() *Iter {
	columns := []ColumnInfo{
		{Name: "id", TypeInfo: NativeType{typ: TypeInt, proto: protoVersion4}},
		{Name: "name", TypeInfo: NativeType{typ: TypeVarchar, proto: protoVersion4}},
		{Name: "created", TypeInfo: NativeType{typ: TypeTimestamp, proto: protoVersion4}},
		{Name: "score", TypeInfo: NativeType{typ: TypeBigInt, proto: protoVersion4}},
		{Name: "active", TypeInfo: NativeType{typ: TypeBoolean, proto: protoVersion4}},
		{Name: "data", TypeInfo: NativeType{typ: TypeBlob, proto: protoVersion4}},
		{Name: "uuid", TypeInfo: NativeType{typ: TypeUUID, proto: protoVersion4}},
		{Name: "value", TypeInfo: NativeType{typ: TypeDouble, proto: protoVersion4}},
		{Name: "count", TypeInfo: NativeType{typ: TypeCounter, proto: protoVersion4}},
		{Name: "text", TypeInfo: NativeType{typ: TypeText, proto: protoVersion4}},
	}

	return &Iter{
		meta: resultMetadata{
			columns:        columns,
			colCount:       len(columns),
			actualColCount: len(columns),
		},
		numRows: 1,
	}
}

// createMockIterWithTuples creates a mock iterator with tuple columns
func createMockIterWithTuples() *Iter {
	// Create a tuple with 3 elements
	tupleElems := []TypeInfo{
		NativeType{typ: TypeInt, proto: protoVersion4},
		NativeType{typ: TypeVarchar, proto: protoVersion4},
		NativeType{typ: TypeTimestamp, proto: protoVersion4},
	}

	columns := []ColumnInfo{
		{Name: "id", TypeInfo: NativeType{typ: TypeInt, proto: protoVersion4}},
		{Name: "coords", TypeInfo: TupleTypeInfo{
			NativeType: NativeType{typ: TypeTuple, proto: protoVersion4},
			Elems:      tupleElems,
		}},
		{Name: "name", TypeInfo: NativeType{typ: TypeVarchar, proto: protoVersion4}},
	}

	// actualColCount accounts for tuple expansion: 1 (id) + 3 (tuple elements) + 1 (name) = 5
	actualColCount := 1 + len(tupleElems) + 1

	return &Iter{
		meta: resultMetadata{
			columns:        columns,
			colCount:       len(columns),
			actualColCount: actualColCount,
		},
		numRows: 1,
	}
}

// BenchmarkRowData measures the performance of RowData() with simple columns
func BenchmarkRowData(b *testing.B) {
	iter := createMockIter(10)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		rd, err := iter.RowData()
		if err != nil {
			b.Fatal(err)
		}
		_ = rd
	}
}

// BenchmarkRowDataSmall measures performance with few columns (typical for narrow tables)
func BenchmarkRowDataSmall(b *testing.B) {
	iter := createMockIter(3)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		rd, err := iter.RowData()
		if err != nil {
			b.Fatal(err)
		}
		_ = rd
	}
}

// BenchmarkRowDataLarge measures performance with many columns (wide tables)
func BenchmarkRowDataLarge(b *testing.B) {
	iter := createMockIter(50)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		rd, err := iter.RowData()
		if err != nil {
			b.Fatal(err)
		}
		_ = rd
	}
}

// BenchmarkRowDataWithTypes measures performance with varied column types
func BenchmarkRowDataWithTypes(b *testing.B) {
	iter := createMockIterWithTypes()

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		rd, err := iter.RowData()
		if err != nil {
			b.Fatal(err)
		}
		_ = rd
	}
}

// BenchmarkRowDataWithTuples measures performance with tuple columns
func BenchmarkRowDataWithTuples(b *testing.B) {
	iter := createMockIterWithTuples()

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		rd, err := iter.RowData()
		if err != nil {
			b.Fatal(err)
		}
		_ = rd
	}
}

// BenchmarkRowDataRepeated simulates MapScan calling RowData repeatedly
func BenchmarkRowDataRepeated(b *testing.B) {
	iter := createMockIter(10)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Simulate 100 rows being scanned with MapScan
		for j := 0; j < 100; j++ {
			rd, err := iter.RowData()
			if err != nil {
				b.Fatal(err)
			}
			_ = rd
		}
	}
}

// BenchmarkRowDataAllocation focuses on allocation patterns
func BenchmarkRowDataAllocation(b *testing.B) {
	benchmarks := []struct {
		name string
		iter *Iter
	}{
		{"10cols", createMockIter(10)},
		{"100cols", createMockIter(100)},
		{"1000cols", createMockIter(1000)},
		{"WithTuples", createMockIterWithTuples()},
	}

	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				rd, err := bm.iter.RowData()
				if err != nil {
					b.Fatal(err)
				}
				_ = rd
			}
		})
	}
}
