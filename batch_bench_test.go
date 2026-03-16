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

// benchSink prevents the compiler from eliminating allocations via dead-code
// elimination. Assigned at the end of each benchmark loop.
var benchSink interface{}

// BenchmarkBatchQueryAppend measures the cost of appending entries to a Batch
// via the Query() method. This exercises slice growth and BatchEntry allocation.
func BenchmarkBatchQueryAppend(b *testing.B) {
	for _, size := range []int{10, 100} {
		b.Run(fmt.Sprintf("entries=%d", size), func(b *testing.B) {
			b.ReportAllocs()
			var batch *Batch
			for i := 0; i < b.N; i++ {
				batch = &Batch{
					Type: LoggedBatch,
				}
				for j := 0; j < size; j++ {
					batch.Query("INSERT INTO ks.tbl (pk, v) VALUES (?, ?)", j, fmt.Sprintf("val_%d", j))
				}
			}
			benchSink = batch
		})
	}
}

// BenchmarkBatchQueryAppendPreallocated measures the cost of appending entries
// to a Batch with a pre-allocated Entries slice, to serve as comparison target
// for the Reserve() optimization.
func BenchmarkBatchQueryAppendPreallocated(b *testing.B) {
	for _, size := range []int{10, 100} {
		b.Run(fmt.Sprintf("entries=%d", size), func(b *testing.B) {
			b.ReportAllocs()
			var batch *Batch
			for i := 0; i < b.N; i++ {
				batch = (&Batch{
					Type: LoggedBatch,
				}).Reserve(size)
				for j := 0; j < size; j++ {
					batch.Query("INSERT INTO ks.tbl (pk, v) VALUES (?, ?)", j, fmt.Sprintf("val_%d", j))
				}
			}
			benchSink = batch
		})
	}
}

// BenchmarkBatchBuildWriteFrame measures the cost of building a writeBatchFrame
// from pre-populated batch statements with prepared IDs and queryValues.
// This isolates the allocation patterns in executeBatch's frame-building logic.
func BenchmarkBatchBuildWriteFrame(b *testing.B) {
	for _, size := range []int{10, 100} {
		b.Run(fmt.Sprintf("entries=%d", size), func(b *testing.B) {
			b.ReportAllocs()

			// Pre-build mock column types and values
			colCount := 2
			typ := NativeType{proto: protoVersion4, typ: TypeInt}

			var req *writeBatchFrame
			for i := 0; i < b.N; i++ {
				req = &writeBatchFrame{
					typ:              LoggedBatch,
					statements:       make([]batchStatment, size),
					consistency:      Quorum,
					defaultTimestamp: true,
				}

				stmts := make(map[string]string, size)

				// Simulate the allocation pattern from executeBatch
				for j := 0; j < size; j++ {
					bs := &req.statements[j]
					bs.preparedID = []byte(fmt.Sprintf("prepared_%d", j%5))
					stmts[string(bs.preparedID)] = fmt.Sprintf("INSERT INTO ks.tbl (pk, v) VALUES (?, ?)")

					bs.values = make([]queryValues, colCount)
					for k := 0; k < colCount; k++ {
						val, _ := Marshal(typ, j+k)
						bs.values[k] = queryValues{value: val}
					}
				}
			}
			benchSink = req
		})
	}
}

// BenchmarkBatchBuildWriteFrameBulkAlloc measures the cost of building a
// writeBatchFrame using a single bulk allocation for all queryValues.
// This reflects the optimized allocation pattern that replaces per-statement
// make([]queryValues, ...) calls with a single contiguous slice.
func BenchmarkBatchBuildWriteFrameBulkAlloc(b *testing.B) {
	for _, size := range []int{10, 100} {
		b.Run(fmt.Sprintf("entries=%d", size), func(b *testing.B) {
			b.ReportAllocs()

			colCount := 2
			typ := NativeType{proto: protoVersion4, typ: TypeInt}

			var req *writeBatchFrame
			for i := 0; i < b.N; i++ {
				req = &writeBatchFrame{
					typ:              LoggedBatch,
					statements:       make([]batchStatment, size),
					consistency:      Quorum,
					defaultTimestamp: true,
				}

				// Bulk-allocate all queryValues in a single slice
				allValues := make([]queryValues, size*colCount)

				for j := 0; j < size; j++ {
					bs := &req.statements[j]
					bs.preparedID = []byte(fmt.Sprintf("prepared_%d", j%5))

					bs.values = allValues[j*colCount : (j+1)*colCount]
					for k := 0; k < colCount; k++ {
						val, _ := Marshal(typ, j+k)
						bs.values[k] = queryValues{value: val}
					}
				}
			}
			benchSink = req
		})
	}
}

// BenchmarkBatchWriteFrameSerialization measures the cost of serializing a
// writeBatchFrame to bytes via the framer.
func BenchmarkBatchWriteFrameSerialization(b *testing.B) {
	for _, size := range []int{10, 100} {
		b.Run(fmt.Sprintf("entries=%d", size), func(b *testing.B) {
			b.ReportAllocs()

			colCount := 2
			typ := NativeType{proto: protoVersion4, typ: TypeInt}

			// Pre-build the frame once
			frame := &writeBatchFrame{
				typ:              LoggedBatch,
				statements:       make([]batchStatment, size),
				consistency:      Quorum,
				defaultTimestamp: true,
			}

			for j := 0; j < size; j++ {
				bs := &frame.statements[j]
				bs.preparedID = []byte(fmt.Sprintf("prepared_%d", j%5))
				bs.values = make([]queryValues, colCount)
				for k := 0; k < colCount; k++ {
					val, _ := Marshal(typ, j+k)
					bs.values[k] = queryValues{value: val}
				}
			}

			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				f := newFramer(nil, protoVersion4)
				err := frame.buildFrame(f, 1)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
