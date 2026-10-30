//go:build !race

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package codec

import (
	cqltype "github.com/scylladb/gocql/v2/internal/cqltype"
	"testing"
)

// sync.Pool deliberately drops entries randomly under -race; allocation
// contracts are measured without that instrumentation. Concurrency and borrowed
// lifetime tests still run with -race in v2_codec_direct_test.go and tuple tests.

func TestV2TupleValueIterationAllocationsDoNotScale(t *testing.T) {
	integer := FoundationScalar(t, cqltype.TypeInt)
	tuple := CodecTestMust(cqltype.TupleType(integer))
	list := CodecTestMust(cqltype.ListType(tuple))
	allocated := func(count int) float64 {
		encoded := CodecTestMust(EncodeValueBorrowed(list, ListEncoder(TupleEncoder(OneParam(Int32))), make([]int32, count)))
		sequence := CodecTestMust(DecodeValue(ListDecoder(TupleDecoder(OneColumn(Int32))), encoded))
		return testing.AllocsPerRun(20, func() {
			iterator := sequence.Iterator()
			for iterator.Next() {
				_ = iterator.Value()
			}
			if err := iterator.Err(); err != nil {
				t.Fatal(err)
			}
		})
	}
	small, large := allocated(1), allocated(1024)
	if large > small {
		t.Fatalf("tuple value scratch allocations scale with elements: %g -> %g", small, large)
	}
}
