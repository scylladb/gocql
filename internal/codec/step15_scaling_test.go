//go:build gocql_perf

// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package codec

import (
	"fmt"
	cqltype "github.com/scylladb/gocql/v2/internal/cqltype"
	"github.com/scylladb/gocql/v2/internal/wire"
	"math/bits"
	"testing"
)

func TestStep15NestedVisitBounds(t *testing.T) {
	for _, depth := range []int{1, 4, 16} {
		for _, fragments := range []int{1, 256, 65536} {
			t.Run(fmt.Sprintf("depth%d/S%d", depth, fragments), func(t *testing.T) {
				typ := CodecTestMust(cqltype.ScalarType(cqltype.TypeBlob))
				v := BlobValue(make([]byte, 1<<20))
				for i := 0; i < depth; i++ {
					typ = CodecTestMust(cqltype.ListType(typ))
					v = CodecTestMust(ListValue(typ, []Value{v}))
				}
				encoded := CodecTestMust(EncodeValueBorrowed(typ, DynamicCodec, v))
				raw := encoded.Bytes().OwnedBytes()
				spans := make([]wire.Span, fragments)
				for i := range spans {
					spans[i] = wire.Borrow(raw[len(raw)*i/fragments : len(raw)*(i+1)/fragments])
				}
				chain := CodecTestMust(wire.NewChain(spans...))
				plan := CodecTestMust(compileValueDecoder(DynamicCodec, typ))
				wire.ResetPerfCounters()
				ResetPerfCounters()
				value := CodecTestMust(plan.Decode(NewValueReader(typ, cqltype.Present, chain, nil)))
				for i := 0; i < depth; i++ {
					it := CodecTestMust(value.Elements())
					if !it.Next() {
						t.Fatal(it.Err())
					}
					value = it.Value()
					if it.Next() || it.Err() != nil {
						t.Fatal(it.Err())
					}
				}
				c := wire.ReadPerfCounters()
				if c.SequentialVisits > int64(32*(depth+1)) || c.IndexComparisons > int64(8*depth*(bits.Len(uint(fragments))+1)) || c.CopyBytes != 0 || ReadPerfTypeChecks() != 0 {
					t.Fatal("nested skip growth", c)
				}
				t.Logf("D=%d S=%d actual=%+v type_checks=%d", depth, fragments, wire.ReadPerfCounters(), ReadPerfTypeChecks())
			})
		}
	}
}
func TestStep15DynamicIndexJBounds(t *testing.T) {
	blob := CodecTestMust(cqltype.ScalarType(cqltype.TypeBlob))
	for _, n := range []int{1, 128, 4096} {
		for _, fragments := range []int{1, 256, 65536} {
			t.Run(fmt.Sprintf("N%d/S%d", n, fragments), func(t *testing.T) {
				specs := make([]cqltype.ColumnSpec, n)
				values := make([]Value, n)
				for i := range specs {
					specs[i] = cqltype.ColumnSpec{Name: fmt.Sprint("v", i), Type: blob}
					values[i] = BlobValue(make([]byte, (1<<20)/n))
				}
				cols := cqltype.NewColumns(specs...)
				raw := cqltype.BytesView(dynamicTestRowBody(t, cols, values), nil).OwnedBytes()
				spans := make([]wire.Span, fragments)
				for i := range spans {
					spans[i] = wire.Borrow(raw[len(raw)*i/fragments : len(raw)*(i+1)/fragments])
				}
				chain := CodecTestMust(wire.NewChain(spans...))
				plan := CodecTestMust(DynamicRows.Compile(cols))
				row := CodecTestMust(plan.Decode(NewRowReader(cols, chain, nil)))
				ResetPerfCounters()
				wire.ResetPerfCounters()
				CodecTestMust(row.At(n - 1))
				first := wire.ReadPerfCounters()
				wire.ResetPerfCounters()
				for j := 0; j < 16; j++ {
					CodecTestMust(row.At(n - 1))
				}
				builds, cells := ReadPerfDynamicIndexCounters()
				c := wire.ReadPerfCounters()
				if builds != 1 || cells != int64(n) || first.SequentialVisits > int64(8*n+fragments) || c.SequentialVisits != 0 || c.Takes != 0 || c.IndexComparisons > int64(32*(bits.Len(uint(fragments))+1)) || c.CopyBytes != 0 || c.Retains != 0 || c.Releases != 0 {
					t.Fatal("dynamic index repeated row scan", builds, cells, first, c)
				}
				t.Logf("N=%d S=%d first=%+v J=16 repeated=%+v", n, fragments, first, wire.ReadPerfCounters())
			})
		}
	}
}
