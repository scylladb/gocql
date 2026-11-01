// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package codec

import (
	"fmt"
	"testing"

	cqltype "github.com/scylladb/gocql/v2/internal/cqltype"
	"github.com/scylladb/gocql/v2/internal/wire"
)

type step15WideRow struct {
	Numbers [32]int32
	Texts   [32]cqltype.Text
	Blobs   [32]cqltype.Bytes
	Lists   [32]Sequence[int32]
}

func BenchmarkStep15NarrowWideRows(b *testing.B) {
	integer := CodecTestMust(cqltype.ScalarType(cqltype.TypeInt))
	text := CodecTestMust(cqltype.ScalarType(cqltype.TypeText))
	blob := CodecTestMust(cqltype.ScalarType(cqltype.TypeBlob))
	list := CodecTestMust(cqltype.ListType(integer))
	for _, groups := range []int{1, 8, 32} {
		for _, fragments := range []int{1, 1024} {
			fields := make([]OutputField[step15WideRow], 0, groups*4)
			specs := make([]cqltype.ColumnSpec, 0, groups*4)
			values := make([]Value, 0, groups*4)
			for i := 0; i < groups; i++ {
				index := i
				names := [4]string{fmt.Sprint("n", i), fmt.Sprint("t", i), fmt.Sprint("b", i), fmt.Sprint("l", i)}
				specs = append(specs, cqltype.ColumnSpec{Name: names[0], Type: integer}, cqltype.ColumnSpec{Name: names[1], Type: text}, cqltype.ColumnSpec{Name: names[2], Type: blob}, cqltype.ColumnSpec{Name: names[3], Type: list})
				fields = append(fields, Column(names[0], Int32, func(r *step15WideRow) *int32 { return &r.Numbers[index] }), Column(names[1], TextCodec, func(r *step15WideRow) *cqltype.Text { return &r.Texts[index] }), Column(names[2], BlobCodec, func(r *step15WideRow) *cqltype.Bytes { return &r.Blobs[index] }), Column(names[3], ListDecoder(Int32), func(r *step15WideRow) *Sequence[int32] { return &r.Lists[index] }))
				values = append(values, IntValue(int32(i)), CodecTestMust(TextValue("safe-🙂")), BlobValue(make([]byte, 4096)), CodecTestMust(ListValue(list, []Value{IntValue(1), IntValue(2)})))
			}
			columns := cqltype.NewColumns(specs...)
			decoder := Row(fields...)
			plan := CodecTestMust(decoder.Compile(columns))
			raw := cqltype.BytesView(encodeTestParams(b, columns, DynamicParams, values), nil).OwnedBytes()
			spans := make([]wire.Span, fragments)
			for i := range spans {
				spans[i] = wire.Borrow(raw[len(raw)*i/fragments : len(raw)*(i+1)/fragments])
			}
			chain := CodecTestMust(wire.NewChain(spans...))
			lease := wire.NewLease(chain, nil)
			for _, into := range []bool{false, true} {
				b.Run(fmt.Sprintf("columns%d/fragments%d/into%t", 4*groups, fragments, into), func(b *testing.B) {
					var cursor wire.Cursor
					reader := RowReader{columns: columns, cursor: &cursor, lease: lease}
					var dst step15WideRow
					cursor = *wire.NewCursor(chain)
					if e := plan.DecodeInto(&reader, &dst); e != nil {
						b.Fatal(e)
					}
					b.ReportAllocs()
					b.SetBytes(int64(len(raw)))
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						cursor = *wire.NewCursor(chain)
						reader.index = 0
						if into {
							if e := plan.DecodeInto(&reader, &dst); e != nil {
								b.Fatal(e)
							}
						} else {
							value, e := plan.Decode(&reader)
							if e != nil {
								b.Fatal(e)
							}
							dst = value
						}
						if dst.Numbers[groups-1] != int32(groups-1) || dst.Blobs[groups-1].Len() != 4096 {
							b.Fatal("row")
						}
					}
				})
			}
			lease.Release()
		}
	}
}
