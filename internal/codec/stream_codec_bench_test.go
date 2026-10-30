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
	"bytes"
	"context"
	"fmt"
	"testing"

	cqltype "github.com/scylladb/gocql/v2/internal/cqltype"
	"github.com/scylladb/gocql/v2/internal/wire"
)

type streamRepeatSource struct {
	cell   []byte
	offset int
}

func (s *streamRepeatSource) Read(p []byte) (int, error) {
	n := copy(p, s.cell[s.offset:])
	s.offset += n
	if s.offset == len(s.cell) {
		s.offset = 0
	}
	return n, nil
}
func BenchmarkV2StreamFinalStorage(b *testing.B) {
	typ := CodecTestMust(cqltype.ScalarType(cqltype.TypeBlob))
	cols := cqltype.NewColumns(cqltype.ColumnSpec{Name: "blob", Type: typ})
	plan := CodecTestMust(StreamOneColumn(StreamBlob).Compile(cols))
	for _, size := range []int{32, 4096, 512000} {
		b.Run(fmt.Sprint(size), func(b *testing.B) {
			data := StreamCell(bytes.Repeat([]byte{0x71}, size))
			source := &streamRepeatSource{cell: data}
			const batch = 32
			first := CodecTestMust(wire.NewStream(context.Background(), source, int64(batch*len(data)), wire.StreamLimits{}))
			reader := CodecTestMust(NewStreamRowReader(cols, first))
			dst := make([]byte, 0, size)
			// Warm all reusable scopes before measuring per-page/per-row work.
			for i := 0; i < batch; i++ {
				reader.index = 0
				plan.Reset(&dst)
				if err := plan.DecodeInto(reader, &dst); err != nil {
					b.Fatal(err)
				}
			}
			b.ReportAllocs()
			b.SetBytes(int64(batch * size))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				scope, err := wire.NewStream(context.Background(), source, int64(batch*len(data)), wire.StreamLimits{})
				if err != nil {
					b.Fatal(err)
				}
				reader.scope = scope
				for row := 0; row < batch; row++ {
					reader.index = 0
					plan.Reset(&dst)
					if err := plan.DecodeInto(reader, &dst); err != nil {
						b.Fatal(err)
					}
				}
			}
			b.ReportMetric(batch, "rows/op")
		})
	}
}
func BenchmarkV2StreamingNestedElements(b *testing.B) {
	integer := CodecTestMust(cqltype.ScalarType(cqltype.TypeInt))
	typ := CodecTestMust(cqltype.ListType(integer))
	cols := cqltype.NewColumns(cqltype.ColumnSpec{Name: "list", Type: typ})
	plan := CodecTestMust(StreamOneColumn(StreamList(StreamInt32)).Compile(cols))
	for _, count := range []int{16, 1024} {
		b.Run(fmt.Sprint(count), func(b *testing.B) {
			values := make([]int32, count)
			data := streamEncodedFixture(b, typ, ListEncoder(Int32), values)
			source := &streamRepeatSource{cell: data}
			const batch = 32
			dst := make([]int32, 0, count)
			first := CodecTestMust(wire.NewStream(context.Background(), source, int64(batch*len(data)), wire.StreamLimits{}))
			reader := CodecTestMust(NewStreamRowReader(cols, first))
			for i := 0; i < batch; i++ {
				reader.index = 0
				plan.Reset(&dst)
				if err := plan.DecodeInto(reader, &dst); err != nil {
					b.Fatal(err)
				}
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				scope := CodecTestMust(wire.NewStream(context.Background(), source, int64(batch*len(data)), wire.StreamLimits{}))
				reader.scope = scope
				for row := 0; row < batch; row++ {
					reader.index = 0
					plan.Reset(&dst)
					if err := plan.DecodeInto(reader, &dst); err != nil {
						b.Fatal(err)
					}
				}
			}
			b.ReportMetric(batch*float64(count), "elements/op")
		})
	}
}
func BenchmarkV2RowDestinationFragments(b *testing.B) {
	type row struct {
		Number  int32
		Payload cqltype.Bytes
		Pad     [256]int64
	}
	integer, blob := CodecTestMust(cqltype.ScalarType(cqltype.TypeInt)), CodecTestMust(cqltype.ScalarType(cqltype.TypeBlob))
	cols := cqltype.NewColumns(cqltype.ColumnSpec{Name: "number", Type: integer}, cqltype.ColumnSpec{Name: "payload", Type: blob})
	decoder := Row(Column("number", Int32, func(r *row) *int32 { return &r.Number }), Column("payload", BlobCodec, func(r *row) *cqltype.Bytes { return &r.Payload }))
	plan := CodecTestMust(decoder.Compile(cols))
	payload := bytes.Repeat([]byte{0x31}, 512000)
	framed := append(streamEncodedFixture(b, integer, Int32, int32(1)), streamEncodedFixture(b, blob, BytesEncoder, payload)...)
	for _, fragments := range []int{1, 1024, 65536} {
		parts := make([]wire.Span, 0, fragments)
		for start := 0; start < len(framed); {
			end := min(len(framed), start+(len(framed)+fragments-1)/fragments)
			parts = append(parts, wire.Borrow(framed[start:end]))
			start = end
		}
		chain := CodecTestMust(wire.NewChain(parts...))
		for _, direct := range []bool{false, true} {
			b.Run(fmt.Sprintf("fragments=%d/into=%v", fragments, direct), func(b *testing.B) {
				var cursor wire.Cursor
				reader := RowReader{columns: cols, cursor: &cursor}
				var dst row
				cursor = *wire.NewCursor(chain)
				if err := plan.DecodeInto(&reader, &dst); err != nil {
					b.Fatal(err)
				}
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					cursor = *wire.NewCursor(chain)
					reader.index = 0
					if direct {
						if err := plan.DecodeInto(&reader, &dst); err != nil {
							b.Fatal(err)
						}
					} else {
						value, err := plan.Decode(&reader)
						if err != nil {
							b.Fatal(err)
						}
						dst = value
					}
				}
			})
		}
	}
}
