// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package codec

import (
	"bytes"
	"fmt"
	"testing"

	cqltype "github.com/scylladb/gocql/v2/internal/cqltype"
	"github.com/scylladb/gocql/v2/internal/wire"
)

var step15DynamicSink DynamicRow
var step15ValueSink Value
var step15CollectSink []int32
var step15UUIDSink cqltype.UUID

// These two cases exactly reproduce the original benchmark work: varchar into
// existing []byte and UUID into a fixed scalar. Borrowed Text is measured apart.
func BenchmarkStep15BaselineEquivalent(b *testing.B) {
	b.Run("Varchar1024IntoBytes", func(b *testing.B) {
		data := bytes.Repeat([]byte{'x'}, 1024)
		typ := CodecTestMust(cqltype.ScalarType(cqltype.TypeVarchar))
		r := NewValueReader(typ, cqltype.Present, CodecTestMust(wire.NewChain(wire.Borrow(data))), nil)
		dst := make([]byte, 1024)
		b.ReportAllocs()
		b.ResetTimer()
		for b.Loop() {
			text, e := TextCodec.Decode(r)
			if e != nil {
				b.Fatal(e)
			}
			if _, e = text.Bytes().CopyTo(dst); e != nil {
				b.Fatal(e)
			}
		}
	})
	b.Run("UUID", func(b *testing.B) {
		r := ScalarReader(b, cqltype.TypeUUID, cqltype.Present, make([]byte, 16))
		b.ReportAllocs()
		b.ResetTimer()
		for b.Loop() {
			v, e := UUIDCodec.Decode(r)
			if e != nil {
				b.Fatal(e)
			}
			step15UUIDSink = v
		}
	})
}
func BenchmarkStep15BorrowedSizes(b *testing.B) {
	for _, size := range []int{0, 32, 4096, 500 << 10} {
		for _, text := range []bool{false, true} {
			b.Run(fmt.Sprintf("bytes%d/text%t", size, text), func(b *testing.B) {
				data := bytes.Repeat([]byte{'q'}, size)
				typ := CodecTestMust(cqltype.ScalarType(cqltype.TypeBlob))
				if text {
					typ = CodecTestMust(cqltype.ScalarType(cqltype.TypeText))
				}
				stringData := string(data)
				b.Run("bind", func(b *testing.B) {
					b.ReportAllocs()
					b.SetBytes(int64(size))
					for b.Loop() {
						if text {
							if _, e := EncodeValueBorrowed(typ, StringEncoder, stringData); e != nil {
								b.Fatal(e)
							}
						} else {
							if _, e := EncodeValueBorrowed(typ, BytesEncoder, data); e != nil {
								b.Fatal(e)
							}
						}
					}
				})
				chain := CodecTestMust(wire.NewChain(wire.Borrow(data)))
				reader := NewValueReader(typ, cqltype.Present, chain, nil)
				b.Run("decode", func(b *testing.B) {
					b.ReportAllocs()
					b.SetBytes(int64(size))
					for b.Loop() {
						if text {
							v, e := TextCodec.Decode(reader)
							if e != nil {
								b.Fatal(e)
							}
							scalarTextSink = v
						} else {
							v, e := BlobCodec.Decode(reader)
							if e != nil {
								b.Fatal(e)
							}
							scalarBytesSink = v
						}
					}
				})
			})
		}
	}
}
func BenchmarkStep15DynamicRows(b *testing.B) {
	for _, cells := range []int{1, 32, 128} {
		for _, fragments := range []int{1, 1024} {
			b.Run(fmt.Sprintf("cells%d/fragments%d", cells, fragments), func(b *testing.B) {
				typ := CodecTestMust(cqltype.ScalarType(cqltype.TypeBlob))
				cols := make([]cqltype.ColumnSpec, cells)
				values := make([]Value, cells)
				for i := range cols {
					cols[i] = cqltype.ColumnSpec{Name: fmt.Sprint("v", i), Type: typ}
					values[i] = BlobValue(bytes.Repeat([]byte{'x'}, 32))
				}
				columns := cqltype.NewColumns(cols...)
				raw := cqltype.BytesView(encodeTestParams(b, columns, DynamicParams, values), nil).OwnedBytes()
				spans := make([]wire.Span, fragments)
				for i := range spans {
					spans[i] = wire.Borrow(raw[len(raw)*i/fragments : len(raw)*(i+1)/fragments])
				}
				chain := CodecTestMust(wire.NewChain(spans...))
				plan := CodecTestMust(DynamicRows.Compile(columns))
				var dst DynamicRow
				b.ReportAllocs()
				b.SetBytes(int64(len(raw)))
				b.ResetTimer()
				for b.Loop() {
					reader := NewRowReader(columns, chain, nil)
					if e := plan.DecodeInto(reader, &dst); e != nil {
						b.Fatal(e)
					}
					v, e := dst.At(cells - 1)
					if e != nil {
						b.Fatal(e)
					}
					step15ValueSink = v
				}
				step15DynamicSink = dst
			})
		}
	}
}
func BenchmarkStep15LazyAndMaterialized(b *testing.B) {
	integer := CodecTestMust(cqltype.ScalarType(cqltype.TypeInt))
	for _, n := range []int{1, 1024} {
		for _, kind := range []string{"list", "vector", "tuple", "udt", "map"} {
			b.Run(fmt.Sprintf("%s/%d", kind, n), func(b *testing.B) {
				if kind == "map" {
					typ := CodecTestMust(cqltype.MapType(integer, integer))
					entries := make([]Entry[int32, int32], n)
					encoded := CodecTestMust(EncodeValueBorrowed(typ, MapEncoder(Int32, Int32), entries))
					m := CodecTestMust(DecodeValue(MapDecoder(Int32, Int32), encoded))
					b.ReportAllocs()
					b.ResetTimer()
					for b.Loop() {
						it := m.Iterator()
						for it.Next() {
							containerIntSink = it.Value().Value
						}
						if e := it.Err(); e != nil {
							b.Fatal(e)
						}
					}
					return
				}
				typ := CodecTestMust(cqltype.ListType(integer))
				enc, dec := ListEncoder(Int32), ListDecoder(Int32)
				if kind == "vector" {
					typ = CodecTestMust(cqltype.VectorDataType(integer, uint16(n)))
					enc, dec = VectorEncoder(Int32), VectorDecoder(Int32)
				}
				if kind == "tuple" {
					tuple := CodecTestMust(cqltype.TupleType(integer))
					typ = CodecTestMust(cqltype.ListType(tuple))
					enc, dec = ListEncoder(TupleEncoder(OneParam(Int32))), ListDecoder(TupleDecoder(OneColumn(Int32)))
				}
				if kind == "udt" {
					udt := CodecTestMust(cqltype.UDTType("ks", "r", cqltype.ColumnSpec{Name: "n", Type: integer}))
					typ = CodecTestMust(cqltype.ListType(udt))
					o := UDTOptions{Keyspace: "ks", Name: "r"}
					enc, dec = ListEncoder(UDTEncoder(OneParam(Int32), o)), ListDecoder(UDTDecoder(OneColumn(Int32), o))
				}
				encoded := CodecTestMust(EncodeValueBorrowed(typ, enc, make([]int32, n)))
				seq := CodecTestMust(DecodeValue(dec, encoded))
				for _, mode := range []string{"iterate", "collect", "clone"} {
					b.Run(mode, func(b *testing.B) {
						b.ReportAllocs()
						b.ResetTimer()
						for b.Loop() {
							switch mode {
							case "iterate":
								it := seq.Iterator()
								for it.Next() {
									containerIntSink = it.Value()
								}
								if e := it.Err(); e != nil {
									b.Fatal(e)
								}
							case "collect":
								values, e := Collect(seq.Iterator())
								if e != nil {
									b.Fatal(e)
								}
								step15CollectSink = values
							case "clone":
								containerSequenceSink = seq.Clone()
							}
						}
					})
				}
			})
		}
	}
}

// The one-time row-owned physical index is setup here. Its allocation/build cost
// is measured separately by BenchmarkStep15DynamicRows. This measures actual J
// repeated late-column accesses at independently varied N and S.
func BenchmarkStep15DynamicRepeatedAccess(b *testing.B) {
	blob := CodecTestMust(cqltype.ScalarType(cqltype.TypeBlob))
	for _, n := range []int{1, 128, 4096} {
		for _, fragments := range []int{1, 256, 65536} {
			b.Run(fmt.Sprintf("N%d/S%d", n, fragments), func(b *testing.B) {
				cols := make([]cqltype.ColumnSpec, n)
				values := make([]Value, n)
				for i := range cols {
					cols[i] = cqltype.ColumnSpec{Name: fmt.Sprint("v", i), Type: blob}
					values[i] = BlobValue(make([]byte, (1<<20)/n))
				}
				columns := cqltype.NewColumns(cols...)
				raw := cqltype.BytesView(encodeTestParams(b, columns, DynamicParams, values), nil).OwnedBytes()
				parts := make([]wire.Span, fragments)
				for i := range parts {
					parts[i] = wire.Borrow(raw[len(raw)*i/fragments : len(raw)*(i+1)/fragments])
				}
				chain := CodecTestMust(wire.NewChain(parts...))
				plan := CodecTestMust(DynamicRows.Compile(columns))
				row := CodecTestMust(plan.Decode(NewRowReader(columns, chain, nil)))
				CodecTestMust(row.At(n - 1))
				b.ReportAllocs()
				b.ResetTimer()
				for b.Loop() {
					v, e := row.At(n - 1)
					if e != nil {
						b.Fatal(e)
					}
					step15ValueSink = v
				}
			})
		}
	}
}
