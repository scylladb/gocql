// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package codec

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"reflect"
	"testing"

	cqltype "github.com/scylladb/gocql/v2/internal/cqltype"
	"github.com/scylladb/gocql/v2/internal/wire"
)

// Wrapping the public decoder interface deliberately keeps custom codecs on
// the generic immutable ValueReader path.
type genericRowScalar[T any] struct{ Decoder[T] }

func (g genericRowScalar[T]) compileValueDecoder(typ cqltype.DataType) (Decoder[T], error) {
	decoder, err := compileValueDecoder(g.Decoder, typ)
	return genericRowScalar[T]{decoder}, err
}

func rowScalarFrame(prefix int32, body []byte) []byte {
	return append(binary.BigEndian.AppendUint32(nil, uint32(prefix)), body...)
}

func checkRowScalarEquivalent[T any](t *testing.T, kind cqltype.Type, decoder Decoder[T], input []byte, missing, expired bool) {
	t.Helper()
	for _, fragment := range []int{1, 3, 1024} {
		t.Run(fmt.Sprint(fragment), func(t *testing.T) {
			var spans []wire.Span
			for left := input; len(left) > 0; {
				n := min(fragment, len(left))
				spans = append(spans, wire.Borrow(left[:n]))
				left = left[n:]
			}
			chain, err := wire.NewChain(spans...)
			if err != nil {
				t.Fatal(err)
			}
			columns := cqltype.NewColumns(cqltype.ColumnSpec{Name: "value", Type: rowTestType(t, kind)})
			lease := wire.NewLease(chain, nil)
			if expired {
				lease.Release()
			} else {
				defer lease.Release()
			}
			direct, generic := NewRowReader(columns, chain, lease), NewRowReader(columns, chain, lease)
			direct.allowMissing, generic.allowMissing = missing, missing
			var got, want T
			directErr := readColumnInto(direct, decoder, &got)
			genericErr := readColumnInto(generic, genericRowScalar[T]{decoder}, &want)
			if fmt.Sprint(directErr) != fmt.Sprint(genericErr) {
				t.Fatalf("error direct=%v generic=%v", directErr, genericErr)
			}
			if direct.index != generic.index || direct.cursor.Remaining() != generic.cursor.Remaining() {
				t.Fatalf("cursor direct=(%d,%d) generic=(%d,%d)", direct.index, direct.cursor.Remaining(), generic.index, generic.cursor.Remaining())
			}
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("value direct=%#v generic=%#v", got, want)
			}
		})
	}
}

func TestFusedRowIntegerSemantics(t *testing.T) {
	for name, input := range map[string][]byte{
		"positive":         rowScalarFrame(4, []byte{0, 0, 0, 42}),
		"negative":         rowScalarFrame(4, []byte{0xff, 0xff, 0xff, 0xfe}),
		"minimum":          rowScalarFrame(4, []byte{0x80, 0, 0, 0}),
		"null":             rowScalarFrame(-1, nil),
		"unset":            rowScalarFrame(-2, nil),
		"invalid_negative": rowScalarFrame(math.MinInt32, nil),
		"empty":            rowScalarFrame(0, nil),
		"short_width":      rowScalarFrame(3, []byte{1, 2, 3}),
		"long_width":       rowScalarFrame(5, []byte{1, 2, 3, 4, 5}),
		"truncated_body":   rowScalarFrame(4, []byte{1, 2, 3}),
		"truncated_prefix": []byte{0, 0, 0},
	} {
		t.Run(name, func(t *testing.T) { checkRowScalarEquivalent(t, cqltype.TypeInt, Int32, input, false, false) })
	}
	checkRowScalarEquivalent(t, cqltype.TypeInt, Int32, nil, true, false)
	checkRowScalarEquivalent(t, cqltype.TypeInt, Int32, nil, true, true)
	checkRowScalarEquivalent(t, cqltype.TypeInt, Int32, rowScalarFrame(4, []byte{1, 2, 3, 4}), false, true)
	checkRowScalarEquivalent(t, cqltype.TypeBlob, Int32, rowScalarFrame(4, []byte{1, 2, 3, 4}), false, false)
	checkRowScalarEquivalent(t, cqltype.TypeTinyInt, Int8, rowScalarFrame(1, []byte{0x80}), false, false)
	checkRowScalarEquivalent(t, cqltype.TypeSmallInt, Int16, rowScalarFrame(2, []byte{0x80, 0}), false, false)
	checkRowScalarEquivalent(t, cqltype.TypeBigInt, Int64, rowScalarFrame(8, []byte{0x80, 0, 0, 0, 0, 0, 0, 0}), false, false)
	checkRowScalarEquivalent(t, cqltype.TypeCounter, CounterCodec, rowScalarFrame(8, []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xfe}), false, false)
}

func TestFusedRowBlobAndGenericFallback(t *testing.T) {
	for name, input := range map[string][]byte{
		"present":   rowScalarFrame(5, []byte("bytes")),
		"empty":     rowScalarFrame(0, nil),
		"null":      rowScalarFrame(-1, nil),
		"unset":     rowScalarFrame(-2, nil),
		"truncated": rowScalarFrame(5, []byte("byte")),
	} {
		t.Run(name, func(t *testing.T) { checkRowScalarEquivalent(t, cqltype.TypeBlob, BlobCodec, input, false, false) })
	}
	checkRowScalarEquivalent(t, cqltype.TypeBlob, BlobCodec, rowScalarFrame(1, []byte{1}), false, true)
	checkRowScalarEquivalent(t, cqltype.TypeInt, BlobCodec, rowScalarFrame(4, []byte{1, 2, 3, 4}), false, false)
	checkRowScalarEquivalent(t, cqltype.TypeFloat, Float32, rowScalarFrame(4, []byte{0, 0, 0, 0}), false, false)
	checkRowScalarEquivalent(t, cqltype.TypeBlob, BlobCodec, nil, true, false)

	columns := cqltype.NewColumns(cqltype.ColumnSpec{Name: "value", Type: rowTestType(t, cqltype.TypeBlob)})
	chain, _ := wire.NewChain(wire.Borrow(rowScalarFrame(3, []byte("abc"))))
	lease := wire.NewLease(chain, nil)
	if !lease.Retain() {
		t.Fatal("retain failed")
	}
	reader := NewRowReader(columns, chain, lease)
	var value cqltype.Bytes
	if err := readColumnInto(reader, BlobCodec, &value); err != nil {
		t.Fatal(err)
	}
	lease.Release()
	if !bytes.Equal(value.OwnedBytes(), []byte("abc")) {
		t.Fatal("retained view invalid")
	}
	lease.Release()
	if _, err := value.At(0); err != cqltype.ErrReleased {
		t.Fatalf("released view err=%v", err)
	}
}

func TestFusedRowCompiledGuardsAndDestination(t *testing.T) {
	columns := cqltype.NewColumns(cqltype.ColumnSpec{Name: "value", Type: rowTestType(t, cqltype.TypeInt)})
	compiled, err := compileValueDecoder(Int32, rowTestType(t, cqltype.TypeInt))
	if err != nil {
		t.Fatal(err)
	}
	for _, decoder := range []Decoder[int32]{Int32, compiled} {
		checkRowScalarEquivalent(t, cqltype.TypeBlob, decoder, rowScalarFrame(4, []byte{1, 2, 3, 4}), false, false)
		chain, _ := wire.NewChain(wire.Borrow(rowScalarFrame(3, []byte{1, 2, 3})))
		value := int32(99)
		if err := readColumnInto(NewRowReader(columns, chain, nil), decoder, &value); err == nil || value != 99 {
			t.Fatalf("destination overwritten: value=%d err=%v", value, err)
		}
		check := NewRowReader(columns, chain, nil)
		if err := readColumnInto(check, decoder, nil); err == nil || check.index != 1 {
			t.Fatalf("nil dst err=%v index=%d", err, check.index)
		}
	}
}

type fusedBenchRow struct {
	Key, Row int32
	Blob     cqltype.Bytes
}

func BenchmarkFusedRowPaged(b *testing.B) {
	columns := cqltype.NewColumns(
		cqltype.ColumnSpec{Name: "key", Type: rowTestType(b, cqltype.TypeInt)},
		cqltype.ColumnSpec{Name: "row", Type: rowTestType(b, cqltype.TypeInt)},
		cqltype.ColumnSpec{Name: "blob", Type: rowTestType(b, cqltype.TypeBlob)},
	)
	var data []byte
	for i := 0; i < 32; i++ {
		data = append(data, rowScalarFrame(4, []byte{0, 0, 0, 42})...)
		data = append(data, rowScalarFrame(4, []byte{0, 0, 0, byte(i)})...)
		data = append(data, rowScalarFrame(4096, make([]byte, 4096))...)
	}
	chain, _ := wire.NewChain(wire.Borrow(data))
	lease := wire.NewLease(chain, nil)
	defer lease.Release()
	for _, direct := range []bool{false, true} {
		name := "generic"
		if direct {
			name = "fused"
		}
		b.Run(name, func(b *testing.B) {
			var integer Decoder[int32] = Int32
			var blob Decoder[cqltype.Bytes] = BlobCodec
			if !direct {
				integer = genericRowScalar[int32]{integer}
				blob = genericRowScalar[cqltype.Bytes]{blob}
			}
			plan, err := Row(
				Column("key", integer, func(r *fusedBenchRow) *int32 { return &r.Key }),
				Column("row", integer, func(r *fusedBenchRow) *int32 { return &r.Row }),
				Column("blob", blob, func(r *fusedBenchRow) *cqltype.Bytes { return &r.Blob }),
			).Compile(columns)
			if err != nil {
				b.Fatal(err)
			}
			var dst fusedBenchRow
			b.ReportAllocs()
			b.SetBytes(32 * 4096)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				reader := NewRowReader(columns, chain, lease)
				for row := 0; row < 32; row++ {
					reader.index = 0
					if err := plan.DecodeInto(reader, &dst); err != nil {
						b.Fatal(err)
					}
				}
				if err := reader.cursor.RequireEnd(); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

type capturedRowInt struct {
	Decoder[int32]
	seen  *ValueReader
	calls *int
}

func (c capturedRowInt) Decode(r ValueReader) (int32, error) {
	*c.seen = r
	*c.calls++
	return c.Decoder.Decode(r)
}
func TestFusedRowCustomDecoderSnapshot(t *testing.T) {
	columns := cqltype.NewColumns(
		cqltype.ColumnSpec{Name: "first", Type: rowTestType(t, cqltype.TypeInt)},
		cqltype.ColumnSpec{Name: "second", Type: rowTestType(t, cqltype.TypeInt)},
	)
	data := append(rowScalarFrame(4, []byte{0, 0, 0, 17}), rowScalarFrame(4, []byte{0, 0, 0, 29})...)
	chain, _ := wire.NewChain(wire.Borrow(data))
	lease := wire.NewLease(chain, nil)
	defer lease.Release()
	reader := NewRowReader(columns, chain, lease)
	var snapshot ValueReader
	var calls int
	custom := capturedRowInt{Decoder: Int32, seen: &snapshot, calls: &calls}
	var first, second int32
	if err := readColumnInto(reader, custom, &first); err != nil {
		t.Fatal(err)
	}
	if err := readColumnInto(reader, Int32, &second); err != nil {
		t.Fatal(err)
	}
	stillFirst, err := Int32.Decode(snapshot)
	if err != nil || first != 17 || second != 29 || stillFirst != 17 || calls != 1 {
		t.Fatalf("custom snapshot: first=%d second=%d captured=%d calls=%d err=%v", first, second, stillFirst, calls, err)
	}
}
