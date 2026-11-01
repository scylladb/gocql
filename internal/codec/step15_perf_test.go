//go:build gocql_perf

// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package codec

import (
	"bytes"
	"errors"
	"fmt"
	"net/netip"
	"testing"

	cqltype "github.com/scylladb/gocql/v2/internal/cqltype"
	"github.com/scylladb/gocql/v2/internal/wire"
)

func TestStep15BorrowIdentityAndWrites(t *testing.T) {
	for _, size := range []int{0, 32, 4096, 500 << 10} {
		for _, text := range []bool{false, true} {
			t.Run(fmt.Sprintf("size%d/text%t", size, text), func(t *testing.T) {
				data := bytes.Repeat([]byte{'q'}, size)
				typ := CodecTestMust(cqltype.ScalarType(cqltype.TypeBlob))
				if text {
					typ = CodecTestMust(cqltype.ScalarType(cqltype.TypeText))
				}
				wire.ResetPerfCounters()
				var encoded EncodedValue
				var err error
				if text {
					encoded, err = EncodeValueBorrowed(typ, StringEncoder, string(data))
				} else {
					encoded, err = EncodeValueBorrowed(typ, BytesEncoder, data)
				}
				if err != nil {
					t.Fatal(err)
				}
				c := wire.ReadPerfCounters()
				if c.CopyBytes != 0 {
					t.Fatal(c)
				}
				var view cqltype.Bytes
				if text {
					v, e := DecodeValue(TextCodec, encoded)
					if e != nil {
						t.Fatal(e)
					}
					view = v.Bytes()
				} else {
					view, err = DecodeValue(BlobCodec, encoded)
					if err != nil {
						t.Fatal(err)
					}
				}
				cqltype.EachBytes(view, func(s wire.Span) error {
					body := cqltype.BytesChain(encoded.body)
					same := false
					body.Each(func(p wire.Span) error {
						if len(s.Bytes()) > 0 && len(p.Bytes()) > 0 && &s.Bytes()[0] == &p.Bytes()[0] {
							same = true
						}
						return nil
					})
					if !same {
						t.Fatal("decoder payload backing changed")
					}
					return nil
				})
				if !text && size > 0 {
					cqltype.EachBytes(view, func(s wire.Span) error {
						if &s.Bytes()[0] != &data[0] {
							t.Fatal("bind payload copied")
						}
						return nil
					})
				}
				t.Logf("bind+decode counters=%+v payload_bytes=%d", wire.ReadPerfCounters(), size)
			})
		}
	}
	integer := CodecTestMust(cqltype.ScalarType(cqltype.TypeBigInt))
	wire.ResetPerfCounters()
	value := CodecTestMust(EncodeValueOwned(integer, Int64, int64(42)))
	c := wire.ReadPerfCounters()
	if c.ScalarWrites != 2 || c.ScalarBytes != 12 || c.CopyBytes != 0 {
		t.Fatal(c)
	}
	if CodecTestMust(DecodeValue(Int64, value)) != 42 {
		t.Fatal("scalar")
	}
	t.Logf("owned scalar: %+v", c)
}

func step15SequenceWork[T any](t *testing.T, typ cqltype.DataType, encoder Encoder[[]T], decoder Decoder[Sequence[T]], input []T) {
	t.Helper()
	encoded := CodecTestMust(EncodeValueBorrowed(typ, encoder, input))
	raw := encoded.Bytes().OwnedBytes()
	spans := make([]wire.Span, len(raw))
	for i := range raw {
		spans[i] = wire.Borrow(raw[i : i+1])
	}
	chain := CodecTestMust(wire.NewChain(spans...))
	compiled := CodecTestMust(compileValueDecoder(decoder, typ))
	ResetPerfCounters()
	wire.ResetPerfCounters()
	seq := CodecTestMust(compiled.Decode(NewValueReader(typ, cqltype.Present, chain, nil)))
	it := seq.Iterator()
	n := 0
	for it.Next() {
		_ = it.Value()
		n++
	}
	if it.Err() != nil || n != len(input) {
		t.Fatal(n, it.Err())
	}
	c := wire.ReadPerfCounters()
	comp, maps := ReadPerfCounters()
	if c.CopyBytes != 0 || c.Retains != 0 || c.Releases != 0 || comp != 0 || maps != 0 || ReadPerfTypeChecks() != 0 {
		t.Fatal(c, comp, maps)
	}
	t.Logf("elements=%d bytes=%d counts=%+v compiles=%d name_maps=%d CheckType=%d", n, len(raw), c, comp, maps, ReadPerfTypeChecks())
}
func TestStep15LazyTraversalCounts(t *testing.T) {
	integer := CodecTestMust(cqltype.ScalarType(cqltype.TypeInt))
	for _, n := range []int{1, 1024} {
		t.Run(fmt.Sprintf("list%d", n), func(t *testing.T) {
			step15SequenceWork(t, CodecTestMust(cqltype.ListType(integer)), ListEncoder(Int32), ListDecoder(Int32), make([]int32, n))
		})
		t.Run(fmt.Sprintf("vector%d", n), func(t *testing.T) {
			step15SequenceWork(t, CodecTestMust(cqltype.VectorDataType(integer, uint16(n))), VectorEncoder(Int32), VectorDecoder(Int32), make([]int32, n))
		})
		t.Run(fmt.Sprintf("tuple%d", n), func(t *testing.T) {
			tuple := CodecTestMust(cqltype.TupleType(integer))
			step15SequenceWork(t, CodecTestMust(cqltype.ListType(tuple)), ListEncoder(TupleEncoder(OneParam(Int32))), ListDecoder(TupleDecoder(OneColumn(Int32))), make([]int32, n))
		})
		t.Run(fmt.Sprintf("udt%d", n), func(t *testing.T) {
			udt := CodecTestMust(cqltype.UDTType("ks", "record", cqltype.ColumnSpec{Name: "n", Type: integer}))
			o := UDTOptions{Keyspace: "ks", Name: "record"}
			step15SequenceWork(t, CodecTestMust(cqltype.ListType(udt)), ListEncoder(UDTEncoder(OneParam(Int32), o)), ListDecoder(UDTDecoder(OneColumn(Int32), o)), make([]int32, n))
		})
		t.Run(fmt.Sprintf("map%d", n), func(t *testing.T) {
			typ := CodecTestMust(cqltype.MapType(integer, integer))
			values := make([]Entry[int32, int32], n)
			for i := range values {
				values[i] = Entry[int32, int32]{Key: int32(i), Value: int32(i)}
			}
			encoded := CodecTestMust(EncodeValueBorrowed(typ, MapEncoder(Int32, Int32), values))
			compiled := CodecTestMust(compileValueDecoder(MapDecoder(Int32, Int32), typ))
			ResetPerfCounters()
			wire.ResetPerfCounters()
			v := CodecTestMust(compiled.Decode(NewValueReader(typ, cqltype.Present, cqltype.BytesChain(encoded.body), nil)))
			it := v.Iterator()
			count := 0
			for it.Next() {
				count++
			}
			if it.Err() != nil || count != n {
				t.Fatal(count, it.Err())
			}
			c := wire.ReadPerfCounters()
			comp, maps := ReadPerfCounters()
			if c.CopyBytes != 0 || comp != 0 || maps != 0 || ReadPerfTypeChecks() != 0 {
				t.Fatal(c, comp, maps)
			}
			t.Logf("N=%d counts=%+v", n, c)
		})
	}
}

func TestStep15DynamicLazyReuse(t *testing.T) {
	integer := CodecTestMust(cqltype.ScalarType(cqltype.TypeInt))
	blob := CodecTestMust(cqltype.ScalarType(cqltype.TypeBlob))
	tuple := CodecTestMust(cqltype.TupleType(integer, blob))
	list := CodecTestMust(cqltype.ListType(tuple))
	cols := cqltype.NewColumns(cqltype.ColumnSpec{Name: "list", Type: list})
	values := make([]Value, 1024)
	for i := range values {
		values[i] = CodecTestMust(TupleValue(tuple, []Value{IntValue(int32(i)), BlobValue(bytes.Repeat([]byte{'x'}, 4096))}))
	}
	sequence := CodecTestMust(ListValue(list, values))
	source := dynamicTestRowBody(t, cols, []Value{sequence})
	plan := CodecTestMust(DynamicRows.Compile(cols))
	ResetPerfCounters()
	wire.ResetPerfCounters()
	for page := 0; page < 4; page++ {
		reader := NewRowReader(cols, source, nil)
		row := CodecTestMust(plan.Decode(reader))
		v := CodecTestMust(row.Get("list"))
		it := CodecTestMust(v.Elements())
		n := 0
		for it.Next() {
			tuple := CodecTestMust(it.Value().Elements())
			for tuple.Next() {
				_ = tuple.Value()
			}
			if tuple.Err() != nil {
				t.Fatal(tuple.Err())
			}
			n++
		}
		if it.Err() != nil || n != 1024 {
			t.Fatal(n, it.Err())
		}
	}
	comp, maps := ReadPerfCounters()
	c := wire.ReadPerfCounters()
	if comp != 0 || maps != 0 || c.CopyBytes != 0 || c.Retains != 0 || c.Releases != 0 || ReadPerfTypeChecks() != 0 {
		t.Fatal(comp, maps, c)
	}
	t.Logf("4 pages 1024 nested 4KiB payloads warm_compiles=%d name_maps=%d counts=%+v", comp, maps, c)
}

func step15OneScalarWrite[T any](t *testing.T, typ cqltype.DataType, codec Encoder[T], value T) {
	t.Helper()
	wire.ResetPerfCounters()
	encoded := CodecTestMust(EncodeValueOwned(typ, codec, value))
	c := wire.ReadPerfCounters()
	if c.ScalarWrites != 2 || c.ScalarBytes != encoded.Bytes().Len()+4 || c.CopyBytes != 0 {
		t.Fatal("framing plus one body write", c)
	}
	t.Logf("%s %+v", typ, c)
}
func TestStep15AllScalarArenaWrites(t *testing.T) {
	step15OneScalarWrite(t, CodecTestMust(cqltype.ScalarType(cqltype.TypeUUID)), UUIDCodec, cqltype.UUID{})
	step15OneScalarWrite(t, CodecTestMust(cqltype.ScalarType(cqltype.TypeInet)), InetCodec, netip.MustParseAddr("2001:db8::1"))
	step15OneScalarWrite(t, CodecTestMust(cqltype.ScalarType(cqltype.TypeDuration)), DurationCodec, cqltype.Duration{Months: 2147483647, Days: 2147483647, Nanoseconds: 9223372036854775807})
}

func TestStep15CompiledScalarSafetyAndWarmChecks(t *testing.T) {
	integer := CodecTestMust(cqltype.ScalarType(cqltype.TypeInt))
	text := CodecTestMust(cqltype.ScalarType(cqltype.TypeText))
	blob := CodecTestMust(cqltype.ScalarType(cqltype.TypeBlob))
	list := CodecTestMust(cqltype.ListType(integer))
	type result struct {
		N        int32
		Text     cqltype.Text
		Blob     cqltype.Bytes
		List     Sequence[int32]
		Nullable cqltype.Nullable[int32]
		Empty    cqltype.EmptyValueOf[int32]
		Dynamic  Value
	}
	cols := cqltype.NewColumns(cqltype.ColumnSpec{Name: "n", Type: integer}, cqltype.ColumnSpec{Name: "text", Type: text}, cqltype.ColumnSpec{Name: "blob", Type: blob}, cqltype.ColumnSpec{Name: "list", Type: list}, cqltype.ColumnSpec{Name: "nullable", Type: integer}, cqltype.ColumnSpec{Name: "empty", Type: integer}, cqltype.ColumnSpec{Name: "dynamic", Type: integer})
	body := encodeTestParams(t, cols, DynamicParams, []Value{IntValue(3), CodecTestMust(TextValue("safe-🙂")), BlobValue([]byte("borrowed")), CodecTestMust(ListValue(list, []Value{IntValue(7)})), IntValue(4), IntValue(5), IntValue(6)})
	decoder := Row(Column("n", Int32, func(r *result) *int32 { return &r.N }), Column("text", TextCodec, func(r *result) *cqltype.Text { return &r.Text }), Column("blob", BlobCodec, func(r *result) *cqltype.Bytes { return &r.Blob }), Column("list", ListDecoder(Int32), func(r *result) *Sequence[int32] { return &r.List }), Column("nullable", NullableDecoder(Int32), func(r *result) *cqltype.Nullable[int32] { return &r.Nullable }), Column("empty", EmptyCodec(Int32), func(r *result) *cqltype.EmptyValueOf[int32] { return &r.Empty }), Column("dynamic", DynamicCodec, func(r *result) *Value { return &r.Dynamic }))
	plan := CodecTestMust(decoder.Compile(cols))
	lease := wire.NewLease(body, nil)
	var dst result
	ResetPerfCounters()
	for page := 0; page < 10000; page++ {
		if e := plan.DecodeInto(NewRowReader(cols, body, lease), &dst); e != nil {
			t.Fatal(e)
		}
		it := dst.List.Iterator()
		for it.Next() {
		}
		if it.Err() != nil {
			t.Fatal(it.Err())
		}
	}
	comp, maps := ReadPerfCounters()
	if comp != 0 || maps != 0 || ReadPerfTypeChecks() != 0 {
		t.Fatal("warm recursive/type work", comp, maps, ReadPerfTypeChecks())
	}
	t.Log("10000 compiled scalar/text/blob/container/nullable/empty/dynamic rows Compile=0 CheckType=0 name_maps=0")
	changed := cqltype.NewColumns(cqltype.ColumnSpec{Name: "n", Type: text})
	direct := CodecTestMust(OneColumn(Int32).Compile(cqltype.NewColumns(cqltype.ColumnSpec{Name: "n", Type: integer})))
	if _, e := direct.Decode(NewRowReader(changed, CodecTestMust(wire.NewChain(wire.Borrow([]byte{0, 0, 0, 4, 0, 0, 0, 3}))), nil)); e == nil {
		t.Fatal("mismatched reader accepted")
	}
	lease.Release()
	if e := plan.DecodeInto(NewRowReader(cols, body, lease), &dst); !errors.Is(e, cqltype.ErrReleased) {
		t.Fatal("expired compiled reader", e)
	}
}
