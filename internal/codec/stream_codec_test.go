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
	"encoding/binary"
	"errors"
	"io"
	"math"
	"net/netip"
	"reflect"
	"sync"
	"testing"
	"unsafe"

	cqltype "github.com/scylladb/gocql/v2/internal/cqltype"
	"github.com/scylladb/gocql/v2/internal/wire"
)

type streamChunkSource struct {
	data                []byte
	offset, step, reads int
	record              func([]byte, int)
}

func (s *streamChunkSource) Read(p []byte) (int, error) {
	if s.offset == len(s.data) {
		return 0, io.EOF
	}
	n := min(len(p), len(s.data)-s.offset)
	if s.step > 0 {
		n = min(n, s.step)
	}
	if s.record != nil {
		s.record(p[:n], s.offset)
	}
	copy(p, s.data[s.offset:s.offset+n])
	s.offset += n
	s.reads++
	return n, nil
}
func StreamCell(body []byte) []byte {
	b := make([]byte, 4+len(body))
	binary.BigEndian.PutUint32(b, uint32(len(body)))
	copy(b[4:], body)
	return b
}
func streamEncodedFixture[T any](t testing.TB, typ cqltype.DataType, encoder Encoder[T], value T) []byte {
	t.Helper()
	v, err := EncodeValueBorrowed(typ, encoder, value)
	if err != nil {
		t.Fatal(err)
	}
	if v.state == cqltype.Null {
		return []byte{255, 255, 255, 255}
	}
	return StreamCell(v.Bytes().OwnedBytes())
}
func streamReaderFixture(t testing.TB, columns cqltype.Columns, source io.Reader, size int) (*StreamRowReader, *wire.Stream) {
	t.Helper()
	scope, err := wire.NewStream(context.Background(), source, int64(size), wire.StreamLimits{MaxDepth: 130, DiscardBytes: 127})
	if err != nil {
		t.Fatal(err)
	}
	r, err := NewStreamRowReader(columns, scope)
	if err != nil {
		t.Fatal(err)
	}
	return r, scope
}
func StreamDecodeFixture[T any](t testing.TB, typ cqltype.DataType, decoder StreamDecoder[T], data []byte, dst *T, step int) error {
	t.Helper()
	columns := cqltype.NewColumns(cqltype.ColumnSpec{Name: "v", Type: typ})
	r, scope := streamReaderFixture(t, columns, &streamChunkSource{data: data, step: step}, len(data))
	plan, err := StreamOneColumn(decoder).Compile(columns)
	if err != nil {
		return err
	}
	plan.Reset(dst)
	if err := plan.DecodeInto(r, dst); err != nil {
		return err
	}
	if err := r.RequireEnd(); err != nil {
		return err
	}
	return scope.RequireEnd()
}
func streamScalarCase[T any](t *testing.T, kind cqltype.Type, encoder Encoder[T], decoder StreamDecoder[T], value T) {
	t.Helper()
	typ := FoundationScalar(t, kind)
	data := streamEncodedFixture(t, typ, encoder, value)
	for step := 1; step <= len(data); step++ {
		var dst T
		if err := StreamDecodeFixture(t, typ, decoder, data, &dst, step); err != nil {
			t.Fatalf("%s split %d: %v", kind, step, err)
		}
		if !reflect.DeepEqual(dst, value) {
			t.Fatalf("%s: %#v != %#v", kind, dst, value)
		}
	}
}
func TestV2StreamingScalarDomains(t *testing.T) {
	streamScalarCase(t, cqltype.TypeBoolean, Bool, StreamBool, true)
	streamScalarCase(t, cqltype.TypeTinyInt, Int8, StreamInt8, int8(-128))
	streamScalarCase(t, cqltype.TypeSmallInt, Int16, StreamInt16, int16(-32768))
	streamScalarCase(t, cqltype.TypeInt, Int32, StreamInt32, int32(math.MinInt32))
	streamScalarCase(t, cqltype.TypeBigInt, Int64, StreamInt64, int64(math.MinInt64))
	streamScalarCase(t, cqltype.TypeFloat, Float32, StreamFloat32, float32(-1.25))
	streamScalarCase(t, cqltype.TypeDouble, Float64, StreamFloat64, math.Inf(-1))
	streamScalarCase(t, cqltype.TypeCounter, CounterCodec, StreamCounter, Counter(-1))
	streamScalarCase(t, cqltype.TypeDate, DateCodec, StreamDate, Date(math.MinInt32))
	streamScalarCase(t, cqltype.TypeTime, TimeCodec, StreamTime, TimeOfDay(nanosecondsPerDay-1))
	streamScalarCase(t, cqltype.TypeTimestamp, TimestampCodec, StreamTimestamp, Timestamp(math.MinInt64))
	streamScalarCase(t, cqltype.TypeDuration, DurationCodec, StreamDuration, cqltype.Duration{Months: math.MinInt32, Days: math.MinInt32, Nanoseconds: math.MinInt64})
	streamScalarCase(t, cqltype.TypeUUID, UUIDCodec, StreamUUID, cqltype.UUID{1, 2, 3, 4, 5})
	streamScalarCase(t, cqltype.TypeTimeUUID, TimeUUIDCodec, StreamTimeUUID, TimeUUID(cqltype.NewTimeUUID()))
	streamScalarCase(t, cqltype.TypeInet, InetCodec, StreamInet, netip.MustParseAddr("2001:db8::1"))
	streamScalarCase(t, cqltype.TypeInet, InetCodec, StreamInet, netip.MustParseAddr("127.0.0.1"))
	var badBool bool
	if err := StreamDecodeFixture(t, FoundationScalar(t, cqltype.TypeBoolean), StreamBool, StreamCell([]byte{2}), &badBool, 1); err == nil {
		t.Fatal("invalid boolean accepted")
	}
	var badTime TimeOfDay
	bad := make([]byte, 8)
	binary.BigEndian.PutUint64(bad, uint64(nanosecondsPerDay))
	if err := StreamDecodeFixture(t, FoundationScalar(t, cqltype.TypeTime), StreamTime, StreamCell(bad), &badTime, 1); err == nil {
		t.Fatal("invalid time accepted")
	}
	var id TimeUUID
	if err := StreamDecodeFixture(t, FoundationScalar(t, cqltype.TypeTimeUUID), StreamTimeUUID, StreamCell(make([]byte, 16)), &id, 1); err == nil {
		t.Fatal("non-v1 UUID accepted")
	}
	var duration cqltype.Duration
	if err := StreamDecodeFixture(t, FoundationScalar(t, cqltype.TypeDuration), StreamDuration, StreamCell([]byte{1, 2, 0}), &duration, 1); err == nil {
		t.Fatal("mixed duration signs accepted")
	}
}
func TestV2StreamingBytesCapacityNullAndText(t *testing.T) {
	typ := FoundationScalar(t, cqltype.TypeText)
	payload := bytes.Repeat([]byte("A€𐍈"), 1024)
	data := StreamCell(payload)
	for _, step := range []int{1, 2, 3, 7, 32, 1024} {
		backing := make([]byte, len(payload))
		dst := backing[:0]
		if err := StreamDecodeFixture(t, typ, StreamText, data, &dst, step); err != nil {
			t.Fatal(err)
		}
		if &dst[0] != &backing[0] || !bytes.Equal(dst, payload) {
			t.Fatal("text changed storage/content")
		}
	}
	dst := make([]byte, 0, len(payload)-1)
	source := &streamChunkSource{data: data}
	cols := cqltype.NewColumns(cqltype.ColumnSpec{Name: "text", Type: typ})
	r, _ := streamReaderFixture(t, cols, source, len(data))
	plan := CodecTestMust(StreamOneColumn(StreamText).Compile(cols))
	err := plan.DecodeInto(r, &dst)
	var capacity *StreamCapacityError
	if !errors.As(err, &capacity) || source.offset != 4 {
		t.Fatalf("capacity consumed body: %v read %d", err, source.offset)
	}
	var invalid []byte = make([]byte, 0, 10)
	for _, bad := range [][]byte{{0xff}, {0xe0, 0x80, 0x80}, {0xed, 0xa0, 0x80}, {0xf4, 0x90, 0x80, 0x80}, {0xe2, 0x82}} {
		if err := StreamDecodeFixture(t, typ, StreamText, StreamCell(bad), &invalid, 1); err == nil {
			t.Fatalf("invalid text %x", bad)
		}
	}
	if err := StreamDecodeFixture(t, FoundationScalar(t, cqltype.TypeAscii), StreamASCII, StreamCell([]byte{0x80}), &invalid, 1); err == nil {
		t.Fatal("invalid ASCII accepted")
	}
	backing := make([]byte, 12)
	nullable := cqltype.Nullable[[]byte]{Value: backing[:6], Valid: true}
	if err := StreamDecodeFixture(t, typ, StreamNullable(StreamText), []byte{255, 255, 255, 255}, &nullable, 1); err != nil || nullable.Valid || len(nullable.Value) != 0 || cap(nullable.Value) != 12 {
		t.Fatalf("nullable reset: %+v %v", nullable, err)
	}
	if err := StreamDecodeFixture(t, typ, StreamNullable(StreamText), StreamCell(nil), &nullable, 1); err != nil || !nullable.Valid || len(nullable.Value) != 0 || cap(nullable.Value) != 12 {
		t.Fatal("present empty lost null distinction", err)
	}
	if err := StreamDecodeFixture(t, typ, StreamText, []byte{255, 255, 255, 254}, &invalid, 1); err == nil {
		t.Fatal("unset result accepted")
	}
	empty := cqltype.EmptyValueOf[int32]{Value: 1}
	if err := StreamDecodeFixture(t, FoundationScalar(t, cqltype.TypeInt), StreamEmpty(StreamInt32), StreamCell(nil), &empty, 1); err != nil || !empty.Empty {
		t.Fatal("logical Empty", err)
	}
	if _, err := StreamEmpty(StreamText).Compile(typ); err == nil {
		t.Fatal("text Empty accepted")
	}
}
func TestV2StreamingDirectReadTargets(t *testing.T) {
	payload := bytes.Repeat([]byte{0x71}, 500<<10)
	data := StreamCell(payload)
	dst := make([]byte, 0, len(payload))
	start := uintptr(unsafe.Pointer(unsafe.SliceData(dst)))
	end := start + uintptr(cap(dst))
	reads := 0
	source := &streamChunkSource{data: data, step: 4093, record: func(p []byte, offset int) {
		if offset < 4 {
			return
		}
		ptr := uintptr(unsafe.Pointer(unsafe.SliceData(p)))
		if ptr < start || ptr+uintptr(len(p)) > end {
			t.Fatal("payload source was read into driver storage")
		}
		reads++
	}}
	typ := FoundationScalar(t, cqltype.TypeBlob)
	cols := cqltype.NewColumns(cqltype.ColumnSpec{Name: "v", Type: typ})
	r, scope := streamReaderFixture(t, cols, source, len(data))
	p := CodecTestMust(StreamOneColumn(StreamBlob).Compile(cols))
	if err := p.DecodeInto(r, &dst); err != nil {
		t.Fatal(err)
	}
	if err := scope.RequireEnd(); err != nil || reads < 2 || !bytes.Equal(dst, payload) {
		t.Fatal("direct ranges", err)
	}
}
func TestV2StreamingNumericPayloadStorage(t *testing.T) {
	varint := FoundationScalar(t, cqltype.TypeVarint)
	dst := make([]byte, 0, 6)
	want := []byte{0xff, 0xff, 0x80}
	if err := StreamDecodeFixture(t, varint, StreamVarint, StreamCell(want), &dst, 1); err != nil || !bytes.Equal(dst, want) {
		t.Fatal("signed varint bytes", err)
	}
	if err := StreamDecodeFixture(t, varint, StreamVarint, StreamCell(nil), &dst, 1); err == nil {
		t.Fatal("empty varint accepted")
	}
	decimal := FoundationScalar(t, cqltype.TypeDecimal)
	value := Decimal{Scale: -7, Unscaled: CodecTestMust(VarintFromBytes([]byte{0x80, 0}))}
	data := streamEncodedFixture(t, decimal, DecimalCodec, value)
	out := StreamDecimalValue{Unscaled: make([]byte, 0, 2)}
	if err := StreamDecodeFixture(t, decimal, StreamDecimal, data, &out, 1); err != nil || out.Scale != -7 || !bytes.Equal(out.Unscaled, []byte{0x80, 0}) {
		t.Fatal("decimal", out, err)
	}
	source := &streamChunkSource{data: data}
	cols := cqltype.NewColumns(cqltype.ColumnSpec{Name: "d", Type: decimal})
	reader, _ := streamReaderFixture(t, cols, source, len(data))
	plan := CodecTestMust(StreamOneColumn(StreamDecimal).Compile(cols))
	short := StreamDecimalValue{Unscaled: make([]byte, 0, 1)}
	if err := plan.DecodeInto(reader, &short); err == nil || source.offset != 4 {
		t.Fatalf("decimal capacity preflight: %v offset %d", err, source.offset)
	}
}

func TestV2StreamingNestedCollectionCapacityAndBacking(t *testing.T) {
	blob := FoundationScalar(t, cqltype.TypeBlob)
	list := CodecTestMust(cqltype.ListType(blob))
	data := streamEncodedFixture(t, list, ListEncoder(BytesEncoder), [][]byte{[]byte("one"), []byte("two")})
	first, second := make([]byte, 8), make([]byte, 8)
	out := [][]byte{first[:0], second[:0]}
	if err := StreamDecodeFixture(t, list, StreamList(StreamBlob), data, &out, 1); err != nil || len(out) != 2 || string(out[0]) != "one" || &out[0][0] != &first[0] || &out[1][0] != &second[0] {
		t.Fatal("nested storage", out, err)
	}
	// Decode reslices seeded slots beyond len, preserving their child backing.
	out = out[:0]
	if err := StreamDecodeFixture(t, list, StreamList(StreamBlob), data, &out, 3); err != nil || &out[0][0] != &first[0] {
		t.Fatal("reused nested storage", err)
	}
	source := &streamChunkSource{data: data}
	cols := cqltype.NewColumns(cqltype.ColumnSpec{Name: "v", Type: list})
	r, _ := streamReaderFixture(t, cols, source, len(data))
	plan := CodecTestMust(StreamOneColumn(StreamList(StreamBlob)).Compile(cols))
	small := make([][]byte, 0, 1)
	if err := plan.DecodeInto(r, &small); err == nil || source.offset != 8 {
		t.Fatalf("outer count capacity %v offset %d", err, source.offset)
	}
	missingChild := [][]byte{make([]byte, 0, 2), make([]byte, 0, 8)}
	if err := StreamDecodeFixture(t, list, StreamList(StreamBlob), data, &missingChild, 1); err == nil {
		t.Fatal("child capacity grew")
	}
	m := CodecTestMust(cqltype.MapType(blob, list))
	input := []Entry[[]byte, [][]byte]{{Key: []byte("k"), Value: [][]byte{[]byte("one"), []byte("two")}}}
	mdata := streamEncodedFixture(t, m, MapEncoder(BytesEncoder, ListEncoder(BytesEncoder)), input)
	mapped := []StreamMapEntry[[]byte, [][]byte]{{Key: make([]byte, 0, 4), Value: [][]byte{first[:0], second[:0]}}}
	if err := StreamDecodeFixture(t, m, StreamMap(StreamBlob, StreamList(StreamBlob)), mdata, &mapped, 1); err != nil || string(mapped[0].Key) != "k" || string(mapped[0].Value[1]) != "two" {
		t.Fatal("map", mapped, err)
	}
	set := CodecTestMust(cqltype.SetType(blob))
	setData := streamEncodedFixture(t, set, SetEncoder(BytesEncoder), [][]byte{[]byte("x")})
	out = out[:0]
	if err := StreamDecodeFixture(t, set, StreamSet(StreamBlob), setData, &out, 1); err != nil || string(out[0]) != "x" {
		t.Fatal("set", err)
	}
	if _, err := StreamList(StreamInt32).Compile(list); err == nil {
		t.Fatal("nested mismatch accepted")
	}
}
func TestV2StreamingVectors(t *testing.T) {
	integer := FoundationScalar(t, cqltype.TypeInt)
	text := FoundationScalar(t, cqltype.TypeText)
	fixed := CodecTestMust(cqltype.VectorDataType(integer, 3))
	data := streamEncodedFixture(t, fixed, VectorEncoder(Int32), []int32{1, -2, 3})
	out := make([]int32, 0, 3)
	if err := StreamDecodeFixture(t, fixed, StreamVector(StreamInt32), data, &out, 1); err != nil || !reflect.DeepEqual(out, []int32{1, -2, 3}) {
		t.Fatal("fixed vector", out, err)
	}
	if err := StreamDecodeFixture(t, fixed, StreamVector(StreamInt32), StreamCell(data[4:len(data)-1]), &out, 1); err == nil {
		t.Fatal("short vector accepted")
	}
	variable := CodecTestMust(cqltype.VectorDataType(text, 2))
	vdata := streamEncodedFixture(t, variable, VectorEncoder(StringEncoder), []string{"€", ""})
	vout := [][]byte{make([]byte, 0, 4), make([]byte, 0, 1)}
	if err := StreamDecodeFixture(t, variable, StreamVector(StreamText), vdata, &vout, 1); err != nil || string(vout[0]) != "€" || len(vout[1]) != 0 {
		t.Fatal("variable vector", vout, err)
	}
	nested := CodecTestMust(cqltype.VectorDataType(fixed, 2))
	nData := streamEncodedFixture(t, nested, VectorEncoder(VectorEncoder(Int32)), [][]int32{{1, 2, 3}, {4, 5, 6}})
	nestedOut := [][]int32{make([]int32, 0, 3), make([]int32, 0, 3)}
	if err := StreamDecodeFixture(t, nested, StreamVector(StreamVector(StreamInt32)), nData, &nestedOut, 1); err != nil || nestedOut[1][2] != 6 {
		t.Fatal("nested vector", nestedOut, err)
	}
}
func TestV2StreamingRecordsAndEncodedOutput(t *testing.T) {
	type record struct {
		ID       int32
		Name     cqltype.Nullable[[]byte]
		Unmapped int
	}
	integer, text := FoundationScalar(t, cqltype.TypeInt), FoundationScalar(t, cqltype.TypeText)
	tuple := CodecTestMust(cqltype.TupleType(integer, text))
	encoder := TupleEncoder(Params(ParamAt(0, Int32, func(r record) int32 { return r.ID }), ParamAt(1, NullableEncoder(BytesEncoder), func(r record) cqltype.Nullable[[]byte] { return r.Name })))
	_ = encoder // Text uses a typed string input fixture below.
	params := TupleEncoder(PairParams(1, OneParam(Int32), OneParam(NullableEncoder(StringEncoder))))
	data := streamEncodedFixture(t, tuple, params, Pair[int32, cqltype.Nullable[string]]{First: 7, Second: cqltype.Nullable[string]{Value: "name", Valid: true}})
	row := StreamRow(func(r *record) { r.Unmapped = 0 }, StreamColumnAt(0, StreamInt32, func(r *record) *int32 { return &r.ID }), StreamColumnAt(1, StreamNullable(StreamText), func(r *record) *cqltype.Nullable[[]byte] { return &r.Name }))
	backing := make([]byte, 10)
	out := record{Name: cqltype.Nullable[[]byte]{Value: backing[:0]}, Unmapped: 9}
	if err := StreamDecodeFixture(t, tuple, StreamTuple(row), data, &out, 1); err != nil || out.ID != 7 || string(out.Name.Value) != "name" || out.Unmapped != 0 || &out.Name.Value[0] != &backing[0] {
		t.Fatal("tuple", out, err)
	}
	// Omitted nullable suffix must preserve destination capacity and reset Valid.
	short := StreamCell(data[4:12])
	if err := StreamDecodeFixture(t, tuple, StreamTuple(row), short, &out, 1); err != nil || out.Name.Valid || cap(out.Name.Value) != 10 {
		t.Fatal("omitted suffix", out, err)
	}
	udt := CodecTestMust(cqltype.UDTType("ks", "entry", cqltype.ColumnSpec{Name: "id", Type: integer}, cqltype.ColumnSpec{Name: "name", Type: text}))
	named := StreamRow(func(r *record) { r.Unmapped = 0 }, StreamColumn("id", StreamInt32, func(r *record) *int32 { return &r.ID }), StreamColumn("name", StreamNullable(StreamText), func(r *record) *cqltype.Nullable[[]byte] { return &r.Name }))
	decoder := StreamUDT(UDTOptions{Keyspace: "ks", Name: "entry"}, named)
	if err := StreamDecodeFixture(t, udt, decoder, data, &out, 1); err != nil || out.ID != 7 {
		t.Fatal("UDT", err)
	}
	if _, err := StreamUDT(UDTOptions{}, named).Compile(udt); err == nil {
		t.Fatal("missing identity accepted")
	}
	if _, err := StreamUDT(UDTOptions{Keyspace: "ks", Name: "wrong"}, named).Compile(udt); err == nil {
		t.Fatal("wrong identity accepted")
	}
	strict := StreamRow(func(r *record) {}, StreamColumn("id", StreamInt32, func(r *record) *int32 { return &r.ID }))
	if _, err := StreamUDT(UDTOptions{Keyspace: "ks", Name: "entry"}, strict).Compile(udt); err == nil {
		t.Fatal("extra UDT field accepted without policy")
	}
	relaxed := StreamUDT(UDTOptions{Keyspace: "ks", Name: "entry", AllowExtraDatabaseFields: true}, strict)
	if err := StreamDecodeFixture(t, udt, relaxed, data, &out, 1); err != nil {
		t.Fatal("checked UDT skip", err)
	}
	for _, typ := range []cqltype.DataType{tuple, udt} {
		encoded := StreamEncodedValue{Bytes: make([]byte, 0, len(data)-4)}
		if err := StreamDecodeFixture(t, typ, StreamEncoded, data, &encoded, 1); err != nil || !bytes.Equal(encoded.Bytes, data[4:]) || !encoded.Type.Equal(typ) {
			t.Fatalf("encoded %s: %x %v", typ, encoded.Bytes, err)
		}
	}
	// Encoded output must still reject malformed nested semantics.
	bad := append([]byte(nil), data...)
	bad[len(bad)-1] = 0xff
	encoded := StreamEncodedValue{Bytes: make([]byte, 0, len(data))}
	if err := StreamDecodeFixture(t, tuple, StreamEncoded, bad, &encoded, 1); err == nil {
		t.Fatal("encoded output skipped text validation")
	}
	zero := CodecTestMust(cqltype.TupleType())
	unit := Unit{}
	empty := StreamCell(nil)
	if err := StreamDecodeFixture(t, zero, StreamTuple(StreamRow(func(*Unit) {})), empty, &unit, 1); err != nil {
		t.Fatal("zero tuple", err)
	}
	wrapped := cqltype.EmptyValueOf[Unit]{}
	if err := StreamDecodeFixture(t, zero, StreamEmpty(StreamTuple(StreamRow(func(*Unit) {}))), empty, &wrapped, 1); err != nil || !wrapped.Empty {
		t.Fatal("zero tuple Empty", err)
	}
}
func TestV2StreamingSkippedValuesAreBoundedAndChecked(t *testing.T) {
	boolean, text, blob := FoundationScalar(t, cqltype.TypeBoolean), FoundationScalar(t, cqltype.TypeText), FoundationScalar(t, cqltype.TypeBlob)
	ignored := StreamRow(func(*Unit) {})
	for _, tc := range []struct {
		typ  cqltype.DataType
		body []byte
	}{{boolean, []byte{2}}, {text, []byte{0xff}}, {CodecTestMust(cqltype.ListType(boolean)), []byte{0, 0, 0, 1, 0, 0, 0, 1, 2}}} {
		cols := cqltype.NewColumns(cqltype.ColumnSpec{Name: "ignored", Type: tc.typ})
		data := StreamCell(tc.body)
		r, _ := streamReaderFixture(t, cols, &streamChunkSource{data: data, step: 1}, len(data))
		plan := CodecTestMust(ignored.Compile(cols))
		dst := Unit{}
		if err := plan.DecodeInto(r, &dst); err == nil {
			t.Fatal("malformed ignored value accepted")
		}
	}
	cols := cqltype.NewColumns(cqltype.ColumnSpec{Name: "large", Type: blob})
	data := StreamCell(make([]byte, 8<<20))
	source := &streamChunkSource{data: data}
	r, scope := streamReaderFixture(t, cols, source, len(data))
	plan := CodecTestMust(ignored.Compile(cols))
	dst := Unit{}
	if err := plan.DecodeInto(r, &dst); err != nil {
		t.Fatal(err)
	}
	if err := scope.RequireEnd(); err != nil {
		t.Fatal(err)
	}
	if len(r.shared.discard) != 127 || source.reads > (len(data)/127)+3 {
		t.Fatal("discard storage/read bound", len(r.shared.discard), source.reads)
	}
}
func TestV2StreamingPlanConcurrencyAndNilDestinations(t *testing.T) {
	integer := FoundationScalar(t, cqltype.TypeInt)
	cols := cqltype.NewColumns(cqltype.ColumnSpec{Name: "v", Type: integer})
	plan := CodecTestMust(StreamOneColumn(StreamInt32).Compile(cols))
	data := streamEncodedFixture(t, integer, Int32, int32(42))
	var group sync.WaitGroup
	for i := 0; i < 8; i++ {
		group.Add(1)
		go func() {
			defer group.Done()
			for n := 0; n < 20; n++ {
				r, _ := streamReaderFixture(t, cols, bytes.NewReader(data), len(data))
				var dst int32
				plan.Reset(&dst)
				if err := plan.DecodeInto(r, &dst); err != nil || dst != 42 {
					t.Error("shared immutable plan", err)
				}
			}
		}()
	}
	group.Wait()
	r, _ := streamReaderFixture(t, cols, bytes.NewReader(data), len(data))
	var validation *cqltype.ValidationError
	if err := plan.DecodeInto(r, nil); !errors.As(err, &validation) {
		t.Fatal("nil destination", err)
	}
}

func TestV2StreamingEncodedNestedFinalReadIdentity(t *testing.T) {
	blob := FoundationScalar(t, cqltype.TypeBlob)
	list := CodecTestMust(cqltype.ListType(blob))
	payload := bytes.Repeat([]byte{0x72}, 300<<10)
	data := streamEncodedFixture(t, list, ListEncoder(BytesEncoder), [][]byte{payload})
	dst := StreamEncodedValue{Bytes: make([]byte, 0, len(data)-4)}
	start := uintptr(unsafe.Pointer(unsafe.SliceData(dst.Bytes)))
	end := start + uintptr(cap(dst.Bytes))
	bulk := 0
	source := &streamChunkSource{data: data, step: 4093, record: func(p []byte, offset int) {
		if offset < 12 {
			return
		}
		if len(p) < 64 {
			return
		}
		address := uintptr(unsafe.Pointer(unsafe.SliceData(p)))
		if address < start || address+uintptr(len(p)) > end {
			t.Fatal("encoded payload staged in driver scratch")
		}
		bulk++
	}}
	columns := cqltype.NewColumns(cqltype.ColumnSpec{Name: "encoded", Type: list})
	r, _ := streamReaderFixture(t, columns, source, len(data))
	plan := CodecTestMust(StreamOneColumn(StreamEncoded).Compile(columns))
	if err := plan.DecodeInto(r, &dst); err != nil || !bytes.Equal(dst.Bytes, data[4:]) || bulk == 0 {
		t.Fatal("encoded final storage", err)
	}
	if r.shared.discard != nil {
		t.Fatal("encoded output allocated discard storage")
	}
}
func TestV2StreamingRowsDuplicateNamesAndMissingUDTFields(t *testing.T) {
	integer := FoundationScalar(t, cqltype.TypeInt)
	columns := cqltype.NewColumns(cqltype.ColumnSpec{Name: "dup", Type: integer}, cqltype.ColumnSpec{Name: "dup", Type: integer})
	data := append(StreamCell([]byte{0, 0, 0, 1}), StreamCell([]byte{0, 0, 0, 2})...)
	decoder := StreamRow(func(*Pair[int32, int32]) {}, StreamColumnAt(0, StreamInt32, func(p *Pair[int32, int32]) *int32 { return &p.First }), StreamColumnAt(1, StreamInt32, func(p *Pair[int32, int32]) *int32 { return &p.Second }))
	plan := CodecTestMust(decoder.Compile(columns))
	reader, _ := streamReaderFixture(t, columns, bytes.NewReader(data), len(data))
	var dst Pair[int32, int32]
	plan.Reset(&dst)
	if err := plan.DecodeInto(reader, &dst); err != nil || dst.First != 1 || dst.Second != 2 {
		t.Fatal("duplicate positional names", dst, err)
	}
	if _, err := StreamRow(func(*int32) {}, StreamColumn("dup", StreamInt32, func(v *int32) *int32 { return v })).Compile(columns); err == nil {
		t.Fatal("ambiguous result alias accepted")
	}
	type value struct{ ID, Missing int32 }
	udt := CodecTestMust(cqltype.UDTType("ks", "entry", cqltype.ColumnSpec{Name: "id", Type: integer}))
	row := StreamRow(func(*value) {}, StreamColumn("id", StreamInt32, func(v *value) *int32 { return &v.ID }), StreamColumn("missing", StreamInt32, func(v *value) *int32 { return &v.Missing }))
	out := value{Missing: 9}
	options := UDTOptions{Keyspace: "ks", Name: "entry", AllowMissingFields: true}
	if err := StreamDecodeFixture(t, udt, StreamUDT(options, row), StreamCell(StreamCell([]byte{0, 0, 0, 7})), &out, 1); err != nil || out.ID != 7 || out.Missing != 0 {
		t.Fatal("missing application field reset", out, err)
	}
	extra := CodecTestMust(cqltype.UDTType("ks", "entry", cqltype.ColumnSpec{Name: "id", Type: integer}, cqltype.ColumnSpec{Name: "extra", Type: integer}))
	options.AllowExtraDatabaseFields = true
	var single int32
	if err := StreamDecodeFixture(t, extra, StreamUDT(options, StreamOneColumn(StreamInt32)), StreamCell(data), &single, 1); err != nil || single != 1 {
		t.Fatal("single-column UDT policy", single, err)
	}
}
