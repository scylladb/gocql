// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package gocql

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"math"
	"math/big"
	"net/netip"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/scylladb/gocql/v2/internal/wire"
)

func scalarType(t testing.TB, kind Type) DataType {
	t.Helper()
	typ, err := ScalarType(kind)
	if err != nil {
		t.Fatal(err)
	}
	return typ
}
func scalarHex(t testing.TB, s string) []byte {
	t.Helper()
	b, err := hex.DecodeString(s)
	if err != nil {
		t.Fatal(err)
	}
	return b
}
func scalarReader(t testing.TB, kind Type, state CellState, chunks ...[]byte) ValueReader {
	t.Helper()
	spans := make([]wire.Span, len(chunks))
	for i, b := range chunks {
		spans[i] = wire.Borrow(b)
	}
	chain, err := wire.NewChain(spans...)
	if err != nil {
		t.Fatal(err)
	}
	return newValueReader(scalarType(t, kind), state, chain, nil)
}
func scalarEncode[T any](t testing.TB, codec Encoder[T], kind Type, v T) ([]byte, error) {
	t.Helper()
	builder := wire.NewBuilder(16)
	w := newValueWriter(builder, scalarType(t, kind), true)
	if err := codec.Encode(w, v); err != nil {
		abortValueWriter(w)
		if builder.Len() != 0 {
			t.Fatal("failed encoding retained a partial cell")
		}
		return nil, err
	}
	if err := finishValueWriter(w); err != nil {
		return nil, err
	}
	chain, err := builder.Freeze()
	if err != nil {
		return nil, err
	}
	b := make([]byte, chain.Len())
	_, err = chain.CopyTo(b)
	return b, err
}
func scalarGolden[T comparable](t *testing.T, codec Codec[T], kind Type, value T, encoded string) {
	t.Helper()
	want := scalarHex(t, encoded)
	frame, err := scalarEncode(t, codec, kind, value)
	if err != nil {
		t.Fatal(err)
	}
	if len(frame) < 4 || binary.BigEndian.Uint32(frame[:4]) != uint32(len(want)) || !bytes.Equal(frame[4:], want) {
		t.Fatalf("encoding: got %x, want body %x", frame, want)
	}
	for split := 0; split <= len(want); split++ {
		r := scalarReader(t, kind, Present, want[:split], want[split:])
		got, err := codec.Decode(r)
		if err != nil {
			t.Fatalf("split %d: %v", split, err)
		}
		if got != value {
			t.Fatalf("split %d: wrong decoded scalar", split)
		}
	}
	for _, state := range []CellState{Null, Unset, Empty} {
		if _, err := codec.Decode(scalarReader(t, kind, state)); err == nil {
			t.Fatalf("accepted state %d", state)
		}
	}
}
func TestV2ScalarGolden(t *testing.T) {
	t.Run("boolean", func(t *testing.T) { scalarGolden(t, Bool, TypeBoolean, true, "01") })
	t.Run("tinyint", func(t *testing.T) { scalarGolden(t, Int8, TypeTinyInt, int8(-128), "80") })
	t.Run("smallint", func(t *testing.T) { scalarGolden(t, Int16, TypeSmallInt, int16(-32768), "8000") })
	t.Run("int", func(t *testing.T) { scalarGolden(t, Int32, TypeInt, int32(-2147483648), "80000000") })
	t.Run("bigint", func(t *testing.T) { scalarGolden(t, Int64, TypeBigInt, int64(math.MinInt64), "8000000000000000") })
	t.Run("counter", func(t *testing.T) { scalarGolden(t, CounterCodec, TypeCounter, Counter(-42), "ffffffffffffffd6") })
	t.Run("float", func(t *testing.T) { scalarGolden(t, Float32, TypeFloat, float32(1.5), "3fc00000") })
	t.Run("double", func(t *testing.T) { scalarGolden(t, Float64, TypeDouble, -2.5, "c004000000000000") })
	t.Run("date_epoch", func(t *testing.T) { scalarGolden(t, DateCodec, TypeDate, Date(0), "80000000") })
	t.Run("date_min", func(t *testing.T) { scalarGolden(t, DateCodec, TypeDate, Date(math.MinInt32), "00000000") })
	t.Run("date_max", func(t *testing.T) { scalarGolden(t, DateCodec, TypeDate, Date(math.MaxInt32), "ffffffff") })
	t.Run("time", func(t *testing.T) { scalarGolden(t, TimeCodec, TypeTime, TimeOfDay(1), "0000000000000001") })
	t.Run("timestamp", func(t *testing.T) { scalarGolden(t, TimestampCodec, TypeTimestamp, Timestamp(-1), "ffffffffffffffff") })
	t.Run("uuid", func(t *testing.T) {
		scalarGolden(t, UUIDCodec, TypeUUID, UUID{0, 1, 2, 3, 4, 5, 0x46, 7, 8, 9, 10, 11, 12, 13, 14, 15}, "000102030405460708090a0b0c0d0e0f")
	})
	t.Run("timeuuid", func(t *testing.T) {
		scalarGolden(t, TimeUUIDCodec, TypeTimeUUID, TimeUUID{0, 1, 2, 3, 4, 5, 0x16, 7, 8, 9, 10, 11, 12, 13, 14, 15}, "000102030405160708090a0b0c0d0e0f")
	})
	t.Run("ipv4", func(t *testing.T) { scalarGolden(t, InetCodec, TypeInet, netip.MustParseAddr("192.0.2.1"), "c0000201") })
	t.Run("ipv6", func(t *testing.T) {
		scalarGolden(t, InetCodec, TypeInet, netip.MustParseAddr("2001:db8::1"), "20010db8000000000000000000000001")
	})
	t.Run("ipv4_mapped", func(t *testing.T) {
		scalarGolden(t, InetCodec, TypeInet, netip.MustParseAddr("::ffff:192.0.2.1"), "00000000000000000000ffffc0000201")
	})
	t.Run("duration", func(t *testing.T) {
		scalarGolden(t, DurationCodec, TypeDuration, Duration{Months: 1, Days: 2, Nanoseconds: 3}, "020406")
	})
	t.Run("negative_duration", func(t *testing.T) {
		scalarGolden(t, DurationCodec, TypeDuration, Duration{Months: -1, Days: -2, Nanoseconds: -3}, "010305")
	})
	t.Run("duration_min", func(t *testing.T) {
		scalarGolden(t, DurationCodec, TypeDuration, Duration{Months: math.MinInt32, Days: math.MinInt32, Nanoseconds: math.MinInt64}, "f0fffffffff0ffffffffffFFFFFFFFFFFFFFFF")
	})
}

func scalarRejectLengths[T any](t *testing.T, codec Codec[T], kind Type, width int) {
	t.Helper()
	for n := 0; n <= width+1; n++ {
		if n == width {
			continue
		}
		if _, err := codec.Decode(scalarReader(t, kind, Present, make([]byte, n))); err == nil {
			t.Fatalf("%s accepted length %d", kind, n)
		}
	}
}
func TestV2ScalarMalformed(t *testing.T) {
	scalarRejectLengths(t, Bool, TypeBoolean, 1)
	scalarRejectLengths(t, Int8, TypeTinyInt, 1)
	scalarRejectLengths(t, Int16, TypeSmallInt, 2)
	scalarRejectLengths(t, Int32, TypeInt, 4)
	scalarRejectLengths(t, Int64, TypeBigInt, 8)
	scalarRejectLengths(t, Float32, TypeFloat, 4)
	scalarRejectLengths(t, Float64, TypeDouble, 8)
	scalarRejectLengths(t, CounterCodec, TypeCounter, 8)
	scalarRejectLengths(t, DateCodec, TypeDate, 4)
	scalarRejectLengths(t, TimeCodec, TypeTime, 8)
	scalarRejectLengths(t, TimestampCodec, TypeTimestamp, 8)
	scalarRejectLengths(t, UUIDCodec, TypeUUID, 16)
	scalarRejectLengths(t, TimeUUIDCodec, TypeTimeUUID, 16)
	if _, err := Bool.Decode(scalarReader(t, TypeBoolean, Present, []byte{2})); err == nil {
		t.Fatal("accepted boolean 2")
	}
	for _, v := range []TimeOfDay{-1, TimeOfDay(nanosecondsPerDay)} {
		if _, err := scalarEncode(t, TimeCodec, TypeTime, v); err == nil {
			t.Fatal("accepted invalid time")
		}
		var data [8]byte
		binary.BigEndian.PutUint64(data[:], uint64(v))
		if _, err := TimeCodec.Decode(scalarReader(t, TypeTime, Present, data[:])); err == nil {
			t.Fatal("decoded invalid time")
		}
	}
	if _, err := scalarEncode(t, TimeUUIDCodec, TypeTimeUUID, TimeUUID{}); err == nil {
		t.Fatal("encoded invalid timeuuid")
	}
	for _, v := range []netip.Addr{{}, netip.MustParseAddr("fe80::1%eth0")} {
		if _, err := scalarEncode(t, InetCodec, TypeInet, v); err == nil {
			t.Fatal("encoded invalid inet")
		}
	}
	for n := 0; n <= 17; n++ {
		if n == 4 || n == 16 {
			continue
		}
		if _, err := InetCodec.Decode(scalarReader(t, TypeInet, Present, make([]byte, n))); err == nil {
			t.Fatalf("inet accepted %d bytes", n)
		}
	}
	for _, data := range [][]byte{nil, {0}, {0, 0}, {0, 0, 0, 0}, {1, 2, 0}, {0xff}, {0xf1, 0, 0, 0, 0, 0, 0}} {
		if _, err := DurationCodec.Decode(scalarReader(t, TypeDuration, Present, data)); err == nil {
			t.Fatalf("accepted malformed duration %x", data)
		}
	}
	durationMax := scalarHex(t, "f0fffffffef0fffffffefffffffffffffffffe")
	for n := 0; n < len(durationMax); n++ {
		if _, err := DurationCodec.Decode(scalarReader(t, TypeDuration, Present, durationMax[:n])); err == nil {
			t.Fatalf("duration accepted truncated prefix of %d bytes", n)
		}
	}
	if value, err := DurationCodec.Decode(scalarReader(t, TypeDuration, Present, durationMax)); err != nil || value != (Duration{Months: math.MaxInt32, Days: math.MaxInt32, Nanoseconds: math.MaxInt64}) {
		t.Fatalf("duration maximum failed: %v", err)
	}
	for _, v := range []Duration{{Months: 1, Days: -1}, {Days: 1, Nanoseconds: -1}, {Months: -1, Nanoseconds: 1}} {
		if _, err := scalarEncode(t, DurationCodec, TypeDuration, v); err == nil {
			t.Fatal("accepted mixed duration signs")
		}
	}
	for _, data := range [][]byte{nil, {0}, {0, 0, 0, 0}} {
		if _, err := DecimalCodec.Decode(scalarReader(t, TypeDecimal, Present, data)); err == nil {
			t.Fatal("accepted malformed decimal")
		}
	}
	if _, err := VarintFromBytes(nil); err == nil {
		t.Fatal("accepted empty varint input")
	}
	if _, err := VarintCodec.Decode(scalarReader(t, TypeVarint, Present)); err == nil {
		t.Fatal("decoded empty varint")
	}
	if _, err := Bool.Decode(newValueReader(scalarType(t, TypeBoolean), Null, BorrowBytes([]byte{1}).chain, nil)); err == nil {
		t.Fatal("accepted null with bytes")
	}
}

func FuzzV2ScalarUTF8(f *testing.F) {
	for _, seed := range []string{"", "hello", "é€😀", "\xed\xa0\x80", "\xf4\x90\x80\x80", "\xe2\x82"} {
		f.Add([]byte(seed), uint32(1))
	}
	f.Fuzz(func(t *testing.T, data []byte, splitPoint uint32) {
		split := int(uint64(splitPoint) % uint64(len(data)+1))
		decoded, err := TextCodec.Decode(scalarReader(t, TypeText, Present, data[:split], data[split:]))
		if (err == nil) != utf8.Valid(data) {
			t.Fatalf("UTF-8 validation disagreed at split %d: %x", split, data)
		}
		if err == nil && decoded.String() != string(data) {
			t.Fatal("valid text bytes changed")
		}
	})
}

func TestV2ScalarTextValidationAndBorrowing(t *testing.T) {
	for _, s := range []string{"", "plain ASCII", "é€😀\x00", "\u007f\u0080\u07ff\u0800\ud7ff\ue000\uffff\U00010000\U0010ffff"} {
		for split := 0; split <= len(s); split++ {
			value, err := TextCodec.Decode(scalarReader(t, TypeVarchar, Present, []byte(s[:split]), []byte(s[split:])))
			if err != nil || value.String() != s {
				t.Fatalf("valid UTF-8 split %d: %v", split, err)
			}
		}
		frame, err := scalarEncode(t, StringEncoder, TypeText, s)
		if err != nil || !bytes.Equal(frame[4:], []byte(s)) {
			t.Fatalf("text encode: %v", err)
		}
	}
	for _, data := range [][]byte{{0x80}, {0xc0, 0x80}, {0xc1, 0xbf}, {0xe0, 0x9f, 0xbf}, {0xed, 0xa0, 0x80}, {0xf0, 0x8f, 0xbf, 0xbf}, {0xf4, 0x90, 0x80, 0x80}, {0xf5, 0x80, 0x80, 0x80}, {0xe2, 0x82}, {0xc2, 0x41}} {
		for split := 0; split <= len(data); split++ {
			if _, err := TextCodec.Decode(scalarReader(t, TypeText, Present, data[:split], data[split:])); err == nil {
				t.Fatalf("accepted invalid UTF-8 %x split %d", data, split)
			}
		}
		if _, err := scalarEncode(t, StringEncoder, TypeText, string(data)); err == nil {
			t.Fatal("encoded invalid UTF-8")
		}
	}
	if _, err := scalarEncode(t, ASCIIEncoder, TypeAscii, "café"); err == nil {
		t.Fatal("encoded non-ASCII")
	}
	if _, err := ASCIICodec.Decode(scalarReader(t, TypeAscii, Present, []byte{0x7f}, []byte{0x80})); err == nil {
		t.Fatal("decoded non-ASCII")
	}
	if _, err := scalarEncode(t, ASCIICodec, TypeAscii, BorrowText("é")); err == nil {
		t.Fatal("borrowed Text bypassed ASCII validation")
	}
	if err := StringEncoder.CheckType(scalarType(t, TypeAscii)); err == nil {
		t.Fatal("StringEncoder accepted ASCII metadata")
	}
	source := []byte("hello")
	value, err := TextCodec.Decode(scalarReader(t, TypeText, Present, source[:2], source[2:]))
	if err != nil {
		t.Fatal(err)
	}
	clone := TextCodec.Clone(value)
	source[0] = 'j'
	if value.String() != "jello" || clone.String() != "hello" {
		t.Fatal("text borrow or clone ownership wrong")
	}
}

func TestV2ScalarBlobStatesAndWrappers(t *testing.T) {
	frame, err := scalarEncode(t, BytesEncoder, TypeBlob, []byte(nil))
	if err != nil || !bytes.Equal(frame, []byte{0, 0, 0, 0}) {
		t.Fatalf("nil blob should be present empty: %x %v", frame, err)
	}
	empty, err := BlobCodec.Decode(scalarReader(t, TypeBlob, Present))
	if err != nil || empty.Len() != 0 {
		t.Fatal("empty blob rejected")
	}
	nullable := NullableCodec(Int32)
	frame, err = scalarEncode(t, nullable, TypeInt, Nullable[int32]{})
	if err != nil || !bytes.Equal(frame, []byte{255, 255, 255, 255}) {
		t.Fatal("nullable did not encode null")
	}
	null, err := nullable.Decode(scalarReader(t, TypeInt, Null))
	if err != nil || null.Valid {
		t.Fatal("nullable null decoded incorrectly")
	}
	valid, err := nullable.Decode(scalarReader(t, TypeInt, Present, []byte{0, 0, 0, 0}))
	if err != nil || !valid.Valid {
		t.Fatal("nullable lost present zero")
	}
	if _, err := nullable.Decode(scalarReader(t, TypeInt, Unset)); err == nil {
		t.Fatal("nullable accepted unset")
	}
	if _, err := scalarEncode(t, nullable, TypeBlob, Nullable[int32]{}); err == nil {
		t.Fatal("null bypassed type checking")
	}
	if _, err := scalarEncode(t, BindEncoder(Int32), TypeBlob, BindValue[int32]{State: Unset}); err == nil {
		t.Fatal("unset bypassed type checking")
	}
	frame, err = scalarEncode(t, BindEncoder(Int32), TypeInt, BindValue[int32]{State: Unset})
	if err != nil || !bytes.Equal(frame, []byte{255, 255, 255, 254}) {
		t.Fatal("unset marker incorrect")
	}
	builder := wire.NewBuilder(16)
	writer := newValueWriter(builder, scalarType(t, TypeInt), false)
	if err := BindEncoder(Int32).Encode(writer, BindValue[int32]{State: Unset}); err == nil {
		t.Fatal("nested unset accepted")
	}
	abortValueWriter(writer)
	wrapped := EmptyCodec(Int32)
	frame, err = scalarEncode(t, wrapped, TypeInt, EmptyValueOf[int32]{Empty: true})
	if err != nil || len(frame) != 4 {
		t.Fatal("empty encoding incorrect")
	}
	for _, state := range []CellState{Present, Empty} {
		v, err := wrapped.Decode(scalarReader(t, TypeInt, state))
		if err != nil || !v.Empty {
			t.Fatal("empty decoding incorrect")
		}
	}
	for _, kind := range []Type{TypeText, TypeAscii, TypeVarchar, TypeBlob, TypeCounter, TypeDuration} {
		if emptySupported(scalarType(t, kind)) {
			t.Fatalf("unexpected empty support for %s", kind)
		}
	}
	if err := EmptyCodec(TextCodec).CheckType(scalarType(t, TypeText)); err == nil {
		t.Fatal("EmptyCodec accepted text")
	}
	type customID int32
	named := NamedScalar(Int32, func(v customID) int32 { return int32(v) }, func(v int32) customID { return customID(v) })
	scalarGolden(t, named, TypeInt, customID(-7), "fffffff9")
}

func TestV2ScalarBigNumbers(t *testing.T) {
	cases := []struct{ decimal, wire string }{{"0", "00"}, {"1", "01"}, {"127", "7f"}, {"128", "0080"}, {"255", "00ff"}, {"256", "0100"}, {"-1", "ff"}, {"-128", "80"}, {"-129", "ff7f"}, {"-256", "ff00"}, {"18446744073709551616", "010000000000000000"}, {"-18446744073709551617", "feffffffffffffffff"}}
	for _, test := range cases {
		value, ok := new(big.Int).SetString(test.decimal, 10)
		if !ok {
			t.Fatal("bad fixture")
		}
		v := VarintFromBigInt(value)
		frame, err := scalarEncode(t, VarintCodec, TypeVarint, v)
		want := scalarHex(t, test.wire)
		if err != nil || !bytes.Equal(frame[4:], want) {
			t.Fatalf("varint %s: %x %v", test.decimal, frame, err)
		}
		if v.BigInt().Cmp(value) != 0 {
			t.Fatalf("BigInt %s roundtrip failed", test.decimal)
		}
		for split := 0; split <= len(want); split++ {
			decoded, err := VarintCodec.Decode(scalarReader(t, TypeVarint, Present, want[:split], want[split:]))
			if err != nil || decoded.BigInt().Cmp(value) != 0 {
				t.Fatalf("varint %s split %d: %v", test.decimal, split, err)
			}
		}
	}
	for _, test := range []struct{ in, out string }{{"0000007f", "7f"}, {"ffffff80", "80"}, {"00000080", "0080"}, {"ffffff7f", "ff7f"}} {
		data := scalarHex(t, test.in)
		v, err := VarintCodec.Decode(scalarReader(t, TypeVarint, Present, data[:1], data[1:2], data[2:]))
		if err != nil {
			t.Fatal(err)
		}
		frame, err := scalarEncode(t, VarintCodec, TypeVarint, v)
		if err != nil || hex.EncodeToString(frame[4:]) != test.out {
			t.Fatalf("canonicalization: %x %v", frame, err)
		}
	}
	decimal := Decimal{Scale: -2, Unscaled: VarintFromBigInt(big.NewInt(-129))}
	frame, err := scalarEncode(t, DecimalCodec, TypeDecimal, decimal)
	if err != nil || hex.EncodeToString(frame[4:]) != "fffffffeff7f" {
		t.Fatal("decimal signed scale incorrect")
	}
	decoded, err := DecimalCodec.Decode(scalarReader(t, TypeDecimal, Present, frame[4:6], frame[6:9], frame[9:]))
	if err != nil || decoded.Scale != -2 || decoded.Unscaled.BigInt().Int64() != -129 {
		t.Fatal("decimal decode failed")
	}
	data := []byte{0x01, 0x02}
	borrowed, err := VarintFromBytes(data)
	if err != nil {
		t.Fatal(err)
	}
	clone := VarintCodec.Clone(borrowed)
	data[1] = 3
	if borrowed.BigInt().Int64() != 259 || clone.BigInt().Int64() != 258 {
		t.Fatal("varint borrow/clone wrong")
	}
	huge := new(big.Int).Lsh(big.NewInt(1), 20000)
	huge.Neg(huge)
	if VarintFromBigInt(huge).BigInt().Cmp(huge) != 0 {
		t.Fatal("large varint lost precision")
	}
}

func TestV2ScalarCalendarBoundaries(t *testing.T) {
	for _, d := range []Date{math.MinInt32, -1, 0, 1, math.MaxInt32} {
		calendar, err := d.Time()
		if err != nil {
			t.Fatal(err)
		}
		actual, err := DateFromTime(calendar)
		if err != nil || actual != d {
			t.Fatalf("date %d roundtrip: %d %v", d, actual, err)
		}
	}
	local := time.Date(1970, 1, 1, 0, 30, 0, 0, time.FixedZone("east", 3600))
	d, err := DateFromTime(local)
	if err != nil || d != 0 {
		t.Fatal("DateFromTime lost calendar location")
	}
	if _, err := DateFromTime(time.Date(6_000_000, 1, 1, 0, 0, 0, 0, time.UTC)); err == nil {
		t.Fatal("accepted out-of-range calendar date")
	}
	for _, ms := range []Timestamp{math.MinInt64, -1001, -1000, -1, 0, 1, math.MaxInt64} {
		calendar, err := ms.Time()
		if err != nil {
			t.Fatal(err)
		}
		actual, err := TimestampFromTime(calendar)
		if err != nil || actual != ms {
			t.Fatalf("timestamp %d roundtrip: %d %v", ms, actual, err)
		}
	}
	for _, test := range []struct {
		time time.Time
		want Timestamp
	}{{time.Unix(-1, 999_999_999), -1}, {time.Unix(-1, 1), -1000}, {time.Unix(0, 999_999), 0}} {
		actual, err := TimestampFromTime(test.time)
		if err != nil || actual != test.want {
			t.Fatalf("timestamp floor: %d %v", actual, err)
		}
	}
	minTime, _ := Timestamp(math.MinInt64).Time()
	maxTime, _ := Timestamp(math.MaxInt64).Time()
	for _, calendar := range []time.Time{minTime.Add(-time.Millisecond), maxTime.Add(time.Millisecond)} {
		if _, err := TimestampFromTime(calendar); err == nil {
			t.Fatal("timestamp overflow accepted")
		}
	}
}

func TestV2ScalarFloatBits(t *testing.T) {
	for _, bits := range []uint32{0, 1 << 31, 0x7f800000, 0xff800000, 0x7fc00042} {
		value := math.Float32frombits(bits)
		frame, err := scalarEncode(t, Float32, TypeFloat, value)
		if err != nil {
			t.Fatal(err)
		}
		actual, err := Float32.Decode(scalarReader(t, TypeFloat, Present, frame[4:5], frame[5:]))
		if err != nil || math.Float32bits(actual) != bits {
			t.Fatalf("float bits %x changed", bits)
		}
	}
	for _, bits := range []uint64{0, 1 << 63, 0x7ff0000000000000, 0xfff0000000000000, 0x7ff8000000000042} {
		value := math.Float64frombits(bits)
		frame, err := scalarEncode(t, Float64, TypeDouble, value)
		if err != nil {
			t.Fatal(err)
		}
		actual, err := Float64.Decode(scalarReader(t, TypeDouble, Present, frame[4:7], frame[7:]))
		if err != nil || math.Float64bits(actual) != bits {
			t.Fatalf("float bits %x changed", bits)
		}
	}
}

func TestV2ScalarPublicValueOwnership(t *testing.T) {
	typ := scalarType(t, TypeBlob)
	source := []byte{1, 2, 3}
	borrowed, err := EncodeValueBorrowed(typ, BytesEncoder, source)
	if err != nil {
		t.Fatal(err)
	}
	owned, err := EncodeValueOwned(typ, BytesEncoder, source)
	if err != nil {
		t.Fatal(err)
	}
	source[0] = 4
	borrowedBytes, err := DecodeValue(BlobCodec, borrowed)
	if err != nil {
		t.Fatal(err)
	}
	ownedBytes, err := DecodeValue(BlobCodec, owned)
	if err != nil {
		t.Fatal(err)
	}
	if b, _ := borrowedBytes.At(0); b != 4 {
		t.Fatal("borrowed encoding copied input")
	}
	if b, _ := ownedBytes.At(0); b != 1 {
		t.Fatal("owned encoding retained input alias")
	}
	empty, err := EncodeValueBorrowed(scalarType(t, TypeInt), EmptyCodec(Int32), EmptyValueOf[int32]{Empty: true})
	if err != nil || empty.State() != Empty || empty.Bytes().Len() != 0 {
		t.Fatalf("explicit Empty state lost: %v", err)
	}
	decoded, err := DecodeValue(EmptyCodec(Int32), empty)
	if err != nil || !decoded.Empty {
		t.Fatal("explicit Empty did not decode")
	}
	if _, err := DecodeValue(Int32, empty); err == nil {
		t.Fatal("nonempty decoder accepted Empty")
	}
	if _, err := NewEncodedValue(typ, Empty, Bytes{}); err == nil {
		t.Fatal("blob admitted a distinct Empty state")
	}
	if _, err := NewEncodedValue(scalarType(t, TypeInt), Null, BorrowBytes([]byte{1})); err == nil {
		t.Fatal("null admitted payload bytes")
	}
}

func TestV2ScalarLeaseAndLargeBorrowing(t *testing.T) {
	data := []byte("borrowed")
	chain, err := wire.NewChain(wire.Borrow(data[:3]), wire.Borrow(data[3:]))
	if err != nil {
		t.Fatal(err)
	}
	lease := wire.NewLease(chain, nil)
	value, err := TextCodec.Decode(newValueReader(scalarType(t, TypeText), Present, chain, lease))
	if err != nil {
		t.Fatal(err)
	}
	clone := TextCodec.Clone(value)
	lease.Release()
	if _, err := value.Bytes().At(0); !errors.Is(err, ErrReleased) {
		t.Fatal("text lost page lease")
	}
	if clone.String() != "borrowed" {
		t.Fatal("clone did not detach")
	}
	if _, err := TextCodec.Decode(newValueReader(scalarType(t, TypeText), Present, chain, lease)); !errors.Is(err, ErrReleased) {
		t.Fatal("decode accepted released lease")
	}
	builder := wire.NewBuilder(16)
	large := []byte(strings.Repeat("a", 1<<20))
	w := newValueWriter(builder, scalarType(t, TypeBlob), true)
	if err := BytesEncoder.Encode(w, large); err != nil {
		t.Fatal(err)
	}
	if err := finishValueWriter(w); err != nil {
		t.Fatal(err)
	}
	out, err := builder.Freeze()
	if err != nil {
		t.Fatal(err)
	}
	var borrowed bool
	if err := out.Each(func(s wire.Span) error {
		if s.Len() == len(large) {
			borrowed = &s.Bytes()[0] == &large[0]
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if !borrowed {
		t.Fatal("blob encoding copied payload")
	}
}
