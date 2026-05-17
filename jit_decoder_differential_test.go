//go:build unit
// +build unit

package gocql

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"net"
	"reflect"
	"testing"
	"time"
)

// The JIT decoder exists only to be a faster spelling of Unmarshal, so every
// fast path it installs must agree with the generic path it bypasses — on the
// decoded value, on whether an input is rejected, and on the CQL-NULL (nil)
// versus empty-value (non-nil, zero-length) distinction the wire format draws.
// TestJITDecoderMatchesGenericUnmarshal enumerates every (CQL type, Go type)
// pair compileColumnDecoder special-cases and cross-checks the two.

func nat(t Type) NativeType { return NativeType{typ: t, proto: 4} }

func be32(v uint32) []byte { b := make([]byte, 4); binary.BigEndian.PutUint32(b, v); return b }
func be64(v uint64) []byte { b := make([]byte, 8); binary.BigEndian.PutUint64(b, v); return b }

// destFactory builds a fresh, non-nil destination pointer.
type destFactory struct {
	name string
	new  func() any
}

var (
	dstString  = destFactory{"*string", func() any { return new(string) }}
	dstBytes   = destFactory{"*[]byte", func() any { return new([]byte) }}
	dstInt     = destFactory{"*int", func() any { return new(int) }}
	dstInt8    = destFactory{"*int8", func() any { return new(int8) }}
	dstInt16   = destFactory{"*int16", func() any { return new(int16) }}
	dstInt32   = destFactory{"*int32", func() any { return new(int32) }}
	dstInt64   = destFactory{"*int64", func() any { return new(int64) }}
	dstFloat32 = destFactory{"*float32", func() any { return new(float32) }}
	dstFloat64 = destFactory{"*float64", func() any { return new(float64) }}
	dstBool    = destFactory{"*bool", func() any { return new(bool) }}
	dstTime    = destFactory{"*time.Time", func() any { return new(time.Time) }}
	dstUUID    = destFactory{"*UUID", func() any { return new(UUID) }}
	dstIP      = destFactory{"*net.IP", func() any { return new(net.IP) }}
)

// jitDecoderCases enumerates the fast paths compileColumnDecoder installs,
// with wire inputs covering NULL, empty, valid, and malformed values.
func jitDecoderCases() []struct {
	cql    Type
	dests  []destFactory
	inputs [][]byte
} {
	text := [][]byte{nil, {}, []byte("hello world"), {0x41, 0xC3, 0xA9}}
	return []struct {
		cql    Type
		dests  []destFactory
		inputs [][]byte
	}{
		{TypeVarchar, []destFactory{dstString, dstBytes}, text},
		{TypeText, []destFactory{dstString, dstBytes}, text},
		{TypeAscii, []destFactory{dstString, dstBytes}, text},
		{TypeBlob, []destFactory{dstBytes, dstString}, text},

		{TypeInt, []destFactory{dstInt, dstInt32, dstInt64},
			[][]byte{nil, {}, be32(42), be32(math.MaxUint32), {1, 2, 3}}},
		{TypeBigInt, []destFactory{dstInt64, dstInt},
			[][]byte{nil, {}, be64(123456789), be64(math.MaxUint64), {1, 2, 3}}},
		{TypeCounter, []destFactory{dstInt64, dstInt},
			[][]byte{nil, {}, be64(7), {1, 2, 3}}},
		{TypeSmallInt, []destFactory{dstInt16, dstInt},
			[][]byte{nil, {}, {0x01, 0x02}, {0xFF, 0xFE}, {1, 2, 3}}},
		{TypeTinyInt, []destFactory{dstInt8},
			[][]byte{nil, {}, {0x7F}, {0xFE}, {1, 2}}},

		{TypeBoolean, []destFactory{dstBool}, [][]byte{nil, {}, {0}, {1}, {2}, {0, 0}}},
		{TypeFloat, []destFactory{dstFloat32},
			[][]byte{nil, {}, be32(math.Float32bits(3.14)), {1, 2, 3}}},
		{TypeDouble, []destFactory{dstFloat64},
			[][]byte{nil, {}, be64(math.Float64bits(3.14)), {1, 2, 3}}},

		{TypeTimestamp, []destFactory{dstTime, dstInt64},
			[][]byte{nil, {}, be64(uint64(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC).UnixMilli())), {1, 2, 3}}},

		{TypeUUID, []destFactory{dstUUID, dstString},
			[][]byte{nil, {}, bytes.Repeat([]byte{7}, 16), {1, 2, 3}}},
		{TypeTimeUUID, []destFactory{dstUUID, dstString},
			[][]byte{nil, {}, bytes.Repeat([]byte{7}, 16), {1, 2, 3}}},

		{TypeInet, []destFactory{dstIP, dstString},
			[][]byte{nil, {}, {192, 168, 0, 1}, bytes.Repeat([]byte{1}, 16), {1, 2, 3}}},
	}
}

func TestJITDecoderMatchesGenericUnmarshal(t *testing.T) {
	for _, tc := range jitDecoderCases() {
		for _, df := range tc.dests {
			for _, in := range tc.inputs {
				name := fmt.Sprintf("%v/%s/len%d", tc.cql, df.name, len(in))
				if in == nil {
					name = fmt.Sprintf("%v/%s/null", tc.cql, df.name)
				}
				t.Run(name, func(t *testing.T) {
					info := nat(tc.cql)

					jitDest := df.new()
					dec := compileColumnDecoder(info, reflect.TypeOf(jitDest))
					jitErr := dec(in, jitDest)

					genDest := df.new()
					genErr := Unmarshal(info, in, genDest)

					if (jitErr != nil) != (genErr != nil) {
						t.Fatalf("error mismatch: jit=%v generic=%v", jitErr, genErr)
					}
					if jitErr != nil {
						return // both rejected; error text may legitimately differ
					}
					got := reflect.ValueOf(jitDest).Elem().Interface()
					want := reflect.ValueOf(genDest).Elem().Interface()
					if !reflect.DeepEqual(got, want) {
						t.Fatalf("value mismatch: jit=%#v generic=%#v", got, want)
					}
				})
			}
		}
	}
}

// TestJITDecoderRejectsTypedNilDestination checks a typed-nil destination
// pointer, which passes the decoders' type assertion but cannot be written
// through. The generic path reports it; the fast path must not panic.
func TestJITDecoderRejectsTypedNilDestination(t *testing.T) {
	typedNils := []struct {
		cql  Type
		dest any
		data []byte
	}{
		{TypeInt, (*int32)(nil), be32(42)},
		{TypeInt, (*int)(nil), be32(42)},
		{TypeInt, (*int64)(nil), be32(42)},
		{TypeBigInt, (*int64)(nil), be64(7)},
		{TypeBigInt, (*int)(nil), be64(7)},
		{TypeSmallInt, (*int16)(nil), []byte{0, 1}},
		{TypeSmallInt, (*int)(nil), []byte{0, 1}},
		{TypeTinyInt, (*int8)(nil), []byte{1}},
		{TypeVarchar, (*string)(nil), []byte("x")},
		{TypeVarchar, (*[]byte)(nil), []byte("x")},
		{TypeBlob, (*[]byte)(nil), []byte("x")},
		{TypeBlob, (*string)(nil), []byte("x")},
		{TypeAscii, (*string)(nil), []byte("x")},
		{TypeBoolean, (*bool)(nil), []byte{1}},
		{TypeFloat, (*float32)(nil), be32(1)},
		{TypeDouble, (*float64)(nil), be64(1)},
		{TypeTimestamp, (*time.Time)(nil), be64(1)},
		{TypeTimestamp, (*int64)(nil), be64(1)},
		{TypeUUID, (*UUID)(nil), bytes.Repeat([]byte{7}, 16)},
		{TypeUUID, (*string)(nil), bytes.Repeat([]byte{7}, 16)},
		{TypeInet, (*net.IP)(nil), []byte{192, 168, 0, 1}},
		{TypeInet, (*string)(nil), []byte{192, 168, 0, 1}},
	}

	for _, tc := range typedNils {
		t.Run(fmt.Sprintf("%v/%T", tc.cql, tc.dest), func(t *testing.T) {
			info := nat(tc.cql)
			dec := compileColumnDecoder(info, reflect.TypeOf(tc.dest))

			var jitErr error
			func() {
				defer func() {
					if r := recover(); r != nil {
						t.Fatalf("fast path panicked on a typed-nil destination: %v", r)
					}
				}()
				jitErr = dec(tc.data, tc.dest)
			}()

			genErr := Unmarshal(info, tc.data, tc.dest)
			if (jitErr != nil) != (genErr != nil) {
				t.Fatalf("error mismatch: jit=%v generic=%v", jitErr, genErr)
			}
		})
	}
}

// TestScanSingleElementTupleColumn covers a tuple column that expands to
// exactly one destination: actualColCount then equals len(columns), so the
// JIT gate admits a row the compiled decoders cannot handle — tuple values
// need the [bytes]-envelope expansion scanColumn performs.
func TestScanSingleElementTupleColumn(t *testing.T) {
	tupleInfo := TupleTypeInfo{
		NativeType: nat(TypeTuple),
		Elems:      []TypeInfo{nat(TypeInt)},
	}
	columns := []ColumnInfo{{Name: "t", TypeInfo: tupleInfo}}
	data, err := Marshal(tupleInfo, []any{99})
	if err != nil {
		t.Fatalf("unexpected error from reference Marshal: %v", err)
	}

	meta := resultMetadata{columns: columns, actualColCount: 1}
	iter := &Iter{
		framer:  &benchFramerT{cols: [][]byte{data}},
		meta:    meta,
		numRows: 1,
	}

	var got int
	if !iter.Scan(&got) {
		t.Fatalf("Scan failed: %v", iter.err)
	}
	if got != 99 {
		t.Fatalf("got %d, want 99", got)
	}
}

type benchFramerT struct {
	cols [][]byte
	pos  int
}

func (f *benchFramerT) ReadBytesInternal() ([]byte, error) {
	if f.pos >= len(f.cols) {
		f.pos = 0
	}
	d := f.cols[f.pos]
	f.pos++
	return d, nil
}
func (f *benchFramerT) GetCustomPayload() map[string][]byte { return nil }
func (f *benchFramerT) GetHeaderWarnings() []string         { return nil }
func (f *benchFramerT) Release()                            {}

// TestCompiledRowDecoderUsableForPlainColumns guards the tuple fix from
// over-reaching: an ordinary row must still take the fast path.
func TestCompiledRowDecoderUsableForPlainColumns(t *testing.T) {
	columns := []ColumnInfo{
		{Name: "id", TypeInfo: nat(TypeInt)},
		{Name: "name", TypeInfo: nat(TypeVarchar)},
		{Name: "tags", TypeInfo: CollectionType{
			NativeType: nat(TypeSet), Elem: nat(TypeVarchar),
		}},
	}
	var id int32
	var name string
	var tags []string
	dec := getOrCompileRowDecoder(columns, []any{&id, &name, &tags})
	if !dec.usable {
		t.Fatal("expected the fast path to remain available for non-tuple columns")
	}
}

// TestCompiledCachesAreBounded checks the process-wide caches stop growing at
// maxCompiledCacheEntries, so a workload with unbounded distinct shapes cannot
// retain compiled closures for the process lifetime.
func TestCompiledCachesAreBounded(t *testing.T) {
	// These caches are process-wide; leave them as they were found.
	t.Cleanup(func() {
		decoderCache.Range(func(k, _ any) bool { decoderCache.Delete(k); return true })
		decoderCacheCount.Store(0)
	})
	decoderCache.Range(func(k, _ any) bool { decoderCache.Delete(k); return true })
	decoderCacheCount.Store(0)

	var dst int64
	dest := []any{&dst}
	for i := 0; i < maxCompiledCacheEntries+100; i++ {
		// Distinct CQL type codes give distinct cache keys; unknown codes
		// simply compile to the generic fallback.
		columns := []ColumnInfo{{Name: "c", TypeInfo: NativeType{typ: Type(i), proto: 4}}}
		if dec := getOrCompileRowDecoder(columns, dest); dec == nil {
			t.Fatal("expected a decoder even once the cache is full")
		}
	}

	if got := decoderCacheCount.Load(); got > maxCompiledCacheEntries {
		t.Fatalf("cache grew to %d entries, past the %d bound", got, maxCompiledCacheEntries)
	}
	var live int
	decoderCache.Range(func(_, _ any) bool { live++; return true })
	if live > maxCompiledCacheEntries {
		t.Fatalf("cache holds %d entries, past the %d bound", live, maxCompiledCacheEntries)
	}
}
