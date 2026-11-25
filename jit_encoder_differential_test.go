//go:build unit
// +build unit

package gocql

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"net"
	"reflect"
	"testing"
	"time"
)

// Every JIT fast path must agree with Marshal on bytes, NULL vs empty, and
// rejected inputs; marshalQueryValuesJIT relies on the latter to skip re-marshalling.
func TestJITEncoderMatchesGenericMarshal(t *testing.T) {
	str, empty := "hello", ""
	bs, emptyBs := []byte("hello"), []byte{}
	i32, i, i64, i16, i8 := int32(42), 42, int64(42), int16(42), int8(42)
	f32, f64 := float32(3.14), 3.14
	tr := true
	ts := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	u := UUID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
	ip := net.IPv4(192, 168, 0, 1)

	cases := []struct {
		cql    Type
		values []any
	}{
		{TypeVarchar, []any{str, empty, bs, emptyBs, []byte(nil), &str, &empty, &bs, (*string)(nil), (*[]byte)(nil), "héllo"}},
		{TypeText, []any{str, empty, bs, emptyBs, &str, (*string)(nil), "héllo"}},
		{TypeAscii, []any{str, empty, bs, "héllo", []byte{0x41, 0xC3, 0xA9}, &str, (*string)(nil)}},
		{TypeBlob, []any{bs, emptyBs, []byte(nil), str, empty, &bs, (*[]byte)(nil)}},

		{TypeInt, []any{
			i32, i, i64, i16, i8,
			int32(math.MinInt32), int32(math.MaxInt32),
			int64(math.MaxInt32) + 1, int64(math.MinInt32) - 1, // out of range
			math.MaxInt32 + 1, // int, out of range
			&i32, &i, &i64, (*int32)(nil), (*int)(nil), (*int64)(nil),
		}},
		{TypeBigInt, []any{i64, i, int64(math.MinInt64), int64(math.MaxInt64), &i64, &i, (*int64)(nil)}},
		{TypeCounter, []any{i64, i, &i64, (*int64)(nil)}},
		{TypeSmallInt, []any{
			i16, i, int16(math.MinInt16), int16(math.MaxInt16),
			math.MaxInt16 + 1, math.MinInt16 - 1, // int, out of range
			&i16, (*int16)(nil),
		}},
		{TypeTinyInt, []any{i8, int8(math.MinInt8), int8(math.MaxInt8), &i8, (*int8)(nil)}},

		{TypeBoolean, []any{true, false, &tr, (*bool)(nil)}},
		{TypeFloat, []any{f32, float32(0), float32(math.Inf(1)), &f32, (*float32)(nil)}},
		{TypeDouble, []any{f64, float64(0), math.Inf(-1), &f64, (*float64)(nil)}},

		{TypeTimestamp, []any{
			ts, time.Time{}, i64,
			time.Date(300000, 1, 1, 0, 0, 0, 0, time.UTC), // beyond CQL range
			&ts, (*time.Time)(nil),
		}},

		{TypeUUID, []any{u, UUID{}, &u, (*UUID)(nil)}},
		{TypeTimeUUID, []any{u, &u, (*UUID)(nil)}},

		{TypeInet, []any{
			ip, net.IP(nil), net.IP{}, net.IPv6loopback,
			"192.168.0.1", "::1", "", "not-an-ip",
			&ip, (*net.IP)(nil),
		}},
	}

	for _, tc := range cases {
		for vi, v := range tc.values {
			t.Run(fmt.Sprintf("%v/%d_%T", tc.cql, vi, v), func(t *testing.T) {
				info := nat(tc.cql)

				enc := compileColumnEncoder(info, reflect.TypeOf(v))
				jitOut, jitErr := enc(v)
				genOut, genErr := Marshal(info, v)

				if (jitErr != nil) != (genErr != nil) {
					t.Fatalf("error mismatch for %#v: jit=%v generic=%v", v, jitErr, genErr)
				}
				if jitErr != nil {
					if jitErr.Error() != genErr.Error() {
						t.Fatalf("error text mismatch for %#v: jit=%v generic=%v", v, jitErr, genErr)
					}
					return
				}
				if !bytes.Equal(jitOut, genOut) {
					t.Fatalf("bytes mismatch for %#v: jit=%v generic=%v", v, jitOut, genOut)
				}
				// nil encodes to CQL NULL, a zero-length non-nil slice to an
				// empty value; the framer writes them differently.
				if (jitOut == nil) != (genOut == nil) {
					t.Fatalf("nil-vs-empty mismatch for %#v: jit=%#v generic=%#v", v, jitOut, genOut)
				}
			})
		}
	}
}

// TestMarshalQueryValuesJITDeclinesMismatchedLengths: a tuple bind marker can
// expand into more values than columns, so the fast path must decline.
func TestMarshalQueryValuesJITDeclinesMismatchedLengths(t *testing.T) {
	columns := []ColumnInfo{{Name: "a", TypeInfo: nat(TypeInt)}}
	values := []any{int32(1), int32(2)} // more values than columns
	dst := make([]queryValues, len(values))

	handled, err := marshalQueryValuesJIT(stmtFor(columns), values, dst)
	if handled || err != nil {
		t.Fatalf("expected the fast path to decline, got handled=%v err=%v", handled, err)
	}
}

// TestMarshalQueryValuesJITSurfacesEncodeError checks that an encode failure is
// reported rather than swallowed into a silent fallback that marshals
// everything a second time.
func TestMarshalQueryValuesJITSurfacesEncodeError(t *testing.T) {
	columns := []ColumnInfo{{Name: "a", TypeInfo: nat(TypeInt)}}
	values := []any{int64(math.MaxInt32) + 1} // out of range for a CQL int
	dst := make([]queryValues, len(values))

	handled, err := marshalQueryValuesJIT(stmtFor(columns), values, dst)
	if !handled {
		t.Fatal("expected the fast path to handle these values")
	}
	if err == nil {
		t.Fatal("expected an out-of-range encode error to be surfaced")
	}
	if _, genErr := Marshal(columns[0].TypeInfo, values[0]); genErr == nil {
		t.Fatal("generic Marshal accepted a value the fast path rejected")
	}
}

// TestMarshalQueryValuesJITRecoversTornString checks the fast path keeps
// Marshal's #1160 guarantee: a torn string fails the query instead of panicking.
func TestMarshalQueryValuesJITRecoversTornString(t *testing.T) {
	columns := []ColumnInfo{{Name: "a", TypeInfo: nat(TypeVarchar)}}
	values := []any{tornString(5)}
	dst := make([]queryValues, len(values))

	handled, err := marshalQueryValuesJIT(stmtFor(columns), values, dst)
	if !handled || !errors.As(err, &MarshalError{}) {
		t.Fatalf("expected a handled MarshalError, got handled=%v err=%v", handled, err)
	}
}

// TestJITEncoderNilAgainstComplexColumns pins untyped-nil handling per column
// category: NULL for native/collection, rejected for UDT/custom, never a silent NULL.
func TestJITEncoderNilAgainstComplexColumns(t *testing.T) {
	intType := nat(TypeInt)
	cases := []struct {
		name      string
		info      TypeInfo
		wantError bool
	}{
		{"int", intType, false},
		{"varchar", nat(TypeVarchar), false},
		{"list", CollectionType{NativeType: nat(TypeList), Elem: intType}, false},
		{"set", CollectionType{NativeType: nat(TypeSet), Elem: intType}, false},
		{"map", CollectionType{NativeType: nat(TypeMap), Key: intType, Elem: intType}, false},
		{"udt", UDTTypeInfo{
			NativeType: nat(TypeUDT), KeySpace: "ks", Name: "u",
			Elements: []UDTField{{Name: "a", Type: intType}},
		}, true},
		{"custom", NativeType{typ: TypeCustom, proto: 4, custom: "org.apache.cassandra.db.marshal.Foo"}, true},
		{"tuple", TupleTypeInfo{NativeType: nat(TypeTuple), Elems: []TypeInfo{intType}}, true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			out, err := compileColumnEncoder(tc.info, reflect.TypeOf(nil))(nil)
			if tc.wantError {
				if err == nil {
					t.Fatalf("expected nil to be rejected, got %v", out)
				}
				if _, genErr := Marshal(tc.info, nil); genErr == nil {
					t.Fatal("generic Marshal disagrees: expected an error")
				}
				return
			}
			if err != nil {
				t.Fatalf("expected nil to encode as NULL, got error %v", err)
			}
			if out != nil {
				t.Fatalf("expected a nil slice (CQL NULL), got %#v", out)
			}
			// Cross-check against Marshal for the categories it accepts.
			genOut, genErr := Marshal(tc.info, nil)
			if genErr != nil || genOut != nil {
				t.Fatalf("generic Marshal disagrees: out=%#v err=%v", genOut, genErr)
			}
		})
	}
}

// stmtFor builds a prepared statement whose request metadata carries columns,
// which is where the encoder cache reads them from.
func stmtFor(columns []ColumnInfo) *preparedStatment {
	return &preparedStatment{
		request: preparedMetadata{resultMetadata: resultMetadata{columns: columns}},
	}
}
