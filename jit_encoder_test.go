//go:build unit
// +build unit

package gocql

import (
	"bytes"
	"math"
	"net"
	"reflect"
	"testing"
	"time"
)

// BenchmarkMarshalJIT benchmarks the JIT encoder against the generic Marshal path.
func BenchmarkMarshalJIT(b *testing.B) {
	columns := []ColumnInfo{
		{Name: "id", TypeInfo: NativeType{typ: TypeInt, proto: 4}},
		{Name: "counter", TypeInfo: NativeType{typ: TypeBigInt, proto: 4}},
		{Name: "name", TypeInfo: NativeType{typ: TypeVarchar, proto: 4}},
		{Name: "active", TypeInfo: NativeType{typ: TypeBoolean, proto: 4}},
		{Name: "created", TypeInfo: NativeType{typ: TypeTimestamp, proto: 4}},
		{Name: "uuid", TypeInfo: NativeType{typ: TypeUUID, proto: 4}},
	}

	values := []any{
		int32(42),
		int64(123456789),
		"hello world",
		true,
		time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
		UUID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
	}

	b.Run("JIT_Encode", func(b *testing.B) {
		b.ReportAllocs()
		enc := getOrCompileParamEncoder(columns, values)
		for i := 0; i < b.N; i++ {
			for j, v := range values {
				_, err := enc.encoders[j](v)
				if err != nil {
					b.Fatal(err)
				}
			}
		}
	})

	b.Run("GenericMarshal", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			for j, v := range values {
				_, err := Marshal(columns[j].TypeInfo, v)
				if err != nil {
					b.Fatal(err)
				}
			}
		}
	})

	// Pointer types benchmark
	id := int32(42)
	counter := int64(123456789)
	name := "hello world"
	active := true
	created := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	uid := UUID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}

	ptrValues := []any{&id, &counter, &name, &active, &created, &uid}

	b.Run("JIT_Encode_Ptr", func(b *testing.B) {
		b.ReportAllocs()
		enc := getOrCompileParamEncoder(columns, ptrValues)
		for i := 0; i < b.N; i++ {
			for j, v := range ptrValues {
				_, err := enc.encoders[j](v)
				if err != nil {
					b.Fatal(err)
				}
			}
		}
	})

	b.Run("GenericMarshal_Ptr", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			for j, v := range ptrValues {
				_, err := Marshal(columns[j].TypeInfo, v)
				if err != nil {
					b.Fatal(err)
				}
			}
		}
	})
}

// TestJITEncoderCorrectness verifies JIT encoder matches generic Marshal output.
func TestJITEncoderCorrectness(t *testing.T) {
	columns := []ColumnInfo{
		{Name: "id", TypeInfo: NativeType{typ: TypeInt, proto: 4}},
		{Name: "counter", TypeInfo: NativeType{typ: TypeBigInt, proto: 4}},
		{Name: "name", TypeInfo: NativeType{typ: TypeVarchar, proto: 4}},
		{Name: "active", TypeInfo: NativeType{typ: TypeBoolean, proto: 4}},
		{Name: "created", TypeInfo: NativeType{typ: TypeTimestamp, proto: 4}},
		{Name: "uuid", TypeInfo: NativeType{typ: TypeUUID, proto: 4}},
	}

	values := []any{
		int32(42),
		int64(123456789),
		"hello world",
		true,
		time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
		UUID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
	}

	enc := getOrCompileParamEncoder(columns, values)

	for i, v := range values {
		jitBytes, jitErr := enc.encoders[i](v)
		marshalBytes, marshalErr := Marshal(columns[i].TypeInfo, v)

		if (jitErr != nil) != (marshalErr != nil) {
			t.Fatalf("column %d (%s): error mismatch: jit=%v, marshal=%v", i, columns[i].Name, jitErr, marshalErr)
		}
		if !bytes.Equal(jitBytes, marshalBytes) {
			t.Fatalf("column %d (%s): bytes mismatch: jit=%v, marshal=%v", i, columns[i].Name, jitBytes, marshalBytes)
		}
	}
}

// TestJITEncoderPointerTypes verifies pointer-type encoding matches Marshal.
func TestJITEncoderPointerTypes(t *testing.T) {
	columns := []ColumnInfo{
		{Name: "id", TypeInfo: NativeType{typ: TypeInt, proto: 4}},
		{Name: "counter", TypeInfo: NativeType{typ: TypeBigInt, proto: 4}},
		{Name: "name", TypeInfo: NativeType{typ: TypeVarchar, proto: 4}},
		{Name: "active", TypeInfo: NativeType{typ: TypeBoolean, proto: 4}},
		{Name: "created", TypeInfo: NativeType{typ: TypeTimestamp, proto: 4}},
		{Name: "uuid", TypeInfo: NativeType{typ: TypeUUID, proto: 4}},
	}

	id := int32(42)
	counter := int64(123456789)
	name := "hello world"
	active := true
	created := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	uid := UUID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}

	values := []any{&id, &counter, &name, &active, &created, &uid}

	enc := getOrCompileParamEncoder(columns, values)

	for i, v := range values {
		jitBytes, jitErr := enc.encoders[i](v)
		marshalBytes, marshalErr := Marshal(columns[i].TypeInfo, v)

		if (jitErr != nil) != (marshalErr != nil) {
			t.Fatalf("column %d (%s): error mismatch: jit=%v, marshal=%v", i, columns[i].Name, jitErr, marshalErr)
		}
		if !bytes.Equal(jitBytes, marshalBytes) {
			t.Fatalf("column %d (%s): bytes mismatch: jit=%v, marshal=%v", i, columns[i].Name, jitBytes, marshalBytes)
		}
	}
}

// TestJITEncoderNilPointers verifies nil pointer encoding.
func TestJITEncoderNilPointers(t *testing.T) {
	columns := []ColumnInfo{
		{Name: "id", TypeInfo: NativeType{typ: TypeInt, proto: 4}},
		{Name: "name", TypeInfo: NativeType{typ: TypeVarchar, proto: 4}},
		{Name: "counter", TypeInfo: NativeType{typ: TypeBigInt, proto: 4}},
	}

	values := []any{(*int32)(nil), (*string)(nil), (*int64)(nil)}

	enc := getOrCompileParamEncoder(columns, values)

	for i, v := range values {
		jitBytes, jitErr := enc.encoders[i](v)
		marshalBytes, marshalErr := Marshal(columns[i].TypeInfo, v)

		if (jitErr != nil) != (marshalErr != nil) {
			t.Fatalf("column %d: error mismatch: jit=%v, marshal=%v", i, jitErr, marshalErr)
		}
		if !bytes.Equal(jitBytes, marshalBytes) {
			t.Fatalf("column %d: bytes mismatch: jit=%v, marshal=%v", i, jitBytes, marshalBytes)
		}
		if jitBytes != nil {
			t.Fatalf("column %d: expected nil for nil pointer, got %v", i, jitBytes)
		}
	}
}

// TestJITEncoderNilValue verifies nil any value.
func TestJITEncoderNilValue(t *testing.T) {
	columns := []ColumnInfo{
		{Name: "id", TypeInfo: NativeType{typ: TypeInt, proto: 4}},
	}
	values := []any{nil}

	enc := getOrCompileParamEncoder(columns, values)
	result, err := enc.encoders[0](nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result != nil {
		t.Fatalf("expected nil, got %v", result)
	}
}

// TestJITEncoderEmptyString verifies empty string encoding (non-nil []byte{}).
func TestJITEncoderEmptyString(t *testing.T) {
	columns := []ColumnInfo{
		{Name: "name", TypeInfo: NativeType{typ: TypeVarchar, proto: 4}},
	}
	values := []any{""}

	enc := getOrCompileParamEncoder(columns, values)
	result, err := enc.encoders[0]("")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Empty string should produce non-nil zero-length slice (distinguishes from NULL).
	if result == nil {
		t.Fatal("expected non-nil for empty string")
	}
	if len(result) != 0 {
		t.Fatalf("expected zero-length, got %d", len(result))
	}
}

// TestJITEncoderTimestampZero verifies time.Time{} produces empty non-nil.
func TestJITEncoderTimestampZero(t *testing.T) {
	columns := []ColumnInfo{
		{Name: "ts", TypeInfo: NativeType{typ: TypeTimestamp, proto: 4}},
	}
	values := []any{time.Time{}}

	enc := getOrCompileParamEncoder(columns, values)
	jitBytes, jitErr := enc.encoders[0](time.Time{})
	marshalBytes, marshalErr := Marshal(columns[0].TypeInfo, time.Time{})

	if jitErr != nil || marshalErr != nil {
		t.Fatalf("errors: jit=%v, marshal=%v", jitErr, marshalErr)
	}
	if !bytes.Equal(jitBytes, marshalBytes) {
		t.Fatalf("bytes mismatch: jit=%v, marshal=%v", jitBytes, marshalBytes)
	}
}

// TestJITEncoderInet verifies inet encoding for IP and string types.
func TestJITEncoderInet(t *testing.T) {
	columns := []ColumnInfo{
		{Name: "ip", TypeInfo: NativeType{typ: TypeInet, proto: 4}},
	}

	tests := []struct {
		name  string
		value any
	}{
		{"ipv4", net.IPv4(192, 168, 1, 1).To4()},
		{"ipv4 16byte", net.IPv4(10, 0, 0, 1)}, // 16-byte representation
		{"ipv6", net.ParseIP("2001:db8::1")},
		{"nil ip", net.IP(nil)},
		{"string ipv4", "10.0.0.1"},
		{"string ipv6", "::1"},
		{"string empty", ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			values := []any{tt.value}
			enc := getOrCompileParamEncoder(columns, values)
			jitBytes, jitErr := enc.encoders[0](tt.value)
			marshalBytes, marshalErr := Marshal(columns[0].TypeInfo, tt.value)

			if (jitErr != nil) != (marshalErr != nil) {
				t.Fatalf("error mismatch: jit=%v, marshal=%v", jitErr, marshalErr)
			}
			if !bytes.Equal(jitBytes, marshalBytes) {
				t.Fatalf("bytes mismatch: jit=%v, marshal=%v", jitBytes, marshalBytes)
			}
		})
	}
}

// TestJITEncoderNamedTypes verifies named types fall back to Marshal.
func TestJITEncoderNamedTypes(t *testing.T) {
	columns := []ColumnInfo{
		{Name: "id", TypeInfo: NativeType{typ: TypeInt, proto: 4}},
	}
	values := []any{testNamedInt(42)}

	enc := getOrCompileParamEncoder(columns, values)
	jitBytes, jitErr := enc.encoders[0](testNamedInt(42))
	marshalBytes, marshalErr := Marshal(columns[0].TypeInfo, testNamedInt(42))

	if (jitErr != nil) != (marshalErr != nil) {
		t.Fatalf("error mismatch: jit=%v, marshal=%v", jitErr, marshalErr)
	}
	if !bytes.Equal(jitBytes, marshalBytes) {
		t.Fatalf("bytes mismatch: jit=%v, marshal=%v", jitBytes, marshalBytes)
	}
}

// TestJITEncoderIntOverflow verifies int64→int32 overflow produces error.
func TestJITEncoderIntOverflow(t *testing.T) {
	columns := []ColumnInfo{
		{Name: "id", TypeInfo: NativeType{typ: TypeInt, proto: 4}},
	}

	// int64 overflow
	values64 := []any{int64(math.MaxInt32 + 1)}
	enc64 := getOrCompileParamEncoder(columns, values64)
	_, err := enc64.encoders[0](int64(math.MaxInt32 + 1))
	if err == nil {
		t.Fatal("expected overflow error for int64")
	}

	// int overflow (on 64-bit systems)
	valuesInt := []any{int(math.MaxInt32 + 1)}
	encInt := getOrCompileParamEncoder(columns, valuesInt)
	_, err = encInt.encoders[0](int(math.MaxInt32 + 1))
	if err == nil {
		t.Fatal("expected overflow error for int")
	}
}

// TestJITEncoderCacheKeyVersion verifies different versions produce different keys.
func TestJITEncoderCacheKeyVersion(t *testing.T) {
	columnsV3 := []ColumnInfo{
		{Name: "id", TypeInfo: NativeType{typ: TypeInt, proto: 3}},
	}
	columnsV4 := []ColumnInfo{
		{Name: "id", TypeInfo: NativeType{typ: TypeInt, proto: 4}},
	}
	srcTypes := []reflect.Type{reflect.TypeOf(int32(0))}

	key3 := makeEncoderCacheKey(columnsV3, srcTypes)
	key4 := makeEncoderCacheKey(columnsV4, srcTypes)

	if key3 == key4 {
		t.Fatal("cache keys for different protocol versions must differ")
	}
}
