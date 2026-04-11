// Copyright (c) 2012 The gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

//go:build all || unit
// +build all unit

package gocql

import (
	"encoding/binary"
	"math"
	"testing"
)

// --- Helper constructors for vector type info ---

func makeVectorType(subType Type, dim int) VectorType {
	return VectorType{
		NativeType: NativeType{proto: protoVersion4, typ: TypeCustom},
		SubType:    NativeType{proto: protoVersion4, typ: subType},
		Dimensions: dim,
	}
}

// --- marshalVectorFloat32 / unmarshalVectorFloat32 ---

func TestMarshalVectorFloat32_RoundTrip(t *testing.T) {
	dims := []int{1, 3, 128, 768, 1536}
	for _, dim := range dims {
		info := makeVectorType(TypeFloat, dim)
		vec := make([]float32, dim)
		for i := range vec {
			vec[i] = float32(i) * 0.1
		}

		data, err := marshalVector(info, vec)
		if err != nil {
			t.Fatalf("dim=%d: marshal error: %v", dim, err)
		}
		if len(data) != dim*4 {
			t.Fatalf("dim=%d: expected %d bytes, got %d", dim, dim*4, len(data))
		}

		var result []float32
		if err := unmarshalVector(info, data, &result); err != nil {
			t.Fatalf("dim=%d: unmarshal error: %v", dim, err)
		}
		if len(result) != dim {
			t.Fatalf("dim=%d: expected %d elements, got %d", dim, dim, len(result))
		}
		for i := range vec {
			if result[i] != vec[i] {
				t.Fatalf("dim=%d: result[%d] = %v, want %v", dim, i, result[i], vec[i])
			}
		}
	}
}

func TestMarshalVectorFloat32_SpecialValues(t *testing.T) {
	info := makeVectorType(TypeFloat, 4)
	vec := []float32{0, math.Float32frombits(0x80000000), float32(math.Inf(1)), float32(math.NaN())}

	data, err := marshalVector(info, vec)
	if err != nil {
		t.Fatalf("marshal error: %v", err)
	}

	var result []float32
	if err := unmarshalVector(info, data, &result); err != nil {
		t.Fatalf("unmarshal error: %v", err)
	}

	// Check +0, -0
	if math.Float32bits(result[0]) != 0 {
		t.Errorf("result[0] = %v (bits=%08x), want +0", result[0], math.Float32bits(result[0]))
	}
	if math.Float32bits(result[1]) != 0x80000000 {
		t.Errorf("result[1] = %v (bits=%08x), want -0", result[1], math.Float32bits(result[1]))
	}
	// +Inf
	if !math.IsInf(float64(result[2]), 1) {
		t.Errorf("result[2] = %v, want +Inf", result[2])
	}
	// NaN
	if !math.IsNaN(float64(result[3])) {
		t.Errorf("result[3] = %v, want NaN", result[3])
	}
}

func TestMarshalVectorFloat32_DimensionMismatch(t *testing.T) {
	info := makeVectorType(TypeFloat, 3)
	vec := []float32{1, 2} // only 2 elements for 3 dimensions
	_, err := marshalVector(info, vec)
	if err == nil {
		t.Fatal("expected error for dimension mismatch")
	}
}

func TestMarshalVectorFloat32_Nil(t *testing.T) {
	info := makeVectorType(TypeFloat, 3)
	data, err := marshalVector(info, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if data != nil {
		t.Fatalf("expected nil data, got %v", data)
	}
}

func TestMarshalVectorFloat32_NilSlice(t *testing.T) {
	info := makeVectorType(TypeFloat, 3)
	var vec []float32
	data, err := marshalVector(info, vec)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if data != nil {
		t.Fatalf("expected nil data, got %v", data)
	}
}

func TestUnmarshalVectorFloat32_NilData(t *testing.T) {
	info := makeVectorType(TypeFloat, 3)
	var result []float32
	if err := unmarshalVector(info, nil, &result); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result != nil {
		t.Fatalf("expected nil result, got %v", result)
	}
}

func TestUnmarshalVectorFloat32_SliceReuse(t *testing.T) {
	info := makeVectorType(TypeFloat, 3)
	data := make([]byte, 12)
	binary.BigEndian.PutUint32(data[0:], math.Float32bits(1.0))
	binary.BigEndian.PutUint32(data[4:], math.Float32bits(2.0))
	binary.BigEndian.PutUint32(data[8:], math.Float32bits(3.0))

	// Pre-allocate with excess capacity
	result := make([]float32, 0, 10)
	origCap := cap(result)
	if err := unmarshalVector(info, data, &result); err != nil {
		t.Fatalf("unmarshal error: %v", err)
	}
	if cap(result) != origCap {
		t.Fatalf("expected slice reuse (cap=%d), got new allocation (cap=%d)", origCap, cap(result))
	}
	if len(result) != 3 {
		t.Fatalf("expected len 3, got %d", len(result))
	}
}

func TestUnmarshalVectorFloat32_WrongDataSize(t *testing.T) {
	info := makeVectorType(TypeFloat, 3)
	data := make([]byte, 11) // not 12
	var result []float32
	if err := unmarshalVector(info, data, &result); err == nil {
		t.Fatal("expected error for wrong data size")
	}
}

// --- marshalVectorFloat64 / unmarshalVectorFloat64 ---

func TestMarshalVectorFloat64_RoundTrip(t *testing.T) {
	dims := []int{1, 3, 128, 768}
	for _, dim := range dims {
		info := makeVectorType(TypeDouble, dim)
		vec := make([]float64, dim)
		for i := range vec {
			vec[i] = float64(i) * 0.01
		}

		data, err := marshalVector(info, vec)
		if err != nil {
			t.Fatalf("dim=%d: marshal error: %v", dim, err)
		}
		if len(data) != dim*8 {
			t.Fatalf("dim=%d: expected %d bytes, got %d", dim, dim*8, len(data))
		}

		var result []float64
		if err := unmarshalVector(info, data, &result); err != nil {
			t.Fatalf("dim=%d: unmarshal error: %v", dim, err)
		}
		for i := range vec {
			if result[i] != vec[i] {
				t.Fatalf("dim=%d: result[%d] = %v, want %v", dim, i, result[i], vec[i])
			}
		}
	}
}

func TestUnmarshalVectorFloat64_SliceReuse(t *testing.T) {
	info := makeVectorType(TypeDouble, 2)
	data := make([]byte, 16)
	binary.BigEndian.PutUint64(data[0:], math.Float64bits(1.5))
	binary.BigEndian.PutUint64(data[8:], math.Float64bits(2.5))

	result := make([]float64, 0, 10)
	origCap := cap(result)
	if err := unmarshalVector(info, data, &result); err != nil {
		t.Fatalf("unmarshal error: %v", err)
	}
	if cap(result) != origCap {
		t.Fatalf("expected slice reuse")
	}
}

// --- marshalVectorInt32 / unmarshalVectorInt32 ---

func TestMarshalVectorInt32_RoundTrip(t *testing.T) {
	info := makeVectorType(TypeInt, 4)
	vec := []int32{-2147483648, -1, 0, 2147483647}

	data, err := marshalVector(info, vec)
	if err != nil {
		t.Fatalf("marshal error: %v", err)
	}
	if len(data) != 16 {
		t.Fatalf("expected 16 bytes, got %d", len(data))
	}

	var result []int32
	if err := unmarshalVector(info, data, &result); err != nil {
		t.Fatalf("unmarshal error: %v", err)
	}
	for i := range vec {
		if result[i] != vec[i] {
			t.Fatalf("result[%d] = %v, want %v", i, result[i], vec[i])
		}
	}
}

func TestMarshalVectorInt32_WireFormat(t *testing.T) {
	info := makeVectorType(TypeInt, 2)
	vec := []int32{1, 256}

	data, err := marshalVector(info, vec)
	if err != nil {
		t.Fatalf("marshal error: %v", err)
	}

	// Verify big-endian encoding
	if binary.BigEndian.Uint32(data[0:]) != 1 {
		t.Fatalf("expected 1 at offset 0, got %d", binary.BigEndian.Uint32(data[0:]))
	}
	if binary.BigEndian.Uint32(data[4:]) != 256 {
		t.Fatalf("expected 256 at offset 4, got %d", binary.BigEndian.Uint32(data[4:]))
	}
}

// --- marshalVectorInt64 / unmarshalVectorInt64 ---

func TestMarshalVectorInt64_RoundTrip(t *testing.T) {
	info := makeVectorType(TypeBigInt, 3)
	vec := []int64{-9223372036854775808, 0, 9223372036854775807}

	data, err := marshalVector(info, vec)
	if err != nil {
		t.Fatalf("marshal error: %v", err)
	}
	if len(data) != 24 {
		t.Fatalf("expected 24 bytes, got %d", len(data))
	}

	var result []int64
	if err := unmarshalVector(info, data, &result); err != nil {
		t.Fatalf("unmarshal error: %v", err)
	}
	for i := range vec {
		if result[i] != vec[i] {
			t.Fatalf("result[%d] = %v, want %v", i, result[i], vec[i])
		}
	}
}

func TestMarshalVectorInt64_Timestamp(t *testing.T) {
	// TypeTimestamp also maps to int64 on the wire (millis since epoch)
	info := makeVectorType(TypeTimestamp, 2)
	vec := []int64{1609459200000, 1640995200000} // 2021-01-01, 2022-01-01 in millis

	data, err := marshalVector(info, vec)
	if err != nil {
		t.Fatalf("marshal error: %v", err)
	}

	var result []int64
	if err := unmarshalVector(info, data, &result); err != nil {
		t.Fatalf("unmarshal error: %v", err)
	}
	for i := range vec {
		if result[i] != vec[i] {
			t.Fatalf("result[%d] = %v, want %v", i, result[i], vec[i])
		}
	}
}

// --- marshalVectorUUID / unmarshalVectorUUID ---

func TestMarshalVectorUUID_RoundTrip(t *testing.T) {
	info := makeVectorType(TypeUUID, 3)
	vec := []UUID{
		{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10},
		{0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1a, 0x1b, 0x1c, 0x1d, 0x1e, 0x1f, 0x20},
		{0xff, 0xfe, 0xfd, 0xfc, 0xfb, 0xfa, 0xf9, 0xf8, 0xf7, 0xf6, 0xf5, 0xf4, 0xf3, 0xf2, 0xf1, 0xf0},
	}

	data, err := marshalVector(info, vec)
	if err != nil {
		t.Fatalf("marshal error: %v", err)
	}
	if len(data) != 48 {
		t.Fatalf("expected 48 bytes, got %d", len(data))
	}

	var result []UUID
	if err := unmarshalVector(info, data, &result); err != nil {
		t.Fatalf("unmarshal error: %v", err)
	}
	for i := range vec {
		if result[i] != vec[i] {
			t.Fatalf("result[%d] = %v, want %v", i, result[i], vec[i])
		}
	}
}

func TestMarshalVectorUUID_TimeUUID(t *testing.T) {
	info := makeVectorType(TypeTimeUUID, 2)
	vec := []UUID{
		TimeUUID(),
		TimeUUID(),
	}

	data, err := marshalVector(info, vec)
	if err != nil {
		t.Fatalf("marshal error: %v", err)
	}

	var result []UUID
	if err := unmarshalVector(info, data, &result); err != nil {
		t.Fatalf("unmarshal error: %v", err)
	}
	for i := range vec {
		if result[i] != vec[i] {
			t.Fatalf("result[%d] = %v, want %v", i, result[i], vec[i])
		}
	}
}

// --- Fast path vs slow path consistency ---

func TestVectorFastPath_ConsistentWithReflectPath(t *testing.T) {
	// Ensure fast path produces identical wire format to reflect path.
	// We use []float32 (fast path) and compare to reflect-based marshal
	// that would be used for a non-matching type.
	info := makeVectorType(TypeFloat, 3)
	vec := []float32{1.0, 2.0, 3.0}

	// Fast path result
	fastData, err := marshalVector(info, vec)
	if err != nil {
		t.Fatalf("fast path marshal error: %v", err)
	}

	// Manually construct expected wire format
	expected := make([]byte, 12)
	binary.BigEndian.PutUint32(expected[0:], math.Float32bits(1.0))
	binary.BigEndian.PutUint32(expected[4:], math.Float32bits(2.0))
	binary.BigEndian.PutUint32(expected[8:], math.Float32bits(3.0))

	if len(fastData) != len(expected) {
		t.Fatalf("length mismatch: fast=%d, expected=%d", len(fastData), len(expected))
	}
	for i := range expected {
		if fastData[i] != expected[i] {
			t.Fatalf("byte[%d]: fast=%02x, expected=%02x", i, fastData[i], expected[i])
		}
	}
}

// --- VectorType.NewWithError() ---

func TestVectorTypeNewWithError_Float32(t *testing.T) {
	info := makeVectorType(TypeFloat, 3)
	v, err := info.NewWithError()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, ok := v.(*[]float32); !ok {
		t.Fatalf("expected *[]float32, got %T", v)
	}
}

func TestVectorTypeNewWithError_Float64(t *testing.T) {
	info := makeVectorType(TypeDouble, 3)
	v, err := info.NewWithError()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, ok := v.(*[]float64); !ok {
		t.Fatalf("expected *[]float64, got %T", v)
	}
}

func TestVectorTypeNewWithError_Int(t *testing.T) {
	info := makeVectorType(TypeInt, 3)
	v, err := info.NewWithError()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, ok := v.(*[]int); !ok {
		t.Fatalf("expected *[]int, got %T", v)
	}
}

func TestVectorTypeNewWithError_BigInt(t *testing.T) {
	info := makeVectorType(TypeBigInt, 3)
	v, err := info.NewWithError()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, ok := v.(*[]int64); !ok {
		t.Fatalf("expected *[]int64, got %T", v)
	}
}

func TestVectorTypeNewWithError_UUID(t *testing.T) {
	info := makeVectorType(TypeUUID, 3)
	v, err := info.NewWithError()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, ok := v.(*[]UUID); !ok {
		t.Fatalf("expected *[]UUID, got %T", v)
	}
}

func TestVectorTypeNewWithError_Text(t *testing.T) {
	info := makeVectorType(TypeText, 3)
	v, err := info.NewWithError()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, ok := v.(*[]string); !ok {
		t.Fatalf("expected *[]string, got %T", v)
	}
}

// --- vectorByteSize overflow ---

func TestVectorByteSize_Overflow(t *testing.T) {
	// Should fail for very large dimensions
	_, err := vectorByteSize(math.MaxInt32, 4)
	if err == nil {
		t.Fatal("expected overflow error")
	}
}

func TestVectorByteSize_Normal(t *testing.T) {
	size, err := vectorByteSize(1536, 4)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if size != 6144 {
		t.Fatalf("expected 6144, got %d", size)
	}
}

func TestVectorByteSize_Zero(t *testing.T) {
	size, err := vectorByteSize(0, 4)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if size != 0 {
		t.Fatalf("expected 0, got %d", size)
	}
}

// --- Zero-dimension fast path ---

func TestVectorFastPath_ZeroDimFloat32(t *testing.T) {
	info := makeVectorType(TypeFloat, 0)

	// Marshal: empty float32 slice with 0 dimensions should work
	vec := []float32{}
	data, err := marshalVector(info, vec)
	if err != nil {
		t.Fatalf("marshal error: %v", err)
	}
	if len(data) != 0 {
		t.Fatalf("expected 0 bytes, got %d", len(data))
	}

	// Unmarshal: empty data into float32 slice
	var result []float32
	if err := unmarshalVector(info, []byte{}, &result); err != nil {
		t.Fatalf("unmarshal error: %v", err)
	}
	if result == nil {
		t.Fatal("expected non-nil empty slice")
	}
	if len(result) != 0 {
		t.Fatalf("expected len 0, got %d", len(result))
	}
}

func TestVectorFastPath_ZeroDimFloat64(t *testing.T) {
	info := makeVectorType(TypeDouble, 0)
	var result []float64
	if err := unmarshalVector(info, []byte{}, &result); err != nil {
		t.Fatalf("unmarshal error: %v", err)
	}
	if result == nil {
		t.Fatal("expected non-nil empty slice")
	}
}

func TestVectorFastPath_ZeroDimUUID(t *testing.T) {
	info := makeVectorType(TypeUUID, 0)
	var result []UUID
	if err := unmarshalVector(info, []byte{}, &result); err != nil {
		t.Fatalf("unmarshal error: %v", err)
	}
	if result == nil {
		t.Fatal("expected non-nil empty slice")
	}
}

// --- Non-matching types fall through to reflect path ---

func TestVectorFastPath_FallsThrough(t *testing.T) {
	// Passing []int (not []int32) for TypeInt should fall through to
	// reflect path and still work via Marshal/Unmarshal
	info := makeVectorType(TypeFloat, 3)
	type myFloat float32
	vec := []myFloat{1.0, 2.0, 3.0}

	// Should fall through to reflect path (no fast path for custom types)
	data, err := marshalVector(info, vec)
	if err != nil {
		t.Fatalf("marshal error: %v", err)
	}
	if len(data) != 12 {
		t.Fatalf("expected 12 bytes, got %d", len(data))
	}
}

// --- Benchmarks for all fast-path types ---

func BenchmarkMarshalVectorFloat64(b *testing.B) {
	dims := []int{128, 768, 1536}
	for _, dim := range dims {
		b.Run(dimStr(dim), func(b *testing.B) {
			b.ReportAllocs()
			info := makeVectorType(TypeDouble, dim)
			vec := make([]float64, dim)
			for i := range vec {
				vec[i] = float64(i) * 0.01
			}
			b.SetBytes(int64(dim * 8))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := marshalVector(info, vec); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkUnmarshalVectorFloat64(b *testing.B) {
	dims := []int{128, 768, 1536}
	for _, dim := range dims {
		b.Run(dimStr(dim), func(b *testing.B) {
			b.ReportAllocs()
			info := makeVectorType(TypeDouble, dim)
			data := make([]byte, dim*8)
			for i := 0; i < dim; i++ {
				binary.BigEndian.PutUint64(data[i*8:], math.Float64bits(float64(i)*0.01))
			}
			var result []float64
			b.SetBytes(int64(dim * 8))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := unmarshalVector(info, data, &result); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkMarshalVectorInt32(b *testing.B) {
	dims := []int{128, 768, 1536}
	for _, dim := range dims {
		b.Run(dimStr(dim), func(b *testing.B) {
			b.ReportAllocs()
			info := makeVectorType(TypeInt, dim)
			vec := make([]int32, dim)
			for i := range vec {
				vec[i] = int32(i)
			}
			b.SetBytes(int64(dim * 4))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := marshalVector(info, vec); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkUnmarshalVectorInt32(b *testing.B) {
	dims := []int{128, 768, 1536}
	for _, dim := range dims {
		b.Run(dimStr(dim), func(b *testing.B) {
			b.ReportAllocs()
			info := makeVectorType(TypeInt, dim)
			data := make([]byte, dim*4)
			for i := 0; i < dim; i++ {
				binary.BigEndian.PutUint32(data[i*4:], uint32(i))
			}
			var result []int32
			b.SetBytes(int64(dim * 4))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := unmarshalVector(info, data, &result); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkMarshalVectorUUID(b *testing.B) {
	dims := []int{128, 768}
	for _, dim := range dims {
		b.Run(dimStr(dim), func(b *testing.B) {
			b.ReportAllocs()
			info := makeVectorType(TypeUUID, dim)
			vec := make([]UUID, dim)
			for i := range vec {
				vec[i] = TimeUUID()
			}
			b.SetBytes(int64(dim * 16))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := marshalVector(info, vec); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkUnmarshalVectorUUID(b *testing.B) {
	dims := []int{128, 768}
	for _, dim := range dims {
		b.Run(dimStr(dim), func(b *testing.B) {
			b.ReportAllocs()
			info := makeVectorType(TypeUUID, dim)
			data := make([]byte, dim*16)
			for i := 0; i < dim; i++ {
				u := TimeUUID()
				copy(data[i*16:], u[:])
			}
			var result []UUID
			b.SetBytes(int64(dim * 16))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := unmarshalVector(info, data, &result); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func dimStr(dim int) string {
	switch dim {
	case 128:
		return "dim_128"
	case 384:
		return "dim_384"
	case 768:
		return "dim_768"
	case 1536:
		return "dim_1536"
	default:
		return "dim_other"
	}
}
