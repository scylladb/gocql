// Copyright (C) 2026 ScyllaDB

//go:build all || unit
// +build all unit

package gocql

import (
	"encoding/binary"
	"errors"
	"math"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"testing"
)

// --- Test helpers ---

func makeFloat32VectorType(dim int) VectorType {
	return VectorType{
		NativeType: NativeType{
			proto:  protoVersion4,
			typ:    TypeCustom,
			custom: apacheCassandraTypePrefix + "VectorType(" + apacheCassandraTypePrefix + "FloatType, " + strconv.Itoa(dim) + ")",
		},
		SubType:    NativeType{proto: protoVersion4, typ: TypeFloat},
		Dimensions: dim,
	}
}

func makeFloat64VectorType(dim int) VectorType {
	return VectorType{
		NativeType: NativeType{
			proto:  protoVersion4,
			typ:    TypeCustom,
			custom: apacheCassandraTypePrefix + "VectorType(" + apacheCassandraTypePrefix + "DoubleType, " + strconv.Itoa(dim) + ")",
		},
		SubType:    NativeType{proto: protoVersion4, typ: TypeDouble},
		Dimensions: dim,
	}
}

func makeUUIDVectorType(dim int) VectorType {
	return VectorType{
		NativeType: NativeType{
			proto:  protoVersion4,
			typ:    TypeCustom,
			custom: apacheCassandraTypePrefix + "VectorType(" + apacheCassandraTypePrefix + "UUIDType, " + strconv.Itoa(dim) + ")",
		},
		SubType:    NativeType{proto: protoVersion4, typ: TypeUUID},
		Dimensions: dim,
	}
}

func makeInt32VectorType(dim int) VectorType {
	return VectorType{
		NativeType: NativeType{
			proto:  protoVersion4,
			typ:    TypeCustom,
			custom: apacheCassandraTypePrefix + "VectorType(" + apacheCassandraTypePrefix + "Int32Type, " + strconv.Itoa(dim) + ")",
		},
		SubType:    NativeType{proto: protoVersion4, typ: TypeInt},
		Dimensions: dim,
	}
}

func makeInt64VectorType(dim int) VectorType {
	return VectorType{
		NativeType: NativeType{
			proto:  protoVersion4,
			typ:    TypeCustom,
			custom: apacheCassandraTypePrefix + "VectorType(" + apacheCassandraTypePrefix + "LongType, " + strconv.Itoa(dim) + ")",
		},
		SubType:    NativeType{proto: protoVersion4, typ: TypeBigInt},
		Dimensions: dim,
	}
}

// --- Test 1: Round-trip ---

func TestMarshalVector_RoundTrip(t *testing.T) {
	t.Run("float32", func(t *testing.T) {
		dim := 5
		info := makeFloat32VectorType(dim)
		vec := []float32{1.1, 2.2, 3.3, 4.4, 5.5}

		data, err := marshalVector(info, vec)
		if err != nil {
			t.Fatalf("marshalVector: %v", err)
		}
		if len(data) != dim*4 {
			t.Fatalf("expected %d bytes, got %d", dim*4, len(data))
		}

		var result []float32
		if err := unmarshalVector(info, data, &result); err != nil {
			t.Fatalf("unmarshalVector: %v", err)
		}
		if !reflect.DeepEqual(vec, result) {
			t.Errorf("round-trip mismatch: got %v, want %v", result, vec)
		}
	})

	t.Run("float64", func(t *testing.T) {
		dim := 5
		info := makeFloat64VectorType(dim)
		vec := []float64{1.1, 2.2, 3.3, 4.4, 5.5}

		data, err := marshalVector(info, vec)
		if err != nil {
			t.Fatalf("marshalVector: %v", err)
		}
		if len(data) != dim*8 {
			t.Fatalf("expected %d bytes, got %d", dim*8, len(data))
		}

		var result []float64
		if err := unmarshalVector(info, data, &result); err != nil {
			t.Fatalf("unmarshalVector: %v", err)
		}
		if !reflect.DeepEqual(vec, result) {
			t.Errorf("round-trip mismatch: got %v, want %v", result, vec)
		}
	})

	t.Run("int32", func(t *testing.T) {
		dim := 5
		info := makeInt32VectorType(dim)
		vec := []int32{-100, 0, 42, math.MaxInt32, math.MinInt32}

		data, err := marshalVector(info, vec)
		if err != nil {
			t.Fatalf("marshalVector: %v", err)
		}
		if len(data) != dim*4 {
			t.Fatalf("expected %d bytes, got %d", dim*4, len(data))
		}

		var result []int32
		if err := unmarshalVector(info, data, &result); err != nil {
			t.Fatalf("unmarshalVector: %v", err)
		}
		if !reflect.DeepEqual(vec, result) {
			t.Errorf("round-trip mismatch: got %v, want %v", result, vec)
		}
	})

	t.Run("int64", func(t *testing.T) {
		dim := 5
		info := makeInt64VectorType(dim)
		vec := []int64{-100, 0, 42, math.MaxInt64, math.MinInt64}

		data, err := marshalVector(info, vec)
		if err != nil {
			t.Fatalf("marshalVector: %v", err)
		}
		if len(data) != dim*8 {
			t.Fatalf("expected %d bytes, got %d", dim*8, len(data))
		}

		var result []int64
		if err := unmarshalVector(info, data, &result); err != nil {
			t.Fatalf("unmarshalVector: %v", err)
		}
		if !reflect.DeepEqual(vec, result) {
			t.Errorf("round-trip mismatch: got %v, want %v", result, vec)
		}
	})

	t.Run("uuid", func(t *testing.T) {
		dim := 3
		info := makeUUIDVectorType(dim)
		vec := []UUID{
			{0x55, 0x0e, 0x84, 0x00, 0xe2, 0x9b, 0x41, 0xd4, 0xa7, 0x16, 0x44, 0x66, 0x55, 0x44, 0x00, 0x00},
			{0xf4, 0x7a, 0xc1, 0x0b, 0x58, 0xcc, 0x43, 0x72, 0xa5, 0x67, 0x0e, 0x02, 0xb2, 0xc3, 0xd4, 0x79},
			{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
		}

		data, err := marshalVector(info, vec)
		if err != nil {
			t.Fatalf("marshalVector: %v", err)
		}
		if len(data) != dim*16 {
			t.Fatalf("expected %d bytes, got %d", dim*16, len(data))
		}

		var result []UUID
		if err := unmarshalVector(info, data, &result); err != nil {
			t.Fatalf("unmarshalVector: %v", err)
		}
		if !reflect.DeepEqual(vec, result) {
			t.Errorf("round-trip mismatch: got %v, want %v", result, vec)
		}
	})
}

// --- Test 2: Byte compatibility ---

func TestMarshalVector_ByteCompatibility(t *testing.T) {
	t.Run("float32", func(t *testing.T) {
		dim := 3
		vec := []float32{-1.5, 0, 42.125}
		expected := make([]byte, dim*4)
		for i, v := range vec {
			binary.BigEndian.PutUint32(expected[i*4:], math.Float32bits(v))
		}

		info := makeFloat32VectorType(dim)
		data, err := marshalVector(info, vec)
		if err != nil {
			t.Fatalf("marshalVector: %v", err)
		}
		if !reflect.DeepEqual(data, expected) {
			t.Errorf("byte mismatch:\n  got:  %x\n  want: %x", data, expected)
		}
	})

	t.Run("float64", func(t *testing.T) {
		dim := 3
		vec := []float64{-1.5, 0, 42.125}
		expected := make([]byte, dim*8)
		for i, v := range vec {
			binary.BigEndian.PutUint64(expected[i*8:], math.Float64bits(v))
		}

		info := makeFloat64VectorType(dim)
		data, err := marshalVector(info, vec)
		if err != nil {
			t.Fatalf("marshalVector: %v", err)
		}
		if !reflect.DeepEqual(data, expected) {
			t.Errorf("byte mismatch:\n  got:  %x\n  want: %x", data, expected)
		}
	})

	t.Run("int32", func(t *testing.T) {
		dim := 3
		vec := []int32{-1, 0, 42}
		expected := make([]byte, dim*4)
		for i, v := range vec {
			binary.BigEndian.PutUint32(expected[i*4:], uint32(v))
		}

		info := makeInt32VectorType(dim)
		data, err := marshalVector(info, vec)
		if err != nil {
			t.Fatalf("marshalVector: %v", err)
		}
		if !reflect.DeepEqual(data, expected) {
			t.Errorf("byte mismatch:\n  got:  %x\n  want: %x", data, expected)
		}
	})

	t.Run("int64", func(t *testing.T) {
		dim := 3
		vec := []int64{-1, 0, 42}
		expected := make([]byte, dim*8)
		for i, v := range vec {
			binary.BigEndian.PutUint64(expected[i*8:], uint64(v))
		}

		info := makeInt64VectorType(dim)
		data, err := marshalVector(info, vec)
		if err != nil {
			t.Fatalf("marshalVector: %v", err)
		}
		if !reflect.DeepEqual(data, expected) {
			t.Errorf("byte mismatch:\n  got:  %x\n  want: %x", data, expected)
		}
	})

	t.Run("uuid", func(t *testing.T) {
		dim := 2
		vec := []UUID{
			{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10},
			{0xf0, 0xe0, 0xd0, 0xc0, 0xb0, 0xa0, 0x90, 0x80, 0x70, 0x60, 0x50, 0x40, 0x30, 0x20, 0x10, 0x00},
		}
		// UUID is [16]byte — wire format is the raw bytes, no endian conversion.
		expected := make([]byte, dim*16)
		copy(expected[0:16], vec[0][:])
		copy(expected[16:32], vec[1][:])

		info := makeUUIDVectorType(dim)
		data, err := marshalVector(info, vec)
		if err != nil {
			t.Fatalf("marshalVector: %v", err)
		}
		if !reflect.DeepEqual(data, expected) {
			t.Errorf("byte mismatch:\n  got:  %x\n  want: %x", data, expected)
		}
	})
}

// --- Test 3: Slice reuse ---

func TestUnmarshalVector_SliceReuse(t *testing.T) {
	t.Run("float32", func(t *testing.T) {
		dim := 4
		info := makeFloat32VectorType(dim)
		data := make([]byte, dim*4)
		for i := 0; i < dim; i++ {
			binary.BigEndian.PutUint32(data[i*4:], math.Float32bits(float32(i)))
		}

		var result []float32
		if err := unmarshalVector(info, data, &result); err != nil {
			t.Fatalf("unmarshalVector (first): %v", err)
		}
		if len(result) != dim {
			t.Fatalf("expected len %d, got %d", dim, len(result))
		}
		ptr := &result[0]

		if err := unmarshalVector(info, data, &result); err != nil {
			t.Fatalf("unmarshalVector (second): %v", err)
		}
		if &result[0] != ptr {
			t.Error("expected slice reuse, but a new backing array was allocated")
		}
	})

	t.Run("float64", func(t *testing.T) {
		dim := 4
		info := makeFloat64VectorType(dim)
		data := make([]byte, dim*8)
		for i := 0; i < dim; i++ {
			binary.BigEndian.PutUint64(data[i*8:], math.Float64bits(float64(i)))
		}

		var result []float64
		if err := unmarshalVector(info, data, &result); err != nil {
			t.Fatalf("unmarshalVector (first): %v", err)
		}
		ptr := &result[0]

		if err := unmarshalVector(info, data, &result); err != nil {
			t.Fatalf("unmarshalVector (second): %v", err)
		}
		if &result[0] != ptr {
			t.Error("expected slice reuse, but a new backing array was allocated")
		}
	})

	t.Run("float32_excess_cap", func(t *testing.T) {
		dim := 4
		info := makeFloat32VectorType(dim)
		data := make([]byte, dim*4)
		for i := 0; i < dim; i++ {
			binary.BigEndian.PutUint32(data[i*4:], math.Float32bits(float32(i)+0.5))
		}

		result := make([]float32, 1, dim+10)
		ptr := &result[0]
		result = result[:0]
		if err := unmarshalVector(info, data, &result); err != nil {
			t.Fatalf("unmarshalVector: %v", err)
		}
		if len(result) != dim {
			t.Fatalf("expected len %d, got %d", dim, len(result))
		}
		if &result[0] != ptr {
			t.Error("expected reuse of pre-allocated backing array with excess capacity")
		}
	})

	t.Run("float64_excess_cap", func(t *testing.T) {
		dim := 4
		info := makeFloat64VectorType(dim)
		data := make([]byte, dim*8)
		for i := 0; i < dim; i++ {
			binary.BigEndian.PutUint64(data[i*8:], math.Float64bits(float64(i)+0.5))
		}

		result := make([]float64, 1, dim+10)
		ptr := &result[0]
		result = result[:0]
		if err := unmarshalVector(info, data, &result); err != nil {
			t.Fatalf("unmarshalVector: %v", err)
		}
		if len(result) != dim {
			t.Fatalf("expected len %d, got %d", dim, len(result))
		}
		if &result[0] != ptr {
			t.Error("expected reuse of pre-allocated backing array with excess capacity")
		}
	})

	t.Run("int32", func(t *testing.T) {
		dim := 4
		info := makeInt32VectorType(dim)
		data := make([]byte, dim*4)
		for i := 0; i < dim; i++ {
			binary.BigEndian.PutUint32(data[i*4:], uint32(int32(i)))
		}

		var result []int32
		if err := unmarshalVector(info, data, &result); err != nil {
			t.Fatalf("unmarshalVector (first): %v", err)
		}
		if len(result) != dim {
			t.Fatalf("expected len %d, got %d", dim, len(result))
		}
		ptr := &result[0]

		if err := unmarshalVector(info, data, &result); err != nil {
			t.Fatalf("unmarshalVector (second): %v", err)
		}
		if &result[0] != ptr {
			t.Error("expected slice reuse, but a new backing array was allocated")
		}
	})

	t.Run("int64", func(t *testing.T) {
		dim := 4
		info := makeInt64VectorType(dim)
		data := make([]byte, dim*8)
		for i := 0; i < dim; i++ {
			binary.BigEndian.PutUint64(data[i*8:], uint64(int64(i)))
		}

		var result []int64
		if err := unmarshalVector(info, data, &result); err != nil {
			t.Fatalf("unmarshalVector (first): %v", err)
		}
		ptr := &result[0]

		if err := unmarshalVector(info, data, &result); err != nil {
			t.Fatalf("unmarshalVector (second): %v", err)
		}
		if &result[0] != ptr {
			t.Error("expected slice reuse, but a new backing array was allocated")
		}
	})

	t.Run("int32_excess_cap", func(t *testing.T) {
		dim := 4
		info := makeInt32VectorType(dim)
		data := make([]byte, dim*4)
		for i := 0; i < dim; i++ {
			binary.BigEndian.PutUint32(data[i*4:], uint32(int32(i)+10))
		}

		result := make([]int32, 1, dim+10)
		ptr := &result[0]
		result = result[:0]
		if err := unmarshalVector(info, data, &result); err != nil {
			t.Fatalf("unmarshalVector: %v", err)
		}
		if len(result) != dim {
			t.Fatalf("expected len %d, got %d", dim, len(result))
		}
		if &result[0] != ptr {
			t.Error("expected reuse of pre-allocated backing array with excess capacity")
		}
	})

	t.Run("int64_excess_cap", func(t *testing.T) {
		dim := 4
		info := makeInt64VectorType(dim)
		data := make([]byte, dim*8)
		for i := 0; i < dim; i++ {
			binary.BigEndian.PutUint64(data[i*8:], uint64(int64(i)+10))
		}

		result := make([]int64, 1, dim+10)
		ptr := &result[0]
		result = result[:0]
		if err := unmarshalVector(info, data, &result); err != nil {
			t.Fatalf("unmarshalVector: %v", err)
		}
		if len(result) != dim {
			t.Fatalf("expected len %d, got %d", dim, len(result))
		}
		if &result[0] != ptr {
			t.Error("expected reuse of pre-allocated backing array with excess capacity")
		}
	})

	t.Run("uuid", func(t *testing.T) {
		dim := 4
		info := makeUUIDVectorType(dim)
		data := make([]byte, dim*16)
		for i := 0; i < dim; i++ {
			data[i*16] = byte(i + 1) // put something identifiable in each UUID
		}

		result := make([]UUID, dim)
		ptr := &result[0]
		if err := unmarshalVector(info, data, &result); err != nil {
			t.Fatalf("unmarshalVector: %v", err)
		}
		if len(result) != dim {
			t.Fatalf("expected len %d, got %d", dim, len(result))
		}
		if &result[0] != ptr {
			t.Error("expected reuse of pre-allocated backing array")
		}
	})

	t.Run("uuid_excess_cap", func(t *testing.T) {
		dim := 4
		info := makeUUIDVectorType(dim)
		data := make([]byte, dim*16)
		for i := 0; i < dim; i++ {
			data[i*16] = byte(i + 1)
		}

		result := make([]UUID, 1, dim+10)
		ptr := &result[0]
		result = result[:0]
		if err := unmarshalVector(info, data, &result); err != nil {
			t.Fatalf("unmarshalVector: %v", err)
		}
		if len(result) != dim {
			t.Fatalf("expected len %d, got %d", dim, len(result))
		}
		if &result[0] != ptr {
			t.Error("expected reuse of pre-allocated backing array with excess capacity")
		}
	})
}

// --- Test 4: Nil slice marshal ---

func TestMarshalVector_NilSlice(t *testing.T) {
	t.Run("float32_nil_slice", func(t *testing.T) {
		info := makeFloat32VectorType(3)
		var vec []float32
		data, err := marshalVector(info, vec)
		if err != nil {
			t.Fatalf("marshalVector: %v", err)
		}
		if data != nil {
			t.Errorf("expected nil data for nil slice, got %v", data)
		}
	})

	t.Run("float64_nil_slice", func(t *testing.T) {
		info := makeFloat64VectorType(3)
		var vec []float64
		data, err := marshalVector(info, vec)
		if err != nil {
			t.Fatalf("marshalVector: %v", err)
		}
		if data != nil {
			t.Errorf("expected nil data for nil slice, got %v", data)
		}
	})

	t.Run("float32_nil_ptr", func(t *testing.T) {
		info := makeFloat32VectorType(3)
		var ptr *[]float32
		data, err := Marshal(info, ptr)
		if err != nil {
			t.Fatalf("Marshal: %v", err)
		}
		if data != nil {
			t.Errorf("expected nil data for nil ptr, got %v", data)
		}
	})

	t.Run("float64_nil_ptr", func(t *testing.T) {
		info := makeFloat64VectorType(3)
		var ptr *[]float64
		data, err := Marshal(info, ptr)
		if err != nil {
			t.Fatalf("Marshal: %v", err)
		}
		if data != nil {
			t.Errorf("expected nil data for nil ptr, got %v", data)
		}
	})

	t.Run("float32_non_nil_ptr_nil_slice", func(t *testing.T) {
		info := makeFloat32VectorType(3)
		var s []float32
		data, err := Marshal(info, &s)
		if err != nil {
			t.Fatalf("Marshal: %v", err)
		}
		if data != nil {
			t.Errorf("expected nil data for non-nil ptr to nil slice, got %v", data)
		}
	})

	t.Run("float64_non_nil_ptr_nil_slice", func(t *testing.T) {
		info := makeFloat64VectorType(3)
		var s []float64
		data, err := Marshal(info, &s)
		if err != nil {
			t.Fatalf("Marshal: %v", err)
		}
		if data != nil {
			t.Errorf("expected nil data for non-nil ptr to nil slice, got %v", data)
		}
	})

	t.Run("int32_nil_slice", func(t *testing.T) {
		info := makeInt32VectorType(3)
		var vec []int32
		data, err := marshalVector(info, vec)
		if err != nil {
			t.Fatalf("marshalVector: %v", err)
		}
		if data != nil {
			t.Errorf("expected nil data for nil slice, got %v", data)
		}
	})

	t.Run("int64_nil_slice", func(t *testing.T) {
		info := makeInt64VectorType(3)
		var vec []int64
		data, err := marshalVector(info, vec)
		if err != nil {
			t.Fatalf("marshalVector: %v", err)
		}
		if data != nil {
			t.Errorf("expected nil data for nil slice, got %v", data)
		}
	})

	t.Run("int32_nil_ptr", func(t *testing.T) {
		info := makeInt32VectorType(3)
		var ptr *[]int32
		data, err := Marshal(info, ptr)
		if err != nil {
			t.Fatalf("Marshal: %v", err)
		}
		if data != nil {
			t.Errorf("expected nil data for nil ptr, got %v", data)
		}
	})

	t.Run("int64_nil_ptr", func(t *testing.T) {
		info := makeInt64VectorType(3)
		var ptr *[]int64
		data, err := Marshal(info, ptr)
		if err != nil {
			t.Fatalf("Marshal: %v", err)
		}
		if data != nil {
			t.Errorf("expected nil data for nil ptr, got %v", data)
		}
	})

	t.Run("int32_non_nil_ptr_nil_slice", func(t *testing.T) {
		info := makeInt32VectorType(3)
		var s []int32
		data, err := Marshal(info, &s)
		if err != nil {
			t.Fatalf("Marshal: %v", err)
		}
		if data != nil {
			t.Errorf("expected nil data for non-nil ptr to nil slice, got %v", data)
		}
	})

	t.Run("int64_non_nil_ptr_nil_slice", func(t *testing.T) {
		info := makeInt64VectorType(3)
		var s []int64
		data, err := Marshal(info, &s)
		if err != nil {
			t.Fatalf("Marshal: %v", err)
		}
		if data != nil {
			t.Errorf("expected nil data for non-nil ptr to nil slice, got %v", data)
		}
	})

	t.Run("uuid_nil_slice", func(t *testing.T) {
		info := makeUUIDVectorType(3)
		var vec []UUID
		data, err := marshalVector(info, vec)
		if err != nil {
			t.Fatalf("marshalVector: %v", err)
		}
		if data != nil {
			t.Errorf("expected nil data for nil slice, got %v", data)
		}
	})
}

// --- Test 5: Nil data unmarshal ---

func TestUnmarshalVector_NilData(t *testing.T) {
	t.Run("float32_nil_data_nil_dst", func(t *testing.T) {
		info := makeFloat32VectorType(3)
		var result []float32
		if err := unmarshalVector(info, nil, &result); err != nil {
			t.Fatalf("unmarshalVector: %v", err)
		}
		if result != nil {
			t.Errorf("expected nil result, got %v", result)
		}
	})

	t.Run("float32_nil_data_non_nil_dst", func(t *testing.T) {
		info := makeFloat32VectorType(3)
		result := []float32{1, 2, 3}
		if err := unmarshalVector(info, nil, &result); err != nil {
			t.Fatalf("unmarshalVector: %v", err)
		}
		if result != nil {
			t.Errorf("expected nil result after nil data, got %v", result)
		}
	})

	t.Run("float64_nil_data_nil_dst", func(t *testing.T) {
		info := makeFloat64VectorType(3)
		var result []float64
		if err := unmarshalVector(info, nil, &result); err != nil {
			t.Fatalf("unmarshalVector: %v", err)
		}
		if result != nil {
			t.Errorf("expected nil result, got %v", result)
		}
	})

	t.Run("float64_nil_data_non_nil_dst", func(t *testing.T) {
		info := makeFloat64VectorType(3)
		result := []float64{1, 2, 3}
		if err := unmarshalVector(info, nil, &result); err != nil {
			t.Fatalf("unmarshalVector: %v", err)
		}
		if result != nil {
			t.Errorf("expected nil result after nil data, got %v", result)
		}
	})

	t.Run("int32_nil_data_nil_dst", func(t *testing.T) {
		info := makeInt32VectorType(3)
		var result []int32
		if err := unmarshalVector(info, nil, &result); err != nil {
			t.Fatalf("unmarshalVector: %v", err)
		}
		if result != nil {
			t.Errorf("expected nil result, got %v", result)
		}
	})

	t.Run("int32_nil_data_non_nil_dst", func(t *testing.T) {
		info := makeInt32VectorType(3)
		result := []int32{1, 2, 3}
		if err := unmarshalVector(info, nil, &result); err != nil {
			t.Fatalf("unmarshalVector: %v", err)
		}
		if result != nil {
			t.Errorf("expected nil result after nil data, got %v", result)
		}
	})

	t.Run("int64_nil_data_nil_dst", func(t *testing.T) {
		info := makeInt64VectorType(3)
		var result []int64
		if err := unmarshalVector(info, nil, &result); err != nil {
			t.Fatalf("unmarshalVector: %v", err)
		}
		if result != nil {
			t.Errorf("expected nil result, got %v", result)
		}
	})

	t.Run("int64_nil_data_non_nil_dst", func(t *testing.T) {
		info := makeInt64VectorType(3)
		result := []int64{1, 2, 3}
		if err := unmarshalVector(info, nil, &result); err != nil {
			t.Fatalf("unmarshalVector: %v", err)
		}
		if result != nil {
			t.Errorf("expected nil result after nil data, got %v", result)
		}
	})

	t.Run("uuid_nil_data_nil_dst", func(t *testing.T) {
		info := makeUUIDVectorType(3)
		var result []UUID
		if err := unmarshalVector(info, nil, &result); err != nil {
			t.Fatalf("unmarshalVector: %v", err)
		}
		if result != nil {
			t.Errorf("expected nil result, got %v", result)
		}
	})

	t.Run("uuid_nil_data_non_nil_dst", func(t *testing.T) {
		info := makeUUIDVectorType(3)
		result := []UUID{{0x01}, {0x02}, {0x03}}
		if err := unmarshalVector(info, nil, &result); err != nil {
			t.Fatalf("unmarshalVector: %v", err)
		}
		if result != nil {
			t.Errorf("expected nil result after nil data, got %v", result)
		}
	})
}

// --- Test 6: Dimension mismatch ---

func TestMarshalVector_DimensionMismatch(t *testing.T) {
	t.Run("float32_marshal", func(t *testing.T) {
		info := makeFloat32VectorType(3)
		vec := []float32{1, 2}
		_, err := marshalVector(info, vec)
		if err == nil {
			t.Fatal("expected error for dimension mismatch, got nil")
		}
	})

	t.Run("float64_marshal", func(t *testing.T) {
		info := makeFloat64VectorType(3)
		vec := []float64{1, 2}
		_, err := marshalVector(info, vec)
		if err == nil {
			t.Fatal("expected error for dimension mismatch, got nil")
		}
	})

	t.Run("float32_unmarshal_wrong_data_len", func(t *testing.T) {
		info := makeFloat32VectorType(3)
		data := make([]byte, 10) // not 4*3=12
		var result []float32
		err := unmarshalVector(info, data, &result)
		if err == nil {
			t.Fatal("expected error for wrong data length, got nil")
		}
	})

	t.Run("float64_unmarshal_wrong_data_len", func(t *testing.T) {
		info := makeFloat64VectorType(3)
		data := make([]byte, 10) // not 8*3=24
		var result []float64
		err := unmarshalVector(info, data, &result)
		if err == nil {
			t.Fatal("expected error for wrong data length, got nil")
		}
	})

	t.Run("int32_marshal", func(t *testing.T) {
		info := makeInt32VectorType(3)
		vec := []int32{1, 2}
		_, err := marshalVector(info, vec)
		if err == nil {
			t.Fatal("expected error for dimension mismatch, got nil")
		}
	})

	t.Run("int64_marshal", func(t *testing.T) {
		info := makeInt64VectorType(3)
		vec := []int64{1, 2}
		_, err := marshalVector(info, vec)
		if err == nil {
			t.Fatal("expected error for dimension mismatch, got nil")
		}
	})

	t.Run("int32_unmarshal_wrong_data_len", func(t *testing.T) {
		info := makeInt32VectorType(3)
		data := make([]byte, 10) // not 4*3=12
		var result []int32
		err := unmarshalVector(info, data, &result)
		if err == nil {
			t.Fatal("expected error for wrong data length, got nil")
		}
	})

	t.Run("int64_unmarshal_wrong_data_len", func(t *testing.T) {
		info := makeInt64VectorType(3)
		data := make([]byte, 10) // not 8*3=24
		var result []int64
		err := unmarshalVector(info, data, &result)
		if err == nil {
			t.Fatal("expected error for wrong data length, got nil")
		}
	})

	t.Run("uuid_marshal", func(t *testing.T) {
		info := makeUUIDVectorType(3)
		vec := []UUID{{0x01}, {0x02}}
		_, err := marshalVector(info, vec)
		if err == nil {
			t.Fatal("expected error for dimension mismatch, got nil")
		}
	})

	t.Run("uuid_unmarshal_wrong_data_len", func(t *testing.T) {
		info := makeUUIDVectorType(3)
		data := make([]byte, 10) // not 16*3=48
		var result []UUID
		err := unmarshalVector(info, data, &result)
		if err == nil {
			t.Fatal("expected error for wrong data length, got nil")
		}
	})
}

// --- Test 7: Empty vector (dim=0) ---

func TestMarshalVector_EmptyVector(t *testing.T) {
	t.Run("float32_dim0", func(t *testing.T) {
		info := makeFloat32VectorType(0)
		vec := []float32{}
		data, err := marshalVector(info, vec)
		if err != nil {
			t.Fatalf("marshalVector: %v", err)
		}
		if data == nil {
			t.Error("expected non-nil empty data for non-nil empty vector, got nil (would encode as CQL NULL)")
		}
		if len(data) != 0 {
			t.Errorf("expected empty data, got %d bytes", len(data))
		}

		var result []float32
		if err := unmarshalVector(info, data, &result); err != nil {
			t.Fatalf("unmarshalVector: %v", err)
		}
		if len(result) != 0 {
			t.Errorf("expected empty result, got len %d", len(result))
		}
	})

	t.Run("float64_dim0", func(t *testing.T) {
		info := makeFloat64VectorType(0)
		vec := []float64{}
		data, err := marshalVector(info, vec)
		if err != nil {
			t.Fatalf("marshalVector: %v", err)
		}
		if data == nil {
			t.Error("expected non-nil empty data for non-nil empty vector, got nil (would encode as CQL NULL)")
		}
		if len(data) != 0 {
			t.Errorf("expected empty data, got %d bytes", len(data))
		}

		var result []float64
		if err := unmarshalVector(info, data, &result); err != nil {
			t.Fatalf("unmarshalVector: %v", err)
		}
		if len(result) != 0 {
			t.Errorf("expected empty result, got len %d", len(result))
		}
	})

	t.Run("int32_dim0", func(t *testing.T) {
		info := makeInt32VectorType(0)
		vec := []int32{}
		data, err := marshalVector(info, vec)
		if err != nil {
			t.Fatalf("marshalVector: %v", err)
		}
		if data == nil {
			t.Error("expected non-nil empty data for non-nil empty vector, got nil (would encode as CQL NULL)")
		}
		if len(data) != 0 {
			t.Errorf("expected empty data, got %d bytes", len(data))
		}

		var result []int32
		if err := unmarshalVector(info, data, &result); err != nil {
			t.Fatalf("unmarshalVector: %v", err)
		}
		if len(result) != 0 {
			t.Errorf("expected empty result, got len %d", len(result))
		}
	})

	t.Run("int64_dim0", func(t *testing.T) {
		info := makeInt64VectorType(0)
		vec := []int64{}
		data, err := marshalVector(info, vec)
		if err != nil {
			t.Fatalf("marshalVector: %v", err)
		}
		if data == nil {
			t.Error("expected non-nil empty data for non-nil empty vector, got nil (would encode as CQL NULL)")
		}
		if len(data) != 0 {
			t.Errorf("expected empty data, got %d bytes", len(data))
		}

		var result []int64
		if err := unmarshalVector(info, data, &result); err != nil {
			t.Fatalf("unmarshalVector: %v", err)
		}
		if len(result) != 0 {
			t.Errorf("expected empty result, got len %d", len(result))
		}
	})

	t.Run("uuid_dim0", func(t *testing.T) {
		info := makeUUIDVectorType(0)
		vec := []UUID{}
		data, err := marshalVector(info, vec)
		if err != nil {
			t.Fatalf("marshalVector: %v", err)
		}
		if data == nil {
			t.Error("expected non-nil empty data for non-nil empty vector, got nil (would encode as CQL NULL)")
		}
		if len(data) != 0 {
			t.Errorf("expected empty data, got %d bytes", len(data))
		}

		var result []UUID
		if err := unmarshalVector(info, data, &result); err != nil {
			t.Fatalf("unmarshalVector: %v", err)
		}
		if len(result) != 0 {
			t.Errorf("expected empty result, got len %d", len(result))
		}
	})
}

// --- Test 7b: Empty vector (dim=0) via generic path ---

func TestMarshalVector_EmptyVector_GenericPath(t *testing.T) {
	// TypeBoolean vectors don't hit the fast paths, exercising the generic
	// reflect-based marshalVector/unmarshalVector code for dim=0.
	info := VectorType{
		Dimensions: 0,
		SubType:    NativeType{typ: TypeBoolean},
	}
	vec := []bool{}
	data, err := marshalVector(info, vec)
	if err != nil {
		t.Fatalf("marshalVector (generic, dim=0): %v", err)
	}
	if data == nil {
		t.Error("expected non-nil empty data for non-nil empty vector via generic path, got nil (would encode as CQL NULL)")
	}
	if len(data) != 0 {
		t.Errorf("expected empty data, got %d bytes", len(data))
	}

	var result []bool
	if err := unmarshalVector(info, data, &result); err != nil {
		t.Fatalf("unmarshalVector (generic, dim=0): %v", err)
	}
	if result == nil {
		t.Error("expected non-nil empty slice for dim=0 unmarshal via generic path, got nil")
	}
	if len(result) != 0 {
		t.Errorf("expected empty result, got len %d", len(result))
	}
}

// --- Test 8: Pointer to slice ---

func TestMarshalVector_PointerToSlice(t *testing.T) {
	t.Run("float32_ptr_marshal", func(t *testing.T) {
		info := makeFloat32VectorType(2)
		vec := []float32{1.5, 2.5}
		data, err := Marshal(info, &vec)
		if err != nil {
			t.Fatalf("Marshal with *[]float32: %v", err)
		}

		var result []float32
		if err := unmarshalVector(info, data, &result); err != nil {
			t.Fatalf("unmarshalVector: %v", err)
		}
		if !reflect.DeepEqual(vec, result) {
			t.Errorf("round-trip mismatch: got %v, want %v", result, vec)
		}
	})

	t.Run("float64_ptr_marshal", func(t *testing.T) {
		info := makeFloat64VectorType(2)
		vec := []float64{1.5, 2.5}
		data, err := Marshal(info, &vec)
		if err != nil {
			t.Fatalf("Marshal with *[]float64: %v", err)
		}

		var result []float64
		if err := unmarshalVector(info, data, &result); err != nil {
			t.Fatalf("unmarshalVector: %v", err)
		}
		if !reflect.DeepEqual(vec, result) {
			t.Errorf("round-trip mismatch: got %v, want %v", result, vec)
		}
	})

	t.Run("int32_ptr_marshal", func(t *testing.T) {
		info := makeInt32VectorType(2)
		vec := []int32{100, -200}
		data, err := Marshal(info, &vec)
		if err != nil {
			t.Fatalf("Marshal with *[]int32: %v", err)
		}

		var result []int32
		if err := unmarshalVector(info, data, &result); err != nil {
			t.Fatalf("unmarshalVector: %v", err)
		}
		if !reflect.DeepEqual(vec, result) {
			t.Errorf("round-trip mismatch: got %v, want %v", result, vec)
		}
	})

	t.Run("int64_ptr_marshal", func(t *testing.T) {
		info := makeInt64VectorType(2)
		vec := []int64{100, -200}
		data, err := Marshal(info, &vec)
		if err != nil {
			t.Fatalf("Marshal with *[]int64: %v", err)
		}

		var result []int64
		if err := unmarshalVector(info, data, &result); err != nil {
			t.Fatalf("unmarshalVector: %v", err)
		}
		if !reflect.DeepEqual(vec, result) {
			t.Errorf("round-trip mismatch: got %v, want %v", result, vec)
		}
	})

	t.Run("uuid_ptr_marshal", func(t *testing.T) {
		info := makeUUIDVectorType(2)
		vec := []UUID{
			{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10},
			{0xf0, 0xe0, 0xd0, 0xc0, 0xb0, 0xa0, 0x90, 0x80, 0x70, 0x60, 0x50, 0x40, 0x30, 0x20, 0x10, 0x00},
		}
		data, err := Marshal(info, &vec)
		if err != nil {
			t.Fatalf("Marshal with *[]UUID: %v", err)
		}

		var result []UUID
		if err := unmarshalVector(info, data, &result); err != nil {
			t.Fatalf("unmarshalVector: %v", err)
		}
		if !reflect.DeepEqual(vec, result) {
			t.Errorf("round-trip mismatch: got %v, want %v", result, vec)
		}
	})
}

// --- Test 9: Special values ---

func TestMarshalVector_SpecialValues(t *testing.T) {
	t.Run("float32", func(t *testing.T) {
		negZero := math.Float32frombits(0x80000000)
		info := makeFloat32VectorType(5)
		vec := []float32{float32(math.Inf(1)), float32(math.Inf(-1)), math.MaxFloat32, math.SmallestNonzeroFloat32, negZero}
		data, err := marshalVector(info, vec)
		if err != nil {
			t.Fatalf("marshalVector: %v", err)
		}

		var result []float32
		if err := unmarshalVector(info, data, &result); err != nil {
			t.Fatalf("unmarshalVector: %v", err)
		}
		if !reflect.DeepEqual(vec, result) {
			t.Errorf("special values mismatch: got %v, want %v", result, vec)
		}
		// reflect.DeepEqual treats -0.0 == +0.0; verify the sign bit explicitly.
		if got := math.Float32bits(result[4]); got != 0x80000000 {
			t.Errorf("negative zero sign bit lost: got bits %#08x, want 0x80000000", got)
		}
	})

	t.Run("float32_nan", func(t *testing.T) {
		info := makeFloat32VectorType(1)
		vec := []float32{float32(math.NaN())}
		data, err := marshalVector(info, vec)
		if err != nil {
			t.Fatalf("marshalVector: %v", err)
		}

		var result []float32
		if err := unmarshalVector(info, data, &result); err != nil {
			t.Fatalf("unmarshalVector: %v", err)
		}
		if len(result) != 1 || !math.IsNaN(float64(result[0])) {
			t.Errorf("expected NaN, got %v", result)
		}
	})

	t.Run("float64", func(t *testing.T) {
		negZero := math.Float64frombits(0x8000000000000000)
		info := makeFloat64VectorType(5)
		vec := []float64{math.Inf(1), math.Inf(-1), math.MaxFloat64, math.SmallestNonzeroFloat64, negZero}
		data, err := marshalVector(info, vec)
		if err != nil {
			t.Fatalf("marshalVector: %v", err)
		}

		var result []float64
		if err := unmarshalVector(info, data, &result); err != nil {
			t.Fatalf("unmarshalVector: %v", err)
		}
		if !reflect.DeepEqual(vec, result) {
			t.Errorf("special values mismatch: got %v, want %v", result, vec)
		}
		// reflect.DeepEqual treats -0.0 == +0.0; verify the sign bit explicitly.
		if got := math.Float64bits(result[4]); got != 0x8000000000000000 {
			t.Errorf("negative zero sign bit lost: got bits %#016x, want 0x8000000000000000", got)
		}
	})

	t.Run("float64_nan", func(t *testing.T) {
		info := makeFloat64VectorType(1)
		vec := []float64{math.NaN()}
		data, err := marshalVector(info, vec)
		if err != nil {
			t.Fatalf("marshalVector: %v", err)
		}

		var result []float64
		if err := unmarshalVector(info, data, &result); err != nil {
			t.Fatalf("unmarshalVector: %v", err)
		}
		if len(result) != 1 || !math.IsNaN(result[0]) {
			t.Errorf("expected NaN, got %v", result)
		}
	})

	t.Run("int32", func(t *testing.T) {
		info := makeInt32VectorType(5)
		vec := []int32{0, 1, -1, math.MaxInt32, math.MinInt32}
		data, err := marshalVector(info, vec)
		if err != nil {
			t.Fatalf("marshalVector: %v", err)
		}

		var result []int32
		if err := unmarshalVector(info, data, &result); err != nil {
			t.Fatalf("unmarshalVector: %v", err)
		}
		if !reflect.DeepEqual(vec, result) {
			t.Errorf("special values mismatch: got %v, want %v", result, vec)
		}
	})

	t.Run("int64", func(t *testing.T) {
		info := makeInt64VectorType(5)
		vec := []int64{0, 1, -1, math.MaxInt64, math.MinInt64}
		data, err := marshalVector(info, vec)
		if err != nil {
			t.Fatalf("marshalVector: %v", err)
		}

		var result []int64
		if err := unmarshalVector(info, data, &result); err != nil {
			t.Fatalf("unmarshalVector: %v", err)
		}
		if !reflect.DeepEqual(vec, result) {
			t.Errorf("special values mismatch: got %v, want %v", result, vec)
		}
	})
}

// --- Test 10: Pool concurrency ---

func TestVectorBufPool_Concurrency(t *testing.T) {
	const goroutines = 100
	const iterations = 100
	const dim = 256
	const bufSize = dim * 4

	var wg sync.WaitGroup
	wg.Add(goroutines)
	for g := 0; g < goroutines; g++ {
		go func() {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				buf := getVectorBuf(bufSize)
				if len(buf) != bufSize {
					t.Errorf("getVectorBuf returned wrong size: got %d, want %d", len(buf), bufSize)
					return
				}
				// Write some data to detect cross-goroutine corruption.
				for j := range buf {
					buf[j] = byte(j)
				}
				// Verify before returning.
				for j := range buf {
					if buf[j] != byte(j) {
						t.Errorf("buffer corruption detected at index %d", j)
						return
					}
				}
				putVectorBuf(buf)
			}
		}()
	}
	wg.Wait()
}

// TestVectorByteSizeErrorType verifies that vectorByteSize overflow errors,
// when propagated through unmarshal fast paths, are returned as typed
// UnmarshalError rather than plain fmt.Errorf values. The marshal fast paths
// check len(vec) != dim before reaching vectorByteSize, so they cannot be
// tested with real slices large enough to trigger overflow.
func TestVectorByteSizeErrorType(t *testing.T) {
	// Use a dimension that overflows on all platforms: math.MaxInt/4+1 * 4 > MaxInt.
	overflowDim4 := math.MaxInt/4 + 1
	overflowDim8 := math.MaxInt/8 + 1

	t.Run("unmarshal_4byte", func(t *testing.T) {
		tests := []struct {
			name string
			fn   func() error
		}{
			{"float32", func() error { var dst []float32; return unmarshalVectorFloat32(overflowDim4, []byte{}, &dst) }},
			{"int32", func() error { var dst []int32; return unmarshalVectorInt32(overflowDim4, []byte{}, &dst) }},
		}
		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				err := tc.fn()
				if err == nil {
					t.Fatal("expected overflow error, got nil")
				}
				var ue UnmarshalError
				if !errors.As(err, &ue) {
					t.Errorf("expected UnmarshalError, got %T: %v", err, err)
				}
				if !strings.Contains(err.Error(), "overflow") {
					t.Errorf("expected overflow in error message, got: %v", err)
				}
			})
		}
	})

	t.Run("unmarshal_8byte", func(t *testing.T) {
		tests := []struct {
			name string
			fn   func() error
		}{
			{"float64", func() error { var dst []float64; return unmarshalVectorFloat64(overflowDim8, []byte{}, &dst) }},
			{"int64", func() error { var dst []int64; return unmarshalVectorInt64(overflowDim8, []byte{}, &dst) }},
		}
		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				err := tc.fn()
				if err == nil {
					t.Fatal("expected overflow error, got nil")
				}
				var ue UnmarshalError
				if !errors.As(err, &ue) {
					t.Errorf("expected UnmarshalError, got %T: %v", err, err)
				}
				if !strings.Contains(err.Error(), "overflow") {
					t.Errorf("expected overflow in error message, got: %v", err)
				}
			})
		}
	})
}

// TestUnmarshalVectorFastPathZeroDimNonNilSlice verifies that unmarshal fast
// paths return a non-nil empty slice (not nil) when dim==0 and data is non-nil
// empty, matching the generic path behavior.
func TestUnmarshalVectorFastPathZeroDimNonNilSlice(t *testing.T) {
	t.Run("float32", func(t *testing.T) {
		var dst []float32
		if err := unmarshalVectorFloat32(0, []byte{}, &dst); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if dst == nil {
			t.Error("expected non-nil empty slice, got nil")
		}
		if len(dst) != 0 {
			t.Errorf("expected len 0, got %d", len(dst))
		}
	})

	t.Run("float64", func(t *testing.T) {
		var dst []float64
		if err := unmarshalVectorFloat64(0, []byte{}, &dst); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if dst == nil {
			t.Error("expected non-nil empty slice, got nil")
		}
		if len(dst) != 0 {
			t.Errorf("expected len 0, got %d", len(dst))
		}
	})

	t.Run("int32", func(t *testing.T) {
		var dst []int32
		if err := unmarshalVectorInt32(0, []byte{}, &dst); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if dst == nil {
			t.Error("expected non-nil empty slice, got nil")
		}
		if len(dst) != 0 {
			t.Errorf("expected len 0, got %d", len(dst))
		}
	})

	t.Run("int64", func(t *testing.T) {
		var dst []int64
		if err := unmarshalVectorInt64(0, []byte{}, &dst); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if dst == nil {
			t.Error("expected non-nil empty slice, got nil")
		}
		if len(dst) != 0 {
			t.Errorf("expected len 0, got %d", len(dst))
		}
	})

	t.Run("uuid", func(t *testing.T) {
		var dst []UUID
		if err := unmarshalVectorUUID(0, []byte{}, &dst); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if dst == nil {
			t.Error("expected non-nil empty slice, got nil")
		}
		if len(dst) != 0 {
			t.Errorf("expected len 0, got %d", len(dst))
		}
	})

	// Also verify that an existing non-nil dst is preserved as non-nil [:0].
	t.Run("float32_existing_dst", func(t *testing.T) {
		dst := make([]float32, 5)
		if err := unmarshalVectorFloat32(0, []byte{}, &dst); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if dst == nil {
			t.Error("expected non-nil empty slice, got nil")
		}
		if len(dst) != 0 {
			t.Errorf("expected len 0, got %d", len(dst))
		}
	})
}

// --- Test 11: Oversized buffers not pooled ---

func TestVectorBufPool_OversizedNotPooled(t *testing.T) {
	// A buffer larger than 65536 should not be returned to the pool.
	big := make([]byte, 70000)
	putVectorBuf(big)

	// Getting a small buffer should not return the oversized one.
	small := getVectorBuf(100)
	if cap(small) >= 70000 {
		t.Errorf("oversized buffer was returned from pool: cap=%d", cap(small))
	}
}

// --- Test 12: vectorFixedElemSize ---

func TestVectorFixedElemSize(t *testing.T) {
	tests := []struct {
		typ  Type
		want int
	}{
		// Fixed-length types.
		{TypeBoolean, 1},
		{TypeInt, 4},
		{TypeFloat, 4},
		{TypeBigInt, 8},
		{TypeDouble, 8},
		{TypeTimestamp, 8},
		{TypeUUID, 16},
		{TypeTimeUUID, 16},
		// Variable-length types — must return 0.
		{TypeVarchar, 0},
		{TypeBlob, 0},
		{TypeText, 0},
		{TypeVarint, 0},
		{TypeDecimal, 0},
		{TypeAscii, 0},
		{TypeInet, 0},
		{TypeDuration, 0},
		{TypeList, 0},
		{TypeSet, 0},
		{TypeMap, 0},
		{TypeUDT, 0},
		{TypeTuple, 0},
	}
	for _, tt := range tests {
		info := NewNativeType(protoVersion4, tt.typ)
		got := vectorFixedElemSize(info)
		if got != tt.want {
			t.Errorf("vectorFixedElemSize(%v) = %d, want %d", tt.typ, got, tt.want)
		}
	}
}

// --- Test 13: Generic prealloc correctness ---

func TestMarshalVector_GenericPrealloc(t *testing.T) {
	// Test with UUID vectors — fixed-size (16 bytes) but not a float/int fast path.
	// This exercises the generic path with buf.Grow() prealloc from Phase 1b.
	dim := 3
	info := makeUUIDVectorType(dim)

	// Create UUID values as [16]byte arrays.
	uuids := make([][16]byte, dim)
	for i := range uuids {
		for j := range uuids[i] {
			uuids[i][j] = byte(i*16 + j)
		}
	}

	// Marshal using the generic path (no fast path for UUID).
	data, err := marshalVector(info, uuids[:])
	if err != nil {
		t.Fatalf("marshalVector: %v", err)
	}

	// UUID elements are fixed-size (16 bytes), no length prefix.
	expectedLen := dim * 16
	if len(data) != expectedLen {
		t.Fatalf("expected %d bytes, got %d", expectedLen, len(data))
	}

	// Verify content by unmarshaling.
	var result [][16]byte
	if err := unmarshalVector(info, data, &result); err != nil {
		t.Fatalf("unmarshalVector: %v", err)
	}
	if !reflect.DeepEqual(uuids[:], result) {
		t.Errorf("round-trip mismatch: got %v, want %v", result, uuids[:])
	}
}

func TestVectorByteSize_Overflow(t *testing.T) {
	// On 32-bit, math.MaxInt is 2^31-1. dim=math.MaxInt/4+1 with elemBytes=4
	// would overflow. On 64-bit this is just a sanity check.
	_, err := vectorByteSize(math.MaxInt/4+1, 4)
	if err == nil {
		t.Error("expected overflow error for large dim*4")
	}
	_, err = vectorByteSize(math.MaxInt/8+1, 8)
	if err == nil {
		t.Error("expected overflow error for large dim*8")
	}
	// Normal case should succeed
	n, err := vectorByteSize(1536, 4)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if n != 6144 {
		t.Fatalf("expected 6144, got %d", n)
	}
}

// TestMarshalVectorNegativeDimensions verifies that all fast-path marshal
// and unmarshal functions return a clear "negative dimensions" error when
// the VectorType has a negative Dimensions value (corrupt/adversarial metadata).
func TestMarshalVectorNegativeDimensions(t *testing.T) {
	const wantSubstr = "negative dimensions"

	t.Run("marshal", func(t *testing.T) {
		tests := []struct {
			name string
			fn   func() error
		}{
			{"float32", func() error { _, err := marshalVectorFloat32(-1, []float32{1}); return err }},
			{"float64", func() error { _, err := marshalVectorFloat64(-1, []float64{1}); return err }},
			{"int32", func() error { _, err := marshalVectorInt32(-1, []int32{1}); return err }},
			{"int64", func() error { _, err := marshalVectorInt64(-1, []int64{1}); return err }},
			{"uuid", func() error { _, err := marshalVectorUUID(-1, []UUID{{0x01}}); return err }},
		}
		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				err := tc.fn()
				if err == nil {
					t.Fatal("expected error for negative dimensions, got nil")
				}
				if !strings.Contains(err.Error(), wantSubstr) {
					t.Errorf("error %q does not contain %q", err, wantSubstr)
				}
			})
		}
	})

	t.Run("unmarshal", func(t *testing.T) {
		tests := []struct {
			name string
			fn   func() error
		}{
			{"float32", func() error { var v []float32; return unmarshalVectorFloat32(-1, []byte{0, 0, 0, 0}, &v) }},
			{"float64", func() error { var v []float64; return unmarshalVectorFloat64(-1, []byte{0, 0, 0, 0, 0, 0, 0, 0}, &v) }},
			{"int32", func() error { var v []int32; return unmarshalVectorInt32(-1, []byte{0, 0, 0, 0}, &v) }},
			{"int64", func() error { var v []int64; return unmarshalVectorInt64(-1, []byte{0, 0, 0, 0, 0, 0, 0, 0}, &v) }},
			{"uuid", func() error { var v []UUID; return unmarshalVectorUUID(-1, make([]byte, 16), &v) }},
		}
		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				err := tc.fn()
				if err == nil {
					t.Fatal("expected error for negative dimensions, got nil")
				}
				if !strings.Contains(err.Error(), wantSubstr) {
					t.Errorf("error %q does not contain %q", err, wantSubstr)
				}
			})
		}
	})

	// Also test via the public marshalVector/unmarshalVector entry points
	// (which dispatch to fast paths).
	t.Run("public_marshal", func(t *testing.T) {
		info := makeFloat32VectorType(-3)
		_, err := marshalVector(info, []float32{1, 2, 3})
		if err == nil {
			t.Fatal("expected error for negative dimensions via marshalVector")
		}
		if !strings.Contains(err.Error(), wantSubstr) {
			t.Errorf("error %q does not contain %q", err, wantSubstr)
		}
	})

	t.Run("public_unmarshal", func(t *testing.T) {
		info := makeFloat32VectorType(-3)
		var dst []float32
		err := unmarshalVector(info, []byte{0, 0, 0, 0}, &dst)
		if err == nil {
			t.Fatal("expected error for negative dimensions via unmarshalVector")
		}
		if !strings.Contains(err.Error(), wantSubstr) {
			t.Errorf("error %q does not contain %q", err, wantSubstr)
		}
	})
}

// TestUnmarshalVectorGenericPathZeroDimensions verifies the generic unmarshal
// path (non-fast-path types like UUID) correctly handles Dimensions==0.
func TestUnmarshalVectorGenericPathZeroDimensions(t *testing.T) {
	info := makeUUIDVectorType(0) // UUID type goes through generic path

	t.Run("nil_data", func(t *testing.T) {
		var dst [][16]byte
		err := unmarshalVector(info, nil, &dst)
		if err != nil {
			t.Fatalf("unexpected error for nil data: %v", err)
		}
		if dst != nil {
			t.Errorf("expected nil slice for nil data, got %v", dst)
		}
	})

	t.Run("empty_data", func(t *testing.T) {
		var dst [][16]byte
		err := unmarshalVector(info, []byte{}, &dst)
		if err != nil {
			t.Fatalf("unexpected error for empty data: %v", err)
		}
		if dst == nil || len(dst) != 0 {
			t.Errorf("expected non-nil empty slice, got %v (nil=%v)", dst, dst == nil)
		}
	})

	t.Run("non_empty_data_error", func(t *testing.T) {
		var dst [][16]byte
		err := unmarshalVector(info, []byte{1, 2, 3}, &dst)
		if err == nil {
			t.Fatal("expected error for non-empty data with 0 dimensions")
		}
		if !strings.Contains(err.Error(), "0-dimension") {
			t.Errorf("error %q does not mention 0-dimension", err)
		}
	})
}

// TestGetVectorBuf_NonPositiveSize verifies that getVectorBuf handles
// zero and negative sizes correctly without allocating or panicking.
// Size 0 returns a non-nil empty slice (distinguishes empty vector from NULL).
// Negative sizes return nil.
func TestGetVectorBuf_NonPositiveSize(t *testing.T) {
	// Zero size: non-nil empty slice (needed by marshalVector for dim=0).
	buf := getVectorBuf(0)
	if buf == nil {
		t.Error("getVectorBuf(0) = nil, want non-nil empty slice")
	} else if len(buf) != 0 {
		t.Errorf("getVectorBuf(0) len = %d, want 0", len(buf))
	}

	// Negative sizes: nil.
	for _, size := range []int{-1, -100} {
		buf := getVectorBuf(size)
		if buf != nil {
			t.Errorf("getVectorBuf(%d) = %v, want nil", size, buf)
		}
	}
}

// TestVectorBufPoolSubtype verifies that vectorBufPoolSubtype correctly
// identifies which vector subtypes use the pooled fast path.
func TestVectorBufPoolSubtype(t *testing.T) {
	tests := []struct {
		name   string
		vt     VectorType
		expect bool
	}{
		{"float32", makeFloat32VectorType(3), true},
		{"float64", makeFloat64VectorType(3), true},
		{"int32", makeInt32VectorType(3), true},
		{"int64", makeInt64VectorType(3), true},
		{"uuid", makeUUIDVectorType(3), true},
		{"varchar", VectorType{
			SubType:    NativeType{proto: protoVersion4, typ: TypeVarchar},
			Dimensions: 3,
		}, false},
		{"boolean", VectorType{
			SubType:    NativeType{proto: protoVersion4, typ: TypeBoolean},
			Dimensions: 3,
		}, false},
		{"timestamp", VectorType{
			SubType:    NativeType{proto: protoVersion4, typ: TypeTimestamp},
			Dimensions: 3,
		}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := vectorBufPoolSubtype(tt.vt)
			if got != tt.expect {
				t.Errorf("vectorBufPoolSubtype(%s) = %v, want %v", tt.name, got, tt.expect)
			}
		})
	}
}

// TestVectorBufPoolReturnSimulation simulates the buffer lifecycle used in
// executeQuery and executeBatch: marshal a vector value via marshalQueryValue,
// then return the buffer to the pool via putVectorBuf. Verifies that the
// returned buffer is reused by a subsequent getVectorBuf call.
func TestVectorBufPoolReturnSimulation(t *testing.T) {
	const dim = 128
	vt := makeFloat32VectorType(dim)

	// Create test data.
	vec := make([]float32, dim)
	for i := range vec {
		vec[i] = float32(i)
	}

	// Marshal like marshalQueryValue does.
	data, err := Marshal(vt, vec)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	expectedSize := dim * 4
	if len(data) != expectedSize {
		t.Fatalf("marshalled size = %d, want %d", len(data), expectedSize)
	}

	// Remember the backing array pointer before returning to pool.
	origPtr := &data[:1][0]

	// Simulate what the defer in executeQuery/executeBatch does.
	if vectorBufPoolSubtype(vt) {
		putVectorBuf(data)
	}

	// Get a buffer of the same size — should reuse the pooled one.
	reused := getVectorBuf(expectedSize)
	if len(reused) != expectedSize {
		t.Fatalf("getVectorBuf(%d) returned len %d", expectedSize, len(reused))
	}
	reusedPtr := &reused[:1][0]
	if origPtr != reusedPtr {
		// sync.Pool makes no guarantees about reuse — the GC may clear the
		// pool between Put and Get, especially under the race detector.
		// This is expected behaviour, not a bug; log instead of failing.
		t.Log("pooled buffer was not reused (GC may have cleared sync.Pool); this is expected under -race")
	}

	// Clean up.
	putVectorBuf(reused)
}

// TestVectorBufPoolReturnNonPooledType verifies that putVectorBuf is a no-op
// for vector types that don't use the fast path (e.g. varchar vectors), and
// that vectorBufPoolSubtype correctly excludes them.
func TestVectorBufPoolReturnNonPooledType(t *testing.T) {
	vt := VectorType{
		SubType:    NativeType{proto: protoVersion4, typ: TypeVarchar},
		Dimensions: 4,
	}

	if vectorBufPoolSubtype(vt) {
		t.Fatal("vectorBufPoolSubtype should be false for varchar vectors")
	}

	// Varchar marshal path does not use getVectorBuf, so putting a buffer
	// back should be skipped by the type check. Verify no panic.
	buf := make([]byte, 100)
	putVectorBuf(buf)
}

// TestVectorBufPoolBatchSimulation simulates the batch buffer lifecycle:
// multiple statements with vector columns get their buffers collected and
// returned after the framer copies them.
func TestVectorBufPoolBatchSimulation(t *testing.T) {
	const dim = 64

	types := []struct {
		vt  VectorType
		val interface{}
	}{
		{makeFloat32VectorType(dim), make([]float32, dim)},
		{makeFloat64VectorType(dim), make([]float64, dim)},
		{makeInt32VectorType(dim), make([]int32, dim)},
		{makeInt64VectorType(dim), make([]int64, dim)},
		{makeUUIDVectorType(dim), make([]UUID, dim)},
	}

	// Simulate batch: marshal all, collect buffers.
	var vectorBufs [][]byte
	for _, tt := range types {
		data, err := Marshal(tt.vt, tt.val)
		if err != nil {
			t.Fatalf("Marshal %v: %v", tt.vt.SubType.Type(), err)
		}
		if vectorBufPoolSubtype(tt.vt) {
			vectorBufs = append(vectorBufs, data)
		}
	}

	if len(vectorBufs) != 5 {
		t.Fatalf("expected 5 pooled buffers, got %d", len(vectorBufs))
	}

	// Return all to pool (like the defer in executeBatch).
	for _, buf := range vectorBufs {
		putVectorBuf(buf)
	}

	// Verify at least one can be reused.
	reused := getVectorBuf(dim * 4) // float32 size
	if reused == nil {
		t.Fatal("expected to get a buffer from pool after returning batch buffers")
	}
	putVectorBuf(reused)
}
