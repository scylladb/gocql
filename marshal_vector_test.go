//go:build unit
// +build unit

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package gocql

import (
	"encoding/binary"
	"math"
	"reflect"
	"strconv"
	"testing"
)

// makeDoubleVectorType creates a VectorType for vector<double> with the given dimension.
func makeDoubleVectorType(dim int) VectorType {
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

// makeFloat32VectorType creates a VectorType for vector<float> with the given dimension.
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

func TestMarshalVectorFloat64_RoundTrip(t *testing.T) {
	dim := 5
	info := makeDoubleVectorType(dim)
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
}

func TestMarshalVectorFloat32_RoundTrip(t *testing.T) {
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
}

// TestVectorFloat_ByteCompatibility verifies that the fast path produces
// identical bytes to what the generic reflect-based path would produce.
func TestVectorFloat_ByteCompatibility(t *testing.T) {
	t.Run("float64", func(t *testing.T) {
		dim := 3
		vec := []float64{-1.5, 0, 42.125}
		// Build expected bytes manually using the same encoding the generic path uses.
		expected := make([]byte, dim*8)
		for i, v := range vec {
			binary.BigEndian.PutUint64(expected[i*8:], math.Float64bits(v))
		}

		info := makeDoubleVectorType(dim)
		data, err := marshalVector(info, vec)
		if err != nil {
			t.Fatalf("marshalVector: %v", err)
		}
		if !reflect.DeepEqual(data, expected) {
			t.Errorf("byte mismatch:\n  got:  %x\n  want: %x", data, expected)
		}
	})
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
}

func TestVectorFloat_SliceReuse(t *testing.T) {
	t.Run("float64", func(t *testing.T) {
		dim := 4
		info := makeDoubleVectorType(dim)
		data := make([]byte, dim*8)
		for i := 0; i < dim; i++ {
			binary.BigEndian.PutUint64(data[i*8:], math.Float64bits(float64(i)))
		}

		// First unmarshal allocates.
		var result []float64
		if err := unmarshalVector(info, data, &result); err != nil {
			t.Fatalf("unmarshalVector (first): %v", err)
		}
		if len(result) != dim {
			t.Fatalf("expected len %d, got %d", dim, len(result))
		}

		// Save the underlying array pointer.
		ptr := &result[0]

		// Second unmarshal should reuse the same backing array.
		if err := unmarshalVector(info, data, &result); err != nil {
			t.Fatalf("unmarshalVector (second): %v", err)
		}
		if &result[0] != ptr {
			t.Error("expected slice reuse, but a new backing array was allocated")
		}
	})
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
		ptr := &result[0]

		if err := unmarshalVector(info, data, &result); err != nil {
			t.Fatalf("unmarshalVector (second): %v", err)
		}
		if &result[0] != ptr {
			t.Error("expected slice reuse, but a new backing array was allocated")
		}
	})
	t.Run("float64_excess_cap", func(t *testing.T) {
		dim := 4
		info := makeDoubleVectorType(dim)
		data := make([]byte, dim*8)
		for i := 0; i < dim; i++ {
			binary.BigEndian.PutUint64(data[i*8:], math.Float64bits(float64(i)+0.5))
		}

		// Pre-allocate with excess capacity.
		result := make([]float64, 0, dim+10)
		ptr := &result[:1][0] // get pointer to backing array
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
	t.Run("float32_excess_cap", func(t *testing.T) {
		dim := 4
		info := makeFloat32VectorType(dim)
		data := make([]byte, dim*4)
		for i := 0; i < dim; i++ {
			binary.BigEndian.PutUint32(data[i*4:], math.Float32bits(float32(i)+0.5))
		}

		result := make([]float32, 0, dim+10)
		ptr := &result[:1][0]
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

func TestVectorFloat_NilData(t *testing.T) {
	t.Run("float64_nil_data_nil_dst", func(t *testing.T) {
		info := makeDoubleVectorType(3)
		var result []float64
		if err := unmarshalVector(info, nil, &result); err != nil {
			t.Fatalf("unmarshalVector: %v", err)
		}
		if result != nil {
			t.Errorf("expected nil result, got %v", result)
		}
	})
	t.Run("float64_nil_data_non_nil_dst", func(t *testing.T) {
		info := makeDoubleVectorType(3)
		result := []float64{1, 2, 3}
		if err := unmarshalVector(info, nil, &result); err != nil {
			t.Fatalf("unmarshalVector: %v", err)
		}
		if result != nil {
			t.Errorf("expected nil result after nil data, got %v", result)
		}
	})
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
}

func TestVectorFloat_NilSliceMarshal(t *testing.T) {
	t.Run("float64_nil_slice", func(t *testing.T) {
		info := makeDoubleVectorType(3)
		var vec []float64
		data, err := marshalVector(info, vec)
		if err != nil {
			t.Fatalf("marshalVector: %v", err)
		}
		if data != nil {
			t.Errorf("expected nil data for nil slice, got %v", data)
		}
	})
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
	t.Run("float64_nil_ptr", func(t *testing.T) {
		info := makeDoubleVectorType(3)
		var ptr *[]float64
		// Nil pointer handling is Marshal()'s responsibility, not marshalVector's.
		data, err := Marshal(info, ptr)
		if err != nil {
			t.Fatalf("Marshal: %v", err)
		}
		if data != nil {
			t.Errorf("expected nil data for nil ptr, got %v", data)
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
	t.Run("float64_non_nil_ptr_nil_slice", func(t *testing.T) {
		info := makeDoubleVectorType(3)
		var s []float64 // nil slice
		// Non-nil pointer to nil slice — Marshal() dereferences, marshalVector sees nil slice.
		data, err := Marshal(info, &s)
		if err != nil {
			t.Fatalf("Marshal: %v", err)
		}
		if data != nil {
			t.Errorf("expected nil data for non-nil ptr to nil slice, got %v", data)
		}
	})
	t.Run("float32_non_nil_ptr_nil_slice", func(t *testing.T) {
		info := makeFloat32VectorType(3)
		var s []float32 // nil slice
		data, err := Marshal(info, &s)
		if err != nil {
			t.Fatalf("Marshal: %v", err)
		}
		if data != nil {
			t.Errorf("expected nil data for non-nil ptr to nil slice, got %v", data)
		}
	})
}

func TestVectorFloat_DimensionMismatch(t *testing.T) {
	t.Run("float64_marshal", func(t *testing.T) {
		info := makeDoubleVectorType(3)
		vec := []float64{1, 2} // wrong dimension
		_, err := marshalVector(info, vec)
		if err == nil {
			t.Fatal("expected error for dimension mismatch, got nil")
		}
	})
	t.Run("float32_marshal", func(t *testing.T) {
		info := makeFloat32VectorType(3)
		vec := []float32{1, 2} // wrong dimension
		_, err := marshalVector(info, vec)
		if err == nil {
			t.Fatal("expected error for dimension mismatch, got nil")
		}
	})
	t.Run("float64_unmarshal_wrong_data_len", func(t *testing.T) {
		info := makeDoubleVectorType(3)
		data := make([]byte, 10) // not divisible by 8*3=24
		var result []float64
		err := unmarshalVector(info, data, &result)
		if err == nil {
			t.Fatal("expected error for wrong data length, got nil")
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
}

func TestVectorFloat_EmptyVector(t *testing.T) {
	t.Run("float64_dim0", func(t *testing.T) {
		info := makeDoubleVectorType(0)
		vec := []float64{}
		data, err := marshalVector(info, vec)
		if err != nil {
			t.Fatalf("marshalVector: %v", err)
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
	t.Run("float32_dim0", func(t *testing.T) {
		info := makeFloat32VectorType(0)
		vec := []float32{}
		data, err := marshalVector(info, vec)
		if err != nil {
			t.Fatalf("marshalVector: %v", err)
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
}

func TestVectorFloat_PointerToSlice(t *testing.T) {
	t.Run("float64_ptr_marshal", func(t *testing.T) {
		info := makeDoubleVectorType(2)
		vec := []float64{1.5, 2.5}
		// Marshal() dereferences the pointer, then marshalVector hits the []float64 fast path.
		data, err := Marshal(info, &vec)
		if err != nil {
			t.Fatalf("Marshal with *[]float64: %v", err)
		}

		// Verify the data is correct by unmarshaling.
		var result []float64
		if err := unmarshalVector(info, data, &result); err != nil {
			t.Fatalf("unmarshalVector: %v", err)
		}
		if !reflect.DeepEqual(vec, result) {
			t.Errorf("round-trip mismatch: got %v, want %v", result, vec)
		}
	})
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
}

func TestVectorFloat_SpecialValues(t *testing.T) {
	t.Run("float64", func(t *testing.T) {
		negZero := math.Float64frombits(0x8000000000000000) // -0.0
		info := makeDoubleVectorType(5)
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
	})
	t.Run("float64_nan", func(t *testing.T) {
		info := makeDoubleVectorType(1)
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
	t.Run("float32", func(t *testing.T) {
		negZero := math.Float32frombits(0x80000000) // -0.0
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
}
