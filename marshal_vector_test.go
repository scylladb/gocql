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
	"math/bits"
	"reflect"
	"strconv"
	"strings"
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
		// A 0-dimension vector must marshal to CQL null (nil), matching the
		// generic reflect path; the float fast path must not produce a non-nil
		// zero-length slice here.
		if data != nil {
			t.Errorf("expected nil data for 0-dim vector, got %d bytes (% x)", len(data), data)
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
		if data != nil {
			t.Errorf("expected nil data for 0-dim vector, got %d bytes (% x)", len(data), data)
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
		// Compare bit patterns: reflect.DeepEqual uses == for floats, where
		// -0.0 == 0.0, so it would not catch a flipped sign bit on negative
		// zero. Float64bits makes the negZero element actually verifiable.
		if len(result) != len(vec) {
			t.Fatalf("special values length mismatch: got %d, want %d", len(result), len(vec))
		}
		for i := range vec {
			if math.Float64bits(result[i]) != math.Float64bits(vec[i]) {
				t.Errorf("special values mismatch at %d: got %x, want %x",
					i, math.Float64bits(result[i]), math.Float64bits(vec[i]))
			}
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
		// Bitwise compare so the negative-zero element is actually verified
		// (reflect.DeepEqual treats -0.0 and 0.0 as equal for floats).
		if len(result) != len(vec) {
			t.Fatalf("special values length mismatch: got %d, want %d", len(result), len(vec))
		}
		for i := range vec {
			if math.Float32bits(result[i]) != math.Float32bits(vec[i]) {
				t.Errorf("special values mismatch at %d: got %x, want %x",
					i, math.Float32bits(result[i]), math.Float32bits(vec[i]))
			}
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

// makeVectorType creates a VectorType with the given element type and dimension.
func makeVectorType(elem Type, elemName string, dim int) VectorType {
	return VectorType{
		NativeType: NativeType{
			proto:  protoVersion4,
			typ:    TypeCustom,
			custom: apacheCassandraTypePrefix + "VectorType(" + apacheCassandraTypePrefix + elemName + ", " + strconv.Itoa(dim) + ")",
		},
		SubType:    NativeType{proto: protoVersion4, typ: elem},
		Dimensions: dim,
	}
}

// TestUnmarshalVectorRejectsUnfittableDimensions covers the dimension count as
// what it is on this path: the peer's number, arriving in the column type and
// turned into an allocation before the payload is looked at. A negative one
// panicked in reflect.MakeSlice, and a large enough positive one either panicked
// in makeslice or exhausted the heap outright -- both on the goroutine that
// called Scan, where nothing recovers.
//
// The two overflow cases are the reason the bound sits ahead of the float fast
// paths rather than beside MakeSlice: those paths compare len(data) against
// Dimensions*8 and Dimensions*4, and a count of 1<<61+1 makes the first product
// wrap to exactly 8. They are written in terms of the word size because the wrap
// has to land on the payload length on a 32-bit target too.
func TestUnmarshalVectorRejectsUnfittableDimensions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		info VectorType
		data []byte
		dst  func() any
		want string
	}{
		{
			name: "negative dimensions",
			info: makeFloat32VectorType(-3),
			data: make([]byte, 4),
			dst:  func() any { return new([]float32) },
			want: "negative dimensions",
		},
		{
			name: "negative dimensions into an interface",
			info: makeFloat32VectorType(-3),
			data: make([]byte, 4),
			dst:  func() any { return new(any) },
			want: "negative dimensions",
		},
		{
			// A null value cannot excuse the type: whatever arrived, a vector of
			// -3 elements is not a column this driver can describe.
			name: "negative dimensions with a null value",
			info: makeFloat32VectorType(-3),
			data: nil,
			dst:  func() any { return new([]float32) },
			want: "negative dimensions",
		},
		{
			name: "dimensions that wrap the float64 length check",
			info: makeDoubleVectorType(1<<(bits.UintSize-3) + 1),
			data: make([]byte, 8),
			dst:  func() any { return new([]float64) },
			want: "do not fit in 8 bytes",
		},
		{
			name: "dimensions that wrap the float32 length check",
			info: makeFloat32VectorType(1<<(bits.UintSize-2) + 1),
			data: make([]byte, 4),
			dst:  func() any { return new([]float32) },
			want: "do not fit in 4 bytes",
		},
		{
			// Rejected on the count, so nothing is ever handed to the allocator.
			name: "more dimensions than the address space",
			info: makeVectorType(TypeInt, "Int32Type", 1<<(bits.UintSize-2)),
			data: make([]byte, 4),
			dst:  func() any { return new([]int32) },
			want: "do not fit in 4 bytes",
		},
		{
			name: "short payload into a slice",
			info: makeVectorType(TypeInt, "Int32Type", 3),
			data: []byte{0x01, 0x02},
			dst:  func() any { return new([]int32) },
			want: "do not fit in 2 bytes",
		},
		{
			// The array destination allocates nothing, but the element size the
			// loop derives is len(data)/Dimensions, so a short payload used to
			// decode as zero values rather than as an error.
			name: "short payload into an array",
			info: makeVectorType(TypeInt, "Int32Type", 3),
			data: []byte{0x01, 0x02},
			dst:  func() any { return new([3]int32) },
			want: "do not fit in 2 bytes",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			err := unmarshalVector(test.info, test.data, test.dst())
			if err == nil {
				t.Fatalf("unmarshalVector(dim=%d, %d bytes) returned no error", test.info.Dimensions, len(test.data))
			}
			if !strings.Contains(err.Error(), test.want) {
				t.Errorf("unmarshalVector(dim=%d) error = %q, want it to contain %q", test.info.Dimensions, err, test.want)
			}
		})
	}
}

// TestUnmarshalVectorAcceptsWellFormedPayloads pins the bound as non-narrowing.
// A vector of N elements is never encoded in fewer than N bytes -- a fixed-size
// element is at least one byte wide, and a variable-length one carries a vint
// length prefix that is itself a byte -- so every one of these sits on or above
// the boundary.
func TestUnmarshalVectorAcceptsWellFormedPayloads(t *testing.T) {
	t.Parallel()

	t.Run("float vector at the exact size", func(t *testing.T) {
		t.Parallel()

		var dst []float32
		if err := unmarshalVector(makeFloat32VectorType(3), make([]byte, 12), &dst); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(dst) != 3 {
			t.Errorf("len = %d, want 3", len(dst))
		}
	})

	t.Run("text vector of empty elements", func(t *testing.T) {
		t.Parallel()

		// Three elements, each a vint length of zero: the smallest payload a
		// three-dimension vector can have.
		var dst []string
		if err := unmarshalVector(makeVectorType(TypeText, "UTF8Type", 3), []byte{0x00, 0x00, 0x00}, &dst); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !reflect.DeepEqual(dst, []string{"", "", ""}) {
			t.Errorf("dst = %q, want three empty strings", dst)
		}
	})

	t.Run("payload with a trailing byte", func(t *testing.T) {
		t.Parallel()

		var dst []int32
		if err := unmarshalVector(makeVectorType(TypeInt, "Int32Type", 3), make([]byte, 13), &dst); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(dst) != 3 {
			t.Errorf("len = %d, want 3", len(dst))
		}
	})

	t.Run("null value", func(t *testing.T) {
		t.Parallel()

		var dst []float32
		if err := unmarshalVector(makeFloat32VectorType(3), nil, &dst); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if dst != nil {
			t.Errorf("dst = %v, want nil", dst)
		}
	})
}
