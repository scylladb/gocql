//go:build all || unit
// +build all unit

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
	"testing"
)

// --- Helper to create list CollectionType ---

func listTypeInfo(elemType Type) CollectionType {
	return CollectionType{
		NativeType: NativeType{proto: protoVersion4, typ: TypeList},
		Elem:       NativeType{proto: protoVersion4, typ: elemType},
	}
}

func setTypeInfo(elemType Type) CollectionType {
	return CollectionType{
		NativeType: NativeType{proto: protoVersion4, typ: TypeSet},
		Elem:       NativeType{proto: protoVersion4, typ: elemType},
	}
}

// --- Round-trip correctness tests ---

func TestMarshalListFastPath_Float32_RoundTrip(t *testing.T) {
	info := listTypeInfo(TypeFloat)
	input := []float32{1.5, -2.5, 3.14, 0, math.MaxFloat32, math.SmallestNonzeroFloat32}

	data, err := marshalList(info, input)
	if err != nil {
		t.Fatal(err)
	}

	var output []float32
	if err := unmarshalList(info, data, &output); err != nil {
		t.Fatal(err)
	}

	if len(output) != len(input) {
		t.Fatalf("length mismatch: got %d, want %d", len(output), len(input))
	}
	for i := range input {
		if output[i] != input[i] {
			t.Errorf("index %d: got %v, want %v", i, output[i], input[i])
		}
	}
}

func TestMarshalListFastPath_Float64_RoundTrip(t *testing.T) {
	info := listTypeInfo(TypeDouble)
	input := []float64{1.5, -2.5, math.Pi, 0, math.MaxFloat64, math.SmallestNonzeroFloat64}

	data, err := marshalList(info, input)
	if err != nil {
		t.Fatal(err)
	}

	var output []float64
	if err := unmarshalList(info, data, &output); err != nil {
		t.Fatal(err)
	}

	if len(output) != len(input) {
		t.Fatalf("length mismatch: got %d, want %d", len(output), len(input))
	}
	for i := range input {
		if output[i] != input[i] {
			t.Errorf("index %d: got %v, want %v", i, output[i], input[i])
		}
	}
}

func TestMarshalListFastPath_Int32_RoundTrip(t *testing.T) {
	info := listTypeInfo(TypeInt)
	input := []int32{0, 1, -1, math.MaxInt32, math.MinInt32, 42}

	data, err := marshalList(info, input)
	if err != nil {
		t.Fatal(err)
	}

	var output []int32
	if err := unmarshalList(info, data, &output); err != nil {
		t.Fatal(err)
	}

	if len(output) != len(input) {
		t.Fatalf("length mismatch: got %d, want %d", len(output), len(input))
	}
	for i := range input {
		if output[i] != input[i] {
			t.Errorf("index %d: got %v, want %v", i, output[i], input[i])
		}
	}
}

func TestMarshalListFastPath_Int64_RoundTrip(t *testing.T) {
	info := listTypeInfo(TypeBigInt)
	input := []int64{0, 1, -1, math.MaxInt64, math.MinInt64, 42}

	data, err := marshalList(info, input)
	if err != nil {
		t.Fatal(err)
	}

	var output []int64
	if err := unmarshalList(info, data, &output); err != nil {
		t.Fatal(err)
	}

	if len(output) != len(input) {
		t.Fatalf("length mismatch: got %d, want %d", len(output), len(input))
	}
	for i := range input {
		if output[i] != input[i] {
			t.Errorf("index %d: got %v, want %v", i, output[i], input[i])
		}
	}
}

func TestMarshalListFastPath_Int64Timestamp_RoundTrip(t *testing.T) {
	info := listTypeInfo(TypeTimestamp)
	input := []int64{0, 1714000000000, -1, math.MaxInt64}

	data, err := marshalList(info, input)
	if err != nil {
		t.Fatal(err)
	}

	var output []int64
	if err := unmarshalList(info, data, &output); err != nil {
		t.Fatal(err)
	}

	if len(output) != len(input) {
		t.Fatalf("length mismatch: got %d, want %d", len(output), len(input))
	}
	for i := range input {
		if output[i] != input[i] {
			t.Errorf("index %d: got %v, want %v", i, output[i], input[i])
		}
	}
}

// --- Empty list tests ---

func TestMarshalListFastPath_Float32_Empty(t *testing.T) {
	info := listTypeInfo(TypeFloat)
	input := []float32{}

	data, err := marshalList(info, input)
	if err != nil {
		t.Fatal(err)
	}

	var output []float32
	if err := unmarshalList(info, data, &output); err != nil {
		t.Fatal(err)
	}

	if len(output) != 0 {
		t.Fatalf("expected empty slice, got length %d", len(output))
	}
}

func TestMarshalListFastPath_Int32_Empty(t *testing.T) {
	info := listTypeInfo(TypeInt)
	input := []int32{}

	data, err := marshalList(info, input)
	if err != nil {
		t.Fatal(err)
	}

	var output []int32
	if err := unmarshalList(info, data, &output); err != nil {
		t.Fatal(err)
	}

	if len(output) != 0 {
		t.Fatalf("expected empty slice, got length %d", len(output))
	}
}

// --- Nil list tests ---

func TestMarshalListFastPath_Float32_Nil(t *testing.T) {
	info := listTypeInfo(TypeFloat)
	var input []float32 // nil

	data, err := marshalList(info, input)
	if err != nil {
		t.Fatal(err)
	}
	if data != nil {
		t.Fatalf("expected nil data for nil input, got %v", data)
	}
}

func TestMarshalListFastPath_Int32_Nil(t *testing.T) {
	info := listTypeInfo(TypeInt)
	var input []int32 // nil

	data, err := marshalList(info, input)
	if err != nil {
		t.Fatal(err)
	}
	if data != nil {
		t.Fatalf("expected nil data for nil input, got %v", data)
	}
}

func TestUnmarshalListFastPath_Float32_NilData(t *testing.T) {
	info := listTypeInfo(TypeFloat)
	var output []float32

	if err := unmarshalList(info, nil, &output); err != nil {
		t.Fatal(err)
	}
	if output != nil {
		t.Fatalf("expected nil output, got %v", output)
	}
}

func TestUnmarshalListFastPath_Int32_NilData(t *testing.T) {
	info := listTypeInfo(TypeInt)
	var output []int32

	if err := unmarshalList(info, nil, &output); err != nil {
		t.Fatal(err)
	}
	if output != nil {
		t.Fatalf("expected nil output, got %v", output)
	}
}

// --- Slice reuse tests (unmarshal into existing capacity) ---

func TestUnmarshalListFastPath_Float32_SliceReuse(t *testing.T) {
	info := listTypeInfo(TypeFloat)
	input := []float32{1.0, 2.0, 3.0}

	data, err := marshalList(info, input)
	if err != nil {
		t.Fatal(err)
	}

	// Pre-allocate a slice with extra capacity
	existing := make([]float32, 0, 10)
	if err := unmarshalList(info, data, &existing); err != nil {
		t.Fatal(err)
	}

	if len(existing) != 3 {
		t.Fatalf("length mismatch: got %d, want 3", len(existing))
	}
	if cap(existing) != 10 {
		t.Fatalf("expected capacity to be preserved at 10, got %d", cap(existing))
	}
	for i := range input {
		if existing[i] != input[i] {
			t.Errorf("index %d: got %v, want %v", i, existing[i], input[i])
		}
	}
}

func TestUnmarshalListFastPath_Int64_SliceReuse(t *testing.T) {
	info := listTypeInfo(TypeBigInt)
	input := []int64{100, 200, 300, 400}

	data, err := marshalList(info, input)
	if err != nil {
		t.Fatal(err)
	}

	// Pre-allocate with exact capacity
	existing := make([]int64, 0, 4)
	if err := unmarshalList(info, data, &existing); err != nil {
		t.Fatal(err)
	}

	if len(existing) != 4 {
		t.Fatalf("length mismatch: got %d, want 4", len(existing))
	}
	for i := range input {
		if existing[i] != input[i] {
			t.Errorf("index %d: got %v, want %v", i, existing[i], input[i])
		}
	}
}

// --- Wire format compatibility test ---
// Verify the fast-path produces identical bytes to the reflect path.

func TestMarshalListFastPath_WireCompatibility_Int32(t *testing.T) {
	info := listTypeInfo(TypeInt)
	input := []int32{1, 2, 3}

	// Expected wire format: [count=3] + 3 × ([len=4] + [big-endian int32])
	// Total = 4 + 3*8 = 28 bytes
	expected := make([]byte, 28)
	binary.BigEndian.PutUint32(expected[0:], 3)  // count
	binary.BigEndian.PutUint32(expected[4:], 4)  // elem 0 length
	binary.BigEndian.PutUint32(expected[8:], 1)  // elem 0 value
	binary.BigEndian.PutUint32(expected[12:], 4) // elem 1 length
	binary.BigEndian.PutUint32(expected[16:], 2) // elem 1 value
	binary.BigEndian.PutUint32(expected[20:], 4) // elem 2 length
	binary.BigEndian.PutUint32(expected[24:], 3) // elem 2 value

	data, err := marshalList(info, input)
	if err != nil {
		t.Fatal(err)
	}

	if len(data) != len(expected) {
		t.Fatalf("wire size mismatch: got %d, want %d", len(data), len(expected))
	}
	for i := range expected {
		if data[i] != expected[i] {
			t.Errorf("byte %d: got %02x, want %02x", i, data[i], expected[i])
		}
	}
}

func TestMarshalListFastPath_WireCompatibility_Float64(t *testing.T) {
	info := listTypeInfo(TypeDouble)
	input := []float64{1.5, -2.5}

	// Expected wire format: [count=2] + 2 × ([len=8] + [big-endian float64])
	// Total = 4 + 2*12 = 28 bytes
	expected := make([]byte, 28)
	binary.BigEndian.PutUint32(expected[0:], 2)                       // count
	binary.BigEndian.PutUint32(expected[4:], 8)                       // elem 0 length
	binary.BigEndian.PutUint64(expected[8:], math.Float64bits(1.5))   // elem 0 value
	binary.BigEndian.PutUint32(expected[16:], 8)                      // elem 1 length
	binary.BigEndian.PutUint64(expected[20:], math.Float64bits(-2.5)) // elem 1 value

	data, err := marshalList(info, input)
	if err != nil {
		t.Fatal(err)
	}

	if len(data) != len(expected) {
		t.Fatalf("wire size mismatch: got %d, want %d", len(data), len(expected))
	}
	for i := range expected {
		if data[i] != expected[i] {
			t.Errorf("byte %d: got %02x, want %02x", i, data[i], expected[i])
		}
	}
}

// --- Boundary value tests ---

func TestMarshalListFastPath_Float32_SpecialValues(t *testing.T) {
	info := listTypeInfo(TypeFloat)
	nan := float32(math.NaN())
	inf := float32(math.Inf(1))
	negInf := float32(math.Inf(-1))
	input := []float32{nan, inf, negInf, 0, -0}

	data, err := marshalList(info, input)
	if err != nil {
		t.Fatal(err)
	}

	var output []float32
	if err := unmarshalList(info, data, &output); err != nil {
		t.Fatal(err)
	}

	if len(output) != len(input) {
		t.Fatalf("length mismatch: got %d, want %d", len(output), len(input))
	}
	// NaN != NaN, check bits
	if math.Float32bits(output[0]) != math.Float32bits(nan) {
		t.Errorf("NaN bits mismatch: got %08x, want %08x", math.Float32bits(output[0]), math.Float32bits(nan))
	}
	if output[1] != inf {
		t.Errorf("Inf: got %v, want %v", output[1], inf)
	}
	if output[2] != negInf {
		t.Errorf("-Inf: got %v, want %v", output[2], negInf)
	}
}

func TestMarshalListFastPath_Int32_BoundaryValues(t *testing.T) {
	info := listTypeInfo(TypeInt)
	input := []int32{math.MaxInt32, math.MinInt32, 0, 1, -1}

	data, err := marshalList(info, input)
	if err != nil {
		t.Fatal(err)
	}

	var output []int32
	if err := unmarshalList(info, data, &output); err != nil {
		t.Fatal(err)
	}

	for i := range input {
		if output[i] != input[i] {
			t.Errorf("index %d: got %d, want %d", i, output[i], input[i])
		}
	}
}

func TestMarshalListFastPath_Int64_BoundaryValues(t *testing.T) {
	info := listTypeInfo(TypeBigInt)
	input := []int64{math.MaxInt64, math.MinInt64, 0, 1, -1}

	data, err := marshalList(info, input)
	if err != nil {
		t.Fatal(err)
	}

	var output []int64
	if err := unmarshalList(info, data, &output); err != nil {
		t.Fatal(err)
	}

	for i := range input {
		if output[i] != input[i] {
			t.Errorf("index %d: got %d, want %d", i, output[i], input[i])
		}
	}
}

// --- Wrong-type fallback tests ---
// Verify that passing a non-matching Go type falls through to the reflect path.

func TestMarshalListFastPath_WrongType_FallsToReflect(t *testing.T) {
	info := listTypeInfo(TypeInt)
	// Pass []int instead of []int32 — should fall through to reflect path
	input := []int{1, 2, 3}

	data, err := marshalList(info, input)
	if err != nil {
		t.Fatal(err)
	}

	// Verify the data is valid by unmarshaling via reflect path
	var output []int
	if err := unmarshalList(info, data, &output); err != nil {
		t.Fatal(err)
	}

	if len(output) != 3 || output[0] != 1 || output[1] != 2 || output[2] != 3 {
		t.Errorf("reflect fallback: got %v, want [1 2 3]", output)
	}
}

// --- Set type tests (sets use the same code path as lists) ---

func TestMarshalSetFastPath_Int32_RoundTrip(t *testing.T) {
	info := setTypeInfo(TypeInt)
	input := []int32{10, 20, 30}

	data, err := marshalList(info, input)
	if err != nil {
		t.Fatal(err)
	}

	var output []int32
	if err := unmarshalList(info, data, &output); err != nil {
		t.Fatal(err)
	}

	if len(output) != len(input) {
		t.Fatalf("length mismatch: got %d, want %d", len(output), len(input))
	}
	for i := range input {
		if output[i] != input[i] {
			t.Errorf("index %d: got %v, want %v", i, output[i], input[i])
		}
	}
}

func TestMarshalSetFastPath_Float64_RoundTrip(t *testing.T) {
	info := setTypeInfo(TypeDouble)
	input := []float64{1.1, 2.2, 3.3}

	data, err := marshalList(info, input)
	if err != nil {
		t.Fatal(err)
	}

	var output []float64
	if err := unmarshalList(info, data, &output); err != nil {
		t.Fatal(err)
	}

	if len(output) != len(input) {
		t.Fatalf("length mismatch: got %d, want %d", len(output), len(input))
	}
	for i := range input {
		if output[i] != input[i] {
			t.Errorf("index %d: got %v, want %v", i, output[i], input[i])
		}
	}
}

// --- Large list test ---

func TestMarshalListFastPath_Float32_Large(t *testing.T) {
	info := listTypeInfo(TypeFloat)
	n := 10000
	input := make([]float32, n)
	for i := range input {
		input[i] = float32(i) * 0.1
	}

	data, err := marshalList(info, input)
	if err != nil {
		t.Fatal(err)
	}

	var output []float32
	if err := unmarshalList(info, data, &output); err != nil {
		t.Fatal(err)
	}

	if len(output) != n {
		t.Fatalf("length mismatch: got %d, want %d", len(output), n)
	}
	for i := range input {
		if output[i] != input[i] {
			t.Errorf("index %d: got %v, want %v", i, output[i], input[i])
		}
	}
}

func TestMarshalListFastPath_Int64_Large(t *testing.T) {
	info := listTypeInfo(TypeBigInt)
	n := 10000
	input := make([]int64, n)
	for i := range input {
		input[i] = int64(i) * 1000000
	}

	data, err := marshalList(info, input)
	if err != nil {
		t.Fatal(err)
	}

	var output []int64
	if err := unmarshalList(info, data, &output); err != nil {
		t.Fatal(err)
	}

	if len(output) != n {
		t.Fatalf("length mismatch: got %d, want %d", len(output), n)
	}
	for i := range input {
		if output[i] != input[i] {
			t.Errorf("index %d: got %d, want %d", i, output[i], input[i])
		}
	}
}

// --- Cross-path compatibility test ---
// Verify fast-path marshal produces data that the reflect-path unmarshal can read,
// and vice versa.

func TestMarshalListFastPath_CrossPathCompat_Int32(t *testing.T) {
	info := listTypeInfo(TypeInt)
	input := []int32{100, 200, 300}

	// Fast path marshal
	data, err := marshalList(info, input)
	if err != nil {
		t.Fatal(err)
	}

	// Reflect path unmarshal (use []int which doesn't match fast path)
	var reflectOutput []int
	if err := unmarshalList(info, data, &reflectOutput); err != nil {
		t.Fatal(err)
	}

	for i := range input {
		if int32(reflectOutput[i]) != input[i] {
			t.Errorf("index %d: got %d, want %d", i, reflectOutput[i], input[i])
		}
	}
}

// --- listByteSize overflow test ---

func TestListByteSize_Overflow(t *testing.T) {
	_, err := listByteSize(math.MaxInt/2, 8)
	if err == nil {
		t.Error("expected overflow error, got nil")
	}
}

func TestListByteSize_Zero(t *testing.T) {
	size, err := listByteSize(0, 4)
	if err != nil {
		t.Fatal(err)
	}
	if size != 4 {
		t.Errorf("expected 4 (header only), got %d", size)
	}
}

// --- Unmarshal error cases ---

func TestUnmarshalListFastPath_Float32_TruncatedData(t *testing.T) {
	info := listTypeInfo(TypeFloat)
	// Valid header saying 3 elements, but not enough data
	data := make([]byte, 10) // needs 4 + 3*8 = 28 bytes
	binary.BigEndian.PutUint32(data, 3)

	var output []float32
	err := unmarshalList(info, data, &output)
	if err == nil {
		t.Error("expected error for truncated data, got nil")
	}
}

func TestUnmarshalListFastPath_Int32_TruncatedHeader(t *testing.T) {
	info := listTypeInfo(TypeInt)
	data := make([]byte, 2) // needs at least 4 bytes for header

	var output []int32
	err := unmarshalList(info, data, &output)
	if err == nil {
		t.Error("expected error for truncated header, got nil")
	}
}

func TestUnmarshalListFastPath_NegativeCount(t *testing.T) {
	info := listTypeInfo(TypeInt)
	// Craft a payload with high bit set in count (0x80000000 = -2147483648 as int32)
	data := make([]byte, 4)
	binary.BigEndian.PutUint32(data, 0x80000000)

	var output []int32
	err := unmarshalList(info, data, &output)
	if err == nil {
		t.Error("expected error for negative count, got nil")
	}
}

// --- Null element tests ---
// CQL lists can contain null elements (represented by -1 length prefix).
// The fast path should handle these by writing the zero value, matching the slow path.

func TestUnmarshalListFastPath_Int32_NullElement(t *testing.T) {
	info := listTypeInfo(TypeInt)
	// Wire format: count=3, elem0=1, elem1=null, elem2=3
	// [count=3] [len=4][1] [len=-1] [len=4][3]
	// Total = 4 + 8 + 4 + 8 = 24 bytes
	data := make([]byte, 24)
	binary.BigEndian.PutUint32(data[0:], 3)           // count
	binary.BigEndian.PutUint32(data[4:], 4)           // elem 0 length
	binary.BigEndian.PutUint32(data[8:], 1)           // elem 0 value
	binary.BigEndian.PutUint32(data[12:], 0xFFFFFFFF) // elem 1 length = -1 (null)
	binary.BigEndian.PutUint32(data[16:], 4)          // elem 2 length
	binary.BigEndian.PutUint32(data[20:], 3)          // elem 2 value

	var output []int32
	if err := unmarshalList(info, data, &output); err != nil {
		t.Fatal(err)
	}

	expected := []int32{1, 0, 3}
	if len(output) != len(expected) {
		t.Fatalf("length mismatch: got %d, want %d", len(output), len(expected))
	}
	for i := range expected {
		if output[i] != expected[i] {
			t.Errorf("index %d: got %d, want %d", i, output[i], expected[i])
		}
	}
}

func TestUnmarshalListFastPath_Float32_NullElement(t *testing.T) {
	info := listTypeInfo(TypeFloat)
	// Wire format: count=2, elem0=null, elem1=1.5
	data := make([]byte, 16)
	binary.BigEndian.PutUint32(data[0:], 2)                      // count
	binary.BigEndian.PutUint32(data[4:], 0xFFFFFFFF)             // elem 0 = null
	binary.BigEndian.PutUint32(data[8:], 4)                      // elem 1 length
	binary.BigEndian.PutUint32(data[12:], math.Float32bits(1.5)) // elem 1 value

	var output []float32
	if err := unmarshalList(info, data, &output); err != nil {
		t.Fatal(err)
	}

	if len(output) != 2 {
		t.Fatalf("length mismatch: got %d, want 2", len(output))
	}
	if output[0] != 0 {
		t.Errorf("null element: got %v, want 0", output[0])
	}
	if output[1] != 1.5 {
		t.Errorf("normal element: got %v, want 1.5", output[1])
	}
}

func TestUnmarshalListFastPath_Int64_NullElement(t *testing.T) {
	info := listTypeInfo(TypeBigInt)
	// Wire format: count=2, elem0=100, elem1=null
	data := make([]byte, 20)
	binary.BigEndian.PutUint32(data[0:], 2)           // count
	binary.BigEndian.PutUint32(data[4:], 8)           // elem 0 length
	binary.BigEndian.PutUint64(data[8:], 100)         // elem 0 value
	binary.BigEndian.PutUint32(data[16:], 0xFFFFFFFF) // elem 1 = null

	var output []int64
	if err := unmarshalList(info, data, &output); err != nil {
		t.Fatal(err)
	}

	if len(output) != 2 || output[0] != 100 || output[1] != 0 {
		t.Errorf("got %v, want [100 0]", output)
	}
}

// --- TypeCounter test ---

func TestMarshalListFastPath_Counter_RoundTrip(t *testing.T) {
	info := CollectionType{
		NativeType: NativeType{proto: protoVersion4, typ: TypeList},
		Elem:       NativeType{proto: protoVersion4, typ: TypeCounter},
	}
	input := []int64{10, 20, 30}

	data, err := marshalList(info, input)
	if err != nil {
		t.Fatal(err)
	}

	var output []int64
	if err := unmarshalList(info, data, &output); err != nil {
		t.Fatal(err)
	}

	if len(output) != len(input) {
		t.Fatalf("length mismatch: got %d, want %d", len(output), len(input))
	}
	for i := range input {
		if output[i] != input[i] {
			t.Errorf("index %d: got %d, want %d", i, output[i], input[i])
		}
	}
}

// --- Benchmarks ---

func BenchmarkMarshalListFastPathInt32(b *testing.B) {
	sizes := []struct {
		n    int
		name string
	}{
		{10, "n_10"},
		{100, "n_100"},
		{1000, "n_1000"},
	}

	info := listTypeInfo(TypeInt)

	for _, s := range sizes {
		input := make([]int32, s.n)
		for i := range input {
			input[i] = int32(i)
		}
		b.Run(s.name, func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(s.n * 4))
			for i := 0; i < b.N; i++ {
				_, err := marshalList(info, input)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkUnmarshalListFastPathInt32(b *testing.B) {
	sizes := []struct {
		n    int
		name string
	}{
		{10, "n_10"},
		{100, "n_100"},
		{1000, "n_1000"},
	}

	info := listTypeInfo(TypeInt)

	for _, s := range sizes {
		input := make([]int32, s.n)
		for i := range input {
			input[i] = int32(i)
		}
		data, err := marshalList(info, input)
		if err != nil {
			b.Fatal(err)
		}
		b.Run(s.name, func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(s.n * 4))
			var output []int32
			for i := 0; i < b.N; i++ {
				err := unmarshalList(info, data, &output)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkMarshalListFastPathFloat32(b *testing.B) {
	sizes := []struct {
		n    int
		name string
	}{
		{10, "n_10"},
		{100, "n_100"},
		{1000, "n_1000"},
	}

	info := listTypeInfo(TypeFloat)

	for _, s := range sizes {
		input := make([]float32, s.n)
		for i := range input {
			input[i] = float32(i) * 0.1
		}
		b.Run(s.name, func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(s.n * 4))
			for i := 0; i < b.N; i++ {
				_, err := marshalList(info, input)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkUnmarshalListFastPathFloat32(b *testing.B) {
	sizes := []struct {
		n    int
		name string
	}{
		{10, "n_10"},
		{100, "n_100"},
		{1000, "n_1000"},
	}

	info := listTypeInfo(TypeFloat)

	for _, s := range sizes {
		input := make([]float32, s.n)
		for i := range input {
			input[i] = float32(i) * 0.1
		}
		data, err := marshalList(info, input)
		if err != nil {
			b.Fatal(err)
		}
		b.Run(s.name, func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(s.n * 4))
			var output []float32
			for i := 0; i < b.N; i++ {
				err := unmarshalList(info, data, &output)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkMarshalListFastPathFloat64(b *testing.B) {
	sizes := []struct {
		n    int
		name string
	}{
		{10, "n_10"},
		{100, "n_100"},
		{1000, "n_1000"},
	}

	info := listTypeInfo(TypeDouble)

	for _, s := range sizes {
		input := make([]float64, s.n)
		for i := range input {
			input[i] = float64(i) * 0.1
		}
		b.Run(s.name, func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(s.n * 8))
			for i := 0; i < b.N; i++ {
				_, err := marshalList(info, input)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkUnmarshalListFastPathFloat64(b *testing.B) {
	sizes := []struct {
		n    int
		name string
	}{
		{10, "n_10"},
		{100, "n_100"},
		{1000, "n_1000"},
	}

	info := listTypeInfo(TypeDouble)

	for _, s := range sizes {
		input := make([]float64, s.n)
		for i := range input {
			input[i] = float64(i) * 0.1
		}
		data, err := marshalList(info, input)
		if err != nil {
			b.Fatal(err)
		}
		b.Run(s.name, func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(s.n * 8))
			var output []float64
			for i := 0; i < b.N; i++ {
				err := unmarshalList(info, data, &output)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkMarshalListFastPathInt64(b *testing.B) {
	sizes := []struct {
		n    int
		name string
	}{
		{10, "n_10"},
		{100, "n_100"},
		{1000, "n_1000"},
	}

	info := listTypeInfo(TypeBigInt)

	for _, s := range sizes {
		input := make([]int64, s.n)
		for i := range input {
			input[i] = int64(i) * 1000000
		}
		b.Run(s.name, func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(s.n * 8))
			for i := 0; i < b.N; i++ {
				_, err := marshalList(info, input)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkUnmarshalListFastPathInt64(b *testing.B) {
	sizes := []struct {
		n    int
		name string
	}{
		{10, "n_10"},
		{100, "n_100"},
		{1000, "n_1000"},
	}

	info := listTypeInfo(TypeBigInt)

	for _, s := range sizes {
		input := make([]int64, s.n)
		for i := range input {
			input[i] = int64(i) * 1000000
		}
		data, err := marshalList(info, input)
		if err != nil {
			b.Fatal(err)
		}
		b.Run(s.name, func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(s.n * 8))
			var output []int64
			for i := 0; i < b.N; i++ {
				err := unmarshalList(info, data, &output)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
