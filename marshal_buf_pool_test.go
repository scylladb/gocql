// Copyright (c) 2012 The gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

//go:build all || unit
// +build all unit

package gocql

import (
	"bytes"
	"encoding/binary"
	"math"
	"sync"
	"testing"
)

// --- Pool infrastructure tests ---

func TestGetMarshalBuf_ReturnsNonNil(t *testing.T) {
	buf := getMarshalBuf(0)
	if buf == nil {
		t.Fatal("getMarshalBuf(0) returned nil")
	}
	putMarshalBuf(buf)
}

func TestGetMarshalBuf_RespectsSizeHint(t *testing.T) {
	buf := getMarshalBuf(1024)
	if buf.Cap() < 1024 {
		t.Fatalf("expected capacity >= 1024, got %d", buf.Cap())
	}
	putMarshalBuf(buf)
}

func TestGetMarshalBuf_IsReset(t *testing.T) {
	buf := getMarshalBuf(0)
	buf.WriteString("leftover data")
	putMarshalBuf(buf)

	buf2 := getMarshalBuf(0)
	if buf2.Len() != 0 {
		t.Fatalf("expected buffer to be reset, got len=%d", buf2.Len())
	}
	putMarshalBuf(buf2)
}

func TestPutMarshalBuf_NilSafe(t *testing.T) {
	// Should not panic.
	putMarshalBuf(nil)
}

func TestPutMarshalBuf_DiscardsOversized(t *testing.T) {
	buf := getMarshalBuf(marshalBufMaxCap + 1)
	buf.Write(make([]byte, marshalBufMaxCap+1))
	// This should discard the buffer (not return it to pool).
	// We can't directly verify it was discarded, but we can verify no panic.
	putMarshalBuf(buf)
}

func TestFinishMarshalBuf_EmptyBuffer(t *testing.T) {
	buf := getMarshalBuf(0)
	result := finishMarshalBuf(buf)
	if result == nil {
		t.Fatal("expected non-nil empty slice, got nil")
	}
	if len(result) != 0 {
		t.Fatalf("expected empty slice, got len=%d", len(result))
	}
}

// --- Concurrent safety test ---

func TestMarshalBufPool_ConcurrentSafety(t *testing.T) {
	const goroutines = 100
	const iterations = 1000

	var wg sync.WaitGroup
	wg.Add(goroutines)

	for g := 0; g < goroutines; g++ {
		go func(id int) {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				buf := getMarshalBuf(64)
				buf.WriteString("data from goroutine")
				result := finishMarshalBuf(buf)
				if string(result) != "data from goroutine" {
					t.Errorf("goroutine %d iteration %d: got %q", id, i, result)
					return
				}
			}
		}(g)
	}
	wg.Wait()
}

// --- fixedElemSize tests ---

func TestFixedElemSize(t *testing.T) {
	tests := []struct {
		typ      Type
		expected int
	}{
		{TypeInt, 4},
		{TypeFloat, 4},
		{TypeDate, 4},
		{TypeBigInt, 8},
		{TypeDouble, 8},
		{TypeTimestamp, 8},
		{TypeCounter, 8},
		{TypeTime, 8},
		{TypeUUID, 16},
		{TypeTimeUUID, 16},
		// Small fixed types excluded intentionally (see fixedElemSize comment).
		{TypeBoolean, 0},
		{TypeTinyInt, 0},
		{TypeSmallInt, 0},
		// Variable-length types should return 0.
		{TypeVarchar, 0},
		{TypeBlob, 0},
		{TypeText, 0},
		{TypeVarint, 0},
		{TypeDecimal, 0},
		{TypeCustom, 0},
	}

	for _, tc := range tests {
		info := NativeType{proto: protoVersion4, typ: tc.typ}
		got := fixedElemSize(info)
		if got != tc.expected {
			t.Errorf("fixedElemSize(%v) = %d, want %d", tc.typ, got, tc.expected)
		}
	}
}

// --- Round-trip correctness: marshal with pooled buffers produces identical output ---

// testListRoundTrip marshals input, unmarshals it back, and compares
// element-by-element using equal (so callers can special-case things like NaN).
func testListRoundTrip[T any](t *testing.T, elemType Type, input []T, equal func(a, b T) bool) {
	t.Helper()
	info := CollectionType{
		NativeType: NativeType{proto: protoVersion4, typ: TypeList},
		Elem:       NativeType{proto: protoVersion4, typ: elemType},
	}

	data, err := marshalList(info, input)
	if err != nil {
		t.Fatalf("marshalList: %v", err)
	}

	var output []T
	if err := unmarshalList(info, data, &output); err != nil {
		t.Fatalf("unmarshalList: %v", err)
	}

	if len(output) != len(input) {
		t.Fatalf("len mismatch: got %d, want %d", len(output), len(input))
	}
	for i := range input {
		if !equal(input[i], output[i]) {
			t.Errorf("element %d: got %v, want %v", i, output[i], input[i])
		}
	}
}

func TestMarshalList_PooledRoundTrip(t *testing.T) {
	t.Run("IntSlice", func(t *testing.T) {
		input := []int32{1, 2, 3, -1, 0, math.MaxInt32, math.MinInt32}
		testListRoundTrip(t, TypeInt, input, func(a, b int32) bool { return a == b })
	})

	t.Run("Float32Slice", func(t *testing.T) {
		input := []float32{0.0, 1.5, -1.5, math.MaxFloat32, math.SmallestNonzeroFloat32, float32(math.NaN())}
		// NaN != NaN, so compare bits.
		testListRoundTrip(t, TypeFloat, input, func(a, b float32) bool {
			return math.Float32bits(a) == math.Float32bits(b)
		})
	})

	t.Run("StringSlice", func(t *testing.T) {
		input := []string{"hello", "", "world", "a longer string with spaces"}
		testListRoundTrip(t, TypeVarchar, input, func(a, b string) bool { return a == b })
	})

	t.Run("Empty", func(t *testing.T) {
		testListRoundTrip(t, TypeInt, []int32{}, func(a, b int32) bool { return a == b })
	})

	t.Run("Nil", func(t *testing.T) {
		info := CollectionType{
			NativeType: NativeType{proto: protoVersion4, typ: TypeList},
			Elem:       NativeType{proto: protoVersion4, typ: TypeInt},
		}

		data, err := marshalList(info, nil)
		if err != nil {
			t.Fatalf("marshalList: %v", err)
		}
		if data != nil {
			t.Fatalf("expected nil for nil input, got %v", data)
		}
	})
}

func TestMarshalMap_PooledRoundTrip(t *testing.T) {
	info := CollectionType{
		NativeType: NativeType{proto: protoVersion4, typ: TypeMap},
		Key:        NativeType{proto: protoVersion4, typ: TypeVarchar},
		Elem:       NativeType{proto: protoVersion4, typ: TypeInt},
	}

	input := map[string]int{"alpha": 1, "beta": 2, "gamma": 3}
	data, err := marshalMap(info, input)
	if err != nil {
		t.Fatalf("marshalMap: %v", err)
	}

	var output map[string]int
	if err := unmarshalMap(info, data, &output); err != nil {
		t.Fatalf("unmarshalMap: %v", err)
	}

	if len(output) != len(input) {
		t.Fatalf("len mismatch: got %d, want %d", len(output), len(input))
	}
	for k, v := range input {
		if output[k] != v {
			t.Errorf("key %q: got %d, want %d", k, output[k], v)
		}
	}
}

func TestMarshalMap_PooledRoundTrip_Empty(t *testing.T) {
	info := CollectionType{
		NativeType: NativeType{proto: protoVersion4, typ: TypeMap},
		Key:        NativeType{proto: protoVersion4, typ: TypeVarchar},
		Elem:       NativeType{proto: protoVersion4, typ: TypeInt},
	}

	input := map[string]int{}
	data, err := marshalMap(info, input)
	if err != nil {
		t.Fatalf("marshalMap: %v", err)
	}

	var output map[string]int
	if err := unmarshalMap(info, data, &output); err != nil {
		t.Fatalf("unmarshalMap: %v", err)
	}

	if len(output) != 0 {
		t.Fatalf("expected empty map, got len=%d", len(output))
	}
}

func TestMarshalVector_PooledRoundTrip(t *testing.T) {
	info := VectorType{
		NativeType: NativeType{
			proto:  protoVersion4,
			typ:    TypeCustom,
			custom: apacheCassandraTypePrefix + "VectorType(" + apacheCassandraTypePrefix + "FloatType, 4)",
		},
		SubType:    NativeType{proto: protoVersion4, typ: TypeFloat},
		Dimensions: 4,
	}

	input := []float32{1.0, 2.0, 3.0, 4.0}
	data, err := marshalVector(info, input)
	if err != nil {
		t.Fatalf("marshalVector: %v", err)
	}

	var output []float32
	if err := unmarshalVector(info, data, &output); err != nil {
		t.Fatalf("unmarshalVector: %v", err)
	}

	if len(output) != len(input) {
		t.Fatalf("len mismatch: got %d, want %d", len(output), len(input))
	}
	for i := range input {
		if input[i] != output[i] {
			t.Errorf("element %d: got %f, want %f", i, output[i], input[i])
		}
	}
}

// --- Byte-compatibility: pooled output is identical to expected wire format ---

func TestMarshalList_ByteCompatibility(t *testing.T) {
	info := CollectionType{
		NativeType: NativeType{proto: protoVersion4, typ: TypeList},
		Elem:       NativeType{proto: protoVersion4, typ: TypeInt},
	}

	input := []int32{1, 2}
	data, err := marshalList(info, input)
	if err != nil {
		t.Fatalf("marshalList: %v", err)
	}

	// Build expected wire format manually:
	// 4 bytes: element count (2)
	// For each element: 4 bytes length (4) + 4 bytes data
	var expected bytes.Buffer
	binary.Write(&expected, binary.BigEndian, int32(2)) // count
	binary.Write(&expected, binary.BigEndian, int32(4)) // len(elem 0)
	binary.Write(&expected, binary.BigEndian, int32(1)) // elem 0
	binary.Write(&expected, binary.BigEndian, int32(4)) // len(elem 1)
	binary.Write(&expected, binary.BigEndian, int32(2)) // elem 1

	if !bytes.Equal(data, expected.Bytes()) {
		t.Fatalf("wire format mismatch:\ngot:  %x\nwant: %x", data, expected.Bytes())
	}
}

// --- Concurrent marshal correctness ---

func TestMarshalList_ConcurrentCorrectness(t *testing.T) {
	info := CollectionType{
		NativeType: NativeType{proto: protoVersion4, typ: TypeList},
		Elem:       NativeType{proto: protoVersion4, typ: TypeInt},
	}

	const goroutines = 50
	const size = 100

	var wg sync.WaitGroup
	wg.Add(goroutines)

	for g := 0; g < goroutines; g++ {
		go func() {
			defer wg.Done()
			input := make([]int32, size)
			for i := range input {
				input[i] = int32(i)
			}
			data, err := marshalList(info, input)
			if err != nil {
				t.Errorf("marshalList: %v", err)
				return
			}

			var output []int32
			if err := unmarshalList(info, data, &output); err != nil {
				t.Errorf("unmarshalList: %v", err)
				return
			}

			for i := range input {
				if input[i] != output[i] {
					t.Errorf("element %d: got %d, want %d", i, output[i], input[i])
					return
				}
			}
		}()
	}
	wg.Wait()
}

// --- Benchmarks for list marshal with pooled buffers ---

func BenchmarkMarshalListInt32(b *testing.B) {
	sizes := []struct {
		n    int
		name string
	}{
		{10, "n_10"},
		{100, "n_100"},
		{1000, "n_1000"},
	}

	info := CollectionType{
		NativeType: NativeType{proto: protoVersion4, typ: TypeList},
		Elem:       NativeType{proto: protoVersion4, typ: TypeInt},
	}

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

func BenchmarkMarshalListFloat32(b *testing.B) {
	sizes := []struct {
		n    int
		name string
	}{
		{10, "n_10"},
		{100, "n_100"},
		{1000, "n_1000"},
	}

	info := CollectionType{
		NativeType: NativeType{proto: protoVersion4, typ: TypeList},
		Elem:       NativeType{proto: protoVersion4, typ: TypeFloat},
	}

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

func BenchmarkMarshalListBigInt(b *testing.B) {
	sizes := []struct {
		n    int
		name string
	}{
		{10, "n_10"},
		{100, "n_100"},
		{1000, "n_1000"},
	}

	info := CollectionType{
		NativeType: NativeType{proto: protoVersion4, typ: TypeList},
		Elem:       NativeType{proto: protoVersion4, typ: TypeBigInt},
	}

	for _, s := range sizes {
		input := make([]int64, s.n)
		for i := range input {
			input[i] = int64(i) * 1000
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

func BenchmarkMarshalMapStringInt(b *testing.B) {
	sizes := []struct {
		n    int
		name string
	}{
		{10, "n_10"},
		{100, "n_100"},
	}

	info := CollectionType{
		NativeType: NativeType{proto: protoVersion4, typ: TypeMap},
		Key:        NativeType{proto: protoVersion4, typ: TypeVarchar},
		Elem:       NativeType{proto: protoVersion4, typ: TypeInt},
	}

	for _, s := range sizes {
		input := make(map[string]int, s.n)
		for i := 0; i < s.n; i++ {
			input[string(rune('a'+i%26))+string(rune('0'+i/26))] = i
		}
		b.Run(s.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_, err := marshalMap(info, input)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
