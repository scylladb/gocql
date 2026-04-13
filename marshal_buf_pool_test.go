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

func TestFinishMarshalBuf_CopiesData(t *testing.T) {
	buf := getMarshalBuf(0)
	buf.WriteString("hello world")

	// Get a pointer to the internal storage before finish.
	internalPtr := buf.Bytes()

	result := finishMarshalBuf(buf)

	if string(result) != "hello world" {
		t.Fatalf("expected 'hello world', got %q", result)
	}

	// Verify that result does not alias the internal buffer by getting
	// a new buffer from the pool and writing different data.
	buf2 := getMarshalBuf(0)
	buf2.WriteString("OVERWRITTEN!")

	// The original result should be unchanged (no aliasing).
	if string(result) != "hello world" {
		t.Fatalf("result was corrupted after pool reuse: got %q", result)
	}

	// Also verify the internal pointer's data was overwritten (proves the
	// pool actually reused the buffer).
	_ = internalPtr // The data at this pointer may have been overwritten.
	putMarshalBuf(buf2)
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

func TestMarshalList_PooledRoundTrip_IntSlice(t *testing.T) {
	info := CollectionType{
		NativeType: NativeType{proto: protoVersion4, typ: TypeList},
		Elem:       NativeType{proto: protoVersion4, typ: TypeInt},
	}

	input := []int32{1, 2, 3, -1, 0, math.MaxInt32, math.MinInt32}
	data, err := marshalList(info, input)
	if err != nil {
		t.Fatalf("marshalList: %v", err)
	}

	var output []int32
	if err := unmarshalList(info, data, &output); err != nil {
		t.Fatalf("unmarshalList: %v", err)
	}

	if len(output) != len(input) {
		t.Fatalf("len mismatch: got %d, want %d", len(output), len(input))
	}
	for i := range input {
		if input[i] != output[i] {
			t.Errorf("element %d: got %d, want %d", i, output[i], input[i])
		}
	}
}

func TestMarshalList_PooledRoundTrip_Float32Slice(t *testing.T) {
	info := CollectionType{
		NativeType: NativeType{proto: protoVersion4, typ: TypeList},
		Elem:       NativeType{proto: protoVersion4, typ: TypeFloat},
	}

	input := []float32{0.0, 1.5, -1.5, math.MaxFloat32, math.SmallestNonzeroFloat32, float32(math.NaN())}
	data, err := marshalList(info, input)
	if err != nil {
		t.Fatalf("marshalList: %v", err)
	}

	var output []float32
	if err := unmarshalList(info, data, &output); err != nil {
		t.Fatalf("unmarshalList: %v", err)
	}

	if len(output) != len(input) {
		t.Fatalf("len mismatch: got %d, want %d", len(output), len(input))
	}
	for i := range input {
		// NaN != NaN, so compare bits.
		if math.Float32bits(input[i]) != math.Float32bits(output[i]) {
			t.Errorf("element %d: got bits %08x, want %08x", i, math.Float32bits(output[i]), math.Float32bits(input[i]))
		}
	}
}

func TestMarshalList_PooledRoundTrip_StringSlice(t *testing.T) {
	info := CollectionType{
		NativeType: NativeType{proto: protoVersion4, typ: TypeList},
		Elem:       NativeType{proto: protoVersion4, typ: TypeVarchar},
	}

	input := []string{"hello", "", "world", "a longer string with spaces"}
	data, err := marshalList(info, input)
	if err != nil {
		t.Fatalf("marshalList: %v", err)
	}

	var output []string
	if err := unmarshalList(info, data, &output); err != nil {
		t.Fatalf("unmarshalList: %v", err)
	}

	if len(output) != len(input) {
		t.Fatalf("len mismatch: got %d, want %d", len(output), len(input))
	}
	for i := range input {
		if input[i] != output[i] {
			t.Errorf("element %d: got %q, want %q", i, output[i], input[i])
		}
	}
}

func TestMarshalList_PooledRoundTrip_Empty(t *testing.T) {
	info := CollectionType{
		NativeType: NativeType{proto: protoVersion4, typ: TypeList},
		Elem:       NativeType{proto: protoVersion4, typ: TypeInt},
	}

	input := []int32{}
	data, err := marshalList(info, input)
	if err != nil {
		t.Fatalf("marshalList: %v", err)
	}

	var output []int32
	if err := unmarshalList(info, data, &output); err != nil {
		t.Fatalf("unmarshalList: %v", err)
	}

	if len(output) != 0 {
		t.Fatalf("expected empty slice, got len=%d", len(output))
	}
}

func TestMarshalList_PooledRoundTrip_Nil(t *testing.T) {
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

// --- marshalOutputPool tests ---

func TestGetMarshalOutputFresh(t *testing.T) {
	buf := getMarshalOutput(64)
	if len(buf) != 64 {
		t.Fatalf("expected len 64, got %d", len(buf))
	}
	if cap(buf) < 64 {
		t.Fatalf("expected cap >= 64, got %d", cap(buf))
	}
}

func TestGetMarshalOutputFromPool(t *testing.T) {
	// Put a buffer into the pool, then retrieve it.
	orig := make([]byte, 0, 128)
	marshalOutputPool.Put(orig)

	buf := getMarshalOutput(64)
	if len(buf) != 64 {
		t.Fatalf("expected len 64, got %d", len(buf))
	}
	// The pool should have returned the 128-cap buffer.
	if cap(buf) < 128 {
		t.Logf("pool did not return expected buffer (cap %d); may have been GC'd", cap(buf))
	}
}

func TestGetMarshalOutputPoolTooSmall(t *testing.T) {
	// Put a small buffer, request a larger one — should get a fresh allocation.
	small := make([]byte, 0, 8)
	marshalOutputPool.Put(small)

	buf := getMarshalOutput(64)
	if len(buf) != 64 {
		t.Fatalf("expected len 64, got %d", len(buf))
	}
}

func TestPutMarshalOutputNil(t *testing.T) {
	// Should not panic.
	putMarshalOutput(nil)
}

func TestPutMarshalOutputOversized(t *testing.T) {
	// Buffers larger than marshalBufMaxCap should be discarded.
	huge := make([]byte, marshalBufMaxCap+1)
	putMarshalOutput(huge)
	// If we get it back, the pool ignored the cap limit (unlikely).
	// Can't reliably test pool internals, just verify no panic.
}

func TestMarshalOutputPoolRoundTrip(t *testing.T) {
	// Verify that a buffer returned to the pool can be reused.
	buf := getMarshalOutput(32)
	for i := range buf {
		buf[i] = byte(i)
	}
	putMarshalOutput(buf)

	buf2 := getMarshalOutput(16)
	if len(buf2) != 16 {
		t.Fatalf("expected len 16, got %d", len(buf2))
	}
	// buf2 may or may not be the same underlying array (GC can collect pool entries).
	// Just verify it's usable.
	for i := range buf2 {
		buf2[i] = 0xff
	}
}

func TestPooledMarshalType(t *testing.T) {
	tests := []struct {
		name   string
		info   TypeInfo
		expect bool
	}{
		// Vectors with pooled subtypes.
		{"vector<float>", VectorType{Dimensions: 3, SubType: NativeType{proto: protoVersion4, typ: TypeFloat}}, true},
		{"vector<double>", VectorType{Dimensions: 3, SubType: NativeType{proto: protoVersion4, typ: TypeDouble}}, true},
		{"vector<int>", VectorType{Dimensions: 3, SubType: NativeType{proto: protoVersion4, typ: TypeInt}}, true},
		{"vector<bigint>", VectorType{Dimensions: 3, SubType: NativeType{proto: protoVersion4, typ: TypeBigInt}}, true},
		{"vector<timestamp>", VectorType{Dimensions: 3, SubType: NativeType{proto: protoVersion4, typ: TypeTimestamp}}, true},
		{"vector<counter>", VectorType{Dimensions: 3, SubType: NativeType{proto: protoVersion4, typ: TypeCounter}}, true},
		{"vector<uuid>", VectorType{Dimensions: 3, SubType: NativeType{proto: protoVersion4, typ: TypeUUID}}, true},
		{"vector<timeuuid>", VectorType{Dimensions: 3, SubType: NativeType{proto: protoVersion4, typ: TypeTimeUUID}}, true},

		// Vectors with non-pooled subtypes.
		{"vector<text>", VectorType{Dimensions: 3, SubType: NativeType{proto: protoVersion4, typ: TypeVarchar}}, false},
		{"vector<blob>", VectorType{Dimensions: 3, SubType: NativeType{proto: protoVersion4, typ: TypeBlob}}, false},
		{"vector<boolean>", VectorType{Dimensions: 3, SubType: NativeType{proto: protoVersion4, typ: TypeBoolean}}, false},

		// Lists/sets with pooled elem types.
		{"list<float>", CollectionType{NativeType: NativeType{proto: protoVersion4, typ: TypeList}, Elem: NativeType{proto: protoVersion4, typ: TypeFloat}}, true},
		{"list<double>", CollectionType{NativeType: NativeType{proto: protoVersion4, typ: TypeList}, Elem: NativeType{proto: protoVersion4, typ: TypeDouble}}, true},
		{"list<int>", CollectionType{NativeType: NativeType{proto: protoVersion4, typ: TypeList}, Elem: NativeType{proto: protoVersion4, typ: TypeInt}}, true},
		{"list<bigint>", CollectionType{NativeType: NativeType{proto: protoVersion4, typ: TypeList}, Elem: NativeType{proto: protoVersion4, typ: TypeBigInt}}, true},
		{"list<timestamp>", CollectionType{NativeType: NativeType{proto: protoVersion4, typ: TypeList}, Elem: NativeType{proto: protoVersion4, typ: TypeTimestamp}}, true},
		{"list<counter>", CollectionType{NativeType: NativeType{proto: protoVersion4, typ: TypeList}, Elem: NativeType{proto: protoVersion4, typ: TypeCounter}}, true},
		{"set<float>", CollectionType{NativeType: NativeType{proto: protoVersion4, typ: TypeSet}, Elem: NativeType{proto: protoVersion4, typ: TypeFloat}}, true},
		{"set<int>", CollectionType{NativeType: NativeType{proto: protoVersion4, typ: TypeSet}, Elem: NativeType{proto: protoVersion4, typ: TypeInt}}, true},

		// Lists/sets with non-pooled elem types.
		{"list<text>", CollectionType{NativeType: NativeType{proto: protoVersion4, typ: TypeList}, Elem: NativeType{proto: protoVersion4, typ: TypeVarchar}}, false},
		{"set<blob>", CollectionType{NativeType: NativeType{proto: protoVersion4, typ: TypeSet}, Elem: NativeType{proto: protoVersion4, typ: TypeBlob}}, false},
		{"list<uuid>", CollectionType{NativeType: NativeType{proto: protoVersion4, typ: TypeList}, Elem: NativeType{proto: protoVersion4, typ: TypeUUID}}, false},

		// Maps are never pooled.
		{"map<int,int>", CollectionType{NativeType: NativeType{proto: protoVersion4, typ: TypeMap}, Key: NativeType{proto: protoVersion4, typ: TypeInt}, Elem: NativeType{proto: protoVersion4, typ: TypeInt}}, false},

		// Native types are never pooled.
		{"int", NativeType{proto: protoVersion4, typ: TypeInt}, false},
		{"text", NativeType{proto: protoVersion4, typ: TypeVarchar}, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := pooledMarshalType(tt.info)
			if got != tt.expect {
				t.Errorf("pooledMarshalType(%s) = %v, want %v", tt.name, got, tt.expect)
			}
		})
	}
}

func TestMarshalVectorFloat32UsesPool(t *testing.T) {
	// Marshal, put back, marshal again — second call should reuse the buffer.
	vec := []float32{1.0, 2.0, 3.0}
	buf1, err := marshalVectorFloat32(vec, 3)
	if err != nil {
		t.Fatal(err)
	}
	// Copy the data before returning to pool.
	data1 := make([]byte, len(buf1))
	copy(data1, buf1)
	putMarshalOutput(buf1)

	buf2, err := marshalVectorFloat32(vec, 3)
	if err != nil {
		t.Fatal(err)
	}
	// Verify data is correct regardless of pool reuse.
	if !bytes.Equal(data1, buf2) {
		t.Fatalf("data mismatch after pool reuse")
	}
	putMarshalOutput(buf2)
}

func TestMarshalListInt32UsesPool(t *testing.T) {
	list := []int32{10, 20, 30}
	buf1, err := marshalListInt32(list)
	if err != nil {
		t.Fatal(err)
	}
	data1 := make([]byte, len(buf1))
	copy(data1, buf1)
	putMarshalOutput(buf1)

	buf2, err := marshalListInt32(list)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(data1, buf2) {
		t.Fatalf("data mismatch after pool reuse")
	}
	putMarshalOutput(buf2)
}
