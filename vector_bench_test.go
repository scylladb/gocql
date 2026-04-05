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

const vectorTypePrefix = apacheCassandraTypePrefix + "VectorType(" + apacheCassandraTypePrefix + "FloatType, "
const vectorTypeSuffix = ")"

func makeFloatVectorType(dim int, dimStr string) VectorType {
	return VectorType{
		NativeType: NativeType{
			proto:  protoVersion4,
			typ:    TypeCustom,
			custom: vectorTypePrefix + dimStr + vectorTypeSuffix,
		},
		SubType:    NativeType{proto: protoVersion4, typ: TypeFloat},
		Dimensions: dim,
	}
}

// BenchmarkUnmarshalVectorFloat32 measures unmarshal performance for float32 vectors
// across common embedding dimensions used in AI/ML applications.
func BenchmarkUnmarshalVectorFloat32(b *testing.B) {
	dims := []struct {
		dim    int
		dimStr string
	}{
		{dim: 128, dimStr: "128"},
		{dim: 384, dimStr: "384"},
		{dim: 768, dimStr: "768"},
		{dim: 1536, dimStr: "1536"},
	}

	for _, entry := range dims {
		dim := entry.dim
		dimStr := entry.dimStr
		b.Run("dim_"+dimStr, func(b *testing.B) {
			b.ReportAllocs()

			data := make([]byte, dim*4)
			for i := 0; i < dim; i++ {
				binary.BigEndian.PutUint32(data[i*4:], math.Float32bits(float32(i)*0.1))
			}

			info := makeFloatVectorType(dim, dimStr)
			var result []float32

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

// BenchmarkMarshalVectorFloat32 measures marshal performance for float32 vectors
// across common embedding dimensions.
func BenchmarkMarshalVectorFloat32(b *testing.B) {
	dims := []struct {
		dim    int
		dimStr string
	}{
		{dim: 128, dimStr: "128"},
		{dim: 384, dimStr: "384"},
		{dim: 768, dimStr: "768"},
		{dim: 1536, dimStr: "1536"},
	}

	for _, entry := range dims {
		dim := entry.dim
		dimStr := entry.dimStr
		b.Run("dim_"+dimStr, func(b *testing.B) {
			b.ReportAllocs()

			vec := make([]float32, dim)
			for i := range vec {
				vec[i] = float32(i) * 0.1
			}

			info := makeFloatVectorType(dim, dimStr)

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

// BenchmarkVectorRoundTrip measures full marshal -> unmarshal cycle.
func BenchmarkVectorRoundTrip(b *testing.B) {
	dims := []struct {
		dim    int
		dimStr string
	}{
		{dim: 128, dimStr: "128"},
		{dim: 384, dimStr: "384"},
		{dim: 768, dimStr: "768"},
		{dim: 1536, dimStr: "1536"},
	}

	for _, entry := range dims {
		dim := entry.dim
		dimStr := entry.dimStr
		b.Run("dim_"+dimStr, func(b *testing.B) {
			b.ReportAllocs()

			srcVec := make([]float32, dim)
			for i := range srcVec {
				srcVec[i] = float32(i) * 0.1
			}

			info := makeFloatVectorType(dim, dimStr)
			var dstVec []float32

			b.SetBytes(int64(dim * 4 * 2))
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				data, err := marshalVector(info, srcVec)
				if err != nil {
					b.Fatal(err)
				}
				if err := unmarshalVector(info, data, &dstVec); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkMarshalVectorFloat32Pooled measures marshal with buffer pool return.
// This simulates the real usage pattern where buffers are returned after framer consumption.
func BenchmarkMarshalVectorFloat32Pooled(b *testing.B) {
	dims := []struct {
		dim    int
		dimStr string
	}{
		{dim: 128, dimStr: "128"},
		{dim: 384, dimStr: "384"},
		{dim: 768, dimStr: "768"},
		{dim: 1536, dimStr: "1536"},
	}

	for _, entry := range dims {
		dim := entry.dim
		dimStr := entry.dimStr
		b.Run("dim_"+dimStr, func(b *testing.B) {
			b.ReportAllocs()

			vec := make([]float32, dim)
			for i := range vec {
				vec[i] = float32(i) * 0.1
			}

			info := makeFloatVectorType(dim, dimStr)

			b.SetBytes(int64(dim * 4))
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				data, err := marshalVector(info, vec)
				if err != nil {
					b.Fatal(err)
				}
				// Simulate framer consuming the buffer, then returning it to pool
				putVectorBuf(data)
			}
		})
	}
}

func makeDoubleVectorType(dim int, dimStr string) VectorType {
	return VectorType{
		NativeType: NativeType{
			proto:  protoVersion4,
			typ:    TypeCustom,
			custom: apacheCassandraTypePrefix + "VectorType(" + apacheCassandraTypePrefix + "DoubleType, " + dimStr + ")",
		},
		SubType:    NativeType{proto: protoVersion4, typ: TypeDouble},
		Dimensions: dim,
	}
}

// BenchmarkMarshalVectorFloat64 measures marshal performance for float64 vectors.
func BenchmarkMarshalVectorFloat64(b *testing.B) {
	dims := []struct {
		dim    int
		dimStr string
	}{
		{dim: 128, dimStr: "128"},
		{dim: 384, dimStr: "384"},
		{dim: 768, dimStr: "768"},
		{dim: 1536, dimStr: "1536"},
	}

	for _, entry := range dims {
		dim := entry.dim
		dimStr := entry.dimStr
		b.Run("dim_"+dimStr, func(b *testing.B) {
			b.ReportAllocs()

			vec := make([]float64, dim)
			for i := range vec {
				vec[i] = float64(i) * 0.1
			}

			info := makeDoubleVectorType(dim, dimStr)

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

// BenchmarkVectorWritePath simulates the full marshal + framer.writeBytes path,
// measuring total latency and allocations as seen in the real CQL write path.
func BenchmarkVectorWritePath(b *testing.B) {
	dims := []struct {
		dim    int
		dimStr string
	}{
		{dim: 128, dimStr: "128"},
		{dim: 384, dimStr: "384"},
		{dim: 768, dimStr: "768"},
		{dim: 1536, dimStr: "1536"},
	}

	for _, entry := range dims {
		dim := entry.dim
		dimStr := entry.dimStr
		b.Run("dim_"+dimStr, func(b *testing.B) {
			b.ReportAllocs()

			vec := make([]float32, dim)
			for i := range vec {
				vec[i] = float32(i) * 0.1
			}

			info := makeFloatVectorType(dim, dimStr)

			// Pre-allocate a framer to simulate the write path
			f := newFramer(nil, protoVersion4)

			b.SetBytes(int64(dim * 4))
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				// 1. Marshal the vector
				data, err := marshalVector(info, vec)
				if err != nil {
					b.Fatal(err)
				}
				// 2. Write into framer (simulates writeBytes in writeQueryParams)
				f.buf = f.buf[:0] // reset framer buffer
				f.writeBytes(data)
				// 3. Return pooled buffer
				putVectorBuf(data)
			}
		})
	}
}

// BenchmarkVectorRoundTripPooled measures full marshal -> unmarshal with pool return.
func BenchmarkVectorRoundTripPooled(b *testing.B) {
	dims := []struct {
		dim    int
		dimStr string
	}{
		{dim: 128, dimStr: "128"},
		{dim: 384, dimStr: "384"},
		{dim: 768, dimStr: "768"},
		{dim: 1536, dimStr: "1536"},
	}

	for _, entry := range dims {
		dim := entry.dim
		dimStr := entry.dimStr
		b.Run("dim_"+dimStr, func(b *testing.B) {
			b.ReportAllocs()

			srcVec := make([]float32, dim)
			for i := range srcVec {
				srcVec[i] = float32(i) * 0.1
			}

			info := makeFloatVectorType(dim, dimStr)
			var dstVec []float32

			b.SetBytes(int64(dim * 4 * 2))
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				data, err := marshalVector(info, srcVec)
				if err != nil {
					b.Fatal(err)
				}
				if err := unmarshalVector(info, data, &dstVec); err != nil {
					b.Fatal(err)
				}
				putVectorBuf(data)
			}
		})
	}
}

func makeIntVectorType(dim int, dimStr string) VectorType {
	return VectorType{
		NativeType: NativeType{
			proto:  protoVersion4,
			typ:    TypeCustom,
			custom: apacheCassandraTypePrefix + "VectorType(" + apacheCassandraTypePrefix + "Int32Type, " + dimStr + ")",
		},
		SubType:    NativeType{proto: protoVersion4, typ: TypeInt},
		Dimensions: dim,
	}
}

func makeBigIntVectorType(dim int, dimStr string) VectorType {
	return VectorType{
		NativeType: NativeType{
			proto:  protoVersion4,
			typ:    TypeCustom,
			custom: apacheCassandraTypePrefix + "VectorType(" + apacheCassandraTypePrefix + "LongType, " + dimStr + ")",
		},
		SubType:    NativeType{proto: protoVersion4, typ: TypeBigInt},
		Dimensions: dim,
	}
}

// BenchmarkMarshalVectorInt32 measures marshal performance for int32 vectors.
func BenchmarkMarshalVectorInt32(b *testing.B) {
	dims := []struct {
		dim    int
		dimStr string
	}{
		{dim: 128, dimStr: "128"},
		{dim: 384, dimStr: "384"},
		{dim: 768, dimStr: "768"},
		{dim: 1536, dimStr: "1536"},
	}

	for _, entry := range dims {
		dim := entry.dim
		dimStr := entry.dimStr
		b.Run("dim_"+dimStr, func(b *testing.B) {
			b.ReportAllocs()

			vec := make([]int32, dim)
			for i := range vec {
				vec[i] = int32(i) * 7
			}

			info := makeIntVectorType(dim, dimStr)

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

// BenchmarkMarshalVectorInt32Pooled measures marshal with buffer pool return for int32.
func BenchmarkMarshalVectorInt32Pooled(b *testing.B) {
	dims := []struct {
		dim    int
		dimStr string
	}{
		{dim: 128, dimStr: "128"},
		{dim: 384, dimStr: "384"},
		{dim: 768, dimStr: "768"},
		{dim: 1536, dimStr: "1536"},
	}

	for _, entry := range dims {
		dim := entry.dim
		dimStr := entry.dimStr
		b.Run("dim_"+dimStr, func(b *testing.B) {
			b.ReportAllocs()

			vec := make([]int32, dim)
			for i := range vec {
				vec[i] = int32(i) * 7
			}

			info := makeIntVectorType(dim, dimStr)

			b.SetBytes(int64(dim * 4))
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				data, err := marshalVector(info, vec)
				if err != nil {
					b.Fatal(err)
				}
				putVectorBuf(data)
			}
		})
	}
}

// BenchmarkUnmarshalVectorInt32 measures unmarshal performance for int32 vectors.
func BenchmarkUnmarshalVectorInt32(b *testing.B) {
	dims := []struct {
		dim    int
		dimStr string
	}{
		{dim: 128, dimStr: "128"},
		{dim: 384, dimStr: "384"},
		{dim: 768, dimStr: "768"},
		{dim: 1536, dimStr: "1536"},
	}

	for _, entry := range dims {
		dim := entry.dim
		dimStr := entry.dimStr
		b.Run("dim_"+dimStr, func(b *testing.B) {
			b.ReportAllocs()

			data := make([]byte, dim*4)
			for i := 0; i < dim; i++ {
				binary.BigEndian.PutUint32(data[i*4:], uint32(int32(i)*7))
			}

			info := makeIntVectorType(dim, dimStr)
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

// BenchmarkMarshalVectorInt64 measures marshal performance for int64 vectors.
func BenchmarkMarshalVectorInt64(b *testing.B) {
	dims := []struct {
		dim    int
		dimStr string
	}{
		{dim: 128, dimStr: "128"},
		{dim: 384, dimStr: "384"},
		{dim: 768, dimStr: "768"},
		{dim: 1536, dimStr: "1536"},
	}

	for _, entry := range dims {
		dim := entry.dim
		dimStr := entry.dimStr
		b.Run("dim_"+dimStr, func(b *testing.B) {
			b.ReportAllocs()

			vec := make([]int64, dim)
			for i := range vec {
				vec[i] = int64(i) * 7
			}

			info := makeBigIntVectorType(dim, dimStr)

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

// BenchmarkMarshalVectorInt64Pooled measures marshal with buffer pool return for int64.
func BenchmarkMarshalVectorInt64Pooled(b *testing.B) {
	dims := []struct {
		dim    int
		dimStr string
	}{
		{dim: 128, dimStr: "128"},
		{dim: 384, dimStr: "384"},
		{dim: 768, dimStr: "768"},
		{dim: 1536, dimStr: "1536"},
	}

	for _, entry := range dims {
		dim := entry.dim
		dimStr := entry.dimStr
		b.Run("dim_"+dimStr, func(b *testing.B) {
			b.ReportAllocs()

			vec := make([]int64, dim)
			for i := range vec {
				vec[i] = int64(i) * 7
			}

			info := makeBigIntVectorType(dim, dimStr)

			b.SetBytes(int64(dim * 8))
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				data, err := marshalVector(info, vec)
				if err != nil {
					b.Fatal(err)
				}
				putVectorBuf(data)
			}
		})
	}
}

// BenchmarkUnmarshalVectorInt64 measures unmarshal performance for int64 vectors.
func BenchmarkUnmarshalVectorInt64(b *testing.B) {
	dims := []struct {
		dim    int
		dimStr string
	}{
		{dim: 128, dimStr: "128"},
		{dim: 384, dimStr: "384"},
		{dim: 768, dimStr: "768"},
		{dim: 1536, dimStr: "1536"},
	}

	for _, entry := range dims {
		dim := entry.dim
		dimStr := entry.dimStr
		b.Run("dim_"+dimStr, func(b *testing.B) {
			b.ReportAllocs()

			data := make([]byte, dim*8)
			for i := 0; i < dim; i++ {
				binary.BigEndian.PutUint64(data[i*8:], uint64(int64(i)*7))
			}

			info := makeBigIntVectorType(dim, dimStr)
			var result []int64

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

func makeUUIDBenchVectorType(dim int, dimStr string) VectorType {
	return VectorType{
		NativeType: NativeType{
			proto:  protoVersion4,
			typ:    TypeCustom,
			custom: apacheCassandraTypePrefix + "VectorType(" + apacheCassandraTypePrefix + "UUIDType, " + dimStr + ")",
		},
		SubType:    NativeType{proto: protoVersion4, typ: TypeUUID},
		Dimensions: dim,
	}
}

// BenchmarkMarshalVectorUUID measures marshal performance for UUID vectors.
func BenchmarkMarshalVectorUUID(b *testing.B) {
	dims := []struct {
		dim    int
		dimStr string
	}{
		{dim: 128, dimStr: "128"},
		{dim: 384, dimStr: "384"},
		{dim: 768, dimStr: "768"},
		{dim: 1536, dimStr: "1536"},
	}

	for _, entry := range dims {
		dim := entry.dim
		dimStr := entry.dimStr
		b.Run("dim_"+dimStr, func(b *testing.B) {
			b.ReportAllocs()

			vec := make([]UUID, dim)
			for i := range vec {
				vec[i][0] = byte(i)
				vec[i][1] = byte(i >> 8)
			}

			info := makeUUIDBenchVectorType(dim, dimStr)

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

// BenchmarkMarshalVectorUUIDPooled measures marshal with buffer pool return for UUID.
func BenchmarkMarshalVectorUUIDPooled(b *testing.B) {
	dims := []struct {
		dim    int
		dimStr string
	}{
		{dim: 128, dimStr: "128"},
		{dim: 384, dimStr: "384"},
		{dim: 768, dimStr: "768"},
		{dim: 1536, dimStr: "1536"},
	}

	for _, entry := range dims {
		dim := entry.dim
		dimStr := entry.dimStr
		b.Run("dim_"+dimStr, func(b *testing.B) {
			b.ReportAllocs()

			vec := make([]UUID, dim)
			for i := range vec {
				vec[i][0] = byte(i)
				vec[i][1] = byte(i >> 8)
			}

			info := makeUUIDBenchVectorType(dim, dimStr)

			b.SetBytes(int64(dim * 16))
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				data, err := marshalVector(info, vec)
				if err != nil {
					b.Fatal(err)
				}
				putVectorBuf(data)
			}
		})
	}
}

// BenchmarkUnmarshalVectorUUID measures unmarshal performance for UUID vectors.
func BenchmarkUnmarshalVectorUUID(b *testing.B) {
	dims := []struct {
		dim    int
		dimStr string
	}{
		{dim: 128, dimStr: "128"},
		{dim: 384, dimStr: "384"},
		{dim: 768, dimStr: "768"},
		{dim: 1536, dimStr: "1536"},
	}

	for _, entry := range dims {
		dim := entry.dim
		dimStr := entry.dimStr
		b.Run("dim_"+dimStr, func(b *testing.B) {
			b.ReportAllocs()

			data := make([]byte, dim*16)
			for i := 0; i < dim; i++ {
				data[i*16] = byte(i)
				data[i*16+1] = byte(i >> 8)
			}

			info := makeUUIDBenchVectorType(dim, dimStr)
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
