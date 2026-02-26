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
			typ:    TypeCustom,
			custom: vectorTypePrefix + dimStr + vectorTypeSuffix,
		},
		SubType:    NativeType{typ: TypeFloat},
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
