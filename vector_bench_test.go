// Copyright (c) 2012 The gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package gocql

import (
	"encoding/binary"
	"fmt"
	"math"
	"testing"
)

// BenchmarkUnmarshalVectorFloat32 measures unmarshal performance for float32 vectors
// across common embedding dimensions used in AI/ML applications
func BenchmarkUnmarshalVectorFloat32(b *testing.B) {
	dims := []int{128, 384, 768, 1536}

	for _, dim := range dims {
		b.Run(fmt.Sprintf("dim_%d", dim), func(b *testing.B) {
			b.ReportAllocs()

			// Prepare test data - vector of floats
			data := make([]byte, dim*4)
			for i := 0; i < dim; i++ {
				binary.BigEndian.PutUint32(data[i*4:], math.Float32bits(float32(i)*0.1))
			}

			info := VectorType{
				NativeType: NativeType{proto: protoVersion4, typ: TypeCustom, custom: "org.apache.cassandra.db.marshal.VectorType(org.apache.cassandra.db.marshal.FloatType, " + fmt.Sprintf("%d", dim) + ")"},
				SubType:    NativeType{proto: protoVersion4, typ: TypeFloat},
				Dimensions: dim,
			}

			var result []float32

			b.SetBytes(int64(dim * 4))
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				err := unmarshalVector(info, data, &result)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkUnmarshalVectorFloat32ViaUnmarshal measures the overhead of the public Unmarshal()
// dispatcher when decoding float32 vectors.
func BenchmarkUnmarshalVectorFloat32ViaUnmarshal(b *testing.B) {
	dims := []int{128, 384, 768, 1536}

	for _, dim := range dims {
		b.Run(fmt.Sprintf("dim_%d", dim), func(b *testing.B) {
			b.ReportAllocs()

			data := make([]byte, dim*4)
			for i := 0; i < dim; i++ {
				binary.BigEndian.PutUint32(data[i*4:], math.Float32bits(float32(i)*0.1))
			}

			info := VectorType{
				NativeType: NativeType{proto: protoVersion4, typ: TypeCustom, custom: "org.apache.cassandra.db.marshal.VectorType(org.apache.cassandra.db.marshal.FloatType, " + fmt.Sprintf("%d", dim) + ")"},
				SubType:    NativeType{proto: protoVersion4, typ: TypeFloat},
				Dimensions: dim,
			}

			var result []float32
			b.SetBytes(int64(dim * 4))
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				if err := Unmarshal(info, data, &result); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkUnmarshalVectorFloat32ViaUnmarshal_NativeType verifies and measures the path when the
// schema type is a TypeCustom NativeType with VectorType(...) custom string (not a concrete VectorType).
func BenchmarkUnmarshalVectorFloat32ViaUnmarshal_NativeType(b *testing.B) {
	dims := []int{128, 384, 768, 1536}

	for _, dim := range dims {
		b.Run(fmt.Sprintf("dim_%d", dim), func(b *testing.B) {
			b.ReportAllocs()

			data := make([]byte, dim*4)
			for i := 0; i < dim; i++ {
				binary.BigEndian.PutUint32(data[i*4:], math.Float32bits(float32(i)*0.1))
			}

			info := NewCustomType(protoVersion4, TypeCustom,
				"org.apache.cassandra.db.marshal.VectorType(org.apache.cassandra.db.marshal.FloatType, "+fmt.Sprintf("%d", dim)+")")

			var result []float32
			b.SetBytes(int64(dim * 4))
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				if err := Unmarshal(info, data, &result); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkUnmarshalVectorFloat64ViaUnmarshal measures dispatcher overhead when decoding float64 vectors.
func BenchmarkUnmarshalVectorFloat64ViaUnmarshal(b *testing.B) {
	dims := []int{128, 384, 768, 1536}

	for _, dim := range dims {
		b.Run(fmt.Sprintf("dim_%d", dim), func(b *testing.B) {
			b.ReportAllocs()

			data := make([]byte, dim*8)
			for i := 0; i < dim; i++ {
				binary.BigEndian.PutUint64(data[i*8:], math.Float64bits(float64(i)*0.1))
			}

			info := VectorType{
				NativeType: NativeType{proto: protoVersion4, typ: TypeCustom, custom: "org.apache.cassandra.db.marshal.VectorType(org.apache.cassandra.db.marshal.DoubleType, " + fmt.Sprintf("%d", dim) + ")"},
				SubType:    NativeType{proto: protoVersion4, typ: TypeDouble},
				Dimensions: dim,
			}

			var result []float64
			b.SetBytes(int64(dim * 8))
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				if err := Unmarshal(info, data, &result); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkUnmarshalVectorFloat64ViaUnmarshal_NativeType measures the TypeCustom NativeType VectorType(...) path.
func BenchmarkUnmarshalVectorFloat64ViaUnmarshal_NativeType(b *testing.B) {
	dims := []int{128, 384, 768, 1536}

	for _, dim := range dims {
		b.Run(fmt.Sprintf("dim_%d", dim), func(b *testing.B) {
			b.ReportAllocs()

			data := make([]byte, dim*8)
			for i := 0; i < dim; i++ {
				binary.BigEndian.PutUint64(data[i*8:], math.Float64bits(float64(i)*0.1))
			}

			info := NewCustomType(protoVersion4, TypeCustom,
				"org.apache.cassandra.db.marshal.VectorType(org.apache.cassandra.db.marshal.DoubleType, "+fmt.Sprintf("%d", dim)+")")

			var result []float64
			b.SetBytes(int64(dim * 8))
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				if err := Unmarshal(info, data, &result); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkMarshalVectorFloat32 measures marshal performance for float32 vectors
// across common embedding dimensions
func BenchmarkMarshalVectorFloat32(b *testing.B) {
	dims := []int{128, 384, 768, 1536}

	for _, dim := range dims {
		b.Run(fmt.Sprintf("dim_%d", dim), func(b *testing.B) {
			b.ReportAllocs()

			// Prepare test vector
			vec := make([]float32, dim)
			for i := range vec {
				vec[i] = float32(i) * 0.1
			}

			info := VectorType{
				NativeType: NativeType{proto: protoVersion4, typ: TypeCustom, custom: "org.apache.cassandra.db.marshal.VectorType(org.apache.cassandra.db.marshal.FloatType, " + fmt.Sprintf("%d", dim) + ")"},
				SubType:    NativeType{proto: protoVersion4, typ: TypeFloat},
				Dimensions: dim,
			}

			b.SetBytes(int64(dim * 4))
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				_, err := marshalVector(info, vec)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkVectorRoundTrip measures full marshal->unmarshal cycle
func BenchmarkVectorRoundTrip(b *testing.B) {
	dims := []int{128, 384, 768, 1536}

	for _, dim := range dims {
		b.Run(fmt.Sprintf("dim_%d", dim), func(b *testing.B) {
			b.ReportAllocs()

			// Prepare test vector
			srcVec := make([]float32, dim)
			for i := range srcVec {
				srcVec[i] = float32(i) * 0.1
			}

			info := VectorType{
				NativeType: NativeType{proto: protoVersion4, typ: TypeCustom, custom: "org.apache.cassandra.db.marshal.VectorType(org.apache.cassandra.db.marshal.FloatType, " + fmt.Sprintf("%d", dim) + ")"},
				SubType:    NativeType{proto: protoVersion4, typ: TypeFloat},
				Dimensions: dim,
			}

			var dstVec []float32

			b.SetBytes(int64(dim * 4 * 2)) // marshal + unmarshal
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				data, err := marshalVector(info, srcVec)
				if err != nil {
					b.Fatal(err)
				}

				err = unmarshalVector(info, data, &dstVec)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
