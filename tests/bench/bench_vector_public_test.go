package bench_test

import (
	"encoding/binary"
	"math"
	"strconv"
	"testing"

	"github.com/gocql/gocql"
)

const vectorProto = 4
const apacheCassandraTypePrefix = "org.apache.cassandra.db.marshal."
const vectorTypePrefix = apacheCassandraTypePrefix + "VectorType(" + apacheCassandraTypePrefix + "FloatType, "
const vectorTypeSuffix = ")"

func makeFloatVectorType(dim int) gocql.VectorType {
	dimStr := strconv.Itoa(dim)
	return gocql.VectorType{
		NativeType: gocql.NewCustomType(
			vectorProto,
			gocql.TypeCustom,
			vectorTypePrefix+dimStr+vectorTypeSuffix,
		),
		SubType:    gocql.NewNativeType(vectorProto, gocql.TypeFloat),
		Dimensions: dim,
	}
}

func BenchmarkVectorMarshalFloat32Public(b *testing.B) {
	dims := []int{128, 384, 768, 1536}

	for _, dim := range dims {
		dimStr := strconv.Itoa(dim)
		b.Run("dim_"+dimStr, func(b *testing.B) {
			b.ReportAllocs()

			vec := make([]float32, dim)
			for i := range vec {
				vec[i] = float32(i) * 0.1
			}

			info := makeFloatVectorType(dim)

			b.SetBytes(int64(dim * 4))
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				if _, err := gocql.Marshal(info, vec); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkVectorUnmarshalFloat32Public(b *testing.B) {
	dims := []int{128, 384, 768, 1536}

	for _, dim := range dims {
		dimStr := strconv.Itoa(dim)
		b.Run("dim_"+dimStr, func(b *testing.B) {
			b.ReportAllocs()

			data := make([]byte, dim*4)
			for i := 0; i < dim; i++ {
				binary.BigEndian.PutUint32(data[i*4:], math.Float32bits(float32(i)*0.1))
			}

			info := makeFloatVectorType(dim)
			var result []float32

			b.SetBytes(int64(dim * 4))
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				if err := gocql.Unmarshal(info, data, &result); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkVectorRoundTripPublic(b *testing.B) {
	dims := []int{128, 384, 768, 1536}

	for _, dim := range dims {
		dimStr := strconv.Itoa(dim)
		b.Run("dim_"+dimStr, func(b *testing.B) {
			b.ReportAllocs()

			srcVec := make([]float32, dim)
			for i := range srcVec {
				srcVec[i] = float32(i) * 0.1
			}

			info := makeFloatVectorType(dim)
			var dstVec []float32

			b.SetBytes(int64(dim * 4 * 2))
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				data, err := gocql.Marshal(info, srcVec)
				if err != nil {
					b.Fatal(err)
				}
				if err := gocql.Unmarshal(info, data, &dstVec); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
