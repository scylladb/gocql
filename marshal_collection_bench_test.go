package gocql

import (
	"bytes"
	"fmt"
	"testing"
)

// BenchmarkMarshalListInt32 benchmarks marshaling a list of int32 values.
// Exercises writeCollectionSize once for the list length, plus once per element.
func BenchmarkMarshalListInt32(b *testing.B) {
	for _, size := range []int{10, 100, 1000} {
		data := make([]int32, size)
		for i := range data {
			data[i] = int32(i)
		}
		info := CollectionType{
			NativeType: NativeType{typ: TypeList, proto: 4},
			Elem:       NativeType{typ: TypeInt, proto: 4},
		}
		b.Run(fmt.Sprintf("elems=%d", size), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_, err := Marshal(info, data)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkMarshalMapStringInt32 benchmarks marshaling a map[string]int32.
// Exercises writeCollectionSize once for the map length, plus twice per entry (key + value).
func BenchmarkMarshalMapStringInt32(b *testing.B) {
	for _, size := range []int{10, 100} {
		data := make(map[string]int32, size)
		for i := 0; i < size; i++ {
			data[fmt.Sprintf("key-%d", i)] = int32(i)
		}
		info := CollectionType{
			NativeType: NativeType{typ: TypeMap, proto: 4},
			Key:        NativeType{typ: TypeVarchar, proto: 4},
			Elem:       NativeType{typ: TypeInt, proto: 4},
		}
		b.Run(fmt.Sprintf("elems=%d", size), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_, err := Marshal(info, data)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkWriteCollectionSize micro-benchmarks the writeCollectionSize function directly.
func BenchmarkWriteCollectionSize(b *testing.B) {
	buf := &bytes.Buffer{}
	buf.Grow(64)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		buf.Reset()
		_ = writeCollectionSize(42, buf)
	}
}
