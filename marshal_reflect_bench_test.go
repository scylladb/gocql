//go:build unit

package gocql

import (
	"testing"
	"time"
)

// BenchmarkMarshalReflectFastPath benchmarks Marshal with common types
// to measure the benefit of skipping reflect.ValueOf for known non-pointer types.
func BenchmarkMarshalReflectFastPath(b *testing.B) {
	info := NativeType{proto: 4, typ: TypeBigInt}

	benchmarks := []struct {
		name  string
		info  TypeInfo
		value any
	}{
		{"int64", NativeType{proto: 4, typ: TypeBigInt}, int64(42)},
		{"*int64", NativeType{proto: 4, typ: TypeBigInt}, ptrInt64(42)},
		{"string", NativeType{proto: 4, typ: TypeVarchar}, "hello world"},
		{"*string", NativeType{proto: 4, typ: TypeVarchar}, ptrString("hello world")},
		{"[]byte", NativeType{proto: 4, typ: TypeBlob}, []byte("binary data")},
		{"bool", NativeType{proto: 4, typ: TypeBoolean}, true},
		{"float64", NativeType{proto: 4, typ: TypeDouble}, 3.14},
		{"time.Time", NativeType{proto: 4, typ: TypeTimestamp}, time.Now()},
	}

	_ = info
	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_, err := Marshal(bm.info, bm.value)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func ptrInt64(v int64) *int64    { return &v }
func ptrString(v string) *string { return &v }
