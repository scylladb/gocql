//go:build bench
// +build bench

package lz4

import (
	"fmt"
	"math/rand"
	"testing"
)

func BenchmarkLZ4Compressor(b *testing.B) {
	original := []byte("My Test String")
	var c LZ4Compressor

	b.Run("Encode", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_, err := c.Encode(original)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkLZ4Decode(b *testing.B) {
	var c LZ4Compressor

	for _, size := range []int{1024, 8192, 32768, 131072} {
		// Create compressible data (repeated patterns)
		original := make([]byte, size)
		rng := rand.New(rand.NewSource(42))
		for i := range original {
			original[i] = byte(rng.Intn(26) + 'a')
		}

		compressed, err := c.Encode(original)
		if err != nil {
			b.Fatal(err)
		}

		b.Run(fmt.Sprintf("Decode/size=%dKB", size/1024), func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(size))
			for i := 0; i < b.N; i++ {
				_, err := c.Decode(compressed)
				if err != nil {
					b.Fatal(err)
				}
			}
		})

		b.Run(fmt.Sprintf("DecodeInto/size=%dKB", size/1024), func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(size))
			dst := make([]byte, 0, size)
			for i := 0; i < b.N; i++ {
				dst, err = c.DecodeInto(compressed, dst)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
