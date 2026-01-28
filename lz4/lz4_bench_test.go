//go:build bench
// +build bench

package lz4

import (
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
