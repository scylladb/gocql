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
			if _, err := c.Encode(original); err != nil {
				b.Fatal(err)
			}
		}
	})

	compressed, err := c.Encode(original)
	if err != nil {
		b.Fatal(err)
	}

	b.Run("Decode", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			if _, err := c.Decode(compressed); err != nil {
				b.Fatal(err)
			}
		}
	})
}
