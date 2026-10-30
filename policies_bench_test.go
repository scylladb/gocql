//go:build unit
// +build unit

package gocql

import (
	"testing"
)

// BenchmarkHostIdComparison is a micro-benchmark for isolated host-ID
// comparisons: string==string (current) baseline.
func BenchmarkHostIdComparison(b *testing.B) {
	id1 := "00000000-0000-0000-0000-000000000001"
	id2 := "00000000-0000-0000-0000-000000000001"

	b.Run("StringEqual", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			if id1 != id2 {
				b.Fatal("should be equal")
			}
		}
	})

	// Benchmark with UUIDs for comparison (what the proposed change uses).
	uuid1 := UUID{}
	uuid1[15] = 1
	uuid2 := UUID{}
	uuid2[15] = 1

	b.Run("UUIDEqual", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			if uuid1 != uuid2 {
				b.Fatal("should be equal")
			}
		}
	})
}
