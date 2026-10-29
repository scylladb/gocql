package murmur

import (
	"fmt"
	"math/rand"
	"testing"
)

func TestStreamMatchesContiguousAtEveryBoundary(t *testing.T) {
	rng := rand.New(rand.NewSource(42))
	for n := 0; n < 256; n++ {
		data := make([]byte, n)
		rng.Read(data)
		want := Murmur3H1(data)
		for split := 0; split <= n; split++ {
			var s Stream
			s.Write(data[:split])
			if got := s.Sum64(); got != Murmur3H1(data[:split]) {
				t.Fatalf("prefix n=%d split=%d got=%d", n, split, got)
			}
			s.Write(data[split:])
			if got := s.Sum64(); got != want {
				t.Fatalf("n=%d split=%d got=%d want=%d", n, split, got, want)
			}
			if got := s.Sum64(); got != want {
				t.Fatal("Sum64 consumed state")
			}
		}
	}
}

func TestStreamSingleBytesAndLargeSpans(t *testing.T) {
	data := make([]byte, 1<<20)
	rand.New(rand.NewSource(9)).Read(data)
	for _, size := range []int{1, 3, 15, 16, 17, 1024, 65535, len(data)} {
		var s Stream
		for i := 0; i < len(data); i += size {
			s.Write(data[i:min(i+size, len(data))])
		}
		if s.Sum64() != Murmur3H1(data) {
			t.Fatalf("span size %d", size)
		}
	}
}

func BenchmarkStreamSegments(b *testing.B) {
	for _, size := range []int{1024, 1 << 20} {
		for _, spans := range []int{1, 16, 1024} {
			b.Run(fmt.Sprintf("bytes=%d/spans=%d", size, spans), func(b *testing.B) {
				data := make([]byte, size)
				width := size / spans
				b.SetBytes(int64(size))
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					var s Stream
					for j := 0; j < len(data); j += width {
						s.Write(data[j:min(j+width, len(data))])
					}
					_ = s.Sum64()
				}
			})
		}
	}
}
