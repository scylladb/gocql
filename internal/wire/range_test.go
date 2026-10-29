package wire

import (
	"bytes"
	"fmt"
	"testing"
)

func TestNestedRangesPreserveEndpointTrims(t *testing.T) {
	data := []byte("0123456789abcdef")
	var spans []Span
	for i := range data {
		spans = append(spans, Borrow(data[i:i+1]))
	}
	chain, err := NewChain(spans...)
	if err != nil {
		t.Fatal(err)
	}
	for off := 0; off <= len(data); off++ {
		for n := 0; n <= len(data)-off; n++ {
			outer, err := chain.Slice(int64(off), int64(n))
			if err != nil {
				t.Fatal(err)
			}
			for innerOff := 0; innerOff <= n; innerOff++ {
				for innerN := 0; innerN <= n-innerOff; innerN++ {
					inner, err := outer.Slice(int64(innerOff), int64(innerN))
					if err != nil {
						t.Fatal(err)
					}
					var got []byte
					inner.Each(func(s Span) error { got = append(got, s.Bytes()...); return nil })
					if !bytes.Equal(got, data[off+innerOff:off+innerOff+innerN]) {
						t.Fatalf("off=%d n=%d inner=%d,%d got=%q", off, n, innerOff, innerN, got)
					}
					cursor := NewCursor(outer)
					if err := cursor.Skip(int64(innerOff)); err != nil {
						t.Fatal(err)
					}
					part, err := cursor.Take(int64(innerN))
					if err != nil {
						t.Fatal(err)
					}
					copyOut := make([]byte, innerN)
					if _, err := part.CopyTo(copyOut); err != nil {
						t.Fatal(err)
					}
					if !bytes.Equal(got, copyOut) {
						t.Fatal("cursor range differs")
					}
				}
			}
		}
	}
}

func TestSegmentedRangeViewsDoNotAllocate(t *testing.T) {
	data := make([]byte, 256)
	spans := make([]Span, len(data))
	for i := range data {
		spans[i] = Borrow(data[i : i+1])
	}
	chain, err := NewChain(spans...)
	if err != nil {
		t.Fatal(err)
	}
	allocs := testing.AllocsPerRun(1000, func() {
		view, err := chain.Slice(1, 254)
		if err != nil {
			panic(err)
		}
		cursor := NewCursor(view)
		for cursor.Remaining() >= 7 {
			part, err := cursor.Take(7)
			if err != nil || part.Len() != 7 {
				panic("range")
			}
		}
	})
	if allocs != 0 {
		t.Fatalf("descriptor-only ranges allocate: %v", allocs)
	}
}

func TestRangeIndexIsSharedAndOrdered(t *testing.T) {
	data := make([]byte, 65536)
	spans := make([]Span, len(data))
	for i := range spans {
		spans[i] = Borrow(data[i : i+1])
	}
	c, _ := NewChain(spans...)
	for i, p := range c.tail {
		if p.end != int64(i+2) {
			t.Fatal("bad prefix index")
		}
	}
	v, e := c.Slice(32000, 10000)
	if e != nil {
		t.Fatal(e)
	}
	if &v.tail[0] != &c.tail[32000] {
		t.Fatal("child copied index")
	}
	for _, off := range []int64{0, 1, 1024, 9998, 9999} {
		part, e := v.Slice(off, 1)
		if e != nil || &part.head.data[0] != &data[32000+off] {
			t.Fatal("indexed range changed backing")
		}
	}
}
func BenchmarkRangeLookupScaling(b *testing.B) {
	for _, fragments := range []int{1, 1024, 65536} {
		b.Run(fmt.Sprint(fragments), func(b *testing.B) {
			data := make([]byte, 65536)
			spans := make([]Span, fragments)
			width := len(data) / fragments
			for i := range spans {
				spans[i] = Borrow(data[i*width : (i+1)*width])
			}
			c, _ := NewChain(spans...)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				off := (i * 7919) % (len(data) - 16)
				v, e := c.Slice(int64(off), 16)
				if e != nil || v.Len() != 16 {
					b.Fatal(e)
				}
			}
		})
	}
}
func BenchmarkCursorCellsAndFragments(b *testing.B) {
	for _, cells := range []int{256, 8192} {
		for _, fragments := range []int{1, 256, 65536} {
			b.Run(fmt.Sprintf("cells=%d/fragments=%d", cells, fragments), func(b *testing.B) {
				data := make([]byte, 65536)
				spans := make([]Span, fragments)
				width := len(data) / fragments
				for i := range spans {
					spans[i] = Borrow(data[i*width : (i+1)*width])
				}
				c, _ := NewChain(spans...)
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					cursor := NewCursor(c)
					for j := 0; j < cells; j++ {
						if _, e := cursor.Take(int64(len(data) / cells)); e != nil {
							b.Fatal(e)
						}
					}
				}
			})
		}
	}
}
