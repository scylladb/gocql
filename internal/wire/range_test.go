package wire

import (
	"bytes"
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
