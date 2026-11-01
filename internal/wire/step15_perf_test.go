//go:build gocql_perf

// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package wire

import (
	"fmt"
	"math/bits"
	"testing"
)

func TestStep15TraversalCounts(t *testing.T) {
	const bytes = 1 << 20
	for _, n := range []int{1, 256, 8192} {
		for _, s := range []int{1, 256, 65536} {
			t.Run(fmt.Sprintf("cells%d/fragments%d", n, s), func(t *testing.T) {
				data := make([]byte, bytes)
				spans := make([]Span, s)
				for i := range spans {
					spans[i] = Borrow(data[i*bytes/s : (i+1)*bytes/s])
				}
				ResetPerfCounters()
				chain, err := NewChain(spans...)
				if err != nil {
					t.Fatal(err)
				}
				constructed := ReadPerfCounters()
				if constructed.DescriptorVisits != int64(2*s) {
					t.Fatal(constructed)
				}
				owner := NewLease(chain, nil)
				ResetPerfCounters()
				cursor := NewCursor(chain)
				for i := 0; i < n; i++ {
					v, e := cursor.Take(bytes / int64(n))
					if e != nil || v.Len() != bytes/int64(n) {
						t.Fatal(e)
					}
				}
				c := ReadPerfCounters()
				if c.Takes != int64(n) || c.SequentialVisits > int64(n+s) || c.IndexComparisons != 0 || c.CopyBytes != 0 || c.Retains != 0 || c.Releases != 0 {
					t.Fatal(c)
				}
				t.Logf("N=%d S=%d build=%+v sequential=%+v shared_index_bytes=%d", n, s, constructed, c, 32*(s-1))
				for _, depth := range []int{1, 4, 16} {
					ResetPerfCounters()
					jumps := 0
					for j := 0; j < 128; j++ {
						view := chain
						for d := 0; d < depth; d++ {
							var e error
							view, e = view.Slice(int64((j*7919+d)%1024), view.Len()-1024)
							if e != nil {
								t.Fatal(e)
							}
							jumps++
						}
					}
					c = ReadPerfCounters()
					if c.IndexComparisons > int64(2*jumps*(bits.Len(uint(s))+1)) || c.CopyBytes != 0 || c.Retains != 0 {
						t.Fatal(c)
					}
					t.Logf("depth=%d J=%d random=%+v", depth, jumps, c)
				}
				owner.Release()
			})
		}
	}
}
func TestStep15ScalarArenaCounts(t *testing.T) {
	const count = 1024
	b := NewBuilder(8 * count)
	ResetPerfCounters()
	for i := 0; i < count; i++ {
		if err := b.Uint64(uint64(i)); err != nil {
			t.Fatal(err)
		}
	}
	chain, e := b.Freeze()
	if e != nil {
		t.Fatal(e)
	}
	c := ReadPerfCounters()
	if c.ScalarWrites != count || c.ScalarBytes != 8*count || c.CopyBytes != 0 || chain.count() != 1 {
		t.Fatal(c, chain.count())
	}
	t.Logf("%+v spans=%d", c, chain.count())
}

func TestStep15NestedCursorEndpoints(t *testing.T) {
	data := make([]byte, 100003)
	for i := range data {
		data[i] = byte(i)
	}
	for _, width := range []int{1, 7, 128} {
		var spans []Span
		for at := 0; at < len(data); at += width {
			spans = append(spans, Borrow(data[at:min(at+width, len(data))]))
		}
		source, _ := NewChain(spans...)
		for _, trim := range []int64{0, 13, 127} {
			chain, _ := source.Slice(trim, int64(len(data))-2*trim)
			cursor := NewCursor(chain)
			position := trim
			for _, size := range []int64{0, 1, 16, 33, 8193, cursor.Remaining() - 8243} {
				if size < 0 || size > cursor.Remaining() {
					continue
				}
				before := cursor.Remaining()
				view, e := cursor.TakeNested(size)
				if e != nil || view.Len() != size || cursor.Remaining() != before-size {
					t.Fatal(e)
				}
				index := position
				view.Each(func(s Span) error {
					if len(s.Bytes()) > 0 && &s.Bytes()[0] != &data[index] {
						t.Fatal("nested range changed backing")
					}
					index += int64(len(s.Bytes()))
					return nil
				})
				if index != position+size {
					t.Fatal("nested range lost bytes")
				}
				position += size
			}
			before := *cursor
			if _, e := cursor.TakeNested(cursor.Remaining() + 1); e == nil || cursor.remaining != before.remaining || cursor.index != before.index || cursor.offset != before.offset {
				t.Fatal("invalid nested take advanced")
			}
		}
	}
}
