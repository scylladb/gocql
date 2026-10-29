/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package wire

import (
	"bytes"
	"errors"
	"io"
	"math"
	"testing"
	"unsafe"
)

func chain(t testing.TB, spans ...Span) Chain {
	t.Helper()
	c, err := NewChain(spans...)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func payload(t testing.TB, c Chain) []byte {
	t.Helper()
	b := make([]byte, c.Len())
	if n, err := c.CopyTo(b); err != nil || int64(n) != c.Len() {
		t.Fatalf("CopyTo: %d, %v; length %d", n, err, c.Len())
	}
	return b
}

func TestSpanBorrowAndBounds(t *testing.T) {
	data := []byte{1, 2, 3, 4}
	s := Borrow(data)
	if s.Len() != 4 || &s.Bytes()[0] != &data[0] || cap(s.Bytes()) != 4 {
		t.Fatal("Borrow did not retain a bounded view of the original bytes")
	}
	sub, err := s.Slice(1, 2)
	if err != nil || sub.Len() != 2 || &sub.Bytes()[0] != &data[1] || cap(sub.Bytes()) != 2 {
		t.Fatalf("Slice did not preserve payload identity: %v", err)
	}
	for _, pair := range [][2]int{{-1, 0}, {0, -1}, {5, 0}, {3, 2}, {math.MaxInt, 1}, {1, math.MaxInt}} {
		if _, err := s.Slice(pair[0], pair[1]); !errors.Is(err, ErrBounds) {
			t.Fatalf("Slice%v: %v", pair, err)
		}
	}
	if empty, err := s.Slice(4, 0); err != nil || empty.Len() != 0 {
		t.Fatalf("end slice: %v", err)
	}
}

func TestBorrowStringIsReadOnlyView(t *testing.T) {
	for _, original := range []string{"", "literal storage", string(bytes.Repeat([]byte{42}, 256))} {
		s := BorrowString(original)
		if s.Len() != len(original) || string(s.Bytes()) != original {
			t.Fatal("incorrect string view")
		}
		if len(original) > 0 && unsafe.SliceData(s.Bytes()) != unsafe.StringData(original) {
			t.Fatal("BorrowString copied string storage")
		}
		b := NewBuilder(4)
		mark := b.Mark()
		if err := b.Borrow(s); err != nil {
			t.Fatal(err)
		}
		if len(original) >= 4 && !errors.Is(b.PatchUint32(mark, 123), ErrBorrowedPatch) {
			t.Fatal("builder allowed patching string storage")
		}
		frozen, err := b.Freeze()
		if err != nil || string(payload(t, frozen)) != original {
			t.Fatalf("builder changed string storage: %v", err)
		}
	}
}

func TestChainOwnsDescriptorsAndSharesPayload(t *testing.T) {
	a, b := []byte{1, 2}, []byte{3, 4, 5}
	descriptors := []Span{{}, Borrow(a), Borrow(nil), Borrow(b), {}}
	c := chain(t, descriptors...)
	clear(descriptors)
	if c.Len() != 5 || !bytes.Equal(payload(t, c), []byte{1, 2, 3, 4, 5}) {
		t.Fatal("chain did not retain its descriptors")
	}
	index := 0
	if err := c.Each(func(s Span) error {
		want := [][]byte{a, b}[index]
		if s.Len() == 0 || &s.Bytes()[0] != &want[0] {
			t.Fatal("chain copied payload or retained an empty descriptor")
		}
		index++
		return nil
	}); err != nil || index != 2 {
		t.Fatalf("Each: %v, %d spans", err, index)
	}
	stop := errors.New("stop")
	calls := 0
	if err := c.Each(func(Span) error { calls++; return stop }); err != stop || calls != 1 {
		t.Fatal("Each did not stop on visitor error")
	}
	if err := (Chain{}).Each(nil); err != nil {
		t.Fatal(err)
	}
}

func TestChainSliceAllBoundaries(t *testing.T) {
	data := []byte("abcdefghijk")
	c := chain(t, Borrow(data[:2]), Span{}, Borrow(data[2:3]), Borrow(data[3:8]), Borrow(data[8:]))
	for start := 0; start <= len(data); start++ {
		for end := start; end <= len(data); end++ {
			s, err := c.Slice(int64(start), int64(end-start))
			if err != nil || !bytes.Equal(payload(t, s), data[start:end]) {
				t.Fatalf("Slice(%d,%d): %v", start, end-start, err)
			}
			pos := start
			_ = s.Each(func(span Span) error {
				if &span.Bytes()[0] != &data[pos] {
					t.Fatalf("Slice(%d,%d) copied payload", start, end-start)
				}
				pos += span.Len()
				return nil
			})
		}
	}
	for _, pair := range [][2]int64{{-1, 0}, {0, -1}, {12, 0}, {10, 2}, {math.MaxInt64, 1}, {1, math.MaxInt64}} {
		if _, err := c.Slice(pair[0], pair[1]); !errors.Is(err, ErrBounds) {
			t.Fatalf("Slice%v: %v", pair, err)
		}
	}
	if _, err := addLength(math.MaxInt64, 1); !errors.Is(err, ErrOverflow) {
		t.Fatalf("length overflow: %v", err)
	}
}

func TestChainCopyTo(t *testing.T) {
	c := chain(t, BorrowString("abc"), BorrowString("def"))
	for size := 0; size <= 8; size++ {
		dst := bytes.Repeat([]byte{'!'}, size)
		n, err := c.CopyTo(dst)
		want := min(size, 6)
		if n != want || string(dst[:n]) != "abcdef"[:want] {
			t.Fatalf("size %d: copied %d bytes: %q", size, n, dst)
		}
		if size < 6 && !errors.Is(err, io.ErrShortBuffer) || size >= 6 && err != nil {
			t.Fatalf("size %d: %v", size, err)
		}
		if size > 6 && string(dst[6:]) != string(bytes.Repeat([]byte{'!'}, size-6)) {
			t.Fatal("CopyTo overwrote destination tail")
		}
	}
}

var allocationChain Chain
var allocationSpan Span

func TestSmallViewsDoNotAllocate(t *testing.T) {
	data := []byte("some existing payload")
	if n := testing.AllocsPerRun(1000, func() {
		s := Borrow(data)
		c, _ := NewChain(s)
		allocationChain, _ = c.Slice(2, 8)
		allocationSpan, _ = s.Slice(2, 8)
	}); n != 0 {
		t.Fatalf("small borrowed views allocated %v times", n)
	}
	if n := testing.AllocsPerRun(1000, func() { allocationSpan = BorrowString("read-only literal") }); n != 0 {
		t.Fatalf("string view allocated %v times", n)
	}
}

func BenchmarkBorrowLargePayload(b *testing.B) {
	data := make([]byte, 1<<20)
	b.ReportAllocs()
	b.SetBytes(int64(len(data)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		allocationChain, _ = NewChain(Borrow(data))
	}
}

func BenchmarkChainSlice(b *testing.B) {
	data := make([]byte, 1<<20)
	c := chain(b, Borrow(data[:1<<19]), Borrow(data[1<<19:]))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		allocationChain, _ = c.Slice(1, int64(len(data)-2))
	}
}
