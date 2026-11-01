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
)

func segmented(t testing.TB, data []byte, width int) Chain {
	t.Helper()
	var spans []Span
	for len(data) > 0 {
		n := min(width, len(data))
		spans = append(spans, Borrow(data[:n]))
		data = data[n:]
	}
	return chain(t, spans...)
}

func TestCursorScalarsAcrossEveryBoundary(t *testing.T) {
	data := []byte{0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc, 0xde, 0xf0}
	for split := 0; split <= len(data); split++ {
		c := chain(t, Borrow(data[:split]), Span{}, Borrow(data[split:]))
		for _, test := range []struct {
			n    int
			want uint64
		}{{1, 0x12}, {2, 0x1234}, {4, 0x12345678}, {8, 0x123456789abcdef0}} {
			cur := NewCursor(c)
			var got uint64
			var err error
			switch test.n {
			case 1:
				v, e := cur.Uint8()
				got, err = uint64(v), e
			case 2:
				v, e := cur.Uint16()
				got, err = uint64(v), e
			case 4:
				v, e := cur.Uint32()
				got, err = uint64(v), e
			case 8:
				got, err = cur.Uint64()
			}
			if err != nil || got != test.want || cur.Remaining() != int64(len(data)-test.n) {
				t.Fatalf("split %d scalar %d: %x, %v, %d left", split, test.n, got, err, cur.Remaining())
			}
		}
	}
	cur := NewCursor(segmented(t, data, 1))
	if got, err := cur.Uint64(); err != nil || got != 0x123456789abcdef0 {
		t.Fatalf("eight-way scalar: %x, %v", got, err)
	}
	if err := cur.RequireEnd(); err != nil {
		t.Fatal(err)
	}
}

func TestCursorTruncationDoesNotAdvance(t *testing.T) {
	for width := 1; width <= 8; width++ {
		for length := 0; length < width; length++ {
			data := bytes.Repeat([]byte{0x42}, length)
			cur := NewCursor(segmented(t, data, 1))
			if _, err := cur.scalar(width); !errors.Is(err, io.ErrUnexpectedEOF) || cur.Remaining() != int64(length) || cur.index != 0 || cur.offset != 0 {
				t.Fatalf("width %d, length %d: %v, cursor %+v", width, length, err, cur)
			}
			got, err := cur.Take(int64(length))
			if err != nil || !bytes.Equal(payload(t, got), data) {
				t.Fatalf("failed read damaged remainder: %v", err)
			}
		}
	}
}

func TestCursorTakeSkipAndErrors(t *testing.T) {
	data := []byte("abcdefghijk")
	cur := NewCursor(segmented(t, data, 2))
	if err := cur.RequireEnd(); !errors.Is(err, ErrTrailingBytes) {
		t.Fatalf("RequireEnd: %v", err)
	}
	for _, n := range []int64{-1, 12, math.MaxInt64} {
		if _, err := cur.Take(n); err == nil || cur.Remaining() != 11 {
			t.Fatalf("Take(%d): %v, %d remaining", n, err, cur.Remaining())
		}
		if err := cur.Skip(n); err == nil || cur.Remaining() != 11 {
			t.Fatalf("Skip(%d): %v, %d remaining", n, err, cur.Remaining())
		}
	}
	if err := cur.Skip(3); err != nil {
		t.Fatal(err)
	}
	view, err := cur.Take(5)
	if err != nil || string(payload(t, view)) != "defgh" || cur.Remaining() != 3 {
		t.Fatalf("cross-span Take: %v, %d remaining", err, cur.Remaining())
	}
	if &view.head.Bytes()[0] != &data[3] {
		t.Fatal("Take copied payload")
	}
	if v, err := cur.Uint16(); err != nil || v != 0x696a {
		t.Fatalf("scalar after Take: %x, %v", v, err)
	}
	last, err := cur.Take(1)
	if err != nil || string(payload(t, last)) != "k" {
		t.Fatalf("tail take: %v", err)
	}
	if empty, err := cur.Take(0); err != nil || empty.Len() != 0 {
		t.Fatalf("empty exhausted take: %v", err)
	}
	if err := cur.Skip(0); err != nil {
		t.Fatal(err)
	}
	if err := cur.RequireEnd(); err != nil {
		t.Fatal(err)
	}
}

var allocationScalar uint64

func TestCursorScalarDoesNotAllocate(t *testing.T) {
	c := segmented(t, []byte{1, 2, 3, 4, 5, 6, 7, 8}, 1)
	if n := testing.AllocsPerRun(1000, func() {
		cur := NewCursor(c)
		allocationScalar, _ = cur.Uint64()
	}); n != 0 {
		t.Fatalf("cross-span scalar allocated %v times", n)
	}
}

func BenchmarkCursorCrossSpanScalar(b *testing.B) {
	c := segmented(b, []byte{1, 2, 3, 4, 5, 6, 7, 8}, 1)
	b.ReportAllocs()
	b.SetBytes(8)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cur := NewCursor(c)
		allocationScalar, _ = cur.Uint64()
	}
}

func FuzzCursorTake(f *testing.F) {
	f.Add([]byte("segmented payload"), uint8(3), uint64(5))
	f.Add([]byte{}, uint8(0), uint64(0))
	f.Add(bytes.Repeat([]byte("x"), 1024), uint8(0), uint64(513))
	f.Fuzz(func(t *testing.T, data []byte, width uint8, requested uint64) {
		c := segmented(t, data, int(width)+1)
		for _, nested := range []bool{false, true} {
			cur := NewCursor(c)
			take := cur.Take
			if nested {
				take = cur.TakeNested
			}
			n := int64(requested)
			view, err := take(n)
			if n < 0 || n > int64(len(data)) {
				if err == nil || cur.Remaining() != int64(len(data)) {
					t.Fatal("invalid take advanced cursor or succeeded")
				}
				continue
			}
			if err != nil || !bytes.Equal(payload(t, view), data[:int(n)]) {
				t.Fatalf("valid take failed: %v", err)
			}
			rest, err := take(cur.Remaining())
			if err != nil || !bytes.Equal(payload(t, rest), data[int(n):]) {
				t.Fatalf("remainder failed: %v", err)
			}
		}
	})
}
