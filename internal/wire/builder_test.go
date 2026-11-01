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
	"fmt"
	"math"
	"testing"
)

func check(t testing.TB, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func TestBuilderReservePartsPreservesMarksAndFrozenStorage(t *testing.T) {
	var b Builder
	b.SetSlabSize(8)
	b.ReserveParts(4)
	mark := b.Mark()
	check(t, b.Uint32(0))
	payload := []byte{0x11, 0x22}
	check(t, b.Borrow(Borrow(payload)))
	firstSlab := b.slab
	b.ReserveParts(8)
	check(t, b.PatchUint32(mark, uint32(len(payload))))
	if &b.slab[0] != &firstSlab[0] {
		t.Fatal("descriptor reservation relocated scalar bytes")
	}
	first, err := b.Freeze()
	check(t, err)
	if err := b.PatchUint32(mark, 0); !errors.Is(err, ErrInvalidMark) {
		t.Fatalf("Freeze retained old mark: %v", err)
	}
	// Reuse all reserved descriptors and grow beyond them. The first Chain
	// owns its index and still borrows the original payload.
	for i := 0; i < 20; i++ {
		check(t, b.Borrow(Borrow([]byte{byte(i)})))
	}
	_, err = b.Freeze()
	check(t, err)
	got := make([]byte, first.Len())
	_, err = first.CopyTo(got)
	check(t, err)
	if !bytes.Equal(got, []byte{0, 0, 0, 2, 0x11, 0x22}) {
		t.Fatalf("reserved descriptor reuse changed frozen bytes: %x", got)
	}
	payload[0] = 0x33
	_, err = first.CopyTo(got)
	check(t, err)
	if got[4] != 0x33 {
		t.Fatal("reservation copied borrowed payload")
	}
}

func TestBuilderSlabSizePreservesPublishedStorageAndMarks(t *testing.T) {
	var b Builder
	b.SetSlabSize(8)
	mark := b.Mark()
	check(t, b.Uint32(0))
	check(t, b.Uint32(0x11223344))
	firstSlab := b.slab
	b.SetSlabSize(3)
	check(t, b.Uint32(0x55667788))
	if cap(b.slab) != 4 {
		t.Fatalf("oversized scalar slab capacity = %d", cap(b.slab))
	}
	check(t, b.PatchUint32(mark, 0x99aabbcc))
	if !bytes.Equal(firstSlab, []byte{0x99, 0xaa, 0xbb, 0xcc, 0x11, 0x22, 0x33, 0x44}) {
		t.Fatalf("published slab moved or mark changed: %x", firstSlab)
	}
	first, err := b.Freeze()
	check(t, err)
	want := []byte{0x99, 0xaa, 0xbb, 0xcc, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88}
	for _, size := range []int{0, -1, 64} {
		b.SetSlabSize(size)
		check(t, b.Uint8(42))
		expected := size
		if expected <= 0 {
			expected = defaultSlabSize
		}
		if cap(b.slab) != expected {
			t.Fatalf("size %d: slab capacity = %d", size, cap(b.slab))
		}
		_, err = b.Freeze()
		check(t, err)
		if !bytes.Equal(payload(t, first), want) {
			t.Fatal("subsequent configuration/reuse changed frozen bytes")
		}
	}
}

func TestBuilderScalarsAcrossSlabsAndBorrow(t *testing.T) {
	for _, size := range []int{-1, 0, 1, 3, 4, 7, 64} {
		b := NewBuilder(size)
		check(t, b.Uint8(0x12))
		check(t, b.Uint16(0x3456))
		borrowed := []byte{0x78, 0x9a}
		check(t, b.Borrow(Borrow(borrowed)))
		check(t, b.Borrow(Span{}))
		check(t, b.Uint32(0xbcdef012))
		check(t, b.Uint64(0x3456789abcdef012))
		if b.Len() != 17 {
			t.Fatalf("size %d: length %d", size, b.Len())
		}
		frozen, err := b.Freeze()
		check(t, err)
		want := []byte{0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc, 0xde, 0xf0, 0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc, 0xde, 0xf0, 0x12}
		if b.Len() != 0 || !bytes.Equal(payload(t, frozen), want) {
			t.Fatalf("size %d: bytes %x, builder length %d", size, payload(t, frozen), b.Len())
		}
		seenBorrow := false
		check(t, frozen.Each(func(s Span) error {
			if &s.Bytes()[0] == &borrowed[0] {
				seenBorrow = true
			}
			return nil
		}))
		if !seenBorrow {
			t.Fatal("builder copied borrowed payload")
		}
	}
}

func TestBuilderPatchAndFreezeIndependence(t *testing.T) {
	b := NewBuilder(5)
	mark := b.Mark()
	check(t, b.Uint32(0))
	borrowed := []byte("payload")
	check(t, b.Borrow(Borrow(borrowed)))
	check(t, b.Uint16(0x1234))
	check(t, b.PatchUint32(mark, uint32(len(borrowed))))
	frozen, err := b.Freeze()
	check(t, err)
	want := append([]byte{0, 0, 0, 7}, borrowed...)
	want = append(want, 0x12, 0x34)
	if !bytes.Equal(payload(t, frozen), want) {
		t.Fatalf("patched chain: %x", payload(t, frozen))
	}
	if err := b.PatchUint32(mark, 0xffffffff); !errors.Is(err, ErrInvalidMark) {
		t.Fatalf("stale patch after Freeze: %v", err)
	}
	check(t, b.Uint64(0xffffffffffffffff))
	b.Rollback(mark)
	if b.Len() != 8 {
		t.Fatal("stale rollback changed new generation")
	}
	second, err := b.Freeze()
	check(t, err)
	if !bytes.Equal(payload(t, second), bytes.Repeat([]byte{0xff}, 8)) || !bytes.Equal(payload(t, frozen), want) {
		t.Fatal("builder reuse modified a published chain")
	}
	emptyMark := b.Mark()
	empty, err := b.Freeze()
	check(t, err)
	if empty.Len() != 0 || !errors.Is(b.PatchUint32(emptyMark, 0), ErrInvalidMark) {
		t.Fatal("empty Freeze did not reset builder and invalidate marks")
	}
	if again, err := b.Freeze(); err != nil || again.Len() != 0 {
		t.Fatalf("repeated Freeze: %v", err)
	}
}

func TestBuilderRollbackAndMarkBranches(t *testing.T) {
	b := NewBuilder(4)
	start := b.Mark()
	check(t, b.Uint16(0x1234))
	keep := b.Mark()
	check(t, b.Uint32(0x56789abc))
	removed := b.Mark()
	check(t, b.Borrow(BorrowString("discard")))
	b.Rollback(keep)
	if b.Len() != 2 {
		t.Fatalf("rollback length %d", b.Len())
	}
	check(t, b.Uint64(0xdef0123456789abc))
	// removed's offset now exists again, but it belongs to the discarded branch.
	if err := b.PatchUint32(removed, 0); !errors.Is(err, ErrInvalidMark) {
		t.Fatalf("stale mark became valid after regrowth: %v", err)
	}
	b.Rollback(removed)
	if b.Len() != 10 {
		t.Fatal("stale rollback changed the replacement branch")
	}
	check(t, b.PatchUint32(start, 0xaabbccdd))
	frozen, err := b.Freeze()
	check(t, err)
	if got := payload(t, frozen); !bytes.Equal(got, []byte{0xaa, 0xbb, 0xcc, 0xdd, 0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc}) {
		t.Fatalf("rollback and cross-slab patch: %x", got)
	}
	// Rollback from within an owned span keeps its prefix and future writes
	// must not expose the old suffix through a reused descriptor.
	check(t, b.Uint8(1))
	inside := b.Mark()
	check(t, b.Uint16(0xffff))
	b.Rollback(inside)
	check(t, b.Uint8(2))
	partial, err := b.Freeze()
	check(t, err)
	if !bytes.Equal(payload(t, partial), []byte{1, 2}) {
		t.Fatal("rollback retained a discarded suffix")
	}
}

func TestBuilderPatchRejectsBorrowedBytesTransactionally(t *testing.T) {
	b := NewBuilder(2)
	m := b.Mark()
	check(t, b.Uint16(0x1234))
	data := []byte{0x56, 0x78}
	check(t, b.Borrow(Borrow(data)))
	if err := b.PatchUint32(m, 0xffffffff); !errors.Is(err, ErrBorrowedPatch) {
		t.Fatalf("patch into borrow: %v", err)
	}
	c, err := b.Freeze()
	check(t, err)
	if !bytes.Equal(payload(t, c), []byte{0x12, 0x34, 0x56, 0x78}) || !bytes.Equal(data, []byte{0x56, 0x78}) {
		t.Fatal("failed patch partially modified owned or borrowed bytes")
	}
}

func TestBuilderInvalidMarksAndOverflow(t *testing.T) {
	var b Builder
	other := NewBuilder(8)
	foreign := other.Mark()
	m := b.Mark()
	check(t, b.Uint8(1))
	for _, invalid := range []Mark{{}, foreign} {
		if err := b.PatchUint32(invalid, 0); !errors.Is(err, ErrInvalidMark) {
			t.Fatalf("invalid mark: %v", err)
		}
		b.Rollback(invalid)
		if b.Len() != 1 {
			t.Fatal("invalid rollback changed builder")
		}
	}
	if err := b.PatchUint32(m, 0); !errors.Is(err, ErrBounds) {
		t.Fatalf("short patch: %v", err)
	}
	b.Rollback(m)
	if b.Len() != 0 {
		t.Fatal("rollback to start failed")
	}
	// Synthetic length permits exercising overflow without unsafe or enormous
	// allocations. A rejected append must leave descriptors and bytes intact.
	b.len = math.MaxInt64
	if err := b.Borrow(BorrowString("x")); !errors.Is(err, ErrOverflow) {
		t.Fatalf("borrow overflow: %v", err)
	}
	if err := b.Uint64(1); !errors.Is(err, ErrOverflow) {
		t.Fatalf("scalar overflow: %v", err)
	}
	if b.Len() != math.MaxInt64 || len(b.parts) != 0 || b.slab != nil {
		t.Fatal("overflow changed builder state")
	}
}

func BenchmarkBuilderBorrowLargePayload(b *testing.B) {
	data := make([]byte, 1<<20)
	builder := NewBuilder(16)
	b.ReportAllocs()
	b.SetBytes(int64(len(data)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = builder.Borrow(Borrow(data))
		allocationChain, _ = builder.Freeze()
	}
}

func TestBuilderBorrowDoesNotAllocate(t *testing.T) {
	data := make([]byte, 1<<20)
	b := NewBuilder(16)
	// The first run reserves one descriptor; reuse must never allocate a slab
	// or payload buffer when the builder only receives a borrowed span.
	if n := testing.AllocsPerRun(1000, func() {
		_ = b.Borrow(Borrow(data))
		allocationChain, _ = b.Freeze()
	}); n != 0 {
		t.Fatalf("borrowed-only builder allocated %v times", n)
	}
	if &allocationChain.head.Bytes()[0] != &data[0] {
		t.Fatal("borrowed-only builder copied payload")
	}
}

func BenchmarkBuilderScalarAndBorrow(b *testing.B) {
	data := make([]byte, 1<<20)
	builder := NewBuilder(16)
	b.ReportAllocs()
	b.SetBytes(int64(len(data)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mark := builder.Mark()
		_ = builder.Uint32(0)
		_ = builder.Borrow(Borrow(data))
		_ = builder.PatchUint32(mark, uint32(len(data)))
		allocationChain, _ = builder.Freeze()
	}
}

// Reports arena/slab/descriptor work separately from per-scalar serialization.
func BenchmarkStep15ScalarArena(b *testing.B) {
	for _, count := range []int{1, 1024} {
		b.Run(fmt.Sprint(count), func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(8 * count))
			for i := 0; i < b.N; i++ {
				builder := NewBuilder(8 * count)
				for j := 0; j < count; j++ {
					if e := builder.Uint64(uint64(j)); e != nil {
						b.Fatal(e)
					}
				}
				if _, e := builder.Freeze(); e != nil {
					b.Fatal(e)
				}
			}
			b.ReportMetric(float64(count), "scalar-writes/op")
		})
	}
}
