/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package wire

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"testing"
	"time"
)

type shortStreamReader struct {
	source io.Reader
	size   int
}

func (r shortStreamReader) Read(p []byte) (int, error) { return r.source.Read(p[:min(len(p), r.size)]) }
func newTestStream(t testing.TB, data []byte, chunk int, limits StreamLimits) *Stream {
	t.Helper()
	s, e := NewStream(context.Background(), shortStreamReader{bytes.NewReader(data), chunk}, int64(len(data)), limits)
	if e != nil {
		t.Fatal(e)
	}
	return s
}
func TestStreamSplitScalarsAndCells(t *testing.T) {
	var data []byte
	data = append(data, 42)
	data = binary.BigEndian.AppendUint16(data, 0x1020)
	data = binary.BigEndian.AppendUint32(data, 0x30405060)
	data = binary.BigEndian.AppendUint64(data, 0x708090a0b0c0d0e0)
	data = binary.BigEndian.AppendUint32(data, 5)
	data = append(data, "hello"...)
	data = binary.BigEndian.AppendUint32(data, math.MaxUint32)
	data = binary.BigEndian.AppendUint32(data, 0)
	for chunk := 1; chunk <= len(data); chunk++ {
		s := newTestStream(t, data, chunk, StreamLimits{})
		if v, e := s.Uint8(); v != 42 || e != nil {
			t.Fatalf("u8 %d %v", v, e)
		}
		if v, e := s.Uint16(); v != 0x1020 || e != nil {
			t.Fatalf("u16 %d %v", v, e)
		}
		if v, e := s.Uint32(); v != 0x30405060 || e != nil {
			t.Fatalf("u32 %d %v", v, e)
		}
		if v, e := s.Uint64(); v != 0x708090a0b0c0d0e0 || e != nil {
			t.Fatalf("u64 %d %v", v, e)
		}
		var dst [5]byte
		if e := s.Cell(false, func(state CellState, c *Stream) error {
			if state != CellPresent || c.Remaining() != 5 {
				t.Fatal("present framing")
			}
			return c.ReadFull(dst[:])
		}); e != nil || string(dst[:]) != "hello" {
			t.Fatalf("cell %q %v", dst, e)
		}
		for _, want := range []CellState{CellNull, CellPresent} {
			if e := s.Cell(false, func(state CellState, c *Stream) error {
				if state != want || c.Remaining() != 0 {
					t.Fatal("null/empty collapsed")
				}
				return nil
			}); e != nil {
				t.Fatal(e)
			}
		}
		if e := s.RequireEnd(); e != nil {
			t.Fatal(e)
		}
	}
}

type targetReader struct {
	data      []byte
	offset    int
	bodyStart int
	dst       []byte
	max       int
}

func (r *targetReader) Read(p []byte) (int, error) {
	if r.offset == len(r.data) {
		return 0, io.EOF
	}
	n := min(len(p), len(r.data)-r.offset, r.max)
	if r.offset >= r.bodyStart && &p[0] != &r.dst[r.offset-r.bodyStart] {
		return 0, errors.New("bulk read did not target final caller range")
	}
	copy(p[:n], r.data[r.offset:r.offset+n])
	r.offset += n
	return n, nil
}
func TestStreamDirectFinalRangesAndCapacityPreflight(t *testing.T) {
	payload := bytes.Repeat([]byte("final caller storage "), 1<<14)
	data := binary.BigEndian.AppendUint32(nil, uint32(len(payload)))
	data = append(data, payload...)
	dst := make([]byte, len(payload))
	r := &targetReader{data: data, bodyStart: 4, dst: dst, max: 1031}
	s, e := NewStream(context.Background(), r, int64(len(data)), StreamLimits{})
	if e != nil {
		t.Fatal(e)
	}
	if e = s.Cell(false, func(_ CellState, c *Stream) error { return c.ReadFull(dst) }); e != nil {
		t.Fatal(e)
	}
	if !bytes.Equal(dst, payload) {
		t.Fatal("payload changed")
	}
	r = &targetReader{data: data, bodyStart: 4, dst: dst, max: 1031}
	s, _ = NewStream(context.Background(), r, int64(len(data)), StreamLimits{})
	capacityError := errors.New("caller capacity")
	e = s.Cell(false, func(_ CellState, c *Stream) error {
		if c.Remaining() > 16 {
			return capacityError
		}
		return c.ReadFull(dst[:16])
	})
	if !errors.Is(e, capacityError) || r.offset != 4 {
		t.Fatalf("capacity consumed body: %v %d", e, r.offset)
	}
	if _, e = s.Uint8(); !errors.Is(e, capacityError) {
		t.Fatal("error not sticky")
	}
}
func TestStreamNestedScopesAndLimits(t *testing.T) {
	s := newTestStream(t, []byte{1, 2, 3, 4, 5}, 1, StreamLimits{MaxDepth: 2})
	var old *Stream
	if e := s.Child(4, func(c *Stream) error {
		old = c
		if e := c.Child(2, func(d *Stream) error {
			v, e := d.Uint16()
			if v != 0x0102 {
				t.Fatal(v)
			}
			return e
		}); e != nil {
			return e
		}
		v, e := c.Uint16()
		if v != 0x0304 {
			t.Fatal(v)
		}
		return e
	}); e != nil {
		t.Fatal(e)
	}
	if v, e := s.Uint8(); v != 5 || e != nil {
		t.Fatal(v, e)
	}
	if _, e := old.Uint8(); !errors.Is(e, ErrStreamScope) {
		t.Fatal("escaped child remained active", e)
	}
	for _, tc := range []struct {
		name string
		run  func(*Stream) error
		want error
	}{
		{"depth", func(s *Stream) error {
			return s.Child(1, func(c *Stream) error {
				return c.Child(1, func(d *Stream) error { return d.Child(1, func(*Stream) error { return nil }) })
			})
		}, ErrLimit},
		{"overread", func(s *Stream) error {
			return s.Child(2, func(c *Stream) error { var b [3]byte; return c.ReadFull(b[:]) })
		}, io.ErrUnexpectedEOF},
		{"underread", func(s *Stream) error { return s.Child(2, func(c *Stream) error { return nil }) }, ErrTrailingBytes},
		{"parent", func(s *Stream) error { return s.Child(2, func(c *Stream) error { _, e := s.Uint8(); return e }) }, ErrStreamScope},
		{"negative", func(s *Stream) error { return s.Child(-1, func(*Stream) error { return nil }) }, ErrLength},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := newTestStream(t, []byte{1, 2, 3}, 1, StreamLimits{MaxDepth: 2})
			if e := tc.run(s); !errors.Is(e, tc.want) {
				t.Fatal(e)
			}
		})
	}
	if _, e := NewStream(context.Background(), bytes.NewReader(nil), 4, StreamLimits{MaxBytes: 3}); !errors.Is(e, ErrLimit) {
		t.Fatal(e)
	}
	for _, l := range []StreamLimits{{MaxBytes: -1}, {MaxDepth: -1}, {MaxCount: math.MaxInt64}, {DiscardBytes: -1}} {
		if _, e := NewStream(context.Background(), bytes.NewReader(nil), 0, l); !errors.Is(e, ErrLimit) {
			t.Fatal(e)
		}
	}
	if _, e := CheckProduct(math.MaxInt64, 2, math.MaxInt64); !errors.Is(e, ErrLimit) {
		t.Fatal(e)
	}
	if n, e := CheckProduct(math.MaxInt64, 0, math.MaxInt64); n != 0 || e != nil {
		t.Fatal(n, e)
	}
}
func TestStreamCountsAndLengthSentinels(t *testing.T) {
	for _, n := range []uint32{math.MaxUint32, math.MaxUint32 - 1, math.MaxInt32, 4} {
		data := binary.BigEndian.AppendUint32(nil, n)
		data = append(data, 0, 0)
		s := newTestStream(t, data, 1, StreamLimits{MaxCount: 3})
		if _, e := s.Count32(1); e == nil {
			t.Fatalf("accepted count %d", n)
		}
	}
	for _, n := range []int32{-1, -2, -3, 0, 2, 3} {
		for _, allow := range []bool{false, true} {
			data := binary.BigEndian.AppendUint32(nil, uint32(n))
			data = append(data, 0, 0)
			s := newTestStream(t, data, 1, StreamLimits{})
			e := s.Cell(allow, func(_ CellState, c *Stream) error { return c.Skip() })
			valid := n == -1 || n == -2 && allow || n >= 0 && n <= 2
			if (e == nil) != valid {
				t.Fatalf("length %d allow=%v: %v", n, allow, e)
			}
		}
	}
}
func TestStreamTruncationAndTrailing(t *testing.T) {
	data := binary.BigEndian.AppendUint32(nil, 8)
	data = append(data, 1, 2, 3, 4, 5, 6, 7, 8)
	for cut := 0; cut < len(data); cut++ {
		// The envelope still declares the original size, unlike the physical source.
		s, e := NewStream(context.Background(), shortStreamReader{bytes.NewReader(data[:cut]), 1}, int64(len(data)), StreamLimits{})
		if e != nil {
			t.Fatal(e)
		}
		if e = s.Cell(false, func(_ CellState, c *Stream) error { return c.Skip() }); !errors.Is(e, io.ErrUnexpectedEOF) {
			t.Fatalf("cut %d: %v", cut, e)
		}
	}
}

type noProgressReader struct{}

func (noProgressReader) Read([]byte) (int, error) { return 0, nil }
func TestStreamNoProgressAndCancel(t *testing.T) {
	s, _ := NewStream(context.Background(), noProgressReader{}, 1, StreamLimits{})
	var b [1]byte
	if e := s.ReadFull(b[:]); !errors.Is(e, io.ErrNoProgress) {
		t.Fatal(e)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	s, _ = NewStream(ctx, bytes.NewReader([]byte{1}), 1, StreamLimits{})
	if e := s.ReadFull(b[:]); !errors.Is(e, context.Canceled) {
		t.Fatal(e)
	}
	// The owning transport interrupts the source; the reader launches no worker.
	client, server := net.Pipe()
	defer server.Close()
	ctx, cancel = context.WithCancel(context.Background())
	entered := make(chan struct{})
	s, _ = NewStream(ctx, signalStreamReader{Reader: client, entered: entered}, 1, StreamLimits{})
	done := make(chan error, 1)
	go func() { done <- s.ReadFull(b[:]) }()
	<-entered
	cancel()
	client.Close()
	select {
	case e := <-done:
		if !errors.Is(e, context.Canceled) {
			t.Fatal(e)
		}
	case <-time.After(time.Second):
		t.Fatal("owner interruption did not unblock")
	}
}

type repeatedStreamReader struct {
	bytes   int64
	maxRead int
	reads   int
}

func (r *repeatedStreamReader) Read(p []byte) (int, error) {
	r.reads++
	if r.bytes == 0 {
		return 0, io.EOF
	}
	n := int(min(int64(len(p)), r.bytes))
	r.bytes -= int64(n)
	r.maxRead = max(r.maxRead, n)
	clear(p[:n])
	return n, nil
}
func TestStreamDiscardMemoryBoundAndScopeReuse(t *testing.T) {
	source := &repeatedStreamReader{bytes: 8 << 20}
	s, _ := NewStream(context.Background(), source, source.bytes, StreamLimits{DiscardBytes: 127})
	// A small first ignored cell must not constrain every later read to 1 B.
	if e := s.Child(1, func(c *Stream) error { return c.Skip() }); e != nil {
		t.Fatal(e)
	}
	if e := s.Skip(); e != nil {
		t.Fatal(e)
	}
	if source.reads > 2+(8<<20)/127 {
		t.Fatalf("small-first discard amplified reads: %d", source.reads)
	}
	if cap(s.state.discard) != 127 || source.maxRead != 127 {
		t.Fatal("discard grew", cap(s.state.discard), source.maxRead)
	}
	source = &repeatedStreamReader{bytes: 1 << 20}
	s, _ = NewStream(context.Background(), source, source.bytes, StreamLimits{})
	consume := func(c *Stream) error { _, e := c.Uint8(); return e }
	if e := s.Child(1, consume); e != nil {
		t.Fatal(e)
	}
	if allocs := testing.AllocsPerRun(1000, func() {
		if e := s.Child(1, consume); e != nil {
			panic(e)
		}
	}); allocs != 0 {
		t.Fatalf("per-cell scopes allocate: %v", allocs)
	}
}
func FuzzStreamCells(f *testing.F) {
	f.Add([]byte{0, 0, 0, 3, 1, 2, 3}, uint8(1), uint8(3))
	f.Add([]byte{255, 255, 255, 254}, uint8(2), uint8(0))
	f.Fuzz(func(t *testing.T, data []byte, chunk, depth uint8) {
		if len(data) > 1<<16 {
			t.Skip()
		}
		s, e := NewStream(context.Background(), shortStreamReader{bytes.NewReader(data), int(chunk) + 1}, int64(len(data)), StreamLimits{MaxDepth: int(depth) + 1, DiscardBytes: 127})
		if e != nil {
			t.Fatal(e)
		}
		for s.Remaining() > 0 && s.Err() == nil {
			if e := s.Cell(false, func(_ CellState, c *Stream) error { return c.Skip() }); e != nil {
				break
			}
		}
		if s.Err() == nil && s.Remaining() != 0 {
			t.Fatal("successful parse left bytes")
		}
	})
}
func BenchmarkStreamFinalRanges(b *testing.B) {
	for _, size := range []int{32, 4096, 500 << 10} {
		b.Run(fmt.Sprint(size), func(b *testing.B) {
			dst := make([]byte, size)
			source := &repeatedStreamReader{bytes: math.MaxInt64}
			s, _ := NewStream(context.Background(), source, math.MaxInt32, StreamLimits{MaxBytes: math.MaxInt32})
			if e := s.Child(int64(size), func(c *Stream) error { return c.ReadFull(dst) }); e != nil {
				b.Fatal(e)
			}
			b.ReportAllocs()
			b.SetBytes(int64(size))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				s.remaining = math.MaxInt32
				if e := s.Child(int64(size), func(c *Stream) error { return c.ReadFull(dst) }); e != nil {
					b.Fatal(e)
				}
			}
		})
	}
}

type eofDataReader struct{ data []byte }

func (r *eofDataReader) Read(p []byte) (int, error) {
	n := copy(p, r.data)
	r.data = r.data[n:]
	return n, io.EOF
}
func TestStreamReadHandlesDataWithEOF(t *testing.T) {
	s, _ := NewStream(context.Background(), &eofDataReader{data: []byte("abc")}, 3, StreamLimits{})
	var dst [3]byte
	if e := s.ReadFull(dst[:]); e != nil || string(dst[:]) != "abc" {
		t.Fatal(e)
	}
	if e := s.RequireEnd(); e != nil {
		t.Fatal(e)
	}
	s, _ = NewStream(context.Background(), &eofDataReader{data: []byte("ab")}, 3, StreamLimits{})
	if e := s.ReadFull(dst[:]); !errors.Is(e, io.ErrUnexpectedEOF) {
		t.Fatal(e)
	}
}

// Signal only once the source Read has been invoked, after context preflight.
type signalStreamReader struct {
	io.Reader
	entered chan struct{}
}

func (r signalStreamReader) Read(p []byte) (int, error) { close(r.entered); return r.Reader.Read(p) }
