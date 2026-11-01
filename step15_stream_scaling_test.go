// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package gocql

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"runtime"
	"testing"
	"unsafe"

	"github.com/scylladb/gocql/v2/internal/testkit"
	lz4 "github.com/scylladb/gocql/v2/internal/wirecompress"
)

type step15Units struct {
	input              io.Reader
	width              int
	compressed         bool
	pending            []byte
	units, transformed int64
	sourceCapacity     int
}

func (s *step15Units) Read(p []byte) (int, error) {
	if len(s.pending) == 0 {
		data := make([]byte, s.width)
		n, e := io.ReadFull(s.input, data)
		if n == 0 {
			return 0, e
		}
		if e != nil && e != io.ErrUnexpectedEOF {
			return 0, e
		}
		s.pending = testkit.Unit(data[:n], s.compressed, false)
		s.sourceCapacity = max(s.sourceCapacity, cap(data)+cap(s.pending))
		s.units++
		s.transformed += int64(n)
	}
	n := copy(p, s.pending)
	s.pending = s.pending[n:]
	return n, nil
}

// Fixes the result bytes/cells/rows while independently changing segment count.
// Source unit and application output are established separately and reported.
// HeapAlloc samples are diagnostics, and actual driver byte capacities remain
// the deterministic bound; GC samples include runtime metadata and parser plans.
func TestStep15SegmentCountMemory(t *testing.T) {
	for _, compressed := range []bool{false, true} {
		for _, width := range []int{512, 8192, 130000} {
			t.Run(fmt.Sprintf("compressed%t/width%d", compressed, width), func(t *testing.T) {
				const size = 1 << 20
				source := &step15Units{input: step14Rows(5, 1, size), width: width, compressed: compressed}
				memory := &step14GeneratedConn{step04MemoryConn: &step04MemoryConn{}, input: source}
				var comp Compressor
				if compressed {
					comp = lz4.LZ4Compressor{}
				}
				dst := make([]byte, 0, size)
				backing := unsafe.SliceData(dst[:cap(dst)])
				runtime.GC()
				var idle runtime.MemStats
				runtime.ReadMemStats(&idle)
				r := step04MemoryReservation(5, comp, memory)
				defer r.InternalClose(false)
				response, e := r.InternalExchange(context.Background(), &attemptRequest{Statement: "SELECT value", Consistency: One})
				if e != nil {
					t.Fatal(e)
				}
				defer response.InternalReleaseMetadata()
				if e = response.InternalInstallColumns(response.InternalColumns); e != nil {
					t.Fatal(e)
				}
				plan := codecTestMust(StreamOneColumn(StreamBlob).Compile(response.InternalColumns))
				var peak uint64
				var peakCapacity int64
				sample := func() {
					peakCapacity = max(peakCapacity, response.InternalWorkingStorageBytes())
					runtime.GC()
					var m runtime.MemStats
					runtime.ReadMemStats(&m)
					peak = max(peak, m.HeapAlloc)
				}
				r.InternalConn.InternalR.(*connReader).InternalConn = &step14ObservedConn{Conn: memory, observe: sample}
				if ok, e := decodeStreamRow(response, plan, &dst); !ok || e != nil {
					t.Fatal(ok, e)
				}
				if unsafe.SliceData(dst) != backing || len(dst) != size || bytes.Count(dst, []byte{0x71}) != size {
					t.Fatal("output changed")
				}
				sample()
				if peakCapacity > 320<<10 {
					t.Fatal("segment count grew driver storage", peakCapacity)
				}
				t.Logf("segments=%d source_unit_capacity=%d final_output_capacity=%d peak_driver_byte_capacity=%d idle_heap=%d peak_postGC_heap=%d transform_bytes=%d unit_to_parser_bytes=%d", source.units, source.sourceCapacity, cap(dst), peakCapacity, idle.HeapAlloc, peak, source.transformed, r.InternalUnits.InternalCopies)
			})
		}
	}
}
