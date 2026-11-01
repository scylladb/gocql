//go:build gocql_perf

// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package gocql

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"testing"
	"unsafe"

	"github.com/scylladb/gocql/v2/internal/segment"
	"github.com/scylladb/gocql/v2/internal/wire"
	"github.com/scylladb/gocql/v2/internal/wirecompress"
)

func TestStep15CompressionOutputFrameIdentity(t *testing.T) {
	data := bytes.Repeat([]byte{'q'}, 64<<10)
	source := codecTestMust(wire.NewChain(wire.Borrow(data[:7]), wire.Borrow(data[7:])))
	wirecompress.ResetPerfCounters()
	frame := codecTestMust(segment.EncodeSpans(source, true, true))
	calls, input, size, ptr := wirecompress.ReadPerfCounters()
	found := false
	frame.Each(func(s wire.Span) error {
		if uintptr(unsafe.Pointer(unsafe.SliceData(s.Bytes()))) == ptr && int64(s.Len()) == size {
			found = true
		}
		return nil
	})
	if !found || calls != 1 || input != int64(len(data)) {
		t.Fatal("segment copied compressor output", calls, input, size, found)
	}
	t.Logf("v5 transform_input=%d transformed_output=%d same_output_backing=true", input, size)
	for _, name := range []string{"lz4", "snappy"} {
		t.Run(name, func(t *testing.T) {
			c := &driverConn{InternalVersion: 4}
			if name == "lz4" {
				c.InternalCompressor = wirecompress.LZ4Compressor{}
			} else {
				c.InternalCompressor = wirecompress.SnappyCompressor{}
			}
			c.InternalInitFramerCache()
			values := typedTestChain(t, binary.BigEndian.AppendUint32(nil, uint32(len(data))), data)
			wirecompress.ResetPerfCounters()
			wire.ResetPerfCounters()
			frame := codecTestMust(c.InternalBuildTypedRequest(&attemptRequest{Statement: "INSERT INTO ks.tbl(v)VALUES(?)", Values: values, ValueCount: 1, Consistency: One}, 0))
			calls, input, size, ptr := wirecompress.ReadPerfCounters()
			found := false
			frame.Each(func(s wire.Span) error {
				p := s.Bytes()
				start := uintptr(unsafe.Pointer(unsafe.SliceData(p)))
				if ptr >= start && ptr+uintptr(size) <= start+uintptr(len(p)) {
					found = true
				}
				return nil
			})
			if calls != 1 || !found || wire.ReadPerfCounters().CopyBytes != 0 {
				t.Fatal(fmt.Sprint(calls, input, size, found, wire.ReadPerfCounters()))
			}
			t.Logf("transform_input=%d transformed_output=%d same_output_backing=true source_gather_bytes=0", input, size)
		})
	}
}
