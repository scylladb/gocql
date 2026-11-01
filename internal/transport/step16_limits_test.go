// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package transport

import (
	"context"
	"encoding/binary"
	"math"
	"net"
	"testing"
	"time"

	"github.com/scylladb/gocql/v2/internal/connectivity"
	frm "github.com/scylladb/gocql/v2/internal/frame"
	"github.com/scylladb/gocql/v2/internal/model"
	"github.com/scylladb/gocql/v2/internal/wire"
)

// A validated limit is an explicit setting, including values above the default.
func TestStep16ConfiguredFrameLimits(t *testing.T) {
	for _, limit := range []int64{1024, 256 << 20, (256 << 20) + 1, 512 << 20, 1<<31 - 1} {
		c := &Conn{InternalCfg: &ConnConfig{TypedLimits: connectivity.ProtocolLimits{MaxFrameBytes: limit, MaxDecompressedBytes: limit}}}
		actual := c.typedProtocolLimits()
		if actual.MaxFrameBytes != limit || actual.MaxDecompressedBytes != limit {
			t.Errorf("configured limit %d became frame=%d decompressed=%d", limit, actual.MaxFrameBytes, actual.MaxDecompressedBytes)
		}
	}
}

func TestStep16LargeRequestLimitWithoutPayloadAllocation(t *testing.T) {
	for _, limit := range []int64{(256 << 20) + 1, math.MaxInt32} {
		// One repeated MiB backing gives a large logical frame without a large
		// allocation. Values and headers retain the same immutable ranges.
		c := &Conn{InternalVersion: 4, InternalCfg: &ConnConfig{TypedLimits: connectivity.ProtocolLimits{MaxFrameBytes: limit, MaxDecompressedBytes: limit}}}
		// QUERY body: four-byte CQL length, one-byte CQL, consistency, flags,
		// value count and the value's four-byte length.
		payload := limit - 14
		var length [4]byte
		binary.BigEndian.PutUint32(length[:], uint32(payload))
		spans := []wire.Span{wire.Borrow(length[:])}
		block := make([]byte, 1<<20)
		for remaining := payload; remaining > 0; {
			n := min(remaining, int64(len(block)))
			spans = append(spans, wire.Borrow(block[:int(n)]))
			remaining -= n
		}
		values, err := wire.NewChain(spans...)
		if err != nil {
			t.Fatal(err)
		}
		request := &model.AttemptRequest{Statement: "Q", Values: values, ValueCount: 1}
		packet, err := c.InternalBuildTypedRequest(request, 0)
		if err != nil {
			t.Fatal(limit, err)
		}
		if packet.Len() != limit+frm.HeaderSize {
			t.Fatalf("logical packet length %d, want %d", packet.Len(), limit+frm.HeaderSize)
		}
		header := make([]byte, frm.HeaderSize)
		prefix, _ := packet.Slice(0, frm.HeaderSize)
		prefix.CopyTo(header)
		if got := int64(binary.BigEndian.Uint32(header[5:])); got != limit {
			t.Fatal(got, limit)
		}
		c.InternalCfg.TypedLimits.MaxFrameBytes--
		if _, err := c.InternalBuildTypedRequest(request, 0); err == nil {
			t.Fatal("request above configured bound accepted")
		}
	}
}

func TestStep16HandshakeFramersCaptureConfiguredLimits(t *testing.T) {
	c := &Conn{InternalVersion: 4, InternalCfg: &ConnConfig{TypedLimits: connectivity.ProtocolLimits{MaxFrameBytes: 512 << 20, MaxDecompressedBytes: 511 << 20}}}
	for _, f := range []*framer{c.InternalGetReadFramer(), c.InternalGetWriteFramer()} {
		if f.MaxFrameBytes != 512<<20 || f.MaxDecompressedBytes != 511<<20 {
			t.Fatal("handshake framer lost limits", f.MaxFrameBytes, f.MaxDecompressedBytes)
		}
	}
}

type step16CountingWriter struct{ written int64 }

func (w *step16CountingWriter) Write(p []byte) (int, error) {
	w.written += int64(len(p))
	return len(p), nil
}
func (*step16CountingWriter) SetWriteDeadline(time.Time) error { return nil }

func TestStep16CoalescedLargeSpanCount(t *testing.T) {
	block := make([]byte, 1<<20)
	var spans []wire.Span
	for range 2048 {
		spans = append(spans, wire.Borrow(block))
	}
	spans = append(spans, wire.Borrow(block[:8]))
	chain, err := wire.NewChain(spans...)
	if err != nil {
		t.Fatal(err)
	}
	sink := new(step16CountingWriter)
	writer := &InternalWriteCoalescer{InternalC: sink, InternalWriteCh: make(chan InternalWriteRequest), InternalQuit: make(chan struct{})}
	done := make(chan struct{})
	go func() {
		defer close(done)
		request := <-writer.InternalWriteCh
		var buffers net.Buffers
		request.chain.Each(func(s wire.Span) error { buffers = append(buffers, s.Bytes()); return nil })
		writer.InternalFlushSizes([]chan<- InternalWriteResult{request.resultChan}, buffers, []int64{request.chain.Len()})
	}()
	n, err := writer.InternalWriteSpans(context.Background(), chain)
	if err != nil || n != chain.Len() || sink.written != chain.Len() {
		t.Fatal("coalesced span count truncated", n, chain.Len(), err)
	}
	<-done
}
