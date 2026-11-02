// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package transport

import (
	"context"
	"io"

	frm "github.com/scylladb/gocql/v2/internal/frame"
	"github.com/scylladb/gocql/v2/internal/wire"
)

// nativeStreamReadAhead belongs to one exclusive reservation. Only native body
// bytes enter its buffer; headers and subsequent frames stay on the raw reader.
// It is accessed under the reservation's active lock, without background reads.
type nativeStreamReadAhead struct {
	source    interface{ readAvailable([]byte) (int, error) }
	buf       []byte
	start     int
	end       int
	remaining int64
	limit     int
	target    int
	err       error
}

func (b *nativeStreamReadAhead) begin(source interface{ readAvailable([]byte) (int, error) }, size int64, limit int) {
	b.source = source
	b.start, b.end = 0, 0
	b.remaining = size
	b.limit = min(limit, 4<<10, int(size))
	b.target = min(256, b.limit)
	b.err = nil
}

func (b *nativeStreamReadAhead) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	if b.start != b.end {
		n := copy(p, b.buf[b.start:b.end])
		b.start += n
		if b.start == b.end {
			return n, b.err
		}
		return n, nil
	}
	if b.err != nil {
		return 0, b.err
	}
	if b.remaining == 0 {
		return 0, io.EOF
	}
	p = p[:min(int64(len(p)), b.remaining)]
	// Caller-owned bulk output bypasses scratch; only small scalar/metadata
	// requests speculate. Each source read returns whatever is available now.
	if len(p) >= b.target {
		n, err := b.source.readAvailable(p)
		b.remaining -= int64(n)
		b.err = err
		return n, err
	}
	if cap(b.buf) < b.limit {
		b.buf = make([]byte, b.limit)
	}
	want := min(int64(b.target), b.remaining)
	n, err := b.source.readAvailable(b.buf[:want])
	if n < 0 || int64(n) > want {
		b.err = wire.ErrBounds
		return 0, b.err
	}
	b.remaining -= int64(n)
	b.start, b.end, b.err = 0, n, err
	if n == int(want) {
		b.target = min(b.limit, b.target*2)
	}
	n = copy(p, b.buf[:n])
	b.start = n
	if b.start == b.end {
		return n, err
	}
	return n, nil
}

func (r *InternalExclusiveConn) readStreamEnvelope(ctx context.Context, limits wire.StreamLimits) (frm.FrameHeader, *wire.Stream, error) {
	if r.InternalUnits != nil {
		return frm.ReadStreamEnvelope(ctx, r.InternalUnits, r.InternalConn.InternalVersion, limits, r.InternalConn.typedResponseExtensionFlags())
	}
	header, err := frm.ReadStreamHeader(ctx, r.socketSource(), r.InternalConn.InternalVersion, limits, r.InternalConn.typedResponseExtensionFlags())
	if err != nil {
		return frm.FrameHeader{}, nil, err
	}
	source := interface{ readAvailable([]byte) (int, error) }(r.InternalConn.InternalR.(*InternalConnReader))
	if r.transferred.raw != nil {
		source = &r.transferred
	}
	r.readAhead.begin(source, int64(header.Length), r.InternalLimits.ReadAheadBytes)
	body, err := wire.NewStream(ctx, &r.readAhead, int64(header.Length), limits)
	return header, body, err
}
