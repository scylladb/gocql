// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package transport

import (
	"context"
	"io"
	"net"

	"github.com/scylladb/gocql/v2/internal/wire"
)

// spanContextWriter consumes immutable span descriptors while serializing the
// entire request. Completion, including cancellation, is a lifetime boundary.
type InternalSpanContextWriter interface {
	InternalWriteSpans(context.Context, wire.Chain) (int64, error)
}

// TCP and Unix sockets use writev. Wrappers (including crypto/tls) receive
// ordered spans without a driver gather buffer. TLS record assembly is owned by
// the standard library. Checking every fallback Write prevents a short nil write
// from accidentally skipping the unwritten suffix between spans.
func InternalWriteSpanBuffers(w io.Writer, spans net.Buffers) (int64, error) {
	switch w.(type) {
	case *net.TCPConn, *net.UnixConn:
		return spans.WriteTo(w)
	}
	var total int64
	for _, span := range spans {
		n, err := w.Write(span)
		if n < 0 || n > len(span) {
			return total, io.ErrShortWrite
		}
		total += int64(n)
		if err != nil {
			return total, err
		}
		if n != len(span) {
			return total, io.ErrShortWrite
		}
	}
	return total, nil
}

func (w *InternalDeadlineContextWriter) InternalWriteSpans(ctx context.Context, chain wire.Chain) (int64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	select {
	case <-ctx.Done():
		return 0, ctx.Err()
	case <-w.InternalQuit:
		return 0, ErrConnectionClosed
	case w.InternalSemaphore <- struct{}{}:
	}
	defer func() { <-w.InternalSemaphore }()
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if err := w.InternalW.SetWriteDeadline(InternalEffectiveWriteDeadline(w.InternalTimeout.Load())); err != nil {
		return 0, err
	}

	spans := w.spanScratch[:0]
	defer func() {
		clear(spans)
		// Bound idle descriptor storage independently of request fragmentation.
		// The write semaphore remains held until all payload references are gone.
		if cap(spans) <= 16 {
			w.spanScratch = spans[:0]
		} else {
			w.spanScratch = nil
		}
	}()
	_ = chain.Each(func(s wire.Span) error { spans = append(spans, s.Bytes()); return nil })
	n, err := InternalWriteSpanBuffers(w.InternalW, spans)
	if err == nil && n != chain.Len() {
		err = io.ErrShortWrite
	}
	return n, err
}

func (w *InternalWriteCoalescer) InternalWriteSpans(ctx context.Context, chain wire.Chain) (int64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	resultChan := InternalWriteResultChanPool.Get().(chan InternalWriteResult)
	req := InternalWriteRequest{ctx: ctx, chain: chain, resultChan: resultChan}
	select {
	case <-ctx.Done():
		InternalWriteResultChanPool.Put(resultChan)
		return 0, ctx.Err()
	case <-w.InternalQuit:
		InternalWriteResultChanPool.Put(resultChan)
		return 0, ErrConnectionClosed
	case w.InternalWriteCh <- req:
	}
	if w.InternalTestEnqueuedHook != nil {
		w.InternalTestEnqueuedHook()
	}
	// Cancellation after admission must join the writer before releasing input.
	result := <-resultChan
	InternalWriteResultChanPool.Put(resultChan)
	return result.InternalWritten, result.InternalErr
}
