package gocql

import (
	"context"
	"io"
	"net"

	"github.com/scylladb/gocql/v2/internal/wire"
)

// spanContextWriter consumes immutable span descriptors while serializing the
// entire request. Completion, including cancellation, is a lifetime boundary.
type spanContextWriter interface {
	writeSpans(context.Context, wire.Chain) (int64, error)
}

// TCP and Unix sockets use writev. Wrappers (including crypto/tls) receive
// ordered spans without a driver gather buffer. TLS record assembly is owned by
// the standard library. Checking every fallback Write prevents a short nil write
// from accidentally skipping the unwritten suffix between spans.
func writeSpanBuffers(w io.Writer, spans net.Buffers) (int64, error) {
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

func (w *deadlineContextWriter) writeSpans(ctx context.Context, chain wire.Chain) (int64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	select {
	case <-ctx.Done():
		return 0, ctx.Err()
	case <-w.quit:
		return 0, ErrConnectionClosed
	case w.semaphore <- struct{}{}:
	}
	defer func() { <-w.semaphore }()
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if err := w.w.SetWriteDeadline(effectiveWriteDeadline(w.timeout.Load())); err != nil {
		return 0, err
	}

	var spans net.Buffers
	_ = chain.Each(func(s wire.Span) error { spans = append(spans, s.Bytes()); return nil })
	n, err := writeSpanBuffers(w.w, spans)
	if err == nil && n != chain.Len() {
		err = io.ErrShortWrite
	}
	return n, err
}

func (w *writeCoalescer) writeSpans(ctx context.Context, chain wire.Chain) (int64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	resultChan := writeResultChanPool.Get().(chan writeResult)
	req := writeRequest{chain: chain, resultChan: resultChan}
	select {
	case <-ctx.Done():
		writeResultChanPool.Put(resultChan)
		return 0, ctx.Err()
	case <-w.quit:
		writeResultChanPool.Put(resultChan)
		return 0, ErrConnectionClosed
	case w.writeCh <- req:
	}
	if w.testEnqueuedHook != nil {
		w.testEnqueuedHook()
	}
	// Cancellation after admission must join the writer before releasing input.
	result := <-resultChan
	writeResultChanPool.Put(resultChan)
	return int64(result.n), result.err
}
