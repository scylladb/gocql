// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package transport

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/scylladb/gocql/v2/internal/wire"
)

// A ticket has two owners: its waiting caller and the persistent writer. Only
// the last owner recycles it. Cancellation can stop waiting without retiring
// bytes still accepted by the socket writer.
type writeTicket struct {
	refs   atomic.Int32
	phase  atomic.Uint32 // 0 queued, 1 writing, 2 cancelled before transmission
	ctx    context.Context
	packet wire.Chain
	lease  *wire.Lease
	done   chan InternalWriteResult
}

var ticketPool = sync.Pool{New: func() any { return &writeTicket{done: make(chan InternalWriteResult, 1)} }}

func (t *writeTicket) release() {
	if t.refs.Add(-1) != 0 {
		return
	}
	select {
	case <-t.done:
	default:
	}
	t.ctx, t.packet, t.lease = nil, wire.Chain{}, nil
	ticketPool.Put(t)
}
func (t *writeTicket) finish(result InternalWriteResult) {
	t.lease.Release()
	t.lease = nil
	t.done <- result
	t.release()
}

// pendingWriteError means the writer may still be transmitting. The stream ID
// must become an orphan, and only writer completion may release request storage.
type pendingWriteError struct{ error }

func (e *pendingWriteError) Unwrap() error { return e.error }

type submissionWriter struct {
	conn  *Conn
	ready chan *writeTicket
}

func (c *Conn) submitTyped(ctx context.Context, packet wire.Chain, lease *wire.Lease) (int64, error) {
	c.typedWriterOnce.Do(func() {
		c.typedWriter = &submissionWriter{conn: c, ready: make(chan *writeTicket)}
		go c.typedWriter.run()
	})
	t := ticketPool.Get().(*writeTicket)
	t.refs.Store(2)
	t.phase.Store(0)
	t.ctx, t.packet, t.lease = ctx, packet, lease
	defer t.release()
	select {
	case <-ctx.Done():
		t.finish(InternalWriteResult{InternalErr: ctx.Err()})
		return 0, ctx.Err()
	case <-c.InternalCtx.Done():
		t.finish(InternalWriteResult{InternalErr: ErrConnectionClosed})
		return 0, ErrConnectionClosed
	case c.typedWriter.ready <- t:
	}
	select {
	case result := <-t.done:
		return result.InternalWritten, result.InternalErr
	default:
	}
	select {
	case result := <-t.done:
		return result.InternalWritten, result.InternalErr
	case <-ctx.Done():
		if t.phase.CompareAndSwap(0, 2) {
			return 0, ctx.Err()
		}
		return packet.Len(), &pendingWriteError{ctx.Err()}
	case <-c.InternalCtx.Done():
		if t.phase.CompareAndSwap(0, 2) {
			return 0, ErrConnectionClosed
		}
		return packet.Len(), &pendingWriteError{ErrConnectionClosed}
	}
}

func (w *submissionWriter) run() {
	var pending [16]*writeTicket
	spans := make([]wire.Span, 0, 64)
	for {
		select {
		case <-w.conn.InternalCtx.Done():
			return
		case first := <-w.ready:
			pending[0] = first
			count, bytes := 1, first.packet.Len()
			// Zero configured coalescing: drain only senders already ready. No
			// timer and no wait for another request. A large first frame stands alone.
		collect:
			for count < len(pending) && bytes < 64<<10 {
				select {
				case next := <-w.ready:
					// A request that would exceed the byte limit is sent in the
					// next batch; keep it locally without losing its ticket.
					pending[count] = next
					count++
					bytes += next.packet.Len()
					if bytes >= 64<<10 {
						break collect
					}
				default:
					break collect
				}
			}
			// A batch may include a final frame beyond the threshold above.
			// Split before transmission to keep the strict 64 KiB batch bound.
			for start := 0; start < count; {
				end, total := start, int64(0)
				spans = spans[:0]
				for end < count {
					t := pending[end]
					if total > 0 && t.packet.Len() > (64<<10)-total {
						break
					}
					end++
					if t.ctx.Err() != nil || w.conn.InternalCtx.Err() != nil {
						t.phase.CompareAndSwap(0, 2)
					}
					if !t.phase.CompareAndSwap(0, 1) {
						err := t.ctx.Err()
						if err == nil {
							err = ErrConnectionClosed
						}
						t.finish(InternalWriteResult{InternalErr: err})
						pending[end-1] = nil
						continue
					}
					_ = t.packet.Each(func(s wire.Span) error { spans = append(spans, s); return nil })
					total += t.packet.Len()
				}
				var written int64
				var err error
				if total > 0 {
					if writer, ok := w.conn.InternalW.(InternalSpanListWriter); ok {
						written, err = writer.InternalWriteSpanList(w.conn.InternalCtx, spans, total)
					} else {
						packet, buildErr := wire.NewChain(spans...)
						err = buildErr
						if err == nil {
							written, err = w.conn.InternalW.(spanContextWriter).InternalWriteSpans(w.conn.InternalCtx, packet)
						}
					}
				}
				remaining := written
				for i := start; i < end; i++ {
					t := pending[i]
					pending[i] = nil
					if t == nil {
						continue
					}
					n := min(max(remaining, 0), t.packet.Len())
					requestErr := err
					if n == t.packet.Len() {
						requestErr = nil
					}
					t.finish(InternalWriteResult{InternalWritten: n, InternalErr: requestErr})
					remaining -= n
				}
				clear(spans)
				if cap(spans) > 128 {
					spans = nil
				}
				// Publish completions before Close joins the calls waiting on us.
				if err != nil {
					w.conn.InternalCloseWithError(err)
				}
				start = end
			}
		}
	}
}
