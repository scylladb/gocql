// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package transport

import (
	"context"
	"errors"
	"testing"

	"github.com/scylladb/gocql/v2/internal/wire"
)

func TestQueuedTicketConnectionLossRemainsRetryable(t *testing.T) {
	ctx, closeConnection := context.WithCancel(context.Background())
	defer closeConnection()
	conn := &Conn{InternalCtx: ctx}
	// Accept the ticket, but hold it before the writer claims transmission.
	conn.typedWriterOnce.Do(func() { conn.typedWriter = &submissionWriter{conn: conn, ready: make(chan *writeTicket)} })
	accepted := make(chan *writeTicket, 1)
	go func() { accepted <- <-conn.typedWriter.ready }()
	chain, _ := wire.NewChain(wire.Borrow([]byte("queued request")))
	drained := make(chan struct{})
	lease := wire.NewLease(chain, func() { close(drained) })
	done := make(chan InternalWriteResult, 1)
	go func() {
		n, err := conn.submitTyped(context.Background(), chain, lease)
		done <- InternalWriteResult{InternalWritten: n, InternalErr: err}
	}()
	ticket := <-accepted
	closeConnection()
	result := <-done
	if result.InternalWritten != 0 || !errors.Is(result.InternalErr, ErrConnectionClosed) || errors.Is(result.InternalErr, context.Canceled) {
		t.Fatal(result)
	}
	select {
	case <-drained:
		t.Fatal("accepted ticket storage drained before writer completion")
	default:
	}
	ticket.finish(InternalWriteResult{InternalErr: ErrConnectionClosed})
	<-drained
}
