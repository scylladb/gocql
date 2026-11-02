// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package transport

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/scylladb/gocql/v2/internal/wire"
)

type queuedWriteProbe struct {
	bytes.Buffer
	deadlines int
}

func (p *queuedWriteProbe) SetWriteDeadline(time.Time) error { p.deadlines++; return nil }

func TestCoalescerCancelsQueuedFrameBeforeTransmission(t *testing.T) {
	probe := new(queuedWriteProbe)
	quit, tick, armed := make(chan struct{}), make(chan time.Time), make(chan struct{})
	w := &InternalWriteCoalescer{InternalC: probe, InternalQuit: quit, InternalWriteCh: make(chan InternalWriteRequest)}
	flushed := make(chan struct{})
	go func() { w.InternalWriteFlusherImpl(tick, func() { close(armed) }); close(flushed) }()
	ctx, cancel := context.WithCancel(context.Background())
	chain, _ := wire.NewChain(wire.Borrow([]byte("must not be sent")))
	done := make(chan InternalWriteResult, 1)
	go func() {
		n, err := w.InternalWriteSpans(ctx, chain)
		done <- InternalWriteResult{InternalWritten: n, InternalErr: err}
	}()
	<-armed
	cancel()
	tick <- time.Now()
	result := <-done
	if result.InternalWritten != 0 || !errors.Is(result.InternalErr, context.Canceled) {
		t.Fatal(result)
	}
	close(quit)
	<-flushed
	if probe.Len() != 0 || probe.deadlines != 0 {
		t.Fatal("cancelled queued frame reached socket", probe.Len(), probe.deadlines)
	}
}
