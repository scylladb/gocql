// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package transport

import (
	"bytes"
	"context"
	"errors"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/scylladb/gocql/v2/internal/wire"
)

type scratchTestWriter struct {
	bytes.Buffer
	outcome string
}

func (*scratchTestWriter) SetWriteDeadline(time.Time) error { return nil }
func (w *scratchTestWriter) Write(p []byte) (int, error) {
	switch w.outcome {
	case "short":
		return 0, nil
	case "error":
		return 0, io.ErrClosedPipe
	case "panic":
		panic("writer panic")
	}
	return w.Buffer.Write(p)
}

func scratchTestChain(t *testing.T, parts ...[]byte) wire.Chain {
	t.Helper()
	spans := make([]wire.Span, len(parts))
	for i, part := range parts {
		spans[i] = wire.Borrow(part)
	}
	chain, err := wire.NewChain(spans...)
	if err != nil {
		t.Fatal(err)
	}
	return chain
}

func TestTypedWriterScratchReleasesPayloadsOnEveryExit(t *testing.T) {
	for _, outcome := range []string{"success", "short", "error", "panic"} {
		t.Run(outcome, func(t *testing.T) {
			sink := &scratchTestWriter{outcome: outcome}
			writer := &InternalDeadlineContextWriter{InternalW: sink, InternalSemaphore: make(chan struct{}, 1), InternalQuit: make(chan struct{})}
			chain := scratchTestChain(t, []byte("first"), []byte("second"))
			var err error
			var caught any
			func() {
				defer func() { caught = recover() }()
				_, err = writer.InternalWriteSpans(context.Background(), chain)
			}()
			switch outcome {
			case "success":
				if err != nil || sink.String() != "firstsecond" {
					t.Fatalf("write = %q, %v", sink.String(), err)
				}
			case "short":
				if !errors.Is(err, io.ErrShortWrite) {
					t.Fatal(err)
				}
			case "error":
				if !errors.Is(err, io.ErrClosedPipe) {
					t.Fatal(err)
				}
			case "panic":
				if caught == nil {
					t.Fatal("writer panic lost")
				}
			}
			if len(writer.InternalSemaphore) != 0 || len(writer.spanScratch) != 0 {
				t.Fatal("write did not release scratch/semaphore")
			}
			for _, part := range writer.spanScratch[:cap(writer.spanScratch)] {
				if part != nil {
					t.Fatal("idle scratch retains borrowed request payload")
				}
			}
			// A terminal write must leave the next request independent, even after
			// a partially written or panicking predecessor.
			sink.outcome = "success"
			sink.Reset()
			if _, err := writer.InternalWriteSpans(context.Background(), chain); err != nil || sink.String() != "firstsecond" {
				t.Fatalf("next write = %q, %v", sink.String(), err)
			}
		})
	}
}

func TestTypedWriterScratchBoundAndConcurrentOrdering(t *testing.T) {
	sink := &scratchTestWriter{}
	writer := &InternalDeadlineContextWriter{InternalW: sink, InternalSemaphore: make(chan struct{}, 1), InternalQuit: make(chan struct{})}
	parts := make([][]byte, 17)
	for i := range parts {
		parts[i] = []byte{byte(i)}
	}
	if _, err := writer.InternalWriteSpans(context.Background(), scratchTestChain(t, parts...)); err != nil {
		t.Fatal(err)
	}
	if writer.spanScratch != nil {
		t.Fatal("oversized descriptor storage retained")
	}
	sink.Reset()
	var wg sync.WaitGroup
	for i := range 32 {
		chain := scratchTestChain(t, []byte{byte(i)}, []byte{byte(i)})
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := writer.InternalWriteSpans(context.Background(), chain); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	if sink.Len() != 64 {
		t.Fatalf("wrong byte count: %d", sink.Len())
	}
	for i := 0; i < sink.Len(); i += 2 {
		if sink.Bytes()[i] != sink.Bytes()[i+1] {
			t.Fatal("concurrent request spans interleaved")
		}
	}
}
