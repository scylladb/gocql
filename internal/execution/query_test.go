// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package execution

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/scylladb/gocql/v2/internal/codec"
	"github.com/scylladb/gocql/v2/internal/cqltype"
)

func testOwnedQuery(t *testing.T) *Query[[]byte, Unit] {
	t.Helper()
	s, _ := attachmentSession()
	typ, err := cqltype.ScalarType(cqltype.TypeBlob)
	if err != nil {
		t.Fatal(err)
	}
	q, err := NewUnpreparedQuery(s, Statement[[]byte, Unit]{Kind: OrdinaryStatement, CQL: "INSERT INTO ks.t(v) VALUES(?)", Params: codec.OneParam(codec.BytesEncoder), Rows: codec.NoRows}, cqltype.NewColumns(cqltype.ColumnSpec{Name: "v", Type: typ}), Options{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(q.Close)
	return q
}

func TestUninitializedQueryReturnsValidationErrors(t *testing.T) {
	var q Query[Unit, Unit]
	if _, err := q.Exec(context.Background(), Unit{}); err == nil {
		t.Fatal("zero query executed")
	}
	q.Close()
	if err := q.Wait(context.Background()); err == nil {
		t.Fatal("zero query wait succeeded")
	}
}

func TestQueryOwnsInputAndRejectsOverlapUntilWriterDrain(t *testing.T) {
	q := testOwnedQuery(t)
	input := []byte("original")
	if err := q.capture(context.Background(), input); err != nil {
		t.Fatal(err)
	}
	input[0] = 'X'
	cell := q.bound.privateStorage.InternalRequest.InternalValues.Cells[0].Body
	got := make([]byte, cell.Len())
	_, _ = cell.CopyTo(got)
	if string(got) != "original" {
		t.Fatal(string(got))
	}
	if err := q.capture(context.Background(), input); !errors.Is(err, ErrBusy) {
		t.Fatal(err)
	}
	writer := q.state.owner.InternalWriterLease(q.bound.privateStorage.InternalRequest.InternalValues.Framed)
	q.state.owner.InternalRelease() // execution returned after cancellation
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	if err := q.Wait(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	if err := q.capture(context.Background(), input); !errors.Is(err, ErrBusy) {
		t.Fatal(err)
	}
	writer.Release()
	if err := q.Wait(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := q.capture(context.Background(), input); err != nil {
		t.Fatal(err)
	}
	q.state.owner.InternalRelease()
	q.Close()
	if err := q.capture(context.Background(), input); !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}
}

func TestQueryConcurrentAdmissionAndClose(t *testing.T) {
	q := testOwnedQuery(t)
	var wg sync.WaitGroup
	start, winner := make(chan struct{}), make(chan struct{}, 1)
	for range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			err := q.capture(context.Background(), []byte("capture"))
			if err == nil {
				winner <- struct{}{}
			} else if !errors.Is(err, ErrBusy) {
				t.Error(err)
			}
		}()
	}
	close(start)
	wg.Wait()
	if len(winner) != 1 {
		t.Fatal("admissions", len(winner))
	}
	q.Close()
	q.state.owner.InternalRelease()
	if err := q.Wait(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestClosedQueryRemainsRegisteredUntilAcceptedWritesDrain(t *testing.T) {
	q := testOwnedQuery(t)
	if err := q.capture(context.Background(), []byte("pending")); err != nil {
		t.Fatal(err)
	}
	writer := q.state.owner.InternalWriterLease(q.bound.privateStorage.InternalRequest.InternalValues.Framed)
	q.state.owner.InternalRelease()
	q.Close()
	q.state.runtime.InternalMu.Lock()
	_, registered := q.state.runtime.queries[&q.state]
	q.state.runtime.InternalMu.Unlock()
	if !registered {
		t.Fatal("session lost a closed query's pending transport storage")
	}
	drain := q.state.runtime.StopActive()
	done := make(chan struct{})
	go func() { drain.Wait(); close(done) }()
	select {
	case <-done:
		t.Fatal("session drain bypassed accepted write")
	default:
	}
	writer.Release()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("session did not join transport drain")
	}
}

func TestQueryRetiresInlineExecutionCaptures(t *testing.T) {
	q := testOwnedQuery(t)
	if err := q.capture(context.Background(), []byte("owned")); err != nil {
		t.Fatal(err)
	}
	_, cancel, request, err := InternalAdmitExecution(context.Background(), q.state.runtime.Session, &q.bound, Options{})
	if err != nil {
		t.Fatal(err)
	}
	if request.InternalLogical != &q.state.inlineLogical {
		t.Fatal("ordinary execution was not embedded")
	}
	request.InternalLogical.installStop(func() { t.Error("retired stop callback invoked") })
	cancel()
	request.InternalLogical.InternalFinish(Result{}, nil)
	request.InternalLogical.InternalReleaseInput()
	q.state.owner.InternalRelease()
	if q.state.inlineLogical.lifetime.stop != nil || q.state.request.InternalValues.Framed.Len() != 0 || q.state.request.InternalLogical != nil {
		t.Fatal("idle execution retained captures")
	}
}

func TestQueryIdleRetentionAndBatchCapture(t *testing.T) {
	q := testOwnedQuery(t)
	input := bytes.Repeat([]byte{37}, 128<<10)
	if err := q.capture(context.Background(), input); err != nil {
		t.Fatal(err)
	}
	q.state.owner.InternalRelease()
	if q.buffer.RetainedBytes() > 64<<10 {
		t.Fatal("oversized idle storage")
	}
	input = []byte("batch entry")
	entry, err := q.BatchEntry(input)
	if err != nil {
		t.Fatal(err)
	}
	input[0] = 'X'
	if err := q.capture(context.Background(), []byte("replacement")); err != nil {
		t.Fatal(err)
	}
	q.state.owner.InternalRelease()
	body := entry.privateStorage.InternalRequest.InternalValues.Cells[0].Body
	got := make([]byte, body.Len())
	_, _ = body.CopyTo(got)
	if string(got) != "batch entry" {
		t.Fatal(string(got))
	}
	if entry.privateStorage.InternalRequest.query != nil {
		t.Fatal("batch retained reusable query")
	}
	if len(q.state.runtime.InternalExecutions) != 0 || len(q.state.runtime.queries) != 1 {
		t.Fatal("per-operation registration")
	}
}
