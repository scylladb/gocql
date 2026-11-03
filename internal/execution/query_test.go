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

func testOwnedQuery(t testing.TB) *Query[[]byte, Unit] {
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

func TestQueryWaitCompletesForItsCapturedGeneration(t *testing.T) {
	q := testOwnedQuery(t)
	if err := q.capture(context.Background(), []byte("first")); err != nil {
		t.Fatal(err)
	}
	writer := q.state.owner.InternalWriterLease(q.bound.privateStorage.InternalRequest.InternalValues.Framed)
	q.state.owner.InternalRelease()
	waited := make(chan error, 1)
	go func() { waited <- q.Wait(context.Background()) }()
	deadline := time.Now().Add(time.Second)
	for {
		q.state.mu.Lock()
		waiting := q.state.done != nil
		q.state.mu.Unlock()
		if waiting {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("waiter did not register")
		}
		time.Sleep(time.Millisecond)
	}
	writer.Release()
	if err := q.capture(context.Background(), []byte("second")); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-waited:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("old waiter joined the new generation")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	if err := q.Wait(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("new generation reused a completed notification", err)
	}
	writer.Release() // an old lease's repeated release must not drain the new input
	if err := q.capture(context.Background(), []byte("overlap")); !errors.Is(err, ErrBusy) {
		t.Fatal(err)
	}
	q.state.owner.InternalRelease()
}

func TestQuerySessionCloseRacesExecutionRegistration(t *testing.T) {
	for range 30 {
		q := testOwnedQuery(t)
		if err := q.capture(context.Background(), []byte("captured")); err != nil {
			t.Fatal(err)
		}
		start, closed := make(chan struct{}), make(chan struct{})
		go func() { <-start; _ = q.state.runtime.Session.Close(); close(closed) }()
		close(start)
		_, cancel, request, err := InternalAdmitExecution(context.Background(), q.state.runtime.Session, &q.bound, Options{})
		if err == nil {
			cancel()
			request.InternalLogical.InternalFinish(Result{}, nil)
			request.InternalLogical.InternalReleaseInput()
		} else if !errors.Is(err, ErrClosed) {
			t.Fatal(err)
		}
		q.state.owner.InternalRelease()
		select {
		case <-closed:
		case <-time.After(time.Second):
			t.Fatal("session close did not join admitted capture")
		}
		if err := q.capture(context.Background(), []byte("late")); !errors.Is(err, ErrClosed) {
			t.Fatal(err)
		}
	}
}

type queryRequestObserver struct{ request func() }

func (o queryRequestObserver) ObserveRequest(RequestEvent) { o.request() }
func (queryRequestObserver) ObserveAttempt(AttemptEvent)   {}

func TestQueryRequestObserverCanCloseQuery(t *testing.T) {
	q := testOwnedQuery(t)
	if err := q.capture(context.Background(), []byte("observer")); err != nil {
		t.Fatal(err)
	}
	_, cancel, request, err := InternalAdmitExecution(context.Background(), q.state.runtime.Session, &q.bound, Options{Observer: queryRequestObserver{request: q.Close}})
	if err != nil {
		t.Fatal(err)
	}
	defer cancel()
	logical := request.InternalLogical
	logical.installStop(func() { logical.finishAndRelease(Result{}, ErrClosed) })
	q.state.owner.InternalRelease()
	done := make(chan struct{})
	go func() { logical.finishAndRelease(Result{}, nil); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("observer deadlocked while closing its query")
	}
	ctx, stop := context.WithTimeout(context.Background(), time.Second)
	defer stop()
	if err := q.Wait(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestQuerySessionCloseJoinsRequestObserver(t *testing.T) {
	q := testOwnedQuery(t)
	if err := q.capture(context.Background(), []byte("observer")); err != nil {
		t.Fatal(err)
	}
	entered, release := make(chan struct{}), make(chan struct{})
	_, cancel, request, err := InternalAdmitExecution(context.Background(), q.state.runtime.Session, &q.bound, Options{Observer: queryRequestObserver{request: func() { close(entered); <-release }}})
	if err != nil {
		t.Fatal(err)
	}
	defer cancel()
	logical := request.InternalLogical
	logical.installStop(func() { logical.finishAndRelease(Result{}, ErrClosed) })
	q.state.owner.InternalRelease()
	finished := make(chan struct{})
	go func() { logical.finishAndRelease(Result{}, nil); close(finished) }()
	<-entered
	closed := make(chan struct{})
	go func() { _ = q.state.runtime.Session.Close(); close(closed) }()
	deadline := time.Now().Add(time.Second)
	for {
		q.state.mu.Lock()
		stopping := q.state.closed
		q.state.mu.Unlock()
		if stopping {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("session did not stop query admission")
		}
		time.Sleep(time.Millisecond)
	}
	select {
	case <-closed:
		t.Fatal("session close bypassed blocked request observer")
	case <-time.After(time.Millisecond):
	}
	close(release)
	for _, done := range []<-chan struct{}{finished, closed} {
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatal("observer did not drain")
		}
	}
}

func TestQueryGenerationAccessDelaysStorageRetirement(t *testing.T) {
	q := testOwnedQuery(t)
	if err := q.capture(context.Background(), []byte("pinned")); err != nil {
		t.Fatal(err)
	}
	cursor := queryCursor{query: &q.state, generation: q.state.generation}
	if !cursor.acquire() {
		t.Fatal("could not pin generation")
	}
	q.state.owner.InternalRelease()
	if err := q.capture(context.Background(), []byte("overlap")); !errors.Is(err, ErrBusy) {
		t.Fatal("execution reused while cursor method remained active", err)
	}
	wait, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	if err := q.Wait(wait); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	cursor.release()
	if err := q.capture(context.Background(), []byte("new")); err != nil {
		t.Fatal(err)
	}
	if cursor.acquire() {
		t.Fatal("old generation acquired reused storage")
	}
	q.state.owner.InternalRelease()
}
