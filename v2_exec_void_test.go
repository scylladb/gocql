package gocql

import (
	"context"
	"encoding/binary"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	frm "github.com/scylladb/gocql/v2/internal/frame"
)

type execVoidObserver struct {
	mu        sync.Mutex
	requests  []RequestEvent
	attempts  []AttemptEvent
	onAttempt func(AttemptEvent)
}

func (o *execVoidObserver) ObserveRequest(event RequestEvent) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.requests = append(o.requests, event)
}
func (o *execVoidObserver) ObserveAttempt(event AttemptEvent) {
	o.mu.Lock()
	o.attempts = append(o.attempts, event)
	o.mu.Unlock()
	if o.onAttempt != nil {
		o.onAttempt(event)
	}
}

func TestV2RuntimeExecVoidCompletionAndMetadata(t *testing.T) {
	s, conns, peers := runtimeTestSession(t, 1)
	trace := UUID{15: 42}
	served := make(chan struct{})
	go func() {
		defer close(served)
		stream := typedTestReadRequest(t, conns[0], peers[0])
		body := append([]byte{}, trace[:]...)
		body = binary.BigEndian.AppendUint16(body, 1)
		body = binary.BigEndian.AppendUint16(body, uint16(len("warning")))
		body = append(body, "warning"...)
		body = binary.BigEndian.AppendUint16(body, 1) // custom payload entries
		body = binary.BigEndian.AppendUint16(body, 3)
		body = append(body, "key"...)
		body = binary.BigEndian.AppendUint32(body, 3)
		body = append(body, "val"...)
		body = binary.BigEndian.AppendUint32(body, frm.ResultKindVoid)
		response := typedTestResponse(t, 4, stream, body)
		response[1] = frm.FlagTracing | frm.FlagWarning | frm.FlagCustomPayload
		if _, err := peers[0].Write(response); err != nil {
			t.Error(err)
		}
	}()
	b := runtimeQuery(t) // Exec must ignore this binding's Bytes row decoder.
	observer := new(execVoidObserver)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	result, err := Exec(ctx, s, b, Options{Trace: true, Observer: observer})
	if err != nil || result.Attempts != 1 || result.Rows != 0 || len(result.TraceIDs) != 1 || result.TraceIDs[0] != trace || len(result.Warnings) != 1 || result.Warnings[0] != "warning" {
		t.Fatalf("result = %+v, %v", result, err)
	}
	if len(result.CustomPayload) != 1 || result.CustomPayload[0].Key != "key" || string(result.CustomPayload[0].Value.Value.OwnedBytes()) != "val" {
		t.Fatalf("custom payload = %+v", result.CustomPayload)
	}
	b.Close()
	if err := b.Wait(ctx); err != nil {
		t.Fatal(err)
	}
	<-served
	observer.mu.Lock()
	requests, attempts := observer.requests, observer.attempts
	observer.mu.Unlock()
	if len(requests) != 1 || len(attempts) != 1 || requests[0].Err != nil || requests[0].Result.TraceIDs[0] != trace || requests[0].Result.Warnings[0] != "warning" {
		t.Fatalf("observer events = %+v / %+v", requests, attempts)
	}
	if len(requests[0].Result.CustomPayload) != 1 || string(requests[0].Result.CustomPayload[0].Value.Value.OwnedBytes()) != "val" {
		t.Fatalf("observer custom payload = %+v", requests[0].Result.CustomPayload)
	}
	if m := s.Metrics(); m.Requests != 1 || m.Attempts != 1 || m.InFlight != 0 || m.Errors != 0 {
		t.Fatal(m)
	}
	if ctx.Err() != nil {
		t.Fatal("Exec cancelled its caller context")
	}
}

func TestV2RuntimeExecDrainsAndValidatesNonVoidReplies(t *testing.T) {
	emptyTrailing := binary.BigEndian.AppendUint32(nil, frm.ResultKindRows)
	emptyTrailing = binary.BigEndian.AppendUint32(emptyTrailing, 0) // flags
	emptyTrailing = binary.BigEndian.AppendUint32(emptyTrailing, 0) // columns
	emptyTrailing = binary.BigEndian.AppendUint32(emptyTrailing, 0) // rows
	emptyTrailing = append(emptyTrailing, 99)
	for _, tc := range []struct {
		name   string
		bodies [][]byte
		rows   int64
		column bool
	}{
		{"empty-columns", [][]byte{runtimeRowsBody(nil)}, 0, true},
		{"paged-columns", [][]byte{runtimeRowsBody([]byte{1}, "first"), runtimeRowsBody(nil, "second")}, 2, true},
		{"malformed-empty-rows", [][]byte{emptyTrailing}, 0, false},
		{"malformed-void", [][]byte{{0, 0, 0, frm.ResultKindVoid, 99}}, 0, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, conns, peers := runtimeTestSession(t, 1)
			served := make(chan struct{})
			go func() {
				defer close(served)
				for _, body := range tc.bodies {
					stream := typedTestReadRequest(t, conns[0], peers[0])
					if _, err := peers[0].Write(typedTestResponse(t, 4, stream, body)); err != nil {
						t.Error(err)
						return
					}
				}
			}()
			b := runtimeQuery(t)
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			result, err := Exec(ctx, s, b, Options{})
			if err == nil || result.Rows != tc.rows {
				t.Fatalf("result = %+v, %v", result, err)
			}
			if tc.column && (!strings.Contains(err.Error(), "result columns require Run") || result.Attempts != len(tc.bodies)) {
				t.Fatalf("paged column rejection changed: %+v, %v", result, err)
			}
			b.Close()
			if err := b.Wait(ctx); err != nil {
				t.Fatal(err)
			}
			<-served
			if m := s.Metrics(); m.Requests != 1 || m.InFlight != 0 || m.Errors != 1 {
				t.Fatal(m)
			}
		})
	}
}

func TestV2RuntimeExecCancellationDrainsAcceptedInput(t *testing.T) {
	for _, closeSession := range []bool{false, true} {
		name := "caller"
		if closeSession {
			name = "session"
		}
		t.Run(name, func(t *testing.T) {
			s, conns, peers := runtimeTestSession(t, 1)
			accepted := make(chan struct{})
			reply := make(chan struct{})
			served := make(chan struct{})
			go func() {
				defer close(served)
				stream := typedTestReadRequest(t, conns[0], peers[0])
				close(accepted)
				<-reply
				_, _ = peers[0].Write(typedTestResponse(t, 4, stream, []byte{0, 0, 0, frm.ResultKindVoid}))
			}()
			b := runtimeQuery(t)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := make(chan error, 1)
			go func() {
				_, err := Exec(ctx, s, b, Options{})
				done <- err
			}()
			<-accepted
			b.Close()
			short, stop := context.WithTimeout(context.Background(), 5*time.Millisecond)
			if err := b.Wait(short); !errors.Is(err, context.DeadlineExceeded) {
				t.Fatal("accepted input was released before acknowledgement/cancellation", err)
			}
			stop()
			if closeSession {
				if err := s.Close(); err != nil {
					t.Fatal(err)
				}
			} else {
				cancel()
			}
			select {
			case err := <-done:
				if !errors.Is(err, context.Canceled) && !errors.Is(err, ErrClosed) {
					t.Fatal(err)
				}
			case <-time.After(time.Second):
				t.Fatal("Exec did not stop after cancellation")
			}
			close(reply)
			joined, stop := context.WithTimeout(context.Background(), time.Second)
			defer stop()
			if err := b.Wait(joined); err != nil {
				t.Fatal(err)
			}
			<-served
			if m := s.Metrics(); m.Requests != 1 || m.InFlight != 0 || m.Errors != 1 {
				t.Fatal(m)
			}
		})
	}
}

func TestV2RuntimeExecCancellationAfterVoidAcknowledgement(t *testing.T) {
	for _, closeSession := range []bool{false, true} {
		name := "caller"
		if closeSession {
			name = "session"
		}
		t.Run(name, func(t *testing.T) {
			s, conns, peers := runtimeTestSession(t, 1)
			acknowledged := make(chan struct{})
			continueAttempt := make(chan struct{})
			observer := &execVoidObserver{onAttempt: func(event AttemptEvent) {
				if event.Err == nil {
					// The native VOID reply has been parsed, but execution has not
					// accepted the successful attempt's outcome yet.
					close(acknowledged)
					<-continueAttempt
				}
			}}
			go func() {
				stream := typedTestReadRequest(t, conns[0], peers[0])
				_, _ = peers[0].Write(typedTestResponse(t, 4, stream, []byte{0, 0, 0, frm.ResultKindVoid}))
			}()
			b := runtimeQuery(t)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := make(chan error, 1)
			go func() {
				_, err := Exec(ctx, s, b, Options{Observer: observer})
				done <- err
			}()
			select {
			case <-acknowledged:
			case <-time.After(time.Second):
				t.Fatal("VOID acknowledgement was not observed")
			}
			closed := make(chan error, 1)
			if closeSession {
				go func() { closed <- s.Close() }()
			} else {
				cancel()
			}
			select {
			case err := <-done:
				if !errors.Is(err, context.Canceled) && !errors.Is(err, ErrClosed) {
					t.Fatal(err)
				}
			case <-time.After(time.Second):
				t.Fatal("Exec ignored cancellation after the acknowledgement")
			}
			close(continueAttempt)
			b.Close()
			joined, stop := context.WithTimeout(context.Background(), time.Second)
			defer stop()
			if err := b.Wait(joined); err != nil {
				t.Fatal(err)
			}
			if closeSession {
				select {
				case err := <-closed:
					if err != nil {
						t.Fatal(err)
					}
				case <-time.After(time.Second):
					t.Fatal("session close did not join the acknowledged attempt")
				}
			}
			if m := s.Metrics(); m.Requests != 1 || m.Attempts != 1 || m.InFlight != 0 || m.Errors != 1 {
				t.Fatal(m)
			}
		})
	}
}
