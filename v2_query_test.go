// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package gocql

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"testing"
	"time"

	frm "github.com/scylladb/gocql/v2/internal/frame"
	"github.com/scylladb/gocql/v2/internal/testkit"
	"github.com/scylladb/gocql/v2/internal/wirecompress"
)

func TestQueryPageBufferedProtocolTLSMatrix(t *testing.T) {
	modes := append([]step08Mode(nil), step08Modes...)
	modes = append(modes, step08Mode{"v3lz4frame", 3, wirecompress.LZ4Compressor{}, false}, step08Mode{"v4lz4frame", 4, wirecompress.LZ4Compressor{}, false}, step08Mode{"v4snappyframe", 4, wirecompress.SnappyCompressor{}, false}, step08Mode{"v4lz4frametls", 4, wirecompress.LZ4Compressor{}, true})
	for _, mode := range modes {
		t.Run(mode.name, func(t *testing.T) {
			s, c, peer := step08StreamSession(t, mode)
			query := codecTestMust(NewUnpreparedStreamQuery(s, Statement[Unit, Bytes]{Kind: OrdinaryStatement, CQL: "SELECT value FROM ks.tbl", Params: NoParams, Rows: OneColumn(BlobCodec)}, NewColumns(), StreamOneColumn(StreamBlob), StreamOptions{}))
			defer query.Close()
			served := make(chan struct{})
			go func() {
				defer close(served)
				stream := typedTestReadRequest(t, c, peer)
				packet := step08Packet(mode, testkit.BlobRows(mode.version, uint16(stream), []byte("complete")))
				if mode.version < 5 && mode.compression != nil {
					compressed, err := mode.compression.Encode(packet[9:])
					if err != nil {
						t.Error(err)
						return
					}
					packet[1] |= frm.FlagCompress
					binary.BigEndian.PutUint32(packet[5:9], uint32(len(compressed)))
					packet = append(packet[:9], compressed...)
				}
				for i := 0; i < len(packet); i++ {
					if _, err := peer.Write(packet[i : i+1]); err != nil {
						t.Error(err)
						return
					}
				}
			}()
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			cursor := codecTestMust(query.Run(ctx, Unit{}))
			defer cursor.Close()
			dst := make([]byte, 0, 16)
			if !cursor.NextInto(&dst) || string(dst) != "complete" || cursor.NextInto(&dst) || cursor.Err() != nil {
				t.Fatal(string(dst), cursor.Err())
			}
			<-served
		})
	}
}

func TestQueryBufferedStreamsMultiplexAndReuseSeededStorage(t *testing.T) {
	s, conns, peers := runtimeTestSession(t, 1)
	statement := Statement[Unit, Bytes]{Kind: OrdinaryStatement, CQL: "SELECT value FROM ks.tbl", Params: NoParams, Rows: OneColumn(BlobCodec)}
	q := codecTestMust(NewUnpreparedStreamQuery(s, statement, NewColumns(), StreamOneColumn(StreamBlob), StreamOptions{}))
	other := codecTestMust(NewUnpreparedStreamQuery(s, statement, NewColumns(), StreamOneColumn(StreamBlob), StreamOptions{}))
	defer q.Close()
	defer other.Close()
	served := make(chan struct{})
	go func() {
		defer close(served)
		for _, body := range [][]byte{runtimeRowsBody([]byte{1}, "one"), runtimeRowsBody(nil, "other"), runtimeRowsBody(nil, "two"), runtimeRowsBody(nil, "again")} {
			stream := typedTestReadRequest(t, conns[0], peers[0])
			if _, err := peers[0].Write(typedTestResponse(t, 4, stream, body)); err != nil {
				t.Error(err)
				return
			}
		}
	}()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	first := codecTestMust(q.Run(ctx, Unit{}))
	if _, err := q.Run(ctx, Unit{}); !errors.Is(err, ErrBusy) {
		t.Fatal(err)
	}
	second := codecTestMust(other.Run(ctx, Unit{})) // shares the one connection
	output := make([]byte, 0, 16)
	backing := &output[:cap(output)][0]
	if !first.NextInto(&output) || string(output) != "one" {
		t.Fatal(first.Err(), string(output))
	}
	var otherOutput = make([]byte, 0, 16)
	if !second.NextInto(&otherOutput) || string(otherOutput) != "other" || second.NextInto(&otherOutput) {
		t.Fatal(second.Err())
	}
	if err := second.Close(); err != nil {
		t.Fatal(err)
	}
	if !first.NextInto(&output) || string(output) != "two" || first.NextInto(&output) {
		t.Fatal(first.Err())
	}
	if &output[0] != backing {
		t.Fatal("seeded destination was replaced")
	}
	if err := q.Wait(ctx); err != nil {
		t.Fatal(err)
	}
	reused := codecTestMust(q.Run(ctx, Unit{}))
	if err := first.Close(); err != nil {
		t.Fatal(err)
	} // stale cursor cannot cancel reuse
	if first.NextInto(&output) {
		t.Fatal("stale cursor advanced")
	}
	if !reused.NextInto(&output) || string(output) != "again" || reused.NextInto(&output) {
		t.Fatal(reused.Err())
	}
	if err := reused.Close(); err != nil {
		t.Fatal(err)
	}
	if info := reused.Info(); info.DeliveredRows != 1 || info.Pages != 1 {
		t.Fatal(info)
	}
	<-served
}

func TestQueryCancelledAcceptedWriteStaysBusyUntilDrain(t *testing.T) {
	s, conns, peers := runtimeTestSession(t, 1)
	entered := make(chan struct{})
	conns[0].InternalW = &deadlineContextWriter{InternalW: &step04AcceptedWriter{Conn: conns[0].InternalR.(*connReader).InternalConn, entered: entered}, InternalSemaphore: make(chan struct{}, 1), InternalQuit: make(chan struct{})}
	typ := codecTestMust(ScalarType(TypeBlob))
	q := codecTestMust(NewUnpreparedQuery(s, Statement[[]byte, Unit]{Kind: OrdinaryStatement, CQL: "INSERT INTO ks.tbl(value)VALUES(?)", Params: OneParam(BytesEncoder), Rows: NoRows}, NewColumns(ColumnSpec{Name: "value", Type: typ}), Options{}))
	defer q.Close()
	input := bytes.Repeat([]byte{31}, 128<<10)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { _, err := q.Exec(ctx, input); done <- err }()
	<-entered
	input[0] = 99 // execution has captured an owned snapshot
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("cancellation joined a blocked writer")
	}
	if _, err := q.Routing(input); !errors.Is(err, ErrBusy) {
		t.Fatal(err)
	}
	served := make(chan struct{})
	go func() {
		defer close(served)
		for range 2 {
			stream := typedTestReadRequest(t, conns[0], peers[0])
			if _, err := peers[0].Write(typedTestResponse(t, 4, stream, []byte{0, 0, 0, frm.ResultKindVoid})); err != nil {
				t.Error(err)
				return
			}
		}
	}()
	wait, stop := context.WithTimeout(context.Background(), time.Second)
	defer stop()
	if err := q.Wait(wait); err != nil {
		t.Fatal(err)
	}
	if _, err := q.Exec(wait, []byte("reuse")); err != nil {
		t.Fatal(err)
	}
	<-served
}

func TestQueryBlockedObserverKeepsQueryBusy(t *testing.T) {
	s, conns, peers := runtimeTestSession(t, 1)
	entered, release := make(chan struct{}), make(chan struct{})
	observer := &execVoidObserver{onAttempt: func(AttemptEvent) { close(entered); <-release }}
	query := codecTestMust(NewUnpreparedQuery(s, Statement[Unit, Unit]{Kind: OrdinaryStatement, CQL: "INSERT INTO ks.tbl(k)VALUES(1)", Params: NoParams, Rows: NoRows}, NewColumns(), Options{Observer: observer}))
	defer query.Close()
	served := make(chan struct{})
	go func() {
		defer close(served)
		for range 2 {
			stream := typedTestReadRequest(t, conns[0], peers[0])
			if _, err := peers[0].Write(typedTestResponse(t, 4, stream, []byte{0, 0, 0, frm.ResultKindVoid})); err != nil {
				t.Error(err)
				return
			}
		}
	}()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { _, err := query.Exec(ctx, Unit{}); done <- err }()
	<-entered
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	wait, stop := context.WithTimeout(context.Background(), time.Millisecond)
	defer stop()
	if err := query.Wait(wait); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	if _, err := query.Routing(Unit{}); !errors.Is(err, ErrBusy) {
		t.Fatal(err)
	}
	close(release)
	joined, finish := context.WithTimeout(context.Background(), time.Second)
	defer finish()
	if err := query.Wait(joined); err != nil {
		t.Fatal(err)
	}
	observer.onAttempt = nil
	if _, err := query.Exec(joined, Unit{}); err != nil {
		t.Fatal(err)
	}
	<-served
}

func TestQueryOwnedRowsPreserveDestinationOnCompletion(t *testing.T) {
	s, conns, peers := runtimeTestSession(t, 1)
	type output struct{ Value []byte }
	decoder := RowInto(func(*output) {}, Column("value", OwnedBlobCodec, func(v *output) *[]byte { return &v.Value }))
	q := codecTestMust(NewUnpreparedQuery(s, Statement[Unit, output]{Kind: OrdinaryStatement, CQL: "SELECT value FROM ks.tbl", Params: NoParams, Rows: decoder}, NewColumns(), Options{}))
	defer q.Close()
	served := make(chan struct{})
	go func() {
		defer close(served)
		stream := typedTestReadRequest(t, conns[0], peers[0])
		_, err := peers[0].Write(typedTestResponse(t, 4, stream, runtimeRowsBody(nil, "one", "two")))
		if err != nil {
			t.Error(err)
		}
	}()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	rows := codecTestMust(q.Run(ctx, Unit{}))
	defer rows.Close()
	dst := output{Value: make([]byte, 0, 8)}
	backing := &dst.Value[:cap(dst.Value)][0]
	for _, want := range []string{"one", "two"} {
		if !rows.NextInto(&dst) || string(dst.Value) != want || &dst.Value[0] != backing {
			t.Fatal(rows.Err(), dst)
		}
	}
	if rows.NextInto(&dst) || rows.Err() != nil || string(dst.Value) != "two" {
		t.Fatal(rows.Err(), dst)
	}
	if err := q.Wait(ctx); err != nil {
		t.Fatal(err)
	}
	<-served
}

type queryCompletionObserver struct{ notify func() }

func (o queryCompletionObserver) ObserveRequest(RequestEvent) { o.notify() }
func (queryCompletionObserver) ObserveAttempt(AttemptEvent)   {}

func TestStreamQueryRequestObserverCanCloseCursor(t *testing.T) {
	s, conns, peers := runtimeTestSession(t, 1)
	var cursor *RowStream[[]byte]
	observer := queryCompletionObserver{notify: func() {
		if err := cursor.Close(); err != nil {
			t.Error(err)
		}
	}}
	q := codecTestMust(NewUnpreparedStreamQuery(s, Statement[Unit, Bytes]{Kind: OrdinaryStatement, CQL: "SELECT value FROM ks.tbl", Params: NoParams, Rows: OneColumn(BlobCodec)}, NewColumns(), StreamOneColumn(StreamBlob), StreamOptions{Options: Options{Observer: observer}}))
	defer q.Close()
	go func() {
		stream := typedTestReadRequest(t, conns[0], peers[0])
		_, err := peers[0].Write(typedTestResponse(t, 4, stream, runtimeRowsBody(nil, "value")))
		if err != nil {
			t.Error(err)
		}
	}()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	cursor = codecTestMust(q.Run(ctx, Unit{}))
	done := make(chan error, 1)
	go func() {
		dst := make([]byte, 0, 16)
		for cursor.NextInto(&dst) {
		}
		done <- cursor.Err()
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("observer deadlocked closing its cursor")
	}
	if err := q.Wait(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestQueryDecoderCanCloseItsQuery(t *testing.T) {
	s, conns, peers := runtimeTestSession(t, 1)
	type output struct{ Value []byte }
	var q *Query[Unit, output]
	decoder := RowInto(func(*output) { q.Close() }, Column("value", OwnedBlobCodec, func(dst *output) *[]byte { return &dst.Value }))
	q = codecTestMust(NewUnpreparedQuery(s, Statement[Unit, output]{Kind: OrdinaryStatement, CQL: "SELECT value FROM ks.tbl", Params: NoParams, Rows: decoder}, NewColumns(), Options{}))
	defer q.Close()
	go func() {
		stream := typedTestReadRequest(t, conns[0], peers[0])
		_, err := peers[0].Write(typedTestResponse(t, 4, stream, runtimeRowsBody(nil, "value")))
		if err != nil {
			t.Error(err)
		}
	}()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	rows := codecTestMust(q.Run(ctx, Unit{}))
	done := make(chan struct{})
	go func() { var dst output; _ = rows.NextInto(&dst); _ = rows.Close(); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("decoder deadlocked closing its query")
	}
	if err := q.Wait(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestStreamQueryCallerDeadlineDrainsWithoutAdvancing(t *testing.T) {
	s, conns, peers := runtimeTestSession(t, 1)
	q := codecTestMust(NewUnpreparedStreamQuery(s, Statement[Unit, Bytes]{Kind: OrdinaryStatement, CQL: "SELECT value FROM ks.tbl", Params: NoParams, Rows: OneColumn(BlobCodec)}, NewColumns(), StreamOneColumn(StreamBlob), StreamOptions{}))
	defer q.Close()
	go func() {
		stream := typedTestReadRequest(t, conns[0], peers[0])
		_, err := peers[0].Write(typedTestResponse(t, 4, stream, runtimeRowsBody(nil, "value")))
		if err != nil {
			t.Error(err)
		}
	}()
	caller, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	cursor := codecTestMust(q.Run(caller, Unit{}))
	<-caller.Done()
	joined, stop := context.WithTimeout(context.Background(), time.Second)
	defer stop()
	if err := q.Wait(joined); err != nil {
		t.Fatal("idle cursor did not drain on caller deadline", err)
	}
	if !errors.Is(cursor.Err(), context.DeadlineExceeded) {
		t.Fatal("caller deadline cause changed", cursor.Err())
	}
}

func TestStreamQueryExplicitCloseWithoutDeadlineDrains(t *testing.T) {
	s, conns, peers := runtimeTestSession(t, 1)
	q := codecTestMust(NewUnpreparedStreamQuery(s, Statement[Unit, Bytes]{Kind: OrdinaryStatement, CQL: "SELECT value FROM ks.tbl", Params: NoParams, Rows: OneColumn(BlobCodec)}, NewColumns(), StreamOneColumn(StreamBlob), StreamOptions{Options: Options{Timeout: Option[time.Duration]{Set: true, Value: 0}}}))
	go func() {
		stream := typedTestReadRequest(t, conns[0], peers[0])
		_, err := peers[0].Write(typedTestResponse(t, 4, stream, runtimeRowsBody(nil, "value")))
		if err != nil {
			t.Error(err)
		}
	}()
	cursor := codecTestMust(q.Run(context.Background(), Unit{}))
	q.Close()
	joined, stop := context.WithTimeout(context.Background(), time.Second)
	defer stop()
	if err := q.Wait(joined); err != nil {
		t.Fatal("explicit close without callback did not drain", err)
	}
	if !errors.Is(cursor.Err(), ErrClosed) {
		t.Fatal(cursor.Err())
	}
}

func TestQueryCopiedRowsHandleCannotTouchReuse(t *testing.T) {
	s, conns, peers := runtimeTestSession(t, 1)
	q := codecTestMust(NewUnpreparedQuery(s, Statement[Unit, Bytes]{Kind: OrdinaryStatement, CQL: "SELECT value FROM ks.tbl", Params: NoParams, Rows: OneColumn(BlobCodec)}, NewColumns(), Options{}))
	defer q.Close()
	go func() {
		for _, value := range []string{"old", "new"} {
			stream := typedTestReadRequest(t, conns[0], peers[0])
			if _, err := peers[0].Write(typedTestResponse(t, 4, stream, runtimeRowsBody(nil, value))); err != nil {
				t.Error(err)
				return
			}
		}
	}()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	old := codecTestMust(q.Run(ctx, Unit{}))
	copied := *old
	if !old.Next() {
		t.Fatal(old.Err())
	}
	value := old.Row()
	lease := codecTestMust(old.Retain())
	defer lease.Close()
	if old.Next() {
		t.Fatal("unexpected row")
	}
	if err := q.Wait(ctx); err != nil {
		t.Fatal(err)
	}
	current := codecTestMust(q.Run(ctx, Unit{}))
	defer current.Close()
	if copied.Next() || copied.Close() != nil || copied.Err() != nil {
		t.Fatal("copied old rows changed newer execution")
	}
	if _, err := copied.Retain(); !errors.Is(err, ErrNoRows) {
		t.Fatal(err)
	}
	if !current.Next() || string(current.Row().OwnedBytes()) != "new" {
		t.Fatal(current.Err())
	}
	if string(value.OwnedBytes()) != "old" {
		t.Fatal("retained page changed")
	}
	if current.Next() {
		t.Fatal("unexpected row")
	}
}

func TestQueryDeliveredPageSurvivesCloseUntilAdvance(t *testing.T) {
	for _, manual := range []bool{false, true} {
		t.Run(fmt.Sprint(manual), func(t *testing.T) {
			s, conns, peers := runtimeTestSession(t, 1)
			q := codecTestMust(NewUnpreparedQuery(s, Statement[Unit, Bytes]{Kind: OrdinaryStatement, CQL: "SELECT value FROM ks.tbl", Params: NoParams, Rows: OneColumn(BlobCodec)}, NewColumns(), Options{}))
			defer q.Close()
			go func() {
				stream := typedTestReadRequest(t, conns[0], peers[0])
				if _, err := peers[0].Write(typedTestResponse(t, 4, stream, runtimeRowsBody(nil, "delivered"))); err != nil {
					t.Error(err)
				}
			}()
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			var value Bytes
			if manual {
				page := codecTestMust(q.RunPage(ctx, Unit{}, StartPage()))
				defer page.Close()
				if !page.NextInto(&value) {
					t.Fatal(page.Err())
				}
				before := value
				if err := s.Close(); err != nil {
					t.Fatal(err)
				}
				if string(before.OwnedBytes()) != "delivered" {
					t.Fatal("shutdown recycled delivered page")
				}
				if page.NextInto(&value) || value.Len() != 0 || !errors.Is(page.Err(), ErrClosed) {
					t.Fatal(page.Err())
				}
			} else {
				rows := codecTestMust(q.Run(ctx, Unit{}))
				defer rows.Close()
				if !rows.NextInto(&value) {
					t.Fatal(rows.Err())
				}
				before := value
				if err := s.Close(); err != nil {
					t.Fatal(err)
				}
				if string(before.OwnedBytes()) != "delivered" {
					t.Fatal("shutdown recycled delivered row")
				}
				if rows.NextInto(&value) || value.Len() != 0 || !errors.Is(rows.Err(), ErrClosed) {
					t.Fatal(rows.Err())
				}
			}
		})
	}
}

func TestQueryCopiedStreamHandleCannotTouchReuse(t *testing.T) {
	s, conns, peers := runtimeTestSession(t, 1)
	q := codecTestMust(NewUnpreparedStreamQuery(s, Statement[Unit, Bytes]{Kind: OrdinaryStatement, CQL: "SELECT value FROM ks.tbl", Params: NoParams, Rows: OneColumn(BlobCodec)}, NewColumns(), StreamOneColumn(StreamBlob), StreamOptions{}))
	defer q.Close()
	go func() {
		for _, value := range []string{"old", "new"} {
			stream := typedTestReadRequest(t, conns[0], peers[0])
			if _, err := peers[0].Write(typedTestResponse(t, 4, stream, runtimeRowsBody(nil, value))); err != nil {
				t.Error(err)
				return
			}
		}
	}()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	old := codecTestMust(q.Run(ctx, Unit{}))
	copied := *old
	oldValue := make([]byte, 0, 16)
	if !old.NextInto(&oldValue) || string(oldValue) != "old" || old.NextInto(&oldValue) {
		t.Fatal(old.Err())
	}
	if err := q.Wait(ctx); err != nil {
		t.Fatal(err)
	}
	before := copied.Info()
	current := codecTestMust(q.Run(ctx, Unit{}))
	defer current.Close()
	value := make([]byte, 0, 16)
	if copied.NextInto(&value) || copied.Close() != nil || copied.Err() != nil {
		t.Fatal("copied old stream changed newer execution")
	}
	if copied.Info().DeliveredRows != before.DeliveredRows {
		t.Fatal("old terminal result changed")
	}
	if !current.NextInto(&value) || string(value) != "new" || current.NextInto(&value) {
		t.Fatal(current.Err())
	}
	if string(oldValue) != "old" {
		t.Fatal("owned output changed across reuse")
	}
}

func TestZeroCursorHandlesAreExhausted(t *testing.T) {
	var rows Rows[[]byte]
	var page Page[[]byte]
	var stream RowStream[[]byte]
	value := []byte("owned")
	if rows.Next() || rows.NextInto(&value) || rows.Close() != nil || rows.Err() != nil {
		t.Fatal("zero rows")
	}
	if page.Next() || page.NextInto(&value) || page.Close() != nil || page.Err() != nil || page.State().More() {
		t.Fatal("zero page")
	}
	if stream.NextInto(&value) || stream.Close() != nil || stream.Err() != nil {
		t.Fatal("zero stream")
	}
}
