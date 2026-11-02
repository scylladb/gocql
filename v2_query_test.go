// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package gocql

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
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
