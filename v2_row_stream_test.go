/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package gocql

import (
	"bytes"
	"context"
	"encoding/binary"

	"errors"
	"fmt"

	"net"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	lz4 "github.com/scylladb/gocql/lz4"
	"github.com/scylladb/gocql/v2/internal/segment"
	"github.com/scylladb/gocql/v2/internal/wire"
)

func step07StreamSession(t *testing.T, proto byte, compressor Compressor) (*Session, *Conn, net.Conn) {
	t.Helper()
	s, cs, ps := runtimeTestSession(t, 0)
	c, peer := typedTestConn(t, proto, compressor)
	c.host = &HostInfo{hostId: UUID{15: 1}, state: NodeUp}
	c.session = s
	s.hostSource.hosts[c.host.HostID()] = c.host
	picker := newDefaultConnPicker(1)
	picker.Put(c)
	pool := &hostConnPool{host: c.host, connPicker: picker, repairCtx: c.ctx}
	s.pool.hostConnPools[c.host.hostUUID()] = pool
	_ = cs
	_ = ps
	reader := c.r.(*connReader)
	reader.conn = &step07ReadTargets{Conn: reader.conn}
	go c.serve(c.ctx)
	return s, c, peer
}
func step07Response(t *testing.T, c *Conn, stream int, body []byte) []byte {
	t.Helper()
	data := typedTestResponse(t, c.version, stream, body)
	if c.version != 5 {
		return data
	}
	var out bytes.Buffer
	for off := 0; off < len(data); {
		end := min(off+segment.MaxPayloadSize, len(data))
		chain, err := wire.NewChain(wire.Borrow(data[off:end]))
		if err != nil {
			t.Fatal(err)
		}
		encoded, err := segment.EncodeSpans(chain, len(data) <= segment.MaxPayloadSize, c.segCompressor != nil)
		if err != nil {
			t.Fatal(err)
		}
		_ = encoded.Each(func(span wire.Span) error { out.Write(span.Bytes()); return nil })
		off = end
	}
	return out.Bytes()
}

func TestStep07StreamPagingModes(t *testing.T) {
	for _, mode := range []struct {
		name        string
		proto       byte
		compression Compressor
	}{{"v3", 3, nil}, {"v4", 4, nil}, {"v5raw", 5, nil}, {"v5lz4", 5, lz4.LZ4Compressor{}}} {
		t.Run(mode.name, func(t *testing.T) {
			s, c, peer := step07StreamSession(t, mode.proto, mode.compression)
			payload := bytes.Repeat([]byte("caller-owned"), 40000)
			done := make(chan struct{})
			go func() {
				defer close(done)
				for _, body := range [][]byte{runtimeRowsBody([]byte{}), runtimeRowsBody([]byte{1}, string(payload)), runtimeRowsBody(nil, "last")} {
					stream := typedTestReadRequest(t, c, peer)
					if _, err := peer.Write(step07Response(t, c, stream, body)); err != nil {
						t.Error(err)
						return
					}
				}
			}()
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			b := runtimeQuery(t)
			cursor, err := RunStream(ctx, s, b, StreamOneColumn(StreamBlob), StreamOptions{})
			if err != nil {
				t.Fatal(err)
			}
			if info := cursor.Info(); info.Pages != 1 || info.OutputStarted {
				t.Fatal(info)
			}
			b.Close()
			dst := make([]byte, 0, len(payload))
			backing := &dst[:cap(dst)][0]
			if !cursor.NextInto(&dst) || !bytes.Equal(dst, payload) || &dst[0] != backing {
				t.Fatal("first", len(dst), cursor.Err())
			}
			if info := cursor.Info(); info.Pages != 2 || info.DeliveredRows != 1 || !info.OutputStarted {
				t.Fatal(info)
			}
			owned := append([]byte(nil), dst...)
			if !cursor.NextInto(&dst) || string(dst) != "last" {
				t.Fatal(cursor.Err())
			}
			if cursor.NextInto(&dst) || cursor.Err() != nil {
				t.Fatal(cursor.Err())
			}
			if string(dst) != "last" {
				t.Fatal("exhaustion touched output")
			}
			if err := cursor.Close(); err != nil {
				t.Fatal(err)
			}
			if info := cursor.Info(); info.Pages != 3 || info.DeliveredRows != 2 || info.Result.Attempts != 3 {
				t.Fatal(info)
			}
			if err := b.Wait(ctx); err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(owned, payload) || string(dst) != "last" {
				t.Fatal("output lost at close")
			}
			<-done
		})
	}
}
func TestStep07StreamFirstRowBeforePageAndNoReplay(t *testing.T) {
	s, c, peer := step07StreamSession(t, 4, nil)
	first := make(chan struct{})
	later := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		stream := typedTestReadRequest(t, c, peer)
		body := runtimeRowsBody(nil, "first", "second")
		frame := step07Response(t, c, stream, body)
		split := len(frame) - len("second") - 4
		if _, err := peer.Write(frame[:split]); err != nil {
			t.Error(err)
			return
		}
		close(first)
		<-later
		peer.Close()
	}()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	b := runtimeQuery(t)
	defer b.Close()
	cursor, err := RunStream(ctx, s, b, StreamOneColumn(StreamBlob), StreamOptions{Options: Options{Idempotent: true}})
	if err != nil {
		t.Fatal(err)
	}
	dst := make([]byte, 0, 20)
	if !cursor.NextInto(&dst) || string(dst) != "first" {
		t.Fatal(cursor.Err())
	}
	<-first
	close(later)
	if cursor.NextInto(&dst) {
		t.Fatal("truncated row succeeded")
	}
	if cursor.Err() == nil || cursor.Info().Result.Attempts != 1 || cursor.Info().DeliveredRows != 1 {
		t.Fatal(cursor.Err(), cursor.Info())
	}
	firstErr := cursor.Err()
	cursor.NextInto(nil)
	cursor.Close()
	if cursor.Err() != firstErr {
		t.Fatal("first error replaced")
	}
	<-done
}
func TestStep07StreamCapacityAndNilNoResetReplay(t *testing.T) {
	for _, nilDst := range []bool{false, true} {
		t.Run(map[bool]string{false: "capacity", true: "nil"}[nilDst], func(t *testing.T) {
			s, c, peer := step07StreamSession(t, 4, nil)
			done := make(chan struct{})
			go func() {
				defer close(done)
				stream := typedTestReadRequest(t, c, peer)
				_, _ = peer.Write(step07Response(t, c, stream, runtimeRowsBody(nil, "payload")))
			}()
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			b := runtimeQuery(t)
			cursor, err := RunStream(ctx, s, b, StreamOneColumn(StreamBlob), StreamOptions{})
			if err != nil {
				t.Fatal(err)
			}
			dst := make([]byte, 0, 2)
			var ok bool
			if nilDst {
				ok = cursor.NextInto(nil)
			} else {
				ok = cursor.NextInto(&dst)
			}
			if ok || cursor.Err() == nil {
				t.Fatal("invalid destination succeeded")
			}
			if !nilDst {
				var capacity *StreamCapacityError
				if !errors.As(cursor.Err(), &capacity) || capacity.Required != 7 || capacity.Available != 2 {
					t.Fatal(cursor.Err())
				}
			}
			if cursor.Info().OutputStarted == nilDst {
				t.Fatal(cursor.Info())
			}
			b.Close()
			if err := b.Wait(ctx); err != nil {
				t.Fatal(err)
			}
			cursor.Close()
			<-done
		})
	}
}

// The custom plan blocks after Reset. Close/cancel must join that invocation;
// concurrent NextInto must never enter Reset or access its destination.
type step07GatedDecoder struct {
	entered chan struct{}
	leave   chan struct{}
	resets  atomic.Int32
}

func (d *step07GatedDecoder) Compile(Columns) (StreamRowPlan[[]byte], error) { return d, nil }
func (d *step07GatedDecoder) Reset(dst *[]byte) {
	d.resets.Add(1)
	close(d.entered)
	<-d.leave
	*dst = (*dst)[:0]
}
func (d *step07GatedDecoder) DecodeInto(r *StreamRowReader, dst *[]byte) error {
	plan, err := StreamBlob.Compile(codecTestMust(ScalarType(TypeBlob)))
	if err != nil {
		return err
	}
	return ReadStreamColumn(r, plan, dst)
}
func TestStep07StreamCloseJoinsDestinationAndConcurrentRejection(t *testing.T) {
	for _, action := range []string{"close", "cancel", "session", "concurrent"} {
		t.Run(action, func(t *testing.T) {
			s, c, peer := step07StreamSession(t, 4, nil)
			go func() {
				stream := typedTestReadRequest(t, c, peer)
				_, _ = peer.Write(step07Response(t, c, stream, runtimeRowsBody(nil, "payload")))
			}()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			b := runtimeQuery(t)
			bDone := make(chan struct{})
			decoder := &step07GatedDecoder{entered: make(chan struct{}), leave: make(chan struct{})}
			cursor, err := RunStream(ctx, s, b, decoder, StreamOptions{})
			if err != nil {
				t.Fatal(err)
			}
			dst := make([]byte, 0, 20)
			go func() { cursor.NextInto(&dst); close(bDone) }()
			<-decoder.entered
			closed := make(chan struct{})
			go func() {
				defer close(closed)
				switch action {
				case "close":
					cursor.Close()
				case "cancel":
					cancel()
					cursor.Close()
				case "session":
					s.Close()
				case "concurrent":
					other := []byte("unchanged")
					if cursor.NextInto(&other) || string(other) != "unchanged" {
						t.Error("rejected call touched storage")
					}
					cursor.Close()
				}
			}()
			select {
			case <-closed:
				t.Fatal("close returned during Reset")
			case <-time.After(10 * time.Millisecond):
			}
			close(decoder.leave)
			select {
			case <-closed:
			case <-time.After(time.Second):
				t.Fatal("close failed to join")
			}
			<-bDone
			if decoder.resets.Load() != 1 {
				t.Fatal("unexpected reset", decoder.resets.Load())
			}
			snapshot := append([]byte(nil), dst...)
			time.Sleep(time.Millisecond)
			if !bytes.Equal(snapshot, dst) {
				t.Fatal("late destination write")
			}
			b.Close()
			wait, stop := context.WithTimeout(context.Background(), time.Second)
			defer stop()
			if err := b.Wait(wait); err != nil {
				t.Fatal(err)
			}
		})
	}
}
func TestStep07StreamIdleSessionDrainAndPoolExhaustion(t *testing.T) {
	s, c, peer := step07StreamSession(t, 4, nil)
	go func() {
		stream := typedTestReadRequest(t, c, peer)
		_, _ = peer.Write(step07Response(t, c, stream, runtimeRowsBody(nil, "payload")))
	}()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	b := runtimeQuery(t)
	cursor, err := RunStream(ctx, s, b, StreamOneColumn(StreamBlob), StreamOptions{})
	if err != nil {
		t.Fatal(err)
	}
	short, stop := context.WithTimeout(ctx, 10*time.Millisecond)
	defer stop()
	if second, err := RunStream(short, s, b, StreamOneColumn(StreamBlob), StreamOptions{}); second != nil || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(second, err)
	}
	b.Close()
	s.Close()
	if err := b.Wait(ctx); err != nil {
		t.Fatal(err)
	}
	if cursor.NextInto(nil) || !errors.Is(cursor.Err(), ErrClosed) {
		t.Fatal(cursor.Err())
	}
	if len(s.typedRuntime().executions) != 0 {
		t.Fatal("idle operation leaked")
	}
}
func TestStep07StreamPagingCycleAndLimits(t *testing.T) {
	for _, limit := range []bool{false, true} {
		t.Run(map[bool]string{false: "cycle", true: "limit"}[limit], func(t *testing.T) {
			s, c, peer := step07StreamSession(t, 4, nil)
			config := DefaultConfig("localhost")
			if limit {
				config.Limits.MaxPagingStates = 1
			}
			s.typedRuntime().config = config
			done := make(chan struct{})
			go func() {
				defer close(done)
				tokens := [][]byte{{}, {1}, {}}
				if limit {
					tokens = tokens[:2]
				}
				for _, token := range tokens {
					stream := typedTestReadRequest(t, c, peer)
					if _, err := peer.Write(step07Response(t, c, stream, runtimeRowsBody(token))); err != nil {
						return
					}
				}
			}()
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			b := runtimeQuery(t)
			defer b.Close()
			cursor, err := RunStream(ctx, s, b, StreamOneColumn(StreamBlob), StreamOptions{})
			if err != nil {
				t.Fatal(err)
			}
			dst := []byte("untouched")
			if cursor.NextInto(&dst) {
				t.Fatal("invented row")
			}
			want := ErrPagingNoProgress
			if limit {
				want = ErrPagingStateLimit
			}
			if !errors.Is(cursor.Err(), want) || string(dst) != "untouched" || cursor.Info().OutputStarted {
				t.Fatal(cursor.Err(), cursor.Info())
			}
			<-done
		})
	}
}
func TestStep07StreamCapabilityBeforeQuery(t *testing.T) {
	s, c, _ := step07StreamSession(t, 4, SnappyCompressor{})
	b := runtimeQuery(t)
	defer b.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err := RunStream(ctx, s, b, StreamOneColumn(StreamBlob), StreamOptions{})
	var capability *StreamCapabilityError
	if !errors.As(err, &capability) {
		t.Fatal(err)
	}
	if c.streams.InUse() != 0 {
		t.Fatal("capability leaked stream")
	}
	_, err = RunStream(ctx, s, b, StreamOneColumn(StreamBlob), StreamOptions{Options: Options{Prefetch: Option[int]{Set: true, Value: 1}}})
	var validation *ValidationError
	if !errors.As(err, &validation) {
		t.Fatal(err)
	}
}

// Raw read targets must include the exact final caller body range.
type step07ReadTargets struct {
	net.Conn
	mu      sync.Mutex
	targets [][]byte
}

func (c *step07ReadTargets) Read(p []byte) (int, error) {
	c.mu.Lock()
	c.targets = append(c.targets, p)
	c.mu.Unlock()
	return c.Conn.Read(p)
}
func TestStep07StreamRawReadCallerBacking(t *testing.T) {
	s, c, peer := step07StreamSession(t, 4, nil)
	reader := c.r.(*connReader)
	spy := reader.conn.(*step07ReadTargets)
	payload := bytes.Repeat([]byte{7}, 128<<10)
	go func() {
		stream := typedTestReadRequest(t, c, peer)
		_, _ = peer.Write(step07Response(t, c, stream, runtimeRowsBody(nil, string(payload))))
	}()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	b := runtimeQuery(t)
	defer b.Close()
	cursor, err := RunStream(ctx, s, b, StreamOneColumn(StreamBlob), StreamOptions{})
	if err != nil {
		t.Fatal(err)
	}
	dst := make([]byte, 0, len(payload))
	if !cursor.NextInto(&dst) {
		t.Fatal(cursor.Err())
	}
	spy.mu.Lock()
	found := false
	for _, p := range spy.targets {
		if len(p) == len(dst) && &p[0] == &dst[0] {
			found = true
		}
	}
	spy.mu.Unlock()
	if !found || !bytes.Equal(dst, payload) {
		t.Fatal("body did not read directly into caller backing")
	}
	cursor.NextInto(&dst)
	cursor.Close()
}

type step07CountStreamDecoder struct{ compiles atomic.Int32 }

func (d *step07CountStreamDecoder) Compile(columns Columns) (StreamRowPlan[[]byte], error) {
	d.compiles.Add(1)
	return StreamOneColumn(StreamBlob).Compile(columns)
}
func TestStep07StreamPreparedMetadataFencesAndPlanIsolation(t *testing.T) {
	for _, mismatch := range []bool{false, true} {
		t.Run(map[bool]string{false: "matching", true: "count-mismatch"}[mismatch], func(t *testing.T) {
			s, c, peer := step07StreamSession(t, 4, nil)
			s.cfg.Keyspace = "ks"
			columns := NewColumns(ColumnSpec{Keyspace: "ks", Table: "tbl", Name: "value", Type: codecTestMust(ScalarType(TypeBlob))})
			state, _ := step03PreparedState(t, s, columns)
			bound := &Bound[Bytes]{request: executionRequest{statement: state.statement, prepared: state, values: encodedValues{columns: NewColumns()}, owner: newInputOwner(), routing: RoutingInfo{Kind: OrdinaryStatement}}, rows: OneColumn(BlobCodec)}
			decoder := &step07CountStreamDecoder{}
			done := make(chan struct{})
			go func() {
				defer close(done)
				stream := typedTestReadRequest(t, c, peer)
				body := []byte{0, 0, 0, 2, 0, 0, 0, 4, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 3, 'a', 'b', 'c'}
				if mismatch {
					body[11] = 2
					body = append(body, 255, 255, 255, 255)
				}
				_, _ = peer.Write(step07Response(t, c, stream, body))
			}()
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			cursor, err := RunStream(ctx, s, bound, decoder, StreamOptions{})
			if mismatch {
				var protocol *ProtocolError
				if !errors.As(err, &protocol) || cursor != nil {
					t.Fatal(cursor, err)
				}
			} else {
				if err != nil {
					t.Fatal(err)
				}
				dst := make([]byte, 0, 3)
				if !cursor.NextInto(&dst) || string(dst) != "abc" {
					t.Fatal(cursor.Err())
				}
				cursor.NextInto(&dst)
				cursor.Close()
				if decoder.compiles.Load() != 1 {
					t.Fatal("unchanged metadata recompiled", decoder.compiles.Load())
				}
			}
			bound.Close()
			if err := bound.Wait(ctx); err != nil {
				t.Fatal(err)
			}
			<-done
		})
	}
}
func TestStep07StreamBadV5IntegrityCannotDeliver(t *testing.T) {
	s, c, peer := step07StreamSession(t, 5, nil)
	go func() {
		stream := typedTestReadRequest(t, c, peer)
		frame := step07Response(t, c, stream, runtimeRowsBody(nil, "payload"))
		frame[len(frame)-1] ^= 1
		_, _ = peer.Write(frame)
	}()
	b := runtimeQuery(t)
	defer b.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	cursor, err := RunStream(ctx, s, b, StreamOneColumn(StreamBlob), StreamOptions{})
	if err == nil || cursor != nil {
		t.Fatal("invalid checksum exposed cursor", cursor, err)
	}
}
func TestStep07StreamPageDeadlineEndsAtExactBoundary(t *testing.T) {
	s, c, peer := step07StreamSession(t, 4, nil)
	go func() {
		stream := typedTestReadRequest(t, c, peer)
		_, _ = peer.Write(step07Response(t, c, stream, runtimeRowsBody(nil, "value")))
	}()
	b := runtimeQuery(t)
	defer b.Close()
	cursor, err := RunStream(context.Background(), s, b, StreamOneColumn(StreamBlob), StreamOptions{Options: Options{PageTimeout: Option[time.Duration]{Set: true, Value: 20 * time.Millisecond}}})
	if err != nil {
		t.Fatal(err)
	}
	dst := make([]byte, 0, 10)
	if !cursor.NextInto(&dst) {
		t.Fatal(cursor.Err())
	}
	time.Sleep(30 * time.Millisecond)
	if cursor.NextInto(&dst) || cursor.Close() != nil {
		t.Fatal("completed page acquired idle timeout", cursor.Err())
	}
}

func TestStep07StreamPartialFirstRowNeverReplays(t *testing.T) {
	s, c, peer := step07StreamSession(t, 4, nil)
	done := make(chan struct{})
	go func() {
		defer close(done)
		stream := typedTestReadRequest(t, c, peer)
		frame := step07Response(t, c, stream, runtimeRowsBody(nil, "payload"))
		_, _ = peer.Write(frame[:len(frame)-4])
		peer.Close()
	}()
	b := runtimeQuery(t)
	defer b.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	cursor, err := RunStream(ctx, s, b, StreamOneColumn(StreamBlob), StreamOptions{Options: Options{Idempotent: true}})
	if err != nil {
		t.Fatal(err)
	}
	dst := make([]byte, 0, 20)
	if cursor.NextInto(&dst) || cursor.Err() == nil {
		t.Fatal("partial row succeeded")
	}
	if info := cursor.Info(); !info.OutputStarted || info.DeliveredRows != 0 || info.Result.Attempts != 1 {
		t.Fatal(info)
	}
	if string(dst[:3]) != "pay" {
		t.Fatal("fixture did not write partial caller storage", dst)
	}
	cursor.Close()
	<-done
}

func TestStep07StreamReprepareCanonicalInputAndNoRepairAfterOutput(t *testing.T) {
	s, _, peer := step07StreamSession(t, 4, nil)
	s.cfg.Keyspace = "ks"
	typ := codecTestMust(ScalarType(TypeBlob))
	columns := NewColumns(ColumnSpec{Keyspace: "ks", Table: "tbl", Name: "value", Type: typ})
	state, old := step03PreparedState(t, s, columns)
	state.parameters = columns
	state.info.PartitionKeyIndexes = []int{0}
	count := 0
	params := codecTestMust(OneParam[[]byte](countingBlobEncoder{calls: &count}).Compile(columns))
	prepared := &Prepared[[]byte, Bytes]{state: state, kind: OrdinaryStatement, params: params, rows: OneColumn(BlobCodec)}
	input := []byte("canonical-input")
	bound := codecTestMust(BindBorrowed(prepared, input))
	done := make(chan error, 1)
	go func() {
		for step := 0; step < 4; step++ {
			op, id, body, err := step03ReadRequest(peer)
			if err != nil {
				done <- err
				return
			}
			if step == 1 {
				if op != 9 {
					done <- errors.New("missing reprepare")
					return
				}
				_, err = peer.Write(typedTestResponse(t, 4, id, step03PreparedBody([]byte("new"), columns, columns, 0)))
			} else {
				if op != 10 || !bytes.Contains(body, input) {
					done <- errors.New("canonical input changed")
					return
				}
				switch step {
				case 0:
					_, err = peer.Write(step03ErrorResponse(t, id, ErrCodeUnprepared, old.id))
				case 2:
					_, err = peer.Write(typedTestResponse(t, 4, id, runtimeRowsBody([]byte("next"), "first")))
				case 3:
					_, err = peer.Write(step07ErrorExtensions(step03ErrorResponse(t, id, ErrCodeUnprepared, []byte("new"))))
				}
			}
			if err != nil {
				done <- err
				return
			}
		}
		done <- nil
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	cursor, err := RunStream(ctx, s, bound, StreamOneColumn(StreamBlob), StreamOptions{})
	if err != nil {
		t.Fatal(err)
	}
	dst := make([]byte, 0, 20)
	if !cursor.NextInto(&dst) || string(dst) != "first" {
		t.Fatal(cursor.Err())
	}
	if cursor.NextInto(&dst) {
		t.Fatal("unprepared replay after output")
	}
	var unprepared *RequestErrUnprepared
	if !errors.As(cursor.Err(), &unprepared) {
		t.Fatal(cursor.Err())
	}
	if count != 1 || cursor.Info().DeliveredRows != 1 || cursor.Info().Result.Attempts != 3 {
		t.Fatal(count, cursor.Info())
	}
	info := cursor.Info()
	if len(info.Result.Warnings) != 1 || info.Result.Warnings[0] != "terminal" || len(info.Result.TraceIDs) != 1 || info.Result.TraceIDs[0][15] != 7 || len(info.Result.CustomPayload) != 1 || string(info.Result.CustomPayload[0].Value.Value.OwnedBytes()) != "new" {
		t.Fatal("terminal attempt metadata missing", info)
	}
	bound.Close()
	cursor.Close()
	if err := bound.Wait(ctx); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestStep07V3NegotiatedTabletPayloadEnvelope(t *testing.T) {
	for _, streaming := range []bool{false, true} {
		for _, negotiated := range []bool{false, true} {
			t.Run(fmt.Sprintf("stream=%t/negotiated=%t", streaming, negotiated), func(t *testing.T) {
				s, c, peer := step07StreamSession(t, 3, nil)
				c.framers.defaults.tabletsRoutingV1 = negotiated
				done := make(chan struct{})
				go func() {
					defer close(done)
					id := typedTestReadRequest(t, c, peer)
					body := append([]byte{0, 0}, runtimeRowsBody(nil, "value")...)
					frame := typedTestResponse(t, 3, id, body)
					frame[1] = 4
					_, _ = peer.Write(frame)
				}()
				b := runtimeQuery(t)
				defer b.Close()
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()
				var err error
				if streaming {
					var cursor *RowStream[[]byte]
					cursor, err = RunStream(ctx, s, b, StreamOneColumn(StreamBlob), StreamOptions{})
					if err == nil {
						dst := make([]byte, 0, 10)
						if !cursor.NextInto(&dst) || string(dst) != "value" {
							t.Fatal(cursor.Err())
						}
						cursor.NextInto(&dst)
						cursor.Close()
					}
				}
				if !streaming {
					var page *Page[Bytes]
					page, err = RunPage(ctx, s, b, StartPage(), Options{})
					if err == nil {
						if !page.Next() || string(page.Row().OwnedBytes()) != "value" {
							t.Fatal(page.Err())
						}
						page.Close()
					}
				}
				if (err == nil) != negotiated {
					t.Fatal("negotiated flag fence", err)
				}
				<-done
			})
		}
	}
}

func TestStep07CloseGenerationRejectsLateCancellation(t *testing.T) {
	s, c, peer := step07StreamSession(t, 4, nil)
	go func() {
		id := typedTestReadRequest(t, c, peer)
		_, _ = peer.Write(step07Response(t, c, id, runtimeRowsBody(nil, "value")))
	}()
	decoder := &step07GatedDecoder{entered: make(chan struct{}), leave: make(chan struct{})}
	b := runtimeQuery(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	cursor, err := RunStream(ctx, s, b, decoder, StreamOptions{})
	if err != nil {
		t.Fatal(err)
	}
	dst := make([]byte, 0, 10)
	nextDone := make(chan struct{})
	go func() { cursor.NextInto(&dst); close(nextDone) }()
	<-decoder.entered
	first := make(chan error, 1)
	go func() { first <- cursor.Close() }()
	for !cursor.closing.Load() {
		select {
		case <-ctx.Done():
			t.Fatal("close did not begin")
		default:
			runtime.Gosched()
		}
	}
	late := make(chan error, 1)
	go func() { late <- cursor.closeWithError(context.Canceled) }()
	select {
	case err := <-late:
		t.Fatal("duplicate close did not join", err)
	case <-time.After(10 * time.Millisecond):
	}
	close(decoder.leave)
	if err := <-first; err != nil {
		t.Fatal("explicit close acquired cancellation error", err)
	}
	if err := <-late; err != nil {
		t.Fatal("late cancellation replaced close", err)
	}
	<-nextDone
	b.Close()
	if err := b.Wait(ctx); err != nil {
		t.Fatal(err)
	}
	if cursor.Err() != nil {
		t.Fatal(cursor.Err())
	}
}

func step07ErrorExtensions(frame []byte) []byte {
	extension := make([]byte, 16)
	extension[15] = 7
	extension = binary.BigEndian.AppendUint16(extension, 1)
	extension = binary.BigEndian.AppendUint16(extension, 8)
	extension = append(extension, "terminal"...)
	extension = binary.BigEndian.AppendUint16(extension, 1)
	extension = binary.BigEndian.AppendUint16(extension, 6)
	extension = append(extension, "latest"...)
	extension = binary.BigEndian.AppendUint32(extension, 3)
	extension = append(extension, "new"...)
	result := append([]byte(nil), frame[:9]...)
	result[1] = 2 | 4 | 8
	binary.BigEndian.PutUint32(result[5:], uint32(len(extension)+len(frame)-9))
	result = append(result, extension...)
	result = append(result, frame[9:]...)
	return result
}

func TestStep07LatestAttemptMetadataRemainsBudgetedDuringRetry(t *testing.T) {
	s, c, peer := step07StreamSession(t, 4, nil)
	profile := defaultProfile()
	profile.Retry = step03RetryOnce{}
	handle, _ := NewProfile(profile)
	payload := bytes.Repeat([]byte{'x'}, 12000)
	extension := binary.BigEndian.AppendUint16(nil, 1)
	extension = binary.BigEndian.AppendUint16(extension, 3)
	extension = append(extension, "big"...)
	extension = binary.BigEndian.AppendUint32(extension, uint32(len(payload)))
	extension = append(extension, payload...)
	withPayload := func(frame []byte) []byte {
		h := append([]byte(nil), frame[:9]...)
		h[1] = 4
		binary.BigEndian.PutUint32(h[5:], uint32(len(extension)+len(frame)-9))
		h = append(h, extension...)
		return append(h, frame[9:]...)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		id := typedTestReadRequest(t, c, peer)
		if _, err := peer.Write(typedTestResponse(t, 4, id, runtimeRowsBody([]byte{}))); err != nil {
			return
		}
		id = typedTestReadRequest(t, c, peer)
		if _, err := peer.Write(withPayload(step03ErrorResponse(t, id, ErrCodeServer, nil))); err != nil {
			return
		}
		id = typedTestReadRequest(t, c, peer)
		_, _ = peer.Write(withPayload(typedTestResponse(t, 4, id, runtimeRowsBody(nil, "value"))))
	}()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	b := runtimeQuery(t)
	defer b.Close()
	cursor, err := RunStream(ctx, s, b, StreamOneColumn(StreamBlob), StreamOptions{Options: Options{Profile: handle}, Limits: StreamLimits{MaxMetadataBytes: 20000}})
	if err != nil {
		t.Fatal(err)
	}
	dst := make([]byte, 0, 10)
	if cursor.NextInto(&dst) {
		t.Fatal("metadata budget ignored old latest-attempt storage")
	}
	var protocol *ProtocolError
	if !errors.As(cursor.Err(), &protocol) {
		t.Fatal(cursor.Err())
	}
	cursor.Close()
	if budget := c.streamBudget.bytes(); budget != 0 {
		t.Fatal("metadata owner leaked", budget)
	}
	<-done
}
