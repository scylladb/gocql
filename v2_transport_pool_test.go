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

import implEndpoint "github.com/scylladb/gocql/v2/internal/endpoint"

import implCluster "github.com/scylladb/gocql/v2/internal/cluster"

import (
	runtimeerrors "github.com/scylladb/gocql/v2/internal/runtimeerrors"

	"bytes"
	"context"
	"crypto/tls"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"

	frm "github.com/scylladb/gocql/v2/internal/frame"
	"github.com/scylladb/gocql/v2/internal/segment"
	"github.com/scylladb/gocql/v2/internal/streams"
	"github.com/scylladb/gocql/v2/internal/wire"
	lz4 "github.com/scylladb/gocql/v2/internal/wirecompress"
)

func step04Reserve(t *testing.T, c *driverConn) *exclusiveConn {
	t.Helper()
	go c.InternalServe(c.InternalCtx)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	t.Cleanup(cancel)
	r, err := c.InternalReserveExclusive(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { r.InternalClose(false) })
	return r
}
func step04WriteResponse(t *testing.T, c *driverConn, server net.Conn, stream int, body []byte) {
	t.Helper()
	frame := typedTestResponse(t, c.InternalVersion, stream, body)
	if c.InternalVersion < 5 {
		if _, err := server.Write(frame); err != nil {
			t.Error(err)
		}
		return
	}
	for offset := 0; offset < len(frame); {
		end := min(offset+segment.MaxPayloadSize, len(frame))
		part := typedTestChain(t, frame[offset:end])
		packet, err := segment.EncodeSpans(part, len(frame) <= segment.MaxPayloadSize, c.InternalSegCompressor != nil)
		if err != nil {
			t.Error(err)
			return
		}
		if _, err := server.Write(typedTestBytes(t, packet)); err != nil {
			t.Error(err)
			return
		}
		offset = end
	}
}
func TestV2ExclusiveStreamingModesAndSafeReuse(t *testing.T) {
	for _, mode := range []struct {
		version     byte
		compression Compressor
	}{{3, nil}, {4, nil}, {5, nil}, {5, lz4.LZ4Compressor{}}} {
		t.Run(string(rune('0'+mode.version))+func() string {
			if mode.compression != nil {
				return "lz4"
			}
			return "raw"
		}(), func(t *testing.T) {
			c, server := typedTestConn(t, mode.version, mode.compression)
			r := step04Reserve(t, c)
			payload := bytes.Repeat([]byte("segmented-value"), 20000)
			sent := make(chan struct{})
			go func() {
				defer close(sent)
				stream := typedTestReadRequest(t, c, server)
				step04WriteResponse(t, c, server, stream, typedTestRowsBody(payload))
			}()
			response, err := r.InternalExchange(context.Background(), &attemptRequest{Statement: "SELECT value FROM ks.tbl", Consistency: One})
			if err != nil {
				t.Fatal(err)
			}
			if err = response.InternalInstallColumns(response.InternalColumns); err != nil {
				t.Fatal(err)
			}
			plan := codecTestMust(StreamOneColumn(StreamBlob).Compile(response.InternalColumns))
			dst := make([]byte, 0, len(payload))
			backing := &dst[:cap(dst)][0]
			ok, err := decodeStreamRow(response, plan, &dst)
			if err != nil || !ok || !bytes.Equal(dst, payload) || &dst[0] != backing {
				t.Fatal("stream decode", ok, err)
			}
			if ok, err = decodeStreamRow(response, plan, &dst); err != nil || ok {
				t.Fatal("exhaustion", ok, err)
			}
			<-sent
			response.InternalReleaseMetadata()
			if err = r.InternalClose(true); err != nil {
				t.Fatal(err)
			}
			// A delayed old cursor close cannot affect a reused shared connection.
			r.InternalClose(false)
			if c.Closed() {
				t.Fatal("released cursor closed reused connection")
			}
			go func() {
				stream := typedTestReadRequest(t, c, server)
				step04WriteResponse(t, c, server, stream, typedTestRowsBody([]byte("shared")))
			}()
			page, err := c.InternalExecuteTyped(context.Background(), &attemptRequest{Statement: "SELECT value", Consistency: One})
			if err != nil {
				t.Fatal(err)
			}
			page.Owner.Release()
		})
	}
}
func TestV2ExclusiveRawFirstRowBeforeRemainingResponse(t *testing.T) {
	c, server := typedTestConn(t, 4, nil)
	r := step04Reserve(t, c)
	first, second := []byte("first"), []byte("second")
	body := typedTestRowsBody(first)
	binary.BigEndian.PutUint32(body[len(body)-len(first)-8:], 2)
	firstEnd := len(body)
	body = binary.BigEndian.AppendUint32(body, uint32(len(second)))
	body = append(body, second...)
	delivered := make(chan struct{})
	serverDone := make(chan struct{})
	go func() {
		defer close(serverDone)
		stream := typedTestReadRequest(t, c, server)
		frame := typedTestResponse(t, 4, stream, body)
		if _, err := server.Write(frame[:9+firstEnd]); err != nil {
			t.Error(err)
			return
		}
		<-delivered
		if _, err := server.Write(frame[9+firstEnd:]); err != nil {
			t.Error(err)
		}
	}()
	response, err := r.InternalExchange(context.Background(), &attemptRequest{Statement: "SELECT value", Consistency: One})
	if err != nil {
		t.Fatal(err)
	}
	if err = response.InternalInstallColumns(response.InternalColumns); err != nil {
		t.Fatal(err)
	}
	plan := codecTestMust(StreamOneColumn(StreamBlob).Compile(response.InternalColumns))
	dst := make([]byte, 0, 32)
	if ok, err := decodeStreamRow(response, plan, &dst); !ok || err != nil || string(dst) != "first" {
		t.Fatal(ok, err, string(dst))
	}
	close(delivered)
	if ok, err := decodeStreamRow(response, plan, &dst); !ok || err != nil || string(dst) != "second" {
		t.Fatal(ok, err, string(dst))
	}
	<-serverDone
	r.InternalClose(true)
}
func TestV2ExclusiveWaitsForOrphanAndAcquisitionCancellation(t *testing.T) {
	c, server := typedTestConn(t, 4, nil)
	go c.InternalServe(c.InternalCtx)
	requestCtx, cancel := context.WithCancel(context.Background())
	requested := make(chan struct{})
	allowLate := make(chan struct{})
	go func() {
		stream := typedTestReadRequest(t, c, server)
		close(requested)
		<-allowLate
		step04WriteResponse(t, c, server, stream, typedTestRowsBody(nil))
	}()
	done := make(chan error)
	go func() {
		_, err := c.InternalExecuteTyped(requestCtx, &attemptRequest{Statement: "SELECT value", Consistency: One})
		done <- err
	}()
	<-requested
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	ctx, stop := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer stop()
	if _, err := c.InternalReserveExclusive(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("orphan acquisition", err)
	}
	if c.InternalExclusiveFlag.Load() {
		t.Fatal("canceled acquisition retained admission gate")
	}
	close(allowLate)
	deadline := time.After(time.Second)
	for c.InternalStreams.InUse() != 0 {
		select {
		case <-deadline:
			t.Fatal("late orphan not released")
		default:
			time.Sleep(time.Millisecond)
		}
	}
	ctx2, cancel2 := context.WithTimeout(context.Background(), time.Second)
	defer cancel2()
	r, err := c.InternalReserveExclusive(ctx2)
	if err != nil {
		t.Fatal(err)
	}
	r.InternalClose(true)
}
func TestV2ExclusiveCloseStopsBlockedCallerStorage(t *testing.T) {
	c, server := typedTestConn(t, 4, nil)
	r := step04Reserve(t, c)
	entered := make(chan struct{})
	go func() {
		stream := typedTestReadRequest(t, c, server)
		body := typedTestRowsBody([]byte("payload"))
		frame := typedTestResponse(t, 4, stream, body)
		server.Write(frame[:len(frame)-7])
		close(entered)
	}()
	response, err := r.InternalExchange(context.Background(), &attemptRequest{Statement: "SELECT value", Consistency: One})
	if err != nil {
		t.Fatal(err)
	}
	if err = response.InternalInstallColumns(response.InternalColumns); err != nil {
		t.Fatal(err)
	}
	plan := codecTestMust(StreamOneColumn(StreamBlob).Compile(response.InternalColumns))
	dst := make([]byte, 0, 16)
	done := make(chan error)
	go func() { _, err := decodeStreamRow(response, plan, &dst); done <- err }()
	<-entered
	if err = r.InternalClose(false); err != nil {
		t.Fatal(err)
	}
	if err = <-done; err == nil {
		t.Fatal("blocked decode succeeded")
	}
	snapshot := append([]byte(nil), dst[:cap(dst)]...)
	time.Sleep(time.Millisecond)
	if !bytes.Equal(snapshot, dst[:cap(dst)]) {
		t.Fatal("caller storage modified after close")
	}
}
func TestV2OrphanBudgetUsesExactCapacity(t *testing.T) {
	for _, capacity := range []int{1, 2, 65} {
		c, server := typedTestConn(t, 4, nil)
		cfg := DefaultConfig("localhost")
		cfg.Pool.MaxOrphanedStreams = 1024
		c.InternalCfg = &driverConnConfig{TypedConfig: connectivityConfigPointer(cfg)}
		c.InternalStreams = streams.NewLimited(capacity)
		go c.InternalServe(c.InternalCtx)
		received := make(chan struct{})
		go func() {
			for i := 0; i < capacity; i++ {
				typedTestReadRequest(t, c, server)
				received <- struct{}{}
			}
		}()
		for i := 0; i < capacity; i++ {
			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan error)
			go func() {
				_, err := c.InternalExecuteTyped(ctx, &attemptRequest{Statement: "SELECT", Consistency: One})
				done <- err
			}()
			select {
			case <-received:
			case err := <-done:
				t.Fatalf("capacity %d request %d ended before write: %v", capacity, i, err)
			case <-time.After(time.Second):
				t.Fatal("request not written")
			}
			cancel()
			<-done
		}

		if !c.Closed() {
			t.Fatal("effective orphan budget failed to poison", capacity)
		}
	}
}
func TestV2PoolSizingModesAndPendingShards(t *testing.T) {
	makeConn := func(shard int) *driverConn {
		return &driverConn{InternalScyllaSupported: *func() *ScyllaConnectionFeatures {
			opaqueStorageValuex := new(ScyllaConnectionFeatures)
			opaqueStorageValuex.ScyllaHostFeatures = *func() *ScyllaHostFeatures {
				opaqueStorageValue := new(ScyllaHostFeatures)
				implEndpoint.ScyllaHostFeaturesData(opaqueStorageValue).InternalNrShards = 3
				return opaqueStorageValue
			}()
			implEndpoint.ScyllaConnectionFeaturesData(opaqueStorageValuex).InternalShard = shard
			return opaqueStorageValuex
		}(), InternalStreams: streams.New(), InternalCtx: context.Background()}
	}
	c := makeConn(0)
	opts := DefaultConfig("localhost").Pool
	opts.ConnectionsPerHost = 2
	p := codecTestMust(newTypedConnPicker(c, opts))
	if err := p.Put(c); err != nil {
		t.Fatal(err)
	}
	if a, b := p.NextShard(); a != 0 || b != 0 {
		t.Fatal("host override targeted shard")
	}
	if err := p.Put(makeConn(0)); err != nil {
		t.Fatal(err)
	}
	if err := p.Put(makeConn(2)); err == nil {
		t.Fatal("host budget exceeded")
	}
	if n, m := p.Size(); n != 2 || m != 0 {
		t.Fatal(n, m)
	}
	opts.ConnectionsPerHost = 0
	opts.ConnectionsPerShard = 2
	p = codecTestMust(newTypedConnPicker(c, opts))
	for i := 0; i < 6; i++ {
		shard, shards := p.NextShard()
		if shards != 3 {
			t.Fatal("no shard target", i)
		}
		if err := p.Put(makeConn(shard)); err != nil {
			t.Fatal(err)
		}
		p.InternalDialFinished(shard, shards)
	}
	if n, m := p.Size(); n != 6 || m != 0 {
		t.Fatal(n, m)
	}
}

var _ io.Reader = (*verifiedStreamUnits)(nil)

func step04EventPacket(t *testing.T, proto byte, kind string) []byte {
	t.Helper()
	e := newTypedEnvelope()
	e.Str(kind, false)
	if kind == "CLIENT_ROUTES_CHANGE" {
		e.Str("UPDATED", false)
		e.U16(0)
		e.U16(0)
	} else {
		e.Str("DOWN", false)
		e.U8(4)
		e.Chain(typedTestChain(t, []byte{127, 0, 0, 2}))
		e.U32(9042)
	}
	body := typedTestBytes(t, codecTestMust(e.Freeze()))
	out := []byte{proto | 0x80, 0, 0xff, 0xff, byte(frm.OpEvent)}
	out = binary.BigEndian.AppendUint32(out, uint32(len(body)))
	return append(out, body...)
}
func TestV2ExclusiveTLSStreaming(t *testing.T) {
	for _, mode := range []struct {
		version    byte
		compressor Compressor
	}{{3, nil}, {4, nil}, {5, nil}, {5, lz4.LZ4Compressor{}}} {
		version := mode.version
		t.Run(fmt.Sprintf("%d-%v", version, mode.compressor), func(t *testing.T) {
			c, rawServer := typedTestConn(t, version, mode.compressor)
			defer rawServer.Close()
			rawClient := c.InternalR.(*connReader).InternalConn
			cert, clientConfig := step04TLS(t)
			client := tls.Client(rawClient, clientConfig)
			server := tls.Server(rawServer, &tls.Config{Certificates: []tls.Certificate{cert}})
			done := make(chan error, 1)
			go func() { done <- server.Handshake() }()
			if err := client.Handshake(); err != nil {
				t.Fatal(err)
			}
			if err := <-done; err != nil {
				t.Fatal(err)
			}
			c.InternalR.(*connReader).InternalConn = client
			c.InternalW = &deadlineContextWriter{InternalW: client, InternalSemaphore: make(chan struct{}, 1), InternalQuit: make(chan struct{})}
			r := step04Reserve(t, c)
			payload := bytes.Repeat([]byte("tls-final-storage"), 30000)
			go func() {
				stream := typedTestReadRequest(t, c, server)
				step04WriteResponse(t, c, server, stream, typedTestRowsBody(payload))
			}()
			response, err := r.InternalExchange(context.Background(), &attemptRequest{Statement: "SELECT value", Consistency: One})
			if err != nil {
				t.Fatal(err)
			}
			if err = response.InternalInstallColumns(response.InternalColumns); err != nil {
				t.Fatal(err)
			}
			plan := codecTestMust(StreamOneColumn(StreamBlob).Compile(response.InternalColumns))
			dst := make([]byte, 0, len(payload))
			if ok, err := decodeStreamRow(response, plan, &dst); !ok || err != nil || !bytes.Equal(dst, payload) {
				t.Fatal("TLS stream", ok, err)
			}
			if err = r.InternalClose(true); err != nil {
				t.Fatal(err)
			}
		})
	}
}
func TestV2ExclusiveV5TrailingControlReuse(t *testing.T) {
	for _, bad := range []bool{false, true} {
		t.Run(fmt.Sprint(bad), func(t *testing.T) {
			c, server := typedTestConn(t, 5, nil)
			c.InternalHooks = newConnectionHooks(func() *Session {
				opaqueStorageValuexx := new(Session)
				implCluster.SessionData(opaqueStorageValuexx).InternalCtx = c.InternalCtx
				implCluster.SessionData(opaqueStorageValuexx).InternalLogger = nopLogger{}
				return opaqueStorageValuexx
			}())
			r := step04Reserve(t, c)
			go func() {
				stream := typedTestReadRequest(t, c, server)
				result := typedTestResponse(t, 5, stream, typedTestRowsBody([]byte("one")))
				control := step04EventPacket(t, 5, "CLIENT_ROUTES_CHANGE")
				if bad {
					control = control[:len(control)-1]
				}
				packet := codecTestMust(segment.EncodeSpans(typedTestChain(t, result, control), true, false))
				server.Write(typedTestBytes(t, packet))
			}()
			response, err := r.InternalExchange(context.Background(), &attemptRequest{Statement: "SELECT value", Consistency: One})
			if err != nil {
				t.Fatal(err)
			}
			if err = response.InternalInstallColumns(response.InternalColumns); err != nil {
				t.Fatal(err)
			}
			dst := make([]byte, 0, 8)
			plan := codecTestMust(StreamOneColumn(StreamBlob).Compile(response.InternalColumns))
			if ok, err := decodeStreamRow(response, plan, &dst); !ok || err != nil {
				t.Fatal(ok, err)
			}
			err = r.InternalClose(true)
			if bad {
				if err == nil || !c.Closed() {
					t.Fatal("bad trailing control reused connection", err)
				}
			} else {
				if err != nil || c.Closed() {
					t.Fatal("valid trailing control prevented reuse", err)
				}
			}
		})
	}
}
func TestV2ExclusiveControlOverflowPoisonsAndReleases(t *testing.T) {
	for _, limit := range []int64{512, 1 << 20} {
		t.Run(fmt.Sprint(limit), func(t *testing.T) {
			c, server := typedTestConn(t, 4, nil)
			consumer := &eventDebouncer{InternalTimer: time.NewTimer(time.Hour), InternalLogger: nopLogger{}}
			defer consumer.InternalTimer.Stop()
			consumer.InternalMu.Lock()
			c.InternalHooks = newConnectionHooks(func() *Session {
				opaqueStorageValuexxx := new(Session)
				implCluster.SessionData(opaqueStorageValuexxx).InternalCtx = c.InternalCtx
				implCluster.SessionData(opaqueStorageValuexxx).InternalLogger = nopLogger{}
				implCluster.SessionData(opaqueStorageValuexxx).InternalNodeEvents = consumer
				return opaqueStorageValuexxx
			}())
			go c.InternalServe(c.InternalCtx)
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			r, err := c.InternalReserveExclusiveWithLimits(ctx, StreamLimits{MaxMetadataBytes: limit})
			if err != nil {
				consumer.InternalMu.Unlock()
				t.Fatal(err)
			}
			serverDone := make(chan struct{})
			go func() {
				defer close(serverDone)
				typedTestReadRequest(t, c, server)
				for i := 0; i < 50; i++ {
					if _, err := server.Write(step04EventPacket(t, 4, "STATUS_CHANGE")); err != nil {
						return
					}
				}
			}()
			_, err = r.InternalExchange(context.Background(), &attemptRequest{Statement: "SELECT value", Consistency: One})
			if err == nil || !c.Closed() {
				consumer.InternalMu.Unlock()
				t.Fatal("control overflow remained reusable", err)
			}
			consumer.InternalMu.Unlock()
			c.InternalWaitEventDispatch()
			<-serverDone
			if r.InternalMetadataBudget.Bytes() != 0 {
				t.Fatal("overflow leaked retained control charges", r.InternalMetadataBudget.Bytes())
			}
		})
	}
}
func TestV2EventEnqueueCloseRaceReleasesEveryCharge(t *testing.T) {
	for iteration := 0; iteration < 20; iteration++ {
		c, _ := typedTestConn(t, 4, nil)
		c.InternalHooks = newConnectionHooks(func() *Session {
			opaqueStorageValuexxxx := new(Session)
			implCluster.SessionData(opaqueStorageValuexxxx).InternalCtx = c.InternalCtx
			implCluster.SessionData(opaqueStorageValuexxxx).InternalLogger = nopLogger{}
			return opaqueStorageValuexxxx
		}())
		budget := &streamMetadataBudget{Limit: 1 << 20}
		var wg sync.WaitGroup
		wg.Add(4)
		for worker := 0; worker < 4; worker++ {
			go func() {
				defer wg.Done()
				for i := 0; i < 50; i++ {
					if err := budget.Reserve(16); err != nil {
						t.Error(err)
						return
					}
					c.InternalDispatchEventOwned(&frm.ClientRoutesChanged{}, func() { budget.Release(16) })
				}
			}()
		}
		c.InternalCancel()
		wg.Wait()
		c.InternalWaitEventDispatch()
		if n := budget.Bytes(); n != 0 {
			t.Fatal("terminal queue retained charges", iteration, n)
		}
	}
}

type step04HeaderPause struct {
	entered, release chan struct{}
	once             sync.Once
}

func (o *step04HeaderPause) ObserveFrameHeader(context.Context, ObservedFrameHeader) {
	o.once.Do(func() { close(o.entered); <-o.release })
}
func TestV2ExclusiveHandoffWaitsForWholeV5Segment(t *testing.T) {
	c, server := typedTestConn(t, 5, nil)
	gate := &step04HeaderPause{entered: make(chan struct{}), release: make(chan struct{})}
	c.InternalFrameObserver = gate
	go c.InternalServe(c.InternalCtx)
	event := step04EventPacket(t, 5, "CLIENT_ROUTES_CHANGE")
	packet := codecTestMust(segment.EncodeSpans(typedTestChain(t, event, event), true, false))
	sent := make(chan struct{})
	go func() { server.Write(typedTestBytes(t, packet)); close(sent) }()
	<-gate.entered
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	done := make(chan *exclusiveConn)
	failed := make(chan error, 1)
	go func() {
		r, err := c.InternalReserveExclusive(ctx)
		if err != nil {
			failed <- err
			return
		}
		done <- r
	}()
	select {
	case <-done:
		t.Fatal("handoff occurred with unread validated segment frames")
	case err := <-failed:
		t.Fatal(err)
	case <-time.After(10 * time.Millisecond):
	}
	close(gate.release)
	var r *exclusiveConn
	select {
	case r = <-done:
	case err := <-failed:
		t.Fatal(err)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	<-sent
	r.InternalClose(true)
}
func TestV2ExclusiveHandoffFinishesPartialHeader(t *testing.T) {
	c, server := typedTestConn(t, 4, nil)
	go c.InternalServe(c.InternalCtx)
	packet := step04EventPacket(t, 4, "CLIENT_ROUTES_CHANGE")
	first := make(chan struct{})
	go func() { server.Write(packet[:1]); close(first) }()
	<-first
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	done := make(chan *exclusiveConn)
	failed := make(chan error, 1)
	go func() {
		r, err := c.InternalReserveExclusive(ctx)
		if err != nil {
			failed <- err
			return
		}
		done <- r
	}()
	select {
	case <-done:
		t.Fatal("handoff occurred mid-header")
	case err := <-failed:
		t.Fatal(err)
	case <-time.After(10 * time.Millisecond):
	}
	sent := make(chan struct{})
	go func() { server.Write(packet[1:]); close(sent) }()
	var r *exclusiveConn
	select {
	case r = <-done:
	case err := <-failed:
		t.Fatal(err)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	<-sent
	r.InternalClose(true)
}

type step04AcceptedWriter struct {
	net.Conn
	entered chan struct{}
	once    sync.Once
}

func (w *step04AcceptedWriter) Write(b []byte) (int, error) {
	w.once.Do(func() { close(w.entered) })
	return w.Conn.Write(b)
}
func TestV2ExclusiveAcquisitionDrainsAcceptedWriterOwnership(t *testing.T) {
	c, server := typedTestConn(t, 4, nil)
	go c.InternalServe(c.InternalCtx)
	entered := make(chan struct{})
	c.InternalW = &deadlineContextWriter{InternalW: &step04AcceptedWriter{Conn: c.InternalR.(*connReader).InternalConn, entered: entered}, InternalSemaphore: make(chan struct{}, 1), InternalQuit: make(chan struct{})}
	payload := bytes.Repeat([]byte("borrowed"), 20000)
	prefix := binary.BigEndian.AppendUint32(nil, uint32(len(payload)))
	values := typedTestChain(t, prefix, payload)
	var released atomic.Bool
	inputDrained := make(chan struct{})
	owner := wire.NewLease(values, func() { released.Store(true); close(inputDrained) })
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := c.InternalExecuteTyped(ctx, &attemptRequest{Statement: "INSERT INTO ks.tbl(value) VALUES(?)", Values: values, ValueCount: 1, Consistency: One, Lease: owner})
		done <- err
	}()
	<-entered
	owner.Release()
	cancel()
	wait, stop := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer stop()
	if _, err := c.InternalReserveExclusive(wait); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("reservation bypassed accepted writer", err)
	}
	if released.Load() {
		t.Fatal("input released while accepted writer still reads it")
	}
	serverDone := make(chan struct{})
	go func() {
		defer close(serverDone)
		stream := typedTestReadRequest(t, c, server)
		step04WriteResponse(t, c, server, stream, typedTestRowsBody(nil))
	}()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	<-serverDone
	select {
	case <-inputDrained:
	case <-time.After(time.Second):
		t.Fatal("accepted writer did not drain input after transmission")
	}
}

func TestV2ExclusiveClosedResponseDropsValidatedArena(t *testing.T) {
	c, server := typedTestConn(t, 5, nil)
	r := step04Reserve(t, c)
	sent := make(chan struct{})
	go func() {
		defer close(sent)
		stream := typedTestReadRequest(t, c, server)
		step04WriteResponse(t, c, server, stream, typedTestRowsBody(bytes.Repeat([]byte("value"), 30000)))
	}()
	response, err := r.InternalExchange(context.Background(), &attemptRequest{Statement: "SELECT value", Consistency: One})
	if err != nil {
		t.Fatal(err)
	}
	if err = response.InternalInstallColumns(response.InternalColumns); err != nil {
		t.Fatal(err)
	}
	dst := make([]byte, 0, 150000)
	plan := codecTestMust(StreamOneColumn(StreamBlob).Compile(response.InternalColumns))
	if ok, err := decodeStreamRow(response, plan, &dst); !ok || err != nil {
		t.Fatal(ok, err)
	}
	heldUnits := r.InternalUnits
	if heldUnits.InternalUnits.StorageBytes() <= 16 {
		t.Fatal("fixture allocated no transform arena")
	}
	<-sent
	if err = r.InternalClose(true); err != nil {
		t.Fatal(err)
	}
	if heldUnits.InternalData != nil || heldUnits.InternalUnits.StorageBytes() != 16 {
		t.Fatal("closed response retained transform arena")
	}
	response.InternalReleaseMetadata()
	if response.InternalBody != nil || response.InternalReader != nil || response.InternalBudget != nil || response.InternalColumns.Len() != 0 {
		t.Fatal("released response retained control/reader state")
	}
	if len(dst) != 150000 || dst[0] != 'v' {
		t.Fatal("caller-owned output invalidated on close")
	}
}
func TestV2InitialOpeningCannotReplaceInstalledSharder(t *testing.T) {
	makeConn := func(shard, shards int) *driverConn {
		return &driverConn{InternalScyllaSupported: *func() *ScyllaConnectionFeatures {
			opaqueStorageValuexxxxxx := new(ScyllaConnectionFeatures)
			opaqueStorageValuexxxxxx.ScyllaHostFeatures = *func() *ScyllaHostFeatures {
				opaqueStorageValuexxxxx := new(ScyllaHostFeatures)
				implEndpoint.ScyllaHostFeaturesData(opaqueStorageValuexxxxx).InternalNrShards = shards
				return opaqueStorageValuexxxxx
			}()
			implEndpoint.ScyllaConnectionFeaturesData(opaqueStorageValuexxxxxx).InternalShard = shard
			return opaqueStorageValuexxxxxx
		}(), InternalStreams: streams.New()}
	}
	config := DefaultConfig("localhost").Pool
	first := makeConn(0, 3)
	p := codecTestMust(newTypedConnPicker(first, config))
	if err := p.InternalPutInitial(first); err != nil {
		t.Fatal(err)
	}
	if err := p.InternalPutInitial(makeConn(1, 3)); err != nil {
		t.Fatal("compatible initial opening rejected", err)
	}
	if err := p.InternalPutInitial(makeConn(0, 2)); err == nil || p.GetShardCount() != 3 {
		t.Fatal("late initial opening replaced established sharder")
	}
}

func TestV2PageCancellationGenerationDoesNotReachLaterPageOrSharedUse(t *testing.T) {
	c, server := typedTestConn(t, 4, nil)
	r := step04Reserve(t, c)
	serverDone := make(chan struct{})
	go func() {
		defer close(serverDone)
		for i := 0; i < 3; i++ {
			stream := typedTestReadRequest(t, c, server)
			step04WriteResponse(t, c, server, stream, typedTestRowsBody([]byte("value")))
		}
	}()
	pageCtx, cancelPage := context.WithCancel(r.InternalCtx)
	first, err := r.InternalExchange(pageCtx, &attemptRequest{Statement: "SELECT value", Consistency: One})
	if err != nil {
		t.Fatal(err)
	}
	oldGeneration := r.InternalIoGeneration
	if err = first.InternalInstallColumns(first.InternalColumns); err != nil {
		t.Fatal(err)
	}
	plan := codecTestMust(StreamOneColumn(StreamBlob).Compile(first.InternalColumns))
	dst := make([]byte, 0, 16)
	if ok, err := decodeStreamRow(first, plan, &dst); !ok || err != nil {
		t.Fatal(ok, err)
	}
	first.InternalReleaseMetadata()
	cancelPage()
	secondCtx, cancelSecond := context.WithCancel(r.InternalCtx)
	defer cancelSecond()
	second, err := r.InternalExchange(secondCtx, &attemptRequest{Statement: "SELECT value", Consistency: One})
	if err != nil {
		t.Fatal("old page context canceled new page", err)
	}
	r.InternalCancelIOGeneration(oldGeneration, context.Canceled)
	if c.Closed() || r.InternalClosing.Load() {
		t.Fatal("delayed old page cancellation reached new page")
	}
	if err = second.InternalInstallColumns(second.InternalColumns); err != nil {
		t.Fatal(err)
	}
	if ok, err := decodeStreamRow(second, plan, &dst); !ok || err != nil {
		t.Fatal(ok, err)
	}
	second.InternalReleaseMetadata()
	if err = r.InternalClose(true); err != nil {
		t.Fatal(err)
	}
	r.InternalCancelIOGeneration(oldGeneration, context.Canceled)
	page, err := c.InternalExecuteTyped(context.Background(), &attemptRequest{Statement: "SELECT value", Consistency: One})
	if err != nil {
		t.Fatal("delayed callback reached shared reuse", err)
	}
	page.Owner.Release()
	<-serverDone
}
func TestV2PageContextCancellationWinsAndJoinsRead(t *testing.T) {
	c, server := typedTestConn(t, 4, nil)
	r := step04Reserve(t, c)
	prefixWritten := make(chan struct{})
	serverDone := make(chan struct{})
	go func() {
		defer close(serverDone)
		stream := typedTestReadRequest(t, c, server)
		body := typedTestRowsBody([]byte("blocked"))
		frame := typedTestResponse(t, 4, stream, body)
		server.Write(frame[:len(frame)-7])
		close(prefixWritten)
		var b [1]byte
		server.Read(b[:])
	}()
	pageCtx, cancelPage := context.WithCancel(r.InternalCtx)
	response, err := r.InternalExchange(pageCtx, &attemptRequest{Statement: "SELECT value", Consistency: One})
	if err != nil {
		t.Fatal(err)
	}
	if err = response.InternalInstallColumns(response.InternalColumns); err != nil {
		t.Fatal(err)
	}
	plan := codecTestMust(StreamOneColumn(StreamBlob).Compile(response.InternalColumns))
	dst := make([]byte, 0, 16)
	done := make(chan error, 1)
	go func() { _, err := decodeStreamRow(response, plan, &dst); done <- err }()
	<-prefixWritten
	cancelPage()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatal("winning cancellation erased", err)
		}
	case <-time.After(time.Second):
		t.Fatal("page cancellation failed to interrupt read")
	}
	if err = r.InternalClose(false); err != nil {
		t.Fatal(err)
	}
	<-serverDone
	if !c.Closed() {
		t.Fatal("canceled page returned to shared pool")
	}
}

type step04TLSCoalescedRecorder struct {
	conn      *tls.Conn
	payload   []byte
	remaining int
	partial   bool
	borrowed  int
	entered   chan struct{}
	once      sync.Once
}

func (w *step04TLSCoalescedRecorder) SetWriteDeadline(deadline time.Time) error {
	return w.conn.SetWriteDeadline(deadline)
}
func (w *step04TLSCoalescedRecorder) Write(p []byte) (int, error) {
	w.once.Do(func() { close(w.entered) })
	if len(p) > 1024 {
		at := uintptr(unsafe.Pointer(&p[0]))
		start := uintptr(unsafe.Pointer(&w.payload[0]))
		if at < start || at+uintptr(len(p)) > start+uintptr(len(w.payload)) {
			return 0, fmt.Errorf("coalescer gathered borrowed TLS plaintext")
		}
		w.borrowed += len(p)
	}
	if w.partial && len(p) > w.remaining {
		n, err := w.conn.Write(p[:w.remaining])
		w.remaining -= n
		if err != nil {
			return n, err
		}
		return n, io.ErrShortWrite
	}
	n, err := w.conn.Write(p)
	if w.partial {
		w.remaining -= n
	}
	return n, err
}
func TestV2TypedTLSCoalescingBorrowedOrderPartialWriteAndDrain(t *testing.T) {
	for _, partial := range []bool{false, true} {
		t.Run(fmt.Sprint(partial), func(t *testing.T) {
			c, unused := typedTestConn(t, 4, nil)
			unused.Close()
			c.InternalCancel()
			client, server, _, cleanup := step04BenchTLS(t)
			defer cleanup()
			ctx, cancel := context.WithCancel(context.Background())
			c.InternalCtx, c.InternalCancel = ctx, cancel
			c.InternalClosed = false
			c.InternalR = &connReader{InternalConn: client}
			payload := bytes.Repeat([]byte("original-borrowed"), 10000)
			values := typedTestChain(t, binary.BigEndian.AppendUint32(nil, uint32(len(payload))), payload)
			recorder := &step04TLSCoalescedRecorder{conn: client, payload: payload, remaining: len(payload) / 2, partial: partial, entered: make(chan struct{})}
			c.InternalW = newWriteCoalescer(recorder, 0, 100*time.Microsecond, c.InternalCtx.Done())
			go c.InternalServe(c.InternalCtx)
			serverDone := make(chan error, 1)
			go func() {
				if partial {
					_, err := io.Copy(io.Discard, server)
					serverDone <- err
					return
				}
				stream := typedTestReadRequest(t, c, server)
				response := typedTestResponse(t, 4, stream, binary.BigEndian.AppendUint32(nil, 1))
				_, err := server.Write(response)
				serverDone <- err
			}()
			var released atomic.Bool
			owner := wire.NewLease(values, func() { released.Store(true) })
			result := make(chan error, 1)
			go func() {
				page, err := c.InternalExecuteTyped(context.Background(), &attemptRequest{Statement: "INSERT INTO ks.tbl(value) VALUES(?)", Values: values, ValueCount: 1, Consistency: One, Lease: owner})
				if page != nil {
					page.Owner.Release()
				}
				result <- err
			}()
			<-recorder.entered
			owner.Release()
			err := <-result
			if partial {
				var query *QueryError
				if err == nil || !errors.As(err, &query) || !runtimeerrors.QueryErrorData(query).InternalPotentiallyExecuted || !c.Closed() {
					t.Fatal("partial TLS write not poisoned", err)
				}
			} else if err != nil || recorder.borrowed != len(payload) {
				t.Fatal("ordered TLS spans", err, recorder.borrowed)
			}
			if !released.Load() {
				t.Fatal("accepted TLS writer input did not drain")
			}
			if err := <-serverDone; err != nil {
				t.Fatal("TLS peer", err)
			}
		})
	}
}

type step04FixedHostDialer struct{ conn net.Conn }

func (d step04FixedHostDialer) DialHost(context.Context, *HostInfo) (*DialedHost, error) {
	return &DialedHost{Conn: d.conn}, nil
}
func TestV2UnsupportedCompressionRejectedBeforeStartupOrQuery(t *testing.T) {
	for _, compressor := range []Compressor{SnappyCompressor{}, lz4.LZ4Compressor{}} {
		t.Run(compressor.Name(), func(t *testing.T) {
			client, server := net.Pipe()
			defer server.Close()
			config := DefaultConfig("localhost")
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			session := func() *Session {
				opaqueStorageValuexxxxxxx := new(Session)
				implCluster.SessionData(opaqueStorageValuexxxxxxx).InternalCtx = ctx
				implCluster.SessionData(opaqueStorageValuexxxxxxx).InternalLogger = nopLogger{}
				return opaqueStorageValuexxxxxxx
			}()
			host := func() *HostInfo {
				opaqueStorageValuexxxxxxxx := new(HostInfo)
				implEndpoint.HostInfoData(opaqueStorageValuexxxxxxxx).InternalConnectAddress = net.ParseIP("127.0.0.1")
				implEndpoint.HostInfoData(opaqueStorageValuexxxxxxxx).InternalPort = 9042
				return opaqueStorageValuexxxxxxxx
			}()
			done := make(chan error, 1)
			go func() {
				var header [9]byte
				if _, err := io.ReadFull(server, header[:]); err != nil {
					done <- err
					return
				}
				if header[4] != byte(frm.OpOptions) {
					done <- fmt.Errorf("expected OPTIONS")
					return
				}
				if _, err := io.CopyN(io.Discard, server, int64(binary.BigEndian.Uint32(header[5:]))); err != nil {
					done <- err
					return
				}
				response := []byte{0x84, 0, header[2], header[3], byte(frm.OpSupported), 0, 0, 0, 2, 0, 0}
				if _, err := server.Write(response); err != nil {
					done <- err
					return
				}
				var next [1]byte
				n, err := server.Read(next[:])
				if n != 0 || err == nil {
					done <- fmt.Errorf("unsupported compressor sent STARTUP/query fallback")
					return
				}
				done <- nil
			}()
			cfg := &driverConnConfig{TypedConfig: connectivityConfigPointer(config), ProtoVersion: 4, CQLVersion: "3.0.0", ConnectTimeout: time.Second, HostDialer: step04FixedHostDialer{client}, Compressor: compressor, Logger: nopLogger{}}
			conn, err := implCluster.InternalSessionDialWithoutObserver(session, ctx, host, cfg, connErrorHandlerFn(func(*driverConn, error, bool) {}), 0, 0)
			var unsupported *UnsupportedSettingError
			if conn != nil || !errors.As(err, &unsupported) {
				t.Fatal("requested compression silently downgraded", err)
			}
			if err := <-done; err != nil {
				t.Fatal(err)
			}
		})
	}
	config := DefaultConfig("localhost")
	config.Protocol = 5
	config.Compression = SnappyCompression
	var unsupported *UnsupportedSettingError
	if err := validateConfig(config); !errors.As(err, &unsupported) {
		t.Fatal("known protocol/compressor incompatibility not explicit", err)
	}
}
