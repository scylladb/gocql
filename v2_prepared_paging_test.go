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

import implExecution "github.com/scylladb/gocql/v2/internal/execution"

import (
	implCluster "github.com/scylladb/gocql/v2/internal/cluster"
	implEndpoint "github.com/scylladb/gocql/v2/internal/endpoint"
)

import (
	routing "github.com/scylladb/gocql/v2/internal/routing"

	cqltype "github.com/scylladb/gocql/v2/internal/cqltype"
	model "github.com/scylladb/gocql/v2/internal/model"

	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/maphash"
	"io"
	"net"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/scylladb/gocql/v2/internal/wire"
)

func step03Page(t *testing.T, values ...string) *Page[Bytes] {
	t.Helper()
	typ := codecTestMust(ScalarType(TypeBlob))
	columns := NewColumns(ColumnSpec{Name: "value", Type: typ})
	var body []byte
	for _, v := range values {
		body = binary.BigEndian.AppendUint32(body, uint32(len(v)))
		body = append(body, v...)
	}
	chain := typedTestChain(t, body)
	return codecTestMust(newPage(&responsePage{InternalResponseData: responseData{Columns: columns, Rows: chain, RowCount: int32(len(values)), Owner: wire.NewLease(chain, nil), State: completePageState()}}, OneColumn(BlobCodec), nil))
}
func TestV2PageNextIntoMixedCurrentAndRetention(t *testing.T) {
	p := step03Page(t, "one", "two", "three")
	defer p.Close()
	var first, second Bytes
	if !p.NextInto(&first) || string(first.OwnedBytes()) != "one" {
		t.Fatal(p.Err())
	}
	retained := codecTestMust(p.Retain())
	defer retained.Close()
	original := first
	first = BorrowBytes([]byte("edited"))
	if string(p.Row().OwnedBytes()) != "edited" || string(codecTestMust(p.Clone()).OwnedBytes()) != "edited" {
		t.Fatal("current destination not observed")
	}
	if !p.Next() || string(p.Row().OwnedBytes()) != "two" || string(first.OwnedBytes()) != "edited" {
		t.Fatal("mixed Next modified old destination")
	}
	if !p.NextInto(&second) || string(second.OwnedBytes()) != "three" {
		t.Fatal(p.Err())
	}
	if p.NextInto(&second) || second.Len() != 0 || p.Err() != nil {
		t.Fatal("exhaustion", p.Err())
	}
	if p.NextInto(nil) || p.Err() != nil {
		t.Fatal("terminal nil replaced exhaustion", p.Err())
	}
	if string(original.OwnedBytes()) != "one" {
		t.Fatal("retention lost")
	}
	p.Close()
	if string(original.OwnedBytes()) != "one" {
		t.Fatal("close lost retained page")
	}
	retained.Close()
	if _, err := original.At(0); !errors.Is(err, ErrReleased) {
		t.Fatal(err)
	}
}
func TestV2PageNextIntoTerminalClearing(t *testing.T) {
	for _, mode := range []string{"empty", "nil", "closed", "malformed", "cancelled"} {
		t.Run(mode, func(t *testing.T) {
			p := step03Page(t, "one")
			defer p.Close()
			dst := BorrowBytes([]byte("stale"))
			switch mode {
			case "empty":
				p = step03Page(t)
				defer p.Close()
			case "nil":
				if p.NextInto(nil) {
					t.Fatal("nil accepted")
				}
				var e *ValidationError
				if !errors.As(p.Err(), &e) || implExecution.PageData(p).InternalIndex != 0 {
					t.Fatal(p.Err())
				}
			case "closed":
				p.Close()
			case "malformed":
				implExecution.PageData(p).InternalResponse.Rows = typedTestChain(t, []byte{0, 0, 0, 5, 1})
				implExecution.PageData(p).InternalCursor = wire.NewCursor(implExecution.PageData(p).InternalResponse.Rows)
			case "cancelled":
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				implExecution.PageData(p).InternalCtx = ctx
			}
			if p.NextInto(&dst) || dst.Len() != 0 {
				t.Fatal("false did not clear destination")
			}
			previous := p.Err()
			if p.NextInto(nil) || p.Err() != previous {
				t.Fatal("terminal error changed", p.Err())
			}
		})
	}
}
func TestV2PageNextIntoPartialErrorClears(t *testing.T) {
	integer := codecTestMust(ScalarType(TypeInt))
	blob := codecTestMust(ScalarType(TypeBlob))
	type row struct {
		Number int32
		Value  Bytes
		Unused int
	}
	cols := NewColumns(ColumnSpec{Name: "n", Type: integer}, ColumnSpec{Name: "v", Type: blob})
	body := typedTestChain(t, []byte{0, 0, 0, 4, 0, 0, 0, 7, 0, 0, 0, 5, 1})
	decoder := Row(Column("n", Int32, func(r *row) *int32 { return &r.Number }), Column("v", BlobCodec, func(r *row) *Bytes { return &r.Value }))
	p := codecTestMust(newPage(&responsePage{InternalResponseData: responseData{Columns: cols, Rows: body, RowCount: 1, Owner: wire.NewLease(body, nil)}}, decoder, nil))
	defer p.Close()
	dst := row{Unused: 8}
	if p.NextInto(&dst) || dst.Number != 0 || dst.Unused != 0 || p.Err() == nil {
		t.Fatal("partial row published", dst, p.Err())
	}
}
func TestV2RowsNextIntoPagingAndCloseInterrupt(t *testing.T) {
	s, conns, peers := runtimeTestSession(t, 1)
	nextRequested := make(chan struct{})
	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		for i, body := range [][]byte{runtimeRowsBody([]byte{}), runtimeRowsBody([]byte("next"), "one")} {
			stream := typedTestReadRequest(t, conns[0], peers[0])
			if _, err := peers[0].Write(typedTestResponse(t, 4, stream, body)); err != nil {
				t.Error(err)
				return
			}
			_ = i
		}
		typedTestReadRequest(t, conns[0], peers[0])
		close(nextRequested)
	}()
	b := runtimeQuery(t)
	defer b.Close()
	rows := codecTestMust(Run(context.Background(), s, b, Options{}))
	defer rows.Close()
	var dst Bytes
	if !rows.NextInto(&dst) || string(dst.OwnedBytes()) != "one" {
		t.Fatal(rows.Err())
	}
	retained := codecTestMust(rows.Retain())
	defer retained.Close()
	copyView := dst
	done := make(chan bool)
	go func() { done <- rows.NextInto(&dst) }()
	<-nextRequested
	closed := make(chan error)
	go func() { closed <- rows.Close() }()
	select {
	case ok := <-done:
		if ok || dst.Len() != 0 {
			t.Fatal("cancelled NextInto succeeded")
		}
	case <-time.After(time.Second):
		t.Fatal("close did not interrupt next page")
	}
	if err := <-closed; !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if string(copyView.OwnedBytes()) != "one" {
		t.Fatal("retained view lost")
	}
	<-stopped
	if rows.NextInto(nil) || !errors.Is(rows.Err(), context.Canceled) {
		t.Fatal("terminal error changed")
	}
}
func TestV2RowsActiveNilDoesNotPage(t *testing.T) {
	s, conns, peers := runtimeTestSession(t, 1)
	go func() {
		stream := typedTestReadRequest(t, conns[0], peers[0])
		_, _ = peers[0].Write(typedTestResponse(t, 4, stream, runtimeRowsBody([]byte("more"))))
	}()
	b := runtimeQuery(t)
	defer b.Close()
	rows := codecTestMust(Run(context.Background(), s, b, Options{}))
	defer rows.Close()
	if rows.NextInto(nil) {
		t.Fatal("nil accepted")
	}
	var e *ValidationError
	if !errors.As(rows.Err(), &e) {
		t.Fatal(rows.Err())
	}
	if implExecution.PageData(implExecution.RowsData(rows).InternalPage).InternalIndex != 0 {
		t.Fatal("nil consumed row")
	}
}
func TestV2PagingHistoryCyclesLimitsAndTokenIdentity(t *testing.T) {
	h := pagingHistory{InternalMaxCount: 4, InternalMaxBytes: 4}
	a, b := RestorePageState([]byte("a")), RestorePageState([]byte("b"))
	if h.InternalAdd(a) != nil || h.InternalAdd(b) != nil || !errors.Is(h.InternalAdd(a), ErrPagingNoProgress) {
		t.Fatal("nonconsecutive cycle")
	}
	if h.InternalCount != 2 || h.InternalBytes != 2 {
		t.Fatal("cycle stored twice")
	}
	entry := h.InternalEntries
	found := false
	for _, v := range entry {
		if bytes.Equal(model.PageToken(v.InternalState), model.PageToken(a)) {
			found = &model.PageToken(v.InternalState)[0] == &model.PageToken(a)[0]
		}
	}
	if !found {
		t.Fatal("history copied payload")
	}
	if h.InternalAdd(RestorePageState(nil)) != nil || !errors.Is(h.InternalAdd(RestorePageState([]byte{})), ErrPagingNoProgress) {
		t.Fatal("empty cycle")
	}
	if !errors.Is(h.InternalAdd(RestorePageState([]byte("long"))), ErrPagingStateLimit) || h.InternalCount != 3 {
		t.Fatal("byte limit")
	}
	h = pagingHistory{InternalMaxCount: 1, InternalMaxBytes: 100}
	if h.InternalAdd(a) != nil || !errors.Is(h.InternalAdd(b), ErrPagingStateLimit) {
		t.Fatal("count limit")
	}
}
func TestV2PagingLimitsConfigurationAndCompleteState(t *testing.T) {
	c := DefaultConfig("localhost")
	c.Limits.MaxPagingStates = 0
	if validateConfig(c) == nil {
		t.Fatal("zero state budget")
	}
	c = DefaultConfig("localhost")
	c.Limits.MaxPagingStateBytes = -1
	if validateConfig(c) == nil {
		t.Fatal("negative byte budget")
	}
	if _, err := RunPage[Unit](nil, nil, nil, completePageState(), Options{}); !errors.Is(err, ErrPagingComplete) {
		t.Fatal("complete admitted", err)
	}
}
func TestV2RowsCycleAndBudgetBeforePrefetch(t *testing.T) {
	for _, prefetch := range []int{0, 1} {
		for _, mode := range []string{"cycle", "budget"} {
			t.Run(fmt.Sprintf("prefetch=%v/%s", prefetch, mode), func(t *testing.T) {
				s, conns, peers := runtimeTestSession(t, 1)
				rt := runtimeFor(s)
				rt.InternalConfig.Limits.MaxPagingStates = 2
				rt.InternalConfig.Limits.MaxPagingStateBytes = 2
				profile := defaultProfile()
				profile.Prefetch = prefetch
				handle := codecTestMust(NewProfile(profile))
				tokens := []string{"a", "b", "a"}
				if mode == "budget" {
					tokens = tokens[:2]
				}
				serverDone := make(chan struct{})
				go func() {
					defer close(serverDone)
					for _, token := range tokens {
						stream := typedTestReadRequest(t, conns[0], peers[0])
						_, _ = peers[0].Write(typedTestResponse(t, 4, stream, runtimeRowsBody([]byte(token), token)))
					}
				}()
				b := runtimeQuery(t)
				defer b.Close()
				rows := codecTestMust(Run(context.Background(), s, b, Options{Profile: handle}))
				defer rows.Close()
				if mode == "budget" {
					implExecution.RowsData(rows).InternalHistory.InternalMaxCount = 1
				}
				count := 0
				for rows.Next() {
					count++
				}
				want := ErrPagingNoProgress
				if mode == "budget" {
					want = ErrPagingStateLimit
				}
				if !errors.Is(rows.Err(), want) {
					t.Fatal(count, rows.Err())
				}
				<-serverDone
			})
		}
	}
}

type step03CountingDecoder struct {
	calls *atomic.Int32
	hook  func()
}

func (d step03CountingDecoder) Compile(c Columns) (RowPlan[Bytes], error) {
	d.calls.Add(1)
	if d.hook != nil {
		d.hook()
	}
	return OneColumn(BlobCodec).Compile(c)
}
func (d step03CountingDecoder) Clone(v Bytes) Bytes { return v.Clone() }
func TestV2ResultPlanCacheScopedReuseAndNoCompileLock(t *testing.T) {
	cols := NewColumns(ColumnSpec{Name: "v", Type: codecTestMust(ScalarType(TypeBlob))})
	entry := &typedResultMetadata{InternalColumns: cols}
	cache := &resultPlanCache[Bytes]{}
	var calls atomic.Int32
	d := step03CountingDecoder{calls: &calls}
	for i := 0; i < 100; i++ {
		if _, err := cache.InternalSelectPlan(&responsePage{InternalMetadata: entry, InternalResponseData: responseData{Columns: cols}}, d); err != nil {
			t.Fatal(err)
		}
	}
	if calls.Load() != 1 {
		t.Fatal("recompiled exact entry", calls.Load())
	}
	equivalent := NewColumns(cqltype.ColumnSlice(cols)...)
	if _, err := cache.InternalSelectPlan(&responsePage{InternalResponseData: responseData{Columns: equivalent}}, d); err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 1 {
		t.Fatal("recompiled equal shape")
	}
	another := &resultPlanCache[Bytes]{}
	_, _ = another.InternalSelectPlan(&responsePage{InternalMetadata: entry, InternalResponseData: responseData{Columns: cols}}, d)
	if calls.Load() != 2 {
		t.Fatal("shared different decoder cache")
	}
	cold := &resultPlanCache[Bytes]{}
	entered, done := make(chan struct{}), make(chan struct{})
	d.hook = func() { close(entered); <-done }
	completed := make(chan error)
	go func() {
		_, err := cold.InternalSelectPlan(&responsePage{InternalMetadata: entry, InternalResponseData: responseData{Columns: cols}}, d)
		completed <- err
	}()
	<-entered
	cold.InternalMu.Lock()
	cold.InternalMu.Unlock()
	close(done)
	if err := <-completed; err != nil {
		t.Fatal(err)
	}
}
func step03PreparedState(t *testing.T, s *Session, columns Columns) (*preparedState, *hostPrepared) {
	t.Helper()
	tracker := runtimeFor(s).InternalSchemaTracker()
	dependencies := tracker.InternalDependencies("ks", NewColumns(), columns)
	epoch := dependencies.InternalSnapshot()
	result := &typedResultMetadata{InternalColumns: columns, InternalEpoch: epoch, InternalTracker: tracker}
	hp := &hostPrepared{InternalId: []byte("id"), InternalMetadataID: []byte("meta"), InternalColumns: columns, InternalResult: result, InternalEpoch: epoch, InternalRevision: 1}
	state := &preparedState{InternalSession: s, InternalProtocol: 4, InternalStatement: "SELECT value FROM ks.tbl", InternalKeyspace: "ks", InternalParameters: NewColumns(), InternalColumns: columns, InternalResult: result, InternalDependencies: dependencies, InternalInitialEpoch: epoch, InternalResultHost: UUID{15: 1}, InternalInfo: PreparedInfo{Keyspace: "ks", Table: "tbl"}, InternalHosts: map[UUID]*hostPrepared{{15: 1}: hp}}
	return state, hp
}
func TestV2PreparedResponseRetainsMetadataAcrossSchemaChange(t *testing.T) {
	s := func() *Session {
		opaqueStorageValue := new(Session)
		implCluster.SessionData(opaqueStorageValue).InternalLogger = nopLogger{}
		return opaqueStorageValue
	}()
	columns := NewColumns(ColumnSpec{Keyspace: "ks", Table: "tbl", Name: "value", Type: codecTestMust(ScalarType(TypeBlob))})
	state, hp := step03PreparedState(t, s, columns)
	implCluster.InternalSessionInvalidatePreparedSchema(s, Event{Keyspace: "other", Object: "other", Target: "TABLE"})
	if !hp.InternalEpoch.InternalCurrent() {
		t.Fatal("unrelated keyspace invalidated")
	}
	implCluster.InternalSessionInvalidatePreparedSchema(s, Event{Keyspace: "ks", Object: "other", Target: "TABLE"})
	if !hp.InternalEpoch.InternalCurrent() {
		t.Fatal("unrelated table invalidated")
	}
	implCluster.InternalSessionInvalidatePreparedSchema(s, Event{Keyspace: "ks", Object: "tbl", Target: "TABLE"})
	if hp.InternalEpoch.InternalCurrent() {
		t.Fatal("related table not invalidated")
	}
	response := &responsePage{InternalResponseData: responseData{NoMetadata: true, ColumnCount: 1}}
	if err := selectPreparedResponse(state, UUID{15: 1}, hp, hp.InternalMetadataID, true, response); err != nil || response.InternalMetadata != hp.InternalResult || !response.Columns.Equal(columns) {
		t.Fatal("retained response rejected", err)
	}
	for _, bad := range []*responsePage{{InternalResponseData: responseData{NoMetadata: true, ColumnCount: 2}}, {InternalResponseData: responseData{NoMetadata: true, ColumnCount: 1, MetadataID: []byte("bad")}}} {
		if selectPreparedResponse(state, UUID{15: 1}, hp, hp.InternalMetadataID, true, bad) == nil {
			t.Fatal("mismatched metadata accepted")
		}
	}
}
func TestV2PreparedPublicationExactEntryEpochAndOpaqueIDs(t *testing.T) {
	s := func() *Session {
		opaqueStorageValuex := new(Session)
		implCluster.SessionData(opaqueStorageValuex).InternalLogger = nopLogger{}
		return opaqueStorageValuex
	}()
	blob := codecTestMust(ScalarType(TypeBlob))
	columns := NewColumns(ColumnSpec{Keyspace: "ks", Table: "tbl", Name: "value", Type: blob})
	state, old := step03PreparedState(t, s, columns)
	id := UUID{15: 1}
	newer := *old
	newer.InternalRevision++
	newer.InternalMetadataID = []byte("a")
	state.InternalHosts[id] = &newer
	response := &responsePage{InternalResponseData: responseData{Kind: 2, Columns: columns, MetadataID: []byte("z")}}
	if err := selectPreparedResponse(state, id, old, old.InternalMetadataID, true, response); err != nil {
		t.Fatal(err)
	}
	if state.InternalHosts[id] != &newer {
		t.Fatal("stale response replaced newer entry")
	}
	response = &responsePage{InternalResponseData: responseData{Kind: 2, Columns: columns, MetadataID: []byte("0")}}
	if err := selectPreparedResponse(state, id, &newer, newer.InternalMetadataID, true, response); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(state.InternalHosts[id].InternalMetadataID, []byte("0")) {
		t.Fatal("opaque IDs treated as ordered")
	}
	current := state.InternalHosts[id]
	implCluster.InternalSessionInvalidatePreparedSchema(s, Event{Keyspace: "ks", Object: "tbl", Target: "TABLE"})
	response = &responsePage{InternalResponseData: responseData{Kind: 2, Columns: columns, MetadataID: []byte("later")}}
	_ = selectPreparedResponse(state, id, current, current.InternalMetadataID, true, response)
	if state.InternalHosts[id] != current {
		t.Fatal("stale schema publication")
	}
}
func TestV2PreparedDependencyTypesAndSnapshotChanges(t *testing.T) {
	s := func() *Session {
		opaqueStorageValuexx := new(Session)
		implCluster.SessionData(opaqueStorageValuexx).InternalLogger = nopLogger{}
		return opaqueStorageValuexx
	}()
	integer := codecTestMust(ScalarType(TypeInt))
	udt := codecTestMust(UDTType("ks", "wanted", ColumnSpec{Name: "n", Type: integer}))
	columns := NewColumns(ColumnSpec{Keyspace: "ks", Table: "tbl", Name: "v", Type: udt})
	_, hp := step03PreparedState(t, s, columns)
	implCluster.InternalSessionInvalidatePreparedSchema(s, Event{Keyspace: "ks", Object: "unrelated", Target: "TYPE"})
	if !hp.InternalEpoch.InternalCurrent() {
		t.Fatal("unrelated UDT invalidated")
	}
	implCluster.InternalSessionInvalidatePreparedSchema(s, Event{Keyspace: "ks", Object: "wanted", Target: "TYPE"})
	if hp.InternalEpoch.InternalCurrent() {
		t.Fatal("UDT dependency ignored")
	}
	tracker := runtimeFor(s).InternalSchemaTracker()
	d := tracker.InternalDependencies("ks", NewColumns(), columns)
	stamp := d.InternalSnapshot()
	old := routing.NewMetadataSnapshot(routing.SnapshotState{InternalPartitioner: "p", InternalKeyspaces: map[string]*KeyspaceMetadata{"ks": {Name: "ks", Tables: map[string]*TableMetadata{"tbl": {Name: "tbl", Keyspace: "ks"}}}}})
	next := routing.NewMetadataSnapshot(routing.SnapshotState{InternalPartitioner: "p", InternalKeyspaces: map[string]*KeyspaceMetadata{"ks": cloneV2Keyspace(routing.SnapshotData(old).InternalKeyspaces["ks"])}})
	implCluster.InternalSessionUpdatePreparedSchemaSnapshot(s, old, next)
	if !stamp.InternalCurrent() {
		t.Fatal("unchanged refresh invalidated")
	}
	routing.SnapshotData(next).InternalKeyspaces["ks"].Tables["other"] = &TableMetadata{Keyspace: "ks", Name: "other"}
	implCluster.InternalSessionUpdatePreparedSchemaSnapshot(s, old, next)
	if !stamp.InternalCurrent() {
		t.Fatal("unrelated refresh invalidated")
	}
	delete(routing.SnapshotData(next).InternalKeyspaces["ks"].Tables, "tbl")
	implCluster.InternalSessionUpdatePreparedSchemaSnapshot(s, old, next)
	if stamp.InternalCurrent() {
		t.Fatal("table drop refresh ignored")
	}
}

func step03PreparedBody(id []byte, parameters, columns Columns, pk ...int) []byte {
	b := binary.BigEndian.AppendUint32(nil, 4)
	b = binary.BigEndian.AppendUint16(b, uint16(len(id)))
	b = append(b, id...)
	for part, c := range []Columns{parameters, columns} {
		b = binary.BigEndian.AppendUint32(b, 0)
		b = binary.BigEndian.AppendUint32(b, uint32(c.Len()))
		if part == 0 {
			b = binary.BigEndian.AppendUint32(b, uint32(len(pk)))
			for _, i := range pk {
				b = binary.BigEndian.AppendUint16(b, uint16(i))
			}
		}
		for _, col := range cqltype.ColumnSlice(c) {
			for _, text := range []string{col.Keyspace, col.Table, col.Name} {
				b = binary.BigEndian.AppendUint16(b, uint16(len(text)))
				b = append(b, text...)
			}
			b = binary.BigEndian.AppendUint16(b, uint16(col.Type.Kind()))
		}
	}
	return b
}
func step03ReadRequest(server net.Conn) (byte, int, []byte, error) {
	var h [9]byte
	if _, e := io.ReadFull(server, h[:]); e != nil {
		return 0, 0, nil, e
	}
	b := make([]byte, binary.BigEndian.Uint32(h[5:]))
	_, e := io.ReadFull(server, b)
	return h[4], int(binary.BigEndian.Uint16(h[2:4])), b, e
}
func TestV2PrepareHostFlightsCancellationAndSchemaCompatibility(t *testing.T) {
	for _, incompatible := range []bool{false, true} {
		t.Run(fmt.Sprint(incompatible), func(t *testing.T) {
			s, conns, peers := runtimeTestSession(t, 1)
			conn := conns[0]
			columns := NewColumns(ColumnSpec{Keyspace: "ks", Table: "tbl", Name: "value", Type: codecTestMust(ScalarType(TypeBlob))})
			state, old := step03PreparedState(t, s, columns)
			entered, release := make(chan struct{}), make(chan struct{})
			serverDone := make(chan error, 1)
			go func() {
				op, stream, _, err := step03ReadRequest(peers[0])
				if err != nil {
					serverDone <- err
					return
				}
				if op != 9 {
					serverDone <- fmt.Errorf("opcode %d", op)
					return
				}
				close(entered)
				<-release
				params := NewColumns()
				if incompatible {
					params = NewColumns(ColumnSpec{Name: "new", Type: codecTestMust(ScalarType(TypeInt))})
				}
				_, err = peers[0].Write(typedTestResponse(t, 4, stream, step03PreparedBody([]byte("new"), params, columns)))
				serverDone <- err
			}()
			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan error)
			go func() {
				_, e := runtimeFor(s).InternalPrepareOnConnExpected(ctx, conn, state, encodedValues{Columns: NewColumns()}, true, old)
				done <- e
			}()
			<-entered
			second := make(chan error)
			go func() {
				_, e := runtimeFor(s).InternalPrepareOnConnExpected(context.Background(), conn, state, encodedValues{Columns: NewColumns()}, true, old)
				second <- e
			}()
			deadline := time.Now().Add(time.Second)
			for {
				state.InternalMu.RLock()
				flight := state.InternalPreparing[implEndpoint.InternalHostInfoHostUUID(conn.InternalHost)]
				waiters := int32(0)
				if flight != nil {
					waiters = flight.InternalWaiters.Load()
				}
				state.InternalMu.RUnlock()
				if waiters >= 2 {
					break
				}
				if time.Now().After(deadline) {
					t.Fatal("waiter did not join")
				}
				runtime.Gosched()
			}
			cancel()
			if e := <-done; !errors.Is(e, context.Canceled) {
				t.Fatal(e)
			}
			close(release)
			err := <-second
			var changed *SchemaChangedError
			if incompatible {
				if !errors.As(err, &changed) {
					t.Fatal(err)
				}
			} else if err != nil {
				t.Fatal(err)
			}
			if err := <-serverDone; err != nil {
				t.Fatal(err)
			}
			if !incompatible {
				if hp := state.InternalHosts[implEndpoint.InternalHostInfoHostUUID(conn.InternalHost)]; hp == old || !bytes.Equal(hp.InternalId, []byte("new")) {
					t.Fatal("host ID not updated")
				}
			}
		})
	}
}
func TestV2WarmPreparedHandleSurvivesDiscoveryCacheLock(t *testing.T) {
	s, conns, _ := runtimeTestSession(t, 1)
	cols := NewColumns(ColumnSpec{Keyspace: "ks", Table: "tbl", Name: "value", Type: codecTestMust(ScalarType(TypeBlob))})
	state, hp := step03PreparedState(t, s, cols)
	rt := runtimeFor(s)
	rt.InternalMu.Lock()
	done := make(chan error, 1)
	go func() {
		got, e := runtimeFor(s).InternalPrepareOnConn(context.Background(), conns[0], state, encodedValues{}, false)
		if e == nil && got != hp {
			e = errors.New("wrong entry")
		}
		done <- e
	}()
	select {
	case e := <-done:
		if e != nil {
			t.Fatal(e)
		}
	case <-time.After(time.Second):
		rt.InternalMu.Unlock()
		t.Fatal("warm execution used session discovery lock")
	}
	rt.InternalMu.Unlock()
}

func TestV2PreparedMetadataIntroducesUDTDependency(t *testing.T) {
	s := func() *Session {
		opaqueStorageValuexxx := new(Session)
		implCluster.SessionData(opaqueStorageValuexxx).InternalLogger = nopLogger{}
		return opaqueStorageValuexxx
	}()
	columns := NewColumns(ColumnSpec{Keyspace: "ks", Table: "tbl", Name: "value", Type: codecTestMust(ScalarType(TypeBlob))})
	state, old := step03PreparedState(t, s, columns)
	udt := codecTestMust(UDTType("ks", "introduced", ColumnSpec{Name: "n", Type: codecTestMust(ScalarType(TypeInt))}))
	changed := NewColumns(ColumnSpec{Keyspace: "ks", Table: "tbl", Name: "value", Type: udt})
	response := &responsePage{InternalResponseData: responseData{Kind: 2, Columns: changed, MetadataID: []byte("next")}}
	if err := selectPreparedResponse(state, UUID{15: 1}, old, old.InternalMetadataID, true, response, state.InternalDependencies.InternalTracker.InternalStart()); err != nil {
		t.Fatal(err)
	}
	current := state.InternalHosts[UUID{15: 1}]
	if current == old || !current.InternalEpoch.InternalCurrent() {
		t.Fatal("changed result not installed")
	}
	implCluster.InternalSessionInvalidatePreparedSchema(s, Event{Keyspace: "ks", Target: "TYPE", Object: "introduced"})
	if current.InternalEpoch.InternalCurrent() || state.InternalCacheCurrent() {
		t.Fatal("new UDT dependency was not registered")
	}
}
func TestV2PreparedNewDependencyEventDuringAttemptFencesPublication(t *testing.T) {
	s := func() *Session {
		opaqueStorageValuexxxx := new(Session)
		implCluster.SessionData(opaqueStorageValuexxxx).InternalLogger = nopLogger{}
		return opaqueStorageValuexxxx
	}()
	columns := NewColumns(ColumnSpec{Keyspace: "ks", Table: "tbl", Name: "value", Type: codecTestMust(ScalarType(TypeBlob))})
	state, old := step03PreparedState(t, s, columns)
	started := state.InternalDependencies.InternalTracker.InternalStart()
	implCluster.InternalSessionInvalidatePreparedSchema(s, Event{Keyspace: "ks", Target: "TYPE", Object: "introduced"})
	udt := codecTestMust(UDTType("ks", "introduced", ColumnSpec{Name: "n", Type: codecTestMust(ScalarType(TypeInt))}))
	changed := NewColumns(ColumnSpec{Keyspace: "ks", Table: "tbl", Name: "value", Type: udt})
	response := &responsePage{InternalResponseData: responseData{Kind: 2, Columns: changed, MetadataID: []byte("stale")}}
	if err := selectPreparedResponse(state, UUID{15: 1}, old, old.InternalMetadataID, true, response, started); err != nil {
		t.Fatal(err)
	}
	if state.InternalHosts[UUID{15: 1}] != old {
		t.Fatal("new dependency event lost")
	}
	if !response.Columns.Equal(changed) {
		t.Fatal("losing response lost supplied metadata")
	}
}
func step03ErrorResponse(t *testing.T, stream, code int, id []byte) []byte {
	body := binary.BigEndian.AppendUint32(nil, uint32(code))
	body = binary.BigEndian.AppendUint16(body, 4)
	body = append(body, "test"...)
	if code == ErrCodeUnprepared {
		body = binary.BigEndian.AppendUint16(body, uint16(len(id)))
		body = append(body, id...)
	}
	response := typedTestResponse(t, 4, stream, body)
	response[4] = 0
	return response
}

type step03RetryOnce struct{}

func (step03RetryOnce) Decide(e AttemptError) RetryDecision {
	if e.Attempt == 1 {
		return RetryDecision{Action: RetrySame}
	}
	return RetryDecision{}
}
func TestV2PreparedRetryRepreparePagingNeverReencodes(t *testing.T) {
	s, conns, peers := runtimeTestSession(t, 1)
	implCluster.SessionData(s).InternalCfg.Keyspace = "ks"
	typ := codecTestMust(ScalarType(TypeBlob))
	columns := NewColumns(ColumnSpec{Keyspace: "ks", Table: "tbl", Name: "value", Type: typ})
	state, old := step03PreparedState(t, s, columns)
	state.InternalParameters = columns
	state.InternalInfo.PartitionKeyIndexes = []int{0}
	count := 0
	params := codecTestMust(OneParam[[]byte](countingBlobEncoder{calls: &count}).Compile(columns))
	handle := func() *Prepared[[]byte,
		Bytes] {
		opaqueStorageValuexxxxx := new(Prepared[[]byte,
			Bytes])
		implExecution.PreparedData(opaqueStorageValuexxxxx).InternalState = state
		implExecution.PreparedData(opaqueStorageValuexxxxx).InternalParams = params
		implExecution.PreparedData(opaqueStorageValuexxxxx).InternalRows = OneColumn(BlobCodec)
		implExecution.PreparedData(opaqueStorageValuexxxxx).InternalPlans = &resultPlanCache[Bytes]{}
		return opaqueStorageValuexxxxx
	}()
	input := []byte("canonical-payload")
	bound := codecTestMust(BindBorrowed(handle, input))
	defer bound.Close()
	serverDone := make(chan error, 1)
	go func() {
		for step := 0; step < 5; step++ {
			op, stream, body, err := step03ReadRequest(peers[0])
			if err != nil {
				serverDone <- err
				return
			}
			if step == 2 {
				if op != 9 {
					serverDone <- fmt.Errorf("wanted PREPARE got %d", op)
					return
				}
				_, err = peers[0].Write(typedTestResponse(t, 4, stream, step03PreparedBody([]byte("new"), columns, columns, 0)))
			} else {
				if op != 10 || !bytes.Contains(body, input) {
					serverDone <- fmt.Errorf("missing original execution bytes step %d", step)
					return
				}
				switch step {
				case 0:
					_, err = peers[0].Write(step03ErrorResponse(t, stream, ErrCodeServer, nil))
				case 1:
					_, err = peers[0].Write(step03ErrorResponse(t, stream, ErrCodeUnprepared, old.InternalId))
				case 3:
					_, err = peers[0].Write(typedTestResponse(t, 4, stream, runtimeRowsBody([]byte("a"), "one")))
				case 4:
					_, err = peers[0].Write(typedTestResponse(t, 4, stream, runtimeRowsBody(nil, "two")))
				}
			}
			if err != nil {
				serverDone <- err
				return
			}
		}
		serverDone <- nil
	}()
	profile := defaultProfile()
	profile.Retry = step03RetryOnce{}
	options := Options{Profile: codecTestMust(NewProfile(profile))}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	rows := codecTestMust(Run(ctx, s, bound, options))
	defer rows.Close()
	bound.Close()
	var dst Bytes
	var values []string
	for rows.NextInto(&dst) {
		values = append(values, string(dst.OwnedBytes()))
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if fmt.Sprint(values) != "[one two]" || count != 1 {
		t.Fatal(values, count)
	}
	if err := bound.Wait(ctx); err != nil {
		t.Fatal(err)
	}
	if err := <-serverDone; err != nil {
		t.Fatal(err)
	}
	_ = conns
}
func TestV2PreparedHostRoutingChangesFailBeforeResend(t *testing.T) {
	for _, mode := range []string{"partition-key-order", "table", "type", "partitioner", "conditional"} {
		t.Run(mode, func(t *testing.T) {
			s, conns, peers := runtimeTestSession(t, 1)
			typ := codecTestMust(ScalarType(TypeBlob))
			parameters := NewColumns(ColumnSpec{Keyspace: "ks", Table: "tbl", Name: "a", Type: typ}, ColumnSpec{Keyspace: "ks", Table: "tbl", Name: "b", Type: typ})
			columns := NewColumns(cqltype.ColumnSlice(parameters)[0])
			state, old := step03PreparedState(t, s, columns)
			state.InternalParameters = parameters
			state.InternalInfo.PartitionKeyIndexes = []int{0, 1}
			current, pk := parameters, []int{0, 1}
			switch mode {
			case "partition-key-order":
				pk = []int{1, 0}
			case "table":
				changed := append([]ColumnSpec(nil), cqltype.ColumnSlice(parameters)...)
				changed[0].Table = "other"
				current = NewColumns(changed...)
			case "type":
				changed := append([]ColumnSpec(nil), cqltype.ColumnSlice(parameters)...)
				changed[1].Type = codecTestMust(ScalarType(TypeInt))
				current = NewColumns(changed...)
			case "partitioner":
				state.InternalPartitioner = "different"
			case "conditional":
				state.InternalInfo.Conditional = true
			}
			done := make(chan error, 1)
			go func() {
				_, stream, _, err := step03ReadRequest(peers[0])
				if err == nil {
					_, err = peers[0].Write(typedTestResponse(t, 4, stream, step03PreparedBody([]byte("new"), current, columns, pk...)))
				}
				done <- err
			}()
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			_, err := runtimeFor(s).InternalPrepareOnConnExpected(ctx, conns[0], state, encodedValues{Columns: parameters}, true, old)
			var changed *SchemaChangedError
			if !errors.As(err, &changed) {
				t.Fatal(mode, err)
			}
			if state.InternalHosts[UUID{15: 1}] != old {
				t.Fatal("incompatible prepare installed")
			}
			if err := <-done; err != nil {
				t.Fatal(err)
			}
		})
	}
}
func TestV2PreparedWorkerShutdownJoinsWithoutInput(t *testing.T) {
	s, conns, peers := runtimeTestSession(t, 1)
	cols := NewColumns(ColumnSpec{Keyspace: "ks", Table: "tbl", Name: "value", Type: codecTestMust(ScalarType(TypeBlob))})
	state, old := step03PreparedState(t, s, cols)
	requested := make(chan struct{})
	go func() { _, _, _, _ = step03ReadRequest(peers[0]); close(requested) }()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := runtimeFor(s).InternalPrepareOnConnExpected(ctx, conns[0], state, encodedValues{Columns: NewColumns()}, true, old)
		done <- err
	}()
	<-requested
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	closed := make(chan error, 1)
	go func() { closed <- s.Close() }()
	select {
	case err := <-closed:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("session did not join prepare worker")
	}
	state.InternalMu.RLock()
	pending := len(state.InternalPreparing)
	state.InternalMu.RUnlock()
	if pending != 0 {
		t.Fatal("worker retained after close")
	}
}

func TestV2PreparedColdHostsDoNotSerialize(t *testing.T) {
	s, conns, peers := runtimeTestSession(t, 2)
	columns := NewColumns(ColumnSpec{Keyspace: "ks", Table: "tbl", Name: "value", Type: codecTestMust(ScalarType(TypeBlob))})
	state, old := step03PreparedState(t, s, columns)
	firstRead, release := make(chan struct{}), make(chan struct{})
	servers := make(chan error, 2)
	go func() {
		_, stream, _, err := step03ReadRequest(peers[0])
		close(firstRead)
		<-release
		if err == nil {
			_, err = peers[0].Write(typedTestResponse(t, 4, stream, step03PreparedBody([]byte("first"), NewColumns(), columns)))
		}
		servers <- err
	}()
	go func() {
		_, stream, _, err := step03ReadRequest(peers[1])
		if err == nil {
			_, err = peers[1].Write(typedTestResponse(t, 4, stream, step03PreparedBody([]byte("second"), NewColumns(), columns)))
		}
		servers <- err
	}()
	first := make(chan error, 1)
	go func() {
		_, err := runtimeFor(s).InternalPrepareOnConnExpected(context.Background(), conns[0], state, encodedValues{Columns: NewColumns()}, true, old)
		first <- err
	}()
	<-firstRead
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	entry, err := runtimeFor(s).InternalPrepareOnConn(ctx, conns[1], state, encodedValues{Columns: NewColumns()}, false)
	if err != nil || !bytes.Equal(entry.InternalId, []byte("second")) {
		t.Fatal("unrelated host serialized", err)
	}
	close(release)
	if err := <-first; err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		if err := <-servers; err != nil {
			t.Fatal(err)
		}
	}
}
func TestV2PreparedPrepareResultDoesNotOverwriteReplacement(t *testing.T) {
	for _, change := range []string{"entry", "epoch"} {
		t.Run(change, func(t *testing.T) {
			s, conns, peers := runtimeTestSession(t, 1)
			cols := NewColumns(ColumnSpec{Keyspace: "ks", Table: "tbl", Name: "value", Type: codecTestMust(ScalarType(TypeBlob))})
			state, old := step03PreparedState(t, s, cols)
			received, release := make(chan struct{}), make(chan struct{})
			serverDone := make(chan error, 1)
			go func() {
				_, stream, _, err := step03ReadRequest(peers[0])
				close(received)
				<-release
				if err == nil {
					_, err = peers[0].Write(typedTestResponse(t, 4, stream, step03PreparedBody([]byte("late"), NewColumns(), cols)))
				}
				serverDone <- err
			}()
			result := make(chan *hostPrepared, 1)
			failure := make(chan error, 1)
			go func() {
				entry, err := runtimeFor(s).InternalPrepareOnConnExpected(context.Background(), conns[0], state, encodedValues{Columns: NewColumns()}, true, old)
				result <- entry
				failure <- err
			}()
			<-received
			expected := old
			if change == "entry" {
				replacement := *old
				replacement.InternalId = []byte("current")
				expected = &replacement
				state.InternalMu.Lock()
				state.InternalHosts[UUID{15: 1}] = expected
				state.InternalMu.Unlock()
			} else {
				implCluster.InternalSessionInvalidatePreparedSchema(s, Event{Keyspace: "ks", Target: "TABLE", Object: "tbl"})
			}
			close(release)
			entry := <-result
			if err := <-failure; err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(entry.InternalId, []byte("late")) {
				t.Fatal("losing response unavailable")
			}
			state.InternalMu.RLock()
			actual := state.InternalHosts[UUID{15: 1}]
			state.InternalMu.RUnlock()
			if actual != expected {
				t.Fatal("late prepare overwrote replacement")
			}
			if err := <-serverDone; err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestV2PagingHistoryHashCollisionComparesFullBytes(t *testing.T) {
	a, b := RestorePageState([]byte("a")), RestorePageState([]byte("b"))
	h := pagingHistory{InternalMaxCount: 4, InternalMaxBytes: 8}
	_ = h.InternalAdd(b)
	hash := maphash.Bytes(h.InternalSeed, model.PageToken(b))
	h.InternalEntries[hash] = pagingHistoryEntry{InternalState: a}
	h.InternalCount = 1
	h.InternalBytes = 1
	if err := h.InternalAdd(b); err != nil {
		t.Fatal("hash collision mistaken for equality", err)
	}
	if h.InternalCount != 2 || h.InternalEntries[hash].InternalNext == nil {
		t.Fatal("collision entry lost")
	}
	if !errors.Is(h.InternalAdd(b), ErrPagingNoProgress) {
		t.Fatal("exact duplicate lost")
	}
}
func TestV2ManualPageEmptyContinuationSurvivesClose(t *testing.T) {
	p := step03Page(t)
	implExecution.PageData(p).InternalResponse.State = RestorePageState(nil)
	state := p.State()
	p.Close()
	if !state.More() || state.Complete() || state.Bytes() == nil || len(state.Bytes()) != 0 {
		t.Fatal("empty continuation collapsed")
	}
	if !p.State().More() {
		t.Fatal("close lost state")
	}
}
func TestV2PreparedOldPageKeepsMatchingPlan(t *testing.T) {
	s := func() *Session {
		opaqueStorageValuexxxxxx := new(Session)
		implCluster.SessionData(opaqueStorageValuexxxxxx).InternalLogger = nopLogger{}
		return opaqueStorageValuexxxxxx
	}()
	columns := NewColumns(ColumnSpec{Keyspace: "ks", Table: "tbl", Name: "value", Type: codecTestMust(ScalarType(TypeBlob))})
	state, old := step03PreparedState(t, s, columns)
	host := UUID{15: 1}
	changed := NewColumns(ColumnSpec{Keyspace: "ks", Table: "tbl", Name: "value", Type: codecTestMust(ScalarType(TypeInt))})
	replacement := &responsePage{InternalResponseData: responseData{Kind: 2, Columns: changed, MetadataID: []byte("new")}}
	if err := selectPreparedResponse(state, host, old, old.InternalMetadataID, true, replacement); err != nil {
		t.Fatal(err)
	}
	chain := typedTestChain(t, []byte{0, 0, 0, 3, 'o', 'l', 'd'})
	response := &responsePage{InternalResponseData: responseData{NoMetadata: true, ColumnCount: 1, RowCount: 1, Rows: chain, Owner: wire.NewLease(chain, nil)}}
	if err := selectPreparedResponse(state, host, old, old.InternalMetadataID, true, response); err != nil {
		t.Fatal(err)
	}
	p := codecTestMust(newPageWithPlans(response, OneColumn(BlobCodec), &resultPlanCache[Bytes]{}, nil))
	defer p.Close()
	var dst Bytes
	if !p.NextInto(&dst) || string(dst.OwnedBytes()) != "old" {
		t.Fatal("old matching page lost", p.Err())
	}
}

func TestV2ResultPlanCompileSchemaFenceKeepsRetainedPage(t *testing.T) {
	s := func() *Session {
		opaqueStorageValuexxxxxxx := new(Session)
		implCluster.SessionData(opaqueStorageValuexxxxxxx).InternalLogger = nopLogger{}
		return opaqueStorageValuexxxxxxx
	}()
	columns := NewColumns(ColumnSpec{Keyspace: "ks", Table: "tbl", Name: "value", Type: codecTestMust(ScalarType(TypeBlob))})
	_, hp := step03PreparedState(t, s, columns)
	var calls atomic.Int32
	decoder := step03CountingDecoder{calls: &calls, hook: func() {
		implCluster.InternalSessionInvalidatePreparedSchema(s, Event{Keyspace: "ks", Target: "TABLE", Object: "tbl"})
	}}
	cache := &resultPlanCache[Bytes]{}
	response := &responsePage{InternalMetadata: hp.InternalResult, InternalResponseData: responseData{Columns: columns}}
	if _, err := cache.InternalSelectPlan(response, decoder); err != nil {
		t.Fatal(err)
	}
	cache.InternalMu.Lock()
	cached := cache.InternalPlan
	cache.InternalMu.Unlock()
	if cached != nil {
		t.Fatal("compile published through changed schema epoch")
	}
}

func TestV2PagerCompletionCanInspectIterator(t *testing.T) {
	p := step03Page(t)
	pageObserved := false
	implExecution.PageData(p).InternalRelease = func() { _ = p.Err(); _ = p.Close(); pageObserved = true }
	p.Close()
	if !pageObserved {
		t.Fatal("page completion missing")
	}
	rows := func() *Rows[Bytes] {
		opaqueStorageValuexxxxxxxx := new(Rows[Bytes])
		implExecution.RowsData(opaqueStorageValuexxxxxxxx).InternalPage = step03Page(t)
		implExecution.RowsData(opaqueStorageValuexxxxxxxx).InternalCtx = context.Background()
		return opaqueStorageValuexxxxxxxx
	}()
	rowsObserved := false
	implExecution.RowsData(rows).InternalRelease = func() { _ = rows.Err(); _ = rows.Info(); _ = rows.Close(); rowsObserved = true }
	rows.Close()
	if !rowsObserved {
		t.Fatal("rows completion missing")
	}
}

func TestV2PagerConcurrentCloseReleasesExactlyOnce(t *testing.T) {
	p := step03Page(t, "value")
	var releases atomic.Int32
	implExecution.PageData(p).InternalRelease = func() { releases.Add(1); _ = p.Err(); _ = p.Close() }
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); _ = p.Close() }()
	}
	wg.Wait()
	if releases.Load() != 1 {
		t.Fatal("page release count", releases.Load())
	}
	rows := func() *Rows[Bytes] {
		opaqueStorageValuexxxxxxxxx := new(Rows[Bytes])
		implExecution.RowsData(opaqueStorageValuexxxxxxxxx).InternalPage = step03Page(t)
		implExecution.RowsData(opaqueStorageValuexxxxxxxxx).InternalCtx = context.Background()
		return opaqueStorageValuexxxxxxxxx
	}()
	releases.Store(0)
	implExecution.RowsData(rows).InternalRelease = func() { releases.Add(1); _ = rows.Info(); _ = rows.Close() }
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); _ = rows.Close() }()
	}
	wg.Wait()
	if releases.Load() != 1 {
		t.Fatal("rows release count", releases.Load())
	}
}

func TestV2DeliveredPageSurvivesSessionCloseUntilAdvance(t *testing.T) {
	for _, manual := range []bool{false, true} {
		t.Run(fmt.Sprint(manual), func(t *testing.T) {
			s, conns, peers := runtimeTestSession(t, 1)
			serverDone := make(chan struct{})
			go func() {
				defer close(serverDone)
				stream := typedTestReadRequest(t, conns[0], peers[0])
				_, _ = peers[0].Write(typedTestResponse(t, 4, stream, runtimeRowsBody([]byte("more"), "delivered")))
			}()
			b := runtimeQuery(t)
			defer b.Close()
			var dst Bytes
			if manual {
				p := codecTestMust(RunPage(context.Background(), s, b, StartPage(), Options{}))
				defer p.Close()
				if !p.NextInto(&dst) {
					t.Fatal(p.Err())
				}
				before := dst
				_ = s.Close()
				if string(before.OwnedBytes()) != "delivered" {
					t.Fatal("shutdown recycled delivered page")
				}
				if p.NextInto(&dst) || dst.Len() != 0 || !errors.Is(p.Err(), ErrClosed) {
					t.Fatal("closed page advance", p.Err())
				}
			} else {
				r := codecTestMust(Run(context.Background(), s, b, Options{}))
				defer r.Close()
				if !r.NextInto(&dst) {
					t.Fatal(r.Err())
				}
				before := dst
				_ = s.Close()
				if string(before.OwnedBytes()) != "delivered" {
					t.Fatal("shutdown recycled delivered rows")
				}
				if r.NextInto(&dst) || dst.Len() != 0 || !errors.Is(r.Err(), ErrClosed) {
					t.Fatal("closed rows advance", r.Err())
				}
			}
			<-serverDone
		})
	}
}

func TestV2PreparedConflictingHostsRetainResponseAndCoalesce(t *testing.T) {
	s := func() *Session {
		opaqueStorageValuexxxxxxxxxx := new(Session)
		implCluster.SessionData(opaqueStorageValuexxxxxxxxxx).InternalLogger = nopLogger{}
		return opaqueStorageValuexxxxxxxxxx
	}()
	implCluster.SessionData(s).InternalCtx, implCluster.SessionData(s).InternalCancel = context.WithCancel(context.Background())
	defer s.Close()
	columns := NewColumns(ColumnSpec{Keyspace: "ks", Table: "tbl", Name: "value", Type: codecTestMust(ScalarType(TypeBlob))})
	state, old := step03PreparedState(t, s, columns)
	host := UUID{15: 2}
	second := *old
	state.InternalHosts[host] = &second
	changed := NewColumns(ColumnSpec{Keyspace: "ks", Table: "tbl", Name: "value", Type: codecTestMust(ScalarType(TypeInt))})
	for i := 0; i < 100; i++ {
		response := &responsePage{InternalResponseData: responseData{Kind: 2, Columns: changed, MetadataID: []byte("conflict")}}
		if err := selectPreparedResponse(state, host, &second, second.InternalMetadataID, true, response); err != nil {
			t.Fatal(err)
		}
		if !response.Columns.Equal(changed) {
			t.Fatal("matching response metadata lost")
		}
	}
	if !state.InternalConflictRefresh.Load() || !implCluster.InternalSessionPeripheral(s).InternalRetryScheduled.Load() {
		t.Fatal("conflict not scheduled")
	}
	state.InternalMu.RLock()
	latest := state.InternalResult
	state.InternalMu.RUnlock()
	if latest != old.InternalResult {
		t.Fatal("remote order guessed")
	}
	if old.InternalEpoch.InternalCurrent() {
		t.Fatal("conflicting cache entries not invalidated")
	}
}

func TestV2ResultPlanPublicationSharesEpochGate(t *testing.T) {
	s := func() *Session {
		opaqueStorageValuexxxxxxxxxxx := new(Session)
		implCluster.SessionData(opaqueStorageValuexxxxxxxxxxx).InternalLogger = nopLogger{}
		return opaqueStorageValuexxxxxxxxxxx
	}()
	columns := NewColumns(ColumnSpec{Keyspace: "ks", Table: "tbl", Name: "value", Type: codecTestMust(ScalarType(TypeBlob))})
	_, hp := step03PreparedState(t, s, columns)
	tracker := hp.InternalResult.InternalTracker
	entered, release := make(chan struct{}), make(chan struct{})
	var calls atomic.Int32
	decoder := step03CountingDecoder{calls: &calls, hook: func() {
		if !tracker.InternalMu.TryLock() {
			t.Error("user Compile ran under epoch gate")
		} else {
			tracker.InternalMu.Unlock()
		}
		close(entered)
		<-release
	}}
	cache := &resultPlanCache[Bytes]{}
	done := make(chan error, 1)
	go func() {
		_, err := cache.InternalSelectPlan(&responsePage{InternalMetadata: hp.InternalResult, InternalResponseData: responseData{Columns: columns}}, decoder)
		done <- err
	}()
	<-entered
	cache.InternalMu.Lock()
	close(release)
	deadline := time.Now().Add(time.Second)
	for tracker.InternalMu.TryLock() {
		tracker.InternalMu.Unlock()
		if time.Now().After(deadline) {
			cache.InternalMu.Unlock()
			t.Fatal("cache publication did not acquire epoch gate")
		}
		runtime.Gosched()
	}
	invalidating, invalidated := make(chan struct{}), make(chan struct{})
	go func() {
		close(invalidating)
		tracker.InternalBump(preparedDependencyKey{InternalKind: 'b', InternalKeyspace: "ks", InternalName: "tbl"})
		close(invalidated)
	}()
	<-invalidating
	select {
	case <-invalidated:
		cache.InternalMu.Unlock()
		t.Fatal("invalidation interleaved with publication")
	default:
	}
	cache.InternalMu.Unlock()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	<-invalidated
	if hp.InternalEpoch.InternalCurrent() {
		t.Fatal("invalidation was lost")
	}
}
func TestV2InitialPrepareDoesNotCacheChangedEpoch(t *testing.T) {
	s, conns, peers := runtimeTestSession(t, 1)
	implCluster.SessionData(s).InternalCfg.Keyspace = "ks"
	columns := NewColumns(ColumnSpec{Keyspace: "ks", Table: "tbl", Name: "value", Type: codecTestMust(ScalarType(TypeBlob))})
	serverDone := make(chan error, 1)
	go func() {
		_, stream, _, err := step03ReadRequest(peers[0])
		if err == nil {
			implCluster.InternalSessionInvalidatePreparedSchema(s, Event{Keyspace: "ks", Target: "TABLE", Object: "tbl"})
			_, err = peers[0].Write(typedTestResponse(t, 4, stream, step03PreparedBody([]byte("old"), NewColumns(), columns)))
		}
		serverDone <- err
	}()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	state, err := runtimeFor(s).InternalPrepareTyped(ctx, "SELECT value FROM ks.tbl", "ks", *func() *PrepareOptions {
		opaqueStorageValuexxxxxxxxxxxx := new(PrepareOptions)
		implExecution.PrepareOptionsData(opaqueStorageValuexxxxxxxxxxxx).InternalConnection = conns[0]
		return opaqueStorageValuexxxxxxxxxxxx
	}())
	if err != nil {
		t.Fatal(err)
	}
	if state.InternalCacheCurrent() {
		t.Fatal("prepare crossing relevant epoch accepted as current")
	}
	rt := runtimeFor(s)
	rt.InternalMu.Lock()
	count := len(rt.InternalPrepared)
	rt.InternalMu.Unlock()
	if count != 0 {
		t.Fatal("stale initial prepare published")
	}
	if err := <-serverDone; err != nil {
		t.Fatal(err)
	}
}

func TestV2StalePrepareFailurePreservesDiscoveryBound(t *testing.T) {
	s, conns, peers := runtimeTestSession(t, 1)
	implCluster.SessionData(s).InternalCfg.Keyspace = "ks"
	cols := NewColumns(ColumnSpec{Keyspace: "ks", Table: "tbl", Name: "value", Type: codecTestMust(ScalarType(TypeBlob))})
	state, _ := step03PreparedState(t, s, cols)
	rt := runtimeFor(s)
	rt.InternalConfig.PreparedCacheSize = 1
	key := prepareCacheKey{InternalCql: "A", InternalKeyspace: "ks"}
	rt.InternalPrepared[key] = state
	rt.InternalPrepareOrder = []prepareCacheKey{key}
	implCluster.InternalSessionInvalidatePreparedSchema(s, Event{Keyspace: "ks", Target: "TABLE", Object: "tbl"})
	done := make(chan error, 1)
	go func() {
		for i := 0; i < 3; i++ {
			_, stream, _, err := step03ReadRequest(peers[0])
			if err != nil {
				done <- err
				return
			}
			var response []byte
			if i == 0 {
				response = step03ErrorResponse(t, stream, ErrCodeServer, nil)
			} else {
				response = typedTestResponse(t, 4, stream, step03PreparedBody([]byte{byte(i)}, NewColumns(), cols))
			}
			if _, err = peers[0].Write(response); err != nil {
				done <- err
				return
			}
		}
		done <- nil
	}()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	options := *func() *PrepareOptions {
		opaqueStorageValuexxxxxxxxxxxxx := new(PrepareOptions)
		implExecution.PrepareOptionsData(opaqueStorageValuexxxxxxxxxxxxx).InternalConnection = conns[0]
		return opaqueStorageValuexxxxxxxxxxxxx
	}()
	if _, err := runtimeFor(s).InternalPrepareTyped(ctx, "A", "ks", options); err == nil {
		t.Fatal("failed prepare accepted")
	}
	rt.InternalMu.Lock()
	staleOrder := len(rt.InternalPrepareOrder)
	rt.InternalMu.Unlock()
	if staleOrder != 0 {
		t.Fatal("stale failed entry retained LRU record")
	}
	if _, err := runtimeFor(s).InternalPrepareTyped(ctx, "B", "ks", options); err != nil {
		t.Fatal(err)
	}
	if _, err := runtimeFor(s).InternalPrepareTyped(ctx, "C", "ks", options); err != nil {
		t.Fatal(err)
	}
	rt.InternalMu.Lock()
	count, order := len(rt.InternalPrepared), len(rt.InternalPrepareOrder)
	_, latest := rt.InternalPrepared[prepareCacheKey{InternalCql: "C", InternalKeyspace: "ks"}]
	rt.InternalMu.Unlock()
	if count != 1 || order != 1 || !latest {
		t.Fatal("discovery cache exceeded bound", count, order)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestV2DiscoveryOrderRemovesAllDuplicates(t *testing.T) {
	a, b := prepareCacheKey{InternalCql: "a"}, prepareCacheKey{InternalCql: "b"}
	rt := &sessionRuntime{InternalPrepareOrder: []prepareCacheKey{a, b, a}}
	rt.InternalTouchPrepare(a)
	if len(rt.InternalPrepareOrder) != 2 || rt.InternalPrepareOrder[0] != b || rt.InternalPrepareOrder[1] != a {
		t.Fatal("duplicate LRU order", rt.InternalPrepareOrder)
	}
	rt.InternalForgetPrepareOrder(a)
	if len(rt.InternalPrepareOrder) != 1 || rt.InternalPrepareOrder[0] != b {
		t.Fatal("forget did not remove key")
	}
}
func TestV2DiscoveryCapacityZeroAndConcurrentMisses(t *testing.T) {
	for _, capacity := range []int{0, 1} {
		t.Run(fmt.Sprint(capacity), func(t *testing.T) {
			s, conns, peers := runtimeTestSession(t, 1)
			implCluster.SessionData(s).InternalCfg.Keyspace = "ks"
			rt := runtimeFor(s)
			rt.InternalProfile = DefaultConfig("localhost").DefaultProfile
			rt.InternalConfig.PreparedCacheSize = capacity
			columns := NewColumns(ColumnSpec{Keyspace: "ks", Table: "tbl", Name: "value", Type: codecTestMust(ScalarType(TypeBlob))})
			serverDone := make(chan error, 1)
			go func() {
				streams := make([]int, 2)
				for i := range streams {
					_, stream, _, err := step03ReadRequest(peers[0])
					if err != nil {
						serverDone <- err
						return
					}
					streams[i] = stream
				}
				for i, stream := range streams {
					if _, err := peers[0].Write(typedTestResponse(t, 4, stream, step03PreparedBody([]byte{byte(i + 1)}, NewColumns(), columns))); err != nil {
						serverDone <- err
						return
					}
				}
				serverDone <- nil
			}()
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			done := make(chan error, 2)
			for _, cql := range []string{"A", "B"} {
				go func(cql string) {
					_, err := runtimeFor(s).InternalPrepareTyped(ctx, cql, "ks", *func() *PrepareOptions {
						opaqueStorageValuexxxxxxxxxxxxxx := new(PrepareOptions)
						implExecution.PrepareOptionsData(opaqueStorageValuexxxxxxxxxxxxxx).InternalConnection = conns[0]
						return opaqueStorageValuexxxxxxxxxxxxxx
					}())
					done <- err
				}(cql)
			}
			for i := 0; i < 2; i++ {
				if err := <-done; err != nil {
					t.Fatal(err)
				}
			}
			if err := <-serverDone; err != nil {
				t.Fatal(err)
			}
			rt.InternalMu.Lock()
			count, order := len(rt.InternalPrepared), len(rt.InternalPrepareOrder)
			rt.InternalMu.Unlock()
			if count != capacity || order != capacity {
				t.Fatal("concurrent discovery exceeded capacity", count, order, capacity)
			}
		})
	}
}
