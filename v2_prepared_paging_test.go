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
	return codecTestMust(newPage(&responsePage{responseData: responseData{Columns: columns, Rows: chain, RowCount: int32(len(values)), Owner: wire.NewLease(chain, nil), State: completePageState()}}, OneColumn(BlobCodec), nil))
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
				if !errors.As(p.Err(), &e) || p.index != 0 {
					t.Fatal(p.Err())
				}
			case "closed":
				p.Close()
			case "malformed":
				p.response.Rows = typedTestChain(t, []byte{0, 0, 0, 5, 1})
				p.cursor = wire.NewCursor(p.response.Rows)
			case "cancelled":
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				p.ctx = ctx
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
	p := codecTestMust(newPage(&responsePage{responseData: responseData{Columns: cols, Rows: body, RowCount: 1, Owner: wire.NewLease(body, nil)}}, decoder, nil))
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
	if rows.page.index != 0 {
		t.Fatal("nil consumed row")
	}
}
func TestV2PagingHistoryCyclesLimitsAndTokenIdentity(t *testing.T) {
	h := pagingHistory{maxCount: 4, maxBytes: 4}
	a, b := RestorePageState([]byte("a")), RestorePageState([]byte("b"))
	if h.add(a) != nil || h.add(b) != nil || !errors.Is(h.add(a), ErrPagingNoProgress) {
		t.Fatal("nonconsecutive cycle")
	}
	if h.count != 2 || h.bytes != 2 {
		t.Fatal("cycle stored twice")
	}
	entry := h.entries
	found := false
	for _, v := range entry {
		if bytes.Equal(model.PageToken(v.state), model.PageToken(a)) {
			found = &model.PageToken(v.state)[0] == &model.PageToken(a)[0]
		}
	}
	if !found {
		t.Fatal("history copied payload")
	}
	if h.add(RestorePageState(nil)) != nil || !errors.Is(h.add(RestorePageState([]byte{})), ErrPagingNoProgress) {
		t.Fatal("empty cycle")
	}
	if !errors.Is(h.add(RestorePageState([]byte("long"))), ErrPagingStateLimit) || h.count != 3 {
		t.Fatal("byte limit")
	}
	h = pagingHistory{maxCount: 1, maxBytes: 100}
	if h.add(a) != nil || !errors.Is(h.add(b), ErrPagingStateLimit) {
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
				rt := s.typedRuntime()
				rt.config.Limits.MaxPagingStates = 2
				rt.config.Limits.MaxPagingStateBytes = 2
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
					rows.history.maxCount = 1
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
	entry := &typedResultMetadata{columns: cols}
	cache := &resultPlanCache[Bytes]{}
	var calls atomic.Int32
	d := step03CountingDecoder{calls: &calls}
	for i := 0; i < 100; i++ {
		if _, err := cache.selectPlan(&responsePage{metadata: entry, responseData: responseData{Columns: cols}}, d); err != nil {
			t.Fatal(err)
		}
	}
	if calls.Load() != 1 {
		t.Fatal("recompiled exact entry", calls.Load())
	}
	equivalent := NewColumns(cqltype.ColumnSlice(cols)...)
	if _, err := cache.selectPlan(&responsePage{responseData: responseData{Columns: equivalent}}, d); err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 1 {
		t.Fatal("recompiled equal shape")
	}
	another := &resultPlanCache[Bytes]{}
	_, _ = another.selectPlan(&responsePage{metadata: entry, responseData: responseData{Columns: cols}}, d)
	if calls.Load() != 2 {
		t.Fatal("shared different decoder cache")
	}
	cold := &resultPlanCache[Bytes]{}
	entered, done := make(chan struct{}), make(chan struct{})
	d.hook = func() { close(entered); <-done }
	completed := make(chan error)
	go func() {
		_, err := cold.selectPlan(&responsePage{metadata: entry, responseData: responseData{Columns: cols}}, d)
		completed <- err
	}()
	<-entered
	cold.mu.Lock()
	cold.mu.Unlock()
	close(done)
	if err := <-completed; err != nil {
		t.Fatal(err)
	}
}
func step03PreparedState(t *testing.T, s *Session, columns Columns) (*preparedState, *hostPrepared) {
	t.Helper()
	tracker := s.typedRuntime().schemaTracker()
	dependencies := tracker.dependencies("ks", NewColumns(), columns)
	epoch := dependencies.snapshot()
	result := &typedResultMetadata{columns: columns, epoch: epoch, tracker: tracker}
	hp := &hostPrepared{id: []byte("id"), metadataID: []byte("meta"), columns: columns, result: result, epoch: epoch, revision: 1}
	state := &preparedState{session: s, protocol: 4, statement: "SELECT value FROM ks.tbl", keyspace: "ks", parameters: NewColumns(), columns: columns, result: result, dependencies: dependencies, initialEpoch: epoch, resultHost: UUID{15: 1}, info: PreparedInfo{Keyspace: "ks", Table: "tbl"}, hosts: map[UUID]*hostPrepared{{15: 1}: hp}}
	return state, hp
}
func TestV2PreparedResponseRetainsMetadataAcrossSchemaChange(t *testing.T) {
	s := &Session{logger: nopLogger{}}
	columns := NewColumns(ColumnSpec{Keyspace: "ks", Table: "tbl", Name: "value", Type: codecTestMust(ScalarType(TypeBlob))})
	state, hp := step03PreparedState(t, s, columns)
	s.invalidatePreparedSchema(Event{Keyspace: "other", Object: "other", Target: "TABLE"})
	if !hp.epoch.current() {
		t.Fatal("unrelated keyspace invalidated")
	}
	s.invalidatePreparedSchema(Event{Keyspace: "ks", Object: "other", Target: "TABLE"})
	if !hp.epoch.current() {
		t.Fatal("unrelated table invalidated")
	}
	s.invalidatePreparedSchema(Event{Keyspace: "ks", Object: "tbl", Target: "TABLE"})
	if hp.epoch.current() {
		t.Fatal("related table not invalidated")
	}
	response := &responsePage{responseData: responseData{NoMetadata: true, ColumnCount: 1}}
	if err := selectPreparedResponse(state, UUID{15: 1}, hp, hp.metadataID, true, response); err != nil || response.metadata != hp.result || !response.Columns.Equal(columns) {
		t.Fatal("retained response rejected", err)
	}
	for _, bad := range []*responsePage{{responseData: responseData{NoMetadata: true, ColumnCount: 2}}, {responseData: responseData{NoMetadata: true, ColumnCount: 1, MetadataID: []byte("bad")}}} {
		if selectPreparedResponse(state, UUID{15: 1}, hp, hp.metadataID, true, bad) == nil {
			t.Fatal("mismatched metadata accepted")
		}
	}
}
func TestV2PreparedPublicationExactEntryEpochAndOpaqueIDs(t *testing.T) {
	s := &Session{logger: nopLogger{}}
	blob := codecTestMust(ScalarType(TypeBlob))
	columns := NewColumns(ColumnSpec{Keyspace: "ks", Table: "tbl", Name: "value", Type: blob})
	state, old := step03PreparedState(t, s, columns)
	id := UUID{15: 1}
	newer := *old
	newer.revision++
	newer.metadataID = []byte("a")
	state.hosts[id] = &newer
	response := &responsePage{responseData: responseData{Kind: 2, Columns: columns, MetadataID: []byte("z")}}
	if err := selectPreparedResponse(state, id, old, old.metadataID, true, response); err != nil {
		t.Fatal(err)
	}
	if state.hosts[id] != &newer {
		t.Fatal("stale response replaced newer entry")
	}
	response = &responsePage{responseData: responseData{Kind: 2, Columns: columns, MetadataID: []byte("0")}}
	if err := selectPreparedResponse(state, id, &newer, newer.metadataID, true, response); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(state.hosts[id].metadataID, []byte("0")) {
		t.Fatal("opaque IDs treated as ordered")
	}
	current := state.hosts[id]
	s.invalidatePreparedSchema(Event{Keyspace: "ks", Object: "tbl", Target: "TABLE"})
	response = &responsePage{responseData: responseData{Kind: 2, Columns: columns, MetadataID: []byte("later")}}
	_ = selectPreparedResponse(state, id, current, current.metadataID, true, response)
	if state.hosts[id] != current {
		t.Fatal("stale schema publication")
	}
}
func TestV2PreparedDependencyTypesAndSnapshotChanges(t *testing.T) {
	s := &Session{logger: nopLogger{}}
	integer := codecTestMust(ScalarType(TypeInt))
	udt := codecTestMust(UDTType("ks", "wanted", ColumnSpec{Name: "n", Type: integer}))
	columns := NewColumns(ColumnSpec{Keyspace: "ks", Table: "tbl", Name: "v", Type: udt})
	_, hp := step03PreparedState(t, s, columns)
	s.invalidatePreparedSchema(Event{Keyspace: "ks", Object: "unrelated", Target: "TYPE"})
	if !hp.epoch.current() {
		t.Fatal("unrelated UDT invalidated")
	}
	s.invalidatePreparedSchema(Event{Keyspace: "ks", Object: "wanted", Target: "TYPE"})
	if hp.epoch.current() {
		t.Fatal("UDT dependency ignored")
	}
	tracker := s.typedRuntime().schemaTracker()
	d := tracker.dependencies("ks", NewColumns(), columns)
	stamp := d.snapshot()
	old := &MetadataSnapshot{partitioner: "p", keyspaces: map[string]*KeyspaceMetadata{"ks": {Name: "ks", Tables: map[string]*TableMetadata{"tbl": {Name: "tbl", Keyspace: "ks"}}}}}
	next := &MetadataSnapshot{partitioner: "p", keyspaces: map[string]*KeyspaceMetadata{"ks": cloneV2Keyspace(old.keyspaces["ks"])}}
	s.updatePreparedSchemaSnapshot(old, next)
	if !stamp.current() {
		t.Fatal("unchanged refresh invalidated")
	}
	next.keyspaces["ks"].Tables["other"] = &TableMetadata{Keyspace: "ks", Name: "other"}
	s.updatePreparedSchemaSnapshot(old, next)
	if !stamp.current() {
		t.Fatal("unrelated refresh invalidated")
	}
	delete(next.keyspaces["ks"].Tables, "tbl")
	s.updatePreparedSchemaSnapshot(old, next)
	if stamp.current() {
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
				_, e := s.prepareOnConnExpected(ctx, conn, state, encodedValues{Columns: NewColumns()}, true, old)
				done <- e
			}()
			<-entered
			second := make(chan error)
			go func() {
				_, e := s.prepareOnConnExpected(context.Background(), conn, state, encodedValues{Columns: NewColumns()}, true, old)
				second <- e
			}()
			deadline := time.Now().Add(time.Second)
			for {
				state.mu.RLock()
				flight := state.preparing[conn.host.hostUUID()]
				waiters := int32(0)
				if flight != nil {
					waiters = flight.waiters.Load()
				}
				state.mu.RUnlock()
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
				if hp := state.hosts[conn.host.hostUUID()]; hp == old || !bytes.Equal(hp.id, []byte("new")) {
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
	rt := s.typedRuntime()
	rt.mu.Lock()
	done := make(chan error, 1)
	go func() {
		got, e := s.prepareOnConn(context.Background(), conns[0], state, encodedValues{}, false)
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
		rt.mu.Unlock()
		t.Fatal("warm execution used session discovery lock")
	}
	rt.mu.Unlock()
}

func TestV2PreparedMetadataIntroducesUDTDependency(t *testing.T) {
	s := &Session{logger: nopLogger{}}
	columns := NewColumns(ColumnSpec{Keyspace: "ks", Table: "tbl", Name: "value", Type: codecTestMust(ScalarType(TypeBlob))})
	state, old := step03PreparedState(t, s, columns)
	udt := codecTestMust(UDTType("ks", "introduced", ColumnSpec{Name: "n", Type: codecTestMust(ScalarType(TypeInt))}))
	changed := NewColumns(ColumnSpec{Keyspace: "ks", Table: "tbl", Name: "value", Type: udt})
	response := &responsePage{responseData: responseData{Kind: 2, Columns: changed, MetadataID: []byte("next")}}
	if err := selectPreparedResponse(state, UUID{15: 1}, old, old.metadataID, true, response, state.dependencies.tracker.start()); err != nil {
		t.Fatal(err)
	}
	current := state.hosts[UUID{15: 1}]
	if current == old || !current.epoch.current() {
		t.Fatal("changed result not installed")
	}
	s.invalidatePreparedSchema(Event{Keyspace: "ks", Target: "TYPE", Object: "introduced"})
	if current.epoch.current() || state.cacheCurrent() {
		t.Fatal("new UDT dependency was not registered")
	}
}
func TestV2PreparedNewDependencyEventDuringAttemptFencesPublication(t *testing.T) {
	s := &Session{logger: nopLogger{}}
	columns := NewColumns(ColumnSpec{Keyspace: "ks", Table: "tbl", Name: "value", Type: codecTestMust(ScalarType(TypeBlob))})
	state, old := step03PreparedState(t, s, columns)
	started := state.dependencies.tracker.start()
	s.invalidatePreparedSchema(Event{Keyspace: "ks", Target: "TYPE", Object: "introduced"})
	udt := codecTestMust(UDTType("ks", "introduced", ColumnSpec{Name: "n", Type: codecTestMust(ScalarType(TypeInt))}))
	changed := NewColumns(ColumnSpec{Keyspace: "ks", Table: "tbl", Name: "value", Type: udt})
	response := &responsePage{responseData: responseData{Kind: 2, Columns: changed, MetadataID: []byte("stale")}}
	if err := selectPreparedResponse(state, UUID{15: 1}, old, old.metadataID, true, response, started); err != nil {
		t.Fatal(err)
	}
	if state.hosts[UUID{15: 1}] != old {
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
	s.cfg.Keyspace = "ks"
	typ := codecTestMust(ScalarType(TypeBlob))
	columns := NewColumns(ColumnSpec{Keyspace: "ks", Table: "tbl", Name: "value", Type: typ})
	state, old := step03PreparedState(t, s, columns)
	state.parameters = columns
	state.info.PartitionKeyIndexes = []int{0}
	count := 0
	params := codecTestMust(OneParam[[]byte](countingBlobEncoder{calls: &count}).Compile(columns))
	handle := &Prepared[[]byte, Bytes]{state: state, params: params, rows: OneColumn(BlobCodec), plans: &resultPlanCache[Bytes]{}}
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
					_, err = peers[0].Write(step03ErrorResponse(t, stream, ErrCodeUnprepared, old.id))
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
			state.parameters = parameters
			state.info.PartitionKeyIndexes = []int{0, 1}
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
				state.partitioner = "different"
			case "conditional":
				state.info.Conditional = true
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
			_, err := s.prepareOnConnExpected(ctx, conns[0], state, encodedValues{Columns: parameters}, true, old)
			var changed *SchemaChangedError
			if !errors.As(err, &changed) {
				t.Fatal(mode, err)
			}
			if state.hosts[UUID{15: 1}] != old {
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
		_, err := s.prepareOnConnExpected(ctx, conns[0], state, encodedValues{Columns: NewColumns()}, true, old)
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
	state.mu.RLock()
	pending := len(state.preparing)
	state.mu.RUnlock()
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
		_, err := s.prepareOnConnExpected(context.Background(), conns[0], state, encodedValues{Columns: NewColumns()}, true, old)
		first <- err
	}()
	<-firstRead
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	entry, err := s.prepareOnConn(ctx, conns[1], state, encodedValues{Columns: NewColumns()}, false)
	if err != nil || !bytes.Equal(entry.id, []byte("second")) {
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
				entry, err := s.prepareOnConnExpected(context.Background(), conns[0], state, encodedValues{Columns: NewColumns()}, true, old)
				result <- entry
				failure <- err
			}()
			<-received
			expected := old
			if change == "entry" {
				replacement := *old
				replacement.id = []byte("current")
				expected = &replacement
				state.mu.Lock()
				state.hosts[UUID{15: 1}] = expected
				state.mu.Unlock()
			} else {
				s.invalidatePreparedSchema(Event{Keyspace: "ks", Target: "TABLE", Object: "tbl"})
			}
			close(release)
			entry := <-result
			if err := <-failure; err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(entry.id, []byte("late")) {
				t.Fatal("losing response unavailable")
			}
			state.mu.RLock()
			actual := state.hosts[UUID{15: 1}]
			state.mu.RUnlock()
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
	h := pagingHistory{maxCount: 4, maxBytes: 8}
	_ = h.add(b)
	hash := maphash.Bytes(h.seed, model.PageToken(b))
	h.entries[hash] = pagingHistoryEntry{state: a}
	h.count = 1
	h.bytes = 1
	if err := h.add(b); err != nil {
		t.Fatal("hash collision mistaken for equality", err)
	}
	if h.count != 2 || h.entries[hash].next == nil {
		t.Fatal("collision entry lost")
	}
	if !errors.Is(h.add(b), ErrPagingNoProgress) {
		t.Fatal("exact duplicate lost")
	}
}
func TestV2ManualPageEmptyContinuationSurvivesClose(t *testing.T) {
	p := step03Page(t)
	p.response.State = RestorePageState(nil)
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
	s := &Session{logger: nopLogger{}}
	columns := NewColumns(ColumnSpec{Keyspace: "ks", Table: "tbl", Name: "value", Type: codecTestMust(ScalarType(TypeBlob))})
	state, old := step03PreparedState(t, s, columns)
	host := UUID{15: 1}
	changed := NewColumns(ColumnSpec{Keyspace: "ks", Table: "tbl", Name: "value", Type: codecTestMust(ScalarType(TypeInt))})
	replacement := &responsePage{responseData: responseData{Kind: 2, Columns: changed, MetadataID: []byte("new")}}
	if err := selectPreparedResponse(state, host, old, old.metadataID, true, replacement); err != nil {
		t.Fatal(err)
	}
	chain := typedTestChain(t, []byte{0, 0, 0, 3, 'o', 'l', 'd'})
	response := &responsePage{responseData: responseData{NoMetadata: true, ColumnCount: 1, RowCount: 1, Rows: chain, Owner: wire.NewLease(chain, nil)}}
	if err := selectPreparedResponse(state, host, old, old.metadataID, true, response); err != nil {
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
	s := &Session{logger: nopLogger{}}
	columns := NewColumns(ColumnSpec{Keyspace: "ks", Table: "tbl", Name: "value", Type: codecTestMust(ScalarType(TypeBlob))})
	_, hp := step03PreparedState(t, s, columns)
	var calls atomic.Int32
	decoder := step03CountingDecoder{calls: &calls, hook: func() { s.invalidatePreparedSchema(Event{Keyspace: "ks", Target: "TABLE", Object: "tbl"}) }}
	cache := &resultPlanCache[Bytes]{}
	response := &responsePage{metadata: hp.result, responseData: responseData{Columns: columns}}
	if _, err := cache.selectPlan(response, decoder); err != nil {
		t.Fatal(err)
	}
	cache.mu.Lock()
	cached := cache.plan
	cache.mu.Unlock()
	if cached != nil {
		t.Fatal("compile published through changed schema epoch")
	}
}

func TestV2PagerCompletionCanInspectIterator(t *testing.T) {
	p := step03Page(t)
	pageObserved := false
	p.release = func() { _ = p.Err(); _ = p.Close(); pageObserved = true }
	p.Close()
	if !pageObserved {
		t.Fatal("page completion missing")
	}
	rows := &Rows[Bytes]{page: step03Page(t), ctx: context.Background()}
	rowsObserved := false
	rows.release = func() { _ = rows.Err(); _ = rows.Info(); _ = rows.Close(); rowsObserved = true }
	rows.Close()
	if !rowsObserved {
		t.Fatal("rows completion missing")
	}
}

func TestV2PagerConcurrentCloseReleasesExactlyOnce(t *testing.T) {
	p := step03Page(t, "value")
	var releases atomic.Int32
	p.release = func() { releases.Add(1); _ = p.Err(); _ = p.Close() }
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); _ = p.Close() }()
	}
	wg.Wait()
	if releases.Load() != 1 {
		t.Fatal("page release count", releases.Load())
	}
	rows := &Rows[Bytes]{page: step03Page(t), ctx: context.Background()}
	releases.Store(0)
	rows.release = func() { releases.Add(1); _ = rows.Info(); _ = rows.Close() }
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
	s := &Session{logger: nopLogger{}}
	s.ctx, s.cancel = context.WithCancel(context.Background())
	defer s.Close()
	columns := NewColumns(ColumnSpec{Keyspace: "ks", Table: "tbl", Name: "value", Type: codecTestMust(ScalarType(TypeBlob))})
	state, old := step03PreparedState(t, s, columns)
	host := UUID{15: 2}
	second := *old
	state.hosts[host] = &second
	changed := NewColumns(ColumnSpec{Keyspace: "ks", Table: "tbl", Name: "value", Type: codecTestMust(ScalarType(TypeInt))})
	for i := 0; i < 100; i++ {
		response := &responsePage{responseData: responseData{Kind: 2, Columns: changed, MetadataID: []byte("conflict")}}
		if err := selectPreparedResponse(state, host, &second, second.metadataID, true, response); err != nil {
			t.Fatal(err)
		}
		if !response.Columns.Equal(changed) {
			t.Fatal("matching response metadata lost")
		}
	}
	if !state.conflictRefresh.Load() || !s.peripheral().retryScheduled.Load() {
		t.Fatal("conflict not scheduled")
	}
	state.mu.RLock()
	latest := state.result
	state.mu.RUnlock()
	if latest != old.result {
		t.Fatal("remote order guessed")
	}
	if old.epoch.current() {
		t.Fatal("conflicting cache entries not invalidated")
	}
}

func TestV2ResultPlanPublicationSharesEpochGate(t *testing.T) {
	s := &Session{logger: nopLogger{}}
	columns := NewColumns(ColumnSpec{Keyspace: "ks", Table: "tbl", Name: "value", Type: codecTestMust(ScalarType(TypeBlob))})
	_, hp := step03PreparedState(t, s, columns)
	tracker := hp.result.tracker
	entered, release := make(chan struct{}), make(chan struct{})
	var calls atomic.Int32
	decoder := step03CountingDecoder{calls: &calls, hook: func() {
		if !tracker.mu.TryLock() {
			t.Error("user Compile ran under epoch gate")
		} else {
			tracker.mu.Unlock()
		}
		close(entered)
		<-release
	}}
	cache := &resultPlanCache[Bytes]{}
	done := make(chan error, 1)
	go func() {
		_, err := cache.selectPlan(&responsePage{metadata: hp.result, responseData: responseData{Columns: columns}}, decoder)
		done <- err
	}()
	<-entered
	cache.mu.Lock()
	close(release)
	deadline := time.Now().Add(time.Second)
	for tracker.mu.TryLock() {
		tracker.mu.Unlock()
		if time.Now().After(deadline) {
			cache.mu.Unlock()
			t.Fatal("cache publication did not acquire epoch gate")
		}
		runtime.Gosched()
	}
	invalidating, invalidated := make(chan struct{}), make(chan struct{})
	go func() { close(invalidating); tracker.bump(preparedDependencyKey{'b', "ks", "tbl"}); close(invalidated) }()
	<-invalidating
	select {
	case <-invalidated:
		cache.mu.Unlock()
		t.Fatal("invalidation interleaved with publication")
	default:
	}
	cache.mu.Unlock()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	<-invalidated
	if hp.epoch.current() {
		t.Fatal("invalidation was lost")
	}
}
func TestV2InitialPrepareDoesNotCacheChangedEpoch(t *testing.T) {
	s, conns, peers := runtimeTestSession(t, 1)
	s.cfg.Keyspace = "ks"
	columns := NewColumns(ColumnSpec{Keyspace: "ks", Table: "tbl", Name: "value", Type: codecTestMust(ScalarType(TypeBlob))})
	serverDone := make(chan error, 1)
	go func() {
		_, stream, _, err := step03ReadRequest(peers[0])
		if err == nil {
			s.invalidatePreparedSchema(Event{Keyspace: "ks", Target: "TABLE", Object: "tbl"})
			_, err = peers[0].Write(typedTestResponse(t, 4, stream, step03PreparedBody([]byte("old"), NewColumns(), columns)))
		}
		serverDone <- err
	}()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	state, err := s.prepareTyped(ctx, "SELECT value FROM ks.tbl", "ks", PrepareOptions{connection: conns[0]})
	if err != nil {
		t.Fatal(err)
	}
	if state.cacheCurrent() {
		t.Fatal("prepare crossing relevant epoch accepted as current")
	}
	rt := s.typedRuntime()
	rt.mu.Lock()
	count := len(rt.prepared)
	rt.mu.Unlock()
	if count != 0 {
		t.Fatal("stale initial prepare published")
	}
	if err := <-serverDone; err != nil {
		t.Fatal(err)
	}
}

func TestV2StalePrepareFailurePreservesDiscoveryBound(t *testing.T) {
	s, conns, peers := runtimeTestSession(t, 1)
	s.cfg.Keyspace = "ks"
	cols := NewColumns(ColumnSpec{Keyspace: "ks", Table: "tbl", Name: "value", Type: codecTestMust(ScalarType(TypeBlob))})
	state, _ := step03PreparedState(t, s, cols)
	rt := s.typedRuntime()
	rt.config.PreparedCacheSize = 1
	key := prepareCacheKey{cql: "A", keyspace: "ks"}
	rt.prepared[key] = state
	rt.prepareOrder = []prepareCacheKey{key}
	s.invalidatePreparedSchema(Event{Keyspace: "ks", Target: "TABLE", Object: "tbl"})
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
	options := PrepareOptions{connection: conns[0]}
	if _, err := s.prepareTyped(ctx, "A", "ks", options); err == nil {
		t.Fatal("failed prepare accepted")
	}
	rt.mu.Lock()
	staleOrder := len(rt.prepareOrder)
	rt.mu.Unlock()
	if staleOrder != 0 {
		t.Fatal("stale failed entry retained LRU record")
	}
	if _, err := s.prepareTyped(ctx, "B", "ks", options); err != nil {
		t.Fatal(err)
	}
	if _, err := s.prepareTyped(ctx, "C", "ks", options); err != nil {
		t.Fatal(err)
	}
	rt.mu.Lock()
	count, order := len(rt.prepared), len(rt.prepareOrder)
	_, latest := rt.prepared[prepareCacheKey{cql: "C", keyspace: "ks"}]
	rt.mu.Unlock()
	if count != 1 || order != 1 || !latest {
		t.Fatal("discovery cache exceeded bound", count, order)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestV2DiscoveryOrderRemovesAllDuplicates(t *testing.T) {
	a, b := prepareCacheKey{cql: "a"}, prepareCacheKey{cql: "b"}
	rt := &sessionRuntime{prepareOrder: []prepareCacheKey{a, b, a}}
	rt.touchPrepare(a)
	if len(rt.prepareOrder) != 2 || rt.prepareOrder[0] != b || rt.prepareOrder[1] != a {
		t.Fatal("duplicate LRU order", rt.prepareOrder)
	}
	rt.forgetPrepareOrder(a)
	if len(rt.prepareOrder) != 1 || rt.prepareOrder[0] != b {
		t.Fatal("forget did not remove key")
	}
}
func TestV2DiscoveryCapacityZeroAndConcurrentMisses(t *testing.T) {
	for _, capacity := range []int{0, 1} {
		t.Run(fmt.Sprint(capacity), func(t *testing.T) {
			s, conns, peers := runtimeTestSession(t, 1)
			s.cfg.Keyspace = "ks"
			rt := s.typedRuntime()
			rt.profile = DefaultConfig("localhost").DefaultProfile
			rt.config.PreparedCacheSize = capacity
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
					_, err := s.prepareTyped(ctx, cql, "ks", PrepareOptions{connection: conns[0]})
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
			rt.mu.Lock()
			count, order := len(rt.prepared), len(rt.prepareOrder)
			rt.mu.Unlock()
			if count != capacity || order != capacity {
				t.Fatal("concurrent discovery exceeded capacity", count, order, capacity)
			}
		})
	}
}
