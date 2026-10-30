//go:build unit

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
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type step03ScopedSchemaBackend struct {
	*schemaDataMock
	keyspace string
	fail     atomic.Bool
	queried  chan struct{}
	once     sync.Once
	calls    atomic.Int32
}

func (b *step03ScopedSchemaBackend) queryV2System(ctx context.Context, r *attemptRequest) (*responsePage, error) {
	args, err := v2SchemaMockArguments(r)
	if err != nil {
		return nil, err
	}
	scopedDescribe := len(args) == 0 && r.Statement == `DESCRIBE KEYSPACE "`+b.keyspace+`" WITH INTERNALS`
	if !scopedDescribe && (len(args) != 1 || args[0] != b.keyspace) {
		return nil, fmt.Errorf("unrelated or unscoped required schema query: %s %v", r.Statement, args)
	}
	if strings.Contains(r.Statement, "FROM system_schema.keyspaces") {
		b.calls.Add(1)
		b.once.Do(func() { close(b.queried) })
		if b.fail.CompareAndSwap(true, false) {
			return nil, errors.New("injected temporary required refresh failure")
		}
	}
	return b.schemaDataMock.queryV2System(ctx, r)
}
func waitStep03Reconciliation(t *testing.T, p *v2PeripheralState) {
	t.Helper()
	deadline := time.Now().Add(4 * time.Second)
	for {
		p.mu.Lock()
		pending := len(p.preparedRefreshScopes)
		p.mu.Unlock()
		if pending == 0 && !p.retryScheduled.Load() {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("required reconciliation did not finish")
		}
		time.Sleep(time.Millisecond)
	}
}
func TestV2PreparedConflictRunsOnlyRelevantTypedRefresh(t *testing.T) {
	for _, mode := range []string{"disabled", "excluded", "minimal"} {
		t.Run(mode, func(t *testing.T) {
			s, mock := refreshAllSchemaSession(t)
			p := s.peripheral()
			p.mu.Lock()
			p.config = DefaultConfig("localhost")
			if mode == "disabled" {
				p.config.Metadata.Mode = MetadataDisabled
			} else if mode == "excluded" {
				p.config.Metadata.Keyspaces = []string{"ks2"}
			} else {
				p.config.Metadata.Mode = MetadataMinimal
			}
			p.mu.Unlock()
			before := s.Metadata()
			backend := &step03ScopedSchemaBackend{schemaDataMock: mock, keyspace: "ks1", queried: make(chan struct{})}
			s.control = backend
			mock.mu.Lock()
			mock.knownKeyspaces["ks1"] = append(mock.knownKeyspaces["ks1"], tableInfo{name: "from_conflict", columns: []columnInfo{{name: "id", kind: "partition_key"}}})
			mock.mu.Unlock()
			state := &preparedState{session: s, info: PreparedInfo{Keyspace: "ks1", Table: "t1"}}
			for i := 0; i < 1000; i++ {
				if err := state.noteMetadataConflict(); err != nil {
					t.Fatal(err)
				}
			}
			select {
			case <-backend.queried:
			case <-time.After(3 * time.Second):
				t.Fatal("required relevant refresh did not query typed source")
			}
			waitStep03Reconciliation(t, p)
			if backend.calls.Load() != 1 {
				t.Fatal("required refresh not coalesced", backend.calls.Load())
			}
			if after := s.Metadata(); after.keyspaces["ks1"].Tables["from_conflict"] != nil || mode != "minimal" && after != before || mode == "minimal" && len(after.keyspaces["ks1"].Tables) != 0 {
				t.Fatal("required refresh broadened public metadata")
			}
			if err := state.reconcilePendingConflict(); err != nil || state.conflictDirty.Load() {
				t.Fatal("successful reconciliation left conflict dirty", err)
			}
		})
	}
}
func TestV2PreparedConflictRefreshRetriesDirtyScope(t *testing.T) {
	s, mock := refreshAllSchemaSession(t)
	backend := &step03ScopedSchemaBackend{schemaDataMock: mock, keyspace: "ks1", queried: make(chan struct{})}
	backend.fail.Store(true)
	s.control = backend
	p := s.peripheral()
	p.mu.Lock()
	p.config = DefaultConfig("localhost")
	p.config.Metadata.Mode = MetadataDisabled
	p.mu.Unlock()
	state := &preparedState{session: s, info: PreparedInfo{Keyspace: "ks1", Table: "t1"}}
	if err := state.noteMetadataConflict(); err != nil {
		t.Fatal(err)
	}
	<-backend.queried
	p.mu.Lock()
	_, dirty := p.preparedRefreshScopes["ks1"]
	p.mu.Unlock()
	if !dirty {
		t.Fatal("failed scope was dropped")
	}
	waitStep03Reconciliation(t, p)
	if backend.calls.Load() != 2 {
		t.Fatal("failed required scope did not retry", backend.calls.Load())
	}
}
func TestV2PreparedConflictOverflowPropagatesAndRetries(t *testing.T) {
	s := &Session{logger: nopLogger{}}
	s.ctx, s.cancel = context.WithCancel(context.Background())
	defer s.Close()
	cols := NewColumns(ColumnSpec{Keyspace: "ks", Table: "tbl", Name: "value", Type: codecTestMust(ScalarType(TypeBlob))})
	state, old := step03PreparedState(t, s, cols)
	host := UUID{15: 2}
	other := *old
	state.hosts[host] = &other
	page := step03Page(t, "retained")
	defer page.Close()
	if !page.Next() {
		t.Fatal(page.Err())
	}
	retained := codecTestMust(page.Retain())
	defer retained.Close()
	view := page.Row()
	p := s.peripheral()
	p.mu.Lock()
	p.preparedRefreshScopes = make(map[string]uint64)
	for i := 0; i < maxPreparedRefreshScopes; i++ {
		p.preparedRefreshScopes[fmt.Sprint(i)] = 1
	}
	p.mu.Unlock()
	changed := NewColumns(ColumnSpec{Keyspace: "ks", Table: "tbl", Name: "value", Type: codecTestMust(ScalarType(TypeInt))})
	response := &responsePage{responseData: responseData{Kind: 2, Columns: changed, MetadataID: []byte("conflict")}}
	err := selectPreparedResponse(state, host, &other, other.metadataID, true, response)
	var protocol *ProtocolError
	if !errors.As(err, &protocol) || protocol.Stage != "prepared schema reconciliation" {
		t.Fatal("overflow disappeared from logical result", err)
	}
	if !state.conflictDirty.Load() || state.conflictEpoch.Load() != 0 {
		t.Fatal("unscheduled conflict not left dirty")
	}
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := s.prepareOnConnExpected(cancelled, nil, state, encodedValues{}, false, nil); !errors.Is(err, context.Canceled) {
		t.Fatal("canceled waiter performed reconciliation work", err)
	}
	if err := state.reconcilePendingConflict(); !errors.As(err, &protocol) {
		t.Fatal("future operation silently bypassed unresolved conflict", err)
	}
	if string(view.OwnedBytes()) != "retained" {
		t.Fatal("earlier delivered page invalidated")
	}
	p.mu.Lock()
	delete(p.preparedRefreshScopes, "0")
	p.mu.Unlock()
	if err := state.reconcilePendingConflict(); err != nil {
		t.Fatal("available scope did not retry", err)
	}
	p.mu.Lock()
	_, queued := p.preparedRefreshScopes["ks"]
	p.mu.Unlock()
	if !queued || state.conflictEpoch.Load() == 0 {
		t.Fatal("retry was not registered")
	}
}
func TestV2PreparedConflictCanRescheduleAfterReconciliation(t *testing.T) {
	s := &Session{logger: nopLogger{}}
	s.ctx, s.cancel = context.WithCancel(context.Background())
	defer s.Close()
	state := &preparedState{session: s, info: PreparedInfo{Keyspace: "ks", Table: "tbl"}}
	if err := state.noteMetadataConflict(); err != nil {
		t.Fatal(err)
	}
	first := state.conflictEpoch.Load()
	if err := state.noteMetadataConflict(); err != nil || state.conflictEpoch.Load() != first {
		t.Fatal("same conflict not coalesced")
	}
	p := s.peripheral()
	p.mu.Lock()
	delete(p.preparedRefreshScopes, "ks")
	p.schemaReconciled.Add(1)
	p.mu.Unlock()
	if err := state.noteMetadataConflict(); err != nil || state.conflictEpoch.Load() == first {
		t.Fatal("future conflicts permanently suppressed", err)
	}
}
