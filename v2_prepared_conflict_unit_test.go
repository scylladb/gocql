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

import implCluster "github.com/scylladb/gocql/v2/internal/cluster"

import (
	routing "github.com/scylladb/gocql/v2/internal/routing"

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

func (b *step03ScopedSchemaBackend) InternalQueryV2System(ctx context.Context, r *attemptRequest) (*responsePage, error) {
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
	return b.schemaDataMock.InternalQueryV2System(ctx, r)
}
func waitStep03Reconciliation(t *testing.T, p *v2PeripheralState) {
	t.Helper()
	deadline := time.Now().Add(4 * time.Second)
	for {
		p.InternalMu.Lock()
		pending := len(p.InternalPreparedRefreshScopes)
		p.InternalMu.Unlock()
		if pending == 0 && !p.InternalRetryScheduled.Load() {
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
			p := implCluster.InternalSessionPeripheral(s)
			p.InternalMu.Lock()
			p.InternalConfig = DefaultConfig("localhost")
			if mode == "disabled" {
				p.InternalConfig.Metadata.Mode = MetadataDisabled
			} else if mode == "excluded" {
				p.InternalConfig.Metadata.Keyspaces = []string{"ks2"}
			} else {
				p.InternalConfig.Metadata.Mode = MetadataMinimal
			}
			p.InternalMu.Unlock()
			before := s.Metadata()
			backend := &step03ScopedSchemaBackend{schemaDataMock: mock, keyspace: "ks1", queried: make(chan struct{})}
			implCluster.SessionData(s).InternalControl = backend
			mock.mu.Lock()
			mock.knownKeyspaces["ks1"] = append(mock.knownKeyspaces["ks1"], tableInfo{name: "from_conflict", columns: []columnInfo{{name: "id", kind: "partition_key"}}})
			mock.mu.Unlock()
			state := &preparedState{InternalSession: s, InternalInfo: PreparedInfo{Keyspace: "ks1", Table: "t1"}}
			for i := 0; i < 1000; i++ {
				if err := state.InternalNoteMetadataConflict(); err != nil {
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
			if after := s.Metadata(); routing.SnapshotData(after).InternalKeyspaces["ks1"].Tables["from_conflict"] != nil || mode != "minimal" && after != before || mode == "minimal" && len(routing.SnapshotData(after).InternalKeyspaces["ks1"].Tables) != 0 {
				t.Fatal("required refresh broadened public metadata")
			}
			if err := state.InternalReconcilePendingConflict(); err != nil || state.InternalConflictDirty.Load() {
				t.Fatal("successful reconciliation left conflict dirty", err)
			}
		})
	}
}
func TestV2PreparedConflictRefreshRetriesDirtyScope(t *testing.T) {
	s, mock := refreshAllSchemaSession(t)
	backend := &step03ScopedSchemaBackend{schemaDataMock: mock, keyspace: "ks1", queried: make(chan struct{})}
	backend.fail.Store(true)
	implCluster.SessionData(s).InternalControl = backend
	p := implCluster.InternalSessionPeripheral(s)
	p.InternalMu.Lock()
	p.InternalConfig = DefaultConfig("localhost")
	p.InternalConfig.Metadata.Mode = MetadataDisabled
	p.InternalMu.Unlock()
	state := &preparedState{InternalSession: s, InternalInfo: PreparedInfo{Keyspace: "ks1", Table: "t1"}}
	if err := state.InternalNoteMetadataConflict(); err != nil {
		t.Fatal(err)
	}
	<-backend.queried
	p.InternalMu.Lock()
	_, dirty := p.InternalPreparedRefreshScopes["ks1"]
	p.InternalMu.Unlock()
	if !dirty {
		t.Fatal("failed scope was dropped")
	}
	waitStep03Reconciliation(t, p)
	if backend.calls.Load() != 2 {
		t.Fatal("failed required scope did not retry", backend.calls.Load())
	}
}
func TestV2PreparedConflictOverflowPropagatesAndRetries(t *testing.T) {
	s := func() *Session {
		opaqueStorageValue := new(Session)
		implCluster.SessionData(opaqueStorageValue).InternalLogger = nopLogger{}
		return opaqueStorageValue
	}()
	implCluster.SessionData(s).InternalCtx, implCluster.SessionData(s).InternalCancel = context.WithCancel(context.Background())
	defer s.Close()
	cols := NewColumns(ColumnSpec{Keyspace: "ks", Table: "tbl", Name: "value", Type: codecTestMust(ScalarType(TypeBlob))})
	state, old := step03PreparedState(t, s, cols)
	host := UUID{15: 2}
	other := *old
	state.InternalHosts[host] = &other
	page := step03Page(t, "retained")
	defer page.Close()
	if !page.Next() {
		t.Fatal(page.Err())
	}
	retained := codecTestMust(page.Retain())
	defer retained.Close()
	view := page.Row()
	p := implCluster.InternalSessionPeripheral(s)
	p.InternalMu.Lock()
	p.InternalPreparedRefreshScopes = make(map[string]uint64)
	for i := 0; i < maxPreparedRefreshScopes; i++ {
		p.InternalPreparedRefreshScopes[fmt.Sprint(i)] = 1
	}
	p.InternalMu.Unlock()
	changed := NewColumns(ColumnSpec{Keyspace: "ks", Table: "tbl", Name: "value", Type: codecTestMust(ScalarType(TypeInt))})
	response := &responsePage{InternalResponseData: responseData{Kind: 2, Columns: changed, MetadataID: []byte("conflict")}}
	err := selectPreparedResponse(state, host, &other, other.InternalMetadataID, true, response)
	var protocol *ProtocolError
	if !errors.As(err, &protocol) || protocol.Stage != "prepared schema reconciliation" {
		t.Fatal("overflow disappeared from logical result", err)
	}
	if !state.InternalConflictDirty.Load() || state.InternalConflictEpoch.Load() != 0 {
		t.Fatal("unscheduled conflict not left dirty")
	}
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := runtimeFor(s).InternalPrepareOnConnExpected(cancelled, nil, state, encodedValues{}, false, nil); !errors.Is(err, context.Canceled) {
		t.Fatal("canceled waiter performed reconciliation work", err)
	}
	if err := state.InternalReconcilePendingConflict(); !errors.As(err, &protocol) {
		t.Fatal("future operation silently bypassed unresolved conflict", err)
	}
	if string(view.OwnedBytes()) != "retained" {
		t.Fatal("earlier delivered page invalidated")
	}
	p.InternalMu.Lock()
	delete(p.InternalPreparedRefreshScopes, "0")
	p.InternalMu.Unlock()
	if err := state.InternalReconcilePendingConflict(); err != nil {
		t.Fatal("available scope did not retry", err)
	}
	p.InternalMu.Lock()
	_, queued := p.InternalPreparedRefreshScopes["ks"]
	p.InternalMu.Unlock()
	if !queued || state.InternalConflictEpoch.Load() == 0 {
		t.Fatal("retry was not registered")
	}
}
func TestV2PreparedConflictCanRescheduleAfterReconciliation(t *testing.T) {
	s := func() *Session {
		opaqueStorageValuex := new(Session)
		implCluster.SessionData(opaqueStorageValuex).InternalLogger = nopLogger{}
		return opaqueStorageValuex
	}()
	implCluster.SessionData(s).InternalCtx, implCluster.SessionData(s).InternalCancel = context.WithCancel(context.Background())
	defer s.Close()
	state := &preparedState{InternalSession: s, InternalInfo: PreparedInfo{Keyspace: "ks", Table: "tbl"}}
	if err := state.InternalNoteMetadataConflict(); err != nil {
		t.Fatal(err)
	}
	first := state.InternalConflictEpoch.Load()
	if err := state.InternalNoteMetadataConflict(); err != nil || state.InternalConflictEpoch.Load() != first {
		t.Fatal("same conflict not coalesced")
	}
	p := implCluster.InternalSessionPeripheral(s)
	p.InternalMu.Lock()
	delete(p.InternalPreparedRefreshScopes, "ks")
	p.InternalSchemaReconciled.Add(1)
	p.InternalMu.Unlock()
	if err := state.InternalNoteMetadataConflict(); err != nil || state.InternalConflictEpoch.Load() == first {
		t.Fatal("future conflicts permanently suppressed", err)
	}
}
