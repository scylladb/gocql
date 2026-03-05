//go:build unit
// +build unit

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package gocql

import (
	"fmt"
	"log"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gocql/gocql/internal/tests/mock"
	"github.com/gocql/gocql/tablets"

	frm "github.com/gocql/gocql/internal/frame"
)

var (
	typeVarchar = NativeType{proto: 4, typ: TypeVarchar}
	typeBoolean = NativeType{proto: 4, typ: TypeBoolean}
	typeDouble  = NativeType{proto: 4, typ: TypeDouble}
	typeInt     = NativeType{proto: 4, typ: TypeInt}
	typeMapSS   = CollectionType{
		NativeType: NativeType{proto: 4, typ: TypeMap},
		Key:        NativeType{proto: 4, typ: TypeVarchar},
		Elem:       NativeType{proto: 4, typ: TypeVarchar},
	}
	typeMapSB = CollectionType{
		NativeType: NativeType{proto: 4, typ: TypeMap},
		Key:        NativeType{proto: 4, typ: TypeVarchar},
		Elem:       NativeType{proto: 4, typ: TypeBlob},
	}
	typeSetS = CollectionType{
		NativeType: NativeType{proto: 4, typ: TypeSet},
		Elem:       NativeType{proto: 4, typ: TypeVarchar},
	}
)

var keyspaceMeta = resultMetadata{
	columns: []ColumnInfo{
		{Name: "durable_writes", TypeInfo: typeBoolean},
		{Name: "replication", TypeInfo: typeMapSS},
	},
	actualColCount: 2,
	colCount:       2,
}

var tableMeta = resultMetadata{
	columns: []ColumnInfo{
		{Name: "table_name", TypeInfo: typeVarchar},
		{Name: "bloom_filter_fp_chance", TypeInfo: typeDouble},
		{Name: "caching", TypeInfo: typeMapSS},
		{Name: "comment", TypeInfo: typeVarchar},
		{Name: "compaction", TypeInfo: typeMapSS},
		{Name: "compression", TypeInfo: typeMapSS},
		{Name: "crc_check_chance", TypeInfo: typeDouble},
		{Name: "default_time_to_live", TypeInfo: typeInt},
		{Name: "gc_grace_seconds", TypeInfo: typeInt},
		{Name: "max_index_interval", TypeInfo: typeInt},
		{Name: "memtable_flush_period_in_ms", TypeInfo: typeInt},
		{Name: "min_index_interval", TypeInfo: typeInt},
		{Name: "speculative_retry", TypeInfo: typeVarchar},
		{Name: "flags", TypeInfo: typeSetS},
		{Name: "extensions", TypeInfo: typeMapSB},
	},
	actualColCount: 15,
	colCount:       15,
}

var columnMeta = resultMetadata{
	columns: []ColumnInfo{
		{Name: "table_name", TypeInfo: typeVarchar},
		{Name: "column_name", TypeInfo: typeVarchar},
		{Name: "clustering_order", TypeInfo: typeVarchar},
		{Name: "type", TypeInfo: typeVarchar},
		{Name: "kind", TypeInfo: typeVarchar},
		{Name: "position", TypeInfo: typeInt},
	},
	actualColCount: 6,
	colCount:       6,
}

func mustMarshal(info TypeInfo, value interface{}) []byte {
	b, err := Marshal(info, value)
	if err != nil {
		panic(fmt.Sprintf("mustMarshal(%v, %v): %v", info, value, err))
	}
	return b
}

func marshalRow(meta resultMetadata, values []interface{}) [][]byte {
	if len(meta.columns) != len(values) {
		panic(fmt.Sprintf("marshalRow: column count %d != value count %d", len(meta.columns), len(values)))
	}
	row := make([][]byte, len(values))
	for i, col := range meta.columns {
		row[i] = mustMarshal(col.TypeInfo, values[i])
	}
	return row
}

func makeKeyspaceRow(durableWrites bool) [][]byte {
	replication := map[string]string{
		"class":              "org.apache.cassandra.locator.SimpleStrategy",
		"replication_factor": "1",
	}
	return marshalRow(keyspaceMeta, []interface{}{durableWrites, replication})
}

func makeTableRow(tableName string) [][]byte {
	return marshalRow(tableMeta, []interface{}{
		tableName,              // table_name
		float64(0.01),          // bloom_filter_fp_chance
		map[string]string(nil), // caching
		"",                     // comment
		map[string]string(nil), // compaction
		map[string]string(nil), // compression
		float64(0),             // crc_check_chance
		0,                      // default_time_to_live
		0,                      // gc_grace_seconds
		0,                      // max_index_interval
		0,                      // memtable_flush_period_in_ms
		0,                      // min_index_interval
		"",                     // speculative_retry
		[]string(nil),          // flags
		map[string][]byte(nil), // extensions
	})
}

func makeColumnRow(tableName, colName, kind string, position int) [][]byte {
	return marshalRow(columnMeta, []interface{}{
		tableName, // table_name
		colName,   // column_name
		"none",    // clustering_order
		"int",     // type
		kind,      // kind
		position,  // position
	})
}

func makeIter(meta resultMetadata, rows ...[][]byte) *Iter {
	if len(rows) == 0 {
		return &Iter{}
	}
	var allData [][]byte
	for _, row := range rows {
		allData = append(allData, row...)
	}
	return &Iter{
		meta:    meta,
		framer:  &mock.MockFramer{Data: allData},
		numRows: len(rows),
	}
}

type tableInfo struct {
	name    string
	columns []columnInfo
}

type columnInfo struct {
	name     string
	kind     string // "partition_key", "clustering", "regular"
	position int
}

type schemaDataMock struct {
	fakeControlConn

	mu                        sync.Mutex
	awaitSchemaAgreementCalls int
	queries                   []queryRecord

	knownKeyspaces map[string][]tableInfo
	queryDelay     time.Duration
}

func (m *schemaDataMock) awaitSchemaAgreement() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.awaitSchemaAgreementCalls++
	return nil
}

func (m *schemaDataMock) query(statement string, values ...interface{}) *Iter {
	m.mu.Lock()
	m.queries = append(m.queries, queryRecord{method: "query", stmt: statement})
	delay := m.queryDelay
	m.mu.Unlock()

	if delay > 0 {
		time.Sleep(delay)
	}

	return &Iter{}
}

func (m *schemaDataMock) querySystem(statement string, values ...interface{}) *Iter {
	m.mu.Lock()
	m.queries = append(m.queries, queryRecord{method: "querySystem", stmt: statement})
	delay := m.queryDelay
	m.mu.Unlock()

	if delay > 0 {
		time.Sleep(delay)
	}

	if strings.HasPrefix(statement, "SELECT durable_writes, replication FROM system_schema.keyspaces") {
		ksName, _ := values[0].(string)
		if _, ok := m.knownKeyspaces[ksName]; ok {
			return makeIter(keyspaceMeta, makeKeyspaceRow(true))
		}
		return &Iter{}
	}

	if strings.HasPrefix(statement, "SELECT * FROM system_schema.tables WHERE keyspace_name = ?") &&
		!strings.Contains(statement, "AND table_name") {
		ksName, _ := values[0].(string)
		tables, ok := m.knownKeyspaces[ksName]
		if !ok || len(tables) == 0 {
			return &Iter{}
		}
		var rows [][]byte
		for _, t := range tables {
			rows = append(rows, makeTableRow(t.name)...)
		}
		return &Iter{
			meta:    tableMeta,
			framer:  &mock.MockFramer{Data: rows},
			numRows: len(tables),
		}
	}

	if strings.HasPrefix(statement, "SELECT * FROM system_schema.tables WHERE keyspace_name = ? AND table_name = ?") {
		ksName, _ := values[0].(string)
		tblName, _ := values[1].(string)
		tables, ok := m.knownKeyspaces[ksName]
		if ok {
			for _, t := range tables {
				if t.name == tblName {
					return makeIter(tableMeta, makeTableRow(t.name))
				}
			}
		}
		return &Iter{}
	}

	if strings.HasPrefix(statement, "SELECT * FROM system_schema.columns WHERE keyspace_name = ?") &&
		!strings.Contains(statement, "AND table_name") {
		ksName, _ := values[0].(string)
		tables, ok := m.knownKeyspaces[ksName]
		if !ok {
			return &Iter{}
		}
		var rows [][]byte
		count := 0
		for _, t := range tables {
			for _, c := range t.columns {
				rows = append(rows, makeColumnRow(t.name, c.name, c.kind, c.position)...)
				count++
			}
		}
		if count == 0 {
			return &Iter{}
		}
		return &Iter{
			meta:    columnMeta,
			framer:  &mock.MockFramer{Data: rows},
			numRows: count,
		}
	}

	if strings.HasPrefix(statement, "SELECT * FROM system_schema.columns WHERE keyspace_name = ? AND table_name = ?") {
		ksName, _ := values[0].(string)
		tblName, _ := values[1].(string)
		tables, ok := m.knownKeyspaces[ksName]
		if !ok {
			return &Iter{}
		}
		var rows [][]byte
		count := 0
		for _, t := range tables {
			if t.name != tblName {
				continue
			}
			for _, c := range t.columns {
				rows = append(rows, makeColumnRow(t.name, c.name, c.kind, c.position)...)
				count++
			}
		}
		if count == 0 {
			return &Iter{}
		}
		return &Iter{
			meta:    columnMeta,
			framer:  &mock.MockFramer{Data: rows},
			numRows: count,
		}
	}

	return &Iter{}
}

func (m *schemaDataMock) resetQueries() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.queries = nil
}

func (m *schemaDataMock) getStatements() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	stmts := make([]string, len(m.queries))
	for i, q := range m.queries {
		stmts[i] = q.stmt
	}
	return stmts
}

func (m *schemaDataMock) getQueryCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.queries)
}

func (m *schemaDataMock) getAwaitSchemaAgreementCalls() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.awaitSchemaAgreementCalls
}

func newSchemaEventTestSession(control controlConnection, policy HostSelectionPolicy, keyspace string) *Session {
	s := &Session{
		control: control,
		policy:  policy,
		logger:  log.Default(),
		cfg:     ClusterConfig{Keyspace: keyspace},
	}
	s.hostSource = &ringDescriber{cfg: &s.cfg, logger: s.logger}
	s.metadataDescriber = &metadataDescriber{
		session: s,
		metadata: &Metadata{
			tabletsMetadata: tablets.NewCowTabletList(),
		},
	}
	return s
}

func newSchemaEventTestSessionWithMock(mockCtrl *schemaDataMock) *Session {
	s := newSchemaEventTestSession(mockCtrl, &trackingPolicy{}, "")
	s.useSystemSchema = true
	s.hasAggregatesAndFunctions = false
	return s
}

func populateKeyspace(s *Session, ksName string, tableNames ...string) {
	ks := &KeyspaceMetadata{
		Name:          ksName,
		DurableWrites: true,
		Tables:        make(map[string]*TableMetadata),
	}
	for _, tbl := range tableNames {
		ks.Tables[tbl] = &TableMetadata{
			Keyspace: ksName,
			Name:     tbl,
			Columns: map[string]*ColumnMetadata{
				"id": {Keyspace: ksName, Table: tbl, Name: "id", Kind: ColumnPartitionKey},
			},
		}
	}
	s.metadataDescriber.metadata.keyspaceMetadata.set(ksName, ks)
}

type trackingPolicy struct {
	roundRobinHostPolicy
	mu                   sync.Mutex
	keyspaceChangedCalls []KeyspaceUpdateEvent
}

func (t *trackingPolicy) KeyspaceChanged(event KeyspaceUpdateEvent) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.keyspaceChangedCalls = append(t.keyspaceChangedCalls, event)
}

func (t *trackingPolicy) getKeyspaceChangedCalls() []KeyspaceUpdateEvent {
	t.mu.Lock()
	defer t.mu.Unlock()
	dst := make([]KeyspaceUpdateEvent, len(t.keyspaceChangedCalls))
	copy(dst, t.keyspaceChangedCalls)
	return dst
}

type queryRecord struct {
	method string
	stmt   string
}

func addTestTablets(t *testing.T, session *Session, ksName, tblName string) {
	t.Helper()
	t1, err := tablets.TabletInfoBuilder{
		KeyspaceName: ksName,
		TableName:    tblName,
		FirstToken:   0,
		LastToken:    100,
	}.Build()
	if err != nil {
		t.Fatal(err)
	}
	t2, err := tablets.TabletInfoBuilder{
		KeyspaceName: ksName,
		TableName:    tblName,
		FirstToken:   101,
		LastToken:    200,
	}.Build()
	if err != nil {
		t.Fatal(err)
	}
	session.metadataDescriber.AddTablet(t1)
	session.metadataDescriber.AddTablet(t2)
}

func TestHandleSchemaEvent(t *testing.T) {
	t.Parallel()

	t.Run("cache_state", func(t *testing.T) {
		t.Parallel()
		tests := []struct {
			name           string
			keyspaces      map[string][]string // ks → tables to pre-populate
			tablets        [][2]string         // (ks, table) pairs to add tablets for
			event          frame
			wantKsGone     []string    // keyspaces removed from cache
			wantKsPresent  []string    // keyspaces still in cache
			wantTblGone    [][2]string // (ks, table) removed from Tables map
			wantTblPresent [][2]string // (ks, table) still in Tables map
			wantTblInvalid [][2]string // (ks, table) in tablesInvalidated
			wantTablets    int         // expected tablet count; -1 to skip check
		}{
			{
				name:        "keyspace/CREATED clears cache",
				keyspaces:   map[string][]string{"test_ks": {"tbl_a", "tbl_b"}},
				event:       &frm.SchemaChangeKeyspace{Change: "CREATED", Keyspace: "test_ks"},
				wantKsGone:  []string{"test_ks"},
				wantTablets: -1,
			},
			{
				name:        "keyspace/UPDATED clears cache and removes tablets",
				keyspaces:   map[string][]string{"test_ks": {"tbl_a"}},
				tablets:     [][2]string{{"test_ks", "tbl_a"}},
				event:       &frm.SchemaChangeKeyspace{Change: "UPDATED", Keyspace: "test_ks"},
				wantKsGone:  []string{"test_ks"},
				wantTablets: 0,
			},
			{
				name:        "keyspace/DROPPED clears cache and removes tablets",
				keyspaces:   map[string][]string{"test_ks": {"tbl_a"}},
				tablets:     [][2]string{{"test_ks", "tbl_a"}},
				event:       &frm.SchemaChangeKeyspace{Change: "DROPPED", Keyspace: "test_ks"},
				wantKsGone:  []string{"test_ks"},
				wantTablets: 0,
			},
			{
				name:        "keyspace/CREATED does not remove tablets",
				keyspaces:   map[string][]string{"test_ks": {"tbl_a"}},
				tablets:     [][2]string{{"test_ks", "tbl_a"}},
				event:       &frm.SchemaChangeKeyspace{Change: "CREATED", Keyspace: "test_ks"},
				wantKsGone:  []string{"test_ks"},
				wantTablets: 2,
			},
			{
				name:           "table/CREATED invalidates table only",
				keyspaces:      map[string][]string{"test_ks": {"tbl_a", "tbl_b"}},
				event:          &frm.SchemaChangeTable{Change: "CREATED", Keyspace: "test_ks", Object: "tbl_a"},
				wantKsPresent:  []string{"test_ks"},
				wantTblGone:    [][2]string{{"test_ks", "tbl_a"}},
				wantTblPresent: [][2]string{{"test_ks", "tbl_b"}},
				wantTblInvalid: [][2]string{{"test_ks", "tbl_a"}},
				wantTablets:    -1,
			},
			{
				name:           "table/UPDATED invalidates table and removes tablets",
				keyspaces:      map[string][]string{"test_ks": {"tbl_a", "tbl_b"}},
				tablets:        [][2]string{{"test_ks", "tbl_a"}},
				event:          &frm.SchemaChangeTable{Change: "UPDATED", Keyspace: "test_ks", Object: "tbl_a"},
				wantKsPresent:  []string{"test_ks"},
				wantTblGone:    [][2]string{{"test_ks", "tbl_a"}},
				wantTblPresent: [][2]string{{"test_ks", "tbl_b"}},
				wantTablets:    0,
			},
			{
				name:        "table/DROPPED removes tablets",
				keyspaces:   map[string][]string{"test_ks": {"tbl_a"}},
				tablets:     [][2]string{{"test_ks", "tbl_a"}},
				event:       &frm.SchemaChangeTable{Change: "DROPPED", Keyspace: "test_ks", Object: "tbl_a"},
				wantTablets: 0,
			},
			{
				name:        "table/CREATED does not remove tablets",
				keyspaces:   map[string][]string{"test_ks": {"tbl_a"}},
				tablets:     [][2]string{{"test_ks", "tbl_a"}},
				event:       &frm.SchemaChangeTable{Change: "CREATED", Keyspace: "test_ks", Object: "tbl_a"},
				wantTablets: 2,
			},
			{
				name:        "type/CREATED clears entire keyspace",
				keyspaces:   map[string][]string{"test_ks": {"tbl_a", "tbl_b"}},
				event:       &frm.SchemaChangeType{Change: "CREATED", Keyspace: "test_ks", Object: "my_type"},
				wantKsGone:  []string{"test_ks"},
				wantTablets: -1,
			},
			{
				name:        "function/CREATED clears entire keyspace",
				keyspaces:   map[string][]string{"test_ks": {"tbl_a"}},
				event:       &frm.SchemaChangeFunction{Change: "CREATED", Keyspace: "test_ks", Name: "fn", Args: []string{"int"}},
				wantKsGone:  []string{"test_ks"},
				wantTablets: -1,
			},
			{
				name:        "aggregate/CREATED clears entire keyspace",
				keyspaces:   map[string][]string{"test_ks": {"tbl_a"}},
				event:       &frm.SchemaChangeAggregate{Change: "CREATED", Keyspace: "test_ks", Name: "agg", Args: []string{"int"}},
				wantKsGone:  []string{"test_ks"},
				wantTablets: -1,
			},
			// Cross-isolation
			{
				name:           "keyspace/DROPPED does not affect other keyspace",
				keyspaces:      map[string][]string{"ks_a": {"tbl_a"}, "ks_b": {"tbl_b"}},
				event:          &frm.SchemaChangeKeyspace{Change: "DROPPED", Keyspace: "ks_a"},
				wantKsGone:     []string{"ks_a"},
				wantKsPresent:  []string{"ks_b"},
				wantTblPresent: [][2]string{{"ks_b", "tbl_b"}},
				wantTablets:    -1,
			},
			{
				name:           "table/UPDATED does not affect other tables",
				keyspaces:      map[string][]string{"test_ks": {"tbl_a", "tbl_b", "tbl_c"}},
				event:          &frm.SchemaChangeTable{Change: "UPDATED", Keyspace: "test_ks", Object: "tbl_a"},
				wantKsPresent:  []string{"test_ks"},
				wantTblGone:    [][2]string{{"test_ks", "tbl_a"}},
				wantTblPresent: [][2]string{{"test_ks", "tbl_b"}, {"test_ks", "tbl_c"}},
				wantTablets:    -1,
			},
			// Tablet cross-isolation
			{
				name:        "table/DROPPED for different table keeps tablets",
				keyspaces:   map[string][]string{"test_ks": {"tbl_a", "tbl_b"}},
				tablets:     [][2]string{{"test_ks", "tbl_a"}},
				event:       &frm.SchemaChangeTable{Change: "DROPPED", Keyspace: "test_ks", Object: "tbl_b"},
				wantTablets: 2,
			},
			{
				name:        "keyspace/DROPPED for different keyspace keeps tablets",
				keyspaces:   map[string][]string{"ks_a": {"tbl_a"}, "ks_b": {"tbl_b"}},
				tablets:     [][2]string{{"ks_a", "tbl_a"}},
				event:       &frm.SchemaChangeKeyspace{Change: "DROPPED", Keyspace: "ks_b"},
				wantTablets: 2,
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
				ctrl := &schemaDataMock{knownKeyspaces: map[string][]tableInfo{}}
				s := newSchemaEventTestSessionWithMock(ctrl)
				for ks, tables := range tt.keyspaces {
					populateKeyspace(s, ks, tables...)
				}
				for _, tb := range tt.tablets {
					addTestTablets(t, s, tb[0], tb[1])
				}

				s.handleSchemaEvent([]frame{tt.event})

				for _, ks := range tt.wantKsGone {
					if _, found := s.metadataDescriber.metadata.keyspaceMetadata.getKeyspace(ks); found {
						t.Errorf("keyspace %q should have been removed from cache", ks)
					}
				}
				for _, ks := range tt.wantKsPresent {
					if _, found := s.metadataDescriber.metadata.keyspaceMetadata.getKeyspace(ks); !found {
						t.Errorf("keyspace %q should still be in cache", ks)
					}
				}
				for _, pair := range tt.wantTblGone {
					ks, _ := s.metadataDescriber.metadata.keyspaceMetadata.getKeyspace(pair[0])
					if ks != nil {
						if _, ok := ks.Tables[pair[1]]; ok {
							t.Errorf("table %s.%s should have been removed from Tables map", pair[0], pair[1])
						}
					}
				}
				for _, pair := range tt.wantTblPresent {
					ks, _ := s.metadataDescriber.metadata.keyspaceMetadata.getKeyspace(pair[0])
					if ks == nil {
						t.Errorf("keyspace %q not found when checking table %s", pair[0], pair[1])
					} else if _, ok := ks.Tables[pair[1]]; !ok {
						t.Errorf("table %s.%s should still be in Tables map", pair[0], pair[1])
					}
				}
				for _, pair := range tt.wantTblInvalid {
					ks, _ := s.metadataDescriber.metadata.keyspaceMetadata.getKeyspace(pair[0])
					if ks == nil {
						t.Errorf("keyspace %q not found when checking tablesInvalidated %s", pair[0], pair[1])
					} else if _, ok := ks.tablesInvalidated[pair[1]]; !ok {
						t.Errorf("table %s.%s should be in tablesInvalidated", pair[0], pair[1])
					}
				}
				if tt.wantTablets >= 0 {
					if n := len(s.metadataDescriber.getTablets()); n != tt.wantTablets {
						t.Errorf("expected %d tablets, got %d", tt.wantTablets, n)
					}
				}
			})
		}
	})

	t.Run("callbacks", func(t *testing.T) {
		t.Parallel()
		tests := []struct {
			name                string
			populateTables      []string
			event               frame
			wantSchemaAgreement int
			wantKsChanged       []KeyspaceUpdateEvent
		}{
			{
				name:                "keyspace event calls schema agreement and policy",
				event:               &frm.SchemaChangeKeyspace{Change: "UPDATED", Keyspace: "test_ks"},
				wantSchemaAgreement: 1,
				wantKsChanged:       []KeyspaceUpdateEvent{{Keyspace: "test_ks", Change: "UPDATED"}},
			},
			{
				name:                "table event: no schema agreement, no policy callback",
				populateTables:      []string{"tbl_a"},
				event:               &frm.SchemaChangeTable{Change: "CREATED", Keyspace: "test_ks", Object: "tbl_a"},
				wantSchemaAgreement: 0,
			},
			{
				name:                "type event: no schema agreement, no policy callback",
				event:               &frm.SchemaChangeType{Change: "CREATED", Keyspace: "test_ks", Object: "my_type"},
				wantSchemaAgreement: 0,
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
				ctrl := &schemaDataMock{knownKeyspaces: map[string][]tableInfo{}}
				policy := &trackingPolicy{}
				s := newSchemaEventTestSession(ctrl, policy, "")
				s.useSystemSchema = true
				populateKeyspace(s, "test_ks", tt.populateTables...)

				s.handleSchemaEvent([]frame{tt.event})

				if got := ctrl.getAwaitSchemaAgreementCalls(); got != tt.wantSchemaAgreement {
					t.Fatalf("awaitSchemaAgreement: got %d, want %d", got, tt.wantSchemaAgreement)
				}
				kc := policy.getKeyspaceChangedCalls()
				if len(tt.wantKsChanged) == 0 {
					if len(kc) != 0 {
						t.Fatalf("KeyspaceChanged should not be called, got %+v", kc)
					}
				} else {
					if len(kc) != len(tt.wantKsChanged) {
						t.Fatalf("KeyspaceChanged: got %d calls, want %d", len(kc), len(tt.wantKsChanged))
					}
					for i, want := range tt.wantKsChanged {
						if kc[i] != want {
							t.Errorf("KeyspaceChanged[%d]: got %+v, want %+v", i, kc[i], want)
						}
					}
				}
			})
		}
	})

	fullRefresh := func(ksName string) map[string]int {
		return map[string]int{
			"SELECT durable_writes, replication FROM system_schema.keyspaces WHERE keyspace_name = ?": 1,
			"SELECT * FROM system_schema.tables WHERE keyspace_name = ?":                              1,
			"SELECT * FROM system_schema.columns WHERE keyspace_name = ?":                             1,
			"SELECT * FROM system_schema.types WHERE keyspace_name = ?":                               1,
			"SELECT * FROM system_schema.indexes WHERE keyspace_name = ?":                             1,
			"SELECT * FROM system_schema.views WHERE keyspace_name = ?":                               1,
			fmt.Sprintf("DESCRIBE KEYSPACE %s WITH INTERNALS", ksName):                                1,
		}
	}
	tableRefresh := map[string]int{
		"SELECT * FROM system_schema.tables WHERE keyspace_name = ? AND table_name = ?":     1,
		"SELECT * FROM system_schema.columns WHERE keyspace_name = ? AND table_name = ?":    1,
		"SELECT * FROM system_schema.indexes WHERE keyspace_name = ? AND table_name = ?":    1,
		"SELECT * FROM system_schema.views WHERE keyspace_name = ? AND base_table_name = ?": 1,
	}
	noQueries := map[string]int{}

	assertExpectedQueries := func(t *testing.T, ctrl *schemaDataMock, expected map[string]int) {
		t.Helper()
		stmts := ctrl.getStatements()
		if len(expected) == 0 {
			if len(stmts) != 0 {
				t.Errorf("expected 0 queries, got %d:\n%s", len(stmts), strings.Join(stmts, "\n"))
			}
			return
		}
		wantTotal := 0
		for stmt, wantCount := range expected {
			wantTotal += wantCount
			gotCount := 0
			for _, s := range stmts {
				if s == stmt {
					gotCount++
				}
			}
			if gotCount != wantCount {
				t.Errorf("query %q: got %d, want %d", stmt, gotCount, wantCount)
			}
		}
		if len(stmts) != wantTotal {
			t.Errorf("total queries: got %d, want %d\nqueries:\n%s",
				len(stmts), wantTotal, strings.Join(stmts, "\n"))
		}
	}

	t.Run("GetKeyspace", func(t *testing.T) {
		t.Parallel()
		tests := []struct {
			name                string
			knownKeyspaces      map[string][]tableInfo
			populateKs          map[string][]string
			disableSystemSchema bool
			event               frame // nil = no event
			getKeyspace         string
			wantError           bool
			expectedQueries     map[string]int // nil = skip check; empty = expect 0 queries
			wantNoRequery       bool           // second identical call fires 0 queries
		}{
			{
				name: "after keyspace event: refreshes and caches",
				knownKeyspaces: map[string][]tableInfo{
					"test_ks": {{name: "tbl_a", columns: []columnInfo{{name: "id", kind: "partition_key", position: 0}}}},
				},
				populateKs:      map[string][]string{"test_ks": {"tbl_a"}},
				event:           &frm.SchemaChangeKeyspace{Change: "UPDATED", Keyspace: "test_ks"},
				getKeyspace:     "test_ks",
				expectedQueries: fullRefresh("test_ks"),
				wantNoRequery:   true,
			},
			{
				name:            "after table event: returns cached, no queries",
				populateKs:      map[string][]string{"test_ks": {"tbl_a"}},
				event:           &frm.SchemaChangeTable{Change: "UPDATED", Keyspace: "test_ks", Object: "tbl_a"},
				getKeyspace:     "test_ks",
				expectedQueries: noQueries,
			},
			{
				name: "after type event: refreshes and caches",
				knownKeyspaces: map[string][]tableInfo{
					"test_ks": {{name: "tbl_a", columns: []columnInfo{{name: "id", kind: "partition_key", position: 0}}}},
				},
				populateKs:      map[string][]string{"test_ks": {"tbl_a"}},
				event:           &frm.SchemaChangeType{Change: "CREATED", Keyspace: "test_ks", Object: "my_type"},
				getKeyspace:     "test_ks",
				expectedQueries: fullRefresh("test_ks"),
				wantNoRequery:   true,
			},
			{
				name:            "uncached keyspace: refreshes and caches",
				knownKeyspaces:  map[string][]tableInfo{"new_ks": {}},
				getKeyspace:     "new_ks",
				expectedQueries: fullRefresh("new_ks"),
				wantNoRequery:   true,
			},
			{
				name:        "unknown keyspace: returns error",
				getKeyspace: "nonexistent",
				wantError:   true,
			},
			{
				name:                "useSystemSchema=false: returns error",
				disableSystemSchema: true,
				getKeyspace:         "test_ks",
				wantError:           true,
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
				knownKs := tt.knownKeyspaces
				if knownKs == nil {
					knownKs = map[string][]tableInfo{}
				}
				ctrl := &schemaDataMock{knownKeyspaces: knownKs}
				s := newSchemaEventTestSessionWithMock(ctrl)
				if tt.disableSystemSchema {
					s.useSystemSchema = false
				}
				for ks, tables := range tt.populateKs {
					populateKeyspace(s, ks, tables...)
				}
				if tt.event != nil {
					s.handleSchemaEvent([]frame{tt.event})
				}

				ctrl.resetQueries()

				ks, err := s.metadataDescriber.GetKeyspace(tt.getKeyspace)
				if tt.wantError {
					if err == nil {
						t.Fatal("expected error")
					}
					return
				}
				if err != nil {
					t.Fatalf("GetKeyspace failed: %v", err)
				}
				if ks.Name != tt.getKeyspace {
					t.Fatalf("expected keyspace %s, got %s", tt.getKeyspace, ks.Name)
				}
				if tt.expectedQueries != nil {
					assertExpectedQueries(t, ctrl, tt.expectedQueries)
				}
				if tt.wantNoRequery {
					ctrl.resetQueries()
					_, err = s.metadataDescriber.GetKeyspace(tt.getKeyspace)
					if err != nil {
						t.Fatalf("second GetKeyspace failed: %v", err)
					}
					assertExpectedQueries(t, ctrl, noQueries)
				}
			})
		}
	})

	t.Run("GetTable", func(t *testing.T) {
		t.Parallel()
		tests := []struct {
			name            string
			knownKeyspaces  map[string][]tableInfo
			populateKs      map[string][]string
			event           frame
			getTable        [2]string // [ks, table]
			wantError       bool
			expectedQueries map[string]int // nil = skip check; empty = expect 0 queries
			expectNoRequery bool
		}{
			{
				name: "after table event: refreshes only that table",
				knownKeyspaces: map[string][]tableInfo{
					"test_ks": {
						{name: "tbl_a", columns: []columnInfo{{name: "id", kind: "partition_key", position: 0}}},
						{name: "tbl_b", columns: []columnInfo{{name: "id", kind: "partition_key", position: 0}}},
					},
				},
				populateKs:      map[string][]string{"test_ks": {"tbl_a", "tbl_b"}},
				event:           &frm.SchemaChangeTable{Change: "UPDATED", Keyspace: "test_ks", Object: "tbl_a"},
				getTable:        [2]string{"test_ks", "tbl_a"},
				expectedQueries: tableRefresh,
				expectNoRequery: true,
			},
			{
				name:            "after table event: other table returns cached directly",
				populateKs:      map[string][]string{"test_ks": {"tbl_a", "tbl_b"}},
				event:           &frm.SchemaChangeTable{Change: "UPDATED", Keyspace: "test_ks", Object: "tbl_a"},
				getTable:        [2]string{"test_ks", "tbl_b"},
				expectedQueries: noQueries,
			},
			{
				name: "after keyspace event: refreshes full keyspace",
				knownKeyspaces: map[string][]tableInfo{
					"test_ks": {{name: "tbl_a", columns: []columnInfo{{name: "id", kind: "partition_key", position: 0}}}},
				},
				populateKs:      map[string][]string{"test_ks": {"tbl_a"}},
				event:           &frm.SchemaChangeKeyspace{Change: "UPDATED", Keyspace: "test_ks"},
				getTable:        [2]string{"test_ks", "tbl_a"},
				expectedQueries: fullRefresh("test_ks"),
				expectNoRequery: true,
			},
			{
				name: "after type event: refreshes full keyspace",
				knownKeyspaces: map[string][]tableInfo{
					"test_ks": {{name: "tbl_a", columns: []columnInfo{{name: "id", kind: "partition_key", position: 0}}}},
				},
				populateKs:      map[string][]string{"test_ks": {"tbl_a"}},
				event:           &frm.SchemaChangeType{Change: "CREATED", Keyspace: "test_ks", Object: "my_type"},
				getTable:        [2]string{"test_ks", "tbl_a"},
				expectedQueries: fullRefresh("test_ks"),
			},
			{
				name: "unknown table: returns error",
				knownKeyspaces: map[string][]tableInfo{
					"test_ks": {{name: "tbl_a", columns: []columnInfo{{name: "id", kind: "partition_key", position: 0}}}},
				},
				populateKs: map[string][]string{"test_ks": {"tbl_a"}},
				getTable:   [2]string{"test_ks", "nonexistent"},
				wantError:  true,
			},
			{
				name:      "unknown keyspace: returns error",
				getTable:  [2]string{"nonexistent", "tbl_a"},
				wantError: true,
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
				knownKs := tt.knownKeyspaces
				if knownKs == nil {
					knownKs = map[string][]tableInfo{}
				}
				ctrl := &schemaDataMock{knownKeyspaces: knownKs}
				s := newSchemaEventTestSessionWithMock(ctrl)
				for ks, tables := range tt.populateKs {
					populateKeyspace(s, ks, tables...)
				}
				if tt.event != nil {
					s.handleSchemaEvent([]frame{tt.event})
				}

				ctrl.resetQueries()

				tbl, err := s.metadataDescriber.GetTable(tt.getTable[0], tt.getTable[1])
				if tt.wantError {
					if err == nil {
						t.Fatal("expected error")
					}
					return
				}
				if err != nil {
					t.Fatalf("GetTable failed: %v", err)
				}
				if tbl.Name != tt.getTable[1] {
					t.Fatalf("expected table %s, got %s", tt.getTable[1], tbl.Name)
				}
				if tt.expectedQueries != nil {
					assertExpectedQueries(t, ctrl, tt.expectedQueries)
				}
				if tt.expectNoRequery {
					ctrl.resetQueries()
					_, err = s.metadataDescriber.GetTable(tt.getTable[0], tt.getTable[1])
					if err != nil {
						t.Fatalf("second GetTable failed: %v", err)
					}
					assertExpectedQueries(t, ctrl, noQueries)
				}
			})
		}
	})

	t.Run("batch/multiple_table_events", func(t *testing.T) {
		t.Parallel()
		ctrl := &schemaDataMock{
			knownKeyspaces: map[string][]tableInfo{
				"test_ks": {
					{name: "tbl_a", columns: []columnInfo{{name: "id", kind: "partition_key", position: 0}}},
					{name: "tbl_b", columns: []columnInfo{{name: "id", kind: "partition_key", position: 0}}},
					{name: "tbl_c", columns: []columnInfo{{name: "id", kind: "partition_key", position: 0}}},
				},
			},
		}
		s := newSchemaEventTestSessionWithMock(ctrl)
		populateKeyspace(s, "test_ks", "tbl_a", "tbl_b", "tbl_c")

		s.handleSchemaEvent([]frame{
			&frm.SchemaChangeTable{Change: "UPDATED", Keyspace: "test_ks", Object: "tbl_a"},
			&frm.SchemaChangeTable{Change: "UPDATED", Keyspace: "test_ks", Object: "tbl_b"},
		})

		ks, _ := s.metadataDescriber.metadata.keyspaceMetadata.getKeyspace("test_ks")
		if _, ok := ks.Tables["tbl_a"]; ok {
			t.Fatal("tbl_a should be invalidated")
		}
		if _, ok := ks.Tables["tbl_b"]; ok {
			t.Fatal("tbl_b should be invalidated")
		}
		if _, ok := ks.Tables["tbl_c"]; !ok {
			t.Fatal("tbl_c should still be cached")
		}

		ctrl.resetQueries()

		_, err := s.metadataDescriber.GetTable("test_ks", "tbl_a")
		if err != nil {
			t.Fatalf("GetTable(tbl_a) failed: %v", err)
		}
		if ctrl.getQueryCount() == 0 {
			t.Fatal("expected queries for tbl_a")
		}

		ctrl.resetQueries()

		_, err = s.metadataDescriber.GetTable("test_ks", "tbl_b")
		if err != nil {
			t.Fatalf("GetTable(tbl_b) failed: %v", err)
		}
		if ctrl.getQueryCount() == 0 {
			t.Fatal("expected queries for tbl_b")
		}

		ctrl.resetQueries()

		_, err = s.metadataDescriber.GetTable("test_ks", "tbl_c")
		if err != nil {
			t.Fatalf("GetTable(tbl_c) failed: %v", err)
		}
		if got := ctrl.getQueryCount(); got != 0 {
			t.Fatalf("tbl_c not invalidated, expected 0 queries, got %d", got)
		}
	})

	t.Run("batch/mixed_keyspace_and_table_events", func(t *testing.T) {
		t.Parallel()
		ctrl := &schemaDataMock{
			knownKeyspaces: map[string][]tableInfo{
				"test_ks": {
					{name: "tbl_a", columns: []columnInfo{{name: "id", kind: "partition_key", position: 0}}},
				},
			},
		}
		policy := &trackingPolicy{}
		s := newSchemaEventTestSession(ctrl, policy, "")
		s.useSystemSchema = true
		s.hasAggregatesAndFunctions = false
		populateKeyspace(s, "test_ks", "tbl_a")

		s.handleSchemaEvent([]frame{
			&frm.SchemaChangeTable{Change: "CREATED", Keyspace: "test_ks", Object: "tbl_a"},
			&frm.SchemaChangeKeyspace{Change: "UPDATED", Keyspace: "test_ks"},
		})

		if got := ctrl.getAwaitSchemaAgreementCalls(); got != 1 {
			t.Fatalf("awaitSchemaAgreement: got %d, want 1", got)
		}
		if _, found := s.metadataDescriber.metadata.keyspaceMetadata.getKeyspace("test_ks"); found {
			t.Fatal("keyspace should have been removed from cache by keyspace event")
		}

		ctrl.resetQueries()

		ks, err := s.metadataDescriber.GetKeyspace("test_ks")
		if err != nil {
			t.Fatalf("GetKeyspace failed: %v", err)
		}
		if ks.Name != "test_ks" {
			t.Fatalf("expected test_ks, got %s", ks.Name)
		}
		if got := ctrl.getQueryCount(); got == 0 {
			t.Fatal("expected queries after keyspace was cleared")
		}
	})
}
