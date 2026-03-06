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
	queryError     error // if set, querySystem returns an Iter with this error
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
	queryErr := m.queryError
	m.mu.Unlock()

	if delay > 0 {
		time.Sleep(delay)
	}

	if queryErr != nil {
		return &Iter{err: queryErr}
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

func (m *schemaDataMock) setQueryError(err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.queryError = err
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
		"SELECT * FROM system_schema.tables WHERE keyspace_name = ? AND table_name = ?":                     1,
		"SELECT * FROM system_schema.columns WHERE keyspace_name = ? AND table_name = ?":                    1,
		"SELECT * FROM system_schema.indexes WHERE keyspace_name = ? AND table_name = ?":                    1,
		"SELECT * FROM system_schema.views WHERE keyspace_name = ? AND base_table_name = ? ALLOW FILTERING": 1,
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

// TestSchemaRefreshConcurrent validates that concurrent GetKeyspace/GetTable
// calls for an uncached or invalidated keyspace result in only a single set
// of schema queries, not one per caller.
func TestSchemaRefreshConcurrent(t *testing.T) {
	t.Parallel()

	const concurrency = 10

	knownKeyspaces := map[string][]tableInfo{
		"test_ks": {
			{name: "tbl_a", columns: []columnInfo{{name: "id", kind: "partition_key", position: 0}}},
		},
	}

	fullRefreshCount := 7  // keyspace + tables + columns + types + indexes + views + DESCRIBE
	tableRefreshCount := 4 // tables + columns + indexes + views (filtered by table_name)

	t.Run("GetKeyspace/uncached", func(t *testing.T) {
		t.Parallel()
		ctrl := &schemaDataMock{
			knownKeyspaces: knownKeyspaces,
			queryDelay:     10 * time.Millisecond,
		}
		s := newSchemaEventTestSessionWithMock(ctrl)

		var wg sync.WaitGroup
		for range concurrency {
			wg.Add(1)
			go func() {
				defer wg.Done()
				_, _ = s.metadataDescriber.GetKeyspace("test_ks")
			}()
		}
		wg.Wait()

		if got := ctrl.getQueryCount(); got != fullRefreshCount {
			t.Errorf("expected %d queries (single full refresh), got %d", fullRefreshCount, got)
		}
	})

	t.Run("GetKeyspace/after_invalidation", func(t *testing.T) {
		t.Parallel()
		ctrl := &schemaDataMock{
			knownKeyspaces: knownKeyspaces,
			queryDelay:     10 * time.Millisecond,
		}
		s := newSchemaEventTestSessionWithMock(ctrl)
		populateKeyspace(s, "test_ks", "tbl_a")

		s.handleSchemaEvent([]frame{
			&frm.SchemaChangeKeyspace{Change: "UPDATED", Keyspace: "test_ks"},
		})

		ctrl.resetQueries()

		var wg sync.WaitGroup
		for range concurrency {
			wg.Add(1)
			go func() {
				defer wg.Done()
				_, _ = s.metadataDescriber.GetKeyspace("test_ks")
			}()
		}
		wg.Wait()

		if got := ctrl.getQueryCount(); got != fullRefreshCount {
			t.Errorf("expected %d queries (single full refresh), got %d", fullRefreshCount, got)
		}
	})

	t.Run("GetTable/after_table_invalidation", func(t *testing.T) {
		t.Parallel()
		ctrl := &schemaDataMock{
			knownKeyspaces: knownKeyspaces,
			queryDelay:     10 * time.Millisecond,
		}
		s := newSchemaEventTestSessionWithMock(ctrl)
		populateKeyspace(s, "test_ks", "tbl_a")

		s.handleSchemaEvent([]frame{
			&frm.SchemaChangeTable{Change: "UPDATED", Keyspace: "test_ks", Object: "tbl_a"},
		})

		ctrl.resetQueries()

		var wg sync.WaitGroup
		for range concurrency {
			wg.Add(1)
			go func() {
				defer wg.Done()
				_, _ = s.metadataDescriber.GetTable("test_ks", "tbl_a")
			}()
		}
		wg.Wait()

		if got := ctrl.getQueryCount(); got != tableRefreshCount {
			t.Errorf("expected %d queries (single table refresh), got %d", tableRefreshCount, got)
		}
	})
}

// TestConcurrentSchemaRefreshErrorHandling verifies that concurrent
// GetKeyspace and GetTable calls behave correctly when the underlying
// schema queries succeed or fail, including mixed scenarios where
// errors are injected mid-flight.
func TestConcurrentSchemaRefreshErrorHandling(t *testing.T) {
	t.Parallel()

	const concurrency = 10

	defaultTables := map[string][]tableInfo{
		"test_ks": {
			{name: "tbl_a", columns: []columnInfo{{name: "id", kind: "partition_key", position: 0}}},
			{name: "tbl_b", columns: []columnInfo{{name: "pk", kind: "partition_key", position: 0}}},
		},
	}

	t.Run("GetKeyspace/all_succeed", func(t *testing.T) {
		t.Parallel()
		ctrl := &schemaDataMock{
			knownKeyspaces: defaultTables,
			queryDelay:     10 * time.Millisecond,
		}
		s := newSchemaEventTestSessionWithMock(ctrl)

		var wg sync.WaitGroup
		results := make([]*KeyspaceMetadata, concurrency)
		errs := make([]error, concurrency)
		for i := range concurrency {
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()
				results[idx], errs[idx] = s.metadataDescriber.GetKeyspace("test_ks")
			}(i)
		}
		wg.Wait()

		for i := range concurrency {
			if errs[i] != nil {
				t.Errorf("goroutine %d: unexpected error: %v", i, errs[i])
			}
			if results[i] == nil {
				t.Errorf("goroutine %d: got nil metadata", i)
			} else if results[i].Name != "test_ks" {
				t.Errorf("goroutine %d: expected keyspace test_ks, got %s", i, results[i].Name)
			}
		}
	})

	t.Run("GetKeyspace/all_fail", func(t *testing.T) {
		t.Parallel()
		injectedErr := fmt.Errorf("injected query failure")
		ctrl := &schemaDataMock{
			knownKeyspaces: defaultTables,
			queryDelay:     10 * time.Millisecond,
			queryError:     injectedErr,
		}
		s := newSchemaEventTestSessionWithMock(ctrl)

		var wg sync.WaitGroup
		errs := make([]error, concurrency)
		for i := range concurrency {
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()
				_, errs[idx] = s.metadataDescriber.GetKeyspace("test_ks")
			}(i)
		}
		wg.Wait()

		for i := range concurrency {
			if errs[i] == nil {
				t.Errorf("goroutine %d: expected error, got nil", i)
			}
		}
	})

	t.Run("GetKeyspace/fail_then_succeed", func(t *testing.T) {
		t.Parallel()
		// First wave fails, second wave succeeds — verifies singleflight
		// does not cache the error permanently.
		injectedErr := fmt.Errorf("transient failure")
		ctrl := &schemaDataMock{
			knownKeyspaces: defaultTables,
			queryDelay:     10 * time.Millisecond,
			queryError:     injectedErr,
		}
		s := newSchemaEventTestSessionWithMock(ctrl)

		// Wave 1: all fail.
		var wg sync.WaitGroup
		for range concurrency {
			wg.Add(1)
			go func() {
				defer wg.Done()
				_, _ = s.metadataDescriber.GetKeyspace("test_ks")
			}()
		}
		wg.Wait()

		// Clear error and retry — should succeed.
		ctrl.setQueryError(nil)
		ctrl.resetQueries()

		ks, err := s.metadataDescriber.GetKeyspace("test_ks")
		if err != nil {
			t.Fatalf("second attempt should succeed, got: %v", err)
		}
		if ks.Name != "test_ks" {
			t.Fatalf("expected keyspace test_ks, got %s", ks.Name)
		}
	})

	t.Run("GetKeyspace/nonexistent_keyspace", func(t *testing.T) {
		t.Parallel()
		ctrl := &schemaDataMock{
			knownKeyspaces: defaultTables,
			queryDelay:     10 * time.Millisecond,
		}
		s := newSchemaEventTestSessionWithMock(ctrl)

		var wg sync.WaitGroup
		errs := make([]error, concurrency)
		for i := range concurrency {
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()
				_, errs[idx] = s.metadataDescriber.GetKeyspace("no_such_ks")
			}(i)
		}
		wg.Wait()

		for i := range concurrency {
			if errs[i] == nil {
				t.Errorf("goroutine %d: expected ErrKeyspaceDoesNotExist, got nil", i)
			}
		}
	})

	t.Run("GetTable/all_succeed", func(t *testing.T) {
		t.Parallel()
		ctrl := &schemaDataMock{
			knownKeyspaces: defaultTables,
			queryDelay:     10 * time.Millisecond,
		}
		s := newSchemaEventTestSessionWithMock(ctrl)
		populateKeyspace(s, "test_ks", "tbl_a")
		s.metadataDescriber.invalidateTableSchema("test_ks", "tbl_a")

		var wg sync.WaitGroup
		results := make([]*TableMetadata, concurrency)
		errs := make([]error, concurrency)
		for i := range concurrency {
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()
				results[idx], errs[idx] = s.metadataDescriber.GetTable("test_ks", "tbl_a")
			}(i)
		}
		wg.Wait()

		for i := range concurrency {
			if errs[i] != nil {
				t.Errorf("goroutine %d: unexpected error: %v", i, errs[i])
			}
			if results[i] == nil {
				t.Errorf("goroutine %d: got nil table metadata", i)
			} else if results[i].Name != "tbl_a" {
				t.Errorf("goroutine %d: expected tbl_a, got %s", i, results[i].Name)
			}
		}
	})

	t.Run("GetTable/all_fail", func(t *testing.T) {
		t.Parallel()
		injectedErr := fmt.Errorf("injected table query failure")
		ctrl := &schemaDataMock{
			knownKeyspaces: defaultTables,
			queryError:     injectedErr,
			queryDelay:     10 * time.Millisecond,
		}
		s := newSchemaEventTestSessionWithMock(ctrl)
		populateKeyspace(s, "test_ks", "tbl_a")
		s.metadataDescriber.invalidateTableSchema("test_ks", "tbl_a")

		var wg sync.WaitGroup
		errs := make([]error, concurrency)
		for i := range concurrency {
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()
				_, errs[idx] = s.metadataDescriber.GetTable("test_ks", "tbl_a")
			}(i)
		}
		wg.Wait()

		for i := range concurrency {
			if errs[i] == nil {
				t.Errorf("goroutine %d: expected error, got nil", i)
			}
		}
	})

	t.Run("GetTable/fail_then_succeed", func(t *testing.T) {
		t.Parallel()
		injectedErr := fmt.Errorf("transient table failure")
		ctrl := &schemaDataMock{
			knownKeyspaces: defaultTables,
			queryDelay:     10 * time.Millisecond,
			queryError:     injectedErr,
		}
		s := newSchemaEventTestSessionWithMock(ctrl)
		populateKeyspace(s, "test_ks", "tbl_a")
		s.metadataDescriber.invalidateTableSchema("test_ks", "tbl_a")

		// Wave 1: all fail.
		var wg sync.WaitGroup
		for range concurrency {
			wg.Add(1)
			go func() {
				defer wg.Done()
				_, _ = s.metadataDescriber.GetTable("test_ks", "tbl_a")
			}()
		}
		wg.Wait()

		// Clear error, re-invalidate (the failed refresh may have left
		// tablesInvalidated in an inconsistent state), and retry.
		ctrl.setQueryError(nil)
		ctrl.resetQueries()
		s.metadataDescriber.invalidateTableSchema("test_ks", "tbl_a")

		tbl, err := s.metadataDescriber.GetTable("test_ks", "tbl_a")
		if err != nil {
			t.Fatalf("second attempt should succeed, got: %v", err)
		}
		if tbl.Name != "tbl_a" {
			t.Fatalf("expected tbl_a, got %s", tbl.Name)
		}
	})

	t.Run("GetTable/nonexistent_table", func(t *testing.T) {
		t.Parallel()
		ctrl := &schemaDataMock{
			knownKeyspaces: defaultTables,
			queryDelay:     10 * time.Millisecond,
		}
		s := newSchemaEventTestSessionWithMock(ctrl)
		populateKeyspace(s, "test_ks", "tbl_a")

		var wg sync.WaitGroup
		errs := make([]error, concurrency)
		for i := range concurrency {
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()
				_, errs[idx] = s.metadataDescriber.GetTable("test_ks", "no_such_table")
			}(i)
		}
		wg.Wait()

		for i := range concurrency {
			if errs[i] == nil {
				t.Errorf("goroutine %d: expected ErrNotFound, got nil", i)
			}
		}
	})

	t.Run("GetKeyspace_and_GetTable/concurrent_mixed", func(t *testing.T) {
		t.Parallel()
		// Exercises the interplay between concurrent keyspace and table
		// refreshes hitting the singleflight groups simultaneously.
		ctrl := &schemaDataMock{
			knownKeyspaces: defaultTables,
			queryDelay:     5 * time.Millisecond,
		}
		s := newSchemaEventTestSessionWithMock(ctrl)

		var wg sync.WaitGroup
		ksErrs := make([]error, concurrency)
		tblErrs := make([]error, concurrency)
		for i := range concurrency {
			wg.Add(2)
			go func(idx int) {
				defer wg.Done()
				_, ksErrs[idx] = s.metadataDescriber.GetKeyspace("test_ks")
			}(i)
			go func(idx int) {
				defer wg.Done()
				_, tblErrs[idx] = s.metadataDescriber.GetTable("test_ks", "tbl_a")
			}(i)
		}
		wg.Wait()

		for i := range concurrency {
			if ksErrs[i] != nil {
				t.Errorf("GetKeyspace goroutine %d: unexpected error: %v", i, ksErrs[i])
			}
			if tblErrs[i] != nil {
				t.Errorf("GetTable goroutine %d: unexpected error: %v", i, tblErrs[i])
			}
		}
	})

	t.Run("GetTable/different_tables_concurrent", func(t *testing.T) {
		t.Parallel()
		// Two different tables invalidated concurrently: each gets its own
		// singleflight key, so both refresh independently.
		ctrl := &schemaDataMock{
			knownKeyspaces: defaultTables,
			queryDelay:     5 * time.Millisecond,
		}
		s := newSchemaEventTestSessionWithMock(ctrl)
		populateKeyspace(s, "test_ks", "tbl_a", "tbl_b")
		s.metadataDescriber.invalidateTableSchema("test_ks", "tbl_a")
		s.metadataDescriber.invalidateTableSchema("test_ks", "tbl_b")

		var wg sync.WaitGroup
		aErrs := make([]error, concurrency)
		bErrs := make([]error, concurrency)
		for i := range concurrency {
			wg.Add(2)
			go func(idx int) {
				defer wg.Done()
				_, aErrs[idx] = s.metadataDescriber.GetTable("test_ks", "tbl_a")
			}(i)
			go func(idx int) {
				defer wg.Done()
				_, bErrs[idx] = s.metadataDescriber.GetTable("test_ks", "tbl_b")
			}(i)
		}
		wg.Wait()

		for i := range concurrency {
			if aErrs[i] != nil {
				t.Errorf("tbl_a goroutine %d: unexpected error: %v", i, aErrs[i])
			}
			if bErrs[i] != nil {
				t.Errorf("tbl_b goroutine %d: unexpected error: %v", i, bErrs[i])
			}
		}

		// Verify both tables are now cached.
		for _, name := range []string{"tbl_a", "tbl_b"} {
			tbl, err := s.metadataDescriber.GetTable("test_ks", name)
			if err != nil {
				t.Errorf("GetTable(%s) after refresh: %v", name, err)
			} else if tbl.Name != name {
				t.Errorf("expected %s, got %s", name, tbl.Name)
			}
		}
	})

	t.Run("GetTable/different_tables_one_fails", func(t *testing.T) {
		t.Parallel()
		// tbl_a exists in the mock, tbl_x does not — concurrent refreshes
		// for both: one succeeds, one gets ErrNotFound.
		ctrl := &schemaDataMock{
			knownKeyspaces: defaultTables,
			queryDelay:     5 * time.Millisecond,
		}
		s := newSchemaEventTestSessionWithMock(ctrl)
		populateKeyspace(s, "test_ks", "tbl_a", "tbl_x")
		s.metadataDescriber.invalidateTableSchema("test_ks", "tbl_a")
		s.metadataDescriber.invalidateTableSchema("test_ks", "tbl_x")

		var wg sync.WaitGroup
		aErrs := make([]error, concurrency)
		xErrs := make([]error, concurrency)
		for i := range concurrency {
			wg.Add(2)
			go func(idx int) {
				defer wg.Done()
				_, aErrs[idx] = s.metadataDescriber.GetTable("test_ks", "tbl_a")
			}(i)
			go func(idx int) {
				defer wg.Done()
				_, xErrs[idx] = s.metadataDescriber.GetTable("test_ks", "tbl_x")
			}(i)
		}
		wg.Wait()

		for i := range concurrency {
			if aErrs[i] != nil {
				t.Errorf("tbl_a goroutine %d: unexpected error: %v", i, aErrs[i])
			}
			if xErrs[i] == nil {
				t.Errorf("tbl_x goroutine %d: expected ErrNotFound, got nil", i)
			}
		}
	})
}
