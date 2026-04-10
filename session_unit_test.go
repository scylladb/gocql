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
/*
 * Content before git sha 34fdeebefcbf183ed7f916f931aa0586fdaa1b40
 * Copyright (c) 2016, The Gocql authors,
 * provided under the BSD-3-Clause License.
 * See the NOTICE file distributed with this work for additional information.
 */

package gocql

import (
	"context"
	"errors"
	"reflect"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gocql/gocql/tablets"
)

func TestShouldPrepareNonDML(t *testing.T) {
	t.Parallel()

	nonDMLStatements := []string{
		"CREATE TABLE ks.tbl (id int PRIMARY KEY)",
		"ALTER TABLE ks.tbl ADD col text",
		"DROP TABLE ks.tbl",
		"TRUNCATE ks.tbl",
		"CREATE KEYSPACE ks WITH replication = {'class': 'SimpleStrategy'}",
		"DROP KEYSPACE ks",
		"GRANT SELECT ON ks.tbl TO user1",
		"USE ks",
	}

	for _, stmt := range nonDMLStatements {
		t.Run(stmt, func(t *testing.T) {
			q := &Query{stmt: stmt, routingInfo: &queryRoutingInfo{}}
			if q.shouldPrepare() {
				t.Errorf("shouldPrepare(%q) = true, want false", stmt)
			}
		})
	}
}

func TestShouldPrepareDML(t *testing.T) {
	t.Parallel()

	dmlStatements := []string{
		"SELECT * FROM ks.tbl",
		"INSERT INTO ks.tbl (id) VALUES (?)",
		"UPDATE ks.tbl SET col = ? WHERE id = ?",
		"DELETE FROM ks.tbl WHERE id = ?",
		"BEGIN BATCH INSERT INTO ks.tbl (id) VALUES (1) APPLY BATCH",
		"BEGIN BATCH INSERT INTO ks.tbl (id) VALUES (1) APPLY BATCH;",
		"BEGIN UNLOGGED BATCH INSERT INTO ks.tbl (id) VALUES (1) APPLY BATCH",
		"  SELECT * FROM ks.tbl",
		"\t INSERT INTO ks.tbl (id) VALUES (?)",
		"\u00a0SELECT * FROM ks.tbl",
	}

	for _, stmt := range dmlStatements {
		t.Run(stmt, func(t *testing.T) {
			q := &Query{stmt: stmt, routingInfo: &queryRoutingInfo{}}
			if !q.shouldPrepare() {
				t.Errorf("shouldPrepare(%q) = false, want true", stmt)
			}
		})
	}
}

func TestAsyncSessionInit(t *testing.T) {
	t.Parallel()

	// Build a 3 node cluster to test host metric mapping
	var addresses = []string{
		"127.0.0.1",
		"127.0.0.2",
		"127.0.0.3",
	}
	// only build 1 of the servers so that we can test not connecting to the last
	// one
	srv := NewTestServerWithAddress(addresses[0]+":0", t, defaultProto, context.Background())
	defer srv.Stop()

	// just choose any port
	cluster := testCluster(defaultProto, srv.Address, addresses[1]+":9999", addresses[2]+":9999")
	cluster.PoolConfig.HostSelectionPolicy = SingleHostReadyPolicy(RoundRobinHostPolicy())
	db, err := cluster.CreateSession()
	if err != nil {
		t.Fatalf("NewCluster: %v", err)
	}
	defer db.Close()

	// make sure the session works
	if err := db.Query("void").Exec(); err != nil {
		t.Fatalf("unexpected error from void")
	}
}

func TestExtractKeyspaceTableFromDDL(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		ddl       string
		wantKS    string
		wantTable string
	}{
		{
			name:      "simple_create_table",
			ddl:       "CREATE TABLE gocql_test.my_table (id int PRIMARY KEY)",
			wantKS:    "gocql_test",
			wantTable: "my_table",
		},
		{
			name:      "create_table_if_not_exists",
			ddl:       "CREATE TABLE IF NOT EXISTS gocql_test.my_table (id int PRIMARY KEY)",
			wantKS:    "gocql_test",
			wantTable: "my_table",
		},
		{
			name:      "lowercase_create_table",
			ddl:       "create table gocql_test.my_table (id int primary key)",
			wantKS:    "gocql_test",
			wantTable: "my_table",
		},
		{
			name:      "mixed_case_if_not_exists",
			ddl:       "Create Table If Not Exists gocql_test.my_table (id int PRIMARY KEY)",
			wantKS:    "gocql_test",
			wantTable: "my_table",
		},
		{
			name:      "no_keyspace_prefix",
			ddl:       "CREATE TABLE my_table (id int PRIMARY KEY)",
			wantKS:    "",
			wantTable: "",
		},
		{
			name:      "empty_string",
			ddl:       "",
			wantKS:    "",
			wantTable: "",
		},
		{
			name:      "create_keyspace_ignored",
			ddl:       "CREATE KEYSPACE my_ks WITH replication = {}",
			wantKS:    "",
			wantTable: "",
		},
		{
			name:      "materialized_view_ignored",
			ddl:       "CREATE MATERIALIZED VIEW my_ks.my_view AS SELECT * FROM my_ks.my_table WHERE id IS NOT NULL PRIMARY KEY (id)",
			wantKS:    "",
			wantTable: "",
		},
		{
			name:      "multiline_ddl",
			ddl:       "CREATE TABLE gocql_test.test_single_routing_key (\n\tfirst_id int,\n\tsecond_id int,\n\tPRIMARY KEY (first_id, second_id)\n)",
			wantKS:    "gocql_test",
			wantTable: "test_single_routing_key",
		},
		{
			name:      "tablets_disabled_keyspace",
			ddl:       "CREATE TABLE gocql_test_tablets_disabled.my_table (id int PRIMARY KEY)",
			wantKS:    "gocql_test_tablets_disabled",
			wantTable: "my_table",
		},
		{
			name:      "drop_table_if_exists",
			ddl:       "DROP TABLE IF EXISTS gocql_test.my_table",
			wantKS:    "gocql_test",
			wantTable: "my_table",
		},
		{
			name:      "drop_table_if_exists_lowercase",
			ddl:       "drop table if exists gocql_test.my_table",
			wantKS:    "gocql_test",
			wantTable: "my_table",
		},
		{
			name:      "drop_table_no_keyspace",
			ddl:       "DROP TABLE IF EXISTS my_table",
			wantKS:    "",
			wantTable: "",
		},
		{
			name:      "table_with_space_before_paren",
			ddl:       "CREATE TABLE gocql_test.t1 (id int PRIMARY KEY)",
			wantKS:    "gocql_test",
			wantTable: "t1",
		},
		{
			name:      "drop_keyspace_returns_empty",
			ddl:       "DROP KEYSPACE IF EXISTS gocql_test",
			wantKS:    "",
			wantTable: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotKS, gotTable := extractKeyspaceTableFromDDL(tt.ddl)
			if gotKS != tt.wantKS {
				t.Errorf("extractKeyspaceTableFromDDL(%q) keyspace = %q, want %q", tt.ddl, gotKS, tt.wantKS)
			}
			if gotTable != tt.wantTable {
				t.Errorf("extractKeyspaceTableFromDDL(%q) table = %q, want %q", tt.ddl, gotTable, tt.wantTable)
			}
		})
	}
}

func TestTableMetadataAfterInvalidation(t *testing.T) {
	t.Parallel()

	ctrl := &schemaDataMock{
		knownKeyspaces: map[string][]tableInfo{
			"test_ks": {
				{name: "tbl_a", columns: []columnInfo{{name: "id", kind: "partition_key", position: 0}}},
			},
		},
	}
	s := newSchemaEventTestSessionWithMock(ctrl)
	defer s.Close()
	s.isInitialized = true
	populateKeyspace(s, "test_ks", "tbl_a")

	tbl, err := s.TableMetadata("test_ks", "tbl_a")
	if err != nil {
		t.Fatalf("initial TableMetadata failed: %v", err)
	}
	if tbl.Name != "tbl_a" {
		t.Fatalf("expected table name tbl_a, got %s", tbl.Name)
	}

	s.metadataDescriber.invalidateTableSchema("test_ks", "tbl_a")

	ctrl.resetQueries()

	tbl, err = s.TableMetadata("test_ks", "tbl_a")
	if err != nil {
		t.Fatalf("TableMetadata after invalidation failed: %v", err)
	}
	if tbl.Name != "tbl_a" {
		t.Fatalf("expected table name tbl_a, got %s", tbl.Name)
	}
	if ctrl.getQueryCount() == 0 {
		t.Fatal("expected queries to refresh tbl_a after invalidation")
	}
}

func TestTableMetadataAfterKeyspaceInvalidation(t *testing.T) {
	t.Parallel()

	ctrl := &schemaDataMock{
		knownKeyspaces: map[string][]tableInfo{
			"test_ks": {
				{name: "tbl_a", columns: []columnInfo{{name: "id", kind: "partition_key", position: 0}}},
			},
		},
	}
	s := newSchemaEventTestSessionWithMock(ctrl)
	defer s.Close()
	s.isInitialized = true
	populateKeyspace(s, "test_ks", "tbl_a")

	_, err := s.TableMetadata("test_ks", "tbl_a")
	if err != nil {
		t.Fatalf("initial TableMetadata failed: %v", err)
	}

	s.metadataDescriber.invalidateKeyspaceSchema("test_ks")

	ctrl.resetQueries()

	tbl, err := s.TableMetadata("test_ks", "tbl_a")
	if err != nil {
		t.Fatalf("TableMetadata after keyspace invalidation failed: %v", err)
	}
	if tbl.Name != "tbl_a" {
		t.Fatalf("expected table name tbl_a, got %s", tbl.Name)
	}
	if ctrl.getQueryCount() == 0 {
		t.Fatal("expected queries to reload keyspace after invalidation")
	}
}

func newTestSessionForTableMetadata(ctrl *schemaDataMock) *Session {
	s := newSchemaEventTestSessionWithMock(ctrl)
	s.isInitialized = true
	return s
}

func TestScyllaIsCdcTableAfterInvalidation(t *testing.T) {
	t.Parallel()

	ctrl := &schemaDataMock{
		knownKeyspaces: map[string][]tableInfo{
			"test_ks": {
				{name: "tbl_scylla_cdc_log", columns: []columnInfo{{name: "id", kind: "partition_key", position: 0}}},
			},
		},
	}
	s := newTestSessionForTableMetadata(ctrl)
	defer s.Close()
	populateKeyspace(s, "test_ks", "tbl_scylla_cdc_log")

	_, err := scyllaIsCdcTable(s, "test_ks", "tbl_scylla_cdc_log")
	if err != nil {
		t.Fatalf("initial scyllaIsCdcTable failed: %v", err)
	}

	s.metadataDescriber.invalidateTableSchema("test_ks", "tbl_scylla_cdc_log")
	ctrl.resetQueries()

	_, err = scyllaIsCdcTable(s, "test_ks", "tbl_scylla_cdc_log")
	if err != nil {
		t.Fatalf("scyllaIsCdcTable after invalidation failed: %v", err)
	}
	if ctrl.getQueryCount() == 0 {
		t.Fatal("expected queries to refresh tbl_scylla_cdc_log after invalidation")
	}
}

func TestScyllaIsCdcTableNotCdcSuffix(t *testing.T) {
	t.Parallel()

	ctrl := &schemaDataMock{
		knownKeyspaces: map[string][]tableInfo{
			"test_ks": {
				{name: "regular_table", columns: []columnInfo{{name: "id", kind: "partition_key", position: 0}}},
			},
		},
	}
	s := newTestSessionForTableMetadata(ctrl)
	defer s.Close()
	populateKeyspace(s, "test_ks", "regular_table")

	isCdc, err := scyllaIsCdcTable(s, "test_ks", "regular_table")
	if err != nil {
		t.Fatalf("scyllaIsCdcTable failed: %v", err)
	}
	if isCdc {
		t.Fatal("expected regular_table to not be a CDC table")
	}
}

func TestTestTableName(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		parts []string
		want  string
	}{
		{
			name: "basic",
			want: "testtesttablename_basic",
		},
		{
			name:  "with_parts",
			parts: []string{"single"},
			want:  "testtesttablename_with_parts_single",
		},
		{
			name:  "multiple_parts",
			parts: []string{"foo", "bar"},
			want:  "testtesttablename_multiple_parts_foo_bar",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := testTableName(t, tt.parts...)
			if got != tt.want {
				t.Errorf("testTableName() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestTestTableNameSanitizesSpecialChars(t *testing.T) {
	t.Parallel()

	t.Run("sub/with/slashes", func(t *testing.T) {
		got := testTableName(t)
		if strings.Contains(got, "/") {
			t.Errorf("expected no slashes, got %q", got)
		}
		if strings.Contains(got, "__") {
			t.Errorf("expected no consecutive underscores, got %q", got)
		}
	})
}

func TestTestTableNameTruncation(t *testing.T) {
	t.Parallel()

	long := "abcdefghijklmnopqrstuvwxyz0123456789abcdefghijklmnopqrstuvwxyz"
	t.Run(long, func(t *testing.T) {
		got := testTableName(t, "extra")
		if len(got) > maxCQLIdentifierLen {
			t.Errorf("len = %d, want <= %d; value = %q", len(got), maxCQLIdentifierLen, got)
		}
		// Should preserve chars from both the start and end around the hash.
		if got[:5] != "testt" {
			t.Errorf("expected prefix from test name, got %q", got)
		}
		if !strings.HasSuffix(got, "_extra") {
			t.Errorf("expected suffix from test name and parts, got %q", got)
		}
		if len(got) != maxCQLIdentifierLen {
			t.Errorf("expected truncated name to use full identifier budget, got len=%d value=%q", len(got), got)
		}
		if got[15] != '_' || got[32] != '_' {
			t.Errorf("expected <first-n>_<hash>_<last-n> structure, got %q", got)
		}
		for _, ch := range got[16:32] {
			if (ch < '0' || ch > '9') && (ch < 'a' || ch > 'f') {
				t.Errorf("expected hex hash in the middle, got %q", got)
				break
			}
		}
	})
}

func TestTestTableNameUniqueness(t *testing.T) {
	t.Parallel()

	a := testTableName(t, "alpha")
	b := testTableName(t, "beta")
	if a == b {
		t.Errorf("expected different names, both got %q", a)
	}
}

// testWarningFramer is a mock framerInterface that returns configurable warnings.
type testWarningFramer struct {
	warnings      []string
	customPayload map[string][]byte
	released      bool
}

func (f *testWarningFramer) ReadBytesInternal() ([]byte, error) { return nil, nil }
func (f *testWarningFramer) GetCustomPayload() map[string][]byte {
	return f.customPayload
}
func (f *testWarningFramer) GetHeaderWarnings() []string { return f.warnings }
func (f *testWarningFramer) Release()                    { f.released = true }

type recordingWarningHandler struct {
	calls     int
	lastHost  *HostInfo
	lastQry   ExecutableQuery
	queryStmt string
	warnings  []string
}

func (h *recordingWarningHandler) HandleWarnings(qry ExecutableQuery, host *HostInfo, warnings []string) {
	h.calls++
	h.lastQry = qry
	h.lastHost = host
	if query, ok := qry.(*Query); ok {
		h.queryStmt = query.stmt
	}
	h.warnings = slices.Clone(warnings)
}

type staticConnPicker struct {
	conn *Conn
}

func (p staticConnPicker) Pick(Token, ExecutableQuery) *Conn { return p.conn }
func (p staticConnPicker) Put(*Conn) error                   { return nil }
func (p staticConnPicker) Remove(*Conn)                      {}
func (p staticConnPicker) InFlight() int                     { return 0 }
func (p staticConnPicker) Size() (int, int)                  { return 1, 0 }
func (p staticConnPicker) Close()                            {}
func (p staticConnPicker) NextShard() (shardID, nrShards int) {
	return 0, 0
}
func (p staticConnPicker) GetConnectionCount() int       { return 1 }
func (p staticConnPicker) GetExcessConnectionCount() int { return 0 }
func (p staticConnPicker) GetShardCount() int            { return 0 }

type staticSelectedHost struct {
	host *HostInfo
}

func (h staticSelectedHost) Info() *HostInfo { return h.host }
func (h staticSelectedHost) Token() Token    { return nil }
func (h staticSelectedHost) Mark(error)      {}

type pagingTestConn struct {
	executeQueryFunc func(ctx context.Context, qry *Query) *Iter
}

func (*pagingTestConn) Close() {}
func (*pagingTestConn) exec(context.Context, frameBuilder, Tracer, time.Duration) (*framer, error) {
	return nil, nil
}
func (*pagingTestConn) awaitSchemaAgreement(context.Context) error { return nil }
func (c *pagingTestConn) executeQuery(ctx context.Context, qry *Query) *Iter {
	return c.executeQueryFunc(ctx, qry)
}
func (*pagingTestConn) querySystem(context.Context, string, ...interface{}) *Iter { return nil }
func (*pagingTestConn) getIsSchemaV2() bool                                       { return false }
func (*pagingTestConn) setSchemaV2(bool)                                          {}
func (*pagingTestConn) getScyllaSupported() ScyllaConnectionFeatures {
	return ScyllaConnectionFeatures{}
}

type fixedRetryPolicy struct {
	maxRetries int
	retryType  RetryType
}

func (p *fixedRetryPolicy) Attempt(q RetryableQuery) bool {
	return q.Attempts() <= p.maxRetries
}

func (p *fixedRetryPolicy) GetRetryType(error) RetryType {
	return p.retryType
}

type executorTestQuery struct {
	ctx         context.Context
	rt          RetryPolicy
	spec        SpeculativeExecutionPolicy
	idempotent  bool
	consistency Consistency
	attempts    int
	borrowed    int
	released    int
	executeFunc func(context.Context, *Conn) *Iter
}

func (q *executorTestQuery) borrowForExecution() {
	q.borrowed++
}

func (q *executorTestQuery) releaseAfterExecution() {
	q.released++
}

func (q *executorTestQuery) execute(ctx context.Context, conn *Conn) *Iter {
	return q.executeFunc(ctx, conn)
}

func (q *executorTestQuery) attempt(string, time.Time, time.Time, *Iter, *HostInfo) {
	q.attempts++
}

func (q *executorTestQuery) retryPolicy() RetryPolicy {
	return q.rt
}

func (q *executorTestQuery) speculativeExecutionPolicy() SpeculativeExecutionPolicy {
	if q.spec == nil {
		return NonSpeculativeExecution{}
	}
	return q.spec
}

func (q *executorTestQuery) GetRoutingKey() ([]byte, error) { return nil, nil }
func (q *executorTestQuery) Keyspace() string               { return "" }
func (q *executorTestQuery) Table() string                  { return "" }
func (q *executorTestQuery) IsIdempotent() bool             { return q.idempotent }
func (q *executorTestQuery) IsLWT() bool                    { return false }
func (q *executorTestQuery) GetCustomPartitioner() Partitioner {
	return nil
}
func (q *executorTestQuery) GetHostID() string { return "" }

func (q *executorTestQuery) withContext(ctx context.Context) ExecutableQuery {
	q2 := *q
	q2.ctx = ctx
	return &q2
}

func (q *executorTestQuery) Attempts() int {
	return q.attempts
}

func (q *executorTestQuery) SetConsistency(c Consistency) {
	q.consistency = c
}

func (q *executorTestQuery) GetConsistency() Consistency {
	return q.consistency
}

func (q *executorTestQuery) Context() context.Context {
	if q.ctx == nil {
		return context.Background()
	}
	return q.ctx
}

func (q *executorTestQuery) GetSession() *Session { return nil }

func newTestQueryExecutor(host *HostInfo) *queryExecutor {
	return &queryExecutor{
		pool: &policyConnPool{
			hostConnPools: map[string]*hostConnPool{
				host.HostID(): &hostConnPool{
					host:       host,
					connPicker: staticConnPicker{conn: &Conn{}},
				},
			},
		},
	}
}

func newWarningTestQuery() *Query {
	return &Query{
		context:     context.Background(),
		routingInfo: &queryRoutingInfo{},
		metrics:     &queryMetrics{m: make(map[string]*hostMetrics)},
		rt:          &SimpleRetryPolicy{NumRetries: 0},
		spec:        NonSpeculativeExecution{},
	}
}

func TestIterWarnings(t *testing.T) {
	t.Parallel()

	t.Run("NoFramer", func(t *testing.T) {
		iter := &Iter{}
		warnings := iter.Warnings()
		if len(warnings) != 0 {
			t.Errorf("expected no warnings, got %v", warnings)
		}
	})

	t.Run("SinglePage", func(t *testing.T) {
		framer := &testWarningFramer{warnings: []string{"warn1", "warn2"}}
		iter := &Iter{framer: framer}

		warnings := iter.Warnings()
		want := []string{"warn1", "warn2"}
		if !slices.Equal(warnings, want) {
			t.Errorf("Warnings() = %v, want %v", warnings, want)
		}
	})

	t.Run("ReturnsCopy", func(t *testing.T) {
		framer := &testWarningFramer{warnings: []string{"warn1"}}
		iter := &Iter{framer: framer}

		w1 := iter.Warnings()
		w2 := iter.Warnings()

		// Mutating w1 should not affect w2
		w1[0] = "mutated"
		if w2[0] == "mutated" {
			t.Error("Warnings() returned a shared slice, expected independent copies")
		}
	})

	t.Run("AccumulatedAcrossPages", func(t *testing.T) {
		page1Framer := &testWarningFramer{warnings: []string{"page1-warn1", "page1-warn2"}}
		iter := &Iter{
			framer:  page1Framer,
			numRows: 1,
			pos:     1,
			next:    nil,
		}

		if w := iter.framer.GetHeaderWarnings(); len(w) > 0 {
			iter.allWarnings = append(iter.allWarnings, w...)
		}
		iter.framer.Release()
		page2Framer := &testWarningFramer{warnings: []string{"page2-warn1"}}
		iter.framer = page2Framer

		warnings := iter.Warnings()
		want := []string{"page1-warn1", "page1-warn2", "page2-warn1"}
		if !slices.Equal(warnings, want) {
			t.Errorf("Warnings() = %v, want %v", warnings, want)
		}

		if !page1Framer.released {
			t.Error("page 1 framer was not released")
		}
	})

	t.Run("AfterClose", func(t *testing.T) {
		framer := &testWarningFramer{warnings: []string{"last-page-warn"}}
		iter := &Iter{
			framer:      framer,
			allWarnings: []string{"prev-page-warn"},
		}

		iter.Close()

		if !framer.released {
			t.Error("framer was not released on Close()")
		}
		if iter.framer != nil {
			t.Error("framer was not nilled on Close()")
		}

		warnings := iter.Warnings()
		want := []string{"prev-page-warn", "last-page-warn"}
		if !slices.Equal(warnings, want) {
			t.Errorf("Warnings() after Close() = %v, want %v", warnings, want)
		}
	})

	t.Run("EmptyPages", func(t *testing.T) {
		iter := &Iter{
			allWarnings: []string{"page1-warn"},
		}
		page2Framer := &testWarningFramer{warnings: nil}
		iter.framer = page2Framer

		warnings := iter.Warnings()
		want := []string{"page1-warn"}
		if !slices.Equal(warnings, want) {
			t.Errorf("Warnings() = %v, want %v", warnings, want)
		}
	})

	t.Run("CloseIdempotent", func(t *testing.T) {
		framer := &testWarningFramer{warnings: []string{"warn"}}
		iter := &Iter{framer: framer}

		iter.Close()
		iter.Close()

		warnings := iter.Warnings()
		want := []string{"warn"}
		if !slices.Equal(warnings, want) {
			t.Errorf("Warnings() after double Close() = %v, want %v", warnings, want)
		}
	})
}

func TestNewErrorIterWithReleasedFramer(t *testing.T) {
	t.Parallel()

	t.Run("PreservesMetadata", func(t *testing.T) {
		payload := map[string][]byte{"tablet": {1, 2, 3}}
		framer := &testWarningFramer{
			warnings:      []string{"warn1"},
			customPayload: payload,
		}

		iter := newErrorIterWithReleasedFramer(errors.New("boom"), framer)

		if !framer.released {
			t.Fatal("expected framer to be released")
		}
		if !slices.Equal(iter.Warnings(), []string{"warn1"}) {
			t.Fatalf("Warnings() = %v, want %v", iter.Warnings(), []string{"warn1"})
		}
		if !reflect.DeepEqual(iter.GetCustomPayload(), payload) {
			t.Fatalf("GetCustomPayload() = %v, want %v", iter.GetCustomPayload(), payload)
		}
	})
}

func TestIterWarningHandler(t *testing.T) {
	t.Parallel()

	t.Run("CloseDispatchesAccumulatedWarnings", func(t *testing.T) {
		handler := &recordingWarningHandler{}
		host := &HostInfo{hostId: UUID{1}}
		qry := &Query{
			routingInfo: &queryRoutingInfo{},
			metrics:     &queryMetrics{m: make(map[string]*hostMetrics)},
		}
		iter := (&Iter{
			framer:      &testWarningFramer{warnings: []string{"page2"}},
			allWarnings: []string{"page1"},
			host:        host,
		}).bindWarningHandler(qry, handler)

		if err := iter.Close(); err != nil {
			t.Fatalf("Close() returned unexpected error: %v", err)
		}

		want := []string{"page1", "page2"}
		if !slices.Equal(handler.warnings, want) {
			t.Fatalf("handler warnings = %v, want %v", handler.warnings, want)
		}
		if handler.calls != 1 {
			t.Fatalf("handler call count = %d, want 1", handler.calls)
		}
		if handler.lastHost != host {
			t.Fatal("handler host mismatch")
		}
		if handler.lastQry != qry {
			t.Fatal("handler query mismatch")
		}
	})

	t.Run("CloseIsIdempotent", func(t *testing.T) {
		handler := &recordingWarningHandler{}
		iter := (&Iter{
			framer: &testWarningFramer{warnings: []string{"warn"}},
		}).bindWarningHandler(&Query{
			routingInfo: &queryRoutingInfo{},
			metrics:     &queryMetrics{m: make(map[string]*hostMetrics)},
		}, handler)

		iter.Close()
		iter.Close()

		if handler.calls != 1 {
			t.Fatalf("handler call count = %d, want 1", handler.calls)
		}
	})

	t.Run("CopyPageDataTransfersReleasedMetadata", func(t *testing.T) {
		src := newErrorIterWithReleasedFramer(errors.New("boom"), &testWarningFramer{
			warnings:      []string{"warn"},
			customPayload: map[string][]byte{"k": {9}},
		})
		dst := &Iter{
			allWarnings: []string{"first-page"},
		}

		dst.copyPageData(src)

		wantWarnings := []string{"first-page", "warn"}
		if !slices.Equal(dst.Warnings(), wantWarnings) {
			t.Fatalf("Warnings() = %v, want %v", dst.Warnings(), wantWarnings)
		}
		if !reflect.DeepEqual(dst.GetCustomPayload(), map[string][]byte{"k": {9}}) {
			t.Fatalf("GetCustomPayload() = %v, want %v", dst.GetCustomPayload(), map[string][]byte{"k": {9}})
		}
	})

	t.Run("BindIgnoresNilHandler", func(t *testing.T) {
		iter := (&Iter{}).bindWarningHandler(&Query{
			routingInfo: &queryRoutingInfo{},
			metrics:     &queryMetrics{m: make(map[string]*hostMetrics)},
		}, nil)
		if iter.warningHandler != nil {
			t.Fatal("expected warning handler to remain nil")
		}
	})

	t.Run("HostPreservedAcrossClose", func(t *testing.T) {
		handler := &recordingWarningHandler{}
		host := &HostInfo{port: 9042, hostId: UUID{2}}
		iter := (&Iter{
			framer: &testWarningFramer{warnings: []string{"warn"}},
			host:   host,
		}).bindWarningHandler(&Batch{
			context:     context.Background(),
			routingInfo: &queryRoutingInfo{},
			metrics:     &queryMetrics{m: make(map[string]*hostMetrics)},
			rt:          &SimpleRetryPolicy{NumRetries: 0},
			spec:        NonSpeculativeExecution{},
		}, handler)

		iter.Close()

		if handler.lastHost != host {
			t.Fatal("expected handler to receive the iterator host")
		}
	})

	t.Run("CloseClearsBatchWarningQueryReference", func(t *testing.T) {
		handler := &recordingWarningHandler{}
		batch := &Batch{
			context:     context.Background(),
			routingInfo: &queryRoutingInfo{},
			metrics:     &queryMetrics{m: make(map[string]*hostMetrics)},
			rt:          &SimpleRetryPolicy{NumRetries: 0},
			spec:        NonSpeculativeExecution{},
		}
		iter := (&Iter{
			framer: &testWarningFramer{warnings: []string{"warn"}},
		}).bindWarningHandler(batch, handler)

		if err := iter.Close(); err != nil {
			t.Fatalf("Close() returned unexpected error: %v", err)
		}
		if handler.lastQry != batch {
			t.Fatal("handler batch mismatch")
		}
		if iter.warningQuery != nil {
			t.Fatal("expected warning query to be cleared after Close")
		}
		if iter.warningQueryOwned {
			t.Fatal("expected warningQueryOwned to be false after Close")
		}
	})

	t.Run("CloseWithoutWarningsDoesNotInvokeHandler", func(t *testing.T) {
		handler := &recordingWarningHandler{}
		iter := (&Iter{
			framer: &testWarningFramer{},
		}).bindWarningHandler(&Query{
			context:     context.Background(),
			routingInfo: &queryRoutingInfo{},
			metrics:     &queryMetrics{m: make(map[string]*hostMetrics)},
			rt:          &SimpleRetryPolicy{NumRetries: 0},
			spec:        NonSpeculativeExecution{},
		}, handler)

		iter.Close()

		if handler.calls != 0 {
			t.Fatalf("handler call count = %d, want 0", handler.calls)
		}
	})

	t.Run("HandleWarningsOnceAfterManualAccumulation", func(t *testing.T) {
		handler := &recordingWarningHandler{}
		iter := (&Iter{
			allWarnings: []string{"warn1"},
			host:        &HostInfo{hostId: UUID{3}},
		}).bindWarningHandler(&Query{
			routingInfo: &queryRoutingInfo{},
			metrics:     &queryMetrics{m: make(map[string]*hostMetrics)},
		}, handler)

		iter.handleWarningsOnce()
		iter.handleWarningsOnce()

		if handler.calls != 1 {
			t.Fatalf("handler call count = %d, want 1", handler.calls)
		}
	})

	t.Run("QueryReleaseBeforeCloseKeepsWarningQueryAlive", func(t *testing.T) {
		handler := &recordingWarningHandler{}
		qry := newWarningTestQuery()
		qry.refCount = 1
		qry.stmt = "SELECT now() FROM system.local"
		iter := (&Iter{
			framer: &testWarningFramer{warnings: []string{"warn"}},
		}).bindWarningHandler(qry, handler)

		qry.Release()

		if qry.stmt != "SELECT now() FROM system.local" {
			t.Fatalf("query statement reset before iterator close: %q", qry.stmt)
		}
		if err := iter.Close(); err != nil {
			t.Fatalf("Close() returned unexpected error: %v", err)
		}
		if handler.calls != 1 {
			t.Fatalf("handler call count = %d, want 1", handler.calls)
		}
		capturedQry, ok := handler.lastQry.(*Query)
		if !ok {
			t.Fatalf("handler query type = %T, want *Query", handler.lastQry)
		}
		if capturedQry != qry {
			t.Fatal("handler query mismatch")
		}
		if handler.queryStmt != "SELECT now() FROM system.local" {
			t.Fatalf("handler saw query statement %q, want %q", handler.queryStmt, "SELECT now() FROM system.local")
		}
	})

	t.Run("ReleasedErrorIterAutoFinalizesOnBind", func(t *testing.T) {
		handler := &recordingWarningHandler{}
		qry := newWarningTestQuery()
		qry.refCount = 1
		qry.stmt = "SELECT fail()"

		iter := newErrorIterWithReleasedFramer(errors.New("boom"), &testWarningFramer{
			warnings: []string{"warn"},
		}).bindWarningHandler(qry, handler)

		if got := atomic.LoadUint32(&qry.refCount); got != 1 {
			t.Fatalf("query refCount = %d, want 1", got)
		}
		if iter.warningQuery != nil {
			t.Fatal("expected warning query to be released")
		}
		if handler.calls != 1 {
			t.Fatalf("handler call count = %d, want 1", handler.calls)
		}
		if !slices.Equal(handler.warnings, []string{"warn"}) {
			t.Fatalf("handler warnings = %v, want %v", handler.warnings, []string{"warn"})
		}
		if err := iter.Close(); err == nil || err.Error() != "boom" {
			t.Fatalf("Close() = %v, want boom", err)
		}
	})
}

func TestIterAutoFinalizeOnTerminalConsumption(t *testing.T) {
	t.Parallel()

	t.Run("ScanEOFReleasesResources", func(t *testing.T) {
		handler := &recordingWarningHandler{}
		qry := newWarningTestQuery()
		qry.refCount = 1
		framer := &testWarningFramer{warnings: []string{"scan-eof"}}
		iter := (&Iter{
			framer:  framer,
			numRows: 1,
			meta: resultMetadata{
				actualColCount: 0,
			},
		}).bindWarningHandler(qry, handler)

		if !iter.Scan() {
			t.Fatal("expected first Scan() to succeed")
		}
		if iter.Scan() {
			t.Fatal("expected second Scan() to report EOF")
		}
		if !framer.released {
			t.Fatal("expected EOF to release the framer")
		}
		if iter.framer != nil {
			t.Fatal("expected framer to be cleared after EOF")
		}
		if got := atomic.LoadUint32(&qry.refCount); got != 1 {
			t.Fatalf("query refCount = %d, want 1", got)
		}
		if handler.calls != 1 {
			t.Fatalf("handler call count = %d, want 1", handler.calls)
		}
		if !slices.Equal(handler.warnings, []string{"scan-eof"}) {
			t.Fatalf("handler warnings = %v, want %v", handler.warnings, []string{"scan-eof"})
		}
	})

	t.Run("ScannerNextEOFReleasesResources", func(t *testing.T) {
		handler := &recordingWarningHandler{}
		qry := newWarningTestQuery()
		qry.refCount = 1
		framer := &testWarningFramer{warnings: []string{"scanner-eof"}}
		iter := (&Iter{
			framer:  framer,
			numRows: 1,
			meta: resultMetadata{
				actualColCount: 0,
			},
		}).bindWarningHandler(qry, handler)
		scanner := iter.Scanner()

		if !scanner.Next() {
			t.Fatal("expected first Next() to succeed")
		}
		if err := scanner.Scan(); err != nil {
			t.Fatalf("Scan() returned unexpected error: %v", err)
		}
		if scanner.Next() {
			t.Fatal("expected second Next() to report EOF")
		}
		if !framer.released {
			t.Fatal("expected EOF to release the framer")
		}
		if iter.framer != nil {
			t.Fatal("expected framer to be cleared after EOF")
		}
		if got := atomic.LoadUint32(&qry.refCount); got != 1 {
			t.Fatalf("query refCount = %d, want 1", got)
		}
		if handler.calls != 1 {
			t.Fatalf("handler call count = %d, want 1", handler.calls)
		}
		if !slices.Equal(handler.warnings, []string{"scanner-eof"}) {
			t.Fatalf("handler warnings = %v, want %v", handler.warnings, []string{"scanner-eof"})
		}
	})
}

func TestQueryExecutorRetryAndDiscardWarningHandling(t *testing.T) {
	t.Parallel()

	t.Run("SpeculativeLoserIsDiscardedWithoutWarnings", func(t *testing.T) {
		host := (&HostInfo{hostId: UUID{4}}).setState(NodeUp)
		handler := &recordingWarningHandler{}
		framer := &testWarningFramer{warnings: []string{"loser"}}
		qry := &executorTestQuery{
			rt:         &fixedRetryPolicy{maxRetries: 0, retryType: Rethrow},
			spec:       NonSpeculativeExecution{},
			idempotent: true,
		}
		qry.executeFunc = func(context.Context, *Conn) *Iter {
			return (&Iter{framer: framer}).bindWarningHandler(qry, handler)
		}

		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		executor := newTestQueryExecutor(host)
		executor.run(ctx, qry, func() SelectedHost { return staticSelectedHost{host: host} }, make(chan *Iter))

		if handler.calls != 0 {
			t.Fatalf("handler call count = %d, want 0", handler.calls)
		}
		if !framer.released {
			t.Fatal("speculative loser framer was not released")
		}
		if qry.released != 1 {
			t.Fatalf("releaseAfterExecution calls = %d, want 1", qry.released)
		}
	})

	t.Run("RetriedAttemptStillWarnsOnce", func(t *testing.T) {
		host := (&HostInfo{hostId: UUID{5}}).setState(NodeUp)
		handler := &recordingWarningHandler{}
		firstFramer := &testWarningFramer{warnings: []string{"retry-warn"}}
		finalFramer := &testWarningFramer{}
		qry := &executorTestQuery{
			ctx:        context.Background(),
			rt:         &fixedRetryPolicy{maxRetries: 1, retryType: Retry},
			spec:       NonSpeculativeExecution{},
			idempotent: true,
		}

		attempt := 0
		qry.executeFunc = func(context.Context, *Conn) *Iter {
			attempt++
			if attempt == 1 {
				return (&Iter{err: errors.New("boom"), framer: firstFramer}).bindWarningHandler(qry, handler)
			}
			return (&Iter{framer: finalFramer}).bindWarningHandler(qry, handler)
		}

		executor := newTestQueryExecutor(host)
		iter := executor.do(context.Background(), qry, func() SelectedHost { return staticSelectedHost{host: host} })
		defer iter.Close()

		if iter.err != nil {
			t.Fatalf("unexpected final error: %v", iter.err)
		}
		if !firstFramer.released {
			t.Fatal("retried attempt framer was not released")
		}
		if handler.calls != 1 {
			t.Fatalf("handler call count = %d, want 1", handler.calls)
		}
		if !slices.Equal(handler.warnings, []string{"retry-warn"}) {
			t.Fatalf("handler warnings = %v, want %v", handler.warnings, []string{"retry-warn"})
		}
	})
}

func TestIterCloseCleansPrefetchedNextPage(t *testing.T) {
	t.Parallel()

	t.Run("MaterializedNextPageIsReleasedWithoutDispatchingItsWarnings", func(t *testing.T) {
		handler := &recordingWarningHandler{}
		qry := newWarningTestQuery()
		currentFramer := &testWarningFramer{warnings: []string{"current"}}
		nextFramer := &testWarningFramer{warnings: []string{"prefetched"}}
		iter := (&Iter{
			framer: currentFramer,
			next: &nextIter{
				next: (&Iter{framer: nextFramer}).bindWarningHandler(qry, handler),
			},
		}).bindWarningHandler(qry, handler)

		iter.Close()

		if !currentFramer.released {
			t.Fatal("current framer was not released")
		}
		if !nextFramer.released {
			t.Fatal("prefetched next framer was not released")
		}
		if handler.calls != 1 {
			t.Fatalf("handler call count = %d, want 1", handler.calls)
		}
		if !slices.Equal(handler.warnings, []string{"current"}) {
			t.Fatalf("handler warnings = %v, want %v", handler.warnings, []string{"current"})
		}
		if iter.next != nil {
			t.Fatal("expected prefetched next iterator to be cleared on Close")
		}
	})

	t.Run("LatePrefetchResultIsClosedAfterCancellation", func(t *testing.T) {
		handler := &recordingWarningHandler{}
		next := newNextIter(newWarningTestQuery(), 1)

		next.close()
		select {
		case <-next.qry.Context().Done():
		default:
			t.Fatal("expected next-page context to be canceled")
		}

		lateFramer := &testWarningFramer{warnings: []string{"late"}}
		next.storeFetched((&Iter{framer: lateFramer}).bindWarningHandler(next.qry, handler))

		if !lateFramer.released {
			t.Fatal("late prefetched framer was not released")
		}
		if handler.calls != 0 {
			t.Fatalf("handler call count = %d, want 0", handler.calls)
		}
	})
}

func TestSliceMapClosesIterator(t *testing.T) {
	t.Parallel()

	handler := &recordingWarningHandler{}
	qry := newWarningTestQuery()
	framer := &testWarningFramer{warnings: []string{"slice-map"}}
	iter := (&Iter{
		framer: framer,
		meta: resultMetadata{
			actualColCount: 0,
		},
	}).bindWarningHandler(qry, handler)

	rows, err := iter.SliceMap()
	if err != nil {
		t.Fatalf("unexpected SliceMap error: %v", err)
	}
	if len(rows) != 0 {
		t.Fatalf("expected no rows, got %d", len(rows))
	}
	if !framer.released {
		t.Fatal("expected SliceMap to release the iterator framer")
	}
	if handler.calls != 1 {
		t.Fatalf("handler call count = %d, want 1", handler.calls)
	}
	if !slices.Equal(handler.warnings, []string{"slice-map"}) {
		t.Fatalf("handler warnings = %v, want %v", handler.warnings, []string{"slice-map"})
	}
}

func TestIterFetchNextPageRetiresConsumedFetchContextOnly(t *testing.T) {
	t.Parallel()

	rootCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var fetchedQry *Query
	nextPageFramer := &testWarningFramer{warnings: []string{"next"}}
	conn := &pagingTestConn{
		executeQueryFunc: func(_ context.Context, qry *Query) *Iter {
			fetchedQry = qry
			return &Iter{
				framer:  nextPageFramer,
				numRows: 1,
				next:    newNextIter(qry, 1),
			}
		},
	}

	baseQry := newWarningTestQuery().WithContext(rootCtx)
	baseQry.conn = conn
	currentFramer := &testWarningFramer{warnings: []string{"current"}}
	iter := &Iter{
		framer:  currentFramer,
		numRows: 1,
		pos:     1,
		next:    newNextIter(baseQry, 1),
	}
	defer iter.Close()

	if !iter.fetchNextPage() {
		t.Fatal("expected next page fetch to succeed")
	}
	if fetchedQry == nil {
		t.Fatal("expected next-page query to execute")
	}
	select {
	case <-fetchedQry.Context().Done():
	default:
		t.Fatal("expected consumed next-page context to be canceled")
	}
	select {
	case <-iter.next.qry.Context().Done():
		t.Fatal("expected following page context to remain active")
	default:
	}
	if !currentFramer.released {
		t.Fatal("expected current page framer to be released")
	}
	if iter.framer != nextPageFramer {
		t.Fatal("expected fetched page framer to become current")
	}
}

func TestQueryIterManualPagingDefersHiddenEmptyPageWarnings(t *testing.T) {
	t.Parallel()

	handler := &recordingWarningHandler{}
	firstFramer := &testWarningFramer{warnings: []string{"empty-page"}}
	finalFramer := &testWarningFramer{warnings: []string{"final-page"}}
	baseQry := newWarningTestQuery()
	baseQry.refCount = 1
	baseQry.PageState([]byte("initial"))

	call := 0
	baseQry.conn = &pagingTestConn{
		executeQueryFunc: func(_ context.Context, qry *Query) *Iter {
			call++
			switch call {
			case 1:
				if !slices.Equal(qry.pageState, []byte("initial")) {
					t.Fatalf("first page state = %q, want %q", qry.pageState, []byte("initial"))
				}
				return (&Iter{
					framer:  firstFramer,
					numRows: 0,
					meta: resultMetadata{
						pagingState: []byte("next"),
					},
				}).bindWarningHandler(qry, handler)
			case 2:
				if !slices.Equal(qry.pageState, []byte("next")) {
					t.Fatalf("second page state = %q, want %q", qry.pageState, []byte("next"))
				}
				return (&Iter{
					framer:  finalFramer,
					numRows: 1,
				}).bindWarningHandler(qry, handler)
			default:
				t.Fatalf("unexpected executeQuery call %d", call)
				return nil
			}
		},
	}

	iter := baseQry.Iter()

	if call != 2 {
		t.Fatalf("executeQuery call count = %d, want 2", call)
	}
	if handler.calls != 0 {
		t.Fatalf("handler call count before Close = %d, want 0", handler.calls)
	}
	if !firstFramer.released {
		t.Fatal("hidden empty-page framer was not released")
	}
	if warnings := iter.Warnings(); !slices.Equal(warnings, []string{"empty-page", "final-page"}) {
		t.Fatalf("Warnings() = %v, want %v", warnings, []string{"empty-page", "final-page"})
	}

	if err := iter.Close(); err != nil {
		t.Fatalf("Close() returned unexpected error: %v", err)
	}
	if handler.calls != 1 {
		t.Fatalf("handler call count after Close = %d, want 1", handler.calls)
	}
	if !slices.Equal(handler.warnings, []string{"empty-page", "final-page"}) {
		t.Fatalf("handler warnings = %v, want %v", handler.warnings, []string{"empty-page", "final-page"})
	}
}

func TestQueryIterManualPagingPreservesHiddenWarningsOnTerminalError(t *testing.T) {
	t.Parallel()

	handler := &recordingWarningHandler{}
	firstFramer := &testWarningFramer{warnings: []string{"empty-page"}}
	baseQry := newWarningTestQuery()
	baseQry.refCount = 1
	baseQry.PageState([]byte("initial"))

	call := 0
	baseQry.conn = &pagingTestConn{
		executeQueryFunc: func(_ context.Context, qry *Query) *Iter {
			call++
			switch call {
			case 1:
				if !slices.Equal(qry.pageState, []byte("initial")) {
					t.Fatalf("first page state = %q, want %q", qry.pageState, []byte("initial"))
				}
				return (&Iter{
					framer:  firstFramer,
					numRows: 0,
					meta: resultMetadata{
						pagingState: []byte("next"),
					},
				}).bindWarningHandler(qry, handler)
			case 2:
				if !slices.Equal(qry.pageState, []byte("next")) {
					t.Fatalf("second page state = %q, want %q", qry.pageState, []byte("next"))
				}
				return newErrorIterWithReleasedFramer(errors.New("boom"), &testWarningFramer{
					warnings: []string{"final-error"},
				}).bindWarningHandler(qry, handler)
			default:
				t.Fatalf("unexpected executeQuery call %d", call)
				return nil
			}
		},
	}

	iter := baseQry.Iter()

	if call != 2 {
		t.Fatalf("executeQuery call count = %d, want 2", call)
	}
	if !firstFramer.released {
		t.Fatal("hidden empty-page framer was not released")
	}
	if handler.calls != 1 {
		t.Fatalf("handler call count after Iter = %d, want 1", handler.calls)
	}
	if !slices.Equal(handler.warnings, []string{"empty-page", "final-error"}) {
		t.Fatalf("handler warnings = %v, want %v", handler.warnings, []string{"empty-page", "final-error"})
	}
	if warnings := iter.Warnings(); !slices.Equal(warnings, []string{"empty-page", "final-error"}) {
		t.Fatalf("Warnings() = %v, want %v", warnings, []string{"empty-page", "final-error"})
	}
	if err := iter.Close(); err == nil || err.Error() != "boom" {
		t.Fatalf("Close() = %v, want boom", err)
	}
}

func TestTableTabletsMetadata(t *testing.T) {
	t.Parallel()

	t.Run("HappyPath", func(t *testing.T) {
		t.Parallel()

		ctrl := &schemaDataMock{knownKeyspaces: map[string][]tableInfo{}}
		s := newSchemaEventTestSessionWithMock(ctrl)
		defer s.Close()
		s.isInitialized = true
		s.tabletsRoutingV1 = true

		addTestTablets(t, s, "test_ks", "tbl_a")

		entries, err := s.TableTabletsMetadata("test_ks", "tbl_a")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(entries) != 2 {
			t.Fatalf("expected 2 tablet entries, got %d", len(entries))
		}
	})

	t.Run("ClosedSession", func(t *testing.T) {
		t.Parallel()

		ctrl := &schemaDataMock{knownKeyspaces: map[string][]tableInfo{}}
		s := newSchemaEventTestSessionWithMock(ctrl)
		defer s.Close()
		s.isInitialized = true
		s.tabletsRoutingV1 = true
		s.isClosed = true

		_, err := s.TableTabletsMetadata("ks", "tb")
		if !errors.Is(err, ErrSessionClosed) {
			t.Fatalf("expected ErrSessionClosed, got %v", err)
		}
	})

	t.Run("NotReady", func(t *testing.T) {
		t.Parallel()

		ctrl := &schemaDataMock{knownKeyspaces: map[string][]tableInfo{}}
		s := newSchemaEventTestSessionWithMock(ctrl)
		defer s.Close()
		s.tabletsRoutingV1 = true

		_, err := s.TableTabletsMetadata("ks", "tb")
		if !errors.Is(err, ErrSessionNotReady) {
			t.Fatalf("expected ErrSessionNotReady, got %v", err)
		}
	})

	t.Run("TabletsNotEnabled", func(t *testing.T) {
		t.Parallel()

		ctrl := &schemaDataMock{knownKeyspaces: map[string][]tableInfo{}}
		s := newSchemaEventTestSessionWithMock(ctrl)
		defer s.Close()
		s.isInitialized = true

		_, err := s.TableTabletsMetadata("ks", "tb")
		if !errors.Is(err, ErrTabletsNotUsed) {
			t.Fatalf("expected ErrTabletsNotUsed, got %v", err)
		}
	})

	t.Run("EmptyKeyspace", func(t *testing.T) {
		t.Parallel()

		ctrl := &schemaDataMock{knownKeyspaces: map[string][]tableInfo{}}
		s := newSchemaEventTestSessionWithMock(ctrl)
		defer s.Close()
		s.isInitialized = true
		s.tabletsRoutingV1 = true

		_, err := s.TableTabletsMetadata("", "tb")
		if !errors.Is(err, ErrNoKeyspace) {
			t.Fatalf("expected ErrNoKeyspace, got %v", err)
		}
	})

	t.Run("EmptyTable", func(t *testing.T) {
		t.Parallel()

		ctrl := &schemaDataMock{knownKeyspaces: map[string][]tableInfo{}}
		s := newSchemaEventTestSessionWithMock(ctrl)
		defer s.Close()
		s.isInitialized = true
		s.tabletsRoutingV1 = true

		_, err := s.TableTabletsMetadata("ks", "")
		if !errors.Is(err, ErrNoTable) {
			t.Fatalf("expected ErrNoTable, got %v", err)
		}
	})

	t.Run("NoData", func(t *testing.T) {
		t.Parallel()

		ctrl := &schemaDataMock{knownKeyspaces: map[string][]tableInfo{}}
		s := newSchemaEventTestSessionWithMock(ctrl)
		defer s.Close()
		s.isInitialized = true
		s.tabletsRoutingV1 = true

		entries, err := s.TableTabletsMetadata("ks", "nonexistent")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if entries != nil {
			t.Fatalf("expected nil for nonexistent table, got %d entries", len(entries))
		}
	})
}

func TestForEachTablet(t *testing.T) {
	t.Parallel()

	t.Run("HappyPath", func(t *testing.T) {
		t.Parallel()

		ctrl := &schemaDataMock{knownKeyspaces: map[string][]tableInfo{}}
		s := newSchemaEventTestSessionWithMock(ctrl)
		defer s.Close()
		s.isInitialized = true
		s.tabletsRoutingV1 = true

		addTestTablets(t, s, "ks1", "tbl_a")
		addTestTablets(t, s, "ks2", "tbl_b")

		visited := make(map[string]int)
		err := s.ForEachTablet(func(keyspace, table string, entries tablets.TabletEntryList) bool {
			visited[keyspace+"."+table] = len(entries)
			return true
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(visited) != 2 {
			t.Fatalf("expected 2 tables visited, got %d", len(visited))
		}
		if visited["ks1.tbl_a"] != 2 {
			t.Fatalf("expected 2 entries for ks1.tbl_a, got %d", visited["ks1.tbl_a"])
		}
		if visited["ks2.tbl_b"] != 2 {
			t.Fatalf("expected 2 entries for ks2.tbl_b, got %d", visited["ks2.tbl_b"])
		}
	})

	t.Run("EarlyStop", func(t *testing.T) {
		t.Parallel()

		ctrl := &schemaDataMock{knownKeyspaces: map[string][]tableInfo{}}
		s := newSchemaEventTestSessionWithMock(ctrl)
		defer s.Close()
		s.isInitialized = true
		s.tabletsRoutingV1 = true

		addTestTablets(t, s, "ks1", "tbl_a")
		addTestTablets(t, s, "ks2", "tbl_b")

		count := 0
		err := s.ForEachTablet(func(keyspace, table string, entries tablets.TabletEntryList) bool {
			count++
			return false
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if count != 1 {
			t.Fatalf("expected 1 callback invocation, got %d", count)
		}
	})

	t.Run("ClosedSession", func(t *testing.T) {
		t.Parallel()

		ctrl := &schemaDataMock{knownKeyspaces: map[string][]tableInfo{}}
		s := newSchemaEventTestSessionWithMock(ctrl)
		defer s.Close()
		s.isInitialized = true
		s.tabletsRoutingV1 = true
		s.isClosed = true

		err := s.ForEachTablet(func(keyspace, table string, entries tablets.TabletEntryList) bool {
			t.Fatal("callback should not be called on closed session")
			return true
		})
		if !errors.Is(err, ErrSessionClosed) {
			t.Fatalf("expected ErrSessionClosed, got %v", err)
		}
	})

	t.Run("TabletsNotEnabled", func(t *testing.T) {
		t.Parallel()

		ctrl := &schemaDataMock{knownKeyspaces: map[string][]tableInfo{}}
		s := newSchemaEventTestSessionWithMock(ctrl)
		defer s.Close()
		s.isInitialized = true

		err := s.ForEachTablet(func(keyspace, table string, entries tablets.TabletEntryList) bool {
			t.Fatal("callback should not be called when tablets not enabled")
			return true
		})
		if !errors.Is(err, ErrTabletsNotUsed) {
			t.Fatalf("expected ErrTabletsNotUsed, got %v", err)
		}
	})

	t.Run("NilCallback", func(t *testing.T) {
		t.Parallel()

		ctrl := &schemaDataMock{knownKeyspaces: map[string][]tableInfo{}}
		s := newSchemaEventTestSessionWithMock(ctrl)
		defer s.Close()
		s.isInitialized = true
		s.tabletsRoutingV1 = true

		addTestTablets(t, s, "ks", "tb")

		err := s.ForEachTablet(nil)
		if err != nil {
			t.Fatalf("expected nil error for nil callback, got %v", err)
		}
	})
}

func TestFindTabletReplicasUnsafeForToken(t *testing.T) {
	t.Parallel()

	t.Run("NilMetadataDescriber", func(t *testing.T) {
		t.Parallel()

		s := &Session{}
		s.metadataDescriber = nil

		result := s.findTabletReplicasUnsafeForToken("ks", "tb", 42)
		if result != nil {
			t.Fatalf("expected nil replicas for nil metadataDescriber, got %v", result)
		}
	})

	t.Run("NilMetadata", func(t *testing.T) {
		t.Parallel()

		s := &Session{}
		s.metadataDescriber = &metadataDescriber{
			session:  s,
			metadata: nil,
		}

		result := s.findTabletReplicasUnsafeForToken("ks", "tb", 42)
		if result != nil {
			t.Fatalf("expected nil replicas for nil metadata, got %v", result)
		}
	})

	t.Run("ClosedSession", func(t *testing.T) {
		t.Parallel()

		ctrl := &schemaDataMock{knownKeyspaces: map[string][]tableInfo{}}
		s := newSchemaEventTestSessionWithMock(ctrl)
		defer s.Close()
		s.isInitialized = true
		s.isClosed = true

		result := s.findTabletReplicasUnsafeForToken("ks", "tb", 42)
		if result != nil {
			t.Fatalf("expected nil replicas for closed session, got %v", result)
		}
	})
}

func TestTableMetadataValidation(t *testing.T) {
	t.Parallel()

	t.Run("EmptyTableReturnsErrNoTable", func(t *testing.T) {
		t.Parallel()

		ctrl := &schemaDataMock{knownKeyspaces: map[string][]tableInfo{}}
		s := newSchemaEventTestSessionWithMock(ctrl)
		defer s.Close()
		s.isInitialized = true

		_, err := s.TableMetadata("ks", "")
		if !errors.Is(err, ErrNoTable) {
			t.Fatalf("TableMetadata: expected ErrNoTable, got %v", err)
		}
	})
}
