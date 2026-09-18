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
	"encoding/binary"
	"errors"
	"fmt"
	"reflect"
	"runtime"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/gocql/gocql/internal/lru"
	"github.com/gocql/gocql/tablets"
)

func TestQueryIterReturnsErrSessionNotReady(t *testing.T) {
	t.Parallel()

	q := &Query{
		session:     &Session{},
		routingInfo: &queryRoutingInfo{},
		metrics:     newQueryMetrics(),
	}
	if err := q.Iter().Close(); !errors.Is(err, ErrSessionNotReady) {
		t.Fatalf("query error = %v, want %v", err, ErrSessionNotReady)
	}
}

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

	cluster := testCluster(defaultProto, addresses[0], addresses[1], addresses[2])
	cluster.Port = srv.port()
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
	calls         int
	lastHost      *HostInfo
	lastQry       ExecutableQuery
	queryStmt     string
	queryAttempts int
	queryMetrics  *queryMetrics
	warnings      []string
}

func (h *recordingWarningHandler) HandleWarnings(qry ExecutableQuery, host *HostInfo, warnings []string) {
	h.calls++
	h.lastQry = qry
	h.lastHost = host
	if query, ok := qry.(*Query); ok {
		h.queryStmt = query.stmt
		h.queryAttempts = query.Attempts()
		h.queryMetrics = query.metrics
	}
	h.warnings = slices.Clone(warnings)
}

func TestIterWarningHandlerPinsExactExecutionMetrics(t *testing.T) {
	t.Parallel()

	q := newWarningTestQuery()
	prepareQueryMetrics(&q.metrics, &q.metricsOwner)
	executionMetrics := q.metrics
	finishUnobservedTestAttempt(executionMetrics, time.Nanosecond)
	executionQuery := cloneQuery(q, executionMetrics)
	handler := &recordingWarningHandler{}
	iter := (&Iter{
		framer: &testWarningFramer{warnings: []string{"pinned"}},
	}).bindWarningHandler(executionQuery, handler)

	if refs := executionMetrics.refs.Load(); refs != 2 {
		t.Fatalf("execution metrics references = %d, want owner plus warning iterator (2)", refs)
	}
	q.reset()
	if q.metrics == executionMetrics {
		t.Fatal("source query did not detach from warning-pinned execution metrics")
	}
	if refs := executionMetrics.refs.Load(); refs != 1 {
		t.Fatalf("execution metrics references after source reset = %d, want warning iterator only", refs)
	}

	if err := iter.Close(); err != nil {
		t.Fatalf("Close returned error: %v", err)
	}
	if handler.calls != 1 || handler.lastQry.Attempts() != 1 {
		t.Fatalf("warning handler calls/attempts = (%d,%d), want (1,1)", handler.calls, handler.lastQry.Attempts())
	}
	if refs := executionMetrics.refs.Load(); refs != 0 {
		t.Fatalf("execution metrics references after warning dispatch = %d, want 0", refs)
	}
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
	executeQueryFunc            func(ctx context.Context, qry *Query) *Iter
	executeQueryWithMetricsFunc func(ctx context.Context, qry *Query, metrics *queryMetrics) *Iter
}

func (*pagingTestConn) Close() {}
func (*pagingTestConn) exec(context.Context, frameBuilder, Tracer, time.Duration) (*framer, error) {
	return nil, nil
}
func (*pagingTestConn) awaitSchemaAgreement(context.Context) error { return nil }
func (c *pagingTestConn) executeQuery(ctx context.Context, qry *Query) *Iter {
	return c.executeQueryFunc(ctx, qry)
}
func (c *pagingTestConn) executeQueryWithMetrics(
	ctx context.Context, qry *Query, metrics *queryMetrics,
) *Iter {
	if c.executeQueryWithMetricsFunc != nil {
		return c.executeQueryWithMetricsFunc(ctx, qry, metrics)
	}
	return c.executeQuery(ctx, qry)
}
func (*pagingTestConn) querySystem(context.Context, string, ...any) *Iter { return nil }
func (*pagingTestConn) getIsSchemaV2() bool                               { return false }
func (*pagingTestConn) setSchemaV2(bool)                                  {}
func (*pagingTestConn) isScyllaConn() bool                                { return false }
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

type consistencyRetryPolicy struct {
	consistency Consistency
}

func (p *consistencyRetryPolicy) Attempt(q RetryableQuery) bool {
	if q.Attempts() != 1 {
		return false
	}
	q.SetConsistency(p.consistency)
	return true
}

func (*consistencyRetryPolicy) GetRetryType(error) RetryType {
	return Retry
}

type synchronizedRetryPolicy struct {
	maxRetries int
	entered    atomic.Int32
	allEntered chan struct{}
}

func (p *synchronizedRetryPolicy) Attempt(q RetryableQuery) bool {
	if p.entered.Add(1) == 2 {
		close(p.allEntered)
	}
	<-p.allEntered
	return q.Attempts() <= p.maxRetries
}

func (*synchronizedRetryPolicy) GetRetryType(error) RetryType {
	return Retry
}

type executorTestQuery struct {
	ctx               context.Context
	rt                RetryPolicy
	spec              SpeculativeExecutionPolicy
	idempotent        bool
	consistency       Consistency
	hostID            string
	attempts          atomic.Int32
	borrowed          atomic.Int32
	released          atomic.Int32
	consistencyWrites atomic.Int32
	executeFunc       func(context.Context, *Conn) *Iter
	finishFunc        func(attemptToken, time.Time, *Iter, *HostInfo)
}

func (q *executorTestQuery) borrowForExecution() {
	q.borrowed.Add(1)
}

func (q *executorTestQuery) releaseAfterExecution() {
	q.released.Add(1)
}

func (q *executorTestQuery) execute(ctx context.Context, conn *Conn, _ *queryMetrics) *Iter {
	return q.executeFunc(ctx, conn)
}

func (q *executorTestQuery) finishAttempt(token attemptToken, _ string, end time.Time, iter *Iter, host *HostInfo) {
	if q.finishFunc != nil {
		q.finishFunc(token, end, iter, host)
		return
	}
	defer token.metrics.release()
	token.metrics.finishAttempt(token, end.Sub(token.start), host, false, false)
	q.attempts.Add(1)
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
func (q *executorTestQuery) GetHostID() string { return q.hostID }

func (q *executorTestQuery) withContext(ctx context.Context) ExecutableQuery {
	q2 := &executorTestQuery{
		ctx:         ctx,
		rt:          q.rt,
		spec:        q.spec,
		idempotent:  q.idempotent,
		consistency: q.consistency,
		hostID:      q.hostID,
		executeFunc: q.executeFunc,
		finishFunc:  q.finishFunc,
	}
	q2.attempts.Store(q.attempts.Load())
	q2.borrowed.Store(q.borrowed.Load())
	q2.released.Store(q.released.Load())
	return q2
}

func (q *executorTestQuery) Attempts() int {
	return int(q.attempts.Load())
}

func (q *executorTestQuery) SetConsistency(c Consistency) {
	q.consistencyWrites.Add(1)
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
	policy := RoundRobinHostPolicy()
	policy.AddHost(host)
	return &queryExecutor{
		pool: &policyConnPool{
			hostConnPools: map[UUID]*hostConnPool{
				host.hostUUID(): &hostConnPool{
					host:       host,
					connPicker: staticConnPicker{conn: &Conn{host: host}},
				},
			},
		},
		policy: policy,
	}
}

func TestQueryMetricsRecordHostAdjustmentTracksTotalsAndHosts(t *testing.T) {
	t.Parallel()

	qm := newQueryMetrics()
	host1 := &HostInfo{hostId: UUID{1}}
	host2 := &HostInfo{hostId: UUID{2}}

	qm.recordHostAdjustment(1, 10*time.Nanosecond, host1)
	if qm.host.Attempts != 1 || qm.host.TotalLatency != 10 {
		t.Fatalf("first host metrics = %+v, want attempts=1 latency=10", qm.host)
	}
	if got := qm.attempts(); got != 1 {
		t.Fatalf("attempts = %d, want 1", got)
	}
	if got := qm.latency(); got != 10 {
		t.Fatalf("latency = %d, want 10", got)
	}

	qm.recordHostAdjustment(2, 20*time.Nanosecond, host1)
	if qm.host.Attempts != 3 || qm.host.TotalLatency != 30 {
		t.Fatalf("updated host metrics = %+v, want attempts=3 latency=30", qm.host)
	}
	if qm.extra != nil {
		t.Fatal("extra host metrics map allocated for one host")
	}

	qm.recordHostAdjustment(1, 6*time.Nanosecond, host2)
	metrics := qm.extra[host2.hostUUID()]
	if metrics.Attempts != 1 || metrics.TotalLatency != 6 {
		t.Fatalf("second host metrics = %+v, want attempts=1 latency=6", metrics)
	}
	if qm.extra == nil {
		t.Fatal("extra host metrics map not allocated for second host")
	}
	if got := qm.attempts(); got != 4 {
		t.Fatalf("attempts = %d, want 4", got)
	}
	if got := qm.latency(); got != 9 {
		t.Fatalf("latency = %d, want 9", got)
	}
	if got := qm.nextAttempt.Load(); got != 4 {
		t.Fatalf("next attempt = %d, want 4", got)
	}

	qm.reset()
	if got := qm.attempts(); got != 0 {
		t.Fatalf("attempts after reset = %d, want 0", got)
	}
	if got := qm.latency(); got != 0 {
		t.Fatalf("latency after reset = %d, want 0", got)
	}
}

func TestQueryMetricsTotalsDoNotClampAtPackedLimits(t *testing.T) {
	t.Parallel()

	const (
		largeAttempts = 1<<20 + 1
		largeLatency  = int64(1<<44) + 7
	)
	qm := preFilledQueryMetrics(map[UUID]*hostMetrics{
		{1}: {Attempts: largeAttempts, TotalLatency: largeLatency},
	})

	if attempts, latency := qm.totalsSnapshot(); attempts != largeAttempts || latency != largeLatency {
		t.Fatalf("large totals = (%d,%d), want (%d,%d)", attempts, latency, largeAttempts, largeLatency)
	}

	const addLatency = int64(1<<44) + 11
	if previous := qm.addTotals(1, addLatency); previous != largeAttempts {
		t.Fatalf("previous attempts = %d, want %d", previous, largeAttempts)
	}
	if attempts, latency := qm.totalsSnapshot(); attempts != largeAttempts+1 || latency != largeLatency+addLatency {
		t.Fatalf("updated large totals = (%d,%d), want (%d,%d)",
			attempts, latency, largeAttempts+1, largeLatency+addLatency)
	}
}

func TestQueryMetricsTotalsConcurrentOverflowTransition(t *testing.T) {
	t.Parallel()

	const (
		workers        = 8
		addsPerWorker  = 20_000
		seedLatency    = int64(17)
		latencyPerAdd  = int64(3)
		totalAdditions = workers * addsPerWorker
	)

	seedAttempts := queryMetricsAttemptsMax - 1000
	qm := newQueryMetrics()
	if previous := qm.addTotals(int(seedAttempts), seedLatency); previous != 0 {
		t.Fatalf("seed previous attempts = %d, want 0", previous)
	}
	if packed := qm.totals.Load(); packed == queryMetricsFullTotals {
		t.Fatal("seed totals unexpectedly use full-width storage")
	}

	previousAttempts := make([]int64, totalAdditions)
	start := make(chan struct{})
	var ready sync.WaitGroup
	var writers sync.WaitGroup
	ready.Add(workers)
	for worker := range workers {
		offset := worker * addsPerWorker
		writers.Add(1)
		go func() {
			defer writers.Done()
			ready.Done()
			<-start
			for i := range addsPerWorker {
				previousAttempts[offset+i] = qm.addTotals(1, latencyPerAdd)
			}
		}()
	}
	ready.Wait()
	close(start)
	writers.Wait()

	wantAttempts := seedAttempts + totalAdditions
	wantLatency := seedLatency + int64(totalAdditions)*latencyPerAdd
	if attempts, latency := qm.totalsSnapshot(); attempts != wantAttempts || latency != wantLatency {
		t.Fatalf("concurrent totals = (%d,%d), want (%d,%d)",
			attempts, latency, wantAttempts, wantLatency)
	}
	if packed := qm.totals.Load(); packed != queryMetricsFullTotals {
		t.Fatalf("packed totals = %#x, want full-width sentinel %#x", packed, uint64(queryMetricsFullTotals))
	}

	seen := make([]bool, totalAdditions)
	for _, previous := range previousAttempts {
		offset := previous - seedAttempts
		if offset < 0 || offset >= int64(totalAdditions) {
			t.Fatalf("previous attempts = %d, want range [%d,%d)", previous, seedAttempts, wantAttempts)
		}
		if seen[int(offset)] {
			t.Fatalf("duplicate previous attempt index %d", previous)
		}
		seen[int(offset)] = true
	}
}

func TestQueryMetricsRecordHostAdjustmentKeepsEmptyHostIDSeparate(t *testing.T) {
	t.Parallel()

	qm := newQueryMetrics()
	emptyHostID := &HostInfo{}
	realHostID := &HostInfo{hostId: UUID{1}}

	qm.recordHostAdjustment(1, 10*time.Nanosecond, emptyHostID)
	qm.recordHostAdjustment(1, 6*time.Nanosecond, realHostID)

	if !qm.hostInitialized || !qm.hostID.IsEmpty() {
		t.Fatalf("primary host ID = %v initialized=%t, want empty initialized", qm.hostID, qm.hostInitialized)
	}
	if qm.host.Attempts != 1 || qm.host.TotalLatency != 10 {
		t.Fatalf("primary host metrics = %+v, want empty host metrics", qm.host)
	}
	if metrics := qm.extra[realHostID.hostUUID()]; metrics.Attempts != 1 || metrics.TotalLatency != 6 {
		t.Fatalf("extra real host metrics = %+v, want attempts=1 latency=6", metrics)
	}
}

func TestQueryMetricsAttemptWithoutSnapshotSkipsHostStorage(t *testing.T) {
	t.Parallel()

	qm := newQueryMetrics()
	token := qm.beginAttempt()
	defer token.metrics.release()
	attempt, metrics, _ := qm.finishAttempt(
		token, 10*time.Nanosecond, &HostInfo{hostId: UUID{1}}, false, false,
	)
	if attempt != 0 {
		t.Fatalf("attempt index = %d, want 0", attempt)
	}
	if metrics != nil {
		t.Fatalf("metrics = %+v, want nil", metrics)
	}
	if got := qm.attempts(); got != 1 {
		t.Fatalf("attempts = %d, want 1", got)
	}
	if got := qm.latency(); got != 10 {
		t.Fatalf("latency = %d, want 10", got)
	}
	if qm.hostInitialized || !qm.hostID.IsEmpty() || qm.host != (hostMetrics{}) {
		t.Fatal("host metrics were stored without snapshot request")
	}
	if qm.extra != nil {
		t.Fatal("extra host metrics map allocated without snapshot request")
	}
}

func TestQueryMetricsObservedAttemptAllocatesOneImmutableHostSnapshot(t *testing.T) {
	const runs = 1000
	host := &HostInfo{hostId: UUID{1}}
	qm := newQueryMetrics()
	var snapshot *hostMetrics
	var attemptMetrics AttemptMetrics

	allocs := testing.AllocsPerRun(runs, func() {
		qm.reset()
		token := qm.beginAttempt()
		_, snapshot, attemptMetrics = qm.finishAttempt(token, time.Nanosecond, host, true, true)
		token.metrics.release()
		if snapshot == nil || snapshot.Attempts != 1 || snapshot.TotalLatency != 1 ||
			attemptMetrics.Attempts() != 1 {
			panic("unexpected host metrics snapshot")
		}
	})
	// Metrics is a retainable pointer with no release callback. A distinct heap
	// object is therefore required for every immutable observer snapshot.
	if allocs != 1 {
		t.Fatalf("first observed attempt allocations = %f, want 1", allocs)
	}
}

func TestQueryMetricsObservedSnapshotsRemainImmutableAcrossResets(t *testing.T) {
	const runs = 32
	host1 := &HostInfo{hostId: UUID{1}}
	host2 := &HostInfo{hostId: UUID{2}}
	qm := newQueryMetrics()
	retained := make([]*hostMetrics, 0, runs)
	retainedAttempts := make([]AttemptMetrics, 0, runs)
	for i := 0; i < runs; i++ {
		qm.reset()
		host := host1
		if i%2 != 0 {
			host = host2
		}
		token := qm.beginAttempt()
		_, snapshot, attemptMetrics := qm.finishAttempt(token, time.Duration(i+1)*time.Nanosecond, host, true, true)
		token.metrics.release()
		retained = append(retained, snapshot)
		retainedAttempts = append(retainedAttempts, attemptMetrics)
	}

	for i, snapshot := range retained {
		if snapshot == nil || snapshot.Attempts != 1 || snapshot.TotalLatency != int64(i+1) {
			t.Fatalf("retained snapshot %d = %+v, want attempts=1 latency=%d", i, snapshot, i+1)
		}
		expectedHost := host1
		if i%2 != 0 {
			expectedHost = host2
		}
		assertSingleAttemptMetric(t, retainedAttempts[i], AttemptMetric{
			Attempt: 0,
			Host:    expectedHost,
			Latency: int64(i + 1),
		})
		for j := i + 1; j < len(retained); j++ {
			if snapshot == retained[j] {
				t.Fatalf("snapshots %d and %d reused the same address", i, j)
			}
		}
	}
}

func TestQueryObserverDeprecatedMetricsIncludeManualHostUpdates(t *testing.T) {
	t.Parallel()

	host := &HostInfo{hostId: UUID{1}}
	start := time.Unix(0, 100)
	var observedQuery ObservedQueryWithAttemptMetrics
	var observed bool
	q := &Query{
		context: context.Background(),
		metrics: newQueryMetrics(),
		observer: unitQueryObserverWithAttemptMetricsFunc(func(
			_ context.Context, query ObservedQueryWithAttemptMetrics,
		) {
			observedQuery, observed = query, true
		}),
		routingInfo: &queryRoutingInfo{},
		stmt:        "SELECT v FROM tbl WHERE k = ?",
		values:      []any{1},
	}

	q.AddAttempts(2, host)
	q.AddLatency(30, host)
	finishTestAttempt(q, q.metrics, "ks", start.Add(10*time.Nanosecond), start, &Iter{}, host)

	if !observed {
		t.Fatal("query observation was not called")
	}
	if observedQuery.Metrics == nil || observedQuery.Metrics.Attempts != 3 || observedQuery.Metrics.TotalLatency != 40 {
		t.Fatalf("deprecated metrics = %+v, want attempts=3 latency=40", observedQuery.Metrics)
	}
	if observedQuery.Attempt != 2 || observedQuery.Host != host {
		t.Fatalf("query observation = %+v, want attempt=2 host=%p", observedQuery, host)
	}
	assertSingleAttemptMetric(t, observedQuery.AttemptMetrics, AttemptMetric{
		Attempt: 2,
		Host:    host,
		Latency: 10,
	})
}

func TestBatchObserverDeprecatedMetricsIncludeManualHostUpdates(t *testing.T) {
	t.Parallel()

	host := &HostInfo{hostId: UUID{1}}
	start := time.Unix(0, 200)
	var observedBatch ObservedBatchWithAttemptMetrics
	var observed bool
	b := &Batch{
		context: context.Background(),
		metrics: newQueryMetrics(),
		observer: unitBatchObserverWithAttemptMetricsFunc(func(
			_ context.Context, batch ObservedBatchWithAttemptMetrics,
		) {
			observedBatch, observed = batch, true
		}),
		routingInfo: &queryRoutingInfo{},
		Entries: []BatchEntry{
			{Stmt: "INSERT INTO tbl (k, v) VALUES (?, ?)", Args: []any{1, "v"}},
		},
	}

	b.AddAttempts(2, host)
	b.AddLatency(30, host)
	finishTestAttempt(b, b.metrics, "ks", start.Add(10*time.Nanosecond), start, &Iter{}, host)

	if !observed {
		t.Fatal("batch observation was not called")
	}
	if observedBatch.Metrics == nil || observedBatch.Metrics.Attempts != 3 || observedBatch.Metrics.TotalLatency != 40 {
		t.Fatalf("deprecated metrics = %+v, want attempts=3 latency=40", observedBatch.Metrics)
	}
	if observedBatch.Attempt != 2 || observedBatch.Host != host {
		t.Fatalf("batch observation = %+v, want attempt=2 host=%p", observedBatch, host)
	}
	assertSingleAttemptMetric(t, observedBatch.AttemptMetrics, AttemptMetric{
		Attempt: 2,
		Host:    host,
		Latency: 10,
	})
}

func TestQueryMetricsLatencyMakesProgressWithConcurrentAttempts(t *testing.T) {
	qm := newQueryMetrics()
	stop := make(chan struct{})
	var goroutines sync.WaitGroup
	t.Cleanup(func() {
		close(stop)
		goroutines.Wait()
	})
	for i := 0; i < min(4, runtime.GOMAXPROCS(0)); i++ {
		goroutines.Add(1)
		go func() {
			defer goroutines.Done()
			for {
				select {
				case <-stop:
					return
				default:
					finishUnobservedTestAttempt(qm, time.Nanosecond)
				}
			}
		}()
	}

	readsDone := make(chan error, 1)
	goroutines.Add(1)
	go func() {
		defer goroutines.Done()
		for i := 0; i < 1000; i++ {
			attempts, latency := qm.totalsSnapshot()
			if attempts < 0 || latency < 0 || latency != attempts {
				readsDone <- fmt.Errorf("incoherent totals attempts=%d latency=%d", attempts, latency)
				return
			}
			if got := qm.latency(); got != 0 && got != 1 {
				readsDone <- fmt.Errorf("average latency=%d, want 0 or 1", got)
				return
			}
		}
		readsDone <- nil
	}()

	select {
	case err := <-readsDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("metrics readers made no progress during sustained concurrent updates")
	}
}

func finishUnobservedTestAttempt(metrics *queryMetrics, latency time.Duration) (int, *hostMetrics) {
	token := metrics.beginAttempt()
	defer token.metrics.release()
	attempt, snapshot, _ := metrics.finishAttempt(token, latency, nil, false, false)
	return attempt, snapshot
}

func finishTestAttempt(
	q ExecutableQuery,
	metrics *queryMetrics,
	keyspace string,
	end, start time.Time,
	iter *Iter,
	host *HostInfo,
) {
	token := metrics.beginAttempt()
	token.start = start
	q.finishAttempt(token, keyspace, end, iter, host)
}

type unitQueryObserverFunc func(context.Context, ObservedQuery)

func (f unitQueryObserverFunc) ObserveQuery(ctx context.Context, q ObservedQuery) {
	f(ctx, q)
}

type unitQueryObserverWithAttemptMetricsFunc func(context.Context, ObservedQueryWithAttemptMetrics)

func (unitQueryObserverWithAttemptMetricsFunc) ObserveQuery(context.Context, ObservedQuery) {
	panic("legacy query observer callback used for extended observer")
}

func (f unitQueryObserverWithAttemptMetricsFunc) ObserveQueryWithAttemptMetrics(
	ctx context.Context, q ObservedQueryWithAttemptMetrics,
) {
	f(ctx, q)
}

type unitBatchObserverFunc func(context.Context, ObservedBatch)

func (f unitBatchObserverFunc) ObserveBatch(ctx context.Context, b ObservedBatch) {
	f(ctx, b)
}

type unitBatchObserverWithAttemptMetricsFunc func(context.Context, ObservedBatchWithAttemptMetrics)

func (unitBatchObserverWithAttemptMetricsFunc) ObserveBatch(context.Context, ObservedBatch) {
	panic("legacy batch observer callback used for extended observer")
}

func (f unitBatchObserverWithAttemptMetricsFunc) ObserveBatchWithAttemptMetrics(
	ctx context.Context, b ObservedBatchWithAttemptMetrics,
) {
	f(ctx, b)
}

func assertSingleAttemptMetric(t *testing.T, metrics AttemptMetrics, want AttemptMetric) {
	t.Helper()
	assertAttemptMetrics(t, metrics, []AttemptMetric{want})
}

func assertAttemptMetrics(t *testing.T, metrics AttemptMetrics, want []AttemptMetric) {
	t.Helper()

	if got := metrics.Attempts(); got != len(want) {
		t.Fatalf("attempt metrics attempts = %d, want %d", got, len(want))
	}
	var wantLatency int64
	for _, attempt := range want {
		wantLatency += attempt.Latency
	}
	if got := metrics.TotalLatency(); got != wantLatency {
		t.Fatalf("attempt metrics latency = %d, want %d", got, wantLatency)
	}

	var attempts []AttemptMetric
	metrics.ForEachAttempt(func(attempt AttemptMetric) bool {
		attempts = append(attempts, attempt)
		return true
	})
	if !slices.Equal(attempts, want) {
		t.Fatalf("attempt metrics entries = %+v, want %+v", attempts, want)
	}
}

func TestQueryObserverAttemptMetricsAndDeprecatedMetrics(t *testing.T) {
	t.Parallel()

	host := &HostInfo{hostId: UUID{1}}
	start := time.Unix(0, 100)
	var observations []ObservedQueryWithAttemptMetrics
	q := &Query{
		context: context.Background(),
		metrics: newQueryMetrics(),
		observer: unitQueryObserverWithAttemptMetricsFunc(func(
			_ context.Context, observed ObservedQueryWithAttemptMetrics,
		) {
			observations = append(observations, observed)
		}),
		routingInfo: &queryRoutingInfo{},
		stmt:        "SELECT v FROM tbl WHERE k = ?",
		values:      []any{1},
	}

	metrics := q.metrics
	finishTestAttempt(q, metrics, "ks", start.Add(10*time.Nanosecond), start, &Iter{numRows: 2}, host)
	finishTestAttempt(
		q,
		metrics,
		"ks",
		start.Add(30*time.Nanosecond),
		start.Add(10*time.Nanosecond),
		&Iter{numRows: 3},
		host,
	)

	if len(observations) != 2 {
		t.Fatalf("observations = %d, want 2", len(observations))
	}

	first := observations[0]
	if first.Metrics == nil || first.Metrics.Attempts != 1 || first.Metrics.TotalLatency != 10 {
		t.Fatalf("first deprecated metrics = %+v, want attempts=1 latency=10", first.Metrics)
	}
	if first.Attempt != 0 || first.Rows != 2 || first.Host != host {
		t.Fatalf("first observation = %+v, want attempt=0 rows=2 host=%p", first, host)
	}
	assertSingleAttemptMetric(t, first.AttemptMetrics, AttemptMetric{
		Attempt: 0,
		Host:    host,
		Latency: 10,
	})

	second := observations[1]
	if second.Metrics == nil || second.Metrics.Attempts != 2 || second.Metrics.TotalLatency != 30 {
		t.Fatalf("second deprecated metrics = %+v, want attempts=2 latency=30", second.Metrics)
	}
	if second.Attempt != 1 || second.Rows != 3 || second.Host != host {
		t.Fatalf("second observation = %+v, want attempt=1 rows=3 host=%p", second, host)
	}
	assertAttemptMetrics(t, second.AttemptMetrics, []AttemptMetric{
		{Attempt: 0, Host: host, Latency: 10},
		{Attempt: 1, Host: host, Latency: 20},
	})
}

func TestBatchObserverAttemptMetricsAndDeprecatedMetrics(t *testing.T) {
	t.Parallel()

	host := &HostInfo{hostId: UUID{1}}
	start := time.Unix(0, 200)
	var observedBatch ObservedBatchWithAttemptMetrics
	var observed bool
	b := &Batch{
		context: context.Background(),
		metrics: newQueryMetrics(),
		observer: unitBatchObserverWithAttemptMetricsFunc(func(
			_ context.Context, batch ObservedBatchWithAttemptMetrics,
		) {
			observedBatch, observed = batch, true
		}),
		routingInfo: &queryRoutingInfo{},
		Entries: []BatchEntry{
			{Stmt: "INSERT INTO tbl (k, v) VALUES (?, ?)", Args: []any{1, "v"}},
		},
	}

	finishTestAttempt(b, b.metrics, "ks", start.Add(12*time.Nanosecond), start, &Iter{}, host)

	if !observed {
		t.Fatal("batch observation was not called")
	}
	if observedBatch.Metrics == nil || observedBatch.Metrics.Attempts != 1 || observedBatch.Metrics.TotalLatency != 12 {
		t.Fatalf("deprecated metrics = %+v, want attempts=1 latency=12", observedBatch.Metrics)
	}
	if observedBatch.Attempt != 0 || observedBatch.Host != host || observedBatch.Keyspace != "ks" {
		t.Fatalf("batch observation = %+v, want attempt=0 host=%p keyspace=ks", observedBatch, host)
	}
	if len(observedBatch.Statements) != 1 || observedBatch.Statements[0] != "INSERT INTO tbl (k, v) VALUES (?, ?)" {
		t.Fatalf("batch statements = %+v, want inserted statement", observedBatch.Statements)
	}
	if len(observedBatch.Values) != 1 || !reflect.DeepEqual(observedBatch.Values[0], []any{1, "v"}) {
		t.Fatalf("batch values = %+v, want [[1 v]]", observedBatch.Values)
	}
	assertSingleAttemptMetric(t, observedBatch.AttemptMetrics, AttemptMetric{
		Attempt: 0,
		Host:    host,
		Latency: 12,
	})
}

func TestBatchObserverAttemptMetricsAreCumulative(t *testing.T) {
	t.Parallel()

	host1 := &HostInfo{hostId: UUID{1}}
	host2 := &HostInfo{hostId: UUID{2}}
	var observations []ObservedBatchWithAttemptMetrics
	b := &Batch{
		context: context.Background(),
		metrics: newQueryMetrics(),
		observer: unitBatchObserverWithAttemptMetricsFunc(func(
			_ context.Context, batch ObservedBatchWithAttemptMetrics,
		) {
			observations = append(observations, batch)
		}),
		routingInfo: &queryRoutingInfo{},
	}
	start := time.Unix(0, 200)

	metrics := b.metrics
	finishTestAttempt(b, metrics, "ks", start.Add(5*time.Nanosecond), start, &Iter{}, host1)
	finishTestAttempt(
		b,
		metrics,
		"ks",
		start.Add(12*time.Nanosecond),
		start.Add(5*time.Nanosecond),
		&Iter{},
		host2,
	)

	if len(observations) != 2 {
		t.Fatalf("observations = %d, want 2", len(observations))
	}
	assertSingleAttemptMetric(t, observations[0].AttemptMetrics, AttemptMetric{
		Attempt: 0,
		Host:    host1,
		Latency: 5,
	})
	assertAttemptMetrics(t, observations[1].AttemptMetrics, []AttemptMetric{
		{Attempt: 0, Host: host1, Latency: 5},
		{Attempt: 1, Host: host2, Latency: 7},
	})
	if observations[0].Metrics.Attempts != 1 || observations[1].Metrics.Attempts != 1 {
		t.Fatalf("deprecated per-host attempts = (%d,%d), want (1,1)",
			observations[0].Metrics.Attempts, observations[1].Metrics.Attempts)
	}
}

func TestQueryObserverAttemptMetricsConcurrentAttempts(t *testing.T) {
	t.Parallel()

	host := &HostInfo{hostId: UUID{1}}
	observed := make(chan ObservedQueryWithAttemptMetrics, 2)
	q := &Query{
		context: context.Background(),
		metrics: newQueryMetrics(),
		observer: unitQueryObserverWithAttemptMetricsFunc(func(
			_ context.Context, query ObservedQueryWithAttemptMetrics,
		) {
			observed <- query
		}),
		routingInfo: &queryRoutingInfo{},
		stmt:        "SELECT v FROM tbl WHERE k = ?",
		values:      []any{1},
	}

	metrics := q.metrics
	start := make(chan struct{})
	done := make(chan struct{}, 2)
	for _, latency := range []time.Duration{10 * time.Nanosecond, 20 * time.Nanosecond} {
		latency := latency
		go func() {
			<-start
			now := time.Unix(0, int64(latency))
			finishTestAttempt(q, metrics, "ks", now.Add(latency), now, &Iter{}, host)
			done <- struct{}{}
		}()
	}
	close(start)
	for i := 0; i < 2; i++ {
		<-done
	}

	observations := []ObservedQueryWithAttemptMetrics{<-observed, <-observed}
	slices.SortFunc(observations, func(a, b ObservedQueryWithAttemptMetrics) int {
		return a.AttemptMetrics.Attempts() - b.AttemptMetrics.Attempts()
	})
	seenAttempts := map[int]bool{}
	seenDeprecatedAttempts := map[int]bool{}
	var observedLatency int64
	for observationIndex, observation := range observations {
		seenAttempts[observation.Attempt] = true
		if observation.Metrics == nil {
			t.Fatal("deprecated metrics are nil")
		}
		seenDeprecatedAttempts[observation.Metrics.Attempts] = true

		var attempts []AttemptMetric
		observation.AttemptMetrics.ForEachAttempt(func(attempt AttemptMetric) bool {
			attempts = append(attempts, attempt)
			return true
		})
		if len(attempts) != observationIndex+1 {
			t.Fatalf("attempt metrics entries = %+v, want %d entries", attempts, observationIndex+1)
		}
		var totalLatency int64
		foundCurrent := false
		for i, attempt := range attempts {
			if i > 0 && attempts[i-1].Attempt >= attempt.Attempt {
				t.Fatalf("attempt metrics not in launch order: %+v", attempts)
			}
			if attempt.Host != host || attempt.Latency <= 0 {
				t.Fatalf("attempt metric = %+v, want host=%p and positive latency", attempt, host)
			}
			if attempt.Attempt == observation.Attempt {
				foundCurrent = true
				observedLatency += attempt.Latency
			}
			totalLatency += attempt.Latency
		}
		if !foundCurrent {
			t.Fatalf("snapshot %+v does not include current attempt %d", attempts, observation.Attempt)
		}
		if observation.AttemptMetrics.TotalLatency() != totalLatency {
			t.Fatalf("attempt metrics latency = %d, want %d", observation.AttemptMetrics.TotalLatency(), totalLatency)
		}
	}

	if !seenAttempts[0] || !seenAttempts[1] {
		t.Fatalf("observed attempts = %+v, want attempts 0 and 1", seenAttempts)
	}
	if !seenDeprecatedAttempts[1] || !seenDeprecatedAttempts[2] {
		t.Fatalf("deprecated metric attempts = %+v, want snapshots 1 and 2", seenDeprecatedAttempts)
	}
	if observedLatency != 30 {
		t.Fatalf("observed attempt latency = %d, want 30", observedLatency)
	}
	if got := q.metrics.attempts(); got != 2 {
		t.Fatalf("query attempts = %d, want 2", got)
	}
	if got := q.metrics.latency(); got != 15 {
		t.Fatalf("query latency = %d, want 15", got)
	}
}

func TestQueryObserverAttemptMetricsOutOfOrderCompletion(t *testing.T) {
	t.Parallel()

	host1 := &HostInfo{hostId: UUID{1}}
	host2 := &HostInfo{hostId: UUID{2}}
	observed := make(chan ObservedQueryWithAttemptMetrics, 2)
	q := &Query{
		context: context.Background(),
		metrics: newQueryMetrics(),
		observer: unitQueryObserverWithAttemptMetricsFunc(func(
			_ context.Context, query ObservedQueryWithAttemptMetrics,
		) {
			observed <- query
		}),
		routingInfo: &queryRoutingInfo{},
	}

	start := time.Unix(0, 100)
	firstLaunched := q.metrics.beginAttempt()
	firstLaunched.start = start
	secondLaunched := q.metrics.beginAttempt()
	secondLaunched.start = start

	q.finishAttempt(secondLaunched, "ks", start.Add(20*time.Nanosecond), &Iter{}, host2)
	firstSnapshot := (<-observed).AttemptMetrics
	assertAttemptMetrics(t, firstSnapshot, []AttemptMetric{
		{Attempt: 1, Host: host2, Latency: 20},
	})

	q.finishAttempt(firstLaunched, "ks", start.Add(10*time.Nanosecond), &Iter{}, host1)
	secondSnapshot := (<-observed).AttemptMetrics
	assertAttemptMetrics(t, secondSnapshot, []AttemptMetric{
		{Attempt: 0, Host: host1, Latency: 10},
		{Attempt: 1, Host: host2, Latency: 20},
	})

	// Completing another attempt must not mutate a retained earlier snapshot.
	assertAttemptMetrics(t, firstSnapshot, []AttemptMetric{
		{Attempt: 1, Host: host2, Latency: 20},
	})
}

func TestQueryMetricsExecutionGenerationIsolation(t *testing.T) {
	t.Parallel()

	host := &HostInfo{hostId: UUID{1}}
	observed := make(chan ObservedQueryWithAttemptMetrics, 2)
	q := &Query{
		context: context.Background(),
		metrics: newQueryMetrics(),
		observer: unitQueryObserverWithAttemptMetricsFunc(func(
			_ context.Context, query ObservedQueryWithAttemptMetrics,
		) {
			observed <- query
		}),
		routingInfo: &queryRoutingInfo{},
	}

	start := time.Unix(0, 100)
	oldMetrics := q.metrics
	oldToken := oldMetrics.beginAttempt()
	oldToken.start = start

	newMetrics := prepareQueryMetrics(&q.metrics, &q.metricsOwner)
	if newMetrics == oldMetrics {
		t.Fatal("new execution reused metrics still pinned by an old attempt")
	}
	newToken := newMetrics.beginAttempt()
	newToken.start = start
	q.finishAttempt(newToken, "ks", start.Add(7*time.Nanosecond), &Iter{}, host)
	newObservation := <-observed

	q.finishAttempt(oldToken, "ks", start.Add(11*time.Nanosecond), &Iter{}, host)
	oldObservation := <-observed

	if attempts, latency := q.metrics.totalsSnapshot(); attempts != 1 || latency != 7 {
		t.Fatalf("current execution totals = (%d,%d), want (1,7)", attempts, latency)
	}
	if attempts, latency := oldMetrics.totalsSnapshot(); attempts != 1 || latency != 11 {
		t.Fatalf("old execution totals = (%d,%d), want (1,11)", attempts, latency)
	}
	assertSingleAttemptMetric(t, newObservation.AttemptMetrics, AttemptMetric{
		Attempt: 0,
		Host:    host,
		Latency: 7,
	})
	assertSingleAttemptMetric(t, oldObservation.AttemptMetrics, AttemptMetric{
		Attempt: 0,
		Host:    host,
		Latency: 11,
	})
	if refs := oldMetrics.refs.Load(); refs != 0 {
		t.Fatalf("old execution references = %d, want 0", refs)
	}
}

func TestConnectionBoundQueryForwardsExplicitMetrics(t *testing.T) {
	t.Parallel()

	q := newWarningTestQuery()
	pinnedMetrics := newQueryMetrics()
	if q.metrics == pinnedMetrics {
		t.Fatal("test requires distinct field and execution metrics")
	}
	q.conn = &pagingTestConn{
		executeQueryFunc: func(context.Context, *Query) *Iter {
			t.Fatal("connection-bound query used the legacy metrics path")
			return nil
		},
		executeQueryWithMetricsFunc: func(
			_ context.Context,
			gotQuery *Query,
			gotMetrics *queryMetrics,
		) *Iter {
			if gotQuery != q {
				t.Fatal("connection-bound query forwarded a different Query")
			}
			if gotMetrics != pinnedMetrics {
				t.Fatal("connection-bound query did not forward its pinned metrics")
			}
			return &Iter{}
		},
	}

	if iter := q.executeQueryWithMetrics(pinnedMetrics); iter == nil {
		t.Fatal("connection-bound query returned a nil iterator")
	}
}

func TestNextIterFetchForwardsPinnedMetrics(t *testing.T) {
	t.Parallel()

	q := newWarningTestQuery()
	prepareQueryMetrics(&q.metrics, &q.metricsOwner)
	var pinnedMetrics *queryMetrics
	q.conn = &pagingTestConn{
		executeQueryFunc: func(context.Context, *Query) *Iter {
			t.Fatal("automatic page used the legacy metrics path")
			return nil
		},
		executeQueryWithMetricsFunc: func(
			_ context.Context,
			_ *Query,
			gotMetrics *queryMetrics,
		) *Iter {
			if gotMetrics != pinnedMetrics {
				t.Fatal("automatic page did not forward its pinned metrics")
			}
			return &Iter{}
		},
	}

	page := newNextIter(q, 1)
	pinnedMetrics = page.qry.metrics

	iter := page.fetch()
	if iter == nil {
		t.Fatal("automatic page returned a nil iterator")
	}
	page.consume()
	iter.Close()
	if refs := pinnedMetrics.refs.Load(); refs != 1 {
		t.Fatalf("pinned metrics references = %d, want only the root owner", refs)
	}
}

func TestBindWarningHandlerWithMetricsUsesExplicitMetrics(t *testing.T) {
	t.Parallel()

	q := newWarningTestQuery()
	metricsOwner := newWarningTestQuery()
	prepareQueryMetrics(&metricsOwner.metrics, &metricsOwner.metricsOwner)
	pinnedMetrics := metricsOwner.metrics
	finishUnobservedTestAttempt(pinnedMetrics, time.Nanosecond)
	handler := &recordingWarningHandler{}
	iter := (&Iter{
		framer: &testWarningFramer{warnings: []string{"pinned"}},
	}).bindWarningHandlerWithMetrics(q, pinnedMetrics, handler)

	if iter.warningMetrics != pinnedMetrics {
		t.Fatal("warning handler did not retain the explicit execution metrics")
	}
	if refs := pinnedMetrics.refs.Load(); refs != 2 {
		t.Fatalf("pinned metrics references while iterator is open = %d, want owner plus iterator (2)", refs)
	}
	iter.Close()
	if handler.calls != 1 {
		t.Fatalf("warning handler calls = %d, want 1", handler.calls)
	}
	if handler.queryMetrics != pinnedMetrics || handler.queryAttempts != 1 {
		t.Fatalf(
			"warning handler metrics = (%p,%d), want pinned execution (%p,1)",
			handler.queryMetrics,
			handler.queryAttempts,
			pinnedMetrics,
		)
	}
	if refs := pinnedMetrics.refs.Load(); refs != 1 {
		t.Fatalf("pinned metrics references after iterator close = %d, want owner only", refs)
	}
}

func TestQueryResetDetachesMetricsPinnedByAutomaticPage(t *testing.T) {
	t.Parallel()

	q := newWarningTestQuery()
	prepareQueryMetrics(&q.metrics, &q.metricsOwner)
	oldMetrics := q.metrics
	page := newNextIter(q, 1)
	if refs := oldMetrics.refs.Load(); refs != 2 {
		t.Fatalf("metrics references with page owner = %d, want 2", refs)
	}

	q.reset()
	if q.metrics == oldMetrics {
		t.Fatal("pooled query retained metrics still owned by an automatic page")
	}
	if page.qry.metrics != oldMetrics {
		t.Fatal("automatic page did not retain its original metrics run")
	}
	if refs := oldMetrics.refs.Load(); refs != 1 {
		t.Fatalf("old metrics references after query reset = %d, want page owner only", refs)
	}

	page.close()
	if refs := oldMetrics.refs.Load(); refs != 0 {
		t.Fatalf("old metrics references after page close = %d, want 0", refs)
	}
}

func TestNextIterReleasesExactMetricsRetainedBeforeQueryDetach(t *testing.T) {
	t.Parallel()

	q := newWarningTestQuery()
	prepareQueryMetrics(&q.metrics, &q.metricsOwner)
	retainedMetrics := q.metrics
	page := newNextIter(q, 1)
	if refs := retainedMetrics.refs.Load(); refs != 2 {
		t.Fatalf("metrics references with page owner = %d, want 2", refs)
	}

	// A reentrant use of the page Query can detach it to a new metrics run.
	// Closing the page must still release the exact run retained above.
	detachedMetrics := prepareQueryMetrics(&page.qry.metrics, &page.qry.metricsOwner)
	if detachedMetrics == retainedMetrics {
		t.Fatal("page query did not detach from the retained metrics run")
	}
	page.close()

	if refs := retainedMetrics.refs.Load(); refs != 1 {
		t.Fatalf("retained metrics references after page close = %d, want root owner only", refs)
	}
	if refs := detachedMetrics.refs.Load(); refs != 1 {
		t.Fatalf("detached metrics references after page close = %d, want page query owner", refs)
	}
}

func TestWithContextMetricsAreCopyOnWrite(t *testing.T) {
	t.Parallel()

	q := newWarningTestQuery()
	qCopy := q.WithContext(context.Background())
	if qCopy.metrics != q.metrics {
		t.Fatal("Query.WithContext eagerly allocated metrics instead of making a shallow copy")
	}
	if !q.metrics.ownerClaimed || q.metrics.ownerEpoch != qCopy.metricsOwner.epoch {
		t.Fatal("Query.WithContext did not transfer the copy-on-write owner")
	}
	originalMetrics := q.metrics
	prepareQueryMetrics(&q.metrics, &q.metricsOwner)
	if q.metrics == originalMetrics {
		t.Fatal("executing the source query did not detach from its WithContext copy")
	}
	finishUnobservedTestAttempt(qCopy.metrics, 7*time.Nanosecond)
	rawCopy := *qCopy
	prepareQueryMetrics(&rawCopy.metrics, &rawCopy.metricsOwner)
	if rawCopy.metrics == qCopy.metrics {
		t.Fatal("direct Query value copy did not detach on first execution")
	}
	if attempts, latency := qCopy.metrics.totalsSnapshot(); attempts != 1 || latency != 7 {
		t.Fatalf("direct Query value copy changed source totals = (%d,%d), want (1,7)",
			attempts, latency)
	}

	page := newNextIter(qCopy, 1)
	if page.qry.metrics != qCopy.metrics {
		t.Fatal("private automatic-page copy did not share the query metrics run")
	}
	page.close()

	b := &Batch{metrics: newQueryMetrics()}
	bCopy := b.WithContext(context.Background())
	if bCopy.metrics != b.metrics {
		t.Fatal("Batch.WithContext eagerly allocated metrics instead of making a shallow copy")
	}
	originalBatchMetrics := b.metrics
	prepareQueryMetrics(&b.metrics, &b.metricsOwner)
	if b.metrics == originalBatchMetrics {
		t.Fatal("executing the source batch did not detach from its WithContext copy")
	}
	finishUnobservedTestAttempt(bCopy.metrics, 11*time.Nanosecond)
	rawBatchCopy := *bCopy
	prepareQueryMetrics(&rawBatchCopy.metrics, &rawBatchCopy.metricsOwner)
	if rawBatchCopy.metrics == bCopy.metrics {
		t.Fatal("direct Batch value copy did not detach on first execution")
	}
	if attempts, latency := bCopy.metrics.totalsSnapshot(); attempts != 1 || latency != 11 {
		t.Fatalf("direct Batch value copy changed source totals = (%d,%d), want (1,11)",
			attempts, latency)
	}
}

func TestCloneQueryAccountsForEveryField(t *testing.T) {
	t.Parallel()

	// Keep this list in sync with the ordinary shallow-copy assignments in
	// cloneQuery. A newly added Query field must choose copy semantics here
	// instead of silently inheriting its zero value in speculative, paging,
	// retry, and WithContext clones.
	copied := map[string]struct{}{
		"trace":                      {},
		"context":                    {},
		"pageContextParent":          {},
		"spec":                       {},
		"rt":                         {},
		"conn":                       {},
		"observer":                   {},
		"metrics":                    {},
		"session":                    {},
		"customPayload":              {},
		"getKeyspace":                {},
		"routingInfo":                {},
		"binding":                    {},
		"keyspace":                   {},
		"nowInSecondsValue":          {},
		"hostID":                     {},
		"stmt":                       {},
		"routingKey":                 {},
		"values":                     {},
		"pageState":                  {},
		"pkMarshalCache":             {},
		"requestTimeout":             {},
		"defaultTimestampValue":      {},
		"prefetch":                   {},
		"pageSize":                   {},
		"cons":                       {},
		"serialCons":                 {},
		"disableAutoPage":            {},
		"deferReleasedErrorFinalize": {},
		"idempotent":                 {},
		"skipPrepare":                {},
		"disableSkipMetadata":        {},
		"defaultTimestamp":           {},
	}
	speciallyHandled := map[string]struct{}{
		"executionAttempts": {},
		"metricsOwner":      {},
		"refCount":          {},
		"prepareCache":      {},
	}

	queryType := reflect.TypeOf(Query{})
	for i := 0; i < queryType.NumField(); i++ {
		name := queryType.Field(i).Name
		_, isCopied := copied[name]
		_, isSpecial := speciallyHandled[name]
		if isCopied == isSpecial {
			t.Errorf("Query field %q must be in exactly one cloneQuery field list", name)
		}
		delete(copied, name)
		delete(speciallyHandled, name)
	}
	for name := range copied {
		t.Errorf("cloneQuery copied-field list contains unknown Query field %q", name)
	}
	for name := range speciallyHandled {
		t.Errorf("cloneQuery special-field list contains unknown Query field %q", name)
	}
}

func TestWithContextFromStaleValueDoesNotStealMetricsOwner(t *testing.T) {
	t.Parallel()

	q := newWarningTestQuery()
	prepareQueryMetrics(&q.metrics, &q.metricsOwner)
	staleQuery := *q
	currentQueryMetrics := prepareQueryMetrics(&q.metrics, &q.metricsOwner)
	finishUnobservedTestAttempt(currentQueryMetrics, 13*time.Nanosecond)

	queryCopy := staleQuery.WithContext(context.Background())
	prepareQueryMetrics(&queryCopy.metrics, &queryCopy.metricsOwner)
	if queryCopy.metrics == currentQueryMetrics {
		t.Fatal("WithContext from a stale Query value stole the current metrics owner")
	}
	if attempts, latency := currentQueryMetrics.totalsSnapshot(); attempts != 1 || latency != 13 {
		t.Fatalf("stale Query.WithContext changed current totals = (%d,%d), want (1,13)",
			attempts, latency)
	}

	b := &Batch{metrics: newQueryMetrics()}
	prepareQueryMetrics(&b.metrics, &b.metricsOwner)
	staleBatch := *b
	currentBatchMetrics := prepareQueryMetrics(&b.metrics, &b.metricsOwner)
	finishUnobservedTestAttempt(currentBatchMetrics, 17*time.Nanosecond)

	batchCopy := staleBatch.WithContext(context.Background())
	prepareQueryMetrics(&batchCopy.metrics, &batchCopy.metricsOwner)
	if batchCopy.metrics == currentBatchMetrics {
		t.Fatal("WithContext from a stale Batch value stole the current metrics owner")
	}
	if attempts, latency := currentBatchMetrics.totalsSnapshot(); attempts != 1 || latency != 17 {
		t.Fatalf("stale Batch.WithContext changed current totals = (%d,%d), want (1,17)",
			attempts, latency)
	}
}

func TestQueryResetReusesMetricsAllocationAndOwner(t *testing.T) {
	q := newWarningTestQuery()
	q.reset()

	metricsChanged := false
	ownerLost := false
	allocs := testing.AllocsPerRun(1000, func() {
		metrics := q.metrics
		q.reset()
		if q.metrics != metrics {
			metricsChanged = true
		}
		if q.metricsOwner.self != &q.metricsOwner ||
			!q.metrics.ownerClaimed ||
			q.metrics.ownerEpoch != q.metricsOwner.epoch {
			ownerLost = true
		}
	})
	if metricsChanged {
		t.Fatal("pooled Query reset replaced idle metrics storage")
	}
	if ownerLost {
		t.Fatal("pooled Query reset lost its metrics ownership marker")
	}
	if allocs != 1 {
		t.Fatalf("Query reset allocations = %f, want only the fresh routing info", allocs)
	}
}

func TestWithContextDoesNotAllocateMetrics(t *testing.T) {
	query := newWarningTestQuery()
	var queryCopy *Query
	queryAllocs := testing.AllocsPerRun(1000, func() {
		queryCopy = query.WithContext(context.Background())
	})
	if queryAllocs != 1 {
		t.Fatalf("Query.WithContext allocations = %f, want only the returned Query copy", queryAllocs)
	}
	if queryCopy == nil {
		t.Fatal("Query.WithContext returned nil")
	}

	batch := &Batch{metrics: newQueryMetrics()}
	var batchCopy *Batch
	batchAllocs := testing.AllocsPerRun(1000, func() {
		batchCopy = batch.WithContext(context.Background())
	})
	if batchAllocs != 1 {
		t.Fatalf("Batch.WithContext allocations = %f, want only the returned Batch copy", batchAllocs)
	}
	if batchCopy == nil {
		t.Fatal("Batch.WithContext returned nil")
	}
}

func TestQueryMetricsConcurrentValueCopiesClaimDifferentRuns(t *testing.T) {
	t.Parallel()

	base := Query{metrics: newQueryMetrics()}
	first := base
	second := base
	start := make(chan struct{})
	results := make(chan *queryMetrics, 2)

	go func() {
		<-start
		results <- prepareQueryMetrics(&first.metrics, &first.metricsOwner)
	}()
	go func() {
		<-start
		results <- prepareQueryMetrics(&second.metrics, &second.metricsOwner)
	}()
	close(start)

	firstMetrics := <-results
	secondMetrics := <-results
	if firstMetrics == secondMetrics {
		t.Fatal("concurrent Query value copies claimed the same metrics run")
	}
	if firstMetrics.refs.Load() != 1 || secondMetrics.refs.Load() != 1 {
		t.Fatalf("claimed references = (%d,%d), want one owner each",
			firstMetrics.refs.Load(), secondMetrics.refs.Load())
	}
}

func TestObservedMetricsSourceCompatibleLayout(t *testing.T) {
	t.Parallel()

	// Keep old positional literals compiling. These are valid in downstream
	// packages even though Metrics points at an unexported concrete type.
	_ = ObservedQuery{time.Time{}, time.Time{}, nil, nil, nil, "", "", nil, 0, 0}
	_ = ObservedBatch{time.Time{}, time.Time{}, nil, nil, nil, "", nil, nil, 0}

	assertFields := func(value any, want []string) {
		t.Helper()
		typ := reflect.TypeOf(value)
		if typ.NumField() != len(want) {
			t.Fatalf("%s field count = %d, want %d", typ, typ.NumField(), len(want))
		}
		for i, name := range want {
			if got := typ.Field(i).Name; got != name {
				t.Fatalf("%s field %d = %s, want %s", typ, i, got, name)
			}
		}
	}
	assertFields(ObservedQuery{}, []string{
		"Start", "End", "Err", "Host", "Metrics", "Keyspace", "Statement", "Values", "Rows", "Attempt",
	})
	assertFields(ObservedBatch{}, []string{
		"Start", "End", "Err", "Host", "Metrics", "Keyspace", "Statements", "Values", "Attempt",
	})

	if got := (ObservedQueryWithAttemptMetrics{}).AttemptMetrics.Attempts(); got != 0 {
		t.Fatalf("zero query attempt metrics count = %d, want 0", got)
	}
	if got := (ObservedBatchWithAttemptMetrics{}).AttemptMetrics.Attempts(); got != 0 {
		t.Fatalf("zero batch attempt metrics count = %d, want 0", got)
	}

	// Keep equality and formatting of the legacy metrics value unchanged.
	left := hostMetrics{Attempts: 1, TotalLatency: 2}
	right := hostMetrics{Attempts: 1, TotalLatency: 2}
	if left != right {
		t.Fatal("equal legacy host metrics compare unequal")
	}
	if got, want := fmt.Sprintf("%+v", left), "{Attempts:1 TotalLatency:2}"; got != want {
		t.Fatalf("legacy host metrics formatting = %q, want %q", got, want)
	}
}

func TestAttemptMetricsReportsRecordedAttempts(t *testing.T) {
	t.Parallel()

	host := &HostInfo{hostId: UUID{1}}
	qm := newQueryMetrics()

	first := qm.beginAttempt()
	_, firstHostMetrics, _ := qm.finishAttempt(first, 10*time.Nanosecond, host, true, false)
	first.metrics.release()
	if firstHostMetrics.Attempts != 1 || firstHostMetrics.TotalLatency != 10 {
		t.Fatalf("first host metrics = %+v, want attempts=1 latency=10", firstHostMetrics)
	}

	second := qm.beginAttempt()
	attempt, secondHostMetrics, _ := qm.finishAttempt(second, 20*time.Nanosecond, host, true, false)
	second.metrics.release()
	if secondHostMetrics.Attempts != 2 || secondHostMetrics.TotalLatency != 30 {
		t.Fatalf("second host metrics = %+v, want attempts=2 latency=30", secondHostMetrics)
	}
	if firstHostMetrics.Attempts != 1 || firstHostMetrics.TotalLatency != 10 {
		t.Fatalf("first host metrics mutated = %+v, want attempts=1 latency=10", firstHostMetrics)
	}

	attemptMetric := AttemptMetric{
		Attempt: attempt,
		Host:    host,
		Latency: 20,
	}
	metrics := newAttemptMetrics(attemptMetric)
	if metrics.Attempts() != 1 || metrics.TotalLatency() != 20 {
		t.Fatalf("attempt metrics = %+v, want attempts=1 latency=20", metrics)
	}

	var attempts []AttemptMetric
	metrics.ForEachAttempt(func(attempt AttemptMetric) bool {
		attempts = append(attempts, attempt)
		return true
	})
	if len(attempts) != 1 || attempts[0] != attemptMetric {
		t.Fatalf("attempt metrics entries = %+v, want [%+v]", attempts, attemptMetric)
	}

	twoAttempts := metrics.withAttempt(AttemptMetric{Attempt: attempt + 1, Host: host, Latency: 30})
	var stopped []AttemptMetric
	twoAttempts.ForEachAttempt(func(attempt AttemptMetric) bool {
		stopped = append(stopped, attempt)
		return false
	})
	if !slices.Equal(stopped, []AttemptMetric{attemptMetric}) {
		t.Fatalf("early-stop entries = %+v, want [%+v]", stopped, attemptMetric)
	}

	var zero AttemptMetrics
	if zero.Attempts() != 0 || zero.TotalLatency() != 0 {
		t.Fatalf("zero attempt metrics = %+v, want zero totals", zero)
	}
	var zeroAttempts []AttemptMetric
	zero.ForEachAttempt(func(attempt AttemptMetric) bool {
		zeroAttempts = append(zeroAttempts, attempt)
		return true
	})
	if len(zeroAttempts) != 0 {
		t.Fatalf("zero attempt metrics entries = %+v, want none", zeroAttempts)
	}
}

func TestAttemptMetricsIsIntentionallyNotComparable(t *testing.T) {
	t.Parallel()

	if reflect.TypeOf(AttemptMetrics{}).Comparable() {
		t.Fatal("AttemptMetrics is comparable; add an unexported non-comparable field to preserve representation freedom")
	}
}

func TestAttemptMetricsPersistentHistoryScalesAndStaysOrdered(t *testing.T) {
	t.Parallel()

	const attemptsCount = 1024
	var metrics AttemptMetrics
	var retained AttemptMetrics
	retainedOrdinals := make(map[int]bool)
	for i := 0; i < attemptsCount; i++ {
		ordinal := (i * 641) % attemptsCount
		metrics = metrics.withAttempt(AttemptMetric{
			Attempt: ordinal,
			Latency: int64(ordinal + 1),
		})
		if i == attemptsCount/2-1 {
			retained = metrics
			retained.ForEachAttempt(func(attempt AttemptMetric) bool {
				retainedOrdinals[attempt.Attempt] = true
				return true
			})
		}
	}

	if metrics.Attempts() != attemptsCount {
		t.Fatalf("attempt count = %d, want %d", metrics.Attempts(), attemptsCount)
	}
	if metrics.TotalLatency() != attemptsCount*(attemptsCount+1)/2 {
		t.Fatalf("total latency = %d, want %d", metrics.TotalLatency(), attemptsCount*(attemptsCount+1)/2)
	}
	if metrics.root == nil || metrics.root.height > 20 {
		t.Fatalf("persistent history height = %d, want balanced tree", attemptMetricNodeHeight(metrics.root))
	}
	ordinal := 0
	metrics.ForEachAttempt(func(attempt AttemptMetric) bool {
		if attempt.Attempt != ordinal {
			t.Fatalf("attempt ordinal = %d, want %d", attempt.Attempt, ordinal)
		}
		ordinal++
		return true
	})
	if ordinal != attemptsCount {
		t.Fatalf("iterated attempts = %d, want %d", ordinal, attemptsCount)
	}

	var retainedCount int
	retained.ForEachAttempt(func(attempt AttemptMetric) bool {
		if !retainedOrdinals[attempt.Attempt] {
			t.Fatalf("retained snapshot gained attempt %d", attempt.Attempt)
		}
		retainedCount++
		return true
	})
	if retainedCount != attemptsCount/2 {
		t.Fatalf("retained attempts = %d, want %d", retainedCount, attemptsCount/2)
	}
}

func TestQueryMetricsRecordHostAdjustmentSerializesTotalsWithHostMetrics(t *testing.T) {
	t.Parallel()

	qm := newQueryMetrics()
	host := &HostInfo{hostId: UUID{1}}

	qm.l.Lock()
	started := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)

		close(started)
		qm.recordHostAdjustment(1, 10*time.Nanosecond, host)
	}()

	<-started
	deadline := time.After(100 * time.Millisecond)
	tick := time.NewTicker(time.Millisecond)
	defer tick.Stop()

	for {
		select {
		case <-done:
			qm.l.Unlock()
			t.Fatal("host adjustment completed while host metrics lock was held")
		case <-tick.C:
			if got, _ := qm.totalsSnapshot(); got != 0 {
				qm.l.Unlock()
				<-done
				t.Fatalf("total attempts advanced before host metrics lock: got %d, want 0", got)
			}
		case <-deadline:
			if got, _ := qm.totalsSnapshot(); got != 0 {
				qm.l.Unlock()
				<-done
				t.Fatalf("total attempts advanced before host metrics lock: got %d, want 0", got)
			}
			qm.l.Unlock()
			<-done
			if attempts, latency := qm.totalsSnapshot(); attempts != 1 || latency != 10 {
				t.Fatalf("totals = (%d,%d), want (1,10)", attempts, latency)
			}
			if qm.host.Attempts != 1 || qm.host.TotalLatency != 10 {
				t.Fatalf("host metrics = %+v, want attempts=1 latency=10", qm.host)
			}
			return
		}
	}
}

func BenchmarkQueryMetricsUnobservedAttemptLifecycle(b *testing.B) {
	qm := newQueryMetrics()
	finishUnobservedTestAttempt(qm, time.Nanosecond)
	qm.reset()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if i%1024 == 0 {
			qm.reset()
		}
		finishUnobservedTestAttempt(qm, time.Nanosecond)
	}
}

func BenchmarkQueryMetricsResetAndUnobservedAttemptLifecycle(b *testing.B) {
	qm := newQueryMetrics()
	finishUnobservedTestAttempt(qm, time.Nanosecond)
	qm.reset()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		qm.reset()
		finishUnobservedTestAttempt(qm, time.Nanosecond)
	}
}

func BenchmarkQueryResetWithStraggler(b *testing.B) {
	q := newWarningTestQuery()
	q.reset()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		stragglerMetrics := q.metrics
		stragglerMetrics.retain()
		q.reset()
		stragglerMetrics.release()
	}
}

func BenchmarkQueryMetricsRecordHostAdjustment(b *testing.B) {
	hosts := []*HostInfo{
		{hostId: UUID{1}},
		{hostId: UUID{2}},
		{hostId: UUID{3}},
		{hostId: UUID{4}},
	}
	qm := newQueryMetrics()
	for _, host := range hosts {
		qm.recordHostAdjustment(1, time.Nanosecond, host)
	}
	qm.reset()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; {
		qm.reset()
		for _, host := range hosts {
			if i >= b.N {
				break
			}
			qm.recordHostAdjustment(1, time.Nanosecond, host)
			i++
		}
	}
}

func BenchmarkAttemptMetricsPersistentHistory(b *testing.B) {
	for _, attemptsPerExecution := range []int{1, 2, 4, 8} {
		b.Run(fmt.Sprintf("Attempts%d", attemptsPerExecution), func(b *testing.B) {
			var metrics AttemptMetrics
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				metrics = AttemptMetrics{}
				for attempt := 0; attempt < attemptsPerExecution; attempt++ {
					metrics = metrics.withAttempt(AttemptMetric{Attempt: attempt, Latency: 1})
				}
			}
			if metrics.Attempts() != attemptsPerExecution {
				b.Fatalf("attempt history count = %d, want %d", metrics.Attempts(), attemptsPerExecution)
			}
		})
	}
}

// Exercises the cache-hit path of Session.routingKeyInfo, called on every
// Pick() by TokenAwareHostPolicy for every query/batch.
func BenchmarkSessionRoutingKeyInfoCacheHit(b *testing.B) {
	const keyspace, stmt = "benchks", "insert into t (id) values (?)"

	s := &Session{}
	s.routingKeyInfoCache.lru = lru.New[routingKeyInfoCacheKey](100)
	inflight := &inflightCachedEntry{value: &routingKeyInfo{keyspace: keyspace, table: "t"}}
	s.routingKeyInfoCache.lru.Add(routingKeyInfoCacheKey{keyspace: keyspace, stmt: stmt}, inflight)

	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := s.routingKeyInfo(ctx, stmt, keyspace, 0); err != nil {
			b.Fatal(err)
		}
	}
}

func newWarningTestQuery() *Query {
	return &Query{
		context:     context.Background(),
		routingInfo: &queryRoutingInfo{},
		metrics:     newQueryMetrics(),
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
			metrics:     newQueryMetrics(),
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
			metrics:     newQueryMetrics(),
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
			metrics:     newQueryMetrics(),
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
			metrics:     newQueryMetrics(),
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
			metrics:     newQueryMetrics(),
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
			metrics:     newQueryMetrics(),
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
			metrics:     newQueryMetrics(),
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
		metrics := newQueryMetrics()
		var executionAttempts atomic.Int64
		metrics.retain()
		executor.run(
			ctx, qry, qry, metrics, &executionAttempts,
			func() SelectedHost { return staticSelectedHost{host: host} },
			make(chan queryExecutionResult),
		)

		if handler.calls != 0 {
			t.Fatalf("handler call count = %d, want 0", handler.calls)
		}
		if !framer.released {
			t.Fatal("speculative loser framer was not released")
		}
		if qry.released.Load() != 1 {
			t.Fatalf("releaseAfterExecution calls = %d, want 1", qry.released.Load())
		}
	})

	t.Run("LateSpeculativeResultCannotBeBuffered", func(t *testing.T) {
		host := (&HostInfo{hostId: UUID{18}}).setState(NodeUp)
		handler := &recordingWarningHandler{}
		framer := &testWarningFramer{warnings: []string{"late-loser"}}
		metrics := newQueryMetrics()
		warningBatch := &Batch{metrics: metrics}
		qry := &executorTestQuery{
			rt:         &fixedRetryPolicy{maxRetries: 0, retryType: Rethrow},
			spec:       NonSpeculativeExecution{},
			idempotent: true,
		}
		qry.executeFunc = func(context.Context, *Conn) *Iter {
			return (&Iter{framer: framer}).bindWarningHandler(warningBatch, handler)
		}

		results := queryExecutionResults()
		if capacity := cap(results); capacity != 0 {
			t.Fatalf("execution result channel capacity = %d, want unbuffered", capacity)
		}

		ctx, cancel := context.WithCancel(context.Background())
		cancel() // Models a loser completing after the winner has been returned.

		executor := newTestQueryExecutor(host)
		var executionAttempts atomic.Int64
		metrics.retain() // runner ownership consumed by run.
		executor.run(
			ctx, qry, qry, metrics, &executionAttempts,
			func() SelectedHost { return staticSelectedHost{host: host} },
			results,
		)

		if !framer.released {
			t.Fatal("late speculative result framer was not released")
		}
		if handler.calls != 0 {
			t.Fatalf("warning handler call count = %d, want 0", handler.calls)
		}
		if refs := metrics.refs.Load(); refs != 0 {
			t.Fatalf("execution metrics references = %d, want 0", refs)
		}
		if released := qry.released.Load(); released != 1 {
			t.Fatalf("releaseAfterExecution calls = %d, want 1", released)
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
		metrics := newQueryMetrics()
		var executionAttempts atomic.Int64
		iter, _ := executor.do(
			context.Background(), qry, metrics, &executionAttempts,
			func() SelectedHost { return staticSelectedHost{host: host} },
		)
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

func TestQueryExecutorAutomaticPageHasIndependentRetryBudget(t *testing.T) {
	t.Parallel()

	host := (&HostInfo{hostId: UUID{9}}).setState(NodeUp)
	executor := newTestQueryExecutor(host)
	metrics := newQueryMetrics()
	calls := 0
	qry := &executorTestQuery{
		ctx:        context.Background(),
		rt:         &fixedRetryPolicy{maxRetries: 1, retryType: Retry},
		spec:       NonSpeculativeExecution{},
		idempotent: true,
	}
	qry.executeFunc = func(context.Context, *Conn) *Iter {
		calls++
		switch calls {
		case 1:
			return &Iter{} // first page
		case 2:
			return &Iter{err: errors.New("transient second-page failure")}
		case 3:
			return &Iter{} // second-page retry
		default:
			t.Fatalf("unexpected query attempt %d", calls)
			return &Iter{}
		}
	}

	firstPage, err := executor.executeQuery(qry, metrics)
	if err != nil || firstPage.err != nil {
		t.Fatalf("first page failed: executor=%v iter=%v", err, firstPage.err)
	}
	firstPage.Close()

	// Automatic paging starts a new executor run but deliberately preserves
	// the logical metrics run and cumulative observer history.
	secondPageQuery := qry.withContext(context.Background())
	secondPage, err := executor.executeQuery(secondPageQuery, metrics)
	if err != nil || secondPage.err != nil {
		t.Fatalf("second page failed: executor=%v iter=%v", err, secondPage.err)
	}
	secondPage.Close()

	if calls != 3 {
		t.Fatalf("physical attempts = %d, want first page plus second-page retry (3)", calls)
	}
	if attempts, _ := metrics.totalsSnapshot(); attempts != 3 {
		t.Fatalf("cumulative metrics attempts = %d, want 3", attempts)
	}
}

func TestQueryExecutorPropagatesWinningRetryConsistency(t *testing.T) {
	t.Run("NonSpeculative", func(t *testing.T) {
		host := (&HostInfo{hostId: UUID{14}}).setState(NodeUp)
		executor := newTestQueryExecutor(host)
		metrics := newQueryMetrics()
		calls := 0
		qry := &executorTestQuery{
			ctx:         context.Background(),
			rt:          &consistencyRetryPolicy{consistency: All},
			spec:        NonSpeculativeExecution{},
			idempotent:  true,
			consistency: One,
		}
		qry.executeFunc = func(context.Context, *Conn) *Iter {
			calls++
			if calls == 1 {
				return &Iter{err: errors.New("retry")}
			}
			return &Iter{}
		}

		iter, err := executor.executeQuery(qry, metrics)
		if err != nil || iter.err != nil {
			t.Fatalf("execution failed: executor=%v iter=%v", err, iter.err)
		}
		iter.Close()
		if got := qry.GetConsistency(); got != All {
			t.Fatalf("caller consistency = %v, want winning retry consistency %v", got, All)
		}
	})

	t.Run("SpeculativeWinner", func(t *testing.T) {
		host := (&HostInfo{hostId: UUID{15}}).setState(NodeUp)
		secondHost := (&HostInfo{hostId: UUID{16}}).setState(NodeUp)
		executor := newTestQueryExecutor(host)
		executor.policy.AddHost(secondHost)
		executor.pool.hostConnPools[secondHost.hostUUID()] = &hostConnPool{
			host:       secondHost,
			connPicker: staticConnPicker{conn: &Conn{host: secondHost}},
		}
		metrics := newQueryMetrics()
		var calls atomic.Int32
		mainStarted := make(chan struct{})
		qry := &executorTestQuery{
			ctx:         context.Background(),
			rt:          &consistencyRetryPolicy{consistency: All},
			spec:        &SimpleSpeculativeExecution{NumAttempts: 1, TimeoutDelay: time.Millisecond},
			idempotent:  true,
			consistency: One,
		}
		qry.executeFunc = func(ctx context.Context, _ *Conn) *Iter {
			switch calls.Add(1) {
			case 1:
				close(mainStarted)
				<-ctx.Done()
				return &Iter{err: ctx.Err()}
			case 2:
				<-mainStarted
				return &Iter{err: errors.New("speculative retry")}
			case 3:
				return &Iter{}
			default:
				t.Fatal("unexpected extra attempt")
				return &Iter{}
			}
		}

		iter, err := executor.executeQuery(qry, metrics)
		if err != nil || iter.err != nil {
			t.Fatalf("execution failed: executor=%v iter=%v", err, iter.err)
		}
		iter.Close()
		if got := qry.GetConsistency(); got != All {
			t.Fatalf("caller consistency = %v, want winning retry consistency %v", got, All)
		}

		deadline := time.After(2 * time.Second)
		poll := time.NewTicker(time.Millisecond)
		defer poll.Stop()
		for qry.released.Load() != 3 {
			select {
			case <-deadline:
				t.Fatalf("execution borrows released = %d, want 3", qry.released.Load())
			case <-poll.C:
			}
		}
		if got := qry.GetConsistency(); got != All {
			t.Fatalf("loser changed caller consistency to %v, want winner value %v", got, All)
		}
	})
}

func TestQueryExecutorSuccessfulAttemptDoesNotWriteConsistency(t *testing.T) {
	t.Parallel()

	host := (&HostInfo{hostId: UUID{17}}).setState(NodeUp)
	qry := &executorTestQuery{
		ctx:         context.Background(),
		rt:          &fixedRetryPolicy{maxRetries: 0, retryType: Rethrow},
		spec:        NonSpeculativeExecution{},
		idempotent:  true,
		consistency: One,
		executeFunc: func(context.Context, *Conn) *Iter {
			return &Iter{}
		},
	}

	iter, err := newTestQueryExecutor(host).executeQuery(qry, newQueryMetrics())
	if err != nil || iter.err != nil {
		t.Fatalf("execution failed: executor=%v iter=%v", err, iter.err)
	}
	iter.Close()
	if writes := qry.consistencyWrites.Load(); writes != 0 {
		t.Fatalf("consistency writes = %d, want 0 on unchanged successful execution", writes)
	}
}

func TestQueryExecutorSuccessfulAttemptDoesNotAddRetryCounterAllocation(t *testing.T) {
	host := (&HostInfo{hostId: UUID{13}}).setState(NodeUp)
	executor := newTestQueryExecutor(host)
	metrics := newQueryMetrics()
	iter := &Iter{}
	qry := &executorTestQuery{
		ctx:        context.Background(),
		rt:         &fixedRetryPolicy{maxRetries: 0, retryType: Rethrow},
		spec:       NonSpeculativeExecution{},
		idempotent: true,
		executeFunc: func(context.Context, *Conn) *Iter {
			return iter
		},
	}
	selected := staticSelectedHost{host: host}
	hostIter := func() SelectedHost { return selected }

	allocs := testing.AllocsPerRun(1000, func() {
		if got, _ := executor.do(qry.Context(), qry, metrics, nil, hostIter); got != iter {
			panic("unexpected iterator")
		}
	})
	// getPool uses hostUUID() (zero-alloc); the retry counter must stay stack-local too.
	if allocs != 0 {
		t.Fatalf("successful execution allocations = %f, want 0", allocs)
	}
}

func TestQueryExecutorSpeculativeBranchesShareRetryBudget(t *testing.T) {
	host := (&HostInfo{hostId: UUID{10}}).setState(NodeUp)
	secondHost := (&HostInfo{hostId: UUID{11}}).setState(NodeUp)
	executor := newTestQueryExecutor(host)
	executor.policy.AddHost(secondHost)
	executor.pool.hostConnPools[secondHost.hostUUID()] = &hostConnPool{
		host:       secondHost,
		connPicker: staticConnPicker{conn: &Conn{host: secondHost}},
	}
	metrics := newQueryMetrics()
	policy := &synchronizedRetryPolicy{
		maxRetries: 1,
		allEntered: make(chan struct{}),
	}
	initialAttemptsStarted := make(chan struct{})
	var calls atomic.Int32
	qry := &executorTestQuery{
		ctx:        context.Background(),
		rt:         policy,
		spec:       &SimpleSpeculativeExecution{NumAttempts: 1, TimeoutDelay: time.Millisecond},
		idempotent: true,
	}
	qry.executeFunc = func(context.Context, *Conn) *Iter {
		attempt := calls.Add(1)
		if attempt <= 2 {
			if attempt == 2 {
				close(initialAttemptsStarted)
			}
			<-initialAttemptsStarted
			return &Iter{err: errors.New("initial attempt failed")}
		}
		return &Iter{}
	}

	iter, err := executor.executeQuery(qry, metrics)
	if err != nil {
		t.Fatalf("executor failed: %v", err)
	}
	iter.Close()

	deadline := time.After(2 * time.Second)
	poll := time.NewTicker(time.Millisecond)
	defer poll.Stop()
	for qry.released.Load() != 3 {
		select {
		case <-deadline:
			t.Fatalf(
				"execution borrows released = %d, want 3 (borrowed=%d calls=%d policy=%d)",
				qry.released.Load(), qry.borrowed.Load(), calls.Load(), policy.entered.Load(),
			)
		case <-poll.C:
		}
	}

	if got := calls.Load(); got != 2 {
		t.Fatalf("physical attempts = %d, want two failed speculative branches sharing one exhausted retry budget", got)
	}
	if got := policy.entered.Load(); got != 2 {
		t.Fatalf("retry policy calls = %d, want 2", got)
	}
	if attempts, _ := metrics.totalsSnapshot(); attempts != 2 {
		t.Fatalf("cumulative metrics attempts = %d, want 2", attempts)
	}
}

func TestQueryExecutorSpeculativeAttemptOrdinalsFollowLaunchOrder(t *testing.T) {
	host := (&HostInfo{hostId: UUID{6}}).setState(NodeUp)
	secondHost := (&HostInfo{hostId: UUID{7}}).setState(NodeUp)
	executor := newTestQueryExecutor(host)
	executor.policy.AddHost(secondHost)
	executor.pool.hostConnPools[secondHost.hostUUID()] = &hostConnPool{
		host:       secondHost,
		connPicker: staticConnPicker{conn: &Conn{host: secondHost}},
	}
	metrics := newQueryMetrics()

	firstStarted := make(chan struct{})
	releaseFirst := make(chan struct{})
	var releaseOnce sync.Once
	release := func() {
		releaseOnce.Do(func() { close(releaseFirst) })
	}
	defer release()

	type observation struct {
		attempt int
		metrics AttemptMetrics
	}
	observed := make(chan observation, 2)
	var launches atomic.Int32
	qry := &executorTestQuery{
		ctx:        context.Background(),
		rt:         &fixedRetryPolicy{maxRetries: 0, retryType: Rethrow},
		spec:       &SimpleSpeculativeExecution{NumAttempts: 1, TimeoutDelay: time.Millisecond},
		idempotent: true,
	}
	qry.executeFunc = func(context.Context, *Conn) *Iter {
		if launches.Add(1) == 1 {
			close(firstStarted)
			<-releaseFirst
		}
		return &Iter{}
	}
	qry.finishFunc = func(token attemptToken, end time.Time, _ *Iter, completedHost *HostInfo) {
		defer token.metrics.release()
		attempt, _, attemptMetrics := token.metrics.finishAttempt(token, end.Sub(token.start), completedHost, true, true)
		observed <- observation{attempt: attempt, metrics: attemptMetrics}
	}

	result := make(chan *Iter, 1)
	go func() {
		iter, err := executor.executeQuery(qry, metrics)
		if err != nil {
			result <- &Iter{err: err}
			return
		}
		result <- iter
	}()

	select {
	case <-firstStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("first attempt did not start")
	}

	select {
	case iter := <-result:
		if iter.err != nil {
			t.Fatalf("speculative execution returned error: %v", iter.err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("later speculative attempt did not complete promptly")
	}

	fast := <-observed
	if fast.attempt != 1 {
		t.Fatalf("fast attempt ordinal = %d, want 1", fast.attempt)
	}
	var fastEntries []AttemptMetric
	fast.metrics.ForEachAttempt(func(attempt AttemptMetric) bool {
		fastEntries = append(fastEntries, attempt)
		return true
	})
	if len(fastEntries) != 1 || fastEntries[0].Attempt != 1 {
		t.Fatalf("fast snapshot = %+v, want only launch ordinal 1", fastEntries)
	}

	release()
	slow := <-observed
	if slow.attempt != 0 {
		t.Fatalf("slow attempt ordinal = %d, want 0", slow.attempt)
	}
	var slowEntries []AttemptMetric
	slow.metrics.ForEachAttempt(func(attempt AttemptMetric) bool {
		slowEntries = append(slowEntries, attempt)
		return true
	})
	if len(slowEntries) != 2 || slowEntries[0].Attempt != 0 || slowEntries[1].Attempt != 1 {
		t.Fatalf("final snapshot = %+v, want launch ordinals [0 1]", slowEntries)
	}
}

func TestQueryForExecutionPreservesConcreteTypes(t *testing.T) {
	queryMetrics := newQueryMetrics()
	finishUnobservedTestAttempt(queryMetrics, 4*time.Nanosecond)
	for i := 1; i < 7; i++ {
		finishUnobservedTestAttempt(queryMetrics, 0)
	}
	var queryAttempts atomic.Int64
	queryAttempts.Store(2)
	query := &Query{
		metrics:     newQueryMetrics(),
		routingInfo: &queryRoutingInfo{},
		cons:        One,
	}
	executionQuery := queryForRetryExecution(query, queryMetrics)
	concreteExecutionQuery, ok := executionQuery.(*Query)
	if !ok {
		t.Fatalf("execution query has type %T, want *Query", executionQuery)
	}
	if concreteExecutionQuery.Attempts() != 7 {
		t.Fatalf("execution query attempts = %d, want cumulative count 7", concreteExecutionQuery.Attempts())
	}
	retryableQuery := queryForExecution(executionQuery, queryMetrics, &queryAttempts)
	concreteQuery, ok := retryableQuery.(*Query)
	if !ok {
		t.Fatalf("retry-policy query has type %T, want *Query", retryableQuery)
	}
	if concreteQuery.Attempts() != 2 {
		t.Fatalf("query retry attempts = %d, want 2", concreteQuery.Attempts())
	}
	concreteQuery.SetConsistency(All)
	if query.GetConsistency() != One {
		t.Fatalf("original query consistency = %v, want %v", query.GetConsistency(), One)
	}
	if concreteQuery.GetConsistency() != All {
		t.Fatalf("execution query consistency = %v, want %v", concreteQuery.GetConsistency(), All)
	}
	concreteExecutionQuery.SetConsistency(concreteQuery.GetConsistency())
	if concreteExecutionQuery.GetConsistency() != All {
		t.Fatalf("retry execution consistency = %v, want %v", concreteExecutionQuery.GetConsistency(), All)
	}

	batchMetrics := newQueryMetrics()
	finishUnobservedTestAttempt(batchMetrics, 9*time.Nanosecond)
	for i := 1; i < 8; i++ {
		finishUnobservedTestAttempt(batchMetrics, 0)
	}
	var batchAttempts atomic.Int64
	batchAttempts.Store(3)
	batch := &Batch{
		metrics:     newQueryMetrics(),
		routingInfo: &queryRoutingInfo{},
		Cons:        One,
	}
	executionBatch := queryForRetryExecution(batch, batchMetrics)
	concreteExecutionBatch, ok := executionBatch.(*Batch)
	if !ok {
		t.Fatalf("execution batch has type %T, want *Batch", executionBatch)
	}
	if concreteExecutionBatch.Attempts() != 8 {
		t.Fatalf("execution batch attempts = %d, want cumulative count 8", concreteExecutionBatch.Attempts())
	}
	retryableBatch := queryForExecution(executionBatch, batchMetrics, &batchAttempts)
	concreteBatch, ok := retryableBatch.(*Batch)
	if !ok {
		t.Fatalf("retry-policy batch has type %T, want *Batch", retryableBatch)
	}
	if concreteBatch.Attempts() != 3 {
		t.Fatalf("batch retry attempts = %d, want 3", concreteBatch.Attempts())
	}
	concreteBatch.SetConsistency(All)
	if batch.GetConsistency() != One {
		t.Fatalf("original batch consistency = %v, want %v", batch.GetConsistency(), One)
	}
	if concreteBatch.GetConsistency() != All {
		t.Fatalf("execution batch consistency = %v, want %v", concreteBatch.GetConsistency(), All)
	}
	concreteExecutionBatch.SetConsistency(concreteBatch.GetConsistency())
	if concreteExecutionBatch.GetConsistency() != All {
		t.Fatalf("retry execution batch consistency = %v, want %v", concreteExecutionBatch.GetConsistency(), All)
	}
}

func TestSpeculativeExecutionViewCapturesObserverPayload(t *testing.T) {
	t.Parallel()

	host := &HostInfo{hostId: UUID{12}}
	start := time.Unix(0, 100)

	queryObserved := make(chan ObservedQueryWithAttemptMetrics, 1)
	query := &Query{
		context: context.Background(),
		observer: unitQueryObserverWithAttemptMetricsFunc(func(
			_ context.Context, observed ObservedQueryWithAttemptMetrics,
		) {
			queryObserved <- observed
		}),
		metrics:     newQueryMetrics(),
		routingInfo: &queryRoutingInfo{},
		stmt:        "old query",
		values:      []any{"old value"},
	}
	capturedQuery := queryForSpeculativeExecution(query, query.metrics).(*Query)
	query.observer = unitQueryObserverFunc(func(context.Context, ObservedQuery) {
		t.Error("new-generation query observer received old attempt")
	})
	query.stmt = "new query"
	query.values = []any{"new value"}

	queryToken := query.metrics.beginAttempt()
	queryToken.start = start
	capturedQuery.finishAttempt(
		queryToken, "ks", start.Add(time.Nanosecond), &Iter{}, host,
	)
	queryObservation := <-queryObserved
	if queryObservation.Statement != "old query" ||
		!reflect.DeepEqual(queryObservation.Values, []any{"old value"}) {
		t.Fatalf("captured query payload = (%q,%v), want old generation", queryObservation.Statement, queryObservation.Values)
	}

	batchObserved := make(chan ObservedBatchWithAttemptMetrics, 1)
	batch := &Batch{
		context: context.Background(),
		observer: unitBatchObserverWithAttemptMetricsFunc(func(
			_ context.Context, observed ObservedBatchWithAttemptMetrics,
		) {
			batchObserved <- observed
		}),
		metrics:     newQueryMetrics(),
		routingInfo: &queryRoutingInfo{},
		Entries: []BatchEntry{
			{Stmt: "old batch", Args: []any{"old value"}},
		},
	}
	capturedBatch := queryForSpeculativeExecution(batch, batch.metrics).(*Batch)
	batch.observer = unitBatchObserverFunc(func(context.Context, ObservedBatch) {
		t.Error("new-generation batch observer received old attempt")
	})
	batch.Entries[0] = BatchEntry{Stmt: "new batch", Args: []any{"new value"}}

	batchToken := batch.metrics.beginAttempt()
	batchToken.start = start
	capturedBatch.finishAttempt(
		batchToken, "ks", start.Add(time.Nanosecond), &Iter{}, host,
	)
	batchObservation := <-batchObserved
	if !slices.Equal(batchObservation.Statements, []string{"old batch"}) ||
		!reflect.DeepEqual(batchObservation.Values, [][]any{{"old value"}}) {
		t.Fatalf(
			"captured batch payload = (%v,%v), want old generation",
			batchObservation.Statements, batchObservation.Values,
		)
	}
}

func TestScannerScanTupleColumnUsesRawColumnIndex(t *testing.T) {
	t.Parallel()

	tupleInfo := TupleTypeInfo{
		NativeType: NativeType{proto: protoVersion4, typ: TypeTuple},
		Elems: []TypeInfo{
			NativeType{proto: protoVersion4, typ: TypeVarchar},
			NativeType{proto: protoVersion4, typ: TypeVarchar},
		},
	}

	encodeTuple := func(values ...string) []byte {
		var out []byte
		for _, v := range values {
			data := []byte(v)
			var lenBuf [4]byte
			binary.BigEndian.PutUint32(lenBuf[:], uint32(len(data)))
			out = append(out, lenBuf[:]...)
			out = append(out, data...)
		}
		return out
	}

	iter := &Iter{
		framer:  &testWarningFramer{},
		numRows: 1,
		meta: resultMetadata{
			columns: []ColumnInfo{
				{Name: "pair", TypeInfo: tupleInfo},
				{Name: "tail", TypeInfo: NativeType{proto: protoVersion4, typ: TypeVarchar}},
			},
			actualColCount: 3,
		},
	}

	scanner := iter.Scanner().(*iterScanner)
	scanner.valid = true
	scanner.cols[0] = encodeTuple("left", "right")
	scanner.cols[1] = []byte("tail")

	var left, right, tail string
	if err := scanner.Scan(&left, &right, &tail); err != nil {
		t.Fatalf("Scan() returned unexpected error: %v", err)
	}
	if left != "left" || right != "right" || tail != "tail" {
		t.Fatalf("scanned values = (%q, %q, %q), want (%q, %q, %q)", left, right, tail, "left", "right", "tail")
	}
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

func TestQueryObserverMetricsContinueAcrossAutomaticPages(t *testing.T) {
	t.Parallel()

	host := &HostInfo{hostId: UUID{8}}
	baseQry := newWarningTestQuery()
	baseQry.refCount = 1
	var observations []ObservedQueryWithAttemptMetrics
	baseQry.observer = unitQueryObserverWithAttemptMetricsFunc(func(
		_ context.Context, query ObservedQueryWithAttemptMetrics,
	) {
		observations = append(observations, query)
	})

	var firstPageMetrics *queryMetrics
	call := 0
	baseQry.conn = &pagingTestConn{
		executeQueryFunc: func(context.Context, *Query) *Iter {
			t.Fatal("automatic paging used the legacy metrics path")
			return nil
		},
		executeQueryWithMetricsFunc: func(
			_ context.Context,
			qry *Query,
			metrics *queryMetrics,
		) *Iter {
			call++
			if call == 1 {
				firstPageMetrics = metrics
			} else if metrics != firstPageMetrics {
				t.Fatal("automatic page detached from the first page metrics run")
			}

			start := time.Unix(0, int64(call)*100)
			finishTestAttempt(
				qry,
				metrics,
				"ks",
				start.Add(time.Duration(call)*time.Nanosecond),
				start,
				&Iter{numRows: 1},
				host,
			)
			if call == 1 {
				nextQry := cloneQueryForNextPage(qry, metrics, []byte("next"))
				return &Iter{
					framer:  &testWarningFramer{},
					numRows: 1,
					pos:     1,
					next:    newNextIter(nextQry, 1),
				}
			}
			if call == 2 {
				return &Iter{framer: &testWarningFramer{}, numRows: 1}
			}
			t.Fatalf("unexpected executeQuery call %d", call)
			return nil
		},
	}

	iter := baseQry.Iter()
	if !iter.fetchNextPage() {
		t.Fatal("automatic next-page fetch failed")
	}
	defer iter.Close()

	if len(observations) != 2 || observations[0].Attempt != 0 || observations[1].Attempt != 1 {
		t.Fatalf("page observations = %+v, want attempts [0 1]", observations)
	}
	assertAttemptMetrics(t, observations[1].AttemptMetrics, []AttemptMetric{
		{Attempt: 0, Host: host, Latency: 1},
		{Attempt: 1, Host: host, Latency: 2},
	})
	if attempts, latency := firstPageMetrics.totalsSnapshot(); attempts != 2 || latency != 3 {
		t.Fatalf("automatic-page totals = (%d,%d), want (2,3)", attempts, latency)
	}
	if refs := firstPageMetrics.refs.Load(); refs != 1 {
		t.Fatalf("automatic-page references after consume = %d, want root owner only", refs)
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
	host := &HostInfo{hostId: UUID{7}}
	var observations []ObservedQueryWithAttemptMetrics
	baseQry.observer = unitQueryObserverWithAttemptMetricsFunc(func(
		_ context.Context, query ObservedQueryWithAttemptMetrics,
	) {
		observations = append(observations, query)
	})

	call := 0
	baseQry.conn = &pagingTestConn{
		executeQueryFunc: func(context.Context, *Query) *Iter {
			t.Fatal("manual paging used the legacy metrics path")
			return nil
		},
		executeQueryWithMetricsFunc: func(
			_ context.Context,
			qry *Query,
			metrics *queryMetrics,
		) *Iter {
			call++
			start := time.Unix(0, int64(call)*100)
			finishTestAttempt(
				qry,
				metrics,
				"ks",
				start.Add(time.Duration(call)*time.Nanosecond),
				start,
				&Iter{},
				host,
			)
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
				}).bindWarningHandlerWithMetrics(qry, metrics, handler)
			case 2:
				if !slices.Equal(qry.pageState, []byte("next")) {
					t.Fatalf("second page state = %q, want %q", qry.pageState, []byte("next"))
				}
				return (&Iter{
					framer:  finalFramer,
					numRows: 1,
				}).bindWarningHandlerWithMetrics(qry, metrics, handler)
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
	if len(observations) != 2 || observations[0].Attempt != 0 || observations[1].Attempt != 1 {
		t.Fatalf("page observations = %+v, want attempts [0 1]", observations)
	}
	assertAttemptMetrics(t, observations[1].AttemptMetrics, []AttemptMetric{
		{Attempt: 0, Host: host, Latency: 1},
		{Attempt: 1, Host: host, Latency: 2},
	})
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

type keyspaceCapturingQueryObserver struct{ keyspace string }

func (o *keyspaceCapturingQueryObserver) ObserveQuery(_ context.Context, q ObservedQuery) {
	o.keyspace = q.Keyspace
}

type keyspaceCapturingBatchObserver struct{ keyspace string }

func (o *keyspaceCapturingBatchObserver) ObserveBatch(_ context.Context, b ObservedBatch) {
	o.keyspace = b.Keyspace
}

// keyspaceCapturingExecutable is a fake ExecutableQuery that records the
// keyspace argument queryExecutor.attemptQuery passes to attempt(). Keyspace()
// returns effectiveKS (the per-statement override), while attempt captures
// whatever attemptQuery actually forwards, so the test fails if attemptQuery
// stops using the effective keyspace.
type keyspaceCapturingExecutable struct {
	ExecutableQuery
	effectiveKS     string
	attemptKeyspace string
}

func (e *keyspaceCapturingExecutable) Keyspace() string { return e.effectiveKS }

func (e *keyspaceCapturingExecutable) execute(context.Context, *Conn, *queryMetrics) *Iter {
	return &Iter{}
}

func (e *keyspaceCapturingExecutable) finishAttempt(_ attemptToken, keyspace string, _ time.Time, _ *Iter, _ *HostInfo) {
	e.attemptKeyspace = keyspace
}

// TestAttemptQueryReportsEffectiveKeyspace verifies that queryExecutor.attemptQuery
// forwards the query's effective keyspace (Query.Keyspace(), which honors the
// proto v5 SetKeyspace override) to attempt(), rather than the pool/session
// keyspace. Reverting attemptQuery to pass the pool keyspace fails this test.
func TestAttemptQueryReportsEffectiveKeyspace(t *testing.T) {
	const overrideKS = "override_ks"

	qe := &queryExecutor{}
	exec := &keyspaceCapturingExecutable{effectiveKS: overrideKS}
	conn := &Conn{host: &HostInfo{hostId: UUID{1}}}

	var localAttempts int64
	qe.attemptQuery(context.Background(), exec, newQueryMetrics(), nil, &localAttempts, conn)

	if exec.attemptKeyspace != overrideKS {
		t.Fatalf("attemptQuery forwarded keyspace %q to attempt, want %q", exec.attemptKeyspace, overrideKS)
	}
}

// TestQueryAttemptReportsOverrideKeyspaceToObserver verifies the end-to-end wiring
// from Query.attempt through to the observer: a per-query keyspace override
// (Query.SetKeyspace) must surface in ObservedQuery.Keyspace.
func TestQueryAttemptReportsOverrideKeyspaceToObserver(t *testing.T) {
	const overrideKS = "override_ks"

	obs := &keyspaceCapturingQueryObserver{}
	q := &Query{
		stmt:        "SELECT * FROM t",
		routingInfo: &queryRoutingInfo{},
		metrics:     newQueryMetrics(),
		observer:    obs,
	}
	q.SetKeyspace(overrideKS)

	if got := q.Keyspace(); got != overrideKS {
		t.Fatalf("Query.Keyspace() = %q, want %q", got, overrideKS)
	}

	token := q.metrics.beginAttempt()
	q.finishAttempt(token, q.Keyspace(), time.Now(), &Iter{}, &HostInfo{hostId: UUID{1}})

	if obs.keyspace != overrideKS {
		t.Fatalf("ObservedQuery.Keyspace = %q, want %q", obs.keyspace, overrideKS)
	}
}

// TestBatchAttemptReportsOverrideKeyspaceToObserver is the batch counterpart of
// TestQueryAttemptReportsOverrideKeyspaceToObserver.
func TestBatchAttemptReportsOverrideKeyspaceToObserver(t *testing.T) {
	const overrideKS = "override_ks"

	obs := &keyspaceCapturingBatchObserver{}
	b := &Batch{
		routingInfo: &queryRoutingInfo{},
		metrics:     newQueryMetrics(),
		observer:    obs,
	}
	b.SetKeyspace(overrideKS)

	if got := b.Keyspace(); got != overrideKS {
		t.Fatalf("Batch.Keyspace() = %q, want %q", got, overrideKS)
	}

	token := b.metrics.beginAttempt()
	b.finishAttempt(token, b.Keyspace(), time.Now(), &Iter{}, &HostInfo{hostId: UUID{2}})

	if obs.keyspace != overrideKS {
		t.Fatalf("ObservedBatch.Keyspace = %q, want %q", obs.keyspace, overrideKS)
	}
}

// TestQueryRoutingInfoAccessorsConcurrentWithWrite exercises every accessor that
// reads queryRoutingInfo concurrently with a writer mutating it under
// routingInfo.mu (as GetRoutingKey and Conn.executeQuery both do).
//
// Both keyspace and table are covered: the token-aware host policy reads them
// together (policies.go, scylla.go call qry.Keyspace() and qry.Table() in one
// expression) from every speculative execution goroutine, so both reads must
// take routingInfo.mu. Under -race this fails if either lock is dropped.
//
// The writer alternates between values of differing length so the unsynchronized
// case is a genuine torn-read of the string header, not just a formal race.
func TestQueryRoutingInfoAccessorsConcurrentWithWrite(t *testing.T) {
	const iterations = 1000

	t.Run("Query", func(t *testing.T) {
		q := &Query{
			routingInfo: &queryRoutingInfo{},
		}

		var wg sync.WaitGroup
		wg.Add(2)

		// Writer: mimic GetRoutingKey/executeQuery updating routingInfo under the lock.
		go func() {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				q.routingInfo.mu.Lock()
				if i%2 == 0 {
					q.routingInfo.keyspace, q.routingInfo.table = "ks", "tbl"
				} else {
					q.routingInfo.keyspace, q.routingInfo.table = "a_much_longer_keyspace", "a_much_longer_table"
				}
				q.routingInfo.mu.Unlock()
			}
		}()

		// Reader: mimic attemptQuery and the token-aware policy reading both fields,
		// plus the pair read Conn.executeQuery does for a tablet-routing hint. The
		// pair must come from one critical section: keyspace and table are always
		// written together, so reading them separately can pair one goroutine's
		// keyspace with another's table even with both reads individually locked.
		go func() {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				_ = q.Keyspace()
				_ = q.Table()

				ks, tbl := q.routingInfo.keyspaceTable()
				if (ks == "ks") != (tbl == "tbl") {
					t.Errorf("keyspaceTable() returned a mismatched pair: %q / %q", ks, tbl)
				}
			}
		}()

		wg.Wait()
	})

	t.Run("Batch", func(t *testing.T) {
		b := &Batch{
			routingInfo: &queryRoutingInfo{},
		}

		var wg sync.WaitGroup
		wg.Add(2)

		go func() {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				b.routingInfo.mu.Lock()
				if i%2 == 0 {
					b.routingInfo.table = "tbl"
				} else {
					b.routingInfo.table = "a_much_longer_table"
				}
				b.routingInfo.mu.Unlock()
			}
		}()

		go func() {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				_ = b.Keyspace()
				_ = b.Table()
			}
		}()

		wg.Wait()
	})
}

// TestSetKeyspaceInvalidatesRoutingInfo pins that changing the keyspace override
// drops the routing metadata GetRoutingKey resolved for the previous one.
//
// Keyspace() prefers routingInfo.keyspace over q.keyspace, and the partitioner
// and table are only ever refreshed by GetRoutingKey. A host-pinned or explicitly
// routed query never calls GetRoutingKey again, so a stale cache would route
// (and be reported to observers) with the previous keyspace's metadata.
func TestSetKeyspaceInvalidatesRoutingInfo(t *testing.T) {
	t.Parallel()

	// Stand in for GetRoutingKey having resolved the first override.
	cached := func(ri *queryRoutingInfo) {
		ri.mu.Lock()
		defer ri.mu.Unlock()
		ri.keyspace = "ks_a"
		ri.table = "tbl_a"
		ri.partitioner = murmur3Partitioner{}
		ri.lwt = true
	}

	t.Run("Query", func(t *testing.T) {
		q := &Query{routingInfo: &queryRoutingInfo{}}
		q.SetKeyspace("ks_a")
		cached(q.routingInfo)

		q.SetKeyspace("ks_b")

		require.Equal(t, "ks_b", q.Keyspace(), "Keyspace() must follow the new override, not the stale cache")
		require.Empty(t, q.Table(), "cached table must not survive a keyspace change")
		require.Nil(t, q.routingInfo.getPartitioner(), "cached partitioner must not survive a keyspace change")
		require.False(t, q.routingInfo.isLWT(), "cached lwt flag must not survive a keyspace change")
	})

	t.Run("Batch", func(t *testing.T) {
		b := &Batch{routingInfo: &queryRoutingInfo{}}
		b.SetKeyspace("ks_a")
		cached(b.routingInfo)

		b.SetKeyspace("ks_b")

		require.Equal(t, "ks_b", b.Keyspace())
		require.Empty(t, b.Table())
		require.Nil(t, b.routingInfo.getPartitioner(), "cached partitioner must not survive a keyspace change")
		require.False(t, b.routingInfo.isLWT(), "cached lwt flag must not survive a keyspace change")
	})

	// WithContext returns a shallow copy that shares the routingInfo pointer, so
	// dropping the cache in place would reach back into the query the copy came
	// from. That query is not necessarily going to rebuild it: GetRoutingKey
	// returns early when an explicit routing key is set, which leaves it routing
	// on a nil partitioner and reporting an empty table — a worse failure than the
	// stale cache, and one the source never asked for.
	t.Run("Query/WithContext does not disturb the source", func(t *testing.T) {
		q := &Query{routingInfo: &queryRoutingInfo{}, routingKey: []byte{0x01}}
		q.SetKeyspace("ks_a")
		cached(q.routingInfo)

		copied := q.WithContext(context.Background())
		copied.SetKeyspace("ks_b")

		require.Equal(t, "ks_b", copied.Keyspace(), "the copy must follow its own override")
		require.Empty(t, copied.Table(), "the copy must not inherit the source's cached table")

		require.Equal(t, "ks_a", q.Keyspace(), "the source's resolved keyspace must survive")
		require.Equal(t, "tbl_a", q.Table(), "the source's cached table must survive")
		require.NotNil(t, q.routingInfo.getPartitioner(), "the source's cached partitioner must survive")
		require.True(t, q.routingInfo.isLWT(), "the source's cached lwt flag must survive")
	})

	t.Run("Batch/WithContext does not disturb the source", func(t *testing.T) {
		b := &Batch{routingInfo: &queryRoutingInfo{}}
		b.SetKeyspace("ks_a")
		cached(b.routingInfo)

		copied := b.WithContext(context.Background())
		copied.SetKeyspace("ks_b")

		require.Equal(t, "ks_b", copied.Keyspace())
		require.Nil(t, copied.routingInfo.getPartitioner(), "the copy must not inherit the source's cached partitioner")

		require.Equal(t, "tbl_a", b.Table(), "the source's cached table must survive")
		require.NotNil(t, b.routingInfo.getPartitioner(), "the source's cached partitioner must survive")
		require.True(t, b.routingInfo.isLWT(), "the source's cached lwt flag must survive")
	})
}

// TestResolveRoutingKeyspaceTable pins the precedence used to pick the keyspace
// and table a prepared statement targets. The default (SetKeyspace override or
// Cluster.Keyspace) must never shadow the per-column metadata: routingKeyInfo
// seeds its keyspace variable with that default in order to build the cache key,
// and if the default won, a statement against another keyspace's table would be
// routed with the session keyspace and get the wrong partitioner.
func TestResolveRoutingKeyspaceTable(t *testing.T) {
	t.Parallel()

	col := func(ks, tbl string) []ColumnInfo {
		return []ColumnInfo{{Keyspace: ks, Table: tbl, Name: "id"}}
	}

	tests := []struct {
		name         string
		meta         preparedMetadata
		def          string
		wantKeyspace string
		wantTable    string
	}{
		{
			name:         "global table spec wins over the default",
			meta:         preparedMetadata{keyspace: "global_ks", table: "global_tbl"},
			def:          "session_ks",
			wantKeyspace: "global_ks",
			wantTable:    "global_tbl",
		},
		{
			name: "global table spec wins over per-column metadata",
			meta: preparedMetadata{
				keyspace:       "global_ks",
				table:          "global_tbl",
				resultMetadata: resultMetadata{columns: col("column_ks", "column_tbl")},
			},
			def:          "session_ks",
			wantKeyspace: "global_ks",
			wantTable:    "global_tbl",
		},
		{
			// The regression this helper exists for: no global table spec, so the
			// keyspace must come from the columns and not from the session default.
			name: "per-column metadata wins over the default",
			meta: preparedMetadata{
				resultMetadata: resultMetadata{columns: col("column_ks", "column_tbl")},
			},
			def:          "session_ks",
			wantKeyspace: "column_ks",
			wantTable:    "column_tbl",
		},
		{
			name: "per-column keyspace fills in a partial global spec",
			meta: preparedMetadata{
				table:          "global_tbl",
				resultMetadata: resultMetadata{columns: col("column_ks", "column_tbl")},
			},
			def:          "session_ks",
			wantKeyspace: "column_ks",
			wantTable:    "global_tbl",
		},
		{
			name:         "default is the last resort",
			meta:         preparedMetadata{},
			def:          "session_ks",
			wantKeyspace: "session_ks",
			wantTable:    "",
		},
		{
			name:         "no source at all yields empty",
			meta:         preparedMetadata{},
			def:          "",
			wantKeyspace: "",
			wantTable:    "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			keyspace, table := resolveRoutingKeyspaceTable(&tt.meta, tt.def)
			if keyspace != tt.wantKeyspace {
				t.Errorf("keyspace = %q, want %q", keyspace, tt.wantKeyspace)
			}
			if table != tt.wantTable {
				t.Errorf("table = %q, want %q", table, tt.wantTable)
			}
		})
	}
}
