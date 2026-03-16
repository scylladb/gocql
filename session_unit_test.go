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
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/gocql/gocql/internal/lru"
	"github.com/gocql/gocql/tablets"
)

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

// TestRoutingPlan_IsLWT_WithPlan verifies that isLWT() returns the plan's
// lwt value without requiring the mutex when a plan is set.
func TestRoutingPlan_IsLWT_WithPlan(t *testing.T) {
	t.Parallel()

	ri := &queryRoutingInfo{}
	plan := &routingPlan{lwt: true, keyspace: "ks", table: "tbl"}
	ri.plan.Store(plan)

	if !ri.isLWT() {
		t.Error("expected isLWT() to return true when plan.lwt is true")
	}

	planFalse := &routingPlan{lwt: false}
	ri.plan.Store(planFalse)
	if ri.isLWT() {
		t.Error("expected isLWT() to return false when plan.lwt is false")
	}
}

// TestRoutingPlan_IsLWT_WithoutPlan verifies that isLWT() falls back to the
// mutex-protected field when no plan is set.
func TestRoutingPlan_IsLWT_WithoutPlan(t *testing.T) {
	t.Parallel()

	ri := &queryRoutingInfo{}
	ri.lwt = true

	if !ri.isLWT() {
		t.Error("expected isLWT() to return true from fallback field")
	}

	ri.lwt = false
	if ri.isLWT() {
		t.Error("expected isLWT() to return false from fallback field")
	}
}

// TestRoutingPlan_GetPartitioner_WithPlan verifies that getPartitioner()
// reads from the plan when available.
func TestRoutingPlan_GetPartitioner_WithPlan(t *testing.T) {
	t.Parallel()

	ri := &queryRoutingInfo{}
	mp := murmur3Partitioner{}
	plan := &routingPlan{partitioner: mp}
	ri.plan.Store(plan)

	got := ri.getPartitioner()
	if _, ok := got.(murmur3Partitioner); !ok {
		t.Errorf("expected murmur3Partitioner from plan, got %T", got)
	}
}

// TestRoutingPlan_GetPartitioner_WithoutPlan verifies that getPartitioner()
// falls back to the mutex-protected field when no plan is set.
func TestRoutingPlan_GetPartitioner_WithoutPlan(t *testing.T) {
	t.Parallel()

	ri := &queryRoutingInfo{}
	op := orderedPartitioner{}
	ri.partitioner = op

	got := ri.getPartitioner()
	if _, ok := got.(orderedPartitioner); !ok {
		t.Errorf("expected orderedPartitioner from fallback, got %T", got)
	}
}

// TestRoutingPlan_QueryKeyspace verifies that Query.Keyspace() prefers the
// plan's keyspace over the fallback field.
func TestRoutingPlan_QueryKeyspace(t *testing.T) {
	t.Parallel()

	q := &Query{routingInfo: &queryRoutingInfo{}}
	plan := &routingPlan{keyspace: "from_plan", table: "t"}
	q.routingInfo.plan.Store(plan)

	if got := q.Keyspace(); got != "from_plan" {
		t.Errorf("expected Keyspace()=%q from plan, got %q", "from_plan", got)
	}
}

// TestRoutingPlan_QueryKeyspace_Fallback verifies that Query.Keyspace()
// falls back to routingInfo.keyspace when no plan is set.
func TestRoutingPlan_QueryKeyspace_Fallback(t *testing.T) {
	t.Parallel()

	q := &Query{routingInfo: &queryRoutingInfo{keyspace: "from_field"}}

	if got := q.Keyspace(); got != "from_field" {
		t.Errorf("expected Keyspace()=%q from field, got %q", "from_field", got)
	}
}

// TestRoutingPlan_QueryKeyspace_GetKeyspaceOverride verifies that the
// getKeyspace function takes precedence over both plan and field.
func TestRoutingPlan_QueryKeyspace_GetKeyspaceOverride(t *testing.T) {
	t.Parallel()

	q := &Query{routingInfo: &queryRoutingInfo{keyspace: "from_field"}}
	q.getKeyspace = func() string { return "from_func" }
	plan := &routingPlan{keyspace: "from_plan"}
	q.routingInfo.plan.Store(plan)

	if got := q.Keyspace(); got != "from_func" {
		t.Errorf("expected Keyspace()=%q from getKeyspace func, got %q", "from_func", got)
	}
}

// TestRoutingPlan_QueryTable verifies that Query.Table() prefers the plan.
func TestRoutingPlan_QueryTable(t *testing.T) {
	t.Parallel()

	q := &Query{routingInfo: &queryRoutingInfo{table: "from_field"}}
	plan := &routingPlan{table: "from_plan"}
	q.routingInfo.plan.Store(plan)

	if got := q.Table(); got != "from_plan" {
		t.Errorf("expected Table()=%q from plan, got %q", "from_plan", got)
	}
}

// TestRoutingPlan_QueryTable_Fallback verifies that Query.Table() falls
// back to routingInfo.table when no plan is set.
func TestRoutingPlan_QueryTable_Fallback(t *testing.T) {
	t.Parallel()

	q := &Query{routingInfo: &queryRoutingInfo{table: "from_field"}}

	if got := q.Table(); got != "from_field" {
		t.Errorf("expected Table()=%q from field, got %q", "from_field", got)
	}
}

// TestRoutingPlan_BatchTable verifies that Batch.Table() prefers the plan.
func TestRoutingPlan_BatchTable(t *testing.T) {
	t.Parallel()

	b := &Batch{routingInfo: &queryRoutingInfo{table: "from_field"}}
	plan := &routingPlan{table: "from_plan"}
	b.routingInfo.plan.Store(plan)

	if got := b.Table(); got != "from_plan" {
		t.Errorf("expected Batch.Table()=%q from plan, got %q", "from_plan", got)
	}
}

// TestBatch_Keyspace_WithPlan verifies that Batch.Keyspace() returns the
// plan's keyspace when a plan is stored, and falls back to b.keyspace
// (session default) when no plan is set.
func TestBatch_Keyspace_WithPlan(t *testing.T) {
	t.Parallel()

	// Without plan: returns session-default b.keyspace
	b := &Batch{
		keyspace:    "session_default",
		routingInfo: &queryRoutingInfo{},
	}
	if got := b.Keyspace(); got != "session_default" {
		t.Errorf("without plan: expected Keyspace()=%q, got %q", "session_default", got)
	}

	// With plan: returns plan's keyspace
	plan := &routingPlan{keyspace: "plan_ks", table: "plan_tbl"}
	b.routingInfo.plan.Store(plan)
	if got := b.Keyspace(); got != "plan_ks" {
		t.Errorf("with plan: expected Keyspace()=%q, got %q", "plan_ks", got)
	}
}

// TestBatch_Table_WithPlan verifies that Batch.Table() returns "" when no
// plan is set (routingInfo.table is never populated for batches), and
// returns the plan's table when a plan is stored.
func TestBatch_Table_WithPlan(t *testing.T) {
	t.Parallel()

	// Without plan: returns "" (routingInfo.table is never set for Batch)
	b := &Batch{
		keyspace:    "session_default",
		routingInfo: &queryRoutingInfo{},
	}
	if got := b.Table(); got != "" {
		t.Errorf("without plan: expected Table()=%q, got %q", "", got)
	}

	// With plan: returns plan's table
	plan := &routingPlan{keyspace: "ks", table: "my_table"}
	b.routingInfo.plan.Store(plan)
	if got := b.Table(); got != "my_table" {
		t.Errorf("with plan: expected Table()=%q, got %q", "my_table", got)
	}
}

// TestBatch_Keyspace_PlanEmptyFallback verifies that when the plan has an
// empty keyspace, Batch.Keyspace() falls through to b.keyspace.
func TestBatch_Keyspace_PlanEmptyFallback(t *testing.T) {
	t.Parallel()

	b := &Batch{
		keyspace:    "session_default",
		routingInfo: &queryRoutingInfo{},
	}
	// Plan with empty keyspace — should fall back to b.keyspace
	plan := &routingPlan{keyspace: "", table: "tbl"}
	b.routingInfo.plan.Store(plan)

	if got := b.Keyspace(); got != "session_default" {
		t.Errorf("expected Keyspace()=%q when plan.keyspace is empty, got %q", "session_default", got)
	}
}

// TestBatch_TabletRouting_EnabledWithPlan verifies that when a Batch has
// a cached routingPlan with keyspace and table set, the tablet routing
// path in tokenAwareHostPolicy.Pick() can match tablet replicas.
// Previously Batch.Table() always returned "" so tablet routing was a
// no-op for batches.
func TestBatch_TabletRouting_EnabledWithPlan(t *testing.T) {
	t.Parallel()

	b := &Batch{
		keyspace:    "session_default",
		routingInfo: &queryRoutingInfo{},
	}
	plan := &routingPlan{keyspace: "ks", table: "tbl"}
	b.routingInfo.plan.Store(plan)

	// After storing a plan, Keyspace() and Table() should return
	// the plan values, which enables tablet routing in Pick().
	if got := b.Keyspace(); got != "ks" {
		t.Errorf("expected Keyspace()=%q from plan, got %q", "ks", got)
	}
	if got := b.Table(); got != "tbl" {
		t.Errorf("expected Table()=%q from plan, got %q", "tbl", got)
	}
}

// TestBatch_TabletRouting_DisabledWithoutPlan verifies that without a
// cached plan, Batch.Table() returns "" which means tablet routing in
// Pick() will find no tablet replicas and fall through to the token ring.
func TestBatch_TabletRouting_DisabledWithoutPlan(t *testing.T) {
	t.Parallel()

	b := &Batch{
		keyspace:    "session_default",
		routingInfo: &queryRoutingInfo{},
	}

	// Without a plan, Table() returns "" — tablet routing cannot match.
	if got := b.Table(); got != "" {
		t.Errorf("expected Table()=%q without plan, got %q", "", got)
	}
	// Keyspace() still returns the session default.
	if got := b.Keyspace(); got != "session_default" {
		t.Errorf("expected Keyspace()=%q without plan, got %q", "session_default", got)
	}
}

// TestBatch_PlanInvalidation verifies that clearing the plan pointer
// (as done on RequestErrUnprepared) reverts Batch.Keyspace() and
// Batch.Table() to their pre-plan values.
func TestBatch_PlanInvalidation(t *testing.T) {
	t.Parallel()

	b := &Batch{
		keyspace:    "session_default",
		routingInfo: &queryRoutingInfo{},
	}

	// Store a plan
	plan := &routingPlan{keyspace: "ks", table: "tbl"}
	b.routingInfo.plan.Store(plan)
	if b.Keyspace() != "ks" || b.Table() != "tbl" {
		t.Fatal("plan should be active")
	}

	// Simulate RequestErrUnprepared: clear the plan pointer
	b.routingInfo.plan.Store(nil)

	if got := b.Keyspace(); got != "session_default" {
		t.Errorf("after invalidation: expected Keyspace()=%q, got %q", "session_default", got)
	}
	if got := b.Table(); got != "" {
		t.Errorf("after invalidation: expected Table()=%q, got %q", "", got)
	}
}

// TestRoutingPlan_QueryReset verifies that Query.reset() clears the plan
// pointer (new queryRoutingInfo has nil plan).
func TestRoutingPlan_QueryReset(t *testing.T) {
	t.Parallel()

	q := &Query{routingInfo: &queryRoutingInfo{}}
	plan := &routingPlan{lwt: true, keyspace: "ks", table: "tbl"}
	q.routingInfo.plan.Store(plan)

	if q.routingInfo.plan.Load() == nil {
		t.Fatal("plan should be set before reset")
	}

	q.reset()

	if q.routingInfo.plan.Load() != nil {
		t.Error("expected plan to be nil after reset()")
	}
	if q.routingInfo.isLWT() {
		t.Error("expected isLWT() to be false after reset()")
	}
}

// TestRoutingPlan_QueryPoolReset verifies that a Query obtained from the
// pool starts with a nil plan.
func TestRoutingPlan_QueryPoolReset(t *testing.T) {
	t.Parallel()

	q := queryPool.Get().(*Query)
	defer queryPool.Put(q)

	if q.routingInfo.plan.Load() != nil {
		t.Error("expected fresh pool Query to have nil plan")
	}
}

// TestRoutingPlan_LRUCache verifies that routingPlanCache stores
// and retrieves plans correctly, Remove deletes them, and the LRU
// evicts the oldest entry when the cache exceeds its capacity.
func TestRoutingPlan_LRUCache(t *testing.T) {
	t.Parallel()

	c := routingPlanLRU{lru: lru.New(2)}

	plan := &routingPlan{keyspace: "ks", table: "tbl", lwt: true}
	c.Put("SELECT * FROM tbl WHERE id = ?", plan)

	got := c.Get("SELECT * FROM tbl WHERE id = ?")
	if got == nil {
		t.Fatal("expected plan to be found in cache")
	}
	if got.keyspace != "ks" || got.table != "tbl" || !got.lwt {
		t.Errorf("unexpected plan fields: %+v", got)
	}

	// Simulate invalidation via Remove
	c.Remove("SELECT * FROM tbl WHERE id = ?")
	if c.Get("SELECT * FROM tbl WHERE id = ?") != nil {
		t.Error("expected plan to be deleted from cache after Remove")
	}

	// Verify LRU eviction: capacity is 2, inserting 3 entries should evict the oldest.
	p1 := &routingPlan{keyspace: "ks1"}
	p2 := &routingPlan{keyspace: "ks2"}
	p3 := &routingPlan{keyspace: "ks3"}
	c.Put("stmt1", p1)
	c.Put("stmt2", p2)
	c.Put("stmt3", p3)

	if c.Get("stmt1") != nil {
		t.Error("expected stmt1 to be evicted (LRU capacity is 2)")
	}
	if c.Get("stmt2") == nil {
		t.Error("expected stmt2 to still be in cache")
	}
	if c.Get("stmt3") == nil {
		t.Error("expected stmt3 to still be in cache")
	}
}

// TestRoutingPlan_LRUCache_PutRace verifies that concurrent Put calls
// for the same statement return the same plan (the winner's plan).
func TestRoutingPlan_LRUCache_PutRace(t *testing.T) {
	t.Parallel()

	c := routingPlanLRU{lru: lru.New(100)}

	const goroutines = 50
	var wg sync.WaitGroup
	results := make([]*routingPlan, goroutines)
	wg.Add(goroutines)

	for i := 0; i < goroutines; i++ {
		go func(idx int) {
			defer wg.Done()
			plan := &routingPlan{keyspace: fmt.Sprintf("ks%d", idx)}
			results[idx] = c.Put("stmt", plan)
		}(i)
	}
	wg.Wait()

	// All results should point to the same plan (the winner).
	winner := results[0]
	for i := 1; i < goroutines; i++ {
		if results[i] != winner {
			t.Errorf("goroutine %d got different plan than goroutine 0", i)
		}
	}
}

// TestRoutingPlan_ConcurrentAccess verifies that concurrent reads of isLWT()
// and getPartitioner() via the atomic plan pointer are safe.
func TestRoutingPlan_ConcurrentAccess(t *testing.T) {
	t.Parallel()

	ri := &queryRoutingInfo{}
	plan := &routingPlan{lwt: true, partitioner: murmur3Partitioner{}}
	ri.plan.Store(plan)

	const goroutines = 100
	var wg sync.WaitGroup
	wg.Add(goroutines * 2)

	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			if !ri.isLWT() {
				t.Error("concurrent isLWT() returned false")
			}
		}()
		go func() {
			defer wg.Done()
			if ri.getPartitioner() == nil {
				t.Error("concurrent getPartitioner() returned nil")
			}
		}()
	}

	wg.Wait()
}

// TestCreateRoutingKeyFromPlan verifies that createRoutingKeyFromPlan
// produces the same output as createRoutingKey for the same inputs.
func TestCreateRoutingKeyFromPlan(t *testing.T) {
	t.Parallel()

	intType := NativeType{proto: 4, typ: TypeInt}

	// Single partition key column
	rki := &routingKeyInfo{
		indexes: []int{0},
		types:   []TypeInfo{intType},
	}
	plan := &routingPlan{
		indexes: []int{0},
		types:   []TypeInfo{intType},
	}
	values := []interface{}{42}

	got1, err1 := createRoutingKey(rki, values)
	if err1 != nil {
		t.Fatalf("createRoutingKey error: %v", err1)
	}
	got2, err2 := createRoutingKeyFromPlan(plan, values)
	if err2 != nil {
		t.Fatalf("createRoutingKeyFromPlan error: %v", err2)
	}
	if string(got1) != string(got2) {
		t.Errorf("routing keys differ: createRoutingKey=%x, createRoutingKeyFromPlan=%x", got1, got2)
	}

	// Composite partition key
	rkiComp := &routingKeyInfo{
		indexes: []int{0, 1},
		types:   []TypeInfo{intType, intType},
	}
	planComp := &routingPlan{
		indexes: []int{0, 1},
		types:   []TypeInfo{intType, intType},
	}
	valuesComp := []interface{}{42, 99}

	got3, err3 := createRoutingKey(rkiComp, valuesComp)
	if err3 != nil {
		t.Fatalf("createRoutingKey (composite) error: %v", err3)
	}
	got4, err4 := createRoutingKeyFromPlan(planComp, valuesComp)
	if err4 != nil {
		t.Fatalf("createRoutingKeyFromPlan (composite) error: %v", err4)
	}
	if string(got3) != string(got4) {
		t.Errorf("composite routing keys differ: createRoutingKey=%x, createRoutingKeyFromPlan=%x", got3, got4)
	}
}

// TestCreateRoutingKeyFromPlan_Nil verifies nil plan returns nil.
func TestCreateRoutingKeyFromPlan_Nil(t *testing.T) {
	t.Parallel()

	got, err := createRoutingKeyFromPlan(nil, []interface{}{42})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != nil {
		t.Errorf("expected nil routing key for nil plan, got %x", got)
	}
}

// ---------------------------------------------------------------------------
// Benchmarks for routingPlan cache (commit: session: add routingPlan cache)
// ---------------------------------------------------------------------------

// BenchmarkIsLWT measures the isLWT() hot path with and without an atomic
// plan pointer. WithPlan exercises the lock-free atomic.Pointer read;
// WithoutPlan exercises the RWMutex.RLock fallback.
func BenchmarkIsLWT(b *testing.B) {
	b.Run("WithPlan", func(b *testing.B) {
		ri := &queryRoutingInfo{}
		ri.plan.Store(&routingPlan{lwt: true})
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = ri.isLWT()
		}
	})

	b.Run("WithoutPlan", func(b *testing.B) {
		ri := &queryRoutingInfo{lwt: true}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = ri.isLWT()
		}
	})
}

// BenchmarkGetPartitioner measures the getPartitioner() hot path with and
// without an atomic plan pointer, mirroring BenchmarkIsLWT.
func BenchmarkGetPartitioner(b *testing.B) {
	part := murmur3Partitioner{}

	b.Run("WithPlan", func(b *testing.B) {
		ri := &queryRoutingInfo{}
		ri.plan.Store(&routingPlan{partitioner: part})
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = ri.getPartitioner()
		}
	})

	b.Run("WithoutPlan", func(b *testing.B) {
		ri := &queryRoutingInfo{partitioner: part}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = ri.getPartitioner()
		}
	})
}

// BenchmarkRoutingPlanCacheLookup measures the LRU-based plan cache
// used by Session.getRoutingPlan(). CacheHit benchmarks the steady-state
// lookup; CacheMiss_Store benchmarks the cold-path Put.
func BenchmarkRoutingPlanCacheLookup(b *testing.B) {
	stmt := "SELECT * FROM ks.tbl WHERE id = ?"
	plan := &routingPlan{keyspace: "ks", table: "tbl", lwt: true}

	b.Run("CacheHit", func(b *testing.B) {
		c := routingPlanLRU{lru: lru.New(1000)}
		c.Put(stmt, plan)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			p := c.Get(stmt)
			if p == nil {
				b.Fatal("expected cache hit")
			}
		}
	})

	b.Run("CacheMiss_Store", func(b *testing.B) {
		// Use more distinct keys than cache capacity so every cycle
		// through the key pool forces evictions, ensuring each Put
		// measures a true cache-miss store rather than a hit.
		const (
			cacheCap = 64
			maxKeys  = 1024
		)
		c := routingPlanLRU{lru: lru.New(cacheCap)}
		stmts := make([]string, maxKeys)
		for i := range stmts {
			stmts[i] = fmt.Sprintf("SELECT * FROM ks.tbl WHERE id = ? /* %d */", i)
		}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			c.Put(stmts[i%maxKeys], plan)
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
		if got[:5] != "testt" {
			t.Errorf("expected prefix from test name, got %q", got)
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

// BenchmarkCreateRoutingKey compares createRoutingKeyFromPlan (plan-based)
// against createRoutingKey (routingKeyInfo-based) for single and composite
// partition keys. The two paths are functionally identical; this benchmark
// verifies no performance regression in the plan-based path.
func BenchmarkCreateRoutingKey(b *testing.B) {
	intType := NativeType{proto: 4, typ: TypeInt}

	// Single partition key
	rkiSingle := &routingKeyInfo{indexes: []int{0}, types: []TypeInfo{intType}}
	planSingle := &routingPlan{indexes: []int{0}, types: []TypeInfo{intType}}
	valsSingle := []interface{}{42}

	// Composite partition key (2 columns)
	rkiComp := &routingKeyInfo{indexes: []int{0, 1}, types: []TypeInfo{intType, intType}}
	planComp := &routingPlan{indexes: []int{0, 1}, types: []TypeInfo{intType, intType}}
	valsComp := []interface{}{42, 99}

	b.Run("FromPlan_Single", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _ = createRoutingKeyFromPlan(planSingle, valsSingle)
		}
	})

	b.Run("FromInfo_Single", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _ = createRoutingKey(rkiSingle, valsSingle)
		}
	})

	b.Run("FromPlan_Composite", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _ = createRoutingKeyFromPlan(planComp, valsComp)
		}
	})

	b.Run("FromInfo_Composite", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _ = createRoutingKey(rkiComp, valsComp)
		}
	})
}
