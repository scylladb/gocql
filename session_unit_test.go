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
	"strings"
	"testing"

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
