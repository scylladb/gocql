//go:build unit
// +build unit

// Copyright (c) 2015 The gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package gocql

import (
	"fmt"
	"sync"
	"testing"
)

func TestGetKeyspaceMetadataMissingKeyspaceClosesIter(t *testing.T) {
	t.Parallel()

	framer := &trackingMockFramer{}
	session := &Session{
		useSystemSchema: true,
		control: &systemSchemaTestControl{
			iter: &Iter{framer: framer},
		},
	}

	_, err := getKeyspaceMetadata(session, "missing_keyspace")

	if err != ErrKeyspaceDoesNotExist {
		t.Fatalf("getKeyspaceMetadata() error = %v, want %v", err, ErrKeyspaceDoesNotExist)
	}
	if !framer.released {
		t.Fatal("expected iterator framer to be released on missing keyspace")
	}
}

// Tests metadata "compilation" from example data which might be returned
// from metadata schema queries (see getKeyspaceMetadata, getTableMetadata, and getColumnMetadata)
func TestCompileMetadata(t *testing.T) {
	t.Parallel()

	keyspace := &KeyspaceMetadata{
		Name: "V2Keyspace",
	}
	tables := []TableMetadata{
		{
			Keyspace: "V2Keyspace",
			Name:     "Table1",
		},
		{
			Keyspace: "V2Keyspace",
			Name:     "Table2",
		},
	}
	columns := []ColumnMetadata{
		{
			Keyspace:       "V2Keyspace",
			Table:          "Table1",
			Name:           "KEY1",
			Kind:           ColumnPartitionKey,
			ComponentIndex: 0,
			Type:           "text",
		},
		{
			Keyspace:       "V2Keyspace",
			Table:          "Table1",
			Name:           "Key1",
			Kind:           ColumnPartitionKey,
			ComponentIndex: 0,
			Type:           "text",
		},
		{
			Keyspace:       "V2Keyspace",
			Table:          "Table2",
			Name:           "Column1",
			Kind:           ColumnPartitionKey,
			ComponentIndex: 0,
			Type:           "text",
		},
		{
			Keyspace:       "V2Keyspace",
			Table:          "Table2",
			Name:           "Column2",
			Kind:           ColumnClusteringKey,
			ComponentIndex: 0,
			Type:           "text",
		},
		{
			Keyspace:        "V2Keyspace",
			Table:           "Table2",
			Name:            "Column3",
			Kind:            ColumnClusteringKey,
			ComponentIndex:  1,
			Type:            "text",
			ClusteringOrder: "desc",
		},
		{
			Keyspace: "V2Keyspace",
			Table:    "Table2",
			Name:     "Column4",
			Kind:     ColumnRegular,
			Type:     "text",
		},
		{
			Keyspace: "V2Keyspace",
			Table:    "view",
			Name:     "ColReg",
			Kind:     ColumnRegular,
			Type:     "text",
		},
		{
			Keyspace: "V2Keyspace",
			Table:    "view",
			Name:     "ColCK",
			Kind:     ColumnClusteringKey,
			Type:     "text",
		},
		{
			Keyspace: "V2Keyspace",
			Table:    "view",
			Name:     "ColPK",
			Kind:     ColumnPartitionKey,
			Type:     "text",
		},
		{
			Keyspace:        "V2Keyspace",
			Table:           "buckets_by_owner_index",
			Name:            "idx_token",
			Kind:            ColumnClusteringKey,
			ComponentIndex:  0,
			Type:            "bigint",
			ClusteringOrder: "asc",
		},
		{
			Keyspace:        "V2Keyspace",
			Table:           "buckets_by_owner_index",
			Name:            "name",
			Kind:            ColumnClusteringKey,
			ComponentIndex:  1,
			Type:            "text",
			ClusteringOrder: "asc",
		},
		{
			Keyspace: "V2Keyspace",
			Table:    "buckets_by_owner_index",
			Name:     "owner",
			Kind:     ColumnPartitionKey,
			Type:     "text",
		},
	}
	// Consider an index by column `owner` on the base table `buckets` with
	// partition key `name`.
	//
	// CREATE INDEX buckets_by_owner ON stg_msk_a.buckets(owner);
	indexes := []IndexMetadata{
		{Name: "buckets_by_owner"},
	}
	views := []ViewMetadata{
		{
			KeyspaceName: "V2Keyspace",
			ViewName:     "view",
		},
		{
			KeyspaceName: "V2Keyspace",
			ViewName:     "buckets_by_owner_index",
		},
	}
	compileMetadata(keyspace, tables, columns, nil, nil, nil, indexes, views, nil)
	assertKeyspaceMetadata(
		t,
		keyspace,
		&KeyspaceMetadata{
			Name: "V2Keyspace",
			Views: map[string]*ViewMetadata{
				"view": {
					PartitionKey: []*ColumnMetadata{
						{
							Name: "ColPK",
							Type: "text",
						},
					},
					ClusteringColumns: []*ColumnMetadata{
						{
							Name: "ColCK",
							Type: "text",
						},
					},
					OrderedColumns: []string{
						"ColPK", "ColCK", "ColReg",
					},
					Columns: map[string]*ColumnMetadata{
						"ColPK": {
							Name: "ColPK",
							Kind: ColumnPartitionKey,
							Type: "text",
						},
						"ColCK": {
							Name: "ColCK",
							Kind: ColumnClusteringKey,
							Type: "text",
						},
						"ColReg": {
							Name: "ColReg",
							Kind: ColumnRegular,
							Type: "text",
						},
					},
				},
			},
			Tables: map[string]*TableMetadata{
				"Table1": {
					PartitionKey: []*ColumnMetadata{
						{
							Name: "Key1",
							Type: "text",
						},
					},
					ClusteringColumns: []*ColumnMetadata{},
					Columns: map[string]*ColumnMetadata{
						"KEY1": {
							Name: "KEY1",
							Type: "text",
							Kind: ColumnPartitionKey,
						},
						"Key1": {
							Name: "Key1",
							Type: "text",
							Kind: ColumnPartitionKey,
						},
					},
					OrderedColumns: []string{
						"Key1",
					},
				},
				"Table2": {
					PartitionKey: []*ColumnMetadata{
						{
							Name: "Column1",
							Type: "text",
						},
					},
					ClusteringColumns: []*ColumnMetadata{
						{
							Name:  "Column2",
							Type:  "text",
							Order: ASC,
						},
						{
							Name:  "Column3",
							Type:  "text",
							Order: DESC,
						},
					},
					Columns: map[string]*ColumnMetadata{
						"Column1": {
							Name: "Column1",
							Type: "text",
							Kind: ColumnPartitionKey,
						},
						"Column2": {
							Name:  "Column2",
							Type:  "text",
							Order: ASC,
							Kind:  ColumnClusteringKey,
						},
						"Column3": {
							Name:  "Column3",
							Type:  "text",
							Order: DESC,
							Kind:  ColumnClusteringKey,
						},
						"Column4": {
							Name: "Column4",
							Type: "text",
							Kind: ColumnRegular,
						},
					},
					OrderedColumns: []string{
						"Column1", "Column2", "Column3", "Column4",
					},
				},
			},
			Indexes: map[string]*IndexMetadata{
				"buckets_by_owner": {
					Name:      "buckets_by_owner",
					TableName: "buckets_by_owner_index",
					PartitionKey: []*ColumnMetadata{
						{Name: "owner", Type: "text"},
					},
					ClusteringColumns: []*ColumnMetadata{
						{Name: "idx_token", Type: "bigint"},
						{Name: "name", Type: "text"},
					},
					OrderedColumns: []string{
						"owner", "idx_token", "name",
					},
					Columns: map[string]*ColumnMetadata{
						"owner": {
							Name: "owner",
							Type: "text",
							Kind: ColumnPartitionKey,
						},
						"idx_token": {
							Name:  "idx_token",
							Type:  "bigint",
							Order: ASC,
							Kind:  ColumnClusteringKey,
						},
						"name": {
							Name:  "name",
							Type:  "text",
							Order: ASC,
							Kind:  ColumnClusteringKey,
						},
					},
				},
			},
		},
	)
}

func assertPartitionKey(t *testing.T, keyspaceName, tableName string, actual, expected []*ColumnMetadata) {
	if len(expected) != len(actual) {
		t.Errorf("Expected len(%s.Tables[%s].PartitionKey) to be %v but was %v", keyspaceName, tableName, len(expected), len(actual))
	} else {
		for i := range expected {
			if expected[i].Name != actual[i].Name {
				t.Errorf("Expected %s.Tables[%s].PartitionKey[%d].Name to be '%v' but was '%v'", keyspaceName, tableName, i, expected[i].Name, actual[i].Name)
			}
			if keyspaceName != actual[i].Keyspace {
				t.Errorf("Expected %s.Tables[%s].PartitionKey[%d].Keyspace to be '%v' but was '%v'", keyspaceName, tableName, i, keyspaceName, actual[i].Keyspace)
			}
			if tableName != actual[i].Table {
				t.Errorf("Expected %s.Tables[%s].PartitionKey[%d].Table to be '%v' but was '%v'", keyspaceName, tableName, i, tableName, actual[i].Table)
			}
			if expected[i].Type != actual[i].Type {
				t.Errorf("Expected %s.Tables[%s].PartitionKey[%d].Type.Type to be %v but was %v", keyspaceName, tableName, i, expected[i].Type, actual[i].Type)
			}
			if i != actual[i].ComponentIndex {
				t.Errorf("Expected %s.Tables[%s].PartitionKey[%d].ComponentIndex to be %v but was %v", keyspaceName, tableName, i, i, actual[i].ComponentIndex)
			}
			if ColumnPartitionKey != actual[i].Kind {
				t.Errorf("Expected %s.Tables[%s].PartitionKey[%d].Kind to be '%v' but was '%v'", keyspaceName, tableName, i, ColumnPartitionKey, actual[i].Kind)
			}
		}
	}
}
func assertClusteringColumns(t *testing.T, keyspaceName, tableName string, actual, expected []*ColumnMetadata) {
	if len(expected) != len(actual) {
		t.Errorf("Expected len(%s.Tables[%s].ClusteringColumns) to be %v but was %v", keyspaceName, tableName, len(expected), len(actual))
	} else {
		for i := range expected {
			if actual[i] == nil {
				t.Fatalf("Unexpected nil value: %s.Tables[%s].ClusteringColumns[%d]", keyspaceName, tableName, i)
			}
			if expected[i].Name != actual[i].Name {
				t.Errorf("Expected %s.Tables[%s].ClusteringColumns[%d].Name to be '%v' but was '%v'", keyspaceName, tableName, i, expected[i].Name, actual[i].Name)
			}
			if keyspaceName != actual[i].Keyspace {
				t.Errorf("Expected %s.Tables[%s].ClusteringColumns[%d].Keyspace to be '%v' but was '%v'", keyspaceName, tableName, i, keyspaceName, actual[i].Keyspace)
			}
			if tableName != actual[i].Table {
				t.Errorf("Expected %s.Tables[%s].ClusteringColumns[%d].Table to be '%v' but was '%v'", keyspaceName, tableName, i, tableName, actual[i].Table)
			}
			if expected[i].Type != actual[i].Type {
				t.Errorf("Expected %s.Tables[%s].ClusteringColumns[%d].Type.Type to be %v but was %v", keyspaceName, tableName, i, expected[i].Type, actual[i].Type)
			}
			if i != actual[i].ComponentIndex {
				t.Errorf("Expected %s.Tables[%s].ClusteringColumns[%d].ComponentIndex to be %v but was %v", keyspaceName, tableName, i, i, actual[i].ComponentIndex)
			}
			if expected[i].Order != actual[i].Order {
				t.Errorf("Expected %s.Tables[%s].ClusteringColumns[%d].Order to be %v but was %v", keyspaceName, tableName, i, expected[i].Order, actual[i].Order)
			}
			if ColumnClusteringKey != actual[i].Kind {
				t.Errorf("Expected %s.Tables[%s].ClusteringColumns[%d].Kind to be '%v' but was '%v'", keyspaceName, tableName, i, ColumnClusteringKey, actual[i].Kind)
			}
		}
	}
}

func assertColumns(t *testing.T, keyspaceName, tableName string, actual, expected map[string]*ColumnMetadata) {
	if len(expected) != len(actual) {
		eKeys := make([]string, 0, len(expected))
		for key := range expected {
			eKeys = append(eKeys, key)
		}
		aKeys := make([]string, 0, len(actual))
		for key := range actual {
			aKeys = append(aKeys, key)
		}
		t.Errorf("Expected len(%s.Tables[%s].Columns) to be %v (keys:%v) but was %v (keys:%v)", keyspaceName, tableName, len(expected), eKeys, len(actual), aKeys)
	} else {
		for keyC := range expected {
			ec := expected[keyC]
			ac, found := actual[keyC]

			if !found {
				t.Errorf("Expected %s.Tables[%s].Columns[%s] but was not found", keyspaceName, tableName, keyC)
			} else {
				if keyC != ac.Name {
					t.Errorf("Expected %s.Tables[%s].Columns[%s].Name to be '%v' but was '%v'", keyspaceName, tableName, keyC, keyC, tableName)
				}
				if keyspaceName != ac.Keyspace {
					t.Errorf("Expected %s.Tables[%s].Columns[%s].Keyspace to be '%v' but was '%v'", keyspaceName, tableName, keyC, keyspaceName, ac.Keyspace)
				}
				if tableName != ac.Table {
					t.Errorf("Expected %s.Tables[%s].Columns[%s].Table to be '%v' but was '%v'", keyspaceName, tableName, keyC, tableName, ac.Table)
				}
				if ec.Type != ac.Type {
					t.Errorf("Expected %s.Tables[%s].Columns[%s].Type.Type to be %v but was %v", keyspaceName, tableName, keyC, ec.Type, ac.Type)
				}
				if ec.Order != ac.Order {
					t.Errorf("Expected %s.Tables[%s].Columns[%s].Order to be %v but was %v", keyspaceName, tableName, keyC, ec.Order, ac.Order)
				}
				if ec.Kind != ac.Kind {
					t.Errorf("Expected %s.Tables[%s].Columns[%s].Kind to be '%v' but was '%v'", keyspaceName, tableName, keyC, ec.Kind, ac.Kind)
				}
			}
		}
	}
}

func assertOrderedColumns(t *testing.T, keyspaceName, tableName string, actual, expected []string) {
	if len(expected) != len(actual) {
		t.Errorf("Expected len(%s.Tables[%s].OrderedColumns to be %v but was %v", keyspaceName, tableName, len(expected), len(actual))
	} else {
		for i, eoc := range expected {
			aoc := actual[i]
			if eoc != aoc {
				t.Errorf("Expected %s.Tables[%s].OrderedColumns[%d] to be %s, but was %s", keyspaceName, tableName, i, eoc, aoc)
			}
		}
	}
}

func assertTableMetadata(t *testing.T, keyspaceName string, actual, expected map[string]*TableMetadata) {
	if len(expected) != len(actual) {
		t.Errorf("Expected len(%s.Tables) to be %v but was %v", keyspaceName, len(expected), len(actual))
	}
	for keyT := range expected {
		et := expected[keyT]
		at, found := actual[keyT]

		if !found {
			t.Errorf("Expected %s.Tables[%s] but was not found", keyspaceName, keyT)
		} else {
			if keyT != at.Name {
				t.Errorf("Expected %s.Tables[%s].Name to be %v but was %v", keyspaceName, keyT, keyT, at.Name)
			}
			assertPartitionKey(t, keyspaceName, keyT, at.PartitionKey, et.PartitionKey)
			assertClusteringColumns(t, keyspaceName, keyT, at.ClusteringColumns, et.ClusteringColumns)
			assertColumns(t, keyspaceName, keyT, at.Columns, et.Columns)
			assertOrderedColumns(t, keyspaceName, keyT, at.OrderedColumns, et.OrderedColumns)
		}
	}
}

func assertViewsMetadata(t *testing.T, keyspaceName string, actual, expected map[string]*ViewMetadata) {
	if len(expected) != len(actual) {
		t.Errorf("Expected len(%s.Views) to be %v but was %v", keyspaceName, len(expected), len(actual))
	}
	for keyT := range expected {
		et := expected[keyT]
		at, found := actual[keyT]

		if !found {
			t.Errorf("Expected %s.Views[%s] but was not found", keyspaceName, keyT)
		} else {
			if keyT != at.ViewName {
				t.Errorf("Expected %s.Views[%s].Name to be %v but was %v", keyspaceName, keyT, keyT, at.ViewName)
			}
			assertPartitionKey(t, keyspaceName, keyT, at.PartitionKey, et.PartitionKey)
			assertClusteringColumns(t, keyspaceName, keyT, at.ClusteringColumns, et.ClusteringColumns)
			assertColumns(t, keyspaceName, keyT, at.Columns, et.Columns)
			assertOrderedColumns(t, keyspaceName, keyT, at.OrderedColumns, et.OrderedColumns)
		}
	}
}

func assertIndicesMetadata(t *testing.T, keyspaceName string, actual, expected map[string]*IndexMetadata) {
	if len(expected) != len(actual) {
		t.Errorf("Expected len(%s.Indexes) to be %v but was %v", keyspaceName, len(expected), len(actual))
	}
	for key := range expected {
		viewName := key + "_index"
		et := expected[key]
		at, found := actual[key]

		if !found {
			t.Errorf("Expected %s.Indexes[%s] but was not found", keyspaceName, key)
		} else {
			if et.Name != at.Name {
				t.Errorf("Expected %s.Indexes[%s].Name to be %v but was %v", keyspaceName, key, et.Name, at.Name)
			}
			assertPartitionKey(t, keyspaceName, viewName, at.PartitionKey, et.PartitionKey)
			assertClusteringColumns(t, keyspaceName, viewName, at.ClusteringColumns, et.ClusteringColumns)
			assertColumns(t, keyspaceName, viewName, at.Columns, et.Columns)
			assertOrderedColumns(t, keyspaceName, viewName, at.OrderedColumns, et.OrderedColumns)
		}
	}
}

// Helper function for asserting that actual metadata returned was as expected
func assertKeyspaceMetadata(t *testing.T, actual, expected *KeyspaceMetadata) {
	assertTableMetadata(t, expected.Name, actual.Tables, expected.Tables)
	assertViewsMetadata(t, expected.Name, actual.Views, expected.Views)
	assertIndicesMetadata(t, expected.Name, actual.Indexes, expected.Indexes)
}

func TestCowKeyspaceMetadataMapInvalidateTypes(t *testing.T) {
	t.Parallel()

	m := &cowKeyspaceMetadataMap{}
	ks := &KeyspaceMetadata{
		Name:  "test_ks",
		Types: map[string]*TypeMetadata{"mytype": {Name: "mytype"}},
	}
	m.set("test_ks", ks)

	m.invalidateTypes("test_ks")

	got, found := m.getKeyspace("test_ks")
	if !found {
		t.Fatal("keyspace should still exist after invalidateTypes")
	}
	if got.typesInvalidationGen == 0 {
		t.Fatalf("expected typesInvalidationGen to be bumped, got %d", got.typesInvalidationGen)
	}
	if !got.typesInvalidated {
		t.Fatal("expected typesInvalidated to be true")
	}
	// Types map should still be present (not removed)
	if got.Types == nil {
		t.Fatal("Types map should not be nil")
	}
}

func TestCowKeyspaceMetadataMapInvalidateFunctions(t *testing.T) {
	t.Parallel()

	m := &cowKeyspaceMetadataMap{}
	ks := &KeyspaceMetadata{
		Name:      "test_ks",
		Functions: map[string]*FunctionMetadata{"myfunc": {Name: "myfunc"}},
	}
	m.set("test_ks", ks)

	m.invalidateFunctions("test_ks")

	got, found := m.getKeyspace("test_ks")
	if !found {
		t.Fatal("keyspace should still exist after invalidateFunctions")
	}
	if got.functionsInvalidationGen == 0 {
		t.Fatalf("expected functionsInvalidationGen to be bumped, got %d", got.functionsInvalidationGen)
	}
	if got.aggregatesInvalidationGen == 0 {
		t.Fatalf("expected aggregatesInvalidationGen to be bumped, got %d", got.aggregatesInvalidationGen)
	}
	if !got.functionsInvalidated {
		t.Fatal("expected functionsInvalidated to be true")
	}
	if !got.aggregatesInvalidated {
		t.Fatal("expected aggregatesInvalidated to be true")
	}
}

func TestCowKeyspaceMetadataMapInvalidateAggregates(t *testing.T) {
	t.Parallel()

	m := &cowKeyspaceMetadataMap{}
	ks := &KeyspaceMetadata{
		Name:       "test_ks",
		Aggregates: map[string]*AggregateMetadata{"myagg": {Name: "myagg"}},
	}
	m.set("test_ks", ks)

	m.invalidateAggregates("test_ks")

	got, found := m.getKeyspace("test_ks")
	if !found {
		t.Fatal("keyspace should still exist after invalidateAggregates")
	}
	if got.aggregatesInvalidationGen == 0 {
		t.Fatalf("expected aggregatesInvalidationGen to be bumped, got %d", got.aggregatesInvalidationGen)
	}
	if !got.aggregatesInvalidated {
		t.Fatal("expected aggregatesInvalidated to be true")
	}
}

func TestCowKeyspaceMetadataMapInvalidateFunctionsBumpsGeneration(t *testing.T) {
	t.Parallel()

	m := &cowKeyspaceMetadataMap{}
	m.set("test_ks", &KeyspaceMetadata{Name: "test_ks"})

	m.invalidateFunctions("test_ks")
	first, _ := m.getKeyspace("test_ks")
	firstGen := first.functionsInvalidationGen
	m.invalidateFunctions("test_ks")

	got, found := m.getKeyspace("test_ks")
	if !found {
		t.Fatal("keyspace should still exist after repeated invalidateFunctions")
	}
	if got.functionsInvalidationGen <= firstGen {
		t.Fatalf("expected generation to increase, got %d after %d", got.functionsInvalidationGen, firstGen)
	}
	if got.functionsInvalidationGen == 0 {
		t.Fatalf("expected functionsInvalidationGen to be bumped, got %d", got.functionsInvalidationGen)
	}
	if got.aggregatesInvalidationGen == 0 {
		t.Fatalf("expected aggregatesInvalidationGen to be bumped, got %d", got.aggregatesInvalidationGen)
	}
	if !got.functionsInvalidated || !got.aggregatesInvalidated {
		t.Fatal("expected repeated invalidation to keep dirty flags set")
	}
}

func TestCowKeyspaceMetadataMapReplaceKeyspaceOnEmptyMap(t *testing.T) {
	t.Parallel()

	m := &cowKeyspaceMetadataMap{}
	replacement := &KeyspaceMetadata{Name: "test_ks"}

	m.replaceKeyspace("test_ks", replacement, 0, 0, 0)

	got, found := m.getKeyspace("test_ks")
	if !found {
		t.Fatal("expected keyspace to be stored")
	}
	if got != replacement {
		t.Fatal("expected replacement keyspace to be stored as-is")
	}
}

func TestCowKeyspaceMetadataMapInvalidateNonexistentKeyspace(t *testing.T) {
	t.Parallel()

	m := &cowKeyspaceMetadataMap{}

	// Should not panic on non-existent keyspace
	m.invalidateTypes("nonexistent")
	m.invalidateFunctions("nonexistent")
	m.invalidateAggregates("nonexistent")
}

func TestKeyspaceMetadataNeedsPartialRefresh(t *testing.T) {
	t.Parallel()

	ks := &KeyspaceMetadata{Name: "test_ks"}
	if ks.needsPartialRefresh() {
		t.Fatal("fresh keyspace should not need partial refresh")
	}

	ks.typesInvalidated = true
	ks.typesInvalidationGen = 1
	if !ks.needsPartialRefresh() {
		t.Fatal("should need partial refresh when types invalidated")
	}

	ks.typesInvalidated = false
	ks.typesInvalidationGen = 0
	ks.functionsInvalidated = true
	ks.functionsInvalidationGen = 1
	if !ks.needsPartialRefresh() {
		t.Fatal("should need partial refresh when functions invalidated")
	}

	ks.functionsInvalidated = false
	ks.functionsInvalidationGen = 0
	ks.aggregatesInvalidated = true
	ks.aggregatesInvalidationGen = 1
	if !ks.needsPartialRefresh() {
		t.Fatal("should need partial refresh when aggregates invalidated")
	}
}

func TestKeyspaceMetadataInvalidateFunctionsCascadesToAggregates(t *testing.T) {
	t.Parallel()

	ks := &KeyspaceMetadata{}
	ks.invalidateFunctions()

	if ks.functionsInvalidationGen == 0 {
		t.Fatalf("expected functionsInvalidationGen to be bumped, got %d", ks.functionsInvalidationGen)
	}
	if ks.aggregatesInvalidationGen == 0 {
		t.Fatalf("expected aggregatesInvalidationGen to be bumped, got %d", ks.aggregatesInvalidationGen)
	}
	if !ks.functionsInvalidated || !ks.aggregatesInvalidated {
		t.Fatal("invalidateFunctions should set both dirty flags")
	}
}

func TestKeyspaceMetadataInvalidateTypesOnlyBumpsTypesGeneration(t *testing.T) {
	t.Parallel()

	ks := &KeyspaceMetadata{}
	ks.invalidateTypes()

	if ks.typesInvalidationGen == 0 {
		t.Fatalf("expected typesInvalidationGen to be bumped, got %d", ks.typesInvalidationGen)
	}
	if !ks.typesInvalidated {
		t.Fatal("invalidateTypes should set types dirty flag")
	}
	if ks.functionsInvalidationGen != 0 {
		t.Fatalf("expected functionsInvalidationGen to remain 0, got %d", ks.functionsInvalidationGen)
	}
	if ks.aggregatesInvalidationGen != 0 {
		t.Fatalf("expected aggregatesInvalidationGen to remain 0, got %d", ks.aggregatesInvalidationGen)
	}
}

func TestKeyspaceMetadataClonePreservesInvalidationFlags(t *testing.T) {
	t.Parallel()

	ks := &KeyspaceMetadata{
		Name:                      "test_ks",
		Tables:                    map[string]*TableMetadata{},
		Functions:                 map[string]*FunctionMetadata{},
		Aggregates:                map[string]*AggregateMetadata{},
		Types:                     map[string]*TypeMetadata{},
		Indexes:                   map[string]*IndexMetadata{},
		Views:                     map[string]*ViewMetadata{},
		typesInvalidated:          true,
		functionsInvalidated:      true,
		aggregatesInvalidated:     true,
		typesInvalidationGen:      3,
		functionsInvalidationGen:  4,
		aggregatesInvalidationGen: 5,
	}

	cloned := ks.Clone()
	if cloned.typesInvalidationGen != 3 {
		t.Fatalf("Clone should preserve typesInvalidationGen, got %d", cloned.typesInvalidationGen)
	}
	if !cloned.typesInvalidated {
		t.Fatal("Clone should preserve typesInvalidated")
	}
	if cloned.functionsInvalidationGen != 4 {
		t.Fatalf("Clone should preserve functionsInvalidationGen, got %d", cloned.functionsInvalidationGen)
	}
	if !cloned.functionsInvalidated {
		t.Fatal("Clone should preserve functionsInvalidated")
	}
	if cloned.aggregatesInvalidationGen != 5 {
		t.Fatalf("Clone should preserve aggregatesInvalidationGen, got %d", cloned.aggregatesInvalidationGen)
	}
	if !cloned.aggregatesInvalidated {
		t.Fatal("Clone should preserve aggregatesInvalidated")
	}
}

// With categories carried forward, the DESCRIBE fetched later (higher
// createStmtsSeq) wins; a still-dirty entry has no usable text.
func TestCowKeyspaceMetadataMapReplaceKeyspaceKeepsCreateStmtsAfterCompletedPartialRefresh(t *testing.T) {
	t.Parallel()

	for _, cat := range []string{"types", "functions", "aggregates"} {
		for _, mode := range []string{"clean/partial newer", "clean/full newer", "dirty"} {
			dirty := mode == "dirty"
			t.Run(fmt.Sprintf("%s/%s", cat, mode), func(t *testing.T) {
				t.Parallel()

				m := &cowKeyspaceMetadataMap{}
				m.set("test_ks", &KeyspaceMetadata{Name: "test_ks", CreateStmts: "old"})

				newTypes := map[string]*TypeMetadata{"t": {Name: "t"}}
				newFuncs := map[string]*FunctionMetadata{"f": {Name: "f"}}
				newAggs := map[string]*AggregateMetadata{"a": {Name: "a"}}
				m.updateKeyspace("test_ks", func(ks *KeyspaceMetadata) {
					switch cat {
					case "types":
						ks.invalidateTypes()
						ks.Types, ks.typesInvalidated = newTypes, dirty
					case "functions":
						ks.invalidateFunctions()
						ks.Functions, ks.functionsInvalidated = newFuncs, dirty
						ks.Aggregates, ks.aggregatesInvalidated = newAggs, dirty
					case "aggregates":
						ks.aggregatesInvalidationGen++
						ks.Aggregates, ks.aggregatesInvalidated = newAggs, dirty
					}
					ks.CreateStmts, ks.createStmtsSeq = "partial describe", 10
					if dirty {
						ks.CreateStmts = ""
					}
				})

				replSeq, want := uint64(5), "partial describe"
				switch mode {
				case "clean/full newer":
					replSeq, want = 20, "full describe"
				case "dirty":
					want = ""
				}
				replacement := &KeyspaceMetadata{Name: "test_ks", CreateStmts: "full describe", createStmtsSeq: replSeq}
				m.replaceKeyspace("test_ks", replacement, 0, 0, 0)

				got, _ := m.getKeyspace("test_ks")
				if got.CreateStmts != want {
					t.Fatalf("CreateStmts = %q, want %q", got.CreateStmts, want)
				}
				switch cat {
				case "types":
					if len(got.Types) != 1 {
						t.Fatalf("expected newer types preserved, got %v", got.Types)
					}
				case "functions":
					if len(got.Functions) != 1 || len(got.Aggregates) != 1 {
						t.Fatalf("expected newer functions/aggregates preserved, got %v %v", got.Functions, got.Aggregates)
					}
				case "aggregates":
					if len(got.Aggregates) != 1 {
						t.Fatalf("expected newer aggregates preserved, got %v", got.Aggregates)
					}
				}
			})
		}
	}
}

func TestKeyspaceMetadataToCQLConcurrentNoRace(t *testing.T) {
	t.Parallel()

	ks := &KeyspaceMetadata{Name: "ks", StrategyClass: "SimpleStrategy", StrategyOptions: map[string]any{"replication_factor": "1"}}

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := ks.ToCQL(); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
}

func TestInvalidationGenerationNotReusedAfterKeyspaceRecreate(t *testing.T) {
	t.Parallel()

	m := &cowKeyspaceMetadataMap{}
	m.set("ks", &KeyspaceMetadata{Name: "ks"})
	m.invalidateTypes("ks")
	old, _ := m.getKeyspace("ks")
	snapshotGen := old.typesInvalidationGen

	m.removeKeyspace("ks")
	m.set("ks", &KeyspaceMetadata{Name: "ks"})
	m.invalidateTypes("ks")

	// Mirrors the apply guard in refreshPartialKeyspaceSchema.
	stale := m.updateKeyspace("ks", func(ks *KeyspaceMetadata) {
		if ks.typesInvalidated && ks.typesInvalidationGen == snapshotGen {
			ks.typesInvalidated = false
		}
	})
	if !stale {
		t.Fatal("keyspace should exist")
	}
	got, _ := m.getKeyspace("ks")
	if !got.typesInvalidated {
		t.Fatal("stale partial result must be rejected; dirty flag was cleared")
	}
}
