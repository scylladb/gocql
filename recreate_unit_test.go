//go:build all || unit
// +build all unit

// Copyright (C) 2017 ScyllaDB

package gocql

import (
	"encoding/json"
	"os"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestCQLHelpersEscape(t *testing.T) {
	tests := []struct {
		name string
		in   any
		want string
	}{
		{"int", 42, "42"},
		{"float64", 3.5, "3.5"},
		{"bool true", true, "true"},
		{"bool false", false, "false"},
		{"string", "it's fine", "'it''s fine'"},
		{"bytes", []byte("raw"), "raw"},
		{"unsupported type", []int{1, 2}, ""},
		{"nil", nil, ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := cqlHelpers.escape(tc.in); got != tc.want {
				t.Errorf("escape(%#v) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}

func TestCQLHelpersFixStrategy(t *testing.T) {
	tests := []struct{ in, want string }{
		{"org.apache.cassandra.locator.NetworkTopologyStrategy", "NetworkTopologyStrategy"},
		{"org.apache.cassandra.locator.SimpleStrategy", "SimpleStrategy"},
		{"NetworkTopologyStrategy", "NetworkTopologyStrategy"}, // already short, no-op
	}
	for _, tc := range tests {
		if got := cqlHelpers.fixStrategy(tc.in); got != tc.want {
			t.Errorf("fixStrategy(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

func TestCQLHelpersStripFrozen(t *testing.T) {
	tests := []struct{ in, want string }{
		{"frozen<list<int>>", "list<int>"},
		{"text", "text"}, // no frozen wrapper, no-op
	}
	for _, tc := range tests {
		if got := cqlHelpers.stripFrozen(tc.in); got != tc.want {
			t.Errorf("stripFrozen(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

func TestCQLHelpersZip(t *testing.T) {
	got := cqlHelpers.zip([]string{"a", "b"}, []string{"int", "text"})
	want := [][]string{{"a", "int"}, {"b", "text"}}
	if !cmp.Equal(got, want) {
		t.Errorf("zip() diff:\n%s", cmp.Diff(want, got))
	}
}

func TestCQLHelpersPartitionKeyString(t *testing.T) {
	pk1 := &ColumnMetadata{Name: "id"}
	pk2a := &ColumnMetadata{Name: "a"}
	pk2b := &ColumnMetadata{Name: "b"}
	ck := &ColumnMetadata{Name: "ts"}

	tests := []struct {
		name string
		pks  []*ColumnMetadata
		cks  []*ColumnMetadata
		want string
	}{
		{"single pk, no clustering", []*ColumnMetadata{pk1}, nil, "id"},
		{"single pk with clustering", []*ColumnMetadata{pk1}, []*ColumnMetadata{ck}, "id, ts"},
		{"composite pk", []*ColumnMetadata{pk2a, pk2b}, nil, "(a, b)"},
		{"composite pk with clustering", []*ColumnMetadata{pk2a, pk2b}, []*ColumnMetadata{ck}, "(a, b), ts"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := cqlHelpers.partitionKeyString(tc.pks, tc.cks); got != tc.want {
				t.Errorf("partitionKeyString() = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestTypesSortedTopologically(t *testing.T) {
	// "address" is referenced by "person" (person.FieldTypes contains "address"),
	// so it must sort before "person".
	ks := &KeyspaceMetadata{
		Types: map[string]*TypeMetadata{
			"person": {
				Name:       "person",
				FieldNames: []string{"name", "home"},
				FieldTypes: []string{"text", "frozen<address>"},
			},
			"address": {
				Name:       "address",
				FieldNames: []string{"street"},
				FieldTypes: []string{"text"},
			},
		},
	}

	sorted := ks.typesSortedTopologically()
	if len(sorted) != 2 {
		t.Fatalf("expected 2 types, got %d", len(sorted))
	}
	if sorted[0].Name != "address" || sorted[1].Name != "person" {
		t.Errorf("expected [address, person], got [%s, %s]", sorted[0].Name, sorted[1].Name)
	}
}

func TestKeyspaceToCQL(t *testing.T) {
	tests := []struct {
		name string
		ks   *KeyspaceMetadata
		want string
	}{
		{
			name: "network topology strategy, durable writes true",
			ks: &KeyspaceMetadata{
				Name:            "ks1",
				StrategyClass:   "org.apache.cassandra.locator.NetworkTopologyStrategy",
				StrategyOptions: map[string]any{"datacenter1": "3"},
				DurableWrites:   true,
			},
			want: "CREATE KEYSPACE ks1 WITH replication = {\n    'class': 'NetworkTopologyStrategy',\n    'datacenter1': '3'\n};\n",
		},
		{
			name: "durable writes false is rendered explicitly",
			ks: &KeyspaceMetadata{
				Name:            "ks2",
				StrategyClass:   "org.apache.cassandra.locator.SimpleStrategy",
				StrategyOptions: map[string]any{"replication_factor": "1"},
				DurableWrites:   false,
			},
			want: "CREATE KEYSPACE ks2 WITH replication = {\n    'class': 'SimpleStrategy',\n    'replication_factor': '1'\n} AND durable_writes = 'false';\n",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var sb strings.Builder
			if err := tc.ks.keyspaceToCQL(&sb); err != nil {
				t.Fatal(err)
			}
			if got := sb.String(); got != tc.want {
				t.Errorf("keyspaceToCQL() diff:\n%s", cmp.Diff(tc.want, got))
			}
		})
	}
}

// TestKeyspaceToCQL_TabletsNotModeled documents a known gap (scylladb/gocql#1055):
// KeyspaceMetadata has no field for Scylla's tablets replication setting, so
// keyspaceToCQL can never render a `tablets = {...}` clause. Flip this test once
// tablets is modeled and rendered.
func TestKeyspaceToCQL_TabletsNotModeled(t *testing.T) {
	ks := &KeyspaceMetadata{
		Name:            "ks1",
		StrategyClass:   "org.apache.cassandra.locator.NetworkTopologyStrategy",
		StrategyOptions: map[string]any{"datacenter1": "2"},
		DurableWrites:   true,
	}
	var sb strings.Builder
	if err := ks.keyspaceToCQL(&sb); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(sb.String(), "tablets") {
		t.Fatal("keyspaceToCQL now renders a tablets clause: update this test and see scylladb/gocql#1055")
	}
}

func TestUserTypeToCQL(t *testing.T) {
	ks := &KeyspaceMetadata{}
	tm := &TypeMetadata{
		Keyspace:   "ks1",
		Name:       "address",
		FieldNames: []string{"street", "city"},
		FieldTypes: []string{"text", "text"},
	}
	var sb strings.Builder
	if err := ks.userTypeToCQL(&sb, tm); err != nil {
		t.Fatal(err)
	}
	want := "\nCREATE TYPE ks1.address (\n    street text,\n    city text\n);\n"
	if got := sb.String(); got != want {
		t.Errorf("userTypeToCQL() diff:\n%s", cmp.Diff(want, got))
	}
}

func TestFunctionToCQL(t *testing.T) {
	ks := &KeyspaceMetadata{}
	fm := &FunctionMetadata{
		Name:              "avgstate",
		ArgumentNames:     []string{"state", "val"},
		ArgumentTypes:     []string{"frozen<tuple<int,bigint>>", "int"},
		CalledOnNullInput: false,
		ReturnType:        "frozen<tuple<int,bigint>>",
		Language:          "lua",
		Body:              "return state",
	}
	var sb strings.Builder
	if err := ks.functionToCQL(&sb, "ks1", fm); err != nil {
		t.Fatal(err)
	}
	want := "\nCREATE FUNCTION ks1.avgstate (state\n    tuple<int,bigint>, val\n    int)\n    RETURNS NULL ON NULL INPUT\n    RETURNS frozen<tuple<int,bigint>>\n    LANGUAGE lua\n    AS $$return state$$;\n"
	if got := sb.String(); got != want {
		t.Errorf("functionToCQL() diff:\n%s", cmp.Diff(want, got))
	}
}

func TestAggregateToCQL(t *testing.T) {
	ks := &KeyspaceMetadata{}
	am := &AggregateMetadata{
		Name:          "myavg",
		ArgumentTypes: []string{"frozen<int>"},
		StateType:     "frozen<tuple<int,bigint>>",
		StateFunc:     FunctionMetadata{Name: "avgstate"},
		FinalFunc:     FunctionMetadata{Name: "avgfinal"},
		InitCond:      "(0, 0)",
	}
	var sb strings.Builder
	if err := ks.aggregateToCQL(&sb, am); err != nil {
		t.Fatal(err)
	}
	got := sb.String()
	if !strings.Contains(got, "CREATE AGGREGATE .myavg") ||
		!strings.Contains(got, "int") ||
		!strings.Contains(got, "SFUNC avgstate") ||
		!strings.Contains(got, "STYPE tuple<int,bigint>") ||
		!strings.Contains(got, "FINALFUNC avgfinal") ||
		!strings.Contains(got, "INITCOND (0, 0)") {
		t.Errorf("aggregateToCQL() unexpected output:\n%s", got)
	}
}

func TestViewToCQL(t *testing.T) {
	ks := &KeyspaceMetadata{}
	vm := &ViewMetadata{
		KeyspaceName:      "ks1",
		ViewName:          "by_email",
		BaseTableName:     "users",
		WhereClause:       "email IS NOT NULL",
		IncludeAllColumns: true,
		PartitionKey:      []*ColumnMetadata{{Name: "email"}},
		ClusteringColumns: []*ColumnMetadata{{Name: "id", ClusteringOrder: "ASC"}},
	}
	var sb strings.Builder
	if err := ks.viewToCQL(&sb, vm); err != nil {
		t.Fatal(err)
	}
	got := sb.String()
	if !strings.Contains(got, "CREATE MATERIALIZED VIEW ks1.by_email AS") ||
		!strings.Contains(got, "SELECT *") ||
		!strings.Contains(got, "FROM ks1.users") ||
		!strings.Contains(got, "WHERE email IS NOT NULL") ||
		!strings.Contains(got, "PRIMARY KEY (email, id)") {
		t.Errorf("viewToCQL() unexpected output:\n%s", got)
	}
}

func TestIndexToCQL(t *testing.T) {
	ks := &KeyspaceMetadata{}

	t.Run("regular index", func(t *testing.T) {
		im := &IndexMetadata{
			Name:         "idx_by_name",
			KeyspaceName: "ks1",
			TableName:    "users",
			Kind:         "COMPOSITES",
			Options:      map[string]string{"target": "name"},
		}
		var sb strings.Builder
		if err := ks.indexToCQL(&sb, im); err != nil {
			t.Fatal(err)
		}
		want := "\nCREATE INDEX idx_by_name ON ks1.users (name);\n"
		if got := sb.String(); got != want {
			t.Errorf("indexToCQL() diff:\n%s", cmp.Diff(want, got))
		}
	})

	t.Run("custom index kind is skipped", func(t *testing.T) {
		im := &IndexMetadata{Name: "idx_custom", KeyspaceName: "ks1", TableName: "users", Kind: IndexKindCustom}
		var sb strings.Builder
		if err := ks.indexToCQL(&sb, im); err != nil {
			t.Fatal(err)
		}
		if got := sb.String(); got != "" {
			t.Errorf("indexToCQL() for custom index should emit nothing, got %q", got)
		}
	})

	t.Run("composite partition/clustering target", func(t *testing.T) {
		im := &IndexMetadata{
			Name:         "idx_composite",
			KeyspaceName: "ks1",
			TableName:    "users",
			Kind:         "COMPOSITES",
			Options:      map[string]string{"target": `{"pk":["a","b"],"ck":["c"]}`},
		}
		var sb strings.Builder
		if err := ks.indexToCQL(&sb, im); err != nil {
			t.Fatal(err)
		}
		want := "\nCREATE INDEX idx_composite ON ks1.users ((a,b), c);\n"
		if got := sb.String(); got != want {
			t.Errorf("indexToCQL() diff:\n%s", cmp.Diff(want, got))
		}
	})
}

func TestTableToCQL(t *testing.T) {
	ks := &KeyspaceMetadata{}
	tm := &TableMetadata{
		Name:           "users",
		OrderedColumns: []string{"id", "name"},
		Columns: map[string]*ColumnMetadata{
			"id":   {Name: "id", Type: "uuid"},
			"name": {Name: "name", Type: "text"},
		},
		PartitionKey: []*ColumnMetadata{{Name: "id"}},
		Options: TableMetadataOptions{
			Comment: "test table",
		},
	}
	var sb strings.Builder
	if err := ks.tableToCQL(&sb, "ks1", tm); err != nil {
		t.Fatal(err)
	}
	got := sb.String()
	if !strings.Contains(got, "CREATE TABLE ks1.users (") ||
		!strings.Contains(got, "id uuid PRIMARY KEY") ||
		!strings.Contains(got, "name text") ||
		!strings.Contains(got, "comment = 'test table'") {
		t.Errorf("tableToCQL() unexpected output:\n%s", got)
	}
}

func TestTableToCQL_CompositePartitionAndClustering(t *testing.T) {
	ks := &KeyspaceMetadata{}
	tm := &TableMetadata{
		Name:              "events",
		OrderedColumns:    []string{"tenant", "day", "ts", "payload"},
		PartitionKey:      []*ColumnMetadata{{Name: "tenant"}, {Name: "day"}},
		ClusteringColumns: []*ColumnMetadata{{Name: "ts", ClusteringOrder: "DESC"}},
		Columns: map[string]*ColumnMetadata{
			"tenant":  {Name: "tenant", Type: "text"},
			"day":     {Name: "day", Type: "date"},
			"ts":      {Name: "ts", Type: "timestamp"},
			"payload": {Name: "payload", Type: "blob"},
		},
	}
	var sb strings.Builder
	if err := ks.tableToCQL(&sb, "ks1", tm); err != nil {
		t.Fatal(err)
	}
	got := sb.String()
	if !strings.Contains(got, "PRIMARY KEY ((tenant, day), ts)") ||
		!strings.Contains(got, "CLUSTERING ORDER BY (ts DESC)") {
		t.Errorf("tableToCQL() unexpected output:\n%s", got)
	}
}

func TestToCQL_CachedShortCircuit(t *testing.T) {
	// When CreateStmts is already populated (e.g. from a server DESCRIBE
	// response), ToCQL must return it verbatim without touching any of the
	// (possibly stale or incomplete) struct fields.
	ks := &KeyspaceMetadata{
		Name:        "ignored",
		CreateStmts: "-- cached statements --",
	}
	got, err := ks.ToCQL()
	if err != nil {
		t.Fatal(err)
	}
	if got != "-- cached statements --" {
		t.Errorf("ToCQL() = %q, want cached value unchanged", got)
	}
}

func TestToCQL_BuildsFromMetadataWhenUncached(t *testing.T) {
	ks := &KeyspaceMetadata{
		Name:          "ks1",
		StrategyClass: "org.apache.cassandra.locator.SimpleStrategy",
		StrategyOptions: map[string]any{
			"replication_factor": "1",
		},
		DurableWrites: true,
		Types: map[string]*TypeMetadata{
			"address": {Keyspace: "ks1", Name: "address", FieldNames: []string{"street"}, FieldTypes: []string{"text"}},
		},
		Tables: map[string]*TableMetadata{
			"users": {
				Name:           "users",
				OrderedColumns: []string{"id"},
				PartitionKey:   []*ColumnMetadata{{Name: "id"}},
				Columns:        map[string]*ColumnMetadata{"id": {Name: "id", Type: "uuid"}},
			},
		},
	}

	got, err := ks.ToCQL()
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(got, "CREATE KEYSPACE ks1") ||
		!strings.Contains(got, "CREATE TYPE ks1.address") ||
		!strings.Contains(got, "CREATE TABLE ks1.users") {
		t.Errorf("ToCQL() missing expected statements:\n%s", got)
	}
	// Result is cached back onto CreateStmts.
	if ks.CreateStmts != got {
		t.Errorf("ToCQL() did not populate CreateStmts cache")
	}
}

func TestScyllaEncryptionOptionsUnmarshalBinary(t *testing.T) {
	const (
		input  = "testdata/recreate/scylla_encryption_options.bin"
		golden = "testdata/recreate/scylla_encryption_options_golden.json"
	)

	inputBuf, err := os.ReadFile(input)
	if err != nil {
		t.Fatal(err)
	}

	goldenBuf, err := os.ReadFile(golden)
	if err != nil {
		t.Fatal(err)
	}

	goldenOpts := &scyllaEncryptionOptions{}
	if err := json.Unmarshal(goldenBuf, goldenOpts); err != nil {
		t.Fatal(err)
	}

	opts := &scyllaEncryptionOptions{}
	if err := opts.UnmarshalBinary(inputBuf); err != nil {
		t.Fatal(err)
	}

	if !cmp.Equal(goldenOpts, opts) {
		t.Error(cmp.Diff(goldenOpts, opts))
	}
}
