//go:build unit
// +build unit

/*
 * Copyright (C) 2026 ScyllaDB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package gocql

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"
	"slices"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
)

// KeyspaceMetadata.ToCQL and everything it calls had no coverage in either
// lane. TestRecreateSchema appears to cover them, but it does not: ToCQL
// short-circuits on a non-empty CreateStmts, which any server supporting
// DESCRIBE KEYSPACE ... WITH INTERNALS populates, so on a modern cluster the
// generator never runs. These tests drive it directly from hand-built
// metadata, which also means no cluster and no dependence on how a particular
// server reports its schema.

func col(name, typ string, kind ColumnKind) *ColumnMetadata {
	return &ColumnMetadata{Name: name, Type: typ, Kind: kind}
}

// TestToCQLUsesServerStatementsWhenPresent pins the short-circuit itself: a
// keyspace that already carries CreateStmts is echoed back untouched, and the
// generator is not consulted. This is the branch that hides all the others.
func TestToCQLUsesServerStatementsWhenPresent(t *testing.T) {
	t.Parallel()

	ks := &KeyspaceMetadata{
		Name:        "ks",
		CreateStmts: "CREATE KEYSPACE server_said_so;",
		// Deliberately inconsistent with CreateStmts: if the generator ran, the
		// output would mention this table.
		Tables: map[string]*TableMetadata{"t": {
			Name:           "t",
			OrderedColumns: []string{"id"},
			Columns:        map[string]*ColumnMetadata{"id": col("id", "int", ColumnPartitionKey)},
			PartitionKey:   []*ColumnMetadata{col("id", "int", ColumnPartitionKey)},
		}},
	}

	got, err := ks.ToCQL()
	if err != nil {
		t.Fatalf("ToCQL: %v", err)
	}
	if got != "CREATE KEYSPACE server_said_so;" {
		t.Errorf("ToCQL returned %q, want the stored CreateStmts verbatim", got)
	}
	if strings.Contains(got, "CREATE TABLE") {
		t.Error("the generator ran even though CreateStmts was populated")
	}
}

func TestKeyspaceToCQL(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name  string
		ks    *KeyspaceMetadata
		wants []string
	}{
		{
			name: "network topology strategy is emitted unqualified",
			ks: &KeyspaceMetadata{
				Name:            "ks",
				DurableWrites:   true,
				StrategyClass:   "org.apache.cassandra.locator.NetworkTopologyStrategy",
				StrategyOptions: map[string]any{"dc1": "3"},
			},
			// fixStrategy drops the org.apache.cassandra.locator. prefix.
			wants: []string{
				"CREATE KEYSPACE ks WITH replication = {",
				"'class': 'NetworkTopologyStrategy'",
				"'dc1': '3'",
				"durable_writes = true",
			},
		},
		{
			name: "durable_writes false is emitted explicitly",
			ks: &KeyspaceMetadata{
				Name:            "ks",
				DurableWrites:   false,
				StrategyClass:   "SimpleStrategy",
				StrategyOptions: map[string]any{"replication_factor": "1"},
			},
			wants: []string{"'class': 'SimpleStrategy'", "durable_writes = false"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got, err := tc.ks.ToCQL()
			if err != nil {
				t.Fatalf("ToCQL: %v", err)
			}
			for _, want := range tc.wants {
				if !strings.Contains(got, want) {
					t.Errorf("output is missing %q\n--- got ---\n%s", want, got)
				}
			}
		})
	}
}

func TestTypesSortedTopologically(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name  string
		types []*TypeMetadata
		// want, when set, is the exact order expected. Most cases only assert
		// the invariant, because several orders are equally valid; the
		// collision cases have one correct answer.
		want []string
	}{
		{
			name: "one dependency edge",
			types: []*TypeMetadata{
				{Keyspace: "ks", Name: "inner", FieldNames: []string{"a"}, FieldTypes: []string{"int"}},
				{Keyspace: "ks", Name: "outer", FieldNames: []string{"b"}, FieldTypes: []string{"frozen<inner>"}},
			},
			want: []string{"inner", "outer"},
		},
		{
			// A single edge is settled by one comparison, so it cannot show
			// whether the ordering is a real traversal. A chain needs the
			// relation to compose: inner must precede outer even though
			// nothing states that directly.
			name: "three-level chain",
			types: []*TypeMetadata{
				{Keyspace: "ks", Name: "inner", FieldNames: []string{"a"}, FieldTypes: []string{"int"}},
				{Keyspace: "ks", Name: "middle", FieldNames: []string{"b"}, FieldTypes: []string{"frozen<inner>"}},
				{Keyspace: "ks", Name: "outer", FieldNames: []string{"c"}, FieldTypes: []string{"frozen<middle>"}},
			},
			want: []string{"inner", "middle", "outer"},
		},
		{
			// "varchar" contains "var". Matching substrings would read that
			// as a dependency of a on var, closing a cycle with the real
			// var -> a edge and emitting var before the a it embeds.
			name: "a type name that is a substring of a built-in",
			types: []*TypeMetadata{
				{Keyspace: "ks", Name: "a", FieldNames: []string{"s"}, FieldTypes: []string{"varchar"}},
				{Keyspace: "ks", Name: "var", FieldNames: []string{"x"}, FieldTypes: []string{"frozen<a>"}},
			},
			want: []string{"a", "var"},
		},
		{
			// Same collision between two user types rather than with a
			// built-in: "inner" contains "in".
			name: "a type name that is a substring of another type name",
			types: []*TypeMetadata{
				{Keyspace: "ks", Name: "in", FieldNames: []string{"p"}, FieldTypes: []string{"frozen<inner>"}},
				{Keyspace: "ks", Name: "inner", FieldNames: []string{"q"}, FieldTypes: []string{"int"}},
			},
			want: []string{"inner", "in"},
		},
		{
			// system_schema.types keys a UDT by its unquoted name but stores
			// the reference with its quotes: "z-type" is keyed z-type and
			// referenced as frozen<"z-type">. Splitting on punctuation would
			// yield "z" and "type" and drop the dependency.
			name: "a quoted type name containing punctuation",
			types: []*TypeMetadata{
				{Keyspace: "ks", Name: "holder", FieldNames: []string{"x"}, FieldTypes: []string{`frozen<"z-type">`}},
				{Keyspace: "ks", Name: "z-type", FieldNames: []string{"a"}, FieldTypes: []string{"int"}},
			},
			want: []string{"z-type", "holder"},
		},
		{
			// A user type may be named after a built-in, quoted at creation.
			// A bare "text" field is then the built-in, not a reference to
			// that type: following it would close a cycle with the real
			// text -> t2 edge and emit text before the t2 it embeds.
			name: "a user type named after a built-in",
			types: []*TypeMetadata{
				{Keyspace: "ks", Name: "text", FieldNames: []string{"x"}, FieldTypes: []string{"frozen<t2>"}},
				{Keyspace: "ks", Name: "t2", FieldNames: []string{"p"}, FieldTypes: []string{"text"}},
			},
			want: []string{"t2", "text"},
		},
		{
			// The other direction: frozen<text> is a genuine reference to that
			// user type -- frozen never wraps a scalar built-in -- so the edge
			// must still be followed even though the bare text beside it is not.
			name: "a built-in and a user type of the same name in one field list",
			types: []*TypeMetadata{
				{Keyspace: "ks", Name: "text", FieldNames: []string{"a"}, FieldTypes: []string{"int"}},
				{Keyspace: "ks", Name: "holder", FieldNames: []string{"p", "q"}, FieldTypes: []string{"text", "frozen<text>"}},
			},
			want: []string{"text", "holder"},
		},
		{
			// frozen<map<text,int>> is a frozen collection, not a reference to
			// a type named map. Treating the operand of frozen as a user type
			// without checking for a constructor would invent an edge here.
			name: "a frozen collection beside a user type named after its constructor",
			types: []*TypeMetadata{
				{Keyspace: "ks", Name: "map", FieldNames: []string{"x"}, FieldTypes: []string{"frozen<other>"}},
				{Keyspace: "ks", Name: "other", FieldNames: []string{"y"}, FieldTypes: []string{"frozen<map<text,int>>"}},
			},
			want: []string{"other", "map"},
		},
		{
			// The other direction: a constructor name written without
			// arguments is a genuine reference, and the server stores it
			// unquoted, so the edge must still be followed.
			name: "a user type named after a constructor, actually referenced",
			types: []*TypeMetadata{
				{Keyspace: "ks", Name: "map", FieldNames: []string{"a"}, FieldTypes: []string{"int"}},
				{Keyspace: "ks", Name: "holder", FieldNames: []string{"m"}, FieldTypes: []string{"frozen<map>"}},
			},
			want: []string{"map", "holder"},
		},
		{
			// A custom marshal type is a dotted Java class name. Tokenising it
			// would invent edges to any user type sharing one of its segments.
			name: "a custom marshal type beside a user type named after its package",
			types: []*TypeMetadata{
				{Keyspace: "ks", Name: "org", FieldNames: []string{"x"}, FieldTypes: []string{"frozen<other>"}},
				{Keyspace: "ks", Name: "other", FieldNames: []string{"y"}, FieldTypes: []string{`'org.apache.cassandra.db.marshal.UTF8Type'`}},
			},
			want: []string{"other", "org"},
		},
		{
			name: "one type embedded by two others",
			types: []*TypeMetadata{
				{Keyspace: "ks", Name: "shared", FieldNames: []string{"a"}, FieldTypes: []string{"int"}},
				{Keyspace: "ks", Name: "left", FieldNames: []string{"b"}, FieldTypes: []string{"frozen<shared>"}},
				{Keyspace: "ks", Name: "right", FieldNames: []string{"c"}, FieldTypes: []string{"list<frozen<shared>>"}},
			},
			// shared sorts last by name, so this order can only come from the
			// dependency walk -- unlike the two cases above, where the correct
			// order happens to be alphabetical.
			want: []string{"shared", "left", "right"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			// typesSortedTopologically reads ks.Types, a map, so the order it
			// starts from varies per call. Sample it rather than relying on
			// one draw: with two types, one of the two possible orders is
			// already correct, so a single round would let a sorter that does
			// nothing through about half the time.
			const rounds = 50
			var first []string

			for i := 0; i < rounds; i++ {
				ks := &KeyspaceMetadata{Types: map[string]*TypeMetadata{}}
				for _, tm := range tc.types {
					ks.Types[tm.Name] = tm
				}

				sorted := ks.typesSortedTopologically()
				if len(sorted) != len(tc.types) {
					t.Fatalf("round %d: got %d types, want %d", i, len(sorted), len(tc.types))
				}

				pos := make(map[string]int, len(sorted))
				order := make([]string, 0, len(sorted))
				for j, tm := range sorted {
					pos[tm.Name] = j
					order = append(order, tm.Name)
				}

				// No type may be declared before one it embeds. Dependencies
				// are read the same way the sorter reads them -- whole CQL
				// identifiers -- so that a name like "var" inside "varchar" is
				// not mistaken for one. The exact orders asserted by the
				// collision cases below check that matching independently.
				for _, tm := range sorted {
					for _, ft := range tm.FieldTypes {
						for _, ref := range cqlTypeIdentifiers(ft) {
							if !ref.namesUserType() {
								continue
							}
							at, isType := pos[ref.name]
							if isType && ref.name != tm.Name && at > pos[tm.Name] {
								t.Fatalf("round %d: %s embeds %s but is declared first: %v",
									i, tm.Name, ref.name, order)
							}
						}
					}
				}

				if tc.want != nil && !slices.Equal(order, tc.want) {
					t.Fatalf("round %d: got order %v, want %v", i, order, tc.want)
				}

				// The output must not depend on map iteration order either, or
				// a regenerated schema dump would churn between runs.
				if i == 0 {
					first = order
				} else if !slices.Equal(order, first) {
					t.Fatalf("round %d produced %v, round 0 produced %v -- order is not deterministic",
						i, order, first)
				}
			}
		})
	}
}

// TestKeyspaceToCQLRejectsUnrenderableOption covers the one path where a value
// of arbitrary type reaches escape: StrategyOptions is map[string]any, so the
// template is where an unhandled type has to surface. It used to render as
// nothing, leaving `'key': ` in the middle of a dump.
func TestKeyspaceToCQLRejectsUnrenderableOption(t *testing.T) {
	t.Parallel()

	ks := &KeyspaceMetadata{
		Name:            "ks",
		StrategyClass:   "SimpleStrategy",
		StrategyOptions: map[string]any{"replication_factor": []string{"nope"}},
	}

	var sb strings.Builder
	err := ks.keyspaceToCQL(&sb)
	if err == nil {
		t.Fatalf("keyspaceToCQL = %q, nil; want an error", sb.String())
	}
	if !strings.Contains(err.Error(), "cannot render") {
		t.Errorf("keyspaceToCQL error = %q, want it to say what could not be rendered", err)
	}
}

// TestIdent pins the quoting rule. Quoting only what has to be quoted is the
// point: an ordinary schema has to keep producing the output it always has, or
// every dump in existence changes.
func TestIdent(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct{ in, want string }{
		// Bare: lower case, digits and underscores, not starting with a digit.
		{"t", "t"},
		{"my_table", "my_table"},
		{"_leading", "_leading"},
		{"t2", "t2"},
		// Punctuation cannot appear in a bare identifier.
		{"z-type", `"z-type"`},
		{"my ks", `"my ks"`},
		{"a.b", `"a.b"`},
		// A bare identifier is folded to lower case, so anything carrying
		// upper case has to be quoted to name the same thing back.
		{"MyTable", `"MyTable"`},
		{"T", `"T"`},
		// A digit cannot lead.
		{"2fast", `"2fast"`},
		// Reserved words are syntax wherever they appear.
		{"select", `"select"`},
		{"order", `"order"`},
		{"token", `"token"`},
		{"set", `"set"`},
		// Not reserved, despite reading like CQL.
		{"comment", "comment"},
		{"type", "type"},
		{"key", "key"},
		// A quote inside the name doubles, as in CQL.
		{`a"b`, `"a""b"`},
		// Empty is not a legal bare identifier.
		{"", `""`},
	} {
		if got := cqlHelpers.ident(tc.in); got != tc.want {
			t.Errorf("ident(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

// TestToCQLQuotesIdentifiers walks every statement ToCQL emits with a name
// that has to be quoted in it. Each of these used to render bare, and the
// server answers a SyntaxException to the result.
func TestToCQLQuotesIdentifiers(t *testing.T) {
	t.Parallel()

	ks := &KeyspaceMetadata{
		Name:            "my-ks",
		StrategyClass:   "SimpleStrategy",
		StrategyOptions: map[string]any{"replication_factor": "1"},
		Types: map[string]*TypeMetadata{
			"z-type": {Keyspace: "my-ks", Name: "z-type", FieldNames: []string{"order"}, FieldTypes: []string{"int"}},
		},
		Tables: map[string]*TableMetadata{
			"my-tbl": {
				Keyspace:          "my-ks",
				Name:              "my-tbl",
				PartitionKey:      []*ColumnMetadata{{Name: "pk", Type: "int"}},
				ClusteringColumns: []*ColumnMetadata{{Name: "Order", Type: "int", ClusteringOrder: "DESC"}},
				OrderedColumns:    []string{"pk", "Order", "select"},
				Columns: map[string]*ColumnMetadata{
					"pk":     {Name: "pk", Type: "int", Kind: ColumnPartitionKey},
					"Order":  {Name: "Order", Type: "int", Kind: ColumnClusteringKey},
					"select": {Name: "select", Type: "text", Kind: ColumnRegular},
				},
			},
		},
		Indexes: map[string]*IndexMetadata{
			"my-idx": {
				Name: "my-idx", KeyspaceName: "my-ks", TableName: "my-tbl",
				Options: map[string]string{"target": `{"pk":["pk"],"ck":["Order"]}`},
			},
		},
		Views: map[string]*ViewMetadata{
			"my-view": {
				KeyspaceName: "my-ks", ViewName: "my-view", BaseTableName: "my-tbl",
				WhereClause:    `"Order" IS NOT NULL`,
				OrderedColumns: []string{"pk", "Order"},
				PartitionKey:   []*ColumnMetadata{{Name: "pk"}},
			},
		},
		Functions: map[string]*FunctionMetadata{
			"my-fn": {
				Keyspace: "my-ks", Name: "my-fn",
				ArgumentNames: []string{"select"}, ArgumentTypes: []string{"int"},
				ReturnType: "int", Language: "lua", Body: "return 1",
			},
		},
		Aggregates: map[string]*AggregateMetadata{
			"my-agg": {
				Keyspace: "my-ks", Name: "my-agg", ArgumentTypes: []string{"int"},
				StateFunc: FunctionMetadata{Name: "my-fn"}, StateType: "int",
				FinalFunc: FunctionMetadata{Name: "my-final"},
			},
		},
	}

	got, err := ks.ToCQL()
	if err != nil {
		t.Fatalf("ToCQL: %v", err)
	}

	for _, want := range []string{
		`CREATE KEYSPACE "my-ks" WITH`,
		`CREATE TYPE "my-ks"."z-type" (`,
		`"order" int`,
		`CREATE TABLE "my-ks"."my-tbl" (`,
		`"Order" int`,
		`"select" text`,
		`PRIMARY KEY (pk, "Order")`,
		`CLUSTERING ORDER BY ("Order" DESC)`,
		`CREATE INDEX "my-idx" ON "my-ks"."my-tbl" ((pk), "Order")`,
		`CREATE MATERIALIZED VIEW "my-ks"."my-view" AS`,
		`FROM "my-ks"."my-tbl"`,
		`CREATE FUNCTION "my-ks"."my-fn" (`,
		`CREATE AGGREGATE "my-ks"."my-agg"(`,
		`SFUNC "my-fn"`,
		`FINALFUNC "my-final"`,
	} {
		if !strings.Contains(got, want) {
			t.Errorf("missing %q\n--- got ---\n%s", want, got)
		}
	}

	// A name that needs no quotes must not gain any, or every existing dump
	// changes.
	for _, unwanted := range []string{`"pk"`, `"int"`, `"lua"`} {
		if strings.Contains(got, unwanted) {
			t.Errorf("quoted %s, which needs no quotes\n--- got ---\n%s", unwanted, got)
		}
	}
}

func TestTableColumnToCQL(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name string
		tm   *TableMetadata
		want string
	}{
		{
			name: "single partition key is declared inline",
			tm: &TableMetadata{
				OrderedColumns: []string{"id", "v"},
				Columns: map[string]*ColumnMetadata{
					"id": col("id", "int", ColumnPartitionKey),
					"v":  col("v", "text", ColumnRegular),
				},
				PartitionKey: []*ColumnMetadata{col("id", "int", ColumnPartitionKey)},
			},
			want: "id int PRIMARY KEY,\n    v text",
		},
		{
			name: "static column carries the static keyword",
			tm: &TableMetadata{
				OrderedColumns: []string{"id", "c", "s"},
				Columns: map[string]*ColumnMetadata{
					"id": col("id", "int", ColumnPartitionKey),
					"c":  col("c", "int", ColumnClusteringKey),
					"s":  col("s", "text", ColumnStatic),
				},
				PartitionKey:      []*ColumnMetadata{col("id", "int", ColumnPartitionKey)},
				ClusteringColumns: []*ColumnMetadata{col("c", "int", ColumnClusteringKey)},
			},
			want: "s text static",
		},
		{
			name: "composite partition key moves to a PRIMARY KEY clause",
			tm: &TableMetadata{
				OrderedColumns: []string{"a", "b", "v"},
				Columns: map[string]*ColumnMetadata{
					"a": col("a", "int", ColumnPartitionKey),
					"b": col("b", "int", ColumnPartitionKey),
					"v": col("v", "text", ColumnRegular),
				},
				PartitionKey: []*ColumnMetadata{
					col("a", "int", ColumnPartitionKey),
					col("b", "int", ColumnPartitionKey),
				},
			},
			want: "PRIMARY KEY ((a, b))",
		},
		{
			name: "clustering columns follow the partition key",
			tm: &TableMetadata{
				OrderedColumns: []string{"a", "c", "d", "v"},
				Columns: map[string]*ColumnMetadata{
					"a": col("a", "int", ColumnPartitionKey),
					"c": col("c", "int", ColumnClusteringKey),
					"d": col("d", "int", ColumnClusteringKey),
					"v": col("v", "text", ColumnRegular),
				},
				PartitionKey: []*ColumnMetadata{col("a", "int", ColumnPartitionKey)},
				// Two clustering columns, so the separator arm of
				// partitionKeyString runs as well as the first-column arm.
				ClusteringColumns: []*ColumnMetadata{
					col("c", "int", ColumnClusteringKey),
					col("d", "int", ColumnClusteringKey),
				},
			},
			want: "PRIMARY KEY (a, c, d)",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got := cqlHelpers.tableColumnToCQL(tc.tm)
			if !strings.Contains(got, tc.want) {
				t.Errorf("column clause is missing %q\n--- got ---\n%s", tc.want, got)
			}
		})
	}
}

func TestTablePropertiesToCQL(t *testing.T) {
	t.Parallel()

	opts := TableMetadataOptions{
		Comment:        "hi",
		GcGraceSeconds: 864000,
		Caching:        map[string]string{"keys": "ALL"},
		Compaction:     map[string]string{"class": "SizeTieredCompactionStrategy"},
		Compression:    map[string]string{"sstable_compression": "LZ4Compressor"},
	}

	t.Run("clustering order leads the property list", func(t *testing.T) {
		t.Parallel()

		cks := []*ColumnMetadata{{Name: "c", ClusteringOrder: "DESC"}}
		got, err := cqlHelpers.tablePropertiesToCQL(cks, opts, nil)
		if err != nil {
			t.Fatalf("tablePropertiesToCQL: %v", err)
		}
		if !strings.HasPrefix(got, "CLUSTERING ORDER BY (c DESC)") {
			t.Errorf("clustering order is not first\n--- got ---\n%s", got)
		}
		for _, want := range []string{"comment = 'hi'", "gc_grace_seconds = 864000", "caching = "} {
			if !strings.Contains(got, want) {
				t.Errorf("properties missing %q\n--- got ---\n%s", want, got)
			}
		}
	})

	t.Run("no clustering columns means no order clause", func(t *testing.T) {
		t.Parallel()

		got, err := cqlHelpers.tablePropertiesToCQL(nil, opts, nil)
		if err != nil {
			t.Fatalf("tablePropertiesToCQL: %v", err)
		}
		if strings.Contains(got, "CLUSTERING ORDER") {
			t.Errorf("emitted a clustering order for a table with none\n--- got ---\n%s", got)
		}
	})
}

func TestToCQLHelpers(t *testing.T) {
	t.Parallel()

	t.Run("escape", func(t *testing.T) {
		t.Parallel()

		for _, tc := range []struct {
			in   any
			want string
			// wantErr marks a type escape cannot render. It used to answer
			// "" for these, which the caller writes as `key = ` and the
			// server rejects a whole dump away from the cause.
			wantErr bool
		}{
			{in: "plain", want: "'plain'"},
			{in: "it's", want: "'it''s'"}, // the injection-relevant case
			{in: 42, want: "42"},
			{in: 1.5, want: "1.5"},
			{in: true, want: "true"},
			{in: false, want: "false"},
			{in: []byte("raw"), want: "raw"},
			// Widths other than int and float64 reach escape through
			// StrategyOptions, which the metadata carries as any.
			{in: int64(42), want: "42"},
			{in: int32(42), want: "42"},
			{in: uint(42), want: "42"},
			{in: uint64(42), want: "42"},
			{in: float32(1.5), want: "1.5"},
			{in: struct{}{}, wantErr: true},
			{in: nil, wantErr: true},
			{in: []string{"a"}, wantErr: true},
		} {
			got, err := cqlHelpers.escape(tc.in)
			if tc.wantErr {
				if err == nil {
					t.Errorf("escape(%#v) = %q, nil; want an error", tc.in, got)
				}
				continue
			}
			if err != nil {
				t.Errorf("escape(%#v): %v", tc.in, err)
				continue
			}
			if got != tc.want {
				t.Errorf("escape(%#v) = %q, want %q", tc.in, got, tc.want)
			}
		}
	})

	t.Run("escapeString", func(t *testing.T) {
		t.Parallel()

		for _, tc := range []struct{ in, want string }{
			{"plain", "'plain'"},
			{"it's", "'it''s'"},
			{`a "b" c`, `'a "b" c'`},
			{"", "''"},
		} {
			if got := cqlHelpers.escapeString(tc.in); got != tc.want {
				t.Errorf("escapeString(%q) = %q, want %q", tc.in, got, tc.want)
			}
		}
	})

	t.Run("fixStrategy", func(t *testing.T) {
		t.Parallel()

		if got := cqlHelpers.fixStrategy("org.apache.cassandra.locator.SimpleStrategy"); got != "SimpleStrategy" {
			t.Errorf("fixStrategy = %q", got)
		}
	})

	t.Run("stripFrozen", func(t *testing.T) {
		t.Parallel()

		for _, tc := range []struct{ in, want string }{
			{"frozen<addr>", "addr"},
			{"frozen<tuple<int, double>>", "tuple<int, double>"},
			{"frozen<map<text, int>>", "map<text, int>"},
			// Not frozen-wrapped: the closing bracket used to come off
			// anyway, which corrupts every parameterised type.
			{"map<text, int>", "map<text, int>"},
			{"tuple<int, double>", "tuple<int, double>"},
			{"list<frozen<a>>", "list<frozen<a>>"},
			{"text", "text"},
			{"int", "int"},
			// Degenerate: a prefix with no matching bracket is left as it is
			// rather than half-unwrapped.
			{"frozen<unterminated", "frozen<unterminated"},
			{"", ""},
		} {
			if got := cqlHelpers.stripFrozen(tc.in); got != tc.want {
				t.Errorf("stripFrozen(%q) = %q, want %q", tc.in, got, tc.want)
			}
		}
	})

	t.Run("zip", func(t *testing.T) {
		t.Parallel()

		got := cqlHelpers.zip([]string{"a", "b"}, []string{"int", "text"})
		if len(got) != 2 || got[0][0] != "a" || got[0][1] != "int" || got[1][1] != "text" {
			t.Errorf("zip = %v", got)
		}
	})
}

// TestToCQLGeneratesWholeKeyspace drives ToCQL's orchestration: every section
// it emits -- keyspace, types, tables, indexes, functions, aggregates and views
// -- plus the ordering constraints that make the output replayable.
//
// The per-section rendering is asserted separately above; what this adds is
// that ToCQL actually reaches each loop, and in an order a server can replay.
func TestToCQLGeneratesWholeKeyspace(t *testing.T) {
	t.Parallel()

	ks := &KeyspaceMetadata{
		Name:            "ks",
		DurableWrites:   true,
		StrategyClass:   "SimpleStrategy",
		StrategyOptions: map[string]any{"replication_factor": "1"},
		Types: map[string]*TypeMetadata{
			"addr": {Keyspace: "ks", Name: "addr", FieldNames: []string{"street", "zip"}, FieldTypes: []string{"text", "int"}},
		},
		Tables: map[string]*TableMetadata{
			"users": {
				Name:           "users",
				OrderedColumns: []string{"id", "home"},
				Columns: map[string]*ColumnMetadata{
					"id":   col("id", "uuid", ColumnPartitionKey),
					"home": col("home", "frozen<addr>", ColumnRegular),
				},
				PartitionKey: []*ColumnMetadata{col("id", "uuid", ColumnPartitionKey)},
				Options:      TableMetadataOptions{GcGraceSeconds: 864000},
			},
		},
		Indexes: map[string]*IndexMetadata{
			"users_by_home": {
				Name: "users_by_home", KeyspaceName: "ks", TableName: "users",
				Options: map[string]string{"target": "home"},
			},
		},
		Functions: map[string]*FunctionMetadata{
			"fsum": {
				Keyspace: "ks", Name: "fsum",
				ArgumentNames: []string{"a"}, ArgumentTypes: []string{"int"},
				ReturnType: "int", Language: "lua", Body: "return a", CalledOnNullInput: true,
			},
		},
		Aggregates: map[string]*AggregateMetadata{
			"total": {
				Keyspace: "ks", Name: "total",
				ArgumentTypes: []string{"int"}, ReturnType: "int", StateType: "int",
				InitCond:  "0",
				StateFunc: FunctionMetadata{Name: "fsum"},
			},
		},
		Views: map[string]*ViewMetadata{
			"users_by_zip": {
				KeyspaceName: "ks", ViewName: "users_by_zip", BaseTableName: "users",
				WhereClause:    "home IS NOT NULL",
				OrderedColumns: []string{"id", "home"},
				PartitionKey:   []*ColumnMetadata{col("id", "uuid", ColumnPartitionKey)},
			},
		},
	}

	got, err := ks.ToCQL()
	if err != nil {
		t.Fatalf("ToCQL: %v", err)
	}

	for _, want := range []string{
		"CREATE KEYSPACE ks WITH replication = {",
		"CREATE TYPE ks.addr",
		"street text",
		"CREATE TABLE ks.users",
		"id uuid PRIMARY KEY",
		"home frozen<addr>",
		"gc_grace_seconds = 864000",
		"CREATE INDEX users_by_home ON ks.users (home);",
		"CREATE FUNCTION ks.fsum",
		"CREATE AGGREGATE ks.total(",
		"CREATE MATERIALIZED VIEW ks.users_by_zip AS",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("generated schema is missing %q\n--- got ---\n%s", want, got)
		}
	}

	// Ordering constraints a server enforces on replay.
	for _, ord := range []struct{ first, then, why string }{
		{"CREATE TYPE ks.addr", "CREATE TABLE ks.users", "a type must precede the table using it"},
		{"CREATE TABLE ks.users", "CREATE INDEX users_by_home", "an index needs its base table"},
		{"CREATE TABLE ks.users", "CREATE MATERIALIZED VIEW ks.users_by_zip", "a view needs its base table"},
		{"CREATE FUNCTION ks.fsum", "CREATE AGGREGATE ks.total(", "an aggregate needs its state function"},
	} {
		// Index returns -1 for a statement that was never emitted, and -1 is
		// less than any real offset, so comparing the two alone would let a
		// missing first silently satisfy the constraint.
		i, j := strings.Index(got, ord.first), strings.Index(got, ord.then)
		if i < 0 {
			t.Errorf("%q was never emitted, so %s cannot be checked\n--- got ---\n%s", ord.first, ord.why, got)
			continue
		}
		if j < 0 {
			t.Errorf("%q was never emitted, so %s cannot be checked\n--- got ---\n%s", ord.then, ord.why, got)
			continue
		}
		if i > j {
			t.Errorf("%q comes after %q: %s\n--- got ---\n%s", ord.first, ord.then, ord.why, got)
		}
	}

	// ToCQL caches its own output in CreateStmts.
	if ks.CreateStmts != got {
		t.Error("ToCQL did not cache the generated schema in CreateStmts")
	}
}

func TestTableExtensionsToCQL(t *testing.T) {
	t.Parallel()

	blob, err := os.ReadFile("testdata/recreate/scylla_encryption_options.bin")
	if err != nil {
		t.Fatal(err)
	}

	t.Run("encryption options are decoded and rendered as CQL", func(t *testing.T) {
		t.Parallel()

		got, err := cqlHelpers.tableExtensionsToCQL(map[string]any{"scylla_encryption_options": blob})
		if err != nil {
			t.Fatalf("tableExtensionsToCQL: %v", err)
		}
		if len(got) != 1 {
			t.Fatalf("got %d properties, want 1: %q", len(got), got)
		}
		if !strings.HasPrefix(got[0], "scylla_encryption_options = ") {
			t.Errorf("unexpected property: %q", got[0])
		}
		// CQL quotes strings with ', so a surviving double quote is JSON that
		// was never converted and will not parse.
		if strings.Contains(got[0], `"`) {
			t.Errorf("double quotes survived into CQL: %s", got[0])
		}
	})

	t.Run("a quote in an option value cannot escape its string", func(t *testing.T) {
		t.Parallel()

		for _, tc := range []struct {
			name   string
			cipher string
			want   string
		}{
			// A single quote doubles. It used to pass straight through
			// json.Marshal and close the string it was inside, which is the
			// same injection as the table comment one function over.
			{"a single quote", "it's", `'cipher_algorithm': 'it''s'`},
			// A double quote is data. json.Marshal wrote it as \" and the
			// quote rewrite turned that into \', a backslash escape CQL does
			// not have.
			{"a double quote", `a"b`, `'cipher_algorithm': 'a"b'`},
			{"both", `a"b it's`, `'cipher_algorithm': 'a"b it''s'`},
		} {
			t.Run(tc.name, func(t *testing.T) {
				t.Parallel()

				got := cqlHelpers.encryptionOptionsToCQL(&scyllaEncryptionOptions{CipherAlgorithm: tc.cipher})
				if !strings.Contains(got, tc.want) {
					t.Errorf("encryptionOptionsToCQL = %s, want it to contain %s", got, tc.want)
				}
				if strings.Contains(got, `\'`) {
					t.Errorf("emitted a backslash escape, which CQL does not have: %s", got)
				}
			})
		}
	})

	t.Run("secret_key_strength stays a bare number", func(t *testing.T) {
		t.Parallel()

		got := cqlHelpers.encryptionOptionsToCQL(&scyllaEncryptionOptions{SecretKeyStrength: 128})
		if !strings.Contains(got, `'secret_key_strength': 128`) {
			t.Errorf("encryptionOptionsToCQL = %s, want an unquoted strength", got)
		}
	})

	t.Run("unknown extensions are dropped", func(t *testing.T) {
		t.Parallel()

		got, err := cqlHelpers.tableExtensionsToCQL(map[string]any{"something_else": []byte("x")})
		if err != nil {
			t.Fatalf("tableExtensionsToCQL: %v", err)
		}
		if len(got) != 0 {
			t.Errorf("unknown extension rendered as %q", got)
		}
	})

	t.Run("no extensions yields no properties", func(t *testing.T) {
		t.Parallel()

		got, err := cqlHelpers.tableExtensionsToCQL(nil)
		if err != nil {
			t.Fatalf("tableExtensionsToCQL: %v", err)
		}
		if len(got) != 0 {
			t.Errorf("got %q, want none", got)
		}
	})
}

func TestTableOptionsToCQLScyllaExtras(t *testing.T) {
	t.Parallel()

	// Options render as "name = value", so a prefix match is what identifies
	// one. Local to this test: nothing else in the package needs it, and the
	// test binary spans enough files that a name like this does not belong in
	// package scope.
	hasOption := func(in []string, prefix string) bool {
		return slices.ContainsFunc(in, func(s string) bool {
			return strings.HasPrefix(s, prefix)
		})
	}

	t.Run("cdc is emitted when set", func(t *testing.T) {
		t.Parallel()

		got := cqlHelpers.tableOptionsToCQL(TableMetadataOptions{CDC: map[string]string{"enabled": "true"}})
		if !hasOption(got, "cdc = ") {
			t.Errorf("cdc missing from %q", got)
		}
	})

	t.Run("cdc is omitted when unset", func(t *testing.T) {
		t.Parallel()

		got := cqlHelpers.tableOptionsToCQL(TableMetadataOptions{})
		if hasOption(got, "cdc = ") {
			t.Errorf("cdc emitted for a table that has none: %q", got)
		}
	})

	t.Run("in_memory is emitted only when true", func(t *testing.T) {
		t.Parallel()

		on := cqlHelpers.tableOptionsToCQL(TableMetadataOptions{InMemory: true})
		if !hasOption(on, "in_memory = ") {
			t.Errorf("in_memory missing from %q", on)
		}

		off := cqlHelpers.tableOptionsToCQL(TableMetadataOptions{})
		if hasOption(off, "in_memory = ") {
			t.Errorf("in_memory emitted for a non in-memory table: %q", off)
		}
	})
}

// TestTableOptionsToCQLEscaping pins the option values against a schema that
// carries quotes. Both quote characters matter: a single quote has to double
// so it stays inside the CQL string, and a double quote has to survive as
// data. Rendering used to rewrite every double quote to a single one after
// escaping, to re-quote marshalled JSON, which let a comment close its own
// string and open a second property.
func TestTableOptionsToCQLEscaping(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name string
		ops  TableMetadataOptions
		want string
	}{
		{
			// The whole comment has to stay one option. Rendering used to
			// emit comment = 'x' AND gc_grace_seconds = 0 AND comment = 'y',
			// which replays as three properties, two of them forged.
			name: "a double quote in a comment stays data",
			ops:  TableMetadataOptions{Comment: `x" AND gc_grace_seconds = 0 AND comment = "y`},
			want: `comment = 'x" AND gc_grace_seconds = 0 AND comment = "y'`,
		},
		{
			name: "a single quote in a comment doubles",
			ops:  TableMetadataOptions{Comment: "it's"},
			want: `comment = 'it''s'`,
		},
		{
			name: "a map value is a CQL string, not JSON",
			ops:  TableMetadataOptions{Caching: map[string]string{"keys": "ALL"}},
			want: `caching = {'keys': 'ALL'}`,
		},
		{
			name: "a quote inside a map value doubles",
			ops:  TableMetadataOptions{Compaction: map[string]string{"class": "it's"}},
			want: `compaction = {'class': 'it''s'}`,
		},
		{
			name: "a quote inside a map key doubles",
			ops:  TableMetadataOptions{Compression: map[string]string{"it's": "x"}},
			want: `compression = {'it''s': 'x'}`,
		},
		{
			name: "map entries are ordered by key",
			ops:  TableMetadataOptions{Caching: map[string]string{"rows_per_partition": "NONE", "keys": "ALL"}},
			want: `caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got := cqlHelpers.tableOptionsToCQL(tc.ops)
			if !slices.Contains(got, tc.want) {
				t.Errorf("missing option %q\n--- got ---\n%s", tc.want, strings.Join(got, "\n"))
			}
		})
	}
}

func TestIndexToCQL(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name    string
		im      *IndexMetadata
		want    string
		wantNot string
	}{
		{
			name: "plain target is emitted as-is",
			im: &IndexMetadata{
				Name: "users_by_email", KeyspaceName: "ks", TableName: "users",
				Options: map[string]string{"target": "email"},
			},
			want: "CREATE INDEX users_by_email ON ks.users (email);",
		},
		{
			name: "a JSON target is expanded into partition and clustering keys",
			im: &IndexMetadata{
				Name: "idx", KeyspaceName: "ks", TableName: "t",
				Options: map[string]string{"target": `{"pk":["a","b"],"ck":["c"]}`},
			},
			want: "CREATE INDEX idx ON ks.t ((a,b), c);",
		},
		{
			name: "custom indexes are skipped -- Scylla does not support them",
			im: &IndexMetadata{
				Name: "custom", KeyspaceName: "ks", TableName: "t", Kind: IndexKindCustom,
				Options: map[string]string{"target": "x"},
			},
			wantNot: "CREATE INDEX",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			var sb strings.Builder
			ks := &KeyspaceMetadata{Name: "ks"}
			if err := ks.indexToCQL(&sb, tc.im); err != nil {
				t.Fatalf("indexToCQL: %v", err)
			}
			got := sb.String()
			if tc.want != "" && !strings.Contains(got, tc.want) {
				t.Errorf("missing %q\n--- got ---\n%s", tc.want, got)
			}
			if tc.wantNot != "" && strings.Contains(got, tc.wantNot) {
				t.Errorf("emitted %q when it should have been skipped\n--- got ---\n%s", tc.wantNot, got)
			}
		})
	}
}

func TestViewToCQL(t *testing.T) {
	t.Parallel()

	base := &KeyspaceMetadata{Name: "ks"}

	t.Run("named columns are listed", func(t *testing.T) {
		t.Parallel()

		vm := &ViewMetadata{
			KeyspaceName: "ks", ViewName: "v", BaseTableName: "t",
			WhereClause:    "id IS NOT NULL",
			OrderedColumns: []string{"id", "name"},
			PartitionKey:   []*ColumnMetadata{col("id", "int", ColumnPartitionKey)},
		}
		var sb strings.Builder
		if err := base.viewToCQL(&sb, vm); err != nil {
			t.Fatalf("viewToCQL: %v", err)
		}
		got := sb.String()
		for _, want := range []string{
			"CREATE MATERIALIZED VIEW ks.v AS",
			"FROM ks.t",
			"WHERE id IS NOT NULL",
			"name",
		} {
			if !strings.Contains(got, want) {
				t.Errorf("missing %q\n--- got ---\n%s", want, got)
			}
		}
	})

	t.Run("IncludeAllColumns selects star", func(t *testing.T) {
		t.Parallel()

		vm := &ViewMetadata{
			KeyspaceName: "ks", ViewName: "v", BaseTableName: "t",
			WhereClause: "id IS NOT NULL", IncludeAllColumns: true,
			OrderedColumns: []string{"id"},
			PartitionKey:   []*ColumnMetadata{col("id", "int", ColumnPartitionKey)},
		}
		var sb strings.Builder
		if err := base.viewToCQL(&sb, vm); err != nil {
			t.Fatalf("viewToCQL: %v", err)
		}
		if got := sb.String(); !strings.Contains(got, "SELECT *") {
			t.Errorf("expected SELECT *\n--- got ---\n%s", got)
		}
	})
}

func TestFunctionAndAggregateToCQL(t *testing.T) {
	t.Parallel()

	ks := &KeyspaceMetadata{Name: "ks"}

	fn := &FunctionMetadata{
		Keyspace: "ks", Name: "avgfinal",
		ArgumentNames: []string{"state"}, ArgumentTypes: []string{"frozen<tuple<int, double>>"},
		ReturnType: "double", Language: "lua", Body: "return 1", CalledOnNullInput: true,
	}

	t.Run("function", func(t *testing.T) {
		t.Parallel()

		var sb strings.Builder
		if err := ks.functionToCQL(&sb, "ks", fn); err != nil {
			t.Fatalf("functionToCQL: %v", err)
		}
		got := sb.String()
		for _, want := range []string{
			"CREATE FUNCTION ks.avgfinal",
			"state",
			// stripFrozen unwraps the argument type.
			"tuple<int, double>",
			"CALLED ON NULL INPUT",
			"RETURNS double",
			"LANGUAGE lua",
			"$$return 1$$",
		} {
			if !strings.Contains(got, want) {
				t.Errorf("missing %q\n--- got ---\n%s", want, got)
			}
		}
	})

	t.Run("function that returns null on null input", func(t *testing.T) {
		t.Parallel()

		strict := *fn
		strict.CalledOnNullInput = false
		var sb strings.Builder
		if err := ks.functionToCQL(&sb, "ks", &strict); err != nil {
			t.Fatalf("functionToCQL: %v", err)
		}
		if got := sb.String(); !strings.Contains(got, "RETURNS NULL ON NULL INPUT") {
			t.Errorf("expected RETURNS NULL ON NULL INPUT\n--- got ---\n%s", got)
		}
	})

	t.Run("aggregate", func(t *testing.T) {
		t.Parallel()

		am := &AggregateMetadata{
			Keyspace: "ks", Name: "average",
			ArgumentTypes: []string{"double"},
			ReturnType:    "double",
			StateType:     "frozen<tuple<int, double>>",
			InitCond:      "(0, 0)",
			StateFunc:     FunctionMetadata{Name: "avgstate"},
			FinalFunc:     FunctionMetadata{Name: "avgfinal"},
		}
		var sb strings.Builder
		if err := ks.aggregateToCQL(&sb, am); err != nil {
			t.Fatalf("aggregateToCQL: %v", err)
		}
		got := sb.String()
		for _, want := range []string{
			"CREATE AGGREGATE ks.average(",
			"SFUNC avgstate",
			// The template applies stripFrozen to StateType, so this renders
			// "tuple<int, double>" where
			// testdata/recreate/aggregates_golden.cql has
			// "frozen<tuple<int, double>>". Both are valid CQL: a tuple is
			// intrinsically frozen, so the wrapper is redundant rather than
			// required. Checked against ScyllaDB 2026.3.0 -- it accepts either
			// spelling and echoes back the frozen one, which is why the golden,
			// captured from DESCRIBE, has it. A cosmetic divergence, not a
			// defect.
			"STYPE tuple<int, double>",
			"FINALFUNC avgfinal",
			"INITCOND (0, 0)",
		} {
			if !strings.Contains(got, want) {
				t.Errorf("missing %q\n--- got ---\n%s", want, got)
			}
		}
	})
}

// TestScyllaEncryptionOptionsUnmarshalBinary exercises the blob decoder that
// tableExtensionsToCQL depends on. It was TestScyllaEncryptionOptionsUnmarshaller
// in recreate_test.go, which is integration-tagged, so the decoder had no
// coverage in the lane that runs on every PR -- even though it only reads two
// files and unmarshals, and needs no cluster. Moved here rather than copied, so
// there is no counterpart to keep in sync.
func TestScyllaEncryptionOptionsUnmarshalBinary(t *testing.T) {
	t.Parallel()

	blob, err := os.ReadFile("testdata/recreate/scylla_encryption_options.bin")
	if err != nil {
		t.Fatal(err)
	}
	goldenBuf, err := os.ReadFile("testdata/recreate/scylla_encryption_options_golden.json")
	if err != nil {
		t.Fatal(err)
	}

	want := &scyllaEncryptionOptions{}
	if err := json.Unmarshal(goldenBuf, want); err != nil {
		t.Fatal(err)
	}

	got := &scyllaEncryptionOptions{}
	if err := got.UnmarshalBinary(blob); err != nil {
		t.Fatalf("UnmarshalBinary: %v", err)
	}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("decoded blob differs from the golden (-want +got):\n%s", diff)
	}
}

// TestTableExtensionsToCQLRejectsNonBlob covers the one value in the exported
// Extensions map that ToCQL decodes. It used to be type-asserted, so a caller
// who put anything but a []byte there panicked the render.
func TestTableExtensionsToCQLRejectsNonBlob(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name string
		ext  any
	}{
		{"a string", "oops"},
		{"an int", 7},
		{"nil", nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got, err := cqlHelpers.tableExtensionsToCQL(map[string]any{"scylla_encryption_options": tc.ext})
			if err == nil {
				t.Fatalf("tableExtensionsToCQL(%v) = %q, nil; want an error", tc.ext, got)
			}
			if !strings.Contains(err.Error(), "want []byte") {
				t.Errorf("tableExtensionsToCQL(%v) error = %q, want it to name the expected type", tc.ext, err)
			}
		})
	}
}

// TestScyllaEncryptionOptionsUnmarshalBinaryRejectsShortBlobs pins the bounds
// checks. Each of these used to panic with a slice-bounds error rather than
// return, on a blob the server supplied.
func TestScyllaEncryptionOptionsUnmarshalBinaryRejectsShortBlobs(t *testing.T) {
	t.Parallel()

	// le32 is the little-endian length prefix the format uses.
	le32 := func(v uint32) []byte {
		b := make([]byte, 4)
		binary.LittleEndian.PutUint32(b, v)
		return b
	}
	concat := func(parts ...[]byte) []byte {
		var out []byte
		for _, p := range parts {
			out = append(out, p...)
		}
		return out
	}

	for _, tc := range []struct {
		name string
		blob []byte
		// wantErr, when set, must appear in the error. Only the entry-count
		// guard can produce it, and that guard cannot be pinned by the error
		// merely being non-nil: without it the loop would still fail on the
		// second read, after make() had already been asked for a map of
		// 1<<30 entries.
		wantErr string
	}{
		{name: "no blob at all", blob: nil},
		{name: "shorter than the entry count", blob: []byte{1, 2}},
		{name: "an entry count and nothing else", blob: le32(1)},
		{name: "a key length past the end", blob: concat(le32(1), le32(100))},
		{name: "a key that stops short", blob: concat(le32(1), le32(10), []byte("abc"))},
		{name: "a key but no value length", blob: concat(le32(1), le32(3), []byte("abc"))},
		{name: "a value length past the end", blob: concat(le32(1), le32(3), []byte("abc"), le32(100))},
		{name: "a value that stops short", blob: concat(le32(1), le32(3), []byte("abc"), le32(10), []byte("xy"))},
		// An entry needs at least 8 bytes, so this count cannot be honest.
		// It has to be rejected before it reaches make(): a blob claiming
		// 1<<30 entries would otherwise ask for roughly a gigabyte.
		{
			name:    "an entry count larger than the blob",
			blob:    concat(le32(1<<30), le32(3), []byte("abc")),
			wantErr: "claims 1073741824 entries",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			enc := &scyllaEncryptionOptions{}
			err := enc.UnmarshalBinary(tc.blob)
			if err == nil {
				t.Fatalf("UnmarshalBinary(%v) = nil, want an error", tc.blob)
			}
			if tc.wantErr != "" && !strings.Contains(err.Error(), tc.wantErr) {
				t.Errorf("UnmarshalBinary(%v) = %q, want it to mention %q", tc.blob, err, tc.wantErr)
			}
		})
	}
}

// TestScyllaEncryptionOptionsUnmarshalBinaryAcceptsWellFormed guards against
// the bounds checks rejecting a blob they should read, which the golden-file
// test above would not catch for values it does not carry.
func TestScyllaEncryptionOptionsUnmarshalBinaryAcceptsWellFormed(t *testing.T) {
	t.Parallel()

	le32 := func(v uint32) []byte {
		b := make([]byte, 4)
		binary.LittleEndian.PutUint32(b, v)
		return b
	}
	var blob []byte
	blob = append(blob, le32(2)...)
	for _, kv := range [][2]string{{"cipher_algorithm", "AES/ECB/PKCS5Padding"}, {"secret_key_strength", "128"}} {
		blob = append(blob, le32(uint32(len(kv[0])))...)
		blob = append(blob, kv[0]...)
		blob = append(blob, le32(uint32(len(kv[1])))...)
		blob = append(blob, kv[1]...)
	}

	got := &scyllaEncryptionOptions{}
	if err := got.UnmarshalBinary(blob); err != nil {
		t.Fatalf("UnmarshalBinary: %v", err)
	}
	want := &scyllaEncryptionOptions{CipherAlgorithm: "AES/ECB/PKCS5Padding", SecretKeyStrength: 128}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("decoded blob differs (-want +got):\n%s", diff)
	}
}

func TestCQLTypeIdentifiers(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		// name, not in, because two inputs differing only in whitespace
		// rewrite to the same subtest name, and the empty input has none.
		name string
		in   string
		want []string
	}{
		{"a native type", "int", []string{"int"}},
		{"a frozen user type", "frozen<addr>", []string{"frozen", "addr"}},
		{"a user type inside a collection", "map<text, frozen<addr>>", []string{"map", "text", "frozen", "addr"}},
		{"a tuple", "tuple<int, double>", []string{"tuple", "int", "double"}},
		// The dimension is a literal, not a type: no identifier starts with a
		// digit, so it must not be offered as a dependency.
		{"a vector dimension", "vector<float, 3>", []string{"vector", "float"}},
		// Whitespace may separate a constructor from its arguments; it still
		// has to read as parameterised, or the constructor itself would be
		// offered as a dependency.
		{"a space before the arguments", "frozen <addr>", []string{"frozen", "addr"}},
		{"a tab before the arguments", "frozen\t<addr>", []string{"frozen", "addr"}},
		// A single-quoted custom marshal type is a Java class name, not a path
		// of UDT names: descending into it would offer org, apache and so on.
		{"a custom marshal type", `'org.apache.cassandra.db.marshal.UTF8Type'`, nil},
		{"a custom marshal type as the frozen operand", `frozen<'org.apache.cassandra.db.marshal.UTF8Type'>`, []string{"frozen"}},
		{"a custom marshal type inside a collection", `map<text, 'org.apache.cassandra.db.marshal.Int32Type'>`, []string{"map", "text"}},
		{"a doubled quote inside a single-quoted literal", `'it''s'`, nil},
		{"an unterminated single-quoted literal", `'unterminated`, nil},
		{"a frozen vector", "frozen<vector<float, 1024>>", []string{"frozen", "vector", "float"}},
		// "var" must not fall out of "varchar".
		{"a native type containing a shorter name", "varchar", []string{"varchar"}},
		// A quoted name is one identifier, punctuation and all.
		{"a quoted name as the frozen operand", `frozen<"z-type">`, []string{"frozen", "z-type"}},
		{"a quoted name alone", `"z-type"`, []string{"z-type"}},
		{"quoted names containing punctuation and a space", `map<"a-b", frozen<"c d">>`, []string{"map", "a-b", "frozen", "c d"}},
		// A doubled quote inside the quotes is one literal quote.
		{"a doubled quote inside a quoted name", `frozen<"a""b">`, []string{"frozen", `a"b`}},
		// Degenerate input must not hang or panic.
		{"empty input", "", nil},
		{"a lone quote", `"`, nil},
		{"an unterminated quoted name", `frozen<"unterminated`, []string{"frozen", "unterminated"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			var got []string
			for _, ref := range cqlTypeIdentifiers(tc.in) {
				got = append(got, ref.name)
			}
			if !slices.Equal(got, tc.want) {
				t.Errorf("cqlTypeIdentifiers(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}

func TestCQLTypeRefNamesUserType(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name string
		ref  cqlTypeRef
		want bool
	}{
		{"bare built-in is not a user type", cqlTypeRef{name: "text"}, false},
		// A constructor name is only decisive when applied to arguments. Bare,
		// it is available to a user type of that name -- which is what lets
		// frozen<map> mean the user type map rather than a collection.
		{"bare constructor name is available to a user type", cqlTypeRef{name: "frozen"}, true},
		{"bare unknown name is a user type", cqlTypeRef{name: "addr"}, true},
		// frozen never wraps a scalar built-in, so its operand is a user type
		// even when the name collides with one.
		{"frozen operand is a user type", cqlTypeRef{name: "text", frozenOperand: true}, true},
		// The server only keeps quotes for names that need them, and never
		// quotes a built-in.
		{"quoted name is a user type", cqlTypeRef{name: "text", quoted: true}, true},
		// A constructor applied to arguments is never a user type, even as the
		// operand of frozen -- frozen<map<...>> is a frozen collection.
		{"parameterised constructor is not a user type",
			cqlTypeRef{name: "map", parameterised: true, frozenOperand: true}, false},
		// Written without arguments the name is available to a user type.
		{"bare constructor name as a frozen operand is a user type",
			cqlTypeRef{name: "map", frozenOperand: true}, true},
		{"vector is a constructor", cqlTypeRef{name: "vector", parameterised: true}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			if got := tc.ref.namesUserType(); got != tc.want {
				t.Errorf("namesUserType(%+v) = %v, want %v", tc.ref, got, tc.want)
			}
		})
	}
}

// TestCQLTypeIdentifiersClassifiesParsedTypes checks the classification the
// sorter actually depends on, on parsed input rather than hand-built refs.
// TestCQLTypeIdentifiers only compares names, so it cannot see a constructor
// being mistaken for a user type.
func TestCQLTypeIdentifiersClassifiesParsedTypes(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		in   string
		name string
		want bool
	}{
		{"frozen<addr>", "addr", true},
		{"frozen<addr>", "frozen", false},
		{"frozen<list<text>>", "list", false},
		{"frozen<map>", "map", true},
		{"vector<float, 3>", "vector", false},
		{"vector<float, 3>", "float", false},
		{`frozen<"z-type">`, "z-type", true},
		// Whitespace between a constructor and its arguments must not stop it
		// reading as parameterised, or the constructor is offered as a
		// dependency in its own right.
		{"frozen <addr>", "frozen", false},
		{"frozen\t<addr>", "frozen", false},
		{"frozen\n<addr>", "frozen", false},
		{"map <text, int>", "map", false},
		// Malformed input must not corrupt what follows: the literal consumes
		// the operand slot that frozen< opened, so the text after it is the
		// built-in, not a user type inheriting a stale frozen.
		{"frozen<3>text", "text", false},
		// Same for a custom type: the literal consumes the operand slot, so
		// what follows is not read as the thing frozen was applied to.
		{`frozen<'x'>text`, "text", false},
		// A bare frozen opens no operand slot, so the token after it is read on
		// its own merits -- int stays the built-in.
		{"map<frozen, int>", "int", false},
	} {
		t.Run(fmt.Sprintf("%q/%s", tc.in, tc.name), func(t *testing.T) {
			t.Parallel()

			found := false
			for _, ref := range cqlTypeIdentifiers(tc.in) {
				if ref.name != tc.name {
					continue
				}
				found = true
				if got := ref.namesUserType(); got != tc.want {
					t.Errorf("namesUserType(%s) in %q = %v, want %v", tc.name, tc.in, got, tc.want)
				}
			}
			if !found {
				t.Fatalf("%q did not yield an identifier %q", tc.in, tc.name)
			}
		})
	}
}

// TestToCQLIsDeterministic pins that a regenerated dump does not reorder
// itself between runs. ToCQL walks six collections, five of them maps, and Go
// randomises map iteration -- before they were ordered, a keyspace with three
// tables produced three different outputs over 300 calls.
//
// Several entities per category matter here: with one apiece there is nothing
// to permute, so a fixture that size cannot see the problem.
func TestToCQLIsDeterministic(t *testing.T) {
	t.Parallel()

	build := func() *KeyspaceMetadata {
		ks := &KeyspaceMetadata{
			Name:            "ks",
			DurableWrites:   true,
			StrategyClass:   "SimpleStrategy",
			StrategyOptions: map[string]any{"replication_factor": "1"},
			Types:           map[string]*TypeMetadata{},
			Tables:          map[string]*TableMetadata{},
			Indexes:         map[string]*IndexMetadata{},
			Functions:       map[string]*FunctionMetadata{},
			Aggregates:      map[string]*AggregateMetadata{},
			Views:           map[string]*ViewMetadata{},
		}
		for _, n := range []string{"a_t", "b_t", "c_t"} {
			ks.Types[n] = &TypeMetadata{Keyspace: "ks", Name: n, FieldNames: []string{"f"}, FieldTypes: []string{"int"}}
			ks.Tables[n] = &TableMetadata{
				Name: n, OrderedColumns: []string{"id"},
				Columns:      map[string]*ColumnMetadata{"id": col("id", "uuid", ColumnPartitionKey)},
				PartitionKey: []*ColumnMetadata{col("id", "uuid", ColumnPartitionKey)},
			}
			ks.Indexes[n+"_idx"] = &IndexMetadata{
				Name: n + "_idx", KeyspaceName: "ks", TableName: n,
				Options: map[string]string{"target": "id"},
			}
			ks.Functions[n+"_fn"] = &FunctionMetadata{
				Keyspace: "ks", Name: n + "_fn",
				ArgumentNames: []string{"x"}, ArgumentTypes: []string{"int"},
				ReturnType: "int", Language: "lua", Body: "return x", CalledOnNullInput: true,
			}
			ks.Aggregates[n+"_agg"] = &AggregateMetadata{
				Keyspace: "ks", Name: n + "_agg",
				ArgumentTypes: []string{"int"}, ReturnType: "int", StateType: "int",
				InitCond: "0", StateFunc: FunctionMetadata{Name: n + "_fn"},
			}
			ks.Views[n+"_v"] = &ViewMetadata{
				KeyspaceName: "ks", ViewName: n + "_v", BaseTableName: n,
				WhereClause: "id IS NOT NULL", OrderedColumns: []string{"id"},
				PartitionKey: []*ColumnMetadata{col("id", "uuid", ColumnPartitionKey)},
			}
		}
		return ks
	}

	// A fresh keyspace each round: ToCQL caches its output in CreateStmts, so
	// reusing one would short-circuit every call after the first.
	const rounds = 100
	var first string
	for i := 0; i < rounds; i++ {
		got, err := build().ToCQL()
		if err != nil {
			t.Fatalf("round %d: ToCQL: %v", i, err)
		}
		if i == 0 {
			first = got
			continue
		}
		if got != first {
			t.Fatalf("round %d differs from round 0 -- the dump reorders itself between runs", i)
		}
	}
}
