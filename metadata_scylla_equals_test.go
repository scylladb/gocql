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
	"reflect"
	"testing"
)

// The Equals family decides whether a schema refresh sees a table as changed
// (see refreshAllSchema). A comparison that silently ignores a field makes a
// real change invisible, so most of what follows is field-by-field drift
// detection rather than a handful of hand-written pairs: a new field on one of
// these structs fails the test until it is either compared or listed as
// deliberately ignored.

// differentValue returns a value of the same type as v that is never equal to
// it. The bool reports whether the kind is handled; callers fail rather than
// skip, so a field this helper cannot mutate surfaces as a test failure
// instead of a silent hole in the drift check.
func differentValue(v reflect.Value) (reflect.Value, bool) {
	switch v.Kind() {
	case reflect.String:
		return reflect.ValueOf(v.String() + "-changed").Convert(v.Type()), true
	case reflect.Bool:
		return reflect.ValueOf(!v.Bool()).Convert(v.Type()), true
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return reflect.ValueOf(v.Int() + 1).Convert(v.Type()), true
	case reflect.Float32, reflect.Float64:
		return reflect.ValueOf(v.Float() + 1).Convert(v.Type()), true
	case reflect.Map:
		if v.Type().Key().Kind() != reflect.String {
			return reflect.Value{}, false
		}
		// A copy with one extra entry. The added key changes the length, which
		// every map comparison in this file notices, and the zero element keeps
		// this independent of what the map holds.
		changed := reflect.MakeMap(v.Type())
		iter := v.MapRange()
		for iter.Next() {
			changed.SetMapIndex(iter.Key(), iter.Value())
		}
		key := reflect.ValueOf("added-key").Convert(v.Type().Key())
		changed.SetMapIndex(key, reflect.New(v.Type().Elem()).Elem())
		return changed, true
	case reflect.Slice:
		// Appending a zero element changes the length, for the same reason.
		return reflect.Append(v, reflect.New(v.Type().Elem()).Elem()), true
	case reflect.Struct:
		// Mutate the first field this helper can handle, so a nested struct is
		// compared through its own Equals rather than skipped.
		changed := reflect.New(v.Type())
		changed.Elem().Set(v)
		for i := 0; i < v.NumField(); i++ {
			if !v.Type().Field(i).IsExported() {
				continue
			}
			field := changed.Elem().Field(i)
			if nested, ok := differentValue(field); ok {
				field.Set(nested)
				return changed.Elem(), true
			}
		}
		return reflect.Value{}, false
	default:
		return reflect.Value{}, false
	}
}

// assertEveryFieldCompared mutates each exported field of *want in turn and
// requires equals to reject the result. There is deliberately no exclusion
// list: every exported field on all four of these structs is compared today,
// so a field that cannot be mutated is a finding rather than something to
// configure away.
func assertEveryFieldCompared(t *testing.T, want any, equals func(a, b any) bool) {
	t.Helper()

	typ := reflect.TypeOf(want).Elem()

	for i := 0; i < typ.NumField(); i++ {
		field := typ.Field(i)
		if !field.IsExported() {
			continue
		}

		t.Run(field.Name, func(t *testing.T) {
			mutated := reflect.New(typ)
			mutated.Elem().Set(reflect.ValueOf(want).Elem())

			target := mutated.Elem().Field(i)
			changed, ok := differentValue(target)
			if !ok {
				t.Fatalf("differentValue cannot mutate %s (kind %s); extend the helper "+
					"so this field stays covered by the drift check", field.Name, target.Kind())
			}
			target.Set(changed)

			if equals(want, mutated.Interface()) {
				t.Errorf("Equals() = true after changing %s; the field is not compared", field.Name)
			}
		})
	}
}

func populatedTableMetadataOptions() *TableMetadataOptions {
	return &TableMetadataOptions{
		Caching:                 map[string]string{"keys": "ALL"},
		Compaction:              map[string]string{"class": "SizeTieredCompactionStrategy"},
		Compression:             map[string]string{"sstable_compression": "LZ4Compressor"},
		CDC:                     map[string]string{"enabled": "true"},
		SpeculativeRetry:        "99p",
		Comment:                 "a table",
		Version:                 "3",
		Partitioner:             "org.apache.cassandra.dht.Murmur3Partitioner",
		GcGraceSeconds:          864000,
		MaxIndexInterval:        2048,
		MemtableFlushPeriodInMs: 1,
		MinIndexInterval:        128,
		ReadRepairChance:        0.1,
		BloomFilterFpChance:     0.01,
		DefaultTimeToLive:       60,
		DcLocalReadRepairChance: 0.2,
		CrcCheckChance:          0.9,
		InMemory:                true,
	}
}

func populatedColumnMetadata() *ColumnMetadata {
	return &ColumnMetadata{
		Keyspace:        "ks",
		Table:           "tbl",
		Name:            "col",
		Type:            "text",
		ClusteringOrder: "asc",
		ComponentIndex:  1,
		Kind:            ColumnRegular,
		Order:           ASC,
		Index: ColumnIndexMetadata{
			Name:    "idx",
			Type:    "COMPOSITES",
			Options: map[string]any{"target": "col"},
		},
	}
}

func populatedTableMetadata() *TableMetadata {
	partitionKey := populatedColumnMetadata()
	partitionKey.Name = "pk"
	clustering := populatedColumnMetadata()
	clustering.Name = "ck"

	return &TableMetadata{
		Keyspace:          "ks",
		Name:              "tbl",
		PartitionKey:      []*ColumnMetadata{partitionKey},
		ClusteringColumns: []*ColumnMetadata{clustering},
		Columns: map[string]*ColumnMetadata{
			"pk": partitionKey,
			"ck": clustering,
		},
		OrderedColumns: []string{"pk", "ck"},
		Flags:          []string{"compound"},
		Extensions:     map[string]any{"scylla_encryption_options": []byte("blob")},
		Options:        *populatedTableMetadataOptions(),
	}
}

// deepCopyTableMetadata produces an independent value that must compare equal,
// so a test can mutate one side without disturbing the other.
func deepCopyTableMetadata(src *TableMetadata) *TableMetadata {
	dst := *src

	dst.PartitionKey = make([]*ColumnMetadata, len(src.PartitionKey))
	for i, c := range src.PartitionKey {
		col := *c
		dst.PartitionKey[i] = &col
	}
	dst.ClusteringColumns = make([]*ColumnMetadata, len(src.ClusteringColumns))
	for i, c := range src.ClusteringColumns {
		col := *c
		dst.ClusteringColumns[i] = &col
	}
	dst.Columns = make(map[string]*ColumnMetadata, len(src.Columns))
	for k, c := range src.Columns {
		col := *c
		dst.Columns[k] = &col
	}
	dst.OrderedColumns = append([]string(nil), src.OrderedColumns...)
	dst.Flags = append([]string(nil), src.Flags...)
	dst.Extensions = make(map[string]any, len(src.Extensions))
	for k, v := range src.Extensions {
		dst.Extensions[k] = v
	}

	return &dst
}

func TestTableMetadataOptionsEqualsComparesEveryField(t *testing.T) {
	t.Parallel()

	want := populatedTableMetadataOptions()
	if !want.Equals(populatedTableMetadataOptions()) {
		t.Fatal("Equals() = false for two identically populated values")
	}

	assertEveryFieldCompared(t, want, func(a, b any) bool {
		return a.(*TableMetadataOptions).Equals(b.(*TableMetadataOptions))
	})
}

// TestTableMetadataOptionsEqualsMapKeys covers what the drift test cannot: it
// mutates a map by adding an entry, which changes the length, so the
// length check in compareStringMaps short-circuits before the per-key
// comparison. A key renamed in place keeps the length identical and is the
// case that actually exercises the loop -- a compaction strategy swapped for
// another with the same number of options looks exactly like this.
func TestTableMetadataOptionsEqualsMapKeys(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		mutate func(*TableMetadataOptions)
		want   bool
	}{
		{name: "identical", mutate: func(*TableMetadataOptions) {}, want: true},
		{
			name: "caching key renamed",
			mutate: func(o *TableMetadataOptions) {
				o.Caching = map[string]string{"rows_per_partition": "ALL"}
			},
		},
		{
			name: "caching value changed",
			mutate: func(o *TableMetadataOptions) {
				o.Caching = map[string]string{"keys": "NONE"}
			},
		},
		{
			name: "compaction key renamed",
			mutate: func(o *TableMetadataOptions) {
				o.Compaction = map[string]string{"strategy": "SizeTieredCompactionStrategy"}
			},
		},
		{
			name:   "nil map versus populated",
			mutate: func(o *TableMetadataOptions) { o.CDC = nil },
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			a := populatedTableMetadataOptions()
			b := populatedTableMetadataOptions()
			tt.mutate(b)

			if got := a.Equals(b); got != tt.want {
				t.Errorf("Equals() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestColumnMetadataEqualsComparesEveryField(t *testing.T) {
	t.Parallel()

	want := populatedColumnMetadata()
	if !want.Equals(populatedColumnMetadata()) {
		t.Fatal("Equals() = false for two identically populated values")
	}

	assertEveryFieldCompared(t, want, func(a, b any) bool {
		return a.(*ColumnMetadata).Equals(b.(*ColumnMetadata))
	})
}

// TestColumnMetadataEqualsIncludesIndex pins a second field of the nested
// index. The drift test above does reach Index -- differentValue recurses into
// a struct -- but it stops at the first field it can mutate, and
// ColumnIndexMetadata declares Options first, so Index.Name is never the field
// it changes. This covers that one directly.
func TestColumnMetadataEqualsIncludesIndex(t *testing.T) {
	t.Parallel()

	a := populatedColumnMetadata()
	b := populatedColumnMetadata()
	b.Index.Name = "different-index"

	if a.Equals(b) {
		t.Error("Equals() = true for columns differing only in Index.Name")
	}
}

// TestColumnIndexMetadataEqualsComparesEveryField is the drift guard for
// ColumnIndexMetadata. TestColumnIndexMetadataEquals below pins the specific
// semantics (a renamed key, a composite value); this one only ensures no
// exported field can be added and left out of the comparison unnoticed.
func TestColumnIndexMetadataEqualsComparesEveryField(t *testing.T) {
	t.Parallel()

	want := populatedColumnMetadata().Index
	other := populatedColumnMetadata().Index
	if !want.Equals(&other) {
		t.Fatal("Equals() = false for two identically populated values")
	}

	assertEveryFieldCompared(t, &want, func(a, b any) bool {
		return a.(*ColumnIndexMetadata).Equals(b.(*ColumnIndexMetadata))
	})
}

func TestColumnIndexMetadataEquals(t *testing.T) {
	t.Parallel()

	base := func() *ColumnIndexMetadata {
		return &ColumnIndexMetadata{
			Name:    "idx",
			Type:    "COMPOSITES",
			Options: map[string]any{"target": "col", "nested": []string{"a", "b"}},
		}
	}

	tests := []struct {
		name   string
		mutate func(*ColumnIndexMetadata)
		want   bool
	}{
		{name: "identical", mutate: func(*ColumnIndexMetadata) {}, want: true},
		{name: "different name", mutate: func(c *ColumnIndexMetadata) { c.Name = "other" }},
		{name: "different type", mutate: func(c *ColumnIndexMetadata) { c.Type = "KEYS" }},
		{
			name:   "extra option",
			mutate: func(c *ColumnIndexMetadata) { c.Options["extra"] = "value" },
		},
		{
			name:   "missing option",
			mutate: func(c *ColumnIndexMetadata) { delete(c.Options, "target") },
		},
		{
			name:   "same key different value",
			mutate: func(c *ColumnIndexMetadata) { c.Options["target"] = "other" },
		},
		{
			// Options is map[string]any, so its values are compared with
			// reflect.DeepEqual rather than ==. A slice value must still be
			// compared by content.
			name:   "same key different slice value",
			mutate: func(c *ColumnIndexMetadata) { c.Options["nested"] = []string{"a", "c"} },
		},
		{
			// Equal length, disjoint keys: a length-only check would pass this.
			name: "same size different keys",
			mutate: func(c *ColumnIndexMetadata) {
				delete(c.Options, "target")
				c.Options["renamed"] = "col"
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			a, b := base(), base()
			tt.mutate(b)

			if got := a.Equals(b); got != tt.want {
				t.Errorf("Equals() = %v, want %v", got, tt.want)
			}
		})
	}
}

// TestCompareInterfaceMapsDirectly exercises compareInterfaceMaps on its own,
// because its two callers reach it differently. TableMetadata.Equals checks the
// lengths before calling, so the helper's own length check is redundant on that
// path. refreshAllSchema does not: it compares a keyspace's StrategyOptions
// through this helper with no guard of its own, which makes that length check
// live logic -- it is what notices a replication option being added or removed,
// and that decides whether the keyspace's tablets are dropped.
func TestCompareInterfaceMapsDirectly(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		a, b map[string]any
		want bool
	}{
		{name: "both nil", a: nil, b: nil, want: true},
		{name: "nil versus empty", a: nil, b: map[string]any{}, want: true},
		{
			name: "identical",
			a:    map[string]any{"k": []byte("v")},
			b:    map[string]any{"k": []byte("v")},
			want: true,
		},
		{
			name: "different lengths",
			a:    map[string]any{"k": "v"},
			b:    map[string]any{"k": "v", "extra": "x"},
		},
		{
			name: "shorter receiver",
			a:    map[string]any{},
			b:    map[string]any{"k": "v"},
		},
		{
			name: "same length disjoint keys",
			a:    map[string]any{"k": "v"},
			b:    map[string]any{"other": "v"},
		},
		{
			name: "same key different value",
			a:    map[string]any{"k": "v"},
			b:    map[string]any{"k": "w"},
		},
		{
			// DeepEqual rather than ==, so composite values compare by content
			// and a byte slice that merely looks alike is still equal.
			name: "composite values compared by content",
			a:    map[string]any{"k": map[string]int{"n": 1}},
			b:    map[string]any{"k": map[string]int{"n": 1}},
			want: true,
		},
		{
			name: "composite values differing",
			a:    map[string]any{"k": map[string]int{"n": 1}},
			b:    map[string]any{"k": map[string]int{"n": 2}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if got := compareInterfaceMaps(tt.a, tt.b); got != tt.want {
				t.Errorf("compareInterfaceMaps() = %v, want %v", got, tt.want)
			}
		})
	}
}

// TestTableMetadataEqualsComparesEveryField is the drift guard for
// TableMetadata. TestTableMetadataEquals below pins the specific semantics
// (a renamed column key, a reordered column list); this one only ensures no
// exported field can be added and left out of the comparison unnoticed.
func TestTableMetadataEqualsComparesEveryField(t *testing.T) {
	t.Parallel()

	want := populatedTableMetadata()
	if !want.Equals(deepCopyTableMetadata(want)) {
		t.Fatal("Equals() = false for two identically populated values")
	}

	assertEveryFieldCompared(t, want, func(a, b any) bool {
		return a.(*TableMetadata).Equals(b.(*TableMetadata))
	})
}

func TestTableMetadataEquals(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		mutate func(*TableMetadata)
		want   bool
	}{
		{name: "identical", mutate: func(*TableMetadata) {}, want: true},
		{name: "keyspace", mutate: func(m *TableMetadata) { m.Keyspace = "other" }},
		{name: "name", mutate: func(m *TableMetadata) { m.Name = "other" }},
		{
			name:   "partition key column",
			mutate: func(m *TableMetadata) { m.PartitionKey[0].Name = "other" },
		},
		{
			name:   "clustering column",
			mutate: func(m *TableMetadata) { m.ClusteringColumns[0].Type = "int" },
		},
		{
			name:   "column map value",
			mutate: func(m *TableMetadata) { m.Columns["pk"].Type = "int" },
		},
		{
			// Same count, different key: the length guard passes and
			// compareColumnsMap has to do the work.
			name: "column map key renamed",
			mutate: func(m *TableMetadata) {
				m.Columns["renamed"] = m.Columns["pk"]
				delete(m.Columns, "pk")
			},
		},
		{
			name:   "ordered columns reordered",
			mutate: func(m *TableMetadata) { m.OrderedColumns = []string{"ck", "pk"} },
		},
		{name: "flags", mutate: func(m *TableMetadata) { m.Flags = []string{"dense"} }},
		{
			name:   "options",
			mutate: func(m *TableMetadata) { m.Options.GcGraceSeconds = 0 },
		},
		{
			name:   "extension value",
			mutate: func(m *TableMetadata) { m.Extensions["scylla_encryption_options"] = []byte("other") },
		},
		{
			name:   "extension added",
			mutate: func(m *TableMetadata) { m.Extensions["extra"] = []byte("x") },
		},
		{
			// Equal length, disjoint keys: compareInterfaceMaps has to notice
			// the missing key rather than stopping at the length check.
			name: "extension key renamed",
			mutate: func(m *TableMetadata) {
				m.Extensions["renamed"] = m.Extensions["scylla_encryption_options"]
				delete(m.Extensions, "scylla_encryption_options")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			a := populatedTableMetadata()
			b := deepCopyTableMetadata(a)
			tt.mutate(b)

			if got := a.Equals(b); got != tt.want {
				t.Errorf("Equals() = %v, want %v", got, tt.want)
			}
		})
	}
}

// TestTableMetadataEqualsLengthMismatch exercises the guards that make the
// comparison safe as well as correct. The cases do not all do the same work.
//
// compareColumnSlices and compareStringSlices range over the receiver's slice
// and index the argument's at the same offset, so they run off the end when the
// receiver is the longer one. Dropping the length check ahead of any of them --
// partition key, clustering columns, ordered columns, flags -- makes this test
// panic rather than merely report unequal.
//
// The two map-backed cases are weaker, and are here for correctness rather than
// for safety. compareColumnsMap cannot index past an end, so dropping the
// columns guard is caught only because the shorter-receiver direction then
// returns a wrong true. compareInterfaceMaps repeats the length check itself,
// which makes the extensions guard redundant: dropping it changes no behaviour
// and this test stays green. That case is kept because differing extension
// counts must still compare unequal, not because it pins the guard.
func TestTableMetadataEqualsLengthMismatch(t *testing.T) {
	t.Parallel()

	shorten := map[string]func(*TableMetadata){
		"partition key": func(m *TableMetadata) { m.PartitionKey = nil },
		"clustering columns": func(m *TableMetadata) {
			m.ClusteringColumns = nil
		},
		"columns":         func(m *TableMetadata) { delete(m.Columns, "pk") },
		"ordered columns": func(m *TableMetadata) { m.OrderedColumns = []string{"pk"} },
		"flags":           func(m *TableMetadata) { m.Flags = nil },
		"extensions":      func(m *TableMetadata) { m.Extensions = nil },
	}

	for name, shorter := range shorten {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			full := populatedTableMetadata()
			trimmed := deepCopyTableMetadata(full)
			shorter(trimmed)

			// compareColumnSlices and compareStringSlices range over the
			// receiver's slice and index the argument's at the same offset, so
			// the overrun needs the receiver to be the longer of the two:
			// full.Equals(trimmed) is the call that panics if a length guard is
			// dropped. The reverse direction cannot overrun, since it ranges
			// over the shorter slice; it is here so the guard is shown to
			// reject the mismatch either way round rather than only when the
			// receiver happens to be longer.
			if full.Equals(trimmed) {
				t.Error("Equals() = true for a longer receiver and shorter argument")
			}
			if trimmed.Equals(full) {
				t.Error("Equals() = true for a shorter receiver and longer argument")
			}
		})
	}
}

// TestMetadataEqualsNilHandling pins the shared nil contract: two nils are
// equal, a nil and a non-nil are not, and neither dereferences.
func TestMetadataEqualsNilHandling(t *testing.T) {
	t.Parallel()

	t.Run("TableMetadataOptions", func(t *testing.T) {
		t.Parallel()

		var nilOpts *TableMetadataOptions
		populated := populatedTableMetadataOptions()

		if !nilOpts.Equals(nil) {
			t.Error("Equals() = false for two nils, want true")
		}
		if populated.Equals(nil) {
			t.Error("Equals(nil) = true for a populated receiver")
		}
		if nilOpts.Equals(populated) {
			t.Error("nil.Equals(populated) = true")
		}
	})

	t.Run("ColumnMetadata", func(t *testing.T) {
		t.Parallel()

		var nilCol *ColumnMetadata
		populated := populatedColumnMetadata()

		if !nilCol.Equals(nil) {
			t.Error("Equals() = false for two nils, want true")
		}
		if populated.Equals(nil) {
			t.Error("Equals(nil) = true for a populated receiver")
		}
		if nilCol.Equals(populated) {
			t.Error("nil.Equals(populated) = true")
		}
	})

	t.Run("ColumnIndexMetadata", func(t *testing.T) {
		t.Parallel()

		var nilIdx *ColumnIndexMetadata
		populated := &ColumnIndexMetadata{Name: "idx"}

		if !nilIdx.Equals(nil) {
			t.Error("Equals() = false for two nils, want true")
		}
		if populated.Equals(nil) {
			t.Error("Equals(nil) = true for a populated receiver")
		}
		if nilIdx.Equals(populated) {
			t.Error("nil.Equals(populated) = true")
		}
	})

	t.Run("TableMetadata", func(t *testing.T) {
		t.Parallel()

		var nilTable *TableMetadata
		populated := populatedTableMetadata()

		if !nilTable.Equals(nil) {
			t.Error("Equals() = false for two nils, want true")
		}
		if populated.Equals(nil) {
			t.Error("Equals(nil) = true for a populated receiver")
		}
		if nilTable.Equals(populated) {
			t.Error("nil.Equals(populated) = true")
		}
	})

	// A nil entry inside a compared slice must not panic either: the driver
	// builds these slices from server rows, and a gap leaves a nil behind.
	t.Run("nil column inside a slice", func(t *testing.T) {
		t.Parallel()

		a := populatedTableMetadata()
		b := deepCopyTableMetadata(a)
		b.PartitionKey[0] = nil

		if a.Equals(b) {
			t.Error("Equals() = true when one partition key column is nil")
		}
		if b.Equals(a) {
			t.Error("Equals() = true when the receiver's partition key column is nil")
		}
	})
}

// TestTableMetadataEqualsEmptyAndZero covers the shapes a table can legitimately
// have before any rows are compiled into it, which is when refreshAllSchema is
// most likely to compare two of them.
func TestTableMetadataEqualsEmptyAndZero(t *testing.T) {
	t.Parallel()

	if !(&TableMetadata{}).Equals(&TableMetadata{}) {
		t.Error("Equals() = false for two zero-valued tables")
	}

	nilMaps := &TableMetadata{Keyspace: "ks", Name: "tbl"}
	emptyMaps := &TableMetadata{
		Keyspace:   "ks",
		Name:       "tbl",
		Columns:    map[string]*ColumnMetadata{},
		Extensions: map[string]any{},
	}

	// A nil map and an empty map are both "no entries", and the length-based
	// guards treat them alike. Pinning this keeps a refresh from reporting a
	// change when the server simply returned no rows for a section.
	if !nilMaps.Equals(emptyMaps) {
		t.Error("Equals() = false for nil maps versus empty maps, want true")
	}
}
