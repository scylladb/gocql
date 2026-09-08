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
	"bytes"
	"reflect"
	"testing"
)

func TestRowMapBytesFastPath(t *testing.T) {
	src := []byte{1, 2, 3, 4}
	var nilBlob []byte
	emptyBlob := []byte{}
	other := []any{"other"}
	rd := &RowData{
		Columns: []string{"blob_col", "nil_blob_col", "empty_blob_col", "other_col"},
		Values:  []any{&src, &nilBlob, &emptyBlob, &other},
	}

	m := make(map[string]any, len(rd.Columns))
	rd.rowMap(m)

	got, ok := m["blob_col"].([]byte)
	if !ok {
		t.Fatalf("blob_col: expected []byte, got %T", m["blob_col"])
	}
	if !bytes.Equal(got, src) {
		t.Fatalf("blob_col: content mismatch, got %v want %v", got, src)
	}
	if len(got) > 0 && &got[0] == &src[0] {
		t.Fatal("blob_col: expected a defensive copy, but backing array is shared with source")
	}
	// Mutating the copy must not affect the original source slice.
	got[0] = 99
	if src[0] == 99 {
		t.Fatal("blob_col: mutating the returned copy mutated the source slice")
	}

	// A nil []byte column comes back as a typed-nil []byte inside the
	// interface (matching the pre-existing reflect-path behavior for nil
	// slices), not an untyped nil.
	nilGot, ok := m["nil_blob_col"].([]byte)
	if !ok || nilGot != nil {
		t.Fatalf("nil_blob_col: expected a nil []byte, got %#v", m["nil_blob_col"])
	}

	// A non-nil, empty []byte column must come back as a distinct non-nil
	// empty slice (not aliased to the source, not collapsed to nil) --
	// exercising the b != nil branch with a zero-length slice.
	emptyGot, ok := m["empty_blob_col"].([]byte)
	if !ok || emptyGot == nil || len(emptyGot) != 0 {
		t.Fatalf("empty_blob_col: expected a non-nil, empty []byte, got %#v", m["empty_blob_col"])
	}

	gotOther, ok := m["other_col"].([]any)
	if !ok || len(gotOther) != 1 || gotOther[0] != "other" {
		t.Fatalf("other_col: unexpected value %#v", m["other_col"])
	}
	// The generic reflect path must also return a defensive copy: mutating
	// it must not affect the original source slice.
	gotOther[0] = "mutated"
	if other[0] != "other" {
		t.Fatal("other_col: mutating the returned copy mutated the source slice")
	}
}

// TestGetCassandraLongTypeRejectsShortSplits verifies that a composite Java type
// name whose argument list is missing or truncated degrades to an opaque custom
// type instead of panicking. getCassandraLongType is reached from readTypeInfo
// with a wire-supplied subtype string, so an unguarded names[1] or a negative
// make() raises a runtime.Error that parseFrame's recover re-panics, killing the
// serve goroutine. Only the MapType arm guarded its split length.
//
// The well-formed names are here so the degrade path cannot silently swallow
// valid input.
func TestGetCassandraLongTypeRejectsShortSplits(t *testing.T) {
	t.Parallel()

	const p = apacheCassandraTypePrefix

	tests := []struct {
		name string
		typ  string
		want TypeInfo
	}{
		{name: "vector without arguments", typ: p + "VectorType", want: NewNativeType(0, TypeCustom)},
		{name: "vector without dimensions", typ: p + "VectorType(" + p + "FloatType)", want: NewNativeType(0, TypeCustom)},
		{name: "udt without arguments", typ: p + "UserType", want: NewNativeType(0, TypeCustom)},
		{name: "udt without a name", typ: p + "UserType(gocql_test)", want: NewNativeType(0, TypeCustom)},
		{name: "udt field without a type", typ: p + "UserType(gocql_test,706572736f6e,616765)", want: NewNativeType(0, TypeCustom)},
		{
			name: "vector of float",
			typ:  p + "VectorType(" + p + "FloatType, 3)",
			want: VectorType{
				NativeType: NewCustomType(0, TypeCustom, p+"VectorType"),
				SubType:    NewNativeType(0, TypeFloat),
				Dimensions: 3,
			},
		},
		{
			name: "udt",
			typ:  p + "UserType(gocql_test,706572736f6e,616765:" + p + "Int32Type)",
			want: UDTTypeInfo{
				NativeType: NewNativeType(0, TypeUDT),
				KeySpace:   "gocql_test",
				Name:       "person",
				Elements:   []UDTField{{Name: "age", Type: NewNativeType(0, TypeInt)}},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			got := getCassandraLongType(test.typ, 0, nopLogger{})
			if !reflect.DeepEqual(got, test.want) {
				t.Errorf("getCassandraLongType(%q) = %#v, want %#v", test.typ, got, test.want)
			}
		})
	}
}
