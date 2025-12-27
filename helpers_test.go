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
	"reflect"
	"testing"
)

func TestGetCassandraType_Set(t *testing.T) {
	t.Parallel()

	typ := getCassandraType("set<text>", protoVersion4, &defaultLogger{})
	set, ok := typ.(CollectionType)
	if !ok {
		t.Fatalf("expected CollectionType got %T", typ)
	} else if set.typ != TypeSet {
		t.Fatalf("expected type %v got %v", TypeSet, set.typ)
	}

	inner, ok := set.Elem.(NativeType)
	if !ok {
		t.Fatalf("expected to get NativeType got %T", set.Elem)
	} else if inner.typ != TypeText {
		t.Fatalf("expected to get %v got %v for set value", TypeText, set.typ)
	}
}

func TestGetCassandraType(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input string
		exp   TypeInfo
	}{
		{
			"set<text>", CollectionType{
				NativeType: NativeType{typ: TypeSet},

				Elem: NativeType{typ: TypeText},
			},
		},
		{
			"map<text, varchar>", CollectionType{
				NativeType: NativeType{typ: TypeMap},

				Key:  NativeType{typ: TypeText},
				Elem: NativeType{typ: TypeVarchar},
			},
		},
		{
			"list<int>", CollectionType{
				NativeType: NativeType{typ: TypeList},
				Elem:       NativeType{typ: TypeInt},
			},
		},
		{
			"tuple<int, int, text>", TupleTypeInfo{
				NativeType: NativeType{typ: TypeTuple},

				Elems: []TypeInfo{
					NativeType{typ: TypeInt},
					NativeType{typ: TypeInt},
					NativeType{typ: TypeText},
				},
			},
		},
		{
			"frozen<map<text, frozen<list<frozen<tuple<int, int>>>>>>", CollectionType{
				NativeType: NativeType{typ: TypeMap},

				Key: NativeType{typ: TypeText},
				Elem: CollectionType{
					NativeType: NativeType{typ: TypeList},
					Elem: TupleTypeInfo{
						NativeType: NativeType{typ: TypeTuple},

						Elems: []TypeInfo{
							NativeType{typ: TypeInt},
							NativeType{typ: TypeInt},
						},
					},
				},
			},
		},
		{
			"frozen<tuple<frozen<tuple<text, frozen<list<frozen<tuple<int, int>>>>>>, frozen<tuple<text, frozen<list<frozen<tuple<int, int>>>>>>,  frozen<map<text, frozen<list<frozen<tuple<int, int>>>>>>>>",
			TupleTypeInfo{
				NativeType: NativeType{typ: TypeTuple},
				Elems: []TypeInfo{
					TupleTypeInfo{
						NativeType: NativeType{typ: TypeTuple},
						Elems: []TypeInfo{
							NativeType{typ: TypeText},
							CollectionType{
								NativeType: NativeType{typ: TypeList},
								Elem: TupleTypeInfo{
									NativeType: NativeType{typ: TypeTuple},
									Elems: []TypeInfo{
										NativeType{typ: TypeInt},
										NativeType{typ: TypeInt},
									},
								},
							},
						},
					},
					TupleTypeInfo{
						NativeType: NativeType{typ: TypeTuple},
						Elems: []TypeInfo{
							NativeType{typ: TypeText},
							CollectionType{
								NativeType: NativeType{typ: TypeList},
								Elem: TupleTypeInfo{
									NativeType: NativeType{typ: TypeTuple},
									Elems: []TypeInfo{
										NativeType{typ: TypeInt},
										NativeType{typ: TypeInt},
									},
								},
							},
						},
					},
					CollectionType{
						NativeType: NativeType{typ: TypeMap},
						Key:        NativeType{typ: TypeText},
						Elem: CollectionType{
							NativeType: NativeType{typ: TypeList},
							Elem: TupleTypeInfo{
								NativeType: NativeType{typ: TypeTuple},
								Elems: []TypeInfo{
									NativeType{typ: TypeInt},
									NativeType{typ: TypeInt},
								},
							},
						},
					},
				},
			},
		},
		{
			"frozen<tuple<frozen<tuple<int, int>>, int, frozen<tuple<int, int>>>>", TupleTypeInfo{
				NativeType: NativeType{typ: TypeTuple},

				Elems: []TypeInfo{
					TupleTypeInfo{
						NativeType: NativeType{typ: TypeTuple},

						Elems: []TypeInfo{
							NativeType{typ: TypeInt},
							NativeType{typ: TypeInt},
						},
					},
					NativeType{typ: TypeInt},
					TupleTypeInfo{
						NativeType: NativeType{typ: TypeTuple},

						Elems: []TypeInfo{
							NativeType{typ: TypeInt},
							NativeType{typ: TypeInt},
						},
					},
				},
			},
		},
		{
			"frozen<map<frozen<tuple<int, int>>, int>>", CollectionType{
				NativeType: NativeType{typ: TypeMap},

				Key: TupleTypeInfo{
					NativeType: NativeType{typ: TypeTuple},

					Elems: []TypeInfo{
						NativeType{typ: TypeInt},
						NativeType{typ: TypeInt},
					},
				},
				Elem: NativeType{typ: TypeInt},
			},
		},
		{
			"set<smallint>", CollectionType{
				NativeType: NativeType{typ: TypeSet},
				Elem:       NativeType{typ: TypeSmallInt},
			},
		},
		{
			"list<tinyint>", CollectionType{
				NativeType: NativeType{typ: TypeList},
				Elem:       NativeType{typ: TypeTinyInt},
			},
		},
		{"smallint", NativeType{typ: TypeSmallInt}},
		{"tinyint", NativeType{typ: TypeTinyInt}},
		{"duration", NativeType{typ: TypeDuration}},
		{"date", NativeType{typ: TypeDate}},
		{
			"list<date>", CollectionType{
				NativeType: NativeType{typ: TypeList},
				Elem:       NativeType{typ: TypeDate},
			},
		},
		{
			"set<duration>", CollectionType{
				NativeType: NativeType{typ: TypeSet},
				Elem:       NativeType{typ: TypeDuration},
			},
		},
		{
			"vector<float, 3>", VectorType{
				NativeType: NativeType{
					typ:    TypeCustom,
					custom: "org.apache.cassandra.db.marshal.VectorType",
				},
				SubType:    NativeType{typ: TypeFloat},
				Dimensions: 3,
			},
		},
		{
			"vector<vector<float, 3>, 5>", VectorType{
				NativeType: NativeType{
					typ:    TypeCustom,
					custom: "org.apache.cassandra.db.marshal.VectorType",
				},
				SubType: VectorType{
					NativeType: NativeType{
						typ:    TypeCustom,
						custom: "org.apache.cassandra.db.marshal.VectorType",
					},
					SubType:    NativeType{typ: TypeFloat},
					Dimensions: 3,
				},
				Dimensions: 5,
			},
		},
		{
			"vector<map<uuid,timestamp>, 5>", VectorType{
				NativeType: NativeType{
					typ:    TypeCustom,
					custom: "org.apache.cassandra.db.marshal.VectorType",
				},
				SubType: CollectionType{
					NativeType: NativeType{typ: TypeMap},
					Key:        NativeType{typ: TypeUUID},
					Elem:       NativeType{typ: TypeTimestamp},
				},
				Dimensions: 5,
			},
		},
		{
			"vector<frozen<tuple<int, float>>, 100>", VectorType{
				NativeType: NativeType{
					typ:    TypeCustom,
					custom: "org.apache.cassandra.db.marshal.VectorType",
				},
				SubType: TupleTypeInfo{
					NativeType: NativeType{typ: TypeTuple},
					Elems: []TypeInfo{
						NativeType{typ: TypeInt},
						NativeType{typ: TypeFloat},
					},
				},
				Dimensions: 100,
			},
		},
	}

	for _, test := range tests {
		t.Run(test.input, func(t *testing.T) {
			got := getCassandraType(test.input, 0, &defaultLogger{})

			// TODO(zariel): define an equal method on the types?
			if !reflect.DeepEqual(got, test.exp) {
				t.Fatalf("expected %v got %v", test.exp, got)
			}
		})
	}
}

func TestRowMapPreservesPointers(t *testing.T) {
	t.Parallel()

	// Test that rowMap preserves pointer types in the map
	var testString string
	var testInt int

	// Simulate MapScan: pre-populate the map with pointers
	m := map[string]interface{}{
		"text_col": &testString,
		"int_col":  &testInt,
	}

	rowData := RowData{
		Columns: []string{"text_col", "int_col"},
		Values:  []interface{}{&testString, &testInt},
	}

	// Simulate what Scan does - populate the values
	testString = "hello"
	testInt = 42

	rowData.rowMap(m)

	// Verify that pointers are preserved in the map
	textPtr, ok := m["text_col"].(*string)
	if !ok {
		t.Fatalf("expected *string in map, got %T", m["text_col"])
	}
	if textPtr != &testString {
		t.Fatal("pointer in map is not the same as original pointer")
	}

	intPtr, ok := m["int_col"].(*int)
	if !ok {
		t.Fatalf("expected *int in map, got %T", m["int_col"])
	}
	if intPtr != &testInt {
		t.Fatal("pointer in map is not the same as original pointer")
	}
}

func TestRowMapNonPointers(t *testing.T) {
	t.Parallel()

	// Test that rowMap still works correctly for non-pointer values
	testString := "hello"
	testInt := 42

	rowData := RowData{
		Columns: []string{"text_col", "int_col"},
		Values:  []interface{}{testString, testInt},
	}

	m := make(map[string]interface{})
	rowData.rowMap(m)

	// Verify that non-pointer values are still dereferenced/copied correctly
	if m["text_col"] != "hello" {
		t.Fatalf("expected 'hello' in map, got %v", m["text_col"])
	}

	if m["int_col"] != 42 {
		t.Fatalf("expected 42 in map, got %v", m["int_col"])
	}
}

func TestRowMapMixedPointers(t *testing.T) {
	t.Parallel()

	// Test mixed case: some columns provided with pointers, some not
	var providedText string
	providedText = "provided"

	// Simulate RowData with pointers
	var defaultInt int
	defaultInt = 42

	// Pre-populate map with only one pointer (simulating partial MapScan input)
	m := map[string]interface{}{
		"text_col": &providedText,
		// int_col is NOT in the map initially
	}

	rowData := RowData{
		Columns: []string{"text_col", "int_col"},
		Values:  []interface{}{&providedText, &defaultInt},
	}

	rowData.rowMap(m)

	// Verify that the provided pointer is preserved
	textPtr, ok := m["text_col"].(*string)
	if !ok {
		t.Fatalf("expected *string for text_col, got %T", m["text_col"])
	}
	if textPtr != &providedText {
		t.Fatal("text_col pointer is not the same as original pointer")
	}

	// Verify that the non-provided column gets the dereferenced value
	intVal, ok := m["int_col"].(int)
	if !ok {
		t.Fatalf("expected int for int_col, got %T", m["int_col"])
	}
	if intVal != 42 {
		t.Fatalf("expected 42 for int_col, got %v", intVal)
	}
}
