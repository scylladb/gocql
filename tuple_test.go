//go:build integration
// +build integration

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
	"fmt"
	"reflect"
	"testing"
)

type serializationCoord struct{ X, Y int32 }

var serializationCoordCodec = serializationTuple(
	Params(ParamAt(0, Int32, func(v serializationCoord) int32 { return v.X }), ParamAt(1, Int32, func(v serializationCoord) int32 { return v.Y })),
	Row(ColumnAt(0, Int32, func(v *serializationCoord) *int32 { return &v.X }), ColumnAt(1, Int32, func(v *serializationCoord) *int32 { return &v.Y })))

type serializationNullableCoord struct{ X, Y Nullable[int32] }

var serializationNullableCoordCodec = serializationTuple(
	Params(ParamAt(0, NullableCodec(Int32), func(v serializationNullableCoord) Nullable[int32] { return v.X }), ParamAt(1, NullableCodec(Int32), func(v serializationNullableCoord) Nullable[int32] { return v.Y })),
	Row(ColumnAt(0, NullableCodec(Int32), func(v *serializationNullableCoord) *Nullable[int32] { return &v.X }), ColumnAt(1, NullableCodec(Int32), func(v *serializationNullableCoord) *Nullable[int32] { return &v.Y })))

func tupleTable(t *testing.T, s *Session, typ string) string {
	t.Helper()
	table := serializationTableName(t)
	serializationDDL(t, s, fmt.Sprintf("CREATE TABLE %s(id int PRIMARY KEY,val %s)", table, typ))
	return table
}

func TestTupleSimple(t *testing.T) {
	t.Parallel()
	s := createV2Session(t)
	defer s.Close()
	table := tupleTable(t, s, "frozen<tuple<int,int>>")
	serializationWrite(t, s, fmt.Sprintf("INSERT INTO %s(id,val) VALUES(?,(?,?))", table), IntValue(1), IntValue(100), IntValue(-100))
	type result struct {
		ID    int32
		Coord serializationCoord
	}
	decoder := Row(Column("id", Int32, func(r *result) *int32 { return &r.ID }), Column("val", serializationCoordCodec, func(r *result) *serializationCoord { return &r.Coord }))
	row, err := serializationOne(s, fmt.Sprintf("SELECT id,val FROM %s WHERE id=?", table), decoder, IntValue(1))
	if err != nil {
		t.Fatal(err)
	}
	if row.ID != 1 || row.Coord.X != 100 || row.Coord.Y != -100 {
		t.Fatalf("unexpected id and coordinates: %+v", row)
	}
}

func TestTuple_NullTuple(t *testing.T) {
	t.Parallel()
	s := createV2Session(t)
	defer s.Close()
	table := tupleTable(t, s, "frozen<tuple<int,int>>")
	got := serializationRoundTrip(t, s, table, "val", 1, serializationNullableCoordCodec, serializationNullableCoord{})
	if got.X.Valid || got.Y.Valid {
		t.Fatalf("expected two null members: %+v", got)
	}
}

func TestTuple_TupleNotSet(t *testing.T) {
	t.Parallel()
	s := createV2Session(t)
	defer s.Close()
	table := tupleTable(t, s, "frozen<tuple<int,int>>")
	want := serializationCoord{1, 2}
	got := serializationRoundTrip(t, s, table, "val", 1, serializationCoordCodec, want)
	if got != want {
		t.Fatalf("expected %+v, got %+v", want, got)
	}
	serializationWrite(t, s, fmt.Sprintf("INSERT INTO %s(id) VALUES(?)", table), IntValue(2))
	nullable, err := serializationOne(s, fmt.Sprintf("SELECT val FROM %s WHERE id=?", table), OneColumn(NullableDecoder(serializationCoordCodec)), IntValue(2))
	if err != nil {
		t.Fatal(err)
	}
	if nullable.Valid || nullable.Value != (serializationCoord{}) {
		t.Fatalf("unset tuple must be explicit null with zero destination: %+v", nullable)
	}
	if _, err := serializationOne(s, fmt.Sprintf("SELECT val FROM %s WHERE id=?", table), OneColumn(serializationCoordCodec), IntValue(2)); err == nil {
		t.Fatal("nonnullable tuple accepted null")
	}
}

func checkDynamicTuple(t *testing.T, mode string) {
	t.Helper()
	s := createV2Session(t)
	defer s.Close()
	table := tupleTable(t, s, "frozen<tuple<int,int>>")
	switch mode {
	case "present":
		serializationWrite(t, s, fmt.Sprintf("INSERT INTO %s(id,val) VALUES(1,(1,2))", table))
	case "members":
		serializationWrite(t, s, fmt.Sprintf("INSERT INTO %s(id,val) VALUES(1,(null,null))", table))
	case "absent":
		serializationWrite(t, s, fmt.Sprintf("INSERT INTO %s(id) VALUES(1)", table))
	}
	r, err := v2ReadOne(t.Context(), s, fmt.Sprintf("SELECT * FROM %s", table))
	if err != nil {
		t.Fatal(err)
	}
	if r.Columns().Len() != 2 {
		t.Fatalf("tuple expanded physical columns: %d", r.Columns().Len())
	}
	v, err := r.Get("val")
	if err != nil {
		t.Fatal(err)
	}
	if mode == "absent" {
		if v.State() != Null {
			t.Fatal("unset tuple is not null")
		}
		return
	}
	iter, err := v.Elements()
	if err != nil {
		t.Fatal(err)
	}
	members, err := Collect(iter)
	if err != nil {
		t.Fatal(err)
	}
	if len(members) != 2 {
		t.Fatalf("expected 2 members, got %d", len(members))
	}
	for i, m := range members {
		if mode == "members" {
			if m.State() != Null {
				t.Errorf("member %d not null", i)
			}
		} else {
			n, e := m.Signed()
			if e != nil || n != int64(i+1) {
				t.Errorf("member %d: %d, %v", i, n, e)
			}
		}
	}
}
func TestTupleMapScan(t *testing.T)       { t.Parallel(); checkDynamicTuple(t, "present") }
func TestTupleMapScanNil(t *testing.T)    { t.Parallel(); checkDynamicTuple(t, "members") }
func TestTupleMapScanNotSet(t *testing.T) { t.Parallel(); checkDynamicTuple(t, "absent") }

type serializationTextPair struct{ A, B Nullable[string] }

var serializationTextPairCodec = serializationTuple(
	Params(ParamAt(0, NullableCodec(serializationString), func(v serializationTextPair) Nullable[string] { return v.A }), ParamAt(1, NullableCodec(serializationString), func(v serializationTextPair) Nullable[string] { return v.B })),
	Row(ColumnAt(0, NullableCodec(serializationString), func(v *serializationTextPair) *Nullable[string] { return &v.A }), ColumnAt(1, NullableCodec(serializationString), func(v *serializationTextPair) *Nullable[string] { return &v.B })))

func TestTupleLastFieldEmpty(t *testing.T) {
	t.Parallel()
	s := createV2Session(t)
	defer s.Close()
	table := tupleTable(t, s, "frozen<tuple<text,text>>")
	want := serializationTextPair{Nullable[string]{Value: "abc", Valid: true}, Nullable[string]{Value: "", Valid: true}}
	got := serializationRoundTrip(t, s, table, "val", 1, serializationTextPairCodec, want)
	if got != want {
		t.Fatalf("trailing present empty text became null: expected %+v, got %+v", want, got)
	}
}

type serializationMixedTuple struct {
	A int32
	B string
}

var serializationMixedTupleCodec = serializationTuple(
	Params(ParamAt(0, Int32, func(v serializationMixedTuple) int32 { return v.A }), ParamAt(1, serializationString, func(v serializationMixedTuple) string { return v.B })),
	Row(ColumnAt(0, Int32, func(v *serializationMixedTuple) *int32 { return &v.A }), ColumnAt(1, serializationString, func(v *serializationMixedTuple) *string { return &v.B })))

func TestTuple_NestedCollection(t *testing.T) {
	t.Parallel()
	s := createV2Session(t)
	defer s.Close()
	table := tupleTable(t, s, "list<frozen<tuple<int,text>>>")
	// Each historical representation has an explicit typed adapter. Slice/array
	// members use Value's discriminant instead of untyped heterogeneous elements.
	sliceCodec := NamedScalar(serializationMixedTupleCodec, func(v []Value) serializationMixedTuple {
		n, _ := v[0].Signed()
		x, _ := v[1].Text()
		return serializationMixedTuple{int32(n), x.String()}
	}, func(v serializationMixedTuple) []Value { return []Value{IntValue(v.A), TextValue(v.B)} })
	arrayCodec := NamedScalar(serializationMixedTupleCodec, func(v [2]Value) serializationMixedTuple {
		n, _ := v[0].Signed()
		x, _ := v[1].Text()
		return serializationMixedTuple{int32(n), x.String()}
	}, func(v serializationMixedTuple) [2]Value { return [2]Value{IntValue(v.A), TextValue(v.B)} })
	t.Run("slice", func(t *testing.T) {
		input := [][]Value{{IntValue(1), TextValue("2")}, {IntValue(3), TextValue("4")}}
		got := serializationRoundTrip(t, s, table, "val", 0, serializationList(sliceCodec), input)
		for i := range got {
			for j := range got[i] {
				assertSerializationValue(t, input[i][j], got[i][j])
			}
		}
	})
	t.Run("array", func(t *testing.T) {
		input := [][2]Value{{IntValue(1), TextValue("2")}, {IntValue(3), TextValue("4")}}
		got := serializationRoundTrip(t, s, table, "val", 1, serializationList(arrayCodec), input)
		for i := range got {
			for j := range got[i] {
				assertSerializationValue(t, input[i][j], got[i][j])
			}
		}
	})
	t.Run("struct", func(t *testing.T) {
		input := []serializationMixedTuple{{1, "2"}, {3, "4"}}
		got := serializationRoundTrip(t, s, table, "val", 2, serializationList(serializationMixedTupleCodec), input)
		if !reflect.DeepEqual(got, input) {
			t.Fatalf("expected %#v, got %#v", input, got)
		}
	})
}
func TestTuple_NullableNestedCollection(t *testing.T) {
	t.Parallel()
	s := createV2Session(t)
	defer s.Close()
	table := tupleTable(t, s, "list<frozen<tuple<text,text>>>")
	valid := func(s string) Nullable[string] { return Nullable[string]{Value: s, Valid: true} }
	input := []serializationTextPair{{valid("1"), Nullable[string]{}}, {Nullable[string]{}, valid("2")}, {valid("3"), valid("")}}
	sliceCodec := NamedScalar(serializationTextPairCodec, func(v []Nullable[string]) serializationTextPair { return serializationTextPair{v[0], v[1]} }, func(v serializationTextPair) []Nullable[string] { return []Nullable[string]{v.A, v.B} })
	arrayCodec := NamedScalar(serializationTextPairCodec, func(v [2]Nullable[string]) serializationTextPair { return serializationTextPair{v[0], v[1]} }, func(v serializationTextPair) [2]Nullable[string] { return [2]Nullable[string]{v.A, v.B} })
	t.Run("slice", func(t *testing.T) {
		v := [][]Nullable[string]{{input[0].A, input[0].B}, {input[1].A, input[1].B}, {input[2].A, input[2].B}}
		got := serializationRoundTrip(t, s, table, "val", 0, serializationList(sliceCodec), v)
		if !reflect.DeepEqual(v, got) {
			t.Fatalf("expected %#v, got %#v", v, got)
		}
	})
	t.Run("array", func(t *testing.T) {
		v := [][2]Nullable[string]{{input[0].A, input[0].B}, {input[1].A, input[1].B}, {input[2].A, input[2].B}}
		got := serializationRoundTrip(t, s, table, "val", 1, serializationList(arrayCodec), v)
		if !reflect.DeepEqual(v, got) {
			t.Fatalf("expected %#v, got %#v", v, got)
		}
	})
	t.Run("struct", func(t *testing.T) {
		got := serializationRoundTrip(t, s, table, "val", 2, serializationList(serializationTextPairCodec), input)
		if !reflect.DeepEqual(input, got) {
			t.Fatalf("expected %#v, got %#v", input, got)
		}
	})
}
