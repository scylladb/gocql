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
	"strings"
	"testing"
	"time"
)

type position struct {
	Lat, Lon int32
	Padding  string
}

var positionFields = serializationUDT(
	Params(Param("lat", Int32, func(p position) int32 { return p.Lat }), Param("lon", Int32, func(p position) int32 { return p.Lon }), Param("padding", serializationString, func(p position) string { return p.Padding })),
	Row(Column("lat", Int32, func(p *position) *int32 { return &p.Lat }), Column("lon", Int32, func(p *position) *int32 { return &p.Lon }), Column("padding", serializationString, func(p *position) *string { return &p.Padding })), UDTOptions{})

// The original per-member hooks become an application-owned typed Codec.
// Clone is valid before any metadata has been compiled.
type positionCodec struct{}

func (positionCodec) CheckType(t DataType) error              { return positionFields.CheckType(t) }
func (positionCodec) Encode(w *ValueWriter, p position) error { return positionFields.Encode(w, p) }
func (positionCodec) Decode(r ValueReader) (position, error)  { return positionFields.Decode(r) }
func (positionCodec) Clone(p position) position               { return p }

func udtTable(t *testing.T, s *Session, fields string) (string, string) {
	t.Helper()
	table, typ := serializationTableName(t), serializationTypeName(t)
	serializationDDL(t, s, fmt.Sprintf("CREATE TYPE %s(%s)", typ, fields))
	serializationDDL(t, s, fmt.Sprintf("CREATE TABLE %s(id int PRIMARY KEY,udt_col frozen<%s>)", table, typ))
	return table, typ
}
func TestUDT_Marshaler(t *testing.T) {
	t.Parallel()
	s := createV2Session(t)
	defer s.Close()
	table, _ := udtTable(t, s, "lat int,lon int,padding text")
	want := position{-1, 2, strings.Repeat("X", 1000)}
	got := serializationRoundTrip(t, s, table, "udt_col", 1, positionCodec{}, want)
	if got != want {
		t.Fatalf("custom position codec: expected %+v, got %+v", want, got)
	}
}

type serializationHorse struct{ Name, Owner string }

var serializationHorseCodec = serializationUDT(
	Params(Param("name", serializationString, func(v serializationHorse) string { return v.Name }), Param("owner", serializationString, func(v serializationHorse) string { return v.Owner })),
	Row(Column("name", serializationString, func(v *serializationHorse) *string { return &v.Name }), Column("owner", serializationString, func(v *serializationHorse) *string { return &v.Owner })), UDTOptions{})

func TestUDT_Reflect(t *testing.T) {
	t.Parallel()
	s := createV2Session(t)
	defer s.Close()
	table, _ := udtTable(t, s, "name text,owner text")
	// Explicit accessors replace struct-tag discovery while preserving both fields.
	want := serializationHorse{"pony", "jim"}
	got := serializationRoundTrip(t, s, table, "udt_col", 1, serializationHorseCodec, want)
	if got != want {
		t.Fatalf("expected %+v, got %+v", want, got)
	}
}
func TestUDT_NullObject(t *testing.T) {
	t.Parallel()
	s := createV2Session(t)
	defer s.Close()
	table, _ := udtTable(t, s, "name text,owner text")
	serializationWrite(t, s, fmt.Sprintf("INSERT INTO %s(id) VALUES(1)", table))
	got := Nullable[serializationHorse]{Value: serializationHorse{"temp", "temp"}, Valid: true}
	var err error
	got, err = serializationOne(s, fmt.Sprintf("SELECT udt_col FROM %s WHERE id=1", table), OneColumn(NullableDecoder(serializationHorseCodec)))
	if err != nil {
		t.Fatal(err)
	}
	if got.Valid || got.Value != (serializationHorse{}) {
		t.Fatalf("null UDT did not reset destination: %+v", got)
	}
	if _, err := serializationOne(s, fmt.Sprintf("SELECT udt_col FROM %s WHERE id=1", table), OneColumn(serializationHorseCodec)); err == nil {
		t.Fatal("nonnullable UDT accepted null")
	}
}
func TestMapScanUDT(t *testing.T) {
	t.Parallel()
	s := createV2Session(t)
	defer s.Close()
	table, typ := serializationTableName(t), serializationTypeName(t)
	serializationDDL(t, s, fmt.Sprintf("CREATE TYPE %s(created_timestamp timestamp,message text)", typ))
	serializationDDL(t, s, fmt.Sprintf("CREATE TABLE %s(id int PRIMARY KEY,type int,log_entries list<frozen<%s>>)", table, typ))
	type entry struct {
		Created Timestamp
		Message string
	}
	codec := serializationUDT(Params(Param("created_timestamp", TimestampCodec, func(v entry) Timestamp { return v.Created }), Param("message", serializationString, func(v entry) string { return v.Message })), Row(Column("created_timestamp", TimestampCodec, func(v *entry) *Timestamp { return &v.Created }), Column("message", serializationString, func(v *entry) *string { return &v.Message })), UDTOptions{})
	now := time.Now().Truncate(time.Millisecond)
	stamp, err := TimestampFromTime(now)
	if err != nil {
		t.Fatal(err)
	}
	want := []entry{{stamp, "test time now"}}
	serializationRoundTrip(t, s, table, "log_entries", 1, serializationList(codec), want)
	serializationWrite(t, s, fmt.Sprintf("UPDATE %s SET type=1 WHERE id=1", table))
	rows, err := v2Query(t.Context(), s, fmt.Sprintf("SELECT * FROM %s WHERE id=1", table))
	if err != nil {
		t.Fatal(err)
	}
	if !rows.Next() {
		t.Fatalf("missing row: %v", rows.Err())
	}
	lease, err := rows.Retain()
	if err != nil {
		t.Fatal(err)
	}
	defer lease.Close()
	retained := rows.Row()
	clone, err := rows.Clone()
	if err != nil {
		t.Fatal(err)
	}
	if err := rows.Close(); err != nil {
		t.Fatal(err)
	}
	for _, r := range []DynamicRow{retained, clone} {
		v, err := r.Get("log_entries")
		if err != nil {
			t.Fatal(err)
		}
		it, err := v.Elements()
		if err != nil {
			t.Fatal(err)
		}
		entries, err := Collect(it)
		if err != nil {
			t.Fatal(err)
		}
		if len(entries) != 1 {
			t.Fatalf("expected 1 entry, got %d", len(entries))
		}
		fs, err := entries[0].Fields()
		if err != nil {
			t.Fatal(err)
		}
		fields, err := Collect(fs)
		if err != nil {
			t.Fatal(err)
		}
		if len(fields) != 2 {
			t.Fatalf("expected timestamp and message, got %d fields", len(fields))
		}
		for _, f := range fields {
			switch f.Name {
			case "created_timestamp":
				n, e := f.Value.Signed()
				if e != nil || Timestamp(n) != stamp {
					t.Fatalf("timestamp: %d %v", n, e)
				}
			case "message":
				v, e := f.Value.Text()
				if e != nil || v.String() != want[0].Message {
					t.Fatalf("message: %v %v", v, e)
				}
			default:
				t.Fatalf("unexpected field %q", f.Name)
			}
		}
	}
}
func TestUDT_MissingField(t *testing.T) {
	t.Parallel()
	s := createV2Session(t)
	defer s.Close()
	table, _ := udtTable(t, s, "name text,owner text")
	type col struct{ Name string }
	p := Params(Param("name", serializationString, func(v col) string { return v.Name }))
	r := Row(Column("name", serializationString, func(v *col) *string { return &v.Name }))
	strict := serializationUDT(p, r, UDTOptions{})
	if err := serializationExec(s, fmt.Sprintf("INSERT INTO %s(id,udt_col) VALUES(1,?)", table), OneParam(strict), col{"test"}); err == nil {
		t.Fatal("strict UDT accepted an unaccounted database field")
	}
	codec := serializationUDT(p, r, UDTOptions{AllowExtraDatabaseFields: true})
	want := col{"test"}
	got := serializationRoundTrip(t, s, table, "udt_col", 1, codec, want)
	if got != want {
		t.Fatalf("expected %+v, got %+v", want, got)
	}
}
func TestUDT_EmptyCollections(t *testing.T) {
	t.Parallel()
	s := createV2Session(t)
	defer s.Close()
	table, _ := udtTable(t, s, "a list<text>,b map<text,text>,c set<text>")
	type udt struct {
		A Nullable[[]string]
		B Nullable[map[string]string]
		C Nullable[[]string]
	}
	list, mp, set := NullableCodec(serializationList(serializationString)), NullableCodec(serializationMap(serializationString, serializationString)), NullableCodec(serializationSet(serializationString))
	codec := serializationUDT(Params(Param("a", list, func(v udt) Nullable[[]string] { return v.A }), Param("b", mp, func(v udt) Nullable[map[string]string] { return v.B }), Param("c", set, func(v udt) Nullable[[]string] { return v.C })), Row(Column("a", list, func(v *udt) *Nullable[[]string] { return &v.A }), Column("b", mp, func(v *udt) *Nullable[map[string]string] { return &v.B }), Column("c", set, func(v *udt) *Nullable[[]string] { return &v.C })), UDTOptions{})
	got := serializationRoundTrip(t, s, table, "udt_col", 1, codec, udt{})
	if got.A.Valid || got.B.Valid || got.C.Valid || got.A.Value != nil || got.B.Value != nil || got.C.Value != nil {
		t.Fatalf("null collections: %+v", got)
	}
	got = serializationRoundTrip(t, s, table, "udt_col", 2, codec, udt{A: Nullable[[]string]{Valid: true}, B: Nullable[map[string]string]{Valid: true}, C: Nullable[[]string]{Valid: true}})
	if !got.A.Valid || !got.B.Valid || !got.C.Valid || len(got.A.Value) != 0 || len(got.B.Value) != 0 || len(got.C.Value) != 0 {
		t.Fatalf("present empty collections: %+v", got)
	}
}
func TestUDT_UpdateField(t *testing.T) {
	t.Parallel()
	s := createV2Session(t)
	defer s.Close()
	table, typ := udtTable(t, s, "name text,owner text")
	type col struct {
		Name, Owner string
		Data        Nullable[string]
	}
	p := Params(Param("name", serializationString, func(v col) string { return v.Name }), Param("owner", serializationString, func(v col) string { return v.Owner }), Param("data", NullableCodec(serializationString), func(v col) Nullable[string] { return v.Data }))
	r := Row(Column("name", serializationString, func(v *col) *string { return &v.Name }), Column("owner", serializationString, func(v *col) *string { return &v.Owner }), Column("data", NullableCodec(serializationString), func(v *col) *Nullable[string] { return &v.Data }))
	want := col{Name: "test-name", Owner: "test-owner"}
	if err := serializationExec(s, fmt.Sprintf("INSERT INTO %s(id,udt_col) VALUES(1,?)", table), OneParam(UDTEncoder(p, UDTOptions{})), want); err == nil {
		t.Fatal("unknown application field accepted without AllowMissingFields")
	}
	if err := serializationExec(s, fmt.Sprintf("INSERT INTO %s(id,udt_col) VALUES(1,?)", table), OneParam(UDTEncoder(p, UDTOptions{AllowMissingFields: true})), want); err != nil {
		t.Fatal(err)
	}
	serializationDDL(t, s, fmt.Sprintf("ALTER TYPE %s ADD data text", typ))
	got, err := serializationOne(s, fmt.Sprintf("SELECT udt_col FROM %s WHERE id=1", table), OneColumn(UDTDecoder(r, UDTOptions{})))
	if err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("added trailing field: expected %+v, got %+v", want, got)
	}
}
func TestUDT_ScanNullUDT(t *testing.T) {
	t.Parallel()
	s := createV2Session(t)
	defer s.Close()
	table, _ := udtTable(t, s, "lat int,lon int,padding text")
	serializationWrite(t, s, fmt.Sprintf("INSERT INTO %s(id) VALUES(1)", table))
	got, err := serializationOne(s, fmt.Sprintf("SELECT udt_col FROM %s WHERE id=1", table), OneColumn(NullableDecoder(positionCodec{})))
	if err != nil {
		t.Fatal(err)
	}
	if got.Valid {
		t.Fatal("expected null custom UDT")
	}
}
