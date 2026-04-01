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
	Lat     int    `cql:"lat"`
	Lon     int    `cql:"lon"`
	Padding string `json:"padding"`
}

// NOTE: due to current implementation details it is not currently possible to use
// a pointer receiver type for the UDTMarshaler interface to handle UDT's
func (p position) MarshalUDT(name string, info TypeInfo) ([]byte, error) {
	switch name {
	case "lat":
		return Marshal(info, p.Lat)
	case "lon":
		return Marshal(info, p.Lon)
	case "padding":
		return Marshal(info, p.Padding)
	default:
		return nil, fmt.Errorf("unknown column for position: %q", name)
	}
}

func (p *position) UnmarshalUDT(name string, info TypeInfo, data []byte) error {
	switch name {
	case "lat":
		return Unmarshal(info, data, &p.Lat)
	case "lon":
		return Unmarshal(info, data, &p.Lon)
	case "padding":
		return Unmarshal(info, data, &p.Padding)
	default:
		return fmt.Errorf("unknown column for position: %q", name)
	}
}

func TestUDT_Marshaler(t *testing.T) {
	t.Parallel()

	session := createSession(t)
	defer session.Close()

	table := testTableName(t)
	typeName := testTypeName(t)

	err := createTable(session, fmt.Sprintf(`CREATE TYPE gocql_test.%s(
		lat int,
		lon int,
		padding text);`, typeName))
	if err != nil {
		t.Fatal(err)
	}

	err = createTable(session, fmt.Sprintf(`CREATE TABLE gocql_test.%s(
		id int,
		name text,
		loc frozen<%s>,

		primary key(id)
	);`, table, typeName))
	if err != nil {
		t.Fatal(err)
	}

	const (
		expLat = -1
		expLon = 2
	)
	pad := strings.Repeat("X", 1000)

	err = session.Query(fmt.Sprintf("INSERT INTO %s(id, name, loc) VALUES(?, ?, ?)", table), 1, "test", &position{expLat, expLon, pad}).Exec()
	if err != nil {
		t.Fatal(err)
	}

	pos := &position{}

	err = session.Query(fmt.Sprintf("SELECT loc FROM %s WHERE id = ?", table), 1).Scan(pos)
	if err != nil {
		t.Fatal(err)
	}

	if pos.Lat != expLat {
		t.Errorf("expeceted lat to be be %d got %d", expLat, pos.Lat)
	}
	if pos.Lon != expLon {
		t.Errorf("expeceted lon to be be %d got %d", expLon, pos.Lon)
	}
	if pos.Padding != pad {
		t.Errorf("expected to get padding %q got %q\n", pad, pos.Padding)
	}
}

func TestUDT_Reflect(t *testing.T) {
	t.Parallel()

	// Uses reflection instead of implementing the marshaling type
	session := createSession(t)
	defer session.Close()

	table := testTableName(t)
	typeName := testTypeName(t)

	err := createTable(session, fmt.Sprintf(`CREATE TYPE gocql_test.%s(
		name text,
		owner text);`, typeName))
	if err != nil {
		t.Fatal(err)
	}

	err = createTable(session, fmt.Sprintf(`CREATE TABLE gocql_test.%s(
		position int,
		horse frozen<%s>,

		primary key(position)
	);`, table, typeName))
	if err != nil {
		t.Fatal(err)
	}

	type horse struct {
		Name  string `cql:"name"`
		Owner string `cql:"owner"`
	}

	insertedHorse := &horse{
		Name:  "pony",
		Owner: "jim",
	}

	err = session.Query(fmt.Sprintf("INSERT INTO %s(position, horse) VALUES(?, ?)", table), 1, insertedHorse).Exec()
	if err != nil {
		t.Fatal(err)
	}

	retrievedHorse := &horse{}
	err = session.Query(fmt.Sprintf("SELECT horse FROM %s WHERE position = ?", table), 1).Scan(retrievedHorse)
	if err != nil {
		t.Fatal(err)
	}

	if *retrievedHorse != *insertedHorse {
		t.Fatalf("expected to get %+v got %+v", insertedHorse, retrievedHorse)
	}
}

func TestUDT_NullObject(t *testing.T) {
	t.Parallel()

	session := createSession(t)
	defer session.Close()

	table := testTableName(t)
	typeName := testTypeName(t)

	err := createTable(session, fmt.Sprintf(`CREATE TYPE gocql_test.%s(
		name text,
		owner text);`, typeName))
	if err != nil {
		t.Fatal(err)
	}

	err = createTable(session, fmt.Sprintf(`CREATE TABLE gocql_test.%s(
		id uuid,
		udt_col frozen<%s>,

		primary key(id)
	);`, table, typeName))
	if err != nil {
		t.Fatal(err)
	}

	type col struct {
		Name  string `cql:"name"`
		Owner string `cql:"owner"`
	}

	id := TimeUUID()
	err = session.Query(fmt.Sprintf("INSERT INTO %s(id) VALUES(?)", table), id).Exec()
	if err != nil {
		t.Fatal(err)
	}

	readCol := &col{
		Name:  "temp",
		Owner: "temp",
	}

	err = session.Query(fmt.Sprintf("SELECT udt_col FROM %s WHERE id = ?", table), id).Scan(readCol)
	if err != nil {
		t.Fatal(err)
	}

	if readCol.Name != "" {
		t.Errorf("expected empty string to be returned for null udt: got %q", readCol.Name)
	}
	if readCol.Owner != "" {
		t.Errorf("expected empty string to be returned for null udt: got %q", readCol.Owner)
	}
}

func TestMapScanUDT(t *testing.T) {
	t.Parallel()

	session := createSession(t)
	defer session.Close()

	table := testTableName(t)
	typeName := testTypeName(t)

	err := createTable(session, fmt.Sprintf(`CREATE TYPE gocql_test.%s (
		created_timestamp timestamp,
		message text
	);`, typeName))
	if err != nil {
		t.Fatal(err)
	}

	err = createTable(session, fmt.Sprintf(`CREATE TABLE gocql_test.%s (
		id uuid PRIMARY KEY,
		type int,
		log_entries list<frozen <%s>>
	);`, table, typeName))
	if err != nil {
		t.Fatal(err)
	}

	entry := []struct {
		CreatedTimestamp time.Time `cql:"created_timestamp"`
		Message          string    `cql:"message"`
	}{
		{
			CreatedTimestamp: time.Now().Truncate(time.Millisecond),
			Message:          "test time now",
		},
	}

	id, _ := RandomUUID()
	const typ = 1

	err = session.Query(fmt.Sprintf("INSERT INTO %s(id, type, log_entries) VALUES (?, ?, ?)", table), id, typ, entry).Exec()
	if err != nil {
		t.Fatal(err)
	}

	rawResult := map[string]interface{}{}
	err = session.Query(fmt.Sprintf(`SELECT * FROM %s WHERE id = ?`, table), id).MapScan(rawResult)
	if err != nil {
		t.Fatal(err)
	}

	logEntries, ok := rawResult["log_entries"].([]map[string]interface{})
	if !ok {
		t.Fatal("log_entries not in scanned map")
	}

	if len(logEntries) != 1 {
		t.Fatalf("expected to get 1 log_entry got %d", len(logEntries))
	}

	logEntry := logEntries[0]

	timestamp, ok := logEntry["created_timestamp"]
	if !ok {
		t.Error("created_timestamp not unmarshalled into map")
	} else {
		if ts, ok := timestamp.(time.Time); ok {
			if !ts.In(time.UTC).Equal(entry[0].CreatedTimestamp.In(time.UTC)) {
				t.Errorf("created_timestamp not equal to stored: got %v expected %v", ts.In(time.UTC), entry[0].CreatedTimestamp.In(time.UTC))
			}
		} else {
			t.Errorf("created_timestamp was not time.Time got: %T", timestamp)
		}
	}

	message, ok := logEntry["message"]
	if !ok {
		t.Error("message not unmarshalled into map")
	} else {
		if ts, ok := message.(string); ok {
			if ts != message {
				t.Errorf("message not equal to stored: got %v expected %v", ts, entry[0].Message)
			}
		} else {
			t.Errorf("message was not string got: %T", message)
		}
	}
}

func TestUDT_MissingField(t *testing.T) {
	t.Parallel()

	session := createSession(t)
	defer session.Close()

	table := testTableName(t)
	typeName := testTypeName(t)

	err := createTable(session, fmt.Sprintf(`CREATE TYPE gocql_test.%s(
		name text,
		owner text);`, typeName))
	if err != nil {
		t.Fatal(err)
	}

	err = createTable(session, fmt.Sprintf(`CREATE TABLE gocql_test.%s(
		id uuid,
		udt_col frozen<%s>,

		primary key(id)
	);`, table, typeName))
	if err != nil {
		t.Fatal(err)
	}

	type col struct {
		Name string `cql:"name"`
	}

	writeCol := &col{
		Name: "test",
	}

	id := TimeUUID()
	err = session.Query(fmt.Sprintf("INSERT INTO %s(id, udt_col) VALUES(?, ?)", table), id, writeCol).Exec()
	if err != nil {
		t.Fatal(err)
	}

	readCol := &col{}
	err = session.Query(fmt.Sprintf("SELECT udt_col FROM %s WHERE id = ?", table), id).Scan(readCol)
	if err != nil {
		t.Fatal(err)
	}

	if readCol.Name != writeCol.Name {
		t.Errorf("expected %q: got %q", writeCol.Name, readCol.Name)
	}
}

func TestUDT_EmptyCollections(t *testing.T) {
	t.Parallel()

	session := createSession(t)
	defer session.Close()

	table := testTableName(t)
	typeName := testTypeName(t)

	err := createTable(session, fmt.Sprintf(`CREATE TYPE gocql_test.%s(
		a list<text>,
		b map<text, text>,
		c set<text>
	);`, typeName))
	if err != nil {
		t.Fatal(err)
	}

	err = createTable(session, fmt.Sprintf(`CREATE TABLE gocql_test.%s(
		id uuid,
		udt_col frozen<%s>,

		primary key(id)
	);`, table, typeName))
	if err != nil {
		t.Fatal(err)
	}

	type udt struct {
		A []string          `cql:"a"`
		B map[string]string `cql:"b"`
		C []string          `cql:"c"`
	}

	id := TimeUUID()
	err = session.Query(fmt.Sprintf("INSERT INTO %s(id, udt_col) VALUES(?, ?)", table), id, &udt{}).Exec()
	if err != nil {
		t.Fatal(err)
	}

	var val udt
	err = session.Query(fmt.Sprintf("SELECT udt_col FROM %s WHERE id=?", table), id).Scan(&val)
	if err != nil {
		t.Fatal(err)
	}

	if val.A != nil {
		t.Errorf("expected to get nil got %#+v", val.A)
	}
	if val.B != nil {
		t.Errorf("expected to get nil got %#+v", val.B)
	}
	if val.C != nil {
		t.Errorf("expected to get nil got %#+v", val.C)
	}
}

func TestUDT_UpdateField(t *testing.T) {
	t.Parallel()

	session := createSession(t)
	defer session.Close()

	table := testTableName(t)
	typeName := testTypeName(t)

	err := createTable(session, fmt.Sprintf(`CREATE TYPE gocql_test.%s(
		name text,
		owner text);`, typeName))
	if err != nil {
		t.Fatal(err)
	}

	err = createTable(session, fmt.Sprintf(`CREATE TABLE gocql_test.%s(
		id uuid,
		udt_col frozen<%s>,

		primary key(id)
	);`, table, typeName))
	if err != nil {
		t.Fatal(err)
	}

	type col struct {
		Name  string `cql:"name"`
		Owner string `cql:"owner"`
		Data  string `cql:"data"`
	}

	writeCol := &col{
		Name:  "test-name",
		Owner: "test-owner",
	}

	id := TimeUUID()
	err = session.Query(fmt.Sprintf("INSERT INTO %s(id, udt_col) VALUES(?, ?)", table), id, writeCol).Exec()
	if err != nil {
		t.Fatal(err)
	}

	if err := createTable(session, fmt.Sprintf(`ALTER TYPE gocql_test.%s ADD data text;`, typeName)); err != nil {
		t.Fatal(err)
	}

	readCol := &col{}
	err = session.Query(fmt.Sprintf("SELECT udt_col FROM %s WHERE id = ?", table), id).Scan(readCol)
	if err != nil {
		t.Fatal(err)
	}

	if *readCol != *writeCol {
		t.Errorf("expected %+v: got %+v", *writeCol, *readCol)
	}
}

func TestUDT_ScanNullUDT(t *testing.T) {
	t.Parallel()

	session := createSession(t)
	defer session.Close()

	table := testTableName(t)
	typeName := testTypeName(t)

	err := createTable(session, fmt.Sprintf(`CREATE TYPE gocql_test.%s(
		lat int,
		lon int,
		padding text);`, typeName))
	if err != nil {
		t.Fatal(err)
	}

	err = createTable(session, fmt.Sprintf(`CREATE TABLE gocql_test.%s(
		id int,
		name text,
		loc frozen<%s>,
		primary key(id)
	);`, table, typeName))
	if err != nil {
		t.Fatal(err)
	}

	err = session.Query(fmt.Sprintf("INSERT INTO %s(id, name) VALUES(?, ?)", table), 1, "test").Exec()
	if err != nil {
		t.Fatal(err)
	}

	pos := &position{}

	err = session.Query(fmt.Sprintf("SELECT loc FROM %s WHERE id = ?", table), 1).Scan(pos)
	if err != nil {
		t.Fatal(err)
	}
}
