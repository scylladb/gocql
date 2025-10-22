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
	"testing"
	"time"
)

func TestBatch_Errors(t *testing.T) {
	session := createSession(t)
	defer session.Close()

	if err := createTable(session, `CREATE TABLE gocql_test.batch_errors (id int primary key, val inet)`); err != nil {
		t.Fatal(err)
	}

	b := session.Batch(LoggedBatch)
	b = b.Query("SELECT * FROM gocql_test.batch_errors WHERE id=2 AND val=?", nil)
	if err := b.Exec(); err == nil {
		t.Fatal("expected to get error for invalid query in batch")
	}
}

func TestBatch_WithTimestamp(t *testing.T) {
	session := createSession(t)
	defer session.Close()

	if err := createTable(session, `CREATE TABLE gocql_test.batch_ts (id int primary key, val text)`); err != nil {
		t.Fatal(err)
	}

	micros := time.Now().UnixNano()/1e3 - 1000

	b := session.Batch(LoggedBatch)
	b.WithTimestamp(micros)
	b = b.Query("INSERT INTO gocql_test.batch_ts (id, val) VALUES (?, ?)", 1, "val")
	b = b.Query("INSERT INTO gocql_test.batch_ts (id, val) VALUES (?, ?)", 2, "val")

	if err := b.Exec(); err != nil {
		t.Fatal(err)
	}

	var storedTs int64
	if err := session.Query(`SELECT writetime(val) FROM gocql_test.batch_ts WHERE id = ?`, 1).Scan(&storedTs); err != nil {
		t.Fatal(err)
	}

	if storedTs != micros {
		t.Errorf("got ts %d, expected %d", storedTs, micros)
	}
}

func TestBatch_WithServerTimeout(t *testing.T) {
	session := createSession(t)
	defer session.Close()

	if err := createTable(session, `CREATE TABLE gocql_test.batch_server_timeout (id int primary key, val text)`); err != nil {
		t.Fatal(err)
	}

	// Test batch with server timeout
	b := session.Batch(LoggedBatch)
	b.WithServerTimeout(500 * time.Millisecond)
	b = b.Query("INSERT INTO gocql_test.batch_server_timeout (id, val) VALUES (?, ?)", 1, "test1")
	b = b.Query("INSERT INTO gocql_test.batch_server_timeout (id, val) VALUES (?, ?)", 2, "test2")

	if err := b.Exec(); err != nil {
		t.Fatal(err)
	}

	// Verify the data was inserted
	var val string
	if err := session.Query(`SELECT val FROM gocql_test.batch_server_timeout WHERE id = ?`, 1).Scan(&val); err != nil {
		t.Fatal(err)
	}

	if val != "test1" {
		t.Errorf("got val %s, expected test1", val)
	}
}

func TestBatch_WithServerTimeoutAndTimestamp(t *testing.T) {
	session := createSession(t)
	defer session.Close()

	if err := createTable(session, `CREATE TABLE gocql_test.batch_timeout_ts (id int primary key, val text)`); err != nil {
		t.Fatal(err)
	}

	micros := time.Now().UnixNano()/1e3 - 1000

	// Test batch with both server timeout and timestamp
	b := session.Batch(LoggedBatch)
	b.WithServerTimeout(500 * time.Millisecond)
	b.WithTimestamp(micros)
	b = b.Query("INSERT INTO gocql_test.batch_timeout_ts (id, val) VALUES (?, ?)", 1, "val1")
	b = b.Query("INSERT INTO gocql_test.batch_timeout_ts (id, val) VALUES (?, ?)", 2, "val2")

	if err := b.Exec(); err != nil {
		t.Fatal(err)
	}

	// Verify the timestamp was applied
	var storedTs int64
	if err := session.Query(`SELECT writetime(val) FROM gocql_test.batch_timeout_ts WHERE id = ?`, 1).Scan(&storedTs); err != nil {
		t.Fatal(err)
	}

	if storedTs != micros {
		t.Errorf("got ts %d, expected %d", storedTs, micros)
	}
}
