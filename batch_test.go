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
	"testing"
	"time"
)

func TestBatch_Errors(t *testing.T) {
	session := createSession(t)
	defer session.Close()

	table := testTableName(t)

	if err := createTable(session, fmt.Sprintf(`CREATE TABLE gocql_test.%s (id int primary key, val inet)`, table)); err != nil {
		t.Fatal(err)
	}

	b := session.Batch(LoggedBatch)
	b = b.Query(fmt.Sprintf("SELECT * FROM gocql_test.%s WHERE id=2 AND val=?", table), nil)
	if err := b.Exec(); err == nil {
		t.Fatal("expected to get error for invalid query in batch")
	}
}

func TestBatch_WithTimestamp(t *testing.T) {
	session := createSession(t)
	defer session.Close()

	table := testTableName(t)

	if err := createTable(session, fmt.Sprintf(`CREATE TABLE gocql_test.%s (id int primary key, val text)`, table)); err != nil {
		t.Fatal(err)
	}

	micros := time.Now().UnixNano()/1e3 - 1000

	b := session.Batch(LoggedBatch)
	b.WithTimestamp(micros)
	b = b.Query(fmt.Sprintf("INSERT INTO gocql_test.%s (id, val) VALUES (?, ?)", table), 1, "val")
	b = b.Query(fmt.Sprintf("INSERT INTO gocql_test.%s (id, val) VALUES (?, ?)", table), 2, "val")

	if err := b.Exec(); err != nil {
		t.Fatal(err)
	}

	var storedTs int64
	if err := session.Query(fmt.Sprintf(`SELECT writetime(val) FROM gocql_test.%s WHERE id = ?`, table), 1).Scan(&storedTs); err != nil {
		t.Fatal(err)
	}

	if storedTs != micros {
		t.Errorf("got ts %d, expected %d", storedTs, micros)
	}
}
