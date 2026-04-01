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
	"errors"
	"fmt"
	"testing"
)

func TestErrorsParse(t *testing.T) {
	t.Parallel()

	session := createSession(t)
	defer session.Close()

	table := testTableName(t)

	if err := createTable(session, fmt.Sprintf(`CREATE TABLE gocql_test.%s (id int primary key)`, table)); err != nil {
		t.Fatal("create:", err)
	}

	if err := createTable(session, fmt.Sprintf(`CREATE TABLE gocql_test.%s (id int primary key)`, table)); err == nil {
		t.Fatal("Should have gotten already exists error from cassandra server.")
	} else {
		e := &RequestErrAlreadyExists{}
		if errors.As(err, &e) {
			if e.Table != table {
				t.Fatalf("expected error table to be %q but was %q", table, e.Table)
			}
		} else {
			t.Fatalf("expected to get RequestErrAlreadyExists instead got %T", e)
		}
	}
}
