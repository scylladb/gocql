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
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
)

func TestProto1BatchInsert(t *testing.T) {
	t.Parallel()

	session := createV2Session(t)
	defer session.Close()

	table := testTableName(t)

	if err := v2CreateTable(context.Background(), session, fmt.Sprintf(("CREATE TABLE "+fixtureKeyspace("")+".%s (id int primary key)"), table)); err != nil {
		t.Fatal(err)
	}

	begin := "BEGIN BATCH"
	end := "APPLY BATCH"
	query := fmt.Sprintf("INSERT INTO %s (id) VALUES (?)", table)
	fullQuery := strings.Join([]string{begin, query, end}, "\n")
	bound := sessionTestBind(t, session, fullQuery, IntValue(5))
	defer bound.Close()
	if _, err := Exec(context.Background(), session, bound, Options{Consistency: Option[Consistency]{Value: Quorum, Set: true}}); err != nil {
		t.Fatal(err)
	}
}

func TestShouldPrepareFunction(t *testing.T) {
	t.Parallel()

	var shouldPrepareTests = []struct {
		Stmt   string
		Result bool
	}{
		{`
      BEGIN BATCH
        INSERT INTO users (userID, password)
        VALUES ('smith', 'secret')
      APPLY BATCH
    ;
      `, true},
		{`INSERT INTO users (userID, password, name) VALUES ('user2', 'ch@ngem3b', 'second user')`, true},
		{`BEGIN COUNTER BATCH UPDATE stats SET views = views + 1 WHERE pageid = 1 APPLY BATCH`, true},
		{`delete name from users where userID = 'smith';`, true},
		{`  UPDATE users SET password = 'secret' WHERE userID = 'smith'   `, true},
		{`CREATE TABLE users (
        user_name varchar PRIMARY KEY,
        password varchar,
        gender varchar,
        session_token varchar,
        state varchar,
        birth_year bigint
      );`, false},
	}

	// Automatic keyword-based preparation was removed: every original CQL
	// case can now be bound explicitly as QUERY, and Prepare explicitly asks
	// the server for metadata, including DDL previously classified false.
	session := &Session{logger: nopLogger{}}
	defer session.Close()
	for _, test := range shouldPrepareTests {
		t.Run(fmt.Sprintf("formerly_prepared_%t_%s", test.Result, firstCQLWord(test.Stmt)), func(t *testing.T) {
			statement := Statement[Unit, Unit]{CQL: test.Stmt, Params: NoParams, Rows: NoRows}
			bound, err := BindUnpreparedOwned(statement, NewColumns(), Unit{})
			if err != nil {
				t.Fatal(err)
			}
			bound.Close()
			if _, err := Prepare(context.Background(), session, statement, PrepareOptions{}); !errors.Is(err, ErrNoConnections) {
				t.Fatalf("explicit Prepare must attempt a connection for %q: %v", test.Stmt, err)
			}
		})
	}
}
