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
	"context"
	"testing"
)

func TestGetRoutingKeySkipsDDL(t *testing.T) {
	t.Parallel()

	ddlStatements := []string{
		"CREATE TABLE ks.tbl (id int PRIMARY KEY)",
		"ALTER TABLE ks.tbl ADD col text",
		"DROP TABLE ks.tbl",
		"TRUNCATE ks.tbl",
		"CREATE KEYSPACE ks WITH replication = {'class': 'SimpleStrategy'}",
		"DROP KEYSPACE ks",
		"GRANT SELECT ON ks.tbl TO user1",
		"USE ks",
	}

	for _, stmt := range ddlStatements {
		// session is intentionally nil — if GetRoutingKey tries to call
		// routingKeyInfo it will panic, proving the guard works.
		q := &Query{
			stmt:        stmt,
			routingInfo: &queryRoutingInfo{},
		}
		key, err := q.GetRoutingKey()
		if err != nil {
			t.Errorf("GetRoutingKey(%q) returned error: %v", stmt, err)
		}
		if key != nil {
			t.Errorf("GetRoutingKey(%q) returned non-nil key: %v", stmt, key)
		}
	}
}

func TestGetRoutingKeyDMLNeedsSession(t *testing.T) {
	t.Parallel()

	// DML statements should NOT be short-circuited — they need to go through
	// routingKeyInfo to compute a routing key. With a nil session this will
	// panic, confirming that the guard does not block DML.
	dmlStatements := []string{
		"SELECT * FROM ks.tbl",
		"INSERT INTO ks.tbl (id) VALUES (?)",
		"UPDATE ks.tbl SET col = ? WHERE id = ?",
		"DELETE FROM ks.tbl WHERE id = ?",
	}

	for _, stmt := range dmlStatements {
		q := &Query{
			stmt:        stmt,
			routingInfo: &queryRoutingInfo{},
		}

		func() {
			defer func() {
				if r := recover(); r == nil {
					t.Errorf("GetRoutingKey(%q) did not panic with nil session — shouldPrepare guard may be blocking DML", stmt)
				}
			}()
			q.GetRoutingKey()
		}()
	}
}

func TestAsyncSessionInit(t *testing.T) {
	t.Parallel()

	// Build a 3 node cluster to test host metric mapping
	var addresses = []string{
		"127.0.0.1",
		"127.0.0.2",
		"127.0.0.3",
	}
	// only build 1 of the servers so that we can test not connecting to the last
	// one
	srv := NewTestServerWithAddress(addresses[0]+":0", t, defaultProto, context.Background())
	defer srv.Stop()

	// just choose any port
	cluster := testCluster(defaultProto, srv.Address, addresses[1]+":9999", addresses[2]+":9999")
	cluster.PoolConfig.HostSelectionPolicy = SingleHostReadyPolicy(RoundRobinHostPolicy())
	db, err := cluster.CreateSession()
	if err != nil {
		t.Fatalf("NewCluster: %v", err)
	}
	defer db.Close()

	// make sure the session works
	if err := db.Query("void").Exec(); err != nil {
		t.Fatalf("unexpected error from void")
	}
}
