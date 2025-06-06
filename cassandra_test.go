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
	"bytes"
	"context"
	"errors"
	"fmt"
	"github.com/gocql/gocql/internal/tests"
	"math"
	"math/big"
	"net"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"
	"unicode"

	"github.com/stretchr/testify/require"
	"gopkg.in/inf.v0"
)

func TestEmptyHosts(t *testing.T) {
	cluster := createCluster()
	cluster.Hosts = nil
	if session, err := cluster.CreateSession(); err == nil {
		session.Close()
		t.Error("expected err, got nil")
	}
}

func TestInvalidPeerEntry(t *testing.T) {
	t.Skip("dont mutate system tables, rewrite this to test what we mean to test")
	session := createSession(t)

	// rack, release_version, schema_version, tokens are all null
	query := session.Query("INSERT into system.peers (peer, data_center, host_id, rpc_address) VALUES (?, ?, ?, ?)",
		"169.254.235.45",
		"datacenter1",
		"35c0ec48-5109-40fd-9281-9e9d4add2f1e",
		"169.254.235.45",
	)

	if err := query.Exec(); err != nil {
		t.Fatal(err)
	}

	session.Close()

	cluster := createCluster()
	cluster.PoolConfig.HostSelectionPolicy = TokenAwareHostPolicy(RoundRobinHostPolicy())
	session = createSessionFromCluster(cluster, t)
	defer func() {
		session.Query("DELETE from system.peers where peer = ?", "169.254.235.45").Exec()
		session.Close()
	}()

	// check we can perform a query
	iter := session.Query("select peer from system.peers").Iter()
	var peer string
	for iter.Scan(&peer) {
	}
	if err := iter.Close(); err != nil {
		t.Fatal(err)
	}
}

// TestUseStatementError checks to make sure the correct error is returned when the user tries to execute a use statement.
func TestUseStatementError(t *testing.T) {
	session := createSession(t)
	defer session.Close()

	if err := session.Query("USE gocql_test").Exec(); err != nil {
		if err != ErrUseStmt {
			t.Fatalf("expected ErrUseStmt, got " + err.Error())
		}
	} else {
		t.Fatal("expected err, got nil.")
	}
}

// TestInvalidKeyspace checks that an invalid keyspace will return promptly and without a flood of connections
func TestInvalidKeyspace(t *testing.T) {
	cluster := createCluster()
	cluster.Keyspace = "invalidKeyspace"
	session, err := cluster.CreateSession()
	if err != nil {
		if err != ErrNoConnectionsStarted {
			t.Fatalf("Expected ErrNoConnections but got %v", err)
		}
	} else {
		session.Close() //Clean up the session
		t.Fatal("expected err, got nil.")
	}
}

func TestTracing(t *testing.T) {
	session := createSession(t)
	defer session.Close()

	if err := createTable(session, `CREATE TABLE gocql_test.trace (id int primary key)`); err != nil {
		t.Fatal("create:", err)
	}

	buf := &bytes.Buffer{}
	trace := &TraceWriter{session: session, w: buf}
	if err := session.Query(`INSERT INTO trace (id) VALUES (?)`, 42).Trace(trace).Exec(); err != nil {
		t.Fatal("insert:", err)
	} else if buf.Len() == 0 {
		t.Fatal("insert: failed to obtain any tracing")
	}
	trace.mu.Lock()
	buf.Reset()
	trace.mu.Unlock()

	var value int
	if err := session.Query(`SELECT id FROM trace WHERE id = ?`, 42).Trace(trace).Scan(&value); err != nil {
		t.Fatal("select:", err)
	} else if value != 42 {
		t.Fatalf("value: expected %d, got %d", 42, value)
	} else if buf.Len() == 0 {
		t.Fatal("select: failed to obtain any tracing")
	}

	// also works from session tracer
	session.SetTrace(trace)
	trace.mu.Lock()
	buf.Reset()
	trace.mu.Unlock()
	if err := session.Query(`SELECT id FROM trace WHERE id = ?`, 42).Scan(&value); err != nil {
		t.Fatal("select:", err)
	}
	if buf.Len() == 0 {
		t.Fatal("select: failed to obtain any tracing")
	}
}

func TestObserve(t *testing.T) {
	session := createSession(t)
	defer session.Close()

	if err := createTable(session, `CREATE TABLE gocql_test.observe (id int primary key)`); err != nil {
		t.Fatal("create:", err)
	}

	var (
		observedErr      error
		observedKeyspace string
		observedStmt     string
	)

	const keyspace = "gocql_test"

	resetObserved := func() {
		observedErr = errors.New("placeholder only") // used to distinguish err=nil cases
		observedKeyspace = ""
		observedStmt = ""
	}

	observer := funcQueryObserver(func(ctx context.Context, o ObservedQuery) {
		observedKeyspace = o.Keyspace
		observedStmt = o.Statement
		observedErr = o.Err
	})

	// select before inserted, will error but the reporting is err=nil as the query is valid
	resetObserved()
	var value int
	if err := session.Query(`SELECT id FROM observe WHERE id = ?`, 43).Observer(observer).Scan(&value); err == nil {
		t.Fatal("select: expected error")
	} else if observedErr != nil {
		t.Fatalf("select: observed error expected nil, got %q", observedErr)
	} else if observedKeyspace != keyspace {
		t.Fatal("select: unexpected observed keyspace", observedKeyspace)
	} else if observedStmt != `SELECT id FROM observe WHERE id = ?` {
		t.Fatal("select: unexpected observed stmt", observedStmt)
	}

	resetObserved()
	if err := session.Query(`INSERT INTO observe (id) VALUES (?)`, 42).Observer(observer).Exec(); err != nil {
		t.Fatal("insert:", err)
	} else if observedErr != nil {
		t.Fatal("insert:", observedErr)
	} else if observedKeyspace != keyspace {
		t.Fatal("insert: unexpected observed keyspace", observedKeyspace)
	} else if observedStmt != `INSERT INTO observe (id) VALUES (?)` {
		t.Fatal("insert: unexpected observed stmt", observedStmt)
	}

	resetObserved()
	value = 0
	if err := session.Query(`SELECT id FROM observe WHERE id = ?`, 42).Observer(observer).Scan(&value); err != nil {
		t.Fatal("select:", err)
	} else if value != 42 {
		t.Fatalf("value: expected %d, got %d", 42, value)
	} else if observedErr != nil {
		t.Fatal("select:", observedErr)
	} else if observedKeyspace != keyspace {
		t.Fatal("select: unexpected observed keyspace", observedKeyspace)
	} else if observedStmt != `SELECT id FROM observe WHERE id = ?` {
		t.Fatal("select: unexpected observed stmt", observedStmt)
	}

	// also works from session observer
	resetObserved()
	oSession := createSession(t, func(config *ClusterConfig) { config.QueryObserver = observer })
	if err := oSession.Query(`SELECT id FROM observe WHERE id = ?`, 42).Scan(&value); err != nil {
		t.Fatal("select:", err)
	} else if observedErr != nil {
		t.Fatal("select:", err)
	} else if observedKeyspace != keyspace {
		t.Fatal("select: unexpected observed keyspace", observedKeyspace)
	} else if observedStmt != `SELECT id FROM observe WHERE id = ?` {
		t.Fatal("select: unexpected observed stmt", observedStmt)
	}

	// reports errors when the query is poorly formed
	resetObserved()
	value = 0
	if err := session.Query(`SELECT id FROM unknown_table WHERE id = ?`, 42).Observer(observer).Scan(&value); err == nil {
		t.Fatal("select: expecting error")
	} else if observedErr == nil {
		t.Fatal("select: expecting observed error")
	} else if observedKeyspace != keyspace {
		t.Fatal("select: unexpected observed keyspace", observedKeyspace)
	} else if observedStmt != `SELECT id FROM unknown_table WHERE id = ?` {
		t.Fatal("select: unexpected observed stmt", observedStmt)
	}
}

func TestObserve_Pagination(t *testing.T) {
	session := createSession(t)
	defer session.Close()

	if err := createTable(session, `CREATE TABLE gocql_test.observe2 (id int, PRIMARY KEY (id))`); err != nil {
		t.Fatal("create:", err)
	}

	var observedRows int

	resetObserved := func() {
		observedRows = -1
	}

	observer := funcQueryObserver(func(ctx context.Context, o ObservedQuery) {
		observedRows = o.Rows
	})

	// insert 100 entries, relevant for pagination
	for i := 0; i < 50; i++ {
		if err := session.Query(`INSERT INTO observe2 (id) VALUES (?)`, i).Exec(); err != nil {
			t.Fatal("insert:", err)
		}
	}

	resetObserved()

	// read the 100 entries in paginated entries of size 10. Expecting 5 observations, each with 10 rows
	scanner := session.Query(`SELECT id FROM observe2 LIMIT 100`).
		Observer(observer).
		PageSize(10).
		Iter().Scanner()
	for i := 0; i < 50; i++ {
		if !scanner.Next() {
			t.Fatalf("next: should still be true: %d: %v", i, scanner.Err())
		}
		if i%10 == 0 {
			if observedRows != 10 {
				t.Fatalf("next: expecting a paginated query with 10 entries, got: %d (%d)", observedRows, i)
			}
		} else if observedRows != -1 {
			t.Fatalf("next: not expecting paginated query (-1 entries), got: %d", observedRows)
		}

		resetObserved()
	}

	if scanner.Next() {
		t.Fatal("next: no more entries where expected")
	}
}

func TestPaging(t *testing.T) {
	session := createSession(t)
	defer session.Close()

	if err := createTable(session, "CREATE TABLE gocql_test.paging (id int primary key)"); err != nil {
		t.Fatal("create table:", err)
	}
	for i := 0; i < 100; i++ {
		if err := session.Query("INSERT INTO paging (id) VALUES (?)", i).Exec(); err != nil {
			t.Fatal("insert:", err)
		}
	}

	iter := session.Query("SELECT id FROM paging").PageSize(10).Iter()
	var id int
	count := 0
	for iter.Scan(&id) {
		count++
	}
	if err := iter.Close(); err != nil {
		t.Fatal("close:", err)
	}
	if count != 100 {
		t.Fatalf("expected %d, got %d", 100, count)
	}
}

func TestPagingWithAllowFiltering(t *testing.T) {
	session := createSession(t)

	t.Cleanup(func() {
		if err := session.Query("DROP TABLE gocql_test.pagging_with_allow_filtering").Exec(); err != nil {
			t.Fatal("drop table:", err)
		}
		session.Close()
	})

	const (
		targetP1             = 50
		targetP2             = 50
		totalExpectedResults = 30
		pageSize             = 5
		deletedRageStart     = 10
		deletedRageEnd       = 20
		// Some record range is being deleted, to test tombstones appearance
		expectedCount = totalExpectedResults - (deletedRageEnd - deletedRageStart)
	)

	paginatedSelect := fmt.Sprintf("SELECT c1, f1 FROM gocql_test.pagging_with_allow_filtering WHERE p1 = %d AND p2 = %d AND f1 < %d ALLOW FILTERING;", targetP1, targetP2, totalExpectedResults)
	validateResult := func(t *testing.T, results []int) {
		if len(results) != expectedCount {
			t.Fatalf("expected %d got %d: %d", expectedCount, len(results), results)
		}

		sort.Ints(results)

		expect := make([]int, 0, expectedCount)
		for i := 0; i < totalExpectedResults; i++ {
			if i >= deletedRageStart && i < deletedRageEnd {
				continue
			}
			expect = append(expect, i)
		}

		if !reflect.DeepEqual(results, expect) {
			t.Fatalf("expected %v\ngot %v", expect, results)
		}
	}

	t.Run("Prepare", func(t *testing.T) {
		if err := createTable(session,
			"CREATE TABLE gocql_test.pagging_with_allow_filtering (p1 int, p2 int, c1 int, f1 int, "+
				"PRIMARY KEY ((p1, p2), c1)) WITH CLUSTERING ORDER BY (c1 DESC)"); err != nil {
			t.Fatal("create table:", err)
		}

		// Insert extra records
		for i := 0; i < 100; i++ {
			if err := session.Query("INSERT INTO gocql_test.pagging_with_allow_filtering (p1,p2,c1,f1) VALUES (?,?,?,?)", i, i, i, i).Exec(); err != nil {
				t.Fatal("insert:", err)
			}
		}

		// Insert records to a target partition
		for i := 0; i < 100; i++ {
			if err := session.Query("INSERT INTO gocql_test.pagging_with_allow_filtering (p1,p2,c1,f1) VALUES (?,?,?,?)", targetP1, targetP2, i, i).Exec(); err != nil {
				t.Fatal("insert:", err)
			}
		}

		if err := session.Query("DELETE FROM gocql_test.pagging_with_allow_filtering WHERE p1 = ? AND p2 = ? AND c1 >= ? AND c1 < ?", targetP1, targetP2, deletedRageStart, deletedRageEnd).Exec(); err != nil {
			t.Fatal("insert:", err)
		}
	})

	t.Run("AutoPagination", func(t *testing.T) {
		for _, c := range []Consistency{One, Quorum} {
			t.Run(c.String(), func(t *testing.T) {
				iter := session.Query(paginatedSelect).Consistency(c).PageSize(pageSize).Iter()

				var c1, f1 int
				var results []int

				for iter.Scan(&c1, &f1) {
					if c1 != f1 {
						t.Fatalf("expected c1 and f1 values to be the same, but got c1=%d f1=%d", c1, f1)
					}
					results = append(results, f1)
				}
				if err := iter.Close(); err != nil {
					t.Fatal("select:", err.Error())
				}
				validateResult(t, results)
			})
		}
	})

	t.Run("ManualPagination", func(t *testing.T) {
		for _, c := range []Consistency{One, Quorum} {
			t.Run(c.String(), func(t *testing.T) {

				var c1, f1 int
				var results []int
				var currentPageState []byte

				qry := session.Query(paginatedSelect).Consistency(c).PageSize(pageSize)

				for {
					iter := qry.PageState(currentPageState).Iter()

					// Here we make sure that all iterator, but last one have some data in it
					if !iter.LastPage() && iter.NumRows() == 0 {
						t.Errorf("expected at least one row, but got 0")
					}
					for iter.Scan(&c1, &f1) {
						if c1 != f1 {
							t.Fatalf("expected c1 and f1 values to be the same, but got c1=%d f1=%d", c1, f1)
						}
						results = append(results, f1)
					}
					if err := iter.Close(); err != nil {
						t.Fatal("select:", err.Error())
					}
					if iter.LastPage() {
						break
					}
					newPageState := iter.PageState()
					if len(currentPageState) == len(newPageState) && bytes.Compare(newPageState, currentPageState) == 0 {
						t.Fatalf("page state did not change")
					}
					currentPageState = newPageState
				}

				validateResult(t, results)
			})
		}
	})

}

func TestPagingWithBind(t *testing.T) {
	session := createSession(t)
	defer session.Close()

	if err := createTable(session, "CREATE TABLE gocql_test.paging_bind (id int, val int, primary key(id,val))"); err != nil {
		t.Fatal("create table:", err)
	}
	for i := 0; i < 100; i++ {
		if err := session.Query("INSERT INTO paging_bind (id,val) VALUES (?,?)", 1, i).Exec(); err != nil {
			t.Fatal("insert:", err)
		}
	}

	q := session.Query("SELECT val FROM paging_bind WHERE id = ? AND val < ?", 1, 50).PageSize(10)
	iter := q.Iter()
	var id int
	count := 0
	for iter.Scan(&id) {
		count++
	}
	if err := iter.Close(); err != nil {
		t.Fatal("close:", err)
	}
	if count != 50 {
		t.Fatalf("expected %d, got %d", 50, count)
	}

	iter = q.Bind(1, 20).Iter()
	count = 0
	for iter.Scan(&id) {
		count++
	}
	if count != 20 {
		t.Fatalf("expected %d, got %d", 20, count)
	}
	if err := iter.Close(); err != nil {
		t.Fatal("close:", err)
	}
}

func TestCAS(t *testing.T) {
	cluster := createCluster()
	cluster.SerialConsistency = LocalSerial
	session := createSessionFromClusterTabletsDisabled(cluster, t)
	defer session.Close()

	if err := createTable(session, `CREATE TABLE cas_table (
			title         varchar,
			revid   	  timeuuid,
			last_modified timestamp,
			PRIMARY KEY (title, revid)
		)`); err != nil {
		t.Fatal("create:", err)
	}

	title, revid, modified := "baz", TimeUUID(), time.Now()
	var titleCAS string
	var revidCAS UUID
	var modifiedCAS time.Time

	if applied, err := session.Query(`INSERT INTO cas_table (title, revid, last_modified)
		VALUES (?, ?, ?) IF NOT EXISTS`,
		title, revid, modified).ScanCAS(&titleCAS, &revidCAS, &modifiedCAS); err != nil {
		t.Fatal("insert:", err)
	} else if !applied {
		t.Fatal("insert should have been applied")
	}

	if applied, err := session.Query(`INSERT INTO cas_table (title, revid, last_modified)
		VALUES (?, ?, ?) IF NOT EXISTS`,
		title, revid, modified).ScanCAS(&titleCAS, &revidCAS, &modifiedCAS); err != nil {
		t.Fatal("insert:", err)
	} else if applied {
		t.Fatal("insert should not have been applied")
	} else if title != titleCAS || revid != revidCAS {
		t.Fatalf("expected %s/%v/%v but got %s/%v/%v", title, revid, modified, titleCAS, revidCAS, modifiedCAS)
	}

	tenSecondsLater := modified.Add(10 * time.Second)

	if applied, err := session.Query(`DELETE FROM cas_table WHERE title = ? and revid = ? IF last_modified = ?`,
		title, revid, tenSecondsLater).ScanCAS(&modifiedCAS); err != nil {
		t.Fatal("delete:", err)
	} else if applied {
		t.Fatal("delete should have not been applied")
	}

	if modifiedCAS.Unix() != tenSecondsLater.Add(-10*time.Second).Unix() {
		t.Fatalf("Was expecting modified CAS to be %v; but was one second later", modifiedCAS.UTC())
	}

	if _, err := session.Query(`DELETE FROM cas_table WHERE title = ? and revid = ? IF last_modified = ?`,
		title, revid, tenSecondsLater).ScanCAS(); !strings.HasPrefix(err.Error(), "gocql: not enough columns to scan into") {
		t.Fatalf("delete: was expecting count mismatch error but got: %q", err.Error())
	}

	if applied, err := session.Query(`DELETE FROM cas_table WHERE title = ? and revid = ? IF last_modified = ?`,
		title, revid, modified).ScanCAS(&modifiedCAS); err != nil {
		t.Fatal("delete:", err)
	} else if !applied {
		t.Fatal("delete should have been applied")
	}

	if err := session.Query(`TRUNCATE cas_table`).Exec(); err != nil {
		t.Fatal("truncate:", err)
	}

	successBatch := session.Batch(LoggedBatch)
	successBatch.Query("INSERT INTO cas_table (title, revid, last_modified) VALUES (?, ?, ?) IF NOT EXISTS", title, revid, modified)
	if applied, _, err := session.ExecuteBatchCAS(successBatch, &titleCAS, &revidCAS, &modifiedCAS); err != nil {
		t.Fatal("insert:", err)
	} else if !applied {
		t.Fatalf("insert should have been applied: title=%v revID=%v modified=%v", titleCAS, revidCAS, modifiedCAS)
	}

	successBatch = session.Batch(LoggedBatch)
	successBatch.Query("INSERT INTO cas_table (title, revid, last_modified) VALUES (?, ?, ?) IF NOT EXISTS", title+"_foo", revid, modified)
	casMap := make(map[string]interface{})
	if applied, _, err := session.MapExecuteBatchCAS(successBatch, casMap); err != nil {
		t.Fatal("insert:", err)
	} else if !applied {
		t.Fatal("insert should have been applied")
	}

	failBatch := session.Batch(LoggedBatch)
	failBatch.Query("INSERT INTO cas_table (title, revid, last_modified) VALUES (?, ?, ?) IF NOT EXISTS", title, revid, modified)
	if applied, _, err := session.ExecuteBatchCAS(successBatch, &titleCAS, &revidCAS, &modifiedCAS); err != nil {
		t.Fatal("insert:", err)
	} else if applied {
		t.Fatalf("insert should have not been applied: title=%v revID=%v modified=%v", titleCAS, revidCAS, modifiedCAS)
	}

	insertBatch := session.Batch(LoggedBatch)
	insertBatch.Query("INSERT INTO cas_table (title, revid, last_modified) VALUES ('_foo', 2c3af400-73a4-11e5-9381-29463d90c3f0, DATEOF(NOW()))")
	insertBatch.Query("INSERT INTO cas_table (title, revid, last_modified) VALUES ('_foo', 3e4ad2f1-73a4-11e5-9381-29463d90c3f0, DATEOF(NOW()))")
	if err := session.ExecuteBatch(insertBatch); err != nil {
		t.Fatal("insert:", err)
	}

	failBatch = session.Batch(LoggedBatch)
	failBatch.Query("UPDATE cas_table SET last_modified = DATEOF(NOW()) WHERE title='_foo' AND revid=2c3af400-73a4-11e5-9381-29463d90c3f0 IF last_modified=DATEOF(NOW());")
	failBatch.Query("UPDATE cas_table SET last_modified = DATEOF(NOW()) WHERE title='_foo' AND revid=3e4ad2f1-73a4-11e5-9381-29463d90c3f0 IF last_modified=DATEOF(NOW());")
	if applied, iter, err := session.ExecuteBatchCAS(failBatch, &titleCAS, &revidCAS, &modifiedCAS); err != nil {
		t.Fatal("insert:", err)
	} else if applied {
		t.Fatalf("insert should have not been applied: title=%v revID=%v modified=%v", titleCAS, revidCAS, modifiedCAS)
	} else {
		if scan := iter.Scan(&applied, &titleCAS, &revidCAS, &modifiedCAS); scan && applied {
			t.Fatalf("insert should have been applied: title=%v revID=%v modified=%v", titleCAS, revidCAS, modifiedCAS)
		} else if !scan {
			t.Fatal("should have scanned another row")
		}
		if err := iter.Close(); err != nil {
			t.Fatal("scan:", err)
		}
	}

	casMap = make(map[string]interface{})
	if applied, err := session.Query(`SELECT revid FROM cas_table WHERE title = ?`,
		title+"_foo").MapScanCAS(casMap); err != nil {
		t.Fatal("select:", err)
	} else if applied {
		t.Fatal("select shouldn't have returned applied")
	}

	if _, err := session.Query(`SELECT revid FROM cas_table WHERE title = ?`,
		title+"_foo").ScanCAS(&revidCAS); err == nil {
		t.Fatal("select: should have returned an error")
	}

	notCASBatch := session.Batch(LoggedBatch)
	notCASBatch.Query("INSERT INTO cas_table (title, revid, last_modified) VALUES (?, ?, ?)", title+"_baz", revid, modified)
	casMap = make(map[string]interface{})
	if _, _, err := session.MapExecuteBatchCAS(notCASBatch, casMap); err != ErrNotFound {
		t.Fatal("insert should have returned not found:", err)
	}

	notCASBatch = session.Batch(LoggedBatch)
	notCASBatch.Query("INSERT INTO cas_table (title, revid, last_modified) VALUES (?, ?, ?)", title+"_baz", revid, modified)
	casMap = make(map[string]interface{})
	if _, _, err := session.ExecuteBatchCAS(notCASBatch, &revidCAS); err != ErrNotFound {
		t.Fatal("insert should have returned not found:", err)
	}

	failBatch = session.Batch(LoggedBatch)
	failBatch.Query("UPDATE cas_table SET last_modified = DATEOF(NOW()) WHERE title='_foo' AND revid=3e4ad2f1-73a4-11e5-9381-29463d90c3f0 IF last_modified = ?", modified)
	if _, _, err := session.ExecuteBatchCAS(failBatch, new(bool)); err == nil {
		t.Fatal("update should have errored")
	}
	// make sure MapScanCAS does not panic when MapScan fails
	casMap = make(map[string]interface{})
	casMap["last_modified"] = false
	if _, err := session.Query(`UPDATE cas_table SET last_modified = DATEOF(NOW()) WHERE title='_foo' AND revid=3e4ad2f1-73a4-11e5-9381-29463d90c3f0 IF last_modified = ?`,
		modified).MapScanCAS(casMap); err == nil {
		t.Fatal("update should hvae errored", err)
	}

	// make sure MapExecuteBatchCAS does not panic when MapScan fails
	failBatch = session.Batch(LoggedBatch)
	failBatch.Query("UPDATE cas_table SET last_modified = DATEOF(NOW()) WHERE title='_foo' AND revid=3e4ad2f1-73a4-11e5-9381-29463d90c3f0 IF last_modified = ?", modified)
	casMap = make(map[string]interface{})
	casMap["last_modified"] = false
	if _, _, err := session.MapExecuteBatchCAS(failBatch, casMap); err == nil {
		t.Fatal("update should have errored")
	}
}

func TestConsistencySerial(t *testing.T) {
	session := createSession(t)
	defer session.Close()

	type testStruct struct {
		name               string
		id                 int
		consistency        Consistency
		expectedPanicValue string
	}

	testCases := []testStruct{
		{
			name:               "Any",
			consistency:        Any,
			expectedPanicValue: "Serial consistency can only be SERIAL or LOCAL_SERIAL got ANY",
		}, {
			name:               "One",
			consistency:        One,
			expectedPanicValue: "Serial consistency can only be SERIAL or LOCAL_SERIAL got ONE",
		}, {
			name:               "Two",
			consistency:        Two,
			expectedPanicValue: "Serial consistency can only be SERIAL or LOCAL_SERIAL got TWO",
		}, {
			name:               "Three",
			consistency:        Three,
			expectedPanicValue: "Serial consistency can only be SERIAL or LOCAL_SERIAL got THREE",
		}, {
			name:               "Quorum",
			consistency:        Quorum,
			expectedPanicValue: "Serial consistency can only be SERIAL or LOCAL_SERIAL got QUORUM",
		}, {
			name:               "LocalQuorum",
			consistency:        LocalQuorum,
			expectedPanicValue: "Serial consistency can only be SERIAL or LOCAL_SERIAL got LOCAL_QUORUM",
		}, {
			name:               "EachQuorum",
			consistency:        EachQuorum,
			expectedPanicValue: "Serial consistency can only be SERIAL or LOCAL_SERIAL got EACH_QUORUM",
		}, {
			name:               "Serial",
			id:                 8,
			consistency:        Serial,
			expectedPanicValue: "",
		}, {
			name:               "LocalSerial",
			id:                 9,
			consistency:        LocalSerial,
			expectedPanicValue: "",
		}, {
			name:               "LocalOne",
			consistency:        LocalOne,
			expectedPanicValue: "Serial consistency can only be SERIAL or LOCAL_SERIAL got LOCAL_ONE",
		},
	}

	err := session.Query("CREATE TABLE IF NOT EXISTS gocql_test.consistency_serial (id int PRIMARY KEY)").Exec()
	if err != nil {
		t.Fatalf("can't create consistency_serial table:%v", err)
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if tc.expectedPanicValue == "" {
				err = session.Query("INSERT INTO gocql_test.consistency_serial (id) VALUES (?)", tc.id).SerialConsistency(tc.consistency).Exec()
				if err != nil {
					t.Fatal(err)
				}

				var receivedID int
				err = session.Query("SELECT * FROM gocql_test.consistency_serial WHERE id=?", tc.id).Scan(&receivedID)
				if err != nil {
					t.Fatal(err)
				}

				require.Equal(t, tc.id, receivedID)
			} else {
				require.PanicsWithValue(t, tc.expectedPanicValue, func() {
					session.Query("INSERT INTO gocql_test.consistency_serial (id) VALUES (?)", tc.id).SerialConsistency(tc.consistency)
				})
			}
		})
	}
}

func TestDurationType(t *testing.T) {
	session := createSession(t)
	defer session.Close()

	if session.cfg.ProtoVersion < protoVersion5 {
		t.Skip("Duration type is not supported. Please use protocol version >= 4 and cassandra version >= 3.11")
	}

	if err := createTable(session, `CREATE TABLE gocql_test.duration_table (
		k int primary key, v duration
	)`); err != nil {
		t.Fatal("create:", err)
	}

	durations := []Duration{
		Duration{
			Months:      250,
			Days:        500,
			Nanoseconds: 300010001,
		},
		Duration{
			Months:      -250,
			Days:        -500,
			Nanoseconds: -300010001,
		},
		Duration{
			Months:      0,
			Days:        128,
			Nanoseconds: 127,
		},
		Duration{
			Months:      0x7FFFFFFF,
			Days:        0x7FFFFFFF,
			Nanoseconds: 0x7FFFFFFFFFFFFFFF,
		},
	}
	for _, durationSend := range durations {
		if err := session.Query(`INSERT INTO gocql_test.duration_table (k, v) VALUES (1, ?)`, durationSend).Exec(); err != nil {
			t.Fatal(err)
		}

		var id int
		var duration Duration
		if err := session.Query(`SELECT k, v FROM gocql_test.duration_table`).Scan(&id, &duration); err != nil {
			t.Fatal(err)
		}
		if duration.Months != durationSend.Months || duration.Days != durationSend.Days || duration.Nanoseconds != durationSend.Nanoseconds {
			t.Fatalf("Unexpeted value returned, expected=%v, received=%v", durationSend, duration)
		}
	}
}

func TestMapScanCAS(t *testing.T) {
	session := createSessionFromClusterTabletsDisabled(createCluster(), t)
	defer session.Close()

	if err := createTable(session, `CREATE TABLE cas_table2 (
			title         varchar,
			revid   	  timeuuid,
			last_modified timestamp,
			deleted boolean,
			PRIMARY KEY (title, revid)
		)`); err != nil {
		t.Fatal("create:", err)
	}

	title, revid, modified, deleted := "baz", TimeUUID(), time.Now(), false
	mapCAS := map[string]interface{}{}

	if applied, err := session.Query(`INSERT INTO cas_table2 (title, revid, last_modified, deleted)
		VALUES (?, ?, ?, ?) IF NOT EXISTS`,
		title, revid, modified, deleted).MapScanCAS(mapCAS); err != nil {
		t.Fatal("insert:", err)
	} else if !applied {
		t.Fatalf("insert should have been applied: title=%v revID=%v modified=%v", title, revid, modified)
	}

	mapCAS = map[string]interface{}{}
	if applied, err := session.Query(`INSERT INTO cas_table2 (title, revid, last_modified, deleted)
		VALUES (?, ?, ?, ?) IF NOT EXISTS`,
		title, revid, modified, deleted).MapScanCAS(mapCAS); err != nil {
		t.Fatal("insert:", err)
	} else if applied {
		t.Fatalf("insert should have been applied: title=%v revID=%v modified=%v", title, revid, modified)
	} else if title != mapCAS["title"] || revid != mapCAS["revid"] || deleted != mapCAS["deleted"] {
		t.Fatalf("expected %s/%v/%v/%v but got %s/%v/%v%v", title, revid, modified, false, mapCAS["title"], mapCAS["revid"], mapCAS["last_modified"], mapCAS["deleted"])
	}

}

func TestBatch(t *testing.T) {
	session := createSession(t)
	defer session.Close()

	if err := createTable(session, `CREATE TABLE gocql_test.batch_table (id int primary key)`); err != nil {
		t.Fatal("create table:", err)
	}

	batch := session.Batch(LoggedBatch)
	for i := 0; i < 100; i++ {
		batch.Query(`INSERT INTO batch_table (id) VALUES (?)`, i)
	}

	if err := session.ExecuteBatch(batch); err != nil {
		t.Fatal("execute batch:", err)
	}

	count := 0
	if err := session.Query(`SELECT COUNT(*) FROM batch_table`).Scan(&count); err != nil {
		t.Fatal("select count:", err)
	} else if count != 100 {
		t.Fatalf("count: expected %d, got %d\n", 100, count)
	}
}

func TestUnpreparedBatch(t *testing.T) {
	t.Skip("FLAKE skipping")
	session := createSession(t)
	defer session.Close()

	if err := createTable(session, `CREATE TABLE gocql_test.batch_unprepared (id int primary key, c counter)`); err != nil {
		t.Fatal("create table:", err)
	}

	batch := session.Batch(UnloggedBatch)

	for i := 0; i < 100; i++ {
		batch.Query(`UPDATE batch_unprepared SET c = c + 1 WHERE id = 1`)
	}

	if err := session.ExecuteBatch(batch); err != nil {
		t.Fatal("execute batch:", err)
	}

	count := 0
	if err := session.Query(`SELECT COUNT(*) FROM batch_unprepared`).Scan(&count); err != nil {
		t.Fatal("select count:", err)
	} else if count != 1 {
		t.Fatalf("count: expected %d, got %d\n", 100, count)
	}

	if err := session.Query(`SELECT c FROM batch_unprepared`).Scan(&count); err != nil {
		t.Fatal("select count:", err)
	} else if count != 100 {
		t.Fatalf("count: expected %d, got %d\n", 100, count)
	}
}

// TestBatchLimit tests gocql to make sure batch operations larger than the maximum
// statement limit are not submitted to a cassandra node.
func TestBatchLimit(t *testing.T) {
	session := createSession(t)
	defer session.Close()

	if err := createTable(session, `CREATE TABLE gocql_test.batch_table2 (id int primary key)`); err != nil {
		t.Fatal("create table:", err)
	}

	batch := session.Batch(LoggedBatch)
	for i := 0; i < 65537; i++ {
		batch.Query(`INSERT INTO batch_table2 (id) VALUES (?)`, i)
	}
	if err := session.ExecuteBatch(batch); err != ErrTooManyStmts {
		t.Fatal("gocql attempted to execute a batch larger than the support limit of statements.")
	}

}

func TestWhereIn(t *testing.T) {
	session := createSession(t)
	defer session.Close()

	if err := createTable(session, `CREATE TABLE gocql_test.where_in_table (id int, cluster int, primary key (id,cluster))`); err != nil {
		t.Fatal("create table:", err)
	}

	if err := session.Query("INSERT INTO where_in_table (id, cluster) VALUES (?,?)", 100, 200).Exec(); err != nil {
		t.Fatal("insert:", err)
	}

	iter := session.Query("SELECT * FROM where_in_table WHERE id = ? AND cluster IN (?)", 100, 200).Iter()
	var id, cluster int
	count := 0
	for iter.Scan(&id, &cluster) {
		count++
	}

	if id != 100 || cluster != 200 {
		t.Fatalf("Was expecting id and cluster to be (100,200) but were (%d,%d)", id, cluster)
	}
}

// TestTooManyQueryArgs tests to make sure the library correctly handles the application level bug
// whereby too many query arguments are passed to a query
func TestTooManyQueryArgs(t *testing.T) {
	session := createSession(t)
	defer session.Close()

	if err := createTable(session, `CREATE TABLE gocql_test.too_many_query_args (id int primary key, value int)`); err != nil {
		t.Fatal("create table:", err)
	}

	_, err := session.Query(`SELECT * FROM too_many_query_args WHERE id = ?`, 1, 2).Iter().SliceMap()

	if err == nil {
		t.Fatal("'`SELECT * FROM too_many_query_args WHERE id = ?`, 1, 2' should return an error")
	}

	batch := session.Batch(UnloggedBatch)
	batch.Query("INSERT INTO too_many_query_args (id, value) VALUES (?, ?)", 1, 2, 3)
	err = session.ExecuteBatch(batch)

	if err == nil {
		t.Fatal("'`INSERT INTO too_many_query_args (id, value) VALUES (?, ?)`, 1, 2, 3' should return an error")
	}

	// TODO: should indicate via an error code that it is an invalid arg?

}

// TestNotEnoughQueryArgs tests to make sure the library correctly handles the application level bug
// whereby not enough query arguments are passed to a query
func TestNotEnoughQueryArgs(t *testing.T) {
	session := createSession(t)
	defer session.Close()

	if err := createTable(session, `CREATE TABLE gocql_test.not_enough_query_args (id int, cluster int, value int, primary key (id, cluster))`); err != nil {
		t.Fatal("create table:", err)
	}

	_, err := session.Query(`SELECT * FROM not_enough_query_args WHERE id = ? and cluster = ?`, 1).Iter().SliceMap()

	if err == nil {
		t.Fatal("'`SELECT * FROM not_enough_query_args WHERE id = ? and cluster = ?`, 1' should return an error")
	}

	batch := session.Batch(UnloggedBatch)
	batch.Query("INSERT INTO not_enough_query_args (id, cluster, value) VALUES (?, ?, ?)", 1, 2)
	err = session.ExecuteBatch(batch)

	if err == nil {
		t.Fatal("'`INSERT INTO not_enough_query_args (id, cluster, value) VALUES (?, ?, ?)`, 1, 2' should return an error")
	}
}

// TestCreateSessionTimeout tests to make sure the CreateSession function timeouts out correctly
// and prevents an infinite loop of connection retries.
func TestCreateSessionTimeout(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		select {
		case <-time.After(2 * time.Second):
			t.Error("no startup timeout")
		case <-ctx.Done():
		}
	}()

	cluster := createCluster()
	cluster.Hosts = []string{"127.0.0.1:1"}
	session, err := cluster.CreateSession()
	if err == nil {
		session.Close()
		t.Fatal("expected ErrNoConnectionsStarted, but no error was returned.")
	}
}

func TestReconnection(t *testing.T) {
	cluster := createCluster()
	cluster.ReconnectInterval = 1 * time.Second
	session := createSessionFromCluster(cluster, t)
	defer session.Close()

	h := session.hostSource.getHostsList()[0]
	session.handleNodeDown(h.ConnectAddress(), h.Port())

	if h.State() != NodeDown {
		t.Fatal("Host should be NodeDown but not.")
	}

	time.Sleep(cluster.ReconnectInterval + h.Version().nodeUpDelay() + 1*time.Second)

	if h.State() != NodeUp {
		t.Fatal("Host should be NodeUp but not. Failed to reconnect.")
	}
}

type FullName struct {
	FirstName string
	LastName  string
}

func (n FullName) MarshalCQL(info TypeInfo) ([]byte, error) {
	return []byte(n.FirstName + " " + n.LastName), nil
}

func (n *FullName) UnmarshalCQL(info TypeInfo, data []byte) error {
	t := strings.SplitN(string(data), " ", 2)
	n.FirstName, n.LastName = t[0], t[1]
	return nil
}

func TestMapScanWithRefMap(t *testing.T) {
	session := createSession(t)
	defer session.Close()
	if err := createTable(session, `CREATE TABLE gocql_test.scan_map_ref_table (
			testtext       text PRIMARY KEY,
			testfullname   text,
			testint        int,
		)`); err != nil {
		t.Fatal("create table:", err)
	}
	m := make(map[string]interface{})
	m["testtext"] = "testtext"
	m["testfullname"] = FullName{"John", "Doe"}
	m["testint"] = 100

	if err := session.Query(`INSERT INTO scan_map_ref_table (testtext, testfullname, testint) values (?,?,?)`,
		m["testtext"], m["testfullname"], m["testint"]).Exec(); err != nil {
		t.Fatal("insert:", err)
	}

	var testText string
	var testFullName FullName
	ret := map[string]interface{}{
		"testtext":     &testText,
		"testfullname": &testFullName,
		// testint is not set here.
	}
	iter := session.Query(`SELECT * FROM scan_map_ref_table`).Iter()
	if ok := iter.MapScan(ret); !ok {
		t.Fatal("select:", iter.Close())
	} else {
		if ret["testtext"] != "testtext" {
			t.Fatal("returned testtext did not match")
		}
		f := ret["testfullname"].(FullName)
		if f.FirstName != "John" || f.LastName != "Doe" {
			t.Fatal("returned testfullname did not match")
		}
		if ret["testint"] != 100 {
			t.Fatal("returned testinit did not match")
		}
	}
	if testText != "testtext" {
		t.Fatal("returned testtext did not match")
	}
	if testFullName.FirstName != "John" || testFullName.LastName != "Doe" {
		t.Fatal("returned testfullname did not match")
	}

	// using MapScan to read a nil int value
	intp := new(int64)
	ret = map[string]interface{}{
		"testint": &intp,
	}
	if err := session.Query("INSERT INTO scan_map_ref_table(testtext, testint) VALUES(?, ?)", "null-int", nil).Exec(); err != nil {
		t.Fatal(err)
	}
	err := session.Query(`SELECT testint FROM scan_map_ref_table WHERE testtext = ?`, "null-int").MapScan(ret)
	if err != nil {
		t.Fatal(err)
	} else if v := ret["testint"].(*int64); v != nil {
		t.Fatalf("testint should be nil got %+#v", v)
	}

}

func TestMapScan(t *testing.T) {
	session := createSession(t)
	defer session.Close()
	if err := createTable(session, `CREATE TABLE gocql_test.scan_map_table (
			fullname       text PRIMARY KEY,
			age            int,
			address        inet,
			data           blob,
		)`); err != nil {
		t.Fatal("create table:", err)
	}

	if err := session.Query(`INSERT INTO scan_map_table (fullname, age, address) values (?,?,?)`,
		"Grace Hopper", 31, net.ParseIP("10.0.0.1")).Exec(); err != nil {
		t.Fatal("insert:", err)
	}
	if err := session.Query(`INSERT INTO scan_map_table (fullname, age, address, data) values (?,?,?,?)`,
		"Ada Lovelace", 30, net.ParseIP("10.0.0.2"), []byte(`{"foo": "bar"}`)).Exec(); err != nil {
		t.Fatal("insert:", err)
	}

	iter := session.Query(`SELECT * FROM scan_map_table`).Iter()

	// First iteration
	row := make(map[string]interface{})
	if !iter.MapScan(row) {
		t.Fatal("select:", iter.Close())
	}
	tests.AssertEqual(t, "fullname", "Ada Lovelace", row["fullname"])
	tests.AssertEqual(t, "age", 30, row["age"])
	tests.AssertEqual(t, "address", "10.0.0.2", row["address"])
	tests.AssertDeepEqual(t, "data", []byte(`{"foo": "bar"}`), row["data"])

	// Second iteration using a new map
	row = make(map[string]interface{})
	if !iter.MapScan(row) {
		t.Fatal("select:", iter.Close())
	}
	tests.AssertEqual(t, "fullname", "Grace Hopper", row["fullname"])
	tests.AssertEqual(t, "age", 31, row["age"])
	tests.AssertEqual(t, "address", "10.0.0.1", row["address"])
	tests.AssertDeepEqual(t, "data", []byte(nil), row["data"])
}

func TestSliceMap(t *testing.T) {
	session := createSession(t)
	defer session.Close()
	if err := createTable(session, `CREATE TABLE gocql_test.slice_map_table (
			testuuid       timeuuid PRIMARY KEY,
			testtimestamp  timestamp,
			testvarchar    varchar,
			testbigint     bigint,
			testblob       blob,
			testbool       boolean,
			testfloat      float,
			testdouble     double,
			testint        int,
			testdecimal    decimal,
			testlist       list<text>,
			testset        set<int>,
			testmap        map<varchar, varchar>,
			testvarint     varint,
			testinet			 inet
		)`); err != nil {
		t.Fatal("create table:", err)
	}
	m := make(map[string]interface{})

	bigInt := new(big.Int)
	if _, ok := bigInt.SetString("830169365738487321165427203929228", 10); !ok {
		t.Fatal("Failed setting bigint by string")
	}

	m["testuuid"] = TimeUUID()
	m["testvarchar"] = "Test VarChar"
	m["testbigint"] = time.Now().Unix()
	m["testtimestamp"] = time.Now().Truncate(time.Millisecond).UTC()
	m["testblob"] = []byte("test blob")
	m["testbool"] = true
	m["testfloat"] = float32(4.564)
	m["testdouble"] = float64(4.815162342)
	m["testint"] = 2343
	m["testdecimal"] = inf.NewDec(100, 0)
	m["testlist"] = []string{"quux", "foo", "bar", "baz", "quux"}
	m["testset"] = []int{1, 2, 3, 4, 5, 6, 7, 8, 9}
	m["testmap"] = map[string]string{"field1": "val1", "field2": "val2", "field3": "val3"}
	m["testvarint"] = bigInt
	m["testinet"] = "213.212.2.19"
	sliceMap := []map[string]interface{}{m}
	if err := session.Query(`INSERT INTO slice_map_table (testuuid, testtimestamp, testvarchar, testbigint, testblob, testbool, testfloat, testdouble, testint, testdecimal, testlist, testset, testmap, testvarint, testinet) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		m["testuuid"], m["testtimestamp"], m["testvarchar"], m["testbigint"], m["testblob"], m["testbool"], m["testfloat"], m["testdouble"], m["testint"], m["testdecimal"], m["testlist"], m["testset"], m["testmap"], m["testvarint"], m["testinet"]).Exec(); err != nil {
		t.Fatal("insert:", err)
	}
	if returned, retErr := session.Query(`SELECT * FROM slice_map_table`).Iter().SliceMap(); retErr != nil {
		t.Fatal("select:", retErr)
	} else {
		matchSliceMap(t, sliceMap, returned[0])
	}

	// Test for Iter.MapScan()
	{
		testMap := make(map[string]interface{})
		if !session.Query(`SELECT * FROM slice_map_table`).Iter().MapScan(testMap) {
			t.Fatal("MapScan failed to work with one row")
		}
		matchSliceMap(t, sliceMap, testMap)
	}

	// Test for Query.MapScan()
	{
		testMap := make(map[string]interface{})
		if session.Query(`SELECT * FROM slice_map_table`).MapScan(testMap) != nil {
			t.Fatal("MapScan failed to work with one row")
		}
		matchSliceMap(t, sliceMap, testMap)
	}
}
func matchSliceMap(t *testing.T, sliceMap []map[string]interface{}, testMap map[string]interface{}) {
	if sliceMap[0]["testuuid"] != testMap["testuuid"] {
		t.Fatal("returned testuuid did not match")
	}
	if sliceMap[0]["testtimestamp"] != testMap["testtimestamp"] {
		t.Fatal("returned testtimestamp did not match")
	}
	if sliceMap[0]["testvarchar"] != testMap["testvarchar"] {
		t.Fatal("returned testvarchar did not match")
	}
	if sliceMap[0]["testbigint"] != testMap["testbigint"] {
		t.Fatal("returned testbigint did not match")
	}
	if !reflect.DeepEqual(sliceMap[0]["testblob"], testMap["testblob"]) {
		t.Fatal("returned testblob did not match")
	}
	if sliceMap[0]["testbool"] != testMap["testbool"] {
		t.Fatal("returned testbool did not match")
	}
	if sliceMap[0]["testfloat"] != testMap["testfloat"] {
		t.Fatal("returned testfloat did not match")
	}
	if sliceMap[0]["testdouble"] != testMap["testdouble"] {
		t.Fatal("returned testdouble did not match")
	}
	if sliceMap[0]["testinet"] != testMap["testinet"] {
		t.Fatal("returned testinet did not match")
	}

	expectedDecimal := sliceMap[0]["testdecimal"].(*inf.Dec)
	returnedDecimal := testMap["testdecimal"].(*inf.Dec)

	if expectedDecimal.Cmp(returnedDecimal) != 0 {
		t.Fatal("returned testdecimal did not match")
	}

	if !reflect.DeepEqual(sliceMap[0]["testlist"], testMap["testlist"]) {
		t.Fatal("returned testlist did not match")
	}
	if !reflect.DeepEqual(sliceMap[0]["testset"], testMap["testset"]) {
		t.Fatal("returned testset did not match")
	}
	if !reflect.DeepEqual(sliceMap[0]["testmap"], testMap["testmap"]) {
		t.Fatal("returned testmap did not match")
	}
	if sliceMap[0]["testint"] != testMap["testint"] {
		t.Fatal("returned testint did not match")
	}
}

type MyRetryPolicy struct {
}

func (*MyRetryPolicy) Attempt(q RetryableQuery) bool {
	if q.Attempts() > 5 {
		return false
	}
	return true
}

func (*MyRetryPolicy) GetRetryType(err error) RetryType {
	var executedErr *QueryError
	if errors.As(err, &executedErr) && !executedErr.IsIdempotent() {
		return Ignore
	}
	return Retry
}

func Test_RetryPolicyIdempotence(t *testing.T) {
	session := createSession(t)
	defer session.Close()

	testCases := []struct {
		name                  string
		idempotency           bool
		expectedNumberOfTries int
	}{
		{
			name:                  "with retry",
			idempotency:           true,
			expectedNumberOfTries: 6,
		},
		{
			name:                  "without retry",
			idempotency:           false,
			expectedNumberOfTries: 1,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			q := session.Query("INSERT INTO  gocql_test.not_existing_table(event_id, time, args) VALUES (?,?,?)", 4, UUIDFromTime(time.Now()), "test")

			q.Idempotent(tc.idempotency)
			q.RetryPolicy(&MyRetryPolicy{})
			q.Consistency(All)

			_ = q.Exec()
			require.Equal(t, tc.expectedNumberOfTries, q.Attempts())
		})
	}
}

func TestSmallInt(t *testing.T) {
	session := createSession(t)
	defer session.Close()

	if err := createTable(session, `CREATE TABLE gocql_test.smallint_table (
			testsmallint  smallint PRIMARY KEY,
		)`); err != nil {
		t.Fatal("create table:", err)
	}
	m := make(map[string]interface{})
	m["testsmallint"] = int16(2)
	sliceMap := []map[string]interface{}{m}
	if err := session.Query(`INSERT INTO smallint_table (testsmallint) VALUES (?)`,
		m["testsmallint"]).Exec(); err != nil {
		t.Fatal("insert:", err)
	}
	if returned, retErr := session.Query(`SELECT * FROM smallint_table`).Iter().SliceMap(); retErr != nil {
		t.Fatal("select:", retErr)
	} else {
		if sliceMap[0]["testsmallint"] != returned[0]["testsmallint"] {
			t.Fatal("returned testsmallint did not match")
		}
	}
}

func TestScanWithNilArguments(t *testing.T) {
	session := createSession(t)
	defer session.Close()

	if err := createTable(session, `CREATE TABLE gocql_test.scan_with_nil_arguments (
			foo   varchar,
			bar   int,
			PRIMARY KEY (foo, bar)
	)`); err != nil {
		t.Fatal("create:", err)
	}
	for i := 1; i <= 20; i++ {
		if err := session.Query("INSERT INTO scan_with_nil_arguments (foo, bar) VALUES (?, ?)",
			"squares", i*i).Exec(); err != nil {
			t.Fatal("insert:", err)
		}
	}

	iter := session.Query("SELECT * FROM scan_with_nil_arguments WHERE foo = ?", "squares").Iter()
	var n int
	count := 0
	for iter.Scan(nil, &n) {
		count += n
	}
	if err := iter.Close(); err != nil {
		t.Fatal("close:", err)
	}
	if count != 2870 {
		t.Fatalf("expected %d, got %d", 2870, count)
	}
}

func TestScanCASWithNilArguments(t *testing.T) {
	session := createSessionFromClusterTabletsDisabled(createCluster(), t)
	defer session.Close()

	if err := createTable(session, `CREATE TABLE scan_cas_with_nil_arguments (
		foo   varchar,
		bar   varchar,
		PRIMARY KEY (foo, bar)
	)`); err != nil {
		t.Fatal("create:", err)
	}

	foo := "baz"
	var cas string

	if applied, err := session.Query(`INSERT INTO scan_cas_with_nil_arguments (foo, bar)
		VALUES (?, ?) IF NOT EXISTS`,
		foo, foo).ScanCAS(nil, nil); err != nil {
		t.Fatal("insert:", err)
	} else if !applied {
		t.Fatal("insert should have been applied")
	}

	if applied, err := session.Query(`INSERT INTO scan_cas_with_nil_arguments (foo, bar)
		VALUES (?, ?) IF NOT EXISTS`,
		foo, foo).ScanCAS(&cas, nil); err != nil {
		t.Fatal("insert:", err)
	} else if applied {
		t.Fatal("insert should not have been applied")
	} else if foo != cas {
		t.Fatalf("expected %v but got %v", foo, cas)
	}

	if applied, err := session.Query(`INSERT INTO scan_cas_with_nil_arguments (foo, bar)
		VALUES (?, ?) IF NOT EXISTS`,
		foo, foo).ScanCAS(nil, &cas); err != nil {
		t.Fatal("insert:", err)
	} else if applied {
		t.Fatal("insert should not have been applied")
	} else if foo != cas {
		t.Fatalf("expected %v but got %v", foo, cas)
	}
}

func TestRebindQueryInfo(t *testing.T) {
	session := createSession(t)
	defer session.Close()

	if err := createTable(session, "CREATE TABLE gocql_test.rebind_query (id int, value text, PRIMARY KEY (id))"); err != nil {
		t.Fatalf("failed to create table with error '%v'", err)
	}

	if err := session.Query("INSERT INTO rebind_query (id, value) VALUES (?, ?)", 23, "quux").Exec(); err != nil {
		t.Fatalf("insert into rebind_query failed, err '%v'", err)
	}

	if err := session.Query("INSERT INTO rebind_query (id, value) VALUES (?, ?)", 24, "w00t").Exec(); err != nil {
		t.Fatalf("insert into rebind_query failed, err '%v'", err)
	}

	q := session.Query("SELECT value FROM rebind_query WHERE ID = ?")
	q.Bind(23)

	iter := q.Iter()
	var value string
	for iter.Scan(&value) {
	}

	if value != "quux" {
		t.Fatalf("expected %v but got %v", "quux", value)
	}

	q.Bind(24)
	iter = q.Iter()

	for iter.Scan(&value) {
	}

	if value != "w00t" {
		t.Fatalf("expected %v but got %v", "w00t", value)
	}
}

// TestStaticQueryInfo makes sure that the application can manually bind query parameters using the simplest possible static binding strategy
func TestStaticQueryInfo(t *testing.T) {
	session := createSession(t)
	defer session.Close()

	if err := createTable(session, "CREATE TABLE gocql_test.static_query_info (id int, value text, PRIMARY KEY (id))"); err != nil {
		t.Fatalf("failed to create table with error '%v'", err)
	}

	if err := session.Query("INSERT INTO static_query_info (id, value) VALUES (?, ?)", 113, "foo").Exec(); err != nil {
		t.Fatalf("insert into static_query_info failed, err '%v'", err)
	}

	autobinder := func(q *QueryInfo) ([]interface{}, error) {
		values := make([]interface{}, 1)
		values[0] = 113
		return values, nil
	}

	qry := session.Bind("SELECT id, value FROM static_query_info WHERE id = ?", autobinder)

	if err := qry.Exec(); err != nil {
		t.Fatalf("expose query info failed, error '%v'", err)
	}

	iter := qry.Iter()

	var id int
	var value string

	iter.Scan(&id, &value)

	if err := iter.Close(); err != nil {
		t.Fatalf("query with exposed info failed, err '%v'", err)
	}

	if value != "foo" {
		t.Fatalf("Expected value %s, but got %s", "foo", value)
	}

}

type ClusteredKeyValue struct {
	Id      int
	Cluster int
	Value   string
}

func (kv *ClusteredKeyValue) Bind(q *QueryInfo) ([]interface{}, error) {
	values := make([]interface{}, len(q.Args))

	for i, info := range q.Args {
		fieldName := upcaseInitial(info.Name)
		value := reflect.ValueOf(kv)
		field := reflect.Indirect(value).FieldByName(fieldName)
		values[i] = field.Addr().Interface()
	}

	return values, nil
}

func upcaseInitial(str string) string {
	for i, v := range str {
		return string(unicode.ToUpper(v)) + str[i+1:]
	}
	return ""
}

// TestBoundQueryInfo makes sure that the application can manually bind query parameters using the query meta data supplied at runtime
func TestBoundQueryInfo(t *testing.T) {

	session := createSession(t)
	defer session.Close()

	if err := createTable(session, "CREATE TABLE gocql_test.clustered_query_info (id int, cluster int, value text, PRIMARY KEY (id, cluster))"); err != nil {
		t.Fatalf("failed to create table with error '%v'", err)
	}

	write := &ClusteredKeyValue{Id: 200, Cluster: 300, Value: "baz"}

	insert := session.Bind("INSERT INTO clustered_query_info (id, cluster, value) VALUES (?, ?,?)", write.Bind)

	if err := insert.Exec(); err != nil {
		t.Fatalf("insert into clustered_query_info failed, err '%v'", err)
	}

	read := &ClusteredKeyValue{Id: 200, Cluster: 300}

	qry := session.Bind("SELECT id, cluster, value FROM clustered_query_info WHERE id = ? and cluster = ?", read.Bind)

	iter := qry.Iter()

	var id, cluster int
	var value string

	iter.Scan(&id, &cluster, &value)

	if err := iter.Close(); err != nil {
		t.Fatalf("query with clustered_query_info info failed, err '%v'", err)
	}

	if value != "baz" {
		t.Fatalf("Expected value %s, but got %s", "baz", value)
	}

}

// TestBatchQueryInfo makes sure that the application can manually bind query parameters when executing in a batch
func TestBatchQueryInfo(t *testing.T) {
	session := createSession(t)
	defer session.Close()

	if err := createTable(session, "CREATE TABLE gocql_test.batch_query_info (id int, cluster int, value text, PRIMARY KEY (id, cluster))"); err != nil {
		t.Fatalf("failed to create table with error '%v'", err)
	}

	write := func(q *QueryInfo) ([]interface{}, error) {
		values := make([]interface{}, 3)
		values[0] = 4000
		values[1] = 5000
		values[2] = "bar"
		return values, nil
	}

	batch := session.Batch(LoggedBatch)
	batch.Bind("INSERT INTO batch_query_info (id, cluster, value) VALUES (?, ?,?)", write)

	if err := session.ExecuteBatch(batch); err != nil {
		t.Fatalf("batch insert into batch_query_info failed, err '%v'", err)
	}

	read := func(q *QueryInfo) ([]interface{}, error) {
		values := make([]interface{}, 2)
		values[0] = 4000
		values[1] = 5000
		return values, nil
	}

	qry := session.Bind("SELECT id, cluster, value FROM batch_query_info WHERE id = ? and cluster = ?", read)

	iter := qry.Iter()

	var id, cluster int
	var value string

	iter.Scan(&id, &cluster, &value)

	if err := iter.Close(); err != nil {
		t.Fatalf("query with batch_query_info info failed, err '%v'", err)
	}

	if value != "bar" {
		t.Fatalf("Expected value %s, but got %s", "bar", value)
	}
}

func getRandomConn(t *testing.T, session *Session) *Conn {
	conn := session.getConn()
	if conn == nil {
		t.Fatal("unable to get a connection")
	}
	return conn
}

func injectInvalidPreparedStatement(t *testing.T, session *Session, table string) (string, *Conn) {
	if err := createTable(session, `CREATE TABLE gocql_test.`+table+` (
			foo   varchar,
			bar   int,
			PRIMARY KEY (foo, bar)
	)`); err != nil {
		t.Fatal("create:", err)
	}

	stmt := "INSERT INTO " + table + " (foo, bar) VALUES (?, 7)"

	conn := getRandomConn(t, session)

	flight := new(inflightPrepare)
	key := session.stmtsLRU.keyFor(conn.host.HostID(), "", stmt)
	session.stmtsLRU.add(key, flight)

	flight.preparedStatment = &preparedStatment{
		id: []byte{'f', 'o', 'o', 'b', 'a', 'r'},
		request: preparedMetadata{
			resultMetadata: resultMetadata{
				colCount:       1,
				actualColCount: 1,
				columns: []ColumnInfo{
					{
						Keyspace: "gocql_test",
						Table:    table,
						Name:     "foo",
						TypeInfo: NativeType{
							typ: TypeVarchar,
						},
					},
				},
			},
		},
	}

	return stmt, conn
}

func TestPrepare_MissingSchemaPrepare(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s := createSession(t)
	conn := getRandomConn(t, s)
	defer s.Close()

	insertQry := s.Query("INSERT INTO invalidschemaprep (val) VALUES (?)", 5)
	if err := conn.executeQuery(ctx, insertQry).err; err == nil {
		t.Fatal("expected error, but got nil.")
	}

	if err := createTable(s, "CREATE TABLE gocql_test.invalidschemaprep (val int, PRIMARY KEY (val))"); err != nil {
		t.Fatal("create table:", err)
	}

	if err := conn.executeQuery(ctx, insertQry).err; err != nil {
		t.Fatal(err) // unconfigured columnfamily
	}
}

func TestPrepare_ReprepareStatement(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	session := createSession(t)
	defer session.Close()

	stmt, conn := injectInvalidPreparedStatement(t, session, "test_reprepare_statement")
	query := session.Query(stmt, "bar")
	if err := conn.executeQuery(ctx, query).Close(); err != nil {
		t.Fatalf("Failed to execute query for reprepare statement: %v", err)
	}
}

func TestPrepare_ReprepareBatch(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	session := createSession(t)
	defer session.Close()

	stmt, conn := injectInvalidPreparedStatement(t, session, "test_reprepare_statement_batch")
	batch := session.Batch(UnloggedBatch)
	batch.Query(stmt, "bar")
	if err := conn.executeBatch(ctx, batch).Close(); err != nil {
		t.Fatalf("Failed to execute query for reprepare statement: %v", err)
	}
}

func TestQueryInfo(t *testing.T) {
	session := createSession(t)
	defer session.Close()

	conn := getRandomConn(t, session)
	info, err := conn.prepareStatement(context.Background(), "SELECT release_version, host_id FROM system.local WHERE key = ?", nil)

	if err != nil {
		t.Fatalf("Failed to execute query for preparing statement: %v", err)
	}

	if x := len(info.request.columns); x != 1 {
		t.Fatalf("Was not expecting meta data for %d query arguments, but got %d\n", 1, x)
	}

	if x := len(info.response.columns); x != 2 {
		t.Fatalf("Was not expecting meta data for %d result columns, but got %d\n", 2, x)
	}
}

// TestPreparedCacheEviction will make sure that the cache size is maintained
func TestPrepare_PreparedCacheEviction(t *testing.T) {
	const maxPrepared = 4

	clusterHosts := getClusterHosts()
	host := clusterHosts[0]
	cluster := createCluster()
	cluster.MaxPreparedStmts = maxPrepared
	cluster.Events.DisableSchemaEvents = true
	cluster.Hosts = []string{host}

	cluster.HostFilter = WhiteListHostFilter(host)

	session := createSessionFromCluster(cluster, t)
	defer session.Close()

	if err := createTable(session, "CREATE TABLE gocql_test.prepcachetest (id int,mod int,PRIMARY KEY (id))"); err != nil {
		t.Fatalf("failed to create table with error '%v'", err)
	}
	// clear the cache
	session.stmtsLRU.clear()

	//Fill the table
	for i := 0; i < 2; i++ {
		if err := session.Query("INSERT INTO prepcachetest (id,mod) VALUES (?, ?)", i, 10000%(i+1)).Exec(); err != nil {
			t.Fatalf("insert into prepcachetest failed, err '%v'", err)
		}
	}
	//Populate the prepared statement cache with select statements
	var id, mod int
	for i := 0; i < 2; i++ {
		err := session.Query("SELECT id,mod FROM prepcachetest WHERE id = "+strconv.FormatInt(int64(i), 10)).Scan(&id, &mod)
		if err != nil {
			t.Fatalf("select from prepcachetest failed, error '%v'", err)
		}
	}

	//generate an update statement to test they are prepared
	err := session.Query("UPDATE prepcachetest SET mod = ? WHERE id = ?", 1, 11).Exec()
	if err != nil {
		t.Fatalf("update prepcachetest failed, error '%v'", err)
	}

	//generate a delete statement to test they are prepared
	err = session.Query("DELETE FROM prepcachetest WHERE id = ?", 1).Exec()
	if err != nil {
		t.Fatalf("delete from prepcachetest failed, error '%v'", err)
	}

	//generate an insert statement to test they are prepared
	err = session.Query("INSERT INTO prepcachetest (id,mod) VALUES (?, ?)", 3, 11).Exec()
	if err != nil {
		t.Fatalf("insert into prepcachetest failed, error '%v'", err)
	}

	session.stmtsLRU.mu.Lock()
	defer session.stmtsLRU.mu.Unlock()

	//Make sure the cache size is maintained
	if session.stmtsLRU.lru.Len() != session.stmtsLRU.lru.MaxEntries {
		t.Fatalf("expected cache size of %v, got %v", session.stmtsLRU.lru.MaxEntries, session.stmtsLRU.lru.Len())
	}

	// Walk through all the configured hosts and test cache retention and eviction
	for _, host := range session.hostSource.hosts {
		_, ok := session.stmtsLRU.lru.Get(session.stmtsLRU.keyFor(host.HostID(), session.cfg.Keyspace, "SELECT id,mod FROM prepcachetest WHERE id = 0"))
		if ok {
			t.Errorf("expected first select to be purged but was in cache for host=%q", host)
		}

		_, ok = session.stmtsLRU.lru.Get(session.stmtsLRU.keyFor(host.HostID(), session.cfg.Keyspace, "SELECT id,mod FROM prepcachetest WHERE id = 1"))
		if !ok {
			t.Errorf("exepected second select to be in cache for host=%q", host)
		}

		_, ok = session.stmtsLRU.lru.Get(session.stmtsLRU.keyFor(host.HostID(), session.cfg.Keyspace, "INSERT INTO prepcachetest (id,mod) VALUES (?, ?)"))
		if !ok {
			t.Errorf("expected insert to be in cache for host=%q", host)
		}

		_, ok = session.stmtsLRU.lru.Get(session.stmtsLRU.keyFor(host.HostID(), session.cfg.Keyspace, "UPDATE prepcachetest SET mod = ? WHERE id = ?"))
		if !ok {
			t.Errorf("expected update to be in cached for host=%q", host)
		}

		_, ok = session.stmtsLRU.lru.Get(session.stmtsLRU.keyFor(host.HostID(), session.cfg.Keyspace, "DELETE FROM prepcachetest WHERE id = ?"))
		if !ok {
			t.Errorf("expected delete to be cached for host=%q", host)
		}
	}
}

func TestPrepare_PreparedCacheKey(t *testing.T) {
	session := createSession(t)
	defer session.Close()

	// create a second keyspace
	cluster2 := createCluster()
	createKeyspace(t, cluster2, "gocql_test2", false)
	cluster2.Keyspace = "gocql_test2"
	session2, err := cluster2.CreateSession()
	if err != nil {
		t.Fatal("create session:", err)
	}
	defer session2.Close()

	// both keyspaces have a table named "test_stmt_cache_key"
	if err := createTable(session, "CREATE TABLE gocql_test.test_stmt_cache_key (id varchar primary key, field varchar)"); err != nil {
		t.Fatal("create table:", err)
	}
	if err := createTable(session2, "CREATE TABLE gocql_test2.test_stmt_cache_key (id varchar primary key, field varchar)"); err != nil {
		t.Fatal("create table:", err)
	}

	// both tables have a single row with the same partition key but different column value
	if err = session.Query(`INSERT INTO test_stmt_cache_key (id, field) VALUES (?, ?)`, "key", "one").Exec(); err != nil {
		t.Fatal("insert:", err)
	}
	if err = session2.Query(`INSERT INTO test_stmt_cache_key (id, field) VALUES (?, ?)`, "key", "two").Exec(); err != nil {
		t.Fatal("insert:", err)
	}

	// should be able to see different values in each keyspace
	var value string
	if err = session.Query("SELECT field FROM test_stmt_cache_key WHERE id = ?", "key").Scan(&value); err != nil {
		t.Fatal("select:", err)
	}
	if value != "one" {
		t.Errorf("Expected one, got %s", value)
	}

	if err = session2.Query("SELECT field FROM test_stmt_cache_key WHERE id = ?", "key").Scan(&value); err != nil {
		t.Fatal("select:", err)
	}
	if value != "two" {
		t.Errorf("Expected two, got %s", value)
	}
}

// TestMarshalFloat64Ptr tests to see that a pointer to a float64 is marshalled correctly.
func TestMarshalFloat64Ptr(t *testing.T) {
	session := createSession(t)
	defer session.Close()

	if err := createTable(session, "CREATE TABLE gocql_test.float_test (id double, test double, primary key (id))"); err != nil {
		t.Fatal("create table:", err)
	}
	testNum := float64(7500)
	if err := session.Query(`INSERT INTO float_test (id,test) VALUES (?,?)`, float64(7500.00), &testNum).Exec(); err != nil {
		t.Fatal("insert float64:", err)
	}
}

// TestMarshalInet tests to see that a pointer to a float64 is marshalled correctly.
func TestMarshalInet(t *testing.T) {
	session := createSession(t)
	defer session.Close()

	if err := createTable(session, "CREATE TABLE gocql_test.inet_test (ip inet, name text, primary key (ip))"); err != nil {
		t.Fatal("create table:", err)
	}
	stringIp := "123.34.45.56"
	if err := session.Query(`INSERT INTO inet_test (ip,name) VALUES (?,?)`, stringIp, "Test IP 1").Exec(); err != nil {
		t.Fatal("insert string inet:", err)
	}
	var stringResult string
	if err := session.Query("SELECT ip FROM inet_test").Scan(&stringResult); err != nil {
		t.Fatalf("select for string from inet_test 1 failed: %v", err)
	}
	if stringResult != stringIp {
		t.Errorf("Expected %s, was %s", stringIp, stringResult)
	}

	var ipResult net.IP
	if err := session.Query("SELECT ip FROM inet_test").Scan(&ipResult); err != nil {
		t.Fatalf("select for net.IP from inet_test 1 failed: %v", err)
	}
	if ipResult.String() != stringIp {
		t.Errorf("Expected %s, was %s", stringIp, ipResult.String())
	}

	if err := session.Query(`DELETE FROM inet_test WHERE ip = ?`, stringIp).Exec(); err != nil {
		t.Fatal("delete inet table:", err)
	}

	netIp := net.ParseIP("222.43.54.65")
	if err := session.Query(`INSERT INTO inet_test (ip,name) VALUES (?,?)`, netIp, "Test IP 2").Exec(); err != nil {
		t.Fatal("insert netIp inet:", err)
	}

	if err := session.Query("SELECT ip FROM inet_test").Scan(&stringResult); err != nil {
		t.Fatalf("select for string from inet_test 2 failed: %v", err)
	}
	if stringResult != netIp.String() {
		t.Errorf("Expected %s, was %s", netIp.String(), stringResult)
	}
	if err := session.Query("SELECT ip FROM inet_test").Scan(&ipResult); err != nil {
		t.Fatalf("select for net.IP from inet_test 2 failed: %v", err)
	}
	if ipResult.String() != netIp.String() {
		t.Errorf("Expected %s, was %s", netIp.String(), ipResult.String())
	}

}

func TestVarint(t *testing.T) {
	session := createSession(t)
	defer session.Close()

	if err := createTable(session, "CREATE TABLE gocql_test.varint_test (id varchar, test varint, test2 varint, primary key (id))"); err != nil {
		t.Fatalf("failed to create table with error '%v'", err)
	}

	if err := session.Query(`INSERT INTO varint_test (id, test) VALUES (?, ?)`, "id", 0).Exec(); err != nil {
		t.Fatalf("insert varint: %v", err)
	}

	var result int
	if err := session.Query("SELECT test FROM varint_test").Scan(&result); err != nil {
		t.Fatalf("select from varint_test failed: %v", err)
	}

	if result != 0 {
		t.Errorf("Expected 0, was %d", result)
	}

	if err := session.Query(`INSERT INTO varint_test (id, test) VALUES (?, ?)`, "id", -1).Exec(); err != nil {
		t.Fatalf("insert varint: %v", err)
	}

	if err := session.Query("SELECT test FROM varint_test").Scan(&result); err != nil {
		t.Fatalf("select from varint_test failed: %v", err)
	}

	if result != -1 {
		t.Errorf("Expected -1, was %d", result)
	}

	if err := session.Query(`INSERT INTO varint_test (id, test) VALUES (?, ?)`, "id", nil).Exec(); err != nil {
		t.Fatalf("insert varint: %v", err)
	}

	if err := session.Query("SELECT test FROM varint_test").Scan(&result); err != nil {
		t.Fatalf("select from varint_test failed: %v", err)
	}

	if result != 0 {
		t.Errorf("Expected 0, was %d", result)
	}

	var nullableResult *int

	if err := session.Query("SELECT test FROM varint_test").Scan(&nullableResult); err != nil {
		t.Fatalf("select from varint_test failed: %v", err)
	}

	if nullableResult != nil {
		t.Errorf("Expected nil, was %d", nullableResult)
	}

	if err := session.Query(`INSERT INTO varint_test (id, test) VALUES (?, ?)`, "id", int64(math.MaxInt32)+1).Exec(); err != nil {
		t.Fatalf("insert varint: %v", err)
	}

	var result64 int64
	if err := session.Query("SELECT test FROM varint_test").Scan(&result64); err != nil {
		t.Fatalf("select from varint_test failed: %v", err)
	}

	if result64 != int64(math.MaxInt32)+1 {
		t.Errorf("Expected %d, was %d", int64(math.MaxInt32)+1, result64)
	}

	biggie := new(big.Int)
	biggie.SetString("36893488147419103232", 10) // > 2**64
	if err := session.Query(`INSERT INTO varint_test (id, test) VALUES (?, ?)`, "id", biggie).Exec(); err != nil {
		t.Fatalf("insert varint: %v", err)
	}

	resultBig := new(big.Int)
	if err := session.Query("SELECT test FROM varint_test").Scan(resultBig); err != nil {
		t.Fatalf("select from varint_test failed: %v", err)
	}

	if resultBig.String() != biggie.String() {
		t.Errorf("Expected %s, was %s", biggie.String(), resultBig.String())
	}

	err := session.Query("SELECT test FROM varint_test").Scan(&result64)
	if err == nil || strings.Index(err.Error(), "the data value should be in the int64 range") == -1 {
		t.Errorf("expected out of range error since value is too big for int64, result:%d", result64)
	}

	// value not set in cassandra, leave bind variable empty
	resultBig = new(big.Int)
	if err := session.Query("SELECT test2 FROM varint_test").Scan(resultBig); err != nil {
		t.Fatalf("select from varint_test failed: %v", err)
	}

	if resultBig.Int64() != 0 {
		t.Errorf("Expected %s, was %s", biggie.String(), resultBig.String())
	}

	// can use double pointer to explicitly detect value is not set in cassandra
	if err := session.Query("SELECT test2 FROM varint_test").Scan(&resultBig); err != nil {
		t.Fatalf("select from varint_test failed: %v", err)
	}

	if resultBig != nil {
		t.Errorf("Expected %v, was %v", nil, *resultBig)
	}
}

// TestQueryStats confirms that the stats are returning valid data. Accuracy may be questionable.
func TestQueryStats(t *testing.T) {
	session := createSession(t)
	defer session.Close()
	qry := session.Query("SELECT * FROM system.peers")
	if err := qry.Exec(); err != nil {
		t.Fatalf("query failed. %v", err)
	} else {
		if qry.Attempts() < 1 {
			t.Fatal("expected at least 1 attempt, but got 0")
		}
		if qry.Latency() <= 0 {
			t.Fatalf("expected latency to be greater than 0, but got %v instead.", qry.Latency())
		}
	}
}

// TestIterHosts confirms that host is added to Iter when the query succeeds.
func TestIterHost(t *testing.T) {
	session := createSession(t)
	defer session.Close()
	iter := session.Query("SELECT * FROM system.peers").Iter()

	// check if Host method works
	if iter.Host() == nil {
		t.Error("No host in iter")
	}
}

// TestBatchStats confirms that the stats are returning valid data. Accuracy may be questionable.
func TestBatchStats(t *testing.T) {
	session := createSession(t)
	defer session.Close()

	if err := createTable(session, "CREATE TABLE gocql_test.batchStats (id int, PRIMARY KEY (id))"); err != nil {
		t.Fatalf("failed to create table with error '%v'", err)
	}

	b := session.Batch(LoggedBatch)
	b.Query("INSERT INTO batchStats (id) VALUES (?)", 1)
	b.Query("INSERT INTO batchStats (id) VALUES (?)", 2)

	if err := session.ExecuteBatch(b); err != nil {
		t.Fatalf("query failed. %v", err)
	} else {
		if b.Attempts() < 1 {
			t.Fatal("expected at least 1 attempt, but got 0")
		}
		if b.Latency() <= 0 {
			t.Fatalf("expected latency to be greater than 0, but got %v instead.", b.Latency())
		}
	}
}

type funcBatchObserver func(context.Context, ObservedBatch)

func (f funcBatchObserver) ObserveBatch(ctx context.Context, o ObservedBatch) {
	f(ctx, o)
}

func TestBatchObserve(t *testing.T) {
	session := createSession(t)
	defer session.Close()

	if err := createTable(session, `CREATE TABLE gocql_test.batch_observe_table (id int, other int, PRIMARY KEY (id))`); err != nil {
		t.Fatal("create table:", err)
	}

	type observation struct {
		observedErr      error
		observedKeyspace string
		observedStmts    []string
		observedValues   [][]interface{}
	}

	var observedBatch *observation

	batch := session.Batch(LoggedBatch)
	batch.Observer(funcBatchObserver(func(ctx context.Context, o ObservedBatch) {
		if observedBatch != nil {
			t.Fatal("batch observe called more than once")
		}

		observedBatch = &observation{
			observedKeyspace: o.Keyspace,
			observedStmts:    o.Statements,
			observedErr:      o.Err,
			observedValues:   o.Values,
		}
	}))
	for i := 0; i < 100; i++ {
		// hard coding 'i' into one of the values for better  testing of observation
		batch.Query(fmt.Sprintf(`INSERT INTO batch_observe_table (id,other) VALUES (?,%d)`, i), i)
	}

	if err := session.ExecuteBatch(batch); err != nil {
		t.Fatal("execute batch:", err)
	}
	if observedBatch == nil {
		t.Fatal("batch observation has not been called")
	}
	if len(observedBatch.observedStmts) != 100 {
		t.Fatal("expecting 100 observed statements, got", len(observedBatch.observedStmts))
	}
	if observedBatch.observedErr != nil {
		t.Fatal("not expecting to observe an error", observedBatch.observedErr)
	}
	if observedBatch.observedKeyspace != "gocql_test" {
		t.Fatalf("expecting keyspace 'gocql_test', got %q", observedBatch.observedKeyspace)
	}
	for i, stmt := range observedBatch.observedStmts {
		if stmt != fmt.Sprintf(`INSERT INTO batch_observe_table (id,other) VALUES (?,%d)`, i) {
			t.Fatal("unexpected query", stmt)
		}

		tests.AssertDeepEqual(t, "observed value", []interface{}{i}, observedBatch.observedValues[i])
	}
}

// TestNilInQuery tests to see that a nil value passed to a query is handled by Cassandra
// TODO validate the nil value by reading back the nil. Need to fix Unmarshalling.
func TestNilInQuery(t *testing.T) {
	session := createSession(t)
	defer session.Close()

	if err := createTable(session, "CREATE TABLE gocql_test.testNilInsert (id int, count int, PRIMARY KEY (id))"); err != nil {
		t.Fatalf("failed to create table with error '%v'", err)
	}
	if err := session.Query("INSERT INTO testNilInsert (id,count) VALUES (?,?)", 1, nil).Exec(); err != nil {
		t.Fatalf("failed to insert with err: %v", err)
	}

	var id int

	if err := session.Query("SELECT id FROM testNilInsert").Scan(&id); err != nil {
		t.Fatalf("failed to select with err: %v", err)
	} else if id != 1 {
		t.Fatalf("expected id to be 1, got %v", id)
	}
}

// Don't initialize time.Time bind variable if cassandra timestamp column is empty
func TestEmptyTimestamp(t *testing.T) {
	session := createSession(t)

	defer session.Close()

	if err := createTable(session, "CREATE TABLE gocql_test.test_empty_timestamp (id int, time timestamp, num int, PRIMARY KEY (id))"); err != nil {
		t.Fatalf("failed to create table with error '%v'", err)
	}

	if err := session.Query("INSERT INTO test_empty_timestamp (id, num) VALUES (?,?)", 1, 561).Exec(); err != nil {
		t.Fatalf("failed to insert with err: %v", err)
	}

	var timeVal time.Time

	if err := session.Query("SELECT time FROM test_empty_timestamp where id = ?", 1).Scan(&timeVal); err != nil {
		t.Fatalf("failed to select with err: %v", err)
	}

	if !timeVal.IsZero() {
		t.Errorf("time.Time bind variable should be zero (was %s)", timeVal)
	}
}

// Integration test of just querying for data from the system.schema_keyspace table where the keyspace DOES exist.
func TestGetKeyspaceMetadata(t *testing.T) {
	session := createSession(t)
	defer session.Close()

	keyspaceMetadata, err := getKeyspaceMetadata(session, "gocql_test")
	if err != nil {
		t.Fatalf("failed to query the keyspace metadata with err: %v", err)
	}
	if keyspaceMetadata == nil {
		t.Fatal("failed to query the keyspace metadata, nil returned")
	}
	if keyspaceMetadata.Name != "gocql_test" {
		t.Errorf("Expected keyspace name to be 'gocql' but was '%s'", keyspaceMetadata.Name)
	}
	if keyspaceMetadata.StrategyClass != "org.apache.cassandra.locator.NetworkTopologyStrategy" {
		t.Errorf("Expected replication strategy class to be 'org.apache.cassandra.locator.NetworkTopologyStrategy' but was '%s'", keyspaceMetadata.StrategyClass)
	}
	if keyspaceMetadata.StrategyOptions == nil {
		t.Error("Expected replication strategy options map but was nil")
	}
	rfStr, ok := keyspaceMetadata.StrategyOptions["datacenter1"]
	if !ok {
		t.Fatalf("Expected strategy option 'datacenter1' but was not found in %v", keyspaceMetadata.StrategyOptions)
	}
	rfInt, err := strconv.Atoi(rfStr.(string))
	if err != nil {
		t.Fatalf("Error converting string to int with err: %v", err)
	}
	if rfInt != *flagRF {
		t.Errorf("Expected replication factor to be %d but was %d", *flagRF, rfInt)
	}
}

// Integration test of just querying for data from the system.schema_keyspace table where the keyspace DOES NOT exist.
func TestGetKeyspaceMetadataFails(t *testing.T) {
	session := createSession(t)
	defer session.Close()

	_, err := getKeyspaceMetadata(session, "gocql_keyspace_does_not_exist")

	if err != ErrKeyspaceDoesNotExist || err == nil {
		t.Fatalf("Expected error of type ErrKeySpaceDoesNotExist. Instead, error was %v", err)
	}
}

// Integration test of the routing key calculation
func TestRoutingKey(t *testing.T) {
	session := createSession(t)
	defer session.Close()

	if err := createTable(session, "CREATE TABLE gocql_test.test_single_routing_key (first_id int, second_id int, PRIMARY KEY (first_id, second_id))"); err != nil {
		t.Fatalf("failed to create table with error '%v'", err)
	}
	if err := createTable(session, "CREATE TABLE gocql_test.test_composite_routing_key (first_id int, second_id int, PRIMARY KEY ((first_id, second_id)))"); err != nil {
		t.Fatalf("failed to create table with error '%v'", err)
	}

	initCacheSize := session.routingKeyInfoCache.lru.Len()

	routingKeyInfo, err := session.routingKeyInfo(context.Background(), "SELECT * FROM test_single_routing_key WHERE second_id=? AND first_id=?")
	if err != nil {
		t.Fatalf("failed to get routing key info due to error: %v", err)
	}
	if routingKeyInfo == nil {
		t.Fatal("Expected routing key info, but was nil")
	}
	if len(routingKeyInfo.indexes) != 1 {
		t.Fatalf("Expected routing key indexes length to be 1 but was %d", len(routingKeyInfo.indexes))
	}
	if routingKeyInfo.indexes[0] != 1 {
		t.Errorf("Expected routing key index[0] to be 1 but was %d", routingKeyInfo.indexes[0])
	}
	if len(routingKeyInfo.types) != 1 {
		t.Fatalf("Expected routing key types length to be 1 but was %d", len(routingKeyInfo.types))
	}
	if routingKeyInfo.types[0] == nil {
		t.Fatal("Expected routing key types[0] to be non-nil")
	}
	if routingKeyInfo.types[0].Type() != TypeInt {
		t.Fatalf("Expected routing key types[0].Type to be %v but was %v", TypeInt, routingKeyInfo.types[0].Type())
	}

	// verify the cache is working
	routingKeyInfo, err = session.routingKeyInfo(context.Background(), "SELECT * FROM test_single_routing_key WHERE second_id=? AND first_id=?")
	if err != nil {
		t.Fatalf("failed to get routing key info due to error: %v", err)
	}
	if len(routingKeyInfo.indexes) != 1 {
		t.Fatalf("Expected routing key indexes length to be 1 but was %d", len(routingKeyInfo.indexes))
	}
	if routingKeyInfo.indexes[0] != 1 {
		t.Errorf("Expected routing key index[0] to be 1 but was %d", routingKeyInfo.indexes[0])
	}
	if len(routingKeyInfo.types) != 1 {
		t.Fatalf("Expected routing key types length to be 1 but was %d", len(routingKeyInfo.types))
	}
	if routingKeyInfo.types[0] == nil {
		t.Fatal("Expected routing key types[0] to be non-nil")
	}
	if routingKeyInfo.types[0].Type() != TypeInt {
		t.Fatalf("Expected routing key types[0] to be %v but was %v", TypeInt, routingKeyInfo.types[0].Type())
	}
	cacheSize := session.routingKeyInfoCache.lru.Len()
	if cacheSize != initCacheSize+1 {
		t.Errorf("Expected cache size to be %d but was %d", initCacheSize+1, cacheSize)
	}

	query := session.Query("SELECT * FROM test_single_routing_key WHERE second_id=? AND first_id=?", 1, 2)
	routingKey, err := query.GetRoutingKey()
	if err != nil {
		t.Fatalf("Failed to get routing key due to error: %v", err)
	}
	expectedRoutingKey := []byte{0, 0, 0, 2}
	if !reflect.DeepEqual(expectedRoutingKey, routingKey) {
		t.Errorf("Expected routing key %v but was %v", expectedRoutingKey, routingKey)
	}

	routingKeyInfo, err = session.routingKeyInfo(context.Background(), "SELECT * FROM test_composite_routing_key WHERE second_id=? AND first_id=?")
	if err != nil {
		t.Fatalf("failed to get routing key info due to error: %v", err)
	}
	if routingKeyInfo == nil {
		t.Fatal("Expected routing key info, but was nil")
	}
	if len(routingKeyInfo.indexes) != 2 {
		t.Fatalf("Expected routing key indexes length to be 2 but was %d", len(routingKeyInfo.indexes))
	}
	if routingKeyInfo.indexes[0] != 1 {
		t.Errorf("Expected routing key index[0] to be 1 but was %d", routingKeyInfo.indexes[0])
	}
	if routingKeyInfo.indexes[1] != 0 {
		t.Errorf("Expected routing key index[1] to be 0 but was %d", routingKeyInfo.indexes[1])
	}
	if len(routingKeyInfo.types) != 2 {
		t.Fatalf("Expected routing key types length to be 1 but was %d", len(routingKeyInfo.types))
	}
	if routingKeyInfo.types[0] == nil {
		t.Fatal("Expected routing key types[0] to be non-nil")
	}
	if routingKeyInfo.types[0].Type() != TypeInt {
		t.Fatalf("Expected routing key types[0] to be %v but was %v", TypeInt, routingKeyInfo.types[0].Type())
	}
	if routingKeyInfo.types[1] == nil {
		t.Fatal("Expected routing key types[1] to be non-nil")
	}
	if routingKeyInfo.types[1].Type() != TypeInt {
		t.Fatalf("Expected routing key types[0] to be %v but was %v", TypeInt, routingKeyInfo.types[1].Type())
	}

	query = session.Query("SELECT * FROM test_composite_routing_key WHERE second_id=? AND first_id=?", 1, 2)
	routingKey, err = query.GetRoutingKey()
	if err != nil {
		t.Fatalf("Failed to get routing key due to error: %v", err)
	}
	expectedRoutingKey = []byte{0, 4, 0, 0, 0, 2, 0, 0, 4, 0, 0, 0, 1, 0}
	if !reflect.DeepEqual(expectedRoutingKey, routingKey) {
		t.Errorf("Expected routing key %v but was %v", expectedRoutingKey, routingKey)
	}

	// verify the cache is working
	cacheSize = session.routingKeyInfoCache.lru.Len()
	if cacheSize != initCacheSize+2 {
		t.Errorf("Expected cache size to be %d but was %d", initCacheSize+2, cacheSize)
	}
}

// Integration test of the token-aware policy-based connection pool
func TestTokenAwareConnPool(t *testing.T) {
	cluster := createCluster()
	cluster.PoolConfig.HostSelectionPolicy = TokenAwareHostPolicy(RoundRobinHostPolicy())

	// force metadata query to page
	cluster.PageSize = 1

	session := createSessionFromCluster(cluster, t)
	defer session.Close()

	expectedPoolSize := cluster.NumConns * len(session.hostSource.getHostsList())

	// wait for pool to fill
	for i := 0; i < 50; i++ {
		if session.pool.Size() == expectedPoolSize {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	if expectedPoolSize != session.pool.Size() {
		t.Errorf("Expected pool size %d but was %d", expectedPoolSize, session.pool.Size())
	}

	// add another cf so there are two pages when fetching table metadata from our keyspace
	if err := createTable(session, "CREATE TABLE gocql_test.test_token_aware_other_cf (id int, data text, PRIMARY KEY (id))"); err != nil {
		t.Fatalf("failed to create test_token_aware table with err: %v", err)
	}

	if err := createTable(session, "CREATE TABLE gocql_test.test_token_aware (id int, data text, PRIMARY KEY (id))"); err != nil {
		t.Fatalf("failed to create test_token_aware table with err: %v", err)
	}
	query := session.Query("INSERT INTO test_token_aware (id, data) VALUES (?,?)", 42, "8 * 6 =")
	if err := query.Exec(); err != nil {
		t.Fatalf("failed to insert with err: %v", err)
	}

	query = session.Query("SELECT data FROM test_token_aware where id = ?", 42).Consistency(One)
	var data string
	if err := query.Scan(&data); err != nil {
		t.Error(err)
	}

	// TODO add verification that the query went to the correct host
}

func TestNegativeStream(t *testing.T) {
	session := createSession(t)
	defer session.Close()

	conn := getRandomConn(t, session)

	const stream = -50
	writer := frameWriterFunc(func(f *framer, streamID int) error {
		f.writeHeader(0, opOptions, stream)
		return f.finish()
	})

	frame, err := conn.exec(context.Background(), writer, nil)
	if err == nil {
		t.Fatalf("expected to get an error on stream %d", stream)
	} else if frame != nil {
		t.Fatalf("expected to get nil frame got %+v", frame)
	}
}

func TestManualQueryPaging(t *testing.T) {
	const rowsToInsert = 5

	session := createSession(t)
	defer session.Close()

	if err := createTable(session, "CREATE TABLE gocql_test.testManualPaging (id int, count int, PRIMARY KEY (id))"); err != nil {
		t.Fatal(err)
	}

	for i := 0; i < rowsToInsert; i++ {
		err := session.Query("INSERT INTO testManualPaging(id, count) VALUES(?, ?)", i, i*i).Exec()
		if err != nil {
			t.Fatal(err)
		}
	}

	// disable auto paging, 1 page per iteration
	query := session.Query("SELECT id, count FROM testManualPaging").PageState(nil).PageSize(2)
	var id, count, fetched int

	iter := query.Iter()
	// NOTE: this isnt very indicative of how it should be used, the idea is that
	// the page state is returned to some client who will send it back to manually
	// page through the results.
	for {
		for iter.Scan(&id, &count) {
			if count != (id * id) {
				t.Fatalf("got wrong value from iteration: got %d expected %d", count, id*id)
			}

			fetched++
		}

		if !iter.LastPage() {
			// more pages
			iter = query.PageState(iter.PageState()).Iter()
		} else {
			break
		}
	}

	if err := iter.Close(); err != nil {
		t.Fatal(err)
	}

	if fetched != rowsToInsert {
		t.Fatalf("expected to fetch %d rows got %d", rowsToInsert, fetched)
	}
}

// Issue 475
func TestSessionBindRoutingKey(t *testing.T) {
	cluster := createCluster()
	cluster.PoolConfig.HostSelectionPolicy = TokenAwareHostPolicy(RoundRobinHostPolicy())

	session := createSessionFromCluster(cluster, t)
	defer session.Close()

	if err := createTable(session, `CREATE TABLE gocql_test.test_bind_routing_key (
			key     varchar,
			value   int,
			PRIMARY KEY (key)
		)`); err != nil {

		t.Fatal(err)
	}

	const (
		key   = "routing-key"
		value = 5
	)

	fn := func(info *QueryInfo) ([]interface{}, error) {
		return []interface{}{key, value}, nil
	}

	q := session.Bind("INSERT INTO test_bind_routing_key(key, value) VALUES(?, ?)", fn)
	if err := q.Exec(); err != nil {
		t.Fatal(err)
	}
}

func TestJSONSupport(t *testing.T) {
	session := createSession(t)
	defer session.Close()

	if session.cfg.ProtoVersion < protoVersion4 {
		t.Skip("skipping JSON support on proto < 4")
	}

	if err := createTable(session, `CREATE TABLE gocql_test.test_json (
		    id text PRIMARY KEY,
		    age int,
		    state text
		)`); err != nil {

		t.Fatal(err)
	}

	err := session.Query("INSERT INTO test_json JSON ?", `{"id": "user123", "age": 42, "state": "TX"}`).Exec()
	if err != nil {
		t.Fatal(err)
	}

	var (
		id    string
		age   int
		state string
	)

	err = session.Query("SELECT id, age, state FROM test_json WHERE id = ?", "user123").Scan(&id, &age, &state)
	if err != nil {
		t.Fatal(err)
	}

	if id != "user123" {
		t.Errorf("got id %q expected %q", id, "user123")
	}
	if age != 42 {
		t.Errorf("got age %d expected %d", age, 42)
	}
	if state != "TX" {
		t.Errorf("got state %q expected %q", state, "TX")
	}
}

func TestUnmarshallNestedTypes(t *testing.T) {
	session := createSession(t)
	defer session.Close()

	if err := createTable(session, `CREATE TABLE gocql_test.test_557 (
		    id text PRIMARY KEY,
		    val list<frozen<map<text, text> > >
		)`); err != nil {

		t.Fatal(err)
	}

	m := []map[string]string{
		{"key1": "val1"},
		{"key2": "val2"},
	}

	const id = "key"
	err := session.Query("INSERT INTO test_557(id, val) VALUES(?, ?)", id, m).Exec()
	if err != nil {
		t.Fatal(err)
	}

	var data []map[string]string
	if err := session.Query("SELECT val FROM test_557 WHERE id = ?", id).Scan(&data); err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(data, m) {
		t.Fatalf("%+#v != %+#v", data, m)
	}
}

func TestSchemaReset(t *testing.T) {
	if flagCassVersion.Major == 0 || flagCassVersion.Before(2, 1, 3) {
		t.Skipf("skipping TestSchemaReset due to CASSANDRA-7910 in Cassandra <2.1.3 version=%v", flagCassVersion)
	}

	cluster := createCluster()
	cluster.NumConns = 1

	session := createSessionFromCluster(cluster, t)
	defer session.Close()

	if err := createTable(session, `CREATE TABLE gocql_test.test_schema_reset (
		id text PRIMARY KEY)`); err != nil {

		t.Fatal(err)
	}

	const key = "test"

	err := session.Query("INSERT INTO test_schema_reset(id) VALUES(?)", key).Exec()
	if err != nil {
		t.Fatal(err)
	}

	var id string
	err = session.Query("SELECT * FROM test_schema_reset WHERE id=?", key).Scan(&id)
	if err != nil {
		t.Fatal(err)
	} else if id != key {
		t.Fatalf("expected to get id=%q got=%q", key, id)
	}

	if err := createTable(session, `ALTER TABLE gocql_test.test_schema_reset ADD val text`); err != nil {
		t.Fatal(err)
	}

	const expVal = "test-val"
	err = session.Query("INSERT INTO test_schema_reset(id, val) VALUES(?, ?)", key, expVal).Exec()
	if err != nil {
		t.Fatal(err)
	}

	var val string
	err = session.Query("SELECT * FROM test_schema_reset WHERE id=?", key).Scan(&id, &val)
	if err != nil {
		t.Fatal(err)
	}

	if id != key {
		t.Errorf("expected to get id=%q got=%q", key, id)
	}
	if val != expVal {
		t.Errorf("expected to get val=%q got=%q", expVal, val)
	}
}

func TestCreateSession_DontSwallowError(t *testing.T) {
	t.Skip("This test is bad, and the resultant error from cassandra changes between versions")
	cluster := createCluster()
	cluster.ProtoVersion = 0x100
	session, err := cluster.CreateSession()
	if err == nil {
		session.Close()

		t.Fatal("expected to get an error for unsupported protocol")
	}

	if flagCassVersion.Major < 3 {
		// TODO: we should get a distinct error type here which include the underlying
		// cassandra error about the protocol version, for now check this here.
		if !strings.Contains(err.Error(), "Invalid or unsupported protocol version") {
			t.Fatalf(`expcted to get error "unsupported protocol version" got: %q`, err)
		}
	} else {
		if !strings.Contains(err.Error(), "unsupported response version") {
			t.Fatalf(`expcted to get error "unsupported response version" got: %q`, err)
		}
	}
}

func TestControl_DiscoverProtocol(t *testing.T) {
	cluster := createCluster()
	cluster.ProtoVersion = 0

	session, err := cluster.CreateSession()
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()

	if session.cfg.ProtoVersion == 0 {
		t.Fatal("did not discovery protocol")
	}
}

// TestUnsetCol verify unset column will not replace an existing column
func TestUnsetCol(t *testing.T) {
	session := createSession(t)
	defer session.Close()

	if session.cfg.ProtoVersion < protoVersion4 {
		t.Skip("Unset Values are not supported in protocol < 4")
	}

	if err := createTable(session, "CREATE TABLE gocql_test.testUnsetInsert (id int, my_int int, my_text text, PRIMARY KEY (id))"); err != nil {
		t.Fatalf("failed to create table with error '%v'", err)
	}
	if err := session.Query("INSERT INTO testUnSetInsert (id,my_int,my_text) VALUES (?,?,?)", 1, 2, "3").Exec(); err != nil {
		t.Fatalf("failed to insert with err: %v", err)
	}
	if err := session.Query("INSERT INTO testUnSetInsert (id,my_int,my_text) VALUES (?,?,?)", 1, UnsetValue, UnsetValue).Exec(); err != nil {
		t.Fatalf("failed to insert with err: %v", err)
	}

	var id, mInt int
	var mText string

	if err := session.Query("SELECT id, my_int ,my_text FROM testUnsetInsert").Scan(&id, &mInt, &mText); err != nil {
		t.Fatalf("failed to select with err: %v", err)
	} else if id != 1 || mInt != 2 || mText != "3" {
		t.Fatalf("Expected results: 1, 2, \"3\", got %v, %v, %v", id, mInt, mText)
	}
}

// TestUnsetColBatch verify unset column will not replace a column in batch
func TestUnsetColBatch(t *testing.T) {
	session := createSession(t)
	defer session.Close()

	if session.cfg.ProtoVersion < protoVersion4 {
		t.Skip("Unset Values are not supported in protocol < 4")
	}

	if err := createTable(session, "CREATE TABLE gocql_test.batchUnsetInsert (id int, my_int int, my_text text, PRIMARY KEY (id))"); err != nil {
		t.Fatalf("failed to create table with error '%v'", err)
	}

	b := session.Batch(LoggedBatch)
	b.Query("INSERT INTO gocql_test.batchUnsetInsert(id, my_int, my_text) VALUES (?,?,?)", 1, 1, UnsetValue)
	b.Query("INSERT INTO gocql_test.batchUnsetInsert(id, my_int, my_text) VALUES (?,?,?)", 1, UnsetValue, "")
	b.Query("INSERT INTO gocql_test.batchUnsetInsert(id, my_int, my_text) VALUES (?,?,?)", 2, 2, UnsetValue)

	if err := session.ExecuteBatch(b); err != nil {
		t.Fatalf("query failed. %v", err)
	} else {
		if b.Attempts() < 1 {
			t.Fatal("expected at least 1 attempt, but got 0")
		}
		if b.Latency() <= 0 {
			t.Fatalf("expected latency to be greater than 0, but got %v instead.", b.Latency())
		}
	}
	var id, mInt, count int
	var mText string
	if err := session.Query("SELECT count(*) FROM gocql_test.batchUnsetInsert;").Scan(&count); err != nil {
		t.Fatalf("Failed to select with err: %v", err)
	} else if count != 2 {
		t.Fatalf("Expected Batch Insert count 2, got %v", count)
	}

	if err := session.Query("SELECT id, my_int ,my_text FROM gocql_test.batchUnsetInsert where id=1;").Scan(&id, &mInt, &mText); err != nil {
		t.Fatalf("failed to select with err: %v", err)
	} else if id != mInt {
		t.Fatalf("expected id, my_int to be 1, got %v and %v", id, mInt)
	}
}

func TestQuery_NamedValues(t *testing.T) {
	session := createSession(t)
	defer session.Close()

	if err := createTable(session, "CREATE TABLE gocql_test.named_query(id int, value text, PRIMARY KEY (id))"); err != nil {
		t.Fatal(err)
	}

	err := session.Query("INSERT INTO gocql_test.named_query(id, value) VALUES(:id, :value)", NamedValue("id", 1), NamedValue("value", "i am a value")).Exec()
	if err != nil {
		t.Fatal(err)
	}
	var value string
	if err := session.Query("SELECT VALUE from gocql_test.named_query WHERE id = :id", NamedValue("id", 1)).Scan(&value); err != nil {
		t.Fatal(err)
	}
}

// This test ensures that queries are sent to the specified host only
func TestQuery_SetHostID(t *testing.T) {
	session := createSession(t)
	defer session.Close()

	hosts := session.GetHosts()

	const iterations = 5
	for _, expectedHost := range hosts {
		for i := 0; i < iterations; i++ {
			var actualHostID string
			err := session.Query("SELECT host_id FROM system.local").
				SetHostID(expectedHost.HostID()).
				Scan(&actualHostID)
			if err != nil {
				t.Fatal(err)
			}

			if expectedHost.HostID() != actualHostID {
				t.Fatalf("Expected query to be executed on host %s, but it was executed on %s",
					expectedHost.HostID(),
					actualHostID,
				)
			}
		}
	}

	// ensuring properly handled invalid host id
	err := session.Query("SELECT host_id FROM system.local").
		SetHostID("[invalid]").
		Exec()
	if !errors.Is(err, ErrNoPool) {
		t.Fatalf("Expected error to be: %v, but got %v", ErrNoPool, err)
	}

	// ensuring that the driver properly handles the case
	// when specified host for the query is down
	host := hosts[0]
	pool, _ := session.pool.getPoolByHostID(host.HostID())
	// simulating specified host is down
	pool.host.setState(NodeDown)
	err = session.Query("SELECT host_id FROM system.local").
		SetHostID(host.HostID()).
		Exec()
	if !errors.Is(err, ErrHostDown) {
		t.Fatalf("Expected error to be: %v, but got %v", ErrHostDown, err)
	}
}
