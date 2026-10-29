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
	"fmt"
	"math"
	"math/big"
	"net"
	"net/netip"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	frm "github.com/scylladb/gocql/v2/internal/frame"
	"github.com/scylladb/gocql/v2/internal/murmur"

	"github.com/stretchr/testify/require"
)

func TestEmptyHosts(t *testing.T) {
	t.Parallel()
	config := createV2Cluster()
	config.ContactPoints = nil
	session, err := Connect(context.Background(), config)
	if err == nil {
		session.Close()
		t.Fatal("expected error for empty contact points")
	}
}

func TestInvalidPeerEntry(t *testing.T) {
	t.Parallel()
	if *flagDistribution != "cassandra" {
		t.Skip("incomplete-peer wire fixture uses Cassandra's native v4 metadata")
	}
	fixture := &incompletePeerDialer{}
	config := createV2Cluster(func(c *Config) { c.Protocol = 4; c.Compression = NoCompression; c.TLS = nil; c.Dialer = fixture })
	session, err := Connect(context.Background(), config)
	require.NoError(t, err)
	defer session.Close()
	require.Positive(t, fixture.injected.Load(), "discovery must consume injected incomplete peer")
	hosts := session.Metadata().Hosts()
	require.Len(t, hosts, *clusterSize)
	for _, host := range hosts {
		require.NotEqual(t, "35c0ec48-5109-40fd-9281-9e9d4add2f1e", host.ID.String())
	}
	require.Equal(t, "local", cassandraText(t, cassandraRead(t, session, "SELECT key FROM system.local"), "key"))
}

// TestUseStatementError checks to make sure the correct error is returned when the user tries to execute a use statement.
func TestUseStatementError(t *testing.T) {
	t.Parallel()
	session := createV2Session(t)
	defer session.Close()
	_, err := v2Exec(context.Background(), session, "USE gocql_test")
	require.Error(t, err)
	require.ErrorIs(t, err, ErrUseStmt)
}

// TestInvalidKeyspace checks that an invalid keyspace will return promptly and without a flood of connections
func TestInvalidKeyspace(t *testing.T) {
	t.Parallel()
	config := createV2Cluster()
	config.Keyspace = "invalidKeyspace"
	session, err := Connect(context.Background(), config)
	if err == nil {
		session.Close()
		t.Fatal("expected invalid keyspace to fail")
	}
	require.Error(t, err)
}

func TestTracing(t *testing.T) {
	t.Parallel()
	session := createV2Session(t)
	defer session.Close()
	table := testTableName(t)
	cassandraDDL(t, session, fmt.Sprintf("CREATE TABLE %s (id int PRIMARY KEY)", table))
	write := cassandraWriteBound(t, session, fmt.Sprintf("INSERT INTO %s (id) VALUES (?)", table), []Value{IntValue(42)}, PrepareOptions{})
	result, err := Exec(context.Background(), session, write, Options{Trace: true})
	require.NoError(t, err)
	require.NotEmpty(t, result.TraceIDs)
	checkTrace := func(ids []UUID) {
		t.Helper()
		require.NotEmpty(t, ids)
		for _, id := range ids {
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			trace, err := session.Trace(ctx, id)
			cancel()
			require.NoError(t, err)
			require.Equal(t, id, trace.ID)
			require.NotEmpty(t, trace.Events)
		}
	}
	checkTrace(result.TraceIDs)
	read := cassandraDynamicBound(t, session, fmt.Sprintf("SELECT id FROM %s WHERE id = ?", table), []Value{IntValue(42)}, PrepareOptions{})
	// Options are explicit per execution; the same binding can be traced repeatedly.
	for i := 0; i < 2; i++ {
		rows, err := Run(context.Background(), session, read, Options{Trace: true})
		require.NoError(t, err)
		defer rows.Close()
		require.True(t, rows.Next())
		require.Equal(t, int64(42), cassandraInt(t, rows.Row(), "id"))
		require.False(t, rows.Next())
		require.NoError(t, rows.Close())
		checkTrace(rows.Info().TraceIDs)
	}
}

func TestObserve(t *testing.T) {
	t.Parallel()
	session := createV2Session(t)
	defer session.Close()
	table := testTableName(t)
	cassandraDDL(t, session, fmt.Sprintf("CREATE TABLE %s (id int PRIMARY KEY)", table))
	observer := &cassandraObserver{}
	check := func(stmt string, wantErr bool) {
		t.Helper()
		events, attempts := observer.snapshot()
		require.Len(t, events, 1)
		require.NotEmpty(t, attempts)
		require.Equal(t, stmt, events[0].Statement)
		require.Equal(t, "gocql_test", events[0].Routing.Keyspace)
		require.Equal(t, wantErr, events[0].Err != nil)
	}
	selectCQL := fmt.Sprintf("SELECT id FROM %s WHERE id = ?", table)
	read := cassandraDynamicBound(t, session, selectCQL, []Value{IntValue(43)}, PrepareOptions{})
	rows, err := Run(context.Background(), session, read, Options{Observer: observer})
	require.NoError(t, err)
	defer rows.Close()
	require.False(t, rows.Next())
	require.NoError(t, rows.Close())
	check(selectCQL, false)
	observer.reset()
	insertCQL := fmt.Sprintf("INSERT INTO %s (id) VALUES (?)", table)
	write := cassandraWriteBound(t, session, insertCQL, []Value{IntValue(42)}, PrepareOptions{})
	_, err = Exec(context.Background(), session, write, Options{Observer: observer})
	require.NoError(t, err)
	check(insertCQL, false)
	observer.reset()
	read = cassandraDynamicBound(t, session, selectCQL, []Value{IntValue(42)}, PrepareOptions{})
	require.Equal(t, int64(42), cassandraInt(t, cassandraReadBound(t, session, read, Options{Observer: observer}), "id"))
	check(selectCQL, false)
	observer.reset()
	other := createV2Session(t, func(c *Config) { c.Observer = observer })
	defer other.Close()
	observer.reset()
	read = cassandraDynamicBound(t, other, selectCQL, []Value{IntValue(42)}, PrepareOptions{})
	require.Equal(t, int64(42), cassandraInt(t, cassandraReadBound(t, other, read, Options{}), "id"))
	check(selectCQL, false)
	observer.reset()
	bad := cassandraUnprepared(t, "SELECT id FROM unknown_table WHERE id = 42", DynamicRows)
	_, err = Run(context.Background(), session, bad, Options{Observer: observer})
	require.Error(t, err)
	check("SELECT id FROM unknown_table WHERE id = 42", true)
}

func TestObserve_Pagination(t *testing.T) {
	t.Parallel()
	session := createV2Session(t)
	defer session.Close()
	table := testTableName(t)
	cassandraDDL(t, session, fmt.Sprintf("CREATE TABLE %s (id int PRIMARY KEY)", table))
	for i := int32(0); i < 50; i++ {
		cassandraWrite(t, session, fmt.Sprintf("INSERT INTO %s (id) VALUES (?)", table), IntValue(i))
	}
	observer := &cassandraObserver{}
	bound := cassandraUnprepared(t, fmt.Sprintf("SELECT id FROM %s LIMIT 100", table), DynamicRows)
	// The typed observer reports each transport attempt and one completed logical
	// request; row counts belong to Result instead of mutable per-page callbacks.
	rows, err := Run(context.Background(), session, bound, Options{PageSize: Option[int32]{Value: 10, Set: true}, Prefetch: Option[int]{Value: 0, Set: true}, Observer: observer})
	require.NoError(t, err)
	defer rows.Close()
	for i := 0; i < 50; i++ {
		require.True(t, rows.Next(), "row %d: %v", i, rows.Err())
		_, attempts := observer.snapshot()
		require.Equal(t, i/10+1, len(attempts))
	}
	require.False(t, rows.Next())
	require.NoError(t, rows.Close())
	events, attempts := observer.snapshot()
	require.Len(t, events, 1)
	require.GreaterOrEqual(t, len(attempts), 5)
	require.Equal(t, int64(50), events[0].Result.Rows)
}

func TestPaging(t *testing.T) {
	t.Parallel()
	session := createV2Session(t)
	defer session.Close()
	table := testTableName(t)
	cassandraDDL(t, session, fmt.Sprintf("CREATE TABLE %s (id int PRIMARY KEY)", table))
	for i := int32(0); i < 100; i++ {
		cassandraWrite(t, session, fmt.Sprintf("INSERT INTO %s (id) VALUES (?)", table), IntValue(i))
	}
	bound := cassandraUnprepared(t, fmt.Sprintf("SELECT id FROM %s", table), DynamicRows)
	rows := cassandraRows(t, session, bound, Options{PageSize: Option[int32]{Value: 10, Set: true}})
	require.Len(t, rows, 100)
}

func TestPagingWithAllowFiltering(t *testing.T) {
	t.Parallel()
	session := createV2Session(t)
	defer session.Close()
	table := testTableName(t)
	const targetP1, targetP2, totalExpectedResults, pageSize, deletedRangeStart, deletedRangeEnd = 50, 50, 30, 5, 10, 20
	selectCQL := fmt.Sprintf("SELECT c1,f1 FROM %s WHERE p1=50 AND p2=50 AND f1<30 ALLOW FILTERING", table)
	validate := func(t *testing.T, rows []DynamicRow) {
		t.Helper()
		var got []int
		for _, row := range rows {
			c1, f1 := cassandraInt(t, row, "c1"), cassandraInt(t, row, "f1")
			require.Equal(t, c1, f1)
			got = append(got, int(f1))
		}
		sort.Ints(got)
		var want []int
		for i := 0; i < totalExpectedResults; i++ {
			if i < deletedRangeStart || i >= deletedRangeEnd {
				want = append(want, i)
			}
		}
		require.Equal(t, want, got)
	}
	t.Run("Prepare", func(t *testing.T) {
		cassandraDDL(t, session, fmt.Sprintf("CREATE TABLE %s (p1 int,p2 int,c1 int,f1 int,PRIMARY KEY ((p1,p2),c1)) WITH CLUSTERING ORDER BY (c1 DESC)", table))
		for i := int32(0); i < 100; i++ {
			cassandraWrite(t, session, fmt.Sprintf("INSERT INTO %s (p1,p2,c1,f1) VALUES (?,?,?,?)", table), IntValue(i), IntValue(i), IntValue(i), IntValue(i))
			cassandraWrite(t, session, fmt.Sprintf("INSERT INTO %s (p1,p2,c1,f1) VALUES (?,?,?,?)", table), IntValue(targetP1), IntValue(targetP2), IntValue(i), IntValue(i))
		}
		cassandraWrite(t, session, fmt.Sprintf("DELETE FROM %s WHERE p1=? AND p2=? AND c1>=? AND c1<?", table), IntValue(targetP1), IntValue(targetP2), IntValue(deletedRangeStart), IntValue(deletedRangeEnd))
	})
	bound := cassandraUnprepared(t, selectCQL, DynamicRows)
	t.Run("AutoPagination", func(t *testing.T) {
		for _, c := range []Consistency{One, Quorum} {
			t.Run(c.String(), func(t *testing.T) {
				validate(t, cassandraRows(t, session, bound, Options{Consistency: Option[Consistency]{Value: c, Set: true}, PageSize: Option[int32]{Value: pageSize, Set: true}}))
			})
		}
	})
	t.Run("ManualPagination", func(t *testing.T) {
		for _, c := range []Consistency{One, Quorum} {
			t.Run(c.String(), func(t *testing.T) {
				var result []DynamicRow
				state := StartPage()
				for {
					page, err := RunPage(context.Background(), session, bound, state, Options{Consistency: Option[Consistency]{Value: c, Set: true}, PageSize: Option[int32]{Value: pageSize, Set: true}})
					require.NoError(t, err)
					defer page.Close()
					next := page.State()
					// RunPage exposes empty tombstone/filter pages. Their continuation
					// must still advance, and their reported count must match decoded rows.
					pageRows := int64(0)
					for page.Next() {
						pageRows++
						row, err := page.Clone()
						require.NoError(t, err)
						result = append(result, row)
					}
					require.Equal(t, page.Info().Rows, pageRows)
					require.NoError(t, page.Close())
					if next.Complete() {
						break
					}
					require.NotEqual(t, state.Bytes(), next.Bytes())
					state = RestorePageState(next.Bytes())
				}
				validate(t, result)
			})
		}
	})
	cassandraDDL(t, session, "DROP TABLE "+table)
}

func TestPagingWithBind(t *testing.T) {
	t.Parallel()
	session := createV2Session(t)
	defer session.Close()
	table := testTableName(t)
	cassandraDDL(t, session, fmt.Sprintf("CREATE TABLE %s (id int,val int,PRIMARY KEY(id,val))", table))
	for i := int32(0); i < 100; i++ {
		cassandraWrite(t, session, fmt.Sprintf("INSERT INTO %s(id,val) VALUES (?,?)", table), IntValue(1), IntValue(i))
	}
	prepared := cassandraPrepared(t, session, Statement[[]Value, DynamicRow]{CQL: fmt.Sprintf("SELECT val FROM %s WHERE id=? AND val<?", table), Params: DynamicParams, Rows: DynamicRows}, PrepareOptions{})
	first := cassandraBound(t, prepared, []Value{IntValue(1), IntValue(50)})
	second := cassandraBound(t, prepared, []Value{IntValue(1), IntValue(20)})
	options := Options{PageSize: Option[int32]{Value: 10, Set: true}}
	require.Len(t, cassandraRows(t, session, first, options), 50)
	require.Len(t, cassandraRows(t, session, second, options), 20)
	require.Len(t, cassandraRows(t, session, first, options), 50, "new binding must not mutate old binding")
}

func TestCAS(t *testing.T) {
	t.Parallel()
	session := createV2SessionFromConfig(createV2Cluster(), t, true)
	defer session.Close()
	table := testTableName(t)
	cassandraDDL(t, session, fmt.Sprintf("CREATE TABLE %s(title varchar,revid timeuuid,last_modified timestamp,PRIMARY KEY(title,revid))", table))
	title := "baz"
	rev := NewTimeUUID()
	modified := Timestamp(time.Now().UnixMilli())
	revValue := cassandraValue(TimeUUIDValue(TimeUUID(rev)))
	insertCQL := fmt.Sprintf("INSERT INTO %s(title,revid,last_modified) VALUES (?,?,?) IF NOT EXISTS", table)
	values := []Value{TextValue(title), revValue, TimestampValue(modified)}
	insert := cassandraCASBound(t, session, insertCQL, values, DynamicRows)
	first := cassandraReadBound(t, session, insert, Options{})
	require.True(t, first.Applied)
	second := cassandraReadBound(t, session, insert, Options{})
	require.False(t, second.Applied)
	require.True(t, second.Previous.Set)
	require.Equal(t, title, cassandraText(t, second.Previous.Value, "title"))
	gotRev, err := cassandraField(t, second.Previous.Value, "revid").UUID()
	require.NoError(t, err)
	require.Equal(t, rev, gotRev)
	deleteCQL := fmt.Sprintf("DELETE FROM %s WHERE title=? AND revid=? IF last_modified=?", table)
	failedDelete := cassandraCASBound(t, session, deleteCQL, []Value{TextValue(title), revValue, TimestampValue(modified + 10000)}, DynamicRows)
	deleted := cassandraReadBound(t, session, failedDelete, Options{})
	require.False(t, deleted.Applied)
	require.Equal(t, int64(modified), cassandraInt(t, deleted.Previous.Value, "last_modified"))
	// Missing or incompatible previous-value decoders fail explicitly.
	invalid, err := Prepare(context.Background(), session, Statement[[]Value, CASResult[Unit]]{CQL: deleteCQL, Params: DynamicParams, Rows: CASRows(NoRows)}, PrepareOptions{})
	if err == nil {
		bound := cassandraBound(t, invalid, []Value{TextValue(title), revValue, TimestampValue(modified + 10000)})
		rows, e := Run(context.Background(), session, bound, Options{})
		if rows != nil {
			defer rows.Close()
		}
		err = e
	}
	require.Error(t, err)
	successDelete := cassandraCASBound(t, session, deleteCQL, values, DynamicRows)
	require.True(t, cassandraReadBound(t, session, successDelete, Options{}).Applied)
	cassandraWrite(t, session, "TRUNCATE "+table)
	runBatch := func(items []BatchItem, wantApplied bool, wantRows int) {
		t.Helper()
		page, err := RunBatch(context.Background(), session, LoggedBatch, items, CASRows(DynamicRows), Options{})
		require.NoError(t, err)
		defer page.Close()
		count := 0
		for page.Next() {
			require.Equal(t, wantApplied, page.Row().Applied)
			if !wantApplied {
				require.True(t, page.Row().Previous.Set)
			}
			count++
		}
		require.NoError(t, page.Close())
		require.Equal(t, wantRows, count)
	}
	runBatch([]BatchItem{cassandraItem(t, insert)}, true, 1)
	other := cassandraCASBound(t, session, insertCQL, []Value{TextValue(title + "_foo"), revValue, TimestampValue(modified)}, DynamicRows)
	runBatch([]BatchItem{cassandraItem(t, other)}, true, 1)
	runBatch([]BatchItem{cassandraItem(t, other)}, false, 1)
	var seed, conditional []BatchItem
	for _, id := range []string{"2c3af400-73a4-11e5-9381-29463d90c3f0", "3e4ad2f1-73a4-11e5-9381-29463d90c3f0"} {
		r, e := ParseUUID(id)
		require.NoError(t, e)
		rv := cassandraValue(TimeUUIDValue(TimeUUID(r)))
		seed = append(seed, cassandraItem(t, cassandraWriteBound(t, session, fmt.Sprintf("INSERT INTO %s(title,revid,last_modified) VALUES (?,?,?)", table), []Value{TextValue("_foo"), rv, TimestampValue(modified)}, PrepareOptions{})))
		conditional = append(conditional, cassandraItem(t, cassandraCASBound(t, session, fmt.Sprintf("UPDATE %s SET last_modified=? WHERE title=? AND revid=? IF last_modified=?", table), []Value{TimestampValue(modified + 10000), TextValue("_foo"), rv, TimestampValue(modified + 20000)}, DynamicRows)))
	}
	cassandraBatch(t, session, LoggedBatch, seed, Options{})
	runBatch(conditional, false, 2)
	// CASRows rejects ordinary SELECTs and unconditional batches; the former
	// MapScanCAS silent false result is replaced by validated result metadata.
	_, err = Prepare(context.Background(), session, Statement[[]Value, CASResult[DynamicRow]]{CQL: fmt.Sprintf("SELECT revid FROM %s WHERE title=?", table), Params: DynamicParams, Rows: CASRows(DynamicRows)}, PrepareOptions{})
	require.Error(t, err)
	for i := 0; i < 2; i++ {
		page, err := RunBatch(context.Background(), session, LoggedBatch, seed, CASRows(DynamicRows), Options{})
		if page != nil {
			page.Close()
		}
		require.Error(t, err)
	}
	for _, batch := range []bool{false, true} {
		badDecoder := CASRows(OneColumn(Bool))
		bound := cassandraDynamicBound(t, session, fmt.Sprintf("UPDATE %s SET last_modified=? WHERE title=? AND revid=? IF last_modified=?", table), []Value{TimestampValue(modified + 10000), TextValue("_foo"), cassandraValue(TimeUUIDValue(TimeUUID(cassandraUUID("3e4ad2f1-73a4-11e5-9381-29463d90c3f0")))), TimestampValue(modified + 20000)}, PrepareOptions{})
		if batch {
			page, e := RunBatch(context.Background(), session, LoggedBatch, []BatchItem{cassandraItem(t, bound)}, badDecoder, Options{})
			if page != nil {
				page.Close()
			}
			require.Error(t, e)
		} else {
			bad := &Bound[CASResult[bool]]{request: bound.request, rows: badDecoder}
			rows, e := Run(context.Background(), session, bad, Options{})
			if rows != nil {
				rows.Close()
			}
			require.Error(t, e)
		}
	}
}

func TestConsistencySerial(t *testing.T) {
	t.Parallel()
	session := createV2Session(t)
	defer session.Close()
	table := testTableName(t)
	cassandraDDL(t, session, fmt.Sprintf("CREATE TABLE %s(id int PRIMARY KEY)", table))
	for i, tc := range []struct {
		name        string
		consistency Consistency
	}{{"Any", Any}, {"One", One}, {"Two", Two}, {"Three", Three}, {"Quorum", Quorum}, {"LocalQuorum", LocalQuorum}, {"EachQuorum", EachQuorum}, {"Serial", Serial}, {"LocalSerial", LocalSerial}, {"LocalOne", LocalOne}} {
		t.Run(tc.name, func(t *testing.T) {
			bound := cassandraWriteBound(t, session, fmt.Sprintf("INSERT INTO %s(id) VALUES (?)", table), []Value{IntValue(int32(i + 1))}, PrepareOptions{})
			_, err := Exec(context.Background(), session, bound, Options{SerialConsistency: Option[Option[Consistency]]{Set: true, Value: Option[Consistency]{Set: true, Value: tc.consistency}}})
			if tc.consistency == Serial || tc.consistency == LocalSerial {
				require.NoError(t, err)
				require.Equal(t, int64(i+1), cassandraInt(t, cassandraRead(t, session, fmt.Sprintf("SELECT id FROM %s WHERE id=?", table), IntValue(int32(i+1))), "id"))
			} else {
				require.Error(t, err)
				require.Contains(t, err.Error(), "serial consistency")
			}
		})
	}
}

func TestDurationType(t *testing.T) {
	t.Parallel()
	session := createV2Session(t)
	defer session.Close()
	if session.cfg.ProtoVersion < 5 {
		t.Skip("skipped below protocol 5 by this test's own guard, not by a limit of the duration type")
	}
	table := testTableName(t)
	cassandraDDL(t, session, fmt.Sprintf("CREATE TABLE %s(k int PRIMARY KEY,v duration)", table))
	for _, want := range []Duration{{Months: 250, Days: 500, Nanoseconds: 300010001}, {Months: -250, Days: -500, Nanoseconds: -300010001}, {Days: 128, Nanoseconds: 127}, {Months: 0x7fffffff, Days: 0x7fffffff, Nanoseconds: 0x7fffffffffffffff}} {
		cassandraWrite(t, session, fmt.Sprintf("INSERT INTO %s(k,v) VALUES(1,?)", table), cassandraValue(DurationValue(want)))
		row := cassandraRead(t, session, "SELECT k,v FROM "+table)
		require.Equal(t, int64(1), cassandraInt(t, row, "k"))
		got, e := cassandraField(t, row, "v").Duration()
		require.NoError(t, e)
		require.Equal(t, want, got)
	}
}

func TestMapScanCAS(t *testing.T) {
	t.Parallel()
	session := createV2SessionFromConfig(createV2Cluster(), t, true)
	defer session.Close()
	table := testTableName(t)
	cassandraDDL(t, session, fmt.Sprintf("CREATE TABLE %s(title varchar,revid timeuuid,last_modified timestamp,deleted boolean,PRIMARY KEY(title,revid))", table))
	rev := NewTimeUUID()
	modified := Timestamp(time.Now().UnixMilli())
	bound := cassandraCASBound(t, session, fmt.Sprintf("INSERT INTO %s(title,revid,last_modified,deleted) VALUES(?,?,?,?) IF NOT EXISTS", table), []Value{TextValue("baz"), cassandraValue(TimeUUIDValue(TimeUUID(rev))), TimestampValue(modified), BoolValue(false)}, DynamicRows)
	require.True(t, cassandraReadBound(t, session, bound, Options{}).Applied)
	result := cassandraReadBound(t, session, bound, Options{})
	require.False(t, result.Applied)
	require.True(t, result.Previous.Set)
	row := result.Previous.Value
	require.Equal(t, "baz", cassandraText(t, row, "title"))
	got, e := cassandraField(t, row, "revid").UUID()
	require.NoError(t, e)
	require.Equal(t, rev, got)
	require.False(t, cassandraBool(t, row, "deleted"))
	require.Equal(t, int64(modified), cassandraInt(t, row, "last_modified"))
}

func TestBatch(t *testing.T) {
	t.Parallel()
	session := createV2Session(t)
	defer session.Close()
	table := testTableName(t)
	cassandraDDL(t, session, fmt.Sprintf("CREATE TABLE %s(id int PRIMARY KEY)", table))
	prepared := cassandraPrepared(t, session, Statement[int32, Unit]{CQL: fmt.Sprintf("INSERT INTO %s(id) VALUES(?)", table), Params: OneParam(Int32), Rows: NoRows}, PrepareOptions{})
	var items []BatchItem
	for i := int32(0); i < 100; i++ {
		items = append(items, cassandraItem(t, cassandraBound(t, prepared, i)))
	}
	cassandraBatch(t, session, LoggedBatch, items, Options{})
	require.Equal(t, int64(100), cassandraInt(t, cassandraRead(t, session, "SELECT COUNT(*) FROM "+table), "count"))
}

func TestUnpreparedBatch(t *testing.T) {
	t.Parallel()
	session := createV2Session(t)
	defer session.Close()
	table := testTableName(t)
	cassandraDDL(t, session, fmt.Sprintf("CREATE TABLE %s(id int PRIMARY KEY,c counter)", table))
	bound := cassandraUnprepared(t, fmt.Sprintf("UPDATE %s SET c=c+1 WHERE id=1", table), NoRows)
	items := make([]BatchItem, 100)
	for i := range items {
		items[i] = cassandraItem(t, bound)
	}
	cassandraBatch(t, session, UnloggedBatch, items, Options{})
	require.Equal(t, int64(1), cassandraInt(t, cassandraRead(t, session, "SELECT COUNT(*) FROM "+table), "count"))
	require.Equal(t, int64(100), cassandraInt(t, cassandraRead(t, session, "SELECT c FROM "+table), "c"))
}

// TestBatchLimit tests gocql to make sure batch operations larger than the maximum
// statement limit are not submitted to a cassandra node.
func TestBatchLimit(t *testing.T) {
	t.Parallel()
	session := createV2Session(t)
	defer session.Close()
	table := testTableName(t)
	cassandraDDL(t, session, fmt.Sprintf("CREATE TABLE %s(id int PRIMARY KEY)", table))
	bound := cassandraWriteBound(t, session, fmt.Sprintf("INSERT INTO %s(id) VALUES(?)", table), []Value{IntValue(1)}, PrepareOptions{})
	items := make([]BatchItem, 65537)
	for i := range items {
		items[i] = cassandraItem(t, bound)
	}
	before := session.Metrics()
	_, err := ExecBatch(context.Background(), session, LoggedBatch, items, Options{})
	require.Error(t, err)
	require.Contains(t, err.Error(), "65535")
	require.Equal(t, before.Requests, session.Metrics().Requests, "oversized batches must be rejected before submission")
}

func TestWhereIn(t *testing.T) {
	t.Parallel()
	session := createV2Session(t)
	defer session.Close()
	table := testTableName(t)
	cassandraDDL(t, session, fmt.Sprintf("CREATE TABLE %s(id int,cluster int,PRIMARY KEY(id,cluster))", table))
	cassandraWrite(t, session, fmt.Sprintf("INSERT INTO %s(id,cluster) VALUES(?,?)", table), IntValue(100), IntValue(200))
	row := cassandraRead(t, session, fmt.Sprintf("SELECT * FROM %s WHERE id=? AND cluster IN (?)", table), IntValue(100), IntValue(200))
	require.Equal(t, int64(100), cassandraInt(t, row, "id"))
	require.Equal(t, int64(200), cassandraInt(t, row, "cluster"))
}

// TestTooManyQueryArgs tests to make sure the library correctly handles the application level bug
// whereby too many query arguments are passed to a query
func TestTooManyQueryArgs(t *testing.T) {
	t.Parallel()
	session := createV2Session(t)
	defer session.Close()
	table := testTableName(t)
	cassandraDDL(t, session, fmt.Sprintf("CREATE TABLE %s(id int PRIMARY KEY,value int)", table))
	read := cassandraPrepared(t, session, Statement[[]Value, DynamicRow]{CQL: fmt.Sprintf("SELECT * FROM %s WHERE id=?", table), Params: DynamicParams, Rows: DynamicRows}, PrepareOptions{})
	_, err := BindOwned(read, []Value{IntValue(1), IntValue(2)})
	require.Error(t, err)
	write := cassandraPrepared(t, session, Statement[[]Value, Unit]{CQL: fmt.Sprintf("INSERT INTO %s(id,value) VALUES(?,?)", table), Params: DynamicParams, Rows: NoRows}, PrepareOptions{})
	_, err = BindOwned(write, []Value{IntValue(1), IntValue(2), IntValue(3)})
	require.Error(t, err, "invalid batch entry must fail during binding")
}

// TestNotEnoughQueryArgs tests to make sure the library correctly handles the application level bug
// whereby not enough query arguments are passed to a query
func TestNotEnoughQueryArgs(t *testing.T) {
	t.Parallel()
	session := createV2Session(t)
	defer session.Close()
	table := testTableName(t)
	cassandraDDL(t, session, fmt.Sprintf("CREATE TABLE %s(id int,cluster int,value int,PRIMARY KEY(id,cluster))", table))
	read := cassandraPrepared(t, session, Statement[[]Value, DynamicRow]{CQL: fmt.Sprintf("SELECT * FROM %s WHERE id=? AND cluster=?", table), Params: DynamicParams, Rows: DynamicRows}, PrepareOptions{})
	_, err := BindOwned(read, []Value{IntValue(1)})
	require.Error(t, err)
	write := cassandraPrepared(t, session, Statement[[]Value, Unit]{CQL: fmt.Sprintf("INSERT INTO %s(id,cluster,value) VALUES(?,?,?)", table), Params: DynamicParams, Rows: NoRows}, PrepareOptions{})
	_, err = BindOwned(write, []Value{IntValue(1), IntValue(2)})
	require.Error(t, err, "invalid batch entry must fail during binding")
}

// TestCreateSessionTimeout tests to make sure the CreateSession function timeouts out correctly
// and prevents an infinite loop of connection retries.
func TestCreateSessionTimeout(t *testing.T) {
	t.Parallel()
	config := createV2Cluster()
	config.ContactPoints = []string{"127.0.0.1:1"}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	start := time.Now()
	session, err := Connect(ctx, config)
	if err == nil {
		session.Close()
		t.Fatal("expected startup failure")
	}
	require.Less(t, time.Since(start), 2*time.Second)
}

// TestReconnection verifies that a node marked down is eventually reconnected.
// WARNING: This test must NOT use t.Parallel(). It calls session.handleNodeDown()
// which mutates shared HostInfo state visible to all concurrent sessions.
//
//nolint:paralleltest // mutates shared HostInfo state via handleNodeDown()
func TestReconnection(t *testing.T) {
	config := createV2Cluster(func(c *Config) { c.Reconnect = cassandraReconnect{} })
	session := createV2SessionFromConfig(config, t, false)
	defer session.Close()
	host := session.hostSource.getHostsList()[0]
	session.handleNodeDown(host.ConnectAddress(), host.Port())
	defer host.setState(NodeUp)
	require.Equal(t, NodeDown, host.State())
	require.True(t, pollUntil(31*time.Second, func() bool { return host.State() == NodeUp }), "host did not reconnect")
}

type FullName struct {
	FirstName string
	LastName  string
}

func TestMapScanWithRefMap(t *testing.T) {
	t.Parallel()
	session := createV2Session(t)
	defer session.Close()
	table := testTableName(t)
	cassandraDDL(t, session, fmt.Sprintf("CREATE TABLE %s(testtext text PRIMARY KEY,testfullname text,testint int)", table))
	type input struct {
		Text   string
		Name   FullName
		Number int32
	}
	params := Params(Param("testtext", StringEncoder, func(v input) string { return v.Text }), Param("testfullname", fullNameCodec{}, func(v input) FullName { return v.Name }), Param("testint", Int32, func(v input) int32 { return v.Number }))
	prepared := cassandraPrepared(t, session, Statement[input, Unit]{CQL: fmt.Sprintf("INSERT INTO %s(testtext,testfullname,testint) VALUES(?,?,?)", table), Params: params, Rows: NoRows}, PrepareOptions{})
	_, err := Exec(context.Background(), session, cassandraBound(t, prepared, input{"testtext", FullName{"John", "Doe"}, 100}), Options{})
	require.NoError(t, err)
	type output struct {
		Text   Text
		Name   FullName
		Number int32
	}
	decoder := Row(Column("testtext", TextCodec, func(v *output) *Text { return &v.Text }), Column("testfullname", fullNameCodec{}, func(v *output) *FullName { return &v.Name }), Column("testint", Int32, func(v *output) *int32 { return &v.Number }))
	got := cassandraReadBound(t, session, cassandraUnprepared(t, "SELECT * FROM "+table, decoder), Options{})
	require.Equal(t, "testtext", got.Text.String())
	require.Equal(t, FullName{"John", "Doe"}, got.Name)
	require.Equal(t, int32(100), got.Number)
	cassandraWrite(t, session, fmt.Sprintf("INSERT INTO %s(testtext,testint) VALUES(?,?)", table), TextValue("null-int"), NullValue(IntValue(0).Type()))
	nullable := cassandraPrepared(t, session, Statement[string, Nullable[int32]]{CQL: fmt.Sprintf("SELECT testint FROM %s WHERE testtext=?", table), Params: OneParam(StringEncoder), Rows: OneColumn(NullableCodec(Int32))}, PrepareOptions{})
	require.False(t, cassandraReadBound(t, session, cassandraBound(t, nullable, "null-int"), Options{}).Valid)
}

func TestMapScan(t *testing.T) {
	t.Parallel()
	session := createV2Session(t)
	defer session.Close()
	table := testTableName(t)
	cassandraDDL(t, session, fmt.Sprintf("CREATE TABLE %s(fullname text PRIMARY KEY,age int,address inet,data blob)", table))
	cassandraWrite(t, session, fmt.Sprintf("INSERT INTO %s(fullname,age,address) VALUES(?,?,?)", table), TextValue("Grace Hopper"), IntValue(31), cassandraValue(InetValue(netip.MustParseAddr("10.0.0.1"))))
	cassandraWrite(t, session, fmt.Sprintf("INSERT INTO %s(fullname,age,address,data) VALUES(?,?,?,?)", table), TextValue("Ada Lovelace"), IntValue(30), cassandraValue(InetValue(netip.MustParseAddr("10.0.0.2"))), BlobValue([]byte(`{"foo": "bar"}`)))
	rows := cassandraRows(t, session, cassandraUnprepared(t, "SELECT * FROM "+table, DynamicRows), Options{})
	require.Len(t, rows, 2)
	for i, want := range []struct {
		name    string
		age     int64
		address string
	}{{"Ada Lovelace", 30, "10.0.0.2"}, {"Grace Hopper", 31, "10.0.0.1"}} {
		row := rows[i]
		require.Equal(t, want.name, cassandraText(t, row, "fullname"))
		require.Equal(t, want.age, cassandraInt(t, row, "age"))
		ip, err := cassandraField(t, row, "address").Inet()
		require.NoError(t, err)
		require.Equal(t, want.address, ip.String())
		data := cassandraField(t, row, "data")
		if i == 0 {
			b, e := data.Bytes()
			require.NoError(t, e)
			require.Equal(t, []byte(`{"foo": "bar"}`), b.OwnedBytes())
		} else {
			require.Equal(t, Null, data.State())
		}
	}
}

func TestSliceMap(t *testing.T) {
	t.Parallel()
	session := createV2Session(t)
	defer session.Close()
	table := testTableName(t)
	cassandraDDL(t, session, fmt.Sprintf("CREATE TABLE %s(testuuid timeuuid PRIMARY KEY,testtimestamp timestamp,testvarchar varchar,testbigint bigint,testblob blob,testbool boolean,testfloat float,testdouble double,testint int,testdecimal decimal,testlist list<text>,testset set<int>,testmap map<varchar,varchar>,testvarint varint,testinet inet)", table))
	textType, err := ScalarType(TypeVarchar)
	require.NoError(t, err)
	intType := IntValue(0).Type()
	listType, err := ListType(textType)
	require.NoError(t, err)
	setType, err := SetType(intType)
	require.NoError(t, err)
	varcharType, err := ScalarType(TypeVarchar)
	require.NoError(t, err)
	mapType, err := MapType(varcharType, varcharType)
	require.NoError(t, err)
	bigInt, ok := new(big.Int).SetString("830169365738487321165427203929228", 10)
	require.True(t, ok)
	expected := []ValueField{{"testuuid", cassandraValue(TimeUUIDValue(TimeUUID(NewTimeUUID())))}, {"testtimestamp", TimestampValue(Timestamp(time.Now().UnixMilli()))}, {"testvarchar", TextValue("Test VarChar")}, {"testbigint", BigIntValue(time.Now().Unix())}, {"testblob", BlobValue([]byte("test blob"))}, {"testbool", BoolValue(true)}, {"testfloat", FloatValue(4.564)}, {"testdouble", DoubleValue(4.815162342)}, {"testint", IntValue(2343)}, {"testdecimal", DecimalValue(Decimal{Unscaled: VarintFromBigInt(big.NewInt(100))})}, {"testlist", cassandraValue(ListValue(listType, []Value{TextValue("quux"), TextValue("foo"), TextValue("bar"), TextValue("baz"), TextValue("quux")}))}, {"testset", cassandraValue(SetValue(setType, []Value{IntValue(1), IntValue(2), IntValue(3), IntValue(4), IntValue(5), IntValue(6), IntValue(7), IntValue(8), IntValue(9)}))}, {"testmap", cassandraValue(MapValue(mapType, []ValueEntry{{TextValue("field1"), TextValue("val1")}, {TextValue("field2"), TextValue("val2")}, {TextValue("field3"), TextValue("val3")}}))}, {"testvarint", VarintValue(VarintFromBigInt(bigInt))}, {"testinet", cassandraValue(InetValue(netip.MustParseAddr("213.212.2.19")))}}
	var values []Value
	for _, field := range expected {
		values = append(values, field.Value)
	}
	cassandraWrite(t, session, fmt.Sprintf("INSERT INTO %s(testuuid,testtimestamp,testvarchar,testbigint,testblob,testbool,testfloat,testdouble,testint,testdecimal,testlist,testset,testmap,testvarint,testinet) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", table), values...)
	check := func(row DynamicRow) {
		t.Helper()
		require.Equal(t, len(expected), row.Columns().Len())
		for _, field := range expected {
			actual := cassandraField(t, row, field.Name)
			want, err := EncodeValueOwned(actual.Type(), DynamicCodec, field.Value)
			require.NoError(t, err, field.Name)
			got, err := EncodeValueOwned(actual.Type(), DynamicCodec, actual)
			require.NoError(t, err, field.Name)
			require.Equal(t, want.State(), got.State(), field.Name)
			require.Equal(t, want.Bytes().OwnedBytes(), got.Bytes().OwnedBytes(), field.Name)
		}
	}
	// Preserve the three result-consumption forms as cloned slices, a cursor,
	// and an exactly-one-row read, all with explicit dynamic values.
	bound := cassandraUnprepared(t, "SELECT * FROM "+table, DynamicRows)
	rows := cassandraRows(t, session, bound, Options{})
	require.Len(t, rows, 1)
	check(rows[0])
	cursor, err := Run(context.Background(), session, bound, Options{})
	require.NoError(t, err)
	defer cursor.Close()
	require.True(t, cursor.Next())
	check(cursor.Row())
	require.NoError(t, cursor.Close())
	check(cassandraRead(t, session, "SELECT * FROM "+table))
}

func Test_RetryPolicyIdempotence(t *testing.T) {
	t.Parallel()
	session := createV2Session(t)
	defer session.Close()
	for _, tc := range []struct {
		name       string
		idempotent bool
		attempts   int
	}{{"with retry", true, 6}, {"without retry", false, 1}} {
		t.Run(tc.name, func(t *testing.T) {
			observer := &cassandraObserver{}
			bound := cassandraUnprepared(t, "INSERT INTO gocql_test.not_existing_table(event_id,time,args) VALUES(4,now(),'test')", NoRows)
			_, err := Exec(context.Background(), session, bound, Options{Idempotent: tc.idempotent, Retry: cassandraRetry{}, Consistency: Option[Consistency]{Value: All, Set: true}, Observer: observer})
			require.Error(t, err)
			events, attempts := observer.snapshot()
			require.Len(t, attempts, tc.attempts)
			require.Len(t, events, 1)
		})
	}
}

func TestSmallInt(t *testing.T) {
	t.Parallel()
	session := createV2Session(t)
	defer session.Close()
	table := testTableName(t)
	cassandraDDL(t, session, fmt.Sprintf("CREATE TABLE %s(testsmallint smallint PRIMARY KEY)", table))
	cassandraWrite(t, session, fmt.Sprintf("INSERT INTO %s(testsmallint) VALUES(?)", table), SmallIntValue(2))
	require.Equal(t, int64(2), cassandraInt(t, cassandraRead(t, session, "SELECT * FROM "+table), "testsmallint"))
}

func TestScanWithNilArguments(t *testing.T) {
	t.Parallel()
	session := createV2Session(t)
	defer session.Close()
	table := testTableName(t)
	cassandraDDL(t, session, fmt.Sprintf("CREATE TABLE %s(foo varchar,bar int,PRIMARY KEY(foo,bar))", table))
	for i := int32(1); i <= 20; i++ {
		cassandraWrite(t, session, fmt.Sprintf("INSERT INTO %s(foo,bar) VALUES(?,?)", table), TextValue("squares"), IntValue(i*i))
	}
	type result struct{ N int32 }
	decoder := Row(IgnoreColumn[result]("foo"), Column("bar", Int32, func(r *result) *int32 { return &r.N }))
	prepared := cassandraPrepared(t, session, Statement[string, result]{CQL: fmt.Sprintf("SELECT * FROM %s WHERE foo=?", table), Params: OneParam(StringEncoder), Rows: decoder}, PrepareOptions{})
	rows, err := Run(context.Background(), session, cassandraBound(t, prepared, "squares"), Options{})
	require.NoError(t, err)
	defer rows.Close()
	var sum int32
	for rows.Next() {
		sum += rows.Row().N
	}
	require.NoError(t, rows.Close())
	require.Equal(t, int32(2870), sum)
}

func TestScanCASWithNilArguments(t *testing.T) {
	t.Parallel()
	session := createV2SessionFromConfig(createV2Cluster(), t, true)
	defer session.Close()
	table := testTableName(t)
	cassandraDDL(t, session, fmt.Sprintf("CREATE TABLE %s(foo varchar,bar varchar,PRIMARY KEY(foo,bar))", table))
	cql := fmt.Sprintf("INSERT INTO %s(foo,bar) VALUES(?,?) IF NOT EXISTS", table)
	values := []Value{TextValue("baz"), TextValue("baz")}
	ignored := Row(IgnoreColumn[Unit]("foo"), IgnoreColumn[Unit]("bar"))
	require.True(t, cassandraReadBound(t, session, cassandraCASBound(t, session, cql, values, ignored), Options{}).Applied)
	type result struct{ Text Text }
	for _, selected := range []string{"foo", "bar"} {
		other := "foo"
		if selected == "foo" {
			other = "bar"
		}
		decoder := Row(Column(selected, TextCodec, func(v *result) *Text { return &v.Text }), IgnoreColumn[result](other))
		got := cassandraReadBound(t, session, cassandraCASBound(t, session, cql, values, decoder), Options{})
		require.False(t, got.Applied)
		require.True(t, got.Previous.Set)
		require.Equal(t, "baz", got.Previous.Value.Text.String())
	}
}

func TestRebindQueryInfo(t *testing.T) {
	t.Parallel()
	session := createV2Session(t)
	defer session.Close()
	table := testTableName(t)
	cassandraDDL(t, session, fmt.Sprintf("CREATE TABLE %s(id int PRIMARY KEY,value text)", table))
	cassandraWrite(t, session, fmt.Sprintf("INSERT INTO %s(id,value) VALUES(?,?)", table), IntValue(23), TextValue("quux"))
	cassandraWrite(t, session, fmt.Sprintf("INSERT INTO %s(id,value) VALUES(?,?)", table), IntValue(24), TextValue("w00t"))
	prepared := cassandraPrepared(t, session, Statement[int32, Text]{CQL: fmt.Sprintf("SELECT value FROM %s WHERE id=?", table), Params: OneParam(Int32), Rows: OneColumn(TextCodec)}, PrepareOptions{})
	first := cassandraBound(t, prepared, int32(23))
	second := cassandraBound(t, prepared, int32(24))
	require.Equal(t, "quux", cassandraReadBound(t, session, first, Options{}).String())
	require.Equal(t, "w00t", cassandraReadBound(t, session, second, Options{}).String())
	require.Equal(t, "quux", cassandraReadBound(t, session, first, Options{}).String())
}

// TestStaticQueryInfo makes sure that the application can manually bind query parameters using the simplest possible static binding strategy
func TestStaticQueryInfo(t *testing.T) {
	t.Parallel()
	session := createV2Session(t)
	defer session.Close()
	table := testTableName(t)
	cassandraDDL(t, session, fmt.Sprintf("CREATE TABLE %s(id int PRIMARY KEY,value text)", table))
	cassandraWrite(t, session, fmt.Sprintf("INSERT INTO %s(id,value) VALUES(?,?)", table), IntValue(113), TextValue("foo"))
	prepared := cassandraPrepared(t, session, Statement[int32, DynamicRow]{CQL: fmt.Sprintf("SELECT id,value FROM %s WHERE id=?", table), Params: OneParam(Int32), Rows: DynamicRows}, PrepareOptions{})
	require.Equal(t, 1, prepared.Parameters().Len())
	bound := cassandraBound(t, prepared, int32(113))
	for i := 0; i < 2; i++ {
		row := cassandraReadBound(t, session, bound, Options{})
		require.Equal(t, int64(113), cassandraInt(t, row, "id"))
		require.Equal(t, "foo", cassandraText(t, row, "value"))
	}
}

type ClusteredKeyValue struct {
	Id      int32
	Cluster int32
	Value   string
}

// TestBoundQueryInfo makes sure that the application can manually bind query parameters using the query meta data supplied at runtime
func TestBoundQueryInfo(t *testing.T) {
	t.Parallel()
	session := createV2Session(t)
	defer session.Close()
	table := testTableName(t)
	cassandraDDL(t, session, fmt.Sprintf("CREATE TABLE %s(id int,cluster int,value text,PRIMARY KEY(id,cluster))", table))
	params := Params(Param("value", StringEncoder, func(v ClusteredKeyValue) string { return v.Value }), Param("cluster", Int32, func(v ClusteredKeyValue) int32 { return v.Cluster }), Param("id", Int32, func(v ClusteredKeyValue) int32 { return v.Id }))
	prepared := cassandraPrepared(t, session, Statement[ClusteredKeyValue, Unit]{CQL: fmt.Sprintf("INSERT INTO %s(id,cluster,value) VALUES(?,?,?)", table), Params: params, Rows: NoRows}, PrepareOptions{})
	require.Equal(t, 3, prepared.Parameters().Len())
	_, err := Exec(context.Background(), session, cassandraBound(t, prepared, ClusteredKeyValue{200, 300, "baz"}), Options{})
	require.NoError(t, err)
	readParams := Params(Param("cluster", Int32, func(v ClusteredKeyValue) int32 { return v.Cluster }), Param("id", Int32, func(v ClusteredKeyValue) int32 { return v.Id }))
	read := cassandraPrepared(t, session, Statement[ClusteredKeyValue, DynamicRow]{CQL: fmt.Sprintf("SELECT id,cluster,value FROM %s WHERE id=? AND cluster=?", table), Params: readParams, Rows: DynamicRows}, PrepareOptions{})
	row := cassandraReadBound(t, session, cassandraBound(t, read, ClusteredKeyValue{Id: 200, Cluster: 300}), Options{})
	require.Equal(t, "baz", cassandraText(t, row, "value"))
	require.Equal(t, int64(200), cassandraInt(t, row, "id"))
	require.Equal(t, int64(300), cassandraInt(t, row, "cluster"))
}

// TestBatchQueryInfo makes sure that the application can manually bind query parameters when executing in a batch
func TestBatchQueryInfo(t *testing.T) {
	t.Parallel()
	session := createV2Session(t)
	defer session.Close()
	table := testTableName(t)
	cassandraDDL(t, session, fmt.Sprintf("CREATE TABLE %s(id int,cluster int,value text,PRIMARY KEY(id,cluster))", table))
	params := Params(Param("id", Int32, func(v ClusteredKeyValue) int32 { return v.Id }), Param("cluster", Int32, func(v ClusteredKeyValue) int32 { return v.Cluster }), Param("value", StringEncoder, func(v ClusteredKeyValue) string { return v.Value }))
	prepared := cassandraPrepared(t, session, Statement[ClusteredKeyValue, Unit]{CQL: fmt.Sprintf("INSERT INTO %s(id,cluster,value) VALUES(?,?,?)", table), Params: params, Rows: NoRows}, PrepareOptions{})
	require.Equal(t, 3, prepared.Parameters().Len())
	bound := cassandraBound(t, prepared, ClusteredKeyValue{4000, 5000, "bar"})
	cassandraBatch(t, session, LoggedBatch, []BatchItem{cassandraItem(t, bound)}, Options{})
	read := cassandraDynamicBound(t, session, fmt.Sprintf("SELECT id,cluster,value FROM %s WHERE id=? AND cluster=?", table), []Value{IntValue(4000), IntValue(5000)}, PrepareOptions{})
	row := cassandraReadBound(t, session, read, Options{})
	require.Equal(t, "bar", cassandraText(t, row, "value"))
	require.Equal(t, int64(4000), cassandraInt(t, row, "id"))
	require.Equal(t, int64(5000), cassandraInt(t, row, "cluster"))
}

// hostConnWaitTimeout bounds the waits below. Generous on purpose: exceeding it means
// the pools never came up, which is a failure worth reporting rather than a slow start.
const hostConnWaitTimeout = 15 * time.Second

// pollUntil calls cond every 50ms until it holds or timeout elapses.
func pollUntil(timeout time.Duration, cond func() bool) bool {
	deadline := time.After(timeout)
	for {
		if cond() {
			return true
		}

		select {
		case <-deadline:
			return false
		case <-time.After(50 * time.Millisecond):
		}
	}
}

// waitForHostConns returns one pooled connection per host the session considers up,
// waiting until every one of them has yielded a connection.
//
// createV2Session waits for schema agreement, not for connections, and hostConnPool.pickable
// reports false while a pool is still empty -- it only schedules the fill. A host walk run
// straight after createV2Session therefore skips whichever pools have not come up yet, which
// silently narrows a per-host assertion down to the node that answered first.
//
// The host list is re-read on every attempt, so a host marked up while this waits is
// included and one marked down stops being required.
func waitForHostConns(t *testing.T, session *Session) map[string]*Conn {
	t.Helper()

	conns := make(map[string]*Conn)
	var pending, down []string

	ok := pollUntil(hostConnWaitTimeout, func() bool {
		clear(conns)
		pending = pending[:0]
		down = down[:0]

		for _, host := range session.hostSource.getHostsList() {
			// Collected rather than merely skipped: a host that stays down keeps the
			// count below *clusterSize while contributing nothing to pending, so the
			// failure below would otherwise report "2 of 3 connected, 0 still without
			// a connection: []" and name no address to go and look at.
			if !host.IsUp() {
				down = append(down, host.ConnectAddressAndPort())
				continue
			}

			addr := host.ConnectAddressAndPort()
			pool, found := session.pool.getPool(host)
			if !found {
				pending = append(pending, addr)
				continue
			}

			if conn := pool.Pick(nil, nil); conn != nil {
				conns[addr] = conn
			} else {
				pending = append(pending, addr)
			}
		}

		// Against *clusterSize rather than "at least one": hostSource discovers peers
		// asynchronously too, so early on the list itself is short and every host in it
		// can have a connection while two nodes are not represented at all. Waiting for
		// len(pending) == 0 alone would be satisfied by that, and reintroduce one level
		// up exactly the "passed having checked one of three" hole this closes.
		//
		// >= rather than ==, so a session that has discovered more up hosts than
		// -clusterSize claims still converges instead of polling to the deadline.
		return len(pending) == 0 && len(conns) >= *clusterSize
	})

	if !ok {
		t.Fatalf("after %s the session had %d of %d hosts connected; up host(s) still without a connection: %v; host(s) marked down: %v",
			hostConnWaitTimeout, len(conns), *clusterSize, pending, down)
	}

	return conns
}

// getRandomConn returns a pooled connection, waiting for one rather than failing the
// moment none is pickable. See waitForHostConns: an empty pool straight after
// createV2Session is a pool still filling, not an anomaly.
func getRandomConn(t *testing.T, session *Session) *Conn {
	t.Helper()

	var conn *Conn
	if !pollUntil(hostConnWaitTimeout, func() bool {
		conn = session.getConn()
		return conn != nil
	}) {
		t.Fatal("unable to get a connection")
	}
	return conn
}

func injectInvalidPreparedStatement(t *testing.T, session *Session, table string) (*Bound[Unit], *Conn) {
	t.Helper()
	cassandraDDL(t, session, fmt.Sprintf("CREATE TABLE %s(foo varchar,bar int,PRIMARY KEY(foo,bar))", table))
	conn := getRandomConn(t, session)
	prepared := cassandraPrepared(t, session, Statement[string, Unit]{CQL: fmt.Sprintf("INSERT INTO %s(foo,bar) VALUES(?,7)", table), Params: OneParam(StringEncoder), Rows: NoRows}, PrepareOptions{HostID: Option[UUID]{Value: conn.host.hostUUID(), Set: true}, connection: conn})
	prepared.state.mu.Lock()
	host := prepared.state.hosts[conn.host.hostUUID()]
	host.id = []byte("foobar")
	prepared.state.hosts[conn.host.hostUUID()] = host
	prepared.state.mu.Unlock()
	return cassandraBound(t, prepared, "bar"), conn
}

func TestPrepare_MissingSchemaPrepare(t *testing.T) {
	t.Parallel()
	session := createV2Session(t)
	defer session.Close()
	table := testTableName(t)
	conn := getRandomConn(t, session)
	statement := Statement[int32, Unit]{CQL: fmt.Sprintf("INSERT INTO %s(val) VALUES(?)", table), Params: OneParam(Int32), Rows: NoRows}
	options := PrepareOptions{HostID: Option[UUID]{Value: conn.host.hostUUID(), Set: true}}
	_, err := Prepare(context.Background(), session, statement, options)
	require.Error(t, err)
	cassandraDDL(t, session, fmt.Sprintf("CREATE TABLE %s(val int PRIMARY KEY)", table))
	prepared := cassandraPrepared(t, session, statement, options)
	_, err = Exec(context.Background(), session, cassandraBound(t, prepared, int32(5)), Options{HostID: options.HostID})
	require.NoError(t, err)
}

func TestPrepare_ReprepareStatement(t *testing.T) {
	t.Parallel()
	session := createV2Session(t)
	defer session.Close()
	table := testTableName(t)
	bound, conn := injectInvalidPreparedStatement(t, session, table)
	_, err := Exec(context.Background(), session, bound, Options{HostID: Option[UUID]{Value: conn.host.hostUUID(), Set: true}, connection: conn})
	require.NoError(t, err)
	bound.request.prepared.mu.RLock()
	id := append([]byte(nil), bound.request.prepared.hosts[conn.host.hostUUID()].id...)
	bound.request.prepared.mu.RUnlock()
	require.NotEqual(t, []byte("foobar"), id)
	require.Equal(t, int64(7), cassandraInt(t, cassandraRead(t, session, "SELECT bar FROM "+table+" WHERE foo=?", TextValue("bar")), "bar"))
}

func TestPrepare_ReprepareBatch(t *testing.T) {
	t.Parallel()
	session := createV2Session(t)
	defer session.Close()
	table := testTableName(t)
	bound, conn := injectInvalidPreparedStatement(t, session, table)
	cassandraBatch(t, session, UnloggedBatch, []BatchItem{cassandraItem(t, bound)}, Options{HostID: Option[UUID]{Value: conn.host.hostUUID(), Set: true}, connection: conn})
	bound.request.prepared.mu.RLock()
	id := append([]byte(nil), bound.request.prepared.hosts[conn.host.hostUUID()].id...)
	bound.request.prepared.mu.RUnlock()
	require.NotEqual(t, []byte("foobar"), id)
	require.Equal(t, int64(7), cassandraInt(t, cassandraRead(t, session, "SELECT bar FROM "+table+" WHERE foo=?", TextValue("bar")), "bar"))
}

func TestQueryInfo(t *testing.T) {
	t.Parallel()
	session := createV2Session(t)
	defer session.Close()
	prepared := cassandraPrepared(t, session, Statement[string, DynamicRow]{CQL: "SELECT release_version,host_id FROM system.local WHERE key=?", Params: OneParam(StringEncoder), Rows: DynamicRows}, PrepareOptions{})
	require.Equal(t, 1, prepared.Parameters().Len())
	require.Equal(t, 2, prepared.Columns().Len())
}

// TestPreparedCacheEviction will make sure that the cache size is maintained
func TestPrepare_PreparedCacheEviction(t *testing.T) {
	t.Parallel()
	const maxPrepared = 4
	session := createV2Session(t, func(c *Config) { c.PreparedCacheSize = maxPrepared; c.Metadata.SchemaEvents = false })
	defer session.Close()
	table := testTableName(t)
	cassandraDDL(t, session, fmt.Sprintf("CREATE TABLE %s(id int PRIMARY KEY,mod int)", table))
	runtime := session.typedRuntime()
	runtime.mu.Lock()
	clear(runtime.prepared)
	runtime.prepareOrder = nil
	runtime.mu.Unlock()
	insert := fmt.Sprintf("INSERT INTO %s(id,mod) VALUES(?,?)", table)
	for i := int32(0); i < 2; i++ {
		cassandraWrite(t, session, insert, IntValue(i), IntValue(10000%(i+1)))
	}
	selects := []string{fmt.Sprintf("SELECT id,mod FROM %s WHERE id=0", table), fmt.Sprintf("SELECT id,mod FROM %s WHERE id=1", table)}
	for _, cql := range selects {
		prepared := cassandraPrepared(t, session, Statement[Unit, DynamicRow]{CQL: cql, Params: NoParams, Rows: DynamicRows}, PrepareOptions{})
		cassandraReadBound(t, session, cassandraBound(t, prepared, Unit{}), Options{})
	}
	update := fmt.Sprintf("UPDATE %s SET mod=? WHERE id=?", table)
	cassandraWrite(t, session, update, IntValue(1), IntValue(11))
	del := fmt.Sprintf("DELETE FROM %s WHERE id=?", table)
	cassandraWrite(t, session, del, IntValue(1))
	cassandraWrite(t, session, insert, IntValue(3), IntValue(11))
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	require.Len(t, runtime.prepared, maxPrepared)
	cached := map[string]bool{}
	for key := range runtime.prepared {
		cached[key.cql] = true
	}
	require.False(t, cached[selects[0]])
	for _, cql := range []string{selects[1], insert, update, del} {
		require.True(t, cached[cql], "expected cached %q", cql)
	}
}

func TestPrepare_PreparedCacheKey(t *testing.T) {
	t.Parallel()
	session := createV2Session(t)
	defer session.Close()
	table := testTableName(t)
	ks := testKeyspaceName(t, "ks2")
	other := cassandraOtherSession(t, session, ks)
	defer other.Close()
	defer cassandraDDL(t, session, "DROP KEYSPACE "+ks)
	for _, s := range []*Session{session, other} {
		cassandraDDL(t, s, fmt.Sprintf("CREATE TABLE %s(id varchar PRIMARY KEY,field varchar)", table))
	}
	cassandraWrite(t, session, fmt.Sprintf("INSERT INTO %s(id,field) VALUES(?,?)", table), TextValue("key"), TextValue("one"))
	cassandraWrite(t, other, fmt.Sprintf("INSERT INTO %s(id,field) VALUES(?,?)", table), TextValue("key"), TextValue("two"))
	cql := fmt.Sprintf("SELECT field FROM %s WHERE id=?", table)
	require.Equal(t, "one", cassandraText(t, cassandraRead(t, session, cql, TextValue("key")), "field"))
	require.Equal(t, "two", cassandraText(t, cassandraRead(t, other, cql, TextValue("key")), "field"))
}

// TestMarshalFloat64Ptr tests to see that a pointer to a float64 is marshalled correctly.
func TestMarshalFloat64Ptr(t *testing.T) {
	t.Parallel()
	session := createV2Session(t)
	defer session.Close()
	table := testTableName(t)
	cassandraDDL(t, session, fmt.Sprintf("CREATE TABLE %s(id double PRIMARY KEY,test double)", table))
	testNum := 7500.0
	params := Params(Param("id", Float64, func(v *float64) float64 { return *v }), Param("test", Float64, func(v *float64) float64 { return *v }))
	prepared := cassandraPrepared(t, session, Statement[*float64, Unit]{CQL: fmt.Sprintf("INSERT INTO %s(id,test) VALUES(?,?)", table), Params: params, Rows: NoRows}, PrepareOptions{})
	_, err := Exec(context.Background(), session, cassandraBound(t, prepared, &testNum), Options{})
	require.NoError(t, err)
	got, err := cassandraField(t, cassandraRead(t, session, "SELECT test FROM "+table), "test").Float()
	require.NoError(t, err)
	require.Equal(t, testNum, got)
}

// TestMarshalInet tests to see that a pointer to a float64 is marshalled correctly.
func TestMarshalInet(t *testing.T) {
	t.Parallel()
	session := createV2Session(t)
	defer session.Close()
	table := testTableName(t)
	cassandraDDL(t, session, fmt.Sprintf("CREATE TABLE %s(ip inet PRIMARY KEY,name text)", table))
	ipFromNet, ok := netip.AddrFromSlice(net.ParseIP("222.43.54.65"))
	require.True(t, ok)
	for _, want := range []netip.Addr{netip.MustParseAddr("123.34.45.56"), ipFromNet.Unmap()} {
		cassandraWrite(t, session, fmt.Sprintf("INSERT INTO %s(ip,name) VALUES(?,?)", table), cassandraValue(InetValue(want)), TextValue("Test IP"))
		for i := 0; i < 2; i++ {
			row := cassandraRead(t, session, "SELECT ip FROM "+table)
			got, err := cassandraField(t, row, "ip").Inet()
			require.NoError(t, err)
			require.Equal(t, want.String(), got.String())
			require.Equal(t, want, got)
		}
		cassandraWrite(t, session, fmt.Sprintf("DELETE FROM %s WHERE ip=?", table), cassandraValue(InetValue(want)))
	}
}

func TestVarint(t *testing.T) {
	t.Parallel()
	session := createV2Session(t)
	defer session.Close()
	table := testTableName(t)
	cassandraDDL(t, session, fmt.Sprintf("CREATE TABLE %s(id varchar PRIMARY KEY,test varint,test2 varint)", table))
	insert := fmt.Sprintf("INSERT INTO %s(id,test) VALUES(?,?)", table)
	selectCQL := "SELECT test FROM " + table
	for _, want := range []int64{0, -1, int64(math.MaxInt32) + 1} {
		cassandraWrite(t, session, insert, TextValue("id"), VarintValue(VarintFromBigInt(big.NewInt(want))))
		row := cassandraRead(t, session, selectCQL)
		got, err := cassandraField(t, row, "test").Varint()
		require.NoError(t, err)
		require.Equal(t, want, got.BigInt().Int64())
	}
	cassandraWrite(t, session, insert, TextValue("id"), NullValue(VarintValue(Varint{}).Type()))
	nullable := cassandraUnprepared(t, selectCQL, OneColumn(NullableCodec(VarintCodec)))
	null := cassandraReadBound(t, session, nullable, Options{})
	require.False(t, null.Valid)
	require.Zero(t, null.Value.BigInt().Sign())
	biggie, ok := new(big.Int).SetString("36893488147419103232", 10)
	require.True(t, ok)
	cassandraWrite(t, session, insert, TextValue("id"), VarintValue(VarintFromBigInt(biggie)))
	got, err := cassandraField(t, cassandraRead(t, session, selectCQL), "test").Varint()
	require.NoError(t, err)
	require.Equal(t, biggie.String(), got.BigInt().String())
	rows, err := Run(context.Background(), session, cassandraUnprepared(t, selectCQL, OneColumn(cassandraVarintInt64{})), Options{})
	require.NoError(t, err)
	defer rows.Close()
	require.False(t, rows.Next())
	require.ErrorContains(t, rows.Close(), "int64 range")
	absent := cassandraReadBound(t, session, cassandraUnprepared(t, "SELECT test2 FROM "+table, OneColumn(NullableCodec(VarintCodec))), Options{})
	require.False(t, absent.Valid)
	require.Zero(t, absent.Value.BigInt().Sign())
}

// TestQueryStats confirms that the stats are returning valid data. Accuracy may be questionable.
func TestQueryStats(t *testing.T) {
	t.Parallel()
	session := createV2Session(t)
	defer session.Close()
	before := session.Metrics()
	rows, err := v2Query(context.Background(), session, "SELECT * FROM system.peers")
	require.NoError(t, err)
	for rows.Next() {
	}
	require.NoError(t, rows.Close())
	info := rows.Info()
	require.Positive(t, info.Attempts)
	require.Positive(t, info.Latency)
	require.Greater(t, session.Metrics().Requests, before.Requests)
}

// TestIterHosts confirms that host is added to Iter when the query succeeds.
func TestIterHost(t *testing.T) {
	t.Parallel()
	session := createV2Session(t)
	defer session.Close()
	rows, err := v2Query(context.Background(), session, "SELECT * FROM system.peers")
	require.NoError(t, err)
	defer rows.Close()
	require.NotEqual(t, UUID{}, rows.Info().HostID)
}

// TestBatchStats confirms that the stats are returning valid data. Accuracy may be questionable.
func TestBatchStats(t *testing.T) {
	t.Parallel()
	session := createV2Session(t)
	defer session.Close()
	table := testTableName(t)
	cassandraDDL(t, session, fmt.Sprintf("CREATE TABLE %s(id int PRIMARY KEY)", table))
	var items []BatchItem
	for _, id := range []int32{1, 2} {
		items = append(items, cassandraItem(t, cassandraWriteBound(t, session, fmt.Sprintf("INSERT INTO %s(id) VALUES(?)", table), []Value{IntValue(id)}, PrepareOptions{})))
	}
	info := cassandraBatch(t, session, LoggedBatch, items, Options{})
	require.Positive(t, info.Attempts)
	require.Positive(t, info.Latency)
}

func TestBatchObserve(t *testing.T) {
	t.Parallel()
	session := createV2Session(t)
	defer session.Close()
	table := testTableName(t)
	cassandraDDL(t, session, fmt.Sprintf("CREATE TABLE %s(id int PRIMARY KEY,other int)", table))
	observer := &cassandraObserver{}
	var items []BatchItem
	var cqls []string
	for i := int32(0); i < 100; i++ {
		cql := fmt.Sprintf("INSERT INTO %s(id,other) VALUES(?,%d)", table, i)
		cqls = append(cqls, cql)
		items = append(items, cassandraItem(t, cassandraWriteBound(t, session, cql, []Value{IntValue(i)}, PrepareOptions{})))
	}
	cassandraBatch(t, session, LoggedBatch, items, Options{Observer: observer})
	events, _ := observer.snapshot()
	require.Len(t, events, 1)
	require.NoError(t, events[0].Err)
	require.Equal(t, "gocql_test", events[0].Routing.Keyspace)
	require.Equal(t, cqls, events[0].BatchStatements)
	// Observations expose typed parameter descriptors, never retained input
	// payloads. Verify each bound value by reading the resulting rows.
	rows := cassandraRows(t, session, cassandraUnprepared(t, "SELECT id,other FROM "+table, DynamicRows), Options{})
	require.Len(t, rows, 100)
	for _, row := range rows {
		require.Equal(t, cassandraInt(t, row, "id"), cassandraInt(t, row, "other"))
	}
}

// TestNilInQuery tests to see that a nil value passed to a query is handled by Cassandra
// TODO validate the nil value by reading back the nil. Need to fix Unmarshalling.
func TestNilInQuery(t *testing.T) {
	t.Parallel()
	session := createV2Session(t)
	defer session.Close()
	table := testTableName(t)
	cassandraDDL(t, session, fmt.Sprintf("CREATE TABLE %s(id int PRIMARY KEY,count int)", table))
	cassandraWrite(t, session, fmt.Sprintf("INSERT INTO %s(id,count) VALUES(?,?)", table), IntValue(1), NullValue(IntValue(0).Type()))
	row := cassandraRead(t, session, "SELECT id,count FROM "+table)
	require.Equal(t, int64(1), cassandraInt(t, row, "id"))
	require.Equal(t, Null, cassandraField(t, row, "count").State())
}

// Don't initialize time.Time bind variable if cassandra timestamp column is empty
func TestEmptyTimestamp(t *testing.T) {
	t.Parallel()
	session := createV2Session(t)
	defer session.Close()
	table := testTableName(t)
	cassandraDDL(t, session, fmt.Sprintf("CREATE TABLE %s(id int PRIMARY KEY,time timestamp,num int)", table))
	cassandraWrite(t, session, fmt.Sprintf("INSERT INTO %s(id,num) VALUES(?,?)", table), IntValue(1), IntValue(561))
	read := cassandraPrepared(t, session, Statement[int32, Nullable[Timestamp]]{CQL: fmt.Sprintf("SELECT time FROM %s WHERE id=?", table), Params: OneParam(Int32), Rows: OneColumn(NullableCodec(TimestampCodec))}, PrepareOptions{})
	value := cassandraReadBound(t, session, cassandraBound(t, read, int32(1)), Options{})
	require.False(t, value.Valid)
	require.Zero(t, value.Value)
}

// Integration test of just querying for data from the system.schema_keyspace table where the keyspace DOES exist.
func TestGetKeyspaceMetadata(t *testing.T) {
	t.Parallel()

	session := createV2Session(t)
	defer session.Close()

	keyspaceMetadata, err := cassandraKeyspace(session, "gocql_test")
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

func TestSessionMetadataAPIs(t *testing.T) {
	t.Parallel()

	session := createV2Session(t)
	defer session.Close()

	const ks = "gocql_test"

	if _, err := cassandraKeyspace(session, ks); err != nil {
		t.Fatalf("failed to get initial keyspace metadata: %v", err)
	}

	waitForSchemaRefresh := func() {
		if err := session.AwaitSchemaAgreement(context.Background()); err != nil {
			t.Logf("schema agreement warning: %v", err)
		}
		require.NoError(t, session.RefreshMetadata(context.Background()))
	}

	t.Run("TableMetadata", func(t *testing.T) {
		t.Run("basic_table_after_create", func(t *testing.T) {
			table := testTableName(t)
			if err := v2CreateTable(context.Background(), session, fmt.Sprintf(
				"CREATE TABLE IF NOT EXISTS %s.%s (pk int PRIMARY KEY, v int)", ks, table)); err != nil {
				t.Fatalf("create table: %v", err)
			}
			defer v2Exec(context.Background(), session, fmt.Sprintf("DROP TABLE IF EXISTS %s.%s", ks, table))

			waitForSchemaRefresh()

			tm, err := cassandraTable(session, ks, table)
			if err != nil {
				t.Fatalf("TableMetadata failed: %v", err)
			}
			if tm.Name != table {
				t.Errorf("expected table name %q, got %q", table, tm.Name)
			}
			if tm.Keyspace != ks {
				t.Errorf("expected keyspace %q, got %q", ks, tm.Keyspace)
			}
		})

		t.Run("columns_and_partition_key", func(t *testing.T) {
			table := testTableName(t)
			if err := v2CreateTable(context.Background(), session, fmt.Sprintf(
				"CREATE TABLE IF NOT EXISTS %s.%s (pk1 int, pk2 text, ck int, val blob, PRIMARY KEY ((pk1, pk2), ck))", ks, table)); err != nil {
				t.Fatalf("create table: %v", err)
			}
			defer v2Exec(context.Background(), session, fmt.Sprintf("DROP TABLE IF EXISTS %s.%s", ks, table))

			waitForSchemaRefresh()

			tm, err := cassandraTable(session, ks, table)
			if err != nil {
				t.Fatalf("TableMetadata failed: %v", err)
			}

			if len(tm.PartitionKey) != 2 {
				t.Fatalf("expected 2 partition key columns, got %d", len(tm.PartitionKey))
			}
			if tm.PartitionKey[0].Name != "pk1" || tm.PartitionKey[1].Name != "pk2" {
				t.Errorf("unexpected partition key columns: %v, %v", tm.PartitionKey[0].Name, tm.PartitionKey[1].Name)
			}

			if len(tm.ClusteringColumns) != 1 || tm.ClusteringColumns[0].Name != "ck" {
				t.Errorf("expected clustering column 'ck', got %v", tm.ClusteringColumns)
			}

			for _, col := range []string{"pk1", "pk2", "ck", "val"} {
				if _, ok := tm.Columns[col]; !ok {
					t.Errorf("expected column %q in metadata", col)
				}
			}
		})

		t.Run("with_secondary_index", func(t *testing.T) {
			if isTabletsSupported() {
				t.Skip("secondary indexes are not supported on tables with tablets")
			}

			table := testTableName(t)
			if err := v2CreateTable(context.Background(), session, fmt.Sprintf(
				"CREATE TABLE IF NOT EXISTS %s.%s (pk int PRIMARY KEY, v int)", ks, table)); err != nil {
				t.Fatalf("create table: %v", err)
			}
			defer v2Exec(context.Background(), session, fmt.Sprintf("DROP TABLE IF EXISTS %s.%s", ks, table))

			idxName := table + "_v_idx"
			if err := v2CreateTable(context.Background(), session, fmt.Sprintf(
				"CREATE INDEX IF NOT EXISTS %s ON %s.%s (v)", idxName, ks, table)); err != nil {
				t.Fatalf("create index: %v", err)
			}

			waitForSchemaRefresh()

			require.NoError(t, session.RefreshMetadata(context.Background()))
			km, err := cassandraKeyspace(session, ks)
			if err != nil {
				t.Fatalf("KeyspaceMetadata failed: %v", err)
			}
			if _, ok := km.Indexes[idxName]; !ok {
				t.Errorf("expected index %q in keyspace metadata indexes", idxName)
			}
		})

		t.Run("with_materialized_view", func(t *testing.T) {
			if flagCassVersion.Before(3, 0, 0) {
				t.Skip("materialized views require Cassandra 3.0+")
			}
			if isTabletsSupported() {
				t.Skip("materialized views are not supported on tables with tablets")
			}

			baseTable := testTableName(t, "base")
			viewName := testTableName(t, "view")
			if err := v2CreateTable(context.Background(), session, fmt.Sprintf(
				"CREATE TABLE IF NOT EXISTS %s.%s (pk int, ck int, v int, PRIMARY KEY (pk, ck))", ks, baseTable)); err != nil {
				t.Fatalf("create base table: %v", err)
			}
			defer v2Exec(context.Background(), session, fmt.Sprintf("DROP MATERIALIZED VIEW IF EXISTS %s.%s", ks, viewName))
			defer v2Exec(context.Background(), session, fmt.Sprintf("DROP TABLE IF EXISTS %s.%s", ks, baseTable))

			if err := v2CreateTable(context.Background(), session, fmt.Sprintf(
				"CREATE MATERIALIZED VIEW IF NOT EXISTS %s.%s AS SELECT pk, ck, v FROM %s.%s WHERE pk IS NOT NULL AND ck IS NOT NULL AND v IS NOT NULL PRIMARY KEY (v, pk, ck)",
				ks, viewName, ks, baseTable)); err != nil {
				t.Fatalf("create materialized view: %v", err)
			}

			waitForSchemaRefresh()

			tm, err := cassandraTable(session, ks, baseTable)
			if err != nil {
				t.Fatalf("TableMetadata for base table failed: %v", err)
			}
			if tm.Name != baseTable {
				t.Errorf("expected table name %q, got %q", baseTable, tm.Name)
			}

			require.NoError(t, session.RefreshMetadata(context.Background()))
			km, err := cassandraKeyspace(session, ks)
			if err != nil {
				t.Fatalf("KeyspaceMetadata failed: %v", err)
			}
			if _, ok := km.Views[viewName]; !ok {
				t.Errorf("expected view %q in keyspace metadata", viewName)
			}
			if km.Views[viewName].BaseTableName != baseTable {
				t.Errorf("expected view base table %q, got %q", baseTable, km.Views[viewName].BaseTableName)
			}
		})

		t.Run("after_alter_table", func(t *testing.T) {
			table := testTableName(t)
			if err := v2CreateTable(context.Background(), session, fmt.Sprintf(
				"CREATE TABLE IF NOT EXISTS %s.%s (pk int PRIMARY KEY, v int)", ks, table)); err != nil {
				t.Fatalf("create table: %v", err)
			}
			defer v2Exec(context.Background(), session, fmt.Sprintf("DROP TABLE IF EXISTS %s.%s", ks, table))

			if err := v2CreateTable(context.Background(), session, fmt.Sprintf(
				"ALTER TABLE %s.%s ADD v2 text", ks, table)); err != nil {
				t.Fatalf("alter table: %v", err)
			}

			waitForSchemaRefresh()

			tm, err := cassandraTable(session, ks, table)
			if err != nil {
				t.Fatalf("TableMetadata failed: %v", err)
			}
			if _, ok := tm.Columns["v2"]; !ok {
				t.Errorf("expected column 'v2' after ALTER TABLE, got columns: %v", columnNames(tm.Columns))
			}
		})

		t.Run("after_drop_and_recreate", func(t *testing.T) {
			table := testTableName(t)
			if err := v2CreateTable(context.Background(), session, fmt.Sprintf(
				"CREATE TABLE IF NOT EXISTS %s.%s (pk int PRIMARY KEY, v int)", ks, table)); err != nil {
				t.Fatalf("create table: %v", err)
			}

			waitForSchemaRefresh()

			if _, err := cassandraTable(session, ks, table); err != nil {
				t.Fatalf("TableMetadata before drop failed: %v", err)
			}

			if err := v2CreateTable(context.Background(), session, fmt.Sprintf("DROP TABLE %s.%s", ks, table)); err != nil {
				t.Fatalf("drop table: %v", err)
			}
			if err := v2CreateTable(context.Background(), session, fmt.Sprintf(
				"CREATE TABLE %s.%s (pk text PRIMARY KEY, new_col int)", ks, table)); err != nil {
				t.Fatalf("recreate table: %v", err)
			}
			defer v2Exec(context.Background(), session, fmt.Sprintf("DROP TABLE IF EXISTS %s.%s", ks, table))

			waitForSchemaRefresh()

			tm, err := cassandraTable(session, ks, table)
			if err != nil {
				t.Fatalf("TableMetadata after recreate failed: %v", err)
			}
			if _, ok := tm.Columns["new_col"]; !ok {
				t.Errorf("expected column 'new_col' after recreate, got columns: %v", columnNames(tm.Columns))
			}
			if _, ok := tm.Columns["v"]; ok {
				t.Errorf("old column 'v' should not exist after recreate")
			}
		})

		t.Run("nonexistent_table", func(t *testing.T) {
			km, found := session.Metadata().Keyspace(ks)
			require.True(t, found)
			_, found = km.Tables["does_not_exist_at_all"]
			require.False(t, found)
		})

		t.Run("empty_table_name", func(t *testing.T) {
			km, found := session.Metadata().Keyspace(ks)
			require.True(t, found)
			_, found = km.Tables[""]
			require.False(t, found)
		})

		t.Run("empty_keyspace", func(t *testing.T) { _, found := session.Metadata().Keyspace(""); require.False(t, found) })
	})

	t.Run("KeyspaceMetadata", func(t *testing.T) {
		t.Run("includes_new_table", func(t *testing.T) {
			table := testTableName(t)
			if err := v2CreateTable(context.Background(), session, fmt.Sprintf(
				"CREATE TABLE IF NOT EXISTS %s.%s (pk int PRIMARY KEY, v int)", ks, table)); err != nil {
				t.Fatalf("create table: %v", err)
			}
			defer v2Exec(context.Background(), session, fmt.Sprintf("DROP TABLE IF EXISTS %s.%s", ks, table))

			waitForSchemaRefresh()

			require.NoError(t, session.RefreshMetadata(context.Background()))
			km, err := cassandraKeyspace(session, ks)
			if err != nil {
				t.Fatalf("KeyspaceMetadata failed: %v", err)
			}
			if _, ok := km.Tables[table]; !ok {
				t.Fatalf("expected table %q in keyspace metadata, got tables: %v", table, tableNames(km.Tables))
			}
		})

		t.Run("excludes_dropped_table", func(t *testing.T) {
			table := testTableName(t)
			if err := v2CreateTable(context.Background(), session, fmt.Sprintf(
				"CREATE TABLE IF NOT EXISTS %s.%s (pk int PRIMARY KEY, v int)", ks, table)); err != nil {
				t.Fatalf("create table: %v", err)
			}

			waitForSchemaRefresh()

			require.NoError(t, session.RefreshMetadata(context.Background()))
			km, err := cassandraKeyspace(session, ks)
			if err != nil {
				t.Fatalf("KeyspaceMetadata before drop failed: %v", err)
			}
			if _, ok := km.Tables[table]; !ok {
				t.Fatalf("expected table %q before drop", table)
			}

			if err := v2CreateTable(context.Background(), session, fmt.Sprintf("DROP TABLE %s.%s", ks, table)); err != nil {
				t.Fatalf("drop table: %v", err)
			}

			waitForSchemaRefresh()

			require.NoError(t, session.RefreshMetadata(context.Background()))
			km, err = cassandraKeyspace(session, ks)
			if err != nil {
				t.Fatalf("KeyspaceMetadata after drop failed: %v", err)
			}
			if _, ok := km.Tables[table]; ok {
				t.Errorf("table %q should not appear after DROP", table)
			}
		})

		t.Run("multiple_tables", func(t *testing.T) {
			tables := []string{testTableName(t, "a"), testTableName(t, "b"), testTableName(t, "c")}
			for _, table := range tables {
				if err := v2CreateTable(context.Background(), session, fmt.Sprintf(
					"CREATE TABLE IF NOT EXISTS %s.%s (pk int PRIMARY KEY)", ks, table)); err != nil {
					t.Fatalf("create table %s: %v", table, err)
				}
				defer v2Exec(context.Background(), session, fmt.Sprintf("DROP TABLE IF EXISTS %s.%s", ks, table))
			}

			waitForSchemaRefresh()

			require.NoError(t, session.RefreshMetadata(context.Background()))
			km, err := cassandraKeyspace(session, ks)
			if err != nil {
				t.Fatalf("KeyspaceMetadata failed: %v", err)
			}
			for _, table := range tables {
				if _, ok := km.Tables[table]; !ok {
					t.Errorf("expected table %q in keyspace metadata", table)
				}
			}
		})

		t.Run("nonexistent_keyspace", func(t *testing.T) {
			_, found := session.Metadata().Keyspace("keyspace_that_does_not_exist_xyz")
			require.False(t, found)
		})

		t.Run("empty_keyspace", func(t *testing.T) { _, found := session.Metadata().Keyspace(""); require.False(t, found) })
	})
}

func tableNames(tables map[string]*TableMetadata) []string {
	names := make([]string, 0, len(tables))
	for name := range tables {
		names = append(names, name)
	}
	return names
}

func columnNames(columns map[string]*ColumnMetadata) []string {
	names := make([]string, 0, len(columns))
	for name := range columns {
		names = append(names, name)
	}
	return names
}

// Integration test of just querying for data from the system.schema_keyspace table where the keyspace DOES NOT exist.
func TestGetKeyspaceMetadataFails(t *testing.T) {
	t.Parallel()
	session := createV2Session(t)
	defer session.Close()
	_, found := session.Metadata().Keyspace("gocql_keyspace_does_not_exist")
	require.False(t, found, "snapshot must report a missing keyspace")
}

// Integration test of the routing key calculation
func TestRoutingKey(t *testing.T) {
	t.Parallel()
	session := createV2Session(t, func(c *Config) { c.Metadata.SchemaEvents = false })
	defer session.Close()
	single, composite := testTableName(t, "single"), testTableName(t, "composite")
	cassandraDDL(t, session, fmt.Sprintf("CREATE TABLE %s(first_id int,second_id int,PRIMARY KEY(first_id,second_id))", single))
	cassandraDDL(t, session, fmt.Sprintf("CREATE TABLE %s(first_id int,second_id int,PRIMARY KEY((first_id,second_id)))", composite))
	runtime := session.typedRuntime()
	runtime.mu.Lock()
	initial := len(runtime.prepared)
	runtime.mu.Unlock()
	for i, tc := range []struct {
		table   string
		indexes []int
		key     []byte
	}{{single, []int{1}, []byte{0, 0, 0, 2}}, {composite, []int{1, 0}, []byte{0, 4, 0, 0, 0, 2, 0, 0, 4, 0, 0, 0, 1, 0}}} {
		statement := Statement[[]Value, DynamicRow]{CQL: fmt.Sprintf("SELECT * FROM %s WHERE second_id=? AND first_id=?", tc.table), Params: DynamicParams, Rows: DynamicRows}
		prepared := cassandraPrepared(t, session, statement, PrepareOptions{})
		require.Equal(t, tc.indexes, prepared.Info().PartitionKeyIndexes)
		for _, index := range tc.indexes {
			column, ok := prepared.Parameters().At(index)
			require.True(t, ok)
			require.Equal(t, TypeInt, column.Type.Kind())
		}
		again := cassandraPrepared(t, session, statement, PrepareOptions{})
		require.Same(t, prepared.state, again.state, "preparing again must hit typed cache")
		bound := cassandraBound(t, prepared, []Value{IntValue(1), IntValue(2)})
		require.True(t, bound.Routing().Token.Set)
		require.Equal(t, murmur.Murmur3H1(tc.key), bound.Routing().Token.Value)
		runtime.mu.Lock()
		size := len(runtime.prepared)
		runtime.mu.Unlock()
		require.Equal(t, initial+i+1, size)
	}
}

// Integration test of the token-aware policy-based connection pool
func TestTokenAwareConnPool(t *testing.T) {
	t.Parallel()
	session := createV2Session(t, func(c *Config) {
		c.Pool.ConnectionsPerHost = 1
		p := c.DefaultProfile.Snapshot()
		p.PageSize = 1
		require.NoError(t, c.DefaultProfile.Replace(p))
	})
	defer session.Close()
	conns := waitForHostConns(t, session)
	require.Len(t, conns, len(session.Metadata().Hosts()))
	table, other := testTableName(t), testTableName(t, "other")
	for _, name := range []string{table, other} {
		cassandraDDL(t, session, fmt.Sprintf("CREATE TABLE %s(id int PRIMARY KEY,data text)", name))
	}
	cassandraWrite(t, session, fmt.Sprintf("INSERT INTO %s(id,data) VALUES(?,?)", table), IntValue(42), TextValue("8 * 6 ="))
	bound := cassandraDynamicBound(t, session, fmt.Sprintf("SELECT data FROM %s WHERE id=?", table), []Value{IntValue(42)}, PrepareOptions{})
	require.True(t, bound.Routing().Token.Set)
	require.Equal(t, "8 * 6 =", cassandraText(t, cassandraReadBound(t, session, bound, Options{Consistency: Option[Consistency]{Value: One, Set: true}}), "data"))
}

func TestNegativeStream(t *testing.T) {
	t.Parallel()

	session := createV2Session(t)
	defer session.Close()

	conn := getRandomConn(t, session)

	const stream = -50
	writer := frameWriterFunc(func(f *framer, streamID int) error {
		f.writeHeader(0, frm.OpOptions, stream)
		return f.finish()
	})

	frame, err := conn.exec(context.Background(), writer, nil, time.Second)
	if err == nil {
		t.Fatalf("expected to get an error on stream %d", stream)
	} else if frame != nil {
		t.Fatalf("expected to get nil frame got %+v", frame)
	}
}

func TestManualQueryPaging(t *testing.T) {
	t.Parallel()
	const count = 5
	session := createV2Session(t)
	defer session.Close()
	table := testTableName(t)
	cassandraDDL(t, session, fmt.Sprintf("CREATE TABLE %s(id int PRIMARY KEY,count int)", table))
	for i := int32(0); i < count; i++ {
		cassandraWrite(t, session, fmt.Sprintf("INSERT INTO %s(id,count) VALUES(?,?)", table), IntValue(i), IntValue(i*i))
	}
	bound := cassandraUnprepared(t, "SELECT id,count FROM "+table, DynamicRows)
	state := StartPage()
	fetched := 0
	for {
		page, err := RunPage(context.Background(), session, bound, state, Options{PageSize: Option[int32]{Value: 2, Set: true}})
		require.NoError(t, err)
		defer page.Close()
		for page.Next() {
			row := page.Row()
			id := cassandraInt(t, row, "id")
			require.Equal(t, id*id, cassandraInt(t, row, "count"))
			fetched++
		}
		state = page.State()
		require.NoError(t, page.Close())
		if state.Complete() {
			break
		}
		state = RestorePageState(state.Bytes())
	}
	require.Equal(t, count, fetched)
}

// Issue 475
func TestSessionBindRoutingKey(t *testing.T) {
	t.Parallel()
	session := createV2Session(t)
	defer session.Close()
	table := testTableName(t)
	cassandraDDL(t, session, fmt.Sprintf("CREATE TABLE %s(key varchar PRIMARY KEY,value int)", table))
	type input struct {
		Key   string
		Value int32
	}
	params := Params(Param("key", StringEncoder, func(v input) string { return v.Key }), Param("value", Int32, func(v input) int32 { return v.Value }))
	prepared := cassandraPrepared(t, session, Statement[input, Unit]{CQL: fmt.Sprintf("INSERT INTO %s(key,value) VALUES(?,?)", table), Params: params, Rows: NoRows}, PrepareOptions{})
	require.Equal(t, []int{0}, prepared.Info().PartitionKeyIndexes)
	bound := cassandraBound(t, prepared, input{"routing-key", 5})
	require.True(t, bound.Routing().Token.Set)
	require.Equal(t, murmur.Murmur3H1([]byte("routing-key")), bound.Routing().Token.Value)
	_, err := Exec(context.Background(), session, bound, Options{})
	require.NoError(t, err)
}

func TestJSONSupport(t *testing.T) {
	t.Parallel()
	session := createV2Session(t)
	defer session.Close()
	if session.cfg.ProtoVersion < 4 {
		t.Skip("skipping JSON support on proto < 4")
	}
	table := testTableName(t)
	cassandraDDL(t, session, fmt.Sprintf("CREATE TABLE %s(id text PRIMARY KEY,age int,state text)", table))
	cassandraWrite(t, session, fmt.Sprintf("INSERT INTO %s JSON ?", table), TextValue(`{"id": "user123", "age": 42, "state": "TX"}`))
	row := cassandraRead(t, session, fmt.Sprintf("SELECT id,age,state FROM %s WHERE id=?", table), TextValue("user123"))
	require.Equal(t, "user123", cassandraText(t, row, "id"))
	require.Equal(t, int64(42), cassandraInt(t, row, "age"))
	require.Equal(t, "TX", cassandraText(t, row, "state"))
}

func TestUnmarshallNestedTypes(t *testing.T) {
	t.Parallel()
	session := createV2Session(t)
	defer session.Close()
	table := testTableName(t)
	cassandraDDL(t, session, fmt.Sprintf("CREATE TABLE %s(id text PRIMARY KEY,val list<frozen<map<text,text>>>)", table))
	type nested = []Entry[string, string]
	want := []nested{{{Key: "key1", Value: "val1"}}, {{Key: "key2", Value: "val2"}}}
	prepared := cassandraPrepared(t, session, Statement[[]nested, Unit]{CQL: fmt.Sprintf("INSERT INTO %s(id,val) VALUES('key',?)", table), Params: OneParam(ListEncoder(MapEncoder(StringEncoder, StringEncoder))), Rows: NoRows}, PrepareOptions{})
	_, err := Exec(context.Background(), session, cassandraBound(t, prepared, want), Options{})
	require.NoError(t, err)
	row := cassandraReadBound(t, session, cassandraUnprepared(t, fmt.Sprintf("SELECT val FROM %s WHERE id='key'", table), OneColumn(ListDecoder(MapDecoder(TextCodec, TextCodec)))), Options{})
	iter := row.Iterator()
	var got []nested
	for iter.Next() {
		var entries nested
		mapIter := iter.Value().Iterator()
		for mapIter.Next() {
			entry := mapIter.Value()
			entries = append(entries, Entry[string, string]{Key: entry.Key.String(), Value: entry.Value.String()})
		}
		require.NoError(t, mapIter.Err())
		got = append(got, entries)
	}
	require.NoError(t, iter.Err())
	require.Equal(t, want, got)
}

func TestSchemaReset(t *testing.T) {
	t.Parallel()
	if flagCassVersion.Major == 0 || flagCassVersion.Before(2, 1, 3) {
		t.Skipf("skipping TestSchemaReset due to CASSANDRA-7910 in Cassandra <2.1.3 version=%v", flagCassVersion)
	}
	session := createV2Session(t, func(c *Config) { c.Pool.ConnectionsPerHost = 1 })
	defer session.Close()
	table := testTableName(t)
	cassandraDDL(t, session, fmt.Sprintf("CREATE TABLE %s(id text PRIMARY KEY)", table))
	cassandraWrite(t, session, fmt.Sprintf("INSERT INTO %s(id) VALUES(?)", table), TextValue("test"))
	selectCQL := fmt.Sprintf("SELECT * FROM %s WHERE id=?", table)
	before := cassandraDynamicBound(t, session, selectCQL, []Value{TextValue("test")}, PrepareOptions{})
	require.Equal(t, "test", cassandraText(t, cassandraReadBound(t, session, before, Options{}), "id"))
	cassandraDDL(t, session, fmt.Sprintf("ALTER TABLE %s ADD val text", table))
	cassandraWrite(t, session, fmt.Sprintf("INSERT INTO %s(id,val) VALUES(?,?)", table), TextValue("test"), TextValue("test-val"))
	row := cassandraReadBound(t, session, before, Options{})
	require.Equal(t, "test", cassandraText(t, row, "id"))
	require.Equal(t, "test-val", cassandraText(t, row, "val"))
}

func TestCreateSession_DontSwallowError(t *testing.T) {
	t.Parallel()
	config := createV2Cluster()
	config.Protocol = 0x100
	session, err := Connect(context.Background(), config)
	if err == nil {
		session.Close()
		t.Fatal("expected unsupported protocol error")
	}
	require.Contains(t, err.Error(), "protocol")
}

func TestControl_DiscoverProtocol(t *testing.T) {
	t.Parallel()
	config := createV2Cluster()
	config.Protocol = 0
	config.Compression = NoCompression
	session, err := Connect(context.Background(), config)
	require.NoError(t, err)
	defer session.Close()
	require.NotZero(t, session.cfg.ProtoVersion)
}

// TestUnsetCol verify unset column will not replace an existing column
func TestUnsetCol(t *testing.T) {
	t.Parallel()
	session := createV2Session(t)
	defer session.Close()
	if session.cfg.ProtoVersion < 4 {
		t.Skip("Unset Values are not supported in protocol < 4")
	}
	table := testTableName(t)
	cassandraDDL(t, session, fmt.Sprintf("CREATE TABLE %s(id int PRIMARY KEY,my_int int,my_text text)", table))
	cql := fmt.Sprintf("INSERT INTO %s(id,my_int,my_text) VALUES(?,?,?)", table)
	cassandraWrite(t, session, cql, IntValue(1), IntValue(2), TextValue("3"))
	cassandraWrite(t, session, cql, IntValue(1), UnsetDynamic(IntValue(0).Type()), UnsetDynamic(TextValue("").Type()))
	row := cassandraRead(t, session, "SELECT id,my_int,my_text FROM "+table)
	require.Equal(t, int64(1), cassandraInt(t, row, "id"))
	require.Equal(t, int64(2), cassandraInt(t, row, "my_int"))
	require.Equal(t, "3", cassandraText(t, row, "my_text"))
}

// TestUnsetColBatch verify unset column will not replace a column in batch
func TestUnsetColBatch(t *testing.T) {
	t.Parallel()
	session := createV2Session(t)
	defer session.Close()
	if session.cfg.ProtoVersion < 4 {
		t.Skip("Unset Values are not supported in protocol < 4")
	}
	table := testTableName(t)
	cassandraDDL(t, session, fmt.Sprintf("CREATE TABLE %s(id int PRIMARY KEY,my_int int,my_text text)", table))
	cql := fmt.Sprintf("INSERT INTO %s(id,my_int,my_text) VALUES(?,?,?)", table)
	var items []BatchItem
	for _, values := range [][]Value{{IntValue(1), IntValue(1), UnsetDynamic(TextValue("").Type())}, {IntValue(1), UnsetDynamic(IntValue(0).Type()), TextValue("")}, {IntValue(2), IntValue(2), UnsetDynamic(TextValue("").Type())}} {
		items = append(items, cassandraItem(t, cassandraWriteBound(t, session, cql, values, PrepareOptions{})))
	}
	info := cassandraBatch(t, session, LoggedBatch, items, Options{})
	require.Positive(t, info.Attempts)
	require.Positive(t, info.Latency)
	require.Equal(t, int64(2), cassandraInt(t, cassandraRead(t, session, "SELECT count(*) FROM "+table), "count"))
	row := cassandraRead(t, session, "SELECT id,my_int,my_text FROM "+table+" WHERE id=1")
	require.Equal(t, int64(1), cassandraInt(t, row, "id"))
	require.Equal(t, int64(1), cassandraInt(t, row, "my_int"))
	require.Equal(t, "", cassandraText(t, row, "my_text"))
}

func TestQuery_NamedValues(t *testing.T) {
	t.Parallel()
	session := createV2Session(t)
	defer session.Close()
	table := testTableName(t)
	cassandraDDL(t, session, fmt.Sprintf("CREATE TABLE %s(id int PRIMARY KEY,value text)", table))
	prepared := cassandraPrepared(t, session, Statement[[]ValueField, Unit]{CQL: fmt.Sprintf("INSERT INTO %s(id,value) VALUES(:id,:value)", table), Params: NamedDynamicParams, Rows: NoRows}, PrepareOptions{})
	bound := cassandraBound(t, prepared, []ValueField{{Name: "value", Value: TextValue("i am a value")}, {Name: "id", Value: IntValue(1)}})
	_, err := Exec(context.Background(), session, bound, Options{})
	require.NoError(t, err)
	read := cassandraPrepared(t, session, Statement[[]ValueField, Text]{CQL: fmt.Sprintf("SELECT value FROM %s WHERE id=:id", table), Params: NamedDynamicParams, Rows: OneColumn(TextCodec)}, PrepareOptions{})
	require.Equal(t, "i am a value", cassandraReadBound(t, session, cassandraBound(t, read, []ValueField{{Name: "id", Value: IntValue(1)}}), Options{}).String())
}

// TestQuery_SetHostID ensures that queries are sent to the specified host only.
// WARNING: This test must NOT use t.Parallel(). It calls pool.host.setState(NodeDown)
// which mutates shared HostInfo state visible to all concurrent sessions.
//
//nolint:paralleltest // mutates shared HostInfo state via setState(NodeDown)
func TestQuery_SetHostID(t *testing.T) {
	session := createV2Session(t)
	defer session.Close()
	waitForHostConns(t, session)
	bound := cassandraUnprepared(t, "SELECT host_id FROM system.local", OneColumn(UUIDCodec))
	hosts := session.Metadata().Hosts()
	for _, host := range hosts {
		for i := 0; i < 5; i++ {
			require.Equal(t, host.ID, cassandraReadBound(t, session, bound, Options{HostID: Option[UUID]{Value: host.ID, Set: true}}))
		}
	}
	_, err := ParseUUID("[invalid]")
	require.Error(t, err, "invalid host IDs are rejected while constructing typed UUIDs")
	_, err = Run(context.Background(), session, bound, Options{HostID: Option[UUID]{Value: UUID{}, Set: true}})
	require.ErrorIs(t, err, ErrNoConnections)
	host := hosts[0]
	pool, ok := session.pool.getPoolByHostID(host.ID.String())
	require.True(t, ok)
	pool.host.setState(NodeDown)
	defer pool.host.setState(NodeUp)
	_, err = Run(context.Background(), session, bound, Options{HostID: Option[UUID]{Value: host.ID, Set: true}})
	require.ErrorIs(t, err, ErrNoConnections)
}

func TestQuery_WithNowInSeconds(t *testing.T) {
	session := createV2Session(t)
	defer session.Close()
	if session.cfg.ProtoVersion < 5 {
		t.Skip("Query now in seconds are only available on protocol >= 5")
	}
	table := testTableName(t)
	cassandraDDL(t, session, fmt.Sprintf("CREATE TABLE %s(id int PRIMARY KEY,val text)", table))
	write := cassandraWriteBound(t, session, fmt.Sprintf("INSERT INTO %s(id,val) VALUES(?,?) USING TTL 20", table), []Value{IntValue(1), TextValue("val")}, PrepareOptions{})
	_, err := Exec(context.Background(), session, write, Options{NowInSeconds: Option[int32]{Value: 0, Set: true}})
	require.NoError(t, err)
	read := cassandraPrepared(t, session, Statement[int32, int32]{CQL: fmt.Sprintf("SELECT TTL(val) FROM %s WHERE id=?", table), Params: OneParam(Int32), Rows: OneColumn(Int32)}, PrepareOptions{})
	require.Equal(t, int32(10), cassandraReadBound(t, session, cassandraBound(t, read, int32(1)), Options{NowInSeconds: Option[int32]{Value: 10, Set: true}}))
}

func TestQuery_SetKeyspace(t *testing.T) {
	session := createV2Session(t)
	defer session.Close()
	if session.cfg.ProtoVersion < 5 {
		t.Skip("keyspace for QUERY message is not supported in protocol < 5")
	}
	ks := testKeyspaceName(t)
	table := testTableName(t)
	cassandraDDL(t, session, fmt.Sprintf("CREATE KEYSPACE %s WITH replication={'class':'SimpleStrategy','replication_factor':'1'}", ks))
	defer cassandraDDL(t, session, "DROP KEYSPACE "+ks)
	cassandraDDL(t, session, fmt.Sprintf("CREATE TABLE %s.%s(id int PRIMARY KEY,value text)", ks, table))
	write := cassandraWriteBound(t, session, fmt.Sprintf("INSERT INTO %s(id,value) VALUES(?,?)", table), []Value{IntValue(1), TextValue("text")}, PrepareOptions{Keyspace: ks})
	_, err := Exec(context.Background(), session, write, Options{Keyspace: ks})
	require.NoError(t, err)
	prepared := cassandraPrepared(t, session, Statement[Unit, DynamicRow]{CQL: "SELECT * FROM " + table, Params: NoParams, Rows: DynamicRows}, PrepareOptions{Keyspace: ks})
	for _, bound := range []*Bound[DynamicRow]{cassandraBound(t, prepared, Unit{}), cassandraUnprepared(t, "SELECT * FROM "+table, DynamicRows)} {
		row := cassandraReadBound(t, session, bound, Options{Keyspace: ks})
		require.Equal(t, int64(1), cassandraInt(t, row, "id"))
		require.Equal(t, "text", cassandraText(t, row, "value"))
	}
}

// TestLargeSizeQuery runs a query bigger than the max allowed size of the payload of a frame,
// so it should be sent as 2 different frames where each contains a self-contained bit set to zero.
func TestLargeSizeQuery(t *testing.T) {
	session := createV2Session(t)
	defer session.Close()
	if session.cfg.ProtoVersion < 5 {
		t.Skip("segmented frames are only produced on protocol >= 5")
	}
	table := testTableName(t)
	cassandraDDL(t, session, fmt.Sprintf("CREATE TABLE %s(id int PRIMARY KEY,text_col text)", table))
	text := strings.Repeat("a", 500000)
	cassandraWrite(t, session, fmt.Sprintf("INSERT INTO %s(id,text_col) VALUES(?,?)", table), IntValue(1), TextValue(text))
	require.Equal(t, text, cassandraText(t, cassandraRead(t, session, "SELECT text_col FROM "+table), "text_col"))
}

// TestCompressorNegotiated pins the compressor the suite was asked for to the one the
// connections actually ended up with.
//
// startupCoordinator.startup silently clears Conn.compressor when the server's SUPPORTED
// response does not list Compressor.Name(), so a lane configured with -compressor=lz4
// against a server that does not offer lz4 would run entirely uncompressed while
// session.cfg.Compressor stayed non-nil. Every compression-dependent test guards on the
// connection and would simply skip, leaving the lane green and empty. Fail here instead,
// with the mismatch named.
//
// Every host is checked, not just the one session.getConn happens to pick: the compressor
// is negotiated per connection from that node's own SUPPORTED response, so on a cluster
// where one node does not advertise the compressor a single-connection check passes while
// queries routed to that node run uncompressed. One connection per host is the right
// granularity, since the SUPPORTED response is a property of the node.
//
// waitForHostConns is what makes "every host" true. Walking the hosts directly would skip
// the pools still filling, so on a three-node cluster this would usually inspect one node
// and pass -- leaving exactly the degraded node it exists to catch invisible.
//
// Deliberately not parallel. A parallel test resumes only once the sequential tests at its
// level have started, which would land this diagnosis at the tail of a ~470s run, after
// every test it explains has already skipped.
func TestCompressorNegotiated(t *testing.T) {
	if *flagCompressTest == "" || *flagCompressTest == "no-compression" {
		t.Skip("no compressor requested")
	}

	session := createV2Session(t)
	defer session.Close()

	conns := waitForHostConns(t, session)
	t.Logf("checked the negotiated compressor on %d host(s)", len(conns))

	for addr, conn := range conns {
		switch {
		case conn.compressor == nil:
			t.Errorf("%s: requested -compressor=%s but the connection negotiated none; the server's SUPPORTED response did not offer it",
				addr, *flagCompressTest)
		case conn.compressor.Name() != *flagCompressTest:
			t.Errorf("%s: requested -compressor=%s but the connection negotiated %q",
				addr, *flagCompressTest, conn.compressor.Name())
		}
	}
}

// TestQueryCompressionNotWorthIt runs a query that is not likely to be compressed efficiently
// (uncompressed payload size > compressed payload size).
// So, it should send a Compressed Frame where:
//  1. Compressed length is set to the length of the uncompressed payload;
//  2. Uncompressed length is set to zero;
//  3. Payload is the uncompressed payload.
func TestQueryCompressionNotWorthIt(t *testing.T) {
	session := createV2Session(t)
	defer session.Close()
	if session.cfg.ProtoVersion < 5 {
		t.Skip("compressed segments are only produced on protocol >= 5")
	}
	if getRandomConn(t, session).compressor == nil {
		t.Skip("no compressor negotiated on the connection; the compressed-segment path is unreachable")
	}
	table := testTableName(t)
	cassandraDDL(t, session, fmt.Sprintf("CREATE TABLE %s(id int PRIMARY KEY,text_col text)", table))
	text := "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890!@#$%^&*()_+"
	cassandraWrite(t, session, fmt.Sprintf("INSERT INTO %s(id,text_col) VALUES(?,?)", table), IntValue(1), TextValue(text))
	require.Equal(t, text, cassandraText(t, cassandraRead(t, session, fmt.Sprintf("SELECT text_col FROM %s WHERE id=?", table), IntValue(1)), "text_col"))
}

// TestPrepareExecuteMetadataChangedFlag covers native v5 result metadata IDs
// and the negotiated Scylla v4 extension with the same exact-connection flow.
// The protocol gate selects one path per lane; advertised-but-unnegotiated
// extension support is a failure, not a capability skip.
func TestPrepareExecuteMetadataChangedFlag(t *testing.T) {
	for _, tc := range []struct {
		name  string
		table string
		// gate returns a reason to skip, or "" to run. It may also fail outright, for
		// a state that must not be allowed to pass as a skip.
		gate func(t *testing.T, session *Session, conn *Conn) string
	}{
		{
			name:  "native protocol v5",
			table: "metadata_changed",
			gate: func(t *testing.T, session *Session, conn *Conn) string {
				if session.cfg.ProtoVersion < protoVersion5 {
					return "Metadata_changed mechanism is only available in proto > 4"
				}
				if *flagDistribution == "scylla" && flagCassVersion.Before(2025, 3, 0) {
					return "ScyllaDB before 2025.3 does not exchange result metadata ids"
				}
				return ""
			},
		},
		{
			name:  "protocol v4 with SCYLLA_USE_METADATA_ID",
			table: "scylla_metadata_changed",
			gate: func(t *testing.T, session *Session, conn *Conn) string {
				// The extension backports the v5 result metadata id to v4 and is
				// negotiated there only, so a run pinned to another protocol version
				// says nothing about it either way.
				if conn.version&protoVersionMask != protoVersion4 {
					return "SCYLLA_USE_METADATA_ID is negotiated on protocol v4 only"
				}
				// Skip only for a server that cannot do this at all. If the server
				// advertised SCYLLA_USE_METADATA_ID and the driver still failed to
				// negotiate it, that is a regression — and since this is the only
				// end-to-end coverage of the extension, skipping would turn that
				// regression green. Fail instead.
				if !conn.scyllaSupported.IsMetadataIDSupported() {
					return "server does not advertise SCYLLA_USE_METADATA_ID"
				}
				if !conn.usesMetadataID() {
					t.Fatal("server advertises SCYLLA_USE_METADATA_ID but the driver did not negotiate it")
				}
				return ""
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			session := createV2Session(t, func(c *Config) {
				c.Metadata.SchemaEvents = false
				c.Metadata.AutomaticAgreement = false
				c.Metadata.RefreshAfterAgreement = false
			})
			defer session.Close()

			// We have to specify conn for all queries to ensure that
			// all queries are running on the same node
			conn := getRandomConn(t, session)
			if reason := tc.gate(t, session, conn); reason != "" {
				t.Skip(reason)
			}

			runMetadataChangedFlow(t, session, conn, testTableName(t, tc.table))
		})
	}
}

// runMetadataChangedFlow drives the METADATA_CHANGED flow against
// gocql_test.<table>, with every statement pinned to conn so they all land on the
// node whose prepared-statement cache entry is being inspected.
func runMetadataChangedFlow(t *testing.T, session *Session, conn *Conn, table string) {
	t.Helper()
	qualified := "gocql_test." + table
	cassandraDDL(t, session, "CREATE TABLE "+qualified+"(id int PRIMARY KEY)")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	host := Option[UUID]{Value: conn.host.hostUUID(), Set: true}
	options := Options{HostID: host, connection: conn}
	prepareOptions := PrepareOptions{HostID: host, connection: conn}
	write := cassandraWriteBound(t, session, "INSERT INTO "+qualified+"(id) VALUES(?)", []Value{IntValue(1)}, prepareOptions)
	_, err := Exec(ctx, session, write, options)
	require.NoError(t, err)
	prepared := cassandraPrepared(t, session, Statement[Unit, DynamicRow]{CQL: "SELECT * FROM " + qualified, Params: NoParams, Rows: DynamicRows}, prepareOptions)
	bound := cassandraBound(t, prepared, Unit{})
	row := cassandraReadBound(t, session, bound, options)
	require.Equal(t, 1, row.Columns().Len())
	require.Equal(t, int64(1), cassandraInt(t, row, "id"))
	cached := func() hostPrepared {
		t.Helper()
		prepared.state.mu.RLock()
		defer prepared.state.mu.RUnlock()
		v, ok := prepared.state.hosts[host.Value]
		require.True(t, ok)
		v.id = append([]byte(nil), v.id...)
		v.metadataID = append([]byte(nil), v.metadataID...)
		return v
	}
	before := cached()
	require.NotEmpty(t, before.metadataID)
	alter := cassandraUnprepared(t, "ALTER TABLE "+qualified+" ADD new_col int", NoRows)
	_, err = Exec(ctx, session, alter, options)
	require.NoError(t, err)
	require.NoError(t, session.AwaitSchemaAgreement(ctx))
	write = cassandraWriteBound(t, session, "INSERT INTO "+qualified+"(id,new_col) VALUES(?,?)", []Value{IntValue(2), IntValue(10)}, prepareOptions)
	_, err = Exec(ctx, session, write, options)
	require.NoError(t, err)
	checkRows := func() {
		t.Helper()
		rows, err := Run(ctx, session, bound, options)
		require.NoError(t, err)
		defer rows.Close()
		found := map[int64]Value{}
		for rows.Next() {
			row := rows.Row()
			require.Equal(t, 2, row.Columns().Len())
			found[cassandraInt(t, row, "id")] = cassandraField(t, row, "new_col").Clone()
		}
		require.NoError(t, rows.Close())
		require.Len(t, found, 2)
		require.Equal(t, Null, found[1].State())
		value, err := found[2].Signed()
		require.NoError(t, err)
		require.Equal(t, int64(10), value)
	}
	require.Equal(t, before.generation, session.typedGeneration(), "metadata-ID coverage must not be replaced by generation-triggered reprepare")
	checkRows()
	after := cached()
	require.NotEqual(t, before.metadataID, after.metadataID)
	require.False(t, before.columns.Equal(after.columns))
	// Servers may invalidate the statement on ALTER and force an UNPREPARED
	// repair here. Both paths must update the metadata ID and columns; the
	// explicit stale-ID execution below must consume METADATA_CHANGED.
	// Reintroduce exactly the old result ID and column layout. Preserve the
	// revision ordering so the next change must be observable as a new revision.
	stale := before
	stale.revision = after.revision
	prepared.state.mu.Lock()
	prepared.state.hosts[host.Value] = stale
	prepared.state.mu.Unlock()
	checkRows()
	healed := cached()
	require.True(t, healed.metadataChanged)
	require.NotEqual(t, before.metadataID, healed.metadataID)
	require.Equal(t, after.metadataID, healed.metadataID)
	require.True(t, after.columns.Equal(healed.columns))
	require.Greater(t, healed.revision, after.revision)
	checkRows()
	unchanged := cached()
	require.Equal(t, healed.revision, unchanged.revision, "matching result metadata must not replace cached metadata")
	require.Equal(t, healed.metadataID, unchanged.metadataID)
}

func TestStmtCacheUsesOverriddenKeyspace(t *testing.T) {
	session := createV2Session(t)
	defer session.Close()
	if session.cfg.ProtoVersion < 5 {
		t.Skip("This tests only runs on proto > 4 due SetKeyspace availability")
	}
	ks := testKeyspaceName(t)
	table := testTableName(t)
	cassandraDDL(t, session, fmt.Sprintf("CREATE KEYSPACE %s WITH replication={'class':'SimpleStrategy','replication_factor':1}", ks))
	defer cassandraDDL(t, session, "DROP KEYSPACE "+ks)
	for _, keyspace := range []string{"gocql_test", ks} {
		cassandraDDL(t, session, fmt.Sprintf("CREATE TABLE %s.%s(id int PRIMARY KEY)", keyspace, table))
	}
	defer cassandraDDL(t, session, "DROP TABLE gocql_test."+table)
	insert := "INSERT INTO " + table + "(id) VALUES(?)"
	for i, keyspace := range []string{"gocql_test", ks} {
		bound := cassandraWriteBound(t, session, insert, []Value{IntValue(int32(i + 1))}, PrepareOptions{Keyspace: keyspace})
		cassandraBatch(t, session, LoggedBatch, []BatchItem{cassandraItem(t, bound)}, Options{Keyspace: keyspace})
	}
	for i, keyspace := range []string{"gocql_test", ks} {
		prepared := cassandraPrepared(t, session, Statement[Unit, int32]{CQL: "SELECT * FROM " + table, Params: NoParams, Rows: OneColumn(Int32)}, PrepareOptions{Keyspace: keyspace})
		require.Equal(t, keyspace, prepared.Info().Keyspace)
		require.Equal(t, int32(i+1), cassandraReadBound(t, session, cassandraBound(t, prepared, Unit{}), Options{Keyspace: keyspace}))
	}
}

func TestRoutingKeyCacheUsesOverriddenKeyspace(t *testing.T) {
	session := createV2Session(t)
	defer session.Close()
	if session.cfg.ProtoVersion < 5 {
		t.Skip("This tests only runs on proto > 4 due SetKeyspace availability")
	}
	ks := testKeyspaceName(t)
	table := testTableName(t)
	cassandraDDL(t, session, fmt.Sprintf("CREATE KEYSPACE %s WITH replication={'class':'SimpleStrategy','replication_factor':1}", ks))
	defer cassandraDDL(t, session, "DROP KEYSPACE "+ks)
	for _, keyspace := range []string{"gocql_test", ks} {
		cassandraDDL(t, session, fmt.Sprintf("CREATE TABLE %s.%s(id int PRIMARY KEY)", keyspace, table))
	}
	defer cassandraDDL(t, session, "DROP TABLE gocql_test."+table)
	insert := "INSERT INTO " + table + "(id) VALUES(?)"
	selectCQL := "SELECT * FROM " + table + " WHERE id=?"
	for i, keyspace := range []string{"gocql_test", ks} {
		write := cassandraWriteBound(t, session, insert, []Value{IntValue(int32(i + 1))}, PrepareOptions{Keyspace: keyspace})
		require.Equal(t, keyspace, write.Routing().Keyspace)
		require.True(t, write.Routing().Token.Set)
		item := cassandraItem(t, write)
		require.Equal(t, keyspace, item.request.routing.Keyspace)
		runtime := session.typedRuntime()
		runtime.mu.Lock()
		found := false
		for key, state := range runtime.prepared {
			if key.cql == insert && key.keyspace == keyspace {
				found = true
				require.Equal(t, keyspace, state.info.Keyspace)
			}
		}
		runtime.mu.Unlock()
		require.True(t, found, "cache must include keyspace-specific binding")
		read := cassandraDynamicBound(t, session, selectCQL, []Value{IntValue(1)}, PrepareOptions{Keyspace: keyspace})
		require.Equal(t, keyspace, read.Routing().Keyspace)
		require.True(t, read.Routing().Token.Set)
	}
}

// TestPrepareExecuteScyllaEmptyMetadataID covers the rolling-upgrade case: a
// prepared statement cached before SCYLLA_USE_METADATA_ID was negotiated has no
// result metadata ID, and the prepared cache is keyed by host and survives
// reconnects, so it can still be executed over an extension-enabled connection.
//
// Two things have to hold. The driver must not ask the server to skip metadata for
// such a statement — it has no ID for the server to compare against, so a skipped
// response would leave it decoding against whatever it had cached. And Scylla must
// accept the empty ID as a mismatch rather than rejecting the frame, answering with
// METADATA_CHANGED and a fresh ID so the statement heals itself.
//
// The same scenario is covered in the sibling drivers: java-driver's
// PreparedStatementIT.should_handle_empty_metadata_id_when_executing_statement_when_supported
// (scylladb/java-driver#758) and python-driver's
// test_empty_sentinel_id_triggers_metadata_changed (scylladb/python-driver#770).
func TestPrepareExecuteScyllaEmptyMetadataID(t *testing.T) {
	session := createV2Session(t, func(c *Config) {
		c.Metadata.SchemaEvents = false
		c.Metadata.AutomaticAgreement = false
		c.Metadata.RefreshAfterAgreement = false
	})
	defer session.Close()
	conn := getRandomConn(t, session)
	if conn.version&protoVersionMask != protoVersion4 {
		t.Skip("SCYLLA_USE_METADATA_ID is negotiated on protocol v4 only")
	}
	if !conn.scyllaSupported.IsMetadataIDSupported() {
		t.Skip("server does not advertise SCYLLA_USE_METADATA_ID")
	}
	require.True(t, conn.usesMetadataID(), "server advertises metadata IDs but driver did not negotiate them")
	table := testTableName(t)
	cassandraDDL(t, session, fmt.Sprintf("CREATE TABLE %s(id int PRIMARY KEY,val int)", table))
	cassandraWrite(t, session, fmt.Sprintf("INSERT INTO %s(id,val) VALUES(?,?)", table), IntValue(1), IntValue(7))
	host := Option[UUID]{Value: conn.host.hostUUID(), Set: true}
	prepared := cassandraPrepared(t, session, Statement[int32, DynamicRow]{CQL: fmt.Sprintf("SELECT id,val FROM %s WHERE id=?", table), Params: OneParam(Int32), Rows: DynamicRows}, PrepareOptions{HostID: host, connection: conn})
	bound := cassandraBound(t, prepared, int32(1))
	cassandraReadBound(t, session, bound, Options{HostID: host, connection: conn})
	prepared.state.mu.Lock()
	cached, ok := prepared.state.hosts[host.Value]
	require.True(t, ok)
	require.NotEmpty(t, cached.metadataID)
	cached.metadataID = nil
	cached.revision++
	prepared.state.hosts[host.Value] = cached
	prepared.state.mu.Unlock()
	row := cassandraReadBound(t, session, bound, Options{HostID: host, connection: conn})
	require.Equal(t, int64(1), cassandraInt(t, row, "id"))
	require.Equal(t, int64(7), cassandraInt(t, row, "val"))
	prepared.state.mu.RLock()
	healed := prepared.state.hosts[host.Value]
	prepared.state.mu.RUnlock()
	require.NotEmpty(t, healed.metadataID, "empty metadata ID must be repaired from response")
	require.True(t, healed.metadataChanged)
	require.Greater(t, healed.revision, cached.revision)
}
