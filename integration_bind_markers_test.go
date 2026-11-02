//go:build integration
// +build integration

/*
 * Copyright (C) 2026 ScyllaDB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package gocql

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

// These tests bind IN restrictions, whose anonymous ? markers get a name the
// server synthesizes. ScyllaDB 2026.3 reports "IN(ck)" for ck IN ? and
// "value(IN(ck))" for each marker of ck IN (?, ?), and the spelling varies
// between versions. None of these tests may depend on that name.

// createBindMarkersTable creates a table holding rows (pk, ck) for pk in 1..2
// and ck in 1..3, and returns its name qualified with the keyspace.
func createBindMarkersTable(t *testing.T, session *Session) string {
	t.Helper()
	table := "gocql_test." + testTableName(t)
	require.NoError(t, createTable(session, fmt.Sprintf(
		"CREATE TABLE %s (pk int, ck int, v text, PRIMARY KEY (pk, ck))", table)))
	for pk := 1; pk <= 2; pk++ {
		for ck := 1; ck <= 3; ck++ {
			require.NoError(t, session.Query(fmt.Sprintf(
				"INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)", table), pk, ck, "v").Exec())
		}
	}
	return table
}

// scanInts returns the single int column of every row the query returns.
func scanInts(t *testing.T, iter *Iter) []int {
	t.Helper()
	var (
		got []int
		v   int
	)
	for iter.Scan(&v) {
		got = append(got, v)
	}
	require.NoError(t, iter.Close())
	return got
}

func TestBindMarkers_PositionalIN(t *testing.T) {
	t.Parallel()

	session := createSession(t)
	defer session.Close()
	table := createBindMarkersTable(t, session)

	tests := []struct {
		name   string
		stmt   string
		values []any
		want   []int
	}{
		{
			name:   "clustering IN ?",
			stmt:   "SELECT ck FROM %s WHERE pk = ? AND ck IN ?",
			values: []any{1, []int{1, 3}},
			want:   []int{1, 3},
		},
		{
			name:   "clustering IN (?, ?)",
			stmt:   "SELECT ck FROM %s WHERE pk = ? AND ck IN (?, ?)",
			values: []any{1, 1, 3},
			want:   []int{1, 3},
		},
		{
			name:   "partition IN ?",
			stmt:   "SELECT pk FROM %s WHERE pk IN ? AND ck = ?",
			values: []any{[]int{1, 2}, 2},
			want:   []int{1, 2},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			stmt := fmt.Sprintf(tc.stmt, table)

			got := scanInts(t, session.Query(stmt, tc.values...).Iter())
			require.ElementsMatch(t, tc.want, got)

			// A binding callback must bind by index: Args[i].Name of an IN
			// marker is whatever name the coordinator synthesized.
			var argNames []string
			got = scanInts(t, session.Bind(stmt, func(q *QueryInfo) ([]any, error) {
				argNames = argNames[:0]
				for _, arg := range q.Args {
					argNames = append(argNames, arg.Name)
				}
				return tc.values, nil
			}).Iter())
			require.ElementsMatch(t, tc.want, got)
			t.Logf("server reported bind marker names %q", argNames)
		})
	}
}

func TestBindMarkers_NamedIN(t *testing.T) {
	t.Parallel()

	session := createSession(t)
	defer session.Close()
	table := createBindMarkersTable(t, session)

	tests := []struct {
		name   string
		stmt   string
		values []any
		want   []int
	}{
		{
			name:   "IN :name",
			stmt:   "SELECT ck FROM %s WHERE pk = :pk AND ck IN :cks",
			values: []any{NamedValue("pk", 1), NamedValue("cks", []int{1, 3})},
			want:   []int{1, 3},
		},
		{
			name:   "IN :name out of marker order",
			stmt:   "SELECT ck FROM %s WHERE pk = :pk AND ck IN :cks",
			values: []any{NamedValue("cks", []int{1, 3}), NamedValue("pk", 1)},
			want:   []int{1, 3},
		},
		{
			name:   "IN (:a, :b) out of marker order",
			stmt:   "SELECT ck FROM %s WHERE pk = :pk AND ck IN (:a, :b)",
			values: []any{NamedValue("b", 3), NamedValue("pk", 1), NamedValue("a", 1)},
			want:   []int{1, 3},
		},
		{
			name:   "partition IN :name",
			stmt:   "SELECT pk FROM %s WHERE pk IN :pks AND ck = :ck",
			values: []any{NamedValue("ck", 2), NamedValue("pks", []int{1, 2})},
			want:   []int{1, 2},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := scanInts(t, session.Query(fmt.Sprintf(tc.stmt, table), tc.values...).Iter())
			require.ElementsMatch(t, tc.want, got)
		})
	}

	t.Run("repeated marker bound once", func(t *testing.T) {
		// ScyllaDB reports a repeated :name marker once, Cassandra once per
		// occurrence; one NamedValue binds it either way.
		stmt := fmt.Sprintf("SELECT ck FROM %s WHERE pk = :v AND ck = :v", table)
		got := scanInts(t, session.Query(stmt, NamedValue("v", 2)).Iter())
		require.Equal(t, []int{2}, got)
	})

	t.Run("rejected by the driver", func(t *testing.T) {
		stmt := fmt.Sprintf("SELECT ck FROM %s WHERE pk = :pk AND ck IN :cks", table)

		err := session.Query(stmt, NamedValue("pk", 1), []int{1}).Exec()
		require.ErrorContains(t, err, "cannot mix named and positional")

		err = session.Query(stmt, NamedValue("pk", 1), NamedValue("no_such_marker", []int{1})).Exec()
		require.ErrorContains(t, err, `no bind marker named "no_such_marker"`)

		err = session.Query(stmt, NamedValue("pk", 1), NamedValue("pk", 2)).Exec()
		require.ErrorContains(t, err, `named value "pk" passed more than once`)
	})
}

// TestBindMarkers_RoutingKey pins that named values produce the same routing
// key as positional ones, whatever their order.
func TestBindMarkers_RoutingKey(t *testing.T) {
	t.Parallel()

	session := createSession(t)
	defer session.Close()
	table := createBindMarkersTable(t, session)

	positional := session.Query(fmt.Sprintf("SELECT ck FROM %s WHERE pk = ? AND ck IN ?", table), 2, []int{1})
	want, err := positional.GetRoutingKey()
	require.NoError(t, err)
	require.NotNil(t, want)

	named := session.Query(fmt.Sprintf("SELECT ck FROM %s WHERE pk = :pk AND ck IN :cks", table),
		NamedValue("cks", []int{1}), NamedValue("pk", 2))
	got, err := named.GetRoutingKey()
	require.NoError(t, err)
	require.Equal(t, want, got)
}

// TestBindMarkers_ReprepareAfterEviction pins transparent repreparation for
// IN statements: when the coordinator no longer knows the cached prepared id,
// the driver prepares again and the statement still binds correctly, both
// positionally and by explicit name.
func TestBindMarkers_ReprepareAfterEviction(t *testing.T) {
	t.Parallel()

	session := createSession(t)
	defer session.Close()
	table := createBindMarkersTable(t, session)

	tests := []struct {
		name   string
		stmt   string
		values []any
	}{
		{"positional", "SELECT ck FROM %s WHERE pk = ? AND ck IN ?", []any{1, []int{1, 3}}},
		{"named", "SELECT ck FROM %s WHERE pk = :pk AND ck IN :cks", []any{NamedValue("cks", []int{1, 3}), NamedValue("pk", 1)}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			stmt := fmt.Sprintf(tc.stmt, table)
			conn := getRandomConn(t, session)

			// Replace the cached prepared id with one the server never issued,
			// keeping the real metadata, as if the server had evicted it.
			info, err := conn.prepareStatement(ctx, stmt, nil, conn.getCurrentKeyspace(), session.cfg.Timeout)
			require.NoError(t, err)
			stale := *info
			stale.id = []byte("evicted")
			flight := &inflightPrepare{done: make(chan struct{}), preparedStatment: &stale}
			close(flight.done)
			session.stmtsLRU.add(session.stmtsLRU.keyFor(conn.host.hostUUID(), conn.getCurrentKeyspace(), stmt), flight)

			got := scanInts(t, conn.executeQuery(ctx, session.Query(stmt, tc.values...)))
			require.ElementsMatch(t, []int{1, 3}, got)
		})
	}
}

// TestBindMarkers_ReprepareAfterSchemaChange pins that a schema change, which
// makes the coordinator drop its prepared statements, leads the driver to
// prepare again and hand binding callbacks the refreshed metadata.
func TestBindMarkers_ReprepareAfterSchemaChange(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	session := createSession(t)
	defer session.Close()
	table := createBindMarkersTable(t, session)
	stmt := fmt.Sprintf("SELECT * FROM %s WHERE pk = ? AND ck IN ?", table)
	conn := getRandomConn(t, session)

	var infos []QueryInfo
	bind := func() *Query {
		return session.Bind(stmt, func(q *QueryInfo) ([]any, error) {
			infos = append(infos, *q)
			return []any{1, []int{1, 3}}, nil
		})
	}
	rvalNames := func(q QueryInfo) []string {
		names := make([]string, len(q.Rval))
		for i, c := range q.Rval {
			names[i] = c.Name
		}
		return names
	}

	require.NoError(t, conn.executeQuery(ctx, bind()).Close())
	require.Len(t, infos, 1)
	require.NotContains(t, rvalNames(infos[0]), "extra")

	require.NoError(t, session.Query(fmt.Sprintf("ALTER TABLE %s ADD extra int", table)).Exec())

	iter := conn.executeQuery(ctx, bind())
	rows, err := iter.SliceMap()
	require.NoError(t, err)
	require.Len(t, rows, 2)
	for _, row := range rows {
		require.Contains(t, row, "extra")
	}

	// Once with the stale metadata, rejected as UNPREPARED, then again after
	// the driver prepared the statement anew. The prepared id itself may not
	// change: ScyllaDB derives it from the statement text.
	require.Len(t, infos, 3, "the second execution must go through UNPREPARED and reprepare")
	require.NotContains(t, rvalNames(infos[1]), "extra")
	require.Contains(t, rvalNames(infos[2]), "extra", "binding callback must see the reprepared metadata")
}
