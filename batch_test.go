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
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestBatch_Errors(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	session := createV2Session(t)
	defer session.Close()
	table := testTableName(t)
	require.NoError(t, v2CreateTable(ctx, session, fmt.Sprintf(`CREATE TABLE gocql_test.%s (id int primary key, val inet)`, table)))

	// Keep the invalid SELECT inside the native BATCH, so the server rejects
	// the batch itself rather than a preceding PREPARE operation.
	inet, err := ScalarType(TypeInet)
	require.NoError(t, err)
	bound, err := BindUnpreparedOwned(Statement[[]Value, Unit]{
		CQL:    fmt.Sprintf("SELECT * FROM gocql_test.%s WHERE id=2 AND val=?", table),
		Params: DynamicParams, Rows: NoRows,
	}, NewColumns(ColumnSpec{Name: "val", Type: inet}), []Value{NullValue(inet)})
	require.NoError(t, err)
	defer bound.Close()
	item, err := BatchEntry(bound)
	require.NoError(t, err)
	_, err = ExecBatch(ctx, session, LoggedBatch, []BatchItem{item}, Options{})
	require.Error(t, err, "expected to get error for invalid query in batch")
	var server RequestError
	require.ErrorAs(t, err, &server)
}

func TestBatch_WithTimestamp(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	session := createV2Session(t)
	defer session.Close()
	table := testTableName(t)
	require.NoError(t, v2CreateTable(ctx, session, fmt.Sprintf(`CREATE TABLE gocql_test.%s (id int primary key, val text)`, table)))
	micros := time.Now().UnixMicro() - 1000
	first := sessionTestBind(t, session, fmt.Sprintf("INSERT INTO gocql_test.%s (id, val) VALUES (?, ?)", table), IntValue(1), codecTestMust(TextValue("val")))
	defer first.Close()
	second := sessionTestBind(t, session, fmt.Sprintf("INSERT INTO gocql_test.%s (id, val) VALUES (?, ?)", table), IntValue(2), codecTestMust(TextValue("val")))
	defer second.Close()
	_, err := ExecBatch(ctx, session, LoggedBatch, []BatchItem{sessionTestItem(t, first), sessionTestItem(t, second)}, Options{Timestamp: Option[int64]{Value: micros, Set: true}})
	require.NoError(t, err)
	for _, id := range []int32{1, 2} {
		row, err := v2ReadOne(ctx, session, fmt.Sprintf(`SELECT writetime(val) AS written FROM gocql_test.%s WHERE id = ?`, table), IntValue(id))
		require.NoError(t, err)
		require.Equal(t, micros, sessionTestInt(t, row, "written"))
	}
}

func TestBatch_WithNowInSeconds(t *testing.T) {
	ctx := context.Background()
	session := createV2Session(t)
	defer session.Close()
	if session.cfg.ProtoVersion < protoVersion5 {
		t.Skip("Batch now in seconds are only available on protocol >= 5")
	}
	table := testTableName(t)
	require.NoError(t, v2CreateTable(ctx, session, fmt.Sprintf(`CREATE TABLE %s (id int primary key, val text)`, table)))
	bound := sessionTestBind(t, session, fmt.Sprintf("INSERT INTO %s (id, val) VALUES (?, ?) USING TTL 20", table), IntValue(1), codecTestMust(TextValue("val")))
	defer bound.Close()
	_, err := ExecBatch(ctx, session, LoggedBatch, []BatchItem{sessionTestItem(t, bound)}, Options{NowInSeconds: Option[int32]{Value: 0, Set: true}})
	require.NoError(t, err)

	prepared, err := Prepare(ctx, session, Statement[int32, int32]{CQL: fmt.Sprintf(`SELECT TTL(val) FROM %s WHERE id = ?`, table), Params: OneParam(Int32), Rows: OneColumn(Int32)}, PrepareOptions{})
	require.NoError(t, err)
	read, err := BindOwned(prepared, int32(1))
	require.NoError(t, err)
	defer read.Close()
	rows, err := Run(ctx, session, read, Options{NowInSeconds: Option[int32]{Value: 10, Set: true}})
	require.NoError(t, err)
	defer rows.Close()
	require.True(t, rows.Next())
	require.Equal(t, int32(10), rows.Row())
	require.False(t, rows.Next())
	require.NoError(t, rows.Err())
}

func TestBatch_SetKeyspace(t *testing.T) {
	ctx := context.Background()
	session := createV2Session(t)
	defer session.Close()
	if session.cfg.ProtoVersion < protoVersion5 {
		t.Skip("keyspace for BATCH message is not supported in protocol < 5")
	}
	const keyspace = "gocql_keyspace_override_test"
	require.NoError(t, v2CreateTable(ctx, session, `CREATE KEYSPACE IF NOT EXISTS `+keyspace+` WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}`))
	require.NoError(t, v2CreateTable(ctx, session, "CREATE TABLE IF NOT EXISTS "+keyspace+".batch_keyspace(id int, value text, PRIMARY KEY (id))"))
	// Explicit prepare keyspace and execution keyspace must agree. Unqualified
	// member CQL still exercises the v5 BATCH keyspace flag.
	prepared, err := Prepare(ctx, session, Statement[[]Value, Unit]{CQL: "INSERT INTO batch_keyspace(id, value) VALUES (?, ?)", Params: DynamicParams, Rows: NoRows}, PrepareOptions{Keyspace: keyspace})
	require.NoError(t, err)
	first, err := BindOwned(prepared, []Value{IntValue(1), codecTestMust(TextValue("val1"))})
	require.NoError(t, err)
	defer first.Close()
	second, err := BindOwned(prepared, []Value{IntValue(2), codecTestMust(TextValue("val2"))})
	require.NoError(t, err)
	defer second.Close()
	_, err = ExecBatch(ctx, session, LoggedBatch, []BatchItem{sessionTestItem(t, first), sessionTestItem(t, second)}, Options{Keyspace: keyspace})
	require.NoError(t, err)
	rows, err := v2Query(ctx, session, "SELECT id, value FROM "+keyspace+".batch_keyspace")
	require.NoError(t, err)
	defer rows.Close()
	got := map[int64]string{}
	for rows.Next() {
		row := rows.Row()
		value, err := row.Get("value")
		require.NoError(t, err)
		text, err := value.Text()
		require.NoError(t, err)
		got[sessionTestInt(t, row, "id")] = text.String()
	}
	require.NoError(t, rows.Err())
	require.NoError(t, rows.Close())
	require.Equal(t, map[int64]string{1: "val1", 2: "val2"}, got)
}
