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

	"github.com/scylladb/gocql/v2/internal/tests"
)

// TestKeyspaceTable checks that fully qualified prepared statements retain the
// server's keyspace/table metadata independently of later configuration changes.
func TestKeyspaceTable(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cluster := createV2Cluster()
	v2ConfigureRouting(t, &cluster, RoutingOptions{TokenAware: true})
	session, err := Connect(ctx, cluster)
	if err != nil {
		t.Fatal("connect:", err)
	}
	defer session.Close()
	cluster.Keyspace = "wrong_keyspace"
	keyspace, table := v2SchemaRoutingName(t), v2SchemaRoutingName(t)
	v2CreateRoutingKeyspace(t, session, keyspace, 1)
	defer cleanup(t, session, keyspace)
	if err := v2CreateTable(ctx, session, fmt.Sprintf(`CREATE TABLE %s.%s (pk int, ck int, v int, PRIMARY KEY (pk, ck))`, keyspace, table)); err != nil {
		t.Fatal(err)
	}
	if _, err := v2SchemaRoutingExec(ctx, session, fmt.Sprintf(`INSERT INTO %s.%s(pk, ck, v) VALUES (?, ?, ?)`, keyspace, table), Options{Consistency: Option[Consistency]{Value: One, Set: true}}, IntValue(1), IntValue(2), IntValue(3)); err != nil {
		t.Fatal(err)
	}
	prepared, err := Prepare(ctx, session, Statement[[]Value, DynamicRow]{CQL: fmt.Sprintf(`SELECT pk FROM %s.%s WHERE pk = ? LIMIT 1`, keyspace, table), Params: DynamicParams, Rows: DynamicRows}, PrepareOptions{})
	if err != nil {
		t.Fatal(err)
	}
	bound, err := BindOwned(prepared, []Value{IntValue(1)})
	if err != nil {
		t.Fatal(err)
	}
	defer bound.Close()
	rows, err := Run(ctx, session, bound, Options{Consistency: Option[Consistency]{Value: One, Set: true}})
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	if !rows.Next() {
		t.Fatalf("missing row: %v", rows.Err())
	}
	row := rows.Row()
	tests.AssertEqual(t, "pk", int32(1), v2SchemaRoutingInt(t, row, "pk"))
	if rows.Next() {
		t.Fatal("unexpected extra row")
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	info := prepared.Info()
	tests.AssertEqual(t, "prepared.Keyspace", keyspace, info.Keyspace)
	tests.AssertEqual(t, "prepared.Table", table, info.Table)
	tests.AssertEqual(t, "bound.Keyspace", keyspace, bound.Routing().Keyspace)
	tests.AssertEqual(t, "bound.Table", table, bound.Routing().Table)
	tests.AssertTrue(t, "partition-key token is available", bound.Routing().Token.Set)
	metadata, ok := session.Metadata().Keyspace(keyspace)
	tests.AssertTrue(t, "keyspace is present", ok)
	tableMetadata := metadata.Tables[table]
	tests.AssertTrue(t, "table is present", tableMetadata != nil)
	tests.AssertEqual(t, "partition key metadata", "pk", tableMetadata.PartitionKey[0].Name)
	tests.AssertEqual(t, "clustering key metadata", "ck", tableMetadata.ClusteringColumns[0].Name)
}
