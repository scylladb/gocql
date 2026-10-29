//go:build integration

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package gocql

import (
	"bytes"
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"
)

type step03LiveDestination struct {
	Value   int32
	Payload Bytes
}
type step03LiveDecoder struct {
	calls *atomic.Int32
	base  RowDecoder[step03LiveDestination]
}

func (d step03LiveDecoder) Compile(c Columns) (RowPlan[step03LiveDestination], error) {
	d.calls.Add(1)
	return d.base.Compile(c)
}
func (d step03LiveDecoder) Clone(v step03LiveDestination) step03LiveDestination {
	return d.base.Clone(v)
}

func TestV2PreparedPagingConcurrentSchemaAndDestinations(t *testing.T) {
	t.Parallel()
	session := createV2Session(t)
	defer session.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	table := fmt.Sprintf("step03_rows_%x", time.Now().UnixNano())
	other := table + "_ddl"
	if err := v2CreateTable(ctx, session, "CREATE TABLE "+table+"(id int,val int,payload blob,PRIMARY KEY(id,val))"); err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = v2CreateTable(context.Background(), session, "DROP TABLE IF EXISTS "+other)
		_ = v2CreateTable(context.Background(), session, "DROP TABLE IF EXISTS "+table)
	}()
	payload := bytes.Repeat([]byte("live-borrowed"), 512)
	for i := int32(0); i < 64; i++ {
		cassandraWrite(t, session, "INSERT INTO "+table+"(id,val,payload) VALUES(?,?,?)", IntValue(1), IntValue(i), BlobValue(payload))
	}
	var calls atomic.Int32
	decoder := step03LiveDecoder{calls: &calls, base: Row(Column("val", Int32, func(r *step03LiveDestination) *int32 { return &r.Value }), Column("payload", BlobCodec, func(r *step03LiveDestination) *Bytes { return &r.Payload }))}
	prepared := codecTestMust(Prepare(ctx, session, Statement[int32, step03LiveDestination]{CQL: "SELECT val,payload FROM " + table + " WHERE id=?", Params: OneParam(Int32), Rows: decoder}, PrepareOptions{}))
	bound := codecTestMust(BindBorrowed(prepared, int32(1)))
	defer bound.Close()
	churn := make(chan error, 1)
	go func() {
		for i := 0; i < 8; i++ {
			if err := v2CreateTable(ctx, session, "CREATE TABLE "+other+"(id int PRIMARY KEY)"); err != nil {
				churn <- err
				return
			}
			if err := v2CreateTable(ctx, session, "DROP TABLE "+other); err != nil {
				churn <- err
				return
			}
		}
		churn <- nil
	}()
	rows := codecTestMust(Run(ctx, session, bound, Options{PageSize: Option[int32]{Value: 3, Set: true}}))
	defer rows.Close()
	var dst step03LiveDestination
	var retained *PageLease
	var first Bytes
	count := 0
	for rows.NextInto(&dst) {
		if dst.Value != int32(count) || !bytes.Equal(dst.Payload.OwnedBytes(), payload) {
			t.Fatal("row mismatch", count, dst.Value)
		}
		if count == 0 {
			retained = codecTestMust(rows.Retain())
			defer retained.Close()
			first = dst.Payload
		}
		count++
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if count != 64 || dst.Value != 0 || dst.Payload.Len() != 0 {
		t.Fatal("count/final reset", count, dst)
	}
	if rows.NextInto(nil) || rows.Err() != nil {
		t.Fatal("exhaustion replaced by nil error", rows.Err())
	}
	if calls.Load() != 1 {
		t.Fatal("steady pages recompiled", calls.Load())
	}
	if err := <-churn; err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(first.OwnedBytes(), payload) {
		t.Fatal("retained output lost during DDL")
	}
	bound.Close()
	if err := bound.Wait(ctx); err != nil {
		t.Fatal(err)
	}
}
