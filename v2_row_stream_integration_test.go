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
	"errors"
	"fmt"
	"testing"
	"time"
)

func TestStep07RunStreamLive(t *testing.T) {
	config := createV2Cluster(func(c *Config) {
		c.Pool.ConnectionsPerHost = 1
		c.KeepaliveInterval = 0
		c.Metadata.PeriodicRefresh = 0
	})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	s, err := Connect(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	keyspace := testKeyspaceName(t, fmt.Sprintf("%x", time.Now().UnixNano()))
	exec := func(cql string) {
		t.Helper()
		b := codecTestMust(BindUnpreparedOwned(Statement[Unit, Unit]{CQL: cql, Kind: OrdinaryStatement, Params: NoParams, Rows: NoRows}, NewColumns(), Unit{}))
		defer b.Close()
		if _, err := Exec(ctx, s, b, Options{}); err != nil {
			t.Fatal(err)
		}
	}
	exec("CREATE KEYSPACE " + keyspace + " WITH replication={'class':'NetworkTopologyStrategy','replication_factor':1}")
	defer func() {
		cleanup, stop := context.WithTimeout(context.Background(), 30*time.Second)
		defer stop()
		b, err := BindUnpreparedOwned(Statement[Unit, Unit]{CQL: "DROP KEYSPACE " + keyspace, Kind: OrdinaryStatement, Params: NoParams, Rows: NoRows}, NewColumns(), Unit{})
		if err == nil {
			defer b.Close()
			_, err = Exec(cleanup, s, b, Options{})
		}
		if err != nil {
			t.Error("unique keyspace cleanup", err)
		}
	}()
	exec("CREATE TABLE " + keyspace + ".records(id int PRIMARY KEY,value blob,pieces list<blob>,txt text)")
	type input struct {
		ID     int32
		Value  []byte
		Pieces [][]byte
		Text   string
	}
	params := Params(Param("id", Int32, func(v input) int32 { return v.ID }), Param("value", BytesEncoder, func(v input) []byte { return v.Value }), Param("pieces", ListEncoder(BytesEncoder), func(v input) [][]byte { return v.Pieces }), Param("txt", StringEncoder, func(v input) string { return v.Text }))
	prepared := codecTestMust(Prepare(ctx, s, Statement[input, Unit]{CQL: "INSERT INTO " + keyspace + ".records(id,value,pieces,txt)VALUES(?,?,?,?)", Kind: OrdinaryStatement, Params: params, Rows: NoRows}, PrepareOptions{}))
	payload := bytes.Repeat([]byte("segment-crossing"), 30000)
	for i := 0; i < 5; i++ {
		b := codecTestMust(BindOwned(prepared, input{int32(i), payload, [][]byte{[]byte("nested"), []byte("storage")}, "live-text"}))
		_, err := Exec(ctx, s, b, Options{})
		b.Close()
		if err != nil {
			t.Fatal(err)
		}
		if err := b.Wait(ctx); err != nil {
			t.Fatal(err)
		}
	}
	statement := Statement[Unit, DynamicRow]{CQL: "SELECT id,value,pieces,txt FROM " + keyspace + ".records", Kind: OrdinaryStatement, Params: NoParams, Rows: DynamicRows}
	selectPrepared := codecTestMust(Prepare(ctx, s, statement, PrepareOptions{}))
	bound := codecTestMust(BindOwned(selectPrepared, Unit{}))
	defer bound.Close()
	type output struct {
		ID     int32
		Value  []byte
		Pieces [][]byte
		Text   []byte
	}
	decoder := StreamRow(func(*output) {}, StreamColumn("id", StreamInt32, func(r *output) *int32 { return &r.ID }), StreamColumn("value", StreamBlob, func(r *output) *[]byte { return &r.Value }), StreamColumn("pieces", StreamList(StreamBlob), func(r *output) *[][]byte { return &r.Pieces }), StreamColumn("txt", StreamText, func(r *output) *[]byte { return &r.Text }))
	options := StreamOptions{Options: Options{PageSize: Option[int32]{Set: true, Value: 2}}}
	if config.Protocol < 5 && config.Compression != NoCompression {
		_, err := RunStream(ctx, s, bound, decoder, options)
		var capability *StreamCapabilityError
		if !errors.As(err, &capability) {
			t.Fatal("frame compression pre-submit capability", err)
		}
		t.Log("frame-compressed streaming explicitly rejected before query")
		return
	}
	stream, err := RunStream(ctx, s, bound, decoder, options)
	if err != nil {
		t.Fatal(err)
	}
	dst := output{Value: make([]byte, 0, len(payload)), Text: make([]byte, 0, 32), Pieces: [][]byte{make([]byte, 0, 16), make([]byte, 0, 16)}}
	valueBacking := &dst.Value[:cap(dst.Value)][0]
	nestedBacking := &dst.Pieces[0][:cap(dst.Pieces[0])][0]
	seen := map[int32]bool{}
	for stream.NextInto(&dst) {
		if seen[dst.ID] || !bytes.Equal(dst.Value, payload) || string(dst.Text) != "live-text" || len(dst.Pieces) != 2 || string(dst.Pieces[0]) != "nested" || string(dst.Pieces[1]) != "storage" {
			t.Fatal("streamed live output", dst.ID, len(dst.Value), dst.Pieces)
		}
		if &dst.Value[0] != valueBacking || &dst.Pieces[0][0] != nestedBacking {
			t.Fatal("caller capacity replaced")
		}
		seen[dst.ID] = true
	}
	if err := stream.Err(); err != nil {
		t.Fatal(err)
	}
	if err := stream.Close(); err != nil {
		t.Fatal(err)
	}
	info := stream.Info()
	if len(seen) != 5 || info.DeliveredRows != 5 || info.Pages < 3 || info.Result.Attempts < 3 || !info.OutputStarted {
		t.Fatal("incomplete live stream", len(seen), info)
	}
	if !bytes.Equal(dst.Value, payload) {
		t.Fatal("output lost after close")
	}
	// A separate caller destination is never borrowed past early Close. Ordinary
	// configured pool repair must restore subsequent execution within its budget.
	early, err := RunStream(ctx, s, bound, decoder, options)
	if err != nil {
		t.Fatal(err)
	}
	if !early.NextInto(&dst) {
		t.Fatal(early.Err())
	}
	if err := early.Close(); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(20 * time.Second)
	for {
		ready := true
		for _, host := range s.hostSource.getHostsList() {
			pool, ok := s.pool.getPool(host)
			if !ok {
				ready = false
				break
			}
			pool.mu.RLock()
			count, missing := pool.connPicker.Size()
			pool.mu.RUnlock()
			if count != 1 || missing != 0 {
				ready = false
				break
			}
		}
		if ready {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("bounded pool repair failed")
		}
		time.Sleep(10 * time.Millisecond)
	}
	rows, err := Run(ctx, s, bound, Options{Prefetch: Option[int]{Set: true, Value: 0}})
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	for rows.Next() {
		count++
	}
	if err := rows.Close(); err != nil || count != 5 {
		t.Fatal("shared execution after stream repair", count, err)
	}
	emptyBound := codecTestMust(BindUnpreparedOwned(Statement[Unit, DynamicRow]{CQL: "SELECT id,value,pieces,txt FROM " + keyspace + ".records WHERE id=-1", Kind: OrdinaryStatement, Params: NoParams, Rows: DynamicRows}, NewColumns(), Unit{}))
	empty, err := RunStream(ctx, s, emptyBound, decoder, options)
	if err != nil {
		t.Fatal(err)
	}
	if empty.NextInto(&dst) || empty.Err() != nil || empty.Info().OutputStarted {
		t.Fatal("empty result", empty.Err(), empty.Info())
	}
	empty.Close()
	emptyBound.Close()
	bound.Close()
	if err := bound.Wait(ctx); err != nil {
		t.Fatal(err)
	}
	t.Logf("public RunStream protocol=%d compression=%d tls=%v rows=%d pages=%d bytes/row=%d attempts=%d", config.Protocol, config.Compression, *flagRunSslTest, info.DeliveredRows, info.Pages, len(payload), info.Result.Attempts)
}
