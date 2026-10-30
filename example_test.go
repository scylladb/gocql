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

package gocql_test

import (
	"context"
	"fmt"
	gocql "github.com/scylladb/gocql/v2"

	"log"
)

func Example() {
	// Assumes example.tweet(timeline text, id uuid, text text, PRIMARY KEY(timeline,id)).
	ctx := context.Background()
	config := gocql.DefaultConfig("localhost:9042")
	config.Keyspace = "example"
	session, err := gocql.Connect(ctx, config)
	if err != nil {
		log.Fatal(err)
	}
	defer session.Close()
	type tweet struct {
		ID             gocql.UUID
		Timeline, Text string
	}
	insert, err := gocql.Prepare(ctx, session, gocql.Statement[tweet, gocql.Unit]{
		CQL:    "INSERT INTO tweet (id,timeline,text) VALUES (?,?,?)",
		Params: gocql.Params(gocql.Param("id", gocql.UUIDCodec, func(v tweet) gocql.UUID { return v.ID }), gocql.Param("timeline", gocql.StringEncoder, func(v tweet) string { return v.Timeline }), gocql.Param("text", gocql.StringEncoder, func(v tweet) string { return v.Text })), Rows: gocql.NoRows,
	}, gocql.PrepareOptions{})
	if err != nil {
		log.Fatal(err)
	}
	bound, err := gocql.BindOwned(insert, tweet{gocql.NewTimeUUID(), "me", "hello world"})
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		bound.Close()
		if err := bound.Wait(ctx); err != nil {
			log.Print(err)
		}
	}()
	if _, err := gocql.Exec(ctx, session, bound, gocql.Options{}); err != nil {
		log.Fatal(err)
	}
	type result struct {
		ID   gocql.UUID
		Text gocql.Text
	}
	query, err := gocql.Prepare(ctx, session, gocql.Statement[string, result]{CQL: "SELECT id,text FROM tweet WHERE timeline = ?", Params: gocql.OneParam(gocql.StringEncoder), Rows: gocql.Row(gocql.Column("id", gocql.UUIDCodec, func(v *result) *gocql.UUID { return &v.ID }), gocql.Column("text", gocql.TextCodec, func(v *result) *gocql.Text { return &v.Text }))}, gocql.PrepareOptions{})
	if err != nil {
		log.Fatal(err)
	}
	selectBound, err := gocql.BindOwned(query, "me")
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		selectBound.Close()
		if err := selectBound.Wait(ctx); err != nil {
			log.Print(err)
		}
	}()
	rows, err := gocql.Run(ctx, session, selectBound, gocql.Options{})
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()
	var row result
	for rows.NextInto(&row) {
		fmt.Println(row.ID, row.Text.String())
	}
	if err := rows.Err(); err != nil {
		log.Fatal(err)
	}
}
