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

// ExampleRunBatch_conditional consumes every typed row returned by a conditional batch.
func ExampleRunBatch_conditional() {
	// Assumes example.my_lwt_batch_table(pk text, ck text, version int, value text,
	// PRIMARY KEY(pk, ck)), with initial rows (pk1,ck1,1,a) and (pk1,ck2,1,A).
	ctx := context.Background()
	config := gocql.DefaultConfig("localhost:9042")
	config.Keyspace = "example"
	session, err := gocql.Connect(ctx, config)
	if err != nil {
		log.Fatal(err)
	}
	defer session.Close()
	type update struct {
		PK, CK, Value string
		Version       int32
	}
	type previous struct {
		PK, CK  gocql.Text
		Version int32
	}
	previousRows := gocql.Row(
		gocql.Column("pk", gocql.TextCodec, func(v *previous) *gocql.Text { return &v.PK }),
		gocql.Column("ck", gocql.TextCodec, func(v *previous) *gocql.Text { return &v.CK }),
		gocql.Column("version", gocql.Int32, func(v *previous) *int32 { return &v.Version }),
	)
	conditional := gocql.CASRows(previousRows)
	prepared, err := gocql.Prepare(ctx, session, gocql.Statement[update, gocql.CASResult[previous]]{
		CQL: "UPDATE example.my_lwt_batch_table SET value=? WHERE pk=? AND ck=? IF version=?",
		Params: gocql.Params(
			gocql.Param("value", gocql.StringEncoder, func(v update) string { return v.Value }),
			gocql.Param("pk", gocql.StringEncoder, func(v update) string { return v.PK }),
			gocql.Param("ck", gocql.StringEncoder, func(v update) string { return v.CK }),
			gocql.Param("version", gocql.Int32, func(v update) int32 { return v.Version }),
		), Rows: conditional,
	}, gocql.PrepareOptions{})
	if err != nil {
		log.Fatal(err)
	}
	var entries []gocql.BatchItem
	for _, value := range []update{{"pk1", "ck1", "b", 1}, {"pk1", "ck2", "B", 0}} {
		bound, err := gocql.BindOwned(prepared, value)
		if err != nil {
			log.Fatal(err)
		}
		defer func() {
			bound.Close()
			if err := bound.Wait(ctx); err != nil {
				log.Print(err)
			}
		}()
		entry, err := gocql.BatchEntry(bound)
		if err != nil {
			log.Fatal(err)
		}
		entries = append(entries, entry)
	}
	page, err := gocql.RunBatch(ctx, session, gocql.LoggedBatch, entries, conditional, gocql.Options{})
	if err != nil {
		log.Fatal(err)
	}
	defer page.Close()
	for page.Next() {
		value := page.Row()
		fmt.Println("applied:", value.Applied)
		if value.Previous.Set {
			fmt.Println(value.Previous.Value.CK.String(), value.Previous.Value.Version)
		}
	}
	if err := page.Err(); err != nil {
		log.Fatal(err)
	}
}
