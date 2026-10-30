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
	"github.com/scylladb/gocql/v2"
	"log"
)

func Example_paging() {
	ctx := context.Background()
	s, err := gocql.Connect(ctx, gocql.DefaultConfig("localhost:9042"))
	if err != nil {
		log.Fatal(err)
	}
	defer s.Close()
	b, err := gocql.BindUnpreparedOwned(gocql.Statement[gocql.Unit, int32]{CQL: "SELECT id FROM example.itoa", Params: gocql.NoParams, Rows: gocql.OneColumn(gocql.Int32)}, gocql.NewColumns(), gocql.Unit{})
	if err != nil {
		log.Fatal(err)
	}
	defer func() { b.Close(); _ = b.Wait(ctx) }()
	var state gocql.PageState
	for {
		page, err := gocql.RunPage(ctx, s, b, state, gocql.Options{PageSize: gocql.Option[int32]{Set: true, Value: 2}})
		if err != nil {
			log.Fatal(err)
		}
		for page.Next() {
			fmt.Println(page.Row())
		}
		state = page.State()
		if err := page.Close(); err != nil {
			log.Fatal(err)
		}
		if state.Complete() {
			break
		}
	}
}
