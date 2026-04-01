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
	"fmt"
	"sync/atomic"

	"testing"
)

func BenchmarkConnStress(b *testing.B) {
	const workers = 16

	cluster := createCluster()
	cluster.NumConns = 1
	session := createSessionFromCluster(cluster, b)
	defer session.Close()

	table := testTableName(b)

	if err := createTable(session, fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (id int primary key)", table)); err != nil {
		b.Fatal(err)
	}

	var seed uint64
	writer := func(pb *testing.PB) {
		seed := atomic.AddUint64(&seed, 1)
		var i uint64 = 0
		for pb.Next() {
			if err := session.Query(fmt.Sprintf("insert into %s (id) values (?)", table), i*seed).Exec(); err != nil {
				b.Error(err)
				return
			}
			i++
		}
	}

	b.SetParallelism(workers)
	b.RunParallel(writer)
}

func BenchmarkConnRoutingKey(b *testing.B) {
	const workers = 16

	cluster := createCluster()
	cluster.NumConns = 1
	cluster.PoolConfig.HostSelectionPolicy = TokenAwareHostPolicy(RoundRobinHostPolicy())
	session := createSessionFromCluster(cluster, b)
	defer session.Close()

	table := testTableName(b)

	if err := createTable(session, fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (id int primary key)", table)); err != nil {
		b.Fatal(err)
	}

	var seed uint64
	writer := func(pb *testing.PB) {
		seed := atomic.AddUint64(&seed, 1)
		var i uint64 = 0
		query := session.Query(fmt.Sprintf("insert into %s (id) values (?)", table))

		for pb.Next() {
			if _, err := query.Bind(i * seed).GetRoutingKey(); err != nil {
				b.Error(err)
				return
			}
			i++
		}
	}

	b.SetParallelism(workers)
	b.RunParallel(writer)
}
