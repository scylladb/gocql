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

package gocql_test

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/gocql/gocql"
)

// Example_batchWithServerTimeout demonstrates how to execute a batch with server-side timeout.
// The USING TIMEOUT clause is a ScyllaDB-specific feature.
func Example_batchWithServerTimeout() {
	/* The example assumes the following CQL was used to setup the keyspace:
	create keyspace example with replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };
	create table example.users (userid int, password text, name text, PRIMARY KEY(userid));
	*/
	cluster := gocql.NewCluster("localhost:9042")
	cluster.Keyspace = "example"
	cluster.ProtoVersion = 4
	session, err := cluster.CreateSession()
	if err != nil {
		log.Fatal(err)
	}
	defer session.Close()

	ctx := context.Background()

	// Create a batch with server-side timeout of 500ms
	b := session.Batch(gocql.LoggedBatch).WithContext(ctx)
	b.WithServerTimeout(500 * time.Millisecond)

	// Add statements to the batch
	b.Query("INSERT INTO example.users (userid, password, name) VALUES (?, ?, ?)", 2, "ch@ngem3b", "second user")
	b.Query("UPDATE example.users SET password = ? WHERE userid = ?", "ps22dhds", 3)
	b.Query("INSERT INTO example.users (userid, password) VALUES (?, ?)", 4, "ch@ngem3c")
	b.Query("DELETE name FROM example.users WHERE userid = ?", 1)

	err = session.ExecuteBatch(b)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("Batch executed successfully with server timeout")
}

// Example_batchWithServerTimeoutAndTimestamp demonstrates how to use both server timeout and timestamp.
func Example_batchWithServerTimeoutAndTimestamp() {
	cluster := gocql.NewCluster("localhost:9042")
	cluster.Keyspace = "example"
	cluster.ProtoVersion = 4
	session, err := cluster.CreateSession()
	if err != nil {
		log.Fatal(err)
	}
	defer session.Close()

	// Create a batch with both server-side timeout and custom timestamp
	b := session.Batch(gocql.LoggedBatch)
	b.WithServerTimeout(500 * time.Millisecond)
	b.WithTimestamp(time.Now().UnixNano() / 1000) // Timestamp in microseconds

	// Add statements to the batch
	b.Query("INSERT INTO example.users (userid, password, name) VALUES (?, ?, ?)", 5, "pass1", "user five")
	b.Query("INSERT INTO example.users (userid, password, name) VALUES (?, ?, ?)", 6, "pass2", "user six")

	err = session.ExecuteBatch(b)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("Batch executed successfully with timeout and timestamp")
}
