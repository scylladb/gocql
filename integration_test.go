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

// This file groups integration tests where Cassandra has to be set up with some special integration variables
import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/gocql/gocql/internal/tests"
)

// TestAuthentication verifies that gocql will work with a host configured to only accept authenticated connections
func TestAuthentication(t *testing.T) {

	if !*flagRunAuthTest {
		t.Skip("Authentication is not configured in the target cluster")
	}

	cluster := createCluster()

	cluster.Authenticator = PasswordAuthenticator{
		Username: "cassandra",
		Password: "cassandra",
	}

	session, err := cluster.CreateSession()

	if err != nil {
		t.Fatalf("Authentication error: %s", err)
	}

	session.Close()
}

func TestGetHostsFromSystem(t *testing.T) {
	clusterHosts := getClusterHosts()
	cluster := createCluster()
	session := createSessionFromCluster(cluster, t)

	hosts, partitioner, err := session.hostSource.GetHostsFromSystem()

	tests.AssertTrue(t, "err == nil", err == nil)
	tests.AssertEqual(t, "len(hosts)", len(clusterHosts), len(hosts))
	tests.AssertTrue(t, "len(partitioner) != 0", len(partitioner) != 0)
}

// TestRingDiscovery makes sure that you can autodiscover other cluster members
// when you seed a cluster config with just one node
func TestRingDiscovery(t *testing.T) {
	clusterHosts := getClusterHosts()
	cluster := createCluster()
	cluster.Hosts = clusterHosts[:1]

	session := createSessionFromCluster(cluster, t)
	defer session.Close()

	if *clusterSize > 1 {
		// wait for autodiscovery to update the pool with the list of known hosts
		time.Sleep(*flagAutoWait)
	}

	session.pool.mu.RLock()
	defer session.pool.mu.RUnlock()
	size := len(session.pool.hostConnPools)

	if *clusterSize != size {
		for p, pool := range session.pool.hostConnPools {
			t.Logf("p=%q host=%v ips=%s", p, pool.host, pool.host.ConnectAddress().String())

		}
		t.Errorf("Expected a cluster size of %d, but actual size was %d", *clusterSize, size)
	}
}

// TestHostFilterDiscovery ensures that host filtering works even when we discover hosts
func TestHostFilterDiscovery(t *testing.T) {
	clusterHosts := getClusterHosts()
	if len(clusterHosts) < 2 {
		t.Skip("skipping because we don't have 2 or more hosts")
	}
	cluster := createCluster()
	rr := RoundRobinHostPolicy().(*roundRobinHostPolicy)
	cluster.PoolConfig.HostSelectionPolicy = rr
	// we'll filter out the second host
	filtered := clusterHosts[1]
	cluster.Hosts = clusterHosts[:1]
	cluster.HostFilter = HostFilterFunc(func(host *HostInfo) bool {
		if host.ConnectAddress().String() == filtered {
			return false
		}
		return true
	})
	session := createSessionFromCluster(cluster, t)
	defer session.Close()

	tests.AssertEqual(t, "len(clusterHosts)-1 != len(rr.hosts.get())", len(clusterHosts)-1, len(rr.hosts.get()))
}

// TestHostFilterInitial ensures that host filtering works for the initial
// connection including the control connection
func TestHostFilterInitial(t *testing.T) {
	clusterHosts := getClusterHosts()
	if len(clusterHosts) < 2 {
		t.Skip("skipping because we don't have 2 or more hosts")
	}
	cluster := createCluster()
	rr := RoundRobinHostPolicy().(*roundRobinHostPolicy)
	cluster.PoolConfig.HostSelectionPolicy = rr
	// we'll filter out the second host
	filtered := clusterHosts[1]
	cluster.HostFilter = HostFilterFunc(func(host *HostInfo) bool {
		if host.ConnectAddress().String() == filtered {
			return false
		}
		return true
	})
	session := createSessionFromCluster(cluster, t)
	defer session.Close()

	tests.AssertEqual(t, "len(clusterHosts)-1 != len(rr.hosts.get())", len(clusterHosts)-1, len(rr.hosts.get()))
}

func TestApplicationInformation(t *testing.T) {
	cluster := createCluster()
	s, err := cluster.CreateSession()
	if err != nil {
		t.Fatalf("ApplicationInformation error: %s", err)
	}
	var clientsTableName string
	for _, tableName := range []string{"system_views.clients", "system.clients"} {
		iter := s.Query("select client_options from " + tableName).Iter()
		_, err = iter.SliceMap()
		if err == nil {
			clientsTableName = tableName
			break
		}
	}

	if clientsTableName == "" {
		t.Skip("Skipping because server does have `client_options` in clients table")
	}

	tcases := []struct {
		testName string
		name     string
		version  string
		clientID string
	}{
		{
			testName: "full",
			name:     "my-application",
			version:  "1.0.0",
			clientID: "my-client-id",
		},
		{
			testName: "empty",
		},
		{
			testName: "name-only",
			name:     "my-application",
		},
		{
			testName: "version-only",
			version:  "1.0.0",
		},
		{
			testName: "client-id-only",
			clientID: "my-client-id",
		},
	}
	for _, tcase := range tcases {
		t.Run(tcase.testName, func(t *testing.T) {
			cluster := createCluster()
			cluster.ApplicationInfo = NewStaticApplicationInfo(tcase.name, tcase.version, tcase.clientID)
			s, err := cluster.CreateSession()
			if err != nil {
				t.Fatalf("failed to connect to the cluster: %s", err)
			}
			defer s.Close()

			var row map[string]string
			iter := s.Query("select client_options from " + clientsTableName).Iter()
			found := false
			for iter.Scan(&row) {
				if tcase.name != "" {
					if row["APPLICATION_NAME"] != tcase.name {
						continue
					}
				} else {
					if _, ok := row["APPLICATION_NAME"]; ok {
						continue
					}
				}
				if tcase.version != "" {
					if row["APPLICATION_VERSION"] != tcase.version {
						continue
					}
				} else {
					if _, ok := row["APPLICATION_VERSION"]; ok {
						continue
					}
				}
				if tcase.clientID != "" {
					if row["CLIENT_ID"] != tcase.clientID {
						continue
					}
				} else {
					if _, ok := row["CLIENT_ID"]; ok {
						continue
					}
				}
				found = true
				break
			}
			if iter.Close() != nil {
				t.Fatalf("failed to execute query: %s", iter.Close().Error())
			}
			if !found {
				t.Fatalf("failed to find the application info row")
			}
		})
	}

}

func TestWriteFailure(t *testing.T) {
	t.Skip("skipped due to unknown purpose")
	cluster := createCluster()
	createKeyspace(t, cluster, "test", false)
	cluster.Keyspace = "test"
	session, err := cluster.CreateSession()
	if err != nil {
		t.Fatal("create session:", err)
	}
	defer session.Close()
	if err := createTable(session, "CREATE TABLE test.test (id int,value int,PRIMARY KEY (id))"); err != nil {
		t.Fatalf("failed to create table with error '%v'", err)
	}
	if err := session.Query(`INSERT INTO test.test (id, value) VALUES (1, 1)`).Exec(); err != nil {
		errWrite, ok := err.(*RequestErrWriteFailure)
		if ok {
			if session.cfg.ProtoVersion >= protoVersion5 {
				// ErrorMap should be filled with some hosts that should've errored
				if len(errWrite.ErrorMap) == 0 {
					t.Fatal("errWrite.ErrorMap should have some failed hosts but it didn't have any")
				}
			} else {
				// Map doesn't get filled for V4
				if len(errWrite.ErrorMap) != 0 {
					t.Fatal("errWrite.ErrorMap should have length 0, it's: ", len(errWrite.ErrorMap))
				}
			}
		} else {
			t.Fatalf("error (%s) should be RequestErrWriteFailure, it's: %T", err, err)
		}
	} else {
		t.Fatal("a write fail error should have happened when querying test keyspace")
	}

	if err = session.Query("DROP KEYSPACE test").Exec(); err != nil {
		t.Fatal(err)
	}
}

func TestCustomPayloadMessages(t *testing.T) {
	t.Skip("SKIPPING")
	cluster := createCluster()
	session := createSessionFromCluster(cluster, t)
	defer session.Close()

	if err := createTable(session, "CREATE TABLE gocql_test.testCustomPayloadMessages (id int, value int, PRIMARY KEY (id))"); err != nil {
		t.Fatal(err)
	}

	// QueryMessage
	var customPayload = map[string][]byte{"a": []byte{10, 20}, "b": []byte{20, 30}}
	query := session.Query("SELECT id FROM testCustomPayloadMessages where id = ?", 42).Consistency(One).CustomPayload(customPayload)
	iter := query.Iter()
	rCustomPayload := iter.GetCustomPayload()
	if !reflect.DeepEqual(customPayload, rCustomPayload) {
		t.Fatal("The received custom payload should match the sent")
	}
	iter.Close()

	// Insert query
	query = session.Query("INSERT INTO testCustomPayloadMessages(id,value) VALUES(1, 1)").Consistency(One).CustomPayload(customPayload)
	iter = query.Iter()
	rCustomPayload = iter.GetCustomPayload()
	if !reflect.DeepEqual(customPayload, rCustomPayload) {
		t.Fatal("The received custom payload should match the sent")
	}
	iter.Close()

	// Batch Message
	b := session.Batch(LoggedBatch)
	b.CustomPayload = customPayload
	b.Query("INSERT INTO testCustomPayloadMessages(id,value) VALUES(1, 1)")
	if err := session.ExecuteBatch(b); err != nil {
		t.Fatalf("query failed. %v", err)
	}
}

func TestCustomPayloadValues(t *testing.T) {
	t.Skip("SKIPPING")
	cluster := createCluster()
	session := createSessionFromCluster(cluster, t)
	defer session.Close()

	if err := createTable(session, "CREATE TABLE gocql_test.testCustomPayloadValues (id int, value int, PRIMARY KEY (id))"); err != nil {
		t.Fatal(err)
	}

	values := []map[string][]byte{map[string][]byte{"a": []byte{10, 20}, "b": []byte{20, 30}}, nil, map[string][]byte{"a": []byte{10, 20}, "b": nil}}

	for _, customPayload := range values {
		query := session.Query("SELECT id FROM testCustomPayloadValues where id = ?", 42).Consistency(One).CustomPayload(customPayload)
		iter := query.Iter()
		rCustomPayload := iter.GetCustomPayload()
		if !reflect.DeepEqual(customPayload, rCustomPayload) {
			t.Fatal("The received custom payload should match the sent")
		}
	}
}

func TestSessionAwaitSchemaAgreement(t *testing.T) {
	session := createSession(t)
	defer session.Close()

	if err := session.AwaitSchemaAgreement(context.Background()); err != nil {
		t.Fatalf("expected session.AwaitSchemaAgreement to not return an error but got '%v'", err)
	}
}

func TestSessionAwaitSchemaAgreementSessionClosed(t *testing.T) {
	session := createSession(t)
	session.Close()

	if err := session.AwaitSchemaAgreement(context.Background()); !errors.Is(err, ErrConnectionClosed) {
		t.Fatalf("expected session.AwaitSchemaAgreement to return ErrConnectionClosed but got '%v'", err)
	}

}

func TestSessionAwaitSchemaAgreementContextCanceled(t *testing.T) {
	session := createSession(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if err := session.AwaitSchemaAgreement(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected session.AwaitSchemaAgreement to return 'context canceled' but got '%v'", err)
	}

}

func TestNewConnectWithLowTimeout(t *testing.T) {
	// Point of these tests to make sure that with low timeout connection creation will gracefully fail

	type TestExpectation int
	const (
		DontRun TestExpectation = iota
		Fail    TestExpectation = iota
		Pass    TestExpectation = iota
		CanPass TestExpectation = iota
	)

	match := func(t *testing.T, e TestExpectation, result error) {
		t.Helper()

		switch e {
		case DontRun:
			t.Fatal("should not be run")
		case Fail:
			if result == nil {
				t.Fatal("should return an error")
			}
		case Pass:
			if result != nil {
				t.Fatalf("should pass, but returned an error: %s", result.Error())
			}
		case CanPass:
			if result == nil {
				t.Log("test passed due to high timeout")
			}
		default:
			panic(fmt.Sprintf("unknown test expectation: %v", e))
		}
	}

	for _, lowTimeout := range []time.Duration{1 * time.Nanosecond, 10 * time.Nanosecond, 100 * time.Nanosecond} {
		canPassOnHighTimeout := Fail
		if lowTimeout >= 100*time.Nanosecond {
			canPassOnHighTimeout = CanPass
		}
		t.Run(lowTimeout.String(), func(t *testing.T) {
			for _, tcase := range []struct {
				name                       string
				getCluster                 func() *ClusterConfig
				connect                    TestExpectation
				regularQuery               TestExpectation
				controlQuery               TestExpectation
				controlQueryAfterReconnect TestExpectation
			}{
				{
					name: "Timeout",
					getCluster: func() *ClusterConfig {
						cluster := createCluster()
						cluster.Timeout = lowTimeout
						return cluster
					},
					connect:                    Pass,
					regularQuery:               Fail,
					controlQuery:               Pass,
					controlQueryAfterReconnect: Pass,
				},
				{
					name: "MetadataSchemaRequestTimeout",
					getCluster: func() *ClusterConfig {
						cluster := createCluster()
						cluster.MetadataSchemaRequestTimeout = lowTimeout
						return cluster
					},
					connect:      Pass,
					regularQuery: Pass,
					controlQuery: Fail,
					// It breaks control connection, then it can start reconnecting in any moment
					// As result test is not stable
					controlQueryAfterReconnect: Fail,
				},
				{
					name: "WriteTimeout",
					getCluster: func() *ClusterConfig {
						cluster := createCluster()
						cluster.WriteTimeout = lowTimeout
						return cluster
					},
					connect:      Pass,
					regularQuery: canPassOnHighTimeout,
					controlQuery: canPassOnHighTimeout,
					// It breaks control connection, then it can start reconnecting in any moment
					// As result test is not stable
					controlQueryAfterReconnect: canPassOnHighTimeout,
				},
				{
					name: "ReadTimeout",
					getCluster: func() *ClusterConfig {
						cluster := createCluster()
						cluster.ReadTimeout = lowTimeout
						return cluster
					},
					connect: Pass,
					// When data is available immediately reading from socket is not failing,
					// despite that deadline is in the past
					// Because of that even with low read timeout it can pass
					regularQuery: CanPass,
					controlQuery: CanPass,
					// It breaks control connection, then it can start reconnecting in any moment
					// As result test is not stable
					controlQueryAfterReconnect: CanPass,
				},
				{
					name: "AllTimeouts",
					getCluster: func() *ClusterConfig {
						cluster := createCluster()
						cluster.Timeout = lowTimeout
						cluster.ReadTimeout = lowTimeout
						cluster.WriteTimeout = lowTimeout
						cluster.MetadataSchemaRequestTimeout = lowTimeout
						return cluster
					},
					connect:                    Pass,
					regularQuery:               Fail,
					controlQuery:               Fail,
					controlQueryAfterReconnect: Fail,
				},
			} {
				t.Run(tcase.name, func(t *testing.T) {
					var (
						s   *Session
						err error
					)

					t.Run("Connect", func(t *testing.T) {
						s, err = tcase.getCluster().CreateSession()
						match(t, tcase.connect, err)
						if err != nil {
							t.Fatal("failed to create session", err.Error())
						}
					})
					if s != nil {
						defer s.Close()
					} else {
						if tcase.connect == Fail {
							t.FailNow()
						} else {
							t.Fatal("session was not created")
						}
					}

					if tcase.regularQuery != DontRun {
						t.Run("Regular Query", func(t *testing.T) {
							err = s.Query("SELECT key FROM system.local WHERE key='local'").Exec()
							match(t, tcase.regularQuery, err)
						})
					}

					if tcase.controlQuery != DontRun {
						t.Run("Query from control connection", func(t *testing.T) {
							err = s.control.querySystem("SELECT key FROM system.local WHERE key='local'").err
							match(t, tcase.controlQuery, err)
						})
					}

					if tcase.controlQueryAfterReconnect != DontRun {
						t.Run("Query from control connection after reconnect", func(t *testing.T) {
							s, err = tcase.getCluster().CreateSession()
							if err != nil {
								t.Fatal("failed to create session", err.Error())
							}
							defer s.Close()
							err = s.control.reconnect()
							if err != nil {
								t.Fatalf("failed to reconnect to control connection: %v", err)
							}
							err = s.control.querySystem("SELECT key FROM system.local WHERE key='local'").err
							match(t, tcase.controlQueryAfterReconnect, err)
						})
					}
				})
			}
		})
	}
}
