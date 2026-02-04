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
	"flag"
	"fmt"
	"log"
	"net"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gocql/gocql/lz4"
)

var (
	flagCluster       = flag.String("cluster", "127.0.0.1", "a comma-separated list of host:port tuples")
	flagProto         = flag.Int("proto", 0, "protcol version")
	flagCQL           = flag.String("cql", "3.0.0", "CQL version")
	flagRF            = flag.Int("rf", 1, "replication factor for test keyspace")
	clusterSize       = flag.Int("clusterSize", 1, "the expected size of the cluster")
	flagRetry         = flag.Int("retries", 5, "number of times to retry queries")
	flagAutoWait      = flag.Duration("autowait", 1000*time.Millisecond, "time to wait for autodiscovery to fill the hosts poll")
	flagRunSslTest    = flag.Bool("runssl", false, "Set to true to run ssl test")
	flagRunAuthTest   = flag.Bool("runauth", false, "Set to true to run authentication test")
	flagCompressTest  = flag.String("compressor", "no-compression", "compressor to use")
	flagTimeout       = flag.Duration("gocql.timeout", 5*time.Second, "sets the connection `timeout` for all operations")
	flagClusterSocket = flag.String("cluster-socket", "", "nodes socket files separated by comma")
	flagDistribution  = flag.String("distribution", "scylla", "database distribution - scylla or cassandra")
	flagCassVersion   cassVersion
)

func init() {
	flag.Var(&flagCassVersion, "gocql.cversion", "the cassandra version being tested against")

	log.SetFlags(log.Lshortfile | log.LstdFlags)
}

func getClusterHosts() []string {
	return strings.Split(*flagCluster, ",")
}

func addSslOptions(cluster *ClusterConfig) *ClusterConfig {
	if *flagRunSslTest {
		if *flagDistribution == "cassandra" {
			cluster.Port = 9042
		} else {
			cluster.Port = 9142
		}
		cluster.SslOpts = &SslOptions{
			CertPath:               "testdata/pki/gocql.crt",
			KeyPath:                "testdata/pki/gocql.key",
			CaPath:                 "testdata/pki/ca.crt",
			EnableHostVerification: false,
		}
	}
	return cluster
}

type OnceManager struct {
	keyspaces map[string]*sync.Once
	mu        sync.Mutex
}

func NewOnceManager() *OnceManager {
	return &OnceManager{
		keyspaces: make(map[string]*sync.Once),
	}
}

func (o *OnceManager) GetOnce(key string) *sync.Once {
	o.mu.Lock()
	defer o.mu.Unlock()

	if once, exists := o.keyspaces[key]; exists {
		return once
	}
	o.keyspaces[key] = &sync.Once{}
	return o.keyspaces[key]
}

var initKeyspaceOnce = NewOnceManager()

var isTabletsSupportedFlag *bool
var isTabletsSupportedOnce sync.RWMutex

func isTabletsSupported() bool {
	isTabletsSupportedOnce.RLock()
	if isTabletsSupportedFlag != nil {
		isTabletsSupportedOnce.RUnlock()
		return *isTabletsSupportedFlag
	}
	isTabletsSupportedOnce.RUnlock()
	isTabletsSupportedOnce.Lock()
	defer isTabletsSupportedOnce.Unlock()
	if isTabletsSupportedFlag != nil {
		return *isTabletsSupportedFlag
	}
	var result bool

	s, err := createCluster().CreateSession()
	if err != nil {
		panic(fmt.Errorf("failed to create session: %v", err))
	}
	res := make(map[string]interface{})
	err = s.Query("select * from system.local").MapScan(res)
	if err != nil {
		panic(fmt.Errorf("failed to read system.local: %v", err))
	}

	features, _ := res["supported_features"]
	featuresCasted, _ := features.(string)
	for _, feature := range strings.Split(featuresCasted, ",") {
		if feature == "TABLETS" {
			result = true
			isTabletsSupportedFlag = &result
			return true
		}
	}
	result = false
	isTabletsSupportedFlag = &result
	return false
}

var isTabletsAutoEnabledFlag *bool
var isTabletsAutoEnabledOnce sync.RWMutex

func isTabletsAutoEnabled() bool {
	isTabletsAutoEnabledOnce.RLock()
	if isTabletsAutoEnabledFlag != nil {
		isTabletsAutoEnabledOnce.RUnlock()
		return *isTabletsAutoEnabledFlag
	}
	isTabletsAutoEnabledOnce.RUnlock()
	isTabletsAutoEnabledOnce.Lock()
	defer isTabletsAutoEnabledOnce.Unlock()
	if isTabletsAutoEnabledFlag != nil {
		return *isTabletsAutoEnabledFlag
	}

	s, err := createCluster().CreateSession()
	if err != nil {
		panic(fmt.Errorf("failed to create session: %v", err))
	}

	err = s.Query("DROP KEYSPACE IF EXISTS gocql_check_tablets_enabled").Exec()
	if err != nil {
		panic(fmt.Errorf("failed to delete keyspace: %v", err))
	}
	err = s.Query("CREATE KEYSPACE gocql_check_tablets_enabled WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': '1'}").Exec()
	if err != nil {
		panic(fmt.Errorf("failed to delete keyspace: %v", err))
	}

	res := make(map[string]interface{})
	err = s.Query("describe keyspace gocql_check_tablets_enabled").MapScan(res)
	if err != nil {
		panic(fmt.Errorf("failed to read system.local: %v", err))
	}

	createStmt, _ := res["create_statement"]
	createStmtCasted, _ := createStmt.(string)
	result := strings.Contains(strings.ToLower(createStmtCasted), "and tablets")
	isTabletsAutoEnabledFlag = &result
	return result
}

func createTable(s *Session, table string) error {
	// lets just be really sure
	if err := s.control.awaitSchemaAgreement(); err != nil {
		log.Printf("error waiting for schema agreement pre create table=%q err=%v\n", table, err)
		return err
	}

	if err := s.Query(table).RetryPolicy(&SimpleRetryPolicy{NumRetries: 3}).Idempotent(true).Exec(); err != nil {
		log.Printf("error creating table table=%q err=%v\n", table, err)
		return err
	}

	if err := s.control.awaitSchemaAgreement(); err != nil {
		log.Printf("error waiting for schema agreement post create table=%q err=%v\n", table, err)
		return err
	}

	return nil
}

func createCluster(opts ...func(*ClusterConfig)) *ClusterConfig {
	clusterHosts := getClusterHosts()
	cluster := NewCluster(clusterHosts...)
	cluster.ProtoVersion = *flagProto
	cluster.CQLVersion = *flagCQL
	cluster.Timeout = *flagTimeout
	cluster.Consistency = Quorum
	cluster.MaxWaitSchemaAgreement = 2 * time.Minute // travis might be slow
	if *flagRetry > 0 {
		cluster.RetryPolicy = &SimpleRetryPolicy{NumRetries: *flagRetry}
	}

	switch *flagCompressTest {
	case "snappy":
		cluster.Compressor = &SnappyCompressor{}
	case "lz4":
		cluster.Compressor = &lz4.LZ4Compressor{}
	case "no-compression":
	default:
		panic("invalid compressor: " + *flagCompressTest)
	}

	cluster = addSslOptions(cluster)

	for _, opt := range opts {
		opt(cluster)
	}

	return cluster
}

func createKeyspace(tb testing.TB, cluster *ClusterConfig, keyspace string, disableTablets bool) {
	tb.Helper()

	c := *cluster
	c.Keyspace = "system"
	c.Timeout = 30 * time.Second
	session, err := c.CreateSession()
	if err != nil {
		tb.Fatalf("failed to create session: %v", err)
	}
	defer session.Close()

	err = createTable(session, `DROP KEYSPACE IF EXISTS `+keyspace)
	if err != nil {
		tb.Fatalf("unable to drop keyspace: %v", err)
	}

	query := fmt.Sprintf(`CREATE KEYSPACE %s
	WITH replication = {
		'class' : 'NetworkTopologyStrategy',
		'replication_factor' : %d
	}`, keyspace, *flagRF)

	if isTabletsSupported() {
		if disableTablets {
			query += " AND tablets = {'enabled': false}"
		} else if !isTabletsAutoEnabled() {
			query += " AND tablets = {'enabled': true};"
		}
	}

	err = createTable(session, query)
	if err != nil {
		tb.Fatalf("unable to create table: %v", err)
	}
}

type testKeyspaceOpts struct {
	tabletsDisabled bool
}

func (o *testKeyspaceOpts) KeyspaceName() string {
	if o.tabletsDisabled {
		return "gocql_test_tablets_disabled"
	}
	return "gocql_test"
}

func createSessionFromClusterHelper(cluster *ClusterConfig, tb testing.TB, opts testKeyspaceOpts) *Session {
	// Drop and re-create the keyspace once. Different tests should use their own
	// individual tables, but can assume that the table does not exist before.
	initKeyspaceOnce.GetOnce(opts.KeyspaceName()).Do(func() {
		createKeyspace(tb, cluster, opts.KeyspaceName(), opts.tabletsDisabled)
	})

	cluster.Keyspace = opts.KeyspaceName()
	session, err := cluster.CreateSession()
	if err != nil {
		tb.Fatalf("failed to create session: %v", err)
	}

	if err := session.control.awaitSchemaAgreement(); err != nil {
		tb.Fatalf("failed to wait on schema agreement: %v", err)
	}

	return session
}

func getClusterSocketFile() []string {
	var res []string
	for _, socketFile := range strings.Split(*flagClusterSocket, ",") {
		if socketFile != "" {
			res = append(res, socketFile)
		}
	}
	return res
}

func createSessionFromClusterTabletsDisabled(cluster *ClusterConfig, tb testing.TB) *Session {
	return createSessionFromClusterHelper(cluster, tb, testKeyspaceOpts{tabletsDisabled: true})
}

func createSessionFromCluster(cluster *ClusterConfig, tb testing.TB) *Session {
	return createSessionFromClusterHelper(cluster, tb, testKeyspaceOpts{tabletsDisabled: false})
}

func createSession(tb testing.TB, opts ...func(config *ClusterConfig)) *Session {
	cluster := createCluster(opts...)
	return createSessionFromCluster(cluster, tb)
}

func createViews(t *testing.T, session *Session) {
	if err := session.Query(`
		CREATE TYPE IF NOT EXISTS gocql_test.basicView (
		birthday timestamp,
		nationality text,
		weight text,
		height text);	`).Exec(); err != nil {
		t.Fatalf("failed to create view with err: %v", err)
	}
}

func createMaterializedViews(t *testing.T, session *Session) {
	if flagCassVersion.Before(3, 0, 0) {
		return
	}
	if err := session.Query(`CREATE TABLE IF NOT EXISTS gocql_test.view_table (
		    userid text,
		    year int,
		    month int,
    		    PRIMARY KEY (userid));`).Exec(); err != nil {
		t.Fatalf("failed to create materialized view with err: %v", err)
	}
	if err := session.Query(`CREATE TABLE IF NOT EXISTS gocql_test.view_table2 (
		    userid text,
		    year int,
		    month int,
    		    PRIMARY KEY (userid));`).Exec(); err != nil {
		t.Fatalf("failed to create materialized view with err: %v", err)
	}
	if err := session.Query(`CREATE MATERIALIZED VIEW IF NOT EXISTS gocql_test.view_view AS
		   SELECT year, month, userid
		   FROM gocql_test.view_table
		   WHERE year IS NOT NULL AND month IS NOT NULL AND userid IS NOT NULL
		   PRIMARY KEY (userid, year);`).Exec(); err != nil {
		t.Fatalf("failed to create materialized view with err: %v", err)
	}
	if err := session.Query(`CREATE MATERIALIZED VIEW IF NOT EXISTS gocql_test.view_view2 AS
		   SELECT year, month, userid
		   FROM gocql_test.view_table2
		   WHERE year IS NOT NULL AND month IS NOT NULL AND userid IS NOT NULL
		   PRIMARY KEY (userid, year);`).Exec(); err != nil {
		t.Fatalf("failed to create materialized view with err: %v", err)
	}
}

func createFunctions(t *testing.T, session *Session) {
	if err := session.Query(`
		CREATE OR REPLACE FUNCTION gocql_test.avgState ( state tuple<int,bigint>, val int )
		CALLED ON NULL INPUT
		RETURNS tuple<int,bigint>
		LANGUAGE java AS
		$$if (val !=null) {state.setInt(0, state.getInt(0)+1); state.setLong(1, state.getLong(1)+val.intValue());}return state;$$;	`).Exec(); err != nil {
		t.Fatalf("failed to create function with err: %v", err)
	}
	if err := session.Query(`
		CREATE OR REPLACE FUNCTION gocql_test.avgFinal ( state tuple<int,bigint> )
		CALLED ON NULL INPUT
		RETURNS double
		LANGUAGE java AS
		$$double r = 0; if (state.getInt(0) == 0) return null; r = state.getLong(1); r/= state.getInt(0); return Double.valueOf(r);$$
	`).Exec(); err != nil {
		t.Fatalf("failed to create function with err: %v", err)
	}
}

func createAggregate(t *testing.T, session *Session) {
	createFunctions(t, session)
	if err := session.Query(`
		CREATE OR REPLACE AGGREGATE gocql_test.average(int)
		SFUNC avgState
		STYPE tuple<int,bigint>
		FINALFUNC avgFinal
		INITCOND (0,0);
	`).Exec(); err != nil {
		t.Fatalf("failed to create aggregate with err: %v", err)
	}
	if err := session.Query(`
		CREATE OR REPLACE AGGREGATE gocql_test.average2(int)
		SFUNC avgState
		STYPE tuple<int,bigint>
		FINALFUNC avgFinal
		INITCOND (0,0);
	`).Exec(); err != nil {
		t.Fatalf("failed to create aggregate with err: %v", err)
	}
}

func staticAddressTranslator(newAddr net.IP, newPort int) AddressTranslator {
	return AddressTranslatorFunc(func(addr net.IP, port int) (net.IP, int) {
		return newAddr, newPort
	})
}

func assertDeepEqual(t *testing.T, description string, expected, actual interface{}) {
	t.Helper()
	if !reflect.DeepEqual(expected, actual) {
		t.Fatalf("expected %s to be (%#v) but was (%#v) instead", description, expected, actual)
	}
}
