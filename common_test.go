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
	"hash/fnv"
	"log"
	"net"
	"os"
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

// integrationTestSetup is set by an init() in an integration-tagged file to run
// one-time setup (e.g. tablet probes) before any test executes.
var integrationTestSetup func()

func init() {
	flag.Var(&flagCassVersion, "gocql.cversion", "the cassandra version being tested against")

	log.SetFlags(log.Lshortfile | log.LstdFlags)
}

func TestMain(m *testing.M) {
	flag.Parse()
	if integrationTestSetup != nil {
		integrationTestSetup()
	}
	os.Exit(m.Run())
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
var isTabletsSupportedOnce sync.Once

func isTabletsSupported() bool {
	isTabletsSupportedOnce.Do(probeTabletsSupported)
	if isTabletsSupportedFlag == nil {
		return false
	}
	return *isTabletsSupportedFlag
}

func probeTabletsSupported() {
	s, err := createCluster().CreateSession()
	if err != nil {
		panic(fmt.Errorf("failed to create session: %v", err))
	}
	defer s.Close()

	res := make(map[string]any)
	err = s.Query("select * from system.local").MapScan(res)
	if err != nil {
		panic(fmt.Errorf("failed to read system.local: %v", err))
	}

	features, _ := res["supported_features"]
	featuresCasted, _ := features.(string)
	for _, feature := range strings.Split(featuresCasted, ",") {
		if feature == "TABLETS" {
			result := true
			isTabletsSupportedFlag = &result
			return
		}
	}
	result := false
	isTabletsSupportedFlag = &result
}

var isTabletsAutoEnabledFlag *bool
var isTabletsAutoEnabledOnce sync.Once

func isTabletsAutoEnabled() bool {
	isTabletsAutoEnabledOnce.Do(probeTabletsAutoEnabled)
	if isTabletsAutoEnabledFlag == nil {
		return false
	}
	return *isTabletsAutoEnabledFlag
}

func probeTabletsAutoEnabled() {
	s, err := createCluster().CreateSession()
	if err != nil {
		panic(fmt.Errorf("failed to create session: %v", err))
	}
	defer s.Close()

	err = s.Query("DROP KEYSPACE IF EXISTS gocql_check_tablets_enabled").Exec()
	if err != nil {
		panic(fmt.Errorf("failed to delete keyspace: %v", err))
	}
	err = s.Query("CREATE KEYSPACE gocql_check_tablets_enabled WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': '1'}").Exec()
	if err != nil {
		panic(fmt.Errorf("failed to create keyspace: %v", err))
	}

	res := make(map[string]any)
	err = s.Query("describe keyspace gocql_check_tablets_enabled").MapScan(res)
	if err != nil {
		panic(fmt.Errorf("failed to describe keyspace: %v", err))
	}

	err = s.Query("DROP KEYSPACE IF EXISTS gocql_check_tablets_enabled").Exec()
	if err != nil {
		panic(fmt.Errorf("failed to drop probe keyspace: %v", err))
	}

	createStmt, _ := res["create_statement"]
	createStmtCasted, _ := createStmt.(string)
	result := strings.Contains(strings.ToLower(createStmtCasted), "and tablets")
	isTabletsAutoEnabledFlag = &result
}

// initTabletProbes runs the tablet-support and tablet-auto-enabled probes eagerly.
// Called from TestMain before any tests run to avoid races with parallel test startup.
func initTabletProbes() {
	probeTabletsSupported()
	if isTabletsSupportedFlag != nil && *isTabletsSupportedFlag {
		probeTabletsAutoEnabled()
	}
}

func createTable(s *Session, table string) error {
	if err := s.Query(table).RetryPolicy(&SimpleRetryPolicy{NumRetries: 3}).Idempotent(true).Exec(); err != nil {
		log.Printf("error creating table table=%q err=%v\n", table, err)
		return err
	}

	if err := s.control.awaitSchemaAgreement(); err != nil {
		log.Printf("error waiting for schema agreement post create table=%q err=%v\n", table, err)
		return err
	}

	// Invalidate schema cache to avoid races with debounced schema events.
	// Use per-table invalidation when possible (cheaper than keyspace-wide)
	// to reduce cache thrashing when parallel tests all perform DDL on the
	// same shared keyspace. Falls back to keyspace-wide invalidation for
	// non-TABLE DDL (e.g. DROP KEYSPACE, CREATE TYPE).
	ks, tbl := extractKeyspaceTableFromDDL(table)
	if ks == "" {
		ks = s.cfg.Keyspace
	}
	if ks != "" && tbl != "" {
		s.metadataDescriber.invalidateTableSchema(ks, tbl)
	} else if ks != "" {
		s.metadataDescriber.invalidateKeyspaceSchema(ks)
	}

	return nil
}

// createTables executes multiple DDL statements with a single
// awaitSchemaAgreement call at the end, reducing the serialization bottleneck
// when parallel tests all need schema agreement. Each statement is still
// executed and cache-invalidated individually.
func createTables(s *Session, ddls ...string) error {
	for _, ddl := range ddls {
		if err := s.Query(ddl).RetryPolicy(&SimpleRetryPolicy{NumRetries: 3}).Idempotent(true).Exec(); err != nil {
			log.Printf("error creating table table=%q err=%v\n", ddl, err)
			return err
		}
	}

	if err := s.control.awaitSchemaAgreement(); err != nil {
		log.Printf("error waiting for schema agreement after batch DDL err=%v\n", err)
		return err
	}

	// Invalidate caches for all affected tables/keyspaces.
	for _, ddl := range ddls {
		ks, tbl := extractKeyspaceTableFromDDL(ddl)
		if ks == "" {
			ks = s.cfg.Keyspace
		}
		if ks != "" && tbl != "" {
			s.metadataDescriber.invalidateTableSchema(ks, tbl)
		} else if ks != "" {
			s.metadataDescriber.invalidateKeyspaceSchema(ks)
		}
	}

	return nil
}

// extractKeyspaceTableFromDDL extracts the keyspace and table names from a DDL
// statement like "CREATE TABLE gocql_test.table_name (...)".
// Returns ("", "") for non-TABLE DDL or when keyspace is not qualified.
func extractKeyspaceTableFromDDL(ddl string) (keyspace, table string) {
	upper := strings.ToUpper(ddl)
	idx := strings.Index(upper, "TABLE")
	if idx < 0 {
		return "", ""
	}
	rest := strings.TrimSpace(ddl[idx+len("TABLE"):])
	// Skip optional "IF [NOT] EXISTS" between TABLE and the name.
	upperRest := strings.ToUpper(rest)
	if strings.HasPrefix(upperRest, "IF NOT EXISTS") {
		rest = strings.TrimSpace(rest[len("IF NOT EXISTS"):])
	} else if strings.HasPrefix(upperRest, "IF EXISTS") {
		rest = strings.TrimSpace(rest[len("IF EXISTS"):])
	}
	// Extract keyspace.table
	dot := strings.Index(rest, ".")
	if dot < 0 {
		return "", ""
	}
	ks := rest[:dot]
	// Extract table name: everything after the dot until whitespace or '('
	nameRest := rest[dot+1:]
	end := strings.IndexAny(nameRest, " \t\n(")
	if end < 0 {
		return ks, nameRest
	}
	return ks, nameRest[:end]
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
	// Create a fresh policy to avoid sharing the policy instance with the caller.
	// Shallow copy of cluster config shares the HostSelectionPolicy pointer, which
	// would cause "sharing token aware host selection policy between sessions" panic
	// when both createKeyspace's session and the caller's session try to Init() it.
	c.PoolConfig.HostSelectionPolicy = nil
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
	table1 := testTableName(t, "1")
	table2 := testTableName(t, "2")
	view1 := testTableName(t, "view1")
	view2 := testTableName(t, "view2")
	if err := session.Query(fmt.Sprintf(`CREATE TABLE IF NOT EXISTS gocql_test.%s (
		    userid text,
		    year int,
		    month int,
    		    PRIMARY KEY (userid));`, table1)).Exec(); err != nil {
		t.Fatalf("failed to create materialized view with err: %v", err)
	}
	if err := session.Query(fmt.Sprintf(`CREATE TABLE IF NOT EXISTS gocql_test.%s (
		    userid text,
		    year int,
		    month int,
    		    PRIMARY KEY (userid));`, table2)).Exec(); err != nil {
		t.Fatalf("failed to create materialized view with err: %v", err)
	}
	if err := session.Query(fmt.Sprintf(`CREATE MATERIALIZED VIEW IF NOT EXISTS gocql_test.%s AS
		   SELECT year, month, userid
		   FROM gocql_test.%s
		   WHERE year IS NOT NULL AND month IS NOT NULL AND userid IS NOT NULL
		   PRIMARY KEY (userid, year);`, view1, table1)).Exec(); err != nil {
		t.Fatalf("failed to create materialized view with err: %v", err)
	}
	if err := session.Query(fmt.Sprintf(`CREATE MATERIALIZED VIEW IF NOT EXISTS gocql_test.%s AS
		   SELECT year, month, userid
		   FROM gocql_test.%s
		   WHERE year IS NOT NULL AND month IS NOT NULL AND userid IS NOT NULL
		   PRIMARY KEY (userid, year);`, view2, table2)).Exec(); err != nil {
		t.Fatalf("failed to create materialized view with err: %v", err)
	}
}

func createFunctions(t *testing.T, session *Session) {
	fnState := testTableName(t, "avgstate")
	fnFinal := testTableName(t, "avgfinal")
	if err := session.Query(fmt.Sprintf(`
		CREATE OR REPLACE FUNCTION gocql_test.%s ( state tuple<int,bigint>, val int )
		CALLED ON NULL INPUT
		RETURNS tuple<int,bigint>
		LANGUAGE java AS
		$$if (val !=null) {state.setInt(0, state.getInt(0)+1); state.setLong(1, state.getLong(1)+val.intValue());}return state;$$;	`, fnState)).Exec(); err != nil {
		t.Fatalf("failed to create function with err: %v", err)
	}
	if err := session.Query(fmt.Sprintf(`
		CREATE OR REPLACE FUNCTION gocql_test.%s ( state tuple<int,bigint> )
		CALLED ON NULL INPUT
		RETURNS double
		LANGUAGE java AS
		$$double r = 0; if (state.getInt(0) == 0) return null; r = state.getLong(1); r/= state.getInt(0); return Double.valueOf(r);$$
	`, fnFinal)).Exec(); err != nil {
		t.Fatalf("failed to create function with err: %v", err)
	}
}

func createAggregate(t *testing.T, session *Session) {
	fnState := testTableName(t, "avgstate")
	fnFinal := testTableName(t, "avgfinal")
	aggName := testTableName(t, "average")
	aggName2 := testTableName(t, "average2")
	createFunctions(t, session)
	if err := session.Query(fmt.Sprintf(`
		CREATE OR REPLACE AGGREGATE gocql_test.%s(int)
		SFUNC %s
		STYPE tuple<int,bigint>
		FINALFUNC %s
		INITCOND (0,0);
	`, aggName, fnState, fnFinal)).Exec(); err != nil {
		t.Fatalf("failed to create aggregate with err: %v", err)
	}
	if err := session.Query(fmt.Sprintf(`
		CREATE OR REPLACE AGGREGATE gocql_test.%s(int)
		SFUNC %s
		STYPE tuple<int,bigint>
		FINALFUNC %s
		INITCOND (0,0);
	`, aggName2, fnState, fnFinal)).Exec(); err != nil {
		t.Fatalf("failed to create aggregate with err: %v", err)
	}
}

const maxCQLIdentifierLen = 48
const testTableNameHashLen = 16

// testTableName builds a CQL-safe table name from t.Name() and optional parts.
// Truncates to 48 chars (CQL limit) using <first-n>_<fnv64a hash>_<last-n>
// when needed.
func testTableName(t testing.TB, parts ...string) string {
	name := strings.ToLower(t.Name())
	for _, p := range parts {
		name += "_" + strings.ToLower(p)
	}

	var b strings.Builder
	prevUnderscore := false
	for _, r := range name {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') {
			b.WriteRune(r)
			prevUnderscore = false
		} else if !prevUnderscore {
			b.WriteByte('_')
			prevUnderscore = true
		}
	}
	name = strings.Trim(b.String(), "_")

	if len(name) > maxCQLIdentifierLen {
		h := fnv.New64a()
		h.Write([]byte(name))
		hash := fmt.Sprintf("%016x", h.Sum64()) // 16 hex chars for better collision resistance
		remaining := maxCQLIdentifierLen - testTableNameHashLen - 2
		prefixLen := remaining / 2
		suffixLen := remaining - prefixLen
		name = name[:prefixLen] + "_" + hash + "_" + name[len(name)-suffixLen:]
	}
	return name
}

// testTypeName builds a CQL-safe UDT type name from t.Name() and optional parts.
// Analogous to testTableName but intended for CREATE TYPE / frozen<type> references.
func testTypeName(t testing.TB, parts ...string) string {
	return testTableName(t, parts...)
}

// testKeyspaceName builds a CQL-safe keyspace name from t.Name() and optional parts.
// Analogous to testTableName but intended for CREATE/DROP KEYSPACE statements.
func testKeyspaceName(t testing.TB, parts ...string) string {
	return testTableName(t, parts...)
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
