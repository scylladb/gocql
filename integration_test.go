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
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/scylladb/gocql/v2/internal/tests"
)

func init() {
	// Register integration-only setup that runs before any test (called from TestMain).
	// This eagerly probes tablet support so parallel tests don't race on lazy init.
	integrationTestSetup = initTabletProbes
}

// TestAuthentication verifies that gocql will work with a host configured to only accept authenticated connections
func TestAuthentication(t *testing.T) {
	t.Parallel()

	if !*flagRunAuthTest {
		t.Skip("Authentication is not configured in the target cluster")
	}

	cluster := createV2Cluster()

	cluster.Auth = PasswordAuth("cassandra", "cassandra")

	session, err := Connect(context.Background(), cluster)

	if err != nil {
		t.Fatalf("Authentication error: %s", err)
	}

	session.Close()
	cluster.Auth = PasswordAuth("cassandra", "step08-invalid-password")
	rejected, err := Connect(context.Background(), cluster)
	if err == nil {
		rejected.Close()
		t.Fatal("server accepted rejected credentials")
	}
	var authErr RequestError
	if !errors.As(err, &authErr) || authErr.GetCode() != ErrCodeCredentials {
		t.Fatalf("expected authentication rejection, got %T: %v", err, err)
	}
}

func TestGetHostsFromSystem(t *testing.T) {
	t.Parallel()

	clusterHosts := getClusterHosts()
	cluster := createV2Cluster()
	session := createV2SessionFromConfig(cluster, t, false)
	defer session.Close()
	if err := session.RefreshMetadata(context.Background()); err != nil {
		t.Fatal(err)
	}
	hosts := session.Metadata().Hosts()
	row, err := v2ReadOne(context.Background(), session, "SELECT partitioner FROM system.local")
	if err != nil {
		t.Fatal(err)
	}
	value, err := row.At(0)
	if err != nil {
		t.Fatal(err)
	}
	partitioner, err := value.Text()
	tests.AssertTrue(t, "err == nil", err == nil)
	tests.AssertEqual(t, "len(hosts)", len(clusterHosts), len(hosts))
	tests.AssertTrue(t, "len(partitioner) != 0", len(partitioner.String()) != 0)
}

// TestRingDiscovery makes sure that you can autodiscover other cluster members
// when you seed a cluster config with just one node
func TestRingDiscovery(t *testing.T) {
	t.Parallel()

	clusterHosts := getClusterHosts()
	cluster := createV2Cluster()
	cluster.ContactPoints = clusterHosts[:1]

	session := createV2SessionFromConfig(cluster, t, false)
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
	t.Parallel()

	clusterHosts := getClusterHosts()
	if len(clusterHosts) < 2 {
		t.Skip("skipping because we don't have 2 or more hosts")
	}
	cluster := createV2Cluster()
	rr, err := NewRoutingPolicy(RoutingOptions{})
	if err != nil {
		t.Fatal(err)
	}
	profile := cluster.DefaultProfile.Snapshot()
	profile.Routing = rr
	if err := cluster.DefaultProfile.Replace(profile); err != nil {
		t.Fatal(err)
	}
	// we'll filter out the second host
	filtered := clusterHosts[1]
	cluster.ContactPoints = clusterHosts[:1]
	cluster.HostFilter = func(host HostSnapshot) bool { return host.Address.Addr().String() != filtered }
	session := createV2SessionFromConfig(cluster, t, false)
	defer session.Close()

	plan := rr.Plan(RoutingInfo{}, session.Metadata())
	targets := make(map[UUID]bool)
	for target, ok := plan.Next(); ok; target, ok = plan.Next() {
		targets[target.HostID] = true
	}
	tests.AssertEqual(t, "eligible routing hosts", len(clusterHosts)-1, len(targets))
	for _, host := range session.Metadata().Hosts() {
		if host.Address.Addr().String() == filtered && (host.Enabled || targets[host.ID]) {
			t.Fatalf("filtered host remains eligible: %+v", host)
		}
	}
	if ch := session.control.getConn(); ch != nil && ch.host.ConnectAddress().String() == filtered {
		t.Fatal("control connection selected a filtered host")
	}
}

// TestHostFilterInitial ensures that host filtering works for the initial
// connection including the control connection
func TestHostFilterInitial(t *testing.T) {
	t.Parallel()

	clusterHosts := getClusterHosts()
	if len(clusterHosts) < 2 {
		t.Skip("skipping because we don't have 2 or more hosts")
	}
	cluster := createV2Cluster()
	rr, err := NewRoutingPolicy(RoutingOptions{})
	if err != nil {
		t.Fatal(err)
	}
	profile := cluster.DefaultProfile.Snapshot()
	profile.Routing = rr
	if err := cluster.DefaultProfile.Replace(profile); err != nil {
		t.Fatal(err)
	}
	// we'll filter out the second host
	filtered := clusterHosts[1]
	cluster.HostFilter = func(host HostSnapshot) bool { return host.Address.Addr().String() != filtered }
	session := createV2SessionFromConfig(cluster, t, false)
	defer session.Close()

	plan := rr.Plan(RoutingInfo{}, session.Metadata())
	targets := make(map[UUID]bool)
	for target, ok := plan.Next(); ok; target, ok = plan.Next() {
		targets[target.HostID] = true
	}
	tests.AssertEqual(t, "eligible routing hosts", len(clusterHosts)-1, len(targets))
	for _, host := range session.Metadata().Hosts() {
		if host.Address.Addr().String() == filtered && (host.Enabled || targets[host.ID]) {
			t.Fatalf("filtered host remains eligible: %+v", host)
		}
	}
	if ch := session.control.getConn(); ch != nil && ch.host.ConnectAddress().String() == filtered {
		t.Fatal("control connection selected a filtered host")
	}
}

func TestApplicationInformation(t *testing.T) {
	t.Parallel()

	cluster := createV2Cluster()
	s, err := Connect(context.Background(), cluster)
	if err != nil {
		t.Fatalf("ApplicationInformation error: %s", err)
	}
	defer s.Close()
	var clientsTableName string
	for _, tableName := range []string{"system_views.clients", "system.clients"} {
		_, err = readV2ClientOptions(context.Background(), s, "select client_options from "+tableName)
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
			cluster := createV2Cluster()
			cluster.Identity = IdentityOptions{ApplicationName: tcase.name, ApplicationVersion: tcase.version, ClientID: tcase.clientID}
			s, err := Connect(context.Background(), cluster)
			if err != nil {
				t.Fatalf("failed to connect to the cluster: %s", err)
			}
			defer s.Close()

			rows, err := readV2ClientOptions(context.Background(), s, "select client_options from "+clientsTableName)
			if err != nil {
				t.Fatalf("failed to execute query: %v", err)
			}
			found := false
			for _, row := range rows {
				if row[sessionIDStartupKey] != s.ID() {
					continue
				}
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
			if !found {
				t.Fatalf("failed to find the application info row")
			}
		})
	}

}

// TestDriverConfigReporting verifies end to end that a session reports one
// SESSION_ID shared by all of its connections, and that DRIVER_CONFIG is
// reported exactly once, by the control connection, mirroring the existing
// TestApplicationInformation precedent.
func TestDriverConfigReporting(t *testing.T) {
	t.Parallel()

	cluster := createV2Cluster()
	s, err := Connect(context.Background(), cluster)
	if err != nil {
		t.Fatalf("failed to connect to the cluster: %s", err)
	}
	defer s.Close()

	// Read the id through the exported accessor rather than the field: an
	// application correlating its own logs with system.clients has only the
	// accessor, so that is the path worth exercising end to end.
	wantSessionID := s.ID()
	if wantSessionID == "" {
		t.Fatal("expected the session to have a non-empty id")
	}

	var clientsTableName string
	for _, tableName := range []string{"system_views.clients", "system.clients"} {
		if _, err := readV2ClientOptions(context.Background(), s, "select client_options from "+tableName); err == nil {
			clientsTableName = tableName
			break
		}
	}
	if clientsTableName == "" {
		t.Skip("Skipping because server does not have `client_options` in clients table")
	}

	// Wait until the pools have finished filling so that the number of
	// connections this session owns stops moving. Without this the poll below
	// could reach its exit condition while connections are still being opened.
	if err := waitUntilPoolsStopFilling(context.Background(), s, 10*time.Second); err != nil {
		t.Fatalf("failed to wait for the connection pools to fill: %s", err)
	}

	// The clients table is node-local: every node lists only the connections
	// made to it. Integration runs use a three node cluster, so a load-balanced
	// query round-robins across the cluster and most polls would land on a node
	// that the control connection was never made to, and therefore never see
	// DRIVER_CONFIG at all. Run every poll on the control connection itself so
	// the rows always come from the one node whose client list is being
	// reasoned about.
	//
	// Stopping at the first row carrying DRIVER_CONFIG would also make the
	// "exactly once" assertion below vacuous: the control connection is
	// established before the pool ones, so its row is the first to appear, and a
	// regression that also reported DRIVER_CONFIG on pool connections would go
	// unnoticed because their rows are not in the table yet. Wait for every
	// connection to that node to be accounted for instead.
	var configs []string
	deadline := time.After(10 * time.Second)
	for {
		// Re-read the control connection's host on every pass: it can move if
		// the control connection reconnects, and the pool size it is compared
		// against has to belong to the same node. Re-reading the pool size also
		// means that should a pool start filling again, the bar rises with it
		// instead of leaving the loop free to exit on a stale count.
		ch := s.control.getConn()
		if ch == nil {
			t.Fatal("expected the session to have a control connection")
		}
		controlHost := ch.host

		// Connections this session has to the control connection's node: its
		// pool for that host, plus the control connection itself, which is
		// dialed outside the pool.
		wantConns := 1
		if hostPool, ok := s.pool.getPool(controlHost); ok {
			wantConns += hostPool.Size()
		}

		configs = nil
		conns := 0

		rows, err := readV2ClientOptions(context.Background(), s, "select client_options from "+clientsTableName)
		if err != nil {
			t.Fatalf("failed to execute query: %v", err)
		}
		for _, row := range rows {
			if row[sessionIDStartupKey] != wantSessionID {
				continue
			}
			conns++
			if config, ok := row[driverConfigStartupKey]; ok {
				configs = append(configs, config)
			}
		}

		// More rows than connections is fine: a connection that has already been
		// closed can linger in the clients table. Those rows cannot hide a
		// regression, they can only reveal one, since a second DRIVER_CONFIG is
		// a failure no matter which connection produced it.
		if conns >= wantConns && len(configs) > 0 {
			break
		}

		select {
		case <-deadline:
			t.Fatalf("expected all %d connections with SESSION_ID %q to node %s to be listed in %s with at least one reporting DRIVER_CONFIG within 10s, saw %d connections and %d DRIVER_CONFIG",
				wantConns, wantSessionID, controlHost.ConnectAddress(), clientsTableName, conns, len(configs))
		case <-time.After(250 * time.Millisecond):
		}
	}

	if len(configs) != 1 {
		t.Errorf("expected exactly one connection with SESSION_ID %q to report DRIVER_CONFIG, got %d", wantSessionID, len(configs))
	}
	// Assert on the decoded report rather than a byte-exact string: the payload
	// is a full configuration description whose exact serialization is the unit
	// tests' business, while what only a live server can confirm is that it
	// round-trips intact and that the ScyllaDB-gated key is decided correctly.
	for _, config := range configs {
		// STARTUP retains the versioned driver-report wire schema even though the
		// application now configures the session with Config.
		if strings.Contains(strings.ToLower(config), `"password"`) || strings.Contains(strings.ToLower(config), `"username"`) {
			t.Error("driver configuration exposes authentication credentials")
		}
		var report driverConfigReport
		if err := json.Unmarshal([]byte(config), &report); err != nil {
			t.Errorf("expected %s to be valid JSON, got %q: %v", driverConfigStartupKey, config, err)
			continue
		}
		if report.Version != driverConfigVersion {
			t.Errorf("expected %s version %d, got %d", driverConfigStartupKey, driverConfigVersion, report.Version)
		}
		if got, want := report.ControlPlane.Schema.Agreement.TimeoutMs, cluster.Metadata.AgreementTimeout.Milliseconds(); got != want {
			t.Errorf("expected schema agreement timeout-ms %d, got %d", want, got)
		}
		// server-side-ms describes the USING TIMEOUT clause, which only ScyllaDB
		// understands. The fake server the unit tests run against cannot
		// exercise either side of that gate.
		wantServerSide := false
		if ch := s.control.getConn(); ch != nil {
			wantServerSide = ch.conn.isScyllaConn()
		}
		if got := report.ControlPlane.Queries.System.Timeout.ServerSideMs != nil; got != wantServerSide {
			t.Errorf("expected server-side-ms present=%v against this server, got %v", wantServerSide, got)
		}
	}
}

func TestWriteFailure(t *testing.T) {
	t.Parallel()

	requireCassandraFixtures(t)
	cluster := createV2Cluster()
	session, err := Connect(context.Background(), cluster)
	if err != nil {
		t.Fatal("create session:", err)
	}
	defer session.Close()

	if err := v2CreateTable(context.Background(), session, fmt.Sprintf("CREATE KEYSPACE IF NOT EXISTS %s WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': %d}", fixtureKeyspace("_fail"), *flagRF)); err != nil {
		t.Fatal(err)
	}
	table := testTableName(t, session.ID())

	if err := v2CreateTable(context.Background(), session, fmt.Sprintf("CREATE TABLE %s.%s (id int,value int,PRIMARY KEY (id))", fixtureKeyspace("_fail"), table)); err != nil {
		t.Fatalf("failed to create table with error '%v'", err)
	}
	if _, err := v2Exec(context.Background(), session, fmt.Sprintf(`INSERT INTO %s.%s (id, value) VALUES (1, 1)`, fixtureKeyspace("_fail"), table)); err != nil {
		var errWrite *RequestErrWriteFailure
		if errors.As(err, &errWrite) {
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

	if _, err = v2Exec(context.Background(), session, "DROP KEYSPACE "+fixtureKeyspace("_fail")); err != nil {
		t.Fatal(err)
	}
}

func TestCustomPayloadMessages(t *testing.T) {
	t.Parallel()

	requireCassandraFixtures(t)
	cluster := createV2Cluster()
	session := createV2SessionFromConfig(cluster, t, false)
	defer session.Close()

	table := testTableName(t, session.ID())

	if err := v2CreateTable(context.Background(), session, fmt.Sprintf("CREATE TABLE %s.%s (id int, value int, PRIMARY KEY (id))", session.cfg.Keyspace, table)); err != nil {
		t.Fatal(err)
	}

	customPayload := []PayloadEntry{{Key: "a", Value: Nullable[Bytes]{Valid: true, Value: BorrowBytes([]byte{10, 20})}}, {Key: "b", Value: Nullable[Bytes]{Valid: true, Value: BorrowBytes([]byte{20, 30})}}}
	options := Options{Consistency: Option[Consistency]{Value: One, Set: true}, CustomPayload: customPayload}

	// Prepared SELECT retains the EXECUTE custom-payload path.
	selectStatement := Statement[[]Value, DynamicRow]{CQL: fmt.Sprintf("SELECT id FROM %s where id = ?", table), Params: DynamicParams, Rows: DynamicRows}
	prepared, err := Prepare(context.Background(), session, selectStatement, PrepareOptions{})
	if err != nil {
		t.Fatal(err)
	}
	bound, err := BindOwned(prepared, []Value{IntValue(42)})
	if err != nil {
		t.Fatal(err)
	}
	defer bound.Close()
	page, err := RunPage(context.Background(), session, bound, StartPage(), options)
	if err != nil {
		t.Fatal(err)
	}
	assertV2CustomPayload(t, customPayload, page.Info().CustomPayload)
	if err := page.Close(); err != nil {
		t.Fatal(err)
	}

	// Unprepared INSERT retains the QUERY custom-payload path.
	insert, err := BindUnpreparedOwned(Statement[Unit, Unit]{CQL: fmt.Sprintf("INSERT INTO %s(id,value) VALUES(1, 1)", table), Params: NoParams, Rows: NoRows}, NewColumns(), Unit{})
	if err != nil {
		t.Fatal(err)
	}
	defer insert.Close()
	result, err := Exec(context.Background(), session, insert, options)
	if err != nil {
		t.Fatal(err)
	}
	assertV2CustomPayload(t, customPayload, result.CustomPayload)

	// Native BATCH retains its independent custom-payload frame path.
	item, err := BatchEntry(insert)
	if err != nil {
		t.Fatal(err)
	}
	result, err = ExecBatch(context.Background(), session, LoggedBatch, []BatchItem{item}, options)
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}
	assertV2CustomPayload(t, customPayload, result.CustomPayload)
}

func TestCustomPayloadValues(t *testing.T) {
	t.Parallel()
	requireCassandraFixtures(t)
	session := createV2Session(t)
	defer session.Close()
	table := testTableName(t, session.ID())
	if err := v2CreateTable(context.Background(), session, fmt.Sprintf("CREATE TABLE %s.%s (id int, value int, PRIMARY KEY (id))", session.cfg.Keyspace, table)); err != nil {
		t.Fatal(err)
	}

	values := [][]PayloadEntry{
		{{Key: "a", Value: Nullable[Bytes]{Valid: true, Value: BorrowBytes([]byte{10, 20})}}, {Key: "b", Value: Nullable[Bytes]{Valid: true, Value: BorrowBytes([]byte{20, 30})}}},
		nil,
		{{Key: "a", Value: Nullable[Bytes]{Valid: true, Value: BorrowBytes([]byte{10, 20})}}, {Key: "b", Value: Nullable[Bytes]{}}},
	}
	statement := Statement[[]Value, DynamicRow]{CQL: fmt.Sprintf("SELECT id FROM %s where id = ?", table), Params: DynamicParams, Rows: DynamicRows}
	prepared, err := Prepare(context.Background(), session, statement, PrepareOptions{})
	if err != nil {
		t.Fatal(err)
	}
	bound, err := BindOwned(prepared, []Value{IntValue(42)})
	if err != nil {
		t.Fatal(err)
	}
	defer bound.Close()
	for _, customPayload := range values {
		page, err := RunPage(context.Background(), session, bound, StartPage(), Options{Consistency: Option[Consistency]{Value: One, Set: true}, CustomPayload: customPayload})
		if err != nil {
			t.Fatal(err)
		}
		assertV2CustomPayload(t, customPayload, page.Info().CustomPayload)
		if err := page.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

func assertV2CustomPayload(t testing.TB, want, got []PayloadEntry) {
	t.Helper()
	decode := func(entries []PayloadEntry) map[string][]byte {
		if entries == nil {
			return nil
		}
		result := make(map[string][]byte, len(entries))
		for _, entry := range entries {
			if _, exists := result[entry.Key]; exists {
				t.Fatalf("duplicate custom-payload key %q", entry.Key)
			}
			if entry.Value.Valid {
				result[entry.Key] = entry.Value.Value.OwnedBytes()
			} else {
				result[entry.Key] = nil
			}
		}
		return result
	}
	if !reflect.DeepEqual(decode(want), decode(got)) {
		t.Fatalf("received custom payload differs: want %v, got %v", decode(want), decode(got))
	}
}

func requireCassandraFixtures(t *testing.T) {
	t.Helper()
	if *flagDistribution != "cassandra" || !*flagCassandraFixtures {
		t.Skip("requires Cassandra write-failure/custom-payload fixtures and -runcassandrafixtures")
	}
}

func TestSessionAwaitSchemaAgreement(t *testing.T) {
	t.Parallel()

	session := createV2Session(t)
	defer session.Close()

	if err := session.AwaitSchemaAgreement(context.Background()); err != nil {
		t.Fatalf("expected session.AwaitSchemaAgreement to not return an error but got '%v'", err)
	}
}

func TestSessionAwaitSchemaAgreementSessionClosed(t *testing.T) {
	t.Parallel()

	session := createV2Session(t)
	session.Close()

	if err := session.AwaitSchemaAgreement(context.Background()); !errors.Is(err, ErrClosed) {
		t.Fatalf("expected session.AwaitSchemaAgreement to return ErrClosed but got '%v'", err)
	}

}

func TestSessionAwaitSchemaAgreementContextCanceled(t *testing.T) {
	t.Parallel()

	session := createV2Session(t)
	defer session.Close()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if err := session.AwaitSchemaAgreement(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected session.AwaitSchemaAgreement to return 'context canceled' but got '%v'", err)
	}

}

func TestNewConnectWithLowTimeout(t *testing.T) {
	t.Parallel()

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
				getCluster                 func() Config
				connect                    TestExpectation
				regularQuery               TestExpectation
				controlQuery               TestExpectation
				controlQueryAfterReconnect TestExpectation
			}{
				{
					name: "Timeout",
					getCluster: func() Config {
						cluster := createV2Cluster()
						profile := cluster.DefaultProfile.Snapshot()
						profile.Timeout = lowTimeout
						if err := cluster.DefaultProfile.Replace(profile); err != nil {
							t.Fatal(err)
						}
						return cluster
					},
					connect: Pass,
					// At 100ns the query can complete before the deadline fires,
					// the same race already tolerated below for WriteTimeout/ReadTimeout.
					regularQuery:               canPassOnHighTimeout,
					controlQuery:               Pass,
					controlQueryAfterReconnect: Pass,
				},
				{
					name: "MetadataSchemaRequestTimeout",
					getCluster: func() Config {
						cluster := createV2Cluster()
						cluster.Metadata.RequestTimeout = lowTimeout
						return cluster
					},
					connect:                    Pass,
					regularQuery:               Pass,
					controlQuery:               CanPass,
					controlQueryAfterReconnect: CanPass,
				},
				{
					name: "WriteTimeout",
					getCluster: func() Config {
						cluster := createV2Cluster()
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
					getCluster: func() Config {
						cluster := createV2Cluster()
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
					getCluster: func() Config {
						cluster := createV2Cluster()
						profile := cluster.DefaultProfile.Snapshot()
						profile.Timeout = lowTimeout
						if err := cluster.DefaultProfile.Replace(profile); err != nil {
							t.Fatal(err)
						}
						cluster.ReadTimeout = lowTimeout
						cluster.WriteTimeout = lowTimeout
						cluster.Metadata.RequestTimeout = lowTimeout
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
						s, err = Connect(context.Background(), tcase.getCluster())
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
							_, err = v2ReadOne(context.Background(), s, "SELECT key FROM system.local WHERE key='local'")
							match(t, tcase.regularQuery, err)
						})
					}

					if tcase.controlQuery != DontRun {
						t.Run("Query from control connection", func(t *testing.T) {
							err = queryV2ControlKey(context.Background(), s)
							match(t, tcase.controlQuery, err)
						})
					}

					if tcase.controlQueryAfterReconnect != DontRun {
						t.Run("Query from control connection after reconnect", func(t *testing.T) {
							s, err = Connect(context.Background(), tcase.getCluster())
							if err != nil {
								t.Fatal("failed to create session", err.Error())
							}
							defer s.Close()
							err = s.control.reconnect()
							if err != nil {
								t.Fatalf("failed to reconnect to control connection: %v", err)
							}
							err = queryV2ControlKey(context.Background(), s)
							match(t, tcase.controlQueryAfterReconnect, err)
						})
					}
				})
			}
		})
	}
}

// readV2ClientOptions keeps node-local startup reports on the control connection.
func readV2ClientOptions(ctx context.Context, session *Session, cql string) ([]map[string]string, error) {
	handle := session.control.getConn()
	if handle == nil {
		return nil, errNoControl
	}
	conn, ok := handle.conn.(*Conn)
	if !ok {
		return nil, fmt.Errorf("control connection does not support typed queries")
	}
	statement := Statement[Unit, Mapping[Text, Text]]{CQL: cql, Params: NoParams, Rows: OneColumn(MapDecoder(TextCodec, TextCodec))}
	rows, err := readV2SystemRows(ctx, conn, statement, NewColumns(), Unit{})
	if err != nil {
		return nil, err
	}
	result := make([]map[string]string, 0, len(rows))
	for _, row := range rows {
		values := make(map[string]string)
		entries := row.Iterator()
		for entries.Next() {
			entry := entries.Value()
			values[entry.Key.String()] = entry.Value.String()
		}
		if err := entries.Err(); err != nil {
			return nil, err
		}
		result = append(result, values)
	}
	return result, nil
}

func queryV2ControlKey(ctx context.Context, session *Session) error {
	handle := session.control.getConn()
	if handle == nil {
		return errNoControl
	}
	conn, ok := handle.conn.(*Conn)
	if !ok {
		return fmt.Errorf("control connection does not support typed queries")
	}
	statement := Statement[Unit, Text]{CQL: "SELECT key FROM system.local WHERE key='local'", Params: NoParams, Rows: OneColumn(TextCodec)}
	_, err := readV2SystemRows(ctx, conn, statement, NewColumns(), Unit{})
	return err
}
