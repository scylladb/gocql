//go:build unit
// +build unit

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
 * Copyright (c) 2012, The Gocql authors,
 * provided under the BSD-3-Clause License.
 * See the NOTICE file distributed with this work for additional information.
 */

package gocql

import (
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/assert"

	frm "github.com/gocql/gocql/internal/frame"

	"github.com/gocql/gocql/internal/streams"
)

const (
	defaultProto = protoVersion3
)

type brokenDNSResolver struct{}

func (b brokenDNSResolver) LookupIP(host string) ([]net.IP, error) {
	err := errors.New("this error comes from mocked broken resolver")
	return nil, &net.DNSError{
		UnwrapErr: err,
		Err:       err.Error(),
		Server:    host,
	}
}

func TestApprove(t *testing.T) {
	tests := map[bool]bool{
		approve("org.apache.cassandra.auth.PasswordAuthenticator", []string{}):                                             true,
		approve("org.apache.cassandra.auth.MutualTlsWithPasswordFallbackAuthenticator", []string{}):                        true,
		approve("org.apache.cassandra.auth.MutualTlsAuthenticator", []string{}):                                            true,
		approve("com.instaclustr.cassandra.auth.SharedSecretAuthenticator", []string{}):                                    true,
		approve("com.datastax.bdp.cassandra.auth.DseAuthenticator", []string{}):                                            true,
		approve("io.aiven.cassandra.auth.AivenAuthenticator", []string{}):                                                  true,
		approve("com.amazon.helenus.auth.HelenusAuthenticator", []string{}):                                                true,
		approve("com.ericsson.bss.cassandra.ecaudit.auth.AuditAuthenticator", []string{}):                                  true,
		approve("com.scylladb.auth.SaslauthdAuthenticator", []string{}):                                                    true,
		approve("com.scylladb.auth.TransitionalAuthenticator", []string{}):                                                 true,
		approve("com.instaclustr.cassandra.auth.InstaclustrPasswordAuthenticator", []string{}):                             true,
		approve("com.apache.cassandra.auth.FakeAuthenticator", []string{}):                                                 true,
		approve("com.apache.cassandra.auth.FakeAuthenticator", nil):                                                        true,
		approve("com.apache.cassandra.auth.FakeAuthenticator", []string{"com.apache.cassandra.auth.FakeAuthenticator"}):    true,
		approve("com.apache.cassandra.auth.FakeAuthenticator", []string{"com.apache.cassandra.auth.NotFakeAuthenticator"}): false,
	}
	for k, v := range tests {
		if k != v {
			t.Fatalf("expected '%v', got '%v'", k, v)
		}
	}
}

func testCluster(proto frm.ProtoVersion, addresses ...string) *ClusterConfig {
	cluster := NewCluster(addresses...)
	cluster.ProtoVersion = int(proto)
	cluster.disableControlConn = true
	cluster.PoolConfig.HostSelectionPolicy = RoundRobinHostPolicy()
	return cluster
}

func TestSimple(t *testing.T) {
	srv := NewTestServer(t, defaultProto, context.Background())
	defer srv.Stop()

	cluster := testCluster(defaultProto, srv.Address)
	db, err := cluster.CreateSession()
	if err != nil {
		t.Fatalf("0x%x: NewCluster: %v", defaultProto, err)
	}

	if err := db.Query("void").Exec(); err != nil {
		t.Fatalf("0x%x: %v", defaultProto, err)
	}
}

func TestSSLSimple(t *testing.T) {
	srv := NewSSLTestServer(t, defaultProto, context.Background())
	defer srv.Stop()

	db, err := createTestSslCluster(srv.Address, defaultProto, true).CreateSession()
	if err != nil {
		t.Fatalf("0x%x: NewCluster: %v", defaultProto, err)
	}

	if err := db.Query("void").Exec(); err != nil {
		t.Fatalf("0x%x: %v", defaultProto, err)
	}
}

func TestSSLSimpleNoClientCert(t *testing.T) {
	srv := NewSSLTestServer(t, defaultProto, context.Background())
	defer srv.Stop()

	db, err := createTestSslCluster(srv.Address, defaultProto, false).CreateSession()
	if err != nil {
		t.Fatalf("0x%x: NewCluster: %v", defaultProto, err)
	}

	if err := db.Query("void").Exec(); err != nil {
		t.Fatalf("0x%x: %v", defaultProto, err)
	}
}

func createTestSslCluster(addr string, proto frm.ProtoVersion, useClientCert bool) *ClusterConfig {
	cluster := testCluster(proto, addr)
	sslOpts := &SslOptions{
		CaPath:                 "testdata/pki/ca.crt",
		EnableHostVerification: false,
	}

	if useClientCert {
		sslOpts.CertPath = "testdata/pki/gocql.crt"
		sslOpts.KeyPath = "testdata/pki/gocql.key"
	}

	cluster.SslOpts = sslOpts
	return cluster
}

func TestClosed(t *testing.T) {
	t.Skip("Skipping the execution of TestClosed for now to try to concentrate on more important test failures on Travis")

	srv := NewTestServer(t, defaultProto, context.Background())
	defer srv.Stop()

	session, err := newTestSession(defaultProto, srv.Address)
	if err != nil {
		t.Fatalf("0x%x: NewCluster: %v", defaultProto, err)
	}

	session.Close()

	if err := session.Query("void").Exec(); err != ErrSessionClosed {
		t.Fatalf("0x%x: expected %#v, got %#v", defaultProto, ErrSessionClosed, err)
	}
}

func newTestSession(proto frm.ProtoVersion, addresses ...string) (*Session, error) {
	return testCluster(proto, addresses...).CreateSession()
}

var _ DNSResolver = brokenDNSResolver{}

func TestDNSLookupConnected(t *testing.T) {
	log := &testLogger{}

	// Override the default DNS resolver and restore at the end

	srv := NewTestServer(t, defaultProto, context.Background())
	defer srv.Stop()

	cluster := NewCluster("cassandra1.invalid", srv.Address, "cassandra2.invalid")
	cluster.Logger = log
	cluster.ProtoVersion = int(defaultProto)
	cluster.disableControlConn = true
	cluster.DNSResolver = brokenDNSResolver{}

	// CreateSession() should attempt to resolve the DNS name "cassandraX.invalid"
	// and fail, but continue to connect via srv.Address
	_, err := cluster.CreateSession()
	if err != nil {
		t.Fatal("CreateSession() should have connected")
	}

	if !strings.Contains(log.String(), "failed to resolve endpoint") {
		t.Fatalf("Expected to receive 'failed to resolve endpoint' log message  - got '%s' instead", log.String())
	}
}

func TestDNSLookupError(t *testing.T) {
	log := &testLogger{}

	// Override the default DNS resolver and restore at the end
	hosts := []string{"cassandra1.invalid", "cassandra2.invalid"}

	cluster := NewCluster(hosts...)
	cluster.Logger = log
	cluster.ProtoVersion = int(defaultProto)
	cluster.disableControlConn = true
	cluster.DNSResolver = brokenDNSResolver{}

	// CreateSession() should attempt to resolve each DNS name "cassandraX.invalid"
	// and fail since it could not resolve any dns entries
	_, err := cluster.CreateSession()
	if err == nil {
		t.Fatal("CreateSession() should have returned an error")
	}

	if !strings.Contains(log.String(), "failed to resolve endpoint") {
		t.Fatalf("Expected to receive 'failed to resolve endpoint' log message  - got '%s' instead", log.String())
	}

	if !strings.Contains(err.Error(), "unable to create session: failed to resolve any of the provided hostnames") {
		t.Fatalf("Expected CreateSession() to fail with error message that contains 'unable to create session: failed to resolve any of the provided hostnames'")
	}

	for _, host := range hosts {
		expected := fmt.Sprintf("failed to resolve endpoint \"%s\": lookup  on %s: this error comes from mocked broken resolver", host, host)
		if !strings.Contains(err.Error(), expected) {
			t.Fatalf("Expected to fail with error message that contains '%s'", expected)
		}
	}
}

func TestStartupTimeout(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	log := &testLogger{}

	srv := NewTestServer(t, defaultProto, ctx)
	defer srv.Stop()

	// Tell the server to never respond to Startup frame
	atomic.StoreInt32(&srv.TimeoutOnStartup, 1)

	startTime := time.Now()
	cluster := NewCluster(srv.Address)
	cluster.Logger = log
	cluster.ProtoVersion = int(defaultProto)
	cluster.disableControlConn = true
	// Set very long query connection timeout
	// so we know CreateSession() is using the ConnectTimeout
	cluster.Timeout = time.Second * 5
	cluster.ConnectTimeout = 600 * time.Millisecond

	// Create session should timeout during connect attempt
	_, err := cluster.CreateSession()
	if err == nil {
		t.Fatal("CreateSession() should have returned a timeout error")
	}

	elapsed := time.Since(startTime)
	if elapsed > time.Second*5 {
		t.Fatal("ConnectTimeout is not respected")
	}

	if !errors.Is(err, ErrNoConnectionsStarted) {
		t.Fatalf("Expected to receive no connections error - got '%s'", err)
	}

	if !strings.Contains(log.String(), "no response to connection startup within timeout") && !strings.Contains(log.String(), "no response received from cassandra within timeout period") {
		t.Fatalf("Expected to receive timeout log message  - got '%s'", log.String())
	}

	cancel()
}

func TestTimeout(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	srv := NewTestServer(t, defaultProto, ctx)
	defer srv.Stop()

	db, err := newTestSession(defaultProto, srv.Address)
	if err != nil {
		t.Fatalf("NewCluster: %v", err)
	}
	defer db.Close()

	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()

		select {
		case <-time.After(5 * time.Second):
			t.Errorf("no timeout")
		case <-ctx.Done():
		}
	}()

	if err := db.Query("kill").WithContext(ctx).Exec(); err == nil {
		t.Fatal("expected error got nil")
	}
	cancel()

	wg.Wait()
}

func TestCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	srv := NewTestServer(t, defaultProto, ctx)
	defer srv.Stop()

	cluster := testCluster(defaultProto, srv.Address)
	cluster.Timeout = 1 * time.Second
	db, err := cluster.CreateSession()
	if err != nil {
		t.Fatalf("NewCluster: %v", err)
	}
	defer db.Close()

	qry := db.Query("timeout").WithContext(ctx)

	// Make sure we finish the query without leftovers
	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		err = qry.Exec()
		wg.Done()
	}()
	// The query will timeout after about 1 seconds, so cancel it after a short pause
	time.AfterFunc(20*time.Millisecond, cancel)
	wg.Wait()

	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected to get context cancel error: '%v', got '%v'", context.Canceled, err)
	}
}

type testQueryObserver struct {
	metrics map[string]*hostMetrics
	verbose bool
	logger  StdLogger
}

func (o *testQueryObserver) ObserveQuery(ctx context.Context, q ObservedQuery) {
	host := q.Host.ConnectAddress().String()
	o.metrics[host] = q.Metrics
	if o.verbose {
		o.logger.Printf("Observed query %q. Returned %v rows, took %v on host %q with %v attempts and total latency %v. Error: %q\n",
			q.Statement, q.Rows, q.End.Sub(q.Start), host, q.Metrics.Attempts, q.Metrics.TotalLatency, q.Err)
	}
}

func (o *testQueryObserver) GetMetrics(host *HostInfo) *hostMetrics {
	return o.metrics[host.ConnectAddress().String()]
}

// TestQueryRetry will test to make sure that gocql will execute
// the exact amount of retry queries designated by the user.
func TestQueryRetry(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	srv := NewTestServer(t, defaultProto, ctx)
	defer srv.Stop()

	db, err := newTestSession(defaultProto, srv.Address)
	if err != nil {
		t.Fatalf("NewCluster: %v", err)
	}
	defer db.Close()

	go func() {
		select {
		case <-ctx.Done():
			return
		case <-time.After(5 * time.Second):
			t.Errorf("no timeout")
		}
	}()

	rt := &SimpleRetryPolicy{NumRetries: 1}

	qry := db.Query("kill").RetryPolicy(rt)
	if err := qry.Exec(); err == nil {
		t.Fatalf("expected error")
	}

	requests := atomic.LoadInt64(&srv.nKillReq)
	attempts := qry.Attempts()
	if requests != int64(attempts) {
		t.Fatalf("expected requests %v to match query attempts %v", requests, attempts)
	}

	// the query will only be attempted once, but is being retried
	if requests != int64(rt.NumRetries) {
		t.Fatalf("failed to retry the query %v time(s). Query executed %v times", rt.NumRetries, requests-1)
	}
}

func TestQueryMultinodeWithMetrics(t *testing.T) {
	log := &testLogger{}
	defer func() {
		os.Stdout.WriteString(log.String())
	}()

	// Build a 3 node cluster to test host metric mapping
	var nodes []*TestServer
	var addresses = []string{
		"127.0.0.1",
		"127.0.0.2",
		"127.0.0.3",
	}
	// Can do with 1 context for all servers
	ctx := context.Background()
	for _, ip := range addresses {
		srv := NewTestServerWithAddress(ip+":0", t, defaultProto, ctx)
		defer srv.Stop()
		nodes = append(nodes, srv)
	}

	db, err := newTestSession(defaultProto, nodes[0].Address, nodes[1].Address, nodes[2].Address)
	if err != nil {
		t.Fatalf("NewCluster: %v", err)
	}
	defer db.Close()

	// 1 retry per host
	rt := &SimpleRetryPolicy{NumRetries: 3}
	observer := &testQueryObserver{metrics: make(map[string]*hostMetrics), verbose: false, logger: log}
	qry := db.Query("kill").RetryPolicy(rt).Observer(observer).Idempotent(true)
	if err := qry.Exec(); err == nil {
		t.Fatalf("expected error")
	}

	for i, ip := range addresses {
		var host *HostInfo
		for _, clusterHost := range db.GetHosts() {
			if clusterHost.connectAddress.String() == ip {
				host = clusterHost
			}
		}

		if host == nil {
			t.Fatalf("failed to observe host info for address %v", ip)
		}

		queryMetric := qry.metrics.hostMetrics(host)
		observedMetrics := observer.GetMetrics(host)

		requests := int(atomic.LoadInt64(&nodes[i].nKillReq))
		hostAttempts := queryMetric.Attempts
		if requests != hostAttempts {
			t.Fatalf("expected requests %v to match query attempts %v", requests, hostAttempts)
		}

		if hostAttempts != observedMetrics.Attempts {
			t.Fatalf("expected observed attempts %v to match query attempts %v on host %v", observedMetrics.Attempts, hostAttempts, ip)
		}

		hostLatency := queryMetric.TotalLatency
		observedLatency := observedMetrics.TotalLatency
		if hostLatency != observedLatency {
			t.Fatalf("expected observed latency %v to match query latency %v on host %v", observedLatency, hostLatency, ip)
		}
	}
	// the query will only be attempted once, but is being retried
	attempts := qry.Attempts()
	if attempts != rt.NumRetries {
		t.Fatalf("failed to retry the query %v time(s). Query executed %v times", rt.NumRetries, attempts)
	}

}

type testRetryPolicy struct {
	NumRetries int
}

func (t *testRetryPolicy) Attempt(qry RetryableQuery) bool {
	return qry.Attempts() <= t.NumRetries
}
func (t *testRetryPolicy) GetRetryType(err error) RetryType {
	var executedErr *QueryError
	if errors.As(err, &executedErr) && executedErr.PotentiallyExecuted() && !executedErr.IsIdempotent() {
		return Rethrow
	}
	return Retry
}

func TestSpeculativeExecution(t *testing.T) {
	log := &testLogger{}
	defer func() {
		os.Stdout.WriteString(log.String())
	}()

	// Build a 3 node cluster
	var nodes []*TestServer
	var addresses = []string{
		"127.0.0.1",
		"127.0.0.2",
		"127.0.0.3",
	}
	// Can do with 1 context for all servers
	ctx := context.Background()
	for _, ip := range addresses {
		srv := NewTestServerWithAddress(ip+":0", t, defaultProto, ctx)
		defer srv.Stop()
		nodes = append(nodes, srv)
	}

	db, err := newTestSession(defaultProto, nodes[0].Address, nodes[1].Address, nodes[2].Address)
	if err != nil {
		t.Fatalf("NewCluster: %v", err)
	}
	defer db.Close()

	// Create a test retry policy, 6 retries will cover 2 executions
	rt := &testRetryPolicy{NumRetries: 8}
	// test Speculative policy with 1 additional execution
	sp := &SimpleSpeculativeExecution{NumAttempts: 1, TimeoutDelay: 200 * time.Millisecond}

	// Build the query
	qry := db.Query("speculative").RetryPolicy(rt).SetSpeculativeExecutionPolicy(sp).Idempotent(true)

	// Execute the query and close, check that it doesn't error out
	if err := qry.Exec(); err != nil {
		t.Errorf("The query failed with '%v'!\n", err)
	}
	requests1 := atomic.LoadInt64(&nodes[0].nKillReq)
	requests2 := atomic.LoadInt64(&nodes[1].nKillReq)
	requests3 := atomic.LoadInt64(&nodes[2].nKillReq)

	// Spec Attempts == 1, so expecting to see only 1 regular + 1 speculative = 2 nodes attempted
	if requests1 != 0 && requests2 != 0 && requests3 != 0 {
		t.Error("error: all 3 nodes were attempted, should have been only 2")
	}

	// Only the 4th request will generate results, so
	if requests1 != 4 && requests2 != 4 && requests3 != 4 {
		t.Error("error: none of 3 nodes was attempted 4 times!")
	}

	// "speculative" query will succeed on one arbitrary node after 4 attempts, so
	// expecting to see 4 (on successful node) + not more than 2 (as cancelled on another node) == 6
	if requests1+requests2+requests3 > 6 {
		t.Errorf("error: expected to see 6 attempts, got %v\n", requests1+requests2+requests3)
	}
}

// This tests that the policy connection pool handles SSL correctly
func TestPolicyConnPoolSSL(t *testing.T) {
	srv := NewSSLTestServer(t, defaultProto, context.Background())
	defer srv.Stop()

	cluster := createTestSslCluster(srv.Address, defaultProto, true)
	cluster.PoolConfig.HostSelectionPolicy = RoundRobinHostPolicy()

	db, err := cluster.CreateSession()
	if err != nil {
		t.Fatalf("failed to create new session: %v", err)
	}

	if err := db.Query("void").Exec(); err != nil {
		t.Fatalf("query failed due to error: %v", err)
	}
	db.Close()

	// wait for the pool to drain
	time.Sleep(100 * time.Millisecond)
	size := db.pool.Size()
	if size != 0 {
		t.Fatalf("connection pool did not drain, still contains %d connections", size)
	}
}

func TestQueryTimeout(t *testing.T) {
	srv := NewTestServer(t, defaultProto, context.Background())
	defer srv.Stop()

	cluster := testCluster(defaultProto, srv.Address)
	// Set the timeout arbitrarily low so that the query hits the timeout in a
	// timely manner.
	cluster.Timeout = 1 * time.Millisecond

	db, err := cluster.CreateSession()
	if err != nil {
		t.Fatalf("NewCluster: %v", err)
	}
	defer db.Close()

	ch := make(chan error, 1)

	go func() {
		err := db.Query("timeout").Exec()
		if err != nil {
			ch <- err
			return
		}
		t.Errorf("err was nil, expected to get a timeout after %v", db.cfg.Timeout)
	}()

	select {
	case err := <-ch:
		if !errors.Is(err, ErrTimeoutNoResponse) {
			t.Fatalf("expected to get %v for timeout got %v", ErrTimeoutNoResponse, err)
		}
	case <-time.After(40*time.Millisecond + db.cfg.Timeout):
		// ensure that the query goroutines have been scheduled
		t.Fatalf("query did not timeout after %v", db.cfg.Timeout)
	}
}

func BenchmarkSingleConn(b *testing.B) {
	srv := NewTestServer(b, 3, context.Background())
	defer srv.Stop()

	cluster := testCluster(protoVersion3, srv.Address)
	// Set the timeout arbitrarily low so that the query hits the timeout in a
	// timely manner.
	cluster.Timeout = 500 * time.Millisecond
	cluster.NumConns = 1
	db, err := cluster.CreateSession()
	if err != nil {
		b.Fatalf("NewCluster: %v", err)
	}
	defer db.Close()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			err := db.Query("void").Exec()
			if err != nil {
				b.Error(err)
				return
			}
		}
	})
}

func TestQueryTimeoutReuseStream(t *testing.T) {
	t.Skip("no longer tests anything")
	// TODO(zariel): move this to conn test, we really just want to check what
	// happens when a conn is

	srv := NewTestServer(t, defaultProto, context.Background())
	defer srv.Stop()

	cluster := testCluster(defaultProto, srv.Address)
	// Set the timeout arbitrarily low so that the query hits the timeout in a
	// timely manner.
	cluster.Timeout = 1 * time.Millisecond
	cluster.NumConns = 1

	db, err := cluster.CreateSession()
	if err != nil {
		t.Fatalf("NewCluster: %v", err)
	}
	defer db.Close()

	db.Query("slow").Exec()

	err = db.Query("void").Exec()
	if err != nil {
		t.Fatal(err)
	}
}

func TestQueryTimeoutClose(t *testing.T) {
	srv := NewTestServer(t, defaultProto, context.Background())
	defer srv.Stop()

	cluster := testCluster(defaultProto, srv.Address)
	// Set the timeout arbitrarily low so that the query hits the timeout in a
	// timely manner.
	cluster.Timeout = 1000 * time.Millisecond
	cluster.NumConns = 1

	db, err := cluster.CreateSession()
	if err != nil {
		t.Fatalf("NewCluster: %v", err)
	}

	ch := make(chan error)
	go func() {
		err := db.Query("timeout").Exec()
		ch <- err
	}()
	// ensure that the above goroutine gets sheduled
	time.Sleep(50 * time.Millisecond)

	db.Close()
	select {
	case err = <-ch:
	case <-time.After(1 * time.Second):
		t.Fatal("timedout waiting to get a response once cluster is closed")
	}

	if !errors.Is(err, ErrConnectionClosed) {
		t.Fatalf("expected to get %v or an error wrapping it, got %v", ErrConnectionClosed, err)
	}
}

func TestStream0(t *testing.T) {
	// TODO: replace this with type check
	const expErr = "gocql: received unexpected frame on stream 0"

	var buf bytes.Buffer
	f := newFramer(nil, protoVersion4)
	f.writeHeader(0, frm.OpResult, 0)
	f.writeInt(frm.ResultKindVoid)
	f.buf[0] |= 0x80
	if err := f.finish(); err != nil {
		t.Fatal(err)
	}
	if err := f.writeTo(&buf); err != nil {
		t.Fatal(err)
	}

	conn := &Conn{
		r:       bufio.NewReader(&buf),
		streams: streams.New(),
		logger:  &defaultLogger{},
		cfg:     &ConnConfig{},
	}

	err := conn.recv(context.Background())
	if err == nil {
		t.Fatal("expected to get an error on stream 0")
	} else if !strings.HasPrefix(err.Error(), expErr) {
		t.Fatalf("expected to get error prefix %q got %q", expErr, err.Error())
	}
}

func TestContext_Timeout(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	srv := NewTestServer(t, defaultProto, ctx)
	defer srv.Stop()

	cluster := testCluster(defaultProto, srv.Address)
	cluster.Timeout = 5 * time.Second
	db, err := cluster.CreateSession()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	ctx, cancel = context.WithCancel(ctx)
	cancel()

	err = db.Query("timeout").WithContext(ctx).Exec()
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected to get context cancel error: %v got %v", context.Canceled, err)
	}
}

type TestReconnectionPolicy struct {
	NumRetries       int
	GetIntervalCalls []int
}

func (c *TestReconnectionPolicy) GetInterval(currentRetry int) time.Duration {
	c.GetIntervalCalls = append(c.GetIntervalCalls, currentRetry)
	return time.Duration(0)
}

func (c *TestReconnectionPolicy) GetMaxRetries() int {
	return c.NumRetries
}

func TestInitialRetryPolicy(t *testing.T) {
	t.Parallel()

	tcase := []struct {
		NumRetries               int
		ProtoVersion             int
		ExpectedGetIntervalCalls []int
		ExpectedErr              string
	}{
		{
			NumRetries:               1,
			ProtoVersion:             0,
			ExpectedGetIntervalCalls: nil,
			ExpectedErr:              "gocql: unable to create session: unable to connect to the cluster, last error: unable to discover protocol version:"},
		{
			NumRetries:               2,
			ProtoVersion:             0,
			ExpectedGetIntervalCalls: []int{1},
			ExpectedErr:              "gocql: unable to create session: unable to connect to the cluster, last error: unable to discover protocol version:"},
		{
			NumRetries:               3,
			ProtoVersion:             0,
			ExpectedGetIntervalCalls: []int{1, 2},
			ExpectedErr:              "gocql: unable to create session: unable to connect to the cluster, last error: unable to discover protocol version:"},
		{
			NumRetries:               1,
			ProtoVersion:             protoVersion4,
			ExpectedGetIntervalCalls: nil,
			ExpectedErr:              "gocql: unable to create session: unable to connect to the cluster, last error: unable to create control connection: unable to connect to initial hosts:"},
		{
			NumRetries:               2,
			ProtoVersion:             protoVersion4,
			ExpectedGetIntervalCalls: []int{1},
			ExpectedErr:              "gocql: unable to create session: unable to connect to the cluster, last error: unable to create control connection: unable to connect to initial hosts:"},
		{
			NumRetries:               3,
			ProtoVersion:             protoVersion4,
			ExpectedGetIntervalCalls: []int{1, 2},
			ExpectedErr:              "gocql: unable to create session: unable to connect to the cluster, last error: unable to create control connection: unable to connect to initial hosts:"},
	}

	for id := range tcase {
		tc := tcase[id]
		t.Run(fmt.Sprintf("NumRetries=%d_ProtocolVersion=%d", tc.NumRetries, tc.ProtoVersion), func(t *testing.T) {
			t.Parallel()

			// Use a loopback address with a well-known closed port so the test
			// remains deterministic even when a local Cassandra-compatible
			// service is listening on 9042.
			cluster := NewCluster("127.0.0.1:1")
			policy := &TestReconnectionPolicy{NumRetries: tc.NumRetries}
			cluster.InitialReconnectionPolicy = policy
			cluster.ProtoVersion = tc.ProtoVersion
			_, err := cluster.CreateSession()
			if err == nil {
				t.Fatal("expected to get an error")
			}
			if !strings.Contains(err.Error(), tc.ExpectedErr) {
				t.Errorf("expected error to contain %q got %q", tc.ExpectedErr, err.Error())
			}
			if !cmp.Equal(tc.ExpectedGetIntervalCalls, policy.GetIntervalCalls) {
				t.Errorf("expected GetInterval calls to be (%+v) but was (%+v) instead", tc.ExpectedGetIntervalCalls, policy.GetIntervalCalls)
			}
		})
	}
}

func TestContext_CanceledBeforeExec(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var reqCount uint64

	srv := newTestServerOpts{
		addr:     "127.0.0.1:0",
		protocol: defaultProto,
		recvHook: func(f *framer) {
			if f.header.Op == frm.OpStartup || f.header.Op == frm.OpOptions {
				// ignore statup and heartbeat messages
				return
			}
			atomic.AddUint64(&reqCount, 1)
		},
	}.newServer(t, ctx)

	defer srv.Stop()

	cluster := testCluster(defaultProto, srv.Address)
	cluster.Timeout = 5 * time.Second
	db, err := cluster.CreateSession()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	startupRequestCount := atomic.LoadUint64(&reqCount)

	ctx, cancel = context.WithCancel(ctx)
	cancel()

	err = db.Query("timeout").WithContext(ctx).Exec()
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected to get context cancel error: %v got %v", context.Canceled, err)
	}

	// Queries are executed by separate goroutine and we don't have a synchronization point that would allow us to
	// check if a request was sent or not.
	// Fall back to waiting a little bit.
	time.Sleep(100 * time.Millisecond)

	queryRequestCount := atomic.LoadUint64(&reqCount) - startupRequestCount
	if queryRequestCount != 0 {
		t.Fatalf("expected that no request is sent to server, sent %d requests", queryRequestCount)
	}
}

func TestCallReqReuseDoesNotInvalidateOutstandingTimeout(t *testing.T) {
	t.Parallel()

	oldCall := getCallReq(1)
	oldTimeout := oldCall.timeout
	oldCall.done.Done()
	putCallReq(oldCall)

	newCall := getCallReq(2)
	defer newCall.done.Done()
	defer close(newCall.timeout)

	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("closing old timeout should not panic after putCallReq: %v", r)
		}
	}()

	close(oldTimeout)

	select {
	case <-newCall.timeout:
		t.Fatal("closing the old timeout unexpectedly closed the new call timeout")
	default:
	}
}

type testContextWriter struct {
	n       int
	err     error
	onWrite func()
}

func (w testContextWriter) writeContext(ctx context.Context, p []byte) (int, error) {
	if w.onWrite != nil {
		w.onWrite()
	}
	if w.n == 0 && w.err == nil {
		return len(p), nil
	}
	return w.n, w.err
}

func (w testContextWriter) setWriteTimeout(timeout time.Duration) {}

type contextWriterFunc func(context.Context, []byte) (int, error)

func (fn contextWriterFunc) writeContext(ctx context.Context, p []byte) (int, error) {
	return fn(ctx, p)
}

func (fn contextWriterFunc) setWriteTimeout(timeout time.Duration) {}

func newTestExecConn(t *testing.T, w contextWriter) (*Conn, net.Conn) {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	server, client := net.Pipe()
	c := newTestConnWithFramerPool()
	c.ctx = ctx
	c.cancel = cancel
	c.conn = client
	c.w = w
	c.logger = nopLogger{}
	c.errorHandler = connErrorHandlerFn(func(*Conn, error, bool) {})
	c.streams = streams.New()
	c.calls = make(map[int]*callReq)

	return c, server
}

func waitForSingleCall(t *testing.T, c *Conn) *callReq {
	t.Helper()

	deadline := time.After(2 * time.Second)
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()

	for {
		c.mu.Lock()
		for _, call := range c.calls {
			c.mu.Unlock()
			return call
		}
		c.mu.Unlock()

		select {
		case <-deadline:
			t.Fatal("timed out waiting for in-flight call")
		case <-ticker.C:
		}
	}
}

func detachSingleCall(t *testing.T, c *Conn) *callReq {
	t.Helper()

	c.mu.Lock()
	defer c.mu.Unlock()
	for streamID, call := range c.calls {
		delete(c.calls, streamID)
		return call
	}

	t.Fatal("expected an in-flight call")
	return nil
}

type testStreamObserver struct {
	ctx *testStreamObserverContext
}

func (o *testStreamObserver) StreamContext(context.Context) StreamObserverContext {
	return o.ctx
}

type testStreamObserverContext struct {
	started   chan struct{}
	abandoned chan struct{}
	finished  chan struct{}
}

func newTestStreamObserverContext() *testStreamObserverContext {
	return &testStreamObserverContext{
		started:   make(chan struct{}, 1),
		abandoned: make(chan struct{}, 1),
		finished:  make(chan struct{}, 1),
	}
}

func (o *testStreamObserverContext) StreamStarted(ObservedStream) {
	select {
	case o.started <- struct{}{}:
	default:
	}
}

func (o *testStreamObserverContext) StreamAbandoned(ObservedStream) {
	select {
	case o.abandoned <- struct{}{}:
	default:
	}
}

func (o *testStreamObserverContext) StreamFinished(ObservedStream) {
	select {
	case o.finished <- struct{}{}:
	default:
	}
}

func TestExecCloseWithError(t *testing.T) {
	t.Parallel()

	t.Run("BuildFrameErrorReleasesResources", func(t *testing.T) {
		c, server := newTestExecConn(t, testContextWriter{})
		defer server.Close()

		_, err := c.exec(context.Background(), frameWriterFunc(func(f *framer, streamID int) error {
			return io.ErrUnexpectedEOF
		}), nil, 0)
		if !errors.Is(err, io.ErrUnexpectedEOF) {
			t.Fatalf("expected build error %v, got %v", io.ErrUnexpectedEOF, err)
		}

		c.mu.Lock()
		defer c.mu.Unlock()
		if len(c.calls) != 0 {
			t.Fatalf("expected no in-flight calls after build error, got %d", len(c.calls))
		}
	})

	t.Run("ContextCanceledBeforeWriteReleasesResources", func(t *testing.T) {
		writeEntered := make(chan struct{})
		c, server := newTestExecConn(t, contextWriterFunc(func(ctx context.Context, p []byte) (int, error) {
			close(writeEntered)
			<-ctx.Done()
			return 0, ctx.Err()
		}))
		defer server.Close()

		ctx, cancel := context.WithCancel(context.Background())
		errCh := make(chan error, 1)
		go func() {
			_, err := c.exec(ctx, frameWriterFunc(func(f *framer, streamID int) error {
				f.buf = append(f.buf[:0], 'x')
				return nil
			}), nil, 0)
			errCh <- err
		}()

		select {
		case <-writeEntered:
		case <-time.After(2 * time.Second):
			t.Fatal("exec never reached the write path")
		}
		cancel()

		select {
		case err := <-errCh:
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("expected context cancel error %v, got %v", context.Canceled, err)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("exec deadlocked after context cancellation before write")
		}

		c.mu.Lock()
		defer c.mu.Unlock()
		if len(c.calls) != 0 {
			t.Fatalf("expected no in-flight calls after canceled write, got %d", len(c.calls))
		}
	})

	t.Run("ResponseErrorReleasesResources", func(t *testing.T) {
		c, server := newTestExecConn(t, testContextWriter{})
		defer server.Close()

		errCh := make(chan error, 1)
		go func() {
			_, err := c.exec(context.Background(), frameWriterFunc(func(f *framer, streamID int) error {
				f.buf = append(f.buf[:0], 'x')
				return nil
			}), nil, 0)
			errCh <- err
		}()

		waitForSingleCall(t, c)
		call := detachSingleCall(t, c)
		call.resp <- callResp{err: io.EOF}

		select {
		case err := <-errCh:
			if !errors.Is(err, io.EOF) {
				t.Fatalf("expected response error %v, got %v", io.EOF, err)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("exec deadlocked after response error")
		}

		c.mu.Lock()
		defer c.mu.Unlock()
		if len(c.calls) != 0 {
			t.Fatalf("expected no in-flight calls after response error, got %d", len(c.calls))
		}
	})

	t.Run("PartialWriteDoesNotDeadlock", func(t *testing.T) {
		c, server := newTestExecConn(t, testContextWriter{
			n:   1,
			err: io.ErrUnexpectedEOF,
		})
		defer server.Close()

		errCh := make(chan error, 1)
		go func() {
			_, err := c.exec(context.Background(), frameWriterFunc(func(f *framer, streamID int) error {
				f.buf = append(f.buf[:0], 'x')
				return nil
			}), nil, 0)
			errCh <- err
		}()

		select {
		case err := <-errCh:
			if !errors.Is(err, io.ErrUnexpectedEOF) {
				t.Fatalf("expected write error %v, got %v", io.ErrUnexpectedEOF, err)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("exec deadlocked after partial write failure")
		}
	})

	t.Run("ConnectionCloseErrorDoesNotDeadlock", func(t *testing.T) {
		writeStarted := make(chan struct{})
		var writeStartedOnce sync.Once
		c, server := newTestExecConn(t, testContextWriter{
			onWrite: func() {
				writeStartedOnce.Do(func() {
					close(writeStarted)
				})
			},
		})
		defer server.Close()

		closeDone := make(chan struct{})
		go func() {
			<-writeStarted
			c.closeWithError(io.EOF)
			close(closeDone)
		}()

		errCh := make(chan error, 1)
		go func() {
			_, err := c.exec(context.Background(), frameWriterFunc(func(f *framer, streamID int) error {
				f.buf = append(f.buf[:0], 'x')
				return nil
			}), nil, 0)
			errCh <- err
		}()

		select {
		case err := <-errCh:
			if !errors.Is(err, io.EOF) {
				t.Fatalf("expected close error %v, got %v", io.EOF, err)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("exec deadlocked after closeWithError")
		}

		select {
		case <-closeDone:
		case <-time.After(2 * time.Second):
			t.Fatal("closeWithError deadlocked waiting for exec to release the call")
		}
	})

	t.Run("TimeoutUnblocksAbandonRecvCall", func(t *testing.T) {
		c, server := newTestExecConn(t, testContextWriter{})
		defer server.Close()

		errCh := make(chan error, 1)
		go func() {
			_, err := c.exec(context.Background(), frameWriterFunc(func(f *framer, streamID int) error {
				f.buf = append(f.buf[:0], 'x')
				return nil
			}), nil, time.Millisecond)
			errCh <- err
		}()

		call := waitForSingleCall(t, c)

		select {
		case err := <-errCh:
			if !errors.Is(err, ErrTimeoutNoResponse) {
				t.Fatalf("expected timeout error %v, got %v", ErrTimeoutNoResponse, err)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("exec deadlocked waiting for timeout")
		}

		if !c.removeCallIfOpen(call.streamID) {
			t.Fatal("expected timed out call to still be registered")
		}

		done := make(chan struct{})
		go func() {
			c.abandonRecvCall(call, c.getReadFramer())
			close(done)
		}()

		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Fatal("abandonRecvCall deadlocked after timeout")
		}
	})

	t.Run("ContextCancelUnblocksAbandonRecvCall", func(t *testing.T) {
		c, server := newTestExecConn(t, testContextWriter{})
		defer server.Close()

		ctx, cancel := context.WithCancel(context.Background())
		errCh := make(chan error, 1)
		go func() {
			_, err := c.exec(ctx, frameWriterFunc(func(f *framer, streamID int) error {
				f.buf = append(f.buf[:0], 'x')
				return nil
			}), nil, 0)
			errCh <- err
		}()

		call := waitForSingleCall(t, c)
		cancel()

		select {
		case err := <-errCh:
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("expected context cancel error %v, got %v", context.Canceled, err)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("exec deadlocked waiting for context cancellation")
		}

		if !c.removeCallIfOpen(call.streamID) {
			t.Fatal("expected canceled call to still be registered")
		}

		done := make(chan struct{})
		go func() {
			c.abandonRecvCall(call, c.getReadFramer())
			close(done)
		}()

		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Fatal("abandonRecvCall deadlocked after context cancellation")
		}
	})

	t.Run("ConnectionCloseAbandonsInflightStream", func(t *testing.T) {
		writeStarted := make(chan struct{})
		var writeStartedOnce sync.Once
		observerCtx := newTestStreamObserverContext()
		c, server := newTestExecConn(t, testContextWriter{
			onWrite: func() {
				writeStartedOnce.Do(func() {
					close(writeStarted)
				})
			},
		})
		c.streamObserver = &testStreamObserver{ctx: observerCtx}
		defer server.Close()

		errCh := make(chan error, 1)
		go func() {
			_, err := c.exec(context.Background(), frameWriterFunc(func(f *framer, streamID int) error {
				f.buf = append(f.buf[:0], 'x')
				return nil
			}), nil, 0)
			errCh <- err
		}()

		select {
		case <-observerCtx.started:
		case <-time.After(2 * time.Second):
			t.Fatal("stream observer did not observe the request start")
		}

		select {
		case <-writeStarted:
		case <-time.After(2 * time.Second):
			t.Fatal("exec never reached the write path")
		}

		closeDone := make(chan struct{})
		go func() {
			c.Close()
			close(closeDone)
		}()

		select {
		case err := <-errCh:
			if !errors.Is(err, ErrConnectionClosed) {
				t.Fatalf("expected close error %v, got %v", ErrConnectionClosed, err)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("exec deadlocked after Close")
		}

		select {
		case <-observerCtx.abandoned:
		case <-time.After(2 * time.Second):
			t.Fatal("Close did not abandon the in-flight stream")
		}

		select {
		case <-observerCtx.finished:
			t.Fatal("Close should not mark the in-flight stream as finished")
		default:
		}

		select {
		case <-closeDone:
		case <-time.After(2 * time.Second):
			t.Fatal("Close did not wait for the in-flight exec cleanup")
		}

		c.mu.Lock()
		defer c.mu.Unlock()
		if c.calls != nil {
			t.Fatal("expected in-flight calls to be detached on Close")
		}
	})
}

// tcpConnPair returns a matching set of a TCP client side and server side connection.
func tcpConnPair() (s, c net.Conn, err error) {
	l, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		// maybe ipv6 works, if ipv4 fails?
		l, err = net.Listen("tcp6", "[::1]:0")
		if err != nil {
			return nil, nil, err
		}
	}
	defer l.Close() // we only try to accept one connection, so will stop listening.

	addr := l.Addr()
	done := make(chan struct{})
	var errDial error
	go func(done chan<- struct{}) {
		c, errDial = net.Dial(addr.Network(), addr.String())
		close(done)
	}(done)

	s, err = l.Accept()
	<-done

	if err == nil {
		err = errDial
	}

	if err != nil {
		if s != nil {
			s.Close()
		}
		if c != nil {
			c.Close()
		}
	}

	return s, c, err
}

func TestWriteCoalescing(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	server, client, err := tcpConnPair()
	if err != nil {
		t.Fatal(err)
	}

	done := make(chan struct{}, 1)
	var (
		buf      bytes.Buffer
		bufMutex sync.Mutex
	)
	go func() {
		defer close(done)
		defer server.Close()
		var err error
		b := make([]byte, 256)
		var n int
		for {
			if n, err = server.Read(b); err != nil {
				break
			}
			bufMutex.Lock()
			buf.Write(b[:n])
			bufMutex.Unlock()
		}
		if err != io.EOF {
			t.Errorf("unexpected read error: %v", err)
		}
	}()
	enqueued := make(chan struct{})
	resetTimer := make(chan struct{})
	w := &writeCoalescer{
		writeCh: make(chan writeRequest),
		c:       client,
		quit:    ctx.Done(),
		testEnqueuedHook: func() {
			enqueued <- struct{}{}
		},
		testFlushedHook: func() {
			client.Close()
		},
	}
	w.setWriteTimeout(500 * time.Millisecond)
	timerC := make(chan time.Time, 1)
	go func() {
		w.writeFlusherImpl(timerC, func() { resetTimer <- struct{}{} })
	}()

	go func() {
		if _, err := w.writeContext(context.Background(), []byte("one")); err != nil {
			t.Error(err)
		}
	}()

	go func() {
		if _, err := w.writeContext(context.Background(), []byte("two")); err != nil {
			t.Error(err)
		}
	}()

	<-enqueued
	<-resetTimer
	<-enqueued

	// flush
	timerC <- time.Now()

	<-done

	if got := buf.String(); got != "onetwo" && got != "twoone" {
		t.Fatalf("expected to get %q got %q", "onetwo or twoone", got)
	}
}

func TestWriteCoalescing_WriteAfterClose(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var buf bytes.Buffer
	defer cancel()
	server, client, err := tcpConnPair()
	if err != nil {
		t.Fatal(err)
	}

	done := make(chan struct{}, 1)
	go func() {
		io.Copy(&buf, server)
		server.Close()
		close(done)
	}()
	w := newWriteCoalescer(client, 0, 5*time.Millisecond, ctx.Done())

	// ensure 1 write works
	if _, err := w.writeContext(context.Background(), []byte("one")); err != nil {
		t.Fatal(err)
	}

	client.Close()
	<-done
	if v := buf.String(); v != "one" {
		t.Fatalf("expected buffer to be %q got %q", "one", v)
	}

	// now close and do a write, we should error
	cancel()
	client.Close() // close client conn too, since server won't see the answer anyway.

	if _, err := w.writeContext(context.Background(), []byte("two")); err == nil {
		t.Fatal("expected to get error for write after closing")
	} else if err != io.EOF {
		t.Fatalf("expected to get EOF got %v", err)
	}
}

func TestSkipMetadata(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	srv := NewTestServer(t, protoVersion4, ctx)
	defer srv.Stop()

	cfg := testCluster(protoVersion4, srv.Address)
	cfg.DisableSkipMetadata = false

	db, err := cfg.CreateSession()
	if err != nil {
		t.Fatalf("NewCluster: %v", err)
	}
	defer db.Close()

	if err := db.Query("select nometadata").Exec(); err != nil {
		t.Fatalf("expected no error got: %v", err)
	}

	if err := db.Query("select metadata").Exec(); err != nil {
		t.Fatalf("expected no error got: %v", err)
	}
}

func TestPrepareBatchMetadataMultipleKeyspaceTables(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	srv := NewTestServer(t, protoVersion4, ctx)
	defer srv.Stop()

	cfg := testCluster(protoVersion4, srv.Address)
	db, err := cfg.CreateSession()
	if err != nil {
		t.Fatalf("CreateSession: %v", err)
	}
	defer db.Close()

	conn := db.getConn()
	if conn == nil {
		t.Fatal("expected connection, got nil")
	}

	stmt := "BEGIN BATCH INSERT INTO ks1.tbl1 (col1) VALUES (?) INSERT INTO ks2.tbl2 (col2) VALUES (?) APPLY BATCH"
	info, err := conn.prepareStatement(ctx, stmt, nil, time.Second)
	if err != nil {
		t.Fatalf("prepareStatement failed: %v", err)
	}

	if got := len(info.request.columns); got != 2 {
		t.Fatalf("expected 2 request columns, got %d", got)
	}

	col0 := info.request.columns[0]
	if col0.Keyspace != "ks1" || col0.Table != "tbl1" || col0.Name != "col1" {
		t.Fatalf("unexpected column 0: %+v", col0)
	}

	col1 := info.request.columns[1]
	if col1.Keyspace != "ks2" || col1.Table != "tbl2" || col1.Name != "col2" {
		t.Fatalf("unexpected column 1: %+v", col1)
	}

	if info.request.keyspace != "" || info.request.table != "" {
		t.Fatalf("expected empty prepared keyspace/table for mixed batch, got %q/%q", info.request.keyspace, info.request.table)
	}
}

type recordingFrameHeaderObserver struct {
	t      *testing.T
	mu     sync.Mutex
	frames []ObservedFrameHeader
}

func (r *recordingFrameHeaderObserver) ObserveFrameHeader(ctx context.Context, frm ObservedFrameHeader) {
	r.mu.Lock()
	r.frames = append(r.frames, frm)
	r.mu.Unlock()
}

func (r *recordingFrameHeaderObserver) getFrames() []ObservedFrameHeader {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.frames
}

func TestFrameHeaderObserver(t *testing.T) {
	srv := NewTestServer(t, defaultProto, context.Background())
	defer srv.Stop()

	cluster := testCluster(defaultProto, srv.Address)
	cluster.NumConns = 1
	observer := &recordingFrameHeaderObserver{t: t}
	cluster.FrameHeaderObserver = observer

	db, err := cluster.CreateSession()
	if err != nil {
		t.Fatal(err)
	}

	if err := db.Query("void").Exec(); err != nil {
		t.Fatal(err)
	}

	frames := observer.getFrames()
	expFrames := []frm.Op{frm.OpSupported, frm.OpReady, frm.OpResult}
	if len(frames) != len(expFrames) {
		t.Fatalf("Expected to receive %d frames, instead received %d", len(expFrames), len(frames))
	}

	for i, op := range expFrames {
		if op != frames[i].Opcode {
			t.Fatalf("expected frame %d to be %v got %v", i, op, frames[i])
		}
	}
	voidResultFrame := frames[2]
	if voidResultFrame.Length != int32(4) {
		t.Fatalf("Expected to receive frame with body length 4, instead received body length %d", voidResultFrame.Length)
	}
}

func NewTestServerWithAddress(addr string, t testing.TB, protocol uint8, ctx context.Context) *TestServer {
	return newTestServerOpts{
		addr:     addr,
		protocol: protocol,
	}.newServer(t, ctx)
}

func NewTestServerWithAddressAndSupportedFactory(addr string, t testing.TB, protocol uint8, ctx context.Context, supportedFactory testSupportedFactory) *TestServer {
	return newTestServerOpts{
		addr:             addr,
		protocol:         protocol,
		supportedFactory: supportedFactory,
	}.newServer(t, ctx)
}

type newTestServerOpts struct {
	addr             string
	protocol         uint8
	supportedFactory testSupportedFactory
	recvHook         func(*framer)
}

func (nts newTestServerOpts) newServer(t testing.TB, ctx context.Context) *TestServer {
	laddr, err := net.ResolveTCPAddr("tcp", nts.addr)
	if err != nil {
		t.Fatal(err)
	}

	listen, err := net.ListenTCP("tcp", laddr)
	if err != nil {
		t.Fatal(err)
	}

	headerSize := 9

	ctx, cancel := context.WithCancel(ctx)
	srv := &TestServer{
		Address:    listen.Addr().String(),
		listen:     listen,
		t:          t,
		protocol:   nts.protocol,
		headerSize: headerSize,
		ctx:        ctx,
		cancel:     cancel,

		supportedFactory: nts.supportedFactory,
		onRecv:           nts.recvHook,
	}

	go srv.closeWatch()
	go srv.serve()

	return srv
}

func NewTestServer(t testing.TB, protocol uint8, ctx context.Context) *TestServer {
	return NewTestServerWithAddress("127.0.0.1:0", t, protocol, ctx)
}

func NewSSLTestServer(t testing.TB, protocol uint8, ctx context.Context) *TestServer {
	return NewSSLTestServerWithSupportedFactory(t, protocol, ctx, nil)
}

func NewSSLTestServerWithSupportedFactory(t testing.TB, protocol uint8, ctx context.Context, supportedFactory testSupportedFactory) *TestServer {
	pem, err := os.ReadFile("testdata/pki/ca.crt")
	certPool := x509.NewCertPool()
	if !certPool.AppendCertsFromPEM(pem) {
		t.Fatalf("Failed parsing or appending certs")
	}
	mycert, err := tls.LoadX509KeyPair("testdata/pki/cassandra.crt", "testdata/pki/cassandra.key")
	if err != nil {
		t.Fatalf("could not load cert")
	}
	config := &tls.Config{
		Certificates: []tls.Certificate{mycert},
		RootCAs:      certPool,
	}
	listen, err := tls.Listen("tcp", "127.0.0.1:0", config)
	if err != nil {
		t.Fatal(err)
	}

	headerSize := 9

	ctx, cancel := context.WithCancel(ctx)
	srv := &TestServer{
		Address:    listen.Addr().String(),
		listen:     listen,
		t:          t,
		protocol:   protocol,
		headerSize: headerSize,
		ctx:        ctx,
		cancel:     cancel,

		supportedFactory: supportedFactory,
	}

	go srv.closeWatch()
	go srv.serve()
	return srv
}

type TestServer struct {
	Address          string
	TimeoutOnStartup int32
	t                testing.TB
	listen           net.Listener
	nKillReq         int64
	supportedFactory testSupportedFactory

	protocol   byte
	headerSize int
	ctx        context.Context
	cancel     context.CancelFunc

	mu     sync.Mutex
	closed bool

	// onRecv is a hook point for tests, called in receive loop.
	onRecv func(*framer)
}

type testSupportedFactory func(conn net.Conn) map[string][]string

func (srv *TestServer) session() (*Session, error) {
	return testCluster(frm.ProtoVersion(srv.protocol), srv.Address).CreateSession()
}

func (srv *TestServer) host() *HostInfo {
	hosts, err := resolveInitialEndpoint(nil, srv.Address, 9042)
	if err != nil {
		srv.t.Fatal(err)
	}
	return hosts[0]
}

func (srv *TestServer) closeWatch() {
	<-srv.ctx.Done()

	srv.mu.Lock()
	defer srv.mu.Unlock()

	srv.closeLocked()
}

func (srv *TestServer) serve() {
	defer srv.listen.Close()
	for !srv.isClosed() {
		conn, err := srv.listen.Accept()
		if err != nil {
			break
		}

		var exts map[string][]string
		if srv.supportedFactory != nil {
			exts = (srv.supportedFactory)(conn)
		}

		go func(conn net.Conn, exts map[string][]string) {
			defer conn.Close()
			for !srv.isClosed() {
				framer, err := srv.readFrame(conn)
				if err != nil {
					if err == io.EOF || errors.Is(err, net.ErrClosed) {
						return
					}
					srv.errorLocked(err)
					return
				}

				if srv.onRecv != nil {
					srv.onRecv(framer)
				}

				go srv.process(conn, framer, exts)
			}
		}(conn, exts)
	}
}

func (srv *TestServer) isClosed() bool {
	srv.mu.Lock()
	defer srv.mu.Unlock()
	return srv.closed
}

func (srv *TestServer) closeLocked() {
	if srv.closed {
		return
	}

	srv.closed = true

	srv.listen.Close()
	srv.cancel()
}

func (srv *TestServer) Stop() {
	srv.mu.Lock()
	defer srv.mu.Unlock()
	srv.closeLocked()
}

func (srv *TestServer) errorLocked(err any) {
	srv.mu.Lock()
	defer srv.mu.Unlock()
	if srv.closed {
		return
	}
	srv.t.Error(err)
}

func (srv *TestServer) process(conn net.Conn, reqFrame *framer, exts map[string][]string) {
	head := reqFrame.header
	if head == nil {
		srv.errorLocked("process frame with a nil header")
		return
	}
	respFrame := newFramer(nil, reqFrame.proto)

	switch head.Op {
	case frm.OpStartup:
		if atomic.LoadInt32(&srv.TimeoutOnStartup) > 0 {
			// Do not respond to startup command
			// wait until we get a cancel signal
			select {
			case <-srv.ctx.Done():
				return
			}
		}
		respFrame.writeHeader(0, frm.OpReady, head.Stream)
	case frm.OpOptions:
		respFrame.writeHeader(0, frm.OpSupported, head.Stream)
		respFrame.writeStringMultiMap(exts)
	case frm.OpQuery:
		query := reqFrame.readLongString()
		first := query
		if n := strings.Index(query, " "); n > 0 {
			first = first[:n]
		}
		switch strings.ToLower(first) {
		case "kill":
			atomic.AddInt64(&srv.nKillReq, 1)
			respFrame.writeHeader(0, frm.OpError, head.Stream)
			respFrame.writeInt(0x1001)
			respFrame.writeString("query killed")
		case "use":
			respFrame.writeInt(frm.ResultKindKeyspace)
			respFrame.writeString(strings.TrimSpace(query[3:]))
		case "void":
			respFrame.writeHeader(0, frm.OpResult, head.Stream)
			respFrame.writeInt(frm.ResultKindVoid)
		case "timeout":
			<-srv.ctx.Done()
			return
		case "slow":
			go func() {
				respFrame.writeHeader(0, frm.OpResult, head.Stream)
				respFrame.writeInt(frm.ResultKindVoid)
				respFrame.buf[0] = srv.protocol | 0x80
				select {
				case <-srv.ctx.Done():
					return
				case <-time.After(50 * time.Millisecond):
					respFrame.finish()
					respFrame.writeTo(conn)
				}
			}()
			return
		case "speculative":
			atomic.AddInt64(&srv.nKillReq, 1)
			if atomic.LoadInt64(&srv.nKillReq) > 3 {
				respFrame.writeHeader(0, frm.OpResult, head.Stream)
				respFrame.writeInt(frm.ResultKindVoid)
				respFrame.writeString("speculative query success on the node " + srv.Address)
			} else {
				respFrame.writeHeader(0, frm.OpError, head.Stream)
				respFrame.writeInt(0x1001)
				respFrame.writeString("speculative error")
				rand.Seed(time.Now().UnixNano())
				<-time.After(time.Millisecond * 120)
			}
		default:
			respFrame.writeHeader(0, frm.OpResult, head.Stream)
			respFrame.writeInt(frm.ResultKindVoid)
		}
	case frm.OpError:
		respFrame.writeHeader(0, frm.OpError, head.Stream)
		respFrame.buf = append(respFrame.buf, reqFrame.buf...)
	case frm.OpPrepare:
		query := strings.TrimSpace(reqFrame.readLongString())
		lower := strings.ToLower(query)
		name := ""
		if strings.HasPrefix(lower, "select ") {
			name = strings.TrimPrefix(lower, "select ")
			if n := strings.Index(name, " "); n > 0 {
				name = name[:n]
			}
		} else if strings.HasPrefix(lower, "begin batch") {
			name = "batchmetadata"
		} else {
			name = lower
		}
		switch name {
		case "nometadata":
			respFrame.writeHeader(0, frm.OpResult, head.Stream)
			respFrame.writeInt(frm.ResultKindPrepared)
			// <id>
			respFrame.writeShortBytes(binary.BigEndian.AppendUint64(nil, 1))
			// <metadata>
			respFrame.writeInt(0) // <flags>
			respFrame.writeInt(0) // <columns_count>
			if srv.protocol >= protoVersion4 {
				respFrame.writeInt(0) // <pk_count>
			}
			// <result_metadata>
			respFrame.writeInt(int32(frm.FlagNoMetaData)) // <flags>
			respFrame.writeInt(0)
		case "metadata":
			respFrame.writeHeader(0, frm.OpResult, head.Stream)
			respFrame.writeInt(frm.ResultKindPrepared)
			// <id>
			respFrame.writeShortBytes(binary.BigEndian.AppendUint64(nil, 2))
			// <metadata>
			respFrame.writeInt(0) // <flags>
			respFrame.writeInt(0) // <columns_count>
			if srv.protocol >= protoVersion4 {
				respFrame.writeInt(0) // <pk_count>
			}
			// <result_metadata>
			respFrame.writeInt(int32(frm.FlagGlobalTableSpec)) // <flags>
			respFrame.writeInt(1)                              // <columns_count>
			// <global_table_spec>
			respFrame.writeString("keyspace")
			respFrame.writeString("table")
			// <col_spec_0>
			respFrame.writeString("col0")             // <name>
			respFrame.writeShort(uint16(TypeBoolean)) // <type>
		case "batchmetadata":
			respFrame.writeHeader(0, frm.OpResult, head.Stream)
			respFrame.writeInt(frm.ResultKindPrepared)
			// <id>
			respFrame.writeShortBytes(binary.BigEndian.AppendUint64(nil, 3))
			// <metadata>
			respFrame.writeInt(0) // <flags>
			respFrame.writeInt(2) // <columns_count>
			if srv.protocol >= protoVersion4 {
				respFrame.writeInt(0) // <pk_count>
			}
			// <col_spec_0>
			respFrame.writeString("ks1")
			respFrame.writeString("tbl1")
			respFrame.writeString("col1")
			respFrame.writeShort(uint16(TypeInt))
			// <col_spec_1>
			respFrame.writeString("ks2")
			respFrame.writeString("tbl2")
			respFrame.writeString("col2")
			respFrame.writeShort(uint16(TypeInt))
			// <result_metadata>
			respFrame.writeInt(int32(frm.FlagNoMetaData))
			respFrame.writeInt(0)
		default:
			respFrame.writeHeader(0, frm.OpError, head.Stream)
			respFrame.writeInt(0)
			respFrame.writeString("unsupported query: " + name)
		}
	case frm.OpExecute:
		b := reqFrame.readShortBytesCopy()
		id := binary.BigEndian.Uint64(b)
		// <query_parameters>
		reqFrame.readConsistency() // <consistency>
		var flags byte
		if srv.protocol > protoVersion4 {
			ui := reqFrame.readInt()
			flags = byte(ui)
		} else {
			flags = reqFrame.readByte()
		}
		switch id {
		case 1:
			if flags&frm.FlagSkipMetaData != 0 {
				respFrame.writeHeader(0, frm.OpError, head.Stream)
				respFrame.writeInt(0)
				respFrame.writeString("skip metadata unexpected")
			} else {
				respFrame.writeHeader(0, frm.OpResult, head.Stream)
				respFrame.writeInt(frm.ResultKindRows)
				// <metadata>
				respFrame.writeInt(0) // <flags>
				respFrame.writeInt(0) // <columns_count>
				// <rows_count>
				respFrame.writeInt(0)
			}
		case 2:
			if flags&frm.FlagSkipMetaData != 0 {
				respFrame.writeHeader(0, frm.OpResult, head.Stream)
				respFrame.writeInt(frm.ResultKindRows)
				// <metadata>
				respFrame.writeInt(0) // <flags>
				respFrame.writeInt(0) // <columns_count>
				// <rows_count>
				respFrame.writeInt(0)
			} else {
				respFrame.writeHeader(0, frm.OpError, head.Stream)
				respFrame.writeInt(0)
				respFrame.writeString("skip metadata expected")
			}
		default:
			respFrame.writeHeader(0, frm.OpError, head.Stream)
			respFrame.writeInt(ErrCodeUnprepared)
			respFrame.writeString("unprepared")
			respFrame.writeShortBytes(binary.BigEndian.AppendUint64(nil, id))
		}
	default:
		respFrame.writeHeader(0, frm.OpError, head.Stream)
		respFrame.writeInt(0)
		respFrame.writeString("not supported")
	}

	respFrame.buf[0] = srv.protocol | 0x80

	if err := respFrame.finish(); err != nil {
		srv.errorLocked(err)
	}

	if err := respFrame.writeTo(conn); err != nil {
		if !errors.Is(err, net.ErrClosed) {
			srv.errorLocked(err)
		}
	}
}

func (srv *TestServer) readFrame(conn net.Conn) (*framer, error) {
	buf := make([]byte, srv.headerSize)
	head, err := readHeader(conn, buf)
	if err != nil {
		return nil, err
	}
	framer := newFramer(nil, srv.protocol)

	err = framer.readFrame(conn, &head)
	if err != nil {
		return nil, err
	}

	// should be a request frame
	if head.Version.Response() {
		return nil, fmt.Errorf("expected to read a request frame got version: %v", head.Version)
	} else if head.Version.Version() != srv.protocol {
		return nil, fmt.Errorf("expected to read protocol version 0x%x got 0x%x", srv.protocol, head.Version.Version())
	}

	return framer, nil
}

func TestGetSchemaAgreement(t *testing.T) {
	schema_version1 := ParseUUIDMust("af810386-a694-11ef-81fa-3aea73156247")
	peersRows := []schemaAgreementHost{
		{
			DataCenter:    "datacenter1",
			HostID:        ParseUUIDMust("b2035fd9-e0ca-4857-8c45-e63c00fb7c43"),
			Rack:          "rack1",
			RPCAddress:    "127.0.0.3",
			SchemaVersion: schema_version1,
		},
		{
			DataCenter:    "datacenter1",
			HostID:        ParseUUIDMust("4b21ee4c-acea-4267-8e20-aaed5361a0dd"),
			Rack:          "rack1",
			RPCAddress:    "127.0.0.2",
			SchemaVersion: schema_version1,
		},
		{
			DataCenter:    "datacenter2",
			HostID:        ParseUUIDMust("dfef4a22-b8d8-47e9-aee5-8c19d4b7a9e3"),
			Rack:          "rack1",
			RPCAddress:    "127.0.0.5",
			SchemaVersion: ParseUUIDMust("875a938a-a695-11ef-4314-85c8ef0ebaa2"),
		},
	}

	var logger StdLogger

	t.Run("SchemaNotConsistent", func(t *testing.T) {
		err := getSchemaAgreement(
			[]string{"875a938a-a695-11ef-4314-85c8ef0ebaa2"},
			peersRows,
			logger,
		)

		assert.Error(t, err, "error expected when local schema is different then others")
	})

	t.Run("ZeroTokenNodeSchemaNotConsistent", func(t *testing.T) {
		err := getSchemaAgreement(
			[]string{"af810386-a694-11ef-81fa-3aea73156247"},
			peersRows,
			logger,
		)

		assert.Error(t, err, "expected error when zero-token node has different schema")
	})

	t.Run("SchemaConsistent", func(t *testing.T) {
		peersRows[2].SchemaVersion = schema_version1
		err := getSchemaAgreement(
			[]string{"af810386-a694-11ef-81fa-3aea73156247"},
			peersRows,
			logger,
		)

		assert.NoError(t, err, "expected no error when all nodes have the same schema")
	})
}

func TestUseKeyspaceQuoteEscaping(t *testing.T) {
	tests := []struct {
		keyspace string
		want     string
	}{
		{"simple", `USE "simple"`},
		{`my"ks`, `USE "my""ks"`},
		{`a""b`, `USE "a""""b"`},
		{`"`, `USE """"`},
		{"", `USE ""`},
	}
	for _, tt := range tests {
		got := useKeyspaceStmt(tt.keyspace)
		if got != tt.want {
			t.Errorf("keyspace %q: got %q, want %q", tt.keyspace, got, tt.want)
		}
	}
}

// newTestConnWithFramerPool creates a minimal Conn with an initialized framer pool
// suitable for testing releaseFramer and EWMA logic.
func newTestConnWithFramerPool() *Conn {
	c := &Conn{}
	c.framers.defaults = framerConfig{
		proto: protoVersion4 & protoVersionMask,
	}
	c.framers.initPool(c)
	return c
}

func buildTestFrame(t *testing.T, f *framer, req frameBuilder, streamID int) ([]byte, frm.FrameHeader) {
	t.Helper()

	if err := req.buildFrame(f, streamID); err != nil {
		t.Fatalf("buildFrame failed: %v", err)
	}

	buf := append([]byte(nil), f.buf...)
	header, err := readHeader(bytes.NewReader(buf), make([]byte, headSize))
	if err != nil {
		t.Fatalf("readHeader failed: %v", err)
	}

	return buf, header
}

func TestReleaseFramer(t *testing.T) {
	t.Parallel()

	t.Run("EWMAEquilibrium", func(t *testing.T) {
		c := newTestConnWithFramerPool()

		// Release framers with bufCap == avg. EWMA should not drift.
		for i := 0; i < 20; i++ {
			f := c.getReadFramer()
			// readBuffer is defaultBufSize (128), avg starts at defaultBufSize
			c.releaseReadFramer(f)
		}

		avg := c.framers.readPool.bufAvgSize.Load()
		if avg != defaultBufSize {
			t.Errorf("EWMA should stay at defaultBufSize=%d when all buffers equal, got %d", defaultBufSize, avg)
		}
	})

	t.Run("DelegatesFramerReleaseToConn", func(t *testing.T) {
		c := newTestConnWithFramerPool()

		f := c.getReadFramer()
		f.readBuffer = make([]byte, 4096)

		f.Release()

		avgAfterFirstRelease := c.framers.readPool.bufAvgSize.Load()
		if avgAfterFirstRelease <= defaultBufSize {
			t.Fatalf("framer.Release() should route through Conn.releaseFramer and update EWMA, got %d", avgAfterFirstRelease)
		}

		f.Release()

		if avgAfterSecondRelease := c.framers.readPool.bufAvgSize.Load(); avgAfterSecondRelease != avgAfterFirstRelease {
			t.Fatalf("second framer.Release() should be a no-op: first avg=%d second avg=%d", avgAfterFirstRelease, avgAfterSecondRelease)
		}
	})

	t.Run("EWMAConvergesUpward", func(t *testing.T) {
		c := newTestConnWithFramerPool()

		// Release framers with a larger buffer; EWMA should converge toward it.
		const targetSize = 4096
		for i := 0; i < 100; i++ {
			f := c.getReadFramer()
			f.readBuffer = make([]byte, targetSize)
			c.releaseReadFramer(f)
		}

		avg := c.framers.readPool.bufAvgSize.Load()
		// After 100 iterations with weight=8, avg should be very close to targetSize.
		// Allow 1% tolerance.
		if avg < targetSize*99/100 || avg > targetSize*101/100 {
			t.Errorf("EWMA should converge to ~%d, got %d", targetSize, avg)
		}
	})

	t.Run("EWMAConvergesDownward", func(t *testing.T) {
		c := newTestConnWithFramerPool()

		// First, push EWMA up.
		for i := 0; i < 100; i++ {
			f := c.getReadFramer()
			f.readBuffer = make([]byte, 4096)
			c.releaseReadFramer(f)
		}

		// Now release framers with small buffers; EWMA should converge back down.
		// Due to upward bias (+4 rounding), convergence downward is slower.
		const smallSize = 256
		for i := 0; i < 200; i++ {
			f := c.getReadFramer()
			f.readBuffer = make([]byte, smallSize)
			c.releaseReadFramer(f)
		}

		avg := c.framers.readPool.bufAvgSize.Load()
		// Due to the upward-biased rounding (+framerBufEWMAWeight/2), the EWMA settles
		// slightly above the actual sample value when converging downward. The steady-state
		// offset is at most framerBufEWMAWeight/2 (i.e., 4) per step which compounds to
		// roughly framerBufEWMAWeight/2 above the target. Allow generous tolerance.
		if avg < smallSize || avg > smallSize+2*framerBufEWMAWeight {
			t.Errorf("EWMA should converge toward ~%d (with upward bias), got %d", smallSize, avg)
		}
	})

	t.Run("ShrinkOversizedBuffer", func(t *testing.T) {
		c := newTestConnWithFramerPool()

		// EWMA starts at defaultBufSize (128). Release a very large framer.
		f := c.getReadFramer()
		f.readBuffer = make([]byte, 100000)
		origBuf := f.readBuffer
		c.releaseReadFramer(f)

		// Get the framer back from the pool and check that its buffer was shrunk.
		f2 := c.getReadFramer()
		if cap(f2.readBuffer) >= cap(origBuf) {
			t.Errorf("oversized buffer should have been shrunk: original cap=%d, new cap=%d",
				cap(origBuf), cap(f2.readBuffer))
		}
		// Shrink target should be at least defaultBufSize.
		if cap(f2.readBuffer) < defaultBufSize {
			t.Errorf("shrunk buffer should be at least defaultBufSize=%d, got cap=%d",
				defaultBufSize, cap(f2.readBuffer))
		}
		c.releaseReadFramer(f2)
	})

	t.Run("NoShrinkNormalBuffer", func(t *testing.T) {
		c := newTestConnWithFramerPool()

		// Release a few framers with identical buffers; none should be shrunk.
		for i := 0; i < 10; i++ {
			f := c.getReadFramer()
			origCap := cap(f.readBuffer)
			c.releaseReadFramer(f)
			f2 := c.getReadFramer()
			if cap(f2.readBuffer) != origCap {
				t.Errorf("iteration %d: normal-sized buffer should not be shrunk: orig cap=%d, new cap=%d",
					i, origCap, cap(f2.readBuffer))
			}
			c.releaseReadFramer(f2)
		}
	})

	t.Run("ShrinkFloorIsDefaultBufSize", func(t *testing.T) {
		c := newTestConnWithFramerPool()

		// Push EWMA down to a very small value by releasing tiny buffers.
		// The shrink target should never go below defaultBufSize.
		for i := 0; i < 100; i++ {
			f := c.getReadFramer()
			f.readBuffer = make([]byte, 1) // Tiny buffer
			c.releaseReadFramer(f)
		}

		// Now release a moderately large buffer that triggers shrink.
		f := c.getReadFramer()
		f.readBuffer = make([]byte, 10000)
		c.releaseReadFramer(f)

		f2 := c.getReadFramer()
		if cap(f2.readBuffer) < defaultBufSize {
			t.Errorf("shrink target should respect defaultBufSize floor: got cap=%d, want >= %d",
				cap(f2.readBuffer), defaultBufSize)
		}
		c.releaseReadFramer(f2)
	})

	t.Run("NilFramer", func(t *testing.T) {
		c := newTestConnWithFramerPool()
		// Should not panic.
		c.releaseReadFramer(nil)
	})

	t.Run("NoPool", func(t *testing.T) {
		c := &Conn{} // No pool initialized.
		f := newFramer(nil, protoVersion4)
		// Should not panic, framer is just dropped.
		c.releaseReadFramer(f)
	})

	t.Run("ReadAndWritePoolsAreSeparate", func(t *testing.T) {
		c := newTestConnWithFramerPool()

		readFramer := c.getReadFramer()
		readFramer.readBuffer = make([]byte, 100000)
		c.releaseReadFramer(readFramer)

		writeFramer := c.getWriteFramer()
		writeFramer.buf = make([]byte, 0, 8192)
		c.releaseWriteFramer(writeFramer)

		if writeAvg := c.framers.writePool.bufAvgSize.Load(); writeAvg <= defaultBufSize {
			t.Fatalf("writer pool should track its own EWMA, got %d", writeAvg)
		}

		writeFramer = c.getWriteFramer()
		if cap(writeFramer.buf) >= cap(readFramer.readBuffer) {
			t.Fatalf("writer framer should not inherit oversized reader buffer state, got writer cap=%d reader cap=%d", cap(writeFramer.buf), cap(readFramer.readBuffer))
		}
		c.releaseWriteFramer(writeFramer)
	})

	t.Run("WriteFramerResetsCustomPayloadFlagBetweenUses", func(t *testing.T) {
		c := newTestConnWithFramerPool()
		const streamID = 7

		f := c.getWriteFramer()
		payloadReq := &writeQueryFrame{
			statement: "SELECT now() FROM system.local",
			customPayload: map[string][]byte{
				"k": []byte("v"),
			},
		}
		_, payloadHeader := buildTestFrame(t, f, payloadReq, streamID)
		if payloadHeader.Flags&frm.FlagCustomPayload == 0 {
			t.Fatalf("custom payload frame should set %v, got flags=%08b", frm.FlagCustomPayload, payloadHeader.Flags)
		}

		c.releaseWriteFramer(f)
		if got, want := f.flags, c.framers.defaults.flags; got != want {
			t.Fatalf("releaseWriteFramer should restore default flags: got %08b want %08b", got, want)
		}

		f = c.getWriteFramer()
		plainReq := &writeQueryFrame{statement: "SELECT now() FROM system.local"}
		plainBuf, plainHeader := buildTestFrame(t, f, plainReq, streamID)
		if plainHeader.Flags != c.framers.defaults.flags {
			t.Fatalf("plain query should use default flags after pooled reuse: got %08b want %08b", plainHeader.Flags, c.framers.defaults.flags)
		}

		fresh := newFramer(nil, protoVersion4)
		freshBuf, freshHeader := buildTestFrame(t, fresh, plainReq, streamID)
		if plainHeader.Flags != freshHeader.Flags {
			t.Fatalf("reused plain query flags do not match fresh framer: got %08b want %08b", plainHeader.Flags, freshHeader.Flags)
		}
		if !bytes.Equal(plainBuf, freshBuf) {
			t.Fatal("reused plain query frame does not match fresh framer output")
		}

		c.releaseWriteFramer(f)
	})

	t.Run("WriteFramerResetsTracingFlagBetweenUses", func(t *testing.T) {
		c := newTestConnWithFramerPool()
		const streamID = 9

		f := c.getWriteFramer()
		f.trace()
		tracedReq := &writeQueryFrame{statement: "SELECT now() FROM system.local"}
		_, tracedHeader := buildTestFrame(t, f, tracedReq, streamID)
		if tracedHeader.Flags&frm.FlagTracing == 0 {
			t.Fatalf("traced query should set %v, got flags=%08b", frm.FlagTracing, tracedHeader.Flags)
		}

		c.releaseWriteFramer(f)
		if got, want := f.flags, c.framers.defaults.flags; got != want {
			t.Fatalf("releaseWriteFramer should restore default flags: got %08b want %08b", got, want)
		}

		f = c.getWriteFramer()
		_, plainHeader := buildTestFrame(t, f, tracedReq, streamID)
		if plainHeader.Flags&frm.FlagTracing != 0 {
			t.Fatalf("plain query should not inherit tracing flag after pooled reuse: got %08b", plainHeader.Flags)
		}

		c.releaseWriteFramer(f)
	})
}
