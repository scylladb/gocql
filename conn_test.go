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
	"syscall"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

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

// waitForPoolSize blocks until session's connection pool holds at least want
// connections, or timeout elapses.
//
// CreateSession only guarantees the first connection to each host: the rest of
// a pool is filled asynchronously, so a test making a claim about every
// connection of a session has to wait for them rather than assert on whichever
// ones it happened to observe.
func waitForPoolSize(session *Session, want int, timeout time.Duration) error {
	deadline := time.After(timeout)
	for {
		if got := session.pool.Size(); got >= want {
			return nil
		}

		select {
		case <-deadline:
			return fmt.Errorf("pool reached %d of %d connections in %s", session.pool.Size(), want, timeout)
		case <-time.After(time.Millisecond):
		}
	}
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

	// Use bare IP (no port) so all entries are portless; the driver falls back
	// to cfg.Port, which is where the test server is bound.
	cluster := NewCluster("cassandra1.invalid", "127.0.0.1", "cassandra2.invalid")
	cluster.Port = srv.port()
	cluster.Logger = log
	cluster.ProtoVersion = int(defaultProto)
	cluster.disableControlConn = true
	cluster.DNSResolver = brokenDNSResolver{}

	// CreateSession() should attempt to resolve the DNS name "cassandraX.invalid"
	// and fail, but continue to connect via 127.0.0.1
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
		srv := NewTestServerWithAddress(ip+":9042", t, defaultProto, ctx)
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
		srv := NewTestServerWithAddress(ip+":9042", t, defaultProto, ctx)
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
		r: &connReader{
			r: bufio.NewReader(&buf),
		},
		streams: streams.New(),
		logger:  &defaultLogger{},
		cfg:     &ConnConfig{},
	}

	err := conn.recv(context.Background(), false)
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

// heartBeat used to panic on an OPTIONS reply that was neither SUPPORTED nor an
// error frame. parseFrame builds a frame for every opcode it knows, so a server can
// produce one, and heartBeat runs on its own goroutine with nothing recovering above
// it -- so a malformed reply took the process down rather than the connection. It is
// counted as a heartbeat failure now, and six of those close the connection.
//
// The test asserts the connection survives one such reply; it fails on the old code
// by aborting the test binary, which is exactly what the panic did in production.
func TestHeartBeatSurvivesUnexpectedOptionsResponse(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	srv := NewTestServer(t, defaultProto, ctx)
	defer srv.Stop()

	cluster := testCluster(defaultProto, srv.Address)
	cluster.NumConns = 1
	db, err := cluster.CreateSession()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Startup reads SUPPORTED off an OPTIONS of its own, so the pool has to be up
	// before the replies are spoiled -- otherwise the connection never opens and the
	// heartbeat under test never runs.
	if err := waitForPoolSize(db, 1, 5*time.Second); err != nil {
		t.Fatal(err)
	}
	served := atomic.LoadInt64(&srv.nOptions)
	atomic.StoreInt32(&srv.readyOnOptions, 1)

	// The first beat is a second out; wait for the server to see it rather than
	// sleeping past it.
	deadline := time.Now().Add(10 * time.Second)
	for atomic.LoadInt64(&srv.nOptions) == served {
		if time.Now().After(deadline) {
			t.Fatal("the connection never sent another OPTIONS")
		}
		time.Sleep(10 * time.Millisecond)
	}

	if err := db.Query("void").Exec(); err != nil {
		t.Fatalf("the session did not survive an unexpected heartbeat reply: %v", err)
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
	c.r = &connReader{
		conn: client,
		r:    bufio.NewReader(client),
	}
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

	const execCloseTestTimeout = 10 * time.Second

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
		case <-time.After(execCloseTestTimeout):
			t.Fatal("exec never reached the write path")
		}
		cancel()

		select {
		case err := <-errCh:
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("expected context cancel error %v, got %v", context.Canceled, err)
			}
		case <-time.After(execCloseTestTimeout):
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
		case <-time.After(execCloseTestTimeout):
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
		case <-time.After(execCloseTestTimeout):
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
		case <-time.After(execCloseTestTimeout):
			t.Fatal("exec deadlocked after closeWithError")
		}

		select {
		case <-closeDone:
		case <-time.After(execCloseTestTimeout):
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
		case <-time.After(execCloseTestTimeout):
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
		case <-time.After(execCloseTestTimeout):
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
		case <-time.After(execCloseTestTimeout):
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
		case <-time.After(execCloseTestTimeout):
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
		case <-time.After(execCloseTestTimeout):
			t.Fatal("stream observer did not observe the request start")
		}

		select {
		case <-writeStarted:
		case <-time.After(execCloseTestTimeout):
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
		case <-time.After(execCloseTestTimeout):
			t.Fatal("exec deadlocked after Close")
		}

		select {
		case <-observerCtx.abandoned:
		case <-time.After(execCloseTestTimeout):
			t.Fatal("Close did not abandon the in-flight stream")
		}

		select {
		case <-observerCtx.finished:
			t.Fatal("Close should not mark the in-flight stream as finished")
		default:
		}

		select {
		case <-closeDone:
		case <-time.After(execCloseTestTimeout):
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
	info, err := conn.prepareStatement(ctx, stmt, nil, "", time.Second)
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

	// stallSystemQueries, when non-zero, makes system.peers/system.local queries
	// hang until the server is stopped, simulating a stalled backend.
	stallSystemQueries int32

	// readyOnOptions, when non-zero, answers OPTIONS with READY instead of
	// SUPPORTED -- a reply that is neither SUPPORTED nor an error frame, which is
	// the case heartBeat has no arm for. Every connection reads SUPPORTED off an
	// OPTIONS during startup too, so a test has to raise this only once its pool is
	// up. nOptions counts the OPTIONS served, so it can then wait for the heartbeat
	// it perturbed rather than sleeping for one.
	readyOnOptions int32
	nOptions       int64

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

func (srv *TestServer) port() int {
	return srv.listen.Addr().(*net.TCPAddr).Port
}

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
					if err == io.EOF || errors.Is(err, net.ErrClosed) || errors.Is(err, syscall.ECONNRESET) {
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

// testMetadataIDOffset separates a canned result metadata id from the prepared id
// it belongs to, so reading one where the other belongs is visible on the wire
// rather than coincidentally equal.
const testMetadataIDOffset = 0x1000

func (srv *TestServer) process(conn net.Conn, reqFrame *framer, exts map[string][]string) {
	head := reqFrame.header
	if head == nil {
		srv.errorLocked("process frame with a nil header")
		return
	}
	respFrame := newFramer(nil, reqFrame.proto)

	// SCYLLA_USE_METADATA_ID puts a result metadata id after the prepared id in both
	// RESULT/Prepared and EXECUTE. The driver opts in whenever the server advertised
	// the key, so the advertised map is the same signal it used, and the canned
	// frames below have to match the layout it will then read and write.
	_, useMetadataID := exts[scyllaUseMetadataID]

	// writeResultMetadataID emits that field for a prepared statement. It is a no-op
	// unless the extension was advertised, so servers that do not advertise it keep
	// producing exactly the frames they did before.
	writeResultMetadataID := func(preparedID uint64) {
		if useMetadataID {
			respFrame.writeShortBytes(binary.BigEndian.AppendUint64(nil, preparedID+testMetadataIDOffset))
		}
	}

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
		atomic.AddInt64(&srv.nOptions, 1)
		if atomic.LoadInt32(&srv.readyOnOptions) != 0 {
			respFrame.writeHeader(0, frm.OpReady, head.Stream)
			break
		}
		respFrame.writeHeader(0, frm.OpSupported, head.Stream)
		respFrame.writeStringMultiMap(exts)
	case frm.OpQuery:
		query := reqFrame.readLongString()
		first := query
		if n := strings.Index(query, " "); n > 0 {
			first = first[:n]
		}
		if atomic.LoadInt32(&srv.stallSystemQueries) != 0 &&
			(strings.Contains(query, "system.peers") || strings.Contains(query, "system.local")) {
			<-srv.ctx.Done()
			return
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
			writeResultMetadataID(1)
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
			writeResultMetadataID(2)
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
			writeResultMetadataID(3)
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
		case "metadatachangednocolumns":
			// Prepared with real result metadata and (under the extension) a metadata
			// id, so the driver caches columns and asks the server to skip metadata on
			// execute. Its EXECUTE reply, id 4 below, is then deliberately malformed.
			respFrame.writeHeader(0, frm.OpResult, head.Stream)
			respFrame.writeInt(frm.ResultKindPrepared)
			// <id>
			respFrame.writeShortBytes(binary.BigEndian.AppendUint64(nil, 4))
			writeResultMetadataID(4)
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
		default:
			respFrame.writeHeader(0, frm.OpError, head.Stream)
			respFrame.writeInt(0)
			respFrame.writeString("unsupported query: " + name)
		}
	case frm.OpExecute:
		b := reqFrame.readShortBytesCopy()
		id := binary.BigEndian.Uint64(b)
		if useMetadataID {
			// The driver writes the result metadata id between the prepared id and the
			// query parameters once the extension is negotiated. Consume it, or every
			// read below lands two bytes plus the id's length too early.
			reqFrame.readShortBytesCopy()
		}
		// <query_parameters>
		reqFrame.readConsistency() // <consistency>
		var flags uint32
		if srv.protocol > protoVersion4 {
			ui := reqFrame.readInt()
			flags = uint32(ui)
		} else {
			flags = uint32(reqFrame.readByte())
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
		case 4:
			// The driver is expected to have asked to skip metadata here: the extension
			// is negotiated, this statement holds a metadata id, and its prepared
			// response carried columns. Refusing otherwise keeps that part of the
			// skip-metadata decision pinned on a live connection.
			if flags&frm.FlagSkipMetaData == 0 {
				respFrame.writeHeader(0, frm.OpError, head.Stream)
				respFrame.writeInt(0)
				respFrame.writeString("skip metadata expected")
			} else {
				// METADATA_CHANGED promises new result metadata; NO_METADATA says none
				// was sent. A driver that adopted the new id would stop being sent
				// metadata altogether, and one that decoded these rows against its
				// cached columns would be decoding against the very columns the server
				// just declared stale. Neither is acceptable, so it must reject this.
				respFrame.writeHeader(0, frm.OpResult, head.Stream)
				respFrame.writeInt(frm.ResultKindRows)
				// <metadata>
				respFrame.writeInt(int32(frm.FlagMetaDataChanged | frm.FlagNoMetaData)) // <flags>
				respFrame.writeInt(0)                                                   // <columns_count>
				respFrame.writeShortBytes(binary.BigEndian.AppendUint64(nil, 0xBEEF))   // <new_metadata_id>
				// <rows_count>
				respFrame.writeInt(0)
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
		if !errors.Is(err, net.ErrClosed) && !errors.Is(err, syscall.ECONNRESET) && !errors.Is(err, syscall.EPIPE) {
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
			"875a938a-a695-11ef-4314-85c8ef0ebaa2",
			peersRows,
			logger,
		)

		assert.Error(t, err, "error expected when local schema is different then others")
	})

	t.Run("ZeroTokenNodeSchemaNotConsistent", func(t *testing.T) {
		err := getSchemaAgreement(
			"af810386-a694-11ef-81fa-3aea73156247",
			peersRows,
			logger,
		)

		assert.Error(t, err, "expected error when zero-token node has different schema")
	})

	t.Run("SchemaConsistent", func(t *testing.T) {
		peersRows[2].SchemaVersion = schema_version1
		err := getSchemaAgreement(
			"af810386-a694-11ef-81fa-3aea73156247",
			peersRows,
			logger,
		)

		assert.NoError(t, err, "expected no error when all nodes have the same schema")
	})

	t.Run("EmptyLocalSchemaVersion", func(t *testing.T) {
		err := getSchemaAgreement(
			"",
			[]schemaAgreementHost{
				{
					DataCenter:    "datacenter1",
					HostID:        ParseUUIDMust("b2035fd9-e0ca-4857-8c45-e63c00fb7c43"),
					Rack:          "rack1",
					RPCAddress:    "127.0.0.3",
					SchemaVersion: schema_version1,
				},
			},
			logger,
		)

		assert.NoError(t, err, "expected no error when local version is empty and peers are consistent")
	})

	t.Run("EmptyLocalWithNoPeers", func(t *testing.T) {
		err := getSchemaAgreement("", nil, logger)

		assert.NoError(t, err, "expected no error when both local and peers are empty")
	})

	t.Run("EmptyLocalWithInconsistentPeers", func(t *testing.T) {
		err := getSchemaAgreement(
			"",
			[]schemaAgreementHost{
				{
					DataCenter:    "datacenter1",
					HostID:        ParseUUIDMust("b2035fd9-e0ca-4857-8c45-e63c00fb7c43"),
					Rack:          "rack1",
					RPCAddress:    "127.0.0.3",
					SchemaVersion: schema_version1,
				},
				{
					DataCenter:    "datacenter2",
					HostID:        ParseUUIDMust("dfef4a22-b8d8-47e9-aee5-8c19d4b7a9e3"),
					Rack:          "rack1",
					RPCAddress:    "127.0.0.5",
					SchemaVersion: ParseUUIDMust("875a938a-a695-11ef-4314-85c8ef0ebaa2"),
				},
			},
			logger,
		)

		assert.Error(t, err, "expected error when peers have different schemas")
	})
}

// TestAwaitSchemaAgreementBoundedByMaxWaitSchemaAgreement verifies that a stalled
// system.peers/system.local query cannot make awaitSchemaAgreement block past
// MaxWaitSchemaAgreement, even when MetadataSchemaRequestTimeout is much larger.
func TestAwaitSchemaAgreementBoundedByMaxWaitSchemaAgreement(t *testing.T) {
	srv := NewTestServer(t, defaultProto, context.Background())
	defer srv.Stop()

	const maxWaitSchemaAgreement = 200 * time.Millisecond
	const metadataSchemaRequestTimeout = 5 * time.Second

	cluster := testCluster(defaultProto, srv.Address)
	cluster.MaxWaitSchemaAgreement = maxWaitSchemaAgreement
	cluster.MetadataSchemaRequestTimeout = metadataSchemaRequestTimeout

	session, err := cluster.CreateSession()
	if err != nil {
		t.Fatalf("CreateSession: %v", err)
	}
	defer session.Close()

	conn := session.getConn()
	if conn == nil {
		t.Fatal("unable to get a connection")
	}

	atomic.StoreInt32(&srv.stallSystemQueries, 1)

	start := time.Now()
	err = conn.awaitSchemaAgreement(context.Background())
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("expected an error from a stalled schema agreement query, got nil")
	}
	if elapsed >= metadataSchemaRequestTimeout {
		t.Fatalf("awaitSchemaAgreement took %v, expected to return near MaxWaitSchemaAgreement (%v), not wait for MetadataSchemaRequestTimeout (%v)",
			elapsed, maxWaitSchemaAgreement, metadataSchemaRequestTimeout)
	}
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

// TestInitFramerCacheScyllaUseMetadataID guards against scyllaUseMetadataID
// being negotiated on the Conn but never reaching the pooled framers that
// actually read/write frames (see frame.go's parseResultMetadata,
// parseResultPrepared, writeExecuteFrame).
//
// It also pins Conn.usesMetadataID against the same config, since the request path
// reads the flag through it: were the two to diverge, the driver would ask the
// server to skip metadata while writing no result metadata ID to compare against.
func TestInitFramerCacheScyllaUseMetadataID(t *testing.T) {
	c := &Conn{
		version:      protoVersion4,
		cqlProtoExts: []cqlProtocolExtension{&scyllaUseMetadataIDExt{}},
	}
	c.initFramerCache()

	if !c.framers.defaults.scyllaUseMetadataID {
		t.Fatal("framerConfig.scyllaUseMetadataID should be true once SCYLLA_USE_METADATA_ID is negotiated")
	}

	if !c.usesMetadataID() {
		t.Error("Conn.usesMetadataID() should agree with the framer config")
	}

	wf := c.getWriteFramer()
	if !wf.scyllaUseMetadataID {
		t.Error("write framer obtained from pool should have scyllaUseMetadataID set")
	}

	rf := c.getReadFramer()
	if !rf.scyllaUseMetadataID {
		t.Error("read framer obtained from pool should have scyllaUseMetadataID set")
	}
}

// TestInitFramerCacheWithoutScyllaUseMetadataID is the negative counterpart: with
// no extension negotiated, nothing on the connection may claim otherwise.
func TestInitFramerCacheWithoutScyllaUseMetadataID(t *testing.T) {
	c := &Conn{version: protoVersion4}
	c.initFramerCache()

	if c.framers.defaults.scyllaUseMetadataID {
		t.Error("framerConfig.scyllaUseMetadataID should be false when the extension was not negotiated")
	}
	if c.usesMetadataID() {
		t.Error("Conn.usesMetadataID() should be false when the extension was not negotiated")
	}
	if c.getWriteFramer().scyllaUseMetadataID {
		t.Error("write framer should not have scyllaUseMetadataID set")
	}
}

// TestInitFramerCacheProtoV3IgnoresScyllaUseMetadataID pins the negotiation gate from
// the connection's end: a v3 connection to a server advertising SCYLLA_USE_METADATA_ID
// must come out of the handshake with the extension nowhere on it. It goes through
// parseCQLProtocolExtensions rather than setting cqlProtoExts directly, because that
// list is also what startupCoordinator.startup serializes into STARTUP — an extension
// filtered out there is one the driver neither announces nor encodes.
func TestInitFramerCacheProtoV3IgnoresScyllaUseMetadataID(t *testing.T) {
	t.Parallel()

	c := &Conn{version: protoVersion3, logger: &testLogger{}}
	c.cqlProtoExts = parseCQLProtocolExtensions(map[string][]string{scyllaUseMetadataID: {}}, c.version, c.logger)
	c.initFramerCache()

	if findCQLProtoExtByName(c.cqlProtoExts, scyllaUseMetadataID) != nil {
		t.Error("SCYLLA_USE_METADATA_ID should not be negotiated on a protocol v3 connection")
	}
	if c.framers.defaults.scyllaUseMetadataID {
		t.Error("framerConfig.scyllaUseMetadataID should be false on a protocol v3 connection")
	}
	if c.usesMetadataID() {
		t.Error("Conn.usesMetadataID() should be false on a protocol v3 connection")
	}
	if c.tracksResultMetadataID() {
		t.Error("Conn.tracksResultMetadataID() should be false on a protocol v3 connection")
	}
	if c.getWriteFramer().scyllaUseMetadataID {
		t.Error("write framer should not have scyllaUseMetadataID set on a protocol v3 connection")
	}
}

// TestConnTracksResultMetadataID pins the question the EXECUTE path actually asks:
// does this connection exchange result metadata IDs at all? Two mechanisms answer
// yes — native protocol v5, where the field is mandatory, and SCYLLA_USE_METADATA_ID,
// which backports it to v4 — and the skip-metadata default follows either, per
// scylladb/scylla-drivers#81.
//
// usesMetadataID must stay narrower than that: the integration tests use it to
// assert the extension was negotiated specifically, so a v5 connection reporting
// true there would turn a negotiation regression green.
func TestConnTracksResultMetadataID(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		name        string
		proto       byte
		exts        []cqlProtocolExtension
		wantTracked bool
		wantExt     bool
	}{
		{name: "v4 without the extension", proto: protoVersion4},
		{
			name:        "v4 with the extension",
			proto:       protoVersion4,
			exts:        []cqlProtocolExtension{&scyllaUseMetadataIDExt{}},
			wantTracked: true,
			wantExt:     true,
		},
		{
			// v5 makes the field mandatory, so no extension is involved.
			name:        "v5 without the extension",
			proto:       protoVersion5,
			wantTracked: true,
		},
		{
			// Not reachable through negotiation — newScyllaUseMetadataIDExt opts in on
			// v4 only — but initCache must not read the two mechanisms as exclusive.
			name:        "v5 with the extension",
			proto:       protoVersion5,
			exts:        []cqlProtocolExtension{&scyllaUseMetadataIDExt{}},
			wantTracked: true,
			wantExt:     true,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			c := &Conn{version: tt.proto, cqlProtoExts: tt.exts}
			c.initFramerCache()

			if got := c.tracksResultMetadataID(); got != tt.wantTracked {
				t.Errorf("Conn.tracksResultMetadataID() = %v, want %v", got, tt.wantTracked)
			}
			if got := c.usesMetadataID(); got != tt.wantExt {
				t.Errorf("Conn.usesMetadataID() = %v, want %v", got, tt.wantExt)
			}
		})
	}
}

// TestConnTracksResultMetadataIDMasksDirectionBit guards the derivation against the
// request/response direction bit: tracksResultMetadataID reads the framer config,
// which initCache masks with protoVersionMask. Folding the bit into the version
// instead would make a v4 connection look like protocol 0x84 and silently start
// skipping metadata with no ID to recover it.
func TestConnTracksResultMetadataIDMasksDirectionBit(t *testing.T) {
	t.Parallel()

	c := &Conn{version: protoVersion4 | 0x80}
	c.initFramerCache()

	if c.tracksResultMetadataID() {
		t.Error("Conn.tracksResultMetadataID() = true for v4 with the direction bit set, want false")
	}
}

// TestShouldSkipResultMetadata pins the skip-metadata decision as the EXECUTE path
// makes it, composing metadataIDTracked with shouldSkipResultMetadata exactly as
// Conn.executeQueryWithMetrics does.
//
// Three behaviours matter most. The override itself: with a metadata-ID exchange
// active and an ID in hand, metadata is skipped even though the session-level
// DisableSkipMetadata is set, because the server reports changes via
// METADATA_CHANGED. Its precondition: a connection whose cached prepared statement
// has no ID yet (prepared before SCYLLA_USE_METADATA_ID was negotiated, reachable
// during a rolling upgrade because the prepared cache is keyed by host and survives
// reconnects) must NOT skip — there is nothing for the server to compare against.
// And the column-set gate, which is what keeps statements whose prepared response
// carries no result metadata decodable at all. A per-query NoSkipMetadata()
// overrides all of it.
//
// idExchangeActive stands for Conn.tracksResultMetadataID: native protocol v5 or
// the SCYLLA_USE_METADATA_ID extension. TestConnTracksResultMetadataID covers which
// connections set it.
func TestShouldSkipResultMetadata(t *testing.T) {
	t.Parallel()

	id := []byte{0xAA, 0xBB}

	tests := []struct {
		name               string
		sessionDisableSkip bool
		queryDisableSkip   bool
		idExchangeActive   bool
		resultMetadataID   []byte
		hasColumns         bool
		want               bool
	}{
		{name: "default skips when columns present", hasColumns: true, want: true},
		{name: "no columns never skips", hasColumns: false, want: false},
		{name: "session DisableSkipMetadata forces metadata", sessionDisableSkip: true, hasColumns: true, want: false},
		{name: "query NoSkipMetadata forces metadata", queryDisableSkip: true, hasColumns: true, want: false},
		{
			name:               "id exchange with an id overrides session DisableSkipMetadata",
			sessionDisableSkip: true,
			idExchangeActive:   true,
			resultMetadataID:   id,
			hasColumns:         true,
			want:               true,
		},
		{
			name:               "query NoSkipMetadata still wins under the id exchange",
			sessionDisableSkip: true,
			queryDisableSkip:   true,
			idExchangeActive:   true,
			resultMetadataID:   id,
			hasColumns:         true,
			want:               false,
		},
		{
			// Statements whose RESULT/Prepared carries no result metadata (LIST ROLES OF
			// is the motivating case) are handed an ID hashed from empty metadata. Current
			// ScyllaDB compares the returned ID against that same empty-metadata ID, always
			// matches, and never sets METADATA_CHANGED, so a skipped response would be
			// undecodable. The server-side fixes, scylladb/scylladb#29233 and #29275, are
			// both closed unmerged — this gate is what keeps such statements working.
			name:               "empty prepared column set never skips (scylladb/scylladb#29275)",
			sessionDisableSkip: true,
			idExchangeActive:   true,
			resultMetadataID:   id,
			hasColumns:         false,
			want:               false,
		},
		{
			// Same gate with skipping opted into explicitly, i.e. with nothing else left
			// to stop it.
			name:             "empty prepared column set never skips even when opted in",
			idExchangeActive: true,
			resultMetadataID: id,
			hasColumns:       false,
			want:             false,
		},
		{
			// Rolling upgrade: statement prepared before the extension was negotiated.
			name:               "id exchange without an id does not override",
			sessionDisableSkip: true,
			idExchangeActive:   true,
			resultMetadataID:   nil,
			hasColumns:         true,
			want:               false,
		},
		{
			name:               "id exchange with a zero-length id does not override",
			sessionDisableSkip: true,
			idExchangeActive:   true,
			resultMetadataID:   []byte{},
			hasColumns:         true,
			want:               false,
		},
		{
			// An id alone is not enough either: with no id exchange on the connection the
			// server has no way to report a change, so this must fall back to the session
			// setting.
			name:               "id without an id exchange does not override",
			sessionDisableSkip: true,
			idExchangeActive:   false,
			resultMetadataID:   id,
			hasColumns:         true,
			want:               false,
		},
		{
			// A user who explicitly opted into skipping keeps it regardless of the ID;
			// that is upstream behaviour and this change does not narrow it.
			name:               "explicit opt-in skips even without an id",
			sessionDisableSkip: false,
			idExchangeActive:   true,
			resultMetadataID:   nil,
			hasColumns:         true,
			want:               true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tracked := metadataIDTracked(tt.idExchangeActive, tt.resultMetadataID)
			got := shouldSkipResultMetadata(tt.sessionDisableSkip, tt.queryDisableSkip, tracked, tt.hasColumns)
			if got != tt.want {
				t.Errorf("shouldSkipResultMetadata(session=%v, query=%v, tracked=%v (idExchange=%v, id=%v), cols=%v) = %v, want %v",
					tt.sessionDisableSkip, tt.queryDisableSkip, tracked, tt.idExchangeActive, tt.resultMetadataID,
					tt.hasColumns, got, tt.want)
			}
		})
	}
}

// TestExecuteMetadataChangedWithoutColumns pins that a RESULT/Rows which sets
// METADATA_CHANGED while also setting NO_METADATA is rejected rather than decoded.
//
// The combination is malformed — METADATA_CHANGED obliges the server to include the
// new metadata — and both ways of carrying on are unrecoverable. Adopting the new id
// makes the server match it from then on and stop sending metadata at all; reusing the
// columns cached at prepare time decodes rows against the exact column set the server
// has just declared stale, which is the misdecode the mechanism exists to prevent. So
// the query must fail with the old id left in the cache, ready to be resent.
//
// It doubles as the only unit-level coverage of a negotiated SCYLLA_USE_METADATA_ID
// connection driven end to end: the mock server advertises the extension, so the driver
// reads a result metadata id out of RESULT/Prepared and writes one back on EXECUTE, and
// the server rejects the execute outright if skip_metadata was not requested.
func TestExecuteMetadataChangedWithoutColumns(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	srv := NewTestServerWithAddressAndSupportedFactory("127.0.0.1:0", t, protoVersion4, ctx,
		func(net.Conn) map[string][]string {
			return map[string][]string{scyllaUseMetadataID: {""}}
		})
	defer srv.Stop()

	db, err := testCluster(protoVersion4, srv.Address).CreateSession()
	if err != nil {
		t.Fatalf("CreateSession: %v", err)
	}
	defer db.Close()

	conn := db.getConn()
	if conn == nil {
		t.Fatal("no connection available")
	}
	if !conn.usesMetadataID() {
		t.Fatal("server advertised SCYLLA_USE_METADATA_ID but the driver did not negotiate it")
	}

	const stmt = "select metadatachangednocolumns"
	err = db.Query(stmt).Iter().Close()
	if err == nil {
		t.Fatal("expected an error for a METADATA_CHANGED response carrying no column metadata")
	}
	if !strings.Contains(err.Error(), "sent no column metadata") {
		t.Fatalf("unexpected error: %v", err)
	}

	// The id from the malformed response must not have been cached: the entry still
	// carries the one handed out at prepare time, so the next execute resends it and
	// the server gets another chance to answer with the metadata it owes.
	key := db.stmtsLRU.keyFor(conn.host.hostUUID(), conn.getCurrentKeyspace(), stmt)
	inflight, ok := db.stmtsLRU.get(key)
	if !ok {
		t.Fatal("prepared statement should still be cached")
	}
	want := binary.BigEndian.AppendUint64(nil, 4+testMetadataIDOffset)
	if got := inflight.preparedStatment.resultMetadataID; !bytes.Equal(got, want) {
		t.Errorf("cached resultMetadataID = % X, want the prepare-time id % X", got, want)
	}
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

// segmentTestTimeout bounds every wait in the v5 segment tests. It has to be
// generous enough for a loaded -race runner, but finite: an unbounded receive
// would turn a lost response into a whole-package `go test -timeout` kill instead
// of a failure pointing at the test.
const segmentTestTimeout = 10 * time.Second

// recvCallResp waits for a response to be delivered to call, failing the test on
// the test goroutine if it never arrives. Assertions must not run on a child
// goroutine: require's FailNow is only valid on the test goroutine, and a test
// that does not wait for its own assertions can return while they are still in
// flight — a genuine mismatch is then dropped or reported as a panic after the
// test completed.
func recvCallResp(t *testing.T, call *callReq) callResp {
	t.Helper()

	select {
	case resp := <-call.resp:
		return resp
	case <-time.After(segmentTestTimeout):
		t.Fatalf("timed out waiting for a response on stream %d", call.streamID)
		return callResp{}
	}
}

func TestConnProcessAllFramesInSingleSegment(t *testing.T) {
	server, client, err := tcpConnPair()
	require.NoError(t, err)
	defer server.Close()
	defer client.Close()

	w := &deadlineContextWriter{
		w:         server,
		semaphore: make(chan struct{}, 1),
		quit:      make(chan struct{}),
	}
	w.timeout.Store(int64(time.Second * 10))

	c := &Conn{
		r: &connReader{
			conn: server,
			r:    bufio.NewReader(server),
		},
		calls:      make(map[int]*callReq),
		version:    protoVersion5,
		addr:       server.RemoteAddr().String(),
		streams:    streams.New(),
		isSchemaV2: true,
		logger:     nopLogger{},
		w:          w,
	}
	c.writeTimeout.Store(int64(time.Second * 10))

	call1 := &callReq{
		timeout:  make(chan struct{}),
		streamID: 1,
		resp:     make(chan callResp),
	}

	call2 := &callReq{
		timeout:  make(chan struct{}),
		streamID: 2,
		resp:     make(chan callResp),
	}

	c.calls[1] = call1
	c.calls[2] = call2

	req := writeQueryFrame{
		statement: "SELECT * FROM system.local",
		params: queryParams{
			consistency: Quorum,
			keyspace:    "gocql_test",
		},
	}

	framer1 := newFramer(nil, protoVersion5)
	err = req.buildFrame(framer1, 1)
	require.NoError(t, err)

	framer2 := newFramer(nil, protoVersion5)
	err = req.buildFrame(framer2, 2)
	require.NoError(t, err)

	// Write from the test goroutine: this is a real loopback socket, and the two
	// frames plus the segment header and CRC are ~124 bytes, so the write completes
	// into the socket buffer without a reader.
	var buf []byte
	buf = append(buf, framer1.buf...)
	buf = append(buf, framer2.buf...)

	uncompressedSegment, err := newUncompressedSegment(buf, true)
	require.NoError(t, err)

	_, err = client.Write(uncompressedSegment)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), segmentTestTimeout)
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- c.recvSegment(ctx)
	}()

	// Receive in segment order: processAllFramesInSegment consumes the frames
	// sequentially and each send is on an unbuffered channel, so frame 1 is fully
	// delivered before frame 2 starts.
	//
	// The header is skipped because resp.framer has already parsed it; framer.buf
	// holds the body only.
	resp1 := recvCallResp(t, call1)
	require.Equal(t, framer1.buf[9:], resp1.framer.buf)

	resp2 := recvCallResp(t, call2)
	require.Equal(t, framer2.buf[9:], resp2.framer.buf)

	select {
	case err := <-errCh:
		require.NoError(t, err)
	case <-time.After(segmentTestTimeout):
		t.Fatal("recvSegment did not return after delivering both frames")
	}
}

// TestConnProcessReservedStreamFrameInSegment verifies that a reserved-stream
// frame (streamID -1) packed inside a self-contained v5 segment has its body
// consumed from the segment payload rather than the socket. A normal response
// frame for a real call follows it in the same segment; if the reserved frame's
// body were read from the socket (c.r) instead of the segment reader, the
// segment reader would be left misaligned and the trailing frame would never be
// delivered to its call (the test would time out).
func TestConnProcessReservedStreamFrameInSegment(t *testing.T) {
	server, client, err := tcpConnPair()
	require.NoError(t, err)
	defer server.Close()
	defer client.Close()

	w := &deadlineContextWriter{
		w:         server,
		semaphore: make(chan struct{}, 1),
		quit:      make(chan struct{}),
	}
	w.timeout.Store(int64(time.Second * 10))

	c := &Conn{
		r: &connReader{
			conn: server,
			r:    bufio.NewReader(server),
		},
		calls:      make(map[int]*callReq),
		version:    protoVersion5,
		addr:       server.RemoteAddr().String(),
		streams:    streams.New(),
		isSchemaV2: true,
		logger:     nopLogger{},
		w:          w,
	}
	c.writeTimeout.Store(int64(time.Second * 10))

	// Only stream 2 has a registered call; the reserved stream -1 frame ahead of
	// it goes through the readFrameIntoFramer body-read path.
	call2 := &callReq{timeout: make(chan struct{}), streamID: 2, resp: make(chan callResp)}
	c.calls[2] = call2

	req := writeQueryFrame{
		statement: "SELECT * FROM system.local",
		params:    queryParams{consistency: Quorum, keyspace: "gocql_test"},
	}

	// A request-direction frame parses as an error on the response path; for
	// stream -1 that error is tolerated (logged) — all we need here is for its
	// body to be consumed from the segment reader.
	reservedFramer := newFramer(nil, protoVersion5)
	require.NoError(t, req.buildFrame(reservedFramer, -1))
	framer2 := newFramer(nil, protoVersion5)
	require.NoError(t, req.buildFrame(framer2, 2))

	// Written from the test goroutine; see TestConnProcessAllFramesInSingleSegment.
	var buf []byte
	buf = append(buf, reservedFramer.buf...)
	buf = append(buf, framer2.buf...)

	seg, err := newUncompressedSegment(buf, true)
	require.NoError(t, err)
	_, err = client.Write(seg)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), segmentTestTimeout)
	defer cancel()

	errCh := make(chan error, 1)
	go func() { errCh <- c.recvSegment(ctx) }()

	// This is the assertion the test exists for: if the reserved frame's body had
	// been read from the socket instead of the segment payload, the segment reader
	// would be misaligned and this response would never arrive (or arrive corrupt).
	resp2 := recvCallResp(t, call2)
	require.Equal(t, framer2.buf[9:], resp2.framer.buf)

	select {
	case err := <-errCh:
		require.NoError(t, err)
	case <-time.After(segmentTestTimeout):
		t.Fatal("recvSegment did not return; reserved-stream frame body was likely read from the socket, not the segment")
	}
}

// deadlineRecordingConn is a net.Conn whose SetReadDeadline calls are recorded
// so tests can assert the exact sequence of deadline values passed.
//
// Read blocks until proceed is closed, then returns (0, io.EOF). A sync.Once
// guards the close of readEntered so that repeated Read calls (e.g. from
// bufio's internal retry logic) do not panic.
type deadlineRecordingConn struct {
	readEnteredOnce sync.Once
	readEntered     chan struct{} // closed on first Read entry
	proceed         chan struct{} // close to unblock Read

	mu        sync.Mutex
	deadlines []time.Time // all values passed to SetReadDeadline, in order
}

var _ net.Conn = (*deadlineRecordingConn)(nil)

func (c *deadlineRecordingConn) SetReadDeadline(t time.Time) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.deadlines = append(c.deadlines, t)
	return nil
}

func (c *deadlineRecordingConn) Read(p []byte) (int, error) {
	c.readEnteredOnce.Do(func() { close(c.readEntered) })
	<-c.proceed
	return 0, io.EOF
}

func (c *deadlineRecordingConn) Write(p []byte) (int, error)        { return 0, io.ErrClosedPipe }
func (c *deadlineRecordingConn) Close() error                       { return nil }
func (c *deadlineRecordingConn) LocalAddr() net.Addr                { return nil }
func (c *deadlineRecordingConn) RemoteAddr() net.Addr               { return nil }
func (c *deadlineRecordingConn) SetDeadline(t time.Time) error      { return nil }
func (c *deadlineRecordingConn) SetWriteDeadline(t time.Time) error { return nil }

// TestRecvSegmentDisarmsReadDeadlineOnIdleConn verifies that recvSegment
// disarms the read deadline (SetReadDeadline(time.Time{})) on the underlying
// connection while reading the first segment header, and re-arms it afterwards.
//
// The disarm is driven by the connReader.disarm flag rather than by zeroing the
// operational timeout: the configured timeout value is left intact throughout,
// so a concurrent finalizeConnection switching ConnectTimeout -> ReadTimeout is
// never clobbered.
//
// This exercises the fix for the idle proto-v5 connection timeout regression:
// without the disarm, connReader.Read re-arms the deadline on every call, so
// a short ReadTimeout fires on a healthy idle connection and serve() drops it.
func TestRecvSegmentDisarmsReadDeadlineOnIdleConn(t *testing.T) {
	t.Parallel()

	const configuredTimeout = 5 * time.Millisecond

	mock := &deadlineRecordingConn{
		readEntered: make(chan struct{}),
		proceed:     make(chan struct{}),
	}

	cr := &connReader{
		conn: mock,
		r:    bufio.NewReader(mock),
	}
	cr.SetTimeout(configuredTimeout)

	w := &deadlineContextWriter{
		w:         mock,
		semaphore: make(chan struct{}, 1),
		quit:      make(chan struct{}),
	}

	c := &Conn{
		r:       cr,
		calls:   make(map[int]*callReq),
		version: protoVersion5,
		streams: streams.New(),
		w:       w,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	errCh := make(chan error, 1)
	go func() { errCh <- c.recvSegment(ctx) }()

	// Wait until connReader.Read has been entered — at this point the disarm
	// flag is set and SetReadDeadline(zero) has already been called, but the
	// re-arm has not yet happened.
	select {
	case <-mock.readEntered:
	case <-ctx.Done():
		t.Fatal("timed out waiting for recvSegment to enter Read")
	}

	// --- assertions valid while recvSegment is blocked inside Read ---

	// SetReadDeadline must have been called with the zero time to disarm the
	// deadline. connReader.Read clears the deadline whenever the disarm flag is
	// set, so every value seen before the segment read must be the zero time.
	mock.mu.Lock()
	deadlines := append([]time.Time(nil), mock.deadlines...)
	mock.mu.Unlock()

	require.NotEmpty(t, deadlines, "expected at least one SetReadDeadline call before the segment read")
	for i, d := range deadlines {
		require.True(t, d.IsZero(),
			"SetReadDeadline call %d should pass the zero time to disarm the deadline; got %v", i, d)
	}

	// The disarm flag must be set so connReader.Read does not re-arm the
	// deadline while blocking for segment data...
	require.True(t, cr.disarm.Load(),
		"connReader disarm flag should be set while recvSegment blocks on the initial segment read")
	// ...while the operational timeout value is left intact (not zeroed), so a
	// concurrent finalizeConnection cannot be clobbered by a restore.
	require.Equal(t, configuredTimeout, cr.GetTimeout(),
		"connReader timeout should be preserved (not zeroed) while recvSegment blocks on the initial segment read")

	// Unblock Read — mock returns (0, io.EOF), which terminates recvSegment.
	close(mock.proceed)

	select {
	case err := <-errCh:
		// io.EOF is the expected outcome: we fed no bytes so recvSegment has
		// nothing to parse and propagates the EOF from the mock.
		// The critical invariant is that the error is NOT ErrReadHeaderTimeout:
		// io.EOF is a clean close, not a timeout, so serve() must treat it as fatal.
		require.ErrorIs(t, err, io.EOF,
			"recvSegment should propagate io.EOF from a closed read, not misclassify it")
		require.False(t, errors.Is(err, ErrReadHeaderTimeout),
			"io.EOF must not be wrapped as ErrReadHeaderTimeout")
	case <-ctx.Done():
		t.Fatal("timed out waiting for recvSegment to return after Read was unblocked")
	}

	// --- assertions valid after recvSegment has returned ---

	// The disarm flag must be cleared so subsequent body reads are bounded by
	// the operational timeout again.
	require.False(t, cr.disarm.Load(),
		"connReader disarm flag should be cleared after recvSegment returns")
	// The configured timeout is unchanged throughout, regardless of the error path.
	require.Equal(t, configuredTimeout, cr.GetTimeout(),
		"connReader timeout should remain its configured value after recvSegment returns")
}

// nonBlockingDeadlineConn is a net.Conn that records SetReadDeadline calls and
// returns immediately from Read, so a single connReader.Read can be exercised
// synchronously. setDeadlineErr, when set, makes SetReadDeadline fail; reads
// counts the Read calls that actually reached the connection.
type nonBlockingDeadlineConn struct {
	setDeadlineErr error
	deadlines      []time.Time
	reads          int
}

var _ net.Conn = (*nonBlockingDeadlineConn)(nil)

func (c *nonBlockingDeadlineConn) SetReadDeadline(t time.Time) error {
	c.deadlines = append(c.deadlines, t)
	return c.setDeadlineErr
}
func (c *nonBlockingDeadlineConn) Read(p []byte) (int, error)         { c.reads++; return 0, io.EOF }
func (c *nonBlockingDeadlineConn) Write(p []byte) (int, error)        { return 0, io.ErrClosedPipe }
func (c *nonBlockingDeadlineConn) Close() error                       { return nil }
func (c *nonBlockingDeadlineConn) LocalAddr() net.Addr                { return nil }
func (c *nonBlockingDeadlineConn) RemoteAddr() net.Addr               { return nil }
func (c *nonBlockingDeadlineConn) SetDeadline(t time.Time) error      { return nil }
func (c *nonBlockingDeadlineConn) SetWriteDeadline(t time.Time) error { return nil }

// TestConnReaderReadClearsDeadlineWhenTimeoutDisabled verifies that
// connReader.Read arms a read deadline when a positive timeout is configured
// and clears any previously-armed deadline when the timeout is disabled (0).
// Without the clear, a deadline armed during connection setup (e.g.
// ConnectTimeout) would persist and keep expiring on an idle connection.
func TestConnReaderReadClearsDeadlineWhenTimeoutDisabled(t *testing.T) {
	t.Parallel()

	mock := &nonBlockingDeadlineConn{}
	cr := &connReader{conn: mock, r: bufio.NewReader(mock)}

	buf := make([]byte, 1)

	// Positive timeout: Read arms a concrete (non-zero) deadline.
	cr.SetTimeout(5 * time.Second)
	_, _ = cr.Read(buf)
	require.Len(t, mock.deadlines, 1)
	require.False(t, mock.deadlines[0].IsZero(),
		"a positive timeout should arm a non-zero read deadline")

	// Disabled timeout: Read must clear the previously-armed deadline.
	cr.SetTimeout(0)
	_, _ = cr.Read(buf)
	require.Len(t, mock.deadlines, 2)
	require.True(t, mock.deadlines[1].IsZero(),
		"disabling the timeout should clear the read deadline (zero time)")
}

// TestConnReaderReadWithNilConnAndPositiveTimeoutDoesNotPanic verifies
// connReader.Read does not dereference a nil conn when a positive timeout is
// configured. Only test constructions produce a nil conn, but the
// deadline-arming branch must be guarded consistently with the other branches.
func TestConnReaderReadWithNilConnAndPositiveTimeoutDoesNotPanic(t *testing.T) {
	t.Parallel()
	cr := &connReader{r: bufio.NewReader(bytes.NewReader([]byte{0x01}))}
	cr.SetTimeout(5 * time.Second)
	require.NotPanics(t, func() {
		buf := make([]byte, 1)
		_, _ = cr.Read(buf)
	})
}

// TestConnReaderReadReturnsDeadlineError verifies that connReader.Read reports a
// failing SetReadDeadline instead of reading with no deadline (or a stale one),
// matching the write path (deadlineContextWriter.writeContext). All three
// deadline branches are covered: disarmed, positive timeout, disabled timeout.
func TestConnReaderReadReturnsDeadlineError(t *testing.T) {
	t.Parallel()

	setDeadlineErr := errors.New("set read deadline failed")

	for _, tc := range []struct {
		name    string
		timeout time.Duration
		disarm  bool
	}{
		{name: "disarmed", timeout: 5 * time.Second, disarm: true},
		{name: "positive timeout", timeout: 5 * time.Second},
		{name: "timeout disabled", timeout: 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			mock := &nonBlockingDeadlineConn{setDeadlineErr: setDeadlineErr}
			cr := &connReader{conn: mock, r: bufio.NewReader(mock)}
			cr.SetTimeout(tc.timeout)
			cr.setDisarm(tc.disarm)

			n, err := cr.Read(make([]byte, 1))
			require.ErrorIs(t, err, setDeadlineErr)
			require.Zero(t, n)
			require.Zero(t, mock.reads,
				"Read must not touch the connection once arming the deadline failed")
			// Marked as well as reported: the underlying error need not be a
			// net.Error, so "nothing was consumed" has to be recoverable by name for
			// bodyReadDesyncedConn to classify a body read that hits this as fatal.
			require.ErrorIs(t, err, errArmReadDeadline)
			require.True(t, bodyReadDesyncedConn(err),
				"a body read that never reached the socket leaves the whole body queued")
		})
	}
}

// scriptedConn is a net.Conn whose reads come from a script and which records the
// read deadlines it was given, so tests can drive connReader.Read's retry loop.
type scriptedConn struct {
	steps     []scriptedRead
	step      int
	deadlines []time.Time
}

var _ net.Conn = (*scriptedConn)(nil)

func (c *scriptedConn) Read(p []byte) (int, error) {
	if c.step >= len(c.steps) {
		return 0, io.EOF
	}
	step := c.steps[c.step]
	c.step++
	return copy(p, step.data), step.err
}

func (c *scriptedConn) SetReadDeadline(t time.Time) error {
	c.deadlines = append(c.deadlines, t)
	return nil
}
func (c *scriptedConn) Write(p []byte) (int, error)      { return 0, io.ErrClosedPipe }
func (c *scriptedConn) Close() error                     { return nil }
func (c *scriptedConn) LocalAddr() net.Addr              { return nil }
func (c *scriptedConn) RemoteAddr() net.Addr             { return nil }
func (c *scriptedConn) SetDeadline(time.Time) error      { return nil }
func (c *scriptedConn) SetWriteDeadline(time.Time) error { return nil }

// TestConnReaderReadRetriesWhileMakingProgress pins connReader.Read's retry policy.
// ReadTimeout is a per-read budget, so a large body over a slow link can legitimately
// need more than one; but it is also the driver's "identify faulty connections early"
// mechanism (ClusterConfig.ReadTimeout), so a peer that has actually stopped must not
// get maxReadAttempts budgets before the connection is dropped. Progress is what
// separates the two.
func TestConnReaderReadRetriesWhileMakingProgress(t *testing.T) {
	t.Parallel()

	// Every step is one read attempt; a step that carries data and a timeout is a
	// slow-but-progressing peer, a step with only a timeout is a stalled one.
	for _, tc := range []struct {
		name          string
		bufLen        int
		steps         []scriptedRead
		wantN         int
		wantErr       bool
		wantDeadlines int
	}{
		{
			// The fault-detection guarantee: nothing delivered means the peer stopped,
			// so this fails within one ReadTimeout even though data would follow.
			name:          "empty timeout is not resumed",
			bufLen:        4,
			steps:         []scriptedRead{{err: timeoutErr{}}, {data: []byte{1, 2, 3, 4}}},
			wantN:         0,
			wantErr:       true,
			wantDeadlines: 1,
		},
		{
			// The tolerance guarantee: bytes accumulate across attempts and no data is
			// dropped, so a body slower than one ReadTimeout still completes.
			name:   "accumulates across progressing attempts",
			bufLen: 4,
			steps: []scriptedRead{
				{data: []byte{1}, err: timeoutErr{}},
				{data: []byte{2}, err: timeoutErr{}},
				{data: []byte{3, 4}},
			},
			wantN:         4,
			wantDeadlines: 3,
		},
		{
			name:   "stops once progress stops",
			bufLen: 4,
			steps: []scriptedRead{
				{data: []byte{1}, err: timeoutErr{}},
				{err: timeoutErr{}},
				{data: []byte{2, 3, 4}},
			},
			wantN:         1,
			wantErr:       true,
			wantDeadlines: 2,
		},
		{
			// A non-timeout network error is not resumable, whatever Temporary() says
			// about it — the stream position can no longer be trusted.
			name:   "non-timeout net error is not retried",
			bufLen: 4,
			steps: []scriptedRead{
				{data: []byte{1}, err: &net.OpError{Op: "read", Err: errors.New("connection reset by peer")}},
				{data: []byte{2, 3, 4}},
			},
			wantN:         1,
			wantErr:       true,
			wantDeadlines: 1,
		},
		{
			// Bounded: a peer trickling one byte per ReadTimeout forever still fails,
			// after maxReadAttempts.
			name:   "gives up after maxReadAttempts",
			bufLen: 10,
			steps: []scriptedRead{
				{data: []byte{1}, err: timeoutErr{}},
				{data: []byte{2}, err: timeoutErr{}},
				{data: []byte{3}, err: timeoutErr{}},
				{data: []byte{4}, err: timeoutErr{}},
				{data: []byte{5}, err: timeoutErr{}},
				{data: []byte{6}, err: timeoutErr{}},
			},
			wantN:         maxReadAttempts,
			wantErr:       true,
			wantDeadlines: maxReadAttempts,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mock := &scriptedConn{steps: tc.steps}
			cr := &connReader{conn: mock, r: bufio.NewReader(mock)}
			cr.SetTimeout(time.Second)

			n, err := cr.Read(make([]byte, tc.bufLen))

			require.Equal(t, tc.wantN, n)
			if tc.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			require.Len(t, mock.deadlines, tc.wantDeadlines,
				"one fresh read deadline per attempt")
		})
	}
}

// TestConnReadDelegatesToConnReader verifies that the exported (*Conn).Read is
// wired to the connection's connReader, so *Conn keeps satisfying io.Reader for
// external callers after the read-path refactor.
func TestConnReadDelegatesToConnReader(t *testing.T) {
	t.Parallel()

	payload := []byte{0xDE, 0xAD, 0xBE, 0xEF}
	c := &Conn{r: &connReader{r: bufio.NewReader(bytes.NewReader(payload))}}

	buf := make([]byte, len(payload))
	n, err := c.Read(buf)
	require.NoError(t, err)
	require.Equal(t, len(payload), n)
	require.Equal(t, payload, buf)
}

// scriptedReadSource is a connReadSource whose reads come from a script of
// (bytes, error) steps and which records every setDisarm transition. It lets tests
// drive partial reads, timing-out reads and the disarm/re-arm pairing without a
// socket, and — being a connReadSource — it can be installed as Conn.r.
//
// A step means "the peer delivered these bytes, and then this error", not "one
// Read call": a step's bytes are handed out across as many reads as the caller
// takes to consume them, and the error surfaces on the read that empties it. So a
// script stays meaningful regardless of how the code under test sizes its reads —
// which matters because the header readers deliberately read the first byte on its
// own (headerReader).
type scriptedReadSource struct {
	steps    []scriptedRead
	step     int
	consumed int    // bytes of steps[step].data already delivered
	disarms  []bool // every value passed to setDisarm, in order
	arms     int    // reads started while not disarmed
	readLens []int  // len(p) of every read, in order
}

type scriptedRead struct {
	data []byte
	err  error
}

var _ connReadSource = (*scriptedReadSource)(nil)

func (s *scriptedReadSource) Read(p []byte) (int, error) {
	if len(s.disarms) == 0 || !s.disarms[len(s.disarms)-1] {
		s.arms++
	}
	s.readLens = append(s.readLens, len(p))
	if s.step >= len(s.steps) {
		return 0, io.EOF
	}
	step := s.steps[s.step]
	n := copy(p, step.data[s.consumed:])
	s.consumed += n
	if s.consumed < len(step.data) {
		// More of this step's bytes to come; its error belongs to the read that
		// delivers the last of them.
		return n, nil
	}
	s.step++
	s.consumed = 0
	return n, step.err
}

func (s *scriptedReadSource) Close() error               { return nil }
func (s *scriptedReadSource) RemoteAddr() net.Addr       { return nil }
func (s *scriptedReadSource) SetTimeout(_ time.Duration) {}
func (s *scriptedReadSource) GetTimeout() time.Duration  { return 0 }
func (s *scriptedReadSource) setDisarm(v bool)           { s.disarms = append(s.disarms, v) }

// timeoutErr is a net.Error reporting a timeout, standing in for the
// os.ErrDeadlineExceeded a real socket returns once its read deadline expires.
type timeoutErr struct{}

func (timeoutErr) Error() string   { return "i/o timeout" }
func (timeoutErr) Timeout() bool   { return true }
func (timeoutErr) Temporary() bool { return true }

var _ net.Error = timeoutErr{}

// TestHeaderReadTimeoutIsBenignOnlyWhenNothingWasConsumed pins the line between
// the two kinds of header-read timeout, for both header readers.
//
// Consumed nothing: the peer was simply idle, serve() logs ErrReadHeaderTimeout and
// reads again. Consumed part of a header: the stream is now at an unknown offset, so
// the error must NOT be normalised — serve() has to close the connection instead of
// resuming and mis-framing every frame that follows.
func TestHeaderReadTimeoutIsBenignOnlyWhenNothingWasConsumed(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name      string
		steps     []scriptedRead
		wantIsErr bool
	}{
		{
			name:      "nothing consumed",
			steps:     []scriptedRead{{err: timeoutErr{}}},
			wantIsErr: true,
		},
		{
			name:      "partial header consumed",
			steps:     []scriptedRead{{data: []byte{0x84, 0x00, 0x00, 0x01}, err: timeoutErr{}}},
			wantIsErr: false,
		},
	} {
		t.Run("frame header/"+tc.name, func(t *testing.T) {
			c := &Conn{r: &scriptedReadSource{steps: tc.steps}}

			_, err := c.readFrameHeader(c.r)

			require.Error(t, err)
			require.Equal(t, tc.wantIsErr, errors.Is(err, ErrReadHeaderTimeout),
				"errors.Is(ErrReadHeaderTimeout) on %q", err)
		})

		t.Run("segment header/"+tc.name, func(t *testing.T) {
			c := &Conn{r: &scriptedReadSource{steps: tc.steps}}

			_, err := c.readFirstSegmentHeader()

			require.Error(t, err)
			require.Equal(t, tc.wantIsErr, errors.Is(err, ErrReadHeaderTimeout),
				"errors.Is(ErrReadHeaderTimeout) on %q", err)
		})
	}
}

// TestProcessFrameDesyncedBodyIsFatalAndWakesCaller pins the two halves of what
// has to happen when a frame body cannot be read off the wire:
//
//   - processFrame returns the error, so serve() closes the connection. The
//     unread remainder of the body is still queued on the socket, so reusing the
//     connection would read it as the next frame header and mis-frame everything
//     after it.
//   - the waiting caller is handed the error first. head.Stream has already been
//     removed from c.calls by this point, so closeWithError can no longer find the
//     call: returning without delivering would leave exec() waiting out its full
//     request timeout for a response that can never arrive.
func TestProcessFrameDesyncedBodyIsFatalAndWakesCaller(t *testing.T) {
	t.Parallel()

	header := []byte{
		protoVersion4 | protoDirectionMask, 0x00, 0x00, 0x01, byte(frm.OpResult),
		0x00, 0x00, 0x00, 0x08, // body length 8
	}

	// The peer sends a complete header and then stalls partway through the body.
	r := &scriptedReadSource{steps: []scriptedRead{
		{data: header},
		{data: []byte{0x01, 0x02, 0x03}, err: timeoutErr{}},
	}}

	// Buffered so the delivery does not need a second goroutine.
	call := &callReq{timeout: make(chan struct{}), streamID: 1, resp: make(chan callResp, 1)}
	c := &Conn{
		r:       r,
		calls:   map[int]*callReq{1: call},
		version: protoVersion4,
		streams: streams.New(),
		logger:  nopLogger{},
	}

	err := c.processFrame(context.Background(), c.r)

	require.Error(t, err, "a half-read body desyncs the connection and must be fatal")
	var netErr net.Error
	require.ErrorAs(t, err, &netErr)

	select {
	case resp := <-call.resp:
		require.ErrorIs(t, resp.err, err, "the waiting caller must be handed the same failure")
	default:
		t.Fatal("the call was never woken: it would wait out its full request timeout")
	}
}

// armFailingConn serves a fixed byte stream and fails SetReadDeadline from
// failFromArm onwards, with a plain error that is deliberately not a net.Error —
// net.Conn.SetReadDeadline is only conventionally a *net.OpError, and a connection
// from a user-supplied Dialer or HostDialer need not follow that.
type armFailingConn struct {
	data        []byte
	off         int
	failFromArm int
	err         error
	arms        int
}

var _ net.Conn = (*armFailingConn)(nil)

func (c *armFailingConn) SetReadDeadline(time.Time) error {
	c.arms++
	if c.arms >= c.failFromArm {
		return c.err
	}
	return nil
}

func (c *armFailingConn) Read(p []byte) (int, error) {
	if c.off >= len(c.data) {
		return 0, io.EOF
	}
	n := copy(p, c.data[c.off:])
	c.off += n
	return n, nil
}

func (c *armFailingConn) Write(p []byte) (int, error)      { return 0, io.ErrClosedPipe }
func (c *armFailingConn) Close() error                     { return nil }
func (c *armFailingConn) LocalAddr() net.Addr              { return nil }
func (c *armFailingConn) RemoteAddr() net.Addr             { return nil }
func (c *armFailingConn) SetDeadline(time.Time) error      { return nil }
func (c *armFailingConn) SetWriteDeadline(time.Time) error { return nil }

// A calls map that disagrees with the callReq it holds is a driver bug, not
// something a peer can provoke. It used to panic anyway -- on the serve goroutine,
// outside parseFrame's recover, and with a string value that recover could not have
// converted even in scope -- so a bookkeeping slip killed the process. Now it fails
// the connection whose stream accounting is in doubt, and nothing else.
func TestProcessFrameStreamIDMismatchIsAnError(t *testing.T) {
	t.Parallel()

	header := []byte{
		protoVersion4 | protoDirectionMask, 0x00, 0x00, 0x01, byte(frm.OpResult),
		0x00, 0x00, 0x00, 0x00, // no body
	}

	// Keyed under stream 1, but the call believes it is stream 2. resp is buffered so
	// the delivery does not need a second goroutine.
	call := &callReq{timeout: make(chan struct{}), streamID: 2, resp: make(chan callResp, 1)}
	c := &Conn{
		calls:   map[int]*callReq{1: call},
		version: protoVersion4,
		streams: streams.New(),
		logger:  nopLogger{},
	}

	err := c.processFrameSource(context.Background(), frameSource{r: bytes.NewReader(header)})
	require.Error(t, err)
	require.Contains(t, err.Error(), "stream 1")
	require.Contains(t, err.Error(), "stream 2")

	// And the caller is handed it. head.Stream has already left c.calls, so
	// closeWithError's drain cannot reach this call: returning without delivering
	// would wake it only on c.ctx.Done(), with a generic ErrConnectionClosed, and
	// would skip the stream observer and putCallReq altogether.
	select {
	case resp := <-call.resp:
		require.ErrorIs(t, resp.err, err)
	default:
		t.Fatal("the call was never woken: it would wait out its full request timeout")
	}
}

// TestProcessFrameFailedDeadlineArmIsFatal pins that a body read which never
// reached the socket is fatal to the connection, and that widening the rule that
// far did not make every read failure fatal.
//
// A failed read-deadline arm is the worst case of a desynced body: connReader.Read
// returns before touching the socket, so the entire body is still queued on a
// connection that is otherwise healthy, and serve() reads it as the next frame
// header. Nothing else surfaces it. Classifying it needs the error to say so —
// SetReadDeadline's error need not be a net.Error, so the net.Error test alone let
// this through as a per-request failure and kept the connection in the pool.
//
// The second case is the boundary: a short read out of a buffer that has already
// arrived yields io.ErrUnexpectedEOF, which is not a net.Error either, and must
// stay per-request. The stream is not desynced — there is no socket behind it — so
// the connection survives and only the one request fails.
func TestProcessFrameFailedDeadlineArmIsFatal(t *testing.T) {
	t.Parallel()

	header := []byte{
		protoVersion4 | protoDirectionMask, 0x00, 0x00, 0x01, byte(frm.OpResult),
		0x00, 0x00, 0x00, 0x08, // body length 8
	}
	armErr := errors.New("set read deadline failed")

	for _, tc := range []struct {
		name      string
		source    func() (frameSource, func())
		wantFatal bool
	}{
		{
			name: "failed deadline arm",
			source: func() (frameSource, func()) {
				// Two arms read the header — one for its first byte with the deadline
				// disarmed, one for the rest (headerReader) — so the third is the body's.
				// If that arithmetic ever changes, the header read fails instead and the
				// "caller was woken" assertion below catches it rather than the test
				// passing for the wrong reason.
				mock := &armFailingConn{data: header, failFromArm: 3, err: armErr}
				cr := &connReader{conn: mock, r: bufio.NewReader(mock)}
				cr.SetTimeout(5 * time.Second)
				return frameSource{r: cr}, func() {
					require.Equal(t, len(header), mock.off,
						"the whole header must have been read before the body arm failed")
				}
			},
			wantFatal: true,
		},
		{
			name: "short read from an arrived buffer",
			source: func() (frameSource, func()) {
				// A header promising 8 body bytes over a buffer holding 3.
				buf := append(append([]byte(nil), header...), 0x01, 0x02, 0x03)
				return frameSource{r: bytes.NewReader(buf)}, func() {}
			},
			wantFatal: false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			src, checkSource := tc.source()

			// Buffered so the delivery does not need a second goroutine.
			call := &callReq{timeout: make(chan struct{}), streamID: 1, resp: make(chan callResp, 1)}
			c := &Conn{
				calls:   map[int]*callReq{1: call},
				version: protoVersion4,
				streams: streams.New(),
				logger:  nopLogger{},
			}

			err := c.processFrameSource(context.Background(), src)
			checkSource()

			// The caller is always woken, fatal or not: head.Stream has already been
			// removed from c.calls, so closeWithError can no longer find the call.
			var delivered error
			select {
			case resp := <-call.resp:
				delivered = resp.err
			default:
				t.Fatal("the call was never woken: it would wait out its full request timeout")
			}
			require.Error(t, delivered)

			if tc.wantFatal {
				require.ErrorIs(t, err, errArmReadDeadline,
					"an unread body on a live connection must close it")
				require.ErrorIs(t, err, armErr, "the underlying failure must stay inspectable")
				require.ErrorIs(t, delivered, err, "the waiting caller must be handed the same failure")
				return
			}

			require.NoError(t, err,
				"a short read out of an arrived buffer leaves no unread bytes on a socket, so the connection survives")
			require.ErrorIs(t, delivered, io.ErrUnexpectedEOF)
		})
	}
}

// TestReadFirstSegmentHeaderDisarmsAndRearms pins that the idle wait for a segment
// header runs with the read deadline disarmed and that the deadline is restored on
// every exit path — including the error path, where a missed re-arm would leave the
// connection reading with no deadline for the rest of its life.
func TestReadFirstSegmentHeaderDisarmsAndRearms(t *testing.T) {
	t.Parallel()

	header := mustUncompressedSegment(t, []byte("hello"), true)

	for _, tc := range []struct {
		name  string
		steps []scriptedRead
	}{
		{"header read succeeds", []scriptedRead{{data: header}}},
		{"header read fails", []scriptedRead{{err: timeoutErr{}}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := &scriptedReadSource{steps: tc.steps}
			c := &Conn{r: r}

			_, _ = c.readFirstSegmentHeader()

			// Asserted as a shape rather than an exact slice: the re-arm can come from
			// headerReader (as soon as a byte arrives) as well as from the deferred
			// clear, so a successful read legitimately clears twice. What must hold is
			// that the read starts disarmed, ends re-armed, and is never re-disarmed —
			// the last of those is what a redundant deferred clear must stay harmless
			// for.
			require.NotEmpty(t, r.disarms, "the header read must disarm the deadline")
			require.True(t, r.disarms[0], "the idle wait must start with the deadline disarmed")
			require.False(t, r.disarms[len(r.disarms)-1],
				"the deadline must be re-armed before readFirstSegmentHeader returns")
			for i, d := range r.disarms[1:] {
				require.False(t, d, "setDisarm(true) at index %d: the deadline must never be re-disarmed", i+1)
			}
		})
	}
}

// TestHeaderReadDisarmIsBoundedToTheFirstByte pins that only the idle wait for a
// header's first byte runs without a read deadline.
//
// The disarm is per-read (connReader.armDeadline runs once per read attempt), so a
// disarm held across the whole header read leaves every byte of it unbounded: a peer
// that sends one byte and then stops holds the serve goroutine for as long as it
// keeps the socket open, and ReadTimeout never drops the connection. That cannot be
// asserted by waiting — the point of the bug is that nothing ever returns — so it is
// pinned on the mechanism instead: the first read asks for a single byte, and the
// deadline is re-armed before the second read starts.
func TestHeaderReadDisarmIsBoundedToTheFirstByte(t *testing.T) {
	t.Parallel()

	// A peer that delivers one byte of the header and then stalls until the re-armed
	// deadline expires.
	steps := []scriptedRead{{data: []byte{0x84}}, {err: timeoutErr{}}}

	for _, tc := range []struct {
		name string
		read func(*Conn) error
	}{
		{
			name: "frame header",
			read: func(c *Conn) error { _, err := c.readFrameHeader(c.r); return err },
		},
		{
			name: "segment header",
			read: func(c *Conn) error { _, err := c.readFirstSegmentHeader(); return err },
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			r := &scriptedReadSource{steps: steps}
			c := &Conn{r: r}

			err := tc.read(c)

			require.Error(t, err)
			// Not normalised: a timeout that already consumed part of a header leaves
			// the stream at an unknown offset, so serve() must close the connection
			// rather than resume and mis-frame everything after it.
			require.False(t, errors.Is(err, ErrReadHeaderTimeout),
				"a timeout partway through a header must stay fatal, got %q", err)

			require.GreaterOrEqual(t, len(r.readLens), 2,
				"the header must be read in more than one read for the deadline to be re-armed in between")
			require.Equal(t, 1, r.readLens[0],
				"the unbounded read must ask for a single byte, so only the idle wait is unbounded")
			require.Equal(t, 1, r.arms,
				"exactly one read must start with the deadline re-armed: the one after the first byte arrived")
		})
	}
}

// TestRecvSegmentReassemblesFrameWithSplitHeader verifies that recvSegment
// correctly reassembles a single CQL frame that is split across multiple
// non-self-contained segments even when the 9-byte CQL frame header itself
// spans two segments. The first segment intentionally carries only 4 payload
// bytes, so the header must be accumulated across both segments before it can
// be parsed.
func TestRecvSegmentReassemblesFrameWithSplitHeader(t *testing.T) {
	server, client, err := tcpConnPair()
	require.NoError(t, err)
	defer server.Close()
	defer client.Close()

	w := &deadlineContextWriter{
		w:         server,
		semaphore: make(chan struct{}, 1),
		quit:      make(chan struct{}),
	}
	w.timeout.Store(int64(time.Second * 10))

	c := &Conn{
		r: &connReader{
			conn: server,
			r:    bufio.NewReader(server),
		},
		calls:      make(map[int]*callReq),
		version:    protoVersion5,
		addr:       server.RemoteAddr().String(),
		streams:    streams.New(),
		isSchemaV2: true,
		w:          w,
	}
	c.writeTimeout.Store(int64(time.Second * 10))

	call1 := &callReq{timeout: make(chan struct{}), streamID: 1, resp: make(chan callResp)}
	c.calls[1] = call1

	req := writeQueryFrame{
		statement: "SELECT * FROM system.local",
		params:    queryParams{consistency: Quorum, keyspace: "gocql_test"},
	}
	framer := newFramer(nil, protoVersion5)
	require.NoError(t, req.buildFrame(framer, 1))
	full := append([]byte(nil), framer.buf...)

	// Split the frame into two non-self-contained segments. The first carries
	// only 4 bytes, fewer than the 9-byte CQL frame header, so the header is
	// split across both segments.
	const splitAt = 4
	seg1, err := newUncompressedSegment(full[:splitAt], false)
	require.NoError(t, err)
	seg2, err := newUncompressedSegment(full[splitAt:], false)
	require.NoError(t, err)

	// The helper goroutines only ferry results out; every assertion runs on the
	// test goroutine. A require failing inside a helper exits that goroutine via
	// runtime.Goexit, which here would starve recvSegment of its response
	// delivery and park the test until the suite timeout — and an assertion in
	// the response goroutine is not synchronised with test completion, so it
	// could be skipped outright when the test finishes first.
	writeErr := make(chan error, 1)
	go func() {
		buf := append(append([]byte(nil), seg1...), seg2...)
		_, err := client.Write(buf)
		writeErr <- err
	}()

	// A generous margin rather than something tight, so this does not flake
	// under -race on a loaded CI runner (see TestRecvSegmentPayloadReadIsBounded),
	// while still failing this test rather than the whole suite on a hang.
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	errCh := make(chan error, 1)
	go func() { errCh <- c.recvSegment(ctx) }()

	respBuf := make(chan []byte, 1)
	go func() {
		resp := <-call1.resp
		close(call1.timeout)
		// resp.framer.buf holds the frame body (the header is already parsed).
		respBuf <- resp.framer.buf
	}()

	select {
	case <-ctx.Done():
		t.Fatal("timed out waiting for reassembled frame")
	case err := <-errCh:
		require.NoError(t, err)
	}

	// Delivery to call1.resp happens-before recvSegment returns, so neither
	// receive can block for long once errCh has fired.
	require.NoError(t, <-writeErr, "writing the segmented stream")
	require.Equal(t, full[9:], <-respBuf)
}

// TestRecvSegmentPayloadReadIsBounded verifies that after the (idle-disarmed)
// segment header is read, the read deadline is re-armed for the payload read,
// so a peer that sends a segment header and then stalls does not hang the
// receive loop indefinitely.
func TestRecvSegmentPayloadReadIsBounded(t *testing.T) {
	t.Parallel()

	server, client, err := tcpConnPair()
	require.NoError(t, err)
	defer server.Close()
	defer client.Close()

	cr := &connReader{conn: server, r: bufio.NewReader(server)}
	// Use a generous timeout/watchdog margin (rather than something tighter
	// like 100ms/2s) so this doesn't flake under -race on a loaded CI runner,
	// where scheduling jitter alone can exceed a couple hundred milliseconds.
	const readTimeout = 500 * time.Millisecond
	cr.SetTimeout(readTimeout)

	c := &Conn{
		r:       cr,
		calls:   make(map[int]*callReq),
		version: protoVersion5,
		streams: streams.New(),
	}

	// Build a valid self-contained segment but send ONLY its 6-byte header: the
	// header advertises a payload the peer never delivers (a mid-transfer stall).
	payload := make([]byte, 128)
	seg, err := newUncompressedSegment(payload, true)
	require.NoError(t, err)
	const uncompressedSegmentHeaderSize = 6

	go func() {
		_, _ = client.Write(seg[:uncompressedSegmentHeaderSize])
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	errCh := make(chan error, 1)
	go func() { errCh <- c.recvSegment(ctx) }()

	select {
	case err := <-errCh:
		require.Error(t, err, "recvSegment should fail when the advertised payload never arrives")
		require.False(t, errors.Is(err, ErrReadHeaderTimeout),
			"a mid-transfer payload stall must not be normalised to the benign idle header timeout")
	case <-time.After(5 * time.Second):
		t.Fatal("recvSegment hung on the payload read; the read deadline was not re-armed after the segment header")
	}
}

// TestStartupOptionsKeepDriverKeys pins that ApplicationInfo cannot overwrite the
// STARTUP options the driver owns.
//
// CQL_VERSION, DRIVER_NAME and DRIVER_VERSION describe the driver and the
// protocol it speaks. A callback that set CQL_VERSION could make every connection
// in the cluster fail its handshake; one that set DRIVER_NAME or DRIVER_VERSION
// would misreport the driver to the server for the life of the connection.
// SESSION_ID is driver-owned for the same reason: it is what correlates a
// session's connections in system.clients.
func TestStartupOptionsKeepDriverKeys(t *testing.T) {
	t.Parallel()

	const (
		cqlVersion    = "3.4.5"
		driverName    = "gocql"
		driverVersion = "1.2.3"
		sessionID     = "91b0b1a2-0000-4000-8000-000000000001"
	)

	t.Run("no ApplicationInfo", func(t *testing.T) {
		m := startupOptions(cqlVersion, driverName, driverVersion, nil, nil, sessionID, false)

		require.Equal(t, map[string]string{
			"CQL_VERSION":       cqlVersion,
			"DRIVER_NAME":       driverName,
			"DRIVER_VERSION":    driverVersion,
			sessionIDStartupKey: sessionID,
		}, m)
	})

	t.Run("application options are kept", func(t *testing.T) {
		m := startupOptions(cqlVersion, driverName, driverVersion,
			NewStaticApplicationInfo("app", "9.9.9", "client-id"), nil, sessionID, false)

		require.Equal(t, "app", m["APPLICATION_NAME"])
		require.Equal(t, "9.9.9", m["APPLICATION_VERSION"])
		require.Equal(t, "client-id", m["CLIENT_ID"])
		require.Equal(t, cqlVersion, m["CQL_VERSION"])
		require.Equal(t, driverName, m["DRIVER_NAME"])
		require.Equal(t, driverVersion, m["DRIVER_VERSION"])
		require.Equal(t, sessionID, m[sessionIDStartupKey])
	})

	t.Run("driver-owned keys win", func(t *testing.T) {
		m := startupOptions(cqlVersion, driverName, driverVersion,
			applicationInfoFunc(func(opts map[string]string) {
				opts["CQL_VERSION"] = "9.9.9"
				opts["DRIVER_NAME"] = "not-gocql"
				opts["DRIVER_VERSION"] = "0.0.0"
				opts[sessionIDStartupKey] = "not-the-session-id"
				opts["APPLICATION_NAME"] = "app"
			}), nil, sessionID, false)

		require.Equal(t, cqlVersion, m["CQL_VERSION"], "a custom CQL version would fail every handshake")
		require.Equal(t, driverName, m["DRIVER_NAME"])
		require.Equal(t, driverVersion, m["DRIVER_VERSION"])
		require.Equal(t, sessionID, m[sessionIDStartupKey], "a custom session id would break correlating a session's connections")
		require.Equal(t, "app", m["APPLICATION_NAME"], "keys the driver does not own must still come through")
	})
}

// applicationInfoFunc adapts a function to ApplicationInfo, so a test can supply
// options StaticApplicationInfo would never produce.
type applicationInfoFunc func(map[string]string)

func (f applicationInfoFunc) UpdateStartupOptions(opts map[string]string) { f(opts) }

// TestSystemRequestTimeoutRaceWithFinalizeConnection pins the state
// driver-issued system queries read (Conn.systemRequest, holding the client-side
// timeout and the pre-rendered USING TIMEOUT clause) as safe to read while
// finalizeConnection switches it from ConnectTimeout to
// MetadataSchemaRequestTimeout.
//
// The two are concurrent in practice: the control connection is registered for
// server push events during controlConn.setupConn, but its
// finalizeConnection() is deferred to the very end of Session.init. A keyspace
// SCHEMA_CHANGE delivered inside that window (a freshly bootstrapped cluster
// still creating its internal keyspaces is enough) makes the event-debouncer
// goroutine run handleKeyspaceChange -> awaitSchemaAgreement -> querySystem on
// the connection the main goroutine is mid-write on.
//
// Run under -race: were the two values plain fields again, this would report
// "DATA RACE" on them — a torn string header for the clause and a stale timeout.
func TestSystemRequestTimeoutRaceWithFinalizeConnection(t *testing.T) {
	t.Parallel()

	// finalizeConnection only stores into atomics - Conn.writeTimeout,
	// Conn.systemRequest, and the timeout field of each of these two - so neither
	// needs a net.Conn behind it.
	c := &Conn{
		r: &connReader{},
		w: &deadlineContextWriter{},
		cfg: &ConnConfig{
			ConnectTimeout: 10 * time.Second,
			WriteTimeout:   2 * time.Second,
			ReadTimeout:    3 * time.Second,
		},
		session: &Session{
			cfg: ClusterConfig{MetadataSchemaRequestTimeout: 42 * time.Millisecond},
		},
		// Only a ScyllaDB connection renders the USING TIMEOUT clause.
		scyllaSupported: ScyllaConnectionFeatures{
			ScyllaHostFeatures: ScyllaHostFeatures{isScylla: true},
		},
	}
	c.setSystemRequestTimeout(c.cfg.ConnectTimeout)

	// The only two snapshots a reader may observe: the one the connection is
	// dialed with, and the one finalizeConnection replaces it with. Spelling both
	// out keeps this test from re-deriving the rendering rule, which is
	// TestSystemRequestStateClauseFollowsTimeout's job.
	var (
		beforeFinalize = systemRequestState{
			timeout:     c.cfg.ConnectTimeout,
			usingClause: " USING TIMEOUT 10000ms",
		}
		afterFinalize = systemRequestState{
			timeout:     c.session.cfg.MetadataSchemaRequestTimeout,
			usingClause: " USING TIMEOUT 42ms",
		}
	)

	const readers = 4
	done := make(chan struct{})
	var wg, spinning sync.WaitGroup
	wg.Add(readers)
	spinning.Add(readers)
	for i := 0; i < readers; i++ {
		go func() {
			defer wg.Done()
			first := true
			for {
				select {
				case <-done:
					return
				default:
				}
				// The single load querySystem performs.
				state := c.getSystemRequestState()
				if first {
					// Only write once every reader is already spinning, so the
					// two sides genuinely overlap.
					spinning.Done()
					first = false
				}
				// Both values must come from the same snapshot: the clause the
				// server is asked to honour always matches the deadline the
				// client is waiting on, never the other transition's value.
				if state != beforeFinalize && state != afterFinalize {
					t.Errorf("observed %+v, which is neither transition's snapshot; want %+v or %+v",
						state, beforeFinalize, afterFinalize)
					return
				}
			}
		}()
	}

	spinning.Wait()
	c.finalizeConnection()
	close(done)
	wg.Wait()

	require.Equal(t, afterFinalize, c.getSystemRequestState(),
		"finalizeConnection should publish the metadata timeout and its clause as one snapshot")
}

// TestSystemRequestStateClauseFollowsTimeout pins the USING TIMEOUT clause as
// derived purely from the timeout in effect and whether the peer is ScyllaDB.
//
// The zero case is the one worth guarding: MetadataSchemaRequestTimeout is
// documented as "positive or zero" (see ClusterConfig.Validate), and by the time
// finalizeConnection applies it, startupCoordinator.options has already rendered a
// clause from ConnectTimeout. Were that clause carried over, a caller who disabled
// the metadata timeout would still have every system query bounded server-side by
// an unrelated setting - while DRIVER_CONFIG reported no timeout at all, since
// buildControlPlaneReport omits a non-positive one.
func TestSystemRequestStateClauseFollowsTimeout(t *testing.T) {
	t.Parallel()

	newConn := func(isScylla bool) *Conn {
		c := &Conn{scyllaSupported: ScyllaConnectionFeatures{
			ScyllaHostFeatures: ScyllaHostFeatures{isScylla: isScylla},
		}}
		// As startupCoordinator.options leaves it: a clause rendered from
		// ConnectTimeout, before finalizeConnection applies the metadata timeout.
		c.setSystemRequestTimeout(600 * time.Millisecond)
		return c
	}

	t.Run("scylla renders the clause", func(t *testing.T) {
		require.Equal(t, systemRequestState{
			timeout:     600 * time.Millisecond,
			usingClause: " USING TIMEOUT 600ms",
		}, newConn(true).getSystemRequestState())
	})

	t.Run("cassandra renders no clause", func(t *testing.T) {
		require.Equal(t, systemRequestState{timeout: 600 * time.Millisecond},
			newConn(false).getSystemRequestState())
	})

	t.Run("a disabled timeout clears the clause", func(t *testing.T) {
		c := newConn(true)
		c.setSystemRequestTimeout(0)
		require.Equal(t, systemRequestState{}, c.getSystemRequestState(),
			"a zero metadata timeout must not leave the ConnectTimeout clause in force")
	})

	// The cases above assert the snapshot itself; this one goes through
	// systemRequestStatement, which is how the query paths reach it.
	t.Run("the statement carries the clause in force", func(t *testing.T) {
		c := newConn(true)
		stmt, timeout := c.systemRequestStatement(qrySystemLocal)
		require.Equal(t, qrySystemLocal+" USING TIMEOUT 600ms", stmt)
		require.Equal(t, 600*time.Millisecond, timeout)

		c.setSystemRequestTimeout(0)
		stmt, timeout = c.systemRequestStatement(qrySystemLocal)
		require.Equal(t, qrySystemLocal, stmt, "a disabled timeout appends nothing")
		require.Equal(t, time.Duration(0), timeout)
	})
}
