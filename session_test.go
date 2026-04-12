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
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"
)

func TestSessionAPI(t *testing.T) {
	t.Parallel()

	cfg := &ClusterConfig{}

	s := &Session{
		cfg:    *cfg,
		cons:   Quorum,
		policy: RoundRobinHostPolicy(),
		logger: cfg.logger(),
	}

	s.pool = cfg.PoolConfig.buildPool(s)
	s.executor = &queryExecutor{
		pool:   s.pool,
		policy: s.policy,
	}
	defer s.Close()

	s.SetConsistency(All)
	if s.cons != All {
		t.Fatalf("expected consistency 'All', got '%v'", s.cons)
	}

	s.SetPageSize(100)
	if s.pageSize != 100 {
		t.Fatalf("expected pageSize 100, got %v", s.pageSize)
	}

	s.SetPrefetch(0.75)
	if s.prefetch != 0.75 {
		t.Fatalf("expceted prefetch 0.75, got %v", s.prefetch)
	}

	trace := NewTracer(nil)

	s.SetTrace(trace)
	if s.trace != trace {
		t.Fatalf("expected tracer '%v',got '%v'", trace, s.trace)
	}

	qry := s.Query("test", 1)
	if v, ok := qry.values[0].(int); !ok {
		t.Fatalf("expected qry.values[0] to be an int, got %v", qry.values[0])
	} else if v != 1 {
		t.Fatalf("expceted qry.values[0] to be 1, got %v", v)
	} else if qry.stmt != "test" {
		t.Fatalf("expected qry.stmt to be 'test', got '%v'", qry.stmt)
	}

	boundQry := s.Bind("test", func(q *QueryInfo) ([]any, error) {
		return nil, nil
	})
	if boundQry.binding == nil {
		t.Fatal("expected qry.binding to be defined, got nil")
	} else if boundQry.stmt != "test" {
		t.Fatalf("expected qry.stmt to be 'test', got '%v'", boundQry.stmt)
	}

	itr := s.executeQuery(qry)
	if itr.err != ErrSessionNotReady {
		t.Fatalf("expected itr.err to be '%v', got '%v'", ErrSessionNotReady, itr.err)
	}

	testBatch := s.Batch(LoggedBatch)
	testBatch.Query("test")
	err := s.ExecuteBatch(testBatch)

	if err != ErrSessionNotReady {
		t.Fatalf("expected session.ExecuteBatch to return '%v', got '%v'", ErrSessionNotReady, err)
	}

	s.Close()
	if !s.Closed() {
		t.Fatal("expected s.Closed() to be true, got false")
	}
	//Should just return cleanly
	s.Close()

	err = s.ExecuteBatch(testBatch)
	if err != ErrSessionClosed {
		t.Fatalf("expected session.ExecuteBatch to return '%v', got '%v'", ErrSessionClosed, err)
	}
}

type funcQueryObserver func(context.Context, ObservedQuery)

func (f funcQueryObserver) ObserveQuery(ctx context.Context, o ObservedQuery) {
	f(ctx, o)
}

func TestQueryBasicAPI(t *testing.T) {
	t.Parallel()

	qry := &Query{routingInfo: &queryRoutingInfo{}}

	// Initiate host
	ip := "127.0.0.1"

	qry.metrics = preFilledQueryMetrics(map[string]*hostMetrics{ip: {Attempts: 0, TotalLatency: 0}})
	if qry.Latency() != 0 {
		t.Fatalf("expected Query.Latency() to return 0, got %v", qry.Latency())
	}

	qry.metrics = preFilledQueryMetrics(map[string]*hostMetrics{ip: {Attempts: 2, TotalLatency: 4}})
	if qry.Attempts() != 2 {
		t.Fatalf("expected Query.Attempts() to return 2, got %v", qry.Attempts())
	}
	if qry.Latency() != 2 {
		t.Fatalf("expected Query.Latency() to return 2, got %v", qry.Latency())
	}

	qry.AddAttempts(2, &HostInfo{hostname: ip, connectAddress: net.ParseIP(ip), port: 9042})
	if qry.Attempts() != 4 {
		t.Fatalf("expected Query.Attempts() to return 4, got %v", qry.Attempts())
	}

	qry.Consistency(All)
	if qry.GetConsistency() != All {
		t.Fatalf("expected Query.GetConsistency to return 'All', got '%s'", qry.GetConsistency())
	}

	qry.Consistency(LocalSerial)
	if qry.GetConsistency() != LocalSerial {
		t.Fatalf("expected Query.GetConsistency to return 'LocalSerial', got '%s'", qry.GetConsistency())
	}

	qry.SerialConsistency(LocalSerial)
	if qry.GetConsistency() != LocalSerial {
		t.Fatalf("expected Query.GetConsistency to return 'LocalSerial', got '%s'", qry.GetConsistency())
	}

	trace := NewTracer(nil)
	qry.Trace(trace)
	if qry.trace != trace {
		t.Fatalf("expected Query.Trace to be '%v', got '%v'", trace, qry.trace)
	}

	observer := funcQueryObserver(func(context.Context, ObservedQuery) {})
	qry.Observer(observer)
	if qry.observer == nil { // can't compare func to func, checking not nil instead
		t.Fatal("expected Query.QueryObserver to be set, got nil")
	}

	qry.PageSize(10)
	if qry.pageSize != 10 {
		t.Fatalf("expected Query.PageSize to be 10, got %v", qry.pageSize)
	}

	qry.Prefetch(0.75)
	if qry.prefetch != 0.75 {
		t.Fatalf("expected Query.Prefetch to be 0.75, got %v", qry.prefetch)
	}

	rt := &SimpleRetryPolicy{NumRetries: 3}
	if qry.RetryPolicy(rt); qry.rt != rt {
		t.Fatalf("expected Query.RetryPolicy to be '%v', got '%v'", rt, qry.rt)
	}

	qry.Bind(qry)
	if qry.values[0] != qry {
		t.Fatalf("expected Query.Values[0] to be '%v', got '%v'", qry, qry.values[0])
	}
}

func TestQueryShouldPrepare(t *testing.T) {
	t.Parallel()

	toPrepare := []string{"select * ", "INSERT INTO", "update table", "delete from", "begin batch"}
	cantPrepare := []string{"create table", "USE table", "LIST keyspaces", "alter table", "drop table", "grant user", "revoke user"}

	for i := 0; i < len(toPrepare); i++ {
		q := &Query{stmt: toPrepare[i], routingInfo: &queryRoutingInfo{}}
		if !q.shouldPrepare() {
			t.Fatalf("expected Query.shouldPrepare to return true, got false for statement '%v'", toPrepare[i])
		}
	}

	for i := 0; i < len(cantPrepare); i++ {
		q := &Query{stmt: cantPrepare[i], routingInfo: &queryRoutingInfo{}}
		if q.shouldPrepare() {
			t.Fatalf("expected Query.shouldPrepare to return false, got true for statement '%v'", cantPrepare[i])
		}
	}
}

func TestBatchBasicAPI(t *testing.T) {
	t.Parallel()

	cfg := &ClusterConfig{RetryPolicy: &SimpleRetryPolicy{NumRetries: 2}}

	s := &Session{
		cfg:    *cfg,
		cons:   Quorum,
		logger: cfg.logger(),
	}
	defer s.Close()

	s.pool = cfg.PoolConfig.buildPool(s)

	// Test UnloggedBatch
	b := s.Batch(UnloggedBatch)
	if b.Type != UnloggedBatch {
		t.Fatalf("expceted batch.Type to be '%v', got '%v'", UnloggedBatch, b.Type)
	} else if b.rt != cfg.RetryPolicy {
		t.Fatalf("expceted batch.RetryPolicy to be '%v', got '%v'", cfg.RetryPolicy, b.rt)
	}

	// Test LoggedBatch
	b = s.Batch(LoggedBatch)
	if b.Type != LoggedBatch {
		t.Fatalf("expected batch.Type to be '%v', got '%v'", LoggedBatch, b.Type)
	}

	ip := "127.0.0.1"

	// Test attempts
	b.metrics = preFilledQueryMetrics(map[string]*hostMetrics{ip: {Attempts: 1}})
	if b.Attempts() != 1 {
		t.Fatalf("expected batch.Attempts() to return %v, got %v", 1, b.Attempts())
	}

	b.AddAttempts(2, &HostInfo{hostname: ip, connectAddress: net.ParseIP(ip), port: 9042})
	if b.Attempts() != 3 {
		t.Fatalf("expected batch.Attempts() to return %v, got %v", 3, b.Attempts())
	}

	// Test latency
	if b.Latency() != 0 {
		t.Fatalf("expected batch.Latency() to be 0, got %v", b.Latency())
	}

	b.metrics = preFilledQueryMetrics(map[string]*hostMetrics{ip: {Attempts: 1, TotalLatency: 4}})
	if b.Latency() != 4 {
		t.Fatalf("expected batch.Latency() to return %v, got %v", 4, b.Latency())
	}

	// Test Consistency
	b.Cons = One
	if b.GetConsistency() != One {
		t.Fatalf("expected batch.GetConsistency() to return 'One', got '%s'", b.GetConsistency())
	}

	trace := NewTracer(nil)
	b.Trace(trace)
	if b.trace != trace {
		t.Fatalf("expected batch.Trace to be '%v', got '%v'", trace, b.trace)
	}

	// Test batch.Query()
	b.Query("test", 1)
	if b.Entries[0].Stmt != "test" {
		t.Fatalf("expected batch.Entries[0].Statement to be 'test', got '%v'", b.Entries[0].Stmt)
	} else if b.Entries[0].Args[0].(int) != 1 {
		t.Fatalf("expected batch.Entries[0].Args[0] to be 1, got %v", b.Entries[0].Args[0])
	}

	b.Bind("test2", func(q *QueryInfo) ([]any, error) {
		return nil, nil
	})

	if b.Entries[1].Stmt != "test2" {
		t.Fatalf("expected batch.Entries[1].Statement to be 'test2', got '%v'", b.Entries[1].Stmt)
	} else if b.Entries[1].binding == nil {
		t.Fatal("expected batch.Entries[1].binding to be defined, got nil")
	}

	// Test RetryPolicy
	r := &SimpleRetryPolicy{NumRetries: 4}

	b.RetryPolicy(r)
	if b.rt != r {
		t.Fatalf("expected batch.RetryPolicy to be '%v', got '%v'", r, b.rt)
	}

	if b.Size() != 2 {
		t.Fatalf("expected batch.Size() to return 2, got %v", b.Size())
	}

}

func TestConsistencyNames(t *testing.T) {
	t.Parallel()

	names := map[fmt.Stringer]string{
		Any:         "ANY",
		One:         "ONE",
		Two:         "TWO",
		Three:       "THREE",
		Quorum:      "QUORUM",
		All:         "ALL",
		LocalQuorum: "LOCAL_QUORUM",
		EachQuorum:  "EACH_QUORUM",
		Serial:      "SERIAL",
		LocalSerial: "LOCAL_SERIAL",
		LocalOne:    "LOCAL_ONE",
	}

	for k, v := range names {
		if k.String() != v {
			t.Fatalf("expected '%v', got '%v'", v, k.String())
		}
	}
}

func TestIsUseStatement(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		input string
		exp   bool
	}{
		{"USE foo", true},
		{"USe foo", true},
		{"UsE foo", true},
		{"Use foo", true},
		{"uSE foo", true},
		{"uSe foo", true},
		{"usE foo", true},
		{"use foo", true},
		{"SELECT ", false},
		{"UPDATE ", false},
		{"INSERT ", false},
		{"", false},
	}

	for _, tc := range testCases {
		v := isUseStatement(tc.input)
		if v != tc.exp {
			t.Fatalf("expected %v but got %v for statement %q", tc.exp, v, tc.input)
		}
	}
}

type simpleTestRetryPolycy struct {
	RetryType  RetryType
	NumRetries int
}

func (p *simpleTestRetryPolycy) Attempt(q RetryableQuery) bool {
	return q.Attempts() <= p.NumRetries
}

func (p *simpleTestRetryPolycy) GetRetryType(error) RetryType {
	return p.RetryType
}

// TestRetryType_IgnoreRethrow verify that with Ignore/Rethrow retry types:
// - retries stopped
// - return error is not nil on Rethrow, Ignore
// - observed error is not nil
func TestRetryType_IgnoreRethrow(t *testing.T) {
	t.Parallel()

	session := createSession(t)
	defer session.Close()

	var observedErr error
	var observedAttempts int

	resetObserved := func() {
		observedErr = nil
		observedAttempts = 0
	}

	observer := funcQueryObserver(func(ctx context.Context, o ObservedQuery) {
		observedErr = o.Err
		observedAttempts++
	})

	for i, caseParams := range []struct {
		retries   int
		retryType RetryType
	}{
		{0, Ignore},  // check that stops retries
		{1, Ignore},  // check that stops retries
		{0, Rethrow}, // check that stops retries
		{1, Rethrow}, // check that stops retries
	} {
		retryPolicy := &simpleTestRetryPolycy{RetryType: caseParams.retryType, NumRetries: caseParams.retries}

		err := session.Query("INSERT INTO gocql_test.invalid_table(value) VALUES(1)").Idempotent(true).RetryPolicy(retryPolicy).Observer(observer).Exec()

		if err == nil {
			t.Fatalf("case %d [%v] Expected unconfigured table error, got: nil", i, caseParams.retryType)
		}

		if observedErr == nil {
			t.Fatalf("case %d expected unconfigured table error in Obserer, got: nil", i)
		}

		expectedAttempts := caseParams.retries
		if expectedAttempts == 0 {
			expectedAttempts = 1
		}
		if observedAttempts != expectedAttempts {
			t.Fatalf("case %d expected %d attempts, got: %d", i, expectedAttempts, observedAttempts)
		}

		resetObserved()
	}
}

type sessionCache struct {
	orig       tls.ClientSessionCache
	values     map[string][][]byte
	caches     map[string][]int64
	valuesLock sync.Mutex
}

func (c *sessionCache) Get(sessionKey string) (session *tls.ClientSessionState, ok bool) {
	return c.orig.Get(sessionKey)
}

func (c *sessionCache) Put(sessionKey string, cs *tls.ClientSessionState) {
	ticket, _, err := cs.ResumptionState()
	if err != nil {
		panic(err)
	}
	if len(ticket) == 0 {
		panic("ticket should not be empty")
	}
	c.valuesLock.Lock()
	c.values[sessionKey] = append(c.values[sessionKey], ticket)
	c.valuesLock.Unlock()
	c.orig.Put(sessionKey, cs)
}

func (c *sessionCache) NumberOfTickets() int {
	c.valuesLock.Lock()
	defer c.valuesLock.Unlock()
	total := 0
	for _, tickets := range c.values {
		total += len(tickets)
	}
	return total
}

func newSessionCache() *sessionCache {
	return &sessionCache{
		orig:       tls.NewLRUClientSessionCache(1024),
		values:     make(map[string][][]byte),
		caches:     make(map[string][]int64),
		valuesLock: sync.Mutex{},
	}
}

func withSessionCache(cache tls.ClientSessionCache) func(config *ClusterConfig) {
	return func(config *ClusterConfig) {
		config.SslOpts = &SslOptions{
			EnableHostVerification: false,
			Config: &tls.Config{
				ClientSessionCache: cache,
				InsecureSkipVerify: true,
			},
		}
	}
}

func TestTLSTicketResumption(t *testing.T) {
	t.Parallel()

	t.Skip("TLS ticket resumption is only supported by 2025.2 and later")

	c := newSessionCache()
	session := createSession(t, withSessionCache(c))
	defer session.Close()

	waitAllConnectionsOpened := func() error {
		println("wait all connections opened")
		defer println("end of wait all connections closed")
		endtime := time.Now().UTC().Add(time.Second * 10)
		for {
			if time.Now().UTC().After(endtime) {
				return fmt.Errorf("timed out waiting for all connections opened")
			}
			missing, err := session.MissingConnections()
			if err != nil {
				return fmt.Errorf("failed to get missing connections count: %w", err)
			}
			if missing == 0 {
				return nil
			}
			time.Sleep(time.Millisecond * 100)
		}
	}

	if err := waitAllConnectionsOpened(); err != nil {
		t.Fatal(err)
	}
	tickets := c.NumberOfTickets()
	if tickets == 0 {
		t.Fatal("no tickets learned, which means that server does not support TLS tickets")
	}

	session.CloseAllConnections()
	if err := waitAllConnectionsOpened(); err != nil {
		t.Fatal(err)
	}
	newTickets1 := c.NumberOfTickets()

	session.CloseAllConnections()
	if err := waitAllConnectionsOpened(); err != nil {
		t.Fatal(err)
	}
	newTickets2 := c.NumberOfTickets()

	if newTickets1 != tickets {
		t.Fatalf("new tickets learned, it looks like tls tickets where not reused: new %d, was %d", newTickets1, tickets)
	}
	if newTickets2 != tickets {
		t.Fatalf("new tickets learned, it looks like tls tickets where not reused: new %d, was %d", newTickets2, tickets)
	}
}
