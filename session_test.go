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
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Typed request helpers keep parameter construction explicit and bind inputs once.
func sessionTestBind(t testing.TB, session *Session, cql string, values ...Value) *Bound[Unit] {
	t.Helper()
	prepared, err := Prepare(context.Background(), session, Statement[[]Value, Unit]{CQL: cql, Params: DynamicParams, Rows: NoRows}, PrepareOptions{})
	if err != nil {
		t.Fatal(err)
	}
	bound, err := BindOwned(prepared, values)
	if err != nil {
		t.Fatal(err)
	}
	return bound
}
func sessionTestItem[R any](t testing.TB, bound *Bound[R]) BatchItem {
	t.Helper()
	item, err := BatchEntry(bound)
	if err != nil {
		t.Fatal(err)
	}
	return item
}
func sessionTestInt(t testing.TB, row DynamicRow, name string) int64 {
	t.Helper()
	value, err := row.Get(name)
	if err != nil {
		t.Fatal(err)
	}
	integer, err := value.Signed()
	if err != nil {
		t.Fatal(err)
	}
	return integer
}

func TestSessionAPI(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	config := DefaultConfig("127.0.0.1")
	profile := config.DefaultProfile.Snapshot()
	profile.Consistency, profile.PageSize, profile.Prefetch = All, 100, 1
	require.NoError(t, config.DefaultProfile.Replace(profile))
	// Session setters are replaced by atomic profile snapshots. Fractional
	// prefetch became a bounded count of zero or one physical page.
	session := &Session{logger: nopLogger{}}
	session.typedRuntime().profile = config.DefaultProfile
	defer session.Close()
	resolved, err := resolveOptions(session, Options{Trace: true})
	require.NoError(t, err)
	require.Equal(t, All, resolved.profile.Consistency)
	require.Equal(t, int32(100), resolved.profile.PageSize)
	require.Equal(t, 1, resolved.profile.Prefetch)
	require.True(t, resolved.options.Trace)

	intType, err := ScalarType(TypeInt)
	require.NoError(t, err)
	var encoded int32
	statement := Statement[int32, Unit]{CQL: "INSERT INTO test(id) VALUES (?)", Params: Params(ParamAt(0, Int32, func(n int32) int32 { encoded = n; return n })), Rows: NoRows}
	bound, err := BindUnpreparedOwned(statement, NewColumns(ColumnSpec{Name: "id", Type: intType}), int32(1))
	require.NoError(t, err)
	defer bound.Close()
	require.Equal(t, int32(1), encoded, "typed parameter accessor replaces callback binding")
	// An incomplete session now reports ErrNoConnections from the typed path.
	// It does not construct the old mutable Query/Iter error holder.
	_, err = Run(ctx, session, bound, Options{})
	require.ErrorIs(t, err, ErrNoConnections)
	batch := []BatchItem{sessionTestItem(t, bound)}
	_, err = ExecBatch(ctx, session, LoggedBatch, batch, Options{})
	require.ErrorIs(t, err, ErrNoConnections)
	require.NoError(t, session.Close())
	require.True(t, session.Closed())
	require.NoError(t, session.Close())
	_, err = ExecBatch(ctx, session, LoggedBatch, batch, Options{})
	require.ErrorIs(t, err, ErrClosed)
	_, err = Exec(ctx, session, bound, Options{})
	require.ErrorIs(t, err, ErrClosed)
}

type sessionTestObserver struct {
	mu       sync.Mutex
	requests []RequestEvent
	attempts []AttemptEvent
}

func (o *sessionTestObserver) ObserveRequest(e RequestEvent) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.requests = append(o.requests, e)
}
func (o *sessionTestObserver) ObserveAttempt(e AttemptEvent) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.attempts = append(o.attempts, e)
}
func (o *sessionTestObserver) snapshot() ([]RequestEvent, []AttemptEvent) {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]RequestEvent(nil), o.requests...), append([]AttemptEvent(nil), o.attempts...)
}

type sessionTestRetry struct {
	action      RetryAction
	maxAttempts int
}

func (p sessionTestRetry) Decide(e AttemptError) RetryDecision {
	if e.Attempt >= p.maxAttempts {
		return RetryDecision{Action: StopRetry}
	}
	return RetryDecision{Action: p.action}
}

func TestQueryBasicAPI(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	session := createV2Session(t)
	defer session.Close()
	table := testTableName(t)
	require.NoError(t, v2CreateTable(ctx, session, fmt.Sprintf("CREATE TABLE %s (id int PRIMARY KEY)", table)))
	prepared, err := Prepare(ctx, session, Statement[int32, Unit]{CQL: fmt.Sprintf("INSERT INTO %s(id) VALUES (?)", table), Params: OneParam(Int32), Rows: NoRows}, PrepareOptions{})
	require.NoError(t, err)
	require.Equal(t, 1, prepared.Parameters().Len())
	require.Equal(t, table, prepared.Info().Table)
	original, err := BindOwned(prepared, int32(1))
	require.NoError(t, err)
	defer original.Close()
	rebound, err := BindOwned(prepared, int32(2))
	require.NoError(t, err)
	defer rebound.Close()

	// Query setters become per-admission Options. Trace IDs and timing belong
	// to each execution result; a Bound never accumulates mutable metrics.
	zero := Result{}
	require.Zero(t, zero.Attempts)
	require.Zero(t, zero.Latency)
	retry := sessionTestRetry{action: RetryNext, maxAttempts: 3}
	observer := new(sessionTestObserver)
	options := Options{Consistency: Option[Consistency]{Value: All, Set: true}, SerialConsistency: Option[Option[Consistency]]{Value: Option[Consistency]{Value: LocalSerial, Set: true}, Set: true}, PageSize: Option[int32]{Value: 10, Set: true}, Prefetch: Option[int]{Value: 1, Set: true}, Retry: retry, Trace: true, Observer: observer}
	resolved, err := resolveOptions(session, options)
	require.NoError(t, err)
	require.Equal(t, All, resolved.profile.Consistency)
	require.Equal(t, LocalSerial, resolved.profile.SerialConsistency.Value)
	require.Equal(t, int32(10), resolved.profile.PageSize)
	require.Equal(t, 1, resolved.profile.Prefetch)
	require.Equal(t, retry, resolved.profile.Retry)
	serialRead := options
	serialRead.Consistency.Value = LocalSerial
	resolvedSerial, err := resolveOptions(session, serialRead)
	require.NoError(t, err)
	require.Equal(t, LocalSerial, resolvedSerial.profile.Consistency)

	for _, bound := range []*Bound[Unit]{original, rebound, original} {
		result, err := Exec(ctx, session, bound, options)
		require.NoError(t, err)
		require.Equal(t, 1, result.Attempts)
		require.Greater(t, result.Latency, time.Duration(0))
		require.Equal(t, All, result.AchievedConsistency)
		require.NotEmpty(t, result.TraceIDs)
	}
	requests, attempts := observer.snapshot()
	require.Len(t, requests, 3)
	require.Len(t, attempts, 3)
	for i := range attempts {
		require.Equal(t, 1, attempts[i].Attempt)
		require.NoError(t, attempts[i].Err)
	}
	for _, id := range []int32{1, 2} {
		row, err := v2ReadOne(ctx, session, fmt.Sprintf("SELECT id FROM %s WHERE id=?", table), IntValue(id))
		require.NoError(t, err)
		require.Equal(t, int64(id), sessionTestInt(t, row, "id"))
	}
	// Freeze a profile at admission, then replace it while the request remains
	// admitted. This directly covers the setter-to-snapshot migration boundary.
	profile := session.typedRuntime().profile.Snapshot()
	profile.PageSize = 10
	handle, err := NewProfile(profile)
	require.NoError(t, err)
	_, cancel, admitted, err := admitExecution(ctx, session, original, Options{Profile: handle})
	require.NoError(t, err)
	profile.PageSize = 100
	require.NoError(t, handle.Replace(profile))
	require.Equal(t, int32(10), admitted.options.profile.PageSize)
	require.Equal(t, int32(100), handle.Snapshot().PageSize)
	admitted.logical.finish(Result{}, nil)
	admitted.owner.release()
	cancel()
	original.Close()
	require.NoError(t, original.Wait(ctx))
	_, err = Exec(ctx, session, original, Options{})
	require.ErrorIs(t, err, ErrClosed)
}

func TestQueryShouldPrepare(t *testing.T) {
	t.Parallel()
	toPrepare := []string{"select * ", "INSERT INTO", "update table", "delete from", "begin batch"}
	cantPrepare := []string{"create table", "USE table", "LIST keyspaces", "alter table", "drop table", "grant user", "revoke user"}
	// Preparation is explicit in v2. Preserve every old classification input:
	// non-USE statements bind without keyword guessing, and explicit Prepare
	// reaches connection selection even for formerly unprepared DDL.
	session := &Session{logger: nopLogger{}}
	defer session.Close()
	for _, cql := range append(toPrepare, cantPrepare...) {
		t.Run(cql, func(t *testing.T) {
			statement := Statement[Unit, Unit]{CQL: cql, Params: NoParams, Rows: NoRows}
			bound, err := BindUnpreparedOwned(statement, NewColumns(), Unit{})
			if firstCQLWord(cql) == "USE" {
				require.ErrorIs(t, err, ErrUseStmt)
				_, err = Prepare(context.Background(), session, statement, PrepareOptions{})
				require.ErrorIs(t, err, ErrUseStmt)
				return
			}
			require.NoError(t, err)
			bound.Close()
			_, err = Prepare(context.Background(), session, statement, PrepareOptions{})
			require.ErrorIs(t, err, ErrNoConnections)
		})
	}
}

func TestBatchBasicAPI(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	retry := sessionTestRetry{action: RetryNext, maxAttempts: 2}
	session := createV2Session(t, func(config *Config) {
		profile := config.DefaultProfile.Snapshot()
		profile.Retry = retry
		require.NoError(t, config.DefaultProfile.Replace(profile))
	})
	defer session.Close()
	table := testTableName(t)
	require.NoError(t, v2CreateTable(ctx, session, fmt.Sprintf("CREATE TABLE %s (id int PRIMARY KEY)", table)))
	first := sessionTestBind(t, session, fmt.Sprintf("INSERT INTO %s(id) VALUES (?)", table), IntValue(1))
	defer first.Close()
	second := sessionTestBind(t, session, fmt.Sprintf("INSERT INTO %s(id) VALUES (?)", table), IntValue(2))
	defer second.Close()
	items := []BatchItem{sessionTestItem(t, first), sessionTestItem(t, second)}
	require.Len(t, items, 2)
	inherited, err := resolveOptions(session, Options{})
	require.NoError(t, err)
	require.Equal(t, retry, inherited.profile.Retry)
	override := sessionTestRetry{action: RetryNext, maxAttempts: 4}
	observer := new(sessionTestObserver)
	options := Options{Consistency: Option[Consistency]{Value: One, Set: true}, Retry: override, Trace: true, Observer: observer}
	resolved, err := resolveOptions(session, options)
	require.NoError(t, err)
	require.Equal(t, override, resolved.profile.Retry)
	require.Equal(t, One, resolved.profile.Consistency)
	require.True(t, resolved.options.Trace)
	// Native batch kind remains explicit; entries are immutable bound plans.
	// Per-request Result/Observer replace attempts/latency on a mutable batch.
	require.Zero(t, (Result{}).Attempts)
	require.Zero(t, (Result{}).Latency)
	for _, kind := range []BatchKind{UnloggedBatch, LoggedBatch} {
		result, err := ExecBatch(ctx, session, kind, items, options)
		require.NoError(t, err)
		require.Equal(t, 1, result.Attempts)
		require.Greater(t, result.Latency, time.Duration(0))
		require.Equal(t, One, result.AchievedConsistency)
		require.NotEmpty(t, result.TraceIDs)
	}
	requests, attempts := observer.snapshot()
	require.Len(t, requests, 2)
	require.Len(t, attempts, 2)
	for _, request := range requests {
		require.Len(t, request.BatchStatements, 2)
	}
	for _, id := range []int32{1, 2} {
		row, err := v2ReadOne(ctx, session, fmt.Sprintf("SELECT id FROM %s WHERE id=?", table), IntValue(id))
		require.NoError(t, err)
		require.Equal(t, int64(id), sessionTestInt(t, row, "id"))
	}
	// BatchEntry is only a descriptor. Closing any member prevents the entire
	// batch admission, and other members remain available for separate work.
	first.Close()
	_, err = ExecBatch(ctx, session, LoggedBatch, items, options)
	require.ErrorIs(t, err, ErrClosed)
	require.NoError(t, first.Wait(ctx))
	_, err = Exec(ctx, session, second, Options{})
	require.NoError(t, err)
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
		bound, err := BindUnpreparedOwned(Statement[Unit, Unit]{CQL: tc.input, Params: NoParams, Rows: NoRows}, NewColumns(), Unit{})
		if bound != nil {
			bound.Close()
		}
		v := errors.Is(err, ErrUseStmt)
		if v != tc.exp {
			t.Fatalf("expected %v but got %v for statement %q", tc.exp, v, tc.input)
		}
	}
}

// The old Ignore mode stopped retries but preserved errors. Its v2 equivalent
// is StopRetry; IgnoreWriteError is a separate explicit error-suppression policy.
func TestRetryType_IgnoreRethrow(t *testing.T) {
	t.Parallel()
	session := createV2Session(t)
	defer session.Close()
	ctx := context.Background()
	bound, err := BindUnpreparedOwned(Statement[Unit, Unit]{CQL: "INSERT INTO gocql_test.invalid_table(value) VALUES(1)", Params: NoParams, Rows: NoRows}, NewColumns(), Unit{})
	require.NoError(t, err)
	defer bound.Close()
	for _, mode := range []string{"Ignore", "Rethrow"} {
		for _, retries := range []int{0, 1} {
			t.Run(fmt.Sprintf("%s/%d", mode, retries), func(t *testing.T) {
				observer := new(sessionTestObserver)
				retry := sessionTestRetry{action: StopRetry, maxAttempts: retries + 1}
				_, err := Exec(ctx, session, bound, Options{Idempotent: true, Retry: retry, Observer: observer})
				require.Error(t, err)
				var server RequestError
				require.ErrorAs(t, err, &server)
				requests, attempts := observer.snapshot()
				require.Len(t, requests, 1)
				require.Len(t, attempts, 1)
				require.Error(t, requests[0].Err)
				require.Error(t, attempts[0].Err)
				require.Equal(t, 1, requests[0].Result.Attempts)
			})
		}
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
	if cs == nil {
		c.orig.Put(sessionKey, nil)
		return
	}
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

func withSessionCache(cache tls.ClientSessionCache) func(*Config) {
	return func(config *Config) {
		// Preserve fixture roots, mutual TLS identity, and verifier callbacks.
		config.TLS = config.TLS.Clone()
		config.TLS.ClientSessionCache = cache
	}
}

func TestTLSTicketResumption(t *testing.T) {
	t.Parallel()

	if !*flagRunSslTest || *flagDistribution != "scylla" {
		t.Skip("requires a ScyllaDB TLS fixture")
	}
	if flagCassVersion.Major < 2025 || (flagCassVersion.Major == 2025 && flagCassVersion.Minor < 2) {
		t.Skip("TLS ticket resumption requires ScyllaDB 2025.2 or later")
	}

	c := newSessionCache()
	var handshakes, resumed atomic.Int64
	session := createV2Session(t, withSessionCache(c), func(config *Config) {
		config.TLS.VerifyConnection = func(state tls.ConnectionState) error {
			handshakes.Add(1)
			if state.DidResume {
				resumed.Add(1)
			}
			return nil
		}
	})
	defer session.Close()

	waitAllConnectionsOpened := func() error {
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

	for round := 0; round < 2; round++ {
		previousHandshakes, previousResumed := handshakes.Load(), resumed.Load()
		session.CloseAllConnections()
		if err := waitAllConnectionsOpened(); err != nil {
			t.Fatal(err)
		}
		if handshakes.Load() <= previousHandshakes {
			t.Fatal("pool reconnection did not establish new TLS connections")
		}
		if resumed.Load() <= previousResumed {
			t.Fatal("none of the replacement TLS connections resumed a session")
		}
	}
}
