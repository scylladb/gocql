// Copyright (c) 2015 The gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

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
 * Copyright (c) 2016, The Gocql authors,
 * provided under the BSD-3-Clause License.
 * See the NOTICE file distributed with this work for additional information.
 */

package gocql

import (
	"errors"
	"fmt"
	"github.com/gocql/gocql/internal/tests"
	"net"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
)

// Tests of the round-robin host selection policy implementation
func TestRoundRobbin(t *testing.T) {
	t.Parallel()

	policy := RoundRobinHostPolicy()

	hosts := [...]*HostInfo{
		{hostId: "0", connectAddress: net.IPv4(0, 0, 0, 1)},
		{hostId: "1", connectAddress: net.IPv4(0, 0, 0, 2)},
	}

	for _, host := range hosts {
		policy.AddHost(host)
	}

	got := make(map[string]bool)
	it := policy.Pick(nil)
	for h := it(); h != nil; h = it() {
		id := h.Info().hostId
		if got[id] {
			t.Fatalf("got duplicate host: %v", id)
		}
		got[id] = true
	}
	if len(got) != len(hosts) {
		t.Fatalf("expected %d hosts got %d", len(hosts), len(got))
	}
}

func TestRoundRobbinSameConnectAddress(t *testing.T) {
	t.Parallel()

	policy := RoundRobinHostPolicy()

	hosts := [...]*HostInfo{
		{hostId: "0", connectAddress: net.IPv4(0, 0, 0, 1), port: 9042},
		{hostId: "1", connectAddress: net.IPv4(0, 0, 0, 1), port: 9043},
	}

	for _, host := range hosts {
		policy.AddHost(host)
	}

	got := make(map[string]bool)
	it := policy.Pick(nil)
	for h := it(); h != nil; h = it() {
		id := h.Info().hostId
		if got[id] {
			t.Fatalf("got duplicate host: %v", id)
		}
		got[id] = true
	}
	if len(got) != len(hosts) {
		t.Fatalf("expected %d hosts got %d", len(hosts), len(got))
	}
}

// Tests of the token-aware host selection policy implementation with a
// round-robin host selection policy fallback.
func TestHostPolicy_TokenAware_SimpleStrategy(t *testing.T) {
	t.Parallel()

	const keyspace = "myKeyspace"
	policy := TokenAwareHostPolicy(RoundRobinHostPolicy())
	policyInternal := policy.(*tokenAwareHostPolicy)
	policyInternal.getKeyspaceName = func() string { return keyspace }
	policyInternal.getKeyspaceMetadata = func(ks string) (*KeyspaceMetadata, error) {
		return nil, errors.New("not initalized")
	}

	query := &Query{routingInfo: &queryRoutingInfo{}}
	query.getKeyspace = func() string { return keyspace }

	iter := policy.Pick(nil)
	if iter == nil {
		t.Fatal("host iterator was nil")
	}
	actual := iter()
	if actual != nil {
		t.Fatalf("expected nil from iterator, but was %v", actual)
	}

	// set the hosts
	hosts := [...]*HostInfo{
		{hostId: "0", connectAddress: net.IPv4(10, 0, 0, 1), tokens: []string{"00"}},
		{hostId: "1", connectAddress: net.IPv4(10, 0, 0, 2), tokens: []string{"25"}},
		{hostId: "2", connectAddress: net.IPv4(10, 0, 0, 3), tokens: []string{"50"}},
		{hostId: "3", connectAddress: net.IPv4(10, 0, 0, 4), tokens: []string{"75"}},
	}
	for _, host := range &hosts {
		policy.AddHost(host)
	}

	policy.SetPartitioner("OrderedPartitioner")

	policyInternal.getKeyspaceMetadata = func(keyspaceName string) (*KeyspaceMetadata, error) {
		if keyspaceName != keyspace {
			return nil, fmt.Errorf("unknown keyspace: %s", keyspaceName)
		}
		return &KeyspaceMetadata{
			Name:          keyspace,
			StrategyClass: "SimpleStrategy",
			StrategyOptions: map[string]interface{}{
				"class":              "SimpleStrategy",
				"replication_factor": 2,
			},
		}, nil
	}
	policy.KeyspaceChanged(KeyspaceUpdateEvent{Keyspace: keyspace})

	// The SimpleStrategy above should generate the following replicas.
	// It's handy to have as reference here.
	tests.AssertDeepEqual(t, "replicas", map[string]tokenRingReplicas{
		"myKeyspace": {
			{orderedToken("00"), []*HostInfo{hosts[0], hosts[1]}},
			{orderedToken("25"), []*HostInfo{hosts[1], hosts[2]}},
			{orderedToken("50"), []*HostInfo{hosts[2], hosts[3]}},
			{orderedToken("75"), []*HostInfo{hosts[3], hosts[0]}},
		},
	}, policyInternal.getMetadataReadOnly().replicas)

	// now the token ring is configured
	query.RoutingKey([]byte("20"))
	iter = policy.Pick(query)
	// shuffling is enabled by default, expecfing
	expectHosts(t, "hosts[0]", iter, "1", "2")
	// then rest of the hosts
	expectHosts(t, "rest", iter, "0", "3")
	expectNoMoreHosts(t, iter)
}

func TestHostPolicy_TokenAware_LWT_DisablesHostShuffling(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		hosts      []*HostInfo
		routingKey string
		lwt        bool
		shuffle    bool
		want       []string
	}{
		"token 08 shuffling configured": {hosts: []*HostInfo{
			{hostId: "0", connectAddress: net.IPv4(10, 0, 0, 1), tokens: []string{"00", "10", "20"}},
			{hostId: "1", connectAddress: net.IPv4(10, 0, 0, 3), tokens: []string{"25", "35", "45"}},
			{hostId: "2", connectAddress: net.IPv4(10, 0, 0, 2), tokens: []string{"00", "10", "20"}},
			{hostId: "3", connectAddress: net.IPv4(10, 0, 0, 4), tokens: []string{"25", "35", "45"}},
			{hostId: "4", connectAddress: net.IPv4(10, 0, 0, 3), tokens: []string{"50", "60", "70"}},
			{hostId: "5", connectAddress: net.IPv4(10, 0, 0, 4), tokens: []string{"50", "60", "70"}},
		}, routingKey: "8", lwt: true, shuffle: true, want: []string{"0", "2", "3", "4", "5", "1"}},
		"token 08 shuffling not configured": {hosts: []*HostInfo{
			{hostId: "0", connectAddress: net.IPv4(10, 0, 0, 1), tokens: []string{"00", "10", "20"}},
			{hostId: "1", connectAddress: net.IPv4(10, 0, 0, 3), tokens: []string{"25", "35", "45"}},
			{hostId: "2", connectAddress: net.IPv4(10, 0, 0, 2), tokens: []string{"00", "10", "20"}},
			{hostId: "3", connectAddress: net.IPv4(10, 0, 0, 4), tokens: []string{"25", "35", "45"}},
			{hostId: "4", connectAddress: net.IPv4(10, 0, 0, 3), tokens: []string{"50", "60", "70"}},
			{hostId: "5", connectAddress: net.IPv4(10, 0, 0, 4), tokens: []string{"50", "60", "70"}},
		}, routingKey: "8", lwt: true, shuffle: false, want: []string{"0", "2", "3", "4", "5", "1"}},
		"token 30 shuffling configured": {hosts: []*HostInfo{
			{hostId: "0", connectAddress: net.IPv4(10, 0, 0, 1), tokens: []string{"00", "10", "20"}},
			{hostId: "1", connectAddress: net.IPv4(10, 0, 0, 3), tokens: []string{"25", "35", "45"}},
			{hostId: "2", connectAddress: net.IPv4(10, 0, 0, 2), tokens: []string{"00", "10", "20"}},
			{hostId: "3", connectAddress: net.IPv4(10, 0, 0, 4), tokens: []string{"25", "35", "45"}},
			{hostId: "4", connectAddress: net.IPv4(10, 0, 0, 3), tokens: []string{"50", "60", "70"}},
			{hostId: "5", connectAddress: net.IPv4(10, 0, 0, 4), tokens: []string{"50", "60", "70"}},
		}, routingKey: "30", lwt: true, shuffle: true, want: []string{"1", "3", "2", "4", "5", "0"}},
		"token 30 shuffling not configured": {hosts: []*HostInfo{
			{hostId: "0", connectAddress: net.IPv4(10, 0, 0, 1), tokens: []string{"00", "10", "20"}},
			{hostId: "1", connectAddress: net.IPv4(10, 0, 0, 3), tokens: []string{"25", "35", "45"}},
			{hostId: "2", connectAddress: net.IPv4(10, 0, 0, 2), tokens: []string{"00", "10", "20"}},
			{hostId: "3", connectAddress: net.IPv4(10, 0, 0, 4), tokens: []string{"25", "35", "45"}},
			{hostId: "4", connectAddress: net.IPv4(10, 0, 0, 3), tokens: []string{"50", "60", "70"}},
			{hostId: "5", connectAddress: net.IPv4(10, 0, 0, 4), tokens: []string{"50", "60", "70"}},
		}, routingKey: "30", lwt: true, shuffle: false, want: []string{"1", "3", "2", "4", "5", "0"}},
		"token 55 shuffling configured": {hosts: []*HostInfo{
			{hostId: "0", connectAddress: net.IPv4(10, 0, 0, 1), tokens: []string{"00", "10", "20"}},
			{hostId: "1", connectAddress: net.IPv4(10, 0, 0, 3), tokens: []string{"25", "35", "45"}},
			{hostId: "2", connectAddress: net.IPv4(10, 0, 0, 2), tokens: []string{"00", "10", "20"}},
			{hostId: "3", connectAddress: net.IPv4(10, 0, 0, 4), tokens: []string{"25", "35", "45"}},
			{hostId: "4", connectAddress: net.IPv4(10, 0, 0, 3), tokens: []string{"50", "60", "70"}},
			{hostId: "5", connectAddress: net.IPv4(10, 0, 0, 4), tokens: []string{"50", "60", "70"}},
		}, routingKey: "55", lwt: true, shuffle: true, want: []string{"4", "5", "2", "3", "0", "1"}},
		"token 55 shuffling not configured": {hosts: []*HostInfo{
			{hostId: "0", connectAddress: net.IPv4(10, 0, 0, 1), tokens: []string{"00", "10", "20"}},
			{hostId: "1", connectAddress: net.IPv4(10, 0, 0, 3), tokens: []string{"25", "35", "45"}},
			{hostId: "2", connectAddress: net.IPv4(10, 0, 0, 2), tokens: []string{"00", "10", "20"}},
			{hostId: "3", connectAddress: net.IPv4(10, 0, 0, 4), tokens: []string{"25", "35", "45"}},
			{hostId: "4", connectAddress: net.IPv4(10, 0, 0, 3), tokens: []string{"50", "60", "70"}},
			{hostId: "5", connectAddress: net.IPv4(10, 0, 0, 4), tokens: []string{"50", "60", "70"}},
		}, routingKey: "55", lwt: true, shuffle: false, want: []string{"4", "5", "2", "3", "0", "1"}},
	}
	const keyspace = "myKeyspace"
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			policy := createPolicy(keyspace, tc.shuffle)
			for _, host := range tc.hosts {
				policy.AddHost(host)
			}
			query := &Query{
				routingKey:  []byte(tc.routingKey),
				routingInfo: &queryRoutingInfo{lwt: tc.lwt},
			}
			query.getKeyspace = func() string { return keyspace }
			iter := policy.Pick(query)
			var hostIds []string
			for host := iter(); host != nil; host = iter() {
				hostIds = append(hostIds, host.Info().hostId)
			}
			if diff := cmp.Diff(hostIds, tc.want); diff != "" {
				t.Errorf("expected %s, got %s, diff %s", tc.want, hostIds, diff)
			}
		})
	}
}

func createPolicy(keyspace string, shuffle bool) HostSelectionPolicy {
	policy := TokenAwareHostPolicy(RoundRobinHostPolicy())
	policyInternal := policy.(*tokenAwareHostPolicy)
	policyInternal.getKeyspaceName = func() string { return keyspace }
	policyInternal.getKeyspaceMetadata = func(ks string) (*KeyspaceMetadata, error) {
		return nil, errors.New("not initalized")
	}
	policy.SetPartitioner("OrderedPartitioner")

	policyInternal.getKeyspaceMetadata = func(keyspaceName string) (*KeyspaceMetadata, error) {
		if keyspaceName != keyspace {
			return nil, fmt.Errorf("unknown keyspace: %s", keyspaceName)
		}
		return &KeyspaceMetadata{
			Name:          keyspace,
			StrategyClass: "SimpleStrategy",
			StrategyOptions: map[string]interface{}{
				"class":              "SimpleStrategy",
				"replication_factor": 2,
			},
		}, nil
	}
	policyInternal.shuffleReplicas = shuffle
	policy.KeyspaceChanged(KeyspaceUpdateEvent{Keyspace: keyspace})
	return policy
}

func TestHostPolicy_RoundRobin_NilHostInfo(t *testing.T) {
	t.Parallel()

	policy := RoundRobinHostPolicy()

	host := &HostInfo{hostId: "host-1"}
	policy.AddHost(host)

	iter := policy.Pick(nil)
	next := iter()
	if next == nil {
		t.Fatal("got nil host")
	} else if v := next.Info(); v == nil {
		t.Fatal("got nil HostInfo")
	} else if v.HostID() != host.HostID() {
		t.Fatalf("expected host %v got %v", host, v)
	}

	next = iter()
	if next != nil {
		t.Errorf("expected to get nil host got %+v", next)
		if next.Info() == nil {
			t.Fatalf("HostInfo is nil")
		}
	}
}

func TestHostPolicy_TokenAware_NilHostInfo(t *testing.T) {
	t.Parallel()

	policy := TokenAwareHostPolicy(RoundRobinHostPolicy())
	policyInternal := policy.(*tokenAwareHostPolicy)
	policyInternal.getKeyspaceName = func() string { return "myKeyspace" }
	policyInternal.getKeyspaceMetadata = func(ks string) (*KeyspaceMetadata, error) {
		return nil, errors.New("not initialized")
	}

	hosts := [...]*HostInfo{
		{connectAddress: net.IPv4(10, 0, 0, 0), tokens: []string{"00"}},
		{connectAddress: net.IPv4(10, 0, 0, 1), tokens: []string{"25"}},
		{connectAddress: net.IPv4(10, 0, 0, 2), tokens: []string{"50"}},
		{connectAddress: net.IPv4(10, 0, 0, 3), tokens: []string{"75"}},
	}
	for _, host := range hosts {
		policy.AddHost(host)
	}
	policy.SetPartitioner("OrderedPartitioner")

	query := &Query{routingInfo: &queryRoutingInfo{}}
	query.getKeyspace = func() string { return "myKeyspace" }
	query.RoutingKey([]byte("20"))

	iter := policy.Pick(query)
	next := iter()
	if next == nil {
		t.Fatal("got nil host")
	} else if v := next.Info(); v == nil {
		t.Fatal("got nil HostInfo")
	} else if !v.ConnectAddress().Equal(hosts[1].ConnectAddress()) {
		t.Fatalf("expected peer 1 got %v", v.ConnectAddress())
	}

	// Empty the hosts to trigger the panic when using the fallback.
	for _, host := range hosts {
		policy.RemoveHost(host)
	}

	next = iter()
	if next != nil {
		t.Errorf("expected to get nil host got %+v", next)
		if next.Info() == nil {
			t.Fatalf("HostInfo is nil")
		}
	}
}

func TestCOWList_Add(t *testing.T) {
	t.Parallel()

	var cow cowHostList

	toAdd := [...]net.IP{net.IPv4(10, 0, 0, 1), net.IPv4(10, 0, 0, 2), net.IPv4(10, 0, 0, 3)}

	for _, addr := range toAdd {
		if !cow.add(&HostInfo{connectAddress: addr}) {
			t.Fatal("did not add peer which was not in the set")
		}
	}

	hosts := cow.get()
	if len(hosts) != len(toAdd) {
		t.Fatalf("expected to have %d hosts got %d", len(toAdd), len(hosts))
	}

	set := make(map[string]bool)
	for _, host := range hosts {
		set[string(host.ConnectAddress())] = true
	}

	for _, addr := range toAdd {
		if !set[string(addr)] {
			t.Errorf("addr was not in the host list: %q", addr)
		}
	}
}

// TestSimpleRetryPolicy makes sure that we only allow 1 + numRetries attempts
func TestSimpleRetryPolicy(t *testing.T) {
	t.Parallel()

	q := &Query{routingInfo: &queryRoutingInfo{}}

	// this should allow a total of 3 tries.
	rt := &SimpleRetryPolicy{NumRetries: 2}

	regular_error := errors.New("regular error")

	qe1 := &QueryError{
		err:                 errors.New("connection error"),
		potentiallyExecuted: false,
		isIdempotent:        false,
	}

	qe2 := &QueryError{
		err:                 errors.New("timeout error"),
		potentiallyExecuted: true,
		isIdempotent:        true,
	}

	qe3 := &QueryError{
		err:                 errors.New("write timeout"),
		potentiallyExecuted: true,
		isIdempotent:        false,
	}

	cases := []struct {
		attempts     int
		allow        bool
		err          error
		retryType    RetryType
		LWTRetryType RetryType
	}{
		{0, true, qe1, RetryNextHost, Retry},
		{1, true, qe2, RetryNextHost, Retry},
		{2, true, qe3, Rethrow, Rethrow},
		{3, false, regular_error, RetryNextHost, Retry},
		{4, false, regular_error, RetryNextHost, Retry},
		{5, false, regular_error, RetryNextHost, Retry},
	}

	for _, c := range cases {
		q.metrics = preFilledQueryMetrics(map[string]*hostMetrics{"127.0.0.1": {Attempts: c.attempts}})
		if c.retryType != rt.GetRetryType(c.err) {
			t.Fatalf("retry type for %v should be %v", c.err, c.retryType)
		}
		if c.LWTRetryType != rt.GetRetryTypeLWT(c.err) {
			t.Fatalf("LWT retry type for %v should be %v", c.err, c.LWTRetryType)
		}
		if c.allow && !rt.Attempt(q) {
			t.Fatalf("should allow retry after %d attempts", c.attempts)
		}
		if !c.allow && rt.Attempt(q) {
			t.Fatalf("should not allow retry after %d attempts", c.attempts)
		}
	}
}

func TestLWTSimpleRetryPolicy(t *testing.T) {
	t.Parallel()

	ebrp := &SimpleRetryPolicy{NumRetries: 2}
	// Verify that SimpleRetryPolicy implements both interfaces
	var _ RetryPolicy = ebrp
	var lwt_rt LWTRetryPolicy = ebrp
	tests.AssertEqual(t, "retry type of LWT policy", lwt_rt.GetRetryTypeLWT(nil), Retry)
}

func TestExponentialBackoffPolicy(t *testing.T) {
	t.Parallel()

	// test with defaults
	sut := &ExponentialBackoffRetryPolicy{NumRetries: 2}

	regular_error := errors.New("regular error")

	qe1 := &QueryError{
		err:                 errors.New("connection error"),
		potentiallyExecuted: false,
		isIdempotent:        false,
	}

	qe2 := &QueryError{
		err:                 errors.New("timeout error"),
		potentiallyExecuted: true,
		isIdempotent:        true,
	}

	qe3 := &QueryError{
		err:                 errors.New("write timeout"),
		potentiallyExecuted: true,
		isIdempotent:        false,
	}

	cases := []struct {
		attempts     int
		delay        time.Duration
		err          error
		retryType    RetryType
		LWTRetryType RetryType
	}{
		{1, 100 * time.Millisecond, qe1, RetryNextHost, Retry},
		{2, (2) * 100 * time.Millisecond, qe2, RetryNextHost, Retry},
		{3, (2 * 2) * 100 * time.Millisecond, qe3, Rethrow, Rethrow},
		{4, (2 * 2 * 2) * 100 * time.Millisecond, regular_error, RetryNextHost, Retry},
	}
	for _, c := range cases {
		if c.retryType != sut.GetRetryType(c.err) {
			t.Fatalf("retry type for %v should be %v", c.err, c.retryType)
		}
		if c.LWTRetryType != sut.GetRetryTypeLWT(c.err) {
			t.Fatalf("LWT retry type for %v should be %v", c.err, c.LWTRetryType)
		}
		// test 100 times for each case
		for i := 0; i < 100; i++ {
			d := sut.napTime(c.attempts)
			if d < c.delay-(100*time.Millisecond)/2 {
				t.Fatalf("Delay %d less than jitter min of %d", d, c.delay-100*time.Millisecond/2)
			}
			if d > c.delay+(100*time.Millisecond)/2 {
				t.Fatalf("Delay %d greater than jitter max of %d", d, c.delay+100*time.Millisecond/2)
			}
		}
	}
}

func TestLWTExponentialBackoffPolicy(t *testing.T) {
	t.Parallel()

	ebrp := &ExponentialBackoffRetryPolicy{NumRetries: 2}
	// Verify that ExponentialBackoffRetryPolicy implements both interfaces
	var _ RetryPolicy = ebrp
	var lwt_rt LWTRetryPolicy = ebrp
	tests.AssertEqual(t, "retry type of LWT policy", lwt_rt.GetRetryTypeLWT(nil), Retry)
}

func TestDowngradingConsistencyRetryPolicy(t *testing.T) {
	t.Parallel()

	q := &Query{cons: LocalQuorum, routingInfo: &queryRoutingInfo{}}

	rewt0 := &RequestErrWriteTimeout{
		Received:  0,
		WriteType: "SIMPLE",
	}

	rewt1 := &RequestErrWriteTimeout{
		Received:  1,
		WriteType: "BATCH",
	}

	rewt2 := &RequestErrWriteTimeout{
		WriteType: "UNLOGGED_BATCH",
	}

	rert := &RequestErrReadTimeout{}

	reu0 := &RequestErrUnavailable{
		Alive: 0,
	}

	reu1 := &RequestErrUnavailable{
		Alive: 1,
	}

	// this should allow a total of 3 tries.
	consistencyLevels := []Consistency{Three, Two, One}
	rt := &DowngradingConsistencyRetryPolicy{ConsistencyLevelsToTry: consistencyLevels}
	cases := []struct {
		attempts  int
		allow     bool
		err       error
		retryType RetryType
	}{
		{0, true, rewt0, Rethrow},
		{3, true, rewt1, Ignore},
		{1, true, rewt2, Retry},
		{2, true, rert, Retry},
		{4, false, reu0, Rethrow},
		{16, false, reu1, Retry},
	}

	for _, c := range cases {
		q.metrics = preFilledQueryMetrics(map[string]*hostMetrics{"127.0.0.1": {Attempts: c.attempts}})
		if c.retryType != rt.GetRetryType(c.err) {
			t.Fatalf("retry type should be %v", c.retryType)
		}
		if c.allow && !rt.Attempt(q) {
			t.Fatalf("should allow retry after %d attempts", c.attempts)
		}
		if !c.allow && rt.Attempt(q) {
			t.Fatalf("should not allow retry after %d attempts", c.attempts)
		}
	}
}

// expectHosts makes sure that the next len(hostIDs) returned from iter is a permutation of hostIDs.
func expectHosts(t *testing.T, msg string, iter NextHost, hostIDs ...string) {
	t.Helper()

	expectedHostIDs := make(map[string]struct{}, len(hostIDs))
	for i := range hostIDs {
		expectedHostIDs[hostIDs[i]] = struct{}{}
	}

	expectedStr := func() string {
		keys := make([]string, 0, len(expectedHostIDs))
		for k := range expectedHostIDs {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		return strings.Join(keys, ", ")
	}

	for len(expectedHostIDs) > 0 {
		host := iter()
		if host == nil || host.Info() == nil {
			t.Fatalf("%s: expected hostID one of {%s}, but got nil", msg, expectedStr())
		}
		hostID := host.Info().HostID()
		if _, ok := expectedHostIDs[hostID]; !ok {
			t.Fatalf("%s: expected host ID one of {%s}, but got %s", msg, expectedStr(), hostID)
		}
		delete(expectedHostIDs, hostID)
	}
}

func expectNoMoreHosts(t *testing.T, iter NextHost) {
	t.Helper()
	host := iter()
	if host == nil {
		// success
		return
	}
	info := host.Info()
	if info == nil {
		t.Fatalf("expected no more hosts, but got host with nil Info()")
		return
	}
	t.Fatalf("expected no more hosts, but got %s", info.HostID())
}

func TestHostPolicy_DCAwareRR(t *testing.T) {
	t.Parallel()

	p := DCAwareRoundRobinPolicy("local")

	hosts := [...]*HostInfo{
		{hostId: "0", connectAddress: net.ParseIP("10.0.0.1"), dataCenter: "local"},
		{hostId: "1", connectAddress: net.ParseIP("10.0.0.2"), dataCenter: "local"},
		{hostId: "2", connectAddress: net.ParseIP("10.0.0.3"), dataCenter: "remote"},
		{hostId: "3", connectAddress: net.ParseIP("10.0.0.4"), dataCenter: "remote"},
	}

	for _, host := range hosts {
		p.AddHost(host)
	}

	got := make(map[string]bool, len(hosts))
	var dcs []string

	it := p.Pick(nil)
	for h := it(); h != nil; h = it() {
		id := h.Info().hostId
		dc := h.Info().dataCenter

		if got[id] {
			t.Fatalf("got duplicate host %s", id)
		}
		got[id] = true
		dcs = append(dcs, dc)
	}

	if len(got) != len(hosts) {
		t.Fatalf("expected %d hosts got %d", len(hosts), len(got))
	}

	var remote bool
	for _, dc := range dcs {
		if dc == "local" {
			if remote {
				t.Fatalf("got local dc after remote: %v", dcs)
			}
		} else {
			remote = true
		}
	}

}

func TestHostPolicy_DCAwareRR_disableDCFailover(t *testing.T) {
	t.Parallel()

	p := DCAwareRoundRobinPolicy("local", HostPolicyOptionDisableDCFailover)

	hosts := [...]*HostInfo{
		{hostId: "0", connectAddress: net.ParseIP("10.0.0.1"), dataCenter: "local"},
		{hostId: "1", connectAddress: net.ParseIP("10.0.0.2"), dataCenter: "local"},
		{hostId: "2", connectAddress: net.ParseIP("10.0.0.3"), dataCenter: "remote"},
		{hostId: "3", connectAddress: net.ParseIP("10.0.0.4"), dataCenter: "remote"},
	}

	for _, host := range hosts {
		p.AddHost(host)
	}

	got := make(map[string]bool, len(hosts))
	var dcs []string

	it := p.Pick(nil)
	for h := it(); h != nil; h = it() {
		id := h.Info().hostId
		dc := h.Info().dataCenter

		if got[id] {
			t.Fatalf("got duplicate host %s", id)
		}
		got[id] = true
		dcs = append(dcs, dc)
	}

	if len(got) != 2 {
		t.Fatalf("expected %d hosts got %d", 2, len(got))
	}

	for _, dc := range dcs {
		if dc == "remote" {
			t.Fatalf("got remote dc but failover was diabled")
		}
	}
}

// Tests of the token-aware host selection policy implementation with a
// DC aware round-robin host selection policy fallback
// with {"class": "NetworkTopologyStrategy", "a": 1, "b": 1, "c": 1} replication.
func TestHostPolicy_TokenAware(t *testing.T) {
	t.Parallel()

	const keyspace = "myKeyspace"
	policy := TokenAwareHostPolicy(DCAwareRoundRobinPolicy("local"))
	policyInternal := policy.(*tokenAwareHostPolicy)
	policyInternal.getKeyspaceName = func() string { return keyspace }
	policyInternal.getKeyspaceMetadata = func(ks string) (*KeyspaceMetadata, error) {
		return nil, errors.New("not initialized")
	}

	query := &Query{routingInfo: &queryRoutingInfo{}}
	query.getKeyspace = func() string { return keyspace }

	iter := policy.Pick(nil)
	if iter == nil {
		t.Fatal("host iterator was nil")
	}
	actual := iter()
	if actual != nil {
		t.Fatalf("expected nil from iterator, but was %v", actual)
	}

	// set the hosts
	hosts := [...]*HostInfo{
		{hostId: "0", connectAddress: net.IPv4(10, 0, 0, 1), tokens: []string{"05"}, dataCenter: "remote1"},
		{hostId: "1", connectAddress: net.IPv4(10, 0, 0, 2), tokens: []string{"10"}, dataCenter: "local"},
		{hostId: "2", connectAddress: net.IPv4(10, 0, 0, 3), tokens: []string{"15"}, dataCenter: "remote2"},
		{hostId: "3", connectAddress: net.IPv4(10, 0, 0, 4), tokens: []string{"20"}, dataCenter: "remote1"},
		{hostId: "4", connectAddress: net.IPv4(10, 0, 0, 5), tokens: []string{"25"}, dataCenter: "local"},
		{hostId: "5", connectAddress: net.IPv4(10, 0, 0, 6), tokens: []string{"30"}, dataCenter: "remote2"},
		{hostId: "6", connectAddress: net.IPv4(10, 0, 0, 7), tokens: []string{"35"}, dataCenter: "remote1"},
		{hostId: "7", connectAddress: net.IPv4(10, 0, 0, 8), tokens: []string{"40"}, dataCenter: "local"},
		{hostId: "8", connectAddress: net.IPv4(10, 0, 0, 9), tokens: []string{"45"}, dataCenter: "remote2"},
		{hostId: "9", connectAddress: net.IPv4(10, 0, 0, 10), tokens: []string{"50"}, dataCenter: "remote1"},
		{hostId: "10", connectAddress: net.IPv4(10, 0, 0, 11), tokens: []string{"55"}, dataCenter: "local"},
		{hostId: "11", connectAddress: net.IPv4(10, 0, 0, 12), tokens: []string{"60"}, dataCenter: "remote2"},
	}
	for _, host := range hosts {
		policy.AddHost(host)
	}

	// the token ring is not setup without the partitioner, but the fallback
	// should work
	if actual := policy.Pick(nil)(); actual == nil {
		t.Fatal("expected to get host from fallback got nil")
	}

	query.RoutingKey([]byte("30"))
	if actual := policy.Pick(query)(); actual == nil {
		t.Fatal("expected to get host from fallback got nil")
	}

	policy.SetPartitioner("OrderedPartitioner")

	policyInternal.getKeyspaceMetadata = func(keyspaceName string) (*KeyspaceMetadata, error) {
		if keyspaceName != keyspace {
			return nil, fmt.Errorf("unknown keyspace: %s", keyspaceName)
		}
		return &KeyspaceMetadata{
			Name:          keyspace,
			StrategyClass: "NetworkTopologyStrategy",
			StrategyOptions: map[string]interface{}{
				"class":   "NetworkTopologyStrategy",
				"local":   1,
				"remote1": 1,
				"remote2": 1,
			},
		}, nil
	}
	policy.KeyspaceChanged(KeyspaceUpdateEvent{Keyspace: "myKeyspace"})

	// The NetworkTopologyStrategy above should generate the following replicas.
	// It's handy to have as reference here.
	tests.AssertDeepEqual(t, "replicas", map[string]tokenRingReplicas{
		"myKeyspace": {
			{orderedToken("05"), []*HostInfo{hosts[0], hosts[1], hosts[2]}},
			{orderedToken("10"), []*HostInfo{hosts[1], hosts[2], hosts[3]}},
			{orderedToken("15"), []*HostInfo{hosts[2], hosts[3], hosts[4]}},
			{orderedToken("20"), []*HostInfo{hosts[3], hosts[4], hosts[5]}},
			{orderedToken("25"), []*HostInfo{hosts[4], hosts[5], hosts[6]}},
			{orderedToken("30"), []*HostInfo{hosts[5], hosts[6], hosts[7]}},
			{orderedToken("35"), []*HostInfo{hosts[6], hosts[7], hosts[8]}},
			{orderedToken("40"), []*HostInfo{hosts[7], hosts[8], hosts[9]}},
			{orderedToken("45"), []*HostInfo{hosts[8], hosts[9], hosts[10]}},
			{orderedToken("50"), []*HostInfo{hosts[9], hosts[10], hosts[11]}},
			{orderedToken("55"), []*HostInfo{hosts[10], hosts[11], hosts[0]}},
			{orderedToken("60"), []*HostInfo{hosts[11], hosts[0], hosts[1]}},
		},
	}, policyInternal.getMetadataReadOnly().replicas)

	// now the token ring is configured
	query.RoutingKey([]byte("23"))
	iter = policy.Pick(query)
	// first should be host with matching token from the local DC
	expectHosts(t, "matching token from local DC", iter, "4")
	// next are in non-deterministic order
	expectHosts(t, "rest", iter, "0", "1", "2", "3", "5", "6", "7", "8", "9", "10", "11")
	expectNoMoreHosts(t, iter)
}

// Tests of the token-aware host selection policy implementation with a
// DC aware round-robin host selection policy fallback
// with {"class": "NetworkTopologyStrategy", "a": 2, "b": 2, "c": 2} replication.
func TestHostPolicy_TokenAware_NetworkStrategy(t *testing.T) {
	t.Parallel()

	const keyspace = "myKeyspace"
	policy := TokenAwareHostPolicy(DCAwareRoundRobinPolicy("local"), NonLocalReplicasFallback())
	policyInternal := policy.(*tokenAwareHostPolicy)
	policyInternal.getKeyspaceName = func() string { return keyspace }
	policyInternal.getKeyspaceMetadata = func(ks string) (*KeyspaceMetadata, error) {
		return nil, errors.New("not initialized")
	}

	query := &Query{routingInfo: &queryRoutingInfo{}}
	query.getKeyspace = func() string { return keyspace }

	iter := policy.Pick(nil)
	if iter == nil {
		t.Fatal("host iterator was nil")
	}
	actual := iter()
	if actual != nil {
		t.Fatalf("expected nil from iterator, but was %v", actual)
	}

	// set the hosts
	hosts := [...]*HostInfo{
		{hostId: "0", connectAddress: net.IPv4(10, 0, 0, 1), tokens: []string{"05"}, dataCenter: "remote1"},
		{hostId: "1", connectAddress: net.IPv4(10, 0, 0, 2), tokens: []string{"10"}, dataCenter: "local"},
		{hostId: "2", connectAddress: net.IPv4(10, 0, 0, 3), tokens: []string{"15"}, dataCenter: "remote2"},
		{hostId: "3", connectAddress: net.IPv4(10, 0, 0, 4), tokens: []string{"20"}, dataCenter: "remote1"}, // 1
		{hostId: "4", connectAddress: net.IPv4(10, 0, 0, 5), tokens: []string{"25"}, dataCenter: "local"},   // 2
		{hostId: "5", connectAddress: net.IPv4(10, 0, 0, 6), tokens: []string{"30"}, dataCenter: "remote2"}, // 3
		{hostId: "6", connectAddress: net.IPv4(10, 0, 0, 7), tokens: []string{"35"}, dataCenter: "remote1"}, // 4
		{hostId: "7", connectAddress: net.IPv4(10, 0, 0, 8), tokens: []string{"40"}, dataCenter: "local"},   // 5
		{hostId: "8", connectAddress: net.IPv4(10, 0, 0, 9), tokens: []string{"45"}, dataCenter: "remote2"}, // 6
		{hostId: "9", connectAddress: net.IPv4(10, 0, 0, 10), tokens: []string{"50"}, dataCenter: "remote1"},
		{hostId: "10", connectAddress: net.IPv4(10, 0, 0, 11), tokens: []string{"55"}, dataCenter: "local"},
		{hostId: "11", connectAddress: net.IPv4(10, 0, 0, 12), tokens: []string{"60"}, dataCenter: "remote2"},
	}
	for _, host := range hosts {
		policy.AddHost(host)
	}

	policy.SetPartitioner("OrderedPartitioner")

	policyInternal.getKeyspaceMetadata = func(keyspaceName string) (*KeyspaceMetadata, error) {
		if keyspaceName != keyspace {
			return nil, fmt.Errorf("unknown keyspace: %s", keyspaceName)
		}
		return &KeyspaceMetadata{
			Name:          keyspace,
			StrategyClass: "NetworkTopologyStrategy",
			StrategyOptions: map[string]interface{}{
				"class":   "NetworkTopologyStrategy",
				"local":   2,
				"remote1": 2,
				"remote2": 2,
			},
		}, nil
	}
	policy.KeyspaceChanged(KeyspaceUpdateEvent{Keyspace: keyspace})

	// The NetworkTopologyStrategy above should generate the following replicas.
	// It's handy to have as reference here.
	tests.AssertDeepEqual(t, "replicas", map[string]tokenRingReplicas{
		keyspace: {
			{orderedToken("05"), []*HostInfo{hosts[0], hosts[1], hosts[2], hosts[3], hosts[4], hosts[5]}},
			{orderedToken("10"), []*HostInfo{hosts[1], hosts[2], hosts[3], hosts[4], hosts[5], hosts[6]}},
			{orderedToken("15"), []*HostInfo{hosts[2], hosts[3], hosts[4], hosts[5], hosts[6], hosts[7]}},
			{orderedToken("20"), []*HostInfo{hosts[3], hosts[4], hosts[5], hosts[6], hosts[7], hosts[8]}},
			{orderedToken("25"), []*HostInfo{hosts[4], hosts[5], hosts[6], hosts[7], hosts[8], hosts[9]}},
			{orderedToken("30"), []*HostInfo{hosts[5], hosts[6], hosts[7], hosts[8], hosts[9], hosts[10]}},
			{orderedToken("35"), []*HostInfo{hosts[6], hosts[7], hosts[8], hosts[9], hosts[10], hosts[11]}},
			{orderedToken("40"), []*HostInfo{hosts[7], hosts[8], hosts[9], hosts[10], hosts[11], hosts[0]}},
			{orderedToken("45"), []*HostInfo{hosts[8], hosts[9], hosts[10], hosts[11], hosts[0], hosts[1]}},
			{orderedToken("50"), []*HostInfo{hosts[9], hosts[10], hosts[11], hosts[0], hosts[1], hosts[2]}},
			{orderedToken("55"), []*HostInfo{hosts[10], hosts[11], hosts[0], hosts[1], hosts[2], hosts[3]}},
			{orderedToken("60"), []*HostInfo{hosts[11], hosts[0], hosts[1], hosts[2], hosts[3], hosts[4]}},
		},
	}, policyInternal.getMetadataReadOnly().replicas)

	// now the token ring is configured
	query.RoutingKey([]byte("18"))
	iter = policy.Pick(query)
	// first should be hosts with matching token from the local DC
	expectHosts(t, "matching token from local DC", iter, "4", "7")
	// rest should be hosts with matching token from remote DCs
	expectHosts(t, "matching token from remote DCs", iter, "3", "5", "6", "8")
	// followed by other hosts
	expectHosts(t, "rest", iter, "0", "1", "2", "9", "10", "11")
	expectNoMoreHosts(t, iter)
}

func TestHostPolicy_RackAwareRR(t *testing.T) {
	t.Parallel()

	p := RackAwareRoundRobinPolicy("local", "b")

	hosts := [...]*HostInfo{
		{hostId: "0", connectAddress: net.ParseIP("10.0.0.1"), dataCenter: "local", rack: "a"},
		{hostId: "1", connectAddress: net.ParseIP("10.0.0.2"), dataCenter: "local", rack: "a"},
		{hostId: "2", connectAddress: net.ParseIP("10.0.0.3"), dataCenter: "local", rack: "b"},
		{hostId: "3", connectAddress: net.ParseIP("10.0.0.4"), dataCenter: "local", rack: "b"},
		{hostId: "4", connectAddress: net.ParseIP("10.0.0.5"), dataCenter: "remote", rack: "a"},
		{hostId: "5", connectAddress: net.ParseIP("10.0.0.6"), dataCenter: "remote", rack: "a"},
		{hostId: "6", connectAddress: net.ParseIP("10.0.0.7"), dataCenter: "remote", rack: "b"},
		{hostId: "7", connectAddress: net.ParseIP("10.0.0.8"), dataCenter: "remote", rack: "b"},
	}

	for _, host := range hosts {
		p.AddHost(host)
	}

	it := p.Pick(nil)

	// Must start with rack-local hosts
	expectHosts(t, "rack-local hosts", it, "3", "2")
	// Then dc-local hosts
	expectHosts(t, "dc-local hosts", it, "0", "1")
	// Then the remote hosts
	expectHosts(t, "remote hosts", it, "4", "5", "6", "7")
	expectNoMoreHosts(t, it)
}

// Tests of the token-aware host selection policy implementation with a
// DC & Rack aware round-robin host selection policy fallback
func TestHostPolicy_TokenAware_RackAware(t *testing.T) {
	t.Parallel()

	const keyspace = "myKeyspace"
	policy := TokenAwareHostPolicy(RackAwareRoundRobinPolicy("local", "b"))
	policyWithFallback := TokenAwareHostPolicy(RackAwareRoundRobinPolicy("local", "b"), NonLocalReplicasFallback())

	policyInternal := policy.(*tokenAwareHostPolicy)
	policyInternal.getKeyspaceName = func() string { return keyspace }
	policyInternal.getKeyspaceMetadata = func(ks string) (*KeyspaceMetadata, error) {
		return nil, errors.New("not initialized")
	}

	policyWithFallbackInternal := policyWithFallback.(*tokenAwareHostPolicy)
	policyWithFallbackInternal.getKeyspaceName = policyInternal.getKeyspaceName
	policyWithFallbackInternal.getKeyspaceMetadata = policyInternal.getKeyspaceMetadata

	query := &Query{routingInfo: &queryRoutingInfo{}}
	query.getKeyspace = func() string { return keyspace }

	iter := policy.Pick(nil)
	if iter == nil {
		t.Fatal("host iterator was nil")
	}
	actual := iter()
	if actual != nil {
		t.Fatalf("expected nil from iterator, but was %v", actual)
	}

	// set the hosts
	hosts := [...]*HostInfo{
		{hostId: "0", connectAddress: net.IPv4(10, 0, 0, 1), tokens: []string{"05"}, dataCenter: "remote", rack: "a"},
		{hostId: "1", connectAddress: net.IPv4(10, 0, 0, 2), tokens: []string{"10"}, dataCenter: "remote", rack: "b"},
		{hostId: "2", connectAddress: net.IPv4(10, 0, 0, 3), tokens: []string{"15"}, dataCenter: "local", rack: "a"},
		{hostId: "3", connectAddress: net.IPv4(10, 0, 0, 4), tokens: []string{"20"}, dataCenter: "local", rack: "b"},
		{hostId: "4", connectAddress: net.IPv4(10, 0, 0, 5), tokens: []string{"25"}, dataCenter: "remote", rack: "a"},
		{hostId: "5", connectAddress: net.IPv4(10, 0, 0, 6), tokens: []string{"30"}, dataCenter: "remote", rack: "b"},
		{hostId: "6", connectAddress: net.IPv4(10, 0, 0, 7), tokens: []string{"35"}, dataCenter: "local", rack: "a"},
		{hostId: "7", connectAddress: net.IPv4(10, 0, 0, 8), tokens: []string{"40"}, dataCenter: "local", rack: "b"},
		{hostId: "8", connectAddress: net.IPv4(10, 0, 0, 9), tokens: []string{"45"}, dataCenter: "remote", rack: "a"},
		{hostId: "9", connectAddress: net.IPv4(10, 0, 0, 10), tokens: []string{"50"}, dataCenter: "remote", rack: "b"},
		{hostId: "10", connectAddress: net.IPv4(10, 0, 0, 11), tokens: []string{"55"}, dataCenter: "local", rack: "a"},
		{hostId: "11", connectAddress: net.IPv4(10, 0, 0, 12), tokens: []string{"60"}, dataCenter: "local", rack: "b"},
	}
	for _, host := range hosts {
		policy.AddHost(host)
		policyWithFallback.AddHost(host)
	}

	// the token ring is not setup without the partitioner, but the fallback
	// should work
	if actual := policy.Pick(nil)(); actual == nil {
		t.Fatal("expected to get host from fallback got nil")
	}

	query.RoutingKey([]byte("30"))
	if actual := policy.Pick(query)(); actual == nil {
		t.Fatal("expected to get host from fallback got nil")
	}

	policy.SetPartitioner("OrderedPartitioner")
	policyWithFallback.SetPartitioner("OrderedPartitioner")

	policyInternal.getKeyspaceMetadata = func(keyspaceName string) (*KeyspaceMetadata, error) {
		if keyspaceName != keyspace {
			return nil, fmt.Errorf("unknown keyspace: %s", keyspaceName)
		}
		return &KeyspaceMetadata{
			Name:          keyspace,
			StrategyClass: "NetworkTopologyStrategy",
			StrategyOptions: map[string]interface{}{
				"class":  "NetworkTopologyStrategy",
				"local":  2,
				"remote": 2,
			},
		}, nil
	}
	policyWithFallbackInternal.getKeyspaceMetadata = policyInternal.getKeyspaceMetadata
	policy.KeyspaceChanged(KeyspaceUpdateEvent{Keyspace: "myKeyspace"})
	policyWithFallback.KeyspaceChanged(KeyspaceUpdateEvent{Keyspace: "myKeyspace"})

	// The NetworkTopologyStrategy above should generate the following replicas.
	// It's handy to have as reference here.
	tests.AssertDeepEqual(t, "replicas", map[string]tokenRingReplicas{
		"myKeyspace": {
			{orderedToken("05"), []*HostInfo{hosts[0], hosts[1], hosts[2], hosts[3]}},
			{orderedToken("10"), []*HostInfo{hosts[1], hosts[2], hosts[3], hosts[4]}},
			{orderedToken("15"), []*HostInfo{hosts[2], hosts[3], hosts[4], hosts[5]}},
			{orderedToken("20"), []*HostInfo{hosts[3], hosts[4], hosts[5], hosts[6]}},
			{orderedToken("25"), []*HostInfo{hosts[4], hosts[5], hosts[6], hosts[7]}},
			{orderedToken("30"), []*HostInfo{hosts[5], hosts[6], hosts[7], hosts[8]}},
			{orderedToken("35"), []*HostInfo{hosts[6], hosts[7], hosts[8], hosts[9]}},
			{orderedToken("40"), []*HostInfo{hosts[7], hosts[8], hosts[9], hosts[10]}},
			{orderedToken("45"), []*HostInfo{hosts[8], hosts[9], hosts[10], hosts[11]}},
			{orderedToken("50"), []*HostInfo{hosts[9], hosts[10], hosts[11], hosts[0]}},
			{orderedToken("55"), []*HostInfo{hosts[10], hosts[11], hosts[0], hosts[1]}},
			{orderedToken("60"), []*HostInfo{hosts[11], hosts[0], hosts[1], hosts[2]}},
		},
	}, policyInternal.getMetadataReadOnly().replicas)

	query.RoutingKey([]byte("23"))

	// now the token ring is configured
	// Test the policy with fallback
	iter = policyWithFallback.Pick(query)

	// first should be host with matching token from the local DC & rack
	expectHosts(t, "matching token from local DC and local rack", iter, "7")
	// next should be host with matching token from local DC and other rack
	expectHosts(t, "matching token from local DC and non-local rack", iter, "6")
	// next should be hosts with matching token from other DC, in any order
	expectHosts(t, "matching token from non-local DC", iter, "4", "5")
	// then the local DC & rack that didn't match the token
	expectHosts(t, "non-matching token from local DC and local rack", iter, "3", "11")
	// then the local DC & other rack that didn't match the token
	expectHosts(t, "non-matching token from local DC and non-local rack", iter, "2", "10")
	// finally, the other DC that didn't match the token
	expectHosts(t, "non-matching token from non-local DC", iter, "0", "1", "8", "9")
	expectNoMoreHosts(t, iter)

	// Test the policy without fallback
	iter = policy.Pick(query)

	// first should be host with matching token from the local DC & Rack
	expectHosts(t, "matching token from local DC and local rack", iter, "7")
	// next should be the other two hosts from local DC & rack
	expectHosts(t, "non-matching token local DC and local rack", iter, "3", "11")
	// then the three hosts from the local DC but other rack
	expectHosts(t, "local DC, non-local rack", iter, "2", "6", "10")
	// then the 6 hosts from the other DC
	expectHosts(t, "non-local DC", iter, "0", "1", "4", "5", "8", "9")
	expectNoMoreHosts(t, iter)
}

func TestHostPolicy_TokenAware_Issue1274(t *testing.T) {
	t.Parallel()

	policy := TokenAwareHostPolicy(DCAwareRoundRobinPolicy("local"))
	policyInternal := policy.(*tokenAwareHostPolicy)
	policyInternal.getKeyspaceName = func() string { return "myKeyspace" }
	policyInternal.getKeyspaceMetadata = func(ks string) (*KeyspaceMetadata, error) {
		return nil, errors.New("not initialized")
	}

	query := &Query{routingInfo: &queryRoutingInfo{}}
	query.getKeyspace = func() string { return "myKeyspace" }

	iter := policy.Pick(nil)
	if iter == nil {
		t.Fatal("host iterator was nil")
	}
	actual := iter()
	if actual != nil {
		t.Fatalf("expected nil from iterator, but was %v", actual)
	}

	// set the hosts
	hosts := [...]*HostInfo{
		{hostId: "0", connectAddress: net.IPv4(10, 0, 0, 1), tokens: []string{"05"}, dataCenter: "remote1"},
		{hostId: "1", connectAddress: net.IPv4(10, 0, 0, 2), tokens: []string{"10"}, dataCenter: "local"},
		{hostId: "2", connectAddress: net.IPv4(10, 0, 0, 3), tokens: []string{"15"}, dataCenter: "remote2"},
		{hostId: "3", connectAddress: net.IPv4(10, 0, 0, 4), tokens: []string{"20"}, dataCenter: "remote1"},
		{hostId: "4", connectAddress: net.IPv4(10, 0, 0, 5), tokens: []string{"25"}, dataCenter: "local"},
		{hostId: "5", connectAddress: net.IPv4(10, 0, 0, 6), tokens: []string{"30"}, dataCenter: "remote2"},
		{hostId: "6", connectAddress: net.IPv4(10, 0, 0, 7), tokens: []string{"35"}, dataCenter: "remote1"},
		{hostId: "7", connectAddress: net.IPv4(10, 0, 0, 8), tokens: []string{"40"}, dataCenter: "local"},
		{hostId: "8", connectAddress: net.IPv4(10, 0, 0, 9), tokens: []string{"45"}, dataCenter: "remote2"},
		{hostId: "9", connectAddress: net.IPv4(10, 0, 0, 10), tokens: []string{"50"}, dataCenter: "remote1"},
		{hostId: "10", connectAddress: net.IPv4(10, 0, 0, 11), tokens: []string{"55"}, dataCenter: "local"},
		{hostId: "11", connectAddress: net.IPv4(10, 0, 0, 12), tokens: []string{"60"}, dataCenter: "remote2"},
	}

	policy.SetPartitioner("OrderedPartitioner")

	policyInternal.getKeyspaceMetadata = func(keyspaceName string) (*KeyspaceMetadata, error) {
		if keyspaceName != "myKeyspace" {
			return nil, fmt.Errorf("unknown keyspace: %s", keyspaceName)
		}
		return &KeyspaceMetadata{
			Name:          "myKeyspace",
			StrategyClass: "NetworkTopologyStrategy",
			StrategyOptions: map[string]interface{}{
				"class":   "NetworkTopologyStrategy",
				"local":   1,
				"remote1": 1,
				"remote2": 1,
			},
		}, nil
	}
	policy.KeyspaceChanged(KeyspaceUpdateEvent{Keyspace: "myKeyspace"})

	cancel := make(chan struct{})

	// now the token ring is configured
	for _, host := range hosts {
		host := host
		go func() {
			for {
				select {
				case <-cancel:
					return
				default:
					policy.AddHost(host)
					policy.RemoveHost(host)
				}
			}
		}()
	}

	time.Sleep(100 * time.Millisecond)
	close(cancel)
}

func TestTokenAwarePolicyReset(t *testing.T) {
	t.Parallel()

	policy := TokenAwareHostPolicy(
		RackAwareRoundRobinPolicy("local", "b"),
		NonLocalReplicasFallback(),
	)
	policyInternal := policy.(*tokenAwareHostPolicy)

	if policyInternal.fallback == nil {
		t.Fatal("fallback is nil")
	}
	if !policyInternal.nonLocalReplicasFallback {
		t.Fatal("nonLocalReplicasFallback is false")
	}

	policy.Init(&Session{logger: &defaultLogger{}})
	if policyInternal.getKeyspaceMetadata == nil {
		t.Fatal("keyspace metatadata fn is nil")
	}
	if policyInternal.getKeyspaceName == nil {
		t.Fatal("keyspace name fn is nil")
	}
	if policyInternal.logger == nil {
		t.Fatal("logger is nil")
	}

	// Reset - should reset fields that were set in Init
	policy.Reset()

	if policyInternal.fallback == nil { // we don't touch fallback
		t.Fatal("fallback is nil")
	}
	if !policyInternal.nonLocalReplicasFallback { // we don't touch nonLocalReplicasFallback
		t.Fatal("nonLocalReplicasFallback is false")
	}
	if policyInternal.getKeyspaceMetadata != nil {
		t.Fatal("keyspace metatadata fn is not nil")
	}
	if policyInternal.getKeyspaceName != nil {
		t.Fatal("keyspace name fn is not nil")
	}
	if policyInternal.logger != nil {
		t.Fatal("logger is nil")
	}
}
