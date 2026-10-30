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

import implCluster "github.com/scylladb/gocql/v2/internal/cluster"

import implEndpoint "github.com/scylladb/gocql/v2/internal/endpoint"

import (
	"errors"
	"fmt"
	"net"
	"sort"
	"strings"
	"testing"
)

// tUUID returns a deterministic UUID for testing. Byte 0 is always set to
// a non-zero sentinel (0xFE) so that even tUUID(0) is distinguishable from
// the zero UUID, and the last two bytes encode n.
func tUUID(n int) UUID {
	var u UUID
	u[0] = 0xFE
	u[14] = byte(n >> 8)
	u[15] = byte(n)
	return u
}

// tID returns the string representation of tUUID(n), suitable for passing to
// expectHosts and other string-based comparisons.
func tID(n int) string {
	return tUUID(n).String()
}

func createPolicy(keyspace string, shuffle bool) HostSelectionPolicy {
	policy := TokenAwareHostPolicy(RoundRobinHostPolicy())
	policyInternal := policy.(*tokenAwareHostPolicy)
	implCluster.InternalTokenAwareHostPolicyData(policyInternal).InternalGetKeyspaceName = func() string { return keyspace }
	implCluster.InternalTokenAwareHostPolicyData(policyInternal).InternalGetKeyspaceMetadata = func(ks string) (*KeyspaceMetadata, error) {
		return nil, errors.New("not initalized")
	}
	policy.SetPartitioner("OrderedPartitioner")
	implCluster.InternalTokenAwareHostPolicyData(policyInternal).InternalGetKeyspaceMetadata = func(keyspaceName string) (*KeyspaceMetadata, error) {
		if keyspaceName != keyspace {
			return nil, fmt.Errorf("unknown keyspace: %s", keyspaceName)
		}
		return &KeyspaceMetadata{
			Name:          keyspace,
			StrategyClass: "SimpleStrategy",
			StrategyOptions: map[string]any{
				"class":              "SimpleStrategy",
				"replication_factor": 2,
			},
		}, nil
	}
	implCluster.InternalTokenAwareHostPolicyData(policyInternal).InternalShuffleReplicas = shuffle
	policy.KeyspaceChanged(KeyspaceUpdateEvent{Keyspace: keyspace})
	return policy
}

func TestCOWList_Add(t *testing.T) {
	t.Parallel()

	var cow cowHostList

	toAdd := [...]net.IP{net.IPv4(10, 0, 0, 1), net.IPv4(10, 0, 0, 2), net.IPv4(10, 0, 0, 3)}

	for _, addr := range toAdd {
		if !cow.InternalAdd(func() *HostInfo {
			opaqueStorageValue := new(HostInfo)
			implEndpoint.HostInfoData(opaqueStorageValue).InternalConnectAddress = addr
			return opaqueStorageValue
		}()) {
			t.Fatal("did not add peer which was not in the set")
		}
	}

	hosts := cow.InternalGet().InternalAllHosts()
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

func TestHostInfoList_HostByID(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		hosts   []*HostInfo
		wantMap bool
	}{
		{
			name:    "linear below threshold",
			hosts:   makeHostInfoListTestHosts(hostInfoListMapThreshold - 1),
			wantMap: false,
		},
		{
			name:    "map at threshold",
			hosts:   makeHostInfoListTestHosts(hostInfoListMapThreshold),
			wantMap: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			hosts := newHostInfoList(tt.hosts)
			if (hosts.InternalHostsByID != nil) != tt.wantMap {
				t.Fatalf("hostsByID presence = %v, want %v", hosts.InternalHostsByID != nil, tt.wantMap)
			}
			if tt.wantMap {
				if hosts.InternalHostIDs != nil {
					t.Fatalf("hostIDs = %v, want nil when hostsByID is used", hosts.InternalHostIDs)
				}
			} else if len(hosts.InternalHostIDs) != len(tt.hosts) {
				t.Fatalf("len(hostIDs) = %d, want %d", len(hosts.InternalHostIDs), len(tt.hosts))
			}

			for _, host := range tt.hosts {
				if got := hosts.InternalHostByID(implEndpoint.InternalHostInfoHostUUID(host)); got != host {
					t.Fatalf("hostByID(%s) = %v, want %v", host.HostID(), got, host)
				}
			}

			if got := hosts.InternalHostByID(tUUID(9999)); got != nil {
				t.Fatalf("hostByID(missing) = %v, want nil", got)
			}
			if got := hosts.InternalHostByID(UUID{}); got != nil {
				t.Fatalf("hostByID(zero) = %v, want nil", got)
			}
		})
	}
}

func TestHostInfoList_AllHostsClipsCapacity(t *testing.T) {
	t.Parallel()

	original := makeHostInfoListTestHosts(2)
	withSpareCapacity := make([]*HostInfo, len(original), len(original)+1)
	copy(withSpareCapacity, original)

	hosts := newHostInfoList(withSpareCapacity)
	snapshot := hosts.InternalAllHosts()
	appended := append(snapshot, func() *HostInfo {
		opaqueStorageValuex := new(HostInfo)
		implEndpoint.HostInfoData(opaqueStorageValuex).InternalHostId = tUUID(100)
		return opaqueStorageValuex
	}())

	if len(snapshot) != len(original) {
		t.Fatalf("len(allHosts()) = %d, want %d", len(snapshot), len(original))
	}
	if len(appended) != len(original)+1 {
		t.Fatalf("len(appended) = %d, want %d", len(appended), len(original)+1)
	}
	if &snapshot[0] == &appended[0] {
		t.Fatal("append reused allHosts backing array")
	}
}

func TestCOWList_AddAll(t *testing.T) {
	t.Parallel()

	var cow cowHostList
	hosts := makeHostInfoListTestHosts(hostInfoListMapThreshold)

	if !cow.InternalAddAll(hosts) {
		t.Fatal("did not add hosts which were not in the set")
	}
	if got := cow.InternalGet().InternalLen(); got != len(hosts) {
		t.Fatalf("len() = %d, want %d", got, len(hosts))
	}
	if cow.InternalGet().InternalHostsByID == nil {
		t.Fatal("expected host ID map at threshold")
	}
	for _, host := range hosts {
		if got := cow.InternalGet().InternalHostByID(implEndpoint.InternalHostInfoHostUUID(host)); got != host {
			t.Fatalf("hostByID(%s) = %v, want %v", host.HostID(), got, host)
		}
	}
	if cow.InternalAddAll(hosts) {
		t.Fatal("added duplicate hosts")
	}
}

func makeHostInfoListTestHosts(n int) []*HostInfo {
	hosts := make([]*HostInfo, n)
	for i := range hosts {
		hosts[i] = func() *HostInfo {
			opaqueStorageValuexx := new(HostInfo)
			implEndpoint.HostInfoData(opaqueStorageValuexx).InternalHostId = tUUID(i + 1)
			implEndpoint.HostInfoData(opaqueStorageValuexx).InternalConnectAddress = net.IPv4(10, 0, byte(i>>8), byte(i))
			implEndpoint.HostInfoData(opaqueStorageValuexx).InternalPort = 9042
			return opaqueStorageValuexx
		}(

		// expectHosts makes sure that the next len(hostIDs) returned from iter is a permutation of hostIDs.
		)

	}
	return hosts
}

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

func TestTokenAwarePolicyReset(t *testing.T) {
	t.Parallel()

	policy := TokenAwareHostPolicy(
		RackAwareRoundRobinPolicy("local", "b"),
		NonLocalReplicasFallback(),
	)
	policyInternal := policy.(*tokenAwareHostPolicy)

	if implCluster.InternalTokenAwareHostPolicyData(policyInternal).InternalFallback == nil {
		t.Fatal("fallback is nil")
	}
	if !implCluster.InternalTokenAwareHostPolicyData(policyInternal).InternalNonLocalReplicasFallback {
		t.Fatal("nonLocalReplicasFallback is false")
	}

	policy.Init(func() *Session {
		opaqueStorageValuexxx := new(Session)
		implCluster.SessionData(opaqueStorageValuexxx).InternalLogger = &defaultLogger{}
		return opaqueStorageValuexxx
	}())
	if implCluster.InternalTokenAwareHostPolicyData(policyInternal).InternalGetKeyspaceMetadata == nil {
		t.Fatal("keyspace metatadata fn is nil")
	}
	if implCluster.InternalTokenAwareHostPolicyData(policyInternal).InternalGetKeyspaceName == nil {
		t.Fatal("keyspace name fn is nil")
	}
	if implCluster.InternalTokenAwareHostPolicyData(policyInternal).InternalLogger == nil {
		t.Fatal("logger is nil")
	}

	// Reset - should reset fields that were set in Init
	policy.Reset()

	if implCluster.InternalTokenAwareHostPolicyData(policyInternal).InternalFallback == nil { // we don't touch fallback
		t.Fatal("fallback is nil")
	}
	if !implCluster.InternalTokenAwareHostPolicyData(policyInternal).InternalNonLocalReplicasFallback { // we don't touch nonLocalReplicasFallback
		t.Fatal("nonLocalReplicasFallback is false")
	}
	if implCluster.InternalTokenAwareHostPolicyData(policyInternal).InternalGetKeyspaceMetadata != nil {
		t.Fatal("keyspace metatadata fn is not nil")
	}
	if implCluster.InternalTokenAwareHostPolicyData(policyInternal).InternalGetKeyspaceName != nil {
		t.Fatal("keyspace name fn is not nil")
	}
	if implCluster.InternalTokenAwareHostPolicyData(policyInternal).InternalLogger != nil {
		t.Fatal("logger is nil")
	}
}

// TestSelectedHostTokenInt64 verifies the unboxed routing token: TokenInt64()
// returns the raw int64, and Token() lazily boxes to a non-nil value so the
// shard-aware invariant (a nil token would disable shard-aware picking) holds
// for any caller, not just the raw fast path.
func TestSelectedHostTokenInt64(t *testing.T) {
	t.Parallel()

	sh := int64SelectedHost{InternalInfo: func() *HostInfo {
		opaqueStorageValuexxxx := new(HostInfo)
		return opaqueStorageValuexxxx
	}(), InternalTokenCasted: int64Token(42)}
	tok, ok := sh.TokenInt64()
	if !ok || tok != int64Token(42) {
		t.Fatalf("TokenInt64() = %d, %v; want 42, true", tok, ok)
	}
	if got := sh.Token(); got == nil || got != int64Token(42) {
		t.Fatalf("Token() = %v; want boxed int64Token(42)", got)
	}
	if got := sh.Token(); got == nil || got != int64Token(42) {
		t.Fatalf("Token() (again) = %v; want boxed int64Token(42)", got)
	}

	shBoxed := selectedHost{InternalInfo: func() *HostInfo {
		opaqueStorageValuexxxxx := new(HostInfo)
		return opaqueStorageValuexxxxx
	}(), InternalToken: int64Token(7)}
	if tok, ok := shBoxed.TokenInt64(); ok || tok != 0 {
		t.Fatalf("TokenInt64() = %d, %v; want 0, false for a boxed-only host", tok, ok)
	}
	if got := shBoxed.Token(); got != int64Token(7) {
		t.Fatalf("Token() = %v; want boxed int64Token(7)", got)
	}
}

type fixedInt64Partitioner int64

func (f fixedInt64Partitioner) Name() string               { return "FixedInt64Partitioner" }
func (f fixedInt64Partitioner) Hash([]byte) Token          { return int64Token(f) }
func (f fixedInt64Partitioner) ParseString(s string) Token { return parseInt64Token(s) }

func TestHostSetInline(t *testing.T) {
	var s hostSet
	hosts := make([]*HostInfo, 9)
	for i := range hosts {
		hosts[i] = func() *HostInfo {
			opaqueStorageValuexxxxxx := new(HostInfo)
			return opaqueStorageValuexxxxxx
		}()

		s.InternalAdd(hosts[i])
	}
	// All 9 should be tracked inline (no overflow map).
	if s.InternalOverflow != nil {
		t.Fatal("expected inline-only storage for 9 hosts")
	}
	for i, h := range hosts {
		if !s.InternalContains(h) {
			t.Fatalf("host %d not found in inline set", i)
		}
	}
	// Unknown host should not be found.
	if s.InternalContains(func() *HostInfo {
		opaqueStorageValuexxxxxxx := new(HostInfo)
		return opaqueStorageValuexxxxxxx
	}(),
	) {
		t.Fatal("unexpected contains=true for unknown host")
	}
}

func TestHostSetOverflow(t *testing.T) {
	var s hostSet
	hosts := make([]*HostInfo, 15) // exceeds inline capacity of 9
	for i := range hosts {
		hosts[i] = func() *HostInfo {
			opaqueStorageValuexxxxxxxx := new(HostInfo)
			return opaqueStorageValuexxxxxxxx
		}()

		s.InternalAdd(hosts[i])
	}
	// Should have spilled to map.
	if s.InternalOverflow == nil {
		t.Fatal("expected overflow map for 15 hosts")
	}
	// Every host must be found, including those added before and after spill.
	for i, h := range hosts {
		if !s.InternalContains(h) {
			t.Fatalf("host %d not found after overflow", i)
		}
	}
	// Unknown host should not be found.
	if s.InternalContains(func() *HostInfo {
		opaqueStorageValuexxxxxxxxx := new(HostInfo)
		return opaqueStorageValuexxxxxxxxx
	}(),
	) {
		t.Fatal("unexpected contains=true for unknown host in overflow mode")
	}
}

func TestHostSetOverflowPreservesInlineEntries(t *testing.T) {
	var s hostSet
	// Fill inline storage exactly.
	inline := make([]*HostInfo, 9)
	for i := range inline {
		inline[i] = func() *HostInfo {
			opaqueStorageValuexxxxxxxxxx := new(HostInfo)
			return opaqueStorageValuexxxxxxxxxx
		}()

		s.InternalAdd(inline[i])
	}
	// Add one more to trigger spill.
	extra := func() *HostInfo {
		opaqueStorageValuexxxxxxxxxxx := new(HostInfo)
		return opaqueStorageValuexxxxxxxxxxx
	}()

	s.InternalAdd(extra)

	if s.InternalOverflow == nil {
		t.Fatal("expected overflow map after 10th add")
	}
	// Inline entries must be findable via the map path.
	for i, h := range inline {
		if !s.InternalContains(h) {
			t.Fatalf("inline host %d lost after spill", i)
		}
	}
	if !s.InternalContains(extra) {
		t.Fatal("extra host not found after spill")
	}
}
