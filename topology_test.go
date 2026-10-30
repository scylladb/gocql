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

import implEndpoint "github.com/scylladb/gocql/v2/internal/endpoint"

import (
	"fmt"
	"math"
	"sort"
	"testing"
)

func TestPlacementStrategy_SimpleStrategy(t *testing.T) {
	t.Parallel()

	host0 := func() *HostInfo {
		opaqueStorageValue := new(HostInfo)
		implEndpoint.HostInfoData(opaqueStorageValue).InternalHostId = tUUID(0)
		return opaqueStorageValue
	}()
	host25 := func() *HostInfo {
		opaqueStorageValuex := new(HostInfo)
		implEndpoint.HostInfoData(opaqueStorageValuex).InternalHostId = tUUID(25)
		return opaqueStorageValuex
	}()
	host50 := func() *HostInfo {
		opaqueStorageValuexx := new(HostInfo)
		implEndpoint.HostInfoData(opaqueStorageValuexx).InternalHostId = tUUID(50)
		return opaqueStorageValuexx
	}()
	host75 := func() *HostInfo {
		opaqueStorageValuexxx := new(HostInfo)
		implEndpoint.HostInfoData(opaqueStorageValuexxx).InternalHostId = tUUID(75)
		return opaqueStorageValuexxx
	}()

	tokens := []hostToken{
		{InternalToken: intToken(0), InternalHost: host0},
		{InternalToken: intToken(25), InternalHost: host25},
		{InternalToken: intToken(50), InternalHost: host50},
		{InternalToken: intToken(75), InternalHost: host75},
	}

	hosts := []*HostInfo{host0, host25, host50, host75}

	strat := &simpleStrategy{InternalRf: 2}
	tokenReplicas := strat.InternalReplicaMap(&tokenRing{InternalHosts: hosts, InternalTokens: tokens})
	if len(tokenReplicas) != len(tokens) {
		t.Fatalf("expected replica map to have %d items but has %d", len(tokens), len(tokenReplicas))
	}

	for _, replicas := range tokenReplicas {
		if len(replicas.InternalHosts) != strat.InternalRf {
			t.Errorf("expected to have %d replicas got %d for token=%v", strat.InternalRf, len(replicas.InternalHosts), replicas.InternalToken)
		}
	}

	for i, token := range tokens {
		ht := tokenReplicas.InternalReplicasFor(token.InternalToken)
		if ht.InternalToken != token.InternalToken {
			t.Errorf("token %v not in replica map: %v", token, ht.InternalHosts)
		}

		for j, replica := range ht.InternalHosts {
			exp := tokens[(i+j)%len(tokens)].InternalHost
			if exp != replica {
				t.Errorf("expected host %v to be a replica of %v got %v", implEndpoint.HostInfoData(exp).InternalHostId, token, implEndpoint.HostInfoData(replica).InternalHostId)
			}
		}
	}
}

func TestPlacementStrategy_NetworkStrategy(t *testing.T) {
	t.Parallel()

	const (
		totalDCs   = 3
		racksPerDC = 3
		hostsPerDC = 5
	)

	tests := []struct {
		name                   string
		strat                  *networkTopology
		expectedReplicaMapSize int
	}{
		{
			name: "full",
			strat: &networkTopology{
				InternalDcs: map[string]int{
					"dc1": 1,
					"dc2": 2,
					"dc3": 3,
				},
			},
			expectedReplicaMapSize: hostsPerDC * totalDCs,
		},
		{
			name: "missing",
			strat: &networkTopology{
				InternalDcs: map[string]int{
					"dc2": 2,
					"dc3": 3,
				},
			},
			expectedReplicaMapSize: hostsPerDC * 2,
		},
		{
			name: "zero",
			strat: &networkTopology{
				InternalDcs: map[string]int{
					"dc1": 0,
					"dc2": 2,
					"dc3": 3,
				},
			},
			expectedReplicaMapSize: hostsPerDC * 2,
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			var (
				hosts  []*HostInfo
				tokens []hostToken
			)
			dcRing := make(map[string][]hostToken, totalDCs)
			hostIdx := 0
			for i := 0; i < totalDCs; i++ {
				var dcTokens []hostToken
				dc := fmt.Sprintf("dc%d", i+1)

				for j := 0; j < hostsPerDC; j++ {
					rack := fmt.Sprintf("rack%d", (j%racksPerDC)+1)
					tokenStr := fmt.Sprintf("%s:%s:%d", dc, rack, j)

					h := func() *HostInfo {
						opaqueStorageValuexxxx := new(HostInfo)
						implEndpoint.HostInfoData(opaqueStorageValuexxxx).InternalHostId = tUUID(hostIdx)
						implEndpoint.HostInfoData(opaqueStorageValuexxxx).InternalDataCenter = dc
						implEndpoint.HostInfoData(opaqueStorageValuexxxx).InternalRack = rack
						return opaqueStorageValuexxxx
					}()
					hostIdx++

					token := hostToken{
						InternalToken: orderedToken(tokenStr),
						InternalHost:  h,
					}

					tokens = append(tokens, token)
					dcTokens = append(dcTokens, token)

					hosts = append(hosts, h)
				}

				sort.Sort(&tokenRing{InternalTokens: dcTokens})
				dcRing[dc] = dcTokens
			}

			if len(tokens) != hostsPerDC*totalDCs {
				t.Fatalf("expected %d tokens in the ring got %d", hostsPerDC*totalDCs, len(tokens))
			}
			sort.Sort(&tokenRing{InternalTokens: tokens})

			var expReplicas int
			for _, rf := range test.strat.InternalDcs {
				expReplicas += rf
			}

			tokenReplicas := test.strat.InternalReplicaMap(&tokenRing{InternalHosts: hosts, InternalTokens: tokens})
			if len(tokenReplicas) != test.expectedReplicaMapSize {
				t.Fatalf("expected replica map to have %d items but has %d", test.expectedReplicaMapSize,
					len(tokenReplicas))
			}
			if !sort.IsSorted(tokenReplicas) {
				t.Fatal("replica map was not sorted by token")
			}

			for token, replicas := range tokenReplicas {
				if len(replicas.InternalHosts) != expReplicas {
					t.Fatalf("expected to have %d replicas got %d for token=%v", expReplicas, len(replicas.InternalHosts), token)
				}
			}

			for dc, rf := range test.strat.InternalDcs {
				if rf == 0 {
					continue
				}
				dcTokens := dcRing[dc]
				for i, th := range dcTokens {
					token := th.InternalToken
					allReplicas := tokenReplicas.InternalReplicasFor(token)
					if allReplicas.InternalToken != token {
						t.Fatalf("token %v not in replica map", token)
					}

					var replicas []*HostInfo
					for _, replica := range allReplicas.InternalHosts {
						if implEndpoint.HostInfoData(replica).InternalDataCenter == dc {
							replicas = append(replicas, replica)
						}
					}

					if len(replicas) != rf {
						t.Fatalf("expected %d replicas in dc %q got %d", rf, dc, len(replicas))
					}

					var lastRack string
					for j, replica := range replicas {
						// expected is in the next rack
						var exp *HostInfo
						if lastRack == "" {
							// primary, first replica
							exp = dcTokens[(i+j)%len(dcTokens)].InternalHost
						} else {
							for k := 0; k < len(dcTokens); k++ {
								// walk around the ring from i + j to find the next host the
								// next rack
								p := (i + j + k) % len(dcTokens)
								h := dcTokens[p].InternalHost
								if implEndpoint.HostInfoData(h).InternalRack != lastRack {
									exp = h
									break
								}
							}
							if implEndpoint.HostInfoData(exp).InternalRack == lastRack {
								t.Fatal("no more racks")
							}
						}
						lastRack = implEndpoint.HostInfoData(replica).InternalRack
					}
				}
			}
		})
	}
}

// TestTokenRingReplicas_ReplicasForInt64MatchesReplicasFor checks replicasForInt64
// matches replicasFor for every query token, including boundaries and wraparound.
func TestTokenRingReplicas_ReplicasForInt64MatchesReplicasFor(t *testing.T) {
	t.Parallel()

	host0 := func() *HostInfo {
		opaqueStorageValuexxxxx := new(HostInfo)
		implEndpoint.HostInfoData(opaqueStorageValuexxxxx).InternalHostId = tUUID(0)
		return opaqueStorageValuexxxxx
	}()
	host1 := func() *HostInfo {
		opaqueStorageValuexxxxxx := new(HostInfo)
		implEndpoint.HostInfoData(opaqueStorageValuexxxxxx).InternalHostId = tUUID(1)
		return opaqueStorageValuexxxxxx
	}()
	host2 := func() *HostInfo {
		opaqueStorageValuexxxxxxx := new(HostInfo)
		implEndpoint.HostInfoData(opaqueStorageValuexxxxxxx).InternalHostId = tUUID(2)
		return opaqueStorageValuexxxxxxx
	}()

	replicas := tokenRingReplicas{
		{InternalToken: int64Token(-100), InternalHosts: []*HostInfo{host0}},
		{InternalToken: int64Token(0), InternalHosts: []*HostInfo{host1}},
		{InternalToken: int64Token(100), InternalHosts: []*HostInfo{host2}},
	}

	queries := []int64Token{
		math.MinInt64, -1000, -101, -100, -99, -50, -1,
		0, 1, 50, 99, 100, 101, 1000, math.MaxInt64,
	}
	for _, q := range queries {
		want := replicas.InternalReplicasFor(q)
		got := replicas.InternalReplicasForInt64(q)
		if want != got {
			t.Errorf("token %d: replicasFor=%p (ring token %v) replicasForInt64=%p (ring token %v) diverge",
				q, want, tokensOf(want), got, tokensOf(got))
		}
	}
}

// tokensOf is a small test helper to render a *hostTokens' token for
// error messages without panicking on nil.
func tokensOf(h *hostTokens) any {
	if h == nil {
		return nil
	}
	return h.InternalToken
}

func TestTokenRingReplicas_ReplicasForInt64_EmptyRing(t *testing.T) {
	t.Parallel()

	var replicas tokenRingReplicas
	if got := replicas.InternalReplicasForInt64(42); got != nil {
		t.Fatalf("expected nil for an empty ring, got %v", got)
	}
}

// TestTokenRingReplicas_ReplicasForInt64_MismatchedTokenTypePanicsLikeReplicasFor
// checks a mismatched ring token type panics the same way replicasFor does.
func TestTokenRingReplicas_ReplicasForInt64_MismatchedTokenTypePanicsLikeReplicasFor(t *testing.T) {
	t.Parallel()

	host0 := func() *HostInfo {
		opaqueStorageValuexxxxxxxx := new(HostInfo)
		implEndpoint.HostInfoData(opaqueStorageValuexxxxxxxx).InternalHostId = tUUID(0)
		return opaqueStorageValuexxxxxxxx
	}()
	replicas := tokenRingReplicas{
		{InternalToken: intToken(0), InternalHosts: []*HostInfo{host0}},
	}

	mustPanic := func(t *testing.T, name string, fn func()) {
		defer func() {
			if recover() == nil {
				t.Errorf("%s: expected a panic from the mismatched Token type, got none", name)
			}
		}()
		fn()
	}

	mustPanic(t, "replicasFor", func() { replicas.InternalReplicasFor(int64Token(50)) })
	mustPanic(t, "replicasForInt64", func() { replicas.InternalReplicasForInt64(int64Token(50)) })
}
