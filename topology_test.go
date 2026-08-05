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
	"fmt"
	"math"
	"sort"
	"testing"
)

func TestPlacementStrategy_SimpleStrategy(t *testing.T) {
	t.Parallel()

	host0 := &HostInfo{hostId: tUUID(0)}
	host25 := &HostInfo{hostId: tUUID(25)}
	host50 := &HostInfo{hostId: tUUID(50)}
	host75 := &HostInfo{hostId: tUUID(75)}

	tokens := []hostToken{
		{intToken(0), host0},
		{intToken(25), host25},
		{intToken(50), host50},
		{intToken(75), host75},
	}

	hosts := []*HostInfo{host0, host25, host50, host75}

	strat := &simpleStrategy{rf: 2}
	tokenReplicas := strat.replicaMap(&tokenRing{hosts: hosts, tokens: tokens})
	if len(tokenReplicas) != len(tokens) {
		t.Fatalf("expected replica map to have %d items but has %d", len(tokens), len(tokenReplicas))
	}

	for _, replicas := range tokenReplicas {
		if len(replicas.hosts) != strat.rf {
			t.Errorf("expected to have %d replicas got %d for token=%v", strat.rf, len(replicas.hosts), replicas.token)
		}
	}

	for i, token := range tokens {
		ht := tokenReplicas.replicasFor(token.token)
		if ht.token != token.token {
			t.Errorf("token %v not in replica map: %v", token, ht.hosts)
		}

		for j, replica := range ht.hosts {
			exp := tokens[(i+j)%len(tokens)].host
			if exp != replica {
				t.Errorf("expected host %v to be a replica of %v got %v", exp.hostId, token, replica.hostId)
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
				dcs: map[string]int{
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
				dcs: map[string]int{
					"dc2": 2,
					"dc3": 3,
				},
			},
			expectedReplicaMapSize: hostsPerDC * 2,
		},
		{
			name: "zero",
			strat: &networkTopology{
				dcs: map[string]int{
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

					h := &HostInfo{hostId: tUUID(hostIdx), dataCenter: dc, rack: rack}
					hostIdx++

					token := hostToken{
						token: orderedToken(tokenStr),
						host:  h,
					}

					tokens = append(tokens, token)
					dcTokens = append(dcTokens, token)

					hosts = append(hosts, h)
				}

				sort.Sort(&tokenRing{tokens: dcTokens})
				dcRing[dc] = dcTokens
			}

			if len(tokens) != hostsPerDC*totalDCs {
				t.Fatalf("expected %d tokens in the ring got %d", hostsPerDC*totalDCs, len(tokens))
			}
			sort.Sort(&tokenRing{tokens: tokens})

			var expReplicas int
			for _, rf := range test.strat.dcs {
				expReplicas += rf
			}

			tokenReplicas := test.strat.replicaMap(&tokenRing{hosts: hosts, tokens: tokens})
			if len(tokenReplicas) != test.expectedReplicaMapSize {
				t.Fatalf("expected replica map to have %d items but has %d", test.expectedReplicaMapSize,
					len(tokenReplicas))
			}
			if !sort.IsSorted(tokenReplicas) {
				t.Fatal("replica map was not sorted by token")
			}

			for token, replicas := range tokenReplicas {
				if len(replicas.hosts) != expReplicas {
					t.Fatalf("expected to have %d replicas got %d for token=%v", expReplicas, len(replicas.hosts), token)
				}
			}

			for dc, rf := range test.strat.dcs {
				if rf == 0 {
					continue
				}
				dcTokens := dcRing[dc]
				for i, th := range dcTokens {
					token := th.token
					allReplicas := tokenReplicas.replicasFor(token)
					if allReplicas.token != token {
						t.Fatalf("token %v not in replica map", token)
					}

					var replicas []*HostInfo
					for _, replica := range allReplicas.hosts {
						if replica.dataCenter == dc {
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
							exp = dcTokens[(i+j)%len(dcTokens)].host
						} else {
							for k := 0; k < len(dcTokens); k++ {
								// walk around the ring from i + j to find the next host the
								// next rack
								p := (i + j + k) % len(dcTokens)
								h := dcTokens[p].host
								if h.rack != lastRack {
									exp = h
									break
								}
							}
							if exp.rack == lastRack {
								t.Fatal("no more racks")
							}
						}
						lastRack = replica.rack
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

	host0 := &HostInfo{hostId: tUUID(0)}
	host1 := &HostInfo{hostId: tUUID(1)}
	host2 := &HostInfo{hostId: tUUID(2)}

	replicas := tokenRingReplicas{
		{token: int64Token(-100), hosts: []*HostInfo{host0}},
		{token: int64Token(0), hosts: []*HostInfo{host1}},
		{token: int64Token(100), hosts: []*HostInfo{host2}},
	}

	queries := []int64Token{
		math.MinInt64, -1000, -101, -100, -99, -50, -1,
		0, 1, 50, 99, 100, 101, 1000, math.MaxInt64,
	}
	for _, q := range queries {
		want := replicas.replicasFor(q)
		got := replicas.replicasForInt64(q)
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
	return h.token
}

func TestTokenRingReplicas_ReplicasForInt64_EmptyRing(t *testing.T) {
	t.Parallel()

	var replicas tokenRingReplicas
	if got := replicas.replicasForInt64(42); got != nil {
		t.Fatalf("expected nil for an empty ring, got %v", got)
	}
}

// TestTokenRingReplicas_ReplicasForInt64_MismatchedTokenTypePanicsLikeReplicasFor
// checks a mismatched ring token type panics the same way replicasFor does.
func TestTokenRingReplicas_ReplicasForInt64_MismatchedTokenTypePanicsLikeReplicasFor(t *testing.T) {
	t.Parallel()

	host0 := &HostInfo{hostId: tUUID(0)}
	replicas := tokenRingReplicas{
		{token: intToken(0), hosts: []*HostInfo{host0}},
	}

	mustPanic := func(t *testing.T, name string, fn func()) {
		defer func() {
			if recover() == nil {
				t.Errorf("%s: expected a panic from the mismatched Token type, got none", name)
			}
		}()
		fn()
	}

	mustPanic(t, "replicasFor", func() { replicas.replicasFor(int64Token(50)) })
	mustPanic(t, "replicasForInt64", func() { replicas.replicasForInt64(int64Token(50)) })
}
