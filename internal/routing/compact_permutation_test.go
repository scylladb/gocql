// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package routing

import (
	"encoding/binary"
	"fmt"
	"math/rand/v2"
	"reflect"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"

	"github.com/scylladb/gocql/v2/internal/cqltype"
	"github.com/scylladb/gocql/v2/internal/metadata"
)

func compactFixture(n int) (*MetadataSnapshot, RoutingInfo) {
	hosts := make([]HostSnapshot, n+320)
	for i := range hosts {
		binary.BigEndian.PutUint64(hosts[i].ID[8:], uint64(i+1000))
		hosts[i].Datacenter, hosts[i].Rack = "local", "rack"
		hosts[i].Enabled, hosts[i].Connected, hosts[i].Up, hosts[i].Shards = true, true, true, 4
	}
	replicas := make([]Target, n)
	for i := range replicas {
		// Even small replica sets refer to global host positions above 255.
		replicas[i] = Target{HostID: hosts[len(hosts)-1-i].ID, ShardID: Option[uint32]{Value: uint32(i % 4), Set: true}}
	}
	m := NewMetadataSnapshot(SnapshotState{
		InternalHosts: hosts, InternalHostIndex: InternalBuildRoutingHostIndex(hosts),
		InternalKeyspaces: map[string]*metadata.KeyspaceMetadata{"ks": {Tables: map[string]*metadata.TableMetadata{"table": {Name: "table"}}}},
		InternalRanges:    map[string][]TokenRange{"ks": {{Replicas: replicas}}},
	})
	return m, RoutingInfo{Keyspace: "ks", Table: "table", Token: Option[int64]{Set: true}, Consistency: cqltype.Quorum, Kind: OrdinaryStatement}
}

func compactDrain(t *testing.T, plan QueryPlan, limit int) []Target {
	t.Helper()
	var targets []Target
	for i := 0; i <= limit; i++ {
		target, ok := plan.Next()
		if !ok {
			return targets
		}
		targets = append(targets, target)
	}
	t.Fatal("routing plan did not terminate")
	return nil
}

// Retain the pre-compaction algorithm as an order oracle. This verifies the
// same PCG draws, rejection threshold and Fisher-Yates order in both modes.
func originalPermutation(n int, seed uint64, conditional bool) []int {
	result := make([]int, n)
	for i := range result {
		result[i] = i
	}
	if conditional {
		return result
	}
	rng := rand.NewPCG(seed, seed^0x9e3779b97f4a7c15)
	for i := n - 1; i > 0; i-- {
		bound := uint64(i + 1)
		threshold := -bound % bound
		v := rng.Uint64()
		for v < threshold {
			v = rng.Uint64()
		}
		j := int(v % bound)
		result[i], result[j] = result[j], result[i]
	}
	return result
}

func TestCompactPermutationBoundariesAndOrder(t *testing.T) {
	for _, n := range []int{0, 1, 2, 3, 15, 16, 17, 255, 256, 257, 300} {
		for _, seed := range []uint64{0, 1, ^uint64(0)} {
			for _, consistency := range []Consistency{cqltype.Quorum, Serial, LocalSerial} {
				t.Run(fmt.Sprintf("replicas%d/seed%d/consistency%d", n, seed, consistency), func(t *testing.T) {
					m, info := compactFixture(n)
					info.Consistency = consistency
					policy := &InternalRoutingPolicy{InternalOptions: RoutingOptions{TokenAware: true}, seed: seed}
					replicas := slices.Clone(SnapshotData(m).InternalRanges["ks"][0].Replicas)
					plan := policy.Plan(info, m).(*InternalLazyRoutingPlan)
					if (plan.permutation != nil) != (n > 16) {
						t.Fatal("wrong permutation storage mode", n)
					}
					got := compactDrain(t, plan, len(SnapshotData(m).InternalHosts)+n)
					if len(got) != len(SnapshotData(m).InternalHosts) {
						t.Fatal("host traversal incomplete", len(got))
					}
					wantOrder := originalPermutation(n, seed, consistency == Serial || consistency == LocalSerial)
					for i, index := range wantOrder {
						if got[i] != replicas[index] {
							t.Fatalf("replica %d: got %+v want %+v", i, got[i], replicas[index])
						}
					}
					seen := make(map[UUID]bool)
					for _, target := range got {
						if seen[target.HostID] {
							t.Fatal("duplicate host", target)
						}
						seen[target.HostID] = true
					}
					if !reflect.DeepEqual(SnapshotData(m).InternalRanges["ks"][0].Replicas, replicas) {
						t.Fatal("mutated published replicas")
					}
				})
			}
		}
	}
}

func TestCompactPermutationLocalityLeaderLatencyAndFallback(t *testing.T) {
	for _, n := range []int{16, 17} {
		for _, leader := range []bool{false, true} {
			for _, slow := range []bool{false, true} {
				for _, remote := range []bool{false, true} {
					t.Run(fmt.Sprintf("replicas%d/leader%t/slow%t/remote%t", n, leader, slow, remote), func(t *testing.T) {
						m, info := compactFixture(0)
						hosts := SnapshotData(m).InternalHosts[:8]
						hosts[1].Rack = "other"
						hosts[2].Datacenter, hosts[2].Rack = "remote", "other"
						hosts[4].Connected = false
						hosts[5].Datacenter, hosts[5].Connected = "remote", false
						hosts[6].Enabled = false
						hosts[7].Up = false
						SnapshotData(m).InternalHosts = hosts
						SnapshotData(m).InternalHostIndex = InternalBuildRoutingHostIndex(hosts)
						target := func(i int, shard uint32) Target {
							return Target{HostID: hosts[i].ID, ShardID: Option[uint32]{Value: shard, Set: true}}
						}
						replicas := []Target{target(2, 0), target(1, 1), target(0, 0), target(0, 1), target(6, 0), target(4, 0), target(7, 0)}
						for len(replicas) < n {
							replicas = append(replicas, Target{HostID: UUID{0: 255, 15: byte(len(replicas))}})
						}
						SnapshotData(m).InternalKeyspaces["ks"].ConsistencyMode = "global"
						root := placementNode(int64(1), &InternalTabletRange{InternalSpan: TokenRange{Start: -1, End: 1, Replicas: replicas}, InternalVersion: Option[uint64]{Set: true}}, nil, nil)
						SnapshotData(m).InternalPlacements = InternalPlacementPut(nil, InternalMetadataTable{InternalKeyspace: "ks", InternalTable: "table"}, InternalTabletPlacement{InternalRanges: root}, InternalComparePlacementTable)
						policy := &InternalRoutingPolicy{InternalOptions: RoutingOptions{TokenAware: true, LeaderAware: leader, Datacenter: "local", Rack: "rack", AllowRemote: remote}}
						if slow {
							policy.InternalDecisions.Store(&InternalRoutingDecisions{InternalSlow: map[UUID]time.Time{hosts[2].ID: time.Now().Add(time.Hour)}})
						}
						info.Conditional = true
						got := compactDrain(t, policy.Plan(info, m), len(hosts)+n)
						want := []Target{target(0, 0), target(0, 1), target(1, 1)}
						if remote && !slow && !leader {
							want = append(want, target(2, 0))
						}
						for _, i := range []int{3, 4, 7} {
							want = append(want, Target{HostID: hosts[i].ID})
						}
						if remote {
							want = append(want, Target{HostID: hosts[5].ID})
						}
						if remote && leader && !slow {
							first := target(2, 0)
							first.StrictShard = true
							want = append([]Target{first}, want...)
						}
						if remote && slow {
							last := target(2, 0)
							last.StrictShard = leader
							want = append(want, last)
						}
						if !reflect.DeepEqual(got, want) {
							t.Fatalf("got %+v\nwant %+v", got, want)
						}
					})
				}
			}
		}
	}
}

func TestCompactPermutationPlacementInvalidation(t *testing.T) {
	for _, n := range []int{16, 17, 257} {
		for _, consumed := range []int{0, 1} {
			t.Run(fmt.Sprintf("replicas%d/consumed%d", n, consumed), func(t *testing.T) {
				m, info := compactFixture(n)
				signal := new(atomic.Uint64)
				SnapshotData(m).InternalPlacementSignal = signal
				policy := &InternalRoutingPolicy{InternalOptions: RoutingOptions{TokenAware: true}}
				// A second pass must also remain clear of stale inline and int indexes.
				policy.InternalDecisions.Store(&InternalRoutingDecisions{InternalSlow: map[UUID]time.Time{SnapshotData(m).InternalHosts[0].ID: time.Now().Add(time.Hour)}})
				plan := policy.Plan(info, m).(*InternalLazyRoutingPlan)
				seen := make(map[UUID]bool)
				if consumed != 0 {
					target, ok := plan.Next()
					if !ok || !target.ShardID.Set {
						t.Fatal("missing initial replica")
					}
					seen[target.HostID] = true
				}
				signal.Add(1)
				for _, target := range compactDrain(t, plan, len(SnapshotData(m).InternalHosts)) {
					if target.ShardID.Set {
						t.Fatal("stale replica shard after invalidation", target)
					}
					if seen[target.HostID] {
						t.Fatal("duplicate invalidated destination", target)
					}
					seen[target.HostID] = true
				}
				if len(seen) != len(SnapshotData(m).InternalHosts) {
					t.Fatal("invalidated plan omitted hosts", len(seen))
				}
				if plan.replicas != nil || plan.permutation != nil || plan.leader.Set {
					t.Fatal("retained invalid placement")
				}
			})
		}
	}
}

func TestCompactPermutationConcurrentIndependentPlans(t *testing.T) {
	for _, n := range []int{16, 17, 257} {
		m, info := compactFixture(n)
		policy := &InternalRoutingPolicy{InternalOptions: RoutingOptions{TokenAware: true, ShuffleReplicas: true}}
		var wg sync.WaitGroup
		for range 16 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for range 4 {
					plan := policy.Plan(info, m)
					seen := make(map[UUID]bool)
					for _, target := range compactDrain(t, plan, len(SnapshotData(m).InternalHosts)) {
						if seen[target.HostID] {
							t.Error("plans shared mutable permutation")
						}
						seen[target.HostID] = true
					}
					if len(seen) != len(SnapshotData(m).InternalHosts) {
						t.Error("concurrent plan incomplete")
					}
				}
			}()
		}
		wg.Wait()
	}
}

func TestCompactPermutationLayout(t *testing.T) {
	t.Logf("pointer_bytes=%d target_bytes=%d plan_bytes=%d", unsafe.Sizeof(uintptr(0)), unsafe.Sizeof(Target{}), unsafe.Sizeof(InternalLazyRoutingPlan{}))
	if unsafe.Sizeof(uintptr(0)) == 8 && unsafe.Sizeof(InternalLazyRoutingPlan{}) != 512 {
		t.Fatal("unexpected compact plan layout")
	}
}
