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

package gocql

import (
	"errors"
	"fmt"
	"net"
	"testing"
)

// BenchmarkHostSetAdd benchmarks the stack-allocated hostSet.add path
// that replaced the heap-allocated map[*HostInfo]bool.
func BenchmarkHostSetAdd(b *testing.B) {
	hosts := make([]*HostInfo, 4)
	for i := range hosts {
		hosts[i] = &HostInfo{}
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var s hostSet
		for _, h := range hosts {
			s.add(h)
		}
	}
}

// BenchmarkHostSetContains benchmarks hostSet.contains for a miss (worst case).
func BenchmarkHostSetContains(b *testing.B) {
	var s hostSet
	hosts := make([]*HostInfo, 4)
	for i := range hosts {
		hosts[i] = &HostInfo{}
		s.add(hosts[i])
	}
	needle := &HostInfo{} // not in the set

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = s.contains(needle)
	}
}

// BenchmarkShuffleHostsInPlace benchmarks the lock-free in-place shuffle
// that replaced the allocating shuffleHosts().
func BenchmarkShuffleHostsInPlace(b *testing.B) {
	hosts := make([]*HostInfo, 3)
	for i := range hosts {
		hosts[i] = &HostInfo{}
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		shuffleHostsInPlace(hosts)
	}
}

// BenchmarkPartitionHealthy benchmarks the in-place stable partition
// that replaced the make+append healthy/unhealthy split.
func BenchmarkPartitionHealthy(b *testing.B) {
	// Create a minimal session with a pool so IsBusy doesn't panic.
	sess := &Session{
		pool: &policyConnPool{
			hostConnPools: make(map[string]*hostConnPool),
		},
	}

	hosts := make([]*HostInfo, 6)
	for i := range hosts {
		hosts[i] = &HostInfo{}
	}
	// Make a working copy so we don't disturb ordering across iterations.
	work := make([]*HostInfo, len(hosts))

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		copy(work, hosts)
		partitionHealthy(work, sess)
	}
}

// BenchmarkTokenAwarePick benchmarks the full token-aware Pick() path
// end-to-end: token lookup, replica selection, deduplication, shuffling,
// and iteration through all hosts. This is the hot path on every query.
func BenchmarkTokenAwarePick(b *testing.B) {
	const keyspace = "bench_ks"
	policy := TokenAwareHostPolicy(RoundRobinHostPolicy())
	policyInternal := policy.(*tokenAwareHostPolicy)
	policyInternal.getKeyspaceName = func() string { return keyspace }
	policyInternal.getKeyspaceMetadata = func(ks string) (*KeyspaceMetadata, error) {
		if ks != keyspace {
			return nil, fmt.Errorf("unknown keyspace: %s", ks)
		}
		return &KeyspaceMetadata{
			Name:          keyspace,
			StrategyClass: "SimpleStrategy",
			StrategyOptions: map[string]interface{}{
				"class":              "SimpleStrategy",
				"replication_factor": 3,
			},
		}, nil
	}

	hosts := make([]*HostInfo, 6)
	for i := range hosts {
		hosts[i] = &HostInfo{
			hostId:         fmt.Sprintf("host-%d", i),
			connectAddress: net.IPv4(10, 0, 0, byte(i+1)),
			tokens:         []string{fmt.Sprintf("%02d", i*17)},
		}
		policy.AddHost(hosts[i])
	}

	policy.SetPartitioner("OrderedPartitioner")
	policy.KeyspaceChanged(KeyspaceUpdateEvent{Keyspace: keyspace})

	query := &Query{routingInfo: &queryRoutingInfo{}}
	query.getKeyspace = func() string { return keyspace }
	query.RoutingKey([]byte("30"))

	// Verify the iterator works before benchmarking.
	iter := policy.Pick(query)
	if iter == nil {
		b.Fatal("Pick returned nil")
	}
	count := 0
	for iter() != nil {
		count++
	}
	if count == 0 {
		b.Fatal("Pick iterator returned no hosts")
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		iter := policy.Pick(query)
		for iter() != nil {
		}
	}
}

// BenchmarkTokenAwarePickNoShuffle benchmarks Pick() with shuffling disabled
// (LWT path), isolating the deduplication and iteration cost.
func BenchmarkTokenAwarePickNoShuffle(b *testing.B) {
	const keyspace = "bench_ks"
	policy := TokenAwareHostPolicy(RoundRobinHostPolicy(), DontShuffleReplicas())
	policyInternal := policy.(*tokenAwareHostPolicy)
	policyInternal.getKeyspaceName = func() string { return keyspace }
	policyInternal.getKeyspaceMetadata = func(ks string) (*KeyspaceMetadata, error) {
		if ks != keyspace {
			return nil, errors.New("unknown keyspace")
		}
		return &KeyspaceMetadata{
			Name:          keyspace,
			StrategyClass: "SimpleStrategy",
			StrategyOptions: map[string]interface{}{
				"class":              "SimpleStrategy",
				"replication_factor": 3,
			},
		}, nil
	}

	hosts := make([]*HostInfo, 6)
	for i := range hosts {
		hosts[i] = &HostInfo{
			hostId:         fmt.Sprintf("host-%d", i),
			connectAddress: net.IPv4(10, 0, 0, byte(i+1)),
			tokens:         []string{fmt.Sprintf("%02d", i*17)},
		}
		policy.AddHost(hosts[i])
	}

	policy.SetPartitioner("OrderedPartitioner")
	policy.KeyspaceChanged(KeyspaceUpdateEvent{Keyspace: keyspace})

	query := &Query{routingInfo: &queryRoutingInfo{}}
	query.getKeyspace = func() string { return keyspace }
	query.RoutingKey([]byte("30"))

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		iter := policy.Pick(query)
		for iter() != nil {
		}
	}
}
