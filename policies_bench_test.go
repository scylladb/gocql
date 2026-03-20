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
