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

package gocql

import (
	"sync"
	"testing"
)

// TestQueryResetPreservesAllocations verifies that reset() preserves the
// routingInfo and metrics pointers rather than allocating new ones.
func TestQueryResetPreservesAllocations(t *testing.T) {
	t.Parallel()

	q := &Query{
		routingInfo: &queryRoutingInfo{},
		metrics:     &queryMetrics{m: make(map[string]*hostMetrics)},
		refCount:    1,
	}

	// Save original pointers.
	origRI := q.routingInfo
	origM := q.metrics

	// Populate some state that reset should clear.
	q.routingInfo.keyspace = "test_ks"
	q.routingInfo.table = "test_table"
	q.routingInfo.lwt = true
	q.metrics.m["host1"] = &hostMetrics{Attempts: 5}
	q.metrics.totalAttempts = 5
	q.stmt = "SELECT * FROM test"

	q.reset()

	// Pointers must be preserved (same address).
	if q.routingInfo != origRI {
		t.Fatal("reset() allocated a new routingInfo instead of reusing the existing one")
	}
	if q.metrics != origM {
		t.Fatal("reset() allocated a new metrics instead of reusing the existing one")
	}

	// Fields must be zeroed.
	if q.routingInfo.keyspace != "" || q.routingInfo.table != "" || q.routingInfo.lwt {
		t.Fatalf("reset() did not zero routingInfo fields: %+v", q.routingInfo)
	}
	if len(q.metrics.m) != 0 {
		t.Fatalf("reset() did not clear metrics map: got %d entries", len(q.metrics.m))
	}
	if q.metrics.totalAttempts != 0 {
		t.Fatalf("reset() did not zero totalAttempts: got %d", q.metrics.totalAttempts)
	}
	if q.stmt != "" {
		t.Fatalf("reset() did not zero Query fields: stmt=%q", q.stmt)
	}
	if q.refCount != 1 {
		t.Fatalf("reset() did not set refCount to 1: got %d", q.refCount)
	}
}

// TestQueryResetNilMetrics verifies that reset() handles a nil metrics pointer
// (first allocation from pool.New before defaultsFromSession is called).
func TestQueryResetNilMetrics(t *testing.T) {
	t.Parallel()

	q := &Query{
		routingInfo: &queryRoutingInfo{},
		refCount:    1,
	}

	origRI := q.routingInfo

	q.reset()

	if q.routingInfo != origRI {
		t.Fatal("reset() allocated a new routingInfo")
	}
	if q.metrics != nil {
		t.Fatal("reset() should preserve nil metrics as nil")
	}
	if q.refCount != 1 {
		t.Fatalf("expected refCount 1, got %d", q.refCount)
	}
}

// TestQueryPoolRoundTrip verifies the full get/reset/put/get cycle reuses allocations.
func TestQueryPoolRoundTrip(t *testing.T) {
	t.Parallel()

	q := queryPool.Get().(*Query)

	// Pool.New should have created both.
	if q.routingInfo == nil {
		t.Fatal("queryPool.New did not set routingInfo")
	}
	if q.metrics == nil {
		t.Fatal("queryPool.New did not set metrics")
	}

	origRI := q.routingInfo
	origM := q.metrics

	// Simulate usage.
	q.stmt = "INSERT INTO foo (a) VALUES (?)"
	q.routingInfo.keyspace = "ks"
	q.metrics.m["host"] = &hostMetrics{Attempts: 3}
	q.metrics.totalAttempts = 3

	// Return to pool (simulates decRefCount hitting 0).
	q.reset()
	queryPool.Put(q)

	// Get it back.
	q2 := queryPool.Get().(*Query)

	// On this machine, with no concurrent GC pressure, we should get the same object.
	// But sync.Pool does not guarantee it, so we only check if we happen to get it back.
	if q2 == q {
		if q2.routingInfo != origRI {
			t.Fatal("routingInfo was not reused after pool round-trip")
		}
		if q2.metrics != origM {
			t.Fatal("metrics was not reused after pool round-trip")
		}
		if len(q2.metrics.m) != 0 {
			t.Fatalf("metrics map not cleared: %d entries", len(q2.metrics.m))
		}
		if q2.stmt != "" {
			t.Fatalf("stmt not cleared: %q", q2.stmt)
		}
	}

	// Clean up.
	q2.reset()
	queryPool.Put(q2)
}

// TestQueryPoolConcurrency stress-tests the pool under concurrent access.
func TestQueryPoolConcurrency(t *testing.T) {
	t.Parallel()

	const goroutines = 16
	const iterations = 1000

	var wg sync.WaitGroup
	wg.Add(goroutines)

	for g := 0; g < goroutines; g++ {
		go func() {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				q := queryPool.Get().(*Query)
				if q.routingInfo == nil {
					panic("routingInfo is nil from pool")
				}
				if q.refCount != 1 {
					panic("refCount is not 1 from pool")
				}

				// Simulate usage.
				q.stmt = "SELECT 1"
				q.routingInfo.keyspace = "ks"
				if q.metrics != nil {
					q.metrics.m["host"] = &hostMetrics{Attempts: 1}
					q.metrics.totalAttempts = 1
				}

				// Return.
				q.reset()
				queryPool.Put(q)
			}
		}()
	}

	wg.Wait()
}

// BenchmarkQueryPoolNewAlloc benchmarks the old pattern: always allocating new routingInfo and metrics.
func BenchmarkQueryPoolNewAlloc(b *testing.B) {
	for b.Loop() {
		q := &Query{routingInfo: &queryRoutingInfo{}, refCount: 1}
		q.metrics = &queryMetrics{m: make(map[string]*hostMetrics)}
		q.stmt = "SELECT * FROM test"
		q.routingInfo.keyspace = "ks"
		q.metrics.m["host"] = &hostMetrics{Attempts: 1}
		q.metrics.totalAttempts = 1
		_ = q
	}
}

// BenchmarkQueryPoolReuse benchmarks the new pattern: pool get/put with in-place reuse.
func BenchmarkQueryPoolReuse(b *testing.B) {
	for b.Loop() {
		q := queryPool.Get().(*Query)
		if q.metrics == nil {
			q.metrics = &queryMetrics{m: make(map[string]*hostMetrics)}
		}
		q.stmt = "SELECT * FROM test"
		q.routingInfo.keyspace = "ks"
		q.metrics.m["host"] = &hostMetrics{Attempts: 1}
		q.metrics.totalAttempts = 1

		q.reset()
		queryPool.Put(q)
	}
}

// BenchmarkQueryPoolNewAllocParallel benchmarks the old allocation pattern under contention.
func BenchmarkQueryPoolNewAllocParallel(b *testing.B) {
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			q := &Query{routingInfo: &queryRoutingInfo{}, refCount: 1}
			q.metrics = &queryMetrics{m: make(map[string]*hostMetrics)}
			q.stmt = "SELECT * FROM test"
			q.routingInfo.keyspace = "ks"
			q.metrics.m["host"] = &hostMetrics{Attempts: 1}
			q.metrics.totalAttempts = 1
			_ = q
		}
	})
}

// BenchmarkQueryPoolReuseParallel benchmarks the pool reuse pattern under contention.
func BenchmarkQueryPoolReuseParallel(b *testing.B) {
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			q := queryPool.Get().(*Query)
			if q.metrics == nil {
				q.metrics = &queryMetrics{m: make(map[string]*hostMetrics)}
			}
			q.stmt = "SELECT * FROM test"
			q.routingInfo.keyspace = "ks"
			q.metrics.m["host"] = &hostMetrics{Attempts: 1}
			q.metrics.totalAttempts = 1

			q.reset()
			queryPool.Put(q)
		}
	})
}
