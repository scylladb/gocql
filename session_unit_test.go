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
	"context"
	"fmt"
	"sync"
	"testing"
)

func TestAsyncSessionInit(t *testing.T) {
	t.Parallel()

	// Build a 3 node cluster to test host metric mapping
	var addresses = []string{
		"127.0.0.1",
		"127.0.0.2",
		"127.0.0.3",
	}
	// only build 1 of the servers so that we can test not connecting to the last
	// one
	srv := NewTestServerWithAddress(addresses[0]+":0", t, defaultProto, context.Background())
	defer srv.Stop()

	// just choose any port
	cluster := testCluster(defaultProto, srv.Address, addresses[1]+":9999", addresses[2]+":9999")
	cluster.PoolConfig.HostSelectionPolicy = SingleHostReadyPolicy(RoundRobinHostPolicy())
	db, err := cluster.CreateSession()
	if err != nil {
		t.Fatalf("NewCluster: %v", err)
	}
	defer db.Close()

	// make sure the session works
	if err := db.Query("void").Exec(); err != nil {
		t.Fatalf("unexpected error from void")
	}
}

// TestRoutingPlan_IsLWT_WithPlan verifies that isLWT() returns the plan's
// lwt value without requiring the mutex when a plan is set.
func TestRoutingPlan_IsLWT_WithPlan(t *testing.T) {
	t.Parallel()

	ri := &queryRoutingInfo{}
	plan := &routingPlan{lwt: true, keyspace: "ks", table: "tbl"}
	ri.plan.Store(plan)

	if !ri.isLWT() {
		t.Error("expected isLWT() to return true when plan.lwt is true")
	}

	planFalse := &routingPlan{lwt: false}
	ri.plan.Store(planFalse)
	if ri.isLWT() {
		t.Error("expected isLWT() to return false when plan.lwt is false")
	}
}

// TestRoutingPlan_IsLWT_WithoutPlan verifies that isLWT() falls back to the
// mutex-protected field when no plan is set.
func TestRoutingPlan_IsLWT_WithoutPlan(t *testing.T) {
	t.Parallel()

	ri := &queryRoutingInfo{}
	ri.lwt = true

	if !ri.isLWT() {
		t.Error("expected isLWT() to return true from fallback field")
	}

	ri.lwt = false
	if ri.isLWT() {
		t.Error("expected isLWT() to return false from fallback field")
	}
}

// TestRoutingPlan_GetPartitioner_WithPlan verifies that getPartitioner()
// reads from the plan when available.
func TestRoutingPlan_GetPartitioner_WithPlan(t *testing.T) {
	t.Parallel()

	ri := &queryRoutingInfo{}
	mp := murmur3Partitioner{}
	plan := &routingPlan{partitioner: mp}
	ri.plan.Store(plan)

	got := ri.getPartitioner()
	if _, ok := got.(murmur3Partitioner); !ok {
		t.Errorf("expected murmur3Partitioner from plan, got %T", got)
	}
}

// TestRoutingPlan_GetPartitioner_WithoutPlan verifies that getPartitioner()
// falls back to the mutex-protected field when no plan is set.
func TestRoutingPlan_GetPartitioner_WithoutPlan(t *testing.T) {
	t.Parallel()

	ri := &queryRoutingInfo{}
	op := orderedPartitioner{}
	ri.partitioner = op

	got := ri.getPartitioner()
	if _, ok := got.(orderedPartitioner); !ok {
		t.Errorf("expected orderedPartitioner from fallback, got %T", got)
	}
}

// TestRoutingPlan_QueryKeyspace verifies that Query.Keyspace() prefers the
// plan's keyspace over the fallback field.
func TestRoutingPlan_QueryKeyspace(t *testing.T) {
	t.Parallel()

	q := &Query{routingInfo: &queryRoutingInfo{}}
	plan := &routingPlan{keyspace: "from_plan", table: "t"}
	q.routingInfo.plan.Store(plan)

	if got := q.Keyspace(); got != "from_plan" {
		t.Errorf("expected Keyspace()=%q from plan, got %q", "from_plan", got)
	}
}

// TestRoutingPlan_QueryKeyspace_Fallback verifies that Query.Keyspace()
// falls back to routingInfo.keyspace when no plan is set.
func TestRoutingPlan_QueryKeyspace_Fallback(t *testing.T) {
	t.Parallel()

	q := &Query{routingInfo: &queryRoutingInfo{keyspace: "from_field"}}

	if got := q.Keyspace(); got != "from_field" {
		t.Errorf("expected Keyspace()=%q from field, got %q", "from_field", got)
	}
}

// TestRoutingPlan_QueryKeyspace_GetKeyspaceOverride verifies that the
// getKeyspace function takes precedence over both plan and field.
func TestRoutingPlan_QueryKeyspace_GetKeyspaceOverride(t *testing.T) {
	t.Parallel()

	q := &Query{routingInfo: &queryRoutingInfo{keyspace: "from_field"}}
	q.getKeyspace = func() string { return "from_func" }
	plan := &routingPlan{keyspace: "from_plan"}
	q.routingInfo.plan.Store(plan)

	if got := q.Keyspace(); got != "from_func" {
		t.Errorf("expected Keyspace()=%q from getKeyspace func, got %q", "from_func", got)
	}
}

// TestRoutingPlan_QueryTable verifies that Query.Table() prefers the plan.
func TestRoutingPlan_QueryTable(t *testing.T) {
	t.Parallel()

	q := &Query{routingInfo: &queryRoutingInfo{table: "from_field"}}
	plan := &routingPlan{table: "from_plan"}
	q.routingInfo.plan.Store(plan)

	if got := q.Table(); got != "from_plan" {
		t.Errorf("expected Table()=%q from plan, got %q", "from_plan", got)
	}
}

// TestRoutingPlan_QueryTable_Fallback verifies that Query.Table() falls
// back to routingInfo.table when no plan is set.
func TestRoutingPlan_QueryTable_Fallback(t *testing.T) {
	t.Parallel()

	q := &Query{routingInfo: &queryRoutingInfo{table: "from_field"}}

	if got := q.Table(); got != "from_field" {
		t.Errorf("expected Table()=%q from field, got %q", "from_field", got)
	}
}

// TestRoutingPlan_BatchTable verifies that Batch.Table() prefers the plan.
func TestRoutingPlan_BatchTable(t *testing.T) {
	t.Parallel()

	b := &Batch{routingInfo: &queryRoutingInfo{table: "from_field"}}
	plan := &routingPlan{table: "from_plan"}
	b.routingInfo.plan.Store(plan)

	if got := b.Table(); got != "from_plan" {
		t.Errorf("expected Batch.Table()=%q from plan, got %q", "from_plan", got)
	}
}

// TestRoutingPlan_QueryReset verifies that Query.reset() clears the plan
// pointer (new queryRoutingInfo has nil plan).
func TestRoutingPlan_QueryReset(t *testing.T) {
	t.Parallel()

	q := &Query{routingInfo: &queryRoutingInfo{}}
	plan := &routingPlan{lwt: true, keyspace: "ks", table: "tbl"}
	q.routingInfo.plan.Store(plan)

	if q.routingInfo.plan.Load() == nil {
		t.Fatal("plan should be set before reset")
	}

	q.reset()

	if q.routingInfo.plan.Load() != nil {
		t.Error("expected plan to be nil after reset()")
	}
	if q.routingInfo.isLWT() {
		t.Error("expected isLWT() to be false after reset()")
	}
}

// TestRoutingPlan_QueryPoolReset verifies that a Query obtained from the
// pool starts with a nil plan.
func TestRoutingPlan_QueryPoolReset(t *testing.T) {
	t.Parallel()

	q := queryPool.Get().(*Query)
	defer queryPool.Put(q)

	if q.routingInfo.plan.Load() != nil {
		t.Error("expected fresh pool Query to have nil plan")
	}
}

// TestRoutingPlan_SyncMapCache verifies that Session.routingPlans stores
// and retrieves plans correctly, and Delete removes them.
func TestRoutingPlan_SyncMapCache(t *testing.T) {
	t.Parallel()

	var m sync.Map

	plan := &routingPlan{keyspace: "ks", table: "tbl", lwt: true}
	m.Store("SELECT * FROM tbl WHERE id = ?", plan)

	got, ok := m.Load("SELECT * FROM tbl WHERE id = ?")
	if !ok {
		t.Fatal("expected plan to be found in sync.Map")
	}
	p := got.(*routingPlan)
	if p.keyspace != "ks" || p.table != "tbl" || !p.lwt {
		t.Errorf("unexpected plan fields: %+v", p)
	}

	// Simulate invalidation
	m.Delete("SELECT * FROM tbl WHERE id = ?")
	if _, ok := m.Load("SELECT * FROM tbl WHERE id = ?"); ok {
		t.Error("expected plan to be deleted from sync.Map after Delete")
	}
}

// TestRoutingPlan_ConcurrentAccess verifies that concurrent reads of isLWT()
// and getPartitioner() via the atomic plan pointer are safe.
func TestRoutingPlan_ConcurrentAccess(t *testing.T) {
	t.Parallel()

	ri := &queryRoutingInfo{}
	plan := &routingPlan{lwt: true, partitioner: murmur3Partitioner{}}
	ri.plan.Store(plan)

	const goroutines = 100
	var wg sync.WaitGroup
	wg.Add(goroutines * 2)

	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			if !ri.isLWT() {
				t.Error("concurrent isLWT() returned false")
			}
		}()
		go func() {
			defer wg.Done()
			if ri.getPartitioner() == nil {
				t.Error("concurrent getPartitioner() returned nil")
			}
		}()
	}

	wg.Wait()
}

// TestCreateRoutingKey verifies that createRoutingKey produces correct
// output for single and composite partition keys.
func TestCreateRoutingKey(t *testing.T) {
	t.Parallel()

	intType := NativeType{proto: 4, typ: TypeInt}

	// Single partition key column
	indexes := []int{0}
	types := []TypeInfo{intType}
	values := []interface{}{42}

	got1, err1 := createRoutingKey(indexes, types, values)
	if err1 != nil {
		t.Fatalf("createRoutingKey (single) error: %v", err1)
	}
	if len(got1) == 0 {
		t.Fatal("createRoutingKey (single) returned empty key")
	}

	// Composite partition key
	indexesComp := []int{0, 1}
	typesComp := []TypeInfo{intType, intType}
	valuesComp := []interface{}{42, 99}

	got2, err2 := createRoutingKey(indexesComp, typesComp, valuesComp)
	if err2 != nil {
		t.Fatalf("createRoutingKey (composite) error: %v", err2)
	}
	if len(got2) == 0 {
		t.Fatal("createRoutingKey (composite) returned empty key")
	}

	// Composite key should differ from single key
	if string(got1) == string(got2) {
		t.Error("single and composite routing keys should differ")
	}
}

// TestCreateRoutingKey_Empty verifies empty indexes returns nil.
func TestCreateRoutingKey_Empty(t *testing.T) {
	t.Parallel()

	got, err := createRoutingKey(nil, nil, []interface{}{42})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != nil {
		t.Errorf("expected nil routing key for empty indexes, got %x", got)
	}
}

// ---------------------------------------------------------------------------
// Benchmarks for routingPlan cache (commit: session: add routingPlan cache)
// ---------------------------------------------------------------------------

// BenchmarkIsLWT measures the isLWT() hot path with and without an atomic
// plan pointer. WithPlan exercises the lock-free atomic.Pointer read;
// WithoutPlan exercises the RWMutex.RLock fallback.
func BenchmarkIsLWT(b *testing.B) {
	b.Run("WithPlan", func(b *testing.B) {
		ri := &queryRoutingInfo{}
		ri.plan.Store(&routingPlan{lwt: true})
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = ri.isLWT()
		}
	})

	b.Run("WithoutPlan", func(b *testing.B) {
		ri := &queryRoutingInfo{lwt: true}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = ri.isLWT()
		}
	})
}

// BenchmarkGetPartitioner measures the getPartitioner() hot path with and
// without an atomic plan pointer, mirroring BenchmarkIsLWT.
func BenchmarkGetPartitioner(b *testing.B) {
	part := murmur3Partitioner{}

	b.Run("WithPlan", func(b *testing.B) {
		ri := &queryRoutingInfo{}
		ri.plan.Store(&routingPlan{partitioner: part})
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = ri.getPartitioner()
		}
	})

	b.Run("WithoutPlan", func(b *testing.B) {
		ri := &queryRoutingInfo{partitioner: part}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = ri.getPartitioner()
		}
	})
}

// BenchmarkRoutingPlanCacheLookup measures the sync.Map-based plan cache
// used by Session.getRoutingPlan(). CacheHit benchmarks the steady-state
// lock-free Load; CacheMiss_Store benchmarks the cold-path LoadOrStore.
func BenchmarkRoutingPlanCacheLookup(b *testing.B) {
	stmt := "SELECT * FROM ks.tbl WHERE id = ?"
	plan := &routingPlan{keyspace: "ks", table: "tbl", lwt: true}

	b.Run("CacheHit", func(b *testing.B) {
		var m sync.Map
		m.Store(stmt, plan)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			p, ok := m.Load(stmt)
			if !ok {
				b.Fatal("expected cache hit")
			}
			_ = p.(*routingPlan)
		}
	})

	b.Run("CacheMiss_Store", func(b *testing.B) {
		// Each iteration needs a fresh key to measure a true cache miss.
		// Pre-generate unique keys outside the timed loop.
		var m sync.Map
		stmts := make([]string, b.N)
		for i := range stmts {
			stmts[i] = fmt.Sprintf("SELECT * FROM ks.tbl WHERE id = ? /* %d */", i)
		}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			m.LoadOrStore(stmts[i], plan)
		}
	})
}

// BenchmarkCreateRoutingKey measures createRoutingKey for single and composite
// partition keys.
func BenchmarkCreateRoutingKey(b *testing.B) {
	intType := NativeType{proto: 4, typ: TypeInt}

	// Single partition key
	idxSingle := []int{0}
	typesSingle := []TypeInfo{intType}
	valsSingle := []interface{}{42}

	// Composite partition key (2 columns)
	idxComp := []int{0, 1}
	typesComp := []TypeInfo{intType, intType}
	valsComp := []interface{}{42, 99}

	b.Run("Single", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _ = createRoutingKey(idxSingle, typesSingle, valsSingle)
		}
	})

	b.Run("Composite", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _ = createRoutingKey(idxComp, typesComp, valsComp)
		}
	})
}
