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
	"testing"

	"github.com/gocql/gocql/internal/lru"
)

func newTestPreparedLRU() *preparedLRU {
	return &preparedLRU{lru: lru.New[stmtCacheKey](16)}
}

func completedInflight(id []byte) *inflightPrepare {
	done := make(chan struct{})
	close(done)
	return &inflightPrepare{
		done:             done,
		preparedStatment: &preparedStatment{id: id},
	}
}

func TestPreparedLRU_updateMetadataIfSame(t *testing.T) {
	key := stmtCacheKey{hostID: UUID{1}, keyspace: "ks", statement: "SELECT * FROM t"}
	oldID := []byte{1, 2, 3}

	t.Run("replaces when present and identity matches", func(t *testing.T) {
		p := newTestPreparedLRU()
		cached := completedInflight(oldID)
		p.add(key, cached)

		newEntry := completedInflight(oldID)
		newEntry.preparedStatment.resultMetadataID = []byte{9, 9}

		if !p.updateMetadataIfSame(key, cached.preparedStatment, newEntry) {
			t.Fatal("expected updateMetadataIfSame to return true")
		}
		got, ok := p.get(key)
		if !ok || got != newEntry {
			t.Fatal("cache entry was not replaced with the new inflightPrepare")
		}
	})

	t.Run("no-op when key absent", func(t *testing.T) {
		p := newTestPreparedLRU()
		expect := completedInflight(oldID)
		if p.updateMetadataIfSame(key, expect.preparedStatment, completedInflight(oldID)) {
			t.Fatal("expected false when the key is absent")
		}
		if _, ok := p.get(key); ok {
			t.Fatal("absent key must not be inserted")
		}
	})

	t.Run("no-op when cached entry is a different generation", func(t *testing.T) {
		p := newTestPreparedLRU()
		newer := completedInflight([]byte{7, 7, 7})
		p.add(key, newer)

		// expect points at some other, stale prepared statement.
		stale := completedInflight(oldID)
		if p.updateMetadataIfSame(key, stale.preparedStatment, completedInflight(oldID)) {
			t.Fatal("expected false when the cached entry is a different generation")
		}
		got, ok := p.get(key)
		if !ok || got != newer {
			t.Fatal("a differing (newer) cache entry must not be clobbered")
		}
	})

	t.Run("no-op when cached generation differs but id is identical", func(t *testing.T) {
		// Regression guard: a reprepare of the same statement typically yields the
		// same prepared id, so an id-only check would wrongly overwrite the newer
		// generation. Pointer identity must reject it.
		p := newTestPreparedLRU()
		newerSameID := completedInflight(oldID)
		p.add(key, newerSameID)

		stale := completedInflight(oldID) // same id bytes, different *preparedStatment
		if p.updateMetadataIfSame(key, stale.preparedStatment, completedInflight(oldID)) {
			t.Fatal("expected false when only the id matches but the generation differs")
		}
		got, ok := p.get(key)
		if !ok || got != newerSameID {
			t.Fatal("a newer generation with the same id must not be clobbered")
		}
	})

	t.Run("no-op when cached entry is still in-flight", func(t *testing.T) {
		p := newTestPreparedLRU()
		inflight := &inflightPrepare{done: make(chan struct{})} // done not closed
		p.add(key, inflight)

		expect := completedInflight(oldID)
		if p.updateMetadataIfSame(key, expect.preparedStatment, completedInflight(oldID)) {
			t.Fatal("expected false when the cached entry is still in-flight")
		}
		got, ok := p.get(key)
		if !ok || got != inflight {
			t.Fatal("an in-flight cache entry must not be replaced")
		}
	})
}

// TestStmtCacheKey_HostIDEquality verifies that stmtCacheKey's UUID-typed
// hostID field distinguishes and matches hosts exactly like the old
// string-typed field did, including the "empty host" collapse (a HostInfo
// with no hostId set must map to the same key as another HostInfo with no
// hostId set, since both hostUUID() and the old HostID() return the zero
// value for an unset host).
func TestStmtCacheKey_HostIDEquality(t *testing.T) {
	t.Parallel()

	hostA := &HostInfo{hostId: tUUID(1)}
	hostB := &HostInfo{hostId: tUUID(2)}
	hostAEmpty1 := &HostInfo{}
	hostAEmpty2 := &HostInfo{}

	p := newTestPreparedLRU()

	keyA := p.keyFor(hostA.hostUUID(), "ks", "SELECT 1")
	keyB := p.keyFor(hostB.hostUUID(), "ks", "SELECT 1")
	keyAAgain := p.keyFor(hostA.hostUUID(), "ks", "SELECT 1")
	keyEmpty1 := p.keyFor(hostAEmpty1.hostUUID(), "ks", "SELECT 1")
	keyEmpty2 := p.keyFor(hostAEmpty2.hostUUID(), "ks", "SELECT 1")

	if keyA == keyB {
		t.Fatal("distinct host UUIDs must not produce equal cache keys")
	}
	if keyA != keyAAgain {
		t.Fatal("the same host UUID must produce equal cache keys")
	}
	if keyEmpty1 != keyEmpty2 {
		t.Fatal("two hosts with no hostId set must collapse to the same (zero-value) cache key")
	}
	if keyA == keyEmpty1 {
		t.Fatal("a real host UUID must not collide with the empty-host zero-value key")
	}

	// Round-trip through the actual cache to make sure UUID keys behave
	// correctly as map keys end-to-end, not just via ==.
	entry := completedInflight([]byte{9})
	p.add(keyA, entry)
	if got, ok := p.get(keyB); ok {
		t.Fatalf("keyB must be a cache miss, got %v", got)
	}
	if got, ok := p.get(keyA); !ok || got != entry {
		t.Fatal("keyA must retrieve the entry stored under it")
	}
}

// TestPreparedLRU_keyFor_ZeroAlloc guards the whole point of switching
// stmtCacheKey.hostID from string to UUID: building a cache key from a
// HostInfo must not allocate. HostInfo.HostID() (the old call site) returns
// a string via UUID.String(), which heap-allocates a []byte plus its string
// conversion on every call; HostInfo.hostUUID() returns the raw comparable
// UUID value with no allocation at all.
func TestPreparedLRU_keyFor_ZeroAlloc(t *testing.T) {
	host := &HostInfo{hostId: tUUID(7)}
	p := newTestPreparedLRU()

	var key stmtCacheKey
	allocs := testing.AllocsPerRun(1000, func() {
		key = p.keyFor(host.hostUUID(), "ks", "SELECT * FROM t WHERE id = ?")
	})
	if allocs != 0 {
		t.Errorf("keyFor(host.hostUUID(), ...) allocated %.2f allocs/op, want 0", allocs)
	}
	if key.hostID != tUUID(7) {
		t.Fatalf("sanity check failed: key.hostID = %v, want %v", key.hostID, tUUID(7))
	}
}
