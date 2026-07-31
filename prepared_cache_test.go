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
	key := stmtCacheKey{hostID: "h", keyspace: "ks", statement: "SELECT * FROM t"}
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
