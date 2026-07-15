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

	t.Run("replaces when present and id matches", func(t *testing.T) {
		p := newTestPreparedLRU()
		p.add(key, completedInflight(oldID))

		newEntry := completedInflight(oldID)
		newEntry.preparedStatment.resultMetadataID = []byte{9, 9}

		if !p.updateMetadataIfSame(key, oldID, newEntry) {
			t.Fatal("expected updateMetadataIfSame to return true")
		}
		got, ok := p.get(key)
		if !ok || got != newEntry {
			t.Fatal("cache entry was not replaced with the new inflightPrepare")
		}
	})

	t.Run("no-op when key absent", func(t *testing.T) {
		p := newTestPreparedLRU()
		if p.updateMetadataIfSame(key, oldID, completedInflight(oldID)) {
			t.Fatal("expected false when the key is absent")
		}
		if _, ok := p.get(key); ok {
			t.Fatal("absent key must not be inserted")
		}
	})

	t.Run("no-op when cached id differs (newer/other prepare)", func(t *testing.T) {
		p := newTestPreparedLRU()
		newerID := []byte{7, 7, 7}
		newer := completedInflight(newerID)
		p.add(key, newer)

		if p.updateMetadataIfSame(key, oldID, completedInflight(oldID)) {
			t.Fatal("expected false when the cached prepared id does not match expectID")
		}
		got, ok := p.get(key)
		if !ok || got != newer {
			t.Fatal("a differing (newer) cache entry must not be clobbered")
		}
	})

	t.Run("no-op when cached entry is still in-flight", func(t *testing.T) {
		p := newTestPreparedLRU()
		inflight := &inflightPrepare{done: make(chan struct{})} // done not closed
		p.add(key, inflight)

		if p.updateMetadataIfSame(key, oldID, completedInflight(oldID)) {
			t.Fatal("expected false when the cached entry is still in-flight")
		}
		got, ok := p.get(key)
		if !ok || got != inflight {
			t.Fatal("an in-flight cache entry must not be replaced")
		}
	})
}
