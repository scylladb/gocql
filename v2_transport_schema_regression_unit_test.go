//go:build unit

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package gocql

import "testing"

func TestStep04LateTableRefreshEntrant(t *testing.T) {
	ctrl := &schemaDataMock{knownKeyspaces: map[string][]tableInfo{"test_ks": {{name: "tbl_a", columns: []columnInfo{{name: "id", kind: "partition_key", position: 0}}}}}}
	s := newSchemaEventTestSessionWithMock(ctrl)
	defer s.Close()
	populateKeyspace(s, "test_ks", "tbl_a")
	s.metadataDescriber.invalidateTableSchema("test_ks", "tbl_a")
	snapshot, reloaded, err := s.metadataDescriber.getKeyspaceInternal("test_ks")
	if err != nil {
		t.Fatal(err)
	}
	_, needed, err := s.metadataDescriber.getTableFromSnapshot("test_ks", "tbl_a", snapshot, reloaded)
	if err != nil || !needed {
		t.Fatal(needed, err)
	}
	// Caller A is now paused after deciding refreshNeeded. Caller B completes
	// its entire flight before A can enter the singleflight group.
	if err := s.metadataDescriber.deduplicatedRefreshTable("test_ks", "tbl_a"); err != nil {
		t.Fatal(err)
	}
	ctrl.resetQueries()
	if err := s.metadataDescriber.deduplicatedRefreshTable("test_ks", "tbl_a"); err != nil {
		t.Fatal(err)
	}
	if got := ctrl.getQueryCount(); got != 0 {
		t.Fatalf("stale entrant after completed flight reissued %d schema queries", got)
	}
	// A newer invalidation still requires and obtains a fresh schema fetch.
	s.metadataDescriber.invalidateTableSchema("test_ks", "tbl_a")
	if err := s.metadataDescriber.deduplicatedRefreshTable("test_ks", "tbl_a"); err != nil {
		t.Fatal(err)
	}
	if got := ctrl.getQueryCount(); got != 4 {
		t.Fatalf("newer invalidation skipped required refresh: %d", got)
	}
}
