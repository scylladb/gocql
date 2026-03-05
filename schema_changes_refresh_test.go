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

	frm "github.com/gocql/gocql/internal/frame"
	"github.com/gocql/gocql/tablets"
)

func TestIsSystemKeyspace(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		keyspace string
		expected bool
	}{
		{"system keyspace", "system", true},
		{"system_schema keyspace", "system_schema", true},
		{"system_auth keyspace", "system_auth", true},
		{"system_traces keyspace", "system_traces", true},
		{"system_distributed keyspace", "system_distributed", true},
		{"user keyspace", "my_keyspace", false},
		{"empty keyspace", "", false},
		{"prefix match only", "systems", false},
		{"system-like name", "system_custom", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := isSystemKeyspace(tt.keyspace)
			if result != tt.expected {
				t.Errorf("isSystemKeyspace(%q) = %v, want %v", tt.keyspace, result, tt.expected)
			}
		})
	}
}

func TestSchemaChangesRefreshModeString(t *testing.T) {
	t.Parallel()

	tests := []struct {
		mode     SchemaChangesRefreshMode
		expected string
	}{
		{SchemaChangesRefreshAll, "all"},
		{SchemaChangesRefreshKeyspace, "keyspace"},
		{SchemaChangesRefreshTable, "table"},
		{SchemaChangesRefreshMode(99), "unknown(99)"},
	}

	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			result := tt.mode.String()
			if result != tt.expected {
				t.Errorf("SchemaChangesRefreshMode(%d).String() = %q, want %q", tt.mode, result, tt.expected)
			}
		})
	}
}

// createTestSession creates a minimal Session suitable for testing handleSchemaEvent.
// It uses a pre-populated metadata cache and a mock control connection.
func createTestSession(mode SchemaChangesRefreshMode) *Session {
	cfg := ClusterConfig{
		SchemaChangesRefreshMode: mode,
	}
	s := &Session{
		cfg:             cfg,
		logger:          &defaultLogger{},
		control:         &schemaTestControlConn{},
		policy:          &schemaTestHostPolicy{},
		useSystemSchema: true,
	}
	s.metadataDescriber = newMetadataDescriber(s)
	return s
}

// schemaTestControlConn is a minimal controlConnection implementation for schema event tests.
// querySystem returns an empty Iter (numRows=0) so that getKeyspaceMetadata returns
// ErrKeyspaceDoesNotExist, which is handled gracefully by the refresh methods.
type schemaTestControlConn struct{}

func (c *schemaTestControlConn) getConn() *connHost                                  { return nil }
func (c *schemaTestControlConn) awaitSchemaAgreement() error                         { return nil }
func (c *schemaTestControlConn) query(statement string, values ...interface{}) *Iter { return &Iter{} }
func (c *schemaTestControlConn) querySystem(statement string, values ...interface{}) *Iter {
	return &Iter{}
}
func (c *schemaTestControlConn) discoverProtocol(hosts []*HostInfo) (int, error) { return 0, nil }
func (c *schemaTestControlConn) connect(hosts []*HostInfo) error                 { return nil }
func (c *schemaTestControlConn) close()                                          {}
func (c *schemaTestControlConn) getSession() *Session                            { return nil }
func (c *schemaTestControlConn) reconnect() error                                { return nil }

// schemaTestHostPolicy is a minimal HostSelectionPolicy implementation for schema event tests.
type schemaTestHostPolicy struct {
	mu              sync.Mutex
	keyspaceChanges []string
}

func (p *schemaTestHostPolicy) AddHost(host *HostInfo)            {}
func (p *schemaTestHostPolicy) RemoveHost(host *HostInfo)         {}
func (p *schemaTestHostPolicy) HostUp(host *HostInfo)             {}
func (p *schemaTestHostPolicy) HostDown(host *HostInfo)           {}
func (p *schemaTestHostPolicy) SetPartitioner(partitioner string) {}
func (p *schemaTestHostPolicy) Init(s *Session)                   {}
func (p *schemaTestHostPolicy) Reset()                            {}
func (p *schemaTestHostPolicy) IsLocal(host *HostInfo) bool       { return true }
func (p *schemaTestHostPolicy) Pick(ExecutableQuery) NextHost     { return nil }
func (p *schemaTestHostPolicy) IsOperational(s *Session) error    { return nil }
func (p *schemaTestHostPolicy) KeyspaceChanged(event KeyspaceUpdateEvent) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.keyspaceChanges = append(p.keyspaceChanges, event.Keyspace)
}

func (p *schemaTestHostPolicy) getKeyspaceChanges() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	result := make([]string, len(p.keyspaceChanges))
	copy(result, p.keyspaceChanges)
	return result
}

// populateKeyspaceCache pre-populates the metadata cache with dummy keyspace metadata.
func populateKeyspaceCache(s *Session, keyspaces ...string) {
	for _, ks := range keyspaces {
		s.metadataDescriber.metadata.keyspaceMetadata.set(ks, &KeyspaceMetadata{
			Name:   ks,
			Tables: map[string]*TableMetadata{},
		})
	}
}

// getCachedKeyspaces returns the set of keyspace names currently in the metadata cache.
func getCachedKeyspaces(s *Session) map[string]bool {
	result := make(map[string]bool)
	for k := range s.metadataDescriber.metadata.keyspaceMetadata.get() {
		result[k] = true
	}
	return result
}

func TestHandleSchemaEvent_RefreshAll_TableChange(t *testing.T) {
	t.Parallel()

	s := createTestSession(SchemaChangesRefreshAll)
	populateKeyspaceCache(s, "my_keyspace", "other_keyspace", "system")

	frames := []frame{
		&frm.SchemaChangeTable{
			Keyspace: "my_keyspace",
			Object:   "my_table",
			Change:   "CREATED",
		},
	}

	s.handleSchemaEvent(frames)

	// In RefreshAll mode, clearSchema is called for the affected keyspace,
	// then refreshAllSchema attempts to refresh ALL cached keyspaces.
	// Since there's no real DB, the refresh fails with ErrKeyspaceDoesNotExist
	// and all keyspaces get cleared from the cache.
	cached := getCachedKeyspaces(s)
	if cached["my_keyspace"] {
		t.Error("my_keyspace should have been cleared from cache (mock refresh fails with ErrKeyspaceDoesNotExist)")
	}
	if cached["other_keyspace"] {
		t.Error("other_keyspace should have been cleared in RefreshAll mode (all keyspaces are refreshed)")
	}
}

func TestHandleSchemaEvent_RefreshKeyspace_TableChange(t *testing.T) {
	t.Parallel()

	s := createTestSession(SchemaChangesRefreshKeyspace)
	populateKeyspaceCache(s, "my_keyspace", "other_keyspace", "system")

	frames := []frame{
		&frm.SchemaChangeTable{
			Keyspace: "my_keyspace",
			Object:   "my_table",
			Change:   "CREATED",
		},
	}

	s.handleSchemaEvent(frames)

	cached := getCachedKeyspaces(s)

	// In RefreshKeyspace mode, only my_keyspace and system keyspaces are refreshed.
	// Since there's no real DB, the refresh fails and my_keyspace gets cleared.
	// other_keyspace should remain untouched.
	if cached["my_keyspace"] {
		t.Error("my_keyspace should have been cleared (mock refresh fails with ErrKeyspaceDoesNotExist)")
	}
	if !cached["other_keyspace"] {
		t.Error("other_keyspace should still be in cache in RefreshKeyspace mode, but it was removed")
	}
}

func TestHandleSchemaEvent_RefreshTable_TableChange(t *testing.T) {
	t.Parallel()

	s := createTestSession(SchemaChangesRefreshTable)
	populateKeyspaceCache(s, "my_keyspace", "other_keyspace", "system")

	frames := []frame{
		&frm.SchemaChangeTable{
			Keyspace: "my_keyspace",
			Object:   "my_table",
			Change:   "UPDATED",
		},
	}

	s.handleSchemaEvent(frames)

	cached := getCachedKeyspaces(s)

	// In RefreshTable mode with a table-level event, only my_keyspace is affected.
	// The mock returns no rows so the table is treated as implicitly dropped.
	// The keyspace itself should remain in the cache (table-level refresh does not
	// clear the whole keyspace).
	if !cached["my_keyspace"] {
		t.Error("my_keyspace should remain in cache in RefreshTable mode (only the table is affected)")
	}
	if !cached["other_keyspace"] {
		t.Error("other_keyspace should still be in cache in RefreshTable mode, but it was removed")
	}
}

func TestHandleSchemaEvent_RefreshTable_NonTableChange(t *testing.T) {
	t.Parallel()

	s := createTestSession(SchemaChangesRefreshTable)
	populateKeyspaceCache(s, "my_keyspace", "other_keyspace", "system")

	// Send a keyspace-level event (not table-level)
	frames := []frame{
		&frm.SchemaChangeKeyspace{
			Keyspace: "my_keyspace",
			Change:   "UPDATED",
		},
	}

	s.handleSchemaEvent(frames)

	cached := getCachedKeyspaces(s)

	// In RefreshTable mode with a non-table event, the affected keyspace falls back
	// to keyspace-level refresh. Since mock has no real DB, the refresh clears it.
	// other_keyspace should remain.
	if cached["my_keyspace"] {
		t.Error("my_keyspace should have been cleared (keyspace-level fallback, mock refresh fails)")
	}
	if !cached["other_keyspace"] {
		t.Error("other_keyspace should still be in cache in RefreshTable mode with non-table event, but it was removed")
	}
}

func TestHandleSchemaEvent_RefreshAll_MultipleKeyspaces(t *testing.T) {
	t.Parallel()

	s := createTestSession(SchemaChangesRefreshAll)
	populateKeyspaceCache(s, "ks1", "ks2", "ks3", "system")

	frames := []frame{
		&frm.SchemaChangeTable{
			Keyspace: "ks1",
			Object:   "table1",
			Change:   "CREATED",
		},
		&frm.SchemaChangeTable{
			Keyspace: "ks2",
			Object:   "table2",
			Change:   "DROPPED",
		},
	}

	s.handleSchemaEvent(frames)

	// In RefreshAll mode, all cached keyspaces are refreshed.
	// Since mock has no real DB, all are cleared.
	cached := getCachedKeyspaces(s)
	if cached["ks1"] {
		t.Error("ks1 should have been cleared in RefreshAll mode")
	}
	if cached["ks2"] {
		t.Error("ks2 should have been cleared in RefreshAll mode")
	}
	if cached["ks3"] {
		t.Error("ks3 should have been cleared in RefreshAll mode (all keyspaces are refreshed)")
	}
}

func TestHandleSchemaEvent_RefreshKeyspace_MultipleKeyspaces(t *testing.T) {
	t.Parallel()

	s := createTestSession(SchemaChangesRefreshKeyspace)
	populateKeyspaceCache(s, "ks1", "ks2", "ks3", "system")

	frames := []frame{
		&frm.SchemaChangeTable{
			Keyspace: "ks1",
			Object:   "table1",
			Change:   "CREATED",
		},
		&frm.SchemaChangeFunction{
			Keyspace: "ks2",
			Name:     "my_func",
			Change:   "CREATED",
		},
	}

	s.handleSchemaEvent(frames)

	cached := getCachedKeyspaces(s)

	// ks1 and ks2 were affected; ks3 should remain untouched.
	if !cached["ks3"] {
		t.Error("ks3 should still be in cache in RefreshKeyspace mode, but it was removed")
	}
}

func TestHandleSchemaEvent_RefreshTable_MixedEvents(t *testing.T) {
	t.Parallel()

	s := createTestSession(SchemaChangesRefreshTable)
	populateKeyspaceCache(s, "ks1", "ks2", "ks3", "system")

	// Mix of table and non-table events in different keyspaces.
	frames := []frame{
		&frm.SchemaChangeTable{
			Keyspace: "ks1",
			Object:   "table1",
			Change:   "CREATED",
		},
		&frm.SchemaChangeType{
			Keyspace: "ks2",
			Object:   "my_type",
			Change:   "CREATED",
		},
	}

	s.handleSchemaEvent(frames)

	cached := getCachedKeyspaces(s)

	// ks1 and ks2 are affected, ks3 should remain untouched.
	if !cached["ks3"] {
		t.Error("ks3 should still be in cache in RefreshTable mode with mixed events, but it was removed")
	}
}

func TestHandleSchemaEvent_RefreshKeyspace_OnlyAffectedCleared(t *testing.T) {
	t.Parallel()

	s := createTestSession(SchemaChangesRefreshKeyspace)
	populateKeyspaceCache(s, "my_keyspace", "other_keyspace", "third_keyspace", "system")

	frames := []frame{
		&frm.SchemaChangeTable{
			Keyspace: "my_keyspace",
			Object:   "my_table",
			Change:   "DROPPED",
		},
	}

	s.handleSchemaEvent(frames)

	cached := getCachedKeyspaces(s)

	// Only my_keyspace should be cleared. other_keyspace and third_keyspace should be untouched.
	if !cached["other_keyspace"] {
		t.Error("other_keyspace should remain in cache but was removed")
	}
	if !cached["third_keyspace"] {
		t.Error("third_keyspace should remain in cache but was removed")
	}
}

func TestHandleSchemaEvent_RefreshTable_OnlyTableKeyspaceCleared(t *testing.T) {
	t.Parallel()

	s := createTestSession(SchemaChangesRefreshTable)
	populateKeyspaceCache(s, "my_keyspace", "other_keyspace", "third_keyspace", "system")

	frames := []frame{
		&frm.SchemaChangeTable{
			Keyspace: "my_keyspace",
			Object:   "my_table",
			Change:   "CREATED",
		},
	}

	s.handleSchemaEvent(frames)

	cached := getCachedKeyspaces(s)

	// Only my_keyspace should be affected. other_keyspace and third_keyspace should be untouched.
	if !cached["other_keyspace"] {
		t.Error("other_keyspace should remain in cache but was removed")
	}
	if !cached["third_keyspace"] {
		t.Error("third_keyspace should remain in cache but was removed")
	}
}

func TestHandleSchemaEvent_KeyspaceDropped(t *testing.T) {
	t.Parallel()

	for _, mode := range []SchemaChangesRefreshMode{
		SchemaChangesRefreshAll,
		SchemaChangesRefreshKeyspace,
		SchemaChangesRefreshTable,
	} {
		t.Run(mode.String(), func(t *testing.T) {
			s := createTestSession(mode)
			populateKeyspaceCache(s, "my_keyspace", "other_keyspace", "system")

			frames := []frame{
				&frm.SchemaChangeKeyspace{
					Keyspace: "my_keyspace",
					Change:   "DROPPED",
				},
			}

			s.handleSchemaEvent(frames)

			cached := getCachedKeyspaces(s)

			// The dropped keyspace should be removed from cache in all modes.
			if cached["my_keyspace"] {
				t.Errorf("[mode=%s] my_keyspace should have been cleared from cache after DROP", mode)
			}
		})
	}
}

func TestHandleSchemaEvent_AllSchemaChangeTypes(t *testing.T) {
	t.Parallel()

	// Test that all schema change frame types are handled without panics.
	for _, mode := range []SchemaChangesRefreshMode{
		SchemaChangesRefreshAll,
		SchemaChangesRefreshKeyspace,
		SchemaChangesRefreshTable,
	} {
		t.Run(mode.String(), func(t *testing.T) {
			s := createTestSession(mode)
			populateKeyspaceCache(s, "test_ks", "system")

			frames := []frame{
				&frm.SchemaChangeKeyspace{Keyspace: "test_ks", Change: "UPDATED"},
				&frm.SchemaChangeTable{Keyspace: "test_ks", Object: "t1", Change: "CREATED"},
				&frm.SchemaChangeType{Keyspace: "test_ks", Object: "my_type", Change: "CREATED"},
				&frm.SchemaChangeFunction{Keyspace: "test_ks", Name: "my_func", Change: "CREATED"},
				&frm.SchemaChangeAggregate{Keyspace: "test_ks", Name: "my_agg", Change: "CREATED"},
			}

			// Should not panic
			s.handleSchemaEvent(frames)
		})
	}
}

func TestHandleSchemaEvent_EmptyFrames(t *testing.T) {
	t.Parallel()

	for _, mode := range []SchemaChangesRefreshMode{
		SchemaChangesRefreshAll,
		SchemaChangesRefreshKeyspace,
		SchemaChangesRefreshTable,
	} {
		t.Run(mode.String(), func(t *testing.T) {
			s := createTestSession(mode)
			populateKeyspaceCache(s, "my_keyspace", "system")

			// Empty frame list should be handled gracefully without clearing cache.
			s.handleSchemaEvent(nil)
			s.handleSchemaEvent([]frame{})

			cached := getCachedKeyspaces(s)
			if !cached["my_keyspace"] {
				t.Error("my_keyspace should remain in cache when no events are processed")
			}
		})
	}
}

func TestHandleSchemaEvent_SystemKeyspacePreserved(t *testing.T) {
	t.Parallel()

	// In RefreshKeyspace and RefreshTable modes, a schema change to a user keyspace
	// should NOT affect other non-affected user keyspaces.
	for _, mode := range []SchemaChangesRefreshMode{
		SchemaChangesRefreshKeyspace,
		SchemaChangesRefreshTable,
	} {
		t.Run(mode.String(), func(t *testing.T) {
			s := createTestSession(mode)
			populateKeyspaceCache(s, "user_ks", "untouched_ks", "system", "system_auth")

			frames := []frame{
				&frm.SchemaChangeTable{
					Keyspace: "user_ks",
					Object:   "my_table",
					Change:   "CREATED",
				},
			}

			s.handleSchemaEvent(frames)

			cached := getCachedKeyspaces(s)

			// untouched_ks should remain in cache — it was not affected by the event.
			if !cached["untouched_ks"] {
				t.Errorf("[mode=%s] untouched_ks should remain in cache but was removed", mode)
			}

			// System keyspaces are refreshed but cleared by mock (no real DB).
			// The key point is untouched_ks is not affected.
		})
	}
}

func TestHandleSchemaEvent_RefreshKeyspace_SystemKeyspaceEvent(t *testing.T) {
	t.Parallel()

	s := createTestSession(SchemaChangesRefreshKeyspace)
	populateKeyspaceCache(s, "user_ks", "system", "system_auth")

	// Event on a system keyspace itself
	frames := []frame{
		&frm.SchemaChangeTable{
			Keyspace: "system_auth",
			Object:   "roles",
			Change:   "UPDATED",
		},
	}

	s.handleSchemaEvent(frames)

	cached := getCachedKeyspaces(s)

	// user_ks should remain untouched since the event only affects system_auth.
	if !cached["user_ks"] {
		t.Error("user_ks should remain in cache when event is for system_auth")
	}
}

func TestHandleSchemaEvent_TableChange_TabletRemoval(t *testing.T) {
	t.Parallel()

	for _, mode := range []SchemaChangesRefreshMode{
		SchemaChangesRefreshAll,
		SchemaChangesRefreshKeyspace,
		SchemaChangesRefreshTable,
	} {
		t.Run(mode.String(), func(t *testing.T) {
			s := createTestSession(mode)

			s.metadataDescriber.metadata.tabletsMetadata = tablets.NewCowTabletList()
			builder := tablets.NewTabletInfoBuilder()
			builder.KeyspaceName = "my_keyspace"
			builder.TableName = "my_table"
			builder.FirstToken = 0
			builder.LastToken = 100
			tablet, err := builder.Build()
			if err != nil {
				t.Fatalf("failed to build tablet: %v", err)
			}
			s.metadataDescriber.AddTablet(tablet)

			populateKeyspaceCache(s, "my_keyspace", "system")

			tabletsBefore := s.metadataDescriber.getTablets()
			if len(tabletsBefore) != 1 {
				t.Fatalf("expected 1 tablet before event, got %d", len(tabletsBefore))
			}

			frames := []frame{
				&frm.SchemaChangeTable{
					Keyspace: "my_keyspace",
					Object:   "my_table",
					Change:   "DROPPED",
				},
			}

			s.handleSchemaEvent(frames)

			// After DROP, tablets for this table should be removed.
			tabletsAfter := s.metadataDescriber.getTablets()
			if len(tabletsAfter) != 0 {
				t.Errorf("expected 0 tablets after DROP event, got %d", len(tabletsAfter))
			}
		})
	}
}

func TestHandleSchemaEvent_KeyspaceChange_TabletRemoval(t *testing.T) {
	t.Parallel()

	for _, mode := range []SchemaChangesRefreshMode{
		SchemaChangesRefreshAll,
		SchemaChangesRefreshKeyspace,
		SchemaChangesRefreshTable,
	} {
		t.Run(mode.String(), func(t *testing.T) {
			s := createTestSession(mode)

			s.metadataDescriber.metadata.tabletsMetadata = tablets.NewCowTabletList()

			builder1 := tablets.NewTabletInfoBuilder()
			builder1.KeyspaceName = "my_keyspace"
			builder1.TableName = "table1"
			builder1.FirstToken = 0
			builder1.LastToken = 50
			tablet1, err := builder1.Build()
			if err != nil {
				t.Fatalf("failed to build tablet1: %v", err)
			}
			s.metadataDescriber.AddTablet(tablet1)

			builder2 := tablets.NewTabletInfoBuilder()
			builder2.KeyspaceName = "my_keyspace"
			builder2.TableName = "table2"
			builder2.FirstToken = 51
			builder2.LastToken = 100
			tablet2, err := builder2.Build()
			if err != nil {
				t.Fatalf("failed to build tablet2: %v", err)
			}
			s.metadataDescriber.AddTablet(tablet2)

			populateKeyspaceCache(s, "my_keyspace", "system")

			frames := []frame{
				&frm.SchemaChangeKeyspace{
					Keyspace: "my_keyspace",
					Change:   "DROPPED",
				},
			}

			s.handleSchemaEvent(frames)

			// After keyspace DROP, all tablets for this keyspace should be removed.
			tabletsAfter := s.metadataDescriber.getTablets()
			if len(tabletsAfter) != 0 {
				t.Errorf("expected 0 tablets after keyspace DROP event, got %d", len(tabletsAfter))
			}
		})
	}
}

func TestHandleSchemaEvent_RefreshTable_DuplicateKeyspaceEvents(t *testing.T) {
	t.Parallel()

	s := createTestSession(SchemaChangesRefreshTable)
	populateKeyspaceCache(s, "my_keyspace", "other_keyspace", "system")

	// Multiple table events for the same keyspace — deduplication via affectedKeyspaces map
	// ensures the keyspace is only refreshed once.
	frames := []frame{
		&frm.SchemaChangeTable{
			Keyspace: "my_keyspace",
			Object:   "table1",
			Change:   "CREATED",
		},
		&frm.SchemaChangeTable{
			Keyspace: "my_keyspace",
			Object:   "table2",
			Change:   "UPDATED",
		},
	}

	s.handleSchemaEvent(frames)

	cached := getCachedKeyspaces(s)

	// other_keyspace should remain untouched.
	if !cached["other_keyspace"] {
		t.Error("other_keyspace should remain in cache but was removed")
	}
}

func TestSchemaChangesRefreshMode_DefaultIsRefreshAll(t *testing.T) {
	t.Parallel()

	cfg := NewCluster("127.0.0.1")
	if cfg.SchemaChangesRefreshMode != SchemaChangesRefreshAll {
		t.Errorf("expected default SchemaChangesRefreshMode to be SchemaChangesRefreshAll (0), got %d (%s)",
			cfg.SchemaChangesRefreshMode, cfg.SchemaChangesRefreshMode)
	}
}

func TestSchemaChangesRefreshMode_Validate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		mode    SchemaChangesRefreshMode
		wantErr bool
	}{
		{"RefreshAll is valid", SchemaChangesRefreshAll, false},
		{"RefreshKeyspace is valid", SchemaChangesRefreshKeyspace, false},
		{"RefreshTable is valid", SchemaChangesRefreshTable, false},
		{"negative value is invalid", SchemaChangesRefreshMode(-1), true},
		{"value above max is invalid", SchemaChangesRefreshTable + 1, true},
		{"large value is invalid", SchemaChangesRefreshMode(42), true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := NewCluster("127.0.0.1")
			cfg.SchemaChangesRefreshMode = tt.mode
			err := cfg.Validate()
			if tt.wantErr && err == nil {
				t.Errorf("expected Validate() to return error for mode %d, got nil", tt.mode)
			}
			if !tt.wantErr && err != nil {
				t.Errorf("expected Validate() to succeed for mode %d, got: %v", tt.mode, err)
			}
		})
	}
}

func TestRefreshKeyspacesSchema_SystemKeyspaceSkipped(t *testing.T) {
	t.Parallel()

	// When refreshKeyspacesSchema is called with a system keyspace,
	// the system keyspace is refreshed via refreshSystemKeyspacesLocked (not double-refreshed).
	s := createTestSession(SchemaChangesRefreshKeyspace)
	populateKeyspaceCache(s, "system", "system_auth")

	// Should not panic; error is expected since mock DB returns no rows.
	err := s.metadataDescriber.refreshKeyspacesSchema(map[string]struct{}{
		"system": {},
	})
	if err != nil {
		t.Errorf("unexpected error from refreshKeyspacesSchema: %v", err)
	}
}

func TestRefreshKeyspacesSchema_BatchedRefresh(t *testing.T) {
	t.Parallel()

	// Verify that refreshKeyspacesSchema handles multiple keyspaces in a single call
	// without panics. The lock is acquired once for the entire batch.
	s := createTestSession(SchemaChangesRefreshKeyspace)
	populateKeyspaceCache(s, "ks1", "ks2", "ks3", "system", "system_auth")

	err := s.metadataDescriber.refreshKeyspacesSchema(map[string]struct{}{
		"ks1": {},
		"ks2": {},
	})
	if err != nil {
		t.Errorf("unexpected error from refreshKeyspacesSchema: %v", err)
	}

	cached := getCachedKeyspaces(s)

	// ks3 should be untouched — it was not in the affected set and is not a system keyspace.
	if !cached["ks3"] {
		t.Error("ks3 should remain in cache but was removed")
	}
}

// --- Table-level refresh tests ---

// populateKeyspaceCacheWithTables creates a keyspace cache entry with tables, indexes, and views.
func populateKeyspaceCacheWithTables(s *Session, keyspaceName string, tables map[string]*TableMetadata, indexes map[string]*IndexMetadata, views map[string]*ViewMetadata) {
	ks := &KeyspaceMetadata{
		Name:   keyspaceName,
		Tables: tables,
	}
	if indexes != nil {
		ks.Indexes = indexes
	} else {
		ks.Indexes = map[string]*IndexMetadata{}
	}
	if views != nil {
		ks.Views = views
	} else {
		ks.Views = map[string]*ViewMetadata{}
	}
	ks.Functions = map[string]*FunctionMetadata{}
	ks.Aggregates = map[string]*AggregateMetadata{}
	ks.Types = map[string]*TypeMetadata{}
	s.metadataDescriber.metadata.keyspaceMetadata.set(keyspaceName, ks)
}

func TestCompileTableMetadata_BasicTable(t *testing.T) {
	t.Parallel()

	tables := []TableMetadata{
		{Keyspace: "ks", Name: "users"},
	}
	columns := []ColumnMetadata{
		{Keyspace: "ks", Table: "users", Name: "id", Kind: ColumnPartitionKey, ComponentIndex: 0},
		{Keyspace: "ks", Table: "users", Name: "name", Kind: ColumnRegular},
		{Keyspace: "ks", Table: "users", Name: "age", Kind: ColumnRegular},
	}

	table, indexes, views := compileTableMetadata(tables, columns, nil, nil)

	if table == nil {
		t.Fatal("expected compiled table, got nil")
	}
	if table.Name != "users" {
		t.Errorf("expected table name 'users', got %q", table.Name)
	}
	if len(table.Columns) != 3 {
		t.Errorf("expected 3 columns, got %d", len(table.Columns))
	}
	if len(table.PartitionKey) != 1 {
		t.Errorf("expected 1 partition key column, got %d", len(table.PartitionKey))
	}
	if table.PartitionKey[0].Name != "id" {
		t.Errorf("expected partition key 'id', got %q", table.PartitionKey[0].Name)
	}
	if len(indexes) != 0 {
		t.Errorf("expected 0 indexes, got %d", len(indexes))
	}
	if len(views) != 0 {
		t.Errorf("expected 0 views, got %d", len(views))
	}
}

func TestCompileTableMetadata_WithClusteringKey(t *testing.T) {
	t.Parallel()

	tables := []TableMetadata{
		{Keyspace: "ks", Name: "events"},
	}
	columns := []ColumnMetadata{
		{Keyspace: "ks", Table: "events", Name: "user_id", Kind: ColumnPartitionKey, ComponentIndex: 0},
		{Keyspace: "ks", Table: "events", Name: "event_time", Kind: ColumnClusteringKey, ComponentIndex: 0, ClusteringOrder: "desc"},
		{Keyspace: "ks", Table: "events", Name: "data", Kind: ColumnRegular},
	}

	table, _, _ := compileTableMetadata(tables, columns, nil, nil)

	if table == nil {
		t.Fatal("expected compiled table, got nil")
	}
	if len(table.ClusteringColumns) != 1 {
		t.Fatalf("expected 1 clustering column, got %d", len(table.ClusteringColumns))
	}
	if table.ClusteringColumns[0].Name != "event_time" {
		t.Errorf("expected clustering column 'event_time', got %q", table.ClusteringColumns[0].Name)
	}
	if table.ClusteringColumns[0].Order != DESC {
		t.Errorf("expected DESC order for clustering column, got %v", table.ClusteringColumns[0].Order)
	}
}

func TestCompileTableMetadata_WithIndex(t *testing.T) {
	t.Parallel()

	tables := []TableMetadata{
		{Keyspace: "ks", Name: "users"},
	}
	columns := []ColumnMetadata{
		{Keyspace: "ks", Table: "users", Name: "id", Kind: ColumnPartitionKey, ComponentIndex: 0},
		{Keyspace: "ks", Table: "users", Name: "email", Kind: ColumnRegular},
		// Index backing view columns (table_name = "email_idx_index")
		{Keyspace: "ks", Table: "email_idx_index", Name: "email", Kind: ColumnPartitionKey, ComponentIndex: 0},
		{Keyspace: "ks", Table: "email_idx_index", Name: "id", Kind: ColumnClusteringKey, ComponentIndex: 0},
	}
	indexes := []IndexMetadata{
		{Name: "email_idx", KeyspaceName: "ks", TableName: "users", Kind: "COMPOSITES"},
	}

	table, compiledIndexes, compiledViews := compileTableMetadata(tables, columns, indexes, nil)

	if table == nil {
		t.Fatal("expected compiled table, got nil")
	}
	if len(table.Columns) != 2 {
		t.Errorf("expected 2 table columns, got %d", len(table.Columns))
	}
	if len(compiledIndexes) != 1 {
		t.Fatalf("expected 1 index, got %d", len(compiledIndexes))
	}
	idx := compiledIndexes["email_idx"]
	if idx == nil {
		t.Fatal("expected email_idx index")
	}
	if len(idx.Columns) != 2 {
		t.Errorf("expected 2 index columns, got %d", len(idx.Columns))
	}
	if len(compiledViews) != 0 {
		t.Errorf("expected 0 views, got %d", len(compiledViews))
	}
}

func TestCompileTableMetadata_WithView(t *testing.T) {
	t.Parallel()

	tables := []TableMetadata{
		{Keyspace: "ks", Name: "users"},
	}
	columns := []ColumnMetadata{
		{Keyspace: "ks", Table: "users", Name: "id", Kind: ColumnPartitionKey, ComponentIndex: 0},
		{Keyspace: "ks", Table: "users", Name: "name", Kind: ColumnRegular},
		// View columns
		{Keyspace: "ks", Table: "users_by_name", Name: "name", Kind: ColumnPartitionKey, ComponentIndex: 0},
		{Keyspace: "ks", Table: "users_by_name", Name: "id", Kind: ColumnClusteringKey, ComponentIndex: 0},
	}
	views := []ViewMetadata{
		{KeyspaceName: "ks", ViewName: "users_by_name", BaseTableName: "users"},
	}

	table, _, compiledViews := compileTableMetadata(tables, columns, nil, views)

	if table == nil {
		t.Fatal("expected compiled table, got nil")
	}
	if len(table.Columns) != 2 {
		t.Errorf("expected 2 table columns, got %d", len(table.Columns))
	}
	if len(compiledViews) != 1 {
		t.Fatalf("expected 1 view, got %d", len(compiledViews))
	}
	view := compiledViews["users_by_name"]
	if view == nil {
		t.Fatal("expected users_by_name view")
	}
	if len(view.Columns) != 2 {
		t.Errorf("expected 2 view columns, got %d", len(view.Columns))
	}
	if len(view.PartitionKey) != 1 {
		t.Errorf("expected 1 partition key in view, got %d", len(view.PartitionKey))
	}
}

func TestCompileTableMetadata_Empty(t *testing.T) {
	t.Parallel()

	table, indexes, views := compileTableMetadata(nil, nil, nil, nil)
	if table != nil {
		t.Error("expected nil table for empty input")
	}
	if indexes != nil {
		t.Error("expected nil indexes for empty input")
	}
	if views != nil {
		t.Error("expected nil views for empty input")
	}
}

func TestMergeTableIntoCache_AddNewTable(t *testing.T) {
	t.Parallel()

	s := createTestSession(SchemaChangesRefreshTable)

	existingTables := map[string]*TableMetadata{
		"existing_table": {Keyspace: "ks", Name: "existing_table", Columns: map[string]*ColumnMetadata{}},
	}
	populateKeyspaceCacheWithTables(s, "ks", existingTables, nil, nil)

	newTable := &TableMetadata{Keyspace: "ks", Name: "new_table", Columns: map[string]*ColumnMetadata{
		"id": {Keyspace: "ks", Table: "new_table", Name: "id"},
	}}

	s.metadataDescriber.mu.Lock()
	err := s.metadataDescriber.mergeTableIntoCache("ks", "new_table", newTable, nil, nil)
	s.metadataDescriber.mu.Unlock()

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	ks, ok := s.metadataDescriber.metadata.keyspaceMetadata.getKeyspace("ks")
	if !ok {
		t.Fatal("keyspace 'ks' not found in cache")
	}
	if len(ks.Tables) != 2 {
		t.Errorf("expected 2 tables, got %d", len(ks.Tables))
	}
	if _, ok := ks.Tables["existing_table"]; !ok {
		t.Error("existing_table should still be in cache")
	}
	if _, ok := ks.Tables["new_table"]; !ok {
		t.Error("new_table should be in cache")
	}
}

func TestMergeTableIntoCache_UpdateExistingTable(t *testing.T) {
	t.Parallel()

	s := createTestSession(SchemaChangesRefreshTable)

	existingTables := map[string]*TableMetadata{
		"my_table": {Keyspace: "ks", Name: "my_table", Columns: map[string]*ColumnMetadata{
			"id": {Keyspace: "ks", Table: "my_table", Name: "id"},
		}},
	}
	populateKeyspaceCacheWithTables(s, "ks", existingTables, nil, nil)

	// Updated table with an additional column.
	updatedTable := &TableMetadata{Keyspace: "ks", Name: "my_table", Columns: map[string]*ColumnMetadata{
		"id":   {Keyspace: "ks", Table: "my_table", Name: "id"},
		"name": {Keyspace: "ks", Table: "my_table", Name: "name"},
	}}

	s.metadataDescriber.mu.Lock()
	err := s.metadataDescriber.mergeTableIntoCache("ks", "my_table", updatedTable, nil, nil)
	s.metadataDescriber.mu.Unlock()

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	ks, _ := s.metadataDescriber.metadata.keyspaceMetadata.getKeyspace("ks")
	if len(ks.Tables) != 1 {
		t.Errorf("expected 1 table, got %d", len(ks.Tables))
	}
	tbl := ks.Tables["my_table"]
	if tbl == nil {
		t.Fatal("my_table not found in cache")
	}
	if len(tbl.Columns) != 2 {
		t.Errorf("expected 2 columns after update, got %d", len(tbl.Columns))
	}
}

func TestMergeTableIntoCache_ReplacesOldIndexes(t *testing.T) {
	t.Parallel()

	s := createTestSession(SchemaChangesRefreshTable)

	existingTables := map[string]*TableMetadata{
		"my_table": {Keyspace: "ks", Name: "my_table"},
	}
	existingIndexes := map[string]*IndexMetadata{
		"old_idx":   {Name: "old_idx", TableName: "my_table"},
		"other_idx": {Name: "other_idx", TableName: "other_table"},
	}
	populateKeyspaceCacheWithTables(s, "ks", existingTables, existingIndexes, nil)

	newTable := &TableMetadata{Keyspace: "ks", Name: "my_table"}
	newIndexes := map[string]*IndexMetadata{
		"new_idx": {Name: "new_idx", TableName: "my_table"},
	}

	s.metadataDescriber.mu.Lock()
	err := s.metadataDescriber.mergeTableIntoCache("ks", "my_table", newTable, newIndexes, nil)
	s.metadataDescriber.mu.Unlock()

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	ks, _ := s.metadataDescriber.metadata.keyspaceMetadata.getKeyspace("ks")

	// old_idx for my_table should be removed, new_idx should be present.
	if _, ok := ks.Indexes["old_idx"]; ok {
		t.Error("old_idx should have been removed")
	}
	if _, ok := ks.Indexes["new_idx"]; !ok {
		t.Error("new_idx should be present")
	}
	// other_idx (for other_table) should be preserved.
	if _, ok := ks.Indexes["other_idx"]; !ok {
		t.Error("other_idx for other_table should be preserved")
	}
}

func TestMergeTableIntoCache_ReplacesOldViews(t *testing.T) {
	t.Parallel()

	s := createTestSession(SchemaChangesRefreshTable)

	existingTables := map[string]*TableMetadata{
		"my_table": {Keyspace: "ks", Name: "my_table"},
	}
	existingViews := map[string]*ViewMetadata{
		"old_view":   {ViewName: "old_view", BaseTableName: "my_table"},
		"other_view": {ViewName: "other_view", BaseTableName: "other_table"},
	}
	populateKeyspaceCacheWithTables(s, "ks", existingTables, nil, existingViews)

	newTable := &TableMetadata{Keyspace: "ks", Name: "my_table"}
	newViews := map[string]*ViewMetadata{
		"new_view": {ViewName: "new_view", BaseTableName: "my_table"},
	}

	s.metadataDescriber.mu.Lock()
	err := s.metadataDescriber.mergeTableIntoCache("ks", "my_table", newTable, nil, newViews)
	s.metadataDescriber.mu.Unlock()

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	ks, _ := s.metadataDescriber.metadata.keyspaceMetadata.getKeyspace("ks")

	if _, ok := ks.Views["old_view"]; ok {
		t.Error("old_view should have been removed")
	}
	if _, ok := ks.Views["new_view"]; !ok {
		t.Error("new_view should be present")
	}
	if _, ok := ks.Views["other_view"]; !ok {
		t.Error("other_view for other_table should be preserved")
	}
}

func TestRemoveTableFromCache_Basic(t *testing.T) {
	t.Parallel()

	s := createTestSession(SchemaChangesRefreshTable)

	existingTables := map[string]*TableMetadata{
		"table1": {Keyspace: "ks", Name: "table1"},
		"table2": {Keyspace: "ks", Name: "table2"},
	}
	existingIndexes := map[string]*IndexMetadata{
		"idx1": {Name: "idx1", TableName: "table1"},
		"idx2": {Name: "idx2", TableName: "table2"},
	}
	existingViews := map[string]*ViewMetadata{
		"view1": {ViewName: "view1", BaseTableName: "table1"},
		"view2": {ViewName: "view2", BaseTableName: "table2"},
	}
	populateKeyspaceCacheWithTables(s, "ks", existingTables, existingIndexes, existingViews)

	s.metadataDescriber.mu.Lock()
	err := s.metadataDescriber.removeTableFromCache("ks", "table1")
	s.metadataDescriber.mu.Unlock()

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	ks, _ := s.metadataDescriber.metadata.keyspaceMetadata.getKeyspace("ks")

	// table1 and its index/view should be removed.
	if _, ok := ks.Tables["table1"]; ok {
		t.Error("table1 should have been removed")
	}
	if _, ok := ks.Indexes["idx1"]; ok {
		t.Error("idx1 should have been removed")
	}
	if _, ok := ks.Views["view1"]; ok {
		t.Error("view1 should have been removed")
	}

	// table2 and its index/view should remain.
	if _, ok := ks.Tables["table2"]; !ok {
		t.Error("table2 should remain")
	}
	if _, ok := ks.Indexes["idx2"]; !ok {
		t.Error("idx2 should remain")
	}
	if _, ok := ks.Views["view2"]; !ok {
		t.Error("view2 should remain")
	}
}

func TestRemoveTableFromCache_NonExistentKeyspace(t *testing.T) {
	t.Parallel()

	s := createTestSession(SchemaChangesRefreshTable)

	// Removing from a non-existent keyspace should not panic.
	s.metadataDescriber.mu.Lock()
	err := s.metadataDescriber.removeTableFromCache("nonexistent", "table1")
	s.metadataDescriber.mu.Unlock()

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestHandleSchemaEvent_RefreshTable_DropRemovesTable(t *testing.T) {
	t.Parallel()

	s := createTestSession(SchemaChangesRefreshTable)

	existingTables := map[string]*TableMetadata{
		"table1": {Keyspace: "my_keyspace", Name: "table1"},
		"table2": {Keyspace: "my_keyspace", Name: "table2"},
	}
	existingIndexes := map[string]*IndexMetadata{
		"idx1": {Name: "idx1", TableName: "table1"},
	}
	populateKeyspaceCacheWithTables(s, "my_keyspace", existingTables, existingIndexes, nil)
	populateKeyspaceCache(s, "system")

	frames := []frame{
		&frm.SchemaChangeTable{
			Keyspace: "my_keyspace",
			Object:   "table1",
			Change:   "DROPPED",
		},
	}

	s.handleSchemaEvent(frames)

	ks, ok := s.metadataDescriber.metadata.keyspaceMetadata.getKeyspace("my_keyspace")
	if !ok {
		t.Fatal("keyspace my_keyspace should still exist in cache")
	}
	if _, ok := ks.Tables["table1"]; ok {
		t.Error("table1 should have been removed after DROP")
	}
	if _, ok := ks.Indexes["idx1"]; ok {
		t.Error("idx1 should have been removed after table1 DROP")
	}
	if _, ok := ks.Tables["table2"]; !ok {
		t.Error("table2 should remain after table1 DROP")
	}
}

func TestHandleSchemaEvent_RefreshTable_CreateDoesNotPanic(t *testing.T) {
	t.Parallel()

	s := createTestSession(SchemaChangesRefreshTable)

	// Pre-populate with an existing keyspace containing one table.
	existingTables := map[string]*TableMetadata{
		"existing": {Keyspace: "ks", Name: "existing"},
	}
	populateKeyspaceCacheWithTables(s, "ks", existingTables, nil, nil)
	populateKeyspaceCache(s, "system")

	// CREATE event for a new table — with mock DB the query returns no rows,
	// so the table is treated as implicitly dropped (no-op on cache).
	frames := []frame{
		&frm.SchemaChangeTable{
			Keyspace: "ks",
			Object:   "new_table",
			Change:   "CREATED",
		},
	}

	// Should not panic.
	s.handleSchemaEvent(frames)

	// existing table should still be there.
	ks, ok := s.metadataDescriber.metadata.keyspaceMetadata.getKeyspace("ks")
	if !ok {
		t.Fatal("keyspace 'ks' should still be in cache")
	}
	if _, ok := ks.Tables["existing"]; !ok {
		t.Error("existing table should still be in cache")
	}
}

func TestHandleSchemaEvent_RefreshTable_MixedTableAndKeyspaceEvents(t *testing.T) {
	t.Parallel()

	s := createTestSession(SchemaChangesRefreshTable)

	// ks1 has tables.
	existingTables := map[string]*TableMetadata{
		"table_a": {Keyspace: "ks1", Name: "table_a"},
		"table_b": {Keyspace: "ks1", Name: "table_b"},
	}
	populateKeyspaceCacheWithTables(s, "ks1", existingTables, nil, nil)
	populateKeyspaceCache(s, "ks2", "ks3", "system")

	// A table DROP in ks1 and a type change in ks2 (keyspace-level fallback).
	frames := []frame{
		&frm.SchemaChangeTable{
			Keyspace: "ks1",
			Object:   "table_a",
			Change:   "DROPPED",
		},
		&frm.SchemaChangeType{
			Keyspace: "ks2",
			Object:   "my_type",
			Change:   "CREATED",
		},
	}

	s.handleSchemaEvent(frames)

	// ks1: table_a should be removed, table_b should remain.
	ks1, ok := s.metadataDescriber.metadata.keyspaceMetadata.getKeyspace("ks1")
	if !ok {
		t.Fatal("keyspace ks1 should still exist")
	}
	if _, ok := ks1.Tables["table_a"]; ok {
		t.Error("table_a should have been removed after DROP")
	}
	if _, ok := ks1.Tables["table_b"]; !ok {
		t.Error("table_b should remain after table_a DROP")
	}

	// ks3 should be completely untouched.
	if cached := getCachedKeyspaces(s); !cached["ks3"] {
		t.Error("ks3 should remain untouched")
	}
}

func TestRefreshTablesSchema_TableChangeSkipsKeyspaceFallback(t *testing.T) {
	t.Parallel()

	s := createTestSession(SchemaChangesRefreshTable)

	// Set up ks1 with tables, plus system.
	existingTables := map[string]*TableMetadata{
		"table1": {Keyspace: "ks1", Name: "table1"},
		"table2": {Keyspace: "ks1", Name: "table2"},
	}
	populateKeyspaceCacheWithTables(s, "ks1", existingTables, nil, nil)
	populateKeyspaceCache(s, "system")

	// Table-only change (no keyspace fallbacks).
	tableChanges := []tableChange{
		{keyspace: "ks1", table: "table1", change: "DROPPED"},
	}

	err := s.metadataDescriber.refreshTablesSchema(tableChanges, map[string]struct{}{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	ks1, ok := s.metadataDescriber.metadata.keyspaceMetadata.getKeyspace("ks1")
	if !ok {
		t.Fatal("ks1 should still be in cache")
	}
	if _, ok := ks1.Tables["table1"]; ok {
		t.Error("table1 should have been removed by table-level drop")
	}
	if _, ok := ks1.Tables["table2"]; !ok {
		t.Error("table2 should remain after table1 drop")
	}
}

func TestMergeTableIntoCache_NoExistingKeyspace(t *testing.T) {
	t.Parallel()

	s := createTestSession(SchemaChangesRefreshTable)

	// Don't populate any cache.
	newTable := &TableMetadata{Keyspace: "ks", Name: "new_table"}

	s.metadataDescriber.mu.Lock()
	err := s.metadataDescriber.mergeTableIntoCache("ks", "new_table", newTable, nil, nil)
	s.metadataDescriber.mu.Unlock()

	// Should fall back to refreshSchema which will fail with ErrKeyspaceDoesNotExist.
	// The error should not be propagated since it's handled gracefully.
	if err != nil && err != ErrKeyspaceDoesNotExist {
		t.Errorf("unexpected error type: %v", err)
	}
}

func TestCompileTableMetadata_OrderedColumns(t *testing.T) {
	t.Parallel()

	tables := []TableMetadata{
		{Keyspace: "ks", Name: "events"},
	}
	columns := []ColumnMetadata{
		{Keyspace: "ks", Table: "events", Name: "data", Kind: ColumnRegular},
		{Keyspace: "ks", Table: "events", Name: "user_id", Kind: ColumnPartitionKey, ComponentIndex: 0},
		{Keyspace: "ks", Table: "events", Name: "event_time", Kind: ColumnClusteringKey, ComponentIndex: 0},
	}

	table, _, _ := compileTableMetadata(tables, columns, nil, nil)

	if table == nil {
		t.Fatal("expected compiled table")
	}

	// OrderedColumns should be: partition keys, clustering keys, then other columns.
	expected := []string{"user_id", "event_time", "data"}
	if len(table.OrderedColumns) != len(expected) {
		t.Fatalf("expected %d ordered columns, got %d: %v", len(expected), len(table.OrderedColumns), table.OrderedColumns)
	}
	for i, name := range expected {
		if table.OrderedColumns[i] != name {
			t.Errorf("ordered column %d: expected %q, got %q", i, name, table.OrderedColumns[i])
		}
	}
}

// --- New tests for coverage gaps ---

func TestRefreshTablesSchema_WithKeyspaceFallbacks(t *testing.T) {
	t.Parallel()

	s := createTestSession(SchemaChangesRefreshTable)

	// ks1: has tables (will receive table change)
	// ks2: will receive keyspace-level fallback (type change)
	existingTables1 := map[string]*TableMetadata{
		"table1": {Keyspace: "ks1", Name: "table1"},
		"table2": {Keyspace: "ks1", Name: "table2"},
	}
	populateKeyspaceCacheWithTables(s, "ks1", existingTables1, nil, nil)
	populateKeyspaceCache(s, "ks2", "ks3", "system")

	tableChanges := []tableChange{
		{keyspace: "ks1", table: "table1", change: "DROPPED"},
	}
	keyspaceFallbacks := map[string]struct{}{
		"ks2": {},
	}

	err := s.metadataDescriber.refreshTablesSchema(tableChanges, keyspaceFallbacks)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// ks1: table1 should be removed, table2 should remain.
	ks1, ok := s.metadataDescriber.metadata.keyspaceMetadata.getKeyspace("ks1")
	if !ok {
		t.Fatal("ks1 should still be in cache")
	}
	if _, ok := ks1.Tables["table1"]; ok {
		t.Error("table1 should have been removed by table-level drop")
	}
	if _, ok := ks1.Tables["table2"]; !ok {
		t.Error("table2 should remain after table1 drop")
	}

	// ks3 should remain untouched.
	cached := getCachedKeyspaces(s)
	if !cached["ks3"] {
		t.Error("ks3 should remain in cache but was removed")
	}
}

func TestRefreshTablesSchema_TableChangeInFallbackKeyspaceSkipped(t *testing.T) {
	t.Parallel()

	// When a keyspace appears in both tableChanges and keyspaceFallbacks,
	// the table change should be skipped (keyspace was already fully refreshed).
	s := createTestSession(SchemaChangesRefreshTable)

	existingTables := map[string]*TableMetadata{
		"table1": {Keyspace: "ks1", Name: "table1"},
		"table2": {Keyspace: "ks1", Name: "table2"},
	}
	populateKeyspaceCacheWithTables(s, "ks1", existingTables, nil, nil)
	populateKeyspaceCache(s, "system")

	tableChanges := []tableChange{
		{keyspace: "ks1", table: "table1", change: "DROPPED"},
	}
	keyspaceFallbacks := map[string]struct{}{
		"ks1": {}, // same keyspace as the table change
	}

	err := s.metadataDescriber.refreshTablesSchema(tableChanges, keyspaceFallbacks)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Since ks1 is in keyspaceFallbacks, it was refreshed at keyspace level.
	// The table-level drop should NOT have been applied — the keyspace-level
	// refresh already cleared everything (mock returns ErrKeyspaceDoesNotExist).
	// The keyspace should have been cleared by the fallback refresh.
	cached := getCachedKeyspaces(s)
	if cached["ks1"] {
		t.Error("ks1 should have been cleared by keyspace-level fallback refresh")
	}
}

func TestHandleSchemaEvent_RefreshTable_SameKeyspaceTableAndNonTableEvent(t *testing.T) {
	t.Parallel()

	// When the same keyspace has both a table event and a non-table event in RefreshTable mode,
	// the keyspace should appear in keyspaceFallbacks and the table change should be skipped.
	s := createTestSession(SchemaChangesRefreshTable)

	existingTables := map[string]*TableMetadata{
		"table1": {Keyspace: "ks1", Name: "table1"},
		"table2": {Keyspace: "ks1", Name: "table2"},
	}
	populateKeyspaceCacheWithTables(s, "ks1", existingTables, nil, nil)
	populateKeyspaceCache(s, "ks2", "system")

	// Both a table event and a type event for ks1.
	frames := []frame{
		&frm.SchemaChangeTable{
			Keyspace: "ks1",
			Object:   "table1",
			Change:   "DROPPED",
		},
		&frm.SchemaChangeType{
			Keyspace: "ks1",
			Object:   "my_type",
			Change:   "CREATED",
		},
	}

	s.handleSchemaEvent(frames)

	// ks2 should remain untouched.
	cached := getCachedKeyspaces(s)
	if !cached["ks2"] {
		t.Error("ks2 should remain in cache but was removed")
	}

	// ks1 was in keyspaceFallbacks due to the type event, so the whole keyspace
	// was refreshed at keyspace level (which clears it since mock has no real DB).
	if cached["ks1"] {
		t.Error("ks1 should have been cleared by keyspace-level fallback refresh")
	}
}

func TestRefreshTablesSchema_SystemKeyspaceTableChangeSkipped(t *testing.T) {
	t.Parallel()

	// Table changes targeting system keyspaces should be skipped by refreshTablesSchema
	// because system keyspaces are already refreshed by refreshSystemKeyspacesLocked.
	s := createTestSession(SchemaChangesRefreshTable)

	populateKeyspaceCache(s, "system_auth", "system", "user_ks")

	tableChanges := []tableChange{
		{keyspace: "system_auth", table: "roles", change: "UPDATED"},
	}

	err := s.metadataDescriber.refreshTablesSchema(tableChanges, map[string]struct{}{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// user_ks should remain untouched.
	cached := getCachedKeyspaces(s)
	if !cached["user_ks"] {
		t.Error("user_ks should remain in cache but was removed")
	}
}

func TestRemoveTableFromCache_NonExistentTable(t *testing.T) {
	t.Parallel()

	// Removing a non-existent table from an existing keyspace should be a no-op.
	s := createTestSession(SchemaChangesRefreshTable)

	existingTables := map[string]*TableMetadata{
		"table1": {Keyspace: "ks", Name: "table1"},
	}
	populateKeyspaceCacheWithTables(s, "ks", existingTables, nil, nil)

	s.metadataDescriber.mu.Lock()
	err := s.metadataDescriber.removeTableFromCache("ks", "nonexistent_table")
	s.metadataDescriber.mu.Unlock()

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	ks, ok := s.metadataDescriber.metadata.keyspaceMetadata.getKeyspace("ks")
	if !ok {
		t.Fatal("keyspace should still exist")
	}
	if _, ok := ks.Tables["table1"]; !ok {
		t.Error("table1 should remain in cache after removing nonexistent table")
	}
	if len(ks.Tables) != 1 {
		t.Errorf("expected 1 table, got %d", len(ks.Tables))
	}
}

func TestCompileTableMetadata_CompositePartitionKey(t *testing.T) {
	t.Parallel()

	tables := []TableMetadata{
		{Keyspace: "ks", Name: "events"},
	}
	columns := []ColumnMetadata{
		{Keyspace: "ks", Table: "events", Name: "tenant_id", Kind: ColumnPartitionKey, ComponentIndex: 0},
		{Keyspace: "ks", Table: "events", Name: "user_id", Kind: ColumnPartitionKey, ComponentIndex: 1},
		{Keyspace: "ks", Table: "events", Name: "data", Kind: ColumnRegular},
	}

	table, _, _ := compileTableMetadata(tables, columns, nil, nil)

	if table == nil {
		t.Fatal("expected compiled table, got nil")
	}
	if len(table.PartitionKey) != 2 {
		t.Fatalf("expected 2 partition key columns, got %d", len(table.PartitionKey))
	}
	if table.PartitionKey[0].Name != "tenant_id" {
		t.Errorf("expected partition key[0] 'tenant_id', got %q", table.PartitionKey[0].Name)
	}
	if table.PartitionKey[1].Name != "user_id" {
		t.Errorf("expected partition key[1] 'user_id', got %q", table.PartitionKey[1].Name)
	}
}

func TestCompileTableMetadata_CompositeClusteringKey(t *testing.T) {
	t.Parallel()

	tables := []TableMetadata{
		{Keyspace: "ks", Name: "events"},
	}
	columns := []ColumnMetadata{
		{Keyspace: "ks", Table: "events", Name: "user_id", Kind: ColumnPartitionKey, ComponentIndex: 0},
		{Keyspace: "ks", Table: "events", Name: "event_time", Kind: ColumnClusteringKey, ComponentIndex: 0, ClusteringOrder: "desc"},
		{Keyspace: "ks", Table: "events", Name: "event_id", Kind: ColumnClusteringKey, ComponentIndex: 1, ClusteringOrder: "asc"},
		{Keyspace: "ks", Table: "events", Name: "data", Kind: ColumnRegular},
	}

	table, _, _ := compileTableMetadata(tables, columns, nil, nil)

	if table == nil {
		t.Fatal("expected compiled table, got nil")
	}
	if len(table.ClusteringColumns) != 2 {
		t.Fatalf("expected 2 clustering columns, got %d", len(table.ClusteringColumns))
	}
	if table.ClusteringColumns[0].Name != "event_time" {
		t.Errorf("expected clustering column[0] 'event_time', got %q", table.ClusteringColumns[0].Name)
	}
	if table.ClusteringColumns[0].Order != DESC {
		t.Errorf("expected DESC order for clustering column[0], got %v", table.ClusteringColumns[0].Order)
	}
	if table.ClusteringColumns[1].Name != "event_id" {
		t.Errorf("expected clustering column[1] 'event_id', got %q", table.ClusteringColumns[1].Name)
	}
	if table.ClusteringColumns[1].Order != ASC {
		t.Errorf("expected ASC order for clustering column[1], got %v", table.ClusteringColumns[1].Order)
	}
}

func TestCompileTableMetadata_WithIndexAndView(t *testing.T) {
	t.Parallel()

	// Test that both an index and a view can coexist on the same base table.
	tables := []TableMetadata{
		{Keyspace: "ks", Name: "users"},
	}
	columns := []ColumnMetadata{
		// Base table columns
		{Keyspace: "ks", Table: "users", Name: "id", Kind: ColumnPartitionKey, ComponentIndex: 0},
		{Keyspace: "ks", Table: "users", Name: "email", Kind: ColumnRegular},
		{Keyspace: "ks", Table: "users", Name: "name", Kind: ColumnRegular},
		// Index backing view columns
		{Keyspace: "ks", Table: "email_idx_index", Name: "email", Kind: ColumnPartitionKey, ComponentIndex: 0},
		{Keyspace: "ks", Table: "email_idx_index", Name: "id", Kind: ColumnClusteringKey, ComponentIndex: 0},
		// Materialized view columns
		{Keyspace: "ks", Table: "users_by_name", Name: "name", Kind: ColumnPartitionKey, ComponentIndex: 0},
		{Keyspace: "ks", Table: "users_by_name", Name: "id", Kind: ColumnClusteringKey, ComponentIndex: 0},
	}
	indexes := []IndexMetadata{
		{Name: "email_idx", KeyspaceName: "ks", TableName: "users", Kind: "COMPOSITES"},
	}
	views := []ViewMetadata{
		{KeyspaceName: "ks", ViewName: "users_by_name", BaseTableName: "users"},
	}

	table, compiledIndexes, compiledViews := compileTableMetadata(tables, columns, indexes, views)

	if table == nil {
		t.Fatal("expected compiled table, got nil")
	}
	if len(table.Columns) != 3 {
		t.Errorf("expected 3 table columns, got %d", len(table.Columns))
	}
	if len(compiledIndexes) != 1 {
		t.Fatalf("expected 1 index, got %d", len(compiledIndexes))
	}
	idx := compiledIndexes["email_idx"]
	if idx == nil {
		t.Fatal("expected email_idx index")
	}
	if len(idx.Columns) != 2 {
		t.Errorf("expected 2 index columns, got %d", len(idx.Columns))
	}
	if len(compiledViews) != 1 {
		t.Fatalf("expected 1 view, got %d", len(compiledViews))
	}
	view := compiledViews["users_by_name"]
	if view == nil {
		t.Fatal("expected users_by_name view")
	}
	if len(view.Columns) != 2 {
		t.Errorf("expected 2 view columns, got %d", len(view.Columns))
	}
}

func TestMergeTableIntoCache_PreservesFunctionsAggregatesTypes(t *testing.T) {
	t.Parallel()

	s := createTestSession(SchemaChangesRefreshTable)

	// Set up a keyspace with functions, aggregates, and types.
	ks := &KeyspaceMetadata{
		Name:       "ks",
		Tables:     map[string]*TableMetadata{"t1": {Keyspace: "ks", Name: "t1"}},
		Indexes:    map[string]*IndexMetadata{},
		Views:      map[string]*ViewMetadata{},
		Functions:  map[string]*FunctionMetadata{"my_func": {Name: "my_func"}},
		Aggregates: map[string]*AggregateMetadata{"my_agg": {Name: "my_agg"}},
		Types:      map[string]*TypeMetadata{"my_type": {Name: "my_type"}},
	}
	s.metadataDescriber.metadata.keyspaceMetadata.set("ks", ks)

	newTable := &TableMetadata{Keyspace: "ks", Name: "t2", Columns: map[string]*ColumnMetadata{}}

	s.metadataDescriber.mu.Lock()
	err := s.metadataDescriber.mergeTableIntoCache("ks", "t2", newTable, nil, nil)
	s.metadataDescriber.mu.Unlock()

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	updated, _ := s.metadataDescriber.metadata.keyspaceMetadata.getKeyspace("ks")

	if _, ok := updated.Functions["my_func"]; !ok {
		t.Error("Functions should be preserved after table merge")
	}
	if _, ok := updated.Aggregates["my_agg"]; !ok {
		t.Error("Aggregates should be preserved after table merge")
	}
	if _, ok := updated.Types["my_type"]; !ok {
		t.Error("Types should be preserved after table merge")
	}
	if _, ok := updated.Tables["t1"]; !ok {
		t.Error("existing table t1 should be preserved")
	}
	if _, ok := updated.Tables["t2"]; !ok {
		t.Error("new table t2 should be present")
	}
}

func TestMergeTableIntoCache_NilTable(t *testing.T) {
	t.Parallel()

	// Passing nil for the table parameter should not insert a nil entry.
	s := createTestSession(SchemaChangesRefreshTable)

	existingTables := map[string]*TableMetadata{
		"t1": {Keyspace: "ks", Name: "t1"},
	}
	populateKeyspaceCacheWithTables(s, "ks", existingTables, nil, nil)

	s.metadataDescriber.mu.Lock()
	err := s.metadataDescriber.mergeTableIntoCache("ks", "nonexistent", nil, nil, nil)
	s.metadataDescriber.mu.Unlock()

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	updated, _ := s.metadataDescriber.metadata.keyspaceMetadata.getKeyspace("ks")
	if _, ok := updated.Tables["nonexistent"]; ok {
		t.Error("nil table should not be inserted into the tables map")
	}
	if len(updated.Tables) != 1 {
		t.Errorf("expected 1 table, got %d", len(updated.Tables))
	}
}

func TestMergeTableIntoCache_InvalidatesCreateStmts(t *testing.T) {
	t.Parallel()

	s := createTestSession(SchemaChangesRefreshTable)

	ks := &KeyspaceMetadata{
		Name:        "ks",
		Tables:      map[string]*TableMetadata{"t1": {Keyspace: "ks", Name: "t1"}},
		Indexes:     map[string]*IndexMetadata{},
		Views:       map[string]*ViewMetadata{},
		Functions:   map[string]*FunctionMetadata{},
		Aggregates:  map[string]*AggregateMetadata{},
		Types:       map[string]*TypeMetadata{},
		CreateStmts: "CREATE TABLE t1 (...);",
	}
	s.metadataDescriber.metadata.keyspaceMetadata.set("ks", ks)

	newTable := &TableMetadata{Keyspace: "ks", Name: "t2"}

	s.metadataDescriber.mu.Lock()
	err := s.metadataDescriber.mergeTableIntoCache("ks", "t2", newTable, nil, nil)
	s.metadataDescriber.mu.Unlock()

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	updated, _ := s.metadataDescriber.metadata.keyspaceMetadata.getKeyspace("ks")
	if updated.CreateStmts != "" {
		t.Errorf("CreateStmts should be invalidated after mergeTableIntoCache, got %q", updated.CreateStmts)
	}
}

func TestRemoveTableFromCache_InvalidatesCreateStmts(t *testing.T) {
	t.Parallel()

	s := createTestSession(SchemaChangesRefreshTable)

	ks := &KeyspaceMetadata{
		Name:        "ks",
		Tables:      map[string]*TableMetadata{"t1": {Keyspace: "ks", Name: "t1"}},
		Indexes:     map[string]*IndexMetadata{},
		Views:       map[string]*ViewMetadata{},
		Functions:   map[string]*FunctionMetadata{},
		Aggregates:  map[string]*AggregateMetadata{},
		Types:       map[string]*TypeMetadata{},
		CreateStmts: "CREATE TABLE t1 (...);",
	}
	s.metadataDescriber.metadata.keyspaceMetadata.set("ks", ks)

	s.metadataDescriber.mu.Lock()
	err := s.metadataDescriber.removeTableFromCache("ks", "t1")
	s.metadataDescriber.mu.Unlock()

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	updated, _ := s.metadataDescriber.metadata.keyspaceMetadata.getKeyspace("ks")
	if updated.CreateStmts != "" {
		t.Errorf("CreateStmts should be invalidated after removeTableFromCache, got %q", updated.CreateStmts)
	}
}

func TestHandleSchemaEvent_KeyspaceChangedCallback(t *testing.T) {
	t.Parallel()

	for _, mode := range []SchemaChangesRefreshMode{
		SchemaChangesRefreshAll,
		SchemaChangesRefreshKeyspace,
		SchemaChangesRefreshTable,
	} {
		t.Run(mode.String(), func(t *testing.T) {
			s := createTestSession(mode)
			populateKeyspaceCache(s, "ks1", "system")

			// A keyspace event should trigger KeyspaceChanged on the policy.
			frames := []frame{
				&frm.SchemaChangeKeyspace{
					Keyspace: "ks1",
					Change:   "UPDATED",
				},
			}

			s.handleSchemaEvent(frames)

			policy := s.policy.(*schemaTestHostPolicy)
			changes := policy.getKeyspaceChanges()
			if len(changes) != 1 {
				t.Fatalf("expected 1 KeyspaceChanged callback, got %d", len(changes))
			}
			if changes[0] != "ks1" {
				t.Errorf("expected KeyspaceChanged for 'ks1', got %q", changes[0])
			}
		})
	}
}

func TestHandleSchemaEvent_TableEventNoKeyspaceChangedCallback(t *testing.T) {
	t.Parallel()

	// Table-level events should NOT trigger KeyspaceChanged callback.
	s := createTestSession(SchemaChangesRefreshKeyspace)
	populateKeyspaceCache(s, "ks1", "system")

	frames := []frame{
		&frm.SchemaChangeTable{
			Keyspace: "ks1",
			Object:   "my_table",
			Change:   "CREATED",
		},
	}

	s.handleSchemaEvent(frames)

	policy := s.policy.(*schemaTestHostPolicy)
	changes := policy.getKeyspaceChanges()
	if len(changes) != 0 {
		t.Errorf("expected 0 KeyspaceChanged callbacks for table event, got %d", len(changes))
	}
}

func TestHandleSchemaEvent_RefreshTable_NoClearSchemaForTableEvents(t *testing.T) {
	t.Parallel()

	// In RefreshTable mode, SchemaChangeTable events should NOT call clearSchema,
	// so the keyspace cache entry should remain even when the mock refresh cannot re-populate.
	s := createTestSession(SchemaChangesRefreshTable)

	existingTables := map[string]*TableMetadata{
		"table1": {Keyspace: "ks", Name: "table1"},
		"table2": {Keyspace: "ks", Name: "table2"},
	}
	populateKeyspaceCacheWithTables(s, "ks", existingTables, nil, nil)
	populateKeyspaceCache(s, "system")

	frames := []frame{
		&frm.SchemaChangeTable{
			Keyspace: "ks",
			Object:   "table1",
			Change:   "UPDATED",
		},
	}

	s.handleSchemaEvent(frames)

	// The keyspace should still exist in cache (not cleared).
	ks, ok := s.metadataDescriber.metadata.keyspaceMetadata.getKeyspace("ks")
	if !ok {
		t.Fatal("keyspace 'ks' should still be in cache — clearSchema should NOT be called in RefreshTable mode for table events")
	}

	// table2 should still be in the keyspace.
	if _, ok := ks.Tables["table2"]; !ok {
		t.Error("table2 should remain in cache")
	}
}

func TestHandleSchemaEvent_RefreshKeyspace_NoClearSchemaForTableEvents(t *testing.T) {
	t.Parallel()

	// In RefreshKeyspace mode, SchemaChangeTable events should NOT call clearSchema
	// in handleSchemaEvent. Instead, the keyspace is refreshed eagerly via
	// refreshKeyspacesSchema → refreshSchema, which overwrites the cache entry.
	// Since the mock DB returns no rows, refreshSchema returns ErrKeyspaceDoesNotExist,
	// and refreshKeyspacesSchema clears the keyspace as a fallback — but this happens
	// inside the refresh method, not in handleSchemaEvent itself.
	s := createTestSession(SchemaChangesRefreshKeyspace)

	existingTables := map[string]*TableMetadata{
		"table1": {Keyspace: "ks", Name: "table1"},
		"table2": {Keyspace: "ks", Name: "table2"},
	}
	populateKeyspaceCacheWithTables(s, "ks", existingTables, nil, nil)
	populateKeyspaceCache(s, "system")

	// Verify tables exist before.
	ksBefore, _ := s.metadataDescriber.metadata.keyspaceMetadata.getKeyspace("ks")
	if len(ksBefore.Tables) != 2 {
		t.Fatalf("expected 2 tables before event, got %d", len(ksBefore.Tables))
	}

	frames := []frame{
		&frm.SchemaChangeTable{
			Keyspace: "ks",
			Object:   "table1",
			Change:   "UPDATED",
		},
	}

	s.handleSchemaEvent(frames)

	// The keyspace ends up cleared because the mock DB has no real data,
	// so refreshSchema returns ErrKeyspaceDoesNotExist and refreshKeyspacesSchema
	// clears it. In production (with a real DB), the keyspace would be re-populated.
	cached := getCachedKeyspaces(s)
	if cached["ks"] {
		t.Error("ks should have been cleared by refreshKeyspacesSchema fallback (mock refresh returns ErrKeyspaceDoesNotExist)")
	}
}

func TestRefreshTablesSchema_MultipleTablesAcrossKeyspaces(t *testing.T) {
	t.Parallel()

	s := createTestSession(SchemaChangesRefreshTable)

	// Set up ks1 and ks2 each with tables.
	existingTables1 := map[string]*TableMetadata{
		"t1": {Keyspace: "ks1", Name: "t1"},
		"t2": {Keyspace: "ks1", Name: "t2"},
	}
	existingTables2 := map[string]*TableMetadata{
		"t3": {Keyspace: "ks2", Name: "t3"},
		"t4": {Keyspace: "ks2", Name: "t4"},
	}
	populateKeyspaceCacheWithTables(s, "ks1", existingTables1, nil, nil)
	populateKeyspaceCacheWithTables(s, "ks2", existingTables2, nil, nil)
	populateKeyspaceCache(s, "system")

	tableChanges := []tableChange{
		{keyspace: "ks1", table: "t1", change: "DROPPED"},
		{keyspace: "ks2", table: "t3", change: "DROPPED"},
	}

	err := s.metadataDescriber.refreshTablesSchema(tableChanges, map[string]struct{}{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// ks1: t1 dropped, t2 remains.
	ks1, ok := s.metadataDescriber.metadata.keyspaceMetadata.getKeyspace("ks1")
	if !ok {
		t.Fatal("ks1 should still be in cache")
	}
	if _, ok := ks1.Tables["t1"]; ok {
		t.Error("ks1.t1 should have been removed")
	}
	if _, ok := ks1.Tables["t2"]; !ok {
		t.Error("ks1.t2 should remain")
	}

	// ks2: t3 dropped, t4 remains.
	ks2, ok := s.metadataDescriber.metadata.keyspaceMetadata.getKeyspace("ks2")
	if !ok {
		t.Fatal("ks2 should still be in cache")
	}
	if _, ok := ks2.Tables["t3"]; ok {
		t.Error("ks2.t3 should have been removed")
	}
	if _, ok := ks2.Tables["t4"]; !ok {
		t.Error("ks2.t4 should remain")
	}
}

func TestMergeTableIntoCache_CopyOnWriteSafety(t *testing.T) {
	t.Parallel()

	// After mergeTableIntoCache, modifying the original keyspace's tables map
	// should not affect the merged result.
	s := createTestSession(SchemaChangesRefreshTable)

	existingTables := map[string]*TableMetadata{
		"t1": {Keyspace: "ks", Name: "t1"},
	}
	populateKeyspaceCacheWithTables(s, "ks", existingTables, nil, nil)

	// Save reference to original.
	originalKs, _ := s.metadataDescriber.metadata.keyspaceMetadata.getKeyspace("ks")

	newTable := &TableMetadata{Keyspace: "ks", Name: "t2"}
	s.metadataDescriber.mu.Lock()
	err := s.metadataDescriber.mergeTableIntoCache("ks", "t2", newTable, nil, nil)
	s.metadataDescriber.mu.Unlock()

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Modify the original's tables map.
	originalKs.Tables["injected"] = &TableMetadata{Keyspace: "ks", Name: "injected"}

	// The merged cache entry should NOT have the injected table.
	updated, _ := s.metadataDescriber.metadata.keyspaceMetadata.getKeyspace("ks")
	if _, ok := updated.Tables["injected"]; ok {
		t.Error("injected table should not appear in merged cache entry — copy-on-write is broken")
	}
}

func TestHandleSchemaEvent_RefreshKeyspace_KeyspaceCreated(t *testing.T) {
	t.Parallel()

	// KEYSPACE CREATED event in RefreshKeyspace mode.
	s := createTestSession(SchemaChangesRefreshKeyspace)
	populateKeyspaceCache(s, "other_ks", "system")

	frames := []frame{
		&frm.SchemaChangeKeyspace{
			Keyspace: "new_ks",
			Change:   "CREATED",
		},
	}

	s.handleSchemaEvent(frames)

	cached := getCachedKeyspaces(s)

	// other_ks should remain untouched.
	if !cached["other_ks"] {
		t.Error("other_ks should remain in cache but was removed")
	}

	// KeyspaceChanged should have been called.
	policy := s.policy.(*schemaTestHostPolicy)
	changes := policy.getKeyspaceChanges()
	if len(changes) != 1 || changes[0] != "new_ks" {
		t.Errorf("expected KeyspaceChanged for 'new_ks', got %v", changes)
	}
}

func TestCompileTableMetadata_StaticColumn(t *testing.T) {
	t.Parallel()

	tables := []TableMetadata{
		{Keyspace: "ks", Name: "users"},
	}
	columns := []ColumnMetadata{
		{Keyspace: "ks", Table: "users", Name: "id", Kind: ColumnPartitionKey, ComponentIndex: 0},
		{Keyspace: "ks", Table: "users", Name: "cluster", Kind: ColumnClusteringKey, ComponentIndex: 0},
		{Keyspace: "ks", Table: "users", Name: "static_col", Kind: ColumnStatic},
		{Keyspace: "ks", Table: "users", Name: "data", Kind: ColumnRegular},
	}

	table, _, _ := compileTableMetadata(tables, columns, nil, nil)

	if table == nil {
		t.Fatal("expected compiled table, got nil")
	}
	if len(table.Columns) != 4 {
		t.Errorf("expected 4 columns, got %d", len(table.Columns))
	}
	if len(table.PartitionKey) != 1 {
		t.Errorf("expected 1 partition key, got %d", len(table.PartitionKey))
	}
	if len(table.ClusteringColumns) != 1 {
		t.Errorf("expected 1 clustering column, got %d", len(table.ClusteringColumns))
	}

	// OrderedColumns: partition keys first, then clustering, then others (including static).
	expected := []string{"id", "cluster", "static_col", "data"}
	if len(table.OrderedColumns) != len(expected) {
		t.Fatalf("expected %d ordered columns, got %d: %v", len(expected), len(table.OrderedColumns), table.OrderedColumns)
	}
	for i, name := range expected {
		if table.OrderedColumns[i] != name {
			t.Errorf("ordered column %d: expected %q, got %q", i, name, table.OrderedColumns[i])
		}
	}
}

func TestRefreshTablesSchema_DeduplicatesSameTable(t *testing.T) {
	t.Parallel()

	s := createTestSession(SchemaChangesRefreshTable)

	existingTables := map[string]*TableMetadata{
		"t1": {Keyspace: "ks", Name: "t1"},
		"t2": {Keyspace: "ks", Name: "t2"},
	}
	populateKeyspaceCacheWithTables(s, "ks", existingTables, nil, nil)
	populateKeyspaceCache(s, "system")

	// Two events for the same table: first CREATED, then DROPPED.
	// Only the last (DROPPED) should take effect.
	tableChanges := []tableChange{
		{keyspace: "ks", table: "t1", change: "CREATED"},
		{keyspace: "ks", table: "t1", change: "DROPPED"},
	}

	err := s.metadataDescriber.refreshTablesSchema(tableChanges, map[string]struct{}{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	ks, ok := s.metadataDescriber.metadata.keyspaceMetadata.getKeyspace("ks")
	if !ok {
		t.Fatal("ks should still be in cache")
	}
	if _, ok := ks.Tables["t1"]; ok {
		t.Error("t1 should have been dropped (last event was DROPPED)")
	}
	if _, ok := ks.Tables["t2"]; !ok {
		t.Error("t2 should remain untouched")
	}
}

func TestRefreshTablesSchema_DeduplicateKeepsLastOnly(t *testing.T) {
	t.Parallel()

	s := createTestSession(SchemaChangesRefreshTable)

	existingTables := map[string]*TableMetadata{
		"t1": {Keyspace: "ks", Name: "t1"},
	}
	populateKeyspaceCacheWithTables(s, "ks", existingTables, nil, nil)
	populateKeyspaceCache(s, "system")

	// Three events for the same table: DROPPED, CREATED, DROPPED.
	// Only the last (DROPPED) should take effect.
	tableChanges := []tableChange{
		{keyspace: "ks", table: "t1", change: "DROPPED"},
		{keyspace: "ks", table: "t1", change: "CREATED"},
		{keyspace: "ks", table: "t1", change: "DROPPED"},
	}

	err := s.metadataDescriber.refreshTablesSchema(tableChanges, map[string]struct{}{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	ks, ok := s.metadataDescriber.metadata.keyspaceMetadata.getKeyspace("ks")
	if !ok {
		t.Fatal("ks should still be in cache")
	}
	if _, ok := ks.Tables["t1"]; ok {
		t.Error("t1 should have been dropped (last event was DROPPED)")
	}
}
