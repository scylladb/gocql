//go:build unit
// +build unit

package gocql

import "testing"

// refreshAllSchemaSession returns a session whose schema mock knows ks1 with
// tables t1 and t2, and ks2 with t3; all three are loaded into the cache and
// have tablets.
func refreshAllSchemaSession(t *testing.T) (*Session, *schemaDataMock) {
	t.Helper()

	col := func(name string) []columnInfo { return []columnInfo{{name: name, kind: "partition_key"}} }
	ctrl := &schemaDataMock{knownKeyspaces: map[string][]tableInfo{
		"ks1": {{name: "t1", columns: col("id")}, {name: "t2", columns: col("id")}},
		"ks2": {{name: "t3", columns: col("id")}},
	}}
	s := newSchemaEventTestSessionWithMock(ctrl)
	t.Cleanup(s.Close)

	for _, ks := range []string{"ks1", "ks2"} {
		if _, err := s.metadataDescriber.GetKeyspace(ks); err != nil {
			t.Fatalf("load %s: %v", ks, err)
		}
	}
	addTestTablets(t, s, "ks1", "t1")
	addTestTablets(t, s, "ks1", "t2")
	addTestTablets(t, s, "ks2", "t3")
	return s, ctrl
}

func refreshAllSchemaNow(t *testing.T, s *Session) {
	t.Helper()
	if err := s.metadataDescriber.refreshAllSchema(); err != nil {
		t.Fatalf("refreshAllSchema: %v", err)
	}
	s.metadataDescriber.metadata.tabletsMetadata.Flush()
}

func hasTablets(s *Session, keyspace, table string) bool {
	return len(s.metadataDescriber.metadata.tabletsMetadata.GetTableTablets(keyspace, table)) > 0
}

// requireTablets checks which of the three tables still have tablets.
func requireTablets(t *testing.T, s *Session, want map[string]bool) {
	t.Helper()
	for _, kt := range [][2]string{{"ks1", "t1"}, {"ks1", "t2"}, {"ks2", "t3"}} {
		key := kt[0] + "." + kt[1]
		if got := hasTablets(s, kt[0], kt[1]); got != want[key] {
			t.Errorf("%s has tablets = %v, want %v", key, got, want[key])
		}
	}
}

// refreshAllSchema runs after the control connection reconnects, when
// schema events may have been missed. Tablets are dropped only for what
// actually changed.
func TestRefreshAllSchemaUnchanged(t *testing.T) {
	t.Parallel()

	s, _ := refreshAllSchemaSession(t)
	refreshAllSchemaNow(t, s)
	requireTablets(t, s, map[string]bool{"ks1.t1": true, "ks1.t2": true, "ks2.t3": true})
}

func TestRefreshAllSchemaKeyspaceDropped(t *testing.T) {
	t.Parallel()

	s, ctrl := refreshAllSchemaSession(t)
	delete(ctrl.knownKeyspaces, "ks2")

	refreshAllSchemaNow(t, s)

	if _, found := s.metadataDescriber.metadata.keyspaceMetadata.getKeyspace("ks2"); found {
		t.Error("dropped keyspace ks2 is still cached")
	}
	if _, found := s.metadataDescriber.metadata.keyspaceMetadata.getKeyspace("ks1"); !found {
		t.Error("keyspace ks1 was dropped from the cache")
	}
	requireTablets(t, s, map[string]bool{"ks1.t1": true, "ks1.t2": true, "ks2.t3": false})
}

func TestRefreshAllSchemaTableDropped(t *testing.T) {
	t.Parallel()

	s, ctrl := refreshAllSchemaSession(t)
	ctrl.knownKeyspaces["ks1"] = ctrl.knownKeyspaces["ks1"][1:] // t1 is gone

	refreshAllSchemaNow(t, s)
	requireTablets(t, s, map[string]bool{"ks1.t1": false, "ks1.t2": true, "ks2.t3": true})
}

func TestRefreshAllSchemaTableChanged(t *testing.T) {
	t.Parallel()

	s, ctrl := refreshAllSchemaSession(t)
	ctrl.knownKeyspaces["ks1"][1].columns = append(ctrl.knownKeyspaces["ks1"][1].columns,
		columnInfo{name: "added", kind: "regular"})

	refreshAllSchemaNow(t, s)
	requireTablets(t, s, map[string]bool{"ks1.t1": true, "ks1.t2": false, "ks2.t3": true})
}

// A replication change moves every tablet of the keyspace, so all of its
// tablets are dropped, not just those of changed tables.
func TestRefreshAllSchemaReplicationChanged(t *testing.T) {
	t.Parallel()

	s, _ := refreshAllSchemaSession(t)
	cached, _ := s.metadataDescriber.metadata.keyspaceMetadata.getKeyspace("ks1")
	stale := cached.Clone()
	stale.StrategyOptions = map[string]any{"replication_factor": "3"}
	s.metadataDescriber.metadata.keyspaceMetadata.set("ks1", stale)

	refreshAllSchemaNow(t, s)
	requireTablets(t, s, map[string]bool{"ks1.t1": false, "ks1.t2": false, "ks2.t3": true})
}
