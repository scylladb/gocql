//go:build unit
// +build unit

package gocql

import implCluster "github.com/scylladb/gocql/v2/internal/cluster"

import (
	"errors"
	"strings"
	"testing"
)

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
	t.Cleanup(func() { _ = s.Close() })

	for _, ks := range []string{"ks1", "ks2"} {
		if _, err := implCluster.SessionData(s).InternalMetadataDescriber.GetKeyspace(ks); err != nil {
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
	if err := implCluster.SessionData(s).InternalMetadataDescriber.InternalRefreshAllSchema(); err != nil {
		t.Fatalf("refreshAllSchema: %v", err)
	}
	implCluster.MetadataData(implCluster.SessionData(s).InternalMetadataDescriber.InternalMetadata).InternalTabletsMetadata.Flush()
}

func hasTablets(s *Session, keyspace, table string) bool {
	return len(implCluster.MetadataData(implCluster.SessionData(s).InternalMetadataDescriber.InternalMetadata).InternalTabletsMetadata.GetTableTablets(keyspace, table)) > 0
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

	if _, found := implCluster.MetadataData(implCluster.SessionData(s).InternalMetadataDescriber.InternalMetadata).InternalKeyspaceMetadata.InternalGetKeyspace("ks2"); found {
		t.Error("dropped keyspace ks2 is still cached")
	}
	if _, found := implCluster.MetadataData(implCluster.SessionData(s).InternalMetadataDescriber.InternalMetadata).InternalKeyspaceMetadata.InternalGetKeyspace("ks1"); !found {
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

func TestRefreshAllSchemaReplicationChanged(t *testing.T) {
	t.Parallel()

	s, _ := refreshAllSchemaSession(t)
	cached, _ := implCluster.MetadataData(implCluster.SessionData(s).InternalMetadataDescriber.InternalMetadata).InternalKeyspaceMetadata.InternalGetKeyspace("ks1")
	stale := cached.Clone()
	stale.StrategyOptions = map[string]any{"replication_factor": "3"}
	implCluster.MetadataData(implCluster.SessionData(s).InternalMetadataDescriber.InternalMetadata).InternalKeyspaceMetadata.InternalSet("ks1", stale)

	refreshAllSchemaNow(t, s)
	requireTablets(t, s, map[string]bool{"ks1.t1": false, "ks1.t2": false, "ks2.t3": true})
}

func TestRefreshAllSchemaError(t *testing.T) {
	t.Parallel()

	s, ctrl := refreshAllSchemaSession(t)
	ctrl.setQueryError(errors.New("schema unavailable"))

	err := implCluster.SessionData(s).InternalMetadataDescriber.InternalRefreshAllSchema()
	if err == nil || !strings.Contains(err.Error(), "schema unavailable") {
		t.Fatalf("refreshAllSchema error = %v, want the query error", err)
	}
	implCluster.MetadataData(implCluster.SessionData(s).InternalMetadataDescriber.InternalMetadata).InternalTabletsMetadata.Flush()
	for _, ks := range []string{"ks1", "ks2"} {
		if _, found := implCluster.MetadataData(implCluster.SessionData(s).InternalMetadataDescriber.InternalMetadata).InternalKeyspaceMetadata.InternalGetKeyspace(ks); !found {
			t.Errorf("keyspace %s was dropped after a failed refresh", ks)
		}
	}
	requireTablets(t, s, map[string]bool{"ks1.t1": true, "ks1.t2": true, "ks2.t3": true})
}

func TestKeyspaceMetadataQueryError(t *testing.T) {
	t.Parallel()

	ctrl := &schemaDataMock{knownKeyspaces: map[string][]tableInfo{"ks1": nil}}
	s := newSchemaEventTestSessionWithMock(ctrl)
	defer s.Close()
	ctrl.setQueryError(errors.New("schema unavailable"))

	_, err := getKeyspaceMetadata(s, "ks1")
	if err == nil || errors.Is(err, ErrKeyspaceDoesNotExist) || !strings.Contains(err.Error(), "schema unavailable") {
		t.Fatalf("getKeyspaceMetadata error = %v, want the query error", err)
	}
}

func TestRefreshAllSchemaErrorUnwraps(t *testing.T) {
	t.Parallel()

	for i := 0; i < 20; i++ {
		s, ctrl := refreshAllSchemaSession(t)
		errSchema := errors.New("schema unavailable")
		ctrl.setQueryError(errSchema)

		if err := implCluster.SessionData(s).InternalMetadataDescriber.InternalRefreshAllSchema(); !errors.Is(err, errSchema) {
			t.Fatalf("run %d: refreshAllSchema error = %v, which does not wrap the query error", i, err)
		}
	}
}
