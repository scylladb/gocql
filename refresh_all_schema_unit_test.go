//go:build unit
// +build unit

package gocql

import (
	"errors"
	"strings"
	"testing"

	frm "github.com/gocql/gocql/internal/frame"
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

// A failed refresh reports the error and drops nothing. The keyspace query's
// failure must not read as "keyspace does not exist": refreshAllSchema would
// then evict the keyspace and all of its tablets.
func TestRefreshAllSchemaError(t *testing.T) {
	t.Parallel()

	s, ctrl := refreshAllSchemaSession(t)
	ctrl.setQueryError(errors.New("schema unavailable"))

	err := s.metadataDescriber.refreshAllSchema()
	if err == nil || !strings.Contains(err.Error(), "schema unavailable") {
		t.Fatalf("refreshAllSchema error = %v, want the query error", err)
	}
	s.metadataDescriber.metadata.tabletsMetadata.Flush()
	for _, ks := range []string{"ks1", "ks2"} {
		if _, found := s.metadataDescriber.metadata.keyspaceMetadata.getKeyspace(ks); !found {
			t.Errorf("keyspace %s was dropped after a failed refresh", ks)
		}
	}
	requireTablets(t, s, map[string]bool{"ks1.t1": true, "ks1.t2": true, "ks2.t3": true})
}

// Loading a keyspace through a failing query reports the failure rather than
// ErrKeyspaceDoesNotExist.
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

// Whichever schema query fails first, its cause stays reachable with
// errors.Is: the queries run concurrently, so every one of them has to wrap.
func TestRefreshAllSchemaErrorUnwraps(t *testing.T) {
	t.Parallel()

	for i := 0; i < 20; i++ {
		s, ctrl := refreshAllSchemaSession(t)
		errSchema := errors.New("schema unavailable")
		ctrl.setQueryError(errSchema)

		if err := s.metadataDescriber.refreshAllSchema(); !errors.Is(err, errSchema) {
			t.Fatalf("run %d: refreshAllSchema error = %v, which does not wrap the query error", i, err)
		}
	}
}

// Each schema query wraps its error with %w, so the cause stays reachable
// whichever query a refresh sees fail first.
func TestSchemaQueryErrorsWrap(t *testing.T) {
	t.Parallel()

	errQuery := errors.New("query failed")
	failing := &Iter{err: errQuery}

	tests := []struct {
		name    string
		stmt    string // the failing statement contains this
		iter    *Iter
		call    func(s *Session) error
		wantMsg string
	}{
		{"KeyspaceNoRows", "system_schema.keyspaces", failing,
			func(s *Session) error { _, err := getKeyspaceMetadata(s, "ks1"); return err },
			"error querying keyspace schema"},
		// The row arrives but reading it fails.
		{"KeyspaceRowUnreadable", "system_schema.keyspaces", &Iter{err: errQuery, numRows: 1},
			func(s *Session) error { _, err := getKeyspaceMetadata(s, "ks1"); return err },
			"error querying keyspace schema"},
		{"Tables", "system_schema.tables", failing,
			func(s *Session) error { _, err := getTableMetadata(s, "ks1"); return err },
			"error querying table schema"},
		{"Columns", "system_schema.columns", failing,
			func(s *Session) error { _, err := getColumnMetadata(s, "ks1"); return err },
			"error querying column schema"},
		{"Views", "system_schema.views", failing,
			func(s *Session) error { _, err := getViewMetadata(s, "ks1"); return err },
			"error querying view schema"},
		{"CreateStatements", "DESCRIBE KEYSPACE", failing,
			func(s *Session) error { _, err := getCreateStatements(s, "ks1"); return err },
			"error querying keyspace schema"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctrl := &schemaDataMock{knownKeyspaces: map[string][]tableInfo{"ks1": nil}}
			ctrl.iterFor = func(stmt string) *Iter {
				if strings.Contains(stmt, tc.stmt) {
					return tc.iter
				}
				return nil
			}
			s := newSchemaEventTestSessionWithMock(ctrl)
			defer s.Close()

			err := tc.call(s)
			if !errors.Is(err, errQuery) || !strings.Contains(err.Error(), tc.wantMsg) {
				t.Fatalf("error = %v, want %q wrapping %v", err, tc.wantMsg, errQuery)
			}
		})
	}
}

// Servers too old for DESCRIBE KEYSPACE answer it with a syntax error; the
// create statements are then rebuilt client side, so it is not an error.
func TestGetCreateStatementsDescribeUnsupported(t *testing.T) {
	t.Parallel()

	ctrl := &schemaDataMock{knownKeyspaces: map[string][]tableInfo{"ks1": nil}}
	ctrl.iterFor = func(stmt string) *Iter {
		if strings.Contains(stmt, "DESCRIBE KEYSPACE") {
			return &Iter{err: frm.ErrorFrame{Code: ErrCodeSyntax, Message: "line 1:0 no viable alternative"}}
		}
		return nil
	}
	s := newSchemaEventTestSessionWithMock(ctrl)
	defer s.Close()

	stmts, err := getCreateStatements(s, "ks1")
	if err != nil || stmts != nil {
		t.Fatalf("getCreateStatements = %q, %v; want nil, nil", stmts, err)
	}
}
