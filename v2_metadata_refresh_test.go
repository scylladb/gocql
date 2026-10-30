//go:build unit

package gocql

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
)

func TestV2MetadataDropDuringFetchIsConfirmed(t *testing.T) {
	s, mock := refreshAllSchemaSession(t)
	var dropped atomic.Bool
	mock.iterFor = func(statement string) *nativeResultFixture {
		if strings.HasPrefix(statement, "SELECT * FROM system_schema.tables WHERE") && dropped.CompareAndSwap(false, true) {
			mock.mu.Lock()
			delete(mock.knownKeyspaces, "ks2")
			mock.mu.Unlock()
			return &nativeResultFixture{err: errors.New("keyspace disappeared between enumeration and query")}
		}
		return nil
	}
	// The injected failure may target either fetched keyspace. The keyspace that
	// still exists must retain and report its failure; the confirmed absent one
	// must never be restored from the older enumerated rows.
	err := s.refreshV2AllSchema(context.Background())
	if err != nil {
		var partial *PartialRefreshError
		if !errors.As(err, &partial) {
			t.Fatal(err)
		}
		if _, wrong := partial.Keyspaces["ks2"]; wrong {
			t.Fatal("confirmed drop reported as stale", err)
		}
	}
	if _, exists := s.metadataDescriber.metadata.keyspaceMetadata.getKeyspace("ks2"); exists {
		t.Fatal("confirmed dropped keyspace was restored")
	}
}
func TestV2MetadataPartialRefreshRetainsPriorSnapshot(t *testing.T) {
	s, mock := refreshAllSchemaSession(t)
	before := s.Metadata()
	cause := errors.New("temporary schema read failure")
	// The actual column query selects an explicit column list.
	mock.iterFor = func(statement string) *nativeResultFixture {
		if strings.Contains(statement, "FROM system_schema.columns") {
			return &nativeResultFixture{err: cause}
		}
		return nil
	}
	err := s.refreshV2AllSchema(context.Background())
	var partial *PartialRefreshError
	if !errors.As(err, &partial) || !errors.Is(err, cause) {
		t.Fatal("partial error lost cause", err)
	}
	for _, snapshot := range []*MetadataSnapshot{before, s.Metadata()} {
		ks, ok := snapshot.Keyspace("ks1")
		if !ok || ks.Tables["t1"] == nil {
			t.Fatal("valid previous keyspace lost")
		}
	}
}
