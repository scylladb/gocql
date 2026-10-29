//go:build integration
// +build integration

package gocql

import (
	"testing"

	"github.com/scylladb/gocql/v2/internal/tests"
)

func TestSchemaQueries(t *testing.T) {
	t.Parallel()

	cluster := createV2Cluster()

	v2ConfigureRouting(t, &cluster, RoutingOptions{TokenAware: true})

	session := createV2SessionFromConfig(cluster, t, false)
	defer session.Close()

	keyspaceMetadata, ok := session.Metadata().Keyspace("gocql_test")
	if !ok {
		t.Fatal("keyspace missing from typed metadata snapshot")
	}
	tests.AssertTrue(t, "keyspace present in metadata snapshot", keyspaceMetadata.Name == "gocql_test")
}
