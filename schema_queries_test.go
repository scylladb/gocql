//go:build integration
// +build integration

package gocql

import (
	"github.com/scylladb/gocql/v2/internal/tests"
	"testing"
)

func TestSchemaQueries(t *testing.T) {
	cluster := createCluster()

	fallback := RoundRobinHostPolicy()
	cluster.PoolConfig.HostSelectionPolicy = TokenAwareHostPolicy(fallback)

	session := createSessionFromCluster(cluster, t)
	defer session.Close()

	keyspaceMetadata, err := session.metadataDescriber.getSchema("gocql_test")
	if err != nil {
		t.Fatal("unable to get keyspace metadata for keyspace: ", err)
	}
	tests.AssertTrue(t, "keyspace present in metadataDescriber", keyspaceMetadata.Name == "gocql_test")
}
