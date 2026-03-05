//go:build integration
// +build integration

package gocql

import (
	"testing"

	"github.com/gocql/gocql/internal/tests"
)

func TestSchemaQueries(t *testing.T) {
	cluster := createCluster()

	fallback := RoundRobinHostPolicy()
	cluster.PoolConfig.HostSelectionPolicy = TokenAwareHostPolicy(fallback)

	session := createSessionFromCluster(cluster, t)
	defer session.Close()

	keyspaceMetadata, err := session.metadataDescriber.GetKeyspace("gocql_test")
	if err != nil {
		t.Fatal("unable to get keyspace metadata for keyspace: ", err)
	}
	tests.AssertTrue(t, "keyspace present in metadataDescriber", keyspaceMetadata.Name == "gocql_test")
}
