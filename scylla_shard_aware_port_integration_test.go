//go:build integration
// +build integration

package gocql

import "testing"

func TestShardAwarePortIntegrationNoReconnections(t *testing.T) {
	t.Parallel()

	testShardAwarePortNoReconnections(t, func() *ClusterConfig {
		c := createCluster()
		c.Port = 9042
		return c
	})
}

func TestShardAwarePortIntegrationMaliciousNAT(t *testing.T) {
	t.Parallel()

	testShardAwarePortMaliciousNAT(t, func() *ClusterConfig {
		c := createCluster()
		c.Port = 9042
		return c
	})
}

func TestShardAwarePortIntegrationUnreachable(t *testing.T) {
	t.Parallel()

	testShardAwarePortUnreachable(t, func() *ClusterConfig {
		c := createCluster()
		c.Port = 9042
		return c
	})
}

func TestShardAwarePortIntegrationUnusedIfNotEnabled(t *testing.T) {
	t.Parallel()

	testShardAwarePortUnusedIfNotEnabled(t, func() *ClusterConfig {
		c := createCluster()
		c.Port = 9042
		return c
	})
}
