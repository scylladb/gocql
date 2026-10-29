//go:build integration
// +build integration

package gocql

import (
	"context"
	"testing"
)

func TestShardAwarePortIntegrationNoReconnections(t *testing.T) {
	t.Parallel()

	testShardAwarePortNoReconnections(t, makeIntegrationShardAwareCluster)
}

func TestShardAwarePortIntegrationMaliciousNAT(t *testing.T) {
	t.Parallel()

	testShardAwarePortMaliciousNAT(t, makeIntegrationShardAwareCluster)
}

func TestShardAwarePortIntegrationUnreachable(t *testing.T) {
	t.Parallel()

	testShardAwarePortUnreachable(t, makeIntegrationShardAwareCluster)
}

func TestShardAwarePortIntegrationUnusedIfNotEnabled(t *testing.T) {
	t.Parallel()

	testShardAwarePortUnusedIfNotEnabled(t, makeIntegrationShardAwareCluster)
}

func makeIntegrationShardAwareCluster() shardAwareTestCluster {
	config := createV2Cluster()
	return shardAwareTestCluster{
		useTLS: config.TLS != nil || config.TLSForHost != nil,
		port:   shardTestContactPort(config.ContactPoints, config.Port),
		connect: func(ctx context.Context, dialer Dialer, disableShardAwarePort bool) (*Session, error) {
			config.Dialer = dialer
			config.Pool.ShardAwarePort = !disableShardAwarePort
			return Connect(ctx, config)
		},
	}
}
