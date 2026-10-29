//go:build integration
// +build integration

package gocql

import (
	"context"
	"net"
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
		connect: func(ctx context.Context, dialer legacyDialer, disableShardAwarePort bool) (*Session, error) {
			config.Dialer = DialerFunc(func(ctx context.Context, target DialTarget) (net.Conn, error) {
				if target.LocalAddress.IsValid() {
					ctx = context.WithValue(ctx, scyllaSourcePortCtx{}, target.LocalAddress.Port())
				}
				return dialer.DialContext(ctx, target.Network, target.Address)
			})
			config.Pool.ShardAwarePort = !disableShardAwarePort
			return Connect(ctx, config)
		},
	}
}
