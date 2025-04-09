//go:build integration
// +build integration

package gocql

import (
	"context"
	"testing"
	"time"
)

// Check if session fail to start if DC name provided in the policy is wrong
func TestDCValidationTokenAware(t *testing.T) {
	cluster := createCluster()

	fallback := DCAwareRoundRobinPolicy("WRONG_DC")
	cluster.PoolConfig.HostSelectionPolicy = TokenAwareHostPolicy(fallback)

	_, err := cluster.CreateSession()
	if err == nil {
		t.Fatal("createSession was expected to fail with wrong DC name provided.")
	}
}

func TestDCValidationDCAware(t *testing.T) {
	cluster := createCluster()
	cluster.PoolConfig.HostSelectionPolicy = DCAwareRoundRobinPolicy("WRONG_DC")

	_, err := cluster.CreateSession()
	if err == nil {
		t.Fatal("createSession was expected to fail with wrong DC name provided.")
	}
}

func TestDCValidationRackAware(t *testing.T) {
	cluster := createCluster()
	cluster.PoolConfig.HostSelectionPolicy = RackAwareRoundRobinPolicy("WRONG_DC", "RACK")

	_, err := cluster.CreateSession()
	if err == nil {
		t.Fatal("createSession was expected to fail with wrong DC name provided.")
	}
}

// This test ensures  that when all hosts are down, the query execution does not hang.
func TestNoHangAllHostsDown(t *testing.T) {
	cluster := createCluster()
	session := createSessionFromCluster(cluster, t)

	hosts := session.GetHosts()
	dc := hosts[0].DataCenter()
	rack := hosts[0].Rack()
	session.Close()

	policies := []HostSelectionPolicy{
		DCAwareRoundRobinPolicy(dc),
		DCAwareRoundRobinPolicy(dc, HostPolicyOptionDisableDCFailover),
		TokenAwareHostPolicy(DCAwareRoundRobinPolicy(dc)),
		TokenAwareHostPolicy(DCAwareRoundRobinPolicy(dc, HostPolicyOptionDisableDCFailover)),
		RackAwareRoundRobinPolicy(dc, rack),
		RackAwareRoundRobinPolicy(dc, rack, HostPolicyOptionDisableDCFailover),
		TokenAwareHostPolicy(RackAwareRoundRobinPolicy(dc, rack)),
		TokenAwareHostPolicy(RackAwareRoundRobinPolicy(dc, rack, HostPolicyOptionDisableDCFailover)),
		nil,
	}

	for _, policy := range policies {
		cluster = createCluster()
		cluster.PoolConfig.HostSelectionPolicy = policy
		session = createSessionFromCluster(cluster, t)
		hosts = session.GetHosts()

		// simulating hosts are down
		for _, host := range hosts {
			pool, _ := session.pool.getPoolByHostID(host.HostID())
			pool.host.setState(NodeDown)
			if policy != nil {
				policy.AddHost(host)
			}
		}

		ctx, _ := context.WithTimeout(context.Background(), 12*time.Second)
		_ = session.Query("SELECT host_id FROM system.local").WithContext(ctx).Exec()
		if ctx.Err() != nil {
			t.Errorf("policy %T should be no hangups when all hosts are down", policy)
		}

		// remove all host except one
		if policy != nil {
			for i, host := range hosts {
				if i != 0 {
					policy.RemoveHost(host)
				}
			}
		}

		ctx, _ = context.WithTimeout(context.Background(), 12*time.Second)
		_ = session.Query("SELECT host_id FROM system.local").WithContext(ctx).Exec()
		if ctx.Err() != nil {
			t.Errorf("policy %T should be no hangups when all hosts are down", policy)
		}
		session.Close()
	}
}
