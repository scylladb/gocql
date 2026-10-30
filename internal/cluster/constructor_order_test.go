// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cluster

import (
	"testing"

	"github.com/scylladb/gocql/v2/internal/endpoint"
	"github.com/scylladb/gocql/v2/internal/pool"
	"github.com/scylladb/gocql/v2/internal/settings"
	"github.com/scylladb/gocql/v2/internal/transport"
)

func TestTypedPoolSnapshotCapturedBeforePublication(t *testing.T) {
	for _, tc := range []struct {
		name              string
		host, shard, want int
	}{{"default-unset-host", 0, 1, 2}, {"host-override", 3, 1, 3}, {"shard-override", 0, 3, 6}} {
		t.Run(tc.name, func(t *testing.T) {
			typed := settings.DefaultConfig("127.0.0.1")
			typed.Pool.ConnectionsPerHost = tc.host
			typed.Pool.ConnectionsPerShard = tc.shard
			cfg := InternalNewClusterSettings("127.0.0.1")
			var captured *Config
			s, err := newSessionCommonWithRuntime(*cfg, &typed, func(s *Session, c Config) *Config {
				if s.privateStorage.InternalPool != nil || s.privateStorage.InternalEventBus != nil || s.privateStorage.InternalMetadataDescriber != nil {
					t.Fatal("typed settings attached after coordinator publication")
				}
				captured = &c
				return captured
			})
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			if captured == nil || s.privateStorage.InternalConnCfg.TypedConfig != captured || s.privateStorage.InternalPool.InternalServices.Config != captured {
				t.Fatal("transport/pool did not capture the initialized snapshot")
			}
			// Mutating the supplied configuration cannot change the captured counts.
			typed.Pool.ConnectionsPerHost = 99
			typed.Pool.ConnectionsPerShard = 99
			features := endpoint.ScyllaConnectionFeatures{}
			endpoint.ScyllaHostFeaturesData(&features.ScyllaHostFeatures).InternalNrShards = 2
			endpoint.ScyllaHostFeaturesData(&features.ScyllaHostFeatures).InternalMsbIgnore = 12
			conn := &transport.Conn{InternalScyllaSupported: features}
			p := pool.InternalNewHostPool(s.privateStorage.InternalPool.InternalServices, new(endpoint.HostInfo), 17, "")
			defer p.Close()
			if err := p.InternalInitConnPicker(conn); err != nil {
				t.Fatal(err)
			}
			picker, ok := p.InternalConnPicker.(*pool.InternalTypedConnPicker)
			if !ok {
				t.Fatalf("typed bootstrap selected %T", p.InternalConnPicker)
			}
			if got := picker.InternalTarget(); got != tc.want {
				t.Fatalf("captured target=%d want=%d", got, tc.want)
			}
		})
	}
}

func TestUnsetTypedConfigurationPreservesLegacyPicker(t *testing.T) {
	cfg := InternalNewClusterSettings("127.0.0.1")
	s, err := InternalNewSessionCommon(*cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if s.privateStorage.InternalPool.InternalServices.Config != nil {
		t.Fatal("unset configuration became a typed override")
	}
	p := pool.InternalNewHostPool(s.privateStorage.InternalPool.InternalServices, new(endpoint.HostInfo), 2, "")
	defer p.Close()
	if err := p.InternalInitConnPicker(new(transport.Conn)); err != nil {
		t.Fatal(err)
	}
	if _, ok := p.InternalConnPicker.(*pool.InternalDefaultConnPicker); !ok {
		t.Fatalf("unset picker=%T", p.InternalConnPicker)
	}
}
