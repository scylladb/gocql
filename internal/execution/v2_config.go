// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package execution

import (
	"context"
	implCluster "github.com/scylladb/gocql/v2/internal/cluster"
)

func OpenSingleHost(ctx context.Context, config Config, hostID UUID) (*implCluster.Session, error) {
	if hostID.IsEmpty() {
		return nil, typeValidation("host ID", "empty UUID")
	}
	previous := config.HostFilter
	config.HostFilter = func(host HostSnapshot) bool { return host.ID == hostID && (previous == nil || previous(host)) }
	return Connect(ctx, config)
}

func Connect(ctx context.Context, config Config) (*implCluster.Session, error) {
	return implCluster.InternalConnectCluster(ctx, config, func(s *implCluster.Session, c Config) *Config {
		rt := InternalRuntimeFor(s)
		rt.InternalProfile = c.DefaultProfile
		rt.InternalConfig = c
		return &rt.InternalConfig
	})
}
