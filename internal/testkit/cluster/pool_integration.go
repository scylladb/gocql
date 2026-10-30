//go:build integration

// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cluster

import (
	"fmt"
	runtime "github.com/scylladb/gocql/v2/internal/cluster"
)

func MissingConnections(s *runtime.Session) (int, error) {
	pool := runtime.SessionData(s).InternalPool
	if pool == nil {
		return 0, fmt.Errorf("pool is nil")
	}
	return pool.MissingConnections()
}

// CloseAllConnections injects loss of the existing configured connections;
// repair continues through the same pool capacity and scheduling state.
func CloseAllConnections(s *runtime.Session) {
	if pool := runtime.SessionData(s).InternalPool; pool != nil {
		pool.CloseAllConnections()
	}
}
