// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package gocql

import (
	"github.com/scylladb/gocql/v2/internal/connectivity"
	"github.com/scylladb/gocql/v2/internal/settings"
)

func connectivityConfig(c Config) connectivity.Config {
	return settings.InternalConnectivityConfig(c)
}
func connectivityConfigPointer(c Config) *connectivity.Config {
	return settings.InternalConnectivityConfigPointer(c)
}
