// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package pool

import (
	implEndpoint "github.com/scylladb/gocql/v2/internal/endpoint"
	logging "github.com/scylladb/gocql/v2/internal/logging"
	implSettings "github.com/scylladb/gocql/v2/internal/settings"
)

type Config = implSettings.Config
type ConvictionPolicy = implSettings.ConvictionPolicy

type HostInfo = implEndpoint.HostInfo

type PoolOptions = implSettings.PoolOptions

type ScyllaConnectionFeatures = implEndpoint.ScyllaConnectionFeatures

type StdLogger = logging.StdLogger
