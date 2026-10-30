// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package pool

import (
	implTransport "github.com/scylladb/gocql/v2/internal/transport"
)

type Conn = implTransport.Conn

type ConnErrorHandler = implTransport.ConnErrorHandler

type StreamLimits = implTransport.StreamLimits

func checkedV2PoolBudget(a0 PoolOptions, a1 int) (int, error) {
	return implTransport.InternalCheckedV2PoolBudget(a0, a1)
}

type exclusiveConn = implTransport.InternalExclusiveConn

func validateV2ConnectionSharding(a0 ScyllaConnectionFeatures) error {
	return implTransport.InternalValidateV2ConnectionSharding(a0)
}
