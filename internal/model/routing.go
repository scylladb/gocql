// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package model

import (
	cqltype "github.com/scylladb/gocql/v2/internal/cqltype"
)

type RoutingInfo struct {
	Keyspace, Table string
	Token           cqltype.Option[int64]
	Kind            cqltype.StatementKind
	Conditional     bool
	Consistency     cqltype.Consistency
	HostID          cqltype.Option[cqltype.UUID]
	ShardID         cqltype.Option[uint32]
}
