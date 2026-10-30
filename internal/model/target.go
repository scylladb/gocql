// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package model
import "github.com/scylladb/gocql/v2/internal/cqltype"
type Target struct {
	StrictShard bool
	HostID      cqltype.UUID
	ShardID     cqltype.Option[uint32]
}
