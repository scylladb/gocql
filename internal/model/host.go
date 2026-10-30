// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package model
import("net/netip"; "github.com/scylladb/gocql/v2/internal/cqltype")
type HostSnapshot struct {
	ID                     cqltype.UUID
	Address                netip.AddrPort
	Datacenter, Rack       string
	Tokens                 []int64
	Enabled, Connected, Up bool
	Shards                 uint32
}
