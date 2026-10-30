// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package execution

import (
	implCluster "github.com/scylladb/gocql/v2/internal/cluster"
	cqltype "github.com/scylladb/gocql/v2/internal/cqltype"
	"time"
	// IncludeMetadata requests full result metadata even when a validated cache exists.
)

type Options struct {
	IncludeMetadata      bool
	Profile              *ProfileHandle
	Consistency          Option[Consistency]
	SerialConsistency    Option[Option[Consistency]]
	Timeout, PageTimeout Option[time.Duration]
	PageSize             Option[int32]
	Prefetch             Option[int]
	Idempotent           bool
	Timestamp            Option[int64]
	NowInSeconds         Option[int32]
	Keyspace             string
	Trace                bool
	CustomPayload        []PayloadEntry
	HostID               Option[UUID]
	ShardID              Option[uint32]
	Retry                RetryPolicy
	Observer             Observer
	privateStorage       OptionsStorage
}

type InternalResolvedOptions struct {
	InternalProfile Profile
	InternalOptions Options
}

func InternalResolveOptions(s *implCluster.Session, options Options) (InternalResolvedOptions, error) {
	p := defaultProfile()
	if s != nil {
		rt := InternalRuntimeFor(s)
		if rt.InternalProfile != nil {
			p = rt.InternalProfile.Snapshot()
		}
	}
	if options.Profile != nil {
		p = options.Profile.Snapshot()
	}
	if options.Consistency.Set {
		p.Consistency = options.Consistency.Value
	}
	if options.SerialConsistency.Set {
		p.SerialConsistency = options.SerialConsistency.Value
	}
	if options.Timeout.Set {
		p.Timeout = options.Timeout.Value
	}
	if options.PageTimeout.Set {
		p.PageTimeout = options.PageTimeout.Value
	}
	if options.PageSize.Set {
		p.PageSize = options.PageSize.Value
	}
	if options.Prefetch.Set {
		p.Prefetch = options.Prefetch.Value
	}
	if options.Retry != nil {
		p.Retry = options.Retry
	}
	if options.Consistency.Set && !validConsistency(p.Consistency) {
		return InternalResolvedOptions{}, typeValidation("consistency", "invalid consistency")
	}
	if options.SerialConsistency.Set && p.SerialConsistency.Set && p.SerialConsistency.Value != Serial && p.SerialConsistency.Value != LocalSerial {
		return InternalResolvedOptions{}, typeValidation("serial consistency", "must be SERIAL or LOCAL_SERIAL")
	}
	if options.Timeout.Set && p.Timeout < 0 || options.PageTimeout.Set && p.PageTimeout < 0 {
		return InternalResolvedOptions{}, typeValidation("timeout", "must not be negative")
	}
	if options.PageSize.Set && p.PageSize <= 0 {
		return InternalResolvedOptions{}, typeValidation("page size", "must be positive")
	}
	if options.Prefetch.Set && (p.Prefetch < 0 || p.Prefetch > 1) {
		return InternalResolvedOptions{}, typeValidation("prefetch", "must be zero or one")
	}
	if options.HostID.Set && options.HostID.Value.IsEmpty() {
		return InternalResolvedOptions{}, typeValidation("host ID", "empty UUID")
	}
	if options.ShardID.Set && !options.HostID.Set {
		return InternalResolvedOptions{}, typeValidation("shard", "requires a host ID")
	}
	keys := make(map[string]bool, len(options.CustomPayload))
	for _, entry := range options.CustomPayload {
		if len(entry.Key) > 65535 || keys[entry.Key] {
			return InternalResolvedOptions{}, typeValidation("custom payload", "invalid or duplicate key")
		}
		keys[entry.Key] = true
		if entry.Value.Valid {
			if err := cqltype.CheckBytesAlive(entry.Value.Value); err != nil {
				return InternalResolvedOptions{}, err
			}
		}
	}
	options.CustomPayload = append([]PayloadEntry(nil), options.CustomPayload...)
	return InternalResolvedOptions{InternalProfile: p, InternalOptions: options}, nil
}
