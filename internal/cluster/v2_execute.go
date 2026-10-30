// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cluster

import implEndpoint "github.com/scylladb/gocql/v2/internal/endpoint"

import (
	"context"
)

func InternalSessionTypedGeneration(s *Session) uint64 {
	return InternalSessionTypedSchemaGeneration(s)
}

func InternalSessionSelectTypedConn(s *Session, ctx context.Context, routing RoutingInfo, plan QueryPlan) (*Conn, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if s.Closed() {
		return nil, ErrClosed
	}
	if s.privateStorage.InternalPool == nil || s.privateStorage.InternalHostSource == nil {
		return nil, ErrNoConnections
	}
	pickTarget := func(target Target) *Conn {
		if routing.HostID.Set && target.HostID != routing.HostID.Value {
			return nil
		}
		s.privateStorage.InternalPool.InternalMu.RLock()
		pool := s.privateStorage.InternalPool.InternalHostConnPools[target.HostID]
		s.privateStorage.InternalPool.InternalMu.RUnlock()
		if pool == nil || pool.InternalHost == nil || !pool.InternalHost.IsUp() || InternalClusterSettingsFilterHost(&s.privateStorage.InternalCfg, pool.InternalHost) {
			return nil
		}
		return InternalSessionPickTypedPool(s, pool, target, routing)
	}
	if plan != nil {
		for {
			target, ok := plan.Next()
			if !ok {
				return nil, ErrNoConnections
			}
			if target.StrictShard && !target.ShardID.Set {
				return nil, typeValidation("routing shard", "strict target requires a shard ID")
			}
			if conn := pickTarget(target); conn != nil {
				return conn, nil
			}
			if target.StrictShard {
				return nil, typeValidation("routing shard", "strict target is unavailable")
			}
		}
	}
	if routing.HostID.Set {
		if conn := pickTarget(Target{HostID: routing.HostID.Value, ShardID: routing.ShardID, StrictShard: routing.ShardID.Set}); conn != nil {
			return conn, nil
		}
		return nil, ErrNoConnections
	}
	hosts := s.privateStorage.InternalHostSource.InternalGetHostsList()
	if len(hosts) == 0 {
		return nil, ErrNoConnections
	}
	pick := func(host *HostInfo, _ Target) *Conn {
		return pickTarget(Target{HostID: implEndpoint.InternalHostInfoHostUUID(host)})
	}
	start := int(s.privateStorage.nextTypedHost.Add(1) % uint64(len(hosts)))
	for i := 0; i < len(hosts); i++ {
		if conn := pick(hosts[(start+i)%len(hosts)], Target{}); conn != nil {
			return conn, nil
		}
	}
	return nil, ErrNoConnections
}

func InternalSessionObserveSafely(s *Session, fn func()) {
	defer func() {
		if v := recover(); v != nil && s.privateStorage.InternalLogger != nil {
			func() {
				defer func() { _ = recover() }()
				s.privateStorage.InternalLogger.Printf("gocql: observer panic: %v", v)
			}()
		}
	}()
	fn()
}

// pickTypedPool carries native shard targets through connection selection without
// manufacturing an old query object or discarding table-specific tablet routing.
func InternalSessionPickTypedPool(s *Session, pool *hostConnPool, target Target, routing RoutingInfo) *Conn {
	defer pool.InternalRequestTypedRepair()
	pool.InternalMu.RLock()
	defer pool.InternalMu.RUnlock()
	if !pool.InternalPickable() {
		return nil
	}
	if picker, ok := pool.InternalConnPicker.(*typedConnPicker); ok {
		return picker.InternalPickTarget(target, routing)
	}
	picker, ok := pool.InternalConnPicker.(*scyllaConnPicker)
	if !ok {
		if routing.Token.Set && !target.ShardID.Set && !routing.ShardID.Set {
			if p, ok := pool.InternalConnPicker.(int64ConnPicker); ok {
				return p.PickInt64(routing.Token.Value)
			}
		}
		conn := pool.InternalConnPicker.Pick(nil)
		shard := target.ShardID
		if routing.ShardID.Set {
			shard = routing.ShardID
		}
		if conn != nil && shard.Set && (target.StrictShard || routing.ShardID.Set) && uint32(implEndpoint.ScyllaConnectionFeaturesData(&conn.InternalScyllaSupported).InternalShard) != shard.Value {
			return nil
		}
		return conn
	}
	shard := target.ShardID
	if !shard.Set && routing.Token.Set && routing.Keyspace != "" && routing.Table != "" {
		if targets, err := s.Metadata().Replicas(routing.Keyspace, routing.Table, routing.Token.Value); err == nil {
			for _, candidate := range targets {
				if candidate.HostID == implEndpoint.InternalHostInfoHostUUID(pool.InternalHost) && candidate.ShardID.Set {
					shard = candidate.ShardID
					break
				}
			}
		}
	}
	picker.InternalMu.RLock()
	defer picker.InternalMu.RUnlock()
	if len(picker.InternalConns) == 0 {
		return nil
	}
	index := -1
	if shard.Set {
		if uint64(shard.Value) < uint64(len(picker.InternalConns)) {
			index = int(shard.Value)
		} else if target.StrictShard || routing.ShardID.Set || routing.Conditional {
			return nil
		}
	} else if routing.Token.Set {
		index = picker.InternalShardOf(int64Token(routing.Token.Value))
	}
	if index >= 0 && index < len(picker.InternalConns) && picker.InternalConns[index] != nil {
		conn := picker.InternalConns[index]
		if shard.Set || routing.Conditional {
			return conn
		}
		return picker.InternalMaybeReplaceWithLessBusyConnection(conn)
	}
	if index >= 0 && (target.StrictShard || routing.ShardID.Set || routing.Conditional) {
		return nil
	}
	return picker.InternalLeastBusyConn()
}

func InternalSessionApplyTypedFeedbackStamped(s *Session, response *responsePage, routing RoutingInfo, stamp tabletFeedbackStamp, features framerConfig) {
	if response == nil || routing.Keyspace == "" || routing.Table == "" || s.privateStorage.InternalMetadataDescriber == nil {
		return
	}
	key := "tablets-routing-v1"
	if features.InternalTabletsRoutingV2 {
		key = "tablets-routing-v2"
	} else if !features.InternalTabletsRoutingV1 {
		return
	}
	for _, entry := range response.Payload {
		if entry.Key != key || !entry.Value.Valid {
			continue
		}
		hint, err := parseTypedTabletPayload(entry.Value.Value, routing.Keyspace, routing.Table, features.InternalTabletsRoutingV2)
		if err != nil {
			if s.privateStorage.InternalLogger != nil {
				s.privateStorage.InternalLogger.Printf("gocql: tablet hint rejected: %v", err)
			}
			return
		}
		InternalSessionQueueTabletFeedback(s, hint, stamp)
		return
	}
}
