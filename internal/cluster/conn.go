/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/*
 * Content before git sha 34fdeebefcbf183ed7f916f931aa0586fdaa1b40
 * Copyright (c) 2012, The Gocql authors,
 * provided under the BSD-3-Clause License.
 * See the NOTICE file distributed with this work for additional information.
 */

package cluster

import implEndpoint "github.com/scylladb/gocql/v2/internal/endpoint"

import (
	"context"

	implTransport "github.com/scylladb/gocql/v2/internal/transport"
	"time"
)

func (s *Session) connectShard(ctx context.Context, host *HostInfo, errorHandler ConnErrorHandler,
	shardID, nrShards int) (*Conn, error) {
	return InternalSessionDialShard(s, ctx, host, s.privateStorage.InternalConnCfg, errorHandler, shardID, nrShards)
}

// dial establishes a connection to a Cassandra node and notifies the session's connectObserver.
// note: every connection needs to get `conn.finalizeConnection` called on it when initialization process is done
func InternalSessionDial(s *Session, ctx context.Context, host *HostInfo, connConfig *ConnConfig, errorHandler ConnErrorHandler) (*Conn, error) {
	return InternalSessionDialShard(s, ctx, host, connConfig, errorHandler, 0, 0)
}

func InternalSessionDialShard(s *Session, ctx context.Context, host *HostInfo, connConfig *ConnConfig, errorHandler ConnErrorHandler,
	shardID, nrShards int) (*Conn, error) {
	lifetimeCtx := ctx
	if connConfig.TypedConfig != nil {
		if connConfig.ConnectTimeout > 0 {
			var cancel context.CancelFunc
			ctx, cancel = context.WithTimeout(ctx, connConfig.ConnectTimeout)
			defer cancel()
		}
		if deadline := implEndpoint.InternalHostInfoClaimSetupDeadline(host); !deadline.IsZero() {
			var cancel context.CancelFunc
			ctx, cancel = context.WithDeadline(ctx, deadline)
			defer cancel()
		}
		ctx = context.WithValue(ctx, connectionLifetimeKey{}, lifetimeCtx)
	}
	if s.privateStorage.InternalClientRoutesHandler != nil && connConfig.TypedConfig != nil && !implEndpoint.InternalHostInfoHostUUID(host).IsEmpty() {
		var observed ObservedConnect
		if s.privateStorage.connectObserver != nil {
			observed.Host = host
			observed.Start = time.Now()
		}
		conn, err := s.dialV2Routes(ctx, host, connConfig, errorHandler)
		if s.privateStorage.connectObserver != nil {
			observed.End = time.Now()
			observed.Err = err
			s.privateStorage.connectObserver.ObserveConnect(observed)
		}
		return conn, err
	}
	var obs ObservedConnect

	current := implEndpoint.InternalHostInfoGetTranslatedConnectionInfo(host)
	updated, err := translateHostAddresses(s.privateStorage.InternalAddressTranslator, host, s.privateStorage.InternalLogger)
	if err != nil {
		return nil, err
	}
	if current == nil || !updated.Equal(current) {
		implEndpoint.InternalHostInfoSetTranslatedConnectionInfo(host, updated)
	}

	if s.privateStorage.connectObserver != nil {
		obs.Host = host
		obs.Start = time.Now()
	}

	conn, err := InternalSessionDialWithoutObserver(s, ctx, host, connConfig, errorHandler, shardID, nrShards)

	if s.privateStorage.connectObserver != nil {
		obs.End = time.Now()
		obs.Err = err
		s.privateStorage.connectObserver.ObserveConnect(obs)
	}

	return conn, err
}

// dialWithoutObserver establishes connection to a Cassandra node.
//
// dialWithoutObserver does not notify the connection observer, so you most probably want to call dial() instead.
//
// If nrShards is zero, shard-aware dialing is disabled.
func InternalSessionDialWithoutObserver(s *Session, ctx context.Context, host *HostInfo, cfg *ConnConfig, errorHandler ConnErrorHandler, shardID, nrShards int) (*Conn, error) {
	var reporter implTransport.InternalStartupReporter
	if s.privateStorage.InternalDriverConfigReporter != nil {
		reporter = s.privateStorage.InternalDriverConfigReporter
	}
	return implTransport.Dial(ctx, host, cfg, errorHandler, shardID, nrShards, implTransport.Opening{Context: s.privateStorage.InternalCtx, Hooks: InternalNewConnectionHooks(s), Identity: startupIdentity{DriverName: s.privateStorage.InternalCfg.DriverName, DriverVersion: s.privateStorage.InternalCfg.DriverVersion, ApplicationInfo: s.privateStorage.InternalCfg.ApplicationInfo, SessionID: s.privateStorage.id}, Reporter: reporter, MetadataTimeout: s.privateStorage.InternalCfg.MetadataSchemaRequestTimeout, CoalesceWait: s.privateStorage.InternalCfg.WriteCoalesceWaitTime, PoolPolicy: connectionPoolPolicy{InternalDisableShardAware: s.privateStorage.InternalCfg.DisableShardAwarePort, InternalMaxExcessRate: s.privateStorage.InternalCfg.MaxExcessShardConnectionsRate}, MaxRequests: s.privateStorage.InternalCfg.MaxRequestsPerConn, FrameObserver: s.privateStorage.frameObserver, StreamObserver: s.privateStorage.streamObserver})
}
