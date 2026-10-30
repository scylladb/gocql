// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cluster

import implNet "net"

func InternalNewPoolServices(s *Session) poolServices {
	cfg, _ := s.v2PoolConfig()
	return poolServices{Context: s.privateStorage.InternalCtx, Logger: s.privateStorage.InternalLogger, Config: cfg, Conviction: s.privateStorage.InternalCfg.ConvictionPolicy, Reconnection: s.privateStorage.InternalCfg.ReconnectionPolicy, Worker: func(f func()) bool { return InternalSessionPeripheral(s).InternalStartWorker(f) }, Dial: s.connectShard, Connected: func(receiver *Session) func(a0 *hostConnPool) {
		return func(a0 *hostConnPool) {
			InternalSessionHandlePoolConnected(receiver, a0)
		}
	}(s), Down: func(receiver *Session) func(a0 implNet.
		IP, a1 int) {
		return func(a0 implNet.IP, a1 int) {
			InternalSessionHandleNodeDown(receiver, a0, a1)
		}
	}(s)}
}
func InternalNewHostConnPool(s *Session, h *HostInfo, size int, keyspace string) *hostConnPool {
	return newHostPool(InternalNewPoolServices(s), h, size, keyspace)
}
