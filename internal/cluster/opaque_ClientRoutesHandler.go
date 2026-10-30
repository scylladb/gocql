// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cluster

import (
	"sync"
	"sync/atomic"

	"github.com/scylladb/gocql/v2/events"

	"github.com/scylladb/gocql/v2/internal/eventbus"
)

type ClientRoutesHandlerStorage struct {
	needsResync           atomic.Bool
	InternalSession       *Session
	InternalWorkers       sync.WaitGroup
	stopOnce              sync.Once
	queueMu               sync.Mutex
	connectionEpoch       map[string]uint64
	pairEpoch             map[InternalPair]uint64
	InternalLog           StdLogger
	InternalC             InternalControlConnection
	InternalResolver      DNSResolver
	InternalSub           *eventbus.Subscriber[events.Event]
	InternalRouteCache    InternalClientRouteCache
	InternalAddrOverrides map[string]string // connectionID → user-supplied ConnectionAddr
	InternalUpdateTasks   chan InternalUpdateTask
	InternalCloseChan     chan struct{}
	InternalCfg           ClientRoutesConfig
	InternalPickTLSPorts  bool
	InternalInitialized   atomic.Bool
}

func ClientRoutesHandlerData(v *ClientRoutesHandler) *ClientRoutesHandlerStorage {
	return &v.privateStorage
}
