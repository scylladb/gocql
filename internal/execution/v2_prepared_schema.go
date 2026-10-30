// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package execution

import (
	implCluster "github.com/scylladb/gocql/v2/internal/cluster"
	"sync/atomic"
)

func (rt *InternalSessionRuntime) InternalSchemaTracker() *preparedSchemaTracker {
	rt.InternalMu.Lock()
	defer rt.InternalMu.Unlock()
	if rt.schema == nil {
		rt.schema = &preparedSchemaTracker{InternalEpochs: make(map[preparedDependencyKey]*atomic.Uint64)}
	}
	return rt.schema
}

func (s *InternalPreparedState) InternalCacheCurrent() bool {
	s.InternalMu.RLock()
	defer s.InternalMu.RUnlock()
	return s.InternalInitialEpoch.InternalCurrent()
}
func (rt *InternalSessionRuntime) beginPreparedWorker(s *implCluster.Session) bool {
	rt.prepareWorkersMu.Lock()
	defer rt.prepareWorkersMu.Unlock()
	if rt.prepareWorkersClosed || s.Closed() {
		return false
	}
	rt.prepareWorkers.Add(1)
	return true
}

// Remote schema observations have no total order. Coalesce the need for an
// authoritative typed refresh instead of picking a winner by opaque server ID.
func (s *InternalPreparedState) InternalNoteMetadataConflict() error {
	if s.InternalSession == nil {
		return nil
	}
	if s.InternalSession.Closed() {
		return ErrClosed
	}
	s.conflictMu.Lock()
	defer s.conflictMu.Unlock()
	return s.queueMetadataConflict()
}
func (s *InternalPreparedState) queueMetadataConflict() error {
	p := implCluster.InternalSessionPeripheral(s.InternalSession)
	s.InternalConflictDirty.Store(true)
	p.InternalMu.Lock()
	epoch := p.InternalSchemaReconciled.Load() + 1
	_, pending := p.InternalPreparedRefreshScopes[s.InternalInfo.Keyspace]
	p.InternalMu.Unlock()
	if s.InternalConflictEpoch.Load() == epoch && pending {
		return nil
	}
	if err := implCluster.InternalSessionScheduleV2PreparedRefresh(s.InternalSession, s.InternalInfo.Keyspace); err != nil {
		s.InternalConflictEpoch.Store(0)
		return err
	}
	s.InternalConflictEpoch.Store(epoch)
	s.InternalConflictRefresh.Store(true)
	target := "TABLE"
	if s.InternalInfo.Table == "" {
		target = "KEYSPACE"
	}
	implCluster.InternalSessionInvalidatePreparedSchema(s.InternalSession, implCluster.Event{Keyspace: s.InternalInfo.Keyspace, Object: s.InternalInfo.Table, Target: target})
	implCluster.InternalSessionMarkV2SchemaStale(s.InternalSession, s.InternalInfo.Keyspace, false)
	return nil
}
func (s *InternalPreparedState) InternalReconcilePendingConflict() error {
	if !s.InternalConflictDirty.Load() || s.InternalSession == nil {
		return nil
	}
	s.conflictMu.Lock()
	defer s.conflictMu.Unlock()
	p := implCluster.InternalSessionPeripheral(s.InternalSession)
	p.InternalMu.Lock()
	_, pending := p.InternalPreparedRefreshScopes[s.InternalInfo.Keyspace]
	p.InternalMu.Unlock()
	if pending {
		return nil
	}
	if s.InternalConflictEpoch.Load() != 0 {
		s.InternalConflictDirty.Store(false)
		return nil
	}
	return s.queueMetadataConflict()
}
