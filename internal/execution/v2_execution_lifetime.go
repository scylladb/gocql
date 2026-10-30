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
	"context"
	implCluster "github.com/scylladb/gocql/v2/internal/cluster"
	"sync"
	"time"
)

// Each logical operation has a closed input owner of its own. Its final drain
// includes accepted writers and speculative/prefetch workers, independently of
// output page leases and other operations using the same binding.
type InternalExecutionLifetime struct {
	mu          sync.Mutex
	cancel      context.CancelFunc
	stop        func()
	stopping    bool
	releaseOnce sync.Once
	done        chan struct{}
}

func (l *InternalLogicalExecution) InternalReleaseInput() {
	l.lifetime.releaseOnce.Do(func() { l.request.InternalOwner.InternalRelease() })
}
func (l *InternalLogicalExecution) installStop(stop func()) {
	l.lifetime.mu.Lock()
	stopping := l.lifetime.stopping
	if !stopping {
		l.lifetime.stop = stop
	}
	l.lifetime.mu.Unlock()
	if stopping {
		stop()
	}
}
func (l *InternalLogicalExecution) shutdown() {
	l.lifetime.mu.Lock()
	l.lifetime.stopping = true
	stop := l.lifetime.stop
	l.lifetime.stop = nil
	l.lifetime.mu.Unlock()
	l.lifetime.cancel()
	if stop != nil {
		stop()
	}
}
func (s *InternalSessionRuntime) registerExecution(l *InternalLogicalExecution, parent *inputOwner) error {
	implCluster.SessionData(s.Session).InternalSessionStateMu.RLock()
	defer implCluster.SessionData(s.Session).InternalSessionStateMu.RUnlock()
	if implCluster.SessionData(s.Session).InternalIsClosing || implCluster.SessionData(s.Session).InternalIsClosed {
		return ErrClosed
	}
	rt := s
	rt.InternalMu.Lock()
	defer rt.InternalMu.Unlock()
	if rt.InternalExecutions == nil {
		rt.InternalExecutions = make(map[*InternalLogicalExecution]struct{})
	}
	rt.InternalExecutions[l] = struct{}{}
	owner := newInputOwner()
	owner.InternalRefs = 1
	owner.InternalClosed = true
	owner.InternalOnDrain = func() {
		parent.InternalRelease()
		rt.InternalMu.Lock()
		delete(rt.InternalExecutions, l)
		rt.InternalMu.Unlock()
		close(l.lifetime.done)
	}
	l.request.InternalOwner = owner
	return nil
}
func (s *InternalSessionRuntime) stopExecutions() []*InternalLogicalExecution {
	rt := s
	rt.InternalMu.Lock()
	active := make([]*InternalLogicalExecution, 0, len(rt.InternalExecutions))
	for l := range rt.InternalExecutions {
		active = append(active, l)
	}
	rt.InternalMu.Unlock()
	// Cancel every operation before joining one of them.
	for _, l := range active {
		l.lifetime.cancel()
	}
	for _, l := range active {
		l.shutdown()
	}
	return active
}

// A zero duration disables only this local deadline. WithTimeout itself avoids
// allocating a timer when the caller already has an earlier deadline.
func InternalExecutionContext(parent context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	return implCluster.InternalExecutionContext(parent, timeout)
}
