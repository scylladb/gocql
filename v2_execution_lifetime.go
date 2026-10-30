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

package gocql

import (
	"context"
	"sync"
	"time"
)

// Each logical operation has a closed input owner of its own. Its final drain
// includes accepted writers and speculative/prefetch workers, independently of
// output page leases and other operations using the same binding.
type executionLifetime struct {
	mu          sync.Mutex
	cancel      context.CancelFunc
	stop        func()
	stopping    bool
	releaseOnce sync.Once
	done        chan struct{}
}

func (l *logicalExecution) releaseInput() {
	l.lifetime.releaseOnce.Do(func() { l.request.owner.release() })
}
func (l *logicalExecution) installStop(stop func()) {
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
func (l *logicalExecution) shutdown() {
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
func (s *Session) registerExecution(l *logicalExecution, parent *inputOwner) error {
	s.sessionStateMu.RLock()
	defer s.sessionStateMu.RUnlock()
	if s.isClosing || s.isClosed {
		return ErrClosed
	}
	rt := s.typedRuntime()
	rt.mu.Lock()
	defer rt.mu.Unlock()
	if rt.executions == nil {
		rt.executions = make(map[*logicalExecution]struct{})
	}
	rt.executions[l] = struct{}{}
	owner := newInputOwner()
	owner.refs = 1
	owner.closed = true
	owner.onDrain = func() {
		parent.release()
		rt.mu.Lock()
		delete(rt.executions, l)
		rt.mu.Unlock()
		close(l.lifetime.done)
	}
	l.request.owner = owner
	return nil
}
func (s *Session) stopExecutions() []*logicalExecution {
	rt := s.typedRuntime()
	rt.mu.Lock()
	active := make([]*logicalExecution, 0, len(rt.executions))
	for l := range rt.executions {
		active = append(active, l)
	}
	rt.mu.Unlock()
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
func executionContext(parent context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	if timeout > 0 {
		return context.WithTimeout(parent, timeout)
	}
	return context.WithCancel(parent)
}
