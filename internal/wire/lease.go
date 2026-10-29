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

package wire

import (
	"math"
	"sync/atomic"
)

// Lease keeps a Chain's backing storage alive until its final Release. A Lease
// must not be copied. Lease objects are never recycled, so an old pointer cannot
// retain a new owner's storage (the pooled-reference ABA problem).
type Lease struct {
	refs    atomic.Int64
	chain   Chain
	release func()
}

// NewLease creates a lease with one reference. release runs synchronously and
// exactly once on the final Release; it may be nil.
func NewLease(chain Chain, release func()) *Lease {
	l := &Lease{chain: chain, release: release}
	l.refs.Store(1)
	return l
}

// Retain acquires another reference, or returns false after the final release.
func (l *Lease) Retain() bool {
	if l == nil {
		return false
	}
	for {
		n := l.refs.Load()
		if n <= 0 || n == math.MaxInt64 {
			return false
		}
		if l.refs.CompareAndSwap(n, n+1) {
			return true
		}
	}
}

// Alive reports whether references remain. It does not acquire a reference;
// callers accessing storage must already hold a reference for the entire use.
// In particular, a concurrent final Release can invalidate a positive result.
func (l *Lease) Alive() bool { return l != nil && l.refs.Load() > 0 }

// Release drops a reference. Calls on nil or an already released lease are
// harmless. Every successful Retain still requires its own Release.
func (l *Lease) Release() {
	if l == nil {
		return
	}
	for {
		n := l.refs.Load()
		if n <= 0 {
			return
		}
		if l.refs.CompareAndSwap(n, n-1) {
			if n == 1 && l.release != nil {
				l.release()
			}
			return
		}
	}
}

// Chain returns the borrowed view. The caller must hold a reference throughout
// all uses of the returned view. A nil or fully released lease returns empty.
func (l *Lease) Chain() Chain {
	if !l.Alive() {
		return Chain{}
	}
	return l.chain
}
