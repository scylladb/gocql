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
	"sync"
	"sync/atomic"
	"testing"
)

func TestLeaseLifecycleAndNoResurrection(t *testing.T) {
	var calls atomic.Int64
	c := chain(t, BorrowString("retained"))
	l := NewLease(c, func() { calls.Add(1) })
	if !l.Alive() || !l.Retain() || l.Chain().Len() != 8 {
		t.Fatal("new lease cannot be retained or read")
	}
	l.Release()
	if calls.Load() != 0 || l.Chain().Len() != 8 {
		t.Fatal("storage released with an outstanding reference")
	}
	l.Release()
	l.Release()
	if calls.Load() != 1 || l.Retain() || l.Alive() || l.Chain().Len() != 0 {
		t.Fatal("final release was not exactly once or lease was resurrected")
	}
	other := NewLease(c, nil)
	if l == other || l.Retain() || !other.Retain() {
		t.Fatal("new lease reused the old lease identity")
	}
	other.Release()
	other.Release()
	var nilLease *Lease
	nilLease.Release()
	if nilLease.Retain() || nilLease.Alive() || nilLease.Chain().Len() != 0 {
		t.Fatal("nil lease must be inert")
	}
}

func TestLeaseConcurrentReferences(t *testing.T) {
	var calls atomic.Int64
	l := NewLease(chain(t, BorrowString("stable")), func() { calls.Add(1) })
	var wg sync.WaitGroup
	for range 32 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 1000 {
				if !l.Retain() {
					t.Error("retain failed while the original reference remained held")
					return
				}
				if l.Chain().Len() != 6 {
					t.Error("retained lease lost its chain")
				}
				l.Release()
			}
		}()
	}
	wg.Wait()
	if calls.Load() != 0 {
		t.Fatal("callback ran with original reference still held")
	}
	l.Release()
	if calls.Load() != 1 || l.Retain() {
		t.Fatal("concurrent references did not end in a single final release")
	}
}

func TestLeaseRetainRacesFinalRelease(t *testing.T) {
	for range 200 {
		var calls atomic.Int64
		l := NewLease(Chain{}, func() { calls.Add(1) })
		start := make(chan struct{})
		var wg sync.WaitGroup
		for range 8 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				if l.Retain() {
					l.Release()
				}
			}()
		}
		close(start)
		l.Release()
		wg.Wait()
		if calls.Load() != 1 || l.Retain() {
			t.Fatalf("final-release race: %d callbacks", calls.Load())
		}
	}
}

func TestLeaseRefcountOverflow(t *testing.T) {
	l := NewLease(Chain{}, nil)
	l.refs.Store(math.MaxInt64)
	if l.Retain() || l.refs.Load() != math.MaxInt64 {
		t.Fatal("refcount overflow was not rejected")
	}
}

func BenchmarkLeaseRetainRelease(b *testing.B) {
	l := NewLease(Chain{}, nil)
	b.Cleanup(l.Release)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		l.Retain()
		l.Release()
	}
}
