// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package wire

import "testing"

func TestStep15ReceiveCacheOwnership(t *testing.T) {
	cache := new(ReceiveCache)
	data, epoch := cache.Acquire(4096)
	data[0] = 71
	chain, _ := NewChain(Borrow(data))
	lease := NewLease(chain, func() { cache.Release(data, epoch) })
	if !lease.Retain() {
		t.Fatal("retain")
	}
	lease.Release()
	if n, _ := cache.Storage(); n != 0 {
		t.Fatal("returned while retained")
	}
	other, _ := cache.Acquire(4096)
	if &other[0] == &data[0] {
		t.Fatal("live page reused")
	}
	lease.Release()
	if n, _ := cache.Storage(); n != 4096 {
		t.Fatal(n)
	}
	reused, nextEpoch := cache.Acquire(4096)
	if &reused[0] != &data[0] || reused[0] != 71 {
		t.Fatal("not actual backing reuse")
	}
	fresh := NewLease(CodecTestChain(t, reused), func() { cache.Release(reused, nextEpoch) })
	if fresh == lease || lease.Retain() {
		t.Fatal("lease ABA")
	}
	fresh.Release()
	cache.Clear()
	if n, c := cache.Storage(); n != 0 || c != 0 {
		t.Fatal("clear", n, c)
	}
	stale, staleEpoch := cache.Acquire(8192)
	cache.Clear()
	cache.Release(stale, staleEpoch)
	if n, _ := cache.Storage(); n != 0 {
		t.Fatal("stale generation returned")
	}
	for i := 0; i < 8; i++ {
		cache.Release(make([]byte, 1<<20), cache.epoch)
	}
	if n, c := cache.Storage(); n > ReceiveCacheMaxBytes || c > ReceiveCacheMaxObjects {
		t.Fatal("object/byte bound", n, c)
	}
	cache.Clear()
	cache.Release(make([]byte, ReceiveCacheMaxObject+1), cache.epoch)
	if n, _ := cache.Storage(); n != 0 {
		t.Fatal("oversized cached")
	}
	for i := 0; i < 3; i++ {
		cache.Release(make([]byte, 8<<20), cache.epoch)
	}
	if n, c := cache.Storage(); n != ReceiveCacheMaxBytes || c != 2 {
		t.Fatal("total budget", n, c)
	}
	active, e := cache.Acquire(8 << 20)
	cache.Close()
	cache.Release(active, e)
	if n, c := cache.Storage(); n != 0 || c != 0 {
		t.Fatal("shutdown retained", n, c)
	}
}
func CodecTestChain(t *testing.T, b []byte) Chain {
	t.Helper()
	c, e := NewChain(Borrow(b))
	if e != nil {
		t.Fatal(e)
	}
	return c
}

func TestStep15ReceiveCacheMixedAdmission(t *testing.T) {
	cache := new(ReceiveCache)
	for i := 0; i < 4; i++ {
		cache.Release(make([]byte, 32+i), cache.epoch)
	}
	hot, e := cache.Acquire(512 << 10)
	hot[0] = 71
	cache.Release(hot, e)
	reused, e := cache.Acquire(512 << 10)
	if &reused[0] != &hot[0] || reused[0] != 71 {
		t.Fatal("four tiny slots starved hot frame backing")
	}
	cache.Release(reused, e)
	for i := 0; i < 1000; i++ {
		cache.Release(make([]byte, 32+i%4), cache.epoch)
	}
	reused, e = cache.Acquire(512 << 10)
	if &reused[0] != &hot[0] {
		t.Fatal("tiny traffic evicted hot frame")
	}
	cache.Release(reused, e)
	for i := 0; i < 64; i++ {
		other, e := cache.Acquire(1024 + (i%4)*1024)
		cache.Release(other, e)
		large, e := cache.Acquire(512 << 10)
		if &large[0] != &hot[0] {
			t.Fatal("mixed classes lost hot backing")
		}
		cache.Release(large, e)
	}
	bytes, objects := cache.Storage()
	if bytes > ReceiveCacheMaxBytes || objects > ReceiveCacheMaxObjects {
		t.Fatal(bytes, objects)
	}
	cache.Clear()
	stale, e := cache.Acquire(1 << 20)
	cache.Clear()
	cache.Release(stale, e)
	if n, _ := cache.Storage(); n != 0 {
		t.Fatal("epoch stale admission")
	}
	cache.Close()
}
