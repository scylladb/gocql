// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package wire

import "sync"

// ReceiveCache reuses only fully released raw v3/v4 frame allocations. One
// session owns it, with hard idle byte/object/individual limits independent of
// connection/shard counts. Outstanding page bytes are ordinary demand, not idle.
type ReceiveCache struct {
	mu           sync.Mutex
	entries      [][]byte
	bytes        int
	epoch        uint64
	closed       bool
	hits, misses uint64
}

const ReceiveCacheMaxBytes = 16 << 20
const ReceiveCacheMaxObjects = 64
const ReceiveCacheMaxObject = 9 << 20

func (c *ReceiveCache) Acquire(n int) ([]byte, uint64) {
	if c == nil {
		return make([]byte, n), 0
	}
	c.mu.Lock()
	epoch := c.epoch
	if !c.closed {
		best := -1
		for i, b := range c.entries {
			if cap(b) >= n && (cap(b) <= 4096 || n >= cap(b)-n) && (best < 0 || cap(b) < cap(c.entries[best])) {
				best = i
			}
		}
		if best >= 0 {
			c.hits++
			b := c.entries[best]
			c.bytes -= cap(b)
			last := len(c.entries) - 1
			c.entries[best] = c.entries[last]
			c.entries[last] = nil
			c.entries = c.entries[:last]
			c.mu.Unlock()
			return b[:n], epoch
		}
	}
	c.misses++
	c.mu.Unlock()
	return make([]byte, n), epoch
}
func (c *ReceiveCache) Release(b []byte, epoch uint64) {
	if c == nil || cap(b) > ReceiveCacheMaxObject || cap(b) == 0 {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed || epoch != c.epoch {
		return
	}
	// Keep the largest reusable frame classes under the same fixed session cap.
	// Inspect at most 64 slots; tiny metadata cannot starve data frame reuse.
	for len(c.entries) >= ReceiveCacheMaxObjects || cap(b) > ReceiveCacheMaxBytes-c.bytes {
		smallest := -1
		for i, v := range c.entries {
			if smallest < 0 || cap(v) < cap(c.entries[smallest]) {
				smallest = i
			}
		}
		if smallest < 0 || cap(c.entries[smallest]) >= cap(b) {
			return
		}
		c.bytes -= cap(c.entries[smallest])
		last := len(c.entries) - 1
		c.entries[smallest] = c.entries[last]
		c.entries[last] = nil
		c.entries = c.entries[:last]
	}
	c.entries = append(c.entries, b[:cap(b)])
	c.bytes += cap(b)
}
func (c *ReceiveCache) Clear() {
	if c == nil {
		return
	}
	c.mu.Lock()
	clear(c.entries)
	c.entries = nil
	c.bytes = 0
	c.epoch++
	c.mu.Unlock()
}
func (c *ReceiveCache) Close() {
	if c == nil {
		return
	}
	c.mu.Lock()
	clear(c.entries)
	c.entries = nil
	c.bytes = 0
	c.epoch++
	c.closed = true
	c.mu.Unlock()
}
func (c *ReceiveCache) Storage() (bytes, objects int) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.bytes, len(c.entries)
}

// ReceiveCacheStats reports cumulative reuse and current idle retention. Active
// response pages are excluded; they remain subject to frame/decompression limits.
type ReceiveCacheStats struct {
	Hits, Misses                   uint64
	RetainedBytes, RetainedObjects int
}

func (c *ReceiveCache) Stats() ReceiveCacheStats {
	if c == nil {
		return ReceiveCacheStats{}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return ReceiveCacheStats{Hits: c.hits, Misses: c.misses, RetainedBytes: c.bytes, RetainedObjects: len(c.entries)}
}
