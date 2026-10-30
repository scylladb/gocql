//go:build integration
// +build integration

// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package pool

import "fmt"

func (p *InternalPolicyConnPool) MissingConnections() (int, error) {
	p.InternalMu.Lock()
	defer p.InternalMu.Unlock()

	total := 0

	// close the pools
	for _, pool := range p.InternalHostConnPools {
		pool.InternalMu.RLock()
		_, missing := pool.InternalConnPicker.Size()
		closed := pool.InternalClosed
		pool.InternalMu.RUnlock()
		if closed {
			return 0, fmt.Errorf("pool for %s is closed", pool.InternalHost.HostID())
		}
		total += missing
	}
	return total, nil
}

func (p *InternalScyllaConnPicker) CloseAllConnections() {
	p.InternalNrConns = 0
	InternalCloseConns(p.InternalConns...)
	for id := range p.InternalConns {
		p.InternalConns[id] = nil
	}
}

// Snapshot under the same locks used by admission, then close outside them.
// Conn.Close synchronously calls the pool error handler and may schedule repair.
func (pool *InternalHostConnPool) CloseAllConnections() {
	pool.InternalMu.Lock()
	if pool.InternalClosed {
		pool.InternalMu.Unlock()
		return
	}
	var conns []*Conn
	switch picker := pool.InternalConnPicker.(type) {
	case *InternalTypedConnPicker:
		picker.InternalMu.RLock()
		for _, shard := range picker.InternalConns {
			conns = append(conns, shard...)
		}
		conns = append(conns, picker.InternalExcess...)
		picker.InternalMu.RUnlock()
	case *InternalScyllaConnPicker:
		conns = append(conns, picker.InternalConns...)
	case *InternalDefaultConnPicker:
		conns = append(conns, picker.InternalConns...)
	}
	for _, conn := range conns {
		pool.InternalConnPicker.Remove(conn)
	}
	pool.InternalState = InternalPoolBroken
	pool.signalReservationLocked()
	pool.InternalMu.Unlock()
	InternalCloseConns(conns...)
	pool.InternalFill()
}

func (p *InternalPolicyConnPool) CloseAllConnections() {
	p.InternalMu.RLock()
	pools := make([]*InternalHostConnPool, 0, len(p.InternalHostConnPools))
	for _, pool := range p.InternalHostConnPools {
		pools = append(pools, pool)
	}
	p.InternalMu.RUnlock()
	for _, pool := range pools {
		pool.CloseAllConnections()
	}
}

func (p *InternalDefaultConnPicker) CloseAllConnections() {
	InternalCloseConns(p.InternalConns...)
	p.InternalConns = p.InternalConns[:0]
}
func (p *InternalNopConnPicker) CloseAllConnections() {
}
