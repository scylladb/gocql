//go:build integration
// +build integration

package gocql

// This file contains code to enable easy access to driver internals
// To be used only for integration test

import "fmt"

func (p *policyConnPool) MissingConnections() (int, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	total := 0

	// close the pools
	for _, pool := range p.hostConnPools {
		pool.mu.RLock()
		_, missing := pool.connPicker.Size()
		closed := pool.closed
		pool.mu.RUnlock()
		if closed {
			return 0, fmt.Errorf("pool for %s is closed", pool.host.HostID())
		}
		total += missing
	}
	return total, nil
}

func (s *Session) MissingConnections() (int, error) {
	if s.pool == nil {
		return 0, fmt.Errorf("pool is nil")
	}
	return s.pool.MissingConnections()
}

type ConnPickerIntegration interface {
	Pick(Token) *Conn
	Put(*Conn) error
	Remove(conn *Conn)
	InFlight() int
	Size() (int, int)
	Close()
	CloseAllConnections()

	// NextShard returns the shardID to connect to.
	// nrShard specifies how many shards the host has.
	// If nrShards is zero, the caller shouldn't use shard-aware port.
	NextShard() (shardID, nrShards int)
}

func (p *scyllaConnPicker) CloseAllConnections() {
	p.nrConns = 0
	closeConns(p.conns...)
	for id := range p.conns {
		p.conns[id] = nil
	}
}

func (p *defaultConnPicker) CloseAllConnections() {
	closeConns(p.conns...)
	p.conns = p.conns[:0]
}

func (p *nopConnPicker) CloseAllConnections() {
}

// Snapshot under the same locks used by admission, then close outside them.
// Conn.Close synchronously calls the pool error handler and may schedule repair.
func (pool *hostConnPool) CloseAllConnections() {
	pool.mu.Lock()
	if pool.closed {
		pool.mu.Unlock()
		return
	}
	var conns []*Conn
	switch picker := pool.connPicker.(type) {
	case *typedConnPicker:
		picker.mu.RLock()
		for _, shard := range picker.conns {
			conns = append(conns, shard...)
		}
		conns = append(conns, picker.excess...)
		picker.mu.RUnlock()
	case *scyllaConnPicker:
		conns = append(conns, picker.conns...)
	case *defaultConnPicker:
		conns = append(conns, picker.conns...)
	}
	for _, conn := range conns {
		pool.connPicker.Remove(conn)
	}
	pool.state = poolBroken
	pool.signalReservationLocked()
	pool.mu.Unlock()
	closeConns(conns...)
	pool.fill()
}

func (p *policyConnPool) CloseAllConnections() {
	p.mu.RLock()
	pools := make([]*hostConnPool, 0, len(p.hostConnPools))
	for _, pool := range p.hostConnPools {
		pools = append(pools, pool)
	}
	p.mu.RUnlock()
	for _, pool := range pools {
		pool.CloseAllConnections()
	}
}

func (s *Session) CloseAllConnections() {
	if s.pool != nil {
		s.pool.CloseAllConnections()
	}
}
