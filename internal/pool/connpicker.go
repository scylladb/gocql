// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package pool

import (
	"fmt"
	implEndpoint "github.com/scylladb/gocql/v2/internal/endpoint"
	"sync"
	"sync/atomic"
)

type ConnPicker interface {
	Pick(implEndpoint.Token) *Conn
	Put(*Conn) error
	Remove(conn *Conn)
	InFlight() int
	Size() (int, int)
	Close()

	// NextShard returns the shardID to connect to.
	// nrShard specifies how many shards the host has.
	// If nrShards is zero, the caller shouldn't use shard-aware port.
	NextShard() (shardID, nrShards int)

	GetConnectionCount() int
	GetExcessConnectionCount() int
	GetShardCount() int
}

// int64ConnPicker is an optional fast path on ConnPicker: shard-aware conn
// selection from a raw int64 routing token, avoiding the Token interface
// boxing. hostConnPool uses it when the routing token is an int64Token.
type InternalInt64ConnPicker interface {
	PickInt64(int64) *Conn
}

type InternalDefaultConnPicker struct {
	InternalConns []*Conn
	pos           uint32
	InternalSize  int
	InternalMu    sync.RWMutex
}

func (p *InternalDefaultConnPicker) GetConnectionCount() int {
	p.InternalMu.Lock()
	defer p.InternalMu.Unlock()
	return len(p.InternalConns)
}

func (p *InternalDefaultConnPicker) GetExcessConnectionCount() int {
	return 0
}

func (p *InternalDefaultConnPicker) GetShardCount() int {
	// It is not supposed to be used for scylla nodes and therefore does not know anything about shards count
	return 0
}

func InternalNewDefaultConnPicker(size int) *InternalDefaultConnPicker {
	if size <= 0 {
		panic(fmt.Sprintf("invalid pool size %d", size))
	}
	return &InternalDefaultConnPicker{
		InternalSize: size,
	}
}

func (p *InternalDefaultConnPicker) Remove(conn *Conn) {
	p.InternalMu.Lock()
	defer p.InternalMu.Unlock()

	for i, candidate := range p.InternalConns {
		if candidate == conn {
			last := len(p.InternalConns) - 1
			p.InternalConns[i], p.InternalConns = p.InternalConns[last], p.InternalConns[:last]
			break
		}
	}
}

func (p *InternalDefaultConnPicker) Close() {
	p.InternalMu.Lock()
	defer p.InternalMu.Unlock()

	conns := p.InternalConns
	p.InternalConns = nil
	for _, conn := range conns {
		if conn != nil {
			conn.Close()
		}
	}
}

func (p *InternalDefaultConnPicker) InFlight() int {
	p.InternalMu.RLock()
	size := len(p.InternalConns)
	p.InternalMu.RUnlock()
	return size
}

func (p *InternalDefaultConnPicker) Size() (int, int) {
	p.InternalMu.RLock()
	size := len(p.InternalConns)
	p.InternalMu.RUnlock()
	return size, p.InternalSize - size
}

func (p *InternalDefaultConnPicker) Pick(implEndpoint.Token) *Conn {
	return p.pickLeastBusy()
}

// PickInt64 is the raw-int64 variant of Pick; the token is unused, the least
// busy connection is picked either way.
func (p *InternalDefaultConnPicker) PickInt64(int64) *Conn {
	return p.pickLeastBusy()
}

func (p *InternalDefaultConnPicker) pickLeastBusy() *Conn {
	pos := int(atomic.AddUint32(&p.pos, 1) - 1)

	p.InternalMu.RLock()
	size := len(p.InternalConns)

	var (
		leastBusyConn    *Conn
		streamsAvailable int
	)

	// find the conn which has the most available streams, this is racy
	for i := 0; i < size; i++ {
		conn := p.InternalConns[(pos+i)%size]
		if conn == nil {
			continue
		}
		if streams := conn.AvailableStreams(); streams > streamsAvailable {
			leastBusyConn = conn
			streamsAvailable = streams
		}
	}
	p.InternalMu.RUnlock()

	return leastBusyConn
}

func (p *InternalDefaultConnPicker) Put(conn *Conn) error {
	p.InternalMu.Lock()
	p.InternalConns = append(p.InternalConns, conn)
	p.InternalMu.Unlock()
	return nil
}

func (*InternalDefaultConnPicker) NextShard() (shardID, nrShards int) {
	return 0, 0
}

// nopConnPicker is a no-operation implementation of ConnPicker, it's used when
// hostConnPool is created to allow deferring creation of the actual ConnPicker
// to the point where we have first connection.
type InternalNopConnPicker struct{}

func (p InternalNopConnPicker) GetConnectionCount() int {
	return 0
}

func (p InternalNopConnPicker) GetExcessConnectionCount() int {
	return 0
}

func (p InternalNopConnPicker) GetShardCount() int {
	return 0
}

func (InternalNopConnPicker) Pick(implEndpoint.Token) *Conn {
	return nil
}

func (InternalNopConnPicker) PickInt64(int64) *Conn {
	return nil
}

func (InternalNopConnPicker) Put(*Conn) error {
	return nil
}

func (InternalNopConnPicker) Remove(conn *Conn) {
}

func (InternalNopConnPicker) InFlight() int {
	return 0
}

func (InternalNopConnPicker) Size() (int, int) {
	// Return 1 to make hostConnPool to try to establish a connection.
	// When first connection is established hostConnPool replaces nopConnPicker
	// with a different ConnPicker implementation.
	return 0, 1
}

func (InternalNopConnPicker) Close() {
}

func (InternalNopConnPicker) NextShard() (shardID, nrShards int) {
	return 0, 0
}
