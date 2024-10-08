package gocql

import (
	"fmt"
	"sync"
	"sync/atomic"
)

type ConnPicker interface {
	Pick(Token, ExecutableQuery) *Conn
	Put(*Conn)
	Remove(conn *Conn)
	InFlight() int
	Size() (int, int)
	Close()

	// NextShard returns the shardID to connect to.
	// nrShard specifies how many shards the host has.
	// If nrShards is zero, the caller shouldn't use shard-aware port.
	NextShard() (shardID, nrShards int)
}

type defaultConnPicker struct {
	conns []*Conn
	pos   uint32
	size  int
	mu    sync.RWMutex
}

func newDefaultConnPicker(size int) *defaultConnPicker {
	if size <= 0 {
		panic(fmt.Sprintf("invalid pool size %d", size))
	}
	return &defaultConnPicker{
		size: size,
	}
}

func (p *defaultConnPicker) Remove(conn *Conn) {
	p.mu.Lock()
	defer p.mu.Unlock()

	for i, candidate := range p.conns {
		if candidate == conn {
			last := len(p.conns) - 1
			p.conns[i], p.conns = p.conns[last], p.conns[:last]
			break
		}
	}
}

func (p *defaultConnPicker) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()

	conns := p.conns
	p.conns = nil
	for _, conn := range conns {
		if conn != nil {
			conn.Close()
		}
	}
}

func (p *defaultConnPicker) InFlight() int {
	size := len(p.conns)
	return size
}

func (p *defaultConnPicker) Size() (int, int) {
	size := len(p.conns)
	return size, p.size - size
}

func (p *defaultConnPicker) Pick(Token, ExecutableQuery) *Conn {
	pos := int(atomic.AddUint32(&p.pos, 1) - 1)
	size := len(p.conns)

	var (
		leastBusyConn    *Conn
		streamsAvailable int
	)

	// find the conn which has the most available streams, this is racy
	for i := 0; i < size; i++ {
		conn := p.conns[(pos+i)%size]
		if conn == nil {
			continue
		}
		if streams := conn.AvailableStreams(); streams > streamsAvailable {
			leastBusyConn = conn
			streamsAvailable = streams
		}
	}

	return leastBusyConn
}

func (p *defaultConnPicker) Put(conn *Conn) {
	p.mu.Lock()
	p.conns = append(p.conns, conn)
	p.mu.Unlock()
}

func (*defaultConnPicker) NextShard() (shardID, nrShards int) {
	return 0, 0
}

// nopConnPicker is a no-operation implementation of ConnPicker, it's used when
// hostConnPool is created to allow deferring creation of the actual ConnPicker
// to the point where we have first connection.
type nopConnPicker struct{}

func (nopConnPicker) Pick(Token, ExecutableQuery) *Conn {
	return nil
}

func (nopConnPicker) Put(*Conn) {
}

func (nopConnPicker) Remove(conn *Conn) {
}

func (nopConnPicker) InFlight() int {
	return 0
}

func (nopConnPicker) Size() (int, int) {
	// Return 1 to make hostConnPool to try to establish a connection.
	// When first connection is established hostConnPool replaces nopConnPicker
	// with a different ConnPicker implementation.
	return 0, 1
}

func (nopConnPicker) Close() {
}

func (nopConnPicker) NextShard() (shardID, nrShards int) {
	return 0, 0
}
