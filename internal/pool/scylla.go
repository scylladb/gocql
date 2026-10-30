// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package pool

import (
	"errors"
	"fmt"
	"math"

	"sync"
	"sync/atomic"

	"time"

	"github.com/scylladb/gocql/v2/internal/debug"
	implEndpoint "github.com/scylladb/gocql/v2/internal/endpoint"
)

// scyllaConnPicker is a specialised ConnPicker that selects connections based
// on token trying to get connection to a shard containing the given token.
// A list of excess connections is maintained to allow for lazy closing of
// connections to already opened shards. Keeping excess connections open helps
// reaching equilibrium faster since the likelihood of hitting the same shard
// decreases with the number of connections to the shard.
//
// scyllaConnPicker keeps track of the details about the shard-aware port.
// When used as a Dialer, it connects to the shard-aware port instead of the
// regular port (if the node supports it). For each subsequent connection
// it tries to make, the shard that it aims to connect to is chosen
// in a round-robin fashion.
type InternalScyllaConnPicker struct {
	InternalLogger StdLogger
	// disableShardAwarePortUntil is used to temporarily disable new connections to the shard-aware port temporarily
	InternalDisableShardAwarePortUntil *atomic.Pointer[time.Time]
	InternalAddress                    string
	InternalExcessConns                []*Conn
	InternalConns                      []*Conn
	InternalMu                         sync.RWMutex
	InternalNrShards                   int
	InternalPos                        uint64
	InternalLastAttemptedShard         int
	InternalMsbIgnore                  uint64
	InternalNrConns                    int
	InternalExcessConnsLimitRate       float32
	InternalHostId                     UUID
	InternalShardAwarePortDisabled     bool
}

func InternalNewScyllaConnPicker(conn *Conn, logger StdLogger) *InternalScyllaConnPicker {
	addr := conn.Address()

	if implEndpoint.ScyllaHostFeaturesData(&conn.InternalScyllaSupported.ScyllaHostFeatures).InternalNrShards == 0 {
		panic(fmt.Sprintf("scylla: %s not a sharded connection", addr))
	}

	if debug.Enabled {
		logger.Printf("scylla: %s new conn picker sharding options %+v", addr, conn.InternalScyllaSupported)
	}

	return &InternalScyllaConnPicker{
		InternalAddress:                addr,
		InternalHostId:                 implEndpoint.HostInfoData(conn.InternalHost).InternalHostId,
		InternalNrShards:               implEndpoint.ScyllaHostFeaturesData(&conn.InternalScyllaSupported.ScyllaHostFeatures).InternalNrShards,
		InternalMsbIgnore:              implEndpoint.ScyllaHostFeaturesData(&conn.InternalScyllaSupported.ScyllaHostFeatures).InternalMsbIgnore,
		InternalLastAttemptedShard:     0,
		InternalShardAwarePortDisabled: conn.InternalPoolPolicy.InternalDisableShardAware,
		InternalLogger:                 logger,
		InternalExcessConnsLimitRate:   conn.InternalPoolPolicy.InternalMaxExcessRate,

		InternalDisableShardAwarePortUntil: new(atomic.Pointer[time.Time]),
	}
}

func (p *InternalScyllaConnPicker) Pick(t implEndpoint.Token) *Conn {
	p.InternalMu.RLock()
	defer p.InternalMu.RUnlock()

	if len(p.InternalConns) == 0 {
		return nil
	}

	if t == nil {
		return p.InternalLeastBusyConn()
	}

	mmt, ok := t.(implEndpoint.InternalInt64Token)
	// double check if that's murmur3 token
	if !ok {
		return nil
	}

	return p.pickInt64Locked(mmt)
}

// PickInt64 is the raw-int64 fast-path variant of Pick, avoiding the Token
// interface boxing. It must only be called with int64-based routing tokens
// (Murmur3/ScyllaCDC), which is guaranteed by int64TokenSelectedHost.
func (p *InternalScyllaConnPicker) PickInt64(token int64) *Conn {
	p.InternalMu.RLock()
	defer p.InternalMu.RUnlock()

	if len(p.InternalConns) == 0 {
		return nil
	}

	return p.pickInt64Locked(implEndpoint.InternalInt64Token(token))
}

// pickInt64Locked selects the shard-aware connection for the given raw token.
// Must be called with p.mu held.
func (p *InternalScyllaConnPicker) pickInt64Locked(mmt implEndpoint.InternalInt64Token) *Conn {
	idx := -1

	if idx == -1 {
		idx = p.InternalShardOf(mmt)
	}

	if c := p.InternalConns[idx]; c != nil {
		// We have this shard's connection
		// so let's give it to the caller.
		// But only if it's not loaded too much and load is well distributed.
		return p.InternalMaybeReplaceWithLessBusyConnection(c)
	}
	return p.InternalLeastBusyConn()
}

func (p *InternalScyllaConnPicker) InternalMaybeReplaceWithLessBusyConnection(c *Conn) *Conn {
	if !InternalIsHeavyLoaded(c) {
		return c
	}
	alternative := p.InternalLeastBusyConn()
	if alternative == nil || alternative.AvailableStreams()*120 > c.AvailableStreams()*100 {
		return c
	} else {
		return alternative
	}
}

func InternalIsHeavyLoaded(c *Conn) bool {
	return c.InternalStreams.NumStreams/2 > c.AvailableStreams()
}

func (p *InternalScyllaConnPicker) InternalLeastBusyConn() *Conn {
	var (
		leastBusyConn    *Conn
		streamsAvailable int
	)
	idx := int(atomic.AddUint64(&p.InternalPos, 1))
	// find the conn which has the most available streams, this is racy
	for i := range p.InternalConns {
		if conn := p.InternalConns[(idx+i)%len(p.InternalConns)]; conn != nil {
			if streams := conn.AvailableStreams(); streams > streamsAvailable {
				leastBusyConn = conn
				streamsAvailable = streams
			}
		}
	}
	return leastBusyConn
}

func (p *InternalScyllaConnPicker) InternalShardOf(token implEndpoint.InternalInt64Token) int {
	shards := uint64(p.InternalNrShards)
	z := uint64(token+math.MinInt64) << p.InternalMsbIgnore
	lo := z & 0xffffffff
	hi := (z >> 32) & 0xffffffff
	mul1 := lo * shards
	mul2 := hi * shards
	sum := (mul1 >> 32) + mul2
	return int(sum >> 32)
}

func (p *InternalScyllaConnPicker) Put(conn *Conn) error {
	var (
		nrShards = implEndpoint.ScyllaHostFeaturesData(&conn.InternalScyllaSupported.ScyllaHostFeatures).InternalNrShards
		shard    = implEndpoint.ScyllaConnectionFeaturesData(&conn.InternalScyllaSupported).InternalShard
	)

	if nrShards == 0 {
		return errors.New("server reported that it has no shards")
	}

	p.InternalMu.Lock()
	defer p.InternalMu.Unlock()

	if nrShards != p.InternalNrShards {
		if debug.Enabled {
			p.InternalLogger.Printf("scylla: %s shard count changed from %d to %d, rebuilding connection pool",
				p.InternalAddress, p.InternalNrShards, nrShards)
		}
		p.handleShardCountChange(conn, nrShards)
	} else if nrShards != len(p.InternalConns) {
		conns := p.InternalConns
		p.InternalConns = make([]*Conn, nrShards)
		copy(p.InternalConns, conns)
	}

	if c := p.InternalConns[shard]; c != nil {
		if conn.InternalIsShardAware {
			// A connection made to the shard-aware port resulted in duplicate
			// connection to the same shard being made. Because this is never
			// intentional, it suggests that a NAT or AddressTranslator
			// changes the source port along the way, therefore we can't trust
			// the shard-aware port to return connection to the shard
			// that we requested. Fall back to non-shard-aware port for some time.
			p.InternalLogger.Printf(
				"scylla: connection to shard-aware address %s resulted in wrong shard being assigned; please check that you are not behind a NAT or AddressTranslater which changes source ports; falling back to non-shard-aware port for %v",
				p.InternalAddress,
				InternalScyllaShardAwarePortFallbackDuration,
			)
			until := time.Now().Add(InternalScyllaShardAwarePortFallbackDuration)
			p.InternalDisableShardAwarePortUntil.Store(&until)

			return fmt.Errorf("connection landed on %d shard that already has connection", shard)
		} else {
			p.InternalExcessConns = append(p.InternalExcessConns, conn)
			if debug.Enabled {
				p.InternalLogger.Printf("scylla: %s put shard %d excess connection total: %d missing: %d excess: %d", p.InternalAddress, shard, p.InternalNrConns, p.InternalNrShards-p.InternalNrConns, len(p.InternalExcessConns))
			}
		}
	} else {
		p.InternalConns[shard] = conn
		p.InternalNrConns++
		if debug.Enabled {
			p.InternalLogger.Printf("scylla: %s put shard %d connection total: %d missing: %d", p.InternalAddress, shard, p.InternalNrConns, p.InternalNrShards-p.InternalNrConns)
		}
	}

	if p.shouldCloseExcessConns() {
		p.closeExcessConnsLocked()
	}

	return nil
}

func (p *InternalScyllaConnPicker) handleShardCountChange(newConn *Conn, newShardCount int) {
	oldShardCount := p.InternalNrShards
	oldConns := make([]*Conn, len(p.InternalConns))
	copy(oldConns, p.InternalConns)

	if debug.Enabled {
		p.InternalLogger.Printf("scylla: %s handling shard topology change from %d to %d", p.InternalAddress, oldShardCount, newShardCount)
	}

	newConns := make([]*Conn, newShardCount)
	var toClose []*Conn
	migratedCount := 0

	for i, conn := range oldConns {
		if conn == nil {
			continue
		}
		if i < newShardCount {
			newConns[i] = conn
			migratedCount++
		} else {
			toClose = append(toClose, conn)
		}
	}

	p.InternalNrShards = newShardCount
	p.InternalMsbIgnore = implEndpoint.ScyllaHostFeaturesData(&newConn.InternalScyllaSupported.ScyllaHostFeatures).InternalMsbIgnore
	p.InternalConns = newConns
	p.InternalNrConns = migratedCount
	p.InternalLastAttemptedShard = 0

	if len(toClose) > 0 {
		go InternalCloseConns(toClose...)
	}

	if debug.Enabled {
		p.InternalLogger.Printf("scylla: %s migrated %d/%d connections to new shard topology, closing %d excess connections", p.InternalAddress, migratedCount, len(oldConns), len(toClose))
	}
}

func (p *InternalScyllaConnPicker) shouldCloseExcessConns() bool {
	if p.InternalNrConns >= p.InternalNrShards {
		return true
	}
	return len(p.InternalExcessConns) > int(p.InternalExcessConnsLimitRate*float32(p.InternalNrShards))
}

func (p *InternalScyllaConnPicker) GetConnectionCount() int {
	p.InternalMu.RLock()
	defer p.InternalMu.RUnlock()
	return p.InternalNrConns
}

func (p *InternalScyllaConnPicker) GetExcessConnectionCount() int {
	p.InternalMu.RLock()
	defer p.InternalMu.RUnlock()
	return len(p.InternalExcessConns)
}

func (p *InternalScyllaConnPicker) GetShardCount() int {
	p.InternalMu.RLock()
	defer p.InternalMu.RUnlock()
	return p.InternalNrShards
}

func (p *InternalScyllaConnPicker) Remove(conn *Conn) {
	shard := implEndpoint.ScyllaConnectionFeaturesData(&conn.InternalScyllaSupported).InternalShard

	if implEndpoint.ScyllaHostFeaturesData(&conn.InternalScyllaSupported.ScyllaHostFeatures).InternalNrShards == 0 {
		// It is possible for Remove to be called before the connection is added to the pool.
		// Ignoring these connections here is safe.
		if debug.Enabled {
			p.InternalLogger.Printf("scylla: %s has unknown sharding state, ignoring it", p.InternalAddress)
		}
		return
	}
	if debug.Enabled {
		p.InternalLogger.Printf("scylla: %s remove shard %d connection", p.InternalAddress, shard)
	}

	p.InternalMu.Lock()
	defer p.InternalMu.Unlock()

	if p.InternalConns[shard] != nil {
		p.InternalConns[shard] = nil
		p.InternalNrConns--
	}
}

func (p *InternalScyllaConnPicker) InFlight() int {
	p.InternalMu.RLock()
	defer p.InternalMu.RUnlock()
	result := 0
	for _, conn := range p.InternalConns {
		if conn != nil {
			result = result + (conn.InternalStreams.InUse())
		}
	}
	return result
}

func (p *InternalScyllaConnPicker) Size() (int, int) {
	p.InternalMu.RLock()
	defer p.InternalMu.RUnlock()
	return p.InternalNrConns, p.InternalNrShards - p.InternalNrConns
}

func (p *InternalScyllaConnPicker) Close() {
	p.InternalMu.Lock()
	conns := p.InternalConns
	p.InternalConns = nil
	p.InternalNrConns = 0
	excessConns := p.InternalExcessConns
	p.InternalExcessConns = nil
	p.InternalMu.Unlock()

	// Close connections outside of the lock to avoid deadlocks when a
	// connection close triggers HandleError. See scylladb/gocql#53.
	if len(conns) > 0 {
		if debug.Enabled {
			p.InternalLogger.Printf("scylla: %s closing %d connections", p.InternalAddress, len(conns))
		}
		go InternalCloseConns(conns...)
	} else if debug.Enabled {
		p.InternalLogger.Printf("scylla: %s no connections to close", p.InternalAddress)
	}

	if len(excessConns) > 0 {
		if debug.Enabled {
			p.InternalLogger.Printf("scylla: %s closing %d excess connections", p.InternalAddress, len(excessConns))
		}
		go InternalCloseConns(excessConns...)
	} else if debug.Enabled {
		p.InternalLogger.Printf("scylla: %s no excess connections to close", p.InternalAddress)
	}
}

// closeExcessConnsLocked closes excess connections. Must be called with p.mu held.
// The actual connection closing happens asynchronously outside the lock.
func (p *InternalScyllaConnPicker) closeExcessConnsLocked() {
	if len(p.InternalExcessConns) == 0 {
		if debug.Enabled {
			p.InternalLogger.Printf("scylla: %s no excess connections to close", p.InternalAddress)
		}
		return
	}

	conns := p.InternalExcessConns
	p.InternalExcessConns = nil

	if debug.Enabled {
		p.InternalLogger.Printf("scylla: %s closing %d excess connections", p.InternalAddress, len(conns))
	}
	go InternalCloseConns(conns...)
}

// closeConns closes a list of connections. Must be called outside of any lock
// to avoid deadlocks when a connection close triggers HandleError.
// See scylladb/gocql#53.
func InternalCloseConns(conns ...*Conn) {
	for _, conn := range conns {
		if conn != nil {
			conn.Close()
		}
	}
}

// NextShard returns the shardID to connect to.
// nrShard specifies how many shards the host has.
// If nrShards is zero, the caller shouldn't use shard-aware port.
func (p *InternalScyllaConnPicker) NextShard() (shardID, nrShards int) {
	if p.InternalShardAwarePortDisabled {
		return 0, 0
	}

	if disableUntil := p.InternalDisableShardAwarePortUntil.Load(); disableUntil != nil && time.Now().Before(*disableUntil) {
		// There is suspicion that the shard-aware-port is not reachable
		// or misconfigured, fall back to the non-shard-aware port
		return 0, 0
	}

	p.InternalMu.Lock()
	defer p.InternalMu.Unlock()

	// Find the shard without a connection
	// It's important to start counting from 1 here because we want
	// to consider the next shard after the previously attempted one
	for i := 1; i <= p.InternalNrShards; i++ {
		shardID := (p.InternalLastAttemptedShard + i) % p.InternalNrShards
		if p.InternalConns == nil || p.InternalConns[shardID] == nil {
			p.InternalLastAttemptedShard = shardID
			return shardID, p.InternalNrShards
		}
	}

	// We did not find an unallocated shard
	// We will dial the non-shard-aware port
	return 0, 0
}

const InternalScyllaShardAwarePortFallbackDuration time.Duration = 5 * time.Minute
