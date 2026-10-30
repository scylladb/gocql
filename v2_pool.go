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
	"math"
	"sync"
	"sync/atomic"
	"time"
)

type poolState uint8

const (
	poolInitializing poolState = iota
	poolReady
	poolBroken
	poolClosed
)

func (s *Session) v2PoolConfig() (*Config, bool) {
	if s == nil || s.connCfg == nil || s.connCfg.TypedConfig == nil {
		return nil, false
	}
	return s.connCfg.TypedConfig, true
}

// typedConnPicker keeps actual shard membership separate from sizing mode.
// Positive host count is a total budget; otherwise each shard has its own
// connection target. Reservation never removes a connection from this budget.
type typedConnPicker struct {
	mu            sync.RWMutex
	conns         [][]*Conn
	excess        []*Conn
	retired       []*Conn
	pending       []int
	options       PoolOptions
	nrShards      int
	msbIgnore     uint64
	pos           atomic.Uint64
	fallbackUntil time.Time
	closed        bool
	generation    uint64
}

func newTypedConnPicker(c *Conn, opts PoolOptions) (*typedConnPicker, error) {
	if err := validateV2ConnectionSharding(c.scyllaSupported); err != nil {
		return nil, err
	}
	shards := max(1, c.scyllaSupported.nrShards)
	if _, err := checkedV2PoolBudget(opts, shards); err != nil {
		return nil, err
	}
	return &typedConnPicker{conns: make([][]*Conn, shards), pending: make([]int, shards), nrShards: shards, msbIgnore: c.scyllaSupported.msbIgnore, options: opts}, nil
}
func (p *typedConnPicker) target() int {
	if p.options.ConnectionsPerHost > 0 {
		return p.options.ConnectionsPerHost
	}
	return p.nrShards * p.options.ConnectionsPerShard
}
func (p *typedConnPicker) count() int {
	n := 0
	for _, cs := range p.conns {
		n += len(cs)
	}
	return n
}
func (p *typedConnPicker) Size() (int, int) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	n := p.count()
	return n, max(0, p.target()-n)
}
func (p *typedConnPicker) GetConnectionCount() int { n, _ := p.Size(); return n }
func (p *typedConnPicker) GetExcessConnectionCount() int {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return len(p.excess)
}
func (p *typedConnPicker) GetShardCount() int { p.mu.RLock(); defer p.mu.RUnlock(); return p.nrShards }
func (p *typedConnPicker) InFlight() int {
	p.mu.RLock()
	defer p.mu.RUnlock()
	n := 0
	for _, cs := range p.conns {
		for _, c := range cs {
			n += c.streams.InUse()
		}
	}
	return n
}
func (p *typedConnPicker) NextShard() (int, int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed || p.options.ConnectionsPerHost > 0 || !p.options.ShardAwarePort || p.nrShards <= 1 || time.Now().Before(p.fallbackUntil) {
		return 0, 0
	}
	shard := -1
	load := math.MaxInt
	for i, cs := range p.conns {
		n := len(cs) + p.pending[i]
		if n < p.options.ConnectionsPerShard && n < load {
			shard, load = i, n
		}
	}
	if shard < 0 {
		return 0, 0
	}
	p.pending[shard]++
	return shard, p.nrShards
}
func (p *typedConnPicker) dialFinished(shard, shards int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if shards == p.nrShards && shard >= 0 && shard < len(p.pending) && p.pending[shard] > 0 {
		p.pending[shard]--
	}
}
func (p *typedConnPicker) Put(c *Conn) error { return p.put(c, 0, false) }
func (p *typedConnPicker) put(c *Conn, expected uint64, check bool) error {
	return p.putMode(c, expected, check, false)
}
func (p *typedConnPicker) putInitial(c *Conn) error { return p.putMode(c, 0, false, true) }
func (p *typedConnPicker) putMode(c *Conn, expected uint64, check, initial bool) error {
	if err := validateV2ConnectionSharding(c.scyllaSupported); err != nil {
		return err
	}
	if _, err := checkedV2PoolBudget(p.options, max(1, c.scyllaSupported.nrShards)); err != nil {
		return err
	}
	p.mu.Lock()
	if check && p.generation != expected {
		p.mu.Unlock()
		return typeValidation("pool generation", "opening attempt belongs to superseded sharder")
	}
	if p.closed {
		p.mu.Unlock()
		return ErrClosed
	}
	shards := max(1, c.scyllaSupported.nrShards)
	shard := c.scyllaSupported.shard
	if initial && p.count() > 0 && (shards != p.nrShards || c.scyllaSupported.msbIgnore != p.msbIgnore) {
		p.mu.Unlock()
		return typeValidation("pool generation", "late initial opening conflicts with installed sharder")
	}
	if shard < 0 || shard >= shards {
		p.mu.Unlock()
		return typeValidation("shard", "server reported invalid shard")
	}
	var retired []*Conn
	if shards != p.nrShards || c.scyllaSupported.msbIgnore != p.msbIgnore {
		for _, cs := range p.conns {
			retired = append(retired, cs...)
		}
		retired = append(retired, p.excess...)
		p.conns = make([][]*Conn, shards)
		p.pending = make([]int, shards)
		p.excess = nil
		p.nrShards = shards
		p.msbIgnore = c.scyllaSupported.msbIgnore
		p.generation++
	}
	if c.requestedShard.Set && uint32(shard) != c.requestedShard.Value {
		p.fallbackUntil = time.Now().Add(scyllaShardAwarePortFallbackDuration)
		p.retired = append(p.retired, retired...)
		p.mu.Unlock()
		return typeValidation("shard target", "source-port/NAT mapping assigned shard %d instead of %d; ordinary-port fallback enabled", shard, c.requestedShard.Value)
	}
	if p.options.ConnectionsPerHost > 0 && p.count() >= p.target() {
		p.retired = append(p.retired, retired...)
		p.mu.Unlock()
		return ErrNoStreams
	}
	if p.options.ConnectionsPerHost > 0 || len(p.conns[shard]) < p.options.ConnectionsPerShard {
		p.conns[shard] = append(p.conns[shard], c)
	} else if p.excessForShard(shard) < p.options.MaxExcessPerShard && p.count() < p.target() {
		p.excess = append(p.excess, c)
	} else {
		p.retired = append(p.retired, retired...)
		p.mu.Unlock()
		return ErrNoStreams
	}
	if p.count() >= p.target() {
		retired = append(retired, p.excess...)
		p.excess = nil
	}
	p.retired = append(p.retired, retired...)
	p.mu.Unlock()
	return nil
}
func (p *typedConnPicker) Remove(c *Conn) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for i, cs := range p.conns {
		for j, v := range cs {
			if v == c {
				copy(cs[j:], cs[j+1:])
				cs[len(cs)-1] = nil
				p.conns[i] = cs[:len(cs)-1]
				return
			}
		}
	}
	for i, v := range p.excess {
		if v == c {
			copy(p.excess[i:], p.excess[i+1:])
			p.excess[len(p.excess)-1] = nil
			p.excess = p.excess[:len(p.excess)-1]
			return
		}
	}
}
func (p *typedConnPicker) Close() {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return
	}
	p.closed = true
	var all []*Conn
	for _, cs := range p.conns {
		all = append(all, cs...)
	}
	all = append(all, p.excess...)
	all = append(all, p.retired...)
	p.retired = nil
	p.conns = nil
	p.excess = nil
	p.mu.Unlock()
	closeConns(all...)
}
func (p *typedConnPicker) shardOf(token int64) int {
	z := uint64(token+math.MinInt64) << p.msbIgnore
	sum := ((z & 0xffffffff) * uint64(p.nrShards) >> 32) + (z>>32)*uint64(p.nrShards)
	return int(sum >> 32)
}
func (p *typedConnPicker) takeRetired() []*Conn {
	p.mu.Lock()
	defer p.mu.Unlock()
	v := p.retired
	p.retired = nil
	return v
}

func (p *typedConnPicker) Pick(t Token) *Conn {
	if raw, ok := t.(int64Token); ok {
		return p.PickInt64(int64(raw))
	}
	return p.pickTarget(Target{}, RoutingInfo{})
}
func (p *typedConnPicker) PickInt64(t int64) *Conn {
	return p.pickTarget(Target{}, RoutingInfo{Token: Option[int64]{Value: t, Set: true}})
}
func (p *typedConnPicker) pickTarget(target Target, routing RoutingInfo) *Conn {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.closed {
		return nil
	}
	shard := -1
	requested := target.ShardID
	if routing.ShardID.Set {
		requested = routing.ShardID
	}
	if requested.Set {
		if uint64(requested.Value) < uint64(len(p.conns)) {
			shard = int(requested.Value)
		} else if target.StrictShard || routing.ShardID.Set || routing.Conditional {
			return nil
		}
	} else if routing.Token.Set {
		shard = p.shardOf(routing.Token.Value)
	}
	choose := func(cs []*Conn) *Conn {
		var best, waiting *Conn
		available := 0
		if len(cs) == 0 {
			return nil
		}
		start := int(p.pos.Add(1) % uint64(len(cs)))
		for i := range cs {
			c := cs[(start+i)%len(cs)]
			if n := c.AvailableStreams(); n > available {
				best, available = c, n
			} else if waiting == nil && c.exclusiveFlag.Load() && c.streams.Available() > 0 {
				waiting = c
			}
		}
		if best == nil {
			return waiting
		}
		return best
	}
	var preferred *Conn
	if shard >= 0 && shard < len(p.conns) {
		preferred = choose(p.conns[shard])
		if preferred != nil && preferred.AvailableStreams() > 0 {
			return preferred
		}
	}
	if (target.StrictShard || routing.ShardID.Set || routing.Conditional) && shard >= 0 {
		return preferred
	}

	var best *Conn
	available := 0
	for _, cs := range p.conns {
		if c := choose(cs); c != nil {
			if n := c.AvailableStreams(); n > available {
				best, available = c, n
			} else if best == nil {
				best = c
			}
		}
	}
	if available == 0 && preferred != nil {
		return preferred
	}
	return best
}

// Pool reservation retains an existing configured slot. It never opens an
// unbounded side connection when every slot is busy or reserved.
func (pool *hostConnPool) signalReservationLocked() {
	if pool.reservationChanged != nil {
		close(pool.reservationChanged)
		pool.reservationChanged = nil
	}
}
func (pool *hostConnPool) signalReservation() {
	pool.mu.Lock()
	pool.signalReservationLocked()
	pool.mu.Unlock()
}
func (pool *hostConnPool) reserveStream(ctx context.Context, target Target, routing RoutingInfo) (*exclusiveConn, error) {
	return pool.reserveStreamWithLimits(ctx, target, routing, StreamLimits{})
}
func (pool *hostConnPool) reserveStreamWithLimits(ctx context.Context, target Target, routing RoutingInfo, limits StreamLimits) (*exclusiveConn, error) {
	if ctx == nil {
		return nil, typeValidation("stream context", "nil context")
	}
	if (!target.HostID.IsEmpty() && target.HostID != pool.host.hostUUID()) || routing.HostID.Set && routing.HostID.Value != pool.host.hostUUID() {
		return nil, ErrNoConnections
	}
	for {
		pool.mu.RLock()
		if pool.closed {
			pool.mu.RUnlock()
			return nil, ErrClosed
		}
		var candidates []*Conn
		if p, ok := pool.connPicker.(*typedConnPicker); ok {
			p.mu.RLock()
			shard := -1
			strict := target.StrictShard || routing.ShardID.Set || routing.Conditional
			requested := target.ShardID
			if routing.ShardID.Set {
				requested = routing.ShardID
			}
			if requested.Set {
				if uint64(requested.Value) < uint64(len(p.conns)) {
					shard = int(requested.Value)
				} else if strict {
					p.mu.RUnlock()
					pool.mu.RUnlock()
					return nil, ErrNoConnections
				}
			} else if routing.Token.Set {
				shard = p.shardOf(routing.Token.Value)
			}
			if strict && shard >= len(p.conns) {
				p.mu.RUnlock()
				pool.mu.RUnlock()
				return nil, ErrNoConnections
			}
			for i, cs := range p.conns {
				if strict && shard >= 0 && i != shard {
					continue
				}
				if i == shard {
					candidates = append(append([]*Conn(nil), cs...), candidates...)
				} else {
					candidates = append(candidates, cs...)
				}
			}
			p.mu.RUnlock()
		} else if p, ok := pool.connPicker.(*defaultConnPicker); ok {
			p.mu.RLock()
			shard := target.ShardID
			if routing.ShardID.Set {
				shard = routing.ShardID
			}
			strict := target.StrictShard || routing.ShardID.Set
			for _, c := range p.conns {
				if shard.Set && strict && uint32(c.scyllaSupported.shard) != shard.Value {
					continue
				}
				candidates = append(candidates, c)
			}
			p.mu.RUnlock()
		}
		_, missing := pool.connPicker.Size()
		repair := missing > 0 && !pool.retryStopped
		pool.mu.RUnlock()
		if repair {
			pool.scheduleTypedRepair()
		}
		var capability error
		// Prefer an idle connection before closing admission on a busy candidate.
		for pass := 0; pass < 2; pass++ {
			for _, c := range candidates {
				if c.exclusiveFlag.Load() {
					continue
				}
				c.mu.Lock()
				idle := c.sharedOps == 0 && len(c.calls) == 0 && c.streams.InUse() == 0
				c.mu.Unlock()
				if pass == 0 && !idle || pass == 1 && idle {
					continue
				}
				r, err := c.reserveExclusiveWithLimits(ctx, limits)
				if err == nil {
					return r, nil
				}
				if ctx.Err() != nil {
					return nil, ctx.Err()
				}
				if _, ok := err.(*StreamCapabilityError); ok {
					capability = err
				}
			}
		}
		if capability != nil && len(candidates) > 0 {
			return nil, capability
		}
		pool.mu.Lock()
		if pool.closed {
			pool.mu.Unlock()
			return nil, ErrClosed
		}
		if pool.reservationChanged == nil {
			pool.reservationChanged = make(chan struct{})
		}
		changed := pool.reservationChanged
		// Close the notification race if a reservation ended before registration.
		available := false
		for _, c := range candidates {
			if !c.exclusiveFlag.Load() && !c.Closed() {
				available = true
				break
			}
		}
		pool.mu.Unlock()
		if available {
			continue
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-pool.repairCtx.Done():
			return nil, ErrClosed
		case <-changed:
		}
	}
}

func (pool *hostConnPool) transportState() poolState {
	pool.mu.RLock()
	defer pool.mu.RUnlock()
	return pool.state
}

var _ ConnPicker = (*typedConnPicker)(nil)

func (p *typedConnPicker) excessForShard(shard int) int {
	n := 0
	for _, c := range p.excess {
		if c.scyllaSupported.shard == shard {
			n++
		}
	}
	return n
}
func (p *typedConnPicker) openingGeneration() uint64 {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.generation
}

func (pool *hostConnPool) scheduleTypedRepair() {
	config, ok := pool.session.v2PoolConfig()
	if !ok {
		return
	}
	pool.mu.Lock()
	if pool.closed || pool.filling || pool.repairScheduled || pool.retryStopped {
		pool.mu.Unlock()
		return
	}
	pool.repairScheduled = true
	attempt := pool.reconnectAttempt
	pool.reconnectAttempt++
	pool.mu.Unlock()
	policy := config.Reconnect
	if policy == nil {
		pool.mu.Lock()
		pool.repairScheduled = false
		pool.retryStopped = true
		pool.mu.Unlock()
		return
	}
	// User policy calls run outside all pool/picker locks.
	delay, again := policy.Delay(attempt)
	if delay < 0 {
		again = false
		pool.logger.Printf("gocql: reconnect policy returned negative delay")
	}
	pool.mu.Lock()
	if !again {
		pool.repairScheduled = false
		pool.retryStopped = true
		pool.mu.Unlock()
		return
	}
	closed := pool.closed
	pool.mu.Unlock()
	if closed {
		return
	}
	go func() {
		timer := time.NewTimer(max(delay, time.Millisecond))
		defer timer.Stop()
		select {
		case <-pool.repairCtx.Done():
			return
		case <-timer.C:
		}
		pool.mu.Lock()
		pool.repairScheduled = false
		closed := pool.closed
		pool.mu.Unlock()
		if !closed {
			pool.fillNow()
		}
	}()
}

func (pool *hostConnPool) requestTypedRepair() {
	if _, typed := pool.session.v2PoolConfig(); !typed {
		return
	}
	pool.mu.RLock()
	_, missing := pool.connPicker.Size()
	needed := !pool.closed && !pool.retryStopped && missing > 0
	pool.mu.RUnlock()
	if needed {
		pool.scheduleTypedRepair()
	}
}
