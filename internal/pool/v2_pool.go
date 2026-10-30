// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

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

package pool

import (
	"context"
	implEndpoint "github.com/scylladb/gocql/v2/internal/endpoint"
	"math"
	"sync"
	"sync/atomic"
	"time"
)

type InternalPoolState uint8

const (
	InternalPoolInitializing InternalPoolState = iota
	InternalPoolReady
	InternalPoolBroken
	InternalPoolClosed
)

// typedConnPicker keeps actual shard membership separate from sizing mode.
// Positive host count is a total budget; otherwise each shard has its own
// connection target. Reservation never removes a connection from this budget.
type InternalTypedConnPicker struct {
	InternalMu       sync.RWMutex
	InternalConns    [][]*Conn
	InternalExcess   []*Conn
	retired          []*Conn
	pending          []int
	options          PoolOptions
	InternalNrShards int
	msbIgnore        uint64
	InternalPos      atomic.Uint64
	fallbackUntil    time.Time
	closed           bool
	generation       uint64
}

func InternalNewTypedConnPicker(c *Conn, opts PoolOptions) (*InternalTypedConnPicker, error) {
	if err := validateV2ConnectionSharding(c.InternalScyllaSupported); err != nil {
		return nil, err
	}
	shards := max(1, implEndpoint.ScyllaHostFeaturesData(&c.InternalScyllaSupported.ScyllaHostFeatures).InternalNrShards)
	if _, err := checkedV2PoolBudget(opts, shards); err != nil {
		return nil, err
	}
	return &InternalTypedConnPicker{InternalConns: make([][]*Conn, shards), pending: make([]int, shards), InternalNrShards: shards, msbIgnore: implEndpoint.ScyllaHostFeaturesData(&c.InternalScyllaSupported.ScyllaHostFeatures).InternalMsbIgnore, options: opts}, nil
}
func (p *InternalTypedConnPicker) InternalTarget() int {
	if p.options.ConnectionsPerHost > 0 {
		return p.options.ConnectionsPerHost
	}
	return p.InternalNrShards * p.options.ConnectionsPerShard
}
func (p *InternalTypedConnPicker) InternalCount() int {
	n := 0
	for _, cs := range p.InternalConns {
		n += len(cs)
	}
	return n
}
func (p *InternalTypedConnPicker) Size() (int, int) {
	p.InternalMu.RLock()
	defer p.InternalMu.RUnlock()
	n := p.InternalCount()
	return n, max(0, p.InternalTarget()-n)
}
func (p *InternalTypedConnPicker) GetConnectionCount() int { n, _ := p.Size(); return n }
func (p *InternalTypedConnPicker) GetExcessConnectionCount() int {
	p.InternalMu.RLock()
	defer p.InternalMu.RUnlock()
	return len(p.InternalExcess)
}
func (p *InternalTypedConnPicker) GetShardCount() int {
	p.InternalMu.RLock()
	defer p.InternalMu.RUnlock()
	return p.InternalNrShards
}
func (p *InternalTypedConnPicker) InFlight() int {
	p.InternalMu.RLock()
	defer p.InternalMu.RUnlock()
	n := 0
	for _, cs := range p.InternalConns {
		for _, c := range cs {
			n += c.InternalStreams.InUse()
		}
	}
	return n
}
func (p *InternalTypedConnPicker) NextShard() (int, int) {
	p.InternalMu.Lock()
	defer p.InternalMu.Unlock()
	if p.closed || p.options.ConnectionsPerHost > 0 || !p.options.ShardAwarePort || p.InternalNrShards <= 1 || time.Now().Before(p.fallbackUntil) {
		return 0, 0
	}
	shard := -1
	load := math.MaxInt
	for i, cs := range p.InternalConns {
		n := len(cs) + p.pending[i]
		if n < p.options.ConnectionsPerShard && n < load {
			shard, load = i, n
		}
	}
	if shard < 0 {
		return 0, 0
	}
	p.pending[shard]++
	return shard, p.InternalNrShards
}
func (p *InternalTypedConnPicker) InternalDialFinished(shard, shards int) {
	p.InternalMu.Lock()
	defer p.InternalMu.Unlock()
	if shards == p.InternalNrShards && shard >= 0 && shard < len(p.pending) && p.pending[shard] > 0 {
		p.pending[shard]--
	}
}
func (p *InternalTypedConnPicker) Put(c *Conn) error { return p.InternalPut(c, 0, false) }
func (p *InternalTypedConnPicker) InternalPut(c *Conn, expected uint64, check bool) error {
	return p.putMode(c, expected, check, false)
}
func (p *InternalTypedConnPicker) InternalPutInitial(c *Conn) error {
	return p.putMode(c, 0, false, true)
}
func (p *InternalTypedConnPicker) putMode(c *Conn, expected uint64, check, initial bool) error {
	if err := validateV2ConnectionSharding(c.InternalScyllaSupported); err != nil {
		return err
	}
	if _, err := checkedV2PoolBudget(p.options, max(1, implEndpoint.ScyllaHostFeaturesData(&c.InternalScyllaSupported.ScyllaHostFeatures).InternalNrShards)); err != nil {
		return err
	}
	p.InternalMu.Lock()
	if check && p.generation != expected {
		p.InternalMu.Unlock()
		return typeValidation("pool generation", "opening attempt belongs to superseded sharder")
	}
	if p.closed {
		p.InternalMu.Unlock()
		return ErrClosed
	}
	shards := max(1, implEndpoint.ScyllaHostFeaturesData(&c.InternalScyllaSupported.ScyllaHostFeatures).InternalNrShards)
	shard := implEndpoint.ScyllaConnectionFeaturesData(&c.InternalScyllaSupported).InternalShard
	if initial && p.InternalCount() > 0 && (shards != p.InternalNrShards || implEndpoint.ScyllaHostFeaturesData(&c.InternalScyllaSupported.ScyllaHostFeatures).InternalMsbIgnore != p.msbIgnore) {
		p.InternalMu.Unlock()
		return typeValidation("pool generation", "late initial opening conflicts with installed sharder")
	}
	if shard < 0 || shard >= shards {
		p.InternalMu.Unlock()
		return typeValidation("shard", "server reported invalid shard")
	}
	var retired []*Conn
	if shards != p.InternalNrShards || implEndpoint.ScyllaHostFeaturesData(&c.InternalScyllaSupported.ScyllaHostFeatures).InternalMsbIgnore != p.msbIgnore {
		for _, cs := range p.InternalConns {
			retired = append(retired, cs...)
		}
		retired = append(retired, p.InternalExcess...)
		p.InternalConns = make([][]*Conn, shards)
		p.pending = make([]int, shards)
		p.InternalExcess = nil
		p.InternalNrShards = shards
		p.msbIgnore = implEndpoint.ScyllaHostFeaturesData(&c.InternalScyllaSupported.ScyllaHostFeatures).InternalMsbIgnore
		p.generation++
	}
	if c.InternalRequestedShard.Set && uint32(shard) != c.InternalRequestedShard.Value {
		p.fallbackUntil = time.Now().Add(InternalScyllaShardAwarePortFallbackDuration)
		p.retired = append(p.retired, retired...)
		p.InternalMu.Unlock()
		return typeValidation("shard target", "source-port/NAT mapping assigned shard %d instead of %d; ordinary-port fallback enabled", shard, c.InternalRequestedShard.Value)
	}
	if p.options.ConnectionsPerHost > 0 && p.InternalCount() >= p.InternalTarget() {
		p.retired = append(p.retired, retired...)
		p.InternalMu.Unlock()
		return ErrNoStreams
	}
	if p.options.ConnectionsPerHost > 0 || len(p.InternalConns[shard]) < p.options.ConnectionsPerShard {
		p.InternalConns[shard] = append(p.InternalConns[shard], c)
	} else if p.excessForShard(shard) < p.options.MaxExcessPerShard && p.InternalCount() < p.InternalTarget() {
		p.InternalExcess = append(p.InternalExcess, c)
	} else {
		p.retired = append(p.retired, retired...)
		p.InternalMu.Unlock()
		return ErrNoStreams
	}
	if p.InternalCount() >= p.InternalTarget() {
		retired = append(retired, p.InternalExcess...)
		p.InternalExcess = nil
	}
	p.retired = append(p.retired, retired...)
	p.InternalMu.Unlock()
	return nil
}
func (p *InternalTypedConnPicker) Remove(c *Conn) {
	p.InternalMu.Lock()
	defer p.InternalMu.Unlock()
	for i, cs := range p.InternalConns {
		for j, v := range cs {
			if v == c {
				copy(cs[j:], cs[j+1:])
				cs[len(cs)-1] = nil
				p.InternalConns[i] = cs[:len(cs)-1]
				return
			}
		}
	}
	for i, v := range p.InternalExcess {
		if v == c {
			copy(p.InternalExcess[i:], p.InternalExcess[i+1:])
			p.InternalExcess[len(p.InternalExcess)-1] = nil
			p.InternalExcess = p.InternalExcess[:len(p.InternalExcess)-1]
			return
		}
	}
}
func (p *InternalTypedConnPicker) Close() {
	p.InternalMu.Lock()
	if p.closed {
		p.InternalMu.Unlock()
		return
	}
	p.closed = true
	var all []*Conn
	for _, cs := range p.InternalConns {
		all = append(all, cs...)
	}
	all = append(all, p.InternalExcess...)
	all = append(all, p.retired...)
	p.retired = nil
	p.InternalConns = nil
	p.InternalExcess = nil
	p.InternalMu.Unlock()
	InternalCloseConns(all...)
}
func (p *InternalTypedConnPicker) InternalShardOf(token int64) int {
	z := uint64(token+math.MinInt64) << p.msbIgnore
	sum := ((z & 0xffffffff) * uint64(p.InternalNrShards) >> 32) + (z>>32)*uint64(p.InternalNrShards)
	return int(sum >> 32)
}
func (p *InternalTypedConnPicker) takeRetired() []*Conn {
	p.InternalMu.Lock()
	defer p.InternalMu.Unlock()
	v := p.retired
	p.retired = nil
	return v
}

func (p *InternalTypedConnPicker) Pick(t implEndpoint.Token) *Conn {
	if raw, ok := t.(implEndpoint.InternalInt64Token); ok {
		return p.PickInt64(int64(raw))
	}
	return p.InternalPickTarget(Target{}, RoutingInfo{})
}
func (p *InternalTypedConnPicker) PickInt64(t int64) *Conn {
	return p.InternalPickTarget(Target{}, RoutingInfo{Token: Option[int64]{Value: t, Set: true}})
}
func (p *InternalTypedConnPicker) InternalPickTarget(target Target, routing RoutingInfo) *Conn {
	p.InternalMu.RLock()
	defer p.InternalMu.RUnlock()
	if p.closed {
		return nil
	}
	shard := -1
	requested := target.ShardID
	if routing.ShardID.Set {
		requested = routing.ShardID
	}
	if requested.Set {
		if uint64(requested.Value) < uint64(len(p.InternalConns)) {
			shard = int(requested.Value)
		} else if target.StrictShard || routing.ShardID.Set || routing.Conditional {
			return nil
		}
	} else if routing.Token.Set {
		shard = p.InternalShardOf(routing.Token.Value)
	}
	choose := func(cs []*Conn) *Conn {
		var best, waiting *Conn
		available := 0
		if len(cs) == 0 {
			return nil
		}
		start := int(p.InternalPos.Add(1) % uint64(len(cs)))
		for i := range cs {
			c := cs[(start+i)%len(cs)]
			if n := c.AvailableStreams(); n > available {
				best, available = c, n
			} else if waiting == nil && c.InternalExclusiveFlag.Load() && c.InternalStreams.Available() > 0 {
				waiting = c
			}
		}
		if best == nil {
			return waiting
		}
		return best
	}
	var preferred *Conn
	if shard >= 0 && shard < len(p.InternalConns) {
		preferred = choose(p.InternalConns[shard])
		if preferred != nil && preferred.AvailableStreams() > 0 {
			return preferred
		}
	}
	if (target.StrictShard || routing.ShardID.Set || routing.Conditional) && shard >= 0 {
		return preferred
	}

	var best *Conn
	available := 0
	for _, cs := range p.InternalConns {
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
func (pool *InternalHostConnPool) signalReservationLocked() {
	if pool.reservationChanged != nil {
		close(pool.reservationChanged)
		pool.reservationChanged = nil
	}
}
func (pool *InternalHostConnPool) signalReservation() {
	pool.InternalMu.Lock()
	pool.signalReservationLocked()
	pool.InternalMu.Unlock()
}
func (pool *InternalHostConnPool) InternalReserveStream(ctx context.Context, target Target, routing RoutingInfo) (*exclusiveConn, error) {
	return pool.InternalReserveStreamWithLimits(ctx, target, routing, StreamLimits{})
}
func (pool *InternalHostConnPool) InternalReserveStreamWithLimits(ctx context.Context, target Target, routing RoutingInfo, limits StreamLimits) (*exclusiveConn, error) {
	if ctx == nil {
		return nil, typeValidation("stream context", "nil context")
	}
	if (!target.HostID.IsEmpty() && target.HostID != implEndpoint.InternalHostInfoHostUUID(pool.InternalHost)) || routing.HostID.Set && routing.HostID.Value != implEndpoint.InternalHostInfoHostUUID(pool.InternalHost) {
		return nil, ErrNoConnections
	}
	for {
		pool.InternalMu.RLock()
		if pool.InternalClosed {
			pool.InternalMu.RUnlock()
			return nil, ErrClosed
		}
		var candidates []*Conn
		if p, ok := pool.InternalConnPicker.(*InternalTypedConnPicker); ok {
			p.InternalMu.RLock()
			shard := -1
			strict := target.StrictShard || routing.ShardID.Set || routing.Conditional
			requested := target.ShardID
			if routing.ShardID.Set {
				requested = routing.ShardID
			}
			if requested.Set {
				if uint64(requested.Value) < uint64(len(p.InternalConns)) {
					shard = int(requested.Value)
				} else if strict {
					p.InternalMu.RUnlock()
					pool.InternalMu.RUnlock()
					return nil, ErrNoConnections
				}
			} else if routing.Token.Set {
				shard = p.InternalShardOf(routing.Token.Value)
			}
			if strict && shard >= len(p.InternalConns) {
				p.InternalMu.RUnlock()
				pool.InternalMu.RUnlock()
				return nil, ErrNoConnections
			}
			for i, cs := range p.InternalConns {
				if strict && shard >= 0 && i != shard {
					continue
				}
				if i == shard {
					candidates = append(append([]*Conn(nil), cs...), candidates...)
				} else {
					candidates = append(candidates, cs...)
				}
			}
			p.InternalMu.RUnlock()
		} else if p, ok := pool.InternalConnPicker.(*InternalDefaultConnPicker); ok {
			p.InternalMu.RLock()
			shard := target.ShardID
			if routing.ShardID.Set {
				shard = routing.ShardID
			}
			strict := target.StrictShard || routing.ShardID.Set
			for _, c := range p.InternalConns {
				if shard.Set && strict && uint32(implEndpoint.ScyllaConnectionFeaturesData(&c.InternalScyllaSupported).InternalShard) != shard.Value {
					continue
				}
				candidates = append(candidates, c)
			}
			p.InternalMu.RUnlock()
		}
		_, missing := pool.InternalConnPicker.Size()
		repair := missing > 0 && !pool.retryStopped
		pool.InternalMu.RUnlock()
		if repair {
			pool.InternalScheduleTypedRepair()
		}
		var capability error
		// Prefer an idle connection before closing admission on a busy candidate.
		for pass := 0; pass < 2; pass++ {
			for _, c := range candidates {
				if c.InternalExclusiveFlag.Load() {
					continue
				}
				c.InternalMu.Lock()
				idle := c.InternalSharedOps == 0 && len(c.InternalCalls) == 0 && c.InternalStreams.InUse() == 0
				c.InternalMu.Unlock()
				if pass == 0 && !idle || pass == 1 && idle {
					continue
				}
				r, err := c.InternalReserveExclusiveWithLimits(ctx, limits)
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
		pool.InternalMu.Lock()
		if pool.InternalClosed {
			pool.InternalMu.Unlock()
			return nil, ErrClosed
		}
		if pool.reservationChanged == nil {
			pool.reservationChanged = make(chan struct{})
		}
		changed := pool.reservationChanged
		// Close the notification race if a reservation ended before registration.
		available := false
		for _, c := range candidates {
			if !c.InternalExclusiveFlag.Load() && !c.Closed() {
				available = true
				break
			}
		}
		pool.InternalMu.Unlock()
		if available {
			continue
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-pool.InternalRepairCtx.Done():
			return nil, ErrClosed
		case <-changed:
		}
	}
}

func (pool *InternalHostConnPool) InternalTransportState() InternalPoolState {
	pool.InternalMu.RLock()
	defer pool.InternalMu.RUnlock()
	return pool.InternalState
}

var _ ConnPicker = (*InternalTypedConnPicker)(nil)

func (p *InternalTypedConnPicker) excessForShard(shard int) int {
	n := 0
	for _, c := range p.InternalExcess {
		if implEndpoint.ScyllaConnectionFeaturesData(&c.InternalScyllaSupported).InternalShard == shard {
			n++
		}
	}
	return n
}
func (p *InternalTypedConnPicker) InternalOpeningGeneration() uint64 {
	p.InternalMu.RLock()
	defer p.InternalMu.RUnlock()
	return p.generation
}

func (pool *InternalHostConnPool) InternalScheduleTypedRepair() {
	config, ok := pool.typedConfig()
	if !ok {
		return
	}
	pool.InternalMu.Lock()
	if pool.InternalClosed || pool.InternalFilling || pool.InternalRepairScheduled || pool.retryStopped {
		pool.InternalMu.Unlock()
		return
	}
	pool.InternalRepairScheduled = true
	attempt := pool.reconnectAttempt
	pool.reconnectAttempt++
	pool.InternalMu.Unlock()
	policy := config.Reconnect
	if policy == nil {
		pool.InternalMu.Lock()
		pool.InternalRepairScheduled = false
		pool.retryStopped = true
		pool.InternalMu.Unlock()
		return
	}
	// User policy calls run outside all pool/picker locks.
	delay, again := policy.Delay(attempt)
	if delay < 0 {
		again = false
		pool.InternalLogger.Printf("gocql: reconnect policy returned negative delay")
	}
	pool.InternalMu.Lock()
	if !again {
		pool.InternalRepairScheduled = false
		pool.retryStopped = true
		pool.InternalMu.Unlock()
		return
	}
	closed := pool.InternalClosed
	pool.InternalMu.Unlock()
	if closed {
		return
	}
	go func() {
		timer := time.NewTimer(max(delay, time.Millisecond))
		defer timer.Stop()
		select {
		case <-pool.InternalRepairCtx.Done():
			return
		case <-timer.C:
		}
		pool.InternalMu.Lock()
		pool.InternalRepairScheduled = false
		closed := pool.InternalClosed
		pool.InternalMu.Unlock()
		if !closed {
			pool.fillNow()
		}
	}()
}

func (pool *InternalHostConnPool) InternalRequestTypedRepair() {
	if _, typed := pool.typedConfig(); !typed {
		return
	}
	pool.InternalMu.RLock()
	_, missing := pool.InternalConnPicker.Size()
	needed := !pool.InternalClosed && !pool.retryStopped && missing > 0
	pool.InternalMu.RUnlock()
	if needed {
		pool.InternalScheduleTypedRepair()
	}
}
