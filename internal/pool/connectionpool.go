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
/*
 * Content before git sha 34fdeebefcbf183ed7f916f931aa0586fdaa1b40
 * Copyright (c) 2012, The Gocql authors,
 * provided under the BSD-3-Clause License.
 * See the NOTICE file distributed with this work for additional information.
 */

package pool

import (
	"context"
	"fmt"
	"math/rand"
	"net"
	"sync"
	"time"

	"github.com/scylladb/gocql/v2/internal/debug"
	"github.com/scylladb/gocql/v2/tablets"

	"github.com/scylladb/gocql/v2/debounce"
	implEndpoint "github.com/scylladb/gocql/v2/internal/endpoint"
	// interface to implement to receive the host information
)

type SetHosts interface {
	SetHosts(hosts []*HostInfo)
}

// interface to implement to receive the partitioner value
type SetPartitioner interface {
	SetPartitioner(partitioner string)
}

// interface to implement to receive the tablets value
type SetTablets interface {
	SetTablets(tablets tablets.TabletInfoList)
}

type InternalPolicyConnPool struct {
	InternalServices      InternalPoolServices
	InternalHostConnPools map[UUID]*InternalHostConnPool
	InternalKeyspace      string
	InternalPort          int
	InternalNumConns      int
	InternalMu            sync.RWMutex
}

func (p *InternalPolicyConnPool) SetHosts(hosts []*HostInfo) {
	p.InternalMu.Lock()
	defer p.InternalMu.Unlock()

	toRemove := make(map[UUID]struct{})
	for hostID := range p.InternalHostConnPools {
		toRemove[hostID] = struct{}{}
	}

	pools := make(chan *InternalHostConnPool)
	createCount := 0
	for _, host := range hosts {
		if !host.IsUp() {
			// don't create a connection pool for a down host
			continue
		}
		hostID := implEndpoint.InternalHostInfoHostUUID(host)
		if _, exists := p.InternalHostConnPools[hostID]; exists {
			// still have this host, so don't remove it
			delete(toRemove, hostID)
			continue
		}

		createCount++
		go func(host *HostInfo) {
			// create a connection pool for the host
			pools <- InternalNewHostPool(
				p.InternalServices,
				host,
				p.InternalNumConns,
				p.InternalKeyspace,
			)
		}(host)
	}

	// add created pools
	for createCount > 0 {
		pool := <-pools
		createCount--
		if pool.Size() > 0 {
			// add pool only if there a connections available
			p.InternalHostConnPools[implEndpoint.InternalHostInfoHostUUID(pool.InternalHost)] = pool
		}
	}

	for addr := range toRemove {
		pool := p.InternalHostConnPools[addr]
		delete(p.InternalHostConnPools, addr)
		go pool.Close()
	}
}

func (p *InternalPolicyConnPool) InFlight() int {
	p.InternalMu.RLock()
	count := 0
	for _, pool := range p.InternalHostConnPools {
		count += pool.InFlight()
	}
	p.InternalMu.RUnlock()

	return count
}

func (p *InternalPolicyConnPool) Size() int {
	p.InternalMu.RLock()
	count := 0
	for _, pool := range p.InternalHostConnPools {
		count += pool.Size()
	}
	p.InternalMu.RUnlock()

	return count
}

func (p *InternalPolicyConnPool) InternalGetPool(host *HostInfo) (pool *InternalHostConnPool, ok bool) {
	hostID := implEndpoint.InternalHostInfoHostUUID(host)
	p.InternalMu.RLock()
	pool, ok = p.InternalHostConnPools[hostID]
	p.InternalMu.RUnlock()
	return
}

// Rare path (SetHostID/GetHostPoolByID); getPool is the hot path and skips this parse.
func (p *InternalPolicyConnPool) InternalGetPoolByHostID(hostID string) (pool *InternalHostConnPool, ok bool) {
	id, err := ParseUUID(hostID)
	if err != nil {
		return nil, false
	}
	p.InternalMu.RLock()
	pool, ok = p.InternalHostConnPools[id]
	p.InternalMu.RUnlock()
	return
}

// iteratePool runs iter for each host pool, stopping early if it returns false.
//
// The unlock is deferred because iter is caller-supplied: a panic, or a
// runtime.Goexit from something like t.Fatal, would otherwise skip a bare
// RUnlock and leave the read lock held forever, wedging every later addHost,
// removeHost and Close on the pool. The other bare RUnlocks in this file only
// guard reads of internal state and cannot panic.
func (p *InternalPolicyConnPool) InternalIteratePool(iter func(info HostPoolInfo) bool) {
	p.InternalMu.RLock()
	defer p.InternalMu.RUnlock()

	for _, pool := range p.InternalHostConnPools {
		if !iter(pool) {
			break
		}
	}
}

func (p *InternalPolicyConnPool) Close() {
	p.InternalMu.Lock()
	pools := make([]*InternalHostConnPool, 0, len(p.InternalHostConnPools))
	for id, pool := range p.InternalHostConnPools {
		pools = append(pools, pool)
		delete(p.InternalHostConnPools, id)
	}
	p.InternalMu.Unlock()
	for _, pool := range pools {
		pool.Close()
	}
}

func (p *InternalPolicyConnPool) InternalEnsureHost(host *HostInfo) *InternalHostConnPool {
	hostID := implEndpoint.InternalHostInfoHostUUID(host)
	p.InternalMu.Lock()
	pool, ok := p.InternalHostConnPools[hostID]
	if !ok {
		pool = InternalNewHostPool(
			p.InternalServices,
			host,
			p.InternalNumConns,
			p.InternalKeyspace,
		)

		p.InternalHostConnPools[hostID] = pool
	}
	p.InternalMu.Unlock()

	return pool
}
func (p *InternalPolicyConnPool) InternalAddHost(host *HostInfo) {
	p.InternalEnsureHost(host).InternalFill_debounce()
}

func (p *InternalPolicyConnPool) InternalRemoveHost(hostID UUID) {
	p.InternalMu.Lock()
	pool, ok := p.InternalHostConnPools[hostID]
	if !ok {
		p.InternalMu.Unlock()
		return
	}

	delete(p.InternalHostConnPools, hostID)
	p.InternalMu.Unlock()

	go pool.Close()
}

// hostConnPool is a connection pool for a single host.
// Connection selection is based on a provided ConnSelectionPolicy
type InternalHostConnPool struct {
	InternalConnPicker ConnPicker
	InternalLogger     StdLogger
	InternalServices   InternalPoolServices
	InternalHost       *HostInfo
	InternalDebouncer  *debounce.SimpleDebouncer
	InternalKeyspace   string
	InternalSize       int
	// protection for connPicker, closed, filling
	InternalMu                            sync.RWMutex
	InternalClosed                        bool
	InternalFilling                       bool
	InternalState                         InternalPoolState
	generation                            uint64
	reconnectAttempt                      int
	InternalStarted                       bool
	reservationChanged                    chan struct{}
	InternalRepairScheduled, retryStopped bool
	InternalRepairCtx                     context.Context
	InternalRepairCancel                  context.CancelFunc
}

func (pool *InternalHostConnPool) String() string {
	pool.InternalMu.RLock()
	defer pool.InternalMu.RUnlock()
	size, _ := pool.InternalConnPicker.Size()
	return fmt.Sprintf("[filling=%v closed=%v conns=%v size=%v host=%v]",
		pool.InternalFilling, pool.InternalClosed, size, pool.InternalSize, pool.InternalHost)
}

func InternalNewHostPool(services InternalPoolServices, host *HostInfo, size int, keyspace string) *InternalHostConnPool {
	baseCtx := services.Context
	if baseCtx == nil {
		baseCtx = context.Background()
	}
	repairCtx, repairCancel := context.WithCancel(baseCtx)
	pool := &InternalHostConnPool{
		InternalRepairCtx: repairCtx, InternalRepairCancel: repairCancel,
		InternalServices:   services,
		InternalHost:       host,
		InternalSize:       size,
		InternalKeyspace:   keyspace,
		InternalConnPicker: InternalNopConnPicker{},
		InternalFilling:    false,
		InternalClosed:     false,
		InternalLogger:     services.Logger,
		InternalDebouncer:  debounce.NewSimpleDebouncer(),
	}

	// the pool is not filled or connected
	return pool
}

// Pick a connection from this connection pool for the given query.
func (pool *InternalHostConnPool) Pick(token implEndpoint.Token) *Conn {
	defer pool.InternalRequestTypedRepair()
	pool.InternalMu.RLock()
	defer pool.InternalMu.RUnlock()

	if !pool.InternalPickable() {
		return nil
	}

	return pool.InternalConnPicker.Pick(token)
}

// PickInt64 picks a connection for the given raw int64 routing token, avoiding
// the Token interface boxing. Picks fall back to the boxed path when the
// connPicker does not support raw-int64 picking.
func (pool *InternalHostConnPool) PickInt64(token int64) *Conn {
	defer pool.InternalRequestTypedRepair()
	pool.InternalMu.RLock()
	defer pool.InternalMu.RUnlock()

	if !pool.InternalPickable() {
		return nil
	}

	if p, ok := pool.InternalConnPicker.(InternalInt64ConnPicker); ok {
		return p.PickInt64(token)
	}
	return pool.InternalConnPicker.Pick(implEndpoint.InternalInt64Token(token))
}

// PickConn routes a picked host to the raw-int64 fast path when it exposes one,
// and to the boxed Token path otherwise.
func (pool *InternalHostConnPool) PickConn(host implEndpoint.SelectedHost) *Conn {
	if th, ok := host.(implEndpoint.InternalInt64TokenSelectedHost); ok {
		if token, has := th.TokenInt64(); has {
			return pool.PickInt64(int64(token))
		}
	}
	return pool.Pick(host.Token())
}

// pickable reports whether the pool is open and has (or is filling) connections.
// Must be called with pool.mu held.
func (pool *InternalHostConnPool) InternalPickable() bool {
	if pool.InternalClosed {
		return false
	}

	size, missing := pool.InternalConnPicker.Size()
	if missing > 0 && !pool.retryStopped {
		// Legacy callers retain their existing debounce path. Typed runtime repair
		// is requested after releasing pool.mu and always uses ReconnectPolicy.
		if _, typed := pool.typedConfig(); !typed {
			go pool.InternalFill_debounce()
		}

		if size == 0 {
			return false
		}
	}

	return true
}

// Size returns the number of connections currently active in the pool
func (pool *InternalHostConnPool) Size() int {
	pool.InternalMu.RLock()
	defer pool.InternalMu.RUnlock()

	size, _ := pool.InternalConnPicker.Size()
	return size
}

// Size returns the number of connections currently active in the pool
func (pool *InternalHostConnPool) InFlight() int {
	pool.InternalMu.RLock()
	defer pool.InternalMu.RUnlock()

	size := pool.InternalConnPicker.InFlight()
	return size
}

// Close the connection pool
func (pool *InternalHostConnPool) Close() {
	pool.InternalMu.Lock()
	if pool.InternalClosed {
		pool.InternalMu.Unlock()
		return
	}
	pool.InternalClosed = true
	pool.InternalState = InternalPoolClosed
	pool.generation++
	pool.signalReservationLocked()
	if pool.InternalRepairCancel != nil {
		pool.InternalRepairCancel()
	}
	pool.InternalMu.Unlock()

	pool.InternalConnPicker.Close()
}

// Fill the connection pool
func (pool *InternalHostConnPool) InternalFill() {
	if _, typed := pool.typedConfig(); !typed {
		pool.fillNow()
		return
	}
	pool.InternalMu.Lock()
	initial := !pool.InternalStarted && !pool.InternalClosed
	if initial {
		pool.InternalStarted = true
	}
	pool.InternalMu.Unlock()
	if initial {
		pool.fillNow()
	} else {
		pool.InternalScheduleTypedRepair()
	}
}
func (pool *InternalHostConnPool) fillNow() {
	pool.InternalMu.RLock()
	// avoid filling a closed pool, or concurrent filling
	if pool.InternalClosed || pool.InternalFilling {
		pool.InternalMu.RUnlock()
		return
	}

	// determine the filling work to be done
	startCount, fillCount := pool.InternalConnPicker.Size()

	// avoid filling a full (or overfull) pool
	if fillCount <= 0 {
		pool.InternalMu.RUnlock()
		return
	}

	// switch from read to write lock
	pool.InternalMu.RUnlock()
	pool.InternalMu.Lock()

	startCount, fillCount = pool.InternalConnPicker.Size()
	if pool.InternalClosed || pool.InternalFilling || fillCount <= 0 {
		// looks like another goroutine already beat this
		// goroutine to the filling
		pool.InternalMu.Unlock()
		return
	}

	// ok fill the pool
	pool.InternalFilling = true
	if startCount == 0 {
		pool.InternalState = InternalPoolInitializing
	}

	// allow others to access the pool while filling
	pool.InternalMu.Unlock()
	// only this goroutine should make calls to fill/empty the pool at this
	// point until after this routine or its subordinates calls
	// fillingStopped

	// fill only the first connection synchronously
	if startCount == 0 {
		err := pool.InternalConnect()
		pool.logConnectErr(err)

		if err != nil {
			// probably unreachable host
			pool.fillingStopped(err)
			return
		}
		// notify the session that this node is connected
		pool.InternalServices.Worker(func() { pool.InternalServices.Connected(pool) })

		// filled one, let's reload it to see if it has changed
		pool.InternalMu.RLock()
		_, fillCount = pool.InternalConnPicker.Size()
		pool.InternalMu.RUnlock()
	}

	// fill the rest of the pool asynchronously
	go func() {
		err := pool.InternalConnectMany(fillCount)

		// mark the end of filling
		pool.fillingStopped(err)

		if err == nil && startCount > 0 {
			// notify the session that this node is connected again
			pool.InternalServices.Worker(func() { pool.InternalServices.Connected(pool) })
		}
	}()
}

func (pool *InternalHostConnPool) InternalFill_debounce() {
	pool.InternalDebouncer.Debounce(pool.InternalFill)
}

func (pool *InternalHostConnPool) logConnectErr(err error) {
	if opErr, ok := err.(*net.OpError); ok && (opErr.Op == "dial" || opErr.Op == "read") {
		// connection refused
		// these are typical during a node outage so avoid log spam.
		if debug.Enabled {
			pool.InternalLogger.Printf("unable to dial %q: %v\n", pool.InternalHost, err)
		}
	} else if err != nil {
		// unexpected error
		pool.InternalLogger.Printf("error: failed to connect to %q due to error: %v", pool.InternalHost, err)
	}
}

// transition back to a not-filling state.
func (pool *InternalHostConnPool) fillingStopped(err error) {
	if err != nil {
		if debug.Enabled {
			pool.InternalLogger.Printf("gocql: filling stopped %q: %v\n", pool.InternalHost.ConnectAddress(), err)
		}
		// wait for some time to avoid back-to-back filling
		// this provides some time between failed attempts
		// to fill the pool for the host to recover
		if _, typed := pool.typedConfig(); !typed {
			time.Sleep(time.Duration(rand.Int31n(100)+31) * time.Millisecond)
		}
	}

	pool.InternalMu.Lock()
	pool.InternalFilling = false
	count, missing := pool.InternalConnPicker.Size()
	if !pool.InternalClosed {
		if count > 0 {
			pool.InternalState = InternalPoolReady
			if err == nil && missing == 0 {
				pool.reconnectAttempt = 0
				pool.retryStopped = false
			}
		} else if err != nil {
			pool.InternalState = InternalPoolBroken
		}
	}
	host := pool.InternalHost
	port := pool.InternalHost.Port()
	pool.InternalMu.Unlock()
	if missing > 0 {
		pool.InternalScheduleTypedRepair()
	}

	// if we errored and the size is now zero, make sure the host is marked as down
	// see https://github.com/apache/cassandra-gocql-driver/issues/1614
	if debug.Enabled {
		pool.InternalLogger.Printf("gocql: conns of pool after stopped %q: %v\n", host.ConnectAddress(), count)
	}
	if err != nil && count == 0 {
		if pool.InternalServices.Conviction.AddFailure(err, host) {
			pool.InternalServices.Down(host.ConnectAddress(), port)
		}
	}
}

// connectMany creates new connections concurrent.
func (pool *InternalHostConnPool) InternalConnectMany(count int) error {
	if count <= 0 {
		return nil
	}
	workers := min(count, 32)
	var wg sync.WaitGroup
	var mu sync.Mutex
	var connectErr error
	next := 0
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go func() {
			defer wg.Done()
			for {
				mu.Lock()
				if connectErr != nil || next >= count {
					mu.Unlock()
					return
				}
				next++
				mu.Unlock()
				if pool.InternalRepairCtx.Err() != nil {
					return
				}
				err := pool.InternalConnect()
				pool.logConnectErr(err)
				if err != nil {
					mu.Lock()
					if connectErr == nil {
						connectErr = err
					}
					mu.Unlock()
					return
				}
			}
		}()
	}
	wg.Wait()
	return connectErr
}

// create a new connection to the host and add it to the pool
func (pool *InternalHostConnPool) InternalConnect() (err error) {
	pool.InternalMu.Lock()
	picker := pool.InternalConnPicker
	shardID, nrShards := picker.NextShard()
	generation := pool.generation
	var pickerGeneration uint64
	if typed, ok := picker.(*InternalTypedConnPicker); ok {
		pickerGeneration = typed.InternalOpeningGeneration()
	}
	pool.InternalMu.Unlock()
	if typed, ok := picker.(*InternalTypedConnPicker); ok {
		defer typed.InternalDialFinished(shardID, nrShards)
	}

	// TODO: provide a more robust connection retry mechanism, we should also
	// be able to detect hosts that come up by trying to connect to downed ones.
	// try to connect
	var conn *Conn
	reconnectionPolicy := pool.InternalServices.Reconnection
	for i := 0; i < reconnectionPolicy.GetMaxRetries(); i++ {
		conn, err = pool.InternalServices.Dial(pool.InternalRepairCtx, pool.InternalHost, pool, shardID, nrShards)
		if err == nil {
			break
		}
		if opErr, isOpErr := err.(*net.OpError); isOpErr {
			// if the error is not a temporary error (ex: network unreachable) don't
			//  retry
			if !opErr.Temporary() {
				break
			}
		}
		if debug.Enabled {
			pool.InternalLogger.Printf("gocql: connection failed %q: %v, reconnecting with %T\n",
				pool.InternalHost.ConnectAddress(), err, reconnectionPolicy)
		}
		time.Sleep(reconnectionPolicy.GetInterval(i))
	}

	if err != nil {
		return err
	}
	// Validate rejects a non-positive limit, but a custom policy can still
	// return one here, which leaves conn nil without an error.
	if conn == nil {
		return fmt.Errorf("gocql: no connection attempted, %T.GetMaxRetries is not positive",
			reconnectionPolicy)
	}

	openingCtx, openingCancel := conn.InternalOpeningContext(pool.InternalRepairCtx)
	defer openingCancel()
	stopAndJoin := conn.InternalSetupCancellation(openingCtx)
	defer stopAndJoin()
	if pool.InternalKeyspace != "" {
		if err = conn.InternalUseKeyspaceContext(openingCtx, pool.InternalKeyspace); err != nil {
			conn.Close()
			return err
		}
	}
	if err = conn.InternalValidateOpeningTarget(openingCtx); err != nil {
		conn.Close()
		return err
	}
	// A setup cancellation callback cannot survive publication and later close a
	// connection after it has begun serving unrelated requests.
	stopAndJoin()
	if err = openingCtx.Err(); err != nil {
		conn.Close()
		return err
	}

	// add the Conn to the pool
	pool.InternalMu.Lock()
	if pool.InternalClosed || generation != pool.generation || openingCtx.Err() != nil {
		pool.InternalMu.Unlock()
		conn.Close()
		return nil
	}

	typed := conn.InternalCfg != nil && conn.InternalCfg.TypedConfig != nil
	if typed {
		implEndpoint.HostInfoData(pool.InternalHost).InternalMu.Lock()
		if conn.InternalTargetEpoch != implEndpoint.HostInfoData(pool.InternalHost).InternalTransportGeneration || conn.InternalOpeningSharderEpoch != implEndpoint.HostInfoData(pool.InternalHost).InternalSharderGeneration && (implEndpoint.ScyllaHostFeaturesData(&conn.InternalScyllaSupported.ScyllaHostFeatures).InternalNrShards != implEndpoint.ScyllaHostFeaturesData(&implEndpoint.HostInfoData(pool.InternalHost).InternalScyllaFeatures).InternalNrShards || implEndpoint.ScyllaHostFeaturesData(&conn.InternalScyllaSupported.ScyllaHostFeatures).InternalMsbIgnore != implEndpoint.ScyllaHostFeaturesData(&implEndpoint.HostInfoData(pool.InternalHost).InternalScyllaFeatures).InternalMsbIgnore) {
			implEndpoint.HostInfoData(pool.InternalHost).InternalMu.Unlock()
			pool.InternalMu.Unlock()
			conn.Close()
			return typeValidation("pool generation", "opening attempt belongs to superseded endpoint/route")
		}
	}
	// lazily initialize the connPicker when we know the required type
	if err := pool.InternalInitConnPicker(conn); err != nil {
		if typed {
			implEndpoint.HostInfoData(pool.InternalHost).InternalMu.Unlock()
		}
		pool.InternalMu.Unlock()
		conn.Close()
		return err
	}
	put := func() error {
		if p, ok := pool.InternalConnPicker.(*InternalTypedConnPicker); ok {
			_, oldTyped := picker.(*InternalTypedConnPicker)
			if !oldTyped {
				return p.InternalPutInitial(conn)
			}
			return p.InternalPut(conn, pickerGeneration, true)
		}
		return pool.InternalConnPicker.Put(conn)
	}
	if err := put(); err != nil {
		if typed {
			implEndpoint.HostInfoData(pool.InternalHost).InternalMu.Unlock()
		}
		pool.InternalMu.Unlock()
		if p, ok := pool.InternalConnPicker.(*InternalTypedConnPicker); ok {
			InternalCloseConns(p.takeRetired()...)
		}
		conn.Close()
		if debug.Enabled {
			pool.InternalLogger.Printf("gocql: pool connection was not added to the pool: %w", err)
		}
		return nil
	}
	if typed {
		if implEndpoint.HostInfoData(pool.InternalHost).InternalScyllaFeatures != conn.InternalScyllaSupported.ScyllaHostFeatures {
			implEndpoint.InternalHostInfoSetScyllaFeaturesLocked(pool.InternalHost, conn.InternalScyllaSupported.ScyllaHostFeatures)
		}
		implEndpoint.HostInfoData(pool.InternalHost).InternalMu.Unlock()
	}
	pool.InternalState = InternalPoolReady
	pool.signalReservationLocked()
	pool.InternalMu.Unlock()
	if p, ok := pool.InternalConnPicker.(*InternalTypedConnPicker); ok {
		InternalCloseConns(p.takeRetired()...)
	}
	conn.InternalFinalizeConnection()

	return nil
}

func (pool *InternalHostConnPool) InternalInitConnPicker(conn *Conn) error {
	if _, ok := pool.InternalConnPicker.(InternalNopConnPicker); !ok {
		return nil
	}
	if cfg, ok := pool.typedConfig(); ok {
		picker, err := InternalNewTypedConnPicker(conn, cfg.Pool)
		if err != nil {
			return err
		}
		pool.InternalConnPicker = picker
		return nil
	}
	if conn.InternalIsScyllaConn() && implEndpoint.ScyllaHostFeaturesValueData(conn.InternalGetScyllaSupported().ScyllaHostFeatures).InternalNrShards != 0 {
		pool.InternalConnPicker = InternalNewScyllaConnPicker(conn, pool.InternalLogger)
		return nil
	}
	pool.InternalConnPicker = InternalNewDefaultConnPicker(pool.InternalSize)
	return nil
}

// handle any error from a Conn
func (pool *InternalHostConnPool) HandleError(conn *Conn, err error, closed bool) {
	if !closed {
		return
	}
	pool.InternalMu.Lock()
	if pool.InternalClosed {
		pool.InternalMu.Unlock()
		return
	}
	pool.InternalConnPicker.Remove(conn)
	pool.signalReservationLocked()
	if n, _ := pool.InternalConnPicker.Size(); n == 0 {
		pool.InternalState = InternalPoolBroken
	}
	pool.InternalMu.Unlock()
	if debug.Enabled {
		pool.InternalLogger.Printf("gocql: pool connection error %q: %v", conn.InternalAddr, err)
	}
	if _, typed := pool.typedConfig(); typed {
		pool.InternalScheduleTypedRepair()
	} else {
		go pool.InternalFill_debounce()
	}
}

func (pool *InternalHostConnPool) GetConnectionCount() int {
	pool.InternalMu.Lock()
	defer pool.InternalMu.Unlock()
	return pool.InternalConnPicker.GetConnectionCount()
}

func (pool *InternalHostConnPool) GetExcessConnectionCount() int {
	pool.InternalMu.Lock()
	defer pool.InternalMu.Unlock()
	return pool.InternalConnPicker.GetExcessConnectionCount()
}

func (pool *InternalHostConnPool) GetShardCount() int {
	pool.InternalMu.Lock()
	defer pool.InternalMu.Unlock()
	return pool.InternalConnPicker.GetShardCount()
}

func (pool *InternalHostConnPool) Host() implEndpoint.HostInformation {
	return pool.InternalHost
}

func (pool *InternalHostConnPool) IsClosed() bool {
	pool.InternalMu.Lock()
	defer pool.InternalMu.Unlock()
	return pool.InternalClosed
}
