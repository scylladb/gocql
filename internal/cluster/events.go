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
 * Copyright (c) 2016, The Gocql authors,
 * provided under the BSD-3-Clause License.
 * See the NOTICE file distributed with this work for additional information.
 */

package cluster

import implEndpoint "github.com/scylladb/gocql/v2/internal/endpoint"

import (
	"context"
	"net"
	"sync"
	"time"

	"github.com/scylladb/gocql/v2/events"
	"github.com/scylladb/gocql/v2/internal/debug"
	frm "github.com/scylladb/gocql/v2/internal/frame"
)

type InternalEventDebouncer struct {
	InternalLogger   StdLogger
	InternalTimer    *time.Timer
	callback         func([]frame)
	quit             chan struct{}
	done             chan struct{}
	stopOnce         sync.Once
	stopped          bool
	InternalOverflow bool
	InternalResync   func()
	name             string
	InternalEvents   []frame
	releases         []func()
	InternalMu       sync.Mutex
}

func InternalNewEventDebouncer(name string, eventHandler func([]frame), logger StdLogger) *InternalEventDebouncer {
	e := &InternalEventDebouncer{
		name: name,
		quit: make(chan struct{}), done: make(chan struct{}),
		InternalTimer:  time.NewTimer(InternalEventDebounceTime),
		callback:       eventHandler,
		InternalLogger: logger,
	}
	e.InternalTimer.Stop()
	go e.flusher()

	return e
}

func (e *InternalEventDebouncer) InternalStop() {
	e.stopOnce.Do(func() {
		e.InternalMu.Lock()
		e.stopped = true
		e.InternalTimer.Stop()
		close(e.quit)
		e.InternalMu.Unlock()
	})
	<-e.done
}
func (e *InternalEventDebouncer) flusher() {
	defer close(e.done)
	defer func() {
		e.InternalMu.Lock()
		releases := e.releases
		e.releases = nil
		e.InternalEvents = nil
		e.InternalMu.Unlock()
		for _, release := range releases {
			if release != nil {
				release()
			}
		}
	}()
	for {
		select {
		case <-e.quit:
			return
		case <-e.InternalTimer.C:
			e.InternalMu.Lock()
			pending := e.InternalEvents
			releases := e.releases
			e.releases = nil
			overflow := e.InternalOverflow
			e.InternalOverflow = false
			e.InternalEvents = nil
			stopped := e.stopped
			e.InternalMu.Unlock()
			func() {
				defer func() {
					for _, release := range releases {
						if release != nil {
							release()
						}
					}
				}()
				if !stopped && len(pending) > 0 {
					e.callback(pending)
				}
			}()
			if !stopped && overflow && e.InternalResync != nil {
				e.InternalResync()
			}
		}
	}
}

const (
	InternalEventBufferSize   = 1000
	InternalEventDebounceTime = 1 * time.Second
)

// flush must be called with mu locked
func (e *InternalEventDebouncer) flush() {
	if len(e.InternalEvents) == 0 {
		return
	}

	// if the flush interval is faster than the callback then we will end up calling
	// the callback multiple times, probably a bad idea. In this case we could drop
	// frames?
	pending := e.InternalEvents
	releases := e.releases
	e.InternalEvents = nil
	e.releases = nil
	e.InternalMu.Unlock()
	func() {
		defer func() {
			for _, release := range releases {
				if release != nil {
					release()
				}
			}
		}()
		e.callback(pending)
	}()
	e.InternalMu.Lock()
}

func (e *InternalEventDebouncer) InternalDebounce(frame frame) { e.InternalDebounceOwned(frame, nil) }
func (e *InternalEventDebouncer) InternalDebounceOwned(frame frame, release func()) bool {
	return e.debounceOwnedContext(context.Background(), frame, release)
}
func (e *InternalEventDebouncer) debounceOwnedContext(ctx context.Context, frame frame, release func()) bool {
	e.InternalMu.Lock()
	if e.stopped || ctx.Err() != nil {
		e.InternalMu.Unlock()
		return false
	}
	e.InternalTimer.Reset(InternalEventDebounceTime)

	// TODO: probably need a warning to track if this threshold is too low
	if len(e.InternalEvents) < InternalEventBufferSize {
		e.InternalEvents = append(e.InternalEvents, frame)
		e.releases = append(e.releases, release)
		e.InternalMu.Unlock()
		return true
	} else {
		e.InternalOverflow = true
	}
	e.InternalMu.Unlock()
	return false
}

func InternalSessionPublishEvent(s *Session, event events.Event) {
	InternalSessionPublishV2Event(s, event)
	if s.privateStorage.InternalClientRoutesHandler != nil && s.privateStorage.InternalClientRoutesHandler.privateStorage.InternalInitialized.Load() {
		s.privateStorage.InternalClientRoutesHandler.handleRouteEvent(event)
	}
	if s.privateStorage.InternalEventBus == nil {
		return
	}

	if !s.privateStorage.InternalEventBus.PublishEvent(event) {
		s.privateStorage.InternalLogger.Printf("can't publish event, eventbus is full, increase Cluster.EventBusConfig.InputEventsQueueSize; event is dropped")
	}
}

func InternalSessionHandleEvent(s *Session, frame frame) { s.handleEventOwned(frame, nil) }
func (s *Session) handleEventOwned(frame frame, release func()) bool {
	return s.handleEventOwnedContext(context.Background(), frame, release)
}
func (s *Session) handleEventOwnedContext(ctx context.Context, frame frame, release func()) bool {
	switch frame.(type) {
	case *frm.SchemaChangeKeyspace, *frm.SchemaChangeFunction, *frm.SchemaChangeTable, *frm.SchemaChangeAggregate, *frm.SchemaChangeType:
		if s.privateStorage.InternalCfg.Events.DisableSchemaEvents {
			return false
		}
	case *frm.TopologyChangeEventFrame:
		if s.privateStorage.InternalCfg.Events.DisableTopologyEvents {
			return false
		}
	case *frm.StatusChangeEventFrame:
		if s.privateStorage.InternalCfg.Events.DisableNodeStatusEvents {
			return false
		}
	case *frm.ClientRoutesChanged:
		if s.privateStorage.InternalCfg.privateStorage.InternalDisableClientRouteEvents {
			return false
		}
	}
	if debug.Enabled {
		s.privateStorage.InternalLogger.Printf("gocql: handling frame: %v\n", frame)
	}

	source := events.FrameToEvent(frame)
	if source != nil {
		s.recordV2Event(source)
	}
	switch e := source.(type) {
	case *events.TopologyChangeEvent:
		InternalSessionRecordV2Topology(s, e)
	case *events.StatusChangeEvent:
		if e.Change == "DOWN" && !s.privateStorage.InternalCfg.Events.DisableNodeStatusEvents {
			s.fenceV2Down(e.Host, e.Port)
		}
	}
	switch f := frame.(type) {
	case *frm.SchemaChangeKeyspace, *frm.SchemaChangeFunction,
		*frm.SchemaChangeTable, *frm.SchemaChangeAggregate, *frm.SchemaChangeType:

		return s.privateStorage.InternalSchemaEvents.debounceOwnedContext(ctx, frame, release)
	case *frm.TopologyChangeEventFrame, *frm.StatusChangeEventFrame:
		return s.privateStorage.InternalNodeEvents.debounceOwnedContext(ctx, frame, release)
	case *frm.ClientRoutesChanged:
		if s.privateStorage.InternalClientRoutesHandler != nil {
			return InternalClientRoutesHandlerHandleRouteEventOwned(s.privateStorage.InternalClientRoutesHandler, source, release)
		}
	default:
		s.privateStorage.InternalLogger.Printf("gocql: invalid event frame (%T): %v\n", f, f)
	}
	return false

}

func InternalSessionHandleSchemaEvent(s *Session, frames []frame) {
	InternalSessionPublishV2Metadata(s)
	defer InternalSessionPublishV2Metadata(s)
	defer s.scheduleV2SchemaRetry()
	// TODO: debounce events
	for _, frame := range frames {
		switch f := frame.(type) {
		case *frm.SchemaChangeKeyspace:
			InternalSessionMarkV2SchemaStale(s, f.Keyspace, f.Change == "DROPPED")
			s.privateStorage.InternalMetadataDescriber.InternalInvalidateKeyspaceSchema(f.Keyspace)
			s.handleKeyspaceChange(f.Keyspace, f.Change)
		case *frm.SchemaChangeTable:
			s.privateStorage.InternalMetadataDescriber.InternalInvalidateTableSchema(f.Keyspace, f.Object)
			s.handleTableChange(f.Keyspace, f.Object, f.Change)
		case *frm.SchemaChangeAggregate:
			InternalSessionMarkV2SchemaStale(s, f.Keyspace, false)
			s.privateStorage.InternalMetadataDescriber.InternalInvalidateKeyspaceSchema(f.Keyspace)
		case *frm.SchemaChangeFunction:
			InternalSessionMarkV2SchemaStale(s, f.Keyspace, false)
			s.privateStorage.InternalMetadataDescriber.InternalInvalidateKeyspaceSchema(f.Keyspace)
		case *frm.SchemaChangeType:
			InternalSessionMarkV2SchemaStale(s, f.Keyspace, false)
			s.privateStorage.InternalMetadataDescriber.InternalInvalidateKeyspaceSchema(f.Keyspace)
		}
	}
}

func (s *Session) handleSchemaEventAndPublish(frames []frame) {
	InternalSessionHandleSchemaEvent( // Coalesce relevant schema work while keeping each bounded event's publication
		// behind the resulting visible metadata (including diagnostics on failure).
		s, frames)
	scopes := make(map[string]bool)
	for _, f := range frames {
		switch v := f.(type) {
		case *frm.SchemaChangeKeyspace:
			scopes[v.Keyspace] = true
		case *frm.SchemaChangeTable:
			scopes[v.Keyspace] = true
		case *frm.SchemaChangeType:
			scopes[v.Keyspace] = true
		case *frm.SchemaChangeFunction:
			scopes[v.Keyspace] = true
		case *frm.SchemaChangeAggregate:
			scopes[v.Keyspace] = true
		}
	}
	for scope := range scopes {
		if err := InternalSessionRefreshV2Keyspace(s, v2SchemaContext(InternalNewSchemaSource(s)), scope); err != nil {
			s.scheduleV2SchemaRetry()
		}
	}
	if !InternalSessionPublishV2Metadata(s) {
		InternalSessionPublishV2LocalEvent(s, Event{Kind: OverflowEvent, Generation: s.Metadata().Generation(), Change: "PUBLICATION_RESYNC"})
		return
	}
	for _, f := range frames {
		if event := events.FrameToEvent(f); event != nil {
			s.emitV2Event(event)
			if s.privateStorage.InternalEventBus != nil {
				s.privateStorage.InternalEventBus.PublishEvent(event)
			}
		}
	}
}

func (s *Session) handleKeyspaceChange(keyspace, change string) {
	if change == "DROPPED" || change == "UPDATED" {
		s.privateStorage.InternalMetadataDescriber.RemoveTabletsWithKeyspace(keyspace)
	}
	s.privateStorage.InternalPolicy.KeyspaceChanged(KeyspaceUpdateEvent{Keyspace: keyspace, Change: change})
}

func (s *Session) handleTableChange(keyspace, table, change string) {
	if change == "DROPPED" || change == "UPDATED" {
		s.privateStorage.InternalMetadataDescriber.RemoveTabletsWithTable(keyspace, table)
	}
}

// handleNodeEvent handles inbound status and topology change events.
//
// Status events are debounced by host IP; only the latest event is processed.
//
// Topology events are debounced by performing a single full topology refresh
// whenever any topology event comes in.
//
// Topology events are tracked separately from that map, rather than sharing it,
// so that a NEW_NODE is not dropped in favor of a newer UP for the same host
// (which would itself be dropped/ignored, as the node is not yet known). They
// are also refreshed regardless of DisableNodeStatusEvents -- only
// DisableTopologyEvents suppresses them.
func InternalSessionHandleNodeEvent(s *Session, frames []frame) {
	type nodeEvent struct {
		change string
		host   net.IP
		port   int
	}

	topologyEventReceived := false
	// status change events
	sEvents := make(map[string]*nodeEvent)

	for _, frame := range frames {
		switch f := frame.(type) {
		case *frm.TopologyChangeEventFrame:
			topologyEventReceived = true
		case *frm.StatusChangeEventFrame:
			event, ok := sEvents[InternalEventV2Address(f.Host, f.Port).String()]
			if !ok {
				event = &nodeEvent{change: f.Change, host: f.Host, port: f.Port}
				sEvents[InternalEventV2Address(f.Host, f.Port).String()] = event
			}
			event.change = f.Change
		}
	}

	if topologyEventReceived && !s.privateStorage.InternalCfg.Events.DisableTopologyEvents {
		_ = InternalSessionRefreshRingNow(s)
	}

	for _, f := range sEvents {
		if debug.Enabled {
			s.privateStorage.InternalLogger.Printf("gocql: dispatching status change event: %+v\n", f)
		}

		// ignore events we received if they were disabled
		// see https://github.com/apache/cassandra-gocql-driver/issues/1591
		switch f.change {
		case "UP":
			if !s.privateStorage.InternalCfg.Events.DisableNodeStatusEvents {
				InternalSessionHandleNodeUp(s, f.host, f.port)
			}
		case "DOWN":
			if !s.privateStorage.InternalCfg.Events.DisableNodeStatusEvents {
				InternalSessionHandleNodeDown(s, f.host, f.port)
			}
		}
	}
	if !InternalSessionPublishV2Metadata(s) {
		InternalSessionPublishV2LocalEvent(s, Event{Kind: OverflowEvent, Generation: s.Metadata().Generation(), Change: "PUBLICATION_RESYNC"})
		return
	}
	for _, f := range frames {
		if event := events.FrameToEvent(f); event != nil {
			s.emitV2Event(event)
			if s.privateStorage.InternalEventBus != nil {
				s.privateStorage.InternalEventBus.PublishEvent(event)
			}
		}
	}

}

func InternalSessionHandleNodeUp(s *Session, eventIp net.IP, eventPort int) {
	if debug.Enabled {
		s.privateStorage.InternalLogger.Printf("gocql: Session.handleNodeUp: %s:%d\n", eventIp.String(), eventPort)
	}

	host, ok := s.privateStorage.InternalHostSource.InternalGetHostByIP(eventIp.String())
	if !ok {
		s.debounceRingRefresh()
		return
	}

	if InternalClusterSettingsFilterHost(&s.privateStorage.InternalCfg, host) {
		return
	}

	s.startPoolFill(host)
}

func (s *Session) startPoolFill(host *HostInfo) {
	if s.Closed() {
		return
	}
	p := InternalSessionPeripheral(s)
	p.hostMu.Lock()
	if s.privateStorage.InternalHostSource.InternalGetHost(host.HostID()) != host {
		p.hostMu.Unlock()
		return
	}
	pool := s.privateStorage.InternalPool.InternalEnsureHost(host)
	p.hostMu.Unlock()
	s.privateStorage.InternalPolicy.AddHost(host)
	pool.InternalFill_debounce()
}

func InternalSessionHandlePoolConnected(s *Session, pool *hostConnPool) {
	if s == nil || s.Closed() || s.privateStorage.InternalPool == nil || s.privateStorage.InternalHostSource == nil {
		return
	}
	p := InternalSessionPeripheral(s)
	p.hostMu.Lock()
	current, ok := s.privateStorage.InternalPool.InternalGetPool(pool.InternalHost)
	known := s.privateStorage.InternalHostSource.InternalGetHost(pool.InternalHost.HostID())
	if !ok || current != pool || known != pool.InternalHost || pool.Size() == 0 {
		p.hostMu.Unlock()
		return
	}
	implEndpoint.InternalHostInfoSetState(pool.InternalHost, NodeUp)
	p.InternalMu.Lock()
	p.InternalPublicationRevision++
	p.InternalMu.Unlock()
	p.hostMu.Unlock()
	InternalSessionPublishV2Metadata(s)
	InternalSessionObserveV2Lifecycle(s, LifecycleEvent{Kind: "host", Stage: "connected", HostID: implEndpoint.InternalHostInfoHostUUID(pool.InternalHost), At: time.Now()})
	s.synchronizeV2HostPolicy(pool.InternalHost)
}
func InternalSessionHandleNodeConnected(s *Session, host *HostInfo) {
	if s.privateStorage.InternalPool != nil {
		if pool, ok := s.privateStorage.InternalPool.InternalGetPool(host); ok {
			InternalSessionHandlePoolConnected(s, pool)
		}
		return
	}
	defer InternalSessionPublishV2Metadata(s)
	if debug.Enabled {
		s.privateStorage.InternalLogger.Printf("gocql: Session.handleNodeConnected: %s:%d\n", host.ConnectAddress(), host.Port())
	}
	implEndpoint.InternalHostInfoSetState(host, NodeUp)
	InternalSessionObserveV2Lifecycle(s, LifecycleEvent{Kind: "host", Stage: "connected", HostID: implEndpoint.InternalHostInfoHostUUID(host), At: time.Now()})

	if !InternalClusterSettingsFilterHost(&s.privateStorage.InternalCfg, host) {
		s.privateStorage.InternalPolicy.HostUp(host)
	}
}

func InternalSessionHandleNodeDown(s *Session, ip net.IP, port int) {
	defer InternalSessionPublishV2Metadata(s)
	if debug.Enabled {
		s.privateStorage.InternalLogger.Printf("gocql: Session.handleNodeDown: %s:%d\n", ip.String(), port)
	}

	host, ok := s.privateStorage.InternalHostSource.InternalGetHostByIP(ip.String())
	if ok {
		filtered := InternalClusterSettingsFilterHost(&s.privateStorage.InternalCfg, host)
		p := InternalSessionPeripheral(s)
		p.hostMu.Lock()
		implEndpoint.InternalHostInfoSetState(host, NodeDown)
		p.InternalMu.Lock()
		p.InternalPublicationRevision++
		p.InternalMu.Unlock()
		if !filtered && s.privateStorage.InternalPool != nil {
			s.privateStorage.InternalPool.InternalRemoveHost(implEndpoint.InternalHostInfoHostUUID(host))
		}
		p.hostMu.Unlock()
		InternalSessionObserveV2Lifecycle(s, LifecycleEvent{Kind: "host", Stage: "down", HostID: implEndpoint.InternalHostInfoHostUUID(host), At: time.Now()})
		if filtered {
			return
		}
		s.synchronizeV2HostPolicy(host)
	}
}

func InternalSessionRecordV2Topology(s *Session, event *events.TopologyChangeEvent) {
	if s.privateStorage.InternalHostSource == nil || s.privateStorage.InternalCfg.Events.DisableTopologyEvents {
		return
	}
	r := s.privateStorage.InternalHostSource
	r.mu.Lock()
	r.eventEpoch++
	id := r.hostIPToUUID[event.Host.String()]
	if r.tombstones == nil {
		r.tombstones = make(map[string]bool)
	}
	if r.tombstoneAddresses == nil {
		r.tombstoneAddresses = make(map[string]string)
	}
	var removed *HostInfo
	if event.Change == "REMOVED_NODE" && id != "" {
		r.tombstones[id] = true
		r.tombstoneAddresses[event.Host.String()] = id
		removed = r.InternalHosts[id]
		delete(r.InternalHosts, id)
		delete(r.hostIPToUUID, event.Host.String())
		r.revision++
	}
	if event.Change == "NEW_NODE" {
		delete(r.tombstones, id)
		delete(r.tombstones, r.tombstoneAddresses[event.Host.String()])
		delete(r.tombstoneAddresses, event.Host.String())
	}
	r.mu.Unlock()
	if removed != nil {
		if s.privateStorage.InternalPool != nil {
			s.privateStorage.InternalPool.InternalRemoveHost(implEndpoint.InternalHostInfoHostUUID(removed))
		}
		if s.privateStorage.InternalPolicy != nil {
			s.privateStorage.InternalPolicy.RemoveHost(removed)
		}
		if s.privateStorage.InternalMetadataDescriber != nil {
			s.privateStorage.InternalMetadataDescriber.RemoveTabletsWithHost(removed)
		}
	}
	InternalSessionPublishV2Metadata(s)
}
func (s *Session) fenceV2Down(ip net.IP, port int) {
	if s.privateStorage.InternalHostSource == nil || s.privateStorage.InternalPool == nil {
		return
	}
	host, ok := s.privateStorage.InternalHostSource.InternalGetHostByIP(ip.String())
	if !ok {
		return
	}
	filtered := InternalClusterSettingsFilterHost(&s.privateStorage.InternalCfg, host)
	p := InternalSessionPeripheral(s)
	p.hostMu.Lock()
	implEndpoint.InternalHostInfoSetState(host, NodeDown)
	p.InternalMu.Lock()
	p.InternalPublicationRevision++
	p.InternalMu.Unlock()
	if !filtered {
		s.privateStorage.InternalPool.InternalRemoveHost(implEndpoint.InternalHostInfoHostUUID(host))
	}
	p.hostMu.Unlock()
	InternalSessionPublishV2Metadata(s)
}

// A notification may run user code and race a newer DOWN/removal/replacement.
// Reconcile against the current host/pool after each callback; a stale callback
// cannot leave the policy containing the superseded emitting host.
func (s *Session) synchronizeV2HostPolicy(fallback *HostInfo) {
	type stamp struct {
		host       *HostInfo
		pool       *hostConnPool
		state      nodeState
		generation uint64
	}
	capture := func() stamp {
		p := InternalSessionPeripheral(s)
		p.hostMu.Lock()
		defer p.hostMu.Unlock()
		h := s.privateStorage.InternalHostSource.InternalGetHost(fallback.HostID())
		if h == nil {
			return stamp{}
		}
		pool, _ := s.privateStorage.InternalPool.InternalGetPool(h)
		implEndpoint.HostInfoData(h).InternalMu.RLock()
		out := stamp{h, pool, implEndpoint.HostInfoData(h).InternalState, implEndpoint.HostInfoData(h).InternalLifecycleGeneration}
		implEndpoint.HostInfoData(h).InternalMu.RUnlock()
		return out
	}
	for !s.Closed() {
		before := capture()
		if before.host == nil {
			s.privateStorage.InternalPolicy.RemoveHost(fallback)
		} else if before.state == NodeUp && before.pool != nil && before.pool.Size() > 0 && !InternalClusterSettingsFilterHost(&s.privateStorage.InternalCfg, before.host) {
			s.privateStorage.InternalPolicy.HostUp(before.host)
		} else {
			s.privateStorage.InternalPolicy.HostDown(before.host)
		}
		if before == capture() {
			return
		}
	}
}
