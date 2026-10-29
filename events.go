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

package gocql

import (
	"context"
	"net"
	"sync"
	"time"

	"github.com/scylladb/gocql/v2/events"
	"github.com/scylladb/gocql/v2/internal/debug"
	frm "github.com/scylladb/gocql/v2/internal/frame"
)

type eventDebouncer struct {
	logger   StdLogger
	timer    *time.Timer
	callback func([]frame)
	quit     chan struct{}
	done     chan struct{}
	stopOnce sync.Once
	stopped  bool
	overflow bool
	resync   func()
	name     string
	events   []frame
	releases []func()
	mu       sync.Mutex
}

func newEventDebouncer(name string, eventHandler func([]frame), logger StdLogger) *eventDebouncer {
	e := &eventDebouncer{
		name: name,
		quit: make(chan struct{}), done: make(chan struct{}),
		timer:    time.NewTimer(eventDebounceTime),
		callback: eventHandler,
		logger:   logger,
	}
	e.timer.Stop()
	go e.flusher()

	return e
}

func (e *eventDebouncer) stop() {
	e.stopOnce.Do(func() { e.mu.Lock(); e.stopped = true; e.timer.Stop(); close(e.quit); e.mu.Unlock() })
	<-e.done
}
func (e *eventDebouncer) flusher() {
	defer close(e.done)
	defer func() {
		e.mu.Lock()
		releases := e.releases
		e.releases = nil
		e.events = nil
		e.mu.Unlock()
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
		case <-e.timer.C:
			e.mu.Lock()
			pending := e.events
			releases := e.releases
			e.releases = nil
			overflow := e.overflow
			e.overflow = false
			e.events = nil
			stopped := e.stopped
			e.mu.Unlock()
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
			if !stopped && overflow && e.resync != nil {
				e.resync()
			}
		}
	}
}

const (
	eventBufferSize   = 1000
	eventDebounceTime = 1 * time.Second
)

// flush must be called with mu locked
func (e *eventDebouncer) flush() {
	if len(e.events) == 0 {
		return
	}

	// if the flush interval is faster than the callback then we will end up calling
	// the callback multiple times, probably a bad idea. In this case we could drop
	// frames?
	pending := e.events
	releases := e.releases
	e.events = nil
	e.releases = nil
	e.mu.Unlock()
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
	e.mu.Lock()
}

func (e *eventDebouncer) debounce(frame frame) { e.debounceOwned(frame, nil) }
func (e *eventDebouncer) debounceOwned(frame frame, release func()) bool {
	return e.debounceOwnedContext(context.Background(), frame, release)
}
func (e *eventDebouncer) debounceOwnedContext(ctx context.Context, frame frame, release func()) bool {
	e.mu.Lock()
	if e.stopped || ctx.Err() != nil {
		e.mu.Unlock()
		return false
	}
	e.timer.Reset(eventDebounceTime)

	// TODO: probably need a warning to track if this threshold is too low
	if len(e.events) < eventBufferSize {
		e.events = append(e.events, frame)
		e.releases = append(e.releases, release)
		e.mu.Unlock()
		return true
	} else {
		e.overflow = true
	}
	e.mu.Unlock()
	return false
}

func (s *Session) publishEvent(event events.Event) {
	s.publishV2Event(event)
	if s.clientRoutesHandler != nil && s.clientRoutesHandler.initialized.Load() {
		s.clientRoutesHandler.handleRouteEvent(event)
	}
	if s.eventBus == nil {
		return
	}

	if !s.eventBus.PublishEvent(event) {
		s.logger.Printf("can't publish event, eventbus is full, increase Cluster.EventBusConfig.InputEventsQueueSize; event is dropped")
	}
}

func (s *Session) handleEvent(frame frame) { s.handleEventOwned(frame, nil) }
func (s *Session) handleEventOwned(frame frame, release func()) bool {
	return s.handleEventOwnedContext(context.Background(), frame, release)
}
func (s *Session) handleEventOwnedContext(ctx context.Context, frame frame, release func()) bool {
	switch frame.(type) {
	case *frm.SchemaChangeKeyspace, *frm.SchemaChangeFunction, *frm.SchemaChangeTable, *frm.SchemaChangeAggregate, *frm.SchemaChangeType:
		if s.cfg.Events.DisableSchemaEvents {
			return false
		}
	case *frm.TopologyChangeEventFrame:
		if s.cfg.Events.DisableTopologyEvents {
			return false
		}
	case *frm.StatusChangeEventFrame:
		if s.cfg.Events.DisableNodeStatusEvents {
			return false
		}
	case *frm.ClientRoutesChanged:
		if s.cfg.disableClientRouteEvents {
			return false
		}
	}
	if debug.Enabled {
		s.logger.Printf("gocql: handling frame: %v\n", frame)
	}

	source := events.FrameToEvent(frame)
	if source != nil {
		s.recordV2Event(source)
	}
	switch e := source.(type) {
	case *events.TopologyChangeEvent:
		s.recordV2Topology(e)
	case *events.StatusChangeEvent:
		if e.Change == "DOWN" && !s.cfg.Events.DisableNodeStatusEvents {
			s.fenceV2Down(e.Host, e.Port)
		}
	}
	switch f := frame.(type) {
	case *frm.SchemaChangeKeyspace, *frm.SchemaChangeFunction,
		*frm.SchemaChangeTable, *frm.SchemaChangeAggregate, *frm.SchemaChangeType:

		return s.schemaEvents.debounceOwnedContext(ctx, frame, release)
	case *frm.TopologyChangeEventFrame, *frm.StatusChangeEventFrame:
		return s.nodeEvents.debounceOwnedContext(ctx, frame, release)
	case *frm.ClientRoutesChanged:
		if s.clientRoutesHandler != nil {
			return s.clientRoutesHandler.handleRouteEventOwned(source, release)
		}
	default:
		s.logger.Printf("gocql: invalid event frame (%T): %v\n", f, f)
	}
	return false

}

func (s *Session) handleSchemaEvent(frames []frame) {
	s.publishV2Metadata()
	defer s.publishV2Metadata()
	defer s.scheduleV2SchemaRetry()
	// TODO: debounce events
	for _, frame := range frames {
		switch f := frame.(type) {
		case *frm.SchemaChangeKeyspace:
			s.markV2SchemaStale(f.Keyspace, f.Change == "DROPPED")
			s.metadataDescriber.invalidateKeyspaceSchema(f.Keyspace)
			s.handleKeyspaceChange(f.Keyspace, f.Change)
		case *frm.SchemaChangeTable:
			s.metadataDescriber.invalidateTableSchema(f.Keyspace, f.Object)
			s.handleTableChange(f.Keyspace, f.Object, f.Change)
		case *frm.SchemaChangeAggregate:
			s.markV2SchemaStale(f.Keyspace, false)
			s.metadataDescriber.invalidateKeyspaceSchema(f.Keyspace)
		case *frm.SchemaChangeFunction:
			s.markV2SchemaStale(f.Keyspace, false)
			s.metadataDescriber.invalidateKeyspaceSchema(f.Keyspace)
		case *frm.SchemaChangeType:
			s.markV2SchemaStale(f.Keyspace, false)
			s.metadataDescriber.invalidateKeyspaceSchema(f.Keyspace)
		}
	}
}

func (s *Session) handleSchemaEventAndPublish(frames []frame) {
	// Coalesce relevant schema work while keeping each bounded event's publication
	// behind the resulting visible metadata (including diagnostics on failure).
	s.handleSchemaEvent(frames)
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
		if err := s.refreshV2Keyspace(v2SchemaContext(s), scope); err != nil {
			s.scheduleV2SchemaRetry()
		}
	}
	if !s.publishV2Metadata() {
		s.publishV2LocalEvent(Event{Kind: OverflowEvent, Generation: s.Metadata().Generation(), Change: "PUBLICATION_RESYNC"})
		return
	}
	for _, f := range frames {
		if event := events.FrameToEvent(f); event != nil {
			s.emitV2Event(event)
			if s.eventBus != nil {
				s.eventBus.PublishEvent(event)
			}
		}
	}
}

func (s *Session) handleKeyspaceChange(keyspace, change string) {
	if change == "DROPPED" || change == "UPDATED" {
		s.metadataDescriber.RemoveTabletsWithKeyspace(keyspace)
	}
	s.policy.KeyspaceChanged(KeyspaceUpdateEvent{Keyspace: keyspace, Change: change})
}

func (s *Session) handleTableChange(keyspace, table, change string) {
	if change == "DROPPED" || change == "UPDATED" {
		s.metadataDescriber.RemoveTabletsWithTable(keyspace, table)
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
func (s *Session) handleNodeEvent(frames []frame) {
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
			event, ok := sEvents[eventV2Address(f.Host, f.Port).String()]
			if !ok {
				event = &nodeEvent{change: f.Change, host: f.Host, port: f.Port}
				sEvents[eventV2Address(f.Host, f.Port).String()] = event
			}
			event.change = f.Change
		}
	}

	if topologyEventReceived && !s.cfg.Events.DisableTopologyEvents {
		_ = s.refreshRingNow()
	}

	for _, f := range sEvents {
		if debug.Enabled {
			s.logger.Printf("gocql: dispatching status change event: %+v\n", f)
		}

		// ignore events we received if they were disabled
		// see https://github.com/apache/cassandra-gocql-driver/issues/1591
		switch f.change {
		case "UP":
			if !s.cfg.Events.DisableNodeStatusEvents {
				s.handleNodeUp(f.host, f.port)
			}
		case "DOWN":
			if !s.cfg.Events.DisableNodeStatusEvents {
				s.handleNodeDown(f.host, f.port)
			}
		}
	}
	if !s.publishV2Metadata() {
		s.publishV2LocalEvent(Event{Kind: OverflowEvent, Generation: s.Metadata().Generation(), Change: "PUBLICATION_RESYNC"})
		return
	}
	for _, f := range frames {
		if event := events.FrameToEvent(f); event != nil {
			s.emitV2Event(event)
			if s.eventBus != nil {
				s.eventBus.PublishEvent(event)
			}
		}
	}

}

func (s *Session) handleNodeUp(eventIp net.IP, eventPort int) {
	if debug.Enabled {
		s.logger.Printf("gocql: Session.handleNodeUp: %s:%d\n", eventIp.String(), eventPort)
	}

	host, ok := s.hostSource.getHostByIP(eventIp.String())
	if !ok {
		s.debounceRingRefresh()
		return
	}

	if s.cfg.filterHost(host) {
		return
	}

	s.startPoolFill(host)
}

func (s *Session) startPoolFill(host *HostInfo) {
	if s.Closed() {
		return
	}
	p := s.peripheral()
	p.hostMu.Lock()
	if s.hostSource.getHost(host.HostID()) != host {
		p.hostMu.Unlock()
		return
	}
	pool := s.pool.ensureHost(host)
	p.hostMu.Unlock()
	s.policy.AddHost(host)
	pool.fill_debounce()
}

func (s *Session) handlePoolConnected(pool *hostConnPool) {
	if s == nil || s.Closed() || s.pool == nil || s.hostSource == nil {
		return
	}
	p := s.peripheral()
	p.hostMu.Lock()
	current, ok := s.pool.getPool(pool.host)
	known := s.hostSource.getHost(pool.host.HostID())
	if !ok || current != pool || known != pool.host || pool.Size() == 0 {
		p.hostMu.Unlock()
		return
	}
	pool.host.setState(NodeUp)
	p.mu.Lock()
	p.publicationRevision++
	p.mu.Unlock()
	p.hostMu.Unlock()
	s.publishV2Metadata()
	s.observeV2Lifecycle(LifecycleEvent{Kind: "host", Stage: "connected", HostID: pool.host.hostUUID(), At: time.Now()})
	s.synchronizeV2HostPolicy(pool.host)
}
func (s *Session) handleNodeConnected(host *HostInfo) {
	if s.pool != nil {
		if pool, ok := s.pool.getPool(host); ok {
			s.handlePoolConnected(pool)
		}
		return
	}
	defer s.publishV2Metadata()
	if debug.Enabled {
		s.logger.Printf("gocql: Session.handleNodeConnected: %s:%d\n", host.ConnectAddress(), host.Port())
	}

	host.setState(NodeUp)
	s.observeV2Lifecycle(LifecycleEvent{Kind: "host", Stage: "connected", HostID: host.hostUUID(), At: time.Now()})

	if !s.cfg.filterHost(host) {
		s.policy.HostUp(host)
	}
}

func (s *Session) handleNodeDown(ip net.IP, port int) {
	defer s.publishV2Metadata()
	if debug.Enabled {
		s.logger.Printf("gocql: Session.handleNodeDown: %s:%d\n", ip.String(), port)
	}

	host, ok := s.hostSource.getHostByIP(ip.String())
	if ok {
		filtered := s.cfg.filterHost(host)
		p := s.peripheral()
		p.hostMu.Lock()
		host.setState(NodeDown)
		p.mu.Lock()
		p.publicationRevision++
		p.mu.Unlock()
		if !filtered && s.pool != nil {
			s.pool.removeHost(host.hostUUID())
		}
		p.hostMu.Unlock()
		s.observeV2Lifecycle(LifecycleEvent{Kind: "host", Stage: "down", HostID: host.hostUUID(), At: time.Now()})
		if filtered {
			return
		}
		s.synchronizeV2HostPolicy(host)
	}
}

func (s *Session) recordV2Topology(event *events.TopologyChangeEvent) {
	if s.hostSource == nil || s.cfg.Events.DisableTopologyEvents {
		return
	}
	r := s.hostSource
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
		removed = r.hosts[id]
		delete(r.hosts, id)
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
		if s.pool != nil {
			s.pool.removeHost(removed.hostUUID())
		}
		if s.policy != nil {
			s.policy.RemoveHost(removed)
		}
		if s.metadataDescriber != nil {
			s.metadataDescriber.RemoveTabletsWithHost(removed)
		}
	}
	s.publishV2Metadata()
}
func (s *Session) fenceV2Down(ip net.IP, port int) {
	if s.hostSource == nil || s.pool == nil {
		return
	}
	host, ok := s.hostSource.getHostByIP(ip.String())
	if !ok {
		return
	}
	filtered := s.cfg.filterHost(host)
	p := s.peripheral()
	p.hostMu.Lock()
	host.setState(NodeDown)
	p.mu.Lock()
	p.publicationRevision++
	p.mu.Unlock()
	if !filtered {
		s.pool.removeHost(host.hostUUID())
	}
	p.hostMu.Unlock()
	s.publishV2Metadata()
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
		p := s.peripheral()
		p.hostMu.Lock()
		defer p.hostMu.Unlock()
		h := s.hostSource.getHost(fallback.HostID())
		if h == nil {
			return stamp{}
		}
		pool, _ := s.pool.getPool(h)
		h.mu.RLock()
		out := stamp{h, pool, h.state, h.lifecycleGeneration}
		h.mu.RUnlock()
		return out
	}
	for !s.Closed() {
		before := capture()
		if before.host == nil {
			s.policy.RemoveHost(fallback)
		} else if before.state == NodeUp && before.pool != nil && before.pool.Size() > 0 && !s.cfg.filterHost(before.host) {
			s.policy.HostUp(before.host)
		} else {
			s.policy.HostDown(before.host)
		}
		if before == capture() {
			return
		}
	}
}
