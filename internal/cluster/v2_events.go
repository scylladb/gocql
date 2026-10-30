// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cluster

import implMetadata "github.com/scylladb/gocql/v2/internal/metadata"

import (
	routing "github.com/scylladb/gocql/v2/internal/routing"

	"net"
	"net/netip"
	"slices"

	"time"

	oldevents "github.com/scylladb/gocql/v2/events"
)

type EventKind uint8

const (
	TopologyEvent EventKind = iota
	StatusEvent
	SchemaEvent
	ControlEvent
	ClientRouteEvent
	OverflowEvent
)

type Event struct {
	Kind                             EventKind
	Generation                       uint64
	Change, Target, Keyspace, Object string
	ArgumentTypes                    []string
	HostID                           UUID
	Address                          netip.AddrPort
}
type EventFilter struct {
	Kinds    []EventKind
	Keyspace string
	Capacity int
}

// Subscription owns a bounded queue. Overflow discards queued deltas and emits
// OverflowEvent, then closes. Subscribe again before reading Session.Metadata.
type Subscription struct {
	privateStorage SubscriptionStorage
}

func (s *Subscription) Events() <-chan Event {
	if s == nil {
		return nil
	}
	return s.privateStorage.events
}
func (s *Subscription) Close() {
	if s == nil {
		return
	}
	s.privateStorage.mu.Lock()
	if !s.privateStorage.closed {
		s.privateStorage.closed = true
		close(s.privateStorage.events)
	}
	s.privateStorage.mu.Unlock()
	if s.privateStorage.parent != nil {
		s.privateStorage.parent.InternalMu.Lock()
		delete(s.privateStorage.parent.InternalSubscribers, s)
		s.privateStorage.parent.InternalMu.Unlock()
	}
}
func (s *Subscription) accepts(e Event) bool {
	if e.Kind == OverflowEvent {
		return true
	}
	if s.privateStorage.filter.Keyspace != "" && e.Keyspace != s.privateStorage.filter.Keyspace {
		return false
	}
	if len(s.privateStorage.filter.Kinds) == 0 {
		return true
	}
	for _, kind := range s.privateStorage.filter.Kinds {
		if kind == e.Kind {
			return true
		}
	}
	return false
}
func InternalSubscriptionPublish(s *Subscription, e Event) {
	s.privateStorage.mu.Lock()
	defer s.privateStorage.mu.Unlock()
	if s.privateStorage.closed || !s.accepts(e) {
		return
	}
	e.ArgumentTypes = append([]string(nil), e.ArgumentTypes...)
	if e.Kind != OverflowEvent {
		select {
		case s.privateStorage.events <- e:
			return
		default:
		}
	}
	highest := e.Generation
	for {
		select {
		case discarded := <-s.privateStorage.events:
			if discarded.Generation > highest {
				highest = discarded.Generation
			}
		default:
			change := "RESYNC"
			if e.Kind == OverflowEvent && e.Change != "" {
				change = e.Change
			}
			s.privateStorage.events <- Event{Kind: OverflowEvent, Generation: highest, Change: change}
			s.privateStorage.closed = true
			close(s.privateStorage.events)
			return
		}
	}
}
func (s *Session) Subscribe(filter EventFilter) (*Subscription, error) {
	if s == nil || s.Closed() {
		return nil, ErrClosed
	}
	if filter.Capacity < 0 {
		return nil, typeValidation("subscription capacity", "must not be negative")
	}
	if filter.Capacity == 0 {
		filter.Capacity = 64
	}
	for _, kind := range filter.Kinds {
		if kind > OverflowEvent {
			return nil, typeValidation("event filter", "unknown event kind")
		}
	}
	filter.Kinds = append([]EventKind(nil), filter.Kinds...)
	p := InternalSessionPeripheral(s)
	p.InternalMu.Lock()
	defer p.InternalMu.Unlock()
	if p.closed {
		return nil, ErrClosed
	}
	sub := func() *Subscription {
		opaqueStorageValue := new(Subscription)
		opaqueStorageValue.privateStorage.events = make(chan Event, filter.Capacity)
		opaqueStorageValue.privateStorage.filter = filter
		opaqueStorageValue.privateStorage.parent = p
		return opaqueStorageValue
	}()
	p.InternalSubscribers[sub] = struct{}{}
	return sub, nil
}
func InternalSessionCloseV2Peripheral(s *Session) {
	p := InternalSessionPeripheral(s)
	if p == nil {
		return
	}
	p.InternalMu.Lock()
	if p.closed {
		p.InternalMu.Unlock()
		return
	}
	p.closed = true
	close(p.InternalDone)
	subs := make([]*Subscription, 0, len(p.InternalSubscribers))
	for sub := range p.InternalSubscribers {
		subs = append(subs, sub)
	}
	p.InternalMu.Unlock()
	for _, sub := range subs {
		sub.Close()
	}
	InternalSessionObserveV2Lifecycle(s, LifecycleEvent{Kind: "session", Stage: "closed", At: time.Now()})
}
func InternalEventV2Address(host net.IP, port int) netip.AddrPort {
	ip, ok := netip.AddrFromSlice(host)
	if !ok || port < 0 || port > 65535 {
		return netip.AddrPort{}
	}
	return netip.AddrPortFrom(ip.Unmap(), uint16(port))
}
func InternalSessionPublishV2Event(s *Session, source oldevents.Event) {
	s.publishV2EventMode(source, true, true)
}
func (s *Session) recordV2Event(source oldevents.Event) { s.publishV2EventMode(source, true, false) }
func (s *Session) emitV2Event(source oldevents.Event)   { s.publishV2EventMode(source, false, true) }
func (s *Session) publishV2EventMode(source oldevents.Event, invalidate, deliver bool) {
	p := InternalSessionPeripheral(s)
	if p == nil {
		return
	}
	e := Event{Generation: s.Metadata().Generation()}
	switch v := source.(type) {
	case *oldevents.TopologyChangeEvent:
		e.Kind = TopologyEvent
		e.Change = v.Change
		e.Address = InternalEventV2Address(v.Host, v.Port)
	case *oldevents.StatusChangeEvent:
		e.Kind = StatusEvent
		e.Change = v.Change
		e.Address = InternalEventV2Address(v.Host, v.Port)
	case *oldevents.SchemaChangeKeyspaceEvent:
		e.Kind = SchemaEvent
		e.Target = "KEYSPACE"
		e.Change = v.Change
		e.Keyspace = v.Keyspace
	case *oldevents.SchemaChangeTableEvent:
		e.Kind = SchemaEvent
		e.Target = "TABLE"
		e.Change = v.Change
		e.Keyspace = v.Keyspace
		e.Object = v.Table
	case *oldevents.SchemaChangeTypeEvent:
		e.Kind = SchemaEvent
		e.Target = "TYPE"
		e.Change = v.Change
		e.Keyspace = v.Keyspace
		e.Object = v.TypeName
	case *oldevents.SchemaChangeFunctionEvent:
		e.Kind = SchemaEvent
		e.Target = "FUNCTION"
		e.Change = v.Change
		e.Keyspace = v.Keyspace
		e.Object = v.Function
		e.ArgumentTypes = v.Arguments
	case *oldevents.SchemaChangeAggregateEvent:
		e.Kind = SchemaEvent
		e.Target = "AGGREGATE"
		e.Change = v.Change
		e.Keyspace = v.Keyspace
		e.Object = v.Aggregate
		e.ArgumentTypes = v.Arguments
	case *oldevents.ClientRoutesChangedEvent:
		e.Kind = ClientRouteEvent
		e.Change = v.ChangeType
		e.ArgumentTypes = append(append([]string(nil), v.ConnectionIDs...), v.HostIDs...)
	case *oldevents.ControlConnectionRecreatedEvent:
		e.Kind = ControlEvent
		e.Change = "RECONNECTED"
		e.Address = InternalEventV2Address(v.NewHost.Host, v.NewHost.Port)
		e.HostID, _ = ParseUUID(v.NewHost.HostID)
	default:
		return
	}
	if e.Kind == SchemaEvent && invalidate {
		InternalSessionInvalidatePreparedSchema(s, e)
		p.InternalMu.Lock()
		p.InternalPublicationRevision++
		p.InternalSchemaEpoch[e.Keyspace]++
		p.schemaGeneration.Add(1)
		if e.Target == "KEYSPACE" {
			if e.Change == "DROPPED" {
				p.schemaDropped[e.Keyspace] = true
			}
			if e.Change == "CREATED" {
				delete(p.schemaDropped, e.Keyspace)
			}
		}
		if e.Change == "DROPPED" && s.privateStorage.InternalMetadataDescriber != nil {
			md := s.privateStorage.InternalMetadataDescriber.InternalMetadata
			if e.Target == "KEYSPACE" {
				md.privateStorage.InternalKeyspaceMetadata.InternalRemoveKeyspace(e.Keyspace)
			} else {
				md.privateStorage.InternalKeyspaceMetadata.InternalUpdateKeyspace(e.Keyspace, func(ks *KeyspaceMetadata) {
					switch e.Target {
					case "TABLE":
						implMetadata.InternalKeyspaceMetadataRemoveTable(ks, e.Object)
					case "TYPE":
						delete(ks.Types, e.Object)
					case "FUNCTION":
						for key, f := range ks.Functions {
							if f.Name == e.Object && slices.Equal(f.ArgumentTypes, e.ArgumentTypes) {
								delete(ks.Functions, key)
							}
						}
					case "AGGREGATE":
						for key, a := range ks.Aggregates {
							if a.Name == e.Object && slices.Equal(a.ArgumentTypes, e.ArgumentTypes) {
								delete(ks.Aggregates, key)
							}
						}
					}
				})
			}
		}
		p.InternalMu.Unlock()
		if deliver || e.Change == "DROPPED" {
			if !InternalSessionPublishV2Metadata(s) {
				InternalSessionPublishV2LocalEvent(s, Event{Kind: OverflowEvent, Generation: s.Metadata().Generation(), Change: "PUBLICATION_RESYNC"})
				return
			}
			e.Generation = s.Metadata().Generation()
		}
	}
	if !deliver {
		return
	}
	if e.HostID == (UUID{}) && e.Address.IsValid() {
		for _, host := range routing.SnapshotData(s.Metadata()).InternalHosts {
			if host.Address == e.Address {
				e.HostID = host.ID
				break
			}
		}
	}
	p.InternalMu.Lock()
	subs := make([]*Subscription, 0, len(p.InternalSubscribers))
	for sub := range p.InternalSubscribers {
		subs = append(subs, sub)
	}
	p.InternalMu.Unlock()
	for _, sub := range subs {
		InternalSubscriptionPublish(sub, e)
		sub.privateStorage.mu.Lock()
		closed := sub.privateStorage.closed
		sub.privateStorage.mu.Unlock()
		if closed {
			p.InternalMu.Lock()
			delete(p.InternalSubscribers, sub)
			p.InternalMu.Unlock()
		}
	}
}

func InternalSessionPublishV2LocalEvent(s *Session, event Event) {
	p := InternalSessionPeripheral(s)
	if p == nil {
		return
	}
	p.InternalMu.Lock()
	subs := make([]*Subscription, 0, len(p.InternalSubscribers))
	for sub := range p.InternalSubscribers {
		subs = append(subs, sub)
	}
	p.InternalMu.Unlock()
	for _, sub := range subs {
		InternalSubscriptionPublish(sub, event)
		sub.privateStorage.mu.Lock()
		closed := sub.privateStorage.closed
		sub.privateStorage.mu.Unlock()
		if closed {
			p.InternalMu.Lock()
			delete(p.InternalSubscribers, sub)
			p.InternalMu.Unlock()
		}
	}
}
