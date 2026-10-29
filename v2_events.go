package gocql

import (
	"net"
	"net/netip"
	"slices"
	"sync"
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
	mu     sync.Mutex
	events chan Event
	closed bool
	filter EventFilter
	parent *v2PeripheralState
}

func (s *Subscription) Events() <-chan Event {
	if s == nil {
		return nil
	}
	return s.events
}
func (s *Subscription) Close() {
	if s == nil {
		return
	}
	s.mu.Lock()
	if !s.closed {
		s.closed = true
		close(s.events)
	}
	s.mu.Unlock()
	if s.parent != nil {
		s.parent.mu.Lock()
		delete(s.parent.subscribers, s)
		s.parent.mu.Unlock()
	}
}
func (s *Subscription) accepts(e Event) bool {
	if e.Kind == OverflowEvent {
		return true
	}
	if s.filter.Keyspace != "" && e.Keyspace != s.filter.Keyspace {
		return false
	}
	if len(s.filter.Kinds) == 0 {
		return true
	}
	for _, kind := range s.filter.Kinds {
		if kind == e.Kind {
			return true
		}
	}
	return false
}
func (s *Subscription) publish(e Event) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || !s.accepts(e) {
		return
	}
	e.ArgumentTypes = append([]string(nil), e.ArgumentTypes...)
	if e.Kind != OverflowEvent {
		select {
		case s.events <- e:
			return
		default:
		}
	}
	highest := e.Generation
	for {
		select {
		case discarded := <-s.events:
			if discarded.Generation > highest {
				highest = discarded.Generation
			}
		default:
			change := "RESYNC"
			if e.Kind == OverflowEvent && e.Change != "" {
				change = e.Change
			}
			s.events <- Event{Kind: OverflowEvent, Generation: highest, Change: change}
			s.closed = true
			close(s.events)
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
	p := s.peripheral()
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return nil, ErrClosed
	}
	sub := &Subscription{events: make(chan Event, filter.Capacity), filter: filter, parent: p}
	p.subscribers[sub] = struct{}{}
	return sub, nil
}
func (s *Session) closeV2Peripheral() {
	p := s.peripheral()
	if p == nil {
		return
	}
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return
	}
	p.closed = true
	close(p.done)
	subs := make([]*Subscription, 0, len(p.subscribers))
	for sub := range p.subscribers {
		subs = append(subs, sub)
	}
	p.mu.Unlock()
	for _, sub := range subs {
		sub.Close()
	}
	s.observeV2Lifecycle(LifecycleEvent{Kind: "session", Stage: "closed", At: time.Now()})
}
func eventV2Address(host net.IP, port int) netip.AddrPort {
	ip, ok := netip.AddrFromSlice(host)
	if !ok || port < 0 || port > 65535 {
		return netip.AddrPort{}
	}
	return netip.AddrPortFrom(ip.Unmap(), uint16(port))
}
func (s *Session) publishV2Event(source oldevents.Event) { s.publishV2EventMode(source, true, true) }
func (s *Session) recordV2Event(source oldevents.Event)  { s.publishV2EventMode(source, true, false) }
func (s *Session) emitV2Event(source oldevents.Event)    { s.publishV2EventMode(source, false, true) }
func (s *Session) publishV2EventMode(source oldevents.Event, invalidate, deliver bool) {
	p := s.peripheral()
	if p == nil {
		return
	}
	e := Event{Generation: s.Metadata().Generation()}
	switch v := source.(type) {
	case *oldevents.TopologyChangeEvent:
		e.Kind = TopologyEvent
		e.Change = v.Change
		e.Address = eventV2Address(v.Host, v.Port)
	case *oldevents.StatusChangeEvent:
		e.Kind = StatusEvent
		e.Change = v.Change
		e.Address = eventV2Address(v.Host, v.Port)
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
		e.Address = eventV2Address(v.NewHost.Host, v.NewHost.Port)
		e.HostID, _ = ParseUUID(v.NewHost.HostID)
	default:
		return
	}
	if e.Kind == SchemaEvent && invalidate {
		s.invalidatePreparedSchema(e)
		p.mu.Lock()
		p.publicationRevision++
		p.schemaEpoch[e.Keyspace]++
		p.schemaGeneration.Add(1)
		if e.Target == "KEYSPACE" {
			if e.Change == "DROPPED" {
				p.schemaDropped[e.Keyspace] = true
			}
			if e.Change == "CREATED" {
				delete(p.schemaDropped, e.Keyspace)
			}
		}
		if e.Change == "DROPPED" && s.metadataDescriber != nil {
			md := s.metadataDescriber.metadata
			if e.Target == "KEYSPACE" {
				md.keyspaceMetadata.removeKeyspace(e.Keyspace)
			} else {
				md.keyspaceMetadata.updateKeyspace(e.Keyspace, func(ks *KeyspaceMetadata) {
					switch e.Target {
					case "TABLE":
						ks.removeTable(e.Object)
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
		p.mu.Unlock()
		if deliver || e.Change == "DROPPED" {
			if !s.publishV2Metadata() {
				s.publishV2LocalEvent(Event{Kind: OverflowEvent, Generation: s.Metadata().Generation(), Change: "PUBLICATION_RESYNC"})
				return
			}
			e.Generation = s.Metadata().Generation()
		}
	}
	if !deliver {
		return
	}
	if e.HostID == (UUID{}) && e.Address.IsValid() {
		for _, host := range s.Metadata().hosts {
			if host.Address == e.Address {
				e.HostID = host.ID
				break
			}
		}
	}
	p.mu.Lock()
	subs := make([]*Subscription, 0, len(p.subscribers))
	for sub := range p.subscribers {
		subs = append(subs, sub)
	}
	p.mu.Unlock()
	for _, sub := range subs {
		sub.publish(e)
		sub.mu.Lock()
		closed := sub.closed
		sub.mu.Unlock()
		if closed {
			p.mu.Lock()
			delete(p.subscribers, sub)
			p.mu.Unlock()
		}
	}
}

func (s *Session) publishV2LocalEvent(event Event) {
	p := s.peripheral()
	if p == nil {
		return
	}
	p.mu.Lock()
	subs := make([]*Subscription, 0, len(p.subscribers))
	for sub := range p.subscribers {
		subs = append(subs, sub)
	}
	p.mu.Unlock()
	for _, sub := range subs {
		sub.publish(event)
		sub.mu.Lock()
		closed := sub.closed
		sub.mu.Unlock()
		if closed {
			p.mu.Lock()
			delete(p.subscribers, sub)
			p.mu.Unlock()
		}
	}
}
