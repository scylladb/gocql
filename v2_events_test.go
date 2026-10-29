package gocql

import (
	oldevents "github.com/scylladb/gocql/v2/events"
	"sync"
	"testing"
)

func TestV2SubscriptionsFilterOverflowAndClose(t *testing.T) {
	s := &Session{}
	filter := EventFilter{Kinds: []EventKind{SchemaEvent}, Keyspace: "ks", Capacity: 1}
	sub, err := s.Subscribe(filter)
	if err != nil {
		t.Fatal(err)
	}
	filter.Kinds[0] = StatusEvent
	s.publishV2Event(&oldevents.SchemaChangeTableEvent{Change: "CREATED", Keyspace: "other", Table: "x"})
	select {
	case e := <-sub.Events():
		t.Fatal("keyspace filter ignored", e)
	default:
	}
	s.publishV2Event(&oldevents.SchemaChangeTableEvent{Change: "CREATED", Keyspace: "ks", Table: "a"})
	s.publishV2Event(&oldevents.SchemaChangeTableEvent{Change: "CREATED", Keyspace: "ks", Table: "b"})
	event := <-sub.Events()
	if event.Kind != OverflowEvent || event.Change != "RESYNC" || event.Generation != s.Metadata().Generation() {
		t.Fatal(event)
	}
	s.publishV2Event(&oldevents.SchemaChangeTableEvent{Change: "DROPPED", Keyspace: "ks", Table: "b"})
	if event := <-sub.Events(); event.Kind != SchemaEvent || event.Object != "b" {
		t.Fatal(event)
	}
	sub.Close()
	sub.Close()
	if _, ok := <-sub.Events(); ok {
		t.Fatal("closed subscription not closed")
	}
	s.closeV2Peripheral()
	if _, err := s.Subscribe(EventFilter{}); err == nil {
		t.Fatal("subscribe after close succeeded")
	}
}
func TestV2SubscriptionClosePublishRace(t *testing.T) {
	s := &Session{}
	sub, err := s.Subscribe(EventFilter{Capacity: 1})
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			s.publishV2Event(&oldevents.SchemaChangeKeyspaceEvent{Keyspace: "k"})
		}
	}()
	go func() { defer wg.Done(); sub.Close(); s.closeV2Peripheral() }()
	wg.Wait()
}
