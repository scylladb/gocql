//go:build unit
// +build unit

package gocql

import (
	"net"
	"testing"
	"time"

	"github.com/gocql/gocql/events"
	"github.com/gocql/gocql/internal/eventbus"
)

func TestSessionEventBusPublishesEvents(t *testing.T) {
	s := &Session{
		eventBus: eventbus.New[events.Event](eventbus.EventBusConfig{
			InputEventsQueueSize: 1,
		}, nil),
		logger: &nopLogger{},
	}

	if err := s.eventBus.Start(); err != nil {
		t.Fatalf("starting event bus: %v", err)
	}
	defer s.eventBus.Stop()

	sub := s.SubscribeToEvents("test", 1, nil)
	defer sub.Stop()

	ev := &events.StatusChangeEvent{
		Change: "UP",
		Host:   net.ParseIP("127.0.0.1"),
		Port:   9042,
	}

	s.publishEvent(ev)

	select {
	case received := <-sub.Events():
		if received != ev {
			t.Fatalf("unexpected event pointer: got %p want %p", received, ev)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for event")
	}
}
