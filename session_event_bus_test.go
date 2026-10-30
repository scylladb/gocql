//go:build unit
// +build unit

package gocql

import implCluster "github.com/scylladb/gocql/v2/internal/cluster"

import (
	"net"
	"testing"
	"time"

	"github.com/scylladb/gocql/v2/events"
	"github.com/scylladb/gocql/v2/internal/eventbus"
)

func TestSessionEventBusPublishesEvents(t *testing.T) {
	s := func() *Session {
		opaqueStorageValue := new(Session)
		implCluster.SessionData(opaqueStorageValue).InternalEventBus = eventbus.New[events.Event](eventbus.EventBusConfig{
			InputEventsQueueSize: 1,
		}, nil)
		implCluster.SessionData(opaqueStorageValue).InternalLogger = &nopLogger{}
		return opaqueStorageValue
	}()

	if err := implCluster.SessionData(s).InternalEventBus.Start(); err != nil {
		t.Fatalf("starting event bus: %v", err)
	}
	defer implCluster.SessionData(s).InternalEventBus.Stop()

	sub := s.SubscribeToEvents("test", 1, nil)
	defer sub.Stop()

	ev := &events.StatusChangeEvent{
		Change: "UP",
		Host:   net.ParseIP("127.0.0.1"),
		Port:   9042,
	}
	implCluster.InternalSessionPublishEvent(s, ev)

	select {
	case received := <-sub.Events():
		if received != ev {
			t.Fatalf("unexpected event pointer: got %p want %p", received, ev)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for event")
	}
}
