//go:build integration
// +build integration

package gocql

import (
	"fmt"
	"testing"
	"time"

	"github.com/gocql/gocql/events"
)

// WARNING: This test must NOT use t.Parallel(). It listens for schema events
// and concurrent DDL from parallel tests could cause spurious matches.
//
//nolint:paralleltest // listens for schema events from the global control connection
func TestSessionEventBusReceivesSchemaChangeEvent(t *testing.T) {
	cluster := createCluster()
	cluster.Events.DisableSchemaEvents = false

	sess, err := cluster.CreateSession()
	if err != nil {
		t.Fatalf("unable to create session: %v", err)
	}
	defer sess.Close()

	keyspace := fmt.Sprintf("eventbus_schema_%d", time.Now().UnixNano())

	// Filter events to the specific keyspace this test creates, so that
	// concurrent DDL from parallel tests does not cause spurious matches.
	sub := sess.SubscribeToEvents("schema-event", 10, func(ev events.Event) bool {
		if ks, ok := ev.(*events.SchemaChangeKeyspaceEvent); ok {
			return ks.Keyspace == keyspace
		}
		return false
	})
	defer sub.Stop()

	createStmt := fmt.Sprintf(`CREATE KEYSPACE %s WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}`, keyspace)
	if err := sess.Query(createStmt).Exec(); err != nil {
		t.Fatalf("create keyspace: %v", err)
	}
	defer sess.Query("DROP KEYSPACE " + keyspace).Exec()

	select {
	case ev := <-sub.Events():
		if _, ok := ev.(*events.SchemaChangeKeyspaceEvent); !ok {
			t.Fatalf("unexpected event type: %T", ev)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("timeout waiting for schema change event")
	}
}

func TestSessionEventBusReceivesControlReconnectEvent(t *testing.T) {
	t.Parallel()

	cluster := createCluster()
	cluster.Events.DisableTopologyEvents = true
	cluster.Events.DisableNodeStatusEvents = true

	sess, err := cluster.CreateSession()
	if err != nil {
		t.Fatalf("unable to create session: %v", err)
	}
	defer sess.Close()

	sub := sess.SubscribeToEvents("control-reconnect", 10, func(ev events.Event) bool {
		return ev.Type() == events.SessionEventTypeControlConnectionRecreated
	})
	defer sub.Stop()

	if err := sess.control.reconnect(); err != nil {
		t.Fatalf("control reconnect: %v", err)
	}

	select {
	case ev := <-sub.Events():
		if _, ok := ev.(*events.ControlConnectionRecreatedEvent); !ok {
			t.Fatalf("unexpected event type: %T", ev)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("timeout waiting for control reconnect event")
	}
}
