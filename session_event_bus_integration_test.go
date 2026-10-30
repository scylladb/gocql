//go:build integration
// +build integration

package gocql

import implCluster "github.com/scylladb/gocql/v2/internal/cluster"

import (
	"context"
	"fmt"
	"testing"
	"time"
)

// WARNING: This test must NOT use t.Parallel(). It listens for schema events
// and concurrent DDL from parallel tests could cause spurious matches.
//
//nolint:paralleltest // listens for schema events from the global control connection
func TestSessionEventBusReceivesSchemaChangeEvent(t *testing.T) {
	cluster := createV2Cluster()
	cluster.Metadata.SchemaEvents = true

	sess, err := Connect(context.Background(), cluster)
	if err != nil {
		t.Fatalf("unable to create session: %v", err)
	}
	defer sess.Close()

	keyspace := fmt.Sprintf("eventbus_schema_%d", time.Now().UnixNano())

	// Filter events to the specific keyspace this test creates, so that
	// concurrent DDL from parallel tests does not cause spurious matches.
	sub, err := sess.Subscribe(EventFilter{Kinds: []EventKind{SchemaEvent}, Keyspace: keyspace, Capacity: 10})
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()

	createStmt := fmt.Sprintf(`CREATE KEYSPACE %s WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}`, keyspace)
	if _, err := v2Exec(context.Background(), sess, createStmt); err != nil {
		t.Fatalf("create keyspace: %v", err)
	}
	defer func() {
		if _, err := v2Exec(context.Background(), sess, "DROP KEYSPACE "+keyspace); err != nil {
			t.Error(err)
		}
	}()

	select {
	case ev := <-sub.Events():
		if ev.Kind != SchemaEvent || ev.Target != "KEYSPACE" || ev.Keyspace != keyspace || ev.Change != "CREATED" {
			t.Fatalf("unexpected schema event: %+v", ev)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("timeout waiting for schema change event")
	}
}

func TestSessionEventBusReceivesControlReconnectEvent(t *testing.T) {
	t.Parallel()

	cluster := createV2Cluster()
	cluster.Metadata.TopologyEvents = false
	cluster.Metadata.StatusEvents = false

	sess, err := Connect(context.Background(), cluster)
	if err != nil {
		t.Fatalf("unable to create session: %v", err)
	}
	defer sess.Close()

	sub, err := sess.Subscribe(EventFilter{Kinds: []EventKind{ControlEvent}, Capacity: 10})
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()

	if err := implCluster.SessionData(sess).InternalControl.InternalReconnect(); err != nil {
		t.Fatalf("control reconnect: %v", err)
	}

	select {
	case ev := <-sub.Events():
		if ev.Kind != ControlEvent || ev.Change != "RECONNECTED" || ev.HostID.IsEmpty() || !ev.Address.IsValid() {
			t.Fatalf("unexpected reconnect event: %+v", ev)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("timeout waiting for control reconnect event")
	}
}
