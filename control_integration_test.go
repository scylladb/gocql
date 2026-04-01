//go:build integration
// +build integration

package gocql

import (
	"context"
	"fmt"
	"net"
	"testing"
)

// unixSocketDialer is a special dialer which connects only to the maintenance_socket.
type unixSocketDialer struct {
	dialer     net.Dialer
	socketPath string
}

func (d unixSocketDialer) DialContext(_ context.Context, _, _ string) (net.Conn, error) {
	return d.dialer.Dial("unix", d.socketPath)
}

func TestUnixSockets(t *testing.T) {
	socketFiles := getClusterSocketFile()
	if len(socketFiles) == 0 {
		t.Skip("this test needs path to socket file provided into -cluster-socket cli option")
	}

	c := createCluster()
	c.NumConns = 1
	c.DisableInitialHostLookup = true
	c.ProtoVersion = protoVersion3
	c.ReconnectInterval = 0
	c.WriteCoalesceWaitTime = 0

	c.Events.DisableNodeStatusEvents = true
	c.Events.DisableTopologyEvents = true
	c.Events.DisableSchemaEvents = true

	d := net.Dialer{
		Timeout: c.Timeout,
	}
	if c.SocketKeepalive > 0 {
		d.KeepAlive = c.SocketKeepalive
	}

	c.Dialer = unixSocketDialer{
		dialer:     d,
		socketPath: socketFiles[0],
	}

	sess, err := c.CreateSession()
	if err != nil {
		t.Fatalf("unable to create session: %v", err)
	}

	defer sess.Close()

	keyspace := testKeyspaceName(t)

	err = createTable(sess, `DROP KEYSPACE IF EXISTS `+keyspace)
	if err != nil {
		t.Fatal("unable to drop keyspace if exists:", err)
	}

	err = createTable(sess, fmt.Sprintf(`CREATE KEYSPACE %s
	WITH replication = {
		'class' : 'NetworkTopologyStrategy',
		'replication_factor' : 1
	}`, keyspace))
	if err != nil {
		t.Fatal("unable to create keyspace:", err)
	}
}
