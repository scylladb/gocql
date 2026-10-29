//go:build integration
// +build integration

package gocql

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"
)

// unixSocketDialer is a special dialer which connects only to the maintenance_socket.
type unixSocketDialer struct {
	dialer     net.Dialer
	socketPath string
}

func (d unixSocketDialer) DialContext(ctx context.Context, target DialTarget) (net.Conn, error) {
	return d.dialer.DialContext(ctx, target.Network, target.Address)
}

func TestUnixSockets(t *testing.T) {
	t.Parallel()

	socketFiles := getClusterSocketFile()
	if len(socketFiles) == 0 {
		t.Skip("this test needs path to socket file provided into -cluster-socket cli option")
	}

	c := createV2Cluster()
	// The maintenance socket carries plaintext CQL even when TCP clients use TLS.
	c.TLS = nil
	c.TLSForHost = nil
	c.Pool.ConnectionsPerHost = 1
	c.Metadata.InitialDiscovery = false
	c.Protocol = protoVersion3
	c.Reconnect = unixSocketNoReconnect{}
	c.CoalesceWait = 0

	c.Metadata.StatusEvents = false
	c.Metadata.TopologyEvents = false
	c.Metadata.SchemaEvents = false

	c.ContactPoints = []string{"unix:" + socketFiles[0]}
	c.Dialer = DialerFunc(func(ctx context.Context, target DialTarget) (net.Conn, error) {
		return (&net.Dialer{Timeout: c.ConnectTimeout}).DialContext(ctx, target.Network, target.Address)
	})

	sess, err := Connect(context.Background(), c)
	if err != nil {
		t.Fatalf("unable to create session: %v", err)
	}

	defer sess.Close()

	keyspace := testKeyspaceName(t)

	err = v2CreateTable(context.Background(), sess, `DROP KEYSPACE IF EXISTS `+keyspace)
	if err != nil {
		t.Fatal("unable to drop keyspace if exists:", err)
	}

	err = v2CreateTable(context.Background(), sess, fmt.Sprintf(`CREATE KEYSPACE %s
	WITH replication = {
		'class' : 'NetworkTopologyStrategy',
		'replication_factor' : 1
	}`, keyspace))
	if err != nil {
		t.Fatal("unable to create keyspace:", err)
	}
}

type unixSocketNoReconnect struct{}

func (unixSocketNoReconnect) Delay(int) (time.Duration, bool) { return 0, false }
