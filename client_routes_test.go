//go:build integration
// +build integration

package gocql

import (
	"fmt"
	"net"
	"sort"
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestGetHostPortMapping(t *testing.T) {
	session := createSession(t)
	createKeyspace(t, createCluster(), "gocql_test", true)
	defer session.Close()

	if err := createTable(session, `CREATE TABLE gocql_test.client_routes (
    connection_id uuid,
    host_id uuid,
    Address text,
    port int,
    tls_port int,
    alternator_port int,
    alternator_https_port int,
    Datacenter text,
    Rack text,
    PRIMARY KEY (connection_id, host_id))`); err != nil {
		t.Fatal(err)
	}

	var hostIDs []string
	for i := 0; i < 3; i++ {
		hostIDs = append(hostIDs, MustRandomUUID().String())
	}
	var connectionIDs []string
	for i := 0; i < 3; i++ {
		connectionIDs = append(connectionIDs, MustRandomUUID().String())
	}

	racks := []string{"rack1", "rack2", "rack3"}
	expected := []UnresolvedClientRoute{}
	for id, hostID := range hostIDs {
		rack := racks[id]
		ip := net.ParseIP(fmt.Sprintf("127.0.0.%d", id+1))
		for _, connectionID := range connectionIDs {
			err := session.Query(
				`INSERT INTO gocql_test.client_routes (
                                            connection_id, host_id, Address, port, tls_port, alternator_port, alternator_https_port, Datacenter, Rack) 
						VALUES (?, ?, ?, 9042, 9142, 0, 0, 'dc1', ?);`,
				connectionID, hostID, ip.String(), rack,
			).Exec()
			if err != nil {
				t.Fatalf("unable to insert connection metadata: %s", err.Error())
			}
			expected = append(expected, UnresolvedClientRoute{
				ConnectionID:  connectionID,
				HostID:        hostID,
				Address:       ip.String(),
				CQLPort:       9042,
				SecureCQLPort: 9142,
			})
		}
	}

	sortUnresolvedHostPorts(expected)

	tcases := []struct {
		name     string
		method   func(controlConnection) ([]UnresolvedClientRoute, error)
		expected []UnresolvedClientRoute
	}{
		{
			name: "get-all",
			method: func(controlConnection) ([]UnresolvedClientRoute, error) {
				return getHostPortMappingFromCluster(session.control, "gocql_test.client_routes", nil, nil)
			},
			expected: expected,
		},
		{
			name: "get-all-hosts",
			method: func(controlConnection) ([]UnresolvedClientRoute, error) {
				return getHostPortMappingFromCluster(session.control, "gocql_test.client_routes", connectionIDs, nil)
			},
			expected: expected,
		},
		{
			name: "get-all-connections",
			method: func(controlConnection) ([]UnresolvedClientRoute, error) {
				return getHostPortMappingFromCluster(session.control, "gocql_test.client_routes", nil, hostIDs)
			},
			expected: expected,
		},
		{
			name: "get-concrete",
			method: func(controlConnection) ([]UnresolvedClientRoute, error) {
				return getHostPortMappingFromCluster(session.control, "gocql_test.client_routes", connectionIDs, hostIDs)
			},
			expected: expected,
		},
		{
			name: "get-concrete-host",
			method: func(controlConnection) ([]UnresolvedClientRoute, error) {
				return getHostPortMappingFromCluster(session.control, "gocql_test.client_routes", connectionIDs, hostIDs)
			},
			expected: expected,
		},
	}

	for _, tc := range tcases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := tc.method(session.control)
			if err != nil {
				t.Fatal(err)
			}

			sortUnresolvedHostPorts(got)

			if diff := cmp.Diff(got, tc.expected); diff != "" {
				t.Errorf("got unexpected result: %s", diff)
			}
		})
	}
}

func sortUnresolvedHostPorts(xs []UnresolvedClientRoute) {
	sort.Slice(xs, func(i, j int) bool {
		a, b := xs[i], xs[j]

		if a.ConnectionID != b.ConnectionID {
			return a.ConnectionID < b.ConnectionID // or bytes.Compare if raw [16]byte
		}
		return a.HostID < b.HostID
	})
}
