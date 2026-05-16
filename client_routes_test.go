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
	t.Parallel()

	keyspace := testKeyspaceName(t)
	cluster := createCluster()
	createKeyspace(t, cluster, keyspace, true)

	cluster.Keyspace = keyspace
	session, err := cluster.CreateSession()
	if err != nil {
		t.Fatalf("failed to create session: %v", err)
	}
	defer session.Close()

	table := testTableName(t)
	qualifiedTable := keyspace + "." + table

	if err := createTable(session, fmt.Sprintf(`CREATE TABLE %s.%s (
    connection_id uuid,
    host_id uuid,
    Address text,
    port int,
    tls_port int,
    alternator_port int,
    alternator_https_port int,
    Datacenter text,
    Rack text,
    PRIMARY KEY (connection_id, host_id))`, keyspace, table)); err != nil {
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
	var expected []clientRoute
	var expectedTLS []clientRoute
	for id, hostID := range hostIDs {
		rack := racks[id]
		ip := net.ParseIP(fmt.Sprintf("127.0.0.%d", id+1))
		for _, connectionID := range connectionIDs {
			err := session.Query(
				fmt.Sprintf(`INSERT INTO %s (
                                            connection_id, host_id, Address, port, tls_port, alternator_port, alternator_https_port, Datacenter, Rack)
						VALUES (?, ?, ?, 9042, 9142, 0, 0, 'dc1', ?);`, qualifiedTable),
				connectionID, hostID, ip.String(), rack,
			).Exec()
			if err != nil {
				t.Fatalf("unable to insert connection metadata: %s", err.Error())
			}
			expected = append(expected, clientRoute{
				connectionID: connectionID,
				hostID:       hostID,
				address:      ip.String(),
				port:         9042,
			})
			expectedTLS = append(expectedTLS, clientRoute{
				connectionID: connectionID,
				hostID:       hostID,
				address:      ip.String(),
				port:         9142,
			})
		}
	}

	sortClientRoutes(expected)
	sortClientRoutes(expectedTLS)
	expectedFirstHost := filterClientRoutesByHostID(expected, hostIDs[0])

	tcases := []struct {
		name     string
		method   func(controlConnection) ([]clientRoute, error)
		expected []clientRoute
	}{
		{
			name: "get-for-connection-ids",
			method: func(controlConnection) ([]clientRoute, error) {
				return getHostPortMappingForConnectionIDs(session.control, qualifiedTable, connectionIDs, false)
			},
			expected: expected,
		},
		{
			name: "get-for-pairs",
			method: func(controlConnection) ([]clientRoute, error) {
				return getHostPortMappingForPairs(session.control, qualifiedTable, makeClientRoutePairs(connectionIDs, hostIDs), false)
			},
			expected: expected,
		},
		{
			name: "get-for-single-host-pairs",
			method: func(controlConnection) ([]clientRoute, error) {
				return getHostPortMappingForPairs(session.control, qualifiedTable, makeClientRoutePairs(connectionIDs, hostIDs[:1]), false)
			},
			expected: expectedFirstHost,
		},
		{
			name: "get-for-connection-ids-tls",
			method: func(controlConnection) ([]clientRoute, error) {
				return getHostPortMappingForConnectionIDs(session.control, qualifiedTable, connectionIDs, true)
			},
			expected: expectedTLS,
		},
	}

	for _, tc := range tcases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := tc.method(session.control)
			if err != nil {
				t.Fatal(err)
			}

			sortClientRoutes(got)

			if diff := cmp.Diff(got, tc.expected, cmp.AllowUnexported(clientRoute{})); diff != "" {
				t.Errorf("got unexpected result: %s", diff)
			}
		})
	}
}

func makeClientRoutePairs(connectionIDs, hostIDs []string) []pair {
	pairs := make([]pair, 0, len(connectionIDs)*len(hostIDs))
	for _, connectionID := range connectionIDs {
		for _, hostID := range hostIDs {
			pairs = append(pairs, pair{connectionID: connectionID, hostID: hostID})
		}
	}
	return pairs
}

func filterClientRoutesByHostID(routes []clientRoute, hostID string) []clientRoute {
	var filtered []clientRoute
	for _, route := range routes {
		if route.hostID == hostID {
			filtered = append(filtered, route)
		}
	}
	return filtered
}

func sortClientRoutes(xs []clientRoute) {
	sort.Slice(xs, func(i, j int) bool {
		a, b := xs[i], xs[j]

		if a.connectionID != b.connectionID {
			return a.connectionID < b.connectionID // or bytes.Compare if raw [16]byte
		}
		return a.hostID < b.hostID
	})
}
