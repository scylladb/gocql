//go:build unit
// +build unit

package gocql

import (
	"errors"
	"fmt"
	"net"
	"testing"
)

// dnsResolverFunc adapts a function to the DNSResolver interface.
type dnsResolverFunc func(host string) ([]net.IP, error)

func (f dnsResolverFunc) LookupIP(host string) ([]net.IP, error) {
	return f(host)
}

type fakeControlConn struct {
	statement string
	values    []any
}

func (f *fakeControlConn) getConn() *connHost          { return nil }
func (f *fakeControlConn) awaitSchemaAgreement() error { return nil }
func (f *fakeControlConn) query(statement string, values ...any) *Iter {
	f.statement = statement
	f.values = values
	return &Iter{}
}
func (f *fakeControlConn) querySystem(statement string, values ...any) *Iter {
	return &Iter{}
}
func (f *fakeControlConn) discoverProtocol(hosts []*HostInfo) (int, error) { return 0, nil }
func (f *fakeControlConn) connect(hosts []*HostInfo) error                 { return nil }
func (f *fakeControlConn) close()                                          {}
func (f *fakeControlConn) getSession() *Session                            { return nil }
func (f *fakeControlConn) reconnect() error                                { return nil }

type testHostInfo struct {
	hostID string
}

func (t testHostInfo) HostID() string                     { return t.hostID }
func (t testHostInfo) Rack() string                       { return "" }
func (t testHostInfo) DataCenter() string                 { return "" }
func (t testHostInfo) BroadcastAddress() net.IP           { return nil }
func (t testHostInfo) ListenAddress() net.IP              { return nil }
func (t testHostInfo) RPCAddress() net.IP                 { return nil }
func (t testHostInfo) PreferredIP() net.IP                { return nil }
func (t testHostInfo) Peer() net.IP                       { return nil }
func (t testHostInfo) UntranslatedConnectAddress() net.IP { return nil }
func (t testHostInfo) Port() int                          { return 0 }
func (t testHostInfo) Partitioner() string                { return "" }
func (t testHostInfo) ClusterName() string                { return "" }
func (t testHostInfo) ScyllaShardAwarePort() uint16       { return 0 }
func (t testHostInfo) ScyllaShardAwarePortTLS() uint16    { return 0 }
func (t testHostInfo) ScyllaShardCount() int              { return 0 }

func TestMerge(t *testing.T) {
	list := clientRouteList{
		{ConnectionID: "c1", HostID: "h1", Address: "a1", CQLPort: 9042},
	}

	// Same record: no change expected.
	list.Merge(clientRouteList{
		{ConnectionID: "c1", HostID: "h1", Address: "a1", CQLPort: 9042},
	})
	if len(list) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(list))
	}

	// Updated address: record should be replaced.
	list.Merge(clientRouteList{
		{ConnectionID: "c1", HostID: "h1", Address: "a2", CQLPort: 9043},
	})
	if list[0].Address != "a2" || list[0].CQLPort != 9043 {
		t.Fatalf("expected record to update")
	}

	// New record: should be appended.
	list = clientRouteList{}
	list.Merge(clientRouteList{
		{ConnectionID: "c2", HostID: "h2", Address: "a3", CQLPort: 9044},
	})
	if len(list) != 1 {
		t.Fatalf("expected new record to be appended")
	}
}

func TestFindByHostID(t *testing.T) {
	list := clientRouteList{
		{ConnectionID: "c1", HostID: "h1"},
		{ConnectionID: "c1", HostID: "h2"},
	}

	rec := list.FindByHostID("h1")
	if rec == nil {
		t.Fatalf("expected FindByHostID to locate record")
	}
	rec.ConnectionID = "updated"
	if list[0].ConnectionID != "updated" {
		t.Fatalf("expected FindByHostID to return pointer to list element")
	}

	if list.FindByHostID("h3") != nil {
		t.Fatalf("expected nil for missing host")
	}
}

func TestClientRoutesHandlerTranslateHost(t *testing.T) {
	addr := AddressPort{Address: net.ParseIP("1.1.1.1"), Port: 9042}
	noHost := testHostInfo{hostID: ""}
	missingHost := testHostInfo{hostID: "missing"}

	resolver := dnsResolverFunc(func(host string) ([]net.IP, error) {
		return []net.IP{net.ParseIP("10.0.0.1")}, nil
	})

	handler := &ClientRoutesHandler{
		resolver: resolver,
		routes:   make(clientRouteList, 0),
	}

	res, err := handler.TranslateHost(noHost, addr)
	if err != nil {
		t.Fatalf("unexpected error for empty hostID: %v", err)
	}
	if !res.Equal(addr) {
		t.Fatalf("expected address to pass through when hostID is empty")
	}

	_, err = handler.TranslateHost(missingHost, addr)
	if err == nil {
		t.Fatalf("expected error for missing host entry")
	}

	handler.routes = clientRouteList{
		{ConnectionID: "c1", HostID: "h1", CQLPort: 9042, SecureCQLPort: 9142},
	}

	handler.pickTLSPorts = false
	res, err = handler.TranslateHost(testHostInfo{hostID: "h1"}, addr)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.Port != 9042 {
		t.Fatalf("expected non-TLS port, got %d", res.Port)
	}

	handler.pickTLSPorts = true
	res, err = handler.TranslateHost(testHostInfo{hostID: "h1"}, addr)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.Port != 9142 {
		t.Fatalf("expected TLS port, got %d", res.Port)
	}

	errorHandler := &ClientRoutesHandler{
		resolver: dnsResolverFunc(func(host string) ([]net.IP, error) {
			return nil, errors.New("lookup failed")
		}),
		routes: clientRouteList{{ConnectionID: "c2", HostID: "h2", Address: "host", CQLPort: 9042}},
	}
	_, err = errorHandler.TranslateHost(testHostInfo{hostID: "h2"}, addr)
	if err == nil {
		t.Fatalf("expected resolver error to bubble up")
	}
}

func TestGetHostPortMappingFromClusterQuery(t *testing.T) {
	tcases := []struct {
		name          string
		connectionIDs []string
		hostIDs       []string
		expectedStmt  string
		expectedVals  []any
	}{
		{
			name:         "all",
			expectedStmt: "select connection_id, host_id, address, port, tls_port from system.client_routes allow filtering",
		},
		{
			name:          "connections-only",
			connectionIDs: []string{"c1", "c2"},
			expectedStmt:  "select connection_id, host_id, address, port, tls_port from system.client_routes where connection_id in (?,?) allow filtering",
			expectedVals:  []any{"c1", "c2"},
		},
		{
			name:         "hosts-only",
			hostIDs:      []string{"h1"},
			expectedStmt: "select connection_id, host_id, address, port, tls_port from system.client_routes where host_id in (?) allow filtering",
			expectedVals: []any{"h1"},
		},
		{
			name:          "connections-and-hosts",
			connectionIDs: []string{"c1"},
			hostIDs:       []string{"h1", "h2"},
			expectedStmt:  "select connection_id, host_id, address, port, tls_port from system.client_routes where connection_id in (?) and host_id in (?,?)",
			expectedVals:  []any{"c1", "h1", "h2"},
		},
		{
			name:          "empty-slices",
			connectionIDs: []string{},
			hostIDs:       []string{},
			expectedStmt:  "select connection_id, host_id, address, port, tls_port from system.client_routes allow filtering",
		},
	}

	for _, tc := range tcases {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := &fakeControlConn{}
			_, err := getHostPortMappingFromCluster(ctrl, "system.client_routes", tc.connectionIDs, tc.hostIDs)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if ctrl.statement != tc.expectedStmt {
				t.Fatalf("statement mismatch: got %q want %q", ctrl.statement, tc.expectedStmt)
			}
			if fmt.Sprint(ctrl.values) != fmt.Sprint(tc.expectedVals) {
				t.Fatalf("values mismatch: got %v want %v", ctrl.values, tc.expectedVals)
			}
		})
	}
}

// TestUpdateHostPortMapping_FullRefresh_PrunesStaleEntries simulates the same
// sequence of operations that updateHostPortMapping performs (lock → Merge → unlock)
// to verify that a full refresh correctly prunes a host that disappeared.
