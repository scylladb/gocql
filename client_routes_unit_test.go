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
	m := clientRouteMap{
		"h1": {"c1": {connectionID: "c1", hostID: "h1", address: "a1", cqlPort: 9042}},
	}

	// Same record: no change expected.
	m.merge([]clientRoute{
		{connectionID: "c1", hostID: "h1", address: "a1", cqlPort: 9042},
	}, []string{"c1"}, nil)
	if len(m) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(m))
	}

	// Updated address: record should be replaced.
	m.merge([]clientRoute{
		{connectionID: "c1", hostID: "h1", address: "a2", cqlPort: 9043},
	}, []string{"c1"}, nil)
	rec := m["h1"]["c1"]
	if rec.address != "a2" || rec.cqlPort != 9043 {
		t.Fatalf("expected record to update")
	}

	// New record: should be added.
	m = make(clientRouteMap)
	m.merge([]clientRoute{
		{connectionID: "c2", hostID: "h2", address: "a3", cqlPort: 9044},
	}, []string{"c2"}, nil)
	if len(m) != 1 {
		t.Fatalf("expected new record to be added")
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
		routes:   make(clientRouteMap),
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

	handler.routes = clientRouteMap{
		"h1": {"c1": {connectionID: "c1", hostID: "h1", cqlPort: 9042, secureCQLPort: 9142}},
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
			routes: clientRouteMap{"h2": {"c2": {connectionID: "c2", hostID: "h2", address: "host", cqlPort: 9042}}},
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

func TestMerge_DeletedHost(t *testing.T) {
	m := clientRouteMap{
		"h1": {"c1": {connectionID: "c1", hostID: "h1", address: "a1", cqlPort: 9042}},
		"h2": {"c1": {connectionID: "c1", hostID: "h2", address: "a2", cqlPort: 9042}},
	}

	// Simulate event for (c1, h1) where query returned nothing → (c1,h1) should be removed.
	m.merge(nil, []string{"c1"}, []string{"h1"})

	if len(m) != 1 {
		t.Fatalf("expected 1 entry after pruning deleted host, got %d", len(m))
	}
	if _, ok := m["h2"]; !ok {
		t.Fatalf("expected h2 to survive")
	}
}

func TestMerge_UpdatedHost(t *testing.T) {
	m := clientRouteMap{
		"h1": {
			"c1": {connectionID: "c1", hostID: "h1", address: "old-addr", cqlPort: 9042},
			"c2": {connectionID: "c2", hostID: "h1", address: "old-addr2", cqlPort: 9042},
		},
		"h2": {"c1": {connectionID: "c1", hostID: "h2", address: "keep", cqlPort: 9042}},
	}

	// h1 address changed; fresh query returns new data for h1. h2 is not affected.
	m.merge([]clientRoute{
		{connectionID: "c1", hostID: "h1", address: "new-addr", cqlPort: 9043},
		{connectionID: "c2", hostID: "h1", address: "new-addr2", cqlPort: 9043},
	}, []string{"c1", "c2"}, []string{"h1"})

	if len(m) != 2 || len(m["h1"]) != 2 {
		t.Fatalf("expected 2 hosts with h1 having 2 connections, got %d hosts", len(m))
	}
	if r := m["h1"]["c1"]; r.address != "new-addr" {
		t.Fatalf("expected c1/h1 to have new address, got %s", r.address)
	}
	if r := m["h1"]["c2"]; r.address != "new-addr2" {
		t.Fatalf("expected c2/h1 to have new address, got %s", r.address)
	}
	if r := m["h2"]["c1"]; r.address != "keep" {
		t.Fatalf("expected h2 entry to be preserved unchanged")
	}
}

func TestMerge_FullRefresh_PrunesAllStale(t *testing.T) {
	m := clientRouteMap{
		"h1": {"c1": {connectionID: "c1", hostID: "h1", address: "a1", cqlPort: 9042}},
		"h2": {"c1": {connectionID: "c1", hostID: "h2", address: "a2", cqlPort: 9042}},
		"h3": {"c1": {connectionID: "c1", hostID: "h3", address: "a3", cqlPort: 9042}},
	}

	// Full refresh for connection c1: all entries for c1 are pruned, only h1 and h2 returned.
	m.merge([]clientRoute{
		{connectionID: "c1", hostID: "h1", address: "a1", cqlPort: 9042},
		{connectionID: "c1", hostID: "h2", address: "a2", cqlPort: 9042},
	}, []string{"c1"}, nil)

	if len(m) != 2 {
		t.Fatalf("expected 2 entries after full refresh prune, got %d", len(m))
	}
	if _, ok := m["h3"]; ok {
		t.Fatalf("expected h3 to be pruned")
}
}

func TestTranslateHost_ConnectionAddrOverride(t *testing.T) {
	addr := AddressPort{Address: net.ParseIP("1.1.1.1"), Port: 9042}

	resolvedIPs := map[string]net.IP{
		"route-addr":    net.ParseIP("10.0.0.1"),
		"override-addr": net.ParseIP("10.0.0.99"),
	}
	resolver := dnsResolverFunc(func(host string) ([]net.IP, error) {
		if ip, ok := resolvedIPs[host]; ok {
			return []net.IP{ip}, nil
		}
		return nil, fmt.Errorf("unknown host %s", host)
	})
	routes := clientRouteMap{
		"h1": {"c1": {connectionID: "c1", hostID: "h1", address: "route-addr", cqlPort: 9042}},
	}

	t.Run("no override uses route address", func(t *testing.T) {
		handler := &ClientRoutesHandler{
			addrOverrides: map[string]string{},
			resolver:      resolver,
			routes:        routes,
		}

		res, err := handler.TranslateHost(testHostInfo{hostID: "h1"}, addr)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !res.Address.Equal(resolvedIPs["route-addr"]) {
			t.Fatalf("expected route-addr IP %v, got %v", resolvedIPs["route-addr"], res.Address)
		}
	})

	t.Run("override replaces route address", func(t *testing.T) {
		handler := &ClientRoutesHandler{
			addrOverrides: map[string]string{"c1": "override-addr"},
			resolver:      resolver,
			routes:        routes,
		}

		res, err := handler.TranslateHost(testHostInfo{hostID: "h1"}, addr)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !res.Address.Equal(resolvedIPs["override-addr"]) {
			t.Fatalf("expected override-addr IP %v, got %v", resolvedIPs["override-addr"], res.Address)
		}
	})
}

// TestUpdateHostPortMapping_FullRefresh_PrunesStaleEntries simulates the same
// sequence of operations that updateHostPortMapping performs (lock → Merge → unlock)
// to verify that a full refresh correctly prunes a host that disappeared.
func TestUpdateHostPortMapping_FullRefresh_PrunesStaleEntries(t *testing.T) {
	// Existing routes: h1, h2, h3.
	routes := clientRouteMap{
		"h1": {"c1": {connectionID: "c1", hostID: "h1", address: "a1", cqlPort: 9042}},
		"h2": {"c1": {connectionID: "c1", hostID: "h2", address: "a2", cqlPort: 9042}},
		"h3": {"c1": {connectionID: "c1", hostID: "h3", address: "a3", cqlPort: 9042}},
	}

	// Cluster now returns only h1 and h2 (h3 was decommissioned).
	incoming := []clientRoute{
		{connectionID: "c1", hostID: "h1", address: "a1", cqlPort: 9042},
		{connectionID: "c1", hostID: "h2", address: "a2", cqlPort: 9042},
	}

	routes.merge(incoming, []string{"c1"}, nil)

	if len(routes) != 2 {
		t.Fatalf("expected 2 entries after full-refresh prune, got %d", len(routes))
	}
	if _, ok := routes["h3"]; ok {
		t.Fatalf("h3 should have been pruned by full refresh")
	}
}
