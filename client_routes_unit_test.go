//go:build unit
// +build unit

package gocql

import implCluster "github.com/scylladb/gocql/v2/internal/cluster"

import (
	"errors"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/scylladb/gocql/v2/events"
	"github.com/scylladb/gocql/v2/internal/eventbus"
)

// dnsResolverFunc adapts a function to the DNSResolver interface.
type dnsResolverFunc func(host string) ([]net.IP, error)

func (f dnsResolverFunc) LookupIP(host string) ([]net.IP, error) {
	return f(host)
}

type fakeControlConn struct {
	statement string
	values    []any
	iter      *nativeResultFixture
}

func (f *fakeControlConn) InternalGetConn() *connHost          { return nil }
func (f *fakeControlConn) InternalAwaitSchemaAgreement() error { return nil }
func (f *fakeControlConn) query(statement string, values ...any) *nativeResultFixture {
	f.statement = statement
	f.values = values
	if f.iter != nil {
		return f.iter
	}
	return &nativeResultFixture{}
}
func (f *fakeControlConn) querySystem(statement string, values ...any) *nativeResultFixture {
	return &nativeResultFixture{}
}
func (f *fakeControlConn) InternalDiscoverProtocol(hosts []*HostInfo) (int, error) { return 0, nil }
func (f *fakeControlConn) InternalConnect(hosts []*HostInfo) error                 { return nil }
func (f *fakeControlConn) InternalClose()                                          {}
func (f *fakeControlConn) InternalGetSession() *Session                            { return nil }
func (f *fakeControlConn) InternalReconnect() error                                { return nil }

func newClientRoutesEventHarness(t *testing.T, allowedConnectionIDs []string) (*ClientRoutesHandler, *eventbus.EventBus[events.Event]) {
	t.Helper()

	bus := eventbus.New[events.Event](eventbus.EventBusConfig{InputEventsQueueSize: 1}, nil)
	if err := bus.Start(); err != nil {
		t.Fatalf("starting event bus: %v", err)
	}

	h := func() *ClientRoutesHandler {
		opaqueStorageValue := new(ClientRoutesHandler)
		implCluster.ClientRoutesHandlerData(opaqueStorageValue).InternalSub = bus.Subscribe("port-mux", 1, nil)
		implCluster.ClientRoutesHandlerData(opaqueStorageValue).InternalUpdateTasks = make(chan updateTask, 1)
		implCluster.ClientRoutesHandlerData(opaqueStorageValue).InternalCloseChan = make(chan struct{})
		return opaqueStorageValue
	}()

	implCluster.InternalClientRoutesHandlerStartReadingEvents(h, allowedConnectionIDs)

	t.Cleanup(func() {
		if err := bus.Stop(); err != nil {
			t.Fatalf("stopping event bus: %v", err)
		}
	})

	return h, bus
}

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

func newCacheEntry(routes ...clientRoute) clientRouteCacheEntry {
	return clientRouteCacheEntry{InternalAllRoutes: routes}
}

func newCache(routes map[string]clientRouteCacheEntry) clientRouteCache {
	return clientRouteCache{InternalRoutes: routes}
}

func newCacheEntryWithCurrent(current clientRoute, routes ...clientRoute) clientRouteCacheEntry {
	entry := clientRouteCacheEntry{InternalAllRoutes: routes}
	entry.BindCurrent(current.InternalConnectionID)
	return entry
}

func TestClientRoutesHandlerTranslateHost(t *testing.T) {
	addr := AddressPort{Address: net.ParseIP("1.1.1.1"), Port: 9042}
	noHost := testHostInfo{hostID: ""}
	missingHost := testHostInfo{hostID: "missing"}

	resolver := dnsResolverFunc(func(host string) ([]net.IP, error) {
		return []net.IP{net.ParseIP("10.0.0.1")}, nil
	})

	handler := func() *ClientRoutesHandler {
		opaqueStorageValuex := new(ClientRoutesHandler)
		implCluster.ClientRoutesHandlerData(opaqueStorageValuex).InternalResolver = resolver
		implCluster.ClientRoutesHandlerData(opaqueStorageValuex).InternalRouteCache = newClientRouteCache()
		return opaqueStorageValuex
	}()

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
	implCluster.ClientRoutesHandlerData(handler).InternalRouteCache = newCache(map[string]clientRouteCacheEntry{
		"h1": newCacheEntry(clientRoute{InternalConnectionID: "c1", InternalHostID: "h1", InternalPort: 9042}),
	})

	res, err = handler.TranslateHost(testHostInfo{hostID: "h1"}, addr)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !res.Address.Equal(net.ParseIP("10.0.0.1")) {
		t.Fatalf("expected resolved IP 10.0.0.1, got %v", res.Address)
	}
	if res.Port != 9042 {
		t.Fatalf("expected port 9042, got %d", res.Port)
	}

	errorHandler := func() *ClientRoutesHandler {
		opaqueStorageValuexx := new(ClientRoutesHandler)
		implCluster.ClientRoutesHandlerData(opaqueStorageValuexx).InternalResolver = dnsResolverFunc(func(host string) ([]net.IP, error) {
			return nil, errors.New("lookup failed")
		})
		implCluster.ClientRoutesHandlerData(opaqueStorageValuexx).InternalRouteCache = newCache(map[string]clientRouteCacheEntry{"h2": newCacheEntry(clientRoute{InternalConnectionID: "c2", InternalHostID: "h2", InternalAddress: "host", InternalPort: 9042})})
		return opaqueStorageValuexx
	}()

	_, err = errorHandler.TranslateHost(testHostInfo{hostID: "h2"}, addr)
	if err == nil {
		t.Fatalf("expected resolver error to bubble up")
	}

	// Route with port 0 should return an error before attempting DNS.
	zeroPortHandler := func() *ClientRoutesHandler {
		opaqueStorageValuexxx := new(ClientRoutesHandler)
		implCluster.ClientRoutesHandlerData(opaqueStorageValuexxx).InternalResolver = dnsResolverFunc(func(host string) ([]net.IP, error) {
			t.Fatal("DNS should not be called when port is 0")
			return nil, nil
		})
		implCluster.ClientRoutesHandlerData(opaqueStorageValuexxx).InternalRouteCache = newCache(map[string]clientRouteCacheEntry{"h3": newCacheEntry(clientRoute{InternalConnectionID: "c3", InternalHostID: "h3", InternalAddress: "host", InternalPort: 0})})
		return opaqueStorageValuexxx
	}()

	_, err = zeroPortHandler.TranslateHost(testHostInfo{hostID: "h3"}, addr)
	if err == nil {
		t.Fatalf("expected error for zero port")
	}
}

func TestClientRoutesHandlerTranslateHost_CurrentRoute(t *testing.T) {
	addr := AddressPort{Address: net.ParseIP("1.1.1.1"), Port: 9042}
	resolvedIPs := map[string]net.IP{
		"addr-c1": net.ParseIP("10.0.0.1"),
		"addr-c2": net.ParseIP("10.0.0.2"),
	}
	handler := func() *ClientRoutesHandler {
		opaqueStorageValuexxxx := new(ClientRoutesHandler)
		implCluster.ClientRoutesHandlerData(opaqueStorageValuexxxx).InternalPickTLSPorts = false
		implCluster.ClientRoutesHandlerData(opaqueStorageValuexxxx).InternalResolver = dnsResolverFunc(func(host string) ([]net.IP, error) {
			if ip, ok := resolvedIPs[host]; ok {
				return []net.IP{ip}, nil
			}
			return nil, fmt.Errorf("unknown host %s", host)
		})
		implCluster.ClientRoutesHandlerData(opaqueStorageValuexxxx).InternalRouteCache = newCache(map[string]clientRouteCacheEntry{
			"h1": newCacheEntry(
				clientRoute{InternalConnectionID: "c1", InternalHostID: "h1", InternalAddress: "addr-c1", InternalPort: 9042},
				clientRoute{InternalConnectionID: "c2", InternalHostID: "h1", InternalAddress: "addr-c2", InternalPort: 9042},
			),
		})
		return opaqueStorageValuexxxx

		// First call picks the first route in the record.
	}()

	res1, err := handler.TranslateHost(testHostInfo{hostID: "h1"}, addr)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !res1.Address.Equal(resolvedIPs["addr-c1"]) {
		t.Fatalf("expected first route to resolve addr-c1, got %v", res1.Address)
	}
	implCluster.

		// Refresh the host and replace c1 with c2. The next call should fall back to
		// the remaining route.
		ClientRoutesHandlerData(handler).InternalRouteCache.ReplaceByConnectionIDs([]string{"c1"}, []clientRoute{
		{InternalConnectionID: "c2", InternalHostID: "h1", InternalAddress: "addr-c2", InternalPort: 9042},
	})

	res2, err := handler.TranslateHost(testHostInfo{hostID: "h1"}, addr)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !res2.Address.Equal(resolvedIPs["addr-c2"]) {
		t.Fatalf("expected fallback route to resolve addr-c2, got %v", res2.Address)
	}
}

func TestClientRoutesHandlerTranslateHost_PreservesCurrentAcrossRefresh(t *testing.T) {
	addr := AddressPort{Address: net.ParseIP("1.1.1.1"), Port: 9042}
	resolvedIPs := map[string]net.IP{
		"addr-c1-old": net.ParseIP("10.0.0.1"),
		"addr-c1-new": net.ParseIP("10.0.0.2"),
		"addr-c2":     net.ParseIP("10.0.0.3"),
	}
	resolver := dnsResolverFunc(func(host string) ([]net.IP, error) {
		if ip, ok := resolvedIPs[host]; ok {
			return []net.IP{ip}, nil
		}
		return nil, fmt.Errorf("unknown host %s", host)
	})

	newHandler := func() *ClientRoutesHandler {
		return func() *ClientRoutesHandler {
			opaqueStorageValuexxxxx := new(ClientRoutesHandler)
			implCluster.ClientRoutesHandlerData(opaqueStorageValuexxxxx).InternalResolver = resolver
			implCluster.ClientRoutesHandlerData(opaqueStorageValuexxxxx).InternalRouteCache = newCache(map[string]clientRouteCacheEntry{
				"h1": newCacheEntryWithCurrent(
					clientRoute{InternalConnectionID: "c1", InternalHostID: "h1", InternalAddress: "addr-c1-old", InternalPort: 9042},
					clientRoute{InternalConnectionID: "c1", InternalHostID: "h1", InternalAddress: "addr-c1-old", InternalPort: 9042},
					clientRoute{InternalConnectionID: "c2", InternalHostID: "h1", InternalAddress: "addr-c2", InternalPort: 9042},
				),
			})
			return opaqueStorageValuexxxxx
		}()

	}

	for _, tc := range []struct {
		name    string
		replace func(*clientRouteCache)
	}{
		{
			name: "connection-ids",
			replace: func(c *clientRouteCache) {
				c.ReplaceByConnectionIDs([]string{"c1"}, []clientRoute{
					{InternalConnectionID: "c1", InternalHostID: "h1", InternalAddress: "addr-c1-new", InternalPort: 9042},
				})
			},
		},
		{
			name: "pairs",
			replace: func(c *clientRouteCache) {
				c.ReplaceByPairs([]pair{{InternalConnectionID: "c1", InternalHostID: "h1"}}, []clientRoute{
					{InternalConnectionID: "c1", InternalHostID: "h1", InternalAddress: "addr-c1-new", InternalPort: 9042},
				})
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			handler := newHandler()
			tc.replace(&implCluster.ClientRoutesHandlerData(handler).InternalRouteCache)

			res, err := handler.TranslateHost(testHostInfo{hostID: "h1"}, addr)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if !res.Address.Equal(resolvedIPs["addr-c1-new"]) {
				t.Fatalf("expected preferred route to stay on c1 with updated address, got %v", res.Address)
			}
		})
	}
}

func TestClientRoutesHandlerTranslateHost_PrunesMissingHostAcrossRefresh(t *testing.T) {
	addr := AddressPort{Address: net.ParseIP("1.1.1.1"), Port: 9042}
	resolvedIPs := map[string]net.IP{
		"addr-h1": net.ParseIP("10.0.1.1"),
		"addr-h2": net.ParseIP("10.0.1.2"),
		"addr-h3": net.ParseIP("10.0.1.3"),
	}
	handler := func() *ClientRoutesHandler {
		opaqueStorageValuexxxxxx := new(ClientRoutesHandler)
		implCluster.ClientRoutesHandlerData(opaqueStorageValuexxxxxx).InternalResolver = dnsResolverFunc(func(host string) ([]net.IP, error) {
			if ip, ok := resolvedIPs[host]; ok {
				return []net.IP{ip}, nil
			}
			return nil, fmt.Errorf("unknown host %s", host)
		})
		implCluster.ClientRoutesHandlerData(opaqueStorageValuexxxxxx).InternalRouteCache = newCache(map[string]clientRouteCacheEntry{
			"h1": newCacheEntryWithCurrent(
				clientRoute{InternalConnectionID: "c1", InternalHostID: "h1", InternalAddress: "addr-h1", InternalPort: 9042},
				clientRoute{InternalConnectionID: "c1", InternalHostID: "h1", InternalAddress: "addr-h1", InternalPort: 9042},
			),
			"h2": newCacheEntry(clientRoute{InternalConnectionID: "c1", InternalHostID: "h2", InternalAddress: "addr-h2", InternalPort: 9042}),
			"h3": newCacheEntry(clientRoute{InternalConnectionID: "c1", InternalHostID: "h3", InternalAddress: "addr-h3", InternalPort: 9042}),
		})
		return opaqueStorageValuexxxxxx
	}()

	if _, err := handler.TranslateHost(testHostInfo{hostID: "h3"}, addr); err != nil {
		t.Fatalf("unexpected error before refresh: %v", err)
	}
	implCluster.ClientRoutesHandlerData(handler).InternalRouteCache.ReplaceByConnectionIDs([]string{"c1"}, []clientRoute{
		{InternalConnectionID: "c1", InternalHostID: "h1", InternalAddress: "addr-h1-new", InternalPort: 9042},
		{InternalConnectionID: "c1", InternalHostID: "h2", InternalAddress: "addr-h2", InternalPort: 9042},
	})

	if _, err := handler.TranslateHost(testHostInfo{hostID: "h3"}, addr); err == nil {
		t.Fatal("expected h3 to be pruned after refresh")
	}
}

func TestGetHostPortMappingForConnectionIDsQuery(t *testing.T) {
	tcases := []struct {
		name          string
		connectionIDs []string
		expectedStmt  string
		expectedVals  []any
		expectedErr   bool
	}{
		{
			name:          "multiple-connections",
			connectionIDs: []string{"c1", "c2"},
			expectedStmt:  "select connection_id, host_id, address, port, tls_port from system.client_routes where connection_id in ?",
			expectedVals:  []any{[]string{"c1", "c2"}},
		},
		{
			name:          "single-connection",
			connectionIDs: []string{"c1"},
			expectedStmt:  "select connection_id, host_id, address, port, tls_port from system.client_routes where connection_id in ?",
			expectedVals:  []any{[]string{"c1"}},
		},
		{
			name:        "empty-slices",
			expectedErr: true,
		},
	}

	for _, tc := range tcases {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := &fakeControlConn{}
			_, err := getHostPortMappingForConnectionIDs(ctrl, "system.client_routes", tc.connectionIDs, false)
			if tc.expectedErr {
				if err == nil {
					t.Fatal("expected error")
				}
				return
			}
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

func TestGetHostPortMappingForPairsQuery(t *testing.T) {
	tcases := []struct {
		name         string
		pairs        []pair
		expectedStmt string
		expectedVals []any
		expectedErr  bool
	}{
		{
			name: "single-pair",
			pairs: []pair{
				{InternalConnectionID: "c1", InternalHostID: "h1"},
			},
			expectedStmt: "select connection_id, host_id, address, port, tls_port from system.client_routes where connection_id in ? and host_id in ?",
			expectedVals: []any{[]string{"c1"}, []string{"h1"}},
		},
		{
			name: "multiple-pairs",
			pairs: []pair{
				{InternalConnectionID: "c1", InternalHostID: "h1"},
				{InternalConnectionID: "c2", InternalHostID: "h2"},
			},
			expectedStmt: "select connection_id, host_id, address, port, tls_port from system.client_routes where connection_id in ? and host_id in ?",
			expectedVals: []any{[]string{"c1", "c2"}, []string{"h1", "h2"}},
		},
		{
			name:        "empty-slices",
			expectedErr: true,
		},
	}

	for _, tc := range tcases {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := &fakeControlConn{}
			_, err := getHostPortMappingForPairs(ctrl, "system.client_routes", tc.pairs, false)
			if tc.expectedErr {
				if err == nil {
					t.Fatal("expected error")
				}
				return
			}
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

func TestGetHostPortMappingForPairsFiltersCrossProductRows(t *testing.T) {
	meta := resultMetadata{
		Columns: []ColumnInfo{
			{Name: "connection_id", TypeInfo: typeVarchar},
			{Name: "host_id", TypeInfo: typeVarchar},
			{Name: "address", TypeInfo: typeVarchar},
			{Name: "port", TypeInfo: typeInt},
			{Name: "tls_port", TypeInfo: typeInt},
		},
		ActualColCount: 5,
		ColCount:       5,
	}

	ctrl := &fakeControlConn{
		iter: makeIter(meta,
			marshalRow(meta, []any{"c1", "h1", "10.0.0.1", 9042, 9142}),
			marshalRow(meta, []any{"c1", "h2", "10.0.0.2", 9043, 9143}),
			marshalRow(meta, []any{"c2", "h1", "10.0.0.3", 9044, 9144}),
			marshalRow(meta, []any{"c2", "h2", "10.0.0.4", 9045, 9145}),
		),
	}

	got, err := getHostPortMappingForPairs(ctrl, "system.client_routes", []pair{
		{InternalConnectionID: "c1", InternalHostID: "h1"},
		{InternalConnectionID: "c2", InternalHostID: "h2"},
	}, false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	want := []clientRoute{
		{InternalConnectionID: "c1", InternalHostID: "h1", InternalAddress: "10.0.0.1", InternalPort: 9042},
		{InternalConnectionID: "c2", InternalHostID: "h2", InternalAddress: "10.0.0.4", InternalPort: 9045},
	}
	if len(got) != len(want) {
		t.Fatalf("unexpected row count: got %d want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("row #%d = %+v, want %+v", i, got[i], want[i])
		}
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
	newRoutes := func() clientRouteCache {
		return newCache(map[string]clientRouteCacheEntry{"h1": newCacheEntry(clientRoute{InternalConnectionID: "c1", InternalHostID: "h1", InternalAddress: "route-addr", InternalPort: 9042})})
	}

	t.Run("no override uses route address", func(t *testing.T) {
		handler := func() *ClientRoutesHandler {
			opaqueStorageValuexxxxxxx := new(ClientRoutesHandler)
			implCluster.ClientRoutesHandlerData(opaqueStorageValuexxxxxxx).InternalAddrOverrides = map[string]string{}
			implCluster.ClientRoutesHandlerData(opaqueStorageValuexxxxxxx).InternalResolver = resolver
			implCluster.ClientRoutesHandlerData(opaqueStorageValuexxxxxxx).InternalRouteCache = newRoutes()
			return opaqueStorageValuexxxxxxx
		}()

		res, err := handler.TranslateHost(testHostInfo{hostID: "h1"}, addr)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !res.Address.Equal(resolvedIPs["route-addr"]) {
			t.Fatalf("expected route-addr IP %v, got %v", resolvedIPs["route-addr"], res.Address)
		}
	})

	t.Run("override replaces route address", func(t *testing.T) {
		handler := func() *ClientRoutesHandler {
			opaqueStorageValuexxxxxxxx := new(ClientRoutesHandler)
			implCluster.ClientRoutesHandlerData(opaqueStorageValuexxxxxxxx).InternalAddrOverrides = map[string]string{"c1": "override-addr"}
			implCluster.ClientRoutesHandlerData(opaqueStorageValuexxxxxxxx).InternalResolver = resolver
			implCluster.ClientRoutesHandlerData(opaqueStorageValuexxxxxxxx).InternalRouteCache = newRoutes()
			return opaqueStorageValuexxxxxxxx
		}()

		res, err := handler.TranslateHost(testHostInfo{hostID: "h1"}, addr)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !res.Address.Equal(resolvedIPs["override-addr"]) {
			t.Fatalf("expected override-addr IP %v, got %v", resolvedIPs["override-addr"], res.Address)
		}
	})
}
func TestStop_DoesNotPanicWithConcurrentAsyncSends(t *testing.T) {
	h := func() *ClientRoutesHandler {
		opaqueStorageValuexxxxxxxxx := new(ClientRoutesHandler)
		implCluster.ClientRoutesHandlerData(opaqueStorageValuexxxxxxxxx).InternalLog = &defaultLogger{}
		implCluster.ClientRoutesHandlerData(opaqueStorageValuexxxxxxxxx).InternalC = &fakeControlConn{}
		implCluster.ClientRoutesHandlerData(opaqueStorageValuexxxxxxxxx).InternalCloseChan = make(chan struct{})
		implCluster.ClientRoutesHandlerData(opaqueStorageValuexxxxxxxxx).InternalUpdateTasks = make(chan updateTask, 1024)
		implCluster.ClientRoutesHandlerData(opaqueStorageValuexxxxxxxxx).InternalRouteCache = newClientRouteCache()
		implCluster.ClientRoutesHandlerData(opaqueStorageValuexxxxxxxxx).InternalCfg = ClientRoutesConfig{
			TableName: "system.client_routes",
			Endpoints: ClientRoutesEndpointList{{ConnectionID: "c1"}},
		}
		return opaqueStorageValuexxxxxxxxx
	}()

	implCluster.InternalClientRoutesHandlerStartUpdateWorker(h)

	const goroutines = 200
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			implCluster.InternalClientRoutesHandlerUpdateHostPortMappingAsync(h, updateTask{InternalConnectionIDs: []string{"c1"}})
		}()
	}
	close(start) // release all goroutines simultaneously
	h.Stop()     // race against the concurrent sends — must not panic
	wg.Wait()
}

func TestUpdateHostPortMappingSyncCompletes(t *testing.T) {
	h := func() *ClientRoutesHandler {
		opaqueStorageValuexxxxxxxxxx := new(ClientRoutesHandler)
		implCluster.ClientRoutesHandlerData(opaqueStorageValuexxxxxxxxxx).InternalLog = &defaultLogger{}
		implCluster.ClientRoutesHandlerData(opaqueStorageValuexxxxxxxxxx).InternalC = &fakeControlConn{}
		implCluster.ClientRoutesHandlerData(opaqueStorageValuexxxxxxxxxx).InternalCloseChan = make(chan struct{})
		implCluster.ClientRoutesHandlerData(opaqueStorageValuexxxxxxxxxx).InternalUpdateTasks = make(chan updateTask, 1024)
		implCluster.ClientRoutesHandlerData(opaqueStorageValuexxxxxxxxxx).InternalRouteCache = newClientRouteCache()
		implCluster.ClientRoutesHandlerData(opaqueStorageValuexxxxxxxxxx).InternalCfg = ClientRoutesConfig{
			TableName: "system.client_routes",
			Endpoints: ClientRoutesEndpointList{{ConnectionID: "c1"}},
		}
		return opaqueStorageValuexxxxxxxxxx
	}()

	implCluster.InternalClientRoutesHandlerStartUpdateWorker(h)
	defer h.Stop()

	done := make(chan error, 1)
	go func() {
		done <- implCluster.InternalClientRoutesHandlerUpdateHostPortMappingSync(h, updateTask{InternalConnectionIDs: []string{"c1"}})
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for synchronous update")
	}
}

func TestStartReadingEventsConnectionIDsOnlySchedulesRefresh(t *testing.T) {
	h, bus := newClientRoutesEventHarness(t, []string{"c1"})

	bus.PublishEventBlocking(&events.ClientRoutesChangedEvent{
		ChangeType:    "UPDATED",
		ConnectionIDs: []string{"c1"},
		HostIDs:       []string{},
	})

	select {
	case task := <-implCluster.ClientRoutesHandlerData(h).InternalUpdateTasks:
		if len(task.InternalConnectionIDs) != 1 || task.InternalConnectionIDs[0] != "c1" {
			t.Fatalf("unexpected connection IDs: %v", task.InternalConnectionIDs)
		}
		if task.InternalPairs != nil {
			t.Fatalf("expected connectionIDs-only refresh, got pairs: %v", task.InternalPairs)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for refresh task")
	}
}

func TestStartReadingEventsUsesCartesianPairs(t *testing.T) {
	h, bus := newClientRoutesEventHarness(t, []string{"c1", "c2"})

	bus.PublishEventBlocking(&events.ClientRoutesChangedEvent{
		ChangeType:    "UPDATED",
		ConnectionIDs: []string{"c1", "c2"},
		HostIDs:       []string{"h1", "h2"},
	})

	select {
	case task := <-implCluster.ClientRoutesHandlerData(h).InternalUpdateTasks:
		want := []pair{
			{InternalConnectionID: "c1", InternalHostID: "h1"},
			{InternalConnectionID: "c1", InternalHostID: "h2"},
			{InternalConnectionID: "c2", InternalHostID: "h1"},
			{InternalConnectionID: "c2", InternalHostID: "h2"},
		}
		if len(task.InternalPairs) != len(want) {
			t.Fatalf("unexpected pair count: got %d want %d", len(task.InternalPairs), len(want))
		}
		for i := range want {
			if task.InternalPairs[i] != want[i] {
				t.Fatalf("pair #%d = %+v, want %+v", i, task.InternalPairs[i], want[i])
			}
		}
		if task.InternalConnectionIDs != nil {
			t.Fatalf("expected pairs-only refresh, got connection IDs: %v", task.InternalConnectionIDs)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for refresh task")
	}
}

func TestClientRoutesHandlerInitializeIsIdempotent(t *testing.T) {
	s := func() *Session {
		opaqueStorageValuexxxxxxxxxxx := new(Session)
		implCluster.SessionData(opaqueStorageValuexxxxxxxxxxx).InternalControl = &fakeControlConn{}
		implCluster.SessionData(opaqueStorageValuexxxxxxxxxxx).InternalEventBus = eventbus.New[events.Event](eventbus.EventBusConfig{
			InputEventsQueueSize: 1,
		}, nil)
		implCluster.SessionData(opaqueStorageValuexxxxxxxxxxx).InternalLogger = &nopLogger{}
		return opaqueStorageValuexxxxxxxxxxx
	}()

	if err := implCluster.SessionData(s).InternalEventBus.Start(); err != nil {
		t.Fatalf("starting event bus: %v", err)
	}
	defer implCluster.SessionData(s).InternalEventBus.Stop()

	h := NewClientRoutesAddressTranslator(ClientRoutesConfig{
		TableName: "system.client_routes",
		Endpoints: ClientRoutesEndpointList{{ConnectionID: "c1"}},
	}, nil, false, &nopLogger{})
	defer h.Stop()

	if err := h.Initialize(s); err != nil {
		t.Fatalf("first Initialize call failed: %v", err)
	}
	if !implCluster.ClientRoutesHandlerData(h).InternalInitialized.Load() {
		t.Fatal("expected handler to be marked initialized")
	}

	if err := h.Initialize(s); err == nil {
		t.Fatal("expected second Initialize call to fail")
	}
}

func TestClientRoutesHandlerTranslateHost_RetainsCurrentRouteAfterQueryError(t *testing.T) {
	addr := AddressPort{Address: net.ParseIP("1.1.1.1"), Port: 9042}
	resolvedIPs := map[string]net.IP{
		"a1": net.ParseIP("10.0.0.1"),
	}
	h := func() *ClientRoutesHandler {
		opaqueStorageValuexxxxxxxxxxxx := new(ClientRoutesHandler)
		implCluster.ClientRoutesHandlerData(opaqueStorageValuexxxxxxxxxxxx).InternalLog = &defaultLogger{}
		implCluster.ClientRoutesHandlerData(opaqueStorageValuexxxxxxxxxxxx).InternalC = &fakeControlConn{iter: &nativeResultFixture{err: errors.New("query failed")}}
		implCluster.ClientRoutesHandlerData(opaqueStorageValuexxxxxxxxxxxx).InternalCloseChan = make(chan struct{})
		implCluster.ClientRoutesHandlerData(opaqueStorageValuexxxxxxxxxxxx).InternalUpdateTasks = make(chan updateTask, 1024)
		implCluster.ClientRoutesHandlerData(opaqueStorageValuexxxxxxxxxxxx).InternalResolver = dnsResolverFunc(func(host string) ([]net.IP, error) {
			if ip, ok := resolvedIPs[host]; ok {
				return []net.IP{ip}, nil
			}
			return nil, fmt.Errorf("unknown host %s", host)
		})
		implCluster.ClientRoutesHandlerData(opaqueStorageValuexxxxxxxxxxxx).InternalRouteCache = newCache(map[string]clientRouteCacheEntry{
			"h1": newCacheEntryWithCurrent(
				clientRoute{InternalConnectionID: "c1", InternalHostID: "h1", InternalAddress: "a1", InternalPort: 9042},
				clientRoute{InternalConnectionID: "c1", InternalHostID: "h1", InternalAddress: "a1", InternalPort: 9042},
			),
		})
		implCluster.ClientRoutesHandlerData(opaqueStorageValuexxxxxxxxxxxx).InternalCfg = ClientRoutesConfig{
			TableName: "system.client_routes",
			Endpoints: ClientRoutesEndpointList{{ConnectionID: "c1"}},
		}
		return opaqueStorageValuexxxxxxxxxxxx
	}()

	implCluster.InternalClientRoutesHandlerStartUpdateWorker(h)
	defer h.Stop()

	before, err := h.TranslateHost(testHostInfo{hostID: "h1"}, addr)
	if err != nil {
		t.Fatalf("unexpected error before query failure: %v", err)
	}

	err = implCluster.InternalClientRoutesHandlerUpdateHostPortMappingSync(h, updateTask{InternalConnectionIDs: []string{"c1"}})
	if err == nil {
		t.Fatal("expected query error")
	}

	after, err := h.TranslateHost(testHostInfo{hostID: "h1"}, addr)
	if err != nil {
		t.Fatalf("unexpected error after query failure: %v", err)
	}
	if !after.Equal(before) {
		t.Fatalf("expected route to remain stable after query failure, got %v want %v", after, before)
	}
}
