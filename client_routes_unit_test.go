//go:build unit
// +build unit

package gocql

import (
	"errors"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/gocql/gocql/events"
	"github.com/gocql/gocql/internal/eventbus"
)

// dnsResolverFunc adapts a function to the DNSResolver interface.
type dnsResolverFunc func(host string) ([]net.IP, error)

func (f dnsResolverFunc) LookupIP(host string) ([]net.IP, error) {
	return f(host)
}

type fakeControlConn struct {
	statement string
	values    []any
	iter      *Iter
}

func (f *fakeControlConn) getConn() *connHost          { return nil }
func (f *fakeControlConn) awaitSchemaAgreement() error { return nil }
func (f *fakeControlConn) query(statement string, values ...any) *Iter {
	f.statement = statement
	f.values = values
	if f.iter != nil {
		return f.iter
	}
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

func newClientRoutesEventHarness(t *testing.T, allowedConnectionIDs []string) (*ClientRoutesHandler, *eventbus.EventBus[events.Event]) {
	t.Helper()

	bus := eventbus.New[events.Event](eventbus.EventBusConfig{InputEventsQueueSize: 1}, nil)
	if err := bus.Start(); err != nil {
		t.Fatalf("starting event bus: %v", err)
	}

	h := &ClientRoutesHandler{
		sub:         bus.Subscribe("port-mux", 1, nil),
		updateTasks: make(chan updateTask, 1),
		closeChan:   make(chan struct{}),
	}
	h.startReadingEvents(allowedConnectionIDs)

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
	return clientRouteCacheEntry{allRoutes: routes}
}

func newCache(routes map[string]clientRouteCacheEntry) clientRouteCache {
	return clientRouteCache{routes: routes}
}

func newCacheEntryWithCurrent(current clientRoute, routes ...clientRoute) clientRouteCacheEntry {
	entry := clientRouteCacheEntry{allRoutes: routes}
	entry.BindCurrent(current.connectionID)
	return entry
}

func TestClientRoutesHandlerTranslateHost(t *testing.T) {
	addr := AddressPort{Address: net.ParseIP("1.1.1.1"), Port: 9042}
	noHost := testHostInfo{hostID: ""}
	missingHost := testHostInfo{hostID: "missing"}

	resolver := dnsResolverFunc(func(host string) ([]net.IP, error) {
		return []net.IP{net.ParseIP("10.0.0.1")}, nil
	})

	handler := &ClientRoutesHandler{
		resolver:   resolver,
		routeCache: newClientRouteCache(),
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

	handler.routeCache = newCache(map[string]clientRouteCacheEntry{
		"h1": newCacheEntry(clientRoute{connectionID: "c1", hostID: "h1", port: 9042}),
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

	errorHandler := &ClientRoutesHandler{
		resolver: dnsResolverFunc(func(host string) ([]net.IP, error) {
			return nil, errors.New("lookup failed")
		}),
		routeCache: newCache(map[string]clientRouteCacheEntry{"h2": newCacheEntry(clientRoute{connectionID: "c2", hostID: "h2", address: "host", port: 9042})}),
	}
	_, err = errorHandler.TranslateHost(testHostInfo{hostID: "h2"}, addr)
	if err == nil {
		t.Fatalf("expected resolver error to bubble up")
	}

	// Route with port 0 should return an error before attempting DNS.
	zeroPortHandler := &ClientRoutesHandler{
		resolver: dnsResolverFunc(func(host string) ([]net.IP, error) {
			t.Fatal("DNS should not be called when port is 0")
			return nil, nil
		}),
		routeCache: newCache(map[string]clientRouteCacheEntry{"h3": newCacheEntry(clientRoute{connectionID: "c3", hostID: "h3", address: "host", port: 0})}),
	}
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
	handler := &ClientRoutesHandler{
		pickTLSPorts: false,
		resolver: dnsResolverFunc(func(host string) ([]net.IP, error) {
			if ip, ok := resolvedIPs[host]; ok {
				return []net.IP{ip}, nil
			}
			return nil, fmt.Errorf("unknown host %s", host)
		}),
		routeCache: newCache(map[string]clientRouteCacheEntry{
			"h1": newCacheEntry(
				clientRoute{connectionID: "c1", hostID: "h1", address: "addr-c1", port: 9042},
				clientRoute{connectionID: "c2", hostID: "h1", address: "addr-c2", port: 9042},
			),
		}),
	}

	// First call picks the first route in the record.
	res1, err := handler.TranslateHost(testHostInfo{hostID: "h1"}, addr)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !res1.Address.Equal(resolvedIPs["addr-c1"]) {
		t.Fatalf("expected first route to resolve addr-c1, got %v", res1.Address)
	}

	// Refresh the host and replace c1 with c2. The next call should fall back to
	// the remaining route.
	handler.routeCache.ReplaceByConnectionIDs([]string{"c1"}, []clientRoute{
		{connectionID: "c2", hostID: "h1", address: "addr-c2", port: 9042},
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
		return &ClientRoutesHandler{
			resolver: resolver,
			routeCache: newCache(map[string]clientRouteCacheEntry{
				"h1": newCacheEntryWithCurrent(
					clientRoute{connectionID: "c1", hostID: "h1", address: "addr-c1-old", port: 9042},
					clientRoute{connectionID: "c1", hostID: "h1", address: "addr-c1-old", port: 9042},
					clientRoute{connectionID: "c2", hostID: "h1", address: "addr-c2", port: 9042},
				),
			}),
		}
	}

	for _, tc := range []struct {
		name    string
		replace func(*clientRouteCache)
	}{
		{
			name: "connection-ids",
			replace: func(c *clientRouteCache) {
				c.ReplaceByConnectionIDs([]string{"c1"}, []clientRoute{
					{connectionID: "c1", hostID: "h1", address: "addr-c1-new", port: 9042},
				})
			},
		},
		{
			name: "pairs",
			replace: func(c *clientRouteCache) {
				c.ReplaceByPairs([]pair{{connectionID: "c1", hostID: "h1"}}, []clientRoute{
					{connectionID: "c1", hostID: "h1", address: "addr-c1-new", port: 9042},
				})
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			handler := newHandler()
			tc.replace(&handler.routeCache)

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
	handler := &ClientRoutesHandler{
		resolver: dnsResolverFunc(func(host string) ([]net.IP, error) {
			if ip, ok := resolvedIPs[host]; ok {
				return []net.IP{ip}, nil
			}
			return nil, fmt.Errorf("unknown host %s", host)
		}),
		routeCache: newCache(map[string]clientRouteCacheEntry{
			"h1": newCacheEntryWithCurrent(
				clientRoute{connectionID: "c1", hostID: "h1", address: "addr-h1", port: 9042},
				clientRoute{connectionID: "c1", hostID: "h1", address: "addr-h1", port: 9042},
			),
			"h2": newCacheEntry(clientRoute{connectionID: "c1", hostID: "h2", address: "addr-h2", port: 9042}),
			"h3": newCacheEntry(clientRoute{connectionID: "c1", hostID: "h3", address: "addr-h3", port: 9042}),
		}),
	}

	if _, err := handler.TranslateHost(testHostInfo{hostID: "h3"}, addr); err != nil {
		t.Fatalf("unexpected error before refresh: %v", err)
	}

	handler.routeCache.ReplaceByConnectionIDs([]string{"c1"}, []clientRoute{
		{connectionID: "c1", hostID: "h1", address: "addr-h1-new", port: 9042},
		{connectionID: "c1", hostID: "h2", address: "addr-h2", port: 9042},
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
				{connectionID: "c1", hostID: "h1"},
			},
			expectedStmt: "select connection_id, host_id, address, port, tls_port from system.client_routes where connection_id in ? and host_id in ?",
			expectedVals: []any{[]string{"c1"}, []string{"h1"}},
		},
		{
			name: "multiple-pairs",
			pairs: []pair{
				{connectionID: "c1", hostID: "h1"},
				{connectionID: "c2", hostID: "h2"},
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
		return newCache(map[string]clientRouteCacheEntry{"h1": newCacheEntry(clientRoute{connectionID: "c1", hostID: "h1", address: "route-addr", port: 9042})})
	}

	t.Run("no override uses route address", func(t *testing.T) {
		handler := &ClientRoutesHandler{
			addrOverrides: map[string]string{},
			resolver:      resolver,
			routeCache:    newRoutes(),
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
			routeCache:    newRoutes(),
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
func TestStop_DoesNotPanicWithConcurrentAsyncSends(t *testing.T) {
	h := &ClientRoutesHandler{
		log:         &defaultLogger{},
		c:           &fakeControlConn{},
		closeChan:   make(chan struct{}),
		updateTasks: make(chan updateTask, 1024),
		routeCache:  newClientRouteCache(),
		cfg: ClientRoutesConfig{
			TableName: "system.client_routes",
			Endpoints: ClientRoutesEndpointList{{ConnectionID: "c1"}},
		},
	}
	h.startUpdateWorker()

	const goroutines = 200
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			h.updateHostPortMappingAsync(updateTask{connectionIDs: []string{"c1"}})
		}()
	}
	close(start) // release all goroutines simultaneously
	h.Stop()     // race against the concurrent sends — must not panic
	wg.Wait()
}

func TestUpdateHostPortMappingSyncCompletes(t *testing.T) {
	h := &ClientRoutesHandler{
		log:         &defaultLogger{},
		c:           &fakeControlConn{},
		closeChan:   make(chan struct{}),
		updateTasks: make(chan updateTask, 1024),
		routeCache:  newClientRouteCache(),
		cfg: ClientRoutesConfig{
			TableName: "system.client_routes",
			Endpoints: ClientRoutesEndpointList{{ConnectionID: "c1"}},
		},
	}
	h.startUpdateWorker()
	defer h.Stop()

	done := make(chan error, 1)
	go func() {
		done <- h.updateHostPortMappingSync(updateTask{connectionIDs: []string{"c1"}})
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
	case task := <-h.updateTasks:
		if len(task.connectionIDs) != 1 || task.connectionIDs[0] != "c1" {
			t.Fatalf("unexpected connection IDs: %v", task.connectionIDs)
		}
		if task.pairs != nil {
			t.Fatalf("expected connectionIDs-only refresh, got pairs: %v", task.pairs)
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
	case task := <-h.updateTasks:
		want := []pair{
			{connectionID: "c1", hostID: "h1"},
			{connectionID: "c1", hostID: "h2"},
			{connectionID: "c2", hostID: "h1"},
			{connectionID: "c2", hostID: "h2"},
		}
		if len(task.pairs) != len(want) {
			t.Fatalf("unexpected pair count: got %d want %d", len(task.pairs), len(want))
		}
		for i := range want {
			if task.pairs[i] != want[i] {
				t.Fatalf("pair #%d = %+v, want %+v", i, task.pairs[i], want[i])
			}
		}
		if task.connectionIDs != nil {
			t.Fatalf("expected pairs-only refresh, got connection IDs: %v", task.connectionIDs)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for refresh task")
	}
}

func TestClientRoutesHandlerInitializeIsIdempotent(t *testing.T) {
	s := &Session{
		control: &fakeControlConn{},
		eventBus: eventbus.New[events.Event](eventbus.EventBusConfig{
			InputEventsQueueSize: 1,
		}, nil),
		logger: &nopLogger{},
	}
	if err := s.eventBus.Start(); err != nil {
		t.Fatalf("starting event bus: %v", err)
	}
	defer s.eventBus.Stop()

	h := NewClientRoutesAddressTranslator(ClientRoutesConfig{
		TableName: "system.client_routes",
		Endpoints: ClientRoutesEndpointList{{ConnectionID: "c1"}},
	}, nil, false, &nopLogger{})
	defer h.Stop()

	if err := h.Initialize(s); err != nil {
		t.Fatalf("first Initialize call failed: %v", err)
	}
	if !h.initialized {
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
	h := &ClientRoutesHandler{
		log:         &defaultLogger{},
		c:           &fakeControlConn{iter: &Iter{err: errors.New("query failed")}},
		closeChan:   make(chan struct{}),
		updateTasks: make(chan updateTask, 1024),
		resolver: dnsResolverFunc(func(host string) ([]net.IP, error) {
			if ip, ok := resolvedIPs[host]; ok {
				return []net.IP{ip}, nil
			}
			return nil, fmt.Errorf("unknown host %s", host)
		}),
		routeCache: newCache(map[string]clientRouteCacheEntry{
			"h1": newCacheEntryWithCurrent(
				clientRoute{connectionID: "c1", hostID: "h1", address: "a1", port: 9042},
				clientRoute{connectionID: "c1", hostID: "h1", address: "a1", port: 9042},
			),
		}),
		cfg: ClientRoutesConfig{
			TableName: "system.client_routes",
			Endpoints: ClientRoutesEndpointList{{ConnectionID: "c1"}},
		},
	}
	h.startUpdateWorker()
	defer h.Stop()

	before, err := h.TranslateHost(testHostInfo{hostID: "h1"}, addr)
	if err != nil {
		t.Fatalf("unexpected error before query failure: %v", err)
	}

	err = h.updateHostPortMappingSync(updateTask{connectionIDs: []string{"c1"}})
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
