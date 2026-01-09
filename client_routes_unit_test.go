//go:build unit
// +build unit

package gocql

import (
	"errors"
	"fmt"
	"net"
	"sync/atomic"
	"testing"
	"time"
)

type dnsResolverFunc func(string) ([]net.IP, error)

// LookupIP implements DNSResolver for dnsResolverFunc.
func (f dnsResolverFunc) LookupIP(host string) ([]net.IP, error) { return f(host) }

type clientRoutesResolverFunc func(endpoint ResolvedClientRoute) ([]net.IP, net.IP, error)

func (f clientRoutesResolverFunc) Resolve(endpoint ResolvedClientRoute) ([]net.IP, net.IP, error) {
	return f(endpoint)
}

type fakeControlConn struct {
	statement string
	values    []interface{}
}

func (f *fakeControlConn) getConn() *connHost          { return nil }
func (f *fakeControlConn) awaitSchemaAgreement() error { return nil }
func (f *fakeControlConn) query(statement string, values ...interface{}) *Iter {
	f.statement = statement
	f.values = values
	return &Iter{}
}
func (f *fakeControlConn) querySystem(statement string, values ...interface{}) *Iter {
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

func TestResolvedClientRouteCloneNewerNeedsUpdate(t *testing.T) {
	ip1 := net.ParseIP("127.0.0.1")
	ip2 := net.ParseIP("127.0.0.2")
	base := ResolvedClientRoute{
		UnresolvedClientRoute: UnresolvedClientRoute{
			ConnectionID: "c1",
			HostID:       "h1",
			Address:      "host",
			CQLPort:      9042,
		},
		allKnownIPs: []net.IP{ip1},
		currentIP:   ip1,
		updateTime:  time.Unix(10, 0),
	}

	clone := base.Clone()
	clone.allKnownIPs[0][0] = 8
	clone.currentIP[0] = 9

	if base.allKnownIPs[0][0] == 8 {
		t.Fatalf("Clone should not share allKnownIPs slices")
	}
	if base.currentIP[0] == 9 {
		t.Fatalf("Clone should not share currentIP slices")
	}

	newerIP := ResolvedClientRoute{currentIP: ip2}
	if !(ResolvedClientRoute{}).Newer(newerIP) {
		t.Fatalf("expected Newer to prefer non-nil currentIP")
	}

	newerTime := ResolvedClientRoute{updateTime: time.Unix(20, 0)}
	if !base.Newer(newerTime) {
		t.Fatalf("expected Newer to prefer newer updateTime")
	}

	if !(ResolvedClientRoute{currentIP: nil}).NeedsUpdate() {
		t.Fatalf("expected NeedsUpdate for missing currentIP")
	}
	if !(ResolvedClientRoute{currentIP: ip1}).NeedsUpdate() {
		t.Fatalf("expected NeedsUpdate for missing allKnownIPs")
	}
	if !(ResolvedClientRoute{currentIP: ip1, allKnownIPs: []net.IP{ip1}, forcedResolve: true}).NeedsUpdate() {
		t.Fatalf("expected NeedsUpdate when forcedResolve is set")
	}
	if (ResolvedClientRoute{currentIP: ip1, allKnownIPs: []net.IP{ip1}}).NeedsUpdate() {
		t.Fatalf("did not expect NeedsUpdate for fully resolved route")
	}
}

func TestResolvedClientRouteListMergeWithUnresolved(t *testing.T) {
	list := ResolvedClientRouteList{
		{
			UnresolvedClientRoute: UnresolvedClientRoute{
				ConnectionID: "c1",
				HostID:       "h1",
				Address:      "a1",
				CQLPort:      9042,
			},
			forcedResolve: false,
		},
	}

	list.MergeWithUnresolved(UnresolvedClientRouteList{
		{
			ConnectionID: "c1",
			HostID:       "h1",
			Address:      "a1",
			CQLPort:      9042,
		},
	})
	if len(list) != 1 || list[0].forcedResolve {
		t.Fatalf("expected unchanged record when unresolved is equal")
	}

	list.MergeWithUnresolved(UnresolvedClientRouteList{
		{
			ConnectionID: "c1",
			HostID:       "h1",
			Address:      "a2",
			CQLPort:      9043,
		},
	})
	if list[0].Address != "a2" || list[0].CQLPort != 9043 || !list[0].forcedResolve {
		t.Fatalf("expected record to update and force resolve")
	}

	list = ResolvedClientRouteList{}
	list.MergeWithUnresolved(UnresolvedClientRouteList{
		{
			ConnectionID: "c2",
			HostID:       "h2",
			Address:      "a3",
			CQLPort:      9044,
		},
	})
	if len(list) != 1 || !list[0].forcedResolve {
		t.Fatalf("expected new record to be appended with forcedResolve")
	}
}

func TestResolvedClientRouteListMergeWithResolved(t *testing.T) {
	older := ResolvedClientRoute{
		UnresolvedClientRoute: UnresolvedClientRoute{ConnectionID: "c1", HostID: "h1"},
		updateTime:            time.Unix(10, 0),
	}
	newer := ResolvedClientRoute{
		UnresolvedClientRoute: UnresolvedClientRoute{ConnectionID: "c1", HostID: "h1"},
		updateTime:            time.Unix(20, 0),
		currentIP:             net.ParseIP("10.0.0.1"),
	}

	list := ResolvedClientRouteList{older}
	other := ResolvedClientRouteList{newer, {UnresolvedClientRoute: UnresolvedClientRoute{ConnectionID: "c2", HostID: "h2"}}}
	list.MergeWithResolved(&other)

	if list[0].updateTime != newer.updateTime || list[0].currentIP == nil {
		t.Fatalf("expected newer record to replace older one")
	}
	if len(list) != 2 {
		t.Fatalf("expected new record to be appended")
	}

	list = ResolvedClientRouteList{newer}
	stale := ResolvedClientRouteList{older}
	list.MergeWithResolved(&stale)
	if list[0].updateTime != newer.updateTime {
		t.Fatalf("expected newer record to be preserved when other is stale")
	}
}

func TestResolvedClientRouteListUpdateIfNewerAndFindByHostID(t *testing.T) {
	list := ResolvedClientRouteList{{
		UnresolvedClientRoute: UnresolvedClientRoute{ConnectionID: "c1", HostID: "h1"},
		updateTime:            time.Unix(10, 0),
	}}

	older := ResolvedClientRoute{UnresolvedClientRoute: UnresolvedClientRoute{ConnectionID: "c1", HostID: "h1"}, updateTime: time.Unix(5, 0)}
	if list.UpdateIfNewer(older) {
		t.Fatalf("expected UpdateIfNewer to ignore older record")
	}

	newer := ResolvedClientRoute{UnresolvedClientRoute: UnresolvedClientRoute{ConnectionID: "c1", HostID: "h1"}, updateTime: time.Unix(15, 0)}
	if !list.UpdateIfNewer(newer) {
		t.Fatalf("expected UpdateIfNewer to accept newer record")
	}

	rec := list.FindByHostID("h1")
	if rec == nil {
		t.Fatalf("expected FindByHostID to locate record")
	}
	rec.ConnectionID = "updated"
	if list[0].ConnectionID != "updated" {
		t.Fatalf("expected FindByHostID to return pointer to list element")
	}
}

func TestSimpleClientRoutesResolverResolve(t *testing.T) {
	calls := 0
	resolver := dnsResolverFunc(func(host string) ([]net.IP, error) {
		calls++
		return []net.IP{net.ParseIP("10.0.0.1"), net.ParseIP("10.0.0.2")}, nil
	})

	res := newSimpleClientRoutesResolver(time.Hour, resolver)
	endpoint := ResolvedClientRoute{
		UnresolvedClientRoute: UnresolvedClientRoute{Address: "example"},
		currentIP:             net.ParseIP("10.0.0.2"),
	}

	all, current, err := res.Resolve(endpoint)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if calls != 1 {
		t.Fatalf("expected resolver to be called once, got %d", calls)
	}
	if current == nil || !current.Equal(endpoint.currentIP) {
		t.Fatalf("expected currentIP to be preserved when present")
	}
	if len(all) != 2 {
		t.Fatalf("expected allKnownIPs to be returned")
	}

	_, _, err = res.Resolve(endpoint)
	if err != nil {
		t.Fatalf("unexpected error from cached resolve: %v", err)
	}
	if calls != 1 {
		t.Fatalf("expected cached resolve to avoid LookupIP, got %d", calls)
	}

	resolveErr := errors.New("resolve failed")
	errorResolver := dnsResolverFunc(func(host string) ([]net.IP, error) {
		return nil, resolveErr
	})
	errorRes := newSimpleClientRoutesResolver(0, errorResolver)
	endpoint.allKnownIPs = []net.IP{net.ParseIP("10.0.0.9")}
	all, current, err = errorRes.Resolve(endpoint)
	if !errors.Is(err, resolveErr) {
		t.Fatalf("expected resolver error to propagate")
	}
	if len(all) != 1 || current == nil || !current.Equal(endpoint.currentIP) {
		t.Fatalf("expected existing values to be returned on error")
	}

	emptyResolver := dnsResolverFunc(func(host string) ([]net.IP, error) {
		return []net.IP{}, nil
	})
	emptyRes := newSimpleClientRoutesResolver(0, emptyResolver)
	_, _, err = emptyRes.Resolve(ResolvedClientRoute{UnresolvedClientRoute: UnresolvedClientRoute{Address: "example"}})
	if err == nil {
		t.Fatalf("expected error when resolver returns empty list")
	}
}

func TestClientRoutesHandlerTranslateHost(t *testing.T) {
	addr := AddressPort{Address: net.ParseIP("1.1.1.1"), Port: 9042}
	noHost := testHostInfo{hostID: ""}
	missingHost := testHostInfo{hostID: "missing"}

	handler := &ClientRoutesHandler{}
	handler.resolvedEndpoints.Store(&ResolvedClientRouteList{})

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

	resolvedList := ResolvedClientRouteList{
		{
			UnresolvedClientRoute: UnresolvedClientRoute{ConnectionID: "c1", HostID: "h1", CQLPort: 9042, SecureCQLPort: 9142},
			currentIP:             net.ParseIP("10.0.0.1"),
		},
	}

	handler.pickTLSPorts = false
	handler.resolvedEndpoints.Store(&resolvedList)
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
		resolver: clientRoutesResolverFunc(func(endpoint ResolvedClientRoute) ([]net.IP, net.IP, error) {
			return nil, nil, errors.New("lookup failed")
		}),
	}
	errorList := ResolvedClientRouteList{{UnresolvedClientRoute: UnresolvedClientRoute{ConnectionID: "c2", HostID: "h2", Address: "host"}}}
	errorHandler.resolvedEndpoints.Store(&errorList)
	_, err = errorHandler.TranslateHost(testHostInfo{hostID: "h2"}, addr)
	if err == nil {
		t.Fatalf("expected resolver error to bubble up")
	}
}

func TestClientRoutesHandlerTranslateHost_CASCollision(t *testing.T) {
	addr := AddressPort{Address: net.ParseIP("1.1.1.1"), Port: 9042}
	resolverStarted := make(chan struct{})
	releaseResolver := make(chan struct{})
	resolver := clientRoutesResolverFunc(func(endpoint ResolvedClientRoute) ([]net.IP, net.IP, error) {
		close(resolverStarted)
		<-releaseResolver
		ip := net.ParseIP("10.0.0.1")
		return []net.IP{ip}, ip, nil
	})

	handler := &ClientRoutesHandler{resolver: resolver, pickTLSPorts: false}
	origList := ResolvedClientRouteList{{UnresolvedClientRoute: UnresolvedClientRoute{ConnectionID: "c1", HostID: "h1", Address: "host", CQLPort: 9042}}}
	handler.resolvedEndpoints.Store(&origList)

	done := make(chan error, 1)
	go func() {
		_, err := handler.TranslateHost(testHostInfo{hostID: "h1"}, addr)
		done <- err
	}()

	<-resolverStarted
	altList := ResolvedClientRouteList{}
	handler.resolvedEndpoints.Store(&altList)
	close(releaseResolver)
	time.Sleep(10 * time.Millisecond)
	handler.resolvedEndpoints.Store(&origList)

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("unexpected error after CAS collision: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("TranslateHost timed out after CAS collision")
	}
}

func TestClientRoutesHandlerResolveAndUpdateInPlace(t *testing.T) {
	var inFlight int32
	var maxInFlight int32
	called := make(chan string, 4)
	resolveErr := errors.New("resolve error")

	resolver := clientRoutesResolverFunc(func(endpoint ResolvedClientRoute) ([]net.IP, net.IP, error) {
		curr := atomic.AddInt32(&inFlight, 1)
		for {
			prev := atomic.LoadInt32(&maxInFlight)
			if curr > prev && atomic.CompareAndSwapInt32(&maxInFlight, prev, curr) {
				break
			}
			if curr <= prev {
				break
			}
		}
		defer atomic.AddInt32(&inFlight, -1)

		called <- endpoint.Address
		time.Sleep(10 * time.Millisecond)
		if endpoint.Address == "err" {
			return nil, nil, resolveErr
		}
		ip := net.ParseIP("10.0.0.1")
		return []net.IP{ip}, ip, nil
	})

	handler := &ClientRoutesHandler{
		resolver: resolver,
		cfg: ClientRoutesConfig{
			MaxResolverConcurrency:       2,
			ResolveHealthyEndpointPeriod: time.Hour,
		},
	}

	now := time.Now().UTC()
	ip := net.ParseIP("10.0.0.2")
	records := ResolvedClientRouteList{
		{
			UnresolvedClientRoute: UnresolvedClientRoute{ConnectionID: "c1", HostID: "h1", Address: "healthy"},
			currentIP:             ip,
			allKnownIPs:           []net.IP{ip},
			updateTime:            now,
		},
		{
			UnresolvedClientRoute: UnresolvedClientRoute{ConnectionID: "c2", HostID: "h2", Address: "forced"},
			forcedResolve:         true,
		},
		{
			UnresolvedClientRoute: UnresolvedClientRoute{ConnectionID: "c3", HostID: "h3", Address: "empty"},
		},
		{
			UnresolvedClientRoute: UnresolvedClientRoute{ConnectionID: "c4", HostID: "h4", Address: "stale"},
			currentIP:             ip,
			allKnownIPs:           []net.IP{ip},
			updateTime:            now.Add(-2 * time.Hour),
		},
		{
			UnresolvedClientRoute: UnresolvedClientRoute{ConnectionID: "c5", HostID: "h5", Address: "err"},
		},
	}

	err := handler.resolveAndUpdateInPlace(records)
	if err == nil || !errors.Is(err, resolveErr) {
		t.Fatalf("expected aggregated error to include resolver error")
	}
	close(called)

	calledMap := map[string]bool{}
	for addr := range called {
		calledMap[addr] = true
	}

	if calledMap["healthy"] {
		t.Fatalf("did not expect healthy endpoint to be resolved")
	}
	for _, addr := range []string{"forced", "empty", "stale", "err"} {
		if !calledMap[addr] {
			t.Fatalf("expected resolver to be called for %s", addr)
		}
	}

	if atomic.LoadInt32(&maxInFlight) > int32(handler.cfg.MaxResolverConcurrency) {
		t.Fatalf("expected max concurrency <= %d, got %d", handler.cfg.MaxResolverConcurrency, maxInFlight)
	}

	if records[1].currentIP == nil || len(records[1].allKnownIPs) == 0 || records[1].forcedResolve {
		t.Fatalf("expected forced endpoint to be resolved and forcedResolve cleared")
	}
}

func TestGetHostPortMappingFromClusterQuery(t *testing.T) {
	tcases := []struct {
		name          string
		connectionIDs []string
		hostIDs       []string
		expectedStmt  string
		expectedVals  []interface{}
	}{
		{
			name:         "all",
			expectedStmt: "select connection_id, host_id, address, port, tls_port from system.client_routes allow filtering",
		},
		{
			name:          "connections-only",
			connectionIDs: []string{"c1", "c2"},
			expectedStmt:  "select connection_id, host_id, address, port, tls_port from system.client_routes where connection_id in (?,?) allow filtering",
			expectedVals:  []interface{}{"c1", "c2"},
		},
		{
			name:         "hosts-only",
			hostIDs:      []string{"h1"},
			expectedStmt: "select connection_id, host_id, address, port, tls_port from system.client_routes where host_id in (?) allow filtering",
			expectedVals: []interface{}{"h1"},
		},
		{
			name:          "connections-and-hosts",
			connectionIDs: []string{"c1"},
			hostIDs:       []string{"h1", "h2"},
			expectedStmt:  "select connection_id, host_id, address, port, tls_port from system.client_routes where connection_id in (?) and host_id in (?,?)",
			expectedVals:  []interface{}{"c1", "h1", "h2"},
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
