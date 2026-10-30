//go:build unit

package gocql

import implExecution "github.com/scylladb/gocql/v2/internal/execution"

import (
	implCluster "github.com/scylladb/gocql/v2/internal/cluster"
	implEndpoint "github.com/scylladb/gocql/v2/internal/endpoint"
)

import (
	routing "github.com/scylladb/gocql/v2/internal/routing"

	"context"
	"crypto/tls"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	oldevents "github.com/scylladb/gocql/v2/events"
	frm "github.com/scylladb/gocql/v2/internal/frame"
)

func TestStep05ExplicitContactsHaveActualDistinctIdentities(t *testing.T) {
	ids := []UUID{MustRandomUUID(), MustRandomUUID()}
	contacts := make([]string, 2)
	for i := range contacts {
		id := ids[i]
		srv := newControlTestServer(t, func(s *controlTestServer) {
			s.localRows = func(stream int, response *framer) {
				row := systemLocalRow(net.IPv4(127, 0, 0, 1))
				row[0] = id[:]
				writeRows(response, stream, systemLocalCols, [][][]byte{row})
			}
		})
		contacts[i] = srv.Address
	}
	config := DefaultConfig(contacts...)
	config.Protocol = 4
	config.Metadata.Mode = MetadataDisabled
	config.Metadata.InitialDiscovery = false
	config.Metadata.AutomaticAgreement = false
	config.Pool.ShardAwarePort = false
	config.Pool.ConnectionsPerHost = 1
	config.Logger = nopLogger{}
	s, err := Connect(context.Background(), config)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	hosts := s.Metadata().Hosts()
	if len(hosts) != 2 {
		t.Fatalf("contact identities collapsed: %+v", hosts)
	}
	for _, id := range ids {
		found := false
		for _, h := range hosts {
			if h.ID == id {
				found = true
			}
		}
		if !found {
			t.Fatal("missing actual UUID", id, hosts)
		}
	}
	if err := s.RefreshMetadata(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(s.Metadata().Hosts()) != 2 {
		t.Fatal("explicit refresh discovered unrelated peers or collapsed contacts")
	}
}

func TestStep05IncompleteKnownPeerRetainsCompleteTopology(t *testing.T) {
	good := newTestPeer("127.0.0.2")
	srv := newRingTestServer(t, good)
	s := srv.session(t, nil)
	before := ringHosts(s)
	incomplete := good
	incomplete.rack = ""
	srv.setPeers(incomplete)
	var partial *PartialRefreshError
	if err := implCluster.InternalSessionRefreshRingNow(s); !errors.As(err, &partial) || partial.Topology == nil {
		t.Fatal("incomplete read was not a visible partial failure", err)
	}
	if got := ringHosts(s); !reflect.DeepEqual(got, before) {
		t.Fatal("incomplete read erased valid topology", before, got)
	}
}

func TestStep05TopologyRemovalTombstoneAndRecreate(t *testing.T) {
	peer := newTestPeer("127.0.0.2")
	srv := newRingTestServer(t, peer)
	s := srv.session(t, nil)
	implCluster.InternalSessionRecordV2Topology(s, &oldevents.TopologyChangeEvent{Change: "REMOVED_NODE", Host: peer.ip, Port: 9042})
	if implCluster.SessionData(s).InternalHostSource.InternalGetHost(peer.hostID.String()) != nil {
		t.Fatal("removed host still visible")
	}
	// Gossip still advertises its old row. A post-event fetch cannot resurrect it.
	if err := implCluster.InternalSessionRefreshRingNow(s); err != nil {
		t.Fatal(err)
	}
	if implCluster.SessionData(s).InternalHostSource.InternalGetHost(peer.hostID.String()) != nil {
		t.Fatal("stale gossip resurrected removed host")
	}
	implCluster.InternalSessionRecordV2Topology(s, &oldevents.TopologyChangeEvent{Change: "NEW_NODE", Host: peer.ip, Port: 9042})
	if err := implCluster.InternalSessionRefreshRingNow(s); err != nil {
		t.Fatal(err)
	}
	if implCluster.SessionData(s).InternalHostSource.InternalGetHost(peer.hostID.String()) == nil {
		t.Fatal("new identity epoch did not allow rediscovery")
	}
}

type step05BlockingPolicy struct {
	RoutingPolicy
	block            atomic.Bool
	entered, release chan struct{}
}

func (p *step05BlockingPolicy) Validate(*MetadataSnapshot) error {
	if p.block.CompareAndSwap(true, false) {
		close(p.entered)
		<-p.release
	}
	return nil
}
func TestStep05SnapshotPublicationCannotCrossDrop(t *testing.T) {
	s, _ := refreshAllSchemaSession(t)
	before := s.Metadata()
	base := DefaultConfig("localhost")
	policy := &step05BlockingPolicy{RoutingPolicy: base.DefaultProfile.Snapshot().Routing, entered: make(chan struct{}), release: make(chan struct{})}
	profile := base.DefaultProfile.Snapshot()
	profile.Routing = policy
	if err := base.DefaultProfile.Replace(profile); err != nil {
		t.Fatal(err)
	}
	implCluster.InternalSessionConfigureV2Peripheral(s, base)
	policy.block.Store(true)
	done := make(chan struct{})
	go func() { defer close(done); implCluster.InternalSessionPublishV2Metadata(s) }()
	<-policy.entered
	implCluster.InternalSessionPublishV2Event(s, &oldevents.SchemaChangeTableEvent{Change: "DROPPED", Keyspace: "ks1", Table: "t1"})
	after := s.Metadata()
	close(policy.release)
	<-done
	if s.Metadata().Generation() < after.Generation() {
		t.Fatal("publication generation moved backwards")
	}
	if ks, _ := s.Metadata().Keyspace("ks1"); ks.Tables["t1"] != nil {
		t.Fatal("old builder resurrected dropped table")
	}
	if ks, _ := before.Keyspace("ks1"); ks.Tables["t1"] == nil {
		t.Fatal("retained snapshot mutated")
	}
}

func TestStep05TableRefreshCannotCrossDropAndFailure(t *testing.T) {
	s, mock := refreshAllSchemaSession(t)
	entered, release := make(chan struct{}), make(chan struct{})
	var gate atomic.Bool
	mock.iterFor = func(stmt string) *nativeResultFixture {
		if strings.Contains(stmt, "FROM system_schema.tables") && gate.CompareAndSwap(false, true) {
			close(entered)
			<-release
		}
		return nil
	}
	done := make(chan error, 1)
	go func() {
		done <- implCluster.SessionData(s).InternalMetadataDescriber.InternalRefreshTableSchema("ks1", "t1")
	}()
	<-entered
	implCluster.InternalSessionPublishV2Event(s, &oldevents.SchemaChangeTableEvent{Change: "DROPPED", Keyspace: "ks1", Table: "t1"})
	close(release)
	var partial *PartialRefreshError
	if err := <-done; !errors.As(err, &partial) {
		t.Fatal("stale table refresh accepted", err)
	}
	if ks, _ := s.Metadata().Keyspace("ks1"); ks.Tables["t1"] != nil {
		t.Fatal("stale table refresh resurrected table")
	}
	// A later failing keyspace read preserves the latest valid snapshot, including
	// the table tombstone, rather than the pre-drop retained schema.
	mock.mu.Lock()
	mock.queryError = errors.New("temporary failure")
	mock.mu.Unlock()
	_ = implCluster.InternalSessionRefreshV2Keyspace(s, context.Background(), "ks1")
	if ks, _ := s.Metadata().Keyspace("ks1"); ks.Tables["t1"] != nil {
		t.Fatal("failure fallback resurrected table")
	}
}

func TestStep05SchemaCapabilityAndMetadataScope(t *testing.T) {
	s, mock := refreshAllSchemaSession(t)
	implCluster.SessionData(s).InternalUseSystemSchema = false
	if err := implCluster.InternalSessionRefreshV2AllSchema(s, context.Background()); err == nil {
		t.Fatal("unsupported schema silently succeeded")
	} else {
		var unsupported *UnsupportedSettingError
		if !errors.As(err, &unsupported) {
			t.Fatal(err)
		}
	}
	implCluster.SessionData(s).InternalUseSystemSchema = true
	for _, mode := range []MetadataMode{MetadataDisabled, MetadataMinimal, MetadataFull} {
		config := DefaultConfig("localhost")
		config.Metadata.Mode = mode
		config.Metadata.Keyspaces = []string{"ks1"}
		implCluster.InternalSessionConfigureV2Peripheral(s, config)
		mock.mu.Lock()
		mock.queries = nil
		mock.mu.Unlock()
		if err := implCluster.InternalSessionRefreshV2AllSchema(s, context.Background()); err != nil {
			t.Fatal(mode, err)
		}
		mock.mu.Lock()
		queries := append([]queryRecord(nil), mock.queries...)
		mock.mu.Unlock()
		if mode == MetadataDisabled && len(queries) != 0 {
			t.Fatal("disabled schema issued optional queries", queries)
		}
		for _, q := range queries {
			if strings.Contains(q.stmt, "'ks2'") {
				t.Fatal("filtered keyspace queried", q)
			}
		}
		if mode == MetadataMinimal {
			ks, _ := s.Metadata().Keyspace("ks1")
			if ks == nil || len(ks.Tables) != 0 {
				t.Fatal("minimal metadata expanded schema")
			}
		}
	}
}

func TestStep05OverflowHighestGenerationAndIsolation(t *testing.T) {
	s := func() *Session {
		opaqueStorageValue := new(Session)
		return opaqueStorageValue
	}()

	defer s.Close()
	slow, _ := s.Subscribe(EventFilter{Capacity: 2})
	fast, _ := s.Subscribe(EventFilter{Capacity: 8})
	defer fast.Close()
	for _, generation := range []uint64{40, 99, 41} {
		implCluster.InternalSessionPublishV2LocalEvent(s, Event{Kind: StatusEvent, Generation: generation})
	}
	marker, ok := <-slow.Events()
	if !ok || marker.Kind != OverflowEvent || marker.Generation != 99 {
		t.Fatal(marker, ok)
	}
	if _, ok := <-slow.Events(); ok {
		t.Fatal("overflow marker was not terminal")
	}
	for _, generation := range []uint64{40, 99, 41} {
		if e := <-fast.Events(); e.Kind != StatusEvent || e.Generation != generation {
			t.Fatal("one slow subscriber affected another", e)
		}
	}
	if len(implCluster.InternalSessionPeripheral(s).InternalSubscribers) != 1 {
		t.Fatal("terminal subscriber leaked in registry")
	}
}

func TestStep05ShutdownJoinsWorkersAndRepeatedClosers(t *testing.T) {
	s := func() *Session {
		opaqueStorageValuex := new(Session)
		return opaqueStorageValuex
	}()

	p := implCluster.InternalSessionPeripheral(s)
	canceled, release := make(chan struct{}), make(chan struct{})
	p.InternalStartWorker(func() { <-p.InternalDone; close(canceled); <-release })
	first, second := make(chan struct{}), make(chan struct{})
	go func() { s.Close(); close(first) }()
	<-canceled
	go func() { s.Close(); close(second) }()
	select {
	case <-first:
		t.Fatal("Close returned before admitted worker finished")
	default:
	}
	select {
	case <-second:
		t.Fatal("repeated Close did not join same shutdown")
	default:
	}
	close(release)
	<-first
	<-second
	if p.InternalStartWorker(func() { t.Error("worker admitted after close") }) {
		t.Fatal("worker admitted after close")
	}
}

func TestStep05DebouncerSerialBoundedAndJoined(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	var callbacks atomic.Int32
	var resync atomic.Int32
	d := newEventDebouncer("bounded", func([]frame) {
		if callbacks.Add(1) == 1 {
			close(entered)
			<-release
		}
	}, nopLogger{})
	d.InternalResync = func() { resync.Add(1) }
	for i := 0; i < eventBufferSize+1; i++ {
		d.InternalDebounce(&frm.StatusChangeEventFrame{})
	}
	d.InternalMu.Lock()
	if len(d.InternalEvents) != eventBufferSize || !d.InternalOverflow {
		t.Fatal("unbounded queue or lost overflow")
	}
	d.InternalTimer.Reset(0)
	d.InternalMu.Unlock()
	<-entered
	stopped := make(chan struct{})
	go func() { d.InternalStop(); close(stopped) }()
	select {
	case <-stopped:
		t.Fatal("stop did not join callback")
	default:
	}
	close(release)
	<-stopped
	if callbacks.Load() != 1 {
		t.Fatal("callbacks overlapped")
	}
	d.InternalStop()
}

func step05UUIDRows(id, version UUID) []byte {
	body := binary.BigEndian.AppendUint32(nil, frm.ResultKindRows)
	body = binary.BigEndian.AppendUint32(body, 1)
	body = binary.BigEndian.AppendUint32(body, 2)
	str := func(v string) { body = binary.BigEndian.AppendUint16(body, uint16(len(v))); body = append(body, v...) }
	str("system")
	str("local")
	for _, name := range []string{"host_id", "schema_version"} {
		str(name)
		body = binary.BigEndian.AppendUint16(body, uint16(TypeUUID))
	}
	body = binary.BigEndian.AppendUint32(body, 1)
	for _, id := range []UUID{id, version} {
		body = binary.BigEndian.AppendUint32(body, 16)
		body = append(body, id[:]...)
	}
	return body
}
func step05AgreementConn(t *testing.T, s *Session, id, version UUID, code int) (*driverConn, *atomic.Int32) {
	t.Helper()
	c, server := typedTestConn(t, 4, nil)
	c.InternalHost = func() *HostInfo {
		opaqueStorageValuexx := new(HostInfo)
		implEndpoint.HostInfoData(opaqueStorageValuexx).InternalHostId = id
		implEndpoint.HostInfoData(opaqueStorageValuexx).InternalConnectAddress = net.IPv4(127, 0, 0, 1)
		implEndpoint.HostInfoData(opaqueStorageValuexx).InternalPort = 9042
		return opaqueStorageValuexx
	}()
	c.InternalHooks = newConnectionHooks(s)
	go c.InternalServe(c.InternalCtx)
	count := &atomic.Int32{}
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			header := make([]byte, 9)
			if _, err := io.ReadFull(server, header); err != nil {
				return
			}
			n := binary.BigEndian.Uint32(header[5:])
			if _, err := io.CopyN(io.Discard, server, int64(n)); err != nil {
				return
			}
			stream := int(binary.BigEndian.Uint16(header[2:4]))
			count.Add(1)
			response := typedTestResponse(t, 4, stream, step05UUIDRows(id, version))
			if code != 0 {
				response = step03ErrorResponse(t, stream, code, nil)
			}
			if _, err := server.Write(response); err != nil {
				return
			}
		}
	}()
	t.Cleanup(func() { c.Close(); server.Close(); <-done; c.InternalWaitEventDispatch() })
	return c, count
}
func step05AgreementSession(t *testing.T) (*Session, UUID, UUID) {
	t.Helper()
	s := func() *Session {
		opaqueStorageValuexxx := new(Session)
		implCluster.SessionData(opaqueStorageValuexxx).InternalLogger = nopLogger{}
		return opaqueStorageValuexxx
	}()
	config := DefaultConfig("localhost")
	config.Metadata.AgreementTimeout = 35 * time.Millisecond
	config.Metadata.AgreementPoll = time.Millisecond
	implCluster.InternalSessionConfigureV2Peripheral(s, config)
	implCluster.SessionData(s).InternalPool = &policyConnPool{InternalHostConnPools: make(map[UUID]*hostConnPool)}
	t.Cleanup(func() { s.Close() })
	return s, MustRandomUUID(), MustRandomUUID()
}
func step05AgreementPool(s *Session, c *driverConn) {
	implCluster.SessionData(s).InternalPool.InternalHostConnPools[implEndpoint.InternalHostInfoHostUUID(c.InternalHost)] = &hostConnPool{InternalHost: c.InternalHost, InternalConnPicker: &defaultConnPicker{InternalConns: []*driverConn{c}}}
}
func TestStep05AgreementChecksEnabledDownAndZeroSuccess(t *testing.T) {
	for _, success := range []bool{false, true} {
		t.Run(fmt.Sprint(success), func(t *testing.T) {
			s, id, missing := step05AgreementSession(t)
			hosts := []HostSnapshot{{ID: missing, Enabled: true, Up: false}}
			if success {
				c, _ := step05AgreementConn(t, s, id, MustRandomUUID(), 0)
				step05AgreementPool(s, c)
				hosts = append(hosts, HostSnapshot{ID: id, Enabled: true, Up: true})
			}
			implCluster.InternalSessionPeripheral(s).InternalSnapshot.Store(routing.NewMetadataSnapshot(routing.SnapshotState{InternalHosts: hosts}))
			if err := s.AwaitSchemaAgreement(context.Background()); !errors.Is(err, context.DeadlineExceeded) {
				t.Fatal("missing enabled host/zero successes counted as agreement", err)
			}
		})
	}
}
func TestStep05AgreementRetainsExcludedCoordinatorAndFilters(t *testing.T) {
	s, id, coordinatorID := step05AgreementSession(t)
	version := MustRandomUUID()
	c, _ := step05AgreementConn(t, s, id, version, 0)
	step05AgreementPool(s, c)
	coordinator, count := step05AgreementConn(t, s, coordinatorID, version, 0)
	implCluster.InternalSessionPeripheral(s).InternalSnapshot.Store(routing.NewMetadataSnapshot(routing.SnapshotState{InternalHosts: []HostSnapshot{{ID: id, Enabled: true}, {ID: MustRandomUUID(), Enabled: false}}}))
	if err := implCluster.InternalSessionAwaitV2SchemaAgreementOn(s, context.Background(), coordinator); err != nil {
		t.Fatal(err)
	}
	if count.Load() != 1 {
		t.Fatal("excluded exact coordinator not queried", count.Load())
	}
	different, _ := step05AgreementConn(t, s, coordinatorID, MustRandomUUID(), 0)
	if err := implCluster.InternalSessionAwaitV2SchemaAgreementOn(s, context.Background(), different); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("older topology agreement ignored coordinator schema", err)
	}
}
func TestStep05AgreementTerminalAuthorization(t *testing.T) {
	s, id, _ := step05AgreementSession(t)
	c, count := step05AgreementConn(t, s, id, MustRandomUUID(), ErrCodeUnauthorized)
	step05AgreementPool(s, c)
	implCluster.InternalSessionPeripheral(s).InternalSnapshot.Store(routing.NewMetadataSnapshot(routing.SnapshotState{InternalHosts: []HostSnapshot{{ID: id, Enabled: true}}}))
	var request RequestError
	if err := s.AwaitSchemaAgreement(context.Background()); !errors.As(err, &request) || request.GetCode() != ErrCodeUnauthorized {
		t.Fatal(err)
	}
	if count.Load() != 1 {
		t.Fatal("authorization retried", count.Load())
	}
}

func TestStep05ClientRouteConfigSnapshotsAndValidation(t *testing.T) {
	cfg := DefaultConfig("localhost")
	cfg.Pool.ShardAwarePort = false
	cfg.ClientRoutes = &ClientRoutesOptions{ConnectionIDs: []string{"one", "two"}, HostnameOverrides: map[string]string{"one": "proxy.example"}}
	if err := validateConfig(cfg); err != nil {
		t.Fatal(err)
	}
	snapshot := snapshotConfig(cfg)
	cfg.ClientRoutes.ConnectionIDs[0] = "changed"
	cfg.ClientRoutes.HostnameOverrides["one"] = "changed"
	if snapshot.ClientRoutes.ConnectionIDs[0] != "one" || snapshot.ClientRoutes.HostnameOverrides["one"] != "proxy.example" {
		t.Fatal("route config aliases input")
	}
	edits := []func(*Config){func(c *Config) { c.Pool.ShardAwarePort = true }, func(c *Config) { c.ClientRoutes.ConnectionIDs = []string{"one", "one"} }, func(c *Config) { c.ClientRoutes.HostnameOverrides["unknown"] = "host" }, func(c *Config) { c.ClientRoutes.HostnameOverrides["one"] = "" }, func(c *Config) { c.ClientRoutes.TLS = true }, func(c *Config) { c.ClientRoutes.TLS = true; c.TLS = &tls.Config{InsecureSkipVerify: true} }}
	for _, edit := range edits {
		c := snapshotConfig(snapshot)
		edit(&c)
		if err := validateConfig(c); err == nil {
			t.Fatal("incompatible route setting accepted")
		}
	}
}
func TestStep05ClientRoutesFencePerPairAndPreserveDisjointUpdates(t *testing.T) {
	p := NewClientRoutesAddressTranslator(ClientRoutesConfig{Endpoints: ClientRoutesEndpointList{{ConnectionID: "a"}, {ConnectionID: "b"}}}, nil, false, nopLogger{})
	a := clientRoute{InternalConnectionID: "a", InternalHostID: "host", InternalAddress: "old", InternalPort: 9042}
	b := clientRoute{InternalConnectionID: "b", InternalHostID: "host", InternalAddress: "b", InternalPort: 9042}
	implCluster.ClientRoutesHandlerData(p).InternalRouteCache.ReplaceByConnectionIDs([]string{"a", "b"}, []clientRoute{b, a})
	if route := implCluster.InternalClientRoutesHandlerOrderedRoutes(p, "host")[0]; route.InternalConnectionID != "a" {
		t.Fatal("server row order overrode config order", route)
	}
	old := implCluster.InternalClientRoutesHandlerCaptureRouteEpochs(p)
	changed := updateTask{InternalPairs: []pair{{InternalConnectionID: "a", InternalHostID: "host"}}}
	implCluster.InternalClientRoutesHandlerInvalidateRouteTask(p, changed)
	implCluster.InternalClientRoutesHandlerApplyRouteRead(p, changed, implCluster.InternalClientRoutesHandlerCaptureRouteEpochs(p), nil)
	newer := b
	newer.InternalAddress = "new-b"
	implCluster.InternalClientRoutesHandlerInvalidateRouteTask(p, updateTask{InternalPairs: []pair{{InternalConnectionID: "b", InternalHostID: "host"}}})
	implCluster.InternalClientRoutesHandlerApplyRouteRead(p, updateTask{InternalPairs: []pair{{InternalConnectionID: "b", InternalHostID: "host"}}}, implCluster.InternalClientRoutesHandlerCaptureRouteEpochs(p), []clientRoute{newer})
	if implCluster.InternalClientRoutesHandlerApplyRouteRead(p, updateTask{InternalConnectionIDs: []string{"a", "b"}}, old, []clientRoute{a, b}) {
		t.Fatal("late full read accepted changed pairs")
	}
	routes := implCluster.InternalClientRoutesHandlerOrderedRoutes(p, "host")
	if len(routes) != 1 || routes[0] != newer {
		t.Fatal("stale full read restored deletion or erased disjoint new route", routes)
	}
	implCluster.InternalClientRoutesHandlerBindRoute(p, newer)
	if route := implCluster.InternalClientRoutesHandlerOrderedRoutes(p, "host")[0]; route != newer {
		t.Fatal("sticky route lost")
	}
}
func TestStep05LatePoolConnectedCannotUndoDownOrRemoval(t *testing.T) {
	f := newNodeEventFixture(t)
	h := f.addHost(t, "127.0.10.1")
	old, _ := implCluster.SessionData(f.session).InternalPool.InternalGetPool(h)
	implCluster.InternalSessionHandleNodeDown(f.session, h.ConnectAddress(), h.Port())
	implCluster.InternalSessionHandlePoolConnected(f.session, old)
	if h.IsUp() {
		t.Fatal("delayed fill callback undid down")
	}
	implCluster.SessionData(f.session).InternalHostSource.InternalRemoveHost(h.HostID())
	implCluster.InternalSessionHandlePoolConnected(f.session, old)
	if h.IsUp() {
		t.Fatal("delayed callback resurrected removed host")
	}
}

func BenchmarkStep05MetadataRead(b *testing.B) {
	s := func() *Session {
		opaqueStorageValuexxxx := new(Session)
		return opaqueStorageValuexxxx
	}()

	s.Metadata()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = s.Metadata()
	}
}
func BenchmarkStep05EventPublication(b *testing.B) {
	s := func() *Session {
		opaqueStorageValuexxxxx := new(Session)
		return opaqueStorageValuexxxxx
	}()

	sub, _ := s.Subscribe(EventFilter{Capacity: 1})
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		implCluster.InternalSubscriptionPublish(sub, Event{Kind: StatusEvent})
		<-sub.Events()
	}
}

func TestStep05ClientRouteDNSAndConnectFallbackShareDeadline(t *testing.T) {
	ids := []UUID{MustRandomUUID(), MustRandomUUID(), MustRandomUUID()}
	srv := newControlTestServer(t, func(srv *controlTestServer) {
		srv.queries = func(query string, stream int, resp *framer) bool {
			if !strings.Contains(query, "system.client_routes") {
				return false
			}
			_, port, _ := net.SplitHostPort(srv.Address)
			var goodPort int
			fmt.Sscan(port, &goodPort)
			columns := []rowsCol{{"connection_id", optNative(TypeUUID)}, {"host_id", optNative(TypeUUID)}, {"address", optNative(TypeVarchar)}, {"port", optNative(TypeInt)}, {"tls_port", optNative(TypeInt)}}
			var rows [][][]byte
			for i, id := range ids {
				port := 1
				if i == 2 {
					port = goodPort
				}
				address := []string{"bad-dns", "bad-connect", "good"}[i]
				rows = append(rows, [][]byte{id[:], testLocalHostID[:], []byte(address), binary.BigEndian.AppendUint32(nil, uint32(port)), nil})
			}
			writeRows(resp, stream, columns, rows)
			return true
		}
	})
	config := DefaultConfig(srv.Address)
	config.Protocol = 4
	config.Metadata.Mode = MetadataDisabled
	config.Metadata.InitialDiscovery = false
	config.Pool.ShardAwarePort = false
	config.Pool.ConnectionsPerHost = 1
	config.Logger = nopLogger{}
	config.ClientRoutes = &ClientRoutesOptions{ConnectionIDs: []string{ids[0].String(), ids[1].String(), ids[2].String()}}
	config.DNSResolver = dnsResolverFunc(func(name string) ([]net.IP, error) {
		if name == "bad-dns" {
			return nil, errors.New("DNS unavailable")
		}
		return []net.IP{net.IPv4(127, 0, 0, 1)}, nil
	})
	var mu sync.Mutex
	var deadlines []time.Time
	var targets []string
	config.DialHost = func(ctx context.Context, target DialTarget) (net.Conn, error) {
		if !target.Host.ID.IsEmpty() {
			deadline, _ := ctx.Deadline()
			mu.Lock()
			deadlines = append(deadlines, deadline)
			targets = append(targets, target.Address)
			mu.Unlock()
		}
		return (DefaultDialer{}).DialContext(ctx, target)
	}
	s, err := Connect(context.Background(), config)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	route, ok := implCluster.ClientRoutesHandlerData(implCluster.SessionData(s).InternalClientRoutesHandler).InternalRouteCache.PreferredRoute(testLocalHostID.String())
	if !ok || route.InternalConnectionID != ids[2].String() {
		t.Fatal("did not bind successful configured alternative", route)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(deadlines) < 2 || !deadlines[0].Equal(deadlines[1]) {
		t.Fatal("route alternatives restarted setup budget", deadlines, targets)
	}
}

func TestStep05SuccessfulDDLErrorNeverRetriesStatement(t *testing.T) {
	s, conns, peers := runtimeTestSession(t, 1)
	conn := conns[0]
	config := runtimeFor(s).InternalConfig
	config.Metadata.AutomaticAgreement = true
	config.Metadata.RefreshAfterAgreement = false
	config.Metadata.AgreementTimeout = 100 * time.Millisecond
	runtimeFor(s).InternalConfig = config
	implCluster.InternalSessionConfigureV2Peripheral(s, config)
	metadata := s.Metadata()
	metadataCopy := *metadata
	routing.SnapshotData(&metadataCopy).InternalHosts = append([]HostSnapshot(nil), routing.SnapshotData(metadata).InternalHosts...)
	routing.SnapshotData(&metadataCopy).InternalHosts = append(routing.SnapshotData(&metadataCopy).InternalHosts, HostSnapshot{ID: MustRandomUUID(), Enabled: true})
	implCluster.InternalSessionPeripheral(s).InternalSnapshot.Store(&metadataCopy)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	var ddl atomic.Int32
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			var header [9]byte
			if _, err := io.ReadFull(peers[0], header[:]); err != nil {
				return
			}
			size := binary.BigEndian.Uint32(header[5:])
			body := make([]byte, size)
			if _, err := io.ReadFull(peers[0], body); err != nil {
				return
			}
			stream := int(binary.BigEndian.Uint16(header[2:4]))
			query := ""
			if len(body) >= 4 {
				n := int(binary.BigEndian.Uint32(body))
				if n <= len(body)-4 {
					query = string(body[4 : 4+n])
				}
			}
			if strings.HasPrefix(query, "CREATE") {
				ddl.Add(1)
				response := binary.BigEndian.AppendUint32(nil, frm.ResultKindSchemaChanged)
				for _, v := range []string{"CREATED", "KEYSPACE", "step05"} {
					response = binary.BigEndian.AppendUint16(response, uint16(len(v)))
					response = append(response, v...)
				}
				if _, err := peers[0].Write(typedTestResponse(t, 4, stream, response)); err != nil {
					return
				}
			} else {
				if _, err := peers[0].Write(step03ErrorResponse(t, stream, ErrCodeUnauthorized, nil)); err != nil {
					return
				}
			}
		}
	}()
	b := codecTestMust(BindUnpreparedOwned(Statement[Unit, Unit]{CQL: "CREATE KEYSPACE step05", Params: NoParams, Rows: NoRows}, NewColumns(), Unit{}))
	defer b.Close()
	_, err := Exec(ctx, s, b, *func() *Options {
		opaqueStorageValuexxxxxx := new(Options)
		implExecution.OptionsData(opaqueStorageValuexxxxxx).InternalConnection = conn
		opaqueStorageValuexxxxxx.Idempotent = true
		opaqueStorageValuexxxxxx.Retry = step03RetryOnce{}
		return opaqueStorageValuexxxxxx
	}())
	if err == nil || !strings.Contains(err.Error(), "DDL succeeded") {
		t.Fatal("successful DDL failure not distinguished", err)
	}
	conn.Close()
	peers[0].Close()
	<-done
	if ddl.Load() != 1 {
		t.Fatal("successful DDL replayed", ddl.Load())
	}
}

type step05LifecycleFunc func(LifecycleEvent)

func (f step05LifecycleFunc) ObserveLifecycle(e LifecycleEvent) { f(e) }
func TestStep05ConnectedCallbackPausedAcrossNewerDown(t *testing.T) {
	f := newNodeEventFixture(t)
	host := f.addHost(t, "127.0.10.1")
	conn, _ := typedTestConn(t, 4, nil)
	conn.InternalHost = host
	pool := &hostConnPool{InternalHost: host, InternalServices: newPoolServices(f.session), InternalConnPicker: &defaultConnPicker{InternalConns: []*driverConn{conn}}}
	implCluster.SessionData(f.session).InternalPool.InternalMu.Lock()
	implCluster.SessionData(f.session).InternalPool.InternalHostConnPools[implEndpoint.InternalHostInfoHostUUID(host)] = pool
	implCluster.SessionData(f.session).InternalPool.InternalMu.Unlock()
	entered, release, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
	p := implCluster.InternalSessionPeripheral(f.session)
	p.InternalConfig.LifecycleObserver = step05LifecycleFunc(func(e LifecycleEvent) {
		if e.Stage == "connected" {
			close(entered)
			<-release
		}
	})
	go func() { defer close(done); implCluster.InternalSessionHandlePoolConnected(f.session, pool) }()
	<-entered
	implCluster.InternalSessionHandleNodeDown(f.session, host.ConnectAddress(), host.Port())
	close(release)
	<-done
	if host.IsUp() || len(f.policy.hostUpCalls()) != 0 {
		t.Fatal("delayed callback reintroduced host after DOWN", f.policy.hostUpCalls())
	}
}
func TestStep05TypedTopologyRejectsMalformedTokensAndPorts(t *testing.T) {
	for _, row := range []v2TopologyRow{{InternalPort: -1}, {InternalPort: 65536}, {InternalPort: 9042, InternalTokens: []string{"not-a-token"}}} {
		if _, err := row.InternalHost(9042); err == nil {
			t.Fatal("malformed topology accepted", row)
		}
	}
}

func TestStep05DisabledEventsDoNotMutateMetadata(t *testing.T) {
	s := func() *Session {
		opaqueStorageValuexxxxxxx := new(Session)
		return opaqueStorageValuexxxxxxx
	}()

	defer s.Close()
	implCluster.SessionData(s).InternalCfg.Events.DisableSchemaEvents = true
	implCluster.SessionData(s).InternalCfg.Events.DisableTopologyEvents = true
	implCluster.SessionData(s).InternalCfg.Events.DisableNodeStatusEvents = true
	implCluster.InternalClusterSettingsData(&implCluster.SessionData(s).InternalCfg).InternalDisableClientRouteEvents = true
	before := s.Metadata()
	for _, event := range []frame{&frm.SchemaChangeTable{Change: "DROPPED", Keyspace: "ks", Object: "t"}, &frm.TopologyChangeEventFrame{}, &frm.StatusChangeEventFrame{}, &frm.ClientRoutesChanged{}} {
		implCluster.InternalSessionHandleEvent(s, event)
	}
	if s.Metadata() != before {
		t.Fatal("disabled unsolicited event changed snapshot")
	}
}
func TestStep05PartialBaseReadFailureRetainsLatestSnapshot(t *testing.T) {
	s, mock := refreshAllSchemaSession(t)
	mock.mu.Lock()
	mock.queryError = errors.New("base schema denied temporarily")
	mock.mu.Unlock()
	var partial *PartialRefreshError
	if err := implCluster.InternalSessionRefreshV2Keyspace(s, context.Background(), "ks1"); !errors.As(err, &partial) {
		t.Fatal("retained stale base schema lacks partial error", err)
	}
	if ks, _ := s.Metadata().Keyspace("ks1"); ks == nil || ks.Tables["t1"] == nil {
		t.Fatal("base read failure erased valid schema")
	}
	if len(s.Metadata().Diagnostics()) == 0 {
		t.Fatal("stale schema not observable")
	}
}

func TestStep05EventChargeTransfersUntilDownstreamConsumptionAndDrain(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	var freed atomic.Int32
	downstream := newEventDebouncer("owned", func([]frame) { close(entered); <-release }, nopLogger{})
	conn, _ := typedTestConn(t, 4, nil)
	s := func() *Session {
		opaqueStorageValuexxxxxxxx := new(Session)
		implCluster.SessionData(opaqueStorageValuexxxxxxxx).InternalLogger = nopLogger{}
		implCluster.SessionData(opaqueStorageValuexxxxxxxx).InternalSchemaEvents = downstream
		return opaqueStorageValuexxxxxxxx
	}()
	conn.InternalHooks = newConnectionHooks(s)
	if err := conn.InternalDispatchEventOwned(&frm.SchemaChangeTable{Change: "CREATED", Keyspace: "ks", Object: "t"}, func() { freed.Add(1) }); err != nil {
		t.Fatal(err)
	}
	<-entered
	if freed.Load() != 0 {
		t.Fatal("connection dispatcher released retained downstream event")
	}
	if !downstream.InternalDebounceOwned(&frm.SchemaChangeTable{}, func() { freed.Add(1) }) {
		t.Fatal("second owned event rejected")
	}
	closed := make(chan struct{})
	go func() { s.Close(); close(closed) }()
	select {
	case <-closed:
		t.Fatal("shutdown did not join stalled downstream consumer")
	default:
	}
	close(release)
	<-closed
	conn.InternalWaitEventDispatch()
	if freed.Load() != 2 {
		t.Fatal("downstream consumption/terminal drain did not release exactly once", freed.Load())
	}
	s.Close()
	if freed.Load() != 2 {
		t.Fatal("repeated closure released event twice")
	}
}
func TestStep05RouteEventExpansionFallsBackToFullRefresh(t *testing.T) {
	p := NewClientRoutesAddressTranslator(ClientRoutesConfig{Endpoints: ClientRoutesEndpointList{{ConnectionID: "allowed"}}}, nil, false, nopLogger{})
	// More than the bounded pair capacity must schedule a full connection scope,
	// without allocating the Cartesian product or dropping required recovery.
	hosts := make([]string, 1025)
	for i := range hosts {
		hosts[i] = fmt.Sprint(i)
	}
	var freed atomic.Int32
	if !implCluster.InternalClientRoutesHandlerHandleRouteEventOwned(p, &oldevents.ClientRoutesChangedEvent{ChangeType: "UPDATED", ConnectionIDs: []string{"allowed"}, HostIDs: hosts}, func() { freed.Add(1) }) {
		t.Fatal("full-refresh fallback not admitted")
	}
	task := <-implCluster.ClientRoutesHandlerData(p).InternalUpdateTasks
	if task.InternalPairs != nil || !reflect.DeepEqual(task.InternalConnectionIDs, []string{"allowed"}) {
		t.Fatal("oversized event was not coalesced into full refresh")
	}
	if freed.Load() != 0 {
		t.Fatal("route event released before downstream consumption")
	}
	task.InternalRelease()
	if freed.Load() != 1 {
		t.Fatal("route ownership lost")
	}
	// Queue drain owns every accepted event release during terminal shutdown.
	for i := 0; i < 3; i++ {
		if !implCluster.InternalClientRoutesHandlerUpdateHostPortMappingAsync(p, updateTask{InternalConnectionIDs: []string{"allowed"}, InternalRelease: func() { freed.Add(1) }}) {
			t.Fatal("owned task rejected")
		}
	}
	close(implCluster.ClientRoutesHandlerData(p).InternalCloseChan)
	implCluster.InternalClientRoutesHandlerStartUpdateWorker(p)
	implCluster.ClientRoutesHandlerData(p).InternalWorkers.Wait()
	if freed.Load() != 4 {
		t.Fatal("route worker terminal drain lost/duplicated ownership", freed.Load())
	}
}

func BenchmarkStep05SchemaSnapshotPublication(b *testing.B) {
	for _, count := range []int{1, 1000} {
		b.Run(fmt.Sprint(count), func(b *testing.B) {
			ks := &KeyspaceMetadata{Name: "ks", Tables: make(map[string]*TableMetadata, count)}
			for i := 0; i < count; i++ {
				name := fmt.Sprint(i)
				ks.Tables[name] = &TableMetadata{Name: name, Columns: map[string]*ColumnMetadata{"id": {Name: "id", Type: "int"}}}
			}
			s := func() *Session {
				opaqueStorageValuexxxxxxxxxx := new(Session)
				implCluster.SessionData(opaqueStorageValuexxxxxxxxxx).InternalMetadataDescriber = &metadataDescriber{InternalMetadata: func() *Metadata {
					opaqueStorageValuexxxxxxxxx := new(Metadata)
					return opaqueStorageValuexxxxxxxxx
				}(),
				}
				return opaqueStorageValuexxxxxxxxxx
			}()
			implCluster.MetadataData(implCluster.SessionData(s).InternalMetadataDescriber.InternalMetadata).InternalKeyspaceMetadata.InternalSet("ks", ks)
			implCluster.InternalSessionPublishV2Metadata(s)
			prior := routing.SnapshotData(s.Metadata()).InternalKeyspaces["ks"]
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				implCluster.InternalSessionPublishV2Metadata(s)
			}
			b.StopTimer()
			if routing.SnapshotData(s.Metadata()).InternalKeyspaces["ks"] != prior {
				b.Fatal("unchanged schema was cloned")
			}
		})
	}
}

func TestStep05RouteDeletionAdvancesTransportInsertionEpoch(t *testing.T) {
	p := NewClientRoutesAddressTranslator(ClientRoutesConfig{}, nil, false, nopLogger{})
	host := func() *HostInfo {
		opaqueStorageValuexxxxxxxxxxx := new(HostInfo)
		implEndpoint.HostInfoData(opaqueStorageValuexxxxxxxxxxx).InternalHostId = MustRandomUUID()
		implEndpoint.HostInfoData(opaqueStorageValuexxxxxxxxxxx).InternalConnectAddress = net.IPv4(127, 0, 0, 1)
		implEndpoint.HostInfoData(opaqueStorageValuexxxxxxxxxxx).InternalPort = 9042
		return opaqueStorageValuexxxxxxxxxxx
	}()
	s := func() *Session {
		opaqueStorageValuexxxxxxxxxxxx := new(Session)
		implCluster.SessionData(opaqueStorageValuexxxxxxxxxxxx).InternalHostSource = &ringDescriber{InternalHosts: map[string]*HostInfo{host.HostID(): host}}
		return opaqueStorageValuexxxxxxxxxxxx
	}()
	implCluster.ClientRoutesHandlerData(p).InternalSession = s
	route := clientRoute{InternalConnectionID: "connection", InternalHostID: host.HostID(), InternalAddress: "proxy", InternalPort: 9042}
	implCluster.ClientRoutesHandlerData(p).InternalRouteCache.ReplaceByConnectionIDs([]string{"connection"}, []clientRoute{route})
	before := implEndpoint.InternalHostInfoTransportEpoch(host)
	implCluster.InternalClientRoutesHandlerInvalidateRouteTask(p, updateTask{InternalPairs: []pair{{InternalConnectionID: "connection", InternalHostID: host.HostID()}}})
	if implEndpoint.InternalHostInfoTransportEpoch(host) == before {
		t.Fatal("deletion left late validated transport insertion eligible")
	}
}
func TestStep05EmptyRouteFullReadDetectsNewerPair(t *testing.T) {
	p := NewClientRoutesAddressTranslator(ClientRoutesConfig{}, nil, false, nopLogger{})
	before := implCluster.InternalClientRoutesHandlerCaptureRouteEpochs(p)
	implCluster.InternalClientRoutesHandlerInvalidateRouteTask(p, updateTask{InternalPairs: []pair{{InternalConnectionID: "connection", InternalHostID: "new-host"}}})
	if implCluster.InternalClientRoutesHandlerApplyRouteRead(p, updateTask{InternalConnectionIDs: []string{"connection"}}, before, nil) {
		t.Fatal("empty stale full read falsely reported accepted publication")
	}
}

func TestStep05DDLSucceededStatusSurvivesAgreementCancellation(t *testing.T) {
	s, conns, peers := runtimeTestSession(t, 1)
	conn := conns[0]
	config := runtimeFor(s).InternalConfig
	config.Metadata.AutomaticAgreement = true
	config.Metadata.RefreshAfterAgreement = false
	runtimeFor(s).InternalConfig = config
	implCluster.InternalSessionConfigureV2Peripheral(s, config)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	entered, serverDone := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(serverDone)
		stream := typedTestReadRequest(t, conn, peers[0])
		body := binary.BigEndian.AppendUint32(nil, frm.ResultKindSchemaChanged)
		for _, value := range []string{"CREATED", "KEYSPACE", "step05"} {
			body = binary.BigEndian.AppendUint16(body, uint16(len(value)))
			body = append(body, value...)
		}
		if _, err := peers[0].Write(typedTestResponse(t, 4, stream, body)); err != nil {
			return
		}
		typedTestReadRequest(t, conn, peers[0])
		close(entered)
		<-ctx.Done()
	}()
	bound := codecTestMust(BindUnpreparedOwned(Statement[Unit, Unit]{CQL: "CREATE KEYSPACE step05", Params: NoParams, Rows: NoRows}, NewColumns(), Unit{}))
	defer bound.Close()
	result := make(chan error, 1)
	go func() {
		_, err := Exec(ctx, s, bound, *func() *Options {
			opaqueStorageValuexxxxxxxxxxxxx := new(Options)
			implExecution.OptionsData(opaqueStorageValuexxxxxxxxxxxxx).InternalConnection = conn
			return opaqueStorageValuexxxxxxxxxxxxx
		}())
		result <- err
	}()
	<-entered
	cancel()
	err := <-result
	if !errors.Is(err, context.Canceled) || !strings.Contains(err.Error(), "DDL succeeded") {
		t.Fatal("canceled post-DDL agreement lost acknowledged success", err)
	}
	conn.Close()
	peers[0].Close()
	<-serverDone
}

type step05UnstablePolicy struct {
	RoutingPolicy
	session *Session
}

func (p step05UnstablePolicy) Validate(*MetadataSnapshot) error {
	state := implCluster.InternalSessionPeripheral(p.session)
	state.InternalMu.Lock()
	state.InternalPublicationRevision++
	state.InternalMu.Unlock()
	return nil
}
func TestStep05PublicationContentionEmitsTerminalResync(t *testing.T) {
	s := func() *Session {
		opaqueStorageValuexxxxxxxxxxxxxx := new(Session)
		implCluster.SessionData(opaqueStorageValuexxxxxxxxxxxxxx).InternalHostSource = &ringDescriber{}
		implCluster.SessionData(opaqueStorageValuexxxxxxxxxxxxxx).InternalLogger = nopLogger{}
		return opaqueStorageValuexxxxxxxxxxxxxx
	}()
	defer s.Close()
	s.Metadata()
	config := DefaultConfig("localhost")
	profile := config.DefaultProfile.Snapshot()
	profile.Routing = step05UnstablePolicy{RoutingPolicy: profile.Routing, session: s}
	if err := config.DefaultProfile.Replace(profile); err != nil {
		t.Fatal(err)
	}
	implCluster.InternalSessionConfigureV2Peripheral(s, config)
	sub, _ := s.Subscribe(EventFilter{Capacity: 4})
	implCluster.InternalSessionHandleNodeEvent(s, []frame{&frm.StatusChangeEventFrame{Change: "DOWN", Host: net.IPv4(127, 0, 0, 1), Port: 9042}})
	event := <-sub.Events()
	if event.Kind != OverflowEvent || event.Change != "PUBLICATION_RESYNC" {
		t.Fatal("unpublished delta escaped", event)
	}
	if _, ok := <-sub.Events(); ok {
		t.Fatal("resync marker was not terminal")
	}
}

func TestStep05SchemaRetryRejectedAfterShutdownClearsRegistration(t *testing.T) {
	s := func() *Session {
		opaqueStorageValuexxxxxxxxxxxxxxx := new(Session)
		return opaqueStorageValuexxxxxxxxxxxxxxx
	}()

	implCluster.InternalSessionCloseV2Peripheral(s)
	implCluster.InternalSessionStartV2SchemaRetry(s)
	if implCluster.InternalSessionPeripheral(s).InternalRetryScheduled.Load() {
		t.Fatal("denied worker retained a retry registration")
	}
	s.Close()
}
