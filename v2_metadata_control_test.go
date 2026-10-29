//go:build unit

package gocql

import (
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
	if err := s.refreshRingNow(); !errors.As(err, &partial) || partial.Topology == nil {
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
	s.recordV2Topology(&oldevents.TopologyChangeEvent{Change: "REMOVED_NODE", Host: peer.ip, Port: 9042})
	if s.hostSource.getHost(peer.hostID.String()) != nil {
		t.Fatal("removed host still visible")
	}
	// Gossip still advertises its old row. A post-event fetch cannot resurrect it.
	if err := s.refreshRingNow(); err != nil {
		t.Fatal(err)
	}
	if s.hostSource.getHost(peer.hostID.String()) != nil {
		t.Fatal("stale gossip resurrected removed host")
	}
	s.recordV2Topology(&oldevents.TopologyChangeEvent{Change: "NEW_NODE", Host: peer.ip, Port: 9042})
	if err := s.refreshRingNow(); err != nil {
		t.Fatal(err)
	}
	if s.hostSource.getHost(peer.hostID.String()) == nil {
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
	s.configureV2Peripheral(base)
	policy.block.Store(true)
	done := make(chan struct{})
	go func() { defer close(done); s.publishV2Metadata() }()
	<-policy.entered
	s.publishV2Event(&oldevents.SchemaChangeTableEvent{Change: "DROPPED", Keyspace: "ks1", Table: "t1"})
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
	mock.iterFor = func(stmt string) *Iter {
		if strings.Contains(stmt, "FROM system_schema.tables") && gate.CompareAndSwap(false, true) {
			close(entered)
			<-release
		}
		return nil
	}
	done := make(chan error, 1)
	go func() { done <- s.metadataDescriber.refreshTableSchema("ks1", "t1") }()
	<-entered
	s.publishV2Event(&oldevents.SchemaChangeTableEvent{Change: "DROPPED", Keyspace: "ks1", Table: "t1"})
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
	_ = s.refreshV2Keyspace(context.Background(), "ks1")
	if ks, _ := s.Metadata().Keyspace("ks1"); ks.Tables["t1"] != nil {
		t.Fatal("failure fallback resurrected table")
	}
}

func TestStep05SchemaCapabilityAndMetadataScope(t *testing.T) {
	s, mock := refreshAllSchemaSession(t)
	s.useSystemSchema = false
	if err := s.refreshV2AllSchema(context.Background()); err == nil {
		t.Fatal("unsupported schema silently succeeded")
	} else {
		var unsupported *UnsupportedSettingError
		if !errors.As(err, &unsupported) {
			t.Fatal(err)
		}
	}
	s.useSystemSchema = true
	for _, mode := range []MetadataMode{MetadataDisabled, MetadataMinimal, MetadataFull} {
		config := DefaultConfig("localhost")
		config.Metadata.Mode = mode
		config.Metadata.Keyspaces = []string{"ks1"}
		s.configureV2Peripheral(config)
		mock.mu.Lock()
		mock.queries = nil
		mock.mu.Unlock()
		if err := s.refreshV2AllSchema(context.Background()); err != nil {
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
	s := &Session{}
	defer s.Close()
	slow, _ := s.Subscribe(EventFilter{Capacity: 2})
	fast, _ := s.Subscribe(EventFilter{Capacity: 8})
	defer fast.Close()
	for _, generation := range []uint64{40, 99, 41} {
		s.publishV2LocalEvent(Event{Kind: StatusEvent, Generation: generation})
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
	if len(s.peripheral().subscribers) != 1 {
		t.Fatal("terminal subscriber leaked in registry")
	}
}

func TestStep05ShutdownJoinsWorkersAndRepeatedClosers(t *testing.T) {
	s := &Session{}
	p := s.peripheral()
	canceled, release := make(chan struct{}), make(chan struct{})
	p.startWorker(func() { <-p.done; close(canceled); <-release })
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
	if p.startWorker(func() { t.Error("worker admitted after close") }) {
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
	d.resync = func() { resync.Add(1) }
	for i := 0; i < eventBufferSize+1; i++ {
		d.debounce(&frm.StatusChangeEventFrame{})
	}
	d.mu.Lock()
	if len(d.events) != eventBufferSize || !d.overflow {
		t.Fatal("unbounded queue or lost overflow")
	}
	d.timer.Reset(0)
	d.mu.Unlock()
	<-entered
	stopped := make(chan struct{})
	go func() { d.stop(); close(stopped) }()
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
	d.stop()
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
func step05AgreementConn(t *testing.T, s *Session, id, version UUID, code int) (*Conn, *atomic.Int32) {
	t.Helper()
	c, server := typedTestConn(t, 4, nil)
	c.host = &HostInfo{hostId: id, connectAddress: net.IPv4(127, 0, 0, 1), port: 9042}
	c.session = s
	go c.serve(c.ctx)
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
	t.Cleanup(func() { c.Close(); server.Close(); <-done; c.waitEventDispatch() })
	return c, count
}
func step05AgreementSession(t *testing.T) (*Session, UUID, UUID) {
	t.Helper()
	s := &Session{logger: nopLogger{}}
	config := DefaultConfig("localhost")
	config.Metadata.AgreementTimeout = 35 * time.Millisecond
	config.Metadata.AgreementPoll = time.Millisecond
	s.configureV2Peripheral(config)
	s.pool = &policyConnPool{hostConnPools: make(map[UUID]*hostConnPool)}
	t.Cleanup(func() { s.Close() })
	return s, MustRandomUUID(), MustRandomUUID()
}
func step05AgreementPool(s *Session, c *Conn) {
	s.pool.hostConnPools[c.host.hostUUID()] = &hostConnPool{host: c.host, connPicker: &defaultConnPicker{conns: []*Conn{c}}}
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
			s.peripheral().snapshot.Store(&MetadataSnapshot{hosts: hosts})
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
	s.peripheral().snapshot.Store(&MetadataSnapshot{hosts: []HostSnapshot{{ID: id, Enabled: true}, {ID: MustRandomUUID(), Enabled: false}}})
	if err := s.awaitV2SchemaAgreementOn(context.Background(), coordinator); err != nil {
		t.Fatal(err)
	}
	if count.Load() != 1 {
		t.Fatal("excluded exact coordinator not queried", count.Load())
	}
	different, _ := step05AgreementConn(t, s, coordinatorID, MustRandomUUID(), 0)
	if err := s.awaitV2SchemaAgreementOn(context.Background(), different); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("older topology agreement ignored coordinator schema", err)
	}
}
func TestStep05AgreementTerminalAuthorization(t *testing.T) {
	s, id, _ := step05AgreementSession(t)
	c, count := step05AgreementConn(t, s, id, MustRandomUUID(), ErrCodeUnauthorized)
	step05AgreementPool(s, c)
	s.peripheral().snapshot.Store(&MetadataSnapshot{hosts: []HostSnapshot{{ID: id, Enabled: true}}})
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
	a := clientRoute{"a", "host", "old", 9042}
	b := clientRoute{"b", "host", "b", 9042}
	p.routeCache.ReplaceByConnectionIDs([]string{"a", "b"}, []clientRoute{b, a})
	if route := p.orderedRoutes("host")[0]; route.connectionID != "a" {
		t.Fatal("server row order overrode config order", route)
	}
	old := p.captureRouteEpochs()
	changed := updateTask{pairs: []pair{{"a", "host"}}}
	p.invalidateRouteTask(changed)
	p.applyRouteRead(changed, p.captureRouteEpochs(), nil)
	newer := b
	newer.address = "new-b"
	p.invalidateRouteTask(updateTask{pairs: []pair{{"b", "host"}}})
	p.applyRouteRead(updateTask{pairs: []pair{{"b", "host"}}}, p.captureRouteEpochs(), []clientRoute{newer})
	if p.applyRouteRead(updateTask{connectionIDs: []string{"a", "b"}}, old, []clientRoute{a, b}) {
		t.Fatal("late full read accepted changed pairs")
	}
	routes := p.orderedRoutes("host")
	if len(routes) != 1 || routes[0] != newer {
		t.Fatal("stale full read restored deletion or erased disjoint new route", routes)
	}
	p.bindRoute(newer)
	if route := p.orderedRoutes("host")[0]; route != newer {
		t.Fatal("sticky route lost")
	}
}
func TestStep05LatePoolConnectedCannotUndoDownOrRemoval(t *testing.T) {
	f := newNodeEventFixture(t)
	h := f.addHost(t, "127.0.10.1")
	old, _ := f.session.pool.getPool(h)
	f.session.handleNodeDown(h.ConnectAddress(), h.Port())
	f.session.handlePoolConnected(old)
	if h.IsUp() {
		t.Fatal("delayed fill callback undid down")
	}
	f.session.hostSource.removeHost(h.HostID())
	f.session.handlePoolConnected(old)
	if h.IsUp() {
		t.Fatal("delayed callback resurrected removed host")
	}
}

func BenchmarkStep05MetadataRead(b *testing.B) {
	s := &Session{}
	s.Metadata()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = s.Metadata()
	}
}
func BenchmarkStep05EventPublication(b *testing.B) {
	s := &Session{}
	sub, _ := s.Subscribe(EventFilter{Capacity: 1})
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sub.publish(Event{Kind: StatusEvent})
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
	route, ok := s.clientRoutesHandler.routeCache.PreferredRoute(testLocalHostID.String())
	if !ok || route.connectionID != ids[2].String() {
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
	config := s.typedRuntime().config
	config.Metadata.AutomaticAgreement = true
	config.Metadata.RefreshAfterAgreement = false
	config.Metadata.AgreementTimeout = 100 * time.Millisecond
	s.typedRuntime().config = config
	s.configureV2Peripheral(config)
	metadata := s.Metadata()
	metadataCopy := *metadata
	metadataCopy.hosts = append([]HostSnapshot(nil), metadata.hosts...)
	metadataCopy.hosts = append(metadataCopy.hosts, HostSnapshot{ID: MustRandomUUID(), Enabled: true})
	s.peripheral().snapshot.Store(&metadataCopy)
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
	_, err := Exec(ctx, s, b, Options{connection: conn, Idempotent: true, Retry: step03RetryOnce{}})
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
	conn.host = host
	pool := &hostConnPool{host: host, session: f.session, connPicker: &defaultConnPicker{conns: []*Conn{conn}}}
	f.session.pool.mu.Lock()
	f.session.pool.hostConnPools[host.hostUUID()] = pool
	f.session.pool.mu.Unlock()
	entered, release, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
	p := f.session.peripheral()
	p.config.LifecycleObserver = step05LifecycleFunc(func(e LifecycleEvent) {
		if e.Stage == "connected" {
			close(entered)
			<-release
		}
	})
	go func() { defer close(done); f.session.handlePoolConnected(pool) }()
	<-entered
	f.session.handleNodeDown(host.ConnectAddress(), host.Port())
	close(release)
	<-done
	if host.IsUp() || len(f.policy.hostUpCalls()) != 0 {
		t.Fatal("delayed callback reintroduced host after DOWN", f.policy.hostUpCalls())
	}
}
func TestStep05TypedTopologyRejectsMalformedTokensAndPorts(t *testing.T) {
	for _, row := range []v2TopologyRow{{port: -1}, {port: 65536}, {port: 9042, tokens: []string{"not-a-token"}}} {
		if _, err := row.host(9042); err == nil {
			t.Fatal("malformed topology accepted", row)
		}
	}
}

func TestStep05DisabledEventsDoNotMutateMetadata(t *testing.T) {
	s := &Session{}
	defer s.Close()
	s.cfg.Events.DisableSchemaEvents = true
	s.cfg.Events.DisableTopologyEvents = true
	s.cfg.Events.DisableNodeStatusEvents = true
	s.cfg.disableClientRouteEvents = true
	before := s.Metadata()
	for _, event := range []frame{&frm.SchemaChangeTable{Change: "DROPPED", Keyspace: "ks", Object: "t"}, &frm.TopologyChangeEventFrame{}, &frm.StatusChangeEventFrame{}, &frm.ClientRoutesChanged{}} {
		s.handleEvent(event)
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
	if err := s.refreshV2Keyspace(context.Background(), "ks1"); !errors.As(err, &partial) {
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
	s := &Session{logger: nopLogger{}, schemaEvents: downstream}
	conn.session = s
	if err := conn.dispatchEventOwned(&frm.SchemaChangeTable{Change: "CREATED", Keyspace: "ks", Object: "t"}, func() { freed.Add(1) }); err != nil {
		t.Fatal(err)
	}
	<-entered
	if freed.Load() != 0 {
		t.Fatal("connection dispatcher released retained downstream event")
	}
	if !downstream.debounceOwned(&frm.SchemaChangeTable{}, func() { freed.Add(1) }) {
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
	conn.waitEventDispatch()
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
	if !p.handleRouteEventOwned(&oldevents.ClientRoutesChangedEvent{ChangeType: "UPDATED", ConnectionIDs: []string{"allowed"}, HostIDs: hosts}, func() { freed.Add(1) }) {
		t.Fatal("full-refresh fallback not admitted")
	}
	task := <-p.updateTasks
	if task.pairs != nil || !reflect.DeepEqual(task.connectionIDs, []string{"allowed"}) {
		t.Fatal("oversized event was not coalesced into full refresh")
	}
	if freed.Load() != 0 {
		t.Fatal("route event released before downstream consumption")
	}
	task.release()
	if freed.Load() != 1 {
		t.Fatal("route ownership lost")
	}
	// Queue drain owns every accepted event release during terminal shutdown.
	for i := 0; i < 3; i++ {
		if !p.updateHostPortMappingAsync(updateTask{connectionIDs: []string{"allowed"}, release: func() { freed.Add(1) }}) {
			t.Fatal("owned task rejected")
		}
	}
	close(p.closeChan)
	p.startUpdateWorker()
	p.workers.Wait()
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
			s := &Session{metadataDescriber: &metadataDescriber{metadata: &Metadata{}}}
			s.metadataDescriber.metadata.keyspaceMetadata.set("ks", ks)
			s.publishV2Metadata()
			prior := s.Metadata().keyspaces["ks"]
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				s.publishV2Metadata()
			}
			b.StopTimer()
			if s.Metadata().keyspaces["ks"] != prior {
				b.Fatal("unchanged schema was cloned")
			}
		})
	}
}

func TestStep05RouteDeletionAdvancesTransportInsertionEpoch(t *testing.T) {
	p := NewClientRoutesAddressTranslator(ClientRoutesConfig{}, nil, false, nopLogger{})
	host := &HostInfo{hostId: MustRandomUUID(), connectAddress: net.IPv4(127, 0, 0, 1), port: 9042}
	s := &Session{hostSource: &ringDescriber{hosts: map[string]*HostInfo{host.HostID(): host}}}
	p.session = s
	route := clientRoute{"connection", host.HostID(), "proxy", 9042}
	p.routeCache.ReplaceByConnectionIDs([]string{"connection"}, []clientRoute{route})
	before := host.transportEpoch()
	p.invalidateRouteTask(updateTask{pairs: []pair{{"connection", host.HostID()}}})
	if host.transportEpoch() == before {
		t.Fatal("deletion left late validated transport insertion eligible")
	}
}
func TestStep05EmptyRouteFullReadDetectsNewerPair(t *testing.T) {
	p := NewClientRoutesAddressTranslator(ClientRoutesConfig{}, nil, false, nopLogger{})
	before := p.captureRouteEpochs()
	p.invalidateRouteTask(updateTask{pairs: []pair{{"connection", "new-host"}}})
	if p.applyRouteRead(updateTask{connectionIDs: []string{"connection"}}, before, nil) {
		t.Fatal("empty stale full read falsely reported accepted publication")
	}
}

func TestStep05DDLSucceededStatusSurvivesAgreementCancellation(t *testing.T) {
	s, conns, peers := runtimeTestSession(t, 1)
	conn := conns[0]
	config := s.typedRuntime().config
	config.Metadata.AutomaticAgreement = true
	config.Metadata.RefreshAfterAgreement = false
	s.typedRuntime().config = config
	s.configureV2Peripheral(config)
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
	go func() { _, err := Exec(ctx, s, bound, Options{connection: conn}); result <- err }()
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
	state := p.session.peripheral()
	state.mu.Lock()
	state.publicationRevision++
	state.mu.Unlock()
	return nil
}
func TestStep05PublicationContentionEmitsTerminalResync(t *testing.T) {
	s := &Session{hostSource: &ringDescriber{}, logger: nopLogger{}}
	defer s.Close()
	s.Metadata()
	config := DefaultConfig("localhost")
	profile := config.DefaultProfile.Snapshot()
	profile.Routing = step05UnstablePolicy{RoutingPolicy: profile.Routing, session: s}
	if err := config.DefaultProfile.Replace(profile); err != nil {
		t.Fatal(err)
	}
	s.configureV2Peripheral(config)
	sub, _ := s.Subscribe(EventFilter{Capacity: 4})
	s.handleNodeEvent([]frame{&frm.StatusChangeEventFrame{Change: "DOWN", Host: net.IPv4(127, 0, 0, 1), Port: 9042}})
	event := <-sub.Events()
	if event.Kind != OverflowEvent || event.Change != "PUBLICATION_RESYNC" {
		t.Fatal("unpublished delta escaped", event)
	}
	if _, ok := <-sub.Events(); ok {
		t.Fatal("resync marker was not terminal")
	}
}

func TestStep05SchemaRetryRejectedAfterShutdownClearsRegistration(t *testing.T) {
	s := &Session{}
	s.closeV2Peripheral()
	s.startV2SchemaRetry()
	if s.peripheral().retryScheduled.Load() {
		t.Fatal("denied worker retained a retry registration")
	}
	s.Close()
}
