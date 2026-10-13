//go:build unit
// +build unit

package gocql

import (
	"net"
	"sort"
	"strings"
	"sync"
	"testing"
)

func TestHostInfoFromMap(t *testing.T) {
	t.Parallel()

	hostID := testLocalHostID
	schema := MustRandomUUID()
	row := map[string]any{
		"data_center":       "dc1",
		"rack":              "r1",
		"host_id":           hostID,
		"release_version":   "3.11.4",
		"peer":              "10.0.0.5",
		"cluster_name":      "test cluster",
		"partitioner":       "org.apache.cassandra.dht.Murmur3Partitioner",
		"broadcast_address": "10.0.0.6",
		"preferred_ip":      "10.0.0.7",
		"rpc_address":       "10.0.0.8",
		"listen_address":    "10.0.0.9",
		"tokens":            []string{"1", "2"},
		"schema_version":    schema,
		"unknown_column":    42,
	}

	h, err := hostInfoFromMap(row, 9042)
	if err != nil {
		t.Fatal(err)
	}
	checks := []struct {
		name      string
		got, want any
	}{
		{"DataCenter", h.DataCenter(), "dc1"},
		{"Rack", h.Rack(), "r1"},
		{"HostID", h.HostID(), hostID.String()},
		{"Version", h.Version().String(), "v3.11.4"},
		{"Peer", h.Peer().String(), "10.0.0.5"},
		{"ClusterName", h.ClusterName(), "test cluster"},
		{"Partitioner", h.Partitioner(), "org.apache.cassandra.dht.Murmur3Partitioner"},
		{"BroadcastAddress", h.BroadcastAddress().String(), "10.0.0.6"},
		{"PreferredIP", h.PreferredIP().String(), "10.0.0.7"},
		{"RPCAddress", h.RPCAddress().String(), "10.0.0.8"},
		{"ListenAddress", h.ListenAddress().String(), "10.0.0.9"},
		{"Tokens", strings.Join(h.Tokens(), ","), "1,2"},
		{"SchemaVersion", h.schemaVersion, schema.String()},
		// no native_port: the caller's default applies
		{"Port", h.Port(), 9042},
		// rpc_address is the driver-facing address when it is set
		{"ConnectAddress", h.ConnectAddress().String(), "10.0.0.8"},
	}
	for _, c := range checks {
		if c.got != c.want {
			t.Errorf("%s = %v, want %v", c.name, c.got, c.want)
		}
	}
}

// system.peers_v2 carries the address and port as native_address and
// native_port.
func TestHostInfoFromMapPeersV2(t *testing.T) {
	t.Parallel()

	h, err := hostInfoFromMap(map[string]any{
		"native_address": "10.1.0.1",
		"native_port":    19042,
	}, 9042)
	if err != nil {
		t.Fatal(err)
	}
	if h.RPCAddress().String() != "10.1.0.1" || h.Port() != 19042 || h.ConnectAddress().String() != "10.1.0.1" {
		t.Fatalf("host = %s:%d (rpc %s), want 10.1.0.1:19042", h.ConnectAddress(), h.Port(), h.RPCAddress())
	}
}

// A column of an unexpected Go type is an error naming the column, not a
// panic: the type comes from the server's schema for a system table.
func TestHostInfoFromMapWrongType(t *testing.T) {
	t.Parallel()

	for _, col := range []string{
		"data_center", "rack", "host_id", "release_version", "peer", "cluster_name", "partitioner",
		"broadcast_address", "preferred_ip", "rpc_address", "native_address", "listen_address",
		"native_port", "tokens", "schema_version",
	} {
		_, err := hostInfoFromMap(map[string]any{col: struct{}{}}, 9042)
		if err == nil || err.Error() != "Assertion failed for "+col {
			t.Errorf("%s: err = %v, want an assertion error for the column", col, err)
		}
	}
}

// update only fills in what the receiver does not know yet; it never
// overwrites a value it already has.
func TestHostInfoUpdateFillsOnlyMissing(t *testing.T) {
	t.Parallel()

	from := &HostInfo{
		dataCenter: "dc-new", rack: "r-new", hostId: MustRandomUUID(), port: 19042,
		rpcAddress: net.ParseIP("10.0.0.2"), tokens: []string{"9"}, partitioner: "p-new",
		clusterName: "c-new", workload: "w-new", dseVersion: "d-new",
	}
	from.version.Set("6.0.0")

	empty := &HostInfo{}
	empty.update(from)
	if empty.DataCenter() != "dc-new" || empty.Rack() != "r-new" || empty.HostID() != from.HostID() ||
		empty.Port() != 19042 || !empty.RPCAddress().Equal(from.rpcAddress) || empty.Version().String() != "v6.0.0" ||
		strings.Join(empty.Tokens(), ",") != "9" || empty.Partitioner() != "p-new" || empty.ClusterName() != "c-new" {
		t.Fatalf("update into an empty host = %s, want every field copied", empty)
	}

	known := &HostInfo{
		dataCenter: "dc-old", rack: "r-old", hostId: MustRandomUUID(), port: 9042,
		rpcAddress: net.ParseIP("10.0.0.1"), tokens: []string{"1"}, partitioner: "p-old",
	}
	known.version.Set("5.0.0")
	knownID := known.HostID()
	known.update(from)
	if known.DataCenter() != "dc-old" || known.Rack() != "r-old" || known.HostID() != knownID ||
		known.Port() != 9042 || known.RPCAddress().String() != "10.0.0.1" || known.Version().String() != "v5.0.0" ||
		strings.Join(known.Tokens(), ",") != "1" || known.Partitioner() != "p-old" {
		t.Fatalf("update over known values = %s, want them kept", known)
	}

	// updating from itself must not deadlock on its own lock
	known.update(known)
}

type testPeer struct {
	hostID UUID
	ip     net.IP
	dc     string
	rack   string
	tokens []string
}

var systemPeersCols = []rowsCol{
	{"peer", optNative(TypeInet)},
	{"data_center", optNative(TypeVarchar)},
	{"host_id", optNative(TypeUUID)},
	{"rack", optNative(TypeVarchar)},
	{"rpc_address", optNative(TypeInet)},
	{"tokens", optCollection(TypeSet, optNative(TypeVarchar))},
}

// optBytes encodes s as a value, or as null when it is empty.
func optBytes(s string) []byte {
	if s == "" {
		return nil
	}
	return []byte(s)
}

// ringTestServer is a controlTestServer whose system.peers table a test can
// change between ring refreshes.
type ringTestServer struct {
	*controlTestServer
	mu    sync.Mutex
	peers []testPeer
	// peersErr, when set, makes the system.peers query fail.
	peersErr string
}

func newRingTestServer(t *testing.T, peers ...testPeer) *ringTestServer {
	t.Helper()
	rs := &ringTestServer{peers: peers}
	rs.controlTestServer = newControlTestServer(t, func(srv *controlTestServer) {
		srv.queries = func(query string, stream int, resp *framer) bool {
			if !strings.Contains(query, "system.peers") {
				return false
			}
			rs.mu.Lock()
			peers := append([]testPeer(nil), rs.peers...)
			peersErr := rs.peersErr
			rs.mu.Unlock()
			if peersErr != "" {
				writeErrorResult(resp, stream, ErrCodeServer, peersErr)
				return true
			}
			var rows [][][]byte
			for _, p := range peers {
				id := p.hostID
				rows = append(rows, [][]byte{
					p.ip.To4(), optBytes(p.dc), id[:], optBytes(p.rack), p.ip.To4(), encodeTextSet(p.tokens...),
				})
			}
			writeRows(resp, stream, systemPeersCols, rows)
			return true
		}
	})
	return rs
}

func (rs *ringTestServer) setPeers(peers ...testPeer) {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	rs.peers = peers
}

func (rs *ringTestServer) failPeers(msg string) {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	rs.peersErr = msg
}

func (rs *ringTestServer) session(t *testing.T, configure func(cfg *ClusterConfig)) *Session {
	t.Helper()
	cfg := rs.cluster()
	if configure != nil {
		configure(cfg)
	}
	s, err := cfg.CreateSession()
	if err != nil {
		t.Fatalf("CreateSession: %v", err)
	}
	t.Cleanup(s.Close)
	return s
}

// ringHosts lists the session's known hosts as "hostID@ip" sorted, so tests
// can compare the whole ring at once.
func ringHosts(s *Session) []string {
	var out []string
	for _, h := range s.hostSource.getHostsList() {
		out = append(out, h.HostID()+"@"+h.ConnectAddress().String())
	}
	sort.Strings(out)
	return out
}

func peerKey(p testPeer) string { return p.hostID.String() + "@" + p.ip.String() }

func localKey() string { return testLocalHostID.String() + "@127.0.0.1" }

func sortedKeys(keys ...string) []string {
	sort.Strings(keys)
	return keys
}

func newTestPeer(ip string) testPeer {
	return testPeer{hostID: MustRandomUUID(), ip: net.ParseIP(ip), dc: "dc1", rack: "rack1", tokens: []string{"5"}}
}

func TestRefreshRing(t *testing.T) {
	t.Parallel()

	p2 := newTestPeer("127.0.0.2")
	p3 := newTestPeer("127.0.0.3")
	rs := newRingTestServer(t, p2)
	s := rs.session(t, nil)

	if got, want := ringHosts(s), sortedKeys(localKey(), peerKey(p2)); strings.Join(got, " ") != strings.Join(want, " ") {
		t.Fatalf("initial ring = %v, want %v", got, want)
	}

	// A new peer joins.
	rs.setPeers(p2, p3)
	if err := s.refreshRingNow(); err != nil {
		t.Fatal(err)
	}
	if got, want := ringHosts(s), sortedKeys(localKey(), peerKey(p2), peerKey(p3)); strings.Join(got, " ") != strings.Join(want, " ") {
		t.Fatalf("after join ring = %v, want %v", got, want)
	}
	if h := s.hostSource.getHost(p3.hostID.String()); h == nil || h.DataCenter() != "dc1" || h.Rack() != "rack1" {
		t.Fatalf("joined host = %v, want dc1/rack1", h)
	}

	// p2 keeps its host ID but moves to a new address.
	moved := p2
	moved.ip = net.ParseIP("127.0.0.4")
	rs.setPeers(moved, p3)
	if err := s.refreshRingNow(); err != nil {
		t.Fatal(err)
	}
	if got, want := ringHosts(s), sortedKeys(localKey(), peerKey(moved), peerKey(p3)); strings.Join(got, " ") != strings.Join(want, " ") {
		t.Fatalf("after address change ring = %v, want %v", got, want)
	}

	// p3 leaves.
	rs.setPeers(moved)
	if err := s.refreshRingNow(); err != nil {
		t.Fatal(err)
	}
	if got, want := ringHosts(s), sortedKeys(localKey(), peerKey(moved)); strings.Join(got, " ") != strings.Join(want, " ") {
		t.Fatalf("after leave ring = %v, want %v", got, want)
	}
}

// Peers the driver cannot or should not use never enter the ring.
func TestRefreshRingSkipsPeers(t *testing.T) {
	t.Parallel()

	good := newTestPeer("127.0.0.2")
	noRack := newTestPeer("127.0.0.3")
	noRack.rack = ""
	zeroToken := newTestPeer("127.0.0.4")
	zeroToken.tokens = nil
	otherDC := newTestPeer("127.0.0.5")
	otherDC.dc = "dc2"

	rs := newRingTestServer(t, good, noRack, zeroToken, otherDC)
	s := rs.session(t, func(cfg *ClusterConfig) { cfg.HostFilter = DataCenterHostFilter("dc1") })

	if got, want := ringHosts(s), sortedKeys(localKey(), peerKey(good)); strings.Join(got, " ") != strings.Join(want, " ") {
		t.Fatalf("ring = %v, want only the local host and the valid dc1 peer %v", got, want)
	}
	if err := s.refreshRingNow(); err != nil {
		t.Fatal(err)
	}
	if got, want := ringHosts(s), sortedKeys(localKey(), peerKey(good)); strings.Join(got, " ") != strings.Join(want, " ") {
		t.Fatalf("after refresh ring = %v, want %v", got, want)
	}
}

// A failed lookup reports the error and leaves the ring as it was rather
// than treating every peer as gone.
func TestRefreshRingKeepsRingOnError(t *testing.T) {
	t.Parallel()

	p2 := newTestPeer("127.0.0.2")
	rs := newRingTestServer(t, p2)
	s := rs.session(t, nil)
	before := ringHosts(s)

	rs.failPeers("peers unavailable")
	err := s.refreshRingNow()
	if err == nil || !strings.Contains(err.Error(), "peers unavailable") {
		t.Fatalf("refreshRingNow error = %v, want the peers error", err)
	}
	if got := ringHosts(s); strings.Join(got, " ") != strings.Join(before, " ") {
		t.Fatalf("ring after a failed refresh = %v, want %v", got, before)
	}
}
