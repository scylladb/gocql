//go:build unit
// +build unit

package gocql

import (
	"context"
	"encoding/binary"
	"errors"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	frm "github.com/gocql/gocql/internal/frame"
)

type rowsCol struct {
	name string
	typ  typeOption
}

// writeRows writes a RESULT/Rows reply whose columns may have any type option.
func writeRows(resp *framer, stream int, cols []rowsCol, rows [][][]byte) {
	resp.writeHeader(0, frm.OpResult, stream)
	resp.writeInt(frm.ResultKindRows)
	resp.writeInt(int32(frm.FlagGlobalTableSpec))
	resp.writeInt(int32(len(cols)))
	resp.writeString("system")
	resp.writeString("local")
	for _, c := range cols {
		resp.writeString(c.name)
		c.typ(resp)
	}
	resp.writeInt(int32(len(rows)))
	for _, row := range rows {
		for _, v := range row {
			resp.writeBytes(v)
		}
	}
}

// encodeTextSet encodes a set<text> value.
func encodeTextSet(items ...string) []byte {
	out := binary.BigEndian.AppendUint32(nil, uint32(len(items)))
	for _, it := range items {
		out = binary.BigEndian.AppendUint32(out, uint32(len(it)))
		out = append(out, it...)
	}
	return out
}

var systemLocalCols = []rowsCol{
	{"host_id", optNative(TypeUUID)},
	{"data_center", optNative(TypeVarchar)},
	{"rack", optNative(TypeVarchar)},
	{"rpc_address", optNative(TypeInet)},
	{"release_version", optNative(TypeVarchar)},
	{"partitioner", optNative(TypeVarchar)},
	{"tokens", optCollection(TypeSet, optNative(TypeVarchar))},
}

var testLocalHostID = func() UUID {
	u, err := ParseUUID("8f3e2b1a-4c5d-4e6f-9a0b-1c2d3e4f5a6b")
	if err != nil {
		panic(err)
	}
	return u
}()

func systemLocalRow(rpc net.IP) [][]byte {
	return [][]byte{
		testLocalHostID[:],
		[]byte("dc1"),
		[]byte("rack1"),
		rpc.To4(),
		[]byte("3.0.8"),
		[]byte("org.apache.cassandra.dht.Murmur3Partitioner"),
		encodeTextSet("-100", "100"),
	}
}

// controlTestServer is a TestServer that can stand in for a control connection
// peer: it answers system.local with one row, accepts REGISTER, and counts both.
type controlTestServer struct {
	*TestServer
	mu        sync.Mutex
	locals    int
	registers [][]string

	// queries, when set, is offered every query first and returns true if it
	// answered it.
	queries func(query string, stream int, resp *framer) bool
	// localRows, when set, replaces the system.local reply.
	localRows func(stream int, resp *framer)
	// registerReply, when set, replaces the READY reply to REGISTER.
	registerReply func(stream int, resp *framer)
}

func newControlTestServer(t *testing.T, configure func(srv *controlTestServer)) *controlTestServer {
	t.Helper()
	srv := &controlTestServer{TestServer: NewTestServer(t, protoVersion4, context.Background())}
	t.Cleanup(srv.Stop)
	if configure != nil {
		configure(srv)
	}
	srv.setQueryHook(func(query string, stream int, resp *framer) bool {
		if srv.queries != nil && srv.queries(query, stream, resp) {
			return true
		}
		if !strings.Contains(query, "system.local") {
			return false
		}
		srv.mu.Lock()
		srv.locals++
		localRows := srv.localRows
		srv.mu.Unlock()
		if localRows != nil {
			localRows(stream, resp)
			return true
		}
		writeRows(resp, stream, systemLocalCols, [][][]byte{systemLocalRow(net.IPv4(127, 0, 0, 1))})
		return true
	})
	srv.setRegisterHook(func(events []string, stream int, resp *framer) {
		srv.mu.Lock()
		srv.registers = append(srv.registers, events)
		srv.mu.Unlock()
		if srv.registerReply != nil {
			srv.registerReply(stream, resp)
			return
		}
		resp.writeHeader(0, frm.OpReady, stream)
	})
	return srv
}

func (srv *controlTestServer) registered() [][]string {
	srv.mu.Lock()
	defer srv.mu.Unlock()
	return append([][]string(nil), srv.registers...)
}

func (srv *controlTestServer) cluster() *ClusterConfig {
	cfg := NewCluster(srv.Address)
	cfg.ProtoVersion = protoVersion4
	cfg.Logger = nopLogger{}
	return cfg
}

func TestSingleHostQueryExecutorRunsQueries(t *testing.T) {
	t.Parallel()

	srv := newControlTestServer(t, nil)
	cfg := srv.cluster()
	cfg.ProtoVersion = 0 // the executor assumes v4 rather than negotiating

	e, err := NewSingleHostQueryExecutor(cfg)
	if err != nil {
		t.Fatalf("NewSingleHostQueryExecutor: %v", err)
	}
	defer e.Close()

	if e.session.cfg.ProtoVersion != protoVersion4 {
		t.Fatalf("ProtoVersion = %d, want %d when unset", e.session.cfg.ProtoVersion, protoVersion4)
	}
	if cfg.ProtoVersion != 0 {
		t.Fatal("NewSingleHostQueryExecutor modified the caller's config")
	}

	if err := e.Exec("void"); err != nil {
		t.Fatalf("Exec: %v", err)
	}
	// SELECT would be prepared first; a non-DML statement goes out as a plain
	// QUERY, which the test server answers with the system.local row.
	iter := e.Iter("DESCRIBE TABLE system.local")
	row := map[string]any{}
	if !iter.MapScan(row) {
		t.Fatalf("Iter returned no row: %v", iter.Close())
	}
	if err := iter.Close(); err != nil {
		t.Fatalf("Iter close: %v", err)
	}
	if row["data_center"] != "dc1" || row["host_id"] != testLocalHostID {
		t.Fatalf("row = %v, want data_center=dc1 host_id=%s", row, testLocalHostID)
	}

	host := e.control.getConn().host
	if host.HostID() != testLocalHostID.String() || host.DataCenter() != "dc1" || host.Rack() != "rack1" {
		t.Fatalf("control host = %s %s/%s, want %s dc1/rack1", host.HostID(), host.DataCenter(), host.Rack(), testLocalHostID)
	}
}

// The control connection retries a failed query through its retry policy:
// SimpleRetryPolicy{NumRetries: 3}, so four attempts, and then reports the
// error.
func TestSingleHostQueryExecutorRetriesFailedQuery(t *testing.T) {
	t.Parallel()

	srv := newControlTestServer(t, nil)
	e, err := NewSingleHostQueryExecutor(srv.cluster())
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()

	err = e.Exec("kill")
	if err == nil || !strings.Contains(err.Error(), "query killed") {
		t.Fatalf("Exec error = %v, want the server's error", err)
	}
	if n := atomic.LoadInt64(&srv.nKillReq); n != 4 {
		t.Fatalf("server saw %d attempts, want 4", n)
	}
}

// Hosts are tried in turn until one sets up; an unreachable one is skipped.
func TestSingleHostQueryExecutorSkipsUnreachableHost(t *testing.T) {
	t.Parallel()

	srv := newControlTestServer(t, nil)

	// Hosts must share a port, so the unreachable one is another loopback
	// address on the server's port, where nothing listens.
	_, port, err := net.SplitHostPort(srv.Address)
	if err != nil {
		t.Fatal(err)
	}
	cfg := srv.cluster()
	cfg.Hosts = []string{net.JoinHostPort("127.0.0.2", port), srv.Address}
	cfg.ConnectTimeout = 500 * time.Millisecond
	e, err := NewSingleHostQueryExecutor(cfg)
	if err != nil {
		t.Fatalf("NewSingleHostQueryExecutor: %v", err)
	}
	defer e.Close()
	if err := e.Exec("void"); err != nil {
		t.Fatal(err)
	}
}

func TestSingleHostQueryExecutorFailures(t *testing.T) {
	t.Parallel()

	t.Run("NoHosts", func(t *testing.T) {
		t.Parallel()
		cfg := NewCluster()
		if _, err := NewSingleHostQueryExecutor(cfg); !errors.Is(err, ErrNoHosts) {
			t.Fatalf("err = %v, want ErrNoHosts", err)
		}
	})

	t.Run("InvalidConfig", func(t *testing.T) {
		t.Parallel()
		cfg := NewCluster("127.0.0.1")
		cfg.NumConns = -1
		_, err := NewSingleHostQueryExecutor(cfg)
		if err == nil || !strings.Contains(err.Error(), "new session") {
			t.Fatalf("err = %v, want a new session error", err)
		}
	})

	t.Run("Unreachable", func(t *testing.T) {
		t.Parallel()
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		addr := l.Addr().String()
		l.Close()
		cfg := NewCluster(addr)
		cfg.Logger = nopLogger{}
		_, err = NewSingleHostQueryExecutor(cfg)
		if err == nil || !strings.Contains(err.Error(), "setup") {
			t.Fatalf("err = %v, want a setup error", err)
		}
	})
}

// setupConn is the step between dialing a control connection and using it:
// it reads the host from system.local, applies the host filter and registers
// for events. Each failure must reject the connection.
func TestControlSetupConnFailures(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		configure func(srv *controlTestServer)
		cluster   func(cfg *ClusterConfig)
		wantErr   string
	}{
		{
			name: "NoLocalRow",
			configure: func(srv *controlTestServer) {
				srv.localRows = func(stream int, resp *framer) { writeRows(resp, stream, systemLocalCols, nil) }
			},
			wantErr: "query returned 0 rows",
		},
		{
			name: "LocalQueryFails",
			configure: func(srv *controlTestServer) {
				srv.localRows = func(stream int, resp *framer) {
					writeErrorResult(resp, stream, ErrCodeServer, "system.local unavailable")
				}
			},
			wantErr: "system.local unavailable",
		},
		{
			name:    "HostFiltered",
			cluster: func(cfg *ClusterConfig) { cfg.HostFilter = DataCenterHostFilter("dc2") },
			wantErr: "host was filtered",
		},
		{
			name: "RegisterRejected",
			configure: func(srv *controlTestServer) {
				srv.registerReply = func(stream int, resp *framer) {
					writeErrorResult(resp, stream, ErrCodeProtocol, "no events for you")
				}
			},
			// The server's error arrives as an unexpected frame rather than as
			// the error itself, but its message is kept.
			wantErr: "no events for you",
		},
		{
			name: "RegisterUnexpectedReply",
			configure: func(srv *controlTestServer) {
				srv.registerReply = func(stream int, resp *framer) { writeVoidResult(resp, stream) }
			},
			wantErr: "unexpected frame in response to register",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			srv := newControlTestServer(t, tc.configure)
			cfg := srv.cluster()
			if tc.cluster != nil {
				tc.cluster(cfg)
			}
			_, err := NewSingleHostQueryExecutor(cfg)
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("err = %v, want it to contain %q", err, tc.wantErr)
			}
		})
	}
}

// The control connection registers for exactly the events the config enables,
// and skips REGISTER altogether when none are.
func TestControlRegisterEvents(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		cluster func(cfg *ClusterConfig)
		want    []string
	}{
		{"Default", nil, []string{"TOPOLOGY_CHANGE", "STATUS_CHANGE", "SCHEMA_CHANGE"}},
		{"NoTopology", func(c *ClusterConfig) { c.Events.DisableTopologyEvents = true },
			[]string{"STATUS_CHANGE", "SCHEMA_CHANGE"}},
		{"OnlySchema", func(c *ClusterConfig) {
			c.Events.DisableTopologyEvents = true
			c.Events.DisableNodeStatusEvents = true
		}, []string{"SCHEMA_CHANGE"}},
		{"None", func(c *ClusterConfig) {
			c.Events.DisableTopologyEvents = true
			c.Events.DisableNodeStatusEvents = true
			c.Events.DisableSchemaEvents = true
		}, nil},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			srv := newControlTestServer(t, nil)
			cfg := srv.cluster()
			if tc.cluster != nil {
				tc.cluster(cfg)
			}
			e, err := NewSingleHostQueryExecutor(cfg)
			if err != nil {
				t.Fatal(err)
			}
			defer e.Close()

			got := srv.registered()
			if tc.want == nil {
				if len(got) != 0 {
					t.Fatalf("REGISTER sent for %v with every event disabled", got)
				}
				return
			}
			if len(got) != 1 || strings.Join(got[0], ",") != strings.Join(tc.want, ",") {
				t.Fatalf("registered %v, want [%v]", got, tc.want)
			}
		})
	}
}
