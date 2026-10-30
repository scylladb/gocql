//go:build unit
// +build unit

package gocql

import (
	"context"
	"encoding/binary"

	"net"
	"strings"
	"sync"

	"testing"

	frm "github.com/scylladb/gocql/v2/internal/frame"
)

type rowsCol struct {
	name string
	typ  typeOption
}

// writeRows writes a RESULT/Rows reply whose columns may have any type option.
func writeRows(resp *framer, stream int, cols []rowsCol, rows [][][]byte) {
	resp.WriteHeader(0, frm.OpResult, stream)
	resp.WriteInt(frm.ResultKindRows)
	resp.WriteInt(int32(frm.FlagGlobalTableSpec))
	resp.WriteInt(int32(len(cols)))
	resp.WriteString("system")
	resp.WriteString("local")
	for _, c := range cols {
		resp.WriteString(c.name)
		c.typ(resp)
	}
	resp.WriteInt(int32(len(rows)))
	for _, row := range rows {
		for _, v := range row {
			resp.WriteBytes(v)
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
		resp.WriteHeader(0, frm.OpReady, stream)
	})
	return srv
}

func (srv *controlTestServer) registered() [][]string {
	srv.mu.Lock()
	defer srv.mu.Unlock()
	return append([][]string(nil), srv.registers...)
}

func (srv *controlTestServer) cluster() *clusterSettings {
	cfg := newClusterSettings(srv.Address)
	cfg.ProtoVersion = protoVersion4
	cfg.Logger = nopLogger{}
	return cfg
}
