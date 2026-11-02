//go:build unit
// +build unit

/*
 * Copyright (C) 2026 ScyllaDB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package gocql

import (
	"context"
	"encoding/binary"
	"fmt"
	"net"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	frm "github.com/gocql/gocql/internal/frame"
)

// The tests in this file pin that the driver does not depend on the names a
// server reports for bind markers. ScyllaDB synthesizes names for anonymous ?
// markers (for example "IN(id)" or "in(id)"), and the spelling can change
// between versions, so it can differ between nodes during a rolling upgrade
// and change on a node after it restarts.

// bindMarker is one bind variable as a server reports it in prepared metadata.
type bindMarker struct {
	name string
	typ  TypeInfo
}

// recordedRequest is one statement of an EXECUTE or BATCH as the server saw it.
type recordedRequest struct {
	id         uint64
	names      []string
	values     [][]byte
	unprepared bool
}

// bindMarkerServer answers PREPARE, EXECUTE and BATCH for the statements
// defined on it, the way a node would: it reports bind markers under the names
// it was given, and answers UNPREPARED for an id it does not know.
type bindMarkerServer struct {
	mu       sync.Mutex
	markers  map[string][]bindMarker
	stmtID   map[string]uint64
	known    map[uint64]bool
	nextID   uint64
	requests []recordedRequest
}

func newBindMarkerServer(srv *TestServer, idBase uint64) *bindMarkerServer {
	b := &bindMarkerServer{
		markers: make(map[string][]bindMarker),
		stmtID:  make(map[string]uint64),
		known:   make(map[uint64]bool),
		nextID:  idBase,
	}
	srv.setPrepareHook(b.prepare)
	srv.setExecuteHook(b.execute)
	srv.setBatchHook(b.batch)
	return b
}

// define sets the bind markers the server reports for stmt. Redefining a
// statement models a schema change or a server upgrade: the old prepared id is
// forgotten, so the next EXECUTE of it is answered with UNPREPARED, and
// preparing it again yields a new id with the new metadata.
func (b *bindMarkerServer) define(stmt string, markers ...bindMarker) uint64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	if old, ok := b.stmtID[stmt]; ok {
		delete(b.known, old)
	}
	b.nextID++
	b.markers[stmt] = markers
	b.stmtID[stmt] = b.nextID
	b.known[b.nextID] = true
	return b.nextID
}

func (b *bindMarkerServer) recorded() []recordedRequest {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]recordedRequest(nil), b.requests...)
}

func (b *bindMarkerServer) prepare(stmt string, stream int, resp *framer, writeResultMetadataID func(uint64)) bool {
	b.mu.Lock()
	defer b.mu.Unlock()

	markers, ok := b.markers[stmt]
	if !ok {
		return false
	}
	resp.writeHeader(0, frm.OpResult, stream)
	resp.writeInt(frm.ResultKindPrepared)
	resp.writeShortBytes(binary.BigEndian.AppendUint64(nil, b.stmtID[stmt]))
	writeResultMetadataID(b.stmtID[stmt])
	// <metadata>
	resp.writeInt(0) // <flags>
	resp.writeInt(int32(len(markers)))
	resp.writeInt(0) // <pk_count>
	for _, m := range markers {
		resp.writeString("ks")
		resp.writeString("t")
		resp.writeString(m.name)
		writeTestTypeInfo(resp, m.typ)
	}
	// <result_metadata>
	resp.writeInt(int32(frm.FlagNoMetaData))
	resp.writeInt(0)
	return true
}

func (b *bindMarkerServer) execute(id uint64, req *framer, stream int, resp *framer) bool {
	b.mu.Lock()
	defer b.mu.Unlock()

	r := recordedRequest{id: id}
	req.readConsistency()
	flags := uint32(req.readByte())
	if flags&frm.FlagValues != 0 {
		n := int(req.readShort())
		for i := 0; i < n; i++ {
			if flags&frm.FlagWithNameValues != 0 {
				r.names = append(r.names, req.readString())
			}
			r.values = append(r.values, req.readBytesCopy())
		}
	}
	r.unprepared = !b.known[r.id]
	b.requests = append(b.requests, r)
	if r.unprepared {
		writeTestUnprepared(stream, resp, r.id)
	} else {
		resp.writeHeader(0, frm.OpResult, stream)
		resp.writeInt(frm.ResultKindVoid)
	}
	return true
}

func (b *bindMarkerServer) batch(req *framer, stream int, resp *framer) {
	b.mu.Lock()
	defer b.mu.Unlock()

	req.readByte() // <type>
	n := int(req.readShort())
	var unknown *recordedRequest
	for i := 0; i < n; i++ {
		if kind := req.readByte(); kind != 1 {
			// Answer with an error rather than calling t.Errorf: this runs
			// on a server goroutine that can outlive the test.
			writeErrorResult(resp, stream, 0, fmt.Sprintf("bind marker test server: batch statement %d is not prepared (kind %d)", i, kind))
			return
		}
		r := recordedRequest{id: binary.BigEndian.Uint64(req.readShortBytesCopy())}
		for j, values := 0, int(req.readShort()); j < values; j++ {
			r.values = append(r.values, req.readBytesCopy())
		}
		r.unprepared = !b.known[r.id]
		b.requests = append(b.requests, r)
		if r.unprepared && unknown == nil {
			unknown = &r
		}
	}
	if unknown != nil {
		writeTestUnprepared(stream, resp, unknown.id)
	} else {
		resp.writeHeader(0, frm.OpResult, stream)
		resp.writeInt(frm.ResultKindVoid)
	}
}

func writeTestUnprepared(stream int, resp *framer, id uint64) {
	resp.writeHeader(0, frm.OpError, stream)
	resp.writeInt(ErrCodeUnprepared)
	resp.writeString("unprepared")
	resp.writeShortBytes(binary.BigEndian.AppendUint64(nil, id))
}

func writeTestTypeInfo(f *framer, typ TypeInfo) {
	f.writeShort(uint16(typ.Type()))
	if c, ok := typ.(CollectionType); ok {
		writeTestTypeInfo(f, c.Elem)
	}
}

func testListType(elem Type) TypeInfo {
	return CollectionType{
		NativeType: NewNativeType(protoVersion4, TypeList),
		Elem:       NewNativeType(protoVersion4, elem),
	}
}

func unmarshalTestValue(t *testing.T, typ TypeInfo, data []byte, dst any) {
	t.Helper()
	require.NoError(t, Unmarshal(typ, data, dst))
}

func newBindMarkerSession(t *testing.T, srvs ...*TestServer) *Session {
	t.Helper()
	addrs := make([]string, len(srvs))
	for i, srv := range srvs {
		addrs[i] = srv.Address
	}
	session, err := newTestSession(protoVersion4, addrs...)
	require.NoError(t, err)
	t.Cleanup(session.Close)
	return session
}

// newBindMarkerTestServer starts a node on addr; "127.0.0.1:0" picks a free port.
func newBindMarkerTestServer(t *testing.T, addr string, idBase uint64) (*TestServer, *bindMarkerServer) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	srv := NewTestServerWithAddress(addr, t, protoVersion4, ctx)
	t.Cleanup(srv.Stop)
	return srv, newBindMarkerServer(srv, idBase)
}

const inStmt = "SELECT v FROM ks.t WHERE id IN ? AND c = ?"

// TestPositionalBindingIgnoresSynthesizedNames pins that binding anonymous ?
// markers positionally, directly or through Session.Bind by index, works the
// same whatever name the server synthesizes for an IN marker, and sends no
// names to the coordinator.
func TestPositionalBindingIgnoresSynthesizedNames(t *testing.T) {
	t.Parallel()

	for _, spelling := range []string{"IN(id)", "in(id)"} {
		t.Run(spelling, func(t *testing.T) {
			t.Parallel()

			srv, server := newBindMarkerTestServer(t, "127.0.0.1:0", 100)
			server.define(inStmt,
				bindMarker{name: spelling, typ: testListType(TypeInt)},
				bindMarker{name: "c", typ: NewNativeType(protoVersion4, TypeVarchar)},
			)
			session := newBindMarkerSession(t, srv)

			require.NoError(t, session.Query(inStmt, []int{1, 2}, "x").Exec())

			var argNames []string
			require.NoError(t, session.Bind(inStmt, func(q *QueryInfo) ([]any, error) {
				for _, arg := range q.Args {
					argNames = append(argNames, arg.Name)
				}
				return []any{[]int{1, 2}, "x"}, nil
			}).Exec())
			// The name is whatever the server chose; callbacks must bind by index.
			require.Equal(t, []string{spelling, "c"}, argNames)

			requests := server.recorded()
			require.Len(t, requests, 2)
			for _, r := range requests {
				require.False(t, r.unprepared)
				require.Nil(t, r.names, "positional values must not carry names")
				require.Len(t, r.values, 2)

				var ids []int
				unmarshalTestValue(t, testListType(TypeInt), r.values[0], &ids)
				require.Equal(t, []int{1, 2}, ids)
				var c string
				unmarshalTestValue(t, NewNativeType(protoVersion4, TypeVarchar), r.values[1], &c)
				require.Equal(t, "x", c)
			}
		})
	}
}

// TestNamedBindingExplicitMarkers pins that NamedValues bind to explicit :name
// markers by name, including an IN marker, in any argument order, and that
// the names are sent to the coordinator in marker order.
func TestNamedBindingExplicitMarkers(t *testing.T) {
	t.Parallel()

	const stmt = "SELECT v FROM ks.t WHERE id IN :ids AND c = :c"

	srv, server := newBindMarkerTestServer(t, "127.0.0.1:0", 100)
	server.define(stmt,
		bindMarker{name: "ids", typ: testListType(TypeInt)},
		bindMarker{name: "c", typ: NewNativeType(protoVersion4, TypeVarchar)},
	)
	session := newBindMarkerSession(t, srv)

	require.NoError(t, session.Query(stmt, NamedValue("ids", []int{1, 2}), NamedValue("c", "x")).Exec())
	require.NoError(t, session.Query(stmt, NamedValue("c", "x"), NamedValue("ids", []int{1, 2})).Exec())

	requests := server.recorded()
	require.Len(t, requests, 2)
	for _, r := range requests {
		require.False(t, r.unprepared)
		require.Equal(t, []string{"ids", "c"}, r.names)

		var ids []int
		unmarshalTestValue(t, testListType(TypeInt), r.values[0], &ids)
		require.Equal(t, []int{1, 2}, ids)
		var c string
		unmarshalTestValue(t, NewNativeType(protoVersion4, TypeVarchar), r.values[1], &c)
		require.Equal(t, "x", c)
	}

	t.Run("rejected before reaching the server", func(t *testing.T) {
		for _, tc := range []struct {
			name   string
			values []any
			err    string
		}{
			{"mixed", []any{NamedValue("ids", []int{1}), "x"}, "cannot mix named and positional"},
			{"unknown name", []any{NamedValue("ids", []int{1}), NamedValue("d", "x")}, `no bind marker named "d"`},
		} {
			err := session.Query(stmt, tc.values...).Exec()
			require.ErrorContains(t, err, tc.err, tc.name)
		}
		require.Len(t, server.recorded(), 2, "no EXECUTE may be sent for rejected values")
	})
}

// TestNamedBindingRepeatedMarker pins that one NamedValue binds every
// occurrence of a repeated :name marker when the server reports one marker per
// occurrence, as Cassandra does. ScyllaDB reports it once.
func TestNamedBindingRepeatedMarker(t *testing.T) {
	t.Parallel()

	const stmt = "SELECT v FROM ks.t WHERE pk = :v AND ck = :v"

	srv, server := newBindMarkerTestServer(t, "127.0.0.1:0", 100)
	server.define(stmt,
		bindMarker{name: "v", typ: NewNativeType(protoVersion4, TypeInt)},
		bindMarker{name: "v", typ: NewNativeType(protoVersion4, TypeInt)},
	)
	session := newBindMarkerSession(t, srv)

	require.NoError(t, session.Query(stmt, NamedValue("v", 2)).Exec())

	requests := server.recorded()
	require.Len(t, requests, 1)
	require.Equal(t, []string{"v", "v"}, requests[0].names)
	for _, value := range requests[0].values {
		var v int
		unmarshalTestValue(t, NewNativeType(protoVersion4, TypeInt), value, &v)
		require.Equal(t, 2, v)
	}
}

// TestBatchRejectsNamedValues pins that named values in a batch are rejected
// with the batch error, before they are marshaled by position: here the
// first value would otherwise be marshaled as the list<int> marker.
func TestBatchRejectsNamedValues(t *testing.T) {
	t.Parallel()

	srv, server := newBindMarkerTestServer(t, "127.0.0.1:0", 100)
	server.define(inStmt,
		bindMarker{name: "ids", typ: testListType(TypeInt)},
		bindMarker{name: "c", typ: NewNativeType(protoVersion4, TypeVarchar)},
	)
	session := newBindMarkerSession(t, srv)

	batch := session.Batch(UnloggedBatch)
	batch.Query(inStmt, NamedValue("c", "x"), NamedValue("ids", []int{1}))
	require.ErrorIs(t, batch.Exec(), errNamedValuesInBatch)
	require.Empty(t, server.recorded(), "no BATCH may be sent")
}

// TestReprepareRefreshesBindMetadata pins that when a node answers UNPREPARED
// (its cache was evicted, or it was upgraded and the statement's metadata
// changed), the driver prepares the statement again and binds the values
// against the new metadata: the new prepared id, the new marker names handed
// to binding callbacks, and the new marker types used to marshal values.
func TestReprepareRefreshesBindMetadata(t *testing.T) {
	t.Parallel()

	const stmt = "SELECT v FROM ks.t WHERE id IN ?"

	before := bindMarker{name: "IN(id)", typ: testListType(TypeInt)}
	after := bindMarker{name: "in(id)", typ: testListType(TypeBigInt)}

	tests := []struct {
		name string
		exec func(t *testing.T, session *Session, infos *[]QueryInfo) error
	}{
		{
			name: "query",
			exec: func(t *testing.T, session *Session, _ *[]QueryInfo) error {
				return session.Query(stmt, []int{1, 2}).Exec()
			},
		},
		{
			name: "session bind",
			exec: func(t *testing.T, session *Session, infos *[]QueryInfo) error {
				return session.Bind(stmt, func(q *QueryInfo) ([]any, error) {
					*infos = append(*infos, *q)
					return []any{[]int{1, 2}}, nil
				}).Exec()
			},
		},
		{
			name: "batch",
			exec: func(t *testing.T, session *Session, _ *[]QueryInfo) error {
				batch := session.Batch(UnloggedBatch)
				batch.Query(stmt, []int{1, 2})
				return batch.Exec()
			},
		},
		{
			name: "batch bind",
			exec: func(t *testing.T, session *Session, infos *[]QueryInfo) error {
				batch := session.Batch(UnloggedBatch)
				batch.Bind(stmt, func(q *QueryInfo) ([]any, error) {
					*infos = append(*infos, *q)
					return []any{[]int{1, 2}}, nil
				})
				return batch.Exec()
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			srv, server := newBindMarkerTestServer(t, "127.0.0.1:0", 100)
			oldID := server.define(stmt, before)
			session := newBindMarkerSession(t, srv)

			var infos []QueryInfo
			require.NoError(t, tc.exec(t, session, &infos))
			newID := server.define(stmt, after)
			require.NoError(t, tc.exec(t, session, &infos))

			requests := server.recorded()
			require.Len(t, requests, 3)
			require.Equal(t, oldID, requests[0].id)
			require.False(t, requests[0].unprepared)
			require.Equal(t, oldID, requests[1].id)
			require.True(t, requests[1].unprepared)
			require.Equal(t, newID, requests[2].id)
			require.False(t, requests[2].unprepared)

			// The retried request is marshaled with the new marker type.
			var ids []int64
			unmarshalTestValue(t, after.typ, requests[2].values[0], &ids)
			require.Equal(t, []int64{1, 2}, ids)

			if infos != nil {
				last := infos[len(infos)-1]
				require.Equal(t, binary.BigEndian.AppendUint64(nil, newID), last.Id)
				require.Equal(t, after.name, last.Args[0].Name)
			}
		})
	}
}

// TestMixedNodeSynthesizedNames models a rolling upgrade: two nodes report
// different names for the same anonymous IN marker. Positional binding works
// against both, while a binding callback sees whichever name the coordinator
// it is prepared on reports, which is why callbacks must bind by index.
func TestMixedNodeSynthesizedNames(t *testing.T) {
	t.Parallel()

	// The driver requires every node on the same port.
	srvA, serverA := newBindMarkerTestServer(t, "127.0.0.1:0", 1000)
	srvB, serverB := newBindMarkerTestServer(t, fmt.Sprintf("127.0.0.2:%d", srvA.port()), 2000)
	for _, s := range []struct {
		server   *bindMarkerServer
		spelling string
	}{{serverA, "IN(id)"}, {serverB, "in(id)"}} {
		s.server.define(inStmt,
			bindMarker{name: s.spelling, typ: testListType(TypeInt)},
			bindMarker{name: "c", typ: NewNativeType(protoVersion4, TypeVarchar)},
		)
	}
	session := newBindMarkerSession(t, srvA, srvB)

	seen := make(map[string]bool)
	var mu sync.Mutex
	// Separate loops: the round-robin policy alternates nodes on every request,
	// so interleaving the two kinds would pin each kind to one node.
	for i := 0; i < 10; i++ {
		require.NoError(t, session.Query(inStmt, []int{1, 2}, "x").Exec())
	}
	for i := 0; i < 10; i++ {
		require.NoError(t, session.Bind(inStmt, func(q *QueryInfo) ([]any, error) {
			mu.Lock()
			seen[q.Args[0].Name] = true
			mu.Unlock()
			return []any{[]int{1, 2}, "x"}, nil
		}).Exec())
	}

	require.Equal(t, map[string]bool{"IN(id)": true, "in(id)": true}, seen)
	for i, server := range []*bindMarkerServer{serverA, serverB} {
		requests := server.recorded()
		require.NotEmpty(t, requests, "node %d received no EXECUTE", i)
		for _, r := range requests {
			require.Nil(t, r.names, fmt.Sprintf("node %d: positional values must not carry names", i))
			if r.unprepared {
				continue
			}
			var ids []int
			unmarshalTestValue(t, testListType(TypeInt), r.values[0], &ids)
			require.Equal(t, []int{1, 2}, ids)
		}
	}
}

// TestBindMarkerServerWithMetadataID pins that the EXECUTE hook sees the query
// parameters when SCYLLA_USE_METADATA_ID is negotiated and the driver sends a
// result metadata id before them.
func TestBindMarkerServerWithMetadataID(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	supported := func(net.Conn) map[string][]string { return map[string][]string{scyllaUseMetadataID: {}} }
	srv := NewTestServerWithAddressAndSupportedFactory("127.0.0.1:0", t, protoVersion4, ctx, supported)
	t.Cleanup(srv.Stop)
	server := newBindMarkerServer(srv, 100)
	server.define(inStmt,
		bindMarker{name: "IN(id)", typ: testListType(TypeInt)},
		bindMarker{name: "c", typ: NewNativeType(protoVersion4, TypeVarchar)},
	)
	session := newBindMarkerSession(t, srv)

	require.NoError(t, session.Query(inStmt, []int{1, 2}, "x").Exec())

	requests := server.recorded()
	require.Len(t, requests, 1)
	require.Len(t, requests[0].values, 2)
	var c string
	unmarshalTestValue(t, NewNativeType(protoVersion4, TypeVarchar), requests[0].values[1], &c)
	require.Equal(t, "x", c)
}
