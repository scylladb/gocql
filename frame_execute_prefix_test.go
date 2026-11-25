//go:build unit
// +build unit

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/*
 * Content before git sha 34fdeebefcbf183ed7f916f931aa0586fdaa1b40
 * Copyright (c) 2012, The Gocql authors,
 * provided under the BSD-3-Clause License.
 * See the NOTICE file distributed with this work for additional information.
 */

package gocql

import (
	"bytes"
	"context"
	"testing"

	frm "github.com/gocql/gocql/internal/frame"
)

// The prefix fast path must emit exactly the bytes of the from-scratch path.
func TestExecPrefixMatchesSlowPath(t *testing.T) {
	id, mid := []byte{1, 2, 3, 4}, []byte{9, 8}
	params := queryParams{consistency: LocalQuorum, values: []queryValues{{value: []byte("v")}}, pageSize: 10}
	for _, tc := range []struct {
		customPayload map[string][]byte
		name          string
		proto         byte
		prefixProto   byte
		scyllaMeta    bool
		prefixMeta    bool
		trace         bool
		wantFast      bool
	}{
		{name: "v4", proto: protoVersion4, prefixProto: protoVersion4, wantFast: true},
		{name: "v4 scylla metadata id", proto: protoVersion4, prefixProto: protoVersion4, scyllaMeta: true, prefixMeta: true, wantFast: true},
		{name: "v4 tracing", proto: protoVersion4, prefixProto: protoVersion4, trace: true, wantFast: true},
		{name: "v5", proto: protoVersion5, prefixProto: protoVersion5, prefixMeta: true, wantFast: true},
		{name: "v4 custom payload falls back", proto: protoVersion4, prefixProto: protoVersion4, customPayload: map[string][]byte{"k": {1}}},
		{name: "prefix built without metadata id falls back", proto: protoVersion4, prefixProto: protoVersion4, scyllaMeta: true},
		{name: "prefix built with metadata id falls back", proto: protoVersion4, prefixProto: protoVersion4, prefixMeta: true},
		{name: "prefix version mismatch falls back", proto: protoVersion3, prefixProto: protoVersion4},
	} {
		t.Run(tc.name, func(t *testing.T) {
			build := func(prefix []byte) []byte {
				f := newFramer(nil, tc.proto)
				f.scyllaUseMetadataID = tc.scyllaMeta
				if tc.trace {
					f.trace()
				}
				e := &writeExecuteFrame{preparedID: id, resultMetadataID: mid, prefix: prefix, params: params, customPayload: tc.customPayload}
				if err := e.buildFrame(f, 0x1234); err != nil {
					t.Fatal(err)
				}
				return f.buf
			}
			prefix := buildExecPrefix(tc.prefixProto, tc.prefixMeta, id, mid)
			if got, want := build(prefix), build(nil); !bytes.Equal(got, want) {
				t.Fatalf("prefix path\n got % x\nwant % x", got, want)
			}
			// The opcode byte is copied verbatim from the prefix, so a marked one shows
			// whether the fast path ran.
			marked := append([]byte(nil), prefix...)
			marked[4] = 0xff
			if fast := build(marked)[4] == 0xff; fast != tc.wantFast {
				t.Fatalf("fast path taken = %v, want %v", fast, tc.wantFast)
			}
		})
	}
}

func TestBuildExecPrefixRejectsOversizedID(t *testing.T) {
	tooLong := make([]byte, 1<<16)
	if p := buildExecPrefix(protoVersion4, false, tooLong, nil); p != nil {
		t.Fatal("oversized prepared id: want nil prefix")
	}
	if p := buildExecPrefix(protoVersion4, true, []byte{1}, tooLong); p != nil {
		t.Fatal("oversized result metadata id: want nil prefix")
	}
}

// A recycled frame must hand out zeroed values, sized to the getQueryValues buckets.
func TestExecuteFrameReset(t *testing.T) {
	e := &writeExecuteFrame{preparedID: []byte{1}, prefix: []byte{2}}
	v := e.values(3)
	if len(v) != 3 || cap(v) != 8 {
		t.Fatalf("values(3): len %d cap %d, want 3 and 8", len(v), cap(v))
	}
	v[0].value, v[2].name = []byte("x"), "n"
	e.reset()
	if e.preparedID != nil || e.prefix != nil {
		t.Fatalf("reset kept fields: %+v", e)
	}
	for i, q := range e.values(8) {
		if q.value != nil || q.name != "" {
			t.Fatalf("values[%d] = %+v after reset, want zero", i, q)
		}
	}

	// An oversized request must not displace the retained small buffer.
	if v := e.values(129); len(v) != 129 {
		t.Fatalf("values(129): len %d", len(v))
	}
	e.reset()
	if cap(e.valuesBuf) != 8 {
		t.Fatalf("buffer cap %d after oversized request, want retained 8", cap(e.valuesBuf))
	}
	// Mixed 100/200 widths keep reusing the 128-entry buffer.
	e.values(100)
	b := &e.valuesBuf[0]
	e.values(200)
	if e.values(100); &e.valuesBuf[0] != b {
		t.Fatal("128-entry buffer was replaced by an oversized request")
	}
}

// BenchmarkExecuteFrameBuild isolates EXECUTE serialization into a reused framer.
func BenchmarkExecuteFrameBuild(b *testing.B) {
	id := []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}
	v := bytes.Repeat([]byte{'x'}, 16)
	params := queryParams{
		consistency:           LocalQuorum,
		values:                []queryValues{{value: v}, {value: v}, {value: v}},
		defaultTimestamp:      true,
		defaultTimestampValue: 1700000000000000,
	}
	for _, bc := range []struct {
		name   string
		prefix []byte
	}{
		{name: "noprefix"},
		{name: "prefix", prefix: buildExecPrefix(protoVersion4, false, id, nil)},
	} {
		b.Run(bc.name, func(b *testing.B) {
			f := newFramer(nil, protoVersion4)
			e := &writeExecuteFrame{preparedID: id, prefix: bc.prefix, params: params}
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				if err := e.buildFrame(f, 1); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkExecutePreparedV4 measures a cached prepared EXECUTE end to end
// against the in-process test server (allocs include the server side).
func BenchmarkExecutePreparedV4(b *testing.B) {
	srv := NewTestServer(b, protoVersion4, context.Background())
	defer srv.Stop()

	cluster := testCluster(protoVersion4, srv.Address)
	cluster.NumConns = 1
	db, err := cluster.CreateSession()
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	q := db.Query("select nometadata from t")
	if err := q.Exec(); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := q.Exec(); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkInsertPreparedV4 is the application-shaped write path: a new Query per
// op with three bound values, executed as a cached prepared statement, void result.
func BenchmarkInsertPreparedV4(b *testing.B) {
	srv := NewTestServer(b, protoVersion4, context.Background())
	defer srv.Stop()
	srv.setPrepareHook(func(query string, stream int, resp *framer, _ func(uint64)) bool {
		resp.writeHeader(0, frm.OpResult, stream)
		resp.writeInt(frm.ResultKindPrepared)
		resp.writeShortBytes([]byte{0, 0, 0, 0, 0, 0, 0, 42})
		resp.writeInt(int32(frm.FlagGlobalTableSpec)) // <flags>
		resp.writeInt(3)                              // <columns_count>
		resp.writeInt(0)                              // <pk_count>
		resp.writeString("ks")
		resp.writeString("tbl")
		for _, c := range []struct {
			name string
			typ  Type
		}{{"id", TypeInt}, {"name", TypeVarchar}, {"ts", TypeBigInt}} {
			resp.writeString(c.name)
			resp.writeShort(uint16(c.typ))
		}
		resp.writeInt(int32(frm.FlagNoMetaData)) // <result_metadata flags>
		resp.writeInt(0)
		return true
	})
	srv.setExecuteHook(func(id uint64, _ *framer, stream int, resp *framer) bool {
		resp.writeHeader(0, frm.OpResult, stream)
		resp.writeInt(frm.ResultKindVoid)
		return true
	})

	cluster := testCluster(protoVersion4, srv.Address)
	cluster.NumConns = 1
	db, err := cluster.CreateSession()
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	const stmt = "insert into ks.tbl (id, name, ts) values (?, ?, ?)"
	if err := db.Query(stmt, 1, "n", int64(1)).Exec(); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		q := db.Query(stmt, i, "some-name", int64(i))
		if err := q.Exec(); err != nil {
			b.Fatal(err)
		}
		q.Release()
	}
}
