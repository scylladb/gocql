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
 * Copyright (c) 2016, The Gocql authors,
 * provided under the BSD-3-Clause License.
 * See the NOTICE file distributed with this work for additional information.
 */

package gocql

import (
	"bytes"
	"errors"
	"math"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	frm "github.com/gocql/gocql/internal/frame"
)

func TestFuzzBugs(t *testing.T) {
	t.Parallel()

	// these inputs are found using go-fuzz (https://github.com/dvyukov/go-fuzz)
	// and should cause a panic unless fixed.
	tests := [][]byte{
		[]byte("00000\xa0000"),
		[]byte("\x8000\x0e\x00\x00\x00\x000"),
		[]byte("\x8000\x00\x00\x00\x00\t0000000000"),
		[]byte("\xa0\xff\x01\xae\xefqE\xf2\x1a"),
		[]byte("\x8200\b\x00\x00\x00c\x00\x00\x00\x02000\x01\x00\x00\x00\x03" +
			"\x00\n0000000000\x00\x14000000" +
			"00000000000000\x00\x020000" +
			"\x00\a000000000\x00\x050000000" +
			"\xff0000000000000000000" +
			"0000000"),
		[]byte("\x82\xe600\x00\x00\x00\x000"),
		[]byte("\x8200\b\x00\x00\x00\b0\x00\x00\x00\x040000"),
		[]byte("\x83000\b\x00\x00\x00\x14\x00\x00\x00\x020000000" +
			"000000000"),
		[]byte("\x83000\b\x00\x00\x000\x00\x00\x00\x04\x00\x1000000" +
			"00000000000000e00000" +
			"000\x800000000000000000" +
			"0000000000000"),
	}

	for i, test := range tests {
		t.Logf("test %d input: %q", i, test)

		r := bytes.NewReader(test)
		head, err := readHeader(r, make([]byte, 9))
		if err != nil {
			continue
		}

		framer := newFramer(nil, byte(head.Version))
		err = framer.readFrame(r, &head)
		if err != nil {
			continue
		}

		frame, err := framer.parseFrame()
		if err != nil {
			continue
		}

		t.Errorf("(%d) expected to fail for input % X", i, test)
		t.Errorf("(%d) frame=%+#v", i, frame)
	}
}

func TestFrameWriteTooLong(t *testing.T) {
	t.Parallel()

	if os.Getenv("TRAVIS") == "true" {
		t.Skip("skipping test in travis due to memory pressure with the race detecor")
	}

	framer := newFramer(nil, 3)

	framer.writeHeader(0, frm.OpStartup, 1)
	framer.writeBytes(make([]byte, maxFrameSize+1))
	err := framer.finish()
	if err != ErrFrameTooBig {
		t.Fatalf("expected to get %v got %v", ErrFrameTooBig, err)
	}
}

func TestFrameReadTooLong(t *testing.T) {
	t.Parallel()

	if os.Getenv("TRAVIS") == "true" {
		t.Skip("skipping test in travis due to memory pressure with the race detecor")
	}

	r := &bytes.Buffer{}
	r.Write(make([]byte, maxFrameSize+1))
	// write a new header right after this frame to verify that we can read it
	r.Write([]byte{0x03, 0x00, 0x00, 0x00, byte(frm.OpReady), 0x00, 0x00, 0x00, 0x00})

	framer := newFramer(nil, 3)

	head := frm.FrameHeader{
		Version: protoVersion3,
		Op:      frm.OpReady,
		Length:  r.Len() - 9,
	}

	err := framer.readFrame(r, &head)
	if err != ErrFrameTooBig {
		t.Fatalf("expected to get %v got %v", ErrFrameTooBig, err)
	}

	head, err = readHeader(r, make([]byte, 9))
	if err != nil {
		t.Fatal(err)
	}
	if head.Op != frm.OpReady {
		t.Fatalf("expected to get header %v got %v", frm.OpReady, head.Op)
	}
}

func TestParseResultMetadata_PerColumnSpec(t *testing.T) {
	t.Parallel()

	// Build a synthetic ROWS result metadata frame with FlagGlobalTableSpec unset
	// (per-column keyspace/table encoding). This tests the !globalSpec optimization
	// in parseResultMetadata() which reads keyspace/table from the first column
	// position and reuses them for all columns via skipString().
	fr := newFramer(nil, protoVersion4)
	fr.header = &frm.FrameHeader{Version: protoVersion4}

	// flags: no FlagGlobalTableSpec — per-column keyspace/table
	fr.writeInt(0)
	// colCount
	fr.writeInt(3)

	// Column 0: keyspace/table + name + type
	fr.writeString("test_ks")
	fr.writeString("test_tbl")
	fr.writeString("col_a")
	fr.writeShort(uint16(TypeInt))

	// Column 1: same keyspace/table (will be skipped by optimization)
	fr.writeString("test_ks")
	fr.writeString("test_tbl")
	fr.writeString("col_b")
	fr.writeShort(uint16(TypeVarchar))

	// Column 2: same keyspace/table
	fr.writeString("test_ks")
	fr.writeString("test_tbl")
	fr.writeString("col_c")
	fr.writeShort(uint16(TypeBoolean))

	meta := fr.parseResultMetadata()

	if meta.colCount != 3 {
		t.Fatalf("colCount = %d, want 3", meta.colCount)
	}
	if len(meta.columns) != 3 {
		t.Fatalf("len(columns) = %d, want 3", len(meta.columns))
	}

	// Verify all columns got the correct keyspace/table from the optimization
	for i, col := range meta.columns {
		if col.Keyspace != "test_ks" {
			t.Errorf("columns[%d].Keyspace = %q, want %q", i, col.Keyspace, "test_ks")
		}
		if col.Table != "test_tbl" {
			t.Errorf("columns[%d].Table = %q, want %q", i, col.Table, "test_tbl")
		}
	}

	// Verify column names
	expectedNames := []string{"col_a", "col_b", "col_c"}
	for i, col := range meta.columns {
		if col.Name != expectedNames[i] {
			t.Errorf("columns[%d].Name = %q, want %q", i, col.Name, expectedNames[i])
		}
	}

	// Verify column types
	expectedTypes := []Type{TypeInt, TypeVarchar, TypeBoolean}
	for i, col := range meta.columns {
		nt, ok := col.TypeInfo.(NativeType)
		if !ok {
			t.Fatalf("columns[%d].TypeInfo is %T, want NativeType", i, col.TypeInfo)
		}
		if nt.typ != expectedTypes[i] {
			t.Errorf("columns[%d].Type = %v, want %v", i, nt.typ, expectedTypes[i])
		}
	}

	// Verify the entire buffer was consumed (no misalignment from skipString)
	if len(fr.buf) != 0 {
		t.Errorf("buffer has %d unconsumed bytes, want 0 (possible skipString misalignment)", len(fr.buf))
	}
}

// TestParseResultPreparedTruncatedResultMetadataID verifies that a malformed
// RESULT/Prepared frame whose resultMetadataID short-bytes length runs past the
// frame body is reported as an error, not a serve-goroutine panic. The extension
// makes this field live on protocol v4, and readShortBytesCopy panics with a
// plain error on a short buffer; parseFrame's recover must convert it to a
// returned error.
func TestParseResultPreparedTruncatedResultMetadataID(t *testing.T) {
	t.Parallel()

	fr := newFramer(nil, protoVersion4)
	// Response direction bit set so parseFrame does not reject it as a request.
	fr.header = &frm.FrameHeader{Version: protoVersion4 | 0x80, Op: frm.OpResult}
	fr.scyllaUseMetadataId = true

	fr.writeInt(frm.ResultKindPrepared)
	fr.writeShortBytes([]byte{0x01, 0x02, 0x03}) // preparedID
	// resultMetadataID: claim 10 bytes but supply none.
	fr.writeShort(10)

	frame, err := fr.parseFrame()
	if err == nil {
		t.Fatalf("expected an error for a truncated resultMetadataID, got frame %+v", frame)
	}
	if frame != nil {
		t.Errorf("expected nil frame on error, got %+v", frame)
	}
}

// TestParsePreparedMetadataRejectsInvalidPkeyCount verifies that a RESULT/Prepared
// frame declaring a negative or absurdly large partition-key count is reported as
// an error rather than crashing the serve goroutine (a negative count makes
// make([]int, n) raise a runtime error that parseFrame's recover re-panics) or
// forcing a huge speculative allocation from a small frame.
func TestParsePreparedMetadataRejectsInvalidPkeyCount(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name      string
		pkeyCount int32
	}{
		{name: "negative", pkeyCount: -1},
		{name: "huge", pkeyCount: 1 << 30},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			fr := newFramer(nil, protoVersion4)
			fr.header = &frm.FrameHeader{Version: protoVersion4 | 0x80, Op: frm.OpResult}

			fr.writeInt(frm.ResultKindPrepared)
			fr.writeShortBytes([]byte{0x01, 0x02, 0x03}) // preparedID
			// prepared metadata: flags=0, colCount=0, then the bogus pkeyCount.
			fr.writeInt(0)
			fr.writeInt(0)
			fr.writeInt(tc.pkeyCount)

			frame, err := fr.parseFrame()
			if err == nil {
				t.Fatalf("expected an error for pkeyCount=%d, got frame %+v", tc.pkeyCount, frame)
			}
			if frame != nil {
				t.Errorf("expected nil frame on error, got %+v", frame)
			}
		})
	}
}

func Test_framer_writeExecuteFrame(t *testing.T) {
	tests := []struct {
		name                 string
		protoVersion         byte
		scyllaUseMetadataId  bool
		resultMetadataID     []byte
		wantResultMetadataID []byte
	}{
		{
			name:                "protoVersion4 with ScyllaUseMetadataId false",
			protoVersion:        protoVersion4,
			scyllaUseMetadataId: false,
			resultMetadataID:    []byte{},
			// resultMetadataID is not written on v4 without the extension, so it is not read back.
		},
		{
			name:                 "protoVersion4 with ScyllaUseMetadataId true",
			protoVersion:         protoVersion4,
			scyllaUseMetadataId:  true,
			resultMetadataID:     []byte{4, 5, 6},
			wantResultMetadataID: []byte{4, 5, 6},
		},
		{
			name:                "protoVersion4 with ScyllaUseMetadataId true & nil resultMetadataID",
			protoVersion:        protoVersion4,
			scyllaUseMetadataId: true,
			// A resultPreparedFrame with a nil resultMetadataID (e.g. copyBytes(nil))
			// must serialize to a zero-length short bytes and read back as []byte{}.
			resultMetadataID:     nil,
			wantResultMetadataID: []byte{},
		},
		{
			name:                 "protoVersion5 with resultMetadataID support",
			protoVersion:         protoVersion5,
			scyllaUseMetadataId:  false,
			resultMetadataID:     []byte{4, 5, 6},
			wantResultMetadataID: []byte{4, 5, 6},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			framer := newFramer(nil, tt.protoVersion)
			if tt.scyllaUseMetadataId {
				framer.scyllaUseMetadataId = true
			}

			nowInSeconds := 123
			var params queryParams
			if tt.protoVersion >= protoVersion5 {
				params = queryParams{
					nowInSeconds: &nowInSeconds,
					keyspace:     "test_keyspace",
				}
			} else {
				params = queryParams{}
			}
			frame := writeExecuteFrame{
				preparedID:       []byte{1, 2, 3},
				resultMetadataID: tt.resultMetadataID,
				customPayload: map[string][]byte{
					"key1": []byte("value1"),
				},
				params: params,
			}

			err := framer.writeExecuteFrame(123, frame.preparedID, frame.resultMetadataID, &frame.params, &frame.customPayload)
			if err != nil {
				t.Fatal(err)
			}

			// skipping header
			framer.buf = framer.buf[9:]

			assertDeepEqual(t, "customPayload", frame.customPayload, framer.readBytesMap())
			assertDeepEqual(t, "preparedID", frame.preparedID, framer.readShortBytesCopy())

			if tt.protoVersion >= protoVersion5 || tt.scyllaUseMetadataId {
				assertDeepEqual(t, "resultMetadataID", tt.wantResultMetadataID, framer.readShortBytesCopy())
			}

			assertDeepEqual(t, "constistency", frame.params.consistency, Consistency(framer.readShort()))

			if tt.protoVersion >= protoVersion5 {
				flags := framer.readInt()
				if flags&int(frm.FlagWithNowInSeconds) != int(frm.FlagWithNowInSeconds) {
					t.Fatal("expected flagNowInSeconds to be set, but it is not")
				}

				if flags&int(frm.FlagWithKeyspace) != int(frm.FlagWithKeyspace) {
					t.Fatal("expected flagWithKeyspace to be set, but it is not")
				}
				assertDeepEqual(t, "keyspace", frame.params.keyspace, framer.readString())
				assertDeepEqual(t, "nowInSeconds", nowInSeconds, framer.readInt())
			}
		})
	}
}

func Test_framer_writeBatchFrame(t *testing.T) {
	framer := newFramer(nil, protoVersion5)
	nowInSeconds := 123
	frame := writeBatchFrame{
		customPayload: map[string][]byte{
			"key1": []byte("value1"),
		},
		nowInSeconds: &nowInSeconds,
	}

	err := framer.writeBatchFrame(123, &frame, frame.customPayload)
	if err != nil {
		t.Fatal(err)
	}

	// skipping header
	framer.buf = framer.buf[9:]

	assertDeepEqual(t, "customPayload", frame.customPayload, framer.readBytesMap())
	assertDeepEqual(t, "typ", frame.typ, BatchType(framer.readByte()))
	assertDeepEqual(t, "len(statements)", len(frame.statements), int(framer.readShort()))
	assertDeepEqual(t, "consistency", frame.consistency, Consistency(framer.readShort()))

	flags := framer.readInt()
	if flags&int(frm.FlagWithNowInSeconds) != int(frm.FlagWithNowInSeconds) {
		t.Fatal("expected flagNowInSeconds to be set, but it is not")
	}

	assertDeepEqual(t, "nowInSeconds", nowInSeconds, framer.readInt())
}

// Test_framer_writeBatchFrame_unnamedValues guards the happy path through the
// statement/values write loop (unnamed positional values), which must still
// succeed after named-value rejection was hoisted out of that loop.
func Test_framer_writeBatchFrame_unnamedValues(t *testing.T) {
	framer := newFramer(nil, protoVersion5)
	frame := writeBatchFrame{
		typ:         LoggedBatch,
		consistency: Quorum,
		statements: []batchStatment{
			{
				statement: "INSERT INTO t (id, v) VALUES (?, ?)",
				values: []queryValues{
					{value: []byte{0, 0, 0, 1}},
					{value: []byte("x")},
				},
			},
		},
	}

	if err := framer.writeBatchFrame(1, &frame, frame.customPayload); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// skipping header
	framer.buf = framer.buf[9:]
	assertDeepEqual(t, "typ", frame.typ, BatchType(framer.readByte()))
	assertDeepEqual(t, "len(statements)", len(frame.statements), int(framer.readShort()))
	assertDeepEqual(t, "kind", byte(0), framer.readByte()) // 0 = raw query string
	assertDeepEqual(t, "statement", frame.statements[0].statement, framer.readLongString())
	assertDeepEqual(t, "len(values)", len(frame.statements[0].values), int(framer.readShort()))
	assertDeepEqual(t, "value0", frame.statements[0].values[0].value, framer.readBytesCopy())
	assertDeepEqual(t, "value1", frame.statements[0].values[1].value, framer.readBytesCopy())
}

// On protocols below v5 the keyspace override and now_in_seconds options are
// not part of the wire format. The frame writers must reject them with an
// explicit error (rather than silently dropping them, panicking, or leaving a
// partial frame in the reusable framer buffer).
func Test_framer_writeQueryParams_rejectsUnsupportedOptionsOnV4(t *testing.T) {
	nowInSeconds := 123
	overflow := math.MaxInt32 + 1

	cases := []struct {
		name  string
		proto byte
		opts  queryParams
	}{
		{"keyspace on v4", protoVersion4, queryParams{consistency: Quorum, keyspace: "ks"}},
		{"nowInSeconds on v4", protoVersion4, queryParams{consistency: Quorum, nowInSeconds: &nowInSeconds}},
		{"nowInSeconds overflow on v5", protoVersion5, queryParams{consistency: Quorum, nowInSeconds: &overflow}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			framer := newFramer(nil, tc.proto)
			if err := framer.writeQueryParams(&tc.opts); err == nil {
				t.Fatal("expected an error, got nil")
			}
			if len(framer.buf) != 0 {
				t.Fatalf("expected framer buffer to be untouched on error, got %d bytes", len(framer.buf))
			}
		})
	}
}

func Test_framer_writeBatchFrame_rejectsUnsupportedOptionsOnV4(t *testing.T) {
	nowInSeconds := 123
	overflow := math.MaxInt32 + 1

	cases := []struct {
		name  string
		proto byte
		frame writeBatchFrame
	}{
		{"keyspace on v4", protoVersion4, writeBatchFrame{keyspace: "ks"}},
		{"nowInSeconds on v4", protoVersion4, writeBatchFrame{nowInSeconds: &nowInSeconds}},
		{"nowInSeconds overflow on v5", protoVersion5, writeBatchFrame{nowInSeconds: &overflow}},
		{"named values on v4", protoVersion4, writeBatchFrame{
			statements: []batchStatment{{statement: "INSERT INTO t (id) VALUES (?)", values: []queryValues{{name: "id", value: []byte{1}}}}},
		}},
		{"named values on v5", protoVersion5, writeBatchFrame{
			statements: []batchStatment{{statement: "INSERT INTO t (id) VALUES (?)", values: []queryValues{{name: "id", value: []byte{1}}}}},
		}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			framer := newFramer(nil, tc.proto)
			if err := framer.writeBatchFrame(1, &tc.frame, tc.frame.customPayload); err == nil {
				t.Fatal("expected an error, got nil")
			}
			if len(framer.buf) != 0 {
				t.Fatalf("expected framer buffer to be untouched on error, got %d bytes", len(framer.buf))
			}
		})
	}
}

func Test_framer_writePrepareFrame_rejectsKeyspaceOnV4(t *testing.T) {
	framer := newFramer(nil, protoVersion4)
	prep := &writePrepareFrame{statement: "SELECT * FROM t", keyspace: "ks"}

	// Must return an error, not panic.
	if err := prep.buildFrame(framer, 1); err == nil {
		t.Fatal("expected an error, got nil")
	}
	if len(framer.buf) != 0 {
		t.Fatalf("expected framer buffer to be untouched on error, got %d bytes", len(framer.buf))
	}
}

func Test_defaultFramerFlags(t *testing.T) {
	comp := testMockedCompressor{}

	cases := []struct {
		name       string
		compressor Compressor
		version    byte
		want       byte
	}{
		{"v4 no compressor", nil, protoVersion4, 0},
		{"v4 with compressor", comp, protoVersion4, frm.FlagCompress},
		{"v5 no compressor", nil, protoVersion5, frm.FlagBetaProtocol},
		// v5 compresses at the segment layer, so no frame-header FlagCompress.
		{"v5 with compressor", comp, protoVersion5, frm.FlagBetaProtocol},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := defaultFramerFlags(tc.compressor, tc.version); got != tc.want {
				t.Fatalf("defaultFramerFlags(%v, v%d) = 0x%02x, want 0x%02x", tc.compressor != nil, tc.version, got, tc.want)
			}
		})
	}
}

// newFramer must carry FlagBetaProtocol on proto v5 (so startup/fallback framers
// match the pooled framers from initCache) and must not carry it on v4.
func Test_newFramer_betaProtocolFlag(t *testing.T) {
	v5 := newFramer(nil, protoVersion5)
	if v5.flags&frm.FlagBetaProtocol == 0 {
		t.Error("newFramer(v5) should set FlagBetaProtocol")
	}
	if v5.flags&frm.FlagCompress != 0 {
		t.Error("newFramer(v5) should not set FlagCompress (v5 compresses at the segment layer)")
	}

	v4 := newFramer(nil, protoVersion4)
	if v4.flags&frm.FlagBetaProtocol != 0 {
		t.Error("newFramer(v4) should not set FlagBetaProtocol")
	}
}

// versionFramerFlags carries only the version-derived flags (FlagBetaProtocol on
// v5) and never a compressor-derived flag, since it is applied before the
// compressor is negotiated.
func Test_versionFramerFlags(t *testing.T) {
	if got := versionFramerFlags(protoVersion5); got != frm.FlagBetaProtocol {
		t.Errorf("versionFramerFlags(v5) = 0x%02x, want 0x%02x", got, frm.FlagBetaProtocol)
	}
	if got := versionFramerFlags(protoVersion4); got != 0 {
		t.Errorf("versionFramerFlags(v4) = 0x%02x, want 0x00", got)
	}
	// The version byte may carry the direction/reserved high bit (newFramer
	// passes it unmasked); masking must still resolve the beta flag.
	if got := versionFramerFlags(protoVersion5 | protoDirectionMask); got != frm.FlagBetaProtocol {
		t.Errorf("versionFramerFlags(v5|dir) = 0x%02x, want 0x%02x", got, frm.FlagBetaProtocol)
	}
}

func Test_compressionFramerFlag(t *testing.T) {
	comp := testMockedCompressor{}
	if got := compressionFramerFlag(comp, protoVersion4); got != frm.FlagCompress {
		t.Errorf("compressionFramerFlag(comp, v4) = 0x%02x, want 0x%02x", got, frm.FlagCompress)
	}
	if got := compressionFramerFlag(nil, protoVersion4); got != 0 {
		t.Errorf("compressionFramerFlag(nil, v4) = 0x%02x, want 0x00", got)
	}
	// v5 compresses at the segment layer, so no frame-header FlagCompress.
	if got := compressionFramerFlag(comp, protoVersion5); got != 0 {
		t.Errorf("compressionFramerFlag(comp, v5) = 0x%02x, want 0x00", got)
	}
	// A direction/reserved high bit on the version must not defeat the v5 check
	// and re-enable FlagCompress at v5.
	if got := compressionFramerFlag(comp, protoVersion5|protoDirectionMask); got != 0 {
		t.Errorf("compressionFramerFlag(comp, v5|dir) = 0x%02x, want 0x00", got)
	}
	if got := compressionFramerFlag(comp, protoVersion4|protoDirectionMask); got != frm.FlagCompress {
		t.Errorf("compressionFramerFlag(comp, v4|dir) = 0x%02x, want 0x%02x", got, frm.FlagCompress)
	}
}

// initDefaults must seed FlagBetaProtocol on proto v5 before initCache runs, so
// that getWriteFramer does not strip the beta flag from handshake frames.
func Test_connFramers_initDefaults_betaBeforeCache(t *testing.T) {
	c := &Conn{version: protoVersion5}
	c.initDefaults()

	if c.framers.defaults.flags&frm.FlagBetaProtocol == 0 {
		t.Error("initDefaults(v5) should seed FlagBetaProtocol before the handshake")
	}
	if c.framers.defaults.flags&frm.FlagCompress != 0 {
		t.Error("initDefaults must not seed FlagCompress before the compressor is negotiated")
	}

	c4 := &Conn{version: protoVersion4}
	c4.initDefaults()
	if c4.framers.defaults.flags&frm.FlagBetaProtocol != 0 {
		t.Error("initDefaults(v4) should not seed FlagBetaProtocol")
	}
}

type testMockedCompressor struct {
	// this is an error its methods should return
	expectedError error

	// invalidateDecodedDataLength allows to simulate data decoding invalidation
	invalidateDecodedDataLength bool
}

func (m testMockedCompressor) Name() string {
	return "testMockedCompressor"
}

func (m testMockedCompressor) AppendCompressed(_, src []byte) ([]byte, error) {
	if m.expectedError != nil {
		return nil, m.expectedError
	}
	return src, nil
}

func (m testMockedCompressor) AppendDecompressed(_, src []byte, decompressedLength uint32) ([]byte, error) {
	if m.expectedError != nil {
		return nil, m.expectedError
	}

	// simulating invalid size of decoded data
	if m.invalidateDecodedDataLength {
		return src[:decompressedLength-1], nil
	}

	return src, nil
}

func (m testMockedCompressor) Encode(data []byte) ([]byte, error) {
	if m.expectedError != nil {
		return nil, m.expectedError
	}
	return data, nil
}

func (m testMockedCompressor) Decode(data []byte) ([]byte, error) {
	if m.expectedError != nil {
		return nil, m.expectedError
	}
	return data, nil
}

func Test_readUncompressedFrame(t *testing.T) {
	tests := []struct {
		name        string
		modifyFrame func([]byte) []byte
		expectedErr string
	}{
		{
			name: "header crc24 mismatch",
			modifyFrame: func(frame []byte) []byte {
				// simulating some crc invalidation
				frame[0] = 255
				return frame
			},
			expectedErr: "gocql: crc24 mismatch in frame header",
		},
		{
			name: "body crc32 mismatch",
			modifyFrame: func(frame []byte) []byte {
				// simulating body crc32 mismatch
				frame[len(frame)-1] = 255
				return frame
			},
			expectedErr: "gocql: payload crc32 mismatch",
		},
		{
			name: "invalid frame length",
			modifyFrame: func(frame []byte) []byte {
				// simulating body length invalidation
				frame = frame[:7]
				return frame
			},
			expectedErr: "gocql: failed to read uncompressed frame payload",
		},
		{
			name: "cannot read body checksum",
			modifyFrame: func(frame []byte) []byte {
				// simulating body length invalidation
				frame = frame[:len(frame)-4]
				return frame
			},
			expectedErr: "gocql: failed to read payload crc32",
		},
		{
			name:        "success",
			modifyFrame: nil,
			expectedErr: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			framer := newFramer(nil, protoVersion5)
			req := writeQueryFrame{
				statement: "SELECT * FROM system.local",
				params: queryParams{
					consistency: Quorum,
					keyspace:    "gocql_test",
				},
			}

			err := req.buildFrame(framer, 128)
			require.NoError(t, err)

			frame, err := newUncompressedSegment(framer.buf, true)
			require.NoError(t, err)

			if tt.modifyFrame != nil {
				frame = tt.modifyFrame(frame)
			}

			readFrame, isSelfContained, err := readUncompressedSegment(bytes.NewReader(frame))

			if tt.expectedErr != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tt.expectedErr)
			} else {
				require.NoError(t, err)
				assert.True(t, isSelfContained)
				assert.Equal(t, framer.buf, readFrame)
			}
		})
	}
}

func Test_readCompressedFrame(t *testing.T) {
	tests := []struct {
		name string
		// modifyFrameFn is useful for simulating frame data invalidation
		modifyFrameFn func([]byte) []byte
		compressor    testMockedCompressor

		// expectedErrorMsg is an error message that should be returned by Error() method.
		// We need this to understand which of fmt.Errorf() is returned
		expectedErrorMsg string
	}{
		{
			name: "header crc24 mismatch",
			modifyFrameFn: func(frame []byte) []byte {
				// simulating some crc invalidation
				frame[0] = 255
				return frame
			},
			expectedErrorMsg: "gocql: crc24 mismatch in frame header",
		},
		{
			name: "body crc32 mismatch",
			modifyFrameFn: func(frame []byte) []byte {
				// simulating body crc32 mismatch
				frame[len(frame)-1] = 255
				return frame
			},
			expectedErrorMsg: "gocql: crc32 mismatch in payload",
		},
		{
			name: "invalid frame length",
			modifyFrameFn: func(frame []byte) []byte {
				// simulating body length invalidation
				return frame[:12]
			},
			expectedErrorMsg: "gocql: failed to read compressed frame payload",
		},
		{
			name: "cannot read body checksum",
			modifyFrameFn: func(frame []byte) []byte {
				// simulating body length invalidation
				return frame[:len(frame)-4]
			},
			expectedErrorMsg: "gocql: failed to read payload crc32",
		},
		{
			name:          "failed to encode payload",
			modifyFrameFn: nil,
			compressor: testMockedCompressor{
				expectedError: errors.New("failed to encode payload"),
			},
			expectedErrorMsg: "failed to encode payload",
		},
		{
			name:          "failed to decode payload",
			modifyFrameFn: nil,
			compressor: testMockedCompressor{
				expectedError: errors.New("failed to decode payload"),
			},
			expectedErrorMsg: "failed to decode payload",
		},
		{
			name:          "length mismatch after decoding",
			modifyFrameFn: nil,
			compressor: testMockedCompressor{
				invalidateDecodedDataLength: true,
			},
			expectedErrorMsg: "gocql: length mismatch after payload decoding",
		},
		{
			name:             "success",
			modifyFrameFn:    nil,
			expectedErrorMsg: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			framer := newFramer(nil, protoVersion5)
			req := writeQueryFrame{
				statement: "SELECT * FROM system.local",
				params: queryParams{
					consistency: Quorum,
					keyspace:    "gocql_test",
				},
			}

			err := req.buildFrame(framer, 128)
			require.NoError(t, err)

			frame, err := newCompressedSegment(framer.buf, true, testMockedCompressor{})
			require.NoError(t, err)

			if tt.modifyFrameFn != nil {
				frame = tt.modifyFrameFn(frame)
			}

			readFrame, selfContained, err := readCompressedSegment(bytes.NewReader(frame), tt.compressor)

			switch {
			case tt.expectedErrorMsg != "":
				require.Error(t, err)
				require.Contains(t, err.Error(), tt.expectedErrorMsg)
			case tt.compressor.expectedError != nil:
				require.ErrorIs(t, err, tt.compressor.expectedError)
			default:
				require.NoError(t, err)
				assert.True(t, selfContained)
				assert.Equal(t, framer.buf, readFrame)
			}
		})
	}
}

func TestParseEventFrame_ClientRoutesChanged(t *testing.T) {
	t.Parallel()

	fr := newFramer(nil, protoVersion4)
	fr.header = &frm.FrameHeader{Version: protoVersion4}
	fr.writeString("CLIENT_ROUTES_CHANGE")
	fr.writeString("UPDATED")
	fr.writeStringList([]string{"c1", ""})
	fr.writeStringList([]string{})

	frame := fr.parseEventFrame()
	evt, ok := frame.(*frm.ClientRoutesChanged)
	if !ok {
		t.Fatalf("expected ClientRoutesChanged frame, got %T", frame)
	}
	if evt.ChangeType != "UPDATED" {
		t.Fatalf("ChangeType = %v, want UPDATED", evt.ChangeType)
	}
	if len(evt.ConnectionIDs) != 2 || evt.ConnectionIDs[1] != "" {
		t.Fatalf("ConnectionIDs = %v, want [c1 \"\"]", evt.ConnectionIDs)
	}
	if len(evt.HostIDs) != 0 {
		t.Fatalf("HostIDs = %v, want empty", evt.HostIDs)
	}
}

// failingCompressor compresses by copying (append semantics), but returns an
// error on the (failAt)th AppendCompressed call (1-indexed). It lets a test
// force prepareModernLayout to fail partway through multi-segment framing.
type failingCompressor struct {
	failAt int
	calls  int
}

func (c *failingCompressor) Name() string { return "failing" }

func (c *failingCompressor) AppendCompressed(dst, src []byte) ([]byte, error) {
	c.calls++
	if c.calls == c.failAt {
		return nil, errors.New("compress boom")
	}
	return append(dst, src...), nil
}

func (c *failingCompressor) AppendDecompressed(dst, src []byte, _ uint32) ([]byte, error) {
	return append(dst, src...), nil
}

func (c *failingCompressor) Encode(data []byte) ([]byte, error) {
	return data, nil
}

func (c *failingCompressor) Decode(data []byte) ([]byte, error) {
	return data, nil
}

// TestPrepareModernLayoutLeavesBufIntactOnError verifies that when segmentation
// fails partway through a multi-segment frame, framer.buf is left byte-for-byte
// unchanged so the caller can safely release the framer.
func TestPrepareModernLayoutLeavesBufIntactOnError(t *testing.T) {
	t.Parallel()

	// A payload spanning more than one maxSegmentPayloadSize chunk forces the
	// chunk loop to run, so failing on the second AppendCompressed call fails
	// after the first chunk has already been appended to the local buffer.
	original := bytes.Repeat([]byte{0xAB}, maxSegmentPayloadSize+100)

	f := newFramer(&failingCompressor{failAt: 2}, protoVersion5)
	f.buf = append([]byte(nil), original...)

	err := f.prepareModernLayout()
	if err == nil {
		t.Fatal("expected prepareModernLayout to fail")
	}
	if !bytes.Equal(f.buf, original) {
		t.Fatalf("f.buf was mutated on error: len=%d, want len=%d", len(f.buf), len(original))
	}
}

// TestPrepareModernLayoutRejectsPreV5ProtocolWithError verifies that calling
// prepareModernLayout on a framer negotiated below protocol v5 returns an
// error instead of panicking, since the function's contract is to report
// every failure mode (including this internal precondition) via its error
// return.
func TestPrepareModernLayoutRejectsPreV5ProtocolWithError(t *testing.T) {
	t.Parallel()

	f := newFramer(nil, protoVersion4)
	f.buf = append([]byte(nil), []byte("some frame bytes")...)

	require.NotPanics(t, func() {
		err := f.prepareModernLayout()
		require.Error(t, err)
	})
}

// TestPrepareModernLayoutSuccessUnchanged guards that the local-cursor refactor
// did not change the segmented output on the success path.
func TestPrepareModernLayoutSuccessUnchanged(t *testing.T) {
	t.Parallel()

	for _, size := range []int{1, maxSegmentPayloadSize - 1, maxSegmentPayloadSize, maxSegmentPayloadSize + 1, 2*maxSegmentPayloadSize + 7} {
		original := bytes.Repeat([]byte{0x5A}, size)

		// Reference output computed directly from the segment helpers.
		var want []byte
		src := original
		selfContained := true
		for len(src) > maxSegmentPayloadSize {
			seg, err := newUncompressedSegment(src[:maxSegmentPayloadSize], false)
			if err != nil {
				t.Fatalf("size %d: reference segment: %v", size, err)
			}
			want = append(want, seg...)
			src = src[maxSegmentPayloadSize:]
			selfContained = false
		}
		seg, err := newUncompressedSegment(src, selfContained)
		if err != nil {
			t.Fatalf("size %d: reference tail segment: %v", size, err)
		}
		want = append(want, seg...)

		f := newFramer(nil, protoVersion5)
		f.buf = append([]byte(nil), original...)
		if err := f.prepareModernLayout(); err != nil {
			t.Fatalf("size %d: prepareModernLayout: %v", size, err)
		}
		if !bytes.Equal(f.buf, want) {
			t.Fatalf("size %d: segmented output changed", size)
		}
	}
}
