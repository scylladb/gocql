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
	"io"
	"math"
	"net"
	"os"
	"runtime"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	frm "github.com/gocql/gocql/internal/frame"
	"github.com/gocql/gocql/internal/segment"
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
	framer.writeBytes(make([]byte, frm.MaxFrameSize+1))
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
	r.Write(make([]byte, frm.MaxFrameSize+1))
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

// TestReadFrameBodyErrorKeepsNetError pins that a failed body read stays
// recognisable as a net.Error through readFrame's wrapping. Conn.processFrameSource
// decides whether the connection is desynced (and must be closed) from exactly this
// check; formatting the cause with %v instead of %w silently defeats it, and the
// half-read body is then read back as the next frame header.
func TestReadFrameBodyErrorKeepsNetError(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name string
		r    io.Reader
		want bool
	}{
		{
			name: "timeout mid-body",
			r:    io.MultiReader(bytes.NewReader([]byte{0x01, 0x02}), errReader{timeoutErr{}}),
			want: true,
		},
		{
			// A peer that closes mid-body is equally desyncing, but io.ErrUnexpectedEOF
			// is not a net.Error; the connection dies on the next read instead.
			name: "peer closed mid-body",
			r:    bytes.NewReader([]byte{0x01, 0x02}),
			want: false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newFramer(nil, protoVersion4)
			head := frm.FrameHeader{Version: protoVersion4 | protoDirectionMask, Op: frm.OpReady, Length: 8}

			err := f.readFrame(tc.r, &head)
			require.Error(t, err)

			var netErr net.Error
			require.Equal(t, tc.want, errors.As(err, &netErr),
				"errors.As(net.Error) on %q", err)
		})
	}
}

// TestReadFrameDiscardErrorKeepsNetError is the same pin for the oversized-frame
// path: an over-long frame is normally recovered from by discarding its body, but
// if the discard itself fails the remainder is still on the wire.
func TestReadFrameDiscardErrorKeepsNetError(t *testing.T) {
	t.Parallel()

	f := newFramer(nil, protoVersion4)
	head := frm.FrameHeader{Version: protoVersion4 | protoDirectionMask, Op: frm.OpReady, Length: frm.MaxFrameSize + 1}

	err := f.readFrame(errReader{timeoutErr{}}, &head)
	require.Error(t, err)
	require.NotErrorIs(t, err, ErrFrameTooBig, "the discard failed, so the frame was not skipped")

	var netErr net.Error
	require.True(t, errors.As(err, &netErr), "errors.As(net.Error) on %q", err)
}

// TestReadHeaderRejectsNegativeLength pins that a length field with the high bit
// set is rejected by readHeader. The field is signed on the wire, so such a value
// arrives negative; readFrame also rejects it, but only an error out of readHeader
// closes the connection, and a header this broken means the stream position can no
// longer be trusted.
func TestReadHeaderRejectsNegativeLength(t *testing.T) {
	t.Parallel()

	header := []byte{
		protoVersion4 | protoDirectionMask, 0x00, 0x00, 0x01, byte(frm.OpResult),
		0xFF, 0xFF, 0xFF, 0xFF, // length = -1
	}

	_, err := readHeader(bytes.NewReader(header), make([]byte, headSize))
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid frame body length")
}

// errReader fails every read with err, standing in for a socket whose read
// deadline expired or which was reset.
type errReader struct{ err error }

func (e errReader) Read([]byte) (int, error) { return 0, e.err }

// TestParseResultMetadata_PagingStateBeforeNewMetadataID pins the wire order of
// the two optional fields in a v5 RESULT/Rows <metadata> block.
//
// With only one of HAS_MORE_PAGES / METADATA_CHANGED set, either ordering parses
// identically, so no other test in this repo can tell them apart. Only a frame
// carrying both distinguishes them, and such a frame is hard to obtain from a
// live server (it needs the client to execute with a stale result_metadata_id
// while a page boundary falls in the same response). Synthesising it here is
// both deterministic and stronger than an integration test.
//
// The layout asserted below is the one Cassandra actually emits, read out of
// ResultSet$ResultMetadata$Codec.encode in the 5.0.6 distribution:
//
//	writeInt(flags)
//	writeInt(columnCount)
//	if HAS_MORE_PAGES:                CBUtil.writeValue(pagingState)    // [bytes]
//	if v5+ && METADATA_CHANGED:       CBUtil.writeBytes(resultMetadataId) // [short bytes]
//	if !NO_METADATA:                  global table spec / column specs
//
// Note the differing wire types: paging state is [bytes] (4-byte length) and the
// metadata id is [short bytes] (2-byte length), so swapping the two reads
// desynchronises the rest of the block rather than merely exchanging the values.
//
// Run for both ways the field can be on the wire: native v5, and v4 with
// SCYLLA_USE_METADATA_ID negotiated. The extension reuses this parser, so the v4
// case is the only unit coverage of the read side of METADATA_CHANGED on v4 —
// otherwise it rests entirely on an integration test that skips unless a capable
// server is present.
func TestParseResultMetadata_PagingStateBeforeNewMetadataID(t *testing.T) {
	t.Parallel()

	const (
		keyspace = "test_ks"
		table    = "test_tbl"
	)
	pagingState := []byte{0xDE, 0xAD, 0xBE, 0xEF, 0x01}
	newMetadataID := []byte{0xAA, 0xBB, 0xCC}

	for _, tc := range []struct {
		name      string
		proto     byte
		extension bool
	}{
		{name: "native v5", proto: protoVersion5},
		{name: "v4 with SCYLLA_USE_METADATA_ID", proto: protoVersion4, extension: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			fr := newFramer(nil, tc.proto)
			fr.header = &frm.FrameHeader{Version: frm.ProtoVersion(tc.proto)}
			fr.scyllaUseMetadataID = tc.extension

			fr.writeInt(int32(frm.FlagGlobalTableSpec | frm.FlagHasMorePages | frm.FlagMetaDataChanged))
			fr.writeInt(1) // colCount
			fr.writeBytes(pagingState)
			fr.writeShortBytes(newMetadataID)
			fr.writeString(keyspace)
			fr.writeString(table)
			fr.writeString("col_a")
			fr.writeShort(uint16(TypeInt))

			meta := fr.parseResultMetadata()

			assertDeepEqual(t, "pagingState", pagingState, meta.pagingState)
			assertDeepEqual(t, "newMetadataID", newMetadataID, meta.newMetadataID)

			// The column spec must still be readable, which is what actually proves the
			// two optional fields were consumed in the right order and with the right
			// wire types.
			require.Len(t, meta.columns, 1)
			require.Equal(t, keyspace, meta.columns[0].Keyspace)
			require.Equal(t, table, meta.columns[0].Table)
			require.Equal(t, "col_a", meta.columns[0].Name)
			require.Empty(t, fr.buf, "whole metadata block should be consumed")
		})
	}
}

// TestParseResultMetadata_NewMetadataIDIgnoredBelowV5 pins that the
// METADATA_CHANGED field is only on the wire from v5 onwards: on v4 the flag bit
// must not cause a read, or the parser would consume bytes that belong to the
// column specs.
func TestParseResultMetadata_NewMetadataIDIgnoredBelowV5(t *testing.T) {
	t.Parallel()

	fr := newFramer(nil, protoVersion4)
	fr.header = &frm.FrameHeader{Version: protoVersion4}

	// METADATA_CHANGED set but no id on the wire, as a v4 server would send.
	fr.writeInt(int32(frm.FlagGlobalTableSpec | frm.FlagMetaDataChanged))
	fr.writeInt(1)
	fr.writeString("test_ks")
	fr.writeString("test_tbl")
	fr.writeString("col_a")
	fr.writeShort(uint16(TypeInt))

	meta := fr.parseResultMetadata()

	require.Nil(t, meta.newMetadataID, "no metadata id should be read below v5")
	require.Len(t, meta.columns, 1)
	require.Equal(t, "col_a", meta.columns[0].Name)
	require.Empty(t, fr.buf, "whole metadata block should be consumed")
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
			fr.header = &frm.FrameHeader{Version: protoVersion4 | protoDirectionMask, Op: frm.OpResult}

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
			// Assert on the message, not just on err != nil. Without the guard the
			// "huge" case still returns an error — make() succeeds and the first
			// readShort() then panics on the empty buffer — so a bare err != nil
			// check passes either way and the allocation bound goes untested.
			require.ErrorContains(t, err, "invalid partition key count")
		})
	}
}

// TestParsePreparedMetadataAcceptsValidPkeyCount pins the upper half of the
// pkeyCount guard: a count that the remaining buffer can actually supply must be
// accepted. Without this, a bound tightened by mistake (down to `> 0`, say) would
// reject every prepared statement that has a partition key and no unit test would
// notice — pk_count is 0 in every other prepared frame the suite builds.
func TestParsePreparedMetadataAcceptsValidPkeyCount(t *testing.T) {
	t.Parallel()

	t.Run("exactly at the bound", func(t *testing.T) {
		t.Parallel()

		fr := newFramer(nil, protoVersion4)
		fr.header = &frm.FrameHeader{Version: protoVersion4 | protoDirectionMask}

		// NO_METADATA so parsing stops right after the pkeys, leaving the buffer
		// holding exactly 2*pkeyCount bytes at the guard: pkeyCount == len(buf)/2.
		fr.writeInt(int32(frm.FlagNoMetaData))
		fr.writeInt(0) // colCount
		fr.writeInt(2) // pkeyCount
		fr.writeShort(0)
		fr.writeShort(1)

		// NotPanics, because parsePreparedMetadata is called directly here and so
		// the guard's panic would otherwise take down the whole test binary rather
		// than failing this subtest.
		var meta preparedMetadata
		require.NotPanics(t, func() { meta = fr.parsePreparedMetadata() })

		require.Equal(t, []int{0, 1}, meta.pkeyColumns)
		require.Empty(t, fr.buf, "whole metadata block should be consumed")
	})

	t.Run("full prepared frame", func(t *testing.T) {
		t.Parallel()

		fr := newFramer(nil, protoVersion4)
		fr.header = &frm.FrameHeader{Version: protoVersion4 | protoDirectionMask, Op: frm.OpResult}

		fr.writeInt(frm.ResultKindPrepared)
		fr.writeShortBytes([]byte{0x01, 0x02, 0x03}) // preparedID
		// prepared metadata
		fr.writeInt(int32(frm.FlagGlobalTableSpec))
		fr.writeInt(2) // colCount
		fr.writeInt(2) // pkeyCount
		fr.writeShort(0)
		fr.writeShort(1)
		fr.writeString("test_ks")
		fr.writeString("test_tbl")
		fr.writeString("pk_a")
		fr.writeShort(uint16(TypeInt))
		fr.writeString("pk_b")
		fr.writeShort(uint16(TypeVarchar))
		// result metadata
		fr.writeInt(int32(frm.FlagNoMetaData))
		fr.writeInt(0)

		frame, err := fr.parseFrame()
		require.NoError(t, err)

		prepared, ok := frame.(*resultPreparedFrame)
		require.True(t, ok, "got %T", frame)
		require.Equal(t, []int{0, 1}, prepared.reqMeta.pkeyColumns)
		require.Len(t, prepared.reqMeta.columns, 2)
		require.Empty(t, fr.buf, "whole frame should be consumed")
	})
}

// TestParseResultPreparedTruncatedResultMetadataID verifies that a malformed
// RESULT/Prepared frame whose resultMetadataID short-bytes length runs past the
// frame body is reported as an error, not a serve-goroutine panic. The extension
// makes this field live on protocol v4, and readShortBytesCopy panics with a
// plain error on a short buffer; parseFrame's recover must convert it to a
// returned error.
// readLongString's length is signed, and it lacked the negative guard its siblings
// readBytesCopy and ReadBytesInternal carry. len(f.buf) is never below a negative,
// so the bounds check passed and f.buf[:size] raised a runtime.Error -- which
// parseFrame's recover deliberately re-panics rather than converting.
//
// The assertion is that contract, not the message: every read helper panics with a
// plain error, and that is the whole reason a truncated frame is a protocol error
// instead of a dead serve goroutine. A helper that breaks it breaks the recover.
func TestReadLongStringRejectsNegativeLength(t *testing.T) {
	f := newFramer(nil, protoVersion4)
	f.buf = []byte{0xFF, 0xFF, 0xFF, 0xFF, 'x'} // a declared length of -1

	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("a negative long string length was accepted")
		}
		if _, ok := r.(runtime.Error); ok {
			t.Fatalf("panicked with a runtime.Error, which parseFrame's recover re-panics: %v", r)
		}
		err, ok := r.(error)
		if !ok {
			t.Fatalf("panicked with %T, which parseFrame's recover cannot convert: %v", r, r)
		}
		if !strings.Contains(err.Error(), "-1") {
			t.Errorf("the error does not name the length: %v", err)
		}
	}()

	f.readLongString()
}

func TestParseResultPreparedTruncatedResultMetadataID(t *testing.T) {
	t.Parallel()

	fr := newFramer(nil, protoVersion4)
	// Response direction bit set so parseFrame does not reject it as a request.
	fr.header = &frm.FrameHeader{Version: protoVersion4 | 0x80, Op: frm.OpResult}
	fr.scyllaUseMetadataID = true

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

func Test_framer_writeExecuteFrame(t *testing.T) {
	tests := []struct {
		name                 string
		protoVersion         byte
		scyllaUseMetadataID  bool
		resultMetadataID     []byte
		wantResultMetadataID []byte
	}{
		{
			name:                "protoVersion4 with ScyllaUseMetadataID false",
			protoVersion:        protoVersion4,
			scyllaUseMetadataID: false,
			resultMetadataID:    []byte{},
			// resultMetadataID is not written on v4 without the extension, so it is not read back.
		},
		{
			name:                 "protoVersion4 with ScyllaUseMetadataID true",
			protoVersion:         protoVersion4,
			scyllaUseMetadataID:  true,
			resultMetadataID:     []byte{4, 5, 6},
			wantResultMetadataID: []byte{4, 5, 6},
		},
		{
			name:                "protoVersion4 with ScyllaUseMetadataID true & nil resultMetadataID",
			protoVersion:        protoVersion4,
			scyllaUseMetadataID: true,
			// A resultPreparedFrame with a nil resultMetadataID (e.g. copyBytes(nil))
			// must serialize to a zero-length short bytes and read back as []byte{}.
			resultMetadataID:     nil,
			wantResultMetadataID: []byte{},
		},
		{
			name:                 "protoVersion5 with resultMetadataID support",
			protoVersion:         protoVersion5,
			scyllaUseMetadataID:  false,
			resultMetadataID:     []byte{4, 5, 6},
			wantResultMetadataID: []byte{4, 5, 6},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			framer := newFramer(nil, tt.protoVersion)
			if tt.scyllaUseMetadataID {
				framer.scyllaUseMetadataID = true
			}

			nowInSeconds := 123
			// A non-zero consistency, so reading it back is an assertion that can
			// actually fail: with the zero value on both sides a misaligned read of
			// Consistency(ANY) would still compare equal.
			params := queryParams{consistency: Quorum}
			if tt.protoVersion >= protoVersion5 {
				params.nowInSeconds = &nowInSeconds
				params.keyspace = "test_keyspace"
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

			if tt.protoVersion >= protoVersion5 || tt.scyllaUseMetadataID {
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
			} else {
				// Below v5 the query flags are a single byte, and nothing follows them
				// here. Consuming them and requiring the buffer to be empty turns this
				// into a check that every preceding field was read at exactly the length
				// it was written — in particular the resultMetadataID short bytes.
				require.Zero(t, framer.readByte(), "no query flags expected")
				require.Empty(t, framer.buf, "whole EXECUTE body should be consumed")
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
//
// Driven through buildFrame, the entry point Conn.exec actually uses, for both
// writers that carry queryParams. Calling writeQueryParams directly would assert
// nothing: it is the first thing a fresh framer does, so `len(buf) == 0` holds
// whatever the writer does. Via buildFrame the assertion has teeth — writeQueryFrame
// writes the header, custom payload and statement before writeQueryParams runs, and
// writeExecuteFrame additionally writes the prepared id and (on v5) the result
// metadata id.
//
// wantErr pins which validation rejected the frame, so a case cannot pass by
// tripping an unrelated one.
func Test_framer_queryParamsWriters_rejectUnsupportedOptionsOnV4(t *testing.T) {
	nowInSeconds := 123
	overflow := math.MaxInt32 + 1

	cases := []struct {
		name    string
		proto   byte
		opts    queryParams
		wantErr string
	}{
		{
			name:    "keyspace on v4",
			proto:   protoVersion4,
			opts:    queryParams{consistency: Quorum, keyspace: "ks"},
			wantErr: "keyspace override can only be set with protocol v5 or higher",
		},
		{
			name:    "nowInSeconds on v4",
			proto:   protoVersion4,
			opts:    queryParams{consistency: Quorum, nowInSeconds: &nowInSeconds},
			wantErr: "now_in_seconds can only be set with protocol v5 or higher",
		},
		{
			name:    "nowInSeconds overflow on v5",
			proto:   protoVersion5,
			opts:    queryParams{consistency: Quorum, nowInSeconds: &overflow},
			wantErr: "overflows int32",
		},
	}

	writers := []struct {
		name  string
		build func(opts queryParams) frameBuilder
	}{
		{
			name: "QUERY",
			build: func(opts queryParams) frameBuilder {
				return &writeQueryFrame{statement: "SELECT * FROM system.local", params: opts}
			},
		},
		{
			name: "EXECUTE",
			build: func(opts queryParams) frameBuilder {
				return &writeExecuteFrame{
					preparedID:       []byte{0x01, 0x02},
					resultMetadataID: []byte{0x03, 0x04},
					params:           opts,
				}
			},
		},
	}

	for _, w := range writers {
		t.Run(w.name, func(t *testing.T) {
			for _, tc := range cases {
				t.Run(tc.name, func(t *testing.T) {
					framer := newFramer(nil, tc.proto)

					err := w.build(tc.opts).buildFrame(framer, 1)
					require.Error(t, err)
					require.Contains(t, err.Error(), tc.wantErr)
					require.Empty(t, framer.buf, "a rejected option must not leave a partial frame in the framer buffer")
				})
			}
		})
	}
}

// Test_framer_validateV5Options_acceptsSupportedOptions is the positive half: the
// shared validator must not reject what v5 does support, or the writers above
// would fail every legitimate v5 request.
func Test_framer_validateV5Options_acceptsSupportedOptions(t *testing.T) {
	nowInSeconds := 123
	minInt32, maxInt32 := math.MinInt32, math.MaxInt32

	v5 := newFramer(nil, protoVersion5)
	require.NoError(t, v5.validateV5Options("ks", &nowInSeconds))
	require.NoError(t, v5.validateV5Options("", &minInt32))
	require.NoError(t, v5.validateV5Options("", &maxInt32))

	// And neither option set is required: v4 requests must still pass.
	require.NoError(t, newFramer(nil, protoVersion4).validateV5Options("", nil))
}

func Test_framer_writeBatchFrame_rejectsUnsupportedOptionsOnV4(t *testing.T) {
	nowInSeconds := 123
	overflow := math.MaxInt32 + 1

	namedValues := []queryValues{{name: "id", value: []byte{1}}}

	// wantErr pins which validation rejected the frame, so a case cannot pass by
	// tripping some unrelated error.
	cases := []struct {
		name    string
		proto   byte
		frame   writeBatchFrame
		wantErr string
	}{
		{
			name:    "keyspace on v4",
			proto:   protoVersion4,
			frame:   writeBatchFrame{keyspace: "ks"},
			wantErr: "keyspace override can only be set with protocol v5 or higher",
		},
		{
			name:    "nowInSeconds on v4",
			proto:   protoVersion4,
			frame:   writeBatchFrame{nowInSeconds: &nowInSeconds},
			wantErr: "now_in_seconds can only be set with protocol v5 or higher",
		},
		{
			name:    "nowInSeconds overflow on v5",
			proto:   protoVersion5,
			frame:   writeBatchFrame{nowInSeconds: &overflow},
			wantErr: "overflows int32",
		},
		// Named values are rejected on every protocol version (CASSANDRA-10246),
		// for both raw-statement and prepared-id batch entries.
		{
			name:  "named values on v4",
			proto: protoVersion4,
			frame: writeBatchFrame{
				statements: []batchStatment{{statement: "INSERT INTO t (id) VALUES (?)", values: namedValues}},
			},
			wantErr: "named query values are not supported in batches",
		},
		{
			name:  "named values on v5",
			proto: protoVersion5,
			frame: writeBatchFrame{
				statements: []batchStatment{{statement: "INSERT INTO t (id) VALUES (?)", values: namedValues}},
			},
			wantErr: "named query values are not supported in batches",
		},
		{
			name:  "named values on a prepared statement",
			proto: protoVersion5,
			frame: writeBatchFrame{
				statements: []batchStatment{{preparedID: []byte{0xAA, 0xBB}, values: namedValues}},
			},
			wantErr: "named query values are not supported in batches",
		},
		{
			// The rejection must scan every statement, not just the first.
			name:  "named values in a later statement",
			proto: protoVersion5,
			frame: writeBatchFrame{
				statements: []batchStatment{
					{statement: "INSERT INTO t (id) VALUES (?)", values: []queryValues{{value: []byte{1}}}},
					{statement: "INSERT INTO u (id) VALUES (?)", values: namedValues},
				},
			},
			wantErr: "named query values are not supported in batches",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			framer := newFramer(nil, tc.proto)
			err := framer.writeBatchFrame(1, &tc.frame, tc.frame.customPayload)
			if err == nil {
				t.Fatal("expected an error, got nil")
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("error %q does not contain %q", err, tc.wantErr)
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
		{"v5 no compressor", nil, protoVersion5, 0},
		// v5 compresses at the segment layer, so no frame-header FlagCompress.
		{"v5 with compressor", comp, protoVersion5, 0},
		// A direction/reserved high bit on the version must not defeat the v5
		// check and re-enable FlagCompress at v5, nor suppress it at v4.
		{"v5|dir with compressor", comp, protoVersion5 | protoDirectionMask, 0},
		{"v4|dir with compressor", comp, protoVersion4 | protoDirectionMask, frm.FlagCompress},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := defaultFramerFlags(tc.compressor, tc.version); got != tc.want {
				t.Fatalf("defaultFramerFlags(%v, v%d) = 0x%02x, want 0x%02x", tc.compressor != nil, tc.version, got, tc.want)
			}
		})
	}
}

// No framer may ever set FlagBetaProtocol: opting into a server's in-development
// v5 dialect makes a Cassandra 3.11 handshake succeed and every following frame
// fail, instead of v5 being rejected cleanly (see protoVersion5, CASSGO-88).
// Covered here for every way a request framer is built.
func Test_framerFlags_neverBetaProtocol(t *testing.T) {
	comp := testMockedCompressor{}

	for _, version := range []byte{protoVersion4, protoVersion5, protoVersion5 | protoDirectionMask} {
		for _, compressor := range []Compressor{nil, comp} {
			if got := defaultFramerFlags(compressor, version); got&frm.FlagBetaProtocol != 0 {
				t.Errorf("defaultFramerFlags(%v, 0x%02x) set FlagBetaProtocol", compressor != nil, version)
			}
			if got := newFramer(compressor, version).flags; got&frm.FlagBetaProtocol != 0 {
				t.Errorf("newFramer(%v, 0x%02x) set FlagBetaProtocol", compressor != nil, version)
			}
		}
	}

	// initCache is what the connection actually uses once the handshake completed.
	c := &Conn{version: protoVersion5, compressor: comp, logger: &defaultLogger{}}
	c.initFramerCache()
	if c.framers.defaults.flags&frm.FlagBetaProtocol != 0 {
		t.Error("initFramerCache(v5) seeded FlagBetaProtocol")
	}
}

// newFramer must not set FlagCompress on v5, where compression happens at the
// segment layer instead of via a frame-header flag.
func Test_newFramer_compressFlag(t *testing.T) {
	if flags := newFramer(testMockedCompressor{}, protoVersion5).flags; flags&frm.FlagCompress != 0 {
		t.Error("newFramer(v5) should not set FlagCompress (v5 compresses at the segment layer)")
	}
	if flags := newFramer(testMockedCompressor{}, protoVersion4).flags; flags&frm.FlagCompress == 0 {
		t.Error("newFramer(v4) should set FlagCompress")
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

// AppendCompressed is a no-op "compressor" that still honours the
// SegmentCompressor contract of appending to dst and returning the extended
// slice. Returning src alone would let the mock pass while a caller that encodes
// segments directly into dst (framer.prepareModernLayout) breaks.
func (m testMockedCompressor) AppendCompressed(dst, src []byte) ([]byte, error) {
	if m.expectedError != nil {
		return nil, m.expectedError
	}
	return append(dst, src...), nil
}

func (m testMockedCompressor) AppendDecompressed(dst, src []byte, decompressedLength uint32) ([]byte, error) {
	if m.expectedError != nil {
		return nil, m.expectedError
	}

	// simulating invalid size of decoded data
	if m.invalidateDecodedDataLength {
		return append(dst, src[:decompressedLength-1]...), nil
	}

	return append(dst, src...), nil
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

	// A payload spanning more than one segment.MaxPayloadSize chunk forces the
	// chunk loop to run, so failing on the second AppendCompressed call fails
	// after the first chunk has already been appended to the local buffer.
	original := bytes.Repeat([]byte{0xAB}, segment.MaxPayloadSize+100)

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

	for _, size := range []int{1, segment.MaxPayloadSize - 1, segment.MaxPayloadSize, segment.MaxPayloadSize + 1, 2*segment.MaxPayloadSize + 7} {
		original := bytes.Repeat([]byte{0x5A}, size)

		// Reference output computed directly from the segment helpers.
		var want []byte
		src := original
		selfContained := true
		for len(src) > segment.MaxPayloadSize {
			seg, err := newUncompressedSegment(src[:segment.MaxPayloadSize], false)
			if err != nil {
				t.Fatalf("size %d: reference segment: %v", size, err)
			}
			want = append(want, seg...)
			src = src[segment.MaxPayloadSize:]
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

// expandingCompressor is an incompressible-data compressor that demands capacity
// exactly the way pierrec/lz4 does: its output may exceed its input by the lz4
// block bound (len + len/255 + 16), and it grows dst — reallocating and copying
// everything accumulated so far — whenever dst has less spare capacity than that
// bound. It is what makes an undersized wire buffer observable as an allocation.
type expandingCompressor struct{}

func (expandingCompressor) Name() string { return "expanding" }

func (expandingCompressor) AppendCompressed(dst, src []byte) ([]byte, error) {
	bound := len(src) + len(src)/255 + 16
	oldLen := len(dst)
	if cap(dst)-oldLen < bound {
		grown := make([]byte, oldLen+bound)
		copy(grown, dst)
		dst = grown
	}
	// Expand slightly, so every segment takes segment.AppendCompressed's
	// "compression was not worth it" fallback back to the raw payload.
	out := dst[:oldLen+len(src)+len(src)/255+1]
	copy(out[oldLen:], src)
	return out, nil
}

func (expandingCompressor) AppendDecompressed(dst, src []byte, _ uint32) ([]byte, error) {
	return append(dst, src...), nil
}

func (expandingCompressor) Encode(data []byte) ([]byte, error) { return data, nil }
func (expandingCompressor) Decode(data []byte) ([]byte, error) { return data, nil }

// TestPrepareModernLayoutReusesBuffers pins that a framer reused for consecutive
// v5 requests segments them without allocating: the wire buffer must be kept on
// the framer and swapped with the raw-frame buffer, not built from scratch (which
// allocated the whole wire output, plus a temporary per segment, per request).
//
// The expanding cases additionally cover a multi-segment compressed frame whose
// every payload grows under compression, which is what segment.EncodedSize's
// one-segment slack is for. Expansion does not accumulate across segments: only
// one segment is ever mid-compression, and a segment whose compressed form came
// out larger is rewritten as its raw payload before the next one starts. Note
// that a short estimate would only cost one extra grow rather than a per-request
// allocation, since both buffers converge on the capacity actually needed — this
// asserts the steady state, not the constant.
func TestPrepareModernLayoutReusesBuffers(t *testing.T) {
	for _, tc := range []struct {
		name       string
		compressor Compressor
		size       int
	}{
		{"single segment", nil, 4096},
		{"multi segment", nil, 2*segment.MaxPayloadSize + 7},
		{"compressed", testMockedCompressor{}, 4096},
		{"expanding compressed, single segment", expandingCompressor{}, segment.MaxPayloadSize - 1},
		{"expanding compressed, multi segment", expandingCompressor{}, 5*segment.MaxPayloadSize - 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			payload := bytes.Repeat([]byte{0x5A}, tc.size)
			f := newFramer(tc.compressor, protoVersion5)

			segment := func() {
				f.buf = append(f.buf[:0], payload...)
				if err := f.prepareModernLayout(); err != nil {
					t.Fatalf("prepareModernLayout: %v", err)
				}
			}

			// The first calls legitimately allocate: both buffers still have to
			// grow to the size of this frame.
			for i := 0; i < 5; i++ {
				segment()
			}

			if allocs := testing.AllocsPerRun(20, segment); allocs != 0 {
				t.Errorf("segmenting a warmed-up framer allocated %v times per request, want 0", allocs)
			}
		})
	}
}

// --- queryValues pool tests ---

func TestQueryValuesBucket(t *testing.T) {
	tests := []struct {
		n      int
		bucket int
	}{
		{0, 0}, {1, 0}, {8, 0},
		{9, 1}, {16, 1},
		{17, 2}, {32, 2},
		{33, 3}, {64, 3},
		{65, 4}, {128, 4},
		{129, -1}, {1000, -1},
	}
	for _, tt := range tests {
		got := queryValuesBucket(tt.n)
		if got != tt.bucket {
			t.Errorf("queryValuesBucket(%d) = %d, want %d", tt.n, got, tt.bucket)
		}
	}
}

func TestGetQueryValuesLength(t *testing.T) {
	for _, n := range []int{0, 1, 5, 8, 10, 16, 30, 64, 128, 200} {
		s := getQueryValues(n)
		if len(s) != n {
			t.Errorf("getQueryValues(%d): len = %d, want %d", n, len(s), n)
		}
		// For pooled sizes, capacity should be the bucket size.
		bucket := queryValuesBucket(n)
		if bucket >= 0 {
			wantCap := 8 << bucket
			if cap(s) != wantCap {
				t.Errorf("getQueryValues(%d): cap = %d, want %d", n, cap(s), wantCap)
			}
		}
	}
}

func TestPutGetQueryValuesClearsReferences(t *testing.T) {
	s := getQueryValues(4)
	full := s[:cap(s)] // cap(s) == 8; verify the whole backing array, not just len(s)
	full[0].name = "col1"
	full[0].value = []byte("data")
	full[1].name = "col2"
	full[1].value = []byte("more")
	full[2].isUnset = true
	full[len(full)-1].name = "tail"
	full[len(full)-1].value = []byte("tail-data")
	full[len(full)-1].isUnset = true

	putQueryValues(s)

	// Assert directly on the original backing array: putQueryValues clears it
	// in place, so full still reflects the zeroed state. This is deterministic
	// and does not depend on sync.Pool returning the same object on the next Get.
	for i := range full {
		if full[i].name != "" {
			t.Errorf("element %d: name = %q, want empty after put", i, full[i].name)
		}
		if full[i].value != nil {
			t.Errorf("element %d: value = %v, want nil after put", i, full[i].value)
		}
		if full[i].isUnset {
			t.Errorf("element %d: isUnset = true, want false after put", i)
		}
	}
}

func TestPutQueryValuesNilSafe(t *testing.T) {
	// Must not panic.
	putQueryValues(nil)
}

func TestPutBatchQueryValues(t *testing.T) {
	stmts := make([]batchStatment, 3)
	stmts[0].values = getQueryValues(5)
	stmts[0].values[0].name = "a"
	// stmts[1].values is nil (simulates entry without args)
	stmts[2].values = getQueryValues(10)
	stmts[2].values[0].value = []byte("x")

	putBatchQueryValues(stmts)

	for i, s := range stmts {
		if s.values != nil {
			t.Errorf("stmts[%d].values should be nil after putBatchQueryValues", i)
		}
	}
}

func TestGetQueryValuesOversize(t *testing.T) {
	// Slices larger than 128 should not be pooled.
	s := getQueryValues(200)
	if len(s) != 200 {
		t.Errorf("getQueryValues(200): len = %d, want 200", len(s))
	}
	// Should not panic when returning oversize.
	putQueryValues(s)
}
