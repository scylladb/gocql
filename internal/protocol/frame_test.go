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

package protocol

import (
	. "github.com/scylladb/gocql/v2/internal/cqltype"
	cqltype "github.com/scylladb/gocql/v2/internal/cqltype"
	"github.com/scylladb/gocql/v2/internal/wirecompress"

	"bytes"
	"errors"
	"fmt"
	"io"
	"math"
	"math/rand"
	"net"
	"os"
	"runtime"
	"strings"
	"testing"

	lz4mod "github.com/scylladb/gocql/lz4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	frm "github.com/scylladb/gocql/v2/internal/frame"
	"github.com/scylladb/gocql/v2/internal/segment"
	"github.com/scylladb/gocql/v2/internal/tests"
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
		err = framer.ReadFrame(r, &head)
		if err != nil {
			continue
		}

		frame, err := framer.ParseFrame()
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

	framer.WriteHeader(0, frm.OpStartup, 1)
	framer.WriteBytes(make([]byte, frm.MaxFrameSize+1))
	err := framer.Finish()
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

	err := framer.ReadFrame(r, &head)
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

			err := f.ReadFrame(tc.r, &head)
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

	err := f.ReadFrame(errReader{timeoutErr{}}, &head)
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
			fr.Header = &frm.FrameHeader{Version: frm.ProtoVersion(tc.proto)}
			fr.ScyllaUseMetadataID = tc.extension

			fr.WriteInt(int32(frm.FlagGlobalTableSpec | frm.FlagHasMorePages | frm.FlagMetaDataChanged))
			fr.WriteInt(1) // colCount
			fr.WriteBytes(pagingState)
			fr.WriteShortBytes(newMetadataID)
			fr.WriteString(keyspace)
			fr.WriteString(table)
			fr.WriteString("col_a")
			fr.WriteShort(uint16(TypeInt))

			meta := fr.ParseResultMetadata()

			assertDeepEqual(t, "pagingState", pagingState, meta.PagingState)
			assertDeepEqual(t, "newMetadataID", newMetadataID, meta.NewMetadataID)

			// The column spec must still be readable, which is what actually proves the
			// two optional fields were consumed in the right order and with the right
			// wire types.
			require.Len(t, meta.Columns, 1)
			require.Equal(t, keyspace, meta.Columns[0].Keyspace)
			require.Equal(t, table, meta.Columns[0].Table)
			require.Equal(t, "col_a", meta.Columns[0].Name)
			require.Empty(t, fr.Buf, "whole metadata block should be consumed")
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
	fr.Header = &frm.FrameHeader{Version: protoVersion4}

	// METADATA_CHANGED set but no id on the wire, as a v4 server would send.
	fr.WriteInt(int32(frm.FlagGlobalTableSpec | frm.FlagMetaDataChanged))
	fr.WriteInt(1)
	fr.WriteString("test_ks")
	fr.WriteString("test_tbl")
	fr.WriteString("col_a")
	fr.WriteShort(uint16(TypeInt))

	meta := fr.ParseResultMetadata()

	require.Nil(t, meta.NewMetadataID, "no metadata id should be read below v5")
	require.Len(t, meta.Columns, 1)
	require.Equal(t, "col_a", meta.Columns[0].Name)
	require.Empty(t, fr.Buf, "whole metadata block should be consumed")
}

func TestParseResultMetadata_PerColumnSpec(t *testing.T) {
	t.Parallel()

	// Build a synthetic ROWS result metadata frame with FlagGlobalTableSpec unset
	// (per-column keyspace/table encoding). This tests the !globalSpec optimization
	// in parseResultMetadata() which reads keyspace/table from the first column
	// position and reuses them for all columns via skipString().
	fr := newFramer(nil, protoVersion4)
	fr.Header = &frm.FrameHeader{Version: protoVersion4}

	// flags: no FlagGlobalTableSpec — per-column keyspace/table
	fr.WriteInt(0)
	// colCount
	fr.WriteInt(3)

	// Column 0: keyspace/table + name + type
	fr.WriteString("test_ks")
	fr.WriteString("test_tbl")
	fr.WriteString("col_a")
	fr.WriteShort(uint16(TypeInt))

	// Column 1: same keyspace/table (will be skipped by optimization)
	fr.WriteString("test_ks")
	fr.WriteString("test_tbl")
	fr.WriteString("col_b")
	fr.WriteShort(uint16(TypeVarchar))

	// Column 2: same keyspace/table
	fr.WriteString("test_ks")
	fr.WriteString("test_tbl")
	fr.WriteString("col_c")
	fr.WriteShort(uint16(TypeBoolean))

	meta := fr.ParseResultMetadata()

	if meta.ColCount != 3 {
		t.Fatalf("colCount = %d, want 3", meta.ColCount)
	}
	if len(meta.Columns) != 3 {
		t.Fatalf("len(columns) = %d, want 3", len(meta.Columns))
	}

	// Verify all columns got the correct keyspace/table from the optimization
	for i, col := range meta.Columns {
		if col.Keyspace != "test_ks" {
			t.Errorf("columns[%d].Keyspace = %q, want %q", i, col.Keyspace, "test_ks")
		}
		if col.Table != "test_tbl" {
			t.Errorf("columns[%d].Table = %q, want %q", i, col.Table, "test_tbl")
		}
	}

	// Verify column names
	expectedNames := []string{"col_a", "col_b", "col_c"}
	for i, col := range meta.Columns {
		if col.Name != expectedNames[i] {
			t.Errorf("columns[%d].Name = %q, want %q", i, col.Name, expectedNames[i])
		}
	}

	// Verify column types
	expectedTypes := []Type{TypeInt, TypeVarchar, TypeBoolean}
	for i, col := range meta.Columns {
		nt, ok := col.TypeInfo.(NativeType)
		if !ok {
			t.Fatalf("columns[%d].TypeInfo is %T, want NativeType", i, col.TypeInfo)
		}
		if cqltype.NativeKind(nt) != expectedTypes[i] {
			t.Errorf("columns[%d].Type = %v, want %v", i, cqltype.NativeKind(nt), expectedTypes[i])
		}
	}

	// Verify the entire buffer was consumed (no misalignment from skipString)
	if len(fr.Buf) != 0 {
		t.Errorf("buffer has %d unconsumed bytes, want 0 (possible skipString misalignment)", len(fr.Buf))
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
			fr.Header = &frm.FrameHeader{Version: protoVersion4 | protoDirectionMask, Op: frm.OpResult}

			fr.WriteInt(frm.ResultKindPrepared)
			fr.WriteShortBytes([]byte{0x01, 0x02, 0x03}) // preparedID
			// prepared metadata: flags=0, colCount=0, then the bogus pkeyCount.
			fr.WriteInt(0)
			fr.WriteInt(0)
			fr.WriteInt(tc.pkeyCount)

			frame, err := fr.ParseFrame()
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
		fr.Header = &frm.FrameHeader{Version: protoVersion4 | protoDirectionMask}

		// NO_METADATA so parsing stops right after the pkeys, leaving the buffer
		// holding exactly 2*pkeyCount bytes at the guard: pkeyCount == len(buf)/2.
		fr.WriteInt(int32(frm.FlagNoMetaData))
		fr.WriteInt(0) // colCount
		fr.WriteInt(2) // pkeyCount
		fr.WriteShort(0)
		fr.WriteShort(1)

		// NotPanics, because parsePreparedMetadata is called directly here and so
		// the guard's panic would otherwise take down the whole test binary rather
		// than failing this subtest.
		var meta preparedMetadata
		require.NotPanics(t, func() { meta = fr.ParsePreparedMetadata() })

		require.Equal(t, []int{0, 1}, meta.PkeyColumns)
		require.Empty(t, fr.Buf, "whole metadata block should be consumed")
	})

	t.Run("full prepared frame", func(t *testing.T) {
		t.Parallel()

		fr := newFramer(nil, protoVersion4)
		fr.Header = &frm.FrameHeader{Version: protoVersion4 | protoDirectionMask, Op: frm.OpResult}

		fr.WriteInt(frm.ResultKindPrepared)
		fr.WriteShortBytes([]byte{0x01, 0x02, 0x03}) // preparedID
		// prepared metadata
		fr.WriteInt(int32(frm.FlagGlobalTableSpec))
		fr.WriteInt(2) // colCount
		fr.WriteInt(2) // pkeyCount
		fr.WriteShort(0)
		fr.WriteShort(1)
		fr.WriteString("test_ks")
		fr.WriteString("test_tbl")
		fr.WriteString("pk_a")
		fr.WriteShort(uint16(TypeInt))
		fr.WriteString("pk_b")
		fr.WriteShort(uint16(TypeVarchar))
		// result metadata
		fr.WriteInt(int32(frm.FlagNoMetaData))
		fr.WriteInt(0)

		frame, err := fr.ParseFrame()
		require.NoError(t, err)

		prepared, ok := frame.(*resultPreparedFrame)
		require.True(t, ok, "got %T", frame)
		require.Equal(t, []int{0, 1}, prepared.ReqMeta.PkeyColumns)
		require.Len(t, prepared.ReqMeta.Columns, 2)
		require.Empty(t, fr.Buf, "whole frame should be consumed")
	})
}

func TestReadLongStringRejectsNegativeLength(t *testing.T) {
	f := newFramer(nil, protoVersion4)
	f.Buf = []byte{0xFF, 0xFF, 0xFF, 0xFF, 'x'} // a declared length of -1

	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("a negative long string length was accepted")
		}
		mf, ok := r.(malformedFrameError)
		if !ok {
			t.Fatalf("panicked with %T, which parseFrame re-panics instead of converting: %v", r, r)
		}
		if !strings.Contains(mf.Error(), "-1") {
			t.Errorf("the error does not name the length: %v", mf)
		}
	}()

	f.ReadLongString()
}

// TestMalformedFrameErrorWraps pins the carrier's two jobs: it reads as the error it
// holds, and it gives that error back unchanged, so a caller of parseFrame never sees
// the wrapper.
func TestMalformedFrameErrorWraps(t *testing.T) {
	t.Parallel()

	inner := fmt.Errorf("short buffer: %d", 3)
	mf := malformedFrameError{Err: inner}

	assert.Equal(t, inner.Error(), mf.Error())
	assert.Same(t, inner, errors.Unwrap(mf))

	var err error
	recoverMalformedFrame(mf, &err)
	assert.Same(t, inner, err, "parseFrame must return the wrapped error, not the wrapper")

	assert.Equal(t, "short buffer: 3", malformedFramef("short buffer: %d", 3).Error())
}

// TestRecoverMalformedFrameRePanicsADriverBug pins that only the read path's own value
// becomes an error. Anything else -- a runtime.Error, a bare error from code that does
// not speak this contract, a panic("...") -- keeps unwinding, so a driver bug is never
// reported as a malformed frame from the peer.
func TestRecoverMalformedFrameRePanicsADriverBug(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		value any
	}{
		{name: "runtime error", value: capturedRuntimeError(t)},
		{name: "bare error", value: fmt.Errorf("not enough bytes in buffer")},
		{name: "string", value: "compress flag set with no compressor"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			var err error
			func() {
				defer func() {
					r := recover()
					if r == nil {
						t.Fatalf("%T was swallowed instead of re-panicked", test.value)
					}
					assert.Equal(t, test.value, r, "the panic value must survive unchanged")
				}()

				recoverMalformedFrame(test.value, &err)
			}()

			assert.NoError(t, err, "a re-panicked value must not also be reported as a frame error")
		})
	}
}

// capturedRuntimeError returns a real runtime.Error, which cannot be constructed
// directly.
func capturedRuntimeError(t *testing.T) runtime.Error {
	t.Helper()

	var caught runtime.Error
	func() {
		defer func() {
			var ok bool
			caught, ok = recover().(runtime.Error)
			require.True(t, ok, "expected an out-of-range index to raise a runtime.Error")
		}()

		empty := make([]int, 0)
		_ = empty[len(empty)]
	}()

	return caught
}

// TestRecoverMalformedFrameIgnoresANormalReturn pins the nil case: nothing panicked,
// so the error parseFrame is about to return stays as it is.
func TestRecoverMalformedFrameIgnoresANormalReturn(t *testing.T) {
	t.Parallel()

	err := error(nil)
	recoverMalformedFrame(nil, &err)
	assert.NoError(t, err)

	sentinel := fmt.Errorf("got a request frame from server")
	err = sentinel
	recoverMalformedFrame(nil, &err)
	assert.Same(t, sentinel, err)
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
	fr.Header = &frm.FrameHeader{Version: protoVersion4 | 0x80, Op: frm.OpResult}
	fr.ScyllaUseMetadataID = true

	fr.WriteInt(frm.ResultKindPrepared)
	fr.WriteShortBytes([]byte{0x01, 0x02, 0x03}) // preparedID
	// resultMetadataID: claim 10 bytes but supply none.
	fr.WriteShort(10)

	frame, err := fr.ParseFrame()
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
				framer.ScyllaUseMetadataID = true
			}

			nowInSeconds := 123
			// A non-zero consistency, so reading it back is an assertion that can
			// actually fail: with the zero value on both sides a misaligned read of
			// Consistency(ANY) would still compare equal.
			params := fixtureQueryParams{consistency: Quorum}
			if tt.protoVersion >= protoVersion5 {
				params.nowInSeconds = &nowInSeconds
				params.keyspace = "test_keyspace"
			}
			frame := fixtureExecuteFrame{
				preparedID:       []byte{1, 2, 3},
				resultMetadataID: tt.resultMetadataID,
				customPayload: map[string][]byte{
					"key1": []byte("value1"),
				},
				params: params,
			}

			err := fixtureWriteExecute(framer, 123, frame.preparedID, frame.resultMetadataID, &frame.params, &frame.customPayload)
			if err != nil {
				t.Fatal(err)
			}

			// skipping header
			framer.Buf = framer.Buf[9:]

			assertDeepEqual(t, "customPayload", frame.customPayload, framer.ReadBytesMap())
			assertDeepEqual(t, "preparedID", frame.preparedID, framer.ReadShortBytesCopy())

			if tt.protoVersion >= protoVersion5 || tt.scyllaUseMetadataID {
				assertDeepEqual(t, "resultMetadataID", tt.wantResultMetadataID, framer.ReadShortBytesCopy())
			}

			assertDeepEqual(t, "constistency", frame.params.consistency, Consistency(framer.ReadShort()))

			if tt.protoVersion >= protoVersion5 {
				flags := framer.ReadInt()
				if flags&int(frm.FlagWithNowInSeconds) != int(frm.FlagWithNowInSeconds) {
					t.Fatal("expected flagNowInSeconds to be set, but it is not")
				}

				if flags&int(frm.FlagWithKeyspace) != int(frm.FlagWithKeyspace) {
					t.Fatal("expected flagWithKeyspace to be set, but it is not")
				}
				assertDeepEqual(t, "keyspace", frame.params.keyspace, framer.ReadString())
				assertDeepEqual(t, "nowInSeconds", nowInSeconds, framer.ReadInt())
			} else {
				// Below v5 the query flags are a single byte, and nothing follows them
				// here. Consuming them and requiring the buffer to be empty turns this
				// into a check that every preceding field was read at exactly the length
				// it was written — in particular the resultMetadataID short bytes.
				require.Zero(t, framer.ReadByteValue(), "no query flags expected")
				require.Empty(t, framer.Buf, "whole EXECUTE body should be consumed")
			}
		})
	}
}

func Test_framer_writeBatchFrame(t *testing.T) {
	framer := newFramer(nil, protoVersion5)
	nowInSeconds := 123
	frame := fixtureBatchFrame{
		customPayload: map[string][]byte{
			"key1": []byte("value1"),
		},
		nowInSeconds: &nowInSeconds,
	}

	err := fixtureWriteBatch(framer, 123, &frame, frame.customPayload)
	if err != nil {
		t.Fatal(err)
	}

	// skipping header
	framer.Buf = framer.Buf[9:]

	assertDeepEqual(t, "customPayload", frame.customPayload, framer.ReadBytesMap())
	assertDeepEqual(t, "typ", frame.typ, BatchKind(framer.ReadByteValue()))
	assertDeepEqual(t, "len(statements)", len(frame.statements), int(framer.ReadShort()))
	assertDeepEqual(t, "consistency", frame.consistency, Consistency(framer.ReadShort()))

	flags := framer.ReadInt()
	if flags&int(frm.FlagWithNowInSeconds) != int(frm.FlagWithNowInSeconds) {
		t.Fatal("expected flagNowInSeconds to be set, but it is not")
	}

	assertDeepEqual(t, "nowInSeconds", nowInSeconds, framer.ReadInt())
}

// Test_framer_writeBatchFrame_unnamedValues guards the happy path through the
// statement/values write loop (unnamed positional values), which must still
// succeed after named-value rejection was hoisted out of that loop.
func Test_framer_writeBatchFrame_unnamedValues(t *testing.T) {
	framer := newFramer(nil, protoVersion5)
	frame := fixtureBatchFrame{
		typ:         LoggedBatch,
		consistency: Quorum,
		statements: []fixtureBatchStatement{
			{
				statement: "INSERT INTO t (id, v) VALUES (?, ?)",
				values: []fixtureQueryValue{
					{value: []byte{0, 0, 0, 1}},
					{value: []byte("x")},
				},
			},
		},
	}

	if err := fixtureWriteBatch(framer, 1, &frame, frame.customPayload); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// skipping header
	framer.Buf = framer.Buf[9:]
	assertDeepEqual(t, "typ", frame.typ, BatchKind(framer.ReadByteValue()))
	assertDeepEqual(t, "len(statements)", len(frame.statements), int(framer.ReadShort()))
	assertDeepEqual(t, "kind", byte(0), framer.ReadByteValue()) // 0 = raw query string
	assertDeepEqual(t, "statement", frame.statements[0].statement, framer.ReadLongString())
	assertDeepEqual(t, "len(values)", len(frame.statements[0].values), int(framer.ReadShort()))
	assertDeepEqual(t, "value0", frame.statements[0].values[0].value, framer.ReadBytesCopy())
	assertDeepEqual(t, "value1", frame.statements[0].values[1].value, framer.ReadBytesCopy())
}

func Test_framer_shortBytesWriters_rejectOverlongValues(t *testing.T) {
	tooLong := make([]byte, math.MaxUint16+1)
	ok := []byte{0x01, 0x02}

	for _, tc := range []struct {
		name  string
		proto byte
		build frameBuilder
	}{
		{
			name:  "EXECUTE prepared id",
			proto: protoVersion4,
			build: &fixtureExecuteFrame{preparedID: tooLong, params: fixtureQueryParams{consistency: Quorum}},
		},
		{
			name:  "EXECUTE result metadata id on v5",
			proto: protoVersion5,
			build: &fixtureExecuteFrame{preparedID: ok, resultMetadataID: tooLong, params: fixtureQueryParams{consistency: Quorum}},
		},
		{
			name:  "BATCH prepared id",
			proto: protoVersion4,
			build: &fixtureBatchFrame{
				consistency: Quorum,
				statements:  []fixtureBatchStatement{{preparedID: tooLong}},
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			framer := newFramer(nil, tc.proto)

			err := tc.build.BuildFrame(framer, 1)
			require.Error(t, err)
			require.Contains(t, err.Error(), "overflows the 2-byte length")
			require.Empty(t, framer.Buf, "a rejected value must not leave a partial frame in the framer buffer")
		})
	}
}

func Test_framer_queryParamsWriters_rejectUnsupportedOptionsOnV4(t *testing.T) {
	nowInSeconds := 123

	cases := []struct {
		name    string
		proto   byte
		opts    fixtureQueryParams
		wantErr string
	}{
		{
			name:    "keyspace on v4",
			proto:   protoVersion4,
			opts:    fixtureQueryParams{consistency: Quorum, keyspace: "ks"},
			wantErr: "keyspace override can only be set with protocol v5 or higher",
		},
		{
			name:    "nowInSeconds on v4",
			proto:   protoVersion4,
			opts:    fixtureQueryParams{consistency: Quorum, nowInSeconds: &nowInSeconds},
			wantErr: "now_in_seconds can only be set with protocol v5 or higher",
		},
	}

	writers := []struct {
		name  string
		build func(opts fixtureQueryParams) frameBuilder
	}{
		{
			name: "QUERY",
			build: func(opts fixtureQueryParams) frameBuilder {
				return &fixtureQueryFrame{statement: "SELECT * FROM system.local", params: opts}
			},
		},
		{
			name: "EXECUTE",
			build: func(opts fixtureQueryParams) frameBuilder {
				return &fixtureExecuteFrame{
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

					err := w.build(tc.opts).BuildFrame(framer, 1)
					require.Error(t, err)
					require.Contains(t, err.Error(), tc.wantErr)
					require.Empty(t, framer.Buf, "a rejected option must not leave a partial frame in the framer buffer")
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
	require.NoError(t, v5.ValidateV5Options("ks", &nowInSeconds))
	require.NoError(t, v5.ValidateV5Options("", &minInt32))
	require.NoError(t, v5.ValidateV5Options("", &maxInt32))

	// And neither option set is required: v4 requests must still pass.
	require.NoError(t, newFramer(nil, protoVersion4).ValidateV5Options("", nil))
}

func Test_framer_writeBatchFrame_rejectsUnsupportedOptionsOnV4(t *testing.T) {
	nowInSeconds := 123

	namedValues := []fixtureQueryValue{{name: "id", value: []byte{1}}}

	// wantErr pins which validation rejected the frame, so a case cannot pass by
	// tripping some unrelated error.
	cases := []struct {
		name    string
		proto   byte
		frame   fixtureBatchFrame
		wantErr string
	}{
		{
			name:    "keyspace on v4",
			proto:   protoVersion4,
			frame:   fixtureBatchFrame{keyspace: "ks"},
			wantErr: "keyspace override can only be set with protocol v5 or higher",
		},
		{
			name:    "nowInSeconds on v4",
			proto:   protoVersion4,
			frame:   fixtureBatchFrame{nowInSeconds: &nowInSeconds},
			wantErr: "now_in_seconds can only be set with protocol v5 or higher",
		},
		// Named values are rejected on every protocol version (CASSANDRA-10246),
		// for both raw-statement and prepared-id batch entries.
		{
			name:  "named values on v4",
			proto: protoVersion4,
			frame: fixtureBatchFrame{
				statements: []fixtureBatchStatement{{statement: "INSERT INTO t (id) VALUES (?)", values: namedValues}},
			},
			wantErr: "named query values are not supported in batches",
		},
		{
			name:  "named values on v5",
			proto: protoVersion5,
			frame: fixtureBatchFrame{
				statements: []fixtureBatchStatement{{statement: "INSERT INTO t (id) VALUES (?)", values: namedValues}},
			},
			wantErr: "named query values are not supported in batches",
		},
		{
			name:  "named values on a prepared statement",
			proto: protoVersion5,
			frame: fixtureBatchFrame{
				statements: []fixtureBatchStatement{{preparedID: []byte{0xAA, 0xBB}, values: namedValues}},
			},
			wantErr: "named query values are not supported in batches",
		},
		{
			// The rejection must scan every statement, not just the first.
			name:  "named values in a later statement",
			proto: protoVersion5,
			frame: fixtureBatchFrame{
				statements: []fixtureBatchStatement{
					{statement: "INSERT INTO t (id) VALUES (?)", values: []fixtureQueryValue{{value: []byte{1}}}},
					{statement: "INSERT INTO u (id) VALUES (?)", values: namedValues},
				},
			},
			wantErr: "named query values are not supported in batches",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			framer := newFramer(nil, tc.proto)
			err := fixtureWriteBatch(framer, 1, &tc.frame, tc.frame.customPayload)
			if err == nil {
				t.Fatal("expected an error, got nil")
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("error %q does not contain %q", err, tc.wantErr)
			}
			if len(framer.Buf) != 0 {
				t.Fatalf("expected framer buffer to be untouched on error, got %d bytes", len(framer.Buf))
			}
		})
	}
}

func Test_framer_writePrepareFrame_rejectsKeyspaceOnV4(t *testing.T) {
	framer := newFramer(nil, protoVersion4)
	prep := &writePrepareFrame{Statement: "SELECT * FROM t", Keyspace: "ks"}

	// Must return an error, not panic.
	if err := prep.BuildFrame(framer, 1); err == nil {
		t.Fatal("expected an error, got nil")
	}
	if len(framer.Buf) != 0 {
		t.Fatalf("expected framer buffer to be untouched on error, got %d bytes", len(framer.Buf))
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

func Test_newFramer_compressFlag(t *testing.T) {
	if flags := newFramer(testMockedCompressor{}, protoVersion5).Flags; flags&frm.FlagCompress != 0 {
		t.Error("newFramer(v5) should not set FlagCompress (v5 compresses at the segment layer)")
	}
	if flags := newFramer(testMockedCompressor{}, protoVersion4).Flags; flags&frm.FlagCompress == 0 {
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
			req := fixtureQueryFrame{
				statement: "SELECT * FROM system.local",
				params: fixtureQueryParams{
					consistency: Quorum,
					keyspace:    "gocql_test",
				},
			}

			err := req.BuildFrame(framer, 128)
			require.NoError(t, err)

			frame, err := newUncompressedSegment(framer.Buf, true)
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
				assert.Equal(t, framer.Buf, readFrame)
			}
		})
	}
}

// compressedReadFixture uses a real, smaller LZ4 block so raw fallback cannot
// bypass the existing injected decoder-error/length checks.
type compressedReadFixture struct{ testMockedCompressor }

func (m compressedReadFixture) AppendDecompressed(dst, src []byte, size uint32) ([]byte, error) {
	if m.expectedError != nil {
		return nil, m.expectedError
	}
	out, err := (lz4mod.LZ4Compressor{}).AppendDecompressed(dst, src, size)
	if err == nil && m.invalidateDecodedDataLength && len(out) > 0 {
		out = out[:len(out)-1]
	}
	return out, err
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
			req := fixtureQueryFrame{
				statement: strings.Repeat("SELECT * FROM system.local ", 32),
				params: fixtureQueryParams{
					consistency: Quorum,
					keyspace:    "gocql_test",
				},
			}

			err := req.BuildFrame(framer, 128)
			require.NoError(t, err)

			frame, err := newCompressedSegment(framer.Buf, true, lz4mod.LZ4Compressor{})
			require.NoError(t, err)

			if tt.modifyFrameFn != nil {
				frame = tt.modifyFrameFn(frame)
			}

			readFrame, selfContained, err := readCompressedSegment(bytes.NewReader(frame), compressedReadFixture{tt.compressor})

			switch {
			case tt.expectedErrorMsg != "":
				require.Error(t, err)
				require.Contains(t, err.Error(), tt.expectedErrorMsg)
			case tt.compressor.expectedError != nil:
				require.ErrorIs(t, err, tt.compressor.expectedError)
			default:
				require.NoError(t, err)
				assert.True(t, selfContained)
				assert.Equal(t, framer.Buf, readFrame)
			}
		})
	}
}

func TestParseEventFrame_ClientRoutesChanged(t *testing.T) {
	t.Parallel()

	fr := newFramer(nil, protoVersion4)
	fr.Header = &frm.FrameHeader{Version: protoVersion4}
	fr.WriteString("CLIENT_ROUTES_CHANGE")
	fr.WriteString("UPDATED")
	fr.WriteStringList([]string{"c1", ""})
	fr.WriteStringList([]string{})

	frame := fr.ParseEventFrame()
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
	f.Buf = append([]byte(nil), original...)

	err := f.PrepareModernLayout()
	if err == nil {
		t.Fatal("expected prepareModernLayout to fail")
	}
	if !bytes.Equal(f.Buf, original) {
		t.Fatalf("f.buf was mutated on error: len=%d, want len=%d", len(f.Buf), len(original))
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
	f.Buf = append([]byte(nil), []byte("some frame bytes")...)

	require.NotPanics(t, func() {
		err := f.PrepareModernLayout()
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
		f.Buf = append([]byte(nil), original...)
		if err := f.PrepareModernLayout(); err != nil {
			t.Fatalf("size %d: prepareModernLayout: %v", size, err)
		}
		if !bytes.Equal(f.Buf, want) {
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
				f.Buf = append(f.Buf[:0], payload...)
				if err := f.PrepareModernLayout(); err != nil {
					t.Fatalf("prepareModernLayout: %v", err)
				}
			}

			// The first calls legitimately allocate: both buffers still have to
			// grow to the size of this frame.
			for i := 0; i < 5; i++ {
				segment()
			}

			if allocs := tests.MinAllocsPerRun(3, 20, segment); allocs != 0 {
				t.Errorf("segmenting a warmed-up framer allocated %v times per request, want 0", allocs)
			}
		})
	}
}

// writeRowsResultWithColumnType builds a RESULT/Rows body carrying a single
// column of the custom type customType, so that parseFrame drives readTypeInfo
// over a server-supplied type string.
func writeRowsResultWithColumnType(fr *framer, customType string) {
	fr.WriteInt(frm.ResultKindRows)
	fr.WriteInt(int32(frm.FlagGlobalTableSpec))
	fr.WriteInt(1) // colCount
	fr.WriteString("gocql_test")
	fr.WriteString("vectors")
	fr.WriteString("vec")
	fr.WriteShort(uint16(TypeCustom))
	fr.WriteString(customType)
	fr.WriteInt(0) // numRows
}

// TestParseResultRowsRejectsMalformedVectorType pins that a malformed VectorType
// column is an error rather than a dead serve goroutine. The well-formed cases
// show the guard did not narrow the grammar, and "VectorTypeXxx" that a name
// merely sharing the prefix degrades to an opaque custom type.
func TestParseResultRowsRejectsMalformedVectorType(t *testing.T) {
	t.Parallel()

	const p = apacheCassandraTypePrefix
	const vp = p + "VectorType"

	tests := []struct {
		name    string
		custom  string
		wantErr bool
		want    TypeInfo
	}{
		{name: "no argument list", custom: vp, wantErr: true},
		{name: "unclosed argument list", custom: vp + "(", wantErr: true},
		{name: "empty argument list", custom: vp + "()", wantErr: true},
		{name: "no dimensions", custom: vp + "(" + p + "FloatType)", wantErr: true},
		{name: "empty subtype", custom: vp + "(, 3)", wantErr: true},
		{name: "unterminated", custom: vp + "(" + p + "FloatType, 3", wantErr: true},
		{name: "dimensions not a number", custom: vp + "(" + p + "FloatType, abc)", wantErr: true},
		{name: "negative dimensions", custom: vp + "(" + p + "FloatType, -3)", wantErr: true},
		{name: "zero dimensions", custom: vp + "(" + p + "FloatType, 0)", wantErr: true},
		{
			name:   "not a vector type",
			custom: vp + "Xxx(" + p + "FloatType, 3)",
			want:   NewCustomType(protoVersion4, TypeCustom, vp+"Xxx("+p+"FloatType, 3)"),
		},
		{
			name:   "float vector",
			custom: vp + "(" + p + "FloatType, 3)",
			want: VectorType{
				NativeType: NewCustomType(protoVersion4, TypeCustom, vp+"("+p+"FloatType, 3)"),
				SubType:    NewNativeType(protoVersion4, TypeFloat),
				Dimensions: 3,
			},
		},
		{
			name:   "nested vector",
			custom: vp + "(" + vp + "(" + p + "InetAddressType, 2), 3)",
			want: VectorType{
				NativeType: NewCustomType(protoVersion4, TypeCustom, vp+"("+vp+"("+p+"InetAddressType, 2), 3)"),
				SubType: VectorType{
					NativeType: NewCustomType(protoVersion4, TypeCustom, vp),
					SubType:    NewNativeType(protoVersion4, TypeInet),
					Dimensions: 2,
				},
				Dimensions: 3,
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			fr := newFramer(nil, protoVersion4)
			// Response direction bit set so parseFrame does not reject it as a request.
			fr.Header = &frm.FrameHeader{Version: protoVersion4 | 0x80, Op: frm.OpResult}
			writeRowsResultWithColumnType(fr, test.custom)

			frame, err := fr.ParseFrame()
			if test.wantErr {
				if err == nil {
					t.Fatalf("expected an error for %q, got frame %+v", test.custom, frame)
				}
				if frame != nil {
					t.Errorf("expected nil frame on error, got %+v", frame)
				}
				return
			}

			require.NoError(t, err)
			rows, ok := frame.(*resultRowsFrame)
			require.True(t, ok, "expected a resultRowsFrame, got %T", frame)
			require.Len(t, rows.Meta.Columns, 1)
			require.Equal(t, test.want, rows.Meta.Columns[0].TypeInfo)
		})
	}
}

// Test_framer_v4SnappyRoundTrip drives a real body through the protocol v4 compression
// path and back: framer.finish() compresses the body and rewrites the header length,
// framer.readFrame() reads that length and decompresses it again.
//
// This is the default configuration -- discoverProtocol caps at protoVersion4, so a
// newClusterSettings() user who sets a Compressor lands here and never on v5 segments -- and
// until now nothing covered it. Test_defaultFramerFlags and Test_newFramer_compressFlag
// check only that FlagCompress is set, and Test_readCompressedFrame is about v5 segments
// with a no-op compressor that never compresses a byte. A snappy encode/decode
// asymmetry, or a length field describing the wrong side of the compression, would have
// gone unnoticed by all three.
//
// The framer is driven directly rather than through a query builder: the subject is the
// compression layer, and a fixtureQueryParams route would drag in v5-only option validation.
func Test_framer_v4SnappyRoundTrip(t *testing.T) {
	t.Parallel()

	incompressible := make([]byte, 64*1024)
	// Fixed seed: an incompressible payload has to be the same bytes on every run, or a
	// failure here would not reproduce.
	rnd := rand.New(rand.NewSource(1))
	rnd.Read(incompressible)

	tests := []struct {
		name    string
		payload []byte
		// shrinks records which side of the "not worth it" line the payload falls on.
		// Both are legal on the wire; v4 compresses unconditionally either way.
		shrinks bool
	}{
		{"compressible", bytes.Repeat([]byte("a"), 256*1024), true},
		{"incompressible", incompressible, false},
		// The frame.go "only compress frames which are big enough" TODO: snappy expands
		// a body this short. Pinned here so the current behaviour is deliberate.
		{"tiny", []byte("SELECT * FROM system.local"), false},
		{"empty", []byte{}, false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			w := newFramer(wirecompress.SnappyCompressor{}, protoVersion4)
			require.NotZero(t, w.Flags&frm.FlagCompress, "v4 framer with a compressor must set FlagCompress")

			w.WriteHeader(w.Flags, frm.OpQuery, 42)
			w.Buf = append(w.Buf, test.payload...)
			require.NoError(t, w.Finish())

			wire := append([]byte(nil), w.Buf...)
			require.NotZero(t, wire[1]&frm.FlagCompress, "the compress flag must reach the wire")

			// The body must actually have been transformed, otherwise a compressor that
			// silently returned its input would pass the round-trip below.
			if len(test.payload) > 0 {
				require.NotEqual(t, test.payload, wire[headSize:], "body was not compressed")
			}
			if test.shrinks {
				require.Less(t, len(wire)-headSize, len(test.payload), "compressible payload should shrink")
			} else {
				// Snappy's block format has no stored-raw mode, so literal-only output
				// always exceeds its input by the varint header plus tag bytes. This is
				// the growth the frame.go "only compress frames which are big enough"
				// TODO is about, and pinning it is why the short payloads are here.
				require.Greater(t, len(wire)-headSize, len(test.payload), "incompressible payload should grow")
			}

			r := bytes.NewReader(wire)
			head, err := readHeader(r, make([]byte, headSize))
			require.NoError(t, err)
			// setLength must describe the compressed body, not the original: a reader
			// that trusted the uncompressed length would desync the connection.
			require.Equal(t, len(wire)-headSize, head.Length)
			require.NotZero(t, head.Flags&frm.FlagCompress)

			rf := newFramer(wirecompress.SnappyCompressor{}, protoVersion4)
			require.NoError(t, rf.ReadFrame(r, &head))
			require.Zero(t, r.Len(), "readFrame must consume exactly the declared body")
			// bytes.Equal rather than require.Equal: an empty body decodes to a nil
			// slice, which is the same bytes but not the same value.
			require.True(t, bytes.Equal(test.payload, rf.Buf), "body did not survive the round trip")
		})
	}
}

// Test_framer_v4CompressedFrameNeedsACompressor covers the other half of the v4 read
// path: a peer that sets FlagCompress on a connection the driver believes is
// uncompressed. The body is unreadable, so this has to be an error rather than a body
// parsed as plaintext.
func Test_framer_v4CompressedFrameNeedsACompressor(t *testing.T) {
	t.Parallel()

	w := newFramer(wirecompress.SnappyCompressor{}, protoVersion4)
	w.WriteHeader(w.Flags, frm.OpQuery, 1)
	w.Buf = append(w.Buf, []byte("SELECT * FROM system.local")...)
	require.NoError(t, w.Finish())

	r := bytes.NewReader(append([]byte(nil), w.Buf...))
	head, err := readHeader(r, make([]byte, headSize))
	require.NoError(t, err)

	rf := newFramer(nil, protoVersion4)
	err = rf.ReadFrame(r, &head)
	require.Error(t, err)
	require.Contains(t, err.Error(), "no compressor available with compressed frame body")
}

// Test_framer_v4NoCompressorLeavesTheBodyAlone is the uncompressed control for
// Test_framer_v4SnappyRoundTrip: same framer, same payload, no compressor. It pins that
// the compression above is driven by the compressor and not by something incidental to
// finish().
func Test_framer_v4NoCompressorLeavesTheBodyAlone(t *testing.T) {
	t.Parallel()

	payload := bytes.Repeat([]byte("a"), 1024)

	w := newFramer(nil, protoVersion4)
	require.Zero(t, w.Flags&frm.FlagCompress)

	w.WriteHeader(w.Flags, frm.OpQuery, 1)
	w.Buf = append(w.Buf, payload...)
	require.NoError(t, w.Finish())

	require.Zero(t, w.Buf[1]&frm.FlagCompress)
	require.Equal(t, payload, w.Buf[headSize:])
}

// inflatingCompressor returns a body just over the frame limit whatever it is given.
//
// It exists so the post-compression size guard can be exercised without building a
// ~256 MiB incompressible payload to feed real snappy: the guard is reached through the
// same branch either way, and the realistic route would cost a quarter-gigabyte
// allocation plus the compression of it on every unit run.
//
// The slice is large but free: finish() rejects it before the append, so it is never
// read or copied and stays untouched zero pages. That is why this test needs none of
// the memory-pressure skips TestFrameWriteTooLong and TestFrameReadTooLong carry -- and
// why a matching "exactly at the limit" case is deliberately absent. finish() would
// accept that one and copy the whole 256 MiB into f.buf, which measured 0.54s under
// -race against 0.00s here. Neither of the two pre-existing MaxFrameSize checks has an
// exact-boundary test either, so the off-by-one it would guard is left to the > that
// all three checks share.
type inflatingCompressor struct{}

func (inflatingCompressor) Name() string { return "inflating" }

func (inflatingCompressor) Encode([]byte) ([]byte, error) {
	return make([]byte, frm.MaxFrameSize+1), nil
}

func (inflatingCompressor) Decode(data []byte) ([]byte, error) { return data, nil }

// Test_framer_v4CompressionMayNotOverflowTheFrameLimit covers the asymmetry between the
// two size checks in finish(): the one at the top measures the uncompressed body, but
// the length written into the header is the compressed one.
//
// Snappy grows an incompressible body, so a payload that passes the first check can
// exceed the limit after Encode. Left unchecked the frame goes out declaring a length
// that both this driver's reader and the server refuse, turning a local ErrFrameTooBig
// into a remote rejection or a killed connection.
func Test_framer_v4CompressionMayNotOverflowTheFrameLimit(t *testing.T) {
	t.Parallel()

	f := newFramer(inflatingCompressor{}, protoVersion4)
	require.NotZero(t, f.Flags&frm.FlagCompress)

	f.WriteHeader(f.Flags, frm.OpQuery, 1)
	f.Buf = append(f.Buf, []byte("a body well inside the limit before compression")...)

	// The body above passes the pre-compression check; only the compressed size is
	// over, so reaching ErrFrameTooBig proves the second check is the one that fired.
	require.ErrorIs(t, f.Finish(), ErrFrameTooBig)
}
