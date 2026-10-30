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
	"errors"

	"runtime"

	"testing"

	lz4mod "github.com/scylladb/gocql/v2/internal/wirecompress"

	"github.com/stretchr/testify/require"

	frm "github.com/scylladb/gocql/v2/internal/frame"
)

// errReader fails every read with err, standing in for a socket whose read
// deadline expired or which was reset.
type errReader struct{ err error }

func (e errReader) Read([]byte) (int, error) { return 0, e.err }

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

func Test_framerFlags_neverBetaProtocol(t *testing.T) {
	comp := testMockedCompressor{}

	for _, version := range []byte{protoVersion4, protoVersion5, protoVersion5 | protoDirectionMask} {
		for _, compressor := range []Compressor{nil, comp} {
			if got := defaultFramerFlags(compressor, version); got&frm.FlagBetaProtocol != 0 {
				t.Errorf("defaultFramerFlags(%v, 0x%02x) set FlagBetaProtocol", compressor != nil, version)
			}
			if got := newFramer(compressor, version).Flags; got&frm.FlagBetaProtocol != 0 {
				t.Errorf("newFramer(%v, 0x%02x) set FlagBetaProtocol", compressor != nil, version)
			}
		}
	}

	// initCache is what the connection actually uses once the handshake completed.
	c := &driverConn{InternalVersion: protoVersion5, InternalCompressor: comp, InternalLogger: &defaultLogger{}}
	c.InternalInitFramerCache()
	if c.InternalFramers.InternalDefaults.InternalFlags&frm.FlagBetaProtocol != 0 {
		t.Error("initFramerCache(v5) seeded FlagBetaProtocol")
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
