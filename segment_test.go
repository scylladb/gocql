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

package gocql

import (
	"io"
	"strings"
	"testing"

	"github.com/gocql/gocql/internal/segment"
)

// Segment builders for the tests in this package, which construct transport
// segments to feed the connection's receive path (segment_reassembly_test.go,
// conn_test.go) or to check what the send path produced (frame_test.go).
//
// The codec and its own tests live in internal/segment. What is left here is the
// bridge: these take this package's Compressor, so a test can pass the same
// compressor it configured a Conn or framer with, and narrow it the way the
// production callers do.
//
// Each builds or reads one whole segment in a single call, with buffers of its own.
// No production caller works that way — the receive loop splits header and payload
// across two phases to re-arm its read deadline, and the send path encodes straight
// into the framer's wire buffer — so these model an allocation contract the driver
// deliberately does not follow.

// readUncompressedSegment reads a full uncompressed segment (header + payload) in
// one call, into a payload buffer of its own.
func readUncompressedSegment(r io.Reader) ([]byte, bool, error) {
	var scratch segment.Scratch

	h, err := segment.ReadUncompressedHeader(r)
	if err != nil {
		return nil, false, err
	}
	payload, err := segment.ReadUncompressedPayload(r, h, &scratch)
	if err != nil {
		return nil, false, err
	}
	return payload, h.IsSelfContained, nil
}

// readCompressedSegment reads a full compressed segment (header + payload) in one
// call, into payload buffers of its own.
func readCompressedSegment(r io.Reader, compressor Compressor) ([]byte, bool, error) {
	segComp, err := asSegmentCompressor(compressor)
	if err != nil {
		return nil, false, err
	}

	var scratch segment.Scratch

	h, err := segment.ReadCompressedHeader(r)
	if err != nil {
		return nil, false, err
	}
	payload, err := segment.ReadCompressedPayload(r, h, segComp, &scratch)
	if err != nil {
		return nil, false, err
	}
	return payload, h.IsSelfContained, nil
}

// newUncompressedSegment returns payload as a standalone uncompressed segment.
func newUncompressedSegment(payload []byte, isSelfContained bool) ([]byte, error) {
	return segment.AppendUncompressed(nil, payload, isSelfContained)
}

// newCompressedSegment returns payload as a standalone compressed segment.
func newCompressedSegment(payload []byte, isSelfContained bool, compressor Compressor) ([]byte, error) {
	segComp, err := asSegmentCompressor(compressor)
	if err != nil {
		return nil, err
	}
	return segment.AppendCompressed(nil, payload, isSelfContained, segComp)
}

// testSegmentCompressor is a Compressor that also supports v5 segment compression,
// passing bytes through unchanged.
//
// Declared here rather than reusing frame_test.go's testMockedCompressor because this
// file carries no build tag and that one is behind //go:build unit -- a helper taken
// from a tagged file compiles under `go test -tags unit` and nowhere else.
type testSegmentCompressor struct{}

func (testSegmentCompressor) Name() string                       { return "test-segment" }
func (testSegmentCompressor) Encode(data []byte) ([]byte, error) { return data, nil }
func (testSegmentCompressor) Decode(data []byte) ([]byte, error) { return data, nil }
func (testSegmentCompressor) AppendCompressed(dst, src []byte) ([]byte, error) {
	return append(dst, src...), nil
}

func (testSegmentCompressor) AppendDecompressed(dst, src []byte, decompressedLength uint32) ([]byte, error) {
	return append(dst, src...), nil
}

// TestAsSegmentCompressor covers the narrowing every v5 segment read and write goes
// through.
//
// The nil case is the one worth pinning: both callers hand it whatever compressor the
// connection was configured with, nil included, and rely on nil narrowing to a nil
// segment.Compressor rather than to an error -- that is how internal/segment spells
// "the uncompressed layout". Returning an error for it instead would break every
// uncompressed v5 connection.
func TestAsSegmentCompressor(t *testing.T) {
	t.Run("nil is the uncompressed layout", func(t *testing.T) {
		comp, err := asSegmentCompressor(nil)
		if err != nil {
			t.Fatalf("asSegmentCompressor(nil) returned %v, want no error", err)
		}
		if comp != nil {
			t.Errorf("asSegmentCompressor(nil) = %#v, want a nil segment.Compressor", comp)
		}
	})

	t.Run("a segment compressor narrows to itself", func(t *testing.T) {
		comp, err := asSegmentCompressor(testSegmentCompressor{})
		if err != nil {
			t.Fatalf("asSegmentCompressor returned %v, want no error", err)
		}
		if _, ok := comp.(testSegmentCompressor); !ok {
			t.Errorf("asSegmentCompressor returned %T, want the compressor it was given", comp)
		}
	})

	t.Run("a frame-only compressor is refused by name", func(t *testing.T) {
		comp, err := asSegmentCompressor(SnappyCompressor{})
		if err == nil {
			t.Fatal("asSegmentCompressor accepted a compressor with no segment support")
		}
		if comp != nil {
			t.Errorf("asSegmentCompressor returned %#v alongside its error, want nil", comp)
		}
		// Naming the offending compressor is the whole reason this narrowing lives in
		// this package rather than in internal/segment, which needs only the two
		// Append methods and so cannot reach Name.
		if !strings.Contains(err.Error(), "snappy") {
			t.Errorf("error %q does not name the compressor it rejected", err)
		}
	})
}

// TestResolveSegmentCompressor covers the one place the narrowing now runs.
//
// It happens during the handshake rather than once per segment, which puts a version
// gate on it that the per-segment call never needed: below v5 the segment codec is
// unreachable, and a compressor without segment support is an ordinary configuration
// there -- the driver's own integration suite runs protocol v4 with Snappy. Refusing it
// here would fail every one of those handshakes.
func TestResolveSegmentCompressor(t *testing.T) {
	t.Run("v5 narrows the compressor once", func(t *testing.T) {
		c := &Conn{version: protoVersion5, compressor: testSegmentCompressor{}}
		if err := c.resolveSegmentCompressor(); err != nil {
			t.Fatalf("resolveSegmentCompressor: %v", err)
		}
		if _, ok := c.segCompressor.(testSegmentCompressor); !ok {
			t.Errorf("segCompressor = %#v, want the compressor the connection was given", c.segCompressor)
		}
	})

	t.Run("v4 leaves a frame-only compressor alone", func(t *testing.T) {
		c := &Conn{version: protoVersion4, compressor: SnappyCompressor{}}
		if err := c.resolveSegmentCompressor(); err != nil {
			t.Fatalf("a protocol v4 connection with a frame compressor was refused: %v", err)
		}
		if c.segCompressor != nil {
			t.Errorf("segCompressor = %#v, want nil: v4 never reaches the segment codec", c.segCompressor)
		}
	})

	t.Run("v5 refuses a frame-only compressor by name", func(t *testing.T) {
		c := &Conn{version: protoVersion5, compressor: SnappyCompressor{}}
		err := c.resolveSegmentCompressor()
		if err == nil {
			t.Fatal("a v5 connection with a frame-only compressor was accepted")
		}
		if !strings.Contains(err.Error(), "snappy") {
			t.Errorf("error %q does not name the compressor it rejected", err)
		}
		if c.segCompressor != nil {
			t.Errorf("segCompressor = %#v alongside the error, want nil", c.segCompressor)
		}
	})
}
