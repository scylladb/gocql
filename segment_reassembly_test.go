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

package gocql

import (
	"bytes"
	"context"
	"io"
	"net"
	"strings"
	"testing"
	"time"
)

// segmentReader is a minimal ConnReader backed by an in-memory byte stream,
// used to drive the v5 segment reassembly path without a real socket.
type segmentReader struct {
	r *bytes.Reader
}

func newSegmentReader(b []byte) *segmentReader {
	return &segmentReader{r: bytes.NewReader(b)}
}

func (s *segmentReader) Read(p []byte) (int, error) { return s.r.Read(p) }
func (s *segmentReader) Close() error               { return nil }
func (s *segmentReader) RemoteAddr() net.Addr       { return nil }
func (s *segmentReader) SetTimeout(_ time.Duration) {}
func (s *segmentReader) GetTimeout() time.Duration  { return 0 }

// mustUncompressedSegment builds a single uncompressed transport segment
// carrying payload, failing the test on error.
func mustUncompressedSegment(t *testing.T, payload []byte, selfContained bool) []byte {
	t.Helper()
	seg, err := newUncompressedSegment(payload, selfContained)
	if err != nil {
		t.Fatalf("newUncompressedSegment: %v", err)
	}
	return seg
}

func TestReadContinuationSegmentIntoAppends(t *testing.T) {
	seg := mustUncompressedSegment(t, []byte("hello"), false)
	c := &Conn{r: newSegmentReader(seg)}

	var buf bytes.Buffer
	if err := c.readContinuationSegmentInto(&buf, maxFrameSize); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := buf.String(); got != "hello" {
		t.Fatalf("got %q, want %q", got, "hello")
	}
}

func TestReadContinuationSegmentIntoRejectsSelfContained(t *testing.T) {
	seg := mustUncompressedSegment(t, []byte("hello"), true)
	c := &Conn{r: newSegmentReader(seg)}

	var buf bytes.Buffer
	err := c.readContinuationSegmentInto(&buf, maxFrameSize)
	if err == nil || !strings.Contains(err.Error(), "expected a continuation") {
		t.Fatalf("expected self-contained rejection, got %v", err)
	}
}

func TestReadContinuationSegmentIntoRejectsEmptyPayload(t *testing.T) {
	seg := mustUncompressedSegment(t, nil, false)
	c := &Conn{r: newSegmentReader(seg)}

	var buf bytes.Buffer
	err := c.readContinuationSegmentInto(&buf, maxFrameSize)
	if err == nil || !strings.Contains(err.Error(), "no progress") {
		t.Fatalf("expected no-progress rejection, got %v", err)
	}
}

func TestReadContinuationSegmentIntoRejectsOverLimit(t *testing.T) {
	seg := mustUncompressedSegment(t, []byte("0123456789"), false)
	c := &Conn{r: newSegmentReader(seg)}

	var buf bytes.Buffer
	buf.WriteString("prefix") // 6 bytes already buffered
	// limit 10 leaves room for only 4 more bytes; the 10-byte payload exceeds it.
	err := c.readContinuationSegmentInto(&buf, 10)
	if err == nil || !strings.Contains(err.Error(), "exceeds maximum size") {
		t.Fatalf("expected over-limit rejection, got %v", err)
	}
}

// TestRecvSplitFrameRejectsOversizedLength drives recvSplitFrame with a CQL
// frame header declaring a body length beyond maxFrameSize. The declared
// length is rejected before any large allocation and before processFrame.
func TestRecvSplitFrameRejectsOversizedLength(t *testing.T) {
	// Build a 9-byte CQL frame header (v5 response) whose length field is
	// maxFrameSize+1, then wrap it in a single non-self-contained segment.
	header := make([]byte, 9)
	header[0] = protoVersion5 | protoDirectionMask // version (response)
	header[1] = 0                                  // flags
	// header[2:4] stream, header[4] opcode left zero
	oversized := uint32(maxFrameSize + 1)
	header[5] = byte(oversized >> 24)
	header[6] = byte(oversized >> 16)
	header[7] = byte(oversized >> 8)
	header[8] = byte(oversized)

	seg := mustUncompressedSegment(t, header, false)
	c := &Conn{r: newSegmentReader(seg)}

	err := c.recvSplitFrame(context.Background(), nil)
	if err == nil || !strings.Contains(err.Error(), "invalid frame body length") {
		t.Fatalf("expected oversized-length rejection, got %v", err)
	}
}

// TestRecvSplitFrameRejectsTruncatedHeaderStream ensures the header-accumulation
// loop terminates (with an error) when the peer stops sending before even the
// 9-byte CQL header is complete, rather than looping forever.
func TestRecvSplitFrameRejectsTruncatedHeaderStream(t *testing.T) {
	// A single 4-byte continuation segment, then EOF: fewer than the 9 header
	// bytes recvSplitFrame needs.
	seg := mustUncompressedSegment(t, []byte("abcd"), false)
	c := &Conn{r: newSegmentReader(seg)}

	err := c.recvSplitFrame(context.Background(), nil)
	if err == nil {
		t.Fatalf("expected error on truncated header stream, got nil")
	}
	if !bytes.Contains([]byte(err.Error()), []byte("continuation segment header")) &&
		err != io.EOF {
		t.Fatalf("expected read failure after truncation, got %v", err)
	}
}
