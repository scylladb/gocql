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
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"math"
	"net"
	"runtime"
	"slices"
	"strings"
	"testing"
	"time"

	frm "github.com/gocql/gocql/internal/frame"
	"github.com/gocql/gocql/internal/segment"
	"github.com/gocql/gocql/internal/streams"
)

// segmentReader is a minimal connReadSource backed by an in-memory byte stream,
// used to drive the v5 segment reassembly path without a real socket.
type segmentReader struct {
	r *bytes.Reader
	// disarmed records the current disarm state. There is no deadline to disarm on
	// an in-memory stream, but implementing it is what keeps this reader usable as
	// Conn.r — a reader that could not be disarmed would silently exercise the
	// receive path with the idle-wait handling switched off.
	disarmed bool

	// assembly, budgeted and budgets record the frame-assembly budget, for the same
	// reason: an in-memory stream has no deadline to bound, but a reader that
	// dropped the calls would hide whether recvSegment made them.
	assembly time.Duration
	budgeted bool
	budgets  []bool
}

var _ connReadSource = (*segmentReader)(nil)

func newSegmentReader(b []byte) *segmentReader {
	return &segmentReader{r: bytes.NewReader(b)}
}

func (s *segmentReader) Read(p []byte) (int, error) { return s.r.Read(p) }
func (s *segmentReader) Close() error               { return nil }
func (s *segmentReader) RemoteAddr() net.Addr       { return nil }
func (s *segmentReader) SetTimeout(_ time.Duration) {}
func (s *segmentReader) GetTimeout() time.Duration  { return 0 }
func (s *segmentReader) setDisarm(v bool)           { s.disarmed = v }

// There is no deadline to budget on an in-memory stream either, but the budget is
// recorded so a test can assert recvSegment started and cleared one.
func (s *segmentReader) SetFrameAssemblyTimeout(d time.Duration) { s.assembly = d }
func (s *segmentReader) setReadBudget(v bool)                    { s.budgeted = v; s.budgets = append(s.budgets, v) }

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

func TestReadContinuationSegmentReturnsPayload(t *testing.T) {
	seg := mustUncompressedSegment(t, []byte("hello"), false)
	c := &Conn{r: newSegmentReader(seg)}

	payload, err := c.readContinuationSegment()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := string(payload); got != "hello" {
		t.Fatalf("got %q, want %q", got, "hello")
	}
}

func TestReadContinuationSegmentRejectsSelfContained(t *testing.T) {
	seg := mustUncompressedSegment(t, []byte("hello"), true)
	c := &Conn{r: newSegmentReader(seg)}

	_, err := c.readContinuationSegment()
	if err == nil || !strings.Contains(err.Error(), "expected a continuation") {
		t.Fatalf("expected self-contained rejection, got %v", err)
	}
}

func TestReadContinuationSegmentRejectsEmptyPayload(t *testing.T) {
	seg := mustUncompressedSegment(t, nil, false)
	c := &Conn{r: newSegmentReader(seg)}

	_, err := c.readContinuationSegment()
	if err == nil || !strings.Contains(err.Error(), "no progress") {
		t.Fatalf("expected no-progress rejection, got %v", err)
	}
}

// ReadTimeout cannot bound the time to assemble a segmented frame: every read arms
// it afresh, so a peer that delivers one segment per interval renews its lease for as
// long as it likes, and the only ceiling is the declared frame length. This is what
// FrameAssemblyTimeout adds, and the two cases are the whole of it -- the same
// trickle completes when no budget is configured, so the bound is what rejects it
// rather than the pace.
func TestRecvSegmentBoundsTheTimeToAssembleOneFrame(t *testing.T) {
	// A READY frame on the event stream, split into three segments: with no session
	// attached processFrame parses it and drops it, which is all this needs -- the
	// assertion is about time, not contents.
	const perSegment = 64
	body := bytes.Repeat([]byte{0x5A}, 2*perSegment)
	frame := make([]byte, headSize)
	frame[0] = protoVersion5 | protoDirectionMask
	binary.BigEndian.PutUint16(frame[2:4], uint16(0xFFFF))
	frame[4] = byte(frm.OpReady)
	binary.BigEndian.PutUint32(frame[5:headSize], uint32(len(body)))
	frame = append(frame, body...)

	var segments [][]byte
	for src := frame; len(src) > 0; {
		n := min(len(src), perSegment)
		segments = append(segments, mustUncompressedSegment(t, src[:n], false))
		src = src[n:]
	}
	if len(segments) < 3 {
		t.Fatalf("the frame must need several segments to trickle, got %d", len(segments))
	}

	// Each segment arrives well inside ReadTimeout, so no individual read is ever
	// close to failing; only their sum crosses the budget.
	const gap = 100 * time.Millisecond

	for _, tc := range []struct {
		name    string
		budget  time.Duration
		wantErr bool
	}{
		{name: "no budget configured", budget: 0, wantErr: false},
		{name: "budget shorter than the trickle", budget: 120 * time.Millisecond, wantErr: true},
		// Held as a time.Time for this: as a UnixNano count the deadline wraps past
		// the year 2262 into the past, and the largest budget expressible becomes the
		// harshest one there is.
		{name: "a budget beyond UnixNano's range", budget: math.MaxInt64, wantErr: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client, server := net.Pipe()
			defer client.Close()
			defer server.Close()

			go func() {
				for _, seg := range segments {
					if _, err := server.Write(seg); err != nil {
						return
					}
					time.Sleep(gap)
				}
			}()

			cr := &connReader{conn: client, r: bufio.NewReader(client)}
			cr.SetTimeout(5 * time.Second)
			cr.SetFrameAssemblyTimeout(tc.budget)

			c := &Conn{r: cr, streams: streams.New(), logger: nopLogger{}}
			c.initFramerCache()

			err := c.recvSegment(context.Background())
			if !tc.wantErr {
				if err != nil {
					t.Fatalf("a trickling peer must not be rejected without a budget: %v", err)
				}
				return
			}

			var netErr net.Error
			if !errors.As(err, &netErr) || !netErr.Timeout() {
				t.Fatalf("expected the budget to expire the read, got %v", err)
			}
		})
	}
}

// The budget starts on the first byte of the segment header, not once the header is
// complete. Only the wait for that byte is deliberately unbounded; the rest of the
// header is otherwise covered by ReadTimeout alone, so with ReadTimeout disabled a
// peer dribbling a header out would be bounded by nothing at all -- while
// FrameAssemblyTimeout is documented to run from the moment the peer starts sending.
func TestRecvSegmentBudgetsTheHeaderItself(t *testing.T) {
	seg := mustUncompressedSegment(t, []byte("hello"), true)

	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()

	// The header alone is dribbled a byte per 100ms, so it takes longer than the
	// budget; everything after it arrives at once. That is what isolates the window
	// under test -- a budget that started after the header instead would see only the
	// instant payload and never expire.
	go func() {
		head := segment.UncompressedHeaderSize
		for _, b := range seg[:head] {
			if _, err := server.Write([]byte{b}); err != nil {
				return
			}
			time.Sleep(100 * time.Millisecond)
		}
		_, _ = server.Write(seg[head:])
	}()

	cr := &connReader{conn: client, r: bufio.NewReader(client)}
	cr.SetTimeout(0) // the case that has nothing else to fall back on
	cr.SetFrameAssemblyTimeout(120 * time.Millisecond)

	c := &Conn{r: cr, streams: streams.New(), logger: nopLogger{}}
	c.initFramerCache()

	err := c.recvSegment(context.Background())
	var netErr net.Error
	if !errors.As(err, &netErr) || !netErr.Timeout() {
		t.Fatalf("expected the budget to expire during the header, got %v", err)
	}
	// Not the benign idle timeout: bytes were consumed, so the stream is at an
	// unknown offset and the connection has to go down.
	if errors.Is(err, ErrReadHeaderTimeout) {
		t.Error("a partially-read header must not be reported as an idle timeout")
	}
}

// The budget has to be cleared on the way out. It is absolute, so one left behind
// would still be expiring during the next idle wait for a frame that has not started
// arriving -- turning a healthy connection into a dropped one.
func TestRecvSegmentClearsTheBudgetAfterEachFrame(t *testing.T) {
	frame := make([]byte, headSize)
	frame[0] = protoVersion5 | protoDirectionMask
	binary.BigEndian.PutUint16(frame[2:4], uint16(0xFFFF))
	frame[4] = byte(frm.OpReady)

	r := newSegmentReader(mustUncompressedSegment(t, frame, true))
	c := &Conn{r: r, streams: streams.New(), logger: nopLogger{}}
	c.initFramerCache()

	if err := c.recvSegment(context.Background()); err != nil {
		t.Fatalf("recvSegment: %v", err)
	}

	if want := []bool{true, false}; !slices.Equal(r.budgets, want) {
		t.Errorf("budget calls were %v, want %v", r.budgets, want)
	}
	if r.budgeted {
		t.Error("the budget is still running after the frame was processed")
	}
}

// TestRecvSplitFrameRejectsOverlongStream pins that continuation segments
// carrying more bytes than the CQL frame header declared are rejected, rather
// than appended past the size the reassembly buffer was allocated for.
func TestRecvSplitFrameRejectsOverlongStream(t *testing.T) {
	// A header declaring a 4-byte body, so the whole frame is headSize+4 bytes,
	// followed by a continuation segment carrying 8 body bytes.
	header := make([]byte, headSize)
	header[0] = protoVersion5 | protoDirectionMask
	header[8] = 4

	seg := mustUncompressedSegment(t, []byte("01234567"), false)
	c := &Conn{r: newSegmentReader(seg)}

	err := c.recvSplitFrame(context.Background(), header, time.Time{}, time.Time{})
	if err == nil || !strings.Contains(err.Error(), "exceeds its declared length") {
		t.Fatalf("expected over-long rejection, got %v", err)
	}
}

// TestRecvSplitFrameAllocatesExactlyOneFrameBuffer pins the reassembly buffer
// against the two allocation regressions it is shaped to avoid: growing it as
// payloads arrive (a bytes.Buffer reaches ~2x the frame size), and then copying
// the assembled frame into the read framer instead of handing it over. Both are
// invisible in the output but double or triple the memory a single large response
// occupies.
func TestRecvSplitFrameAllocatesExactlyOneFrameBuffer(t *testing.T) {
	const bodyLen = 3 * segment.MaxPayloadSize

	// Enough streams to also let the framer pool warm up.
	const runs = 4
	body := bytes.Repeat([]byte{0x5A}, bodyLen)
	frame := make([]byte, headSize)
	frame[0] = protoVersion5 | protoDirectionMask
	// Stream -1 is the event stream: with no session attached, processFrame parses
	// the frame and drops it, which is all this test needs. A READY frame carries
	// no body fields, so the filler body is never inspected.
	binary.BigEndian.PutUint16(frame[2:4], uint16(0xFFFF))
	frame[4] = byte(frm.OpReady)
	binary.BigEndian.PutUint32(frame[5:headSize], uint32(bodyLen))
	frame = append(frame, body...)

	var stream []byte
	for src := frame; len(src) > 0; {
		n := min(len(src), segment.MaxPayloadSize)
		stream = append(stream, mustUncompressedSegment(t, src[:n], false)...)
		src = src[n:]
	}

	c := &Conn{
		r:       newSegmentReader(bytes.Repeat(stream, runs)),
		streams: streams.New(),
		logger:  &defaultLogger{},
	}
	c.initFramerCache()

	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	for i := 0; i < runs; i++ {
		// Stream 0 is unused, so processFrame parses and drops the frame; that is
		// enough to exercise reassembly and the framer hand-over.
		if err := c.recvSegment(context.Background()); err != nil {
			t.Fatalf("run %d: recvSegment: %v", i, err)
		}
	}
	runtime.ReadMemStats(&after)

	// One buffer per frame, sized to the frame: anything much above that means
	// either the buffer was grown into or the frame was copied again.
	allocated := after.TotalAlloc - before.TotalAlloc
	if limit := uint64(runs) * uint64(len(frame)) * 3 / 2; allocated > limit {
		t.Errorf("reassembling %d frames of %d bytes allocated %d bytes, want at most %d",
			runs, len(frame), allocated, limit)
	}
}

// TestRecvSplitFrameRejectsOversizedLength drives recvSplitFrame with a CQL
// frame header declaring a body length beyond frm.MaxFrameSize. The declared
// length is rejected before any large allocation and before processFrame.
func TestRecvSplitFrameRejectsOversizedLength(t *testing.T) {
	// Build a 9-byte CQL frame header (v5 response) whose length field is
	// frm.MaxFrameSize+1, then wrap it in a single non-self-contained segment.
	header := make([]byte, 9)
	header[0] = protoVersion5 | protoDirectionMask // version (response)
	header[1] = 0                                  // flags
	// header[2:4] stream, header[4] opcode left zero
	oversized := uint32(frm.MaxFrameSize + 1)
	header[5] = byte(oversized >> 24)
	header[6] = byte(oversized >> 16)
	header[7] = byte(oversized >> 8)
	header[8] = byte(oversized)

	seg := mustUncompressedSegment(t, header, false)
	c := &Conn{r: newSegmentReader(seg)}

	err := c.recvSplitFrame(context.Background(), nil, time.Time{}, time.Time{})
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

	err := c.recvSplitFrame(context.Background(), nil, time.Time{}, time.Time{})
	if err == nil {
		t.Fatalf("expected error on truncated header stream, got nil")
	}
	if !bytes.Contains([]byte(err.Error()), []byte("continuation segment header")) &&
		err != io.EOF {
		t.Fatalf("expected read failure after truncation, got %v", err)
	}
}

// v5ReadyFrame builds a v5 READY response frame on the event stream, declaring
// bodyLen body bytes but carrying only the bytes in body. With no session
// attached, processFrameSource parses such a frame and drops it, which is all
// these tests need from a well-formed one.
func v5ReadyFrame(bodyLen int, body []byte) []byte {
	frame := make([]byte, headSize)
	frame[0] = protoVersion5 | protoDirectionMask
	binary.BigEndian.PutUint16(frame[2:4], uint16(0xFFFF))
	frame[4] = byte(frm.OpReady)
	binary.BigEndian.PutUint32(frame[5:headSize], uint32(bodyLen))
	return append(frame, body...)
}

// TestProcessAllFramesInSegmentBoundsFrameLength pins that a frame header inside
// a self-contained segment cannot declare a body the segment does not carry.
//
// A self-contained segment holds only whole frames, so such a header is a
// framing violation — but readFrame acts on the declared length before it
// discovers the short read, and the io.ErrUnexpectedEOF that follows is not a
// net.Error, so processFrameSource would keep it per-request and leave the
// connection up for the peer to do it again. A ~20-byte segment would buy a
// frm.MaxFrameSize allocation, repeatable.
func TestProcessAllFramesInSegmentBoundsFrameLength(t *testing.T) {
	newConn := func(stream []byte) *Conn {
		c := &Conn{
			r:       newSegmentReader(stream),
			streams: streams.New(),
			logger:  &defaultLogger{},
		}
		c.initFramerCache()
		return c
	}

	// Well below frm.MaxFrameSize, but far enough above the segment that the budget
	// below cannot be met by accident if the bound is removed.
	const declared = 32 << 20

	t.Run("rejects a header longer than the whole segment", func(t *testing.T) {
		seg := mustUncompressedSegment(t, v5ReadyFrame(declared, nil), true)
		c := newConn(seg)

		var before, after runtime.MemStats
		runtime.ReadMemStats(&before)
		err := c.recvSegment(context.Background())
		runtime.ReadMemStats(&after)

		if err == nil || !strings.Contains(err.Error(), "remain in the self-contained segment") {
			t.Fatalf("expected the declared length to be bounded by the segment, got %v", err)
		}
		// The error alone does not prove much: the unbounded path reaches the same
		// failure, just after allocating what the peer asked for.
		if allocated := after.TotalAlloc - before.TotalAlloc; allocated > declared/8 {
			t.Errorf("rejecting the frame allocated %d bytes, want well under the %d it declared", allocated, declared)
		}
	})

	t.Run("bounds each frame by what is left, not by the segment", func(t *testing.T) {
		// A complete frame first, so the second header is checked against the
		// remainder rather than the segment's original length.
		payload := v5ReadyFrame(0, nil)
		payload = append(payload, v5ReadyFrame(declared, nil)...)
		c := newConn(mustUncompressedSegment(t, payload, true))

		err := c.recvSegment(context.Background())
		if err == nil || !strings.Contains(err.Error(), "remain in the self-contained segment") {
			t.Fatalf("expected the second frame to be bounded by the remaining bytes, got %v", err)
		}
	})

	t.Run("accepts a frame that exactly fills the segment", func(t *testing.T) {
		body := bytes.Repeat([]byte{0x5A}, 64)
		c := newConn(mustUncompressedSegment(t, v5ReadyFrame(len(body), body), true))

		if err := c.recvSegment(context.Background()); err != nil {
			t.Fatalf("a frame whose body ends exactly at the segment boundary must be accepted, got %v", err)
		}
	})
}

// slowSegmentReader stalls once, on the first read that reaches delayFrom, so a
// test can put a known lower bound on how long a chosen part of the receive path
// spent waiting on the network.
type slowSegmentReader struct {
	*segmentReader
	delayFrom int64
	delay     time.Duration
	stalled   bool
}

func (s *slowSegmentReader) Read(p []byte) (int, error) {
	if !s.stalled && s.r.Size()-int64(s.r.Len()) >= s.delayFrom {
		s.stalled = true
		time.Sleep(s.delay)
	}
	return s.segmentReader.Read(p)
}

// TestRecvSegmentObservesHeaderOverTheNetworkRead pins FrameHeaderObserver's
// documented contract on v5: Start and End bracket the header coming off the
// network, not the parse.
//
// On v5 the whole segment is read before any CQL header is looked at, so timing
// the header read where it happens measures a bytes.Reader — the window collapses
// to nanoseconds and the network wait, which is the thing worth observing, is
// reported by nobody.
func TestRecvSegmentObservesHeaderOverTheNetworkRead(t *testing.T) {
	// Long enough that an in-memory parse cannot be mistaken for it, short enough
	// not to slow the suite. Only ever asserted as a lower bound.
	const stall = 30 * time.Millisecond

	newConn := func(r connReadSource, observer *recordingFrameHeaderObserver) *Conn {
		c := &Conn{
			r:             r,
			streams:       streams.New(),
			logger:        &defaultLogger{},
			frameObserver: observer,
		}
		c.initFramerCache()
		return c
	}

	t.Run("self-contained segment", func(t *testing.T) {
		seg := mustUncompressedSegment(t, v5ReadyFrame(0, nil), true)
		observer := &recordingFrameHeaderObserver{t: t}
		// Stall before the very first byte: the whole segment read is inside the
		// window the observer should report.
		c := newConn(&slowSegmentReader{segmentReader: newSegmentReader(seg), delay: stall}, observer)

		if err := c.recvSegment(context.Background()); err != nil {
			t.Fatalf("recvSegment: %v", err)
		}

		frames := observer.getFrames()
		if len(frames) != 1 {
			t.Fatalf("expected 1 observed header, got %d", len(frames))
		}
		if got := frames[0].End.Sub(frames[0].Start); got < stall {
			t.Errorf("observed header window is %v, want at least the %v the segment read stalled: "+
				"Start/End are timing the parse, not the network read", got, stall)
		}
	})

	t.Run("header split across segments", func(t *testing.T) {
		// Two bytes of the CQL header in the first segment, the rest plus the body
		// in the second, so the header is only complete after a second network read.
		frame := v5ReadyFrame(0, nil)
		first := mustUncompressedSegment(t, frame[:2], false)
		stream := append(append([]byte{}, first...), mustUncompressedSegment(t, frame[2:], false)...)

		observer := &recordingFrameHeaderObserver{t: t}
		// Stall on the second segment only, so the window can only cover it if
		// recvSplitFrame extended End past the first.
		c := newConn(&slowSegmentReader{
			segmentReader: newSegmentReader(stream),
			delayFrom:     int64(len(first)),
			delay:         stall,
		}, observer)

		if err := c.recvSegment(context.Background()); err != nil {
			t.Fatalf("recvSegment: %v", err)
		}

		frames := observer.getFrames()
		if len(frames) != 1 {
			t.Fatalf("expected 1 observed header, got %d", len(frames))
		}
		if got := frames[0].End.Sub(frames[0].Start); got < stall {
			t.Errorf("observed header window is %v, want at least the %v the second segment stalled: "+
				"End was not extended to when the header finished arriving", got, stall)
		}
	})
}

// invertingCompressor is a segment compressor that actually changes the bytes it is
// given, unlike testSegmentCompressor's passthrough.
//
// That difference is the whole point of using it below: with a passthrough, a payload
// read through the uncompressed path comes back byte-identical to one read through the
// compressed path, so the test cannot tell which layout was used. Inverting makes the
// two paths distinguishable, which is what lets the assertion catch a connection whose
// header and payload readers disagree about the layout.
type invertingCompressor struct{}

func (invertingCompressor) Name() string { return "inverting" }

func invertBytes(dst, src []byte) []byte {
	for _, b := range src {
		dst = append(dst, b^0xFF)
	}
	return dst
}

func (invertingCompressor) Encode(data []byte) ([]byte, error) { return invertBytes(nil, data), nil }
func (invertingCompressor) Decode(data []byte) ([]byte, error) { return invertBytes(nil, data), nil }

func (invertingCompressor) AppendCompressed(dst, src []byte) ([]byte, error) {
	return invertBytes(dst, src), nil
}

func (invertingCompressor) AppendDecompressed(dst, src []byte, _ uint32) ([]byte, error) {
	return invertBytes(dst, src), nil
}

// TestRecvSegmentReadsACompressedSegment drives the connection's receive path with a
// compressor attached, which nothing else here does.
//
// The header reader and the payload reader take their layout from the same field, and
// this is what holds them together: a compressed header is eight bytes where an
// uncompressed one is six, so a connection that resolved its compressor late -- or
// read the layout from a second source -- reads the header at the wrong width and fails
// its CRC24 rather than mis-framing the rest of the stream quietly.
func TestRecvSegmentReadsACompressedSegment(t *testing.T) {
	const payload = "a compressed self-contained segment"

	seg, err := newCompressedSegment([]byte(payload), true, invertingCompressor{})
	if err != nil {
		t.Fatalf("newCompressedSegment: %v", err)
	}

	c := &Conn{
		r:          newSegmentReader(seg),
		version:    protoVersion5,
		compressor: invertingCompressor{},
	}
	if err := c.resolveSegmentCompressor(); err != nil {
		t.Fatalf("resolveSegmentCompressor: %v", err)
	}

	hdr, err := c.readFirstSegmentHeader()
	if err != nil {
		t.Fatalf("readFirstSegmentHeader: %v", err)
	}
	if !hdr.IsSelfContained {
		t.Error("a self-contained segment was read as a continuation")
	}

	got, err := c.readSegmentPayload(hdr)
	if err != nil {
		t.Fatalf("readSegmentPayload: %v", err)
	}
	if string(got) != payload {
		t.Errorf("payload = %q, want %q", got, payload)
	}
}
