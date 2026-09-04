package dialer

import (
	"bytes"
	"errors"
	"fmt"
	"strings"
	"testing"

	frm "github.com/gocql/gocql/internal/frame"
)

// collect feeds the splitter and returns the frames it emitted, copied because the
// emitted slice aliases the splitter's buffer.
func collect(t *testing.T, s *FrameSplitter, chunks ...[]byte) [][]byte {
	t.Helper()

	var got [][]byte
	for _, chunk := range chunks {
		if err := s.Feed(chunk, func(frame []byte) error {
			got = append(got, append([]byte(nil), frame...))
			return nil
		}); err != nil {
			t.Fatalf("Feed: %v", err)
		}
	}
	return got
}

func TestFrameSplitterReassembles(t *testing.T) {
	one := frameV4(opOptions, 0x00, nil)
	two := frameV4(opQuery, 0x00, []byte("0123456789"))

	for _, tc := range []struct {
		name   string
		chunks [][]byte
		want   [][]byte
	}{
		{
			name:   "one whole frame",
			chunks: [][]byte{one},
			want:   [][]byte{one},
		},
		{
			name:   "two frames coalesced into one feed",
			chunks: [][]byte{append(append([]byte(nil), one...), two...)},
			want:   [][]byte{one, two},
		},
		{
			name:   "frame split mid-header",
			chunks: [][]byte{two[:5], two[5:]},
			want:   [][]byte{two},
		},
		{
			name:   "frame split mid-body",
			chunks: [][]byte{two[:12], two[12:]},
			want:   [][]byte{two},
		},
		{
			name:   "frame plus the head of the next",
			chunks: [][]byte{append(append([]byte(nil), one...), two[:4]...), two[4:]},
			want:   [][]byte{one, two},
		},
		{
			name:   "empty feed",
			chunks: [][]byte{nil, one},
			want:   [][]byte{one},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var s FrameSplitter
			got := collect(t, &s, tc.chunks...)

			if len(got) != len(tc.want) {
				t.Fatalf("emitted %d frames, want %d", len(got), len(tc.want))
			}
			for i := range got {
				if !bytes.Equal(got[i], tc.want[i]) {
					t.Errorf("frame %d: got % X, want % X", i, got[i], tc.want[i])
				}
			}
			if s.Pending() {
				t.Error("splitter still holds a partial frame")
			}
		})
	}
}

// TestFrameSplitterByteAtATime is the boundary case every other split reduces to: a
// stream delivered one byte per call must come back out as the same frames.
func TestFrameSplitterByteAtATime(t *testing.T) {
	frames := [][]byte{
		frameV4(opOptions, 0x00, nil),
		frameV4(opQuery, 0x00, bytes.Repeat([]byte{0x5A}, 300)),
		frameV4(opStartup, 0x00, []byte{0x00, 0x00}),
	}

	var stream []byte
	for _, f := range frames {
		stream = append(stream, f...)
	}

	var s FrameSplitter
	chunks := make([][]byte, 0, len(stream))
	for i := range stream {
		chunks = append(chunks, stream[i:i+1])
	}
	got := collect(t, &s, chunks...)

	if len(got) != len(frames) {
		t.Fatalf("emitted %d frames, want %d", len(got), len(frames))
	}
	for i := range got {
		if !bytes.Equal(got[i], frames[i]) {
			t.Errorf("frame %d: got % X, want % X", i, got[i], frames[i])
		}
	}
}

// TestFrameSplitterPendingReportsPartialFrame pins the signal a segment chain needs:
// whether the stream stopped mid-frame.
func TestFrameSplitterPendingReportsPartialFrame(t *testing.T) {
	frame := frameV4(opQuery, 0x00, []byte("0123456789"))

	for _, tc := range []struct {
		name string
		n    int
		want bool
	}{
		{name: "nothing fed", n: 0, want: false},
		{name: "mid-header", n: 4, want: true},
		{name: "header exactly", n: FrameHeaderLen, want: true},
		{name: "mid-body", n: FrameHeaderLen + 3, want: true},
		{name: "whole frame", n: len(frame), want: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var s FrameSplitter
			collect(t, &s, frame[:tc.n])
			if got := s.Pending(); got != tc.want {
				t.Errorf("Pending() = %v, want %v", got, tc.want)
			}
		})
	}
}

// TestFrameSplitterBoundsDeclaredLength pins that a peer- or file-supplied body
// length cannot drive an unbounded append. Without the bound a corrupt header alone
// makes the splitter grow toward a declared 2 GiB body.
func TestFrameSplitterBoundsDeclaredLength(t *testing.T) {
	header := func(bodyLen uint32) []byte {
		return []byte{
			0x04, 0x00, 0x00, 0x7B, byte(opQuery),
			byte(bodyLen >> 24), byte(bodyLen >> 16), byte(bodyLen >> 8), byte(bodyLen),
		}
	}

	for _, tc := range []struct {
		name    string
		bodyLen uint32
		wantErr bool
	}{
		{name: "at the limit", bodyLen: frm.MaxFrameSize, wantErr: false},
		{name: "one past the limit", bodyLen: frm.MaxFrameSize + 1, wantErr: true},
		{name: "high bit set", bodyLen: 0xFFFFFFFF, wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var s FrameSplitter
			err := s.Feed(header(tc.bodyLen), func([]byte) error {
				t.Error("a header-only feed emitted a frame")
				return nil
			})
			if tc.wantErr && err == nil {
				t.Errorf("declared body length %d was accepted", tc.bodyLen)
			}
			if !tc.wantErr && err != nil {
				t.Errorf("declared body length %d was rejected: %v", tc.bodyLen, err)
			}
		})
	}
}

// TestFrameSplitterRefusesPreV3 pins the protocol floor. The splitter reads the body
// length from frame[5:9] and the recorder a 2-byte stream id from frame[2:4], both of
// which are the v3+ layout; on v1/v2 every one of those offsets is a byte early, so
// slicing such a stream emits things that are not frames and records them without
// complaint. Only a caller-pinned ProtoVersion can produce one -- negotiation lands on
// v4 -- which is exactly the case that used to go unreported.
//
// Every length such a frame can arrive in is checked, because the floor used to sit
// behind FrameBodyLen's completeness test and so only ran once nine bytes had arrived.
// A v1/v2 header is eight bytes and a bodyless frame is nothing but its header, so an
// OPTIONS -- which the control connection heartbeats -- was a complete frame one byte
// short of the check meant to refuse it, and Feed answered nil. See consume.
//
// The refusal names the version rather than the length it would otherwise have
// believed, and it latches: a stream whose framing is in doubt cannot be resumed.
func TestFrameSplitterRefusesPreV3(t *testing.T) {
	for _, version := range []byte{0x01, 0x02} {
		// A whole bodyless v1/v2 OPTIONS: version, flags, a 1-byte stream id, the
		// opcode and a 4-byte body length of zero.
		frame := []byte{version, 0x00, 0x00, byte(opOptions), 0x00, 0x00, 0x00, 0x00}

		for _, tc := range []struct {
			name string
			feed []byte
		}{
			{name: "the version byte alone", feed: frame[:1]},
			{name: "a whole bodyless frame", feed: frame},
			{name: "a frame and one byte past it", feed: append(append([]byte(nil), frame...), 0x00)},
		} {
			t.Run(fmt.Sprintf("v%d/%s", version, tc.name), func(t *testing.T) {
				var s FrameSplitter
				err := s.Feed(tc.feed, mustNotEmit(t))
				if err == nil {
					t.Fatalf("%d bytes of a protocol v%d frame were accepted", len(tc.feed), version)
				}
				if want := fmt.Sprintf("protocol v%d", version); !strings.Contains(err.Error(), want) {
					t.Errorf("error does not name the version (%q): %v", want, err)
				}

				// Including a later call carrying nothing, which is how a refused
				// connection's zero-length write reaches here.
				if again := s.Feed(nil, mustNotEmit(t)); again != err {
					t.Errorf("Feed after the refusal = %v, want the latched %v", again, err)
				}
			})
		}
	}
}

// TestFrameSplitterPropagatesEmitError pins that an emit failure stops the feed there
// and then, so the caller stops with the stream positioned exactly after the frame that
// failed rather than partway through the rest of the buffer.
func TestFrameSplitterPropagatesEmitError(t *testing.T) {
	want := errors.New("recording aborted")
	one := frameV4(opOptions, 0x00, nil)
	two := frameV4(opQuery, 0x00, []byte("body"))

	var s FrameSplitter
	calls := 0
	err := s.Feed(append(append([]byte(nil), one...), two...), func([]byte) error {
		calls++
		return want
	})

	if !errors.Is(err, want) {
		t.Fatalf("Feed error = %v, want %v", err, want)
	}
	if calls != 1 {
		t.Errorf("emit called %d times, want 1 — the feed continued past a failure", calls)
	}
}

// TestFrameSplitterStaysFailedAfterEmitError pins that a splitter is one-shot.
//
// The frame that failed in emit is complete and owes no body bytes, so a splitter that
// simply kept it buffered would consume nothing on the next call and emit that very
// frame again -- writing it into the recording twice, where loadFramesFromFile keys by
// stream id and the duplicate quietly replaces the original. Every later call has to
// return the original failure instead, whatever it is handed.
func TestFrameSplitterStaysFailedAfterEmitError(t *testing.T) {
	want := errors.New("disk full")
	one := frameV4(opOptions, 0x00, nil)
	two := frameV4(opQuery, 0x00, []byte("body"))

	var s FrameSplitter
	calls := 0
	emit := func([]byte) error {
		calls++
		return want
	}
	if err := s.Feed(one, emit); !errors.Is(err, want) {
		t.Fatalf("Feed error = %v, want %v", err, want)
	}

	// A second feed, of a perfectly good frame.
	if err := s.Feed(two, func([]byte) error {
		t.Error("emit called after the splitter failed")
		return nil
	}); !errors.Is(err, want) {
		t.Errorf("second Feed error = %v, want the original %v", err, want)
	}
	if calls != 1 {
		t.Errorf("emit called %d times, want 1", calls)
	}
}

// TestFeedReportsALatchedFailureForAnEmptyBuffer pins the one-shot contract against the
// call that carries nothing.
//
// consume is where the latch is normally read, and it does not run when there is nothing
// to consume, so an empty buffer used to come back nil -- reading as a healthy stream on
// a splitter whose framing was already in doubt. ConnectionReplayer.Write is the caller
// that would believe it: it has no failure gate of its own, so a zero-length write after
// a refused STARTUP answered (0, nil). SegmentSplitter.Feed already checked before its
// own loop, and startSegments' two refusals were never latched at all, so the four
// implementations disagreed about one documented contract.
func TestFeedReportsALatchedFailureForAnEmptyBuffer(t *testing.T) {
	want := errors.New("disk full")

	t.Run("FrameSplitter", func(t *testing.T) {
		var s FrameSplitter
		if err := s.Feed(frameV4(opOptions, 0x00, nil), func([]byte) error { return want }); !errors.Is(err, want) {
			t.Fatalf("Feed error = %v, want %v", err, want)
		}
		if err := s.Feed(nil, mustNotEmit(t)); !errors.Is(err, want) {
			t.Errorf("Feed(nil) = %v, want the original %v", err, want)
		}
	})

	for _, tc := range []struct {
		name string
		// drive puts a decoder into its failed state and returns the error it reported.
		// Each entry is a different way in: through a splitter, and through each of
		// startSegments' two refusals.
		drive func(t *testing.T, f *Framing, d *Decoder) error
		// pending is what Pending must report afterwards. It answers "is anything
		// buffered", not "has this failed", so it differs by path.
		pending bool
	}{
		{
			name:    "emit error",
			pending: true,
			drive: func(t *testing.T, f *Framing, d *Decoder) error {
				return d.Feed(frameV4(opOptions, 0x00, nil), func([]byte) error { return want })
			},
		},
		{
			name:    "switch mid-frame",
			pending: true,
			drive: func(t *testing.T, f *Framing, d *Decoder) error {
				if err := d.Feed(responseV5(opResult)[:5], mustNotEmit(t)); err != nil {
					t.Fatalf("Feed of a partial frame: %v", err)
				}
				f.ObserveResponse(responseV5(opReady))
				return d.Feed([]byte{0x00, 0x01, 0x02}, mustNotEmit(t))
			},
		},
		{
			name:    "compressed without a compressor",
			pending: false,
			drive: func(t *testing.T, f *Framing, d *Decoder) error {
				if err := f.ObserveRequest(startupWith([2]string{"COMPRESSION", "lz4"})); err != nil {
					t.Fatalf("ObserveRequest: %v", err)
				}
				f.ObserveResponse(responseV5(opReady))
				return d.Feed([]byte{0x00, 0x01, 0x02}, mustNotEmit(t))
			},
		},
	} {
		t.Run("Decoder/"+tc.name, func(t *testing.T) {
			f := NewFraming(nil)
			d := f.NewDecoder()

			first := tc.drive(t, f, d)
			if first == nil {
				t.Fatal("the decoder was not driven into a failure")
			}

			// The empty call reaches neither splitter, so only a latch can answer it.
			if err := d.Feed(nil, mustNotEmit(t)); !errors.Is(err, first) {
				t.Errorf("Feed(nil) = %v, want the latched %v", err, first)
			}

			// A later call carrying bytes has to report the same error *value*, not
			// another one just like it. errors.Is compares identity, which is what tells
			// a latch apart from a refusal recomputed per call: startSegments' two
			// refusals produce a fresh, equal-looking error every time they run.
			if err := d.Feed(frameV4(opOptions, 0x00, nil), mustNotEmit(t)); !errors.Is(err, first) {
				t.Errorf("a later Feed = %v, want the latched %v", err, first)
			}

			if got := d.Pending(); got != tc.pending {
				t.Errorf("Pending() = %v, want %v", got, tc.pending)
			}
		})
	}
}

// mustNotEmit returns an emit that fails the test if anything reaches it.
func mustNotEmit(t *testing.T) func([]byte) error {
	t.Helper()
	return func([]byte) error {
		t.Error("emit called; no frame should have come out")
		return nil
	}
}

// TestFrameSplitterReleasesOutsizedBuffer pins that the reused frame buffer does not
// pin an outlier for the life of the connection. A frame may declare up to
// frm.MaxFrameSize (256 MiB), and a recorded connection holds four of these splitters.
func TestFrameSplitterReleasesOutsizedBuffer(t *testing.T) {
	big := frameV4(opQuery, 0x00, make([]byte, maxRetainedFrame+1))
	small := frameV4(opQuery, 0x00, []byte("body"))

	var s FrameSplitter
	if got := collect(t, &s, big); len(got) != 1 {
		t.Fatalf("emitted %d frames, want 1", len(got))
	}
	if cap(s.frame) > maxRetainedFrame {
		t.Errorf("kept a %d-byte buffer after an outsized frame", cap(s.frame))
	}

	// Releasing it must not cost the splitter its ability to read the next frame.
	got := collect(t, &s, small)
	if len(got) != 1 || !bytes.Equal(got[0], small) {
		t.Errorf("after the release: emitted %d frames, want the small frame back", len(got))
	}

	// A frame at or below the bound is still reused, which is what keeps the replay
	// hot path allocation-free.
	before := cap(s.frame)
	if before == 0 || before > maxRetainedFrame {
		t.Fatalf("unexpected retained capacity %d", before)
	}
	if got := collect(t, &s, small); len(got) != 1 {
		t.Fatalf("emitted %d frames, want 1", len(got))
	}
	if cap(s.frame) != before {
		t.Errorf("buffer capacity changed from %d to %d across a small frame", before, cap(s.frame))
	}
}

// TestFrameSplitterZeroLengthBody pins that a body-less frame — OPTIONS, READY —
// completes on its header alone.
func TestFrameSplitterZeroLengthBody(t *testing.T) {
	frame := frameV4(opOptions, 0x00, nil)
	if len(frame) != FrameHeaderLen {
		t.Fatalf("expected a header-only frame, got %d bytes", len(frame))
	}

	var s FrameSplitter
	got := collect(t, &s, frame)
	if len(got) != 1 {
		t.Fatalf("emitted %d frames, want 1", len(got))
	}
	if s.Pending() {
		t.Error("a complete body-less frame left the splitter mid-frame")
	}
}

// TestFrameSplitterReservesTheDeclaredBody pins both halves of the reservation rule.
// A completed header reserves the body it declares, so a large frame is not regrown one
// read at a time -- but never more than the splitter would be willing to retain, because
// the declared length is the peer's word and nothing has arrived yet to back it up.
func TestFrameSplitterReservesTheDeclaredBody(t *testing.T) {
	t.Run("reserves the declared body", func(t *testing.T) {
		// Below maxRetainedFrame, so the reservation is the whole body and the buffer
		// must not move again while it arrives.
		frame := frameV4(opQuery, 0x00, make([]byte, maxRetainedFrame/2))

		var s FrameSplitter
		if err := s.Feed(frame[:FrameHeaderLen], mustNotEmit(t)); err != nil {
			t.Fatalf("Feed(header): %v", err)
		}
		reserved := cap(s.frame)
		if reserved < len(frame) {
			t.Fatalf("the header reserved %d bytes, want room for the declared %d", reserved, len(frame))
		}

		// Everything but the last byte, so the frame stays under construction and the
		// buffer is not released before it can be checked.
		rest := frame[FrameHeaderLen : len(frame)-1]
		for len(rest) > 0 {
			n := min(1024, len(rest))
			if err := s.Feed(rest[:n], mustNotEmit(t)); err != nil {
				t.Fatalf("Feed(body): %v", err)
			}
			if cap(s.frame) != reserved {
				t.Fatalf("the buffer regrew from %d to %d while the body arrived", reserved, cap(s.frame))
			}
			rest = rest[n:]
		}

		got := collect(t, &s, frame[len(frame)-1:])
		if len(got) != 1 || !bytes.Equal(got[0], frame) {
			t.Fatalf("emitted %d frames, want the whole frame back", len(got))
		}
	})

	t.Run("does not reserve what it will not retain", func(t *testing.T) {
		// A header alone, declaring the largest body the splitter admits, with nothing
		// behind it -- which is all it takes to pin the allocation for the life of the
		// connection, since the buffer is handed back only once a frame is emitted.
		header := frameV4(opQuery, 0x00, nil)
		header[5] = byte(frm.MaxFrameSize >> 24 & 0xFF)
		header[6] = byte(frm.MaxFrameSize >> 16 & 0xFF)
		header[7] = byte(frm.MaxFrameSize >> 8 & 0xFF)
		header[8] = byte(frm.MaxFrameSize & 0xFF)

		var s FrameSplitter
		if err := s.Feed(header, mustNotEmit(t)); err != nil {
			t.Fatalf("Feed(header): %v", err)
		}

		// Loose enough to survive slices.Grow rounding up to a size class, tight enough
		// that honouring the declared 256 MiB cannot pass.
		if want := 2 * (FrameHeaderLen + maxRetainedFrame); cap(s.frame) > want {
			t.Errorf("a header declaring %d bytes reserved %d, want at most %d", frm.MaxFrameSize, cap(s.frame), want)
		}
		if s.bodyLeft != frm.MaxFrameSize {
			t.Errorf("bodyLeft = %d, want the full declared %d -- only the reservation is capped", s.bodyLeft, frm.MaxFrameSize)
		}
	})

	t.Run("keeps the largest frame the bound admits", func(t *testing.T) {
		// maxRetainedFrame bounds the buffer, header included, so the largest frame kept
		// is FrameHeaderLen+bodyLen == maxRetainedFrame. One byte more and the buffer is
		// handed back -- which is the reservation cap doing nothing wrong: capping it at
		// maxRetainedFrame-FrameHeaderLen instead retains exactly the same frames and
		// merely regrows for the ones above the bound.
		frame := frameV4(opQuery, 0x00, make([]byte, maxRetainedFrame-FrameHeaderLen))
		if len(frame) != maxRetainedFrame {
			t.Fatalf("built a %d-byte frame, want exactly maxRetainedFrame", len(frame))
		}

		var s FrameSplitter
		if got := collect(t, &s, frame); len(got) != 1 {
			t.Fatalf("emitted %d frames, want 1", len(got))
		}
		if cap(s.frame) == 0 {
			t.Error("handed back the buffer for a frame that fits the bound exactly")
		}
		if cap(s.frame) > maxRetainedFrame {
			t.Errorf("kept a %d-byte buffer, want at most maxRetainedFrame (%d)", cap(s.frame), maxRetainedFrame)
		}
	})
}
