package dialer

import (
	"bytes"
	"errors"
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
		{name: "header exactly", n: headerLen, want: true},
		{name: "mid-body", n: headerLen + 3, want: true},
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
	if len(frame) != headerLen {
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
