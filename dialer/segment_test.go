package dialer

import (
	"bytes"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/gocql/gocql/internal/segment"
)

// passthroughCompressor is a no-op compressor that still honours the append
// contract. Using a real lz4 would drag in a separate module; what these tests need
// is the compressed *layout*, not compression.
type passthroughCompressor struct{}

func (passthroughCompressor) AppendCompressed(dst, src []byte) ([]byte, error) {
	return append(dst, src...), nil
}

func (passthroughCompressor) AppendDecompressed(dst, src []byte, _ uint32) ([]byte, error) {
	return append(dst, src...), nil
}

// compressors is the set every decoder test runs against: both segment layouts.
func compressors() []struct {
	name string
	comp SegmentCompressor
} {
	return []struct {
		name string
		comp SegmentCompressor
	}{
		{name: "uncompressed", comp: nil},
		{name: "compressed", comp: passthroughCompressor{}},
	}
}

// mustSegment builds one segment, failing the test if the codec rejects it.
func mustSegment(t *testing.T, payload []byte, selfContained bool, comp SegmentCompressor) []byte {
	t.Helper()

	seg, err := segment.Append(nil, payload, selfContained, comp)
	if err != nil {
		t.Fatalf("build segment: %v", err)
	}
	return seg
}

// decode feeds chunks to a splitter and returns the frames it recovered.
func decode(t *testing.T, s *SegmentSplitter, chunks ...[]byte) [][]byte {
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

func TestSegmentSplitterSelfContained(t *testing.T) {
	one := frameV4(opOptions, 0x00, nil)
	two := frameV4(opQuery, 0x00, []byte("SELECT 1"))

	for _, cc := range compressors() {
		t.Run(cc.name, func(t *testing.T) {
			for _, tc := range []struct {
				name    string
				payload []byte
				want    [][]byte
			}{
				{name: "one frame", payload: one, want: [][]byte{one}},
				{
					name:    "several frames in one segment",
					payload: bytes.Join([][]byte{one, two, one}, nil),
					want:    [][]byte{one, two, one},
				},
				// Accepted, not refused: the driver's own processAllFramesInSegment
				// loops while bytes remain, so it reads an empty self-contained
				// segment as zero frames and no error. Refusing it here would fail
				// a stream gocql handles. The chain rule is the asymmetric one; see
				// Feed.
				{name: "no frames", payload: nil, want: nil},
			} {
				t.Run(tc.name, func(t *testing.T) {
					s := NewSegmentSplitter(cc.comp)
					got := decode(t, s, mustSegment(t, tc.payload, true, cc.comp))

					if len(got) != len(tc.want) {
						t.Fatalf("recovered %d frames, want %d", len(got), len(tc.want))
					}
					for i := range got {
						if !bytes.Equal(got[i], tc.want[i]) {
							t.Errorf("frame %d: got % X, want % X", i, got[i], tc.want[i])
						}
					}
					if s.Pending() {
						t.Error("splitter still holds something incomplete")
					}
				})
			}
		})
	}
}

// TestSegmentSplitterChain covers a frame split across chain segments, including the
// case that needs no special handling here but does in the driver's own reader: a
// frame whose 9-byte CQL header does not fit in the first segment.
func TestSegmentSplitterChain(t *testing.T) {
	frame := frameV4(opQuery, 0x00, bytes.Repeat([]byte{0x5A}, 500))

	for _, cc := range compressors() {
		t.Run(cc.name, func(t *testing.T) {
			for _, tc := range []struct {
				name  string
				split []int // payload sizes of the chain segments
			}{
				{name: "two segments", split: []int{200, len(frame) - 200}},
				{name: "header straddles the boundary", split: []int{4, len(frame) - 4}},
				{name: "one byte at a time in the header", split: []int{1, 1, 1, len(frame) - 3}},
				{name: "many segments", split: []int{50, 50, 50, 50, len(frame) - 200}},
			} {
				t.Run(tc.name, func(t *testing.T) {
					var stream []byte
					offset := 0
					for i, n := range tc.split {
						last := i == len(tc.split)-1
						stream = append(stream, mustSegment(t, frame[offset:offset+n], false, cc.comp)...)
						offset += n
						if last && offset != len(frame) {
							t.Fatalf("split covers %d of %d bytes", offset, len(frame))
						}
					}

					s := NewSegmentSplitter(cc.comp)
					got := decode(t, s, stream)

					if len(got) != 1 {
						t.Fatalf("recovered %d frames, want 1", len(got))
					}
					if !bytes.Equal(got[0], frame) {
						t.Errorf("frame did not survive the chain")
					}
					if s.Pending() {
						t.Error("splitter still holds something incomplete")
					}
				})
			}
		})
	}
}

// TestSegmentSplitterOversizedFrame covers a frame larger than one segment can carry,
// which is the only way the driver itself produces a chain.
func TestSegmentSplitterOversizedFrame(t *testing.T) {
	body := bytes.Repeat([]byte{0x42}, 2*segment.MaxPayloadSize+7)
	frame := frameV4(opQuery, 0x00, body)

	for _, cc := range compressors() {
		t.Run(cc.name, func(t *testing.T) {
			// Encode exactly as the driver would, then decode it back.
			stream, err := AppendSegmented(nil, frame, cc.comp)
			if err != nil {
				t.Fatalf("AppendSegmented: %v", err)
			}

			s := NewSegmentSplitter(cc.comp)
			got := decode(t, s, stream)

			if len(got) != 1 {
				t.Fatalf("recovered %d frames, want 1", len(got))
			}
			if !bytes.Equal(got[0], frame) {
				t.Error("a multi-segment frame did not survive the round trip")
			}
		})
	}
}

// TestSegmentSplitterArbitraryChunks pins that the splitter does not care where the
// stream is cut, which is the whole reason it buffers: it is fed whatever a socket
// read happened to return.
func TestSegmentSplitterArbitraryChunks(t *testing.T) {
	frames := [][]byte{
		frameV4(opOptions, 0x00, nil),
		frameV4(opQuery, 0x00, bytes.Repeat([]byte{0x11}, 400)),
		frameV4(opStartup, 0x00, []byte{0x00, 0x00}),
	}

	for _, cc := range compressors() {
		t.Run(cc.name, func(t *testing.T) {
			// A self-contained segment with two frames, then a chain for the third.
			var stream []byte
			stream = append(stream, mustSegment(t, bytes.Join(frames[:2], nil), true, cc.comp)...)
			stream = append(stream, mustSegment(t, frames[2][:5], false, cc.comp)...)
			stream = append(stream, mustSegment(t, frames[2][5:], false, cc.comp)...)

			for _, chunk := range []int{1, 2, 7, 13, len(stream)} {
				t.Run(fmt.Sprintf("chunks of %d", chunk), func(t *testing.T) {
					s := NewSegmentSplitter(cc.comp)
					var chunks [][]byte
					for i := 0; i < len(stream); i += chunk {
						end := min(i+chunk, len(stream))
						chunks = append(chunks, stream[i:end])
					}
					got := decode(t, s, chunks...)

					if len(got) != len(frames) {
						t.Fatalf("chunk %d: recovered %d frames, want %d", chunk, len(got), len(frames))
					}
					for i := range got {
						if !bytes.Equal(got[i], frames[i]) {
							t.Errorf("chunk %d: frame %d differs", chunk, i)
						}
					}
					if s.Pending() {
						t.Errorf("chunk %d: splitter still holds something incomplete", chunk)
					}
				})
			}
		})
	}
}

// TestSegmentSplitterRejectsMalformedStreams pins the four self-contained-flag rules.
// Each of these is a stream that concatenation alone would accept, re-aligning at the
// wrong offset and yielding plausible garbage.
func TestSegmentSplitterRejectsMalformedStreams(t *testing.T) {
	frame := frameV4(opQuery, 0x00, bytes.Repeat([]byte{0x33}, 100))
	other := frameV4(opOptions, 0x00, nil)

	for _, cc := range compressors() {
		t.Run(cc.name, func(t *testing.T) {
			for _, tc := range []struct {
				name   string
				stream func() []byte
				want   string
			}{
				{
					name: "self-contained segment interrupts a chain",
					stream: func() []byte {
						s := mustSegment(t, frame[:40], false, cc.comp)
						return append(s, mustSegment(t, other, true, cc.comp)...)
					},
					want: "while a split frame was still being reassembled",
				},
				{
					name: "self-contained segment ends mid-frame",
					stream: func() []byte {
						return mustSegment(t, frame[:40], true, cc.comp)
					},
					want: "ended in the middle of a frame",
				},
				{
					name: "chain segment with an empty payload",
					stream: func() []byte {
						s := mustSegment(t, frame[:40], false, cc.comp)
						return append(s, mustSegment(t, nil, false, cc.comp)...)
					},
					want: "made no progress",
				},
				{
					name: "chain carries two frames",
					stream: func() []byte {
						both := append(append([]byte(nil), other...), other...)
						s := mustSegment(t, both[:6], false, cc.comp)
						return append(s, mustSegment(t, both[6:], false, cc.comp)...)
					},
					want: "more than the one frame it was split from",
				},
				{
					name: "chain over-runs its frame",
					stream: func() []byte {
						overrun := append(append([]byte(nil), other...), 0x04, 0x00)
						s := mustSegment(t, overrun[:6], false, cc.comp)
						return append(s, mustSegment(t, overrun[6:], false, cc.comp)...)
					},
					want: "more than the one frame it was split from",
				},
			} {
				t.Run(tc.name, func(t *testing.T) {
					s := NewSegmentSplitter(cc.comp)
					err := s.Feed(tc.stream(), func([]byte) error { return nil })
					if err == nil {
						t.Fatalf("malformed stream was accepted")
					}
					if !strings.Contains(err.Error(), tc.want) {
						t.Errorf("error %q does not mention %q", err, tc.want)
					}
				})
			}
		})
	}
}

// TestSegmentSplitterRejectsCorruptChecksums pins that the codec's CRC checks are
// actually reached through the splitter, since a recording is a file on disk and can
// be truncated or edited.
func TestSegmentSplitterRejectsCorruptChecksums(t *testing.T) {
	frame := frameV4(opOptions, 0x00, nil)

	t.Run("header crc24", func(t *testing.T) {
		seg := mustSegment(t, frame, true, nil)
		seg[0] ^= 0xFF
		s := NewSegmentSplitter(nil)
		if err := s.Feed(seg, func([]byte) error { return nil }); err == nil {
			t.Error("a corrupt segment header was accepted")
		}
	})

	t.Run("payload crc32", func(t *testing.T) {
		seg := mustSegment(t, frame, true, nil)
		seg[segment.UncompressedHeaderSize] ^= 0xFF
		s := NewSegmentSplitter(nil)
		if err := s.Feed(seg, func([]byte) error { return nil }); err == nil {
			t.Error("a corrupt segment payload was accepted")
		}
	})
}

// TestSegmentSplitterPropagatesEmitError pins that a caller aborting stops the feed.
func TestSegmentSplitterPropagatesEmitError(t *testing.T) {
	want := errors.New("recording aborted")
	frame := frameV4(opOptions, 0x00, nil)

	s := NewSegmentSplitter(nil)
	seg := mustSegment(t, append(append([]byte(nil), frame...), frame...), true, nil)

	calls := 0
	err := s.Feed(seg, func([]byte) error {
		calls++
		return want
	})
	if !errors.Is(err, want) {
		t.Fatalf("Feed error = %v, want %v", err, want)
	}
	if calls != 1 {
		t.Errorf("emit called %d times, want 1", calls)
	}
}

// TestSegmentSplitterDecodesAHeaderOnce pins that a segment arriving in pieces has its
// header decoded when its bytes have all arrived, and not once per chunk.
//
// A corrupt header is the observable: the error has to appear on the feed that completes
// the header, and every later feed has to return that same error rather than decoding
// the bad header again.
func TestSegmentSplitterDecodesAHeaderOnce(t *testing.T) {
	seg := mustSegment(t, frameV4(opOptions, 0x00, nil), true, nil)
	seg[0] ^= 0xFF

	s := NewSegmentSplitter(nil)
	emit := func([]byte) error {
		t.Error("a frame was emitted from a segment with a corrupt header")
		return nil
	}

	// Every chunk short of the header decodes nothing, so there is nothing to reject.
	for i := 0; i < segment.UncompressedHeaderSize-1; i++ {
		if err := s.Feed(seg[i:i+1], emit); err != nil {
			t.Fatalf("feeding byte %d returned %v, want no error before the header is whole", i, err)
		}
	}

	first := s.Feed(seg[segment.UncompressedHeaderSize-1:segment.UncompressedHeaderSize], emit)
	if first == nil {
		t.Fatal("the corrupt header was accepted once all of its bytes had arrived")
	}

	// The rest of the segment, and then a perfectly good one.
	if err := s.Feed(seg[segment.UncompressedHeaderSize:], emit); err == nil || err.Error() != first.Error() {
		t.Errorf("later Feed error = %v, want the original %v", err, first)
	}
	if err := s.Feed(mustSegment(t, frameV4(opOptions, 0x00, nil), true, nil), emit); err == nil || err.Error() != first.Error() {
		t.Errorf("Feed of a valid segment after the failure = %v, want the original %v", err, first)
	}
}

// TestSegmentSplitterFeedReusesItsBuffers pins that decoding a segment costs no
// per-segment allocation.
//
// The codec takes an io.Reader, so the obvious spelling is a bytes.NewReader per header
// and per payload -- two allocations per segment on the path the replay benchmarks
// measure, in a decoder that otherwise reuses every buffer it has.
func TestSegmentSplitterFeedReusesItsBuffers(t *testing.T) {
	for _, cc := range compressors() {
		t.Run(cc.name, func(t *testing.T) {
			payload := bytes.Repeat([]byte{0x5A}, 8192)
			frame := frameV4(opQuery, 0x00, payload)
			seg := mustSegment(t, frame, true, cc.comp)

			s := NewSegmentSplitter(cc.comp)
			feed := func() {
				if err := s.Feed(seg, func([]byte) error { return nil }); err != nil {
					t.Fatalf("Feed: %v", err)
				}
			}

			// The first feed legitimately allocates the buffers the rest reuse.
			feed()

			// Allocation count via testing.AllocsPerRun, not a byte budget taken from
			// runtime.MemStats. TotalAlloc counts every allocation in the process
			// between the two reads -- the testing package's own, the runtime's
			// bookkeeping, and whatever -race or a -coverpkg=./... build adds on top --
			// so the threshold it needs is a guess that drifts with the toolchain and
			// the lane. AllocsPerRun attributes allocations to the call instead.
			//
			// Two allocations are the codec's steady state, not a leak: the
			// fixed-size header array and the CRC32 array in internal/segment escape
			// into io.ReadFull's interface call. The spelling this test exists to
			// catch made a bytes.Reader per header and per payload on top of those,
			// which lands at four.
			const allowedAllocs = 2
			if got := testing.AllocsPerRun(8, feed); got > allowedAllocs {
				t.Errorf("decoding a segment made %v allocations, want at most %d — a buffer is not being reused",
					got, allowedAllocs)
			}
		})
	}
}

// TestAppendSegmentedMatchesTheDriversSplitRule pins the encoder against the rule
// framer.prepareModernLayout uses, since the driver decodes what this produces. The
// boundary values are what a mismatch would show up at.
func TestAppendSegmentedMatchesTheDriversSplitRule(t *testing.T) {
	for _, cc := range compressors() {
		t.Run(cc.name, func(t *testing.T) {
			for _, size := range []int{0, 1, segment.MaxPayloadSize - 1, segment.MaxPayloadSize, segment.MaxPayloadSize + 1, 2*segment.MaxPayloadSize + 7} {
				payload := bytes.Repeat([]byte{0x5A}, size)

				// The reference: chain segments of exactly the maximum size while more
				// than one is needed, then a remainder, self-contained only if nothing
				// split.
				var want []byte
				src := payload
				selfContained := true
				for len(src) > segment.MaxPayloadSize {
					want = append(want, mustSegment(t, src[:segment.MaxPayloadSize], false, cc.comp)...)
					src = src[segment.MaxPayloadSize:]
					selfContained = false
				}
				want = append(want, mustSegment(t, src, selfContained, cc.comp)...)

				got, err := AppendSegmented(nil, payload, cc.comp)
				if err != nil {
					t.Fatalf("size %d: %v", size, err)
				}
				if !bytes.Equal(got, want) {
					t.Errorf("size %d: encoded %d bytes, want %d", size, len(got), len(want))
				}
				if bound := segment.EncodedSize(size, cc.comp != nil); len(got) > bound {
					t.Errorf("size %d: encoded %d bytes, segment.EncodedSize reported %d", size, len(got), bound)
				}
			}
		})
	}
}

// TestAppendSegmentedAppends pins that the encoder extends dst rather than replacing
// it, so a caller can build a stream of several frames in one buffer.
func TestAppendSegmentedAppends(t *testing.T) {
	first := frameV4(opOptions, 0x00, nil)
	second := frameV4(opQuery, 0x00, []byte("SELECT 1"))

	stream, err := AppendSegmented(nil, first, nil)
	if err != nil {
		t.Fatal(err)
	}
	stream, err = AppendSegmented(stream, second, nil)
	if err != nil {
		t.Fatal(err)
	}

	s := NewSegmentSplitter(nil)
	got := decode(t, s, stream)

	if len(got) != 2 {
		t.Fatalf("recovered %d frames, want 2", len(got))
	}
	if !bytes.Equal(got[0], first) || !bytes.Equal(got[1], second) {
		t.Error("frames did not survive being appended into one stream")
	}
}

// TestSegmentSplitterReleasesOutsizedBuffer pins that the accumulation buffer does not
// pin an outlier for the life of the connection. Feed appends whatever it is handed, so
// one large read buffer or Write is enough to leave it holding far more than decoding a
// segment needs -- in each of the three splitters a recorded connection builds.
func TestSegmentSplitterReleasesOutsizedBuffer(t *testing.T) {
	// Several maximum-size segments arriving as one chunk.
	frame := frameV4(opQuery, 0x00, bytes.Repeat([]byte{0x7E}, 3*segment.MaxPayloadSize))
	stream, err := AppendSegmented(nil, frame, nil)
	if err != nil {
		t.Fatalf("AppendSegmented: %v", err)
	}

	s := NewSegmentSplitter(nil)
	if got := decode(t, s, stream); len(got) != 1 || !bytes.Equal(got[0], frame) {
		t.Fatalf("emitted %d frames, want the one that was split", len(got))
	}

	bound := segment.HeaderSize(false) + segment.MaxPayloadSize + segment.Crc32Size
	if cap(s.buf) > bound {
		t.Errorf("kept a %d-byte buffer, want at most one maximum-size segment (%d)", cap(s.buf), bound)
	}

	// Releasing it must not cost the splitter its ability to read the next segment.
	small := frameV4(opQuery, 0x00, []byte("body"))
	if got := decode(t, s, mustSegment(t, small, true, nil)); len(got) != 1 || !bytes.Equal(got[0], small) {
		t.Errorf("after the release: emitted %d frames, want the small frame back", len(got))
	}

	// A buffer within the bound is kept, which is what keeps the decode path
	// allocation-free.
	before := cap(s.buf)
	if before == 0 || before > bound {
		t.Fatalf("unexpected retained capacity %d", before)
	}
	if got := decode(t, s, mustSegment(t, small, true, nil)); len(got) != 1 {
		t.Fatalf("emitted %d frames, want 1", len(got))
	}
	if cap(s.buf) != before {
		t.Errorf("buffer capacity changed from %d to %d across a small segment", before, cap(s.buf))
	}
}
