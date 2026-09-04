package dialer

import (
	"bytes"
	"fmt"

	"github.com/gocql/gocql/internal/segment"
)

// SegmentCompressor compresses and decompresses protocol v5 transport segment
// payloads.
//
// It is structurally identical to gocql.SegmentCompressor, and a value of that type
// satisfies this directly. It is redeclared rather than imported because this package
// does not depend on the driver, and because the only compressor that implements it —
// lz4 — lives in a separate module that the driver cannot construct for itself. A
// caller that wants compressed v5 recordings therefore has to hand one in.
//
// A nil SegmentCompressor means the uncompressed segment layout. Note that a typed
// nil is not nil as an interface, so pass an untyped nil rather than a nil variable
// of a concrete compressor type.
type SegmentCompressor interface {
	// AppendCompressed compresses src and appends the compressed bytes to dst.
	AppendCompressed(dst, src []byte) ([]byte, error)

	// AppendDecompressed decompresses src (whose decompressed size is supplied
	// out-of-band as decompressedLength) and appends the result to dst.
	AppendDecompressed(dst, src []byte, decompressedLength uint32) ([]byte, error)
}

// SegmentSplitter recovers CQL frames from a protocol v5 transport-segment byte
// stream delivered in arbitrary chunks.
//
// From v5 the driver wraps each CQL frame in one or more segments: a header carrying
// a 17-bit payload length and a self-contained flag, a header CRC24, the payload
// (optionally compressed) and a payload CRC32. A segment is either self-contained,
// carrying one or more complete frames, or a link in a chain carrying a slice of one
// large frame.
//
// Recovering the frames needs no separate reassembly path for the two shapes. The
// spec (section 2) gives a split frame its own sequence of segments, so a chain never
// shares a segment with anything else, and the ordered concatenation of every
// payload is exactly the frame stream. Feeding that concatenation to a FrameSplitter,
// which already tolerates arbitrary chunk boundaries, therefore handles both several
// frames in one self-contained segment and a frame whose 9-byte header straddles a
// segment boundary.
//
// What concatenation alone loses is the ability to notice a stream that does not mean
// what it says, so the flag is checked rather than ignored — see Feed. Without those
// checks a corrupt or truncated chain does not fail, it silently re-aligns at the
// wrong offset and yields plausible garbage, which in a recording is worse than an
// error.
// The field order is dictated by the fieldalignment vet check, which wants the
// pointer-bearing fields contiguous, so it does not group them by what they do.
type SegmentSplitter struct {
	comp SegmentCompressor
	// latch makes the splitter one-shot, like the frame splitter it wraps: see Feed.
	// A failure here means the segment stream cannot be trusted from this point --
	// the buffer is positioned at a segment that did not decode, or whose payload was
	// already handed on before the check that rejected it -- so resuming would re-feed
	// those bytes.
	latch
	// frames turns the concatenated payloads back into CQL frames.
	frames FrameSplitter
	// scratch is the payload buffer the codec reads into, reused across segments.
	scratch segment.Scratch
	// buf holds the bytes that do not yet add up to a whole segment.
	buf []byte
	// rdr is the io.Reader the codec reads through, pointed at a window of buf. It is
	// a field so that decoding a segment costs no allocation: the codec takes an
	// io.Reader, and a fresh bytes.Reader per header and per payload would be two
	// allocations per segment on the path the replay benchmarks measure.
	rdr bytes.Reader
	// hdr is the header of the segment being accumulated, decoded once its bytes have
	// all arrived; hdrValid says whether it holds one. Keeping it means a segment that
	// arrives in ten chunks verifies its header CRC24 once rather than ten times, and
	// a corrupt header is reported once rather than per chunk.
	hdr      segment.Header
	hdrValid bool
}

// NewSegmentSplitter returns a splitter for a stream in the layout implied by comp:
// the compressed segment layout when it is non-nil, the uncompressed one otherwise.
func NewSegmentSplitter(comp SegmentCompressor) *SegmentSplitter {
	return &SegmentSplitter{comp: comp}
}

// compressed reports whether the stream uses the compressed segment layout.
func (s *SegmentSplitter) compressed() bool {
	return s.comp != nil
}

// Pending reports whether the splitter holds anything incomplete: a partial segment,
// or a chain whose frame has not finished arriving. A stream that ends here ended
// mid-structure.
func (s *SegmentSplitter) Pending() bool {
	return len(s.buf) > 0 || s.frames.Pending()
}

// Feed consumes b, calling emit once for each CQL frame it recovers.
//
// The frame handed to emit is only valid for the duration of the call.
//
// Four rules govern the self-contained flag, and together they are what stops a
// damaged stream from re-aligning silently:
//
//   - A self-contained segment may not arrive while a chain is in progress. This is
//     the same rejection the driver's own reader makes, and the case that matters
//     most: without it an over-running chain does not error, it resumes reading
//     frames at the wrong offset.
//   - A self-contained segment must not end mid-frame. It promised whole frames.
//   - A chain segment must carry a non-empty payload, so a peer cannot drive an
//     endless chain that never progresses. An empty *self-contained* segment is
//     accepted and yields nothing, which looks like the same defect but is not: the
//     driver's own reader accepts it too (processAllFramesInSegment loops while bytes
//     remain, so an empty payload is zero frames and no error), and a splitter that
//     refused what the connection it records accepts would fail a stream gocql itself
//     handles. Only a chain is owed progress, and recvSplitFrame enforces that there.
//   - A chain must yield exactly one frame and nothing after it, since a split frame
//     gets a sequence of segments to itself. A chain here is bounded by the frame it
//     carries, not by the run of continuation segments: the segment that completes a
//     frame ends the chain, so a following continuation segment begins a new one. That
//     matches the driver's own reader, which returns from recvSplitFrame as soon as the
//     frame is whole -- a chain of one segment included.
//
// A splitter is one-shot: any failure is remembered and returned by every later call,
// because a stream that has failed one of those rules cannot be resumed without
// re-feeding the bytes that failed.
func (s *SegmentSplitter) Feed(b []byte, emit func(frame []byte) error) error {
	if err := s.failure(); err != nil {
		return err
	}
	s.buf = append(s.buf, b...)

	headerSize := segment.HeaderSize(s.compressed())

	for {
		if !s.hdrValid {
			if len(s.buf) < headerSize {
				return nil
			}
			s.rdr.Reset(s.buf[:headerSize])
			hdr, err := segment.ReadHeader(&s.rdr, s.compressed())
			if err != nil {
				return s.fail(fmt.Errorf("gocql/dialer: failed to decode segment header: %w", err))
			}
			s.hdr, s.hdrValid = hdr, true
		}

		total := headerSize + s.hdr.PayloadLen + segment.Crc32Size
		if len(s.buf) < total {
			return nil
		}

		chainInProgress := s.frames.Pending()
		if s.hdr.IsSelfContained && chainInProgress {
			return s.fail(fmt.Errorf("gocql/dialer: received a self-contained segment while a split frame was still being reassembled"))
		}
		if !s.hdr.IsSelfContained && s.hdr.PayloadLen == 0 {
			return s.fail(fmt.Errorf("gocql/dialer: segment chain made no progress (empty payload)"))
		}

		s.rdr.Reset(s.buf[headerSize:total])
		payload, err := segment.ReadPayload(&s.rdr, s.hdr, s.comp, &s.scratch)
		if err != nil {
			return s.fail(fmt.Errorf("gocql/dialer: failed to decode segment payload: %w", err))
		}

		emitted := 0
		if err := s.frames.Feed(payload, func(frame []byte) error {
			emitted++
			return emit(frame)
		}); err != nil {
			return s.fail(err)
		}

		if s.hdr.IsSelfContained && s.frames.Pending() {
			return s.fail(fmt.Errorf("gocql/dialer: self-contained segment ended in the middle of a frame"))
		}
		if !s.hdr.IsSelfContained && (emitted > 1 || (emitted == 1 && s.frames.Pending())) {
			return s.fail(fmt.Errorf("gocql/dialer: segment chain carried more than the one frame it was split from"))
		}

		// Drop the consumed segment, keeping whatever came after it. Copying the
		// remainder down rather than re-slicing keeps the buffer from growing without
		// bound over the life of a connection.
		s.buf = append(s.buf[:0], s.buf[total:]...)

		// Copying down bounds the length, not the capacity, and Feed appends whatever
		// it is handed: one large Write or read buffer leaves this holding far more
		// than decoding a segment ever needs, for the life of the connection, in each
		// of the three of these a recorded connection builds. So an outsized buffer is
		// handed back once it has drained, the way the frame splitter inside this one
		// hands back an outsized frame.
		//
		// The bound is one whole segment at the largest the format allows, not
		// maxRetainedFrame: a segment payload is 17 bits, so a stream of maximum-size
		// segments would sit just above that constant and reallocate per segment. For
		// the same reason segment.Scratch is left alone -- both of its buffers are
		// bounded by that payload length already, and releasing them would cost a
		// reallocation per outsized segment on the path the replay benchmarks measure.
		if len(s.buf) == 0 && cap(s.buf) > headerSize+segment.MaxPayloadSize+segment.Crc32Size {
			s.buf = nil
		}
		s.hdrValid = false
	}
}

// AppendSegmented encodes one CQL frame as a transport-segment chain appended to dst
// and returns the extended slice.
//
// The split rule mirrors framer.prepareModernLayout exactly, because a recording is
// replayed to a driver that decodes what that function produces: fixed maximum-size
// chain segments while more than one is needed, then a remainder, and the whole frame
// is self-contained only when no split happened. A frame of exactly the maximum
// payload size is therefore one self-contained segment, not a chain.
//
// On error the returned slice must not be used; the compressed layout can fail after
// having written into dst.
func AppendSegmented(dst, frame []byte, comp SegmentCompressor) ([]byte, error) {
	var err error

	// Reserve the whole encoding up front. Each segment.Append below otherwise grows
	// dst by append alone, so a frame spanning several segments re-grows and copies
	// the buffer once per doubling -- on the path the replay benchmarks measure, and
	// for the replayer on the first large response of every connection, since that is
	// where its reused buffer gets its capacity. framer.prepareModernLayout sizes its
	// wire buffer from the same function for the same reason.
	if need := segment.EncodedSize(len(frame), comp != nil); cap(dst)-len(dst) < need {
		grown := make([]byte, len(dst), len(dst)+need)
		copy(grown, dst)
		dst = grown
	}

	src := frame
	selfContained := true

	for len(src) > segment.MaxPayloadSize {
		if dst, err = segment.Append(dst, src[:segment.MaxPayloadSize], false, comp); err != nil {
			return nil, err
		}
		src = src[segment.MaxPayloadSize:]
		selfContained = false
	}

	return segment.Append(dst, src, selfContained, comp)
}
