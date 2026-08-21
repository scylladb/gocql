package dialer

import (
	"fmt"

	frm "github.com/gocql/gocql/internal/frame"
)

// headerLen is the CQL frame header the splitter slices on: version, flags, a
// 2-byte stream id, the opcode and the 4-byte body length.
//
// It is fixed at the v3+ layout. Negotiation lands on v4 (discoverProtocol) and v5
// keeps the same header, so the 1-byte stream id of v1/v2 only appears if a caller
// pins ProtoVersion to one of them — which these dialers have never handled, here or
// in the offset math they share with GetFrameHash.
const headerLen = 9

// FrameSplitter reassembles whole CQL frames from a byte stream delivered in
// arbitrary chunks.
//
// Neither side of a connection delivers one frame per call. The driver's read path
// fills a bufio.Reader, so a single read can carry several frames or end in the
// middle of one, and either shape has to come back out as the frames that went in.
// The write side happens to deliver one frame per call today, but only by accident
// of how net.Buffers.WriteTo dispatches (see the note in the replayer), so it is fed
// through here too rather than trusting that.
//
// On protocol v5 the bytes arriving from the socket are transport segments rather
// than frames. A SegmentSplitter unwraps those first and feeds their payloads here;
// this type is only ever handed a CQL frame stream.
type FrameSplitter struct {
	// failed is the error that ended this splitter, if one did. A splitter is
	// one-shot: see Feed. It leads the struct only to keep the pointer-bearing fields
	// contiguous, which the fieldalignment vet check requires -- and this type is
	// embedded in the recorder's and replayer's connections, so its layout is theirs
	// too.
	failed error
	// frame is the frame under construction, complete up to len(frame).
	frame []byte
	// bodyLeft counts the body bytes still owed to frame, and is meaningful only
	// once the header is complete — until then the declared length has not been
	// read.
	bodyLeft int
}

// maxRetainedFrame bounds the buffer kept between frames.
//
// The buffer is reused so the common case costs no allocation, but consume admits any
// body length the peer declares, up to frm.MaxFrameSize — 256 MiB. Reusing that for
// the life of the connection would pin it in four places at once on a recorded
// connection (both of the recorder's decoders, the replayer's request decoder, and the
// splitter inside a SegmentSplitter), so an outlier is handed back instead. The driver
// does the same for its own framer buffers, which it releases and reallocates rather
// than growing without bound.
const maxRetainedFrame = 64 << 10

// Feed consumes b, calling emit once for each frame it completes.
//
// The frame handed to emit is only valid for the duration of the call: the buffer is
// reused for the next frame. A caller that keeps it must copy it.
//
// A frame is emitted before the bytes that follow it are looked at, so a caller that
// stops at the first error stops with the stream positioned exactly after the frame
// that failed.
//
// A splitter is one-shot: any failure, including one returned by emit, is remembered
// and returned by every later call. There is no resuming a stream whose framing is in
// doubt — a recording assembled past a failure is worthless, and the alternative is
// worse than useless. A frame that fails in emit is complete but unrecorded, and the
// obvious "leave it buffered for a retry" leaves the splitter owing zero body bytes,
// so the next call would consume nothing, emit that same frame a second time, and
// write it into the recording twice.
func (s *FrameSplitter) Feed(b []byte, emit func(frame []byte) error) error {
	for len(b) > 0 {
		taken, err := s.consume(b, emit)
		if err != nil {
			return err
		}
		b = b[taken:]
	}
	return nil
}

// fail records err as the failure that ended this splitter and returns it.
func (s *FrameSplitter) fail(err error) error {
	if s.failed == nil {
		s.failed = err
	}
	return s.failed
}

// Pending reports whether part of a frame is buffered, i.e. whether the stream
// stopped mid-frame. A segment chain that ends here has not delivered what it
// promised.
func (s *FrameSplitter) Pending() bool {
	return len(s.frame) > 0
}

// consume appends the prefix of b belonging to the frame in progress and, once that
// frame is whole, emits it and resets for the next one. It returns how many bytes it
// took, which is all of b unless b runs on into the following frame.
func (s *FrameSplitter) consume(b []byte, emit func(frame []byte) error) (int, error) {
	if s.failed != nil {
		return 0, s.failed
	}

	// While the header is short the body length is still unknown, so take only what
	// completes the header: anything past it may belong to the next frame.
	headerShort := len(s.frame) < headerLen
	want := s.bodyLeft
	if headerShort {
		want = headerLen - len(s.frame)
	}

	taken := min(want, len(b))
	s.frame = append(s.frame, b[:taken]...)

	if headerShort {
		if len(s.frame) < headerLen {
			return taken, nil
		}
		bodyLen := int(s.frame[5])<<24 | int(s.frame[6])<<16 | int(s.frame[7])<<8 | int(s.frame[8])

		// The length is the peer's to choose, or a recording file's. Without this
		// the splitter would append toward a declared 2 GiB body, one read at a
		// time, on nothing more than a corrupt header. The driver's own reader
		// bounds the same field the same way.
		if bodyLen < 0 || bodyLen > frm.MaxFrameSize {
			return taken, s.fail(fmt.Errorf("gocql/dialer: frame declares an invalid body length %d", bodyLen))
		}
		s.bodyLeft = bodyLen
	} else {
		s.bodyLeft -= taken
	}

	if s.bodyLeft > 0 {
		return taken, nil
	}

	if err := emit(s.frame); err != nil {
		return taken, s.fail(err)
	}

	if cap(s.frame) > maxRetainedFrame {
		s.frame = nil
	} else {
		s.frame = s.frame[:0]
	}
	s.bodyLeft = 0
	return taken, nil
}
