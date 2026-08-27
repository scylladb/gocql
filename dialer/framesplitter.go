package dialer

import (
	"fmt"
	"slices"

	frm "github.com/gocql/gocql/internal/frame"
)

// FrameHeaderLen is the CQL frame header these dialers slice on: version, flags, a
// 2-byte stream id, the opcode and the 4-byte body length.
//
// It is fixed at the v3+ layout. Negotiation lands on v4 (discoverProtocol) and v5
// keeps the same header, so the 1-byte stream id of v1/v2 only appears if a caller
// pins ProtoVersion to one of them — which these dialers have never handled, here or
// in the offset math they share with GetFrameHash (scylladb/gocql#1022).
const FrameHeaderLen = 9

// FrameBodyLen reports the body length that b's header declares, and whether b holds a
// complete header to read it from.
//
// No bound is placed on the value, because the two callers want different answers to an
// impossible one: FrameSplitter.consume refuses it by name, since a stream whose framing
// is in doubt is worth failing loudly, while the replayer only asks whether a record is
// exactly one frame, which an absurd length answers on its own. A length that overflows
// int comes back negative rather than wrapping into something plausible.
func FrameBodyLen(b []byte) (int, bool) {
	if len(b) < FrameHeaderLen {
		return 0, false
	}
	return int(b[5])<<24 | int(b[6])<<16 | int(b[7])<<8 | int(b[8]), true
}

// latch makes a stream one-shot: the first failure is kept and returned by every later
// call, including one that carries nothing.
//
// A stream that failed a framing rule cannot be resumed without re-feeding the bytes
// that failed, so both splitters here want the behaviour and each documents its own
// rules for reaching it. Embedding keeps one implementation of the behaviour itself:
// the copies this replaced had already disagreed once about whether an empty Feed
// reports a latched failure.
//
// It leads the struct it is embedded in. It holds the only pointer-bearing field, and
// the fieldalignment vet check wants those contiguous.
type latch struct {
	failed error
}

// fail records err as the failure that ended the stream and returns it. Only the first
// failure is kept.
func (l *latch) fail(err error) error {
	if l.failed == nil {
		l.failed = err
	}
	return l.failed
}

// failure returns the error that ended the stream, if one did.
func (l *latch) failure() error {
	return l.failed
}

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
	// latch makes the splitter one-shot: see Feed. It leads the struct for the reason
	// given there, which matters more here than elsewhere -- this type is embedded in
	// the recorder's and replayer's connections, so its layout is theirs too.
	latch
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
//
// It governs the frame buffer only. A SegmentSplitter bounds its own accumulation
// buffer at one maximum-size segment, which is larger; see Feed there for why.
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
// "Every later call" includes one with nothing in it. consume is where the latch is
// normally read, and it does not run when there is nothing to consume, so an empty
// buffer would otherwise come back nil and read as a healthy stream — which is how
// ConnectionReplayer.Write, the one caller with no failure gate of its own, would
// answer a zero-length write after ObserveRequest had already refused the connection.
// SegmentSplitter.Feed checks before its own loop for the same reason.
func (s *FrameSplitter) Feed(b []byte, emit func(frame []byte) error) error {
	if err := s.failure(); err != nil {
		return err
	}
	for len(b) > 0 {
		taken, err := s.consume(b, emit)
		if err != nil {
			return err
		}
		b = b[taken:]
	}
	return nil
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
	if err := s.failure(); err != nil {
		return 0, err
	}

	// While the header is short the body length is still unknown, so take only what
	// completes the header: anything past it may belong to the next frame.
	headerShort := len(s.frame) < FrameHeaderLen
	want := s.bodyLeft
	if headerShort {
		want = FrameHeaderLen - len(s.frame)
	}

	taken := min(want, len(b))
	s.frame = append(s.frame, b[:taken]...)

	if headerShort {
		bodyLen, ok := FrameBodyLen(s.frame)
		if !ok {
			return taken, nil
		}

		// The length is the peer's to choose, or a recording file's. Without this
		// the splitter would append toward a declared 2 GiB body, one read at a
		// time, on nothing more than a corrupt header. The driver's own reader
		// bounds the same field the same way.
		if bodyLen < 0 || bodyLen > frm.MaxFrameSize {
			return taken, s.fail(fmt.Errorf("gocql/dialer: frame declares an invalid body length %d", bodyLen))
		}

		// The whole body is owed and its length is now known, so reserve it in one
		// step. Left to append the frame regrows geometrically -- from nil whenever
		// the previous frame was handed back for exceeding maxRetainedFrame -- which
		// on a large frame is dozens of reallocations, each copying everything read
		// so far.
		//
		// The reservation is capped at what this splitter is willing to retain. The
		// length is the peer's to choose and nothing has arrived to back it up yet, so
		// a lone header declaring frm.MaxFrameSize would otherwise pin 256 MiB before a
		// single body byte -- in each of the four splitters a recorded connection builds
		// (both of the recorder's decoders, the replayer's request decoder, and the one
		// inside a SegmentSplitter) -- and hold it until a frame that never completes
		// does. Past the cap the buffer grows with the bytes actually delivered, which is
		// what this did before it reserved anything, and costs nothing extra on a frame
		// that size: it is handed back after emit regardless. bodyLeft keeps the full
		// declared length; only the reservation is bounded.
		//
		// Capped at the constant itself, not the constant less FrameHeaderLen: both
		// retain the same frames, because maxRetainedFrame bounds the buffer with the
		// header in it. Capping lower only under-reserves a frame that is about to be
		// released anyway.
		s.frame = slices.Grow(s.frame, min(bodyLen, maxRetainedFrame))
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
