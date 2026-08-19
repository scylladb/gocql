package dialer

import (
	"fmt"
	"reflect"
	"strings"
	"sync/atomic"
)

// Framing tracks how one connection's bytes are framed, and hands out a decoder per
// direction.
//
// Up to a point in the handshake both directions carry bare CQL frames. Past it, on
// protocol v5, both carry transport segments. The frame bytes do not say which side of
// that line they are on, and on a compressed connection they do not say how large a
// segment header is either, so both facts are latched from the handshake as it goes
// past.
//
// # Where the line is
//
// It is one frame earlier than "the handshake finished", which is the detail worth
// being careful about: getting it wrong by a single frame corrupts every authenticated
// recording.
//
// The driver sets its startupCompleted flag as soon as READY *or* AUTHENTICATE is
// received, and then writes AUTH_RESPONSE through the same gate that decides whether to
// segment. So authentication happens after the switch, not before it:
//
//	driver writes, unsegmented:  OPTIONS, STARTUP
//	driver writes, segmented:    AUTH_RESPONSE onward
//	server writes, unsegmented:  SUPPORTED, READY | AUTHENTICATE
//	server writes, segmented:    AUTH_SUCCESS | AUTH_CHALLENGE onward
//
// native_protocol_v5.spec section 2.3.1 says the same: after sending READY or
// AUTHENTICATE in response to a STARTUP, the server begins framing everything further.
//
// # Why no read or write can straddle it
//
// The switch is driven by a response frame, and observed by both directions, so it has
// to be impossible for a single Read or Write to contain bytes from both sides of it.
//
// On the read side the server sends nothing after READY or AUTHENTICATE until the
// client's next request, so the bytes carrying that frame cannot also carry the first
// segment. On the write side the startup coordinator is strictly
// request-response-request, so AUTH_RESPONSE is written only after AUTHENTICATE was
// read, by which time the latch has flipped.
//
// # Concurrency
//
// A connection's reads and writes run on different goroutines — the driver's serve()
// loop and whichever goroutine is executing a query — so the latches are atomic. The
// decoders are not: each belongs to one direction, and is only ever touched by that
// direction's goroutine.
type Framing struct {
	// comp is the compressor supplied by whoever built the dialer. Immutable.
	comp SegmentCompressor
	// algorithm is the compression algorithm the STARTUP named, kept for the error
	// messages that have to name it. Written by ObserveRequest before compressed is
	// stored and read only after compressed reports true, so the atomic store below
	// publishes it.
	algorithm string
	// compressed latches when the STARTUP request names a compression algorithm,
	// which is what decides the segment header layout.
	compressed atomic.Bool
	// segmented latches when the handshake passes the point described above.
	segmented atomic.Bool
}

// NewFraming returns the framing state for one connection. comp may be nil, in which
// case a connection that turns out to have negotiated compression is an error rather
// than a stream silently decoded with the wrong header size.
//
// A typed-nil compressor -- a nil *lz4.LZ4Compressor handed over as the interface --
// is normalized to that same nil. It would pass every comp == nil check and then
// panic on its first method call, deep in segment decoding, instead of failing as
// the missing dependency it is (ErrSegmentCompressorRequired).
func NewFraming(comp SegmentCompressor) *Framing {
	if v := reflect.ValueOf(comp); v.Kind() == reflect.Pointer && v.IsNil() {
		comp = nil
	}
	return &Framing{comp: comp}
}

// ObserveRequest inspects a request frame for the handshake facts carried by requests,
// and reports a negotiated compression this package cannot record or replay.
//
// Two such cases exist, and both are refused here at the STARTUP rather than left to
// surface later as something else:
//
//   - Compression below protocol v5, which compresses frame *bodies* rather than
//     transport segments. Every recorded body would then be compressed bytes, which
//     GetFrameHash cannot walk; it would fall back to hashing the whole frame, default
//     timestamp included, so the hash would differ on every run and replay would fail
//     to find a response for a request it has. Supporting it means decompressing frame
//     bodies, which is a different feature from segment compression.
//   - An algorithm that is not the one the supplied compressor implements. The segment
//     CRCs cover the compressed payload, so they pass either way and the mismatch
//     surfaces as corrupt CQL frames. Only compressors that report a Name can be
//     checked -- the interface deliberately requires just the two Append methods --
//     which is enough to catch the real ones.
func (f *Framing) ObserveRequest(frame []byte) error {
	algorithm, ok := StartupCompression(frame)
	if !ok || algorithm == "" {
		// An option present but empty names nothing to compress with, and the driver
		// never writes one; treating it as compression would pick the wrong header
		// size for an uncompressed stream.
		return nil
	}

	if !FrameIsProtoV5OrNewer(frame) {
		return fmt.Errorf("gocql/dialer: connection negotiated %q compression at protocol v%d, which compresses frame bodies rather than transport segments; the record and replay dialers support compression only from protocol v5",
			algorithm, frame[0]&protoVersionMask)
	}

	if named, ok := f.comp.(interface{ Name() string }); ok && !strings.EqualFold(named.Name(), algorithm) {
		return fmt.Errorf("gocql/dialer: connection negotiated %q compression but the dialer was given a %q compressor",
			algorithm, named.Name())
	}

	f.algorithm = algorithm
	f.compressed.Store(true)
	return nil
}

// ObserveResponse inspects a response frame and flips the framing latch once the
// handshake reaches the point where both directions switch to transport segments.
//
// Callers pass every response frame: the recorder the ones it reads from the server,
// the replayer the ones it serves from a recording. Either way it is the same frame in
// the same position, which is what lets one implementation serve both.
func (f *Framing) ObserveResponse(frame []byte) {
	if f.segmented.Load() || len(frame) < 5 {
		return
	}

	// Only v5 and newer segment anything. A v4 connection never switches, so the
	// latch stays false for its whole life and every decoder stays a FrameSplitter.
	if !FrameIsProtoV5OrNewer(frame) {
		return
	}

	switch frameOp(frame[3+headerShift(frame)]) {
	case opReady, opAuthenticate:
		f.segmented.Store(true)
	}
}

// Segmented reports whether the connection has passed the point where both directions
// carry transport segments.
func (f *Framing) Segmented() bool {
	return f.segmented.Load()
}

// Compressed reports whether the connection negotiated compression, and therefore
// which of the two segment header layouts is on the wire.
func (f *Framing) Compressed() bool {
	return f.compressed.Load()
}

// segmentCompressor returns the compressor the negotiated layout needs.
//
// A connection that negotiated compression without one supplied is refused here. The
// alternative would be to fall back to the uncompressed layout, which differs in
// header size, so every subsequent offset would be wrong and the failure would surface
// as corrupt frames rather than as the missing dependency it is.
func (f *Framing) segmentCompressor() (SegmentCompressor, error) {
	if !f.compressed.Load() {
		return nil, nil
	}
	if f.comp == nil {
		return nil, fmt.Errorf("%w (the connection negotiated %q)", ErrSegmentCompressorRequired, f.algorithm)
	}
	return f.comp, nil
}

// EncodeResponse wraps a bare CQL response frame for delivery to the driver, in
// whatever framing the connection has reached.
func (f *Framing) EncodeResponse(dst, frame []byte) ([]byte, error) {
	if !f.Segmented() {
		return append(dst, frame...), nil
	}

	comp, err := f.segmentCompressor()
	if err != nil {
		return nil, err
	}
	return AppendSegmented(dst, frame, comp)
}

// NewDecoder returns a decoder for one direction of the connection.
func (f *Framing) NewDecoder() *Decoder {
	return &Decoder{framing: f}
}

// Decoder recovers bare CQL frames from one direction of a connection, following the
// framing switch as the handshake goes past it.
//
// It is not safe for concurrent use, and does not need to be: a connection's two
// directions get one each.
type Decoder struct {
	framing *Framing
	// segments de-frames the segmented part of the stream, and is built when the
	// switch happens because until then the layout it needs is not known.
	segments *SegmentSplitter
	// plain de-frames the unsegmented part: the handshake, and the whole of a pre-v5
	// connection.
	plain FrameSplitter
}

// Feed consumes b, calling emit once for each CQL frame it recovers. The frame handed
// to emit is only valid for the duration of the call.
//
// The framing is re-checked at every frame boundary rather than once per call, because
// emit is what flips the latch: the recorder observes the READY or AUTHENTICATE it has
// just been handed. A buffer that carried that frame and the first transport segment
// together would otherwise have its remainder read as bare frames — a segment's payload
// length low byte taken for a protocol version, its next four bytes for a body length,
// and the result written into a recording. Whether one read can ever carry both is
// argued in Framing, and the argument holds for a directly connected server; it is not
// worth resting on when following the latch costs a loop.
func (d *Decoder) Feed(b []byte, emit func(frame []byte) error) error {
	for len(b) > 0 {
		if d.framing.Segmented() {
			if err := d.startSegments(); err != nil {
				return err
			}
			return d.segments.Feed(b, emit)
		}

		taken, err := d.plain.consume(b, emit)
		if err != nil {
			return err
		}
		b = b[taken:]
	}
	return nil
}

// startSegments builds the segment decoder on first use, once the layout it needs is
// known.
func (d *Decoder) startSegments() error {
	if d.segments != nil {
		return nil
	}

	// The switch happens between frames, never inside one — see Framing. Reaching here
	// mid-frame means either that reasoning is wrong for the connection at hand or the
	// stream is damaged, and continuing would feed the tail of a bare frame to a
	// segment decoder and read whatever the next bytes happened to look like as a
	// segment header.
	if d.plain.Pending() {
		return fmt.Errorf("gocql/dialer: connection switched to transport segments in the middle of a frame")
	}

	comp, err := d.framing.segmentCompressor()
	if err != nil {
		return err
	}
	d.segments = NewSegmentSplitter(comp)
	return nil
}

// Pending reports whether the decoder holds anything incomplete: a partial bare
// frame, or a segment chain that has not yet delivered the frame it promised. A
// connection that ends while Pending reports true was cut mid-structure, so what
// its recording holds is truncated.
func (d *Decoder) Pending() bool {
	if d.segments != nil {
		return d.segments.Pending()
	}
	return d.plain.Pending()
}
