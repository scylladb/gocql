// Package dialer provides the record/replay and single-connection benchmark
// harness the driver uses to exercise its own wire handling against captured
// traffic.
//
// It is not part of the driver's supported API. Its exported identifiers exist so
// the recorder and replayer subpackages and the benchmarks can share frame
// parsing, and they change whenever the wire handling they mirror changes —
// GetFrameHash, for instance, needs whatever protocol context a frame's layout
// depends on but its bytes do not reveal. Expect breaking signature changes
// without a major version bump.
package dialer

import (
	"errors"

	frm "github.com/gocql/gocql/internal/frame"
	"github.com/gocql/gocql/internal/murmur"
)

// ErrProtoV5NotSupported is returned by the record/replay dialers for protocol
// v5+ connections. After the handshake v5 switches to transport segments
// (framer.prepareModernLayout), which these dialers would silently corrupt:
// the recorder slices the byte stream into frames by the fixed CQL header
// offsets, and the replayer patches stream ids in place, invalidating segment
// CRCs. Segment-aware record/replay is tracked in
// https://github.com/scylladb/gocql/issues/937.
var ErrProtoV5NotSupported = errors.New("gocql/dialer: protocol v5+ uses transport segments, which the record/replay dialers do not support (see scylladb/gocql#937)")

// FrameIsProtoV5OrNewer reports whether b starts a CQL frame whose protocol
// version is v5 or newer. It is only meaningful for bytes at a frame boundary.
// The driver's handshake frames are never segment-framed, so checking each
// frame's first byte rejects a v5+ connection during the handshake, before any
// transport segment flows.
func FrameIsProtoV5OrNewer(b []byte) bool {
	return len(b) > 0 && b[0]&protoVersionMask >= protoVersion5
}

type Record struct {
	Data     []byte `json:"data"`
	StreamID int    `json:"stream_id"`
	// UseMetadataID reports whether the SCYLLA_USE_METADATA_ID extension was
	// negotiated on the connection this frame belongs to. It governs whether an
	// EXECUTE request carries a resultMetadataID short-bytes field on protocol
	// v4 (see GetFrameHash). The recorder stamps it per connection; the frame
	// bytes alone cannot reveal it.
	UseMetadataID bool `json:"use_metadata_id"`
}

// scyllaUseMetadataIDKey is the STARTUP/SUPPORTED option key for the
// SCYLLA_USE_METADATA_ID protocol extension (see gocql/scylla.go).
const scyllaUseMetadataIDKey = "SCYLLA_USE_METADATA_ID"

// StartupNegotiatesMetadataID reports whether the given raw request frame is a
// STARTUP that opts into the SCYLLA_USE_METADATA_ID extension. The driver
// serializes the extension as the key SCYLLA_USE_METADATA_ID in the STARTUP
// [string map]. Detection is restricted to STARTUP requests so the same key
// appearing in a SUPPORTED response (a read frame) never trips it.
//
// The body is walked as a proper [string map] and only keys are compared, rather
// than scanning the frame for the literal: gocql's startupOptions puts
// caller-influenced values into the same map — DRIVER_NAME, DRIVER_VERSION,
// DRIVER_CONFIG, SESSION_ID and whatever ApplicationInfo adds — so a substring
// match lets a caller latch this by naming their application after the extension.
// The list keeps growing, which is the point: matching on keys does not care.
// A malformed or truncated map reads as "not negotiated".
func StartupNegotiatesMetadataID(frame []byte) bool {
	if len(frame) < 5 {
		return false
	}
	shift := headerShift(frame)
	if frameOp(frame[3+shift]) != opStartup {
		return false
	}

	// Header: version, flags, stream (1 byte on v1/v2, 2 on v3+), opcode, length(4).
	p := 8 + shift
	readShort := func() (int, bool) {
		if p+2 > len(frame) {
			return 0, false
		}
		v := int(frame[p])<<8 | int(frame[p+1])
		p += 2
		return v, true
	}
	readString := func() ([]byte, bool) {
		n, ok := readShort()
		if !ok || p+n > len(frame) {
			return nil, false
		}
		s := frame[p : p+n]
		p += n
		return s, true
	}

	count, ok := readShort()
	if !ok {
		return false
	}
	for i := 0; i < count; i++ {
		key, ok := readString()
		if !ok {
			return false
		}
		if _, ok := readString(); !ok { // value, skipped
			return false
		}
		if string(key) == scyllaUseMetadataIDKey {
			return true
		}
	}
	return false
}

// A CQL frame carries the protocol version in the low 7 bits of frame[0]; the
// top bit is the request/response direction. Always mask with protoVersionMask
// before comparing a version, so the version tests in this file cannot disagree
// with each other depending on whether the direction bit happens to be set.
const (
	protoVersionMask = 0x7F
	protoVersion1    = 0x01
	protoVersion2    = 0x02
	protoVersion4    = 0x04
	protoVersion5    = 0x05
)

// headerShift reports the extra header byte protocol v3+ spends on its 2-byte
// stream id: v1/v2 put the opcode at frame[3] and the body at frame[8], v3+ put
// them at frame[4] and frame[9]. Callers must have checked that frame is non-empty.
//
// This is the single place the offset is derived, so the parsers in this file cannot
// disagree about where a frame's opcode is. The comparison is masked per the note
// above: the top bit of frame[0] is the request/response direction, and folding it
// into the version makes every response look like a much newer protocol.
func headerShift(frame []byte) int {
	if frame[0]&protoVersionMask > protoVersion2 {
		return 1
	}
	return 0
}

// fits reports whether the n bytes starting at index lie inside frame.
//
// Every length the parsers below walk on is peer- or file-supplied — a recording
// file is just JSON on disk — so each read is checked against this before it
// indexes. The helpers return false rather than a partial result, and their
// callers fall back to hashing the raw bytes, which is what a damaged recording
// deserves and is still stable between record and replay.
//
// n is compared against the space left rather than added to index, because the
// obvious index+n <= len(frame) overflows on a 32-bit int for the largest length
// a [bytes] field can encode, and a negative sum passes every bound.
func fits(frame []byte, index, n int) bool {
	return index >= 0 && index <= len(frame) && n >= 0 && n <= len(frame)-index
}

type frameOp byte

const (
	// header ops
	opError         frameOp = 0x00
	opStartup       frameOp = 0x01
	opReady         frameOp = 0x02
	opAuthenticate  frameOp = 0x03
	opOptions       frameOp = 0x05
	opSupported     frameOp = 0x06
	opQuery         frameOp = 0x07
	opResult        frameOp = 0x08
	opPrepare       frameOp = 0x09
	opExecute       frameOp = 0x0A
	opRegister      frameOp = 0x0B
	opEvent         frameOp = 0x0C
	opBatch         frameOp = 0x0D
	opAuthChallenge frameOp = 0x0E
	opAuthResponse  frameOp = 0x0F
	opAuthSuccess   frameOp = 0x10
)

// addBytes advances index past a [bytes] value: a 4-byte length followed by that
// many bytes.
//
// The length is read as signed, which the CQL spec says it is: -1 encodes a null
// value and -2 an unset one, neither of which carries a payload. Reading it
// unsigned turns a null bind value into a 4 GB payload and the walk never
// recovers, so an EXECUTE with one hashed as raw bytes rather than as itself.
func addBytes(frame []byte, index int) (int, bool) {
	if !fits(frame, index, 4) {
		return 0, false
	}
	bytesLength := int(int32(uint32(frame[index+0])<<24 | uint32(frame[index+1])<<16 | uint32(frame[index+2])<<8 | uint32(frame[index+3])))
	index = index + 4
	if bytesLength > 0 {
		if !fits(frame, index, bytesLength) {
			return 0, false
		}
		index = index + bytesLength
	}
	return index, true
}

func addQueryParams(frame []byte, index int) (int, bool) {
	//use consistency
	if !fits(frame, index, 2) {
		return 0, false
	}
	index = index + 2

	//use query flags
	var flags uint32
	if frame[0]&protoVersionMask > protoVersion4 {
		// For protocol v5+, flags are a 4-byte big-endian uint32
		if !fits(frame, index, 4) {
			return 0, false
		}
		flags = uint32(frame[index])<<24 |
			uint32(frame[index+1])<<16 |
			uint32(frame[index+2])<<8 |
			uint32(frame[index+3])
		index = index + 4
	} else {
		if !fits(frame, index, 1) {
			return 0, false
		}
		flags = uint32(frame[index])
		index = index + 1
	}

	names := false

	// protoV3 specific things
	if frame[0]&protoVersionMask > protoVersion2 {
		if flags&frm.FlagValues == frm.FlagValues && flags&frm.FlagWithNameValues == frm.FlagWithNameValues {
			names = true
		}
	}

	if flags&frm.FlagValues == frm.FlagValues {
		if !fits(frame, index, 2) {
			return 0, false
		}
		valuesLen := int(frame[index])<<8 | int(frame[index+1])
		index = index + 2

		for i := 0; i < valuesLen; i++ {
			if names {
				if !fits(frame, index, 2) {
					return 0, false
				}
				stringLenght := int(frame[index])<<8 | int(frame[index+1])
				if !fits(frame, index, 2+stringLenght) {
					return 0, false
				}
				index = index + 2 + stringLenght
			}

			var ok bool
			if index, ok = addBytes(frame, index); !ok {
				return 0, false
			}
		}
	}

	if flags&frm.FlagPageSize == frm.FlagPageSize {
		if !fits(frame, index, 4) {
			return 0, false
		}
		index = index + 4
	}

	if flags&frm.FlagWithPagingState == frm.FlagWithPagingState {
		var ok bool
		if index, ok = addBytes(frame, index); !ok {
			return 0, false
		}
	}

	if flags&frm.FlagWithSerialConsistency == frm.FlagWithSerialConsistency {
		if !fits(frame, index, 2) {
			return 0, false
		}
		index = index + 2
	}

	// do not use timelaps and keyspace
	return index, true
}

func addHeader(index int) int {
	return index + 8
}

func addCustomPayload(frame []byte, index int, p int) (int, bool) {
	if !fits(frame, 8+p, 2) {
		return 0, false
	}
	customPayloadLenght := int(frame[8+p])<<8 | int(frame[9+p])
	if customPayloadLenght > 0 {
		index = index + 2
	}
	for i := 0; i < customPayloadLenght; i++ {
		if !fits(frame, index, 2) {
			return 0, false
		}
		stringLenght := int(frame[index])<<8 | int(frame[index+1])
		if !fits(frame, index, 2+stringLenght) {
			return 0, false
		}
		index = index + 2 + stringLenght

		var ok bool
		if index, ok = addBytes(frame, index); !ok {
			return 0, false
		}
	}

	return index, true
}

func GetFrameHash(frame []byte, useMetadataID bool) int64 {
	// useMetadataID reports whether the SCYLLA_USE_METADATA_ID extension was
	// negotiated on the connection. On protocol v4 the extension adds a
	// resultMetadataID short-bytes field to EXECUTE requests (the same field v5
	// always carries); it cannot be inferred from the frame bytes, so it is
	// plumbed in by the recorder/replayer (see Record.UseMetadataID).
	//
	// GetFrameHash parses raw CQL request frames. On protocol v5+ the on-wire
	// bytes recorded by the replayer are not a CQL frame but a transport
	// segment produced by framer.prepareModernLayout (segment header, optional
	// CRC/compression, possibly split across segments), so frame[0] is segment
	// data rather than the CQL version byte. Parsing it as a CQL frame would
	// hash the wrong byte range.
	//
	// This is currently dormant because Scylla negotiates at most protocol v4,
	// so v5 segment framing is never produced. Proper segment unwrapping is
	// tracked in https://github.com/scylladb/gocql/issues/937.
	//
	// The check below is only a best-effort heuristic: for a v5 segment,
	// frame[0] is the low byte of the 17-bit segment length, NOT a CQL version
	// byte. It reliably diverts inputs whose first byte looks like a v5+ version
	// (>= 5), but a segment whose length low-byte is < 5 will still fall into
	// the legacy parser below and be mis-hashed. Correctly distinguishing the
	// two requires protocol context that is not plumbed here.
	//
	// TODO(#937): replace this heuristic with real protocol context.
	//
	// Note this guard, and the short-frame guard below it, hash the frame as given,
	// while the raw-bytes fallbacks inside the switch hash it with the stream id
	// already blanked. Both are stable between record and replay, which is all the
	// hash has to be — but they are not interchangeable, so a new fallback has to
	// match the one next to it rather than whichever reads better. The two guards
	// here have no choice: they run before, or on frames too short to contain, the
	// stream id they would have to blank.
	if len(frame) == 0 || frame[0]&protoVersionMask >= protoVersion5 {
		return murmur.Murmur3H1(frame)
	}

	p := headerShift(frame)

	// A frame shorter than its own header — version, flags, stream id, opcode and
	// the 4-byte body length — cannot be parsed at all: the stream-id blanking
	// below and the opcode switch after it both index into it unconditionally.
	if !fits(frame, 0, 8+p) {
		return murmur.Murmur3H1(frame)
	}

	if p == 1 {
		streamID1 := frame[2]
		streamID2 := frame[3]
		defer func() {
			frame[2] = streamID1
			frame[3] = streamID2
		}()
		frame[2] = byte('0')
		frame[3] = byte('0')
	} else {
		streamID1 := frame[2]
		defer func() {
			frame[2] = streamID1
		}()
		frame[2] = byte('0')
	}
	switch frame[3+p] {
	case byte(opStartup):
		// Hash the header up to and including the opcode, deliberately stopping
		// before the 4-byte body length: a connection sends exactly one STARTUP,
		// so the opcode alone identifies it, and the options it carries are of
		// no interest to a replay.
		//
		// Including the length would tie every checked-in recording to the exact
		// set of STARTUP options the driver sent when it was recorded, so adding
		// one (DRIVER_CONFIG, SESSION_ID, ...) would invalidate them all and
		// panic the replay benchmarks until they were regenerated.
		return murmur.Murmur3H1(frame[:4+p])
	case byte(opPrepare):
		return murmur.Murmur3H1(frame)
	case byte(opAuthResponse):
		return murmur.Murmur3H1(frame)
	case byte(opQuery):
		var ok bool
		index := addHeader(p)
		if frame[1]&frm.FlagCustomPayload == frm.FlagCustomPayload {
			if index, ok = addCustomPayload(frame, index, p); !ok {
				return murmur.Murmur3H1(frame)
			}
		}
		endIndex := index
		if endIndex, ok = addQueryParams(frame, endIndex); !ok {
			return murmur.Murmur3H1(frame)
		}
		if index > endIndex {
			return murmur.Murmur3H1(frame)
		}
		return murmur.Murmur3H1(frame[index:endIndex])
	case byte(opExecute):
		var ok bool
		index := addHeader(p)
		if frame[1]&frm.FlagCustomPayload == frm.FlagCustomPayload {
			if index, ok = addCustomPayload(frame, index, p); !ok {
				return murmur.Murmur3H1(frame)
			}
		}

		endIndex := index

		// Every length here is peer- or file-supplied, and this branch now runs on
		// protocol v4 rather than being unreachable, so bound the reads: a truncated
		// or wrongly-stamped recording must fall back to hashing the raw bytes (as the
		// v5 guard above does) rather than panic inside loadResponseFramesFromFiles.
		// Both the length field and the payload it announces have to be checked —
		// a plausible length running off the end walks straight into addQueryParams.
		if !fits(frame, index, 2) {
			return murmur.Murmur3H1(frame)
		}
		preparedIDLen := int(frame[index])<<8 | int(frame[index+1])
		if !fits(frame, endIndex, 2+preparedIDLen) {
			return murmur.Murmur3H1(frame)
		}
		endIndex = endIndex + 2 + preparedIDLen

		// EXECUTE frames carry a resultMetadataID (short bytes) between the
		// preparedID and the query params on protocol v5+, and on protocol v4 when
		// the SCYLLA_USE_METADATA_ID extension is negotiated. Skip it so the
		// query-params offset (and therefore the extracted hash) is correct. The v4
		// case cannot be read from the frame bytes, so it is signalled by the
		// caller via useMetadataID.
		if frame[0]&protoVersionMask > protoVersion4 || useMetadataID {
			if !fits(frame, endIndex, 2) {
				return murmur.Murmur3H1(frame)
			}
			resultMetadataIDLen := int(frame[endIndex])<<8 | int(frame[endIndex+1])
			if !fits(frame, endIndex, 2+resultMetadataIDLen) {
				return murmur.Murmur3H1(frame)
			}
			endIndex = endIndex + 2 + resultMetadataIDLen
		}

		if frame[0]&protoVersionMask > protoVersion1 {
			if endIndex, ok = addQueryParams(frame, endIndex); !ok {
				return murmur.Murmur3H1(frame)
			}
		} else {
			// Bounded by the preparedID length check above, which read the same two
			// bytes at the same index: nothing between here and there moves it.
			valuesLen := int(frame[index])<<8 | int(frame[index+1])
			index = index + 2
			for i := 0; i < valuesLen; i++ {
				if index, ok = addBytes(frame, index); !ok {
					return murmur.Murmur3H1(frame)
				}
			}
			index = index + 2
		}
		// The v1 branch above walks index independently of endIndex and can leave it
		// past the end of the range, so the two still have to be ordered before the
		// slice. endIndex itself is now bounded by the helpers.
		if index > endIndex {
			return murmur.Murmur3H1(frame)
		}
		return murmur.Murmur3H1(frame[index:endIndex])
	case byte(opBatch):
		return murmur.Murmur3H1(frame)
	case byte(opOptions):
		return murmur.Murmur3H1(frame)
	case byte(opRegister):
		return murmur.Murmur3H1(frame)
	default:
		return murmur.Murmur3H1(frame)
	}
}
