package dialer

import (
	"bytes"
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
// [string map], so its presence is detectable by scanning the frame. Detection
// is restricted to STARTUP requests so the same key appearing in a SUPPORTED
// response (a read frame) never trips it.
func StartupNegotiatesMetadataID(frame []byte) bool {
	if len(frame) < 5 {
		return false
	}
	var op byte
	if frame[0] > 0x02 {
		op = frame[4]
	} else {
		op = frame[3]
	}
	if frameOp(op) != opStartup {
		return false
	}
	return bytes.Contains(frame, []byte(scyllaUseMetadataIDKey))
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

func addBytes(frame []byte, index int) int {
	bytesLength := int(frame[index+0])<<24 | int(frame[index+1])<<16 | int(frame[index+2])<<8 | int(frame[index+3])
	index = index + 4
	if bytesLength > 0 {
		index = index + bytesLength
	}
	return index
}

func addQueryParams(frame []byte, index int) int {
	//use consistency
	index = index + 2

	//use query flags
	var flags uint32
	if frame[0]&protoVersionMask > protoVersion4 {
		// For protocol v5+, flags are a 4-byte big-endian uint32
		flags = uint32(frame[index])<<24 |
			uint32(frame[index+1])<<16 |
			uint32(frame[index+2])<<8 |
			uint32(frame[index+3])
		index = index + 4
	} else {
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
		valuesLen := int(frame[index])<<8 | int(frame[index+1])
		index = index + 2

		for i := 0; i < valuesLen; i++ {
			if names {
				stringLenght := int(frame[index])<<8 | int(frame[index+1])
				index = index + 2 + stringLenght
			}

			index = addBytes(frame, index)
		}
	}

	if flags&frm.FlagPageSize == frm.FlagPageSize {
		index = index + 4
	}

	if flags&frm.FlagWithPagingState == frm.FlagWithPagingState {
		index = addBytes(frame, index)
	}

	if flags&frm.FlagWithSerialConsistency == frm.FlagWithSerialConsistency {
		index = index + 2
	}

	// do not use timelaps and keyspace
	return index
}

func addHeader(index int) int {
	return index + 8
}

func addCustomPayload(frame []byte, index int, p int) int {
	customPayloadLenght := int(frame[8+p])<<8 | int(frame[9+p])
	if customPayloadLenght > 0 {
		index = index + 2
	}
	for i := 0; i < customPayloadLenght; i++ {
		stringLenght := int(frame[index])<<8 | int(frame[index+1])
		index = index + 2 + stringLenght
		index = addBytes(frame, index)
	}

	return index
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
	// Note this guard hashes the frame as given, while the raw-bytes fallbacks
	// inside the switch below hash it with the stream id already blanked. Both are
	// stable between record and replay, which is all the hash has to be — but they
	// are not interchangeable, so a new fallback has to match the one next to it
	// rather than whichever reads better.
	if len(frame) == 0 || frame[0]&protoVersionMask >= protoVersion5 {
		return murmur.Murmur3H1(frame)
	}

	var p int
	if frame[0]&protoVersionMask > protoVersion2 {
		p = 1
		streamID1 := frame[2]
		streamID2 := frame[3]
		defer func() {
			frame[2] = streamID1
			frame[3] = streamID2
		}()
		frame[2] = byte('0')
		frame[3] = byte('0')
	} else {
		p = 0
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
		index := addHeader(p)
		if frame[1]&frm.FlagCustomPayload == frm.FlagCustomPayload {
			index = addCustomPayload(frame, index, p)
		}
		endIndex := index
		endIndex = addQueryParams(frame, endIndex)
		return murmur.Murmur3H1(frame[index:endIndex])
	case byte(opExecute):
		index := addHeader(p)
		if frame[1]&frm.FlagCustomPayload == frm.FlagCustomPayload {
			index = addCustomPayload(frame, index, p)
		}

		endIndex := index

		preparedIDLen := int(frame[index])<<8 | int(frame[index+1])
		endIndex = endIndex + 2 + preparedIDLen

		// EXECUTE frames carry a resultMetadataID (short bytes) between the
		// preparedID and the query params on protocol v5+, and on protocol v4 when
		// the SCYLLA_USE_METADATA_ID extension is negotiated. Skip it so the
		// query-params offset (and therefore the extracted hash) is correct. The v4
		// case cannot be read from the frame bytes, so it is signalled by the
		// caller via useMetadataID.
		if frame[0]&protoVersionMask > protoVersion4 || useMetadataID {
			resultMetadataIDLen := int(frame[endIndex])<<8 | int(frame[endIndex+1])
			endIndex = endIndex + 2 + resultMetadataIDLen
		}

		if frame[0]&protoVersionMask > protoVersion1 {
			endIndex = addQueryParams(frame, endIndex)
		} else {
			valuesLen := int(frame[index])<<8 | int(frame[index+1])
			index = index + 2
			for i := 0; i < valuesLen; i++ {
				index = addBytes(frame, index)
			}
			index = index + 2
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
