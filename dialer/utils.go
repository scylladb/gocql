package dialer

import (
	frm "github.com/gocql/gocql/internal/frame"
	"github.com/gocql/gocql/internal/murmur"
)

type Record struct {
	Data     []byte `json:"data"`
	StreamID int    `json:"stream_id"`
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
	if frame[0] > 0x04 {
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
	if frame[0] > 0x02 {
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

func GetFrameHash(frame []byte) int64 {
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
	// two requires protocol context that is not plumbed here (see #937). A CQL
	// request frame carries the protocol version in the low 7 bits of frame[0].
	const (
		protoVersionMask = 0x7F
		protoVersion5    = 0x05
	)
	// TODO(#937): replace this heuristic with real protocol context — for a v5
	// segment frame[0] is a length byte, so segments with a length low-byte < 5
	// are still mis-hashed by the legacy parser below.
	if len(frame) == 0 || frame[0]&protoVersionMask >= protoVersion5 {
		return murmur.Murmur3H1(frame)
	}

	var p int
	if frame[0] > 0x02 {
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
		return murmur.Murmur3H1(frame[:8+p])
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

		// For protocol v5+, EXECUTE frames carry a resultMetadataID (short bytes)
		// between the preparedID and the query params. Skip it so the query-params
		// offset (and therefore the extracted hash) is correct. Currently dormant:
		// Scylla negotiates at most protocol v4, so this branch is not reached in
		// production, but it keeps the parser correct if v5 is enabled.
		if frame[0] > 0x04 {
			resultMetadataIDLen := int(frame[endIndex])<<8 | int(frame[endIndex+1])
			endIndex = endIndex + 2 + resultMetadataIDLen
		}

		if frame[0] > 0x01 {
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
