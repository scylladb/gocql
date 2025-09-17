package dialer

import (
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

	// query flags
	flagValues                byte = 0x01
	flagSkipMetaData          byte = 0x02
	flagPageSize              byte = 0x04
	flagWithPagingState       byte = 0x08
	flagWithSerialConsistency byte = 0x10
	flagDefaultTimestamp      byte = 0x20
	flagWithNameValues        byte = 0x40
	flagWithKeyspace          byte = 0x80

	// header flags
	flagCompress      byte = 0x01
	flagTracing       byte = 0x02
	flagCustomPayload byte = 0x04
	flagWarning       byte = 0x08
	flagBetaProtocol  byte = 0x10
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
	var flags byte
	if frame[0] > 0x04 {
		flags = frame[index+3]
		index = index + 4
	} else {
		flags = frame[index]
		index = index + 1
	}

	names := false

	// protoV3 specific things
	if frame[0] > 0x02 {
		if flags&flagValues == flagValues && flags&flagWithNameValues == flagWithNameValues {
			names = true
		}
	}

	if flags&flagValues == flagValues {
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

	if flags&flagPageSize == flagPageSize {
		index = index + 4
	}

	if flags&flagWithPagingState == flagWithPagingState {
		index = addBytes(frame, index)
	}

	if flags&flagWithSerialConsistency == flagWithSerialConsistency {
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
		if frame[1]&flagCustomPayload == flagCustomPayload {
			index = addCustomPayload(frame, index, p)
		}
		endIndex := index
		endIndex = addQueryParams(frame, endIndex)
		return murmur.Murmur3H1(frame[index:endIndex])
	case byte(opExecute):
		index := addHeader(p)
		if frame[1]&flagCustomPayload == flagCustomPayload {
			index = addCustomPayload(frame, index, p)
		}

		endIndex := index

		preparedIDLen := int(frame[index])<<8 | int(frame[index+1])
		endIndex = endIndex + 2 + preparedIDLen
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
