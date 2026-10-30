// Package testkit contains independent native-wire fixtures. It deliberately
// imports no driver serializer, frame parser or segmentation implementation.
package testkit

import (
	"encoding/binary"
	"hash/crc32"
)

// BlobRows is a RESULT/ROWS frame with one global blob column and no paging.
func BlobRows(version byte, stream uint16, rows ...[]byte) []byte {
	body := binary.BigEndian.AppendUint32(nil, 2) // RESULT kind ROWS
	body = binary.BigEndian.AppendUint32(body, 1) // GLOBAL_TABLES_SPEC
	body = binary.BigEndian.AppendUint32(body, 1) // column count
	for _, name := range []string{"fixture", "rows", "value"} {
		body = binary.BigEndian.AppendUint16(body, uint16(len(name)))
		body = append(body, name...)
	}
	body = binary.BigEndian.AppendUint16(body, 3) // blob type
	body = binary.BigEndian.AppendUint32(body, uint32(len(rows)))
	for _, row := range rows {
		body = binary.BigEndian.AppendUint32(body, uint32(len(row)))
		body = append(body, row...)
	}
	frame := []byte{version | 0x80, 0, byte(stream >> 8), byte(stream), 8}
	frame = binary.BigEndian.AppendUint32(frame, uint32(len(body)))
	return append(frame, body...)
}

// Unit implements protocol v5 section 2 directly. Compressed units use a valid
// literal-only LZ4 block: compression is decoded by the receiver, independently
// of the driver's encoder. This is intentionally not an optimization oracle.
func Unit(payload []byte, compressed, selfContained bool) []byte {
	size := len(payload)
	fields := 3
	bits := uint64(size)
	if selfContained {
		bits |= 1 << 17
	}
	if compressed {
		block := []byte{byte(min(15, size) << 4)}
		if size >= 15 {
			left := size - 15
			for left >= 255 {
				block = append(block, 255)
				left -= 255
			}
			block = append(block, byte(left))
		}
		payload = append(block, payload...)
		fields = 5
		bits = uint64(len(payload)) | uint64(size)<<17
		if selfContained {
			bits |= 1 << 34
		}
	}
	var data []byte
	for i := 0; i < fields; i++ {
		data = append(data, byte(bits>>(8*i)))
	}
	// CRC24: init 0x875060, polynomial 0x1974f0b, MSB first.
	crc := uint32(0x875060)
	for _, b := range data {
		crc ^= uint32(b) << 16
		for bit := 0; bit < 8; bit++ {
			crc <<= 1
			if crc&0x1000000 != 0 {
				crc ^= 0x1974f0b
			}
		}
	}
	data = append(data, byte(crc), byte(crc>>8), byte(crc>>16))
	data = append(data, payload...)
	// The four protocol seed bytes precede the on-wire payload in CRC32 input.
	checksum := crc32.Update(0, crc32.IEEETable, []byte{0xfa, 0x2d, 0x55, 0xca})
	checksum = crc32.Update(checksum, crc32.IEEETable, payload)
	return binary.LittleEndian.AppendUint32(data, checksum)
}

// BindStream changes only a fixture response's stream ID and its first unit's
// checksum. The literal-LZ4 token layout above leaves the header uncompressed.
func BindStream(packet []byte, version byte, compressed bool, stream uint16) {
	if version < 5 {
		binary.BigEndian.PutUint16(packet[2:4], stream)
		return
	}
	header := 6
	skip := 0
	size := int(packet[0]) | int(packet[1])<<8 | int(packet[2]&1)<<16
	if compressed {
		header = 8
		literal := int(packet[header] >> 4)
		skip = 1
		if literal == 15 {
			for {
				n := int(packet[header+skip])
				skip++
				if n != 255 {
					break
				}
			}
		}
	}
	binary.BigEndian.PutUint16(packet[header+skip+2:header+skip+4], stream)
	checksum := crc32.Update(0, crc32.IEEETable, []byte{0xfa, 0x2d, 0x55, 0xca})
	checksum = crc32.Update(checksum, crc32.IEEETable, packet[header:header+size])
	binary.LittleEndian.PutUint32(packet[header+size:header+size+4], checksum)
}
