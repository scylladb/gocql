/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// Native protocol v5 ("modern framing") transport segments. A segment is a
// self-describing, CRC-protected envelope that carries one or more complete CQL
// frames (self-contained) or a slice of a single large CQL frame split across
// several segments (non-self-contained). See the CQL native protocol v5 spec,
// section 2 ("Framing"), for the wire layout.

package gocql

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"

	"github.com/gocql/gocql/internal/crc"
)

const (
	crc24Size = 3
	crc32Size = 4
)

// segmentHeader is the decoded fixed-size header of a v5 transport segment.
//
// The header and the payload are read in two phases (readSegmentHeader then
// readSegmentPayload) so the caller can re-arm the read deadline between the
// possibly-idle wait for the header and the bounded read of the payload.
type segmentHeader struct {
	// payloadLen is the number of payload bytes on the wire that follow the
	// header (the post-compression size for compressed segments).
	payloadLen int
	// uncompressedLen is the size of the payload after decompression. It is 0
	// for uncompressed segments, and also 0 for compressed segments whose
	// payload is stored as-is because compression was not worth it.
	uncompressedLen int
	isSelfContained bool
}

// readSegmentHeader reads and validates the fixed-size header of the next
// segment, consuming only the header bytes. When compressor is non-nil the
// compressed-segment layout is used, otherwise the uncompressed layout.
func readSegmentHeader(r io.Reader, compressor Compressor) (segmentHeader, error) {
	if compressor != nil {
		return readCompressedSegmentHeader(r)
	}
	return readUncompressedSegmentHeader(r)
}

// readSegmentPayload reads and verifies the payload and trailing CRC32 that
// follow a header previously read by readSegmentHeader, returning the
// reconstructed (decompressed, if applicable) payload bytes.
func readSegmentPayload(r io.Reader, h segmentHeader, compressor Compressor) ([]byte, error) {
	if compressor != nil {
		return readCompressedSegmentPayload(r, h, compressor)
	}
	return readUncompressedSegmentPayload(r, h)
}

func readUncompressedSegmentHeader(r io.Reader) (segmentHeader, error) {
	const headerSize = 3

	var header [headerSize + crc24Size]byte
	if _, err := io.ReadFull(r, header[:]); err != nil {
		return segmentHeader{}, fmt.Errorf("gocql: failed to read uncompressed frame, err: %w", err)
	}

	// Compute and verify the header CRC24
	computedHeaderCRC24 := crc.Crc24(header[:headerSize])
	readHeaderCRC24 := uint32(header[3]) | uint32(header[4])<<8 | uint32(header[5])<<16
	if computedHeaderCRC24 != readHeaderCRC24 {
		return segmentHeader{}, fmt.Errorf("gocql: crc24 mismatch in frame header, computed: %d, got: %d", computedHeaderCRC24, readHeaderCRC24)
	}

	headerInt := uint32(header[0]) | uint32(header[1])<<8 | uint32(header[2])<<16
	return segmentHeader{
		payloadLen:      int(headerInt & maxSegmentPayloadSize),
		isSelfContained: (headerInt & (1 << 17)) != 0,
	}, nil
}

func readUncompressedSegmentPayload(r io.Reader, h segmentHeader) ([]byte, error) {
	payload := make([]byte, h.payloadLen)
	if _, err := io.ReadFull(r, payload); err != nil {
		return nil, fmt.Errorf("gocql: failed to read uncompressed frame payload, err: %w", err)
	}

	// Read and verify the payload CRC32
	var crcBuf [crc32Size]byte
	if _, err := io.ReadFull(r, crcBuf[:]); err != nil {
		return nil, fmt.Errorf("gocql: failed to read payload crc32, err: %w", err)
	}

	computedPayloadCRC32 := crc.Crc32(payload)
	readPayloadCRC32 := binary.LittleEndian.Uint32(crcBuf[:])
	if computedPayloadCRC32 != readPayloadCRC32 {
		return nil, fmt.Errorf("gocql: payload crc32 mismatch, computed: %d, got: %d", computedPayloadCRC32, readPayloadCRC32)
	}

	return payload, nil
}

func readCompressedSegmentHeader(r io.Reader) (segmentHeader, error) {
	const headerSize = 5

	var headerBuf [headerSize + crc24Size]byte
	if _, err := io.ReadFull(r, headerBuf[:]); err != nil {
		return segmentHeader{}, err
	}

	// Reading checksum from frame header
	readHeaderChecksum := uint32(headerBuf[5]) | uint32(headerBuf[6])<<8 | uint32(headerBuf[7])<<16
	if computedHeaderChecksum := crc.Crc24(headerBuf[:headerSize]); computedHeaderChecksum != readHeaderChecksum {
		return segmentHeader{}, fmt.Errorf("gocql: crc24 mismatch in frame header, read: %d, computed: %d", readHeaderChecksum, computedHeaderChecksum)
	}

	// First 17 bits - payload size after compression
	compressedLen := uint32(headerBuf[0]) | uint32(headerBuf[1])<<8 | uint32(headerBuf[2]&0x1)<<16

	// The next 17 bits - payload size before compression
	uncompressedLen := (uint32(headerBuf[2]) >> 1) | uint32(headerBuf[3])<<7 | uint32(headerBuf[4]&0b11)<<15

	if compressedLen > uint32(maxFrameSize) {
		return segmentHeader{}, fmt.Errorf("gocql: compressed segment length too large: %d", compressedLen)
	}

	return segmentHeader{
		payloadLen:      int(compressedLen),
		uncompressedLen: int(uncompressedLen),
		isSelfContained: (headerBuf[4] & 0b100) != 0,
	}, nil
}

func readCompressedSegmentPayload(r io.Reader, h segmentHeader, compressor Compressor) ([]byte, error) {
	compressedPayload := make([]byte, h.payloadLen)
	if _, err := io.ReadFull(r, compressedPayload); err != nil {
		return nil, fmt.Errorf("gocql: failed to read compressed frame payload, err: %w", err)
	}

	var crcBuf [crc32Size]byte
	if _, err := io.ReadFull(r, crcBuf[:]); err != nil {
		return nil, fmt.Errorf("gocql: failed to read payload crc32, err: %w", err)
	}

	// Ensuring if payload checksum matches
	readPayloadChecksum := binary.LittleEndian.Uint32(crcBuf[:])
	if computedPayloadChecksum := crc.Crc32(compressedPayload); readPayloadChecksum != computedPayloadChecksum {
		return nil, fmt.Errorf("gocql: crc32 mismatch in payload, read: %d, computed: %d", readPayloadChecksum, computedPayloadChecksum)
	}

	// An uncompressed length of 0 signals that the payload is stored as-is and
	// must not be decompressed (native_protocol_v5.spec 2.2).
	if h.uncompressedLen == 0 {
		return compressedPayload, nil
	}

	uncompressedPayload, err := compressor.AppendDecompressed(nil, compressedPayload, uint32(h.uncompressedLen))
	if err != nil {
		return nil, err
	}
	if len(uncompressedPayload) != h.uncompressedLen {
		return nil, fmt.Errorf("gocql: length mismatch after payload decoding, got %d, expected %d", len(uncompressedPayload), h.uncompressedLen)
	}

	return uncompressedPayload, nil
}

// readUncompressedSegment reads a full uncompressed segment (header + payload)
// in one call. It is a convenience wrapper over the two-phase readers.
func readUncompressedSegment(r io.Reader) ([]byte, bool, error) {
	h, err := readUncompressedSegmentHeader(r)
	if err != nil {
		return nil, false, err
	}
	payload, err := readUncompressedSegmentPayload(r, h)
	if err != nil {
		return nil, false, err
	}
	return payload, h.isSelfContained, nil
}

// readCompressedSegment reads a full compressed segment (header + payload) in
// one call. It is a convenience wrapper over the two-phase readers.
func readCompressedSegment(r io.Reader, compressor Compressor) ([]byte, bool, error) {
	h, err := readCompressedSegmentHeader(r)
	if err != nil {
		return nil, false, err
	}
	payload, err := readCompressedSegmentPayload(r, h, compressor)
	if err != nil {
		return nil, false, err
	}
	return payload, h.isSelfContained, nil
}

func newUncompressedSegment(payload []byte, isSelfContained bool) ([]byte, error) {
	const (
		headerSize       = 6
		selfContainedBit = 1 << 17
	)

	payloadLen := len(payload)
	if payloadLen > maxSegmentPayloadSize {
		return nil, fmt.Errorf("gocql: payload length (%d) exceeds maximum size of %d", payloadLen, maxSegmentPayloadSize)
	}

	// Create the segment
	segmentSize := headerSize + payloadLen + crc32Size
	segment := make([]byte, segmentSize)

	// First 3 bytes: payload length and self-contained flag
	headerInt := uint32(payloadLen)
	if isSelfContained {
		headerInt |= selfContainedBit // Set the self-contained flag
	}

	// Encode the first 3 bytes as a single little-endian integer
	segment[0] = byte(headerInt)
	segment[1] = byte(headerInt >> 8)
	segment[2] = byte(headerInt >> 16)

	// Calculate CRC24 for the first 3 bytes of the header
	checksum := crc.Crc24(segment[:3])

	// Encode CRC24 into the next 3 bytes of the header
	segment[3] = byte(checksum)
	segment[4] = byte(checksum >> 8)
	segment[5] = byte(checksum >> 16)

	copy(segment[headerSize:], payload) // Copy the payload to the segment

	// Calculate CRC32 for the payload
	payloadCRC32 := crc.Crc32(payload)
	binary.LittleEndian.PutUint32(segment[headerSize+payloadLen:], payloadCRC32)

	return segment, nil
}

func newCompressedSegment(uncompressedPayload []byte, isSelfContained bool, compressor Compressor) ([]byte, error) {
	const (
		headerSize       = 5
		selfContainedBit = 1 << 34
	)

	uncompressedLen := len(uncompressedPayload)
	if uncompressedLen > maxSegmentPayloadSize {
		return nil, fmt.Errorf("gocql: payload length (%d) exceeds maximum size of %d", uncompressedLen, maxSegmentPayloadSize)
	}

	compressedPayload, err := compressor.AppendCompressed(nil, uncompressedPayload)
	if err != nil {
		return nil, err
	}

	compressedLen := len(compressedPayload)

	// Compression is not worth it
	if uncompressedLen < compressedLen {
		// native_protocol_v5.spec
		// 2.2
		//  An uncompressed length of 0 signals that the compressed payload
		//  should be used as-is and not decompressed.
		compressedPayload = uncompressedPayload
		compressedLen = uncompressedLen
		uncompressedLen = 0
	}

	// Combine compressed and uncompressed lengths and set the self-contained flag if needed
	combined := uint64(compressedLen) | uint64(uncompressedLen)<<17
	if isSelfContained {
		combined |= selfContainedBit
	}

	var headerBuf [headerSize + crc24Size]byte

	// Write the combined value into the header buffer
	binary.LittleEndian.PutUint64(headerBuf[:], combined)

	// Create a buffer with enough capacity to hold the header, compressed payload, and checksums
	buf := bytes.NewBuffer(make([]byte, 0, headerSize+crc24Size+compressedLen+crc32Size))

	// Write the first 5 bytes of the header (compressed and uncompressed sizes)
	buf.Write(headerBuf[:headerSize])

	// Compute and write the CRC24 checksum of the first 5 bytes
	headerChecksum := crc.Crc24(headerBuf[:headerSize])

	// LittleEndian 3 bytes
	headerBuf[0] = byte(headerChecksum)
	headerBuf[1] = byte(headerChecksum >> 8)
	headerBuf[2] = byte(headerChecksum >> 16)
	buf.Write(headerBuf[:3])

	buf.Write(compressedPayload)

	// Compute and write the CRC32 checksum of the payload
	payloadChecksum := crc.Crc32(compressedPayload)
	binary.LittleEndian.PutUint32(headerBuf[:], payloadChecksum)
	buf.Write(headerBuf[:4])

	return buf.Bytes(), nil
}
