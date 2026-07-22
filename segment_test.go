//go:build unit
// +build unit

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

package gocql

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/gocql/gocql/internal/crc"
)

// makeCompressedSegmentHeaderRaw builds a valid 5-byte compressed-segment
// header (with a correct CRC24) from a raw 40-bit combined value, without any
// clamping. It is used to feed deliberately wide inputs and observe how the
// reader's 17-bit extraction masks them.
func makeCompressedSegmentHeaderRaw(combined uint64) []byte {
	const (
		headerSize = 5
	)

	var wide [8]byte
	binary.LittleEndian.PutUint64(wide[:], combined)

	header := make([]byte, headerSize+crc24Size)
	copy(header[:headerSize], wide[:headerSize])

	checksum := crc.Crc24(header[:headerSize])
	header[headerSize+0] = byte(checksum)
	header[headerSize+1] = byte(checksum >> 8)
	header[headerSize+2] = byte(checksum >> 16)

	return header
}

// makeCompressedSegmentHeader builds a valid 5-byte compressed-segment header
// (with a correct CRC24) for the given lengths and self-contained flag, using
// the same bit layout as newCompressedSegment: compressedLen in bits 0-16,
// uncompressedLen in bits 17-33, self-contained flag at bit 34.
func makeCompressedSegmentHeader(compressedLen, uncompressedLen uint64, isSelfContained bool) []byte {
	const selfContainedBit = 1 << 34

	combined := compressedLen | uncompressedLen<<17
	if isSelfContained {
		combined |= selfContainedBit
	}
	return makeCompressedSegmentHeaderRaw(combined)
}

// incompressibleCompressor mimics compressors (e.g. pierrec/lz4's
// CompressBlock) that report incompressible input by returning an empty result
// with a nil error.
type incompressibleCompressor struct{}

func (incompressibleCompressor) Name() string { return "incompressible" }

func (incompressibleCompressor) AppendCompressed(dst, _ []byte) ([]byte, error) {
	return dst, nil
}

func (incompressibleCompressor) AppendDecompressed(dst, src []byte, _ uint32) ([]byte, error) {
	return append(dst, src...), nil
}

func (incompressibleCompressor) Encode(data []byte) ([]byte, error) {
	return data, nil
}

func (incompressibleCompressor) Decode(data []byte) ([]byte, error) {
	return data, nil
}

// TestNewCompressedSegment_IncompressiblePayloadFallsBackToRaw locks the fix for
// the case where the compressor reports the payload as incompressible (empty
// result). The segment must be emitted as raw (uncompressedLen==0, payloadLen
// equal to the source length) rather than with compressedLen==0 and a nonzero
// uncompressedLen, which the peer cannot decode.
func TestNewCompressedSegment_IncompressiblePayloadFallsBackToRaw(t *testing.T) {
	payload := []byte("this payload is reported as incompressible")

	seg, err := newCompressedSegment(payload, true, incompressibleCompressor{})
	if err != nil {
		t.Fatalf("newCompressedSegment: %v", err)
	}

	h, err := readCompressedSegmentHeader(bytes.NewReader(seg))
	if err != nil {
		t.Fatalf("readCompressedSegmentHeader: %v", err)
	}
	// uncompressedLen==0 signals "use the payload as-is, do not decompress".
	if h.uncompressedLen != 0 {
		t.Fatalf("uncompressedLen = %d, want 0 (raw fallback)", h.uncompressedLen)
	}
	if h.payloadLen != len(payload) {
		t.Fatalf("payloadLen = %d, want %d", h.payloadLen, len(payload))
	}
	if !h.isSelfContained {
		t.Fatalf("isSelfContained = false, want true")
	}
}

func TestReadCompressedSegmentHeader_LengthsBoundedTo17Bits(t *testing.T) {
	// Set every bit in the length/self-contained region. The reader extracts
	// compressedLen and uncompressedLen with a 17-bit mask each, so both must
	// come back clamped to maxSegmentPayloadSize regardless of the wider input.
	// This regression-locks the inherent bound that keeps segment payload
	// allocations safe without an explicit runtime check.
	allBits := uint64(1)<<35 - 1 // bits 0..34 set (both 17-bit fields + self-contained bit)
	header := makeCompressedSegmentHeaderRaw(allBits)

	h, err := readCompressedSegmentHeader(bytes.NewReader(header))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if h.payloadLen != maxSegmentPayloadSize {
		t.Fatalf("payloadLen = %d, want %d", h.payloadLen, maxSegmentPayloadSize)
	}
	if h.uncompressedLen != maxSegmentPayloadSize {
		t.Fatalf("uncompressedLen = %d, want %d", h.uncompressedLen, maxSegmentPayloadSize)
	}
	if !h.isSelfContained {
		t.Fatalf("isSelfContained = false, want true")
	}
}

func TestReadCompressedSegmentHeader_AcceptsMaxLengths(t *testing.T) {
	// The maximum in-range value for both 17-bit fields must be accepted.
	header := makeCompressedSegmentHeader(maxSegmentPayloadSize, maxSegmentPayloadSize, false)

	h, err := readCompressedSegmentHeader(bytes.NewReader(header))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if h.payloadLen != maxSegmentPayloadSize {
		t.Fatalf("payloadLen = %d, want %d", h.payloadLen, maxSegmentPayloadSize)
	}
	if h.uncompressedLen != maxSegmentPayloadSize {
		t.Fatalf("uncompressedLen = %d, want %d", h.uncompressedLen, maxSegmentPayloadSize)
	}
	if h.isSelfContained {
		t.Fatalf("isSelfContained = true, want false")
	}
}
