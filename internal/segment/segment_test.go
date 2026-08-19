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

package segment

import (
	"bytes"
	"encoding/binary"
	"io"
	"runtime"
	"testing"

	"github.com/gocql/gocql/internal/crc"
)

// The helpers below build or read one whole segment in a single call, each with
// buffers of its own. Neither caller of this package works a segment at a time: the
// driver's receive loop reads a header and a payload in two phases so it can re-arm
// its read deadline in between (sharing one Scratch across segments), the send path
// encodes segments directly into the framer's wire buffer, and the record/replay
// dialers buffer a byte stream until a segment is whole. So these live here rather
// than in segment.go — they exist for the tests, and they model an allocation
// contract the production callers deliberately do not follow.

// readUncompressed reads a full uncompressed segment (header + payload) in one
// call, into a payload buffer of its own.
func readUncompressed(r io.Reader) ([]byte, bool, error) {
	var scratch Scratch

	h, err := ReadUncompressedHeader(r)
	if err != nil {
		return nil, false, err
	}
	payload, err := ReadUncompressedPayload(r, h, &scratch)
	if err != nil {
		return nil, false, err
	}
	return payload, h.IsSelfContained, nil
}

// newUncompressed returns payload as a standalone uncompressed segment.
func newUncompressed(payload []byte, isSelfContained bool) ([]byte, error) {
	return AppendUncompressed(nil, payload, isSelfContained)
}

// newCompressed returns payload as a standalone compressed segment.
func newCompressed(payload []byte, isSelfContained bool, comp Compressor) ([]byte, error) {
	return AppendCompressed(nil, payload, isSelfContained, comp)
}

// makeCompressedHeaderRaw builds a valid 5-byte compressed-segment header (with a
// correct CRC24) from a raw 40-bit combined value, without any clamping. It is used
// to feed deliberately wide inputs and observe how the reader's 17-bit extraction
// masks them.
func makeCompressedHeaderRaw(combined uint64) []byte {
	const headerSize = 5

	var wide [8]byte
	binary.LittleEndian.PutUint64(wide[:], combined)

	header := make([]byte, headerSize+Crc24Size)
	copy(header[:headerSize], wide[:headerSize])

	checksum := crc.Crc24(header[:headerSize])
	header[headerSize+0] = byte(checksum)
	header[headerSize+1] = byte(checksum >> 8)
	header[headerSize+2] = byte(checksum >> 16)

	return header
}

// makeCompressedHeader builds a valid 5-byte compressed-segment header (with a
// correct CRC24) for the given lengths and self-contained flag, using the same bit
// layout as AppendCompressed: compressedLen in bits 0-16, uncompressedLen in bits
// 17-33, self-contained flag at bit 34.
func makeCompressedHeader(compressedLen, uncompressedLen uint64, isSelfContained bool) []byte {
	const selfContainedBit = 1 << 34

	combined := compressedLen | uncompressedLen<<17
	if isSelfContained {
		combined |= selfContainedBit
	}
	return makeCompressedHeaderRaw(combined)
}

// incompressibleCompressor mimics compressors (e.g. pierrec/lz4's CompressBlock)
// that report incompressible input by returning an empty result with a nil error.
type incompressibleCompressor struct{}

func (incompressibleCompressor) AppendCompressed(dst, _ []byte) ([]byte, error) {
	return dst, nil
}

func (incompressibleCompressor) AppendDecompressed(dst, src []byte, _ uint32) ([]byte, error) {
	return append(dst, src...), nil
}

// passthroughCompressor is a no-op "compressor" that still honours the contract of
// appending to dst and returning the extended slice. Returning src alone would let
// it pass while a caller that encodes segments directly into dst breaks.
type passthroughCompressor struct{}

func (passthroughCompressor) AppendCompressed(dst, src []byte) ([]byte, error) {
	return append(dst, src...), nil
}

func (passthroughCompressor) AppendDecompressed(dst, src []byte, _ uint32) ([]byte, error) {
	return append(dst, src...), nil
}

// shortAppendCompressor violates the append contract by discarding what it was
// given, which would leave the reserved header outside the returned slice.
type shortAppendCompressor struct{}

func (shortAppendCompressor) AppendCompressed(_, src []byte) ([]byte, error) {
	return src, nil
}

func (shortAppendCompressor) AppendDecompressed(dst, src []byte, _ uint32) ([]byte, error) {
	return append(dst, src...), nil
}

// TestAppendCompressedIncompressiblePayloadFallsBackToRaw locks the case where the
// compressor reports the payload as incompressible (empty result). The segment must
// be emitted as raw (UncompressedLen==0, PayloadLen equal to the source length)
// rather than with compressedLen==0 and a nonzero uncompressedLen, which the peer
// cannot decode.
func TestAppendCompressedIncompressiblePayloadFallsBackToRaw(t *testing.T) {
	payload := []byte("this payload is reported as incompressible")

	seg, err := newCompressed(payload, true, incompressibleCompressor{})
	if err != nil {
		t.Fatalf("newCompressed: %v", err)
	}

	h, err := ReadCompressedHeader(bytes.NewReader(seg))
	if err != nil {
		t.Fatalf("ReadCompressedHeader: %v", err)
	}
	// UncompressedLen==0 signals "use the payload as-is, do not decompress".
	if h.UncompressedLen != 0 {
		t.Fatalf("UncompressedLen = %d, want 0 (raw fallback)", h.UncompressedLen)
	}
	if h.PayloadLen != len(payload) {
		t.Fatalf("PayloadLen = %d, want %d", h.PayloadLen, len(payload))
	}
	if !h.IsSelfContained {
		t.Fatalf("IsSelfContained = false, want true")
	}
}

// TestAppendCompressedRejectsShortAppend pins that a Compressor which discards the
// bytes it was handed is reported rather than slicing out of range.
func TestAppendCompressedRejectsShortAppend(t *testing.T) {
	if _, err := newCompressed([]byte("payload"), true, shortAppendCompressor{}); err == nil {
		t.Fatal("a compressor that dropped the reserved header was accepted")
	}
}

// TestReadPayloadReusesScratch pins that reading consecutive segments with one
// scratch does not allocate. A connection's receive loop is serialized on its
// serve() goroutine, so the payload buffers are reused; allocating them per segment
// costs one (uncompressed) or two (compressed) buffers of up to MaxPayloadSize for
// every segment received.
func TestReadPayloadReusesScratch(t *testing.T) {
	payload := bytes.Repeat([]byte{0x5A}, 8192)

	for _, tc := range []struct {
		name string
		comp Compressor
	}{
		{"uncompressed", nil},
		{"compressed", passthroughCompressor{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var (
				seg []byte
				err error
			)
			if tc.comp != nil {
				seg, err = newCompressed(payload, true, tc.comp)
			} else {
				seg, err = newUncompressed(payload, true)
			}
			if err != nil {
				t.Fatalf("build segment: %v", err)
			}

			var scratch Scratch
			r := bytes.NewReader(seg)
			read := func() {
				r.Reset(seg)
				h, err := ReadHeader(r, tc.comp != nil)
				if err != nil {
					t.Fatalf("ReadHeader: %v", err)
				}
				got, err := ReadPayload(r, h, tc.comp, &scratch)
				if err != nil {
					t.Fatalf("ReadPayload: %v", err)
				}
				if len(got) != len(payload) {
					t.Fatalf("payload length %d, want %d", len(got), len(payload))
				}
			}

			// The first read legitimately allocates the scratch buffers.
			read()

			// Bytes rather than allocation count: reading a segment header still
			// heap-allocates two small fixed-size buffers, which is not what this
			// test is about. Eight reads must not add up to even one payload buffer.
			const reads = 8
			var before, after runtime.MemStats
			runtime.ReadMemStats(&before)
			for i := 0; i < reads; i++ {
				read()
			}
			runtime.ReadMemStats(&after)

			if allocated := after.TotalAlloc - before.TotalAlloc; allocated >= uint64(len(payload)) {
				t.Errorf("reading %d segments allocated %d bytes, want less than one %d-byte payload buffer",
					reads, allocated, len(payload))
			}
		})
	}
}

func TestReadCompressedHeaderLengthsBoundedTo17Bits(t *testing.T) {
	// Set every bit in the length/self-contained region. The reader extracts
	// compressedLen and uncompressedLen with a 17-bit mask each, so both must come
	// back clamped to MaxPayloadSize regardless of the wider input. This
	// regression-locks the inherent bound that keeps segment payload allocations
	// safe without an explicit runtime check.
	allBits := uint64(1)<<35 - 1 // bits 0..34 set (both 17-bit fields + self-contained bit)
	header := makeCompressedHeaderRaw(allBits)

	h, err := ReadCompressedHeader(bytes.NewReader(header))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if h.PayloadLen != MaxPayloadSize {
		t.Fatalf("PayloadLen = %d, want %d", h.PayloadLen, MaxPayloadSize)
	}
	if h.UncompressedLen != MaxPayloadSize {
		t.Fatalf("UncompressedLen = %d, want %d", h.UncompressedLen, MaxPayloadSize)
	}
	if !h.IsSelfContained {
		t.Fatalf("IsSelfContained = false, want true")
	}
}

func TestReadCompressedHeaderAcceptsMaxLengths(t *testing.T) {
	// The maximum in-range value for both 17-bit fields must be accepted.
	header := makeCompressedHeader(MaxPayloadSize, MaxPayloadSize, false)

	h, err := ReadCompressedHeader(bytes.NewReader(header))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if h.PayloadLen != MaxPayloadSize {
		t.Fatalf("PayloadLen = %d, want %d", h.PayloadLen, MaxPayloadSize)
	}
	if h.UncompressedLen != MaxPayloadSize {
		t.Fatalf("UncompressedLen = %d, want %d", h.UncompressedLen, MaxPayloadSize)
	}
	if h.IsSelfContained {
		t.Fatalf("IsSelfContained = true, want false")
	}
}

// TestHeaderSizeMatchesWhatReadHeaderConsumes pins the two constants the buffering
// callers size their reads with against what the decoders actually read. A caller
// that has to collect a whole header before decoding it — the record/replay dialers
// — corrupts the stream if these disagree by even a byte.
func TestHeaderSizeMatchesWhatReadHeaderConsumes(t *testing.T) {
	for _, tc := range []struct {
		name       string
		compressed bool
		segment    func() ([]byte, error)
	}{
		{
			name:       "uncompressed",
			compressed: false,
			segment:    func() ([]byte, error) { return newUncompressed([]byte("payload"), true) },
		},
		{
			name:       "compressed",
			compressed: true,
			segment:    func() ([]byte, error) { return newCompressed([]byte("payload"), true, passthroughCompressor{}) },
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			seg, err := tc.segment()
			if err != nil {
				t.Fatalf("build segment: %v", err)
			}

			r := bytes.NewReader(seg)
			if _, err := ReadHeader(r, tc.compressed); err != nil {
				t.Fatalf("ReadHeader: %v", err)
			}

			consumed := len(seg) - r.Len()
			if want := HeaderSize(tc.compressed); consumed != want {
				t.Errorf("ReadHeader consumed %d bytes, HeaderSize reports %d", consumed, want)
			}
		})
	}
}

// TestEncodedSizeIsAnUpperBound pins that the size EncodedSize reports is enough for
// what Append actually writes, across the split boundary. Under-reporting it makes
// the framer's wire buffer grow mid-encode, which is the allocation the two-buffer
// design exists to avoid.
func TestEncodedSizeIsAnUpperBound(t *testing.T) {
	for _, rawLen := range []int{0, 1, 1024, MaxPayloadSize - 1, MaxPayloadSize, MaxPayloadSize + 1, 2*MaxPayloadSize + 7} {
		for _, comp := range []Compressor{nil, passthroughCompressor{}} {
			payload := bytes.Repeat([]byte{0x42}, rawLen)

			// Mirror how the framer segments a frame: fixed-size continuation
			// chunks, then a remainder that is self-contained only if nothing split.
			var (
				wire          []byte
				err           error
				src           = payload
				selfContained = true
			)
			for len(src) > MaxPayloadSize {
				if wire, err = Append(wire, src[:MaxPayloadSize], false, comp); err != nil {
					t.Fatalf("rawLen=%d: %v", rawLen, err)
				}
				src = src[MaxPayloadSize:]
				selfContained = false
			}
			if wire, err = Append(wire, src, selfContained, comp); err != nil {
				t.Fatalf("rawLen=%d: %v", rawLen, err)
			}

			if bound := EncodedSize(rawLen, comp != nil); len(wire) > bound {
				t.Errorf("rawLen=%d compressed=%v: encoded to %d bytes, EncodedSize reported %d",
					rawLen, comp != nil, len(wire), bound)
			}
		}
	}
}

// TestAppendRoundTrip pins that what Append writes is what the readers read back,
// for both layouts and both values of the self-contained flag.
func TestAppendRoundTrip(t *testing.T) {
	payload := []byte("a payload that survives a round trip")

	for _, selfContained := range []bool{true, false} {
		seg, err := newUncompressed(payload, selfContained)
		if err != nil {
			t.Fatalf("newUncompressed: %v", err)
		}
		got, gotSelfContained, err := readUncompressed(bytes.NewReader(seg))
		if err != nil {
			t.Fatalf("readUncompressed: %v", err)
		}
		if !bytes.Equal(got, payload) {
			t.Errorf("uncompressed round trip: got %q, want %q", got, payload)
		}
		if gotSelfContained != selfContained {
			t.Errorf("uncompressed round trip: self-contained %v, want %v", gotSelfContained, selfContained)
		}

		comp := passthroughCompressor{}
		seg, err = newCompressed(payload, selfContained, comp)
		if err != nil {
			t.Fatalf("newCompressed: %v", err)
		}
		var scratch Scratch
		r := bytes.NewReader(seg)
		h, err := ReadCompressedHeader(r)
		if err != nil {
			t.Fatalf("ReadCompressedHeader: %v", err)
		}
		got, err = ReadCompressedPayload(r, h, comp, &scratch)
		if err != nil {
			t.Fatalf("ReadCompressedPayload: %v", err)
		}
		if !bytes.Equal(got, payload) {
			t.Errorf("compressed round trip: got %q, want %q", got, payload)
		}
		if h.IsSelfContained != selfContained {
			t.Errorf("compressed round trip: self-contained %v, want %v", h.IsSelfContained, selfContained)
		}
	}
}

// TestAppendRejectsOversizedPayload pins the one bound Append needs, since a payload
// longer than the 17-bit length field would otherwise be silently truncated.
func TestAppendRejectsOversizedPayload(t *testing.T) {
	payload := bytes.Repeat([]byte{0x01}, MaxPayloadSize+1)

	if _, err := newUncompressed(payload, true); err == nil {
		t.Error("AppendUncompressed accepted an oversized payload")
	}
	if _, err := newCompressed(payload, true, passthroughCompressor{}); err == nil {
		t.Error("AppendCompressed accepted an oversized payload")
	}
}

// TestReadRejectsCorruptChecksums pins that both CRCs are actually verified.
func TestReadRejectsCorruptChecksums(t *testing.T) {
	payload := []byte("a payload whose checksums matter")

	t.Run("header crc24", func(t *testing.T) {
		seg, err := newUncompressed(payload, true)
		if err != nil {
			t.Fatal(err)
		}
		seg[0] ^= 0xFF // corrupt the length field, leaving its CRC24 stale
		if _, _, err := readUncompressed(bytes.NewReader(seg)); err == nil {
			t.Error("a corrupt header CRC24 was accepted")
		}
	})

	t.Run("payload crc32", func(t *testing.T) {
		seg, err := newUncompressed(payload, true)
		if err != nil {
			t.Fatal(err)
		}
		seg[UncompressedHeaderSize] ^= 0xFF // corrupt a payload byte
		if _, _, err := readUncompressed(bytes.NewReader(seg)); err == nil {
			t.Error("a corrupt payload CRC32 was accepted")
		}
	})
}
