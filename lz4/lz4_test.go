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
/*
 * Content before git sha 34fdeebefcbf183ed7f916f931aa0586fdaa1b40
 * Copyright (c) 2016, The Gocql authors,
 * provided under the BSD-3-Clause License.
 * See the NOTICE file distributed with this work for additional information.
 */

package lz4

import (
	"encoding/binary"
	"math"
	"math/rand"
	"testing"

	"github.com/pierrec/lz4/v4"

	"github.com/stretchr/testify/require"
)

func TestLZ4Compressor(t *testing.T) {
	t.Parallel()

	var c LZ4Compressor
	require.Equal(t, "lz4", c.Name())

	_, err := c.Decode([]byte{0, 1, 2})
	require.EqualError(t, err, "cassandra lz4 block size should be >4, got=3")

	_, err = c.Decode([]byte{0, 1, 2, 4, 5})
	require.EqualError(t, err, "lz4: invalid source or destination buffer too short")

	// If uncompressed size is zero then nothing is decoded even if present.
	decoded, err := c.Decode([]byte{0, 0, 0, 0, 5, 7, 8})
	require.NoError(t, err)
	require.Nil(t, decoded)

	original := []byte("My Test String")
	encoded, err := c.Encode(original)
	require.NoError(t, err)
	decoded, err = c.Decode(encoded)
	require.NoError(t, err)
	require.Equal(t, original, decoded)
}

// TestLZ4Compressor_IncompressibleRoundTrip covers incompressible input, which
// the existing "My Test String" case does not. lz4 cannot shrink random bytes,
// so Encode emits a block slightly larger than the input; Decode must still
// reverse it exactly. This guards the expand-rather-than-compress path.
func TestLZ4Compressor_IncompressibleRoundTrip(t *testing.T) {
	var c LZ4Compressor

	// Random bytes do not compress. Use a fixed seed for determinism.
	rng := rand.New(rand.NewSource(1))
	original := make([]byte, 1024)
	_, _ = rng.Read(original)

	encoded, err := c.Encode(original)
	require.NoError(t, err)
	// Confirm we really are on the expand path: the lz4 block (minus the 4-byte
	// length prefix) is larger than the input.
	require.Greater(t, len(encoded)-dataLengthSize, len(original),
		"random input should expand, exercising the incompressible path")

	decoded, err := c.Decode(encoded)
	require.NoError(t, err)
	require.Equal(t, original, decoded)
}

// TestLZ4Compressor_DecodeRejectsHugeLength ensures a peer-supplied uncompressed
// length above maxDecompressedSize is rejected before allocating, rather than
// driving a multi-GB allocation (or a makeslice panic on 32-bit).
func TestLZ4Compressor_DecodeRejectsHugeLength(t *testing.T) {
	var c LZ4Compressor
	var hdr [dataLengthSize]byte
	binary.BigEndian.PutUint32(hdr[:], maxDecompressedSize+1)
	_, err := c.Decode(append(hdr[:], 0x00))
	require.Error(t, err)
	require.Contains(t, err.Error(), "exceeds maximum")
}

// TestLZ4Compressor_AppendDecompressedRejectsHugeLength is the AppendDecompressed
// counterpart of the Decode case above. The driver never reaches it — the v5
// segment header carries the length in 17 bits — but the method is exported, and
// without the bound a large uncompressedLength is either an unbounded allocation
// or, on 32-bit where int() of a value above math.MaxInt32 goes negative, a slice
// bounds panic inside grow.
func TestLZ4Compressor_AppendDecompressedRejectsHugeLength(t *testing.T) {
	var c LZ4Compressor

	for _, tc := range []struct {
		name   string
		length uint32
	}{
		{"just above the limit", maxDecompressedSize + 1},
		{"negative once narrowed to a 32-bit int", math.MaxUint32},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := c.AppendDecompressed(nil, []byte{0x00}, tc.length)
			require.Error(t, err)
			require.Contains(t, err.Error(), "exceeds maximum")
		})
	}

	// The bound must not reject a legal segment-sized payload.
	original := []byte("My Test String")
	encoded, err := c.AppendCompressed(nil, original)
	require.NoError(t, err)
	decoded, err := c.AppendDecompressed(nil, encoded, uint32(len(original)))
	require.NoError(t, err)
	require.Equal(t, original, decoded)
}

func TestLZ4Compressor_AppendCompressedDecompressed(t *testing.T) {
	c := LZ4Compressor{}

	invalidUncompressedLength := uint32(10)
	_, err := c.AppendDecompressed(nil, []byte{0, 1, 2, 4, 5}, invalidUncompressedLength)
	require.EqualError(t, err, "lz4: invalid source or destination buffer too short")

	original := []byte("My Test String")
	encoded, err := c.AppendCompressed(nil, original)
	require.NoError(t, err)
	decoded, err := c.AppendDecompressed(nil, encoded, uint32(len(original)))
	require.NoError(t, err)
	require.Equal(t, original, decoded)
}

func TestLZ4Compressor_AppendGrowSliceWithData(t *testing.T) {
	var tests = []struct {
		name                 string
		src                  []byte
		dst                  []byte
		shouldReuseDst       bool
		decodeDst            []byte
		shouldReuseDecodeDst bool
	}{
		{
			name:      "both dst are empty",
			src:       []byte("small data"),
			dst:       nil,
			decodeDst: nil,
		},
		{
			name:      "dst is nil",
			src:       []byte("another piece of data"),
			dst:       nil,
			decodeDst: []byte("something"),
		},
		{
			name:      "decodeDst is nil",
			src:       []byte("another piece of data"),
			dst:       []byte("some"),
			decodeDst: nil,
		},
		{
			name:      "both dst are not empty",
			src:       []byte("another piece of data"),
			dst:       []byte("dst"),
			decodeDst: []byte("decodeDst"),
		},
		{
			name:                 "both dst slices have enough capacity",
			src:                  []byte("small"),
			dst:                  createBufWithCapAndData("cap=128", 128),
			shouldReuseDst:       true,
			decodeDst:            createBufWithCapAndData("cap=256", 256),
			shouldReuseDecodeDst: true,
		},
		{
			name:      "both dst slices have some data and not enough capacity",
			src:       []byte("small"),
			dst:       createBufWithCapAndData("data", 6),
			decodeDst: createBufWithCapAndData("wow", 4),
		},
		// The two reuse flags have to disagree in at least one case each way.
		// Every case above sets them equal, which makes both branches below
		// compute the same expectation — so an assertion keyed on the wrong
		// flag would pass on the whole table. These two are recombinations of
		// the dst/decodeDst values already used above, so what each one grows
		// or reuses is pinned by the symmetric cases.
		{
			name:                 "compress dst has capacity, decode dst does not",
			src:                  []byte("small"),
			dst:                  createBufWithCapAndData("cap=128", 128),
			shouldReuseDst:       true,
			decodeDst:            createBufWithCapAndData("wow", 4),
			shouldReuseDecodeDst: false,
		},
		{
			name:                 "decode dst has capacity, compress dst does not",
			src:                  []byte("small"),
			dst:                  createBufWithCapAndData("data", 6),
			shouldReuseDst:       false,
			decodeDst:            createBufWithCapAndData("cap=256", 256),
			shouldReuseDecodeDst: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			compressor := LZ4Compressor{}

			// Appending compressed data to dst,
			// expecting that dst still contains "test"
			result, err := compressor.AppendCompressed(tt.dst, tt.src)
			require.NoError(t, err)

			var expectedCap int
			if tt.shouldReuseDst {
				expectedCap = cap(tt.dst)
			} else {
				expectedCap = len(tt.dst) + lz4.CompressBlockBound(len(tt.src))
			}

			require.Equal(t, expectedCap, cap(result))
			if len(tt.dst) > 0 {
				require.Equal(t, tt.dst, result[:len(tt.dst)])
			}

			uncompressedLen := uint32(len(tt.src))
			result, err = compressor.AppendDecompressed(tt.decodeDst, result[len(tt.dst):], uncompressedLen)
			require.NoError(t, err)

			var expectedDecodeCap int
			if tt.shouldReuseDecodeDst {
				expectedDecodeCap = cap(tt.decodeDst)
			} else {
				expectedDecodeCap = len(tt.decodeDst) + len(tt.src)
			}

			require.Equal(t, expectedDecodeCap, cap(result))
			require.Equal(t, tt.src, result[len(tt.decodeDst):])
		})
	}
}

func createBufWithCapAndData(data string, cap int) []byte {
	buf := make([]byte, cap)
	copy(buf, data)
	return buf[:len(data)]
}

// TestLZ4Compressor_RejectsOversizedHeader ensures both Decode and DecodeInto
// reject an untrusted uncompressed-length header above maxDecompressedSize
// before allocating, rather than only DecodeInto enforcing the bound.
func TestLZ4Compressor_RejectsOversizedHeader(t *testing.T) {
	t.Parallel()

	var c LZ4Compressor

	oversized := make([]byte, 8)
	binary.BigEndian.PutUint32(oversized, uint32(maxDecompressedSize+1))

	_, err := c.Decode(oversized)
	require.ErrorContains(t, err, "uncompressed length out of range")

	_, err = c.DecodeInto(oversized, nil)
	require.ErrorContains(t, err, "uncompressed length out of range")
}
