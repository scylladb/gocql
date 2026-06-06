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
	"bytes"
	"testing"

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

func TestLZ4CompressorDecodeInto(t *testing.T) {
	t.Parallel()

	var c LZ4Compressor

	original := []byte("My Test String that is reasonably long to compress nicely")
	encoded, err := c.Encode(original)
	require.NoError(t, err)

	// nil dst.
	got, err := c.DecodeInto(encoded, nil)
	require.NoError(t, err)
	require.Equal(t, original, got)

	// Reuse a sufficiently-sized buffer; result must match and the previously
	// returned data (copied) must not be corrupted by reuse.
	prev := append([]byte(nil), got...)
	got2, err := c.DecodeInto(encoded, got[:0])
	require.NoError(t, err)
	require.Equal(t, original, got2)
	require.Equal(t, original, prev)

	// Undersized dst must be grown/reallocated, not truncated.
	got3, err := c.DecodeInto(encoded, make([]byte, 0, 1))
	require.NoError(t, err)
	require.Equal(t, original, got3)

	// Short block error.
	_, err = c.DecodeInto([]byte{0, 1, 2}, nil)
	require.EqualError(t, err, "cassandra lz4 block size should be >4, got=3")

	// Zero uncompressed length yields an empty (non-nil-cap-preserving) slice.
	empty, err := c.DecodeInto([]byte{0, 0, 0, 0, 5, 7, 8}, make([]byte, 0, 8))
	require.NoError(t, err)
	require.Len(t, empty, 0)

	// Oversized declared length must be rejected (corruption/DoS guard).
	bad := []byte{0xFF, 0xFF, 0xFF, 0xFF, 1, 2, 3, 4}
	_, err = c.DecodeInto(bad, nil)
	require.Error(t, err)
}

func TestLZ4CompressorEncodeInto(t *testing.T) {
	t.Parallel()

	var c LZ4Compressor

	inputs := [][]byte{
		[]byte("My Test String that is reasonably long to compress nicely"),
		[]byte("x"),
		bytes.Repeat([]byte("abcd1234"), 1000),
		bytes.Repeat([]byte{0}, 4096),
	}

	var dst []byte // reused across all inputs (grows as needed)
	for i, in := range inputs {
		// EncodeInto output must match Encode exactly.
		want, err := c.Encode(in)
		require.NoErrorf(t, err, "input %d Encode", i)

		got, err := c.EncodeInto(in, dst)
		require.NoErrorf(t, err, "input %d EncodeInto", i)
		require.Equalf(t, want, got, "input %d EncodeInto vs Encode", i)

		// Round-trip through Decode.
		decoded, err := c.Decode(got)
		require.NoErrorf(t, err, "input %d Decode", i)
		require.Equalf(t, in, decoded, "input %d round-trip", i)

		dst = got // reuse next iteration
	}

	// Undersized dst must be grown/reallocated.
	got, err := c.EncodeInto(inputs[0], make([]byte, 0, 1))
	require.NoError(t, err)
	decoded, err := c.Decode(got)
	require.NoError(t, err)
	require.Equal(t, inputs[0], decoded)
}
