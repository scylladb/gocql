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
	"fmt"
	"sync"

	"github.com/pierrec/lz4/v4"
)

// compressorPool pools the lz4 compression state. lz4.Compressor carries the
// match hash table it reuses between blocks — ~136 KiB — so declaring one per
// call would allocate and zero that for every frame (on protocol v5, for every
// 128 KiB segment of every frame). It holds no reference to the source or
// destination buffers, so one instance is reusable for any input; the pool only
// exists to keep concurrent compressions from sharing a table.
var compressorPool = sync.Pool{
	New: func() any { return new(lz4.Compressor) },
}

// LZ4Compressor implements the gocql.Compressor interface (and, for native
// protocol v5 segment compression, gocql.SegmentCompressor). It can be used to
// compress incoming and outgoing frames. According to the Cassandra docs the
// LZ4 protocol should be preferred over snappy. (For details refer to
// https://cassandra.apache.org/doc/latest/operating/compression.html)
//
// Implementation note: Cassandra prefixes each compressed block with 4 bytes
// of the uncompressed block length, written in big endian order. But the LZ4
// compression library github.com/pierrec/lz4/v4 does not expect the length
// field, so it needs to be added to compressed blocks sent to Cassandra, and
// removed from ones received from Cassandra before decompression. This applies
// to the Encode/Decode (v4 framing) path; the v5 segment path
// (AppendCompressed/AppendDecompressed) carries the length out-of-band and
// omits the prefix.
type LZ4Compressor struct{}

func (s LZ4Compressor) Name() string {
	return "lz4"
}

const dataLengthSize = 4

// maxDecompressedSize bounds the buffer allocated from a caller-supplied
// uncompressed length: the length prefix in Decode, and the out-of-band length
// in AppendDecompressed. It matches Cassandra's default
// native_transport_max_frame_size (256 MiB): the driver rejects frames larger
// than this anyway, so a length claim above it is corrupt or hostile. Bounding
// here prevents a multi-GB allocation (or, on 32-bit, a makeslice panic) driven
// by a crafted or garbled compressed frame.
//
// On the driver's v5 segment path the length is already bounded to 17 bits by
// the segment header, so for AppendDecompressed this guards direct callers of
// the exported method — and keeps its int() conversion safe on 32-bit, since
// this limit is below math.MaxInt32.
const maxDecompressedSize = 256 * 1024 * 1024

func (s LZ4Compressor) Encode(data []byte) ([]byte, error) {
	maxLength := lz4.CompressBlockBound(len(data))
	buf := make([]byte, maxLength+dataLengthSize)

	compressor := compressorPool.Get().(*lz4.Compressor)
	n, err := compressor.CompressBlock(data, buf[dataLengthSize:])
	compressorPool.Put(compressor)
	// According to lz4.CompressBlock doc, it doesn't fail as long as the dst
	// buffer length is at least lz4.CompressBlockBound(len(data))) bytes, but
	// we check for error anyway just to be thorough. Given that bound, it always
	// emits a valid block (never n==0) — incompressible input yields a block
	// slightly larger than the input rather than an empty one.
	if err != nil {
		return nil, err
	}
	binary.BigEndian.PutUint32(buf[:dataLengthSize], uint32(len(data)))
	return buf[:n+dataLengthSize], nil
}

func (s LZ4Compressor) Decode(data []byte) ([]byte, error) {
	if len(data) < dataLengthSize {
		return nil, fmt.Errorf("cassandra lz4 block size should be >4, got=%d", len(data))
	}
	uncompressedLength := int(binary.BigEndian.Uint32(data))
	if uncompressedLength == 0 {
		return nil, nil
	}
	if uncompressedLength > maxDecompressedSize {
		return nil, fmt.Errorf("cassandra lz4 uncompressed length %d exceeds maximum of %d", uncompressedLength, maxDecompressedSize)
	}
	buf := make([]byte, uncompressedLength)
	n, err := lz4.UncompressBlock(data[dataLengthSize:], buf)
	return buf[:n], err
}

func (s LZ4Compressor) AppendCompressed(dst, src []byte) ([]byte, error) {
	maxLength := lz4.CompressBlockBound(len(src))
	oldDstLen := len(dst)
	dst = grow(dst, maxLength)

	compressor := compressorPool.Get().(*lz4.Compressor)
	n, err := compressor.CompressBlock(src, dst[oldDstLen:])
	compressorPool.Put(compressor)
	if err != nil {
		return nil, err
	}

	return dst[:oldDstLen+n], nil
}

func (s LZ4Compressor) AppendDecompressed(dst, src []byte, uncompressedLength uint32) ([]byte, error) {
	if uncompressedLength == 0 {
		return dst, nil
	}
	if uncompressedLength > maxDecompressedSize {
		return nil, fmt.Errorf("cassandra lz4 uncompressed length %d exceeds maximum of %d", uncompressedLength, maxDecompressedSize)
	}
	oldDstLen := len(dst)
	dst = grow(dst, int(uncompressedLength))
	n, err := lz4.UncompressBlock(src, dst[oldDstLen:])
	return dst[:oldDstLen+n], err
}

// grow grows b to guarantee space for n elements, if needed.
func grow(b []byte, n int) []byte {
	oldLen := len(b)
	if cap(b)-oldLen < n {
		newBuf := make([]byte, oldLen+n)
		copy(newBuf, b)
		b = newBuf
	}
	return b[:oldLen+n]
}

// DecodeInto decompresses LZ4 data into the provided buffer, growing it if
// necessary. Returns the buffer (potentially reallocated) containing the
// decompressed data. This avoids per-frame allocations when the caller
// maintains a reusable buffer (e.g., pooled framers).
func (s LZ4Compressor) DecodeInto(data []byte, dst []byte) ([]byte, error) {
	if len(data) < 4 {
		return dst, fmt.Errorf("cassandra lz4 block size should be >4, got=%d", len(data))
	}
	uncompressedLength := int(binary.BigEndian.Uint32(data))
	if uncompressedLength == 0 {
		return dst[:0], nil
	}
	if uncompressedLength < 0 || uncompressedLength > maxDecompressedSize {
		return dst, fmt.Errorf("cassandra lz4 uncompressed length out of range: %d", uncompressedLength)
	}
	if cap(dst) < uncompressedLength {
		dst = make([]byte, uncompressedLength)
	} else {
		dst = dst[:uncompressedLength]
	}
	n, err := lz4.UncompressBlock(data[4:], dst)
	if err != nil {
		return dst, err
	}
	return dst[:n], nil
}
