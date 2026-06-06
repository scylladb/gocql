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

	"github.com/pierrec/lz4/v4"
)

// maxDecompressedSize is a safety limit to reject corrupt or malicious
// uncompressed length headers. Matches the CQL protocol maxFrameSize (256MB).
const maxDecompressedSize = 256 * 1024 * 1024

// LZ4Compressor implements the gocql.Compressor interface and can be used to
// compress incoming and outgoing frames. According to the Cassandra docs the
// LZ4 protocol should be preferred over snappy. (For details refer to
// https://cassandra.apache.org/doc/latest/operating/compression.html)
//
// Implementation note: Cassandra prefixes each compressed block with 4 bytes
// of the uncompressed block length, written in big endian order. But the LZ4
// compression library github.com/pierrec/lz4/v4 does not expect the length
// field, so it needs to be added to compressed blocks sent to Cassandra, and
// removed from ones received from Cassandra before decompression.
type LZ4Compressor struct{}

func (s LZ4Compressor) Name() string {
	return "lz4"
}

func (s LZ4Compressor) Encode(data []byte) ([]byte, error) {
	dataLen := len(data)
	buf := make([]byte, lz4.CompressBlockBound(dataLen)+4)
	n, err := lz4.CompressBlock(data, buf[4:], nil)
	// According to lz4.CompressBlock doc, it doesn't fail as long as the dst
	// buffer length is at least lz4.CompressBlockBound(len(data))) bytes, but
	// we check for error anyway just to be thorough.
	if err != nil {
		return nil, err
	}
	binary.BigEndian.PutUint32(buf, uint32(dataLen))
	return buf[:n+4], nil
}

func (s LZ4Compressor) Decode(data []byte) ([]byte, error) {
	if len(data) < 4 {
		return nil, fmt.Errorf("cassandra lz4 block size should be >4, got=%d", len(data))
	}
	uncompressedLength := binary.BigEndian.Uint32(data)
	if uncompressedLength == 0 {
		return nil, nil
	}
	buf := make([]byte, uncompressedLength)
	n, err := lz4.UncompressBlock(data[4:], buf)
	return buf[:n], err
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

// EncodeInto compresses data into dst's backing array when it is large enough,
// otherwise it grows dst. The Cassandra LZ4 block format (4-byte big-endian
// uncompressed length followed by the compressed block) is written starting at
// dst[0]. Returns the resulting slice. dst and data must not overlap. This
// avoids per-frame allocations when the caller maintains a reusable buffer
// (e.g., pooled framers). It mirrors Encode exactly except for buffer reuse.
func (s LZ4Compressor) EncodeInto(data []byte, dst []byte) ([]byte, error) {
	dataLen := len(data)
	required := lz4.CompressBlockBound(dataLen) + 4
	if cap(dst) < required {
		dst = make([]byte, required)
	} else {
		dst = dst[:required]
	}
	n, err := lz4.CompressBlock(data, dst[4:], nil)
	if err != nil {
		return dst, err
	}
	binary.BigEndian.PutUint32(dst, uint32(dataLen))
	return dst[:n+4], nil
}
