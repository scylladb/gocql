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

package wirecompress

import (
	"fmt"
	"github.com/klauspost/compress/s2"
	frm "github.com/scylladb/gocql/v2/internal/frame"
)

type SnappyCompressor struct{}

func (s SnappyCompressor) Name() string {
	return "snappy"
}

func (s SnappyCompressor) Encode(data []byte) ([]byte, error) {
	return s2.EncodeSnappy(nil, data), nil
}

func (s SnappyCompressor) Decode(data []byte) ([]byte, error) { return s.DecodeTo(nil, data) }

// DecodeTo reuses caller output capacity after the same expansion check.
func (s SnappyCompressor) DecodedLen(data []byte) (int, error) {
	// s2.Decode allocates the decoded block from the length varint at the head of
	// data -- a length the peer declared, which s2 itself only refuses above 4 GiB.
	// The frame reader bounds the *compressed* body it accepts against
	// frm.MaxFrameSize, but nothing bounds what that body expands into, so a few
	// corrupt header bytes on a short frame are enough to ask for a multi-gigabyte
	// allocation.
	//
	// frm.MaxFrameSize is reused as the ceiling, but that is a driver-side trade
	// rather than a protocol fact. At v4 the header length, and the server's
	// native_transport_max_frame_size, both bound the *compressed* body, so a
	// legitimate frame may carry a body within that limit which decodes above it.
	// Refusing those gives up a case nobody has reported -- a response compressing
	// to under 256 MiB and expanding past it -- in exchange for bounding an
	// allocation whose size the peer controls. LZ4Compressor makes the same trade
	// with maxDecompressedSize in this package.
	//
	// s2.DecodedLen reads the same varint without allocating, so this costs a few
	// bytes of parsing rather than a second pass.
	//
	// The 32-bit hazard lz4's comment cites does not apply here: above MaxInt32 s2
	// already returns ErrTooLarge rather than panicking in makeslice.
	n, err := s2.DecodedLen(data)
	if err != nil {
		return 0, err
	}
	if n > frm.MaxFrameSize {
		return 0, fmt.Errorf("gocql: snappy uncompressed length %d exceeds maximum of %d", n, frm.MaxFrameSize)
	}

	return n, nil
}

func (s SnappyCompressor) DecodeTo(dst, data []byte) ([]byte, error) {
	if _, e := s.DecodedLen(data); e != nil {
		return nil, e
	}
	return s2.Decode(dst[:0], data)
}
