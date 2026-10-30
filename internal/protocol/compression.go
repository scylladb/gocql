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

package protocol

// Compressor is the interface that must be implemented by frame compressors.
//
// Encode compresses data and returns the compressed bytes; Decode reverses it.
// Any protocol-required framing (e.g. LZ4's big-endian uncompressed-length
// prefix expected by Cassandra) is the compressor's responsibility.
//
// Decode must bound what it allocates: it must reject a decompressed length above
// 256 MiB. The frame reader bounds the compressed body it reads, but only Decode
// knows how far that body expands, so a compressor that trusts a length carried
// inside the compressed data lets a short corrupt frame drive an arbitrarily large
// allocation. 256 MiB is the largest frame body the driver accepts, reused here as
// a deliberate ceiling on expansion -- the protocol bounds only the compressed
// side, so this is stricter than the wire requires. Both built-in compressors
// enforce it; a third-party implementation is expected to do the same.
type Compressor interface {
	Name() string
	Encode(data []byte) ([]byte, error)
	Decode(data []byte) ([]byte, error)
}

// SegmentCompressor is an optional capability interface for compressors that
// support native protocol v5 segment compression. The v5 transport carries the
// uncompressed payload length out-of-band in the segment header, so — unlike
// Encode/Decode — no length prefix is embedded in the compressed bytes.
//
// A Compressor that does not also implement SegmentCompressor cannot be used
// with ProtoVersion >= 5; the driver rejects such a configuration up front (see
// the ProtoVersion validation in the cluster config). Both methods append to
// dst and return the extended slice.
type SegmentCompressor interface {
	// AppendCompressed compresses src and appends the compressed bytes to dst.
	AppendCompressed(dst, src []byte) ([]byte, error)

	// AppendDecompressed decompresses src (whose decompressed size is supplied
	// out-of-band as decompressedLength) and appends the result to dst.
	AppendDecompressed(dst, src []byte, decompressedLength uint32) ([]byte, error)
}
