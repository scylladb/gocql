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

package gocql

import (
	"github.com/klauspost/compress/s2"
)

type Compressor interface {
	Name() string
	Encode(data []byte) ([]byte, error)
	Decode(data []byte) ([]byte, error)
}

// CompressorWithBuffer is an optional interface that compressors can implement
// to support buffer reuse during (de)compression. When the framer detects this
// interface, it passes its reusable buffers to avoid per-frame allocations on
// both the read (decompress) and write (compress) paths.
type CompressorWithBuffer interface {
	Compressor
	// DecodeInto decompresses data into dst, growing dst if needed.
	// Returns the buffer (potentially reallocated) containing decompressed data.
	DecodeInto(data []byte, dst []byte) ([]byte, error)
	// EncodeInto compresses data into dst's backing array when it is large
	// enough (otherwise a new buffer is allocated), returning the resulting
	// slice. The existing contents of dst are not preserved. dst and data must
	// not overlap. Implementations must not retain dst or data after returning.
	EncodeInto(data []byte, dst []byte) ([]byte, error)
}

// SnappyCompressor implements the Compressor interface and can be used to
// compress incoming and outgoing frames. It uses S2 compression algorithm
// that is compatible with snappy and aims for high throughput, which is why
// it features concurrent compression for bigger payloads.
type SnappyCompressor struct{}

func (s SnappyCompressor) Name() string {
	return "snappy"
}

func (s SnappyCompressor) Encode(data []byte) ([]byte, error) {
	return s2.EncodeSnappy(nil, data), nil
}

func (s SnappyCompressor) Decode(data []byte) ([]byte, error) {
	return s2.Decode(nil, data)
}

func (s SnappyCompressor) DecodeInto(data []byte, dst []byte) ([]byte, error) {
	return s2.Decode(dst[:0], data)
}

// EncodeInto compresses data into dst's backing array when it is large enough,
// otherwise it allocates. dst and data must not overlap.
func (s SnappyCompressor) EncodeInto(data []byte, dst []byte) ([]byte, error) {
	return s2.EncodeSnappy(dst, data), nil
}
