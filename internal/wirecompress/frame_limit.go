// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package wirecompress

import (
	"encoding/binary"
	"fmt"
	"math"

	"github.com/klauspost/compress/s2"
)

// Custom decoders without this capability keep their unpooled input owner.
type frameOutputDecoder interface {
	DecodedLen([]byte) (int, error)
	DecodeTo([]byte, []byte) ([]byte, error)
}

// FrameDecoderWithLimit gives built-ins the validated connection budget while
// preserving their standalone public default ceiling. Only exact built-ins use
// this override: custom implementations retain their own decoding semantics.
func FrameDecoderWithLimit(c interface{ Name() string }, limit int64) (FrameDecoder, bool) {
	switch c.(type) {
	case LZ4Compressor, *LZ4Compressor:
		return FrameDecoder{limit: limit}, true
	case SnappyCompressor, *SnappyCompressor:
		return FrameDecoder{limit: limit, snappy: true}, true
	default:
		decoder, ok := c.(frameOutputDecoder)
		return FrameDecoder{custom: decoder}, ok
	}
}

// FrameDecoder is a stack-owned checked transform, avoiding a new interface
// box on each built-in receive. Its output is the caller's final arena.
type FrameDecoder struct {
	custom frameOutputDecoder
	limit  int64
	snappy bool
}

func (d FrameDecoder) DecodedLen(data []byte) (int, error) {
	if d.custom != nil {
		return d.custom.DecodedLen(data)
	}
	var size int64
	if d.snappy {
		n, err := s2.DecodedLen(data)
		if err != nil {
			return 0, err
		}
		size = int64(n)
	} else {
		if len(data) < dataLengthSize {
			return 0, fmt.Errorf("cassandra lz4 block size should be >4, got=%d", len(data))
		}
		size = int64(binary.BigEndian.Uint32(data))
	}
	// Native lengths are signed int32. Check before int conversion, output
	// acquisition or decoder allocation, including on 32-bit platforms.
	if size > d.limit || size > math.MaxInt32 || d.limit <= 0 {
		return 0, fmt.Errorf("gocql: decompressed frame length %d exceeds maximum of %d", size, min(d.limit, math.MaxInt32))
	}
	return int(size), nil
}

func (d FrameDecoder) DecodeTo(dst, data []byte) ([]byte, error) {
	if d.custom != nil {
		return d.custom.DecodeTo(dst, data)
	}
	n, err := d.DecodedLen(data)
	if err != nil {
		return nil, err
	}
	if d.snappy {
		return s2.Decode(dst[:0], data)
	}
	if n == 0 {
		return dst[:0], nil
	}
	out, err := appendLZ4Decompressed(dst[:0], data[dataLengthSize:], n)
	if err == nil && len(out) != n {
		err = fmt.Errorf("gocql: decoded frame length %d differs from declared %d", len(out), n)
	}
	return out, err
}
