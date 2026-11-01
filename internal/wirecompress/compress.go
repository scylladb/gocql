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

// Package wirecompress compresses borrowed wire spans without assembling an
// uncompressed input buffer. Only encoded output and span descriptors are stored.
//
// The encoders implement the LZ4 block and Snappy block format descriptions:
// https://github.com/lz4/lz4/blob/dev/doc/lz4_Block_format.md and
// https://github.com/google/snappy/blob/main/format_description.txt.
package wirecompress

import (
	"encoding/binary"
	"errors"
	"math"
	"math/bits"

	"github.com/scylladb/gocql/v2/internal/wire"
)

// ErrTooLarge means the input exceeds the encoder's 32-bit position range or
// the worst-case encoded result and existing destination exceed a Go slice.
var ErrTooLarge = errors.New("wirecompress: block too large")

// AppendLZ4 appends a raw LZ4 block to dst. It includes neither a frame nor an
// uncompressed-length prefix. Empty input produces the valid empty block {0}.
//
// Source spans are read directly, including matches across span boundaries.
// Source must remain immutable during the call and must not overlap the backing
// storage of dst. Input is limited to 2^32-1 bytes and the machine's slice limit.
// On error, the original dst is returned unchanged.
func AppendLZ4(dst []byte, source wire.Chain) ([]byte, error) {
	n, err := checkSize(len(dst), source.Len(), 255)
	if err != nil {
		return dst, err
	}
	var small [8]sourcePart
	input, err := indexSource(source, small[:])
	if err != nil {
		return dst, err
	}
	// The final five input bytes must be literals, and the final match must
	// begin at least twelve bytes before the end (LZ4 block format).
	finder := matchFinder{input: &input, lastStart: n - 12, end: n - 5, maxOffset: 65535}
	anchor := 0
	for {
		position, reference, length, ok := finder.next(anchor)
		if !ok {
			break
		}
		literalLen := position - anchor
		matchLen := length - 4
		token := byte(min(literalLen, 15)<<4 | min(matchLen, 15))
		dst = append(dst, token)
		if literalLen >= 15 {
			dst = appendLZ4Length(dst, literalLen-15)
		}
		dst = input.appendRange(dst, anchor, position)
		offset := position - reference
		dst = append(dst, byte(offset), byte(offset>>8))
		if matchLen >= 15 {
			dst = appendLZ4Length(dst, matchLen-15)
		}
		anchor = position + length
	}
	literalLen := n - anchor
	dst = append(dst, byte(min(literalLen, 15)<<4))
	if literalLen >= 15 {
		dst = appendLZ4Length(dst, literalLen-15)
	}
	out := input.appendRange(dst, anchor, n)
	perfOutput(out, n)
	return out, nil
}

// AppendSnappy appends a standard Snappy block, including the varint
// uncompressed length, to dst. It is compatible with SnappyCompressor's block
// format, and does not use S2-only instructions or stream framing.
//
// Source spans are read directly, including matches across span boundaries.
// Source must remain immutable during the call and must not overlap the backing
// storage of dst. Input is limited to 2^32-1 bytes and the machine's slice limit.
// On error, the original dst is returned unchanged.
func AppendSnappy(dst []byte, source wire.Chain) ([]byte, error) {
	n, err := checkSize(len(dst), source.Len(), 6)
	if err != nil {
		return dst, err
	}
	var small [8]sourcePart
	input, err := indexSource(source, small[:])
	if err != nil {
		return dst, err
	}
	dst = binary.AppendUvarint(dst, uint64(n))
	finder := matchFinder{input: &input, lastStart: n - 4, end: n, maxOffset: math.MaxUint32}
	anchor := 0
	for {
		position, reference, length, ok := finder.next(anchor)
		if !ok {
			break
		}
		dst = appendSnappyLiteral(dst, &input, anchor, position)
		dst = appendSnappyCopy(dst, position-reference, length)
		anchor = position + length
	}
	out := appendSnappyLiteral(dst, &input, anchor, n)
	perfOutput(out, n)
	return out, nil
}

func checkSize(dstLen int, sourceLen int64, divisor uint64) (int, error) {
	if sourceLen < 0 || uint64(sourceLen) > math.MaxUint32 || dstLen < 0 {
		return 0, ErrTooLarge
	}
	// Both formulas bound the entire block, including length and token bytes.
	// The extra 32 also covers the LZ4 bound's usual 16-byte allowance.
	n := uint64(sourceLen)
	bound := n + n/divisor + 32
	if bound > uint64(math.MaxInt-dstLen) {
		return 0, ErrTooLarge
	}
	return int(sourceLen), nil
}

func appendLZ4Length(dst []byte, length int) []byte {
	for length >= 255 {
		dst = append(dst, 255)
		length -= 255
	}
	return append(dst, byte(length))
}

func appendSnappyLiteral(dst []byte, input *sourceIndex, begin, end int) []byte {
	length := end - begin
	if length == 0 {
		return dst
	}
	value := uint32(length - 1)
	if length <= 60 {
		dst = append(dst, byte(value<<2))
	} else {
		width := (bits.Len32(value) + 7) / 8
		dst = append(dst, byte((59+width)<<2))
		for i := 0; i < width; i++ {
			dst = append(dst, byte(value))
			value >>= 8
		}
	}
	return input.appendRange(dst, begin, end)
}

func appendSnappyCopy(dst []byte, offset, length int) []byte {
	for length > 0 {
		n := min(length, 64)
		if offset < 2048 && n >= 4 && n <= 11 {
			dst = append(dst, byte(1|(n-4)<<2|(offset>>8)<<5), byte(offset))
		} else if offset <= 65535 {
			dst = append(dst, byte(2|(n-1)<<2), byte(offset), byte(offset>>8))
		} else {
			dst = append(dst, byte(3|(n-1)<<2), byte(offset), byte(offset>>8), byte(offset>>16), byte(offset>>24))
		}
		length -= n
	}
	return dst
}

// Each descriptor borrows its payload from the source. In particular, this is
// not a window containing copied input bytes: the only allocation that grows
// with input fragmentation is this descriptor array.
type sourcePart struct {
	bytes []byte
	start int
}

type sourceIndex struct {
	parts []sourcePart
}

func indexSource(source wire.Chain, scratch []sourcePart) (sourceIndex, error) {
	count := 0
	if err := source.Each(func(span wire.Span) error {
		if span.Len() != 0 {
			count++
		}
		return nil
	}); err != nil {
		return sourceIndex{}, err
	}
	parts := scratch
	if count > len(parts) {
		parts = make([]sourcePart, count)
	} else {
		parts = parts[:count]
	}
	position, index := 0, 0
	err := source.Each(func(span wire.Span) error {
		data := span.Bytes()
		if len(data) != 0 {
			parts[index] = sourcePart{bytes: data, start: position}
			position += len(data)
			index++
		}
		return nil
	})
	return sourceIndex{parts: parts}, err
}

// appendRange writes literal bytes directly into their final encoded position.
// It never assembles a temporary uncompressed slice.
func (s *sourceIndex) appendRange(dst []byte, begin, end int) []byte {
	if begin == end {
		return dst
	}
	c := sourceCursor{input: s}
	for begin < end {
		data := c.at(begin)
		n := min(len(data), end-begin)
		dst = append(dst, data[:n]...)
		begin += n
	}
	return dst
}

type sourceCursor struct {
	input *sourceIndex
	part  int
}

// at returns a borrowed suffix. Adjacent lookups avoid the descriptor search.
func (c *sourceCursor) at(position int) []byte {
	parts := c.input.parts
	p := parts[c.part]
	if position < p.start || position >= p.start+len(p.bytes) {
		if c.part+1 < len(parts) && position >= parts[c.part+1].start && position < parts[c.part+1].start+len(parts[c.part+1].bytes) {
			c.part++
		} else {
			lo, hi := 0, len(parts)
			for lo < hi {
				mid := lo + (hi-lo)/2
				if parts[mid].start <= position {
					lo = mid + 1
				} else {
					hi = mid
				}
			}
			c.part = lo - 1
		}
		p = parts[c.part]
	}
	return p.bytes[position-p.start:]
}

func (c *sourceCursor) word(position int) uint32 {
	data := c.at(position)
	if len(data) >= 4 {
		return binary.LittleEndian.Uint32(data)
	}
	// Four scalar loads handle a word spanning up to four input spans. There
	// is no scratch slice or uncompressed copying even at a span boundary.
	var value uint32
	for i := 0; i < 4; i++ {
		value |= uint32(c.at(position + i)[0]) << (8 * i)
	}
	return value
}

func (s *sourceIndex) commonLength(a, b, end int) int {
	start := a
	left, right := sourceCursor{input: s}, sourceCursor{input: s}
	for a < end {
		x, y := left.at(a), right.at(b)
		n := min(len(x), len(y), end-a)
		i := 0
		for ; i+8 <= n; i += 8 {
			diff := binary.LittleEndian.Uint64(x[i:]) ^ binary.LittleEndian.Uint64(y[i:])
			if diff != 0 {
				return a - start + i + bits.TrailingZeros64(diff)/8
			}
		}
		for ; i < n; i++ {
			if x[i] != y[i] {
				return a - start + i
			}
		}
		a += n
		b += n
	}
	return a - start
}

// A table entry is a position plus one, allowing zero to denote an unused slot.
// The fixed table holds scalar positions, never input payload. Its size is
// independent of input size and span count.
type matchFinder struct {
	input     *sourceIndex
	lastStart int
	end       int
	maxOffset uint64
	table     [1 << 14]uint32
}

func matchHash(word uint32) uint32 { return (word * 0x9e3779b1) >> (32 - 14) }

func (f *matchFinder) next(begin int) (position, reference, length int, ok bool) {
	scan, candidate := sourceCursor{input: f.input}, sourceCursor{input: f.input}
	skip, step := 32, 1
	for position = begin; position <= f.lastStart; position += min(step, f.lastStart-position+1) {
		step = skip >> 5
		skip++
		word := scan.word(position)
		hash := matchHash(word)
		entry := f.table[hash]
		f.table[hash] = uint32(position) + 1
		if entry == 0 {
			continue
		}
		reference = int(entry - 1)
		offset := position - reference
		if offset <= 0 || uint64(offset) > f.maxOffset || candidate.word(reference) != word {
			continue
		}
		length = 4 + f.input.commonLength(position+4, reference+4, f.end)
		// A four-byte Snappy offset needs five output bytes. Do not emit a
		// match shorter than six, keeping encoding expansion bounded.
		if offset > 65535 && length < 6 {
			continue
		}
		// Seed the tail of a long match so adjacent matches can reference it.
		end := position + length
		for p := max(position+1, end-3); p < end && p <= f.lastStart; p++ {
			f.table[matchHash(scan.word(p))] = uint32(p) + 1
		}
		return position, reference, length, true
	}
	return 0, 0, 0, false
}
