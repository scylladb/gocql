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

package wire

import "encoding/binary"

const defaultSlabSize = 4096

type builderPart struct {
	span  Span
	owned bool
}

type invalidMarks struct{ after, through uint64 }

// Mark identifies a builder position. A mark belongs to one Builder and becomes
// invalid after Freeze, or when Rollback removes the branch on which it arose.
// Its zero value is invalid.
type Mark struct {
	owner      *Builder
	generation uint64
	sequence   uint64
	offset     int64
}

// Builder assembles owned scalar bytes and borrowed payload spans. Its slabs
// never grow or move, and Freeze hands their storage to an immutable Chain.
// A Builder must not be copied and is not safe for concurrent use. Its zero
// value is ready to use.
type Builder struct {
	slabSize     int
	parts        []builderPart
	slab         []byte
	used         int
	currentPart  int
	currentStart int
	len          int64
	generation   uint64
	sequence     uint64
	invalid      []invalidMarks
}

// NewBuilder uses slabs of at least slabSize bytes. Nonpositive sizes select a
// default; individual scalars larger than the configured size get their own
// sufficiently large slab.
func NewBuilder(slabSize int) *Builder {
	if slabSize <= 0 {
		slabSize = defaultSlabSize
	}
	return &Builder{slabSize: slabSize, currentPart: -1}
}

func (b *Builder) Len() int64 { return b.len }

// Borrow appends a read-only span, retaining its backing storage without copy.
func (b *Builder) Borrow(s Span) error {
	if s.Len() == 0 {
		return nil
	}
	n, err := addLength(b.len, s.Len())
	if err != nil {
		return err
	}
	b.parts = append(b.parts, builderPart{span: s})
	b.currentPart = -1
	b.len = n
	return nil
}

func (b *Builder) writable(n int) ([]byte, error) {
	total, err := addLength(b.len, n)
	if err != nil {
		return nil, err
	}
	if len(b.slab)-b.used < n {
		size := b.slabSize
		if size <= 0 {
			size = defaultSlabSize
		}
		b.slab = make([]byte, max(size, n))
		b.used = 0
		b.currentPart = -1
	}
	start := b.used
	b.used += n
	if b.currentPart < 0 {
		b.currentStart = start
		b.currentPart = len(b.parts)
		b.parts = append(b.parts, builderPart{owned: true})
	}
	b.parts[b.currentPart].span = Borrow(b.slab[b.currentStart:b.used])
	b.len = total
	return b.slab[start:b.used], nil
}

func (b *Builder) Uint8(v uint8) error {
	p, err := b.writable(1)
	if err == nil {
		p[0] = v
	}
	return err
}

func (b *Builder) Uint16(v uint16) error {
	p, err := b.writable(2)
	if err == nil {
		binary.BigEndian.PutUint16(p, v)
	}
	return err
}

func (b *Builder) Uint32(v uint32) error {
	p, err := b.writable(4)
	if err == nil {
		binary.BigEndian.PutUint32(p, v)
	}
	return err
}

func (b *Builder) Uint64(v uint64) error {
	p, err := b.writable(8)
	if err == nil {
		binary.BigEndian.PutUint64(p, v)
	}
	return err
}

func (b *Builder) Mark() Mark {
	b.sequence++
	return Mark{owner: b, generation: b.generation, sequence: b.sequence, offset: b.len}
}

func (b *Builder) valid(m Mark) bool {
	if m.owner != b || m.generation != b.generation || m.sequence == 0 || m.sequence > b.sequence || m.offset < 0 || m.offset > b.len {
		return false
	}
	for _, r := range b.invalid {
		if m.sequence > r.after && m.sequence <= r.through {
			return false
		}
	}
	return true
}

// Rollback discards all bytes appended after m. Invalid marks are ignored.
// Marks created after m are invalidated, while m and earlier marks stay valid.
func (b *Builder) Rollback(m Mark) {
	if !b.valid(m) {
		return
	}
	if m.sequence < b.sequence {
		b.invalid = append(b.invalid, invalidMarks{after: m.sequence, through: b.sequence})
	}
	remaining := m.offset
	keep := 0
	for keep < len(b.parts) && remaining > 0 {
		part := &b.parts[keep]
		if int64(part.span.Len()) > remaining {
			part.span, _ = part.span.Slice(0, int(remaining))
			remaining = 0
		} else {
			remaining -= int64(part.span.Len())
		}
		keep++
	}
	clear(b.parts[keep:])
	b.parts = b.parts[:keep]
	b.len = m.offset
	// Discard the writable tail. Future writes use a fresh slab and can never
	// accidentally extend a descriptor over discarded or borrowed bytes.
	b.slab = nil
	b.used = 0
	b.currentPart = -1
}

// PatchUint32 overwrites four owned bytes starting at m. All four bytes are
// checked before any are modified, including when the patch crosses slabs.
func (b *Builder) PatchUint32(m Mark, v uint32) error {
	if !b.valid(m) {
		return ErrInvalidMark
	}
	if b.len-m.offset < 4 {
		return ErrBounds
	}
	off := m.offset
	first := 0
	for off >= int64(b.parts[first].span.Len()) {
		off -= int64(b.parts[first].span.Len())
		first++
	}
	remaining := 4
	for i, skip := first, int(off); remaining > 0; i, skip = i+1, 0 {
		part := b.parts[i]
		if !part.owned {
			return ErrBorrowedPatch
		}
		remaining -= min(remaining, part.span.Len()-skip)
	}
	var data [4]byte
	binary.BigEndian.PutUint32(data[:], v)
	written := 0
	for i, skip := first, int(off); written < len(data); i, skip = i+1, 0 {
		written += copy(b.parts[i].span.data[skip:], data[written:])
	}
	return nil
}

// Freeze publishes the accumulated bytes and resets b for independent reuse.
// It invalidates all existing marks, even when the result is empty. Borrowed
// payloads remain borrowed; the caller must still honor their lifetime.
func (b *Builder) Freeze() (Chain, error) {
	var c Chain
	if len(b.parts) > 0 {
		c.head = b.parts[0].span
		c.len = b.len
		if len(b.parts) > 1 {
			c.tail = make([]Span, len(b.parts)-1)
			for i := 1; i < len(b.parts); i++ {
				c.tail[i-1] = b.parts[i].span
			}
		}
	}
	clear(b.parts)
	b.parts = b.parts[:0]
	b.slab = nil
	b.used = 0
	b.currentPart = -1
	b.len = 0
	b.generation++
	b.sequence = 0
	b.invalid = b.invalid[:0]
	return c, nil
}
