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

// Package wire provides immutable, segmented views of protocol bytes. A view
// borrows its payload: the owner must keep it alive and must not modify it while
// a Span, Chain, or retained Lease refers to it.
package wire

import (
	"errors"
	"io"
	"math"
	"unsafe"
)

var (
	ErrBounds        = errors.New("wire: offset or length out of bounds")
	ErrOverflow      = errors.New("wire: length overflow")
	ErrInvalidMark   = errors.New("wire: invalid builder mark")
	ErrBorrowedPatch = errors.New("wire: cannot patch borrowed bytes")
	ErrTrailingBytes = errors.New("wire: trailing bytes")
)

// Span is an immutable view of contiguous bytes. Its zero value is empty.
type Span struct{ data []byte }

// Borrow borrows b without copying. The caller must not modify b while the
// returned view or any view derived from it is in use.
func Borrow(b []byte) Span { return Span{data: b[:len(b):len(b)]} }

// BorrowString borrows s without copying. The returned bytes MUST NOT be
// modified: their storage may be read-only. This is the only unsafe conversion
// needed by the wire representation.
func BorrowString(s string) Span {
	return Span{data: unsafe.Slice(unsafe.StringData(s), len(s))}
}

func (s Span) Len() int { return len(s.data) }

// Bytes returns a read-only view. It does not grant ownership or permission to
// modify the bytes; use Chain.CopyTo to obtain an owned copy.
func (s Span) Bytes() []byte { return s.data }

func (s Span) Slice(off, n int) (Span, error) {
	if off < 0 || n < 0 || off > len(s.data) || n > len(s.data)-off {
		return Span{}, ErrBounds
	}
	return Span{data: s.data[off : off+n : off+n]}, nil
}

// Chain owns its immutable span descriptors, while borrowing their payloads.
// Copies of a Chain share immutable descriptors and bytes. A one-span Chain
// stores its descriptor inline; its construction and slicing do not allocate.
// The zero value is empty.
type Chain struct {
	head Span
	tail []Span
	len  int64
	// lastLen trims the final tail descriptor without mutating the shared
	// immutable descriptor array. Zero means its complete span is visible.
	lastLen int
}

func addLength(a int64, n int) (int64, error) {
	if a < 0 || n < 0 || int64(n) > math.MaxInt64-a {
		return 0, ErrOverflow
	}
	return a + int64(n), nil
}

// NewChain copies the nonempty span descriptors, never the payload bytes.
func NewChain(spans ...Span) (Chain, error) {
	var c Chain
	count := 0
	for _, s := range spans {
		if s.Len() == 0 {
			continue
		}
		var err error
		c.len, err = addLength(c.len, s.Len())
		if err != nil {
			return Chain{}, err
		}
		count++
	}
	if count == 0 {
		return Chain{}, nil
	}
	if count > 1 {
		c.tail = make([]Span, count-1)
	}
	i := 0
	for _, s := range spans {
		if s.Len() == 0 {
			continue
		}
		if i == 0 {
			c.head = s
		} else {
			c.tail[i-1] = s
		}
		i++
	}
	return c, nil
}

func (c Chain) Len() int64 { return c.len }

func (c Chain) count() int {
	if c.len == 0 {
		return 0
	}
	return 1 + len(c.tail)
}

func (c Chain) span(i int) Span {
	if i == 0 {
		return c.head
	}
	span := c.tail[i-1]
	if i == len(c.tail) && c.lastLen > 0 {
		span.data = span.data[:c.lastLen:c.lastLen]
	}
	return span
}

// Each visits nonempty spans in wire order, stopping on the first error.
func (c Chain) Each(fn func(Span) error) error {
	if c.len == 0 {
		return nil
	}
	if err := fn(c.head); err != nil {
		return err
	}
	for i := 1; i < c.count(); i++ {
		if err := fn(c.span(i)); err != nil {
			return err
		}
	}
	return nil
}

// Slice returns a descriptor-only view of [off, off+n).
func (c Chain) Slice(off, n int64) (Chain, error) {
	if off < 0 || n < 0 || off > c.len || n > c.len-off {
		return Chain{}, ErrBounds
	}
	if n == 0 {
		return Chain{}, nil
	}
	if off == 0 && n == c.len {
		return c, nil
	}
	first := 0
	for off >= int64(c.span(first).Len()) {
		off -= int64(c.span(first).Len())
		first++
	}
	head := c.span(first)
	available := int64(head.Len()) - off
	if n <= available {
		s, _ := head.Slice(int(off), int(n))
		return Chain{head: s, len: n}, nil
	}
	head, _ = head.Slice(int(off), int(available))
	remaining := n - available
	last := first + 1
	for remaining > int64(c.span(last).Len()) {
		remaining -= int64(c.span(last).Len())
		last++
	}
	// First/last trimming is stored in this small value, so every child range
	// shares the immutable middle descriptors without per-cell allocations.
	return Chain{head: head, tail: c.tail[first:last], len: n, lastLen: int(remaining)}, nil
}

// CopyTo explicitly copies the payload into dst. If dst is too short, it copies
// the prefix that fits and returns io.ErrShortBuffer.
func (c Chain) CopyTo(dst []byte) (int, error) {
	n := 0
	for i := 0; i < c.count() && n < len(dst); i++ {
		n += copy(dst[n:], c.span(i).data)
	}
	if int64(n) < c.len {
		return n, io.ErrShortBuffer
	}
	return n, nil
}
