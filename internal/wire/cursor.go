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

import (
	"encoding/binary"
	"io"
)

// Cursor reads a Chain without gathering its payload. Failed reads do not
// advance it. A Cursor is not safe for concurrent use.
type Cursor struct {
	chain     Chain
	index     int
	offset    int
	remaining int64
}

func NewCursor(c Chain) *Cursor { return &Cursor{chain: c, remaining: c.Len()} }

func (c *Cursor) Remaining() int64 { return c.remaining }

func (c *Cursor) scalar(n int) (uint64, error) {
	if int64(n) > c.remaining {
		return 0, io.ErrUnexpectedEOF
	}
	data := c.chain.span(c.index).data[c.offset:]
	if len(data) >= n {
		var v uint64
		switch n {
		case 1:
			v = uint64(data[0])
		case 2:
			v = uint64(binary.BigEndian.Uint16(data))
		case 4:
			v = uint64(binary.BigEndian.Uint32(data))
		case 8:
			v = binary.BigEndian.Uint64(data)
		}
		c.advance(int64(n))
		return v, nil
	}
	// The preflight check proves all bytes exist. Accumulate directly into the
	// scalar, without an allocated scratch buffer or a gathered payload.
	left := n
	var v uint64
	for left > 0 {
		data = c.chain.span(c.index).data[c.offset:]
		take := min(left, len(data))
		for _, b := range data[:take] {
			v = v<<8 | uint64(b)
		}
		c.advance(int64(take))
		left -= take
	}
	return v, nil
}

func (c *Cursor) Uint8() (uint8, error) {
	v, err := c.scalar(1)
	return uint8(v), err
}

func (c *Cursor) Uint16() (uint16, error) {
	v, err := c.scalar(2)
	return uint16(v), err
}

func (c *Cursor) Uint32() (uint32, error) {
	v, err := c.scalar(4)
	return uint32(v), err
}

func (c *Cursor) Uint64() (uint64, error) { return c.scalar(8) }

func (c *Cursor) advance(n int64) {
	c.remaining -= n
	for n > 0 {
		available := c.chain.span(c.index).Len() - c.offset
		if n < int64(available) {
			c.offset += int(n)
			return
		}
		n -= int64(available)
		c.index++
		c.offset = 0
	}
}

func (c *Cursor) check(n int64) error {
	if n < 0 {
		return ErrBounds
	}
	if n > c.remaining {
		return io.ErrUnexpectedEOF
	}
	return nil
}

// Take advances over n bytes and returns a descriptor-only view of them.
func (c *Cursor) Take(n int64) (Chain, error) {
	if err := c.check(n); err != nil {
		return Chain{}, err
	}
	if n == 0 {
		return Chain{}, nil
	}
	// Start from the current descriptor, avoiding a rescan of consumed spans
	// for each value in a long sequential decode.
	head := c.chain.span(c.index)
	head, _ = head.Slice(c.offset, head.Len()-c.offset)
	suffix := Chain{head: head, tail: c.chain.tail[c.index:], len: c.remaining, lastLen: c.chain.lastLen}
	view, err := suffix.Slice(0, n)
	if err != nil {
		return Chain{}, err
	}
	c.advance(n)
	return view, nil
}

func (c *Cursor) Skip(n int64) error {
	if err := c.check(n); err != nil {
		return err
	}
	c.advance(n)
	return nil
}

func (c *Cursor) RequireEnd() error {
	if c.remaining != 0 {
		return ErrTrailingBytes
	}
	return nil
}
