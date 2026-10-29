/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package wire

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"math"
)

var (
	ErrLimit       = errors.New("wire: stream limit exceeded")
	ErrLength      = errors.New("wire: invalid signed length or count")
	ErrStreamScope = errors.New("wire: inactive or nested stream scope")
)

// StreamLimits bounds total envelope work, nested scopes, counts and discard
// scratch. Zero fields select defaults; negative fields are invalid. MaxBytes
// bounds declared bytes even when no full envelope is ever allocated.
type StreamLimits struct {
	MaxBytes     int64
	MaxDepth     int
	MaxCount     int64
	DiscardBytes int
}

// CheckStreamLimits validates and fills defaults without allocating storage.
func CheckStreamLimits(l StreamLimits) (StreamLimits, error) {
	if l.MaxBytes < 0 || l.MaxDepth < 0 || l.MaxCount < 0 || l.DiscardBytes < 0 || l.MaxBytes > math.MaxInt32 || l.MaxCount > math.MaxInt32 {
		return l, ErrLimit
	}
	if l.MaxBytes == 0 {
		l.MaxBytes = 256 << 20
	}
	if l.MaxDepth == 0 {
		l.MaxDepth = 64
	}
	if l.MaxCount == 0 {
		l.MaxCount = math.MaxInt32
	}
	if l.DiscardBytes == 0 {
		l.DiscardBytes = 32 << 10
	}
	return l, nil
}

type streamState struct {
	ctx        context.Context
	source     io.Reader
	limits     StreamLimits
	err        error
	scalar     [8]byte
	discard    []byte
	noProgress int
	children   []*Stream
}

// Stream incrementally consumes one checked envelope or child body. It is a
// scoped mutable reader, never a retained view. Children are callback-scoped;
// parents cannot read while a child is active. All errors are terminal/sticky.
// It is not safe for concurrent use. Cancellation is checked around each read;
// the transport must interrupt a blocked source (deadline/Close) itself. There
// are no background reads or goroutines that could outlive caller storage.
type Stream struct {
	state         *streamState
	remaining     int64
	depth         int
	active, child bool
}

func NewStream(ctx context.Context, source io.Reader, size int64, limits StreamLimits) (*Stream, error) {
	l, err := CheckStreamLimits(limits)
	if err != nil {
		return nil, err
	}
	if ctx == nil || source == nil {
		return nil, ErrStreamScope
	}
	if size < 0 {
		return nil, ErrLength
	}
	if size > l.MaxBytes {
		return nil, ErrLimit
	}
	return &Stream{state: &streamState{ctx: ctx, source: source, limits: l}, remaining: size, active: true}, nil
}

func (s *Stream) Remaining() int64 { return s.remaining }
func (s *Stream) Err() error {
	if s == nil || s.state == nil {
		return ErrStreamScope
	}
	return s.state.err
}
func (s *Stream) fail(err error) error {
	if s.state.err == nil {
		s.state.err = err
	}
	return s.state.err
}
func (s *Stream) check() error {
	if s == nil || s.state == nil {
		return ErrStreamScope
	}
	if s.state.err != nil {
		return s.state.err
	}
	if !s.active || s.child {
		return s.fail(ErrStreamScope)
	}
	if err := s.state.ctx.Err(); err != nil {
		return s.fail(err)
	}
	return nil
}

// Read passes p directly to the underlying source, clipped to this scope's
// boundary. A final read may return bytes and io.EOF, as required by io.Reader.
func (s *Stream) Read(p []byte) (int, error) {
	if err := s.check(); err != nil {
		return 0, err
	}
	if len(p) == 0 {
		return 0, nil
	}
	if s.remaining == 0 {
		return 0, io.EOF
	}
	p = p[:min(int64(len(p)), s.remaining)]
	n, err := s.state.source.Read(p)
	if n < 0 || n > len(p) {
		return 0, s.fail(ErrBounds)
	}
	s.remaining -= int64(n)
	if n == 0 && err == nil {
		s.state.noProgress++
		if s.state.noProgress >= 100 {
			return 0, s.fail(io.ErrNoProgress)
		}
	} else {
		s.state.noProgress = 0
	}
	if contextErr := s.state.ctx.Err(); contextErr != nil {
		return n, s.fail(contextErr)
	}
	if err != nil {
		if err == io.EOF && s.remaining == 0 {
			return n, io.EOF
		}
		if err == io.EOF {
			err = io.ErrUnexpectedEOF
		}
		return n, s.fail(err)
	}
	return n, nil
}

// ReadFull does not read beyond p or the current child. Size preflight happens
// before any bytes are written into p.
func (s *Stream) ReadFull(p []byte) error {
	if err := s.check(); err != nil {
		return err
	}
	if int64(len(p)) > s.remaining {
		return s.fail(io.ErrUnexpectedEOF)
	}
	for len(p) != 0 {
		n, err := s.Read(p)
		p = p[n:]
		if err != nil {
			if err == io.EOF && len(p) == 0 {
				return nil
			}
			return err
		}
	}
	return nil
}

func (s *Stream) scalar(size int) (uint64, error) {
	if err := s.check(); err != nil {
		return 0, err
	}
	p := s.state.scalar[:size]
	if err := s.ReadFull(p); err != nil {
		return 0, err
	}
	switch size {
	case 1:
		return uint64(p[0]), nil
	case 2:
		return uint64(binary.BigEndian.Uint16(p)), nil
	case 4:
		return uint64(binary.BigEndian.Uint32(p)), nil
	default:
		return binary.BigEndian.Uint64(p), nil
	}
}
func (s *Stream) Uint8() (uint8, error)   { n, e := s.scalar(1); return uint8(n), e }
func (s *Stream) Uint16() (uint16, error) { n, e := s.scalar(2); return uint16(n), e }
func (s *Stream) Uint32() (uint32, error) { n, e := s.scalar(4); return uint32(n), e }
func (s *Stream) Uint64() (uint64, error) { return s.scalar(8) }

// Count32 validates a signed count and the minimum framing needed for all its
// elements before a caller reslices/allocates output or begins element bodies.
func (s *Stream) Count32(minBytes int64) (int64, error) {
	if minBytes < 0 {
		return 0, s.fail(ErrLength)
	}
	v, err := s.Uint32()
	if err != nil {
		return 0, err
	}
	n := int64(int32(v))
	if n < 0 {
		return 0, s.fail(ErrLength)
	}
	if n > s.state.limits.MaxCount || (minBytes > 0 && n > s.remaining/minBytes) {
		return 0, s.fail(ErrLimit)
	}
	return n, nil
}

// CheckProduct validates row-count times column-count (or analogous work)
// without overflowing. It does not impose a payload-sized allocation.
func CheckProduct(a, b, limit int64) (int64, error) {
	if a < 0 || b < 0 || limit < 0 {
		return 0, ErrLength
	}
	if b != 0 && a > limit/b {
		return 0, ErrLimit
	}
	return a * b, nil
}

// Child gives fn exactly size bytes. The parent's reservation is made before
// fn runs, but actual reads remain incremental. A failed/incomplete child poisons
// the envelope; no bytes are drained or replayed behind the caller's back.
func (s *Stream) Child(size int64, fn func(*Stream) error) error {
	if err := s.check(); err != nil {
		return err
	}
	if size < 0 {
		return s.fail(ErrLength)
	}
	if size > s.remaining {
		return s.fail(io.ErrUnexpectedEOF)
	}
	if s.depth >= s.state.limits.MaxDepth {
		return s.fail(ErrLimit)
	}
	if fn == nil {
		return s.fail(ErrStreamScope)
	}
	s.remaining -= size
	s.child = true
	// Allocate a scope at most once per nesting depth, never once per cell.
	// A callback must not retain the reader beyond its invocation.
	if len(s.state.children) <= s.depth {
		s.state.children = append(s.state.children, new(Stream))
	}
	child := s.state.children[s.depth]
	*child = Stream{state: s.state, remaining: size, depth: s.depth + 1, active: true}
	defer func() { child.active = false; s.child = false }()
	if err := fn(child); err != nil {
		return s.fail(err)
	}
	return child.RequireEnd()
}

type CellState uint8

const (
	CellPresent CellState = iota
	CellNull
	CellUnset
)

// Cell reads a signed-int32 body length and invokes a limited child. Only -1
// (null) and, when explicitly enabled for inputs, -2 (unset) are sentinels.
// Result decoders pass allowUnset=false. The callback can check capacity from
// child.Remaining before touching a value's body.
func (s *Stream) Cell(allowUnset bool, fn func(CellState, *Stream) error) error {
	v, err := s.Uint32()
	if err != nil {
		return err
	}
	n := int64(int32(v))
	state := CellPresent
	if n == -1 {
		state, n = CellNull, 0
	} else if n == -2 && allowUnset {
		state, n = CellUnset, 0
	} else if n < 0 {
		return s.fail(ErrLength)
	}
	if fn == nil {
		return s.fail(ErrStreamScope)
	}
	return s.Child(n, func(child *Stream) error { return fn(state, child) })
}

// Skip consumes framing-checked bytes with bounded, reused scratch. Type-aware
// skipped-value validation is supplied by the compiled codec above this layer.
func (s *Stream) Skip() error {
	if err := s.check(); err != nil {
		return err
	}
	if s.remaining == 0 {
		return nil
	}
	if s.state.discard == nil {
		s.state.discard = make([]byte, min(int64(s.state.limits.DiscardBytes), s.state.limits.MaxBytes))
	}
	for s.remaining > 0 {
		p := s.state.discard[:min(int64(len(s.state.discard)), s.remaining)]
		if err := s.ReadFull(p); err != nil {
			return err
		}
	}
	return nil
}

func (s *Stream) RequireEnd() error {
	if err := s.check(); err != nil {
		return err
	}
	if s.remaining != 0 {
		return s.fail(ErrTrailingBytes)
	}
	return nil
}

// Context and Limits expose immutable admission data to typed streaming readers.
func (s *Stream) Context() context.Context { return s.state.ctx }
func (s *Stream) Limits() StreamLimits     { return s.state.limits }
