// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package wire

import "io"

// Fail poisons an envelope when a checked buffered decoder rejects its cell.
// Like Child callback failures, subsequent reads return the original error.
func (s *Stream) Fail(err error) error {
	if s == nil || s.state == nil {
		return ErrStreamScope
	}
	return s.fail(err)
}

func (s *Stream) BufferedCells() bool {
	if s == nil || s.state == nil || s.depth != 0 {
		return false
	}
	_, ok := s.state.source.(*Cursor)
	return ok
}

// BufferedCell checks native framing against a complete owned page. It never
// reads ahead, creates child scopes, or applies to genuinely incremental I/O.
func (s *Stream) BufferedCell() (Chain, CellState, error) {
	if err := s.check(); err != nil {
		return Chain{}, CellPresent, err
	}
	cursor, ok := s.state.source.(*Cursor)
	if !ok || s.depth != 0 {
		return Chain{}, CellPresent, s.fail(ErrStreamScope)
	}
	if s.remaining < 4 {
		return Chain{}, CellPresent, s.fail(io.ErrUnexpectedEOF)
	}
	length, err := cursor.Uint32()
	if err != nil {
		return Chain{}, CellPresent, s.fail(err)
	}
	s.remaining -= 4
	n := int64(int32(length))
	state := CellPresent
	if n == -1 {
		state = CellNull
		n = 0
	} else if n < 0 {
		return Chain{}, CellPresent, s.fail(ErrLength)
	}
	if n > s.remaining {
		return Chain{}, state, s.fail(io.ErrUnexpectedEOF)
	}
	if s.depth >= s.state.limits.MaxDepth {
		return Chain{}, state, s.fail(ErrLimit)
	}
	body, err := cursor.Take(n)
	if err != nil {
		return Chain{}, state, s.fail(err)
	}
	s.remaining -= n
	return body, state, nil
}
