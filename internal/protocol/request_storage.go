// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package protocol

import (
	"github.com/scylladb/gocql/v2/internal/wire"
	"sync/atomic"
)

// RequestFrameStorage belongs to one reusable query. A busy slot falls back to
// independent framing for retries whose preceding writer has not yet drained.
type RequestFrameStorage struct {
	busy     atomic.Bool
	envelope TypedEnvelope
	index    wire.FrameIndex
	scalar   []byte
}

func (s *RequestFrameStorage) TryAcquire() bool {
	if !s.busy.CompareAndSwap(false, true) {
		return false
	}
	s.envelope.err = nil
	s.envelope.b.BeginReusableScalar(s.scalar, 256)
	return true
}
func (s *RequestFrameStorage) freeze() (wire.Chain, error) {
	if s.envelope.err != nil {
		return wire.Chain{}, s.envelope.err
	}
	s.scalar = s.envelope.b.ScalarBacking()
	return s.envelope.b.FreezeWithIndex(&s.index)
}
func (s *RequestFrameStorage) Release() {
	if scalar := s.envelope.b.ScalarBacking(); scalar != nil {
		s.scalar = scalar
	}
	s.index.Clear()
	s.envelope.b.RetireFrameReferences()
	s.envelope.err = nil
	s.busy.Store(false)
}
func (s *RequestFrameStorage) RetainedBytes() int {
	return cap(s.scalar) + s.envelope.b.RetainedBytes() + s.index.RetainedBytes()
}
func (s *RequestFrameStorage) Trim(limit int) {
	if s.busy.Load() {
		panic("gocql: trimming framing before writer completion")
	}
	if s.RetainedBytes() > limit {
		s.scalar = nil
		s.envelope = TypedEnvelope{}
		s.index = wire.FrameIndex{}
	}
}
