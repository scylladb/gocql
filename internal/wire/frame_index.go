// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package wire

import "unsafe"

// FrameIndex is private writer-owned storage. A frozen chain may refer to it
// only until the corresponding writer completion; it is never a page lease.
type FrameIndex struct{ tail []indexedSpan }

func (s *FrameIndex) Clear()             { clear(s.tail); s.tail = s.tail[:0] }
func (s *FrameIndex) RetainedBytes() int { return cap(s.tail) * int(unsafe.Sizeof(indexedSpan{})) }

func (b *Builder) BeginReusableScalar(storage []byte, slabSize int) {
	parts, invalid, generation := b.parts[:0], b.invalid[:0], b.generation+1
	*b = Builder{parts: parts, invalid: invalid, slab: storage[:cap(storage)], slabSize: slabSize, currentPart: -1, generation: generation}
}
func (b *Builder) ScalarBacking() []byte { return b.slab }
func (b *Builder) RetainedBytes() int {
	return cap(b.parts)*int(unsafe.Sizeof(builderPart{})) + cap(b.invalid)*int(unsafe.Sizeof(invalidMarks{})) + cap(b.ownedBuffer) + cap(b.slab)
}
func (b *Builder) RetireFrameReferences() {
	clear(b.parts)
	b.parts = b.parts[:0]
	b.ownedBuffer = nil
	b.slab = nil
	b.used = 0
	b.len = 0
	b.currentPart = -1
	b.invalid = b.invalid[:0]
	b.generation++
}
func (b *Builder) FreezeWithIndex(storage *FrameIndex) (Chain, error) {
	return b.freezeWithIndex(storage)
}
