// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package paging

import (
	model "github.com/scylladb/gocql/v2/internal/model"

	"bytes"

	"hash/maphash"
)

// pagingHistory owns references to the same immutable tokens sent on the wire.
// Hash collisions compare full bytes; count and payload budgets are independent.
// This nongeneric state machine is also used by the incremental streaming pager.
type InternalPagingHistoryEntry struct {
	InternalState PageState
	InternalNext  *InternalPagingHistoryEntry
}
type InternalPagingHistory struct {
	InternalSeed     maphash.Seed
	InternalEntries  map[uint64]InternalPagingHistoryEntry
	InternalCount    int
	InternalBytes    int64
	InternalMaxCount int
	InternalMaxBytes int64
}

func (h *InternalPagingHistory) InternalAdd(state PageState) error {
	if !state.More() {
		return nil
	}
	if h.InternalEntries == nil {
		h.InternalSeed = maphash.MakeSeed()
		h.InternalEntries = make(map[uint64]InternalPagingHistoryEntry)
	}
	hash := maphash.Bytes(h.InternalSeed, model.PageToken(state))
	entry, exists := h.InternalEntries[hash]
	if exists {
		if bytes.Equal(model.PageToken(entry.InternalState), model.PageToken(state)) {
			return ErrPagingNoProgress
		}
		for p := entry.InternalNext; p != nil; p = p.InternalNext {
			if bytes.Equal(model.PageToken(p.InternalState), model.PageToken(state)) {
				return ErrPagingNoProgress
			}
		}
	}
	maxCount, maxBytes := h.InternalMaxCount, h.InternalMaxBytes
	if maxCount == 0 {
		maxCount = 65536
	}
	if maxBytes == 0 {
		maxBytes = 16 << 20
	}
	if h.InternalCount >= maxCount || int64(len(model.PageToken(state))) > maxBytes-h.InternalBytes {
		return ErrPagingStateLimit
	}
	candidate := InternalPagingHistoryEntry{InternalState: state}
	if exists {
		previous := entry
		candidate.InternalNext = &previous
	}
	h.InternalEntries[hash] = candidate
	h.InternalCount++
	h.InternalBytes += int64(len(model.PageToken(state)))
	return nil
}
func (h *InternalPagingHistory) InternalClear() {
	h.InternalEntries = nil
	h.InternalCount = 0
	h.InternalBytes = 0
}
