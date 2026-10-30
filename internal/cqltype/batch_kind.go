// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cqltype

// BatchKind selects native protocol batch semantics.
type BatchKind byte

const (
	LoggedBatch   BatchKind = 0
	UnloggedBatch BatchKind = 1
	CounterBatch  BatchKind = 2
)
