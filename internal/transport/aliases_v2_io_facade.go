// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package transport

import (
	"github.com/scylladb/gocql/v2/internal/wire"

	"time"
)

type chainReader = InternalChainReader
type connReadSource = InternalConnReadSource
type connReader = InternalConnReader
type contextWriter = InternalContextWriter

type deadlineDisarmer = InternalDeadlineDisarmer
type deadlineWriter = InternalDeadlineWriter

var errArmReadDeadline = InternalErrArmReadDeadline

type frameSource = InternalFrameSource
type headerReader = InternalHeaderReader

func newChainReader(a0 wire.Chain) *chainReader { return InternalNewChainReader(a0) }
func newWriteCoalescer(a0 deadlineWriter, a1 time.Duration, a2 time.Duration, a3 <-chan struct{}) *writeCoalescer {
	return InternalNewWriteCoalescer(a0, a1, a2, a3)
}

type spanContextWriter = InternalSpanContextWriter
type writeCoalescer = InternalWriteCoalescer
