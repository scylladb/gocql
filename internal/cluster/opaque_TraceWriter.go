// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cluster

import (
	"io"

	"sync"
	"time"
)

type TraceWriterStorage struct {
	session *Session
	w       io.Writer
	mu      sync.Mutex

	maxAttempts   int
	sleepInterval time.Duration
}

func TraceWriterData(v *TraceWriter) *TraceWriterStorage { return &v.privateStorage }
