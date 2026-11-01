//go:build gocql_perf

// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package wirecompress

import (
	"sync/atomic"
	"unsafe"
)

var perfCalls, perfInput, perfOutputLen atomic.Int64
var perfOutputPointer atomic.Uintptr

func perfOutput(output []byte, input int) {
	perfCalls.Add(1)
	perfInput.Add(int64(input))
	perfOutputLen.Store(int64(len(output)))
	perfOutputPointer.Store(uintptr(unsafe.Pointer(unsafe.SliceData(output))))
}

// ResetPerfCounters requires a quiescent workload. Identity is used only while
// the actual output remains held by its frame; it never dereferences uintptr.
func ResetPerfCounters() {
	perfCalls.Store(0)
	perfInput.Store(0)
	perfOutputLen.Store(0)
	perfOutputPointer.Store(0)
}
func ReadPerfCounters() (calls, input, lastOutputLen int64, lastOutput uintptr) {
	return perfCalls.Load(), perfInput.Load(), perfOutputLen.Load(), perfOutputPointer.Load()
}
