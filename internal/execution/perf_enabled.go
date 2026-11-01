//go:build gocql_perf

// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package execution

import "sync/atomic"

type PerfCounters struct{ RequestEvents, AttemptCallbacks, SpeculationSnapshots, SpeculationTimers, PrefetchWorkers, RetryTimers int64 }

var perfCounts [6]atomic.Int64

func perfAdd(kind, n int) { perfCounts[kind].Add(int64(n)) }

// ResetPerfCounters requires a quiescent measured workload.
func ResetPerfCounters() {
	for i := range perfCounts {
		perfCounts[i].Store(0)
	}
}

// ReadPerfCounters returns counts from real work sites.
func ReadPerfCounters() PerfCounters {
	return PerfCounters{perfCounts[0].Load(), perfCounts[1].Load(), perfCounts[2].Load(), perfCounts[3].Load(), perfCounts[4].Load(), perfCounts[5].Load()}
}
