//go:build gocql_perf

// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package wire

import "sync/atomic"

// PerfCounters counts operations on real loops, only in measurement builds.
// Read/reset require an otherwise quiescent measured workload.
type PerfCounters struct {
	DescriptorVisits, IndexComparisons, SequentialVisits, Takes, CopyBytes, ScalarWrites, ScalarBytes, Retains, Releases int64
}

var perfCounts [9]atomic.Int64

func perfAdd(kind, n int) { perfCounts[kind].Add(int64(n)) }
func ResetPerfCounters() {
	for i := range perfCounts {
		perfCounts[i].Store(0)
	}
}
func ReadPerfCounters() PerfCounters {
	return PerfCounters{perfCounts[0].Load(), perfCounts[1].Load(), perfCounts[2].Load(), perfCounts[3].Load(), perfCounts[4].Load(), perfCounts[5].Load(), perfCounts[6].Load(), perfCounts[7].Load(), perfCounts[8].Load()}
}

var perfLive, perfPeak, perfSlabBytes atomic.Int64

func perfLease(delta int64) {
	n := perfLive.Add(delta)
	for old := perfPeak.Load(); n > old; old = perfPeak.Load() {
		if perfPeak.CompareAndSwap(old, n) {
			break
		}
	}
}
func perfSlab(n int) { perfSlabBytes.Add(int64(n)) }

// ReadPerfMemoryCounters counts logical frame bytes charged per owner, not
// backing capacities or deduplicated physical aliases. Reset only with no live leases.
func ReadPerfMemoryCounters() (live, peak, slabAllocated int64) {
	return perfLive.Load(), perfPeak.Load(), perfSlabBytes.Load()
}

// ResetPerfMemoryCounters requires no outstanding owners.
func ResetPerfMemoryCounters() { perfLive.Store(0); perfPeak.Store(0); perfSlabBytes.Store(0) }
