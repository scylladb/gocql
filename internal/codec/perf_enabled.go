//go:build gocql_perf

// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package codec

import "sync/atomic"

var perfCompiles, perfNameMaps atomic.Int64

func perfCompile() { perfCompiles.Add(1) }
func perfNameMap() { perfNameMaps.Add(1) }

// ResetPerfCounters requires a quiescent workload.
func ResetPerfCounters() {
	perfDynamicBuilds.Store(0)
	perfDynamicCells.Store(0)
	perfTypeChecks.Store(0)
	perfCompiles.Store(0)
	perfNameMaps.Store(0)
}

// ReadPerfCounters reports actual compiler and name mapping calls.
func ReadPerfCounters() (compiles, nameMaps int64) { return perfCompiles.Load(), perfNameMaps.Load() }

var perfTypeChecks atomic.Int64

func perfCheckType() { perfTypeChecks.Add(1) }

// ReadPerfTypeChecks reports real built-in CheckType calls, separately from
// constant metadata identity/kind guards on already compiled immutable plans.
func ReadPerfTypeChecks() int64 { return perfTypeChecks.Load() }

var perfDynamicBuilds, perfDynamicCells atomic.Int64

func perfDynamicIndex(cells int) { perfDynamicBuilds.Add(1); perfDynamicCells.Add(int64(cells)) }
func ReadPerfDynamicIndexCounters() (builds, cells int64) {
	return perfDynamicBuilds.Load(), perfDynamicCells.Load()
}
