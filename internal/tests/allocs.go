package tests

import "testing"

// MinAllocsPerRun is testing.AllocsPerRun sampled several times, keeping the
// smallest result.
//
// AllocsPerRun brackets its loop with process-wide malloc counters, so any
// goroutine still alive from an earlier test is counted too, and it divides as
// integers -- a guard over n runs therefore only tolerates n-1 stray
// allocations. Pollution can only add, so the minimum converges on the real
// count. Use it wherever the run count is too small to absorb the noise.
func MinAllocsPerRun(samples, runs int, f func()) float64 {
	best := testing.AllocsPerRun(runs, f)
	for i := 1; i < samples; i++ {
		best = min(best, testing.AllocsPerRun(runs, f))
	}
	return best
}
