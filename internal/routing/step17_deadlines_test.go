// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package routing

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func step17DeadlineCases() []struct {
	name   string
	now    time.Time
	period time.Duration
} {
	return []struct {
		name   string
		now    time.Time
		period time.Duration
	}{
		{"ordinary-monotonic", time.Now(), time.Minute},
		{"cross-UnixNano-2262", time.Date(2262, 4, 11, 23, 47, 16, 800000000, time.UTC), time.Minute},
		{"maximum-positive-duration", time.Now(), time.Duration(1<<63 - 1)},
		{"maximum-after-2262", time.Date(2262, 4, 11, 23, 47, 16, 800000000, time.UTC), time.Duration(1<<63 - 1)},
	}
}

func TestStep17LatencyProbeDeadlineRange(t *testing.T) {
	for _, test := range step17DeadlineCases() {
		t.Run(test.name, func(t *testing.T) {
			policy := &InternalRoutingPolicy{InternalOptions: RoutingOptions{Latency: LatencyOptions{Enabled: true, MinimumSamples: 1, Scale: time.Millisecond, RetryPeriod: test.period, UpdateInterval: time.Hour}}}
			host := UUID{15: 1}
			sample := &InternalRoutingLatency{nextProbe: test.now.Add(-time.Nanosecond)}
			policy.InternalLatencies.Store(host, sample)
			decisions := &InternalRoutingDecisions{InternalSlow: map[UUID]time.Time{host: test.now.Add(-time.Nanosecond)}}
			start := make(chan struct{})
			var group sync.WaitGroup
			var probes atomic.Int32
			for range 32 {
				group.Add(1)
				go func() {
					defer group.Done()
					<-start
					plan := &InternalLazyRoutingPlan{policy: policy, InternalSlow: decisions, InternalNow: test.now}
					if !plan.InternalPenalty(Target{HostID: host}) {
						probes.Add(1)
					}
				}()
			}
			close(start)
			group.Wait()
			if probes.Load() != 1 {
				t.Fatal("concurrent probes", probes.Load())
			}
			sample.mu.Lock()
			deadline := sample.nextProbe
			sample.mu.Unlock()
			if !deadline.Equal(test.now.Add(test.period)) {
				t.Fatal("configured duration narrowed", deadline, test.now.Add(test.period))
			}
			// A backwards clock cannot expire a claim. An abandoned claim expires
			// at the exact configured time, including dates beyond UnixNano's range.
			plan := &InternalLazyRoutingPlan{policy: policy, InternalSlow: decisions, InternalNow: test.now.Add(-time.Second)}
			if !plan.InternalPenalty(Target{HostID: host}) {
				t.Fatal("clock reversal expired claim")
			}
			plan.InternalNow = deadline.Add(-time.Nanosecond)
			if !plan.InternalPenalty(Target{HostID: host}) {
				t.Fatal("claim expired early")
			}
			plan.InternalNow = deadline
			if plan.InternalPenalty(Target{HostID: host}) {
				t.Fatal("abandoned claim did not expire")
			}
			policy.Observe(AttemptEvent{Target: Target{HostID: host}, Started: test.now, Latency: time.Millisecond})
			sample.mu.Lock()
			observed := sample.nextProbe
			sample.mu.Unlock()
			want := test.now.Add(time.Millisecond).Add(test.period)
			if !observed.Equal(want) {
				t.Fatal("Observe deadline narrowed", observed, want)
			}
		})
	}
}

func TestStep17LatencyPublicationDeadlineRange(t *testing.T) {
	for _, test := range step17DeadlineCases() {
		t.Run(test.name, func(t *testing.T) {
			policy := &InternalRoutingPolicy{InternalOptions: RoutingOptions{Latency: LatencyOptions{UpdateInterval: test.period}}}
			start := make(chan struct{})
			var group sync.WaitGroup
			var claims atomic.Int32
			for range 32 {
				group.Add(1)
				go func() {
					defer group.Done()
					<-start
					if policy.claimLatencyUpdate(test.now) {
						claims.Add(1)
					}
				}()
			}
			close(start)
			group.Wait()
			if claims.Load() != 1 {
				t.Fatal("concurrent publication claims", claims.Load())
			}
			winner := policy.updateAfter.Load()
			if winner == nil || !winner.Equal(test.now.Add(test.period)) {
				t.Fatal("publication deadline narrowed", winner)
			}
			for _, clock := range []time.Time{test.now.Add(-time.Hour), test.now, test.now.Add(test.period).Add(-time.Nanosecond)} {
				if policy.claimLatencyUpdate(clock) {
					t.Fatal("early/backwards publication claim", clock)
				}
				if policy.updateAfter.Load() != winner {
					t.Fatal("losing claim replaced winner")
				}
			}
			if !policy.claimLatencyUpdate(*winner) {
				t.Fatal("publication did not become due")
			}
			policy.updateAfter.Store(nil)
			if !policy.claimLatencyUpdate(test.now) {
				t.Fatal("new minimum could not reschedule publication")
			}
		})
	}
}

func TestStep17LatencyWarmDeadlineAllocations(t *testing.T) {
	now := time.Now()
	policy := &InternalRoutingPolicy{InternalOptions: RoutingOptions{Latency: LatencyOptions{RetryPeriod: time.Minute, UpdateInterval: time.Hour}}}
	if !policy.claimLatencyUpdate(now) {
		t.Fatal("initial publication claim")
	}
	if got := testing.AllocsPerRun(1000, func() {
		if policy.claimLatencyUpdate(now) {
			t.Fatal("warm claim succeeded")
		}
	}); got != 0 {
		t.Fatal("warm publication allocated", got)
	}
	host := UUID{15: 1}
	sample := &InternalRoutingLatency{nextProbe: now.Add(time.Minute)}
	policy.InternalLatencies.Store(host, sample)
	plan := &InternalLazyRoutingPlan{policy: policy, InternalSlow: &InternalRoutingDecisions{InternalSlow: map[UUID]time.Time{host: now.Add(-time.Second)}}, InternalNow: now}
	if got := testing.AllocsPerRun(1000, func() {
		if !plan.InternalPenalty(Target{HostID: host}) {
			t.Fatal("held probe claim lost")
		}
	}); got != 0 {
		t.Fatal("held probe check allocated", got)
	}
	if got := testing.AllocsPerRun(1000, func() {
		sample.mu.Lock()
		sample.nextProbe = now.Add(-time.Nanosecond)
		sample.mu.Unlock()
		if plan.InternalPenalty(Target{HostID: host}) {
			t.Fatal("expired probe not claimed")
		}
	}); got != 0 {
		t.Fatal("expired probe selection allocated", got)
	}
}

func BenchmarkStep17LatencyPublicationClaims(b *testing.B) {
	for _, kind := range []string{"warm", "boundary", "competing-boundary"} {
		b.Run(kind, func(b *testing.B) {
			policy := &InternalRoutingPolicy{InternalOptions: RoutingOptions{Latency: LatencyOptions{UpdateInterval: time.Minute}}}
			now := time.Now()
			policy.claimLatencyUpdate(now)
			b.ReportAllocs()
			b.ResetTimer()
			var claims atomic.Int64
			if kind == "competing-boundary" {
				policy.InternalOptions.Latency.UpdateInterval = time.Nanosecond
				policy.updateAfter.Store(nil)
				b.RunParallel(func(pb *testing.PB) {
					var local int64
					for pb.Next() {
						if policy.claimLatencyUpdate(time.Now()) {
							local++
						}
					}
					claims.Add(local)
				})
				b.StopTimer()
				b.ReportMetric(float64(claims.Load())/float64(b.N), "claims/op")
				b.ReportMetric(float64(claims.Load()), "total-claims")
				return
			}
			var local int64
			for i := 0; i < b.N; i++ {
				if kind == "boundary" {
					now = now.Add(2 * time.Minute)
				}
				if policy.claimLatencyUpdate(now) {
					local++
				}
			}
			b.StopTimer()
			b.ReportMetric(float64(local)/float64(b.N), "claims/op")
			b.ReportMetric(float64(local), "total-claims")
		})
	}
}

func BenchmarkStep17LatencyProbeDeadline(b *testing.B) {
	for _, test := range step17DeadlineCases() {
		b.Run(fmt.Sprintf("%s/claim-and-reset", test.name), func(b *testing.B) {
			host := UUID{15: 1}
			sample := &InternalRoutingLatency{}
			policy := &InternalRoutingPolicy{InternalOptions: RoutingOptions{Latency: LatencyOptions{RetryPeriod: test.period}}}
			policy.InternalLatencies.Store(host, sample)
			plan := &InternalLazyRoutingPlan{policy: policy, InternalSlow: &InternalRoutingDecisions{InternalSlow: map[UUID]time.Time{host: test.now.Add(-time.Second)}}, InternalNow: test.now}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				sample.mu.Lock()
				sample.nextProbe = test.now.Add(-time.Nanosecond)
				sample.mu.Unlock()
				if plan.InternalPenalty(Target{HostID: host}) {
					b.Fatal("missing probe")
				}
			}
		})
	}
}
