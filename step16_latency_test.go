// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package gocql

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	routing "github.com/scylladb/gocql/v2/internal/routing"
)

func TestStep16LatencyProbeReservation(t *testing.T) {
	policy := codecTestMust(NewRoutingPolicy(RoutingOptions{TokenAware: true, Latency: LatencyOptions{Enabled: true, MinimumSamples: 1, RetryPeriod: time.Minute, UpdateInterval: time.Hour}})).(*routingPolicy)
	metadata := v2TestMetadata()
	slow := UUID{15: 1}
	healthy := UUID{15: 2}
	// Exactly one replica keeps first-target selection deterministic. A healthy
	// nonreplica must be preferred while another plan owns the retry probe.
	routing.SnapshotData(metadata).InternalRanges["ks"][0].Replicas = []Target{{HostID: slow}}
	policy.Observe(AttemptEvent{Target: Target{HostID: healthy}, Started: time.Now().Add(-2 * time.Minute), Latency: time.Millisecond})
	policy.Observe(AttemptEvent{Target: Target{HostID: slow}, Started: time.Now().Add(-2 * time.Minute), Latency: 10 * time.Millisecond})
	// Publish both real samples; the regular hourly updater is deliberately idle.
	policy.InternalDecisions.Store(&routing.InternalRoutingDecisions{InternalSlow: map[UUID]time.Time{slow: time.Now().Add(-time.Minute)}})
	info := RoutingInfo{Keyspace: "ks", Table: "table", Token: Option[int64]{Set: true}, Kind: OrdinaryStatement}
	// Constructing an abandoned plan must not reserve a probe.
	_ = policy.Plan(info, metadata)
	start := make(chan struct{})
	var group sync.WaitGroup
	var probes atomic.Int32
	for range 32 {
		group.Add(1)
		go func() {
			defer group.Done()
			<-start
			target, ok := policy.Plan(info, metadata).Next()
			if !ok {
				t.Error("missing target")
				return
			}
			if target.HostID == slow {
				probes.Add(1)
			}
		}()
	}
	close(start)
	group.Wait()
	if got := probes.Load(); got != 1 {
		t.Fatalf("expired slow replica got %d concurrent first-target probes; want one", got)
	}
	// An emitted target abandoned before Observe cannot lock out future probes.
	// Advancing the plan clock avoids a timing-dependent minute-long test.
	future := time.Now().Add(2 * time.Minute)
	for attempt := 0; attempt < 2; attempt++ {
		plan := policy.Plan(info, metadata).(*lazyRoutingPlan)
		plan.InternalNow = future
		target, ok := plan.Next()
		if !ok || (target.HostID == slow) != (attempt == 0) {
			t.Fatal("abandoned probe did not expire/reserve once", attempt, target, ok)
		}
	}
	// A completed probe restarts its retry interval even before the decision
	// updater publishes again; sharing a policy shares the reservation state.
	policy.Observe(AttemptEvent{Target: Target{HostID: slow}, Started: future, Latency: 10 * time.Millisecond})
	plan := policy.Plan(info, metadata).(*lazyRoutingPlan)
	plan.InternalNow = future.Add(20 * time.Millisecond)
	if target, ok := plan.Next(); !ok || target.HostID == slow {
		t.Fatal("completed probe did not restart exclusion", target, ok)
	}
}
