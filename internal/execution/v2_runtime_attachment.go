// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package execution

import (
	"context"
	implCluster "github.com/scylladb/gocql/v2/internal/cluster"
)

func (rt *InternalSessionRuntime) StopPrepared() implCluster.InternalExecutionWaiter {
	rt.prepareWorkersMu.Lock()
	rt.prepareWorkersClosed = true
	rt.prepareWorkersMu.Unlock()
	return &rt.prepareWorkers
}

type InternalActiveExecutionDrain []*InternalLogicalExecution

func (d InternalActiveExecutionDrain) Wait() {
	for _, l := range d {
		<-l.lifetime.done
	}
}
func (rt *InternalSessionRuntime) StopActive() implCluster.InternalExecutionWaiter {
	rt.InternalMu.Lock()
	queries := make([]*queryState, 0, len(rt.queries))
	for q := range rt.queries {
		queries = append(queries, q)
	}
	rt.InternalMu.Unlock()
	active := make([]*InternalLogicalExecution, 0, len(queries))
	// Stop admission and cancel every reusable query before joining callbacks.
	for _, q := range queries {
		if l := q.stop(); l != nil {
			active = append(active, l)
		}
	}
	legacy := rt.stopExecutions()
	for _, l := range active {
		l.shutdown()
	}
	return queryDrain{queries: queries, legacy: InternalActiveExecutionDrain(legacy)}
}

type queryDrain struct {
	queries []*queryState
	legacy  InternalActiveExecutionDrain
}

func (d queryDrain) Wait() {
	d.legacy.Wait()
	for _, q := range d.queries {
		_ = q.wait(context.Background())
	}
}
func (rt *InternalSessionRuntime) MetricsSnapshot() MetricsSnapshot { return rt.metricsSnapshot() }
func (rt *InternalSessionRuntime) ProfileHandle() *ProfileHandle    { return rt.InternalProfile }
func (rt *InternalSessionRuntime) ConfigSnapshot() Config           { return rt.InternalConfig }
