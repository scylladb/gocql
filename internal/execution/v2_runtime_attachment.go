// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package execution

import (
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
	return InternalActiveExecutionDrain(InternalRuntimeFor(rt.Session).stopExecutions())
}
func (rt *InternalSessionRuntime) MetricsSnapshot() MetricsSnapshot { return rt.metricsSnapshot() }
func (rt *InternalSessionRuntime) ProfileHandle() *ProfileHandle    { return rt.InternalProfile }
func (rt *InternalSessionRuntime) ConfigSnapshot() Config           { return rt.InternalConfig }
