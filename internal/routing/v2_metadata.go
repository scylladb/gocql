// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package routing

import (
	"fmt"

	model "github.com/scylladb/gocql/v2/internal/model"

	implMetadata "github.com/scylladb/gocql/v2/internal/metadata"
	runtimeerrors "github.com/scylladb/gocql/v2/internal/runtimeerrors"
	"sync/atomic"
	"time"
)

type TokenRange struct {
	Start, End int64
	Replicas   []Target
}
type MetadataSnapshot struct {
	state SnapshotState
}
type InternalMetadataTable = model.TableKey

func (m *MetadataSnapshot) Generation() uint64 {
	if m == nil {
		return 0
	}
	return SnapshotData(m).InternalGeneration
}
func (m *MetadataSnapshot) Hosts() []HostSnapshot {
	if m == nil {
		return nil
	}
	out := append([]HostSnapshot(nil), SnapshotData(m).InternalHosts...)
	for i := range out {
		out[i].Tokens = append([]int64(nil), out[i].Tokens...)
	}
	return out
}
func (m *MetadataSnapshot) Keyspace(name string) (*implMetadata.KeyspaceMetadata, bool) {
	if m == nil {
		return nil, false
	}
	ks, ok := SnapshotData(m).InternalKeyspaces[name]
	if !ok {
		return nil, false
	}
	return implMetadata.InternalCloneV2Keyspace(ks), true
}
func (m *MetadataSnapshot) Replicas(keyspace, table string, token int64) ([]Target, error) {
	if _, _, err := InternalMetadataSnapshotLookupRanges(m, keyspace, table); err != nil {
		return nil, err
	}
	r, _ := m.placementReplicas(keyspace, table, token)
	return append([]Target(nil), r...), nil
}
func (m *MetadataSnapshot) TokenRanges(keyspace, table string) ([]TokenRange, error) {
	if root, ok := InternalMetadataSnapshotTabletRoot(m, InternalMetadataTable{InternalKeyspace: keyspace, InternalTable: table}); ok {
		if _, _, err := InternalMetadataSnapshotLookupRanges(m, keyspace, table); err != nil {
			return nil, err
		}
		var out []TokenRange
		InternalPlacementEach(root, func(_ int64, r *InternalTabletRange) {
			v := r.InternalSpan
			v.Replicas = append([]Target(nil), v.Replicas...)
			out = append(out, v)
		})
		return out, nil
	}
	r, _, e := InternalMetadataSnapshotLookupRanges(m, keyspace, table)
	if e != nil {
		return nil, e
	}
	out := append([]TokenRange(nil), r...)
	for i := range out {
		out[i].Replicas = append([]Target(nil), out[i].Replicas...)
	}
	return out, nil
}
func InternalMetadataSnapshotLookupRanges(m *MetadataSnapshot, keyspace, table string) ([]TokenRange, bool, error) {
	if m == nil {
		return nil, false, runtimeerrors.ErrNoKeyspace
	}
	ks, ok := SnapshotData(m).InternalKeyspaces[keyspace]
	if !ok {
		return nil, false, fmt.Errorf("%s: %w", keyspace, runtimeerrors.ErrNoKeyspace)
	}
	if table != "" {
		if _, ok := ks.Tables[table]; !ok {
			if _, ok := ks.Views[table]; !ok {
				return nil, false, fmt.Errorf("%s.%s: %w", keyspace, table, runtimeerrors.ErrNoTable)
			}
		}
		if r, ok := SnapshotData(m).InternalTablets[InternalMetadataTable{InternalKeyspace: keyspace, InternalTable: table}]; ok {
			return r, true, nil
		}
	}
	return SnapshotData(m).InternalRanges[keyspace], false, nil
}

// Diagnostics reports routing/metadata omissions without substituting fabricated tokens.
func (m *MetadataSnapshot) Diagnostics() []error {
	if m == nil {
		return nil
	}
	return append([]error(nil), SnapshotData(m).InternalDiagnostics...)
}

type MetricsSnapshot struct {
	ReceiveCacheHits, ReceiveCacheMisses                                  uint64
	ReceiveCacheBytes, ReceiveCacheObjects                                int
	Requests, Attempts, Errors, Retries, Speculations, Cancelled, Orphans uint64
	InFlight                                                              int64
	LatencyP50, LatencyP95, LatencyP99                                    time.Duration
}
type SnapshotState struct {
	InternalHostIndex       *InternalRoutingHostIndex
	InternalPlacements      *InternalTabletPlacementTree
	InternalPlacementEpoch  uint64
	InternalPlacementSignal *atomic.Uint64
	InternalGeneration      uint64
	InternalHosts           []HostSnapshot
	InternalKeyspaces       map[string]*implMetadata.KeyspaceMetadata
	InternalSchemaSources   map[string]*implMetadata.KeyspaceMetadata
	InternalRanges          map[string][]TokenRange
	InternalTablets         map[InternalMetadataTable][]TokenRange
	InternalPartitioner     string
	InternalDiagnostics     []error
}

type HostSnapshot = model.HostSnapshot
