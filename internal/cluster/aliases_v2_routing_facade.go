// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cluster

import (
	implMetadata "github.com/scylladb/gocql/v2/internal/metadata"
	model "github.com/scylladb/gocql/v2/internal/model"
	routing "github.com/scylladb/gocql/v2/internal/routing"
	runtimeerrors "github.com/scylladb/gocql/v2/internal/runtimeerrors"
	"github.com/scylladb/gocql/v2/tablets"
)

type AggregateMetadata = implMetadata.AggregateMetadata

type ColumnMetadata = implMetadata.ColumnMetadata

var ErrClosed = runtimeerrors.ErrClosed

var ErrKeyspaceDoesNotExist = runtimeerrors.ErrKeyspaceDoesNotExist

var ErrNoConnections = runtimeerrors.ErrNoConnections

var ErrNoKeyspace = runtimeerrors.ErrNoKeyspace

var ErrNoRows = runtimeerrors.ErrNoRows

var ErrNoTable = runtimeerrors.ErrNoTable
var ErrNotFound = runtimeerrors.ErrNotFound

var ErrSessionClosed = runtimeerrors.ErrSessionClosed
var ErrSessionNotReady = runtimeerrors.ErrSessionNotReady
var ErrTabletsNotUsed = runtimeerrors.ErrTabletsNotUsed

type FunctionMetadata = implMetadata.FunctionMetadata
type HostSnapshot = model.HostSnapshot

type IndexMetadata = implMetadata.IndexMetadata
type KeyspaceMetadata = implMetadata.KeyspaceMetadata

type MetadataSnapshot = routing.MetadataSnapshot
type MetricsSnapshot = routing.MetricsSnapshot

type QueryPlan = routing.QueryPlan

type RoutingInfo = model.RoutingInfo

type RoutingPolicy = routing.RoutingPolicy
type SafeRetryPolicy = routing.SafeRetryPolicy

type TableMetadata = implMetadata.TableMetadata

type Target = model.Target
type TokenPartitioner = routing.TokenPartitioner
type TokenRange = routing.TokenRange
type TypeMetadata = implMetadata.TypeMetadata
type ViewMetadata = implMetadata.ViewMetadata

func buildRoutingHostIndex(a0 []HostSnapshot) *routingHostIndex {
	return routing.InternalBuildRoutingHostIndex(a0)
}
func clonePlacementSnapshot(a0 *MetadataSnapshot) *MetadataSnapshot {
	return routing.InternalClonePlacementSnapshot(a0)
}

func cloneV2Keyspace(a0 *KeyspaceMetadata) *KeyspaceMetadata {
	return implMetadata.InternalCloneV2Keyspace(a0)
}

func compareInterfaceMaps(a0 map[string]any, a1 map[string]any) bool {
	return implMetadata.InternalCompareInterfaceMaps(a0, a1)
}
func comparePlacementTable(a0 metadataTable, a1 metadataTable) int {
	return routing.InternalComparePlacementTable(a0, a1)
}

type cowKeyspaceMetadataMap = implMetadata.InternalCowKeyspaceMetadataMap

type metadataTable = routing.InternalMetadataTable

func observeTabletRanges(a0 *tabletRangeTree, a1 int64, a2 int64) tabletRangeObservation {
	return routing.InternalObserveTabletRanges(a0, a1, a2)
}
func placementEach[K any, V any](a0 *placementTree[K, V], a1 func(K, V)) {
	routing.InternalPlacementEach[K, V](a0, a1)
}
func placementGet[K any, V any](a0 *placementTree[K, V], a1 K, a2 func(K, K) int) *placementTree[K, V] {
	return routing.InternalPlacementGet[K, V](a0, a1, a2)
}
func placementPut[K any, V any](a0 *placementTree[K, V], a1 K, a2 V, a3 func(K, K) int) *placementTree[K, V] {
	return routing.InternalPlacementPut[K, V](a0, a1, a2, a3)
}
func reconcileTabletPlacement(a0 *MetadataSnapshot, a1 *MetadataSnapshot) *tabletPlacementTree {
	return routing.InternalReconcileTabletPlacement(a0, a1)
}

type routingPolicy = routing.InternalRoutingPolicy

func sameTabletObservation(a0 *tabletRange, a1 tablets.TabletInfo) bool {
	return routing.InternalSameTabletObservation(a0, a1)
}

type tabletFeedbackStamp = routing.InternalTabletFeedbackStamp
type tabletPlacement = routing.InternalTabletPlacement
type tabletRange = routing.InternalTabletRange

func tabletRangeAfter(a0 *tabletRangeTree, a1 int64) *tabletRangeTree {
	return routing.InternalTabletRangeAfter(a0, a1)
}

type tabletRangeObservation = routing.InternalTabletRangeObservation
type tabletRangeTree = routing.InternalTabletRangeTree

func updateTabletRange(a0 *tabletRangeTree, a1 tablets.TabletInfo) (*tabletRangeTree, bool) {
	return routing.InternalUpdateTabletRange(a0, a1)
}

type routingHostIndex = routing.InternalRoutingHostIndex
type placementTree[K, V any] = routing.InternalPlacementTree[K, V]
type tabletPlacementTree = routing.InternalTabletPlacementTree
