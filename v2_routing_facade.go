package gocql

import (
	implMetadata "github.com/scylladb/gocql/v2/internal/metadata"
	model "github.com/scylladb/gocql/v2/internal/model"
	routing "github.com/scylladb/gocql/v2/internal/routing"
	runtimeerrors "github.com/scylladb/gocql/v2/internal/runtimeerrors"
	"github.com/scylladb/gocql/v2/tablets"
)

const ASC = implMetadata.ASC

type AggregateMetadata = implMetadata.AggregateMetadata
type AttemptError = routing.AttemptError
type AttemptEvent = routing.AttemptEvent

var CDCTokenPartitioner = routing.CDCTokenPartitioner

const ColumnClusteringKey = implMetadata.ColumnClusteringKey
const ColumnCompact = implMetadata.ColumnCompact

type ColumnIndexMetadata = implMetadata.ColumnIndexMetadata
type ColumnKind = implMetadata.ColumnKind
type ColumnMetadata = implMetadata.ColumnMetadata
type ColumnOrder = implMetadata.ColumnOrder

const ColumnPartitionKey = implMetadata.ColumnPartitionKey
const ColumnRegular = implMetadata.ColumnRegular
const ColumnStatic = implMetadata.ColumnStatic
const ColumnUnkownKind = implMetadata.ColumnUnkownKind
const DESC = implMetadata.DESC

var ErrClosed = runtimeerrors.ErrClosed
var ErrConnectionClosed = runtimeerrors.ErrConnectionClosed
var ErrHostDown = runtimeerrors.ErrHostDown
var ErrKeyspaceDoesNotExist = runtimeerrors.ErrKeyspaceDoesNotExist
var ErrMultipleRows = runtimeerrors.ErrMultipleRows
var ErrNoConnections = runtimeerrors.ErrNoConnections
var ErrNoConnectionsInPool = runtimeerrors.ErrNoConnectionsInPool
var ErrNoKeyspace = runtimeerrors.ErrNoKeyspace
var ErrNoMetadata = runtimeerrors.ErrNoMetadata
var ErrNoPool = runtimeerrors.ErrNoPool
var ErrNoRows = runtimeerrors.ErrNoRows
var ErrNoStreams = runtimeerrors.ErrNoStreams
var ErrNoTable = runtimeerrors.ErrNoTable
var ErrNotFound = runtimeerrors.ErrNotFound
var ErrPagingComplete = runtimeerrors.ErrPagingComplete
var ErrPagingNoProgress = runtimeerrors.ErrPagingNoProgress
var ErrPagingStateLimit = runtimeerrors.ErrPagingStateLimit
var ErrQueryArgLength = runtimeerrors.ErrQueryArgLength
var ErrSessionClosed = runtimeerrors.ErrSessionClosed
var ErrSessionNotReady = runtimeerrors.ErrSessionNotReady
var ErrTabletsNotUsed = runtimeerrors.ErrTabletsNotUsed
var ErrTimeoutNoResponse = runtimeerrors.ErrTimeoutNoResponse
var ErrTooManyStmts = runtimeerrors.ErrTooManyStmts
var ErrTooManyTimeouts = runtimeerrors.ErrTooManyTimeouts
var ErrUnavailable = runtimeerrors.ErrUnavailable
var ErrUnsupported = runtimeerrors.ErrUnsupported
var ErrUseStmt = runtimeerrors.ErrUseStmt

type FunctionMetadata = implMetadata.FunctionMetadata
type HostSnapshot = model.HostSnapshot

const IgnoreWriteError = routing.IgnoreWriteError
const IndexKindCustom = implMetadata.IndexKindCustom

type IndexMetadata = implMetadata.IndexMetadata
type KeyspaceMetadata = implMetadata.KeyspaceMetadata
type LatencyOptions = routing.LatencyOptions
type MetadataSnapshot = routing.MetadataSnapshot
type MetricsSnapshot = routing.MetricsSnapshot

var Murmur3TokenPartitioner = routing.Murmur3TokenPartitioner

func NewRoutingPolicy(a0 RoutingOptions) (RoutingPolicy, error) { return routing.NewRoutingPolicy(a0) }

type PartitionKey = routing.PartitionKey
type QueryError = runtimeerrors.QueryError
type QueryPlan = routing.QueryPlan
type RetryAction = routing.RetryAction
type RetryDecision = routing.RetryDecision

const RetryNext = routing.RetryNext
const RetrySame = routing.RetrySame

type RoutingInfo = model.RoutingInfo
type RoutingOptions = routing.RoutingOptions
type RoutingPolicy = routing.RoutingPolicy
type SafeRetryPolicy = routing.SafeRetryPolicy
type SchemaChangedError = runtimeerrors.SchemaChangedError

func SingleTargetRouting(a0 Target, a1 bool) RoutingPolicy {
	return routing.SingleTargetRouting(a0, a1)
}

const StopRetry = routing.StopRetry

type TableMetadata = implMetadata.TableMetadata
type TableMetadataOptions = implMetadata.TableMetadataOptions
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
func cloneV2AnyMap(a0 map[string]any) map[string]any { return implMetadata.InternalCloneV2AnyMap(a0) }
func cloneV2Keyspace(a0 *KeyspaceMetadata) *KeyspaceMetadata {
	return implMetadata.InternalCloneV2Keyspace(a0)
}
func columnKindFromSchema(a0 string) (ColumnKind, error) {
	return implMetadata.InternalColumnKindFromSchema(a0)
}
func compareInterfaceMaps(a0 map[string]any, a1 map[string]any) bool {
	return implMetadata.InternalCompareInterfaceMaps(a0, a1)
}
func comparePlacementTable(a0 metadataTable, a1 metadataTable) int {
	return routing.InternalComparePlacementTable(a0, a1)
}
func computeRoutingToken(a0 []encodedCell, a1 []int, a2 TokenPartitioner) (Option[int64], error) {
	return routing.InternalComputeRoutingToken(a0, a1, a2)
}

type cowKeyspaceMetadataMap = implMetadata.InternalCowKeyspaceMetadataMap

var cqlHelpers = implMetadata.InternalCqlHelpers
var cqlNativeTypes = implMetadata.InternalCqlNativeTypes

func cqlTypeIdentifiers(a0 string) []cqlTypeRef { return implMetadata.InternalCqlTypeIdentifiers(a0) }

type cqlTypeRef = implMetadata.InternalCqlTypeRef
type lazyRoutingPlan = routing.InternalLazyRoutingPlan
type metadataTable = routing.InternalMetadataTable

func newPartitionKey(a0 []encodedCell, a1 []int) (PartitionKey, bool, error) {
	return routing.InternalNewPartitionKey(a0, a1)
}
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

type routingDecisions = routing.InternalRoutingDecisions
type routingLatency = routing.InternalRoutingLatency

func routingLatencySample(a0 error) bool { return routing.InternalRoutingLatencySample(a0) }

type routingPolicy = routing.InternalRoutingPolicy

func routingToken(a0 []encodedCell, a1 []int) (Option[int64], error) {
	return routing.InternalRoutingToken(a0, a1)
}
func sameTabletObservation(a0 *tabletRange, a1 tablets.TabletInfo) bool {
	return routing.InternalSameTabletObservation(a0, a1)
}

type scyllaEncryptionOptions = implMetadata.InternalScyllaEncryptionOptions
type tabletFeedbackStamp = routing.InternalTabletFeedbackStamp
type tabletPlacement = routing.InternalTabletPlacement
type tabletRange = routing.InternalTabletRange

func tabletRangeAfter(a0 *tabletRangeTree, a1 int64) *tabletRangeTree {
	return routing.InternalTabletRangeAfter(a0, a1)
}
func tabletRangeFor(a0 *tabletRangeTree, a1 int64) *tabletRangeTree {
	return routing.InternalTabletRangeFor(a0, a1)
}

type tabletRangeObservation = routing.InternalTabletRangeObservation
type tabletRangeTree = routing.InternalTabletRangeTree
type targetPlan = routing.InternalTargetPlan

func updateTabletRange(a0 *tabletRangeTree, a1 tablets.TabletInfo) (*tabletRangeTree, bool) {
	return routing.InternalUpdateTabletRange(a0, a1)
}

type routingHostIndex = routing.InternalRoutingHostIndex
type placementTree[K, V any] = routing.InternalPlacementTree[K, V]
type tabletPlacementTree = routing.InternalTabletPlacementTree

func comparePlacementToken(a, b int64) int { return routing.InternalComparePlacementToken(a, b) }
