// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package gocql

import (
	"context"
	"crypto/tls"
	x509 "crypto/x509"
	implCluster "github.com/scylladb/gocql/v2/internal/cluster"
	implExecution "github.com/scylladb/gocql/v2/internal/execution"
	logging "github.com/scylladb/gocql/v2/internal/logging"
	implMetadata "github.com/scylladb/gocql/v2/internal/metadata"
	implTransport "github.com/scylladb/gocql/v2/internal/transport"
	"io"
	"net"
	"net/netip"
	"time"
)

func retiredAcceptAllFilter() retiredHostFilter { return implCluster.AcceptAllFilter() }
func retiredAvoidSlowReplicas(a0 int) func(policy *tokenAwareHostPolicy) {
	return implCluster.AvoidSlowReplicas(a0)
}
func BatchEntry[R any](a0 *Bound[R]) (BatchItem, error) { return implExecution.BatchEntry[R](a0) }

type BatchItem = implExecution.BatchItem

const BatchSizeMaximum = implCluster.BatchSizeMaximum

func BindBorrowed[P any, R any](a0 *Prepared[P, R], a1 P) (*Bound[R], error) {
	return implExecution.BindBorrowed[P, R](a0, a1)
}
func BindOwned[P any, R any](a0 *Prepared[P, R], a1 P) (*Bound[R], error) {
	return implExecution.BindOwned[P, R](a0, a1)
}
func BindUnpreparedBorrowed[P any, R any](a0 Statement[P, R], a1 Columns, a2 P) (*Bound[R], error) {
	return implExecution.BindUnpreparedBorrowed[P, R](a0, a1, a2)
}
func BindUnpreparedOwned[P any, R any](a0 Statement[P, R], a1 Columns, a2 P) (*Bound[R], error) {
	return implExecution.BindUnpreparedOwned[P, R](a0, a1, a2)
}

type Bound[R any] = implExecution.Bound[R]

const ClientRouteEvent = implCluster.ClientRouteEvent

type retiredClientRoutesConfig = implCluster.ClientRoutesConfig
type retiredClientRoutesEndpoint = implCluster.ClientRoutesEndpoint
type retiredClientRoutesEndpointList = implCluster.ClientRoutesEndpointList
type retiredClientRoutesHandler = implCluster.ClientRoutesHandler
type retiredClientRoutesOption = implCluster.ClientRoutesOption
type retiredClusterOption = implCluster.ClusterOption

func Connect(a0 context.Context, a1 Config) (*Session, error) { return implExecution.Connect(a0, a1) }

type retiredConnectObserver = implCluster.ConnectObserver

const ControlEvent = implCluster.ControlEvent

func retiredDCAwareRoundRobinPolicy(a0 string, a1 ...dcAwarePolicyOption) retiredHostSelectionPolicy {
	return implCluster.DCAwareRoundRobinPolicy(a0, a1...)
}
func retiredDataCenterHostFilter(a0 string) retiredHostFilter {
	return implCluster.DataCenterHostFilter(a0)
}
func retiredDataCentreHostFilter(a0 string) retiredHostFilter {
	return implCluster.DataCentreHostFilter(a0)
}
func retiredDenyAllFilter() retiredHostFilter { return implCluster.DenyAllFilter() }
func retiredDontShuffleReplicas() func(*tokenAwareHostPolicy) {
	return implCluster.DontShuffleReplicas()
}

var ErrHostQueryFailed = implCluster.ErrHostQueryFailed
var ErrNoConnectionsStarted = implCluster.ErrNoConnectionsStarted
var ErrNoHosts = implCluster.ErrNoHosts

type Error = implCluster.Error
type Event = implCluster.Event
type EventFilter = implCluster.EventFilter
type EventKind = implCluster.EventKind

func Exec[R any](a0 context.Context, a1 *Session, a2 *Bound[R], a3 Options) (Result, error) {
	return implExecution.Exec[R](a0, a1, a2, a3)
}
func ExecBatch(a0 context.Context, a1 *Session, a2 BatchKind, a3 []BatchItem, a4 Options) (Result, error) {
	return implExecution.ExecBatch(a0, a1, a2, a3, a4)
}

type ExecutionError = implExecution.ExecutionError
type retiredHostFilter = implCluster.HostFilter
type retiredHostFilterFunc = implCluster.HostFilterFunc

func retiredHostPolicyOptionDisableDCFailover(a0 dcFailoverDisabledPolicy) {
	implCluster.HostPolicyOptionDisableDCFailover(a0)
}

type retiredHostSelectionPolicy = implCluster.HostSelectionPolicy
type retiredHostStateNotifier = implCluster.HostStateNotifier
type retiredHostTierer = implCluster.HostTierer
type retiredKeyspaceUpdateEvent = implCluster.KeyspaceUpdateEvent

var retiredMAX_IN_FLIGHT_THRESHOLD = implCluster.MAX_IN_FLIGHT_THRESHOLD

type retiredMetadata = implCluster.Metadata

func retiredNewClientRoutesAddressTranslator(a0 retiredClientRoutesConfig, a1 DNSResolver, a2 bool, a3 StdLogger) *retiredClientRoutesHandler {
	return implCluster.NewClientRoutesAddressTranslator(a0, a1, a2, a3)
}
func retiredNewTraceWriter(a0 *Session, a1 io.Writer) *retiredTraceWriter {
	return implCluster.NewTraceWriter(a0, a1)
}
func retiredNewTracer(a0 *Session) *retiredTracerEnhanced { return implCluster.NewTracer(a0) }

type retiredNextHost = implCluster.NextHost

func retiredNonLocalReplicasFallback() func(policy *tokenAwareHostPolicy) {
	return implCluster.NonLocalReplicasFallback()
}

type retiredObservedConnect = implCluster.ObservedConnect

func OpenSingleHost(a0 context.Context, a1 Config, a2 UUID) (*Session, error) {
	return implExecution.OpenSingleHost(a0, a1, a2)
}

type Options = implExecution.Options

const OverflowEvent = implCluster.OverflowEvent

type Page[R any] = implExecution.Page[R]
type PageLease = implExecution.PageLease
type retiredPoolConfig = implCluster.PoolConfig

func Prepare[P any, R any](a0 context.Context, a1 *Session, a2 Statement[P, R], a3 PrepareOptions) (*Prepared[P, R], error) {
	return implExecution.Prepare[P, R](a0, a1, a2, a3)
}

type PrepareOptions = implExecution.PrepareOptions
type Prepared[P any, R any] = implExecution.Prepared[P, R]
type PreparedInfo = implExecution.PreparedInfo

func retiredRackAwareRoundRobinPolicy(a0 string, a1 string, a2 ...dcAwarePolicyOption) retiredHostSelectionPolicy {
	return implCluster.RackAwareRoundRobinPolicy(a0, a1, a2...)
}

type retiredReadyPolicy = implCluster.ReadyPolicy

func retiredRoundRobinHostPolicy() retiredHostSelectionPolicy {
	return implCluster.RoundRobinHostPolicy()
}

type RowStream[R any] = implExecution.RowStream[R]
type Rows[R any] = implExecution.Rows[R]

func Run[R any](a0 context.Context, a1 *Session, a2 *Bound[R], a3 Options) (*Rows[R], error) {
	return implExecution.Run[R](a0, a1, a2, a3)
}
func RunBatch[R any](a0 context.Context, a1 *Session, a2 BatchKind, a3 []BatchItem, a4 RowDecoder[R], a5 Options) (*Page[R], error) {
	return implExecution.RunBatch[R](a0, a1, a2, a3, a4, a5)
}
func RunPage[R any](a0 context.Context, a1 *Session, a2 *Bound[R], a3 PageState, a4 Options) (*Page[R], error) {
	return implExecution.RunPage[R](a0, a1, a2, a3, a4)
}
func RunStream[B any, R any](a0 context.Context, a1 *Session, a2 *Bound[B], a3 StreamRowDecoder[R], a4 StreamOptions) (*RowStream[R], error) {
	return implExecution.RunStream[B, R](a0, a1, a2, a3, a4)
}
func RunUnpaged[R any](a0 context.Context, a1 *Session, a2 *Bound[R], a3 Options) (*Page[R], error) {
	return implExecution.RunUnpaged[R](a0, a1, a2, a3)
}

const SchemaEvent = implCluster.SchemaEvent

type Session = implCluster.Session

func retiredShuffleReplicas() func(*tokenAwareHostPolicy) { return implCluster.ShuffleReplicas() }
func retiredSingleHostReadyPolicy(a0 retiredHostSelectionPolicy) *singleHostReadyPolicy {
	return implCluster.SingleHostReadyPolicy(a0)
}

const StatusEvent = implCluster.StatusEvent

type StreamInfo = implExecution.StreamInfo
type StreamOptions = implExecution.StreamOptions
type Subscription = implCluster.Subscription

func retiredTokenAwareHostPolicy(a0 retiredHostSelectionPolicy, a1 ...func(*tokenAwareHostPolicy)) retiredHostSelectionPolicy {
	return implCluster.TokenAwareHostPolicy(a0, a1...)
}

const TopologyEvent = implCluster.TopologyEvent

type TraceEntry = implCluster.TraceEntry
type TraceEvent = implCluster.TraceEvent
type TraceInfo = implCluster.TraceInfo
type retiredTraceWriter = implCluster.TraceWriter
type retiredTracerEnhanced = implCluster.TracerEnhanced

func retiredWhiteListHostFilter(a0 ...string) retiredHostFilter {
	return implCluster.WhiteListHostFilter(a0...)
}
func retiredWithClientRoutes(a0 ...retiredClientRoutesOption) func(*clusterSettings) {
	return implCluster.WithClientRoutes(a0...)
}
func retiredWithEndpoints(a0 ...retiredClientRoutesEndpoint) func(*retiredClientRoutesConfig) {
	return implCluster.WithEndpoints(a0...)
}
func retiredWithMaxResolverConcurrency(a0 int) func(*retiredClientRoutesConfig) {
	return implCluster.WithMaxResolverConcurrency(a0)
}
func retiredWithResolveHealthyEndpointPeriod(a0 time.Duration) func(*retiredClientRoutesConfig) {
	return implCluster.WithResolveHealthyEndpointPeriod(a0)
}
func retiredWithShardAwareness(a0 bool) func(*retiredClientRoutesConfig) {
	return implCluster.WithShardAwareness(a0)
}
func retiredWithTable(a0 string) func(*retiredClientRoutesConfig) { return implCluster.WithTable(a0) }

type activeExecutionDrain = implExecution.InternalActiveExecutionDrain
type adaptiveOrderingReport = implCluster.InternalAdaptiveOrderingReport

func admitExecution[R any](a0 context.Context, a1 *Session, a2 *Bound[R], a3 Options) (context.Context, context.CancelFunc, *executionRequest, error) {
	return implExecution.InternalAdmitExecution[R](a0, a1, a2, a3)
}

type agreementPeer = implCluster.InternalAgreementPeer

var asVectorType = implCluster.InternalAsVectorType

func attemptBytesWritten(a0 error) bool { return implExecution.InternalAttemptBytesWritten(a0) }

type batchState = implExecution.InternalBatchState

func bindPrepared[P any, R any](a0 *Prepared[P, R], a1 P, a2 bool) (*Bound[R], error) {
	return implExecution.InternalBindPrepared[P, R](a0, a1, a2)
}
func bindUnprepared[P any, R any](a0 Statement[P, R], a1 Columns, a2 P, a3 bool) (*Bound[R], error) {
	return implExecution.InternalBindUnprepared[P, R](a0, a1, a2, a3)
}
func buildConnectionNodePreferenceReport(a0 *clusterSettings) any {
	return implCluster.InternalBuildConnectionNodePreferenceReport(a0)
}
func buildConnectionReport(a0 *clusterSettings) connectionReport {
	return implCluster.InternalBuildConnectionReport(a0)
}
func buildControlPlaneReport(a0 *clusterSettings, a1 bool) controlPlaneReport {
	return implCluster.InternalBuildControlPlaneReport(a0, a1)
}
func buildLoadBalancingPolicyReport(a0 retiredHostSelectionPolicy) any {
	return implCluster.InternalBuildLoadBalancingPolicyReport(a0)
}
func buildLoadBalancingReport(a0 retiredHostSelectionPolicy) queryLoadBalancingReport {
	return implCluster.InternalBuildLoadBalancingReport(a0)
}
func buildNodeLocationPreferenceReport(a0 retiredHostSelectionPolicy) any {
	return implCluster.InternalBuildNodeLocationPreferenceReport(a0)
}
func buildQueryDefaultsReport(a0 *clusterSettings) queryDefaultsReport {
	return implCluster.InternalBuildQueryDefaultsReport(a0)
}
func buildQueryReport(a0 *Session) queryReport { return implCluster.InternalBuildQueryReport(a0) }
func buildQueryRetryReport(a0 *clusterSettings) queryRetryReport {
	return implCluster.InternalBuildQueryRetryReport(a0)
}
func buildReconnectionPolicyReport(a0 retiredReconnectionPolicy) any {
	return implCluster.InternalBuildReconnectionPolicyReport(a0)
}
func buildRequestsReport(a0 *clusterSettings) requestsReport {
	return implCluster.InternalBuildRequestsReport(a0)
}
func buildRetryPolicyReport(a0 RetryPolicy) any {
	return implCluster.InternalBuildRetryPolicyReport(a0)
}
func buildSocketReport() socketReport               { return implCluster.InternalBuildSocketReport() }
func buildTLSReport(a0 *clusterSettings) *tlsReport { return implCluster.InternalBuildTLSReport(a0) }
func buildTypedLoadBalancingReport(a0 RoutingPolicy) queryLoadBalancingReport {
	return implCluster.InternalBuildTypedLoadBalancingReport(a0)
}
func buildV2VnodeRanges(a0 *tokenRing, a1 *KeyspaceMetadata) ([]TokenRange, error) {
	return implCluster.InternalBuildV2VnodeRanges(a0, a1)
}
func checkSystemSchema(a0 controlConnection) (bool, error) {
	return implCluster.InternalCheckSystemSchema(a0)
}
func clearDestination[R any](a0 *R) { implExecution.InternalClearDestination[R](a0) }

type clientRoute = implCluster.InternalClientRoute
type clientRouteCache = implCluster.InternalClientRouteCache
type clientRouteCacheEntry = implCluster.InternalClientRouteCacheEntry
type clusterMeta = implCluster.InternalClusterMeta
type clusterSettings = implCluster.InternalClusterSettings

func connConfig(a0 *clusterSettings) (*driverConnConfig, error) {
	return implCluster.InternalConnConfig(a0)
}

type connHost = implCluster.InternalConnHost

func connectCluster(a0 context.Context, a1 Config, a2 func(*Session, Config) *Config) (*Session, error) {
	return implCluster.InternalConnectCluster(a0, a1, a2)
}

type connectReport = implCluster.InternalConnectReport

func connectSettings(a0 clusterSettings) (*Session, error) {
	return implCluster.InternalConnectSettings(a0)
}
func connectSettingsAsync(a0 clusterSettings) (*Session, error) {
	return implCluster.InternalConnectSettingsAsync(a0)
}
func connectionOwnedBy(a0 *driverConn, a1 *Session) bool {
	return implCluster.InternalConnectionOwnedBy(a0, a1)
}

type connectionPoolReport = implCluster.InternalConnectionPoolReport
type connectionReport = implCluster.InternalConnectionReport

func consistencyName(a0 Consistency) string { return implCluster.InternalConsistencyName(a0) }

type controlConn = implCluster.InternalControlConn

const controlConnClosing = implCluster.InternalControlConnClosing
const controlConnStarted = implCluster.InternalControlConnStarted
const controlConnStarting = implCluster.InternalControlConnStarting

type controlConnection = implCluster.InternalControlConnection
type controlPlaneQueriesReport = implCluster.InternalControlPlaneQueriesReport
type controlPlaneReport = implCluster.InternalControlPlaneReport
type controlPlaneSchemaReport = implCluster.InternalControlPlaneSchemaReport

func copyBytes(a0 []byte) []byte { return implCluster.InternalCopyBytes(a0) }

type cowHostList = implCluster.InternalCowHostList

func cqlIdentifiers(a0 string) []string          { return implExecution.InternalCqlIdentifiers(a0) }
func createControlConn(a0 *Session) *controlConn { return implCluster.InternalCreateControlConn(a0) }
func currentExecution(a0 *Session) executionLifecycle {
	return implCluster.InternalCurrentExecution(a0)
}
func customPolicyName(a0 any) string { return implCluster.InternalCustomPolicyName(a0) }

type dataCenterHostFilter = implCluster.InternalDataCenterHostFilter
type dcAwarePolicyOption = implCluster.InternalDcAwarePolicyOption
type dcAwareRR = implCluster.InternalDcAwareRR
type dcFailoverDisabledPolicy = implCluster.InternalDcFailoverDisabledPolicy

const defaultCQLPort = implCluster.InternalDefaultCQLPort
const defaultMaxPreparedStmts = implCluster.InternalDefaultMaxPreparedStmts
const defaultMaxStreams = implCluster.InternalDefaultMaxStreams

type driverConfigReport = implCluster.InternalDriverConfigReport
type driverConfigReporter = implCluster.InternalDriverConfigReporter

const driverConfigStartupKey = implCluster.InternalDriverConfigStartupKey
const driverConfigVersion = implCluster.InternalDriverConfigVersion

func effectiveExpBackoff(a0 time.Duration, a1 time.Duration) time.Duration {
	return implCluster.InternalEffectiveExpBackoff(a0, a1)
}
func effectiveInFlightMax(a0 int) int { return implCluster.InternalEffectiveInFlightMax(a0) }
func equalPreparedAggregates(a0 map[string]*AggregateMetadata, a1 map[string]*AggregateMetadata) bool {
	return implCluster.InternalEqualPreparedAggregates(a0, a1)
}
func equalPreparedFunction(a0 FunctionMetadata, a1 FunctionMetadata) bool {
	return implCluster.InternalEqualPreparedFunction(a0, a1)
}
func equalPreparedFunctions(a0 map[string]*FunctionMetadata, a1 map[string]*FunctionMetadata) bool {
	return implCluster.InternalEqualPreparedFunctions(a0, a1)
}

const eventBufferSize = implCluster.InternalEventBufferSize
const eventDebounceTime = implCluster.InternalEventDebounceTime

type eventDebouncer = implCluster.InternalEventDebouncer

func eventV2Address(a0 net.IP, a1 int) netip.AddrPort {
	return implCluster.InternalEventV2Address(a0, a1)
}

type execRowsDecoder = implExecution.InternalExecRowsDecoder
type execRowsPlan = implExecution.InternalExecRowsPlan

func executionContext(a0 context.Context, a1 time.Duration) (context.Context, context.CancelFunc) {
	return implExecution.InternalExecutionContext(a0, a1)
}
func executionErrorResult(a0 error) Result { return implExecution.InternalExecutionErrorResult(a0) }

type executionLifecycle = implCluster.InternalExecutionLifecycle
type executionLifetime = implExecution.InternalExecutionLifetime
type executionRequest = implExecution.InternalExecutionRequest
type executionWaiter = implCluster.InternalExecutionWaiter

func exponentialDelayWindow(a0 time.Duration, a1 time.Duration) (int64, int64) {
	return implCluster.InternalExponentialDelayWindow(a0, a1)
}
func fallbackToNonPreferredNodesAllowed(a0 *tokenAwareHostPolicy) bool {
	return implCluster.InternalFallbackToNonPreferredNodesAllowed(a0)
}
func filterAllowedConnectionIDs(a0 []string, a1 []string) []string {
	return implCluster.InternalFilterAllowedConnectionIDs(a0, a1)
}
func getAggregatesMetadata(a0 *Session, a1 string) ([]AggregateMetadata, error) {
	return implCluster.InternalGetAggregatesMetadata(a0, a1)
}

var getApacheCassandraType = implCluster.InternalGetApacheCassandraType
var getCassandraLongType = implCluster.InternalGetCassandraLongType

func getColumnMetadata(a0 *Session, a1 string) ([]ColumnMetadata, error) {
	return implCluster.InternalGetColumnMetadata(a0, a1)
}
func getColumnMetadataByTable(a0 *Session, a1 string, a2 string) ([]ColumnMetadata, error) {
	return implCluster.InternalGetColumnMetadataByTable(a0, a1, a2)
}
func getCreateStatements(a0 *Session, a1 string) ([]byte, error) {
	return implCluster.InternalGetCreateStatements(a0, a1)
}
func getFunctionsMetadata(a0 *Session, a1 string) ([]FunctionMetadata, error) {
	return implCluster.InternalGetFunctionsMetadata(a0, a1)
}
func getHostPortMappingForConnectionIDs(a0 controlConnection, a1 string, a2 []string, a3 bool) ([]clientRoute, error) {
	return implCluster.InternalGetHostPortMappingForConnectionIDs(a0, a1, a2, a3)
}
func getHostPortMappingForPairs(a0 controlConnection, a1 string, a2 []pair, a3 bool) ([]clientRoute, error) {
	return implCluster.InternalGetHostPortMappingForPairs(a0, a1, a2, a3)
}
func getIndexMetadata(a0 *Session, a1 string) ([]IndexMetadata, error) {
	return implCluster.InternalGetIndexMetadata(a0, a1)
}
func getIndexMetadataByTable(a0 *Session, a1 string, a2 string) ([]IndexMetadata, error) {
	return implCluster.InternalGetIndexMetadataByTable(a0, a1, a2)
}
func getKeyspaceMetadata(a0 *Session, a1 string) (*KeyspaceMetadata, error) {
	return implCluster.InternalGetKeyspaceMetadata(a0, a1)
}
func getPairsFromEvent(a0 []string, a1 []string) []pair {
	return implCluster.InternalGetPairsFromEvent(a0, a1)
}
func getPeersFromQuerySystemPeers(a0 []map[string]any, a1 int, a2 StdLogger) ([]*retiredHostInfo, error) {
	return implCluster.InternalGetPeersFromQuerySystemPeers(a0, a1, a2)
}
func getReplicationFactorFromOpts(a0 any) (int, error) {
	return implCluster.InternalGetReplicationFactorFromOpts(a0)
}
func getStrategy(a0 *KeyspaceMetadata, a1 StdLogger) placementStrategy {
	return implCluster.InternalGetStrategy(a0, a1)
}
func getTableMetadata(a0 *Session, a1 string) ([]TableMetadata, error) {
	return implCluster.InternalGetTableMetadata(a0, a1)
}
func getTableMetadataByName(a0 *Session, a1 string, a2 string) ([]TableMetadata, error) {
	return implCluster.InternalGetTableMetadataByName(a0, a1, a2)
}
func getTypeMetadata(a0 *Session, a1 string) ([]TypeMetadata, error) {
	return implCluster.InternalGetTypeMetadata(a0, a1)
}
func getV2RoutesConnections(a0 context.Context, a1 controlConnection, a2 string, a3 []string, a4 bool) ([]clientRoute, error) {
	return implCluster.InternalGetV2RoutesConnections(a0, a1, a2, a3, a4)
}
func getV2RoutesPairs(a0 context.Context, a1 controlConnection, a2 string, a3 []pair, a4 bool) ([]clientRoute, error) {
	return implCluster.InternalGetV2RoutesPairs(a0, a1, a2, a3, a4)
}
func getViewMetadata(a0 *Session, a1 string) ([]ViewMetadata, error) {
	return implCluster.InternalGetViewMetadata(a0, a1)
}
func getViewMetadataByTable(a0 *Session, a1 string, a2 string) ([]ViewMetadata, error) {
	return implCluster.InternalGetViewMetadataByTable(a0, a1, a2)
}
func hostBusy(a0 *Session, a1 *retiredHostInfo) bool { return implCluster.InternalHostBusy(a0, a1) }
func hostInfoFromMap(a0 map[string]any, a1 int) (*retiredHostInfo, error) {
	return implCluster.InternalHostInfoFromMap(a0, a1)
}

type hostInfoList = implCluster.InternalHostInfoList

const hostInfoListMapThreshold = implCluster.InternalHostInfoListMapThreshold

type hostPrepareFlight = implExecution.InternalHostPrepareFlight
type hostPrepared = implExecution.InternalHostPrepared
type hostSet = implCluster.InternalHostSet
type hostTokens = implCluster.InternalHostTokens
type inFlightReport = implCluster.InternalInFlightReport
type int64SelectedHost = implCluster.InternalInt64SelectedHost

func isNilPolicy(a0 any) bool                     { return implCluster.InternalIsNilPolicy(a0) }
func isValidPeer(a0 *retiredHostInfo) bool        { return implCluster.InternalIsValidPeer(a0) }
func isZeroToken(a0 *retiredHostInfo) bool        { return implCluster.InternalIsZeroToken(a0) }
func learnPortFromHosts(a0 []string) (int, error) { return implCluster.InternalLearnPortFromHosts(a0) }

type loadBalancingCustomReport = implCluster.InternalLoadBalancingCustomReport
type loadBalancingTokenAwareReport = implCluster.InternalLoadBalancingTokenAwareReport
type logicalExecution = implExecution.InternalLogicalExecution

const maxDriverConfigSize = implCluster.InternalMaxDriverConfigSize
const maxPolicyUnwrapDepth = implCluster.InternalMaxPolicyUnwrapDepth
const maxPreparedRefreshScopes = implCluster.InternalMaxPreparedRefreshScopes
const maxReportableInFlight = implCluster.InternalMaxReportableInFlight

type metadataDescriber = implCluster.InternalMetadataDescriber

var mutRandr = &implCluster.InternalMutRandr

type networkTopology = implCluster.InternalNetworkTopology

func newClientRouteCache() clientRouteCache { return implCluster.InternalNewClientRouteCache() }
func newClusterSettings(a0 ...string) *clusterSettings {
	return implCluster.InternalNewClusterSettings(a0...)
}
func newConnectionHooks(a0 *Session) implTransport.InternalConnectionHooks {
	return implCluster.InternalNewConnectionHooks(a0)
}
func newDriverConfigReporter(a0 *Session) *driverConfigReporter {
	return implCluster.InternalNewDriverConfigReporter(a0)
}
func newEventDebouncer(a0 string, a1 func([]frame), a2 StdLogger) *eventDebouncer {
	return implCluster.InternalNewEventDebouncer(a0, a1, a2)
}
func newHostConnPool(a0 *Session, a1 *retiredHostInfo, a2 int, a3 string) *hostConnPool {
	return implCluster.InternalNewHostConnPool(a0, a1, a2, a3)
}
func newHostInfoList(a0 []*retiredHostInfo) *hostInfoList {
	return implCluster.InternalNewHostInfoList(a0)
}
func newMetadataDescriber(a0 *Session) *metadataDescriber {
	return implCluster.InternalNewMetadataDescriber(a0)
}
func newPage[R any](a0 *responsePage, a1 RowDecoder[R], a2 func()) (*Page[R], error) {
	return implExecution.InternalNewPage[R](a0, a1, a2)
}
func newPageWithPlans[R any](a0 *responsePage, a1 RowDecoder[R], a2 *resultPlanCache[R], a3 func()) (*Page[R], error) {
	return implExecution.InternalNewPageWithPlans[R](a0, a1, a2, a3)
}
func newPolicyConnPool(a0 *Session) *policyConnPool   { return implCluster.InternalNewPolicyConnPool(a0) }
func newPoolServices(a0 *Session) poolServices        { return implCluster.InternalNewPoolServices(a0) }
func newSchemaSource(a0 *Session) implMetadata.Source { return implCluster.InternalNewSchemaSource(a0) }
func newSessionCommon(a0 clusterSettings) (*Session, error) {
	return implCluster.InternalNewSessionCommon(a0)
}
func newSessionID(a0 StdLogger) string { return implCluster.InternalNewSessionID(a0) }
func newSingleHost(a0 *retiredHostInfo, a1 byte, a2 time.Duration) *singleHost {
	return implCluster.InternalNewSingleHost(a0, a1, a2)
}

type nodeLocationDCReport = implCluster.InternalNodeLocationDCReport
type nodeLocationRackReport = implCluster.InternalNodeLocationRackReport

func nonNegativeMillis(a0 time.Duration) int64 { return implCluster.InternalNonNegativeMillis(a0) }

type pageFuture = implExecution.InternalPageFuture
type pageReport = implCluster.InternalPageReport
type pair = implCluster.InternalPair

func parseProtocolFromError(a0 error) int { return implCluster.InternalParseProtocolFromError(a0) }
func partitionHealthy(a0 []*retiredHostInfo, a1 *Session) {
	implCluster.InternalPartitionHealthy(a0, a1)
}

type placementStrategy = implCluster.InternalPlacementStrategy

func positiveMillis(a0 time.Duration) *int64 { return implCluster.InternalPositiveMillis(a0) }

type prefetchJob = implExecution.InternalPrefetchJob
type prepareCacheKey = implExecution.InternalPrepareCacheKey
type prepareFlight = implExecution.InternalPrepareFlight
type preparedState = implExecution.InternalPreparedState

var protocolSupportRe = implCluster.InternalProtocolSupportRe

func pseudoRandomUUID() UUID { return implCluster.InternalPseudoRandomUUID() }
func qualifiedForKeyspace(a0 string, a1 string) bool {
	return implExecution.InternalQualifiedForKeyspace(a0, a1)
}

type queryDefaultsReport = implCluster.InternalQueryDefaultsReport
type queryLoadBalancingReport = implCluster.InternalQueryLoadBalancingReport
type queryReport = implCluster.InternalQueryReport
type queryRetryReport = implCluster.InternalQueryRetryReport
type rackAwareRR = implCluster.InternalRackAwareRR

var randr = implCluster.InternalRandr

func readV2AgreementHost(a0 context.Context, a1 UUID, a2 []*driverConn) (UUID, error) {
	return implCluster.InternalReadV2AgreementHost(a0, a1, a2)
}
func readV2ClientRoutes(a0 context.Context, a1 controlConnection, a2 string, a3 string, a4 [][]string, a5 bool) ([]clientRoute, error) {
	return implCluster.InternalReadV2ClientRoutes(a0, a1, a2, a3, a4, a5)
}
func readV2SystemRows[P any, R any](a0 context.Context, a1 *driverConn, a2 Statement[P, R], a3 Columns, a4 P) ([]R, error) {
	return implCluster.InternalReadV2SystemRows[P, R](a0, a1, a2, a3, a4)
}
func readV2SystemRowsVia[P any, R any](a0 context.Context, a1 byte, a2 time.Duration, a3 func(context.Context, *attemptRequest) (*responsePage, error), a4 Statement[P, R], a5 Columns, a6 P) ([]R, error) {
	return implCluster.InternalReadV2SystemRowsVia[P, R](a0, a1, a2, a3, a4, a5, a6)
}
func readV2Topology(a0 context.Context, a1 driverConnInterface, a2 string, a3 int) ([]*retiredHostInfo, error) {
	return implCluster.InternalReadV2Topology(a0, a1, a2, a3)
}
func readV2TraceRows[R any](a0 *Session, a1 []byte, a2 string, a3 RowDecoder[R]) ([]R, error) {
	return implCluster.InternalReadV2TraceRows[R](a0, a1, a2, a3)
}

type readWriteTimeoutReport = implCluster.InternalReadWriteTimeoutReport
type reconnectionConstantReport = implCluster.InternalReconnectionConstantReport
type reconnectionCustomReport = implCluster.InternalReconnectionCustomReport
type reconnectionExponentialReport = implCluster.InternalReconnectionExponentialReport
type reconnectionGroupReport = implCluster.InternalReconnectionGroupReport
type requestDefaultReport = implCluster.InternalRequestDefaultReport

func requestHasCounter(a0 executionRequest) bool { return implExecution.InternalRequestHasCounter(a0) }
func requestQualifiedForKeyspace(a0 *executionRequest, a1 string) bool {
	return implExecution.InternalRequestQualifiedForKeyspace(a0, a1)
}

type requestsReport = implCluster.InternalRequestsReport

func resolveOptions(a0 *Session, a1 Options) (resolvedOptions, error) {
	return implExecution.InternalResolveOptions(a0, a1)
}

type resolvedOptions = implExecution.InternalResolvedOptions
type resultMetadataObservation = implExecution.InternalResultMetadataObservation
type resultPlanCache[R any] = implExecution.InternalResultPlanCache[R]
type retryBackoffExponentialReport = implCluster.InternalRetryBackoffExponentialReport
type retryPolicyCustomReport = implCluster.InternalRetryPolicyCustomReport
type retryPolicyDowngradingReport = implCluster.InternalRetryPolicyDowngradingReport
type retryPolicySimpleReport = implCluster.InternalRetryPolicySimpleReport
type ringDescriber = implCluster.InternalRingDescriber

func roundRobbin(a0 int, a1 ...[]*retiredHostInfo) retiredNextHost {
	return implCluster.InternalRoundRobbin(a0, a1...)
}

type roundRobinHostPolicy = implCluster.InternalRoundRobinHostPolicy

func runSinglePage[R any](a0 context.Context, a1 *Session, a2 *Bound[R], a3 PageState, a4 Options, a5 bool) (*Page[R], error) {
	return implExecution.InternalRunSinglePage[R](a0, a1, a2, a3, a4, a5)
}
func runtimeFor(a0 *Session) *sessionRuntime { return implExecution.InternalRuntimeFor(a0) }

type schemaAgreementReport = implCluster.InternalSchemaAgreementReport
type schemaSource = implCluster.InternalSchemaSource

func scyllaGetTablePartitioner(a0 *Session, a1 string, a2 string) (retiredPartitioner, error) {
	return implCluster.InternalScyllaGetTablePartitioner(a0, a1, a2)
}
func scyllaIsCdcTable(a0 *Session, a1 string, a2 string) (bool, error) {
	return implCluster.InternalScyllaIsCdcTable(a0, a1, a2)
}
func selectPreparedMetadata(a0 *preparedState, a1 UUID, a2 *hostPrepared, a3 []byte, a4 bool, a5 *resultMetadataObservation, a6 ...uint64) error {
	return implExecution.InternalSelectPreparedMetadata(a0, a1, a2, a3, a4, a5, a6...)
}
func selectPreparedResponse(a0 *preparedState, a1 UUID, a2 *hostPrepared, a3 []byte, a4 bool, a5 *responsePage, a6 ...uint64) error {
	return implExecution.InternalSelectPreparedResponse(a0, a1, a2, a3, a4, a5, a6...)
}

type selectedHost = implCluster.InternalSelectedHost
type sessionConnectionHooks = implCluster.InternalSessionConnectionHooks
type sessionRuntime = implExecution.InternalSessionRuntime

func setupTLSConfig(a0 *retiredSslOptions, a1 StdLogger) (*tls.Config, error) {
	return implCluster.InternalSetupTLSConfig(a0, a1)
}
func shardAwareEnabled(a0 *clusterSettings) bool { return implCluster.InternalShardAwareEnabled(a0) }

type shardAwareReport = implCluster.InternalShardAwareReport

func shuffleHosts(a0 []*retiredHostInfo) []*retiredHostInfo {
	return implCluster.InternalShuffleHosts(a0)
}
func shuffleHostsInPlace(a0 []*retiredHostInfo) { implCluster.InternalShuffleHostsInPlace(a0) }

type simpleStrategy = implCluster.InternalSimpleStrategy
type singleHost = implCluster.InternalSingleHost
type singleHostReadyPolicy = implCluster.InternalSingleHostReadyPolicy

func snapshotV2Host(a0 *Session, a1 *retiredHostInfo) (HostSnapshot, error) {
	return implCluster.InternalSnapshotV2Host(a0, a1)
}

type socketReport = implCluster.InternalSocketReport

var splitCompositeTypes = implCluster.InternalSplitCompositeTypes
var splitJavaCompositeTypes = implCluster.InternalSplitJavaCompositeTypes

func strictVerifyPeerCertificate(a0 *x509.CertPool) func([][]byte, [][]*x509.Certificate) error {
	return implCluster.InternalStrictVerifyPeerCertificate(a0)
}

type systemQueriesReport = implCluster.InternalSystemQueriesReport
type systemQueriesTimeoutReport = implCluster.InternalSystemQueriesTimeoutReport

func tableNotFoundError(a0 string, a1 string) error {
	return implCluster.InternalTableNotFoundError(a0, a1)
}
func terminalV2AgreementError(a0 error) bool { return implCluster.InternalTerminalV2AgreementError(a0) }
func timeForTopology(a0 driverConnInterface) time.Duration {
	return implCluster.InternalTimeForTopology(a0)
}

type tlsReport = implCluster.InternalTlsReport
type tokenAwareHostPolicy = implCluster.InternalTokenAwareHostPolicy
type tokenRingReplicas = implCluster.InternalTokenRingReplicas
type traceActivityRow = implExecution.InternalTraceActivityRow
type traceSessionRow = implExecution.InternalTraceSessionRow
type typedAttemptOutcome = implExecution.InternalTypedAttemptOutcome

var unwrapCompositeTypeDefinition = implCluster.InternalUnwrapCompositeTypeDefinition

func unwrapHostSelectionPolicy(a0 retiredHostSelectionPolicy) retiredHostSelectionPolicy {
	return implCluster.InternalUnwrapHostSelectionPolicy(a0)
}

type updateTask = implCluster.InternalUpdateTask

func v2DecodeRoutes() RowDecoder[v2RouteRow] { return implCluster.InternalV2DecodeRoutes() }

type v2LatencySummary = implCluster.InternalV2LatencySummary
type v2PendingTabletFeedback = implCluster.InternalV2PendingTabletFeedback
type v2PeripheralState = implCluster.InternalV2PeripheralState
type v2RouteEpochs = implCluster.InternalV2RouteEpochs
type v2RouteID = implCluster.InternalV2RouteID
type v2RouteParams = implCluster.InternalV2RouteParams
type v2RouteRow = implCluster.InternalV2RouteRow
type v2SafeLogger = logging.InternalV2SafeLogger
type v2SystemQueryer = implCluster.InternalV2SystemQueryer
type v2TabletFeedbackKey = implCluster.InternalV2TabletFeedbackKey

func validV2RouteTable(a0 string) bool { return implCluster.InternalValidV2RouteTable(a0) }
