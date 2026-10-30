// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package gocql

import implCluster "github.com/scylladb/gocql/v2/internal/cluster"

// These private-runtime test names preserve existing test bodies. They are absent
// from the production package and its public API.
var AcceptAllFilter = retiredAcceptAllFilter
var AvoidSlowReplicas = retiredAvoidSlowReplicas

type ClientRoutesConfig = retiredClientRoutesConfig
type ClientRoutesEndpoint = retiredClientRoutesEndpoint
type ClientRoutesEndpointList = retiredClientRoutesEndpointList
type ClientRoutesHandler = retiredClientRoutesHandler
type ClientRoutesOption = retiredClientRoutesOption
type ClusterOption = retiredClusterOption
type ConnectObserver = retiredConnectObserver

var DCAwareRoundRobinPolicy = retiredDCAwareRoundRobinPolicy
var DataCenterHostFilter = retiredDataCenterHostFilter
var DataCentreHostFilter = retiredDataCentreHostFilter
var DenyAllFilter = retiredDenyAllFilter
var DontShuffleReplicas = retiredDontShuffleReplicas

type HostFilter = retiredHostFilter
type HostFilterFunc = retiredHostFilterFunc

var HostPolicyOptionDisableDCFailover = retiredHostPolicyOptionDisableDCFailover

type HostSelectionPolicy = retiredHostSelectionPolicy
type HostStateNotifier = retiredHostStateNotifier
type HostTierer = retiredHostTierer

var MAX_IN_FLIGHT_THRESHOLD = retiredMAX_IN_FLIGHT_THRESHOLD

type Metadata = retiredMetadata

var NewClientRoutesAddressTranslator = retiredNewClientRoutesAddressTranslator
var NewTraceWriter = retiredNewTraceWriter
var NewTracer = retiredNewTracer

type NextHost = retiredNextHost

var NonLocalReplicasFallback = retiredNonLocalReplicasFallback

type ObservedConnect = retiredObservedConnect
type PoolConfig = retiredPoolConfig

var RackAwareRoundRobinPolicy = retiredRackAwareRoundRobinPolicy

type ReadyPolicy = retiredReadyPolicy

var RoundRobinHostPolicy = retiredRoundRobinHostPolicy
var ShuffleReplicas = retiredShuffleReplicas
var SingleHostReadyPolicy = retiredSingleHostReadyPolicy
var TokenAwareHostPolicy = retiredTokenAwareHostPolicy

type TraceWriter = retiredTraceWriter
type TracerEnhanced = retiredTracerEnhanced

var WhiteListHostFilter = retiredWhiteListHostFilter
var WithClientRoutes = retiredWithClientRoutes
var WithEndpoints = retiredWithEndpoints
var WithMaxResolverConcurrency = retiredWithMaxResolverConcurrency
var WithResolveHealthyEndpointPeriod = retiredWithResolveHealthyEndpointPeriod
var WithShardAwareness = retiredWithShardAwareness
var WithTable = retiredWithTable

type ApplicationInfo = retiredApplicationInfo
type HostDialer = retiredHostDialer
type HostInfo = retiredHostInfo
type HostInfoBuilder = retiredHostInfoBuilder

var NewStaticApplicationInfo = retiredNewStaticApplicationInfo

type ScyllaConnectionFeatures = retiredScyllaConnectionFeatures

var ScyllaGetSourcePort = retiredScyllaGetSourcePort

type ScyllaHostFeatures = retiredScyllaHostFeatures
type ScyllaShardAwareDialer = retiredScyllaShardAwareDialer
type ShardDialer = retiredShardDialer
type StaticApplicationInfo = retiredStaticApplicationInfo
type Authenticator = retiredAuthenticator
type FrameHeaderObserver = retiredFrameHeaderObserver
type ObservedFrameHeader = retiredObservedFrameHeader
type ObservedStream = retiredObservedStream
type PasswordAuthenticator = retiredPasswordAuthenticator
type SslOptions = retiredSslOptions
type StreamObserver = retiredStreamObserver
type StreamObserverContext = retiredStreamObserverContext
type Tracer = retiredTracer
type ConstantReconnectionPolicy = retiredConstantReconnectionPolicy
type ExponentialReconnectionPolicy = retiredExponentialReconnectionPolicy
type HostInformation = retiredHostInformation
type HostPoolInfo = retiredHostPoolInfo
type NoReconnectionPolicy = retiredNoReconnectionPolicy
type Partitioner = retiredPartitioner
type ReconnectionPolicy = retiredReconnectionPolicy
type SelectedHost = retiredSelectedHost
type SetHosts = retiredSetHosts
type SetPartitioner = retiredSetPartitioner
type SetTablets = retiredSetTablets
type Token = retiredToken

var WrapTLS = retiredWrapTLS

type Compressor = retiredCompressor
type SegmentCompressor = retiredSegmentCompressor

var NewCustomType = retiredNewCustomType
var NewNativeType = retiredNewNativeType
var NewTupleType = retiredNewTupleType
var NewUDTType = retiredNewUDTType
var NewCollectionType = retiredNewCollectionType

var sessionTestSubscribeToEvents = implCluster.InternalSessionSubscribeToEvents
var sessionTestTableMetadata = implCluster.InternalSessionTableMetadata
var sessionTestTableTabletsMetadata = implCluster.InternalSessionTableTabletsMetadata
var sessionTestForEachTablet = implCluster.InternalSessionForEachTablet
var sessionTestGetHosts = implCluster.InternalSessionGetHosts
var sessionTestGetHostPoolByID = implCluster.InternalSessionGetHostPoolByID

type SerialConsistency = Consistency
type SnappyCompressor = retiredSnappyCompressor
type KeyspaceUpdateEvent = retiredKeyspaceUpdateEvent
type DialedHost = retiredDialedHost

const NodeDown = retiredNodeDown
const NodeUp = retiredNodeUp
