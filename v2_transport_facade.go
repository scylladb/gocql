// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package gocql

import (
	"context"
	implEndpoint "github.com/scylladb/gocql/v2/internal/endpoint"
	implExecution "github.com/scylladb/gocql/v2/internal/execution"
	model "github.com/scylladb/gocql/v2/internal/model"
	schemafence "github.com/scylladb/gocql/v2/internal/schemafence"
	implTransport "github.com/scylladb/gocql/v2/internal/transport"
	"github.com/scylladb/gocql/v2/tablets"
)

type retiredAuthenticator = implTransport.Authenticator
type driverConn = implTransport.Conn
type driverConnConfig = implTransport.ConnConfig
type driverConnErrorHandler = implTransport.ConnErrorHandler
type driverConnInterface = implTransport.ConnInterface
type ErrSchemaMismatch = implTransport.ErrSchemaMismatch
type retiredFrameHeaderObserver = implTransport.FrameHeaderObserver
type retiredObservedFrameHeader = implTransport.ObservedFrameHeader
type retiredObservedStream = implTransport.ObservedStream
type retiredPasswordAuthenticator = implTransport.PasswordAuthenticator
type retiredSslOptions = implTransport.SslOptions
type StreamLimits = implTransport.StreamLimits
type retiredStreamObserver = implTransport.StreamObserver
type retiredStreamObserverContext = implTransport.StreamObserverContext
type retiredTracer = implTransport.Tracer

func approve(a0 string, a1 []string) bool { return implTransport.InternalApprove(a0, a1) }
func bodyReadDesyncedConn(a0 error) bool  { return implTransport.InternalBodyReadDesyncedConn(a0) }

type callReq = implTransport.InternalCallReq

var callReqPool = &implTransport.InternalCallReqPool

type callResp = implTransport.InternalCallResp

func checkedStreamLimits(a0 StreamLimits) (StreamLimits, error) {
	return implTransport.InternalCheckedStreamLimits(a0)
}
func checkedV2PoolBudget(a0 PoolOptions, a1 int) (int, error) {
	return implTransport.InternalCheckedV2PoolBudget(a0, a1)
}

type connErrorHandlerFn = implTransport.InternalConnErrorHandlerFn
type connEvent = implTransport.InternalConnEvent
type connFramers = implTransport.InternalConnFramers
type connectionHooks = implTransport.InternalConnectionHooks
type connectionLifetimeKey = implTransport.InternalConnectionLifetimeKey
type connectionPoolPolicy = implTransport.InternalConnectionPoolPolicy

func decodeStreamRow[R any](a0 *streamResponse, a1 StreamRowPlan[R], a2 *R) (bool, error) {
	return implTransport.InternalDecodeStreamRow[R](a0, a1, a2)
}

var errOrphanBudget = implTransport.InternalErrOrphanBudget

type exclusiveConn = implTransport.InternalExclusiveConn

func fpShrinkSize(a0 int64, a1 int64) int64 { return implTransport.InternalFpShrinkSize(a0, a1) }

const framerBufEWMAWeight = implTransport.InternalFramerBufEWMAWeight
const framerBufShrinkThreshold = implTransport.InternalFramerBufShrinkThreshold

type framerConfig = implTransport.InternalFramerConfig
type framerPool = implTransport.InternalFramerPool

func getCallReq(a0 int) *callReq { return implTransport.InternalGetCallReq(a0) }
func getSchemaAgreement(a0 string, a1 []schemaAgreementHost, a2 StdLogger) error {
	return implTransport.InternalGetSchemaAgreement(a0, a1, a2)
}

type inputOwner = implExecution.InternalInputOwner

const maxCASRetries = implTransport.InternalMaxCASRetries
const maxReasonableBufferSize = implTransport.InternalMaxReasonableBufferSize

func metadataIDTracked(a0 bool, a1 []byte) bool {
	return implTransport.InternalMetadataIDTracked(a0, a1)
}
func newInputOwner() *inputOwner { return implExecution.InternalNewInputOwner() }
func newV2Authenticator(a0 context.Context, a1 HostSnapshot, a2 AuthProvider) *v2Authenticator {
	return implTransport.InternalNewV2Authenticator(a0, a1, a2)
}
func nullableAuthBytes(a0 []byte) Nullable[Bytes] { return implTransport.InternalNullableAuthBytes(a0) }
func parseTypedTabletHint(a0 Bytes, a1 string, a2 string) (tablets.TabletInfo, error) {
	return implTransport.InternalParseTypedTabletHint(a0, a1, a2)
}
func parseTypedTabletPayload(a0 Bytes, a1 string, a2 string, a3 bool) (tablets.TabletInfo, error) {
	return implTransport.InternalParseTypedTabletPayload(a0, a1, a2, a3)
}

type poolOwner = implTransport.InternalPoolOwner
type preparedDependencies = schemafence.InternalPreparedDependencies
type preparedDependencyKey = schemafence.InternalPreparedDependencyKey
type preparedEpochRead = schemafence.InternalPreparedEpochRead
type preparedSchemaStamp = schemafence.InternalPreparedSchemaStamp
type preparedSchemaTracker = schemafence.InternalPreparedSchemaTracker
type protocolError = implTransport.InternalProtocolError

func putCallReq(a0 *callReq) { implTransport.InternalPutCallReq(a0) }

const qrySystemLocal = implTransport.InternalQrySystemLocal
const qrySystemPeers = implTransport.InternalQrySystemPeers
const qrySystemPeersCassandra = implTransport.InternalQrySystemPeersCassandra
const qrySystemPeersV2 = implTransport.InternalQrySystemPeersV2

type responsePage = implTransport.InternalResponsePage
type schemaAgreementHost = implTransport.InternalSchemaAgreementHost

const sessionIDStartupKey = implTransport.InternalSessionIDStartupKey

type startupCoordinator = implTransport.InternalStartupCoordinator
type startupIdentity = implTransport.InternalStartupIdentity

func startupOptions(a0 string, a1 string, a2 string, a3 retiredApplicationInfo, a4 startupReporter, a5 string, a6 bool) map[string]string {
	return implTransport.InternalStartupOptions(a0, a1, a2, a3, a4, a5, a6)
}

type startupReporter = implTransport.InternalStartupReporter
type streamResponse = implTransport.InternalStreamResponse
type systemRequestState = implTransport.InternalSystemRequestState
type tabletsRoutingV2Ext = implEndpoint.InternalTabletsRoutingV2Ext

func translateAddressPort(a0 AddressTranslator, a1 *retiredHostInfo, a2 AddressPort, a3 StdLogger) (AddressPort, error) {
	return implEndpoint.InternalTranslateAddressPort(a0, a1, a2, a3)
}
func translateHostAddresses(a0 AddressTranslator, a1 *retiredHostInfo, a2 StdLogger) (translatedAddresses, error) {
	return implTransport.InternalTranslateHostAddresses(a0, a1, a2)
}

type typedResultMetadata = model.ResultSchemaIdentity

func typedTabletExtensions(a0 []cqlProtocolExtension, a1 map[string][]string) []cqlProtocolExtension {
	return implEndpoint.InternalTypedTabletExtensions(a0, a1)
}
func unmarshalTabletHint(a0 []byte, a1 uint8, a2 string, a3 string) (tablets.TabletInfo, error) {
	return implTransport.InternalUnmarshalTabletHint(a0, a1, a2, a3)
}
func useKeyspaceStmt(a0 string) string { return implTransport.InternalUseKeyspaceStmt(a0) }

type v2Authenticator = implTransport.InternalV2Authenticator

func validateV2ConnectionSharding(a0 retiredScyllaConnectionFeatures) error {
	return implTransport.InternalValidateV2ConnectionSharding(a0)
}
func validateV2Supported(a0 map[string][]string, a1 PoolOptions) error {
	return implTransport.InternalValidateV2Supported(a0, a1)
}

type verifiedStreamUnits = implTransport.InternalVerifiedStreamUnits

func waitCallReqDone(a0 *callReq, a1 string) { implTransport.InternalWaitCallReqDone(a0, a1) }
