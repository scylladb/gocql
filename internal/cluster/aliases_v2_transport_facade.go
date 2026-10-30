// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cluster

import (
	schemafence "github.com/scylladb/gocql/v2/internal/schemafence"
	implTransport "github.com/scylladb/gocql/v2/internal/transport"
	"github.com/scylladb/gocql/v2/tablets"
)

type Authenticator = implTransport.Authenticator
type Conn = implTransport.Conn
type ConnConfig = implTransport.ConnConfig
type ConnErrorHandler = implTransport.ConnErrorHandler
type ConnInterface = implTransport.ConnInterface

type FrameHeaderObserver = implTransport.FrameHeaderObserver

type SslOptions = implTransport.SslOptions

type StreamObserver = implTransport.StreamObserver

type connErrorHandlerFn = implTransport.InternalConnErrorHandlerFn

type connectionLifetimeKey = implTransport.InternalConnectionLifetimeKey
type connectionPoolPolicy = implTransport.InternalConnectionPoolPolicy

type framerConfig = implTransport.InternalFramerConfig

func parseTypedTabletPayload(a0 Bytes, a1 string, a2 string, a3 bool) (tablets.TabletInfo, error) {
	return implTransport.InternalParseTypedTabletPayload(a0, a1, a2, a3)
}

type preparedDependencyKey = schemafence.InternalPreparedDependencyKey

type preparedSchemaTracker = schemafence.InternalPreparedSchemaTracker
type protocolError = implTransport.InternalProtocolError

const qrySystemLocal = implTransport.InternalQrySystemLocal
const qrySystemPeers = implTransport.InternalQrySystemPeers
const qrySystemPeersCassandra = implTransport.InternalQrySystemPeersCassandra
const qrySystemPeersV2 = implTransport.InternalQrySystemPeersV2

type responsePage = implTransport.InternalResponsePage

type startupIdentity = implTransport.InternalStartupIdentity

func translateHostAddresses(a0 AddressTranslator, a1 *HostInfo, a2 StdLogger) (translatedAddresses, error) {
	return implTransport.InternalTranslateHostAddresses(a0, a1, a2)
}
