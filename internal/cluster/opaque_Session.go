/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/*
 * Content before git sha 34fdeebefcbf183ed7f916f931aa0586fdaa1b40
 * Copyright (c) 2012, The Gocql authors,
 * provided under the BSD-3-Clause License.
 * See the NOTICE file distributed with this work for additional information.
 */

package cluster

import (
	"context"

	"sync"
	"sync/atomic"

	"github.com/scylladb/gocql/v2/debounce"
	"github.com/scylladb/gocql/v2/events"

	"github.com/scylladb/gocql/v2/internal/eventbus"
)

type SessionStorage struct {
	InternalTypedMu              sync.Mutex
	nextTypedHost                atomic.Uint64
	InternalTypedState           InternalExecutionLifecycle
	schemaMu                     sync.Mutex
	schema                       *preparedSchemaTracker
	v2Peripheral                 atomic.Pointer[InternalV2PeripheralState]
	InternalControl              InternalControlConnection
	InternalCtx                  context.Context
	InternalLogger               StdLogger
	InternalPolicy               HostSelectionPolicy
	connectObserver              ConnectObserver
	frameObserver                FrameHeaderObserver
	streamObserver               StreamObserver
	initErr                      error
	InternalNodeEvents           *InternalEventDebouncer
	InternalHostSource           *InternalRingDescriber
	InternalPool                 *policyConnPool
	InternalRingRefresher        *debounce.RefreshDebouncer
	readyCh                      chan struct{}
	InternalCancel               context.CancelFunc
	InternalSchemaEvents         *InternalEventDebouncer
	InternalMetadataDescriber    *InternalMetadataDescriber
	InternalEventBus             *eventbus.EventBus[events.Event]
	InternalConnCfg              *ConnConfig
	InternalClientRoutesHandler  *ClientRoutesHandler
	InternalDriverConfigReporter *InternalDriverConfigReporter
	// id is a globally unique identifier for this session, reported to
	// the cluster via the SESSION_ID STARTUP option so that all connections
	// belonging to the same session can be correlated in system.clients.
	id                                string
	InternalAddressTranslator         AddressTranslator
	InternalCfg                       InternalClusterSettings
	mu                                sync.RWMutex
	InternalSessionStateMu            sync.RWMutex
	InternalIsClosing                 bool
	closeDone                         chan struct{}
	InternalHasAggregatesAndFunctions bool
	InternalUseSystemSchema           bool
	InternalTabletsRoutingV1          bool
	InternalIsInitialized             bool
	InternalIsClosed                  bool
}

func SessionData(v *Session) *SessionStorage { return &v.privateStorage }
