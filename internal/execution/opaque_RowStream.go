// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package execution

import (
	"context"

	implCluster "github.com/scylladb/gocql/v2/internal/cluster"
	"github.com/scylladb/gocql/v2/internal/codec"
	"github.com/scylladb/gocql/v2/internal/wire"
	"sync"
	"sync/atomic"
)

type RowStreamStorage[R any] struct {
	cursor              queryCursor
	pageBuffered        bool
	bufferedPage        *responsePage
	bufferedBody        *wire.Stream
	bufferedReader      *codec.StreamRowReader
	bufferedIndex       int32
	bufferedPlans       *streamPlanCache[R]
	attemptMetadata     *streamResponse
	closeDone           chan struct{}
	retryTarget         *Target
	InternalActive      sync.Mutex
	mu                  sync.Mutex
	advancing           atomic.Bool
	InternalClosing     atomic.Bool
	InternalCtx         context.Context
	InternalCancel      context.CancelFunc
	stop                func() bool
	InternalSession     *implCluster.Session
	InternalRequest     *InternalExecutionRequest
	limits              StreamLimits
	InternalDecoder     StreamRowDecoder[R]
	plan                StreamRowPlan[R]
	columns             Columns
	response            *streamResponse
	InternalReservation *exclusiveConn
	prepared            *InternalHostPrepared
	pageCancel          context.CancelFunc

	InternalHistory pagingHistory
	info            StreamInfo
	err             error
	terminal        bool
	cleaned         bool
}

func RowStreamData[R any](v *RowStream[R]) *RowStreamStorage[R] { return &v.privateStorage }
