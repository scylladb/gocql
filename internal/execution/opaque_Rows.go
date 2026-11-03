// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package execution

import (
	"context"

	implCluster "github.com/scylladb/gocql/v2/internal/cluster"

	"sync"
)

type RowsStorage[R any] struct {
	prefetchJobs    chan InternalPrefetchJob
	pendingRelease  func()
	mu              sync.Mutex
	plans           *InternalResultPlanCache[R]
	results         *queryResultStorage[R]
	InternalHistory pagingHistory
	nextErr         error
	rejectColumns   bool
	terminalErr     error
	future          *InternalPageFuture

	InternalCtx     context.Context
	cancel          context.CancelFunc
	session         *implCluster.Session
	InternalRequest *InternalExecutionRequest
	decoder         RowDecoder[R]
	InternalPage    *Page[R]
	info            Result
	err             error
	closed          bool
	valid           bool
	InternalRelease func()
}

func RowsData[R any](v *Rows[R]) *RowsStorage[R] {
	if v.privateStorage.RowsStorage == nil {
		v.privateStorage.RowsStorage = new(RowsStorage[R])
	}
	return v.privateStorage.RowsStorage
}
