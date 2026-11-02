// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package execution

import (
	"context"

	implCluster "github.com/scylladb/gocql/v2/internal/cluster"
	"github.com/scylladb/gocql/v2/internal/wire"
	"sync"
)

type PageStorage[R any] struct {
	cursor            queryCursor
	pendingRelease    func()
	mu                sync.Mutex
	destination       *R
	InternalCtx       context.Context
	cancel            context.CancelFunc
	session           *implCluster.Session
	InternalResponse  *responsePage
	InternalPlan      RowPlan[R]
	InternalCursor    *wire.Cursor
	reader            RowReader
	InternalIndex     int32
	current           R
	valid             bool
	err               error
	closed            bool
	InternalExhausted bool
	InternalRelease   func()
}

func PageData[R any](v *Page[R]) *PageStorage[R] { return &v.privateStorage }
