// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package paging

import (
	runtimeerrors "github.com/scylladb/gocql/v2/internal/runtimeerrors"
)

var ErrPagingNoProgress = runtimeerrors.ErrPagingNoProgress
var ErrPagingStateLimit = runtimeerrors.ErrPagingStateLimit
