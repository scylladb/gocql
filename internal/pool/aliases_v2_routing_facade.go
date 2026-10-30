// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package pool

import (
	model "github.com/scylladb/gocql/v2/internal/model"
	runtimeerrors "github.com/scylladb/gocql/v2/internal/runtimeerrors"
)

var ErrClosed = runtimeerrors.ErrClosed

var ErrNoConnections = runtimeerrors.ErrNoConnections

var ErrNoStreams = runtimeerrors.ErrNoStreams

type RoutingInfo = model.RoutingInfo

type Target = model.Target
