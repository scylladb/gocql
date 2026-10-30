// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package transport

import runtimeerrors "github.com/scylladb/gocql/v2/internal/runtimeerrors"
import model "github.com/scylladb/gocql/v2/internal/model"

var ErrConnectionClosed = runtimeerrors.ErrConnectionClosed

var ErrClosed = runtimeerrors.ErrClosed

var ErrNoStreams = runtimeerrors.ErrNoStreams

var ErrTimeoutNoResponse = runtimeerrors.ErrTimeoutNoResponse

type HostSnapshot = model.HostSnapshot
