// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package routing

import (
	protocol "github.com/scylladb/gocql/v2/internal/protocol"
)

const ErrCodeBootstrapping = protocol.ErrCodeBootstrapping

const ErrCodeOverloaded = protocol.ErrCodeOverloaded

const ErrCodeServer = protocol.ErrCodeServer

const ErrCodeUnavailable = protocol.ErrCodeUnavailable
const ErrCodeUnprepared = protocol.ErrCodeUnprepared

type RequestErrRateLimitReached = protocol.RequestErrRateLimitReached

type RequestErrReadTimeout = protocol.RequestErrReadTimeout
type RequestErrUnavailable = protocol.RequestErrUnavailable

type RequestError = protocol.RequestError
