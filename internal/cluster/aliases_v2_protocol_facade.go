// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cluster

import (
	protocol "github.com/scylladb/gocql/v2/internal/protocol"
)

const ErrCodeBootstrapping = protocol.ErrCodeBootstrapping

const ErrCodeOverloaded = protocol.ErrCodeOverloaded

const ErrCodeReadTimeout = protocol.ErrCodeReadTimeout

const ErrCodeSyntax = protocol.ErrCodeSyntax

const ErrCodeUnprepared = protocol.ErrCodeUnprepared

const ErrCodeWriteTimeout = protocol.ErrCodeWriteTimeout

type RequestError = protocol.RequestError
