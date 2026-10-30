// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package metadata

import (
	protocol "github.com/scylladb/gocql/v2/internal/protocol"
)

const ErrCodeInvalid = protocol.ErrCodeInvalid

const ErrCodeSyntax = protocol.ErrCodeSyntax

type RequestError = protocol.RequestError
