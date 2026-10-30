// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package transport

import (
	model "github.com/scylladb/gocql/v2/internal/model"
	protocol "github.com/scylladb/gocql/v2/internal/protocol"
)

var ErrFrameTooBig = protocol.ErrFrameTooBig

type Result = model.Result

type parsedMetadata = protocol.ParsedMetadata
type responseData = model.ResponseData
type typedEnvelope = protocol.TypedEnvelope
type typedParser = protocol.TypedParser
