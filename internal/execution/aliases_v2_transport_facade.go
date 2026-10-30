// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package execution

import (
	model "github.com/scylladb/gocql/v2/internal/model"
	schemafence "github.com/scylladb/gocql/v2/internal/schemafence"
	implTransport "github.com/scylladb/gocql/v2/internal/transport"
)

type Conn = implTransport.Conn

type StreamLimits = implTransport.StreamLimits

func checkedStreamLimits(a0 StreamLimits) (StreamLimits, error) {
	return implTransport.InternalCheckedStreamLimits(a0)
}

func decodeStreamRow[R any](a0 *streamResponse, a1 StreamRowPlan[R], a2 *R) (bool, error) {
	return implTransport.InternalDecodeStreamRow[R](a0, a1, a2)
}

type exclusiveConn = implTransport.InternalExclusiveConn

type inputOwner = InternalInputOwner

func metadataIDTracked(a0 bool, a1 []byte) bool {
	return implTransport.InternalMetadataIDTracked(a0, a1)
}
func newInputOwner() *inputOwner { return InternalNewInputOwner() }

type preparedDependencies = schemafence.InternalPreparedDependencies
type preparedDependencyKey = schemafence.InternalPreparedDependencyKey

type preparedSchemaStamp = schemafence.InternalPreparedSchemaStamp
type preparedSchemaTracker = schemafence.InternalPreparedSchemaTracker

type responsePage = implTransport.InternalResponsePage

type streamResponse = implTransport.InternalStreamResponse

type typedResultMetadata = model.ResultSchemaIdentity
