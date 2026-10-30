// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package transport

import codec "github.com/scylladb/gocql/v2/internal/codec"
import cqltype "github.com/scylladb/gocql/v2/internal/cqltype"
import model "github.com/scylladb/gocql/v2/internal/model"
import "github.com/scylladb/gocql/v2/internal/wire"

func BorrowBytes(a0 []byte) Bytes { return cqltype.BorrowBytes(a0) }

type Bytes = cqltype.Bytes

type Columns = cqltype.Columns

var ErrReleased = cqltype.ErrReleased

type Nullable[T any] = cqltype.Nullable[T]

type Option[T any] = cqltype.Option[T]

type ProtocolError = cqltype.ProtocolError

type StreamCapabilityError = codec.StreamCapabilityError

type StreamRowPlan[R any] = codec.StreamRowPlan[R]
type StreamRowReader = codec.StreamRowReader

type UUID = cqltype.UUID

type attemptRequest = model.AttemptRequest

func newStreamRowReader(a0 Columns, a1 *wire.Stream) (*StreamRowReader, error) {
	return codec.NewStreamRowReader(a0, a1)
}

func typeValidation(a0 string, a1 string, a2 ...any) error {
	return cqltype.TypeValidation(a0, a1, a2...)
}

type InternalResponseData = model.ResponseData
