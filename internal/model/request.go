// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package model

import (
	"time"

	cqltype "github.com/scylladb/gocql/v2/internal/cqltype"
	"github.com/scylladb/gocql/v2/internal/wire"
)

type PayloadEntry struct {
	Key   string
	Value cqltype.Nullable[cqltype.Bytes]
}

type EncodedCell struct {
	Typ   cqltype.DataType
	State cqltype.CellState
	Body  wire.Chain
}
type EncodedValues struct {
	Columns cqltype.Columns
	Cells   []EncodedCell
	Framed  wire.Chain
}

// attemptRequest contains only immutable protocol inputs. The transport retains
// lease until every accepted writer has stopped accessing its spans.
type AttemptRequest struct {
	Framing RequestFraming

	TabletVersionBlock             byte
	Statement                      string
	PreparedID, ResultMetadataID   []byte
	Values                         wire.Chain
	ValueCount                     int
	Lease                          *wire.Lease
	Consistency, SerialConsistency cqltype.Consistency
	PageSize                       int
	PagingState                    []byte
	Timestamp                      int64
	DefaultTimestamp, SkipMetadata bool
	Keyspace                       string
	NowInSeconds                   *int32
	Trace                          bool
	Timeout                        time.Duration
	Prepare                        bool
	Batch                          *TypedBatchRequest
	Payload                        []PayloadEntry
}
type TypedBatchRequest struct {
	Kind  cqltype.BatchKind
	Items []TypedBatchItem
}
type TypedBatchItem struct {
	Statement  string
	PreparedID []byte
	Values     wire.Chain
	ValueCount int
}

// RequestFraming is reserved for ordinary attempt framing. Its contents are
// reusable only after the accepted submission writer calls Release.
type RequestFraming interface {
	TryAcquire() bool
	Release()
	Trim(int)
	RetainedBytes() int
}
