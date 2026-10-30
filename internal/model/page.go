// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package model

import (
	. "github.com/scylladb/gocql/v2/internal/cqltype"

	"github.com/scylladb/gocql/v2/internal/wire"
)

type ResponseData struct {
	MetadataChanged                     bool
	Owner                               *wire.Lease
	Rows                                wire.Chain
	Columns                             Columns
	RowCount                            int32
	ColumnCount                         int
	Lwt                                 bool
	Keyspace                            string
	SchemaChange                        bool
	PagingState, MetadataID, PreparedID []byte
	NoMetadata                          bool
	BindColumns                         Columns
	PkIndexes                           []int
	Kind                                int
	TraceID                             []byte
	Payload                             []PayloadEntry
	Warnings                            []string
	State                               PageState
	Result                              Result
}
