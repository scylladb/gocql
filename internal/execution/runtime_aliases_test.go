// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package execution_test

import (
	. "github.com/scylladb/gocql/v2"
	"github.com/scylladb/gocql/v2/internal/binding"
	"github.com/scylladb/gocql/v2/internal/codec"
	"github.com/scylladb/gocql/v2/internal/execution"
	"github.com/scylladb/gocql/v2/internal/model"
	"github.com/scylladb/gocql/v2/internal/transport"
)

var newInputOwner = execution.InternalNewInputOwner
var conditionalCQL = binding.InternalConditionalCQL
var newRowReader = codec.NewRowReader

type responsePage = transport.InternalResponsePage
type responseData = model.ResponseData
type pageFuture = execution.InternalPageFuture

func newPage[R any](response *responsePage, decoder RowDecoder[R], release func()) (*Page[R], error) {
	return execution.InternalNewPage(response, decoder, release)
}
