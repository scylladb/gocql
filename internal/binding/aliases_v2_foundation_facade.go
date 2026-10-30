// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package binding

import codec "github.com/scylladb/gocql/v2/internal/codec"
import cqltype "github.com/scylladb/gocql/v2/internal/cqltype"
import model "github.com/scylladb/gocql/v2/internal/model"
import "github.com/scylladb/gocql/v2/internal/wire"

type Columns = cqltype.Columns

type DataType = cqltype.DataType

type ParamPlan[P any] = codec.ParamPlan[P]
type ParamWriter = codec.ParamWriter

type ValueReader = codec.ValueReader

type encodedCell = model.EncodedCell
type encodedValues = model.EncodedValues

func finishParamWriter(a0 *ParamWriter) error { return codec.FinishParamWriter(a0) }

func newParamWriter(a0 *wire.Builder, a1 Columns, a2 bool) *ParamWriter {
	return codec.NewParamWriter(a0, a1, a2)
}

func readValue(a0 *wire.Cursor, a1 DataType, a2 *wire.Lease, a3 bool) (ValueReader, error) {
	return codec.ReadValue(a0, a1, a2, a3)
}
func typeValidation(a0 string, a1 string, a2 ...any) error {
	return cqltype.TypeValidation(a0, a1, a2...)
}

const ConditionalStatement = cqltype.ConditionalStatement

type ParamEncoder[P any] = codec.ParamEncoder[P]

type RowDecoder[R any] = codec.RowDecoder[R]

type StatementKind = cqltype.StatementKind
