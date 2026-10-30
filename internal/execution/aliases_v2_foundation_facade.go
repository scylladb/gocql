// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package execution

import cqltype "github.com/scylladb/gocql/v2/internal/cqltype"
import codec "github.com/scylladb/gocql/v2/internal/codec"
import model "github.com/scylladb/gocql/v2/internal/model"

func typeValidation(a0 string, a1 string, a2 ...any) error {
	return cqltype.TypeValidation(a0, a1, a2...)
}

type BatchKind = cqltype.BatchKind

type Codec[T any] = codec.Codec[T]

func Column[R any, V any](a0 string, a1 Decoder[V], a2 func(*R) *V) OutputField[R] {
	return codec.Column[R, V](a0, a1, a2)
}

type ColumnSpec = cqltype.ColumnSpec
type Columns = cqltype.Columns

const ConditionalStatement = cqltype.ConditionalStatement

type Consistency = cqltype.Consistency

type DataType = cqltype.DataType

type Decoder[T any] = codec.Decoder[T]

type Encoder[T any] = codec.Encoder[T]

var ErrReleased = cqltype.ErrReleased

var InetCodec = codec.InetCodec

var Int32 = codec.Int32

const LocalSerial = cqltype.LocalSerial

func NewColumns(a0 ...ColumnSpec) Columns { return cqltype.NewColumns(a0...) }

type Nullable[T any] = cqltype.Nullable[T]

func NullableCodec[T any](a0 Codec[T]) Codec[Nullable[T]] { return codec.NullableCodec[T](a0) }

const One = cqltype.One

func OneParam[T any](a0 Encoder[T]) ParamEncoder[T] { return codec.OneParam[T](a0) }

type Option[T any] = cqltype.Option[T]

const OrdinaryStatement = cqltype.OrdinaryStatement

type OutputField[R any] = codec.OutputField[R]

type ParamEncoder[P any] = codec.ParamEncoder[P]
type ParamPlan[P any] = codec.ParamPlan[P]

type PayloadEntry = model.PayloadEntry

type ProtocolError = cqltype.ProtocolError

func Row[R any](a0 ...OutputField[R]) RowDecoder[R] { return codec.Row[R](a0...) }

type RowDecoder[R any] = codec.RowDecoder[R]
type RowPlan[R any] = codec.RowPlan[R]
type RowReader = codec.RowReader

func ScalarType(a0 Type) (DataType, error) { return cqltype.ScalarType(a0) }

const Serial = cqltype.Serial

type StatementKind = cqltype.StatementKind

type StreamRowDecoder[R any] = codec.StreamRowDecoder[R]
type StreamRowPlan[R any] = codec.StreamRowPlan[R]

type Text = cqltype.Text

var TextCodec = codec.TextCodec

type TimeUUID = codec.TimeUUID

var TimeUUIDCodec = codec.TimeUUIDCodec

type Timestamp = codec.Timestamp

var TimestampCodec = codec.TimestampCodec

type Type = cqltype.Type

const TypeCounter = cqltype.TypeCounter

const TypeUUID = cqltype.TypeUUID

type UUID = cqltype.UUID

var UUIDCodec = codec.UUIDCodec

type Unit = codec.Unit

const UnknownStatement = cqltype.UnknownStatement
const Unset = cqltype.Unset

type attemptRequest = model.AttemptRequest

func compileParamPlan[P any](a0 ParamEncoder[P], a1 Columns) (ParamPlan[P], error) {
	return codec.CompileParamPlan[P](a0, a1)
}
func compileRowPlan[R any](a0 RowDecoder[R], a1 Columns) (RowPlan[R], error) {
	return codec.CompileRowPlan[R](a0, a1)
}

type encodedCell = model.EncodedCell
type encodedValues = model.EncodedValues

func finishRowReader(a0 *RowReader) error { return codec.FinishRowReader(a0) }

type typedBatchItem = model.TypedBatchItem
type typedBatchRequest = model.TypedBatchRequest

const (
	LoggedBatch   = cqltype.LoggedBatch
	UnloggedBatch = cqltype.UnloggedBatch
	CounterBatch  = cqltype.CounterBatch
)
