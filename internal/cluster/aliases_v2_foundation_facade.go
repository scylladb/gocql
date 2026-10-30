// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cluster

import (
	codec "github.com/scylladb/gocql/v2/internal/codec"
	cqltype "github.com/scylladb/gocql/v2/internal/cqltype"
	model "github.com/scylladb/gocql/v2/internal/model"
)

const All = cqltype.All
const Any = cqltype.Any

type Bytes = cqltype.Bytes

type Codec[T any] = codec.Codec[T]

func Column[R any, V any](a0 string, a1 Decoder[V], a2 func(*R) *V) OutputField[R] {
	return codec.Column[R, V](a0, a1, a2)
}

type ColumnSpec = cqltype.ColumnSpec
type Columns = cqltype.Columns

type Consistency = cqltype.Consistency

type DataType = cqltype.DataType

type Decoder[T any] = codec.Decoder[T]

const EachQuorum = cqltype.EachQuorum

type Encoder[T any] = codec.Encoder[T]

var InetCodec = codec.InetCodec

var Int32 = codec.Int32

const LZ4Compression = cqltype.LZ4Compression

func ListEncoder[T any](a0 Encoder[T]) Encoder[[]T] { return codec.ListEncoder[T](a0) }
func ListType(a0 DataType) (DataType, error)        { return cqltype.ListType(a0) }

const LocalOne = cqltype.LocalOne
const LocalQuorum = cqltype.LocalQuorum
const LocalSerial = cqltype.LocalSerial

func MustRandomUUID() UUID { return cqltype.MustRandomUUID() }

func NewColumns(a0 ...ColumnSpec) Columns { return cqltype.NewColumns(a0...) }

var NoParams = codec.NoParams

type Nullable[T any] = cqltype.Nullable[T]

func NullableCodec[T any](a0 Codec[T]) Codec[Nullable[T]] { return codec.NullableCodec[T](a0) }

const One = cqltype.One

func OneColumn[T any](a0 Decoder[T]) RowDecoder[T]  { return codec.OneColumn[T](a0) }
func OneParam[T any](a0 Encoder[T]) ParamEncoder[T] { return codec.OneParam[T](a0) }

type Option[T any] = cqltype.Option[T]

type OutputField[R any] = codec.OutputField[R]

type ParamEncoder[P any] = codec.ParamEncoder[P]
type ParamPlan[P any] = codec.ParamPlan[P]
type ParamWriter = codec.ParamWriter

func ParseUUID(a0 string) (UUID, error) { return cqltype.ParseUUID(a0) }

type ProtocolError = cqltype.ProtocolError

const Quorum = cqltype.Quorum

func RandomUUID() (UUID, error) { return cqltype.RandomUUID() }

func Row[R any](a0 ...OutputField[R]) RowDecoder[R] { return codec.Row[R](a0...) }

type RowDecoder[R any] = codec.RowDecoder[R]

func ScalarType(a0 Type) (DataType, error) { return cqltype.ScalarType(a0) }

const Serial = cqltype.Serial

const SnappyCompression = cqltype.SnappyCompression

var StringEncoder = codec.StringEncoder

type Text = cqltype.Text

var TextCodec = codec.TextCodec

const Three = cqltype.Three

type TimeUUID = codec.TimeUUID

var TimeUUIDCodec = codec.TimeUUIDCodec

const Two = cqltype.Two

type Type = cqltype.Type

const TypeUUID = cqltype.TypeUUID
const TypeVarchar = cqltype.TypeVarchar

type UUID = cqltype.UUID

var UUIDCodec = codec.UUIDCodec

type Unit = codec.Unit

type ValidationError = cqltype.ValidationError

type ValueReader = codec.ValueReader

func WriteParam[T any](a0 *ParamWriter, a1 Encoder[T], a2 T) error {
	return codec.WriteParam[T](a0, a1, a2)
}

type attemptRequest = model.AttemptRequest

func typeValidation(a0 string, a1 string, a2 ...any) error {
	return cqltype.TypeValidation(a0, a1, a2...)
}
