// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package routing

import (
	cqltype "github.com/scylladb/gocql/v2/internal/cqltype"
	model "github.com/scylladb/gocql/v2/internal/model"
)

type Bytes = cqltype.Bytes

type Consistency = cqltype.Consistency

const LocalOne = cqltype.LocalOne

const LocalSerial = cqltype.LocalSerial

const Null = cqltype.Null

const One = cqltype.One

type Option[T any] = cqltype.Option[T]

const OrdinaryStatement = cqltype.OrdinaryStatement

const Present = cqltype.Present

type ProtocolError = cqltype.ProtocolError

type RoutingInfo = model.RoutingInfo

const Serial = cqltype.Serial

type UUID = cqltype.UUID

const Unset = cqltype.Unset

type ValidationError = cqltype.ValidationError

type encodedCell = model.EncodedCell

func typeValidation(a0 string, a1 string, a2 ...any) error {
	return cqltype.TypeValidation(a0, a1, a2...)
}
