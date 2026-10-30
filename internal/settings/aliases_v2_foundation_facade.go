// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package settings

import (
	cqltype "github.com/scylladb/gocql/v2/internal/cqltype"
)

const Any = cqltype.Any

type Bytes = cqltype.Bytes

type Columns = cqltype.Columns
type Compression = cqltype.Compression

type Consistency = cqltype.Consistency

const LocalOne = cqltype.LocalOne
const LocalQuorum = cqltype.LocalQuorum
const LocalSerial = cqltype.LocalSerial

type Nullable[T any] = cqltype.Nullable[T]

type Option[T any] = cqltype.Option[T]

const Serial = cqltype.Serial

const SnappyCompression = cqltype.SnappyCompression

type UUID = cqltype.UUID

func typeValidation(a0 string, a1 string, a2 ...any) error {
	return cqltype.TypeValidation(a0, a1, a2...)
}
