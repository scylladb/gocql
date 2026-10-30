// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package codec

import (
	cqltype "github.com/scylladb/gocql/v2/internal/cqltype"
)

// Encoder writes the body of one checked CQL value.
type Encoder[T any] interface {
	CheckType(cqltype.DataType) error
	Encode(*ValueWriter, T) error
}

// Decoder reads one checked CQL value and recursively detaches borrowed values.
type Decoder[T any] interface {
	CheckType(cqltype.DataType) error
	Decode(ValueReader) (T, error)
	Clone(T) T
}
type Codec[T any] interface {
	Encoder[T]
	Decoder[T]
}
