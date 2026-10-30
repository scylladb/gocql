// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package pool

import (
	codec "github.com/scylladb/gocql/v2/internal/codec"
	cqltype "github.com/scylladb/gocql/v2/internal/cqltype"
)

type Option[T any] = cqltype.Option[T]

func ParseUUID(a0 string) (UUID, error) { return cqltype.ParseUUID(a0) }

type StreamCapabilityError = codec.StreamCapabilityError

type UUID = cqltype.UUID

func typeValidation(a0 string, a1 string, a2 ...any) error {
	return cqltype.TypeValidation(a0, a1, a2...)
}
