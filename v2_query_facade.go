// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package gocql

import "github.com/scylladb/gocql/v2/internal/execution"
import "github.com/scylladb/gocql/v2/internal/codec"

type Query[P, R any] = execution.Query[P, R]

var ErrBusy = execution.ErrBusy

var OwnedBlobCodec = codec.OwnedBlobCodec

func RowInto[R any](reset func(*R), fields ...OutputField[R]) RowDecoder[R] {
	return codec.RowInto(reset, fields...)
}

type StreamQuery[P, R any] = execution.StreamQuery[P, R]

func NewStreamQuery[P, B, R any](prepared *Prepared[P, B], decoder StreamRowDecoder[R], options StreamOptions) (*StreamQuery[P, R], error) {
	return execution.NewStreamQuery(prepared, decoder, options)
}

func NewUnpreparedStreamQuery[P, B, R any](session *Session, statement Statement[P, B], columns Columns, decoder StreamRowDecoder[R], options StreamOptions) (*StreamQuery[P, R], error) {
	return execution.NewUnpreparedStreamQuery(session, statement, columns, decoder, options)
}

func NewQuery[P, R any](prepared *Prepared[P, R], options Options) (*Query[P, R], error) {
	return execution.NewQuery(prepared, options)
}

func NewUnpreparedQuery[P, R any](session *Session, statement Statement[P, R], columns Columns, options Options) (*Query[P, R], error) {
	return execution.NewUnpreparedQuery(session, statement, columns, options)
}
