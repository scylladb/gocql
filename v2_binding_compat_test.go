// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package gocql

// These bindings expose internal execution seams to the historical protocol and
// cancellation fixtures only. They are absent from the supported public API.
import (
	"context"
	implExecution "github.com/scylladb/gocql/v2/internal/execution"
)

type Bound[R any] = implExecution.Bound[R]

func BatchEntry[R any](a0 *Bound[R]) (BatchItem, error) { return implExecution.BatchEntry[R](a0) }

func BindBorrowed[P any, R any](a0 *Prepared[P, R], a1 P) (*Bound[R], error) {
	return implExecution.BindBorrowed[P, R](a0, a1)
}

func BindOwned[P any, R any](a0 *Prepared[P, R], a1 P) (*Bound[R], error) {
	return implExecution.BindOwned[P, R](a0, a1)
}

func BindUnpreparedBorrowed[P any, R any](a0 Statement[P, R], a1 Columns, a2 P) (*Bound[R], error) {
	return implExecution.BindUnpreparedBorrowed[P, R](a0, a1, a2)
}

func BindUnpreparedOwned[P any, R any](a0 Statement[P, R], a1 Columns, a2 P) (*Bound[R], error) {
	return implExecution.BindUnpreparedOwned[P, R](a0, a1, a2)
}

func Exec[R any](a0 context.Context, a1 *Session, a2 *Bound[R], a3 Options) (Result, error) {
	return implExecution.Exec[R](a0, a1, a2, a3)
}

func Run[R any](a0 context.Context, a1 *Session, a2 *Bound[R], a3 Options) (*Rows[R], error) {
	return implExecution.Run[R](a0, a1, a2, a3)
}

func RunPage[R any](a0 context.Context, a1 *Session, a2 *Bound[R], a3 PageState, a4 Options) (*Page[R], error) {
	return implExecution.RunPage[R](a0, a1, a2, a3, a4)
}

func RunStream[B any, R any](a0 context.Context, a1 *Session, a2 *Bound[B], a3 StreamRowDecoder[R], a4 StreamOptions) (*RowStream[R], error) {
	return implExecution.RunStream[B, R](a0, a1, a2, a3, a4)
}

func RunUnpaged[R any](a0 context.Context, a1 *Session, a2 *Bound[R], a3 Options) (*Page[R], error) {
	return implExecution.RunUnpaged[R](a0, a1, a2, a3)
}
