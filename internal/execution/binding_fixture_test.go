// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package execution_test

import (
	. "github.com/scylladb/gocql/v2"
	implExecution "github.com/scylladb/gocql/v2/internal/execution"
)

// Internal immutable-binding fixtures remain separate from the public query contract.
type Bound[R any] = implExecution.Bound[R]

func BindUnpreparedBorrowed[P any, R any](a0 Statement[P, R], a1 Columns, a2 P) (*Bound[R], error) {
	return implExecution.BindUnpreparedBorrowed[P, R](a0, a1, a2)
}

func BindUnpreparedOwned[P any, R any](a0 Statement[P, R], a1 Columns, a2 P) (*Bound[R], error) {
	return implExecution.BindUnpreparedOwned[P, R](a0, a1, a2)
}
