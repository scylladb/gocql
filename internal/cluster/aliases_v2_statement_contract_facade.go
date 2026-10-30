// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cluster

import binding "github.com/scylladb/gocql/v2/internal/binding"

type Statement[P any, R any] = binding.Statement[P, R]

func validateStatement[P any, R any](a0 Statement[P, R]) error {
	return binding.InternalValidateStatement[P, R](a0)
}
