// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package gocql

import binding "github.com/scylladb/gocql/v2/internal/binding"

type Statement[P any, R any] = binding.Statement[P, R]

func conditionalCQL(a0 string) bool { return binding.InternalConditionalCQL(a0) }
func firstCQLWord(a0 string) string { return binding.InternalFirstCQLWord(a0) }
func validateStatement[P any, R any](a0 Statement[P, R]) error {
	return binding.InternalValidateStatement[P, R](a0)
}
func visitCQLWords(a0 string, a1 func(string) bool) { binding.InternalVisitCQLWords(a0, a1) }
