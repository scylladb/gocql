// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package gocql

import binding "github.com/scylladb/gocql/v2/internal/binding"

func encodeParameters[P any](a0 ParamPlan[P], a1 Columns, a2 P, a3 bool, a4 bool) (encodedValues, error) {
	return binding.InternalEncodeParameters[P](a0, a1, a2, a3, a4)
}
