// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package runtimeerrors

func QueryErrorData(v *QueryError) *QueryErrorState { return &v.state }
func NewQueryError(v QueryErrorState) *QueryError   { return &QueryError{state: v} }
