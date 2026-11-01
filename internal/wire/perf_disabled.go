//go:build !gocql_perf

// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package wire

func perfAdd(kind, n int) {}

func perfLease(delta int64) {}
func perfSlab(n int)        {}
