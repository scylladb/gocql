// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cqltype

// Option distinguishes an explicit zero value from an inherited setting.
type Option[T any] struct {
	Value T
	Set   bool
}
