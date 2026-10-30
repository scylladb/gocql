// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cqltype

type Compression uint8

const (
	NoCompression Compression = iota
	LZ4Compression
	SnappyCompression
)
