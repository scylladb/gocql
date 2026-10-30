// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cqltype

// StatementKind is an explicit classification; the driver never parses CQL to infer it.
type StatementKind uint8

const (
	UnknownStatement StatementKind = iota
	OrdinaryStatement
	ConditionalStatement
)
