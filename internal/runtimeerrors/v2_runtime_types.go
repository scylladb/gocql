// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package runtimeerrors

import (
	"errors"
)

var (
	ErrClosed           = errors.New("gocql: closed")
	ErrPagingStateLimit = errors.New("gocql: paging state history limit exceeded")
	ErrPagingComplete   = errors.New("gocql: completed page cannot be resumed")
	ErrPagingNoProgress = errors.New("gocql: paging made no progress")
	ErrNoRows           = errors.New("gocql: no rows")
	ErrMultipleRows     = errors.New("gocql: multiple rows")
)

// SchemaChangedError requires preparing and binding again after incompatible metadata.
type SchemaChangedError struct {
	Statement         string
	Previous, Current Columns
}

func (e *SchemaChangedError) Error() string {
	return "gocql: bound parameter schema changed for " + e.Statement
}
