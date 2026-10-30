// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cqltype

import (
	"errors"
	"fmt"
)

var ErrReleased = errors.New("gocql: borrowed value has been released")

// ValidationError describes an invalid API argument or type descriptor.
type ValidationError struct {
	Field string
	Err   error
}

func (e *ValidationError) Error() string { return fmt.Sprintf("gocql: invalid %s: %v", e.Field, e.Err) }
func (e *ValidationError) Unwrap() error { return e.Err }

// CodecError records the physical column and nested path of a codec failure.
type CodecError struct {
	Column string
	Index  int
	Path   string
	Stage  string
	Err    error
}

func (e *CodecError) Error() string {
	return fmt.Sprintf("gocql: %s column %d (%s)%s: %v", e.Stage, e.Index, e.Column, e.Path, e.Err)
}
func (e *CodecError) Unwrap() error { return e.Err }

// ProtocolError records the stage and byte offset of invalid protocol data.
type ProtocolError struct {
	Offset int64
	Stage  string
	Err    error
}

func (e *ProtocolError) Error() string {
	return fmt.Sprintf("gocql: %s at byte %d: %v", e.Stage, e.Offset, e.Err)
}
func (e *ProtocolError) Unwrap() error { return e.Err }
