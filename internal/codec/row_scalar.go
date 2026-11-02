// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package codec

import (
	"fmt"
	"io"

	cqltype "github.com/scylladb/gocql/v2/internal/cqltype"
)

// Only the driver's built-in scalar codecs can implement this private hook.
// User codecs continue to receive their independent immutable ValueReader.
type rowValueIntoDecoder[T any] interface {
	decodeColumnInto(*RowReader, *T) error
}

func (c scalarCodec[T]) decodeColumnInto(r *RowReader, dst *T) error {
	if c.decodeRow == nil {
		value, err := r.nextValue()
		if err == nil {
			var decoded T
			decoded, err = c.Decode(value)
			if err == nil {
				*dst = decoded
			}
		}
		return err
	}
	value, err := c.decodeRow(r, c.kind, c.alternative, c.compiled)
	if err == nil {
		*dst = value
	}
	return err
}

// scalarColumn validates a full cell transactionally, leaving its present body
// on the row cursor for direct consumption. On a semantic error it skips the
// body, matching nextValue followed by scalarCodec.Decode. A zero width admits
// an arbitrary blob length; scalar widths require an exact match.
func (r *RowReader) scalarColumn(kind, alternative cqltype.Type, compiled bool, width int64) (int64, error) {
	if r == nil || r.cursor == nil {
		return 0, fmt.Errorf("gocql: nil row reader")
	}
	column, ok := r.columns.At(r.index)
	if !ok {
		return 0, fmt.Errorf("gocql: too many columns: index %d", r.index)
	}
	missing := r.allowMissing && r.cursor.Remaining() == 0
	var length int32 = -1
	if !missing {
		if err := cqltype.ValidateDataType(column.Type); err != nil {
			return 0, fmt.Errorf("gocql: column %d (%s): %w", r.index, column.Name, err)
		}
		if r.lease != nil && !r.lease.Alive() {
			return 0, fmt.Errorf("gocql: column %d (%s): %w", r.index, column.Name, cqltype.ErrReleased)
		}
		checkpoint := *r.cursor
		prefix, err := r.cursor.Uint32()
		length = int32(prefix)
		if err == nil {
			if length < -1 {
				err = fmt.Errorf("invalid cell length %d", length)
			} else if int64(length) > r.cursor.Remaining() {
				err = io.ErrUnexpectedEOF
			}
		}
		if err != nil {
			*r.cursor = checkpoint
			return 0, fmt.Errorf("gocql: column %d (%s): %w", r.index, column.Name, &cqltype.ProtocolError{Stage: "cell", Err: err})
		}
	}
	r.index++
	fail := func(err error) (int64, error) {
		if length >= 0 {
			_ = r.cursor.Skip(int64(length)) // the full length was checked above
		}
		return 0, err
	}
	// Standalone codecs retain their CheckType instrumentation contract.
	if !compiled {
		perfCheckType()
	}
	if err := checkScalarType(column.Type, kind, alternative); err != nil {
		return fail(err)
	}
	if length == -1 {
		return 0, fmt.Errorf("gocql: nonnullable %s cannot decode cell state %d", column.Type.Kind(), cqltype.Null)
	}
	// This is the same present-value lease guard as requirePresent. The integer
	// path also retains its cursor guard before loading the scalar.
	if r.lease != nil && !r.lease.Alive() {
		return fail(cqltype.ErrReleased)
	}
	if width > 0 && int64(length) != width {
		return fail(fmt.Errorf("gocql: %s requires %d bytes, got %d", column.Type.Kind(), width, length))
	}
	return int64(length), nil
}
