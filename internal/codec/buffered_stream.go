// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package codec

import (
	"encoding/binary"
	"errors"
	cqltype "github.com/scylladb/gocql/v2/internal/cqltype"
	"github.com/scylladb/gocql/v2/internal/wire"
	"io"
)

// Only built-in immutable plans implement this interface. Custom codecs and
// incremental transports retain callback-scoped checked StreamValueReaders.
type bufferedStreamPlan[T any] interface {
	bufferedType() cqltype.DataType
	bufferedCapable() bool
	decodeBuffered(wire.Chain, wire.CellState, *streamDecodeState, string, *T) error
}

func decodeBufferedColumn[T any](r *StreamRowReader, p bufferedStreamPlan[T], dst *T) error {
	if err := r.check(); err != nil {
		return err
	}
	column, ok := r.columns.At(r.index)
	if !ok {
		return r.shared.fail(cqltype.TypeValidation("stream column", "too many columns"))
	}
	i := r.index
	r.index++
	body, state, err := r.scope.BufferedCell()
	if err != nil {
		if errors.Is(err, wire.ErrLength) {
			err = cqltype.TypeValidation("stream length", "invalid result length")
		}
		return r.shared.fail(err)
	}
	if !column.Type.Equal(p.bufferedType()) {
		err = cqltype.TypeValidation("type", "compiled streaming metadata changed")
	} else {
		err = p.decodeBuffered(body, state, r.shared, column.Name, dst)
	}
	if err == nil {
		err = r.Context().Err()
	}
	if err != nil {
		r.scope.Fail(err)
		return r.shared.fail(&cqltype.CodecError{Column: column.Name, Index: i, Stage: "stream decode", Err: err})
	}
	return nil
}
func bufferedPresent(typ cqltype.DataType, state wire.CellState) error {
	if state != wire.CellPresent {
		return cqltype.TypeValidation("stream value", "nonnullable %s has state %d", typ, state)
	}
	return nil
}
func (p streamScalarPlan[T]) bufferedType() cqltype.DataType { return p.typ }
func (p streamScalarPlan[T]) bufferedCapable() bool          { return p.buffered != nil }
func (p streamScalarPlan[T]) decodeBuffered(body wire.Chain, state wire.CellState, shared *streamDecodeState, path string, dst *T) error {
	if err := bufferedPresent(p.typ, state); err != nil {
		return err
	}
	if body.Len() < p.minimum || body.Len() > p.maximum {
		return cqltype.TypeValidation("length", "%s requires %d..%d bytes, got %d", p.typ, p.minimum, p.maximum, body.Len())
	}
	value, err := p.buffered(body, shared)
	if errors.Is(err, wire.ErrBounds) {
		err = io.ErrUnexpectedEOF
	}
	if err == nil {
		*dst = value
	}
	return err
}
func (p streamBytesPlan) bufferedType() cqltype.DataType { return p.typ }
func (p streamBytesPlan) bufferedCapable() bool          { return true }
func (p streamBytesPlan) decodeBuffered(body wire.Chain, state wire.CellState, _ *streamDecodeState, path string, dst *[]byte) error {
	if err := bufferedPresent(p.typ, state); err != nil {
		return err
	}
	n := body.Len()
	if p.typ.Kind() == cqltype.TypeVarint && n == 0 {
		return cqltype.TypeValidation("varint", "requires nonempty signed bytes")
	}
	if n > int64(cap(*dst)) {
		return &StreamCapacityError{Path: path, Required: n, Available: int64(cap(*dst))}
	}
	*dst = (*dst)[:int(n)]
	if _, err := body.CopyTo(*dst); err != nil {
		return err
	}
	if p.typ.Kind() == cqltype.TypeText || p.typ.Kind() == cqltype.TypeVarchar || p.typ.Kind() == cqltype.TypeAscii {
		v := newTextValidation(p.typ.Kind() == cqltype.TypeAscii)
		if err := v.write(*dst); err != nil {
			return err
		}
		return v.finish()
	}
	return nil
}
func (p streamDecimalPlan) bufferedType() cqltype.DataType { return p.typ }
func (p streamDecimalPlan) bufferedCapable() bool          { return true }
func (p streamDecimalPlan) decodeBuffered(body wire.Chain, state wire.CellState, _ *streamDecodeState, path string, dst *StreamDecimalValue) error {
	if err := bufferedPresent(p.typ, state); err != nil {
		return err
	}
	n := body.Len()
	if n < 5 {
		return cqltype.TypeValidation("decimal", "requires scale and nonempty integer")
	}
	if n-4 > int64(cap(dst.Unscaled)) {
		return &StreamCapacityError{Path: path, Required: n - 4, Available: int64(cap(dst.Unscaled))}
	}
	var scale [4]byte
	header, _ := body.Slice(0, 4)
	if _, err := header.CopyTo(scale[:]); err != nil {
		return err
	}
	dst.Scale = int32(binary.BigEndian.Uint32(scale[:]))
	dst.Unscaled = dst.Unscaled[:int(n-4)]
	integer, _ := body.Slice(4, n-4)
	_, err := integer.CopyTo(dst.Unscaled)
	return err
}
func (p streamNullablePlan[T]) bufferedType() cqltype.DataType { return p.typ }
func (p streamNullablePlan[T]) bufferedCapable() bool {
	v, ok := p.base.(bufferedStreamPlan[T])
	return ok && v.bufferedCapable()
}
func (p streamNullablePlan[T]) decodeBuffered(body wire.Chain, state wire.CellState, shared *streamDecodeState, path string, dst *cqltype.Nullable[T]) error {
	p.Reset(dst)
	if state == wire.CellNull {
		return nil
	}
	if err := p.base.(bufferedStreamPlan[T]).decodeBuffered(body, state, shared, path, &dst.Value); err != nil {
		return err
	}
	dst.Valid = true
	return nil
}
func (p streamEmptyPlan[T]) bufferedType() cqltype.DataType { return p.typ }
func (p streamEmptyPlan[T]) bufferedCapable() bool {
	v, ok := p.base.(bufferedStreamPlan[T])
	return ok && v.bufferedCapable()
}
func (p streamEmptyPlan[T]) decodeBuffered(body wire.Chain, state wire.CellState, shared *streamDecodeState, path string, dst *cqltype.EmptyValueOf[T]) error {
	if err := bufferedPresent(p.typ, state); err != nil {
		return err
	}
	p.Reset(dst)
	if body.Len() == 0 {
		dst.Empty = true
		return nil
	}
	return p.base.(bufferedStreamPlan[T]).decodeBuffered(body, state, shared, path, &dst.Value)
}

func bufferedSignedVInt(cursor *wire.Cursor) (int64, error) {
	first, err := cursor.Uint8()
	if err != nil {
		return 0, err
	}
	value := newScalarVInt(first)
	for value.remaining > 0 {
		next, err := cursor.Uint8()
		if err != nil {
			return 0, err
		}
		value.add(next)
	}
	return value.signed(), nil
}
func bufferedDuration(body wire.Chain, _ *streamDecodeState) (cqltype.Duration, error) {
	var cursor wire.Cursor
	wire.ResetCursor(&cursor, body)
	months, err := bufferedSignedVInt(&cursor)
	if err != nil {
		return cqltype.Duration{}, err
	}
	days, err := bufferedSignedVInt(&cursor)
	if err != nil {
		return cqltype.Duration{}, err
	}
	nanos, err := bufferedSignedVInt(&cursor)
	if err != nil {
		return cqltype.Duration{}, err
	}
	if cursor.Remaining() != 0 {
		return cqltype.Duration{}, wire.ErrLength
	}
	return durationFromComponents(months, days, nanos)
}
