// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package codec

import (
	"fmt"
	"math"

	cqltype "github.com/scylladb/gocql/v2/internal/cqltype"
	"github.com/scylladb/gocql/v2/internal/wire"
)

// ValueWriter writes one length-framed cell. The enclosing plan owns completion;
// an encoder writes only its body or one explicit null/unset marker.
type ValueWriter struct {
	builder                     *wire.Builder
	typ                         cqltype.DataType
	mark                        wire.Mark
	start                       int64
	state                       cqltype.CellState
	allowUnset, wrote, finished bool
	ownBorrowed                 bool
	err                         error
}

func NewValueWriter(builder *wire.Builder, typ cqltype.DataType, allowUnset bool) *ValueWriter {
	w := &ValueWriter{builder: builder, typ: typ, allowUnset: allowUnset}
	if builder == nil {
		w.err = cqltype.TypeValidation("writer", "nil builder")
		return w
	}
	w.mark = builder.Mark()
	w.start = builder.Len()
	if err := cqltype.ValidateDataType(typ); err != nil {
		w.err = err
		return w
	}
	w.err = builder.Uint32(0)
	return w
}
func newOwnedValueWriter(builder *wire.Builder, typ cqltype.DataType, allowUnset bool) *ValueWriter {
	w := NewValueWriter(builder, typ, allowUnset)
	w.ownBorrowed = true
	return w
}
func (w *ValueWriter) Type() cqltype.DataType { return w.typ }
func (w *ValueWriter) fail(err error) error {
	if w.err == nil {
		w.err = err
	}
	return w.err
}
func (w *ValueWriter) ready() error {
	if w.err != nil {
		return w.err
	}
	if w.finished {
		return w.fail(cqltype.TypeValidation("writer", "cell is already finished"))
	}
	if w.builder == nil {
		return w.fail(cqltype.TypeValidation("writer", "uninitialized writer"))
	}
	return nil
}
func (w *ValueWriter) payload() error {
	if err := w.ready(); err != nil {
		return err
	}
	if w.state != cqltype.Present {
		return w.fail(cqltype.TypeValidation("value", "payload cannot be combined with null or unset"))
	}
	w.wrote = true
	return nil
}
func (w *ValueWriter) setState(state cqltype.CellState) error {
	if err := w.ready(); err != nil {
		return err
	}
	if w.wrote || w.state != cqltype.Present {
		return w.fail(cqltype.TypeValidation("value", "cell state was already written"))
	}
	if state == cqltype.Unset && !w.allowUnset {
		return w.fail(cqltype.TypeValidation("value", "unset is not permitted in this cell"))
	}
	w.state = state
	return nil
}
func (w *ValueWriter) Empty() error { return w.emptyValue() }
func (w *ValueWriter) Null() error  { return w.setState(cqltype.Null) }
func (w *ValueWriter) Unset() error { return w.setState(cqltype.Unset) }
func (w *ValueWriter) emptyValue() error {
	if !emptySupported(w.typ) {
		return w.fail(cqltype.TypeValidation("type", "%s does not support empty values", w.typ))
	}
	return w.setState(cqltype.Empty)
}
func (w *ValueWriter) BorrowBytes(b []byte) error {
	if err := w.payload(); err != nil {
		return err
	}
	if w.ownBorrowed {
		b = append([]byte(nil), b...)
	}
	return w.fail(w.builder.Borrow(wire.Borrow(b)))
}
func (w *ValueWriter) BorrowString(s string) error {
	if err := w.payload(); err != nil {
		return err
	}
	if w.ownBorrowed {
		return w.fail(w.builder.Borrow(wire.Borrow([]byte(s))))
	}
	return w.fail(w.builder.Borrow(wire.BorrowString(s)))
}
func (w *ValueWriter) borrowView(b cqltype.Bytes) error {
	if err := w.payload(); err != nil {
		return err
	}
	return w.fail(cqltype.EachBytes(b, func(s wire.Span) error {
		if w.ownBorrowed {
			s = wire.Borrow(append([]byte(nil), s.Bytes()...))
		}
		return w.builder.Borrow(s)
	}))
}
func (w *ValueWriter) Uint8(v uint8) error {
	if err := w.payload(); err != nil {
		return err
	}
	return w.fail(w.builder.Uint8(v))
}
func (w *ValueWriter) Uint16(v uint16) error {
	if err := w.payload(); err != nil {
		return err
	}
	return w.fail(w.builder.Uint16(v))
}
func (w *ValueWriter) Uint32(v uint32) error {
	if err := w.payload(); err != nil {
		return err
	}
	return w.fail(w.builder.Uint32(v))
}
func (w *ValueWriter) Uint64(v uint64) error {
	if err := w.payload(); err != nil {
		return err
	}
	return w.fail(w.builder.Uint64(v))
}
func (w *ValueWriter) Child(typ cqltype.DataType, encode func(*ValueWriter) error) error {
	if err := w.payload(); err != nil {
		return err
	}
	if encode == nil {
		return w.fail(cqltype.TypeValidation("encoder", "nil child encoder"))
	}
	child := NewValueWriter(w.builder, typ, false)
	child.ownBorrowed = w.ownBorrowed
	if child.err != nil {
		abortValueWriter(child)
		return w.fail(child.err)
	}
	if err := encode(child); err != nil {
		abortValueWriter(child)
		return w.fail(err)
	}
	return w.fail(FinishValueWriter(child))
}
func abortValueWriter(w *ValueWriter) {
	if w == nil || w.finished {
		return
	}
	if w.builder != nil {
		w.builder.Rollback(w.mark)
	}
	w.finished = true
}
func FinishValueWriter(w *ValueWriter) error {
	if w == nil {
		return cqltype.TypeValidation("writer", "nil writer")
	}
	if err := w.ready(); err != nil {
		abortValueWriter(w)
		return err
	}
	length := w.builder.Len() - w.start - 4
	if length < 0 || length > math.MaxInt32 {
		w.fail(cqltype.TypeValidation("value", "cell length %d is outside signed 32-bit range", length))
	}
	var prefix uint32
	switch w.state {
	case cqltype.Present:
		prefix = uint32(length)
	case cqltype.Empty:
		prefix = 0
	case cqltype.Null:
		prefix = math.MaxUint32
	case cqltype.Unset:
		prefix = math.MaxUint32 - 1
	default:
		w.fail(cqltype.TypeValidation("value", "invalid cell state %d", w.state))
	}
	if w.state != cqltype.Present && length != 0 {
		w.fail(cqltype.TypeValidation("value", "null or unset cell contains a payload"))
	}
	if w.err == nil {
		w.fail(w.builder.PatchUint32(w.mark, prefix))
	}
	if w.err != nil {
		abortValueWriter(w)
		return w.err
	}
	w.finished = true
	return nil
}

// ValueReader is an immutable snapshot. Each Cursor starts at the beginning.
type ValueReader struct {
	typ   cqltype.DataType
	state cqltype.CellState
	body  wire.Chain
	lease *wire.Lease
	err   error
}

func NewValueReader(typ cqltype.DataType, state cqltype.CellState, body wire.Chain, lease *wire.Lease) ValueReader {
	r := ValueReader{typ: typ, state: state, body: body, lease: lease}
	r.err = cqltype.ValidateDataType(typ)
	if state > cqltype.Empty || (state != cqltype.Present && body.Len() != 0) {
		r.err = &cqltype.ProtocolError{Stage: "cell", Err: fmt.Errorf("invalid state %d for %d bytes", state, body.Len())}
	}
	return r
}
func (r ValueReader) Type() cqltype.DataType   { return r.typ }
func (r ValueReader) State() cqltype.CellState { return r.state }
func (r ValueReader) Bytes() cqltype.Bytes {
	if r.err != nil {
		panic(r.err)
	}
	return cqltype.BytesView(r.body, r.lease)
}
func (r ValueReader) Cursor() *ValueCursor {
	c := r.cursorValue()
	return &c
}
func (r ValueReader) cursorValue() ValueCursor {
	return ValueCursor{cursor: *wire.NewCursor(r.body), initialized: true, lease: r.lease, initial: r.body.Len(), err: r.err}
}

// ValueCursor reads checked network scalars and nested cells across spans. A
// failure is sticky; malformed data cannot be followed by a successful read.
type ValueCursor struct {
	cursor      wire.Cursor
	initialized bool
	lease       *wire.Lease
	initial     int64
	err         error
}

func (c *ValueCursor) Remaining() int64 {
	if c == nil || !c.initialized {
		return 0
	}
	return c.cursor.Remaining()
}
func (c *ValueCursor) check() error {
	if c.err != nil {
		return c.err
	}
	if !c.initialized {
		c.err = cqltype.TypeValidation("cursor", "uninitialized cursor")
	} else if c.lease != nil && !c.lease.Alive() {
		c.err = cqltype.ErrReleased
	}
	return c.err
}
func (c *ValueCursor) failure(err error) error {
	if err != nil && c.err == nil {
		c.err = &cqltype.ProtocolError{Offset: c.initial - c.Remaining(), Stage: "value", Err: err}
	}
	return c.err
}
func (c *ValueCursor) Uint8() (uint8, error) {
	if err := c.check(); err != nil {
		return 0, err
	}
	v, err := c.cursor.Uint8()
	return v, c.failure(err)
}
func (c *ValueCursor) Uint16() (uint16, error) {
	if err := c.check(); err != nil {
		return 0, err
	}
	v, err := c.cursor.Uint16()
	return v, c.failure(err)
}
func (c *ValueCursor) Uint32() (uint32, error) {
	if err := c.check(); err != nil {
		return 0, err
	}
	v, err := c.cursor.Uint32()
	return v, c.failure(err)
}
func (c *ValueCursor) Uint64() (uint64, error) {
	if err := c.check(); err != nil {
		return 0, err
	}
	v, err := c.cursor.Uint64()
	return v, c.failure(err)
}
func (c *ValueCursor) Take(n int64) (cqltype.Bytes, error) {
	if err := c.check(); err != nil {
		return cqltype.Bytes{}, err
	}
	body, err := c.cursor.Take(n)
	if err != nil {
		return cqltype.Bytes{}, c.failure(err)
	}
	return cqltype.BytesView(body, c.lease), nil
}
func (c *ValueCursor) Child(typ cqltype.DataType) (ValueReader, error) {
	if err := c.check(); err != nil {
		return ValueReader{}, err
	}
	r, err := ReadValue(&c.cursor, typ, c.lease, false)
	if err != nil {
		return ValueReader{}, c.failure(err)
	}
	return r, nil
}
func (c *ValueCursor) RequireEnd() error {
	if err := c.check(); err != nil {
		return err
	}
	return c.failure(c.cursor.RequireEnd())
}

// readValue validates a full framed cell before advancing the caller's cursor.
// Zero-length cells remain Present; EmptyCodec interprets scalar empty values.
func ReadValue(cursor *wire.Cursor, typ cqltype.DataType, lease *wire.Lease, allowUnset bool) (ValueReader, error) {
	if err := cqltype.ValidateDataType(typ); err != nil {
		return ValueReader{}, err
	}
	if lease != nil && !lease.Alive() {
		return ValueReader{}, cqltype.ErrReleased
	}
	if cursor == nil {
		return ValueReader{}, cqltype.TypeValidation("cursor", "nil cursor")
	}
	checkpoint := *cursor
	fail := func(err error) (ValueReader, error) {
		*cursor = checkpoint
		return ValueReader{}, &cqltype.ProtocolError{Stage: "cell", Err: err}
	}
	prefix, err := cursor.Uint32()
	if err != nil {
		return fail(err)
	}
	length := int32(prefix)
	if length == -1 {
		return NewValueReader(typ, cqltype.Null, wire.Chain{}, lease), nil
	}
	if length == -2 && allowUnset {
		return NewValueReader(typ, cqltype.Unset, wire.Chain{}, lease), nil
	}
	if length < 0 {
		return fail(fmt.Errorf("invalid cell length %d", length))
	}
	body, err := cursor.Take(int64(length))
	if err != nil {
		return fail(err)
	}
	return NewValueReader(typ, cqltype.Present, body, lease), nil
}
