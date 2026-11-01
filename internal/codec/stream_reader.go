/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package codec

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"math/bits"

	cqltype "github.com/scylladb/gocql/v2/internal/cqltype"
	"github.com/scylladb/gocql/v2/internal/wire"
)

// StreamCapacityError reports required final storage without implicit growth.

type StreamCapacityError struct {
	Path                string
	Required, Available int64
}

func (e *StreamCapacityError) Error() string {
	return fmt.Sprintf("gocql: streaming capacity at %s: need %d, have %d", e.Path, e.Required, e.Available)
}

// StreamCapabilityError identifies a mode or mapping without an incremental path.
type StreamCapabilityError struct {
	Protocol    int
	Compression cqltype.Compression
	Reason      string
}

func (e *StreamCapabilityError) Error() string {
	return fmt.Sprintf("gocql: streaming protocol %d/compression %d: %s", e.Protocol, e.Compression, e.Reason)
}

// StreamResetter supplies declaration reset for application fields absent from metadata.
// It preserves caller payload capacity without inventing a database type.
type StreamResetter[T any] interface{ Reset(*T) }

// StreamDecoder compiles against actual metadata, including absent payloads.
type StreamDecoder[T any] interface {
	Compile(cqltype.DataType) (StreamValuePlan[T], error)
}
type StreamValuePlan[T any] interface {
	Reset(*T)
	DecodeInto(*StreamValueReader, *T) error
}
type StreamRowDecoder[R any] interface {
	Compile(cqltype.Columns) (StreamRowPlan[R], error)
}
type StreamRowPlan[R any] interface {
	Reset(*R)
	DecodeInto(*StreamRowReader, *R) error
}

type streamCapture struct {
	dst    []byte
	offset int
}
type streamDecodeState struct {
	values       []*StreamValueReader
	rows         []*StreamRowReader
	scalar       [32]byte
	discard      []byte
	discardBytes int
	err          error
	typeMatches  map[streamTypePair]bool
	rowMatches   map[streamColumnsPair]bool
	childMatches map[streamChildPair]*streamValidationPlan
}

func (s *streamDecodeState) value(depth int) *StreamValueReader {
	for len(s.values) <= depth {
		s.values = append(s.values, new(StreamValueReader))
	}
	return s.values[depth]
}
func (s *streamDecodeState) row(depth int) *StreamRowReader {
	for len(s.rows) <= depth {
		s.rows = append(s.rows, new(StreamRowReader))
	}
	return s.rows[depth]
}
func (s *streamDecodeState) fail(err error) error {
	if s.err == nil {
		s.err = err
	}
	return s.err
}

// StreamValueReader is synchronous and limited to the current cell body.
// A custom plan may not retain it, its destination, or any scratch after return.
type StreamValueReader struct {
	scope        *wire.Stream
	typ          cqltype.DataType
	state        cqltype.CellState
	shared       *streamDecodeState
	validation   *streamValidationPlan
	capture      *streamCapture
	captureState streamCapture
	depth        int
	path         string
	active       bool
}

func (r *StreamValueReader) Context() context.Context {
	if r == nil || r.scope == nil {
		return context.Background()
	}
	return r.scope.Context()
}
func (r *StreamValueReader) Type() cqltype.DataType   { return r.typ }
func (r *StreamValueReader) State() cqltype.CellState { return r.state }
func (r *StreamValueReader) Remaining() int64 {
	if r == nil || r.scope == nil {
		return 0
	}
	return r.scope.Remaining()
}
func (r *StreamValueReader) check() error {
	if r == nil || r.scope == nil || r.shared == nil {
		return cqltype.TypeValidation("stream reader", "uninitialized value reader")
	}
	if r.shared.err != nil {
		return r.shared.err
	}
	if !r.active {
		return r.shared.fail(cqltype.TypeValidation("stream reader", "reader escaped its callback"))
	}
	if err := r.Context().Err(); err != nil {
		return r.shared.fail(err)
	}
	return nil
}
func (r *StreamValueReader) fail(err error) error {
	if r == nil || r.shared == nil {
		return err
	}
	return r.shared.fail(err)
}
func (r *StreamValueReader) Read(p []byte) (int, error) {
	if err := r.check(); err != nil {
		return 0, err
	}
	var n int
	var err error
	if r.capture == nil {
		n, err = r.scope.Read(p)
	} else {
		take := min(int64(len(p)), r.Remaining())
		if take > int64(len(r.capture.dst)-r.capture.offset) {
			return 0, r.fail(wire.ErrBounds)
		}
		target := r.capture.dst[r.capture.offset : r.capture.offset+int(take)]
		n, err = r.scope.Read(target)
		copy(p, target[:n]) // Bounded scalar/framing scratch, never a payload gather.
		r.capture.offset += n
	}
	if err != nil && err != io.EOF {
		return n, r.fail(err)
	}
	return n, err
}
func (r *StreamValueReader) readFull(p []byte) error {
	if err := r.check(); err != nil {
		return err
	}
	if int64(len(p)) > r.Remaining() {
		return r.fail(io.ErrUnexpectedEOF)
	}
	for len(p) > 0 {
		n, err := r.Read(p)
		p = p[n:]
		if err != nil {
			if err == io.EOF && len(p) == 0 {
				return nil
			}
			return r.fail(err)
		}
	}
	return nil
}
func (r *StreamValueReader) uint(size int) (uint64, error) {
	if err := r.check(); err != nil {
		return 0, err
	}
	b := r.shared.scalar[:size]
	if err := r.readFull(b); err != nil {
		return 0, err
	}
	switch size {
	case 1:
		return uint64(b[0]), nil
	case 2:
		return uint64(binary.BigEndian.Uint16(b)), nil
	case 4:
		return uint64(binary.BigEndian.Uint32(b)), nil
	default:
		return binary.BigEndian.Uint64(b), nil
	}
}
func (r *StreamValueReader) Uint8() (uint8, error)   { n, e := r.uint(1); return uint8(n), e }
func (r *StreamValueReader) Uint16() (uint16, error) { n, e := r.uint(2); return uint16(n), e }
func (r *StreamValueReader) Uint32() (uint32, error) { n, e := r.uint(4); return uint32(n), e }
func (r *StreamValueReader) Uint64() (uint64, error) { return r.uint(8) }
func (r *StreamValueReader) RequireEnd() error {
	if err := r.check(); err != nil {
		return err
	}
	if err := r.scope.RequireEnd(); err != nil {
		return r.fail(err)
	}
	return nil
}
func (r *StreamValueReader) present() error {
	if err := r.check(); err != nil {
		return err
	}
	if r.state != cqltype.Present {
		return r.fail(cqltype.TypeValidation("stream value", "nonnullable %s has state %d", r.typ, r.state))
	}
	return nil
}
func (r *StreamValueReader) capacity(n int64, available int) error {
	if n < 0 || n > int64(available) {
		return r.fail(&StreamCapacityError{Path: r.path, Required: n, Available: int64(available)})
	}
	return nil
}
func (r *StreamValueReader) childBody(typ cqltype.DataType, state cqltype.CellState, size int64, validation *streamValidationPlan, fn func(*StreamValueReader) error) error {
	if err := r.check(); err != nil {
		return err
	}
	if fn == nil {
		return r.fail(cqltype.TypeValidation("stream child", "nil callback"))
	}
	err := r.scope.Child(size, func(scope *wire.Stream) error {
		child := r.shared.value(r.depth + 1)
		*child = StreamValueReader{scope: scope, typ: typ, state: state, shared: r.shared, validation: validation, capture: r.capture, depth: r.depth + 1, path: r.path, active: true}
		defer func() { child.active = false; child.capture = nil }()
		if err := fn(child); err != nil {
			return err
		}
		return child.RequireEnd()
	})
	if err != nil {
		return r.fail(err)
	}
	return nil
}
func (r *StreamValueReader) child(typ cqltype.DataType, validation *streamValidationPlan, allowNull bool, fn func(*StreamValueReader) error) error {
	length, err := r.Uint32()
	if err != nil {
		return err
	}
	n := int64(int32(length))
	state := cqltype.Present
	if n == -1 && allowNull {
		state, n = cqltype.Null, 0
	} else if n < 0 {
		return r.fail(cqltype.TypeValidation("stream length", "invalid nested cell length %d", n))
	}
	return r.childBody(typ, state, n, validation, fn)
}

// Child reads one signed-length framed member. Result unset is always invalid.
func (r *StreamValueReader) Child(typ cqltype.DataType, fn func(*StreamValueReader) error) error {
	if err := cqltype.ValidateDataType(typ); err != nil {
		return r.fail(err)
	}
	validation, err := r.childValidation(typ)
	if err != nil {
		return r.fail(err)
	}
	return r.child(typ, validation, true, fn)
}

// Skip validates the entire discarded value, including nested scalar semantics.
func (r *StreamValueReader) Skip() error {
	if err := r.check(); err != nil {
		return err
	}
	p := r.validation
	if p == nil {
		var err error
		p, err = compileStreamValidation(r.typ, 0)
		if err != nil {
			return r.fail(err)
		}
	}
	return p.skip(r, true)
}
func (r *StreamValueReader) rawSkip(validate func([]byte) error) error {
	if err := r.check(); err != nil {
		return err
	}
	if r.shared.discard == nil && r.capture == nil {
		r.shared.discard = make([]byte, r.shared.discardBytes)
	}
	for r.Remaining() > 0 {
		var b []byte
		if r.capture != nil {
			n := min(r.Remaining(), int64(r.shared.discardBytes))
			if n > int64(len(r.capture.dst)-r.capture.offset) {
				return r.fail(wire.ErrBounds)
			}
			b = r.capture.dst[r.capture.offset : r.capture.offset+int(n)]
			nread, err := r.scope.Read(b)
			r.capture.offset += nread
			b = b[:nread]
			if validate != nil {
				if e := validate(b); e != nil {
					return r.fail(e)
				}
			}
			if err != nil && !(err == io.EOF && r.Remaining() == 0) {
				return r.fail(err)
			}
		} else {
			b = r.shared.discard[:min(int64(len(r.shared.discard)), r.Remaining())]
			if err := r.readFull(b); err != nil {
				return err
			}
			if validate != nil {
				if err := validate(b); err != nil {
					return r.fail(err)
				}
			}
		}
	}
	return nil
}
func (r *StreamValueReader) count(minimum int64) (int, error) {
	v, err := r.Uint32()
	if err != nil {
		return 0, err
	}
	n := int64(int32(v))
	if n < 0 {
		return 0, r.fail(wire.ErrLength)
	}
	if n > r.scope.Limits().MaxCount || (minimum > 0 && n > r.Remaining()/minimum) {
		return 0, r.fail(wire.ErrLimit)
	}
	return int(n), nil
}
func (r *StreamValueReader) vectorLength() (int64, error) {
	first, err := r.Uint8()
	if err != nil {
		return 0, err
	}
	extra := bits.LeadingZeros8(^first)
	n := uint64(first & byte(0xff>>uint(extra)))
	for i := 0; i < extra; i++ {
		b, e := r.Uint8()
		if e != nil {
			return 0, e
		}
		n = n<<8 | uint64(b)
	}
	if (extra > 0 && n < uint64(1)<<uint(extra*7)) || n > math.MaxInt32 {
		return 0, r.fail(cqltype.TypeValidation("stream vector length", "invalid or noncanonical element length"))
	}
	return int64(n), nil
}

// StreamRowReader visits physical columns in wire order. Runtime owners reuse
// it between rows; RequireEnd checks columns, not a complete multi-row envelope.
type StreamRowReader struct {
	columns      cqltype.Columns
	scope        *wire.Stream
	shared       *streamDecodeState
	validations  []*streamValidationPlan
	index, depth int
	allowMissing bool
	active       bool
	capture      *streamCapture
}

// InternalStreamReaderWorkingStorageBytes reports the actual retained byte
// scratch plus conservative scope descriptor capacities. Validation plans/maps
// depend on schema and are accounted separately from payload working storage.
// Captures alias final application storage and must not be counted again.
func InternalStreamReaderWorkingStorageBytes(r *StreamRowReader) int64 {
	if r == nil || r.shared == nil {
		return 0
	}
	return int64(cap(r.shared.discard)) + 32 + int64(cap(r.shared.values))*192 + int64(cap(r.shared.rows))*128
}

func NewStreamRowReader(columns cqltype.Columns, scope *wire.Stream) (*StreamRowReader, error) {
	if scope == nil {
		return nil, cqltype.TypeValidation("stream reader", "nil envelope")
	}
	state := &streamDecodeState{discardBytes: scope.Limits().DiscardBytes}
	r := &StreamRowReader{columns: columns, scope: scope, shared: state, active: true}
	r.validations = make([]*streamValidationPlan, columns.Len())
	for i, c := range cqltype.ColumnSlice(columns) {
		p, err := compileStreamValidation(c.Type, 0)
		if err != nil {
			return nil, err
		}
		r.validations[i] = p
	}
	return r, nil
}
func (r *StreamRowReader) Context() context.Context {
	if r == nil || r.scope == nil {
		return context.Background()
	}
	return r.scope.Context()
}
func (r *StreamRowReader) check() error {
	if r == nil || r.scope == nil || r.shared == nil {
		return cqltype.TypeValidation("stream row", "uninitialized reader")
	}
	if r.shared.err != nil {
		return r.shared.err
	}
	if !r.active {
		return r.shared.fail(cqltype.TypeValidation("stream row", "inactive reader"))
	}
	if err := r.Context().Err(); err != nil {
		return r.shared.fail(err)
	}
	return nil
}
func (r *StreamRowReader) column(fn func(*StreamValueReader) error) error {
	if err := r.check(); err != nil {
		return err
	}
	column, ok := r.columns.At(r.index)
	if !ok {
		return r.shared.fail(cqltype.TypeValidation("stream column", "too many columns"))
	}
	i := r.index
	r.index++
	state := cqltype.Present
	var size int64
	if r.allowMissing && r.scope.Remaining() == 0 {
		state = cqltype.Null
	} else {
		n, err := r.framingUint32()
		if err != nil {
			return r.shared.fail(err)
		}
		size = int64(int32(n))
		if size == -1 {
			state, size = cqltype.Null, 0
		} else if size < 0 {
			return r.shared.fail(cqltype.TypeValidation("stream length", "invalid result length %d", size))
		}
	}
	err := r.scope.Child(size, func(scope *wire.Stream) error {
		value := r.shared.value(r.depth)
		*value = StreamValueReader{scope: scope, typ: column.Type, state: state, shared: r.shared, validation: r.validations[i], capture: r.capture, depth: r.depth, path: column.Name, active: true}
		defer func() { value.active = false; value.capture = nil }()
		if err := fn(value); err != nil {
			return err
		}
		return value.RequireEnd()
	})
	if err != nil {
		return r.shared.fail(&cqltype.CodecError{Column: column.Name, Index: i, Stage: "stream decode", Err: err})
	}
	return nil
}
func ReadStreamColumn[T any](r *StreamRowReader, plan StreamValuePlan[T], dst *T) error {
	if plan == nil || dst == nil {
		return cqltype.TypeValidation("stream column", "nil plan or destination")
	}
	return r.column(func(value *StreamValueReader) error { return plan.DecodeInto(value, dst) })
}
func (r *StreamRowReader) SkipColumn() error {
	return r.column(func(value *StreamValueReader) error { return value.Skip() })
}
func (r *StreamRowReader) RequireEnd() error {
	if err := r.check(); err != nil {
		return err
	}
	if r.index != r.columns.Len() {
		return r.shared.fail(cqltype.TypeValidation("stream row", "decoded %d of %d columns", r.index, r.columns.Len()))
	}
	return nil
}

func (r *StreamRowReader) framingUint32() (uint32, error) {
	if r.capture == nil {
		return r.scope.Uint32()
	}
	if len(r.capture.dst)-r.capture.offset < 4 {
		return 0, wire.ErrBounds
	}
	b := r.capture.dst[r.capture.offset : r.capture.offset+4]
	if err := r.scope.ReadFull(b); err != nil {
		return 0, err
	}
	r.capture.offset += 4
	return binary.BigEndian.Uint32(b), nil
}

type streamTypePair struct {
	expected, actual *cqltype.DataTypeDescriptor
}
type streamColumnsPair struct {
	expected, actual       *cqltype.ColumnSpec
	expectedLen, actualLen int
}
type streamChildPair struct {
	parent, requested *cqltype.DataTypeDescriptor
}

func (r *StreamValueReader) matches(t cqltype.DataType) bool {
	if cqltype.DescriptorOf(t) == cqltype.DescriptorOf(r.typ) {
		return true
	}
	key := streamTypePair{cqltype.DescriptorOf(t), cqltype.DescriptorOf(r.typ)}
	if known, ok := r.shared.typeMatches[key]; ok {
		return known
	}
	equal := t.Equal(r.typ)
	if r.shared.typeMatches == nil {
		r.shared.typeMatches = make(map[streamTypePair]bool)
	}
	r.shared.typeMatches[key] = equal
	return equal
}
func (r *StreamRowReader) matches(columns cqltype.Columns) bool {
	if columns.Len() == 0 || r.columns.Len() == 0 {
		return columns.Len() == r.columns.Len()
	}
	key := streamColumnsPair{&cqltype.ColumnSlice(columns)[0], &cqltype.ColumnSlice(r.columns)[0], columns.Len(), r.columns.Len()}
	if key.expected == key.actual && key.expectedLen == key.actualLen {
		return true
	}
	if known, ok := r.shared.rowMatches[key]; ok {
		return known
	}
	equal := columns.Equal(r.columns)
	if r.shared.rowMatches == nil {
		r.shared.rowMatches = make(map[streamColumnsPair]bool)
	}
	r.shared.rowMatches[key] = equal
	return equal
}
func (r *StreamValueReader) childValidation(t cqltype.DataType) (*streamValidationPlan, error) {
	if r.validation == nil {
		return nil, cqltype.TypeValidation("stream child", "missing compiled parent validation")
	}
	if child := r.validation.children[cqltype.DescriptorOf(t)]; child != nil {
		return child, nil
	}
	key := streamChildPair{cqltype.DescriptorOf(r.typ), cqltype.DescriptorOf(t)}
	if child := r.shared.childMatches[key]; child != nil {
		return child, nil
	}
	// Equivalent application descriptors are resolved once per response shape.
	for _, child := range r.validation.children {
		if child.typ.Equal(t) {
			if r.shared.childMatches == nil {
				r.shared.childMatches = make(map[streamChildPair]*streamValidationPlan)
			}
			if len(r.shared.childMatches) >= 64 {
				clear(r.shared.childMatches)
			}
			r.shared.childMatches[key] = child
			return child, nil
		}
	}
	return nil, cqltype.TypeValidation("stream child", "requested type is absent from actual parent metadata")
}

func compileStreamValuePlan[T any](decoder StreamDecoder[T], typ cqltype.DataType) (StreamValuePlan[T], error) {
	if decoder == nil {
		return nil, cqltype.TypeValidation("stream codec", "nil value decoder")
	}
	plan, err := decoder.Compile(typ)
	if err == nil && plan == nil {
		err = cqltype.TypeValidation("stream codec", "Compile returned a nil value plan")
	}
	return plan, err
}
func compileStreamRowPlan[R any](decoder StreamRowDecoder[R], columns cqltype.Columns) (StreamRowPlan[R], error) {
	if decoder == nil {
		return nil, cqltype.TypeValidation("stream codec", "nil row decoder")
	}
	plan, err := decoder.Compile(columns)
	if err == nil && plan == nil {
		err = cqltype.TypeValidation("stream codec", "Compile returned a nil row plan")
	}
	return plan, err
}

type streamResetCapability interface{ streamCanReset() bool }

func streamDeclarationCanReset[T any](decoder StreamDecoder[T]) bool {
	if decoder == nil {
		return false
	}
	if c, ok := decoder.(streamResetCapability); ok && !c.streamCanReset() {
		return false
	}
	_, ok := decoder.(StreamResetter[T])
	return ok
}
func resetStreamDeclaration[T any](decoder StreamDecoder[T], dst *T) {
	decoder.(StreamResetter[T]).Reset(dst)
}
