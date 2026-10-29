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

package gocql

import (
	"fmt"
	"io"
	"net/netip"

	"github.com/scylladb/gocql/v2/internal/wire"
)

// Only built-in fixed-domain scalar codecs use this bounded scratch seam. It
// cannot adapt arbitrary buffered codecs or variable-size cells to streaming.
type streamScalarCodec[T any] struct {
	base             Codec[T]
	minimum, maximum int64
}
type streamScalarPlan[T any] struct {
	base             Codec[T]
	typ              DataType
	minimum, maximum int64
}

func (c streamScalarCodec[T]) Compile(t DataType) (StreamValuePlan[T], error) {
	if err := c.base.CheckType(t); err != nil {
		return nil, err
	}
	return streamScalarPlan[T]{base: c.base, typ: t, minimum: c.minimum, maximum: c.maximum}, nil
}
func (p streamScalarPlan[T]) Reset(dst *T) {
	if dst != nil {
		var z T
		*dst = z
	}
}
func (p streamScalarPlan[T]) DecodeInto(r *StreamValueReader, dst *T) error {
	if dst == nil {
		return r.fail(typeValidation("destination", "nil scalar destination"))
	}
	if err := r.present(); err != nil {
		return err
	}
	if !r.matches(p.typ) {
		return r.fail(typeValidation("type", "compiled streaming scalar metadata changed"))
	}
	n := r.Remaining()
	if n < p.minimum || n > p.maximum {
		return r.fail(typeValidation("length", "%s requires %d..%d bytes, got %d", r.typ, p.minimum, p.maximum, n))
	}
	b := r.shared.scalar[:int(n)]
	if err := r.readFull(b); err != nil {
		return err
	}
	chain, _ := wire.NewChain(wire.Borrow(b))
	value, err := p.base.Decode(newValueReader(p.typ, Present, chain, nil))
	if err != nil {
		return r.fail(err)
	}
	*dst = value
	return nil
}
func streamFixed[T any](codec Codec[T], size int64) StreamDecoder[T] {
	return streamScalarCodec[T]{base: codec, minimum: size, maximum: size}
}

var (
	StreamBool      StreamDecoder[bool]       = streamFixed(Bool, 1)
	StreamInt8      StreamDecoder[int8]       = streamFixed(Int8, 1)
	StreamInt16     StreamDecoder[int16]      = streamFixed(Int16, 2)
	StreamInt32     StreamDecoder[int32]      = streamFixed(Int32, 4)
	StreamInt64     StreamDecoder[int64]      = streamFixed(Int64, 8)
	StreamFloat32   StreamDecoder[float32]    = streamFixed(Float32, 4)
	StreamFloat64   StreamDecoder[float64]    = streamFixed(Float64, 8)
	StreamCounter   StreamDecoder[Counter]    = streamFixed(CounterCodec, 8)
	StreamDate      StreamDecoder[Date]       = streamFixed(DateCodec, 4)
	StreamTime      StreamDecoder[TimeOfDay]  = streamFixed(TimeCodec, 8)
	StreamTimestamp StreamDecoder[Timestamp]  = streamFixed(TimestampCodec, 8)
	StreamDuration  StreamDecoder[Duration]   = streamScalarCodec[Duration]{base: DurationCodec, minimum: 3, maximum: 27}
	StreamUUID      StreamDecoder[UUID]       = streamFixed(UUIDCodec, 16)
	StreamTimeUUID  StreamDecoder[TimeUUID]   = streamFixed(TimeUUIDCodec, 16)
	StreamInet      StreamDecoder[netip.Addr] = streamScalarCodec[netip.Addr]{base: InetCodec, minimum: 4, maximum: 16}
	StreamText      StreamDecoder[[]byte]     = streamBytesCodec{kind: TypeText}
	StreamASCII     StreamDecoder[[]byte]     = streamBytesCodec{kind: TypeAscii}
	StreamBlob      StreamDecoder[[]byte]     = streamBytesCodec{kind: TypeBlob}
	StreamVarint    StreamDecoder[[]byte]     = streamBytesCodec{kind: TypeVarint}
)

type streamBytesCodec struct{ kind Type }
type streamBytesPlan struct{ typ DataType }

func (c streamBytesCodec) Compile(t DataType) (StreamValuePlan[[]byte], error) {
	if err := validateDataType(t); err != nil {
		return nil, err
	}
	if c.kind == TypeText {
		if err := checkTextType(t, false); err != nil {
			return nil, err
		}
	} else if t.Kind() != c.kind {
		return nil, typeValidation("type", "streaming %s cannot decode %s", c.kind, t)
	}
	return streamBytesPlan{typ: t}, nil
}
func (p streamBytesPlan) Reset(dst *[]byte) {
	if dst != nil {
		*dst = (*dst)[:0]
	}
}
func (p streamBytesPlan) DecodeInto(r *StreamValueReader, dst *[]byte) error {
	if dst == nil {
		return r.fail(typeValidation("destination", "nil byte destination"))
	}
	if err := r.present(); err != nil {
		return err
	}
	if !r.matches(p.typ) {
		return r.fail(typeValidation("type", "compiled byte metadata changed"))
	}
	n := r.Remaining()
	if p.typ.Kind() == TypeVarint && n == 0 {
		return r.fail(typeValidation("varint", "requires nonempty signed bytes"))
	}
	if err := r.capacity(n, cap(*dst)); err != nil {
		return err
	}
	*dst = (*dst)[:int(n)]
	validator := newTextValidation(p.typ.Kind() == TypeAscii)
	text := p.typ.Kind() == TypeText || p.typ.Kind() == TypeVarchar || p.typ.Kind() == TypeAscii
	b := *dst
	for len(b) > 0 {
		n, err := r.Read(b)
		if text {
			if e := validator.write(b[:n]); e != nil {
				return r.fail(e)
			}
		}
		b = b[n:]
		if err != nil {
			if len(b) == 0 && err == io.EOF {
				break
			}
			return r.fail(err)
		}
	}
	if text {
		if err := validator.finish(); err != nil {
			return r.fail(err)
		}
	}
	return r.RequireEnd()
}

type StreamDecimalValue struct {
	Scale    int32
	Unscaled []byte
}
type streamDecimalCodec struct{}
type streamDecimalPlan struct{ typ DataType }

var StreamDecimal StreamDecoder[StreamDecimalValue] = streamDecimalCodec{}

func (streamDecimalCodec) Compile(t DataType) (StreamValuePlan[StreamDecimalValue], error) {
	if err := DecimalCodec.CheckType(t); err != nil {
		return nil, err
	}
	return streamDecimalPlan{typ: t}, nil
}
func (streamDecimalPlan) Reset(dst *StreamDecimalValue) {
	if dst != nil {
		dst.Scale = 0
		dst.Unscaled = dst.Unscaled[:0]
	}
}
func (p streamDecimalPlan) DecodeInto(r *StreamValueReader, dst *StreamDecimalValue) error {
	if dst == nil {
		return r.fail(typeValidation("destination", "nil decimal destination"))
	}
	if err := r.present(); err != nil {
		return err
	}
	if !r.matches(p.typ) {
		return r.fail(typeValidation("type", "compiled decimal metadata changed"))
	}
	n := r.Remaining()
	if n < 5 {
		return r.fail(typeValidation("decimal", "requires scale and nonempty integer"))
	}
	// Capacity rejection precedes even the scale bytes of this value.
	if err := r.capacity(n-4, cap(dst.Unscaled)); err != nil {
		return err
	}
	scale, err := r.Uint32()
	if err != nil {
		return err
	}
	dst.Scale = int32(scale)
	dst.Unscaled = dst.Unscaled[:int(n-4)]
	return r.readFull(dst.Unscaled)
}

type streamNullableCodec[T any] struct{ base StreamDecoder[T] }
type streamNullablePlan[T any] struct {
	base StreamValuePlan[T]
	typ  DataType
}

func StreamNullable[T any](base StreamDecoder[T]) StreamDecoder[Nullable[T]] {
	return streamNullableCodec[T]{base}
}
func (c streamNullableCodec[T]) Compile(t DataType) (StreamValuePlan[Nullable[T]], error) {
	if c.base == nil {
		return nil, typeValidation("stream codec", "nil nullable child")
	}
	p, e := compileStreamValuePlan(c.base, t)
	if e != nil {
		return nil, e
	}
	return streamNullablePlan[T]{base: p, typ: t}, nil
}
func (p streamNullablePlan[T]) Reset(dst *Nullable[T]) {
	if dst != nil {
		p.base.Reset(&dst.Value)
		dst.Valid = false
	}
}
func (p streamNullablePlan[T]) DecodeInto(r *StreamValueReader, dst *Nullable[T]) error {
	if dst == nil {
		return r.fail(typeValidation("destination", "nil nullable destination"))
	}
	if err := r.check(); err != nil {
		return err
	}
	if !r.matches(p.typ) {
		return r.fail(typeValidation("type", "compiled nullable metadata changed"))
	}
	p.Reset(dst)
	if r.state == Null {
		return r.RequireEnd()
	}
	if err := p.base.DecodeInto(r, &dst.Value); err != nil {
		return err
	}
	dst.Valid = true
	return nil
}

type streamEmptyCodec[T any] struct{ base StreamDecoder[T] }
type streamEmptyPlan[T any] struct {
	base StreamValuePlan[T]
	typ  DataType
}

func StreamEmpty[T any](base StreamDecoder[T]) StreamDecoder[EmptyValueOf[T]] {
	return streamEmptyCodec[T]{base}
}
func (c streamEmptyCodec[T]) Compile(t DataType) (StreamValuePlan[EmptyValueOf[T]], error) {
	if c.base == nil {
		return nil, typeValidation("stream codec", "nil empty child")
	}
	p, e := compileStreamValuePlan(c.base, t)
	if e != nil {
		return nil, e
	}
	if !emptySupported(t) {
		return nil, typeValidation("type", "%s does not support Empty", t)
	}
	return streamEmptyPlan[T]{base: p, typ: t}, nil
}
func (p streamEmptyPlan[T]) Reset(dst *EmptyValueOf[T]) {
	if dst != nil {
		p.base.Reset(&dst.Value)
		dst.Empty = false
	}
}
func (p streamEmptyPlan[T]) DecodeInto(r *StreamValueReader, dst *EmptyValueOf[T]) error {
	if dst == nil {
		return r.fail(typeValidation("destination", "nil empty destination"))
	}
	if err := r.present(); err != nil {
		return err
	}
	if !r.matches(p.typ) {
		return r.fail(typeValidation("type", "compiled empty metadata changed"))
	}
	p.Reset(dst)
	if r.Remaining() == 0 {
		dst.Empty = true
		return nil
	}
	return p.base.DecodeInto(r, &dst.Value)
}

func streamFailure(path string, err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("gocql: %s: %w", path, err)
}

func (c streamScalarCodec[T]) Reset(dst *T) {
	if dst != nil {
		var zero T
		*dst = zero
	}
}
func (c streamBytesCodec) Reset(dst *[]byte) {
	if dst != nil {
		*dst = (*dst)[:0]
	}
}
func (c streamDecimalCodec) Reset(dst *StreamDecimalValue) {
	if dst != nil {
		dst.Scale = 0
		dst.Unscaled = dst.Unscaled[:0]
	}
}
func (c streamNullableCodec[T]) Reset(dst *Nullable[T]) {
	if dst != nil {
		resetStreamDeclaration(c.base, &dst.Value)
		dst.Valid = false
	}
}
func (c streamNullableCodec[T]) streamCanReset() bool { return streamDeclarationCanReset(c.base) }
func (c streamEmptyCodec[T]) Reset(dst *EmptyValueOf[T]) {
	if dst != nil {
		resetStreamDeclaration(c.base, &dst.Value)
		dst.Empty = false
	}
}
func (c streamEmptyCodec[T]) streamCanReset() bool { return streamDeclarationCanReset(c.base) }
