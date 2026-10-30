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
	"encoding/binary"
	"fmt"
	cqltype "github.com/scylladb/gocql/v2/internal/cqltype"
	"io"
	"math"
	"net/netip"
)

// Streaming scalars use bounded native conversion callbacks. They never create
// a ValueReader or call a buffered decoder; variable-size values read directly
// into final caller storage. The largest fixed scalar scratch is 16 bytes.

type streamScalarCodec[T any] struct {
	kind             cqltype.Type
	decode           func(*StreamValueReader) (T, error)
	minimum, maximum int64
}
type streamScalarPlan[T any] struct {
	decode           func(*StreamValueReader) (T, error)
	typ              cqltype.DataType
	minimum, maximum int64
}

func (c streamScalarCodec[T]) Compile(t cqltype.DataType) (StreamValuePlan[T], error) {
	if err := checkScalarType(t, c.kind, 0); err != nil {
		return nil, err
	}
	return streamScalarPlan[T]{decode: c.decode, typ: t, minimum: c.minimum, maximum: c.maximum}, nil
}
func (p streamScalarPlan[T]) Reset(dst *T) {
	if dst != nil {
		var zero T
		*dst = zero
	}
}
func (p streamScalarPlan[T]) DecodeInto(r *StreamValueReader, dst *T) error {
	if dst == nil {
		return r.fail(cqltype.TypeValidation("destination", "nil scalar destination"))
	}
	if err := r.present(); err != nil {
		return err
	}
	if !r.matches(p.typ) {
		return r.fail(cqltype.TypeValidation("type", "compiled streaming scalar metadata changed"))
	}
	n := r.Remaining()
	if n < p.minimum || n > p.maximum {
		return r.fail(cqltype.TypeValidation("length", "%s requires %d..%d bytes, got %d", r.typ, p.minimum, p.maximum, n))
	}
	value, err := p.decode(r)
	if err != nil {
		return r.fail(err)
	}
	if err := r.RequireEnd(); err != nil {
		return err
	}
	*dst = value
	return nil
}
func streamFixed[T any](kind cqltype.Type, size int64, convert func([]byte) (T, error)) StreamDecoder[T] {
	return streamScalarCodec[T]{kind: kind, minimum: size, maximum: size, decode: func(r *StreamValueReader) (T, error) {
		bytes := r.shared.scalar[:int(size)]
		if err := r.readFull(bytes); err != nil {
			var zero T
			return zero, err
		}
		return convert(bytes)
	}}
}
func streamInteger[T ~int8 | ~int16 | ~int32 | ~int64](kind cqltype.Type, size int64) StreamDecoder[T] {
	return streamFixed(kind, size, func(b []byte) (T, error) {
		switch len(b) {
		case 1:
			return T(int8(b[0])), nil
		case 2:
			return T(int16(binary.BigEndian.Uint16(b))), nil
		case 4:
			return T(int32(binary.BigEndian.Uint32(b))), nil
		default:
			return T(int64(binary.BigEndian.Uint64(b))), nil
		}
	})
}
func streamReadSignedVInt(r *StreamValueReader) (int64, error) {
	first, err := r.Uint8()
	if err != nil {
		return 0, err
	}
	v := newScalarVInt(first)
	for v.remaining > 0 {
		next, err := r.Uint8()
		if err != nil {
			return 0, err
		}
		v.add(next)
	}
	return v.signed(), nil
}
func streamReadDuration(r *StreamValueReader) (cqltype.Duration, error) {

	months, err := streamReadSignedVInt(r)
	if err != nil {
		return cqltype.Duration{}, err
	}
	days, err := streamReadSignedVInt(r)
	if err != nil {
		return cqltype.Duration{}, err
	}
	nanos, err := streamReadSignedVInt(r)
	if err != nil {
		return cqltype.Duration{}, err
	}
	return durationFromComponents(months, days, nanos)
}

var (
	StreamBool StreamDecoder[bool] = streamFixed(cqltype.TypeBoolean, 1, func(b []byte) (bool, error) {
		if b[0] > 1 {
			return false, cqltype.TypeValidation("boolean", "must be 0 or 1")
		}
		return b[0] == 1, nil
	})
	StreamInt8      = streamInteger[int8](cqltype.TypeTinyInt, 1)
	StreamInt16     = streamInteger[int16](cqltype.TypeSmallInt, 2)
	StreamInt32     = streamInteger[int32](cqltype.TypeInt, 4)
	StreamInt64     = streamInteger[int64](cqltype.TypeBigInt, 8)
	StreamCounter   = streamInteger[Counter](cqltype.TypeCounter, 8)
	StreamTimestamp = streamInteger[Timestamp](cqltype.TypeTimestamp, 8)
	StreamFloat32   = streamFixed(cqltype.TypeFloat, 4, func(b []byte) (float32, error) { return math.Float32frombits(binary.BigEndian.Uint32(b)), nil })
	StreamFloat64   = streamFixed(cqltype.TypeDouble, 8, func(b []byte) (float64, error) { return math.Float64frombits(binary.BigEndian.Uint64(b)), nil })
	StreamDate      = streamFixed(cqltype.TypeDate, 4, func(b []byte) (Date, error) { return Date(int64(binary.BigEndian.Uint32(b)) - (1 << 31)), nil })
	StreamTime      = streamFixed(cqltype.TypeTime, 8, func(b []byte) (TimeOfDay, error) {
		v := binary.BigEndian.Uint64(b)
		if v >= uint64(nanosecondsPerDay) {
			return 0, cqltype.TypeValidation("time", "outside one day")
		}
		return TimeOfDay(v), nil
	})
	StreamDuration StreamDecoder[cqltype.Duration] = streamScalarCodec[cqltype.Duration]{kind: cqltype.TypeDuration, minimum: 3, maximum: 27, decode: streamReadDuration}
	StreamUUID                                     = streamFixed(cqltype.TypeUUID, 16, func(b []byte) (cqltype.UUID, error) { var v cqltype.UUID; copy(v[:], b); return v, nil })
	StreamTimeUUID                                 = streamFixed(cqltype.TypeTimeUUID, 16, func(b []byte) (TimeUUID, error) {
		var v TimeUUID
		copy(v[:], b)
		if cqltype.UUID(v).Version() != 1 {
			return TimeUUID{}, cqltype.TypeValidation("timeuuid", "must have version 1")
		}
		return v, nil
	})
	StreamInet StreamDecoder[netip.Addr] = streamScalarCodec[netip.Addr]{kind: cqltype.TypeInet, minimum: 4, maximum: 16, decode: func(r *StreamValueReader) (netip.Addr, error) {
		n := r.Remaining()
		if n != 4 && n != 16 {
			return netip.Addr{}, cqltype.TypeValidation("inet", "requires 4 or 16 bytes")
		}
		b := r.shared.scalar[:int(n)]
		if err := r.readFull(b); err != nil {
			return netip.Addr{}, err
		}
		v, _ := netip.AddrFromSlice(b)
		return v, nil
	}}
	StreamText   StreamDecoder[[]byte] = streamBytesCodec{kind: cqltype.TypeText}
	StreamASCII  StreamDecoder[[]byte] = streamBytesCodec{kind: cqltype.TypeAscii}
	StreamBlob   StreamDecoder[[]byte] = streamBytesCodec{kind: cqltype.TypeBlob}
	StreamVarint StreamDecoder[[]byte] = streamBytesCodec{kind: cqltype.TypeVarint}
)

type streamBytesCodec struct{ kind cqltype.Type }
type streamBytesPlan struct{ typ cqltype.DataType }

func (c streamBytesCodec) Compile(t cqltype.DataType) (StreamValuePlan[[]byte], error) {
	if err := cqltype.ValidateDataType(t); err != nil {
		return nil, err
	}
	if c.kind == cqltype.TypeText {
		if err := checkTextType(t, false); err != nil {
			return nil, err
		}
	} else if t.Kind() != c.kind {
		return nil, cqltype.TypeValidation("type", "streaming %s cannot decode %s", c.kind, t)
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
		return r.fail(cqltype.TypeValidation("destination", "nil byte destination"))
	}
	if err := r.present(); err != nil {
		return err
	}
	if !r.matches(p.typ) {
		return r.fail(cqltype.TypeValidation("type", "compiled byte metadata changed"))
	}
	n := r.Remaining()
	if p.typ.Kind() == cqltype.TypeVarint && n == 0 {
		return r.fail(cqltype.TypeValidation("varint", "requires nonempty signed bytes"))
	}
	if err := r.capacity(n, cap(*dst)); err != nil {
		return err
	}
	*dst = (*dst)[:int(n)]
	validator := newTextValidation(p.typ.Kind() == cqltype.TypeAscii)
	text := p.typ.Kind() == cqltype.TypeText || p.typ.Kind() == cqltype.TypeVarchar || p.typ.Kind() == cqltype.TypeAscii
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
type streamDecimalPlan struct{ typ cqltype.DataType }

var StreamDecimal StreamDecoder[StreamDecimalValue] = streamDecimalCodec{}

func (streamDecimalCodec) Compile(t cqltype.DataType) (StreamValuePlan[StreamDecimalValue], error) {
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
		return r.fail(cqltype.TypeValidation("destination", "nil decimal destination"))
	}
	if err := r.present(); err != nil {
		return err
	}
	if !r.matches(p.typ) {
		return r.fail(cqltype.TypeValidation("type", "compiled decimal metadata changed"))
	}
	n := r.Remaining()
	if n < 5 {
		return r.fail(cqltype.TypeValidation("decimal", "requires scale and nonempty integer"))
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
	typ  cqltype.DataType
}

func StreamNullable[T any](base StreamDecoder[T]) StreamDecoder[cqltype.Nullable[T]] {
	return streamNullableCodec[T]{base}
}
func (c streamNullableCodec[T]) Compile(t cqltype.DataType) (StreamValuePlan[cqltype.Nullable[T]], error) {
	if c.base == nil {
		return nil, cqltype.TypeValidation("stream codec", "nil nullable child")
	}
	p, e := compileStreamValuePlan(c.base, t)
	if e != nil {
		return nil, e
	}
	return streamNullablePlan[T]{base: p, typ: t}, nil
}
func (p streamNullablePlan[T]) Reset(dst *cqltype.Nullable[T]) {
	if dst != nil {
		p.base.Reset(&dst.Value)
		dst.Valid = false
	}
}
func (p streamNullablePlan[T]) DecodeInto(r *StreamValueReader, dst *cqltype.Nullable[T]) error {
	if dst == nil {
		return r.fail(cqltype.TypeValidation("destination", "nil nullable destination"))
	}
	if err := r.check(); err != nil {
		return err
	}
	if !r.matches(p.typ) {
		return r.fail(cqltype.TypeValidation("type", "compiled nullable metadata changed"))
	}
	p.Reset(dst)
	if r.state == cqltype.Null {
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
	typ  cqltype.DataType
}

func StreamEmpty[T any](base StreamDecoder[T]) StreamDecoder[cqltype.EmptyValueOf[T]] {
	return streamEmptyCodec[T]{base}
}
func (c streamEmptyCodec[T]) Compile(t cqltype.DataType) (StreamValuePlan[cqltype.EmptyValueOf[T]], error) {
	if c.base == nil {
		return nil, cqltype.TypeValidation("stream codec", "nil empty child")
	}
	p, e := compileStreamValuePlan(c.base, t)
	if e != nil {
		return nil, e
	}
	if !emptySupported(t) {
		return nil, cqltype.TypeValidation("type", "%s does not support Empty", t)
	}
	return streamEmptyPlan[T]{base: p, typ: t}, nil
}
func (p streamEmptyPlan[T]) Reset(dst *cqltype.EmptyValueOf[T]) {
	if dst != nil {
		p.base.Reset(&dst.Value)
		dst.Empty = false
	}
}
func (p streamEmptyPlan[T]) DecodeInto(r *StreamValueReader, dst *cqltype.EmptyValueOf[T]) error {
	if dst == nil {
		return r.fail(cqltype.TypeValidation("destination", "nil empty destination"))
	}
	if err := r.present(); err != nil {
		return err
	}
	if !r.matches(p.typ) {
		return r.fail(cqltype.TypeValidation("type", "compiled empty metadata changed"))
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
func (c streamNullableCodec[T]) Reset(dst *cqltype.Nullable[T]) {
	if dst != nil {
		resetStreamDeclaration(c.base, &dst.Value)
		dst.Valid = false
	}
}
func (c streamNullableCodec[T]) streamCanReset() bool { return streamDeclarationCanReset(c.base) }
func (c streamEmptyCodec[T]) Reset(dst *cqltype.EmptyValueOf[T]) {
	if dst != nil {
		resetStreamDeclaration(c.base, &dst.Value)
		dst.Empty = false
	}
}
func (c streamEmptyCodec[T]) streamCanReset() bool { return streamDeclarationCanReset(c.base) }
