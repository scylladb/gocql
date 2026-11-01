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
	"fmt"
	cqltype "github.com/scylladb/gocql/v2/internal/cqltype"
	"github.com/scylladb/gocql/v2/internal/wire"
)

// streamValidationPlan is compiled once per physical shape. Ignored columns and
// explicit encoded output share these exact incremental validation operations.

type streamValidationPlan struct {
	typ          cqltype.DataType
	scalar       func(*StreamValueReader) error
	element, key *streamValidationPlan
	fields       []*streamValidationPlan
	width        int64
	children     map[*cqltype.DataTypeDescriptor]*streamValidationPlan
	height       int
	building     bool
}

func compileStreamValidation(t cqltype.DataType, depth int) (*streamValidationPlan, error) {
	return compileStreamValidationMemo(t, depth, make(map[*cqltype.DataTypeDescriptor]*streamValidationPlan))
}
func compileStreamValidationMemo(t cqltype.DataType, depth int, memo map[*cqltype.DataTypeDescriptor]*streamValidationPlan) (*streamValidationPlan, error) {
	if err := cqltype.ValidateDataType(t); err != nil {
		return nil, err
	}
	if depth > 128 {
		return nil, cqltype.TypeValidation("type", "streaming type nesting exceeds 128")
	}
	if p := memo[cqltype.DescriptorOf(t)]; p != nil {
		if p.building {
			return nil, cqltype.TypeValidation("type", "cyclic streaming descriptor")
		}
		if depth+p.height > 128 {
			return nil, cqltype.TypeValidation("type", "streaming type nesting exceeds 128")
		}
		return p, nil
	}
	p := &streamValidationPlan{typ: t, building: true}
	memo[cqltype.DescriptorOf(t)] = p
	d := cqltype.DescriptorOf(t)
	child := func(t cqltype.DataType) (*streamValidationPlan, error) {
		return compileStreamValidationMemo(t, depth+1, memo)
	}
	var err error
	switch t.Kind() {
	case cqltype.TypeList, cqltype.TypeSet, cqltype.TypeCustom:
		p.element, err = child(d.Element)
		if err != nil {
			return nil, err
		}
		if t.Kind() == cqltype.TypeCustom {
			if d.Dimension == 0 {
				return nil, cqltype.TypeValidation("dimension", "zero vector dimension")
			}
			p.width, err = vectorFixedWidth(d.Element)
		}
	case cqltype.TypeMap:
		p.key, err = child(d.Key)
		if err == nil {
			p.element, err = child(d.Element)
		}
	case cqltype.TypeTuple, cqltype.TypeUDT:
		if t.Kind() == cqltype.TypeUDT && (d.Keyspace == "" || d.Name == "") {
			return nil, cqltype.TypeValidation("UDT identity", "incomplete descriptor")
		}
		p.fields = make([]*streamValidationPlan, d.Fields.Len())
		for i, c := range cqltype.ColumnSlice(d.Fields) {
			p.fields[i], err = child(c.Type)
			if err != nil {
				return nil, err
			}
		}
	default:
		err = cqltype.ValidateScalarKind(t.Kind())
		if err == nil {
			p.scalar, err = compileStreamScalarValidation(t)
		}
	}
	if err != nil {
		return nil, err
	}
	if p.element != nil || p.key != nil || len(p.fields) > 0 {
		p.children = make(map[*cqltype.DataTypeDescriptor]*streamValidationPlan)
	}
	if p.element != nil {
		p.children[cqltype.DescriptorOf(p.element.typ)] = p.element
	}
	if p.key != nil {
		p.children[cqltype.DescriptorOf(p.key.typ)] = p.key
	}
	for _, child := range p.fields {
		p.children[cqltype.DescriptorOf(child.typ)] = child
	}
	for _, child := range p.children {
		p.height = max(p.height, child.height+1)
	}
	if depth+p.height > 128 {
		return nil, cqltype.TypeValidation("type", "streaming type nesting exceeds 128")
	}
	p.building = false
	return p, nil
}
func (p *streamValidationPlan) skip(r *StreamValueReader, allowNull bool) error {
	if err := r.check(); err != nil {
		return err
	}
	if !r.matches(p.typ) {
		return r.fail(cqltype.TypeValidation("type", "validation metadata changed"))
	}
	if r.state == cqltype.Null && allowNull {
		return r.RequireEnd()
	}
	if err := r.present(); err != nil {
		return err
	}
	if r.Remaining() == 0 && emptySupported(r.typ) {
		return nil
	}
	switch p.typ.Kind() {
	case cqltype.TypeBlob, cqltype.TypeVarint, cqltype.TypeDecimal:
		minimum := int64(0)
		if p.typ.Kind() == cqltype.TypeVarint {
			minimum = 1
		}
		if p.typ.Kind() == cqltype.TypeDecimal {
			minimum = 5
		}
		if r.Remaining() < minimum {
			return r.fail(cqltype.TypeValidation("length", "invalid %s length", p.typ))
		}
		return r.rawSkip(nil)
	case cqltype.TypeText, cqltype.TypeVarchar, cqltype.TypeAscii:
		validator := newTextValidation(p.typ.Kind() == cqltype.TypeAscii)
		if err := r.rawSkip(validator.write); err != nil {
			return err
		}
		return r.failIf(validator.finish())
	case cqltype.TypeList, cqltype.TypeSet, cqltype.TypeMap:
		minimum := int64(4)
		if p.typ.Kind() == cqltype.TypeMap {
			minimum = 8
		}
		count, err := r.count(minimum)
		if err != nil {
			return err
		}
		for i := 0; i < count; i++ {
			if p.key != nil {
				if err := r.child(p.key.typ, p.key, false, func(c *StreamValueReader) error { return p.key.skip(c, false) }); err != nil {
					return streamFailure("map key", err)
				}
			}
			if err := r.child(p.element.typ, p.element, false, func(c *StreamValueReader) error { return p.element.skip(c, false) }); err != nil {
				return streamFailure("element", err)
			}
		}
	case cqltype.TypeTuple, cqltype.TypeUDT:
		for i, f := range p.fields {
			if r.Remaining() == 0 {
				break
			} // Missing suffix is null in protocol records.
			if err := r.child(f.typ, f, true, func(c *StreamValueReader) error { return f.skip(c, true) }); err != nil {
				return fmt.Errorf("gocql: record field %d: %w", i, err)
			}
		}
	case cqltype.TypeCustom:
		count := int(cqltype.DescriptorOf(p.typ).Dimension)
		if p.width > 0 && r.Remaining() != int64(count)*p.width {
			return r.fail(cqltype.TypeValidation("dimension", "incorrect fixed vector body width"))
		}
		if p.width == 0 && r.Remaining() < int64(count) {
			return r.fail(wire.ErrLimit)
		}
		for i := 0; i < count; i++ {
			size := p.width
			if size == 0 {
				var err error
				size, err = r.vectorLength()
				if err != nil {
					return err
				}
			}
			if err := r.childBody(p.element.typ, cqltype.Present, size, p.element, func(c *StreamValueReader) error { return p.element.skip(c, false) }); err != nil {
				return fmt.Errorf("gocql: vector element %d: %w", i, err)
			}
		}
	default:
		if p.scalar == nil {
			return r.fail(cqltype.TypeValidation("type", "missing scalar validation plan"))
		}
		if err := p.scalar(r); err != nil {
			return err
		}

	}
	return r.RequireEnd()
}
func (r *StreamValueReader) failIf(err error) error {
	if err != nil {
		return r.fail(err)
	}
	return nil
}

type StreamEncodedValue struct {
	Type  cqltype.DataType
	State cqltype.CellState
	Bytes []byte
}
type streamEncodedCodec struct{}
type streamEncodedPlan struct{ validation *streamValidationPlan }

var StreamEncoded StreamDecoder[StreamEncodedValue] = streamEncodedCodec{}

func (streamEncodedCodec) Compile(t cqltype.DataType) (StreamValuePlan[StreamEncodedValue], error) {
	p, e := compileStreamValidation(t, 0)
	if e != nil {
		return nil, e
	}
	return streamEncodedPlan{p}, nil
}
func (streamEncodedPlan) Reset(dst *StreamEncodedValue) {
	if dst != nil {
		dst.Type = cqltype.DataType{}
		dst.State = cqltype.Present
		dst.Bytes = dst.Bytes[:0]
	}
}
func (p streamEncodedPlan) DecodeInto(r *StreamValueReader, dst *StreamEncodedValue) error {
	if dst == nil {
		return r.fail(cqltype.TypeValidation("destination", "nil encoded destination"))
	}
	if err := r.check(); err != nil {
		return err
	}
	if !r.matches(p.validation.typ) {
		return r.fail(cqltype.TypeValidation("type", "encoded metadata changed"))
	}
	if err := r.capacity(r.Remaining(), cap(dst.Bytes)); err != nil {
		return err
	}
	dst.Bytes = dst.Bytes[:int(r.Remaining())]
	previous := r.capture
	r.captureState = streamCapture{dst: dst.Bytes}
	r.capture = &r.captureState
	defer func() { r.capture = previous; r.captureState = streamCapture{} }()
	if err := p.validation.skip(r, true); err != nil {
		return err
	}
	dst.Type, dst.State = r.typ, r.state
	if r.state == cqltype.Present && len(dst.Bytes) == 0 && emptySupported(r.typ) {
		dst.State = cqltype.Empty
	}
	return nil
}

func (streamEncodedCodec) Reset(dst *StreamEncodedValue) {
	if dst != nil {
		dst.Type = cqltype.DataType{}
		dst.State = cqltype.Present
		dst.Bytes = dst.Bytes[:0]
	}
}

func streamScalarValidation[T any](typ cqltype.DataType, codec StreamDecoder[T]) (func(*StreamValueReader) error, error) {
	plan, err := codec.Compile(typ)
	if err != nil {
		return nil, err
	}
	native, ok := plan.(streamScalarPlan[T])
	if !ok {
		return nil, cqltype.TypeValidation("stream scalar", "requires native scalar validation")
	}
	return func(r *StreamValueReader) error { var value T; return native.DecodeInto(r, &value) }, nil
}
func compileStreamScalarValidation(t cqltype.DataType) (func(*StreamValueReader) error, error) {
	switch t.Kind() {
	case cqltype.TypeBoolean:
		return streamScalarValidation(t, StreamBool)
	case cqltype.TypeTinyInt:
		return streamScalarValidation(t, StreamInt8)
	case cqltype.TypeSmallInt:
		return streamScalarValidation(t, StreamInt16)
	case cqltype.TypeInt:
		return streamScalarValidation(t, StreamInt32)
	case cqltype.TypeBigInt:
		return streamScalarValidation(t, StreamInt64)
	case cqltype.TypeFloat:
		return streamScalarValidation(t, StreamFloat32)
	case cqltype.TypeDouble:
		return streamScalarValidation(t, StreamFloat64)
	case cqltype.TypeCounter:
		return streamScalarValidation(t, StreamCounter)
	case cqltype.TypeDate:
		return streamScalarValidation(t, StreamDate)
	case cqltype.TypeTime:
		return streamScalarValidation(t, StreamTime)
	case cqltype.TypeTimestamp:
		return streamScalarValidation(t, StreamTimestamp)
	case cqltype.TypeDuration:
		return streamScalarValidation(t, StreamDuration)
	case cqltype.TypeUUID:
		return streamScalarValidation(t, StreamUUID)
	case cqltype.TypeTimeUUID:
		return streamScalarValidation(t, StreamTimeUUID)
	case cqltype.TypeInet:
		return streamScalarValidation(t, StreamInet)
	// Variable-byte types have specialized incremental validation in skip.
	case cqltype.TypeBlob, cqltype.TypeVarint, cqltype.TypeDecimal, cqltype.TypeText, cqltype.TypeVarchar, cqltype.TypeAscii:
		return nil, nil
	default:
		return nil, cqltype.TypeValidation("type", "unsupported scalar validation %s", t)
	}
}
