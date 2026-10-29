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

import "fmt"

// StreamOutputField is an immutable typed field declaration. It never boxes a
// field value and never retains the application destination supplied later.
type StreamOutputField[R any] struct {
	loc             fieldLocation
	compile         func(DataType) (streamFieldPlan[R], error)
	resetMissing    func(*R)
	canResetMissing func() bool
}
type streamFieldPlan[R any] struct {
	reset  func(*R)
	decode func(*StreamRowReader, *R) error
}

func StreamColumn[R, V any](name string, decoder StreamDecoder[V], field func(*R) *V) StreamOutputField[R] {
	return makeStreamColumn(fieldLocation{name: name}, decoder, field)
}
func StreamColumnAt[R, V any](index int, decoder StreamDecoder[V], field func(*R) *V) StreamOutputField[R] {
	return makeStreamColumn(fieldLocation{index: index, positional: true}, decoder, field)
}
func makeStreamColumn[R, V any](loc fieldLocation, decoder StreamDecoder[V], field func(*R) *V) StreamOutputField[R] {
	return StreamOutputField[R]{loc: loc, canResetMissing: func() bool { return field != nil && streamDeclarationCanReset(decoder) }, resetMissing: func(dst *R) {
		if ptr := field(dst); ptr != nil {
			resetStreamDeclaration(decoder, ptr)
		}
	}, compile: func(t DataType) (streamFieldPlan[R], error) {
		if decoder == nil || field == nil {
			return streamFieldPlan[R]{}, typeValidation("stream field", "nil codec or accessor")
		}
		plan, err := compileStreamValuePlan(decoder, t)
		if err != nil {
			return streamFieldPlan[R]{}, err
		}
		return streamFieldPlan[R]{reset: func(dst *R) {
			if ptr := field(dst); ptr != nil {
				plan.Reset(ptr)
			}
		}, decode: func(reader *StreamRowReader, dst *R) error {
			ptr := field(dst)
			if ptr == nil {
				return typeValidation("stream field", "accessor returned nil")
			}
			return ReadStreamColumn(reader, plan, ptr)
		}}, nil
	}}
}

type streamRowsCodec[R any] struct {
	reset  func(*R)
	fields []StreamOutputField[R]
}
type streamRowsPlan[R any] struct {
	reset   func(*R)
	resets  []func(*R)
	ordered []streamFieldPlan[R]
	columns Columns
}

func StreamRow[R any](reset func(*R), fields ...StreamOutputField[R]) StreamRowDecoder[R] {
	return streamRowsCodec[R]{reset: reset, fields: append([]StreamOutputField[R](nil), fields...)}
}
func (c streamRowsCodec[R]) Compile(columns Columns) (StreamRowPlan[R], error) {
	return c.compile(columns, false, UDTOptions{})
}
func (c streamRowsCodec[R]) compile(columns Columns, strict bool, options UDTOptions) (StreamRowPlan[R], error) {
	if c.reset == nil {
		return nil, typeValidation("stream row", "nil application reset callback")
	}
	if err := checkDynamicColumns(columns); err != nil {
		return nil, err
	}
	locations := make([]fieldLocation, len(c.fields))
	for i, f := range c.fields {
		if f.compile == nil || f.resetMissing == nil || f.canResetMissing == nil {
			return nil, typeValidation("stream field", "uninitialized declaration")
		}
		locations[i] = f.loc
	}
	// Generic result rows skip unspecified physical columns. Records use the same
	// exact/missing/extra mapping policy as buffered records.
	mapping, err := streamFieldMapping(columns, locations, !strict || options.AllowExtraDatabaseFields, strict && options.AllowMissingFields)
	if err != nil {
		return nil, err
	}
	plan := streamRowsPlan[R]{reset: c.reset, ordered: make([]streamFieldPlan[R], columns.Len()), columns: columns, resets: make([]func(*R), len(c.fields))}
	matched := make([]bool, len(c.fields))
	for _, field := range mapping {
		if field >= 0 {
			matched[field] = true
		}
	}
	for i, f := range c.fields {
		if !matched[i] && !f.canResetMissing() {
			return nil, typeValidation("stream reset", "absent application field %q needs metadata-independent StreamResetter", f.loc.name)
		}
		plan.resets[i] = f.resetMissing
	}
	for index, field := range mapping {
		if field < 0 {
			plan.ordered[index] = streamFieldPlan[R]{decode: func(r *StreamRowReader, _ *R) error { return r.SkipColumn() }}
			continue
		}
		compiled, err := c.fields[field].compile(columns.columns[index].Type)
		if err != nil {
			return nil, fmt.Errorf("gocql: streaming column %d (%s): %w", index, columns.columns[index].Name, err)
		}
		plan.ordered[index] = compiled
		plan.resets[field] = compiled.reset
	}
	return plan, nil
}
func (p streamRowsPlan[R]) Reset(dst *R) {
	if dst != nil {
		p.reset(dst)
		for _, reset := range p.resets {
			reset(dst)
		}
	}
}
func (p streamRowsPlan[R]) DecodeInto(r *StreamRowReader, dst *R) error {
	if dst == nil {
		return typeValidation("destination", "nil stream row destination")
	}
	if err := r.check(); err != nil {
		return err
	}
	if r.index != 0 || !r.matches(p.columns) {
		return r.shared.fail(typeValidation("stream metadata", "row shape differs from compiled plan"))
	}
	for _, field := range p.ordered {
		if err := field.decode(r, dst); err != nil {
			return r.shared.fail(err)
		}
	}
	return r.RequireEnd()
}

type streamOneColumn[T any] struct{ decoder StreamDecoder[T] }
type streamOneColumnPlan[T any] struct {
	plan    StreamValuePlan[T]
	columns Columns
}

func StreamOneColumn[T any](decoder StreamDecoder[T]) StreamRowDecoder[T] {
	return streamOneColumn[T]{decoder}
}
func (c streamOneColumn[T]) Compile(columns Columns) (StreamRowPlan[T], error) {
	if columns.Len() != 1 {
		return nil, typeValidation("stream row", "expected one column, got %d", columns.Len())
	}
	if c.decoder == nil {
		return nil, typeValidation("stream codec", "nil decoder")
	}
	p, e := compileStreamValuePlan(c.decoder, columns.columns[0].Type)
	if e != nil {
		return nil, e
	}
	return streamOneColumnPlan[T]{p, columns}, nil
}
func (p streamOneColumnPlan[T]) Reset(dst *T) {
	if dst != nil {
		p.plan.Reset(dst)
	}
}
func (p streamOneColumnPlan[T]) DecodeInto(r *StreamRowReader, dst *T) error {
	if dst == nil {
		return typeValidation("destination", "nil stream row destination")
	}
	if err := r.check(); err != nil {
		return err
	}
	if r.index != 0 || !r.matches(p.columns) {
		return r.shared.fail(typeValidation("stream metadata", "single column shape changed"))
	}
	if err := ReadStreamColumn(r, p.plan, dst); err != nil {
		return err
	}
	return r.RequireEnd()
}

type streamRecordCodec[R any] struct {
	kind    Type
	options UDTOptions
	decoder StreamRowDecoder[R]
}
type streamRecordPlan[R any] struct {
	typ         DataType
	columns     Columns
	plan        StreamRowPlan[R]
	validations []*streamValidationPlan
}

func StreamTuple[R any](decoder StreamRowDecoder[R]) StreamDecoder[R] {
	return streamRecordCodec[R]{kind: TypeTuple, decoder: decoder}
}
func StreamUDT[R any](options UDTOptions, decoder StreamRowDecoder[R]) StreamDecoder[R] {
	return streamRecordCodec[R]{kind: TypeUDT, options: options, decoder: decoder}
}
func (c streamRecordCodec[R]) Compile(t DataType) (StreamValuePlan[R], error) {
	columns, err := recordColumns(t, c.kind)
	if err != nil {
		return nil, err
	}
	if c.decoder == nil {
		return nil, typeValidation("stream codec", "nil record decoder")
	}
	if c.kind == TypeUDT {
		if err := checkUDTIdentity(t, c.options); err != nil {
			return nil, err
		}
	}
	var plan StreamRowPlan[R]
	if builtin, ok := c.decoder.(streamRowsCodec[R]); ok {
		plan, err = builtin.compile(columns, true, c.options)
	} else if single, ok := c.decoder.(streamOneColumn[R]); ok {
		composed := streamRowsCodec[R]{reset: func(*R) {}, fields: []StreamOutputField[R]{StreamColumnAt(0, single.decoder, func(value *R) *R { return value })}}
		plan, err = composed.compile(columns, true, c.options)
	} else {
		plan, err = compileStreamRowPlan(c.decoder, columns)
	}
	if err != nil {
		return nil, err
	}
	validations := make([]*streamValidationPlan, columns.Len())
	for i, c := range columns.columns {
		validations[i], err = compileStreamValidation(c.Type, 0)
		if err != nil {
			return nil, err
		}
	}
	return streamRecordPlan[R]{typ: t, columns: columns, plan: plan, validations: validations}, nil
}
func (p streamRecordPlan[R]) Reset(dst *R) {
	if dst != nil {
		p.plan.Reset(dst)
	}
}
func (p streamRecordPlan[R]) DecodeInto(r *StreamValueReader, dst *R) error {
	if dst == nil {
		return r.fail(typeValidation("destination", "nil record destination"))
	}
	if err := r.present(); err != nil {
		return err
	}
	if !r.matches(p.typ) {
		return r.fail(typeValidation("type", "compiled record metadata changed"))
	}
	row := r.shared.row(r.depth + 1)
	*row = StreamRowReader{columns: p.columns, scope: r.scope, shared: r.shared, validations: p.validations, depth: r.depth + 1, allowMissing: true, active: true, capture: r.capture}
	defer func() { row.active = false; row.capture = nil }()
	if err := p.plan.DecodeInto(row, dst); err != nil {
		return r.fail(err)
	}
	if err := row.RequireEnd(); err != nil {
		return r.fail(err)
	}
	return r.RequireEnd()
}

func streamFieldMapping(columns Columns, locations []fieldLocation, extra, missing bool) ([]int, error) {
	mapping := make([]int, columns.Len())
	for i := range mapping {
		mapping[i] = -1
	}
	for i, loc := range locations {
		if !loc.positional && loc.name == "" {
			return nil, typeValidation("stream field", "empty field name")
		}
		matches := 0
		for j, col := range columns.columns {
			if !(loc.positional && loc.index == j || !loc.positional && loc.name == col.Name) {
				continue
			}
			if mapping[j] >= 0 {
				return nil, typeValidation("stream field", "duplicate accessor for column %d", j)
			}
			mapping[j] = i
			matches++
		}
		if matches > 1 {
			return nil, typeValidation("stream field", "duplicate result name %q needs positional mapping", loc.name)
		}
		if matches == 0 && !missing {
			return nil, typeValidation("stream field", "application field %q/%d has no column", loc.name, loc.index)
		}
	}
	if !extra {
		for i, m := range mapping {
			if m < 0 {
				return nil, typeValidation("stream field", "unaccounted database field %d", i)
			}
		}
	}
	return mapping, nil
}

func (c streamRowsCodec[R]) Reset(dst *R) {
	if dst == nil {
		return
	}
	c.reset(dst)
	for _, field := range c.fields {
		field.resetMissing(dst)
	}
}
func (c streamRowsCodec[R]) streamCanReset() bool {
	if c.reset == nil {
		return false
	}
	for _, field := range c.fields {
		if field.canResetMissing == nil || !field.canResetMissing() {
			return false
		}
	}
	return true
}
func (c streamOneColumn[T]) Reset(dst *T) {
	if dst != nil {
		resetStreamDeclaration(c.decoder, dst)
	}
}
func (c streamOneColumn[T]) streamCanReset() bool { return streamDeclarationCanReset(c.decoder) }
func (c streamRecordCodec[R]) Reset(dst *R) {
	if dst != nil {
		c.decoder.(StreamResetter[R]).Reset(dst)
	}
}
func (c streamRecordCodec[R]) streamCanReset() bool {
	if c.decoder == nil {
		return false
	}
	if capable, ok := c.decoder.(streamResetCapability); ok && !capable.streamCanReset() {
		return false
	}
	_, ok := c.decoder.(StreamResetter[R])
	return ok
}
