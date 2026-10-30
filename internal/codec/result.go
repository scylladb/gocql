// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package codec

import (
	cqltype "github.com/scylladb/gocql/v2/internal/cqltype"
)

type CASResult[R any] struct {
	Applied  bool
	Previous cqltype.Option[R]
}
type casRows[R any] struct{ decoder RowDecoder[R] }
type casRowPlan[R any] struct {
	previous RowPlan[R]
	columns  cqltype.Columns
}

func CASRows[R any](decoder RowDecoder[R]) RowDecoder[CASResult[R]] {
	return casRows[R]{decoder: decoder}
}
func (c casRows[R]) Compile(columns cqltype.Columns) (RowPlan[CASResult[R]], error) {
	first, ok := columns.At(0)
	if !ok || first.Name != "[applied]" {
		return nil, cqltype.TypeValidation("CAS result", "first column must be [applied]")
	}
	if err := Bool.CheckType(first.Type); err != nil {
		return nil, err
	}
	_, previous, err := splitColumns(columns, 1)
	if err != nil {
		return nil, err
	}
	p := casRowPlan[R]{columns: previous}
	if previous.Len() > 0 {
		if c.decoder == nil {
			return nil, cqltype.TypeValidation("CAS result", "nil previous-value decoder")
		}
		p.previous, err = CompileRowPlan(c.decoder, previous)
		if err != nil {
			return nil, err
		}
	}
	return p, nil
}
func (p casRowPlan[R]) Decode(reader *RowReader) (CASResult[R], error) {
	var result CASResult[R]
	err := p.DecodeInto(reader, &result)
	if err != nil {
		return CASResult[R]{}, err
	}
	return result, nil
}
func (p casRowPlan[R]) DecodeInto(reader *RowReader, dst *CASResult[R]) error {
	if dst == nil {
		return cqltype.TypeValidation("destination", "nil CAS destination")
	}
	*dst = CASResult[R]{}
	if err := readColumnInto(reader, Bool, &dst.Applied); err != nil {
		return err
	}
	if p.previous == nil {
		return nil
	}
	columns, index := reader.columns, reader.index
	reader.columns, reader.index = p.columns, 0
	err := p.previous.DecodeInto(reader, &dst.Previous.Value)
	if err == nil {
		err = FinishRowReader(reader)
	}
	reader.columns, reader.index = columns, index
	if err != nil {
		return err
	}
	reader.index += p.columns.Len()
	dst.Previous.Set = true
	return nil
}
func (p casRowPlan[R]) Clone(value CASResult[R]) CASResult[R] {
	if value.Previous.Set {
		value.Previous.Value = p.previous.Clone(value.Previous.Value)
	}
	return value
}

func (c casRows[R]) Clone(value CASResult[R]) CASResult[R] {
	if value.Previous.Set {
		value.Previous.Value = c.decoder.Clone(value.Previous.Value)
	}
	return value
}
