// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package binding

import (
	"github.com/scylladb/gocql/v2/internal/codec"
	"github.com/scylladb/gocql/v2/internal/model"
	"github.com/scylladb/gocql/v2/internal/wire"
	"unsafe"
)

// ParameterBuffer belongs to one query. Encode must not overlap an execution
// still using its previous snapshot. No caller-owned input survives Encode.
type ParameterBuffer struct {
	builder   wire.Builder
	storage   []byte
	positions []codec.ParamCell
	cells     []model.EncodedCell
}

func (b *ParameterBuffer) RetainedBytes() int {
	return cap(b.storage) + cap(b.cells)*int(unsafe.Sizeof(model.EncodedCell{})) + cap(b.positions)*int(unsafe.Sizeof(codec.ParamCell{}))
}

func EncodeInto[P any](b *ParameterBuffer, plan ParamPlan[P], columns Columns, value P, allowUnset bool) (encodedValues, error) {
	if columns.Len() > 65535 {
		return encodedValues{}, typeValidation("parameters", "maximum count is 65535")
	}
	n := columns.Len()
	if cap(b.positions) < n {
		b.positions = make([]codec.ParamCell, n)
	} else {
		b.positions = b.positions[:n]
	}
	if cap(b.cells) < n {
		b.cells = make([]model.EncodedCell, n)
	} else {
		b.cells = b.cells[:n]
	}
	b.builder.BeginOwned(b.storage)
	writer := newParamWriter(&b.builder, columns, allowUnset)
	codec.CaptureParamCells(writer, b.positions)
	// The owned builder copies every borrowed span, including custom encoders.
	err := plan.Encode(writer, value)
	if err == nil {
		err = finishParamWriter(writer)
	}
	b.storage = b.builder.OwnedBytes()
	framed, freezeErr := b.builder.Freeze()
	if err != nil {
		return encodedValues{}, err
	}
	if freezeErr != nil {
		return encodedValues{}, freezeErr
	}
	for i, position := range b.positions {
		column, _ := columns.At(i)
		body, err := framed.Slice(position.Start, position.End-position.Start)
		if err != nil {
			return encodedValues{}, err
		}
		b.cells[i] = model.EncodedCell{Typ: column.Type, State: position.State, Body: body}
	}
	return encodedValues{Columns: columns, Cells: b.cells, Framed: framed}, nil
}

// Retire drops large buffers and all views only after transport and callbacks
// have drained. Idle queries retain at most 64 KiB of request backing.
func (b *ParameterBuffer) Retire() {
	clear(b.cells)
	clear(b.positions)
	if cap(b.storage) > 64<<10 {
		b.storage = nil
	} else {
		b.storage = b.storage[:0]
	}
	if b.RetainedBytes() > 64<<10 {
		b.cells = nil
		b.positions = nil
	}
}
