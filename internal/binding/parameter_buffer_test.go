// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package binding

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/scylladb/gocql/v2/internal/codec"
	"github.com/scylladb/gocql/v2/internal/cqltype"
)

func TestOwnedParameterCaptureMatchesCheckedGenericEncoding(t *testing.T) {
	typ, err := cqltype.ScalarType(cqltype.TypeBlob)
	if err != nil {
		t.Fatal(err)
	}
	columns := cqltype.NewColumns(cqltype.ColumnSpec{Name: "value", Type: typ})
	plan, err := codec.OneParam(codec.BindEncoder(codec.BytesEncoder)).Compile(columns)
	if err != nil {
		t.Fatal(err)
	}
	var storage ParameterBuffer
	for _, state := range []cqltype.CellState{cqltype.Present, cqltype.Null, cqltype.Unset} {
		for _, size := range []int{0, 1, 257, 128 << 10} {
			input := cqltype.BindValue[[]byte]{State: state, Value: bytes.Repeat([]byte{41}, size)}
			got, err := EncodeInto(&storage, plan, columns, input, true)
			if err != nil {
				t.Fatal(err)
			}
			want, err := InternalEncodeParameters(plan, columns, input, true, true)
			if err != nil {
				t.Fatal(err)
			}
			g, w := make([]byte, got.Framed.Len()), make([]byte, want.Framed.Len())
			_, _ = got.Framed.CopyTo(g)
			_, _ = want.Framed.CopyTo(w)
			if !bytes.Equal(g, w) || got.Cells[0].State != want.Cells[0].State {
				t.Fatal(state, size, "encoding differs")
			}
			if size > 0 {
				input.Value[0] = 99
			}
			owned := make([]byte, got.Framed.Len())
			_, _ = got.Framed.CopyTo(owned)
			if !bytes.Equal(g, owned) {
				t.Fatal("captured input changed")
			}
			storage.Retire()
			if storage.RetainedBytes() > 64<<10 {
				t.Fatal("idle buffer exceeds limit")
			}
		}
	}
}

func TestOwnedEmptyCaptureHasPresentWireState(t *testing.T) {
	typ, err := cqltype.ScalarType(cqltype.TypeInt)
	if err != nil {
		t.Fatal(err)
	}
	columns := cqltype.NewColumns(cqltype.ColumnSpec{Name: "value", Type: typ})
	plan, err := codec.OneParam(codec.EmptyCodec(codec.Int32)).Compile(columns)
	if err != nil {
		t.Fatal(err)
	}
	var storage ParameterBuffer
	value := cqltype.EmptyValueOf[int32]{Empty: true}
	got, err := EncodeInto(&storage, plan, columns, value, true)
	if err != nil {
		t.Fatal(err)
	}
	want, err := InternalEncodeParameters(plan, columns, value, true, true)
	if err != nil {
		t.Fatal(err)
	}
	if got.Cells[0].State != want.Cells[0].State || got.Cells[0].State != cqltype.Present {
		t.Fatal(got.Cells[0].State, want.Cells[0].State)
	}
}

func TestOwnedParameterRetirementBoundsDescriptorStorage(t *testing.T) {
	typ, err := cqltype.ScalarType(cqltype.TypeInt)
	if err != nil {
		t.Fatal(err)
	}
	columns := make([]cqltype.ColumnSpec, 2048)
	fields := make([]codec.InputField[codec.Unit], len(columns))
	for i := range columns {
		columns[i] = cqltype.ColumnSpec{Name: fmt.Sprint(i), Type: typ}
		fields[i] = codec.ParamAt(i, codec.Int32, func(codec.Unit) int32 { return 42 })
	}
	metadata := cqltype.NewColumns(columns...)
	plan, err := codec.Params(fields...).Compile(metadata)
	if err != nil {
		t.Fatal(err)
	}
	var storage ParameterBuffer
	values, err := EncodeInto(&storage, plan, metadata, codec.Unit{}, true)
	if err != nil {
		t.Fatal(err)
	}
	if len(values.Cells) != len(columns) {
		t.Fatal("capture count", len(values.Cells))
	}
	storage.Retire()
	if storage.RetainedBytes() > 64<<10 {
		t.Fatal("idle request descriptors exceed budget", storage.RetainedBytes())
	}
}
