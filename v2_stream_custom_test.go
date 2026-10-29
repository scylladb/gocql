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
	"bytes"
	"context"
	"errors"
	"testing"
)

type customStreamPair struct {
	A int32
	B []byte
}
type customStreamDecoder struct{ checks *int }
type customStreamPlan struct {
	integer           StreamValuePlan[int32]
	blob              StreamValuePlan[[]byte]
	intType, blobType DataType
}

func (c customStreamDecoder) Compile(t DataType) (StreamValuePlan[customStreamPair], error) {
	*c.checks++
	fields := t.Fields()
	if fields.Len() != 2 {
		return nil, errors.New("expected two fields")
	}
	a, _ := fields.At(0)
	b, _ := fields.At(1)
	integer, err := StreamInt32.Compile(a.Type)
	if err != nil {
		return nil, err
	}
	blob, err := StreamBlob.Compile(b.Type)
	if err != nil {
		return nil, err
	}
	return customStreamPlan{integer, blob, a.Type, b.Type}, nil
}
func (p customStreamPlan) Reset(dst *customStreamPair) { p.integer.Reset(&dst.A); p.blob.Reset(&dst.B) }
func (p customStreamPlan) DecodeInto(r *StreamValueReader, dst *customStreamPair) error {
	if r.Context() == nil {
		return errors.New("missing context")
	}
	if err := r.Child(p.intType, func(child *StreamValueReader) error { return p.integer.DecodeInto(child, &dst.A) }); err != nil {
		return err
	}
	return r.Child(p.blobType, func(child *StreamValueReader) error { return p.blob.DecodeInto(child, &dst.B) })
}
func TestV2StreamingCustomPlansReuseCompiledChildren(t *testing.T) {
	integer, blob := foundationScalar(t, TypeInt), foundationScalar(t, TypeBlob)
	tuple := codecTestMust(TupleType(integer, blob))
	cols := NewColumns(ColumnSpec{Name: "v", Type: tuple})
	checks := 0
	plan := codecTestMust(StreamOneColumn(customStreamDecoder{&checks}).Compile(cols))
	data := streamEncodedFixture(t, tuple, TupleEncoder(PairParams(1, OneParam(Int32), OneParam(BytesEncoder))), Pair[int32, []byte]{7, []byte("payload")})
	combined := bytes.Repeat(data, 100)
	reader, scope := streamReaderFixture(t, cols, bytes.NewReader(combined), len(combined))
	dst := customStreamPair{B: make([]byte, 0, 32)}
	for i := 0; i < 100; i++ {
		reader.index = 0
		plan.Reset(&dst)
		if err := plan.DecodeInto(reader, &dst); err != nil || dst.A != 7 || string(dst.B) != "payload" {
			t.Fatal("custom stream", err)
		}
	}
	if checks != 1 || scope.Remaining() != 0 {
		t.Fatal("custom plan recompiled", checks)
	}
}

type nilStreamValue struct{}

func (nilStreamValue) Compile(DataType) (StreamValuePlan[int32], error) { return nil, nil }

type nilStreamRows struct{}

func (nilStreamRows) Compile(Columns) (StreamRowPlan[int32], error) { return nil, nil }

type nilBufferedRows struct{}

func (nilBufferedRows) Compile(Columns) (RowPlan[int32], error) { return nil, nil }
func (nilBufferedRows) Clone(v int32) int32                     { return v }

type nilBufferedParams struct{}

func (nilBufferedParams) Compile(Columns) (ParamPlan[int32], error) { return nil, nil }
func TestV2CodecNilCompiledPlansAreValidationErrors(t *testing.T) {
	integer := foundationScalar(t, TypeInt)
	cols := NewColumns(ColumnSpec{Name: "v", Type: integer})
	tuple := codecTestMust(TupleType(integer))
	list := codecTestMust(ListType(integer))
	m := codecTestMust(MapType(integer, integer))
	cases := []func() error{
		func() error { _, e := StreamOneColumn(nilStreamValue{}).Compile(cols); return e },
		func() error {
			_, e := StreamRow(func(*int32) {}, StreamColumn("v", nilStreamValue{}, func(p *int32) *int32 { return p })).Compile(cols)
			return e
		},
		func() error { _, e := StreamList(nilStreamValue{}).Compile(list); return e },
		func() error { _, e := StreamMap(nilStreamValue{}, StreamInt32).Compile(m); return e },
		func() error { _, e := StreamNullable(nilStreamValue{}).Compile(integer); return e },
		func() error { _, e := StreamEmpty(nilStreamValue{}).Compile(integer); return e },
		func() error { _, e := StreamTuple(nilStreamRows{}).Compile(tuple); return e },
		func() error { return TupleDecoder(nilBufferedRows{}).CheckType(tuple) },
		func() error { return TupleEncoder(nilBufferedParams{}).CheckType(tuple) },
	}
	for i, test := range cases {
		var validation *ValidationError
		if err := test(); !errors.As(err, &validation) {
			t.Fatalf("case %d: %v", i, err)
		}
	}
}
func TestV2StreamingReaderContextAndTerminalErrors(t *testing.T) {
	_ = context.Canceled
	integer := foundationScalar(t, TypeInt)
	cols := NewColumns(ColumnSpec{Name: "v", Type: integer})
	data := streamCell([]byte{0, 0, 0, 1})
	reader, _ := streamReaderFixture(t, cols, bytes.NewReader(data), len(data))
	plan := codecTestMust(StreamOneColumn(StreamInt32).Compile(cols))
	var dst int32
	if err := plan.DecodeInto(reader, &dst); err != nil {
		t.Fatal(err)
	}
	if err := plan.DecodeInto(reader, &dst); err == nil {
		t.Fatal("second row read without resetting cursor")
	}
}

func TestV2SharedTypeDAGCompilationAndEquality(t *testing.T) {
	left, right := foundationScalar(t, TypeInt), foundationScalar(t, TypeInt)
	for i := 0; i < 64; i++ {
		left = codecTestMust(TupleType(left, left))
		right = codecTestMust(TupleType(right, right))
	}
	if !left.Equal(right) {
		t.Fatal("equivalent shared descriptors differ")
	}
	if err := DynamicCodec.CheckType(left); err != nil {
		t.Fatal(err)
	}
	plan := codecTestMust(compileStreamValidation(left, 0))
	nodes := 0
	for len(plan.fields) > 0 {
		if len(plan.fields) != 2 || plan.fields[0] != plan.fields[1] {
			t.Fatal("shared child metadata expanded into duplicate plans")
		}
		nodes++
		plan = plan.fields[0]
	}
	if nodes != 64 {
		t.Fatal("unexpected graph height", nodes)
	}
	for i := 64; i < 130; i++ {
		left = codecTestMust(TupleType(left, left))
	}
	if _, err := compileStreamValidation(left, 0); err == nil {
		t.Fatal("excessive descriptor depth accepted")
	}
	if err := DynamicCodec.CheckType(left); err == nil {
		t.Fatal("excessive dynamic descriptor depth accepted")
	}
}

type customStreamNoReset struct{}

func (customStreamNoReset) Compile(t DataType) (StreamValuePlan[int32], error) {
	return StreamInt32.Compile(t)
}
func TestV2StreamingMissingPayloadResetPreservesSeeds(t *testing.T) {
	type destination struct {
		ID      int32
		Payload []byte
		Nested  [][]byte
		Custom  int32
	}
	integer := foundationScalar(t, TypeInt)
	udt := codecTestMust(UDTType("ks", "one", ColumnSpec{Name: "id", Type: integer}))
	options := UDTOptions{Keyspace: "ks", Name: "one", AllowMissingFields: true}
	row := StreamRow(func(*destination) {}, StreamColumn("id", StreamInt32, func(d *destination) *int32 { return &d.ID }), StreamColumn("payload", StreamBlob, func(d *destination) *[]byte { return &d.Payload }), StreamColumn("nested", StreamList(StreamBlob), func(d *destination) *[][]byte { return &d.Nested }))
	backing, child := make([]byte, 16), make([]byte, 32)
	out := destination{Payload: backing[:10], Nested: [][]byte{child[:12]}}
	nestedStart := &out.Nested[0]
	data := streamCell(streamCell([]byte{0, 0, 0, 7}))
	for i := 0; i < 2; i++ {
		if err := streamDecodeFixture(t, udt, StreamUDT(options, row), data, &out, 1); err != nil || out.ID != 7 || len(out.Payload) != 0 || cap(out.Payload) != 16 || len(out.Nested) != 0 || cap(out.Nested) != 1 {
			t.Fatal("missing payload reset", out, err)
		}
		out.Payload = out.Payload[:1]
		out.Nested = out.Nested[:1]
		if &out.Payload[0] != &backing[0] || &out.Nested[0] != nestedStart || cap(out.Nested[0]) != 32 {
			t.Fatal("reset lost seeded storage")
		}
	}
	absent := StreamRow(func(*destination) {}, StreamColumn("id", StreamInt32, func(d *destination) *int32 { return &d.ID }), StreamColumn("custom", customStreamNoReset{}, func(d *destination) *int32 { return &d.Custom }))
	if _, err := StreamUDT(options, absent).Compile(udt); err == nil {
		t.Fatal("custom absent mapping silently reset without declaration capability")
	}
	wrapped := StreamRow(func(*Nullable[int32]) {}, StreamColumn("missing", StreamNullable(customStreamNoReset{}), func(d *Nullable[int32]) *Nullable[int32] { return d }))
	if _, err := StreamUDT(UDTOptions{Keyspace: "ks", Name: "one", AllowMissingFields: true, AllowExtraDatabaseFields: true}, wrapped).Compile(udt); err == nil {
		t.Fatal("wrapper advertised unsupported custom reset")
	}
}

func TestV2StreamingAbsentCustomDescendantResetCapability(t *testing.T) {
	integer := foundationScalar(t, TypeInt)
	udt := codecTestMust(UDTType("ks", "one", ColumnSpec{Name: "id", Type: integer}))
	options := UDTOptions{Keyspace: "ks", Name: "one", AllowMissingFields: true, AllowExtraDatabaseFields: true}
	absentList := StreamRow(func(*[]int32) {}, StreamColumn("missing", StreamList(customStreamNoReset{}), func(v *[]int32) *[]int32 { return v }))
	if _, err := StreamUDT(options, absentList).Compile(udt); err == nil {
		t.Fatal("absent list masked custom declaration reset capability")
	}
}

type customStreamResettable struct{ customStreamNoReset }

func (customStreamResettable) Reset(dst *int32) { *dst = 0 }
func TestV2StreamingAbsentCustomResetterSucceeds(t *testing.T) {
	integer := foundationScalar(t, TypeInt)
	udt := codecTestMust(UDTType("ks", "one", ColumnSpec{Name: "id", Type: integer}))
	options := UDTOptions{Keyspace: "ks", Name: "one", AllowMissingFields: true, AllowExtraDatabaseFields: true}
	row := StreamRow(func(*int32) {}, StreamColumn("missing", customStreamResettable{}, func(value *int32) *int32 { return value }))
	value := int32(42)
	if err := streamDecodeFixture(t, udt, StreamUDT(options, row), streamCell(streamCell([]byte{0, 0, 0, 7})), &value, 1); err != nil || value != 0 {
		t.Fatal("custom declaration reset", value, err)
	}
}
