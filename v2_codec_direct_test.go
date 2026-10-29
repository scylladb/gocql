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
	"errors"
	"testing"

	"github.com/scylladb/gocql/v2/internal/wire"
)

func TestV2RowDecodeIntoUsesCallerFieldsAndResets(t *testing.T) {
	type inner struct {
		Number int32
		Text   Text
	}
	type outer struct {
		Inner   inner
		Unused  int
		Padding [128]int64
	}
	integer, text := foundationScalar(t, TypeInt), foundationScalar(t, TypeText)
	tuple := codecTestMust(TupleType(integer, text))
	columns := NewColumns(ColumnSpec{Name: "record", Type: tuple})
	var dst outer
	checkAddress := true
	innerDecoder := Row(ColumnAt(0, Int32, func(value *inner) *int32 {
		if checkAddress && value != &dst.Inner {
			t.Fatal("record decoded through a complete temporary")
		}
		return &value.Number
	}), ColumnAt(1, TextCodec, func(value *inner) *Text { return &value.Text }))
	decoder := Row(Column("record", TupleDecoder(innerDecoder), func(value *outer) *inner {
		if checkAddress && value != &dst {
			t.Fatal("outer row decoded through a temporary")
		}
		return &value.Inner
	}))
	params := OneParam(TupleEncoder(PairParams(1, OneParam(Int32), OneParam(StringEncoder))))
	body := encodeTestParams(t, columns, params, Pair[int32, string]{First: 7, Second: "borrowed"})
	lease := wire.NewLease(body, nil)
	plan := codecTestMust(decoder.Compile(columns))
	dst.Unused = 5
	dst.Padding[100] = 9
	reader := newRowReader(columns, body, lease)
	if err := plan.DecodeInto(reader, &dst); err != nil {
		t.Fatal(err)
	}
	if err := finishRowReader(reader); err != nil {
		t.Fatal(err)
	}
	if dst.Inner.Number != 7 || dst.Inner.Text.String() != "borrowed" || dst.Unused != 0 || dst.Padding[100] != 0 {
		t.Fatal("destination state", dst)
	}
	checkAddress = false
	cloned := decoder.Clone(dst)
	lease.Release()
	if cloned.Inner.Text.String() != "borrowed" {
		t.Fatal("schema independent record Clone")
	}
	var validation *ValidationError
	if err := plan.DecodeInto(newRowReader(columns, body, nil), nil); !errors.As(err, &validation) {
		t.Fatal("nil destination", err)
	}
}

type directTestRowDecoder struct {
	address       *int32
	decodes, into *int
}
type directTestRowPlan struct{ directTestRowDecoder }

func (d directTestRowDecoder) Compile(Columns) (RowPlan[int32], error) {
	return directTestRowPlan{d}, nil
}
func (d directTestRowDecoder) Clone(v int32) int32 { return v }
func (p directTestRowPlan) Decode(r *RowReader) (int32, error) {
	*p.decodes++
	return ReadColumn(r, Int32)
}
func (p directTestRowPlan) DecodeInto(r *RowReader, dst *int32) error {
	*p.into++
	if dst != p.address {
		return errors.New("destination address changed")
	}
	return readColumnInto(r, Int32, dst)
}
func (p directTestRowPlan) Clone(v int32) int32 { return v }
func TestV2RowDecodeIntoCompositionPropagatesDestinations(t *testing.T) {
	integer := foundationScalar(t, TypeInt)
	cols := NewColumns(ColumnSpec{Name: "a", Type: integer}, ColumnSpec{Name: "b", Type: integer})
	body := encodeTestParams(t, cols, PairParams(1, OneParam(Int32), OneParam(Int32)), Pair[int32, int32]{1, 2})
	var dst Pair[int32, int32]
	decodes, into := 0, 0
	left := directTestRowDecoder{&dst.First, &decodes, &into}
	right := directTestRowDecoder{&dst.Second, &decodes, &into}
	plan := codecTestMust(PairRows(1, left, right).Compile(cols))
	if err := plan.DecodeInto(newRowReader(cols, body, nil), &dst); err != nil || dst.First != 1 || dst.Second != 2 || decodes != 0 || into != 2 {
		t.Fatal("pair destination", dst, decodes, into, err)
	}
	applied := foundationScalar(t, TypeBoolean)
	casCols := NewColumns(ColumnSpec{Name: "[applied]", Type: applied}, ColumnSpec{Name: "a", Type: integer})
	casBody := encodeTestParams(t, casCols, PairParams(1, OneParam(Bool), OneParam(Int32)), Pair[bool, int32]{false, 7})
	var cas CASResult[int32]
	custom := directTestRowDecoder{&cas.Previous.Value, &decodes, &into}
	casPlan := codecTestMust(CASRows(custom).Compile(casCols))
	if err := casPlan.DecodeInto(newRowReader(casCols, casBody, nil), &cas); err != nil || !cas.Previous.Set || cas.Previous.Value != 7 || decodes != 0 {
		t.Fatal("CAS destination", cas, err)
	}
}
func TestV2CollectDetachesEveryNestedFamily(t *testing.T) {
	text, blob := foundationScalar(t, TypeText), foundationScalar(t, TypeBlob)
	tuple := codecTestMust(TupleType(text, blob))
	list := codecTestMust(ListType(tuple))
	encoder := ListEncoder(TupleEncoder(PairParams(1, OneParam(StringEncoder), OneParam(BytesEncoder))))
	encoded := codecTestMust(EncodeValueBorrowed(list, encoder, []Pair[string, []byte]{{"one", []byte("blob")}, {"two", []byte("more")}}))
	lease := wire.NewLease(encoded.body.chain, nil)
	borrowed := EncodedValue{typ: list, state: Present, body: Bytes{chain: encoded.body.chain, lease: lease}}
	decoder := ListDecoder(TupleDecoder(PairRows(1, OneColumn(TextCodec), OneColumn(BlobCodec))))
	seq := codecTestMust(DecodeValue(decoder, borrowed))
	values := codecTestMust(Collect(seq.Iterator()))
	dynamic := codecTestMust(DynamicCodec.Decode(newValueReader(list, Present, encoded.body.chain, lease)))
	dynamicValues := codecTestMust(Collect(codecTestMust(dynamic.Elements())))
	lease.Release()
	if values[0].First.String() != "one" || !bytes.Equal(values[1].Second.OwnedBytes(), []byte("more")) {
		t.Fatal("typed Collect retained lease")
	}
	fields := codecTestMust(Collect(codecTestMust(dynamicValues[0].Fields())))
	txt := codecTestMust(fields[0].Value.Text())
	if txt.String() != "one" {
		t.Fatal("dynamic Collect retained lease")
	}
	// Input-side dynamic values borrow caller arrays too; Collect detaches them.
	payload := []byte("input")
	value := codecTestMust(ListValue(codecTestMust(ListType(blob)), []Value{BlobValue(payload)}))
	input := codecTestMust(Collect(codecTestMust(value.Elements())))
	payload[0] = 'X'
	b := codecTestMust(input[0].Bytes())
	if string(b.OwnedBytes()) != "input" {
		t.Fatal("input-side Collect did not detach")
	}
}
func TestV2UDTIdentityCheckedBeforeNullEncoding(t *testing.T) {
	typ := codecTestMust(UDTType("ks", "record", ColumnSpec{Name: "v", Type: foundationScalar(t, TypeInt)}))
	params := Params(Param("v", Int32, func(v int32) int32 { return v }))
	rows := Row(Column("v", Int32, func(v *int32) *int32 { return v }))
	for _, options := range []UDTOptions{{}, {Keyspace: "other", Name: "record"}, {Keyspace: "ks", Name: "wrong"}} {
		if _, err := EncodeValueBorrowed(typ, NullableEncoder(UDTEncoder(params, options)), Nullable[int32]{}); err == nil {
			t.Fatal("invalid UDT identity hidden by null")
		}
		if _, err := compileValueDecoder(NullableDecoder(UDTDecoder(rows, options)), typ); err == nil {
			t.Fatal("invalid UDT identity hidden by nullable decoder")
		}
	}
}
