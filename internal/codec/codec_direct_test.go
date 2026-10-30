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
	"bytes"
	"errors"
	"testing"

	cqltype "github.com/scylladb/gocql/v2/internal/cqltype"
	"github.com/scylladb/gocql/v2/internal/wire"
)

func TestV2RowDecodeIntoUsesCallerFieldsAndResets(t *testing.T) {
	type inner struct {
		Number int32
		Text   cqltype.Text
	}
	type outer struct {
		Inner   inner
		Unused  int
		Padding [128]int64
	}
	integer, text := FoundationScalar(t, cqltype.TypeInt), FoundationScalar(t, cqltype.TypeText)
	tuple := CodecTestMust(cqltype.TupleType(integer, text))
	columns := cqltype.NewColumns(cqltype.ColumnSpec{Name: "record", Type: tuple})
	var dst outer
	checkAddress := true
	innerDecoder := Row(ColumnAt(0, Int32, func(value *inner) *int32 {
		if checkAddress && value != &dst.Inner {
			t.Fatal("record decoded through a complete temporary")
		}
		return &value.Number
	}), ColumnAt(1, TextCodec, func(value *inner) *cqltype.Text { return &value.Text }))
	decoder := Row(Column("record", TupleDecoder(innerDecoder), func(value *outer) *inner {
		if checkAddress && value != &dst {
			t.Fatal("outer row decoded through a temporary")
		}
		return &value.Inner
	}))
	params := OneParam(TupleEncoder(PairParams(1, OneParam(Int32), OneParam(StringEncoder))))
	body := encodeTestParams(t, columns, params, Pair[int32, string]{First: 7, Second: "borrowed"})
	lease := wire.NewLease(body, nil)
	plan := CodecTestMust(decoder.Compile(columns))
	dst.Unused = 5
	dst.Padding[100] = 9
	reader := NewRowReader(columns, body, lease)
	if err := plan.DecodeInto(reader, &dst); err != nil {
		t.Fatal(err)
	}
	if err := FinishRowReader(reader); err != nil {
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
	var validation *cqltype.ValidationError
	if err := plan.DecodeInto(NewRowReader(columns, body, nil), nil); !errors.As(err, &validation) {
		t.Fatal("nil destination", err)
	}
}

type directTestRowDecoder struct {
	address       *int32
	decodes, into *int
}
type directTestRowPlan struct{ directTestRowDecoder }

func (d directTestRowDecoder) Compile(cqltype.Columns) (RowPlan[int32], error) {
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
	integer := FoundationScalar(t, cqltype.TypeInt)
	cols := cqltype.NewColumns(cqltype.ColumnSpec{Name: "a", Type: integer}, cqltype.ColumnSpec{Name: "b", Type: integer})
	body := encodeTestParams(t, cols, PairParams(1, OneParam(Int32), OneParam(Int32)), Pair[int32, int32]{1, 2})
	var dst Pair[int32, int32]
	decodes, into := 0, 0
	left := directTestRowDecoder{&dst.First, &decodes, &into}
	right := directTestRowDecoder{&dst.Second, &decodes, &into}
	plan := CodecTestMust(PairRows(1, left, right).Compile(cols))
	if err := plan.DecodeInto(NewRowReader(cols, body, nil), &dst); err != nil || dst.First != 1 || dst.Second != 2 || decodes != 0 || into != 2 {
		t.Fatal("pair destination", dst, decodes, into, err)
	}
	applied := FoundationScalar(t, cqltype.TypeBoolean)
	casCols := cqltype.NewColumns(cqltype.ColumnSpec{Name: "[applied]", Type: applied}, cqltype.ColumnSpec{Name: "a", Type: integer})
	casBody := encodeTestParams(t, casCols, PairParams(1, OneParam(Bool), OneParam(Int32)), Pair[bool, int32]{false, 7})
	var cas CASResult[int32]
	custom := directTestRowDecoder{&cas.Previous.Value, &decodes, &into}
	casPlan := CodecTestMust(CASRows(custom).Compile(casCols))
	if err := casPlan.DecodeInto(NewRowReader(casCols, casBody, nil), &cas); err != nil || !cas.Previous.Set || cas.Previous.Value != 7 || decodes != 0 {
		t.Fatal("CAS destination", cas, err)
	}
}
func TestV2CollectDetachesEveryNestedFamily(t *testing.T) {
	text, blob := FoundationScalar(t, cqltype.TypeText), FoundationScalar(t, cqltype.TypeBlob)
	tuple := CodecTestMust(cqltype.TupleType(text, blob))
	list := CodecTestMust(cqltype.ListType(tuple))
	encoder := ListEncoder(TupleEncoder(PairParams(1, OneParam(StringEncoder), OneParam(BytesEncoder))))
	encoded := CodecTestMust(EncodeValueBorrowed(list, encoder, []Pair[string, []byte]{{"one", []byte("blob")}, {"two", []byte("more")}}))
	lease := wire.NewLease(cqltype.BytesChain(encoded.body), nil)
	borrowed := EncodedValue{typ: list, state: cqltype.Present, body: cqltype.BytesView(cqltype.BytesChain(encoded.body), lease)}
	decoder := ListDecoder(TupleDecoder(PairRows(1, OneColumn(TextCodec), OneColumn(BlobCodec))))
	seq := CodecTestMust(DecodeValue(decoder, borrowed))
	values := CodecTestMust(Collect(seq.Iterator()))
	dynamic := CodecTestMust(DynamicCodec.Decode(NewValueReader(list, cqltype.Present, cqltype.BytesChain(encoded.body), lease)))
	dynamicValues := CodecTestMust(Collect(CodecTestMust(dynamic.Elements())))
	lease.Release()
	if values[0].First.String() != "one" || !bytes.Equal(values[1].Second.OwnedBytes(), []byte("more")) {
		t.Fatal("typed Collect retained lease")
	}
	fields := CodecTestMust(Collect(CodecTestMust(dynamicValues[0].Fields())))
	txt := CodecTestMust(fields[0].Value.Text())
	if txt.String() != "one" {
		t.Fatal("dynamic Collect retained lease")
	}
	// Input-side dynamic values borrow caller arrays too; Collect detaches them.
	payload := []byte("input")
	value := CodecTestMust(ListValue(CodecTestMust(cqltype.ListType(blob)), []Value{BlobValue(payload)}))
	input := CodecTestMust(Collect(CodecTestMust(value.Elements())))
	payload[0] = 'X'
	b := CodecTestMust(input[0].Bytes())
	if string(b.OwnedBytes()) != "input" {
		t.Fatal("input-side Collect did not detach")
	}
}
func TestV2UDTIdentityCheckedBeforeNullEncoding(t *testing.T) {
	typ := CodecTestMust(cqltype.UDTType("ks", "record", cqltype.ColumnSpec{Name: "v", Type: FoundationScalar(t, cqltype.TypeInt)}))
	params := Params(Param("v", Int32, func(v int32) int32 { return v }))
	rows := Row(Column("v", Int32, func(v *int32) *int32 { return v }))
	for _, options := range []UDTOptions{{}, {Keyspace: "other", Name: "record"}, {Keyspace: "ks", Name: "wrong"}} {
		if _, err := EncodeValueBorrowed(typ, NullableEncoder(UDTEncoder(params, options)), cqltype.Nullable[int32]{}); err == nil {
			t.Fatal("invalid UDT identity hidden by null")
		}
		if _, err := compileValueDecoder(NullableDecoder(UDTDecoder(rows, options)), typ); err == nil {
			t.Fatal("invalid UDT identity hidden by nullable decoder")
		}
	}
}
