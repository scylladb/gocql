//go:build unit
// +build unit

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/*
 * Content before git sha 34fdeebefcbf183ed7f916f931aa0586fdaa1b40
 * Copyright (c) 2016, The Gocql authors,
 * provided under the BSD-3-Clause License.
 * See the NOTICE file distributed with this work for additional information.
 */

package gocql

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"math/big"
	"net"
	"reflect"
	"strings"
	"testing"

	"gopkg.in/inf.v0"
)

type AliasInt int
type AliasUint uint
type AliasUint8 uint8
type AliasUint16 uint16
type AliasUint32 uint32
type AliasUint64 uint64

var marshalTests = []struct {
	Info           TypeInfo
	Data           []byte
	Value          any
	MarshalError   error
	UnmarshalError error
}{
	{
		CollectionType{
			NativeType: NativeType{proto: protoVersion3, typ: TypeList},
			Elem:       NativeType{proto: protoVersion3, typ: TypeInt},
		},
		[]byte("\x00\x00\x00\x02\x00\x00\x00\x04\x00\x00\x00\x01\x00\x00\x00\x04\x00\x00\x00\x02"),
		func() *[]int {
			l := []int{1, 2}
			return &l
		}(),
		nil,
		nil,
	},
}

var unmarshalTests = []struct {
	Info           TypeInfo
	Data           []byte
	Value          any
	UnmarshalError error
}{
	{
		CollectionType{
			NativeType: NativeType{proto: protoVersion3, typ: TypeList},
			Elem:       NativeType{proto: protoVersion3, typ: TypeInt},
		},
		[]byte("\x00\x00\x00\x02\x00\x00\x00\x04\x00\x00"), // truncated data
		func() *[]int {
			l := []int{1, 2}
			return &l
		}(),
		unmarshalErrorf("unmarshal list: unexpected eof"),
	},
}

func decimalize(s string) *inf.Dec {
	i, _ := new(inf.Dec).SetString(s)
	return i
}

func bigintize(s string) *big.Int {
	i, _ := new(big.Int).SetString(s, 10)
	return i
}

func TestMarshal_Encode(t *testing.T) {
	t.Parallel()

	for i, test := range marshalTests {
		if test.MarshalError == nil {
			data, err := Marshal(test.Info, test.Value)
			if err != nil {
				t.Errorf("marshalTest[%d]: %v", i, err)
				continue
			}
			if !bytes.Equal(data, test.Data) {
				t.Errorf("marshalTest[%d]: expected %q, got %q (%#v)", i, test.Data, data, test.Value)
			}
		} else {
			if _, err := Marshal(test.Info, test.Value); err != test.MarshalError {
				t.Errorf("unmarshalTest[%d] (%v=>%t): %#v returned error %#v, want %#v.", i, test.Info, test.Value, test.Value, err, test.MarshalError)
			}
		}
	}
}

func TestMarshal_Decode(t *testing.T) {
	t.Parallel()

	for i, test := range marshalTests {
		if test.UnmarshalError == nil {
			v := reflect.New(reflect.TypeOf(test.Value))
			err := Unmarshal(test.Info, test.Data, v.Interface())
			if err != nil {
				t.Errorf("unmarshalTest[%d] (%v=>%T): %v", i, test.Info, test.Value, err)
				continue
			}
			if !reflect.DeepEqual(v.Elem().Interface(), test.Value) {
				t.Errorf("unmarshalTest[%d] (%v=>%T): expected %#v, got %#v.", i, test.Info, test.Value, test.Value, v.Elem().Interface())
			}
		} else {
			if err := Unmarshal(test.Info, test.Data, test.Value); err != test.UnmarshalError {
				t.Errorf("unmarshalTest[%d] (%v=>%T): %#v returned error %#v, want %#v.", i, test.Info, test.Value, test.Value, err, test.UnmarshalError)
			}
		}
	}
	for i, test := range unmarshalTests {
		v := reflect.New(reflect.TypeOf(test.Value))
		if test.UnmarshalError == nil {
			err := Unmarshal(test.Info, test.Data, v.Interface())
			if err != nil {
				t.Errorf("unmarshalTest[%d] (%v=>%T): %v", i, test.Info, test.Value, err)
				continue
			}
			if !reflect.DeepEqual(v.Elem().Interface(), test.Value) {
				t.Errorf("unmarshalTest[%d] (%v=>%T): expected %#v, got %#v.", i, test.Info, test.Value, test.Value, v.Elem().Interface())
			}
		} else {
			if err := Unmarshal(test.Info, test.Data, v.Interface()); err != test.UnmarshalError {
				t.Errorf("unmarshalTest[%d] (%v=>%T): %#v returned error %#v, want %#v.", i, test.Info, test.Value, test.Value, err, test.UnmarshalError)
			}
		}
	}
}

func equalStringPointerSlice(leftList, rightList []*string) bool {
	if len(leftList) != len(rightList) {
		return false
	}
	for index := range leftList {
		if !reflect.DeepEqual(rightList[index], leftList[index]) {
			return false
		}
	}
	return true
}

func TestMarshalList(t *testing.T) {
	t.Parallel()

	typeInfoV3 := CollectionType{
		NativeType: NativeType{proto: protoVersion3, typ: TypeList},
		Elem:       NativeType{proto: protoVersion3, typ: TypeVarchar},
	}

	type tc struct {
		typeInfo CollectionType
		input    []*string
		expected []*string
	}

	valueA := "valueA"
	valueB := "valueB"
	valueEmpty := ""
	testCases := []tc{
		{
			typeInfo: typeInfoV3,
			input:    []*string{&valueEmpty},
			expected: []*string{&valueEmpty},
		},
		{
			typeInfo: typeInfoV3,
			input:    []*string{nil},
			expected: []*string{nil},
		},
		{
			typeInfo: typeInfoV3,
			input:    []*string{&valueA, nil, &valueB},
			expected: []*string{&valueA, nil, &valueB},
		},
	}

	listDatas := [][]byte{}
	for _, c := range testCases {
		listData, marshalErr := Marshal(c.typeInfo, c.input)
		if nil != marshalErr {
			t.Errorf("Error marshal %+v of type %+v: %s", c.input, c.typeInfo, marshalErr)
		}
		listDatas = append(listDatas, listData)
	}

	outputLists := [][]*string{}

	var outputList []*string

	for i, listData := range listDatas {
		if unmarshalErr := Unmarshal(testCases[i].typeInfo, listData, &outputList); nil != unmarshalErr {
			t.Error(unmarshalErr)
		}
		resultList := []any{}
		for i := range outputList {
			if outputList[i] != nil {
				resultList = append(resultList, *outputList[i])
			} else {
				resultList = append(resultList, nil)
			}
		}
		outputLists = append(outputLists, outputList)
	}

	for index, c := range testCases {
		outputList := outputLists[index]
		if !equalStringPointerSlice(c.expected, outputList) {
			t.Errorf("Lists %+v not equal to lists %+v, but should", c.expected, outputList)
		}
	}
}

type CustomString string

func (c CustomString) MarshalCQL(info TypeInfo) ([]byte, error) {
	return []byte(strings.ToUpper(string(c))), nil
}
func (c *CustomString) UnmarshalCQL(info TypeInfo, data []byte) error {
	*c = CustomString(strings.ToLower(string(data)))
	return nil
}

type MyString string

var typeLookupTest = []struct {
	TypeName     string
	ExpectedType Type
}{
	{"AsciiType", TypeAscii},
	{"LongType", TypeBigInt},
	{"BytesType", TypeBlob},
	{"BooleanType", TypeBoolean},
	{"CounterColumnType", TypeCounter},
	{"DecimalType", TypeDecimal},
	{"DoubleType", TypeDouble},
	{"FloatType", TypeFloat},
	{"Int32Type", TypeInt},
	{"DateType", TypeTimestamp},
	{"TimestampType", TypeTimestamp},
	{"UUIDType", TypeUUID},
	{"UTF8Type", TypeVarchar},
	{"IntegerType", TypeVarint},
	{"TimeUUIDType", TypeTimeUUID},
	{"InetAddressType", TypeInet},
	{"MapType", TypeMap},
	{"ListType", TypeList},
	{"SetType", TypeSet},
	{"unknown", TypeCustom},
	{"ShortType", TypeSmallInt},
	{"ByteType", TypeTinyInt},
}

func testType(t *testing.T, cassType string, expectedType Type) {
	if computedType := getApacheCassandraType(apacheCassandraTypePrefix + cassType); computedType != expectedType {
		t.Errorf("Cassandra custom type lookup for %s failed. Expected %s, got %s.", cassType, expectedType.String(), computedType.String())
	}
}

func TestLookupCassType(t *testing.T) {
	t.Parallel()

	for _, lookupTest := range typeLookupTest {
		testType(t, lookupTest.TypeName, lookupTest.ExpectedType)
	}
}

type MyPointerMarshaler struct{}

func (m *MyPointerMarshaler) MarshalCQL(_ TypeInfo) ([]byte, error) {
	return []byte{42}, nil
}

func TestMarshalTuple(t *testing.T) {
	t.Parallel()

	info := TupleTypeInfo{
		NativeType: NativeType{proto: protoVersion3, typ: TypeTuple},
		Elems: []TypeInfo{
			NativeType{proto: protoVersion3, typ: TypeVarchar},
			NativeType{proto: protoVersion3, typ: TypeVarchar},
		},
	}

	stringToPtr := func(s string) *string { return &s }
	checkString := func(t *testing.T, exp string, got string) {
		if got != exp {
			t.Errorf("expected string to be %v, got %v", exp, got)
		}
	}

	type tupleStruct struct {
		A string
		B *string
	}
	var (
		s1 *string
		s2 *string
	)

	testCases := []struct {
		name       string
		expected   []byte
		value      any
		checkValue any
		check      func(*testing.T, any)
	}{
		{
			name:       "interface-slice:two-strings",
			expected:   []byte("\x00\x00\x00\x03foo\x00\x00\x00\x03bar"),
			value:      []any{"foo", "bar"},
			checkValue: []any{&s1, &s2},
			check: func(t *testing.T, v any) {
				checkString(t, "foo", *s1)
				checkString(t, "bar", *s2)
			},
		},
		{
			name:       "interface-slice:one-string-one-nil-string",
			expected:   []byte("\x00\x00\x00\x03foo\xff\xff\xff\xff"),
			value:      []any{"foo", nil},
			checkValue: []any{&s1, &s2},
			check: func(t *testing.T, v any) {
				checkString(t, "foo", *s1)
				if s2 != nil {
					t.Errorf("expected string to be nil, got %v", *s2)
				}
			},
		},
		{
			name:     "struct:two-strings",
			expected: []byte("\x00\x00\x00\x03foo\x00\x00\x00\x03bar"),
			value: tupleStruct{
				A: "foo",
				B: stringToPtr("bar"),
			},
			checkValue: &tupleStruct{},
			check: func(t *testing.T, v any) {
				got := v.(*tupleStruct)
				if got.A != "foo" {
					t.Errorf("expected A string to be %v, got %v", "foo", got.A)
				}
				if got.B == nil {
					t.Errorf("expected B string to be %v, got nil", "bar")
				}
				if *got.B != "bar" {
					t.Errorf("expected B string to be %v, got %v", "bar", got.B)
				}
			},
		},
		{
			name:       "struct:one-string-one-nil-string",
			expected:   []byte("\x00\x00\x00\x03foo\xff\xff\xff\xff"),
			value:      tupleStruct{A: "foo", B: nil},
			checkValue: &tupleStruct{},
			check: func(t *testing.T, v any) {
				got := v.(*tupleStruct)
				if got.A != "foo" {
					t.Errorf("expected A string to be %v, got %v", "foo", got.A)
				}
				if got.B != nil {
					t.Errorf("expected B string to be nil, got %v", *got.B)
				}
			},
		},
		{
			name:     "arrayslice:two-strings",
			expected: []byte("\x00\x00\x00\x03foo\x00\x00\x00\x03bar"),
			value: [2]*string{
				stringToPtr("foo"),
				stringToPtr("bar"),
			},
			checkValue: &[2]*string{},
			check: func(t *testing.T, v any) {
				got := v.(*[2]*string)
				checkString(t, "foo", *(got[0]))
				checkString(t, "bar", *(got[1]))
			},
		},
		{
			name:     "arrayslice:one-string-one-nil-string",
			expected: []byte("\x00\x00\x00\x03foo\xff\xff\xff\xff"),
			value: [2]*string{
				stringToPtr("foo"),
				nil,
			},
			checkValue: &[2]*string{},
			check: func(t *testing.T, v any) {
				got := v.(*[2]*string)
				checkString(t, "foo", *(got[0]))
				if got[1] != nil {
					t.Errorf("expected string to be nil, got %v", *got[1])
				}
			},
		},
	}

	for i, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			data, err := Marshal(info, tc.value)
			if err != nil {
				t.Errorf("marshalTest[%d]: %v", i, err)
				return
			}
			if !bytes.Equal(data, tc.expected) {
				t.Errorf("marshalTest[%d]: expected %x, got %x",
					i, tc.expected, data)
				return
			}

			err = Unmarshal(info, data, tc.checkValue)
			if err != nil {
				t.Errorf("unmarshalTest[%d]: %v", i, err)
				return
			}

			tc.check(t, tc.checkValue)
		})
	}
}

func TestUnmarshalTuple(t *testing.T) {
	t.Parallel()

	info := TupleTypeInfo{
		NativeType: NativeType{proto: protoVersion3, typ: TypeTuple},
		Elems: []TypeInfo{
			NativeType{proto: protoVersion3, typ: TypeVarchar},
			NativeType{proto: protoVersion3, typ: TypeVarchar},
		},
	}

	// As per the CQL spec, a tuple is a sequence of "bytes" values.
	// Here we encode a null value (length -1) and the "foo" string (length 3)

	data := []byte("\xff\xff\xff\xff\x00\x00\x00\x03foo")

	t.Run("struct-ptr", func(t *testing.T) {
		var tmp struct {
			A *string
			B *string
		}

		err := Unmarshal(info, data, &tmp)
		if err != nil {
			t.Errorf("unmarshalTest: %v", err)
			return
		}

		if tmp.A != nil || *tmp.B != "foo" {
			t.Errorf("unmarshalTest: expected [nil, foo], got [%v, %v]", *tmp.A, *tmp.B)
		}
	})
	t.Run("struct-nonptr", func(t *testing.T) {
		var tmp struct {
			A string
			B string
		}

		err := Unmarshal(info, data, &tmp)
		if err != nil {
			t.Errorf("unmarshalTest: %v", err)
			return
		}

		if tmp.A != "" || tmp.B != "foo" {
			t.Errorf("unmarshalTest: expected [nil, foo], got [%v, %v]", tmp.A, tmp.B)
		}
	})

	t.Run("array", func(t *testing.T) {
		var tmp [2]*string

		err := Unmarshal(info, data, &tmp)
		if err != nil {
			t.Errorf("unmarshalTest: %v", err)
			return
		}

		if tmp[0] != nil || *tmp[1] != "foo" {
			t.Errorf("unmarshalTest: expected [nil, foo], got [%v, %v]", *tmp[0], *tmp[1])
		}
	})
	t.Run("array-nonptr", func(t *testing.T) {
		var tmp [2]string

		err := Unmarshal(info, data, &tmp)
		if err != nil {
			t.Errorf("unmarshalTest: %v", err)
			return
		}

		if tmp[0] != "" || tmp[1] != "foo" {
			t.Errorf("unmarshalTest: expected [nil, foo], got [%v, %v]", tmp[0], tmp[1])
		}
	})
}

func TestMarshalUDTMap(t *testing.T) {
	t.Parallel()

	typeInfo := UDTTypeInfo{
		KeySpace: "",
		Name:     "xyz",
		Elements: []UDTField{
			{Name: "x", Type: NativeType{proto: protoVersion3, typ: TypeInt}},
			{Name: "y", Type: NativeType{proto: protoVersion3, typ: TypeInt}},
			{Name: "z", Type: NativeType{proto: protoVersion3, typ: TypeInt}},
		},
		NativeType: NativeType{proto: protoVersion3, typ: TypeUDT},
	}

	t.Run("partially bound", func(t *testing.T) {
		value := map[string]any{
			"y": 2,
			"z": 3,
		}
		expected := []byte("\xff\xff\xff\xff\x00\x00\x00\x04\x00\x00\x00\x02\x00\x00\x00\x04\x00\x00\x00\x03")

		data, err := Marshal(typeInfo, value)
		if err != nil {
			t.Errorf("got error %#v", err)
		}
		if !bytes.Equal(data, expected) {
			t.Errorf("got value %x", data)
		}
	})
	t.Run("partially bound from the beginning", func(t *testing.T) {
		value := map[string]any{
			"x": 1,
			"y": 2,
		}
		expected := []byte("\x00\x00\x00\x04\x00\x00\x00\x01\x00\x00\x00\x04\x00\x00\x00\x02\xff\xff\xff\xff")

		data, err := Marshal(typeInfo, value)
		if err != nil {
			t.Errorf("got error %#v", err)
		}
		if !bytes.Equal(data, expected) {
			t.Errorf("got value %x", data)
		}
	})
	t.Run("fully bound", func(t *testing.T) {
		value := map[string]any{
			"x": 1,
			"y": 2,
			"z": 3,
		}
		expected := []byte("\x00\x00\x00\x04\x00\x00\x00\x01\x00\x00\x00\x04\x00\x00\x00\x02\x00\x00\x00\x04\x00\x00\x00\x03")

		data, err := Marshal(typeInfo, value)
		if err != nil {
			t.Errorf("got error %#v", err)
		}
		if !bytes.Equal(data, expected) {
			t.Errorf("got value %x", data)
		}
	})
}

func TestMarshalUDTStruct(t *testing.T) {
	t.Parallel()

	typeInfo := UDTTypeInfo{
		KeySpace: "",
		Name:     "xyz",
		Elements: []UDTField{
			{Name: "x", Type: NativeType{proto: protoVersion3, typ: TypeInt}},
			{Name: "y", Type: NativeType{proto: protoVersion3, typ: TypeInt}},
			{Name: "z", Type: NativeType{proto: protoVersion3, typ: TypeInt}},
		},
		NativeType: NativeType{proto: protoVersion3, typ: TypeUDT},
	}

	type xyzStruct struct {
		X int32 `cql:"x"`
		Y int32 `cql:"y"`
		Z int32 `cql:"z"`
	}
	type xyStruct struct {
		X int32 `cql:"x"`
		Y int32 `cql:"y"`
	}
	type yzStruct struct {
		Y int32 `cql:"y"`
		Z int32 `cql:"z"`
	}

	t.Run("partially bound", func(t *testing.T) {
		value := yzStruct{
			Y: 2,
			Z: 3,
		}
		expected := []byte("\xff\xff\xff\xff\x00\x00\x00\x04\x00\x00\x00\x02\x00\x00\x00\x04\x00\x00\x00\x03")

		data, err := Marshal(typeInfo, value)
		if err != nil {
			t.Errorf("got error %#v", err)
		}
		if !bytes.Equal(data, expected) {
			t.Errorf("got value %x", data)
		}
	})
	t.Run("partially bound from the beginning", func(t *testing.T) {
		value := xyStruct{
			X: 1,
			Y: 2,
		}
		expected := []byte("\x00\x00\x00\x04\x00\x00\x00\x01\x00\x00\x00\x04\x00\x00\x00\x02\xff\xff\xff\xff")

		data, err := Marshal(typeInfo, value)
		if err != nil {
			t.Errorf("got error %#v", err)
		}
		if !bytes.Equal(data, expected) {
			t.Errorf("got value %x", data)
		}
	})
	t.Run("fully bound", func(t *testing.T) {
		value := xyzStruct{
			X: 1,
			Y: 2,
			Z: 3,
		}
		expected := []byte("\x00\x00\x00\x04\x00\x00\x00\x01\x00\x00\x00\x04\x00\x00\x00\x02\x00\x00\x00\x04\x00\x00\x00\x03")

		data, err := Marshal(typeInfo, value)
		if err != nil {
			t.Errorf("got error %#v", err)
		}
		if !bytes.Equal(data, expected) {
			t.Errorf("got value %x", data)
		}
	})
}

func TestMarshalNil(t *testing.T) {
	t.Parallel()

	types := []Type{
		TypeAscii,
		TypeBlob,
		TypeBoolean,
		TypeBigInt,
		TypeCounter,
		TypeDecimal,
		TypeDouble,
		TypeFloat,
		TypeInt,
		TypeTimestamp,
		TypeUUID,
		TypeVarchar,
		TypeVarint,
		TypeTimeUUID,
		TypeInet,
	}

	for _, typ := range types {
		data, err := Marshal(NativeType{proto: protoVersion3, typ: typ}, nil)
		if err != nil {
			t.Errorf("unable to marshal nil %v: %v\n", typ, err)
		} else if data != nil {
			t.Errorf("expected to get nil byte for nil %v got % X", typ, data)
		}
	}

	// Collection types also need nil coverage.
	collectionTypes := []Type{TypeList, TypeSet, TypeMap}
	for _, typ := range collectionTypes {
		info := CollectionType{
			NativeType: NativeType{proto: protoVersion3, typ: typ},
			Key:        NativeType{proto: protoVersion3, typ: TypeVarchar},
			Elem:       NativeType{proto: protoVersion3, typ: TypeVarchar},
		}
		data, err := Marshal(info, nil)
		if err != nil {
			t.Errorf("unable to marshal nil %v: %v\n", typ, err)
		} else if data != nil {
			t.Errorf("expected nil bytes for nil %v, got % X", typ, data)
		}
	}
}

func TestUnmarshalInetCopyBytes(t *testing.T) {
	t.Parallel()

	data := []byte{127, 0, 0, 1}
	var ip net.IP
	if err := unmarshalInet(data, &ip); err != nil {
		t.Fatal(err)
	}

	copy(data, []byte{0xFF, 0xFF, 0xFF, 0xFF})
	ip2 := net.IP(data)
	if !ip.Equal(net.IPv4(127, 0, 0, 1)) {
		t.Fatalf("IP memory shared with data: ip=%v ip2=%v", ip, ip2)
	}
}

func BenchmarkUnmarshalVarchar(b *testing.B) {
	b.ReportAllocs()
	src := make([]byte, 1024)
	dst := make([]byte, len(src))

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := unmarshalVarchar(src, &dst); err != nil {
			b.Fatal(err)
		}
	}
}

func TestReadCollectionSize(t *testing.T) {
	t.Parallel()

	listV3 := CollectionType{
		NativeType: NativeType{proto: protoVersion3, typ: TypeList},
		Elem:       NativeType{proto: protoVersion3, typ: TypeVarchar},
	}

	tests := []struct {
		name         string
		info         CollectionType
		data         []byte
		isError      bool
		expectedSize int
	}{
		{
			name:    "short read 0 proto 3",
			info:    listV3,
			data:    []byte{},
			isError: true,
		},
		{
			name:    "short read 1 proto 3",
			info:    listV3,
			data:    []byte{0x01},
			isError: true,
		},
		{
			name:    "short read 2 proto 3",
			info:    listV3,
			data:    []byte{0x01, 0x38},
			isError: true,
		},
		{
			name:    "short read 3 proto 3",
			info:    listV3,
			data:    []byte{0x01, 0x38, 0x42},
			isError: true,
		},
		{
			name:         "good read proto 3",
			info:         listV3,
			data:         []byte{0x01, 0x38, 0x42, 0x22},
			expectedSize: 0x01384222,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			size, _, err := readCollectionSize(test.data)
			if test.isError {
				if err == nil {
					t.Fatal("Expected error, but it was nil")
				}
			} else {
				if err != nil {
					t.Fatalf("Expected no error, got %v", err)
				}
				if size != test.expectedSize {
					t.Fatalf("Expected size of %d, but got %d", test.expectedSize, size)
				}
			}
		})
	}
}

func TestReadUnsignedVInt(t *testing.T) {
	tests := []struct {
		decodedInt  uint64
		encodedVint []byte
	}{
		{
			decodedInt:  0,
			encodedVint: []byte{0},
		},
		{
			decodedInt:  100,
			encodedVint: []byte{100},
		},
		{
			decodedInt:  256000,
			encodedVint: []byte{195, 232, 0},
		},
	}
	for _, test := range tests {
		t.Run(fmt.Sprintf("%d", test.decodedInt), func(t *testing.T) {
			actual, _, err := readUnsignedVInt(test.encodedVint)
			if err != nil {
				t.Fatalf("Expected no error, got %v", err)
			}
			if actual != test.decodedInt {
				t.Fatalf("Expected %d, but got %d", test.decodedInt, actual)
			}
		})
	}
}

func BenchmarkUnmarshalUUID(b *testing.B) {
	b.ReportAllocs()
	src := make([]byte, 16)
	dst := UUID{}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := unmarshalUUID(src, &dst); err != nil {
			b.Fatal(err)
		}
	}
}

func TestUnmarshalUDT(t *testing.T) {
	t.Parallel()

	info := UDTTypeInfo{
		NativeType: NativeType{proto: protoVersion4, typ: TypeUDT},
		Name:       "myudt",
		KeySpace:   "myks",
		Elements: []UDTField{
			{
				Name: "first",
				Type: NativeType{proto: protoVersion4, typ: TypeAscii},
			},
			{
				Name: "second",
				Type: NativeType{proto: protoVersion4, typ: TypeSmallInt},
			},
		},
	}
	data := bytesWithLength( // UDT
		bytesWithLength([]byte("Hello")),    // first
		bytesWithLength([]byte("\x00\x2a")), // second
	)
	value := map[string]any{}
	expectedErr := unmarshalErrorf("can not unmarshal into non-pointer map[string]interface {}")

	if err := Unmarshal(info, data, value); err != expectedErr {
		t.Errorf("(%v=>%T): %#v returned error %#v, want %#v.",
			info, value, value, err, expectedErr)
	}
}

// TestUnmarshalUDTIntoInterface tests that UDTs can be unmarshaled into *any.
// This is used by MapScan when the destination map has a pre-existing entry
// for a UDT column (the value is an any, so Scan receives *any).
func TestUnmarshalUDTIntoInterface(t *testing.T) {
	t.Parallel()

	info := UDTTypeInfo{
		NativeType: NativeType{proto: protoVersion4, typ: TypeUDT},
		Name:       "myudt",
		KeySpace:   "myks",
		Elements: []UDTField{
			{
				Name: "first",
				Type: NativeType{proto: protoVersion4, typ: TypeAscii},
			},
			{
				Name: "second",
				Type: NativeType{proto: protoVersion4, typ: TypeSmallInt},
			},
		},
	}
	data := append(
		bytesWithLength([]byte("Hello")),       // first
		bytesWithLength([]byte("\x00\x2a"))..., // second
	)

	var dest any
	if err := Unmarshal(info, data, &dest); err != nil {
		t.Fatalf("Unmarshal into *any failed: %v", err)
	}

	result, ok := dest.(map[string]any)
	if !ok {
		t.Fatalf("expected map[string]any, got %T", dest)
	}
	if result["first"] != "Hello" {
		t.Errorf("expected first=Hello, got %v", result["first"])
	}
	if result["second"] != int16(42) {
		t.Errorf("expected second=42, got %v (%T)", result["second"], result["second"])
	}

	// nil data should produce nil
	var dest2 any
	if err := Unmarshal(info, nil, &dest2); err != nil {
		t.Fatalf("Unmarshal nil into *any failed: %v", err)
	}
	if dest2 != nil {
		t.Errorf("expected nil for nil data, got %v", dest2)
	}
}

// TestUnmarshalListIntoInterface tests that lists can be unmarshaled into *any
// This is used by MapScan and SliceMap functions.
func TestUnmarshalListIntoInterface(t *testing.T) {
	t.Parallel()

	// Create a list of ints: [1, 2]
	// Format: [list_size (4 bytes), element_length (4 bytes), element_data, ...]
	// Reference: line 63 shows format: \x00\x00\x00\x02\x00\x00\x00\x04\x00\x00\x00\x01\x00\x00\x00\x04\x00\x00\x00\x02
	data := []byte{
		0, 0, 0, 2, // list size: 2 elements
		0, 0, 0, 4, // element 0 length: 4 bytes
		0, 0, 0, 1, // element 0 value: 1
		0, 0, 0, 4, // element 1 length: 4 bytes
		0, 0, 0, 2, // element 1 value: 2
	}

	info := CollectionType{
		NativeType: NativeType{proto: protoVersion4, typ: TypeList},
		Elem:       NativeType{proto: protoVersion4, typ: TypeInt},
	}

	var result any
	if err := Unmarshal(info, data, &result); err != nil {
		t.Fatalf("Unmarshal failed: %v", err)
	}

	// Verify the result is a []int
	slice, ok := result.([]int)
	if !ok {
		t.Fatalf("Expected []int, got %T", result)
	}
	if len(slice) != 2 {
		t.Fatalf("Expected 2 elements, got %d", len(slice))
	}
	expected := []int{1, 2}
	for i, v := range expected {
		if slice[i] != v {
			t.Errorf("Element %d: expected %d, got %d", i, v, slice[i])
		}
	}
}

// TestUnmarshalMapIntoInterface tests that maps can be unmarshaled into *any
// This is used by MapScan and SliceMap functions.
func TestUnmarshalMapIntoInterface(t *testing.T) {
	t.Parallel()

	// Create a map: {"a": 1, "b": 2}
	// Format: [map_size (4 bytes), key_length, key_data, value_length, value_data, ...]
	data := []byte{
		0, 0, 0, 2, // map size: 2 entries
		0, 0, 0, 1, // key 0 length: 1 byte
		'a',        // key 0 value: "a"
		0, 0, 0, 4, // value 0 length: 4 bytes
		0, 0, 0, 1, // value 0: 1
		0, 0, 0, 1, // key 1 length: 1 byte
		'b',        // key 1 value: "b"
		0, 0, 0, 4, // value 1 length: 4 bytes
		0, 0, 0, 2, // value 1: 2
	}

	info := CollectionType{
		NativeType: NativeType{proto: protoVersion4, typ: TypeMap},
		Key:        NativeType{proto: protoVersion4, typ: TypeVarchar},
		Elem:       NativeType{proto: protoVersion4, typ: TypeInt},
	}

	var result any
	if err := Unmarshal(info, data, &result); err != nil {
		t.Fatalf("Unmarshal failed: %v", err)
	}

	// Verify the result is a map[string]int
	m, ok := result.(map[string]int)
	if !ok {
		t.Fatalf("Expected map[string]int, got %T", result)
	}
	if len(m) != 2 {
		t.Fatalf("Expected 2 entries, got %d", len(m))
	}
	if m["a"] != 1 {
		t.Errorf("Expected m[\"a\"] = 1, got %d", m["a"])
	}
	if m["b"] != 2 {
		t.Errorf("Expected m[\"b\"] = 2, got %d", m["b"])
	}
}

// TestUnmarshalListWithVectorIntoInterface tests that lists containing vectors
// can be unmarshaled into *any (issue #692)
func TestUnmarshalListWithVectorIntoInterface(t *testing.T) {
	t.Parallel()

	// Create a list of vectors: [[1.0, 2.0], [3.0, 4.0]]
	// Vector elements are fixed-size floats (4 bytes each)
	// Format: [list_size (4 bytes), element_length, element_data (vector), ...]
	// Each vector is 8 bytes (2 floats * 4 bytes)
	var data []byte

	// List size: 2 vectors
	data = append(data, 0, 0, 0, 2)

	// Vector 1: [1.0, 2.0]
	data = append(data, 0, 0, 0, 8) // vector length: 8 bytes
	float1Bytes := make([]byte, 4)
	binary.BigEndian.PutUint32(float1Bytes, math.Float32bits(1.0))
	data = append(data, float1Bytes...)
	float2Bytes := make([]byte, 4)
	binary.BigEndian.PutUint32(float2Bytes, math.Float32bits(2.0))
	data = append(data, float2Bytes...)

	// Vector 2: [3.0, 4.0]
	data = append(data, 0, 0, 0, 8) // vector length: 8 bytes
	float3Bytes := make([]byte, 4)
	binary.BigEndian.PutUint32(float3Bytes, math.Float32bits(3.0))
	data = append(data, float3Bytes...)
	float4Bytes := make([]byte, 4)
	binary.BigEndian.PutUint32(float4Bytes, math.Float32bits(4.0))
	data = append(data, float4Bytes...)

	info := CollectionType{
		NativeType: NativeType{proto: protoVersion4, typ: TypeList},
		Elem: VectorType{
			NativeType: NativeType{proto: protoVersion4, typ: TypeCustom, custom: apacheCassandraTypePrefix + "VectorType"},
			SubType:    NativeType{proto: protoVersion4, typ: TypeFloat},
			Dimensions: 2,
		},
	}

	var result any
	if err := Unmarshal(info, data, &result); err != nil {
		t.Fatalf("Unmarshal failed: %v", err)
	}

	// Verify the result is a [][]float32
	slice, ok := result.([][]float32)
	if !ok {
		t.Fatalf("Expected [][]float32, got %T", result)
	}
	if len(slice) != 2 {
		t.Fatalf("Expected 2 elements, got %d", len(slice))
	}
	if len(slice[0]) != 2 || slice[0][0] != 1.0 || slice[0][1] != 2.0 {
		t.Errorf("Expected slice[0] = [1.0, 2.0], got %v", slice[0])
	}
	if len(slice[1]) != 2 || slice[1][0] != 3.0 || slice[1][1] != 4.0 {
		t.Errorf("Expected slice[1] = [3.0, 4.0], got %v", slice[1])
	}
}

// bytesWithLength concatenates all data slices and prepends the total length as uint32.
// The length does not count the size of the uint32 used for writing the size.
func bytesWithLength(data ...[]byte) []byte {
	totalLen := 0
	for i := range data {
		totalLen += len(data[i])
	}
	if totalLen > math.MaxUint32 {
		panic("total length overflows")
	}
	ret := make([]byte, totalLen+4)
	binary.BigEndian.PutUint32(ret[:4], uint32(totalLen))
	buf := ret[4:]
	for i := range data {
		n := copy(buf, data[i])
		buf = buf[n:]
	}
	return ret
}

func TestUnmarshalVectorZeroDimensions(t *testing.T) {
	info := VectorType{
		NativeType: NewCustomType(protoVersion4, TypeCustom, apacheCassandraTypePrefix+"VectorType"),
		SubType:    NativeType{proto: protoVersion4, typ: TypeFloat},
		Dimensions: 0,
	}

	t.Run("nil_data", func(t *testing.T) {
		var result []float32
		if err := unmarshalVector(info, nil, &result); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("empty_data", func(t *testing.T) {
		var result []float32
		if err := unmarshalVector(info, []byte{}, &result); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if result == nil {
			t.Fatal("expected non-nil empty slice")
		}
		if len(result) != 0 {
			t.Fatalf("expected len 0, got %d", len(result))
		}
	})

	t.Run("nonempty_data_errors", func(t *testing.T) {
		var result []float32
		err := unmarshalVector(info, []byte{0x01, 0x02}, &result)
		if err == nil {
			t.Fatal("expected error for non-empty data with 0 dimensions")
		}
		if !strings.Contains(err.Error(), "0-dimension") {
			t.Fatalf("expected error mentioning 0-dimension, got: %v", err)
		}
	})

	t.Run("empty_data_into_zero_length_array", func(t *testing.T) {
		var result [0]float32
		if err := unmarshalVector(info, []byte{}, &result); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("empty_data_into_nonzero_length_array_errors", func(t *testing.T) {
		var result [5]float32
		err := unmarshalVector(info, []byte{}, &result)
		if err == nil {
			t.Fatal("expected error for 0-dimension vector into non-zero-length array")
		}
		if !strings.Contains(err.Error(), "array of size 5") {
			t.Fatalf("expected error mentioning array size, got: %v", err)
		}
	})

	t.Run("empty_data_into_interface", func(t *testing.T) {
		var result any
		if err := unmarshalVector(info, []byte{}, &result); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})
}

// TestNativeNewWithErrorConsistentWithGoType verifies that the fast-path type mapping
// in NativeType.NewWithError() stays consistent with the canonical goType() mapping.
// This guards against future changes to one mapping that forget to update the other.
func TestNativeNewWithErrorConsistentWithGoType(t *testing.T) {
	// All NativeType type codes that goType handles (excluding collection/tuple/UDT
	// which are separate TypeInfo implementations).
	nativeTypes := []Type{
		TypeVarchar, TypeAscii, TypeText, TypeInet,
		TypeBigInt, TypeCounter,
		TypeTime,
		TypeTimestamp,
		TypeBlob,
		TypeBoolean,
		TypeFloat,
		TypeDouble,
		TypeInt,
		TypeSmallInt,
		TypeTinyInt,
		TypeDecimal,
		TypeUUID, TypeTimeUUID,
		TypeVarint,
		TypeDate,
		TypeDuration,
	}

	for _, typ := range nativeTypes {
		nt := NativeType{typ: typ, proto: protoVersion4}

		// Get the fast-path result from NewWithError
		fastVal, err := nt.NewWithError()
		if err != nil {
			t.Errorf("NewWithError(%s): unexpected error: %v", typ, err)
			continue
		}

		// Get the canonical type from goType
		canonicalType, err := goType(nt)
		if err != nil {
			t.Errorf("goType(%s): unexpected error: %v", typ, err)
			continue
		}

		// NewWithError returns a pointer (reflect.New(typ).Interface()), so the
		// underlying type is reflect.TypeOf(val).Elem()
		fastType := reflect.TypeOf(fastVal)
		if fastType.Kind() != reflect.Ptr {
			t.Errorf("NewWithError(%s): expected pointer, got %s", typ, fastType.Kind())
			continue
		}
		fastElemType := fastType.Elem()

		if fastElemType != canonicalType {
			t.Errorf("NewWithError(%s) fast-path type %s does not match goType() canonical type %s",
				typ, fastElemType, canonicalType)
		}
	}
}

// TestCollectionNewWithErrorConsistentWithGoType verifies that the fast-path type mapping
// in CollectionType.NewWithError() stays consistent with the canonical goType() mapping.
func TestCollectionNewWithErrorConsistentWithGoType(t *testing.T) {
	elemTypes := []Type{
		TypeInt, TypeBigInt, TypeCounter,
		TypeText, TypeVarchar, TypeAscii,
		TypeBoolean,
		TypeFloat, TypeDouble,
		TypeUUID, TypeTimeUUID,
		TypeTimestamp, TypeDate,
		TypeSmallInt, TypeTinyInt,
		TypeBlob,
	}

	// Test list and set types
	for _, collTyp := range []Type{TypeList, TypeSet} {
		for _, elemTyp := range elemTypes {
			ct := CollectionType{
				NativeType: NativeType{typ: collTyp, proto: protoVersion4},
				Elem:       NativeType{typ: elemTyp, proto: protoVersion4},
			}

			fastVal, err := ct.NewWithError()
			if err != nil {
				t.Errorf("NewWithError(%s<%s>): unexpected error: %v", collTyp, elemTyp, err)
				continue
			}

			canonicalType, err := goType(ct)
			if err != nil {
				t.Errorf("goType(%s<%s>): unexpected error: %v", collTyp, elemTyp, err)
				continue
			}

			fastType := reflect.TypeOf(fastVal)
			if fastType.Kind() != reflect.Ptr {
				t.Errorf("NewWithError(%s<%s>): expected pointer, got %s", collTyp, elemTyp, fastType.Kind())
				continue
			}

			if fastType.Elem() != canonicalType {
				t.Errorf("NewWithError(%s<%s>) fast-path type %s does not match goType() canonical type %s",
					collTyp, elemTyp, fastType.Elem(), canonicalType)
			}
		}
	}

	// Test map types with common key/value combinations
	keyTypes := []Type{TypeText, TypeVarchar, TypeInt}
	valTypes := []Type{
		TypeInt, TypeBigInt,
		TypeText, TypeVarchar,
		TypeBoolean,
		TypeFloat, TypeDouble,
		TypeUUID,
	}

	for _, keyTyp := range keyTypes {
		for _, valTyp := range valTypes {
			ct := CollectionType{
				NativeType: NativeType{typ: TypeMap, proto: protoVersion4},
				Key:        NativeType{typ: keyTyp, proto: protoVersion4},
				Elem:       NativeType{typ: valTyp, proto: protoVersion4},
			}

			fastVal, err := ct.NewWithError()
			if err != nil {
				t.Errorf("NewWithError(map<%s, %s>): unexpected error: %v", keyTyp, valTyp, err)
				continue
			}

			canonicalType, err := goType(ct)
			if err != nil {
				t.Errorf("goType(map<%s, %s>): unexpected error: %v", keyTyp, valTyp, err)
				continue
			}

			fastType := reflect.TypeOf(fastVal)
			if fastType.Kind() != reflect.Ptr {
				t.Errorf("NewWithError(map<%s, %s>): expected pointer, got %s", keyTyp, valTyp, fastType.Kind())
				continue
			}

			if fastType.Elem() != canonicalType {
				t.Errorf("NewWithError(map<%s, %s>) fast-path type %s does not match goType() canonical type %s",
					keyTyp, valTyp, fastType.Elem(), canonicalType)
			}
		}
	}
}

// buildMapBytes builds the CQL binary wire format for a map with n entries.
// keyFn and valFn produce the raw bytes for each key and value given the entry index.
func buildMapBytes(n int, keyFn, valFn func(i int) []byte) []byte {
	// 4 bytes for the entry count
	size := 4
	for i := 0; i < n; i++ {
		size += 4 + len(keyFn(i)) + 4 + len(valFn(i))
	}
	buf := make([]byte, 0, size)
	buf = append(buf, byte(n>>24), byte(n>>16), byte(n>>8), byte(n))
	for i := 0; i < n; i++ {
		k := keyFn(i)
		buf = append(buf, byte(len(k)>>24), byte(len(k)>>16), byte(len(k)>>8), byte(len(k)))
		buf = append(buf, k...)
		v := valFn(i)
		buf = append(buf, byte(len(v)>>24), byte(len(v)>>16), byte(len(v)>>8), byte(len(v)))
		buf = append(buf, v...)
	}
	return buf
}

func int64Bytes(v int64) []byte {
	return []byte{byte(v >> 56), byte(v >> 48), byte(v >> 40), byte(v >> 32),
		byte(v >> 24), byte(v >> 16), byte(v >> 8), byte(v)}
}

func TestUnmarshalMapFastPath(t *testing.T) {
	t.Parallel()

	infoSS := CollectionType{
		NativeType: NativeType{proto: protoVersion4, typ: TypeMap},
		Key:        NativeType{proto: protoVersion4, typ: TypeVarchar},
		Elem:       NativeType{proto: protoVersion4, typ: TypeVarchar},
	}
	infoSI := CollectionType{
		NativeType: NativeType{proto: protoVersion4, typ: TypeMap},
		Key:        NativeType{proto: protoVersion4, typ: TypeVarchar},
		Elem:       NativeType{proto: protoVersion4, typ: TypeBigInt},
	}
	infoII := CollectionType{
		NativeType: NativeType{proto: protoVersion4, typ: TypeMap},
		Key:        NativeType{proto: protoVersion4, typ: TypeBigInt},
		Elem:       NativeType{proto: protoVersion4, typ: TypeBigInt},
	}

	t.Run("nil data sets nil map", func(t *testing.T) {
		var m map[string]string
		if err := unmarshalMap(infoSS, nil, &m); err != nil {
			t.Fatal(err)
		}
		if m != nil {
			t.Fatalf("expected nil map, got %v", m)
		}
	})

	t.Run("empty map", func(t *testing.T) {
		data := buildMapBytes(0, nil, nil)
		var m map[string]string
		if err := unmarshalMap(infoSS, data, &m); err != nil {
			t.Fatal(err)
		}
		if len(m) != 0 {
			t.Fatalf("expected empty map, got %v", m)
		}
	})

	t.Run("map[string]string correctness", func(t *testing.T) {
		data := buildMapBytes(3,
			func(i int) []byte { return []byte(fmt.Sprintf("k%d", i)) },
			func(i int) []byte { return []byte(fmt.Sprintf("v%d", i)) },
		)
		var fast map[string]string
		if err := unmarshalMap(infoSS, data, &fast); err != nil {
			t.Fatal(err)
		}
		expected := map[string]string{"k0": "v0", "k1": "v1", "k2": "v2"}
		if !reflect.DeepEqual(fast, expected) {
			t.Fatalf("got %v, want %v", fast, expected)
		}
	})

	t.Run("map[string]int64 correctness", func(t *testing.T) {
		data := buildMapBytes(3,
			func(i int) []byte { return []byte(fmt.Sprintf("k%d", i)) },
			func(i int) []byte { return int64Bytes(int64(i * 10)) },
		)
		var fast map[string]int64
		if err := unmarshalMap(infoSI, data, &fast); err != nil {
			t.Fatal(err)
		}
		expected := map[string]int64{"k0": 0, "k1": 10, "k2": 20}
		if !reflect.DeepEqual(fast, expected) {
			t.Fatalf("got %v, want %v", fast, expected)
		}
	})

	t.Run("map[int64]int64 correctness", func(t *testing.T) {
		data := buildMapBytes(3,
			func(i int) []byte { return int64Bytes(int64(i)) },
			func(i int) []byte { return int64Bytes(int64(i * 100)) },
		)
		var fast map[int64]int64
		if err := unmarshalMap(infoII, data, &fast); err != nil {
			t.Fatal(err)
		}
		expected := map[int64]int64{0: 0, 1: 100, 2: 200}
		if !reflect.DeepEqual(fast, expected) {
			t.Fatalf("got %v, want %v", fast, expected)
		}
	})

	t.Run("null value decoded as zero", func(t *testing.T) {
		// Build a map with one entry where value is null (size = -1)
		buf := []byte{
			0, 0, 0, 1, // n=1
			0, 0, 0, 3, 'k', 'e', 'y', // key = "key"
			0xff, 0xff, 0xff, 0xff, // value size = -1 (null)
		}
		var m map[string]int64
		if err := unmarshalMap(infoSI, buf, &m); err != nil {
			t.Fatal(err)
		}
		if v, ok := m["key"]; !ok || v != 0 {
			t.Fatalf("expected key→0, got key→%d (ok=%v)", v, ok)
		}
	})

	t.Run("truncated data returns error", func(t *testing.T) {
		buf := []byte{0, 0, 0, 1, 0, 0} // n=1 but truncated key
		var m map[string]string
		err := unmarshalMap(infoSS, buf, &m)
		if err == nil {
			t.Fatal("expected error on truncated data")
		}
	})

	t.Run("fallthrough to reflect for mismatched types", func(t *testing.T) {
		// *map[string]string with TypeBigInt elem should fall through to reflect
		data := buildMapBytes(1,
			func(i int) []byte { return []byte("key") },
			func(i int) []byte { return int64Bytes(42) },
		)
		infoMismatch := CollectionType{
			NativeType: NativeType{proto: protoVersion4, typ: TypeMap},
			Key:        NativeType{proto: protoVersion4, typ: TypeVarchar},
			Elem:       NativeType{proto: protoVersion4, typ: TypeBigInt},
		}
		// The fast path must NOT claim this mismatched elem type (BigInt, not a
		// string), so it has to report handled == false and let the generic
		// reflect path take over. Asserting this protects the dispatch logic.
		var m map[string]string
		handled, err := unmarshalMapFast(infoMismatch, data, &m)
		if err != nil {
			t.Fatalf("unexpected fast-path error: %v", err)
		}
		if handled {
			t.Fatal("expected mismatched elem type to skip the fast path")
		}

		_ = unmarshalMap(infoMismatch, data, &m) // keep the panic-smoke test
	})

	t.Run("ascii map keys/values are validated via generic path", func(t *testing.T) {
		// A byte > 127 is invalid ASCII. With TypeAscii excluded from the
		// raw-string fast path, this must be rejected by serialization/ascii
		// instead of being silently accepted.
		data := buildMapBytes(1,
			func(i int) []byte { return []byte{0x80} }, // invalid ascii key
			func(i int) []byte { return []byte("v") },
		)
		infoAscii := CollectionType{
			NativeType: NativeType{proto: protoVersion4, typ: TypeMap},
			Key:        NativeType{proto: protoVersion4, typ: TypeAscii},
			Elem:       NativeType{proto: protoVersion4, typ: TypeAscii},
		}

		// Fast path must not claim ascii-typed maps.
		var fm map[string]string
		if handled, _ := unmarshalMapFast(infoAscii, data, &fm); handled {
			t.Fatal("expected ascii map to skip the raw-string fast path")
		}

		// Full Unmarshal (generic path) must reject the invalid byte.
		var m map[string]string
		if err := Unmarshal(infoAscii, data, &m); err == nil {
			t.Fatal("expected error unmarshaling invalid ascii, got nil")
		}
	})
}

// namedStrStrMap is a named map type. Because Go type switches match concrete
// types exactly, unmarshalMapFast does NOT recognise it, so unmarshaling into
// it exercises the generic reflect path. This lets us assert that the fast
// path and the generic path agree byte-for-byte on identical wire data.
type namedStrStrMap map[string]string
type namedStrI64Map map[string]int64
type namedI64I64Map map[int64]int64

// TestUnmarshalMapFastVsReflect is a differential test: for the same wire
// bytes it unmarshals via the fast path (concrete map type) and via the
// generic reflect path (named map type) and requires identical results.
func TestUnmarshalMapFastVsReflect(t *testing.T) {
	t.Parallel()

	infoSS := CollectionType{
		NativeType: NativeType{proto: protoVersion4, typ: TypeMap},
		Key:        NativeType{proto: protoVersion4, typ: TypeVarchar},
		Elem:       NativeType{proto: protoVersion4, typ: TypeVarchar},
	}
	infoSI := CollectionType{
		NativeType: NativeType{proto: protoVersion4, typ: TypeMap},
		Key:        NativeType{proto: protoVersion4, typ: TypeVarchar},
		Elem:       NativeType{proto: protoVersion4, typ: TypeBigInt},
	}
	infoII := CollectionType{
		NativeType: NativeType{proto: protoVersion4, typ: TypeMap},
		Key:        NativeType{proto: protoVersion4, typ: TypeBigInt},
		Elem:       NativeType{proto: protoVersion4, typ: TypeBigInt},
	}

	// nullVal returns a -1 (null) length prefix.
	nullPrefix := []byte{0xff, 0xff, 0xff, 0xff}
	withLen := func(b []byte) []byte {
		out := []byte{byte(len(b) >> 24), byte(len(b) >> 16), byte(len(b) >> 8), byte(len(b))}
		return append(out, b...)
	}
	count := func(n int) []byte {
		return []byte{byte(n >> 24), byte(n >> 16), byte(n >> 8), byte(n)}
	}

	t.Run("string/string with null key and null value", func(t *testing.T) {
		// entry0: key "" (len 0), value null
		// entry1: key null, value "v"
		var data []byte
		data = append(data, count(2)...)
		data = append(data, withLen([]byte{})...) // key ""
		data = append(data, nullPrefix...)        // value null
		data = append(data, nullPrefix...)        // key null
		data = append(data, withLen([]byte("v"))...)

		var fast map[string]string
		if err := unmarshalMap(infoSS, data, &fast); err != nil {
			t.Fatalf("fast path: %v", err)
		}
		var slow namedStrStrMap
		if err := unmarshalMap(infoSS, data, &slow); err != nil {
			t.Fatalf("reflect path: %v", err)
		}
		if !reflect.DeepEqual(map[string]string(fast), map[string]string(slow)) {
			t.Fatalf("fast=%v reflect=%v differ", fast, slow)
		}
	})

	t.Run("string/int64 with null value vs reflect", func(t *testing.T) {
		var data []byte
		data = append(data, count(2)...)
		data = append(data, withLen([]byte("a"))...)
		data = append(data, nullPrefix...) // null int64 value
		data = append(data, withLen([]byte("b"))...)
		data = append(data, withLen(int64Bytes(7))...)

		var fast map[string]int64
		if err := unmarshalMap(infoSI, data, &fast); err != nil {
			t.Fatalf("fast: %v", err)
		}
		var slow namedStrI64Map
		if err := unmarshalMap(infoSI, data, &slow); err != nil {
			t.Fatalf("reflect: %v", err)
		}
		if !reflect.DeepEqual(map[string]int64(fast), map[string]int64(slow)) {
			t.Fatalf("fast=%v reflect=%v differ", fast, slow)
		}
	})

	t.Run("int64/int64 duplicate keys keep last vs reflect", func(t *testing.T) {
		// Duplicate key 5 appears twice; map semantics keep the last write.
		var data []byte
		data = append(data, count(2)...)
		data = append(data, withLen(int64Bytes(5))...)
		data = append(data, withLen(int64Bytes(1))...)
		data = append(data, withLen(int64Bytes(5))...)
		data = append(data, withLen(int64Bytes(2))...)

		var fast map[int64]int64
		if err := unmarshalMap(infoII, data, &fast); err != nil {
			t.Fatalf("fast: %v", err)
		}
		var slow namedI64I64Map
		if err := unmarshalMap(infoII, data, &slow); err != nil {
			t.Fatalf("reflect: %v", err)
		}
		if !reflect.DeepEqual(map[int64]int64(fast), map[int64]int64(slow)) {
			t.Fatalf("fast=%v reflect=%v differ", fast, slow)
		}
		if fast[5] != 2 {
			t.Fatalf("expected last-write-wins key 5 -> 2, got %d", fast[5])
		}
	})

	t.Run("nil data agrees", func(t *testing.T) {
		var fast map[string]string
		var slow namedStrStrMap
		if err := unmarshalMap(infoSS, nil, &fast); err != nil {
			t.Fatal(err)
		}
		if err := unmarshalMap(infoSS, nil, &slow); err != nil {
			t.Fatal(err)
		}
		if (fast == nil) != (slow == nil) {
			t.Fatalf("nil disagreement: fast nil=%v reflect nil=%v", fast == nil, slow == nil)
		}
	})

	t.Run("wrong fixed-width value length errors like reflect", func(t *testing.T) {
		// int64 value with 4 bytes (invalid: must be 0 or 8).
		var data []byte
		data = append(data, count(1)...)
		data = append(data, withLen([]byte("k"))...)
		data = append(data, withLen([]byte{0, 0, 0, 1})...) // 4-byte int64 -> invalid

		var fast map[string]int64
		errFast := unmarshalMap(infoSI, data, &fast)
		var slow namedStrI64Map
		errSlow := unmarshalMap(infoSI, data, &slow)
		if (errFast == nil) != (errSlow == nil) {
			t.Fatalf("error disagreement: fast=%v reflect=%v", errFast, errSlow)
		}
	})
}

func BenchmarkUnmarshalMapStringString(b *testing.B) {
	for _, n := range []int{10, 100} {
		data := buildMapBytes(n,
			func(i int) []byte { return []byte(fmt.Sprintf("key-%04d", i)) },
			func(i int) []byte { return []byte(fmt.Sprintf("value-%04d", i)) },
		)
		info := CollectionType{
			NativeType: NativeType{proto: protoVersion4, typ: TypeMap},
			Key:        NativeType{proto: protoVersion4, typ: TypeVarchar},
			Elem:       NativeType{proto: protoVersion4, typ: TypeVarchar},
		}

		b.Run(fmt.Sprintf("fast/elems=%d", n), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				var dest map[string]string
				if err := unmarshalMap(info, data, &dest); err != nil {
					b.Fatal(err)
				}
			}
		})

		b.Run(fmt.Sprintf("reflect/elems=%d", n), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				var dest any
				if err := unmarshalMap(info, data, &dest); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkUnmarshalMapStringInt64(b *testing.B) {
	for _, n := range []int{10, 100} {
		data := buildMapBytes(n,
			func(i int) []byte { return []byte(fmt.Sprintf("key-%04d", i)) },
			func(i int) []byte { return int64Bytes(int64(i)) },
		)
		info := CollectionType{
			NativeType: NativeType{proto: protoVersion4, typ: TypeMap},
			Key:        NativeType{proto: protoVersion4, typ: TypeVarchar},
			Elem:       NativeType{proto: protoVersion4, typ: TypeBigInt},
		}

		b.Run(fmt.Sprintf("fast/elems=%d", n), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				var dest map[string]int64
				if err := unmarshalMap(info, data, &dest); err != nil {
					b.Fatal(err)
				}
			}
		})

		b.Run(fmt.Sprintf("reflect/elems=%d", n), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				var dest any
				if err := unmarshalMap(info, data, &dest); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkUnmarshalMapInt64Int64(b *testing.B) {
	for _, n := range []int{10, 100} {
		data := buildMapBytes(n,
			func(i int) []byte { return int64Bytes(int64(i)) },
			func(i int) []byte { return int64Bytes(int64(i * 100)) },
		)
		info := CollectionType{
			NativeType: NativeType{proto: protoVersion4, typ: TypeMap},
			Key:        NativeType{proto: protoVersion4, typ: TypeBigInt},
			Elem:       NativeType{proto: protoVersion4, typ: TypeBigInt},
		}

		b.Run(fmt.Sprintf("fast/elems=%d", n), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				var dest map[int64]int64
				if err := unmarshalMap(info, data, &dest); err != nil {
					b.Fatal(err)
				}
			}
		})

		b.Run(fmt.Sprintf("reflect/elems=%d", n), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				var dest any
				if err := unmarshalMap(info, data, &dest); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
