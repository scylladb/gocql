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
	Value          interface{}
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
	Value          interface{}
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
		resultList := []interface{}{}
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
		value      interface{}
		checkValue interface{}
		check      func(*testing.T, interface{})
	}{
		{
			name:       "interface-slice:two-strings",
			expected:   []byte("\x00\x00\x00\x03foo\x00\x00\x00\x03bar"),
			value:      []interface{}{"foo", "bar"},
			checkValue: []interface{}{&s1, &s2},
			check: func(t *testing.T, v interface{}) {
				checkString(t, "foo", *s1)
				checkString(t, "bar", *s2)
			},
		},
		{
			name:       "interface-slice:one-string-one-nil-string",
			expected:   []byte("\x00\x00\x00\x03foo\xff\xff\xff\xff"),
			value:      []interface{}{"foo", nil},
			checkValue: []interface{}{&s1, &s2},
			check: func(t *testing.T, v interface{}) {
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
			check: func(t *testing.T, v interface{}) {
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
			check: func(t *testing.T, v interface{}) {
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
			check: func(t *testing.T, v interface{}) {
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
			check: func(t *testing.T, v interface{}) {
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
		value := map[string]interface{}{
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
		value := map[string]interface{}{
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
		value := map[string]interface{}{
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
			size, _, err := readCollectionSize(test.info, test.data)
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
	value := map[string]interface{}{}
	expectedErr := unmarshalErrorf("can not unmarshal into non-pointer map[string]interface {}")

	if err := Unmarshal(info, data, value); err != expectedErr {
		t.Errorf("(%v=>%T): %#v returned error %#v, want %#v.",
			info, value, value, err, expectedErr)
	}
}

// TestUnmarshalListIntoInterface tests that lists can be unmarshaled into *interface{}
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

	var result interface{}
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

// TestUnmarshalMapIntoInterface tests that maps can be unmarshaled into *interface{}
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

	var result interface{}
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
// can be unmarshaled into *interface{} (issue #692)
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
			NativeType: NativeType{proto: protoVersion4, typ: TypeCustom, custom: "org.apache.cassandra.db.marshal.VectorType"},
			SubType:    NativeType{proto: protoVersion4, typ: TypeFloat},
			Dimensions: 2,
		},
	}

	var result interface{}
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
