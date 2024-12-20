//go:build all || unit
// +build all unit

package gocql

import (
	"bytes"
	"encoding/binary"
	"gopkg.in/inf.v0"
	"math"
	"math/big"
	"net"
	"reflect"
	"strings"
	"testing"
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
		NativeType{proto: 2, typ: TypeBoolean},
		[]byte("\x00"),
		false,
		nil,
		nil,
	},
	{
		NativeType{proto: 2, typ: TypeBoolean},
		[]byte("\x01"),
		true,
		nil,
		nil,
	},
	{
		NativeType{proto: 2, typ: TypeDecimal},
		[]byte("\x00\x00\x00\x00\x00"),
		inf.NewDec(0, 0),
		nil,
		nil,
	},
	{
		NativeType{proto: 2, typ: TypeDecimal},
		[]byte("\x00\x00\x00\x00\x64"),
		inf.NewDec(100, 0),
		nil,
		nil,
	},
	{
		NativeType{proto: 2, typ: TypeDecimal},
		[]byte("\x00\x00\x00\x02\x19"),
		decimalize("0.25"),
		nil,
		nil,
	},
	{
		NativeType{proto: 2, typ: TypeDecimal},
		[]byte("\x00\x00\x00\x13\xD5\a;\x20\x14\xA2\x91"),
		decimalize("-0.0012095473475870063"), // From the iconara/cql-rb test suite
		nil,
		nil,
	},
	{
		NativeType{proto: 2, typ: TypeDecimal},
		[]byte("\x00\x00\x00\x13*\xF8\xC4\xDF\xEB]o"),
		decimalize("0.0012095473475870063"), // From the iconara/cql-rb test suite
		nil,
		nil,
	},
	{
		NativeType{proto: 2, typ: TypeDecimal},
		[]byte("\x00\x00\x00\x12\xF2\xD8\x02\xB6R\x7F\x99\xEE\x98#\x99\xA9V"),
		decimalize("-1042342234234.123423435647768234"), // From the iconara/cql-rb test suite
		nil,
		nil,
	},
	{
		NativeType{proto: 2, typ: TypeDecimal},
		[]byte("\x00\x00\x00\r\nJ\x04\"^\x91\x04\x8a\xb1\x18\xfe"),
		decimalize("1243878957943.1234124191998"), // From the datastax/python-driver test suite
		nil,
		nil,
	},
	{
		NativeType{proto: 2, typ: TypeDecimal},
		[]byte("\x00\x00\x00\x06\xe5\xde]\x98Y"),
		decimalize("-112233.441191"), // From the datastax/python-driver test suite
		nil,
		nil,
	},
	{
		NativeType{proto: 2, typ: TypeDecimal},
		[]byte("\x00\x00\x00\x14\x00\xfa\xce"),
		decimalize("0.00000000000000064206"), // From the datastax/python-driver test suite
		nil,
		nil,
	},
	{
		NativeType{proto: 2, typ: TypeDecimal},
		[]byte("\x00\x00\x00\x14\xff\x052"),
		decimalize("-0.00000000000000064206"), // From the datastax/python-driver test suite
		nil,
		nil,
	},
	{
		NativeType{proto: 2, typ: TypeDecimal},
		[]byte("\xff\xff\xff\x9c\x00\xfa\xce"),
		inf.NewDec(64206, -100), // From the datastax/python-driver test suite
		nil,
		nil,
	},
	{
		CollectionType{
			NativeType: NativeType{proto: 2, typ: TypeList},
			Elem:       NativeType{proto: 2, typ: TypeInt},
		},
		[]byte("\x00\x02\x00\x04\x00\x00\x00\x01\x00\x04\x00\x00\x00\x02"),
		[]int{1, 2},
		nil,
		nil,
	},
	{
		CollectionType{
			NativeType: NativeType{proto: 2, typ: TypeList},
			Elem:       NativeType{proto: 2, typ: TypeInt},
		},
		[]byte("\x00\x02\x00\x04\x00\x00\x00\x01\x00\x04\x00\x00\x00\x02"),
		[2]int{1, 2},
		nil,
		nil,
	},
	{
		CollectionType{
			NativeType: NativeType{proto: 2, typ: TypeSet},
			Elem:       NativeType{proto: 2, typ: TypeInt},
		},
		[]byte("\x00\x02\x00\x04\x00\x00\x00\x01\x00\x04\x00\x00\x00\x02"),
		[]int{1, 2},
		nil,
		nil,
	},
	{
		CollectionType{
			NativeType: NativeType{proto: 2, typ: TypeSet},
			Elem:       NativeType{proto: 2, typ: TypeInt},
		},
		[]byte{0, 0}, // encoding of a list should always include the size of the collection
		[]int{},
		nil,
		nil,
	},
	{
		CollectionType{
			NativeType: NativeType{proto: 2, typ: TypeMap},
			Key:        NativeType{proto: 2, typ: TypeVarchar},
			Elem:       NativeType{proto: 2, typ: TypeInt},
		},
		[]byte("\x00\x01\x00\x03foo\x00\x04\x00\x00\x00\x01"),
		map[string]int{"foo": 1},
		nil,
		nil,
	},
	{
		CollectionType{
			NativeType: NativeType{proto: 2, typ: TypeMap},
			Key:        NativeType{proto: 2, typ: TypeVarchar},
			Elem:       NativeType{proto: 2, typ: TypeInt},
		},
		[]byte{0, 0},
		map[string]int{},
		nil,
		nil,
	},
	{
		CollectionType{
			NativeType: NativeType{proto: 2, typ: TypeMap},
			Key:        NativeType{proto: 2, typ: TypeVarchar},
			Elem:       NativeType{proto: 2, typ: TypeBlob},
		},
		[]byte("\x00\x01\x00\x03foo\x00\x05\x01\x02\x03\x04\x05"),
		map[string]interface{}{
			"foo": []byte{0x01, 0x02, 0x03, 0x04, 0x05},
		},
		nil,
		nil,
	},
	{
		CollectionType{
			NativeType: NativeType{proto: 2, typ: TypeList},
			Elem:       NativeType{proto: 2, typ: TypeVarchar},
		},
		bytes.Join([][]byte{
			[]byte("\x00\x01\xFF\xFF"),
			bytes.Repeat([]byte("X"), math.MaxUint16)}, []byte("")),
		[]string{strings.Repeat("X", math.MaxUint16)},
		nil,
		nil,
	},
	{
		CollectionType{
			NativeType: NativeType{proto: 2, typ: TypeMap},
			Key:        NativeType{proto: 2, typ: TypeVarchar},
			Elem:       NativeType{proto: 2, typ: TypeVarchar},
		},
		bytes.Join([][]byte{
			[]byte("\x00\x01\xFF\xFF"),
			bytes.Repeat([]byte("X"), math.MaxUint16),
			[]byte("\xFF\xFF"),
			bytes.Repeat([]byte("Y"), math.MaxUint16)}, []byte("")),
		map[string]string{
			strings.Repeat("X", math.MaxUint16): strings.Repeat("Y", math.MaxUint16),
		},
		nil,
		nil,
	},
	{
		NativeType{proto: 2, typ: TypeInet},
		[]byte("\x7F\x00\x00\x01"),
		net.ParseIP("127.0.0.1").To4(),
		nil,
		nil,
	},
	{
		NativeType{proto: 2, typ: TypeInet},
		[]byte("\xFF\xFF\xFF\xFF"),
		net.ParseIP("255.255.255.255").To4(),
		nil,
		nil,
	},
	{
		NativeType{proto: 2, typ: TypeInet},
		[]byte("\x7F\x00\x00\x01"),
		"127.0.0.1",
		nil,
		nil,
	},
	{
		NativeType{proto: 2, typ: TypeInet},
		[]byte("\xFF\xFF\xFF\xFF"),
		"255.255.255.255",
		nil,
		nil,
	},
	{
		NativeType{proto: 2, typ: TypeInet},
		[]byte("\x21\xDA\x00\xd3\x00\x00\x2f\x3b\x02\xaa\x00\xff\xfe\x28\x9c\x5a"),
		"21da:d3:0:2f3b:2aa:ff:fe28:9c5a",
		nil,
		nil,
	},
	{
		NativeType{proto: 2, typ: TypeInet},
		[]byte("\xfe\x80\x00\x00\x00\x00\x00\x00\x02\x02\xb3\xff\xfe\x1e\x83\x29"),
		"fe80::202:b3ff:fe1e:8329",
		nil,
		nil,
	},
	{
		NativeType{proto: 2, typ: TypeInet},
		[]byte("\x21\xDA\x00\xd3\x00\x00\x2f\x3b\x02\xaa\x00\xff\xfe\x28\x9c\x5a"),
		net.ParseIP("21da:d3:0:2f3b:2aa:ff:fe28:9c5a"),
		nil,
		nil,
	},
	{
		NativeType{proto: 2, typ: TypeInet},
		[]byte("\xfe\x80\x00\x00\x00\x00\x00\x00\x02\x02\xb3\xff\xfe\x1e\x83\x29"),
		net.ParseIP("fe80::202:b3ff:fe1e:8329"),
		nil,
		nil,
	},
	{
		NativeType{proto: 2, typ: TypeBoolean},
		[]byte("\x00"),
		func() *bool {
			b := false
			return &b
		}(),
		nil,
		nil,
	},
	{
		NativeType{proto: 2, typ: TypeBoolean},
		[]byte("\x01"),
		func() *bool {
			b := true
			return &b
		}(),
		nil,
		nil,
	},
	{
		NativeType{proto: 2, typ: TypeBoolean},
		[]byte(nil),
		(*bool)(nil),
		nil,
		nil,
	},
	{
		NativeType{proto: 2, typ: TypeInet},
		[]byte("\x7F\x00\x00\x01"),
		func() *net.IP {
			ip := net.ParseIP("127.0.0.1").To4()
			return &ip
		}(),
		nil,
		nil,
	},
	{
		NativeType{proto: 2, typ: TypeInet},
		[]byte(nil),
		(*net.IP)(nil),
		nil,
		nil,
	},
	{
		CollectionType{
			NativeType: NativeType{proto: 2, typ: TypeList},
			Elem:       NativeType{proto: 2, typ: TypeInt},
		},
		[]byte("\x00\x02\x00\x04\x00\x00\x00\x01\x00\x04\x00\x00\x00\x02"),
		func() *[]int {
			l := []int{1, 2}
			return &l
		}(),
		nil,
		nil,
	},
	{
		CollectionType{
			NativeType: NativeType{proto: 3, typ: TypeList},
			Elem:       NativeType{proto: 3, typ: TypeInt},
		},
		[]byte("\x00\x00\x00\x02\x00\x00\x00\x04\x00\x00\x00\x01\x00\x00\x00\x04\x00\x00\x00\x02"),
		func() *[]int {
			l := []int{1, 2}
			return &l
		}(),
		nil,
		nil,
	},
	{
		CollectionType{
			NativeType: NativeType{proto: 2, typ: TypeList},
			Elem:       NativeType{proto: 2, typ: TypeInt},
		},
		[]byte(nil),
		(*[]int)(nil),
		nil,
		nil,
	},
	{
		CollectionType{
			NativeType: NativeType{proto: 2, typ: TypeMap},
			Key:        NativeType{proto: 2, typ: TypeVarchar},
			Elem:       NativeType{proto: 2, typ: TypeInt},
		},
		[]byte("\x00\x01\x00\x03foo\x00\x04\x00\x00\x00\x01"),
		func() *map[string]int {
			m := map[string]int{"foo": 1}
			return &m
		}(),
		nil,
		nil,
	},
	{
		CollectionType{
			NativeType: NativeType{proto: 2, typ: TypeMap},
			Key:        NativeType{proto: 2, typ: TypeVarchar},
			Elem:       NativeType{proto: 2, typ: TypeInt},
		},
		[]byte(nil),
		(*map[string]int)(nil),
		nil,
		nil,
	},
	{
		NativeType{proto: 2, typ: TypeTinyInt},
		[]byte("\x7f"),
		127, // math.MaxInt8
		nil,
		nil,
	},
	{
		NativeType{proto: 2, typ: TypeTinyInt},
		[]byte("\x7f"),
		"127", // math.MaxInt8
		nil,
		nil,
	},
	{
		NativeType{proto: 2, typ: TypeTinyInt},
		[]byte("\x01"),
		int16(1),
		nil,
		nil,
	},
	{
		NativeType{proto: 2, typ: TypeTinyInt},
		[]byte("\xff"),
		int16(-1),
		nil,
		nil,
	},
	{
		NativeType{proto: 2, typ: TypeTinyInt},
		[]byte("\xff"),
		uint8(255),
		nil,
		nil,
	},
	{
		NativeType{proto: 2, typ: TypeTinyInt},
		[]byte("\xff"),
		uint64(255),
		nil,
		nil,
	},
	{
		NativeType{proto: 2, typ: TypeTinyInt},
		[]byte("\xff"),
		uint32(255),
		nil,
		nil,
	},
	{
		NativeType{proto: 2, typ: TypeTinyInt},
		[]byte("\xff"),
		uint16(255),
		nil,
		nil,
	},
	{
		NativeType{proto: 2, typ: TypeTinyInt},
		[]byte("\xff"),
		uint(255),
		nil,
		nil,
	},
	{
		NativeType{proto: 2, typ: TypeTinyInt},
		[]byte("\xff"),
		AliasUint8(255),
		nil,
		nil,
	},
	{
		NativeType{proto: 2, typ: TypeTinyInt},
		[]byte("\xff"),
		AliasUint64(255),
		nil,
		nil,
	},
	{
		NativeType{proto: 2, typ: TypeTinyInt},
		[]byte("\xff"),
		AliasUint32(255),
		nil,
		nil,
	},
	{
		NativeType{proto: 2, typ: TypeTinyInt},
		[]byte("\xff"),
		AliasUint16(255),
		nil,
		nil,
	},
	{
		NativeType{proto: 2, typ: TypeTinyInt},
		[]byte("\xff"),
		AliasUint(255),
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
			NativeType: NativeType{proto: 3, typ: TypeList},
			Elem:       NativeType{proto: 3, typ: TypeInt},
		},
		[]byte("\x00\x00\x00\x02\x00\x00\x00\x04\x00\x00"), // truncated data
		func() *[]int {
			l := []int{1, 2}
			return &l
		}(),
		unmarshalErrorf("unmarshal list: unexpected eof"),
	},
	{
		CollectionType{
			NativeType: NativeType{proto: 2, typ: TypeMap},
			Key:        NativeType{proto: 2, typ: TypeVarchar},
			Elem:       NativeType{proto: 2, typ: TypeInt},
		},
		[]byte("\x00\x01\x00\x03fo"),
		map[string]int{"foo": 1},
		unmarshalErrorf("unmarshal map: unexpected eof"),
	},
	{
		CollectionType{
			NativeType: NativeType{proto: 2, typ: TypeMap},
			Key:        NativeType{proto: 2, typ: TypeVarchar},
			Elem:       NativeType{proto: 2, typ: TypeInt},
		},
		[]byte("\x00\x01\x00\x03foo\x00\x04\x00\x00"),
		map[string]int{"foo": 1},
		unmarshalErrorf("unmarshal map: unexpected eof"),
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
	typeInfoV2 := CollectionType{
		NativeType: NativeType{proto: 2, typ: TypeList},
		Elem:       NativeType{proto: 2, typ: TypeVarchar},
	}
	typeInfoV3 := CollectionType{
		NativeType: NativeType{proto: 3, typ: TypeList},
		Elem:       NativeType{proto: 3, typ: TypeVarchar},
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
			typeInfo: typeInfoV2,
			input:    []*string{&valueA},
			expected: []*string{&valueA},
		},
		{
			typeInfo: typeInfoV2,
			input:    []*string{&valueA, &valueB},
			expected: []*string{&valueA, &valueB},
		},
		{
			typeInfo: typeInfoV2,
			input:    []*string{&valueA, &valueEmpty, &valueB},
			expected: []*string{&valueA, &valueEmpty, &valueB},
		},
		{
			typeInfo: typeInfoV2,
			input:    []*string{&valueEmpty},
			expected: []*string{&valueEmpty},
		},
		{
			// nil values are marshalled to empty values for protocol < 3
			typeInfo: typeInfoV2,
			input:    []*string{nil},
			expected: []*string{&valueEmpty},
		},
		{
			typeInfo: typeInfoV2,
			input:    []*string{&valueA, nil, &valueB},
			expected: []*string{&valueA, &valueEmpty, &valueB},
		},
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
	for _, lookupTest := range typeLookupTest {
		testType(t, lookupTest.TypeName, lookupTest.ExpectedType)
	}
}

type MyPointerMarshaler struct{}

func (m *MyPointerMarshaler) MarshalCQL(_ TypeInfo) ([]byte, error) {
	return []byte{42}, nil
}

func TestMarshalPointer(t *testing.T) {
	m := &MyPointerMarshaler{}
	typ := NativeType{proto: 2, typ: TypeInt}

	data, err := Marshal(typ, m)

	if err != nil {
		t.Errorf("Pointer marshaling failed. Error: %s", err)
	}
	if len(data) != 1 || data[0] != 42 {
		t.Errorf("Pointer marshaling failed. Expected %+v, got %+v", []byte{42}, data)
	}
}

func TestMarshalTuple(t *testing.T) {
	info := TupleTypeInfo{
		NativeType: NativeType{proto: 3, typ: TypeTuple},
		Elems: []TypeInfo{
			NativeType{proto: 3, typ: TypeVarchar},
			NativeType{proto: 3, typ: TypeVarchar},
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

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			data, err := Marshal(info, tc.value)
			if err != nil {
				t.Errorf("marshalTest: %v", err)
				return
			}

			if !bytes.Equal(data, tc.expected) {
				t.Errorf("marshalTest: expected %x (%v), got %x (%v)",
					tc.expected, decBigInt(tc.expected), data, decBigInt(data))
				return
			}

			err = Unmarshal(info, data, tc.checkValue)
			if err != nil {
				t.Errorf("unmarshalTest: %v", err)
				return
			}

			tc.check(t, tc.checkValue)
		})
	}
}

func TestUnmarshalTuple(t *testing.T) {
	info := TupleTypeInfo{
		NativeType: NativeType{proto: 3, typ: TypeTuple},
		Elems: []TypeInfo{
			NativeType{proto: 3, typ: TypeVarchar},
			NativeType{proto: 3, typ: TypeVarchar},
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
	typeInfo := UDTTypeInfo{NativeType{proto: 3, typ: TypeUDT}, "", "xyz", []UDTField{
		{Name: "x", Type: NativeType{proto: 3, typ: TypeInt}},
		{Name: "y", Type: NativeType{proto: 3, typ: TypeInt}},
		{Name: "z", Type: NativeType{proto: 3, typ: TypeInt}},
	}}

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
	typeInfo := UDTTypeInfo{NativeType{proto: 3, typ: TypeUDT}, "", "xyz", []UDTField{
		{Name: "x", Type: NativeType{proto: 3, typ: TypeInt}},
		{Name: "y", Type: NativeType{proto: 3, typ: TypeInt}},
		{Name: "z", Type: NativeType{proto: 3, typ: TypeInt}},
	}}

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
		data, err := Marshal(NativeType{proto: 3, typ: typ}, nil)
		if err != nil {
			t.Errorf("unable to marshal nil %v: %v\n", typ, err)
		} else if data != nil {
			t.Errorf("expected to get nil byte for nil %v got % X", typ, data)
		}
	}
}

func TestUnmarshalInetCopyBytes(t *testing.T) {
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
	listV2 := CollectionType{
		NativeType: NativeType{proto: 2, typ: TypeList},
		Elem:       NativeType{proto: 2, typ: TypeVarchar},
	}
	listV3 := CollectionType{
		NativeType: NativeType{proto: 3, typ: TypeList},
		Elem:       NativeType{proto: 3, typ: TypeVarchar},
	}

	tests := []struct {
		name         string
		info         CollectionType
		data         []byte
		isError      bool
		expectedSize int
	}{
		{
			name:    "short read 0 proto 2",
			info:    listV2,
			data:    []byte{},
			isError: true,
		},
		{
			name:    "short read 1 proto 2",
			info:    listV2,
			data:    []byte{0x01},
			isError: true,
		},
		{
			name:         "good read proto 2",
			info:         listV2,
			data:         []byte{0x01, 0x38},
			expectedSize: 0x0138,
		},
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
	info := UDTTypeInfo{
		NativeType: NativeType{proto: 4, typ: TypeUDT},
		Name:       "myudt",
		KeySpace:   "myks",
		Elements: []UDTField{
			{
				Name: "first",
				Type: NativeType{proto: 4, typ: TypeAscii},
			},
			{
				Name: "second",
				Type: NativeType{proto: 4, typ: TypeSmallInt},
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
