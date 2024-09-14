package gocql

import (
	"errors"
	"fmt"
	"gopkg.in/inf.v0"
	"math/big"
	"net"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestMarshalAll(t *testing.T) {
	t.Skip("for manual use only")

	TestMarshalBoolean(t)
	TestMarshalBooleanMustFail(t)

	TestMarshalTinyint(t)
	TestMarshalTinyintMustFail(t)

	TestMarshalSmallint(t)
	TestMarshalSmallintMustFail(t)

	TestMarshalInt(t)
	TestMarshalIntMustFail(t)

	TestMarshalBigInt(t)
	TestMarshalBigIntMustFail(t)

	TestMarshalCounter(t)
	TestMarshalCounterMustFail(t)

	TestMarshalVarInt(t)
	TestMarshalVarIntMustFail(t)

	TestMarshalFloat(t)
	TestMarshalFloatMustFail(t)

	TestMarshalDouble(t)
	TestMarshalDoubleMustFail(t)

	TestMarshalDecimal(t)
	TestMarshalDecimalMustFail(t)

	TestMarshalTexts(t)

	TestMarshalAscii(t)
	TestMarshalAsciiMustFail(t)

	TestMarshalUUIDs(t)
	TestMarshalUUIDsMustFail(t)

	TestMarshalsInet(t)
	TestMarshalsInetMustFail(t)

	TestMarshalsTime(t)
	TestMarshalTimeMustFail(t)

	TestMarshalsDate(t)
	TestMarshalDateMustFail(t)

	TestMarshalsDuration(t)
	TestMarshalDurationMustFail(t)

	TestMarshalSetListV2(t)
	TestMarshalSetListV2MustFail(t)

	TestMarshalSetListV4(t)
	TestMarshalSetListV4MustFail(t)

	TestMarshalMapV2(t)
	TestMarshalMapV2MustFail(t)

	TestMarshalMapV4(t)
	TestMarshalMapV4MustFail(t)

	TestMarshalUDT(t)
	TestMarshalUDTMustFail(t)

	TestMarshalsTuple(t)
	TestMarshalTupleMustFail(t)
}

func TestMarshalCustomString(t *testing.T) {
	t.Skip("There is an unsolved issue https://github.com/scylladb/gocql/issues/243")
	// TODO: after the issue is fixed, the relevant test cases should be added to the marshal tests.
	type cstring string

	types := []TypeInfo{
		NativeType{proto: 2, typ: TypeTinyInt},
		NativeType{proto: 2, typ: TypeSmallInt},
		NativeType{proto: 2, typ: TypeInt},
		NativeType{proto: 2, typ: TypeBigInt},
		NativeType{proto: 2, typ: TypeCounter},
		NativeType{proto: 2, typ: TypeVarint},
		NativeType{proto: 2, typ: TypeInet},
		NativeType{proto: 2, typ: TypeDate},
		NativeType{proto: 2, typ: TypeUUID},
		NativeType{proto: 2, typ: TypeTimeUUID},
		NativeType{proto: 2, typ: TypeDuration},
	}

	errs := make([]string, 0)
	for _, tp := range types {
		var val cstring
		data := make([]byte, 0)
		if err := Unmarshal(tp, data, &val); err != nil {
			errs = append(errs, err.Error())
		}
	}
	t.Errorf("\nUnmarshaling of %d cases, have %d errors:\n%s", len(types), len(errs), strings.Join(errs, "\n"))

	errs = make([]string, 0)
	for _, tp := range types {
		var val cstring
		if _, err := Marshal(tp, &val); err != nil {
			errs = append(errs, err.Error())
		}
	}
	if len(errs) > 0 {
		t.Errorf("\nMarshaling of %d cases, have %d errors:\n%s", len(types), len(errs), strings.Join(errs, "\n"))
	}
}

func TestMarshalBigInts(t *testing.T) {
	t.Skip("There is an unsolved issue https://github.com/scylladb/gocql/issues/244")
	// TODO: after the issue is fixed, the relevant test cases should be added to the marshal tests.
	types := []TypeInfo{
		NativeType{proto: 2, typ: TypeTinyInt},
		NativeType{proto: 2, typ: TypeSmallInt},
		NativeType{proto: 2, typ: TypeInt},
	}

	errs := make([]string, 0)
	for _, tp := range types {
		var val big.Int
		data := make([]byte, 0)
		if err := Unmarshal(tp, data, &val); err != nil {
			errs = append(errs, err.Error())
		}
	}
	t.Errorf("\nUnmarshaling of %d cases, have %d errors:\n%s", len(types), len(errs), strings.Join(errs, "\n"))

	errs = make([]string, 0)
	for _, tp := range types {
		val := big.NewInt(0)
		if _, err := Marshal(tp, val); err != nil {
			errs = append(errs, err.Error())
		}
	}
	if len(errs) > 0 {
		t.Errorf("\nMarshaling of %d cases, have %d errors:\n%s", len(types), len(errs), strings.Join(errs, "\n"))
	}
}

func TestUnmarshalBiggerData(t *testing.T) {
	t.Skip("There is an unsolved issue https://github.com/scylladb/gocql/issues/246")
	// TODO: after the issue is fixed, the relevant test cases should be added to the marshal tests.
	types := []TypeInfo{
		NativeType{proto: 2, typ: TypeBoolean},
		NativeType{proto: 2, typ: TypeTinyInt},
		NativeType{proto: 2, typ: TypeSmallInt},
		NativeType{proto: 2, typ: TypeInt},
		NativeType{proto: 2, typ: TypeBigInt},
		NativeType{proto: 2, typ: TypeCounter},
		NativeType{proto: 2, typ: TypeDouble},
		NativeType{proto: 2, typ: TypeFloat},
		NativeType{proto: 2, typ: TypeInet},
		NativeType{proto: 2, typ: TypeTime},
		NativeType{proto: 2, typ: TypeDate},
		NativeType{proto: 2, typ: TypeTimestamp},
		NativeType{proto: 2, typ: TypeVarint},

		// From cassandra protocol description:
		// 5.8 duration
		//
		//  A duration is composed of 3 signed variable length integers ([vint]s).
		//  The first [vint] represents a number of months, the second [vint] represents
		//  a number of days, and the last [vint] represents a number of nanoseconds.
		//  The number of months and days must be valid 32 bits integers whereas the
		//  number of nanoseconds must be a valid 64 bits integer.
		//  A duration can either be positive or negative. If a duration is positive
		//  all the integers must be positive or zero. If a duration is
		//  negative all the numbers must be negative or zero.
		NativeType{proto: 2, typ: TypeDuration},
	}

	noErrs := make([]string, 0)
	for _, tp := range types {
		// This data case overflows all tested types.
		// Special for TypeDuration and TypeVarint this data case composed of 3 [vint]s :
		// 1: 4294967296,
		// 2: 4294967296,
		// 3: 36893488147419103228,
		data := []byte{1, 0, 0, 0, 0, 1, 0, 0, 0, 0, 255, 255, 255, 255, 255, 255, 255, 252}
		val := tp.New()
		if err := Unmarshal(tp, data, val); err != nil {
			continue
		}
		if val != nil {
			val = dereference(val)
		}
		noErrs = append(noErrs, fmt.Sprintf("type:<%s> - value: <%v>", tp.Type().String(), val))
	}
	if len(noErrs) > 0 {
		t.Errorf("\nOut of %d test cases, %d cases without errors:\n%s", len(types), len(noErrs), strings.Join(noErrs, "\n"))
	}
}

func TestUnmarshalSmallerData(t *testing.T) {
	t.Skip("There is an unsolved issue https://github.com/scylladb/gocql/issues/252")
	// TODO: after the issue is fixed, the relevant test cases should be added to the marshal tests.
	types := []TypeInfo{
		NativeType{proto: 2, typ: TypeSmallInt},
		NativeType{proto: 2, typ: TypeInt},
		NativeType{proto: 2, typ: TypeBigInt},
		NativeType{proto: 2, typ: TypeCounter},
		NativeType{proto: 2, typ: TypeDouble},
		NativeType{proto: 2, typ: TypeFloat},
		NativeType{proto: 2, typ: TypeInet},
		NativeType{proto: 2, typ: TypeTime},
		NativeType{proto: 2, typ: TypeDate},
		NativeType{proto: 2, typ: TypeTimestamp},
		NativeType{proto: 2, typ: TypeDuration},
		NativeType{proto: 2, typ: TypeDecimal},
		NativeType{proto: 2, typ: TypeUUID},
		NativeType{proto: 2, typ: TypeTimeUUID},
	}

	errPanic := fmt.Errorf("was panic")

	wrappedUnmarshal := func(info TypeInfo, data []byte, value interface{}) (err error) {
		defer func() {
			if p := recover(); p != nil {
				err = errors.Join(errPanic, p.(error))
			}
		}()
		return Unmarshal(info, data, value)
	}

	noErrs := make([]string, 0)
	panics := make([]string, 0)
	for _, tp := range types {
		data := []byte{0}
		val := tp.New()
		if err := wrappedUnmarshal(tp, data, val); err != nil {
			if errors.Is(err, errPanic) {
				panics = append(panics, fmt.Sprintf("type:<%s> - panic: <%v>", tp.Type().String(), err))
			}
			continue
		}
		if val != nil {
			val = dereference(val)
		}
		noErrs = append(noErrs, fmt.Sprintf("type:<%s> - value: <%v>", tp.Type().String(), val))
	}
	if len(noErrs) > 0 {
		t.Errorf("\nOut of %d test cases, %d cases without errors:\n%s", len(types), len(noErrs), strings.Join(noErrs, "\n"))
	}
	if len(panics) > 0 {
		t.Errorf("\nOut of %d test cases, %d cases with panic:\n%s", len(types), len(panics), strings.Join(panics, "\n"))
	}
}

func TestUnmarshalIntoSmallerTypes(t *testing.T) {
	t.Skip("There is an unsolved issue https://github.com/scylladb/gocql/issues/253")
	// TODO: after the issue is fixed, the relevant test cases should be added to the marshal tests.

	type tCase struct {
		Type  Type
		Data  []byte
		Value interface{}
	}

	tCases := []tCase{
		{Type: TypeSmallInt, Data: []byte("\xff\xff"), Value: new(int8)},
		{Type: TypeSmallInt, Data: []byte("\xff\xff"), Value: new(uint8)},
		{Type: TypeInt, Data: []byte("\xff\xff\xff\xff"), Value: new(int8)},
		{Type: TypeInt, Data: []byte("\xff\xff\xff\xff"), Value: new(uint8)},
		{Type: TypeInt, Data: []byte("\xff\xff\xff\xff"), Value: new(int16)},
		{Type: TypeInt, Data: []byte("\xff\xff\xff\xff"), Value: new(uint16)},
		{Type: TypeBigInt, Data: []byte("\xff\xff\xff\xff\xff\xff\xff\xff"), Value: new(int8)},
		{Type: TypeBigInt, Data: []byte("\xff\xff\xff\xff\xff\xff\xff\xff"), Value: new(uint8)},
		{Type: TypeBigInt, Data: []byte("\xff\xff\xff\xff\xff\xff\xff\xff"), Value: new(int16)},
		{Type: TypeBigInt, Data: []byte("\xff\xff\xff\xff\xff\xff\xff\xff"), Value: new(uint16)},
		{Type: TypeBigInt, Data: []byte("\xff\xff\xff\xff\xff\xff\xff\xff"), Value: new(int32)},
		{Type: TypeBigInt, Data: []byte("\xff\xff\xff\xff\xff\xff\xff\xff"), Value: new(uint32)},
		{Type: TypeCounter, Data: []byte("\xff\xff\xff\xff\xff\xff\xff\xff"), Value: new(int8)},
		{Type: TypeCounter, Data: []byte("\xff\xff\xff\xff\xff\xff\xff\xff"), Value: new(uint8)},
		{Type: TypeCounter, Data: []byte("\xff\xff\xff\xff\xff\xff\xff\xff"), Value: new(int16)},
		{Type: TypeCounter, Data: []byte("\xff\xff\xff\xff\xff\xff\xff\xff"), Value: new(uint16)},
		{Type: TypeCounter, Data: []byte("\xff\xff\xff\xff\xff\xff\xff\xff"), Value: new(int32)},
		{Type: TypeCounter, Data: []byte("\xff\xff\xff\xff\xff\xff\xff\xff"), Value: new(uint32)},
		{Type: TypeVarint, Data: []byte{255, 255, 255, 255, 255, 255, 255, 252}, Value: new(int8)},
		{Type: TypeVarint, Data: []byte{255, 255, 255, 255, 255, 255, 255, 252}, Value: new(uint8)},
		{Type: TypeVarint, Data: []byte{255, 255, 255, 255, 255, 255, 255, 252}, Value: new(int16)},
		{Type: TypeVarint, Data: []byte{255, 255, 255, 255, 255, 255, 255, 252}, Value: new(uint16)},
		{Type: TypeVarint, Data: []byte{255, 255, 255, 255, 255, 255, 255, 252}, Value: new(int32)},
		{Type: TypeVarint, Data: []byte{255, 255, 255, 255, 255, 255, 255, 252}, Value: new(uint32)},
		{Type: TypeVarint, Data: []byte{255, 255, 255, 255, 255, 255, 255, 252}, Value: new(int64)},
	}

	out := make([]string, 0)
	for _, c := range tCases {
		tp := NativeType{proto: 2, typ: c.Type}
		if err := Unmarshal(tp, c.Data, c.Value); err == nil {
			rv := reflect.ValueOf(c.Value)
			if rv.Kind() == reflect.Ptr {
				rv = rv.Elem()
			}
			out = append(out, fmt.Sprintf("type:<%s><%T>\t-\tvalue: <%#v>", tp.Type(), c.Value, rv.Interface()))
		}
	}

	if len(out) != 0 {
		t.Errorf("\nOut of %d test cases, %d cases without errors::\n%s", len(tCases), len(out), strings.Join(out, "\n"))
	}
}

func TestMarshalZeroValues(t *testing.T) {
	t.Skip("There is an unsolved issue https://github.com/scylladb/gocql/issues/250")
	// TODO: after the issue is fixed, the relevant test cases should be added to the marshal tests.

	type tCase struct {
		Type  TypeInfo
		Value interface{}
	}

	tCases := []tCase{
		{Type: NativeType{proto: 2, typ: TypeBoolean}, Value: false},
		{Type: NativeType{proto: 2, typ: TypeTinyInt}, Value: int(0)},
		{Type: NativeType{proto: 2, typ: TypeSmallInt}, Value: int(0)},
		{Type: NativeType{proto: 2, typ: TypeInt}, Value: int(0)},
		{Type: NativeType{proto: 2, typ: TypeBigInt}, Value: int(0)},
		{Type: NativeType{proto: 2, typ: TypeVarint}, Value: int(0)},
		{Type: NativeType{proto: 2, typ: TypeFloat}, Value: float32(0)},
		{Type: NativeType{proto: 2, typ: TypeDouble}, Value: float64(0)},
		{Type: NativeType{proto: 2, typ: TypeDecimal}, Value: inf.Dec{}},
		{Type: NativeType{proto: 2, typ: TypeInet}, Value: make(net.IP, 0)},
		{Type: NativeType{proto: 2, typ: TypeUUID}, Value: UUID{}},
		{Type: NativeType{proto: 2, typ: TypeTimeUUID}, Value: UUID{}},
		{Type: NativeType{proto: 2, typ: TypeTime}, Value: int64(0)},
		{Type: NativeType{proto: 2, typ: TypeDate}, Value: int64(0)},
		{Type: NativeType{proto: 2, typ: TypeDuration}, Value: time.Duration(0)},
		{Type: NativeType{proto: 2, typ: TypeTimestamp}, Value: int64(0)},
		{Type: NativeType{proto: 2, typ: TypeText}, Value: ""},
		{Type: NativeType{proto: 2, typ: TypeAscii}, Value: ""},
		{Type: NativeType{proto: 2, typ: TypeBlob}, Value: ""},
		{Type: NativeType{proto: 2, typ: TypeVarchar}, Value: ""},

		{
			Type: CollectionType{
				NativeType: NativeType{proto: 2, typ: TypeList},
				Elem:       NativeType{proto: 2, typ: TypeSmallInt},
			},
			Value: make([]byte, 0)},
		{
			Type: CollectionType{
				NativeType: NativeType{proto: 2, typ: TypeSet},
				Elem:       NativeType{proto: 2, typ: TypeSmallInt},
			},
			Value: make([]byte, 0)},
		{
			Type: CollectionType{
				NativeType: NativeType{proto: 2, typ: TypeMap},
				Key:        NativeType{proto: 2, typ: TypeSmallInt},
				Elem:       NativeType{proto: 2, typ: TypeSmallInt},
			},
			Value: make(map[int]int)},
		{
			Type: UDTTypeInfo{
				NativeType: NativeType{proto: 3, typ: TypeUDT},
				KeySpace:   "",
				Name:       "test_udt",
				Elements: []UDTField{
					{Name: "1", Type: NativeType{proto: 2, typ: TypeSmallInt}},
				},
			},
			Value: map[string]interface{}{},
		},
		{
			Type: TupleTypeInfo{
				NativeType: NativeType{proto: 3, typ: TypeTuple},
				Elems: []TypeInfo{
					NativeType{proto: 2, typ: TypeSmallInt},
				},
			},
			Value: []interface{}{},
		},
	}

	errs := make([]string, 0)
	mData := make([]string, 0)
	for _, c := range tCases {
		data, err := Marshal(c.Type, c.Value)
		if err != nil {
			errs = append(errs, fmt.Sprintf("type:<%s>\t-\terror: %s", c.Type.Type(), err.Error()))
		} else if len(data) != 0 {
			mData = append(mData, fmt.Sprintf("type:<%s>\t-\tdata: <%#v>", c.Type.Type(), data))
		}
	}

	if len(errs) != 0 {
		t.Errorf("\nOut of %d cases, have %d errors:\n%s", len(tCases), len(errs), strings.Join(errs, "\n"))
	}
	if len(mData) != 0 {
		t.Errorf("\nOut of %d cases, have %d a non zero data:\n%s", len(tCases), len(mData), strings.Join(mData, "\n"))
	}
}
