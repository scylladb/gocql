package gocql

import (
	"testing"

	"github.com/gocql/gocql/marshal/tests/gen/mod"
	"github.com/gocql/gocql/marshal/tests/gen/mustfail/marshal"
	"github.com/gocql/gocql/marshal/tests/gen/mustfail/unmarshal"
)

func TestMarshalIntMustFail(t *testing.T) {
	tType := NativeType{proto: 4, typ: TypeInt}

	mCases := marshal.Group{
		Name:        tType.Type().String(),
		Func:        func(i interface{}) ([]byte, error) { return Marshal(tType, i) },
		DefaultMods: mod.Default,
		Sets: []*marshal.Set{
			{
				Name: "big_vals",
				MarshalIns: []interface{}{
					int64(2147483648), int(2147483648), "2147483648",
					int64(-2147483649), int(-2147483649), "-2147483649",
					uint64(4294967296), uint(4294967296),
				},
			},
			{
				Name: "corrupt_vals",
				MarshalIns: []interface{}{
					"1s2", "1s", "-1s", ".1", ",1", "0.1", "0,1",
				},
			},
		},
	}

	mCases.Gen().RunGroup(t)

	uCases := unmarshal.Group{
		Name:        tType.Type().String(),
		Func:        func(bytes []byte, i interface{}) error { return Unmarshal(tType, bytes, i) },
		DefaultMods: mod.Default,
		Sets: []*unmarshal.Set{
			{
				Name: "big_data",
				Data: []byte("\x80\x00\x00\x00\x00"),
				UnmarshalIns: []interface{}{
					int8(0), int16(0), int32(0), int64(0), int(0), "",
					uint8(0), uint16(0), uint32(0), uint64(0), uint(0),
				},
				Issue: "https://github.com/scylladb/gocql/issues/246",
			},
			{
				Name: "small_data",
				Data: []byte("\x80\x00\x00"),
				UnmarshalIns: []interface{}{
					int8(0), int16(0), int32(0), int64(0), int(0), "",
					uint8(0), uint16(0), uint32(0), uint64(0), uint(0),
				},
				Issue: "https://github.com/scylladb/gocql/issues/252",
			},
			{
				Name:         "small_val_type_+int",
				Data:         []byte("\x7f\xff\xff\xff"),
				UnmarshalIns: []interface{}{int8(0), int16(0)},
			},
			{
				Name:         "small_val_type_-int",
				Data:         []byte("\x80\x00\x00\x00"),
				UnmarshalIns: []interface{}{int8(0), int16(0)},
			},
			{
				Name:         "small_val_type_uint",
				Data:         []byte("\xff\xff\xff\xff"),
				UnmarshalIns: []interface{}{uint8(0), uint16(0)},
			},
		},
	}

	uCases.Gen().RunGroup(t)
}
