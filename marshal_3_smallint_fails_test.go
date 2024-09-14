package gocql

import (
	"testing"

	"github.com/gocql/gocql/marshal/tests/gen/mod"
	"github.com/gocql/gocql/marshal/tests/gen/mustfail/marshal"
	"github.com/gocql/gocql/marshal/tests/gen/mustfail/unmarshal"
)

func TestMarshalSmallintMustFail(t *testing.T) {
	tType := NativeType{proto: 4, typ: TypeTinyInt}

	mCases := &marshal.Group{
		Name:        tType.Type().String(),
		Func:        func(i interface{}) ([]byte, error) { return Marshal(tType, i) },
		DefaultMods: mod.Default,
		Sets: []*marshal.Set{
			{
				Name: "big_vals",
				MarshalIns: []interface{}{
					int32(32768), int64(32768), int(32768), "32768",
					int32(-32769), int64(-32769), int(-32769), "-32769",
					uint32(65536), uint64(65536), uint(65536),
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

	uCases := &unmarshal.Group{
		Name:        tType.Type().String(),
		Func:        func(d []byte, i interface{}) error { return Unmarshal(tType, d, i) },
		DefaultMods: mod.Default,
		Sets: []*unmarshal.Set{
			{
				Name: "big_data",
				Data: []byte("\x80\x00\x00"),
				UnmarshalIns: []interface{}{
					int8(0), int16(0), int32(0), int64(0), int(0), "",
					uint8(0), uint16(0), uint32(0), uint64(0), uint(0),
				},
				Issue: "https://github.com/scylladb/gocql/issues/246",
			},
			{
				Name: "small_data",
				Data: []byte("\x80"),
				UnmarshalIns: []interface{}{
					int8(0), int16(0), int32(0), int64(0), int(0), "",
					uint8(0), uint16(0), uint32(0), uint64(0), uint(0),
				},
				Issue: "https://github.com/scylladb/gocql/issues/252",
			},
			{
				Name:        "small_val_type_+int",
				Data:        []byte("\x7f\xff"),
				UnmarshalIn: int8(0),
				Issue:       "https://github.com/scylladb/gocql/issues/253",
			},
			{
				Name:        "small_val_type_-int",
				Data:        []byte("\x80\x00"),
				UnmarshalIn: int8(0),
				Issue:       "https://github.com/scylladb/gocql/issues/253",
			},
			{
				Name:        "small_val_type_uint",
				Data:        []byte("\xff\xff"),
				UnmarshalIn: uint8(0),
				Issue:       "https://github.com/scylladb/gocql/issues/253",
			},
		},
	}
	uCases.Gen().RunGroup(t)
}
