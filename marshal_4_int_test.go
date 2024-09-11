package gocql

import (
	"testing"

	"github.com/gocql/gocql/marshal/tests"
	"github.com/gocql/gocql/marshal/tests/funcs"
	"github.com/gocql/gocql/marshal/tests/mods"
)

func TestMarshalInt(t *testing.T) {
	tType := NativeType{proto: 4, typ: TypeInt}

	cases := tests.Serialization{
		Funcs: funcs.Default(
			func(i interface{}) ([]byte, error) { return Marshal(tType, i) },
			func(bytes []byte, i interface{}) error { return Unmarshal(tType, bytes, i) },
		),
		DefaultMods: mods.Default,
		Cases: []*tests.Case{
			{
				Name:  "[nil]str",
				Mods:  mods.Non,
				Data:  nil,
				Value: tests.NilRef(""),
			},
			{
				Name:  "[00000000]str",
				Mods:  mods.Ref,
				Data:  []byte("\x00\x00\x00\x00"),
				Value: "0",
			},
			{
				Name:  "[7fffffff]str",
				Mods:  mods.Ref,
				Data:  []byte("\x7f\xff\xff\xff"),
				Value: "2147483647",
			},
			{
				Name:  "[80000000]str",
				Mods:  mods.Ref,
				Data:  []byte("\x80\x00\x00\x00"),
				Value: "-2147483648",
			},
			{
				Name: "[nil]refs",
				Mods: mods.Custom,
				Data: nil,
				Values: tests.NilRefs(
					int8(0), int16(0), int32(0), int64(0), int(0),
					uint8(0), uint16(0), uint32(0), uint64(0), uint(0),
				),
			},
			{
				Name:  "unmarshal nil data",
				Funcs: funcs.ExcludeMarshal(),
				Mods:  mods.Custom,
				Data:  nil,
				Values: []interface{}{
					int8(0), int16(0), int32(0), int64(0), int(0),
					uint8(0), uint16(0), uint32(0), uint64(0), uint(0),
				},
			},
			{
				Name:  "unmarshal zero data",
				Funcs: funcs.ExcludeMarshal(),
				Data:  make([]byte, 0),
				Values: []interface{}{
					int8(0), int16(0), int32(0), int64(0), int(0),
					uint8(0), uint16(0), uint32(0), uint64(0), uint(0),
				},
			},
			{
				Name: "zeros",
				Data: []byte("\x00\x00\x00\x00"),
				Values: []interface{}{
					int8(0), int16(0), int32(0), int64(0), int(0),
					uint8(0), uint16(0), uint32(0), uint64(0), uint(0),
				},
			},
			{
				Data: []byte("\x7f\xff\xff\xff"),
				Values: []interface{}{
					int32(2147483647), int64(2147483647), int(2147483647),
				},
			},
			{
				Data: []byte("\x00\x00\x7f\xff"),
				Values: []interface{}{
					int16(32767), int32(32767), int64(32767), int(32767),
				},
			},
			{
				Data: []byte("\x00\x00\x00\x7f"),
				Values: []interface{}{
					int8(127), int16(127), int32(127), int64(127), int(127),
				},
			},
			{
				Data: []byte("\xff\xff\xff\x80"),
				Values: []interface{}{
					int8(-128), int16(-128), int32(-128), int64(-128), int(-128),
				},
			},
			{
				Data: []byte("\xff\xff\x80\x00"),
				Values: []interface{}{
					int16(-32768), int32(-32768), int64(-32768), int(-32768),
				},
			},
			{
				Data: []byte("\x80\x00\x00\x00"),
				Values: []interface{}{
					int32(-2147483648), int64(-2147483648), int(-2147483648),
				},
			},
			{
				Data: []byte("\x00\x00\x00\xff"),
				Values: []interface{}{
					uint8(255), uint16(255), uint32(255), uint64(255), uint(255),
				},
			},
			{
				Data: []byte("\x00\x00\xff\xff"),
				Values: []interface{}{
					uint16(65535), uint32(65535), uint64(65535), uint(65535),
				},
			},
			{
				Data: []byte("\xff\xff\xff\xff"),
				Values: []interface{}{
					uint32(4294967295), uint64(4294967295), uint(4294967295),
				},
				IssueMarshal: "error: marshal int: value 4294967295 out of range",
			},
		},
	}

	cases.GetModified().Run(t)
}

func TestMarshalIntMustFail(t *testing.T) {
	tType := NativeType{proto: 4, typ: TypeInt}

	cases := tests.MustFail{
		Marshal: &tests.MarshalCases{
			Func:        func(i interface{}) ([]byte, error) { return Marshal(tType, i) },
			DefaultMods: mods.Default,
			Cases: []*tests.MustFailMarshal{
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
		},
		Unmarshal: &tests.UnmarshalCases{
			Func:        func(bytes []byte, i interface{}) error { return Unmarshal(tType, bytes, i) },
			DefaultMods: mods.Default,
			Cases: []*tests.MustFailUnmarshal{
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
		},
	}

	cases.GetModified().Run(t)
}
