package gocql

import (
	"math/big"
	"testing"

	"github.com/gocql/gocql/marshal/tests"
	"github.com/gocql/gocql/marshal/tests/funcs"
	"github.com/gocql/gocql/marshal/tests/mods"
)

func TestMarshalVarInt(t *testing.T) {
	tType := NativeType{proto: 4, typ: TypeVarint}

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
				Name:  "[0000000000000000]str",
				Mods:  mods.Ref,
				Data:  []byte("\x00"),
				Value: "0",
			},
			{
				Name:  "[7fffffffffffffff]str",
				Mods:  mods.Ref,
				Data:  []byte("\x7f\xff\xff\xff\xff\xff\xff\xff"),
				Value: "9223372036854775807",
			},
			{
				Name:  "[8000000000000000]str",
				Mods:  mods.Ref,
				Data:  []byte("\x80\x00\x00\x00\x00\x00\x00\x00"),
				Value: "-9223372036854775808",
			},
			{
				Name:           "23232323232313129223372036854775807_str",
				Data:           []byte("\x04\x79\x71\x25\x02\x55\xaf\x8f\xbd\x69\x86\x48\x7f\xff\xff"),
				Value:          "23232323232313129223372036854775807",
				IssueMarshal:   `error: can not marshal string to bigint: strconv.ParseInt: parsing "23232323232313129223372036854775807": value out of range`,
				IssueUnmarshal: "error: unmarshal int: varint value [4 121 113 37 2 85 175 143 189 105 134 72 127 255 255] out of range for *string (use big.Int)",
			},
			{
				Name: "[nil]refs",
				Mods: mods.Custom,
				Data: nil,
				Values: tests.NilRefs(
					int8(0), int16(0), int32(0), int64(0), int(0), *big.NewInt(0),
					uint8(0), uint16(0), uint32(0), uint64(0), uint(0),
				),
			},
			{
				Name:  "unmarshal nil data",
				Funcs: funcs.ExcludeMarshal(),
				Mods:  mods.Custom,
				Data:  nil,
				Values: []interface{}{
					int8(0), int16(0), int32(0), int64(0), int(0), *big.NewInt(0),
					uint8(0), uint16(0), uint32(0), uint64(0), uint(0),
				},
			},
			{
				Name:  "unmarshal zero data",
				Funcs: funcs.ExcludeMarshal(),
				Data:  make([]byte, 0),
				Values: []interface{}{
					int8(0), int16(0), int32(0), int64(0), int(0), *big.NewInt(0),
					uint8(0), uint16(0), uint32(0), uint64(0), uint(0),
				},
			},
			{
				Data: []byte("\x00"),
				Values: []interface{}{
					int8(0), int16(0), int32(0), int64(0), int(0), *big.NewInt(0),
					uint8(0), uint16(0), uint32(0), uint64(0), uint(0),
				},
			},
			{
				Data:   []byte("\x7f\xff\xff\xff\xff\xff\xff\xff"),
				Values: []interface{}{int64(9223372036854775807), int(9223372036854775807), *big.NewInt(9223372036854775807)},
			},
			{
				Data:   []byte("\x7f\xff\xff\xff"),
				Values: []interface{}{int32(2147483647), int64(2147483647), int(2147483647), *big.NewInt(2147483647)},
			},
			{
				Data:   []byte("\x7f\xff"),
				Values: []interface{}{int16(32767), int32(32767), int64(32767), int(32767), *big.NewInt(32767)},
			},
			{
				Data:   []byte("\x7f"),
				Values: []interface{}{int8(127), int16(127), int32(127), int64(127), int(127), *big.NewInt(127)},
			},
			{
				Data:   []byte("\x80"),
				Values: []interface{}{int8(-128), int16(-128), int32(-128), int64(-128), int(-128), *big.NewInt(-128)},
			},
			{
				Data:   []byte("\x80\x00"),
				Values: []interface{}{int16(-32768), int32(-32768), int64(-32768), int(-32768), *big.NewInt(-32768)},
			},
			{
				Data:   []byte("\x80\x00\x00\x00"),
				Values: []interface{}{int32(-2147483648), int64(-2147483648), int(-2147483648), *big.NewInt(-2147483648)},
			},
			{
				Data:   []byte("\x80\x00\x00\x00\x00\x00\x00\x00"),
				Values: []interface{}{int64(-9223372036854775808), int(-9223372036854775808), *big.NewInt(-9223372036854775808)},
			},
			{
				Data:   []byte("\x00\xff"),
				Values: []interface{}{uint8(255), uint16(255), uint32(255), uint64(255), uint(255), *big.NewInt(255)},
			},
			{
				Data:   []byte("\x00\xff\xff"),
				Values: []interface{}{uint16(65535), uint32(65535), uint64(65535), uint(65535), *big.NewInt(65535)},
			},
			{
				Data:   []byte("\x00\xff\xff\xff\xff"),
				Values: []interface{}{uint32(4294967295), uint64(4294967295), uint(4294967295), *big.NewInt(4294967295)},
			},
			{
				Data:           []byte("\x00\xff\xff\xff\xff\xff\xff\xff\xff"),
				Values:         []interface{}{uint64(18446744073709551615), uint(18446744073709551615)},
				IssueMarshal:   "error: marshal bigint: value 18446744073709551615 out of range",
				IssueUnmarshal: "error: unmarshal int: varint value [0 255 255 255 255 255 255 255 255] out of range for *uint (use big.Int)",
			},
			{
				Name:   "[00ffffffffffffffff]bigInt",
				Data:   []byte("\x00\xff\xff\xff\xff\xff\xff\xff\xff"),
				Values: []interface{}{*big.NewInt(0).Add(big.NewInt(9223372036854775807), big.NewInt(0).Add(big.NewInt(9223372036854775807), big.NewInt(1)))},
			},
		},
	}

	cases.GetModified().Run(t)
}

func TestMarshalVarIntMustFail(t *testing.T) {
	tType := NativeType{proto: 4, typ: TypeVarint}

	cases := tests.MustFail{
		Marshal: &tests.MarshalCases{
			Func:        func(i interface{}) ([]byte, error) { return Marshal(tType, i) },
			DefaultMods: mods.Default,
			Cases: []*tests.MustFailMarshal{
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
					Name:        "big_data",
					Data:        []byte("\xff\x00\xff"),
					UnmarshalIn: &big.Int{},
					Issue:       "the error is not returned",
				},
			},
		},
	}

	cases.GetModified().Run(t)
}
