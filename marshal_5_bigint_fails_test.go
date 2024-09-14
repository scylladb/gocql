package gocql

import (
	"github.com/gocql/gocql/marshal/tests/gen/mod"
	"github.com/gocql/gocql/marshal/tests/gen/mustfail/marshal"
	"github.com/gocql/gocql/marshal/tests/gen/mustfail/unmarshal"
	"math/big"
	"testing"
)

func TestMarshalBigIntMustFail(t *testing.T) {
	tType := NativeType{proto: 4, typ: TypeBigInt}
	mCases := marshal.Group{
		Name:        tType.Type().String(),
		Func:        func(i interface{}) ([]byte, error) { return Marshal(tType, i) },
		DefaultMods: mod.Default,
		Sets: []*marshal.Set{
			{
				Name:       "big_vals",
				MarshalIns: []interface{}{"9223372036854775808", "-9223372036854775809"},
			},
			{
				Name: "big_vals_bigint",
				MarshalIns: []interface{}{
					*big.NewInt(0).Add(big.NewInt(9223372036854775807), big.NewInt(1)),
					*big.NewInt(0).Add(big.NewInt(-9223372036854775808), big.NewInt(-1)),
				},
				Issue: "the error is not returned",
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
				Data: []byte("\x80\x00\x00\x00\x00\x00\x00\x00\x00"),
				UnmarshalIns: []interface{}{
					int8(0), int16(0), int32(0), int64(0), int(0), "", *big.NewInt(0),
					uint8(0), uint16(0), uint32(0), uint64(0), uint(0),
				},
				Issue: "https://github.com/scylladb/gocql/issues/246",
			},
			{
				Name: "small_data",
				Data: []byte("\x80\x00\x00\x00\x00\x00\x00"),
				UnmarshalIns: []interface{}{
					int8(0), int16(0), int32(0), int64(0), int(0), "", *big.NewInt(0),
					uint8(0), uint16(0), uint32(0), uint64(0), uint(0),
				},
				Issue: "https://github.com/scylladb/gocql/issues/252",
			},
			{
				Name:         "small_val_type_+int",
				Data:         []byte("\x7f\xff\xff\xff\xff\xff\xff\xff"),
				UnmarshalIns: []interface{}{int8(0), int16(0), int32(0)},
				Issue:        "https://github.com/scylladb/gocql/issues/253",
			},
			{
				Name:         "small_val_type_-int",
				Data:         []byte("\x80\x00\x00\x00\x00\x00\x00\x00"),
				UnmarshalIns: []interface{}{int8(0), int16(0), int32(0)},
				Issue:        "https://github.com/scylladb/gocql/issues/253",
			},
			{
				Name:         "small_val_type_uint",
				Data:         []byte("\xff\xff\xff\xff\xff\xff\xff\xff"),
				UnmarshalIns: []interface{}{uint8(0), uint16(0), uint32(0)},
				Issue:        "https://github.com/scylladb/gocql/issues/253",
			},
		},
	}

	uCases.Gen().RunGroup(t)
}
