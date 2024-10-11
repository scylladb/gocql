package serialization_test

import (
	"math"
	"math/big"
	"testing"

	"github.com/gocql/gocql"
	"github.com/gocql/gocql/internal/tests/serialization"
	"github.com/gocql/gocql/internal/tests/serialization/mod"
	"github.com/gocql/gocql/serialization/cqlint"
)

func TestMarshalIntCorrupt(t *testing.T) {
	type testSuite struct {
		name      string
		marshal   func(interface{}) ([]byte, error)
		unmarshal func(bytes []byte, i interface{}) error
	}

	tType := gocql.NewNativeType(4, gocql.TypeInt, "")

	testSuites := [2]testSuite{
		{
			name:      "serialization.int",
			marshal:   cqlint.Marshal,
			unmarshal: cqlint.Unmarshal,
		},
		{
			name: "glob",
			marshal: func(i interface{}) ([]byte, error) {
				return gocql.Marshal(tType, i)
			},
			unmarshal: func(bytes []byte, i interface{}) error {
				return gocql.Unmarshal(tType, bytes, i)
			},
		},
	}

	for _, tSuite := range testSuites {
		marshal := tSuite.marshal
		unmarshal := tSuite.unmarshal

		t.Run(tSuite.name, func(t *testing.T) {
			serialization.NegativeMarshalSet{
				Values: mod.Values{
					int64(math.MaxInt32 + 1), int(math.MaxInt32 + 1),
					"2147483648", *big.NewInt(math.MaxInt32 + 1),
					uint32(math.MaxInt32 + 1), uint64(math.MaxInt32 + 1), uint(math.MaxInt32 + 1),
					int64(-2147483649), int(-2147483649),
					"-2147483649", *big.NewInt(-2147483649),
				}.AddVariants(mod.All...),
			}.Run("big_vals", t, marshal)

			serialization.NegativeMarshalSet{
				Values: mod.Values{"1s2", "1s", "-1s", ".1", ",1", "0.1", "0,1"}.AddVariants(mod.All...),
			}.Run("corrupt_vals", t, marshal)

			serialization.NegativeUnmarshalSet{
				Data: []byte("\x80\x00\x00\x00\x00"),
				Values: mod.Values{
					int8(0), int16(0), int32(0), int64(0), int(0),
					uint8(0), uint16(0), uint32(0), uint64(0), uint(0),
					"", *big.NewInt(0),
				}.AddVariants(mod.All...),
			}.Run("big_data", t, unmarshal)

			serialization.NegativeUnmarshalSet{
				Data: []byte("\x80"),
				Values: mod.Values{
					int8(0), int16(0), int32(0), int64(0), int(0),
					uint8(0), uint16(0), uint32(0), uint64(0), uint(0),
					"", *big.NewInt(0),
				}.AddVariants(mod.All...),
			}.Run("small_data1", t, unmarshal)

			serialization.NegativeUnmarshalSet{
				Data: []byte("\x80\x00\x00"),
				Values: mod.Values{
					int8(0), int16(0), int32(0), int64(0), int(0),
					uint8(0), uint16(0), uint32(0), uint64(0), uint(0),
					"", *big.NewInt(0),
				}.AddVariants(mod.All...),
			}.Run("small_data2", t, unmarshal)

			serialization.NegativeUnmarshalSet{
				Data:   []byte("\x80\x00\x00\x00"),
				Values: mod.Values{int8(0), int16(0)}.AddVariants(mod.All...),
			}.Run("small_types_int_2147483648", t, unmarshal)

			serialization.NegativeUnmarshalSet{
				Data:   []byte("\x7f\xff\xff\xff"),
				Values: mod.Values{int8(0), int16(0)}.AddVariants(mod.All...),
			}.Run("small_types_int_-2147483647", t, unmarshal)

			serialization.NegativeUnmarshalSet{
				Data:   []byte("\x00\x00\x80\x00"),
				Values: mod.Values{int8(0), int16(0)}.AddVariants(mod.All...),
			}.Run("small_types_int_32768", t, unmarshal)

			serialization.NegativeUnmarshalSet{
				Data:   []byte("\xff\xff\x7f\xff"),
				Values: mod.Values{int8(0), int16(0)}.AddVariants(mod.All...),
			}.Run("small_types_int_-32769", t, unmarshal)

			serialization.NegativeUnmarshalSet{
				Data:   []byte("\x00\x00\x00\x80"),
				Values: mod.Values{int8(0)}.AddVariants(mod.All...),
			}.Run("small_type_int8_128", t, unmarshal)

			serialization.NegativeUnmarshalSet{
				Data:   []byte("\xff\xff\xff\x7f"),
				Values: mod.Values{int8(0)}.AddVariants(mod.All...),
			}.Run("small_type_int8_-129", t, unmarshal)

			serialization.NegativeUnmarshalSet{
				Data:   []byte("\xff\xff\xff\xff"),
				Values: mod.Values{uint8(0), uint16(0)}.AddVariants(mod.All...),
			}.Run("small_types_uint_4294967295", t, unmarshal)

			serialization.NegativeUnmarshalSet{
				Data:   []byte("\x00\x01\x00\x00"),
				Values: mod.Values{uint8(0), uint16(0)}.AddVariants(mod.All...),
			}.Run("small_types_uint_65536", t, unmarshal)

			serialization.NegativeUnmarshalSet{
				Data:   []byte("\x00\x00\x01\x00"),
				Values: mod.Values{uint8(0)}.AddVariants(mod.All...),
			}.Run("small_type_uint_256", t, unmarshal)

			serialization.NegativeUnmarshalSet{
				Data: []byte("\xff\xff\xff\xff"),
				Values: mod.Values{
					uint8(0), uint16(0), uint32(0), uint64(0), uint(0),
				}.AddVariants(mod.All...),
			}.Run("neg_uints-1", t, unmarshal)

			serialization.NegativeUnmarshalSet{
				Data: []byte("\xff\xff\xff\x80"),
				Values: mod.Values{
					uint8(0), uint16(0), uint32(0), uint64(0), uint(0),
				}.AddVariants(mod.All...),
			}.Run("neg_uints_minInt8", t, unmarshal)

			serialization.NegativeUnmarshalSet{
				Data: []byte("\xff\xff\xff\x7f"),
				Values: mod.Values{
					uint8(0), uint16(0), uint32(0), uint64(0), uint(0),
				}.AddVariants(mod.All...),
			}.Run("neg_uints_minInt8-1", t, unmarshal)

			serialization.NegativeUnmarshalSet{
				Data: []byte("\xff\xff\x80\x00"),
				Values: mod.Values{
					uint8(0), uint16(0), uint32(0), uint64(0), uint(0),
				}.AddVariants(mod.All...),
			}.Run("neg_uints_minInt16", t, unmarshal)

			serialization.NegativeUnmarshalSet{
				Data: []byte("\xff\xff\x7f\xff"),
				Values: mod.Values{
					uint8(0), uint16(0), uint32(0), uint64(0), uint(0),
				}.AddVariants(mod.All...),
			}.Run("neg_uints_minInt16-1", t, unmarshal)

			serialization.NegativeUnmarshalSet{
				Data: []byte("\xff\x80\x00\x00"),
				Values: mod.Values{
					uint8(0), uint16(0), uint32(0), uint64(0), uint(0),
				}.AddVariants(mod.All...),
			}.Run("neg_uints_minInt24", t, unmarshal)

			serialization.NegativeUnmarshalSet{
				Data: []byte("\xff\x7f\xff\xff"),
				Values: mod.Values{
					uint8(0), uint16(0), uint32(0), uint64(0), uint(0),
				}.AddVariants(mod.All...),
			}.Run("neg_uints_minInt24-1", t, unmarshal)

			serialization.NegativeUnmarshalSet{
				Data: []byte("\x80\x00\x00\x00"),
				Values: mod.Values{
					uint8(0), uint16(0), uint32(0), uint64(0), uint(0),
				}.AddVariants(mod.All...),
			}.Run("neg_uints_minInt36", t, unmarshal)
		})
	}
}
