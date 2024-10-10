package serialization_test

import (
	"math"
	"math/big"
	"testing"

	"github.com/gocql/gocql"
	"github.com/gocql/gocql/internal/tests/serialization"
	"github.com/gocql/gocql/internal/tests/serialization/mod"
	"github.com/gocql/gocql/serialization/smallint"
)

func TestMarshalSmallintCorrupt(t *testing.T) {
	type testSuite struct {
		name      string
		marshal   func(interface{}) ([]byte, error)
		unmarshal func(bytes []byte, i interface{}) error
	}

	tType := gocql.NewNativeType(4, gocql.TypeSmallInt, "")

	testSuites := [2]testSuite{
		{
			name:      "serialization.smallint",
			marshal:   smallint.Marshal,
			unmarshal: smallint.Unmarshal,
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
					int32(math.MaxInt16 + 1), int64(math.MaxInt16 + 1), int(math.MaxInt16 + 1),
					uint16(math.MaxInt16 + 1), uint32(math.MaxInt16 + 1), uint64(math.MaxInt16 + 1), uint(math.MaxInt16 + 1),
					"32768", *big.NewInt(math.MaxInt16 + 1),
					int32(math.MinInt16 - 1), int64(math.MinInt16 - 1), int(math.MinInt16 - 1),
					"-32769", *big.NewInt(math.MinInt16 - 1),
				}.AddVariants(mod.All...),
			}.Run("big_vals", t, marshal)

			serialization.NegativeMarshalSet{
				Values: mod.Values{"1s2", "1s", "-1s", ".1", ",1", "0.1", "0,1"}.AddVariants(mod.All...),
			}.Run("corrupt_vals", t, marshal)

			serialization.NegativeUnmarshalSet{
				Data: []byte("\x80\x00\x00"),
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
			}.Run("small_data", t, unmarshal)

			serialization.NegativeUnmarshalSet{
				Data:   []byte("\x00\x80"),
				Values: mod.Values{int8(0)}.AddVariants(mod.All...),
			}.Run("small_type_int8_128", t, unmarshal)

			serialization.NegativeUnmarshalSet{
				Data:   []byte("\x7f\xff"),
				Values: mod.Values{int8(0)}.AddVariants(mod.All...),
			}.Run("small_type_int8_32767", t, unmarshal)

			serialization.NegativeUnmarshalSet{
				Data:   []byte("\xff\x7f"),
				Values: mod.Values{int8(0)}.AddVariants(mod.All...),
			}.Run("small_type_int8_-129", t, unmarshal)

			serialization.NegativeUnmarshalSet{
				Data:   []byte("\x7f\xff"),
				Values: mod.Values{int8(0)}.AddVariants(mod.All...),
			}.Run("small_type_int8_-32768", t, unmarshal)

			serialization.NegativeUnmarshalSet{
				Data:   []byte("\x01\x00"),
				Values: mod.Values{uint8(0)}.AddVariants(mod.All...),
			}.Run("small_type_uint_256", t, unmarshal)

			serialization.NegativeUnmarshalSet{
				Data:   []byte("\xff\xff"),
				Values: mod.Values{uint8(0)}.AddVariants(mod.All...),
			}.Run("small_type_uint_65535", t, unmarshal)

			serialization.NegativeUnmarshalSet{
				Data: []byte("\xff\xff"),
				Values: mod.Values{
					uint8(0), uint16(0), uint32(0), uint64(0), uint(0),
				}.AddVariants(mod.All...),
			}.Run("neg_uints-1", t, unmarshal)

			serialization.NegativeUnmarshalSet{
				Data: []byte("\xff\x80"),
				Values: mod.Values{
					uint8(0), uint16(0), uint32(0), uint64(0), uint(0),
				}.AddVariants(mod.All...),
			}.Run("neg_uints_minInt8", t, unmarshal)

			serialization.NegativeUnmarshalSet{
				Data: []byte("\xff\x7f"),
				Values: mod.Values{
					uint8(0), uint16(0), uint32(0), uint64(0), uint(0),
				}.AddVariants(mod.All...),
			}.Run("neg_uints_minInt8-1", t, unmarshal)

			serialization.NegativeUnmarshalSet{
				Data: []byte("\x80\x00"),
				Values: mod.Values{
					uint8(0), uint16(0), uint32(0), uint64(0), uint(0),
				}.AddVariants(mod.All...),
			}.Run("neg_uints_minInt16", t, unmarshal)
		})
	}
}
