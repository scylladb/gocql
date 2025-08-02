//go:build unit
// +build unit

package serialization_test

import (
	"math/big"
	"testing"

	"github.com/scylladb/gocql/v2"
	"github.com/scylladb/gocql/v2/internal/tests/serialization"
	"github.com/scylladb/gocql/v2/internal/tests/serialization/mod"
	"github.com/scylladb/gocql/v2/serialization/counter"
)

func TestMarshalCounterCorrupt(t *testing.T) {
	t.Parallel()

	type testSuite struct {
		name      string
		marshal   func(interface{}) ([]byte, error)
		unmarshal func(bytes []byte, i interface{}) error
	}

	tType := gocql.NewNativeType(4, gocql.TypeCounter, "")

	testSuites := [2]testSuite{
		{
			name:      "serialization.counter",
			marshal:   counter.Marshal,
			unmarshal: counter.Unmarshal,
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

		serialization.NegativeMarshalSet{
			Values: mod.Values{
				"9223372036854775808",
				"-9223372036854775809",
				*big.NewInt(0).Add(big.NewInt(9223372036854775807), big.NewInt(1)),
				*big.NewInt(0).Add(big.NewInt(-9223372036854775808), big.NewInt(-1)),
			}.AddVariants(mod.All...),
		}.Run("big_vals", t, marshal)

		serialization.NegativeMarshalSet{
			Values: mod.Values{"1s2", "1s", "-1s", ".1", ",1", "0.1", "0,1"}.AddVariants(mod.All...),
		}.Run("corrupt_vals", t, marshal)

		serialization.NegativeUnmarshalSet{
			Data: []byte("\x80\x00\x00\x00\x00\x00\x00\x00\x00"),
			Values: mod.Values{
				int8(0), int16(0), int32(0), int64(0), int(0),
				uint8(0), uint16(0), uint32(0), uint64(0), uint(0),
				"", *big.NewInt(0),
			}.AddVariants(mod.All...),
		}.Run("big_data", t, unmarshal)

		serialization.NegativeUnmarshalSet{
			Data: []byte("\x80\x00\x00\x00\x00\x00\x00"),
			Values: mod.Values{
				int8(0), int16(0), int32(0), int64(0), int(0),
				uint8(0), uint16(0), uint32(0), uint64(0), uint(0),
				"", *big.NewInt(0),
			}.AddVariants(mod.All...),
		}.Run("small_data", t, unmarshal)

		serialization.NegativeUnmarshalSet{
			Data: []byte("\x80"),
			Values: mod.Values{
				int8(0), int16(0), int32(0), int64(0), int(0),
				uint8(0), uint16(0), uint32(0), uint64(0), uint(0),
				"", *big.NewInt(0),
			}.AddVariants(mod.All...),
		}.Run("small_data2", t, unmarshal)

		serialization.NegativeUnmarshalSet{
			Data:   []byte("\x00\x00\x00\x00\x00\x00\x00\x80"),
			Values: mod.Values{int8(0)}.AddVariants(mod.All...),
		}.Run("small_type_int8_128", t, unmarshal)

		serialization.NegativeUnmarshalSet{
			Data:   []byte("\xff\xff\xff\xff\xff\xff\xff\x7f"),
			Values: mod.Values{int8(0)}.AddVariants(mod.All...),
		}.Run("small_type_int8_-129", t, unmarshal)

		serialization.NegativeUnmarshalSet{
			Data:   []byte("\x00\x00\x00\x00\x00\x00\x80\x00"),
			Values: mod.Values{int8(0), int16(0)}.AddVariants(mod.All...),
		}.Run("small_type_int_32768", t, unmarshal)

		serialization.NegativeUnmarshalSet{
			Data:   []byte("\xff\xff\xff\xff\xff\xff\x7f\xff"),
			Values: mod.Values{int8(0)}.AddVariants(mod.All...),
		}.Run("small_type_int8_-32769", t, unmarshal)

		serialization.NegativeUnmarshalSet{
			Data:   []byte("\x00\x00\x00\x00\x80\x00\x00\x00"),
			Values: mod.Values{int8(0), int16(0), int32(0)}.AddVariants(mod.All...),
		}.Run("small_type_int_2147483647", t, unmarshal)

		serialization.NegativeUnmarshalSet{
			Data:   []byte("\xff\xff\xff\x7f\xff\xff\xff\xff"),
			Values: mod.Values{int8(0), int16(0), int32(0)}.AddVariants(mod.All...),
		}.Run("small_type_int_-2147483648", t, unmarshal)

		serialization.NegativeUnmarshalSet{
			Data:   []byte("\x00\x00\x00\x00\x00\x00\x01\x00"),
			Values: mod.Values{uint8(0)}.AddVariants(mod.All...),
		}.Run("small_type_uint8_256", t, unmarshal)

		serialization.NegativeUnmarshalSet{
			Data:   []byte("\x00\x00\x00\x00\x00\x01\x00\x00"),
			Values: mod.Values{uint8(0), uint16(0)}.AddVariants(mod.All...),
		}.Run("small_type_uint_65536", t, unmarshal)

		serialization.NegativeUnmarshalSet{
			Data:   []byte("\x00\x00\x00\x01\x00\x00\x00\x00"),
			Values: mod.Values{uint8(0), uint16(0), uint32(0)}.AddVariants(mod.All...),
		}.Run("small_type_uint_4294967296", t, unmarshal)

		serialization.NegativeUnmarshalSet{
			Data:   []byte("\xff\xff\xff\xff\xff\xff\xff\xff"),
			Values: mod.Values{uint8(0), uint16(0), uint32(0)}.AddVariants(mod.All...),
		}.Run("small_type_uint_max", t, unmarshal)
	}
}
