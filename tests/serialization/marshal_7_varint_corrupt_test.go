package serialization_test

import (
	"math/big"
	"testing"

	"github.com/gocql/gocql"
	"github.com/gocql/gocql/internal/tests/serialization"
	"github.com/gocql/gocql/internal/tests/serialization/mod"
	"github.com/gocql/gocql/serialization/varint"
)

func TestMarshalVarIntCorrupt(t *testing.T) {
	type testSuite struct {
		name      string
		marshal   func(interface{}) ([]byte, error)
		unmarshal func(bytes []byte, i interface{}) error
	}

	tType := gocql.NewNativeType(4, gocql.TypeVarint, "")

	testSuites := [2]testSuite{
		{
			name:      "serialization.varint",
			marshal:   varint.Marshal,
			unmarshal: varint.Unmarshal,
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
				Values: mod.Values{"1s2", "1s", "-1s", ".1", ",1", "0.1", "0,1"}.AddVariants(mod.All...),
			}.Run("corrupt_vals", t, marshal)

			serialization.NegativeUnmarshalSet{
				Data: []byte("\x00\x7f"),
				Values: mod.Values{
					int8(0), int16(0), int32(0), int64(0), int(0),
					uint8(0), uint16(0), uint32(0), uint64(0), uint(0),
					"", *big.NewInt(0),
				}.AddVariants(mod.All...),
			}.Run("corrupt_data+", t, unmarshal)

			serialization.NegativeUnmarshalSet{
				Data: []byte("\xff\x80"),
				Values: mod.Values{
					int8(0), int16(0), int32(0), int64(0), int(0),
					uint8(0), uint16(0), uint32(0), uint64(0), uint(0),
					"", *big.NewInt(0),
				}.AddVariants(mod.All...),
			}.Run("corrupt_data-", t, unmarshal)

			serialization.NegativeUnmarshalSet{
				Data:   []byte("\x00\x80"),
				Values: mod.Values{int8(0)}.AddVariants(mod.All...),
			}.Run("small_type_maxInt8+1", t, unmarshal)

			serialization.NegativeUnmarshalSet{
				Data:   []byte("\xff\x7f"),
				Values: mod.Values{int8(0)}.AddVariants(mod.All...),
			}.Run("small_type_minInt8-1", t, unmarshal)

			serialization.NegativeUnmarshalSet{
				Data:   []byte("\x00\x80\x00"),
				Values: mod.Values{int8(0), int16(0)}.AddVariants(mod.All...),
			}.Run("small_type_maxInt16+1", t, unmarshal)

			serialization.NegativeUnmarshalSet{
				Data:   []byte("\xff\x7f\xff"),
				Values: mod.Values{int8(0)}.AddVariants(mod.All...),
			}.Run("small_type_minInt16-1", t, unmarshal)

			serialization.NegativeUnmarshalSet{
				Data:   []byte("\x00\x80\x00\x00\x00"),
				Values: mod.Values{int8(0), int16(0), int32(0)}.AddVariants(mod.All...),
			}.Run("small_type_maxInt32+1", t, unmarshal)

			serialization.NegativeUnmarshalSet{
				Data:   []byte("\xff\x7f\xff\xff\xff\xff"),
				Values: mod.Values{int8(0), int16(0), int32(0)}.AddVariants(mod.All...),
			}.Run("small_type_minInt32-1", t, unmarshal)

			serialization.NegativeUnmarshalSet{
				Data:   []byte("\x00\x80\x00\x00\x00\x00\x00\x00\x00"),
				Values: mod.Values{int8(0), int16(0), int32(0), int64(0), int(0)}.AddVariants(mod.All...),
			}.Run("small_type_maxInt64+1", t, unmarshal)

			serialization.NegativeUnmarshalSet{
				Data:   []byte("\xff\x7f\xff\xff\xff\xff\xff\xff\xff\xff"),
				Values: mod.Values{int8(0), int16(0), int32(0), int64(0), int(0)}.AddVariants(mod.All...),
			}.Run("small_type_minInt64-1", t, unmarshal)

			serialization.NegativeUnmarshalSet{
				Data:   []byte("\x01\x00"),
				Values: mod.Values{uint8(0)}.AddVariants(mod.All...),
			}.Run("small_type_maxUint8+1", t, unmarshal)

			serialization.NegativeUnmarshalSet{
				Data:   []byte("\x01\x00\x00"),
				Values: mod.Values{uint8(0), uint16(0)}.AddVariants(mod.All...),
			}.Run("small_type_maxUint16+1", t, unmarshal)

			serialization.NegativeUnmarshalSet{
				Data:   []byte("\x01\x00\x00\x00\x00"),
				Values: mod.Values{uint8(0), uint16(0), uint32(0)}.AddVariants(mod.All...),
			}.Run("small_type_maxUint32+1", t, unmarshal)

			serialization.NegativeUnmarshalSet{
				Data:   []byte("\x01\x00\x00\x00\x00\x00\x00\x00\x00"),
				Values: mod.Values{uint8(0), uint16(0), uint32(0), uint64(0), uint(0)}.AddVariants(mod.All...),
			}.Run("small_type_maxUint64+1", t, unmarshal)
		})
	}
}
