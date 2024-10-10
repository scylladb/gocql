package serialization_test

import (
	"math/big"
	"testing"

	"github.com/gocql/gocql"
	"github.com/gocql/gocql/internal/tests/serialization"
	"github.com/gocql/gocql/internal/tests/serialization/mod"
	"github.com/gocql/gocql/serialization/tinyint"
)

func TestMarshalTinyint(t *testing.T) {
	type testSuite struct {
		name      string
		marshal   func(interface{}) ([]byte, error)
		unmarshal func(bytes []byte, i interface{}) error
	}

	tType := gocql.NewNativeType(4, gocql.TypeTinyInt, "")

	testSuites := [2]testSuite{
		{
			name:      "serialization.tinyint",
			marshal:   tinyint.Marshal,
			unmarshal: tinyint.Unmarshal,
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
			serialization.PositiveSet{
				Data: nil,
				Values: mod.Values{
					(*int8)(nil), (*int16)(nil), (*int32)(nil), (*int64)(nil), (*int)(nil),
					(*uint8)(nil), (*uint16)(nil), (*uint32)(nil), (*uint64)(nil), (*uint)(nil),
					(*string)(nil), (*big.Int)(nil), string(""),
				}.AddVariants(mod.CustomType),
			}.Run("[nil]nullable", t, marshal, unmarshal)

			serialization.PositiveSet{
				Data: nil,
				Values: mod.Values{
					int8(0), int16(0), int32(0), int64(0), int(0),
					uint8(0), uint16(0), uint32(0), uint64(0), uint(0),
					"", big.Int{},
				}.AddVariants(mod.CustomType),
			}.Run("[nil]unmarshal", t, nil, unmarshal)

			serialization.PositiveSet{
				Data: make([]byte, 0),
				Values: mod.Values{
					int8(0), int16(0), int32(0), int64(0), int(0),
					uint8(0), uint16(0), uint32(0), uint64(0), uint(0),
					"0", *big.NewInt(0),
				}.AddVariants(mod.All...),
			}.Run("[]unmarshal", t, nil, unmarshal)

			serialization.PositiveSet{
				Data: []byte("\x00"),
				Values: mod.Values{
					int8(0), int16(0), int32(0), int64(0), int(0),
					uint8(0), uint16(0), uint32(0), uint64(0), uint(0),
					"0", *big.NewInt(0),
				}.AddVariants(mod.All...),
			}.Run("zeros", t, marshal, unmarshal)

			serialization.PositiveSet{
				Data: []byte("\x01"),
				Values: mod.Values{
					int8(1), int16(1), int32(1), int64(1), int(1),
					uint8(1), uint16(1), uint32(1), uint64(1), uint(1),
					"1", *big.NewInt(1)}.AddVariants(mod.All...),
			}.Run("1", t, marshal, unmarshal)

			serialization.PositiveSet{
				Data:   []byte("\xff"),
				Values: mod.Values{int8(-1), int16(-1), int32(-1), int64(-1), int(-1), "-1", *big.NewInt(-1)}.AddVariants(mod.All...),
			}.Run("-1", t, marshal, unmarshal)

			serialization.PositiveSet{
				Data: []byte("\x7f"),
				Values: mod.Values{
					int8(127), int16(127), int32(127), int64(127), int(127),
					uint8(127), uint16(127), uint32(127), uint64(127), uint(127),
					"127", *big.NewInt(127)}.AddVariants(mod.All...),
			}.Run("127", t, marshal, unmarshal)

			serialization.PositiveSet{
				Data:   []byte("\x80"),
				Values: mod.Values{int8(-128), int16(-128), int32(-128), int64(-128), int(-128), "-128", *big.NewInt(-128)}.AddVariants(mod.All...),
			}.Run("-128", t, marshal, unmarshal)
		})
	}
}
